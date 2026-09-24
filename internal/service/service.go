// Package service wires the HTTP surface and the durable store into one
// process: strict address binding, database open, a one-way readiness gate,
// actual-endpoint reporting and graceful shutdown.
//
// Startup order is deliberately load-bearing: the requested address is
// validated and bound first (so a fixed port that clashes fails before
// anything else happens, and :0 lets the kernel allocate one), and only after
// the database at the configured path has been created and opened does the
// instance become ready and print its endpoint. A failure at any step closes
// what was already acquired and returns an error, so no half-ready service is
// left behind. Two processes with different SYNC_ADDR/SYNC_DATA pairs share
// neither listener nor database.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/server"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

const (
	// defaultAddr is used only when SYNC_ADDR is unset.
	defaultAddr = "127.0.0.1:8080"
	// defaultDataPath is used only when SYNC_DATA is unset.
	defaultDataPath = "data/sync.db"

	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// Options configures one isolated service instance.
type Options struct {
	// Addr is the host:port to bind (SYNC_ADDR). Empty selects defaultAddr.
	// A fixed address binds exactly that address with no fallback; port 0
	// asks the kernel for a free port, reported through OnReady and the log.
	Addr string
	// DataPath is the SQLite database path (SYNC_DATA). Empty selects
	// defaultDataPath; parent directories are created as needed.
	DataPath string
	// Logger receives the one startup line and shutdown diagnostics. nil
	// uses the standard logger.
	Logger *log.Logger
	// OnReady, if set, is invoked with the actual dialable host:port after
	// the listener is bound, the database is open and readiness is on.
	OnReady func(actualAddr string)
}

// Run starts one instance and blocks until ctx is canceled, then stops
// accepting new requests, drains in-flight ones and closes the database. A
// startup failure (bad address, port already in use, data directory or
// database that cannot be created or opened) returns before the instance is
// ready with every acquired resource already released.
func Run(ctx context.Context, opts Options) error {
	addr := opts.Addr
	if addr == "" {
		addr = defaultAddr
	}
	dataPath := opts.DataPath
	if dataPath == "" {
		dataPath = defaultDataPath
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}

	// Validate first: a malformed SYNC_ADDR exits here, before any data
	// directory is created or store opened, and never falls back.
	host, _, err := parseAddr(addr)
	if err != nil {
		return err
	}

	// Bind before opening the database. A fixed port that is already taken
	// fails here with no files touched; :0 resolves to the allocated port.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		return fmt.Errorf("listener on %s reported unexpected address %v", addr, ln.Addr())
	}
	// Report an address a client can actually dial. An empty host means the
	// request was a wildcard bind (":port"); loopback always reaches it.
	displayHost := host
	if displayHost == "" {
		displayHost = "127.0.0.1"
	}
	actualAddr := net.JoinHostPort(displayHost, strconv.Itoa(tcpAddr.Port))

	if dir := filepath.Dir(dataPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			_ = ln.Close()
			return fmt.Errorf("create data directory %q: %w", dir, err)
		}
	}
	st, err := store.Open(dataPath)
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("open store at %q: %w", dataPath, err)
	}

	// The gate covers the whole surface, /healthz included: nothing answers
	// success until the database is open and the listener is serving.
	var ready atomic.Bool
	srv := &http.Server{
		Handler:           server.NewHandlerWithReadiness(st, ready.Load),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()

	ready.Store(true)
	logger.Printf("sync service listening on %s (data: %s)", actualAddr, dataPath)
	if opts.OnReady != nil {
		opts.OnReady(actualAddr)
	}

	// Block until shutdown is requested or the listener dies on its own.
	var serveResult error
	stoppedGracefully := false
	select {
	case <-ctx.Done():
		stoppedGracefully = true
	case serveResult = <-serveErr:
	}

	// Shutdown closes the listener immediately (new connections are refused)
	// and lets in-flight handlers finish before the store is closed. Waiting
	// long polls are interrupted first so they answer instead of pinning the
	// drain until their wait deadline. It is safe even when Serve has already
	// returned on its own.
	st.InterruptWaits()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Printf("graceful shutdown did not complete: %v; forcing connections closed", err)
		_ = srv.Close()
	}
	if stoppedGracefully {
		serveResult = <-serveErr
	}

	if serveResult != nil && !errors.Is(serveResult, http.ErrServerClosed) {
		_ = st.Close()
		return fmt.Errorf("serve on %s: %w", actualAddr, serveResult)
	}
	if err := st.Close(); err != nil {
		// Shutdown was requested and all handlers have drained; report the
		// close problem without pretending requests were lost.
		logger.Printf("close store at %q: %v", dataPath, err)
	}
	return nil
}

// parseAddr validates that addr is a host:port with a TCP port in range. Port
// 0 is legal and means "let the kernel choose". Anything else is a startup
// error rather than a cue to fall back to a default.
func parseAddr(addr string) (host string, port int, err error) {
	host, portStr, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		return "", 0, fmt.Errorf("invalid SYNC_ADDR %q: expected host:port: %v", addr, splitErr)
	}
	port, convErr := strconv.Atoi(portStr)
	if convErr != nil || port < 0 || port > 65535 {
		return "", 0, fmt.Errorf("invalid SYNC_ADDR %q: port must be an integer between 0 and 65535", addr)
	}
	return host, port, nil
}

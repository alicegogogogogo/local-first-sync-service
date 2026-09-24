package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/server"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func main() {
	os.Exit(run())
}

// run executes startup, serving and shutdown, returning the process exit
// code. Any startup failure (bad SYNC_ADDR, occupied port, unusable
// SYNC_DATA) is a non-zero exit with the store closed and nothing served.
func run() int {
	addr := os.Getenv("SYNC_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	// The address must be an explicit host:port; a malformed value is a
	// startup error, never a silent fallback to the default.
	if _, _, err := net.SplitHostPort(addr); err != nil {
		log.Printf("invalid SYNC_ADDR %q: %v", addr, err)
		return 1
	}

	dataPath := os.Getenv("SYNC_DATA")
	if dataPath == "" {
		dataPath = filepath.Join("data", "sync.db")
	}
	if dir := filepath.Dir(dataPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("create data directory %q: %v", dir, err)
			return 1
		}
	}

	s, err := store.Open(dataPath)
	if err != nil {
		log.Printf("open store at %q: %v", dataPath, err)
		return 1
	}

	// Bind the declared address before announcing readiness: a fixed port
	// that is already taken is a startup error, and an ephemeral :0 port is
	// only known once the listener exists. No fallback or port reuse.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		_ = s.Close()
		log.Printf("listen on %q: %v", addr, err)
		return 1
	}

	// The service is ready only now: the database is open and the listener
	// is bound, so /healthz can only answer 200 from this point on.
	log.Printf("sync service listening on %s (data: %s)", displayAddr(ln.Addr()), dataPath)

	srv := &http.Server{Handler: server.NewHandler(s)}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()

	code := 0
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("serve: %v", err)
			code = 1
		}
	case <-ctx.Done():
		// Stop accepting new requests, drain in-flight ones, then close the
		// database so committed state is readable on the next start.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
			code = 1
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("serve: %v", err)
			code = 1
		}
	}

	if err := s.Close(); err != nil {
		log.Printf("close store: %v", err)
		code = 1
	}
	return code
}

// displayAddr renders the listener address as a dialable host:port. An
// ephemeral port is replaced by the allocated one, and an unspecified host
// (":0", "0.0.0.0:8080") is shown as loopback so the logged address is one a
// client can actually reach.
func displayAddr(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return addr.String()
	}
	if tcp.IP == nil || tcp.IP.IsUnspecified() {
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(tcp.Port))
	}
	return tcp.String()
}

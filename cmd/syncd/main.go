package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/alicegogogogogo/local-first-sync-service/internal/server"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func main() {
	addr := os.Getenv("SYNC_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}

	dataPath := os.Getenv("SYNC_DATA")
	if dataPath == "" {
		dataPath = filepath.Join("data", "sync.db")
	}
	if dir := filepath.Dir(dataPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("create data directory %q: %v", dir, err)
		}
	}

	s, err := store.Open(dataPath)
	if err != nil {
		log.Fatalf("open store at %q: %v", dataPath, err)
	}
	defer func() { _ = s.Close() }()

	log.Printf("sync service listening on %s (data: %s)", addr, dataPath)
	if err := http.ListenAndServe(addr, server.NewHandler(s)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

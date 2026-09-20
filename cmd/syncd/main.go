package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/alicegogogogogo/local-first-sync-service/internal/server"
)

func main() {
	addr := os.Getenv("SYNC_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	dataDir := os.Getenv("SYNC_DATA_DIR")
	if dataDir == "" {
		dataDir = "data"
	}
	store, err := server.NewStore(filepath.Join(dataDir, "state.json"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sync service listening on %s", addr)
	if err := http.ListenAndServe(addr, server.NewHandler(store)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

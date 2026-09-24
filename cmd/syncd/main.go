package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alicegogogogogo/local-first-sync-service/internal/service"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err := service.Run(ctx, service.Options{
		Addr:     os.Getenv("SYNC_ADDR"),
		DataPath: os.Getenv("SYNC_DATA"),
	})
	if err != nil {
		// A startup or serving failure ends the process non-zero with the
		// cause intact; a signal-triggered shutdown returns nil.
		log.Fatal(err)
	}
}

package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/imReese/NexusKV/go/config"
	"github.com/imReese/NexusKV/go/controlplane/app"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.DefaultConfig()
	server := app.NewServer(app.ServerConfig{
		ListenAddress: cfg.Admin.ListenAddress,
	})

	logger.Info("starting control plane", "listen_address", cfg.Admin.ListenAddress)
	if err := http.ListenAndServe(cfg.Admin.ListenAddress, server.Handler()); err != nil {
		logger.Error("control plane stopped", "error", err)
		os.Exit(1)
	}
}

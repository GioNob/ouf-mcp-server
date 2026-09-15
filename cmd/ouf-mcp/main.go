package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/kernel"
)

func main() {
	role := flag.String("role", "mcp-server", "process role: mcp-server or maintenance-worker")
	addr := flag.String("http", ":8080", "HTTP listen address")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch *role {
	case "mcp-server":
		runServer(ctx, logger, *addr)
	case "maintenance-worker":
		logger.Info("maintenance worker role reserved for MCP 1B persistence lifecycle")
		<-ctx.Done()
	default:
		logger.Error("unsupported process role", "role", *role)
		os.Exit(2)
	}
}

func runServer(ctx context.Context, logger *slog.Logger, addr string) {
	handler, err := kernel.NewHTTPHandler(logger)
	if err != nil {
		logger.Error("kernel initialization failed", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP shutdown failed", "error", err)
		}
	}()

	logger.Info("OUF MCP server listening", "address", addr, "protocol", kernel.ProtocolVersion)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}

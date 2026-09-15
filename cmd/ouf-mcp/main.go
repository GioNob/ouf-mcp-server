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

	httpadapter "github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	pg "github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/kernel"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 {
		logger.Error("missing command", "allowed", "server, maintenance-worker, migrate")
		os.Exit(2)
	}
	databaseURL := os.Getenv("MCP_DATABASE_URL")
	if databaseURL == "" {
		logger.Error("MCP_DATABASE_URL is required")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		flags := flag.NewFlagSet("server", flag.ExitOnError)
		addr := flags.String("http", ":8080", "HTTP listen address")
		_ = flags.Parse(os.Args[2:])
		runServer(ctx, logger, databaseURL, *addr)
	case "maintenance-worker":
		flags := flag.NewFlagSet("maintenance-worker", flag.ExitOnError)
		once := flags.Bool("once", false, "run one maintenance cycle")
		interval := flags.Duration("interval", 30*time.Second, "maintenance interval")
		_ = flags.Parse(os.Args[2:])
		runMaintenance(ctx, logger, databaseURL, *interval, *once)
	case "migrate":
		runMigrate(ctx, logger, databaseURL)
	default:
		logger.Error("unsupported command", "command", os.Args[1])
		os.Exit(2)
	}
}

func openStore(ctx context.Context, logger *slog.Logger, databaseURL string) *pg.Store {
	store, err := pg.Open(ctx, databaseURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	return store
}

func runMigrate(ctx context.Context, logger *slog.Logger, databaseURL string) {
	store := openStore(ctx, logger, databaseURL)
	defer store.Close()
	if err := pg.Migrate(ctx, store.Pool()); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}
	logger.Info("database migrations applied")
}

func runMaintenance(ctx context.Context, logger *slog.Logger, databaseURL string, interval time.Duration, once bool) {
	if interval <= 0 {
		logger.Error("maintenance interval must be positive")
		os.Exit(2)
	}
	store := openStore(ctx, logger, databaseURL)
	defer store.Close()
	if err := store.Ready(ctx); err != nil {
		logger.Error("database schema is not ready", "error", err)
		os.Exit(1)
	}
	for {
		cycleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := store.Maintain(cycleCtx, time.Now())
		cancel()
		if err != nil {
			logger.Error("maintenance cycle failed", "error", err)
		} else {
			logger.Info("maintenance cycle completed", "orphansMarkedUnknown", result.OrphansMarkedUnknown, "expiredSessionsDeleted", result.ExpiredSessionsDeleted)
		}
		if once {
			if err != nil {
				os.Exit(1)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func runServer(ctx context.Context, logger *slog.Logger, databaseURL, addr string) {
	store := openStore(ctx, logger, databaseURL)
	defer store.Close()
	if err := store.Ready(ctx); err != nil {
		logger.Error("database schema is not ready; run the migration role first", "error", err)
		os.Exit(1)
	}
	snapshot, err := manifest.Load()
	if err != nil {
		logger.Error("manifest load failed", "error", err)
		os.Exit(1)
	}
	payload, err := snapshot.CanonicalPayload()
	if err != nil {
		logger.Error("manifest serialization failed", "error", err)
		os.Exit(1)
	}
	checksum, err := snapshot.Checksum()
	if err != nil {
		logger.Error("manifest checksum failed", "error", err)
		os.Exit(1)
	}
	if err := store.EnsureManifest(ctx, checksum, snapshot.Version, "mcp-manifest-v1", payload); err != nil {
		logger.Error("manifest persistence failed", "error", err)
		os.Exit(1)
	}
	authClient, err := httpadapter.NewAuthorization(os.Getenv("MCP_AUTHORIZATION_ENDPOINT"), os.Getenv("MCP_WORKLOAD_TOKEN"))
	if err != nil {
		logger.Error("authorization client initialization failed", "error", err)
		os.Exit(1)
	}
	gatewayClient, err := httpadapter.NewGateway(os.Getenv("MCP_GATEWAY_ENDPOINT"), os.Getenv("MCP_WORKLOAD_TOKEN"))
	if err != nil {
		logger.Error("Gateway client initialization failed", "error", err)
		os.Exit(1)
	}
	fingerprintKey := []byte(os.Getenv("MCP_FINGERPRINT_KEY"))
	if os.Getenv("MCP_WORKLOAD_TOKEN") == "" {
		logger.Error("MCP_WORKLOAD_TOKEN is required")
		os.Exit(1)
	}
	if len(fingerprintKey) < 32 {
		logger.Error("MCP_FINGERPRINT_KEY must contain at least 32 bytes")
		os.Exit(1)
	}
	service := &orchestration.Service{Auth: authClient, Admission: store, Gateway: gatewayClient, Audit: store, FingerprintKey: fingerprintKey}
	handler, err := kernel.NewGovernedHTTPHandler(logger, service)
	if err != nil {
		logger.Error("kernel initialization failed", "error", err)
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		readyCtx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := store.Ready(readyCtx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP shutdown failed", "error", err)
		}
	}()
	logger.Info("OUF MCP server listening", "address", addr, "protocol", kernel.ProtocolVersion, "manifestChecksum", checksum)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}

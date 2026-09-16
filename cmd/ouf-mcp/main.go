package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	httpadapter "github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	pg "github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/kernel"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/recovery"
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

func positiveIntEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 10000 {
		return 0, errors.New(name + " must be between 1 and 10000")
	}
	return n, nil
}

func runMaintenance(ctx context.Context, logger *slog.Logger, databaseURL string, interval time.Duration, once bool) {
	if interval <= 0 {
		logger.Error("maintenance interval must be positive")
		os.Exit(2)
	}
	batch, err := positiveIntEnv("MCP_MAINTENANCE_BATCH", 50)
	if err != nil {
		logger.Error("invalid maintenance configuration", "error", err)
		os.Exit(2)
	}
	store := openStore(ctx, logger, databaseURL)
	defer store.Close()
	if err := store.Ready(ctx); err != nil {
		logger.Error("database schema is not ready", "error", err)
		os.Exit(1)
	}
	recoveryClient, err := httpadapter.NewRecovery(os.Getenv("MCP_GATEWAY_RECOVERY_ENDPOINT"), os.Getenv("MCP_WORKLOAD_TOKEN"))
	if err != nil {
		logger.Error("recovery client initialization failed", "error", err)
		os.Exit(1)
	}
	maxUnknownHold := 24 * time.Hour
	if raw := os.Getenv("MCP_MAX_UNKNOWN_HOLD"); raw != "" {
		maxUnknownHold, err = time.ParseDuration(raw)
		if err != nil || maxUnknownHold <= 0 {
			logger.Error("MCP_MAX_UNKNOWN_HOLD must be a positive duration", "error", err)
			os.Exit(2)
		}
	}
	worker := recovery.Service{Store: store, Owner: recoveryClient, Audit: store, WorkerID: "maintenance-" + os.Getenv("HOSTNAME"), Lease: 30 * time.Second, AdmissionGrace: 2 * time.Minute, MaxUnknownHold: maxUnknownHold, Batch: batch}
	for {
		cycleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, cycleErr := store.Maintain(cycleCtx, time.Now())
		var recovered recovery.Result
		var debt pg.DebtCacheReconciliationResult
		if cycleErr == nil {
			recovered, cycleErr = worker.RunOnce(cycleCtx, time.Now())
		}
		if cycleErr == nil {
			debt, cycleErr = store.ReconcileDebtCaches(cycleCtx, batch)
		}
		cancel()
		if cycleErr != nil {
			logger.Error("maintenance cycle failed", "error", cycleErr)
		} else {
			logger.Info("maintenance cycle completed", "orphansMarkedUnknown", result.OrphansMarkedUnknown, "expiredSessionsDeleted", result.ExpiredSessionsDeleted, "recoveryClaimed", recovered.Claimed, "reconciled", recovered.Reconciled, "stillUnknown", recovered.StillUnknown, "unresolved", recovered.Unresolved, "debtCachesChecked", debt.Checked, "debtCachesRepaired", debt.Repaired, "debtCacheDelta", debt.Delta)
		}
		if once {
			if cycleErr != nil {
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
	routedGateway := operational.RoutingGateway{Remote: gatewayClient, Self: store}
	service := &orchestration.Service{Auth: authClient, Admission: store, Gateway: routedGateway, Audit: store, FingerprintKey: fingerprintKey}
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

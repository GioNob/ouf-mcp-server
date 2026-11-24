package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/analysisworker"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/uuid"
)

func runAnalysisWorker(ctx context.Context, logger *slog.Logger, dsn string) error {
	m, err := manifest.Load()
	if err != nil {
		return err
	}
	cfg, err := analysis.RunConfiguration(m)
	if err != nil {
		return err
	}
	if !cfg.Enabled || !cfg.Limits.OwnerDedupQualified {
		return fmt.Errorf("ANALYSIS_EXECUTOR_DISABLED")
	}
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.Ready(ctx); err != nil {
		return err
	}
	source, err := workloadTokenSource()
	if err != nil {
		return err
	}
	gateway, err := httpclient.NewGatewayWithTokenSource(os.Getenv("MCP_GATEWAY_ENDPOINT"), source)
	if err != nil || gateway.Endpoint.Scheme != "https" {
		return fmt.Errorf("analysis gateway requires HTTPS")
	}
	worker := analysisworker.Worker{ID: uuid.NewString(), Store: store, Installation: cfg, Manifest: m, Gateway: gateway}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := worker.Once(ctx); err != nil && !errors.Is(err, analysis.ErrFenced) && !errors.Is(err, analysis.ErrRunExhausted) {
				logger.Error("analysis worker held", "code", "ANALYSIS_WORKER_HELD")
				return err
			}
		}
	}
}

func serveAnalysisReservations(ctx context.Context, dsn string) error {
	addr := os.Getenv("OUF_ANALYSIS_RESERVATION_LISTEN")
	identity := os.Getenv("OUF_ANALYSIS_GATEWAY_CLIENT_URI")
	u, err := url.Parse(identity)
	if addr == "" || err != nil || !u.IsAbs() {
		return fmt.Errorf("reservation listener or Gateway URI missing")
	}
	ca, err := os.ReadFile(os.Getenv("OUF_ANALYSIS_GATEWAY_CA_FILE"))
	if err != nil {
		return fmt.Errorf("reservation client trust unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return fmt.Errorf("invalid reservation client trust")
	}
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.Ready(ctx); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/internal/analysis/v1/reservations/check", analysis.ReservationHandler(store, identity))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8192, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}}
	stop := context.AfterFunc(ctx, func() { _ = srv.Close() })
	defer stop()
	err = srv.ListenAndServeTLS(os.Getenv("OUF_ANALYSIS_RESERVATION_CERT_FILE"), os.Getenv("OUF_ANALYSIS_RESERVATION_KEY_FILE"))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

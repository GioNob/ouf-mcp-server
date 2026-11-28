package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	httpadapter "github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
)

func configureBatchMandates(ctx context.Context, logger *slog.Logger, store httpadapter.BatchEvidenceStore, checksum string) (*httpadapter.BatchMandateClient, error) {
	endpoint := os.Getenv("MCP_ANALYSIS_BATCH_GATEWAY_ENDPOINT")
	caFile := os.Getenv("MCP_ANALYSIS_BATCH_GATEWAY_CA_FILE")
	certFile := os.Getenv("MCP_ANALYSIS_BATCH_WORKLOAD_CERT_FILE")
	keyFile := os.Getenv("MCP_ANALYSIS_BATCH_WORKLOAD_KEY_FILE")
	listen := os.Getenv("MCP_ANALYSIS_BATCH_STATE_LISTEN")
	stateCA := os.Getenv("MCP_ANALYSIS_BATCH_STATE_CA_FILE")
	stateCert := os.Getenv("MCP_ANALYSIS_BATCH_STATE_CERT_FILE")
	stateKey := os.Getenv("MCP_ANALYSIS_BATCH_STATE_KEY_FILE")
	pin := os.Getenv("MCP_ANALYSIS_BATCH_STATE_GATEWAY_CERT_SHA256")
	settings := []string{endpoint, caFile, certFile, keyFile, listen, stateCA, stateCert, stateKey, pin}
	configured := 0
	for _, field := range settings {
		if field != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil, nil
	}
	if configured != len(settings) {
		return nil, errors.New("incomplete batch mandate installation profile")
	}
	client, err := httpadapter.NewBatchMandateClient(endpoint, caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	stateTLS, err := httpadapter.BatchMutualTLS(stateCA, stateCert, stateKey)
	if err != nil {
		return nil, err
	}
	leaf, err := hex.DecodeString(pin)
	if err != nil || len(leaf) != sha256.Size {
		return nil, errors.New("invalid batch mandate state Gateway leaf pin")
	}
	stateTLS.ClientAuth, stateTLS.ClientCAs, stateTLS.RootCAs = tls.RequireAndVerifyClientCert, stateTLS.RootCAs, nil
	evidence := httpadapter.BatchEvidenceHandler{Store: store, ManifestChecksum: checksum, Clock: batchEvidenceClock}
	copy(evidence.GatewayLeafSHA256[:], leaf)
	mux := http.NewServeMux()
	mux.Handle("/internal/analysis-batch/v1/state", evidence)
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, errors.New("cannot bind batch mandate state listener")
	}
	srv := &http.Server{
		Handler: mux, TLSConfig: stateTLS, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 4 * time.Second,
		WriteTimeout: 4 * time.Second, IdleTimeout: 15 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = srv.Shutdown(shutdown)
	}()
	go func() {
		if err := srv.ServeTLS(listener, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("batch mandate state listener stopped")
		}
	}()
	return client, nil
}

package analysisworker

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func fixtureWorker(t *testing.T) (context.Context, Worker, analysis.RunRecord) {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := postgres.Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "false")
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := analysis.RunConfiguration(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Enabled = true
	cfg.Limits.OwnerDedupQualified = true
	cfg.Limits.Shared.DependencySlots = 100000
	cfg.Limits.Shared.DependencyPerMinute = 100000
	cfg.Manifest, cfg.Workload = analysis.Digest("fixture"), uuid.NewString()
	spec := analysis.RunSpec{
		Scope:  analysis.Scope{Service: "fixture", Principal: uuid.NewString(), Tenant: uuid.NewString()},
		Issuer: "fixture", Policy: "fixture", Manifest: cfg.Manifest, Workload: cfg.Workload,
		CommandKey: uuid.NewString(), Limits: cfg.Limits,
		Profile: analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, ColumnCount: 2, SchemaHash: "sha256:" + strings.Repeat("b", 64)},
	}
	r, err := store.BeginRun(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	m := analysis.Mandate{ID: uuid.NewString(), Generation: "1", Decision: "fixture",
		Issued: r.Started, Expires: r.Started.Add(time.Hour), Operations: []string{"source.file.preview", "ouf.semantic.search.batch", "semantic.discovery.request"}}
	raw, _ := json.Marshal(m)
	r, err = store.AttachMandate(ctx, spec, raw)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, Worker{Store: store, ID: uuid.NewString(), Installation: cfg, Manifest: snapshot}, r
}

func TestWorkerSeesRevocationWithoutOwnerDispatch(t *testing.T) { // MCP-50V-02
	ctx, w, r := fixtureWorker(t)
	store, m := w.Store, r.Mandate
	if err := store.DelayRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	reply, _ := json.Marshal(map[string]any{"mandateId": m.ID, "generation": m.Generation,
		"expiresAt": m.Expires, "state": "REVOKED", "decisionRef": "fixture-stop"})
	gateway := statusGateway{t: t, mandate: m, reply: orchestration.MandateResponse{Status: 200, Body: reply}}
	w.Gateway = gateway
	if err := w.Once(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := store.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || r.State != "BLOCKED" || r.Reason != "ANALYSIS_MANDATE_REVOKED" {
		t.Fatal(r.State, r.Reason, err)
	}
	var attempts int
	if err := store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1", r.ID()).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatal("unexpected dispatch attempt", attempts, err)
	}
}

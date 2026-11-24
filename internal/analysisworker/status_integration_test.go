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
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestWorkerSeesRevocationWithoutOwnerDispatch(t *testing.T) { // MCP-50V-02
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := postgres.Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "false")
	cfg, err := analysis.RunConfiguration(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Enabled = true
	cfg.Limits.OwnerDedupQualified = true
	cfg.Manifest, cfg.Workload = analysis.Digest("fixture"), uuid.NewString()
	spec := analysis.RunSpec{
		Scope:  analysis.Scope{Service: "fixture", Principal: uuid.NewString(), Tenant: uuid.NewString()},
		Issuer: "fixture", Policy: "fixture", Manifest: cfg.Manifest, Workload: cfg.Workload,
		CommandKey: uuid.NewString(), Limits: cfg.Limits,
		Profile: analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, ColumnCount: 1, SchemaHash: "sha256:" + strings.Repeat("b", 64)},
	}
	r, err := store.BeginRun(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	m := analysis.Mandate{ID: uuid.NewString(), Generation: "1", Decision: "fixture",
		Issued: r.Started, Expires: r.Started.Add(time.Hour), Operations: []string{"source.file.preview"}}
	raw, _ := json.Marshal(m)
	r, err = store.AttachMandate(ctx, spec, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DelayRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	reply, _ := json.Marshal(map[string]any{"mandateId": m.ID, "generation": m.Generation,
		"expiresAt": m.Expires, "state": "REVOKED", "decisionRef": "fixture-stop"})
	gateway := statusGateway{t: t, mandate: m, reply: orchestration.MandateResponse{Status: 200, Body: reply}}
	w := Worker{Store: store, ID: uuid.NewString(), Installation: cfg, Gateway: gateway}
	if err := w.Once(ctx); err != nil {
		t.Fatal(err)
	}
	r, err = store.ReadRun(ctx, spec.Scope, spec.Issuer, spec.ID())
	if err != nil || r.State != "BLOCKED" || r.Reason != "ANALYSIS_MANDATE_REVOKED" {
		t.Fatal(r.State, r.Reason, err)
	}
	var attempts int
	if err := store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1", r.ID()).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatal("unexpected dispatch attempt", attempts, err)
	}
}

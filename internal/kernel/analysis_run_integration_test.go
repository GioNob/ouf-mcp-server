package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/analysisworker"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fixtureMandates struct {
	owner      *publishedPETOwner
	store      *postgres.Store
	manifest   string
	issues     int
	workload   string
	validUntil int64
}

// Use the durable verifier for every owner call. The listener PET tests
// the authenticated wire protocol independently.
func (g *fixtureMandates) MandateCall(ctx context.Context, action string, raw json.RawMessage, delegation string, _ int64, _ time.Duration) (orchestration.MandateResponse, error) {
	var args struct {
		Operation, AttemptID, AnalysisID, Fence, CommandKey, ManifestChecksum string
		Arguments                                                             json.RawMessage
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return orchestration.MandateResponse{}, err
	}
	reply := orchestration.MandateResponse{Status: 200, Decision: "fixture", Correlation: uuid.NewString()}
	if action == "issue" {
		if delegation != "human-proof" || args.ManifestChecksum != g.manifest || !analysis.CanonicalUUID(args.CommandKey) {
			return reply, fmt.Errorf("invalid issue binding")
		}
		g.issues++
		now := time.Now().UTC()
		mandate := analysis.Mandate{ID: uuid.NewString(), Generation: "1", Decision: "fixture", Issued: now, Expires: now.Add(time.Hour),
			Operations: []string{"source.file.preview", "ouf.semantic.search.batch", "semantic.discovery.request", "semantic.discovery.status", "semantic.discovery.candidates"}}
		reply.Body, _ = json.Marshal(mandate)
		return reply, nil
	}
	if action != "dispatch" || delegation != "" {
		return reply, fmt.Errorf("unexpected interactive authority")
	}
	hash, err := analysis.MandateHash(args.Arguments)
	if err != nil {
		return reply, err
	}
	var check analysis.ReservationCheck
	// The dispatch body and the verifier share plan/intent/attempt/fence.
	if err = json.Unmarshal(raw, &check); err != nil {
		return reply, err
	}
	check.Workload, check.Contract, check.Nonce, check.ArgumentsHash = g.workload, analysis.ReservationContract, uuid.NewString(), hash
	var request struct{ RequestID string }
	if err = json.Unmarshal(args.Arguments, &request); err != nil {
		return reply, err
	}
	check.RequestID = request.RequestID
	proof, err := g.store.CheckReservation(ctx, check)
	if err != nil || !proof.Current || proof.ManifestChecksum != g.manifest || proof.ValidUntil <= time.Now().Unix() {
		return reply, fmt.Errorf("pre-dispatch proof: %v", err)
	}
	g.validUntil = proof.ValidUntil
	capability := map[string]string{"source.file.preview": "ouf.managed-source.preview", "ouf.semantic.search.batch": "ouf.semantic.search.batch",
		"semantic.discovery.request": "ouf.semantic.discovery", "semantic.discovery.status": "ouf.semantic.discovery.status",
		"semantic.discovery.candidates": "ouf.semantic.discovery.candidates"}[args.Operation]
	var discovery struct{ IdempotencyKey string }
	_ = json.Unmarshal(args.Arguments, &discovery)
	result, err := g.owner.Execute(ctx, orchestration.GatewayRequest{CapabilityID: capability, Arguments: args.Arguments, IdempotencyKey: discovery.IdempotencyKey}, 0)
	reply.Body = result.Body
	return reply, err
}

func TestRun18StartPollSummary(t *testing.T) { // MCP-50U-06
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	db, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = postgres.Migrate(ctx, db.Pool()); err != nil {
		t.Fatal(err)
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "false")
	cfg, err := analysis.RunConfiguration(m)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Limits.OwnerDedupQualified = true
	cfg.Limits.ProofSeconds = 2
	cfg.Limits.Shared.DependencySlots = 100000
	cfg.Limits.PreviewPageSize = 5
	cfg.Limits.Shared.SourcePerMinute = 120
	cfg.Limits.Shared.DependencyPerMinute = 100000
	cfg.Limits.Shared.TenantPerMinute = 100000
	limits, _ := json.Marshal(cfg.Limits)
	t.Setenv("OUF_ANALYSIS_RUN_LIMITS", string(limits))
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "true")
	t.Setenv("OUF_ANALYSIS_WORKLOAD", "fixture-"+uuid.NewString())
	t.Setenv("OUF_ANALYSIS_MANIFEST_FILE", "../manifest/capabilities.json")
	cfg, err = analysis.RunConfiguration(m)
	if err != nil {
		t.Fatal(err)
	}
	var fixture railFixture
	raw, err := os.ReadFile("../nextstep/testdata/e2e-20261123.json")
	if err != nil || json.Unmarshal(raw, &fixture) != nil || len(fixture.ProfileColumns) != 18 {
		t.Fatal("missing fixture", err)
	}
	owner := &publishedPETOwner{columns: fixture.ProfileColumns, jobs: map[string]string{}}
	gateway := &fixtureMandates{owner: owner, store: db, manifest: cfg.Manifest, workload: cfg.Workload}
	svc := &orchestration.Service{Auth: analysisSDKAuth{}, Admission: publishedPETStore{db}, Gateway: owner, AnalysisMandates: gateway, RequireDelegation: true, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, svc, "ouf.managed-source.preview ouf.semantic.search ouf.semantic.discovery ouf.semantic.discovery.status ouf.semantic.discovery.candidates")
	calls := 0
	call := func(name string, args nextstep.Object) nextstep.Object {
		t.Helper()
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		calls++
		if err != nil || result.IsError {
			t.Fatal(name, result, err)
		}
		return ownerObject(result)
	}
	out := call("source.analysis.plan", nextstep.Object{"mode": "ALL_COLUMNS", "assetId": uuid.NewString(), "profileId": uuid.NewString(), "commandKey": uuid.NewString()})
	if len(out) != 7 || out["contract"] != analysis.RunContract || out["state"] != "RUNNING" {
		t.Fatal(out)
	}
	next := out["next"].(nextstep.Object)
	if next["action"] != "POLL" {
		t.Fatal(next)
	}
	worker := analysisworker.Worker{Store: db, Manifest: m, Gateway: gateway, ID: uuid.NewString(), Installation: cfg}
	// Four preview pages, one batch and three owner calls per residual.
	limit := time.Now().Add(3 * time.Minute)
	for {
		if !time.Now().Before(limit) {
			t.Fatal("fixture did not reach terminal state")
		}
		if d := time.Until(time.Unix(gateway.validUntil, 0)); d > 0 {
			time.Sleep(d)
		}
		if err := worker.Once(ctx); err != nil {
			t.Fatal(err)
		}
		r, err := db.ReadRun(ctx, analysis.Scope{Service: "ouf-chatgpt", Tenant: "tenant-a", Principal: "user-a"}, "issuer", out["analysisId"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if r.State != "RUNNING" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	var internalCalls int
	if err = db.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1", out["analysisId"]).Scan(&internalCalls); err != nil || internalCalls != 4+1+3*15 {
		t.Fatal(internalCalls, err)
	}
	out = call(next["tool"].(string), next["arguments"].(nextstep.Object))
	next = out["next"].(nextstep.Object)
	if out["state"] != "COMPLETE" || next["action"] != "CALL_TOOL" || next["arguments"].(nextstep.Object)["view"] != "SUMMARY" {
		t.Fatal(out)
	}
	out = call(next["tool"].(string), next["arguments"].(nextstep.Object))
	counts := out["counts"].(nextstep.Object)
	if len(out["items"].([]any)) != 18 || fmt.Sprint(counts["matchedPublished"]) != "3" || fmt.Sprint(counts["candidatesReady"]) != "15" ||
		len(owner.jobs) != 15 || owner.batches != 1 || gateway.issues != 1 || calls != 3 {
		t.Fatal(out, calls)
	}
	t.Logf("chatbot SDK calls = %d (1 start + 1 STATUS + 1 SUMMARY)", calls)
}

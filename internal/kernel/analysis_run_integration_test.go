package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
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

func (g *fixtureMandates) DispatchMandate(ctx context.Context, raw json.RawMessage, limit int64, timeout time.Duration) (analysis.DispatchResponse, error) {
	reply, err := g.MandateCall(ctx, "dispatch", raw, "", limit, timeout)
	return analysis.DispatchResponse{MandateResponse: reply}, err
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

func runRailFixture(t *testing.T) (context.Context, *postgres.Store, *mcp.ClientSession, analysisworker.Worker, *fixtureMandates, *publishedPETOwner) {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	db, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
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
	worker := analysisworker.Worker{Store: db, Manifest: m, Gateway: gateway, ID: uuid.NewString(), Installation: cfg}
	return ctx, db, session, worker, gateway, owner
}

func TestRun18StartPollSummary(t *testing.T) { // MCP-50U-06
	ctx, db, session, worker, gateway, owner := runRailFixture(t)
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
	if err := db.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1", out["analysisId"]).Scan(&internalCalls); err != nil || internalCalls != 4+1+3*15 {
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

type railRenewalAuthority struct{}

func (railRenewalAuthority) AuthorizeRunRenewal(_ context.Context, _ orchestration.Identity, c analysis.RunRenewalCheck) (analysis.RenewalDecision, error) {
	hash, err := c.Hash()
	return analysis.RenewalDecision{RequestHash: hash, DecisionRef: uuid.NewString(), GrantRef: "fixture", ValidUntil: time.Now().Add(time.Minute)}, err
}

func TestRenewalNextAgainstLiveCatalog(t *testing.T) { // MCP-55B-01, PET-mcp-057/163
	t.Setenv("OUF_ANALYSIS_RENEWAL_REVIEW_BASE", "https://ths.invalid/trusted-human/analysis-renewal/")
	t.Setenv("OUF_ANALYSIS_RENEWAL_CHECK_ENDPOINT", "https://gateway.invalid"+analysis.RenewalCheckPath)
	t.Setenv("OUF_ANALYSIS_RENEWAL_ISSUER", "issuer")
	t.Setenv("OUF_ANALYSIS_RENEWAL_AUDIENCE", "gateway")
	t.Setenv("OUF_ANALYSIS_RENEWAL_THS_CLIENT", "ths")
	ctx, db, session, _, gateway, _ := runRailFixture(t)
	call := func(tool string, args nextstep.Object) nextstep.Object {
		t.Helper()
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil || result.IsError {
			t.Fatal(tool, result, err)
		}
		return ownerObject(result)
	}
	follow := func(next nextstep.Object) nextstep.Object {
		t.Helper()
		return call(next["tool"].(string), next["arguments"].(nextstep.Object))
	}
	start := call("source.analysis.plan", nextstep.Object{"mode": "ALL_COLUMNS", "assetId": uuid.NewString(), "profileId": uuid.NewString(), "commandKey": uuid.NewString()})
	id := start["analysisId"].(string)
	scope := analysis.Scope{Service: "ouf-chatgpt", Principal: "user-a", Tenant: "tenant-a"}
	run := func() analysis.RunRecord {
		t.Helper()
		r, err := db.ReadRun(ctx, scope, "issuer", id)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	exhaust := func() {
		t.Helper()
		if _, err := db.Pool().Exec(ctx, "update ouf_mcp.analysis_run set budget_end=clock_timestamp() where analysis_id=$1", id); err != nil {
			t.Fatal(err)
		}
		r := run()
		w, err := db.NextRunWork(ctx, r, uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrRunExhausted) {
			t.Fatal(err)
		}
	}
	i := orchestration.Identity{Issuer: "issuer", PrincipalID: scope.Principal, TenantID: scope.Tenant, ServicePrincipalID: gateway.workload, ActorType: "HUMAN", Delegation: "fixture"}
	decide := func(action string) {
		t.Helper()
		p, err := db.CurrentRenewal(ctx, run())
		if err != nil {
			t.Fatal(err)
		}
		c := analysis.RenewalCommand{ReviewRef: p.ReviewRef, Version: p.Version, Hash: p.Hash, ExpectedSeq: strconv.FormatInt(p.ExpectedSeq, 10), Key: uuid.NewString(), Decision: action, Items: []analysis.RenewalItem{}}
		if action == "CHANGES_REQUESTED" {
			c.Items = []analysis.RenewalItem{{Field: "general", Comment: "Rivedere i limiti"}}
		}
		if _, err = db.DecideRenewal(ctx, i, c, railRenewalAuthority{}); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(status nextstep.Object) nextstep.Object {
		t.Helper()
		n := status["next"].(nextstep.Object)
		if n["action"] != "BLOCKED" || n["actor"] != "HUMAN_THS" || n["reasonCode"] != "ANALYSIS_BUDGET_EXHAUSTED" {
			t.Fatal(n)
		}
		alts := n["alternatives"].([]any)
		a := alts[0].(nextstep.Object)
		if a["action"] != "WAIT_FOR_HUMAN" || a["tool"] != "ouf.wait" {
			t.Fatal(a)
		}
		return a
	}
	exhaust()
	status := follow(start["next"].(nextstep.Object))
	pending := wait(status)
	decide("CHANGES_REQUESTED")
	changed := follow(pending)
	revise := changed["next"].(nextstep.Object)
	if revise["tool"] != analysis.RenewalTool {
		t.Fatal(revise)
	}
	version := follow(revise)
	if fmt.Sprint(version["proposalVersion"]) != "2" {
		t.Fatal(version)
	}
	status = follow(version["next"].(nextstep.Object))
	pending = wait(status)
	decide("APPROVE")
	approved := follow(pending)
	poll := approved["next"].(nextstep.Object)
	if poll["action"] != "POLL" || poll["reasonCode"] != "ANALYSIS_RUNNING" {
		t.Fatal(poll)
	}
	exhaust()
	status = follow(poll)
	pending = wait(status)
	decide("CLOSE_PARTIAL")
	closed := follow(pending)
	summary := follow(closed["next"].(nextstep.Object))
	if summary["state"] != "BLOCKED" || summary["items"] == nil {
		t.Fatal(summary)
	}
}

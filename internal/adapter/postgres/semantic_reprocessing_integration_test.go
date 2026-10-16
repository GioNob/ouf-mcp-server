package postgres

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
 "encoding/hex"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/reprocessing"
	"github.com/google/uuid"
)

type reprocessingAuth struct{ allowed bool }

func (a *reprocessingAuth) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: a.allowed, DecisionRef: "current-fixture-policy"}, nil
}

type reprocessingGateway struct {
	calls   atomic.Int32
	lastKey string
	mu      sync.Mutex
	fail    bool
}

func (g *reprocessingGateway) Execute(_ context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	n := g.calls.Add(1)
	g.mu.Lock()
	g.lastKey = in.IdempotencyKey
	g.mu.Unlock()
	if n == 1 || g.fail {
		return orchestration.GatewayResponse{Status: 403, Problem: &orchestration.Problem{Status: 403, Code: "SEM_CAPABILITY_REQUIRED:ouf.semantic.propose"}}, nil
	}
	return orchestration.GatewayResponse{Status: 200, Body: []byte(`{"status":"DRAFT","fixture":true}`)}, nil
}

type reprocessingFixture struct {
	store   *Store
	service *orchestration.Service
	in      orchestration.Invocation
	prior   uuid.UUID
	gateway *reprocessingGateway
	auth    *reprocessingAuth
}

func newReprocessingFixture(t *testing.T, tenant ...string) *reprocessingFixture {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	s, e := Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	if e = Migrate(ctx, s.Pool()); e != nil {
		t.Fatal(e)
	}
	body := []byte(`{"capabilities":[]}`)
	manifest := reprocessing.Hash(body)
	if e = s.EnsureManifest(ctx, manifest, "reprocessing-fixture-v1", "mcp-manifest-v1", body); e != nil {
		t.Fatal(e)
	}
	id := uuid.NewString()
	if len(tenant) > 0 {
		id = tenant[0]
	}
	g := &reprocessingGateway{}
	auth := &reprocessingAuth{allowed: true}
	service := &orchestration.Service{RequireDelegation: true, Auth: auth, Admission: s, Gateway: g, Audit: s, FingerprintKey: make([]byte, 32), ReprocessingGuard: func() error { return nil }}
	in := orchestration.Invocation{Identity: orchestration.Identity{ServicePrincipalID: "workload-fixture", PrincipalID: "human-fixture", TenantID: id, ActorType: "HUMAN", AuthenticationContextRef: "authn-fixture", Delegation: "synthetic-gateway-delegation"}, CapabilityID: "ouf.semantic.propose", Owner: "semantic", OperationClass: "COMMAND", GatewayBindingRef: "fixture.semantic.create", ManifestChecksum: manifest, Arguments: json.RawMessage(`{"commandKey":"stable-draft","artifactType":"ONTOLOGY"}`), IdempotencyKey: "stable-draft", CorrelationID: uuid.NewString(), Window: time.Minute, Timeout: time.Second, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, ResultBytes: 4096}}
	if len(tenant) > 0 {
		in.IdempotencyKey = "draft-" + uuid.NewString()
		in.Arguments, _ = json.Marshal(map[string]any{"commandKey": in.IdempotencyKey, "artifactType": "ONTOLOGY"})
	}
	result, e := service.Call(ctx, in)
	if e != nil || result.Problem == nil || g.calls.Load() != 1 {
		t.Fatalf("initial failure missing: %+v %v", result, e)
	}
	return &reprocessingFixture{s, service, in, result.AttemptID, g, auth}
}
func (f *reprocessingFixture)permit(t *testing.T,change func(*reprocessing.Permit),caps ...int64)*reprocessing.VerifiedPermit{
 t.Helper();now:=time.Now().Unix();pub1,key1,_:=ed25519.GenerateKey(rand.Reader)
 policy,_:=json.Marshal(reprocessing.TrustPolicy{Schema:"ouf.semantic-deployment-trust-policy.v1",InstallationRef:"fixture",EntityRef:"fixture-entity",Keys:[]reprocessing.TrustKey{{KeyRef:"existing-fixture-installer",IssuerRef:"fixture-installer-issuer",Roles:[]string{"DEPLOYMENT_INTENT","FINAL_DEPLOYMENT_APPROVAL"},PublicKey:hex.EncodeToString(pub1),NotBefore:now-3600,ExpiresAt:now+3600,State:"ACTIVE"}}})
 capacity:=int64(64);if len(caps)>0{capacity=caps[0]}
 configured:=reprocessing.KeyProfile{Schema:"ouf.mcp-reprocessing-key-profile.v1",MaxSessionsPerTenant:capacity,InstallationRef:"fixture",EntityRef:"fixture-entity",InstallerKeyRef:"existing-fixture-installer",IntentIssuerRef:"fixture-installer-issuer",ApprovalIssuerRef:"fixture-installer-issuer",TrustPolicyHash:reprocessing.Hash(policy)}
 profile,_:=json.Marshal(configured)
 p := reprocessing.Permit{Schema: "ouf.mcp-semantic-reprocessing-session.v1", SessionID: uuid.NewString(), DecisionRef: "fixture-human-one-call-consent", Reason: "FIXTURE_EXPLICIT_REPROCESSING", InstallationRef: "fixture", KeyProfileHash: reprocessing.Hash(profile), ManifestChecksum: f.in.ManifestChecksum, ServicePrincipalID: f.in.Identity.ServicePrincipalID, HumanSubject: f.in.Identity.PrincipalID, TenantID: f.in.Identity.TenantID, Owner: "semantic", CapabilityID: f.in.CapabilityID, OperationClass: "COMMAND", CommandKey: f.in.IdempotencyKey, RequestHash: reprocessing.Hash(f.in.Arguments), PredecessorAttemptID: f.prior.String(), PredecessorLockVersion: 2, PredecessorOutcomeCode: "SEM_CAPABILITY_REQUIRED:ouf.semantic.propose", PriorToolCalls: 1, MaximumNewToolCalls: 1, MaximumCumulativeToolCalls: 2, OwnerReceiptAbsentAt: now - 1, IssuedAt: now, ExpiresAt: now + 600}
 if change!=nil{change(&p)};raw,_:=json.Marshal(p)
 intent:=reprocessing.SignatureHeader(configured,raw,"DEPLOYMENT_INTENT");frame,_:=reprocessing.SigningBytes(raw,intent);intent.Signature=hex.EncodeToString(ed25519.Sign(key1,frame))
 approval:=reprocessing.SignatureHeader(configured,raw,"FINAL_DEPLOYMENT_APPROVAL");frame,_=reprocessing.SigningBytes(raw,approval);approval.Signature=hex.EncodeToString(ed25519.Sign(key1,frame))
 env,_:=json.Marshal(reprocessing.Envelope{Payload:raw,Intent:intent,Approval:approval})
 v,e:=reprocessing.Verify(profile,policy,env,reprocessing.Hash(profile),f.in.ManifestChecksum,time.Now());if e!=nil{t.Fatal(e)};return v
}
func (f *reprocessingFixture) snapshot(t *testing.T) []byte {
	t.Helper()
	var raw []byte
	e := f.store.Pool().QueryRow(context.Background(), `select jsonb_build_object('claim',to_jsonb(c),'attempt',to_jsonb(a),'reservation',to_jsonb(b),'window',to_jsonb(w),'guard',to_jsonb(g)) from ouf_mcp.idempotency_claim c join ouf_mcp.tool_attempt a using(attempt_id) join ouf_mcp.budget_reservation b using(attempt_id) join ouf_mcp.attempt_admission_context ac using(attempt_id) join ouf_mcp.budget_window w on w.budget_window_id=ac.budget_window_id join ouf_mcp.retry_guard g on g.equivalence_group_id=ac.equivalence_group_id where c.attempt_id=$1`, f.prior).Scan(&raw)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func TestSemanticReprocessingOneNewDispatchSameOwnerKeyOldHistoryUnchanged(t *testing.T) {
	f := newReprocessingFixture(t)
	ctx := context.Background()
	before := f.snapshot(t)
	replay, e := f.service.Call(ctx, f.in)
	if e != nil || !replay.Replay || f.gateway.calls.Load() != 1 {
		t.Fatal("ordinary replay must stay closed")
	}
	f.service.Reprocessing = f.permit(t, nil)
	result, e := f.service.Call(ctx, f.in)
	if e != nil || result.Problem != nil || result.Replay || f.gateway.calls.Load() != 2 {
		t.Fatalf("new session: %+v %v", result, e)
	}
	if f.gateway.lastKey != f.in.IdempotencyKey {
		t.Fatal("owner key changed")
	}
	if !bytes.Equal(before, f.snapshot(t)) {
		t.Fatal("original history mutated")
	}
	replay, e = f.service.Call(ctx, f.in)
	if e != nil || !replay.Replay || replay.AttemptID != result.AttemptID || f.gateway.calls.Load() != 2 {
		t.Fatal("permit reused")
	}
	var cumulative int64
	e = f.store.Pool().QueryRow(ctx, `select r.prior_tool_calls+b.actual_tool_calls from ouf_mcp.semantic_reprocessing_session r join ouf_mcp.budget_reservation b on b.attempt_id=r.new_attempt_id where r.predecessor_attempt_id=$1`, f.prior).Scan(&cumulative)
	if e != nil || cumulative != 2 {
		t.Fatalf("cumulative history lost: %d %v", cumulative, e)
	}
	if _, e = f.store.Pool().Exec(ctx, `delete from ouf_mcp.semantic_reprocessing_session where predecessor_attempt_id=$1`, f.prior); e == nil {
		t.Fatal("session deletable")
	}
}
func TestSemanticReprocessingConcurrentPermitCannotMultiplyDispatch(t *testing.T) {
	f := newReprocessingFixture(t)
	f.service.Reprocessing = f.permit(t, nil)
	before := f.snapshot(t)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = f.service.Call(context.Background(), f.in) }()
	}
	wg.Wait()
	if f.gateway.calls.Load() != 2 {
		t.Fatalf("duplicate dispatch: %d", f.gateway.calls.Load())
	}
	if !bytes.Equal(before, f.snapshot(t)) {
		t.Fatal("old history changed")
	}
}
func TestSemanticReprocessingRevocationAndScopeDriftCannotDispatch(t *testing.T) {
	f := newReprocessingFixture(t)
	f.service.Reprocessing = f.permit(t, nil)
	f.auth.allowed = false
	if _, e := f.service.Call(context.Background(), f.in); !errors.Is(e, orchestration.ErrUnauthorized) {
		t.Fatal("revocation ignored")
	}
	f.auth.allowed = true
	bad := f.in
	bad.Arguments = json.RawMessage(`{"commandKey":"stable-draft","artifactType":"CLASS"}`)
	if _, e := f.service.Call(context.Background(), bad); e == nil {
		t.Fatal("different body accepted")
	}
	f.service.Reprocessing = f.permit(t, func(p *reprocessing.Permit) { p.PredecessorLockVersion = 1 })
	if _, e := f.service.Call(context.Background(), f.in); e == nil {
		t.Fatal("stale predecessor accepted")
	}
	if f.gateway.calls.Load() != 1 {
		t.Fatal("denied request dispatched")
	}
}
func TestSemanticReprocessingSecondFailureAndAlternativePermitStayClosed(t *testing.T) {
	f := newReprocessingFixture(t)
	f.service.Reprocessing = f.permit(t, nil)
	f.gateway.fail = true
	result, e := f.service.Call(context.Background(), f.in)
	if e != nil || result.Problem == nil {
		t.Fatal("fixture failure missing")
	}
	f.service.Reprocessing = f.permit(t, nil)
	if _, e = f.service.Call(context.Background(), f.in); e == nil {
		t.Fatal("new permit bypassed consumed scope")
	}
	if f.gateway.calls.Load() != 2 {
		t.Fatal("third dispatch")
	}
}
func TestSemanticReprocessingUnknownDebtAndExpiredTimeRemainBlocked(t *testing.T) {
	for _, mode := range []string{"unknown", "guard", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f := newReprocessingFixture(t)
			before := f.snapshot(t)
			if mode == "expired" {
				f.service.Reprocessing = f.permit(t, func(p *reprocessing.Permit) { p.ExpiresAt = p.IssuedAt + 1 })
			} else {
				f.service.Reprocessing = f.permit(t, nil)
			}
			if mode == "unknown" {
				_, e := f.store.Pool().Exec(context.Background(), `update ouf_mcp.tool_attempt set state='UNKNOWN',dispatch_state='UNKNOWN',completed_at=null where attempt_id=$1`, f.prior)
				if e != nil {
					t.Fatal(e)
				}
			}
			if mode == "guard" {
				_, e := f.store.Pool().Exec(context.Background(), `update ouf_mcp.retry_guard set blocking_attempts=1 where equivalence_group_id=(select equivalence_group_id from ouf_mcp.attempt_admission_context where attempt_id=$1)`, f.prior)
				if e != nil {
					t.Fatal(e)
				}
			}
			if _, e := f.service.Call(context.Background(), f.in); e == nil {
				t.Fatal("unsafe reprocessing accepted")
			}
			if f.gateway.calls.Load() != 1 {
				t.Fatal("blocked case dispatched")
			}
			if mode == "expired" && !bytes.Equal(before, f.snapshot(t)) {
				t.Fatal("expiry mutated original")
			}
		})
	}
}

func TestSemanticReprocessingCapacityAndRetentionDoNotDiscardHistoryOrBlockIndependentGraphs(t *testing.T) {
	tenant := uuid.NewString()
	f := newReprocessingFixture(t, tenant)
	f.service.Reprocessing = f.permit(t, nil, 1)
	result, e := f.service.Call(context.Background(), f.in)
	if e != nil || result.Problem != nil {
		t.Fatalf("first bounded session: %+v %v", result, e)
	}
	other := newReprocessingFixture(t, tenant)
	other.service.Reprocessing = other.permit(t, nil, 1)
	if _, e = other.service.Call(context.Background(), other.in); e == nil {
		t.Fatal("capacity ignored")
	}
	if other.gateway.calls.Load() != 1 {
		t.Fatal("exhausted session dispatched")
	}
	ctx := context.Background()
	var cutoff time.Time
	if e = f.store.Pool().QueryRow(ctx, `select clock_timestamp()`).Scan(&cutoff); e != nil {
		t.Fatal(e)
	}
	var count int
	if e = f.store.Pool().QueryRow(ctx, `select ouf_mcp.purge_audit_events($1,10000)`, cutoff).Scan(&count); e != nil {
		t.Fatal(e)
	}
	if e = f.store.Pool().QueryRow(ctx, `select ouf_mcp.purge_terminal_attempt_graph($1,1000)`, cutoff).Scan(&count); e != nil {
		t.Fatal(e)
	}
	var protected, independent int
	if e = f.store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.tool_attempt where attempt_id=$1 or attempt_id=$2`, f.prior, result.AttemptID).Scan(&protected); e != nil {
		t.Fatal(e)
	}
	if protected != 2 {
		t.Fatal("cumulative history was purged")
	}
	if e = f.store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.tool_attempt where attempt_id=$1`, other.prior).Scan(&independent); e != nil {
		t.Fatal(e)
	}
	if independent != 0 {
		t.Fatal("independent eligible graph was blocked by retained session")
	}
}

func TestSemanticReprocessingGuardRevokedAfterAdmissionDoesNotReachOwner(t *testing.T) {
	f := newReprocessingFixture(t)
	f.service.Reprocessing = f.permit(t, nil)
	var checks int
	f.service.ReprocessingGuard = func() error {
		checks++
		if checks > 1 {
			return reprocessing.ErrPermit
		}
		return nil
	}
	if _, e := f.service.Call(context.Background(), f.in); e == nil {
		t.Fatal("revoked guard allowed dispatch")
	}
	if f.gateway.calls.Load() != 1 {
		t.Fatal("late revocation reached owner")
	}
	var state string
	if e := f.store.Pool().QueryRow(context.Background(), `select a.state from ouf_mcp.semantic_reprocessing_session r join ouf_mcp.tool_attempt a on a.attempt_id=r.new_attempt_id where r.predecessor_attempt_id=$1`, f.prior).Scan(&state); e != nil || state != "ADMITTED" {
		t.Fatalf("ambiguous pre-dispatch stage lost: %s %v", state, e)
	}
	// No second reservation or owner call is allowed after the one-use claim.
	f.service.ReprocessingGuard = func() error { return nil }
	result, e := f.service.Call(context.Background(), f.in)
	if e != nil || !result.Replay || f.gateway.calls.Load() != 1 {
		t.Fatal("revoked admission replayed")
	}
}

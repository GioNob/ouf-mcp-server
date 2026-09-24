package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"testing"
	"time"
)

type fakeAuth struct{ allow bool }

func (f *fakeAuth) Authorize(context.Context, AuthorizationRequest) (AuthorizationDecision, error) {
	code := "ALLOW"
	if !f.allow {
		code = "SCOPE_MISSING"
	}
	return AuthorizationDecision{Allowed: f.allow, DecisionRef: "bundle:17:test", DecisionCode: code}, nil
}

type fakeAdmission struct {
	reserves, running, reconciles int
	err                           error
}

func (f *fakeAdmission) Reserve(context.Context, AdmissionRequest) (AdmissionDecision, error) {
	f.reserves++
	return AdmissionDecision{AttemptID: uuid.New()}, f.err
}
func (f *fakeAdmission) Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error {
	f.running++
	return nil
}
func (f *fakeAdmission) Reconcile(context.Context, uuid.UUID, Cost, AttemptOutcome) error {
	f.reconciles++
	return nil
}

type fakeGateway struct {
	calls    int
	response GatewayResponse
}

func (f *fakeGateway) Execute(context.Context, GatewayRequest, time.Duration) (GatewayResponse, error) {
	f.calls++
	return f.response, nil
}

type fakeAudit struct{ events []AuditEvent }

func (f *fakeAudit) Audit(_ context.Context, e AuditEvent) error {
	f.events = append(f.events, e)
	return nil
}

func invocation() Invocation {
	return Invocation{Identity: Identity{ServicePrincipalID: "mcp", PrincipalID: "agent", TenantID: "tenant"}, CapabilityID: "urban.object.related_search", Owner: "udp", OperationClass: "SEARCH", GatewayBindingRef: "capability://urban.object.related_search", ManifestChecksum: "checksum", Arguments: json.RawMessage(`{"targetTypeCodes":["B","A"],"limit":5}`), IdempotencyKey: "idem", CorrelationID: "corr", Window: time.Minute, Timeout: time.Second, RetryThreshold: 3, Maximum: Cost{ToolCalls: 1, ResultBytes: 32}}
}
func TestAuthorizationDenialPrecedesAdmissionAndGateway(t *testing.T) {
	a := &fakeAdmission{}
	g := &fakeGateway{}
	audit := &fakeAudit{}
	_, err := (Service{Auth: &fakeAuth{}, Admission: a, Gateway: g, Audit: audit, FingerprintKey: make([]byte, 32)}).Call(context.Background(), invocation())
	if !errors.Is(err, ErrUnauthorized) || a.reserves != 0 || g.calls != 0 {
		t.Fatalf("err=%v reserve=%d gateway=%d", err, a.reserves, g.calls)
	}
	if len(audit.events) != 1 {
		t.Fatalf("audit events=%d", len(audit.events))
	}
	e := audit.events[0]
	if e.EventType != "AUTHORIZATION_DENIED_PRE_ADMISSION" || e.OutcomeCode != "SCOPE_MISSING" || e.AuthorizationDecisionRef != "bundle:17:test" || e.AttemptID != uuid.Nil {
		t.Fatalf("unexpected audit event: %+v", e)
	}
}
func TestAdmissionDenialPrecedesGateway(t *testing.T) {
	a := &fakeAdmission{err: ErrToolSelectionStall}
	g := &fakeGateway{}
	_, err := (Service{Auth: &fakeAuth{true}, Admission: a, Gateway: g, FingerprintKey: make([]byte, 32)}).Call(context.Background(), invocation())
	if !errors.Is(err, ErrToolSelectionStall) || g.calls != 0 {
		t.Fatalf("err=%v gateway=%d", err, g.calls)
	}
}
func TestGatewayProblemAndRetryAfterArePreserved(t *testing.T) {
	p := &Problem{Type: "urn:problem", Status: 429, Code: "BUDGET_EXHAUSTED", RetryAfter: "17"}
	a := &fakeAdmission{}
	g := &fakeGateway{response: GatewayResponse{Status: 429, Problem: p}}
	got, err := (Service{Auth: &fakeAuth{true}, Admission: a, Gateway: g, FingerprintKey: make([]byte, 32)}).Call(context.Background(), invocation())
	if err != nil || got.Problem != p || a.reconciles != 1 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}
func TestResultLimitReconcilesFailure(t *testing.T) {
	a := &fakeAdmission{}
	g := &fakeGateway{response: GatewayResponse{Status: 200, Body: make([]byte, 33)}}
	_, err := (Service{Auth: &fakeAuth{true}, Admission: a, Gateway: g, FingerprintKey: make([]byte, 32)}).Call(context.Background(), invocation())
	if !errors.Is(err, ErrResultLimit) || a.reconciles != 1 {
		t.Fatalf("err=%v reconciles=%d", err, a.reconciles)
	}
}
func TestFingerprintIgnoresCosmeticTargetOrder(t *testing.T) {
	k := make([]byte, 32)
	a, _ := SemanticFingerprint(k, "cap", json.RawMessage(`{"targetTypeCodes":["A","B"],"limit":5}`))
	b, _ := SemanticFingerprint(k, "cap", json.RawMessage(`{"limit":5,"targetTypeCodes":["B","A"]}`))
	if a != b {
		t.Fatalf("fingerprints differ")
	}
	c, _ := SemanticFingerprint(k, "cap", json.RawMessage(`{"targetTypeCodes":["A","C"],"limit":5}`))
	if a == c {
		t.Fatalf("semantic change collapsed")
	}
}

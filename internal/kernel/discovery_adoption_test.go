package kernel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const adoptionReceipt = `{
  "requestId": "11111111-1111-4111-8111-111111111111",
  "candidateId": "22222222-2222-4222-8222-222222222222",
  "artifactId": "33333333-3333-4333-8333-333333333333",
  "revisionId": "44444444-4444-4444-8444-444444444444",
  "expectedRowVersion": 0,
  "semanticId": "https://example.org/onto/Place",
  "status": "DRAFT",
  "provenance": {
    "providerId": "SCHEMA_GOV_IT",
    "canonicalUri": "https://example.org/onto/Place",
    "sourceLocation": "https://example.org/catalogue/place.ttl",
    "publisher": "https://example.org/publisher",
    "license": null,
    "licenseWarning": "licenza mancante",
    "snapshotId": "55555555-5555-4555-8555-555555555555",
    "rdfSha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "receiptSemantics": "DURABLE_COMMAND_RESULT_NOT_CURRENT_STATE",
  "runtimeAuthorityIssued": false,
  "notReleaseAcceptance": true
}`

func TestSemanticAdoptionDescriptorMatchesOwnerPR49(t *testing.T) {
	source := bytes.ReplaceAll(adoptionContract, []byte("\r\n"), []byte("\n"))
	if fmt.Sprintf("%x", sha256.Sum256(source)) != "7d48d80559f820185ab46f97625f38e86ead17fd3cdcfa533485b622c54152c1" {
		t.Fatal("owner schema differs from commit 8c0b1a23228d6ad0d99f16283f69d448c36b2560")
	}
	var owner struct {
		Defs map[string]map[string]any `json:"$defs"`
	}
	if err := json.Unmarshal(source, &owner); err != nil {
		t.Fatal(err)
	}
	want := owner.Defs["arguments"]
	want["$defs"] = map[string]any{"uuid": owner.Defs["uuid"]}
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range snapshot.ToolEligible() {
		if c.ToolName != "semantic.discovery.adopt" {
			continue
		}
		var got map[string]any
		if json.Unmarshal(c.InputSchema, &got) != nil || !reflect.DeepEqual(want, got) ||
			c.CapabilityID != "ouf.semantic.discovery.adopt" || c.RequiredAuthorization != c.CapabilityID ||
			c.GatewayBindingRef != "capability://"+c.CapabilityID || c.OperationClass != "COMMAND" || c.Owner != "semantic" ||
			c.RequiredScope != "ouf.semantic.discovery" || !reflect.DeepEqual(c.AllowedActors, []string{"HUMAN"}) {
			t.Fatal("adoption descriptor differs from owner contract")
		}
		return
	}
	t.Fatal("missing adoption tool")
}

type adoptionAdmission struct {
	statusAdmission
	commandKey string
	attemptID  uuid.UUID
}

func TestSemanticAdoptionRejectsAgentBeforeAdmission(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range snapshot.ToolEligible() {
		if c.ToolName == "semantic.discovery.adopt" {
			id := requestIdentity{Identity: orchestration.Identity{ActorType: "AI_AGENT", Scopes: []string{c.RequiredScope}}}
			ctx := context.WithValue(context.Background(), identityKey{}, id)
			result, _, err := invoke(ctx, c, "", nil, nil, 1, 262144)
			if err != nil || !result.IsError || result.Content[0].(*mcp.TextContent).Text != `{"code":"authorization denied","retryable":false,"authorizationPhase":"MCP_PRE_ADMISSION","reasonCode":"ACTOR_NOT_ALLOWED"}` {
				t.Fatal(result, err)
			}
			return
		}
	}
	t.Fatal("missing adoption tool")
}

func (a *adoptionAdmission) Reserve(_ context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	if in.OperationClass == "COMMAND" {
		if in.IdempotencyKey == a.commandKey && a.attemptID != uuid.Nil {
			return orchestration.AdmissionDecision{AttemptID: a.attemptID, Replay: true, AttemptState: "SUCCEEDED"}, nil
		}
		a.commandKey, a.attemptID = in.IdempotencyKey, in.AttemptID
	}
	return orchestration.AdmissionDecision{AttemptID: in.AttemptID}, nil
}

type adoptionGateway struct {
	t             *testing.T
	commands      int
	reads         int
	loseResponse  bool
	commandReply  *orchestration.GatewayResponse
	storedReceipt string
}

func (g *adoptionGateway) Execute(_ context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	if in.CapabilityID == "ouf.semantic.discovery.adopt" {
		g.commands++
		if g.loseResponse {
			return orchestration.GatewayResponse{}, fmt.Errorf("private transport diagnostic")
		}
		if g.commandReply != nil {
			return *g.commandReply, nil
		}
		return orchestration.GatewayResponse{Status: 200, Body: []byte(adoptionReceipt)}, nil
	}
	g.reads++
	if in.CapabilityID != "ouf.semantic.proposal.read" || in.OperationClass != "READ" || string(in.Arguments) != `{"commandKey":"adopt-01"}` || in.IdempotencyKey == "" || in.IdempotencyKey == "adopt-01" {
		g.t.Fatal("wrong receipt lookup", in.CapabilityID, string(in.Arguments))
	}
	stored := g.storedReceipt
	if stored == "" {
		stored = adoptionReceipt
	}
	body := `{"operation":"ADOPT","historicalSnapshot":true,"ownerStateMustBeRechecked":true,"result":` + stored + `}`
	return orchestration.GatewayResponse{Status: 200, Body: []byte(body)}, nil
}

type adoptionAuth struct{ denyRead bool }

func (a adoptionAuth) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	allowed := in.CapabilityID == "ouf.semantic.discovery.adopt" || (in.CapabilityID == "ouf.semantic.proposal.read" && !a.denyRead)
	return orchestration.AuthorizationDecision{Allowed: allowed, DecisionRef: "test"}, nil
}

func TestSemanticAdoptionReceiptsReplayLossAndOwnerError(t *testing.T) {
	const readRequired = `{"code":"SEMANTIC_RECEIPT_READ_REQUIRED","retryable":false}`
	ack := &orchestration.GatewayResponse{Status: 202}
	extended := strings.Replace(adoptionReceipt, `"rdfSha256":`, `"ontologySha256":"`+strings.Repeat("b", 64)+`","rdfSha256":`, 1)
	current := strings.NewReplacer(`"DRAFT"`, `"ACTIVE","publicationSetId":"66666666-6666-4666-8666-666666666666"`,
		"DURABLE_COMMAND_RESULT_NOT_CURRENT_STATE", "CURRENT_ADOPTION_REFERENCE").Replace(adoptionReceipt)
	published := strings.Replace(current, "CURRENT_ADOPTION_REFERENCE", "DURABLE_COMMAND_RESULT_NOT_CURRENT_STATE", 1)
	for _, tc := range []struct {
		name         string
		commandReply *orchestration.GatewayResponse
		stored       string
		lost, denied bool
		wantBody     string
		wantError    bool
		wantReads    int
		repeat       bool
	}{
		{name: "direct receipt", wantBody: adoptionReceipt, repeat: true},
		{name: "direct receipt with ontology provenance", commandReply: &orchestration.GatewayResponse{Status: 200, Body: []byte(extended)}, stored: extended, wantBody: extended, repeat: true},
		{name: "direct historical receipt of published revision", commandReply: &orchestration.GatewayResponse{Status: 200, Body: []byte(published)}, denied: true, wantBody: published},
		{name: "acknowledgement", commandReply: ack, wantBody: adoptionReceipt, wantReads: 1, repeat: true},
		{name: "current reference and extended historical receipt", commandReply: &orchestration.GatewayResponse{Status: 200, Body: []byte(current)}, stored: extended, wantBody: extended, wantReads: 1, repeat: true},
		{name: "stored adoption of published revision", commandReply: ack, stored: current, wantBody: current, wantReads: 1, repeat: true},
		{name: "response lost", lost: true, wantBody: adoptionReceipt, wantReads: 1},
		{name: "receipt unavailable", commandReply: ack, stored: "null", wantError: true, wantBody: readRequired, wantReads: 1, repeat: true},
		{name: "other candidate", commandReply: ack, stored: strings.ReplaceAll(adoptionReceipt, "22222222", "99999999"), wantError: true, wantBody: readRequired, wantReads: 1},
		{name: "invalid provenance", commandReply: ack, stored: strings.Replace(extended, strings.Repeat("b", 64), "bad digest", 1), wantError: true, wantBody: readRequired, wantReads: 1},
		{name: "expired", commandReply: &orchestration.GatewayResponse{Status: 409, Problem: &orchestration.Problem{Code: "CANDIDATE_EXPIRED", Detail: "private upstream message"}}, wantError: true, wantBody: `{"code":"CANDIDATE_EXPIRED","retryable":false}`},
		{name: "read denied", lost: true, denied: true, wantError: true, wantBody: `{"code":"authorization denied","retryable":false,"authorizationPhase":"MCP_PRE_ADMISSION","reasonCode":"AUTHORIZATION_DENIED"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway := &adoptionGateway{t: t, commandReply: tc.commandReply, storedReceipt: tc.stored, loseResponse: tc.lost}
			service := &orchestration.Service{Auth: adoptionAuth{denyRead: tc.denied}, Admission: &adoptionAdmission{}, Gateway: gateway, FingerprintKey: make([]byte, 32)}
			in := orchestration.Invocation{
				Identity: orchestration.Identity{PrincipalID: "mapper", TenantID: "tenant", ServicePrincipalID: "mcp", ActorType: "HUMAN"},
				Owner:    "semantic", CapabilityID: "ouf.semantic.discovery.adopt", OperationClass: "COMMAND", GatewayBindingRef: "capability://ouf.semantic.discovery.adopt", ManifestChecksum: "fixture",
				IdempotencyKey: "adopt-01", Arguments: json.RawMessage(`{"commandKey":"adopt-01","requestId":"11111111-1111-4111-8111-111111111111","candidateId":"22222222-2222-4222-8222-222222222222"}`),
				Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 262144}, Timeout: time.Second, Window: time.Minute, RetryThreshold: 3,
			}
			check := func(wantReads int) {
				t.Helper()
				got := callDiscoveryAdoption(context.Background(), service, in)
				body := got.Content[0].(*mcp.TextContent).Text
				if got.IsError != tc.wantError || body != tc.wantBody {
					t.Fatal(body)
				}
				if gateway.commands != 1 || gateway.reads != wantReads {
					t.Fatalf("dispatched %d commands and %d reads, want 1 command and %d reads", gateway.commands, gateway.reads, wantReads)
				}
			}
			check(tc.wantReads)
			if tc.repeat {
				check(tc.wantReads + 1)
			}
		})
	}
}

type adoptionWindowAdmission struct {
	statusAdmission
	until time.Time
}

func (a *adoptionWindowAdmission) Reserve(_ context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	if !a.until.IsZero() {
		return orchestration.AdmissionDecision{}, orchestration.NewWindowBudgetError(orchestration.ErrToolSelectionStall, "TOOL_SELECTION_STALL", "EQUIVALENT_ATTEMPTS", a.until.Add(-7*time.Second), a.until)
	}
	return orchestration.AdmissionDecision{AttemptID: in.AttemptID}, nil
}

func TestSemanticAdoptionReturnsWindowAndResumesSameCommand(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	admission := &adoptionWindowAdmission{until: end}
	gateway := &adoptionGateway{t: t}
	service := &orchestration.Service{Auth: adoptionAuth{}, Admission: admission, Gateway: gateway, FingerprintKey: make([]byte, 32)}
	in := orchestration.Invocation{
		Identity:     orchestration.Identity{ServicePrincipalID: "mcp", PrincipalID: "mapper", TenantID: "tenant"},
		CapabilityID: "ouf.semantic.discovery.adopt", Owner: "semantic", OperationClass: "COMMAND", GatewayBindingRef: "capability://ouf.semantic.discovery.adopt", ManifestChecksum: "fixture",
		Arguments: json.RawMessage(`{"requestId":"11111111-1111-4111-8111-111111111111","candidateId":"22222222-2222-4222-8222-222222222222","commandKey":"adopt-01"}`), IdempotencyKey: "adopt-01",
		Window: time.Minute, Timeout: time.Second, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 262144},
	}
	wait := callDiscoveryAdoption(context.Background(), service, in)
	var timing struct {
		WindowEndsAt      time.Time
		RetryAfterSeconds int
		Dimension         string
	}
	if !wait.IsError || json.Unmarshal([]byte(wait.Content[0].(*mcp.TextContent).Text), &timing) != nil || !timing.WindowEndsAt.Equal(end) || timing.RetryAfterSeconds != 7 || timing.Dimension != "EQUIVALENT_ATTEMPTS" || gateway.commands != 0 || gateway.reads != 0 {
		t.Fatal("lost admission wait or dispatched", wait)
	}
	admission.until = time.Time{}
	got := callDiscoveryAdoption(context.Background(), service, in)
	if got.IsError || got.Content[0].(*mcp.TextContent).Text != adoptionReceipt || gateway.commands != 1 || gateway.reads != 0 {
		t.Fatal("original adoption did not resume", got)
	}
}

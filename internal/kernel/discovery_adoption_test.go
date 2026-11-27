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
		if err := json.Unmarshal(c.InputSchema, &got); err != nil {
			t.Fatal(err)
		}
		props := got["properties"].(map[string]any)
		if props["analysisId"].(map[string]any)["pattern"] != "^[a-f0-9]{64}$" {
			t.Fatal("invalid local analysis context")
		}
		delete(props, "analysisId")
		if !reflect.DeepEqual(want, got) ||
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
	outcome    orchestration.AttemptOutcome
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
			state := "FAILED"
			if a.outcome.Success {
				state = "SUCCEEDED"
			}
			return orchestration.AdmissionDecision{AttemptID: a.attemptID, Replay: true, AttemptState: state, Observation: a.outcome.Observation, AdoptionRefusalCode: a.outcome.AdoptionRefusalCode}, nil
		}
		a.commandKey, a.attemptID = in.IdempotencyKey, in.AttemptID
	}
	return orchestration.AdmissionDecision{AttemptID: in.AttemptID}, nil
}

func (a *adoptionAdmission) Reconcile(_ context.Context, id uuid.UUID, _ orchestration.Cost, outcome orchestration.AttemptOutcome) error {
	if id == a.attemptID {
		a.outcome = outcome
	}
	return nil
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

type adoptionAuth struct{ denyRead, allowPropose bool }

func (a adoptionAuth) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	allowed := (in.CapabilityID == "ouf.semantic.propose" && a.allowPropose) || in.CapabilityID == "ouf.semantic.discovery.adopt" || (in.CapabilityID == "ouf.semantic.proposal.read" && !a.denyRead)
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
		{name: "response lost", lost: true, wantError: true, wantBody: readRequired},
		{name: "receipt unavailable", commandReply: ack, stored: "null", wantError: true, wantBody: readRequired, wantReads: 1, repeat: true},
		{name: "other candidate", commandReply: ack, stored: strings.ReplaceAll(adoptionReceipt, "22222222", "99999999"), wantError: true, wantBody: readRequired, wantReads: 1},
		{name: "invalid provenance", commandReply: ack, stored: strings.Replace(extended, strings.Repeat("b", 64), "bad digest", 1), wantError: true, wantBody: readRequired, wantReads: 1},
		{name: "expired", commandReply: &orchestration.GatewayResponse{Status: 409, Problem: &orchestration.Problem{Code: "CANDIDATE_EXPIRED", Detail: "private upstream message"}}, wantError: true, wantBody: `{"code":"CANDIDATE_EXPIRED","retryable":false,"status":409}`},
		{name: "read denied", commandReply: ack, denied: true, wantError: true, wantBody: `{"code":"authorization denied","retryable":false,"authorizationPhase":"MCP_PRE_ADMISSION","reasonCode":"AUTHORIZATION_DENIED"}`},
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

func TestAdoptionFailureNextOverMCP(t *testing.T) {
	const request = "11111111-1111-4111-8111-111111111111"
	args := map[string]any{"requestId": request, "candidateId": "22222222-2222-4222-8222-222222222222",
		"commandKey": "adopt-01", "analysisId": strings.Repeat("a", 64)}
	for _, tc := range []struct {
		name, code string
		status     int
		canPropose bool
		choices    int
	}{
		{"missing publisher", "CANDIDATE_PUBLISHER_MISSING", 422, true, 3},
		{"proposal denied", "CANDIDATE_PUBLISHER_MISSING", 422, false, 2},
		{"invalid RDF", "CANDIDATE_RDF_INVALID", 400, true, 3},
		{"unknown owner code", "FUTURE_REJECTION", 422, true, 0},
		{"rate limited", "SEM_RATE_LIMITED", 429, true, 0},
		{"gateway failure", "CANDIDATE_PUBLISHER_MISSING", 503, true, 0},
		{"request timed out", "CANDIDATE_PUBLISHER_MISSING", 408, true, 0},
		{"lost response", "", 0, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway := &adoptionGateway{t: t, loseResponse: tc.status == 0, commandReply: &orchestration.GatewayResponse{
				Status: tc.status, Problem: &orchestration.Problem{Code: tc.code},
			}}
			if tc.status == 429 || tc.status == 503 || tc.status == 408 {
				gateway.commandReply.Problem.RetryAfter = "72"
			}
			service := &orchestration.Service{Auth: adoptionAuth{allowPropose: tc.canPropose}, Admission: &adoptionAdmission{},
				Gateway: gateway, FingerprintKey: make([]byte, 32)}
			session := revisionSession(t, service, "ouf.semantic.discovery ouf.semantic.propose")
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.discovery.adopt", Arguments: args})
			if err != nil || !result.IsError || gateway.commands != 1 || gateway.reads != 0 {
				t.Fatal("unexpected adoption or receipt dispatch", err, gateway.commands, gateway.reads)
			}
			if tc.choices > 0 {
				first := checkNext(t, nextContract(t), result)
				if first["action"] != "BLOCKED" || first["actor"] != "AGENT" || first["reasonCode"] != tc.code {
					t.Fatal("initial refusal", first)
				}
				// Discard the first reply. The client knows only the original command.
				result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.discovery.adopt", Arguments: args})
				if err != nil || !result.IsError || gateway.commands != 1 || gateway.reads != 0 {
					t.Fatal("replay dispatched instead of returning the refusal", result, err)
				}
			}
			next := checkNext(t, nextContract(t), result)
			if tc.status == 429 || tc.status == 503 || tc.status == 408 {
				due := service.Admission.(*adoptionAdmission).outcome.Observation.Due
				if time.Until(due) < 70*time.Second {
					t.Fatal("lost owner Retry-After", due)
				}
				if next["tool"] != "ouf.wait" {
					t.Fatal("initial reply skipped the wait", next)
				}
				// Lose the reply and open a fresh SDK session with the same store.
				restarted := revisionSession(t, service, "ouf.semantic.discovery ouf.semantic.propose")
				result, err = restarted.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.discovery.adopt", Arguments: args})
				if err != nil || !result.IsError || gateway.commands != 1 || gateway.reads != 0 {
					t.Fatal("premature receipt read or redispatch", err, gateway.commands, gateway.reads)
				}
				next = checkNext(t, nextContract(t), result)
				if next["tool"] != "ouf.wait" || service.Admission.(*adoptionAdmission).outcome.Observation.Due != due {
					t.Fatal("replay did not preserve the clock", next)
				}
				return
			}
			if tc.choices == 0 {
				want := map[string]any{"commandKey": args["commandKey"], "analysisId": args["analysisId"]}
				if next["action"] != "CALL_TOOL" || next["tool"] != "semantic.proposal.read" || !reflect.DeepEqual(next["arguments"], want) {
					t.Fatal("uncertain outcome lost receipt selector", next)
				}
				return
			}
			options, _ := next["alternatives"].([]any)
			if next["action"] != "BLOCKED" || next["actor"] != "AGENT" || next["reasonCode"] != tc.code || len(options) != tc.choices {
				t.Fatal("incorrect adoption choices", next)
			}
			for i, tool := range []string{"semantic.discovery.candidates", "source.onboarding.create", "semantic.propose"}[:tc.choices] {
				option := options[i].(map[string]any)
				input := option["arguments"].(map[string]any)
				if option["tool"] != tool || input["analysisId"] != args["analysisId"] {
					t.Fatal(option)
				}
				if i == 0 && (input["requestId"] != request || input["includeState"] != true) ||
					i == 1 && !strings.Contains(option["userMessage"].(string), "EXCLUDE") ||
					i > 0 && (input["commandKey"] == nil || input["commandKey"] == args["commandKey"]) {
					t.Fatal("choice lost its evidence or command identity", option)
				}
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

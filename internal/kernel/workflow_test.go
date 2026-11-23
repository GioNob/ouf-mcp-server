package kernel

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func nextContract(t *testing.T) *jsonschema.Resolved {
	t.Helper()
	raw, err := os.ReadFile("../../contracts/ouf-next-step-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func checkNext(t *testing.T, contract *jsonschema.Resolved, result *mcp.CallToolResult) nextstep.Object {
	t.Helper()
	body, err := json.Marshal(result.StructuredContent)
	var envelope struct{ Next nextstep.Object }
	if err != nil || json.Unmarshal(body, &envelope) != nil || envelope.Next == nil {
		t.Fatal("missing structured next")
	}
	if err := contract.Validate(envelope.Next); err != nil {
		t.Fatalf("invalid next: %v", err)
	}
	if result.IsError && !reflect.DeepEqual(textError(t, result)["next"], envelope.Next) {
		t.Fatal("text-only client received a different next")
	}
	return envelope.Next
}

// MCP-50P-002. Read only the textual MCP content, as the E2E client does.
func textError(t *testing.T, result *mcp.CallToolResult) nextstep.Object {
	t.Helper()
	for _, item := range result.Content {
		if text, ok := item.(*mcp.TextContent); ok {
			var body nextstep.Object
			if json.Unmarshal([]byte(text.Text), &body) == nil && body["next"] != nil {
				return body
			}
		}
	}
	t.Fatal("error has no JSON next in content[].text")
	return nil
}

func checkErrorText(t *testing.T, result *mcp.CallToolResult, wantJSON string) {
	t.Helper()
	checkNext(t, nextContract(t), result)
	body := textError(t, result)
	delete(body, "next")
	var want nextstep.Object
	if json.Unmarshal([]byte(wantJSON), &want) != nil || !reflect.DeepEqual(body, want) {
		t.Fatalf("error diagnostic = %v, want %s", body, wantJSON)
	}
}

// MCP-50P-005. Even the budget fallback must fit with both copies of next.
func TestBoundedErrorText(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	s := workflowServer{manifest: snapshot}
	for _, tool := range []string{"source.onboarding.create", "ouf.wait"} {
		t.Run(tool, func(t *testing.T) {
			reg := registeredTool{capability: manifest.Capability{ToolName: tool, NextPolicy: "onboarding"}, limit: 1024}
			result := s.boundedResult(workflowError(nextstep.Object{"padding": strings.Repeat("x", reg.limit)}), reg)
			if !result.IsError || checkNext(t, nextContract(t), result)["reasonCode"] != "TOOL_RESULT_LIMIT" {
				t.Fatal("oversize error lost the bounded continuation")
			}
			wire, err := json.Marshal(result)
			if err != nil || len(wire) > reg.limit {
				t.Fatal("error text exceeded the serialized result limit")
			}
			if tool == "ouf.wait" && textError(t, result)["changed"] != false {
				t.Fatal("wait fallback lost observation status")
			}
		})
	}
}

func checkOwnerAndNext(t *testing.T, result *mcp.CallToolResult, owner map[string]any) {
	t.Helper()
	checkNext(t, nextContract(t), result)
	structured, ok := result.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("unexpected structured receipt type %T", result.StructuredContent)
	}
	body := maps.Clone(structured)
	delete(body, "next")
	if !reflect.DeepEqual(body, owner) {
		t.Fatalf("structured receipt differs from owner: got %v, want %v", body, owner)
	}
}

// Exercise tools/call, including SDK schema failures, on every advertised
// capability. Picker mode also registers the optional status read.
func TestEveryAdvertisedToolReturnsGovernedValidation(t *testing.T) {
	h, err := NewGovernedHTTPHandlerWithFilePicker(nil, &orchestration.Service{}, "https://fixture.invalid/trusted-human/managed-files/")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-OUF-Gateway-Verified", "true")
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "workflow", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	contract := nextContract(t)
	var visited []string
	for _, tool := range list.Tools {
		visited = append(visited, tool.Name)
		t.Run(tool.Name, func(t *testing.T) {
			r, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool.Name, Arguments: map[string]any{"unexpected": "private-marker"}})
			if err != nil || !r.IsError {
				t.Fatalf("validation failed to return a tool error: %v", err)
			}
			next := checkNext(t, contract, r)
			if next["reasonCode"] != "INVALID_TOOL_ARGUMENTS" {
				t.Fatal(next)
			}
			raw, _ := json.Marshal(r)
			if strings.Contains(string(raw), "private-marker") || strings.Contains(string(raw), "oneOf did not validate") {
				t.Fatal("raw input or SDK diagnostic escaped")
			}
		})
	}
	for _, c := range snapshot.ToolEligible() {
		if !slices.Contains(visited, c.ToolName) {
			t.Fatalf("missing %s", c.ToolName)
		}
	}
}

type nextGateway struct {
	reads     atomic.Int32
	runReads  atomic.Int32
	links     atomic.Int32
	deadline  string
	linkEnd   string
	linkError bool
}

func (g *nextGateway) Execute(_ context.Context, req orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	const canonicalType = "https://w3id.org/italia/onto/POI/PointOfInterest"
	var args nextstep.Object
	_ = json.Unmarshal(req.Arguments, &args)
	obs := nextstep.Object{"sourceId": "teatri", "onboardingVersionId": "11111111-1111-4111-8111-111111111111"}
	switch req.CapabilityID {
	case "ouf.managed-source.onboarding.review.prepare":
		obs["state"], obs["lockVersion"], obs["challengeStatus"] = "IN_REVIEW", 2, "CREATED"
		obs["reviewUrl"] = "https://fixture.invalid/review/1"
		obs["expiresAt"] = time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
		obs["observationVersion"] = "sha256:" + strings.Repeat("a", 64)
	case "ouf.managed-source.onboarding.read":
		if args["ifObservationVersion"] == nil {
			obs["workflowState"], obs["allowedActions"], obs["lockVersion"] = "DRAFT", []string{"PREPARE_REVIEW"}, 1
			obs["reasonCode"], obs["executionProposalRequired"] = "DRAFT_READY", false
			break
		}
		if args["ifObservationVersion"] != "sha256:"+strings.Repeat("a", 64) || req.Identity.PrincipalID != "user-a" {
			return orchestration.GatewayResponse{Status: 403, Problem: &orchestration.Problem{Status: 403}}, nil
		}
		g.reads.Add(1)
		obs["state"], obs["lockVersion"] = "ACTIVE", 3
	case "ouf.ingestion.status":
		run := nextstep.Object{"source_ref": "teatri", "runId": "22222222-2222-4222-8222-222222222222", "expectedVersion": 8, "runState": "COMPLETED", "recoveryAction": "TERMINAL", "udpHandoffConfirmed": true, "typeCode": "FILE", "canonicalType": canonicalType}
		if source, ok := args["sourceId"].(string); ok {
			run["source_ref"] = source
		}
		if g.runReads.Add(1) == 1 {
			run["runState"], run["recoveryAction"], run["expectedVersion"] = "PAUSED", "HUMAN_RESUME_OR_ABORT", 7
			run["decisionDeadline"] = g.deadline
		}
		obs = nextstep.Object{"partial": false, "items": []nextstep.Object{run}}
	case recoveryLinkID:
		g.links.Add(1)
		if g.linkError || req.Identity.Delegation == "" || req.Owner != "onboarding" || !reflect.DeepEqual(args, nextstep.Object{
			"sourceId": "teatri", "runId": "22222222-2222-4222-8222-222222222222", "expectedVersion": float64(7),
		}) {
			return orchestration.GatewayResponse{Status: 403, Problem: &orchestration.Problem{Status: 403}}, nil
		}
		obs = nextstep.Object{"reviewUrl": "https://fixture.invalid/run-recovery", "expiresAt": g.linkEnd}
	case "urban.object.search":
		if args["type"] != canonicalType {
			return orchestration.GatewayResponse{Status: 400, Problem: &orchestration.Problem{Status: 400}}, nil
		}
		obs = nextstep.Object{"outcome": "COMPLETE", "items": []nextstep.Object{{"type": canonicalType}}}
	default:
		return orchestration.GatewayResponse{Status: 400, Problem: &orchestration.Problem{Status: 400}}, nil
	}
	body, _ := json.Marshal(obs)
	return orchestration.GatewayResponse{Status: 200, Body: body}, nil
}

// PET-mcp-058/070/168. All calls after the initial read come from next.
// This covers the authorized tail of the pipeline, including the real wait
// handler and delegated Gateway dispatch, without performing a THS mutation.
func TestNextOnlyFromReviewToObjectSearch(t *testing.T) {
	t.Setenv("OUF_WAIT_KEY_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	owner := &nextGateway{deadline: time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format("2006-01-02T15:04:05.000Z"), linkEnd: time.Now().UTC().Add(time.Minute).Format(time.RFC3339)}
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{}, Gateway: owner, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, service, "ouf.managed-source.onboarding.review.prepare ouf.managed-source.onboarding.read ingestion.operations.read urban.object.search "+recoveryLinkID)
	contract := nextContract(t)
	tool := "source.onboarding.read"
	args := nextstep.Object{"sourceId": "teatri", "onboardingVersionId": "11111111-1111-4111-8111-111111111111"}
	for _, expected := range []string{"CALL_TOOL", "WAIT_FOR_HUMAN", "CALL_TOOL", "WAIT_FOR_HUMAN", "CALL_TOOL", "DONE"} {
		res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		next := checkNext(t, contract, res)
		if res.IsError || next["action"] != expected {
			t.Fatalf("%s: %v", tool, next)
		}
		if tool == "source.onboarding.read" {
			key, _ := next["arguments"].(nextstep.Object)["commandKey"].(string)
			id, err := uuid.Parse(key)
			if err != nil || id.Version() != 4 {
				t.Fatal("missing generated command key", next)
			}
			again, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
			if err != nil {
				t.Fatal(err)
			}
			if retry := checkNext(t, contract, again); !reflect.DeepEqual(retry, next) {
				t.Fatal("repeating the same observation changed the command", retry)
			}
		}
		if tool == "ouf.ingestion.status" {
			human := next["human"].(nextstep.Object)
			if next["deadline"] != owner.deadline || human["expiresAt"] != owner.linkEnd || human["reviewUrl"] != "https://fixture.invalid/run-recovery" || human["decisionKind"] != "RESUME_OR_ABORT" {
				t.Fatal("incorrect run decision", next)
			}
		}
		tool, _ = next["tool"].(string)
		args, _ = next["arguments"].(nextstep.Object)
	}
	if owner.reads.Load() != 1 || owner.runReads.Load() != 2 || owner.links.Load() != 1 {
		t.Fatal("wait did not make its authorized conditional read")
	}
}

func TestNextOnlyFromSemanticProposalToHumanDecision(t *testing.T) {
	const challenge = "11111111-1111-4111-8111-111111111111"
	const revision = "22222222-2222-4222-8222-222222222222"
	const reviewURL = "https://onboarding.invalid/review/1"
	const semanticURL = "https://semantic.invalid/unused"
	version := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name                                      string
		atPrepare, ownerURL, noDeadline, denyLink bool
	}{
		{name: "approval with Onboarding URL only"},
		{name: "current challenge during prepare", atPrepare: true, ownerURL: true, noDeadline: true},
		{name: "Onboarding denies link despite Semantic URL", ownerURL: true, denyLink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
			linkEnd := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
			var links, reads atomic.Int32
			var pendingKey atomic.Value
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req orchestration.GatewayRequest
				var args nextstep.Object
				if r.Method != http.MethodPost || r.Header.Get("X-OUF-Delegation") != "human-proof" ||
					json.NewDecoder(r.Body).Decode(&req) != nil || json.Unmarshal(req.Arguments, &args) != nil {
					t.Error("invalid delegated dispatch")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				obs := nextstep.Object{"revisionId": revision, "rowVersion": 4}
				switch r.URL.Path {
				case "/internal/capabilities/v1/execute/semantic/create":
					obs["state"], obs["allowedActions"] = "DRAFT", []string{"PREPARE_REVIEW"}
				case "/internal/capabilities/v1/execute/semantic/validate":
					if !tc.atPrepare {
						obs["state"], obs["allowedActions"] = "VALIDATED", []string{"REQUEST_APPROVAL"}
						obs["currentValidationRunId"] = revision
						break
					}
					fallthrough
				case "/internal/capabilities/v1/execute/semantic/review":
					pendingKey.Store(req.IdempotencyKey)
					current := nextstep.Object{"challengeId": challenge}
					if !tc.noDeadline {
						current["expiresAt"] = deadline
					}
					if tc.ownerURL {
						current["reviewUrl"], obs["reviewUrl"] = semanticURL, semanticURL
					}
					obs["currentChallenge"], obs["version"] = current, version
					obs["state"], obs["allowedActions"] = "UNDER_REVIEW", []string{"WAIT_HUMAN_DECISION"}
				case "/internal/capabilities/v1/execute/" + semanticLinkID:
					links.Add(1)
					if req.CapabilityID != semanticLinkID || req.Owner != "onboarding" || req.OperationClass != "READ" ||
						req.Identity.ActorType != "HUMAN" || !reflect.DeepEqual(args, nextstep.Object{"challengeId": challenge}) {
						t.Error("incorrect semantic link request", args)
					}
					if tc.denyLink {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					obs = nextstep.Object{"reviewUrl": reviewURL, "expiresAt": linkEnd}
				case "/internal/capabilities/v1/execute/semantic/read":
					reads.Add(1)
					if req.CapabilityID != "ouf.semantic.proposal.read" || args["expectedVersion"] != version || args["commandKey"] != pendingKey.Load() {
						t.Error("wait did not re-read the same proposal conditionally", args)
					}
					obs["state"], obs["version"], obs["changed"] = "APPROVED", strings.Repeat("b", 64), true
				default:
					t.Error("unexpected dispatch", r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(obs)
			}))
			defer gateway.Close()
			endpoint, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
			service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{}, FingerprintKey: make([]byte, 32),
				Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture")}}
			session := revisionSession(t, service, "ouf.semantic.propose ouf.semantic.review.prepare ouf.semantic.approval.request ouf.semantic.proposal.read "+semanticLinkID)
			contract := nextContract(t)
			tool := "semantic.propose"
			args := nextstep.Object{"commandKey": "draft-1", "semanticId": "ouf:test:Place", "artifactType": "CLASS", "namespace": "ouf", "localName": "Place",
				"ownerRef": "owner", "authorityRef": "authority", "semanticVersion": "1.0.0", "labels": nextstep.Object{"it": "Luogo"}, "definition": nextstep.Object{}}
			steps := []string{"semantic.review.prepare"}
			if !tc.atPrepare {
				steps = append(steps, "semantic.approval.request")
			}
			steps = append(steps, "ouf.wait")
			if !tc.denyLink {
				steps = append(steps, "semantic.proposal.read")
			}
			for _, target := range steps {
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil {
					t.Fatal(err)
				}
				next := checkNext(t, contract, result)
				if result.IsError {
					t.Fatal(tool, next)
				}
				if target == "ouf.wait" {
					if tc.denyLink {
						checkPermissionAlternatives(t, next, semanticLinkID, "OWNER_AUTHORIZATION_DENIED")
						break
					}
					end := deadline
					if tc.noDeadline {
						end = linkEnd
					}
					human := nextstep.Object{"decisionKind": "APPROVE_OR_REJECT", "reviewUrl": reviewURL, "expiresAt": linkEnd}
					if next["action"] != "WAIT_FOR_HUMAN" || !reflect.DeepEqual(next["human"], human) || next["deadline"] != end {
						t.Fatal("incorrect semantic wait", next)
					}
				} else if tool == "ouf.wait" {
					body := result.StructuredContent.(nextstep.Object)
					if body["changed"] != true || body["observation"].(nextstep.Object)["state"] != "APPROVED" || next["action"] != "POLL" {
						t.Fatal("lost human decision", body)
					}
				} else if next["action"] != "CALL_TOOL" {
					t.Fatal(next)
				}
				if next["tool"] != target {
					t.Fatal("unexpected continuation", next)
				}
				tool, args = next["tool"].(string), next["arguments"].(nextstep.Object)
			}
			if links.Load() != 1 || (!tc.denyLink && reads.Load() != 1) || (tc.denyLink && reads.Load() != 0) {
				t.Fatal("unexpected Onboarding or Semantic read count", links.Load(), reads.Load())
			}
		})
	}
}

func checkPermissionAlternatives(t *testing.T, next nextstep.Object, capability, reason string) {
	t.Helper()
	if next["action"] != "BLOCKED" || next["actor"] != "HUMAN_THS" || next["reasonCode"] != reason ||
		next["human"] != nil || !strings.Contains(next["userMessage"].(string), capability) {
		t.Fatal("incorrect permission diagnosis", next)
	}
	alts := next["alternatives"].([]any)
	if len(alts) != 2 {
		t.Fatal("missing permission alternatives", next)
	}
	for i, view := range []string{"CAPABILITIES", "ROLES"} {
		alt := alts[i].(nextstep.Object)
		if alt["action"] != "CALL_TOOL" || alt["tool"] != "authorization.permissions.read" || !reflect.DeepEqual(alt["arguments"], nextstep.Object{"view": view}) {
			t.Fatal("incorrect permission lookup", alt)
		}
	}
	if !strings.Contains(alts[1].(nextstep.Object)["userMessage"].(string), "authorization.permissions.propose") {
		t.Fatal("missing proposal preparation", alts)
	}
}

func TestRecoveryLinkOwnerDenialOffersPermissionReads(t *testing.T) {
	owner := &nextGateway{deadline: time.Now().UTC().Add(time.Hour).Format(time.RFC3339), linkError: true}
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.CapabilityID == recoveryLinkID && (r.Method != http.MethodPost || r.URL.Path != "/internal/capabilities/v1/execute/"+recoveryLinkID || r.Header.Get("X-OUF-Delegation") != "human-proof") {
			t.Error("incorrect recovery link dispatch")
		}
		req.Identity.Delegation = r.Header.Get("X-OUF-Delegation")
		response, _ := owner.Execute(r.Context(), req, 0)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.Status)
		_, _ = w.Write(response.Body)
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{}, FingerprintKey: make([]byte, 32),
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture")}}
	session := revisionSession(t, service, "ingestion.operations.read "+recoveryLinkID)
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "ouf.ingestion.status", Arguments: nextstep.Object{"sourceId": "teatri"}})
	if err != nil {
		t.Fatal(err)
	}
	next := checkNext(t, nextContract(t), res)
	checkPermissionAlternatives(t, next, recoveryLinkID, "OWNER_AUTHORIZATION_DENIED")
	if owner.links.Load() != 1 {
		t.Fatal("missing owner link attempt")
	}
}

type denyRecoveryLink struct{}

func (denyRecoveryLink) Authorize(ctx context.Context, req orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if req.CapabilityID == recoveryLinkID {
		return orchestration.AuthorizationDecision{DecisionCode: "NO_APPLICABLE_GRANT"}, nil
	}
	return (analysisSDKAuth{}).Authorize(ctx, req)
}

func TestRecoveryLinkPreAdmissionDenial(t *testing.T) {
	owner := &nextGateway{deadline: time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}
	admission := &statusAdmission{}
	service := &orchestration.Service{RequireDelegation: true, Auth: denyRecoveryLink{}, Admission: admission, Gateway: owner, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, service, "ingestion.operations.read")
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "ouf.ingestion.status", Arguments: nextstep.Object{"sourceId": "edicole-trieste"}})
	if err != nil {
		t.Fatal(err)
	}
	checkPermissionAlternatives(t, checkNext(t, nextContract(t), res), recoveryLinkID, "NO_APPLICABLE_GRANT")
	if res.IsError || owner.runReads.Load() != 1 || owner.links.Load() != 0 || admission.calls.Load() != 1 {
		t.Fatal("link denial crossed admission or changed the successful status result")
	}
}

func TestTHSLinkFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result orchestration.Result
		err    error
		want   nextstep.LinkResult
	}{
		{"owner throttle", orchestration.Result{Problem: &orchestration.Problem{Status: 429, RetryAfter: "23"}}, nil, nextstep.LinkResult{Status: nextstep.LinkTransient, RetryAfter: "23"}},
		{"owner outage", orchestration.Result{Problem: &orchestration.Problem{Status: 503}}, nil, nextstep.LinkResult{Status: nextstep.LinkTransient}},
		{"owner permanent", orchestration.Result{Problem: &orchestration.Problem{Status: 503}, Body: []byte(`{"retryable":false}`)}, nil, nextstep.LinkResult{}},
		{"invalid selector", orchestration.Result{Problem: &orchestration.Problem{Status: 422}}, nil, nextstep.LinkResult{}},
		{"network timeout", orchestration.Result{}, context.DeadlineExceeded, nextstep.LinkResult{Status: nextstep.LinkTransient}},
		{"unknown failure", orchestration.Result{}, errors.New("private diagnostic"), nextstep.LinkResult{}},
		{"insecure URL", orchestration.Result{Body: []byte(`{"reviewUrl":"http://fixture.invalid","expiresAt":"2099-01-01T00:00:00Z"}`)}, nil, nextstep.LinkResult{}},
		{"stale link", orchestration.Result{Body: []byte(`{"reviewUrl":"https://fixture.invalid","expiresAt":"2001-01-01T00:00:00Z"}`)}, nil, nextstep.LinkResult{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyLink(semanticLinkID, tc.result, tc.err); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// PET-mcp-058/097/099. Use the cursor supplied for the second column, then
// execute the generated plan against the owner's immutable profile metadata.
func TestNextOnlyFromPreviewCursorToAnalysis(t *testing.T) {
	asset, profile := uuid.NewString(), uuid.NewString()
	const cursor = "opaque-column-values"
	values := make([]string, 1600)
	for i := range values {
		values[i] = fmt.Sprintf("%04d", i) + strings.Repeat("界", 124)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req orchestration.GatewayRequest
		var args nextstep.Object
		if r.URL.Path != "/managed.file/preview" || r.Header.Get("X-OUF-Delegation") == "" ||
			json.NewDecoder(r.Body).Decode(&req) != nil || json.Unmarshal(req.Arguments, &args) != nil ||
			args["assetId"] != asset || args["profileId"] != profile {
			t.Error("invalid delegated preview")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		obs := nextstep.Object{"assetId": asset, "profileId": profile, "format": "CSV", "view": "COLUMNS", "workflowState": "PROFILE_COLUMNS",
			"profileVersion": 1, "columnCount": 2, "schemaHash": "sha256:" + strings.Repeat("a", 64), "hasMore": false, "offset": 0}
		column := nextstep.Object{"name": "categoria", "distinctValuesComplete": true}
		switch calls.Add(1) {
		case 1:
			column["distinctValuesComplete"], column["distinctValuesCursor"] = false, cursor
			column["distinctValues"] = values
			obs["columns"] = []any{nextstep.Object{"name": "id"}, column}
		case 2:
			if len(args) != 4 || args["view"] != "COLUMNS" || args["distinctValuesCursor"] != cursor {
				t.Error("cursor was not replayed exactly", args)
			}
			obs["offset"], obs["columns"] = 1, []any{column}
		case 3:
			if args["offset"] != float64(0) || args["limit"] != float64(128) {
				t.Error("plan did not verify the immutable profile", args)
			}
			obs["columns"] = []any{nextstep.Object{"name": "id"}, column}
		default:
			t.Error("unexpected preview dispatch")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(obs)
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL)
	store := &analysisSDKStore{}
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: store, FingerprintKey: make([]byte, 32),
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture")}}
	session := revisionSession(t, service, "ouf.managed-source.preview")
	contract := nextContract(t)
	name := "source.file.preview"
	args := nextstep.Object{"view": "COLUMNS", "assetId": asset, "profileId": profile}
	for _, target := range []string{"source.file.preview", "source.analysis.plan", "source.analysis.step"} {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		next := checkNext(t, contract, result)
		if result.IsError || next["action"] != "CALL_TOOL" || next["tool"] != target {
			t.Fatal(name, next)
		}
		if target == "source.file.preview" {
			wire, err := json.Marshal(result)
			columns := result.StructuredContent.(nextstep.Object)["columns"].([]any)
			got := columns[1].(nextstep.Object)["distinctValues"].([]any)
			if err != nil || len(wire) > 1<<20 || len(got) != len(values) {
				t.Fatal("large column page lost values or exceeded the result budget")
			}
			for i, value := range values {
				if got[i] != value {
					t.Fatalf("changed distinct value at %d", i)
				}
			}
		}
		name, args = target, next["arguments"].(nextstep.Object)
	}
	if calls.Load() != 3 || store.plans != 1 {
		t.Fatal("missing governed plan or owner read")
	}
	args["taskId"] = strings.Repeat("0", 64)
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil || !result.IsError || checkNext(t, contract, result)["reasonCode"] != "IDEMPOTENCY_KEY_REUSE" {
		t.Fatal("accepted changed work under the same step key")
	}
}

package kernel

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	return envelope.Next
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
			break
		}
		if args["ifObservationVersion"] != "sha256:"+strings.Repeat("a", 64) || req.Identity.PrincipalID != "user-a" {
			return orchestration.GatewayResponse{Status: 403, Problem: &orchestration.Problem{Status: 403}}, nil
		}
		g.reads.Add(1)
		obs["state"], obs["lockVersion"] = "ACTIVE", 3
	case "ouf.ingestion.status":
		run := nextstep.Object{"source_ref": "teatri", "runId": "22222222-2222-4222-8222-222222222222", "expectedVersion": 8, "runState": "COMPLETED", "recoveryAction": "TERMINAL", "udpHandoffConfirmed": true, "typeCode": "TEATRO"}
		if g.runReads.Add(1) == 1 {
			run["runState"], run["recoveryAction"], run["expectedVersion"] = "PAUSED", "HUMAN_RESUME_OR_ABORT", 7
			run["decisionDeadline"], run["reviewUrl"] = g.deadline, "https://unused.invalid/legacy"
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
		obs = nextstep.Object{"outcome": "COMPLETE", "items": []nextstep.Object{{"type": "TEATRO"}}}
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

func TestRecoveryLinkDenialOffersStatusRead(t *testing.T) {
	owner := &nextGateway{deadline: time.Now().UTC().Add(time.Hour).Format(time.RFC3339), linkError: true}
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.CapabilityID == recoveryLinkID && (r.Method != http.MethodPost || r.URL.Path != "/ths-links/ingestion-run-recovery" || r.Header.Get("X-OUF-Delegation") != "human-proof") {
			t.Error("incorrect recovery link dispatch")
		}
		req.Identity.Delegation = r.Header.Get("X-OUF-Delegation")
		response, _ := owner.Execute(r.Context(), req, 0)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.Status)
		_, _ = w.Write(response.Body)
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL)
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{}, FingerprintKey: make([]byte, 32),
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture")}}
	session := revisionSession(t, service, "ingestion.operations.read "+recoveryLinkID)
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "ouf.ingestion.status", Arguments: nextstep.Object{"sourceId": "teatri"}})
	if err != nil {
		t.Fatal(err)
	}
	next := checkNext(t, nextContract(t), res)
	if next["action"] != "BLOCKED" || next["reasonCode"] != "OWNER_REVIEW_LINK_REQUIRED" || owner.links.Load() != 1 {
		t.Fatal(next)
	}
	alt := next["alternatives"].([]any)[0].(nextstep.Object)
	if alt["tool"] != "ouf.ingestion.status" || !reflect.DeepEqual(alt["arguments"], nextstep.Object{"sourceId": "teatri"}) || next["human"] != nil {
		t.Fatal(next)
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
			if args["offset"] != float64(0) || args["limit"] != float64(1) {
				t.Error("plan did not verify the immutable profile", args)
			}
			obs["columns"] = []any{nextstep.Object{"name": "id"}}
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

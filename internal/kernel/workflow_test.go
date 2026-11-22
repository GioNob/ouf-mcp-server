package kernel

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
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
	reads atomic.Int32
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
		if args["ifObservationVersion"] != "sha256:"+strings.Repeat("a", 64) || req.Identity.PrincipalID != "user-a" {
			return orchestration.GatewayResponse{Status: 403, Problem: &orchestration.Problem{Status: 403}}, nil
		}
		g.reads.Add(1)
		obs["state"], obs["lockVersion"] = "ACTIVE", 3
	case "ouf.ingestion.status":
		obs = nextstep.Object{"partial": false, "items": []nextstep.Object{{"source_ref": "teatri", "run_state": "SUCCEEDED", "runState": "COMPLETED", "recoveryAction": "TERMINAL", "udpHandoffConfirmed": true, "typeCode": "TEATRO"}}}
	case "urban.object.search":
		obs = nextstep.Object{"outcome": "COMPLETE", "items": []nextstep.Object{{"type": "TEATRO"}}}
	default:
		return orchestration.GatewayResponse{Status: 400, Problem: &orchestration.Problem{Status: 400}}, nil
	}
	body, _ := json.Marshal(obs)
	return orchestration.GatewayResponse{Status: 200, Body: body}, nil
}

// PET-mcp-058/070/168. All calls after prepare come exclusively from next.
// This covers the authorized tail of the pipeline, including the real wait
// handler and delegated Gateway dispatch, without performing a THS mutation.
func TestNextOnlyFromReviewToObjectSearch(t *testing.T) {
	t.Setenv("OUF_WAIT_KEY_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	owner := &nextGateway{}
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{}, Gateway: owner, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, service, "ouf.managed-source.onboarding.review.prepare ouf.managed-source.onboarding.read ingestion.operations.read urban.object.search")
	contract := nextContract(t)
	tool := "source.onboarding.review.prepare"
	args := nextstep.Object{"sourceId": "teatri", "onboardingVersionId": "11111111-1111-4111-8111-111111111111", "expectedVersion": 1, "commandKey": "review-fixture-1"}
	for _, expected := range []string{"WAIT_FOR_HUMAN", "CALL_TOOL", "CALL_TOOL", "DONE"} {
		res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		next := checkNext(t, contract, res)
		if res.IsError || next["action"] != expected {
			t.Fatalf("%s: %v", tool, next)
		}
		tool, _ = next["tool"].(string)
		args, _ = next["arguments"].(nextstep.Object)
	}
	if owner.reads.Load() != 1 {
		t.Fatal("wait did not make its authorized conditional read")
	}
}

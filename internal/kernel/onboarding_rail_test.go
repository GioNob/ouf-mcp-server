package kernel

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCP-50P-004. Exercise owner problem projection through the SDK and consume
// only text at the client. No command is executed to interpret a refusal.
func TestOnboardingOwnerRail(t *testing.T) {
	const create = "source.onboarding.create"
	const prepare = "source.onboarding.review.prepare"
	const read = "source.onboarding.read"
	const version = "11111111-1111-4111-8111-111111111111"
	analysis := strings.Repeat("a", 64)
	raw, err := os.ReadFile("testdata/value-types/source.onboarding.create.json")
	var fixture struct{ Arguments railObject }
	if err != nil || json.Unmarshal(raw, &fixture) != nil {
		t.Fatal("missing create fixture", err)
	}
	createArgs := fixture.Arguments
	createArgs["commandKey"], createArgs["analysisId"] = uuid.NewString(), analysis
	validCreate := maps.Clone(createArgs)
	validCreate["semanticRefs"] = []string{"urn:example:place@1.0.0"}
	readArgs := railObject{"sourceId": createArgs["sourceId"], "onboardingVersionId": version, "analysisId": analysis}
	prepareArgs := maps.Clone(readArgs)
	prepareArgs["expectedVersion"], prepareArgs["commandKey"] = float64(0), "previous-prepare"
	var proposal railObject
	if err := json.Unmarshal([]byte(reviewExecutionJSON), &proposal); err != nil {
		t.Fatal(err)
	}
	mismatchArgs := maps.Clone(prepareArgs)
	mismatchArgs["executionProposal"] = proposal
	draft := railObject{"reasonCode": "EXECUTION_PROPOSAL_REQUIRED", "workflowState": "DRAFT", "sourceId": readArgs["sourceId"],
		"onboardingVersionId": version, "lockVersion": 0, "allowedActions": []string{"PREPARE_REVIEW"}, "executionProposalRequired": true,
		"missingSections": []string{"udp.resolution", "limits"}}
	for _, tc := range []struct {
		tool, reason, target string
		args, owner          railObject
		status               int
	}{
		{create, "EXECUTION_PROPOSAL_REQUIRED", prepare, validCreate, draft, 200},
		{read, "EXECUTION_PROPOSAL_REQUIRED", prepare, readArgs, draft, 200},
		{prepare, "ONB_EXECUTION_PROPOSAL_REQUIRED", prepare, prepareArgs, railObject{"missingSections": []string{"udp.resolution", "limits"}}, 422},
		{create, "ONB_SEMANTIC_REF_FORM_INVALID", create, createArgs, railObject{"index": float64(0)}, 422},
		{prepare, "ONB_SEMANTIC_REF_VERSION_MISMATCH", read, mismatchArgs, railObject{"semanticRef": "urn:example:place@1.0.0"}, 422},
		{create, "ONB_READER_UNSUPPORTED", read, validCreate, railObject{"fieldName": "shape"}, 422},
		{prepare, "ONB_REVIEW_INTERNAL_ERROR", "", prepareArgs, railObject{}, 500},
	} {
		t.Run(tc.tool+"/"+tc.reason, func(t *testing.T) {
			owner := maps.Clone(tc.owner)
			if tc.status >= 400 {
				owner["code"], owner["detail"], owner["retryable"] = tc.reason, "private-owner-diagnostic", true
			}
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req orchestration.GatewayRequest
				if json.NewDecoder(r.Body).Decode(&req) != nil || strings.Contains(string(req.Arguments), "analysisId") {
					t.Error("invalid owner dispatch")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(owner)
			}))
			defer gateway.Close()
			u, _ := url.Parse(gateway.URL)
			service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{},
				Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture")}, FingerprintKey: make([]byte, 32)}
			session := revisionSession(t, service, "ouf.managed-source.onboarding.create ouf.managed-source.onboarding.review.prepare ouf.managed-source.onboarding.read")
			res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: tc.args})
			if err != nil || res.IsError != (tc.status >= 400) {
				t.Fatalf("unexpected tool result %v, %v", res, err)
			}
			next := checkNext(t, nextContract(t), res)
			body := res.StructuredContent.(railObject)
			if res.IsError {
				body = textError(t, res)
				next = body["next"].(railObject)
				payload, _ := json.Marshal(res)
				if strings.Contains(string(payload), "private-owner-diagnostic") {
					t.Fatal("owner detail escaped")
				}
			}
			for k, v := range tc.owner {
				want, _ := json.Marshal(v)
				got, _ := json.Marshal(body[k])
				if string(got) != string(want) {
					t.Fatalf("owner field %s: %s, want %s", k, got, want)
				}
			}
			if next["reasonCode"] != tc.reason {
				t.Fatal(next)
			}
			if tc.target == "" {
				if next["action"] != "BLOCKED" || next["actor"] != "OPERATOR" || next["alternatives"].([]any)[0].(railObject)["tool"] != nil {
					t.Fatal("internal failure offered a retry", next)
				}
				return
			}
			step := next
			if tc.reason != "ONB_SEMANTIC_REF_VERSION_MISMATCH" {
				if next["action"] != "BLOCKED" || next["actor"] != "AGENT" {
					t.Fatal("expected agent decision", next)
				}
				step = next["alternatives"].([]any)[0].(railObject)
			}
			if step["action"] != "CALL_TOOL" || step["tool"] != tc.target {
				t.Fatal("incorrect continuation", step)
			}
			args := step["arguments"].(railObject)
			switch tc.target {
			case prepare, create:
				key, _ := args["commandKey"].(string)
				if uuid.Validate(key) != nil || key == tc.args["commandKey"] {
					t.Fatal("expected a fresh command key")
				}
				if tc.target == prepare {
					if !strings.Contains(next["userMessage"].(string), "udp.resolution, limits") {
						t.Fatal("missing sections absent from message", next)
					}
					if !reflect.DeepEqual(args, withKey(prepareArgs, key)) {
						t.Fatal("incorrect prepare arguments", args)
					}
				} else if !reflect.DeepEqual(args, withKey(createArgs, key)) || !strings.Contains(next["userMessage"].(string), "semanticId@semanticVersion") {
					t.Fatal("create correction lost original request", step)
				}
			case read:
				want := readArgs
				if tc.tool == create {
					want = railObject{"sourceId": createArgs["sourceId"], "view": "LATEST", "analysisId": analysis}
				}
				if !reflect.DeepEqual(args, want) {
					t.Fatal("incorrect read arguments", args)
				}
			}
		})
	}
}

func withKey(args railObject, key string) railObject {
	out := maps.Clone(args)
	out["commandKey"] = key
	return out
}

package kernel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Official SDK -> scoped policy -> admission -> TLS Gateway transport.
// The owner response is a fixture, not a real HUMAN approval or source run.
func TestSourceReviewUsesExactVersionScopeAndStableCommand(t *testing.T) {
	const version = "11111111-1111-4111-8111-111111111111"
	const executionJSON = `{"semanticBindings":[{"semanticId":"urn:test:theatres","semanticVersion":"1.0.0","revisionId":"11111111-1111-4111-8111-111111111111","publicationSetId":"22222222-2222-4222-8222-222222222222"}],"semanticPublicationSetRef":"22222222-2222-4222-8222-222222222222","udp":{"resolution":{"strategyId":"GOVERNED_IDENTITY","strategyVersion":"1","policyRef":"urn:test:identity","governedIdentity":{"ref":"urn:test:identity","version":"1","tenantId":"tenant-a","canonicalClass":"https://schema.org/PerformingArtsTheater","sourceId":"theatres","maxCandidates":10,"allowAutoNew":true,"signals":[{"id":"name","semanticRef":"https://schema.org/name@22222222-2222-4222-8222-222222222222","comparator":"TEXT_V1","excludesOnDisagreement":false,"uniqueWithinScope":false,"assertionRef":"urn:test:human-review"}],"sufficientRules":[{"id":"name-rule","signalIds":["name"],"assertionRef":"urn:test:human-review"}]}},"materialization":{"policyRef":"urn:test:materialization","properties":[{"sourceField":"https://schema.org/name","propertyIri":"https://schema.org/name","datatype":"http://www.w3.org/2001/XMLSchema#string","accessLabel":"RESTRICTED","authorityOrder":["theatres"]}],"bitemporalProperties":[],"checkpointInterval":1}},"limits":{"maxRows":100,"maxColumns":20,"maxCellChars":1000}}`
	for _, tc := range []struct {
		tool, capability, operation, mode, datatype string
		latest                                      bool
	}{
		{"source.onboarding.review.prepare", "ouf.managed-source.onboarding.review.prepare", "COMMAND", "review-prepare", "string", false},
		{"source.onboarding.review.prepare", "ouf.managed-source.onboarding.review.prepare", "COMMAND", "review-prepare", "decimal", false},
		{"source.onboarding.review.prepare", "ouf.managed-source.onboarding.review.prepare", "COMMAND", "review-prepare", "date", false},
		{"source.onboarding.read", "ouf.managed-source.onboarding.read", "READ", "review-read", "string", false},
		{"source.onboarding.read", "ouf.managed-source.onboarding.read", "READ", "review-read", "string", true},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			var execution map[string]any
			if err := json.Unmarshal([]byte(executionJSON), &execution); err != nil {
				t.Fatal(err)
			}
			udp := execution["udp"].(map[string]any)
			materialization := udp["materialization"].(map[string]any)
			property := materialization["properties"].([]any)[0].(map[string]any)
			property["datatype"] = "http://www.w3.org/2001/XMLSchema#" + tc.datatype
			now := time.Now().UTC()
			attrs := map[string]string{"onboardingVersionId": version}
			if tc.latest {
				attrs = nil
			}
			cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{
				BundleID: "source-review", BundleVersion: 1, ActivatedAt: now,
				Bundle: authorization.PolicyBundle{BundleID: "source-review", Version: 1, PublishedAt: now,
					Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: tc.capability, Operation: tc.operation, RequiredScope: tc.capability, AllowedActors: []string{"HUMAN"}}},
					Grants: []authorization.Grant{{GrantID: "source-review", CapabilityID: tc.capability, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
						Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "source", ResourceID: "theatres", ResourceAttributes: attrs}}},
				},
			}})
			if err := cache.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/capabilities/v1/execute/managed.file/"+tc.mode || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "human-proof" {
					t.Error("governed source review transport changed")
				}
				var in orchestration.GatewayRequest
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Fatal(err)
				}
				assertGatewayDispatchSchema(t, "GATEWAY_SOURCE_REVIEW_PAIRWISE_ROOT", "mcp-gateway-onboarding-"+tc.mode+"-dispatch-v1.json", in)
				if in.CapabilityID != tc.capability || in.Owner != "onboarding" || in.OperationClass != tc.operation || in.MaxResultBytes != 65536 {
					t.Error("owner or result bound changed")
				}
				if tc.operation == "COMMAND" && in.IdempotencyKey != "review-theatres-1" {
					t.Error("stable command key lost")
				}
				var args map[string]any
				if err := json.Unmarshal(in.Arguments, &args); err != nil {
					t.Fatal(err)
				}
				if tc.operation == "COMMAND" && !reflect.DeepEqual(args["executionProposal"], execution) {
					t.Error("typed execution proposal changed")
				}
				if args["sourceId"] != "theatres" || (!tc.latest && args["onboardingVersionId"] != version) || (tc.latest && (args["view"] != "LATEST" || args["onboardingVersionId"] != nil)) {
					t.Error("source/version binding lost")
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"state":"IN_REVIEW","reviewUrl":"https://ths.invalid/review/fixture"}`)
			}))
			defer gateway.Close()
			endpoint, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
			admission := &statusAdmission{}
			service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: admission, Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
			handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
			if err != nil {
				t.Fatal(err)
			}
			for _, scope := range []string{tc.capability, "mcp.connect"} {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					for k, v := range map[string]string{"X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "human-proof", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
						r.Header.Set(k, v)
					}
					handler.ServeHTTP(w, r)
				}))
				client := mcp.NewClient(&mcp.Implementation{Name: "source-review", Version: "1"}, nil)
				session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, source := range []string{"theatres", "other-source"} {
					args := map[string]any{"sourceId": source, "onboardingVersionId": version}
					if tc.latest {
						delete(args, "onboardingVersionId")
						args["view"] = "LATEST"
					}
					if tc.operation == "COMMAND" {
						args["expectedVersion"] = 0
						args["commandKey"] = "review-theatres-1"
						args["executionProposal"] = execution
					}
					before, admitted := calls.Load(), admission.calls.Load()
					res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: args})
					if err != nil {
						t.Fatal(err)
					}
					allowed := scope == tc.capability && source == "theatres"
					if allowed {
						if res.IsError || calls.Load() != before+1 {
							t.Fatalf("authorized review failed: %+v", res)
						}
					} else if !res.IsError || calls.Load() != before || admission.calls.Load() != admitted {
						t.Fatal("denied source review reached admission or Gateway")
					}
					args["approvedBy"] = "user-a"
					before = calls.Load()
					res, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: args})
					if err == nil && !res.IsError {
						t.Fatal("tool accepted caller approval")
					}
					if calls.Load() != before {
						t.Fatal("undeclared authority dispatched")
					}
				}
				session.Close()
				server.Close()
			}
		})
	}
}

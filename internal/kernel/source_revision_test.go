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
func TestSourceRevisionUsesExactParentScopeAndStableCommand(t *testing.T) {
	const version = "11111111-1111-4111-8111-111111111111"
	const executionJSON = `[{"propertyIri":"urn:name","sourceField":"urn:name","reconciliation":{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1","cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"SOURCE_INTERVAL"}}]`
	var execution []any
	if err := json.Unmarshal([]byte(executionJSON), &execution); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ tool, capability, operation, mode string }{
		{"source.onboarding.revision.prepare", "ouf.managed-source.onboarding.revision.prepare", "COMMAND", "prepare"},
		{"source.onboarding.revision.read", "ouf.managed-source.onboarding.revision.read", "READ", "read"},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			now := time.Now().UTC()
			cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{
				BundleID: "source-review", BundleVersion: 1, ActivatedAt: now,
				Bundle: authorization.PolicyBundle{BundleID: "source-review", Version: 1, PublishedAt: now,
					Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: tc.capability, Operation: tc.operation, RequiredScope: tc.capability, AllowedActors: []string{"HUMAN"}}},
					Grants: []authorization.Grant{{GrantID: "source-review", CapabilityID: tc.capability, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
						Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "source", ResourceID: "theatres", ResourceAttributes: map[string]string{"onboardingVersionId": version}}}},
				},
			}})
			if err := cache.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/capabilities/v1/execute/source.revision/"+tc.mode || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "human-proof" {
					t.Error("governed source review transport changed")
				}
				var in orchestration.GatewayRequest
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Fatal(err)
				}
				assertGatewayDispatchSchema(t, "GATEWAY_SOURCE_REVISION_PAIRWISE_ROOT", "mcp-gateway-source-revision-"+tc.mode+"-dispatch-v1.json", in)
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
				if tc.operation == "COMMAND" && !reflect.DeepEqual(args["changes"], execution) {
					t.Error("typed execution proposal changed")
				}
				if args["sourceId"] != "theatres" || (args["onboardingVersionId"] != version && args["parentVersionId"] != version) {
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
					if tc.operation == "READ" && source == "theatres" {
						delete(args, "onboardingVersionId")
						args["parentVersionId"] = version
						args["commandKey"] = "review-theatres-1"
					}
					if tc.operation == "COMMAND" {
						delete(args, "onboardingVersionId")
						args["parentVersionId"] = version
						args["configurationHash"] = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
						args["publicationEffect"] = "NEW_PUBLICATION_MAY_START_ACQUISITION"
						args["expectedVersion"] = 0
						args["commandKey"] = "review-theatres-1"
						args["changes"] = execution
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

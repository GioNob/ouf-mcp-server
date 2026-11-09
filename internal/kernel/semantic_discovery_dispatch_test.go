package kernel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestPublishedDiscoveryMCPDispatchUsesOwnerBindingAndScope(t *testing.T) {
	for _, operation := range []string{"request", "status", "candidates"} {
		t.Run(operation, func(t *testing.T) {
			snapshot, err := manifest.Load()
			if err != nil {
				t.Fatal(err)
			}
			var capability manifest.Capability
			for _, c := range snapshot.Capabilities {
				if c.ToolName == "semantic.discovery."+operation {
					capability = c
				}
			}
			if capability.CapabilityID == "" {
				t.Fatal("discovery descriptor missing")
			}
			now := time.Now().UTC()
			cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "discovery", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "discovery", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capability.CapabilityID, Operation: capability.OperationClass, RequiredScope: "ouf.semantic.discovery", AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "discovery", CapabilityID: capability.CapabilityID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
			if err := cache.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/capabilities/v1/execute/semantic/discovery/"+operation || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
					t.Error("incorrect discovery transport")
				}
				var in orchestration.GatewayRequest
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error(err)
				}
				assertGatewayDispatchSchema(t, "GATEWAY_SEMANTIC_DISCOVERY_PAIRWISE_ROOT", "mcp-gateway-semantic-discovery-"+operation+"-dispatch-v1.json", in)
				if in.CapabilityID != capability.CapabilityID || in.OperationClass != capability.OperationClass || in.Owner != "semantic" || in.Identity.Delegation != "" {
					t.Error("discovery envelope binding changed")
				}
				var args map[string]any
				if json.Unmarshal(in.Arguments, &args) != nil {
					t.Error("arguments invalid")
				}
				if operation == "request" && (args["intent"] != "teatro" || args["idempotencyKey"] != "discovery-test-0001") {
					t.Error("semantic retry key or intent changed")
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"state":"PENDING"}`)
			}))
			defer gateway.Close()
			u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
			service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
			handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
			if err != nil {
				t.Fatal(err)
			}
			for _, scope := range []string{"ouf.semantic.discovery", "mcp.connect"} {
				transport := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
						r.Header.Set(k, v)
					}
					handler.ServeHTTP(w, r)
				}))
				client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
				session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: transport.URL}, nil)
				if err != nil {
					t.Fatal(err)
				}
				tools, err := session.ListTools(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, tool := range tools.Tools {
					if tool.Name == capability.ToolName {
						found = true
						if tool.Annotations == nil || tool.Annotations.ReadOnlyHint != (operation != "request") {
							t.Fatal("incorrect discovery tool annotation")
						}
					}
				}
				if !found {
					t.Fatal("published discovery tool missing")
				}
				args := map[string]any{"requestId": "11111111-1111-4111-8111-111111111111"}
				if operation == "request" {
					args = map[string]any{"requestedArtifactType": "CLASS", "intent": "teatro", "preferredLanguages": []string{"it", "en"}, "idempotencyKey": "discovery-test-0001"}
				}
				before := calls.Load()
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: capability.ToolName, Arguments: args})
				if err != nil {
					t.Fatal(err)
				}
				if scope == "ouf.semantic.discovery" {
					if result.IsError || calls.Load() != before+1 {
						t.Fatalf("candidate dispatch failed: %+v", result)
					}
				} else if !result.IsError || calls.Load() != before {
					t.Fatal("unauthorized discovery reached Gateway")
				}
				session.Close()
				transport.Close()
			}
		})
	}
}


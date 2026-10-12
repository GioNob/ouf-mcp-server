package kernel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ingestionBundleSource struct {
	bundle authorization.ActivePolicyBundle
}

func (s *ingestionBundleSource) FetchActive(context.Context) (authorization.ActivePolicyBundle, error) {
	return s.bundle, nil
}

// Real SDK -> kernel -> current local policy -> admission -> TLS HTTP adapter.
// Gateway/owner response is synthetic: this proves no deployed CSV or lineage.
func TestIngestionReadsUseOwnerCapabilityAndSourceScope(t *testing.T) {
	now := time.Now().UTC()
	source := &ingestionBundleSource{authorization.ActivePolicyBundle{
		BundleID: "ingestion-test", BundleVersion: 1, ActivatedAt: now,
		Bundle: authorization.PolicyBundle{BundleID: "ingestion-test", Version: 1, PublishedAt: now,
			Capabilities: []authorization.CapabilityDescriptor{{
				CapabilityID: "ingestion.operations.read", Operation: "READ", RequiredScope: "ingestion.operations.read", AllowedActors: []string{"HUMAN"},
			}},
			Grants: []authorization.Grant{{
				GrantID: "source-a-read", CapabilityID: "ingestion.operations.read", TenantID: "tenant-a", SubjectID: "user-a",
				ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
				Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "operational",
					ResourceAttributes:  map[string]string{"module": "INGESTION", "sourceRef": "source-a"},
					AllowedDetailLevels: []string{"TENANT_OPERATIONAL"}},
			}},
		},
	}}
	cache := authorization.NewCache(source)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var rejectOwner atomic.Bool
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-workload" || r.Header.Get("X-OUF-Delegation") != "fixture-delegation" {
			t.Error("workload/delegation lost")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if (in.CapabilityID != "ouf.ingestion.status" && in.CapabilityID != "ouf.ingestion.history") ||
			in.GatewayBindingRef != "capability://"+in.CapabilityID ||
			in.AuthorizationDecisionRef != "ingestion-test:1:ingestion.operations.read" {
			t.Error("transport/owner binding changed")
		}
		if strings.Contains(string(in.Arguments), "ingestion.operations.read") {
			t.Error("authorization injected into client arguments")
		}
		if rejectOwner.Load() {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"OWNER_DENIED"}`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"partial":false,"authorization":"AUTHORIZED"}`))
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL)
	admission := &statusAdmission{}
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: admission,
		Gateway:        &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture-workload")},
		FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"ouf.ingestion.status", "ouf.ingestion.history"} {
		for _, tc := range []struct {
			name, subject, tenant, scope, filter string
			allowed                              bool
		}{
			{"authorized", "user-a", "tenant-a", "mcp.connect ingestion.operations.read", "source-a", true},
			{"wrong-source", "user-a", "tenant-a", "mcp.connect ingestion.operations.read", "source-b", false},
			{"collection-with-only-source-grant", "user-a", "tenant-a", "mcp.connect ingestion.operations.read", "", false},
			{"wrong-subject", "user-b", "tenant-a", "mcp.connect ingestion.operations.read", "source-a", false},
			{"wrong-tenant", "user-a", "tenant-b", "mcp.connect ingestion.operations.read", "source-a", false},
			{"missing-scope", "user-a", "tenant-a", "mcp.connect", "source-a", false},
		} {
			t.Run(tool+"/"+tc.name, func(t *testing.T) {
				before, admitted := calls.Load(), admission.calls.Load()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					for key, value := range map[string]string{
						"X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "fixture-delegation", "X-OUF-Service-Principal": "ouf-chatgpt",
						"X-OUF-Principal-ID": tc.subject, "X-OUF-Tenant-ID": tc.tenant, "X-OUF-Actor-Type": "HUMAN",
						"X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "ouf-api-gateway", "X-OUF-Granted-Scopes": tc.scope,
					} {
						r.Header.Set(key, value)
					}
					handler.ServeHTTP(w, r)
				}))
				defer server.Close()
				client := mcp.NewClient(&mcp.Implementation{Name: "ingestion-read-test", Version: "1"}, nil)
				session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				args := map[string]any{"limit": 10}
				if tc.filter != "" {
					args["sourceId"] = tc.filter
				}
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil || result.IsError == tc.allowed {
					t.Fatalf("unexpected result: %+v %v", result, err)
				}
				want := before
				if tc.allowed {
					want++
				}
				if calls.Load() != want || admission.calls.Load() != admitted+(want-before) {
					t.Fatal("authorization/admission boundary violated")
				}
				if !tc.allowed {
					return
				}
				// Client cannot choose a different permission, tenant or resource context.
				forged := map[string]any{"sourceId": "source-a", "limit": 10, "authorizationCapabilityId": "ouf.system.status"}
				invalid, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: forged})
				if err != nil || !invalid.IsError || calls.Load() != want {
					t.Fatal("client authority override accepted")
				}
				// Owner refusal remains an error even when the MCP preflight allowed.
				rejectOwner.Store(true)
				denied, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
				rejectOwner.Store(false)
				if err != nil || !denied.IsError || calls.Load() != want+1 {
					t.Fatal("owner refusal erased")
				}
			})
		}
	}
	// A fresh published policy can revoke the same scope; no new invocation grants it.
	source.bundle.BundleVersion = 2
	source.bundle.Bundle.Version = 2
	source.bundle.ActivatedAt = now.Add(time.Second)
	source.bundle.Bundle.Grants = nil
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	decision, err := cache.Authorize(context.Background(), orchestration.AuthorizationRequest{
		Identity:     orchestration.Identity{PrincipalID: "user-a", TenantID: "tenant-a", ActorType: "HUMAN", AuthenticationContextRef: "1", Issuer: "issuer", Audience: "gateway", Scopes: []string{"ingestion.operations.read"}},
		CapabilityID: "ingestion.operations.read", OperationClass: "READ",
		Resource: orchestration.ResourceContext{ResourceType: "operational", TenantID: "tenant-a", Attributes: map[string]string{"module": "INGESTION", "sourceRef": "source-a", "detailLevel": "TENANT_OPERATIONAL"}},
	})
	if err != nil || decision.Allowed {
		t.Fatal("revocation ignored")
	}
}

func TestIngestionManifestAndResourceBindingFailClosed(t *testing.T) {
	c := manifest.Capability{CapabilityID: "ouf.ingestion.status", Owner: "ingestion", OperationClass: "READ", RequiredAuthorization: "ingestion.operations.read"}
	permission, resource, err := ingestionReadAuthorization(c, []byte(`{"sourceId":"source-a","limit":10}`), "tenant-a")
	if err != nil || permission != "ingestion.operations.read" || resource.ResourceType != "operational" ||
		resource.TenantID != "tenant-a" || resource.Attributes["sourceRef"] != "source-a" {
		t.Fatal("binding lost")
	}
	for _, bad := range []manifest.Capability{
		{CapabilityID: c.CapabilityID, Owner: "udp", OperationClass: "READ", RequiredAuthorization: c.RequiredAuthorization},
		{CapabilityID: c.CapabilityID, Owner: c.Owner, OperationClass: "COMMAND", RequiredAuthorization: c.RequiredAuthorization},
		{CapabilityID: c.CapabilityID, Owner: c.Owner, OperationClass: c.OperationClass, RequiredAuthorization: "ouf.system.status"},
	} {
		if _, _, err := ingestionReadAuthorization(bad, []byte("{}"), "tenant-a"); err == nil {
			t.Fatal("manifest drift accepted")
		}
	}
	other := manifest.Capability{CapabilityID: "ouf.system.status"}
	permission, resource, err = ingestionReadAuthorization(other, []byte("{}"), "tenant-a")
	if err != nil || permission != "" || resource.ResourceType != "" {
		t.Fatal("unrelated capability changed")
	}
}

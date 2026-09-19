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
	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type statusBundle struct {
	value authorization.ActivePolicyBundle
}

func (s statusBundle) FetchActive(context.Context) (authorization.ActivePolicyBundle, error) {
	return s.value, nil
}

type statusAdmission struct{ calls atomic.Int32 }

func (s *statusAdmission) Reserve(_ context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	s.calls.Add(1)
	return orchestration.AdmissionDecision{AttemptID: in.AttemptID}, nil
}
func (*statusAdmission) Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error {
	return nil
}
func (*statusAdmission) Reconcile(context.Context, uuid.UUID, orchestration.Cost, orchestration.AttemptOutcome) error {
	return nil
}

type statusOwner struct{ calls atomic.Int32 }

func (s *statusOwner) SystemStatus(_ context.Context, in orchestration.Identity) ([]byte, error) {
	s.calls.Add(1)
	return []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"partial":true,"securityIncidentCount":1,"incidents":["private-ref"]}`), nil
}

// The Gateway is a transport fixture here, not a replacement for the separate
// Gateway pairwise/security gates. The real SDK, policy cache, orchestration,
// HTTP adapter and owner projection run together.
func TestPublicStatusToolWithConstrainedPolicy(t *testing.T) {
	now := time.Now().UTC()
	bundle := authorization.ActivePolicyBundle{BundleID: "public-test", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{
		BundleID: "public-test", Version: 1, PublishedAt: now,
		Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: "ouf.system.status", Operation: "READ", RequiredScope: "operations.status.read", AllowedActors: []string{"HUMAN"}}},
		Grants:       []authorization.Grant{{GrantID: "status", CapabilityID: "ouf.system.status", TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour), Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "capability", AllowedDetailLevels: []string{"PUBLIC_OPERATIONAL"}}}},
	}}
	cache := authorization.NewCache(statusBundle{bundle})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner := &statusOwner{}
	ownerAPI := operational.NewOwnerAPI(owner)
	var gatewayCalls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-workload" {
			t.Error("missing workload bearer")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if in.AuthorizationDecisionRef != "public-test:1:ouf.system.status" || in.GatewayBindingRef != "capability://ouf.system.status" {
			t.Error("decision/binding lost")
		}
		request := httptest.NewRequest(http.MethodPost, "/api/internal/v1/mcp/operations/status", strings.NewReader(string(in.Arguments)))
		request.Header.Set("X-OUF-Gateway-Verified", "true")
		request.Header.Set("X-OUF-Principal-ID", in.Identity.PrincipalID)
		request.Header.Set("X-OUF-Tenant-ID", in.Identity.TenantID)
		request.Header.Set("X-OUF-Authorization-Decision-Ref", in.AuthorizationDecisionRef)
		ownerAPI.ServeHTTP(w, request)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL)
	admission := &statusAdmission{}
	service := &orchestration.Service{Auth: cache, Admission: admission, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture-workload")}, FingerprintKey: make([]byte, 32)}
	h, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, subject, tenant, scope string
		allowed                      bool
	}{
		{"authorized", "user-a", "tenant-a", "mcp.connect operations.status.read", true},
		{"wrong-user", "user-b", "tenant-a", "mcp.connect operations.status.read", false},
		{"wrong-tenant", "user-a", "tenant-b", "mcp.connect operations.status.read", false},
		{"missing-scope", "user-a", "tenant-a", "mcp.connect", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := gatewayCalls.Load()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range map[string]string{"X-Correlation-ID": "public-status-fixture", "Idempotency-Key": tc.name, "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": tc.subject, "X-OUF-Tenant-ID": tc.tenant, "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "ouf-api-gateway", "X-OUF-Granted-Scopes": tc.scope} {
					r.Header.Set(k, v)
				}
				h.ServeHTTP(w, r)
			}))
			defer server.Close()
			client := mcp.NewClient(&mcp.Implementation{Name: "public-status-test", Version: "1"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "ouf.system.status", Arguments: map[string]any{}})
			if err != nil || result.IsError == tc.allowed {
				t.Fatalf("unexpected tool result: %+v %v", result, err)
			}
			if tc.allowed {
				text := result.Content[0].(*mcp.TextContent).Text
				var fields map[string]any
				if json.Unmarshal([]byte(text), &fields) != nil || len(fields) != 6 || fields["status"] != "DEGRADED" || fields["visibilityClass"] != "PUBLIC_OPERATIONAL" || strings.Contains(text, "private-ref") {
					t.Fatal(text)
				}
				if gatewayCalls.Load() != before+1 {
					t.Fatal("Gateway not traversed")
				}
			} else if gatewayCalls.Load() != before {
				t.Fatal("denied invocation reached Gateway")
			}
		})
	}
	if owner.calls.Load() != 1 || admission.calls.Load() != 1 {
		t.Fatal("denied request reached admission or owner")
	}
}

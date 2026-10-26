package kernel

import (
	"context"
	"encoding/json"
	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

// Separate client sessions recover the same durable owner result.
// The TLS owner response is synthetic; owner PostgreSQL coverage is separate.
func TestOwnedAssetRecoveryAcrossIndependentSDKClients(t *testing.T) {
	const capability = "ouf.managed-source.preview"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{
		BundleID: "recovery", BundleVersion: 1, ActivatedAt: now,
		Bundle: authorization.PolicyBundle{BundleID: "recovery", Version: 1, PublishedAt: now,
			Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capability, Operation: "READ", RequiredScope: capability, AllowedActors: []string{"HUMAN"}}},
			Grants:       []authorization.Grant{{GrantID: "recovery", CapabilityID: capability, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}},
		},
	}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/managed.file/preview" || r.Header.Get("X-OUF-Delegation") != "human-proof" {
			t.Error("unscoped recovery transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			return
		}
		assertGatewayDispatchSchema(t, "GATEWAY_CSV_RECOVERY_PAIRWISE_ROOT", "mcp-gateway-managed-file-dispatch-v1.json", in)
		var args map[string]any
		if err := json.Unmarshal(in.Arguments, &args); err != nil {
			t.Error(err)
			return
		}
		if in.CapabilityID != capability || in.Owner != "onboarding" || in.OperationClass != "READ" || len(args) != 2 || args["view"] != "ASSETS" || args["limit"] != float64(2) {
			t.Error("recovery arguments or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"items":[{"assetId":"11111111-1111-4111-8111-111111111111","status":"PROFILED"}],"hasMore":false}`)
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	admission := &statusAdmission{}
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: admission, Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"user-a", "user-a", "other-user"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "human-proof", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": subject, "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": capability} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "independent-recovery-client", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		before, admitted := calls.Load(), admission.calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.preview", Arguments: map[string]any{"view": "ASSETS", "limit": 2}})
		if err != nil {
			t.Fatal(err)
		}
		if subject == "user-a" {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("owned recovery failed: %+v", result)
			}
			// Assert the owner's real response shape, not just successful dispatch.
			var body struct {
				Items []struct {
					AssetID string `json:"assetId"`
					Status  string `json:"status"`
				} `json:"items"`
				HasMore bool `json:"hasMore"`
			}
			decoded := false
			for _, content := range result.Content {
				if text, ok := content.(*mcp.TextContent); ok {
					if err := json.Unmarshal([]byte(text.Text), &body); err != nil {
						t.Fatal(err)
					}
					decoded = true
				}
			}
			if !decoded || len(body.Items) != 1 || body.Items[0].AssetID != "11111111-1111-4111-8111-111111111111" || body.Items[0].Status != "PROFILED" || body.HasMore {
				t.Fatal("owner items projection was lost or changed")
			}
		} else if !result.IsError || calls.Load() != before || admission.calls.Load() != admitted {
			t.Fatal("other subject reached admission or Gateway")
		}
		for _, bad := range []map[string]any{
			{"view": "ASSETS", "limit": 51}, {"view": "ASSETS", "limit": "2"}, {"view": "ASSETS", "tenantId": "tenant-a"}, {"view": "ASSETS", "assetId": "11111111-1111-4111-8111-111111111111"}, {"view": "ASSETS", "afterAssetId": "not-a-uuid"},
		} {
			before = calls.Load()
			result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.preview", Arguments: bad})
			if err == nil && !result.IsError {
				t.Fatalf("invalid recovery arguments accepted: %+v", bad)
			}
			if calls.Load() != before {
				t.Fatal("invalid recovery arguments dispatched")
			}
		}
		session.Close()
		server.Close()
	}
	if calls.Load() != 2 {
		t.Fatalf("expected exactly two independent reads: %d", calls.Load())
	}
}

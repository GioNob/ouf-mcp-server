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

// SDK -> real cache -> admission -> HTTPS adapter; owner remains synthetic here.
func TestSemanticProposalToolsUseOwnerScopesStableIntentAndClosedGateway(t *testing.T) {
	cases := []struct {
		name, cap, permission, mode, operation string
		args                                   map[string]any
	}{
		{"semantic.propose", "ouf.semantic.propose", "ouf.semantic.propose", "create", "COMMAND", map[string]any{"commandKey": "draft-1", "semanticId": "ouf:test:Place", "artifactType": "CLASS", "namespace": "ouf", "localName": "Place", "ownerRef": "owner", "authorityRef": "authority", "semanticVersion": "1.0.0", "labels": map[string]any{"it": "Luogo"}, "definition": map[string]any{}}},
		{"semantic.review.prepare", "ouf.semantic.review.prepare", "ouf.semantic.review.prepare", "validate", "COMMAND", map[string]any{"commandKey": "validate-1", "revisionId": "11111111-1111-4111-8111-111111111111", "expectedRowVersion": 0}},
		{"semantic.approval.request", "ouf.semantic.approval.request", "ouf.semantic.approval.request", "review", "COMMAND", map[string]any{"commandKey": "review-1", "revisionId": "11111111-1111-4111-8111-111111111111", "expectedRowVersion": 0, "validationRunId": "22222222-2222-4222-8222-222222222222"}},
		{"semantic.proposal.read", "ouf.semantic.proposal.read", "ouf.semantic.propose", "read", "READ", map[string]any{"commandKey": "draft-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			source := statusBundle{authorization.ActivePolicyBundle{BundleID: "semantic", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "semantic", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: tc.permission, Operation: tc.operation, RequiredScope: tc.permission, AllowedActors: []string{"AI_AGENT"}}}, Grants: []authorization.Grant{{GrantID: "semantic-grant", CapabilityID: tc.permission, TenantID: "tenant-a", SubjectID: "agent", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour), Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "semantic.proposalcommand", ResourceAttributes: map[string]string{"module": "SEMANTIC"}}}}}}}
			cache := authorization.NewCache(source)
			if err := cache.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/capabilities/v1/execute/semantic/"+tc.mode || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
					t.Error("governed transport changed")
				}
				var in orchestration.GatewayRequest
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				if in.CapabilityID != tc.cap || in.Owner != "semantic" || in.OperationClass != tc.operation || in.AuthorizationDecisionRef != "semantic:1:"+tc.permission {
					t.Error("scope or owner binding changed")
				}
				if tc.operation == "COMMAND" && in.IdempotencyKey != tc.args["commandKey"] {
					t.Error("stable command key lost")
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"status":"OWNER_RECEIPT_ONLY"}`)
			}))
			defer gateway.Close()
			u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
			service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
			handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
			if err != nil {
				t.Fatal(err)
			}
			for _, scope := range []string{tc.permission, "mcp.connect"} {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "agent", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "AI_AGENT", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
						r.Header.Set(k, v)
					}
					handler.ServeHTTP(w, r)
				}))
				client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
				session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
				if err != nil {
					t.Fatal(err)
				}
				tools, err := session.ListTools(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, tool := range tools.Tools {
					if tool.Name == "semantic.approve" || tool.Name == "semantic.publish" {
						t.Fatal("human operation exposed")
					}
					if tool.Name == tc.name && (tool.Annotations == nil || tool.Annotations.ReadOnlyHint != (tc.operation == "READ")) {
						t.Fatal("incorrect effect hint")
					}
				}
				before := calls.Load()
				res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.name, Arguments: tc.args})
				if err != nil {
					t.Fatal(err)
				}
				if scope == tc.permission {
					if res.IsError || calls.Load() != before+1 {
						t.Fatalf("command failed: %+v", res)
					}
				} else if !res.IsError || calls.Load() != before {
					t.Fatal("denied command dispatched")
				}
				bad := make(map[string]any)
				for k, v := range tc.args {
					bad[k] = v
				}
				bad["approvedBy"] = "agent"
				before = calls.Load()
				res, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.name, Arguments: bad})
				if err == nil && !res.IsError {
					t.Fatal("undeclared authority accepted")
				}
				if calls.Load() != before {
					t.Fatal("invalid input dispatched")
				}
				session.Close()
				server.Close()
			}
		})
	}
}

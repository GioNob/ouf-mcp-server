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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPermissionReadSelectorsRoundTripThroughRealMCPTransport(t *testing.T) {
	const capID = "authorization.permissions.read"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "READ", RequiredScope: capID, AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/authorization/read" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		json.Unmarshal(in.Arguments, &args)
		if (args["subjectId"] != "user-a" && args["externalRoleRef"] != "reviewers" && args["view"] != "ROLES" && args["view"] != "CAPABILITIES") || in.OperationClass != "READ" || in.Owner != "authorization" || in.Identity.Delegation != "" {
			t.Error("proposal arguments or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		if args["view"] == "CAPABILITIES" {
			io.WriteString(w, `[{"capabilityId":"ingestion.run.read","operation":"READ","requiredScope":"ingestion.run.read","allowedActors":["HUMAN"]}]`)
		} else {
			io.WriteString(w, `{"grants":[],"nextAfter":""}`)
		}
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{capID, "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "permissions-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == capID && (tool.Annotations == nil || !tool.Annotations.ReadOnlyHint) {
				t.Fatal("read mislabelled")
			}
		}
		for _, args := range []map[string]any{{"subjectId": "user-a"}, {"view": "GRANTS", "subjectId": "user-a", "limit": 200}, {"externalRoleRef": "reviewers"}, {"view": "ROLES"}, {"view": "CAPABILITIES"}} {
			before := calls.Load()
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: capID, Arguments: args})
			if err != nil {
				t.Fatal(err)
			}
			if scope == capID {
				if result.IsError || calls.Load() != before+1 {
					t.Fatalf("read selector failed: %+v", result)
				}
				if args["view"] == "CAPABILITIES" {
					raw, _ := json.Marshal(result)
					if !strings.Contains(string(raw), "ingestion.run.read") {
						t.Fatal("capability descriptor response lost in MCP transport")
					}
				}
			} else if !result.IsError || calls.Load() != before {
				t.Fatal("unauthorized read reached Gateway")
			}
		}
		session.Close()
		server.Close()
	}
}

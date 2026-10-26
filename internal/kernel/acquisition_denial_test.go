package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Native SDK + real local evaluator + TLS adapter. All account/policy/owner
// evidence is synthetic. No production token, UUID, consent or call is used.
func TestAcquisitionDenialDiagnosisSDK(t *testing.T) {
	for _, tool := range []string{"ouf.ingestion.acquisition.read", "urban.acquisition.read"} {
		for _, scenario := range []struct {
			name, reason string
			version      int64
		}{
			{"scope-missing-with-valid-grant", "SCOPE_MISSING", 50},
			{"no-grant-with-valid-scope", "NO_APPLICABLE_GRANT", 50},
			{"expired-grant", "NO_APPLICABLE_GRANT", 50},
			{"explicit-deny", "EXPLICIT_DENY", 50},
			{"old-snapshot-no-descriptor", "CAPABILITY_NOT_DECLARED", 48},
			{"allowed", "", 50},
			{"owner-refuses-after-local-allow", "", 50},
		} {
			t.Run(tool+"/"+scenario.name, func(t *testing.T) {
				now := time.Now().UTC()
				permission, module := "ingestion.lineage.read", "INGESTION"
				if tool == "urban.acquisition.read" {
					permission, module = "lineage.object.read", "UDP"
				}
				capability := permission
				if scenario.name == "old-snapshot-no-descriptor" {
					capability = "unrelated.read"
				}
				grant := authorization.Grant{GrantID: "ci-read", CapabilityID: capability,
					TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour),
					Constraints: &authorization.GrantConstraints{Effect: "ALLOW",
						ResourceAttributes: map[string]string{"module": module, "sourceRef": "source-a", "jobRef": "11111111-1111-4111-8111-111111111111", "detailLevel": "TENANT_OPERATIONAL"},
						AllowedDataLabels: []string{"ANONYMOUS", "OPEN"}, AllowedDetailLevels: []string{"TENANT_OPERATIONAL"}}}
				if scenario.name == "expired-grant" {
					grant.ValidUntil = now.Add(-time.Second)
				}
				if scenario.name == "explicit-deny" {
					grant.Constraints.Effect = "DENY"
				}
				grants := []authorization.Grant{grant}
				if scenario.name == "no-grant-with-valid-scope" {
					grants = nil
				}
				source := &ingestionBundleSource{authorization.ActivePolicyBundle{
					BundleID: "private-ci-bundle-do-not-disclose", BundleVersion: scenario.version, ActivatedAt: now,
					Bundle: authorization.PolicyBundle{BundleID: "private-ci-bundle-do-not-disclose", Version: scenario.version, PublishedAt: now,
						Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capability, Operation: "READ", RequiredScope: capability, AllowedActors: []string{"HUMAN"}}}, Grants: grants}}}
				cache := authorization.NewCache(source)
				if err := cache.Refresh(context.Background()); err != nil {
					t.Fatal(err)
				}
				var calls atomic.Int32
				gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Header.Get("Authorization") != "Bearer private-ci-workload" || r.Header.Get("X-OUF-Delegation") != "private-ci-delegation" {
						t.Error("workload or delegation changed")
					}
					if scenario.name == "owner-refuses-after-local-allow" {
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
					Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("private-ci-workload")}, FingerprintKey: make([]byte, 32)}
				handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
				if err != nil {
					t.Fatal(err)
				}
				scope := "mcp.connect " + permission
				if scenario.name == "scope-missing-with-valid-grant" {
					scope = "mcp.connect"
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					for k, v := range map[string]string{"X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "private-ci-delegation", "X-OUF-Service-Principal": "ouf-chatgpt",
						"X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1",
						"X-OUF-Token-Issuer": "private-ci-issuer", "X-OUF-Token-Audience": "private-ci-audience", "X-OUF-Granted-Scopes": scope} {
						r.Header.Set(k, v)
					}
					handler.ServeHTTP(w, r)
				}))
				defer server.Close()
				client := mcp.NewClient(&mcp.Implementation{Name: "denial-ci", Version: "1"}, nil)
				session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: map[string]any{
					"sourceId": "source-a", "runId": "11111111-1111-4111-8111-111111111111", "limit": 10}})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				for _, secret := range []string{"private-ci-workload", "private-ci-delegation", "private-ci-issuer", "private-ci-audience", "private-ci-bundle-do-not-disclose"} {
					if strings.Contains(string(raw), secret) {
						t.Fatal("private identity/authority leaked")
					}
				}
				if scenario.reason != "" {
					if !result.IsError || calls.Load() != 0 || admission.calls.Load() != 0 {
						t.Fatalf("denial crossed admission/dispatch: %+v calls=%d", result, calls.Load())
					}
					var diagnosis acquisitionDenial
					if err := json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &diagnosis); err != nil {
						t.Fatal(err)
					}
					if diagnosis.ReasonCode != scenario.reason || diagnosis.EvaluatedPolicyVersion != scenario.version || diagnosis.Retryable || diagnosis.AuthorizationPhase != "MCP_PRE_ADMISSION" || diagnosis.Code != "authorization denied" {
						t.Fatalf("wrong diagnosis: %+v", diagnosis)
					}
				} else {
					if calls.Load() != 1 || admission.calls.Load() != 1 || strings.Contains(string(raw), "MCP_PRE_ADMISSION") {
						t.Fatal("owner outcome relabelled as preflight or automatic replay")
					}
					if result.IsError != (scenario.name == "owner-refuses-after-local-allow") {
						t.Fatal("owner success/denial changed")
					}
				}
			})
		}
	}
}

func TestAcquisitionDenialProjectionIsBoundedAndTyped(t *testing.T) {
	for _, input := range []string{"raw-secret\nSCOPE_MISSING", strings.Repeat("x", 10000), "ALLOW", ""} {
		result := acquisitionErrorResult(fmt.Errorf("wrapped: %w", &orchestration.AuthorizationDeniedError{ReasonCode: input, PolicyVersion: -1}))
		var value map[string]any
		text := result.Content[0].(*mcp.TextContent).Text
		if err := json.Unmarshal([]byte(text), &value); err != nil {
			t.Fatal(err)
		}
		if value["reasonCode"] != "AUTHORIZATION_DENIED" || value["retryable"] != false || len(value) != 4 || len(text) > 220 || result.Meta != nil {
			t.Fatalf("unbounded/unsafe diagnostic: %s", text)
		}
	}
	// A string that looks like a reason is not a decision and cannot create a
	// preflight diagnosis or OAuth challenge. Generic tool errors stay generic.
	result := acquisitionErrorResult(errors.New("SCOPE_MISSING"))
	if strings.Contains(result.Content[0].(*mcp.TextContent).Text, "reasonCode") || result.Meta != nil {
		t.Fatal("untyped error gained authorization semantics")
	}
}

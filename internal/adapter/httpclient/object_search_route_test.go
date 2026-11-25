package httpclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestObjectSearchUsesExactGovernedExecuteBinding(t *testing.T) {
	for _, tc := range []struct{ capability, suffix string }{
		{"urban.object.search", "/urban.object.search"},
		{"ouf.semantic.search", "/semantic/search"},
		{"ouf.semantic.consultation.read", "/semantic/get"},
		{"ouf.system.status", ""},
		{"authorization.permissions.read", "/authorization/read"},
	} {
		t.Run(tc.capability, func(t *testing.T) {
			const base = "/internal/capabilities/v1/execute"
			var seen int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen++
				if r.Method != http.MethodPost || r.URL.Path != base+tc.suffix {
					t.Errorf("unexpected method/path %s %s", r.Method, r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer test-workload" || r.Header.Get("X-OUF-Delegation") != "test-proof" {
					t.Error("workload and delegation headers missing")
				}
				var body struct{ CapabilityID string }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CapabilityID != tc.capability {
					t.Error("capability body differs")
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("{}"))
			}))
			defer server.Close()
			client, err := NewGatewayWithTokenSource(server.URL+base, StaticTokenSource("test-workload"))
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.Execute(context.Background(), orchestration.GatewayRequest{
				CapabilityID: tc.capability, Identity: orchestration.Identity{Delegation: "test-proof"}, MaxResultBytes: 1024,
			}, 3*time.Second)
			if err != nil || result.Status != http.StatusOK || seen != 1 {
				t.Fatalf("routing failed: status=%d calls=%d err=%v", result.Status, seen, err)
			}
		})
	}
}

func TestRelatedSearchAndExplainForwardGovernedRequest(t *testing.T) {
	const base = "/internal/capabilities/v1/execute"
	for _, tc := range []struct {
		capability, owner, operation, scope, args string
		maxBytes                                  int64
	}{
		// MCP-60m-c-01
		{"urban.object.related_search", "udp", "SEARCH", "urban.object.related_search",
			`{"anchorObjectId":"11111111-1111-4111-8111-111111111111","anchorTypeCode":"A","relationIri":"urn:test:related","direction":"OUTBOUND","targetTypeCodes":["B"],"limit":2}`, 1 << 20},
		// MCP-60m-c-02
		{"ouf.operations.explain", "ingestion", "READ", "operations.incident.explain",
			`{"incidentId":"22222222-2222-4222-8222-222222222222"}`, 128 << 10},
	} {
		t.Run(tc.capability, func(t *testing.T) {
			in := orchestration.GatewayRequest{
				CapabilityID: tc.capability, GatewayBindingRef: "capability://" + tc.capability,
				Owner: tc.owner, OperationClass: tc.operation, MaxResultBytes: tc.maxBytes,
				Arguments: json.RawMessage(tc.args), RequestHash: fmt.Sprintf("%x", sha256.Sum256([]byte(tc.args))),
				AuthorizationDecisionRef: "fixture:1:" + tc.scope, CorrelationID: "correlation", IdempotencyKey: "key", AttemptID: "attempt",
				Identity: orchestration.Identity{ServicePrincipalID: "test-client", PrincipalID: "user", ActorType: "HUMAN",
					TenantID: "tenant", AuthenticationContextRef: "1", Scopes: []string{tc.scope}, Delegation: "opaque-test-proof"},
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.RequestURI() != base+"/"+tc.capability {
					t.Errorf("unexpected dispatch: %s %s", r.Method, r.URL.RequestURI())
				}
				for name, want := range map[string]string{
					"Content-Type": "application/json", "Authorization": "Bearer test-workload", "X-OUF-Delegation": in.Identity.Delegation,
					"X-Correlation-ID": in.CorrelationID, "Idempotency-Key": in.IdempotencyKey, "X-Tool-Attempt-ID": in.AttemptID,
				} {
					if r.Header.Get(name) != want {
						t.Errorf("incorrect %s header", name)
					}
				}
				raw, err := io.ReadAll(r.Body)
				var got orchestration.GatewayRequest
				if err != nil || json.Unmarshal(raw, &got) != nil {
					t.Error("invalid Gateway envelope")
					return
				}
				want := in
				want.Identity.Delegation = ""
				want.Identity.Scopes = nil
				if !reflect.DeepEqual(got, want) || bytes.Contains(raw, []byte(in.Identity.Delegation)) || bytes.Contains(raw, []byte("test-workload")) {
					t.Error("forwarded envelope differs from the authorized request")
				}
				_, _ = io.WriteString(w, "{}")
			}))
			defer server.Close()
			for _, suffix := range []string{"", "/"} {
				client, err := NewGatewayWithTokenSource(server.URL+base+suffix, StaticTokenSource("test-workload"))
				if err != nil {
					t.Fatal(err)
				}
				result, err := client.Execute(context.Background(), in, 3*time.Second)
				if err != nil || result.Status != http.StatusOK {
					t.Fatalf("dispatch status=%d err=%v", result.Status, err)
				}
			}
		})
	}
}

package httpclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestObjectSearchUsesExactGovernedExecuteBinding(t *testing.T) {
	for _, tc := range []struct{ capability, suffix string }{
		{"urban.object.search", "/urban.object.search"},
		{"urban.object.related_search", ""},
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

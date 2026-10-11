package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestOperationalDelegationUsesOnlyFixedCapabilityPaths(t *testing.T) {
	capabilities := []string{"ouf.operations.summary", "ouf.operations.incidents", "ouf.ingestion.operations.summary", "ouf.ingestion.operations.incidents", "ouf.gateway.operations.summary", "ouf.gateway.operations.incidents"}
	const proof = "opaque-header-only-context"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var envelope orchestration.GatewayRequest
		if json.Unmarshal(raw, &envelope) != nil {
			t.Error("invalid dispatch")
		}
		if r.URL.Path != "/internal/capabilities/v1/execute/"+envelope.CapabilityID {
			t.Error("wrong governed route", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer current-workload" || r.Header.Get("X-OUF-Delegation") != proof {
			t.Error("credentials not separated")
		}
		if strings.Contains(string(raw), proof) {
			t.Error("delegation serialized")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := NewGatewayWithTokenSource(server.URL+"/internal/capabilities/v1/execute", StaticTokenSource("current-workload"))
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range capabilities {
		_, err := client.Execute(context.Background(), orchestration.GatewayRequest{CapabilityID: capability, Identity: orchestration.Identity{Delegation: proof}}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOperationalDelegationCannotSelectAnArbitraryURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/capabilities/v1/execute" {
			t.Error("unreviewed capability selected path", r.URL.Path)
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	client, _ := NewGatewayWithTokenSource(server.URL+"/internal/capabilities/v1/execute", StaticTokenSource("workload"))
	for _, capability := range []string{"ouf.operations.arbitrary", "../../metadata", "https://evil.invalid/secret"} {
		response, err := client.Execute(context.Background(), orchestration.GatewayRequest{CapabilityID: capability}, time.Second)
		if err != nil || response.Status != http.StatusForbidden {
			t.Fatal(err, response.Status)
		}
	}
}

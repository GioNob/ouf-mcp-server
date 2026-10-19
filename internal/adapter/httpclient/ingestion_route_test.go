package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestIngestionReadUsesReviewedGatewayBinding(t *testing.T) {
	for _, kind := range []string{"status", "history"} {
		capability := "ouf.ingestion." + kind
		fixture := "testdata/gateway-mcp-ingestion-" + kind + ".yaml"
		t.Run(capability, func(t *testing.T) {
			const base = "/internal/capabilities/v1/execute"
			const proof = "opaque-source-read-delegation"
			contract, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			expectedPath := base + "/" + capability
			if !strings.Contains(string(contract), "match: {method: POST, path: "+expectedPath+"}") {
				t.Fatal("pinned Gateway contract does not bind the request path")
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodPost || r.URL.Path != expectedPath || r.URL.RawQuery != "" {
					t.Errorf("request missed reviewed owner route: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Header.Get("Authorization") != "Bearer test-workload" || r.Header.Get("X-OUF-Delegation") != proof {
					t.Error("workload and delegated identity must stay in their own headers")
				}
				raw, _ := io.ReadAll(r.Body)
				if bytes.Contains(raw, []byte(proof)) || bytes.Contains(raw, []byte("test-workload")) {
					t.Error("credential serialized into the request")
				}
				var envelope orchestration.GatewayRequest
				if json.Unmarshal(raw, &envelope) != nil || envelope.CapabilityID != capability ||
					envelope.GatewayBindingRef != "capability://"+capability ||
					envelope.AuthorizationDecisionRef != "fixture:47:ingestion.operations.read" {
					t.Error("capability, policy decision or binding was changed")
				}
				var arguments map[string]any
				if json.Unmarshal(envelope.Arguments, &arguments) != nil || arguments["sourceId"] != "source-fixture" ||
					arguments["limit"] != float64(10) || len(arguments) != 2 {
					t.Error("source filter or bounded arguments changed")
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"sourceId":"source-fixture","status":"OBSERVED"}`))
			}))
			defer server.Close()
			for _, suffix := range []string{"", "/"} {
				client, err := NewGatewayWithTokenSource(server.URL+base+suffix, StaticTokenSource("test-workload"))
				if err != nil {
					t.Fatal(err)
				}
				request := orchestration.GatewayRequest{}
				request.CapabilityID = capability
				request.GatewayBindingRef = "capability://" + capability
				request.AuthorizationDecisionRef = "fixture:47:ingestion.operations.read"
				request.Identity = orchestration.Identity{Delegation: proof}
				request.Arguments = json.RawMessage(`{"sourceId":"source-fixture","limit":10}`)
				request.MaxResultBytes = 1024
				response, err := client.Execute(context.Background(), request, time.Second)
				if err != nil || response.Status != http.StatusOK || response.Problem != nil {
					t.Fatalf("fixed route failed: status=%d err=%v", response.Status, err)
				}
			}
			if calls != 2 {
				t.Fatalf("unexpected retries or dispatches: %d", calls)
			}
		})
	}
}

func TestIngestionReadCannotSelectAnotherRoute(t *testing.T) {
	const base = "/internal/capabilities/v1/execute"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != base || r.URL.RawQuery != "" {
			t.Error("unknown capability selected a route")
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	client, _ := NewGatewayWithTokenSource(server.URL+base, StaticTokenSource("test-workload"))
	for _, capability := range []string{"ouf.ingestion.run", "ouf.ingestion.status/../../metadata", "ouf.ingestion.status?target=other"} {
		response, err := client.Execute(context.Background(), orchestration.GatewayRequest{CapabilityID: capability}, time.Second)
		if err != nil || response.Status != http.StatusForbidden {
			t.Fatalf("unexpected dispatch result: status=%d err=%v", response.Status, err)
		}
	}
}

package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestDelegationIsHeaderOnlyAndNotSerialized(t *testing.T) {
	const proof = "opaque-request-only-proof"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-OUF-Delegation") != proof || r.Header.Get("Authorization") != "Bearer workload" {
			t.Error("missing separated credentials")
		}
		if strings.Contains(string(raw), proof) || strings.Contains(string(raw), "Delegation") {
			t.Error("proof persisted in envelope")
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	client := GatewayClient{Endpoint: endpoint, Client: server.Client(), TokenSource: StaticTokenSource("workload")}
	identity := orchestration.Identity{PrincipalID: "human", Delegation: proof}
	raw, _ := json.Marshal(identity)
	if strings.Contains(string(raw), proof) {
		t.Fatal("identity serialization leaked proof")
	}
	_, err := client.Execute(context.Background(), orchestration.GatewayRequest{Identity: identity}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOperationalRoutesRemainGatewayBoundAndCarryDelegation(t *testing.T) {
	for _, capability := range []string{"ouf.operations.summary", "ouf.ingestion.operations.summary", "ouf.gateway.operations.summary",
		"ouf.operations.incidents", "ouf.ingestion.operations.incidents", "ouf.gateway.operations.incidents"} {
		t.Run(capability, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/execute/"+capability || r.Header.Get("X-OUF-Delegation") != "signed-proof" {
					t.Errorf("wrong path or delegation: %s", r.URL.Path)
				}
				raw, _ := io.ReadAll(r.Body)
				if strings.Contains(string(raw), "signed-proof") {
					t.Error("serialized delegation")
				}
				w.Write([]byte(`{"module":"fixture"}`))
			}))
			defer server.Close()
			endpoint, _ := url.Parse(server.URL + "/execute")
			client := GatewayClient{Endpoint: endpoint, Client: server.Client(), TokenSource: StaticTokenSource("workload")}
			_, err := client.Execute(context.Background(), orchestration.GatewayRequest{CapabilityID: capability, Identity: orchestration.Identity{Delegation: "signed-proof"}}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

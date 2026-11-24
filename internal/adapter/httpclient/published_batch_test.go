package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestPublishedBatchRouteAndSerializedLimit(t *testing.T) { // MCP-50T-08
	request := orchestration.GatewayRequest{
		CapabilityID: "ouf.semantic.search.batch", GatewayBindingRef: "capability://ouf.semantic.search.batch",
		Owner: "semantic", OperationClass: "READ", Identity: orchestration.Identity{Delegation: "synthetic"},
		Arguments:      json.RawMessage(`{"contract":"ouf.semantic.published-batch.v1","intents":[{"intentId":"i","intent":"a<&","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY","context":{}}],"limitPerIntent":3}`),
		MaxResultBytes: 262144,
	}
	expected, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil || r.URL.Path != "/internal/capabilities/v1/execute/semantic/consultation/search-batch" ||
			r.Method != "POST" || !bytes.Equal(raw, expected) || r.Header.Get("X-OUF-Delegation") != "synthetic" {
			t.Error("unexpected Gateway dispatch", r.URL.Path, err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL + "/internal/capabilities/v1/execute")
	client := GatewayClient{Endpoint: endpoint, Client: server.Client(), TokenSource: StaticTokenSource("fixture")}
	for _, limit := range []int{len(expected) - 1, len(expected)} {
		request.MaxRequestBytes = limit
		res, err := client.Execute(context.Background(), request, time.Second)
		if err != nil || (limit < len(expected) && res.Status != 413) || (limit == len(expected) && res.Status != 200) {
			t.Fatal(limit, res, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("oversize envelope dispatched", calls.Load())
	}
}

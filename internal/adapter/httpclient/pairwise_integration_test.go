package httpclient

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/recovery"
)

func TestMCPGatewayUDPPairwise(t *testing.T) {
	gatewayRoot := os.Getenv("GATEWAY_PAIRWISE_ROOT")
	if gatewayRoot == "" {
		t.Skip("GATEWAY_PAIRWISE_ROOT is not set")
	}
	token := "pairwise-workload-token"
	dispatchCalls, recoveryCalls := 0, 0
	udp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && len(r.URL.Path) > len("/internal/v1/attempt-outcomes/") && r.URL.Path[:len("/internal/v1/attempt-outcomes/")] == "/internal/v1/attempt-outcomes/" {
			recoveryCalls++
			backendID := r.URL.Path[len("/internal/v1/attempt-outcomes/"):]
			if r.Header.Get("X-OUF-Capability-ID") != "urban.object.related_search" || r.Header.Get("X-OUF-Recovery-For") != backendID || r.Header.Get("X-Correlation-ID") == "" {
				t.Errorf("invalid recovery path=%s headers=%v", r.URL.Path, r.Header)
				http.Error(w, "invalid", 500)
				return
			}
			if backendID == "udp-unavailable" {
				http.Error(w, "unavailable", 503)
				return
			}
			if backendID == "udp-mismatch" {
				_, _ = w.Write([]byte(`{"BackendRequestID":"other","Outcome":"SUCCEEDED"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"BackendRequestID":"udp-request-1","Outcome":"NOT_DISPATCHED","OutcomeCode":"OWNER_PROVES_NO_DISPATCH"}`))
			return
		}
		dispatchCalls++
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/objects/related-search" || r.Header.Get("X-OUF-Capability-ID") != "urban.object.related_search" || r.Header.Get("X-OUF-Authorization-Decision-Ref") != "decision-1" {
			t.Errorf("invalid UDP dispatch path=%s headers=%v", r.URL.Path, r.Header)
			http.Error(w, "invalid", 500)
			return
		}
		if dispatchCalls == 2 {
			w.Header().Set("Content-Type", "application/problem+json")
			w.Header().Set("Retry-After", "17")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"type":"urn:ouf:budget","title":"Budget exhausted","status":429,"code":"BUDGET_EXHAUSTED","detail":"bounded"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-Request-ID", "udp-request-1")
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer udp.Close()
	python := os.Getenv("PAIRWISE_PYTHON")
	if python == "" {
		python = "python3"
	}
	command := exec.Command(python, filepath.Join(gatewayRoot, "tools", "pairwise_server.py"), "--listen", "127.0.0.1:0")
	command.Dir = gatewayRoot
	command.Env = append(os.Environ(), "PAIRWISE_WORKLOAD_TOKEN="+token, "PAIRWISE_UDP_URL="+udp.URL)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatal("Gateway pairwise server did not publish its port")
	}
	port, err := strconv.Atoi(scanner.Text())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewGateway(fmt.Sprintf("http://127.0.0.1:%d/internal/capabilities/v1/execute/urban.object.related_search", port), token)
	if err != nil {
		t.Fatal(err)
	}
	request := orchestration.GatewayRequest{GatewayBindingRef: "capability://urban.object.related_search", CapabilityID: "urban.object.related_search", Owner: "udp", OperationClass: "SEARCH", Arguments: json.RawMessage(`{"anchorObjectId":"fd971091-2b0d-4daf-977a-81509056315a","anchorTypeCode":"DEHOR","relationIri":"https://example.test/relatedTo","direction":"OUTBOUND","targetTypeCodes":["CIVICO"],"limit":10}`), Identity: orchestration.Identity{ServicePrincipalID: "ouf-mcp-server", PrincipalID: "agent-1", TenantID: "tenant-1", ActorType: "AI_AGENT", AuthenticationContextRef: "authn-1"}, AuthorizationDecisionRef: "decision-1", CorrelationID: "correlation-1", IdempotencyKey: "idempotency-1", AttemptID: "321946e1-8f6f-4091-92a5-7ddce63329a7", RequestHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", MaxResultBytes: 1048576}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := client.Execute(ctx, request, 3*time.Second)
	if err != nil || first.Status != 200 || string(first.Body) != `{"items":[]}` || first.BackendRequestID != "udp-request-1" {
		t.Fatalf("success response %+v err=%v", first, err)
	}
	request.IdempotencyKey = "idempotency-2"
	request.AttemptID = "a7455099-4b16-4e20-8a90-33db8a5ed1ed"
	request.CorrelationID = "correlation-2"
	second, err := client.Execute(ctx, request, 3*time.Second)
	if err != nil || second.Status != 429 || second.Problem == nil || second.Problem.Code != "BUDGET_EXHAUSTED" || second.Problem.RetryAfter != "17" {
		t.Fatalf("problem response %+v err=%v", second, err)
	}
	if dispatchCalls != 2 {
		t.Fatalf("UDP dispatch calls=%d", dispatchCalls)
	}
	recoveryClient, err := NewRecovery(fmt.Sprintf("http://127.0.0.1:%d/internal/capabilities/v1/recovery", port), token)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := recoveryClient.QueryOutcome(ctx, recovery.OwnerQuery{BackendRequestID: "udp-request-1", Owner: "udp", CapabilityID: "urban.object.related_search", CorrelationID: "correlation-recovery-1"})
	if err != nil || evidence.Outcome != "NOT_DISPATCHED" || evidence.OutcomeCode != "OWNER_PROVES_NO_DISPATCH" || evidence.ActualToolCalls != 0 || evidence.ActualResultBytes != 0 {
		t.Fatalf("recovery evidence %+v err=%v", evidence, err)
	}
	for _, backendID := range []string{"udp-unavailable", "udp-mismatch"} {
		_, err = recoveryClient.QueryOutcome(ctx, recovery.OwnerQuery{BackendRequestID: backendID, Owner: "udp", CapabilityID: "urban.object.related_search", CorrelationID: "correlation-" + backendID})
		if err == nil {
			t.Fatalf("non-authoritative owner response became terminal for %s", backendID)
		}
	}
	if recoveryCalls != 3 {
		t.Fatalf("UDP recovery calls=%d", recoveryCalls)
	}
}

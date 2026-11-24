package httpclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type statusRoundTrip func(*http.Request) (*http.Response, error)

func (f statusRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStatusUsesWorkloadOnly(t *testing.T) { // MCP-50V-01
	g, err := NewGateway("https://gateway.invalid", "fixture-workload")
	if err != nil {
		t.Fatal(err)
	}
	body := `{"mandateId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","generation":"1"}`
	calls := 0
	g.Client = &http.Client{Transport: statusRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		raw, err := io.ReadAll(r.Body)
		if err != nil || string(raw) != body || r.Method != "POST" || r.URL.Path != "/internal/analysis/v1/mandates/status" ||
			r.Header.Get("Authorization") != "Bearer fixture-workload" || r.Header.Get("X-OUF-Delegation") != "" {
			t.Fatal("unexpected status binding")
		}
		return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader(`{"code":"ANALYSIS_MANDATE_UNKNOWN"}`))}, nil
	})}
	reply, err := g.MandateCall(context.Background(), "status", []byte(body), "", 4096, time.Second)
	if err != nil || reply.Status != 404 {
		t.Fatal(reply.Status, err)
	}
	if _, err = g.MandateCall(context.Background(), "status", []byte(body), "fixture-interactive", 4096, time.Second); err == nil || calls != 1 {
		t.Fatal("accepted interactive delegation")
	}
}

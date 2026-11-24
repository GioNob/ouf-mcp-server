package httpclient

import (
	"context"
	"encoding/json"
	"os"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
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

func TestRenewalCheckAgainstGatewayBytes(t *testing.T) { // MCP-55B-03
	raw, err := os.ReadFile("../../analysis/testdata/renewal-gateway.json")
	var f struct {
		Command                analysis.RenewalCommand
		Read                   analysis.RenewalProposal
		DecideProof, CheckWire string
		CheckResponse          json.RawMessage
	}
	if err != nil || json.Unmarshal(raw, &f) != nil {
		t.Fatal(err)
	}
	g, err := NewGateway("https://gateway.invalid"+analysis.RenewalCheckPath, "fixture-service")
	if err != nil {
		t.Fatal(err)
	}
	g.Client = &http.Client{Transport: statusRoundTrip(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != f.CheckWire || r.URL.Path != analysis.RenewalCheckPath || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer fixture-service" || r.Header.Get("X-OUF-Delegation") != f.DecideProof {
			t.Fatal("renewal binding", err)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(f.CheckResponse)))}, nil
	})}
	check := analysis.RunRenewalCheck{Command: f.Command, MandateID: f.Read.MandateID, Generation: f.Read.Generation, RunRenewal: analysis.RunRenewal{AnalysisID: f.Read.AnalysisID, IncidentID: f.Read.IncidentID}}
	d, err := g.AuthorizeRunRenewal(context.Background(), orchestration.Identity{ActorType: "HUMAN", Delegation: f.DecideProof}, check)
	if err != nil || d.ValidUntil.Unix() != 1017 || d.GrantRef != "grant1" {
		t.Fatal(d, err)
	}
}

func TestDispatchCrossingCause(t *testing.T) { // MCP-58E-01
	m := analysis.Mandate{ID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Generation: "1"}
	for _, tc := range []struct {
		name       string
		headers    []string
		state, gen string
		revoked    bool
	}{
		{"superseded", []string{"SUPERSEDED"}, "", "", false},
		{"revoked", []string{"REVOKED"}, "", "", true},
		{"both lines", []string{"SUPERSEDED", "REVOKED"}, "", "", true},
		{"both combined", []string{"REVOKED, SUPERSEDED"}, "", "", true},
		{"legacy active", nil, "ACTIVE", "1", false},
		{"legacy revoked", nil, "REVOKED", "1", true},
		{"legacy generation", nil, "ACTIVE", "2", true},
		{"legacy malformed", nil, "ACTIVE", "", false},
		{"legacy unavailable", nil, "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := NewGateway("https://gateway.invalid", "fixture")
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			g.Client = &http.Client{Transport: statusRoundTrip(func(r *http.Request) (*http.Response, error) {
				status := 409
				header := make(http.Header)
				body := `{"code":"ANALYSIS_STALE_EXECUTION"}`
				switch r.URL.Path {
				case "/internal/analysis/v1/mandates/dispatch":
					header.Set("X-OUF-Analysis-Effect", "IN_FLIGHT")
					for _, h := range tc.headers {
						header.Add("X-OUF-Analysis-Stale-Cause", h)
					}
				case "/internal/analysis/v1/mandates/status":
					reads++
					var target map[string]string
					if json.NewDecoder(r.Body).Decode(&target) != nil || target["mandateId"] != m.ID || target["generation"] != m.Generation {
						t.Fatal("wrong mandate")
					}
					status = 200
					raw, _ := json.Marshal(map[string]string{"mandateId": m.ID, "generation": tc.gen, "state": tc.state, "decisionRef": "fixture", "expiresAt": "2030-10-01T00:00:00Z"})
					body = string(raw)
					if tc.state == "" {
						status = 503
					}
				default:
					t.Fatal(r.URL.Path)
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			ctx := context.Background()
			reply, err := g.DispatchMandate(ctx, []byte("{}"), 4096, time.Second)
			if err != nil || reply.Effect != "IN_FLIGHT" || analysis.CrossingRevoked(ctx, g, m, reply.StaleCause) != tc.revoked ||
				(reads == 1) != (tc.headers == nil) || reads > 1 {
				t.Fatal(reply.StaleCause, reads, err)
			}
		})
	}
}

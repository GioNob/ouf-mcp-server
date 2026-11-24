package analysis

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type gatewayFixture struct {
	Read                                           json.RawMessage
	Command                                        RenewalCommand
	CheckWire, CommandWire, ReadProof, DecideProof string
	CheckResponse                                  json.RawMessage
}

type renewalRelayFixture struct {
	proposal RenewalProposal
	seen     orchestration.Identity
	command  RenewalCommand
	status   orchestration.MandateResponse
}

func (f *renewalRelayFixture) MandateCall(_ context.Context, op string, raw json.RawMessage, delegation string, _ int64, _ time.Duration) (orchestration.MandateResponse, error) {
	var target map[string]string
	if op != "status" || delegation != "" || json.Unmarshal(raw, &target) != nil || len(target) != 2 ||
		target["mandateId"] != f.proposal.MandateID || target["generation"] != f.proposal.Generation || f.seen.PrincipalID != "holder" {
		return orchestration.MandateResponse{}, ErrInvalid
	}
	return f.status, nil
}

func (f *renewalRelayFixture) ReadRenewal(_ context.Context, i orchestration.Identity, _ string) (RenewalProposal, error) {
	f.seen = i
	return f.proposal, nil
}

func (f *renewalRelayFixture) DecideRenewal(_ context.Context, i orchestration.Identity, c RenewalCommand, _ RunRenewalAuthority) (json.RawMessage, error) {
	f.seen, f.command = i, c
	return json.RawMessage(`{"proposalState":"CHANGES_REQUESTED"}`), nil
}

func (*renewalRelayFixture) AuthorizeRunRenewal(context.Context, orchestration.Identity, RunRenewalCheck) (RenewalDecision, error) {
	panic("the HTTP relay fixture has no database")
}

// MCP-55B-01. These bytes and opaque proofs come from the approved Python Gateway.
func TestRenewalGatewayFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/renewal-gateway.json")
	var f gatewayFixture
	if err != nil || json.Unmarshal(raw, &f) != nil {
		t.Fatal("gateway fixture", err)
	}
	var store renewalRelayFixture
	if err = json.Unmarshal(f.Read, &store.proposal); err != nil {
		t.Fatal(err)
	}
	p := store.proposal
	check := RunRenewalCheck{RunRenewal: RunRenewal{AnalysisID: p.AnalysisID, IncidentID: p.IncidentID}, Command: f.Command, MandateID: p.MandateID, Generation: p.Generation}
	wire, err := check.Wire()
	if err != nil || string(wire) != f.CheckWire {
		t.Fatalf("renewal-check wire %q %v", wire, err)
	}
	decision, err := DecodeRenewalDecision(f.CheckResponse, wire)
	if err != nil || decision.ValidUntil.Unix() != 1017 || decision.DecisionRef != "iam-decision" {
		t.Fatal(decision, err)
	}
	if _, err = DecodeRenewalDecision(f.CheckResponse, append(wire, ' ')); err == nil {
		t.Fatal("accepted different request bytes")
	}
	c := RenewalChannel{ReviewBase: "https://ths.invalid/trusted-human/analysis-renewal/", CheckEndpoint: "https://gateway.invalid" + RenewalCheckPath,
		Issuer: "https://issuer.invalid", Audience: "gateway", Workload: "worker", THSClient: "ths"}
	now := time.Unix(1001, 0)
	peer, _ := url.Parse("spiffe://gateway.invalid/relay")
	store.status = orchestration.MandateResponse{Status: 200}
	store.status.Body, _ = json.Marshal(map[string]any{"mandateId": p.MandateID, "generation": p.Generation, "state": "ACTIVE", "decisionRef": "fixture", "expiresAt": now.Add(time.Hour)})
	h := RenewalHandler(&store, &store, &store, c, peer.String(), func() time.Time { return now })
	post := func(action, body, proof string, verified bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", renewalRoutes+action, strings.NewReader(body))
		if verified {
			req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{peer}}}}}
		}
		req.Header.Set("X-OUF-Delegation", proof)
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res
	}
	read := `{"reviewRef":"` + p.ReviewRef + `"}`
	res := post("read", read, f.ReadProof, true)
	var got, want map[string]any
	if res.Code != 200 || json.Unmarshal(res.Body.Bytes(), &got) != nil || json.Unmarshal(f.Read, &want) != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("closed read response", res.Code, res.Body.String())
	}
	t.Run("MCP-58E-03", func(t *testing.T) {
		for _, tc := range []struct {
			status, want int
			code, public string
		}{
			{403, 404, "ANALYSIS_AUTHORITY_DENIED", "ANALYSIS_MANDATE_UNKNOWN"},
			{503, 503, "ANALYSIS_AUTHORITY_UNAVAILABLE", "ANALYSIS_AUTHORITY_UNAVAILABLE"},
		} {
			store.status.Status = tc.status
			store.status.Body, _ = json.Marshal(map[string]string{"code": tc.code})
			res := post("read", read, f.ReadProof, true)
			var problem map[string]string
			if res.Code != tc.want || json.Unmarshal(res.Body.Bytes(), &problem) != nil || len(problem) != 1 || problem["code"] != tc.public {
				t.Fatal(res.Code, res.Body.String())
			}
		}
	})
	res = post("decide", f.CommandWire, f.DecideProof, true)
	if res.Code != 200 || store.seen.Delegation != f.DecideProof || store.seen.TenantID != "tenant" || store.seen.PrincipalID != "holder" || !reflect.DeepEqual(store.command, f.Command) {
		t.Fatal("decision relay", res.Code)
	}
	for _, bad := range []*httptest.ResponseRecorder{post("read", read, f.ReadProof, false), post("read", read+" ", f.ReadProof, true), post("decide", f.CommandWire, f.ReadProof, true)} {
		if bad.Code != 403 {
			t.Fatal("invalid hop admitted", bad.Code)
		}
	}
	now = time.Unix(1020, 0)
	if post("read", read, f.ReadProof, true).Code != 403 {
		t.Fatal("expired proof")
	}
	h = RenewalHandler(&store, nil, nil, RenewalChannel{}, peer.String(), time.Now)
	if res = post("read", read, f.ReadProof, true); res.Code != 503 || !bytes.Contains(res.Body.Bytes(), []byte("ANALYSIS_AUTHORITY_UNAVAILABLE")) {
		t.Fatal(res.Code, res.Body.String())
	}
}

func TestRenewalCommandUnicodeAndTypes(t *testing.T) {
	c := RenewalCommand{ReviewRef: strings.Repeat("a", 22), Version: 1, Hash: strings.Repeat("a", 64), ExpectedSeq: "0", Key: "11111111-1111-4111-8111-111111111111", Decision: "APPROVE", Items: []RenewalItem{{"general", strings.Repeat("🌱", 500)}}}
	raw, _ := json.Marshal(c)
	if _, err := ParseRenewalCommand(raw); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(string(raw), `"proposalVersion":1`, `"proposalVersion":"1"`, 1),
		strings.Replace(string(raw), `"expectedSeq":"0"`, `"expectedSeq":"00"`, 1),
		strings.Replace(string(raw), "🌱", "🌱🌱", 1),
		strings.Replace(string(raw), `"field":"general"`, `"field":"general","other":0`, 1),
	} {
		if _, err := ParseRenewalCommand([]byte(bad)); err == nil {
			t.Fatal("invalid command admitted")
		}
	}
	label := PlanLabel("dir/"+strings.Repeat("🌱", 200)+".csv", 18)
	if len([]rune(label)) != 200 || !strings.HasSuffix(label, " · 18 colonne") || strings.Contains(label, "dir") {
		t.Fatal(label)
	}
	res := httptest.NewRecorder()
	at := time.Unix(7, 0).UTC()
	renewalProblem(res, &RenewalBackoff{Cause: ErrRunPressure, RetryAfter: "7", NotBefore: at})
	var problem map[string]string
	if res.Code != http.StatusTooManyRequests || res.Header().Get("Retry-After") != "7" ||
		json.Unmarshal(res.Body.Bytes(), &problem) != nil || problem["code"] != ErrRunPressure.Error() || problem["notBefore"] != at.Format(time.RFC3339Nano) {
		t.Fatal(res)
	}
}

func TestRenewalChannelConfiguration(t *testing.T) {
	for name, values := range map[string][]string{
		"OUF_ANALYSIS_RENEWAL_REVIEW_BASE":    {"http://ths.invalid/trusted-human/analysis-renewal/", "https://ths.invalid/wrong/"},
		"OUF_ANALYSIS_RENEWAL_CHECK_ENDPOINT": {"https://user@gateway.invalid" + RenewalCheckPath, "https://gateway.invalid" + RenewalCheckPath + "?other"},
		"OUF_ANALYSIS_RENEWAL_BODY_BYTES":     {"0", "131073"}, "OUF_ANALYSIS_RENEWAL_PROPOSAL_SECONDS": {"0", "3601"},
	} {
		for _, value := range values {
			t.Run(name, func(t *testing.T) {
				t.Setenv(name, value)
				if _, err := RenewalConfiguration(); err == nil {
					t.Fatal("invalid installation value admitted", name)
				}
			})
		}
	}
}

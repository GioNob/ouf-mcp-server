package operational

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSummaryProjectionRejectsIncompleteEvidence(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"module":"INGESTION","status":"HEALTHY"}`, `{"module":"INGESTION","status":"BOGUS","partial":false}`, `{"module":"OTHER","status":"HEALTHY","partial":false}`} {
		if _, err := summaryModule("INGESTION", []byte(raw), 10); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, raw := range []string{`{"module":"MCP","status":"HEALTHY","partial":true,"secret":"hidden"}`, `{"module":"MCP","status":"UNKNOWN","partial":false,"securityIncidentCount":99}`} {
		got, err := summaryModule("MCP", []byte(raw), 10)
		if err != nil || got["partial"] != true || got["secret"] != nil || got["securityIncidentCount"] != nil {
			t.Fatalf("%v %v", got, err)
		}
	}
}

type deniedSummaryCaller struct{}

func (deniedSummaryCaller) Call(context.Context, orchestration.Invocation) (orchestration.Result, error) {
	return orchestration.Result{}, orchestration.ErrUnauthorized
}
func TestSummaryDoesNotConvertDenialToNoIncidents(t *testing.T) {
	a := Aggregator{Caller: deniedSummaryCaller{}, Self: aggregateSelfFixture{[]byte(`{"module":"MCP","status":"HEALTHY","partial":false}`)}, ManifestChecksum: "manifest"}
	body, err := a.Summary(context.Background(), aggregateInput())
	if !errors.Is(err, orchestration.ErrUnauthorized) || len(body) > 0 {
		t.Fatalf("%s %v", body, err)
	}
}

func TestSummaryOwnerAuthorizesBeforeReading(t *testing.T) {
	for _, mode := range []string{"missing-auth", "deny", "wrong-tenant", "stale", "wrong-detail", "allow"} {
		t.Run(mode, func(t *testing.T) {
			self := &ownerStatusFixture{}
			caller := &aggregateCallerFixture{responses: map[string][]byte{"ouf.ingestion.operations.summary": []byte(`{"module":"INGESTION","status":"HEALTHY","partial":false}`), "ouf.gateway.operations.summary": []byte(`{"module":"GATEWAY","status":"HEALTHY","partial":false}`)}}
			a := &Aggregator{Caller: caller, Self: self, ManifestChecksum: "manifest"}
			h := NewOwnerAPI(self, a)
			d := orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "bundle:summary", PermittedDetailLevel: "TENANT_OPERATIONAL", ResourceScope: map[string]string{"tenantId": "tenant-a", "resourceType": "capability"}}
			switch mode {
			case "deny":
				d.Allowed = false
			case "wrong-tenant":
				d.ResourceScope["tenantId"] = "other"
			case "stale":
				d.DecisionRef = "new-policy"
			case "wrong-detail":
				d.PermittedDetailLevel = "PUBLIC_OPERATIONAL"
			}
			if mode != "missing-auth" {
				h.WithAuthorization(ownerDecisionFixture{decision: d})
			}
			req := httptest.NewRequest("POST", "/api/internal/v1/mcp/operations/summary", strings.NewReader(`{"limit":5}`))
			for k, v := range map[string]string{"X-OUF-Gateway-Verified": "true", "X-OUF-Principal-ID": "reader", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Authorization-Decision-Ref": "bundle:summary", "X-OUF-Delegation": "verified-proof"} {
				req.Header.Set(k, v)
			}
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if mode == "allow" {
				if res.Code != 200 || !self.called {
					t.Fatalf("%d %s", res.Code, res.Body.String())
				}
			} else if res.Code == 200 || self.called || caller.callCount() != 0 {
				t.Fatalf("unauthorized read %d", res.Code)
			}
		})
	}
}
func TestSummaryProjectionCapsItemsAndRemovesProtectedFields(t *testing.T) {
	raw := []byte(`{"module":"INGESTION","status":"DEGRADED","partial":false,"rawLog":"secret","openIncidents":2,"items":[{"incident_id":"1","visibility_class":"TENANT_OPERATIONAL","secret":"hidden"},{"incident_id":"2","visibility_class":"SECURITY_SENSITIVE"}]}`)
	m, err := summaryModule("INGESTION", raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(m)
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "SECURITY_SENSITIVE") || m["partial"] != true || m["openIncidents"] != nil {
		t.Fatal(string(b))
	}
}

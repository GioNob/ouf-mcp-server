package operational

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type ownerStatusFixture struct {
	identity orchestration.Identity
	called   bool
}

func TestOwnerPublicStatusCannotBeRaisedByCaller(t *testing.T) {
	h := NewOwnerAPI(aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"partial":false,"incidents":["secret"],"securityIncidentCount":10,"future":"secret"}`)})
	h.WithAuthorization(ownerAuthFixture{})
	for _, decision := range []string{"", "bundle:1:ouf.system.status"} {
		req := httptest.NewRequest(http.MethodPost, "/api/internal/v1/mcp/operations/status", nil)
		req.Header.Set("X-OUF-Gateway-Verified", "true")
		req.Header.Set("X-OUF-Tenant-ID", "tenant-a")
		req.Header.Set("X-OUF-Principal-ID", "user-a")
		req.Header.Set("X-OUF-Authorization-Decision-Ref", decision)
		req.Header.Set("X-OUF-Permitted-Detail-Level", "SECURITY_SENSITIVE")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if decision == "" {
			if res.Code != http.StatusForbidden {
				t.Fatal(res.Code)
			}
			continue
		}
		var body map[string]any
		if res.Code != 200 || json.Unmarshal(res.Body.Bytes(), &body) != nil || len(body) != 6 || body["visibilityClass"] != "PUBLIC_OPERATIONAL" || body["redacted"] != true || body["status"] != "DEGRADED" {
			t.Fatalf("invalid public result: %d %s", res.Code, res.Body.String())
		}
		if _, ok := body["incidents"]; ok {
			t.Fatal("incident disclosure")
		}
	}
}

func (f *ownerStatusFixture) SystemStatus(_ context.Context, identity orchestration.Identity) ([]byte, error) {
	f.called = true
	f.identity = identity
	return []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`), nil
}

func TestOwnerAPIRequiresGatewayAndTenantContext(t *testing.T) {
	fixture := &ownerStatusFixture{}
	h := NewOwnerAPI(fixture).WithAuthorization(ownerAuthFixture{})

	for _, tc := range []struct {
		name    string
		gateway bool
		tenant  string
		want    int
	}{
		{"no-gateway", false, "tenant-a", http.StatusUnauthorized},
		{"no-tenant", true, "", http.StatusBadRequest},
		{"trusted", true, "tenant-a", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/internal/v1/mcp/operations/status", nil)
			if tc.gateway {
				req.Header.Set("X-OUF-Gateway-Verified", "true")
			}
			if tc.tenant != "" {
				req.Header.Set("X-OUF-Tenant-ID", tc.tenant)
			}
			req.Header.Set("X-OUF-Principal-ID", "principal-a")
			req.Header.Set("X-OUF-Authorization-Decision-Ref", "bundle:1:ouf.system.status")
			resp := httptest.NewRecorder()
			h.ServeHTTP(resp, req)
			if resp.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", resp.Code, tc.want, resp.Body.String())
			}
		})
	}
	if !fixture.called || fixture.identity.TenantID != "tenant-a" || fixture.identity.PrincipalID != "principal-a" {
		t.Fatalf("trusted identity not propagated: called=%v identity=%+v", fixture.called, fixture.identity)
	}
}

func TestOwnerAPIDoesNotExposeMCPProtocolPath(t *testing.T) {
	h := NewOwnerAPI(&ownerStatusFixture{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("X-OUF-Gateway-Verified", "true")
	req.Header.Set("X-OUF-Tenant-ID", "tenant-a")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("owner API accepted MCP protocol path: %d", resp.Code)
	}
}

type ownerAuthFixture struct{}

func (ownerAuthFixture) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "bundle:1:ouf.system.status", PermittedDetailLevel: "PUBLIC_OPERATIONAL", ResourceScope: map[string]string{"tenantId": in.Identity.TenantID, "resourceType": "capability"}}, nil
}

type ownerDecisionFixture struct {
	decision orchestration.AuthorizationDecision
	err      error
}

func (f ownerDecisionFixture) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return f.decision, f.err
}

func TestOwnerReauthorizationStopsRead(t *testing.T) {
	base, _ := (ownerAuthFixture{}).Authorize(context.Background(), orchestration.AuthorizationRequest{Identity: orchestration.Identity{TenantID: "tenant-a"}})
	for _, name := range []string{"unavailable", "denied", "stale-reference", "wrong-tenant", "raised-detail", "wrong-resource"} {
		t.Run(name, func(t *testing.T) {
			provider := &ownerStatusFixture{}
			handler := NewOwnerAPI(provider)
			decision := base
			decision.ResourceScope = map[string]string{"tenantId": "tenant-a", "resourceType": "capability"}
			switch name {
			case "denied":
				decision.Allowed = false
			case "stale-reference":
				decision.DecisionRef = "bundle:2:ouf.system.status"
			case "wrong-tenant":
				decision.ResourceScope["tenantId"] = "other"
			case "raised-detail":
				decision.PermittedDetailLevel = "SECURITY_SENSITIVE"
			case "wrong-resource":
				decision.ResourceScope["resourceType"] = "other"
			}
			if name != "unavailable" {
				handler.WithAuthorization(ownerDecisionFixture{decision: decision})
			}
			req := httptest.NewRequest(http.MethodPost, "/api/internal/v1/mcp/operations/status", nil)
			req.Header.Set("X-OUF-Gateway-Verified", "true")
			req.Header.Set("X-OUF-Principal-ID", "human")
			req.Header.Set("X-OUF-Tenant-ID", "tenant-a")
			req.Header.Set("X-OUF-Authorization-Decision-Ref", base.DecisionRef)
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != 403 && res.Code != 503 {
				t.Fatalf("unexpected status %d", res.Code)
			}
			if provider.called {
				t.Fatal("read occurred before authorization")
			}
		})
	}
}

type aggregateAuthorizationFixture struct {
	request orchestration.AuthorizationRequest
	failure string
}

func (f *aggregateAuthorizationFixture) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	f.request = in
	d := orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "bundle:1:" + in.CapabilityID, PermittedDetailLevel: "TENANT_OPERATIONAL", ResourceScope: map[string]string{"tenantId": in.Identity.TenantID, "resourceType": "capability"}}
	switch f.failure {
	case "error":
		return d, errors.New("policy unavailable")
	case "denied":
		d.Allowed = false
	case "stale-reference":
		d.DecisionRef = "bundle:2:" + in.CapabilityID
	case "wrong-tenant":
		d.ResourceScope["tenantId"] = "other"
	case "raised-detail":
		d.PermittedDetailLevel = "SECURITY_SENSITIVE"
	case "wrong-resource":
		d.ResourceScope["resourceType"] = "other"
	}
	return d, nil
}

func aggregateOwnerRequest(kind, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/internal/v1/mcp/operations/"+kind, strings.NewReader(body))
	req.Header.Set("X-OUF-Gateway-Verified", "true")
	req.Header.Set("X-OUF-Tenant-ID", "tenant-a")
	req.Header.Set("X-OUF-Principal-ID", "operator")
	req.Header.Set("X-OUF-Authorization-Decision-Ref", "bundle:1:ouf.operations."+kind)
	return req
}

func aggregateOwnerFixture() (*ownerStatusFixture, *aggregateCallerFixture, *ownerAPI) {
	self := &ownerStatusFixture{}
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.summary": []byte(`{"module":"INGESTION","status":"HEALTHY","partial":false}`),
		"ouf.gateway.operations.summary": []byte(`{"module":"GATEWAY","status":"HEALTHY","partial":false}`),
		"ouf.ingestion.operations.incidents": []byte(`{"items":[],"partial":false}`),
		"ouf.gateway.operations.incidents": []byte(`{"items":[],"partial":false}`),
	}}
	return self, caller, NewOwnerAPI(self, &Aggregator{Self: self, Caller: caller, ManifestChecksum: "manifest"})
}

func TestAggregateOwnerAuthorizationStopsAllReads(t *testing.T) {
	for _, kind := range []string{"summary", "incidents"} {
		for _, failure := range []string{"missing-principal", "missing-reference", "unavailable", "error", "denied", "stale-reference", "wrong-tenant", "raised-detail", "wrong-resource"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				self, caller, handler := aggregateOwnerFixture()
				auth := &aggregateAuthorizationFixture{failure: failure}
				if failure != "unavailable" {
					handler.WithAuthorization(auth)
				}
				req := aggregateOwnerRequest(kind, "{}")
				if failure == "missing-principal" {
					req.Header.Del("X-OUF-Principal-ID")
				}
				if failure == "missing-reference" {
					req.Header.Del("X-OUF-Authorization-Decision-Ref")
				}
				res := httptest.NewRecorder()
				handler.ServeHTTP(res, req)
				want := http.StatusForbidden
				if failure == "unavailable" || failure == "error" {
					want = http.StatusServiceUnavailable
				}
				if res.Code != want || self.called || caller.callCount() != 0 {
					t.Fatalf("unauthorized read: status=%d self=%v calls=%d", res.Code, self.called, caller.callCount())
				}
			})
		}
	}
}

func TestAggregateOwnerAcceptsOnlyPublishedTypedQueries(t *testing.T) {
	for _, kind := range []string{"summary", "incidents"} {
		for _, body := range []string{"", "{}", `{"sourceId":"source-a","since":"2026-10-11T02:00:00+02:00","limit":100}`} {
			self, caller, handler := aggregateOwnerFixture()
			auth := &aggregateAuthorizationFixture{}
			handler.WithAuthorization(auth)
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, aggregateOwnerRequest(kind, body))
			if res.Code != 200 || !self.called || caller.callCount() != 2 || auth.request.CapabilityID != "ouf.operations."+kind || auth.request.Resource.Attributes["detailLevel"] != "TENANT_OPERATIONAL" || auth.request.Resource.TenantID != "tenant-a" {
				t.Fatalf("valid governed query failed: %d %s request=%+v", res.Code, res.Body.String(), auth.request)
			}
		}
		for _, body := range []string{
			"null", "[]", "{}{}", `{"future":true}`, `{"cursor":"unqualified"}`,
			`{"limit":0}`, `{"limit":101}`, `{"limit":null}`, `{"limit":1.5}`, `{"limit":"1"}`,
			`{"limit":1,"limit":2}`, `{"sourceId":""}`, `{"sourceId":null}`,
			`{"since":"2026-10-11T00:00:00"}`, `{"since":null}`, `{"state":"INVALID"}`,
			`{"sourceId":"` + strings.Repeat("x", 201) + `"}`,
			string([]byte{'{', '"', 's', 'o', 'u', 'r', 'c', 'e', 'I', 'd', '"', ':', '"', 0xff, '"', '}'}),
		} {
			self, caller, handler := aggregateOwnerFixture()
			handler.WithAuthorization(&aggregateAuthorizationFixture{})
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, aggregateOwnerRequest(kind, body))
			if res.Code != 400 || self.called || caller.callCount() != 0 {
				t.Fatalf("invalid query read data: kind=%s body=%q status=%d", kind, body, res.Code)
			}
		}
	}
	for _, kind := range []string{"summary", "incidents"} {
		self, caller, handler := aggregateOwnerFixture()
		handler.WithAuthorization(&aggregateAuthorizationFixture{})
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, aggregateOwnerRequest(kind, "{}"+strings.Repeat(" ", 64<<10)))
		if res.Code != http.StatusRequestEntityTooLarge || self.called || caller.callCount() != 0 {
			t.Fatal("oversized request was silently truncated")
		}
	}
	if _, valid := operationalQuery([]byte(`{"state":"OPEN"}`), false); valid {
		t.Fatal("summary accepted unpublished state filter")
	}
	for _, state := range []string{"OPEN", "RECOVERING", "RESOLVED"} {
		if _, valid := operationalQuery([]byte(`{"state":"`+state+`"}`), true); !valid {
			t.Fatal("published incident lifecycle rejected")
		}
	}
}

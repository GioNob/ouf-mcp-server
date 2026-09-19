package operational

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type ownerStatusFixture struct {
	identity orchestration.Identity
	called   bool
}

func TestOwnerPublicStatusCannotBeRaisedByCaller(t *testing.T) {
	h := NewOwnerAPI(aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"partial":false,"incidents":["secret"],"securityIncidentCount":10,"future":"secret"}`)})
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
	h := NewOwnerAPI(fixture)

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

package trustedclaims

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestRolesRejectMalformedAndUntrustedContext(t *testing.T) {
	for _, raw := range []string{"a a", "b a", " a", "a  b", "a,b", "x\nadmin", strings.Repeat("x", 129)} {
		r := httptest.NewRequest("POST", "/mcp", nil)
		r.Header.Set("X-OUF-Gateway-Verified", "true")
		r.Header.Set("X-OUF-External-Role-Refs", raw)
		if _, err := Read(r); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	r := httptest.NewRequest("POST", "/mcp", nil)
	if _, err := Read(r); err == nil {
		t.Fatal("untrusted origin")
	}
	r.Header.Set("X-OUF-Gateway-Verified", "true")
	r.Header.Add("X-OUF-External-Role-Refs", "a")
	r.Header.Add("X-OUF-External-Role-Refs", "b")
	if _, err := Read(r); err == nil {
		t.Fatal("duplicate headers")
	}
}

func TestRolesRequestOnly(t *testing.T) {
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-OUF-Gateway-Verified", "true")
	r.Header.Set("X-OUF-External-Role-Refs", "ouf:reader ouf:viewer")
	r.Header.Set("X-OUF-Authentication-Context-Ref", "1")
	claims, err := Read(r)
	if err != nil || len(claims.ExternalRoleRefs) != 2 || claims.Acr != "1" {
		t.Fatal(claims, err)
	}
	raw, err := json.Marshal(orchestration.Identity{Claims: claims})
	if err != nil || strings.Contains(string(raw), "ouf:viewer") {
		t.Fatal("claims persisted", string(raw), err)
	}
}

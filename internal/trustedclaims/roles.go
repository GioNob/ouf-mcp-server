// Package trustedclaims reads bounded claims reconstructed by the Gateway.
// The HTTP boundary must also be isolated to Gateway traffic; a header alone
// is not proof of origin. Claims are never accepted from tool arguments.
package trustedclaims

import (
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

var roleID = regexp.MustCompile(`^[A-Za-z0-9_:./-]{1,128}$`)

func Read(r *http.Request) (*orchestration.IdentityClaims, error) {
	if r.Header.Get("X-OUF-Gateway-Verified") != "true" {
		return nil, errors.New("trusted Gateway context required")
	}
	values := r.Header.Values("X-OUF-External-Role-Refs")
	if len(values) > 1 {
		return nil, errors.New("duplicate role context")
	}
	raw := r.Header.Get("X-OUF-External-Role-Refs")
	if len(raw) > 4096 {
		return nil, errors.New("role context too large")
	}
	roles := strings.Fields(raw)
	if len(roles) > 32 || strings.Join(roles, " ") != raw || !sort.StringsAreSorted(roles) {
		return nil, errors.New("invalid role context")
	}
	for n, role := range roles {
		if !roleID.MatchString(role) || (n > 0 && roles[n-1] == role) {
			return nil, errors.New("invalid role reference")
		}
	}
	return &orchestration.IdentityClaims{ExternalRoleRefs: roles, Acr: r.Header.Get("X-OUF-Authentication-Context-Ref")}, nil
}

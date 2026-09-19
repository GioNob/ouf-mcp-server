package kernel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type mutableRoleBundle struct {
	bundle authorization.ActivePolicyBundle
}

func (s *mutableRoleBundle) FetchActive(context.Context) (authorization.ActivePolicyBundle, error) {
	return s.bundle, nil
}

// Exercise both Go HTTP boundaries with the real policy evaluator. Signed
// token/proof transport and forged header stripping are Gateway CI gates.
func TestRoleAssignmentAndRevocationAtBothBoundaries(t *testing.T) {
	now := time.Now().UTC()
	source := &mutableRoleBundle{authorization.ActivePolicyBundle{BundleID: "roles", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{
		BundleID: "roles", Version: 1, PublishedAt: now,
		Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: "ouf.system.status", Operation: "READ", RequiredScope: "operations.status.read", AllowedActors: []string{"HUMAN"}}},
		Grants:       []authorization.Grant{{GrantID: "viewer", CapabilityID: "ouf.system.status", TenantID: "tenant-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour), Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ExternalRoleRef: "ouf:viewer", ResourceType: "capability", AllowedDetailLevels: []string{"PUBLIC_OPERATIONAL"}}}},
	}}}
	cache := authorization.NewCache(source)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner := &statusOwner{}
	ownerAPI := operational.NewOwnerAPI(owner).WithAuthorization(cache)
	dropOwnerRole := false
	handler := modernOnly(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := r.Context().Value(identityKey{}).(requestIdentity).Identity
		decision, err := cache.Authorize(r.Context(), orchestration.AuthorizationRequest{Identity: identity, CapabilityID: "ouf.system.status", OperationClass: "READ", Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: identity.TenantID, Attributes: map[string]string{"detailLevel": "PUBLIC_OPERATIONAL"}}})
		if err != nil || !decision.Allowed {
			w.WriteHeader(403)
			return
		}
		req := httptest.NewRequest("POST", "/api/internal/v1/mcp/operations/status", strings.NewReader("{}"))
		req.Header = r.Header.Clone()
		req.Header.Set("X-OUF-Service-Principal", "ouf-mcp-server")
		req.Header.Set("X-OUF-Authorization-Decision-Ref", decision.DecisionRef)
		if dropOwnerRole {
			req.Header.Del("X-OUF-External-Role-Refs")
		}
		ownerAPI.ServeHTTP(w, req)
	}))
	call := func(roles, tenant, scope string) int {
		// Role in body never establishes authority.
		r := httptest.NewRequest("POST", "/mcp", strings.NewReader(`{"externalRoleRefs":["ouf:viewer"]}`))
		for k, v := range map[string]string{"Mcp-Protocol-Version": ProtocolVersion, "Mcp-Method": "tools/call", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "human", "X-OUF-Tenant-ID": tenant, "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "ouf-api-gateway", "X-OUF-Granted-Scopes": scope, "X-OUF-External-Role-Refs": roles} {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	for _, tc := range []struct {
		name, roles, tenant, scope string
		want                       int
	}{
		{"assigned", "ouf:viewer", "tenant-a", "operations.status.read", 200},
		{"new-login-role-removed", "", "tenant-a", "operations.status.read", 403},
		{"different-role", "ouf:other", "tenant-a", "operations.status.read", 403},
		{"different-tenant", "ouf:viewer", "tenant-b", "operations.status.read", 403},
		{"missing-scope", "ouf:viewer", "tenant-a", "mcp.connect", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := call(tc.roles, tc.tenant, tc.scope); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
	dropOwnerRole = true
	if got := call("ouf:viewer", "tenant-a", "operations.status.read"); got != 403 {
		t.Fatalf("owner did not reauthorize: %d", got)
	}
	dropOwnerRole = false
	source.bundle.BundleVersion = 2
	source.bundle.Bundle.Version = 2
	source.bundle.ActivatedAt = now.Add(time.Millisecond)
	source.bundle.Bundle.PublishedAt = source.bundle.ActivatedAt
	source.bundle.Bundle.Grants = nil
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := call("ouf:viewer", "tenant-a", "operations.status.read"); got != 403 {
		t.Fatalf("revoked grant accepted: %d", got)
	}
	if owner.calls.Load() != 1 {
		t.Fatalf("unauthorized state read: %d", owner.calls.Load())
	}
}

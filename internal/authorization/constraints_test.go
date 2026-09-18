package authorization

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestOptionalConstraintRoundTrip(t *testing.T) {
	for _, raw := range []string{
		`{}`,
		`{"effect":"ALLOW","resourceType":"capability","allowedDetailLevels":["PUBLIC_OPERATIONAL"]}`,
		`{"effect":"DENY"}`,
		`{"externalRoleRef":null,"resourceId":null,"requiredAcr":null}`,
		`{"externalRoleRef":"operator","resourceId":"resource-a","requiredAcr":"mfa","requiredAmr":["otp"],"maxAuthenticationAgeSeconds":60}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var original, detached GrantConstraints
			if err := json.Unmarshal([]byte(raw), &original); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &detached); err != nil {
				t.Fatalf("valid constraints cannot survive cache copy: %s: %v", encoded, err)
			}
			if !reflect.DeepEqual(original, detached) {
				t.Fatalf("constraints changed: %#v -> %#v", original, detached)
			}
		})
	}
}

func TestConstraintDecoderStillRejectsExplicitBlanks(t *testing.T) {
	for _, field := range []string{"effect", "externalRoleRef", "resourceType", "resourceId", "requiredAcr"} {
		for _, value := range []string{`""`, `"  "`, `12`} {
			var constraints GrantConstraints
			if err := json.Unmarshal([]byte(`{"`+field+`":`+value+`}`), &constraints); err == nil {
				t.Fatalf("accepted invalid %s: %s", field, value)
			}
		}
	}
	var constraints GrantConstraints
	if err := json.Unmarshal([]byte(`{"unknownConstraint":true}`), &constraints); err == nil {
		t.Fatal("accepted unknown constraint")
	}
}

func TestCachePublishesDetachedConstrainedGrant(t *testing.T) {
	now := time.Date(2026, 9, 18, 22, 0, 0, 0, time.UTC)
	source := &bundleSourceFixture{bundle: validBundle(now)}
	var constraints GrantConstraints
	if err := json.Unmarshal([]byte(`{"effect":"ALLOW","resourceType":"capability","resourceAttributes":{"module":"MCP"},"allowedDetailLevels":["PUBLIC_OPERATIONAL"],"requiredAmr":["pwd"],"maxAuthenticationAgeSeconds":60}`), &constraints); err != nil {
		t.Fatal(err)
	}
	source.bundle.Bundle.Grants[0].Constraints = &constraints
	cache := NewCache(source)
	cache.now = func() time.Time { return now }
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := orchestration.AuthorizationRequest{
		Identity:     orchestration.Identity{PrincipalID: "user-a", TenantID: "tenant-a", ActorType: "HUMAN", AuthenticationContextRef: "1", Issuer: "issuer", Audience: "ouf", Scopes: []string{"operations.status.read"}, Claims: &orchestration.IdentityClaims{Amr: []string{"pwd"}, AuthenticatedAt: now.Add(-time.Second)}},
		Resource:     orchestration.ResourceContext{Attributes: map[string]string{"module": "MCP", "detailLevel": "PUBLIC_OPERATIONAL"}},
		CapabilityID: "ouf.system.status", OperationClass: "READ",
	}
	// Mutating transport-owned maps, slices and pointers must not alter the pinned policy.
	constraints.ResourceAttributes["module"] = "OTHER"
	constraints.AllowedDetailLevels[0] = "SECURITY_SENSITIVE"
	constraints.RequiredAmr[0] = "otp"
	*constraints.MaxAuthenticationAgeSeconds = 1
	if d, err := cache.Authorize(context.Background(), request); err != nil || !d.Allowed || d.PermittedDetailLevel != "PUBLIC_OPERATIONAL" {
		t.Fatalf("detached policy lost: %+v %v", d, err)
	}
	if err := cache.Refresh(context.Background()); err == nil {
		t.Fatal("accepted mutation under the same policy version")
	}
	for _, detail := range []string{"", "TENANT_OPERATIONAL", "SECURITY_SENSITIVE"} {
		request.Resource.Attributes["detailLevel"] = detail
		if d, err := cache.Authorize(context.Background(), request); err != nil || d.Allowed {
			t.Fatalf("detail %q escaped constraints: %+v %v", detail, d, err)
		}
	}
}

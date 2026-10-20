package kernel

import (
 "testing"
 "github.com/GioNob/ouf-mcp-server/internal/manifest"
)

func TestLatestReviewAuthorizationDoesNotInventVersionAuthority(t *testing.T) {
 c := manifest.Capability{CapabilityID:"ouf.managed-source.onboarding.read", Owner:"onboarding", OperationClass:"READ", RequiredAuthorization:"ouf.managed-source.onboarding.read"}
 _, r, err := sourceReviewAuthorization(c, []byte(`{"sourceId":"source-a","view":"LATEST"}`), "tenant-a")
 if err != nil || r.ResourceID != "source-a" || r.TenantID != "tenant-a" || len(r.Attributes) != 0 { t.Fatalf("latest resource is not bounded to source/tenant: %+v %v", r, err) }
 for _, args := range []string{
  `{"sourceId":"source-a","view":"LATEST","onboardingVersionId":"11111111-1111-4111-8111-111111111111"}`,
  `{"sourceId":"source-a","view":"LATEST","parentVersionId":"11111111-1111-4111-8111-111111111111"}`,
  `{"sourceId":"source-a","view":"UNKNOWN"}`,
  `{"view":"LATEST"}`,
 } {
  if _, _, err := sourceReviewAuthorization(c, []byte(args), "tenant-a"); err == nil { t.Fatalf("ambiguous latest context accepted: %s", args) }
 }
 for _, id := range []string{"ouf.managed-source.onboarding.review.prepare","ouf.onboarding.revision.prepare","ouf.onboarding.revision.read"} {
  c.CapabilityID=id; c.RequiredAuthorization=id
  if isSourcePreparationCapability(id) { c.OperationClass="COMMAND" } else { c.OperationClass="READ" }
  if _, _, err := sourceReviewAuthorization(c, []byte(`{"sourceId":"source-a","view":"LATEST"}`), "tenant-a"); err == nil { t.Fatalf("latest leaked into %s", id) }
 }
}

package authorization

import (
 "encoding/json"
 "os"
 "testing"
 "time"
 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// Same vectors as the versioned Java SDK; proves evaluator semantics, not IAM or cache freshness.
func TestCommonJavaGoConformance(t *testing.T) {
 data, err := os.ReadFile("testdata/authorization-conformance-v1.json")
 if err != nil { t.Fatal(err) }
 var fixture struct {
  Bundle PolicyBundle `json:"bundle"`
  Cases []struct {
   ID string `json:"id"`
   Principal struct {
    SubjectID string `json:"subjectId"`
    TenantID string `json:"tenantId"`
    ActorType string `json:"actorType"`
    ServicePrincipalID *string `json:"servicePrincipalId"`
    AuthenticationContextRef string `json:"authenticationContextRef"`
    Issuer string `json:"issuer"`
    Audience string `json:"audience"`
    Scopes []string `json:"scopes"`
   } `json:"principal"`
   Resource struct {
    ResourceType string `json:"resourceType"`
    ResourceID string `json:"resourceId"`
    TenantID string `json:"tenantId"`
    OrganizationID string `json:"organizationId"`
    Attributes map[string]string `json:"attributes"`
   } `json:"resource"`
   CapabilityID string `json:"capabilityId"`
   Operation string `json:"operation"`
   Now time.Time `json:"now"`
   DecisionCode string `json:"decisionCode"`
  } `json:"cases"`
 }
 if err := json.Unmarshal(data, &fixture); err != nil { t.Fatal(err) }
 for _, c := range fixture.Cases {
  t.Run(c.ID, func(t *testing.T) {
   p := c.Principal
   identity := orchestration.Identity{PrincipalID:p.SubjectID,TenantID:p.TenantID,ActorType:p.ActorType,AuthenticationContextRef:p.AuthenticationContextRef,Issuer:p.Issuer,Audience:p.Audience,Scopes:p.Scopes}
   if p.ServicePrincipalID != nil { identity.ServicePrincipalID = *p.ServicePrincipalID }
   r := c.Resource
   resource := orchestration.ResourceContext{ResourceType:r.ResourceType,ResourceID:r.ResourceID,TenantID:r.TenantID,OrganizationID:r.OrganizationID,Attributes:r.Attributes}
   decision := evaluate(fixture.Bundle,identity,resource,c.CapabilityID,c.Operation,c.Now)
   if decision.DecisionCode != c.DecisionCode || decision.Allowed != (c.DecisionCode == "ALLOW") || decision.BundleID != "conformance" || decision.BundleVersion != 7 { t.Fatalf("decision mismatch: %+v",decision) }
  })
 }
}

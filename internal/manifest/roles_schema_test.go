package manifest

import (
 "encoding/json"
 "testing"
 "github.com/google/jsonschema-go/jsonschema"
)

func TestApplicationRoleInputsRemainClosedAndProposalOnly(t *testing.T) {
 snapshot, err := Load(); if err != nil { t.Fatal(err) }
 schemas := map[string]*jsonschema.Resolved{}
 for _, c := range snapshot.Capabilities {
  if c.CapabilityID != "authorization.permissions.read" && c.CapabilityID != "authorization.permissions.propose" { continue }
  if c.CapabilityID == "authorization.permissions.propose" && c.MCPClass != "MCP_PROPOSAL_ONLY" { t.Fatal("role changes must remain proposals") }
  var s jsonschema.Schema
  if err := json.Unmarshal(c.InputSchema, &s); err != nil { t.Fatal(err) }
  resolved, err := s.Resolve(nil); if err != nil { t.Fatal(err) }; schemas[c.CapabilityID] = resolved
 }
 cases := []struct{cap, input string; valid bool}{
  {"read", `{"view":"ROLES"}`, true},
  {"read", `{"subjectId":"person"}`, true},
  {"read", `{"view":"ROLES","subjectId":"person"}`, false},
  {"read", `{"view":"ROLES","tenantId":"other"}`, false},
  {"propose", `{"operation":"REPLACE_ROLES","reason":"review","roleCatalogue":{"issuer":"https://iam.example","roles":[],"assignments":[]}}`, true},
  {"propose", `{"operation":"REPLACE_ROLES","reason":"review","grantId":"bypass","roleCatalogue":{"issuer":"https://iam.example","roles":[],"assignments":[]}}`, false},
  {"propose", `{"operation":"REPLACE_ROLES","reason":"review","roleCatalogue":{"issuer":"https://iam.example","roles":[],"assignments":[{"assignmentId":"a","roleId":"operator","subjectId":"p","externalRoleRef":"role","validFrom":"2026-09-20T00:00:00Z","validUntil":"2027-09-20T00:00:00Z"}]}}`, false},
  {"propose", `{"operation":"REPLACE_ROLES","reason":"review","roleCatalogue":{"issuer":"https://iam.example","roles":[],"assignments":[{"assignmentId":"a","roleId":"operator","subjectId":"p","externalRoleRef":null,"validFrom":"2026-09-20T00:00:00Z","validUntil":"2027-09-20T00:00:00Z"}]}}`, true},
  {"propose", `{"operation":"REVOKE","grantId":"old","reason":"review"}`, true},
 }
 for _, tc := range cases { var v any; if err := json.Unmarshal([]byte(tc.input), &v); err != nil { t.Fatal(err) }; s:=schemas["authorization.permissions."+tc.cap]; if s==nil {t.Fatal("missing schema")}; err:=s.Validate(v); if (err==nil)!=tc.valid {t.Fatalf("input %s: %v",tc.input,err)} }
}

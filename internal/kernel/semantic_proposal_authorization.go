package kernel

import (
 "encoding/json"
 "fmt"
 "github.com/GioNob/ouf-mcp-server/internal/manifest"
 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func semanticProposalAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
 required := map[string]string{"ouf.semantic.propose":"ouf.semantic.propose", "ouf.semantic.review.prepare":"ouf.semantic.review.prepare", "ouf.semantic.approval.request":"ouf.semantic.approval.request", "ouf.semantic.proposal.read":"ouf.semantic.propose"}[c.CapabilityID]
 if required == "" || c.Owner != "semantic" || c.RequiredAuthorization != required {
  return "", orchestration.ResourceContext{}, fmt.Errorf("invalid semantic proposal manifest binding")
 }
 var input struct { RevisionID string `json:"revisionId"` }
 if err := json.Unmarshal(args, &input); err != nil { return "", orchestration.ResourceContext{}, err }
 return required, orchestration.ResourceContext{ResourceType:"semantic.proposalcommand", ResourceID:input.RevisionID, TenantID:tenant, Attributes:map[string]string{"module":"SEMANTIC"}}, nil
}

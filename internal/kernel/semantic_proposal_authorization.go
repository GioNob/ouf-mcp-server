package kernel

import (
	"encoding/json"
	"fmt"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func semanticProposalAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	required := c.CapabilityID
	if !isSemanticProposalCapability(required) || c.Owner != "semantic" || c.RequiredAuthorization != required {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid semantic proposal manifest binding")
	}
	var input struct {
		RevisionID    string `json:"revisionId"`
		ReviewBatchID string `json:"reviewBatchId"`
		Item          struct {
			RevisionID string `json:"revisionId"`
		} `json:"item"`
		RDFTurtle string `json:"rdfTurtle"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return "", orchestration.ResourceContext{}, err
	}
	if len(input.RDFTurtle) > 49152 {
		return "", orchestration.ResourceContext{}, fmt.Errorf("semantic Turtle input exceeds byte bound")
	}
	resourceID := input.RevisionID
	if resourceID == "" {
		resourceID = input.Item.RevisionID
	}
	if resourceID == "" {
		resourceID = input.ReviewBatchID
	}
	return required, orchestration.ResourceContext{ResourceType: "semantic.proposalcommand", ResourceID: resourceID, TenantID: tenant, Attributes: map[string]string{"module": "SEMANTIC"}}, nil
}

// Consultation is a distinct deployed READ contract, not a proposal command.
func isSemanticProposalCapability(id string) bool {
	switch id {
	case "ouf.semantic.discovery.adopt", "ouf.semantic.propose", "ouf.semantic.review.prepare", "ouf.semantic.approval.request", "ouf.semantic.proposal.read":
		return true
	}
	return false
}

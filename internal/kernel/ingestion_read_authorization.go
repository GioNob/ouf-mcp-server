package kernel

import (
	"encoding/json"
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// Collection preflight matches the owner's operational resource contract.
// A source filter supplies scope; it is not authority. Owners still authorize
// each returned row/job with their own current policy and tenant context.
func ingestionReadAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	if c.CapabilityID != "ouf.ingestion.status" && c.CapabilityID != "ouf.ingestion.history" {
		return "", orchestration.ResourceContext{}, nil
	}
	if c.Owner != "ingestion" || c.OperationClass != "READ" || c.RequiredAuthorization != "ingestion.operations.read" {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid ingestion manifest binding")
	}
	var input operationalInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", orchestration.ResourceContext{}, err
	}
	attributes := map[string]string{"module": "INGESTION", "detailLevel": "TENANT_OPERATIONAL"}
	if input.SourceID != "" {
		attributes["sourceRef"] = input.SourceID
	}
	return c.RequiredAuthorization, orchestration.ResourceContext{
		ResourceType: "operational", TenantID: tenant, Attributes: attributes,
	}, nil
}

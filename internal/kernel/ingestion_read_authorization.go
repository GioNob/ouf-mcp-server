package kernel

import (
	"encoding/json"
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

// The preflight uses the known operational resource. Ingestion authorizes
// the actual row and its source/job attributes before returning any data.
func ingestionReadAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	var permission string
	switch c.CapabilityID {
	case "ouf.ingestion.status", "ouf.ingestion.history":
		permission = "ingestion.operations.read"
	case "ouf.operations.explain":
		permission = "operations.incident.explain"
	default:
		return "", orchestration.ResourceContext{}, nil
	}
	if c.Owner != "ingestion" || c.OperationClass != "READ" || c.RequiredAuthorization != permission {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid ingestion manifest binding")
	}
	var input operationalInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", orchestration.ResourceContext{}, err
	}
	resource := orchestration.ResourceContext{
		ResourceType: "operational", TenantID: tenant,
		Attributes: map[string]string{"module": "INGESTION", "detailLevel": "TENANT_OPERATIONAL"},
	}
	if c.CapabilityID == "ouf.operations.explain" {
		id, err := uuid.Parse(input.IncidentID)
		if err != nil || id == uuid.Nil {
			return "", orchestration.ResourceContext{}, fmt.Errorf("invalid ingestion incident ID")
		}
		resource.ResourceID = id.String()
	} else if input.SourceID != "" {
		resource.Attributes["sourceRef"] = input.SourceID
	}
	return permission, resource, nil
}

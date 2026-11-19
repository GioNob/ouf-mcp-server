package kernel

import (
	"encoding/json"
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

// This is collection-free pre-admission for retained job metadata only.
// The owner independently proves the exact asset/job and tenant/subject custody.
// No caller-supplied CSV label, policy or grant is accepted.
func managedProfileJobAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	if c.CapabilityID != "ouf.managed-source.preview" {
		return "", orchestration.ResourceContext{}, nil
	}
	var input map[string]json.RawMessage
	if err := json.Unmarshal(args, &input); err != nil {
		return "", orchestration.ResourceContext{}, err
	}
	if _, selected := input["jobId"]; !selected {
		return "", orchestration.ResourceContext{}, nil
	}
	if c.Owner != "onboarding" || c.OperationClass != "READ" || c.RequiredAuthorization != c.CapabilityID || tenant == "" || len(args) > 1024 || len(input) != 2 {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid managed profile job binding")
	}
	var asset, job string
	if json.Unmarshal(input["assetId"], &asset) != nil || json.Unmarshal(input["jobId"], &job) != nil {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid managed profile selection")
	}
	for _, value := range []string{asset, job} {
		parsed, err := uuid.Parse(value)
		if err != nil || parsed.String() != value {
			return "", orchestration.ResourceContext{}, fmt.Errorf("canonical managed profile identifiers required")
		}
	}
	return c.RequiredAuthorization, orchestration.ResourceContext{
		ResourceType: "managed-profile-job", ResourceID: job, TenantID: tenant,
		Attributes: map[string]string{
			"module": "ONBOARDING", "assetRef": asset, "jobRef": job, "detailLevel": "TENANT_OPERATIONAL",
		},
	}, nil
}

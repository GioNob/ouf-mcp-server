package kernel

import (
	"encoding/json"
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func isSourceReviewCapability(id string) bool {
	return id == "ouf.managed-source.onboarding.review.prepare" || id == "ouf.managed-source.onboarding.read"
}

func sourceReviewAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	if !isSourceReviewCapability(c.CapabilityID) || c.Owner != "onboarding" || c.RequiredAuthorization != c.CapabilityID {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review manifest binding")
	}
	expectedOperation := "READ"
	if c.CapabilityID == "ouf.managed-source.onboarding.review.prepare" {
		expectedOperation = "COMMAND"
	}
	if c.OperationClass != expectedOperation {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review operation")
	}
	var input struct {
		SourceID  string `json:"sourceId"`
		VersionID string `json:"onboardingVersionId"`
	}
	if err := json.Unmarshal(args, &input); err != nil || input.SourceID == "" || input.VersionID == "" {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review context")
	}
	return c.RequiredAuthorization, orchestration.ResourceContext{ResourceType: "source", ResourceID: input.SourceID, TenantID: tenant, Attributes: map[string]string{"onboardingVersionId": input.VersionID}}, nil
}

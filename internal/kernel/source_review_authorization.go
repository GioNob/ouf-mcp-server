package kernel

import (
	"encoding/json"
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func isSourceReviewCapability(id string) bool {
	return id == "ouf.managed-source.onboarding.review.prepare" || id == "ouf.managed-source.onboarding.read" || id == "ouf.onboarding.revision.prepare" || id == "ouf.onboarding.revision.read"
}

func sourceReviewAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	if !isSourceReviewCapability(c.CapabilityID) || c.Owner != "onboarding" || c.RequiredAuthorization != c.CapabilityID {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review manifest binding")
	}
	expectedOperation := "READ"
	if isSourcePreparationCapability(c.CapabilityID) {
		expectedOperation = "COMMAND"
	}
	if c.OperationClass != expectedOperation {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review operation")
	}
	var input struct {
		SourceID  string `json:"sourceId"`
		VersionID string `json:"onboardingVersionId"`
		ParentID  string `json:"parentVersionId"`
		View string `json:"view"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review context")
	}
	if input.View != "" {
		if c.CapabilityID != "ouf.managed-source.onboarding.read" || input.View != "LATEST" || input.SourceID == "" || input.VersionID != "" || input.ParentID != "" {
			return "", orchestration.ResourceContext{}, fmt.Errorf("invalid latest source review context")
		}
		// Coarse source authorization only. Onboarding selects the durable version
		// and rechecks its current exact-version policy and tenant/subject ownership.
		return c.RequiredAuthorization, orchestration.ResourceContext{ResourceType: "source", ResourceID: input.SourceID, TenantID: tenant}, nil
	}
	if input.VersionID == "" && (c.CapabilityID == "ouf.onboarding.revision.prepare" || c.CapabilityID == "ouf.onboarding.revision.read") {
		input.VersionID = input.ParentID
	}
	if input.SourceID == "" || input.VersionID == "" {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid source review context")
	}
	return c.RequiredAuthorization, orchestration.ResourceContext{ResourceType: "source", ResourceID: input.SourceID, TenantID: tenant, Attributes: map[string]string{"onboardingVersionId": input.VersionID}}, nil
}

func isSourcePreparationCapability(id string) bool {
	return id == "ouf.managed-source.onboarding.review.prepare" || id == "ouf.onboarding.revision.prepare"
}

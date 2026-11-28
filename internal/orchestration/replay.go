package orchestration

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Only these public owner UUIDs are retained at ordinary reconciliation.
// Response bodies, evidence locators and backend correlation IDs stay private.
func replayResultID(in Invocation, body []byte) *uuid.UUID {
	var field string
	switch in.CapabilityID {
	case HandoffIssueCapability:
		field = "handoffId"
	case "ouf.managed-source.file.upload":
		if in.Upload == nil {
			return nil // picker status has its own current owner read
		}
		field = "assetId"
	case "ouf.managed-source.file.profile":
		field = "jobId"
	case "ouf.managed-source.onboarding.create":
		field = "onboardingVersionId"
	case "ouf.semantic.discovery":
		field = "requestId"
	case "ouf.semantic.propose":
		if in.BatchEffectFence != "" {
			// The batch outbox retains its own stable owner key and receipt.
			return nil
		}
		var draft struct{ Status string }
		if json.Unmarshal(body, &draft) != nil || draft.Status != "DRAFT" {
			return nil
		}
		field = "revisionId"
	default:
		return nil
	}
	var receipt map[string]json.RawMessage
	if json.Unmarshal(body, &receipt) != nil {
		return nil
	}
	if in.Upload != nil {
		var status string
		if json.Unmarshal(receipt["status"], &status) != nil || (status != "STAGED" && status != "PROFILED" && status != "QUARANTINED") {
			return nil
		}
	}
	var id uuid.UUID
	if json.Unmarshal(receipt[field], &id) != nil || id == uuid.Nil {
		return nil
	}
	return &id
}

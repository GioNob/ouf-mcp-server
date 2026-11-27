package orchestration

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Only a successful, key-bound choice can finish a previously observed empty
// page. For a mapping, carry only the names explicitly excluded by the caller.
type ColumnDecision struct {
	Key, Asset, Profile string
	Excluded            []string
	Proposal            bool
}

func columnDecision(in Invocation) *ColumnDecision {
	if in.CapabilityID != "ouf.semantic.propose" && in.CapabilityID != "ouf.managed-source.onboarding.create" {
		return nil
	}
	if uuid.Validate(in.IdempotencyKey) != nil {
		return nil
	}
	d := &ColumnDecision{Key: in.IdempotencyKey, Proposal: in.CapabilityID == "ouf.semantic.propose"}
	if d.Proposal {
		return d
	}
	var mapping struct {
		AssetID, ProfileID string
		Fields             []struct{ FieldName, ExtractionDecision string }
	}
	if json.Unmarshal(in.Arguments, &mapping) != nil {
		return nil
	}
	d.Asset, d.Profile = mapping.AssetID, mapping.ProfileID
	for _, field := range mapping.Fields {
		if field.ExtractionDecision == "EXCLUDE" {
			d.Excluded = append(d.Excluded, field.FieldName)
		}
	}
	return d
}

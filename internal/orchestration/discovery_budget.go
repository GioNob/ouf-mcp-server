package orchestration

import (
	"errors"
	"time"
)

const DiscoveryBudgetProfile = "ouf.semantic-discovery-budget.v1"

func isDiscoveryOperation(capability, operation string) bool {
	switch capability {
	case "ouf.semantic.discovery", "ouf.semantic.discovery.adopt":
		return operation == "COMMAND"
	case "ouf.semantic.discovery.status", "ouf.semantic.discovery.candidates", "ouf.semantic.proposal.read":
		return operation == "READ"
	}
	return false
}

// Two consecutive four-call rounds plus a status poll and a receipt read per round.
func discoveryWindowBudget(in AdmissionRequest) (string, Cost, error) {
	maxObjects := int64(100)
	if in.CapabilityID == "ouf.semantic.discovery.adopt" || in.CapabilityID == "ouf.semantic.proposal.read" {
		maxObjects = 1
	}
	if !isDiscoveryOperation(in.CapabilityID, in.OperationClass) || in.ManifestChecksum == "" ||
		in.Window != time.Minute || in.RetryThreshold != 3 || in.Maximum.ToolCalls != 1 ||
		in.Maximum.ResultBytes < 1 || in.Maximum.ResultBytes > 262144 ||
		in.Maximum.DistinctObjects < 0 || in.Maximum.DistinctObjects > maxObjects {
		return "", Cost{}, errors.New("unsupported discovery budget")
	}
	return in.ManifestChecksum + ":" + DiscoveryBudgetProfile, Cost{ToolCalls: 12, ResultBytes: 3 << 20, DistinctObjects: 1200}, nil
}

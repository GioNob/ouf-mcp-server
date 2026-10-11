package orchestration

import (
	"errors"
	"time"
)

const OperationalControlBudgetProfile = "ouf.operational-control-budget.v1"

func IsOperationalProducerRead(capability, operation string) bool {
	if operation != "READ" {
		return false
	}
	switch capability {
	case "ouf.ingestion.operations.summary", "ouf.ingestion.operations.incidents", "ouf.gateway.operations.summary", "ouf.gateway.operations.incidents":
		return true
	default:
		return false
	}
}

func IsOperationalControlRead(capability, operation string) bool {
	if operation != "READ" {
		return false
	}
	switch capability {
	case "ouf.operations.summary", "ouf.operations.incidents", "ouf.ingestion.operations.summary", "ouf.ingestion.operations.incidents", "ouf.gateway.operations.summary", "ouf.gateway.operations.incidents":
		return true
	default:
		return false
	}
}

// These are finite server-owned control-plane limits, independent from
// business execution/recovery sessions. Equivalent-query retry guards stay 3.
// Keeping the window key distinct reserves control capacity without raising
// an existing business budget or letting a new job/correlation reset either.
func AdmissionWindowBudget(in AdmissionRequest) (string, Cost, error) {
	if in.BudgetProfile == "" {
		return in.ManifestChecksum, Cost{ToolCalls: in.Maximum.ToolCalls * int64(in.RetryThreshold), ResultBytes: in.Maximum.ResultBytes * int64(in.RetryThreshold), DistinctObjects: in.Maximum.DistinctObjects * int64(in.RetryThreshold)}, nil
	}
	if in.BudgetProfile != OperationalControlBudgetProfile || !IsOperationalControlRead(in.CapabilityID, in.OperationClass) ||
		in.Window != time.Minute || in.RetryThreshold != 3 || in.Maximum.ToolCalls != 1 || in.Maximum.ResultBytes < 1 ||
		in.Maximum.ResultBytes > 512<<10 || in.Maximum.DistinctObjects < 0 || in.Maximum.DistinctObjects > 100 {
		return "", Cost{}, errors.New("unsupported operational control budget")
	}
	return in.ManifestChecksum + ":" + OperationalControlBudgetProfile, Cost{ToolCalls: 24, ResultBytes: 12 << 20, DistinctObjects: 2400}, nil
}

package orchestration

import "errors"

const (
	ReviewBatchReadBudgetProfile = "ouf.semantic-review-batch-read.v1"
	ReviewBatchPageSize          = 10
)

// Reserve one read per poll interval for THS observations and receipt pages.
// The separate window keeps those reads available after a full discovery round.
func reviewBatchReadWindowBudget(in AdmissionRequest) (string, Cost, error) {
	const calls = 12
	const maxBytes = 262144
	if in.CapabilityID != "ouf.semantic.proposal.read" || in.OperationClass != "READ" ||
		in.ManifestChecksum == "" || in.Window != AdmissionWindow || in.RetryThreshold != AdmissionRetryThreshold ||
		in.Maximum.ToolCalls != 1 || in.Maximum.ResultBytes < 1 || in.Maximum.ResultBytes > maxBytes ||
		in.Maximum.DistinctObjects != ReviewBatchPageSize {
		return "", Cost{}, errors.New("unsupported review batch read budget")
	}
	return in.ManifestChecksum + ":" + ReviewBatchReadBudgetProfile, Cost{
		ToolCalls: calls, ResultBytes: calls * maxBytes, DistinctObjects: calls * ReviewBatchPageSize,
	}, nil
}

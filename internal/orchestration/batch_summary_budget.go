package orchestration

import "errors"

const BatchSummaryBudgetProfile = "ouf.analysis-batch-summary.v1"

// The summary can consume a full page on every admitted read. Isolate that
// accounting from the smaller owner envelopes used to submit and open a review.
func batchSummaryWindowBudget(in AdmissionRequest, calls int64) (string, Cost, error) {
	const pageBytes, pageRows = 8192, 8
	if in.CapabilityID != "ouf.source.analysis.read" || in.OperationClass != "READ" ||
		in.ManifestChecksum == "" || in.Window != AdmissionWindow || in.RetryThreshold != AdmissionRetryThreshold ||
		in.Maximum != (Cost{ToolCalls: 1, ResultBytes: pageBytes, DistinctObjects: pageRows}) {
		return "", Cost{}, errors.New("unsupported analysis batch summary budget")
	}
	return in.ManifestChecksum + ":" + BatchSummaryBudgetProfile, Cost{
		ToolCalls: calls, ResultBytes: calls * pageBytes, DistinctObjects: calls * pageRows,
	}, nil
}

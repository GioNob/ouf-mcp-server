package orchestration

import "testing"

func TestBatchSummaryWindowDoesNotChargeReviewAdmission(t *testing.T) {
	page := AdmissionRequest{
		BudgetProfile: BatchSummaryBudgetProfile, CapabilityID: "ouf.source.analysis.read", OperationClass: "READ",
		ManifestChecksum: "manifest", Window: AdmissionWindow, RetryThreshold: AdmissionRetryThreshold,
		CallsPerMinute: 4, Maximum: Cost{ToolCalls: 1, ResultBytes: 8192, DistinctObjects: 8},
	}
	key, limit, err := AdmissionWindowBudget(page)
	if err != nil || limit != (Cost{ToolCalls: 4, DistinctObjects: 32, ResultBytes: 32768}) {
		t.Fatal(key, limit, err)
	}
	command := page
	command.BudgetProfile, command.CapabilityID, command.OperationClass = "", "ouf.semantic.approval.request", "COMMAND"
	command.Maximum = Cost{ToolCalls: 1, ResultBytes: 262144, DistinctObjects: ReviewBatchPageSize}
	commandKey, commandLimit, err := AdmissionWindowBudget(command)
	if err != nil || commandKey == key || commandLimit != (Cost{ToolCalls: 4, DistinctObjects: 30, ResultBytes: 786432}) {
		t.Fatal(commandKey, commandLimit, err)
	}
	command.BudgetProfile = BatchSummaryBudgetProfile
	if _, _, err := AdmissionWindowBudget(command); err == nil {
		t.Fatal("a review command used the summary window")
	}
	page.Maximum.DistinctObjects++
	if _, _, err := AdmissionWindowBudget(page); err == nil {
		t.Fatal("admitted a summary page beyond the batch limit")
	}
}

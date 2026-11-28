package orchestration

import (
	"encoding/json"
	"testing"
)

func TestReviewBatchReadUsesIndependentAdmissionWindow(t *testing.T) {
	request := AdmissionRequest{
		BudgetProfile: DiscoveryBudgetProfile, CapabilityID: "ouf.semantic.proposal.read", OperationClass: "READ",
		ManifestChecksum: "manifest", Window: AdmissionWindow, RetryThreshold: AdmissionRetryThreshold,
		Maximum: Cost{ToolCalls: 1, ResultBytes: 262144, DistinctObjects: 1},
	}
	discoveryKey, _, err := AdmissionWindowBudget(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Maximum.DistinctObjects = 10
	if _, _, err := AdmissionWindowBudget(request); err == nil {
		t.Fatal("the discovery window admitted a review batch page")
	}
	request.BudgetProfile = ReviewBatchReadBudgetProfile
	key, budget, err := AdmissionWindowBudget(request)
	if err != nil || key == discoveryKey || budget != (Cost{ToolCalls: 12, ResultBytes: 3 << 20, DistinctObjects: 120}) {
		t.Fatal(key, budget, err)
	}
	request.Maximum.DistinctObjects++
	if _, _, err := AdmissionWindowBudget(request); err == nil {
		t.Fatal("admitted a page above the owner contract limit")
	}
}

func TestReviewBatchReadRecognizesWaitSelector(t *testing.T) {
	in := Invocation{
		CapabilityID: "ouf.semantic.proposal.read", OperationClass: "READ",
		Arguments: json.RawMessage(`{"reviewBatchId":"11111111-1111-4111-8111-111111111111"}`), InternalWaitRead: true,
	}
	if isPollRead(in) {
		t.Fatal("receipt read can claim a review batch poll")
	}
	in.BudgetProfile = ReviewBatchReadBudgetProfile
	if !isPollRead(in) {
		t.Fatal("the signed wait's batch selector was rejected")
	}
	page := in
	page.InternalWaitRead = false
	if isPollRead(page) {
		t.Fatal("a direct batch read can claim a signed wait's poll")
	}
	in.Arguments = json.RawMessage(`{"reviewBatchId":"invalid"}`)
	if isPollRead(in) {
		t.Fatal("invalid batch selector was classified as a poll")
	}
}

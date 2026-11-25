package orchestration

import (
	"encoding/json"
	"time"
)

// Reserve room for a command burst alongside the default polling cadence.
const (
	AdmissionWindow                = time.Minute
	AdmissionRetryThreshold        = 3
	DefaultAdmissionCallsPerMinute = 15
	PollIntervalSeconds            = int(AdmissionWindow/time.Second) / (DefaultAdmissionCallsPerMinute - AdmissionRetryThreshold)
)

// A proven admission refusal before dispatch. This supplies timing, never authority.
type WindowBudgetExhaustedError struct {
	Cause             error
	Code              string
	Dimension         string
	WindowEnd         time.Time
	RetryAfterSeconds int64
}

func (e *WindowBudgetExhaustedError) Error() string              { return "orchestration budget exhausted" }
func (e *WindowBudgetExhaustedError) Unwrap() error              { return e.Cause }
func (e *WindowBudgetExhaustedError) BudgetWindowEnd() time.Time { return e.WindowEnd }

func NewWindowBudgetError(cause error, code, dimension string, now, end time.Time) *WindowBudgetExhaustedError {
	seconds := int64((end.Sub(now) + time.Second - 1) / time.Second)
	return &WindowBudgetExhaustedError{Cause: cause, Code: code, Dimension: dimension, WindowEnd: end.UTC(), RetryAfterSeconds: max(1, seconds)}
}

// Match only status selectors that the server itself can project. The legacy
// Gateway upload envelope carries a read when it contains an exact handoff ID.
func isPollRead(in Invocation) bool {
	if in.OperationClass == "COMMAND" && in.CapabilityID != "ouf.managed-source.file.upload" {
		return false
	}
	var selector struct{ HandoffID, JobID, SourceID, OnboardingVersionID, ProposalID, ReviewBatchID, RequestID, CommandKey string }
	if json.Unmarshal(in.Arguments, &selector) != nil {
		return false
	}
	switch in.CapabilityID {
	case "ouf.managed-source.file.upload":
		return in.Upload == nil && selector.HandoffID != ""
	case "ouf.managed-source.preview":
		return in.OperationClass == "READ" && selector.JobID != ""
	case "ouf.managed-source.onboarding.read":
		return in.OperationClass == "READ" && selector.SourceID != "" && selector.OnboardingVersionID != ""
	case "ouf.semantic.proposal.read":
		return in.OperationClass == "READ" && (selector.CommandKey != "" || selector.ProposalID != "" || selector.ReviewBatchID != "")
	case "ouf.onboarding.ths-links.ingestion-run-recovery", "ouf.onboarding.ths-links.semantic-review":
		return in.InternalWaitRead && in.OperationClass == "READ"
	case "authorization.proposal.read":
		return in.InternalWaitRead && in.OperationClass == "READ" && selector.ProposalID != ""
	case "ouf.ingestion.status":
		return in.OperationClass == "READ" && selector.SourceID != ""
	case "ouf.semantic.discovery.status":
		return in.OperationClass == "READ" && selector.RequestID != ""
	case "urban.acquisition.read":
		return in.OperationClass == "READ"
	case "urban.object.search":
		return in.OperationClass == "SEARCH" || in.OperationClass == "READ"
	}
	return false
}

package orchestration

import (
	"errors"
	"time"
)

// Server-owned read capacity selected for source mapping. This does not raise
// equivalent-query retry guards, command budgets or recovery-session permits.
const SemanticReadBudgetProfile = "ouf.semantic-read-budget.v1"

func IsSemanticRead(capability, operation string) bool {
	return operation == "READ" && (capability == "ouf.semantic.search" || capability == "ouf.semantic.consultation.read")
}

func semanticReadWindowBudget(in AdmissionRequest) (string, Cost, error) {
	if !IsSemanticRead(in.CapabilityID, in.OperationClass) ||
		in.ManifestChecksum == "" || in.Window != time.Minute || in.RetryThreshold != 3 ||
		in.Maximum.ToolCalls != 1 || in.Maximum.ResultBytes < 1 || in.Maximum.ResultBytes > 262144 ||
		in.Maximum.DistinctObjects < 0 || in.Maximum.DistinctObjects > 100 {
		return "", Cost{}, errors.New("unsupported semantic read budget")
	}
	return in.ManifestChecksum + ":" + SemanticReadBudgetProfile, Cost{ToolCalls: 60, ResultBytes: 15 << 20, DistinctObjects: 6000}, nil
}

// Only a refusal before dispatch due to this finite window carries retry timing.
// It grants no retry/authorization and never describes an ambiguous owner effect.
type SemanticReadBudgetExhaustedError struct {
	Dimension string
	WindowEnd time.Time
	RetryAfterSeconds int64
	Cause error
}

func (e *SemanticReadBudgetExhaustedError) Error() string { return "orchestration budget exhausted" }
func (e *SemanticReadBudgetExhaustedError) Unwrap() error { return e.Cause }

func NewSemanticReadBudgetExhaustedError(cause error, dimension string, now, end time.Time) error {
	seconds := int64((end.Sub(now) + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return &SemanticReadBudgetExhaustedError{Dimension: dimension, WindowEnd: end.UTC(), RetryAfterSeconds: seconds, Cause: cause}
}

package orchestration

import "time"

// An ordinary poll can follow a burst of other calls in the same policy
// window. Waiting one whole admission window also covers a full retry group,
// regardless of the phase of the database clock.
const (
	AdmissionWindow         = time.Minute
	AdmissionRetryThreshold = 3
	PollIntervalSeconds     = int(AdmissionWindow / time.Second)
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

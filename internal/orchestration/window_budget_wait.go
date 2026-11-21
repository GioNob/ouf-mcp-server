package orchestration

import "time"

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

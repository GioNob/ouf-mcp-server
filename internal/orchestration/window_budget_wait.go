package orchestration

import "time"

// A proven admission refusal before dispatch. This supplies timing, never authority.
type WindowBudgetExhaustedError struct {
	Cause     error
	WindowEnd time.Time
}

func (e *WindowBudgetExhaustedError) Error() string                    { return "orchestration budget exhausted" }
func (e *WindowBudgetExhaustedError) Unwrap() error                    { return e.Cause }
func (e *WindowBudgetExhaustedError) BudgetWindowEnd() time.Time       { return e.WindowEnd }
func (e *SemanticReadBudgetExhaustedError) BudgetWindowEnd() time.Time { return e.WindowEnd }

package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// The client emits only the installation's fixed HTTPS mandate operations.
// Human delegation exists only in request memory for issue.
type AnalysisMandatePort interface {
	MandateCall(context.Context, string, json.RawMessage, string, int64, time.Duration) (MandateResponse, error)
}

type MandateResponse struct {
	Status                                    int
	Body                                      []byte
	Decision, Correlation, RetryAfter, Effect string
}

var ErrMandateTransportUncertain = errors.New("analysis mandate outcome unavailable")

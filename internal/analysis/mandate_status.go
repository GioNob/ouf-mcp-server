package analysis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type DispatchResponse struct {
	orchestration.MandateResponse
	StaleCause string
}

func FetchMandateStatus(ctx context.Context, gateway orchestration.AnalysisMandatePort, id, generation string) (orchestration.MandateResponse, error) {
	raw, _ := json.Marshal(map[string]string{"mandateId": id, "generation": generation})
	return gateway.MandateCall(ctx, "status", raw, "", 4096, 3*time.Second)
}

// An old Gateway's ambiguous crossing requires affirmative evidence that
// the originating mandate or generation is no longer active.
func CrossingRevoked(ctx context.Context, gateway orchestration.AnalysisMandatePort, m Mandate, cause string) bool {
	if cause != "" {
		return cause == "REVOKED"
	}
	reply, err := FetchMandateStatus(ctx, gateway, m.ID, m.Generation)
	if err != nil || reply.Status != 200 {
		return false
	}
	status, ok := decodeMandateStatus(m.ID, reply.Body)
	return ok && (status.State != "ACTIVE" || status.Generation != m.Generation)
}

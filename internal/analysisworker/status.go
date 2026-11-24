package analysisworker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// The read-only control call uses the workload credential and carries no
// owner arguments. Bind positive evidence to the exact issued generation.
func observeMandate(ctx context.Context, gateway orchestration.AnalysisMandatePort, m analysis.Mandate) (state, decision, retry string) {
	raw, _ := json.Marshal(map[string]string{"mandateId": m.ID, "generation": m.Generation})
	reply, err := gateway.MandateCall(ctx, "status", raw, "", 4096, 3*time.Second)
	if errors.Is(err, orchestration.ErrMandateTransportUncertain) {
		return "UNAVAILABLE", "", ""
	}
	if err != nil {
		return "DRIFT", "", ""
	}
	var problem struct{ Code string }
	validProblem := analysis.ValidEvidenceJSON(reply.Body) && json.Unmarshal(reply.Body, &problem) == nil
	switch {
	case reply.Status == 404 && validProblem && problem.Code == "ANALYSIS_MANDATE_UNKNOWN":
		return "REVOKED", "", ""
	case reply.Status == 409 && validProblem && problem.Code == "ANALYSIS_STALE_EXECUTION":
		return "DRIFT", "", ""
	case reply.Status == 429 || reply.Status == 502 || reply.Status == 503 || reply.Status == 504:
		return "UNAVAILABLE", "", reply.RetryAfter
	case reply.Status != 200:
		return "DRIFT", "", ""
	}
	state, decision = analysis.MandateStatus(m, reply.Body)
	return state, decision, ""
}

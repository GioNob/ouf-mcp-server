package analysis

import (
	"context"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// Installation adapter verifies the current HUMAN grant for this exact
// decision, incident, version and finite session. No public tool selects it.
type RunRenewal struct {
	AnalysisID, IncidentID, Reason string
	ExpectedSeq                    int64
	Maximum                        RunCost
	Seconds                        int
}

type RenewalDecision struct {
	Ref        string
	ValidUntil time.Time
}

type RunRenewalAuthority interface {
	AuthorizeRunRenewal(context.Context, orchestration.Identity, RunRecord, RunRenewal) (RenewalDecision, error)
}

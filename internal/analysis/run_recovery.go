package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

var ErrRenewalUsed = errors.New("ANALYSIS_RENEWAL_USED")

type RunRenewal struct {
	AnalysisID      string    `json:"analysisId"`
	IncidentID      string    `json:"incidentId"`
	ProposalVersion int64     `json:"proposalVersion"`
	ProposalHash    string    `json:"proposalHash"`
	ExpectedSeq     int64     `json:"expectedSeq,string"`
	Reason          string    `json:"reason"`
	Increment       RunCost   `json:"increment"`
	Seconds         int       `json:"seconds"`
	ExpiresAt       time.Time `json:"expiresAt"`
}

func (w RunRenewal) SessionEnd(r RunRecord, now time.Time) time.Time {
	end := now.Add(time.Duration(w.Seconds) * time.Second)
	for _, bound := range []time.Time{w.ExpiresAt, r.Mandate.Expires, r.Started.Add(time.Duration(r.Limits.LifetimeSeconds) * time.Second)} {
		if bound.Before(end) {
			end = bound
		}
	}
	return end
}

// Only the adapter receives the opaque HUMAN delegation. The binding and
// its digest contain the logical scope and the exact finite decision.
type RunRenewalCheck struct {
	RunRenewal
	Contract   string `json:"contract"`
	Action     string `json:"action"`
	Issuer     string `json:"issuer"`
	Scope      Scope  `json:"scope"`
	Workload   string `json:"workload"`
	MandateID  string `json:"mandateId"`
	Generation string `json:"generation"`
	Fence      int64  `json:"fence,string"`
}

func (c RunRenewalCheck) Hash() (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return MandateHash(raw)
}

type RenewalDecision struct {
	RequestHash string    `json:"requestHash"`
	DecisionRef string    `json:"decisionRef"`
	GrantRef    string    `json:"grantRef"`
	ValidUntil  time.Time `json:"validUntil"`
}

type RunRenewalAuthority interface {
	// The Gateway renewal-check verifier authenticates the HUMAN and resolves
	// the current grant bound to this mandate, generation and proposal.
	AuthorizeRunRenewal(context.Context, orchestration.Identity, RunRenewalCheck) (RenewalDecision, error)
}

const (
	maxRenewalPercent = 25
	maxRenewalSeconds = 3600
)

func (l RunLimits) RenewalCeiling() RunCost {
	p := int64(l.RenewalPercent)
	if p == 0 {
		p = maxRenewalPercent
	}
	m := l.Maximum
	return RunCost{m.Calls * p / 100, m.Attempts * p / 100, m.Objects * p / 100, m.Bytes * p / 100, m.Millis * p / 100}
}

func (l RunLimits) RenewalDuration() int {
	if l.RenewalSeconds == 0 {
		return maxRenewalSeconds
	}
	return l.RenewalSeconds
}

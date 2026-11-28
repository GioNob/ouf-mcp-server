package analysis

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/google/uuid"
)

type BatchDecision struct {
	Ordinal     int             `json:"ordinal"`
	Action      string          `json:"action"`
	RequestID   string          `json:"requestId,omitempty"`
	CandidateID string          `json:"candidateId,omitempty"`
	Intent      string          `json:"intent,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	Proposal    json.RawMessage `json:"proposal,omitempty"`
	RevisionID  string          `json:"revisionId,omitempty"`
	Rejection   string          `json:"-"`
}

type BatchDecide struct {
	AnalysisID       string          `json:"analysisId"`
	BatchID          string          `json:"batchId"`
	ExpectedRevision int64           `json:"expectedRevision"`
	CommandKey       string          `json:"-"`
	Decisions        []BatchDecision `json:"decisions"`
	// InputFingerprint covers normalized submitted values, including rejected entries.
	InputFingerprint string `json:"-"`
}

type BatchDecisionResult struct {
	Ordinal    int    `json:"ordinal"`
	Action     string `json:"action"`
	Status     string `json:"status"`
	ReasonCode string `json:"reasonCode,omitempty"`
}

type BatchReceipt struct {
	AnalysisID string                `json:"analysisId"`
	BatchID    string                `json:"batchId"`
	Revision   int64                 `json:"revision"`
	Decisions  []BatchDecisionResult `json:"decisions"`
}

type BatchDecisionStore interface {
	DecideBatch(context.Context, Scope, string, BatchStart, BatchDecide) (BatchReceipt, error)
}

func (c *BatchDecide) Normalize() error {
	key, err := uuid.Parse(c.CommandKey)
	if ValidateID(c.AnalysisID) != nil || !CanonicalUUID(c.BatchID) || err != nil || key.String() != c.CommandKey ||
		key.Version() != 4 || key.Variant() != uuid.RFC4122 || c.ExpectedRevision < 0 ||
		len(c.Decisions) < 1 || len(c.Decisions) > BatchMaxItems {
		return ErrInvalid
	}
	slices.SortFunc(c.Decisions, func(a, b BatchDecision) int { return a.Ordinal - b.Ordinal })
	for n, d := range c.Decisions {
		if d.Ordinal < 0 || d.Ordinal >= 1024 || (n > 0 && c.Decisions[n-1].Ordinal == d.Ordinal) {
			return ErrInvalid
		}
	}
	return nil
}

func NormalizeBatchIntent(intent string) string { return strings.Join(strings.Fields(intent), " ") }

func (c BatchDecide) EffectKey(scope Scope, d BatchDecision) string {
	seed := Digest([]any{"ouf.analysis.batch.effect.v1", scope, c.AnalysisID, c.CommandKey, d.Ordinal, d.Action})
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(seed)).String()
}

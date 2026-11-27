package analysis

import (
	"context"
	"encoding/json"
)

// BatchOfferStore exposes the head and bounded decision projection. Pack runs
// locally in the transaction that records the new offer and its membership.
type BatchOfferStore interface {
	ReadBatch(ctx context.Context, scope Scope, id, checksum string, pack func(BatchOffer) ([]BatchOfferItem, error)) (BatchRead, error)
	ReadBatchColumn(ctx context.Context, scope Scope, id, checksum, batchID string, ordinal, offset int) (BatchOffer, BatchOfferItem, error)
}

type BatchRead struct {
	BatchStart
	Counts        Counts
	Phase, Reason string
	Offer         BatchOffer
}

type BatchOffer struct {
	ID, AnalysisID string
	Revision       int64
	Items          []BatchOfferItem
}

// BatchOfferItem includes the evidence version for cursor binding. Only Item
// is public. A later worker can publish evidence for other columns independently.
type BatchOfferItem struct {
	Item            BatchItem
	EvidenceVersion string
}

type BatchItem struct {
	Ordinal int    `json:"ordinal"`
	Name    string `json:"name"`
	BatchColumnMetadata
	Attempt           int              `json:"attempt"`
	RemainingAttempts int              `json:"remainingAttempts"`
	Intent            string           `json:"intent"`
	RequestID         string           `json:"requestId"`
	CandidateCount    int              `json:"candidateCount"`
	Candidates        []BatchCandidate `json:"candidates"`
	CandidateCursor   string           `json:"candidateCursor,omitempty"`
	Allowed           []string         `json:"allowed"`
	TruncatedFields   []string         `json:"truncatedFields,omitempty"`
	ReasonCode        string           `json:"reasonCode,omitempty"`
}

type BatchCandidate struct {
	ID              string   `json:"candidateId"`
	Label           string   `json:"label"`
	Term            string   `json:"term"`
	Score           float64  `json:"score"`
	TruncatedFields []string `json:"truncatedFields,omitempty"`
}

func (i BatchItem) Valid() bool {
	if !UsableColumnName(i.Name) || i.Intent == "" || i.DataType == "" || i.Examples == nil ||
		len(i.Examples) > 2 || !CanonicalUUID(i.RequestID) || i.Attempt < 1 || i.Attempt > 8 ||
		i.CandidateCount < len(i.Candidates) || (i.CandidateCount > 0 && len(i.Candidates) == 0) {
		return false
	}
	seen := make(map[string]bool, len(i.Candidates))
	for _, c := range i.Candidates {
		if !CanonicalUUID(c.ID) || c.Label == "" || c.Term == "" || seen[c.ID] {
			return false
		}
		seen[c.ID] = true
	}
	for _, example := range i.Examples {
		if !json.Valid(example) || example[0] == '{' || example[0] == '[' {
			return false
		}
	}
	return true
}

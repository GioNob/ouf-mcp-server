package analysis

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"
)

type batchOwnerCandidate struct {
	ID           string     `json:"candidateId"`
	OwnerID      string     `json:"candidate_id"`
	Label        string     `json:"label"`
	Term         string     `json:"term"`
	CanonicalURI string     `json:"canonical_uri"`
	Score        *float64   `json:"score"`
	OwnerExpiry  *time.Time `json:"expires_at"`
}

func batchCandidateResult(col BatchWorkColumn, raw []byte, now time.Time) BatchWorkResult {
	var receipt struct {
		RequestID, State string
		Candidates       []batchOwnerCandidate
	}
	blocked := batchBlocked(col, "DISCOVERY_EVIDENCE_UNAVAILABLE")
	if !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &receipt) != nil || receipt.RequestID != col.RequestID ||
		receipt.State != "SUCCEEDED" || receipt.Candidates == nil || len(receipt.Candidates) > 100 {
		return blocked
	}
	count := CompletedCount(col.RequestID, raw)
	if count != nil && col.CompletedCount != nil && *count != *col.CompletedCount {
		return blocked
	}
	if count == nil {
		count = col.CompletedCount
	}
	if count == nil || *count != len(receipt.Candidates) {
		return blocked
	}
	state := "NO_CANDIDATES"
	if *count == 0 {
		if !EmptyCandidatePage(col.RequestID, raw) {
			return blocked
		}
	} else {
		out, _ := DiscoveryEvidenceWithCompletion("semantic.discovery.candidates", col.RequestID, raw, count, now, time.Time{})
		if out.State != "CANDIDATES_READY" {
			return blocked
		}
		state = out.State
	}
	candidates := make([]BatchCandidate, 0, *count)
	for _, row := range receipt.Candidates {
		id, term, label := row.OwnerID, row.CanonicalURI, row.Label
		if id == "" {
			id = row.ID
		}
		if term == "" {
			term = row.Term
		}
		if label == "" {
			label = term
		}
		if (row.ID != "" && id != row.ID) || !CanonicalUUID(id) || term == "" || row.Score == nil ||
			*row.Score < 0 || *row.Score > 1 || (row.OwnerExpiry != nil && !row.OwnerExpiry.After(now)) {
			return blocked
		}
		candidates = append(candidates, BatchCandidate{ID: id, Label: label, Term: term, Score: *row.Score})
	}
	slices.SortFunc(candidates, func(a, b BatchCandidate) int {
		if a.Score != b.Score {
			return cmp.Compare(b.Score, a.Score)
		}
		if a.Term != b.Term {
			return cmp.Compare(a.Term, b.Term)
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return BatchWorkResult{Ordinal: col.Ordinal, State: "READY_FOR_DECISION", CompletedCount: count,
		Candidates: candidates, EvidenceVersion: Digest([]any{col.RequestID, candidates}),
		Outcome: Outcome{State: state, RequestID: col.RequestID, Count: count, ObservedAt: now.UTC().Format(time.RFC3339Nano)}}
}

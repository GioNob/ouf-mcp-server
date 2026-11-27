package analysis

import (
	"encoding/json"
	"slices"
	"unicode/utf8"
)

type BatchResult struct {
	ID    string           `json:"intentId"`
	State string           `json:"state"`
	More  bool             `json:"hasMore"`
	Hits  []PublishedMatch `json:"hits"`
}

func closedFields(raw json.RawMessage, names ...string) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != len(names) {
		return nil, false
	}
	for _, k := range names {
		if v := fields[k]; v == nil || string(v) == "null" {
			return nil, false
		}
	}
	return fields, true
}

func validPin(hit PublishedMatch, context bool) bool {
	if hit.SemanticID == "" || utf8.RuneCountInString(hit.SemanticID) > 2048 || hit.SemanticVersion == "" || utf8.RuneCountInString(hit.SemanticVersion) > 128 ||
		!CanonicalUUID(hit.RevisionID) || !CanonicalUUID(hit.PublicationSetID) {
		return false
	}
	var reason string
	switch hit.Score {
	case 100:
		reason = "EXACT_LABEL"
	case 95:
		reason = "EXACT_LOCAL_NAME"
	case 60:
		reason = "LABEL_PREFIX"
	default:
		return false
	}
	want := []string{reason, "PREFERRED_LANGUAGE"}
	if context {
		want = append(want, "CONTEXT_MATCH")
	}
	return slices.Equal(hit.Reasons, want)
}

// Validate every result before the first ordinal can change. A truncated
// AMBIGUOUS with one visible strong hit can hide a second strong hit.
func AcceptedBatch(task Task, raw []byte) ([]BatchResult, bool) {
	var req Batch
	if json.Unmarshal(task.Arguments, &req) != nil || !ValidEvidenceJSON(raw) {
		return nil, false
	}
	fields, ok := closedFields(raw, "contract", "results")
	var contract string
	var rows []json.RawMessage
	if !ok || json.Unmarshal(fields["contract"], &contract) != nil || contract != BatchContract ||
		json.Unmarshal(fields["results"], &rows) != nil || len(rows) != len(req.Intents) {
		return nil, false
	}
	out := make([]BatchResult, len(rows))
	for n, row := range rows {
		f, ok := closedFields(row, "intentId", "state", "hasMore", "hits")
		r := &out[n]
		if !ok || json.Unmarshal(row, r) != nil || r.ID != req.Intents[n].ID || len(r.Hits) > req.Limit || (r.More && len(r.Hits) != req.Limit) {
			return nil, false
		}
		var hits []json.RawMessage
		if json.Unmarshal(f["hits"], &hits) != nil {
			return nil, false
		}
		seen := map[string]bool{}
		strong, previous := 0, 101
		for h, hit := range r.Hits {
			if _, ok := closedFields(hits[h], "semanticId", "semanticVersion", "revisionId", "publicationSetId", "score", "reasons"); !ok {
				return nil, false
			}
			if !validPin(hit, len(req.Intents[n].Context) > 0) || seen[hit.RevisionID] || hit.Score > previous {
				return nil, false
			}
			if h > 0 && len(req.Intents[n].Languages) == 1 && hit.Score == previous {
				prior := r.Hits[h-1]
				if prior.SemanticID > hit.SemanticID || (prior.SemanticID == hit.SemanticID && prior.RevisionID > hit.RevisionID) {
					return nil, false
				}
			}
			seen[hit.RevisionID] = true
			previous = hit.Score
			if hit.Score >= 95 {
				strong++
			}
		}
		switch r.State {
		case "NONE":
			if len(hits) != 0 || r.More {
				return nil, false
			}
		case "MATCH":
			if strong != 1 {
				return nil, false
			}
		case "AMBIGUOUS":
			if len(hits) == 0 || (strong == 1 && !(r.More && len(hits) == 1)) {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return out, true
}

func SplitBatch(t Task, shrink bool) []Task {
	var b Batch
	if json.Unmarshal(t.Arguments, &b) != nil || len(b.Intents) == 0 {
		return nil
	}
	child := func(intents []Intent, limit int) Task {
		return batchTask(intents, limit, t.Groups, t.DiscoveryText, t.MaxBytes)
	}
	if n := len(b.Intents); n > 1 {
		return []Task{child(b.Intents[:n/2], b.Limit), child(b.Intents[n/2:], b.Limit)}
	}
	if shrink && b.Limit > 1 {
		return []Task{child(b.Intents, b.Limit-1)}
	}
	return nil
}

package analysis

import (
	"context"
)

type BatchSummaryChange struct {
	Ordinal int            `json:"ordinal"`
	Fields  map[string]any `json:"fields"`
}

type BatchReviewItem struct {
	RevisionID, State, PublicationSetID, SemanticID string
}

type BatchReviewStore interface {
	ObserveBatchReview(context.Context, Scope, string, string, string, []BatchReviewItem) error
}

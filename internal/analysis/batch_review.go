package analysis

import (
	"encoding/json"

	"github.com/google/uuid"
)

func BatchChoiceStage(stage string) bool {
	return stage == BatchAdopt || stage == BatchPropose || stage == BatchDraft || stage == BatchReviewInspect || ReviewEffectStage(stage)
}

func ReviewEffectStage(stage string) bool {
	return stage == BatchValidate || stage == BatchReviewAdd
}

func batchReviewKey(effectKey, stage string) string {
	key := uuid.NewSHA1(uuid.MustParse(effectKey), []byte(stage))
	key[6] = key[6]&0x0f | 0x40
	return key.String()
}

type BatchReviewInput struct {
	RevisionID         string `json:"revisionId"`
	ExpectedRowVersion int64  `json:"expectedRowVersion"`
	ValidationRunID    string `json:"validationRunId,omitempty"`
}

func batchReviewTransition(effect *BatchWorkResult, stage string, args any) {
	effect.State, effect.Stage, effect.EffectState = "PENDING", stage, "PENDING"
	effect.EffectNextArgs, _ = json.Marshal(args)
}

func batchPrepareReview(effect *BatchWorkResult, col BatchWorkColumn, read bool, raw []byte) bool {
	type projection struct {
		RowVersion, ExpectedRowVersion       *int64
		Status, PublicationSetID, SemanticID string
	}
	var row projection
	var receipt struct{ Result json.RawMessage }
	if json.Unmarshal(raw, &row) != nil {
		return false
	}
	if read && json.Unmarshal(raw, &receipt) == nil {
		var saved projection
		if json.Unmarshal(receipt.Result, &saved) == nil {
			if row.RowVersion == nil {
				row.RowVersion = saved.RowVersion
			}
			row.Status, row.PublicationSetID, row.ExpectedRowVersion = saved.Status, saved.PublicationSetID, saved.ExpectedRowVersion
			row.SemanticID = saved.SemanticID
		}
	}
	effect.SemanticID = row.SemanticID
	if row.Status == "ACTIVE" && CanonicalUUID(row.PublicationSetID) {
		effect.PublicationSetID = row.PublicationSetID
		return true
	}
	version := row.RowVersion
	if version == nil {
		version = row.ExpectedRowVersion
	}
	if version == nil || *version < 0 || *version > (1<<53)-1 {
		return false
	}
	batchReviewTransition(effect, BatchValidate, struct {
		BatchReviewInput
		CommandKey string `json:"commandKey"`
	}{BatchReviewInput: BatchReviewInput{RevisionID: effect.EffectRef, ExpectedRowVersion: *version},
		CommandKey: batchReviewKey(col.EffectKey, BatchValidate)})
	return true
}

func batchReviewEffect(job BatchWork, col BatchWorkColumn, read bool, raw []byte) (BatchWorkResult, bool) {
	var obs struct {
		Operation, Kind, ReviewBatchID, ReviewContextRef, State string
		Result                                                  json.RawMessage
		HistoricalSnapshot, OwnerStateMustBeRechecked           bool
	}
	if !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &obs) != nil {
		return BatchWorkResult{}, false
	}
	if job.Stage == BatchReviewAdd {
		if obs.Kind != "REVIEW_BATCH" || obs.ReviewContextRef != job.AnalysisID || !CanonicalUUID(obs.ReviewBatchID) {
			return BatchWorkResult{}, false
		}
		switch obs.State {
		case "COLLECTING", "UNDER_REVIEW", "DECIDED", "EXPIRED":
			effect := batchEffectResult(col, "APPLIED", "REVIEW_BATCH_ADDED", col.EffectRevision)
			effect.ReviewBatchID = obs.ReviewBatchID
			return effect, true
		}
		return BatchWorkResult{}, false
	}
	if read {
		if obs.Operation != "VALIDATE" || !obs.HistoricalSnapshot || !obs.OwnerStateMustBeRechecked {
			return BatchWorkResult{}, false
		}
		raw = obs.Result
	}
	var row struct {
		Status         string
		RevisionID     string `json:"revision_id"`
		ValidationRun  string `json:"validation_run_id"`
		CheckedVersion *int64 `json:"validated_row_version"`
		Revision       string `json:"revisionId"`
		Run            string `json:"validationRunId"`
		Version        *int64 `json:"rowVersion"`
	}
	if json.Unmarshal(raw, &row) != nil {
		return BatchWorkResult{}, false
	}
	if row.RevisionID == "" {
		row.RevisionID = row.Revision
	}
	if row.ValidationRun == "" {
		row.ValidationRun = row.Run
	}
	if row.CheckedVersion == nil {
		row.CheckedVersion = row.Version
	}
	if row.Status == "FAIL" && row.RevisionID == col.EffectRevision {
		return batchEffectResult(col, "REFUSED", "SEMANTIC_VALIDATION_FAILED", col.EffectRevision), true
	}
	var submitted BatchReviewInput
	if row.Status != "PASS" || row.RevisionID != col.EffectRevision || !CanonicalUUID(row.ValidationRun) ||
		row.CheckedVersion == nil || json.Unmarshal(col.EffectArguments, &submitted) != nil ||
		*row.CheckedVersion != submitted.ExpectedRowVersion {
		return BatchWorkResult{}, false
	}
	effect := batchEffectResult(col, "PENDING", "VALIDATED", col.EffectRevision)
	batchReviewTransition(&effect, BatchReviewAdd, map[string]any{
		"batchAction": "ADD", "reviewContextRef": job.AnalysisID,
		"item":       BatchReviewInput{RevisionID: col.EffectRevision, ExpectedRowVersion: *row.CheckedVersion, ValidationRunID: row.ValidationRun},
		"commandKey": batchReviewKey(col.EffectKey, BatchReviewAdd),
	})
	return effect, true
}

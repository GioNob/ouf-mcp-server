package nextstep

import (
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

const (
	BatchUnavailable            = "ANALYSIS_BATCH_NOT_AVAILABLE"
	BatchResultLimit            = "ANALYSIS_BATCH_RESULT_LIMIT"
	AnalysisRunning             = "ANALYSIS_RUNNING"
	ReviewBatchCollecting       = "SEMANTIC_REVIEW_BATCH_COLLECTING"
	ReviewBatchEmpty            = "SEMANTIC_REVIEW_BATCH_EMPTY"
	ReviewSingleRequired        = "SEMANTIC_REVIEW_SINGLE_REQUIRED"
	ReviewBatchItemsNotReady    = "SEMANTIC_REVIEW_BATCH_ITEMS_NOT_READY"
	ReviewBatch                 = "SEMANTIC_REVIEW_BATCH"
	ReviewBatchChangesRequested = "SEMANTIC_REVIEW_BATCH_CHANGES_REQUESTED"
	ReviewBatchExpired          = "SEMANTIC_REVIEW_BATCH_EXPIRED"
	ReviewBatchPublished        = "SEMANTIC_REVIEW_BATCH_PUBLISHED"
)

func BatchBlock(code string) Object {
	message := "Verifica le condizioni per proseguire l’analisi a lotti."
	switch code {
	case BatchUnavailable:
		message = "Questa operazione dell’analisi a lotti non è ancora disponibile."
	case BatchResultLimit:
		message = "La risposta dell’analisi a lotti supera il limite di 8 KiB."
	}
	return ManualBlock(code, message, "OPERATOR")
}

// BatchNext projects the continuation for a batch analysis result.
func (e Engine) BatchNext(tool string, input, obs Object, failed bool) Object {
	if failed {
		code, _ := get(obs, nil, "reasonCode|code|Code").(string)
		if !reasonPattern.MatchString(code) {
			code = "OWNER_REQUEST_FAILED"
		}
		if code == "NO_APPLICABLE_GRANT" {
			capability, _ := obs["requiredCapability"].(string)
			return PermissionDenied(capability, code)
		}
		return BatchBlock(code)
	}
	if tool != "source.file.preview" {
		return BatchBlock(BatchUnavailable)
	}
	const plan = "source.analysis.plan"
	rule := manifest.Rule{Tool: plan, Args: map[string]string{
		"assetId": "assetId", "profileId": "profileId", "schemaHash": "schemaHash",
		"mode": "=" + analysis.BatchMode, "commandKey": "#uuid",
	}}
	args := e.project(rule, obs, input)
	if e.Valid == nil || !e.Valid(plan, args) {
		return BatchBlock("ANALYSIS_PROFILE_CHANGED")
	}
	// The preview is complete for this projection. Its transport clock does
	// not delay a local plan command.
	e.Observed = orchestration.OwnerObservation{}
	return e.ownerCall("PLAN_COLUMN_DISCOVERY", plan, args, nil)
}

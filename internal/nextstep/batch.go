package nextstep

import (
	"time"

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

func (e Engine) batchFailure(obs Object) Object {
	code, _ := get(obs, nil, "reasonCode|code|Code").(string)
	if !reasonPattern.MatchString(code) {
		code = "OWNER_REQUEST_FAILED"
	}
	if code == "NO_APPLICABLE_GRANT" {
		capability, _ := obs["requiredCapability"].(string)
		return PermissionDenied(capability, code)
	}
	if code == "AUTHORIZATION_DENIED" || analysis.PermissionHold(code) || code == analysis.ErrEpochChanged.Error() {
		return ManualBlock(code, "Verifica l’autorità corrente nella THS.", "HUMAN_THS")
	}
	return BatchBlock(code)
}

func IsBatchStatusPoll(reason string) bool {
	switch reason {
	case AnalysisRunning, "SEM_DISCOVERY_RATE_LIMITED", "SEM_CANDIDATES_RATE_LIMITED", "SEM_BATCH_RATE_LIMITED":
		return true
	}
	return false
}

func (e Engine) BatchStatusNext(id, reason string, createdAt time.Time, observationDue ...time.Time) Object {
	const read = "source.analysis.read"
	args := Object{"analysisId": id}
	switch reason {
	case "ANALYSIS_CONTINUE":
		args["view"] = "BATCH"
	case "ANALYSIS_SUMMARY":
		args["view"] = "SUMMARY"
	case AnalysisRunning, "SEM_DISCOVERY_RATE_LIMITED", "SEM_CANDIDATES_RATE_LIMITED", "SEM_BATCH_RATE_LIMITED":
		args["view"] = "STATUS"
		if e.Valid == nil || !e.Valid(read, args) {
			return BatchBlock(BatchUnavailable)
		}
		maximum := e.WaitMaxSeconds
		if maximum == 0 {
			maximum = DefaultWaitMaxSeconds
		}
		window := time.Duration(maximum) * time.Second
		now := e.clock()
		start := now
		if !createdAt.IsZero() && !createdAt.After(now) {
			// All readers of this running analysis share a finite issuance window.
			// A new authorized STATUS read can issue the next window after expiry.
			start = createdAt.Add(now.Sub(createdAt) / window * window)
		}
		e.batchPollStart = start
		deadline := start.Add(window)
		obs := Object{"deadline": deadline.UTC().Format(time.RFC3339Nano)}
		if len(observationDue) > 0 && observationDue[0].After(now) && observationDue[0].Before(deadline) {
			obs["notBefore"] = observationDue[0].UTC().Format(time.RFC3339Nano)
		}
		return e.ownerPoll(reason, read, args, obs,
			[]string{"READY_FOR_DECISION", "COMPLETE", "PARTIAL", "BLOCKED"})
	default:
		next := e.batchFailure(Object{"code": reason})
		if e.Valid != nil && e.Valid(read, args) {
			next["alternatives"] = append(next["alternatives"].([]Object), batchManualAlternative(read, args, reason, "Rileggi lo stato corrente."))
		}
		return next
	}
	if e.Valid != nil && e.Valid(read, args) {
		return e.ownerCall(reason, read, args, nil)
	}
	return BatchBlock(BatchUnavailable)
}

func (e Engine) BatchDecisionNext(id, batchID string, revision int64, ordinal int, cursor string, codes ...string) Object {
	const read = "source.analysis.read"
	reason := analysis.DiscoveryDecisionRequired
	if len(codes) > 0 && reasonPattern.MatchString(codes[0]) {
		reason = codes[0]
	}
	next := Block(reason, "", "Valuta le colonne offerte e scegli tra le azioni ammesse per ciascuna voce.")
	options := make([]Object, 0, 3)
	if e.CommandKey != nil {
		const decide = "source.analysis.decide"
		args := Object{"analysisId": id, "batchId": batchID, "expectedRevision": revision}
		args["commandKey"] = e.CommandKey(decide, args, nil, nil)
		options = append(options, alternative(decide, args, reason, "Indica le azioni scelte per gli ordinali del lotto."))
	}
	if cursor != "" {
		args := Object{"analysisId": id, "view": "CANDIDATES", "batchId": batchID, "ordinal": ordinal, "cursor": cursor}
		options = append(options, alternative(read, args, "ANALYSIS_NEXT_PAGE", "Leggi altri dati della colonna."))
	}
	options = append(options, alternative(read, Object{"analysisId": id, "view": "BATCH"}, reason, "Rileggi il lotto corrente."))
	next["alternatives"] = options
	return next
}

func (e Engine) BatchPageNext(args Object, expired bool) Object {
	const read = "source.analysis.read"
	reason := "ANALYSIS_NEXT_PAGE"
	if expired {
		reason = "ANALYSIS_CURSOR_EXPIRED"
	}
	if e.Valid != nil && e.Valid(read, args) {
		return e.ownerCall(reason, read, args, nil)
	}
	return BatchBlock(BatchUnavailable)
}

func BatchAnalysisDone() Object {
	next := step("DONE", "ANALYSIS_COMPLETE", "Ogni colonna ha un esito verificato.")
	next["outcome"] = "SUCCEEDED"
	return next
}

func (e Engine) BatchDecisionReadNext(id, reason string) Object {
	const read = "source.analysis.read"
	args := Object{"analysisId": id}
	if e.Valid != nil && e.Valid(read, args) {
		return e.ownerCall(reason, read, args, nil)
	}
	return BatchBlock(reason)
}

// BatchNext projects the continuation for a batch analysis result.
func (e Engine) BatchNext(tool string, input, obs Object, failed bool) Object {
	if tool == reviewRead || tool == reviewCommand {
		return e.BatchReviewNext(tool, input, obs, failed)
	}
	if failed {
		return e.batchFailure(obs)
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

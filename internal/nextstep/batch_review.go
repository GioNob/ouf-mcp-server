package nextstep

import (
	"fmt"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

const (
	reviewRead            = "semantic.proposal.read"
	reviewCommand         = "semantic.approval.request"
	SourceMappingRequired = "SOURCE_MAPPING_REQUIRED"
)

func batchReviewArgs(id string) Object {
	return Object{"analysisId": id, "projection": analysis.BatchMode}
}

func (e Engine) BatchReviewExpiredRead(input Object, batch string) Object {
	id, _ := input["analysisId"].(string)
	return e.BatchReviewRead(id, batch, ReviewBatchExpired)
}

func (e Engine) BatchReviewRead(id, batch, reason string) Object {
	args := batchReviewArgs(id)
	args["reviewBatchId"] = batch
	return e.batchCall(reason, reviewRead, args)
}

func (e Engine) BatchReviewReceipt(args Object) Object {
	return e.batchCall("SEMANTIC_RECEIPT_READ_REQUIRED", reviewRead, args)
}

func (e Engine) BatchValidationNext(id string, input, obs Object) Object {
	if obs["status"] != "PASS" && obs["state"] != "VALIDATED" {
		return Block("SEMANTIC_VALIDATION_FAILED", "", "Verifica gli errori della validazione della revisione.",
			manualOption{tool: "source.analysis.read", args: Object{"analysisId": id}, reason: "ANALYSIS_CONTINUE", message: "Riprendi l’analisi."})
	}
	revision := get(obs, input, "revision_id|revisionId|@revisionId")
	version := get(obs, input, "validated_row_version|rowVersion|expectedRowVersion|@expectedRowVersion")
	run := get(obs, nil, "validation_run_id|validationRunId|currentValidationRunId")
	if input["revisionId"] != nil && input["revisionId"] != revision {
		return BatchBlock("OWNER_STATE_UNKNOWN")
	}
	args := batchReviewArgs(id)
	args["batchAction"], args["reviewContextRef"] = "ADD", id
	args["item"] = Object{"revisionId": revision, "expectedRowVersion": version, "validationRunId": run}
	if e.CommandKey != nil {
		args["commandKey"] = e.CommandKey(reviewCommand, args, nil, nil)
	}
	return e.batchCall("SEMANTIC_REVIEW_BATCH_ADD", reviewCommand, args)
}

func (e Engine) BatchReviewContext(id string) Object {
	args := batchReviewArgs(id)
	args["reviewContextRef"] = id
	return e.batchCall("ANALYSIS_REVIEW_CONTEXT", reviewRead, args)
}

func (e Engine) batchCall(reason, tool string, args Object) Object {
	if e.Valid != nil && e.Valid(tool, args) {
		if reason == "SEMANTIC_PUBLICATION_PENDING" {
			return e.ownerPoll(reason, tool, args, nil, []string{"ACTIVE", "RETIRED", "DEPRECATED"})
		}
		return e.ownerCall(reason, tool, args, nil)
	}
	return BatchBlock(BatchUnavailable)
}

func (e Engine) BatchReviewNext(tool string, input, obs Object, failed bool) Object {
	id, _ := input["analysisId"].(string)
	batch, _ := obs["reviewBatchId"].(string)
	code, _ := obs["code"].(string)
	if analysis.ValidateID(id) != nil {
		return BatchBlock("OWNER_STATE_UNKNOWN")
	}
	contextID, _ := obs["reviewContextRef"].(string)
	if contextID != "" && contextID != id {
		return BatchBlock("OWNER_STATE_UNKNOWN")
	}
	readAnalysis := Object{"analysisId": id}
	block := func(reason, message string, options ...manualOption) Object {
		return Block(reason, "", message, options...)
	}
	option := func(t string, args Object, reason, message string) manualOption {
		if e.Valid != nil && e.Valid(t, args) {
			return manualOption{tool: t, args: args, reason: reason, message: message}
		}
		return manualOption{}
	}
	read := func(b string) Object {
		args := batchReviewArgs(id)
		args["reviewBatchId"] = b
		return args
	}
	command := func(action, b string, item Object) Object {
		args := batchReviewArgs(id)
		args["batchAction"], args["reviewContextRef"] = action, id
		if b != "" {
			args["reviewBatchId"] = b
		}
		if item != nil {
			args["item"] = item
		}
		if e.CommandKey != nil {
			// The logical submit is stable across observations of the same lot.
			args["commandKey"] = e.CommandKey(reviewCommand, args, nil, nil)
		}
		return args
	}
	proceed := func(reason string) manualOption {
		return option("source.analysis.read", readAnalysis, reason, "Riprendi dalle colonne dell’analisi.")
	}
	mappingArgs := Object{"analysisId": id}
	for _, field := range []string{"assetId", "profileId"} {
		if obs[field] != nil {
			mappingArgs[field] = obs[field]
		}
	}
	if e.CommandKey != nil {
		mappingArgs["commandKey"] = e.CommandKey("source.onboarding.create", mappingArgs, nil, nil)
	}
	mapping := manualOption{tool: "source.onboarding.create", args: mappingArgs, reason: SourceMappingRequired, message: "Prepara il mapping dalle ricevute dell’analisi."}
	if obs["replay"] == true || code == "SEMANTIC_RECEIPT_READ_REQUIRED" || code == "OUTCOME_REQUIRES_RECONCILIATION" {
		args := batchReviewArgs(id)
		args["commandKey"] = input["commandKey"]
		if e.Valid != nil && e.Valid(reviewRead, args) {
			return e.batchCall("SEMANTIC_RECEIPT_READ_REQUIRED", reviewRead, args)
		}
	}
	if failed {
		switch code {
		case "SEM_REVIEW_BATCH_ITEM_TYPE_UNSUPPORTED":
			args := batchReviewArgs(id)
			for _, field := range []string{"revisionId", "expectedRowVersion", "validationRunId"} {
				args[field] = obs[field]
				if args[field] == nil {
					args[field] = lookup(input, "item."+field)
				}
			}
			if e.CommandKey != nil {
				args["commandKey"] = e.CommandKey(reviewCommand, args, nil, nil)
			}
			return block(ReviewSingleRequired, "Questa proposta si approva da sola. Verifica prima le sue dipendenze.",
				option(reviewCommand, args, ReviewSingleRequired, "Chiedi la revisione singola."),
				option(reviewRead, Object{"analysisId": id, "projection": analysis.BatchMode, "reviewContextRef": id}, ReviewSingleRequired, "Prosegui con il lotto del file."))
		case "SEM_REVIEW_BATCH_ITEM_UNDER_REVIEW", "SEM_REVIEW_BATCH_NOT_COLLECTING", "SEM_REVIEW_BATCH_ALREADY_RESUBMITTED":
			if successor, _ := obs["successorBatchId"].(string); successor != "" {
				batch = successor
			}
			return e.batchCall(code, reviewRead, read(batch))
		case "SEM_REVIEW_BATCH_COLLECTING_LIMIT":
			return block(ReviewBatchCollecting, "Il lotto ha raggiunto il limite di raccolta.",
				option(reviewCommand, command("SUBMIT", batch, nil), code, "Chiedi la revisione del lotto."))
		case "SEM_REVIEW_BATCH_EMPTY":
			return block(ReviewBatchEmpty, "Nessuna proposta semantica in attesa di revisione.", proceed(ReviewBatchEmpty), mapping)
		case "SEM_REVIEW_BATCH_ITEMS_NOT_READY":
			var first Object
			if items, _ := obs["items"].([]any); len(items) > 0 {
				first, _ = items[0].(Object)
			}
			return block(ReviewBatchItemsNotReady, "Verifica la prima proposta non pronta.", e.batchNotReady(id, first, command)...)
		case "SEM_REVIEW_BATCH_ITEM_NOT_OWNED", "SEM_REVIEW_BATCH_ITEM_DUPLICATE_ARTIFACT":
			return block(code, "Verifica l’appartenenza della proposta al lotto.", proceed(code))
		}
		return e.batchFailure(obs)
	}
	state := ObservedState(obs)
	if obs["kind"] != "REVIEW_BATCH" {
		if tool == reviewCommand && input["batchAction"] == nil || tool == reviewRead && input["commandKey"] != nil {
			challenge, _ := obs["currentChallenge"].(Object)
			if batchID, _ := challenge["reviewBatchId"].(string); analysis.CanonicalUUID(batchID) {
				return e.batchCall(ReviewBatch, reviewRead, read(batchID))
			}
			if e.Issue != nil && (state == "UNDER_REVIEW" || challenge["challengeStatus"] == "OPEN") {
				args := batchReviewArgs(id)
				args["commandKey"] = input["commandKey"]
				if e.Valid(reviewRead, args) {
					return e.Issue(manifest.Rule{Action: "WAIT_FOR_HUMAN", Reason: "SEMANTIC_REVIEW", Tool: reviewRead, Actor: "HUMAN_THS", Decision: "APPROVE_OR_REJECT"}, args, obs)
				}
			}
			if state == "VALIDATED" && obs["operation"] == "VALIDATE" {
				return e.BatchValidationNext(id, input, obs)
			}
			if state == "APPROVED" {
				args := batchReviewArgs(id)
				args["commandKey"] = input["commandKey"]
				return e.batchCall("SEMANTIC_PUBLICATION_PENDING", reviewRead, args)
			}
			if state == "REJECTED" || state == "EXPIRED" {
				reason := ReviewBatchExpired
				if state == "REJECTED" {
					reason = ReviewBatchChangesRequested
				}
				return block(reason, "Consulta l’esito della revisione singola e riprendi l’analisi.", proceed(reason), mapping)
			}
			if state == "ACTIVE" || state == "PUBLISHED" {
				return e.BatchDecisionReadNext(id, SourceMappingRequired)
			}
		}
		return BatchBlock("OWNER_STATE_UNKNOWN")
	}
	if state != "NONE" && !analysis.CanonicalUUID(batch) {
		return BatchBlock("OWNER_STATE_UNKNOWN")
	}
	if requested := input["reviewBatchId"]; tool == reviewRead && requested != nil && requested != batch {
		return BatchBlock("OWNER_STATE_UNKNOWN")
	}
	var page, rest manualOption
	if offset := obs["nextItemsOffset"]; offset != nil {
		args := read(batch)
		args["itemsOffset"] = offset
		page = option(reviewRead, args, "ANALYSIS_NEXT_PAGE", "Leggi le altre righe del lotto.")
	}
	if remaining, _ := obs["remainingBatchId"].(string); analysis.CanonicalUUID(remaining) {
		rest = option(reviewRead, read(remaining), ReviewBatchCollecting, "Leggi il resto del lotto.")
	}
	total := batchCount(obs["itemCount"])
	counts, _ := obs["counts"].(Object)
	published, changed, rejected := batchCount(counts["published"]), batchCount(counts["changesRequested"]), batchCount(counts["rejected"])
	switch state {
	case "NONE", "DISCARDED":
		return block(ReviewBatchEmpty, "Nessuna proposta semantica in attesa di revisione.", proceed(ReviewBatchEmpty), mapping)
	case "COLLECTING":
		message := fmt.Sprintf("Proposte nel lotto del file: %d. Chiedi la revisione quando la raccolta è completa.", total)
		submit := option(reviewCommand, command("SUBMIT", batch, nil), ReviewBatchCollecting, "Chiedi una sola revisione per il lotto.")
		var removeOption manualOption
		if rows, _ := obs["items"].([]any); len(rows) > 0 {
			row, _ := rows[0].(Object)
			if revision, _ := row["revisionId"].(string); analysis.CanonicalUUID(revision) {
				probe := command("REMOVE", "", Object{"revisionId": revision})
				if e.CommandKey != nil && e.Valid != nil && e.Valid(reviewCommand, probe) {
					args := command("REMOVE", "", nil)
					args["commandKey"] = e.CommandKey(reviewCommand, args, obs, nil)
					removeOption = manualOption{tool: reviewCommand, args: args, reason: ReviewBatchCollecting, message: "Indica la revisione mostrata da togliere dal lotto."}
				}
			}
		}
		if obs["collectionClosed"] != true || !matches(map[string]string{"allowedActions": "SUBMIT_REVIEW"}, obs, nil) {
			submit = manualOption{}
		}
		options := []manualOption{proceed(ReviewBatchCollecting), submit, removeOption, page}
		if tool == reviewRead {
			options[0], options[1] = options[1], options[0]
		}
		return block(ReviewBatchCollecting, message, options...)
	case "UNDER_REVIEW":
		challenge, _ := obs["currentChallenge"].(Object)
		if challenge["challengeId"] != batch || challenge["challengeStatus"] != "OPEN" {
			return BatchBlock("OWNER_STATE_UNKNOWN")
		}
		if end, err := time.Parse(time.RFC3339Nano, fmt.Sprint(challenge["expiresAt"])); err == nil && !e.clock().Before(end) {
			return e.batchCall(ReviewBatchExpired, reviewRead, read(batch))
		}
		args := read(batch)
		if input["commandKey"] != nil && input["reviewBatchId"] == nil || tool == reviewCommand && input["commandKey"] != nil {
			args = batchReviewArgs(id)
			args["commandKey"] = input["commandKey"]
		}
		if e.Valid != nil && e.Valid(reviewRead, args) && e.Issue != nil {
			return e.Issue(manifest.Rule{Action: "WAIT_FOR_HUMAN", Reason: ReviewBatch, Tool: reviewRead, Actor: "HUMAN_THS", Decision: "APPROVE_EACH_ROW"}, args, obs)
		}
	case "DECIDED":
		if changed+rejected > 0 {
			search := manualOption{tool: "semantic.search", args: Object{}, reason: ReviewBatchChangesRequested, message: "Cerca un termine già pubblicato con nuove evidenze."}
			return block(ReviewBatchChangesRequested, fmt.Sprintf("Pubblicati: %d. Con modifiche richieste: %d. Rifiutati: %d. Usa i riscontri delle righe per correggere le proposte interessate.", published, changed, rejected), page, proceed(ReviewBatchChangesRequested), search)
		}
		if published == total && total > 0 {
			if input["commandKey"] != nil || input["fromWait"] == true {
				if page.tool == "" && rest.tool == "" {
					return e.BatchDecisionReadNext(id, SourceMappingRequired)
				}
				return block(ReviewBatchPublished, "La pubblicazione del lotto è conclusa.", page, rest, proceed(SourceMappingRequired))
			}
			return block(ReviewBatchPublished, "La pubblicazione del lotto è conclusa.", page, rest, mapping, proceed(ReviewBatchPublished))
		}
	case "EXPIRED":
		var resubmit, successor manualOption
		if matches(map[string]string{"allowedActions": "RESUBMIT_REVIEW"}, obs, nil) {
			resubmit = option(reviewCommand, command("RESUBMIT", batch, nil), ReviewBatchExpired, "Chiedi una nuova revisione se l’utente la richiede.")
		}
		if next, _ := obs["successorBatchId"].(string); analysis.CanonicalUUID(next) {
			successor = option(reviewRead, read(next), ReviewBatchExpired, "Leggi la nuova revisione.")
		}
		return block(ReviewBatchExpired, fmt.Sprintf("La revisione è scaduta. Proposte ancora valide: %d.", batchCount(counts["expired"])), resubmit, successor, proceed(ReviewBatchExpired), page)
	}
	return BatchBlock("OWNER_STATE_UNKNOWN")
}

func batchCount(value any) int {
	var n int
	_, _ = fmt.Sscan(fmt.Sprint(value), &n)
	return n
}

func (e Engine) batchNotReady(id string, first Object, command func(string, string, Object) Object) []manualOption {
	revision, _ := first["revisionId"].(string)
	if !analysis.CanonicalUUID(revision) {
		return nil
	}
	var options []manualOption
	reason := get(first, nil, "reason|notReadyReason")
	if reason != "ROW_VERSION_CHANGED" && reason != "VALIDATION_NOT_CURRENT" && reason != "NOT_DRAFT" && reason != "UNRESOLVED_DEPENDENCY" {
		return nil
	}
	if reason == "ROW_VERSION_CHANGED" || reason == "VALIDATION_NOT_CURRENT" {
		const prepare = "semantic.review.prepare"
		args := Object{"analysisId": id, "revisionId": revision, "expectedRowVersion": get(first, nil, "rowVersion|expectedRowVersion")}
		if e.CommandKey != nil {
			args["commandKey"] = e.CommandKey(prepare, args, nil, nil)
		}
		if e.Valid != nil && e.Valid(prepare, args) {
			options = append(options, manualOption{tool: prepare, args: args, reason: ReviewBatchItemsNotReady, message: "Prepara nuovamente la revisione."})
		}
	}
	remove := command("REMOVE", "", Object{"revisionId": revision})
	if e.Valid != nil && e.Valid(reviewCommand, remove) {
		options = append(options, manualOption{tool: reviewCommand, args: remove, reason: ReviewBatchItemsNotReady, message: "Togli la proposta dal lotto."})
	}
	return options
}

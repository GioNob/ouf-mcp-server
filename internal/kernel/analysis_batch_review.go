package kernel

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type batchReviewCallKey struct{}

func (s *workflowServer) callBatchReview(ctx context.Context, c manifest.Capability, checksum string, input nextstep.Object, service *orchestration.Service, identity requestIdentity, start analysis.BatchStart) *mcp.CallToolResult {
	id := start.ID
	for _, field := range []string{"reviewContextRef", "analysisId"} {
		if value := input[field]; value != nil && value != id {
			return batchReadFailure(id, analysis.ErrScope)
		}
	}
	store, ok := service.Admission.(analysis.BatchOfferStore)
	if !ok {
		return batchUnavailable(id)
	}
	scope := analysis.IdentityScope(identity.Identity)
	head, err := store.ReadBatch(ctx, scope, id, checksum, nil)
	if err != nil {
		return batchReadFailure(id, err)
	}
	closed := head.Reason == "ANALYSIS_SUMMARY" && head.OpenReviewID == ""
	full := head.FullReviewID != "" && input["reviewBatchId"] == head.FullReviewID
	if input["batchAction"] == "SUBMIT" && head.OpenReviewID != "" && input["reviewBatchId"] == head.OpenReviewID {
		return localBatchResult(nextstep.Object{"analysisId": id, "reviewBatchId": head.OpenReviewID},
			s.engine(ctx).BatchReviewRead(id, head.OpenReviewID, nextstep.ReviewBatch))
	}
	if input["batchAction"] == "SUBMIT" && !closed && !full {
		code := "ANALYSIS_COLLECTION_OPEN"
		result := batchReadFailure(id, analysis.ErrConflict)
		result.StructuredContent = nextstep.Object{"analysisId": id, "code": code}
		result.Meta[batchNextMeta] = s.engine(ctx).BatchDecisionReadNext(id, code)
		return result
	}
	forward := maps.Clone(input)
	delete(forward, "analysisId")
	delete(forward, "projection")
	raw, _ := json.Marshal(forward)
	result, _, err := invoke(context.WithValue(ctx, batchReviewCallKey{}, true), c, checksum, raw, service, orchestration.ReviewBatchPageSize, 262144)
	if err != nil || result == nil {
		return batchReadFailure(id, analysis.ErrUnavailable)
	}
	body := ownerObject(result)
	if body == nil {
		return batchReadFailure(id, analysis.ErrProfile)
	}
	body["analysisId"], body["collectionClosed"] = id, closed
	body["assetId"], body["profileId"] = start.Profile.AssetID, start.Profile.ProfileID
	if !result.IsError && body["kind"] == "REVIEW_BATCH" && body["changed"] != false {
		if body["reviewContextRef"] != id || c.OperationClass == "READ" && input["reviewBatchId"] != nil && body["reviewBatchId"] != input["reviewBatchId"] {
			return batchReadFailure(id, analysis.ErrScope)
		}
		if err := recordBatchReview(ctx, service, scope, id, body, input); err != nil {
			return batchReadFailure(id, err)
		}
	}
	if !result.IsError && c.ToolName == "semantic.proposal.read" && input["commandKey"] != nil && body["kind"] == nil &&
		(body["state"] == "ACTIVE" || body["state"] == "PUBLISHED") {
		if revision, _ := body["revisionId"].(string); analysis.CanonicalUUID(revision) {
			if reviews, ok := service.Admission.(analysis.BatchReviewStore); ok {
				set, _ := body["publicationSetId"].(string)
				if err := reviews.ObserveBatchReview(ctx, scope, id, "", "", []analysis.BatchReviewItem{{
					RevisionID: revision, State: "PUBLISHED", PublicationSetID: set,
				}}); err != nil {
					return batchReadFailure(id, err)
				}
			}
		}
	}
	if result.IsError {
		body = retainBatchFields(body, "analysisId", "code", "reasonCode", "requiredCapability", "retryable", "retryAfter", "notBefore", "windowEndsAt", "replay",
			"revisionId", "expectedRowVersion", "validationRunId", "reviewBatchId", "reviewContextRef", "successorBatchId", "items", "reason")
	}
	engine := s.observedEngine(ctx, result)
	fromWait, _ := ctx.Value(internalWaitReadKey{}).(bool)
	nextInput := input
	if fromWait {
		nextInput = maps.Clone(input)
		nextInput["fromWait"] = true
	}
	projected, next := s.compactBatchReview(engine, c.ToolName, nextInput, body, result.IsError)
	result.StructuredContent = projected
	if result.Meta == nil {
		result.Meta = mcp.Meta{}
	}
	result.Meta[batchNextMeta] = next
	return markBatchResult(result)
}

func (s *workflowServer) prepareBatchReview(ctx context.Context, c manifest.Capability, checksum string, input nextstep.Object, service *orchestration.Service) *mcp.CallToolResult {
	forward := maps.Clone(input)
	id, _ := forward["analysisId"].(string)
	delete(forward, "analysisId")
	wire, _ := json.Marshal(forward)
	result, _, err := invoke(ctx, c, checksum, wire, service, 1, 262144)
	if err != nil || result == nil {
		return batchReadFailure(id, analysis.ErrUnavailable)
	}
	body := ownerObject(result)
	if result.IsError || body["replay"] == true {
		args := nextstep.Object{"analysisId": id, "projection": analysis.BatchMode, "commandKey": input["commandKey"]}
		result.StructuredContent = retainBatchFields(body, "code", "reasonCode", "retryable", "replay")
		result.StructuredContent.(nextstep.Object)["analysisId"] = id
		if result.Meta == nil {
			result.Meta = mcp.Meta{}
		}
		engine := s.observedEngine(ctx, result)
		if body["replay"] == true || body["code"] == "OUTCOME_REQUIRES_RECONCILIATION" {
			result.Meta[batchNextMeta] = engine.BatchReviewReceipt(args)
		} else {
			result.Meta[batchNextMeta] = engine.BatchNext(c.ToolName, input, body, true)
		}
		return markBatchResult(result)
	}
	body["analysisId"] = id
	engine := s.observedEngine(ctx, result)
	return localBatchResult(retainBatchFields(body, "analysisId", "status", "state", "revisionId", "revision_id", "validationRunId", "validation_run_id",
		"validated_row_version", "rowVersion", "error_count", "warning_count"), engine.BatchValidationNext(id, input, body))
}

func retainBatchFields(source nextstep.Object, fields ...string) nextstep.Object {
	out := nextstep.Object{}
	for _, field := range fields {
		if value, ok := source[field]; ok {
			out[field] = value
		}
	}
	return out
}

func batchReviewProblem(problem *orchestration.Problem, raw []byte, now time.Time) *mcp.CallToolResult {
	result := ownerProblemAt(problem, raw, now)
	body := ownerObject(result)
	var detail nextstep.Object
	if json.Unmarshal(raw, &detail) == nil && detail != nil {
		for _, key := range []string{"revisionId", "validationRunId", "reviewBatchId", "reviewContextRef", "successorBatchId"} {
			if value, _ := detail[key].(string); analysis.CanonicalUUID(value) || key == "reviewContextRef" && analysis.ValidateID(value) == nil {
				body[key] = value
			}
		}
		if version, ok := detail["expectedRowVersion"].(float64); ok {
			body["expectedRowVersion"] = version
		}
		if reason, _ := detail["reason"].(string); slices.Contains([]string{"ARTIFACT_TYPE", "SEMANTIC_ID_TOO_LONG", "SEMANTIC_VERSION_TOO_LONG", "CARD_ROW_TOO_LARGE"}, reason) {
			body["reason"] = reason
		}
		if items, _ := detail["items"].([]any); len(items) > 0 {
			if first, ok := items[0].(nextstep.Object); ok {
				body["items"] = []any{retainBatchFields(first, "revisionId", "rowVersion", "expectedRowVersion", "reason", "notReadyReason")}
			}
		}
	}
	result.StructuredContent = body
	return result
}

func recordBatchReview(ctx context.Context, service *orchestration.Service, scope analysis.Scope, id string, body, input nextstep.Object) error {
	batch, _ := body["reviewBatchId"].(string)
	version, _ := body["version"].(string)
	if !analysis.CanonicalUUID(batch) || analysis.ValidateID(version) != nil {
		return nil
	}
	store, ok := service.Admission.(analysis.BatchReviewStore)
	if !ok {
		return analysis.ErrUnavailable
	}
	rows, _ := body["items"].([]any)
	items := make([]analysis.BatchReviewItem, 0, len(rows))
	var added string
	if input["batchAction"] == "ADD" && body["state"] == "COLLECTING" {
		item, _ := input["item"].(nextstep.Object)
		added, _ = item["revisionId"].(string)
	}
	for _, value := range rows {
		row, _ := value.(nextstep.Object)
		revision, _ := row["revisionId"].(string)
		state, _ := row["itemState"].(string)
		if state == "COLLECTED" || state == "NOT_READY" {
			state = "COLLECTING"
		}
		if !analysis.CanonicalUUID(revision) || !slices.Contains([]string{"COLLECTING", "UNDER_REVIEW", "EXPIRED", "PUBLISHED", "CHANGES_REQUESTED", "REJECTED"}, state) {
			return analysis.ErrProfile
		}
		pub, _ := row["publication"].(nextstep.Object)
		set, _ := pub["publicationSetId"].(string)
		if state == "PUBLISHED" && !analysis.CanonicalUUID(set) {
			return analysis.ErrProfile
		}
		semanticID, _ := row["semanticId"].(string)
		confirmedAdd := revision == added && state == "COLLECTING"
		items = append(items, analysis.BatchReviewItem{RevisionID: revision, State: state, PublicationSetID: set, SemanticID: semanticID, ConfirmedAdd: confirmedAdd})
		if revision == added {
			added = ""
		}
	}
	// ADD returns the first page, which may not contain the added revision.
	if analysis.CanonicalUUID(added) {
		items = append(items, analysis.BatchReviewItem{RevisionID: added, State: "COLLECTING", ConfirmedAdd: true})
	}
	if input["batchAction"] == "REMOVE" && (body["state"] == "COLLECTING" || body["state"] == "DISCARDED") {
		if raw, ok := input["item"].(nextstep.Object); ok {
			if revision, _ := raw["revisionId"].(string); analysis.CanonicalUUID(revision) {
				items = append(items, analysis.BatchReviewItem{RevisionID: revision, State: "REMOVED"})
			}
		}
	}
	if len(items) == 0 {
		return nil
	}
	return store.ObserveBatchReview(ctx, scope, id, batch, version, items)
}

func (s *workflowServer) compactBatchReview(engine nextstep.Engine, tool string, input, owner nextstep.Object, failed bool) (nextstep.Object, nextstep.Object) {
	next := func(obs nextstep.Object) nextstep.Object { return engine.BatchReviewNext(tool, input, obs, failed) }
	if failed {
		return owner, next(owner)
	}
	body := retainBatchFields(owner, "analysisId", "kind", "reviewBatchId", "reviewContextRef", "state", "status", "version", "observationVersion",
		"changed", "itemCount", "counts", "maxItems", "allowedActions", "itemsOffset", "nextItemsOffset", "remainingBatchId", "remainingCount",
		"successorBatchId", "publicationSetId", "pollAfterSeconds", "revisionId", "rowVersion", "expiresAt", "challengeId")
	if c, ok := owner["currentChallenge"].(nextstep.Object); ok {
		body["currentChallenge"] = retainBatchFields(c, "challengeId", "reviewBatchId", "challengeStatus", "expiresAt")
	}
	rows, _ := owner["items"].([]any)
	if len(rows) == 0 {
		return body, next(owner)
	}
	items := make([]nextstep.Object, 0, len(rows))
	for _, value := range rows {
		row, _ := value.(nextstep.Object)
		item := retainBatchFields(row, "position", "revisionId", "rowVersion", "currentValidationRunId", "semanticId",
			"artifactType", "discoveryRequestId", "itemState", "notReadyReason", "reviewFeedback", "publication")
		items = append(items, item)
	}
	// Offset is the owner selector. An overlapping owner page starts exactly
	// after the last row delivered by this projection.
	offset, _ := strconv.Atoi(stringNumber(owner["itemsOffset"]))
	var projected, wait nextstep.Object
	if owner["state"] == "UNDER_REVIEW" {
		wait = next(owner)
	}
	for n := len(items); n > 0; n-- {
		obs := maps.Clone(owner)
		body["items"] = items[:n]
		if n < len(items) {
			body["nextItemsOffset"], obs["nextItemsOffset"] = offset+n, offset+n
		}
		projected = wait
		if projected == nil {
			projected = next(obs)
		}
		if batchFits(body, projected) {
			return body, projected
		}
	}
	// Preserve identifiers and the publication set. The revision identifies
	// the receipt even when its display IRI exceeds the page budget.
	row := items[0]
	delete(row, "semanticId")
	if publication, ok := row["publication"].(nextstep.Object); ok {
		row["publication"] = retainBatchFields(publication, "publicationSetId")
	}
	row["truncatedFields"] = []string{"semanticId", "publication.semanticVersion"}
	if !batchFits(body, projected) {
		// A maximum-sized human link can consume most of the envelope.
		// Keep the phase, receipt selector and checked continuation.
		if wait != nil {
			delete(body, "items")
			delete(body, "nextItemsOffset")
			delete(body, "itemsOffset")
		} else {
			return retainBatchFields(body, "analysisId", "reviewContextRef", "reviewBatchId", "version", "kind", "state", "counts",
				"itemCount", "items", "itemsOffset", "nextItemsOffset"), projected
		}
	}
	return body, projected
}

func stringNumber(x any) string {
	switch n := x.(type) {
	case json.Number:
		return n.String()
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	case int:
		return strconv.Itoa(n)
	}
	return ""
}

func compactReviewLink(link nextstep.LinkResult) nextstep.LinkResult {
	if link.Status == nextstep.LinkAvailable {
		wire, err := json.Marshal(link.URL)
		if err != nil || len(link.URL) > 2000 || len(wire) > 6000 {
			return nextstep.LinkResult{}
		}
	}
	return link
}

package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/contracts"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func (w BatchWorker) decisionEffect(ctx context.Context, i orchestration.Identity, checksum string, job BatchWork) ([]BatchWorkResult, time.Time, error) {
	col := job.Columns[0]
	hold := func(code string) ([]BatchWorkResult, time.Time, error) {
		return []BatchWorkResult{batchEffectResult(col, "HELD", code, "")}, time.Time{}, nil
	}
	if !CanonicalUUID(col.EffectKey) || len(col.EffectArguments) == 0 {
		return hold(InvalidOwner)
	}
	tool := "semantic.propose"
	if job.Stage == BatchAdopt {
		tool = "semantic.discovery.adopt"
	}
	switch job.Stage {
	case BatchValidate:
		tool = "semantic.review.prepare"
	case BatchReviewAdd:
		tool = "semantic.approval.request"
	}
	sourceStage := job.Stage
	if job.Stage == BatchReviewInspect {
		sourceStage = col.OriginalAction
		if col.EffectRevision != "" && sourceStage == BatchPropose {
			sourceStage = BatchDraft
		}
	}
	read := job.Stage == BatchDraft || job.Stage == BatchReviewInspect
	if job.PreviousFence != "" && !read {
		prior, dispatched, err := w.Store.BatchDecisionAttempt(ctx, IdentityScope(i), job.PreviousFence, job.Stage, col.EffectArguments)
		if errors.Is(err, ErrConflict) {
			return hold(InvalidOwner)
		}
		if err != nil {
			return nil, time.Time{}, err
		}
		if dispatched {
			if code := prior.AdoptionRefusalCode; code != "" && code != ErrKeyReuse.Error() {
				return []BatchWorkResult{batchEffectResult(col, "REFUSED", code, "")}, time.Time{}, nil
			}
			if prior.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
				return hold("RETRY_WINDOW_UNAVAILABLE")
			}
			if prior.Observation.Due.After(w.now()) {
				return w.delayEffect(job, prior, nil, prior.Observation.Due)
			}
			// A recorded quota refusal rejected the mutation before execution.
			// Every other dispatched attempt must be reconciled at the owner.
			read = prior.AttemptState != "FAILED" || prior.Observation.Status != http.StatusTooManyRequests
		}
	}
	if col.RetryAttempts >= MaxRetryAttempts || (!col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget))) {
		return hold("RETRY_BUDGET_EXHAUSTED")
	}
	args := col.EffectArguments
	if read {
		tool = "semantic.proposal.read"
		if sourceStage != BatchDraft {
			args, _ = json.Marshal(map[string]string{"commandKey": col.EffectKey})
		}
		saved, err := w.Service.SavedObservation(ctx, i, "ouf.semantic.proposal.read", "READ", args)
		if err != nil {
			return hold("RETRY_WINDOW_UNAVAILABLE")
		}
		if saved.Due.After(w.now()) {
			return w.delayEffect(job, orchestration.Result{Observation: saved}, nil, saved.Due)
		}
	}
	job.ReconcileOnly = read
	ordinals := []int{col.Ordinal}
	if err := w.begin(ctx, job, ordinals); err != nil {
		return nil, time.Time{}, err
	}
	col = job.Columns[0]
	r, err := w.callBatchDecision(job.dispatchContext(ctx, ordinals), i, checksum, job, tool, args, col.EffectKey, read)
	if ctx.Err() != nil && r.Observation.At.IsZero() {
		return nil, time.Time{}, ctx.Err()
	}
	if errors.Is(err, orchestration.ErrUnauthorized) || errors.Is(err, ErrEpochChanged) ||
		r.Observation.Status == http.StatusUnauthorized || r.Observation.Status == http.StatusForbidden {
		return hold(batchOwnerFailure(r.Observation, err))
	}
	if r.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
		return hold("RETRY_WINDOW_UNAVAILABLE")
	}
	var window *orchestration.WindowBudgetExhaustedError
	if errors.As(err, &window) || r.Observation.Status == http.StatusTooManyRequests || (read && batchTransient(r.Observation)) {
		return w.delayEffect(job, r, err, time.Time{})
	}
	if !col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget)) {
		return hold("RETRY_BUDGET_EXHAUSTED")
	}
	if !read && job.Stage == BatchAdopt && r.Problem != nil &&
		r.Problem.Code != ErrKeyReuse.Error() && orchestration.AdoptionRefusal(r.Problem.Status, r.Problem.Code) != "" {
		return []BatchWorkResult{batchEffectResult(col, "REFUSED", r.Problem.Code, "")}, time.Time{}, nil
	}
	if err == nil && r.Problem == nil && !r.Replay && r.Observation.Status >= 200 && r.Observation.Status < 300 {
		if ReviewEffectStage(job.Stage) {
			if effect, ok := batchReviewEffect(job, col, read, r.Body); ok {
				return []BatchWorkResult{effect}, time.Time{}, nil
			}
		}
		ref := batchEffectReference(sourceStage, read, col, r.Body)
		if ref != "" {
			code := "CREATED"
			switch sourceStage {
			case BatchAdopt:
				code = "ADOPTED"
			case BatchDraft:
				code = "DRAFT_RECEIPT_VERIFIED"
			}
			effect := batchEffectResult(col, "APPLIED", code, ref)
			if batchPrepareReview(&effect, col, read, r.Body) {
				return []BatchWorkResult{effect}, time.Time{}, nil
			}
		}
	}
	if read {
		if job.Stage == BatchDraft && r.Problem != nil && r.Problem.Status == http.StatusNotFound {
			return []BatchWorkResult{batchEffectResult(col, "REFUSED", "SEMANTIC_DRAFT_UNVERIFIED", "")}, time.Time{}, nil
		}
		return hold("OUTCOME_REQUIRES_RECONCILIATION")
	}
	if r.Problem != nil && r.Problem.Status >= 400 && r.Problem.Status < 500 {
		if job.Stage == BatchReviewAdd && r.Problem.Code == "SEM_REVIEW_BATCH_COLLECTING_LIMIT" {
			var problem struct{ ReviewBatchID, ReviewContextRef string }
			if json.Unmarshal(r.Body, &problem) != nil || problem.ReviewContextRef != job.AnalysisID || !CanonicalUUID(problem.ReviewBatchID) {
				return hold(InvalidOwner)
			}
			effect := batchEffectResult(col, "PENDING", r.Problem.Code, col.EffectRevision)
			effect.State, effect.ReviewBlockedBy = "DECIDED", problem.ReviewBatchID
			return []BatchWorkResult{effect}, time.Time{}, nil
		}
		if job.Stage == BatchReviewAdd && r.Problem.Code == "SEM_REVIEW_BATCH_ITEM_TYPE_UNSUPPORTED" {
			effect := batchEffectResult(col, "APPLIED", r.Problem.Code, col.EffectRevision)
			return []BatchWorkResult{effect}, time.Time{}, nil
		}
		if job.Stage == BatchReviewAdd && r.Problem.Code == "SEM_REVIEW_BATCH_ITEM_UNDER_REVIEW" {
			return hold(r.Problem.Code)
		}
		if r.Problem.Code == ErrKeyReuse.Error() {
			return hold(InvalidOwner)
		}
		if r.Problem.Status != http.StatusRequestTimeout {
			return hold("OWNER_REQUEST_REJECTED")
		}
	}
	if r.AttemptID == uuid.Nil && r.Observation.At.IsZero() {
		return hold(batchOwnerFailure(r.Observation, err))
	}
	// A transport failure or unverified acknowledgement can only lead to a
	// receipt read. Keep the mutation's fence until reconciliation is durable.
	return w.delayEffect(job, r, err, maxTime(w.now().Add(5*time.Second), r.Observation.Due))
}

func (w BatchWorker) callBatchDecision(ctx context.Context, i orchestration.Identity, checksum string, job BatchWork, tool string, args []byte, key string, read bool) (orchestration.Result, error) {
	cap, ok := w.semanticCapability(tool, i.ActorType)
	if !ok {
		return orchestration.Result{}, ErrUnavailable
	}
	if cap.RequiredScope != "" && !slices.Contains(i.Scopes, cap.RequiredScope) {
		return orchestration.Result{}, orchestration.ErrUnauthorized
	}
	var cost struct{ HardMaxResultBytes, HardMaxDistinctObjects, HardMaxElapsedMs int64 }
	if json.Unmarshal(cap.BaseCostEnvelope, &cost) != nil || cost.HardMaxResultBytes < 1 || cost.HardMaxElapsedMs < 1 {
		return orchestration.Result{}, ErrUnavailable
	}
	fence := job.Fence
	resourceID := ""
	if ReviewEffectStage(job.Stage) && !read {
		resourceID = job.Columns[0].EffectRevision
	}
	if read {
		fence, key = "", uuid.NewString()
	}
	return w.Service.Call(ctx, orchestration.Invocation{
		Identity: i, ManifestChecksum: checksum, CapabilityID: cap.CapabilityID, AuthorizationCapabilityID: cap.RequiredAuthorization,
		Owner: cap.Owner, OperationClass: cap.OperationClass, GatewayBindingRef: cap.GatewayBindingRef,
		Resource: orchestration.ResourceContext{ResourceType: "semantic.proposalcommand", ResourceID: resourceID, TenantID: i.TenantID,
			Attributes: map[string]string{"module": "SEMANTIC"}},
		Arguments: args, BatchEffectFence: fence, BatchRoute: job.Route(), IdempotencyKey: key, CorrelationID: uuid.NewString(),
		Window: orchestration.AdmissionWindow, Timeout: min(batchCallTime, time.Duration(cost.HardMaxElapsedMs)*time.Millisecond),
		RetryThreshold: orchestration.AdmissionRetryThreshold,
		Maximum:        orchestration.Cost{ToolCalls: 1, DistinctObjects: cost.HardMaxDistinctObjects, ResultBytes: cost.HardMaxResultBytes},
		CheckDecision: func(d orchestration.AuthorizationDecision) error {
			if decisionEpoch(d) != job.Epoch {
				return ErrEpochChanged
			}
			return nil
		},
	})
}

func (w BatchWorker) delayEffect(job BatchWork, r orchestration.Result, cause error, due time.Time) ([]BatchWorkResult, time.Time, error) {
	results, pause, err := w.delay(job, r, cause, due)
	for n := range results {
		result := &results[n]
		result.EffectState = "PENDING"
		if result.State == "CLOSED" {
			result.EffectState, result.EffectCode = "HELD", result.Outcome.Reason
		}
	}
	return results, pause, err
}

func batchEffectResult(col BatchWorkColumn, state, code, ref string) BatchWorkResult {
	result := BatchWorkResult{Ordinal: col.Ordinal, State: "CLOSED", RetryAttempts: col.RetryAttempts,
		RetryStarted: col.RetryStarted, EffectState: state, EffectCode: code, EffectRef: ref}
	if state == "REFUSED" {
		result.State = "READY_FOR_DECISION"
	}
	return result
}

func batchEffectReference(stage string, read bool, col BatchWorkColumn, raw []byte) string {
	if !ValidEvidenceJSON(raw) {
		return ""
	}
	if read {
		var receipt struct {
			Operation, RevisionID, State string
			Result                       json.RawMessage
			HistoricalSnapshot           bool
			OwnerStateMustBeRechecked    bool
		}
		operation := "CREATE"
		if stage == BatchAdopt {
			operation = "ADOPT"
		}
		if json.Unmarshal(raw, &receipt) != nil || receipt.Operation != operation ||
			!receipt.HistoricalSnapshot || !receipt.OwnerStateMustBeRechecked {
			return ""
		}
		// proposal.read puts the current observation alongside the stored result.
		if stage == BatchDraft && (receipt.RevisionID != col.EffectRevision || receipt.State != "DRAFT") {
			return ""
		}
		raw = receipt.Result
	}
	var result struct {
		Status, RevisionID, RequestID, CandidateID, SemanticID, PublicationSetID string
	}
	if json.Unmarshal(raw, &result) != nil || !CanonicalUUID(result.RevisionID) || result.RevisionID == uuid.Nil.String() {
		return ""
	}
	switch stage {
	case BatchAdopt:
		var intended struct{ RequestID, CandidateID string }
		var receipt map[string]any
		schema, schemaErr := contracts.DiscoveryAdoptionReceiptSchema(read)
		if schemaErr != nil || json.Unmarshal(raw, &receipt) != nil || schema.Validate(receipt) != nil {
			return ""
		}
		if json.Unmarshal(col.EffectArguments, &intended) != nil || result.RequestID != intended.RequestID ||
			result.CandidateID != intended.CandidateID || result.RequestID != col.RequestID ||
			(result.Status == "ACTIVE" && !CanonicalUUID(result.PublicationSetID)) {
			return ""
		}
	case BatchPropose:
		var intended struct{ SemanticID string }
		if json.Unmarshal(col.EffectArguments, &intended) != nil || intended.SemanticID == "" ||
			result.SemanticID != intended.SemanticID || result.Status != "DRAFT" {
			return ""
		}
	case BatchDraft:
		if !read || result.RevisionID != col.EffectRevision {
			return ""
		}
	}
	return result.RevisionID
}

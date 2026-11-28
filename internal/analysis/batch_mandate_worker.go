package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type batchMandateWorkerStore interface {
	BatchMandateStartStore
	NextMandatedBatch(context.Context, time.Time) (Scope, string, error)
	ClaimMandatedBatch(context.Context, Scope, string, string, time.Time, int) (BatchWork, error)
	ClaimBatchMandateControl(context.Context, time.Time, string) (BatchMandateControl, error)
	FinishBatchMandateControl(context.Context, BatchMandateControl, string, time.Time) error
	RecordBatchMandateAttempt(context.Context, BatchWork, []int, time.Time, time.Time) error
	RecordBatchMandatePause(context.Context, BatchWork, time.Time, time.Time) error
	RecordBatchMandateObservation(context.Context, BatchWork, time.Time, time.Time) error
}

func (w BatchWorker) RunDue(ctx context.Context) (bool, error) {
	store, ok := w.Store.(batchMandateWorkerStore)
	if !ok || w.Mandates == nil {
		return false, ErrUnavailable
	}
	claimCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	control, err := store.ClaimBatchMandateControl(claimCtx, w.now(), uuid.NewString())
	if err != nil {
		return false, err
	}
	if control.AnalysisID != "" {
		stop()
		return true, w.runMandateControl(ctx, store, control)
	}
	scope, id, err := store.NextMandatedBatch(claimCtx, w.now())
	if err != nil || id == "" {
		return false, err
	}
	cap, ok := Find(w.Manifest, BatchTool)
	if !ok || cap.PublicationState != "ACTIVE" {
		return false, ErrUnavailable
	}
	limits, err := PublishedLimits(cap)
	if err != nil {
		return false, err
	}
	sum, err := w.Manifest.Checksum()
	if err != nil {
		return false, err
	}
	job, err := store.ClaimMandatedBatch(claimCtx, scope, id, sum, w.now(), limits.MaxIntents)
	stop()
	if err != nil || len(job.Columns) == 0 {
		return false, err
	}
	// Identity scope contributes to the stable original discovery key. The
	// private Gateway request contains no interactive identity or credential.
	i := orchestration.Identity{ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant}
	return w.runClaim(ctx, i, scope, sum, job, limits, time.Time{})
}

func (w BatchWorker) runMandateControl(ctx context.Context, store batchMandateWorkerStore, job BatchMandateControl) error {
	now := w.now()
	end := job.Started.Add(batchBudget)
	writeCtx, stopWrite := context.WithTimeout(context.WithoutCancel(ctx), batchCallTime*2)
	defer stopWrite()
	m := job.Mandate
	bound := m.BoundView()
	if m.State == "ACTIVE" && bound.ExpiresAtMs > 0 && now.UnixMilli() >= bound.ExpiresAtMs {
		bound.State = "EXPIRED"
		return store.CommitBatchMandate(writeCtx, job.AnalysisID, m, bound, now)
	}
	if job.Attempts >= MaxRetryAttempts || !now.Before(end) {
		return store.FinishBatchMandateControl(writeCtx, job, "RETRY_BUDGET_EXHAUSTED", time.Time{})
	}
	callCtx, stop := context.WithTimeout(ctx, min(batchCallTime, end.Sub(now)))
	if m.State == "ACTIVE" && m.Version < 1 {
		stop()
		return store.FinishBatchMandateControl(writeCtx, job, "ANALYSIS_MANDATE_BINDING_MISMATCH", time.Time{})
	}
	var view BatchMandateView
	var err error
	if m.State == "PENDING" {
		view, err = w.Mandates.Issue(callCtx, BatchMandateIssue{m.StartRef, m.Generation, m.CommandKey, job.AnalysisID, job.Epoch, m.StartBinding, ""})
	} else {
		view, err = w.Mandates.Expire(callCtx, m.Key(), m.Version)
		var conflict *BatchMandateError
		if errors.As(err, &conflict) && conflict.Observation.Code == "MANDATE_VERSION_CHANGED" &&
			w.now().UnixMilli() < bound.ExpiresAtMs {
			view, err = w.Mandates.Read(callCtx, m.Key())
		}
	}
	stop()
	if err != nil && m.State == "ACTIVE" && bound.ExpiresAtMs > 0 && w.now().UnixMilli() >= bound.ExpiresAtMs {
		bound.State = "EXPIRED"
		return store.CommitBatchMandate(writeCtx, job.AnalysisID, m, bound, w.now())
	}
	if err == nil {
		if m.State == "ACTIVE" && view.State == "ACTIVE" {
			err = ErrConflict
		} else {
			err = store.CommitBatchMandate(writeCtx, job.AnalysisID, m, view, w.now())
			if !errors.Is(err, ErrConflict) {
				return err
			}
		}
	}
	obs := mandateObservation(err)
	var due time.Time
	if mandateTransient(obs) {
		due = maxTime(obs.Due, w.now().Add(batchBackoff(job.Attempts)))
		if !due.Before(end) {
			obs.Code, due = "RETRY_BUDGET_EXHAUSTED", time.Time{}
		}
	}
	return store.FinishBatchMandateControl(writeCtx, job, obs.Code, due)
}

func batchBackoff(attempts int) time.Duration {
	delay := [...]time.Duration{5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}[min(max(attempts-1, 0), 4)]
	return delay + time.Duration(rand.Int64N(int64(delay)/5+1))
}

func mandateObservation(err error) orchestration.OwnerObservation {
	var failure *BatchMandateError
	if errors.As(err, &failure) {
		return failure.Observation
	}
	return orchestration.OwnerObservation{Code: "ANALYSIS_MANDATE_OUTCOME_UNVERIFIED"}
}

func mandateTransient(obs orchestration.OwnerObservation) bool {
	if obs.Reason == "RETRY_WINDOW_UNAVAILABLE" {
		return false
	}
	switch obs.Status {
	case 429:
		return true
	case 408:
		return obs.Code == "ANALYSIS_REQUEST_TIMEOUT" || obs.Code == "OWNER_REQUEST_FAILED"
	case 503:
		switch obs.Code {
		case "MCP_BATCH_GATEWAY_INTERRUPTED", "ANALYSIS_AUTHORITY_UNAVAILABLE",
			"ANALYSIS_AUTHORIZATION_UNAVAILABLE", "CURRENT_RIGHTS_UNAVAILABLE", "REGISTRY_UNAVAILABLE",
			"ANALYSIS_OWNER_OUTCOME_UNCERTAIN", "ANALYSIS_DISCOVERY_RECEIPT_UNVERIFIED",
			"SEM_BATCH_UNAVAILABLE", "SEM_DISCOVERY_UNAVAILABLE":
			return true
		}
	}
	return false
}

func (w BatchWorker) checkBatchMandate(ctx context.Context, job BatchWork) ([]BatchWorkResult, time.Time, bool, error) {
	m := job.Mandate
	if w.Mandates == nil || m.State != "ACTIVE" || m.Ref == "" || m.Binding == "" {
		code := m.Reason
		if code == "" {
			code = "ANALYSIS_MANDATE_UNAVAILABLE"
		}
		return batchBlock(job, code), time.Time{}, true, nil
	}
	bound := m.BoundView()
	signedExpiry := bound.ExpiresAtMs
	if signedExpiry <= 0 || bound.AnalysisID != job.AnalysisID || bound.Version != m.Version {
		return batchBlock(job, "ANALYSIS_MANDATE_BINDING_MISMATCH"), time.Time{}, true, nil
	}
	if w.now().UnixMilli() >= signedExpiry {
		return batchBlock(job, "MANDATE_EXPIRED"), time.Time{}, true, nil
	}
	col := job.Columns[0]
	if col.RetryAttempts >= MaxRetryAttempts || (!col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget))) {
		return batchBlock(job, "RETRY_BUDGET_EXHAUSTED"), time.Time{}, true, nil
	}
	if !job.CheckMandate {
		return nil, time.Time{}, false, nil
	}
	// Read after a wait or an orphaned lease so a revoked mandate never
	// causes a new dispatch. The real admission is still inside dispatch.
	callCtx, stop := context.WithTimeout(ctx, batchCallTime)
	view, err := w.Mandates.Read(callCtx, m.Key())
	stop()
	if err != nil && w.now().UnixMilli() >= signedExpiry {
		return batchBlock(job, "MANDATE_EXPIRED"), time.Time{}, true, nil
	}
	if err == nil && (view.Ref != m.Ref || view.Generation != m.Generation || view.AnalysisID != job.AnalysisID ||
		view.Version < m.Version || view.ExpiresAtMs != signedExpiry || (view.State == "ACTIVE" && view.Version != m.Version)) {
		err = ErrConflict
	}
	if err == nil {
		if view.State == "ACTIVE" && w.now().UnixMilli() < signedExpiry {
			return nil, time.Time{}, false, nil
		}
		if view.State == "ACTIVE" {
			view.State = "EXPIRED"
		}
		err = batchMandateStateError(view.State)
	}
	obs := mandateObservation(err)
	if mandateTransient(obs) {
		ordinals := make([]int, len(job.Columns))
		for n, col := range job.Columns {
			ordinals[n] = col.Ordinal
		}
		if err := w.begin(ctx, job, ordinals); err != nil {
			return nil, time.Time{}, true, err
		}
	}
	results, pause, delayErr := w.mandateFailure(ctx, job, obs, err)
	return results, pause, true, delayErr
}

func (w BatchWorker) dispatchMandate(ctx context.Context, job BatchWork, ordinals []int, tool string, args []byte, limit int64) (
	r orchestration.Result, binding string, callErr, checkpointErr error,
) {
	cap, ok := w.semanticCapability(tool, "HUMAN")
	if !ok || BatchStageForOperation(tool) != job.Stage {
		return r, "", ErrUnavailable, nil
	}
	var cost struct{ HardMaxResultBytes, HardMaxElapsedMs int64 }
	if json.Unmarshal(cap.BaseCostEnvelope, &cost) != nil || cost.HardMaxResultBytes < 1 || cost.HardMaxElapsedMs < 1 {
		return r, "", ErrUnavailable, nil
	}
	if limit == 0 {
		limit = cost.HardMaxResultBytes
	}
	started := w.now()
	if err := w.Store.BeginBatchDispatch(ctx, job, ordinals, started); err != nil {
		return r, "", nil, err
	}
	now := w.now()
	if now.UnixMilli() >= job.Mandate.BoundView().ExpiresAtMs {
		p := &orchestration.Problem{Status: http.StatusForbidden, Code: "MANDATE_EXPIRED"}
		return orchestration.Result{Observation: orchestration.OwnerObservation{At: now, Status: p.Status, Code: p.Code}, Problem: p}, "", nil, nil
	}
	callCtx, stop := context.WithTimeout(ctx, min(batchCallTime, time.Duration(cost.HardMaxElapsedMs)*time.Millisecond))
	response, binding, err := w.Mandates.Dispatch(callCtx, BatchMandateDispatch{
		BatchMandateKey: job.Mandate.Key(), AnalysisID: job.AnalysisID, Operation: tool, Arguments: args, AttemptID: job.Fence, CorrelationID: uuid.NewString(),
	}, limit)
	stop()
	obs := orchestration.ObserveOwner(response, err, w.now())
	// Count observed dispatch outcomes and transient failures. A refusal can
	// leave an orphaned lease, but it never changes the retry budget.
	if !mandateDenied(obs) {
		writeCtx, release := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer release()
		if checkpointErr := w.Store.(batchMandateWorkerStore).RecordBatchMandateAttempt(writeCtx, job, ordinals, started, w.now()); checkpointErr != nil {
			return r, "", nil, checkpointErr
		}
		recordBatchAttempts(job, ordinals, started)
	}
	return orchestration.Result{Observation: obs, Body: response.Body, Problem: response.Problem}, binding, err, nil
}

func mandateDenied(obs orchestration.OwnerObservation) bool {
	return obs.Status == http.StatusUnauthorized || obs.Status == http.StatusForbidden ||
		obs.Code == "MANDATE_GENERATION_CHANGED" || obs.Code == "MANDATE_VERSION_CHANGED" || obs.Code == "MANDATE_NOT_FOUND"
}

func (w BatchWorker) mandateFailure(ctx context.Context, job BatchWork, obs orchestration.OwnerObservation, err error) ([]BatchWorkResult, time.Time, error) {
	if mandateTransient(obs) {
		if w.now().UnixMilli() >= job.Mandate.BoundView().ExpiresAtMs {
			return batchBlock(job, "MANDATE_EXPIRED"), time.Time{}, nil
		}
		results, pause, delayErr := w.delay(job, orchestration.Result{Observation: obs}, err, time.Time{})
		if delayErr == nil && !pause.IsZero() {
			writeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer stop()
			delayErr = w.Store.(batchMandateWorkerStore).RecordBatchMandatePause(writeCtx, job, pause, w.now())
		}
		return results, pause, delayErr
	}
	code := obs.Code
	if code == "" || code == "SUCCEEDED" || code == "OWNER_OUTCOME_UNCERTAIN" {
		code = batchOwnerFailure(obs, err)
	}
	if obs.Reason == "RETRY_WINDOW_UNAVAILABLE" {
		code = "RETRY_WINDOW_UNAVAILABLE"
	}
	return batchBlock(job, code), time.Time{}, nil
}

func (w BatchWorker) checkpointMandateObservation(ctx context.Context, job BatchWork, result BatchWorkResult, observed time.Time) error {
	due := result.Due
	if job.Stage == BatchStatus || job.Stage == BatchCandidates {
		due = maxTime(due, observed.Add(time.Duration(orchestration.PollIntervalSeconds)*time.Second))
	}
	if due.IsZero() {
		return nil
	}
	writeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer stop()
	return w.Store.(batchMandateWorkerStore).RecordBatchMandateObservation(writeCtx, job, due, w.now())
}

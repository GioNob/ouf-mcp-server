package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const (
	BatchPublished   = "PUBLISHED"
	BatchRequest     = "REQUEST"
	BatchStatus      = "STATUS"
	BatchCandidates  = "CANDIDATES"
	BatchReformulate = "REFORMULATE"
	BatchAdopt       = "ADOPT"
	BatchPropose     = "PROPOSE"
	BatchDraft       = "DRAFT"
	BatchLease       = 20 * time.Second
	batchBudget      = 24 * time.Hour
	batchCallTime    = 4 * time.Second
)

type BatchWork struct {
	AnalysisID, Stage, Fence, PreviousFence, Epoch string
	Profile                                        Profile
	LeaseUntil                                     time.Time
	Columns                                        []BatchWorkColumn
	ReconcileOnly                                  bool
}

type batchDispatchKey struct{}

// BatchDispatch identifies the leased columns that back a transport attempt.
type BatchDispatch struct {
	AnalysisID, Fence, Stage string
	Ordinals                 []int
}

func (j BatchWork) dispatchContext(ctx context.Context, ordinals []int) context.Context {
	return context.WithValue(ctx, batchDispatchKey{}, BatchDispatch{
		AnalysisID: j.AnalysisID, Fence: j.Fence, Stage: j.Stage, Ordinals: ordinals,
	})
}

func BatchDispatchFromContext(ctx context.Context) (BatchDispatch, bool) {
	dispatch, ok := ctx.Value(batchDispatchKey{}).(BatchDispatch)
	return dispatch, ok
}

func (j BatchWork) Route() string {
	if j.Stage == BatchReformulate {
		return BatchRequest
	}
	return j.Stage
}

type BatchWorkColumn struct {
	Ordinal, RetryAttempts    int
	Intent, RequestID         string
	RetryStarted, OwnerEnd    time.Time
	CompletedCount            *int
	EffectKey, EffectRevision string
	EffectArguments           []byte
}

type BatchWorkResult struct {
	Ordinal                            int
	Stage, State                       string
	Due, OwnerEnd, RetryStarted        time.Time
	RetryAttempts                      int
	Outcome                            Outcome
	Candidates                         []BatchCandidate
	EvidenceVersion                    string
	CompletedCount                     *int
	TrafficMetric                      string
	PublishedSolo                      bool
	EffectState, EffectCode, EffectRef string
}

type BatchWorkerStore interface {
	ClaimBatchWork(context.Context, Scope, string, string, string, time.Time, int) (BatchWork, error)
	BeginBatchDispatch(context.Context, BatchWork, []int, time.Time) error
	FinishBatchWork(context.Context, Scope, BatchWork, []BatchWorkResult, time.Time, time.Time) error
	BatchDiscoveryReceipt(context.Context, Scope, string, []byte) (orchestration.Result, error)
	BatchDecisionAttempt(context.Context, Scope, string, string, []byte) (orchestration.Result, bool, error)
}

type BatchWorker struct {
	Store       BatchWorkerStore
	Service     *orchestration.Service
	Manifest    *manifest.Snapshot
	Authorities *BatchAuthorities
	Now         func() time.Time
}

func (w BatchWorker) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}

// Run serves the queue independently of MCP request lifetimes. DB leases
// enforce the same limits across replicas. An idle worker polls locally.
func (w BatchWorker) Run(ctx context.Context, logger *slog.Logger) {
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			for ctx.Err() == nil {
				delay := 250 * time.Millisecond
				id, identity, expires := w.Authorities.next(w.now())
				if id != "" {
					worked, err := w.RunOnce(ctx, id, identity, expires)
					if err != nil && ctx.Err() == nil {
						logger.Warn("batch analysis worker checkpoint unavailable")
						delay = time.Second
					} else if worked {
						continue
					}
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
				case <-timer.C:
				}
				timer.Stop()
			}
		})
	}
	workers.Wait()
}

func (w BatchWorker) RunOnce(ctx context.Context, id string, i orchestration.Identity, expires time.Time) (bool, error) {
	scope := IdentityScope(i)
	now := w.now()
	if !scope.Valid() || i.ActorType != "HUMAN" || i.Delegation == "" || !expires.After(now.Add(batchCallTime+time.Second)) {
		return false, nil
	}
	capability, ok := Find(w.Manifest, BatchTool)
	if !ok || capability.PublicationState != "ACTIVE" {
		return false, ErrUnavailable
	}
	limits, err := PublishedLimits(capability)
	if err != nil {
		return false, err
	}
	checksum, err := w.Manifest.Checksum()
	if err != nil {
		return false, err
	}
	claimCtx, stopClaim := context.WithTimeout(ctx, 2*time.Second)
	job, err := w.Store.ClaimBatchWork(claimCtx, scope, id, checksum, BatchAuthorityBinding(i), now, limits.MaxIntents)
	stopClaim()
	if err != nil || len(job.Columns) == 0 {
		return false, err
	}
	// Bound the call by its durable clocks. The I/O and the
	// subsequent checkpoint never hold the queue transaction open.
	now = w.now()
	deadline := minTime(job.LeaseUntil, expires)
	for _, col := range job.Columns {
		// Already expired columns still need a checkpoint. They are never dispatched.
		if end := col.RetryStarted.Add(batchBudget); !col.RetryStarted.IsZero() && end.After(now) {
			deadline = minTime(deadline, end)
		}
		if col.OwnerEnd.After(now) {
			deadline = minTime(deadline, col.OwnerEnd)
		}
	}
	if w.Now != nil {
		// Keep the remaining duration when the worker uses an injected clock.
		deadline = time.Now().Add(deadline.Sub(now))
	}
	callCtx, stop := context.WithDeadline(ctx, deadline)
	results, pause, err := w.execute(callCtx, i, checksum, job, limits)
	stop()
	if err != nil {
		return true, err
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	return true, w.Store.FinishBatchWork(finishCtx, scope, job, results, pause, w.now())
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (w BatchWorker) execute(ctx context.Context, i orchestration.Identity, checksum string, job BatchWork, limits BatchLimits) ([]BatchWorkResult, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, time.Time{}, err
	}
	c := Coordinator{Service: w.Service, Manifest: w.Manifest}
	epoch, err := c.Epoch(ctx, i)
	if err != nil || epoch != job.Epoch {
		code := "ANALYSIS_AUTHORITY_UNAVAILABLE"
		if errors.Is(err, orchestration.ErrUnauthorized) {
			code = "AUTHORIZATION_DENIED"
		} else if err == nil {
			code = ErrEpochChanged.Error()
		}
		return batchBlock(job, code), time.Time{}, nil
	}
	if job.Stage == BatchPublished {
		return w.published(ctx, i, checksum, job, limits)
	}
	if job.Stage == BatchAdopt || job.Stage == BatchPropose || job.Stage == BatchDraft {
		return w.decisionEffect(ctx, i, checksum, job)
	}
	col := job.Columns[0]
	if !col.OwnerEnd.IsZero() && !w.now().Before(col.OwnerEnd) {
		return batchBlock(job, "DISCOVERY_WAIT_EXPIRED"), time.Time{}, nil
	}
	tool := "semantic.discovery.status"
	args, _ := json.Marshal(map[string]string{"requestId": col.RequestID})
	admissionKey := ""
	switch job.Stage {
	case BatchRequest, BatchReformulate:
		tool = "semantic.discovery.request"
		key := Digest([]any{BatchMode, IdentityScope(i), job.Profile.AssetID, job.Profile.ProfileID, col.Ordinal, 1})
		ownerKey := "batch:" + key
		if job.Stage == BatchReformulate {
			if !CanonicalUUID(col.EffectKey) {
				return batchBlock(job, InvalidOwner), time.Time{}, nil
			}
			ownerKey = col.EffectKey
		}
		args, _ = json.Marshal(map[string]any{"requestedArtifactType": "PROPERTY", "intent": col.Intent,
			"preferredLanguages": limits.Languages, "idempotencyKey": ownerKey})
		admissionKey = job.Fence
		if job.PreviousFence != "" {
			prior, e := w.Store.BatchDiscoveryReceipt(ctx, IdentityScope(i), job.PreviousFence, args)
			if e != nil {
				return nil, time.Time{}, e
			}
			if prior.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
				return batchBlock(job, "RETRY_WINDOW_UNAVAILABLE"), time.Time{}, nil
			}
			if prior.ResultID != nil && prior.AttemptState == "SUCCEEDED" {
				capability, ok := w.semanticCapability(tool, i.ActorType)
				if !ok {
					return batchBlock(job, "ANALYSIS_AUTHORITY_UNAVAILABLE"), time.Time{}, nil
				}
				decision, err := c.decision(ctx, i, capability)
				if err != nil {
					return batchBlock(job, batchOwnerFailure(orchestration.OwnerObservation{}, err)), time.Time{}, nil
				}
				if decisionEpoch(decision) != job.Epoch {
					return batchBlock(job, ErrEpochChanged.Error()), time.Time{}, nil
				}
				next := batchPending(col, BatchStatus, maxTime(w.now(), prior.Observation.Due))
				next.Outcome.RequestID = prior.ResultID.String()
				next.OwnerEnd = col.RetryStarted.Add(batchBudget)
				return []BatchWorkResult{next}, time.Time{}, nil
			}
			if prior.AttemptState == "SUCCEEDED" ||
				((prior.AttemptState == "FAILED" || prior.Observation.Status >= 400) && !batchTransient(prior.Observation)) {
				return batchBlock(job, batchOwnerFailure(prior.Observation, nil)), time.Time{}, nil
			}
			if prior.Observation.Due.After(w.now()) {
				return w.delay(job, prior, nil, prior.Observation.Due)
			}
			// Reconcile by the original owner key. Semantic's request operation
			// atomically returns the original receipt or inserts exactly once.
		}
	case BatchCandidates:
		tool = "semantic.discovery.candidates"
		args, _ = json.Marshal(map[string]any{"requestId": col.RequestID, "includeState": true})
	}
	if col.RetryAttempts >= MaxRetryAttempts || (!col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget))) {
		return batchBlock(job, "RETRY_BUDGET_EXHAUSTED"), time.Time{}, nil
	}
	ordinals := []int{col.Ordinal}
	if err := w.begin(ctx, job, ordinals); err != nil {
		return nil, time.Time{}, err
	}
	col = job.Columns[0]
	r, callErr := w.call(job.dispatchContext(ctx, ordinals), i, checksum, job, tool, args, admissionKey, 0, 0, 0)
	if r.Observation.At.IsZero() && ctx.Err() != nil {
		return nil, time.Time{}, ctx.Err()
	}
	if callErr != nil || r.Problem != nil || r.Replay || r.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
		return w.delay(job, r, callErr, time.Time{})
	}
	var result BatchWorkResult
	if job.Stage == BatchCandidates {
		result = batchCandidateResult(col, r.Body, r.Observation.At)
	} else {
		result = batchDiscoveryResult(col, job.Route(), r)
	}
	if !col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget)) {
		return batchBlock(job, "RETRY_BUDGET_EXHAUSTED"), time.Time{}, nil
	}
	if job.Stage == BatchStatus || job.Stage == BatchCandidates {
		result.TrafficMetric = "GET_USEFUL"
		if result.Stage == BatchStatus && result.State == "PENDING" {
			result.TrafficMetric = "GET_REPEATED"
		}
	}
	return []BatchWorkResult{result}, time.Time{}, nil
}

func (w BatchWorker) begin(ctx context.Context, job BatchWork, ordinals []int) error {
	now := w.now()
	if err := w.Store.BeginBatchDispatch(ctx, job, ordinals, now); err != nil {
		return err
	}
	for n := range job.Columns {
		col := &job.Columns[n]
		if slices.Contains(ordinals, col.Ordinal) {
			col.RetryAttempts++
			if col.RetryStarted.IsZero() {
				col.RetryStarted = now
			}
		}
	}
	return nil
}

func (w BatchWorker) semanticCapability(tool, actor string) (manifest.Capability, bool) {
	cap, ok := Find(w.Manifest, tool)
	return cap, ok && cap.PublicationState == "ACTIVE" && cap.Owner == "semantic" && actor == "HUMAN" &&
		(len(cap.AllowedActors) == 0 || slices.Contains(cap.AllowedActors, actor))
}

func (w BatchWorker) call(ctx context.Context, i orchestration.Identity, checksum string, job BatchWork, tool string, args []byte, admissionKey string, requestBytes int, resultBytes, objects int64) (orchestration.Result, error) {
	cap, ok := w.semanticCapability(tool, i.ActorType)
	if !ok {
		return orchestration.Result{}, ErrUnavailable
	}
	var cost struct{ HardMaxResultBytes, HardMaxDistinctObjects, HardMaxElapsedMs int64 }
	if json.Unmarshal(cap.BaseCostEnvelope, &cost) != nil || cost.HardMaxResultBytes < 1 || cost.HardMaxElapsedMs < 1 {
		return orchestration.Result{}, ErrUnavailable
	}
	if resultBytes == 0 {
		resultBytes, objects = cost.HardMaxResultBytes, cost.HardMaxDistinctObjects
	}
	if tool == "semantic.discovery.status" || tool == "semantic.discovery.candidates" {
		var in struct{ RequestID string }
		_ = json.Unmarshal(args, &in)
		selector, legacy := orchestration.DiscoveryObservationSelectors(w.Service.FingerprintKey, in.RequestID)
		ctx = WithPublicObservation(ctx, selector, legacy, in.RequestID)
	}
	return w.Service.Call(ctx, orchestration.Invocation{
		Identity: i, ManifestChecksum: checksum, CapabilityID: cap.CapabilityID, AuthorizationCapabilityID: cap.RequiredAuthorization,
		Owner: cap.Owner, OperationClass: cap.OperationClass, GatewayBindingRef: cap.GatewayBindingRef,
		Arguments: args, DiscoveryAdmissionKey: admissionKey, BatchRoute: job.Route(), IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(),
		Window: orchestration.AdmissionWindow, Timeout: min(batchCallTime, time.Duration(cost.HardMaxElapsedMs)*time.Millisecond), RetryThreshold: orchestration.AdmissionRetryThreshold,
		MaxRequestBytes: requestBytes, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: objects, ResultBytes: resultBytes},
		CheckDecision: func(d orchestration.AuthorizationDecision) error {
			if decisionEpoch(d) != job.Epoch {
				return ErrEpochChanged
			}
			return nil
		},
	})
}

func (w BatchWorker) published(ctx context.Context, i orchestration.Identity, checksum string, job BatchWork, limits BatchLimits) ([]BatchWorkResult, time.Time, error) {
	var intents []Intent
	groups := make(map[string][]int)
	results := make([]BatchWorkResult, 0, len(job.Columns))
	for _, col := range job.Columns {
		if col.RetryAttempts >= MaxRetryAttempts || (!col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget))) {
			results = append(results, batchBlocked(col, "RETRY_BUDGET_EXHAUSTED"))
			continue
		}
		intent := Intent{Text: col.Intent, Type: "PROPERTY", Languages: limits.Languages}
		if intent.identify() != nil {
			if UsableColumnName(col.Intent) {
				// Semantic discovery accepts headers wider than the published lookup.
				results = append(results, batchPending(col, BatchRequest, w.now()))
			} else {
				results = append(results, batchBlocked(col, "ANALYSIS_COLUMN_INVALID"))
			}
			continue
		}
		if groups[intent.ID] == nil {
			intents = append(intents, intent)
		}
		groups[intent.ID] = append(groups[intent.ID], col.Ordinal)
	}
	if len(intents) == 0 {
		return results, time.Time{}, nil
	}
	task := batchTask(intents, limits.Hits, groups, nil, limits.ResponseBytes)
	eligible := func() BatchWork {
		active := job
		active.Columns = make([]BatchWorkColumn, 0, len(task.Columns))
		for _, col := range job.Columns {
			if slices.Contains(task.Columns, col.Ordinal) {
				active.Columns = append(active.Columns, col)
			}
		}
		return active
	}
	cap, _ := Find(w.Manifest, BatchTool)
	visibility, decision, err := (Coordinator{Service: w.Service, Manifest: w.Manifest}).publishedScope(ctx, i, cap)
	if err != nil || visibility == "" {
		return append(results, batchBlock(eligible(), batchOwnerFailure(orchestration.OwnerObservation{}, err))...), time.Time{}, nil
	}
	if len(task.Arguments)+envelopeSize(i, cap, decision, task.MaxBytes) > limits.RequestBytes {
		return batchPublishedSplit(job, results, w.now()), time.Time{}, nil
	}
	if err := w.begin(ctx, job, task.Columns); err != nil {
		return nil, time.Time{}, err
	}
	r, callErr := w.call(job.dispatchContext(ctx, task.Columns), i, checksum, job, BatchTool, task.Arguments, "", limits.RequestBytes, task.MaxBytes, task.MaxObjects)
	if r.Observation.At.IsZero() && ctx.Err() != nil {
		return nil, time.Time{}, ctx.Err()
	}
	if callErr != nil || r.Problem != nil || r.Replay || r.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
		if r.Problem != nil && (r.Problem.Status == 413 || r.Problem.Status == 422) {
			return batchPublishedSplit(job, results, maxTime(w.now(), r.Observation.Due)), time.Time{}, nil
		}
		retries, pause, err := w.delay(eligible(), r, callErr, time.Time{})
		return append(results, retries...), pause, err
	}
	accepted, valid := AcceptedBatch(task, r.Body)
	if !valid {
		return append(results, batchBlock(eligible(), InvalidOwner)...), time.Time{}, nil
	}
	byOrdinal := make(map[int]BatchResult, len(job.Columns))
	for _, evidence := range accepted {
		for _, ordinal := range groups[evidence.ID] {
			byOrdinal[ordinal] = evidence
		}
	}
	for _, col := range job.Columns {
		evidence, found := byOrdinal[col.Ordinal]
		if !found {
			continue
		}
		if !col.RetryStarted.IsZero() && !w.now().Before(col.RetryStarted.Add(batchBudget)) {
			results = append(results, batchBlocked(col, "RETRY_BUDGET_EXHAUSTED"))
			continue
		}
		if evidence.State == "MATCH" {
			match := evidence.Hits[0]
			results = append(results, BatchWorkResult{Ordinal: col.Ordinal, State: "CLOSED", Outcome: Outcome{
				State: "MATCHED_PUBLISHED", Match: &match}})
		} else {
			results = append(results, batchPending(col, BatchRequest, w.now()))
		}
	}
	return results, time.Time{}, nil
}

func batchPublishedSplit(job BatchWork, results []BatchWorkResult, now time.Time) []BatchWorkResult {
	for _, col := range job.Columns {
		if slices.ContainsFunc(results, func(r BatchWorkResult) bool { return r.Ordinal == col.Ordinal }) {
			continue
		}
		if len(job.Columns) == 1 {
			results = append(results, batchBlocked(col, "SEM_BATCH_SIZE_LIMIT"))
		} else {
			next := batchPending(col, BatchPublished, now)
			next.PublishedSolo, next.RetryAttempts, next.RetryStarted = true, col.RetryAttempts, col.RetryStarted
			results = append(results, next)
		}
	}
	return results
}

func batchPending(col BatchWorkColumn, stage string, due time.Time) BatchWorkResult {
	return BatchWorkResult{Ordinal: col.Ordinal, Stage: stage, State: "PENDING", Due: due,
		OwnerEnd: col.OwnerEnd, CompletedCount: col.CompletedCount, Outcome: Outcome{State: "PENDING", RequestID: col.RequestID}}
}

func batchBlocked(col BatchWorkColumn, code string) BatchWorkResult {
	return BatchWorkResult{Ordinal: col.Ordinal, State: "CLOSED", RetryAttempts: col.RetryAttempts,
		RetryStarted: col.RetryStarted, OwnerEnd: col.OwnerEnd, CompletedCount: col.CompletedCount,
		Outcome: Outcome{State: "BLOCKED", Reason: code, RequestID: col.RequestID}}
}

func batchBlock(job BatchWork, code string) []BatchWorkResult {
	results := make([]BatchWorkResult, 0, len(job.Columns))
	for _, col := range job.Columns {
		results = append(results, batchBlocked(col, code))
	}
	return results
}

func batchTransient(obs orchestration.OwnerObservation) bool {
	return obs.Reason == "OWNER_TIMEOUT" || obs.Status == http.StatusTooManyRequests || obs.Status == http.StatusServiceUnavailable || obs.Status == http.StatusRequestTimeout
}

func batchOwnerFailure(obs orchestration.OwnerObservation, err error) string {
	switch {
	case errors.Is(err, ErrEpochChanged):
		return ErrEpochChanged.Error()
	case errors.Is(err, orchestration.ErrUnauthorized) || obs.Status == 403 || obs.Status == 401:
		return "AUTHORIZATION_DENIED"
	case obs.Reason == "RETRY_WINDOW_UNAVAILABLE":
		return "RETRY_WINDOW_UNAVAILABLE"
	case errors.Is(err, orchestration.ErrResultLimit) || obs.Code == "SUCCEEDED":
		return InvalidOwner
	case obs.Status >= 400 && obs.Status < 500:
		return "OWNER_REQUEST_REJECTED"
	default:
		return "OUTCOME_REQUIRES_RECONCILIATION"
	}
}

func (w BatchWorker) delay(job BatchWork, r orchestration.Result, err error, forced time.Time) ([]BatchWorkResult, time.Time, error) {
	var window *orchestration.WindowBudgetExhaustedError
	windowWait := errors.As(err, &window)
	if windowWait {
		forced = maxTime(forced, window.BudgetWindowEnd())
	}
	excluded := errors.Is(err, ErrObservationInterval)
	if excluded {
		forced = maxTime(forced, r.Observation.Due)
	}
	metric := ""
	if excluded {
		metric = "GET_EXCLUDED"
	}
	if r.Observation.Status == http.StatusTooManyRequests {
		metric = "429_UNATTESTED"
		if r.Observation.Code == "SEM_BATCH_RATE_LIMITED" || r.Observation.Code == "SEM_DISCOVERY_RATE_LIMITED" {
			metric = "429_SEMANTIC_CODE"
		}
	}
	if r.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" || (!batchTransient(r.Observation) && forced.IsZero()) {
		results := batchBlock(job, batchOwnerFailure(r.Observation, err))
		results[0].TrafficMetric = metric
		return results, time.Time{}, nil
	}
	now := w.now()
	results := make([]BatchWorkResult, 0, len(job.Columns))
	var pause time.Time
	for _, col := range job.Columns {
		started, attempts := col.RetryStarted, col.RetryAttempts
		if started.IsZero() {
			started = now
		}
		delay := [...]time.Duration{5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}[min(max(attempts-1, 0), 4)]
		delay += time.Duration(rand.Int64N(int64(delay)/5 + 1))
		floor := now.Add(delay)
		if !forced.IsZero() {
			floor = forced
		}
		due := maxTime(floor, r.Observation.Due)
		if r.Observation.Status == 429 || r.Observation.Status == 503 {
			pause = maxTime(pause, due)
		}
		next := batchPending(col, job.Stage, due)
		if attempts >= MaxRetryAttempts || !due.Before(started.Add(batchBudget)) {
			next = batchBlocked(col, "RETRY_BUDGET_EXHAUSTED")
		}
		next.RetryStarted, next.RetryAttempts = started, attempts
		results = append(results, next)
	}
	results[0].TrafficMetric = metric
	return results, pause, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func batchDiscoveryResult(col BatchWorkColumn, stage string, r orchestration.Result) BatchWorkResult {
	var receipt struct {
		RequestID, State, RecoveryState string
		BudgetDeadline                  *time.Time
	}
	if !ValidEvidenceJSON(r.Body) || json.Unmarshal(r.Body, &receipt) != nil || !CanonicalUUID(receipt.RequestID) ||
		(col.RequestID != "" && col.RequestID != receipt.RequestID) {
		return batchBlocked(col, InvalidOwner)
	}
	col.RequestID = receipt.RequestID
	if col.OwnerEnd.IsZero() {
		col.OwnerEnd = r.Observation.At.Add(batchBudget)
	}
	if receipt.BudgetDeadline != nil && !receipt.BudgetDeadline.IsZero() && receipt.State != "SUCCEEDED" {
		col.OwnerEnd = minTime(col.OwnerEnd, *receipt.BudgetDeadline)
	}
	switch receipt.RecoveryState {
	case "BLOCKED", "PAUSED_EXHAUSTED", "CANCELLED":
		return batchBlocked(col, "DISCOVERY_STOPPED")
	}
	due := r.Observation.Due
	if receipt.State == "SUCCEEDED" {
		col.CompletedCount = CompletedCount(col.RequestID, r.Body)
		return batchPending(col, BatchCandidates, maxTime(due, r.Observation.At))
	}
	switch receipt.State {
	case "PENDING", "RUNNING", "RETRY_WAIT":
		floor := r.Observation.At.Add(time.Duration(orchestration.PollIntervalSeconds) * time.Second)
		return batchPending(col, BatchStatus, maxTime(due, floor))
	case "FAILED", "EXHAUSTED", "CANCELLED":
		return batchBlocked(col, "DISCOVERY_"+receipt.State)
	default:
		if stage == BatchRequest && strings.TrimSpace(receipt.State) == "" {
			return batchPending(col, BatchStatus, maxTime(due, r.Observation.At))
		}
		return batchBlocked(col, InvalidOwner)
	}
}

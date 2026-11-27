package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type Coordinator struct {
	MaxColumnReformulations *int
	CommandKey              string
	Store                   Store
	Service                 *orchestration.Service
	Manifest                *manifest.Snapshot
	Onboarding              bool
	PreviewPageSize         int
}

type ownerObservationKeyContext struct{}
type publicObservationContext struct{}
type publicObservation struct{ selector, legacySelector, request string }

// DiscoveryObservationSelector derives a selector without passing the key to storage.
func DiscoveryObservationSelector(ctx context.Context, request string) string {
	key, _ := ctx.Value(ownerObservationKeyContext{}).([]byte)
	if len(key) == 0 {
		return ""
	}
	return orchestration.DiscoveryObservationSelector(key, request)
}

var ErrObservationInterval = errors.New("OBSERVATION_INTERVAL")

// WithPublicObservation binds a public discovery read to its saved clock.
func WithPublicObservation(ctx context.Context, selector, legacySelector, request string) context.Context {
	return context.WithValue(ctx, publicObservationContext{}, publicObservation{selector: selector, legacySelector: legacySelector, request: request})
}

func PublicObservation(ctx context.Context) (selector, legacySelector, request string) {
	ref, _ := ctx.Value(publicObservationContext{}).(publicObservation)
	return ref.selector, ref.legacySelector, ref.request
}

func (c Coordinator) withOwnerObservationKey(ctx context.Context) context.Context {
	if c.Service == nil || len(c.Service.FingerprintKey) == 0 {
		return ctx
	}
	return WithOwnerObservationKey(ctx, c.Service.FingerprintKey)
}

// WithOwnerObservationKey lets a plan or summary read derive observation selectors.
func WithOwnerObservationKey(ctx context.Context, key []byte) context.Context {
	return context.WithValue(ctx, ownerObservationKeyContext{}, key)
}

func (c Coordinator) authorize(ctx context.Context, i orchestration.Identity, cap manifest.Capability) error {
	_, err := c.authorization(ctx, i, cap)
	return err
}
func (c Coordinator) authorization(ctx context.Context, i orchestration.Identity, cap manifest.Capability) (string, error) {
	d, err := c.decision(ctx, i, cap)
	return d.DecisionRef, err
}

func (c Coordinator) decision(ctx context.Context, i orchestration.Identity, cap manifest.Capability) (orchestration.AuthorizationDecision, error) {
	if i.Delegation == "" {
		return orchestration.AuthorizationDecision{}, &orchestration.AuthorizationDeniedError{ReasonCode: "DELEGATION_REQUIRED"}
	}
	if i.ActorType != "HUMAN" {
		return orchestration.AuthorizationDecision{}, &orchestration.AuthorizationDeniedError{ReasonCode: "ACTOR_NOT_ALLOWED"}
	}
	if !IdentityScope(i).Valid() {
		return orchestration.AuthorizationDecision{}, orchestration.ErrUnauthorized
	}
	d, e := c.Service.Auth.Authorize(ctx, orchestration.AuthorizationRequest{Identity: i, Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: i.TenantID}, CapabilityID: cap.RequiredAuthorization, Owner: cap.Owner, OperationClass: cap.OperationClass})
	if e != nil {
		return orchestration.AuthorizationDecision{}, e
	}
	if !d.Allowed || d.DecisionRef == "" {
		return orchestration.AuthorizationDecision{}, &orchestration.AuthorizationDeniedError{ReasonCode: d.DecisionCode, PolicyVersion: d.BundleVersion, CapabilityID: cap.RequiredAuthorization}
	}
	return d, nil
}
func (c Coordinator) check(ctx context.Context, i orchestration.Identity) (string, error) {
	preview, ok := Find(c.Manifest, "source.file.preview")
	if !ok || preview.PublicationState != "ACTIVE" {
		return "", ErrUnavailable
	}
	if e := c.authorize(ctx, i, preview); e != nil {
		return "", e
	}
	return c.Manifest.Checksum()
}
func decisionEpoch(d orchestration.AuthorizationDecision) string {
	if d.BundleID == "" && d.BundleVersion == 0 {
		return ""
	}
	return Digest([]any{d.BundleID, d.BundleVersion})
}

func (c Coordinator) Epoch(ctx context.Context, i orchestration.Identity) (string, error) {
	preview, ok := Find(c.Manifest, "source.file.preview")
	if !ok || preview.PublicationState != "ACTIVE" {
		return "", ErrUnavailable
	}
	d, err := c.decision(ctx, i, preview)
	return decisionEpoch(d), err
}

func (c Coordinator) CheckEpoch(ctx context.Context, i orchestration.Identity, stored string) error {
	if stored == "" {
		return nil
	}
	current, err := c.Epoch(ctx, i)
	if err == nil && current != stored {
		return ErrEpochChanged
	}
	return err
}

// PlanEpochError identifies the owned analysis recorded by a successful plan command.
type PlanEpochError struct{ AnalysisID string }

func (e *PlanEpochError) Error() string { return ErrEpochChanged.Error() }
func (e *PlanEpochError) Unwrap() error { return ErrEpochChanged }

func (c Coordinator) Plan(ctx context.Context, i orchestration.Identity, asset, profile string, expected int64, work []Work) (View, error) {
	ctx = c.withOwnerObservationKey(ctx)
	epoch, e := c.Epoch(ctx, i)
	if e != nil {
		return View{}, e
	}
	checksum, e := c.Manifest.Checksum()
	if e != nil {
		return View{}, e
	}
	if uuid.Validate(asset) != nil || uuid.Validate(profile) != nil || len(work) < 1 || len(work) > MaxPlanBatch {
		return View{}, ErrInvalid
	}
	for _, w := range work {
		if err := validateDiscoveryArguments(w); err != nil {
			return View{}, err
		}
	}
	// The owner contract makes profile IDs immutable. Reuse the previously
	// verified metadata for later plan batches, retaining current authorization.
	// Otherwise eight batches for 1024 columns repeat the same preview query and
	// hit its equivalent-query guard even though they describe different work.
	scope := IdentityScope(i)
	pin := Profile{AssetID: asset, ProfileID: profile}
	logicalID := AnalysisID(scope, pin, epoch)
	checkReceipt := func(v View) (View, error) {
		err := c.CheckPublishedScope(ctx, i, v.Visibility)
		if (err == nil || errors.Is(err, ErrScope)) && v.Epoch != "" && v.Epoch != epoch {
			return View{}, &PlanEpochError{AnalysisID: v.ID}
		}
		return v, err
	}
	var intent string
	if c.CommandKey != "" {
		// Existing receipts hash the submitted work, including UUID spelling.
		intent = Digest([]any{asset, profile, expected, work})
		if receipts, ok := c.Store.(interface {
			LookupPlanCommand(context.Context, Scope, string, string, string) (View, bool, error)
		}); ok {
			view, found, err := receipts.LookupPlanCommand(ctx, scope, checksum, c.CommandKey, intent)
			if err != nil {
				return view, err
			}
			if found {
				return checkReceipt(view)
			}
		}
	}
	save := func(p Plan) (View, error) {
		if c.MaxColumnReformulations != nil {
			p.MaxColumnReformulations = *c.MaxColumnReformulations
		} else {
			limits, err := SummaryLimits(c.Manifest)
			if err != nil {
				return View{}, err
			}
			p.MaxColumnReformulations = limits.MaxColumnReformulations
		}
		p.CommandKey, p.CommandDigest = c.CommandKey, intent
		v, err := c.Store.PlanAnalysis(ctx, p)
		if err == nil && c.CommandKey != "" && v.ID != p.ID {
			// A concurrent command may have recorded its receipt after lookup.
			return checkReceipt(v)
		}
		return v, err
	}
	retained, readErr := c.Store.ReadAnalysis(ctx, scope, logicalID, checksum, 0, 1)
	if errors.Is(readErr, ErrScope) && epoch != "" {
		legacyID := AnalysisID(scope, pin)
		old, err := c.Store.ReadAnalysis(ctx, scope, legacyID, checksum, 0, 1)
		if err != nil && !errors.Is(err, ErrScope) {
			return View{}, err
		}
		if err == nil && old.Visibility != "" {
			err = c.CheckPublishedScope(ctx, i, old.Visibility)
			if err == nil {
				retained, logicalID, readErr = old, legacyID, nil
			} else if !errors.Is(err, ErrScope) {
				return View{}, err
			}
		}
	}
	if readErr == nil {
		if retained.Seed != "" {
			if err := c.CheckPublishedScope(ctx, i, retained.Visibility); err != nil {
				return View{}, err
			}
			if expected != 0 || retained.Seed != Digest(work) {
				return View{}, ErrConflict
			}
			if c.CommandKey != "" {
				p := Plan{ID: logicalID, Scope: scope, Profile: retained.Profile, Manifest: checksum, Seed: retained.Seed, Visibility: retained.Visibility, Epoch: epoch}
				if logicalID == AnalysisID(scope, pin) {
					p.Epoch = ""
				}
				return save(p)
			}
			if PermissionHold(retained.StopReason) {
				return View{}, ErrPolicyUnchanged
			}
			return c.Store.ReadAnalysis(ctx, scope, logicalID, checksum, 0, 128)
		}
		var plan Plan
		var buildErr error
		if cap, ok := Find(c.Manifest, BatchTool); onlyDiscovery(work) && (!ok || cap.PublicationState == "INACTIVE") {
			plan, buildErr = buildWithoutBatch(scope, retained.Profile, c.Manifest, nil, expected, work, epoch)
		} else {
			plan, buildErr = Build(scope, retained.Profile, c.Manifest, expected, work, epoch)
		}
		if buildErr != nil {
			return View{}, buildErr
		}
		return save(plan)
	}
	if !errors.Is(readErr, ErrScope) || expected != 0 {
		return View{}, readErr
	}
	limits, e := SummaryLimits(c.Manifest)
	if e != nil {
		return View{}, e
	}
	if c.PreviewPageSize != 0 {
		if c.PreviewPageSize < 1 || c.PreviewPageSize > 128 {
			return View{}, ErrInvalid
		}
		limits.PreviewPageSize = c.PreviewPageSize
	}
	var profilePin Profile
	var headers []string
	checkpoints, durable := c.Store.(PreviewStore)
	if durable {
		profilePin, headers, e = checkpoints.PreviewProgress(ctx, scope, logicalID, checksum, epoch)
		if e != nil {
			return View{}, e
		}
	}
	for len(headers) == 0 || len(headers) < profilePin.ColumnCount {
		offset := len(headers)
		body, e := c.readPreviewPage(ctx, i, checksum, asset, profile, offset, limits.PreviewPageSize)
		if e != nil {
			return View{}, e
		}
		page, names, err := previewPage(body, asset, profile, profilePin, offset, limits.PreviewPageSize)
		if err != nil {
			return View{}, err
		}
		profilePin = page
		if durable {
			if e = checkpoints.SavePreviewPage(ctx, scope, logicalID, checksum, page, offset, names, epoch); e != nil {
				return View{}, e
			}
		}
		headers = append(headers, names...)
	}
	batch, available := Find(c.Manifest, BatchTool)
	if onlyDiscovery(work) && available && batch.PublicationState == "ACTIVE" {
		decision, err := c.decision(ctx, i, batch)
		if err != nil {
			return View{}, err
		}
		limits, err := PublishedLimits(batch)
		if err != nil {
			return View{}, err
		}
		if decisionEpoch(decision) != epoch {
			return View{}, ErrEpochChanged
		}
		p, err := BuildPublished(scope, profilePin, c.Manifest, headers, work, visibilityDigest(i, decision), envelopeSize(i, batch, decision.DecisionRef, limits.ResponseBytes), epoch)
		if err != nil {
			return View{}, err
		}
		p.Onboarding = c.Onboarding
		return save(p)
	}
	if onlyDiscovery(work) && available && batch.PublicationState == "REVOKED" {
		return View{}, ErrUnavailable
	}
	var plan Plan
	if onlyDiscovery(work) {
		plan, e = buildWithoutBatch(scope, profilePin, c.Manifest, headers, expected, work, epoch)
	} else {
		plan, e = Build(scope, profilePin, c.Manifest, expected, work, epoch)
		plan.Headers = headers
	}
	if e != nil {
		return View{}, e
	}
	plan.Onboarding = c.Onboarding
	return save(plan)
}
func (c Coordinator) Read(ctx context.Context, i orchestration.Identity, id string, offset, limit int) (View, error) {
	checksum, e := c.check(ctx, i)
	if e != nil {
		return View{}, e
	}
	v, err := c.Store.ReadAnalysis(c.withOwnerObservationKey(ctx), IdentityScope(i), id, checksum, offset, limit)
	if err == nil {
		err = c.CheckPublishedScope(ctx, i, v.Visibility)
		if errors.Is(err, ErrScope) && errors.Is(c.CheckEpoch(ctx, i, v.Epoch), ErrEpochChanged) {
			// ReadAnalysis projects metadata only. Evidence and execution
			// remain subject to their original visibility fence.
			err = nil
		}
	}
	return v, err
}
func (c Coordinator) Step(ctx context.Context, i orchestration.Identity, id, taskID string) (Task, error) {
	checksum, e := c.check(ctx, i)
	if e != nil {
		return Task{}, e
	}
	if ValidateID(id) != nil || ValidateID(taskID) != nil {
		return Task{}, ErrInvalid
	}
	scope := IdentityScope(i)
	metadata, e := c.Store.ReadAnalysisTask(ctx, scope, id, taskID, checksum)
	if e != nil {
		return Task{}, e
	}
	cap, ok := Find(c.Manifest, metadata.Tool)
	if !ok || (!allowed(metadata.Tool) && metadata.Tool != BatchTool) || cap.PublicationState != "ACTIVE" {
		return Task{}, ErrUnavailable
	}
	// A previously saved result is historical evidence. Even its reread requires
	// current authorization for the underlying semantic capability.
	if e = c.CheckEpoch(ctx, i, metadata.Epoch); e != nil {
		return Task{}, e
	}
	if e = c.CheckStepScope(ctx, i, id, checksum, metadata.Visibility); e != nil {
		return Task{}, e
	}
	if metadata.Tool != BatchTool {
		if e = c.authorize(ctx, i, cap); e != nil {
			return Task{}, e
		}
	}
	task, e := c.Store.ClaimAnalysisTask(c.withOwnerObservationKey(ctx), scope, id, taskID, checksum)
	if e != nil {
		return Task{}, e
	}
	if task.State != "RUNNING" || task.Fence == "" {
		return c.withAnalysisSuccessor(ctx, scope, id, checksum, task), nil
	}
	start := time.Now()
	dispatchKey := "analysis:" + Digest([]any{id, task.ID, task.Attempts})
	if task.OwnerKey != "" {
		dispatchKey = task.OwnerKey
	}
	var bundleVersion int64
	checkDecision := func(d orchestration.AuthorizationDecision) error {
		bundleVersion = d.BundleVersion
		if task.Epoch != "" && task.Epoch != decisionEpoch(d) {
			return ErrEpochChanged
		}
		if task.Tool == BatchTool && visibilityDigest(i, d) != task.Visibility {
			return ErrScope
		}
		return nil
	}
	var maxRequest int
	if task.Tool == BatchTool {
		l, err := PublishedLimits(cap)
		if err != nil {
			return Task{}, err
		}
		maxRequest = l.RequestBytes
	}
	r, callErr := c.Service.Call(ctx, orchestration.Invocation{
		Identity: i, CapabilityID: cap.CapabilityID, Owner: cap.Owner, OperationClass: cap.OperationClass,
		AuthorizationCapabilityID: cap.RequiredAuthorization, GatewayBindingRef: cap.GatewayBindingRef,
		MaxRequestBytes: maxRequest, CheckDecision: checkDecision, ManifestChecksum: checksum,
		Arguments: task.Arguments, IdempotencyKey: dispatchKey, CorrelationID: uuid.NewString(),
		Window: orchestration.AdmissionWindow, Timeout: time.Duration(task.MaxTimeMS) * time.Millisecond,
		RetryThreshold: orchestration.AdmissionRetryThreshold,
		Maximum:        orchestration.Cost{ToolCalls: 1, DistinctObjects: task.MaxObjects, ResultBytes: task.MaxBytes},
	})
	f := Finish{State: "HELD", Code: "OUTCOME_REQUIRES_RECONCILIATION", ElapsedMS: time.Since(start).Milliseconds(), Observation: r.Observation}
	if r.AttemptID != uuid.Nil {
		f.AttemptID = r.AttemptID.String()
	}
	var window *orchestration.WindowBudgetExhaustedError
	if errors.As(callErr, &window) {
		f.State = "WAITING"
		f.Code = "ADMISSION_WINDOW_WAIT"
		f.NotBefore = window.BudgetWindowEnd()
		f.Dimension = window.Dimension
		f.WindowWait = true
		f.ElapsedMS = 0
	} else if callErr != nil {
		if errors.Is(callErr, ErrEpochChanged) {
			f.Code = ErrEpochChanged.Error()
		}
		if errors.Is(callErr, orchestration.ErrUnauthorized) {
			f.Code = "AUTHORIZATION_DENIED"
		}
	} else if r.Replay {
		f.Code = "OWNER_RESULT_RECONCILIATION_REQUIRED"
	} else if r.Problem != nil {
		f.Calls = 1
		f.Code = "OWNER_REQUEST_REJECTED"
		if r.Problem.Status == http.StatusForbidden {
			f.Code = orchestration.SafeDenialReason(r.Problem.Code, "OWNER_AUTHORIZATION_DENIED")
			f.PolicyVersion = r.Problem.PolicyVersion
		}
	} else if len(r.Body) > 0 && len(r.Body) <= int(task.MaxBytes) && json.Valid(r.Body) {
		f.State = "SUCCEEDED"
		f.Code = "OWNER_RESPONSE_SAVED_NOT_ADOPTED"
		f.Calls = 1
		f.Body = r.Body
		if r.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
			f.State, f.Code, f.Body = "HELD", InvalidOwner, nil
		}
	}
	transient := r.Observation.Reason == "OWNER_TIMEOUT" || (r.Problem != nil &&
		(r.Problem.Status == 429 || r.Problem.Status == 503 || r.Problem.Status == 408 || transientPolicyDenial(r.Problem, bundleVersion)))
	if cap.OperationClass == "READ" && !r.Replay && r.AttemptID != uuid.Nil && transient {
		delay := []time.Duration{5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}[min(task.Attempts-1, 4)]
		delay += time.Duration(rand.Int64N(int64(delay)/5 + 1))
		f.Calls = 1
		var header string
		if r.Problem != nil {
			header = r.Problem.RetryAfter
		}
		var valid bool
		f.NotBefore, valid = readRetryTime(r.Observation.At, delay, header, r.Body)
		if r.Observation.Due.After(f.NotBefore) {
			f.NotBefore = r.Observation.Due
		}
		switch {
		case !valid || r.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE":
			f.Code = InvalidOwner
		case task.Attempts < MaxRetryAttempts && f.NotBefore.Before(task.FirstAttemptAt.Add(24*time.Hour)):
			f.State, f.Code = "WAITING", "TRANSIENT_OWNER_WAIT"
		default:
			f.Code = "RETRY_BUDGET_EXHAUSTED"
		}
	}
	if task.Tool == BatchTool {
		f = c.batchFinish(task, id, r, callErr, f)
	}
	if r.Observation.Due.After(f.NotBefore) {
		f.NotBefore = r.Observation.Due
	}
	// Reconciliation has its own bounded context. A disconnected MCP client must
	// not erase an already observed outcome; a failed write leaves a fenced hold.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	finished, err := c.Store.FinishAnalysisTask(finishCtx, scope, id, taskID, task.Fence, f)
	cancel()
	if err != nil {
		return finished, err
	}
	if errors.Is(callErr, orchestration.ErrUnauthorized) || errors.Is(callErr, ErrEpochChanged) {
		return finished, callErr
	}
	if r.Problem != nil && r.Problem.Status == http.StatusForbidden && f.State == "HELD" && f.Code != "RETRY_BUDGET_EXHAUSTED" {
		return finished, &OwnerDenial{ReasonCode: f.Code, CapabilityID: cap.RequiredAuthorization, PolicyVersion: f.PolicyVersion}
	}
	return c.withAnalysisSuccessor(ctx, scope, id, checksum, finished), nil
}

func (c Coordinator) withAnalysisSuccessor(ctx context.Context, scope Scope, id, manifest string, task Task) Task {
	if task.Tool != "semantic.discovery.status" || task.State != "SUCCEEDED" {
		return task
	}
	// Start a fresh read budget once the outcome (or replay read) is committed.
	readCtx, cancel := context.WithTimeout(ctx, time.Duration(task.MaxTimeMS)*time.Millisecond)
	defer cancel()
	successor, err := c.Store.ReadAnalysisSuccessor(c.withOwnerObservationKey(readCtx), scope, id, manifest, task)
	if err != nil {
		task.SuccessorUnavailable = true
	} else {
		task.Successor = successor
	}
	return task
}

func PermissionReasons() []string {
	return []string{"NO_APPLICABLE_GRANT", "CAPABILITY_NOT_DECLARED", "SCOPE_MISSING", "ACTOR_NOT_ALLOWED", "EXPLICIT_DENY", "OWNER_AUTHORIZATION_DENIED"}
}

func PermissionHold(code string) bool {
	return slices.Contains(PermissionReasons(), code)
}

func transientPolicyDenial(p *orchestration.Problem, version int64) bool {
	if p.Status != http.StatusForbidden {
		return false
	}
	switch p.Code {
	case "STALE_POLICY_BUNDLE", "NO_POLICY_BUNDLE":
		return true
	case "ACTOR_NOT_ALLOWED", "CAPABILITY_NOT_DECLARED", "DATA_LABEL_REQUIRED", "EXPLICIT_DENY", "HUMAN_REQUIRED",
		"NO_APPLICABLE_GRANT", "SCOPE_MISSING", "TENANT_MISMATCH", "SEM_READ_POLICY_DENIED", "SEM_DISCOVERY_POLICY_DENIED":
		return p.PolicyVersion != nil && *p.PolicyVersion >= 0 && *p.PolicyVersion < version
	}
	return false
}

type OwnerDenial struct {
	ReasonCode, CapabilityID string
	PolicyVersion            *int64
}

func (*OwnerDenial) Error() string { return "owner authorization denied" }

func readRetryTime(now time.Time, backoff time.Duration, header string, body []byte) (time.Time, bool) {
	var obs map[string]any
	// An unstructured HTTP error can still supply Retry-After.
	if body = bytes.TrimSpace(body); len(body) > 0 && body[0] == '{' {
		if json.Unmarshal(body, &obs) != nil {
			return time.Time{}, false
		}
	}
	return orchestration.OwnerDue(obs, now, now.Add(backoff), header)
}

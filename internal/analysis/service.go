package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type Coordinator struct {
	Store           Store
	Service         *orchestration.Service
	Manifest        *manifest.Snapshot
	Onboarding      bool
	PreviewPageSize int
}

func (c Coordinator) authorize(ctx context.Context, i orchestration.Identity, cap manifest.Capability) error {
	_, err := c.authorization(ctx, i, cap)
	return err
}
func (c Coordinator) authorization(ctx context.Context, i orchestration.Identity, cap manifest.Capability) (string, error) {
	if i.Delegation == "" {
		return "", &orchestration.AuthorizationDeniedError{ReasonCode: "DELEGATION_REQUIRED"}
	}
	if i.ActorType != "HUMAN" {
		return "", &orchestration.AuthorizationDeniedError{ReasonCode: "ACTOR_NOT_ALLOWED"}
	}
	if !IdentityScope(i).Valid() {
		return "", orchestration.ErrUnauthorized
	}
	d, e := c.Service.Auth.Authorize(ctx, orchestration.AuthorizationRequest{Identity: i, Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: i.TenantID}, CapabilityID: cap.RequiredAuthorization, Owner: cap.Owner, OperationClass: cap.OperationClass})
	if e != nil {
		return "", e
	}
	if !d.Allowed || d.DecisionRef == "" {
		return "", &orchestration.AuthorizationDeniedError{ReasonCode: d.DecisionCode, PolicyVersion: d.BundleVersion, CapabilityID: cap.RequiredAuthorization}
	}
	return d.DecisionRef, nil
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
func (c Coordinator) Plan(ctx context.Context, i orchestration.Identity, asset, profile string, expected int64, work []Work) (View, error) {
	return c.plan(ctx, i, asset, profile, nil, expected, work)
}

func (c Coordinator) plan(ctx context.Context, i orchestration.Identity, asset, profile string, unit *UnitProfileRef, expected int64, work []Work) (View, error) {
	checksum, e := c.check(ctx, i)
	if e != nil {
		return View{}, e
	}
	if (unit == nil && uuid.Validate(asset) != nil) || uuid.Validate(profile) != nil || len(work) < 1 || len(work) > MaxPlanBatch {
		return View{}, ErrInvalid
	}
	// The owner contract makes profile IDs immutable. Reuse the previously
	// verified metadata for later plan batches, retaining current authorization.
	// Otherwise eight batches for 1024 columns repeat the same preview query and
	// hit its equivalent-query guard even though they describe different work.
	scope := IdentityScope(i)
	preview, _ := Find(c.Manifest, "source.file.preview")
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
	var first Profile
	var firstHeaders []string
	key := Profile{AssetID: asset, ProfileID: profile}
	if unit != nil {
		key.UnitRef = unit.UnitRef
		if unit.UnitRef == "table:0" {
			// The legacy ID needs the owner-resolved asset. Reuse this page below.
			first, firstHeaders, e = c.readProfilePage(ctx, i, preview, checksum, "", profile, unit, Profile{}, 0, limits.PreviewPageSize)
			if e != nil {
				return View{}, e
			}
			asset, key.AssetID = first.AssetID, first.AssetID
		}
	}
	logicalID := AnalysisID(scope, key)
	retained, storedManifest, readErr := ReadCompatible(checksum, func(sum string) (View, error) {
		return c.Store.ReadAnalysis(ctx, scope, logicalID, sum, 0, 1)
	})
	if readErr == nil {
		if unit != nil {
			if firstHeaders == nil {
				first, _, e = c.readProfilePage(ctx, i, preview, checksum, asset, profile, unit, Profile{}, 0, limits.PreviewPageSize)
				if e != nil {
					return View{}, e
				}
			}
			if first != retained.Profile {
				return View{}, ErrProfile
			}
		}
		if retained.Seed != "" {
			if err := c.CheckPublishedScope(ctx, i, retained.Visibility); err != nil {
				return View{}, err
			}
			if expected != 0 || retained.Seed != Digest(work) {
				return View{}, ErrConflict
			}
			return c.Store.ReadAnalysis(ctx, scope, logicalID, storedManifest, 0, 128)
		}
		build := Build
		if cap, ok := Find(c.Manifest, BatchTool); onlyDiscovery(work) && (!ok || cap.PublicationState == "INACTIVE") {
			build = buildWithoutBatch
		}
		plan, buildErr := build(scope, retained.Profile, c.Manifest, expected, work)
		if buildErr != nil {
			return View{}, buildErr
		}
		plan.Manifest = storedManifest
		return c.Store.PlanAnalysis(ctx, plan)
	}
	if !errors.Is(readErr, ErrScope) || expected != 0 {
		return View{}, readErr
	}
	var profilePin Profile
	var headers []string
	// Only an exactly admitted 1.18/1.17 pair shares CSV storage. The
	// predecessor can read the same five-field plan after rollback. Owner
	// calls and admission always use the actual running manifest checksum.
	planManifest := checksum
	if old := manifest.AnalysisPredecessor(checksum); manifest.AnalysisPredecessor(old) != "" && (unit == nil || unit.UnitRef == "table:0") {
		planManifest = old
	}
	progressManifest := checksum
	checkpoints, durable := c.Store.(PreviewStore)
	if durable {
		profilePin, headers, e = checkpoints.PreviewProgress(ctx, scope, logicalID, progressManifest)
		if e == nil && len(headers) == 0 && planManifest != checksum {
			progressManifest = planManifest
			profilePin, headers, e = checkpoints.PreviewProgress(ctx, scope, logicalID, progressManifest)
		}
		if e != nil {
			return View{}, e
		}
	}
	if len(headers) > 0 && unit != nil {
		if firstHeaders != nil && first != profilePin {
			return View{}, ErrProfile
		}
		if unit.UnitRef != "table:0" {
			if profilePin.UnitRef != unit.UnitRef {
				return View{}, ErrProfile
			}
			if profilePin.Version != unit.Version {
				return View{}, UnitError("ONB_UNIT_PROFILE_CHANGED")
			}
			if profilePin.SchemaHash != unit.SchemaHash {
				return View{}, UnitError("ONB_UNIT_SCHEMA_CHANGED")
			}
		}
	}
	for len(headers) == 0 || len(headers) < profilePin.ColumnCount {
		offset := len(headers)
		page, names := first, firstHeaders
		var err error
		if offset != 0 || names == nil {
			page, names, err = c.readProfilePage(ctx, i, preview, checksum, asset, profile, unit, profilePin, offset, limits.PreviewPageSize)
		}
		if err != nil {
			return View{}, err
		}
		profilePin = page
		if durable {
			if e = checkpoints.SavePreviewPage(ctx, scope, logicalID, progressManifest, page, offset, names); e != nil {
				return View{}, e
			}
		}
		headers = append(headers, names...)
	}
	batch, available := Find(c.Manifest, BatchTool)
	if onlyDiscovery(work) && available && batch.PublicationState == "ACTIVE" {
		visibility, decision, err := c.publishedScope(ctx, i, batch)
		if err != nil {
			return View{}, err
		}
		limits, err := PublishedLimits(batch)
		if err != nil {
			return View{}, err
		}
		p, err := BuildPublished(scope, profilePin, c.Manifest, headers, work, visibility, envelopeSize(i, batch, decision, limits.ResponseBytes))
		if err != nil {
			return View{}, err
		}
		p.Onboarding, p.Manifest = c.Onboarding, planManifest
		return c.Store.PlanAnalysis(ctx, p)
	}
	if onlyDiscovery(work) && available && batch.PublicationState == "REVOKED" {
		return View{}, ErrUnavailable
	}
	build := Build
	if onlyDiscovery(work) {
		build = buildWithoutBatch
	}
	plan, e := build(IdentityScope(i), profilePin, c.Manifest, expected, work)
	if e != nil {
		return View{}, e
	}
	plan.Headers, plan.Onboarding, plan.Manifest = headers, c.Onboarding, planManifest
	return c.Store.PlanAnalysis(ctx, plan)
}
func (c Coordinator) Read(ctx context.Context, i orchestration.Identity, id string, offset, limit int) (View, error) {
	checksum, e := c.check(ctx, i)
	if e != nil {
		return View{}, e
	}
	v, _, err := ReadCompatible(checksum, func(sum string) (View, error) {
		return c.Store.ReadAnalysis(ctx, IdentityScope(i), id, sum, offset, limit)
	})
	if err == nil {
		err = c.CheckPublishedScope(ctx, i, v.Visibility)
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
	metadata, storedManifest, e := ReadCompatible(checksum, func(sum string) (Task, error) {
		return c.Store.ReadAnalysisTask(ctx, scope, id, taskID, sum)
	})
	if e != nil {
		return Task{}, e
	}
	cap, ok := Find(c.Manifest, metadata.Tool)
	if !ok || (!allowed(metadata.Tool) && metadata.Tool != BatchTool) || cap.PublicationState != "ACTIVE" {
		return Task{}, ErrUnavailable
	}
	// A previously saved result is historical evidence. Even its reread requires
	// current authorization for the underlying semantic capability.
	if e = c.CheckStepScope(ctx, i, id, storedManifest, metadata.Visibility); e != nil {
		return Task{}, e
	}
	if metadata.Tool != BatchTool {
		if e = c.authorize(ctx, i, cap); e != nil {
			return Task{}, e
		}
	}
	task, e := c.Store.ClaimAnalysisTask(ctx, scope, id, taskID, storedManifest)
	if e != nil {
		return Task{}, e
	}
	if task.State != "RUNNING" || task.Fence == "" {
		return task, nil
	}
	start := time.Now()
	dispatchKey := "analysis:" + Digest([]any{id, task.ID, task.Attempts})
	if task.OwnerKey != "" {
		dispatchKey = task.OwnerKey
	}
	var checkDecision func(orchestration.AuthorizationDecision) error
	var maxRequest int
	if task.Tool == BatchTool {
		l, err := PublishedLimits(cap)
		if err != nil {
			return Task{}, err
		}
		maxRequest = l.RequestBytes
		checkDecision = func(d orchestration.AuthorizationDecision) error {
			if visibilityDigest(i, d) != task.Visibility {
				return ErrScope
			}
			return nil
		}
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
	f := Finish{State: "HELD", Code: "OUTCOME_REQUIRES_RECONCILIATION", ElapsedMS: time.Since(start).Milliseconds()}
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
		if errors.Is(callErr, orchestration.ErrUnauthorized) {
			f.Code = "AUTHORIZATION_DENIED"
		}
	} else if r.Replay {
		f.Code = "OWNER_RESULT_RECONCILIATION_REQUIRED"
	} else if r.Problem != nil {
		f.Calls = 1
		f.Code = "OWNER_REQUEST_REJECTED"
		if r.Problem.Status == http.StatusForbidden {
			f.Code = "OWNER_AUTHORIZATION_DENIED"
		}
		if cap.OperationClass == "READ" && (r.Problem.Status == 429 || r.Problem.Status == 503) {
			delay := []time.Duration{5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}[min(task.Attempts-1, 4)]
			delay += time.Duration(rand.Int64N(int64(delay)/5 + 1))
			f.NotBefore = time.Now().Add(delay)
			if requested := retryAfter(r.Problem.RetryAfter, time.Now()); requested.After(f.NotBefore) {
				f.NotBefore = requested
			}
			if task.Attempts < MaxRetryAttempts && f.NotBefore.Before(task.FirstAttemptAt.Add(24*time.Hour)) {
				f.State = "WAITING"
				f.Code = "TRANSIENT_OWNER_WAIT"
			} else {
				f.Code = "RETRY_BUDGET_EXHAUSTED"
			}
		}
	} else if len(r.Body) > 0 && len(r.Body) <= int(task.MaxBytes) && json.Valid(r.Body) {
		f.State = "SUCCEEDED"
		f.Code = "OWNER_RESPONSE_SAVED_NOT_ADOPTED"
		f.Calls = 1
		f.Body = r.Body
	}
	if task.Tool == BatchTool {
		f = c.batchFinish(task, id, r, callErr, f)
	}
	// Reconciliation has its own bounded context. A disconnected MCP client must
	// not erase an already observed outcome; a failed write leaves a fenced hold.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	finished, err := c.Store.FinishAnalysisTask(finishCtx, scope, id, taskID, task.Fence, f)
	if err == nil && errors.Is(callErr, orchestration.ErrUnauthorized) {
		return finished, callErr
	}
	if err == nil && r.Problem != nil && r.Problem.Status == http.StatusForbidden {
		return finished, &OwnerDenial{ReasonCode: r.Problem.Code, CapabilityID: cap.RequiredAuthorization}
	}
	return finished, err
}

type OwnerDenial struct{ ReasonCode, CapabilityID string }

func (*OwnerDenial) Error() string { return "owner authorization denied" }

func retryAfter(value string, now time.Time) time.Time {
	if n, e := strconv.ParseInt(value, 10, 64); e == nil && n >= 0 {
		n = min(n, 86401)
		return now.Add(time.Duration(n) * time.Second)
	}
	if at, e := http.ParseTime(value); e == nil {
		return at
	}
	return time.Time{}
}

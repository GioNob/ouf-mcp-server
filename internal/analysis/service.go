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
	checksum, e := c.check(ctx, i)
	if e != nil {
		return View{}, e
	}
	if uuid.Validate(asset) != nil || uuid.Validate(profile) != nil || len(work) < 1 || len(work) > MaxPlanBatch {
		return View{}, ErrInvalid
	}
	// The owner contract makes profile IDs immutable. Reuse the previously
	// verified metadata for later plan batches, retaining current authorization.
	// Otherwise eight batches for 1024 columns repeat the same preview query and
	// hit its equivalent-query guard even though they describe different work.
	scope := IdentityScope(i)
	logicalID := AnalysisID(scope, Profile{AssetID: asset, ProfileID: profile})
	retained, readErr := c.Store.ReadAnalysis(ctx, scope, logicalID, checksum, 0, 1)
	if readErr == nil {
		plan, buildErr := Build(scope, retained.Profile, c.Manifest, expected, work)
		if buildErr != nil {
			return View{}, buildErr
		}
		return c.Store.PlanAnalysis(ctx, plan)
	}
	if !errors.Is(readErr, ErrScope) || expected != 0 {
		return View{}, readErr
	}
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
	var profilePin Profile
	var headers []string
	checkpoints, durable := c.Store.(PreviewStore)
	if durable {
		profilePin, headers, e = checkpoints.PreviewProgress(ctx, scope, logicalID, checksum)
		if e != nil {
			return View{}, e
		}
	}
	for len(headers) == 0 || len(headers) < profilePin.ColumnCount {
		offset := len(headers)
		args, _ := json.Marshal(map[string]any{"assetId": asset, "profileId": profile, "view": "COLUMNS", "offset": offset, "limit": limits.PreviewPageSize})
		r, e := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: preview.CapabilityID, Owner: preview.Owner, OperationClass: "READ", GatewayBindingRef: preview.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(), Window: orchestration.AdmissionWindow, Timeout: 10 * time.Second, RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 1 << 20}})
		if e != nil {
			return View{}, e
		}
		if r.Problem != nil && r.Problem.Status == http.StatusForbidden {
			return View{}, &OwnerDenial{ReasonCode: r.Problem.Code, CapabilityID: preview.CapabilityID}
		}
		if r.Problem != nil && (r.Problem.Status == 503 || r.Problem.Status == 429) {
			return View{}, ErrUnavailable
		}
		if r.Replay || r.Problem != nil {
			return View{}, ErrProfile
		}
		page, names, err := previewPage(r.Body, asset, profile, profilePin, offset, limits.PreviewPageSize)
		if err != nil {
			return View{}, err
		}
		profilePin = page
		if durable {
			if e = checkpoints.SavePreviewPage(ctx, scope, logicalID, checksum, page, offset, names); e != nil {
				return View{}, e
			}
		}
		headers = append(headers, names...)
	}
	plan, e := Build(IdentityScope(i), profilePin, c.Manifest, expected, work)
	if e != nil {
		return View{}, e
	}
	plan.Headers, plan.Onboarding = headers, c.Onboarding
	return c.Store.PlanAnalysis(ctx, plan)
}
func (c Coordinator) Read(ctx context.Context, i orchestration.Identity, id string, offset, limit int) (View, error) {
	checksum, e := c.check(ctx, i)
	if e != nil {
		return View{}, e
	}
	return c.Store.ReadAnalysis(ctx, IdentityScope(i), id, checksum, offset, limit)
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
	if !ok || !allowed(metadata.Tool) || cap.PublicationState != "ACTIVE" {
		return Task{}, ErrUnavailable
	}
	// A previously saved result is historical evidence. Even its reread requires
	// current authorization for the underlying semantic capability.
	if e = c.authorize(ctx, i, cap); e != nil {
		return Task{}, e
	}
	task, e := c.Store.ClaimAnalysisTask(ctx, scope, id, taskID, checksum)
	if e != nil {
		return Task{}, e
	}
	if task.State != "RUNNING" || task.Fence == "" {
		return task, nil
	}
	start := time.Now()
	r, callErr := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: cap.CapabilityID, Owner: cap.Owner, OperationClass: cap.OperationClass, GatewayBindingRef: cap.GatewayBindingRef, ManifestChecksum: checksum, Arguments: task.Arguments, IdempotencyKey: "analysis:" + Digest([]any{id, task.ID, task.Attempts}), CorrelationID: uuid.NewString(), Window: orchestration.AdmissionWindow, Timeout: time.Duration(task.MaxTimeMS) * time.Millisecond, RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: task.MaxObjects, ResultBytes: task.MaxBytes}})
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
	// Reconciliation has its own bounded context. A disconnected MCP client must
	// not erase an already observed outcome; a failed write leaves a fenced hold.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	finished, err := c.Store.FinishAnalysisTask(finishCtx, scope, id, taskID, task.Fence, f)
	if err == nil && errors.Is(callErr, orchestration.ErrUnauthorized) {
		return finished, callErr
	}
	if err == nil && r.Problem != nil && r.Problem.Status == http.StatusForbidden {
		return finished, &OwnerDenial{ReasonCode: r.Problem.Code, CapabilityID: cap.CapabilityID}
	}
	return finished, err
}

type OwnerDenial struct{ ReasonCode, CapabilityID string }

func (*OwnerDenial) Error() string { return "owner authorization denied" }

func retryAfter(value string, now time.Time) time.Time {
	if n, e := strconv.ParseInt(value, 10, 64); e == nil && n >= 0 && n <= 86400 {
		return now.Add(time.Duration(n) * time.Second)
	}
	if at, e := http.ParseTime(value); e == nil {
		return at
	}
	return time.Time{}
}

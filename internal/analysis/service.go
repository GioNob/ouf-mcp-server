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
	Store    Store
	Service  *orchestration.Service
	Manifest *manifest.Snapshot
}

func (c Coordinator) authorize(ctx context.Context, i orchestration.Identity, cap manifest.Capability) error {
	if i.Delegation == "" || i.ActorType != "HUMAN" || !IdentityScope(i).Valid() {
		return orchestration.ErrUnauthorized
	}
	d, e := c.Service.Auth.Authorize(ctx, orchestration.AuthorizationRequest{Identity: i, Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: i.TenantID}, CapabilityID: cap.RequiredAuthorization, Owner: cap.Owner, OperationClass: cap.OperationClass})
	if e != nil {
		return e
	}
	if !d.Allowed || d.DecisionRef == "" {
		return orchestration.ErrUnauthorized
	}
	return nil
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
	return c.PlanSelection(ctx, i, ProfileSelection{AssetID: asset, ProfileID: profile}, expected, work)
}

func (c Coordinator) PlanSelection(ctx context.Context, i orchestration.Identity, selection ProfileSelection, expected int64, work []Work) (View, error) {
	checksum, e := c.check(ctx, i)
	if e != nil {
		return View{}, e
	}
	if !selection.valid() || expected < 0 || len(work) < 1 || len(work) > MaxPlanBatch {
		return View{}, ErrInvalid
	}
	// The owner contract makes profile IDs immutable. Reuse the previously
	// verified metadata for later plan batches, retaining current authorization.
	// Otherwise eight batches for 1024 columns repeat the same preview query and
	// hit its equivalent-query guard even though they describe different work.
	scope := IdentityScope(i)
	ref := selection.UnitProfileRef
	key := Profile{AssetID: selection.AssetID, ProfileID: selection.ProfileID}
	if ref != nil {
		key.ProfileID, key.UnitRef = ref.ProfileID, ref.UnitRef
		if key.AssetID == "" {
			origins, ok := c.Store.(UnitOrigins)
			if !ok {
				return View{}, ErrUnitPreviewRequired
			}
			key.AssetID, e = origins.UnitOrigin(ctx, scope, key.ProfileID)
			if e != nil {
				return View{}, e
			}
		}
	}
	logicalID := AnalysisID(scope, key)
	retained, readErr := c.Store.ReadAnalysis(ctx, scope, logicalID, checksum, 0, 1)
	// A COLUMNS plan has a different owner hash from UNIT. Verify the new
	// pin with the owner before reusing the original CSV ledger and hash.
	legacyTable := readErr == nil && key.UnitRef == "table:0" && retained.Profile.UnitRef == ""
	if readErr == nil && !legacyTable {
		if ref != nil {
			if key.AssetID != "" && key.AssetID != retained.Profile.AssetID || retained.Profile.Version != ref.Version {
				return View{}, ErrScope
			}
			if retained.Profile.Reference() != *ref {
				return View{}, ErrUnitSchemaChanged
			}
		}
		plan, buildErr := Build(scope, retained.Profile, c.Manifest, expected, work)
		if buildErr != nil {
			return View{}, buildErr
		}
		return c.Store.PlanAnalysis(ctx, plan)
	}
	if readErr != nil && (!errors.Is(readErr, ErrScope) || expected != 0) {
		return View{}, readErr
	}
	preview, _ := Find(c.Manifest, "source.file.preview")
	page := map[string]any{"view": "COLUMNS", "offset": 0, "limit": 1, "profileId": key.ProfileID}
	if ref != nil {
		page["view"], page["unitRef"] = "UNIT", ref.UnitRef
	}
	page["assetId"] = key.AssetID
	args, _ := json.Marshal(page)
	if len(args) > 1024 {
		return View{}, ErrInvalid
	}
	r, e := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: preview.CapabilityID, Owner: preview.Owner, OperationClass: "READ", GatewayBindingRef: preview.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(), Window: time.Minute, Timeout: 10 * time.Second, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 1 << 20}})
	if e != nil {
		return View{}, e
	}
	if r.Replay || r.Problem != nil {
		if r.Problem != nil && r.Problem.Status == http.StatusConflict && r.Problem.Code == ErrUnitSchemaChanged.Error() {
			return View{}, ErrUnitSchemaChanged
		}
		return View{}, ErrScope
	}
	p, e := verifiedProfile(r.Body, key, ref)
	if e != nil {
		return View{}, e
	}
	if legacyTable {
		if retained.Profile.AssetID != p.AssetID || retained.Profile.ProfileID != p.ProfileID ||
			retained.Profile.Version != p.Version || retained.Profile.ColumnCount != p.ColumnCount {
			return View{}, ErrScope
		}
		p = retained.Profile
	}
	if ref != nil && selection.AssetID != "" {
		origins, ok := c.Store.(UnitOrigins)
		if !ok {
			return View{}, ErrUnavailable
		}
		if e = origins.RememberUnitOrigin(ctx, scope, key.ProfileID, key.AssetID); e != nil {
			return View{}, e
		}
	}
	plan, e := Build(scope, p, c.Manifest, expected, work)
	if e != nil {
		return View{}, e
	}
	return c.Store.PlanAnalysis(ctx, plan)
}

func verifiedProfile(body []byte, key Profile, ref *UnitProfileRef) (Profile, error) {
	if ref != nil {
		p, err := UnitPage(body, key.AssetID, key.ProfileID, key.UnitRef, 0, 1)
		if err != nil {
			return Profile{}, err
		}
		if p.Version != ref.Version {
			return Profile{}, ErrScope
		}
		if p.SchemaHash != ref.SchemaHash {
			return Profile{}, ErrUnitSchemaChanged
		}
		return p, nil
	}
	var p Profile
	var envelope struct {
		View    string            `json:"view"`
		Offset  int               `json:"offset"`
		Columns []json.RawMessage `json:"columns"`
	}
	if json.Unmarshal(body, &p) != nil || json.Unmarshal(body, &envelope) != nil || p.AssetID != key.AssetID || p.ProfileID != key.ProfileID || p.UnitRef != "" || !p.Valid() || envelope.View != "COLUMNS" || envelope.Offset != 0 || len(envelope.Columns) != 1 {
		return Profile{}, ErrScope
	}
	return p, nil
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
	r, callErr := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: cap.CapabilityID, Owner: cap.Owner, OperationClass: cap.OperationClass, GatewayBindingRef: cap.GatewayBindingRef, ManifestChecksum: checksum, Arguments: task.Arguments, IdempotencyKey: "analysis:" + Digest([]any{id, task.ID, task.Attempts}), CorrelationID: uuid.NewString(), Window: time.Minute, Timeout: time.Duration(task.MaxTimeMS) * time.Millisecond, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: task.MaxObjects, ResultBytes: task.MaxBytes}})
	f := Finish{State: "HELD", Code: "OUTCOME_REQUIRES_RECONCILIATION", ElapsedMS: time.Since(start).Milliseconds()}
	if r.AttemptID != uuid.Nil {
		f.AttemptID = r.AttemptID.String()
	}
	var window interface{ BudgetWindowEnd() time.Time }
	if errors.As(callErr, &window) {
		f.State = "WAITING"
		f.Code = "ADMISSION_WINDOW_WAIT"
		f.NotBefore = window.BudgetWindowEnd().Add(time.Second)
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
	return c.Store.FinishAnalysisTask(finishCtx, scope, id, taskID, task.Fence, f)
}
func retryAfter(value string, now time.Time) time.Time {
	if n, e := strconv.ParseInt(value, 10, 64); e == nil && n >= 0 && n <= 86400 {
		return now.Add(time.Duration(n) * time.Second)
	}
	if at, e := http.ParseTime(value); e == nil {
		return at
	}
	return time.Time{}
}

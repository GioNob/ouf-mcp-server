package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func (c Coordinator) publishedScope(ctx context.Context, i orchestration.Identity, cap manifest.Capability) (string, string, error) {
	if i.ActorType != "HUMAN" || i.Delegation == "" {
		return "", "", orchestration.ErrUnauthorized
	}
	d, err := c.Service.Auth.Authorize(ctx, orchestration.AuthorizationRequest{Identity: i, Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: i.TenantID}, CapabilityID: cap.RequiredAuthorization, Owner: cap.Owner, OperationClass: cap.OperationClass})
	if err != nil {
		return "", "", err
	}
	if !d.Allowed || d.DecisionRef == "" {
		return "", "", &orchestration.AuthorizationDeniedError{ReasonCode: d.DecisionCode, PolicyVersion: d.BundleVersion, CapabilityID: cap.RequiredAuthorization}
	}
	return visibilityDigest(i, d), d.DecisionRef, nil
}

func visibilityDigest(i orchestration.Identity, d orchestration.AuthorizationDecision) string {
	scopes := slices.Clone(i.Scopes)
	slices.Sort(scopes)
	var roles, amr []string
	var acr string
	if i.Claims != nil {
		roles = slices.Clone(i.Claims.ExternalRoleRefs)
		slices.Sort(roles)
		amr = slices.Clone(i.Claims.Amr)
		slices.Sort(amr)
		acr = i.Claims.Acr
	}
	return Digest([]any{IdentityScope(i), i.Issuer, i.Audience, i.ActorType, i.AuthenticationContextRef, scopes, roles, amr, acr, d.BundleID, d.BundleVersion, d.ResourceScope, d.PermittedDetailLevel})
}

func (c Coordinator) CheckPublishedScope(ctx context.Context, i orchestration.Identity, expected string) error {
	if expected == "" {
		return nil
	}
	cap, ok := Find(c.Manifest, BatchTool)
	if !ok {
		return ErrUnavailable
	}
	current, _, err := c.publishedScope(ctx, i, cap)
	if err != nil {
		return err
	}
	if current != expected {
		return ErrScope
	}
	return nil
}

// An owned predecessor plan may have no epoch tag. Only a checked historical
// batch decision can distinguish an epoch change from other visibility drift.
func (c Coordinator) ScopeError(ctx context.Context, i orchestration.Identity, id, manifest string, err error) error {
	if !errors.Is(err, ErrScope) || c.Store == nil {
		return err
	}
	view, readErr := c.Store.ReadAnalysis(ctx, IdentityScope(i), id, manifest, 0, 1)
	if readErr != nil {
		return readErr
	}
	if epochErr := c.CheckEpoch(ctx, i, view.Epoch); epochErr != nil {
		return epochErr
	}
	return err
}

// A current denial while attempting a public step stops workers in the
// enrolled plan, including workers that previously obtained a task fence.
func (c Coordinator) CheckStepScope(ctx context.Context, i orchestration.Identity, id, manifest, visibility string) error {
	err := c.ScopeError(ctx, i, id, manifest, c.CheckPublishedScope(ctx, i, visibility))
	if visibility != "" && errors.Is(err, orchestration.ErrUnauthorized) {
		if s, ok := c.Store.(ScopeStopStore); ok {
			if e := s.StopPublished(ctx, IdentityScope(i), id, manifest, visibility); e != nil {
				return e
			}
		}
	}
	return err
}

func onlyDiscovery(work []Work) bool {
	for _, w := range work {
		if w.Tool != "semantic.discovery.request" {
			return false
		}
	}
	return len(work) > 0
}

func envelopeSize(i orchestration.Identity, cap manifest.Capability, decision string, maxBytes int64) int {
	const uuid = "00000000-0000-4000-8000-000000000000"
	digest := Digest("")
	body, _ := json.Marshal(orchestration.GatewayRequest{Identity: i, GatewayBindingRef: cap.GatewayBindingRef, CapabilityID: cap.CapabilityID,
		Owner: cap.Owner, OperationClass: cap.OperationClass, Arguments: json.RawMessage("{}"), AuthorizationDecisionRef: decision,
		CorrelationID: uuid, AttemptID: uuid, IdempotencyKey: "analysis:" + digest, RequestHash: digest, MaxResultBytes: maxBytes})
	return len(body) - 2
}

func (c Coordinator) batchFinish(task Task, id string, r orchestration.Result, callErr error, f Finish) Finish {
	if callErr != nil {
		if errors.Is(callErr, orchestration.ErrUnauthorized) {
			f.StopScope = true
		}
		if errors.Is(callErr, ErrEpochChanged) {
			f.Code, f.StopScope = ErrEpochChanged.Error(), true
		}
		if errors.Is(callErr, ErrScope) {
			f.Code = "ANALYSIS_SCOPE_CHANGED"
			f.StopScope = true
		}
		if errors.Is(callErr, orchestration.ErrResultLimit) {
			f.Code = InvalidOwner
		}
		return f
	}
	if r.Replay {
		return f
	}
	p := r.Problem
	if p == nil {
		f.Calls = 1
		if _, ok := AcceptedBatch(task, r.Body); !ok || len(r.Body) > int(task.MaxBytes) {
			f.State, f.Code, f.Body = "HELD", InvalidOwner, nil
		}
		return f
	}
	switch {
	case p.Status == 403:
		f.StopScope = f.State == "HELD" && f.Code != "RETRY_BUDGET_EXHAUSTED"
	case p.Status == 502 && p.Code == InvalidOwner:
		f.Code = InvalidOwner
	case p.Status == 413 || p.Status == 422:
		f.Children = SplitBatch(task, p.Status == 422)
		f.Code = "SEM_BATCH_REQUEST_TOO_LARGE"
		if p.Status == 422 {
			f.Code = "SEM_BATCH_RESULT_TOO_LARGE"
		}
		if len(f.Children) > 0 {
			f.State, f.Code = "SUCCEEDED", "PUBLISHED_BATCH_SPLIT"
		}
	case batchAbsent(p):
		f.State, f.Code, f.NotBefore = "SUCCEEDED", "PUBLISHED_BATCH_UNAVAILABLE", time.Time{}
		var b Batch
		_ = json.Unmarshal(task.Arguments, &b)
		for _, i := range b.Intents {
			f.Children = append(f.Children, task.ResidualTasks(id, i, true)...)
		}
	case p.Status == 429 && p.Code != "SEM_BATCH_RATE_LIMITED", p.Status == 503 && p.Code != "SEM_BATCH_UNAVAILABLE":
		f.State, f.Code, f.NotBefore = "HELD", "OWNER_REQUEST_REJECTED", time.Time{}
	}
	return f
}

// Only route/method absence (including an older unstructured HTTP 404)
// allow the transport compatibility path. Static INACTIVE is handled at plan time.
func batchAbsent(p *orchestration.Problem) bool {
	switch p.Status {
	case 404, 405, 501:
		// The compatibility signal is HTTP route/method absence. A named
		// business rejection on the same HTTP status stays a rejection.
		switch p.Code {
		case "", "NOT_FOUND", "ROUTE_NOT_FOUND", "METHOD_NOT_ALLOWED", "NOT_IMPLEMENTED":
			return true
		}
	}
	return false
}

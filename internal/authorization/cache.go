package authorization

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

var ErrPolicyUnavailable = errors.New("authorization policy bundle unavailable")

type BundleSource interface {
	FetchActive(context.Context) (ActivePolicyBundle, error)
}

type Cache struct {
	source BundleSource
	now    func() time.Time
	active atomic.Pointer[ActivePolicyBundle]
}

func NewCache(source BundleSource) *Cache {
	return &Cache{source: source, now: time.Now}
}

func (c *Cache) Refresh(ctx context.Context) error {
	if c == nil || c.source == nil {
		return ErrPolicyUnavailable
	}
	bundle, err := c.source.FetchActive(ctx)
	if err != nil {
		return err
	}
	if err := bundle.Validate(); err != nil {
		return fmt.Errorf("invalid authorization policy bundle: %w", err)
	}
	current := c.active.Load()
	if current != nil {
		if bundle.ActivatedAt.Before(current.ActivatedAt) {
			return fmt.Errorf("stale authorization activation rejected: active=%s fetched=%s", current.ActivatedAt, bundle.ActivatedAt)
		}
		if bundle.ActivatedAt.Equal(current.ActivatedAt) &&
			(bundle.BundleID != current.BundleID || bundle.BundleVersion != current.BundleVersion) {
			return errors.New("authorization activation timestamp collision")
		}
	}
	copy := bundle
	c.active.Store(&copy)
	return nil
}

func (c *Cache) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if c == nil {
		return orchestration.AuthorizationDecision{}, ErrPolicyUnavailable
	}
	active := c.active.Load()
	if active == nil {
		return orchestration.AuthorizationDecision{}, ErrPolicyUnavailable
	}
	if in.Identity.PrincipalID == "" || in.Identity.TenantID == "" || in.Identity.ActorType == "" ||
		in.Identity.AuthenticationContextRef == "" || in.Identity.Issuer == "" || in.Identity.Audience == "" {
		return orchestration.AuthorizationDecision{}, errors.New("trusted authorization principal context incomplete")
	}
	if in.Identity.ActorType == "SERVICE" && in.Identity.ServicePrincipalID == "" {
		return orchestration.AuthorizationDecision{}, errors.New("service principal id is required for SERVICE actor")
	}
	resource := in.Resource
	if resource.TenantID == "" {
		resource.TenantID = in.Identity.TenantID
	}
	if resource.ResourceType == "" {
		resource.ResourceType = "capability"
	}
	return evaluate(active.Bundle, in.Identity, resource, in.CapabilityID, in.OperationClass, c.now()), nil
}

type ActivePolicyBundle struct {
	BundleID      string       `json:"bundleId"`
	BundleVersion int64        `json:"bundleVersion"`
	ActivatedAt   time.Time    `json:"activatedAt"`
	Bundle        PolicyBundle `json:"bundle"`
}

func (a ActivePolicyBundle) Validate() error {
	if a.BundleID == "" || a.BundleVersion < 1 || a.ActivatedAt.IsZero() {
		return errors.New("active bundle metadata is incomplete")
	}
	if a.Bundle.BundleID != a.BundleID || a.Bundle.Version != a.BundleVersion {
		return errors.New("active pointer does not match embedded bundle")
	}
	return a.Bundle.Validate()
}

type PolicyBundle struct {
	BundleID     string                 `json:"bundleId"`
	Version      int64                  `json:"version"`
	PublishedAt  time.Time              `json:"publishedAt"`
	Capabilities []CapabilityDescriptor `json:"capabilities"`
	Grants       []Grant                `json:"grants"`
}

func (b PolicyBundle) Validate() error {
	if b.BundleID == "" || b.Version < 1 || b.PublishedAt.IsZero() {
		return errors.New("bundle identity is incomplete")
	}
	for _, c := range b.Capabilities {
		if c.CapabilityID == "" || c.Operation == "" || c.RequiredScope == "" {
			return errors.New("capability descriptor is incomplete")
		}
	}
	for _, g := range b.Grants {
		if g.GrantID == "" || g.CapabilityID == "" || g.TenantID == "" || g.ValidFrom.IsZero() || g.ValidUntil.IsZero() || !g.ValidUntil.After(g.ValidFrom) {
			return errors.New("grant is incomplete")
		}
		if g.SubjectID == "" && g.ServicePrincipalID == "" {
			return errors.New("grant requires subject or service principal")
		}
	}
	return nil
}

type CapabilityDescriptor struct {
	CapabilityID  string   `json:"capabilityId"`
	Operation     string   `json:"operation"`
	RequiredScope string   `json:"requiredScope"`
	AllowedActors []string `json:"allowedActors"`
}

type Grant struct {
	GrantID            string    `json:"grantId"`
	CapabilityID       string    `json:"capabilityId"`
	TenantID           string    `json:"tenantId"`
	SubjectID          string    `json:"subjectId"`
	ServicePrincipalID string    `json:"servicePrincipalId"`
	OrganizationID     string    `json:"organizationId"`
	ValidFrom          time.Time `json:"validFrom"`
	ValidUntil         time.Time `json:"validUntil"`
}

func evaluate(bundle PolicyBundle, principal orchestration.Identity, resource orchestration.ResourceContext, capabilityID, operation string, now time.Time) orchestration.AuthorizationDecision {
	ref := fmt.Sprintf("%s:%d:%s", bundle.BundleID, bundle.Version, capabilityID)
	deny := func(code string) orchestration.AuthorizationDecision {
		return orchestration.AuthorizationDecision{Allowed: false, DecisionRef: ref, DecisionCode: code, BundleID: bundle.BundleID, BundleVersion: bundle.Version}
	}
	if principal.TenantID == "" || principal.TenantID != resource.TenantID {
		return deny("TENANT_MISMATCH")
	}
	var descriptor *CapabilityDescriptor
	for i := range bundle.Capabilities {
		candidate := &bundle.Capabilities[i]
		if candidate.CapabilityID == capabilityID && candidate.Operation == operation {
			descriptor = candidate
			break
		}
	}
	if descriptor == nil {
		return deny("CAPABILITY_NOT_DECLARED")
	}
	if !contains(descriptor.AllowedActors, principal.ActorType) {
		return deny("ACTOR_NOT_ALLOWED")
	}
	if !contains(principal.Scopes, descriptor.RequiredScope) {
		return deny("SCOPE_MISSING")
	}
	for _, grant := range bundle.Grants {
		if grant.CapabilityID != capabilityID || grant.TenantID != principal.TenantID {
			continue
		}
		if now.Before(grant.ValidFrom) || !now.Before(grant.ValidUntil) {
			continue
		}
		if grant.OrganizationID != "" && grant.OrganizationID != resource.OrganizationID {
			continue
		}
		if grant.SubjectID != "" && grant.SubjectID != principal.PrincipalID {
			continue
		}
		if grant.ServicePrincipalID != "" && grant.ServicePrincipalID != principal.ServicePrincipalID {
			continue
		}
		return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: ref, DecisionCode: "ALLOW", BundleID: bundle.BundleID, BundleVersion: bundle.Version}
	}
	return deny("NO_APPLICABLE_GRANT")
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

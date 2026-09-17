package authorization

import (
	"context"
 "encoding/json"
 "crypto/sha256"
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
	active atomic.Pointer[cachedSnapshot]
 maxStaleness time.Duration
}

func NewCache(source BundleSource) *Cache {
	return &Cache{source: source, now: time.Now, maxStaleness: 300*time.Second}
}

type cachedSnapshot struct { ActivePolicyBundle; verifiedAt time.Time; digest [32]byte }
func NewCacheWithMaxStaleness(source BundleSource, age time.Duration) (*Cache,error) {
 if age<time.Second || age>24*time.Hour { return nil, errors.New("policy max staleness must be 1 second..24 hours") }
 c:=NewCache(source);c.maxStaleness=age;return c,nil
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
 // Detach all mutable slices/maps from the transport before publishing a snapshot.
 raw,err:=json.Marshal(bundle);if err!=nil{return err};var detached ActivePolicyBundle
 if err=json.Unmarshal(raw,&detached);err!=nil{return err}
 semantic,_:=json.Marshal(detached.Bundle); digest:=sha256.Sum256(semantic)
 verified:=c.now()
 for {
 current:=c.active.Load()
 if current!=nil {
  if detached.BundleID!=current.BundleID || detached.BundleVersion<current.BundleVersion || detached.ActivatedAt.Before(current.ActivatedAt) {return errors.New("authorization rollback or lineage switch rejected")}
  if detached.BundleVersion==current.BundleVersion && digest!=current.digest {return errors.New("immutable authorization version changed")}
  if detached.ActivatedAt.Equal(current.ActivatedAt) && detached.BundleVersion!=current.BundleVersion {return errors.New("authorization activation timestamp collision")}
  if verified.Before(current.verifiedAt) {return errors.New("older refresh rejected")}
 }
 next:=&cachedSnapshot{ActivePolicyBundle:detached,verifiedAt:verified,digest:digest}
 if c.active.CompareAndSwap(current,next){break}
 }
	return nil
}

func (c *Cache) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if c == nil {
		return orchestration.AuthorizationDecision{}, ErrPolicyUnavailable
	}
	active := c.active.Load()
	if active == nil || c.now().Before(active.verifiedAt) || !c.now().Before(active.verifiedAt.Add(c.maxStaleness)) {
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
 ContentHash string `json:"contentHash,omitempty"`
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
	if len(b.Capabilities)>10000 || len(b.Grants)>10000{return errors.New("bundle cardinality limit")}
 caps:=map[string]bool{};grants:=map[string]bool{}
 for _, c := range b.Capabilities {
 if caps[c.CapabilityID]{return errors.New("duplicate capability")};caps[c.CapabilityID]=true
		if c.CapabilityID == "" || c.Operation == "" || c.RequiredScope == "" {
			return errors.New("capability descriptor is incomplete")
		}
	}
	for _, g := range b.Grants {
 if grants[g.GrantID] || !caps[g.CapabilityID]{return errors.New("invalid grant reference")};grants[g.GrantID]=true
 if g.Constraints!=nil {if err:=g.Constraints.validate();err!=nil{return err}}
		if g.GrantID == "" || g.CapabilityID == "" || g.TenantID == "" || g.ValidFrom.IsZero() || g.ValidUntil.IsZero() || !g.ValidUntil.After(g.ValidFrom) {
			return errors.New("grant is incomplete")
		}
		if g.SubjectID == "" && g.ServicePrincipalID == "" && (g.Constraints==nil || g.Constraints.ExternalRoleRef=="") {
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
 Constraints *GrantConstraints `json:"constraints,omitempty"`
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
	label:=resource.Attributes["dataAccessLabel"];requestedDetail:=resource.Attributes["detailLevel"]
 if resource.Attributes["requiresDataAccessLabel"]=="true" && label=="" {return deny("DATA_LABEL_REQUIRED")}
 if requestedDetail=="SECURITY_SENSITIVE" && principal.ActorType!="HUMAN" {return deny("HUMAN_REQUIRED")}
 allow:=false
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
  if grant.Constraints!=nil {if !grant.Constraints.matches(principal,resource,now){continue};if grant.Constraints.Effect=="DENY"{return deny("EXPLICIT_DENY")}}
  if label!="" && label!="OPEN" && label!="ANONYMOUS" && (grant.Constraints==nil || !contains(grant.Constraints.AllowedDataLabels,label)){continue}
  if requestedDetail!="" && requestedDetail!="PUBLIC_OPERATIONAL" && (grant.Constraints==nil || !contains(grant.Constraints.AllowedDetailLevels,requestedDetail)){continue}
  allow=true
	}
 if !allow {return deny("NO_APPLICABLE_GRANT")}
 scope:=map[string]string{"tenantId":resource.TenantID,"resourceType":resource.ResourceType}
 if resource.ResourceID!="" {scope["resourceId"]=resource.ResourceID}
 for _,key:=range []string{"module","sourceRef","jobRef","dataAccessLabel"} {if value,ok:=resource.Attributes[key];ok {scope[key]=value}}
 detail:=resource.Attributes["detailLevel"];if detail=="" {detail="PUBLIC_OPERATIONAL"}
 return orchestration.AuthorizationDecision{Allowed:true,DecisionRef:ref,DecisionCode:"ALLOW",BundleID:bundle.BundleID,BundleVersion:bundle.Version,ResourceScope:scope,PermittedDetailLevel:detail}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

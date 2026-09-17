package authorization

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type bundleSourceFixture struct {
	bundle ActivePolicyBundle
	err    error
}

func (f *bundleSourceFixture) FetchActive(context.Context) (ActivePolicyBundle, error) {
	return f.bundle, f.err
}

func validBundle(now time.Time) ActivePolicyBundle {
	return ActivePolicyBundle{
		BundleID:      "bundle-main",
		BundleVersion: 7,
		ActivatedAt:   now.Add(-time.Minute),
		Bundle: PolicyBundle{
			BundleID:    "bundle-main",
			Version:     7,
			PublishedAt: now.Add(-2 * time.Minute),
			Capabilities: []CapabilityDescriptor{{
				CapabilityID:  "ouf.system.status",
				Operation:     "READ",
				RequiredScope: "operations.status.read",
				AllowedActors: []string{"HUMAN", "SERVICE"},
			}},
			Grants: []Grant{{
				GrantID:      "grant-status",
				CapabilityID: "ouf.system.status",
				TenantID:     "tenant-a",
				SubjectID:    "user-a",
				ValidFrom:    now.Add(-time.Hour),
				ValidUntil:   now.Add(time.Hour),
			}},
		},
	}
}

func TestCacheFailsClosedBeforeBootstrap(t *testing.T) {
	cache := NewCache(&bundleSourceFixture{})
	_, err := cache.Authorize(context.Background(), orchestration.AuthorizationRequest{})
	if !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("expected policy unavailable, got %v", err)
	}
}

func TestCacheEvaluatesExactBundleAndDecisionRef(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	source := &bundleSourceFixture{bundle: validBundle(now)}
	cache := NewCache(source)
	cache.now = func() time.Time { return now }
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	decision, err := cache.Authorize(context.Background(), orchestration.AuthorizationRequest{
		Identity: orchestration.Identity{
			ServicePrincipalID:       "service:mcp",
			PrincipalID:              "user-a",
			TenantID:                 "tenant-a",
			ActorType:                "HUMAN",
			AuthenticationContextRef: "acr:mfa",
			Issuer:                   "https://issuer.example.invalid",
			Audience:                 "ouf",
			Scopes:                   []string{"operations.status.read"},
		},
		Resource:       orchestration.ResourceContext{ResourceType: "capability", TenantID: "tenant-a"},
		CapabilityID:   "ouf.system.status",
		OperationClass: "READ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed || decision.DecisionCode != "ALLOW" || decision.DecisionRef != "bundle-main:7:ouf.system.status" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestCacheRetainsLastKnownGoodOnRefreshFailure(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	source := &bundleSourceFixture{bundle: validBundle(now)}
	cache := NewCache(source)
	cache.now = func() time.Time { return now }
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.err = errors.New("registry unavailable")
	if err := cache.Refresh(context.Background()); err == nil {
		t.Fatal("expected refresh failure")
	}
	decision, err := cache.Authorize(context.Background(), orchestration.AuthorizationRequest{
		Identity: orchestration.Identity{
			ServicePrincipalID:       "service:mcp",
			PrincipalID:              "user-a",
			TenantID:                 "tenant-a",
			ActorType:                "HUMAN",
			AuthenticationContextRef: "acr:mfa",
			Issuer:                   "https://issuer.example.invalid",
			Audience:                 "ouf",
			Scopes:                   []string{"operations.status.read"},
		},
		CapabilityID:   "ouf.system.status",
		OperationClass: "READ",
	})
	if err != nil || !decision.Allowed {
		t.Fatalf("last known good bundle was not retained: decision=%+v err=%v", decision, err)
	}
}

func TestCacheRejectsStaleActivationAndOrgGrantWithoutOrgContext(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	bundle := validBundle(now)
	bundle.Bundle.Grants[0].OrganizationID = "org-a"
	source := &bundleSourceFixture{bundle: bundle}
	cache := NewCache(source)
	cache.now = func() time.Time { return now }
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	decision, err := cache.Authorize(context.Background(), orchestration.AuthorizationRequest{
		Identity: orchestration.Identity{
			ServicePrincipalID:       "service:mcp",
			PrincipalID:              "user-a",
			TenantID:                 "tenant-a",
			ActorType:                "HUMAN",
			AuthenticationContextRef: "acr:mfa",
			Issuer:                   "https://issuer.example.invalid",
			Audience:                 "ouf",
			Scopes:                   []string{"operations.status.read"},
		},
		CapabilityID:   "ouf.system.status",
		OperationClass: "READ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Allowed || decision.DecisionCode != "NO_APPLICABLE_GRANT" {
		t.Fatalf("org-scoped grant must fail closed without org context: %+v", decision)
	}

	stale := validBundle(now)
	stale.ActivatedAt = bundle.ActivatedAt.Add(-time.Second)
	source.bundle = stale
	if err := cache.Refresh(context.Background()); err == nil {
		t.Fatal("expected stale activation rejection")
	}
}

func TestCacheRejectsNewerActivationWithLowerVersion(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	source := &bundleSourceFixture{bundle: validBundle(now)}
	cache := NewCache(source)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	next := validBundle(now)
	next.BundleID = "bundle-rollback"
	next.BundleVersion = 1
	next.ActivatedAt = now
	next.Bundle.BundleID = "bundle-rollback"
	next.Bundle.Version = 1
	source.bundle = next
	if err := cache.Refresh(context.Background()); err == nil {
		t.Fatal("rollback must require a new monotonic bundle version")
	}
}

func TestCacheFreshnessImmutabilityAndRevocation(t *testing.T) {
 now:=time.Date(2026,9,16,12,0,0,0,time.UTC)
 source:=&bundleSourceFixture{bundle:validBundle(now)};cache,_:=NewCacheWithMaxStaleness(source,time.Minute)
 cache.now=func()time.Time{return now};ctx:=context.Background()
 request:=orchestration.AuthorizationRequest{Identity:orchestration.Identity{PrincipalID:"user-a",TenantID:"tenant-a",ActorType:"HUMAN",AuthenticationContextRef:"mfa",Issuer:"issuer",Audience:"ouf",Scopes:[]string{"operations.status.read"}},CapabilityID:"ouf.system.status",OperationClass:"READ"}
 if err:=cache.Refresh(ctx);err!=nil{t.Fatal(err)}
 pinned:=cache.active.Load()
 source.bundle.Bundle.Grants[0].SubjectID="other"
 if d,e:=cache.Authorize(ctx,request);e!=nil || !d.Allowed{t.Fatal("source mutation changed snapshot",d,e)}
 if err:=cache.Refresh(ctx);err==nil{t.Fatal("same version mutation accepted")}
 now=now.Add(time.Minute)
 if _,err:=cache.Authorize(ctx,request);!errors.Is(err,ErrPolicyUnavailable){t.Fatal("stale snapshot accepted",err)}
 source.bundle.BundleVersion++;source.bundle.Bundle.Version++;source.bundle.ActivatedAt=now
 if err:=cache.Refresh(ctx);err!=nil{t.Fatal(err)}
 if d,e:=cache.Authorize(ctx,request);e!=nil || d.Allowed{t.Fatal("revocation ineffective",d,e)}
 if !evaluate(pinned.Bundle,request.Identity,orchestration.ResourceContext{TenantID:"tenant-a",ResourceType:"capability"},request.CapabilityID,request.OperationClass,now).Allowed{t.Fatal("old pinned snapshot mutated")}
}

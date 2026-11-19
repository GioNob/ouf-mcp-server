package orchestration

import (
	"context"
	"errors"
	"testing"
)

type diagnosticAuth struct{ decision AuthorizationDecision }

func (a diagnosticAuth) Authorize(context.Context, AuthorizationRequest) (AuthorizationDecision, error) {
	return a.decision, nil
}

func TestDenialKeepsCurrentDecisionAndAuditBeforeAdmission(t *testing.T) {
	a, g, audit := &fakeAdmission{}, &fakeGateway{}, &fakeAudit{}
	decision := AuthorizationDecision{Allowed: false, DecisionRef: "private:50:read", DecisionCode: "NO_APPLICABLE_GRANT", BundleID: "private", BundleVersion: 50}
	_, err := (Service{Auth: diagnosticAuth{decision}, Admission: a, Gateway: g, Audit: audit, FingerprintKey: make([]byte, 32)}).Call(context.Background(), invocation())
	var denied *AuthorizationDeniedError
	if !errors.Is(err, ErrUnauthorized) || !errors.As(err, &denied) || err.Error() != "authorization denied" || denied.ReasonCode != decision.DecisionCode || denied.PolicyVersion != 50 {
		t.Fatalf("decision provenance lost: %v", err)
	}
	if a.reserves != 0 || g.calls != 0 || len(audit.events) != 1 || audit.events[0].OutcomeCode != decision.DecisionCode || audit.events[0].AuthorizationDecisionRef != decision.DecisionRef {
		t.Fatal("admission, dispatch or protected audit changed")
	}
}

func TestMissingDelegationCannotInventAnEvaluatedPolicy(t *testing.T) {
	a, g := &fakeAdmission{}, &fakeGateway{}
	_, err := (Service{RequireDelegation: true, Auth: &fakeAuth{true}, Admission: a, Gateway: g, FingerprintKey: make([]byte, 32)}).Call(context.Background(), invocation())
	var denied *AuthorizationDeniedError
	if !errors.As(err, &denied) || !errors.Is(err, ErrUnauthorized) || denied.ReasonCode != "DELEGATION_REQUIRED" || denied.PolicyVersion != 0 || a.reserves != 0 || g.calls != 0 {
		t.Fatalf("missing delegation diagnostic is not bounded: %v", err)
	}
}

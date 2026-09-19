package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/statusview"
)

type statusAuth struct {
	request  AuthorizationRequest
	decision AuthorizationDecision
}

func (a *statusAuth) Authorize(_ context.Context, in AuthorizationRequest) (AuthorizationDecision, error) {
	a.request = in
	return a.decision, nil
}

type statusAudit struct{ event AuditEvent }

func (a *statusAudit) Audit(_ context.Context, in AuditEvent) error { a.event = in; return nil }
func statusInvocation() Invocation {
	in := invocation()
	in.CapabilityID = statusview.Capability
	in.Owner = "mcp"
	in.OperationClass = "READ"
	in.GatewayBindingRef = "capability://" + statusview.Capability
	in.Arguments = json.RawMessage(`{}`)
	in.Maximum.ResultBytes = 1024
	return in
}
func statusDecision() AuthorizationDecision {
	return AuthorizationDecision{Allowed: true, DecisionRef: "bundle:1:ouf.system.status", PermittedDetailLevel: statusview.Public, ResourceScope: map[string]string{"tenantId": "tenant", "resourceType": "capability"}}
}

func TestPublicStatusRequiresBoundedDecisionBeforeAdmission(t *testing.T) {
	for _, name := range []string{"denied", "missing-detail", "higher-detail", "wrong-tenant", "wrong-resource", "missing-ref", "requested-higher"} {
		t.Run(name, func(t *testing.T) {
			a := &statusAuth{decision: statusDecision()}
			in := statusInvocation()
			switch name {
			case "denied":
				a.decision.Allowed = false
			case "missing-detail":
				a.decision.PermittedDetailLevel = ""
			case "higher-detail":
				a.decision.PermittedDetailLevel = "TENANT_OPERATIONAL"
			case "wrong-tenant":
				a.decision.ResourceScope["tenantId"] = "other"
			case "wrong-resource":
				a.decision.ResourceScope["resourceType"] = "source"
			case "missing-ref":
				a.decision.DecisionRef = ""
			case "requested-higher":
				in.Resource.Attributes = map[string]string{"detailLevel": "SECURITY_SENSITIVE"}
			}
			admission := &fakeAdmission{}
			gateway := &fakeGateway{}
			_, err := (Service{Auth: a, Admission: admission, Gateway: gateway}).Call(context.Background(), in)
			if !errors.Is(err, ErrUnauthorized) || admission.reserves != 0 || gateway.calls != 0 {
				t.Fatalf("denial dispatched: %v %+v %+v", err, admission, gateway)
			}
		})
	}
}

func TestPublicStatusRedactsGatewayResponseAndAuditsDecision(t *testing.T) {
	a := &statusAuth{decision: statusDecision()}
	admission := &fakeAdmission{}
	audit := &statusAudit{}
	gateway := &fakeGateway{response: GatewayResponse{Status: 200, Body: []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"partial":true,"securityIncidentCount":7,"incidents":["private"],"futureField":"private"}`)}}
	in := statusInvocation()
	in.Resource.Attributes = map[string]string{"module": "MCP"}
	got, err := (Service{Auth: a, Admission: admission, Gateway: gateway, Audit: audit, FingerprintKey: make([]byte, 32)}).Call(context.Background(), in)
	if err != nil || gateway.calls != 1 || admission.reconciles != 1 {
		t.Fatalf("governed call failed: %v", err)
	}
	if a.request.Resource.Attributes["detailLevel"] != statusview.Public || in.Resource.Attributes["detailLevel"] != "" {
		t.Fatal("public context missing or caller map mutated")
	}
	if strings.Contains(string(got.Body), "private") || strings.Contains(string(got.Body), "Incident") {
		t.Fatalf("restricted data escaped: %s", got.Body)
	}
	if audit.event.AuthorizationDecisionRef != a.decision.DecisionRef || audit.event.PermittedDetailLevel != statusview.Public || !audit.event.Redacted {
		t.Fatalf("missing safe redaction audit: %+v", audit.event)
	}
}

func TestPublicStatusInvalidOwnerResultReconcilesFailure(t *testing.T) {
	a := &statusAuth{decision: statusDecision()}
	admission := &fakeAdmission{}
	audit := &statusAudit{}
	gateway := &fakeGateway{response: GatewayResponse{Status: 200, Body: []byte(`{"secret":"private"}`)}}
	got, err := (Service{Auth: a, Admission: admission, Gateway: gateway, Audit: audit, FingerprintKey: make([]byte, 32)}).Call(context.Background(), statusInvocation())
	if !errors.Is(err, statusview.ErrInvalid) || len(got.Body) != 0 || admission.reconciles != 1 || audit.event.OutcomeCode != "INVALID_PUBLIC_STATUS" {
		t.Fatalf("invalid response treated as success: %+v %v %+v", got, err, audit.event)
	}
}

func TestPublicStatusSanitizesUpstreamFailures(t *testing.T) {
	for _, status := range []int{403, 503} {
		for _, withProblem := range []bool{false, true} {
			response := GatewayResponse{Status: status, Body: []byte(`{"secret":"private"}`)}
			if withProblem {
				response.Problem = &Problem{Status: status, Code: "private", Title: "private"}
			}
			got, err := (Service{Auth: &statusAuth{decision: statusDecision()}, Admission: &fakeAdmission{}, Gateway: &fakeGateway{response: response}, FingerprintKey: make([]byte, 32)}).Call(context.Background(), statusInvocation())
			expected := "STATUS_UNAVAILABLE"
			if status == 403 {
				expected = "NOT_AUTHORIZED"
			}
			if err != nil || len(got.Body) != 0 || got.Problem == nil || got.Problem.Code != expected || got.Problem.Title != expected {
				t.Fatalf("unsafe failure: %+v %v", got, err)
			}
		}
	}
}

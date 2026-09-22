package orchestration

import (
	"context"
	"errors"
	"testing"
)

func TestIncidentCallsRequireTenantBoundDisclosureBeforeAdmission(t *testing.T) {
	for _, cap := range []string{"ouf.operations.incidents", "ouf.ingestion.operations.incidents", "ouf.gateway.operations.incidents"} {
		for _, detail := range []string{"TENANT_OPERATIONAL", "", "RESTRICTED_OPERATIONAL"} {
			a := &statusAuth{decision: AuthorizationDecision{Allowed: true, DecisionRef: "bundle:1:" + cap, PermittedDetailLevel: detail, ResourceScope: map[string]string{"tenantId": "tenant", "resourceType": "capability"}}}
			in := invocation()
			in.Maximum.ResultBytes = 1024
			in.CapabilityID = cap
			in.GatewayBindingRef = "capability://" + cap
			admission := &fakeAdmission{}
			gateway := &fakeGateway{response: GatewayResponse{Status: 200, Body: []byte(`{"items":[],"partial":false,"hasMore":false}`)}}
			_, err := (Service{Auth: a, Admission: admission, Gateway: gateway, FingerprintKey: make([]byte, 32)}).Call(context.Background(), in)
			if a.request.Resource.Attributes["detailLevel"] != "TENANT_OPERATIONAL" {
				t.Fatal("missing requested disclosure", cap)
			}
			if detail == "TENANT_OPERATIONAL" {
				if err != nil || gateway.calls != 1 {
					t.Fatalf("authorized incident failed: %v", err)
				}
			} else if !errors.Is(err, ErrUnauthorized) || admission.reserves != 0 || gateway.calls != 0 {
				t.Fatal("invalid disclosure dispatched", cap, detail, err)
			}
		}
	}
}

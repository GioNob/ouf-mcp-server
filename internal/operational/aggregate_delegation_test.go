package operational

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type delegatedAdmissionFixture struct {
	mu    sync.Mutex
	calls int
}

func (f *delegatedAdmissionFixture) Reserve(context.Context, orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return orchestration.AdmissionDecision{AttemptID: uuid.New()}, nil
}
func (*delegatedAdmissionFixture) Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error {
	return nil
}
func (*delegatedAdmissionFixture) Reconcile(context.Context, uuid.UUID, orchestration.Cost, orchestration.AttemptOutcome) error {
	return nil
}

type delegatedPolicyFixture struct{}

func (delegatedPolicyFixture) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "bundle:1:" + in.CapabilityID, PermittedDetailLevel: in.Resource.Attributes["detailLevel"], ResourceScope: map[string]string{"tenantId": in.Identity.TenantID, "resourceType": in.Resource.ResourceType}}, nil
}

type delegatedGatewayFixture struct {
	mu       sync.Mutex
	requests []orchestration.GatewayRequest
}

func (f *delegatedGatewayFixture) Execute(_ context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, in)
	return orchestration.GatewayResponse{Status: http.StatusOK, Body: []byte(`{"items":[],"partial":false}`)}, nil
}

func TestAggregateDelegationCrossesRealServiceGuardWithoutBecomingPayload(t *testing.T) {
	admission := &delegatedAdmissionFixture{}
	gateway := &delegatedGatewayFixture{}
	service := &orchestration.Service{RequireDelegation: true, Auth: delegatedPolicyFixture{}, Admission: admission, Gateway: gateway, FingerprintKey: make([]byte, 32)}
	self := &ownerStatusFixture{}
	handler := NewOwnerAPI(self, &Aggregator{Caller: service, Self: self, ManifestChecksum: strings.Repeat("a", 64)}).WithAuthorization(delegatedPolicyFixture{}).WithOperationalDelegation("workload")
	request := aggregateOwnerRequest("incidents", `{"sourceId":"source-a"}`)
	request.Header.Set("X-OUF-Service-Principal", "workload")
	request.Header.Set("X-OUF-Actor-Type", "HUMAN")
	request.Header.Set("X-OUF-Capability-ID", "ouf.operations.incidents")
	request.Header.Set("X-OUF-Delegation-Verified", "mcp-operational-aggregate-v1")
	proof := "e30." + strings.Repeat("a", 43)
	request.Header.Set("X-OUF-Delegation", proof)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body struct {
		Partial     bool     `json:"partial"`
		Unavailable []string `json:"unavailableProducers"`
	}
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &body) != nil || body.Partial || len(body.Unavailable) != 0 || len(gateway.requests) != 2 || admission.calls != 2 {
		t.Fatalf("real delegation guard not satisfied: %d %s", response.Code, response.Body.String())
	}
	for _, nested := range gateway.requests {
		if nested.Identity.Delegation != proof || nested.Identity.ActorType != "HUMAN" || nested.Identity.ServicePrincipalID != "workload" {
			t.Fatal("verified context lost")
		}
		raw, _ := json.Marshal(nested)
		if strings.Contains(string(raw), proof) {
			t.Fatal("proof persisted in nested envelope")
		}
	}
	if strings.Contains(response.Body.String(), proof) {
		t.Fatal("proof returned to caller")
	}
}

func TestAggregateDelegationRejectsUnverifiedOrChangedContextBeforeOwnerReads(t *testing.T) {
	for _, mode := range []string{"missing-marker", "missing-proof", "wrong-marker", "wrong-workload", "empty-config", "wrong-capability", "wrong-actor", "malformed", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			self, caller, handler := aggregateOwnerFixture()
			handler.WithAuthorization(delegatedPolicyFixture{}).WithOperationalDelegation("workload")
			req := aggregateOwnerRequest("incidents", `{}`)
			req.Header.Set("X-OUF-Service-Principal", "workload")
			req.Header.Set("X-OUF-Actor-Type", "HUMAN")
			req.Header.Set("X-OUF-Capability-ID", "ouf.operations.incidents")
			req.Header.Set("X-OUF-Delegation-Verified", "mcp-operational-aggregate-v1")
			req.Header.Set("X-OUF-Delegation", "e30."+strings.Repeat("a", 43))
			switch mode {
			case "missing-marker":
				req.Header.Del("X-OUF-Delegation-Verified")
			case "missing-proof":
				req.Header.Del("X-OUF-Delegation")
			case "wrong-marker":
				req.Header.Set("X-OUF-Delegation-Verified", "forged")
			case "wrong-workload":
				req.Header.Set("X-OUF-Service-Principal", "other")
			case "empty-config":
				handler.WithOperationalDelegation("")
			case "wrong-capability":
				req.Header.Set("X-OUF-Capability-ID", "ouf.operations.summary")
			case "wrong-actor":
				req.Header.Set("X-OUF-Actor-Type", "SERVICE")
			case "malformed":
				req.Header.Set("X-OUF-Delegation", "secret plaintext")
			case "oversized":
				req.Header.Set("X-OUF-Delegation", strings.Repeat("a", 16385))
			}
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			if res.Code != http.StatusForbidden || self.called || caller.callCount() != 0 {
				t.Fatal("unverified delegation caused reads", res.Code)
			}
		})
	}
}

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type controlPolicyFixture struct{}

func (controlPolicyFixture) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "policy:1:" + in.CapabilityID, PermittedDetailLevel: in.Resource.Attributes["detailLevel"], ResourceScope: map[string]string{"resourceType": in.Resource.ResourceType, "tenantId": in.Identity.TenantID}}, nil
}

type controlSelfFixture struct{}

func (controlSelfFixture) SystemStatus(context.Context, orchestration.Identity) ([]byte, error) {
	return []byte(`{"module":"MCP","status":"HEALTHY","partial":false,"actionRequired":false}`), nil
}

type controlGatewayFixture struct {
	mu    sync.Mutex
	owner http.Handler
	calls int
}

func (g *controlGatewayFixture) Execute(ctx context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	if in.Owner != "mcp" {
		return orchestration.GatewayResponse{Status: 200, Body: []byte(`{"items":[],"partial":false}`)}, nil
	}
	req := httptest.NewRequest(http.MethodPost, "/api/internal/v1/mcp/operations/incidents", strings.NewReader(string(in.Arguments))).WithContext(ctx)
	for name, value := range map[string]string{"X-OUF-Gateway-Verified": "true", "X-OUF-Principal-ID": in.Identity.PrincipalID, "X-OUF-Tenant-ID": in.Identity.TenantID, "X-OUF-Service-Principal": in.Identity.ServicePrincipalID, "X-OUF-Actor-Type": in.Identity.ActorType, "X-OUF-Capability-ID": in.CapabilityID, "X-OUF-Authorization-Decision-Ref": in.AuthorizationDecisionRef, "X-OUF-Delegation": in.Identity.Delegation, "X-OUF-Delegation-Verified": "mcp-operational-aggregate-v1", "X-Tool-Attempt-ID": in.AttemptID, "X-Correlation-ID": in.CorrelationID} {
		req.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	g.owner.ServeHTTP(response, req)
	return orchestration.GatewayResponse{Status: response.Code, Body: response.Body.Bytes()}, nil
}

func controlStore(t *testing.T) (context.Context, *Store, string) {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err = Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"capabilities":[],"controlProfile":"` + uuid.NewString() + `"}`)
	manifest := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err = store.EnsureManifest(ctx, manifest, "control-budget-integration-v1", "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	// These assertions measure one known minute window, not normal clock rollover.
	// Align the fixture away from the boundary using the authoritative DB clock;
	// no transaction, admission reservation or external operation is held.
	var databaseNow time.Time
	if err = store.Pool().QueryRow(ctx, "select clock_timestamp()").Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	remaining := databaseNow.Truncate(time.Minute).Add(time.Minute).Sub(databaseNow)
	if remaining < 5*time.Second {
		timer := time.NewTimer(remaining + 50*time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	return ctx, store, manifest
}

func controlRequest(manifest, tenant string) orchestration.AdmissionRequest {
	return orchestration.AdmissionRequest{BudgetProfile: orchestration.OperationalControlBudgetProfile, AttemptID: uuid.New(), Identity: orchestration.Identity{ServicePrincipalID: "workload", PrincipalID: "operator", TenantID: tenant, ActorType: "HUMAN"}, CapabilityID: "ouf.gateway.operations.incidents", Owner: "gateway", OperationClass: "READ", ManifestChecksum: manifest, AuthorizationDecisionRef: "policy:1", IdempotencyKey: uuid.NewString(), RequestHash: hash64("control"), SemanticFingerprint: "fingerprint:" + uuid.NewString(), FingerprintVersion: "v1", CorrelationID: uuid.NewString(), Window: time.Minute, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 100, ResultBytes: 512 << 10}}
}

func TestOperationalControlBudgetRealAggregateChargesAllThreeCalls(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "control-real-" + uuid.NewString()
	gateway := &controlGatewayFixture{}
	service := &orchestration.Service{RequireDelegation: true, Auth: controlPolicyFixture{}, Admission: store, Gateway: gateway, Audit: store, FingerprintKey: make([]byte, 32)}
	self := controlSelfFixture{}
	gateway.owner = operational.NewOwnerAPI(self, &operational.Aggregator{Caller: service, Self: self, ManifestChecksum: manifest}).WithAuthorization(controlPolicyFixture{}).WithOperationalDelegation("workload")
	in := orchestration.Invocation{Identity: orchestration.Identity{ServicePrincipalID: "workload", PrincipalID: "operator", TenantID: tenant, ActorType: "HUMAN", Delegation: "e30." + strings.Repeat("a", 43)}, Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: tenant, Attributes: map[string]string{"detailLevel": "TENANT_OPERATIONAL"}}, CapabilityID: "ouf.operations.incidents", Owner: "mcp", OperationClass: "READ", GatewayBindingRef: "capability://ouf.operations.incidents", ManifestChecksum: manifest, Arguments: json.RawMessage(`{"limit":1}`), IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(), Window: time.Minute, Timeout: 3 * time.Second, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 100, ResultBytes: 512 << 10}}
	result, err := service.Call(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	var projected struct {
		Partial     bool     `json:"partial"`
		Unavailable []string `json:"unavailableProducers"`
	}
	if json.Unmarshal(result.Body, &projected) != nil || projected.Partial || len(projected.Unavailable) != 0 || gateway.calls != 3 {
		t.Fatalf("aggregate failed under real admission: %s, calls=%d", result.Body, gateway.calls)
	}
	var consumed, reserved int64
	var windows int
	if err = store.Pool().QueryRow(ctx, `select count(*),coalesce(sum(consumed_tool_calls),0),coalesce(sum(reserved_tool_calls),0) from ouf_mcp.budget_window where tenant_id=$1 and policy_ref=$2`, tenant, manifest+":"+orchestration.OperationalControlBudgetProfile).Scan(&windows, &consumed, &reserved); err != nil {
		t.Fatal(err)
	}
	if windows != 1 || consumed != 3 || reserved != 0 {
		t.Fatal("nested work not charged once in shared control window", windows, consumed, reserved)
	}
}

func TestOperationalControlBudgetFinitePersistentAndBusinessIsolated(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "control-capacity-" + uuid.NewString()
	for index := 0; index < 24; index++ {
		request := controlRequest(manifest, tenant)
		admitted, err := store.Reserve(ctx, request)
		if err != nil {
			t.Fatal(index, err)
		}
		if err = settleControlAdmission(ctx, store, admitted); err != nil {
			t.Fatal(err)
		}
	}
	other, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err = other.Reserve(ctx, controlRequest(manifest, tenant)); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatal("restart/new ids reset control capacity", err)
	}
	request := controlRequest(manifest, tenant)
	request.BudgetProfile = ""
	request.CapabilityID = "urban.object.related_search"
	request.Owner = "udp"
	request.OperationClass = "SEARCH"
	if _, err = other.Reserve(ctx, request); err != nil {
		t.Fatal("control use consumed independent business window", err)
	}
	var windows int
	if err = store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.budget_window where tenant_id=$1`, tenant).Scan(&windows); err != nil || windows != 2 {
		t.Fatal(windows, err)
	}
}

func TestOperationalControlBudgetEquivalentGuardAndIdempotencyRemainFinite(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "control-equivalence-" + uuid.NewString()
	base := controlRequest(manifest, tenant)
	for index := 0; index < 2; index++ {
		request := base
		request.AttemptID = uuid.New()
		request.IdempotencyKey = uuid.NewString()
		admitted, err := store.Reserve(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err = settleControlAdmission(ctx, store, admitted); err != nil {
			t.Fatal(err)
		}
		replayed, err := store.Reserve(ctx, request)
		if err != nil || !replayed.Replay || replayed.AttemptID != admitted.AttemptID {
			t.Fatal("idempotency changed", replayed, err)
		}
	}
	base.AttemptID = uuid.New()
	base.IdempotencyKey = uuid.NewString()
	if _, err := store.Reserve(ctx, base); err == nil {
		t.Fatal("new control profile raised equivalent retry guard")
	}
}

func TestOperationalControlBudgetRejectsForgedProfileBeforeWindowWrites(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "control-forgery-" + uuid.NewString()
	request := controlRequest(manifest, tenant)
	request.CapabilityID = "urban.object.related_search"
	if _, err := store.Reserve(ctx, request); err == nil {
		t.Fatal("business acquired control profile")
	}
	var windows int
	if err := store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.budget_window where tenant_id=$1`, tenant).Scan(&windows); err != nil || windows != 0 {
		t.Fatal("invalid profile wrote window", windows, err)
	}
}

func settleControlAdmission(ctx context.Context, store *Store, admitted orchestration.AdmissionDecision) error {
	if err := store.Dispatch(ctx, admitted.AttemptID, uuid.NewString(), time.Second, admitted.LockVersion); err != nil {
		return err
	}
	return store.Reconcile(ctx, admitted.AttemptID, orchestration.Cost{ToolCalls: 1}, orchestration.AttemptOutcome{Success: true, Code: "OK"})
}

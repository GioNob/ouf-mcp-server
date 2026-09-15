package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"
)

func TestDurableLifecycle(t *testing.T) {
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"capabilities":[]}`)
	manifestHash := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err = store.EnsureManifest(ctx, manifestHash, "integration-v1", "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	session, err := store.CreateSession(ctx, Session{ConversationRef: "application-not-transport", ServicePrincipalID: "service", PrincipalID: "principal", TenantID: "tenant", ManifestChecksum: manifestHash, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	admission := Admission{SessionID: &session.ID, ServicePrincipalID: "service", PrincipalID: "principal", TenantID: "tenant", CapabilityID: "urban.object.related_search", OperationClass: "SEARCH", ManifestChecksum: manifestHash, IdempotencyKey: uuid.NewString(), RequestHash: hash64("request-a"), CorrelationID: uuid.NewString()}
	first, err := store.Admit(ctx, admission)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.Admit(ctx, admission)
	if err != nil || replay.ID != first.ID {
		t.Fatalf("replay %v %v", replay.ID, err)
	}
	conflict := admission
	conflict.RequestHash = hash64("request-b")
	if _, err = store.Admit(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	running, err := store.MarkRunning(ctx, first.ID, "gateway-request", -time.Second, first.LockVersion)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Maintain(ctx, time.Now())
	if err != nil || result.OrphansMarkedUnknown < 1 {
		t.Fatalf("recovery %+v %v", result, err)
	}
	var state, dispatch string
	if err = store.Pool().QueryRow(ctx, "select state,dispatch_state from ouf_mcp.tool_attempt where attempt_id=$1", running.ID).Scan(&state, &dispatch); err != nil {
		t.Fatal(err)
	}
	if state != "UNKNOWN" || dispatch != "UNKNOWN" {
		t.Fatalf("collapsed uncertainty %s/%s", state, dispatch)
	}
	if err = store.AppendAudit(ctx, "ATTEMPT_UNKNOWN", "MAINTENANCE_WORKER", "", &running.ID, manifestHash, []byte(`{"reason":"lease-expired"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, "update ouf_mcp.audit_event set event_type='MUTATED' where attempt_id=$1", running.ID); err == nil {
		t.Fatal("audit mutable")
	}
	if _, err = store.Pool().Exec(ctx, "update ouf_mcp.manifest_snapshot set manifest_version='mutated' where manifest_checksum=$1", manifestHash); err == nil {
		t.Fatal("manifest mutable")
	}
	governed := orchestration.AdmissionRequest{Identity: orchestration.Identity{ServicePrincipalID: "mcp", PrincipalID: "agent", TenantID: "tenant-" + uuid.NewString(), ActorType: "AI_AGENT", AuthenticationContextRef: "authn-1"}, CapabilityID: "urban.object.related_search", Owner: "udp", OperationClass: "SEARCH", ManifestChecksum: manifestHash, AuthorizationDecisionRef: "decision-1", IdempotencyKey: uuid.NewString(), RequestHash: hash64("governed"), SemanticFingerprint: "v1:hmac-sha256:same", FingerprintVersion: "v1", CorrelationID: uuid.NewString(), Window: time.Minute, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, ResultBytes: 1024}}
	reserved, err := store.Reserve(ctx, governed)
	if err != nil {
		t.Fatal(err)
	}
	governedReplay, err := store.Reserve(ctx, governed)
	if err != nil || governedReplay.AttemptID != reserved.AttemptID || !governedReplay.Replay {
		t.Fatalf("governed replay %+v %v", governedReplay, err)
	}
	if err := store.Dispatch(ctx, reserved.AttemptID, "backend-1", time.Second, reserved.LockVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(ctx, reserved.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: 12}, orchestration.AttemptOutcome{Success: true, Code: "SUCCEEDED", BackendRequestID: "backend-1"}); err != nil {
		t.Fatal(err)
	}
	for n := 2; n <= 3; n++ {
		candidate := governed
		candidate.IdempotencyKey = uuid.NewString()
		candidate.RequestHash = hash64(fmt.Sprintf("governed-%d", n))
		_, err = store.Reserve(ctx, candidate)
		if n == 2 && err != nil {
			t.Fatal(err)
		}
		if n == 3 && !errors.Is(err, orchestration.ErrToolSelectionStall) {
			t.Fatalf("expected retry stall, got %v", err)
		}
	}
	expired := uuid.New()
	_, err = store.Pool().Exec(ctx, `insert into ouf_mcp.application_session(application_session_id,service_principal_id,principal_id,tenant_id,created_manifest_checksum,created_at,expires_at) values($1,'s','p','t',$2,transaction_timestamp()-interval '2 hours',transaction_timestamp()-interval '1 hour')`, expired, manifestHash)
	if err != nil {
		t.Fatal(err)
	}
	result, err = store.Maintain(ctx, time.Now())
	if err != nil || result.ExpiredSessionsDeleted < 1 {
		t.Fatalf("expiry %+v %v", result, err)
	}
}
func hash64(seed string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(seed))) }

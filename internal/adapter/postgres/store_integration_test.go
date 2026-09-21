package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/GioNob/ouf-mcp-server/internal/evidence"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/recovery"
	"github.com/google/uuid"
	"os"
	"strings"
	"sync"
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
	runID := uuid.NewString()
	backendGateway := "gateway-request-" + runID
	backendSuccess := "backend-success-" + runID
	backendUnknown := "backend-unknown-" + runID
	backendDebt := "backend-debt-" + runID
	backendViolation := "backend-violation-" + runID
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
	running, err := store.MarkRunning(ctx, first.ID, backendGateway, -time.Second, first.LockVersion)
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
	if err := store.Dispatch(ctx, reserved.AttemptID, backendSuccess, time.Second, reserved.LockVersion); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(ctx, reserved.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: 12}, orchestration.AttemptOutcome{Success: true, Code: "SUCCEEDED", BackendRequestID: backendSuccess}); err != nil {
		t.Fatal(err)
	}
	// A successful invocation no longer consumes the equivalent-retry guard.
	// Exercise the original threshold with a separate, still-active group.
	for n := 1; n <= 3; n++ {
		candidate := governed
		candidate.Identity.TenantID = "retry-" + runID
		candidate.IdempotencyKey = uuid.NewString()
		candidate.RequestHash = hash64(fmt.Sprintf("governed-%d", n))
		_, err = store.Reserve(ctx, candidate)
		if n < 3 && err != nil {
			t.Fatal(err)
		}
		if n == 3 && !errors.Is(err, orchestration.ErrToolSelectionStall) {
			t.Fatalf("expected retry stall, got %v", err)
		}
	}
	uncertain := governed
	uncertain.Identity.TenantID = "tenant-unknown-" + uuid.NewString()
	uncertain.IdempotencyKey = uuid.NewString()
	uncertain.RequestHash = hash64("uncertain")
	uncertain.SemanticFingerprint = "v1:hmac-sha256:uncertain"
	unknownAttempt, err := store.Reserve(ctx, uncertain)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Dispatch(ctx, unknownAttempt.AttemptID, backendUnknown, -time.Second, unknownAttempt.LockVersion); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.MarkStaleAndClaim(ctx, time.Now(), time.Now().Add(-time.Minute), "worker-one", time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].BackendRequestID != backendUnknown {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if _, err = store.Reserve(ctx, uncertain); !errors.Is(err, ErrIdempotencyOutcomeUnknown) {
		t.Fatalf("unsafe retry was not blocked: %v", err)
	}
	blockedPeer := uncertain
	blockedPeer.IdempotencyKey = uuid.NewString()
	blockedPeer.RequestHash = hash64("blocked-peer")
	if _, err = store.Reserve(ctx, blockedPeer); !errors.Is(err, ErrGroupLocked) {
		t.Fatalf("equivalent group admitted during uncertainty: %v", err)
	}
	secondClaim, err := store.MarkStaleAndClaim(ctx, time.Now(), time.Now().Add(-time.Minute), "worker-two", time.Minute, 10)
	if err != nil || len(secondClaim) != 0 {
		t.Fatalf("duplicate claim=%+v err=%v", secondClaim, err)
	}
	if err = store.ApplyOwnerEvidence(ctx, unknownAttempt.AttemptID, "worker-one", recovery.OwnerEvidence{BackendRequestID: backendUnknown, Outcome: "NOT_DISPATCHED", OutcomeCode: "OWNER_PROVES_NO_DISPATCH"}); err != nil {
		t.Fatal(err)
	}
	var recoveredState, recoveredDispatch, reservationState, claimState string
	var blocking int
	if err = store.Pool().QueryRow(ctx, `select ta.state,ta.dispatch_state,br.state,ic.state,rg.blocking_attempts from ouf_mcp.tool_attempt ta join ouf_mcp.budget_reservation br using(attempt_id) join ouf_mcp.idempotency_claim ic using(attempt_id) join ouf_mcp.attempt_admission_context ac using(attempt_id) join ouf_mcp.retry_guard rg using(equivalence_group_id) where ta.attempt_id=$1`, unknownAttempt.AttemptID).Scan(&recoveredState, &recoveredDispatch, &reservationState, &claimState, &blocking); err != nil {
		t.Fatal(err)
	}
	if recoveredState != "FAILED" || recoveredDispatch != "NOT_DISPATCHED" || reservationState != "RELEASED" || claimState != "COMPLETED" || blocking != 0 {
		t.Fatalf("recovery %s/%s reservation=%s claim=%s blocking=%d", recoveredState, recoveredDispatch, reservationState, claimState, blocking)
	}
	preDispatch := governed
	preDispatch.Identity.TenantID = "tenant-admitted-" + uuid.NewString()
	preDispatch.IdempotencyKey = uuid.NewString()
	preDispatch.RequestHash = hash64("never-dispatched")
	preDispatch.SemanticFingerprint = "v1:hmac-sha256:never-dispatched"
	admittedOnly, err := store.Reserve(ctx, preDispatch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, `update ouf_mcp.tool_attempt set admitted_at=transaction_timestamp()-interval '5 minutes' where attempt_id=$1`, admittedOnly.AttemptID); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.MarkStaleAndClaim(ctx, time.Now(), time.Now().Add(-time.Minute), "worker-one", time.Minute, 10)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("pre-dispatch recovery claimed owner: %+v %v", claimed, err)
	}
	if err = store.Pool().QueryRow(ctx, `select ta.state,ta.dispatch_state,br.state,ic.state from ouf_mcp.tool_attempt ta join ouf_mcp.budget_reservation br using(attempt_id) join ouf_mcp.idempotency_claim ic using(attempt_id) where ta.attempt_id=$1`, admittedOnly.AttemptID).Scan(&recoveredState, &recoveredDispatch, &reservationState, &claimState); err != nil {
		t.Fatal(err)
	}
	if recoveredState != "FAILED" || recoveredDispatch != "NOT_DISPATCHED" || reservationState != "RELEASED" || claimState != "COMPLETED" {
		t.Fatalf("pre-dispatch %s/%s reservation=%s claim=%s", recoveredState, recoveredDispatch, reservationState, claimState)
	}
	debtRequest := governed
	debtRequest.Identity.TenantID = "tenant-debt-" + uuid.NewString()
	debtRequest.IdempotencyKey = uuid.NewString()
	debtRequest.RequestHash = hash64("debt-attempt")
	debtRequest.SemanticFingerprint = "v1:hmac-sha256:debt"
	debtRequest.Maximum.DistinctObjects = 2
	debtAttempt, err := store.Reserve(ctx, debtRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Dispatch(ctx, debtAttempt.AttemptID, backendDebt, -time.Second, debtAttempt.LockVersion); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.MarkStaleAndClaim(ctx, time.Now(), time.Now().Add(-time.Minute), "worker-debt", time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].AttemptID != debtAttempt.AttemptID {
		t.Fatalf("debt recovery claim=%+v err=%v", claimed, err)
	}
	if err = store.ApplyOwnerEvidence(ctx, debtAttempt.AttemptID, "worker-debt", recovery.OwnerEvidence{BackendRequestID: backendDebt, Outcome: "UNKNOWN", OutcomeCode: "OWNER_UNAVAILABLE"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, `update ouf_mcp.tool_attempt set unknown_since=transaction_timestamp()-interval '2 days' where attempt_id=$1`, debtAttempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	if count, expireErr := store.ExpireUnknown(ctx, time.Now(), time.Now().Add(-24*time.Hour), 10); expireErr != nil || count != 1 {
		t.Fatalf("maxUnknownHold count=%d err=%v", count, expireErr)
	}
	var debtState string
	var debtAmount, debtCache int64
	if err = store.Pool().QueryRow(ctx, `select ta.state,d.debt_amount,w.current_unresolved_object_debt_total from ouf_mcp.tool_attempt ta join ouf_mcp.budget_object_debt d using(attempt_id) join ouf_mcp.budget_window w on w.budget_window_id=d.budget_window_id where ta.attempt_id=$1`, debtAttempt.AttemptID).Scan(&debtState, &debtAmount, &debtCache); err != nil {
		t.Fatal(err)
	}
	if debtState != "UNRESOLVED" || debtAmount != 2 || debtCache != 2 {
		t.Fatalf("unresolved debt state=%s amount=%d cache=%d", debtState, debtAmount, debtCache)
	}
	ingress := evidence.Service{Store: store}
	identity := evidence.TrustedIdentity{Authenticated: true, ServicePrincipalID: "udp-owner-workload", BackendOwner: "udp"}
	hashes := []string{"v1:hmac-sha256:" + strings.Repeat("a", 64), "v1:hmac-sha256:" + strings.Repeat("b", 64)}
	evidenceInput := evidence.Input{ClaimedOwner: "udp", BackendRequestID: backendDebt, Kind: "OWNER_RESULT", TerminalState: "SUCCEEDED", OutcomeCode: "SUCCEEDED", ResultRef: "result://udp/debt", ActualDistinctObjects: 2, ObjectHashes: hashes}
	ingested, err := ingress.Ingest(ctx, identity, evidenceInput)
	if err != nil {
		t.Fatal(err)
	}
	replayedEvidence, err := ingress.Ingest(ctx, identity, evidenceInput)
	if err != nil || !replayedEvidence.Replay || replayedEvidence.EvidenceRef != ingested.EvidenceRef {
		t.Fatalf("evidence replay %+v err=%v", replayedEvidence, err)
	}
	conflictInput := evidenceInput
	conflictInput.OutcomeCode = "DIFFERENT"
	if _, err = ingress.Ingest(ctx, identity, conflictInput); !errors.Is(err, evidence.ErrIngressConflict) {
		t.Fatalf("evidence conflict not detected: %v", err)
	}
	compensation := evidence.CompensationService{Store: store}
	type compensationAttempt struct {
		result evidence.CompensationResult
		err    error
	}
	results := make(chan compensationAttempt, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, applyErr := compensation.Apply(ctx, debtAttempt.AttemptID, ingested.EvidenceRef, 0)
			results <- compensationAttempt{result: result, err: applyErr}
		}()
	}
	wait.Wait()
	close(results)
	committed := 0
	for attempt := range results {
		if attempt.err == nil && attempt.result.Disposition == "COMPENSATED" && attempt.result.AttemptState == "SUCCEEDED" && attempt.result.OutcomeRevision == 1 {
			committed++
		}
	}
	if committed != 1 {
		t.Fatalf("concurrent compensation commits=%d", committed)
	}
	replayedCompensation, err := (evidence.CompensationService{Store: store}).Apply(ctx, debtAttempt.AttemptID, ingested.EvidenceRef, 0)
	if err != nil || replayedCompensation.Disposition != "IDEMPOTENT_REPLAY" {
		t.Fatalf("compensation replay %+v err=%v", replayedCompensation, err)
	}
	var finalDebt, finalEvidence string
	var finalBlocking int
	if err = store.Pool().QueryRow(ctx, `select d.debt_state,e.evidence_state,g.blocking_attempts from ouf_mcp.budget_object_debt d join ouf_mcp.owner_evidence_inbox e on e.evidence_ref=d.owner_evidence_ref join ouf_mcp.attempt_admission_context ac on ac.attempt_id=d.attempt_id join ouf_mcp.retry_guard g on g.equivalence_group_id=ac.equivalence_group_id where d.attempt_id=$1`, debtAttempt.AttemptID).Scan(&finalDebt, &finalEvidence, &finalBlocking); err != nil {
		t.Fatal(err)
	}
	if finalDebt != "RESOLVED" || finalEvidence != "CONSUMED" || finalBlocking != 0 {
		t.Fatalf("late recovery debt=%s evidence=%s blocker=%d", finalDebt, finalEvidence, finalBlocking)
	}
	violationRequest := governed
	violationRequest.Identity.TenantID = "tenant-violation-" + uuid.NewString()
	violationRequest.IdempotencyKey = uuid.NewString()
	violationRequest.RequestHash = hash64("violation-attempt")
	violationRequest.SemanticFingerprint = "v1:hmac-sha256:violation"
	violationRequest.Maximum.DistinctObjects = 1
	violationAttempt, err := store.Reserve(ctx, violationRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Dispatch(ctx, violationAttempt.AttemptID, backendViolation, -time.Second, violationAttempt.LockVersion); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.MarkStaleAndClaim(ctx, time.Now(), time.Now().Add(-time.Minute), "worker-violation", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("violation claim=%+v err=%v", claimed, err)
	}
	if err = store.ApplyOwnerEvidence(ctx, violationAttempt.AttemptID, "worker-violation", recovery.OwnerEvidence{BackendRequestID: backendViolation, Outcome: "UNKNOWN"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, `update ouf_mcp.tool_attempt set unknown_since=transaction_timestamp()-interval '2 days' where attempt_id=$1`, violationAttempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	if count, expireErr := store.ExpireUnknown(ctx, time.Now(), time.Now().Add(-24*time.Hour), 10); expireErr != nil || count != 1 {
		t.Fatalf("violation expiry count=%d err=%v", count, expireErr)
	}
	badInput := evidence.Input{ClaimedOwner: "udp", BackendRequestID: backendViolation, Kind: "OWNER_RESULT", TerminalState: "SUCCEEDED", OutcomeCode: "SUCCEEDED", ResultRef: "result://udp/invalid", ActualDistinctObjects: 1, ObjectHashes: []string{"v9:hmac-sha256:" + strings.Repeat("c", 64)}}
	badEvidence, err := ingress.Ingest(ctx, identity, badInput)
	if err != nil {
		t.Fatal(err)
	}
	violated, err := (evidence.CompensationService{Store: store}).Apply(ctx, violationAttempt.AttemptID, badEvidence.EvidenceRef, 0)
	if err != nil || violated.Disposition != "COMMITTED_DOMAIN_VIOLATION" || violated.OutcomeCode != "CRYPTO_VIOLATION" {
		t.Fatalf("domain violation %+v err=%v", violated, err)
	}
	var violationDebt, violationEvidence, violationOutcome string
	if err = store.Pool().QueryRow(ctx, `select d.debt_state,e.evidence_state,ta.outcome_code,g.blocking_attempts from ouf_mcp.budget_object_debt d join ouf_mcp.owner_evidence_inbox e on e.attempt_id=d.attempt_id join ouf_mcp.tool_attempt ta on ta.attempt_id=d.attempt_id join ouf_mcp.attempt_admission_context ac on ac.attempt_id=d.attempt_id join ouf_mcp.retry_guard g on g.equivalence_group_id=ac.equivalence_group_id where d.attempt_id=$1`, violationAttempt.AttemptID).Scan(&violationDebt, &violationEvidence, &violationOutcome, &finalBlocking); err != nil {
		t.Fatal(err)
	}
	if violationDebt != "ACTIVE" || violationEvidence != "QUARANTINED" || violationOutcome != "CRYPTO_VIOLATION" || finalBlocking != 1 {
		t.Fatalf("violation debt=%s evidence=%s outcome=%s blocker=%d", violationDebt, violationEvidence, violationOutcome, finalBlocking)
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

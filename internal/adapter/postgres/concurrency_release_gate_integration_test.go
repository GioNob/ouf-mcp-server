package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func concurrencyFixture(t *testing.T) (*Store, context.Context, string) {
	t.Helper()
	s, ctx := maintenanceStore(t)
	payload := []byte(fmt.Sprintf(`{"concurrencyGate":%q}`, uuid.NewString()))
	checksum := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err := s.EnsureManifest(ctx, checksum, "concurrency-gate-v1", "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	return s, ctx, checksum
}

func concurrencyRequest(checksum, tenant, fingerprint string) orchestration.AdmissionRequest {
	return orchestration.AdmissionRequest{
		Identity: orchestration.Identity{ServicePrincipalID: "mcp-concurrency", PrincipalID: "agent", TenantID: tenant, ActorType: "AI_AGENT", AuthenticationContextRef: "authn-concurrency"},
		CapabilityID: "urban.object.related_search", Owner: "udp", OperationClass: "SEARCH", ManifestChecksum: checksum,
		AuthorizationDecisionRef: "authz-concurrency", IdempotencyKey: uuid.NewString(), RequestHash: hash64(uuid.NewString()),
		SemanticFingerprint: fingerprint, FingerprintVersion: "v1", CorrelationID: uuid.NewString(), Window: time.Minute,
		RetryThreshold: 8, Maximum: orchestration.Cost{ToolCalls: 1, ResultBytes: 1024, DistinctObjects: 2},
	}
}

func serializableContention(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func TestConcurrencyGateIdempotencySingleClaim(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	req := concurrencyRequest(checksum, "tenant-idem-"+uuid.NewString(), "v1:hmac-sha256:"+fmt.Sprintf("%064x", 11))
	req.IdempotencyKey = "idem-" + uuid.NewString()
	req.RequestHash = hash64("same-request")

	start := make(chan struct{})
	type outcome struct {
		decision orchestration.AdmissionDecision
		err      error
	}
	results := make(chan outcome, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := s.Reserve(ctx, req)
			results <- outcome{d, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var canonical uuid.UUID
	successes := 0
	for r := range results {
		if r.err != nil {
			if !serializableContention(r.err) {
				t.Fatalf("unexpected contention error: %v", r.err)
			}
			continue
		}
		successes++
		if canonical == uuid.Nil {
			canonical = r.decision.AttemptID
		} else if r.decision.AttemptID != canonical {
			t.Fatalf("idempotency split across attempts: %s vs %s", canonical, r.decision.AttemptID)
		}
	}
	if successes == 0 || canonical == uuid.Nil {
		t.Fatal("no idempotency claimant committed")
	}
	var claims, attempts, reservations int
	if err := s.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.idempotency_claim where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and owner=$4 and capability_id=$5 and operation_class=$6 and idempotency_key=$7`, req.Identity.ServicePrincipalID, req.Identity.PrincipalID, req.Identity.TenantID, req.Owner, req.CapabilityID, req.OperationClass, req.IdempotencyKey).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.tool_attempt where attempt_id=$1`, canonical).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.budget_reservation where attempt_id=$1`, canonical).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if claims != 1 || attempts != 1 || reservations != 1 {
		t.Fatalf("claim=%d attempt=%d reservation=%d", claims, attempts, reservations)
	}
}

func TestConcurrencyGateDispatchCASAndReconcileExactlyOnce(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	req := concurrencyRequest(checksum, "tenant-cas-"+uuid.NewString(), "v1:hmac-sha256:"+fmt.Sprintf("%064x", 12))
	reserved, err := s.Reserve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- s.Dispatch(ctx, reserved.AttemptID, fmt.Sprintf("backend-%d-%s", i, uuid.NewString()), time.Minute, reserved.LockVersion)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	ok, casMiss := 0, 0
	for e := range errs {
		switch {
		case e == nil:
			ok++
		case errors.Is(e, pgx.ErrNoRows):
			casMiss++
		default:
			t.Fatalf("unexpected dispatch result: %v", e)
		}
	}
	if ok != 1 || casMiss != 1 {
		t.Fatalf("dispatch successes=%d casMiss=%d", ok, casMiss)
	}

	reconcileErrs := make(chan error, 4)
	start = make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reconcileErrs <- s.Reconcile(ctx, reserved.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: 7, DistinctObjects: 1}, orchestration.AttemptOutcome{Success: true, Code: "SUCCEEDED", BackendRequestID: "backend-terminal"})
		}()
	}
	close(start)
	wg.Wait()
	close(reconcileErrs)
	for e := range reconcileErrs {
		if e != nil {
			t.Fatalf("concurrent reconcile failed: %v", e)
		}
	}
	var reservedCalls, consumedCalls, reservedObjects, consumedObjects int64
	var reservationState string
	if err := s.Pool().QueryRow(ctx, `select w.reserved_tool_calls,w.consumed_tool_calls,w.reserved_distinct_objects,w.consumed_distinct_objects,br.state from ouf_mcp.budget_window w join ouf_mcp.budget_reservation br using(budget_window_id) where br.attempt_id=$1`, reserved.AttemptID).Scan(&reservedCalls, &consumedCalls, &reservedObjects, &consumedObjects, &reservationState); err != nil {
		t.Fatal(err)
	}
	if reservedCalls != 0 || consumedCalls != 1 || reservedObjects != 0 || consumedObjects != 1 || reservationState != "RECONCILED" {
		t.Fatalf("double reconciliation detected reserved=%d consumed=%d reservedObjects=%d consumedObjects=%d state=%s", reservedCalls, consumedCalls, reservedObjects, consumedObjects, reservationState)
	}
}

func TestConcurrencyGateRecoveryWorkersSingleClaimAndDebt(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	req := concurrencyRequest(checksum, "tenant-recovery-"+uuid.NewString(), "v1:hmac-sha256:"+fmt.Sprintf("%064x", 13))
	reserved, err := s.Reserve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dispatch(ctx, reserved.AttemptID, "backend-recovery-"+uuid.NewString(), -time.Second, reserved.LockVersion); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	start := make(chan struct{})
	type claimResult struct {
		worker string
		ids    []uuid.UUID
		err    error
	}
	claims := make(chan claimResult, 2)
	var wg sync.WaitGroup
	for _, worker := range []string{"worker-a", "worker-b"} {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			candidates, e := s.MarkStaleAndClaim(ctx, now, now.Add(-time.Minute), worker, time.Minute, 10)
			ids := make([]uuid.UUID, 0, len(candidates))
			for _, c := range candidates {
				ids = append(ids, c.AttemptID)
			}
			claims <- claimResult{worker: worker, ids: ids, err: e}
		}()
	}
	close(start)
	wg.Wait()
	close(claims)
	owners := 0
	for c := range claims {
		if c.err != nil {
			t.Fatalf("recovery worker %s failed: %v", c.worker, c.err)
		}
		for _, id := range c.ids {
			if id == reserved.AttemptID {
				owners++
			}
		}
	}
	if owners != 1 {
		t.Fatalf("attempt claimed %d times", owners)
	}
	var blocking int
	var debtRows int
	var debtAmount, cachedDebt int64
	if err := s.Pool().QueryRow(ctx, `select rg.blocking_attempts,w.current_unresolved_object_debt_total from ouf_mcp.attempt_admission_context ac join ouf_mcp.retry_guard rg using(equivalence_group_id) join ouf_mcp.budget_window w using(budget_window_id) where ac.attempt_id=$1`, reserved.AttemptID).Scan(&blocking, &cachedDebt); err != nil {
		t.Fatal(err)
	}
	if err := s.Pool().QueryRow(ctx, `select count(*),coalesce(sum(debt_amount),0) from ouf_mcp.budget_object_debt where attempt_id=$1 and debt_state='ACTIVE'`, reserved.AttemptID).Scan(&debtRows, &debtAmount); err != nil {
		t.Fatal(err)
	}
	if blocking != 1 || debtRows != 1 || debtAmount != 2 || cachedDebt != 2 {
		t.Fatalf("recovery accounting blocking=%d debtRows=%d debt=%d cache=%d", blocking, debtRows, debtAmount, cachedDebt)
	}
}

func TestConcurrencyGateAuthoritativeDebtBeatsStaleCacheDuringAdmission(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	tenant := "tenant-debt-gate-" + uuid.NewString()
	seed := concurrencyRequest(checksum, tenant, "v1:hmac-sha256:"+fmt.Sprintf("%064x", 14))
	seed.RetryThreshold = 2
	reserved, err := s.Reserve(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dispatch(ctx, reserved.AttemptID, "backend-debt-gate-"+uuid.NewString(), -time.Second, reserved.LockVersion); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claimed, err := s.MarkStaleAndClaim(ctx, now, now.Add(-time.Minute), "debt-worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("seed recovery claim=%v err=%v", claimed, err)
	}
	var windowID uuid.UUID
	if err := s.Pool().QueryRow(ctx, `select budget_window_id from ouf_mcp.attempt_admission_context where attempt_id=$1`, reserved.AttemptID).Scan(&windowID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `update ouf_mcp.budget_window set current_unresolved_object_debt_total=0 where budget_window_id=$1`, windowID); err != nil {
		t.Fatal(err)
	}

	candidate := concurrencyRequest(checksum, tenant, "v1:hmac-sha256:"+fmt.Sprintf("%064x", 15))
	candidate.RetryThreshold = 1
	start := make(chan struct{})
	var repairErr, admissionErr error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, repairErr = s.ReconcileDebtCaches(ctx, 10)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, admissionErr = s.Reserve(ctx, candidate)
	}()
	close(start)
	wg.Wait()
	if repairErr != nil {
		t.Fatal(repairErr)
	}
	if admissionErr != nil && serializableContention(admissionErr) {
		// Retry after serialization is required to evaluate the admission invariant.
		_, admissionErr = s.Reserve(ctx, candidate)
	}
	if !errors.Is(admissionErr, ErrBudgetExhausted) {
		t.Fatalf("stale debt cache allowed admission or wrong failure: %v", admissionErr)
	}
	var authoritative, cached int64
	if err := s.Pool().QueryRow(ctx, `select coalesce((select sum(debt_amount) from ouf_mcp.budget_object_debt where budget_window_id=$1 and debt_state='ACTIVE'),0),current_unresolved_object_debt_total from ouf_mcp.budget_window where budget_window_id=$1`, windowID).Scan(&authoritative, &cached); err != nil {
		t.Fatal(err)
	}
	if authoritative != 2 || cached != 2 {
		t.Fatalf("debt forgiveness authoritative=%d cached=%d", authoritative, cached)
	}
}

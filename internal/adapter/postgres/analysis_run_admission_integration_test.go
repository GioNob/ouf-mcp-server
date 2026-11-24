package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Give each replica a single, identifiable PostgreSQL backend. All replicas
// share the database and the fixture clock, but no Go synchronization in Store.
func admissionReplica(t *testing.T, ctx context.Context, s *Store) *Store {
	t.Helper()
	cfg := s.pool.Config()
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &Store{pool: pool, analysisClock: s.analysisClock}
}

func admissionCandidate(p analysis.RunSpec) analysis.RunSpec {
	p.Scope.Tenant, p.Scope.Principal, p.Scope.Service = uuid.NewString(), uuid.NewString(), uuid.NewString()
	p.CommandKey, p.Profile.AssetID = uuid.NewString(), uuid.NewString()
	return p
}

func requireAdmissionWait(t *testing.T, err error, now time.Time) {
	t.Helper()
	var wait *analysis.RenewalBackoff
	if !errors.Is(err, analysis.ErrRunPressure) || !errors.As(err, &wait) || !wait.NotBefore.After(now) || wait.RetryAfter == "" {
		t.Fatalf("expected timed admission backoff, got %v", err)
	}
}

func holdAdmissionLock(t *testing.T, ctx context.Context, s *Store, key string, seed int64) pgx.Tx {
	t.Helper()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	if _, err = tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,$2))", key, seed); err != nil {
		t.Fatal(err)
	}
	return tx
}

func admissionPID(t *testing.T, ctx context.Context, s *Store) int32 {
	t.Helper()
	var pid int32
	if err := s.pool.QueryRow(ctx, "select pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

func awaitAdmissionBlocker(t *testing.T, ctx context.Context, s *Store, waiter int32, blocker pgx.Tx) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		var blocked bool
		err := s.pool.QueryRow(ctx, "select $2::integer=any(pg_blocking_pids($1))", waiter, int32(blocker.Conn().PgConn().PID())).Scan(&blocked)
		if err != nil {
			t.Fatalf("waiting for backend %d to reach its lock: %v", waiter, err)
		}
		if blocked {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestInstallationAdmissionDatabaseLockOrder(t *testing.T) { // MCP-55E-03
	for _, path := range []string{"start", "renew"} {
		t.Run(path, func(t *testing.T) {
			ctx, s, r := fixtureRun(t)
			now := r.NotBefore.Add(time.Second)
			s.analysisClock = func() time.Time { return now }
			replica := admissionReplica(t, ctx, s)
			pid := admissionPID(t, ctx, replica)
			r = exhaustFixture(t, ctx, s, r)
			p := r.RunSpec
			if path == "start" {
				p = admissionCandidate(p)
			}
			// Three independent blockers identify the exact lock on which
			// the real entry point waits at each step, including for a new row.
			locks := []pgx.Tx{
				holdAdmissionLock(t, ctx, s, p.Installation(), 5022),
				holdAdmissionLock(t, ctx, s, analysis.Digest([]string{p.Issuer, p.Scope.Tenant}), 5018),
				holdAdmissionLock(t, ctx, s, p.ID(), 5021),
			}
			if path == "renew" {
				row, err := s.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = row.Rollback(ctx) })
				if _, err = row.Exec(ctx, "select 1 from ouf_mcp.analysis_run where analysis_id=$1 for update", p.ID()); err != nil {
					t.Fatal(err)
				}
				locks = append(locks, row)
			}
			done := make(chan error, 1)
			go func() {
				if path == "start" {
					_, err := replica.BeginRun(ctx, p)
					done <- err
				} else {
					actor, want := renewalCommand(r)
					done <- replica.RenewRunBudget(ctx, actor, want, fixtureRenewal{now: now})
				}
			}()
			for _, lock := range locks {
				awaitAdmissionBlocker(t, ctx, s, pid, lock)
				if err := lock.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInstallationAdmissionAcrossReplicas(t *testing.T) { // MCP-55E-01, MCP-55E-02, PET-mcp-068
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	gate := make(chan struct{})
	result := make(chan error, 4)
	for range 4 {
		replica := admissionReplica(t, ctx, s)
		p := admissionCandidate(r.RunSpec)
		// Same logical installation as the predecessor row, even with
		// distinct service workloads, interactive clients and issuers.
		p.Limits.InstallationRef = r.Workload
		p.Workload, p.Issuer = uuid.NewString(), uuid.NewString()
		go func() {
			<-gate
			_, err := replica.BeginRun(ctx, p)
			result <- err
		}()
	}
	close(gate)
	winners := 0
	for range 4 {
		if err := <-result; err != nil {
			requireAdmissionWait(t, err, now)
		} else {
			winners++
		}
	}
	if winners != 3 {
		t.Fatal("admissions alongside the active predecessor plan", winners)
	}
	// Expiry releases exactly the first plan. ISSUING rows still occupy slots.
	now = r.Mandate.Expires
	if _, err := s.BeginRun(ctx, admissionCandidate(r.RunSpec)); err != nil {
		t.Fatal("mandate expiry did not release capacity", err)
	}
	_, err := s.BeginRun(ctx, admissionCandidate(r.RunSpec))
	requireAdmissionWait(t, err, now)
}

func TestInstallationAdmissionNarrowQuotas(t *testing.T) { // MCP-55E-02
	for _, quota := range []string{"subject", "tenant", "source", "installation"} {
		t.Run(quota, func(t *testing.T) {
			ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) {
				if quota == "tenant" {
					l.Shared.TenantQueue = 1
				}
				if quota == "source" {
					l.Shared.SourceQueue = 1
				}
				if quota == "installation" {
					l.InstallationActivePlans = 1
				}
			})
			now := r.NotBefore.Add(time.Second)
			s.analysisClock = func() time.Time { return now }
			replica := admissionReplica(t, ctx, s)
			p := admissionCandidate(r.RunSpec)
			switch quota {
			case "subject":
				p.Scope.Principal = r.Scope.Principal
			case "tenant":
				p.Scope.Tenant = r.Scope.Tenant
				p.Limits.InstallationRef = uuid.NewString()
			case "source":
				p.Scope.Tenant, p.Profile.AssetID = r.Scope.Tenant, r.Profile.AssetID
				p.Limits.InstallationRef = uuid.NewString()
			}
			_, err := replica.BeginRun(ctx, p)
			requireAdmissionWait(t, err, now)
			if _, err = s.ReadRun(ctx, p.Scope, p.Issuer, p.ID()); !errors.Is(err, analysis.ErrScope) {
				t.Fatal("refused start occupied a plan", err)
			}
			independent := admissionCandidate(r.RunSpec)
			if quota == "installation" {
				independent.Limits.InstallationRef = uuid.NewString()
			}
			if _, err = s.BeginRun(ctx, independent); err != nil {
				t.Fatal("independent scope", err)
			}
		})
	}
}

func TestInstallationAdmissionLastSlot(t *testing.T) { // MCP-55E-03, PET-mcp-115
	for _, path := range []string{"direct", "ths"} {
		t.Run(path, func(t *testing.T) {
			ctx, s, r := fixtureRun(t)
			now := r.NotBefore.Add(time.Second)
			s.analysisClock = func() time.Time { return now }
			r = exhaustFixture(t, ctx, s, r)
			p, err := s.CurrentRenewal(ctx, r)
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, err = s.BeginRun(ctx, admissionCandidate(r.RunSpec)); err != nil {
					t.Fatal(err)
				}
			}
			start, renew := admissionReplica(t, ctx, s), admissionReplica(t, ctx, s)
			startPID, renewPID := admissionPID(t, ctx, start), admissionPID(t, ctx, renew)
			next := admissionCandidate(r.RunSpec)
			actor, want := renewalCommand(r)
			atProof := make(chan struct{})
			race := make(chan struct{})
			authority := fixtureRenewal{now: now, verify: func(*analysis.RenewalDecision) {
				close(atProof)
				select {
				case <-race:
				case <-ctx.Done():
				}
			}}
			renewed := make(chan error, 1)
			go func() {
				if path == "direct" {
					renewed <- renew.RenewRunBudget(ctx, actor, want, authority)
				} else {
					_, err := renew.DecideRenewal(ctx, thsHolder(r), proposalCommand(p, "APPROVE"), authority)
					renewed <- err
				}
			}()
			select {
			case <-atProof:
			case err := <-renewed:
				t.Fatal("renewal failed before the authority check", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			lock := holdAdmissionLock(t, ctx, s, r.Installation(), 5022)
			// Exercise both commit orders. Both candidates must be waiting
			// on this very installation lock before the last slot is released.
			if path == "ths" {
				close(race)
				awaitAdmissionBlocker(t, ctx, s, renewPID, lock)
			}
			started := make(chan error, 1)
			go func() {
				_, err := start.BeginRun(ctx, next)
				started <- err
			}()
			awaitAdmissionBlocker(t, ctx, s, startPID, lock)
			if path == "direct" {
				close(race)
			}
			awaitAdmissionBlocker(t, ctx, s, renewPID, lock)
			if err = lock.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			startErr := <-started
			renewErr := <-renewed
			if (startErr == nil) == (renewErr == nil) {
				t.Fatal("expected exactly one winner", startErr, renewErr)
			}
			var state, incident string
			var sessions int
			err = s.pool.QueryRow(ctx, `select r.state,i.state,(select count(*) from ouf_mcp.analysis_run_budget_session where analysis_id=r.analysis_id)
				from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_incident i using(analysis_id) where r.analysis_id=$1`, r.ID()).Scan(&state, &incident, &sessions)
			if err != nil {
				t.Fatal(err)
			}
			other, readErr := s.ReadRun(ctx, next.Scope, next.Issuer, next.ID())
			if startErr == nil {
				requireAdmissionWait(t, renewErr, now)
				if readErr != nil || other.State != "ISSUING" || state != "BLOCKED" || incident != "OPEN" || sessions != 0 {
					t.Fatal(other.State, state, incident, sessions, readErr)
				}
				waiting, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
				if err != nil || !waiting.NotBefore.After(now) || waiting.Seq != want.ExpectedSeq || waiting.Fence != r.Fence {
					t.Fatal("renewal wait changed the proposal or fence", err)
				}
			} else {
				requireAdmissionWait(t, startErr, now)
				if !errors.Is(readErr, analysis.ErrScope) || state != "RUNNING" || incident != "RECOVERING" || sessions != 1 {
					t.Fatal(state, incident, sessions, readErr)
				}
			}
			_, err = s.BeginRun(ctx, admissionCandidate(r.RunSpec))
			requireAdmissionWait(t, err, now)
			if startErr == nil {
				if err = s.StopRun(ctx, other, "ANALYSIS_CANCELLED"); err != nil {
					t.Fatal(err)
				}
				// Re-open on a different connection after a slot is freed.
				// The blocked run keeps its wait and the same approval sequence.
				restarted := admissionReplica(t, ctx, s)
				err = restarted.RenewRunBudget(ctx, actor, want, fixtureRenewal{now: now})
				requireAdmissionWait(t, err, now)
				waiting, err := restarted.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
				if err != nil {
					t.Fatal(err)
				}
				now = waiting.NotBefore
				if err = restarted.RenewRunBudget(ctx, actor, want, fixtureRenewal{now: now}); err != nil {
					t.Fatal("renewal at notBefore", err)
				}
			}
		})
	}
}

func TestInstallationAdmissionRecoveryKeepsSlot(t *testing.T) { // MCP-55E-04
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	_, first := claimFixture(t, ctx, s, r)
	now = now.Add(time.Minute)
	replica := admissionReplica(t, ctx, s)
	_, second := claimFixture(t, ctx, replica, r)
	if first.Fence == second.Fence || first.IntentKey != second.IntentKey {
		t.Fatal("expected recovery of the same intent under a new fence")
	}
	for range 3 {
		if _, err := s.BeginRun(ctx, admissionCandidate(r.RunSpec)); err != nil {
			t.Fatal("recovered plan counted twice", err)
		}
	}
	alias := r.RunSpec
	alias.CommandKey = uuid.NewString()
	got, err := replica.BeginRun(ctx, alias)
	if err != nil || got.CommandKey != r.CommandKey || got.Mandate.ID != r.Mandate.ID || got.Fence <= r.Fence || !got.Deadline.Equal(r.Deadline) {
		t.Fatal("alias at capacity", err)
	}
	_, err = replica.BeginRun(ctx, admissionCandidate(r.RunSpec))
	requireAdmissionWait(t, err, now)
}

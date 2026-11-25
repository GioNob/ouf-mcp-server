package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func fixtureRun(t *testing.T, configure ...func(*analysis.RunLimits)) (context.Context, *Store, analysis.RunRecord) {
	t.Helper()
	return fixtureRunWithTTL(t, 10*time.Minute, configure...)
}

func fixtureRunWithTTL(t *testing.T, ttl time.Duration, configure ...func(*analysis.RunLimits)) (context.Context, *Store, analysis.RunRecord) {
	t.Helper()
	ctx, s, scope, p, m := analysisFixture(t, 18)
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "false")
	cfg, err := analysis.RunConfiguration(m)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Limits.Shared.DependencySlots = 100000
	cfg.Limits.Shared.DependencyPerMinute = 100000
	for _, edit := range configure {
		edit(&cfg.Limits)
	}
	checksum, _ := m.Checksum()
	spec := analysis.RunSpec{Scope: scope, Issuer: "fixture", Workload: "worker-" + uuid.NewString(), CommandKey: uuid.NewString(), Manifest: checksum, Policy: "fixture-policy", Profile: p, Limits: cfg.Limits}
	r, err := s.BeginRun(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	mandate := analysis.Mandate{ID: uuid.NewString(), Generation: "1", Decision: "fixture", Issued: now, Expires: now.Add(ttl), Operations: []string{"source.file.preview", "ouf.semantic.search.batch", "semantic.discovery.request", "semantic.discovery.status", "semantic.discovery.candidates"}}
	raw, _ := json.Marshal(mandate)
	r, err = s.AttachMandate(ctx, r.RunSpec, raw)
	expected := r.Started.Add(time.Duration(r.Limits.WaitSeconds) * time.Second)
	if mandate.Expires.Before(expected) {
		expected = mandate.Expires
	}
	if err != nil || !r.Deadline.Equal(expected) {
		t.Fatal(r, err)
	}
	return ctx, s, r
}

func TestRunAtomicAliasAndFenceLease(t *testing.T) { // MCP-50U-04, MCP-50U-05
	ctx, s, r := fixtureRun(t)
	alias := r.RunSpec
	alias.CommandKey = uuid.NewString()
	replay, err := s.BeginRun(ctx, alias)
	if err != nil || replay.ID() != r.ID() || replay.CommandKey != r.CommandKey || !replay.Deadline.Equal(r.Deadline) {
		t.Fatal(replay, err)
	}
	alias.Profile.AssetID = uuid.NewString()
	if _, err = s.BeginRun(ctx, alias); !errors.Is(err, analysis.ErrCommandConflict) {
		t.Fatal("key reused", err)
	}
	var wg sync.WaitGroup
	var winners []analysis.ReservationCheck
	var workers []string
	var mu sync.Mutex
	for j := 0; j < 2; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := uuid.NewString()
			work, err := s.NextRunWork(ctx, r, worker)
			if err == nil {
				var c analysis.ReservationCheck
				c, err = s.ReserveRunWork(ctx, r, work)
				if err == nil {
					mu.Lock()
					winners = append(winners, c)
					workers = append(workers, worker)
					mu.Unlock()
				}
			}
			if err != nil && !errors.Is(err, analysis.ErrFenced) && !errors.Is(err, analysis.ErrRunPressure) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(winners) != 1 {
		t.Fatal("claims", len(winners))
	}
	c := winners[0]
	proof, err := s.CheckReservation(ctx, c)
	if err != nil || !proof.Current || proof.ValidUntil <= time.Now().Unix() || proof.RequestID != nil {
		t.Fatal(proof, err)
	}
	altered := c
	altered.ArgumentsHash = analysis.Digest("other")
	if _, err = s.CheckReservation(ctx, altered); !errors.Is(err, analysis.ErrFenced) {
		t.Fatal("wrong arguments", err)
	}
	current, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StopRun(ctx, current, "ANALYSIS_CANCELLED"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CheckReservation(ctx, c); !errors.Is(err, analysis.ErrFenced) {
		t.Fatal("new proof after cancellation", err)
	}
	if err = s.FinishRunWork(ctx, c, workers[0], nil, RunReceipt{}); !errors.Is(err, analysis.ErrFenced) {
		t.Fatal("late publication", err)
	}
	if err = s.RecordCrossing(ctx, c, workers[0], "fixture-decision", "fixture-correlation", []byte(`{"state":"IN_FLIGHT"}`), true); err != nil {
		t.Fatal(err)
	}
	var count int
	err = s.pool.QueryRow(ctx, `select count(*) from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_run r using(analysis_id)
		where a.analysis_id=$1 and a.fence=r.fence and a.proof_until>=to_timestamp($2) and a.finished_at is null and bytes>0 and a.crossing_hash is not null`,
		r.ID(), proof.ValidUntil).Scan(&count)
	if err != nil || count != 1 {
		t.Fatal("lost fence or reservation", count, err)
	}
}

func claimFixture(t *testing.T, ctx context.Context, s *Store, r analysis.RunRecord) (analysis.RunWork, analysis.ReservationCheck) {
	t.Helper()
	current, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	work, err := s.NextRunWork(ctx, current, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	check, err := s.ReserveRunWork(ctx, current, work)
	if err != nil {
		t.Fatal(err)
	}
	return work, check
}

func TestSupersededCrossingOnlyRecordsAttempt(t *testing.T) { // MCP-58E-01
	ctx, s, r := fixtureRun(t)
	w, c := claimFixture(t, ctx, s, r)
	if _, err := s.pool.Exec(ctx, "insert into ouf_mcp.analysis_run_incident(analysis_id,incident_id,state) values($1,$2,'RECOVERING')", r.ID(), uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		var raw string
		err := s.pool.QueryRow(ctx, `select jsonb_build_array(to_jsonb(r),to_jsonb(i))::text from ouf_mcp.analysis_run r
			join ouf_mcp.analysis_run_incident i using(analysis_id) where analysis_id=$1`, r.ID()).Scan(&raw)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := snapshot()
	body := []byte(`{"code":"ANALYSIS_STALE_EXECUTION"}`)
	if err := s.RecordCrossing(ctx, c, w.Worker, "fixture", "correlation", body, false); err != nil {
		t.Fatal(err)
	}
	var digest string
	err := s.pool.QueryRow(ctx, `select crossing_hash from ouf_mcp.analysis_run_attempt
		where attempt_id=$1 and crossing_decision='fixture' and crossing_correlation='correlation' and finished_at is null`, c.AttemptID).Scan(&digest)
	if err != nil || digest != analysis.BytesDigest(body) || snapshot() != before {
		t.Fatal("crossing changed the plan or lost attempt evidence", err)
	}
	if err := s.RecordCrossing(ctx, c, w.Worker, "fixture", "correlation", body, true); err != nil {
		t.Fatal(err)
	}
	current, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || current.State != "BLOCKED" || current.MandateState != "REVOKED" {
		t.Fatal("revocation crossing", current.State, current.MandateState, err)
	}
}

func fixturePreview(t *testing.T, r analysis.RunRecord) []byte {
	t.Helper()
	columns := []map[string]any{}
	for n := 0; n < r.Profile.ColumnCount; n++ {
		columns = append(columns, map[string]any{"name": "fixture", "ordinal": n})
	}
	raw, err := json.Marshal(struct {
		analysis.Profile
		View    string           `json:"view"`
		Columns []map[string]any `json:"columns"`
		Offset  int              `json:"offset"`
		HasMore bool             `json:"hasMore"`
	}{r.Profile, "COLUMNS", columns, 0, false})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRunCrashAroundOwnerInsertion(t *testing.T) { // MCP-50U-07
	for _, inserted := range []bool{false, true} {
		t.Run(fmt.Sprint("inserted-", inserted), func(t *testing.T) {
			ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) { l.OwnerDedupQualified = true })
			m, err := manifest.Load()
			if err != nil {
				t.Fatal(err)
			}
			w, c := claimFixture(t, ctx, s, r)
			receipt := RunReceipt{Decision: "fixture", Correlation: uuid.NewString(), Body: fixturePreview(t, r)}
			if err = s.FinishRunWork(ctx, c, w.Worker, m, receipt); err != nil {
				t.Fatal(err)
			}
			w, c = claimFixture(t, ctx, s, r)
			var batch analysis.Batch
			if err = json.Unmarshal(w.Arguments, &batch); err != nil || len(batch.Intents) != 1 {
				t.Fatal(batch, err)
			}
			receipt.Body, _ = json.Marshal(map[string]any{"contract": analysis.BatchContract, "results": []analysis.BatchResult{{ID: batch.Intents[0].ID, State: "NONE", Hits: []analysis.PublishedMatch{}}}})
			if err = s.FinishRunWork(ctx, c, w.Worker, m, receipt); err != nil {
				t.Fatal(err)
			}
			w, c = claimFixture(t, ctx, s, r)
			owner := map[string]string{}
			submit := func(work analysis.RunWork) string {
				var args struct{ IdempotencyKey string }
				if json.Unmarshal(work.Arguments, &args) != nil || args.IdempotencyKey == "" {
					t.Fatal("missing key")
				}
				if owner[args.IdempotencyKey] == "" {
					owner[args.IdempotencyKey] = uuid.NewString()
				}
				return owner[args.IdempotencyKey]
			}
			if inserted {
				submit(w)
			}
			// Move the executor across the lease boundary without changing
			// the reservation, owner effect or charge in the database.
			afterLease := time.Now().UTC().Add(time.Minute)
			s.analysisClock = func() time.Time { return afterLease }
			replica, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			replica.analysisClock = s.analysisClock
			second, claim := claimFixture(t, ctx, replica, r)
			if claim.ArgumentsHash != c.ArgumentsHash || claim.Fence == c.Fence || claim.AttemptID == c.AttemptID {
				t.Fatal("reconciliation identity")
			}
			job := submit(second)
			receipt.Body, _ = json.Marshal(map[string]string{"requestId": job, "state": "PENDING"})
			if err = s.FinishRunWork(ctx, c, w.Worker, m, receipt); !errors.Is(err, analysis.ErrFenced) {
				t.Fatal("late publication", err)
			}
			for i := 0; i < 2; i++ {
				if err = replica.FinishRunWork(ctx, claim, second.Worker, m, receipt); err != nil {
					t.Fatal(err)
				}
			}
			var jobs, attempts, unsettled int
			err = s.pool.QueryRow(ctx, `select (select count(*) from ouf_mcp.analysis_run_request where analysis_id=$1),
				count(*),count(*) filter(where actual is null) from ouf_mcp.analysis_run_attempt where analysis_id=$1 and operation='semantic.discovery.request'`, r.ID()).Scan(&jobs, &attempts, &unsettled)
			if err != nil || len(owner) != 1 || jobs != 1 || attempts != 2 || unsettled != 1 {
				t.Fatal(owner, jobs, attempts, unsettled, err)
			}
		})
	}
}

type fixtureRenewal struct {
	now    time.Time
	verify func(*analysis.RenewalDecision)
}

func (f fixtureRenewal) AuthorizeRunRenewal(_ context.Context, i orchestration.Identity, c analysis.RunRenewalCheck) (analysis.RenewalDecision, error) {
	if i.Delegation != "fixture-proof" || i.PrincipalID != c.Scope.Principal || c.Action != "renew-budget" || c.Contract != "ouf.analysis.authority-check.v1" {
		return analysis.RenewalDecision{}, orchestration.ErrUnauthorized
	}
	hash, err := c.Hash()
	if err != nil {
		return analysis.RenewalDecision{}, err
	}
	d := analysis.RenewalDecision{RequestHash: hash, DecisionRef: uuid.NewString(), GrantRef: "fixture-grant", ValidUntil: f.now.Add(time.Minute)}
	if f.verify != nil {
		f.verify(&d)
	}
	return d, nil
}

func renewalCommand(r analysis.RunRecord) (orchestration.Identity, analysis.RunRenewal) {
	actor := orchestration.Identity{Issuer: r.Issuer, ServicePrincipalID: r.Scope.Service, TenantID: r.Scope.Tenant, PrincipalID: r.Scope.Principal, ActorType: "HUMAN", Delegation: "fixture-proof"}
	return actor, analysis.RunRenewal{AnalysisID: r.ID(), IncidentID: analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()}),
		ProposalVersion: 1, ProposalHash: analysis.Digest("fixture-proposal"), ExpectedSeq: r.Seq,
		Increment: analysis.RunCost{Calls: 1}, Seconds: 60, ExpiresAt: r.Mandate.Expires, Reason: "fixture"}
}

func TestRunExhaustionAndConcurrentHumanSession(t *testing.T) { // MCP-50U-08, MCP-55A-01, MCP-55A-02
	ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) { l.Maximum.Calls = 4 })
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	// Four preview attempts under the original budget, with a settled
	// receipt for each. The last preview advances to published search.
	for n := 0; n < 3; n++ {
		w, c := claimFixture(t, ctx, s, r)
		if err := s.FinishRunWork(ctx, c, w.Worker, nil, RunReceipt{Code: "ANALYSIS_BACKOFF", RetryAfter: now.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
		verifyPausedRun(t, ctx, s, r)
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	w, c := claimFixture(t, ctx, s, r)
	if err = s.FinishRunWork(ctx, c, w.Worker, m, RunReceipt{Decision: "fixture", Correlation: uuid.NewString(), Body: fixturePreview(t, r)}); err != nil {
		t.Fatal(err)
	}
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	w, err = s.NextRunWork(ctx, r, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrRunExhausted) {
		t.Fatal("expected exhaustion", err)
	}
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || r.State != "BLOCKED" {
		t.Fatal(r, err)
	}
	actor, grant := renewalCommand(r)
	// Both approvals have read the same exhausted version before either
	// can enter the local transaction.
	approved := make(chan struct{}, 2)
	release := make(chan struct{})
	authority := fixtureRenewal{now, func(*analysis.RenewalDecision) {
		approved <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
	}}
	results := make(chan error, 2)
	for n := 0; n < 2; n++ {
		go func() { results <- s.RenewRunBudget(ctx, actor, grant, authority) }()
	}
	for n := 0; n < 2; n++ {
		select {
		case <-approved:
		case err := <-results:
			close(release)
			t.Fatal("failed before authorization", err)
		case <-ctx.Done():
			close(release)
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	a, b := <-results, <-results
	if !((a == nil && errors.Is(b, analysis.ErrRenewalUsed)) || (b == nil && errors.Is(a, analysis.ErrRenewalUsed))) {
		t.Fatal(a, b)
	}
	var compatible bool
	err = s.pool.QueryRow(ctx, `select r.seq=s.expected_seq+1 and r.state='RUNNING' and r.fence=s.first_fence
		and s.first_fence=4 and (s.maximum->>'Calls')::bigint=1 and (s.maximum->>'Attempts')::bigint>0
		from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_budget_session s using(analysis_id) where r.analysis_id=$1`, r.ID()).Scan(&compatible)
	if err != nil || !compatible {
		t.Fatal("CAS, fence or predecessor allowance", err)
	}
	verifyPausedRun(t, ctx, s, r)
	w, c = claimFixture(t, ctx, s, r)
	if err = s.FinishRunWork(ctx, c, w.Worker, nil, RunReceipt{Code: "ANALYSIS_BACKOFF", RetryAfter: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	verifyPausedRun(t, ctx, s, r)
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrRunExhausted) {
		t.Fatal("sixth call", err)
	}
	replica, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.analysisClock = s.analysisClock
	r, err = replica.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || r.State != "BLOCKED" || r.Reason != "ANALYSIS_BUDGET_EXHAUSTED" {
		t.Fatal(r.State, r.Reason, err)
	}
	grant.ExpectedSeq = r.Seq
	if err = replica.RenewRunBudget(ctx, actor, grant, fixtureRenewal{now: now}); !errors.Is(err, analysis.ErrRenewalUsed) {
		t.Fatal("reopened right", err)
	}
	var sessions, incidents, charged int
	err = s.pool.QueryRow(ctx, `select (select count(*) from ouf_mcp.analysis_run_budget_session where analysis_id=$1),
		(select count(*) from ouf_mcp.analysis_run_incident where analysis_id=$1 and state='OPEN'),
		(select sum(calls) from ouf_mcp.analysis_run_attempt where analysis_id=$1)`, r.ID()).Scan(&sessions, &incidents, &charged)
	if err != nil || sessions != 1 || incidents != 1 || charged != 5 {
		t.Fatal(sessions, incidents, charged, err)
	}
}

func TestRunBackoffRestartAndRevocation(t *testing.T) { // MCP-50U-11
	ctx, s, r := fixtureRun(t)
	work, check := claimFixture(t, ctx, s, r)
	pause := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	receipt := RunReceipt{Decision: "fixture", Correlation: uuid.NewString(), Code: "ANALYSIS_BACKOFF", RetryAfter: pause}
	if err := s.FinishRunWork(ctx, check, work.Worker, nil, receipt); err != nil {
		t.Fatal(err)
	}
	replica, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	resumed, err := replica.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || !resumed.NotBefore.Equal(pause) || resumed.Reason != "ANALYSIS_BACKOFF" || !resumed.Deadline.Equal(r.Deadline) {
		t.Fatal(resumed, err)
	}
	if _, err = replica.ReserveRunWork(ctx, resumed, work); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal(err)
	}
	if err = replica.StopRun(ctx, resumed, "ANALYSIS_MANDATE_REVOKED"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CheckReservation(ctx, check); !errors.Is(err, analysis.ErrFenced) {
		t.Fatal("revoked proof", err)
	}
	var charged, active int
	err = s.pool.QueryRow(ctx, `select count(*),count(*) filter(where finished_at is null) from ouf_mcp.analysis_run_attempt where analysis_id=$1`, r.ID()).Scan(&charged, &active)
	if err != nil || charged != 1 || active != 0 {
		t.Fatal(charged, active, err)
	}
}

func TestRunProofSurvivesCompletionAndWorkerHandoff(t *testing.T) { // MCP-50U-13
	ctx, s, r := fixtureRun(t)
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	w, c := claimFixture(t, ctx, s, r)
	proof, err := s.CheckReservation(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	receipt := RunReceipt{Decision: "fixture", Correlation: uuid.NewString(), Body: fixturePreview(t, r)}
	if err = s.FinishRunWork(ctx, c, w.Worker, m, receipt); err != nil {
		t.Fatal(err)
	}
	current, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.NextRunWork(ctx, current, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveRunWork(ctx, current, other); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal("refenced during proof", err)
	}
	var intact bool
	err = s.pool.QueryRow(ctx, `select a.fence=r.fence and a.proof_until=to_timestamp($2) and a.finished_at is not null
		from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_run r using(analysis_id) where attempt_id=$1`, c.AttemptID, proof.ValidUntil).Scan(&intact)
	if err != nil || !intact {
		t.Fatal(intact, err)
	}
}

func TestRunProviderPressureAndHoursLongRecovery(t *testing.T) { // MCP-50U-14
	ctx, s, r := fixtureRunWithTTL(t, 4*time.Hour, func(l *analysis.RunLimits) { l.ActivePlans = 2 })
	now := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	other := r.RunSpec
	other.Profile.AssetID = uuid.NewString()
	other.CommandKey = uuid.NewString()
	second, err := s.BeginRun(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	mandate := r.Mandate
	mandate.ID = uuid.NewString()
	raw, _ := json.Marshal(mandate)
	second, err = s.AttachMandate(ctx, second.RunSpec, raw)
	if err != nil {
		t.Fatal(err)
	}
	w, c := claimFixture(t, ctx, s, r)
	w2, err := s.NextRunWork(ctx, second, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveRunWork(ctx, second, w2); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal("subject/provider slot", err)
	}
	retry := now.Add(2 * time.Hour)
	if err = s.FinishRunWork(ctx, c, w.Worker, nil, RunReceipt{Code: "ANALYSIS_BACKOFF", RetryAfter: retry, Decision: "fixture", Correlation: uuid.NewString()}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveRunWork(ctx, second, w2); err != nil {
		t.Fatal("slot retained through pause", err)
	}
	otherReplica, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer otherReplica.Close()
	otherReplica.analysisClock = func() time.Time { return now }
	now = retry.Add(-time.Second)
	r, err = otherReplica.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = otherReplica.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal("early retry", err)
	}
	now = retry
	verifyPausedRun(t, ctx, otherReplica, r)
	r, err = otherReplica.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	w.Worker = uuid.NewString()
	claim, err := otherReplica.ReserveRunWork(ctx, r, w)
	if err != nil || claim.ArgumentsHash != c.ArgumentsHash || claim.AttemptID == c.AttemptID {
		t.Fatal(claim, err)
	}
	var attempts int
	if err = s.pool.QueryRow(ctx, "select sum(attempts) from ouf_mcp.analysis_run_attempt where analysis_id=$1", r.ID()).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatal(attempts, err)
	}
}

func TestRunIssueRetriesKeepFirstCommandAndBudget(t *testing.T) { // MCP-50U-16
	ctx, s, existing := fixtureRun(t)
	spec := existing.RunSpec
	spec.Scope.Principal = uuid.NewString()
	spec.CommandKey = uuid.NewString()
	r, err := s.BeginRun(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	now := r.Started.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	claimed, n, err := s.ClaimRunIssue(ctx, r)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	retry := now.Add(15 * time.Minute)
	if err = s.RecordIssueFailure(ctx, claimed, "ANALYSIS_BACKOFF", retry); err != nil {
		t.Fatal(err)
	}
	spec.CommandKey = uuid.NewString()
	alias, err := s.BeginRun(ctx, spec)
	if err != nil || alias.CommandKey != r.CommandKey || !alias.NotBefore.Equal(retry) {
		t.Fatal(alias, err)
	}
	if _, _, err = s.ClaimRunIssue(ctx, alias); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal(err)
	}
	now = retry
	second, n, err := s.ClaimRunIssue(ctx, alias)
	if err != nil || n != 2 || !second.BudgetEnd.Equal(r.BudgetEnd) {
		t.Fatal(second, n, err)
	}
}

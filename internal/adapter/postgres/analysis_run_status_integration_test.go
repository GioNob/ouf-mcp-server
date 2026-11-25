package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
)

func verifyPausedRun(t *testing.T, ctx context.Context, s *Store, r analysis.RunRecord) {
	t.Helper()
	claim, err := s.ClaimPausedStatus(ctx, r.Workload)
	if err != nil || claim.Run.ID() != r.ID() || claim.Token == "" {
		t.Fatal("missing status claim", err)
	}
	if err = s.FinishPausedStatus(ctx, claim, "ACTIVE", "fixture", "", 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestStoppedMandateDuringPause(t *testing.T) { // MCP-50V-02
	for _, state := range []string{"REVOKED", "EXPIRED"} {
		t.Run(state, func(t *testing.T) {
			ctx, s, r := fixtureRun(t)
			now := r.NotBefore.Add(time.Second)
			s.analysisClock = func() time.Time { return now }
			w, c := claimFixture(t, ctx, s, r)
			if err := s.FinishRunWork(ctx, c, w.Worker, nil, RunReceipt{Code: "ANALYSIS_BACKOFF", RetryAfter: now.Add(5 * time.Minute)}); err != nil {
				t.Fatal(err)
			}
			claim, err := s.ClaimPausedStatus(ctx, r.Workload)
			if err != nil || claim.Token == "" {
				t.Fatal(err)
			}
			if _, err = s.ClaimPausedStatus(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
				t.Fatal("duplicate status lease", err)
			}
			if err = s.FinishPausedStatus(ctx, claim, "ACTIVE", "fixture", "", 5*time.Second); err != nil {
				t.Fatal(err)
			}
			now = now.Add(4 * time.Second)
			if _, err = s.ClaimPausedStatus(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
				t.Fatal("early poll", err)
			}
			now = now.Add(time.Second)
			old, err := s.ClaimPausedStatus(ctx, r.Workload)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(6 * time.Second)
			fresh, err := s.ClaimPausedStatus(ctx, r.Workload)
			if err != nil || old.Token == fresh.Token {
				t.Fatal("lease recovery", err)
			}
			if err = s.FinishPausedStatus(ctx, old, "ACTIVE", "stale", "", 5*time.Second); !errors.Is(err, analysis.ErrFenced) {
				t.Fatal("late observer affected pause", err)
			}
			if err = s.FinishPausedStatus(ctx, fresh, state, "fixture-stop", "", 5*time.Second); err != nil {
				t.Fatal(err)
			}
			r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
			if err != nil || r.State != "BLOCKED" || r.Reason != "ANALYSIS_MANDATE_"+state || r.MandateState != state {
				t.Fatal(r.State, r.Reason, err)
			}
			var charged int
			if err = s.pool.QueryRow(ctx, "select sum(attempts) from ouf_mcp.analysis_run_attempt where analysis_id=$1", r.ID()).Scan(&charged); err != nil || charged != 1 {
				t.Fatal(charged, err)
			}
			if _, err = s.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrFenced) {
				t.Fatal("dispatch after stop", err)
			}
		})
	}
}

func TestOwnerWaitRequiresFreshStatusOnResume(t *testing.T) { // MCP-50V-02, MCP-58E-04
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	w, c := twoIntentBatch(t, ctx, s, r)
	if err := s.FinishRunWork(ctx, c, w.Worker, nil, noPublishedMatches(t, w)); err != nil {
		t.Fatal(err)
	}
	w, c = claimFixture(t, ctx, s, r)
	job := uuid.NewString()
	receipt := RunReceipt{Decision: "fixture", Correlation: "fixture", Body: []byte(`{"requestId":"` + job + `","pollAfterSeconds":90}`)}
	if err := s.FinishRunWork(ctx, c, w.Worker, nil, receipt); err != nil {
		t.Fatal(err)
	}
	checkDue := func(delay time.Duration) {
		t.Helper()
		var runDue, taskDue time.Time
		err := s.pool.QueryRow(ctx, `select not_before,(checkpoint #>> '{Queue,0,Task,notBefore}')::timestamptz
			from ouf_mcp.analysis_run where analysis_id=$1`, r.ID()).Scan(&runDue, &taskDue)
		if err != nil || !runDue.Equal(now.Add(delay)) || !taskDue.Equal(runDue) {
			t.Fatal("poll deadline", runDue, taskDue, err)
		}
	}
	checkDue(90 * time.Second)
	now = now.Add(89 * time.Second)
	verifyPausedRun(t, ctx, s, r)
	if _, err := s.DueRun(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("first poll admitted early", err)
	}
	// The next control observation is due five seconds after the last one.
	now = now.Add(5 * time.Second)
	verifyPausedRun(t, ctx, s, r)
	w, c = claimFixture(t, ctx, s, r)
	receipt.Body = []byte(`{"requestId":"` + job + `","state":"PENDING","pollAfterSeconds":120}`)
	if err := s.FinishRunWork(ctx, c, w.Worker, nil, receipt); err != nil {
		t.Fatal(err)
	}
	checkDue(120 * time.Second)
	verifyPausedRun(t, ctx, s, r)
	if _, err := s.DueRun(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("owner wait was shortened", err)
	}
	now = now.Add(120 * time.Second)
	verifyPausedRun(t, ctx, s, r)
	current, err := s.DueRun(ctx, r.Workload)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(6 * time.Second)
	if _, err = s.ReserveRunWork(ctx, current, w); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal("restarted worker reused stale ACTIVE", err)
	}
	verifyPausedRun(t, ctx, s, r)
	w, _ = claimFixture(t, ctx, s, r)
	if w.Operation != "semantic.discovery.status" {
		t.Fatal("lost pending owner job")
	}
}

func TestUnavailableStatusSurvivesQuotaWait(t *testing.T) { // MCP-50V-03
	ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) { l.WaitSeconds = 60 })
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	if err := s.DelayRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimPausedStatus(ctx, r.Workload)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishPausedStatus(ctx, c, "UNAVAILABLE", "", "120", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if _, err = s.DueRun(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("unverified work due", err)
	}
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.NextRunWork(ctx, r, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrRunPressure) {
		t.Fatal("unverified reservation", err)
	}
	if _, err = s.ClaimPausedStatus(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("Retry-After ignored", err)
	}
	now = r.Deadline
	c, err = s.ClaimPausedStatus(ctx, r.Workload)
	if err != nil || c.Token != "" {
		t.Fatal("poll past deadline", err)
	}
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || r.State != "BLOCKED" || r.Reason != "WAIT_DEADLINE_EXPIRED" {
		t.Fatal(r.State, r.Reason, err)
	}
}

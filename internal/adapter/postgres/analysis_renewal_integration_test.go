package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// Arrange the same durable plan incident as a failed reservation, leaving
// checkpoint, mandate, attempts and the store clock under the test's control.
func exhaustFixture(t *testing.T, ctx context.Context, s *Store, r analysis.RunRecord) analysis.RunRecord {
	t.Helper()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = exhaustRun(ctx, tx, r); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRenewalMigrationPreserves019Sessions(t *testing.T) { // MCP-55A-02, D-080
	ctx := context.Background()
	f := migrationFixtureBefore(t, "020_")
	s := Store{pool: f.pool}
	// Both plans are written using the predecessor's column lists. Only
	// the first already has session history at the time of migration.
	used, fresh := analysis.Digest("used"), analysis.Digest("fresh")
	for _, id := range []string{used, fresh} {
		_, err := f.pool.Exec(ctx, `insert into ouf_mcp.analysis_run(analysis_id,issuer,workload,tenant,subject,checkpoint,counts,spec,
			started,budget_end,deadline,not_before,state) values($1,'fixture','fixture','fixture','fixture','{}','{}','{}',now(),now(),now(),now(),'BLOCKED')`, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, "insert into ouf_mcp.analysis_run_incident values($1,$2,'OPEN')", id, analysis.Digest(id)); err != nil {
			t.Fatal(err)
		}
	}
	insert019 := func(id string, seq int) error {
		_, err := f.pool.Exec(ctx, `insert into ouf_mcp.analysis_run_budget_session(analysis_id,incident_id,expected_seq,actor,reason,
			decision_ref,maximum,started,ends_at,first_fence) values($1,$2,$3,'human','fixture',$4,
			'{"Calls":1,"Attempts":1,"Objects":1,"Bytes":1,"Millis":1}',now(),now()+interval '1 minute',$3)`, id, analysis.Digest(id), seq, fmt.Sprint(seq))
		return err
	}
	for _, seq := range []int{2, 4} {
		if err := insert019(used, seq); err != nil {
			t.Fatal(err)
		}
	}
	if err := Migrate(ctx, f.pool); err != nil {
		t.Fatal(err)
	}
	if err := s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	var intact bool
	err := f.pool.QueryRow(ctx, `select count(*)=2 and count(*) filter(where renewal_used)=1 and bool_and(increment is null
		and first_fence=expected_seq and renewal_used=(expected_seq=2)) from ouf_mcp.analysis_run_budget_session where analysis_id=$1`, used).Scan(&intact)
	if err != nil || !intact {
		t.Fatal("019 history", err)
	}
	if err = insert019(fresh, 1); err != nil {
		t.Fatal("first predecessor write after migration", err)
	}
	for _, id := range []string{used, fresh} {
		err = insert019(id, 6)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "analysis_run_renewal_once" {
			t.Fatal("expected database uniqueness across versions", err)
		}
	}
}

func TestRenewalDimensionLimits(t *testing.T) { // MCP-55A-03
	ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) {
		l.Maximum = analysis.RunCost{Calls: 3, Attempts: 7, Objects: 11, Bytes: 15, Millis: 19}
	})
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	r = exhaustFixture(t, ctx, s, r)
	actor, want := renewalCommand(r)
	authority := fixtureRenewal{now: now}
	for _, bad := range []analysis.RunCost{
		{}, {Calls: 1}, {Attempts: 2}, {Objects: 3}, {Bytes: 4}, {Millis: 5},
		{Calls: -1, Bytes: 1}, {Attempts: -1, Bytes: 1}, {Objects: -1, Bytes: 1}, {Bytes: -1, Millis: 1}, {Millis: -1, Bytes: 1},
	} {
		want.Increment = bad
		if err := s.RenewRunBudget(ctx, actor, want, authority); !errors.Is(err, analysis.ErrInvalid) {
			t.Fatalf("increment %+v: %v", bad, err)
		}
	}
	want.Increment = analysis.RunCost{Attempts: 1, Objects: 2, Bytes: 3, Millis: 4}
	for _, seconds := range []int{0, 3601} {
		want.Seconds = seconds
		if err := s.RenewRunBudget(ctx, actor, want, authority); !errors.Is(err, analysis.ErrInvalid) {
			t.Fatal(seconds, err)
		}
	}
	want.Seconds = 3600
	if err := s.RenewRunBudget(ctx, actor, want, authority); err != nil {
		t.Fatal(err)
	}
	var end time.Time
	var exact bool
	err := s.pool.QueryRow(ctx, `select ends_at,increment='{"Calls":0,"Attempts":1,"Objects":2,"Bytes":3,"Millis":4}'::jsonb
		and started=$2 and proposal_version=$3 and proposal_hash=$4 and grant_ref='fixture-grant'
		from ouf_mcp.analysis_run_budget_session where analysis_id=$1`, r.ID(), now, want.ProposalVersion, want.ProposalHash).Scan(&end, &exact)
	if err != nil || !exact || !end.Equal(r.Mandate.Expires) {
		t.Fatal("mandate boundary or stored decision", end, exact, err)
	}
}

func TestRenewalAbsoluteDeadline(t *testing.T) { // MCP-55A-03
	for _, elapsed := range []time.Duration{time.Second, 60 * time.Second} {
		t.Run(elapsed.String(), func(t *testing.T) {
			ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) {
				l.LifetimeSeconds, l.WaitSeconds = 60, 60
			})
			now := r.Started.Add(elapsed)
			s.analysisClock = func() time.Time { return now }
			r = exhaustFixture(t, ctx, s, r)
			actor, want := renewalCommand(r)
			err := s.RenewRunBudget(ctx, actor, want, fixtureRenewal{now: now})
			if elapsed == 60*time.Second {
				if !errors.Is(err, analysis.ErrRunExhausted) {
					t.Fatal("absolute lifetime elapsed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			renewed, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
			if err != nil || !renewed.BudgetEnd.Equal(r.Started.Add(time.Minute)) || !renewed.Deadline.Equal(r.Deadline) {
				t.Fatal(renewed.BudgetEnd, renewed.Deadline, err)
			}
		})
	}
}

func TestRenewalConfiguredSession(t *testing.T) { // MCP-55A-03
	ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) {
		l.Maximum.Calls, l.RenewalPercent, l.RenewalSeconds = 19, 10, 90
	})
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	r = exhaustFixture(t, ctx, s, r)
	actor, want := renewalCommand(r)
	authority := fixtureRenewal{now: now}
	want.Increment.Calls = 2
	if err := s.RenewRunBudget(ctx, actor, want, authority); !errors.Is(err, analysis.ErrInvalid) {
		t.Fatal("installation percentage", err)
	}
	want.Increment.Calls, want.Seconds = 1, 91
	if err := s.RenewRunBudget(ctx, actor, want, authority); !errors.Is(err, analysis.ErrInvalid) {
		t.Fatal("installation duration", err)
	}
	want.Seconds, want.ExpiresAt = 90, now.Add(45*time.Second)
	if err := s.RenewRunBudget(ctx, actor, want, authority); err != nil {
		t.Fatal(err)
	}
	r, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || !r.BudgetEnd.Equal(want.ExpiresAt) {
		t.Fatal("authorized proposal expiry", r.BudgetEnd, err)
	}
}

func TestRenewalCommitGuards(t *testing.T) { // MCP-55A-04
	for _, name := range []string{"pressure", "revoked", "uncertain", "expired-proof", "expired-at-commit", "different-proof", "missing-grant", "stale-sequence", "stale-fence"} {
		t.Run(name, func(t *testing.T) {
			ctx, s, r := fixtureRun(t)
			now := r.NotBefore.Add(time.Second)
			s.analysisClock = func() time.Time { return now }
			var pending analysis.ReservationCheck
			if name == "uncertain" {
				w, c := twoIntentBatch(t, ctx, s, r)
				if err := s.FinishRunWork(ctx, c, w.Worker, nil, noPublishedMatches(t, w)); err != nil {
					t.Fatal(err)
				}
				_, pending = claimFixture(t, ctx, s, r)
				now = now.Add(time.Minute) // An expired owner lease still has an uncertain effect.
			}
			r = exhaustFixture(t, ctx, s, r)
			actor, want := renewalCommand(r)
			expected := orchestration.ErrUnauthorized
			authority := fixtureRenewal{now: now}
			switch name {
			case "pressure":
				expected = analysis.ErrRunPressure
				other := r.RunSpec
				other.CommandKey, other.Profile.AssetID = uuid.NewString(), uuid.NewString()
				if _, err := s.BeginRun(ctx, other); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				authority.verify = func(*analysis.RenewalDecision) {
					if err := s.StopRun(ctx, r, "ANALYSIS_MANDATE_REVOKED"); err != nil {
						t.Fatal(err)
					}
				}
			case "uncertain":
				expected = analysis.ErrFenced
			case "expired-proof":
				authority.verify = func(d *analysis.RenewalDecision) { now = d.ValidUntil }
			case "expired-at-commit":
				// The first transaction clock admits the receipt. The second,
				// after the CAS and session insert, must roll both back.
				reads := 0
				s.analysisClock = func() time.Time {
					reads++
					if reads == 1 {
						return now
					}
					return now.Add(time.Minute)
				}
			case "different-proof":
				authority.verify = func(d *analysis.RenewalDecision) { d.RequestHash = analysis.Digest("other") }
			case "missing-grant":
				authority.verify = func(d *analysis.RenewalDecision) { d.GrantRef = "" }
			case "stale-sequence":
				expected = analysis.ErrFenced
				want.ExpectedSeq--
			case "stale-fence":
				expected = analysis.ErrFenced
				authority.verify = func(*analysis.RenewalDecision) {
					if _, err := s.pool.Exec(ctx, "update ouf_mcp.analysis_run set fence=fence+1 where analysis_id=$1", r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := s.RenewRunBudget(ctx, actor, want, authority); !errors.Is(err, expected) {
				t.Fatal(err)
			}
			var blocked bool
			err := s.pool.QueryRow(ctx, `select r.state='BLOCKED' and i.state='OPEN'
				and not exists(select 1 from ouf_mcp.analysis_run_budget_session where analysis_id=$1)
				from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_incident i using(analysis_id) where r.analysis_id=$1`, r.ID()).Scan(&blocked)
			if err != nil || !blocked {
				t.Fatal("failed approval consumed the right or resumed the run", err)
			}
			if name == "uncertain" {
				var intact bool
				err = s.pool.QueryRow(ctx, `select a.owner_key_hash=$2 and a.finished_at is null and a.superseded_at is null
					and a.fence=r.fence and a.fence::text=$3 from ouf_mcp.analysis_run_attempt a
					join ouf_mcp.analysis_run r using(analysis_id) where attempt_id=$1`, pending.AttemptID,
					analysis.BytesDigest([]byte("analysis:"+analysis.Digest([]string{r.ID(), pending.IntentKey}))), pending.Fence).Scan(&intact)
				if err != nil || !intact {
					t.Fatal("uncertain owner effect", err)
				}
			}
		})
	}
}

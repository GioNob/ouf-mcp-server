package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/uuid"
)

func twoIntentBatch(t *testing.T, ctx context.Context, s *Store, r analysis.RunRecord) (analysis.RunWork, analysis.ReservationCheck) {
	t.Helper()
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	w, c := claimFixture(t, ctx, s, r)
	page := bytes.Replace(fixturePreview(t, r), []byte(`"name":"fixture"`), []byte(`"name":"fixture-other"`), 1)
	if err = s.FinishRunWork(ctx, c, w.Worker, m, RunReceipt{Decision: "fixture", Correlation: "fixture", Body: page}); err != nil {
		t.Fatal(err)
	}
	return claimFixture(t, ctx, s, r)
}

func noPublishedMatches(t *testing.T, w analysis.RunWork) RunReceipt {
	t.Helper()
	var b analysis.Batch
	if json.Unmarshal(w.Arguments, &b) != nil || len(b.Intents) != 2 {
		t.Fatal("expected two synthetic intents")
	}
	var rows []analysis.BatchResult
	for _, i := range b.Intents {
		rows = append(rows, analysis.BatchResult{ID: i.ID, State: "NONE", Hits: []analysis.PublishedMatch{}})
	}
	raw, _ := json.Marshal(map[string]any{"contract": analysis.BatchContract, "results": rows})
	return RunReceipt{Decision: "fixture", Correlation: "fixture", Body: raw}
}

func TestRunIndependentIntentFinishesAfterGroupStop(t *testing.T) { // MCP-50V-04, MCP-50V-05
	for _, budget := range []bool{false, true} {
		name := "permanent"
		if budget {
			name = "group-budget"
		}
		t.Run(name, func(t *testing.T) {
			ctx, s, r := fixtureRun(t)
			now := r.NotBefore.Add(time.Second)
			s.analysisClock = func() time.Time { return now }
			w, c := twoIntentBatch(t, ctx, s, r)
			if err := s.FinishRunWork(ctx, c, w.Worker, nil, noPublishedMatches(t, w)); err != nil {
				t.Fatal(err)
			}
			w, c = claimFixture(t, ctx, s, r)
			if w.Operation != "semantic.discovery.request" || len(w.Ordinals) != 1 {
				t.Fatal("fixture first group")
			}
			first := w.Intent
			code := "ANALYSIS_OWNER_FAILURE"
			if budget {
				code = "ANALYSIS_BUDGET_EXHAUSTED"
				if err := s.FinishRunWork(ctx, c, w.Worker, nil, RunReceipt{Code: "ANALYSIS_BACKOFF", RetryAfter: now.Add(time.Minute)}); err != nil {
					t.Fatal(err)
				}
				// Populate the already settled portion of a long incident. The
				// next real reservation must quarantine before allocating attempt 129.
				_, err := s.pool.Exec(ctx, `insert into ouf_mcp.analysis_run_attempt
					(attempt_id,analysis_id,fence,generation,mandate_id,intent_key,operation,arguments_hash,ordinals_ref,ordinals,
					 provider,issuer,tenant,subject,worker,reserved_at,lease_end,calls,attempts,objects,bytes,millis,finished_at,actual)
					select gen_random_uuid(),analysis_id,fence+n,generation,mandate_id,intent_key,operation,arguments_hash,ordinals_ref,ordinals,
					 provider,issuer,tenant,subject,worker,reserved_at,lease_end,calls,attempts,objects,bytes,millis,finished_at,actual
					from ouf_mcp.analysis_run_attempt cross join generate_series(1,127) n where attempt_id=$1`, c.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.pool.Exec(ctx, "update ouf_mcp.analysis_run set fence=fence+127 where analysis_id=$1", r.ID()); err != nil {
					t.Fatal(err)
				}
				now = now.Add(2 * time.Minute)
				verifyPausedRun(t, ctx, s, r)
				r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrRunExhausted) {
					t.Fatal("missing group quarantine", err)
				}
				if _, err = s.ReserveRunWork(ctx, r, w); !errors.Is(err, analysis.ErrFenced) {
					t.Fatal("replayed group", err)
				}
				var incidents int
				if err = s.pool.QueryRow(ctx, "select count(*) from ouf_mcp.analysis_group_quarantine where analysis_id=$1 and intent_key=$2 and state='OPEN'", r.ID(), first).Scan(&incidents); err != nil || incidents != 1 {
					t.Fatal("open incidents", incidents, err)
				}
			} else if err := s.FinishRunWork(ctx, c, w.Worker, nil, RunReceipt{Code: code, Decision: "fixture", Correlation: "fixture"}); err != nil {
				t.Fatal(err)
			}
			w, c = claimFixture(t, ctx, s, r)
			if w.Operation != "semantic.discovery.request" || w.Intent == first {
				t.Fatal("independent intent did not progress")
			}
			job := uuid.NewString()
			receipt := RunReceipt{Decision: "fixture", Correlation: "fixture", Body: []byte(`{"requestId":"` + job + `"}`)}
			if err := s.FinishRunWork(ctx, c, w.Worker, nil, receipt); err != nil {
				t.Fatal(err)
			}
			now = now.Add(5 * time.Second)
			verifyPausedRun(t, ctx, s, r)
			w, c = claimFixture(t, ctx, s, r)
			receipt.Body = []byte(`{"state":"SUCCEEDED","completedCandidateCount":0,"requestId":"` + job + `"}`)
			if err := s.FinishRunWork(ctx, c, w.Worker, nil, receipt); err != nil {
				t.Fatal(err)
			}
			r, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
			if err != nil || r.State != "PARTIAL" {
				t.Fatal(r.State, err)
			}
			summary, err := s.RunSummary(ctx, r.Scope, r.Issuer, r.ID(), analysis.Position{Seq: r.Seq, Last: -1, Limit: 18}, true)
			if err != nil || summary.Counts.Blocked != 1 || summary.Counts.Zero != 17 || len(summary.Items) != 18 || !bytes.Contains(summary.Items[0], []byte(code)) {
				t.Fatal("ordinal evidence or counts", summary.Counts, err)
			}
		})
	}
}

func TestCorrelatedBatchOutage(t *testing.T) { // MCP-50V-06
	ctx, s, r := fixtureRun(t, func(l *analysis.RunLimits) { l.ActivePlans = 2 })
	now := r.NotBefore.Add(2 * time.Second)
	s.analysisClock = func() time.Time { return now }
	other := r.RunSpec
	other.Scope.Principal = uuid.NewString()
	other.Profile.AssetID = uuid.NewString()
	other.CommandKey = uuid.NewString()
	second, err := s.BeginRun(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	m := r.Mandate
	m.ID = uuid.NewString()
	raw, _ := json.Marshal(m)
	second, err = s.AttachMandate(ctx, second.RunSpec, raw)
	if err != nil {
		t.Fatal(err)
	}
	plans := []analysis.RunRecord{r, second}
	outage := RunReceipt{Code: "ANALYSIS_BACKOFF", DependencyCause: "SEM_BATCH_UNAVAILABLE", RetryAfter: now.Add(time.Minute)}
	for _, p := range plans {
		w, c := twoIntentBatch(t, ctx, s, p)
		for range 2 {
			if err = s.FinishRunWork(ctx, c, w.Worker, nil, outage); err != nil {
				t.Fatal(err)
			}
		}
	}
	scope := analysis.Digest([]string{r.Issuer, r.Workload, r.Scope.Service, r.Scope.Tenant})
	var n, g, o int
	var open bool
	check := func(wantOpen bool) {
		t.Helper()
		if err := s.pool.QueryRow(ctx, `select plans,groups,ordinals,resolved_at is null from ouf_mcp.analysis_dependency_counts
			where scope_key=$1 and dependency='semantic' and cause='SEM_BATCH_UNAVAILABLE'`, scope).Scan(&n, &g, &o, &open); err != nil || n != 2 || g != 4 || o != 36 || open != wantOpen {
			t.Fatal(n, g, o, open, err)
		}
	}
	check(true)
	var projected string
	if err := s.pool.QueryRow(ctx, "select to_jsonb(v)::text from ouf_mcp.analysis_dependency_counts v where scope_key=$1", scope).Scan(&projected); err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{r.Workload, r.Scope.Tenant, r.Scope.Principal, second.Scope.Principal, "fixture-other"} {
		if strings.Contains(projected, private) {
			t.Fatal("dependency projection exposed private input")
		}
	}
	now = outage.RetryAfter
	for i, p := range plans {
		// Both are due: the claim may select either one first.
		c, err := s.ClaimPausedStatus(ctx, p.Workload)
		if err != nil || c.Token == "" {
			t.Fatal(err)
		}
		if err = s.FinishPausedStatus(ctx, c, "ACTIVE", "fixture", "", 5*time.Second); err != nil {
			t.Fatal(err)
		}
		w, claim := claimFixture(t, ctx, s, c.Run)
		if err = s.FinishRunWork(ctx, claim, w.Worker, nil, noPublishedMatches(t, w)); err != nil {
			t.Fatal(err)
		}
		check(i == 0)
	}
}

package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed maintenance/019_analysis_run_retention.sql
var installAnalysisRetentionSQL string

// MCP-50V-10. Install twice through the administrative connection, then give
// the test a pool in which every connection runs as the maintenance role.
func analysisRetentionFixture(t *testing.T, ctx context.Context, s *Store) *Store {
	t.Helper()
	for range 2 {
		if _, err := s.pool.Exec(ctx, installAnalysisRetentionSQL); err != nil {
			t.Fatal(err)
		}
	}
	cfg := s.pool.Config()
	cfg.ConnConfig.RuntimeParams["role"] = "ouf_mcp_maintenance_role"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var confined bool
	err = pool.QueryRow(ctx, `select current_user='ouf_mcp_maintenance_role'
		and p.prosecdef and p.proowner='ouf_mcp_retention_owner'::regrole
		and not exists(select 1 from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner)))
			where privilege_type='EXECUTE' and grantee not in (p.proowner,'ouf_mcp_maintenance_role'::regrole))
		and not exists(select 1 from pg_auth_members where roleid=p.proowner)
		and not has_table_privilege(current_user,'ouf_mcp.analysis_run_read','DELETE')
		and not has_table_privilege(current_user,'ouf_mcp.analysis_run_snapshot','DELETE')
		and not has_table_privilege(current_user,'ouf_mcp.analysis_run_attempt','DELETE')
		from pg_proc p where p.oid='ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz)'::regprocedure`).Scan(&confined)
	if err != nil || !confined {
		t.Fatalf("unexpected analysis retention privileges: %v", err)
	}
	return &Store{pool: pool, analysisClock: s.analysisClock}
}

func TestAnalysisRunRetention(t *testing.T) { // MCP-50V-07
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	maintenance := analysisRetentionFixture(t, ctx, s)
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	w, c := claimFixture(t, ctx, s, r)
	if err = s.FinishRunWork(ctx, c, w.Worker, m, RunReceipt{Decision: "fixture", Correlation: "fixture", Body: fixturePreview(t, r)}); err != nil {
		t.Fatal(err)
	}
	// This read pins a noncurrent snapshot across the first sweep.
	if err = s.AuditRunSummary(ctx, r, "fixture", 0, 0); err != nil {
		t.Fatal(err)
	}
	clean := func(p analysis.RunRetention) {
		t.Helper()
		if err := maintenance.CleanRunHistory(ctx, r.Workload, p); err != nil {
			t.Fatal(err)
		}
	}
	counts := func() (a, reads, snapshots int) {
		t.Helper()
		err := s.pool.QueryRow(ctx, `select (select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1),
			(select count(*) from ouf_mcp.analysis_run_read where analysis_id=$1),
			(select count(*) from ouf_mcp.analysis_run_snapshot where analysis_id=$1)`, r.ID()).Scan(&a, &reads, &snapshots)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	// Age fixture rows relative to a fixed maintenance clock, including the
	// terminal snapshot. The definer rejects even test cutoffs in the future.
	now = r.Started.Add(-time.Minute)
	old := now.Add(-10 * 24 * time.Hour)
	age := func() {
		t.Helper()
		for _, stmt := range []string{
			"update ouf_mcp.analysis_run_snapshot set captured_at=$2 where analysis_id=$1",
			"update ouf_mcp.analysis_run_read set served_at=$2 where analysis_id=$1",
			"update ouf_mcp.analysis_run_attempt set reserved_at=$2::timestamptz-interval '2 seconds',lease_end=$2,finished_at=$2 where analysis_id=$1 and finished_at is not null",
		} {
			if _, err := s.pool.Exec(ctx, stmt, r.ID(), old); err != nil {
				t.Fatal(err)
			}
		}
	}
	day := 24 * time.Hour
	p := analysis.RunRetention{Reservations: day, Attempts: day, Snapshots: day, Reads: day}
	age()
	a, reads, snaps := counts()
	clean(p)
	if gotA, gotR, gotS := counts(); gotA != a || gotR != reads || gotS != snaps {
		t.Fatal("purged a running plan")
	}
	maintenanceNow := now
	now = r.NotBefore.Add(2 * time.Second)
	w, c = claimFixture(t, ctx, s, r)
	var b analysis.Batch
	if json.Unmarshal(w.Arguments, &b) != nil || len(b.Intents) != 1 {
		t.Fatal("expected one published intent")
	}
	hit := analysis.PublishedMatch{SemanticID: "urn:fixture:match", SemanticVersion: "1",
		RevisionID: r.Profile.AssetID, PublicationSetID: r.Profile.ProfileID, Score: 100,
		Reasons: []string{"EXACT_LABEL", "PREFERRED_LANGUAGE"}}
	rows := []analysis.BatchResult{{ID: b.Intents[0].ID, State: "MATCH", Hits: []analysis.PublishedMatch{hit}}}
	body, _ := json.Marshal(map[string]any{"contract": analysis.BatchContract, "results": rows})
	if err := s.FinishRunWork(ctx, c, w.Worker, m, RunReceipt{Decision: "fixture", Correlation: "fixture", Body: body}); err != nil {
		t.Fatal(err)
	}
	now = maintenanceNow
	age()
	p.Reservations = 15 * day
	p.Reads = 15 * day
	clean(p)
	if a, reads, snaps = counts(); a != 2 || reads != 1 || snaps != 2 {
		t.Fatal("retained reservation, read and two pinned snapshots", a, reads, snaps)
	}
	p.Reservations = day
	p.Attempts = 15 * day
	clean(p)
	if a, _, _ = counts(); a != 2 {
		t.Fatal("short reservation TTL erased attempt audit")
	}
	p.Attempts = day
	p.Reads = day
	for range 2 {
		clean(p)
		if a, reads, snaps = counts(); a != 0 || reads != 0 || snaps != 1 {
			t.Fatal("terminal snapshot or idempotent purge", a, reads, snaps)
		}
	}
}

package postgres

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func thsHolder(r analysis.RunRecord) orchestration.Identity {
	i, _ := renewalCommand(r)
	i.ServicePrincipalID = r.Workload
	return i
}

func proposalCommand(p analysis.RenewalProposal, decision string) analysis.RenewalCommand {
	return analysis.RenewalCommand{ReviewRef: p.ReviewRef, Version: p.Version, Hash: p.Hash, ExpectedSeq: strconv.FormatInt(p.ExpectedSeq, 10), Key: uuid.NewString(), Decision: decision, Items: []analysis.RenewalItem{}}
}

func TestProposalVersionsAndExpiry(t *testing.T) { // MCP-55B-02
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	r = exhaustFixture(t, ctx, s, r)
	p, err := s.CurrentRenewal(ctx, r)
	if err != nil || p.Version != 1 || p.State != "OPEN" || p.Previous != nil || len(p.Remaining) != r.Profile.ColumnCount {
		t.Fatal(p, err)
	}
	ths := thsHolder(r)
	foreign := ths
	foreign.TenantID += "-foreign"
	if _, err = s.ReadRenewal(ctx, foreign, p.ReviewRef); !errors.Is(err, analysis.ErrScope) {
		t.Fatal(err)
	}
	change := proposalCommand(p, "CHANGES_REQUESTED")
	change.Items = []analysis.RenewalItem{{Field: "Calls", Comment: "Ridurre il numero di chiamate"}}
	authority := fixtureRenewal{now: now}
	if _, err = s.DecideRenewal(ctx, ths, change, authority); err != nil {
		t.Fatal(err)
	}
	chat, _ := renewalCommand(r)
	inc := p.Increment
	inc.Calls--
	revision := analysis.RenewalRevision{AnalysisID: r.ID(), IncidentID: p.IncidentID, Version: 1, Key: uuid.NewString(), RequestedChanges: change.Items, Increment: &inc}
	next, err := s.ReviseRenewal(ctx, chat, revision)
	if err != nil || next.Version != 2 || next.IncidentID != p.IncidentID || next.ReviewRef == p.ReviewRef || next.Previous == nil ||
		next.Previous.Increment != p.Increment || next.Increment != inc || !slices.Equal(next.RequestedChanges, change.Items) {
		t.Fatal(next, err)
	}
	if replay, err := s.ReviseRenewal(ctx, chat, revision); err != nil || replay.ReviewRef != next.ReviewRef {
		t.Fatal(replay, err)
	}
	if old, err := s.ReadRenewal(ctx, ths, p.ReviewRef); err != nil || old.State != "SUPERSEDED" {
		t.Fatal(old, err)
	}
	if _, err = s.DecideRenewal(ctx, ths, proposalCommand(p, "CLOSE_PARTIAL"), nilAuthority(t)); !errors.Is(err, analysis.ErrFenced) {
		t.Fatal(err)
	}
	now = next.Expires
	if _, err = s.ClaimPausedStatus(ctx, r.Workload); !errors.Is(err, analysis.ErrScope) {
		t.Fatal(err)
	}
	var state string
	if err = s.pool.QueryRow(ctx, "select state from ouf_mcp.analysis_renewal_proposal where review_ref=$1", next.ReviewRef).Scan(&state); err != nil || state != "EXPIRED" {
		t.Fatal(state, err)
	}
	if _, err = s.DecideRenewal(ctx, ths, proposalCommand(next, "APPROVE"), nilAuthority(t)); !errors.Is(err, analysis.ErrFenced) {
		t.Fatal(err)
	}
	expired, err := s.ReadRenewal(ctx, ths, next.ReviewRef)
	if err != nil || expired.State != "EXPIRED" {
		t.Fatal(expired.State, err)
	}
	var incident string
	if err = s.pool.QueryRow(ctx, "select state from ouf_mcp.analysis_run_incident where analysis_id=$1", r.ID()).Scan(&incident); err != nil || incident != "OPEN" {
		t.Fatal(incident, err)
	}
}

func nilAuthority(t *testing.T) fixtureRenewal {
	return fixtureRenewal{verify: func(*analysis.RenewalDecision) { t.Fatal("unexpected renewal-check") }}
}

func TestRenewalApprovalReplay(t *testing.T) { // MCP-55B-03
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	r = exhaustFixture(t, ctx, s, r)
	p, err := s.CurrentRenewal(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	i := thsHolder(r)
	approve := proposalCommand(p, "APPROVE")
	a := fixtureRenewal{now: now}
	results := make(chan json.RawMessage, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() { receipt, e := s.DecideRenewal(ctx, i, approve, a); results <- receipt; errs <- e }()
	}
	first, second := <-results, <-results
	if e1, e2 := <-errs, <-errs; e1 != nil || e2 != nil || !bytes.Equal(first, second) || len(first) == 0 {
		t.Fatal(e1, e2)
	}
	restarted := &Store{pool: s.pool, analysisClock: s.analysisClock}
	replay, err := restarted.DecideRenewal(ctx, i, approve, nilAuthority(t))
	if err != nil || !bytes.Equal(replay, first) {
		t.Fatal(string(replay), err)
	}
	other := i
	other.PrincipalID += "-foreign"
	if _, err = s.DecideRenewal(ctx, other, approve, nilAuthority(t)); !errors.Is(err, analysis.ErrScope) {
		t.Fatal(err)
	}
	approve.Decision = "CLOSE_PARTIAL"
	if _, err = s.DecideRenewal(ctx, i, approve, nilAuthority(t)); !errors.Is(err, analysis.ErrCommandConflict) {
		t.Fatal(err)
	}
	current, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil || current.State != "RUNNING" {
		t.Fatal(current.State, err)
	}
	current = exhaustFixture(t, ctx, s, current)
	next, err := s.CurrentRenewal(ctx, current)
	if err != nil || next.IncidentID != p.IncidentID || next.Version != 2 {
		t.Fatal(next, err)
	}
	if _, err = s.DecideRenewal(ctx, i, proposalCommand(next, "APPROVE"), a); !errors.Is(err, analysis.ErrRenewalUsed) {
		t.Fatal(err)
	}
	var sessions, receipts int
	if err = s.pool.QueryRow(ctx, `select (select count(*) from ouf_mcp.analysis_run_budget_session where analysis_id=$1),
		(select count(*) from ouf_mcp.analysis_renewal_receipt where review_ref=$2)`, r.ID(), p.ReviewRef).Scan(&sessions, &receipts); err != nil || sessions != 1 || receipts != 1 {
		t.Fatal(sessions, receipts, err)
	}
}

// An expensive residual cannot consume a finite session. The other group
// still completes through the normal worker before THS closes the remainder.
func TestPartialClosePreservesIndependentOutcomes(t *testing.T) { // MCP-55B-04
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	w, c := twoIntentBatch(t, ctx, s, r)
	if err := s.FinishRunWork(ctx, c, w.Worker, nil, noPublishedMatches(t, w)); err != nil {
		t.Fatal(err)
	}
	var cp analysis.RunCheckpoint
	if err := s.pool.QueryRow(ctx, "select checkpoint from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&cp); err != nil || len(cp.Queue) != 2 {
		t.Fatal(err)
	}
	head := cp.Queue[0]
	cp.Queue[0].Task.MaxObjects = r.Limits.Maximum.Objects + 1
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, "update ouf_mcp.analysis_run set checkpoint=$2 where analysis_id=$1", r.ID(), raw); err != nil {
		t.Fatal(err)
	}
	expectReservation := func(expected error) {
		t.Helper()
		current, e := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
		if e != nil {
			t.Fatal(e)
		}
		work, e := s.NextRunWork(ctx, current, uuid.NewString())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.ReserveRunWork(ctx, current, work); !errors.Is(e, expected) {
			t.Fatal(e)
		}
	}
	expectReservation(analysis.ErrFenced)
	w, c = claimFixture(t, ctx, s, r)
	if w.Intent == head.Intent {
		t.Fatal("independent group held")
	}
	job := uuid.NewString()
	receipt := RunReceipt{Decision: "fixture", Correlation: "fixture", Body: []byte(`{"requestId":"` + job + `"}`)}
	if err = s.FinishRunWork(ctx, c, w.Worker, nil, receipt); err != nil {
		t.Fatal(err)
	}
	w, c = claimFixture(t, ctx, s, r)
	receipt.Body = []byte(`{"state":"SUCCEEDED","completedCandidateCount":0,"requestId":"` + job + `"}`)
	if err = s.FinishRunWork(ctx, c, w.Worker, nil, receipt); err != nil {
		t.Fatal(err)
	}
	expectReservation(analysis.ErrRunExhausted)
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.CurrentRenewal(ctx, r)
	if err != nil || !slices.Equal(p.Remaining, head.Task.Columns) {
		t.Fatal(p.Remaining, err)
	}
	if _, err = s.DecideRenewal(ctx, thsHolder(r), proposalCommand(p, "CLOSE_PARTIAL"), fixtureRenewal{now: now}); err != nil {
		t.Fatal(err)
	}
	summary, err := s.RunSummary(ctx, r.Scope, r.Issuer, r.ID(), analysis.Position{Seq: r.Seq + 1, Last: -1, Limit: 18}, true)
	if err != nil || summary.State() != "PARTIAL" || summary.Counts.Zero != 17 || summary.Counts.Blocked != 1 || len(summary.Items) != 18 {
		t.Fatal(summary.Counts, err)
	}
}

func TestPartialCloseKeepsQuarantineScope(t *testing.T) { // MCP-55B-05
	ctx, s, r := fixtureRun(t)
	now := r.NotBefore.Add(time.Second)
	s.analysisClock = func() time.Time { return now }
	w, c := twoIntentBatch(t, ctx, s, r)
	if err := s.FinishRunWork(ctx, c, w.Worker, nil, noPublishedMatches(t, w)); err != nil {
		t.Fatal(err)
	}
	w, c = claimFixture(t, ctx, s, r)
	if err := s.HoldRunAttempt(ctx, c, w.Worker, "ANALYSIS_EFFECT_UNCERTAIN"); err != nil {
		t.Fatal(err)
	}
	current, err := s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	current = exhaustFixture(t, ctx, s, current)
	p, err := s.CurrentRenewal(ctx, current)
	if err != nil || slices.Contains(p.Remaining, w.Ordinals[0]) {
		t.Fatal("quarantine scope", err)
	}
	if _, err = s.DecideRenewal(ctx, thsHolder(r), proposalCommand(p, "CLOSE_PARTIAL"), fixtureRenewal{now: now}); err != nil {
		t.Fatal(err)
	}
	var intact bool
	err = s.pool.QueryRow(ctx, `select q.state='OPEN' and q.reason='ANALYSIS_EFFECT_UNCERTAIN' and a.finished_at is null
		and q.item->'Task'->'columns'=to_jsonb(a.ordinals) and r.state='BLOCKED'
		from ouf_mcp.analysis_group_quarantine q join ouf_mcp.analysis_run_attempt a using(analysis_id,intent_key)
		join ouf_mcp.analysis_run r using(analysis_id) where a.attempt_id=$1`, c.AttemptID).Scan(&intact)
	if err != nil || !intact {
		t.Fatal(intact, err)
	}
}

func TestRenewal021PredecessorIncident(t *testing.T) { // MCP-55B-02, D-080
	ctx, _, seed := fixtureRun(t)
	old := migrationFixtureBefore(t, "021_")
	s := &Store{pool: old.pool}
	r, err := s.BeginRun(ctx, seed.RunSpec)
	if err != nil {
		t.Fatal(err)
	}
	mandate, _ := json.Marshal(seed.Mandate)
	r, err = s.AttachMandate(ctx, r.RunSpec, mandate)
	if err != nil {
		t.Fatal(err)
	}
	incident := analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()})
	if _, err = old.pool.Exec(ctx, "update ouf_mcp.analysis_run set state='BLOCKED',reason='ANALYSIS_BUDGET_EXHAUSTED',seq=seq+1 where analysis_id=$1", r.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err = old.pool.Exec(ctx, "insert into ouf_mcp.analysis_run_incident values($1,$2,'OPEN')", r.ID(), incident); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, old.pool); err != nil {
		t.Fatal(err)
	}
	if err = s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	r, err = s.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.CurrentRenewal(ctx, r)
	if err != nil || p.Version != 1 || p.IncidentID != incident {
		t.Fatal(p, err)
	}
	// A 020 writer only records its budget session and the new run sequence.
	i, want := renewalCommand(r)
	want.ProposalHash = p.Hash
	want.Increment = p.Increment
	want.Reason = p.Reason
	if err = s.RenewRunBudget(ctx, i, want, fixtureRenewal{now: r.NotBefore}); err != nil {
		t.Fatal(err)
	}
	actual, err := s.ReadRenewal(ctx, thsHolder(r), p.ReviewRef)
	if err != nil || actual.State != "APPROVED" {
		t.Fatal(actual.State, err)
	}
}

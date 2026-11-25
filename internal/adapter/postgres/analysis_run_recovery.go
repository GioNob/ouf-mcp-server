package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/jackc/pgx/v5"
)

func (s *Store) exhaustRun(ctx context.Context, tx pgx.Tx, r analysis.RunRecord) error {
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, "update ouf_mcp.analysis_run set state='BLOCKED',reason='ANALYSIS_BUDGET_EXHAUSTED',seq=seq+1 where analysis_id=$1 returning seq", r.ID()).Scan(&r.Seq)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_run_incident(analysis_id,incident_id,state)
		values($1,$2,'OPEN') on conflict(analysis_id) do update set state='OPEN'`, r.ID(), analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()}))
	if err != nil || r.Mandate.ID == "" {
		return err
	}
	r.State, r.Reason = "BLOCKED", analysis.ErrRunExhausted.Error()
	_, err = ensureProposal(ctx, tx, r, now)
	if errors.Is(err, analysis.ErrScope) {
		return nil
	}
	return err
}

// Keep the smallest independently identifiable work item, its evidence
// and any ambiguous reservation under its original key after quarantine.
func quarantineRunWork(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, item analysis.RunItem, reason string, now time.Time) error {
	var raw []byte
	if err := tx.QueryRow(ctx, "select checkpoint from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&raw); err != nil {
		return err
	}
	var cp analysis.RunCheckpoint
	if json.Unmarshal(raw, &cp) != nil || len(cp.Queue) == 0 || cp.Queue[0].Intent != item.Intent {
		return analysis.ErrRunDrift
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_group_quarantine(analysis_id,intent_key,incident_id,item,opened_at,reason)
		values($1,$2,$3,$4,$5,$6) on conflict(analysis_id,intent_key) do nothing`, r.ID(), item.Intent, analysis.Digest([]string{analysis.RunBudgetProfile, r.ID(), item.Intent}), raw, now, reason)
	if err != nil {
		return err
	}
	before := append([]analysis.Outcome(nil), cp.Outcomes...)
	m, err := manifest.Load()
	if err != nil {
		return err
	}
	if err := cp.FailHead(r, m, reason, false); err != nil {
		return err
	}
	state := cp.Counts().State()
	if len(cp.Queue) > 0 {
		reason = "ANALYSIS_RUNNING"
	}
	return saveRunCheckpoint(ctx, tx, r, cp, before, state, reason, now, now)
}

// Keep the unresolved charge and owner slot. Isolating the intent never
// asserts that the remote mutation completed or frees its provider lease.
func (s *Store) isolateUncertainWork(ctx context.Context, c analysis.ReservationCheck, worker string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", c.AnalysisID))
	if err != nil {
		return err
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	var raw []byte
	err = tx.QueryRow(ctx, `select r.checkpoint->'Queue'->0 from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_attempt a using(analysis_id)
		where r.analysis_id=$1 and r.state='RUNNING' and r.fence=a.fence and a.attempt_id=$2 and a.worker=$3 and a.finished_at is null
		and a.fence::text=$4 and a.lease_end>$5`, r.ID(), c.AttemptID, worker, c.Fence, now).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.ErrFenced
	}
	if err != nil {
		return err
	}
	var item analysis.RunItem
	if json.Unmarshal(raw, &item) != nil || item.Intent != c.IntentKey {
		return analysis.ErrFenced
	}
	if err = quarantineRunWork(ctx, tx, r, item, "ANALYSIS_EFFECT_UNCERTAIN", now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// The verifier runs without a transaction. Recheck scope, version, pressure,
// outstanding effects and expiry under the same locks as first admission.
func (s *Store) RenewRunBudget(ctx context.Context, actor orchestration.Identity, want analysis.RunRenewal, authority analysis.RunRenewalAuthority) error {
	if authority == nil || actor.ActorType != "HUMAN" || actor.Delegation == "" {
		return orchestration.ErrUnauthorized
	}
	r, err := s.ReadRun(ctx, analysis.IdentityScope(actor), actor.Issuer, want.AnalysisID)
	if err != nil {
		return err
	}
	if r.ID() != want.AnalysisID || r.Scope != analysis.IdentityScope(actor) || r.Issuer != actor.Issuer {
		return analysis.ErrRunDrift
	}
	if strings.TrimSpace(want.Reason) == "" || len(want.Reason) > 256 || want.ExpectedSeq < 0 || want.ProposalVersion < 1 || analysis.ValidateID(want.ProposalHash) != nil ||
		strings.ToLower(want.ProposalHash) != want.ProposalHash || want.Increment == (analysis.RunCost{}) || !want.Increment.Fits(r.Limits.RenewalCeiling()) ||
		want.Seconds < 1 || want.Seconds > r.Limits.RenewalDuration() || want.ExpiresAt.IsZero() {
		return analysis.ErrInvalid
	}
	if want.IncidentID != analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()}) {
		return analysis.ErrFenced
	}
	want.ExpiresAt = want.ExpiresAt.UTC()
	check := analysis.RunRenewalCheck{RunRenewal: want, Contract: "ouf.analysis.authority-check.v1", Action: "renew-budget",
		Issuer: r.Issuer, Scope: r.Scope, Workload: r.Workload, MandateID: r.Mandate.ID, Generation: r.Mandate.Generation, Fence: r.Fence}
	hash, err := check.Hash()
	if err != nil {
		return err
	}
	decision, err := authority.AuthorizeRunRenewal(ctx, actor, check)
	if err != nil {
		return err
	}
	if decision.RequestHash != hash || strings.TrimSpace(decision.DecisionRef) == "" || len(decision.DecisionRef) > 256 ||
		strings.TrimSpace(decision.GrantRef) == "" || len(decision.GrantRef) > 256 {
		return orchestration.ErrUnauthorized
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockRunAdmission(ctx, tx, r.Installation(), r.Issuer, r.Scope.Tenant, r.ID()); err != nil {
		return err
	}
	current, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", r.ID()))
	if err != nil {
		return err
	}
	if err = s.renewRunBudgetTx(ctx, tx, r, current, check, decision); err != nil {
		return persistRenewalWait(ctx, tx, current, err)
	}
	return tx.Commit(ctx)
}

// Both the THS command and legacy caller use the 55a admission and consumption gates.
func (s *Store) renewRunBudgetTx(ctx context.Context, tx pgx.Tx, r, current analysis.RunRecord, check analysis.RunRenewalCheck, decision analysis.RenewalDecision) error {
	want := check.RunRenewal
	if want.Increment == (analysis.RunCost{}) || !want.Increment.Fits(current.Limits.RenewalCeiling()) || want.Seconds < 1 || want.Seconds > current.Limits.RenewalDuration() {
		return analysis.ErrInvalid
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	if current.MandateState == "EXPIRED" || !current.Mandate.Expires.After(now) {
		return analysis.ErrMandateExpired
	}
	if current.MandateState != "ACTIVE" || !decision.ValidUntil.After(now) {
		return orchestration.ErrUnauthorized
	}
	if current.RunSpec != r.RunSpec || current.Mandate.ID != check.MandateID || current.Mandate.Generation != check.Generation {
		return analysis.ErrFenced
	}
	var used bool
	err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_budget_session
		where analysis_id=$1 and incident_id=$2 and renewal_used)`, r.ID(), want.IncidentID).Scan(&used)
	if err != nil {
		return err
	}
	if used {
		return analysis.ErrRenewalUsed
	}
	if r.Seq != want.ExpectedSeq || current.Seq != want.ExpectedSeq || current.Fence != check.Fence ||
		current.State != "BLOCKED" || current.Reason != "ANALYSIS_BUDGET_EXHAUSTED" {
		return analysis.ErrFenced
	}
	if current.NotBefore.After(now) {
		return runAdmissionBackoff(now, current.NotBefore)
	}
	var busy bool
	err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_attempt where analysis_id=$1 and (proof_until>$2 or (finished_at is null and superseded_at is null)))
		or exists(select 1 from ouf_mcp.analysis_group_quarantine where analysis_id=$1 and reason='ANALYSIS_EFFECT_UNCERTAIN' and state<>'RESOLVED')`, r.ID(), now).Scan(&busy)
	if err != nil {
		return err
	}
	if busy {
		return analysis.ErrFenced
	}
	if err = admitRun(ctx, tx, current.RunSpec, now); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_run_incident set state='RECOVERING'
		where analysis_id=$1 and incident_id=$2 and state='OPEN'`, r.ID(), want.IncidentID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	end := want.SessionEnd(current, now)
	if !end.After(now) {
		return analysis.ErrRunExhausted
	}
	if err = insertRenewalSession(ctx, tx, current, want, decision, now, end); err != nil {
		return err
	}
	tag, err = tx.Exec(ctx, `update ouf_mcp.analysis_run set budget_end=$4,poll_seq=null,poll_after=null,status_due=$3,status_valid_until=null,status_token=null,status_failures=0,
		seq=seq+1,state='RUNNING',reason='ANALYSIS_RUNNING',
		not_before=greatest($3::timestamptz,(checkpoint #>> '{Queue,0,Task,notBefore}')::timestamptz)
		where analysis_id=$1 and seq=$2`, r.ID(), want.ExpectedSeq, now, end)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	now, err = s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	if !decision.ValidUntil.After(now) {
		return orchestration.ErrUnauthorized
	}
	if !current.Mandate.Expires.After(now) {
		return analysis.ErrMandateExpired
	}
	if !end.After(now) {
		return analysis.ErrRunExhausted
	}
	return nil
}

func insertRenewalSession(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, want analysis.RunRenewal, decision analysis.RenewalDecision, now, end time.Time) error {
	spent, err := runSpend(ctx, tx, r.ID(), 0)
	if err != nil {
		return err
	}
	initial := r.Limits.Maximum
	if !spent.Fits(initial) {
		return analysis.ErrRunDrift
	}
	total := initial.Add(want.Increment)
	// 019 counts only post-fence attempts and accepts at most the initial
	// maximum per session. Give it the remaining allowance it can represent.
	remaining := analysis.RunCost{
		Calls:    min(initial.Calls, total.Calls-spent.Calls),
		Attempts: min(initial.Attempts, total.Attempts-spent.Attempts),
		Objects:  min(initial.Objects, total.Objects-spent.Objects),
		Bytes:    min(initial.Bytes, total.Bytes-spent.Bytes),
		Millis:   min(initial.Millis, total.Millis-spent.Millis),
	}
	legacyMaximum, _ := json.Marshal(remaining)
	increment, _ := json.Marshal(want.Increment)
	tag, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_run_budget_session(analysis_id,incident_id,expected_seq,actor,reason,decision_ref,maximum,
		started,ends_at,first_fence,request_hash,grant_ref,proposal_version,proposal_hash,increment)
		select analysis_id,$2,seq,$4,$5,$6,$7,$8,$9,fence,$10,$11,$12,$13,$14 from ouf_mcp.analysis_run where analysis_id=$1 and seq=$3
		on conflict (analysis_id,incident_id) where renewal_used do nothing`,
		r.ID(), want.IncidentID, want.ExpectedSeq, r.Scope.Principal, want.Reason, decision.DecisionRef, legacyMaximum, now, end,
		decision.RequestHash, decision.GrantRef, want.ProposalVersion, want.ProposalHash, increment)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return analysis.ErrRenewalUsed
	}
	return nil
}

func runSession(ctx context.Context, tx pgx.Tx, r analysis.RunRecord) (analysis.RunCost, int64, error) {
	maximum := r.Limits.Maximum
	var since int64
	var raw []byte
	var cumulative bool
	err := tx.QueryRow(ctx, `select coalesce(increment,maximum),first_fence,increment is not null from ouf_mcp.analysis_run_budget_session
		where analysis_id=$1 order by expected_seq desc limit 1`, r.ID()).Scan(&raw, &since, &cumulative)
	if errors.Is(err, pgx.ErrNoRows) {
		return maximum, 0, nil
	}
	if err != nil {
		return maximum, 0, err
	}
	if err = json.Unmarshal(raw, &maximum); err != nil || !maximum.Fits(r.Limits.Maximum) {
		return maximum, 0, analysis.ErrRunDrift
	}
	if cumulative {
		if maximum == (analysis.RunCost{}) || !maximum.Fits(r.Limits.RenewalCeiling()) {
			return maximum, 0, analysis.ErrRunDrift
		}
		return r.Limits.Maximum.Add(maximum), 0, nil
	}
	return maximum, since, nil // 019 admitted an independent post-fence session.
}

func runSpend(ctx context.Context, tx pgx.Tx, id string, firstFence int64) (analysis.RunCost, error) {
	var spent analysis.RunCost
	err := tx.QueryRow(ctx, `select coalesce(sum(coalesce((actual->>'Calls')::bigint,calls)),0),
		coalesce(sum(attempts),0), coalesce(sum(coalesce((actual->>'Objects')::bigint,objects)),0),
		coalesce(sum(coalesce((actual->>'Bytes')::bigint,bytes)),0),coalesce(sum(coalesce((actual->>'Millis')::bigint,millis)),0)
		from ouf_mcp.analysis_run_attempt where analysis_id=$1 and fence>$2`, id, firstFence).Scan(&spent.Calls, &spent.Attempts, &spent.Objects, &spent.Bytes, &spent.Millis)
	return spent, err
}

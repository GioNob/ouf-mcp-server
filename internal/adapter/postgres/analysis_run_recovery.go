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

func exhaustRun(ctx context.Context, tx pgx.Tx, r analysis.RunRecord) error {
	_, err := tx.Exec(ctx, "update ouf_mcp.analysis_run set state='BLOCKED',reason='ANALYSIS_BUDGET_EXHAUSTED',seq=seq+1 where analysis_id=$1", r.ID())
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_run_incident(analysis_id,incident_id,state)
		values($1,$2,'OPEN') on conflict(analysis_id) do update set state='OPEN'`, r.ID(), analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()}))
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
	if authority == nil || actor.ActorType != "HUMAN" || actor.Delegation == "" || strings.TrimSpace(want.Reason) == "" || len(want.Reason) > 256 {
		return orchestration.ErrUnauthorized
	}
	r, err := s.ReadRun(ctx, analysis.IdentityScope(actor), actor.Issuer, want.AnalysisID)
	if err != nil {
		return err
	}
	if !(analysis.RunCost{Calls: 1, Attempts: 1, Objects: 1, Bytes: 1, Millis: 1}).Fits(want.Maximum) || !want.Maximum.Fits(r.Limits.Maximum) ||
		want.Seconds < 1 || want.Seconds > r.Limits.LifetimeSeconds || want.ExpectedSeq != r.Seq || r.Reason != "ANALYSIS_BUDGET_EXHAUSTED" ||
		want.IncidentID != analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()}) {
		return analysis.ErrFenced
	}
	decision, err := authority.AuthorizeRunRenewal(ctx, actor, r, want)
	if err != nil {
		return err
	}
	if decision.Ref == "" || len(decision.Ref) > 256 {
		return orchestration.ErrUnauthorized
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5018))", analysis.Digest([]string{r.Issuer, r.Scope.Tenant}))
	if err != nil {
		return err
	}
	current, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", r.ID()))
	if err != nil {
		return err
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	if current.Seq != want.ExpectedSeq || current.MandateState != "ACTIVE" || current.State != "BLOCKED" || current.Reason != "ANALYSIS_BUDGET_EXHAUSTED" || !r.Mandate.Expires.After(now) ||
		!decision.ValidUntil.After(now) || current.RunSpec != r.RunSpec {
		return analysis.ErrFenced
	}
	var busy bool
	err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_attempt where analysis_id=$1 and (proof_until>$2 or (finished_at is null and superseded_at is null)))`, r.ID(), now).Scan(&busy)
	if err != nil {
		return err
	}
	if busy {
		return analysis.ErrFenced
	}
	var plans, tenantPlans, sourcePlans int
	err = tx.QueryRow(ctx, `select count(*) filter(where subject=$3),count(*),count(*) filter(where spec->'Profile'->>'assetId'=$4)
		from ouf_mcp.analysis_run where issuer=$1 and tenant=$2 and state in ('ISSUING','RUNNING') and budget_end>$5`,
		r.Issuer, r.Scope.Tenant, r.Scope.Principal, r.Profile.AssetID, now).Scan(&plans, &tenantPlans, &sourcePlans)
	if err != nil {
		return err
	}
	if plans >= r.Limits.ActivePlans || tenantPlans >= r.Limits.Shared.TenantQueue || sourcePlans >= r.Limits.Shared.SourceQueue {
		return analysis.ErrRunPressure
	}
	maximum, _ := json.Marshal(want.Maximum)
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_run_budget_session(analysis_id,incident_id,expected_seq,actor,reason,decision_ref,maximum,started,ends_at,first_fence)
		select $1,$2,$3,$4,$5,$6,$7,$8,least($8::timestamptz+$9::int*interval '1 second',$10),(select fence from ouf_mcp.analysis_run where analysis_id=$1)
		from ouf_mcp.analysis_run_incident where analysis_id=$1 and incident_id=$2 and state='OPEN'`,
		r.ID(), want.IncidentID, want.ExpectedSeq, actor.PrincipalID, want.Reason, decision.Ref, maximum, now, want.Seconds, r.Mandate.Expires)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_run r set budget_end=s.ends_at,status_due=$3,status_valid_until=null,status_token=null,status_failures=0,seq=r.seq+1,state='RUNNING',reason='ANALYSIS_RUNNING',not_before=$3
		from ouf_mcp.analysis_run_budget_session s where r.analysis_id=$1 and s.analysis_id=r.analysis_id and s.expected_seq=$2 and r.seq=$2`, r.ID(), want.ExpectedSeq, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run_incident set state='RECOVERING' where analysis_id=$1", r.ID())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func runSession(ctx context.Context, tx pgx.Tx, r analysis.RunRecord) (analysis.RunCost, int64, error) {
	maximum := r.Limits.Maximum
	var since int64
	var raw []byte
	err := tx.QueryRow(ctx, `select maximum,first_fence from ouf_mcp.analysis_run_budget_session
		where analysis_id=$1 order by expected_seq desc limit 1`, r.ID()).Scan(&raw, &since)
	if errors.Is(err, pgx.ErrNoRows) {
		return maximum, 0, nil
	}
	if err != nil {
		return maximum, 0, err
	}
	if err = json.Unmarshal(raw, &maximum); err != nil || !maximum.Fits(r.Limits.Maximum) {
		return maximum, 0, analysis.ErrRunDrift
	}
	return maximum, since, nil
}

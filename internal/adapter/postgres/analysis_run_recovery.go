package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
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
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_run r set budget_end=s.ends_at,seq=r.seq+1,state='RUNNING',reason='ANALYSIS_RUNNING',not_before=$3
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

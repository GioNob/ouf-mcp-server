package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/jackc/pgx/v5"
)

func (s *Store) BatchDecisionAttempt(ctx context.Context, scope analysis.Scope, fence, stage string, args []byte) (orchestration.Result, bool, error) {
	var result orchestration.Result
	var capability string
	switch stage {
	case analysis.BatchAdopt:
		capability = "ouf.semantic.discovery.adopt"
	case analysis.BatchPropose:
		capability = "ouf.semantic.propose"
	default:
		return result, false, analysis.ErrInvalid
	}
	var due *time.Time
	var dispatch, code, hash string
	err := s.pool.QueryRow(ctx, `select a.state,a.dispatch_state,coalesce(a.outcome_code,''),
		a.owner_due,a.owner_due_reason,a.owner_code,a.owner_status,c.request_hash
		from ouf_mcp.idempotency_claim c join ouf_mcp.tool_attempt a on a.attempt_id=c.attempt_id
		where c.service_principal_id=$1 and c.principal_id=$2 and c.tenant_id=$3 and c.owner='semantic'
		and c.capability_id=$4 and c.operation_class='COMMAND' and c.idempotency_key=$5`,
		scope.Service, scope.Principal, scope.Tenant, capability, "analysis-batch-effect:"+fence).Scan(
		&result.AttemptState, &dispatch, &code, &due, &result.Observation.Reason,
		&result.Observation.Code, &result.Observation.Status, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	want := sha256.Sum256(args)
	if hash != hex.EncodeToString(want[:]) {
		return result, false, analysis.ErrConflict
	}
	if due != nil {
		result.Observation.Due = *due
	}
	if stage == analysis.BatchAdopt && result.AttemptState == "FAILED" &&
		orchestration.AdoptionRefusal(result.Observation.Status, code) != "" {
		result.AdoptionRefusalCode = code
	}
	return result, dispatch != "NOT_DISPATCHED", nil
}

func finishBatchDecisionEffect(ctx context.Context, tx pgx.Tx, job analysis.BatchWork, r analysis.BatchWorkResult, now time.Time) (int, error) {
	if r.EffectState == "" && r.Outcome.State == "BLOCKED" {
		r.EffectState, r.EffectCode = "HELD", r.Outcome.Reason
	}
	var changed int
	err := tx.QueryRow(ctx, `with finished as (
		update ouf_mcp.analysis_batch_column b set state=$5,due_at=$6,fence=null,
			retry_attempts=$7,retry_started_at=$8,
			dispatch_fence=case when $5='PENDING' then b.dispatch_fence end,
			decision_reason=case when $9='REFUSED' then $10 else b.decision_reason end,
			refused_candidates=case when $9='REFUSED' and d.candidate_id is not null
				then array_append(b.refused_candidates,d.candidate_id::text) else b.refused_candidates end
		from ouf_mcp.analysis_batch_choice d
		where b.analysis_id=$1 and b.ordinal=$2 and b.fence=$3 and b.due_at>$4 and b.state='RUNNING' and b.work_stage=$12
		and d.analysis_id=b.analysis_id and d.ordinal=b.ordinal and d.command_key=b.choice_key and d.effect_state='PENDING'
		and not (select stopped or integrity_hold from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1)
		returning b.choice_key
	), updated as (
		update ouf_mcp.analysis_batch_choice d set effect_state=$9,effect_code=nullif($10,''),
			owner_ref=coalesce(nullif($11,'')::uuid,d.owner_ref)
		from finished f where d.analysis_id=$1 and d.ordinal=$2 and d.command_key=f.choice_key
	), held as (
		insert into ouf_mcp.analysis_batch_hold(analysis_id,ordinal,work_stage,fence,reason_code,retry_started_at,retry_attempts,opened_at)
		select $1,$2,$12,$3,$10,$8,$7,$4 from finished where $9='HELD' on conflict do nothing
	) select count(*) from finished`, job.AnalysisID, r.Ordinal, job.Fence, now, r.State, batchTimestamp(r.Due),
		r.RetryAttempts, batchTimestamp(r.RetryStarted), r.EffectState, r.EffectCode, r.EffectRef, job.Stage).Scan(&changed)
	return changed, err
}

func finishBatchReformulation(ctx context.Context, tx pgx.Tx, job analysis.BatchWork, r analysis.BatchWorkResult) error {
	switch {
	case r.State == "CLOSED":
		return checkpointBatchReformulation(ctx, tx, job.AnalysisID, r.Ordinal, "HELD", r.Outcome.Reason, "")
	case r.Stage == analysis.BatchStatus || r.Stage == analysis.BatchCandidates:
		return checkpointBatchReformulation(ctx, tx, job.AnalysisID, r.Ordinal, "APPLIED", "REQUEST_RECORDED", r.Outcome.RequestID)
	default:
		return nil
	}
}

func checkpointBatchReformulation(ctx context.Context, tx pgx.Tx, id string, ordinal int, state, code, ref string) error {
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_batch_choice d set effect_state=$3,effect_code=$4,owner_ref=nullif($5,'')::uuid
		from ouf_mcp.analysis_batch_column b where b.analysis_id=$1 and b.ordinal=$2 and d.analysis_id=b.analysis_id
		and d.ordinal=b.ordinal and d.command_key=b.choice_key and d.action='REFORMULATE' and d.effect_state='PENDING'`,
		id, ordinal, state, code, ref)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrConflict
	}
	return err
}

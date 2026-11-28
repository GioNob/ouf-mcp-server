package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The selected ordinal and any other ordinals in the published lookup share
// one lease. Only the claim transaction holds the global admission mutex.
const batchWorkerAdmissionLock int64 = 0x4f55464241544348

const extendBatchRoutePause = `insert into ouf_mcp.analysis_batch_route_pause(work_stage,due_at) values($1,$2)
	on conflict(work_stage) do update set due_at=greatest(analysis_batch_route_pause.due_at,excluded.due_at)`

const batchClaimHead = `with available as materialized (
	select route.work_stage from (values ('PUBLISHED','PUBLISHED'),('REQUEST','REQUEST'),('STATUS','STATUS'),
		('CANDIDATES','CANDIDATES'),('REFORMULATE','REQUEST'),('ADOPT','ADOPT'),('PROPOSE','PROPOSE'),('DRAFT','DRAFT')) route(work_stage,owner_route)
	where ($3=0 or ($3=1 and route.work_stage in ('PUBLISHED','REQUEST','STATUS','CANDIDATES'))
		or ($3=2 and route.work_stage in ('ADOPT','PROPOSE','DRAFT')))
	and ($4 or not exists(select from ouf_mcp.analysis_batch_route_pause p where p.work_stage=route.owner_route and p.due_at>$2))
	and not exists(select from ouf_mcp.analysis_batch_column live
		where live.state='RUNNING' and live.fence is not null and live.due_at>$2
		and case when live.work_stage='REFORMULATE' then 'REQUEST' else live.work_stage end=route.owner_route)
)
select b.ordinal,b.state,b.work_stage,b.published_solo,coalesce(b.dispatch_fence::text,''),b.retry_attempts,b.retry_started_at
from available r cross join lateral (
	select b.* from ouf_mcp.analysis_batch_column b
	where b.analysis_id=$1 and b.work_stage=r.work_stage and b.state in ('PENDING','RUNNING') and b.due_at<=$2
	and ($3<>1 or $4 or b.mandate_not_before is null or b.mandate_not_before<=$2)
	and ($3<>1 or b.choice_key is null)
	order by b.due_at,b.ordinal limit 1 for update of b skip locked
) b order by b.due_at,b.ordinal limit 1`

func (s *Store) ClaimBatchWork(ctx context.Context, scope analysis.Scope, id, checksum, authority string, now time.Time, limit int) (analysis.BatchWork, error) {
	return s.claimBatchWork(ctx, scope, id, checksum, authority, now, limit, 0)
}

func (s *Store) ClaimMandatedBatch(ctx context.Context, scope analysis.Scope, id, checksum string, now time.Time, limit int) (analysis.BatchWork, error) {
	return s.claimBatchWork(ctx, scope, id, checksum, "", now, limit, 1)
}

func (s *Store) ClaimBatchDecisionWork(ctx context.Context, scope analysis.Scope, id, checksum, authority string, now time.Time, limit int) (analysis.BatchWork, error) {
	return s.claimBatchWork(ctx, scope, id, checksum, authority, now, limit, 2)
}

func (s *Store) claimBatchWork(ctx context.Context, scope analysis.Scope, id, checksum, authority string, now time.Time, limit, rail int) (analysis.BatchWork, error) {
	if limit < 1 || limit > 64 {
		return analysis.BatchWork{}, analysis.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return analysis.BatchWork{}, err
	}
	defer tx.Rollback(ctx)
	start, err := readBatchStart(ctx, tx, scope, id, checksum)
	if err != nil || !start.Initialized {
		return analysis.BatchWork{}, err
	}
	if rail != 1 && start.Authority != "" && start.Authority != authority {
		return analysis.BatchWork{}, analysis.ErrScope
	}
	if rail == 1 && (start.Mandate.State == "" || start.Mandate.State == "PENDING") ||
		rail == 0 && start.Mandate.StartRef != "" {
		return analysis.BatchWork{}, nil
	}
	var held, locked bool
	err = tx.QueryRow(ctx, `select stopped or integrity_hold from ouf_mcp.analysis_counts
		where analysis_id=$1 order by seq desc limit 1`, id).Scan(&held)
	if err != nil || held {
		return analysis.BatchWork{}, err
	}
	if err = tx.QueryRow(ctx, `select pg_try_advisory_xact_lock($1)`, batchWorkerAdmissionLock).Scan(&locked); err != nil || !locked {
		return analysis.BatchWork{}, err
	}
	var active int
	var tenantActive bool
	err = tx.QueryRow(ctx, `select count(distinct b.fence),coalesce(bool_or(p.tenant_id=$2),false)
		from ouf_mcp.analysis_batch_column b join ouf_mcp.profile_analysis p using(analysis_id)
		where b.state='RUNNING' and b.fence is not null and b.due_at>$1`, now, scope.Tenant).Scan(&active, &tenantActive)
	if err != nil || active >= 2 || tenantActive {
		return analysis.BatchWork{}, err
	}
	work := analysis.BatchWork{AnalysisID: id, Epoch: start.Epoch, Profile: start.Profile, Mandate: start.Mandate,
		Fence: uuid.NewString(), LeaseUntil: now.Add(analysis.BatchLease)}
	if rail == 1 && start.Mandate.State == "ACTIVE" {
		err = tx.QueryRow(ctx, `select exists(select from ouf_mcp.analysis_batch_column
			where analysis_id=$1 and choice_key is null and state in ('PENDING','RUNNING')
			and (retry_attempts>0 or mandate_not_before is not null or (state='RUNNING' and transport_fence=fence)))`, id).
			Scan(&work.CheckMandate)
		if err != nil {
			return analysis.BatchWork{}, err
		}
	}
	var ordinal, attempts int
	var state string
	var solo bool
	var started *time.Time
	err = tx.QueryRow(ctx, batchClaimHead, id, now, rail, rail == 1 && start.Mandate.State != "ACTIVE").
		Scan(&ordinal, &state, &work.Stage, &solo, &work.PreviousFence, &attempts, &started)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.BatchWork{}, nil
	}
	if err != nil {
		return analysis.BatchWork{}, err
	}
	ordinals := []int{ordinal}
	if work.Stage == analysis.BatchPublished && state == "PENDING" && !solo && limit > 1 {
		rows, err := tx.Query(ctx, `select ordinal from ouf_mcp.analysis_batch_column
			where analysis_id=$1 and state='PENDING' and work_stage='PUBLISHED' and not published_solo
			and due_at<=$2 and ordinal<>$3 and retry_attempts=$4 and retry_started_at is not distinct from $5
			and ($7<>1 or choice_key is null)
			order by due_at,ordinal limit $6 for update skip locked`, id, now, ordinal, attempts, started, limit-1, rail)
		if err != nil {
			return analysis.BatchWork{}, err
		}
		more, err := pgx.CollectRows(rows, pgx.RowTo[int])
		if err != nil {
			return analysis.BatchWork{}, err
		}
		ordinals = append(ordinals, more...)
	}
	rows, err := tx.Query(ctx, `with claimed as (
		update ouf_mcp.analysis_batch_column set state='RUNNING',due_at=$3,fence=$4
		where analysis_id=$1 and ordinal=any($2::int[])
		returning ordinal,intent,retry_attempts,retry_started_at,owner_deadline,completed_count,choice_key,discovery_arguments_hash
	) select b.ordinal,b.intent,b.retry_attempts,b.retry_started_at,b.owner_deadline,b.completed_count,coalesce(b.discovery_arguments_hash::text,''),
		coalesce(c.request_id::text,''),coalesce(d.effect_key::text,''),d.effect_arguments,coalesce(d.requested_revision_id::text,'')
		from claimed b join ouf_mcp.analysis_column c on c.analysis_id=$1 and c.ordinal=b.ordinal
		left join ouf_mcp.analysis_batch_choice d on d.analysis_id=$1 and d.ordinal=b.ordinal and d.command_key=b.choice_key
		order by b.ordinal`, id, ordinals, work.LeaseUntil, work.Fence)
	if err != nil {
		return analysis.BatchWork{}, err
	}
	work.Columns, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (analysis.BatchWorkColumn, error) {
		var col analysis.BatchWorkColumn
		var retryStarted, ownerEnd *time.Time
		err := row.Scan(&col.Ordinal, &col.Intent, &col.RetryAttempts, &retryStarted, &ownerEnd, &col.CompletedCount, &col.DiscoveryHash,
			&col.RequestID, &col.EffectKey, &col.EffectArguments, &col.EffectRevision)
		if retryStarted != nil {
			col.RetryStarted = *retryStarted
		}
		if ownerEnd != nil {
			col.OwnerEnd = *ownerEnd
		}
		return col, err
	})
	if err == nil && len(work.Columns) != len(ordinals) {
		err = analysis.ErrConflict
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	return work, err
}

func (s *Store) BatchDiscoveryReceipt(ctx context.Context, scope analysis.Scope, fence string, args []byte) (orchestration.Result, error) {
	var result orchestration.Result
	var due *time.Time
	var storedHash string
	err := s.pool.QueryRow(ctx, `select a.state,a.replay_result_id,a.owner_due,a.owner_due_reason,a.owner_code,a.owner_status,c.request_hash
		from ouf_mcp.idempotency_claim c join ouf_mcp.tool_attempt a on a.attempt_id=c.attempt_id
		where c.service_principal_id=$1 and c.principal_id=$2 and c.tenant_id=$3 and c.owner='semantic'
		and c.capability_id='ouf.semantic.discovery' and c.operation_class='COMMAND' and c.idempotency_key=$4 and c.batch_discovery_worker`,
		scope.Service, scope.Principal, scope.Tenant, "analysis-batch:"+fence).Scan(&result.AttemptState, &result.ResultID,
		&due, &result.Observation.Reason, &result.Observation.Code, &result.Observation.Status, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	if due != nil {
		result.Observation.Due = *due
	}
	if err == nil {
		want := sha256.Sum256(args)
		if storedHash != hex.EncodeToString(want[:]) {
			err = analysis.ErrConflict
		}
	}
	return result, err
}

// Reserve the transport fence before the call. Mandated dispatch is accounted
// for after the Gateway outcome, since the Gateway performs admission.
func (s *Store) BeginBatchDispatch(ctx context.Context, job analysis.BatchWork, ordinals []int, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_batch_column b
		set retry_attempts=retry_attempts+case when $8='' then 1 else 0 end,
		retry_started_at=case when $8='' then coalesce(retry_started_at,$4) else retry_started_at end,
		dispatch_fence=case when $6 then b.dispatch_fence else $2 end,reconcile_fence=$2,
		mandate_arguments_hash=nullif($8::text,''),
		discovery_arguments_hash=case when $8<>'' and $5='REQUEST' then $8 else b.discovery_arguments_hash end,
		mandate_not_before=case when $8<>'' and $5 in ('STATUS','CANDIDATES') then $4+interval '31 seconds' end,
		transport_fence=case when $8<>'' then $2 else b.transport_fence end
		where b.analysis_id=$1 and b.fence=$2 and b.ordinal=any($3::int[]) and b.state='RUNNING' and b.due_at>$4
		and b.work_stage=$5 and b.reconcile_fence is distinct from b.fence and b.dispatch_fence is distinct from b.fence
		and b.retry_attempts<128 and (b.retry_started_at is null or b.retry_started_at+interval '24 hours'>$4)
		and not exists(select from ouf_mcp.analysis_batch_route_pause p where p.work_stage=$7 and p.due_at>$4)
		and ($8='' or (b.choice_key is null and exists(select from ouf_mcp.analysis_batch_mandate m
			where m.analysis_id=$1 and m.state='ACTIVE' and m.mandate_ref=nullif($9,'')::uuid and m.generation=$10)))
		and ($8='' or $5<>'REQUEST' or b.discovery_arguments_hash is null or b.discovery_arguments_hash=$8)
		and not (select stopped or integrity_hold from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1)`,
		job.AnalysisID, job.Fence, ordinals, now, job.Stage, job.ReconcileOnly, job.Route(),
		job.DispatchHash, job.Mandate.Ref, job.Mandate.Generation)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ordinals)) {
		return analysis.ErrConflict
	}
	return tx.Commit(ctx)
}

// Consume the lease's transport slot and mark the attempt dispatched in the
// same transaction. Replacement claims lock the same checkpoint rows.
func (s *Store) dispatchBatchAttempt(ctx context.Context, batch analysis.BatchDispatch, attempt uuid.UUID, backend string, lease time.Duration, expected int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `with owned as materialized (
		select ordinal from ouf_mcp.analysis_batch_column
		where analysis_id=$1 and ordinal=any($2::int[]) and fence=$3 and reconcile_fence=$3
		and work_stage=$4 and state='RUNNING' and transport_fence is distinct from fence
		order by ordinal for no key update
	) update ouf_mcp.analysis_batch_column b set transport_fence=$3
	from owned where b.analysis_id=$1 and b.ordinal=owned.ordinal and b.due_at>clock_timestamp()
	and not (select stopped or integrity_hold from ouf_mcp.analysis_counts
		where analysis_id=$1 order by seq desc limit 1)`,
		batch.AnalysisID, batch.Ordinals, batch.Fence, batch.Stage)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 || tag.RowsAffected() != int64(len(batch.Ordinals)) {
		return analysis.ErrConflict
	}
	tag, err = tx.Exec(ctx, markRunningSQL, attempt, backend, lease.String(), expected)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return analysis.ErrConflict
	}
	return tx.Commit(ctx)
}

type batchWorkRow struct {
	Ordinal          int                       `json:"ordinal"`
	Stage            string                    `json:"stage"`
	State            string                    `json:"state"`
	Due              *time.Time                `json:"due"`
	OwnerEnd         *time.Time                `json:"owner_end"`
	RetryStarted     *time.Time                `json:"retry_started"`
	RetryAttempts    int                       `json:"retry_attempts"`
	Outcome          analysis.Outcome          `json:"outcome"`
	Candidates       []analysis.BatchCandidate `json:"candidates,omitempty"`
	EvidenceVersion  string                    `json:"evidence_version"`
	CompletedCount   *int                      `json:"completed_count"`
	PublishedSolo    bool                      `json:"published_solo"`
	DiscoveryBinding string                    `json:"discovery_binding"`
	CheckMandate     bool                      `json:"check_mandate"`
}

func batchTimestamp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	// PostgreSQL rounds JSON timestamps, while pgx truncates binary parameters
	// such as the claim clock. Use the same precision on both paths.
	t = t.Truncate(time.Microsecond)
	return &t
}

func (s *Store) FinishBatchWork(ctx context.Context, scope analysis.Scope, job analysis.BatchWork, results []analysis.BatchWorkResult, pause, now time.Time) error {
	if len(results) != len(job.Columns) || len(results) == 0 {
		return analysis.ErrInvalid
	}
	data := make([]batchWorkRow, 0, len(results))
	for _, result := range results {
		data = append(data, batchWorkRow{result.Ordinal, result.Stage, result.State, batchTimestamp(result.Due),
			batchTimestamp(result.OwnerEnd), batchTimestamp(result.RetryStarted), result.RetryAttempts, result.Outcome,
			result.Candidates, result.EvidenceVersion, result.CompletedCount, result.PublishedSolo, result.DiscoveryBinding, result.CheckMandate})
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockBatchAnalysis(ctx, tx, scope, job.AnalysisID); err != nil {
		return err
	}
	var changed int
	switch job.Stage {
	case analysis.BatchAdopt, analysis.BatchPropose, analysis.BatchDraft:
		changed, err = finishBatchDecisionEffect(ctx, tx, job, results[0], now)
	default:
		err = tx.QueryRow(ctx, finishBatchWorkSQL, job.AnalysisID, job.Fence, now, raw).Scan(&changed)
		if err == nil && changed == 1 && job.Stage == analysis.BatchReformulate {
			err = finishBatchReformulation(ctx, tx, job, results[0])
		}
	}
	if err != nil {
		return err
	}
	if changed != len(results) {
		return analysis.ErrConflict
	}
	mandated := job.Mandate.Ref != "" && (job.Stage == analysis.BatchPublished || job.Stage == analysis.BatchRequest ||
		job.Stage == analysis.BatchStatus || job.Stage == analysis.BatchCandidates)
	if mandated && !pause.IsZero() {
		// The route pause was written at the observation checkpoint.
		pause = time.Time{}
	}
	if mandated {
		for _, result := range results {
			code := result.Outcome.Reason
			var terminal string
			switch code {
			case "MANDATE_REVOKED":
				terminal = "REVOKED"
			case "MANDATE_EXPIRED", "ANALYSIS_MANDATE_EXPIRED":
				terminal = "EXPIRED"
			case "MANDATE_VERSION_CHANGED", "MANDATE_GENERATION_CHANGED", "MANDATE_NOT_FOUND", "CURRENT_RIGHTS_DENIED",
				"RIGHTS_CHANGED", "POLICY_CHANGED", "POLICY_EPOCH_CHANGED", "ANALYSIS_POLICY_EPOCH_CHANGED", "PRINCIPAL_INACTIVE":
				terminal = "HELD"
			}
			if terminal == "" {
				continue
			}
			_, err = tx.Exec(ctx, `with stopped as (
				update ouf_mcp.analysis_batch_mandate set state=$4,reason_code=$5,control_due=null,control_fence=null
				where analysis_id=$1 and mandate_ref=$2 and generation=$3 and state='ACTIVE' returning analysis_id
			) update ouf_mcp.analysis_batch_column b set due_at=least(b.due_at,$6) from stopped
				where b.analysis_id=stopped.analysis_id and b.choice_key is null and b.state='PENDING'`,
				job.AnalysisID, job.Mandate.Ref, job.Mandate.Generation, terminal, code, now)
			if err != nil {
				return err
			}
			break
		}
	}
	if !pause.IsZero() {
		if _, err = tx.Exec(ctx, extendBatchRoutePause, job.Route(), pause); err != nil {
			return err
		}
	}
	for _, result := range results {
		if result.TrafficMetric == "" {
			continue
		}
		_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_batch_traffic(work_stage,metric,observations) values($1,$2,1)
			on conflict(work_stage,metric) do update set observations=analysis_batch_traffic.observations+1`, job.Route(), result.TrafficMetric)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const finishBatchWorkSQL = `with head as materialized (
	select * from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1
), input as materialized (
	select * from jsonb_to_recordset($4::jsonb) as v(ordinal int,stage text,state text,due timestamptz,owner_end timestamptz,
		retry_started timestamptz,retry_attempts int,outcome jsonb,candidates jsonb,evidence_version text,completed_count int,published_solo boolean,
		discovery_binding text,check_mandate boolean)
), finished as (
	update ouf_mcp.analysis_batch_column b set state=v.state,work_stage=coalesce(nullif(v.stage,''),b.work_stage),
		due_at=v.due,fence=null,retry_started_at=v.retry_started,retry_attempts=v.retry_attempts,owner_deadline=v.owner_end,
		dispatch_fence=case when v.state='PENDING' and v.stage=b.work_stage then b.dispatch_fence end,
		candidates=coalesce(v.candidates,'[]'),evidence_version=v.evidence_version,completed_count=v.completed_count,
		discovery_binding=coalesce(nullif(v.discovery_binding,''),b.discovery_binding),
		mandate_not_before=case when v.check_mandate then v.due end,
		published_solo=b.published_solo or v.published_solo
	from input v where b.analysis_id=$1 and b.ordinal=v.ordinal and b.state='RUNNING' and b.fence=$2 and b.due_at>$3
		and not (select stopped or integrity_hold from head)
	returning b.ordinal,b.work_stage,b.retry_started_at,b.retry_attempts,v.outcome,v.completed_count
), holds as (
	insert into ouf_mcp.analysis_batch_hold(analysis_id,ordinal,work_stage,fence,reason_code,retry_started_at,retry_attempts,opened_at)
	select $1,ordinal,work_stage,$2,outcome->>'reasonCode',retry_started_at,retry_attempts,$3 from finished
	where outcome->>'state'='BLOCKED' and outcome->>'reasonCode'<>'ANALYSIS_COLUMN_INVALID'
	on conflict do nothing
), projected as (
	select c.ordinal,c.outcome old,f.outcome,nullif(f.outcome->>'requestId','')::uuid request_id,f.completed_count
	from finished f join ouf_mcp.analysis_column c on c.analysis_id=$1 and c.ordinal=f.ordinal
	where c.outcome is distinct from f.outcome
), changed as (
	update ouf_mcp.analysis_column c set outcome=p.outcome,request_id=p.request_id
	from projected p where c.analysis_id=$1 and c.ordinal=p.ordinal
	returning c.ordinal,p.old,c.outcome
), completion as (
	insert into ouf_mcp.analysis_job_completion(analysis_id,request_id,completed_count)
	select $1,(outcome->>'requestId')::uuid,completed_count from finished
	where outcome ? 'requestId' and completed_count is not null
	on conflict do nothing
), versions as (
	insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
	select $1,c.ordinal,h.seq+1,c.outcome from changed c cross join head h
), delta as (
	select count(*) n,count(*) filter(where old->>'state'='PENDING' and outcome->>'state'<>'PENDING') resolved,
		count(*) filter(where outcome->>'state'='MATCHED_PUBLISHED') matched,
		count(*) filter(where outcome->>'state'='CANDIDATES_READY') ready,
		count(*) filter(where outcome->>'state'='NO_CANDIDATES') zero_count,
		count(*) filter(where outcome->>'state'='BLOCKED') blocked from changed
), counts as (
	insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
	select $1,h.seq+1,h.total,h.matched+d.matched,h.ready+d.ready,h.zero_count+d.zero_count,h.blocked+d.blocked,
		h.pending-d.resolved,h.onboarding,h.stopped,h.integrity_hold from head h cross join delta d where d.n>0
) select count(*) from finished`

package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

func (s *Store) HoldBatchMandateStart(ctx context.Context, id string, m analysis.BatchMandate, reason string) error {
	_, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_batch_mandate set state='HELD',reason_code=$5,control_due=null
		where analysis_id=$1 and start_ref=$2 and generation=$3 and command_key=$4
		and state='PENDING' and control_fence is null`, id, m.StartRef, m.Generation, m.CommandKey, reason)
	return err
}

func (s *Store) CommitBatchMandate(ctx context.Context, id string, m analysis.BatchMandate, view analysis.BatchMandateView, now time.Time) error {
	if !analysis.ValidBatchAttempt(view.Ref) || view.Ref == m.StartRef || view.Generation != m.Generation ||
		view.AnalysisID != id || view.Version < 1 || view.ExpiresAtMs <= 0 {
		return analysis.ErrConflict
	}
	var binding, reason string
	switch view.State {
	case "ACTIVE":
		bound := analysis.BatchMandate{Ref: view.Ref, Binding: view.MandateBinding, StartRef: m.StartRef, Generation: m.Generation}
		signed := bound.BoundView()
		if m.Ref != "" || signed.AnalysisID != id || signed.Version != view.Version || signed.ExpiresAtMs != view.ExpiresAtMs {
			return analysis.ErrConflict
		}
		binding = view.MandateBinding
	case "REVOKED", "EXPIRED":
		if m.Ref != "" && (view.Ref != m.Ref || view.Version < m.Version) {
			return analysis.ErrConflict
		}
		reason = "MANDATE_" + view.State
	default:
		return analysis.ErrConflict
	}
	tag, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_batch_mandate
		set state=$5,reason_code=$6,mandate_ref=$7,mandate_binding=coalesce(mandate_binding,nullif($8,'')),
		control_due=case when $5='ACTIVE' then $9::timestamptz end,control_attempts=0,control_started=null,control_fence=null
		where analysis_id=$1 and start_ref=$2 and generation=$3 and command_key=$4
		and ($10::text='' or control_fence=nullif($10,'')::uuid)
		and ((state in ('PENDING','HELD') and mandate_ref is null)
		     or (state='ACTIVE' and mandate_ref=nullif($11,'')::uuid and $5<>'ACTIVE'))`,
		id, m.StartRef, m.Generation, m.CommandKey, view.State, reason, view.Ref, binding, now, m.ControlFence, m.Ref)
	if err == nil && tag.RowsAffected() != 1 {
		var same bool
		err = s.pool.QueryRow(ctx, `select exists(select from ouf_mcp.analysis_batch_mandate
			where analysis_id=$1 and start_ref=$2 and generation=$3 and command_key=$4 and state=$5 and mandate_ref=$6
			and ($5<>'ACTIVE' or mandate_binding=$7))`,
			id, m.StartRef, m.Generation, m.CommandKey, view.State, view.Ref, binding).Scan(&same)
		if err == nil && !same {
			err = analysis.ErrConflict
		}
	}
	return err
}

func (s *Store) NextMandatedBatch(ctx context.Context, now time.Time) (analysis.Scope, string, error) {
	var scope analysis.Scope
	var id string
	err := s.pool.QueryRow(ctx, `select p.analysis_id,p.service_principal_id,p.principal_id,p.tenant_id
		from ouf_mcp.analysis_batch_column b
		join ouf_mcp.analysis_batch_mandate m using(analysis_id)
		join ouf_mcp.profile_analysis p using(analysis_id)
		cross join lateral (select stopped or integrity_hold held from ouf_mcp.analysis_counts h
			where h.analysis_id=b.analysis_id order by seq desc limit 1) head
		where m.state<>'PENDING' and b.choice_key is null and b.work_stage in ('PUBLISHED','REQUEST','STATUS','CANDIDATES')
		and b.state in ('PENDING','RUNNING') and b.due_at<=$1 and not head.held
		and (m.state<>'ACTIVE' or b.mandate_not_before is null or b.mandate_not_before<=$1)
		and (m.state<>'ACTIVE' or not exists(select from ouf_mcp.analysis_batch_route_pause r
			where r.work_stage=b.work_stage and r.due_at>$1))
		and not exists(select from ouf_mcp.analysis_batch_column live join ouf_mcp.profile_analysis p_live using(analysis_id)
			where live.state='RUNNING' and live.fence is not null and live.due_at>$1 and
				(p_live.tenant_id=p.tenant_id or live.work_stage=b.work_stage))
		order by b.due_at,b.analysis_id,b.ordinal limit 1`, now).
		Scan(&id, &scope.Service, &scope.Principal, &scope.Tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return scope, id, err
}

// FIRST_ROUND is independent of subsequent HUMAN choices.
const batchMandatePhase = `case when h.integrity_hold then 'CLOSED' when h.stopped then 'CANCELLED'
	when exists(select from ouf_mcp.analysis_batch_column b where b.analysis_id=m.analysis_id
		and b.choice_key is null and b.work_stage in ('PUBLISHED','REQUEST','STATUS','CANDIDATES')
		and b.state in ('PENDING','RUNNING')) then 'FIRST_ROUND' else 'FIRST_ROUND_COMPLETE' end`

const batchMandateHead = ` cross join lateral (select stopped,integrity_hold from ouf_mcp.analysis_counts
	where analysis_id=m.analysis_id order by seq desc limit 1) h `

func (s *Store) ClaimBatchMandateControl(ctx context.Context, now time.Time, fence string) (analysis.BatchMandateControl, error) {
	var job analysis.BatchMandateControl
	m := &job.Mandate
	err := s.pool.QueryRow(ctx, `with due as (
		select m.analysis_id from ouf_mcp.analysis_batch_mandate m`+batchMandateHead+`
		where m.control_due<=$1 and (m.state='PENDING' or (m.state='ACTIVE' and `+batchMandatePhase+`<>'FIRST_ROUND'))
		order by m.control_due,m.analysis_id limit 1 for update of m skip locked
	), claimed as (
		update ouf_mcp.analysis_batch_mandate m set control_due=$3,control_fence=$2,
			control_started=coalesce(control_started,$1),control_attempts=least(control_attempts+1,128)
		from due where m.analysis_id=due.analysis_id returning m.*
	)
	select m.analysis_id,p.profile->>'policyEpoch',m.state,m.start_ref::text,m.start_binding,
		coalesce(m.mandate_ref::text,''),coalesce(m.mandate_binding,''),m.command_key::text,m.generation,
		m.control_started,m.control_attempts
	from claimed m join ouf_mcp.profile_analysis p using(analysis_id)`, now, fence, now.Add(analysis.BatchLease)).
		Scan(&job.AnalysisID, &job.Epoch, &m.State, &m.StartRef, &m.StartBinding, &m.Ref, &m.Binding,
			&m.CommandKey, &m.Generation, &job.Started, &job.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	job.Fence = fence
	m.ControlFence = fence
	if m.State == "ACTIVE" {
		m.Version = m.BoundView().Version
	}
	return job, err
}

func (s *Store) FinishBatchMandateControl(ctx context.Context, job analysis.BatchMandateControl, reason string, due time.Time) error {
	_, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_batch_mandate
		set control_due=$4,control_fence=null,reason_code=$5,
		state=case when $4::timestamptz is null and state='PENDING' then 'HELD' else state end
		where analysis_id=$1 and control_fence=$2 and state=$3`,
		job.AnalysisID, job.Fence, job.Mandate.State, batchTimestamp(due), reason)
	return err
}

func (s *Store) RecordBatchMandateAttempt(ctx context.Context, job analysis.BatchWork, ordinals []int, started, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_batch_column
		set retry_attempts=retry_attempts+1,retry_started_at=coalesce(retry_started_at,$4)
		where analysis_id=$1 and ordinal=any($3::int[]) and state='RUNNING'
		and fence=$2 and transport_fence=$2 and due_at>greatest($5,clock_timestamp()) and retry_attempts=$6`,
		job.AnalysisID, job.Fence, ordinals, started, now, job.Columns[0].RetryAttempts)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ordinals)) {
		return analysis.ErrConflict
	}
	return tx.Commit(ctx)
}

func (s *Store) RecordBatchMandatePause(ctx context.Context, job analysis.BatchWork, due, now time.Time) error {
	_, err := s.pool.Exec(ctx, `insert into ouf_mcp.analysis_batch_route_pause(work_stage,due_at)
		select $1,$2 where exists(select from ouf_mcp.analysis_batch_column b where b.analysis_id=$3
			and b.fence=$4 and b.reconcile_fence=$4 and b.state='RUNNING' and b.due_at>greatest($5,clock_timestamp()))
		on conflict(work_stage) do update set due_at=greatest(analysis_batch_route_pause.due_at,excluded.due_at)`,
		job.Route(), due, job.AnalysisID, job.Fence, now)
	return err
}

func (s *Store) RecordBatchMandateObservation(ctx context.Context, job analysis.BatchWork, due, now time.Time) error {
	_, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_batch_column
		set mandate_not_before=greatest(mandate_not_before,least($3::timestamptz,retry_started_at+interval '24 hours',
			case when owner_deadline>=$4 then owner_deadline end))
		where analysis_id=$1 and fence=$2 and transport_fence=$2 and state='RUNNING' and due_at>greatest($4,clock_timestamp())`,
		job.AnalysisID, job.Fence, due, now)
	return err
}

func (s *Store) BatchMandateEvidence(ctx context.Context, q analysis.BatchEvidenceRequest, checksum string, now time.Time) (analysis.BatchEvidence, error) {
	var proof analysis.BatchEvidence
	scope := analysis.Scope{Service: q.Client, Principal: q.Subject, Tenant: q.Tenant}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return proof, err
	}
	defer tx.Rollback(ctx)
	start, err := readBatchStart(ctx, tx, scope, q.AnalysisID, checksum)
	if errors.Is(err, analysis.ErrScope) {
		return proof, nil
	}
	if err != nil {
		return proof, err
	}
	if !start.Seeded || !q.Matches(start, scope) || !analysis.ValidBatchHash(start.Epoch) {
		return proof, nil
	}
	err = tx.QueryRow(ctx, `select `+batchMandatePhase+` from ouf_mcp.analysis_batch_mandate m`+
		batchMandateHead+` where m.analysis_id=$1`, start.ID).Scan(&proof.Phase)
	if err != nil {
		return analysis.BatchEvidence{}, err
	}
	proof.AnalysisID, proof.PolicyEpoch = start.ID, start.Epoch
	m := start.Mandate
	switch q.Action {
	case "issue":
		proof.Bound = q.Version == 0
	case "expire":
		proof.Bound = m.Ref != "" && q.Version == m.Version
	case "dispatch":
		if q.Version != m.Version || m.State != "ACTIVE" {
			return analysis.BatchEvidence{}, nil
		}
		if proof.Phase != "FIRST_ROUND" {
			proof.Bound = true // Allows Gateway expiry after a concurrent close.
			return proof, nil
		}
		return batchDispatchEvidence(ctx, tx, q, proof, now)
	}
	return proof, nil
}

func batchDispatchEvidence(ctx context.Context, tx pgx.Tx, q analysis.BatchEvidenceRequest, proof analysis.BatchEvidence, now time.Time) (analysis.BatchEvidence, error) {
	stage := analysis.BatchStageForOperation(q.Operation)
	rows, err := tx.Query(ctx, `select b.ordinal,c.header,b.intent,b.due_at,
		coalesce(c.request_id::text,''),coalesce(b.discovery_arguments_hash::text,''),coalesce(b.discovery_binding,'')
		from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_column c using(analysis_id,ordinal)
		where b.analysis_id=$1 and b.fence=$2 and b.dispatch_fence=$2 and b.transport_fence=$2
		and b.work_stage=$3 and b.state='RUNNING' and b.choice_key is null
		and b.due_at>greatest($4,clock_timestamp()) and b.mandate_arguments_hash=$5
		order by b.ordinal`, q.AnalysisID, q.AttemptID, stage, now, q.ArgumentsHash)
	if err != nil {
		return analysis.BatchEvidence{}, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ordinal int
		var header, intent, requestID, createHash, binding string
		var lease time.Time
		if err = rows.Scan(&ordinal, &header, &intent, &lease, &requestID, &createHash, &binding); err != nil {
			return analysis.BatchEvidence{}, err
		}
		if intent == "" || intent != analysis.ColumnIntent(header) {
			return analysis.BatchEvidence{}, nil
		}
		count++
		proof.LeaseUntil = lease
		if stage == analysis.BatchPublished {
			continue
		}
		proof.WorkRef = analysis.BatchDiscoveryWorkRef(q.AnalysisID, ordinal)
		if stage == analysis.BatchRequest {
			if createHash != q.ArgumentsHash {
				return analysis.BatchEvidence{}, nil
			}
			proof.DiscoveryArgumentsHash = createHash
		} else {
			if requestID == "" || q.RequestID != requestID || !analysis.ValidBatchHash(createHash) || binding == "" {
				return analysis.BatchEvidence{}, nil
			}
			proof.DiscoveryArgumentsHash, proof.DiscoveryBinding = createHash, binding
		}
	}
	if err = rows.Err(); err != nil {
		return analysis.BatchEvidence{}, err
	}
	proof.Bound = count > 0 && (stage == analysis.BatchPublished || count == 1)
	return proof, nil
}

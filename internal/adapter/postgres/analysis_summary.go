package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	manifestpkg "github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func createAnalysisSummary(ctx context.Context, tx pgx.Tx, p analysis.Plan) error {
	if len(p.Headers) != p.Profile.ColumnCount {
		return analysis.ErrProfile
	}
	blocked := 0
	for _, name := range p.Headers {
		if name == "" {
			blocked++
		}
	}
	tag, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_counts(analysis_id,seq,total,blocked,pending,onboarding)
		values ($1,1,$2,$3,$2::int-$3::int,$4) on conflict do nothing`, p.ID, len(p.Headers), blocked, p.Onboarding)
	if err != nil || tag.RowsAffected() == 0 {
		return err
	}
	_, err = tx.Exec(ctx, `with initial as (
		insert into ouf_mcp.analysis_column(analysis_id,ordinal,header,next_tool,outcome)
		select $1,n-1,name,case when name='' then '' else 'source.analysis.plan' end,
			case when name='' then '{"state":"BLOCKED","reasonCode":"ANALYSIS_COLUMN_INVALID"}'::jsonb
			else '{"state":"PENDING"}'::jsonb end
		from unnest($2::text[]) with ordinality as h(name,n)
		returning analysis_id,ordinal,outcome)
		insert into ouf_mcp.analysis_column_version select analysis_id,ordinal,1,outcome from initial`, p.ID, p.Headers)
	return err
}

func assignSummaryTask(ctx context.Context, tx pgx.Tx, id string, t analysis.Task) error {
	_, err := tx.Exec(ctx, `update ouf_mcp.analysis_column set task_id=$3,next_tool='source.analysis.step',published_first=$4
		where analysis_id=$1 and ordinal=any($2::int[]) and outcome->>'state'='PENDING'
		and request_id is null and (task_id is null or next_tool='source.analysis.plan')`, id, t.Columns, t.ID, t.Tool == analysis.BatchTool)
	return err
}

// The parent analysis row is locked by every writer, including v1 claim and
// finish. Update all changed ordinals, their versions and the count delta as
// one set. A repeated observation preserves the original observation time.
func projectOutcome(ctx context.Context, tx pgx.Tx, id string, columns []int, request string, o analysis.Outcome, next string) error {
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return updateOutcomes(ctx, tx, id, `select c.ordinal,c.outcome as old,$4::jsonb as outcome,
		nullif($3,'')::uuid as request_id,$5::text as next_tool from ouf_mcp.analysis_column c
		where c.analysis_id=$1 and
		  (($2::int[] is not null and c.ordinal=any($2) and (c.request_id is null or c.request_id=nullif($3,'')::uuid))
		    or ($2 is null and c.request_id=nullif($3,'')::uuid))
		  and not (c.next_tool='semantic.discovery.candidates' and $5='semantic.discovery.status')`, columns, request, body, next)
}

// Extending a retained request to another ordinal inherits its verified
// receipt or evidence under the same writer fence, without a new dispatch.
func inheritRequests(ctx context.Context, tx pgx.Tx, id string, tasks []string) error {
	if len(tasks) == 0 {
		return nil
	}
	return updateOutcomes(ctx, tx, id, `select c.ordinal,c.outcome as old,case when prior.outcome->>'state'='NO_CANDIDATES'
		then jsonb_build_object('state','BLOCKED','reasonCode','DISCOVERY_DECISION_REQUIRED','requestId',prior.request_id)
		else prior.outcome end as outcome,prior.request_id,case when prior.outcome->>'state'='NO_CANDIDATES'
		then 'semantic.discovery.candidates' else prior.next_tool end as next_tool
		from ouf_mcp.analysis_column c cross join lateral (
		  select x.outcome,x.request_id,x.next_tool from ouf_mcp.analysis_column x
		  where x.analysis_id=c.analysis_id and x.task_id=c.task_id and x.request_id is not null
		  order by x.ordinal limit 1) prior
		where c.analysis_id=$1 and c.task_id=any($2::text[]) and c.request_id is null`, tasks)
}

func updateOutcomes(ctx context.Context, tx pgx.Tx, id, proposalSQL string, params ...any) error {
	_, err := tx.Exec(ctx, `with head as (
		select * from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1
	), proposed as materialized (`+proposalSQL+`), changed as (
		update ouf_mcp.analysis_column c set outcome=p.outcome,request_id=p.request_id,next_tool=p.next_tool
		from proposed p where c.analysis_id=$1 and c.ordinal=p.ordinal and not (select integrity_hold from head)
		  and (p.old->>'state'='PENDING' or (p.old->>'state'='CANDIDATES_READY' and p.outcome->>'reasonCode'='DISCOVERY_DECISION_REQUIRED')
		    or (p.old->>'reasonCode'='DISCOVERY_DECISION_REQUIRED' and p.outcome->>'state'='NO_CANDIDATES')
		    or (p.old->>'reasonCode'='DISCOVERY_EVIDENCE_UNAVAILABLE'
		     and (p.outcome->>'state' in ('NO_CANDIDATES','CANDIDATES_READY') or p.outcome->>'reasonCode'='DISCOVERY_DECISION_REQUIRED')))
		  and (p.old is distinct from p.outcome or c.next_tool<>p.next_tool)
		returning c.ordinal,p.old,c.outcome
	), versions as (
		insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
		select $1,c.ordinal,h.seq+1,c.outcome from changed c cross join head h where c.old<>c.outcome
		returning ordinal
	), delta as (
		select count(*) filter(where old->>'state'='PENDING') as op,
		  count(*) filter(where old->>'state'='BLOCKED') as ob,
		  count(*) filter(where outcome->>'state'='PENDING') as np,
		  count(*) filter(where outcome->>'state'='BLOCKED') as nb,
		  count(*) filter(where outcome->>'state'='MATCHED_PUBLISHED') as nm,
		  count(*) filter(where outcome->>'state'='NO_CANDIDATES') as nz,
		  count(*) filter(where outcome->>'state'='CANDIDATES_READY') - count(*) filter(where old->>'state'='CANDIDATES_READY') as nr
		from changed where old<>outcome
	), stop as (
		select not exists(select from changed c join proposed p using(ordinal) where p.next_tool<>'')
		and not exists(select from ouf_mcp.analysis_column c where c.analysis_id=$1 and c.next_tool<>''
		and not exists(select from changed u where u.ordinal=c.ordinal)) as stopped
	)
	insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped)
	select $1,h.seq+1,h.total,h.matched+d.nm,h.ready+d.nr,h.zero_count+d.nz,h.blocked-d.ob+d.nb,
		h.pending-d.op+d.np,h.onboarding,(h.pending-d.op+d.np>0 and stop.stopped)
	from head h cross join delta d cross join stop where not h.integrity_hold and
	(exists(select 1 from versions) or h.stopped is distinct from (h.pending-d.op+d.np>0 and stop.stopped))`,
		append([]any{id}, params...)...)
	return err
}

func finishSummary(ctx context.Context, tx pgx.Tx, id string, t analysis.Task, now time.Time, obs orchestration.OwnerObservation) error {
	if t.Tool == analysis.BatchTool {
		return savePublished(ctx, tx, id, t, analysis.Finish{})
	}
	if (t.Tool == "semantic.discovery.status" || t.Tool == "semantic.discovery.candidates") &&
		(t.State == "SUCCEEDED" || !obs.At.IsZero()) {
		var ref struct{ RequestID string }
		if json.Unmarshal(t.Arguments, &ref) != nil || !analysis.CanonicalUUID(ref.RequestID) {
			return analysis.ErrInvalid
		}
		if obs.At.IsZero() {
			obs = orchestration.ObserveOwner(orchestration.GatewayResponse{Body: t.Result}, nil, now)
		}
		if t.NotBefore.After(obs.Due) {
			obs.Due = t.NotBefore
		}
		saved, err := saveDiscoveryClock(ctx, tx, id, ref.RequestID, obs)
		if err != nil {
			return err
		}
		if t.State == "SUCCEEDED" {
			return projectDiscovery(ctx, tx, id, t.Tool, ref.RequestID, t.Result, obs.At, saved.Due)
		}
	}
	if t.State == "HELD" {
		if orchestration.SafeDenialReason(t.Code, "") != "" || t.Code == "OWNER_AUTHORIZATION_DENIED" || t.Code == "OWNER_REQUEST_REJECTED" || t.Code == "RETRY_BUDGET_EXHAUSTED" || t.Code == "AUTHORIZATION_DENIED" {
			return projectOutcome(ctx, tx, id, t.Columns, "", analysis.Outcome{State: "BLOCKED", Reason: t.Code}, "")
		}
		return projectOutcome(ctx, tx, id, t.Columns, "", analysis.Outcome{State: "PENDING"}, "")
	}
	if t.State != "SUCCEEDED" {
		return nil
	}
	var ref struct{ RequestID string }
	switch t.Tool {
	case "semantic.discovery.request":
		if !analysis.ValidEvidenceJSON(t.Result) || json.Unmarshal(t.Result, &ref) != nil || !analysis.CanonicalUUID(ref.RequestID) {
			return projectOutcome(ctx, tx, id, t.Columns, "", analysis.Outcome{State: "BLOCKED", Reason: "DISCOVERY_EVIDENCE_UNAVAILABLE"}, "")
		}
		outcome, next := analysis.DiscoveryContinuation(ref.RequestID, t.Result, now, t.NotBefore, "semantic.discovery.status")
		obs := orchestration.OwnerObservation{Due: outcome.NotBefore, Reason: "OWNER_OBSERVATION", Code: t.Code}
		if _, err := saveDiscoveryClock(ctx, tx, id, ref.RequestID, obs); err != nil {
			return err
		}
		return projectOutcome(ctx, tx, id, t.Columns, ref.RequestID, outcome, next)
	default:
		_, err := tx.Exec(ctx, `update ouf_mcp.analysis_column set next_tool='source.analysis.plan',task_id=null
			where analysis_id=$1 and task_id=$2 and next_tool='source.analysis.step'`, id, t.ID)
		return err
	}
}

func saveDiscoveryClock(ctx context.Context, tx pgx.Tx, id, request string, obs orchestration.OwnerObservation) (orchestration.OwnerObservation, error) {
	// 019 requires a timestamp. The observation time is an immediately elapsed
	// floor for a response without a future due date.
	if obs.Due.IsZero() {
		obs.Due = time.Now().UTC().Truncate(time.Microsecond)
	} else if us := obs.Due.Truncate(time.Microsecond); us.Before(obs.Due) {
		obs.Due = us.Add(time.Microsecond)
	}
	err := tx.QueryRow(ctx, `insert into ouf_mcp.analysis_owner_due as w
		(analysis_id,request_id,not_before,reason,observation_code,observation_status)
		values ($1,$2,$3,$4,$5,$6) on conflict (analysis_id,request_id) do update set
		not_before=greatest(w.not_before,excluded.not_before),
		reason=case when excluded.not_before >= w.not_before then excluded.reason else w.reason end,
		observation_code=excluded.observation_code,observation_status=excluded.observation_status
		returning not_before,reason`, id, request, obs.Due, obs.Reason, obs.Code, obs.Status).Scan(&obs.Due, &obs.Reason)
	return obs, err
}

// A nil evidence payload is an error observation. It can postpone the
// request without replacing verified column evidence or storing an error body.
func (s *Store) RecordDiscovery(ctx context.Context, scope analysis.Scope, id, manifest, tool, request string, raw []byte, obs orchestration.OwnerObservation) (orchestration.OwnerObservation, error) {
	if !analysis.CanonicalUUID(request) {
		return obs, analysis.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return obs, err
	}
	defer tx.Rollback(ctx)
	if _, _, err = analysisHeader(ctx, tx, scope, id, manifest); err != nil {
		return obs, err
	}
	var bound bool
	if err = tx.QueryRow(ctx, `select exists(select from ouf_mcp.analysis_column where analysis_id=$1 and request_id=$2)`, id, request).Scan(&bound); err != nil {
		return obs, err
	}
	if !bound {
		return obs, analysis.ErrScope
	}
	obs, err = saveDiscoveryClock(ctx, tx, id, request, obs)
	if err != nil {
		return obs, err
	}
	if raw != nil {
		if err = projectDiscovery(ctx, tx, id, tool, request, raw, time.Now(), obs.Due); err != nil {
			if errors.Is(err, analysis.ErrProfile) {
				return obs, holdAnalysis(ctx, tx, id)
			}
			return obs, err
		}
	}
	return obs, tx.Commit(ctx)
}

// Commit the hold under the already locked plan row. Conflicting owner
// assertions never replace retained evidence or start another attempt.
func holdAnalysis(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_counts
		(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
		select analysis_id,seq+1,total,matched,ready,zero_count,blocked,pending,onboarding,true,true
		from (select * from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1) h
		where not integrity_hold`, id)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return analysis.ErrProfile
}

func checkAnalysisHold(ctx context.Context, tx pgx.Tx, id string) error {
	var held bool
	err := tx.QueryRow(ctx, `select coalesce((select integrity_hold from ouf_mcp.analysis_counts
		where analysis_id=$1 order by seq desc limit 1),false)`, id).Scan(&held)
	if err != nil {
		return err
	}
	if held {
		return analysis.ErrProfile
	}
	return nil
}

func projectDiscovery(ctx context.Context, tx pgx.Tx, id, tool, request string, raw []byte, now, floor time.Time) error {
	var completion *int
	err := tx.QueryRow(ctx, `select completed_count from ouf_mcp.analysis_job_completion
		where analysis_id=$1 and request_id=$2`, id, request).Scan(&completion)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if count := analysis.CompletedCount(request, raw); count != nil {
		if completion != nil && *completion != *count {
			return analysis.ErrProfile
		}
		if completion == nil {
			_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_job_completion
				select $1,$2,$3 where exists(select from ouf_mcp.analysis_column
				where analysis_id=$1 and request_id=$2)`, id, request, *count)
			if err != nil {
				return err
			}
			completion = count
		}
	}
	if tool == "semantic.discovery.candidates" {
		empty := analysis.EmptyCandidatePage(request, raw)
		if _, err = tx.Exec(ctx, `update ouf_mcp.analysis_column set empty_candidate_page=$3,
			proposal_choice=case when $3 then coalesce(proposal_choice,gen_random_uuid()) end,
			exclusion_choice=case when $3 then coalesce(exclusion_choice,gen_random_uuid()) end
			where analysis_id=$1 and request_id=$2 and (empty_candidate_page<>$3 or ($3 and proposal_choice is null))`, id, request, empty); err != nil {
			return err
		}
	}
	o, next := analysis.DiscoveryEvidenceWithCompletion(tool, request, raw, completion, now, floor)
	return projectOutcome(ctx, tx, id, nil, request, o, next)
}

// Both live summary paths select one column, then read only its task. Older
// rows may have the owner's delay only in the original discovery receipt.
const summaryTimingSQL = `jsonb_build_object(
	'notBefore',w.not_before,'ownerDueReason',w.reason,
	'decisionRequired',c.outcome->>'reasonCode'='DISCOVERY_DECISION_REQUIRED',
	'nextAttemptAt',case when c.next_tool='source.analysis.step' then t.task->'notBefore' end,
	'next_retry_at',case when c.next_tool='source.analysis.step'
		and t.task->>'tool' in ('semantic.discovery.status','semantic.discovery.candidates')
		then (select ` + plannedReadDue + plannedReadClocks + `where t.analysis_id=c.analysis_id and t.task_id=c.task_id) end,
	'completedAt',t.task->'completedAt',
	'result',case when c.request_id::text=t.task->'result'->>'requestId'
		then t.task->'result' end)`

func mergeSummaryTiming(h *analysis.Hint, raw []byte) error {
	var obs map[string]any
	if err := json.Unmarshal(raw, &obs); err != nil {
		return err
	}
	if reason, ok := obs["ownerDueReason"].(string); ok {
		h.OwnerReason = reason
	}
	if decision, ok := obs["decisionRequired"].(bool); ok {
		h.DecisionRequired = decision
	}
	var floor time.Time
	if h.NotBefore != "" {
		var err error
		floor, err = time.Parse(time.RFC3339Nano, h.NotBefore)
		if err != nil {
			return analysis.ErrProfile
		}
	}
	due, valid := orchestration.OwnerDue(obs, time.Now(), floor, "")
	if !valid {
		return analysis.ErrProfile
	}
	if !due.IsZero() {
		h.NotBefore = due.UTC().Format(time.RFC3339Nano)
	}
	return nil
}

const summaryPageSQL = `select jsonb_build_object('ordinal',n,'changedSeq',v.seq::text)||v.outcome` + summaryVersionsSQL

const deniedSummaryPageSQL = `select jsonb_build_object('ordinal',n,'changedSeq',v.seq::text)||v.outcome
	|| case when v.outcome->>'state'='BLOCKED' then jsonb_strip_nulls(jsonb_build_object('policyVersion',
	coalesce((select t.task->'policyVersion' from ouf_mcp.analysis_column c join ouf_mcp.profile_analysis_task t using(analysis_id,task_id)
	where c.analysis_id=$1 and c.ordinal=n and t.task->>'code'=v.outcome->>'reasonCode'),
	(select t.task->'policyVersion' from ouf_mcp.analysis_published_plan p join ouf_mcp.profile_analysis_task t using(analysis_id)
	where p.analysis_id=$1 and p.stop_reason=v.outcome->>'reasonCode' and t.task->>'code'=p.stop_reason order by t.ordinal limit 1)))) else '{}'::jsonb end` + summaryVersionsSQL

const summaryVersionsSQL = `
	from generate_series($2::int+1,least($3::int-1,$2::int+$4::int)) n
	cross join lateral (select seq,outcome from ouf_mcp.analysis_column_version
	where analysis_id=$1 and ordinal=n and seq<=$5 order by seq desc limit 1) v order by n`

func measureSummaryRow(s *analysis.Summary, rows pgx.Rows) {
	s.ReadRows++
	for _, v := range rows.RawValues() {
		s.ReadBytes += len(v)
	}
}

func scanSummaryRow(ctx context.Context, tx pgx.Tx, result *analysis.Summary, query string, args []any, dest ...any) error {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	measureSummaryRow(result, rows)
	return rows.Scan(dest...)
}

func (s *Store) ReadSummary(ctx context.Context, scope analysis.Scope, id, manifest string, pos analysis.Position, page bool) (analysis.Summary, error) {
	var out analysis.Summary
	if pos.Limit < 1 || pos.Limit > 128 || pos.Last < -1 || pos.Last >= 1024 {
		return out, analysis.ErrInvalid
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	c := &out.Counts
	err = scanSummaryRow(ctx, tx, &out, `select h.seq,h.total,h.matched,h.ready,h.zero_count,h.blocked,h.pending,h.onboarding,h.stopped,
		case when h.integrity_hold then 'ANALYSIS_PROFILE_CHANGED' else coalesce(pub.stop_reason,'') end,
		p.profile->>'assetId',p.profile->>'profileId',p.revision,coalesce(pub.visibility_digest::text,''),coalesce(p.profile->>'policyEpoch','')
		from ouf_mcp.profile_analysis p left join ouf_mcp.analysis_published_plan pub using(analysis_id) cross join lateral
		(select * from ouf_mcp.analysis_counts where analysis_id=p.analysis_id and seq <= $6 order by seq desc limit 1) h
		where p.analysis_id=$1 and p.service_principal_id=$2 and p.principal_id=$3 and p.tenant_id=$4 and p.manifest_checksum=any($5::char(64)[])`,
		[]any{id, scope.Service, scope.Principal, scope.Tenant, manifestpkg.AnalysisManifests(manifest), pos.Seq}, &out.Seq, &c.Total, &c.Matched, &c.Ready, &c.Zero, &c.Blocked, &c.Pending, &out.Onboarding, &out.Stopped, &out.Hint.Reason, &out.Hint.AssetID, &out.Hint.ProfileID, &out.Hint.Revision, &out.Visibility, &out.Epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, analysis.ErrScope
	}
	if err != nil {
		return out, err
	}
	if pos.Last >= 0 && out.Seq != pos.Seq {
		return out, analysis.ErrProfile
	}
	var timing []byte
	if pos.Snapshot != uuid.Nil {
		var raw []byte
		var header string
		err = scanSummaryRow(ctx, tx, &out, `select a.frozen_hint,case when a.frozen_hint->>'Tool'='source.analysis.plan' then coalesce(c.header,'') else '' end,
		 `+summaryTimingSQL+` from ouf_mcp.analysis_summary_read a
		 left join ouf_mcp.analysis_column c on c.analysis_id=a.analysis_id and c.ordinal=(a.frozen_hint->>'Ordinal')::int
		 and coalesce(c.task_id,'')=a.frozen_hint->>'TaskID' and coalesce(c.request_id::text,'')=a.frozen_hint->>'RequestID'
		 left join ouf_mcp.profile_analysis_task t on t.analysis_id=a.analysis_id and t.task_id=a.frozen_hint->>'TaskID'
		 left join ouf_mcp.analysis_owner_due w on w.analysis_id=a.analysis_id and w.request_id=nullif(a.frozen_hint->>'RequestID','')::uuid
		 where a.analysis_id=$1 and a.snapshot_id=$2 and a.seq=$3 and a.frozen_hint is not null`, []any{id, pos.Snapshot, out.Seq}, &raw, &header, &timing)
		if errors.Is(err, pgx.ErrNoRows) {
			return out, analysis.ErrExpired
		}
		if err != nil {
			return out, err
		}
		if json.Unmarshal(raw, &out.Hint) != nil {
			return out, analysis.ErrProfile
		}
		out.Hint.Header = header
	} else if out.Stopped || out.Hint.Reason != "" {
		if out.Hint.Reason == "" {
			out.Hint.Reason = "OUTCOME_REQUIRES_RECONCILIATION"
		}
	} else if c.Pending > 0 || c.Blocked > 0 {
		err = scanSummaryRow(ctx, tx, &out, `select c.next_tool,coalesce(c.task_id,''),coalesce(c.request_id::text,''),c.ordinal,
			case when c.next_tool='source.analysis.plan' then c.header else '' end,`+summaryTimingSQL+`
			from (select * from ouf_mcp.analysis_column where analysis_id=$1 and next_tool<>'' order by published_first desc,ordinal limit 1) c
			left join ouf_mcp.profile_analysis_task t using(analysis_id,task_id)
			left join ouf_mcp.analysis_owner_due w on w.analysis_id=c.analysis_id and w.request_id=c.request_id`, []any{id}, &out.Hint.Tool, &out.Hint.TaskID, &out.Hint.RequestID, &out.Hint.Ordinal, &out.Hint.Header, &timing)
	}
	if errors.Is(err, pgx.ErrNoRows) && c.Blocked == c.Total {
		err = scanSummaryRow(ctx, tx, &out, `select outcome->>'reasonCode' from ouf_mcp.analysis_column where analysis_id=$1 and outcome->>'state'='BLOCKED' order by ordinal limit 1`, []any{id}, &out.Hint.Reason)
	} else if pos.Snapshot == uuid.Nil && !out.Stopped && out.Hint.Reason == "" && c.Pending == 0 && c.Ready > 0 && out.Onboarding && out.Hint.Tool == "" {
		err = scanSummaryRow(ctx, tx, &out, `select request_id::text from ouf_mcp.analysis_column where analysis_id=$1 and outcome->>'state'='CANDIDATES_READY' order by ordinal limit 1`, []any{id}, &out.Hint.RequestID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	if len(timing) > 0 {
		decision := out.Hint.DecisionRequired
		if err := mergeSummaryTiming(&out.Hint, timing); err != nil {
			return out, err
		}
		if pos.Snapshot != uuid.Nil {
			out.Hint.DecisionRequired = decision
		}
	}
	if page {
		query := summaryPageSQL
		if c.Blocked > 0 {
			query = deniedSummaryPageSQL
		}
		rows, err := tx.Query(ctx, query, id, pos.Last, c.Total, pos.Limit, out.Seq)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			measureSummaryRow(&out, rows)
			var item json.RawMessage
			if err = rows.Scan(&item); err != nil {
				rows.Close()
				return out, err
			}
			out.Items = append(out.Items, item)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return out, err
		}
		if len(out.Items) != min(pos.Limit, c.Total-pos.Last-1) {
			return out, analysis.ErrProfile
		}
	}
	return out, tx.Commit(ctx)
}

func (s *Store) AuditSummary(ctx context.Context, scope analysis.Scope, id, decision string, seq int64, first, last int, snapshot uuid.UUID, hint *analysis.Hint) error {
	var frozen []byte
	if hint != nil {
		saved := *hint
		saved.Header = ""
		frozen, _ = json.Marshal(saved)
	}
	_, err := s.pool.Exec(ctx, `insert into ouf_mcp.analysis_summary_read
		(analysis_id,service_principal_id,principal_id,tenant_id,decision_ref,seq,first_ordinal,last_ordinal,snapshot_id,frozen_hint)
		values($1,$2,$3,$4,$5,$6,$7,$8,nullif($9,'00000000-0000-0000-0000-000000000000')::uuid,$10)`, id, scope.Service, scope.Principal, scope.Tenant, decision, seq, first, last, snapshot.String(), frozen)
	return err
}

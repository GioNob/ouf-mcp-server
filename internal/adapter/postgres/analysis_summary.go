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
	return updateOutcomes(ctx, tx, id, `select c.ordinal,c.outcome as old,prior.outcome,prior.request_id,prior.next_tool
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
		  and (p.old->>'state'='PENDING' or (p.old->>'reasonCode'='DISCOVERY_EVIDENCE_UNAVAILABLE'
		     and p.outcome->>'state' in ('NO_CANDIDATES','CANDIDATES_READY')))
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
		  count(*) filter(where outcome->>'state'='CANDIDATES_READY') as nr
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

func finishSummary(ctx context.Context, tx pgx.Tx, id string, t analysis.Task, now time.Time) error {
	if t.Tool == analysis.BatchTool {
		return savePublished(ctx, tx, id, t, analysis.Finish{})
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
		return projectOutcome(ctx, tx, id, t.Columns, ref.RequestID, analysis.Outcome{State: "PENDING", RequestID: ref.RequestID}, "semantic.discovery.status")
	case "semantic.discovery.status", "semantic.discovery.candidates":
		if json.Unmarshal(t.Arguments, &ref) != nil || !analysis.CanonicalUUID(ref.RequestID) {
			return analysis.ErrInvalid
		}
		return projectDiscovery(ctx, tx, id, t.Tool, ref.RequestID, t.Result, now)
	default:
		_, err := tx.Exec(ctx, `update ouf_mcp.analysis_column set next_tool='source.analysis.plan',task_id=null
			where analysis_id=$1 and task_id=$2 and next_tool='source.analysis.step'`, id, t.ID)
		return err
	}
}

func (s *Store) ObserveDiscovery(ctx context.Context, scope analysis.Scope, id, manifest, tool, request string, raw []byte) error {
	if !analysis.CanonicalUUID(request) {
		return analysis.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, _, err = analysisHeader(ctx, tx, scope, id, manifest); err != nil {
		return err
	}
	var bound bool
	if err = tx.QueryRow(ctx, `select exists(select from ouf_mcp.analysis_column where analysis_id=$1 and request_id=$2)`, id, request).Scan(&bound); err != nil {
		return err
	}
	if !bound {
		return analysis.ErrScope
	}
	if err = projectDiscovery(ctx, tx, id, tool, request, raw, time.Now()); err != nil {
		if errors.Is(err, analysis.ErrProfile) {
			return holdAnalysis(ctx, tx, id)
		}
		return err
	}
	return tx.Commit(ctx)
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

func projectDiscovery(ctx context.Context, tx pgx.Tx, id, tool, request string, raw []byte, now time.Time) error {
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
	o, next := analysis.DiscoveryEvidenceWithCompletion(tool, request, raw, completion, now)
	return projectOutcome(ctx, tx, id, nil, request, o, next)
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
	if pos.Snapshot != uuid.Nil {
		var raw []byte
		var header string
		err = scanSummaryRow(ctx, tx, &out, `select a.frozen_hint,coalesce(c.header,'') from ouf_mcp.analysis_summary_read a
		 left join ouf_mcp.analysis_column c on a.frozen_hint->>'Tool'='source.analysis.plan' and c.analysis_id=a.analysis_id and c.ordinal=(a.frozen_hint->>'Ordinal')::int
		 where a.analysis_id=$1 and a.snapshot_id=$2 and a.seq=$3 and a.frozen_hint is not null`, []any{id, pos.Snapshot, out.Seq}, &raw, &header)
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
	} else if c.Pending > 0 {
		err = scanSummaryRow(ctx, tx, &out, `select next_tool,coalesce(task_id,''),coalesce(request_id::text,''),ordinal,case when next_tool='source.analysis.plan' then header else '' end
			from ouf_mcp.analysis_column where analysis_id=$1 and next_tool<>'' order by published_first desc,ordinal limit 1`, []any{id}, &out.Hint.Tool, &out.Hint.TaskID, &out.Hint.RequestID, &out.Hint.Ordinal, &out.Hint.Header)
	} else if c.Blocked == c.Total {
		err = scanSummaryRow(ctx, tx, &out, `select outcome->>'reasonCode' from ouf_mcp.analysis_column where analysis_id=$1 and outcome->>'state'='BLOCKED' order by ordinal limit 1`, []any{id}, &out.Hint.Reason)
	} else if out.Onboarding && c.Ready > 0 {
		err = scanSummaryRow(ctx, tx, &out, `select request_id::text from ouf_mcp.analysis_column where analysis_id=$1 and outcome->>'state'='CANDIDATES_READY' order by ordinal limit 1`, []any{id}, &out.Hint.RequestID)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
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

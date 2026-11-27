package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	manifestpkg "github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/jackc/pgx/v5"
)

func publishedMetadata(ctx context.Context, tx pgx.Tx, v *analysis.View) error {
	return tx.QueryRow(ctx, `select coalesce(p.seed_digest::text,''),coalesce(p.visibility_digest::text,''),coalesce(p.compatibility_reason,''),coalesce(p.stop_reason,''),
		coalesce((select profile->>'policyEpoch' from ouf_mcp.profile_analysis where analysis_id=$1),'')
		from (values (1)) dummy(n) left join ouf_mcp.analysis_published_plan p on p.analysis_id=$1`, v.ID).
		Scan(&v.Seed, &v.Visibility, &v.Compatibility, &v.StopReason, &v.Epoch)
}

func (s *Store) StopPublished(ctx context.Context, scope analysis.Scope, id, manifest, visibility string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, _, err = analysisHeader(ctx, tx, scope, id, manifest); err != nil {
		return err
	}
	mode := analysis.View{ID: id}
	if err = publishedMetadata(ctx, tx, &mode); err != nil {
		return err
	}
	if mode.Visibility != visibility || visibility == "" {
		return analysis.ErrScope
	}
	if err = savePublished(ctx, tx, id, analysis.Task{Code: "AUTHORIZATION_DENIED"}, analysis.Finish{StopScope: true}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func publishedStopped(ctx context.Context, tx pgx.Tx, id string) error {
	var stopped bool
	if err := tx.QueryRow(ctx, `select stop_reason<>'' from ouf_mcp.analysis_published_plan where analysis_id=$1`, id).Scan(&stopped); err != nil {
		return err
	}
	if stopped {
		return analysis.ErrScope
	}
	return nil
}

type publishedProjection struct {
	Ordinal int              `json:"ordinal"`
	Outcome analysis.Outcome `json:"outcome"`
	Next    string           `json:"next_tool"`
}

// Published root batches partition the columns. Once a batch finishes, any
// still dormant descendant in its columns can be discarded. A reproduced
// split will generate its own children from its fresh result.
func discardAwaitingChildren(ctx context.Context, tx pgx.Tx, id string, columns []int) error {
	_, err := tx.Exec(ctx, `delete from ouf_mcp.profile_analysis_task where analysis_id=$1
		and task->>'awaitParent'='true' and ($2::int[] is null or to_jsonb($2::int[]) @> (task->'columns'))`, id, columns)
	return err
}

func savePublished(ctx context.Context, tx pgx.Tx, id string, t analysis.Task, f analysis.Finish) error {
	if f.StopScope {
		if err := discardAwaitingChildren(ctx, tx, id, nil); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `update ouf_mcp.analysis_published_plan set stop_reason=$2 where analysis_id=$1`, id, t.Code)
		if err != nil {
			return err
		}
		return updateOutcomes(ctx, tx, id, `select ordinal,outcome as old,jsonb_build_object('state','BLOCKED','reasonCode',$2::text)
			||jsonb_strip_nulls(jsonb_build_object('requestId',request_id)) as outcome, request_id,''::text as next_tool
			from ouf_mcp.analysis_column where analysis_id=$1 and outcome->>'state'='PENDING'`, t.Code)
	}
	if t.State == "WAITING" {
		return nil
	}
	children := f.Children
	var projections []publishedProjection
	project := func(cols []int, o analysis.Outcome, next string) {
		for _, n := range cols {
			projections = append(projections, publishedProjection{n, o, next})
		}
	}
	if t.State == "HELD" {
		project(t.Columns, analysis.Outcome{State: "BLOCKED", Reason: t.Code}, "")
	} else if t.Code == "PUBLISHED_BATCH_UNAVAILABLE" {
		_, err := tx.Exec(ctx, `update ouf_mcp.analysis_published_plan set compatibility_reason=$2 where analysis_id=$1`, id, t.Code)
		if err != nil {
			return err
		}
	} else if t.Code != "PUBLISHED_BATCH_SPLIT" {
		rows, ok := analysis.AcceptedBatch(t, t.Result)
		if !ok {
			return analysis.ErrInvalid
		}
		var b analysis.Batch
		if err := json.Unmarshal(t.Arguments, &b); err != nil {
			return err
		}
		for n, row := range rows {
			cols := t.Groups[row.ID]
			if row.State == "MATCH" {
				project(cols, analysis.Outcome{State: "MATCHED_PUBLISHED", Match: &row.Hits[0]}, "")
			} else if b.Intents[n].Type == "DATATYPE" {
				project(cols, analysis.Outcome{State: "BLOCKED", Reason: "DISCOVERY_TYPE_UNSUPPORTED"}, "")
			} else {
				children = append(children, t.ResidualTasks(id, b.Intents[n], false)...)
			}
		}
	}
	if len(children) > 0 {
		for index := range children {
			child := &children[index]
			child.Visibility, child.Epoch = t.Visibility, t.Epoch
			if child.State == "HELD" {
				project(child.Columns, analysis.Outcome{State: "BLOCKED", Reason: child.Code}, "")
			}
		}
		body, err := json.Marshal(children)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `with added as (
		  insert into ouf_mcp.profile_analysis_task as existing(analysis_id,task_id,task)
		  select $1,child->>'taskId',child from jsonb_array_elements($2::jsonb) with ordinality as v(child,n) order by n
		  on conflict (analysis_id,task_id) do update set task=excluded.task
		  where existing.task->>'awaitParent'='true'
		  returning task_id,task
		), assignments as (
		  select task_id,n::int as ordinal,(task->>'tool'=$4) as published_first from added cross join lateral jsonb_array_elements_text(task->'columns') cols(n)
		  where task->>'state'='READY'
		) update ouf_mcp.analysis_column c set task_id=a.task_id,published_first=a.published_first from assignments a
		where c.analysis_id=$1 and c.ordinal=a.ordinal and c.task_id=$3 and c.outcome->>'state'='PENDING'`, id, body, t.ID, analysis.BatchTool)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `update ouf_mcp.profile_analysis set revision=revision+1 where analysis_id=$1`, id); err != nil {
			return err
		}
	}
	if err := discardAwaitingChildren(ctx, tx, id, t.Columns); err != nil {
		return err
	}
	if len(projections) == 0 {
		return nil
	}
	raw, err := json.Marshal(projections)
	if err != nil {
		return err
	}
	return updateOutcomes(ctx, tx, id, `select c.ordinal,c.outcome as old,p.outcome,c.request_id,p.next_tool
		from jsonb_to_recordset($2::jsonb) as p(ordinal int,outcome jsonb,next_tool text)
		join ouf_mcp.analysis_column c on c.analysis_id=$1 and c.ordinal=p.ordinal`, raw)
}

// Only an enrolled T1 plan can replace a legacy next. The caller's scope,
// current manifest and optional discovery receipt must all identify that plan.
func (s *Store) PublishedHint(ctx context.Context, scope analysis.Scope, id, manifest, request string) (analysis.Summary, bool, error) {
	var out analysis.Summary
	var bound bool
	var timing []byte
	var stepRequest *string
	err := s.pool.QueryRow(ctx, `select m.visibility_digest,h.total,h.pending,h.blocked,h.matched,h.ready,h.zero_count,h.stopped,
	coalesce(c.next_tool,''),coalesce(step.task_id,c.task_id::text,''),coalesce(c.request_id::text,''),m.stop_reason,coalesce(p.profile->>'policyEpoch',''),
	`+summaryTimingSQL+`,step.request_id,coalesce(c.ordinal,0),
	($6='' or exists(select from ouf_mcp.analysis_column where analysis_id=p.analysis_id and request_id=nullif($6,'')::uuid))
	from ouf_mcp.analysis_published_plan m join ouf_mcp.profile_analysis p using(analysis_id)
	cross join lateral (select * from ouf_mcp.analysis_counts where analysis_id=p.analysis_id order by seq desc limit 1) h
	left join lateral (`+liveSummaryColumnSQL+`) c on true `+liveSummaryTimingJoins+`
	where p.analysis_id=$1 and p.service_principal_id=$2 and p.principal_id=$3 and p.tenant_id=$4 and p.manifest_checksum=any($5::char(64)[])`,
		id, scope.Service, scope.Principal, scope.Tenant, manifestpkg.AnalysisManifests(manifest), request).Scan(&out.Visibility, &out.Counts.Total, &out.Counts.Pending, &out.Counts.Blocked,
		&out.Counts.Matched, &out.Counts.Ready, &out.Counts.Zero, &out.Stopped, &out.Hint.Tool, &out.Hint.TaskID, &out.Hint.RequestID, &out.Hint.Reason, &out.Epoch, &timing, &stepRequest, &out.Hint.Ordinal, &bound)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, false, nil
	}
	if err == nil && !bound {
		err = analysis.ErrScope
	}
	if err == nil {
		err = mergeSummaryTiming(ctx, s.pool, scope, &out.Hint, timing, stepRequest)
	}
	return out, true, err
}

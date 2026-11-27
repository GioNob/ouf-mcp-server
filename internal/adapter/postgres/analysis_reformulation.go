package postgres

import (
	"context"
	"errors"
	"slices"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/jackc/pgx/v5"
)

// Count discovery tasks after the first for each ordinal. Epoch replans copy
// ordinary inputs with new dispatch keys. Count each copied input once across
// epochs, retaining multiplicity within a plan and every explicit replan task.
const reformulationHistory = `, discovery as (
	select t.analysis_id,t.ordinal as position,col.n::int as column_number,
	case when t.task->>'reformulated'='true' then to_jsonb(t.task_id)
	else (t.task->'arguments')-'idempotencyKey' end as intent
	from budget_profile b join ouf_mcp.profile_analysis p using(service_principal_id,principal_id,tenant_id)
	join ouf_mcp.profile_analysis_task t using(analysis_id)
	cross join lateral jsonb_array_elements_text(t.task->'columns') col(n)
	where p.profile->>'assetId'=b.asset and p.profile->>'profileId'=b.profile_id and t.task->>'tool'='semantic.discovery.request'
), numbered as (
	select *,row_number() over(partition by analysis_id,column_number order by position) as task_number
	from discovery
), copies as (
	select column_number,intent,count(*) as copies from numbered where task_number>1
	group by column_number,intent,analysis_id
), retained as (
	select column_number,max(copies) as uses from copies group by column_number,intent
) select column_number,sum(uses)::int as used from retained group by column_number`

func lockProfileBudget(ctx context.Context, tx pgx.Tx, p analysis.Plan) error {
	_, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended($1,0))`,
		analysis.Digest([]any{"column-reformulations", p.Scope, p.Profile.AssetID, p.Profile.ProfileID}))
	return err
}

// Both the hint and the command use the same request binding and finite budget.
// An empty live page can enable a reformulation without certifying zero at completion.
const reformulationColumns = `select c.ordinal,c.task_id,coalesce(history.used,0) from ouf_mcp.analysis_column c
	join ouf_mcp.profile_analysis p0 using(analysis_id)
	left join ouf_mcp.analysis_published_plan pub using(analysis_id)
	left join lateral (with budget_profile as (select p0.service_principal_id,p0.principal_id,p0.tenant_id,
	 p0.profile->>'assetId' as asset,p0.profile->>'profileId' as profile_id)` + reformulationHistory + `) history on history.column_number=c.ordinal
	where p0.analysis_id=$1 and p0.service_principal_id=$2 and p0.principal_id=$3 and p0.tenant_id=$4
	 and p0.manifest_checksum=any($5::char(64)[]) and c.request_id=$6 and c.task_id is not null
	 and c.empty_candidate_page and c.outcome->>'reasonCode' in ('DISCOVERY_DECISION_REQUIRED','DISCOVERY_EVIDENCE_UNAVAILABLE')
	 and coalesce(history.used,0)<$7 and coalesce(pub.stop_reason,'')=''
	 and not (select integrity_hold from ouf_mcp.analysis_counts where analysis_id=p0.analysis_id order by seq desc limit 1)`

func (s *Store) ReformulationColumn(ctx context.Context, scope analysis.Scope, id, checksum, request string, limit int) (int, bool, error) {
	var ordinal, used int
	var task string
	err := s.pool.QueryRow(ctx, reformulationColumns+" order by c.ordinal limit 1",
		id, scope.Service, scope.Principal, scope.Tenant, manifest.AnalysisManifests(checksum), request, limit).Scan(&ordinal, &task, &used)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return ordinal, err == nil, err
}

// The analysis header serializes membership changes. A replay or extending a
// discovery to a previously uncovered ordinal consumes no reformulation.
func reservePlanReformulations(ctx context.Context, tx pgx.Tx, p analysis.Plan, existing []analysis.Task) error {
	counts := make([]int, p.Profile.ColumnCount)
	prior := map[string][]int{}
	for _, t := range existing {
		if t.Tool == "semantic.discovery.request" {
			prior[t.ID] = t.Columns
			for _, n := range t.Columns {
				if n < 0 || n >= len(counts) {
					return analysis.ErrInvalid
				}
				counts[n]++
			}
		}
	}
	var used map[int]int
	for _, t := range p.Tasks {
		if t.Tool != "semantic.discovery.request" || t.Reformulated {
			continue
		}
		for _, n := range t.Columns {
			if n < 0 || n >= len(counts) {
				return analysis.ErrInvalid
			}
			if slices.Contains(prior[t.ID], n) {
				continue
			}
			if counts[n] > 0 {
				if used == nil {
					var err error
					used, err = profileUsage(ctx, tx, p)
					if err != nil {
						return err
					}
				}
				ok, err := takeReformulation(ctx, tx, p, n, used[n], p.MaxColumnReformulations)
				if err != nil {
					return err
				}
				used[n]++
				if !ok {
					// Roll back the entire normal plan, including prior ordinal charges.
					return analysis.ErrConflict
				}
			}
			counts[n]++
		}
	}
	return nil
}

func profileUsage(ctx context.Context, tx pgx.Tx, p analysis.Plan) (map[int]int, error) {
	if err := lockProfileBudget(ctx, tx, p); err != nil {
		return nil, err
	}
	const query = `with budget_profile as (select $1::text as service_principal_id,$2::text as principal_id,
		$3::text as tenant_id,$4::text as asset,$5::text as profile_id)` + reformulationHistory
	rows, err := tx.Query(ctx, query, p.Scope.Service, p.Scope.Principal, p.Scope.Tenant, p.Profile.AssetID, p.Profile.ProfileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	used := make(map[int]int)
	for rows.Next() {
		var ordinal, count int
		if err := rows.Scan(&ordinal, &count); err != nil {
			return nil, err
		}
		used[ordinal] = count
	}
	return used, rows.Err()
}

func takeReformulation(ctx context.Context, tx pgx.Tx, p analysis.Plan, ordinal, previouslyUsed, limit int) (bool, error) {
	if previouslyUsed >= limit || limit > 16 {
		return false, nil
	}
	tag, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_reformulation_budget as b
		(service_principal_id,principal_id,tenant_id,asset_id,profile_id,ordinal,used)
		values($1,$2,$3,$4,$5,$6,$7) on conflict(service_principal_id,principal_id,tenant_id,asset_id,profile_id,ordinal)
		do update set used=excluded.used`, p.Scope.Service, p.Scope.Principal, p.Scope.Tenant,
		p.Profile.AssetID, p.Profile.ProfileID, ordinal, previouslyUsed+1)
	return tag.RowsAffected() == 1, err
}

func (s *Store) Reformulate(ctx context.Context, p analysis.Plan, r analysis.Reformulation, limit int, build func(analysis.Task) (analysis.Task, error)) (analysis.View, error) {
	var out analysis.View
	return s.planCommand(ctx, p.Scope, p.Manifest, p.CommandKey, p.CommandDigest, func(tx pgx.Tx) (analysis.View, error) {
		profile, rev, err := analysisHeader(ctx, tx, p.Scope, p.ID, p.Manifest)
		if err != nil {
			return out, err
		}
		if profile != p.Profile {
			return out, analysis.ErrProfile
		}
		if err = lockProfileBudget(ctx, tx, p); err != nil {
			return out, err
		}
		var ordinal, used int
		var taskID string
		err = tx.QueryRow(ctx, reformulationColumns+" and c.ordinal=$8", p.ID, p.Scope.Service, p.Scope.Principal,
			p.Scope.Tenant, manifest.AnalysisManifests(p.Manifest), r.RequestID, limit, r.Column).Scan(&ordinal, &taskID, &used)
		if errors.Is(err, pgx.ErrNoRows) {
			return out, analysis.ErrReplanState
		}
		if err != nil {
			return out, err
		}
		previous, err := readAnalysisTask(ctx, tx, p.ID, taskID)
		if err != nil {
			return out, err
		}
		task, err := build(previous)
		if err != nil {
			return out, err
		}
		ok, err := takeReformulation(ctx, tx, p, ordinal, used, limit)
		if err != nil {
			return out, err
		}
		if !ok {
			return out, analysis.ErrReplanState
		}
		// The plan header fences the current request, summary snapshot and
		// insertion of the replacement task. Prior versions are immutable.
		_, err = tx.Exec(ctx, `with head as (select * from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1),
			old as (select outcome from ouf_mcp.analysis_column where analysis_id=$1 and ordinal=$2),
			reopened as (update ouf_mcp.analysis_column set request_id=null,task_id=null,next_tool='source.analysis.plan',
				empty_candidate_page=false,proposal_choice=null,exclusion_choice=null,decision_attempt=null,outcome='{"state":"PENDING"}' where analysis_id=$1 and ordinal=$2 returning outcome),
			version as (insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
				select $1,$2,head.seq+1,reopened.outcome from head,reopened)
			insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped)
			select $1,seq+1,total,matched,ready,zero_count-((old.outcome->>'state')='NO_CANDIDATES')::int,
				blocked-((old.outcome->>'state')='BLOCKED')::int,pending+1,onboarding,false from head,old`, p.ID, ordinal)
		if err != nil {
			return out, err
		}
		p.Tasks, p.ExpectedRevision = []analysis.Task{task}, rev
		return planAnalysis(ctx, tx, p)
	})
}

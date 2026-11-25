package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Only an explicit dependency code establishes a common cause. Preview
// ordinals have no known semantic group until the profile has been read.
func recordDependency(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, item analysis.RunItem, receipt RunReceipt, now time.Time) error {
	dependency := "semantic"
	if item.Task.Tool == "source.file.preview" {
		dependency = "onboarding"
	}
	cause := receipt.DependencyCause
	if cause != "" {
		switch cause {
		case "SEM_BATCH_RATE_LIMITED", "SEM_BATCH_UNAVAILABLE":
			if item.Task.Tool != analysis.BatchTool {
				return analysis.ErrInvalid
			}
		case "ANALYSIS_AUTHORITY_UNAVAILABLE":
			dependency = "gateway"
		default:
			// Local pressure or an unspecified backoff has no proven shared cause.
			return nil
		}
	} else if receipt.Code != "" {
		return nil
	}
	scope := analysis.Digest([]string{r.Issuer, r.Workload, r.Scope.Service, r.Scope.Tenant})
	rateScope := analysis.Digest([]string{scope, r.Scope.Principal})
	if cause == "SEM_BATCH_RATE_LIMITED" {
		scope = rateScope
	}
	groups := item.Task.Groups
	if len(groups) == 0 {
		group := item.Intent
		if item.Task.Tool == "source.file.preview" {
			group = ""
		}
		groups = map[string][]int{group: item.Task.Columns}
	}
	if cause != "" {
		var incident string
		err := tx.QueryRow(ctx, `insert into ouf_mcp.analysis_dependency_incident(incident_id,scope_key,dependency,cause,opened_at)
			values($1,$2,$3,$4,$5) on conflict(scope_key,dependency,cause) where resolved_at is null
			do update set scope_key=excluded.scope_key returning incident_id`, uuid.NewString(), scope, dependency, cause, now).Scan(&incident)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(groups)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_dependency_impact(incident_id,analysis_id,group_key,ordinal)
			select $1,$2,g.key,n::int from jsonb_each($3::jsonb) g cross join lateral jsonb_array_elements_text(g.value) n
			on conflict(incident_id,analysis_id,group_key,ordinal) do update set recovered_at=null`, incident, r.ID(), raw)
		return err
	}
	// A verified dispatch traversed the Gateway and its named owner. Lock
	// incidents in stable order before changing impact or declaring recovery.
	rows, err := tx.Query(ctx, `select i.incident_id from ouf_mcp.analysis_dependency_incident i
		where i.scope_key in ($1,$5) and i.dependency in ($2,'gateway') and i.resolved_at is null and exists
		(select 1 from ouf_mcp.analysis_dependency_impact a where a.incident_id=i.incident_id and a.analysis_id=$3
		and a.recovered_at is null and a.ordinal=any($4::int[])) order by i.incident_id for update`, scope, dependency, r.ID(), item.Task.Columns, rateScope)
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, id := range ids {
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_dependency_impact set recovered_at=$3 where incident_id=$1 and analysis_id=$2 and ordinal=any($4::int[]) and recovered_at is null", id, r.ID(), now, item.Task.Columns)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `update ouf_mcp.analysis_dependency_incident set resolved_at=$2 where incident_id=$1
			and not exists(select 1 from ouf_mcp.analysis_dependency_impact where incident_id=$1 and recovered_at is null)`, id, now)
		if err != nil {
			return err
		}
	}
	return nil
}

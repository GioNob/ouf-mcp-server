package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A predecessor batch decision was checked against visibility before dispatch.
// Its admission receipt records bundleId:version:capability even for a generic
// 403. A change in roles alone cannot establish a different policy epoch.
func predecessorEpoch(ctx context.Context, tx pgx.Tx, source string) (string, error) {
	var ref string
	err := tx.QueryRow(ctx, `select a.authorization_decision_ref
		from ouf_mcp.profile_analysis_task t join ouf_mcp.attempt_admission_context a
		on a.attempt_id=nullif(t.task->>'attemptId','')::uuid
		where t.analysis_id=$1 and t.task->>'tool'='semantic.search.batch' and (t.task->>'calls')::int>0
		order by t.ordinal limit 1`, source).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	pair, ok := strings.CutSuffix(ref, ":ouf.semantic.search")
	pos := strings.LastIndexByte(pair, ':')
	if !ok || pos <= 0 {
		return "", nil
	}
	version, err := strconv.ParseInt(pair[pos+1:], 10, 64)
	if err != nil || version < 1 {
		return "", nil
	}
	return analysis.Digest([]any{pair[:pos], version}), nil
}

func (s *Store) ReplanAnalysis(ctx context.Context, command analysis.ReplanCommand, build func(*analysis.Plan, []analysis.Task) error) (analysis.View, error) {
	intent := analysis.Digest(map[string]string{"replanOf": command.Source})
	return s.planCommand(ctx, command.Scope, command.Manifest, command.Key, intent, func(tx pgx.Tx) (analysis.View, error) {
		return cloneAnalysis(ctx, tx, command, build)
	})
}

func (s *Store) planCommand(ctx context.Context, scope analysis.Scope, checksum, key, intent string, run func(pgx.Tx) (analysis.View, error)) (analysis.View, error) {
	var out analysis.View
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	// Serialize commands even when two concurrent callers name different sources.
	if _, err = tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended($1,0))`, analysis.Digest([]any{scope, key})); err != nil {
		return out, err
	}
	target, found, err := planReceipt(ctx, tx, scope, key, intent)
	if err != nil {
		return out, err
	}
	if found {
		if err = tx.Commit(ctx); err != nil {
			return out, err
		}
		return s.ReadAnalysis(ctx, scope, target, checksum, 0, 128)
	}
	out, resultErr := run(tx)
	state, code := "SUCCEEDED", "SUCCEEDED"
	if resultErr != nil {
		if !errors.Is(resultErr, analysis.ErrPolicyUnchanged) && !errors.Is(resultErr, analysis.ErrReplanState) && !errors.Is(resultErr, analysis.ErrNotFound) {
			return out, resultErr
		}
		state, code = "FAILED", resultErr.Error()
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.tool_attempt
		(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,
		 idempotency_key,request_hash,correlation_id,state,dispatch_state,completed_at,outcome_code,result_ref)
		values($1,$2,$3,$4,'ouf.source.analysis.plan','COMMAND',$5,$6,$7,$8,$9,'NOT_DISPATCHED',transaction_timestamp(),$10,nullif($11,''))`,
		uuid.New(), scope.Service, scope.Principal, scope.Tenant, checksum, key, intent, uuid.New(), state, code, out.ID)
	if err != nil {
		return analysis.View{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return analysis.View{}, err
	}
	return out, resultErr
}

func cloneAnalysis(ctx context.Context, tx pgx.Tx, command analysis.ReplanCommand, build func(*analysis.Plan, []analysis.Task) error) (analysis.View, error) {
	var out analysis.View
	scope, source, checksum := command.Scope, command.Source, command.Manifest
	epoch, visibility := command.Epoch, command.Visibility
	profile, _, err := analysisHeader(ctx, tx, scope, source, checksum)
	if errors.Is(err, analysis.ErrScope) {
		return out, analysis.ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if err = checkAnalysisHold(ctx, tx, source); err != nil {
		return out, err
	}
	old := analysis.View{ID: source}
	if err = publishedMetadata(ctx, tx, &old); err != nil {
		return out, err
	}
	p := analysis.Plan{ID: analysis.AnalysisID(scope, profile, epoch), Scope: scope, Profile: profile, Manifest: checksum, Epoch: epoch,
		Seed: old.Seed, Visibility: visibility, Replan: true}
	var pending, denied, otherHold bool
	err = tx.QueryRow(ctx, `select h.pending>0,h.onboarding,
		exists(select from ouf_mcp.profile_analysis_task where analysis_id=$1 and task->>'state'='HELD'
			and task->>'code'=any($2::text[])),
		exists(select from ouf_mcp.profile_analysis_task where analysis_id=$1 and task->>'state'='HELD'
			and task->>'code'<>all($2::text[]) and task->>'code' not in ('ANALYSIS_POLICY_EPOCH_CHANGED','ANALYSIS_SCOPE_CHANGED')),
		array(select header from ouf_mcp.analysis_column where analysis_id=$1 order by ordinal)
		from ouf_mcp.analysis_counts h where analysis_id=$1 order by seq desc limit 1`, source, analysis.PermissionReasons()).Scan(&pending, &p.Onboarding, &denied, &otherHold, &p.Headers)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, analysis.ErrReplanState
	}
	if err != nil {
		return out, err
	}
	permission := analysis.PermissionHold(old.StopReason) || (old.StopReason == "" && denied && !pending && !otherHold)
	same := p.ID == source
	if source == analysis.AnalysisID(scope, profile) && !same {
		same = old.Visibility != "" && old.Visibility == visibility
		if !same && (command.PreviousVisibility == "" || old.Visibility != command.PreviousVisibility) {
			oldEpoch, err := predecessorEpoch(ctx, tx, source)
			if err != nil {
				return out, err
			}
			if oldEpoch == "" {
				return out, analysis.ErrReplanState
			}
			same = oldEpoch == epoch
		}
	}
	if same {
		if permission {
			return out, analysis.ErrPolicyUnchanged
		}
		return out, analysis.ErrReplanState
	}
	// Only inputs cross the epoch boundary. The source header lock fences
	// task appends and finishes for the duration of the copy.
	rows, err := tx.Query(ctx, `select jsonb_build_object('taskId',task_id,'tool',task->'tool','arguments',task->'arguments',
		'discoveryText',task->'discoveryText','reformulated',task->'reformulated','columns',task->'columns','observation',task->'observation','intentOrdinals',task->'intentOrdinals','ownerIntent',task->'ownerIntent')
		from ouf_mcp.profile_analysis_task where analysis_id=$1 order by ordinal limit $2`, source, analysis.MaxTasks+1)
	if err != nil {
		return out, err
	}
	inputs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (analysis.Task, error) {
		var raw json.RawMessage
		var t analysis.Task
		if err := row.Scan(&raw); err != nil {
			return t, err
		}
		err := json.Unmarshal(raw, &t)
		return t, err
	})
	if err != nil {
		return out, err
	}
	if err = build(&p, inputs); err != nil {
		return out, err
	}
	return planAnalysis(ctx, tx, p)
}

type planReceiptReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func planReceipt(ctx context.Context, q planReceiptReader, scope analysis.Scope, key, intent string) (string, bool, error) {
	var hash, code, target string
	err := q.QueryRow(ctx, `select request_hash,outcome_code,coalesce(result_ref,'') from ouf_mcp.tool_attempt
		where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and capability_id='ouf.source.analysis.plan'
		and operation_class='COMMAND' and idempotency_key=$4`, scope.Service, scope.Principal, scope.Tenant, key).Scan(&hash, &code, &target)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if intent != hash {
		return "", true, analysis.ErrKeyReuse
	}
	switch code {
	case "SUCCEEDED":
		return target, true, nil
	case analysis.ErrNotFound.Error():
		return "", true, analysis.ErrNotFound
	case analysis.ErrReplanState.Error():
		return "", true, analysis.ErrReplanState
	case analysis.ErrPolicyUnchanged.Error():
		return "", true, analysis.ErrPolicyUnchanged
	default:
		return "", true, analysis.ErrConflict
	}
}

func (s *Store) LookupPlanCommand(ctx context.Context, scope analysis.Scope, checksum, key, intent string) (analysis.View, bool, error) {
	target, found, err := planReceipt(ctx, s.pool, scope, key, intent)
	if !found || err != nil {
		return analysis.View{}, found, err
	}
	view, err := s.ReadAnalysis(ctx, scope, target, checksum, 0, 128)
	return view, true, err
}

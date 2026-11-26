package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	manifestpkg "github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func analysisHeader(ctx context.Context, tx pgx.Tx, scope analysis.Scope, id, manifest string) (analysis.Profile, int64, error) {
	var p analysis.Profile
	var raw []byte
	var rev int64
	var stored string
	err := tx.QueryRow(ctx, `select profile,revision,manifest_checksum from ouf_mcp.profile_analysis where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 for update`, id, scope.Service, scope.Principal, scope.Tenant).Scan(&raw, &rev, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, 0, analysis.ErrScope
	}
	if err != nil {
		return p, 0, err
	}
	if !slices.Contains(manifestpkg.AnalysisManifests(manifest), stored) {
		return p, 0, analysis.ErrScope
	}
	if json.Unmarshal(raw, &p) != nil {
		return p, 0, analysis.ErrScope
	}
	return p, rev, nil
}

// Project the same request clock in a plan page and under the claim's header
// lock. The predecessor may be outside the page returned to the client.
const plannedReadClocks = ` from ouf_mcp.profile_analysis_task t
	left join ouf_mcp.analysis_owner_due w on w.analysis_id=t.analysis_id
		and w.request_id=case when t.task->>'tool' in ('semantic.discovery.status','semantic.discovery.candidates')
			then (t.task->'arguments'->>'requestId')::uuid end
	left join ouf_mcp.profile_analysis_task prior on prior.analysis_id=t.analysis_id
		and t.task->>'tool'='semantic.discovery.status' and (t.task->>'observation')::int>1
		and prior.task->>'tool'=t.task->>'tool' and prior.task->'arguments'=t.task->'arguments'
		and (prior.task->>'observation')::int=(t.task->>'observation')::int-1
		and prior.task->>'state'='SUCCEEDED' `

const plannedReadDue = `greatest(w.not_before,(prior.task->>'notBefore')::timestamptz,
	(prior.task->>'completedAt')::timestamptz + interval '31 seconds')`

func analysisTasks(ctx context.Context, tx pgx.Tx, id string) ([]analysis.Task, error) {
	// Never load potentially large evidence bodies or intent arguments for totals.
	rows, err := tx.Query(ctx, `select t.task-'discoveryText'-'result'-'arguments'-'intentOrdinals'-'visibilityDigest'-'ownerKey'-'policyEpoch'-'ownerIntent',
		`+plannedReadDue+plannedReadClocks+`where t.analysis_id=$1 order by t.ordinal`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := []analysis.Task{}
	for rows.Next() {
		var raw []byte
		var t analysis.Task
		var due *time.Time
		if err = rows.Scan(&raw, &due); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &t); err != nil {
			return nil, err
		}
		if due != nil && due.After(t.NotBefore) && (t.State == "READY" || t.State == "WAITING") {
			t.NotBefore = *due
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}
func analysisView(id string, p analysis.Profile, rev int64, tasks []analysis.Task, offset, limit int) analysis.View {
	tasks = slices.DeleteFunc(tasks, func(t analysis.Task) bool { return t.AwaitParent })
	budget, covered, complete := analysis.Calculate(tasks)
	end := min(len(tasks), offset+limit)
	start := min(offset, len(tasks))
	return analysis.View{ID: id, Revision: rev, Profile: p, Budget: budget, Tasks: tasks[start:end], Offset: offset, HasMore: end < len(tasks), CoveredColumns: covered, EvidenceColumns: complete}
}
func (s *Store) PlanAnalysis(ctx context.Context, p analysis.Plan) (analysis.View, error) {
	if p.CommandKey != "" {
		return s.planCommand(ctx, p.Scope, p.Manifest, p.CommandKey, p.CommandDigest, func(tx pgx.Tx) (analysis.View, error) {
			return planAnalysis(ctx, tx, p)
		})
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return analysis.View{}, e
	}
	defer tx.Rollback(ctx)
	v, e := planAnalysis(ctx, tx, p)
	if e != nil {
		return v, e
	}
	return v, tx.Commit(ctx)
}

func planAnalysis(ctx context.Context, tx pgx.Tx, p analysis.Plan) (analysis.View, error) {
	if !p.Scope.Valid() || !p.Profile.Valid() || (p.Visibility == "" && (len(p.Tasks) < 1 || (len(p.Tasks) > analysis.MaxPlanBatch && !p.Replan && p.Tasks[0].Compatibility == ""))) || len(p.Tasks) > analysis.MaxTasks || p.ID != analysis.AnalysisID(p.Scope, p.Profile, p.Epoch) {
		return analysis.View{}, analysis.ErrInvalid
	}
	raw, _ := json.Marshal(struct {
		analysis.Profile
		Epoch string `json:"policyEpoch,omitempty"`
	}{p.Profile, p.Epoch})
	_, e := tx.Exec(ctx, `insert into ouf_mcp.profile_analysis(analysis_id,service_principal_id,principal_id,tenant_id,profile,manifest_checksum) values($1,$2,$3,$4,$5,$6) on conflict do nothing`, p.ID, p.Scope.Service, p.Scope.Principal, p.Scope.Tenant, raw, p.Manifest)
	if e != nil {
		return analysis.View{}, e
	}
	existing, rev, e := analysisHeader(ctx, tx, p.Scope, p.ID, p.Manifest)
	if e != nil {
		return analysis.View{}, e
	}
	if existing != p.Profile {
		return analysis.View{}, holdAnalysis(ctx, tx, p.ID)
	}
	if e = checkAnalysisHold(ctx, tx, p.ID); e != nil {
		return analysis.View{}, e
	}
	mode := analysis.View{ID: p.ID}
	if e = publishedMetadata(ctx, tx, &mode); e != nil {
		return analysis.View{}, e
	}
	if !p.Replan && analysis.PermissionHold(mode.StopReason) {
		return analysis.View{}, analysis.ErrPolicyUnchanged
	}
	if mode.Seed != "" && (mode.Seed != p.Seed || mode.Visibility != p.Visibility) {
		return analysis.View{}, analysis.ErrConflict
	}
	if mode.Seed == "" && p.Seed != "" {
		if rev != 0 {
			return analysis.View{}, analysis.ErrConflict
		}
		_, e = tx.Exec(ctx, `insert into ouf_mcp.analysis_published_plan(analysis_id,seed_digest,visibility_digest) values($1,$2,$3)`, p.ID, p.Seed, p.Visibility)
		if e != nil {
			return analysis.View{}, e
		}
	}
	tasks, e := analysisTasks(ctx, tx, p.ID)
	if e != nil {
		return analysis.View{}, e
	}
	if !p.Replan {
		if err := reservePlanReformulations(ctx, tx, p, tasks); err != nil {
			return analysis.View{}, err
		}
	}
	indices := map[string]int{}
	for n, t := range tasks {
		indices[t.ID] = n
	}
	changes := []analysis.Task{}
	newCount := 0
	for _, t := range p.Tasks {
		if t.AwaitParent {
			if rev != 0 {
				// The target already owns its child set. A later replan command
				// must not recreate inputs discarded by a completed parent.
				continue
			}
			t.State = "AWAITING_PARENT"
		}
		t.Visibility, t.Epoch = p.Visibility, p.Epoch
		if n, found := indices[t.ID]; found {
			merged := analysis.Union(tasks[n].Columns, t.Columns)
			if len(merged) != len(tasks[n].Columns) {
				tasks[n].Columns = merged
				changes = append(changes, tasks[n])
			}
		} else {
			newCount++
			changes = append(changes, t)
		}
	}
	if len(tasks)+newCount > analysis.MaxTasks {
		return analysis.View{}, analysis.ErrCapacity
	}
	if len(changes) > 0 && rev != p.ExpectedRevision {
		return analysis.View{}, analysis.ErrConflict
	}
	if len(p.Headers) > 0 {
		if e = createAnalysisSummary(ctx, tx, p); e != nil {
			return analysis.View{}, e
		}
	}
	var retainedRequests []string
	for _, t := range changes {
		if t.Tool == "semantic.discovery.request" && t.State == "SUCCEEDED" {
			retainedRequests = append(retainedRequests, t.ID)
		}
		if _, exists := indices[t.ID]; exists {
			columns, _ := json.Marshal(t.Columns)
			_, e = tx.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(task,'{columns}',$3::jsonb) where analysis_id=$1 and task_id=$2`, p.ID, t.ID, columns)
		} else {
			b, _ := json.Marshal(t)
			_, e = tx.Exec(ctx, `insert into ouf_mcp.profile_analysis_task(analysis_id,task_id,task) values($1,$2,$3)`, p.ID, t.ID, b)
		}
		if e != nil {
			return analysis.View{}, e
		}
		if t.AwaitParent {
			continue
		}
		if t.State == "HELD" && t.Code == "DISCOVERY_TYPE_UNSUPPORTED" {
			if e = projectOutcome(ctx, tx, p.ID, t.Columns, "", analysis.Outcome{State: "BLOCKED", Reason: t.Code}, ""); e != nil {
				return analysis.View{}, e
			}
		}
		if t.Tool == analysis.BatchTool && t.State == "HELD" {
			if e = savePublished(ctx, tx, p.ID, t, analysis.Finish{}); e != nil {
				return analysis.View{}, e
			}
		}
		if e = assignSummaryTask(ctx, tx, p.ID, t); e != nil {
			return analysis.View{}, e
		}
	}
	if e = inheritRequests(ctx, tx, p.ID, retainedRequests); e != nil {
		return analysis.View{}, e
	}
	if len(changes) > 0 {
		rev++
		_, e = tx.Exec(ctx, `update ouf_mcp.profile_analysis set revision=$2 where analysis_id=$1`, p.ID, rev)
		if e != nil {
			return analysis.View{}, e
		}
	}
	tasks, e = analysisTasks(ctx, tx, p.ID)
	if e != nil {
		return analysis.View{}, e
	}
	v := analysisView(p.ID, p.Profile, rev, tasks, 0, 128)
	if e = publishedMetadata(ctx, tx, &v); e != nil {
		return analysis.View{}, e
	}
	return v, nil
}
func (s *Store) ReadAnalysis(ctx context.Context, scope analysis.Scope, id, manifest string, offset, limit int) (analysis.View, error) {
	if !scope.Valid() || analysis.ValidateID(id) != nil || offset < 0 || limit < 1 || limit > 128 {
		return analysis.View{}, analysis.ErrInvalid
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return analysis.View{}, e
	}
	defer tx.Rollback(ctx)
	p, rev, e := analysisHeader(ctx, tx, scope, id, manifest)
	if e != nil {
		return analysis.View{}, e
	}
	tasks, e := analysisTasks(ctx, tx, id)
	if e != nil {
		return analysis.View{}, e
	}
	v := analysisView(id, p, rev, tasks, offset, limit)
	if e = publishedMetadata(ctx, tx, &v); e != nil {
		return v, e
	}
	if v.Visibility != "" && v.Epoch == "" {
		v.Epoch, e = predecessorEpoch(ctx, tx, v.ID)
		if e != nil {
			return v, e
		}
	}
	return v, tx.Commit(ctx)
}
func readAnalysisTask(ctx context.Context, tx pgx.Tx, id, taskID string) (analysis.Task, error) {
	var t analysis.Task
	var raw []byte
	var fence *string
	e := tx.QueryRow(ctx, `select task,fence::text from ouf_mcp.profile_analysis_task
		where analysis_id=$1 and task_id=$2 and task->>'awaitParent' is distinct from 'true' for update`, id, taskID).Scan(&raw, &fence)
	if errors.Is(e, pgx.ErrNoRows) {
		return t, analysis.ErrScope
	}
	if e != nil {
		return t, e
	}
	e = json.Unmarshal(raw, &t)
	if fence != nil {
		t.Fence = *fence
	}
	return t, e
}
func saveAnalysisTask(ctx context.Context, tx pgx.Tx, id string, t analysis.Task) error {
	raw, e := json.Marshal(t)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=$3,fence=nullif($4,'')::uuid where analysis_id=$1 and task_id=$2`, id, t.ID, raw, t.Fence)
	return e
}
func (s *Store) ReadAnalysisTask(ctx context.Context, scope analysis.Scope, id, taskID, manifest string) (analysis.Task, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return analysis.Task{}, e
	}
	defer tx.Rollback(ctx)
	if _, _, e = analysisHeader(ctx, tx, scope, id, manifest); e != nil {
		return analysis.Task{}, e
	}
	t, e := readAnalysisTask(ctx, tx, id, taskID)
	if e != nil {
		return t, e
	}
	t.Fence = ""
	return t, tx.Commit(ctx)
}
func (s *Store) ClaimAnalysisTask(ctx context.Context, scope analysis.Scope, id, taskID, manifest string) (analysis.Task, error) {
	var t analysis.Task
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return t, e
	}
	defer tx.Rollback(ctx)
	if _, _, e = analysisHeader(ctx, tx, scope, id, manifest); e != nil {
		return t, e
	}
	if e = checkAnalysisHold(ctx, tx, id); e != nil {
		return t, e
	}
	t, e = readAnalysisTask(ctx, tx, id, taskID)
	if e != nil {
		return t, e
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "select transaction_timestamp()").Scan(&now); e != nil {
		return t, e
	}
	if t.Visibility != "" {
		if err := publishedStopped(ctx, tx, id); err != nil {
			return t, err
		}
	}
	if t.State == "SUCCEEDED" || t.State == "HELD" {
		return t, tx.Commit(ctx)
	}
	if t.State == "READY" {
		var allInvalid bool
		e = tx.QueryRow(ctx, `select coalesce(bool_and(header=''),false)
			from ouf_mcp.analysis_column where analysis_id=$1 and ordinal=any($2::int[])`, id, t.Columns).Scan(&allInvalid)
		if e != nil {
			return t, e
		}
		if allInvalid {
			t.State, t.Code = "HELD", "ANALYSIS_COLUMN_INVALID"
			if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
				return t, e
			}
			if t.Tool == analysis.BatchTool {
				if e = discardAwaitingChildren(ctx, tx, id, t.Columns); e != nil {
					return t, e
				}
			}
			return t, tx.Commit(ctx)
		}
	}
	if t.State == "RUNNING" {
		if !now.Before(t.LeaseUntil) {
			t.State = "HELD"
			t.Code = "PREVIOUS_OUTCOME_REQUIRES_RECONCILIATION"
			t.Fence = uuid.NewString()
			if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
				return t, e
			}
			if e = finishSummary(ctx, tx, id, t, now, orchestration.OwnerObservation{}); e != nil {
				return t, e
			}
		}
		// Never return another worker's fencing token to a concurrent executor.
		t.Fence = ""
		return t, tx.Commit(ctx)
	}
	if t.Tool == "semantic.discovery.status" || t.Tool == "semantic.discovery.candidates" {
		var due *time.Time
		var predecessorReady bool
		if e = tx.QueryRow(ctx, `select `+plannedReadDue+`,prior.task_id is not null`+plannedReadClocks+
			`where t.analysis_id=$1 and t.task_id=$2`, id, taskID).Scan(&due, &predecessorReady); e != nil {
			return t, e
		}
		if t.Observation > 1 && !predecessorReady {
			return t, analysis.ErrConflict
		}
		if due != nil && due.After(now) && due.After(t.NotBefore) {
			t.State, t.Code, t.NotBefore = "WAITING", "OBSERVATION_INTERVAL", *due
			if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
				return t, e
			}
		}
	}
	if now.Before(t.NotBefore) {
		return t, tx.Commit(ctx)
	}
	if t.Attempts >= analysis.MaxRetryAttempts || (!t.FirstAttemptAt.IsZero() && now.Add(time.Duration(t.MaxTimeMS)*time.Millisecond).Sub(t.FirstAttemptAt) >= 24*time.Hour) {
		t.State = "HELD"
		t.Code = "RETRY_BUDGET_EXHAUSTED"
		if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
			return t, e
		}
		if e = finishSummary(ctx, tx, id, t, now, orchestration.OwnerObservation{}); e != nil {
			return t, e
		}
		return t, tx.Commit(ctx)
	}
	var retained int64
	if e = tx.QueryRow(ctx, `select coalesce(sum(coalesce((task->>'resultBytes')::bigint,0)+case when task->>'state'='RUNNING' then (task->>'maxBytes')::bigint else 0 end),0) from ouf_mcp.profile_analysis_task where analysis_id=$1`, id).Scan(&retained); e != nil {
		return t, e
	}
	if retained+t.MaxBytes > analysis.MaxRetainedBytes {
		return t, analysis.ErrCapacity
	}
	t.State = "RUNNING"
	t.Code = ""
	t.Dimension = ""
	t.Attempts++
	if t.FirstAttemptAt.IsZero() {
		t.FirstAttemptAt = now
	}
	t.LeaseUntil = now.Add(time.Duration(t.MaxTimeMS)*time.Millisecond + 15*time.Second)
	t.Fence = uuid.NewString()
	if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
		return t, e
	}
	return t, tx.Commit(ctx)
}
func (s *Store) FinishAnalysisTask(ctx context.Context, scope analysis.Scope, id, taskID, fence string, f analysis.Finish) (analysis.Task, error) {
	var t analysis.Task
	if f.State != "SUCCEEDED" && f.State != "WAITING" && f.State != "HELD" {
		return t, analysis.ErrInvalid
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return t, e
	}
	defer tx.Rollback(ctx)
	// Ownership is independent of catalog churn while the already dispatched
	// worker records an outcome. A new execution still requires the exact manifest.
	var owned bool
	e = tx.QueryRow(ctx, `select true from ouf_mcp.profile_analysis where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 for update`, id, scope.Service, scope.Principal, scope.Tenant).Scan(&owned)
	if e != nil {
		return t, analysis.ErrScope
	}
	t, e = readAnalysisTask(ctx, tx, id, taskID)
	if e != nil {
		return t, e
	}
	if t.Visibility != "" {
		if err := publishedStopped(ctx, tx, id); err != nil {
			return t, err
		}
	}
	if fence == "" || t.Fence != fence || t.State != "RUNNING" {
		return t, analysis.ErrFenced
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "select transaction_timestamp()").Scan(&now); e != nil {
		return t, e
	}
	if !now.Before(t.LeaseUntil) {
		return t, analysis.ErrFenced
	}
	if len(f.Body) > int(t.MaxBytes) || f.Calls < 0 || f.Calls > 1 || f.ElapsedMS < 0 {
		return t, analysis.ErrInvalid
	}
	if len(f.Body) > 0 && !json.Valid(f.Body) {
		return t, analysis.ErrInvalid
	}
	if f.WindowWait {
		if f.Calls != 0 || f.State != "WAITING" || f.NotBefore.IsZero() {
			return t, analysis.ErrInvalid
		}
		t.Attempts--
		t.Waits++
	}
	t.State = f.State
	t.Code = f.Code
	t.PolicyVersion = f.PolicyVersion
	t.Dimension = f.Dimension
	t.AttemptID = f.AttemptID
	t.Calls += f.Calls
	t.CallTimeMS += f.ElapsedMS
	t.ResultBytes += int64(len(f.Body))
	t.NotBefore = f.NotBefore
	t.Result = f.Body
	t.Fence = ""
	t.LeaseUntil = time.Time{}
	if f.State == "SUCCEEDED" {
		t.CompletedAt = now
	}
	if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
		return t, e
	}
	if t.Tool == analysis.BatchTool {
		if e = savePublished(ctx, tx, id, t, f); e != nil {
			return t, e
		}
		return t, tx.Commit(ctx)
	}
	if e = finishSummary(ctx, tx, id, t, now, f.Observation); e != nil {
		if errors.Is(e, analysis.ErrProfile) {
			return t, holdAnalysis(ctx, tx, id)
		}
		return t, e
	}
	return t, tx.Commit(ctx)
}

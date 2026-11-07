package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
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
	if json.Unmarshal(raw, &p) != nil || stored != manifest {
		return p, 0, analysis.ErrScope
	}
	return p, rev, nil
}
func analysisTasks(ctx context.Context, tx pgx.Tx, id string) ([]analysis.Task, error) {
	// Never load potentially large evidence bodies or intent arguments for totals.
	rows, err := tx.Query(ctx, `select task-'result'-'arguments' from ouf_mcp.profile_analysis_task where analysis_id=$1 order by ordinal`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := []analysis.Task{}
	for rows.Next() {
		var raw []byte
		var t analysis.Task
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &t); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}
func analysisView(id string, p analysis.Profile, rev int64, tasks []analysis.Task, offset, limit int) analysis.View {
	budget, covered, complete := analysis.Calculate(tasks)
	end := min(len(tasks), offset+limit)
	start := min(offset, len(tasks))
	return analysis.View{ID: id, Revision: rev, Profile: p, Budget: budget, Tasks: tasks[start:end], Offset: offset, HasMore: end < len(tasks), CoveredColumns: covered, EvidenceColumns: complete}
}
func (s *Store) PlanAnalysis(ctx context.Context, p analysis.Plan) (analysis.View, error) {
	if !p.Scope.Valid() || !p.Profile.Valid() || len(p.Tasks) < 1 || len(p.Tasks) > analysis.MaxPlanBatch || p.ID != analysis.AnalysisID(p.Scope, p.Profile) {
		return analysis.View{}, analysis.ErrInvalid
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return analysis.View{}, e
	}
	defer tx.Rollback(ctx)
	raw, _ := json.Marshal(p.Profile)
	_, e = tx.Exec(ctx, `insert into ouf_mcp.profile_analysis(analysis_id,service_principal_id,principal_id,tenant_id,profile,manifest_checksum) values($1,$2,$3,$4,$5,$6) on conflict do nothing`, p.ID, p.Scope.Service, p.Scope.Principal, p.Scope.Tenant, raw, p.Manifest)
	if e != nil {
		return analysis.View{}, e
	}
	existing, rev, e := analysisHeader(ctx, tx, p.Scope, p.ID, p.Manifest)
	if e != nil {
		return analysis.View{}, e
	}
	if existing != p.Profile {
		return analysis.View{}, analysis.ErrScope
	}
	tasks, e := analysisTasks(ctx, tx, p.ID)
	if e != nil {
		return analysis.View{}, e
	}
	indices := map[string]int{}
	for n, t := range tasks {
		indices[t.ID] = n
	}
	changes := []analysis.Task{}
	newCount := 0
	for _, t := range p.Tasks {
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
	for _, t := range changes {
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
	return v, tx.Commit(ctx)
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
	return analysisView(id, p, rev, tasks, offset, limit), tx.Commit(ctx)
}
func readAnalysisTask(ctx context.Context, tx pgx.Tx, id, taskID string) (analysis.Task, error) {
	var t analysis.Task
	var raw []byte
	var fence *string
	e := tx.QueryRow(ctx, `select task,fence::text from ouf_mcp.profile_analysis_task where analysis_id=$1 and task_id=$2 for update`, id, taskID).Scan(&raw, &fence)
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
	t, e = readAnalysisTask(ctx, tx, id, taskID)
	if e != nil {
		return t, e
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "select transaction_timestamp()").Scan(&now); e != nil {
		return t, e
	}
	if t.State == "SUCCEEDED" || t.State == "HELD" {
		return t, tx.Commit(ctx)
	}
	if t.State == "RUNNING" {
		if !now.Before(t.LeaseUntil) {
			t.State = "HELD"
			t.Code = "PREVIOUS_OUTCOME_REQUIRES_RECONCILIATION"
			t.Fence = uuid.NewString()
			if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
				return t, e
			}
		}
		// Never return another worker's fencing token to a concurrent executor.
		t.Fence = ""
		return t, tx.Commit(ctx)
	}
	if now.Before(t.NotBefore) {
		return t, tx.Commit(ctx)
	}
	if t.Attempts >= analysis.MaxRetryAttempts || (!t.FirstAttemptAt.IsZero() && now.Sub(t.FirstAttemptAt) >= 24*time.Hour) {
		t.State = "HELD"
		t.Code = "RETRY_BUDGET_EXHAUSTED"
		if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
			return t, e
		}
		return t, tx.Commit(ctx)
	}
	// Poll observations are sequential, spaced and durably linked. New caller
	// IDs cannot turn the same observation into additional execution capacity.
	if t.Tool == "semantic.discovery.status" && t.Observation > 1 {
		priorID := analysis.Digest([]any{t.Tool, json.RawMessage(t.Arguments), t.Observation - 1})
		prior, err := readAnalysisTask(ctx, tx, id, priorID)
		if err != nil || prior.State != "SUCCEEDED" {
			return t, analysis.ErrConflict
		}
		earliest := prior.CompletedAt.Add(31 * time.Second)
		if now.Before(earliest) {
			t.State = "WAITING"
			t.Code = "OBSERVATION_INTERVAL"
			t.NotBefore = earliest
			if e = saveAnalysisTask(ctx, tx, id, t); e != nil {
				return t, e
			}
			return t, tx.Commit(ctx)
		}
	}
	var retained int64
	if e = tx.QueryRow(ctx, `select coalesce(sum(coalesce((task->>'resultBytes')::bigint,0)),0) from ouf_mcp.profile_analysis_task where analysis_id=$1`, id).Scan(&retained); e != nil {
		return t, e
	}
	if retained+t.MaxBytes > analysis.MaxRetainedBytes {
		return t, analysis.ErrCapacity
	}
	t.State = "RUNNING"
	t.Code = ""
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
		if f.Calls != 0 || f.State != "WAITING" || !f.NotBefore.After(now) {
			return t, analysis.ErrInvalid
		}
		t.Attempts--
		t.Waits++
	}
	t.State = f.State
	t.Code = f.Code
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
	return t, tx.Commit(ctx)
}

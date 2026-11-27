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

const plannedReadRequest = `case when t.task->>'tool' in ('semantic.discovery.status','semantic.discovery.candidates')
	then (t.task->'arguments'->>'requestId')::uuid end`
const plannedReadTaskKeys = plannedReadRequest + ` as request_id,
	case when t.task->>'tool'='semantic.discovery.status' then (t.task->>'observation')::int end as observation,
	t.task->>'state' as state,(t.task->>'notBefore')::timestamptz as not_before`
const statusPacingFloor = `greatest((t.task->>'notBefore')::timestamptz,
	(t.task->>'completedAt')::timestamptz + interval '31 seconds')`

// Build one predecessor per UUID and observation for the single analysis.
// The status tool is fixed on both sides. Equivalent retained tasks can have
// different IDs. Prefer a success with the latest pacing floor, then plan order.
const plannedReadPredecessors = `status_predecessors as materialized (
	select distinct on (request_id,observation) t.task_id,` + plannedReadTaskKeys + `,
		case when t.task->>'state'='SUCCEEDED' then ` + statusPacingFloor + ` end as successor_not_before
	from ouf_mcp.profile_analysis_task t
	where t.task->>'tool'='semantic.discovery.status' and t.task->>'awaitParent' is distinct from 'true'
		and t.analysis_id=`
const plannedReadPredecessorOrder = ` order by request_id,observation,(t.task->>'state'='SUCCEEDED') desc nulls last,
	successor_not_before desc nulls last,t.ordinal)`
const plannedReadPredecessorsByAnalysis = `with ` + plannedReadPredecessors + `$1` + plannedReadPredecessorOrder
const plannedReadPredecessorsByRequest = `with ` + plannedReadPredecessors + `$1
	and ` + plannedReadRequest + `=($2::jsonb->>'requestId')::uuid and (t.task->>'observation')::int=$3::int` + plannedReadPredecessorOrder
const plannedReadPredecessorJoin = ` left join status_predecessors prior
	on prior.request_id=t.request_id and prior.observation=t.observation-1 `

const predecessorSucceeded = `prior.state='SUCCEEDED'`
const blockedPredecessor = `t.state in ('READY','WAITING') and t.observation>1 and (` + predecessorSucceeded + `) is not true`
const plannedReadClockJoins = ` left join ouf_mcp.analysis_owner_due w on w.analysis_id=t.analysis_id
	and w.request_id=t.request_id ` + plannedReadPredecessorJoin
const plannedReadClocks = ` from (
	select t.analysis_id,t.task_id,t.ordinal,t.task,` + plannedReadTaskKeys + `
	from ouf_mcp.profile_analysis_task t where t.analysis_id=$1
) t ` + plannedReadClockJoins
const plannedReadDue = `greatest(w.not_before,prior.successor_not_before)`

// Read the whole analysis for its totals and dependencies, omitting large
// evidence and intent fields. An empty predecessor ID signals a missing link.
const analysisTasksSQL = plannedReadPredecessorsByAnalysis + `
	select t.task-'discoveryText'-'result'-'arguments'-'intentOrdinals'-'visibilityDigest'-'ownerKey'-'policyEpoch'-'ownerIntent',
	` + plannedReadDue + `,t.request_id::text,
	case when ` + blockedPredecessor + ` then coalesce(prior.task_id::text,'') end` + plannedReadClocks + `order by t.ordinal`

type plannedRead struct {
	request     string
	predecessor *string
	resume      analysis.TaskResume
}

func analysisTasks(ctx context.Context, tx pgx.Tx, id string) ([]analysis.Task, map[string]plannedRead, error) {
	rows, err := tx.Query(ctx, analysisTasksSQL, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	tasks := []analysis.Task{}
	reads := make(map[string]plannedRead)
	for rows.Next() {
		var raw []byte
		var t analysis.Task
		var due *time.Time
		var request, predecessor *string
		if err = rows.Scan(&raw, &due, &request, &predecessor); err != nil {
			return nil, nil, err
		}
		if err = json.Unmarshal(raw, &t); err != nil {
			return nil, nil, err
		}
		if due != nil && due.After(t.NotBefore) && (t.State == "READY" || t.State == "WAITING") {
			t.NotBefore = *due
		}
		if request != nil && !t.AwaitParent {
			reads[t.ID] = plannedRead{request: *request, predecessor: predecessor,
				resume: analysis.TaskResume{ID: t.ID, State: t.State, NotBefore: t.NotBefore}}
		}
		tasks = append(tasks, t)
	}
	return tasks, reads, rows.Err()
}

func analysisView(ctx context.Context, tx pgx.Tx, scope analysis.Scope, id string, p analysis.Profile, rev int64, offset, limit int) (analysis.View, error) {
	tasks, reads, err := analysisTasks(ctx, tx, id)
	if err != nil {
		return analysis.View{}, err
	}
	tasks = slices.DeleteFunc(tasks, func(t analysis.Task) bool { return t.AwaitParent })
	budget, covered, complete := analysis.Calculate(tasks)
	start := min(len(tasks), offset)
	end := min(len(tasks), start+limit)
	view := analysis.View{ID: id, Revision: rev, Profile: p, Budget: budget, Tasks: tasks[start:end],
		Offset: offset, HasMore: end < len(tasks), CoveredColumns: covered, EvidenceColumns: complete}

	selectors := make(map[string]string)
	for _, t := range view.Tasks {
		read, ok := reads[t.ID]
		if ok && (t.State == "READY" || t.State == "WAITING") && selectors[read.request] == "" {
			if selector := analysis.DiscoveryObservationSelector(ctx, read.request); selector != "" {
				selectors[read.request] = selector
			}
		}
	}
	dues, err := readOwnerDues(ctx, tx, scope, selectors)
	if err != nil {
		return analysis.View{}, err
	}
	resolvePredecessor := predecessorResolver(func(id string) plannedRead { return reads[id] })
	for n := range view.Tasks {
		t := &view.Tasks[n]
		read, ok := reads[t.ID]
		if !ok || (t.State != "READY" && t.State != "WAITING") {
			continue
		}
		due := dues[read.request]
		if due.After(t.NotBefore) {
			t.NotBefore = due
		}
		if read.predecessor == nil {
			continue
		}
		resume := resolvePredecessor(*read.predecessor)
		if resume.ID != "" && due.After(resume.NotBefore) {
			resume.NotBefore = due
		}
		t.Predecessor = &resume
	}
	return view, nil
}

// Each link decreases the observation number. Cache the root (including a
// missing link) for every visited task, even when it is outside the page.
func predecessorResolver(lookup func(string) plannedRead) func(string) analysis.TaskResume {
	roots := make(map[string]analysis.TaskResume)
	var resolve func(string) analysis.TaskResume
	resolve = func(id string) analysis.TaskResume {
		if root, found := roots[id]; found {
			return root
		}
		read := lookup(id)
		root := read.resume
		if read.predecessor != nil {
			root = resolve(*read.predecessor)
		}
		roots[id] = root
		return root
	}
	return resolve
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
	// Persist new plans under the manifest accepted by both releases during rollback.
	storedManifest := manifestpkg.AnalysisPredecessor(p.Manifest)
	if storedManifest == "" {
		storedManifest = p.Manifest
	}
	_, e := tx.Exec(ctx, `insert into ouf_mcp.profile_analysis(analysis_id,service_principal_id,principal_id,tenant_id,profile,manifest_checksum) values($1,$2,$3,$4,$5,$6) on conflict do nothing`, p.ID, p.Scope.Service, p.Scope.Principal, p.Scope.Tenant, raw, storedManifest)
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
	tasks, _, e := analysisTasks(ctx, tx, p.ID)
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
	v, e := analysisView(ctx, tx, p.Scope, p.ID, p.Profile, rev, 0, 128)
	if e != nil {
		return analysis.View{}, e
	}
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
	v, e := analysisView(ctx, tx, scope, id, p, rev, offset, limit)
	if e != nil {
		return analysis.View{}, e
	}
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
		var request string
		var predecessorReady bool
		if e = tx.QueryRow(ctx, plannedReadPredecessorsByRequest+`
			select `+plannedReadDue+`,coalesce((`+predecessorSucceeded+`),false),coalesce(t.request_id::text,'')`+plannedReadClocks+
			`where t.task_id=$4`, id, t.Arguments, t.Observation-1, taskID).Scan(&due, &predecessorReady, &request); e != nil {
			return t, e
		}
		if t.Observation > 1 && !predecessorReady {
			return t, analysis.ErrConflict
		}
		bound := t.NotBefore
		if due != nil && due.After(bound) {
			bound = *due
		}
		if selector := analysis.DiscoveryObservationSelector(ctx, request); selector != "" {
			saved, err := readDiscoveryObservation(ctx, tx, scope, selector, "", request)
			if err != nil {
				return t, err
			}
			if saved.Due.After(bound) {
				bound = saved.Due
			}
		}
		if bound.After(now) && (t.State != "WAITING" || bound.After(t.NotBefore)) {
			t.State, t.Code, t.NotBefore = "WAITING", analysis.ErrObservationInterval.Error(), bound
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

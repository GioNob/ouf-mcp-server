package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	manifestpkg "github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type statusObservation struct {
	request     uuid.UUID
	observation int
}

type statusTasks struct {
	byObservation map[statusObservation][]string
	count         int
}

type statusTaskCacheEntry struct {
	tasks    statusTasks
	revision int64
	used     uint64
}

// Cache only task IDs in plan order. The UUID spelling is part of the stored
// task digest, so one computed digest cannot identify every equivalent task.
// A plan revision fences membership. State and clocks always come from SQL.
type statusTaskCache struct {
	mu     sync.Mutex
	plans  map[string]statusTaskCacheEntry
	count  int
	access uint64
}

func (c *statusTaskCache) get(id string, revision int64) (statusTasks, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.plans[id]
	if !ok || entry.revision != revision {
		return statusTasks{}, false
	}
	c.access++
	entry.used = c.access
	c.plans[id] = entry
	return entry.tasks, true
}

func (c *statusTaskCache) put(id string, revision int64, tasks statusTasks) {
	const maxPlans, maxTasks = 128, 8 * analysis.MaxTasks
	if tasks.count > maxTasks {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, exists := c.plans[id]; exists {
		if entry.revision >= revision {
			return
		}
		c.count -= entry.tasks.count
		delete(c.plans, id)
	}
	for len(c.plans) >= maxPlans || c.count+tasks.count > maxTasks {
		var oldestID string
		var oldest statusTaskCacheEntry
		for planID, entry := range c.plans {
			if oldestID == "" || entry.used < oldest.used {
				oldestID, oldest = planID, entry
			}
		}
		delete(c.plans, oldestID)
		c.count -= oldest.tasks.count
	}
	if c.plans == nil {
		c.plans = make(map[string]statusTaskCacheEntry)
	}
	c.access++
	c.plans[id] = statusTaskCacheEntry{tasks: tasks, revision: revision, used: c.access}
	c.count += tasks.count
}

func (s *Store) statusTasksForRevision(ctx context.Context, tx pgx.Tx, id string, revision int64) (statusTasks, error) {
	if tasks, ok := s.statusTasks.get(id, revision); ok {
		return tasks, nil
	}
	rows, err := tx.Query(ctx, `select t.task_id,`+plannedReadRequest+`,(t.task->>'observation')::int
		from ouf_mcp.profile_analysis_task t where t.analysis_id=$1
		and t.task->>'tool'='semantic.discovery.status' and t.task->>'awaitParent' is distinct from 'true'
		order by t.ordinal`, id)
	if err != nil {
		return statusTasks{}, err
	}
	defer rows.Close()
	tasks := statusTasks{byObservation: make(map[statusObservation][]string)}
	for rows.Next() {
		var key statusObservation
		var taskID string
		if err = rows.Scan(&taskID, &key.request, &key.observation); err != nil {
			return statusTasks{}, err
		}
		tasks.byObservation[key] = append(tasks.byObservation[key], taskID)
		tasks.count++
	}
	if err = rows.Err(); err != nil {
		return statusTasks{}, err
	}
	s.statusTasks.put(id, revision, tasks)
	return tasks, nil
}

// Only the indexed IDs for B and the equivalent versions of A are read.
// Taking the latest success floor matches the predecessor chosen on a page.
const analysisSuccessorSQL = `select t.task_id,t.task->>'state',greatest((t.task->>'notBefore')::timestamptz,
	(select max(` + statusPacingFloor + `) from ouf_mcp.profile_analysis_task t
	 where t.analysis_id=$1 and t.task_id=any($3::char(64)[]) and t.task->>'state'='SUCCEEDED'))
	from ouf_mcp.profile_analysis_task t where t.analysis_id=$1 and t.task_id=$2`

func (s *Store) ReadAnalysisSuccessor(ctx context.Context, scope analysis.Scope, id, manifest string, task analysis.Task) (*analysis.TaskResume, error) {
	var args struct{ RequestID uuid.UUID }
	if err := json.Unmarshal(task.Arguments, &args); err != nil {
		return nil, err
	}
	// Revision, task membership and pacing must refer to the same snapshot.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var revision int64
	err = tx.QueryRow(ctx, `select revision from ouf_mcp.profile_analysis
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4
		and manifest_checksum=any($5::char(64)[])`, id, scope.Service, scope.Principal, scope.Tenant,
		manifestpkg.AnalysisManifests(manifest)).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, analysis.ErrScope
	}
	if err != nil {
		return nil, err
	}
	if task.Visibility != "" {
		if err = publishedStopped(ctx, tx, id); err != nil {
			return nil, err
		}
	}
	tasks, err := s.statusTasksForRevision(ctx, tx, id, revision)
	if err != nil {
		return nil, err
	}
	prior := tasks.byObservation[statusObservation{args.RequestID, task.Observation}]
	next := tasks.byObservation[statusObservation{args.RequestID, task.Observation + 1}]
	if len(next) == 0 {
		return nil, nil
	}
	var successor analysis.TaskResume
	var due *time.Time
	err = tx.QueryRow(ctx, analysisSuccessorSQL, id, next[0], prior).Scan(&successor.ID, &successor.State, &due)
	if err != nil {
		return nil, err
	}
	if due != nil {
		successor.NotBefore = *due
	}
	request := args.RequestID.String()
	saved, err := readDiscoveryObservation(ctx, tx, scope, analysis.DiscoveryObservationSelector(ctx, request), "", request)
	if err != nil {
		return nil, err
	}
	if saved.Due.After(successor.NotBefore) {
		successor.NotBefore = saved.Due
	}
	return &successor, nil
}

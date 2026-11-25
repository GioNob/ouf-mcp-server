package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type epochBatchOwner struct {
	preview *analysisGateway
	denial  string
	keys    []string
}

func (g *epochBatchOwner) Execute(ctx context.Context, r orchestration.GatewayRequest, timeout time.Duration) (orchestration.GatewayResponse, error) {
	if r.CapabilityID != "ouf.semantic.search.batch" {
		return g.preview.Execute(ctx, r, timeout)
	}
	g.keys = append(g.keys, r.IdempotencyKey)
	if g.denial != "" {
		return orchestration.GatewayResponse{Status: 403, Problem: &orchestration.Problem{Status: 403, Code: g.denial}}, nil
	}
	var b analysis.Batch
	if err := json.Unmarshal(r.Arguments, &b); err != nil {
		return orchestration.GatewayResponse{}, err
	}
	var results []analysis.BatchResult
	for _, i := range b.Intents {
		results = append(results, analysis.BatchResult{ID: i.ID, State: "MATCH", Hits: []analysis.PublishedMatch{{
			SemanticID: "urn:test:term", SemanticVersion: "1", RevisionID: uuid.NewString(), PublicationSetID: uuid.NewString(),
			Score: 100, Reasons: []string{"EXACT_LABEL", "PREFERRED_LANGUAGE"},
		}}})
	}
	raw, err := json.Marshal(map[string]any{"contract": analysis.BatchContract, "results": results})
	return orchestration.GatewayResponse{Status: 200, Body: raw}, err
}

func TestReplanDeniedAcrossEpochs(t *testing.T) { // MCP-50T-07
	for _, mode := range []string{"current", "predecessor"} {
		t.Run(mode, func(t *testing.T) {
			ctx, db, c, i, auth, gateway, p := coordinatorFixture(t, 4)
			auth.version = 77
			scope := analysis.IdentityScope(i)
			owner := &epochBatchOwner{preview: gateway, denial: "NO_APPLICABLE_GRANT"}
			c.Service.Gateway = owner
			work := []analysis.Work{{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"intent":"column","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`), Columns: []int{0}}}
			old, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
			if err != nil {
				t.Fatal(err)
			}
			sum, err := c.Manifest.Checksum()
			if err != nil {
				t.Fatal(err)
			}
			if mode == "predecessor" {
				// Write precisely the predecessor ID, manifest fence, generic
				// stop reason and task without an epoch tag.
				cap, _ := analysis.Find(c.Manifest, analysis.BatchTool)
				limits, err := analysis.PublishedLimits(cap)
				if err != nil {
					t.Fatal(err)
				}
				legacy, err := analysis.BuildPublished(scope, p, c.Manifest, []string{"tipologia", "tipologia", "tipologia", "tipologia"}, work, old.Visibility, 0)
				if err != nil || limits.MaxIntents < 2 {
					t.Fatal(err, limits)
				}
				legacy.Manifest = manifest.AnalysisManifests(sum)[1]
				old, err = db.PlanAnalysis(ctx, legacy)
				if err != nil {
					t.Fatal(err)
				}
				owner.denial = "SEM_READ_RECEIPT_OR_POLICY_DENIED"
			}
			step, err := c.Step(ctx, i, old.ID, old.Tasks[0].ID)
			var denial *analysis.OwnerDenial
			if !errors.As(err, &denial) || !analysis.PermissionHold(step.Code) {
				t.Fatal(step, err)
			}
			oldTask, err := db.ReadAnalysisTask(ctx, scope, old.ID, step.ID, sum)
			if err != nil {
				t.Fatal(err)
			}
			sameKey := uuid.NewString()
			if _, err = c.Replan(ctx, i, old.ID, sameKey); !errors.Is(err, analysis.ErrPolicyUnchanged) {
				t.Fatal(err)
			}
			auth.version++
			owner.denial = ""
			other := i
			other.PrincipalID = uuid.NewString()
			if _, err := c.Replan(ctx, other, old.ID, uuid.NewString()); !errors.Is(err, analysis.ErrNotFound) {
				t.Fatal(err)
			}
			if _, err = c.Replan(ctx, i, old.ID, sameKey); !errors.Is(err, analysis.ErrPolicyUnchanged) {
				t.Fatal("changed outcome for prior key", err)
			}
			key := uuid.NewString()
			fresh, err := c.Replan(ctx, i, old.ID, key)
			if err != nil || fresh.ID == old.ID || fresh.Budget != old.Budget || len(fresh.Tasks) != 1 {
				t.Fatal(fresh, err)
			}
			task, err := db.ReadAnalysisTask(ctx, scope, fresh.ID, step.ID, sum)
			if err != nil || task.State != "READY" || task.Attempts != 0 || len(task.Result) != 0 || !reflect.DeepEqual(task.Work, oldTask.Work) || !reflect.DeepEqual(task.Groups, oldTask.Groups) {
				t.Fatal(task, err)
			}
			c.CommandKey = key
			if _, err = c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work); !errors.Is(err, analysis.ErrKeyReuse) {
				t.Fatal("ordinary plan accepted replan key", err)
			}
			c.CommandKey = uuid.NewString()
			normal, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
			if err != nil || normal.ID != fresh.ID {
				t.Fatal(normal, err)
			}
			if _, err = c.Replan(ctx, i, old.ID, c.CommandKey); !errors.Is(err, analysis.ErrKeyReuse) {
				t.Fatal("replan accepted ordinary plan key", err)
			}
			otherDB, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			defer otherDB.Close()
			c.Store = otherDB
			retry, err := c.Replan(ctx, i, old.ID, key)
			if err != nil || retry.ID != fresh.ID || retry.Revision != fresh.Revision {
				t.Fatal(retry, err)
			}
			if _, err := c.Replan(ctx, i, fresh.ID, key); !errors.Is(err, analysis.ErrKeyReuse) {
				t.Fatal(err)
			}
			step, err = c.Step(ctx, i, fresh.ID, task.ID)
			if err != nil || step.State != "SUCCEEDED" || len(owner.keys) != 2 || owner.keys[0] == owner.keys[1] {
				t.Fatal(step, err, owner.keys)
			}
			pos := analysis.Position{Seq: 1<<63 - 1, Last: -1, Limit: 4}
			summary, err := db.ReadSummary(ctx, scope, fresh.ID, sum, pos, true)
			if err != nil || summary.State() != "COMPLETE" || summary.Counts.Matched != 4 || len(summary.Items) != 4 {
				t.Fatal(summary, err)
			}
			history, err := c.Read(ctx, i, old.ID, 0, 128)
			if err != nil || history.Budget.Held != 1 || history.Budget.Calls != 1 {
				t.Fatal(history, err)
			}
			if _, err := c.Replan(ctx, i, fresh.ID, uuid.NewString()); !errors.Is(err, analysis.ErrReplanState) {
				t.Fatal(err)
			}
			auth.version++
			if next, err := c.Replan(ctx, i, fresh.ID, uuid.NewString()); err != nil || next.ID == fresh.ID ||
				len(next.Tasks) != 1 || next.Tasks[0].State != "READY" || next.Budget.Calls != 0 {
				t.Fatal(next, err)
			}
		})
	}
}

func TestReplanConcurrentCommands(t *testing.T) {
	ctx, db, c, i, auth, gateway, p := coordinatorFixture(t, 4)
	auth.version = 77
	c.Service.Gateway = &epochBatchOwner{preview: gateway, denial: "NO_APPLICABLE_GRANT"}
	work := []analysis.Work{{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"intent":"column","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`), Columns: []int{0}}}
	old, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
	if err != nil || len(old.Tasks) != 1 {
		t.Fatal(old, err)
	}
	if task, err := c.Step(ctx, i, old.ID, old.Tasks[0].ID); err == nil || task.Code != "NO_APPLICABLE_GRANT" {
		t.Fatal(task, err)
	}
	auth.version++
	shared := uuid.NewString()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var ids [16]string
	var errs [16]error
	for n := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := shared
			if n%2 != 0 {
				key = uuid.NewString()
			}
			<-start
			v, err := c.Replan(ctx, i, old.ID, key)
			ids[n], errs[n] = v.ID, err
		}()
	}
	close(start)
	wg.Wait()
	for n, id := range ids {
		if errs[n] != nil || id != ids[0] || id == old.ID || id == "" {
			t.Fatal(n, id, errs[n])
		}
	}
	var plans, tasks, receipts int
	err = db.pool.QueryRow(ctx, `select
		(select count(*) from ouf_mcp.profile_analysis where principal_id=$1),
		(select count(*) from ouf_mcp.profile_analysis_task where analysis_id=$2),
		(select count(*) from ouf_mcp.tool_attempt where principal_id=$1 and capability_id='ouf.source.analysis.plan'
			and state='SUCCEEDED' and result_ref=$2)`, i.PrincipalID, ids[0]).Scan(&plans, &tasks, &receipts)
	if err != nil || plans != 2 || tasks != 1 || receipts != 9 {
		t.Fatal(plans, tasks, receipts, err)
	}
}

func TestReplanChildrenFollowNewBatchOutcome(t *testing.T) {
	for _, mode := range []string{"reproduced", "orphaned"} {
		t.Run(mode, func(t *testing.T) {
			ctx, db, c, i, auth, gateway, p := coordinatorFixture(t, 2)
			auth.version = 77
			c.Service.Gateway = &publishedFailureOwner{preview: gateway, status: 404}
			work := []analysis.Work{
				{Tool: "semantic.discovery.request", Columns: []int{0}, Arguments: json.RawMessage(`{"intent":"amount","preferredLanguages":["it"],"requestedArtifactType":"DATATYPE"}`)},
				{Tool: "semantic.discovery.request", Columns: []int{1}, Arguments: json.RawMessage(`{"intent":"name","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)},
			}
			old, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
			if err != nil || len(old.Tasks) != 1 {
				t.Fatal(old, err)
			}
			parent := old.Tasks[0].ID
			if task, err := c.Step(ctx, i, old.ID, parent); err != nil || task.Code != "PUBLISHED_BATCH_UNAVAILABLE" {
				t.Fatal(task, err)
			}
			old, err = c.Read(ctx, i, old.ID, 0, 128)
			if err != nil || old.Budget.Held != 1 || len(old.Tasks) != 3 {
				t.Fatal(old, err)
			}
			var held string
			for _, task := range old.Tasks {
				if task.State == "HELD" && task.Code == "DISCOVERY_TYPE_UNSUPPORTED" {
					held = task.ID
				}
			}
			if held == "" {
				t.Fatal(old.Tasks)
			}
			if _, err := c.Replan(ctx, i, old.ID, uuid.NewString()); !errors.Is(err, analysis.ErrReplanState) {
				t.Fatal(err)
			}
			auth.version++
			if _, err := c.Step(ctx, i, old.ID, held); !errors.Is(err, analysis.ErrEpochChanged) {
				t.Fatal(err)
			}
			fresh, err := c.Replan(ctx, i, old.ID, uuid.NewString())
			if err != nil || fresh.ID == old.ID || len(fresh.Tasks) != 1 || fresh.Tasks[0].ID != parent || fresh.Budget.PlannedCalls != 1 {
				t.Fatal(fresh, err)
			}
			var child string
			err = db.pool.QueryRow(ctx, `select task_id from ouf_mcp.profile_analysis_task where analysis_id=$1
				and task->>'awaitParent'='true' and task->>'state'='AWAITING_PARENT' and task->'columns'='[1]'::jsonb`, fresh.ID).Scan(&child)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Step(ctx, i, fresh.ID, child); !errors.Is(err, analysis.ErrScope) {
				t.Fatal("dormant child was addressable", err)
			}
			if mode == "orphaned" {
				c.Service.Gateway = &epochBatchOwner{preview: gateway}
			}
			if task, err := c.Step(ctx, i, fresh.ID, parent); err != nil || task.State != "SUCCEEDED" {
				t.Fatal(task, err)
			}
			// Another replan command must not resurrect discarded children.
			if _, err := c.Replan(ctx, i, old.ID, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			view, err := c.Read(ctx, i, fresh.ID, 0, 128)
			if err != nil || view.Budget.Completed != 1 {
				t.Fatal(view, err)
			}
			if mode == "reproduced" {
				if len(view.Tasks) != 3 || view.Budget.PlannedCalls != 3 || view.Budget.Held != 1 {
					t.Fatal(view)
				}
				c.Service.Gateway = gateway
				task, err := c.Step(ctx, i, fresh.ID, child)
				if err != nil || task.State != "SUCCEEDED" || task.Calls != 1 {
					t.Fatal(task, err)
				}
				return
			}
			if len(view.Tasks) != 1 || view.Budget.PlannedCalls != 1 || view.Tasks[0].ID != parent {
				t.Fatal(view)
			}
			var rows int
			if err := db.pool.QueryRow(ctx, `select count(*) from ouf_mcp.profile_analysis_task where analysis_id=$1`, fresh.ID).Scan(&rows); err != nil || rows != 1 {
				t.Fatal("predecessor would still find a child row", rows, err)
			}
			if _, err := c.Step(ctx, i, fresh.ID, child); !errors.Is(err, analysis.ErrScope) {
				t.Fatal("orphan still addressable", err)
			}
		})
	}
}

func TestPlanReceiptAfterEpochChange(t *testing.T) {
	ctx, _, c, i, auth, _, p := coordinatorFixture(t, 1)
	auth.version = 77
	c.CommandKey = uuid.NewString()
	work := []analysis.Work{{Tool: "semantic.discovery.request", Columns: []int{0}, Arguments: json.RawMessage(`{"intent":"name","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}}
	old, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work); err != nil || retry.ID != old.ID {
		t.Fatal(retry, err)
	}
	changedScope := i
	changedScope.Scopes = []string{"different-scope"}
	if _, err := c.Plan(ctx, changedScope, p.AssetID, p.ProfileID, 0, work); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("same-epoch visibility drift must retain its scope error", err)
	}
	auth.version++
	_, err = c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
	var epoch *analysis.PlanEpochError
	if !errors.Is(err, analysis.ErrEpochChanged) || !errors.As(err, &epoch) || epoch.AnalysisID != old.ID {
		t.Fatal(err)
	}
	if fresh, err := c.Replan(ctx, i, epoch.AnalysisID, uuid.NewString()); err != nil || fresh.ID == old.ID {
		t.Fatal(fresh, err)
	}
}

func TestReplanCopiesAllRevisionsAfterPartialExecution(t *testing.T) {
	ctx, db, c, i, auth, _, p := coordinatorFixture(t, 1024)
	auth.version = 77
	var old analysis.View
	var err error
	for start := 0; start < p.ColumnCount; start += analysis.MaxPlanBatch {
		var work []analysis.Work
		for n := start; n < start+analysis.MaxPlanBatch; n++ {
			work = append(work, searchWork(n))
		}
		old, err = c.Plan(ctx, i, p.AssetID, p.ProfileID, old.Revision, work)
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.Step(ctx, i, old.ID, old.Tasks[0].ID)
	if err != nil || first.State != "SUCCEEDED" {
		t.Fatal(first, err)
	}
	auth.version++
	if _, err = c.Step(ctx, i, old.ID, old.Tasks[1].ID); !errors.Is(err, analysis.ErrEpochChanged) {
		t.Fatal(err)
	}
	fresh, err := c.Replan(ctx, i, old.ID, uuid.NewString())
	if err != nil || fresh.ID == old.ID || fresh.Revision != 1 || fresh.Budget.PlannedCalls != 1024 || fresh.CoveredColumns != 1024 || fresh.Budget.Completed != 0 || fresh.Budget.Calls != 0 {
		t.Fatal(fresh.Budget, err)
	}
	var equal int
	err = db.pool.QueryRow(ctx, `select count(*) from ouf_mcp.profile_analysis_task a join ouf_mcp.profile_analysis_task b using(task_id)
		where a.analysis_id=$1 and b.analysis_id=$2 and a.task->'arguments'=b.task->'arguments'
		and a.task->'columns'=b.task->'columns' and b.task->>'state'='READY' and not (b.task ? 'result')`, old.ID, fresh.ID).Scan(&equal)
	if err != nil || equal != 1024 {
		t.Fatal(equal, err)
	}
}

func TestReplanAfterAcceptedBatchKeepsEvidenceInItsEpoch(t *testing.T) {
	ctx, db, c, i, auth, gateway, p := coordinatorFixture(t, 80)
	auth.version = 77
	owner := &epochBatchOwner{preview: gateway}
	c.Service.Gateway = owner
	var work []analysis.Work
	for n := 0; n < p.ColumnCount; n++ {
		args, err := json.Marshal(map[string]any{"intent": fmt.Sprintf("column%d", n), "preferredLanguages": []string{"it"}, "requestedArtifactType": "PROPERTY"})
		if err != nil {
			t.Fatal(err)
		}
		work = append(work, analysis.Work{Tool: "semantic.discovery.request", Arguments: args, Columns: []int{n}})
	}
	old, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
	if err != nil || len(old.Tasks) < 2 {
		t.Fatal(old, err)
	}
	accepted, err := c.Step(ctx, i, old.ID, old.Tasks[0].ID)
	if err != nil || accepted.State != "SUCCEEDED" {
		t.Fatal(accepted, err)
	}
	auth.version++
	if _, err = c.Step(ctx, i, old.ID, old.Tasks[1].ID); !errors.Is(err, analysis.ErrEpochChanged) {
		t.Fatal(err)
	}
	fresh, err := c.Replan(ctx, i, old.ID, uuid.NewString())
	if err != nil || fresh.ID == old.ID || fresh.Budget.Completed != 0 || len(fresh.Tasks) != len(old.Tasks) {
		t.Fatal(fresh, err)
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	pos := analysis.Position{Seq: 1<<63 - 1, Last: -1, Limit: p.ColumnCount}
	for _, id := range []string{old.ID, fresh.ID} {
		summary, err := db.ReadSummary(ctx, analysis.IdentityScope(i), id, checksum, pos, true)
		if err != nil || (id == old.ID && summary.Counts.Matched != len(accepted.Columns)) ||
			(id == fresh.ID && summary.Counts.Pending != p.ColumnCount) {
			t.Fatal(summary.Counts, err)
		}
	}
	again, err := c.Step(ctx, i, fresh.ID, accepted.ID)
	if err != nil || again.AttemptID == accepted.AttemptID || again.State != "SUCCEEDED" || len(owner.keys) != 2 || owner.keys[0] == owner.keys[1] {
		t.Fatal(again, err, owner.keys)
	}
}

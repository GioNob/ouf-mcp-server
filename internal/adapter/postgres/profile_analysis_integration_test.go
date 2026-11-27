package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func analysisFixture(t *testing.T, n int) (context.Context, *Store, analysis.Scope, analysis.Profile, *manifest.Snapshot) {
	t.Helper()
	ctx, s, _ := controlStore(t)
	m, e := manifest.Load()
	if e != nil {
		t.Fatal(e)
	}
	checksum, _ := m.Checksum()
	payload, _ := m.CanonicalPayload()
	if e = s.EnsureManifest(ctx, checksum, m.Version, "mcp-manifest-v1", payload); e != nil {
		t.Fatal(e)
	}
	return ctx, s, analysis.Scope{Service: "mcp", Principal: uuid.NewString(), Tenant: uuid.NewString()}, analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, SchemaHash: "sha256:" + strings.Repeat("b", 64), ColumnCount: n}, m
}
func searchWork(n int) analysis.Work {
	b, _ := json.Marshal(map[string]any{"q": fmt.Sprintf("property-%d", n), "type": "PROPERTY"})
	return analysis.Work{Tool: "semantic.search", Arguments: b, Columns: []int{n}}
}

func TestBatchStartRetainsModeAndSharesNoLegacyAnalysis(t *testing.T) {
	ctx, store, scope, profile, snapshot := analysisFixture(t, 32)
	checksum, err := snapshot.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := analysis.Build(scope, profile, snapshot, 0, []analysis.Work{searchWork(0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PlanAnalysis(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	start := analysis.BatchStart{ID: analysis.BatchAnalysisID(scope, profile, ""), Profile: profile}
	key, intent := uuid.NewString(), analysis.Digest([]string{analysis.BatchMode, profile.AssetID, profile.ProfileID, profile.SchemaHash})
	if got, err := store.SaveBatchStart(ctx, scope, checksum, key, intent, start); err != nil || got != start {
		t.Fatal(got, err)
	}
	reopened, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, found, err := reopened.LookupBatchStartCommand(ctx, scope, checksum, key, intent); err != nil || !found || got != start {
		t.Fatal(got, found, err)
	}
	if _, err := reopened.SaveBatchStart(ctx, scope, checksum, key, analysis.Digest("changed intent"), start); !errors.Is(err, analysis.ErrKeyReuse) {
		t.Fatal("changed command was accepted", err)
	}
	if _, err := reopened.ReadAnalysis(ctx, scope, start.ID, checksum, 0, 128); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("legacy read opened a batch", err)
	}
	other := scope
	other.Principal = uuid.NewString()
	if _, err := reopened.ReadBatchStart(ctx, other, start.ID, checksum); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("another principal read the batch", err)
	}
	predecessor := manifest.AnalysisPredecessor(checksum)
	if _, err := reopened.ReadAnalysis(ctx, scope, legacy.ID, predecessor, 0, 128); err != nil {
		t.Fatal("predecessor checksum could not read the legacy analysis", err)
	}
	var mode, fence string
	err = reopened.pool.QueryRow(ctx, `select profile->>'mode',manifest_checksum from ouf_mcp.profile_analysis where analysis_id=$1`, start.ID).Scan(&mode, &fence)
	if err != nil || mode != analysis.BatchMode || fence != checksum || fence == predecessor {
		t.Fatal("batch is missing its stored mode or candidate fence", mode, fence, err)
	}
}

func TestProfileAnalysisWideDurablePlanAndNoReset(t *testing.T) {
	for _, n := range []int{20, 50, 100, 200, 300, 400, 1024} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			ctx, s, scope, p, m := analysisFixture(t, n)
			var view analysis.View
			var last analysis.Plan
			for start := 0; start < n; start += 128 {
				work := []analysis.Work{}
				for x := start; x < min(start+128, n); x++ {
					work = append(work, searchWork(x))
				}
				var e error
				last, e = analysis.Build(scope, p, m, view.Revision, work)
				if e != nil {
					t.Fatal(e)
				}
				view, e = s.PlanAnalysis(ctx, last)
				if e != nil {
					t.Fatal(e)
				}
			}
			if view.Budget.PlannedCalls != int64(n) || view.Budget.MaxUsefulCallTimeMS != int64(n)*3000 || view.CoveredColumns != n {
				t.Fatal(view.Budget, view.CoveredColumns)
			}
			again, e := s.PlanAnalysis(ctx, last)
			if e != nil || again.Revision != view.Revision || again.Budget.PlannedCalls != int64(n) {
				t.Fatal("duplicate plan resets or extends", e)
			}
			first := view.Tasks[0]
			claimed, e := s.ClaimAnalysisTask(ctx, scope, view.ID, first.ID, last.Manifest)
			if e != nil {
				t.Fatal(e)
			}
			complete, e := s.FinishAnalysisTask(ctx, scope, view.ID, first.ID, claimed.Fence, analysis.Finish{State: "SUCCEEDED", Code: "EVIDENCE_COLLECTED_NOT_ADOPTED", Calls: 1, Body: json.RawMessage(`{"items":[]}`), ElapsedMS: 7})
			if e != nil || complete.State != "SUCCEEDED" {
				t.Fatal(e)
			}
			other, e := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
			if e != nil {
				t.Fatal(e)
			}
			defer other.Close()
			restored, e := other.ReadAnalysis(ctx, scope, view.ID, last.Manifest, 0, 128)
			if e != nil || restored.Budget.Calls != 1 || restored.Budget.Completed != 1 || restored.EvidenceColumns != 1 {
				t.Fatal(restored, e)
			}
			wrong := scope
			wrong.Principal = "other"
			if _, e = other.ReadAnalysis(ctx, wrong, view.ID, last.Manifest, 0, 128); !errors.Is(e, analysis.ErrScope) {
				t.Fatal("owner leak", e)
			}
			drift := last
			drift.Profile.ColumnCount = n + 1
			if n == 1024 {
				drift.Profile.ColumnCount = 1023
			}
			if _, e = other.PlanAnalysis(ctx, drift); e == nil {
				t.Fatal("profile drift reset")
			}
		})
	}
}
func TestProfileAnalysisFencingAndSharedColumnWork(t *testing.T) {
	ctx, s, scope, p, m := analysisFixture(t, 400)
	w := searchWork(0)
	plan, e := analysis.Build(scope, p, m, 0, []analysis.Work{w})
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.PlanAnalysis(ctx, plan)
	if e != nil {
		t.Fatal(e)
	}
	w.Columns = []int{300}
	extension, _ := analysis.Build(scope, p, m, v.Revision, []analysis.Work{w})
	v, e = s.PlanAnalysis(ctx, extension)
	if e != nil || v.Budget.PlannedCalls != 1 || v.CoveredColumns != 2 {
		t.Fatal(v, e)
	}
	var a, b analysis.Task
	var ae, be error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a, ae = s.ClaimAnalysisTask(ctx, scope, v.ID, v.Tasks[0].ID, plan.Manifest) }()
	go func() { defer wg.Done(); b, be = s.ClaimAnalysisTask(ctx, scope, v.ID, v.Tasks[0].ID, plan.Manifest) }()
	wg.Wait()
	if ae != nil || be != nil || (a.Fence == "") == (b.Fence == "") {
		t.Fatal("concurrent dispatch", ae, be, a.Fence, b.Fence)
	}
	active := a
	if active.Fence == "" {
		active = b
	}
	_, e = s.pool.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(task,'{leaseUntil}',to_jsonb(now()-interval '1 hour')) where analysis_id=$1`, v.ID)
	if e != nil {
		t.Fatal(e)
	}
	held, e := s.ClaimAnalysisTask(ctx, scope, v.ID, active.ID, plan.Manifest)
	if e != nil || held.State != "HELD" || held.Fence != "" {
		t.Fatal(held, e)
	}
	if _, e = s.FinishAnalysisTask(ctx, scope, v.ID, active.ID, active.Fence, analysis.Finish{State: "SUCCEEDED"}); !errors.Is(e, analysis.ErrFenced) {
		t.Fatal("late worker", e)
	}
	resumed, e := s.PlanAnalysis(ctx, extension)
	if e != nil || resumed.Budget.Held != 1 {
		t.Fatal("replan resets hold", e)
	}
}

type analysisAuth struct {
	deny    bool
	version int64
}

func (a *analysisAuth) Authorize(_ context.Context, r orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if a.version != 0 {
		return orchestration.AuthorizationDecision{Allowed: !a.deny, BundleID: "test", BundleVersion: a.version, DecisionRef: fmt.Sprintf("test:%d:%s", a.version, r.CapabilityID)}, nil
	}
	return orchestration.AuthorizationDecision{Allowed: !a.deny, DecisionRef: "analysis-fixture"}, nil
}

type analysisGateway struct {
	profile   analysis.Profile
	calls     int
	transient bool
	rateLimit string
}

func (g *analysisGateway) Execute(_ context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	g.calls++
	if r.CapabilityID == "ouf.managed-source.preview" {
		b, _ := json.Marshal(g.profile)
		var v map[string]any
		json.Unmarshal(b, &v)
		v["view"] = "COLUMNS"
		var page struct{ Offset, Limit int }
		if err := json.Unmarshal(r.Arguments, &page); err != nil {
			return orchestration.GatewayResponse{}, err
		}
		end := min(g.profile.ColumnCount, page.Offset+page.Limit)
		columns := make([]map[string]any, end-page.Offset)
		for i := range columns {
			columns[i] = map[string]any{"name": "tipologia", "ordinal": page.Offset + i}
		}
		v["offset"], v["hasMore"], v["nextOffset"], v["columns"] = page.Offset, end < g.profile.ColumnCount, end, columns
		b, _ = json.Marshal(v)
		return orchestration.GatewayResponse{Status: 200, Body: b}, nil
	}
	if g.transient {
		g.transient = false
		return orchestration.GatewayResponse{Status: 503, Problem: &orchestration.Problem{Status: 503, RetryAfter: "60"}}, nil
	}
	if g.rateLimit != "" {
		header := g.rateLimit
		g.rateLimit = ""
		return orchestration.GatewayResponse{Status: 429, RetryAfter: header,
			Problem: &orchestration.Problem{Status: 429, Code: "RATE_LIMITED", RetryAfter: header}}, nil
	}
	return orchestration.GatewayResponse{Status: 200, Body: []byte(`{"items":[],"evidence":"fixture"}`)}, nil
}
func coordinatorFixture(t *testing.T, n int) (context.Context, *Store, analysis.Coordinator, orchestration.Identity, *analysisAuth, *analysisGateway, analysis.Profile) {
	ctx, s, scope, p, m := analysisFixture(t, n)
	a := &analysisAuth{}
	g := &analysisGateway{profile: p}
	service := &orchestration.Service{RequireDelegation: true, Auth: a, Admission: s, Gateway: g, FingerprintKey: make([]byte, 32)}
	return ctx, s, analysis.Coordinator{Store: s, Service: service, Manifest: m}, orchestration.Identity{ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant, ActorType: "HUMAN", Delegation: "fixture-memory-only"}, a, g, p
}

func TestInitialDiscoveryUsesVerifiedHeaderWithoutPublishedBatch(t *testing.T) {
	for _, state := range []string{"INACTIVE", "ABSENT"} {
		t.Run(state, func(t *testing.T) {
			ctx, db, c, identity, _, _, profile := coordinatorFixture(t, 1)
			for n, capability := range c.Manifest.Capabilities {
				if capability.ToolName == analysis.BatchTool {
					if state == "INACTIVE" {
						c.Manifest.Capabilities[n].PublicationState = state
					} else {
						c.Manifest.Capabilities = append(c.Manifest.Capabilities[:n], c.Manifest.Capabilities[n+1:]...)
					}
					break
				}
			}
			sum, err := c.Manifest.Checksum()
			if err != nil {
				t.Fatal(err)
			}
			payload, err := c.Manifest.CanonicalPayload()
			if err != nil {
				t.Fatal(err)
			}
			if err = db.EnsureManifest(ctx, sum, c.Manifest.Version, "mcp-manifest-v1", payload); err != nil {
				t.Fatal(err)
			}

			const header, description = "tipologia", "Tipo di soggetto che gestisce la strada"
			work := []analysis.Work{{Tool: "semantic.discovery.request", Columns: []int{0}, PreviewName: header,
				Arguments: json.RawMessage(fmt.Sprintf(`{"intent":%q,"preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`, description))}}
			view, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, work)
			if err != nil || len(view.Tasks) != 1 {
				t.Fatal(view, err)
			}
			scope := analysis.IdentityScope(identity)
			task, err := db.ReadAnalysisTask(ctx, scope, view.ID, view.Tasks[0].ID, sum)
			if err != nil {
				t.Fatal(err)
			}
			var args struct{ Intent, IdempotencyKey string }
			wantKey := "analysis:" + analysis.Digest([]any{view.ID, work[0].Tool, map[string]any{
				"intent": header, "preferredLanguages": []string{"it"}, "requestedArtifactType": "PROPERTY",
			}})
			if err := json.Unmarshal(task.Arguments, &args); err != nil || args.Intent != header ||
				args.IdempotencyKey != wantKey || task.DiscoveryText[0] != description {
				t.Fatal(task, args, err)
			}

			predecessor := manifest.AnalysisPredecessor(sum)
			rollback, err := db.ReadAnalysis(ctx, scope, view.ID, predecessor, 0, 1)
			if predecessor == "" || err != nil || rollback.ID != view.ID {
				t.Fatal(predecessor, rollback, err)
			}
		})
	}
}

func TestPublishedPlanIsReadableWithRollbackManifest(t *testing.T) {
	ctx, db, c, identity, _, _, profile := coordinatorFixture(t, 1)
	work := []analysis.Work{{Tool: "semantic.discovery.request", Columns: []int{0},
		Arguments: json.RawMessage(`{"intent":"tipologia","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}}
	view, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, work)
	if err != nil || view.Seed == "" {
		t.Fatal(view, err)
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	predecessor := manifest.AnalysisPredecessor(checksum)
	rollback, err := db.ReadAnalysis(ctx, analysis.IdentityScope(identity), view.ID, predecessor, 0, 1)
	if predecessor == "" || err != nil || rollback.ID != view.ID {
		t.Fatal(predecessor, rollback, err)
	}
}

func TestProfileAnalysisActualExecutionResumeAuthorizationAndWindowWait(t *testing.T) {
	ctx, s, c, i, a, g, p := coordinatorFixture(t, 100)
	work := []analysis.Work{}
	for n := 0; n < 61; n++ {
		work = append(work, searchWork(n))
	}
	view, e := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, work)
	if e != nil || view.Profile.ColumnCount != 100 {
		t.Fatal(view, e)
	}
	for n := 0; n < 60; n++ {
		task, e := c.Step(ctx, i, view.ID, view.Tasks[n].ID)
		if e != nil || task.State != "SUCCEEDED" {
			t.Fatal(n, task, e)
		}
	}
	before := g.calls
	wait, e := c.Step(ctx, i, view.ID, view.Tasks[60].ID)
	if e != nil || wait.State != "WAITING" || wait.Code != "ADMISSION_WINDOW_WAIT" || wait.Attempts != 0 || g.calls != before {
		t.Fatal(wait, e)
	}
	read, e := c.Read(ctx, i, view.ID, 0, 128)
	if e != nil || read.Budget.Calls != 60 || read.Budget.Waiting != 1 || read.Budget.Completed != 60 {
		t.Fatal(read.Budget, e)
	}
	other, e := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	c.Store = other
	saved, e := c.Step(ctx, i, view.ID, view.Tasks[0].ID)
	if e != nil || saved.State != "SUCCEEDED" || g.calls != before || len(saved.Result) == 0 {
		t.Fatal("resume replayed owner", saved, e)
	}
	a.deny = true
	if _, e = c.Step(ctx, i, view.ID, view.Tasks[0].ID); !errors.Is(e, orchestration.ErrUnauthorized) {
		t.Fatal("revoked cached read", e)
	}
	if _, e = c.Read(ctx, i, view.ID, 0, 128); !errors.Is(e, orchestration.ErrUnauthorized) {
		t.Fatal(e)
	}
	var rows int
	if e = s.pool.QueryRow(ctx, `select count(*) from ouf_mcp.tool_attempt where tenant_id=$1`, i.TenantID).Scan(&rows); e != nil || rows != 61 {
		t.Fatal("wait admitted a new attempt", rows, e)
	}
}
func TestProfileAnalysisTransientReadAndInactiveDiscovery(t *testing.T) {
	ctx, s, c, i, _, g, p := coordinatorFixture(t, 20)
	// Exercise an explicitly unpublished descriptor independently of the release catalogue.
	for n := range c.Manifest.Capabilities {
		if c.Manifest.Capabilities[n].ToolName == "semantic.discovery.request" {
			c.Manifest.Capabilities[n].PublicationState = "INACTIVE"
		}
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := c.Manifest.CanonicalPayload()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnsureManifest(ctx, checksum, c.Manifest.Version, "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	v, e := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, []analysis.Work{searchWork(0)})
	if e != nil {
		t.Fatal(e)
	}
	g.transient = true
	task, e := c.Step(ctx, i, v.ID, v.Tasks[0].ID)
	if e != nil || task.State != "WAITING" || task.Attempts != 1 || time.Until(task.NotBefore) < 50*time.Second {
		t.Fatal(task, e)
	}
	before := g.calls
	if _, e = c.Step(ctx, i, v.ID, task.ID); e != nil || g.calls != before {
		t.Fatal("wait busy loop", e)
	}
	_, e = s.pool.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(task,'{notBefore}',to_jsonb(now()-interval '1 minute')) where analysis_id=$1`, v.ID)
	if e != nil {
		t.Fatal(e)
	}
	task, e = c.Step(ctx, i, v.ID, task.ID)
	if e != nil || task.State != "SUCCEEDED" || task.Attempts != 2 || task.Calls != 2 {
		t.Fatal(task, e)
	}
	discovery := analysis.Work{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"requestedArtifactType":"PROPERTY","intent":"Proprietà che indica la tipologia del teatro","preferredLanguages":["it"]}`), Columns: []int{1}}
	plan, e := analysis.Build(analysis.IdentityScope(i), p, c.Manifest, v.Revision, []analysis.Work{discovery})
	if e != nil {
		t.Fatal(e)
	}
	v, e = s.PlanAnalysis(ctx, plan)
	if e != nil {
		t.Fatal(e)
	}
	before = g.calls
	if _, e = c.Step(ctx, i, v.ID, plan.Tasks[0].ID); !errors.Is(e, analysis.ErrUnavailable) || g.calls != before {
		t.Fatal("inactive discovery dispatched", e)
	}
}

func TestProfileAnalysisCoordinatorPlans1024WithOnePassOverOwnedProfile(t *testing.T) {
	ctx, _, c, i, _, g, p := coordinatorFixture(t, 1024)
	var v analysis.View
	for offset := 0; offset < 1024; offset += 128 {
		work := []analysis.Work{}
		for n := offset; n < offset+128; n++ {
			work = append(work, searchWork(n))
		}
		var e error
		v, e = c.Plan(ctx, i, p.AssetID, p.ProfileID, v.Revision, work)
		if e != nil {
			t.Fatal(offset, e)
		}
	}
	if v.Budget.PlannedCalls != 1024 || v.CoveredColumns != 1024 || g.calls != 8 {
		t.Fatal(v.Budget, v.CoveredColumns, g.calls)
	}
}

func TestProfileAnalysisEquivalentSearchOnNextProfileResumesSameTask(t *testing.T) {
	ctx, _, c, i, _, gateway, profile := coordinatorFixture(t, 1)
	for round := 1; round <= 3; round++ {
		profile.AssetID, profile.ProfileID = uuid.NewString(), uuid.NewString()
		gateway.profile = profile
		view, err := c.Plan(ctx, i, profile.AssetID, profile.ProfileID, 0, []analysis.Work{searchWork(0)})
		if err != nil {
			t.Fatal(err)
		}
		before := gateway.calls
		task, err := c.Step(ctx, i, view.ID, view.Tasks[0].ID)
		if err != nil {
			t.Fatal(err)
		}
		if round < 3 {
			if task.State != "SUCCEEDED" || gateway.calls != before+1 {
				t.Fatal("initial search failed", round, task)
			}
			continue
		}
		if task.State != "WAITING" || task.Dimension != "EQUIVALENT_ATTEMPTS" || task.Waits != 1 || task.Attempts != 0 || task.Calls != 0 || gateway.calls != before {
			t.Fatal("window refusal dispatched or consumed a task attempt", task)
		}
		// Cross the actual DB minute boundary, without editing a checkpoint or counter.
		delay := time.Until(task.NotBefore)
		if delay < 0 || delay > time.Minute {
			t.Fatal("invalid wait", delay)
		}
		time.Sleep(delay + 100*time.Millisecond)
		resumeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done, err := c.Step(resumeCtx, i, view.ID, task.ID)
		if err != nil || done.State != "SUCCEEDED" || done.ID != task.ID || done.Attempts != 1 || done.Calls != 1 || done.Waits != 1 || gateway.calls != before+1 {
			t.Fatal("same task failed to resume", done, err)
		}
	}
}

func TestProfileAnalysisFiniteRetryHistoryAndInflightCapacity(t *testing.T) {
	ctx, s, scope, p, m := analysisFixture(t, 20)
	plan, e := analysis.Build(scope, p, m, 0, []analysis.Work{searchWork(0), searchWork(1), searchWork(2), searchWork(3), searchWork(4)})
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.PlanAnalysis(ctx, plan)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.pool.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(task,'{attempts}','128') where analysis_id=$1 and task_id=$2`, v.ID, plan.Tasks[0].ID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.pool.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(task,'{firstAttemptAt}',to_jsonb(now()-interval '25 hours')) where analysis_id=$1 and task_id=$2`, v.ID, plan.Tasks[1].ID)
	if e != nil {
		t.Fatal(e)
	}
	for _, index := range []int{0, 1} {
		held, e := s.ClaimAnalysisTask(ctx, scope, v.ID, plan.Tasks[index].ID, plan.Manifest)
		if e != nil || held.State != "HELD" || held.Code != "RETRY_BUDGET_EXHAUSTED" {
			t.Fatal(held, e)
		}
	}
	_, e = s.pool.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(jsonb_set(task,'{state}','"SUCCEEDED"'),'{resultBytes}',to_jsonb($3::bigint)) where analysis_id=$1 and task_id=$2`, v.ID, plan.Tasks[2].ID, analysis.MaxRetainedBytes-analysis.MaxResultBytes)
	if e != nil {
		t.Fatal(e)
	}
	active, e := s.ClaimAnalysisTask(ctx, scope, v.ID, plan.Tasks[3].ID, plan.Manifest)
	if e != nil || active.Fence == "" {
		t.Fatal(active, e)
	}
	if _, e = s.ClaimAnalysisTask(ctx, scope, v.ID, plan.Tasks[4].ID, plan.Manifest); !errors.Is(e, analysis.ErrCapacity) {
		t.Fatal("inflight capacity unreserved", e)
	}
	resumed, e := s.PlanAnalysis(ctx, plan)
	if e != nil || resumed.Budget.Held != 2 || resumed.Tasks[0].Attempts != 128 {
		t.Fatal("retry history reset", resumed, e)
	}
}

func TestProfileAnalysisDiscoveryCandidateExecutionAndPollPacing(t *testing.T) {
	ctx, s, c, i, _, g, p := coordinatorFixture(t, 20)
	// Exercise the predecessor owner and its per-column status-observation path.
	for n := range c.Manifest.Capabilities {
		if c.Manifest.Capabilities[n].ToolName == analysis.BatchTool {
			c.Manifest.Capabilities[n].PublicationState = "INACTIVE"
		}
		if strings.HasPrefix(c.Manifest.Capabilities[n].ToolName, "semantic.discovery.") {
			c.Manifest.Capabilities[n].PublicationState = "ACTIVE"
		}
	}
	checksum, _ := c.Manifest.Checksum()
	payload, _ := c.Manifest.CanonicalPayload()
	if e := s.EnsureManifest(ctx, checksum, c.Manifest.Version, "mcp-manifest-v1", payload); e != nil {
		t.Fatal(e)
	}
	discovery := analysis.Work{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"requestedArtifactType":"PROPERTY","intent":"Proprietà che indica la tipologia del teatro","preferredLanguages":["it"]}`), Columns: []int{0}}
	v, e := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, []analysis.Work{discovery})
	if e != nil {
		t.Fatal(e)
	}
	if v.Tasks[0].Code != "PUBLISHED_BATCH_UNAVAILABLE" {
		t.Fatal("missing compatibility reason")
	}
	request, e := c.Step(ctx, i, v.ID, v.Tasks[0].ID)
	if e != nil || request.State != "SUCCEEDED" {
		t.Fatal(request, e)
	}
	before := g.calls
	if _, e = c.Step(ctx, i, v.ID, request.ID); e != nil || g.calls != before {
		t.Fatal("discovery request replayed", e)
	}
	// The requestId is a synthetic owner fixture, not an external candidate.
	args := json.RawMessage(`{"requestId":"11111111-1111-4111-8111-111111111111"}`)
	v, e = c.Plan(ctx, i, p.AssetID, p.ProfileID, v.Revision, []analysis.Work{{Tool: "semantic.discovery.status", Arguments: args, Columns: []int{0}, Observation: 1}, {Tool: "semantic.discovery.status", Arguments: args, Columns: []int{0}, Observation: 2}})
	if e != nil {
		t.Fatal(e)
	}
	first, e := c.Step(ctx, i, v.ID, v.Tasks[1].ID)
	if e != nil || first.State != "SUCCEEDED" {
		t.Fatal(first, e)
	}
	before = g.calls
	second, e := c.Step(ctx, i, v.ID, v.Tasks[2].ID)
	if e != nil || second.State != "WAITING" || second.Code != "OBSERVATION_INTERVAL" || second.Attempts != 0 || g.calls != before {
		t.Fatal(second, e)
	}
	if second.NotBefore.Sub(first.CompletedAt) != 31*time.Second {
		t.Fatal("poll pacing changed", second.NotBefore, first.CompletedAt)
	}
}

func TestPlannedDiscoveryUUIDReplayAndFinish(t *testing.T) {
	const canonical = `{"requestId":"abcdef01-2345-4abc-8def-0123456789ab"}`
	const uppercase = `{"requestId":"ABCDEF01-2345-4ABC-8DEF-0123456789AB"}`
	for _, tc := range []struct {
		name, arguments string
		historical      bool
	}{
		{name: "new-uppercase", arguments: uppercase},
		{name: "historical-canonical", arguments: canonical, historical: true},
		{name: "historical-uppercase", arguments: uppercase, historical: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, s, c, identity, _, gateway, profile := coordinatorFixture(t, 1)
			scope := analysis.IdentityScope(identity)
			c.CommandKey = uuid.NewString()
			work := analysis.Work{Tool: "semantic.discovery.status", Arguments: json.RawMessage(tc.arguments), Columns: []int{0}, Observation: 1}
			invalid := work
			invalid.Arguments = json.RawMessage(`{"requestId":"invalid"}`)
			if _, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, []analysis.Work{invalid}); !errors.Is(err, analysis.ErrInvalid) || gateway.calls != 0 {
				t.Fatal("non-UUID input reached the owner during planning", err, gateway.calls)
			}
			// Use the task identity and command receipt formula of 0269770.
			wantID := analysis.Digest([]any{work.Tool, work.Arguments, work.Observation})
			wantReceipt := analysis.Digest([]any{profile.AssetID, profile.ProfileID, int64(0), []analysis.Work{work}})
			if tc.historical {
				plan, err := analysis.Build(scope, profile, c.Manifest, 0, []analysis.Work{work})
				if err != nil {
					t.Fatal(err)
				}
				plan.Tasks[0].Arguments = work.Arguments
				plan.Tasks[0].ID = wantID
				plan.Headers = []string{"fixture"}
				plan.CommandKey = c.CommandKey
				plan.CommandDigest = wantReceipt
				if _, err = s.PlanAnalysis(ctx, plan); err != nil {
					t.Fatal(err)
				}
			}
			view, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, []analysis.Work{work})
			if err != nil || len(view.Tasks) != 1 || view.Tasks[0].ID != wantID {
				t.Fatal("UUID spelling changed the plan identity", view, err)
			}
			before := gateway.calls
			replayed, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, []analysis.Work{work})
			if err != nil || replayed.ID != view.ID || replayed.Revision != view.Revision || len(replayed.Tasks) != 1 ||
				replayed.Tasks[0].ID != wantID || gateway.calls != before {
				t.Fatal("same command opened new work", replayed, err, gateway.calls)
			}
			if target, found, err := planReceipt(ctx, s.pool, scope, c.CommandKey, wantReceipt); err != nil || !found || target != view.ID {
				t.Fatal("stored receipt does not match the predecessor digest", target, found, err)
			}
			changed := work
			changed.Arguments = json.RawMessage(canonical)
			if tc.arguments == canonical {
				changed.Arguments = json.RawMessage(uppercase)
			}
			if _, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, []analysis.Work{changed}); !errors.Is(err, analysis.ErrKeyReuse) || gateway.calls != before {
				t.Fatal("changed arguments reused the command key", err, gateway.calls)
			}
			checksum, _ := c.Manifest.Checksum()
			stored, err := s.ReadAnalysisTask(ctx, scope, view.ID, wantID, checksum)
			if err != nil {
				t.Fatal(err)
			}
			var gotArguments, wantArguments map[string]any
			if err := json.Unmarshal(stored.Arguments, &gotArguments); err != nil {
				t.Fatalf("invalid stored arguments: %v", err)
			}
			if err := json.Unmarshal(work.Arguments, &wantArguments); err != nil {
				t.Fatalf("invalid expected arguments: %v", err)
			}
			if gotArguments["requestId"] != wantArguments["requestId"] {
				t.Fatalf("unexpected stored requestId spelling: got %q, want %q", gotArguments["requestId"], wantArguments["requestId"])
			}
			if !reflect.DeepEqual(gotArguments, wantArguments) {
				t.Fatalf("unexpected stored arguments: got %v, want %v", gotArguments, wantArguments)
			}
			task, err := c.Step(ctx, identity, view.ID, wantID)
			if err != nil || task.State != "SUCCEEDED" || gateway.calls != before+1 {
				t.Fatal("owner result did not complete the task", task, err, gateway.calls)
			}
			stored, err = s.ReadAnalysisTask(ctx, scope, view.ID, wantID, checksum)
			if err != nil || stored.State != "SUCCEEDED" || len(stored.Result) == 0 {
				t.Fatal("owner result was not persisted", stored, err)
			}
		})
	}
}

func TestPlannedDiscoveryPageResolves128ConsecutiveObservations(t *testing.T) {
	ctx, s, scope, profile, m := analysisFixture(t, 1)
	args := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, uuid.NewString()))
	const observations = 128
	work := make([]analysis.Work, 0, observations)
	// The first row needs the entire chain. Every later row shares its suffix.
	for n := observations; n > 0; n-- {
		work = append(work, analysis.Work{Tool: "semantic.discovery.status", Columns: []int{0}, Arguments: args, Observation: n})
	}
	plan, err := analysis.Build(scope, profile, m, 0, work)
	if err != nil {
		t.Fatal(err)
	}
	root := &plan.Tasks[observations-1]
	root.State = "WAITING"
	root.NotBefore = time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err = s.PlanAnalysis(ctx, plan); err != nil {
		t.Fatal(err)
	}
	view, err := s.ReadAnalysis(ctx, scope, plan.ID, plan.Manifest, 0, observations)
	if err != nil || len(view.Tasks) != observations || view.HasMore {
		t.Fatal("expected a full page", len(view.Tasks), view.HasMore, err)
	}
	for n, task := range view.Tasks {
		if task.Observation != observations-n {
			t.Fatal("unexpected observation order", task.Observation, n)
		}
		predecessor := task.Predecessor
		if task.Observation == 1 {
			if predecessor != nil || task.ID != root.ID {
				t.Fatal("first observation has a predecessor", task, predecessor)
			}
			continue
		}
		if predecessor == nil || predecessor.ID != root.ID || predecessor.State != root.State || !predecessor.NotBefore.Equal(root.NotBefore) {
			t.Fatal("wrong chain root", task.Observation, predecessor)
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, reads, err := analysisTasks(ctx, tx, plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	visits := 0
	resolve := predecessorResolver(func(id string) plannedRead {
		visits++
		return reads[id]
	})
	for _, task := range view.Tasks[:observations-1] {
		link := reads[task.ID].predecessor
		if link == nil || resolve(*link).ID != root.ID {
			t.Fatal("wrong chain root in counted projection", task.Observation)
		}
	}
	if visits != observations-1 {
		t.Fatalf("visited %d predecessors, want %d", visits, observations-1)
	}
}

func plannedObservationFixture(t *testing.T, requests, observations int) (context.Context, pgx.Tx, analysis.Plan, time.Time) {
	t.Helper()
	ctx, s, scope, profile, m := analysisFixture(t, 1)
	plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{searchWork(0)})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	if _, err = planAnalysis(ctx, tx, plan); err != nil {
		t.Fatal(err)
	}
	due := time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = tx.Exec(ctx, `insert into ouf_mcp.profile_analysis_task(analysis_id,task_id,task)
		select $1,task_id,jsonb_build_object('taskId',task_id,'tool','semantic.discovery.status',
			'arguments',jsonb_build_object('requestId',case when observation%2=1 then upper(request_id) else request_id end),
			'observation',observation,'state',case when observation=1 then 'WAITING' else 'READY' end,
			'notBefore',case when observation=1 then $4::timestamptz else '0001-01-01T00:00:00Z'::timestamptz end,
			'columns',jsonb_build_array(0))
		from (
			select observation,md5(request_number::text)::uuid::text as request_id,
				lpad(to_hex((request_number-1)*$3::int+observation),64,'0') as task_id
			from generate_series(1,$2::int) requests(request_number)
			cross join generate_series(1,$3::int) observations(observation)
		) tasks order by task_id`, plan.ID, requests, observations, due)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `analyze ouf_mcp.profile_analysis_task`); err != nil {
		t.Fatal(err)
	}
	return ctx, tx, plan, due
}

func TestPlannedDiscoveryPagePredecessorQueryPlan(t *testing.T) {
	const requests, observations = 1024, 4
	ctx, tx, plan, due := plannedObservationFixture(t, requests, observations)
	var raw []byte
	if err := tx.QueryRow(ctx, `explain (format json) `+analysisTasksSQL, plan.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	type planNode struct {
		Type  string     `json:"Node Type"`
		CTE   string     `json:"CTE Name"`
		Plans []planNode `json:"Plans"`
	}
	var explain []struct{ Plan planNode }
	if err := json.Unmarshal(raw, &explain); err != nil || len(explain) != 1 {
		t.Fatal("invalid EXPLAIN output", err, string(raw))
	}
	var checkJoin func(planNode, string)
	var predecessorJoined bool
	checkJoin = func(node planNode, join string) {
		switch node.Type {
		case "Nested Loop", "Hash Join", "Merge Join":
			join = node.Type
		}
		if node.CTE == "status_predecessors" {
			if join != "Hash Join" && join != "Merge Join" {
				t.Fatalf("predecessor uses %q: %s", join, raw)
			}
			predecessorJoined = true
		}
		for _, child := range node.Plans {
			checkJoin(child, join)
		}
	}
	checkJoin(explain[0].Plan, "")
	if !predecessorJoined {
		t.Fatal("missing predecessor join", string(raw))
	}
	view, err := analysisView(ctx, tx, plan.Scope, plan.ID, plan.Profile, 1, requests*observations, 1)
	if err != nil || len(view.Tasks) != 1 || view.HasMore || view.Budget.PlannedCalls != requests*observations+1 {
		t.Fatal("incorrect final page or plan totals", view, err)
	}
	task := view.Tasks[0]
	wantPredecessor := fmt.Sprintf("%064x", (requests-1)*observations+1)
	if task.ID != fmt.Sprintf("%064x", requests*observations) || task.Predecessor == nil ||
		task.Predecessor.ID != wantPredecessor || task.Predecessor.State != "WAITING" || !task.Predecessor.NotBefore.Equal(due) {
		t.Fatal("final page lost its off-page predecessor", task, task.Predecessor)
	}
}

func TestPlannedDiscoverySuccessorQueryPlan(t *testing.T) {
	const requests, observations = 1024, 4
	ctx, tx, plan, _ := plannedObservationFixture(t, requests, observations)
	last := requests * observations
	var raw []byte
	if err := tx.QueryRow(ctx, `explain (format json) `+analysisSuccessorSQL, plan.ID,
		fmt.Sprintf("%064x", last), []string{fmt.Sprintf("%064x", last-1)}).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	type planNode struct {
		Type      string     `json:"Node Type"`
		Index     string     `json:"Index Name"`
		Condition string     `json:"Index Cond"`
		Plans     []planNode `json:"Plans"`
	}
	var explain []struct{ Plan planNode }
	if err := json.Unmarshal(raw, &explain); err != nil || len(explain) != 1 {
		t.Fatal("invalid EXPLAIN output", err, string(raw))
	}
	var check func(planNode)
	var lookups int
	check = func(node planNode) {
		if node.Index == "profile_analysis_task_pkey" && strings.Contains(node.Condition, "analysis_id") && strings.Contains(node.Condition, "task_id") {
			lookups++
		}
		if node.Type == "Seq Scan" || node.Type == "Nested Loop" {
			t.Fatalf("successor query uses %s: %s", node.Type, raw)
		}
		for _, child := range node.Plans {
			check(child)
		}
	}
	check(explain[0].Plan)
	if lookups != 2 {
		t.Fatal("expected indexed lookups for both observations", string(raw))
	}
}

func TestPlannedDiscoverySuccessorUsesCurrentPlanRevision(t *testing.T) {
	ctx, s, scope, profile, m := analysisFixture(t, 1)
	const tool = "semantic.discovery.status"
	const canonical = `{"requestId":"abcdef01-2345-4abc-8def-0123456789ab"}`
	const mixed = `{"requestId":"AbcDEf01-2345-4ABc-8DEf-0123456789aB"}`
	const uppercase = `{"requestId":"ABCDEF01-2345-4ABC-8DEF-0123456789AB"}`
	work := analysis.Work{Tool: tool, Columns: []int{0}, Arguments: json.RawMessage(canonical), Observation: 1}
	plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{work})
	if err != nil {
		t.Fatal(err)
	}
	first := &plan.Tasks[0]
	first.State = "SUCCEEDED"
	first.CompletedAt = time.Date(2039, 12, 31, 0, 0, 0, 0, time.UTC)
	view, err := s.PlanAnalysis(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := s.ReadAnalysisSuccessor(ctx, scope, plan.ID, plan.Manifest, *first); err != nil || next != nil {
		t.Fatal("unexpected successor before extending the plan", next, err)
	}
	newPlan, err := analysis.Build(scope, profile, m, view.Revision, []analysis.Work{
		{Tool: tool, Columns: []int{0}, Arguments: json.RawMessage(uppercase), Observation: 1},
		{Tool: tool, Columns: []int{0}, Arguments: json.RawMessage(mixed), Observation: 2},
		{Tool: tool, Columns: []int{0}, Arguments: json.RawMessage(canonical), Observation: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	equivalent := &newPlan.Tasks[0]
	equivalent.State = "SUCCEEDED"
	equivalent.CompletedAt = first.CompletedAt.Add(time.Minute)
	if _, err = s.PlanAnalysis(ctx, newPlan); err != nil {
		t.Fatal(err)
	}
	successor := newPlan.Tasks[1]
	for range 2 {
		next, err := s.ReadAnalysisSuccessor(ctx, scope, plan.ID, plan.Manifest, *first)
		if err != nil || next == nil || next.ID != successor.ID || next.State != successor.State ||
			!next.NotBefore.Equal(equivalent.CompletedAt.Add(31*time.Second)) {
			t.Fatal("lost the first successor or the latest equivalent pacing floor", next, err)
		}
	}
}

func TestLostPlannedObservationResumesAtRequestDeadline(t *testing.T) {
	ctx, s, c, identity, _, _, profile := coordinatorFixture(t, 2)
	scope := analysis.IdentityScope(identity)
	args := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, uuid.NewString()))
	other := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, uuid.NewString()))
	p, err := analysis.Build(scope, profile, c.Manifest, 0, []analysis.Work{
		{Tool: "semantic.discovery.status", Arguments: args, Columns: []int{0}, Observation: 1},
		{Tool: "semantic.discovery.status", Arguments: args, Columns: []int{0}, Observation: 2},
		{Tool: "semantic.discovery.status", Arguments: other, Columns: []int{1}, Observation: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PlanAnalysis(ctx, p); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimAnalysisTask(ctx, scope, p.ID, p.Tasks[0].ID, p.Manifest)
	if err != nil || first.Fence == "" {
		t.Fatal(first, err)
	}
	observed := time.Now().UTC().Truncate(time.Microsecond)
	body := []byte(`{"state":"RUNNING","pollAfterSeconds":72}`)
	obs := orchestration.ObserveOwner(orchestration.GatewayResponse{Status: 200, Body: body}, nil, observed)
	if _, err = s.FinishAnalysisTask(ctx, scope, p.ID, first.ID, first.Fence, analysis.Finish{
		State: "SUCCEEDED", Calls: 1, Body: body, Observation: obs, NotBefore: obs.Due,
	}); err != nil {
		t.Fatal(err)
	}
	// Lose the reply. The resumed page does not even contain observation one.
	v, err := c.Read(ctx, identity, p.ID, 1, 2)
	if err != nil || len(v.Tasks) != 2 || !v.Tasks[1].NotBefore.IsZero() {
		t.Fatal(v, err)
	}
	wire, err := json.Marshal(v)
	var page nextstep.Object
	if err != nil || json.Unmarshal(wire, &page) != nil {
		t.Fatal("invalid plan page", err)
	}
	read, _ := analysis.Find(c.Manifest, "source.analysis.read")
	signer, err := nextstep.NewSigner(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	now := observed.Add(41 * time.Second)
	engine := nextstep.Engine{Signer: signer, Binding: []byte("reader"), Now: func() time.Time { return now },
		Valid: func(string, nextstep.Object) bool { return true }}
	input := nextstep.Object{"analysisId": p.ID, "offset": 1}
	build := func() nextstep.Object {
		return engine.Evaluate(read, c.Manifest.NextPolicies[read.NextPolicy], input, page, false)
	}
	next := build()
	wait, _ := next["arguments"].(nextstep.Object)
	token, _ := wait["waitRef"].(string)
	ref, err := signer.Open(token, engine.Binding)
	if err != nil || next["tool"] != "ouf.wait" || ref.Read != "source.analysis.step" ||
		ref.NotBefore != observed.Add(72*time.Second).Format(time.RFC3339Nano) || ref.Args["taskId"] != p.Tasks[1].ID {
		t.Fatal(next, ref, err)
	}
	now = obs.Due
	if next := build(); next["tool"] != "source.analysis.step" || next["arguments"].(nextstep.Object)["taskId"] != p.Tasks[1].ID {
		t.Fatal("wrong task at expiry", next)
	}
	held, err := s.ClaimAnalysisTask(ctx, scope, p.ID, p.Tasks[1].ID, p.Manifest)
	if err != nil || held.State != "WAITING" || held.Attempts != 0 || held.Fence != "" || !held.NotBefore.Equal(obs.Due) {
		t.Fatal("direct step ignored the request clock", held, err)
	}
}

func TestWaitingPlannedPredecessorResumesAcrossPages(t *testing.T) {
	ctx, s, c, identity, _, owner, profile := coordinatorFixture(t, 2)
	scope := analysis.IdentityScope(identity)
	request := uuid.NewString()
	args := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, request))
	other := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, uuid.NewString()))
	plan, err := analysis.Build(scope, profile, c.Manifest, 0, []analysis.Work{
		{Tool: "semantic.discovery.status", Arguments: args, Columns: []int{0}, Observation: 1},
		{Tool: "semantic.discovery.status", Arguments: args, Columns: []int{0}, Observation: 2},
		{Tool: "semantic.discovery.status", Arguments: other, Columns: []int{1}, Observation: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PlanAnalysis(ctx, plan); err != nil {
		t.Fatal(err)
	}
	first, second, independent := plan.Tasks[0].ID, plan.Tasks[1].ID, plan.Tasks[2].ID
	owner.rateLimit = "72"
	a, err := c.Step(ctx, identity, plan.ID, first)
	if err != nil || a.State != "WAITING" || a.Code != "TRANSIENT_OWNER_WAIT" || owner.calls != 1 {
		t.Fatal(a, err, owner.calls)
	}
	saved, err := c.Service.SavedObservation(ctx, identity, "ouf.semantic.discovery.status", "READ", args)
	if err != nil || !saved.Due.Equal(a.NotBefore) {
		t.Fatal("lost the recorded owner clock", saved, a, err)
	}
	now := a.NotBefore.Add(-72 * time.Second)
	read, _ := analysis.Find(c.Manifest, "source.analysis.read")
	signer, err := nextstep.NewSigner(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	engine := nextstep.Engine{Signer: signer, Binding: []byte("reader"), Now: func() time.Time { return now },
		Valid: func(string, nextstep.Object) bool { return true }}
	readPage := func(offset, limit int) nextstep.Object {
		t.Helper()
		page, err := c.Read(ctx, identity, plan.ID, offset, limit)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := json.Marshal(page)
		var observation nextstep.Object
		if err != nil || json.Unmarshal(wire, &observation) != nil {
			t.Fatal("invalid plan page", err)
		}
		return engine.Evaluate(read, c.Manifest.NextPolicies[read.NextPolicy], nextstep.Object{"analysisId": plan.ID, "offset": offset}, observation, false)
	}
	assertStep := func(next nextstep.Object, taskID string) {
		t.Helper()
		args, _ := next["arguments"].(nextstep.Object)
		if next["action"] != "CALL_TOOL" || next["tool"] != "source.analysis.step" || args["taskId"] != taskID || !nextstep.CheckedOwnerCall(next) {
			t.Fatal("unexpected next task", next)
		}
	}
	assertStep(readPage(0, 3), independent)
	if task, err := c.Step(ctx, identity, plan.ID, independent); err != nil || task.State != "SUCCEEDED" || owner.calls != 2 {
		t.Fatal(task, err, owner.calls)
	}
	for _, offset := range []int{0, 1} {
		next := readPage(offset, 2)
		wait, _ := next["arguments"].(nextstep.Object)
		token, _ := wait["waitRef"].(string)
		resume, _ := wait["resumeArguments"].(nextstep.Object)
		ref, err := signer.Open(token, engine.Binding)
		if err != nil || next["tool"] != "ouf.wait" || !ref.RestoreArgs(resume) || ref.Read != "source.analysis.step" ||
			ref.Args["taskId"] != first || ref.NotBefore != a.NotBefore.UTC().Format(time.RFC3339Nano) {
			t.Fatal("missing predecessor wait", offset, next, ref, err)
		}
	}
	if task, err := c.Step(ctx, identity, plan.ID, first); err != nil || task.State != "WAITING" || owner.calls != 2 {
		t.Fatal("premature owner call", task, err, owner.calls)
	}
	now = a.NotBefore
	assertStep(readPage(0, 2), first)
	assertStep(readPage(1, 2), first)

	// Age only this fixture's persisted clocks, leaving PostgreSQL's claim
	// clock authoritative and avoiding a real 72-second deadline.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `update ouf_mcp.tool_attempt set owner_due=transaction_timestamp() where attempt_id=$1`, a.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `update ouf_mcp.analysis_owner_due set not_before=transaction_timestamp() where analysis_id=$1 and request_id=$2`, plan.ID, request); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(task,'{notBefore}',to_jsonb(transaction_timestamp()))
		where analysis_id=$1 and task_id=$2`, plan.ID, first); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	a, err = c.Step(ctx, identity, plan.ID, first)
	if err != nil || a.State != "SUCCEEDED" || owner.calls != 3 {
		t.Fatal("predecessor did not resume", a, err, owner.calls)
	}
	reopened, err := c.Read(ctx, identity, plan.ID, 1, 1)
	if err != nil || len(reopened.Tasks) != 1 || reopened.Tasks[0].Predecessor != nil || reopened.Tasks[0].NotBefore.Sub(a.CompletedAt) != 31*time.Second {
		t.Fatal("successor lost its normal pacing", reopened, err)
	}
	now = a.CompletedAt.Add(31 * time.Second)
	assertStep(readPage(1, 1), second)
}

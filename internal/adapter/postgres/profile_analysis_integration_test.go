package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
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

type analysisAuth struct{ deny bool }

func (a *analysisAuth) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: !a.deny, DecisionRef: "analysis-fixture"}, nil
}

type analysisGateway struct {
	profile   analysis.Profile
	calls     int
	transient bool
}

func (g *analysisGateway) Execute(_ context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	g.calls++
	if r.CapabilityID == "ouf.managed-source.preview" {
		b, _ := json.Marshal(g.profile)
		var v map[string]any
		json.Unmarshal(b, &v)
		v["view"] = "COLUMNS"
		v["offset"] = 0
		v["columns"] = []any{map[string]any{"name": "tipologia"}}
		b, _ = json.Marshal(v)
		return orchestration.GatewayResponse{Status: 200, Body: b}, nil
	}
	if g.transient {
		g.transient = false
		return orchestration.GatewayResponse{Status: 503, Problem: &orchestration.Problem{Status: 503, RetryAfter: "60"}}, nil
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

func TestProfileAnalysisCoordinatorPlans1024WithOneOwnedProfileRead(t *testing.T) {
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
	if v.Budget.PlannedCalls != 1024 || v.CoveredColumns != 1024 || g.calls != 1 {
		t.Fatal(v.Budget, v.CoveredColumns, g.calls)
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
	// Activation is confined to this fixture; the production descriptors stay INACTIVE.
	for n := range c.Manifest.Capabilities {
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

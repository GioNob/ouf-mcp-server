package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
)

func TestNormalLegacyPlanSharesColumnReformulationBudget(t *testing.T) {
	ctx, db, c, identity, _, _, profile := coordinatorFixture(t, 2)
	limits, err := analysis.SummaryLimits(c.Manifest)
	if err != nil || limits.MaxColumnReformulations != 7 {
		t.Fatal(limits, err)
	}
	limit := limits.MaxColumnReformulations
	work := analysis.Work{Tool: "semantic.discovery.request", Columns: []int{0}}
	form := func(n int) []analysis.Work {
		work.Arguments = json.RawMessage(fmt.Sprintf(`{"intent":"term %d","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`, n))
		return []analysis.Work{work}
	}
	legacy, err := analysis.Build(analysis.IdentityScope(identity), profile, c.Manifest, 0, form(0))
	if err != nil {
		t.Fatal(err)
	}
	legacy.Headers = []string{"col_a", "col_b"}
	v, err := db.PlanAnalysis(ctx, legacy)
	if err != nil || v.Seed != "" {
		t.Fatal(v, err)
	}
	task, err := db.ClaimAnalysisTask(ctx, legacy.Scope, v.ID, v.Tasks[0].ID, legacy.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	request := uuid.NewString()
	body, _ := json.Marshal(map[string]string{"requestId": request, "state": "PENDING"})
	if _, err = db.FinishAnalysisTask(ctx, legacy.Scope, v.ID, task.ID, task.Fence, analysis.Finish{State: "SUCCEEDED", Calls: 1, Body: body}); err != nil {
		t.Fatal(err)
	}
	empty := []byte(`{"state":"SUCCEEDED","candidates":[],"completedCandidateCount":0}`)
	if err := db.ObserveDiscovery(ctx, legacy.Scope, v.ID, legacy.Manifest, "semantic.discovery.candidates", request, empty, time.Time{}); err != nil {
		t.Fatal(err)
	}
	v, err = c.Reformulate(ctx, identity, v.ID, uuid.NewString(), analysis.Reformulation{Column: 0, RequestID: request, Intent: "term 1"}, limit)
	if err != nil || len(v.Tasks) != 2 {
		t.Fatal(v, err)
	}
	tooLong := form(2)
	tooLong[0].Arguments = json.RawMessage(`{"intent":"a long description of the column","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)
	if _, err = c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, v.Revision, tooLong); !errors.Is(err, analysis.ErrReformulationTermTooLong) {
		t.Fatal("normal plan accepted a long reformulation", err)
	}
	for n := 2; n <= limit; n++ {
		v, err = c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, v.Revision, form(n))
		if err != nil || len(v.Tasks) != n+1 {
			t.Fatal(n, v, err)
		}
	}
	var used int
	err = db.pool.QueryRow(ctx, `select used from ouf_mcp.analysis_reformulation_budget where service_principal_id=$1
		and principal_id=$2 and tenant_id=$3 and asset_id=$4 and profile_id=$5 and ordinal=0`, legacy.Scope.Service,
		legacy.Scope.Principal, legacy.Scope.Tenant, profile.AssetID, profile.ProfileID).Scan(&used)
	if err != nil || used != limit {
		t.Fatal("normal and explicit plans did not share the counter", used, err)
	}
	replay, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, form(limit))
	if err != nil || replay.Revision != v.Revision {
		t.Fatal("duplicate consumed budget", replay, err)
	}
	var refusal *analysis.ReformulationBudgetError
	if _, err = c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, v.Revision, form(limit+1)); !errors.Is(err, analysis.ErrReformulationExhausted) ||
		!errors.As(err, &refusal) || refusal.AnalysisID != v.ID || refusal.RequestID != "" {
		t.Fatal("normal plan did not report the exhausted budget", err)
	}
	work.Columns = []int{1}
	v, err = c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, v.Revision, form(limit+1))
	if err != nil || len(v.Tasks) != limit+2 {
		t.Fatal("independent ordinal blocked", v, err)
	}
}

func TestColumnReformulationFencesRequestsAndBudgets(t *testing.T) {
	ctx, db, c, identity, _, _, profile := coordinatorFixture(t, 2)
	limits, err := analysis.SummaryLimits(c.Manifest)
	if err != nil || limits.MaxColumnReformulations != 7 {
		t.Fatal(limits, err)
	}
	limit := limits.MaxColumnReformulations
	scope := analysis.IdentityScope(identity)
	work := analysis.Work{Tool: "semantic.discovery.request", Columns: []int{0, 1},
		Arguments: json.RawMessage(`{"intent":"codice_strada","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}
	p, err := c.Plan(ctx, identity, profile.AssetID, profile.ProfileID, 0, []analysis.Work{work})
	if err != nil || len(p.Tasks) != 1 {
		t.Fatal(p, err)
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	parent, err := db.ClaimAnalysisTask(ctx, scope, p.ID, p.Tasks[0].ID, checksum)
	var batch analysis.Batch
	if err != nil || json.Unmarshal(parent.Arguments, &batch) != nil || len(batch.Intents) != 1 || batch.Intents[0].Text != "codice_strada" {
		t.Fatal("existing raw intent did not survive published planning", err)
	}
	none := fmt.Sprintf(`{"contract":%q,"results":[{"intentId":%q,"state":"NONE","hasMore":false,"hits":[]}]}`, analysis.BatchContract, batch.Intents[0].ID)
	if _, err = db.FinishAnalysisTask(ctx, scope, p.ID, parent.ID, parent.Fence, analysis.Finish{State: "SUCCEEDED", Body: []byte(none), Calls: 1}); err != nil {
		t.Fatal(err)
	}
	p, err = c.Read(ctx, identity, p.ID, 0, 128)
	if err != nil || len(p.Tasks) != 2 {
		t.Fatal(p, err)
	}
	taskID := p.Tasks[1].ID
	for used := 0; used <= limit; used++ {
		task, err := db.ClaimAnalysisTask(ctx, scope, p.ID, taskID, checksum)
		if err != nil {
			t.Fatal(err)
		}
		request := uuid.NewString()
		receipt := json.RawMessage(fmt.Sprintf(`{"requestId":%q,"state":"PENDING"}`, request))
		if _, err = db.FinishAnalysisTask(ctx, scope, p.ID, taskID, task.Fence, analysis.Finish{State: "SUCCEEDED", Body: receipt, Calls: 1}); err != nil {
			t.Fatal(err)
		}
		page := []byte(`{"state":"SUCCEEDED","candidates":[],"completedCandidateCount":0}`)
		if used == 1 {
			page = []byte(`{"state":"SUCCEEDED","candidates":[]}`)
		}
		if err = db.ObserveDiscovery(ctx, scope, p.ID, checksum, "semantic.discovery.candidates", request, page, time.Time{}); err != nil {
			t.Fatal(err)
		}
		// Simulate a pre-018 plan on every resume, including the exhausted
		// plan. Neither the next hint nor a new command may trust the cache.
		if _, err = db.pool.Exec(ctx, `delete from ouf_mcp.analysis_reformulation_budget
			where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and asset_id=$4 and profile_id=$5`,
			scope.Service, scope.Principal, scope.Tenant, profile.AssetID, profile.ProfileID); err != nil {
			t.Fatal(err)
		}
		col, available, err := db.ReformulationColumn(ctx, scope, p.ID, checksum, request, limit)
		if err != nil || available != (used < limit) || (available && col != 0) {
			t.Fatal(col, available, err)
		}
		key := uuid.NewString()
		r := analysis.Reformulation{Column: 0, RequestID: request, Intent: fmt.Sprintf("Codice via %d", used+1)}
		if used == 0 {
			long := r
			long.Intent = "Identificatore comunale della via"
			if _, err := c.Reformulate(ctx, identity, p.ID, key, long, limit); !errors.Is(err, analysis.ErrReformulationTermTooLong) {
				t.Fatal("accepted a long reformulation", err)
			}
		}
		v, err := c.Reformulate(ctx, identity, p.ID, key, r, limit)
		if used == limit {
			if !errors.Is(err, analysis.ErrReformulationExhausted) {
				t.Fatal("exhausted budget", err)
			}
			break
		}
		if err != nil || v.ID != p.ID || len(v.Tasks) != used+3 {
			t.Fatal(v, err)
		}
		taskID = v.Tasks[len(v.Tasks)-1].ID
		saved, err := db.ReadAnalysisTask(ctx, scope, p.ID, taskID, checksum)
		var args struct{ Intent, IdempotencyKey string }
		if err != nil || json.Unmarshal(saved.Arguments, &args) != nil || args.Intent != r.Intent ||
			len(saved.Columns) != 1 || saved.Columns[0] != 0 || saved.OwnerKey == "" || saved.OwnerKey != args.IdempotencyKey {
			t.Fatal(saved, err)
		}
		replay, err := c.Reformulate(ctx, identity, p.ID, key, r, limit)
		if err != nil || replay.Revision != v.Revision {
			t.Fatal("command replay", replay, err)
		}
		if _, err = c.Reformulate(ctx, identity, p.ID, uuid.NewString(), r, limit); !errors.Is(err, analysis.ErrReplanState) {
			t.Fatal("stale request", err)
		}
		summary, err := db.ReadSummary(ctx, scope, p.ID, checksum, analysis.Position{Seq: math.MaxInt64, Last: -1, Limit: 2}, true)
		if err != nil || summary.Counts.Zero != 0 || summary.Counts.Pending != 1 || summary.Counts.Blocked != 1 || summary.Hint.TaskID != taskID {
			t.Fatal(summary, err)
		}
	}
}

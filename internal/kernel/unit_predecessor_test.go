package kernel

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestUnitPredecessorPlan(t *testing.T) { // MCP-62E-07
	var saved struct {
		Scope            analysis.Scope
		Profile          analysis.Profile
		Task             analysis.Task
		AnalysisID       string
		ManifestChecksum string
		Revision         int64
	}
	raw, err := os.ReadFile("testdata/legacy-unit-plan.json")
	if err != nil || json.Unmarshal(raw, &saved) != nil {
		t.Fatal("invalid predecessor fixture", err)
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	checksum, err := m.Checksum()
	if err != nil || manifest.AnalysisPredecessor(checksum) != saved.ManifestChecksum ||
		analysis.AnalysisID(saved.Scope, saved.Profile) != saved.AnalysisID || saved.Profile.UnitRef != "" {
		t.Fatal("4b39568 plan incompatible", checksum, err)
	}
	t.Run("Postgres", func(t *testing.T) {
		dsn := os.Getenv("MCP_TEST_DATABASE_URL")
		if dsn == "" {
			t.Skip("MCP_TEST_DATABASE_URL required")
		}
		ctx := context.Background()
		db, err := postgres.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := postgres.Migrate(ctx, db.Pool()); err != nil {
			t.Fatal(err)
		}
		pin, _ := json.Marshal(saved.Profile)
		task, _ := json.Marshal(saved.Task)
		tx, err := db.Pool().Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `insert into ouf_mcp.profile_analysis
			(analysis_id,service_principal_id,principal_id,tenant_id,profile,manifest_checksum,revision)
			values($1,$2,$3,$4,$5,$6,$7) on conflict do nothing`, saved.AnalysisID, saved.Scope.Service, saved.Scope.Principal, saved.Scope.Tenant, pin, saved.ManifestChecksum, saved.Revision)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `insert into ouf_mcp.profile_analysis_task(analysis_id,task_id,task) values($1,$2,$3) on conflict do nothing`, saved.AnalysisID, saved.Task.ID, task)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		service := &orchestration.Service{Auth: analysisSDKAuth{}, Admission: &analysisSDKStore{}, Gateway: &unitPageOwner{}, FingerprintKey: make([]byte, 32)}
		c := analysis.Coordinator{Manifest: m, Store: db, Service: service, PreviewPageSize: 1}
		i := orchestration.Identity{ServicePrincipalID: saved.Scope.Service, PrincipalID: saved.Scope.Principal, TenantID: saved.Scope.Tenant, ActorType: "HUMAN", Delegation: "fixture"}
		view, err := c.PlanUnit(ctx, i, fixtureUnit("table:0"), saved.Revision, []analysis.Work{saved.Task.Work})
		if err != nil || view.Revision != saved.Revision || view.Profile != saved.Profile || len(view.Tasks) != 1 || view.Tasks[0].ID != saved.Task.ID {
			t.Fatal("predecessor replay changed the plan", view, err)
		}
		previous, err := db.ReadAnalysis(ctx, saved.Scope, saved.AnalysisID, saved.ManifestChecksum, 0, 1)
		if err != nil || previous.Revision != view.Revision || previous.Profile != view.Profile {
			t.Fatal("predecessor no longer reads its plan", err, previous)
		}
		var stored string
		if err := db.Pool().QueryRow(ctx, "select manifest_checksum from ouf_mcp.profile_analysis where analysis_id=$1", saved.AnalysisID).Scan(&stored); err != nil || stored != saved.ManifestChecksum {
			t.Fatal("stored manifest changed", stored, err)
		}

		// Also exercise rollback of a CSV plan first written by the candidate.
		i.PrincipalID = uuid.NewString()
		fresh, err := c.PlanUnit(ctx, i, fixtureUnit("table:0"), 0, []analysis.Work{saved.Task.Work})
		if err != nil {
			t.Fatal(err)
		}
		scope := analysis.IdentityScope(i)
		before, err := db.ReadAnalysis(ctx, scope, fresh.ID, saved.ManifestChecksum, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		after, err := db.ReadAnalysis(ctx, scope, fresh.ID, checksum, 0, 1)
		if err != nil || !reflect.DeepEqual(before, after) || after.Revision != 1 {
			t.Fatal("CSV plan changed across manifest versions", before, after, err)
		}
	})
}

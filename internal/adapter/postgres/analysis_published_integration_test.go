package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type publishedFailureOwner struct {
	preview *analysisGateway
	status  int
	code    string
	batches int
}

func (g *publishedFailureOwner) Execute(ctx context.Context, r orchestration.GatewayRequest, d time.Duration) (orchestration.GatewayResponse, error) {
	if r.CapabilityID == "ouf.managed-source.preview" {
		return g.preview.Execute(ctx, r, d)
	}
	if r.CapabilityID != "ouf.semantic.search.batch" {
		return orchestration.GatewayResponse{}, errors.New("unexpected remote fallback")
	}
	g.batches++
	if g.status != 200 {
		return orchestration.GatewayResponse{Status: g.status, Problem: &orchestration.Problem{Status: g.status, Code: g.code, RetryAfter: "7200"}}, nil
	}
	return orchestration.GatewayResponse{Status: 200, Body: []byte(`{"contract":"ouf.semantic.published-batch.v1","results":[]}`)}, nil
}

func TestPublishedDurableFailuresAndCompatibility(t *testing.T) { // MCP-50T-07
	for _, tc := range []struct {
		status              int
		code, state, reason string
		children            int
	}{
		{429, "SEM_BATCH_RATE_LIMITED", "WAITING", "TRANSIENT_OWNER_WAIT", 0},
		{503, "SEM_BATCH_UNAVAILABLE", "WAITING", "TRANSIENT_OWNER_WAIT", 0},
		{200, "", "HELD", analysis.InvalidOwner, 0},
		{403, "SEM_READ_RECEIPT_OR_POLICY_DENIED", "HELD", "OWNER_AUTHORIZATION_DENIED", 0},
		{404, "ROUTE_NOT_FOUND", "SUCCEEDED", "PUBLISHED_BATCH_UNAVAILABLE", 4},
		{413, "SEM_BATCH_REQUEST_TOO_LARGE", "SUCCEEDED", "PUBLISHED_BATCH_SPLIT", 2},
		{422, "SEM_BATCH_RESULT_TOO_LARGE", "SUCCEEDED", "PUBLISHED_BATCH_SPLIT", 2},
	} {
		t.Run(tc.reason+tc.code, func(t *testing.T) {
			ctx, s, c, i, _, g, p := coordinatorFixture(t, 4)
			owner := &publishedFailureOwner{preview: g, status: tc.status, code: tc.code}
			c.Service.Gateway = owner
			work := analysis.Work{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"intent":"other","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`), Columns: []int{0}}
			v, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, []analysis.Work{work})
			if err != nil || len(v.Tasks) != 1 {
				t.Fatal(v, err)
			}
			task, err := c.Step(ctx, i, v.ID, v.Tasks[0].ID)
			var denied *analysis.OwnerDenial
			if (tc.status == 403 && !errors.As(err, &denied)) || (tc.status != 403 && err != nil) || task.State != tc.state || task.Code != tc.reason {
				t.Fatal(task, err)
			}
			// Reopen the store as a new executor.
			other, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			c.Store = other
			checksum, err := c.Manifest.Checksum()
			if err != nil {
				t.Fatal(err)
			}
			next, err := c.Read(ctx, i, v.ID, 0, 128)
			if err != nil || len(next.Tasks) != 1+tc.children {
				t.Fatal(next, err)
			}
			if tc.status == 404 && next.Compatibility != tc.reason {
				t.Fatal(next)
			}
			keys := map[string]bool{}
			for _, child := range next.Tasks[1:] {
				if tc.status != 404 {
					if child.Tool != analysis.BatchTool {
						t.Fatal("fallback after failed batch")
					}
					continue
				}
				saved, err := other.ReadAnalysisTask(ctx, analysis.IdentityScope(i), v.ID, child.ID, checksum)
				if err != nil || saved.Tool != "semantic.discovery.request" || len(saved.Columns) != 1 || saved.OwnerKey == "" || keys[saved.OwnerKey] {
					t.Fatal(saved, err)
				}
				keys[saved.OwnerKey] = true
			}
			again, err := c.Step(ctx, i, v.ID, task.ID)
			if tc.status == 403 {
				if !errors.Is(err, analysis.ErrScope) {
					t.Fatal("denied scope restarted", err)
				}
			} else if err != nil || again.Attempts != task.Attempts || owner.batches != 1 {
				t.Fatal(again, err)
			}
			if tc.state == "WAITING" {
				if again.NotBefore.Sub(again.FirstAttemptAt) < 2*time.Hour || again.Attempts != 1 {
					t.Fatal("Retry-After or attempts lost", again)
				}
				_, err = s.pool.Exec(ctx, `update ouf_mcp.profile_analysis_task set task=jsonb_set(jsonb_set(task,'{notBefore}',to_jsonb(now()-interval '1 minute')),'{firstAttemptAt}',to_jsonb(now()-interval '25 hours')) where analysis_id=$1 and task_id=$2`, v.ID, task.ID)
				if err != nil {
					t.Fatal(err)
				}
				exhausted, err := c.Step(ctx, i, v.ID, task.ID)
				if err != nil || exhausted.Code != "RETRY_BUDGET_EXHAUSTED" || owner.batches != 1 {
					t.Fatal(exhausted, err)
				}
			}
		})
	}
}

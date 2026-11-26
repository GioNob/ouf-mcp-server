package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
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
	version *int64
	due     time.Time
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
		var body []byte
		if g.status == 429 || g.status == 503 {
			g.due = time.Now().UTC().Add(3 * time.Hour).Truncate(time.Second)
			first, last := g.due.Add(-time.Minute), g.due
			if g.status == 503 {
				first, last = last, first
			}
			body, _ = json.Marshal(map[string]time.Time{"nextAttemptAt": first, "notBefore": last})
		}
		return orchestration.GatewayResponse{Status: g.status, Body: body, Problem: &orchestration.Problem{Status: g.status, Code: g.code, PolicyVersion: g.version, RetryAfter: "7200"}}, nil
	}
	return orchestration.GatewayResponse{Status: 200, Body: []byte(`{"contract":"ouf.semantic.published-batch.v1","results":[]}`)}, nil
}

func TestPublishedDurableFailuresAndCompatibility(t *testing.T) { // MCP-50T-07
	older, current, newer := int64(76), int64(77), int64(78)
	for _, tc := range []struct {
		status              int
		code, state, reason string
		children            int
		version             *int64
	}{
		{429, "SEM_BATCH_RATE_LIMITED", "WAITING", "TRANSIENT_OWNER_WAIT", 0, nil},
		{503, "SEM_BATCH_UNAVAILABLE", "WAITING", "TRANSIENT_OWNER_WAIT", 0, nil},
		{200, "", "HELD", analysis.InvalidOwner, 0, nil},
		{403, "SEM_READ_RECEIPT_OR_POLICY_DENIED", "HELD", "OWNER_AUTHORIZATION_DENIED", 0, nil},
		{403, "NO_APPLICABLE_GRANT", "HELD", "NO_APPLICABLE_GRANT", 0, nil},
		{403, "NO_APPLICABLE_GRANT", "WAITING", "TRANSIENT_OWNER_WAIT", 0, &older},
		{403, "NO_APPLICABLE_GRANT", "HELD", "NO_APPLICABLE_GRANT", 0, &current},
		{403, "NO_APPLICABLE_GRANT", "HELD", "NO_APPLICABLE_GRANT", 0, &newer},
		{403, "STALE_POLICY_BUNDLE", "WAITING", "TRANSIENT_OWNER_WAIT", 0, nil},
		{403, "NO_POLICY_BUNDLE", "WAITING", "TRANSIENT_OWNER_WAIT", 0, nil},
		{403, "SEM_READ_RECEIPT_INVALID", "HELD", "SEM_READ_RECEIPT_INVALID", 0, &older},
		{404, "ROUTE_NOT_FOUND", "SUCCEEDED", "PUBLISHED_BATCH_UNAVAILABLE", 4, nil},
		{413, "SEM_BATCH_REQUEST_TOO_LARGE", "SUCCEEDED", "PUBLISHED_BATCH_SPLIT", 2, nil},
		{422, "SEM_BATCH_RESULT_TOO_LARGE", "SUCCEEDED", "PUBLISHED_BATCH_SPLIT", 2, nil},
	} {
		t.Run(tc.reason+tc.code, func(t *testing.T) {
			ctx, s, c, i, auth, g, p := coordinatorFixture(t, 4)
			auth.version = current
			owner := &publishedFailureOwner{preview: g, status: tc.status, code: tc.code, version: tc.version}
			c.Service.Gateway = owner
			work := analysis.Work{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"intent":"other","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`), Columns: []int{0}}
			v, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, []analysis.Work{work})
			if err != nil || len(v.Tasks) != 1 {
				t.Fatal(v, err)
			}
			task, err := c.Step(ctx, i, v.ID, v.Tasks[0].ID)
			var denied *analysis.OwnerDenial
			stableDenial := tc.status == 403 && tc.state == "HELD"
			if (stableDenial && !errors.As(err, &denied)) || (!stableDenial && err != nil) || task.State != tc.state || task.Code != tc.reason {
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
			if stableDenial {
				if analysis.PermissionHold(tc.reason) {
					_, err := c.Plan(ctx, i, p.AssetID, p.ProfileID, 0, []analysis.Work{work})
					if !errors.Is(err, analysis.ErrPolicyUnchanged) || owner.batches != 1 {
						t.Fatal("replan under unchanged policy", err, owner.batches)
					}
				}
				summary, err := other.ReadSummary(ctx, analysis.IdentityScope(i), v.ID, checksum, analysis.Position{Seq: 1<<63 - 1, Last: -1, Limit: 4}, true)
				if err != nil || summary.Hint.Reason != tc.reason || len(summary.Items) != 4 {
					t.Fatal(summary, err)
				}
				for _, raw := range summary.Items {
					var item analysis.Outcome
					if json.Unmarshal(raw, &item) != nil || item.Reason != tc.reason || !reflect.DeepEqual(item.PolicyVersion, tc.version) {
						t.Fatal(string(raw))
					}
				}
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
			if stableDenial {
				if !errors.Is(err, analysis.ErrScope) {
					t.Fatal("denied scope restarted", err)
				}
			} else if err != nil || again.Attempts != task.Attempts || owner.batches != 1 {
				t.Fatal(again, err)
			}
			if tc.state == "WAITING" {
				if !owner.due.IsZero() && !again.NotBefore.Equal(owner.due) {
					t.Fatal("owner error deadline lost across restart", again.NotBefore, owner.due)
				}
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

package analysisworker

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type dispatchFixture struct {
	statusGateway
	operation string
	result    orchestration.MandateResponse
	cause     string
}

func (g dispatchFixture) DispatchMandate(_ context.Context, raw json.RawMessage, _ int64, _ time.Duration) (analysis.DispatchResponse, error) {
	var command struct{ Operation string }
	if json.Unmarshal(raw, &command) != nil || command.Operation != g.operation {
		g.t.Fatalf("dispatch %q, expected %q", command.Operation, g.operation)
	}
	return analysis.DispatchResponse{MandateResponse: g.result, StaleCause: g.cause}, nil
}

func TestWorkerSupersededDispatch(t *testing.T) { // MCP-58E-01
	ctx, w, r := fixtureWorker(t)
	body := []byte(`{"code":"ANALYSIS_STALE_EXECUTION"}`)
	w.Gateway = dispatchFixture{statusGateway: statusGateway{t: t}, operation: "source.file.preview", cause: "SUPERSEDED",
		result: orchestration.MandateResponse{Status: 409, Body: body, Decision: "fixture", Correlation: "fixture", Effect: "IN_FLIGHT"}}
	if err := w.Once(ctx); err != nil {
		t.Fatal(err)
	}
	var digest, state, mandate string
	err := w.Store.Pool().QueryRow(ctx, `select a.crossing_hash,r.state,r.mandate_state from ouf_mcp.analysis_run_attempt a
		join ouf_mcp.analysis_run r using(analysis_id) where a.analysis_id=$1 and a.worker=$2`, r.ID(), w.ID).Scan(&digest, &state, &mandate)
	if err != nil || digest != analysis.BytesDigest(body) || state != "RUNNING" || mandate != "ACTIVE" {
		t.Fatal("superseded dispatch", state, mandate, err)
	}
}

func TestSemanticDispatchProblems(t *testing.T) {
	for _, id := range []string{"MCP-58E-05", "MCP-58E-07"} {
		t.Run(id, func(t *testing.T) {
			ctx, w, r := fixtureWorker(t)
			dispatch := func(op string, status int, body []byte, retry string) {
				t.Helper()
				w.Gateway = dispatchFixture{statusGateway: statusGateway{t: t}, operation: op,
					result: orchestration.MandateResponse{Status: status, Body: body, Decision: "fixture", Correlation: "fixture", RetryAfter: retry}}
				if err := w.Once(ctx); err != nil {
					t.Fatal(err)
				}
			}
			page, err := json.Marshal(struct {
				analysis.Profile
				View    string           `json:"view"`
				Columns []map[string]any `json:"columns"`
				Offset  int              `json:"offset"`
				HasMore bool             `json:"hasMore"`
			}{r.Profile, "COLUMNS", []map[string]any{{"name": "first", "ordinal": 0}, {"name": "second", "ordinal": 1}}, 0, false})
			if err != nil {
				t.Fatal(err)
			}
			dispatch("source.file.preview", 200, page, "")
			checkpoint := func() analysis.RunCheckpoint {
				t.Helper()
				var cp analysis.RunCheckpoint
				if err := w.Store.Pool().QueryRow(ctx, "select checkpoint from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&cp); err != nil {
					t.Fatal(err)
				}
				return cp
			}
			before := checkpoint()
			var batch analysis.Batch
			if len(before.Queue) != 1 || json.Unmarshal(before.Queue[0].Task.Arguments, &batch) != nil || len(batch.Intents) != 2 {
				t.Fatal("expected one batch with two distinct intents")
			}
			if id == "MCP-58E-07" {
				start := time.Now().Truncate(time.Second)
				dispatch("ouf.semantic.search.batch", 429, []byte(`{"code":"SEM_BATCH_RATE_LIMITED"}`), "90")
				r, err = w.Store.ReadRun(ctx, r.Scope, r.Issuer, r.ID())
				if err != nil || r.State != "RUNNING" || r.Reason != "ANALYSIS_BACKOFF" || r.NotBefore.Before(start.Add(90*time.Second)) || !reflect.DeepEqual(before, checkpoint()) {
					t.Fatal("batch or deadline changed", r.State, r.Reason, r.NotBefore, err)
				}
				return
			}
			var rows []analysis.BatchResult
			for _, intent := range batch.Intents {
				rows = append(rows, analysis.BatchResult{ID: intent.ID, State: "NONE", Hits: []analysis.PublishedMatch{}})
			}
			raw, err := json.Marshal(map[string]any{"contract": analysis.BatchContract, "results": rows})
			if err != nil {
				t.Fatal(err)
			}
			dispatch("ouf.semantic.search.batch", 200, raw, "")
			residual := checkpoint()
			if len(residual.Queue) != 2 {
				t.Fatal("expected two residuals")
			}
			dispatch("semantic.discovery.request", 409, []byte(`{"code":"SEM_DISCOVERY_IDEMPOTENCY_CONFLICT"}`), "")
			after := checkpoint()
			var reason string
			err = w.Store.Pool().QueryRow(ctx, `select reason from ouf_mcp.analysis_group_quarantine
				where analysis_id=$1 and intent_key=$2 and state='OPEN'`, r.ID(), residual.Queue[0].Intent).Scan(&reason)
			if err != nil || reason != "ANALYSIS_EFFECT_UNCERTAIN" || !reflect.DeepEqual(after.Queue, residual.Queue[1:]) {
				t.Fatal("owner conflict was not isolated", reason, err)
			}
		})
	}
}

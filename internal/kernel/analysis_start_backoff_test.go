package kernel

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/google/jsonschema-go/jsonschema"
)

// MCP-55E-02: a start refused by the installation or subject quota repeats the
// same idempotent source.analysis.plan command after notBefore, never OPERATOR.
func TestStartBackoffRepeatsTheSameCommand(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	s, err := newWorkflowServer(nil, snapshot, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range snapshot.ToolEligible() {
		if c.ToolName != "source.analysis.plan" {
			continue
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
			t.Fatal(err)
		}
		resolved, err := schema.Resolve(nil)
		if err != nil {
			t.Fatal(err)
		}
		s.tools[c.ToolName] = registeredTool{capability: c, input: resolved}
	}
	args := nextstep.Object{"mode": "ALL_COLUMNS", "assetId": "11111111-1111-4111-8111-111111111111",
		"profileId": "22222222-2222-4222-8222-222222222222", "commandKey": "33333333-3333-4333-8333-333333333333"}
	if !s.valid("source.analysis.plan", args) {
		t.Fatal("fixture arguments must satisfy the plan schema")
	}
	at := time.Now().Add(7 * time.Second)
	out := s.startBackoff(context.Background(), args, &analysis.RenewalBackoff{Cause: analysis.ErrRunPressure, NotBefore: at})
	if out == nil || !out.IsError {
		t.Fatal("expected a retryable error result")
	}
	body := out.StructuredContent.(nextstep.Object)
	next := body["next"].(nextstep.Object)
	if body["code"] != "ANALYSIS_BACKOFF" || next["action"] != "POLL" || next["tool"] != "source.analysis.plan" ||
		next["reasonCode"] != "ANALYSIS_BACKOFF" || out.Meta["ouf/httpStatus"] != 429 {
		t.Fatal(body)
	}
	if got := next["arguments"].(nextstep.Object); got["commandKey"] != args["commandKey"] || got["assetId"] != args["assetId"] {
		t.Fatal(got)
	}
	if seconds := next["pollAfterSeconds"].(int); seconds < 6 || seconds > 8 {
		t.Fatal(seconds)
	}
	if body["notBefore"] != at.UTC().Format(time.RFC3339Nano) {
		t.Fatal(body["notBefore"])
	}
	// Without notBefore, a key or a short enough wait the start stays an OPERATOR block.
	for _, err := range []error{analysis.ErrRunPressure, &analysis.RenewalBackoff{Cause: analysis.ErrRunPressure, NotBefore: time.Now().Add(2 * time.Hour)}} {
		if s.startBackoff(context.Background(), args, err) != nil {
			t.Fatal("unexpected POLL", err)
		}
	}
	keyless := nextstep.Object{"mode": "ALL_COLUMNS", "assetId": args["assetId"], "profileId": args["profileId"]}
	if s.startBackoff(context.Background(), keyless, &analysis.RenewalBackoff{Cause: analysis.ErrRunPressure, NotBefore: at}) != nil {
		t.Fatal("a command without commandKey must not be repeated")
	}
}

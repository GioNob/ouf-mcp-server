package nextstep

import (
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

func TestRunMachineDeadline(t *testing.T) { // MCP-50U-03
	now := time.Date(2030, 1, 1, 10, 0, 0, 0, time.UTC)
	r := analysis.RunRecord{State: "RUNNING", Reason: "ANALYSIS_BACKOFF", NotBefore: now.Add(time.Minute), Deadline: now.Add(15 * time.Second)}
	for _, tc := range []struct {
		offset                time.Duration
		state, action, reason string
	}{
		{0, "RUNNING", "POLL", "ANALYSIS_BACKOFF"},
		{15 * time.Second, "RUNNING", "BLOCKED", "WAIT_DEADLINE_EXPIRED"},
		{15 * time.Second, "COMPLETE", "CALL_TOOL", "ANALYSIS_SUMMARY"},
		{time.Hour, "PARTIAL", "CALL_TOOL", "ANALYSIS_SUMMARY"},
	} {
		r.State = tc.state
		n := RunNext(r, now.Add(tc.offset), 5*time.Second)
		if n["action"] != tc.action || n["reasonCode"] != tc.reason {
			t.Fatal(n)
		}
		if tc.action == "POLL" && (n["pollAfterSeconds"] != 15 || n["arguments"].(Object)["view"] != "STATUS") {
			t.Fatal(n)
		}
	}
}

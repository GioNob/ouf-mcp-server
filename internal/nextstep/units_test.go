package nextstep

import (
	"fmt"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

func unitRail(t *testing.T) (*manifest.Snapshot, Engine) {
	t.Helper()
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	e := Engine{Errors: m.NextErrors, Valid: func(tool string, args Object) bool {
		for _, c := range m.Capabilities {
			if c.ToolName == tool {
				return resolve(t, c.InputSchema).Validate(asJSON(t, args)) == nil
			}
		}
		return false
	}}
	return m, e
}

func TestUnitInventoryReachability(t *testing.T) { // MCP-62E-01
	m, e := unitRail(t)
	const profile = "11111111-1111-4111-8111-111111111111"
	cap := manifest.Capability{ToolName: "source.file.preview"}
	for _, count := range []int{0, 1, 2, 4, 5, 12, 64} {
		rows := make([]any, count)
		for i := range rows {
			rows[i] = Object{"unitRef": fmt.Sprintf("sheet:%d", i), "status": "READY", "name": "private-name"}
		}
		seen := map[string]bool{}
		input := Object{"view": "UNITS", "profileId": profile}
		for offset := 0; ; {
			next := e.Evaluate(cap, m.NextPolicies["preview"], input, Object{"profileId": profile, "units": rows[offset:], "hasMore": false}, false)
			if count == 0 {
				if next["reasonCode"] != "SOURCE_UNITS_NONE_READY" || next["actor"] != "AGENT" {
					t.Fatal(next)
				}
				break
			}
			alts := []Object{next}
			if next["action"] == "BLOCKED" {
				alts = next["alternatives"].([]Object)
				if next["reasonCode"] != "SOURCE_UNIT_SELECTION_REQUIRED" || next["actor"] != "AGENT" || len(alts) > 4 {
					t.Fatal(next)
				}
			}
			if strings.Contains(fmt.Sprint(next), "private-name") {
				t.Fatal("sheet name in workflow instructions")
			}
			more := false
			for _, alt := range alts {
				args := alt["arguments"].(Object)
				if args["profileId"] != profile || !e.Valid(cap.ToolName, args) {
					t.Fatal(alt)
				}
				if args["view"] == "UNIT" {
					ref := args["unitRef"].(string)
					if seen[ref] {
						t.Fatal("duplicate option", ref)
					}
					seen[ref] = true
				} else {
					n := args["offset"].(int)
					if n <= offset || n >= count {
						t.Fatal("invalid continuation", n)
					}
					input, offset, more = args, n, true
				}
			}
			if !more {
				break
			}
		}
		if len(seen) != count {
			t.Fatalf("offered %d of %d units", len(seen), count)
		}
	}
}

func TestCSVUnitRailCallCount(t *testing.T) { // MCP-62E-02
	m, e := unitRail(t)
	const id = "11111111-1111-4111-8111-111111111111"
	c := manifest.Capability{ToolName: "source.file.preview"}
	count := func(unit bool) int {
		t.Helper()
		job := Object{"workflowState": "SUCCEEDED", "resultRef": id, "assetId": id}
		page := Object{"format": "CSV", "profileId": id, "assetId": id, "columns": []any{Object{"name": "id"}}}
		if unit {
			job["unitSummary"] = Object{"readyUnits": 1, "singleReadyUnitRef": "table:0"}
			page["status"], page["unitRef"], page["profileVersion"], page["schemaHash"] = "READY", "table:0", 1, "sha256:"+strings.Repeat("a", 64)
		}
		input := Object{"assetId": id, "jobId": id}
		for i, obs := range []Object{job, page} {
			next := e.Evaluate(c, m.NextPolicies["preview"], input, obs, false)
			args := next["arguments"].(Object)
			tool := next["tool"].(string)
			if !e.Valid(tool, args) || unit && i == 0 && args["view"] != "UNIT" {
				t.Fatal(next)
			}
			if tool == "source.analysis.plan" {
				return i + 1
			}
			input = args
		}
		t.Fatal("no analysis plan")
		return 0
	}
	if legacy, unit := count(false), count(true); legacy != 2 || unit != legacy {
		t.Fatalf("calls to first plan: historical %d, unit %d", legacy, unit)
	}
}

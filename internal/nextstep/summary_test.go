package nextstep

import (
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/uuid"
)

func TestSummaryNextAtWorkflowBoundaries(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	e := Engine{Valid: func(tool string, args Object) bool {
		c, ok := analysis.Find(m, tool)
		return ok && c.ToolEligible && resolve(t, c.InputSchema).Validate(asJSON(t, args)) == nil
	}}
	id := strings.Repeat("a", 64)
	job := "11111111-1111-4111-8111-111111111111"
	s := analysis.Summary{Counts: analysis.Counts{Total: 1, Ready: 1}, Hint: analysis.Hint{AssetID: job, ProfileID: job, RequestID: job}}
	for _, tc := range []struct {
		view, cursor, action, reason string
		onboard                      bool
	}{
		{"STATUS", "", "CALL_TOOL", "ANALYSIS_SUMMARY", false},
		{"SUMMARY", strings.Repeat("a", 40), "CALL_TOOL", "ANALYSIS_NEXT_PAGE", true},
		{"SUMMARY", "", "DONE", "ANALYSIS_COMPLETE", false},
		{"SUMMARY", "", "BLOCKED", "MAPPING_DECISION_REQUIRED", true},
	} {
		s.Onboarding = tc.onboard
		next := e.SummaryNext(id, tc.view, tc.cursor, s)
		if next["action"] != tc.action || next["reasonCode"] != tc.reason {
			t.Fatal(next)
		}
		if next["action"] == "DONE" && next["outcome"] != "SUCCEEDED" {
			t.Fatal(next)
		}
		if alts, ok := next["alternatives"].([]Object); ok {
			if len(alts) == 0 {
				t.Fatal("missing observed job", next)
			}
			for _, alt := range alts {
				if !e.Valid(alt["tool"].(string), alt["arguments"].(Object)) {
					t.Fatal(alt)
				}
			}
		}
	}
	s.Counts = analysis.Counts{Total: 2, Zero: 1, Blocked: 1}
	if n := e.SummaryNext(id, "SUMMARY", "", s); n["reasonCode"] != "ANALYSIS_PARTIAL" || n["actor"] != "OPERATOR" {
		t.Fatal(n)
	}
	s.Counts = analysis.Counts{Total: 2, Blocked: 1, Pending: 1}
	s.Hint.Tool = "semantic.discovery.status"
	if n := e.SummaryNext(id, "STATUS", "", s); n["tool"] != s.Hint.Tool {
		t.Fatal(n)
	}
	s.Stopped = true
	if n := e.SummaryNext(id, "STATUS", "", s); n["tool"] != "source.analysis.read" {
		t.Fatal(n)
	}
}

func TestPolicyDenialSummary(t *testing.T) {
	for _, tc := range []struct{ code, actor string }{
		{"NO_APPLICABLE_GRANT", "HUMAN_THS"},
		{analysis.ErrEpochChanged.Error(), "HUMAN_THS"},
		{"OWNER_AUTHORIZATION_DENIED", "HUMAN_THS"},
		{"SEM_READ_RECEIPT_INVALID", "OPERATOR"},
		{"SEM_DISCOVERY_RECEIPT_INVALID", "OPERATOR"},
	} {
		s := analysis.Summary{Counts: analysis.Counts{Total: 4, Blocked: 4}, Hint: analysis.Hint{Reason: tc.code}}
		next := (Engine{}).SummaryNext(strings.Repeat("a", 64), "SUMMARY", "", s)
		if next["reasonCode"] != tc.code || next["actor"] != tc.actor || next["action"] != "BLOCKED" {
			t.Fatal(next)
		}
		alts := next["alternatives"].([]Object)
		if tc.actor == "HUMAN_THS" {
			if len(alts) != 3 || alts[0]["action"] != "WAIT_FOR_HUMAN" || alts[1]["arguments"].(Object)["view"] != "ROLES" || alts[2]["tool"] != "source.analysis.plan" {
				t.Fatal(alts)
			}
			args := alts[2]["arguments"].(Object)
			key, err := uuid.Parse(args["commandKey"].(string))
			if args["replanOf"] != strings.Repeat("a", 64) || len(args) != 2 || err != nil || key.Version() != 4 {
				t.Fatal(args)
			}
		}
		if tc.actor == "OPERATOR" && (len(alts) != 1 || alts[0]["tool"] != nil) {
			t.Fatal(alts)
		}
	}
}

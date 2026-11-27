package nextstep

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

func TestDiscoveryPlanDescribesOnlyDisclosedValues(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	c, _ := analysis.Find(m, "source.file.preview")
	plan, _ := analysis.Find(m, "source.analysis.plan")
	schema := resolve(t, plan.InputSchema)
	e := Engine{Valid: func(name string, args Object) bool {
		return name == plan.ToolName && schema.Validate(asJSON(t, args)) == nil
	}}
	const id = "10000000-0000-4000-8000-000000000001"
	owner := Object{"assetId": id, "profileId": id, "format": "CSV", "columns": []any{
		Object{"name": "fonte_rioneCircoscrizione", "type": "STRING", "distinctCount": 9, "dataAccessLabel": "OPEN",
			"distinctValues": []any{"A", "A", "[REDACTED]", Object{"redacted": true, "value": "occulto"}, "B", "C", "D", "E", "F"}},
		Object{"name": "codiceISTATComune", "distinctValues": []any{"occulto"}, "dataAccessLabel": "RESTRICTED"},
		Object{"name": "codice_strada_comunale", "redacted": true, "distinctValues": []any{"occulto"}},
	}}
	next := e.Evaluate(c, m.NextPolicies[c.NextPolicy], Object{}, owner, false)
	raw, _ := json.Marshal(next)
	if strings.Contains(string(raw), "occulto") || strings.Contains(string(raw), "[REDACTED]") ||
		next["tool"] != plan.ToolName || !strings.Contains(next["userMessage"].(string), "in italiano") {
		t.Fatal(next)
	}
	work := next["arguments"].(Object)["work"].([]Object)
	for i, want := range []string{"fonte rione circoscrizione", "codice ISTAT comune", "codice strada comunale"} {
		if got := work[i]["arguments"].(Object)["intent"]; got != want {
			t.Fatal(i, got)
		}
	}
	want := Object{"originalName": "fonte_rioneCircoscrizione", "dataType": "STRING", "distinctCount": 9, "distinctValues": []any{"A", "B", "C", "D", "E"}}
	if !reflect.DeepEqual(work[0]["intentHint"], want) || work[1]["intentHint"].(Object)["distinctValues"] != nil || work[2]["intentHint"].(Object)["distinctValues"] != nil {
		t.Fatal(work)
	}
	args := work[0]["arguments"].(Object)
	args["intent"] = "Fonte che associa il rione alla circoscrizione amministrativa"
	if !e.Valid(plan.ToolName, next["arguments"].(Object)) {
		t.Fatal("intent authored by the agent rejected")
	}
	if got := intentHint(Object{"redacted": true}, owner["columns"].([]any)[0].(Object), "fonte"); got["distinctValues"] != nil {
		t.Fatal(got)
	}
}

func TestEmptyCandidatesOffersColumnDecisions(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	c, _ := analysis.Find(m, "semantic.discovery.candidates")
	id, request := strings.Repeat("a", 64), "11111111-1111-4111-8111-111111111111"
	input := Object{"analysisId": id, "requestId": request}
	for _, tc := range []struct {
		used    int
		propose bool
		want    []string
	}{
		{0, true, []string{"source.analysis.plan", "semantic.propose", onboardingCreate}},
		{1, false, []string{"source.analysis.plan", onboardingCreate}},
		{2, true, []string{"semantic.propose", onboardingCreate}},
		{2, false, []string{onboardingCreate}},
	} {
		e := Engine{CanPropose: func() bool { return tc.propose }, ReformulationColumn: func(a, r string) (int, bool) {
			return 4, a == id && r == request && tc.used < 2
		}}
		status, _ := analysis.Find(m, "semantic.discovery.status")
		e.Valid = func(string, Object) bool { return true }
		list := e.Evaluate(status, m.NextPolicies[status.NextPolicy], input, Object{"state": "SUCCEEDED", "candidateCount": 0}, false)
		if list["action"] != "CALL_TOOL" || list["tool"] != c.ToolName {
			t.Fatal(list)
		}
		n := e.Evaluate(c, m.NextPolicies[c.NextPolicy], list["arguments"].(Object), Object{"state": "SUCCEEDED", "candidates": []any{}}, false)
		if n["actor"] != "AGENT" || n["action"] != "BLOCKED" {
			t.Fatal(n)
		}
		var names []string
		for _, alt := range n["alternatives"].([]Object) {
			name := alt["tool"].(string)
			names = append(names, name)
			target, _ := analysis.Find(m, name)
			schema, err := DecisionSchema(target.InputSchema)
			args := alt["arguments"].(Object)
			if err != nil || schema.Validate(asJSON(t, args)) != nil {
				t.Fatal(alt, err)
			}
			if name == "source.analysis.plan" {
				if args["replanOf"] != id || args["requestId"] != request || args["column"] != 4 || args["intent"] != nil {
					t.Fatal(args)
				}
				args["intent"] = "Codice comunale che identifica la strada"
				if err := resolve(t, target.InputSchema).Validate(asJSON(t, args)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !reflect.DeepEqual(names, tc.want) {
			t.Fatal(names, tc.want)
		}
	}
}

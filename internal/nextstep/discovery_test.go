package nextstep

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/contracts"
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
	limits, err := analysis.SummaryLimits(m)
	if err != nil || limits.MaxColumnReformulations != 7 {
		t.Fatal(limits, err)
	}
	limit := limits.MaxColumnReformulations
	id, request := strings.Repeat("a", 64), "11111111-1111-4111-8111-111111111111"
	input := Object{"analysisId": id, "requestId": request}
	for _, tc := range []struct {
		used    int
		propose bool
		want    []string
	}{
		{0, true, []string{"source.analysis.plan", "semantic.propose", onboardingCreate}},
		{limit - 1, false, []string{"source.analysis.plan", onboardingCreate}},
		{limit, true, []string{"semantic.propose", onboardingCreate}},
		{limit, false, []string{onboardingCreate}},
	} {
		e := Engine{CanPropose: func() bool { return tc.propose }, ReformulationColumn: func(a, r string) (int, bool) {
			return 4, a == id && r == request && tc.used < limit
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
				if alt["intentHint"] != reformulationIntentHint || !strings.Contains(n["userMessage"].(string), "1–3 parole") {
					t.Fatal(n)
				}
				args["intent"] = "Codice strada comunale"
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

func TestReformulationRefusalsGiveAgentDecisions(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	contract, err := contracts.NextStepSchema()
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := analysis.Find(m, "source.analysis.plan")
	id, request := strings.Repeat("a", 64), "11111111-1111-4111-8111-111111111111"
	input := Object{"replanOf": id, "requestId": request, "column": 4, "commandKey": request, "intent": "Codice comunale della strada"}
	e := Engine{CanPropose: func() bool { return true }, DecisionKeys: func(a, r string) (string, string, error) {
		if a != id || r != request {
			t.Fatal(a, r)
		}
		return request, "22222222-2222-4222-8222-222222222222", nil
	}}
	for _, tc := range []struct {
		reason string
		want   []string
	}{
		{"REFORMULATION_TERM_TOO_LONG", []string{plan.ToolName}},
		{"REFORMULATION_BUDGET_EXHAUSTED", []string{"semantic.propose", onboardingCreate}},
	} {
		next := e.Evaluate(plan, m.NextPolicies[plan.NextPolicy], input, Object{"code": tc.reason}, true)
		if err := contract.Validate(asJSON(t, next)); err != nil || next["action"] != "BLOCKED" || next["actor"] != "AGENT" || next["reasonCode"] != tc.reason {
			t.Fatal(next, err)
		}
		var names []string
		for _, alt := range next["alternatives"].([]Object) {
			tool := alt["tool"].(string)
			names = append(names, tool)
			args := alt["arguments"].(Object)
			target, _ := analysis.Find(m, tool)
			schema, err := DecisionSchema(target.InputSchema)
			if err != nil || schema.Validate(asJSON(t, args)) != nil {
				t.Fatal(alt, err)
			}
			if tool == plan.ToolName {
				if !strings.Contains(next["userMessage"].(string), "1–3 parole") || alt["intentHint"] != reformulationIntentHint ||
					args["replanOf"] != id || args["requestId"] != request || args["column"] != 4 || args["commandKey"] == request || args["intent"] != nil {
					t.Fatal(next)
				}
			} else if args["analysisId"] != id {
				t.Fatal(alt)
			}
		}
		if !reflect.DeepEqual(names, tc.want) {
			t.Fatal(names, tc.want)
		}
	}

	t.Run("ordinary plan at the ninth attempt", func(t *testing.T) {
		policy := m.NextPolicies[plan.NextPolicy]
		code := analysis.ErrReformulationExhausted.Error()
		explicit := e.Evaluate(plan, policy, input, Object{"code": code}, true)
		ordinary := Object{"assetId": request, "profileId": request, "expectedRevision": 8}
		obs := Object{"code": code, "analysisId": id, "requestId": request}
		if next := e.Evaluate(plan, policy, ordinary, obs, true); !reflect.DeepEqual(next, explicit) {
			t.Fatal(next, explicit)
		}

		delete(obs, "requestId")
		next := e.Evaluate(plan, policy, ordinary, obs, true)
		if err := contract.Validate(asJSON(t, next)); err != nil || next["action"] != "BLOCKED" ||
			next["actor"] != "AGENT" || next["reasonCode"] != code {
			t.Fatal(next, err)
		}
		alternatives, ok := next["alternatives"].([]Object)
		if !ok || len(alternatives) != 2 || alternatives[0]["tool"] != "semantic.propose" || alternatives[1]["tool"] != onboardingCreate {
			t.Fatal(next)
		}
		for _, alt := range alternatives {
			if alt["arguments"].(Object)["analysisId"] != id {
				t.Fatal(alt)
			}
		}
	})
}

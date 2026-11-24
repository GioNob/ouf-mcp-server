package nextstep

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestRuleNumericEquality(t *testing.T) {
	for _, tc := range []struct {
		value   any
		literal string
		match   bool
	}{
		{json.Number("0e4"), "0", true}, {int(3), "1|3", true}, {float64(1.25), "1.250", true},
		{json.Number("9007199254740993"), "9007199254740992", false}, {nil, "0", false}, {false, "0", false},
	} {
		if got := matches(map[string]string{"amount": tc.literal}, Object{"amount": tc.value}, nil); got != tc.match {
			t.Errorf("%v matched %q: %v", tc.value, tc.literal, got)
		}
	}
}

func TestDecisionsOutsideAnalysisAndResumableTasks(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	caps := map[string]manifest.Capability{}
	for _, c := range m.ToolEligible() {
		caps[c.ToolName] = c
	}
	e := Engine{Now: time.Now, Valid: func(name string, args Object) bool {
		return resolve(t, caps[name].InputSchema).Validate(asJSON(t, args)) == nil
	}}
	const id = "11111111-1111-4111-8111-111111111111"
	aid, taskID := strings.Repeat("a", 64), strings.Repeat("b", 64)
	input := Object{"requestId": id}
	owner := Object{"state": "SUCCEEDED", "candidateCount": json.Number("0"), "allowedActions": []any{"NONE"}}
	status := caps["semantic.discovery.status"]
	for _, known := range []bool{false, true} {
		if known {
			owner["intent"] = "nome"
		}
		next := e.Evaluate(status, m.NextPolicies[status.NextPolicy], input, owner, false)
		alts := next["alternatives"].([]Object)
		if next["actor"] != "AGENT" || len(alts) != 2 || alts[0]["tool"] != "semantic.search" || alts[1]["tool"] != "semantic.discovery.request" {
			t.Fatal(next)
		}
		for i, alt := range alts {
			args := Object{}
			bound, _ := alt["arguments"].(Object)
			for k, v := range bound {
				args[k] = v
			}
			field := "q"
			if i == 1 {
				field = "intent"
				if uuid.Validate(args["idempotencyKey"].(string)) != nil {
					t.Fatal("missing discovery key")
				}
				args["preferredLanguages"], args["requestedArtifactType"] = []string{"it"}, "PROPERTY"
			}
			if known && args[field] != owner["intent"] || !known && args[field] != nil {
				t.Fatal("wrong intent binding", args)
			}
			if !known {
				args[field] = "nome"
			}
			if !e.Valid(alt["tool"].(string), args) {
				t.Fatal("inexecutable alternative", alt)
			}
		}
	}
	read := caps["source.analysis.read"]
	for _, state := range []string{"WAITING", "HELD", "FAILED", "UNKNOWN"} {
		task := Object{"taskId": taskID, "state": state}
		obs := Object{"analysisId": aid, "tasks": []any{task}}
		for _, retryable := range []bool{false, true} {
			task["retryable"] = retryable
			next := e.Evaluate(read, m.NextPolicies[read.NextPolicy], Object{"analysisId": aid}, obs, false)
			if state == "UNKNOWN" || (!retryable && state != "WAITING") {
				if next["actor"] != "OPERATOR" {
					t.Fatal("unproven recovery", next)
				}
				continue
			}
			alt := next["alternatives"].([]Object)[0]
			args := alt["arguments"].(Object)
			if next["actor"] != "AGENT" || alt["tool"] != "source.analysis.step" || args["taskId"] != taskID || args["analysisId"] != aid || !e.Valid("source.analysis.step", args) {
				t.Fatal("resume lost task identity", next)
			}
		}
	}
}

func TestObedientPollsFitAdmissionWindows(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	args := Object{"requestId": id}
	owner := Object{"state": "RUNNING"}
	c := manifest.Capability{ToolName: "semantic.discovery.status"}
	for offset := time.Duration(0); offset < orchestration.AdmissionWindow; offset += time.Second {
		now := time.Unix(0, 0).Add(offset)
		e := Engine{Now: func() time.Time { return now }, Valid: func(name string, a Object) bool { return name == c.ToolName && reflect.DeepEqual(a, args) }}
		calls := map[time.Time]int{}
		for range 30 {
			next := e.Evaluate(c, m.NextPolicies["discovery"], args, owner, false)
			if next["action"] != "POLL" || next["pollAfterSeconds"] != 5 {
				t.Fatal(next)
			}
			now = now.Add(time.Duration(next["pollAfterSeconds"].(int)) * time.Second)
			window := now.Truncate(orchestration.AdmissionWindow)
			calls[window]++
			if calls[window]+orchestration.AdmissionRetryThreshold > orchestration.DefaultAdmissionCallsPerMinute {
				t.Fatal("cadence exhausted the ordinary window", offset, calls)
			}
		}
	}
	for _, min := range []int{1, 121} {
		delay, ok := retryDelay(Object{"pollAfterSeconds": min}, time.Now())
		if !ok || delay != max(min, orchestration.PollIntervalSeconds) {
			t.Fatal("ignored owner or admission minimum", min, delay)
		}
	}
}

func TestWindowRefusalPreservesCommand(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	e := Engine{Errors: m.NextErrors, Now: time.Now, Valid: func(string, Object) bool { return true }}
	c := manifest.Capability{ToolName: "semantic.review.prepare", OperationClass: "COMMAND"}
	args := Object{"revisionId": uuid.NewString(), "commandKey": uuid.NewString(), "expectedRowVersion": json.Number("123")}
	for _, code := range []string{"TOOL_SELECTION_STALL", "ADMISSION_WINDOW_BUDGET_EXHAUSTED", "SEMANTIC_READ_WINDOW_BUDGET_EXHAUSTED"} {
		for _, seconds := range []int{1, 23, 60} {
			refusal := Object{"code": code, "retryAfterSeconds": json.Number(fmt.Sprint(seconds)), "windowEndsAt": "2026-11-23T12:16:00Z", "retryable": true}
			next := e.Evaluate(c, m.NextPolicies["validation"], args, refusal, true)
			if next["action"] != "POLL" || next["tool"] != c.ToolName || !reflect.DeepEqual(args, next["arguments"]) || next["pollAfterSeconds"] != seconds || next["deadline"] != refusal["windowEndsAt"] {
				t.Fatal(next)
			}
			delete(refusal, "windowEndsAt")
			if e.Evaluate(c, m.NextPolicies["validation"], args, refusal, true)["actor"] != "OPERATOR" {
				t.Fatal("missing deadline permitted retry", code)
			}
			refusal["windowEndsAt"], refusal["retryable"] = "2026-11-23T12:16:00Z", false
			if e.Evaluate(c, m.NextPolicies["validation"], args, refusal, true)["actor"] != "OPERATOR" {
				t.Fatal("permanent refusal permitted retry", code)
			}
		}
	}
}

func TestMachineWaitUsesAbsoluteOwnerClock(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../contracts/ouf-next-step-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	schema := resolve(t, raw)
	start := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	now := start
	c := manifest.Capability{ToolName: "semantic.discovery.status"}
	args := Object{"requestId": "11111111-1111-4111-8111-111111111111"}
	owner := Object{"state": "RUNNING", "pollAfterSeconds": 17}
	e := Engine{Now: func() time.Time { return now }, Valid: func(string, Object) bool { return true }}
	check := func() Object {
		t.Helper()
		next := e.Evaluate(c, m.NextPolicies["discovery"], args, owner, false)
		if err := schema.Validate(asJSON(t, next)); err != nil {
			t.Fatal(err)
		}
		return next
	}
	if next := check(); next["deadline"] != nil || next["pollAfterSeconds"] != 17 {
		t.Fatal(next)
	}
	owner["deadline"] = start.Add(2 * time.Hour).Format(time.RFC3339)
	if next := check(); next["deadline"] != owner["deadline"] {
		t.Fatal(next)
	}
	owner["createdAt"] = start.Format(time.RFC3339)
	hour := start.Add(time.Hour)
	now = hour.Add(-time.Second)
	if next := check(); next["deadline"] != hour.Format(time.RFC3339) || next["action"] != "POLL" {
		t.Fatal(next)
	}
	now = hour
	if next := check(); next["reasonCode"] != "WAIT_DEADLINE_EXPIRED" || next["actor"] != "OPERATOR" {
		t.Fatal(next)
	}
	now = start
	delete(owner, "createdAt")
	owner["startedAt"] = start.Format(time.RFC3339)
	e.WaitMaxSeconds = 120
	if next := check(); next["deadline"] != start.Add(2*time.Minute).Format(time.RFC3339) {
		t.Fatal(next)
	}
	owner["waitUntil"] = start.Add(time.Minute).Format(time.RFC3339)
	if next := check(); next["deadline"] != owner["waitUntil"] {
		t.Fatal(next)
	}
}

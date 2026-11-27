package nextstep

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestDiscoveryReceiptRelativeWaitStartsAtTaskCompletion(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	c, _ := analysis.Find(m, "source.analysis.step")
	start := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	now := start.Add(12 * time.Second)
	e := signedEngine(t, Engine{Now: func() time.Time { return now }, Valid: func(string, Object) bool { return true }})
	input := Object{"analysisId": strings.Repeat("a", 64)}
	receipt := Object{"requestId": "22222222-2222-4222-8222-222222222222", "pollAfterSeconds": 72}
	owner := Object{"state": "SUCCEEDED", "completedAt": start.Format(time.RFC3339), "result": receipt}
	check := func() Object { return e.Evaluate(c, m.NextPolicies[c.NextPolicy], input, owner, false) }
	ref := pollTarget(t, e, check())
	if ref.NotBefore != start.Add(72*time.Second).Format(time.RFC3339) || ref.Read != "semantic.discovery.status" ||
		ref.Args["requestId"] != receipt["requestId"] || ref.Args["analysisId"] != input["analysisId"] {
		t.Fatal(ref)
	}
	now = start.Add(72 * time.Second)
	if next := check(); next["action"] != "CALL_TOOL" || next["tool"] != ref.Read {
		t.Fatal("rereading a completed task restarted its delay", next)
	}
}

func TestDynamicOwnerContinuationsHonorEachField(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	e := signedEngine(t, Engine{Now: func() time.Time { return now }, Valid: func(string, Object) bool { return true }})
	const read = "semantic.proposal.read"
	args := Object{"commandKey": "fixture"}
	ref := WaitRef{Read: read, Args: args}
	e.Errors = []manifest.Rule{{Action: "POLL", Tool: "$self", Terminal: []string{"SUCCEEDED", "FAILED"}}}
	cap := manifest.Capability{ToolName: read, OperationClass: "READ"}
	policy := manifest.Policy{Read: read}
	for _, tc := range []struct {
		name  string
		build func(Object) Object
	}{
		{"refresh", func(obs Object) Object { return ref.Refresh(e, obs) }},
		{"replay", func(obs Object) Object {
			obs["replay"], obs["readTool"], obs["readArguments"] = true, read, args
			return e.Evaluate(manifest.Capability{ToolName: "semantic.approval.request"}, policy, args, obs, false)
		}},
		{"transient read", func(obs Object) Object {
			obs["Status"] = 503
			return e.Evaluate(cap, policy, args, obs, true)
		}},
		{"transient review link", func(obs Object) Object {
			return e.Signer.Issue(e.Binding, manifest.Rule{Tool: read}, args, obs, LinkResult{Status: LinkTransient}, now)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, field := range []string{"nextAttemptAt", "notBefore", "next_retry_at", "retryAt", "pollAfterSeconds", "retryAfterSeconds", "Retry-After", "RetryAfter", "recoveryState"} {
				for _, nested := range []bool{false, true} {
					due := now.Add(72 * time.Second)
					var value any = due.Format(time.RFC3339Nano)
					switch field {
					case "pollAfterSeconds", "retryAfterSeconds", "Retry-After", "RetryAfter":
						value = 72
					case "recoveryState":
						value = "RETRY_WAIT"
						due = now.Add(5 * time.Second)
					}
					obs := Object{field: value}
					if nested {
						obs = Object{"result": obs}
					}
					got := pollTarget(t, e, tc.build(obs))
					if got.Read != read || got.NotBefore != due.Format(time.RFC3339Nano) {
						t.Fatalf("%s nested=%t: %v", field, nested, got)
					}
				}
			}
		})
	}
}

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
	for _, propose := range []bool{false, true} {
		e.CanPropose = func() bool { return propose }
		page := e.Evaluate(status, m.NextPolicies[status.NextPolicy], input, owner, false)
		if page["action"] != "CALL_TOOL" || page["tool"] != "semantic.discovery.candidates" ||
			!reflect.DeepEqual(page["arguments"], Object{"requestId": id, "includeState": true}) {
			t.Fatal(page)
		}
		c := caps["semantic.discovery.candidates"]
		next := e.Evaluate(c, m.NextPolicies[c.NextPolicy], page["arguments"].(Object), Object{"state": "SUCCEEDED", "candidates": []any{}}, false)
		want := []string{"semantic.discovery.request"}
		if propose {
			want = append(want, "semantic.propose")
		}
		alts, ok := next["alternatives"].([]Object)
		if !ok || next["actor"] != "AGENT" || len(alts) != len(want) {
			t.Fatal(next)
		}
		for i, alt := range alts {
			args := alt["arguments"].(Object)
			key := "idempotencyKey"
			if i == 1 {
				key = "commandKey"
			}
			if alt["tool"] != want[i] || len(args) != 1 || uuid.Validate(fmt.Sprint(args[key])) != nil {
				t.Fatal(alt)
			}
		}
	}
	// A live zero still leads to the candidate page and its column decision
	// when the owner has supplied no historical completion count.
	next := e.Evaluate(status, m.NextPolicies[status.NextPolicy], Object{"requestId": id, "analysisId": aid}, owner, false)
	if next["action"] != "CALL_TOOL" || next["tool"] != "semantic.discovery.candidates" ||
		!reflect.DeepEqual(next["arguments"], Object{"requestId": id, "analysisId": aid, "includeState": true}) {
		t.Fatal(next)
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

func TestReadTimedWaitingTaskResumesAtSavedDeadline(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	read, _ := analysis.Find(m, "source.analysis.read")
	now := time.Unix(1800000000, 0).UTC()
	due := now.Add(72 * time.Second)
	id, task := strings.Repeat("a", 64), strings.Repeat("b", 64)
	key := uuid.NewString()
	e := signedEngine(t, Engine{Now: func() time.Time { return now }, Valid: func(string, Object) bool { return true },
		CommandKey: func(string, Object, Object, Object) string { return key }})
	owner := Object{"analysisId": id, "tasks": []any{
		Object{"state": "WAITING", "notBefore": due.Add(time.Minute).Format(time.RFC3339Nano)},
		Object{"state": "WAITING", "taskId": task, "notBefore": due.Format(time.RFC3339Nano)},
	}}
	readAgain := func() Object {
		return e.Evaluate(read, m.NextPolicies[read.NextPolicy], Object{"analysisId": id}, owner, false)
	}
	now = now.Add(41 * time.Second)
	ref := pollTarget(t, e, readAgain())
	if ref.NotBefore != due.Format(time.RFC3339Nano) || ref.Read != "source.analysis.step" ||
		!reflect.DeepEqual(ref.Args, Object{"analysisId": id, "taskId": task, "commandKey": key}) {
		t.Fatal(ref)
	}
	now = due
	if next := readAgain(); !CheckedOwnerCall(next) || next["tool"] != ref.Read || !reflect.DeepEqual(next["arguments"], ref.Args) {
		t.Fatal(next)
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
		e = signedEngine(t, e)
		calls := map[time.Time]int{}
		for range 30 {
			next := e.Evaluate(c, m.NextPolicies["discovery"], args, owner, false)
			if pollTarget(t, e, next).Read != c.ToolName || next["pollAfterSeconds"] != 5 {
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
		now := time.Now()
		due, ok := orchestration.OwnerDue(Object{"pollAfterSeconds": min}, now, now.Add(time.Duration(orchestration.PollIntervalSeconds)*time.Second), "")
		delay := int(due.Sub(now) / time.Second)
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
	now := time.Now().Truncate(time.Second)
	e := signedEngine(t, Engine{Errors: m.NextErrors, Now: func() time.Time { return now }, Valid: func(string, Object) bool { return true }})
	c := manifest.Capability{ToolName: "semantic.review.prepare", OperationClass: "COMMAND"}
	args := Object{"revisionId": uuid.NewString(), "commandKey": uuid.NewString(), "expectedRowVersion": json.Number("123")}
	for _, code := range []string{"TOOL_SELECTION_STALL", "ADMISSION_WINDOW_BUDGET_EXHAUSTED", "SEMANTIC_READ_WINDOW_BUDGET_EXHAUSTED"} {
		for _, seconds := range []int{1, 23, 60} {
			refusal := Object{"code": code, "retryAfterSeconds": json.Number(fmt.Sprint(seconds)), "windowEndsAt": now.Add(time.Duration(seconds) * time.Second).UTC().Format(time.RFC3339), "retryable": true}
			next := e.Evaluate(c, m.NextPolicies["validation"], args, refusal, true)
			ref := pollTarget(t, e, next)
			if ref.Read != c.ToolName || !reflect.DeepEqual(args, ref.Args) || next["pollAfterSeconds"] != seconds || ref.NotBefore != refusal["windowEndsAt"] {
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

func TestLargePollRetainsOnlyAuthenticatedArguments(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	e := signedEngine(t, Engine{})
	args := Object{"commandKey": uuid.NewString(), "text": strings.Repeat("a", 4096), "version": json.Number("9007199254740993")}
	poll := call("POLL", "OWNER_REQUEST_FAILED", "semantic.propose", args)
	poll["terminalStates"], poll["pollAfterSeconds"] = []string{"SUCCEEDED", "FAILED"}, 5
	next := e.Signer.issuePoll(e.Binding, poll, Object{}, now, now.Add(5*time.Second))
	ref := pollTarget(t, e, next)
	if ref.ArgsHash == "" || !reflect.DeepEqual(ref.Args, args) || ref.RestoreArgs(nil) {
		t.Fatal("large retry input was not bound to its reference")
	}
	if ref.RestoreArgs(Object{"commandKey": args["commandKey"], "text": args["text"], "version": json.Number("9007199254740992")}) {
		t.Fatal("modified command accepted")
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
	e = signedEngine(t, e)
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

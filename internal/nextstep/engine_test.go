package nextstep

import (
	"encoding/json"
	"maps"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

func resolve(t *testing.T, raw []byte) *jsonschema.Resolved {
	t.Helper()
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	s, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func asJSON(t *testing.T, value any) Object {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var obj Object
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

// Check the serialized contract independently of the producer, including
// hints that omit arguments and might otherwise repeat the current request.
func assertProgress(t *testing.T, tool string, input, next Object) {
	t.Helper()
	check := func(target Object) {
		t.Helper()
		if target["tool"] != tool {
			return
		}
		if args, supplied := target["arguments"]; supplied && !reflect.DeepEqual(args, input) {
			return
		}
		delay, _ := target["pollAfterSeconds"].(float64)
		states, _ := target["terminalStates"].([]any)
		if target["action"] != "POLL" || delay <= 0 || len(states) == 0 {
			t.Fatalf("%s %v repeated by %v", tool, input, target)
		}
	}
	check(next)
	if alts, ok := next["alternatives"].([]any); ok {
		for _, alt := range alts {
			check(alt.(Object))
		}
	}
}

func signedEngine(t *testing.T, engine Engine) Engine {
	t.Helper()
	var err error
	engine.Signer, err = NewSigner(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	engine.Binding = []byte("fixture/reader")
	return engine
}

func pollTarget(t *testing.T, e Engine, next Object) WaitRef {
	t.Helper()
	args, _ := next["arguments"].(Object)
	token, _ := args["waitRef"].(string)
	ref, err := e.Signer.Open(token, e.Binding)
	resume, _ := args["resumeArguments"].(Object)
	if next["action"] != "POLL" || next["tool"] != "ouf.wait" || err != nil || !ref.RestoreArgs(resume) {
		t.Fatal("invalid opaque poll", next, err)
	}
	return ref
}

func TestSameToolNumericArguments(t *testing.T) {
	input := Object{"offset": json.Number("1e0")}
	next := call("CALL_TOOL", "COLUMN_PAGE", "source.file.preview", Object{"offset": 1})
	if ensureProgress("source.file.preview", input, next)["action"] != "BLOCKED" {
		t.Fatal("equivalent page offset allowed a self call")
	}
	if wireArgumentsEqual(Object{"version": json.Number("9007199254740992")}, Object{"version": json.Number("9007199254740993")}) {
		t.Fatal("distinct versions were merged")
	}
}

func TestInactiveSchedulePreservesRunVersion(t *testing.T) {
	run := Object{"expectedVersion": 1, "runState": "RUNNING", "recoveryAction": "NONE"}
	want := Version(run)
	for _, schedule := range []string{"NONE", "TERMINAL"} {
		row := maps.Clone(run)
		row["scheduleRecoveryAction"] = schedule
		row["scheduleDecisionDeadline"] = "2099-01-01T00:00:00Z"
		if got := Version(row); got != want {
			t.Errorf("schedule %s changed run version from %q to %q", schedule, want, got)
		}
	}
}

func TestOwnerTransitionsAndTargetArguments(t *testing.T) {
	s, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	caps := map[string]manifest.Capability{}
	inputs := map[string]*jsonschema.Resolved{}
	for _, c := range s.ToolEligible() {
		caps[c.ToolName], inputs[c.ToolName] = c, resolve(t, c.InputSchema)
	}
	raw, err := os.ReadFile("../../contracts/ouf-next-step-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	contract := resolve(t, raw)
	key := make([]byte, 32)
	signer, err := NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	binding := []byte("tenant/principal")
	now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	var onboardingLink LinkResult
	e := Engine{
		Signer: signer, Binding: binding,
		Errors: s.NextErrors,
		Valid: func(name string, args Object) bool {
			return inputs[name] != nil && inputs[name].Validate(asJSON(t, args)) == nil
		},
		Issue: func(r manifest.Rule, args, obs Object) Object {
			return signer.Issue(binding, r, args, obs, onboardingLink, now)
		},
		Now: func() time.Time { return now },
		AnalysisID: func(asset, profile string) string {
			return analysis.AnalysisID(analysis.Scope{Service: "service", Principal: "reader", Tenant: "tenant"}, analysis.Profile{AssetID: asset, ProfileID: profile})
		},
	}
	var cases []struct {
		Name, Tool, Action, Target, Reason, Decision string
		Input, Owner, Arguments, Link, Alternative   Object
		Actor, Outcome, UserMessage                  string
		DeniedCapability, RetryAfter                 string
		Failed, NewCommand                           bool
		Delay                                        int
		TerminalStates                               []any
	}
	raw, err = os.ReadFile("testdata/transitions.json")
	if err != nil || json.Unmarshal(raw, &cases) != nil || len(cases) == 0 {
		t.Fatal("transition fixtures unavailable")
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			link, _ := tc.Link["reviewUrl"].(string)
			end, _ := tc.Link["expiresAt"].(string)
			onboardingLink = OwnerLink(link, end, now)
			if tc.DeniedCapability != "" {
				onboardingLink = LinkResult{Status: LinkDenied, Capability: tc.DeniedCapability, Reason: "NO_APPLICABLE_GRANT"}
			} else if tc.RetryAfter != "" {
				onboardingLink = LinkResult{Status: LinkTransient, RetryAfter: tc.RetryAfter}
			}
			c := caps[tc.Tool]
			result := asJSON(t, e.Evaluate(c, s.NextPolicies[c.NextPolicy], tc.Input, tc.Owner, tc.Failed))
			assertProgress(t, tc.Tool, tc.Input, result)
			if err := contract.Validate(result); err != nil {
				t.Fatal(err)
			}
			if result["action"] != tc.Action || (tc.Reason != "" && tc.Reason != result["reasonCode"]) {
				t.Fatalf("unexpected transition %v", result)
			}
			if tc.Reason == "HUMAN_ABORT_REQUIRED" {
				alts := result["alternatives"].([]any)
				human := alts[0].(Object)
				if len(alts) != 1 || result["actor"] != "HUMAN_THS" || human["action"] != "WAIT_FOR_HUMAN" ||
					human["human"].(Object)["reviewUrl"] != tc.Link["reviewUrl"] {
					t.Fatal("missing abort-only human alternative", result)
				}
			}
			if tc.DeniedCapability != "" {
				alts := result["alternatives"].([]any)
				if len(alts) != 2 || !strings.Contains(result["userMessage"].(string), tc.DeniedCapability) {
					t.Fatal("incomplete permission diagnosis", result)
				}
				for i, view := range []string{"CAPABILITIES", "ROLES"} {
					alt := alts[i].(Object)
					args := alt["arguments"].(Object)
					if alt["action"] != "CALL_TOOL" || alt["tool"] != "authorization.permissions.read" || args["view"] != view || !e.Valid("authorization.permissions.read", args) {
						t.Fatal("invalid permission alternative", alt)
					}
				}
			}
			if tc.Actor != "" && result["actor"] != tc.Actor || tc.Outcome != "" && result["outcome"] != tc.Outcome {
				t.Fatal("incorrect actor or outcome", result)
			}
			if tc.UserMessage != "" && result["userMessage"] != tc.UserMessage {
				t.Fatal("incorrect user message", result)
			}
			if tc.Alternative != nil {
				alt := result["alternatives"].([]any)[0].(Object)
				tool := tc.Alternative["tool"].(string)
				args, ok := alt["arguments"].(Object)
				if !ok || alt["tool"] != tool || !reflect.DeepEqual(args, tc.Alternative["arguments"]) || !e.Valid(tool, args) {
					t.Fatal("incorrect alternative", alt)
				}
			}
			if tc.Target != "" {
				args, ok := result["arguments"].(Object)
				targetTool := result["tool"]
				if tc.Action == "POLL" {
					ref := pollTarget(t, e, result)
					targetTool, args = ref.Read, asJSON(t, ref.Args)
				}
				if !ok || targetTool != tc.Target || !e.Valid(tc.Target, args) {
					t.Fatalf("invalid target %s %v", tc.Target, args)
				}
				if tc.NewCommand {
					key, _ := args["commandKey"].(string)
					id, err := uuid.Parse(key)
					if err != nil || id.Version() != 4 || id.Variant() != uuid.RFC4122 || key == tc.Input["commandKey"] {
						t.Fatal("missing fresh UUIDv4", args)
					}
					target := caps[tc.Target]
					retry := e.Evaluate(target, s.NextPolicies[target.NextPolicy], args, Object{"Status": 503}, true)
					ref := pollTarget(t, e, retry)
					if ref.Read != tc.Target || !reflect.DeepEqual(asJSON(t, ref.Args), args) {
						t.Fatal("retry changed command arguments", retry)
					}
				}
				if tc.Arguments != nil {
					want := maps.Clone(tc.Arguments)
					if tc.NewCommand {
						want["commandKey"] = args["commandKey"]
					}
					if !reflect.DeepEqual(want, args) {
						t.Fatalf("arguments = %v, want %v", args, want)
					}
				}
				if tc.Decision != "" {
					token := args["waitRef"].(string)
					ref, err := signer.Open(token, binding)
					if err != nil || ref.Decision != tc.Decision || !e.Valid(ref.Read, ref.ReadArgs()) {
						t.Fatalf("invalid wait binding for %s", tc.Name)
					}
					if _, err := signer.Open(token, []byte("other tenant")); err == nil {
						t.Fatal("waitRef accepted in a different scope")
					}
					if _, err := signer.Open("AA"+token[2:], binding); err == nil && !strings.HasPrefix(token, "AA") {
						t.Fatal("unauthenticated waitRef accepted")
					}
				}
			}
			if tc.Delay != 0 && result["pollAfterSeconds"] != float64(max(tc.Delay, orchestration.PollIntervalSeconds)) {
				t.Fatal("wrong poll delay", result)
			}
			if tc.TerminalStates != nil && !reflect.DeepEqual(result["terminalStates"], tc.TerminalStates) {
				t.Fatalf("terminalStates = %v, want %v", result["terminalStates"], tc.TerminalStates)
			}
		})
	}
	t.Run("next only from upload to candidate selection", func(t *testing.T) {
		tool := "source.file.upload"
		args := Object{}
		for _, name := range []string{"uploaded asset", "profile queued", "job has immutable profile", "preview incomplete second column", "column cursor exhausted", "durable plan", "planned discovery submitted", "owner requests poll", "discovery candidates", "candidate selection"} {
			found := false
			for _, tc := range cases {
				if tc.Name != name {
					continue
				}
				found = true
				if tc.Tool != tool {
					t.Fatalf("expected owner for %s, got %s", tc.Tool, tool)
				}
				c := caps[tool]
				result := asJSON(t, e.Evaluate(c, s.NextPolicies[c.NextPolicy], args, tc.Owner, false))
				assertProgress(t, tool, args, result)
				if err := contract.Validate(result); err != nil || result["action"] != tc.Action {
					t.Fatalf("%s: %v %v", tool, result, err)
				}
				tool, _ = result["tool"].(string)
				args, _ = result["arguments"].(Object)
				if result["action"] == "POLL" {
					ref := pollTarget(t, e, result)
					tool, args = ref.Read, asJSON(t, ref.Args)
				}
				if tool != "" && !e.Valid(tool, args) {
					t.Fatalf("invalid continuation %s", tool)
				}
				break
			}
			if !found {
				t.Fatalf("missing owner response %s", name)
			}
		}
	})
}

// Exercise the safe fallback for every registered policy, including ancillary
// tools. No unknown owner state can complete a workflow or trigger a command.
func TestUnknownOwnerState(t *testing.T) {
	s, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range s.ToolEligible() {
		p := s.NextPolicies[c.NextPolicy]
		next := (Engine{}).Evaluate(c, p, nil, Object{"state": "FUTURE_STATE"}, false)
		if c.ToolName == "authorization.permissions.read" {
			if next["action"] != "DONE" || next["outcome"] != "SUCCEEDED" {
				t.Fatalf("%s: %v", c.ToolName, next)
			}
			continue
		}
		if next["action"] != "BLOCKED" || next["reasonCode"] != "OWNER_STATE_UNKNOWN" || len(next["alternatives"].([]Object)) == 0 {
			t.Fatalf("%s: %v", c.ToolName, next)
		}
	}
}

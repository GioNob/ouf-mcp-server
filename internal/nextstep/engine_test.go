package nextstep

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/jsonschema-go/jsonschema"
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
	e := Engine{
		Errors: s.NextErrors,
		Valid: func(name string, args Object) bool {
			return inputs[name] != nil && inputs[name].Validate(asJSON(t, args)) == nil
		},
		Issue: func(r manifest.Rule, args, obs Object) Object {
			return signer.Issue(binding, r, args, obs, now)
		},
		Now: func() time.Time { return now },
	}
	var cases []struct {
		Name, Tool, Action, Target, Reason, Decision string
		Input, Owner, Arguments                      Object
		Failed                                       bool
		Delay                                        int
	}
	raw, err = os.ReadFile("testdata/transitions.json")
	if err != nil || json.Unmarshal(raw, &cases) != nil || len(cases) == 0 {
		t.Fatal("transition fixtures unavailable")
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			c := caps[tc.Tool]
			result := asJSON(t, e.Evaluate(c, s.NextPolicies[c.NextPolicy], tc.Input, tc.Owner, tc.Failed))
			if err := contract.Validate(result); err != nil {
				t.Fatal(err)
			}
			if result["action"] != tc.Action || (tc.Reason != "" && tc.Reason != result["reasonCode"]) {
				t.Fatalf("unexpected transition %v", result)
			}
			if tc.Target != "" {
				args, ok := result["arguments"].(Object)
				if !ok || result["tool"] != tc.Target || !e.Valid(tc.Target, args) {
					t.Fatalf("invalid target %s %v", tc.Target, args)
				}
				if tc.Arguments != nil && !reflect.DeepEqual(tc.Arguments, args) {
					t.Fatalf("arguments = %v, want %v", args, tc.Arguments)
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
			if tc.Delay != 0 && result["pollAfterSeconds"] != float64(tc.Delay) {
				t.Fatal("wrong poll delay", result)
			}
		})
	}
	t.Run("next only from upload to candidate selection", func(t *testing.T) {
		tool := "source.file.upload"
		args := Object{}
		for _, name := range []string{"uploaded asset", "profile queued", "job has immutable profile", "preview plans metadata discovery", "durable plan", "planned discovery submitted", "owner requests poll", "discovery candidates", "candidate selection"} {
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
				if err := contract.Validate(result); err != nil || result["action"] != tc.Action {
					t.Fatalf("%s: %v %v", tool, result, err)
				}
				tool, _ = result["tool"].(string)
				args, _ = result["arguments"].(Object)
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
		if next["action"] != "BLOCKED" || next["reasonCode"] != "OWNER_STATE_UNKNOWN" || len(next["alternatives"].([]Object)) == 0 {
			t.Fatalf("%s: %v", c.ToolName, next)
		}
	}
}

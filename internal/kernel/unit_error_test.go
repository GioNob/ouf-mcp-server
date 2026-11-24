package kernel

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
)

func TestUnitOwnerErrorsAndNext(t *testing.T) { // MCP-62E-06
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	source, _ := analysis.Find(m, "source.onboarding.create")
	e := nextstep.Engine{Errors: m.NextErrors, Valid: func(tool string, args nextstep.Object) bool {
		c, ok := analysis.Find(m, tool)
		var s jsonschema.Schema
		if !ok || json.Unmarshal(c.InputSchema, &s) != nil {
			return false
		}
		resolved, err := s.Resolve(nil)
		return err == nil && resolved.Validate(args) == nil
	}}
	args := railObject{"unitProfileRef": railObject{"profileId": unitProfile, "unitRef": "sheet:1"}}
	for _, tc := range []struct{ code, action, target, view string }{
		{"ONB_UNIT_SCHEMA_CHANGED", "CALL_TOOL", "source.file.preview", "UNIT"},
		{"ONB_UNIT_PROFILE_CHANGED", "CALL_TOOL", "source.file.preview", "UNIT"},
		{"ONB_PROFILE_UNIT_NOT_FOUND", "CALL_TOOL", "source.file.preview", "UNITS"},
		{"ONB_UNIT_REJECTED", "CALL_TOOL", "source.file.preview", "UNITS"},
		{"ONB_UNIT_SELECTION_CHANGED", "CALL_TOOL", "source.file.preview", "UNITS"},
		{"ONB_UNIT_ALREADY_BOUND", "BLOCKED", "source.file.preview", "UNITS"},
		{"ONB_UNIT_ASSET_ALREADY_BOUND", "BLOCKED", "source.file.preview", "ASSETS"},
		{"ONB_UNIT_PROFILE_OUTDATED", "BLOCKED", "source.file.preview", "UNITS"},
		{"ONB_UNIT_FORMAT_UNSUPPORTED", "BLOCKED", "source.file.upload", ""},
		{"ONB_FILE_MEDIA_TYPE_MISMATCH", "BLOCKED", "source.file.upload", ""},
		{"ONB_FILE_MEDIA_TYPE_UNSUPPORTED", "BLOCKED", "source.file.upload", ""},
		{"ONB_FILE_LIMIT_EXCEEDED", "BLOCKED", "source.file.upload", ""},
	} {
		t.Run(tc.code, func(t *testing.T) {
			result := ownerProblemResult(&orchestration.Problem{Code: tc.code, Status: 422, Detail: "private-fixture", Title: "private-fixture"}, []byte(`{"detail":"private-fixture","retryable":true}`))
			obs := ownerObject(result)
			next := e.Evaluate(source, m.NextPolicies[source.NextPolicy], args, obs, true)
			if !result.IsError || obs["retryable"] != false || next["action"] != tc.action || next["reasonCode"] != tc.code {
				t.Fatal(obs, next)
			}
			target := next
			if tc.action == "BLOCKED" {
				if next["actor"] != "AGENT" {
					t.Fatal(next)
				}
				target = next["alternatives"].([]nextstep.Object)[0]
			}
			if target["tool"] != tc.target {
				t.Fatal(target)
			}
			if tc.view != "" {
				a := target["arguments"].(nextstep.Object)
				if (tc.view != "ASSETS" && a["profileId"] != unitProfile) || a["view"] != tc.view || !e.Valid(tc.target, a) {
					t.Fatal(target)
				}
			}
			wire, _ := json.Marshal([]any{result, next})
			if strings.Contains(string(wire), "private-fixture") {
				t.Fatal("owner detail disclosed")
			}
		})
	}
}

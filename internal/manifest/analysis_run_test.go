package manifest

import (
	"encoding/json"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestRunClosedPlanBranches(t *testing.T) { // MCP-50U-15
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	var cap Capability
	for _, tool := range m.Capabilities {
		if tool.ToolName == "source.analysis.plan" {
			cap = tool
			break
		}
	}
	var input jsonschema.Schema
	if err = json.Unmarshal(cap.InputSchema, &input); err != nil {
		t.Fatal(err)
	}
	schema, err := input.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	args := map[string]any{"mode": "ALL_COLUMNS", "commandKey": id, "assetId": id, "profileId": id}
	if err = schema.Validate(args); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"work", "expectedRevision", "budget", "ordinals", "delegation", "policy"} {
		args[k] = 1
		if schema.Validate(args) == nil {
			t.Fatal("accepted", k)
		}
		delete(args, k)
	}
	for k, v := range map[string]any{"mode": "COLUMNS", "commandKey": "11111111-1111-1111-8111-111111111111", "assetId": nil, "profileId": nil} {
		old := args[k]
		args[k] = v
		if schema.Validate(args) == nil {
			t.Fatal("accepted invalid", k)
		}
		delete(args, k)
		if schema.Validate(args) == nil {
			t.Fatal("accepted missing", k)
		}
		args[k] = old
	}
}

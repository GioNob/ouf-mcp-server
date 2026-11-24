package manifest

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestSourceRevisionPrepareRequiresOneIntent(t *testing.T) {
	snapshot, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	for _, c := range snapshot.ToolEligible() {
		if c.ToolName == "source.onboarding.revision.prepare" {
			for _, instruction := range []string{"EXCLUDE", "MAP", "NEW_COLUMN_DECISION_REQUIRED", "present every named column to the user"} {
				if !strings.Contains(c.Description(), instruction) {
					t.Fatalf("tool description missing %q", instruction)
				}
			}
			if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	oldRules := schema.Properties["changes"].Items.Properties["reconciliation"]
	newRules := schema.Properties["newColumns"].Items.Properties["mapping"].Properties["reconciliation"]
	if !reflect.DeepEqual(oldRules, newRules) {
		t.Fatal("new column and property-change reconciliation contracts differ")
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	const base = `{"sourceId":"source-example","parentVersionId":"00000000-0000-4000-8000-000000000001","expectedVersion":7,"configurationHash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","commandKey":"second-upload-1","publicationEffect":"NEW_PUBLICATION_MAY_START_ACQUISITION"`
	const asset = `,"assetRef":{"assetId":"00000000-0000-4000-8000-000000000002","profileId":"00000000-0000-4000-8000-000000000003"}`
	const unit = `,"unitProfileRef":{"profileId":"00000000-0000-4000-8000-000000000003","unitRef":"sheet:1","profileVersion":1,"schemaHash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	const changes = `,"changes":[{"propertyIri":"urn:name","sourceField":"name","reconciliation":{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1","cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"SOURCE_INTERVAL"}}]`
	const reconciliation = `{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1","cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"UNBOUNDED"}`
	const exclude = `{"column":"note","decision":"EXCLUDE"}`
	const mapped = `{"column":"prezzo","decision":"MAP","mapping":{"propertyIri":"urn:example:prezzo","transform":"DECIMAL_DOT_V1","reconciliation":` + reconciliation + `}}`
	const columns = `,"newColumns":[`
	withDecisions := func(items ...string) string { return asset + columns + strings.Join(items, ",") + "]" }
	distinct := make([]string, 1025)
	for i := range distinct {
		distinct[i] = fmt.Sprintf(`{"column":"c%d","decision":"EXCLUDE"}`, i)
	}
	for _, tc := range []struct {
		name, intent string
		valid        bool
	}{
		{"asset", asset, true},
		{"changes", changes, true},
		{"both", asset + changes, false},
		{"incoming sheet", unit, true},
		{"incoming sheet with decisions", unit + `,"newColumns":[]`, true},
		{"two incoming selectors", asset + unit, false},
		{"sheet and changes", unit + changes, false},
		{"neither", "", false},
		{"null asset", `,"assetRef":null`, false},
		{"missing profile", `,"assetRef":{"assetId":"00000000-0000-4000-8000-000000000002"}`, false},
		{"missing asset", `,"assetRef":{"profileId":"00000000-0000-4000-8000-000000000003"}`, false},
		{"invalid UUID", `,"assetRef":{"assetId":"bad","profileId":"00000000-0000-4000-8000-000000000003"}`, false},
		{"extra field", `,"assetRef":{"assetId":"00000000-0000-4000-8000-000000000002","profileId":"00000000-0000-4000-8000-000000000003","path":"file"}`, false},
		{"explicit empty", withDecisions(), true},
		{"exclude", withDecisions(exclude), true},
		{"map", withDecisions(mapped), true},
		{"mixed", withDecisions(exclude, mapped), true},
		{"changes with decisions", changes + columns + exclude + "]", false},
		{"map without mapping", withDecisions(`{"column":"prezzo","decision":"MAP"}`), false},
		{"exclude with mapping", withDecisions(strings.Replace(mapped, `"MAP"`, `"EXCLUDE"`, 1)), false},
		{"relative property", withDecisions(strings.Replace(mapped, "urn:example:prezzo", "prezzo", 1)), false},
		{"unknown transform", withDecisions(strings.Replace(mapped, "DECIMAL_DOT_V1", "AUTO", 1)), false},
		{"missing rules", withDecisions(strings.Replace(mapped, `,"reconciliation":`+reconciliation, "", 1)), false},
		{"unknown mapping field", withDecisions(strings.Replace(mapped, `"transform":`, `"extra":0,"transform":`, 1)), false},
		{"duplicate authority", withDecisions(strings.Replace(mapped, `"transform":`, `"authorityOrder":["a","a"],"transform":`, 1)), false},
		{"incomplete comparison", withDecisions(strings.Replace(mapped, `"transform":`, `"comparison":{"comparator":"DECIMAL_V1"},"transform":`, 1)), false},
		{"1024 decisions", withDecisions(distinct[:1024]...), true},
		{"1025 decisions", withDecisions(distinct...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := base + tc.intent + "}"
			var args map[string]any
			if err := json.Unmarshal([]byte(input), &args); err != nil {
				t.Fatal(err)
			}
			if err := resolved.Validate(args); (err == nil) != tc.valid {
				t.Fatalf("valid=%t, got %v", tc.valid, err)
			}
		})
	}
}

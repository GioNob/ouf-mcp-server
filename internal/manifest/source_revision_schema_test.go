package manifest

import (
	"encoding/json"
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
			if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	const base = `{"sourceId":"source-example","parentVersionId":"00000000-0000-4000-8000-000000000001","expectedVersion":7,"configurationHash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","commandKey":"second-upload-1","publicationEffect":"NEW_PUBLICATION_MAY_START_ACQUISITION"`
	const asset = `,"assetRef":{"assetId":"00000000-0000-4000-8000-000000000002","profileId":"00000000-0000-4000-8000-000000000003"}`
	const changes = `,"changes":[{"propertyIri":"urn:name","sourceField":"name","reconciliation":{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1","cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"SOURCE_INTERVAL"}}]`
	for _, tc := range []struct {
		name, intent string
		valid        bool
	}{
		{"asset", asset, true},
		{"changes", changes, true},
		{"both", asset + changes, false},
		{"neither", "", false},
		{"null asset", `,"assetRef":null`, false},
		{"missing profile", `,"assetRef":{"assetId":"00000000-0000-4000-8000-000000000002"}`, false},
		{"missing asset", `,"assetRef":{"profileId":"00000000-0000-4000-8000-000000000003"}`, false},
		{"invalid UUID", `,"assetRef":{"assetId":"bad","profileId":"00000000-0000-4000-8000-000000000003"}`, false},
		{"extra field", `,"assetRef":{"assetId":"00000000-0000-4000-8000-000000000002","profileId":"00000000-0000-4000-8000-000000000003","path":"file"}`, false},
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

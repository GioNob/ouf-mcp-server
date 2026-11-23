package manifest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestAnalysisSummaryClosedSchemas(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	var cap Capability
	for _, c := range m.Capabilities {
		if c.ToolName == "source.analysis.read" {
			cap = c
			break
		}
	}
	resolve := func(raw json.RawMessage) *jsonschema.Resolved {
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		r, err := schema.Resolve(nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	in, out := resolve(cap.InputSchema), resolve(cap.OutputSchema)
	validate := func(schema *jsonschema.Resolved, body map[string]any) error {
		raw, _ := json.Marshal(body)
		var wire map[string]any
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		return schema.Validate(wire)
	}
	id := strings.Repeat("a", 64)
	for _, tc := range []struct {
		fields map[string]any
		valid  bool
	}{
		{map[string]any{}, true}, {map[string]any{"offset": 0, "limit": 128}, true},
		{map[string]any{"view": "STATUS"}, true}, {map[string]any{"view": "STATUS", "limit": 1}, false},
		{map[string]any{"view": "SUMMARY", "limit": 32}, true}, {map[string]any{"view": "SUMMARY", "cursor": id}, true},
		{map[string]any{"view": "SUMMARY", "cursor": id, "limit": 1}, false}, {map[string]any{"view": "SUMMARY", "offset": 0}, false},
		{map[string]any{"cursor": id}, false}, {map[string]any{"view": nil}, false}, {map[string]any{"view": "SUMMARY", "limit": 129}, false},
	} {
		tc.fields["analysisId"] = id
		if (validate(in, tc.fields) == nil) != tc.valid {
			t.Fatal(tc)
		}
	}
	body := map[string]any{"analysisId": id, "contract": "ouf.analysis.summary.v2", "changeSeq": "3", "state": "COMPLETE",
		"counts": map[string]any{"total": 1, "matchedPublished": 0, "candidatesReady": 0, "blocked": 0, "noCandidates": 1, "pending": 0},
		"next":   map[string]any{}}
	if err := validate(out, body); err != nil {
		t.Fatal(err)
	}
	item := map[string]any{"ordinal": 0, "changedSeq": "3", "state": "NO_CANDIDATES", "requestId": "11111111-1111-4111-8111-111111111111",
		"candidateCount": 0, "observedAt": "2030-01-01T10:00:00Z"}
	body["items"] = []any{item}
	if err := validate(out, body); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"PENDING", "CANDIDATES_READY", "BLOCKED", "MATCHED_PUBLISHED"} {
		item["state"] = state
		if validate(out, body) == nil {
			t.Fatalf("accepted zero evidence fields on %s", state)
		}
	}
}

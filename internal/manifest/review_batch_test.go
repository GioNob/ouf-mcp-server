package manifest

import (
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestReviewBatchClosedInputForms(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	id := "11111111-1111-4111-8111-111111111111"
	ref := strings.Repeat("a", 64)
	type args = map[string]any
	item := args{"revisionId": id, "expectedRowVersion": float64(0), "validationRunId": id}
	for _, c := range m.ToolEligible() {
		var good, bad []args
		switch c.ToolName {
		case "semantic.proposal.read":
			good = []args{{"commandKey": "legacy", "includeState": true}, {"reviewContextRef": ref, "expectedVersion": ref},
				{"reviewBatchId": id}, {"reviewBatchId": id, "expectedVersion": ref, "itemsOffset": float64(0)}, {"reviewBatchId": id, "itemsOffset": float64(4095)}}
			bad = []args{{}, {"commandKey": "legacy", "itemsOffset": float64(0)}, {"reviewContextRef": ref, "itemsOffset": float64(0)},
				{"reviewBatchId": id, "includeState": true}, {"reviewBatchId": id, "reviewContextRef": ref}, {"reviewContextRef": strings.ToUpper(ref)},
				{"reviewBatchId": id, "itemsOffset": float64(-1)}, {"reviewBatchId": id, "itemsOffset": float64(4096)}, {"reviewBatchId": id, "itemsOffset": 1.5}}
		case "semantic.approval.request":
			good = []args{{"revisionId": id, "expectedRowVersion": float64(0), "validationRunId": id},
				{"batchAction": "ADD", "reviewContextRef": ref, "item": item}, {"batchAction": "REMOVE", "reviewContextRef": ref, "item": args{"revisionId": id}},
				{"batchAction": "SUBMIT", "reviewContextRef": ref, "reviewBatchId": id}, {"batchAction": "RESUBMIT", "reviewContextRef": ref, "reviewBatchId": id}}
			bad = []args{{"batchAction": "ADD", "reviewContextRef": ref, "revisionId": id, "expectedRowVersion": float64(0), "validationRunId": id},
				{"batchAction": "REMOVE", "reviewContextRef": ref, "item": item}, {"batchAction": "ADD", "reviewContextRef": ref, "item": args{"revisionId": id}},
				{"batchAction": "SUBMIT", "reviewBatchId": id}, {"batchAction": "RESUBMIT", "reviewContextRef": ref, "reviewBatchId": id, "item": item}}
			for _, list := range [][]args{good, bad} {
				for _, a := range list {
					a["commandKey"] = "receipt"
				}
			}
		default:
			continue
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
			t.Fatal(err)
		}
		in, err := schema.Resolve(nil)
		if err != nil {
			t.Fatal(err)
		}
		for i, list := range [][]args{good, bad} {
			for _, a := range list {
				for _, context := range []bool{false, true} {
					input := maps.Clone(a)
					if context {
						input["analysisId"] = ref
					}
					if err := in.Validate(input); (err == nil) != (i == 0) {
						t.Fatalf("%s %v: %v", c.ToolName, input, err)
					}
				}
			}
		}
	}
}

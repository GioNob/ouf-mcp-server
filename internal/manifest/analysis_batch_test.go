package manifest

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestAnalysisBatchSchemas(t *testing.T) {
	snapshot, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	type obj = map[string]any
	id := strings.Repeat("a", 64)
	const key = "00000000-0000-4000-8000-000000000001"
	lookup := func(name string) Capability {
		for _, capability := range snapshot.ToolEligible() {
			if capability.ToolName == name {
				return capability
			}
		}
		t.Fatalf("missing tool %s", name)
		return Capability{}
	}
	resolve := func(raw json.RawMessage) *jsonschema.Resolved {
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		resolved, err := schema.Resolve(nil)
		if err != nil {
			t.Fatal(err)
		}
		return resolved
	}
	check := func(label string, schema *jsonschema.Resolved, value obj, valid bool) {
		t.Run(label, func(t *testing.T) {
			if err := schema.Validate(value); (err == nil) != valid {
				t.Fatalf("validation=%v, want valid=%t", err, valid)
			}
		})
	}
	preview := resolve(lookup("source.file.preview").InputSchema)
	check("preview", preview, obj{"mode": "BATCH_V1", "assetId": key, "profileId": key}, true)
	check("preview wrong mode", preview, obj{"mode": "BATCH_V2", "assetId": key, "profileId": key}, false)
	check("preview mixed selector", preview, obj{"mode": "BATCH_V1", "assetId": key, "profileId": key, "jobId": key}, false)
	plan := resolve(lookup("source.analysis.plan").InputSchema)
	start := obj{"mode": "BATCH_V1", "assetId": key, "profileId": key, "schemaHash": id, "commandKey": key}
	check("plan", plan, start, true)
	for field, bad := range (obj{"work": []any{}, "mode": "LEGACY", "schemaHash": "sha256:" + id, "commandKey": "00000000-0000-1000-8000-000000000001"}) {
		args := maps.Clone(start)
		args[field] = bad
		check("plan "+field, plan, args, false)
	}
	read := lookup("source.analysis.read")
	readInput := resolve(read.InputSchema)
	check("read batch", readInput, obj{"analysisId": id, "view": "BATCH"}, true)
	check("read status", readInput, obj{"analysisId": id, "view": "STATUS"}, true)
	check("read summary", readInput, obj{"analysisId": id, "view": "SUMMARY", "cursor": id}, true)
	check("read legacy page", readInput, obj{"analysisId": id, "offset": 1, "limit": 32}, true)
	page := obj{"analysisId": id, "view": "CANDIDATES", "batchId": key, "ordinal": 0, "cursor": id}
	check("read candidates", readInput, page, true)
	for _, field := range []string{"batchId", "ordinal", "cursor"} {
		args := maps.Clone(page)
		delete(args, field)
		check("candidates without "+field, readInput, args, false)
	}
	check("batch with offset", readInput, obj{"analysisId": id, "view": "BATCH", "offset": 0}, false)
	check("summary with ordinal", readInput, obj{"analysisId": id, "view": "SUMMARY", "ordinal": 0}, false)

	decide := lookup("source.analysis.decide")
	decisionInput := resolve(decide.InputSchema)
	proposal := obj{"artifactType": "PROPERTY", "semanticId": "urn:test:address", "namespace": "urn:test:", "localName": "address", "ownerRef": "owner",
		"authorityRef": "authority", "semanticVersion": "1.0.0", "labels": obj{"it": "Indirizzo"}, "definition": obj{}, "commandKey": key}
	actions := []any{
		obj{"ordinal": 0, "action": "ADOPT", "requestId": key, "candidateId": key},
		obj{"ordinal": 1, "action": "REFORMULATE", "intent": "via di accesso"},
		obj{"ordinal": 2, "action": "EXCLUDE", "reason": "Fuori dal mapping"},
		obj{"ordinal": 3, "action": "PROPOSE", "proposal": proposal},
		obj{"ordinal": 4, "action": "PROPOSE", "revisionId": key},
	}
	command := obj{"analysisId": id, "batchId": key, "expectedRevision": 1, "commandKey": key, "decisions": actions}
	check("decide mixed subset", decisionInput, command, true)
	for index, decision := range []obj{
		{"ordinal": 0, "action": "ADOPT", "requestId": key},
		{"ordinal": 0, "action": "ADOPT", "requestId": key, "candidateId": key, "intent": "via"},
		{"ordinal": 0, "action": "REFORMULATE"},
		{"ordinal": 0, "action": "EXCLUDE", "reason": "   "},
		{"ordinal": 0, "action": "PROPOSE", "proposal": obj{}},
		{"ordinal": 0, "action": "PROPOSE", "proposal": proposal, "revisionId": key},
		{"ordinal": 1024, "action": "EXCLUDE", "reason": "Scartata"},
		{"ordinal": 0, "action": "PUBLISH"},
	} {
		args := maps.Clone(command)
		args["decisions"] = []any{decision}
		check(fmt.Sprintf("invalid decision %d", index), decisionInput, args, false)
	}
	for _, n := range []int{0, 8, 9} {
		rows := make([]any, n)
		for i := range rows {
			rows[i] = obj{"ordinal": i, "action": "EXCLUDE", "reason": "Scartata"}
		}
		args := maps.Clone(command)
		args["decisions"] = rows
		check(fmt.Sprintf("%d decisions", n), decisionInput, args, n == 8)
	}
	// The embedded proposal resolves its own references against the decide schema.
	var definitions struct {
		Defs map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(decide.InputSchema, &definitions); err != nil {
		t.Fatal(err)
	}
	var embedded, native obj
	if json.Unmarshal([]byte(strings.ReplaceAll(string(definitions.Defs["nativeProposal"]), "#/$defs/nativeProposal/", "#/")), &embedded) != nil ||
		json.Unmarshal(lookup("semantic.propose").InputSchema, &native) != nil || !reflect.DeepEqual(embedded, native) {
		t.Fatal("PROPOSE diverges from semantic.propose")
	}

	readOutput := resolve(read.OutputSchema)
	item := obj{"ordinal": 0, "name": "indirizzo", "dataType": "string", "examples": []any{"via Roma 1"}, "attempt": 1,
		"remainingAttempts": 7, "intent": "indirizzo", "requestId": key, "candidateCount": 0, "candidates": []any{}, "allowed": []any{"REFORMULATE", "EXCLUDE", "PROPOSE"}}
	batch := obj{"mode": "BATCH_V1", "view": "BATCH", "analysisId": id, "batchId": key, "expectedRevision": 1, "items": []any{item}, "next": obj{}}
	check("batch shape", readOutput, batch, true)
	batch["items"] = []any{}
	check("empty batch", readOutput, batch, false)
	batch["items"] = []any{item, item, item, item, item, item, item, item, item}
	check("ninth batch item", readOutput, batch, false)
	pageOutput := obj{"mode": "BATCH_V1", "view": "CANDIDATES", "analysisId": id, "batchId": key, "ordinal": 0, "attempt": 1,
		"requestId": key, "candidateCount": 1, "candidates": []any{obj{"candidateId": key, "label": "Via", "term": "urn:test:via", "score": 0.85}}, "next": obj{}}
	check("candidate page shape", readOutput, pageOutput, true)
	counts := obj{"total": 1, "matchedPublished": 0, "candidatesReady": 0, "noCandidates": 0, "blocked": 0, "pending": 1}
	status := obj{"mode": "BATCH_V1", "analysisId": id, "revision": 0, "phase": "BLOCKED", "counts": counts, "next": obj{}}
	check("status shape", readOutput, status, true)
	summary := maps.Clone(status)
	summary["view"], summary["changeSeq"] = "SUMMARY", "1"
	summary["items"] = []any{obj{"ordinal": 0, "changedSeq": "1", "state": "EXCLUDED", "action": "EXCLUDE", "reason": "Fuori dal mapping"}}
	check("summary shape", readOutput, summary, true)
	items := make([]any, 9)
	for i := range items {
		items[i] = obj{"ordinal": i, "changedSeq": "1", "state": "PENDING"}
	}
	summary["items"] = items
	check("ninth summary item", readOutput, summary, false)
	receiptOutput := resolve(decide.OutputSchema)
	entry := obj{"ordinal": 0, "action": "ADOPT", "status": "RECORDED"}
	receipt := obj{"analysisId": id, "batchId": key, "revision": 2, "decisions": []any{entry, obj{"ordinal": 1, "action": "REFORMULATE", "status": "REJECTED", "reasonCode": "REFORMULATION_TERM_TOO_LONG"}}, "next": obj{}}
	check("receipt shape", receiptOutput, receipt, true)
	entry["status"] = "PUBLISHED"
	check("receipt foreign status", receiptOutput, receipt, false)

	approval := resolve(lookup("semantic.approval.request").InputSchema)
	legacy := obj{"revisionId": key, "expectedRowVersion": 1, "commandKey": key, "validationRunId": key}
	check("legacy approval", approval, legacy, true)
	add := obj{"analysisId": id, "projection": "BATCH_V1", "batchAction": "ADD", "reviewContextRef": id, "commandKey": key,
		"item": obj{"revisionId": key, "expectedRowVersion": 1, "validationRunId": key}}
	check("compact ADD", approval, add, true)
	add["revisionId"] = key
	check("mixed ADD", approval, add, false)
	review := resolve(lookup("semantic.proposal.read").InputSchema)
	check("legacy review", review, obj{"commandKey": key, "includeState": true}, true)
	byBatch := obj{"analysisId": id, "projection": "BATCH_V1", "reviewBatchId": key, "itemsOffset": 4095}
	check("compact review page", review, byBatch, true)
	byBatch["itemsOffset"] = 4096
	check("out of range review page", review, byBatch, false)
}

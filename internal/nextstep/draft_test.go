package nextstep

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

func TestDraftExits(t *testing.T) {
	raw, err := os.ReadFile("testdata/draft-exits.json")
	var f struct {
		Create, Draft, ExecutionProposal, PrepareFailure, Replacement, Prepared, Active Object
		ReaderFailure, Conflict, Stale                                                  Object
	}
	if err != nil || json.Unmarshal(raw, &f) != nil {
		t.Fatal("missing draft fixtures", err)
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*jsonschema.Resolved{}
	caps := map[string]manifest.Capability{}
	for _, c := range m.ToolEligible() {
		schemas[c.ToolName], caps[c.ToolName] = resolve(t, c.InputSchema), c
	}
	// The D-089 picker registration exposes an empty object. The legacy
	// attachment tool has the same name and requires a file.
	schemas["source.file.upload"] = resolve(t, []byte(`{"type":"object","additionalProperties":false,"properties":{}}`))
	e := Engine{Errors: m.NextErrors, Valid: func(tool string, args Object) bool {
		return schemas[tool] != nil && schemas[tool].Validate(asJSON(t, args)) == nil
	}}
	raw, err = os.ReadFile("../../contracts/ouf-next-step-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	contract := resolve(t, raw)
	run := func(t *testing.T, tool string, input, owner Object, failed bool) Object {
		t.Helper()
		if !e.Valid(tool, input) {
			t.Fatalf("invalid %s call: %v", tool, input)
		}
		c := caps[tool]
		next := e.Evaluate(c, m.NextPolicies[c.NextPolicy], input, owner, failed)
		wire := asJSON(t, next)
		assertProgress(t, tool, input, wire)
		if err := contract.Validate(wire); err != nil {
			t.Fatal(err)
		}
		if target, ok := next["tool"].(string); ok && !e.Valid(target, next["arguments"].(Object)) {
			t.Fatalf("invalid call to %s", target)
		}
		options, _ := next["alternatives"].([]Object)
		for _, alt := range options {
			if target, ok := alt["tool"].(string); ok {
				args, ok := alt["arguments"].(Object)
				completed := args
				if next["actor"] == "AGENT" && target == onboardingCreate {
					completed = maps.Clone(f.Create)
					maps.Copy(completed, args)
				}
				if !ok || !e.Valid(target, completed) {
					t.Fatalf("invalid %s alternative: %v", target, alt)
				}
			}
		}
		return next
	}
	only := func(t *testing.T, next Object, tool string) Object {
		t.Helper()
		options := next["alternatives"].([]Object)
		if next["action"] != "BLOCKED" || next["actor"] != "AGENT" || len(options) != 1 || options[0]["tool"] != tool {
			t.Fatalf("expected agent decision for %s: %v", tool, next)
		}
		return maps.Clone(options[0]["arguments"].(Object))
	}
	manual := func(t *testing.T, next Object) {
		t.Helper()
		alts := next["alternatives"].([]Object)
		if next["actor"] != "OPERATOR" || len(alts) != 1 || alts[0]["tool"] != nil {
			t.Fatal(next)
		}
	}
	createIDs := func(t *testing.T, next Object) Object {
		t.Helper()
		args := only(t, next, onboardingCreate)
		key, _ := args["commandKey"].(string)
		id, err := uuid.Parse(key)
		want := Object{"assetId": f.Create["assetId"], "profileId": f.Create["profileId"], "sourceId": f.Create["sourceId"], "analysisId": f.Create["analysisId"], "commandKey": key}
		if err != nil || id.Version() != 4 || key == f.Create["commandKey"] || !reflect.DeepEqual(args, want) {
			t.Fatal("expected only original identifiers and a fresh key", args)
		}
		return args
	}

	t.Run("MCP-50S-01 deferred CSV replaces the draft and prepares in two calls", func(t *testing.T) {
		proposal := run(t, onboardingCreate, f.Create, f.Draft, false)
		msg := proposal["userMessage"].(string)
		if proposal["reasonCode"] != "EXECUTION_PROPOSAL_REQUIRED" || !strings.Contains(msg, "deferredReaderChecks") || !strings.Contains(msg, "IDENTITY") {
			t.Fatal(proposal)
		}
		prepare := only(t, proposal, onboardingPrepare)
		prepare["executionProposal"] = f.ExecutionProposal
		rejected := run(t, onboardingPrepare, prepare, f.PrepareFailure, true)
		ids := createIDs(t, rejected)
		if ids["commandKey"] == prepare["commandKey"] || !strings.Contains(rejected["userMessage"].(string), "fields[].allowedTransforms") {
			t.Fatal(rejected)
		}
		create := asJSON(t, f.Create)
		maps.Copy(create, ids)
		field := create["fields"].([]any)[0].(Object)
		diag := f.PrepareFailure["fields"].([]any)[0].(Object)
		if field["fieldName"] != diag["fieldName"] || field["transform"] != "TEXT_NFC_TRIM_V1" || lookup(diag, "allowedTransforms.0") != "IDENTITY" {
			t.Fatal("fixture does not reproduce the CSV reader correction")
		}
		field["transform"] = lookup(diag, "allowedTransforms.0")
		next := run(t, onboardingCreate, create, f.Replacement, false)
		prepare = only(t, next, onboardingPrepare)
		prepare["executionProposal"] = f.ExecutionProposal
		if prepare["onboardingVersionId"] != f.Replacement["onboardingVersionId"] || !e.Valid(onboardingPrepare, prepare) {
			t.Fatal("second call does not prepare the replacement", prepare)
		}
		if result := run(t, onboardingPrepare, prepare, f.Prepared, false); result["reasonCode"] != "CURRENT_REVIEW_REQUIRED" {
			t.Fatal(result)
		}
	})

	t.Run("MCP-50S-02 reader refusal offers a new key", func(t *testing.T) {
		next := run(t, onboardingCreate, f.Create, f.ReaderFailure, true)
		createIDs(t, next)
		if !strings.Contains(next["userMessage"].(string), "fields[].allowedTransforms") {
			t.Fatal(next)
		}
	})

	t.Run("MCP-50S-03 conflict uses only authorized resolutions", func(t *testing.T) {
		owner := maps.Clone(f.Conflict)
		next := run(t, onboardingCreate, f.Create, owner, true)
		alts := next["alternatives"].([]Object)
		if len(alts) != 2 || alts[0]["tool"] != onboardingRead || alts[1]["tool"] != "source.file.upload" || len(alts[1]["arguments"].(Object)) != 0 {
			t.Fatal(next)
		}
		read := alts[0]["arguments"].(Object)
		revision := only(t, run(t, onboardingRead, read, f.Active, false), "source.onboarding.revision.prepare")
		if read["sourceId"] != f.Active["sourceId"] || revision["sourceId"] != read["sourceId"] || revision["parentVersionId"] != f.Active["onboardingVersionId"] || revision["expectedVersion"] != f.Active["lockVersion"] || revision["configurationHash"] != f.Active["configurationHash"] || lookup(revision, "assetRef.assetId") != f.Create["assetId"] {
			t.Fatal(revision)
		}
		delete(owner, "boundSourceId")
		only(t, run(t, onboardingCreate, f.Create, owner, true), "source.file.upload")
		owner["resolutions"] = []any{"CREATE_REVISION"}
		manual(t, run(t, onboardingCreate, f.Create, owner, true))
		delete(owner, "resolutions")
		manual(t, run(t, onboardingCreate, f.Create, owner, true))
	})

	t.Run("MCP-50S-04 stale replacement rechecks the current draft", func(t *testing.T) {
		read := only(t, run(t, onboardingCreate, f.Create, f.Stale, true), onboardingRead)
		next := run(t, onboardingRead, read, f.Replacement, false)
		createIDs(t, next)
		if read["view"] != "LATEST" || read["sourceId"] != f.Create["sourceId"] {
			t.Fatal(next)
		}
		active := maps.Clone(f.Replacement)
		active["workflowState"] = "ACTIVE"
		manual(t, run(t, onboardingRead, read, active, false))
		changed := maps.Clone(f.Replacement)
		changed["assetId"] = "99999999-9999-4999-8999-999999999999"
		manual(t, run(t, onboardingRead, read, changed, false))
	})

	t.Run("MCP-50S-05 datatype follows alternative or stops", func(t *testing.T) {
		prepare := only(t, run(t, onboardingCreate, f.Create, f.Draft, false), onboardingPrepare)
		owner := maps.Clone(f.PrepareFailure)
		owner["resolutions"] = []any{"UPLOAD_NEW_FILE"}
		only(t, run(t, onboardingPrepare, prepare, owner, true), "source.file.upload")
		owner["resolutions"] = []any{"CREATE_REVISION"}
		read := only(t, run(t, onboardingPrepare, prepare, owner, true), onboardingRead)
		if read["sourceId"] != prepare["sourceId"] || lookup(read, "revisionAssetRef.assetId") != f.Create["assetId"] {
			t.Fatal(read)
		}
		delete(owner, "resolutions")
		manual(t, run(t, onboardingPrepare, prepare, owner, true))

		delete(prepare, "draftAssetRef")
		delete(prepare, "analysisId")
		ids := only(t, run(t, onboardingPrepare, prepare, f.PrepareFailure, true), onboardingCreate)
		if len(ids) != 2 || ids["sourceId"] != prepare["sourceId"] || ids["commandKey"] == nil {
			t.Fatal("standalone preparation supplied unobserved identifiers", ids)
		}
	})

	t.Run("MCP-50S-06 predecessor schema and draft", func(t *testing.T) {
		extended := maps.Clone(f.Create)
		extended["semanticBindings"] = f.ExecutionProposal["semanticBindings"]
		if e.Valid(onboardingCreate, extended) {
			t.Fatal("create exposed ungoverned semanticBindings")
		}
		legacy := maps.Clone(f.Draft)
		delete(legacy, "deferredReaderChecks")
		next := run(t, onboardingCreate, f.Create, legacy, false)
		args := only(t, next, onboardingPrepare)
		if args["draftAssetRef"] != nil || strings.Contains(next["userMessage"].(string), "deferredReaderChecks") {
			t.Fatal("legacy preparation changed", next)
		}
	})

	t.Run("MCP-50S-07 next stays below 4 KiB for 1024 reported readers", func(t *testing.T) {
		create, draft := maps.Clone(f.Create), maps.Clone(f.Draft)
		fields, checks, diagnostics := []any{}, []any{}, []any{}
		for i := range 1024 {
			name := fmt.Sprintf("fixture_column_%04d", i)
			mapping := maps.Clone(f.Create["fields"].([]any)[0].(Object))
			mapping["fieldName"] = name
			fields = append(fields, mapping)
			checks = append(checks, Object{"fieldName": name, "transform": mapping["transform"]})
			diagnostics = append(diagnostics, Object{"fieldName": name, "transform": mapping["transform"], "allowedTransforms": []any{"IDENTITY"}})
		}
		create["fields"], create["sourceObjectKeyFields"] = fields, []any{"fixture_column_0000"}
		draft["deferredReaderChecks"] = checks
		rejected, unresolved := maps.Clone(f.ReaderFailure), maps.Clone(f.PrepareFailure)
		rejected["fields"], unresolved["fields"] = diagnostics, diagnostics
		proposal := run(t, onboardingCreate, create, draft, false)
		prepare := only(t, proposal, onboardingPrepare)
		prepare["executionProposal"] = f.ExecutionProposal
		reader := run(t, onboardingCreate, create, rejected, true)
		datatype := run(t, onboardingPrepare, prepare, unresolved, true)
		createIDs(t, reader)
		createIDs(t, datatype)
		for _, next := range []Object{proposal, reader, datatype,
			run(t, onboardingCreate, create, f.Stale, true),
			run(t, onboardingCreate, create, f.Conflict, true),
		} {
			raw, err := json.Marshal(next)
			if err != nil || len(raw) >= 4<<10 || strings.Contains(string(raw), "fixture_column_") {
				t.Fatalf("%s: %d bytes, err=%v", next["reasonCode"], len(raw), err)
			}
		}
	})
}

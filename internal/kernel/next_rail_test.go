package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type railObject = map[string]any

type railFixture struct {
	Steps []struct {
		Tool                string
		Input, Owner, Error railObject
	}
	ProfileColumns []string
	Continuation   map[string]railObject
	Catalogue      []railObject
}

// Only responses already delivered to the client may supply decision fields.
type mappingEvidence struct {
	columns                       []any
	candidatePage, candidateQuery railObject
	terms                         []railObject
}

func (e mappingEvidence) adoption() railObject {
	return railObject{
		"requestId":   e.candidateQuery["requestId"],
		"candidateId": e.candidatePage["adoptableCandidateIds"].([]any)[0],
	}
}

func (e mappingEvidence) source() railObject {
	class := e.terms[0]
	properties := map[string]any{}
	var pins []string
	for _, term := range e.terms {
		pins = append(pins, fmt.Sprintf("%s@%s", term["semantic_id"], term["semantic_version"]))
		if term["artifact_type"] == "PROPERTY" {
			properties[term["local_name"].(string)] = term["canonical_uri"]
		}
	}
	var fields []railObject
	var keys []string
	for _, col := range e.columns {
		name := col.(railObject)["name"].(string)
		f := railObject{"fieldName": name, "dataAccessLabel": "OPEN", "extractionDecision": "EXCLUDE"}
		if name == "telefono" {
			f["dataAccessLabel"] = "PERSONAL"
		}
		if iri := properties[name]; iri != nil {
			f["extractionDecision"], f["targetPropertyIri"] = "INCLUDE", iri
			f["transform"] = "TEXT_NFC_TRIM_V1"
			if name == "id_redazionale" {
				keys = append(keys, name)
			}
		}
		fields = append(fields, f)
	}
	return railObject{"sourceId": strings.ToLower(class["local_name"].(string)), "name": class["label"].(railObject)["it"],
		"owner": class["owner_ref"], "targetClassIri": class["canonical_uri"], "semanticRefs": pins,
		"sourceObjectKeyFields": keys, "fields": fields}
}

// MCP-50P-001. The agent proposes the synthetic UDP policy for review.
// Pins and limits come from the semantic.get and preview receipts.
func (e mappingEvidence) executionProposal(preview, source railObject) railObject {
	set := e.terms[0]["publication_set_id"]
	var bindings, signals, properties []railObject
	var ids []string
	for _, term := range e.terms {
		bindings = append(bindings, railObject{"semanticId": term["semantic_id"], "semanticVersion": term["semantic_version"],
			"revisionId": term["revision_id"], "publicationSetId": term["publication_set_id"]})
	}
	for _, field := range source["fields"].([]railObject) {
		if field["extractionDecision"] != "INCLUDE" {
			continue
		}
		iri := field["targetPropertyIri"].(string)
		ids = append(ids, iri)
		signals = append(signals, railObject{"id": iri, "semanticRef": iri + "@" + set.(string), "comparator": "TEXT_V1",
			"uniqueWithinScope": false, "excludesOnDisagreement": false, "assertionRef": "urn:test:review"})
		properties = append(properties, railObject{"sourceField": iri, "propertyIri": iri, "datatype": "http://www.w3.org/2001/XMLSchema#string",
			"accessLabel": field["dataAccessLabel"], "authorityOrder": []any{source["sourceId"]}})
	}
	identity := railObject{"ref": "urn:test:identity", "version": "1", "tenantId": "tenant", "canonicalClass": source["targetClassIri"],
		"sourceId": source["sourceId"], "maxCandidates": 10, "allowAutoNew": false, "signals": signals,
		"sufficientRules": []railObject{{"id": "all-three", "signalIds": ids, "assertionRef": "urn:test:review"}}}
	return railObject{"semanticBindings": bindings, "semanticPublicationSetRef": set,
		"udp": railObject{
			"resolution": railObject{"strategyId": "GOVERNED_IDENTITY", "strategyVersion": "1", "policyRef": identity["ref"], "governedIdentity": identity},
			"materialization": railObject{"policyRef": "urn:test:materialization", "properties": properties,
				"bitemporalProperties": []string{}, "checkpointInterval": 1},
		},
		"limits": railObject{"maxRows": preview["rowCount"], "maxColumns": len(e.columns), "maxCellChars": preview["sizeBytes"]}}
}

func TestNextOnlyCSVToObjects(t *testing.T) {
	for _, mode := range []string{"uninterrupted", "recorded refusals"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { runNextCSV(t, mode == "recorded refusals") })
		})
	}
}

func runNextCSV(t *testing.T, injectRefusals bool) {
	raw, err := os.ReadFile("../nextstep/testdata/e2e-20261123.json")
	if err != nil {
		t.Fatal(err)
	}
	var f railFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	s, err := newWorkflowServer(nil, snapshot, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	register := func(c manifest.Capability, raw []byte) {
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		resolved, err := schema.Resolve(nil)
		if err != nil {
			t.Fatal(err)
		}
		var budget struct{ HardMaxResultBytes int }
		if err := json.Unmarshal(c.BaseCostEnvelope, &budget); err != nil {
			t.Fatal(err)
		}
		decision, err := nextstep.DecisionSchema(raw)
		if err != nil {
			t.Fatal(err)
		}
		s.tools[c.ToolName] = registeredTool{capability: c, input: resolved, decision: decision, limit: budget.HardMaxResultBytes}
	}
	for _, c := range snapshot.ToolEligible() {
		if c.ToolName == "source.file.upload" {
			register(c, []byte(`{"type":"object","additionalProperties":false}`))
			c.ToolName = "source.file.upload.status"
			c.OperationClass = "READ"
			register(c, c.HandoffInputSchema)
		} else {
			register(c, c.InputSchema)
		}
	}
	ctx := context.WithValue(context.Background(), identityKey{}, requestIdentity{
		Identity: orchestration.Identity{ServicePrincipalID: "service", PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture"},
	})
	if s.thsLinks == nil {
		s.thsLinks = make(map[string]linkLookup)
	}
	s.thsLinks[semanticLinkID] = func(context.Context, railObject) nextstep.LinkResult {
		return nextstep.OwnerLink("https://example.invalid/semantic", "2099-12-15T12:00:00Z", time.Now())
	}
	contract := nextContract(t)
	next := railObject{"action": "CALL_TOOL", "tool": "source.file.upload", "arguments": railObject{}}
	check := func(tool string, input railObject, result *mcp.CallToolResult) {
		t.Helper()
		next = checkNext(t, contract, s.validateNext(tool, result))
		if next["actor"] == "OPERATOR" {
			t.Fatalf("%s stopped: %v", tool, next)
		}
		if next["action"] == "BLOCKED" {
			alts := next["alternatives"].([]any)
			if next["actor"] != "AGENT" || len(alts) == 0 {
				t.Fatal("unexpected block", next)
			}
			for _, item := range alts {
				if item.(railObject)["tool"] == tool {
					t.Fatalf("decision repeats %s: %v", tool, next)
				}
			}
		} else if target, ok := next["tool"].(string); ok {
			args := next["arguments"].(railObject)
			if !s.valid(target, args) || (next["action"] != "POLL" && tool == target && reflect.DeepEqual(args, input)) {
				t.Fatal("invalid continuation", next)
			}
		}
	}
	call := func(tool string, owner any, failed bool) railObject {
		t.Helper()
		if next["tool"] != tool || (next["action"] != "CALL_TOOL" && next["action"] != "POLL") {
			t.Fatalf("fixture expects %s; received %v", tool, next)
		}
		if next["action"] == "POLL" {
			time.Sleep(time.Duration(next["pollAfterSeconds"].(float64)) * time.Second)
		}
		args := next["arguments"].(railObject)
		if !s.valid(tool, args) {
			t.Fatal("invalid client arguments", tool, args)
		}
		wire, err := json.Marshal(owner)
		if err != nil {
			t.Fatal(err)
		}
		result := s.decorate(ctx, s.tools[tool], args, &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(wire)}}, IsError: failed,
		})
		check(tool, args, result)
		out := ownerObject(result)
		delete(out, "next")
		got, _ := json.Marshal(out)
		if out["legacyList"] == true {
			got, _ = json.Marshal(out["items"])
		}
		if string(got) != string(wire) {
			t.Fatalf("%s changed owner body", tool)
		}
		if failed {
			refusal := owner.(railObject)
			var textBody railObject
			if json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &textBody) != nil ||
				!reflect.DeepEqual(textBody["next"], next) || next["action"] != "POLL" || next["tool"] != tool ||
				!reflect.DeepEqual(next["arguments"], args) || next["deadline"] != refusal["windowEndsAt"] ||
				next["pollAfterSeconds"] != refusal["retryAfterSeconds"] {
				t.Fatal("admission refusal lost retry", next)
			}
		}
		// Read evidence from the delivered response using client-side JSON types.
		var delivered railObject
		envelope, _ := json.Marshal(out)
		if err := json.Unmarshal(envelope, &delivered); err != nil {
			t.Fatal(err)
		}
		return delivered
	}
	choose := func(tool string, decided railObject) {
		t.Helper()
		if next["action"] != "BLOCKED" || next["actor"] != "AGENT" {
			t.Fatal("expected an agent decision", next)
		}
		for _, item := range next["alternatives"].([]any) {
			alt := item.(railObject)
			if alt["tool"] != tool {
				continue
			}
			args := railObject{}
			if bound, ok := alt["arguments"].(railObject); ok {
				maps.Copy(args, bound)
			}
			for k, v := range decided {
				if _, bound := args[k]; bound {
					t.Fatal("decision overwrote a bound argument", k)
				}
				args[k] = v
			}
			if !s.valid(tool, args) {
				t.Fatal("decision violates target schema", tool, args)
			}
			next = maps.Clone(alt)
			next["arguments"] = args
			return
		}
		t.Fatal("decision outside the rail", tool, next)
	}
	wait := func(read string, observations ...railObject) {
		t.Helper()
		if next["action"] != "WAIT_FOR_HUMAN" || next["tool"] != "ouf.wait" {
			t.Fatal("expected a THS wait", next)
		}
		token := next["arguments"].(railObject)["waitRef"].(string)
		ref, err := s.waits.signer.Open(token, waitBinding(ctx))
		if err != nil || ref.Read != read {
			t.Fatal("wrong wait binding", ref, err)
		}
		reg := s.tools[read]
		index := 0
		reg.run = func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args railObject
			if json.Unmarshal(req.Params.Arguments, &args) != nil || !reflect.DeepEqual(args, ref.ReadArgs()) {
				t.Fatal("wait changed the authorized observation", args)
			}
			if index >= len(observations) {
				return nil, fmt.Errorf("extra owner read for %s", read)
			}
			observation := observations[index]
			index++
			return &mcp.CallToolResult{StructuredContent: observation}, nil
		}
		s.tools[read] = reg
		var req mcp.CallToolRequest
		if err := json.Unmarshal([]byte(`{"params":{"name":"ouf.wait","arguments":{}}}`), &req); err != nil {
			t.Fatal(err)
		}
		result := s.wait(ctx, &req, token)
		if result.IsError || index != len(observations) {
			t.Fatal("wait failed", ownerObject(result))
		}
		check("ouf.wait", railObject{"waitRef": token}, result)
	}

	real := f.Steps
	c := f.Continuation
	replay := func(i int) railObject {
		t.Helper()
		row := real[i]
		if !reflect.DeepEqual(next["arguments"], row.Input) {
			t.Fatal("recorded call differs", row.Tool, next)
		}
		if row.Error != nil {
			return call(row.Tool, row.Error, true)
		}
		return call(row.Tool, row.Owner, false)
	}
	replay(0)
	wait("source.file.upload.status", real[1].Owner, real[3].Owner)
	call("source.file.profile", c["profileJob"], false)
	if next["action"] != "POLL" {
		t.Fatal("expected machine poll", next)
	}
	call("source.file.preview", c["jobComplete"], false)
	if injectRefusals {
		replay(2)
	}
	profile := maps.Clone(real[2].Input)
	var columns []railObject
	for _, name := range f.ProfileColumns {
		columns = append(columns, railObject{"name": name, "distinctValuesComplete": true})
	}
	profile["columns"], profile["format"], profile["rowCount"] = columns, "CSV", 13
	profile["sizeBytes"] = 4096
	preview := call("source.file.preview", profile, false)
	evidence := mappingEvidence{columns: preview["columns"].([]any)}
	work := next["arguments"].(railObject)["work"].([]any)
	if len(work) != 18 || len(work) != len(columns) {
		t.Fatal("incomplete discovery plan", len(work))
	}
	aid := real[4].Input["analysisId"]
	var tasks []railObject
	for i, item := range work {
		job := item.(railObject)
		if job["arguments"].(railObject)["intent"] != columns[i]["name"] {
			t.Fatal("plan lost column intent", i)
		}
		id := fmt.Sprintf("%064x", i+1)
		if i == 0 {
			id = real[4].Input["taskId"].(string)
		}
		tasks = append(tasks, railObject{"taskId": id, "state": "READY", "tool": job["tool"], "columns": job["columns"]})
	}
	view := railObject{"analysisId": aid, "revision": 1, "tasks": tasks, "profile": real[2].Input, "hasMore": false}
	budget := railObject{"plannedCalls": len(tasks), "completedTasks": 0}
	view["budget"] = budget
	call("source.analysis.plan", view, false)
	for i, task := range tasks {
		args := next["arguments"].(railObject)
		if args["taskId"] != task["taskId"] || args["analysisId"] != aid {
			t.Fatal("selected wrong task", i, next)
		}
		receipt := maps.Clone(real[4].Owner)
		request := real[5].Input["requestId"].(string)
		if i != 0 {
			request = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
			result := maps.Clone(receipt["result"].(railObject))
			result["requestId"] = request
			receipt["taskId"], receipt["result"], receipt["columns"] = task["taskId"], result, task["columns"]
			receipt["arguments"] = work[i].(railObject)["arguments"]
		}
		if i == 3 {
			call("source.analysis.step", railObject{"state": "WAITING", "code": "ADMISSION_WINDOW_WAIT",
				"taskId": task["taskId"], "notBefore": time.Now().Add(75 * time.Second).Format(time.RFC3339)}, false)
			if next["action"] != "POLL" || !reflect.DeepEqual(next["arguments"], args) || next["pollAfterSeconds"] != float64(75) {
				t.Fatal("task wait changed the governed retry", next)
			}
		}
		call("source.analysis.step", receipt, false)
		task["state"] = "SUCCEEDED"
		budget["completedTasks"] = i + 1
		query := next["arguments"].(railObject)
		if query["requestId"] != request || query["analysisId"] != aid {
			t.Fatal("discovery observed a different job", next)
		}
		switch i {
		case 0:
			replay(5)
		case 1:
			call("semantic.discovery.status", railObject{"requestId": request, "state": "SUCCEEDED", "candidateCount": 1, "allowedActions": []string{"LIST_CANDIDATES"}}, false)
			evidence.candidateQuery = next["arguments"].(railObject)
			if evidence.candidateQuery["requestId"] != request || evidence.candidateQuery["analysisId"] != aid {
				t.Fatal("candidate listing lost its job", next)
			}
			evidence.candidatePage = call("semantic.discovery.candidates", c["candidatePage"], false)
		case 2:
			call("semantic.discovery.status", railObject{"state": "FAILED", "allowedActions": []string{"NONE"}}, false)
			if next["reasonCode"] != "DISCOVERY_STOPPED" {
				t.Fatal("failure confused with an empty success", next)
			}
			choose("source.analysis.read", nil)
		default:
			call("semantic.discovery.status", railObject{"state": "SUCCEEDED", "candidateCount": 0, "allowedActions": []string{"NONE"}}, false)
		}
		call("source.analysis.read", view, false)
	}
	if next["reasonCode"] != "MAPPING_DECISION_REQUIRED" {
		t.Fatal(next)
	}
	choose("semantic.discovery.adopt", evidence.adoption())
	receipt := maps.Clone(c["adoption"])
	receipt["requestId"] = evidence.candidateQuery["requestId"]
	call("semantic.discovery.adopt", receipt, false)
	call("semantic.review.prepare", c["validated"], false)
	call("semantic.approval.request", c["semanticReview"], false)
	wait("semantic.proposal.read", c["semanticActive"])
	for i, term := range f.Catalogue {
		call("source.analysis.read", view, false)
		query := "luogo"
		if i > 0 {
			// The mapping selects these three columns from the delivered preview.
			mapped := []int{0, 1, 6}
			query = evidence.columns[mapped[i-1]].(railObject)["name"].(string)
			if query != term["local_name"] {
				t.Fatal("fixture term does not correspond to the selected column", query)
			}
		}
		choose("semantic.search", railObject{"q": query})
		call("semantic.search", []railObject{term}, false)
		pin := next["arguments"].(railObject)
		if pin["analysisId"] != aid || pin["semanticId"] != term["semantic_id"] || pin["publicationSetId"] != term["publication_set_id"] || pin["revisionId"] != term["revision_id"] {
			t.Fatal("search did not lead to the exact published term", next)
		}
		detail := call("semantic.get", term, false)
		evidence.terms = append(evidence.terms, detail)
	}
	call("source.analysis.read", view, false)
	decision := evidence.source()
	if !reflect.DeepEqual(decision["sourceObjectKeyFields"], []string{"id_redazionale"}) || len(decision["fields"].([]railObject)) != 18 {
		t.Fatal("mapping lost key or columns", decision)
	}
	choose("source.onboarding.create", decision)
	create := next["arguments"].(railObject)
	if create["assetId"] != preview["assetId"] || create["profileId"] != preview["profileId"] || uuid.Validate(create["commandKey"].(string)) != nil {
		t.Fatal("missing bound source identity", create)
	}
	call("source.onboarding.create", c["draft"], false)
	if next["reasonCode"] != "EXECUTION_PROPOSAL_REQUIRED" {
		t.Fatal("incomplete draft offered an executable command", next)
	}
	proposal := evidence.executionProposal(preview, decision)
	if len(proposal["semanticBindings"].([]railObject)) != 4 || len(proposal["udp"].(railObject)["materialization"].(railObject)["properties"].([]railObject)) != 3 {
		t.Fatal("expected four published bindings and three INCLUDE mappings")
	}
	choose("source.onboarding.review.prepare", railObject{"executionProposal": proposal})
	prepare := next["arguments"].(railObject)
	if prepare["sourceId"] != create["sourceId"] || prepare["onboardingVersionId"] != c["draft"]["onboardingVersionId"] ||
		prepare["expectedVersion"] != c["draft"]["lockVersion"] || prepare["analysisId"] != aid || prepare["commandKey"] == create["commandKey"] {
		t.Fatal("prepare lost the observed draft identity or reused the create key", prepare)
	}
	review := call("source.onboarding.review.prepare", c["sourceReview"], false)
	if review["workflowState"] != "IN_REVIEW" {
		t.Fatal("owner did not issue a review", review)
	}
	wait("source.onboarding.read", c["activate"])
	if next["reasonCode"] != "HUMAN_ACTIVATION_REQUIRED" || next["deadline"] != c["activate"]["activationProgress"].(railObject)["budgetExpiresAt"] {
		t.Fatal("activation must use the owner decision window", next)
	}
	wait("source.onboarding.read", c["approved"])
	call("source.onboarding.read", c["active"], false)
	call("ouf.ingestion.status", c["running"], false)
	call("ouf.ingestion.status", c["complete"], false)
	if next["arguments"].(railObject)["type"] != decision["targetClassIri"] {
		t.Fatal("search lost canonical class", next)
	}
	objects := call("urban.object.search", c["objects"], false)
	if len(objects["items"].([]any)) != 13 || next["action"] != "DONE" || next["outcome"] != "SUCCEEDED" {
		t.Fatal("CSV rail did not finish", next)
	}
}

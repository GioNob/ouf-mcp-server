package nextstep

import (
	"encoding/json"
	"maps"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

type batchTests struct {
	Engine
	m       *manifest.Snapshot
	caps    map[string]manifest.Capability
	fixture map[string]Object
}

func batchTestEngine(t *testing.T) batchTests {
	t.Helper()
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	b := batchTests{m: m, caps: map[string]manifest.Capability{}}
	inputs := map[string]*jsonschema.Resolved{}
	for _, c := range m.ToolEligible() {
		b.caps[c.ToolName] = c
		inputs[c.ToolName] = resolve(t, c.InputSchema)
	}
	b.Errors = m.NextErrors
	b.Valid = func(tool string, args Object) bool {
		return inputs[tool] != nil && inputs[tool].Validate(asJSON(t, args)) == nil
	}
	b.Now = time.Now
	raw, err := os.ReadFile("testdata/review-batch.json")
	if err != nil || json.Unmarshal(raw, &b.fixture) != nil {
		t.Fatal("missing review batch observations", err)
	}
	return b
}

func (b batchTests) next(tool string, args, obs Object, failed bool) Object {
	c := b.caps[tool]
	return b.Evaluate(c, b.m.NextPolicies[c.NextPolicy], args, obs, failed)
}

func batchOptions(t *testing.T, next Object, tools ...string) []Object {
	t.Helper()
	alts, _ := next["alternatives"].([]Object)
	if next["action"] != "BLOCKED" || next["actor"] != "AGENT" || len(alts) != len(tools) {
		t.Fatalf("expected agent decision %v, got %v", tools, next)
	}
	for i, tool := range tools {
		if alts[i]["tool"] != tool {
			t.Fatalf("alternative %d: %v", i, alts[i])
		}
	}
	return alts
}

func TestBatchValidationEntryAndSingleReceipt(t *testing.T) {
	b := batchTestEngine(t)
	ref := b.fixture["collecting"]["reviewContextRef"]
	row := batchItems(b.fixture["collecting"])[0]
	obs := Object{"state": "VALIDATED", "allowedActions": []any{"REQUEST_APPROVAL"}, "rowVersion": 0,
		"revisionId": row["revisionId"], "currentValidationRunId": row["currentValidationRunId"]}
	legacy := Object{"status": "PASS", "validated_row_version": 0, "validation_run_id": row["currentValidationRunId"]}
	for _, tc := range []struct {
		tool string
		obs  Object
	}{{"semantic.propose", obs}, {reviewPrepare, obs}, {proposalRead, obs}, {reviewPrepare, legacy}} {
		for _, batch := range []bool{false, true} {
			input := Object{"commandKey": "receipt", "revisionId": row["revisionId"]}
			if batch {
				input["analysisId"] = ref
			}
			next := b.next(tc.tool, input, tc.obs, false)
			args, _ := next["arguments"].(Object)
			if next["tool"] != approvalRequest || !b.Valid(approvalRequest, args) {
				t.Fatal(tc, next)
			}
			item := args
			if batch {
				item, _ = args["item"].(Object)
				if args["batchAction"] != "ADD" || args["reviewContextRef"] != ref || args["analysisId"] != ref || args["revisionId"] != nil {
					t.Fatal(args)
				}
			} else if args["batchAction"] != nil || args["item"] != nil || args["analysisId"] != nil {
				t.Fatal(args)
			}
			if item["revisionId"] != row["revisionId"] || item["expectedRowVersion"] != 0 || item["validationRunId"] != row["currentValidationRunId"] {
				t.Fatal(item)
			}
		}
	}
}

func TestBatchObservationAlternatives(t *testing.T) {
	b := batchTestEngine(t)
	collect := b.fixture["collecting"]
	id, ref := collect["reviewBatchId"], collect["reviewContextRef"]
	ctx := Object{"reviewContextRef": ref, "analysisId": ref}
	byID := Object{"reviewBatchId": id, "analysisId": ref}
	receipt := Object{"commandKey": "submit", "analysisId": ref}
	for _, action := range []string{"ADD", "REMOVE"} {
		opts := batchOptions(t, b.next(approvalRequest, Object{"batchAction": action, "analysisId": ref}, collect, false),
			analysisRead, approvalRequest, approvalRequest)
		if opts[1]["arguments"].(Object)["batchAction"] != "SUBMIT" || opts[2]["arguments"].(Object)["batchAction"] != "REMOVE" {
			t.Fatal(opts)
		}
	}
	batchOptions(t, b.next(proposalRead, ctx, collect, false), approvalRequest, analysisRead, approvalRequest)
	for _, state := range []string{"NONE", "DISCARDED"} {
		obs := maps.Clone(b.fixture["none"])
		obs["state"] = state
		if state == "DISCARDED" {
			obs["reviewBatchId"] = id
		}
		batchOptions(t, b.next(proposalRead, ctx, obs, false), analysisRead, onboardingCreate)
	}
	all := b.fixture["decidedAllPublished"]
	for _, args := range []Object{ctx, {"reviewBatchId": all["reviewBatchId"]}} {
		batchOptions(t, b.next(proposalRead, args, all, false), onboardingCreate, analysisRead)
	}
	b.WaitRead = true
	n := b.next(proposalRead, Object{"reviewBatchId": all["reviewBatchId"], "expectedVersion": collect["version"]}, all, false)
	if n["tool"] != analysisRead || n["action"] != "CALL_TOOL" {
		t.Fatal("wait by batch ID should resume mapping", n)
	}
	b.WaitRead = false
	n = b.next(proposalRead, receipt, all, false)
	if n["action"] != "CALL_TOOL" || n["tool"] != analysisRead {
		t.Fatal(n)
	}
	remainder := maps.Clone(all)
	remainder["remainingCount"], remainder["remainingBatchId"] = 3, id
	alts := batchOptions(t, b.next(proposalRead, receipt, remainder, false), proposalRead, analysisRead)
	if alts[0]["arguments"].(Object)["reviewBatchId"] != id {
		t.Fatal(alts)
	}
	exp := b.fixture["expired"]
	alts = batchOptions(t, b.next(proposalRead, byID, exp, false), approvalRequest, analysisRead)
	resubmit := alts[0]["arguments"].(Object)
	if resubmit["batchAction"] != "RESUBMIT" || resubmit["reviewBatchId"] != id || resubmit["reviewContextRef"] != ref {
		t.Fatal(resubmit)
	}
	exp["successorBatchId"], exp["allowedActions"] = all["reviewBatchId"], []any{"NONE"}
	batchOptions(t, b.next(proposalRead, byID, exp, false), proposalRead, analysisRead)
	delete(exp, "successorBatchId")
	exp["counts"].(Object)["expired"] = 0
	batchOptions(t, b.next(proposalRead, byID, exp, false), analysisRead)
}

func TestBatchPagesUseTotals(t *testing.T) {
	b := batchTestEngine(t)
	for _, key := range []string{"collecting", "decidedMixed", "decidedAllPublished", "expired"} {
		obs := b.fixture[key]
		items := batchItems(obs)
		var page []any
		for i := 0; i < ReviewBatchPageItems; i++ {
			row := maps.Clone(items[0])
			row["revisionId"] = uuid.NewString()
			page = append(page, row)
		}
		obs["items"], obs["nextItemsOffset"], obs["itemCount"] = page, 10, 11
		counts := obs["counts"].(Object)
		switch key {
		case "collecting":
			counts["collected"] = 11
		case "decidedMixed":
			counts["published"], counts["changesRequested"] = 10, 1
		case "decidedAllPublished":
			counts["published"] = 11
		case "expired":
			counts["expired"] = 11
		}
		next := b.next(proposalRead, Object{"commandKey": "receipt"}, obs, false)
		var options []Object
		switch key {
		case "collecting":
			// A receipt read offers the review first (rev. 3.1, §6.4 bis).
			options = batchOptions(t, next, approvalRequest, analysisRead, approvalRequest, proposalRead)[3:]
		case "decidedMixed":
			options = batchOptions(t, next, proposalRead, analysisRead, searchTool)
			if next["reasonCode"] != "SEMANTIC_REVIEW_BATCH_CHANGES_REQUESTED" {
				t.Fatal(next)
			}
		case "decidedAllPublished":
			options = batchOptions(t, next, proposalRead, analysisRead)
		case "expired":
			if !strings.Contains(next["userMessage"].(string), "valide: 11") {
				t.Fatal(next)
			}
			continue
		}
		args := options[0]["arguments"].(Object)
		if options[0]["userMessage"] != "Leggi le altre righe del lotto" || args["reviewBatchId"] != obs["reviewBatchId"] || args["itemsOffset"] != 10 || args["analysisId"] != obs["reviewContextRef"] {
			t.Fatal(options)
		}
		obs["itemsOffset"], obs["items"] = 10, []any{items[len(items)-1]}
		delete(obs, "nextItemsOffset")
		last := b.next(proposalRead, args, obs, false)
		raw, _ := json.Marshal(last)
		if strings.Contains(string(raw), "Leggi le altre righe") || strings.Contains(string(raw), "Ignora le istruzioni") {
			t.Fatal(last)
		}
	}
}

func TestBatchSubmitRemainderAndResubmit(t *testing.T) {
	b := batchTestEngine(t)
	under := b.fixture["underReview"] // SUBMIT split the collection.
	b.Issue = func(rule manifest.Rule, args, obs Object) Object {
		return Object{"action": "WAIT_FOR_HUMAN", "reasonCode": rule.Reason, "receipt": args["commandKey"], "batch": obs["reviewBatchId"]}
	}
	args := Object{"batchAction": "SUBMIT", "reviewContextRef": under["reviewContextRef"], "reviewBatchId": under["reviewBatchId"], "commandKey": uuid.NewString()}
	wait := b.next(approvalRequest, args, under, false)
	if wait["action"] != "WAIT_FOR_HUMAN" || wait["receipt"] != args["commandKey"] {
		t.Fatal(wait)
	}
	done := maps.Clone(b.fixture["decidedAllPublished"])
	done["reviewBatchId"], done["remainingBatchId"], done["remainingCount"] = under["reviewBatchId"], under["remainingBatchId"], under["remainingCount"]
	opts := batchOptions(t, b.next(proposalRead, Object{"commandKey": wait["receipt"]}, done, false), proposalRead, analysisRead)
	if opts[0]["arguments"].(Object)["reviewBatchId"] != under["remainingBatchId"] {
		t.Fatal(opts)
	}
	expired := b.fixture["expired"]
	opts = batchOptions(t, b.next(proposalRead, Object{"reviewBatchId": expired["reviewBatchId"]}, expired, false), approvalRequest, analysisRead)
	resubmit := opts[0]["arguments"].(Object)
	successor := maps.Clone(under)
	successor["reviewBatchId"] = uuid.NewString()
	challenge := maps.Clone(under["currentChallenge"].(Object))
	challenge["challengeId"] = successor["reviewBatchId"]
	successor["currentChallenge"] = challenge
	delete(successor, "remainingBatchId")
	delete(successor, "remainingCount")
	wait = b.next(approvalRequest, resubmit, successor, false)
	if !b.Valid(approvalRequest, resubmit) || resubmit["batchAction"] != "RESUBMIT" || resubmit["reviewBatchId"] == wait["batch"] ||
		wait["action"] != "WAIT_FOR_HUMAN" || wait["receipt"] != resubmit["commandKey"] || wait["batch"] != successor["reviewBatchId"] {
		t.Fatal(resubmit, wait)
	}
	replay := Object{"replay": true, "readTool": proposalRead, "readArguments": Object{"commandKey": resubmit["commandKey"]}}
	n := b.next(approvalRequest, resubmit, replay, false)
	if n["reasonCode"] != "READ_ORIGINAL_RECEIPT" || n["arguments"].(Object)["commandKey"] != resubmit["commandKey"] {
		t.Fatal(n)
	}
}

func TestBatchConditionalReadAndWaitSelectors(t *testing.T) {
	b := batchTestEngine(t)
	obs := b.fixture["underReview"]
	b.Issue = func(r manifest.Rule, args, body Object) Object {
		if r.Decision != "APPROVE_EACH_ROW" || r.Tool != proposalRead || body["version"] != obs["version"] {
			t.Fatal(r, body)
		}
		return Object{"action": "WAIT_FOR_HUMAN", "reasonCode": r.Reason, "readArgs": args}
	}
	for _, input := range []Object{{"reviewContextRef": obs["reviewContextRef"]}, {"reviewBatchId": obs["reviewBatchId"], "itemsOffset": 0}, {"commandKey": "receipt"}} {
		want := Object{"analysisId": obs["reviewContextRef"], "reviewBatchId": obs["reviewBatchId"]}
		if key := input["commandKey"]; key != nil {
			delete(want, "reviewBatchId")
			want["commandKey"] = key
		}
		n := b.next(proposalRead, input, obs, false)
		if !reflect.DeepEqual(n["readArgs"], want) {
			t.Fatal(n)
		}
		input["expectedVersion"] = obs["version"]
		n = b.next(proposalRead, input, Object{"changed": false, "state": "UNDER_REVIEW"}, false)
		delete(input, "expectedVersion")
		if n["tool"] != proposalRead || !reflect.DeepEqual(n["arguments"], input) {
			t.Fatal(n)
		}
	}
}

func TestBatchOwnerRefusals(t *testing.T) {
	b := batchTestEngine(t)
	base := b.fixture["collecting"]
	id, ref := base["reviewBatchId"], base["reviewContextRef"]
	input := Object{"analysisId": ref, "reviewContextRef": ref, "batchAction": "ADD"}
	errors := b.fixture["errors"]
	next := func(suffix string, body Object) Object {
		obs := maps.Clone(body)
		obs["code"] = "SEM_REVIEW_BATCH_" + suffix
		return b.next(approvalRequest, input, obs, true)
	}
	body := errors["typeUnsupported"].(Object)
	opts := batchOptions(t, next("ITEM_TYPE_UNSUPPORTED", body), approvalRequest, analysisRead)
	single := opts[0]["arguments"].(Object)
	if body["artifactType"] != "VALUE_MAP" || single["batchAction"] != nil || single["revisionId"] != body["revisionId"] || single["validationRunId"] != body["validationRunId"] {
		t.Fatal(single)
	}
	batchOptions(t, next("ITEM_NOT_OWNED", body), analysisRead)
	duplicate := Object{"revisionId": body["revisionId"], "reviewContextRef": ref}
	batchOptions(t, next("ITEM_DUPLICATE_ARTIFACT", duplicate), proposalRead, approvalRequest, analysisRead)
	batchOptions(t, next("COLLECTING_LIMIT", errors["collectingLimit"].(Object)), approvalRequest)
	for _, suffix := range []string{"ITEM_UNDER_REVIEW", "NOT_COLLECTING", "ALREADY_RESUBMITTED"} {
		n := next(suffix, Object{"reviewBatchId": id, "successorBatchId": body["revisionId"]})
		want := id
		if suffix == "ALREADY_RESUBMITTED" {
			want = body["revisionId"]
		}
		if n["tool"] != proposalRead || n["arguments"].(Object)["reviewBatchId"] != want {
			t.Fatal(n)
		}
	}
	if n := next("EMPTY", body); n["tool"] != analysisRead {
		t.Fatal(n)
	}
	for _, suffix := range []string{"NOT_FOUND", "DECISION_REQUIRED", "DEPENDENCY_NOT_APPROVED", "STALE_CARD", "ALREADY_DECIDED"} {
		actor := "HUMAN_THS"
		if suffix == "NOT_FOUND" {
			actor = "OPERATOR"
		}
		if n := next(suffix, body); n["actor"] != actor || n["reasonCode"] != "SEM_REVIEW_BATCH_"+suffix {
			t.Fatal(n)
		}
	}
	notReady := errors["itemsNotReady"].(Object)
	row := batchItems(notReady)[0]
	for _, why := range []string{"ROW_VERSION_CHANGED", "VALIDATION_NOT_CURRENT", "NOT_DRAFT", "UNRESOLVED_DEPENDENCY"} {
		row["reason"] = why
		n := next("ITEMS_NOT_READY", notReady)
		if why == "ROW_VERSION_CHANGED" || why == "VALIDATION_NOT_CURRENT" {
			opts = batchOptions(t, n, reviewPrepare, approvalRequest)
			args := opts[0]["arguments"].(Object)
			if args["revisionId"] != row["revisionId"] || args["expectedRowVersion"] != int64(1) {
				t.Fatal(args)
			}
		} else {
			opts = batchOptions(t, n, approvalRequest)
		}
		args := opts[len(opts)-1]["arguments"].(Object)
		if args["batchAction"] != "REMOVE" || args["item"].(Object)["revisionId"] != row["revisionId"] {
			t.Fatal(args)
		}
	}
}

func TestMalformedBatchStopsAndUnavailableAlternativesAreFiltered(t *testing.T) {
	b := batchTestEngine(t)
	for key, value := range (Object{"state": "FUTURE", "version": "-", "counts": Object{}, "items": nil, "nextItemsOffset": -1,
		"reviewBatchId": "bad", "allowedActions": []any{"FUTURE"}, "reviewContextRef": strings.Repeat("Z", 64)}) {
		obs := maps.Clone(b.fixture["collecting"])
		obs[key] = value
		n := b.next(approvalRequest, Object{"batchAction": "ADD"}, obs, false)
		if n["reasonCode"] != "OWNER_STATE_UNKNOWN" || n["actor"] != "OPERATOR" {
			t.Fatal(key, n)
		}
	}
	b.Valid = func(tool string, args Object) bool { return tool == analysisRead }
	batchOptions(t, b.next(proposalRead, Object{}, b.fixture["none"], false), analysisRead)
	batchOptions(t, b.next(approvalRequest, Object{}, b.fixture["collecting"], false), analysisRead)
}

// Contract rev. 3.1, §6.4 bis: a wait signed for one batch never advances on
// an observation of another batch.
func TestBatchWaitMatchesSignedBatch(t *testing.T) {
	signed, other := "6f0c2c4e-3c1a-4f7e-9b1a-1a2b3c4d5e6f", "7f0c2c4e-3c1a-4f7e-9b1a-1a2b3c4d5e6f"
	ref := WaitRef{Read: proposalRead, Args: Object{"reviewBatchId": signed, "analysisId": strings.Repeat("a", 64)}}
	if !ref.MatchesResource(Object{"kind": "REVIEW_BATCH", "reviewBatchId": signed}) {
		t.Fatal("rejected the signed batch")
	}
	if ref.MatchesResource(Object{"kind": "REVIEW_BATCH", "reviewBatchId": other}) {
		t.Fatal("accepted another batch")
	}
}

package kernel

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type batchFixtureReview struct {
	id, context, submitKey string
	adds                   map[string]nextstep.Object
	submits                int
	expiry                 time.Time
	decided                bool
}

func (o *batchFixtureReview) execute(r orchestration.GatewayRequest) (bool, orchestration.GatewayResponse, error) {
	var args struct {
		AnalysisID, Projection, CommandKey, BatchAction, ReviewContextRef, ReviewBatchID, ChallengeID, RevisionID string
		ExpectedRowVersion                                                                                        int64
		Item                                                                                                      analysis.BatchReviewInput
	}
	err := json.Unmarshal(r.Arguments, &args)
	if err != nil {
		return true, orchestration.GatewayResponse{}, err
	}
	reply := func(body nextstep.Object) (bool, orchestration.GatewayResponse, error) {
		response, err := batchDecisionOwnerReply(body)
		return true, response, err
	}
	fail := func(format string, values ...any) (bool, orchestration.GatewayResponse, error) {
		return true, orchestration.GatewayResponse{}, fmt.Errorf(format, values...)
	}
	switch r.CapabilityID {
	case "ouf.semantic.review.prepare":
		if args.RevisionID == "" || args.CommandKey != r.IdempotencyKey {
			return fail("invalid validation binding")
		}
		return reply(nextstep.Object{"status": "PASS", "revision_id": args.RevisionID, "validation_run_id": uuid.NewString(), "validated_row_version": args.ExpectedRowVersion})
	case "ouf.semantic.approval.request":
		if args.Projection != "" || args.AnalysisID != "" || args.CommandKey != r.IdempotencyKey || analysis.ValidateID(args.ReviewContextRef) != nil {
			return fail("invalid owner review envelope")
		}
		switch args.BatchAction {
		case "ADD":
			if !analysis.CanonicalUUID(args.Item.RevisionID) || !analysis.CanonicalUUID(args.Item.ValidationRunID) || args.Item.ExpectedRowVersion != 0 {
				return fail("invalid ADD item")
			}
			if o.id == "" {
				o.id, o.context, o.adds = uuid.NewString(), args.ReviewContextRef, make(map[string]nextstep.Object)
			}
			if _, exists := o.adds[args.Item.RevisionID]; exists {
				return fail("duplicate ADD for revision")
			}
			o.adds[args.Item.RevisionID] = nextstep.Object{"revisionId": args.Item.RevisionID, "currentValidationRunId": args.Item.ValidationRunID,
				"rowVersion": args.Item.ExpectedRowVersion, "semanticId": "urn:fixture:review:" + args.Item.RevisionID}
			return reply(o.observation())
		case "SUBMIT":
			if args.ReviewBatchID != o.id || args.ReviewContextRef != o.context || len(o.adds) == 0 || o.submits != 0 {
				return fail("duplicate or unbound SUBMIT")
			}
			o.submits++
			o.submitKey, o.expiry = args.CommandKey, time.Now().Add(time.Hour).UTC()
			return reply(o.observation())
		}
	case "ouf.semantic.proposal.read":
		if (o.context != "" && args.ReviewContextRef == o.context) || (o.id != "" && args.ReviewBatchID == o.id) ||
			(o.submitKey != "" && args.CommandKey == o.submitKey) {
			if args.Projection != "" || args.AnalysisID != "" {
				return fail("local projection reached the owner")
			}
			return reply(o.observation())
		}
	case semanticLinkID:
		if args.ChallengeID != o.id {
			return fail("THS challenge was not bound to the lot")
		}
		link := "https://review.example/"
		return reply(nextstep.Object{"reviewUrl": link + strings.Repeat("a", 2000-len(link)), "expiresAt": o.expiry})
	}
	return false, orchestration.GatewayResponse{}, nil
}

func (o *batchFixtureReview) observation() nextstep.Object {
	state, itemState := "COLLECTING", "COLLECTED"
	count, action := "collected", "SUBMIT_REVIEW"
	if o.submits != 0 {
		state, itemState, count, action = "UNDER_REVIEW", "UNDER_REVIEW", "underReview", "WAIT_HUMAN_DECISION"
	}
	if o.decided {
		state, itemState, count, action = "DECIDED", "PUBLISHED", "published", "NONE"
	}
	items := make([]nextstep.Object, 0, len(o.adds))
	for _, revision := range slices.Sorted(maps.Keys(o.adds)) {
		row := maps.Clone(o.adds[revision])
		row["itemState"] = itemState
		if o.decided {
			row["publication"] = nextstep.Object{"publicationSetId": "11111111-1111-4111-8111-111111111111", "semanticVersion": "1"}
		}
		items = append(items, row)
	}
	obs := nextstep.Object{"kind": "REVIEW_BATCH", "reviewContextRef": o.context, "reviewBatchId": o.id, "version": analysis.Digest(state),
		"state": state, "itemCount": len(items), "counts": nextstep.Object{count: len(items)}, "items": items, "itemsOffset": 0, "allowedActions": []string{action}}
	if state == "UNDER_REVIEW" {
		obs["currentChallenge"] = nextstep.Object{"challengeId": o.id, "challengeStatus": "OPEN", "expiresAt": o.expiry}
	}
	return obs
}

func TestBatchReviewPostgresNextOnlyPET(t *testing.T) {
	db, _, worker, search, identity, id := batchWorkerFixtureWithScopes(t, 32, decisionScopes, 4)
	owner := &batchDecisionOwner{GatewayPort: search, receipts: map[string]nextstep.Object{},
		revisionStates: map[string]string{}, calls: map[string]int{}}
	worker.Service.Gateway = owner
	now := time.Now().Add(time.Second)
	worker.Now = func() time.Time { return now }
	client := revisionSessionForScope(t, worker.Service, analysis.IdentityScope(identity), decisionScopes)
	call := func(tool string, args nextstep.Object) nextstep.Object {
		return callBatchDecisionFixture(t, client, tool, args, false)
	}
	follow := func(next nextstep.Object) nextstep.Object {
		t.Helper()
		tool, _ := next["tool"].(string)
		args, _ := next["arguments"].(nextstep.Object)
		if tool == "" || args == nil {
			t.Fatal("missing continuation", next)
		}
		return call(tool, args)
	}
	nextOf := func(body nextstep.Object) nextstep.Object { return body["next"].(nextstep.Object) }
	start := call("source.analysis.read", nextstep.Object{"analysisId": id, "view": "STATUS"})
	if nextOf(start)["action"] != "POLL" {
		t.Fatal("first round has no wait", start)
	}
	batchWorkerDrain(t, worker, identity, id)
	offer := follow(nextOf(follow(nextOf(start))))
	items := offer["items"].([]any)
	if len(items) != 4 || search.candidateCalls != 4 {
		t.Fatal("first round did not cover the residual columns", offer)
	}
	next := nextOf(offer)
	decision := next["alternatives"].([]any)[0].(nextstep.Object)
	args := decision["arguments"].(nextstep.Object)
	args["decisions"] = []nextstep.Object{batchDecisionProposal(0), batchDecisionAdopt(offer, 1),
		{"ordinal": 2, "action": "EXCLUDE", "reason": "Colonna tecnica"},
		{"ordinal": 3, "action": "REFORMULATE", "intent": "residual corrected"},
	}
	receipt := follow(decision)
	statuses := decisionStatuses(receipt)
	if len(statuses) != 4 || slices.ContainsFunc(statuses, func(r analysis.BatchDecisionResult) bool { return r.Status != "RECORDED" }) {
		t.Fatal("decisions were not durably recorded", receipt)
	}
	now = time.Now().Add(time.Second)
	batchWorkerDrain(t, worker, identity, id)
	// Resume with just the ID after discarding the response to decide.
	client = revisionSessionForScope(t, worker.Service, analysis.IdentityScope(identity), decisionScopes)
	offer = call("source.analysis.read", nextstep.Object{"analysisId": id})
	items = offer["items"].([]any)
	if len(items) != 1 || fmt.Sprint(items[0].(nextstep.Object)["ordinal"]) != "3" ||
		fmt.Sprint(items[0].(nextstep.Object)["attempt"]) != "2" {
		t.Fatal("the next round did not isolate the reformulated ordinal", offer)
	}
	decision = nextOf(offer)["alternatives"].([]any)[0].(nextstep.Object)
	decision["arguments"].(nextstep.Object)["decisions"] = []nextstep.Object{batchDecisionAdopt(offer, 0)}
	receipt = follow(decision)
	now = time.Now().Add(time.Second)
	batchWorkerDrain(t, worker, identity, id)
	if len(owner.review.adds) != 3 || owner.review.submits != 0 {
		t.Fatal("collection did not checkpoint each PASS revision", owner.review.adds)
	}
	head := follow(nextOf(receipt))
	page := follow(nextOf(head))
	var ordinal int
	for page["view"] == "SUMMARY" {
		rows := page["items"].([]any)
		if len(rows) == 0 || len(rows) > 8 {
			t.Fatal("invalid summary page", page)
		}
		for _, value := range rows {
			item := value.(nextstep.Object)
			if fmt.Sprint(item["ordinal"]) != fmt.Sprint(ordinal) ||
				ordinal == 2 && (item["state"] != "EXCLUDED" || item["reason"] != "Colonna tecnica") ||
				ordinal != 2 && (item["revisionId"] == nil || item["semanticId"] == nil) {
				t.Fatal("summary lost an ordinal or a mapping receipt", item)
			}
			ordinal++
		}
		page = follow(nextOf(page))
	}
	if ordinal != 32 || nextOf(page)["reasonCode"] != nextstep.ReviewBatchCollecting {
		t.Fatal("summary did not reach the complete owner lot", ordinal, page)
	}
	var submits []nextstep.Object
	for _, value := range nextOf(page)["alternatives"].([]any) {
		option := value.(nextstep.Object)
		if option["tool"] == "semantic.approval.request" && option["arguments"].(nextstep.Object)["batchAction"] == "SUBMIT" {
			submits = append(submits, option)
		}
	}
	if len(submits) != 1 {
		t.Fatal("expected one offered SUBMIT", submits)
	}
	wait := nextOf(follow(submits[0]))
	if wait["action"] != "WAIT_FOR_HUMAN" || owner.review.submits != 1 {
		t.Fatal("no single THS wait", wait)
	}
	client = revisionSessionForScope(t, worker.Service, analysis.IdentityScope(identity), decisionScopes)
	resume := follow(nextOf(call("source.analysis.read", nextstep.Object{"analysisId": id})))
	if nextOf(resume)["action"] != "WAIT_FOR_HUMAN" {
		t.Fatal("resumed analysis did not regain the review wait", resume)
	}
	owner.review.decided = true
	executable := nextOf(follow(wait))
	if executable["reasonCode"] != nextstep.SourceMappingRequired || executable["tool"] != "source.analysis.read" {
		t.Fatal("wait did not hand off to mapping", executable)
	}
	var published int
	if err := db.Pool().QueryRow(t.Context(), `select count(*) from ouf_mcp.analysis_batch_review_item where analysis_id=$1 and state='PUBLISHED'`, id).Scan(&published); err != nil || published != 3 {
		t.Fatal("publication receipts were not retained", published, err)
	}
}

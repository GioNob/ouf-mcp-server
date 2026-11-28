package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const decisionScopes = batchWorkerScopes + " ouf.semantic.propose"

type batchDecisionOwner struct {
	orchestration.GatewayPort
	receipts             map[string]nextstep.Object
	revisionStates       map[string]string
	calls                map[string]int
	lose, refuse, absent string
	omitAdoptionField    string
	readRetryAfter       string
}

func (o *batchDecisionOwner) Execute(ctx context.Context, r orchestration.GatewayRequest, timeout time.Duration) (orchestration.GatewayResponse, error) {
	if r.CapabilityID != "ouf.semantic.discovery.adopt" && r.CapabilityID != "ouf.semantic.propose" &&
		r.CapabilityID != "ouf.semantic.proposal.read" {
		return o.GatewayPort.Execute(ctx, r, timeout)
	}
	o.calls[r.CapabilityID]++
	var args struct{ CommandKey, RequestID, CandidateID, SemanticID string }
	if err := json.Unmarshal(r.Arguments, &args); err != nil {
		return orchestration.GatewayResponse{}, err
	}
	if r.CapabilityID == "ouf.semantic.proposal.read" {
		if o.readRetryAfter != "" {
			after := o.readRetryAfter
			o.readRetryAfter = ""
			return orchestration.GatewayResponse{Status: 503, Problem: &orchestration.Problem{
				Status: 503, Code: "OWNER_UNAVAILABLE", RetryAfter: after,
			}}, nil
		}
		if receipt := o.receipts[args.CommandKey]; receipt != nil {
			response := maps.Clone(receipt)
			revision := receipt["result"].(nextstep.Object)["revisionId"].(string)
			state := o.revisionStates[revision]
			if state == "" {
				state = "DRAFT"
			}
			response["revisionId"], response["state"] = revision, state
			response["version"], response["changed"] = analysis.Digest([]string{revision, state}), true
			return batchDecisionOwnerReply(response)
		}
		return orchestration.GatewayResponse{Status: 404, Problem: &orchestration.Problem{Status: 404, Code: "PROPOSAL_RECEIPT_NOT_FOUND"}}, nil
	}
	if args.CommandKey != r.IdempotencyKey || !analysis.CanonicalUUID(args.CommandKey) {
		return orchestration.GatewayResponse{}, errors.New("owner command missing stable effect key")
	}
	if args.CandidateID != "" && args.CandidateID == o.refuse {
		return orchestration.GatewayResponse{Status: 409, Problem: &orchestration.Problem{Status: 409, Code: "CANDIDATE_EXPIRED"}}, nil
	}
	if args.CandidateID != "" && args.CandidateID == o.absent {
		return orchestration.GatewayResponse{}, context.DeadlineExceeded
	}
	receipt := o.receipts[args.CommandKey]
	if receipt == nil {
		body := nextstep.Object{"status": "DRAFT"}
		operation := "CREATE"
		if r.CapabilityID == "ouf.semantic.discovery.adopt" {
			operation = "ADOPT"
			if err := json.Unmarshal([]byte(adoptionReceipt), &body); err != nil {
				return orchestration.GatewayResponse{}, err
			}
			body["requestId"], body["candidateId"] = args.RequestID, args.CandidateID
			delete(body, o.omitAdoptionField)
		} else {
			body["semanticId"] = args.SemanticID
		}
		body["revisionId"] = uuid.NewString()
		receipt = nextstep.Object{"operation": operation, "result": body, "historicalSnapshot": true, "ownerStateMustBeRechecked": true}
		o.receipts[args.CommandKey] = receipt
	}
	if r.CapabilityID == o.lose {
		o.lose = ""
		return orchestration.GatewayResponse{}, context.DeadlineExceeded
	}
	return batchDecisionOwnerReply(receipt["result"])
}

func batchDecisionOwnerReply(body any) (orchestration.GatewayResponse, error) {
	wire, err := json.Marshal(body)
	return orchestration.GatewayResponse{Status: 200, Body: wire}, err
}

type batchDecisionFixture struct {
	db       *postgres.Store
	dsn, id  string
	worker   analysis.BatchWorker
	identity orchestration.Identity
	owner    *batchDecisionOwner
	search   *batchWorkerOwner
}

func newBatchDecisionFixture(t *testing.T, width int) batchDecisionFixture {
	t.Helper()
	db, dsn, worker, search, identity, id := batchWorkerFixtureWithScopes(t, width, decisionScopes)
	seedBatchDecisionEvidence(t, db, id, width)
	owner := &batchDecisionOwner{GatewayPort: search, receipts: make(map[string]nextstep.Object),
		revisionStates: make(map[string]string), calls: make(map[string]int)}
	worker.Service.Gateway = owner
	return batchDecisionFixture{db: db, dsn: dsn, id: id, worker: worker, identity: identity, owner: owner, search: search}
}

func seedBatchDecisionEvidence(t *testing.T, db *postgres.Store, id string, width int) {
	t.Helper()
	for ord := range width {
		request, candidate := uuid.NewString(), uuid.NewString()
		if _, err := db.Pool().Exec(t.Context(), `with col as (
			update ouf_mcp.analysis_column set request_id=$3,
			outcome=jsonb_build_object('state','CANDIDATES_READY','requestId',$3::uuid,
				'observedAt',statement_timestamp(),'candidateCount',1)
			where analysis_id=$1 and ordinal=$2 returning ordinal
		) update ouf_mcp.analysis_batch_column b set state='READY_FOR_DECISION',due_at=null,work_stage='CANDIDATES',
			evidence_version='fixture',candidates=jsonb_build_array(jsonb_build_object(
			'candidateId',$4::uuid,'label','Indirizzo','term','urn:fixture:address','score',0.9))
			from col where b.analysis_id=$1 and b.ordinal=col.ordinal`, id, ord, request, candidate); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Pool().Exec(t.Context(), `update ouf_mcp.analysis_counts set ready=total,pending=0 where analysis_id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func (f *batchDecisionFixture) restart(t *testing.T) {
	t.Helper()
	db, err := postgres.Open(t.Context(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	service := *f.worker.Service
	service.Admission = db
	f.db, f.worker.Store, f.worker.Service = db, db, &service
}

func (f batchDecisionFixture) session(t *testing.T) *mcp.ClientSession {
	t.Helper()
	return revisionSessionForScope(t, f.worker.Service, analysis.IdentityScope(f.identity), decisionScopes)
}

func callBatchDecisionFixture(t *testing.T, s *mcp.ClientSession, tool string, args nextstep.Object, failure bool) nextstep.Object {
	t.Helper()
	r, err := s.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	body := assertBatchWire(t, nextContract(t), r)
	if r.IsError != failure {
		t.Fatalf("%s: %v", tool, body)
	}
	return body
}

func (f batchDecisionFixture) offer(t *testing.T, s *mcp.ClientSession) nextstep.Object {
	t.Helper()
	return callBatchDecisionFixture(t, s, "source.analysis.read", nextstep.Object{"analysisId": f.id}, false)
}

func (f batchDecisionFixture) decide(t *testing.T, s *mcp.ClientSession, command nextstep.Object, failure bool) nextstep.Object {
	t.Helper()
	return callBatchDecisionFixture(t, s, "source.analysis.decide", command, failure)
}

func batchDecisionCommand(offer nextstep.Object, items ...nextstep.Object) nextstep.Object {
	return nextstep.Object{"analysisId": offer["analysisId"], "batchId": offer["batchId"],
		"expectedRevision": offer["expectedRevision"], "commandKey": uuid.NewString(), "decisions": items}
}

func batchDecisionAdopt(offer nextstep.Object, index int) nextstep.Object {
	item := offer["items"].([]any)[index].(nextstep.Object)
	candidate := item["candidates"].([]any)[0].(nextstep.Object)
	return nextstep.Object{"ordinal": item["ordinal"], "action": "ADOPT", "requestId": item["requestId"], "candidateId": candidate["candidateId"]}
}

func batchDecisionProposal(ordinal int) nextstep.Object {
	return nextstep.Object{"ordinal": ordinal, "action": "PROPOSE", "proposal": nextstep.Object{
		"commandKey": "chat-key", "semanticId": "urn:fixture:NewProperty", "artifactType": "PROPERTY",
		"namespace": "ouf", "localName": "NewProperty", "ownerRef": "owner", "authorityRef": "authority",
		"semanticVersion": "1.0.0", "labels": nextstep.Object{"it": "Proprietà"}, "definition": nextstep.Object{},
	}}
}

func decisionStatuses(body nextstep.Object) []analysis.BatchDecisionResult {
	data, _ := json.Marshal(body["decisions"])
	var result []analysis.BatchDecisionResult
	_ = json.Unmarshal(data, &result)
	return result
}

type loseBatchDecisionReceipt struct{ *postgres.Store }

func (s loseBatchDecisionReceipt) DecideBatch(ctx context.Context, scope analysis.Scope, sum string, start analysis.BatchStart, cmd analysis.BatchDecide) (analysis.BatchReceipt, error) {
	if _, err := s.Store.DecideBatch(ctx, scope, sum, start, cmd); err != nil {
		return analysis.BatchReceipt{}, err
	}
	return analysis.BatchReceipt{}, errors.New("connection lost after commit")
}

func TestBatchDecidePostgresMixedPartialAndRestart(t *testing.T) {
	f := newBatchDecisionFixture(t, 6)
	session := f.session(t)
	offer := f.offer(t, session)
	adopt := batchDecisionAdopt(offer, 0)
	exclude := nextstep.Object{"ordinal": 2, "action": "EXCLUDE", "reason": " Fuori dal mapping "}
	command := batchDecisionCommand(offer,
		adopt, nextstep.Object{"ordinal": 1, "action": "REFORMULATE", "intent": "  residual   fresh "}, exclude,
		batchDecisionProposal(3), nextstep.Object{"ordinal": 4, "action": "REFORMULATE", "intent": "una due tre quattro"})
	// A failure after choices, reserve and CAS have been written still rolls
	// back the entire command, including the offer retirement and receipt.
	_, err := f.db.Pool().Exec(t.Context(), `create function ouf_mcp.abort_batch_commit() returns trigger language plpgsql as $$
		begin raise exception 'simulated crash before commit'; end $$;
		create trigger abort_batch_commit before update on ouf_mcp.analysis_batch_offer
		for each row execute function ouf_mcp.abort_batch_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	f.decide(t, session, command, true)
	var receipts, choices, reserves int
	if err = f.db.Pool().QueryRow(t.Context(), `select
		(select count(*) from ouf_mcp.analysis_batch_receipt),
		(select count(*) from ouf_mcp.analysis_batch_choice),
		(select count(*) from ouf_mcp.analysis_reformulation_budget)`).Scan(&receipts, &choices, &reserves); err != nil ||
		receipts+choices+reserves != 0 || f.offer(t, session)["batchId"] != offer["batchId"] {
		t.Fatal("transaction leaked changes", receipts, choices, reserves, err)
	}
	if _, err = f.db.Pool().Exec(t.Context(), `drop trigger abort_batch_commit on ouf_mcp.analysis_batch_offer`); err != nil {
		t.Fatal(err)
	}
	lostService := *f.worker.Service
	lostService.Admission = loseBatchDecisionReceipt{f.db}
	lost := revisionSessionForScope(t, &lostService, analysis.IdentityScope(f.identity), decisionScopes)
	f.decide(t, lost, command, true)

	f.restart(t)
	session = f.session(t)
	residual := f.offer(t, session)
	items := residual["items"].([]any)
	if len(items) != 2 || fmt.Sprint(items[0].(nextstep.Object)["ordinal"]) != "4" ||
		fmt.Sprint(items[1].(nextstep.Object)["ordinal"]) != "5" || residual["batchId"] == offer["batchId"] {
		t.Fatal("rejected or omitted items absent from residual offer", residual)
	}
	replayInput := maps.Clone(command)
	normalized := slices.Clone(command["decisions"].([]nextstep.Object))
	normalized[1] = nextstep.Object{"ordinal": 1, "action": "REFORMULATE", "intent": "residual fresh"}
	slices.Reverse(normalized)
	replayInput["decisions"] = normalized
	replay := f.decide(t, session, replayInput, false)
	want := []analysis.BatchDecisionResult{
		{Ordinal: 0, Action: "ADOPT", Status: "RECORDED"}, {Ordinal: 1, Action: "REFORMULATE", Status: "RECORDED"},
		{Ordinal: 2, Action: "EXCLUDE", Status: "RECORDED"}, {Ordinal: 3, Action: "PROPOSE", Status: "RECORDED"},
		{Ordinal: 4, Action: "REFORMULATE", Status: "REJECTED", ReasonCode: "REFORMULATION_TERM_TOO_LONG"},
	}
	if fmt.Sprint(replay["revision"]) != "1" || !reflect.DeepEqual(decisionStatuses(replay), want) ||
		replay["next"].(nextstep.Object)["tool"] != "source.analysis.read" {
		t.Fatal("unexpected replay receipt", replay)
	}
	var saved []byte
	if err = f.db.Pool().QueryRow(t.Context(), `select receipt from ouf_mcp.analysis_batch_receipt where command_key=$1`, command["commandKey"]).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	var stored nextstep.Object
	if err = json.Unmarshal(saved, &stored); err != nil {
		t.Fatal(err)
	}
	receiptOnly := maps.Clone(replay)
	delete(receiptOnly, "next")
	got, _ := json.Marshal(receiptOnly)
	expected, _ := json.Marshal(stored)
	if string(got) != string(expected) {
		t.Fatal("replay differs from the committed receipt", string(got), string(expected))
	}
	changed := maps.Clone(command)
	changed["decisions"] = []nextstep.Object{{"ordinal": 2, "action": "EXCLUDE", "reason": "Altra motivazione"}}
	if result := f.decide(t, session, changed, true); result["code"] != "IDEMPOTENCY_KEY_REUSE" {
		t.Fatal("same key was reused against different content", result)
	}

	// Stop after reserving the first worker attempt, before any owner I/O.
	sum, _ := f.worker.Manifest.Checksum()
	now := time.Now().UTC()
	preDispatch, err := f.db.ClaimBatchWork(t.Context(), analysis.IdentityScope(f.identity), f.id, sum,
		analysis.BatchAuthorityBinding(f.identity), now, 1)
	if err != nil || preDispatch.Stage != analysis.BatchAdopt {
		t.Fatal("expected first queued adoption", preDispatch.Stage, err)
	}
	if err = f.db.BeginBatchDispatch(t.Context(), preDispatch, []int{0}, now); err != nil {
		t.Fatal(err)
	}
	f.owner.lose = "ouf.semantic.propose"
	f.restart(t)
	now = preDispatch.LeaseUntil.Add(time.Second)
	f.worker.Now = func() time.Time { return now }
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	now = now.Add(time.Minute)
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	if f.owner.calls["ouf.semantic.discovery.adopt"] != 1 || f.owner.calls["ouf.semantic.propose"] != 1 ||
		f.owner.calls["ouf.semantic.proposal.read"] != 1 || len(f.owner.receipts) != 2 || f.search.commandCalls != 1 {
		t.Fatal("worker did not reconcile once per effect", f.owner.calls, len(f.owner.receipts), f.search.commandCalls)
	}
	var applied, ownerRefs int
	if err = f.db.Pool().QueryRow(t.Context(), `select count(*) filter(where effect_state='APPLIED'),count(owner_ref)
		from ouf_mcp.analysis_batch_choice where analysis_id=$1`, f.id).Scan(&applied, &ownerRefs); err != nil || applied != 4 || ownerRefs != 3 {
		t.Fatal("committed choices missing owner checkpoints", applied, ownerRefs, err)
	}
	session = f.session(t)
	current := f.offer(t, session)
	if current["batchId"] != residual["batchId"] {
		t.Fatal("unrelated worker progress invalidated the residual offer", current)
	}
	var revision string
	if err = f.db.Pool().QueryRow(t.Context(), `select owner_ref::text from ouf_mcp.analysis_batch_choice
		where analysis_id=$1 and action='PROPOSE'`, f.id).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	draft := nextstep.Object{"ordinal": 4, "action": "PROPOSE", "revisionId": revision}
	if body := f.decide(t, session, batchDecisionCommand(current, draft), false); decisionStatuses(body)[0].Status != "RECORDED" {
		t.Fatal("the scoped draft reference was not accepted", body)
	}
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	var linked string
	err = f.db.Pool().QueryRow(t.Context(), `select owner_ref::text from ouf_mcp.analysis_batch_choice
		where analysis_id=$1 and ordinal=4 and effect_code='DRAFT_RECEIPT_VERIFIED'`, f.id).Scan(&linked)
	if err != nil || linked != revision || f.owner.calls["ouf.semantic.propose"] != 1 ||
		f.owner.calls["ouf.semantic.proposal.read"] != 2 {
		t.Fatal("proposal reference did not verify the current draft", linked, f.owner.calls, err)
	}
}

func TestBatchDecidePostgresOrdinalIsExact(t *testing.T) {
	f := newBatchDecisionFixture(t, 2)
	session := f.session(t)
	offer := f.offer(t, session)
	item := nextstep.Object{"action": "EXCLUDE", "reason": "Fuori dal mapping"}
	command := batchDecisionCommand(offer, item)
	for _, invalid := range []json.Number{"1.00000000000000001", "0.5", "1.0", "-1", "1024", "9223372036854775808"} {
		item["ordinal"] = invalid
		if body := f.decide(t, session, command, true); body["code"] != "INVALID_TOOL_ARGUMENTS" {
			t.Fatalf("ordinal %s must produce an envelope error: %v", invalid, body)
		}
	}
	item["ordinal"] = json.Number("1")
	if results := decisionStatuses(f.decide(t, session, command, false)); len(results) != 1 ||
		results[0].Ordinal != 1 || results[0].Status != "RECORDED" {
		t.Fatal("the exact ordinal was not recorded", results)
	}
}

func TestBatchDecidePostgresAdoptRequiresCompleteReceipt(t *testing.T) {
	for _, field := range []string{"provenance", "expectedRowVersion"} {
		t.Run(field, func(t *testing.T) {
			f := newBatchDecisionFixture(t, 1)
			session := f.session(t)
			offer := f.offer(t, session)
			f.owner.omitAdoptionField = field
			f.decide(t, session, batchDecisionCommand(offer, batchDecisionAdopt(offer, 0)), false)
			now := time.Now().UTC()
			f.worker.Now = func() time.Time { return now }
			batchWorkerDrain(t, f.worker, f.identity, f.id)
			var state string
			var linked bool
			if err := f.db.Pool().QueryRow(t.Context(), `select effect_state,owner_ref is not null
				from ouf_mcp.analysis_batch_choice where analysis_id=$1`, f.id).Scan(&state, &linked); err != nil || state != "PENDING" || linked {
				t.Fatal("incomplete acknowledgement did not remain pending", state, linked, err)
			}
			now = now.Add(time.Minute)
			batchWorkerDrain(t, f.worker, f.identity, f.id)
			var code string
			if err := f.db.Pool().QueryRow(t.Context(), `select effect_state,effect_code,owner_ref is not null
				from ouf_mcp.analysis_batch_choice where analysis_id=$1`, f.id).Scan(&state, &code, &linked); err != nil ||
				state != "HELD" || code != "OUTCOME_REQUIRES_RECONCILIATION" || linked ||
				f.owner.calls["ouf.semantic.discovery.adopt"] != 1 || f.owner.calls["ouf.semantic.proposal.read"] != 1 {
				t.Fatal("incomplete stored receipt did not remain unresolved", state, code, linked, f.owner.calls, err)
			}
		})
	}
}

func TestBatchDecidePostgresDraftRequiresCurrentOwnerState(t *testing.T) {
	f := newBatchDecisionFixture(t, 2)
	session := f.session(t)
	offer := f.offer(t, session)
	f.decide(t, session, batchDecisionCommand(offer, batchDecisionProposal(0)), false)
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	var revision string
	if err := f.db.Pool().QueryRow(t.Context(), `select owner_ref::text from ouf_mcp.analysis_batch_choice
		where analysis_id=$1 and ordinal=0`, f.id).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	f.owner.revisionStates[revision] = "UNDER_REVIEW"
	draft := nextstep.Object{"ordinal": 1, "action": "PROPOSE", "revisionId": revision}
	f.decide(t, session, batchDecisionCommand(f.offer(t, session), draft), false)
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	var state string
	var linked bool
	if err := f.db.Pool().QueryRow(t.Context(), `select effect_state,owner_ref is not null
		from ouf_mcp.analysis_batch_choice where analysis_id=$1 and ordinal=1`, f.id).Scan(&state, &linked); err != nil ||
		state != "HELD" || linked || f.owner.calls["ouf.semantic.propose"] != 1 || f.owner.calls["ouf.semantic.proposal.read"] != 1 {
		t.Fatal("historical draft was linked after a change of state", state, linked, f.owner.calls, err)
	}
}

func TestBatchDecidePostgresConcurrentCAS(t *testing.T) {
	f := newBatchDecisionFixture(t, 3)
	a := f.session(t)
	offer := f.offer(t, a)
	exclude := func(ordinal int) nextstep.Object {
		return nextstep.Object{"ordinal": ordinal, "action": "EXCLUDE", "reason": "Fuori dal mapping"}
	}
	member := batchDecisionCommand(offer, exclude(0), exclude(900))
	f.decide(t, a, member, true)
	other := analysis.IdentityScope(f.identity)
	other.Principal = "another-principal"
	intruder := revisionSessionForScope(t, f.worker.Service, other, decisionScopes)
	f.decide(t, intruder, batchDecisionCommand(offer, exclude(0)), true)
	duplicate := batchDecisionCommand(offer, exclude(0), nextstep.Object{"ordinal": 0, "action": "EXCLUDE", "reason": "Diversa"})
	if body := f.decide(t, a, duplicate, true); body["code"] != "INVALID_TOOL_ARGUMENTS" {
		t.Fatal("duplicate ordinal was accepted", body)
	}
	invalidKey := batchDecisionCommand(offer, exclude(0))
	invalidKey["commandKey"], invalidKey["expectedRevision"] = uuid.NewSHA1(uuid.NameSpaceOID, []byte("not v4")).String(), 1
	if body := f.decide(t, a, invalidKey, true); body["code"] == "ANALYSIS_REVISION_CONFLICT" {
		t.Fatal("CAS ran before command key validation", body)
	}
	f.restart(t)
	b := f.session(t)
	type response struct {
		result *mcp.CallToolResult
		err    error
	}
	ready, replies := make(chan struct{}), make(chan response, 2)
	for ordinal, session := range []*mcp.ClientSession{a, b} {
		go func() {
			<-ready
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
				Name: "source.analysis.decide", Arguments: batchDecisionCommand(offer, exclude(ordinal)),
			})
			replies <- response{result, err}
		}()
	}
	close(ready)
	commits := 0
	for range 2 {
		reply := <-replies
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		body := assertBatchWire(t, nextContract(t), reply.result)
		if !reply.result.IsError {
			commits++
		} else if body["code"] != "ANALYSIS_REVISION_CONFLICT" || body["next"].(nextstep.Object)["tool"] != "source.analysis.read" {
			t.Fatal("CAS loser could not reread the analysis", body)
		}
	}
	var receipts, choices int
	err := f.db.Pool().QueryRow(t.Context(), `select (select count(*) from ouf_mcp.analysis_batch_receipt),
		(select count(*) from ouf_mcp.analysis_batch_choice)`).Scan(&receipts, &choices)
	if commits != 1 || receipts != 1 || choices != 1 || err != nil {
		t.Fatal("invalid or competing commands changed the database", commits, receipts, choices, err)
	}
}

func TestBatchDecidePostgresSingleDispatchAfterLeaseTakeover(t *testing.T) {
	f := newBatchDecisionFixture(t, 1)
	session := f.session(t)
	offer := f.offer(t, session)
	f.decide(t, session, batchDecisionCommand(offer, batchDecisionAdopt(offer, 0)), false)
	assertSupersededBatchDispatch(t, f.db, f.dsn, f.worker, f.identity, f.id)
	if f.owner.calls["ouf.semantic.discovery.adopt"] != 1 || len(f.owner.receipts) != 1 {
		t.Fatal("expected one adoption dispatch and receipt after takeover", f.owner.calls, len(f.owner.receipts))
	}
}

type denyBatchReceiptRead struct {
	orchestration.AuthorizationPort
}

func (a denyBatchReceiptRead) Authorize(ctx context.Context, request orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if request.CapabilityID == "ouf.semantic.proposal.read" {
		return orchestration.AuthorizationDecision{DecisionCode: "NO_APPLICABLE_GRANT"}, nil
	}
	return a.AuthorizationPort.Authorize(ctx, request)
}

func TestBatchDecidePostgresPausedReceiptReconciliation(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		t.Run(fmt.Sprintf("revoked=%t", revoked), func(t *testing.T) {
			f := newBatchDecisionFixture(t, 1)
			session := f.session(t)
			offer := f.offer(t, session)
			f.decide(t, session, batchDecisionCommand(offer, batchDecisionProposal(0)), false)
			f.owner.lose, f.owner.readRetryAfter = "ouf.semantic.propose", "7200"
			batchWorkerDrain(t, f.worker, f.identity, f.id)
			now := time.Now().Add(time.Minute)
			f.worker.Now = func() time.Time { return now }
			batchWorkerDrain(t, f.worker, f.identity, f.id)
			var due time.Time
			err := f.db.Pool().QueryRow(t.Context(), `select due_at from ouf_mcp.analysis_batch_column where analysis_id=$1`, f.id).Scan(&due)
			if err != nil || due.Before(now.Add(110*time.Minute)) || f.owner.calls["ouf.semantic.proposal.read"] != 1 {
				t.Fatal("receipt retry did not honor Retry-After", due, f.owner.calls, err)
			}
			f.restart(t)
			now = now.Add(time.Hour)
			if batchWorkerStep(t, f.worker, f.identity, f.id) {
				t.Fatal("worker woke before the durable retry deadline")
			}
			if revoked {
				f.worker.Service.Auth = denyBatchReceiptRead{f.worker.Service.Auth}
			}
			now = due.Add(time.Second)
			batchWorkerDrain(t, f.worker, f.identity, f.id)
			var state, code string
			err = f.db.Pool().QueryRow(t.Context(), `select effect_state,effect_code from ouf_mcp.analysis_batch_choice where analysis_id=$1`, f.id).Scan(&state, &code)
			wantState, wantCode, reads := "APPLIED", "CREATED", 2
			if revoked {
				wantState, wantCode, reads = "HELD", "AUTHORIZATION_DENIED", 1
			}
			if err != nil || state != wantState || code != wantCode || f.owner.calls["ouf.semantic.propose"] != 1 ||
				f.owner.calls["ouf.semantic.proposal.read"] != reads {
				t.Fatal("incorrect recovery after pause", state, code, f.owner.calls, err)
			}
		})
	}
}

func TestBatchDecidePostgresRefusalAndAmbiguousDispatch(t *testing.T) {
	f := newBatchDecisionFixture(t, 3)
	session := f.session(t)
	offer := f.offer(t, session)
	accepted, expired, unknown := batchDecisionAdopt(offer, 0), batchDecisionAdopt(offer, 1), batchDecisionAdopt(offer, 2)
	f.owner.refuse, f.owner.absent = expired["candidateId"].(string), unknown["candidateId"].(string)
	command := batchDecisionCommand(offer, accepted, expired, unknown)
	receipt := f.decide(t, session, command, false)
	crashed := &failBatchCheckpoint{Store: f.db, stage: analysis.BatchAdopt}
	f.worker.Store = crashed
	if worked, err := f.worker.RunOnce(t.Context(), f.id, f.identity, time.Now().Add(time.Hour)); !worked || err == nil || crashed.claimed.Fence == "" {
		t.Fatal("failed to simulate interruption after dispatch", worked, err)
	}
	f.restart(t)
	now := crashed.claimed.LeaseUntil.Add(time.Second)
	f.worker.Now = func() time.Time { return now }
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	now = now.Add(time.Minute)
	batchWorkerDrain(t, f.worker, f.identity, f.id)
	if f.owner.calls["ouf.semantic.discovery.adopt"] != 3 || f.owner.calls["ouf.semantic.proposal.read"] != 2 || len(f.owner.receipts) != 1 {
		t.Fatal("worker resent a dispatched mutation", f.owner.calls, len(f.owner.receipts))
	}
	session = f.session(t)
	reopened := f.offer(t, session)
	items := reopened["items"].([]any)
	if len(items) != 1 {
		t.Fatal("the definitive refusal changed other columns", reopened)
	}
	item := items[0].(nextstep.Object)
	if fmt.Sprint(item["ordinal"]) != "1" || item["reasonCode"] != "CANDIDATE_EXPIRED" ||
		fmt.Sprint(item["candidateCount"]) != "0" || fmt.Sprint(item["allowed"]) != "[REFORMULATE EXCLUDE PROPOSE]" {
		t.Fatal("refusal did not reopen the column with updated choices", item)
	}
	if replay := f.decide(t, session, command, false); !reflect.DeepEqual(decisionStatuses(receipt), decisionStatuses(replay)) {
		t.Fatal("owner effects changed the original receipt", replay)
	}
	closeColumn := nextstep.Object{"ordinal": 1, "action": "EXCLUDE", "reason": "Candidato scaduto"}
	f.decide(t, session, batchDecisionCommand(reopened, closeColumn), false)
	held := f.offer(t, session)
	if held["phase"] != "BLOCKED" || held["next"].(nextstep.Object)["reasonCode"] != "OUTCOME_REQUIRES_RECONCILIATION" {
		t.Fatal("ambiguous owner effect was hidden", held)
	}
}

func TestBatchDecidePostgresNinthAttemptAfterRestart(t *testing.T) {
	f := newBatchDecisionFixture(t, 1)
	session := f.session(t)
	now := time.Now().UTC().Add(time.Second)
	f.worker.Now = func() time.Time { return now }
	for attempt := 1; attempt <= 8; attempt++ {
		if attempt == 8 {
			f.restart(t)
			session = f.session(t)
		}
		offer := f.offer(t, session)
		if items, _ := offer["items"].([]any); len(items) != 1 {
			t.Fatal("offer without the residual column at attempt", attempt, offer)
		}
		item := offer["items"].([]any)[0].(nextstep.Object)
		if fmt.Sprint(item["attempt"]) != fmt.Sprint(attempt) ||
			fmt.Sprint(item["remainingAttempts"]) != fmt.Sprint(8-attempt) {
			t.Fatal("attempt budget did not survive progression", offer)
		}
		command := batchDecisionCommand(offer, nextstep.Object{
			"ordinal": 0, "action": "REFORMULATE", "intent": fmt.Sprintf("residual new%d", attempt),
		})
		body := f.decide(t, session, command, false)
		results := decisionStatuses(body)
		if len(results) != 1 {
			t.Fatal(body)
		}
		if attempt < 8 {
			if results[0].Status != "RECORDED" {
				t.Fatal("valid reformulation lost its reserve", body)
			}
			batchWorkerDrain(t, f.worker, f.identity, f.id)
			now = now.Add(time.Minute)
			// The test clock moves a minute per round, but admission windows
			// follow the database clock: age them so each round gets its own (distinct shifts avoid key collisions).
			if _, err := f.db.Pool().Exec(t.Context(), `update ouf_mcp.budget_window
				set window_start=window_start-make_interval(mins=>$1::int), window_end=window_end-make_interval(mins=>$1::int)`, 1<<attempt); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if results[0].Status != "REJECTED" || results[0].ReasonCode != "REFORMULATION_BUDGET_EXHAUSTED" ||
			fmt.Sprint(body["revision"]) != fmt.Sprint(offer["expectedRevision"]) {
			t.Fatal("the ninth attempt was accepted or advanced the revision", body)
		}
		if replay := f.decide(t, session, command, false); !reflect.DeepEqual(decisionStatuses(replay), results) {
			t.Fatal("all-rejected receipt was not durable", replay)
		}
		residual := f.offer(t, session)
		if residual["batchId"] == offer["batchId"] || f.search.commandCalls != 7 {
			t.Fatal("budget rejection did not leave a fresh residual offer", residual, f.search.commandCalls)
		}
	}

	// A new analysis under a new binding still inherits the consumed budget
	// for the same profile and identity.
	ctx := t.Context()
	sum, _ := f.worker.Manifest.Checksum()
	scope := analysis.IdentityScope(f.identity)
	prev, err := f.db.ReadBatchStart(ctx, scope, f.id, sum)
	if err != nil {
		t.Fatal(err)
	}
	replan := analysis.BatchStart{Profile: prev.Profile, Epoch: "new-policy", Authority: prev.Authority}
	replan.ID = analysis.BatchAnalysisID(scope, replan.Profile, replan.Epoch)
	if err = f.db.SaveBatchPreviewPage(ctx, scope, replan.ID, sum, replan.Epoch, replan.Profile, 0, []analysis.BatchPreviewColumn{{
		Name: "residual_0", Intent: "residual 0", BatchColumnMetadata: analysis.BatchColumnMetadata{DataType: "string", Examples: []json.RawMessage{}},
	}}); err != nil {
		t.Fatal(err)
	}
	replan, err = f.db.SaveBatchStart(ctx, scope, sum, uuid.NewString(), analysis.Digest(replan), replan)
	if err != nil {
		t.Fatal(err)
	}
	seedBatchDecisionEvidence(t, f.db, replan.ID, 1)
	read, err := f.db.ReadBatch(ctx, scope, replan.ID, sum, func(offer analysis.BatchOffer) ([]analysis.BatchOfferItem, error) {
		return offer.Items, nil
	})
	if err != nil || len(read.Offer.Items) != 1 || read.Offer.Items[0].Item.RemainingAttempts != 0 {
		t.Fatal("a new policy binding reset the profile's budget", read, err)
	}
	cmd := analysis.BatchDecide{AnalysisID: replan.ID, BatchID: read.Offer.ID, CommandKey: uuid.NewString(),
		ExpectedRevision: read.Offer.Revision, Decisions: []analysis.BatchDecision{{Ordinal: 0, Action: "REFORMULATE", Intent: "residual again"}}}
	cmd.InputFingerprint = analysis.Digest(cmd)
	rejected, err := f.db.DecideBatch(ctx, scope, sum, replan, cmd)
	if err != nil || len(rejected.Decisions) != 1 || rejected.Decisions[0].ReasonCode != "REFORMULATION_BUDGET_EXHAUSTED" {
		t.Fatal("replan accepted a ninth logical search", rejected, err)
	}
}

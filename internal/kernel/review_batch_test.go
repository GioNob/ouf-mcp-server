package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestReviewBatchProblemThroughDecoration(t *testing.T) {
	s := newRailTestServer(t)
	contract := nextContract(t)
	ref, id := strings.Repeat("a", 64), uuid.NewString()
	for _, tool := range []string{"semantic.approval.request", "semantic.proposal.read"} {
		for _, tc := range []struct {
			code, reason string
			status       int
			tools        []string
		}{
			{"SEM_REVIEW_BATCH_ITEMS_NOT_READY", "VALIDATION_NOT_CURRENT", 409, []string{"semantic.review.prepare", "semantic.approval.request"}},
			{"SEM_REVIEW_BATCH_ITEMS_NOT_READY", "NOT_DRAFT", 409, []string{"semantic.approval.request"}},
			{"SEM_REVIEW_BATCH_ITEM_NOT_OWNED", "", 403, []string{"source.analysis.read"}},
		} {
			row := railObject{"reason": tc.reason, "revisionId": id, "rowVersion": 0, "comment": "private-marker"}
			body := railObject{"reviewBatchId": id, "reviewContextRef": ref, "detail": "private-marker", "code": "private-marker",
				"validationRunId": "private-marker", "expectedRowVersion": -1, "state": "private-marker", "artifactType": "private-marker",
				"items": []any{row, railObject{"reason": "private-marker", "revisionId": id, "rowVersion": 3}}}
			raw, _ := json.Marshal(body)
			p := &orchestration.Problem{Status: tc.status, Code: tc.code}
			projected := reviewBatchProblem(tool, p, raw)
			if projected == nil {
				t.Fatal("missing typed problem", p)
			}
			result := s.decorate(context.Background(), s.tools[tool], railObject{"commandKey": "receipt", "analysisId": ref}, projected)
			next := checkNext(t, contract, result)
			full, _ := json.Marshal(result)
			owner := ownerObject(result)
			if !result.IsError || strings.Contains(string(full), "private-marker") || owner["code"] != tc.code ||
				fmt.Sprint(owner["status"]) != fmt.Sprint(tc.status) || owner["expectedRowVersion"] != nil || next["actor"] != "AGENT" {
				t.Fatal(owner)
			}
			alts := next["alternatives"].([]any)
			if len(alts) != len(tc.tools) {
				t.Fatal(alts)
			}
			for i, target := range tc.tools {
				alt := alts[i].(railObject)
				args := alt["arguments"].(railObject)
				if alt["tool"] != target || !s.valid(target, args) || args["analysisId"] != ref {
					t.Fatal(alt)
				}
				switch target {
				case "semantic.review.prepare":
					if args["revisionId"] != id || args["expectedRowVersion"] != float64(0) {
						t.Fatal(args)
					}
				case "semantic.approval.request":
					if args["batchAction"] != "REMOVE" || args["item"].(railObject)["revisionId"] != id {
						t.Fatal(args)
					}
				}
			}
		}
	}
	p := &orchestration.Problem{Status: 403, Code: "SEM_HUMAN_REQUIRED"}
	if reviewBatchProblem("semantic.approval.request", p, nil) != nil || ownerDenialResult(p, "ouf.semantic.approval.request") == nil {
		t.Fatal("ordinary forbidden response bypassed permission handling")
	}
	p.Code = "SEM_REVIEW_BATCH_ITEM_NOT_OWNED"
	if reviewBatchProblem("semantic.review.prepare", p, nil) != nil {
		t.Fatal("batch projection outside its capabilities")
	}
}

func TestReviewBatchProblemFieldBounds(t *testing.T) {
	id, ref := uuid.NewString(), strings.Repeat("f", 64)
	fields := railObject{"reviewContextRef": ref, "reviewBatchId": id, "successorBatchId": id, "revisionId": id,
		"validationRunId": id, "artifactType": "VALUE_MAP", "state": "EXPIRED", "expectedRowVersion": int64(1<<53 - 1)}
	project := func(input railObject) railObject {
		raw, _ := json.Marshal(input)
		return ownerObject(reviewBatchProblem("semantic.approval.request", &orchestration.Problem{Status: 409, Code: "SEM_REVIEW_BATCH_ITEMS_NOT_READY"}, raw))
	}
	got := project(fields)
	for k, want := range fields {
		if fmt.Sprint(got[k]) != fmt.Sprint(want) {
			t.Fatal(k, got)
		}
	}
	for k, bad := range (railObject{"reviewContextRef": strings.ToUpper(ref), "reviewBatchId": strings.ReplaceAll(id, "-", ""),
		"successorBatchId": true, "revisionId": nil, "validationRunId": "arbitrary", "expectedRowVersion": int64(1 << 53), "items": make([]any, 101)}) {
		in := maps.Clone(fields)
		in[k] = bad
		if _, leaked := project(in)[k]; leaked {
			t.Fatal("invalid field projected", k)
		}
	}
	rows := make([]any, 100)
	for i := range rows {
		rows[i] = railObject{"revisionId": id, "reason": "ROW_VERSION_CHANGED", "rowVersion": i}
	}
	fields["items"] = rows
	if len(project(fields)["items"].([]any)) != 100 {
		t.Fatal("lost the last permitted diagnostic")
	}
}

func TestReviewBatchResultBudget(t *testing.T) {
	s := newRailTestServer(t)
	ctx := context.WithValue(context.Background(), identityKey{}, requestIdentity{Identity: orchestration.Identity{
		ServicePrincipalID: "service", PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture"}})
	id, ref := uuid.NewString(), strings.Repeat("a", 64)
	deadline := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	s.thsLinks = map[string]linkLookup{semanticLinkID: func(context.Context, railObject) nextstep.LinkResult {
		return nextstep.OwnerLink("https://example.invalid/review", deadline, time.Now())
	}}
	for _, tc := range []struct{ tool, action, state, itemState, allowed, count string }{
		{"semantic.approval.request", "ADD", "COLLECTING", "COLLECTED", "SUBMIT_REVIEW", "collected"},
		{"semantic.approval.request", "SUBMIT", "UNDER_REVIEW", "UNDER_REVIEW", "WAIT_HUMAN_DECISION", "underReview"},
		{"semantic.proposal.read", "", "DECIDED", "CHANGES_REQUESTED", "REVISE_ITEMS", "changesRequested"},
		{"ouf.wait", "", "DECIDED", "CHANGES_REQUESTED", "REVISE_ITEMS", "changesRequested"},
	} {
		for _, variant := range []string{"worst", "inflated"} {
			t.Run(tc.tool+tc.action+"/"+variant, func(t *testing.T) {
				reg := s.tools[tc.tool]
				counts := railObject{"collected": 0, "underReview": 0, "published": 0, "changesRequested": 0, "rejected": 0, "expired": 0, "notReady": 0}
				counts[tc.count] = 100
				// Contract rev. 3.1, §5.2: a page of ten rows, each with a 1024-character
				// IRI of query ampersands and a 500-character comment of ampersands.
				// Go writes every & as six bytes.
				rows := make([]any, nextstep.ReviewBatchPageItems)
				for i := range rows {
					semanticID := "https://x/?" + strings.Repeat("&", 1013)
					row := railObject{"revisionId": uuid.NewString(), "rowVersion": 1<<53 - 1, "currentValidationRunId": uuid.NewString(),
						"semanticId": semanticID, "artifactType": "RELATIONSHIP", "discoveryRequestId": uuid.NewString(), "itemState": tc.itemState}
					if tc.state != "COLLECTING" {
						row["position"] = 100
					}
					if tc.state == "DECIDED" {
						row["reviewFeedback"] = railObject{"decision": "CHANGES_REQUESTED", "comment": strings.Repeat("&", 500)}
						if i == 0 {
							delete(row, "reviewFeedback")
							row["itemState"] = "PUBLISHED"
							row["publication"] = railObject{"publicationSetId": id, "semanticVersion": "1.0.0-" + strings.Repeat("x", 122)}
							counts["published"], counts["changesRequested"] = 1, 99
						}
					}
					rows[i] = row
				}
				batch := railObject{"kind": "REVIEW_BATCH", "state": tc.state, "allowedActions": []string{tc.allowed}, "itemCount": 100, "maxItems": 100,
					"reviewBatchId": id, "reviewContextRef": ref, "itemsOffset": 0, "nextItemsOffset": nextstep.ReviewBatchPageItems, "counts": counts, "items": rows, "version": ref}
				if tc.state == "UNDER_REVIEW" {
					batch["currentChallenge"] = railObject{"challengeId": id, "challengeStatus": "OPEN", "expiresAt": deadline}
					batch["pollAfterSeconds"] = 5
				}
				wire, _ := json.Marshal(batch)
				if len(wire) > 100<<10 || reg.limit != 256<<10 {
					t.Fatalf("worst page %d bytes, registered budget %d", len(wire), reg.limit)
				}
				if variant == "inflated" {
					// Defence in depth: an owner page larger than the contract still
					// reaches the agent intact, with the fixed text in Content.
					batch["padding"] = strings.Repeat("&", 16<<10)
					wire, _ = json.Marshal(batch)
				}
				args := railObject{"reviewBatchId": id, "analysisId": ref}
				if tc.action != "" {
					args["batchAction"], args["commandKey"], args["reviewContextRef"] = tc.action, uuid.NewString(), ref
					if tc.action == "ADD" {
						delete(args, "reviewBatchId")
						row := rows[0].(railObject)
						args["item"] = railObject{"revisionId": row["revisionId"], "expectedRowVersion": 1, "validationRunId": row["currentValidationRunId"]}
					}
				}
				var result *mcp.CallToolResult
				if tc.tool == "ouf.wait" {
					read := s.tools["semantic.proposal.read"].capability
					next := s.engine(ctx).Evaluate(read, s.manifest.NextPolicies[read.NextPolicy], args, ownerObject(&mcp.CallToolResult{StructuredContent: batch}), false)
					result = s.boundedResult(waitResult(true, batch, next), reg)
				} else {
					result = s.decorate(ctx, reg, args, &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(wire)}}})
				}
				final, _ := json.Marshal(result)
				compact := result.Content[0].(*mcp.TextContent).Text == nextstep.ReviewBatchText
				if result.IsError || len(final) > reg.limit || compact != (variant == "inflated") {
					t.Fatal("unexpected budget projection", tc.tool, variant, len(final), compact)
				}
				next := checkNext(t, nextContract(t), result)
				got := ownerObject(result)
				if tc.tool == "ouf.wait" {
					got = got["observation"].(railObject)
				} else {
					delete(got, "next")
				}
				if actual, _ := json.Marshal(got); string(actual) != string(wire) || next["reasonCode"] == "OWNER_STATE_UNKNOWN" {
					t.Fatal("lost full observation or continuation")
				}
			})
		}
	}
}

func TestOutstandingSingleSemanticWaitUsesBatchChallenge(t *testing.T) {
	synctest.Test(t, resumeSingleSemanticWait)
}

func resumeSingleSemanticWait(t *testing.T) {
	s := newRailTestServer(t)
	ctx := context.WithValue(context.Background(), identityKey{}, requestIdentity{Identity: orchestration.Identity{
		ServicePrincipalID: "service", PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture"}})
	id, version := uuid.NewString(), strings.Repeat("b", 64)
	deadline := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	s.thsLinks = map[string]linkLookup{semanticLinkID: func(_ context.Context, args railObject) nextstep.LinkResult {
		if !reflect.DeepEqual(args, railObject{"challengeId": id}) {
			t.Fatal(args)
		}
		return nextstep.OwnerLink("https://example.invalid/review", deadline, time.Now())
	}}
	// Issue the predecessor's single-receipt reference before the owner
	// moves the current challenge to the batch. Resume the original token.
	const read = "semantic.proposal.read"
	input := railObject{"commandKey": "older-receipt", "analysisId": version}
	previous := railObject{"state": "UNDER_REVIEW", "version": strings.Repeat("a", 64),
		"currentChallenge": railObject{"challengeId": uuid.NewString(), "expiresAt": deadline}}
	rule := manifest.Rule{Action: "WAIT_FOR_HUMAN", Tool: read, Reason: "SEMANTIC_REVIEW", Decision: "APPROVE_OR_REJECT"}
	issued := s.waits.signer.Issue(waitBinding(ctx), rule, input, previous, nextstep.OwnerLink("https://example.invalid/previous", deadline, time.Now()), time.Now())
	token := issued["arguments"].(railObject)["waitRef"].(string)
	reg := s.tools[read]
	reg.run = func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args railObject
		if json.Unmarshal(req.Params.Arguments, &args) != nil || args["commandKey"] != input["commandKey"] || args["expectedVersion"] != previous["version"] {
			t.Fatal("changed predecessor read", args)
		}
		return &mcp.CallToolResult{StructuredContent: railObject{"state": "UNDER_REVIEW", "version": version, "allowedActions": []string{"WAIT_HUMAN_DECISION"},
			"currentChallenge": railObject{"challengeId": id, "expiresAt": deadline}}}, nil
	}
	s.tools[read] = reg
	req := &mcp.CallToolRequest{}
	if err := json.Unmarshal([]byte(`{"params":{"name":"ouf.wait","arguments":{}}}`), req); err != nil {
		t.Fatal(err)
	}
	result := s.wait(ctx, req, token)
	next := checkNext(t, nextContract(t), result)
	if next["action"] != "WAIT_FOR_HUMAN" || next["human"].(railObject)["decisionKind"] != "APPROVE_OR_REJECT" {
		t.Fatal(next)
	}
	ref, err := s.waits.signer.Open(next["arguments"].(railObject)["waitRef"].(string), waitBinding(ctx))
	if err != nil || !reflect.DeepEqual(ref.ReadArgs(), railObject{"analysisId": version, "expectedVersion": version, "commandKey": "older-receipt"}) {
		t.Fatal(ref, err)
	}
}

package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestBatchSummaryPackingCoversThousandOrdinals(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	sdk := mcp.NewServer(&mcp.Implementation{Name: "summary", Version: "1"}, nil)
	s, err := newWorkflowServer(sdk, snapshot, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range snapshot.ToolEligible() {
		if c.ToolName == "source.analysis.read" || c.ToolName == "semantic.proposal.read" {
			var input jsonschema.Schema
			if err := json.Unmarshal(c.InputSchema, &input); err != nil {
				t.Fatal(err)
			}
			s.register(&mcp.Tool{Name: c.ToolName, InputSchema: &input}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return nil, nil
			})
		}
	}
	const total = 1000
	id := strings.Repeat("f", 64)
	scope := analysis.Scope{Principal: "p", Tenant: "t", Service: "s"}
	binding := analysis.Digest([]any{analysis.BatchMode, "SUMMARY", id})
	now := time.Now()
	pos := analysis.Position{Seq: math.MaxInt64, Last: -1, Limit: 8, Snapshot: uuid.New(), Expires: now.Add(time.Minute).Unix()}
	head := analysis.BatchRead{Phase: "COMPLETE", Reason: "ANALYSIS_SUMMARY"}
	summary := analysis.Summary{Seq: 19, Onboarding: true, Counts: analysis.Counts{Total: total}}
	contract := nextContract(t)
	var token string
	for seen := 0; seen < total; {
		summary.Items = nil
		for ordinal := seen; ordinal < min(seen+pos.Limit, total); ordinal++ {
			raw, err := json.Marshal(nextstep.Object{"state": "EXCLUDED", "changedSeq": "19", "ordinal": ordinal,
				"action": "EXCLUDE", "reason": strings.Repeat("<>&\u2028\u2029", 80)})
			if err != nil {
				t.Fatal(err)
			}
			summary.Items = append(summary.Items, raw)
		}
		result, last, err := s.summaryPage(t.Context(), id, "SUMMARY", scope, pos, summary, 8192, &head)
		if err != nil {
			t.Fatal(err)
		}
		body := assertBatchWire(t, contract, s.finalizeBatch("source.analysis.read", result))
		items := body["items"].([]any)
		if len(items) < 1 || len(items) > 8 || last != seen+len(items)-1 {
			t.Fatal("summary page failed to advance", last, seen, len(items))
		}
		for _, item := range items {
			if fmt.Sprint(item.(nextstep.Object)["ordinal"]) != fmt.Sprint(seen) {
				t.Fatal("ordinal coverage", seen, item)
			}
			seen++
		}
		if seen < total {
			token = body["nextCursor"].(string)
			pos, err = analysis.OpenPosition(token, s.key, scope, binding, now)
			if err != nil || pos.Last != last || pos.Seq != summary.Seq {
				t.Fatal("invalid snapshot continuation", pos, err)
			}
		} else if body["nextCursor"] != nil || body["next"].(nextstep.Object)["tool"] != "semantic.proposal.read" {
			t.Fatal("last page did not open owner context", body)
		}
	}
	if _, err := analysis.OpenPosition(token, s.key, scope, binding, now.Add(time.Minute)); !errors.Is(err, analysis.ErrExpired) {
		t.Fatal("expired snapshot could be read", err)
	}
	other := scope
	other.Principal = "revoked"
	if _, err := analysis.OpenPosition(token, s.key, other, binding, now); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("snapshot scope accepted after identity change", err)
	}
	value := "urn:test:?" + strings.Repeat("&", 2038)
	raw, _ := json.Marshal(nextstep.Object{"state": "MATCHED_PUBLISHED", "changedSeq": "19", "ordinal": 0, "semanticId": value})
	summary.Counts.Total, summary.Items = 1, []json.RawMessage{raw}
	pos.Last, pos.Limit = -1, 8
	var recovered strings.Builder
	for range 10 {
		result, _, err := s.summaryPage(t.Context(), id, "SUMMARY", scope, pos, summary, 8192, &head)
		if err != nil {
			t.Fatal(err)
		}
		body := assertBatchWire(t, contract, s.finalizeBatch("source.analysis.read", result))
		row := body["items"].([]any)[0].(nextstep.Object)
		token, _ = row["semanticIdRef"].(string)
		if part, ok := row["semanticIdPart"].(string); ok {
			recovered.WriteString(part)
			token, _ = body["nextCursor"].(string)
		}
		if token == "" {
			break
		}
		pos, err = analysis.OpenPosition(token, s.key, scope, binding, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	if recovered.String() != value {
		t.Fatal("mapping identity was not retained across detail pages")
	}
}

func TestBatchReviewCompactSDK(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	sdk := mcp.NewServer(&mcp.Implementation{Name: "batch-review-budget", Version: "1"}, nil)
	s, err := newWorkflowServer(sdk, snapshot, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	s.waits.interval = time.Millisecond
	id, batch := strings.Repeat("a", 64), uuid.NewString()
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	link := "https://review.example/"
	link += strings.Repeat("x", 2000-len(link))
	s.thsLinks = map[string]linkLookup{semanticLinkID: func(_ context.Context, id nextstep.Object) nextstep.LinkResult {
		if id["challengeId"] != batch {
			return nextstep.LinkResult{}
		}
		return nextstep.LinkResult{Status: nextstep.LinkAvailable, URL: link, ExpiresAt: expiry}
	}}
	base := nextstep.Object{"analysisId": id, "reviewContextRef": id, "reviewBatchId": batch, "kind": "REVIEW_BATCH",
		"version": strings.Repeat("b", 64), "itemCount": 20, "itemsOffset": 0, "nextItemsOffset": 10, "collectionClosed": true}
	escaped := "<>&\u2028\u2029"
	row := nextstep.Object{"revisionId": uuid.NewString(), "rowVersion": 9007199254740991, "artifactType": "PROPERTY",
		"semanticId": "urn:test:?" + strings.Repeat("&", 1000), "currentValidationRunId": uuid.NewString(),
		"reviewFeedback": nextstep.Object{"decision": "CHANGES_REQUESTED", "comment": strings.Repeat(escaped, 100)},
		"publication":    nextstep.Object{"publicationSetId": uuid.NewString(), "semanticVersion": strings.Repeat(escaped, 25)},
	}
	items := make([]any, 10)
	for n := range items {
		item := maps.Clone(row)
		item["revisionId"], item["position"] = uuid.NewString(), n+1
		items[n] = item
	}
	base["items"] = items
	var owner nextstep.Object
	var failed bool
	for _, c := range snapshot.ToolEligible() {
		switch c.ToolName {
		case "ouf.wait":
			s.registerWait(c)
		case "source.analysis.read", "semantic.approval.request", "semantic.proposal.read", "semantic.review.prepare", "semantic.search", "source.onboarding.create":
			var input jsonschema.Schema
			if err := json.Unmarshal(c.InputSchema, &input); err != nil {
				t.Fatal(err)
			}
			s.register(&mcp.Tool{Name: c.ToolName, InputSchema: &input}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				var args nextstep.Object
				if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
					return nil, err
				}
				body, next := s.compactBatchReview(s.engine(ctx), c.ToolName, args, owner, failed)
				out := localBatchResult(body, next)
				out.IsError = failed
				return out, nil
			})
		}
	}
	identity := requestIdentity{Identity: orchestration.Identity{PrincipalID: "p", ServicePrincipalID: "s", TenantID: "t", ActorType: "HUMAN", Delegation: "test"}}
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return sdk }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, identity)))
	}))
	defer server.Close()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "reader", Version: "1"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	contextRead := nextstep.Object{"analysisId": id, "projection": analysis.BatchMode, "reviewContextRef": id}
	const read = "semantic.proposal.read"
	contract := nextContract(t)
	call := func(tool string, args nextstep.Object, want string) nextstep.Object {
		t.Helper()
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		body := assertBatchWire(t, contract, result)
		next := body["next"].(nextstep.Object)
		if result.IsError != failed || next["reasonCode"] != want {
			t.Fatalf("%s expected %s: %v", tool, want, next)
		}
		return body
	}
	for _, tc := range []struct{ state, count, allowed, reason string }{
		{"COLLECTING", "collected", "SUBMIT_REVIEW", nextstep.ReviewBatchCollecting},
		{"DECIDED", "changesRequested", "REVISE_ITEMS", nextstep.ReviewBatchChangesRequested},
		{"DECIDED", "published", "NONE", nextstep.ReviewBatchPublished},
		{"EXPIRED", "expired", "RESUBMIT_REVIEW", nextstep.ReviewBatchExpired},
		{"NONE", "", "NONE", nextstep.ReviewBatchEmpty},
		{"UNKNOWN", "", "NONE", "OWNER_STATE_UNKNOWN"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			owner = maps.Clone(base)
			owner["state"], owner["counts"], owner["allowedActions"] = tc.state, nextstep.Object{tc.count: 20}, []any{tc.allowed}
			body := call(read, contextRead, tc.reason)
			if tc.state == "DECIDED" && body["nextItemsOffset"] == nil {
				t.Fatal("compact page dropped the owner continuation")
			}
			if strings.Contains(body["next"].(nextstep.Object)["userMessage"].(string), escaped) {
				t.Fatal("owner feedback leaked into the instruction")
			}
		})
	}
	failed = true
	owner = nextstep.Object{"analysisId": id, "reviewBatchId": batch, "code": "SEM_REVIEW_BATCH_ITEMS_NOT_READY",
		"items": []any{nextstep.Object{"revisionId": row["revisionId"], "rowVersion": row["rowVersion"], "reason": "VALIDATION_NOT_CURRENT"}}}
	submit := nextstep.Object{"analysisId": id, "projection": analysis.BatchMode, "reviewContextRef": id,
		"reviewBatchId": batch, "batchAction": "SUBMIT", "commandKey": uuid.NewString()}
	call("semantic.approval.request", submit, nextstep.ReviewBatchItemsNotReady)
	owner["code"], owner["revisionId"], owner["validationRunId"], owner["expectedRowVersion"] =
		"SEM_REVIEW_BATCH_ITEM_TYPE_UNSUPPORTED", row["revisionId"], row["currentValidationRunId"], row["rowVersion"]
	call("semantic.approval.request", submit, nextstep.ReviewSingleRequired)
	failed = false
	owner = maps.Clone(base)
	owner["state"], owner["currentChallenge"] = "UNDER_REVIEW", nextstep.Object{"challengeId": batch, "challengeStatus": "OPEN", "expiresAt": expiry}
	start := call("semantic.approval.request", submit, nextstep.ReviewBatch)
	wait := start["next"].(nextstep.Object)
	if wait["action"] != "WAIT_FOR_HUMAN" || wait["human"].(nextstep.Object)["reviewUrl"] != link {
		t.Fatal("maximum-length THS link did not survive SDK framing", wait)
	}
	owner["state"], owner["counts"], owner["version"] = "DECIDED", nextstep.Object{"published": 20}, strings.Repeat("c", 64)
	call("ouf.wait", wait["arguments"].(nextstep.Object), nextstep.ReviewBatchPublished)
	owner["items"], owner["itemCount"], owner["counts"] = nil, 1, nextstep.Object{"published": 1}
	delete(owner, "nextItemsOffset")
	call("ouf.wait", wait["arguments"].(nextstep.Object), nextstep.SourceMappingRequired)
}

package kernel

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type refusedSummaryStore struct {
	analysisSDKStore
	analysis.SummaryStore
	attempts []time.Time
}

type expiringSummaryStore struct {
	refusedSummaryStore
	positions []analysis.Position
}

func (s *expiringSummaryStore) Reserve(ctx context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	if len(s.attempts) == 0 {
		return s.refusedSummaryStore.Reserve(ctx, in)
	}
	return s.statusAdmission.Reserve(ctx, in)
}

func (s *expiringSummaryStore) ReadSummary(_ context.Context, _ analysis.Scope, _, _ string, pos analysis.Position, _ bool) (analysis.Summary, error) {
	s.positions = append(s.positions, pos)
	return analysis.Summary{Seq: 1, Counts: analysis.Counts{Total: 2, Pending: 2},
		Items: []json.RawMessage{json.RawMessage(`{"ordinal":0,"changedSeq":"1","state":"PENDING"}`)}}, nil
}

func (*expiringSummaryStore) AuditSummary(context.Context, analysis.Scope, string, string, int64, int, int, uuid.UUID, *analysis.Hint) error {
	return nil
}

func TestWaitResumesExpiredSummaryFromFirstPage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, err := manifest.Load()
		if err != nil {
			t.Fatal(err)
		}
		key := make([]byte, 32)
		sdk := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
		s, err := newWorkflowServer(sdk, m, key)
		if err != nil {
			t.Fatal(err)
		}
		store := &expiringSummaryStore{}
		s.analysisService = &orchestration.Service{Auth: analysisSDKAuth{}, Admission: store, FingerprintKey: key}
		read, _ := analysis.Find(m, "source.analysis.read")
		registerAnalysisTool(s, read, m, s.analysisService)
		wait, _ := analysis.Find(m, "ouf.wait")
		s.registerWait(wait)
		i := orchestration.Identity{ActorType: "HUMAN", ServicePrincipalID: "mcp", PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture"}
		ctx := context.WithValue(t.Context(), identityKey{}, requestIdentity{Identity: i})
		contract := nextContract(t)
		call := func(tool string, args nextstep.Object) *mcp.CallToolResult {
			t.Helper()
			wire, _ := json.Marshal(nextstep.Object{"params": nextstep.Object{"name": tool, "arguments": args}})
			var req mcp.CallToolRequest
			if err := json.Unmarshal(wire, &req); err != nil {
				t.Fatal(err)
			}
			result, err := s.tools[tool].run(ctx, &req)
			if err != nil || result == nil {
				t.Fatal(result, err)
			}
			return s.validateNext(tool, result)
		}
		id := strings.Repeat("a", 64)
		position := analysis.Position{Seq: 1, Last: 0, Limit: 1, Expires: time.Now().Add(time.Second).Unix(), Snapshot: uuid.New()}
		cursor := position.Token(key, analysis.IdentityScope(i), id)
		refused := call(read.ToolName, nextstep.Object{"analysisId": id, "view": "SUMMARY", "cursor": cursor})
		next := checkNext(t, contract, refused)
		if !refused.IsError || next["action"] != "POLL" || next["tool"] != wait.ToolName {
			t.Fatal("missing admission wait", next)
		}
		expired := call(wait.ToolName, next["arguments"].(nextstep.Object))
		next = checkNext(t, contract, expired)
		args, _ := next["arguments"].(nextstep.Object)
		if !expired.IsError || ownerObject(expired)["code"] != analysis.ErrExpired.Error() || next["action"] != "CALL_TOOL" ||
			next["tool"] != read.ToolName || !maps.Equal(args, nextstep.Object{"analysisId": id, "view": "SUMMARY"}) {
			t.Fatal("lost the checked first-page continuation", next)
		}
		first := call(read.ToolName, args)
		body := ownerObject(first)
		items, _ := body["items"].([]any)
		if first.IsError || len(store.positions) != 1 || store.positions[0].Last != -1 || len(items) != 1 ||
			items[0].(nextstep.Object)["ordinal"] != json.Number("0") {
			t.Fatal("did not return the first summary page", body, store.positions)
		}
	})
}

func (s *refusedSummaryStore) Reserve(_ context.Context, _ orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	now := time.Now()
	s.attempts = append(s.attempts, now)
	delay := 3 * time.Second
	if len(s.attempts) > 1 {
		delay = 72 * time.Second
	}
	return orchestration.AdmissionDecision{}, orchestration.NewWindowBudgetError(nil, "ADMISSION_WINDOW_BUDGET_EXHAUSTED", "TOOL_CALLS", now, now.Add(delay))
}

func TestWaitRetriesRefusedSummaryRead(t *testing.T) {
	for _, view := range []string{"STATUS", "SUMMARY"} {
		t.Run(view, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m, err := manifest.Load()
				if err != nil {
					t.Fatal(err)
				}
				sdk := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
				s, err := newWorkflowServer(sdk, m, make([]byte, 32))
				if err != nil {
					t.Fatal(err)
				}
				store := &refusedSummaryStore{}
				s.analysisService = &orchestration.Service{Auth: analysisSDKAuth{}, Admission: store, FingerprintKey: make([]byte, 32)}
				c, _ := analysis.Find(m, "source.analysis.read")
				registerAnalysisTool(s, c, m, s.analysisService)
				w, _ := analysis.Find(m, "ouf.wait")
				s.registerWait(w)
				ctx := context.WithValue(context.Background(), identityKey{}, requestIdentity{Identity: orchestration.Identity{
					ActorType: "HUMAN", ServicePrincipalID: "mcp", PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture"}})
				contract := nextContract(t)
				call := func(tool string, args nextstep.Object) (*mcp.CallToolResult, nextstep.Object) {
					t.Helper()
					wire, _ := json.Marshal(nextstep.Object{"params": nextstep.Object{"name": tool, "arguments": args}})
					var req mcp.CallToolRequest
					if err := json.Unmarshal(wire, &req); err != nil {
						t.Fatal(err)
					}
					r, err := s.tools[tool].run(ctx, &req)
					if err != nil || !r.IsError {
						t.Fatal(r, err)
					}
					next := checkNext(t, contract, r)
					if next["action"] != "POLL" || next["tool"] != "ouf.wait" {
						t.Fatal("recoverable window lost its wait", next)
					}
					return r, next["arguments"].(nextstep.Object)
				}
				input := nextstep.Object{"analysisId": strings.Repeat("a", 64), "view": view}
				_, args := call(c.ToolName, input)
				result, args := call(w.ToolName, args)
				if len(store.attempts) != 2 || store.attempts[1].Sub(store.attempts[0]) != 3*time.Second {
					t.Fatal("unexpected admission attempts", store.attempts)
				}
				end := store.attempts[1].Add(72 * time.Second).UTC().Format(time.RFC3339Nano)
				body := ownerObject(result)
				ref, err := s.waits.signer.Open(args["waitRef"].(string), waitBinding(ctx))
				resume, _ := args["resumeArguments"].(nextstep.Object)
				if err != nil || !ref.RestoreArgs(resume) || ref.Read != c.ToolName || !maps.Equal(ref.Args, input) ||
					body["retryable"] != true || body["windowEndsAt"] != end || ref.NotBefore != end {
					t.Fatal("lost retry window or original selector", body, ref, err)
				}
			})
		})
	}
}

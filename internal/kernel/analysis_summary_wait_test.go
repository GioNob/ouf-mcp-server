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
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type refusedSummaryStore struct {
	analysisSDKStore
	analysis.SummaryStore
	attempts []time.Time
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

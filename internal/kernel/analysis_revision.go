package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *workflowServer) reviseAnalysis(ctx context.Context, svc *orchestration.Service, i orchestration.Identity, args nextstep.Object) (result *mcp.CallToolResult) {
	if !s.runConfig.Renewal.Configured() {
		return runFailure(analysis.ErrIssueUnavailable.Error(), 503)
	}
	store, ok := svc.Admission.(analysis.RenewalPlanner)
	runs, hasRun := svc.Admission.(analysis.RunStore)
	if !ok || !hasRun {
		return runFailure(analysis.ErrIssueUnavailable.Error(), 503)
	}
	c := analysis.Coordinator{Service: svc, Manifest: s.manifest}
	checksum, decision, err := c.SummaryAccess(ctx, i)
	if err != nil {
		return summaryFailure(err, "")
	}
	raw, err := json.Marshal(args)
	var v analysis.RenewalRevision
	if err != nil || json.Unmarshal(raw, &v) != nil {
		return summaryFailure(analysis.ErrInvalid, "")
	}
	r, err := runs.ReadRun(ctx, analysis.IdentityScope(i), i.Issuer, v.AnalysisID)
	if err != nil {
		return summaryFailure(err, v.AnalysisID)
	}
	if err = c.CheckPublishedScope(ctx, i, r.Policy); err != nil {
		return summaryFailure(err, v.AnalysisID)
	}
	finish, err := admitAnalysisCall(ctx, svc, i, checksum, decision, args, analysis.RenewalBodyMax, "ouf.source.analysis.renewal.propose", "COMMAND")
	if err != nil {
		return summaryFailure(err, v.AnalysisID)
	}
	p, err := store.ReviseRenewal(ctx, i, v)
	if errors.Is(err, analysis.ErrFenced) || errors.Is(err, analysis.ErrCommandConflict) {
		result = runFailure(analysis.SafeError(err), 409)
	} else if err != nil {
		result = summaryFailure(err, v.AnalysisID)
	} else {
		// A historical command receipt never advertises a superseded URL.
		next := nextstep.Object{"contract": nextstep.Contract, "action": "CALL_TOOL", "reasonCode": "ANALYSIS_OBSERVATION",
			"tool": "source.analysis.read", "arguments": nextstep.Object{"analysisId": p.AnalysisID, "view": "STATUS"},
			"userMessage": "Leggi lo stato della proposta aggiornata."}
		diff := map[string]any{}
		if p.Previous != nil {
			if !p.Expires.Equal(p.Previous.Expires) {
				diff["expiresAt"] = map[string]time.Time{"previous": p.Previous.Expires, "proposed": p.Expires}
			}
			oldValues := p.Previous.Increment.Dimensions()
			newValues := p.Increment.Dimensions()
			for dim, n := range newValues {
				if n != oldValues[dim] {
					diff[dim] = map[string]int64{"previous": oldValues[dim], "proposed": n}
				}
			}
		}
		body := nextstep.Object{"diff": diff, "proposalVersion": p.Version, "previous": p.Previous, "increment": p.Increment, "totalMaximum": p.Total,
			"requestedChanges": p.RequestedChanges, "next": next}
		wire, _ := json.Marshal(body)
		result = &mcp.CallToolResult{StructuredContent: body, Content: []mcp.Content{&mcp.TextContent{Text: string(wire)}}}
	}
	wire, _ := json.Marshal(result)
	end, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err = finish(end, len(wire), 1, !result.IsError); err != nil {
		return summaryFailure(err, v.AnalysisID)
	}
	return result
}

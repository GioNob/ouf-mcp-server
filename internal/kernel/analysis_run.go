package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func runFailure(code string, status int) *mcp.CallToolResult {
	result := workflowError(nextstep.Object{"code": code, "next": nextstep.ManualBlock(code, "Verifica le condizioni di avvio del piano di analisi.", "OPERATOR")})
	result.Meta = mcp.Meta{"ouf/httpStatus": status}
	return result
}

func (s *workflowServer) startRun(ctx context.Context, svc *orchestration.Service, i orchestration.Identity, args nextstep.Object) (result *mcp.CallToolResult) {
	store, _ := svc.Admission.(analysis.Store)
	c := analysis.Coordinator{Service: svc, Store: store, Manifest: s.manifest}
	sum, decision, err := c.SummaryAccess(ctx, i)
	if err != nil {
		return summaryFailure(err, "")
	}
	if !s.runConfig.Enabled {
		return runFailure("ANALYSIS_EXECUTOR_DISABLED", 503)
	}
	if !s.runConfig.Limits.OwnerDedupQualified {
		return runFailure("ANALYSIS_EXECUTOR_UNQUALIFIED", 503)
	}
	if store == nil || svc.AnalysisMandates == nil {
		return runFailure("ANALYSIS_EXECUTOR_UNAVAILABLE", 503)
	}
	asset, _ := args["assetId"].(string)
	profile, _ := args["profileId"].(string)
	key, _ := args["commandKey"].(string)
	c.Onboarding = key == s.commandKey(ctx, "source.analysis.plan", nextstep.Object{"mode": "ALL_COLUMNS", "assetId": asset, "profileId": profile}, nil, nil)
	finish, err := admitAnalysisCall(ctx, svc, i, sum, decision, nextstep.Object{"mode": "ALL_COLUMNS", "assetId": asset, "profileId": profile}, 8192, "ouf.source.analysis.plan", "COMMAND")
	if err != nil {
		if wait := windowAdmissionErrorResult(err); wait != nil {
			t := s.tools["source.analysis.plan"]
			return s.decorate(ctx, t, args, wait)
		}
		return summaryFailure(err, "")
	}
	defer func() {
		end, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		wire, _ := json.Marshal(result)
		if finish(end, len(wire), 1, !result.IsError) != nil {
			result = runFailure("ANALYSIS_STORE_UNAVAILABLE", 503)
		}
	}()
	r, err := c.StartRun(ctx, i, s.runConfig, asset, profile, key)
	if err != nil {
		switch {
		case errors.Is(err, analysis.ErrCommandConflict), errors.Is(err, analysis.ErrRunDrift), errors.Is(err, analysis.ErrFenced):
			return runFailure(err.Error(), 409)
		case errors.Is(err, analysis.ErrIssueUnavailable):
			return runFailure(err.Error(), 503)
		case errors.Is(err, analysis.ErrRunExhausted):
			return runFailure(err.Error(), 409)
		case errors.Is(err, analysis.ErrRunPressure):
			return runFailure("ANALYSIS_BACKOFF", 429)
		case errors.Is(err, analysis.ErrMandateExpired):
			return runFailure(err.Error(), 410)
		default:
			return summaryFailure(err, "")
		}
	}
	next := nextstep.RunNext(r, time.Now(), s.waits.interval)
	if r.State == "BLOCKED" && r.Reason == analysis.ErrRunExhausted.Error() && s.runConfig.Renewal.Configured() {
		if planner, ok := store.(analysis.RenewalPlanner); ok {
			p, e := planner.CurrentRenewal(ctx, r)
			if e != nil && !errors.Is(e, analysis.ErrScope) {
				return summaryFailure(e, r.ID())
			}
			if e == nil {
				next = s.engine(ctx).RenewalNext(p, s.runConfig.Renewal.ReviewBase)
			}
		}
	}
	state := r.State
	if next["reasonCode"] == "WAIT_DEADLINE_EXPIRED" {
		state = "BLOCKED"
	}
	body := nextstep.Object{"contract": analysis.RunContract, "analysisId": r.ID(), "profile": r.Profile, "state": state,
		"changeSeq": strconv.FormatInt(r.Seq, 10), "deadline": r.Deadline.UTC().Format(time.RFC3339Nano), "next": next}
	wire, _ := json.Marshal(body)
	return &mcp.CallToolResult{StructuredContent: body, Content: []mcp.Content{&mcp.TextContent{Text: string(wire)}}}
}

func (s *workflowServer) previewRun(ctx context.Context, args, obs, previous nextstep.Object) nextstep.Object {
	if !s.runConfig.Enabled || !s.runConfig.Limits.OwnerDedupQualified || obs["format"] != "CSV" {
		return previous
	}
	switch obs["workflowState"] {
	case nil, "PROFILE_AVAILABLE", "PROFILE_COLUMNS", "SUCCEEDED":
	default:
		return previous
	}
	asset, _ := obs["assetId"].(string)
	if asset == "" {
		asset, _ = args["assetId"].(string)
	}
	profile, _ := obs["profileId"].(string)
	if profile == "" && (obs["workflowState"] == "SUCCEEDED" || obs["status"] == "SUCCEEDED") {
		profile, _ = obs["resultRef"].(string)
	}
	if !analysis.CanonicalUUID(asset) || !analysis.CanonicalUUID(profile) {
		return previous
	}
	input := nextstep.Object{"assetId": asset, "profileId": profile, "mode": "ALL_COLUMNS"}
	input["commandKey"] = s.commandKey(ctx, "source.analysis.plan", input, nil, nil)
	if !s.valid("source.analysis.plan", input) {
		return previous
	}
	return nextstep.Object{"contract": nextstep.Contract, "action": "CALL_TOOL", "tool": "source.analysis.plan", "arguments": input,
		"reasonCode": "ANALYSIS_ALL_COLUMNS", "userMessage": "Avvia l’analisi delle colonne del profilo."}
}

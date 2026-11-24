package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func summaryFailure(err error, id string) *mcp.CallToolResult {
	code := analysis.SafeError(err)
	if code == "ANALYSIS_OPERATION_HELD" {
		code = "ANALYSIS_STORE_UNAVAILABLE"
	}
	next := nextstep.ManualBlock(code, "Verifica il riferimento e le condizioni di lettura dell’analisi.", "OPERATOR")
	if errors.Is(err, analysis.ErrExpired) {
		next = nextstep.Object{"contract": nextstep.Contract, "action": "CALL_TOOL", "reasonCode": code,
			"tool": "source.analysis.read", "arguments": nextstep.Object{"analysisId": id, "view": "SUMMARY"},
			"userMessage": "Il cursore è scaduto. Leggi il riepilogo corrente dalla prima pagina."}
	}
	if errors.Is(err, orchestration.ErrUnauthorized) {
		code = "AUTHORIZATION_DENIED"
		var denied *orchestration.AuthorizationDeniedError
		if errors.As(err, &denied) && denied.ReasonCode == "NO_APPLICABLE_GRANT" {
			code = denied.ReasonCode
			next = nextstep.PermissionDenied(denied.CapabilityID, code)
		} else {
			next = nextstep.ManualBlock(code, "Verifica il permesso corrente per la lettura.", "HUMAN_THS")
		}
	}
	result := workflowError(nextstep.Object{"code": code, "next": next})
	status := 503
	switch {
	case errors.Is(err, analysis.ErrInvalid), errors.Is(err, analysis.ErrCursor):
		status = 400
	case errors.Is(err, analysis.ErrScope):
		status = 404
	case errors.Is(err, analysis.ErrProfile), errors.Is(err, analysis.ErrConflict):
		status = 409
	case errors.Is(err, analysis.ErrExpired):
		status = 410
	case errors.Is(err, orchestration.ErrUnauthorized):
		status = 403
	}
	result.Meta = mcp.Meta{"ouf/httpStatus": status}
	return result
}

func (s *workflowServer) analysisSummary(ctx context.Context, c analysis.Coordinator, i orchestration.Identity, input nextstep.Object) (response *mcp.CallToolResult) {
	id, _ := input["analysisId"].(string)
	view, _ := input["view"].(string)
	checksum, decision, err := c.SummaryAccess(ctx, i)
	if err != nil {
		return summaryFailure(err, id)
	}
	store, ok := c.Store.(analysis.SummaryStore)
	if !ok {
		return summaryFailure(errors.New("missing summary store"), id)
	}
	limits := s.summaryLimits
	now := time.Now()
	scope := analysis.IdentityScope(i)
	pos := analysis.Position{Seq: math.MaxInt64, Last: -1, Limit: 32, Expires: now.Add(time.Duration(limits.SummaryCursorSeconds) * time.Second).Unix()}
	if n, ok := input["limit"].(float64); ok {
		pos.Limit = int(n)
	}
	if token, ok := input["cursor"].(string); ok {
		pos, err = analysis.OpenPosition(token, s.key, scope, id, now)
		if err != nil {
			return summaryFailure(err, id)
		}
	}
	finish, err := admitSummary(ctx, c.Service, i, checksum, decision, input, limits.SummaryPageBytes)
	if err != nil {
		if wait := windowAdmissionErrorResult(err); wait != nil {
			cap := s.tools["source.analysis.read"].capability
			obs := ownerObject(wait)
			next := s.engine(ctx).Evaluate(cap, s.manifest.NextPolicies[cap.NextPolicy], input, obs, true)
			if runs, ok := c.Store.(analysis.RunStore); ok && view == "STATUS" {
				r, e := runs.ReadRun(ctx, scope, i.Issuer, id)
				if e == nil && r.State == "RUNNING" {
					if e = c.CheckPublishedScope(ctx, i, r.Policy); e != nil {
						return summaryFailure(e, id)
					}
					var window *orchestration.WindowBudgetExhaustedError
					if errors.As(err, &window) && window.BudgetWindowEnd().After(r.NotBefore) {
						r.NotBefore = window.BudgetWindowEnd()
					}
					r.Reason = "ANALYSIS_BACKOFF"
					next = nextstep.RunNext(r, now, s.waits.interval)
				}
			}
			return workflowError(nextstep.Object{"code": obs["code"], "next": next})
		}
		return summaryFailure(err, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	defer func() {
		end, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer stop()
		wire, _ := json.Marshal(response)
		objects := 0
		if response != nil && !response.IsError {
			body, _ := response.StructuredContent.(nextstep.Object)
			items, _ := body["items"].([]json.RawMessage)
			objects = max(1, len(items))
		}
		if err := finish(end, len(wire), objects, response != nil && !response.IsError); err != nil {
			response = summaryFailure(err, id)
		}
	}()
	page := view == "SUMMARY"
	var result analysis.Summary
	runStore, hasRun := c.Store.(interface {
		RunSummary(context.Context, analysis.Scope, string, string, analysis.Position, bool) (analysis.Summary, error)
		AuditRunSummary(context.Context, analysis.RunRecord, string, int, int) error
		ClaimRunPoll(context.Context, analysis.RunRecord, time.Duration) (time.Time, bool, error)
	})
	if hasRun {
		result, err = runStore.RunSummary(ctx, scope, i.Issuer, id, pos, page)
	}
	if !hasRun || errors.Is(err, analysis.ErrScope) {
		result, err = store.ReadSummary(ctx, scope, id, checksum, pos, page)
	}
	if err != nil {
		return summaryFailure(err, id)
	}
	if err = c.EvidenceAccess(ctx, i, result); err != nil {
		return summaryFailure(err, id)
	}
	if result.Run != nil && result.Run.State == "BLOCKED" && result.Run.Reason == analysis.ErrRunExhausted.Error() && pos.Last < 0 {
		if planner, ok := c.Store.(analysis.RenewalPlanner); ok {
			p, e := planner.CurrentRenewal(ctx, *result.Run)
			if e != nil && !errors.Is(e, analysis.ErrScope) {
				return summaryFailure(e, id)
			}
			if e == nil {
				result.Renewal = &p
			}
		}
	}
	if !page && result.Run != nil && result.Run.State == "RUNNING" {
		at, allowed, err := runStore.ClaimRunPoll(ctx, *result.Run, s.waits.interval)
		if err != nil {
			return summaryFailure(err, id)
		}
		if !allowed {
			r := *result.Run
			if at.After(r.NotBefore) {
				r.NotBefore = at
			}
			r.Reason = "ANALYSIS_BACKOFF"
			next := nextstep.RunNext(r, time.Now(), s.waits.interval)
			out := workflowError(nextstep.Object{"code": "ANALYSIS_BACKOFF", "next": next})
			out.Meta = mcp.Meta{"ouf/httpStatus": 429}
			return out
		}
	}
	if page && pos.Last < 0 {
		pos.Snapshot = uuid.New()
	}
	out, last, err := s.summaryPage(ctx, id, view, scope, pos, result, limits.SummaryPageBytes)
	if err != nil {
		return summaryFailure(err, id)
	}
	first := -1
	if page {
		first = pos.Last + 1
	}
	var frozen *analysis.Hint
	if page && pos.Last < 0 && last+1 < result.Counts.Total {
		frozen = &result.Hint
	}
	if result.Run != nil {
		err = runStore.AuditRunSummary(ctx, *result.Run, decision, first, last)
	} else {
		err = store.AuditSummary(ctx, scope, id, decision, result.Seq, first, last, pos.Snapshot, frozen)
	}
	if err != nil {
		return summaryFailure(err, id)
	}
	return out
}

// Try the requested page first. Smaller, non-final pages all carry the same
// size cursor and continuation, so binary search finds the largest fitting prefix.
// summaryEnvelopeReserve leaves room for the framing the MCP server adds after the tool
// returns (_meta.serverInfo, resultType). The configured limit applies to the bytes on the wire.
const summaryEnvelopeReserve = 512

func (s *workflowServer) summaryPage(ctx context.Context, id, view string, scope analysis.Scope, pos analysis.Position, result analysis.Summary, maximum int) (*mcp.CallToolResult, int, error) {
	maximum -= summaryEnvelopeReserve
	engine := s.engine(ctx)
	pos.Seq = result.Seq
	build := func(n int) (*mcp.CallToolResult, int, error) {
		cursor := ""
		body := nextstep.Object{"analysisId": id, "contract": analysis.SummaryContract, "counts": result.Counts,
			"state": result.State(), "changeSeq": strconv.FormatInt(result.Seq, 10)}
		if view == "SUMMARY" {
			body["items"] = result.Items[:n]
			p := pos
			p.Last += n
			if p.Last+1 < result.Counts.Total {
				cursor = p.Token(s.key, scope, id)
				body["nextCursor"] = cursor
			}
		}
		body["next"] = engine.SummaryNext(id, view, cursor, result)
		if r := result.Run; r != nil && cursor == "" && r.State == "BLOCKED" && r.Reason == analysis.ErrRunExhausted.Error() {
			if result.Renewal != nil && result.Renewal.State != "CLOSED_PARTIAL" {
				body["next"] = nextstep.RunNext(*r, time.Now(), s.waits.interval)
			}
			if result.Renewal != nil && result.Renewal.State != "CLOSED_PARTIAL" && s.runConfig.Renewal.Configured() {
				body["next"] = engine.RenewalNext(*result.Renewal, s.runConfig.Renewal.ReviewBase)
			}
		}
		if r := result.Run; r != nil && cursor == "" && r.State == "RUNNING" {
			now := time.Now()
			body["next"] = nextstep.RunNext(*r, now, s.waits.interval)
			if !now.Before(r.Deadline) {
				body["state"] = "BLOCKED"
			}
		}
		wire, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		out := &mcp.CallToolResult{StructuredContent: body, Content: []mcp.Content{&mcp.TextContent{Text: string(wire)}}}
		envelope, err := json.Marshal(out)
		return out, len(envelope), err
	}
	n := len(result.Items)
	out, size, err := build(n)
	if err != nil || size <= maximum {
		return out, pos.Last + n, err
	}
	var best *mcp.CallToolResult
	last := -1
	for low, high := 1, n-1; low <= high; {
		mid := low + (high-low)/2
		candidate, size, err := build(mid)
		if err != nil {
			return nil, -1, err
		}
		if size <= maximum {
			best, last = candidate, pos.Last+mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	if best != nil {
		return best, last, nil
	}
	// A long but valid header can dominate the final next command.
	// Deliver a compact authorized owner read for that column instead.
	if result.Hint.Tool == "source.analysis.plan" && result.State() == "RUNNING" {
		args := nextstep.Object{"assetId": result.Hint.AssetID, "profileId": result.Hint.ProfileID, "view": "COLUMNS", "offset": result.Hint.Ordinal, "limit": 1}
		if s.valid("source.file.preview", args) {
			body := out.StructuredContent.(nextstep.Object)
			body["next"] = nextstep.Object{"contract": nextstep.Contract, "action": "CALL_TOOL", "tool": "source.file.preview", "arguments": args,
				"reasonCode": "ANALYSIS_COLUMN_PREVIEW", "userMessage": "Leggi i metadati della colonna da pianificare."}
			wire, _ := json.Marshal(body)
			out.Content = []mcp.Content{&mcp.TextContent{Text: string(wire)}}
			if raw, _ := json.Marshal(out); len(raw) <= maximum {
				return out, pos.Last + n, nil
			}
		}
	}
	return nil, -1, analysis.ErrCapacity
}

func analysisReadError(err error) *mcp.CallToolResult {
	if errors.Is(err, orchestration.ErrUnauthorized) {
		return governedCallErrorResult(err)
	}
	return errorResult(analysis.SafeError(err), false)
}

func recordAnalysisRead(ctx context.Context, service *orchestration.Service, snapshot *manifest.Snapshot, id, tool, request string, result *mcp.CallToolResult) error {
	if result == nil || result.IsError || len(result.Content) != 1 {
		return nil
	}
	body, ok := result.Content[0].(*mcp.TextContent)
	if !ok || ownerObject(result)["replay"] == true {
		return nil
	}
	store, ok := service.Admission.(analysis.SummaryStore)
	if !ok {
		return nil
	}
	identity, _ := ctx.Value(identityKey{}).(requestIdentity)
	c := analysis.Coordinator{Service: service, Manifest: snapshot}
	checksum, _, err := c.SummaryAccess(ctx, identity.Identity)
	if err != nil {
		return err
	}
	return store.ObserveDiscovery(ctx, analysis.IdentityScope(identity.Identity), id, checksum, tool, request, []byte(body.Text))
}

func summaryInput(tool string, args map[string]any) bool {
	_, view := args["view"]
	_, cursor := args["cursor"]
	return tool == "source.analysis.read" && (view || cursor)
}

func configuredSummary(snapshot *manifest.Snapshot) (analysis.Limits, error) {
	l, err := analysis.SummaryLimits(snapshot)
	if err != nil {
		return l, err
	}
	var preview, bytes, ttl error
	l.PreviewPageSize, preview = setting("OUF_ANALYSIS_PREVIEW_PAGE_SIZE", l.PreviewPageSize, 1, 128)
	l.SummaryPageBytes, bytes = setting("OUF_ANALYSIS_SUMMARY_PAGE_BYTES", l.SummaryPageBytes, 4096, 1<<20)
	l.SummaryCursorSeconds, ttl = setting("OUF_ANALYSIS_CURSOR_SECONDS", l.SummaryCursorSeconds, 1, 86400)
	return l, errors.Join(preview, bytes, ttl)
}

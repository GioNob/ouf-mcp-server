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

func (s *workflowServer) summaryFailure(ctx context.Context, err error, id string) *mcp.CallToolResult {
	code := analysis.SafeError(err)
	if code == "ANALYSIS_OPERATION_HELD" {
		code = "ANALYSIS_STORE_UNAVAILABLE"
	}
	next := nextstep.ManualBlock(code, "Verifica il riferimento e le condizioni di lettura dell’analisi.", "OPERATOR")
	if errors.Is(err, analysis.ErrEpochChanged) {
		next = (nextstep.Engine{}).ReplanNext(id, code)
	}
	if errors.Is(err, analysis.ErrExpired) {
		next = s.engine(ctx).ExpiredSummary(id)
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
	case errors.Is(err, analysis.ErrProfile), errors.Is(err, analysis.ErrConflict), errors.Is(err, analysis.ErrEpochChanged):
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
		return s.summaryFailure(ctx, err, id)
	}
	store, ok := c.Store.(analysis.SummaryStore)
	if !ok {
		return s.summaryFailure(ctx, errors.New("missing summary store"), id)
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
			return s.summaryFailure(ctx, err, id)
		}
	}
	finish, err := admitSummary(ctx, c.Service, i, checksum, decision, input, limits.SummaryPageBytes)
	if err != nil {
		if wait := windowAdmissionErrorResult(err); wait != nil {
			cap := s.tools["source.analysis.read"].capability
			obs := ownerObject(wait)
			obs["next"] = s.engine(ctx).Evaluate(cap, s.manifest.NextPolicies[cap.NextPolicy], input, obs, true)
			return workflowError(obs)
		}
		return s.summaryFailure(ctx, err, id)
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
			response = s.summaryFailure(ctx, err, id)
		}
	}()
	page := view == "SUMMARY"
	result, err := store.ReadSummary(analysis.WithOwnerObservationKey(ctx, c.Service.FingerprintKey), scope, id, checksum, pos, page)
	if err != nil {
		return s.summaryFailure(ctx, err, id)
	}
	if err = c.EvidenceAccess(ctx, i, result); err != nil {
		return s.summaryFailure(ctx, c.ScopeError(ctx, i, id, checksum, err), id)
	}
	if page && pos.Last < 0 {
		pos.Snapshot = uuid.New()
	}
	out, last, err := s.summaryPage(ctx, id, view, scope, pos, result, limits.SummaryPageBytes)
	if err != nil {
		return s.summaryFailure(ctx, err, id)
	}
	first := -1
	if page {
		first = pos.Last + 1
	}
	var frozen *analysis.Hint
	if page && pos.Last < 0 && last+1 < result.Counts.Total {
		frozen = &result.Hint
	}
	if err = store.AuditSummary(ctx, scope, id, decision, result.Seq, first, last, pos.Snapshot, frozen); err != nil {
		return s.summaryFailure(ctx, err, id)
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
		if next := engine.PreviewColumn(result.Hint); next != nil {
			body := out.StructuredContent.(nextstep.Object)
			body["next"] = next
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
	if result == nil || ownerObject(result)["replay"] == true {
		return nil
	}
	obs, observed := result.Meta["ouf/ownerObservation"].(orchestration.OwnerObservation)
	if !observed || obs.At.IsZero() {
		return nil // An admission refusal can carry an existing clock without new evidence.
	}
	store, ok := service.Admission.(analysis.SummaryStore)
	if !ok {
		return nil
	}
	identity, _ := ctx.Value(identityKey{}).(requestIdentity)
	c := analysis.Coordinator{Service: service, Manifest: snapshot}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	checksum, _, err := c.SummaryAccess(ctx, identity.Identity)
	if err != nil {
		return err
	}
	var evidence []byte
	if !result.IsError {
		if len(result.Content) != 1 {
			return analysis.ErrInvalid
		}
		body, ok := result.Content[0].(*mcp.TextContent)
		if !ok {
			return analysis.ErrInvalid
		}
		evidence = []byte(body.Text)
	}
	saved, err := store.RecordDiscovery(ctx, analysis.IdentityScope(identity.Identity), id, checksum, tool, request, evidence, obs)
	if err == nil {
		attachObservation(result, saved)
	}
	return err
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
	limit, err := setting("OUF_ANALYSIS_MAX_COLUMN_REFORMULATIONS", l.MaxColumnReformulations, 0, 16)
	l.MaxColumnReformulations = limit
	return l, errors.Join(preview, bytes, ttl, err)
}

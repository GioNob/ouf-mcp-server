package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const batchResultMeta = "ouf/internalBatchResult"
const batchNextMeta = "ouf/internalBatchNext"
const batchResultText = "Il risultato dell’analisi a lotti è in structuredContent."

type boundedBatchWire struct {
	*mcp.CallToolResult
	tool string
}

// The SDK adds response metadata after receiving middleware returns.
func (r boundedBatchWire) MarshalJSON() ([]byte, error) {
	if wire, err := r.CallToolResult.MarshalJSON(); err == nil && len(wire) <= analysis.BatchMaxResultBytes {
		return wire, nil
	}
	fallback := batchLimitResult(r.tool, r.CallToolResult)
	// Keep the SDK's completion framing when it fits with the bounded error.
	framed := *r.CallToolResult
	framed.Content, framed.StructuredContent, framed.IsError = fallback.Content, fallback.StructuredContent, true
	framed.InputRequests, framed.RequestState, framed.Meta = nil, "", nil
	if info, ok := r.Meta[mcp.MetaKeyServerInfo]; ok {
		framed.Meta = mcp.Meta{mcp.MetaKeyServerInfo: info}
	}
	if wire, err := framed.MarshalJSON(); err == nil && len(wire) <= analysis.BatchMaxResultBytes {
		return wire, nil
	}
	return fallback.MarshalJSON()
}

func workflowResultWire(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		result, err := next(ctx, method, req)
		out, ok := result.(*mcp.CallToolResult)
		if err != nil || !ok || out == nil {
			return result, err
		}
		if call, ok := req.(*mcp.CallToolRequest); ok && out.Meta[batchResultMeta] == true {
			delete(out.Meta, batchResultMeta)
			return boundedBatchWire{CallToolResult: out, tool: call.Params.Name}, nil
		}
		return result, err
	}
}

func batchInput(tool string, args nextstep.Object) bool {
	switch tool {
	case "source.file.preview", "source.analysis.plan":
		return args["mode"] == analysis.BatchMode
	case "source.analysis.decide":
		return true
	case "source.analysis.read":
		return args["view"] == "BATCH" || args["view"] == "CANDIDATES"
	case "semantic.approval.request", "semantic.proposal.read":
		return args["projection"] == analysis.BatchMode
	default:
		return false
	}
}

func legacyInputForms(forms []string) []string {
	var legacy []string
	for _, form := range forms {
		if strings.Contains(form, "mode=BATCH_V1") || strings.Contains(form, "view=BATCH") ||
			strings.Contains(form, "view=CANDIDATES") || strings.Contains(form, "projection=BATCH_V1") {
			continue
		}
		legacy = append(legacy, form)
	}
	return legacy
}

func uniqueDecisionOrdinals(args nextstep.Object) bool {
	decisions, _ := args["decisions"].([]any)
	seen := make(map[float64]bool, len(decisions))
	for _, decision := range decisions {
		item, _ := decision.(nextstep.Object)
		ordinal, ok := item["ordinal"].(float64)
		if !ok || seen[ordinal] {
			return false
		}
		seen[ordinal] = true
	}
	return true
}

func markBatchResult(result *mcp.CallToolResult) *mcp.CallToolResult {
	if result.Meta == nil {
		result.Meta = mcp.Meta{}
	}
	result.Meta[batchResultMeta] = true
	return result
}

func (s *workflowServer) finalizeBatch(tool string, result *mcp.CallToolResult) *mcp.CallToolResult {
	body, _ := result.StructuredContent.(nextstep.Object)
	id, _ := body["analysisId"].(string)
	result = s.validateNext(tool, result)
	if body, ok := result.StructuredContent.(nextstep.Object); ok && analysis.ValidateID(id) == nil {
		body["analysisId"] = id
	}
	if len(result.Meta) == 0 {
		result.Meta = nil
	}
	result.Content = []mcp.Content{&mcp.TextContent{Text: batchResultText}}
	if wire, err := json.Marshal(result); err == nil && len(wire) <= analysis.BatchMaxResultBytes {
		return result
	}
	return batchLimitResult(tool, result)
}

func batchLimitResult(tool string, result *mcp.CallToolResult) *mcp.CallToolResult {
	original, _ := result.StructuredContent.(nextstep.Object)
	id, _ := original["analysisId"].(string)
	const code = nextstep.BatchResultLimit
	body := nextstep.Object{"code": code, "retryable": false, "next": nextstep.BatchBlock(code)}
	if analysis.ValidateID(id) == nil {
		body["analysisId"] = id
	}
	if tool == "ouf.wait" {
		body["changed"], body["observation"] = false, nil
	}
	return &mcp.CallToolResult{IsError: true, StructuredContent: body, Content: []mcp.Content{&mcp.TextContent{Text: batchResultText}}}
}

func batchUnavailable(id string) *mcp.CallToolResult {
	body := nextstep.Object{"code": nextstep.BatchUnavailable, "retryable": false}
	if id != "" {
		body["analysisId"] = id
	}
	return markBatchResult(workflowError(body))
}

func (s *workflowServer) batchPreview(ctx context.Context, c manifest.Capability, checksum string, input nextstep.Object, service *orchestration.Service) (*mcp.CallToolResult, any, error) {
	const limit = 1
	asset, _ := input["assetId"].(string)
	profile, _ := input["profileId"].(string)
	raw, _ := json.Marshal(nextstep.Object{"assetId": asset, "profileId": profile, "view": "COLUMNS", "offset": 0, "limit": limit})
	result, _, err := invoke(ctx, c, checksum, raw, service, 1, 1<<20)
	if err != nil || result == nil || result.IsError {
		return result, nil, err
	}
	if len(result.Content) != 1 {
		return analysisReadError(analysis.ErrProfile), nil, nil
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		return analysisReadError(analysis.ErrProfile), nil, nil
	}
	pin, err := analysis.BatchPreviewProfile([]byte(content.Text), asset, profile, limit)
	if err != nil {
		return analysisReadError(err), nil, nil
	}
	// Only owner-verified immutable profile metadata crosses the MCP boundary.
	result.Content = nil
	result.StructuredContent = pin
	return result, nil, nil
}

func batchStartForRead(ctx context.Context, c analysis.Coordinator, i orchestration.Identity, id string) (analysis.BatchStart, bool, error) {
	store, ok := c.Store.(analysis.BatchStore)
	if !ok || analysis.ValidateID(id) != nil {
		return analysis.BatchStart{}, false, nil
	}
	sum, err := c.Manifest.Checksum()
	if err != nil {
		return analysis.BatchStart{}, false, err
	}
	start, err := store.ReadBatchStart(ctx, analysis.IdentityScope(i), id, sum)
	if errors.Is(err, analysis.ErrScope) {
		return analysis.BatchStart{}, false, nil
	}
	if err != nil {
		return analysis.BatchStart{}, false, err
	}
	epoch, err := c.Epoch(ctx, i)
	if err == nil && epoch != start.Epoch {
		err = analysis.ErrEpochChanged
	}
	if err == nil && start.Authority != "" && start.Authority != analysis.BatchAuthorityBinding(i) {
		err = analysis.ErrScope
	}
	return start, true, err
}

func (s *workflowServer) batchReadSelector(ctx context.Context, tool string, args nextstep.Object) bool {
	if batchInput(tool, args) {
		return true
	}
	id, _ := args["analysisId"].(string)
	i, _ := ctx.Value(identityKey{}).(requestIdentity)
	if tool != "source.analysis.read" || s.analysisService == nil || analysis.ValidateID(id) != nil {
		return false
	}
	store, ok := s.analysisService.Admission.(analysis.BatchStore)
	if !ok {
		return false
	}
	sum, err := s.manifest.Checksum()
	if err != nil {
		return false
	}
	_, err = store.ReadBatchStart(ctx, analysis.IdentityScope(i.Identity), id, sum)
	return err == nil
}

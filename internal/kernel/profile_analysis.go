package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type planWorkInput struct {
	analysis.Work
	Hint struct{ OriginalName string } `json:"intentHint"`
}

func registerAnalysisTool(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var schema jsonschema.Schema
	if e := json.Unmarshal(c.InputSchema, &schema); e != nil {
		panic(e)
	}
	tool := &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &schema, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: c.ToolName == "source.analysis.read"}}
	if len(c.OutputSchema) > 0 {
		var output jsonschema.Schema
		if err := json.Unmarshal(c.OutputSchema, &output); err != nil {
			panic(err)
		}
		tool.OutputSchema = &output
	}
	addWorkflowTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		i, ok := ctx.Value(identityKey{}).(requestIdentity)
		if !ok || i.Delegation == "" || i.ActorType != "HUMAN" {
			if summaryInput(c.ToolName, input) && !batchInput(c.ToolName, input) {
				return server.summaryFailure(ctx, orchestration.ErrUnauthorized, ""), nil, nil
			}
			return actorOrDelegationDenial(i, ok), nil, nil
		}
		_, railCommand := input["commandKey"]
		key, _ := input["commandKey"].(string)
		replan, _ := input["replanOf"].(string)
		if railCommand {
			delete(input, "commandKey")
			if c.ToolName == "source.analysis.step" && key != server.commandKey(ctx, c.ToolName, input, nil, nil) {
				return errorResult("IDEMPOTENCY_KEY_REUSE", false), nil, nil
			}
		}
		store, ok := service.Admission.(analysis.Store)
		if !ok {
			if summaryInput(c.ToolName, input) {
				return server.summaryFailure(ctx, errors.New("missing analysis store"), ""), nil, nil
			}
			return errorResult("ANALYSIS_STORE_NOT_BOUND", false), nil, nil
		}
		coordinator := analysis.Coordinator{Store: store, Service: service, Manifest: snapshot, Onboarding: railCommand, PreviewPageSize: server.summaryLimits.PreviewPageSize, MaxColumnReformulations: &server.summaryLimits.MaxColumnReformulations}
		raw, _ := json.Marshal(input)
		var out any
		var err error
		var successor *analysis.TaskResume
		var successorUnavailable bool
		switch c.ToolName {
		case "source.analysis.plan":
			if input["mode"] == analysis.BatchMode {
				asset, _ := input["assetId"].(string)
				profile, _ := input["profileId"].(string)
				hash, _ := input["schemaHash"].(string)
				start, startErr := coordinator.StartBatch(ctx, i.Identity, asset, profile, hash, key)
				if startErr == nil {
					return batchStatus(start), nil, nil
				}
				err = startErr
				break
			}
			if _, retry := input["requestId"]; retry && replan != "" {
				var r analysis.Reformulation
				if json.Unmarshal(raw, &r) != nil {
					return errorResult("ANALYSIS_INPUT_INVALID", false), nil, nil
				}
				out, err = coordinator.Reformulate(ctx, i.Identity, replan, key, r, server.summaryLimits.MaxColumnReformulations)
				break
			}
			if replan != "" {
				out, err = coordinator.Replan(ctx, i.Identity, replan, key)
				break
			}
			var args struct {
				AssetID          string          `json:"assetId"`
				ProfileID        string          `json:"profileId"`
				ExpectedRevision int64           `json:"expectedRevision"`
				Work             []planWorkInput `json:"work"`
			}
			if json.Unmarshal(raw, &args) != nil {
				return errorResult("ANALYSIS_INPUT_INVALID", false), nil, nil
			}
			work := make([]analysis.Work, len(args.Work))
			for n, item := range args.Work {
				work[n] = item.Work
				work[n].PreviewName = item.Hint.OriginalName
			}
			coordinator.CommandKey = key
			out, err = coordinator.Plan(ctx, i.Identity, args.AssetID, args.ProfileID, args.ExpectedRevision, work)
		case "source.analysis.read":
			id, _ := input["analysisId"].(string)
			start, batch, batchErr := batchStartForRead(ctx, coordinator, i.Identity, id)
			if batch || batchInput(c.ToolName, input) {
				if batchErr != nil {
					return markBatchResult(analysisReadError(batchErr)), nil, nil
				}
				if !batch {
					return analysisReadError(analysis.ErrScope), nil, nil
				}
				if input["offset"] != nil || input["limit"] != nil {
					return markBatchResult(analysisReadError(analysis.ErrInvalid)), nil, nil
				}
				if len(input) == 1 || input["view"] == "STATUS" {
					return batchStatus(start), nil, nil
				}
				return batchUnavailable(start.ID), nil, nil
			}
			if batchErr != nil {
				if summaryInput(c.ToolName, input) {
					return server.summaryFailure(ctx, batchErr, id), nil, nil
				}
				err = batchErr
				break
			}
			if input["view"] != nil {
				return server.analysisSummary(ctx, coordinator, i.Identity, input), nil, nil
			}
			var args struct {
				AnalysisID string `json:"analysisId"`
				Offset     int    `json:"offset"`
				Limit      int    `json:"limit"`
			}
			if json.Unmarshal(raw, &args) != nil {
				return errorResult("ANALYSIS_INPUT_INVALID", false), nil, nil
			}
			if args.Limit == 0 {
				args.Limit = 128
			}
			out, err = coordinator.Read(ctx, i.Identity, args.AnalysisID, args.Offset, args.Limit)
		case "source.analysis.decide":
			id, _ := input["analysisId"].(string)
			start, batch, batchErr := batchStartForRead(ctx, coordinator, i.Identity, id)
			if batchErr != nil {
				return analysisReadError(batchErr), nil, nil
			}
			if !batch {
				return analysisReadError(analysis.ErrScope), nil, nil
			}
			return batchUnavailable(start.ID), nil, nil
		case "source.analysis.step":
			var args struct {
				AnalysisID string `json:"analysisId"`
				TaskID     string `json:"taskId"`
			}
			if json.Unmarshal(raw, &args) != nil {
				return errorResult("ANALYSIS_INPUT_INVALID", false), nil, nil
			}
			var task analysis.Task
			task, err = coordinator.Step(ctx, i.Identity, args.AnalysisID, args.TaskID)
			successor = task.Successor
			successorUnavailable = task.SuccessorUnavailable
			if task.Code == analysis.InvalidOwner {
				result := errorResult(analysis.InvalidOwner, false)
				result.Meta = mcp.Meta{"ouf/httpStatus": 502}
				return result, nil, nil
			}
			task.Epoch, task.OwnerIntent, task.DiscoveryText = "", "", nil
			if task.Visibility != "" {
				task.Visibility, task.OwnerKey, task.Groups, task.Arguments = "", "", nil, nil
				if task.Tool == analysis.BatchTool {
					task.Result = nil
				}
			}
			out = task
			if task.State == "WAITING" && task.Code == "ADMISSION_WINDOW_WAIT" {
				out = struct {
					analysis.Task
					WindowEndsAt      time.Time `json:"windowEndsAt"`
					RetryAfterSeconds int64     `json:"retryAfterSeconds"`
				}{task, task.NotBefore, max(0, int64((time.Until(task.NotBefore)+time.Second-1)/time.Second))}
			}
		default:
			return errorResult("ANALYSIS_INPUT_INVALID", false), nil, nil
		}
		if err != nil {
			var epoch *analysis.PlanEpochError
			if errors.As(err, &epoch) {
				return workflowError(map[string]any{"code": epoch.Error(), "analysisId": epoch.AnalysisID, "retryable": false}), nil, nil
			}
			var budget *analysis.ReformulationBudgetError
			if errors.As(err, &budget) {
				body := map[string]any{"code": budget.Error(), "analysisId": budget.AnalysisID, "retryable": false}
				if budget.RequestID != "" {
					body["requestId"] = budget.RequestID
				}
				return workflowError(body), nil, nil
			}
			if errors.Is(err, orchestration.ErrUnauthorized) {
				return governedCallErrorResult(err), nil, nil
			}
			var denied *analysis.OwnerDenial
			if errors.As(err, &denied) {
				return ownerDenialResult(&orchestration.Problem{Status: 403, Code: denied.ReasonCode, PolicyVersion: denied.PolicyVersion}, denied.CapabilityID), nil, nil
			}
			if wait := windowAdmissionErrorResult(err); wait != nil {
				return wait, nil, nil
			}
			return errorResult(analysis.SafeError(err), false), nil, nil
		}
		body, err := json.Marshal(out)
		if err != nil || len(body) > 1<<20 {
			return errorResult("ANALYSIS_RESULT_LIMIT", false), nil, nil
		}
		result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}
		if successorUnavailable {
			result.Meta = mcp.Meta{"ouf/analysisSuccessorUnavailable": true}
		} else if successor != nil {
			result.Meta = mcp.Meta{"ouf/analysisSuccessor": successor}
		}
		return result, nil, nil
	})
}

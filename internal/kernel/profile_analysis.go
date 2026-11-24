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
			if summaryInput(c.ToolName, input) || (c.ToolName == "source.analysis.plan" && input["mode"] == "ALL_COLUMNS") {
				return summaryFailure(orchestration.ErrUnauthorized, ""), nil, nil
			}
			return actorOrDelegationDenial(i, ok), nil, nil
		}
		if c.ToolName == "source.analysis.plan" && input["mode"] == "ALL_COLUMNS" {
			return server.startRun(ctx, service, i.Identity, input), nil, nil
		}
		_, railCommand := input["commandKey"]
		if key, supplied := input["commandKey"]; supplied {
			delete(input, "commandKey")
			if key != server.commandKey(ctx, c.ToolName, input, nil, nil) {
				return errorResult("IDEMPOTENCY_KEY_REUSE", false), nil, nil
			}
		}
		store, ok := service.Admission.(analysis.Store)
		if !ok {
			if summaryInput(c.ToolName, input) {
				return summaryFailure(errors.New("missing analysis store"), ""), nil, nil
			}
			return errorResult("ANALYSIS_STORE_NOT_BOUND", false), nil, nil
		}
		coordinator := analysis.Coordinator{Store: store, Service: service, Manifest: snapshot, Onboarding: railCommand, PreviewPageSize: server.summaryLimits.PreviewPageSize}
		raw, _ := json.Marshal(input)
		var out any
		var err error
		switch c.ToolName {
		case "source.analysis.plan":
			var args struct {
				AssetID          string          `json:"assetId"`
				ProfileID        string          `json:"profileId"`
				ExpectedRevision int64           `json:"expectedRevision"`
				Work             []analysis.Work `json:"work"`
			}
			if json.Unmarshal(raw, &args) != nil {
				return errorResult("ANALYSIS_INPUT_INVALID", false), nil, nil
			}
			out, err = coordinator.Plan(ctx, i.Identity, args.AssetID, args.ProfileID, args.ExpectedRevision, args.Work)
		case "source.analysis.read":
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
			if task.Code == analysis.InvalidOwner {
				result := errorResult(analysis.InvalidOwner, false)
				result.Meta = mcp.Meta{"ouf/httpStatus": 502}
				return result, nil, nil
			}
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
			if errors.Is(err, orchestration.ErrUnauthorized) {
				return governedCallErrorResult(err), nil, nil
			}
			var denied *analysis.OwnerDenial
			if errors.As(err, &denied) {
				return ownerDenialResult(&orchestration.Problem{Status: 403, Code: denied.ReasonCode}, denied.CapabilityID), nil, nil
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
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}, nil, nil
	})
}

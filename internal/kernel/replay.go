package kernel

import (
	"encoding/json"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A replay is an observation of the original attempt after fresh authorization.
// Supply selectors for the next governed read, never a cached owner payload.
func replayResult(c manifest.Capability, args []byte, r orchestration.Result) *mcp.CallToolResult {
	body := map[string]any{"replay": true, "attemptId": r.AttemptID, "attemptState": r.AttemptState}
	failed := true
	switch r.AttemptState {
	case "ADMITTED", "RUNNING":
		body["code"] = "ATTEMPT_IN_PROGRESS"
		body["retryWithSameKey"] = true
	case "FAILED":
		body["code"] = "ATTEMPT_FAILED"
	case "SUCCEEDED":
		var selectors struct{ AssetID, SourceID, HandoffID, CommandKey string }
		_ = json.Unmarshal(args, &selectors)
		switch c.ToolName {
		case "semantic.review.prepare", "semantic.approval.request":
			body["readTool"] = "semantic.proposal.read"
			body["readArguments"] = map[string]string{"commandKey": selectors.CommandKey}
		case "source.file.profile":
			if r.ResultID != nil {
				body["readTool"] = "source.file.preview"
				body["readArguments"] = map[string]any{"assetId": selectors.AssetID, "jobId": r.ResultID}
			} else {
				body["readTool"] = "source.file.preview"
				body["readArguments"] = map[string]string{"view": "ASSETS"}
			}
		case "source.onboarding.create":
			body["readTool"] = "source.onboarding.read"
			if r.ResultID != nil {
				body["readArguments"] = map[string]any{"sourceId": selectors.SourceID, "onboardingVersionId": r.ResultID}
			} else {
				body["readArguments"] = map[string]string{"sourceId": selectors.SourceID, "view": "LATEST"}
			}
		case "semantic.discovery.request":
			if r.ResultID != nil {
				body["readTool"] = "semantic.discovery.status"
				body["readArguments"] = map[string]any{"requestId": r.ResultID}
			}
		case "source.file.upload":
			if selectors.HandoffID != "" {
				body["readTool"] = "source.file.upload.status"
				body["readArguments"] = map[string]string{"handoffId": selectors.HandoffID}
				body["useOriginalArgumentsWithFreshReadKey"] = true
			} else if r.ResultID != nil {
				body["assetId"] = r.ResultID
			} else {
				body["readTool"] = "source.file.preview"
				body["readArguments"] = map[string]string{"view": "ASSETS"}
			}
		}
		if c.CapabilityID == orchestration.HandoffIssueCapability && r.ResultID != nil {
			body["readTool"] = "source.file.upload.status"
			body["readArguments"] = map[string]any{"handoffId": r.ResultID}
		}
		// The proposal ID is retained for scoped batch draft lookup. The
		// ordinary proposal replay still requires a fresh owner receipt read.
		if r.ResultID != nil && c.ToolName != "semantic.propose" {
			body["resultRef"] = r.ResultID
			failed = false
		} else {
			body["code"] = "OWNER_RESULT_REFERENCE_UNAVAILABLE"
			if c.OperationClass == "READ" || c.OperationClass == "SEARCH" {
				body["readTool"] = c.ToolName
				body["useOriginalArgumentsWithFreshReadKey"] = true
			}
		}
	default:
		body["code"] = "ATTEMPT_STATE_UNAVAILABLE"
	}
	// Preserve established receipt recovery codes and the operational paging contract.
	switch {
	case isSourcePreparationCapability(c.CapabilityID):
		body["code"], body["retryable"] = "SOURCE_REVIEW_READ_REQUIRED", false
	case isSemanticProposalCapability(c.CapabilityID):
		body["code"], body["retryable"] = "SEMANTIC_RECEIPT_READ_REQUIRED", false
	case orchestration.IsOperationalControlRead(c.CapabilityID, c.OperationClass):
		body["code"], body["retryable"] = "OPERATIONAL_READ_REQUIRES_FRESH_AUTHORIZED_ATTEMPT", true
	}
	raw, _ := json.Marshal(body)
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}, IsError: failed}
}

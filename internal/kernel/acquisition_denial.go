package kernel

import (
	"encoding/json"
	"errors"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This is the current caller's failed admission result, not a private audit
// read. No principal, token, grant, resource, bundle name, decisionRef or raw
// error is copied into the projection. A policy version is not an authority.
type acquisitionDenial struct {
	Code                   string `json:"code"`
	Retryable              bool   `json:"retryable"`
	AuthorizationPhase     string `json:"authorizationPhase"`
	ReasonCode             string `json:"reasonCode"`
	EvaluatedPolicyVersion int64  `json:"evaluatedPolicyVersion,omitempty"`
}

func acquisitionErrorResult(err error) *mcp.CallToolResult {
	var denied *orchestration.AuthorizationDeniedError
	if !errors.As(err, &denied) {
		return errorResult(err.Error(), false)
	}
	reason := "AUTHORIZATION_DENIED"
	switch denied.ReasonCode {
	case "DELEGATION_REQUIRED", "TENANT_MISMATCH", "CAPABILITY_NOT_DECLARED",
		"ACTOR_NOT_ALLOWED", "SCOPE_MISSING", "DATA_LABEL_REQUIRED", "HUMAN_REQUIRED",
		"EXPLICIT_DENY", "NO_APPLICABLE_GRANT":
		reason = denied.ReasonCode
	}
	version := denied.PolicyVersion
	if version < 0 {
		version = 0
	}
	value := acquisitionDenial{Code: orchestration.ErrUnauthorized.Error(), Retryable: false,
		AuthorizationPhase: "MCP_PRE_ADMISSION", ReasonCode: reason, EvaluatedPolicyVersion: version}
	raw, _ := json.Marshal(value)
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}
}

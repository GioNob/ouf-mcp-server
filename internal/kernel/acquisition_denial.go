package kernel

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"syscall"

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
	RequiredCapability     string `json:"requiredCapability,omitempty"`
}

func governedCallErrorResult(err error) *mcp.CallToolResult {
	if result := windowAdmissionErrorResult(err); result != nil {
		return result
	}
	if transientNetworkError(err) {
		return errorResult("OWNER_NETWORK_INTERRUPTED", true)
	}
	var denied *orchestration.AuthorizationDeniedError
	if !errors.As(err, &denied) {
		if !errors.Is(err, orchestration.ErrUnauthorized) {
			return errorResult(err.Error(), false)
		}
		denied = &orchestration.AuthorizationDeniedError{}
	}
	reason := orchestration.SafeDenialReason(denied.ReasonCode, "AUTHORIZATION_DENIED")
	version := denied.PolicyVersion
	if version < 0 {
		version = 0
	}
	value := acquisitionDenial{Code: orchestration.ErrUnauthorized.Error(), Retryable: false,
		AuthorizationPhase: "MCP_PRE_ADMISSION", ReasonCode: reason, EvaluatedPolicyVersion: version}
	if reason == "NO_APPLICABLE_GRANT" {
		value.RequiredCapability = denied.CapabilityID
	}
	raw, _ := json.Marshal(value)
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}
}

func transientNetworkError(err error) bool {
	var timedOut net.Error
	return (errors.As(err, &timedOut) && timedOut.Timeout()) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED)
}

func ownerDenialResult(p *orchestration.Problem, capability string) *mcp.CallToolResult {
	if p == nil || p.Status != 403 {
		return nil
	}
	reason := orchestration.SafeDenialReason(p.Code, "OWNER_AUTHORIZATION_DENIED")
	if p.Code == "SEM_CAPABILITY_REQUIRED:ouf.semantic.discovery.adopt" {
		reason = p.Code
	}
	value := acquisitionDenial{Code: "NOT_AUTHORIZED", ReasonCode: reason, AuthorizationPhase: "OWNER"}
	if reason == "NO_APPLICABLE_GRANT" {
		value.RequiredCapability = capability
	}
	raw, _ := json.Marshal(value)
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}
}

func actorOrDelegationDenial(i requestIdentity, present bool) *mcp.CallToolResult {
	if !present {
		return errorResult("UNAUTHENTICATED", false)
	}
	reason := "DELEGATION_REQUIRED"
	if i.Delegation != "" {
		reason = "ACTOR_NOT_ALLOWED"
	}
	return governedCallErrorResult(&orchestration.AuthorizationDeniedError{ReasonCode: reason})
}

package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/contracts"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func callDiscoveryAdoption(ctx context.Context, service *orchestration.Service, in orchestration.Invocation) (out *mcp.CallToolResult) {
	result, err := service.Call(ctx, in)
	defer func() { attachObservation(out, result.Observation) }()
	switch {
	case errors.Is(err, orchestration.ErrIdempotencyOutcomeUnknown) || (err != nil && result.AttemptID != uuid.Nil):
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	case result.Replay:
		if result.AttemptState == "FAILED" && orchestration.RecordedAdoptionRefusal(result.AdoptionRefusalCode) != "" {
			return workflowError(map[string]any{"code": result.AdoptionRefusalCode, "recordedRefusal": true, "retryable": false})
		}
		if result.Observation.Due.After(time.Now()) || result.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
			return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
		}
		return readDiscoveryAdoptionReceipt(ctx, service, in)
	case errors.Is(err, orchestration.ErrIdempotencyConflict):
		return workflowError(map[string]any{"code": "IDEMPOTENCY_KEY_REUSE", "status": 409, "retryable": false})
	case errors.Is(err, orchestration.ErrUnauthorized):
		return governedCallErrorResult(err)
	case errors.Is(err, orchestration.ErrResultLimit):
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	case err != nil:
		if wait := windowAdmissionErrorResult(err); wait != nil {
			return wait
		}
		return errorResult("SEMANTIC_ADOPTION_UNAVAILABLE", false)
	case result.Problem != nil:
		if denial := ownerDenialResult(result.Problem, in.AuthorizationCapabilityID); denial != nil {
			return denial
		}
		return workflowError(map[string]any{"code": result.Problem.Code, "status": result.Problem.Status, "retryable": false})
	}

	var receipt map[string]any
	schema, schemaErr := contracts.DiscoveryAdoptionReceiptSchema(false)
	if schemaErr != nil {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	if json.Unmarshal(result.Body, &receipt) == nil && schema.Validate(receipt) == nil {
		return boundAdoptionReceipt(in.Arguments, receipt, result.Body)
	}
	// A successful command can return a current adoption reference or just
	// an acknowledgement. Only the owner can supply the historical result.
	if result.Observation.Due.After(time.Now()) || result.Observation.Reason == "RETRY_WINDOW_UNAVAILABLE" {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	return readDiscoveryAdoptionReceipt(ctx, service, in)
}

func readDiscoveryAdoptionReceipt(ctx context.Context, service *orchestration.Service, in orchestration.Invocation) (out *mcp.CallToolResult) {
	receiptRead, ok := adoptionReceiptRead()
	if !ok {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	read := in
	read.CapabilityID = receiptRead.CapabilityID
	read.AuthorizationCapabilityID = receiptRead.RequiredAuthorization
	read.GatewayBindingRef = receiptRead.GatewayBindingRef
	read.OperationClass = receiptRead.OperationClass
	read.IdempotencyKey = uuid.NewString()
	read.Arguments, _ = json.Marshal(map[string]string{"commandKey": in.IdempotencyKey})
	saved, err := service.SavedObservation(ctx, read.Identity, read.CapabilityID, read.OperationClass, read.Arguments)
	if err != nil {
		return errorResult("OWNER_CLOCK_UNAVAILABLE", false)
	}
	if saved.Due.After(time.Now()) {
		out = errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
		attachObservation(out, saved)
		return out
	}
	lookup, err := service.Call(ctx, read)
	defer func() { attachObservation(out, lookup.Observation) }()
	if errors.Is(err, orchestration.ErrUnauthorized) {
		return governedCallErrorResult(err)
	}
	if wait := windowAdmissionErrorResult(err); wait != nil {
		return wait
	}
	if denial := ownerDenialResult(lookup.Problem, read.AuthorizationCapabilityID); denial != nil {
		return denial
	}
	var snapshot struct {
		Operation                 string
		Result                    json.RawMessage
		HistoricalSnapshot        bool
		OwnerStateMustBeRechecked bool
	}
	if err != nil || lookup.Problem != nil || json.Unmarshal(lookup.Body, &snapshot) != nil ||
		snapshot.Operation != "ADOPT" || !snapshot.HistoricalSnapshot || !snapshot.OwnerStateMustBeRechecked {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	var receipt map[string]any
	schema, schemaErr := contracts.DiscoveryAdoptionReceiptSchema(true)
	if schemaErr != nil || json.Unmarshal(snapshot.Result, &receipt) != nil || schema.Validate(receipt) != nil {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	return boundAdoptionReceipt(in.Arguments, receipt, snapshot.Result)
}

func boundAdoptionReceipt(arguments []byte, receipt map[string]any, body []byte) *mcp.CallToolResult {
	var args struct{ RequestID, CandidateID string }
	if receipt["status"] == "ACTIVE" && receipt["publicationSetId"] == nil {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	if json.Unmarshal(arguments, &args) != nil || !strings.EqualFold(receipt["requestId"].(string), args.RequestID) ||
		!strings.EqualFold(receipt["candidateId"].(string), args.CandidateID) {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}
}

// Check the same proposal authorization context as invoke, without reserving
// admission or issuing a command. An unavailable decision omits the option.
func (s *workflowServer) canProposeSemantic(ctx context.Context) bool {
	t, ok := s.tools["semantic.propose"]
	i, identified := ctx.Value(identityKey{}).(requestIdentity)
	if !ok || !identified || s.analysisService == nil || s.analysisService.Auth == nil || len(waitBinding(ctx)) == 0 {
		return false
	}
	c := t.capability
	if (len(c.AllowedActors) > 0 && !slices.Contains(c.AllowedActors, i.ActorType)) ||
		(c.RequiredScope != "" && !slices.Contains(i.Scopes, c.RequiredScope)) {
		return false
	}
	id, resource, err := semanticProposalAuthorization(c, []byte("{}"), i.TenantID)
	if err != nil {
		return false
	}
	d, err := s.analysisService.Auth.Authorize(ctx, orchestration.AuthorizationRequest{
		Identity: i.Identity, CapabilityID: id, Resource: resource, Owner: c.Owner, OperationClass: c.OperationClass,
	})
	return err == nil && d.Allowed && d.DecisionRef != ""
}

var adoptionReceiptRead = sync.OnceValues(func() (manifest.Capability, bool) {
	snapshot, err := manifest.Load()
	if err == nil {
		for _, c := range snapshot.ToolEligible() {
			if c.ToolName == "semantic.proposal.read" && c.OperationClass == "READ" && c.Owner == "semantic" {
				return c, true
			}
		}
	}
	return manifest.Capability{}, false
})

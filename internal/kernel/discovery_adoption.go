package kernel

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Owner contract from ouf-semantic-registry PR #49, commit
// 8c0b1a23228d6ad0d99f16283f69d448c36b2560.
//
//go:embed discovery-adoption-v1.schema.json
var adoptionContract []byte

var adoptionReceiptSchema = sync.OnceValues(func() (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(adoptionContract, &schema); err != nil {
		return nil, err
	}
	schema.OneOf = nil
	schema.Ref = "#/$defs/receipt"
	return schema.Resolve(nil)
})

func callDiscoveryAdoption(ctx context.Context, service *orchestration.Service, in orchestration.Invocation) *mcp.CallToolResult {
	result, err := service.Call(ctx, in)
	body := result.Body
	switch {
	case result.Replay || errors.Is(err, orchestration.ErrIdempotencyOutcomeUnknown) || (err != nil && result.AttemptID != uuid.Nil):
		// Reauthorize and reserve one ordinary proposal read under the same
		// delegation. Its argument identifies the original owner command.
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
		lookup, readErr := service.Call(ctx, read)
		if errors.Is(readErr, orchestration.ErrUnauthorized) || (lookup.Problem != nil && lookup.Problem.Status == 403) {
			return errorResult("NOT_AUTHORIZED", false)
		}
		var snapshot struct {
			Operation                 string
			Result                    json.RawMessage
			HistoricalSnapshot        bool
			OwnerStateMustBeRechecked bool
		}
		if readErr != nil || lookup.Problem != nil || json.Unmarshal(lookup.Body, &snapshot) != nil ||
			snapshot.Operation != "ADOPT" || !snapshot.HistoricalSnapshot || !snapshot.OwnerStateMustBeRechecked {
			return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
		}
		body = snapshot.Result
	case errors.Is(err, orchestration.ErrIdempotencyConflict):
		return errorResult("IDEMPOTENCY_KEY_REUSE", false)
	case errors.Is(err, orchestration.ErrUnauthorized):
		return errorResult("NOT_AUTHORIZED", false)
	case errors.Is(err, orchestration.ErrResultLimit):
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	case err != nil:
		return errorResult("SEMANTIC_ADOPTION_UNAVAILABLE", false)
	case result.Problem != nil:
		return errorResult(result.Problem.Code, false)
	}

	var args map[string]string
	var receipt map[string]any
	schema, schemaErr := adoptionReceiptSchema()
	if schemaErr != nil || json.Unmarshal(in.Arguments, &args) != nil || json.Unmarshal(body, &receipt) != nil || schema.Validate(receipt) != nil {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	if !strings.EqualFold(receipt["requestId"].(string), args["requestId"]) || !strings.EqualFold(receipt["candidateId"].(string), args["candidateId"]) {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}
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

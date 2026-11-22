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
	return resolveAdoptionReceiptSchema(false)
})

var recordedAdoptionReceiptSchema = sync.OnceValues(func() (*jsonschema.Resolved, error) {
	return resolveAdoptionReceiptSchema(true)
})

func resolveAdoptionReceiptSchema(recorded bool) (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(adoptionContract, &schema); err != nil {
		return nil, err
	}
	schema.OneOf = nil
	schema.Ref = "#/$defs/receipt"
	// Semantic can attest the companion ontology alongside the candidate RDF.
	provenance := schema.Defs["provenance"]
	provenance.Properties["ontologySha256"] = &jsonschema.Schema{Ref: "#/$defs/provenance/properties/rdfSha256"}
	receipt := schema.Defs["receipt"]
	receipt.Properties["status"] = &jsonschema.Schema{Enum: []any{"DRAFT", "UNDER_REVIEW", "ACTIVE"}}
	receipt.Properties["publicationSetId"] = &jsonschema.Schema{Ref: "#/$defs/uuid"}
	if recorded {
		// proposal.read is separately authorized to disclose the complete
		// stored result, including newer adoption states and owner metadata.
		receipt.AdditionalProperties = nil
		provenance.AdditionalProperties = nil
		receipt.Properties["receiptSemantics"] = &jsonschema.Schema{Enum: []any{"DURABLE_COMMAND_RESULT_NOT_CURRENT_STATE", "CURRENT_ADOPTION_REFERENCE"}}
	}
	return schema.Resolve(nil)
}

func callDiscoveryAdoption(ctx context.Context, service *orchestration.Service, in orchestration.Invocation) *mcp.CallToolResult {
	result, err := service.Call(ctx, in)
	switch {
	case result.Replay || errors.Is(err, orchestration.ErrIdempotencyOutcomeUnknown) || (err != nil && result.AttemptID != uuid.Nil):
		return readDiscoveryAdoptionReceipt(ctx, service, in)
	case errors.Is(err, orchestration.ErrIdempotencyConflict):
		return errorResult("IDEMPOTENCY_KEY_REUSE", false)
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
		return errorResult(result.Problem.Code, false)
	}

	var receipt map[string]any
	schema, schemaErr := adoptionReceiptSchema()
	if schemaErr != nil {
		return errorResult("SEMANTIC_RECEIPT_READ_REQUIRED", false)
	}
	if json.Unmarshal(result.Body, &receipt) == nil && schema.Validate(receipt) == nil {
		return boundAdoptionReceipt(in.Arguments, receipt, result.Body)
	}
	// A successful command can return a current adoption reference or just
	// an acknowledgement. Only the owner can supply the historical result.
	return readDiscoveryAdoptionReceipt(ctx, service, in)
}

func readDiscoveryAdoptionReceipt(ctx context.Context, service *orchestration.Service, in orchestration.Invocation) *mcp.CallToolResult {
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
	lookup, err := service.Call(ctx, read)
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
	schema, schemaErr := recordedAdoptionReceiptSchema()
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

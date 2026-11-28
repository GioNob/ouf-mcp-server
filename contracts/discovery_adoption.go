package contracts

import (
	_ "embed"
	"encoding/json"
	"sync"

	"github.com/google/jsonschema-go/jsonschema"
)

// Owner contract from ouf-semantic-registry PR #49, commit
// 8c0b1a23228d6ad0d99f16283f69d448c36b2560.
//
//go:embed discovery-adoption-v1.schema.json
var discoveryAdoptionContract string

func DiscoveryAdoptionContract() string { return discoveryAdoptionContract }

var adoptionReceiptSchema = sync.OnceValues(func() (*jsonschema.Resolved, error) {
	return resolveAdoptionReceiptSchema(false)
})

var recordedAdoptionReceiptSchema = sync.OnceValues(func() (*jsonschema.Resolved, error) {
	return resolveAdoptionReceiptSchema(true)
})

func DiscoveryAdoptionReceiptSchema(recorded bool) (*jsonschema.Resolved, error) {
	if recorded {
		return recordedAdoptionReceiptSchema()
	}
	return adoptionReceiptSchema()
}

func resolveAdoptionReceiptSchema(recorded bool) (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	if err := json.Unmarshal([]byte(discoveryAdoptionContract), &schema); err != nil {
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

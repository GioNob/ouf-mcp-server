package manifest

import (
	"encoding/json"
	"testing"
)

func TestR4aObjectSearchCandidateIsClosedAndNotAdvertised(t *testing.T) {
	snapshot, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	var candidate *Capability
	for i := range snapshot.Capabilities {
		if snapshot.Capabilities[i].CapabilityID == "urban.object.search" {
			candidate = &snapshot.Capabilities[i]
		}
	}
	if candidate == nil {
		t.Fatal("missing object search candidate")
	}
	if candidate.PublicationState != "INACTIVE" || !candidate.ToolEligible ||
		candidate.Owner != "udp" || candidate.OperationClass != "SEARCH" ||
		candidate.RequiredAuthorization != "urban.object.search" ||
		candidate.GatewayBindingRef != "capability://urban.object.search" {
		t.Fatal("unexpected object search governance binding")
	}
	for _, active := range snapshot.ToolEligible() {
		if active.CapabilityID == candidate.CapabilityID {
			t.Fatal("candidate was advertised")
		}
	}
	var schema struct {
		AdditionalProperties bool                       `json:"additionalProperties"`
		Required             []string                   `json:"required"`
		Properties           map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(candidate.InputSchema, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.AdditionalProperties || len(schema.Required) != 1 || schema.Required[0] != "type" ||
		len(schema.Properties) != 3 {
		t.Fatal("object search schema is not bounded to type, pageSize and cursor")
	}
	for _, key := range []string{"type", "pageSize", "cursor"} {
		if _, ok := schema.Properties[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
}

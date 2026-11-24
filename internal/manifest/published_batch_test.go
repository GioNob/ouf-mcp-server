package manifest

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestPublishedBatchGatewaySnapshot(t *testing.T) { // MCP-50T-01
	raw, err := os.ReadFile("testdata/semantic-search-batch-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if input, err := os.ReadFile("../../.codex-input/semantic-search-batch-v1.json"); err == nil {
		if !bytes.Equal(raw, input) {
			t.Fatal("task-51m snapshot drift")
		}
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var want Capability
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range m.Capabilities {
		if c.CapabilityID != want.CapabilityID {
			continue
		}
		var ceiling, expected struct{ HardMaxResultBytes int64 }
		if json.Unmarshal(c.BaseCostEnvelope, &ceiling) != nil || json.Unmarshal(want.BaseCostEnvelope, &expected) != nil || ceiling != expected || c.MCPClass != "MCP_INTERNAL" {
			t.Fatal("internal batch envelope drift")
		}
		var a, b any
		if json.Unmarshal(c.InputSchema, &a) != nil || json.Unmarshal(want.InputSchema, &b) != nil || !reflect.DeepEqual(a, b) ||
			c.Owner != want.Owner || c.RequiredAuthorization != want.RequiredAuthorization || c.ToolEligible ||
			c.GatewayBindingRef != want.GatewayBindingRef || c.OperationClass != want.OperationClass || !reflect.DeepEqual(c.AllowedActors, want.AllowedActors) {
			t.Fatal("batch dispatch contract differs from Gateway")
		}
		return
	}
	t.Fatal("missing published batch")
}

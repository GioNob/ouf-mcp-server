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

func TestAnalysisManifestCompatibilityIsPinnedPerPublicationState(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range m.Capabilities {
		if c.ToolName != "semantic.search.batch" {
			continue
		}
		for _, tc := range []struct{ state, predecessor string }{
			{"ACTIVE", "0c624454825ebc571690961be309c38d27d78e96ab64e7903ea99c359c4f8daa"},
			{"INACTIVE", "42cdc29936d851ef039488b4ba3488be3aadcb4e03b45906e4de9ae22b424e2f"},
			{"ABSENT", "e7ee7a2c2ed3030133a96f20a7c416bd158c38cc9775cb1b59503842c2f008ab"},
		} {
			if tc.state == "ABSENT" {
				m.Capabilities = append(m.Capabilities[:i], m.Capabilities[i+1:]...)
			} else {
				m.Capabilities[i].PublicationState = tc.state
			}
			sum, err := m.Checksum()
			want := []string{sum, tc.predecessor, "01d00a53be5614115924b4775e0d098938ebae2e8e713ef8dd5b4c772e520c24"}
			if err != nil || !reflect.DeepEqual(AnalysisManifests(sum), want) {
				t.Fatal(tc.state, sum, err)
			}
		}
		if got := AnalysisManifests("unknown"); !reflect.DeepEqual(got, []string{"unknown"}) {
			t.Fatal(got)
		}
		return
	}
	t.Fatal("batch descriptor missing")
}

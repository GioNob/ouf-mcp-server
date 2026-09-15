package manifest

import (
	"encoding/json"
	"testing"
)

func TestCanonicalManifestIsCompleteAndClosed(t *testing.T) {
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	tools := s.ToolEligible()
	if len(tools) != 1 || tools[0].ToolName != "urban.object.related_search" {
		t.Fatalf("unexpected tool registry: %#v", tools)
	}
}

func TestTrustedHumanCapabilityCannotBecomeTool(t *testing.T) {
	s := validTestSnapshot()
	s.Capabilities[0].MCPClass = "TRUSTED_HUMAN_ONLY"
	if err := s.Validate(); err == nil {
		t.Fatal("expected fail-closed classification error")
	}
}

func TestArbitraryQuerySurfaceIsRejected(t *testing.T) {
	s := validTestSnapshot()
	s.Capabilities[0].InputSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"sql":{"type":"string"}}}`)
	if err := s.Validate(); err == nil {
		t.Fatal("expected forbidden query surface error")
	}
}

func validTestSnapshot() *Snapshot {
	return &Snapshot{Version: "1", Capabilities: []Capability{{
		ToolName: "test.safe", CapabilityID: "test.safe", Purpose: "test", UseWhen: []string{"x"}, DoNotUseWhen: []string{"y"},
		PreferredAlternatives: []string{"safe"}, OperationalLimits: json.RawMessage(`{"max":1}`), ResultSemantics: "none",
		SecurityNotes: []string{"none"}, MCPClass: "MCP_TOOL", ToolEligible: true,
		InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
	}}}
}

package manifest

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func TestCanonicalManifestIsCompleteAndClosed(t *testing.T) {
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	tools := s.ToolEligible()
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.ToolName)
		if (tool.MCPClass != "MCP_TOOL" && tool.MCPClass != "MCP_PROPOSAL_ONLY") || !tool.ToolEligible {
			t.Fatalf("tool registry contains non-tool capability: %#v", tool)
		}
		if len(tool.InputSchema) == 0 || strings.Contains(strings.ToLower(string(tool.InputSchema)), `"sql"`) {
			t.Fatalf("tool registry contains unsafe schema for %s", tool.ToolName)
		}
	}
	sort.Strings(names)
	want := []string{
		"authorization.permissions.propose",
		"authorization.permissions.read",
		"authorization.proposal.read",
		"ouf.ingestion.history",
		"ouf.ingestion.status",
		"ouf.operations.explain",
		"ouf.operations.incidents",
		"ouf.operations.summary",
		"ouf.system.status",
		"urban.object.related_search",
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected tool registry names=%v want=%v", names, want)
	}
}

func TestObjectSearchRequiresRuntimeActivation(t *testing.T) {
	s, err := Load()
	if err != nil { t.Fatal(err) }
	found := false
	for _, capability := range s.Capabilities {
		if capability.CapabilityID != "urban.object.search" { continue }
		found = true
		if capability.PublicationState != "INACTIVE" || capability.RequiredAuthorization != "urban.object.search" || capability.GatewayBindingRef != "capability://urban.object.search" {
			t.Fatalf("unsafe search publication: %#v", capability)
		}
	}
	if !found { t.Fatal("object search descriptor missing") }
	for _, capability := range s.ToolEligible() {
		if capability.CapabilityID == "urban.object.search" { t.Fatal("unverified object search must not be advertised") }
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

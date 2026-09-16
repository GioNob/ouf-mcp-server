package deployment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPGatewayOperationalAwarenessPairwise(t *testing.T) {
	root := os.Getenv("GATEWAY_PAIRWISE_ROOT")
	if root == "" {
		t.Skip("GATEWAY_PAIRWISE_ROOT not set")
	}
	manifestRaw, err := os.ReadFile(filepath.Join("..", "manifest", "capabilities.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Capabilities []struct {
			ToolName, CapabilityID, RequiredAuthorizationCapability, GatewayBindingRef string
			ToolEligible                                                               bool `json:"toolEligible"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	expected := map[string]struct{ scope, route, backend string }{
		"ouf.ingestion.status":     {"ingestion.operations.read", "mcp-ingestion-status.yaml", "/api/internal/v1/ingestion/operations/status"},
		"ouf.ingestion.history":    {"ingestion.operations.read", "mcp-ingestion-history.yaml", "/api/internal/v1/ingestion/operations/history"},
		"ouf.operations.incidents": {"operations.incident.read", "mcp-operations-incidents.yaml", "/api/internal/v1/ingestion/operations/incidents"},
		"ouf.operations.summary":   {"operations.status.read", "mcp-operations-summary.yaml", "/api/internal/v1/ingestion/operations/summary"},
	}
	seen := map[string]bool{}
	for _, c := range manifest.Capabilities {
		e, ok := expected[c.CapabilityID]
		if !ok {
			continue
		}
		if !c.ToolEligible || c.ToolName != c.CapabilityID || c.RequiredAuthorizationCapability != e.scope || c.GatewayBindingRef != "capability://"+c.CapabilityID {
			t.Fatalf("MCP operational capability mismatch for %s", c.CapabilityID)
		}
		raw, err := os.ReadFile(filepath.Join(root, "ouf-config", "routes", "northbound", e.route))
		if err != nil {
			t.Fatal(err)
		}
		s := string(raw)
		if !strings.Contains(s, "capabilityRef: "+c.CapabilityID+"@1.0.0") || !strings.Contains(s, "allowedServiceIdentities: [ouf-mcp-server]") || !strings.Contains(s, "service: ouf-ingestion-runtime") || !strings.Contains(s, "path: "+e.backend) {
			t.Fatalf("Gateway operational route mismatch for %s", c.CapabilityID)
		}
		seen[c.CapabilityID] = true
	}
	if len(seen) != len(expected) {
		t.Fatalf("operational capability coverage=%d want=%d", len(seen), len(expected))
	}
}

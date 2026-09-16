package deployment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperationalAwarenessAggregationPairwise(t *testing.T) {
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
			CapabilityID, Owner string
			ToolEligible        bool `json:"toolEligible"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"ouf.operations.incidents", "ouf.operations.summary"} {
		found := false
		for _, c := range manifest.Capabilities {
			if c.CapabilityID == id {
				found = true
				if c.Owner != "mcp" || !c.ToolEligible {
					t.Fatalf("global aggregate capability %s owner/tool mismatch: %+v", id, c)
				}
			}
		}
		if !found {
			t.Fatalf("missing global aggregate capability %s", id)
		}
	}

	checks := map[string][]string{
		"mcp-operations-incidents.yaml": {"sourceRef: mcp-server-operations@1.0.0", "service: ouf-mcp-server", "path: /api/internal/v1/mcp/operations/incidents"},
		"mcp-operations-summary.yaml": {"sourceRef: mcp-server-operations@1.0.0", "service: ouf-mcp-server", "path: /api/internal/v1/mcp/operations/summary"},
		"mcp-ingestion-operations-incidents-producer.yaml": {"capabilityRef: ouf.ingestion.operations.incidents@1.0.0", "service: ouf-ingestion-runtime"},
		"mcp-ingestion-operations-summary-producer.yaml": {"capabilityRef: ouf.ingestion.operations.summary@1.0.0", "service: ouf-ingestion-runtime"},
		"mcp-gateway-operations-incidents.yaml": {"capabilityRef: ouf.gateway.operations.incidents@1.0.0", "service: ouf-gateway-control-plane"},
		"mcp-gateway-operations-summary.yaml": {"capabilityRef: ouf.gateway.operations.summary@1.0.0", "service: ouf-gateway-control-plane"},
		"api-operations-incidents.yaml": {"identity: OIDC", "service: ouf-mcp-server", "path: /api/internal/v1/mcp/operations/incidents"},
		"api-operations-summary.yaml": {"identity: OIDC", "service: ouf-mcp-server", "path: /api/internal/v1/mcp/operations/summary"},
	}
	for name, required := range checks {
		raw, err := os.ReadFile(filepath.Join(root, "ouf-config", "routes", "northbound", name))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if strings.Contains(text, "path: /mcp") {
			t.Fatalf("aggregation route %s targets MCP protocol endpoint", name)
		}
		for _, needle := range required {
			if !strings.Contains(text, needle) {
				t.Fatalf("aggregation route %s missing %q", name, needle)
			}
		}
	}
}

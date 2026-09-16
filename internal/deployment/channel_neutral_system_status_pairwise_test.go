package deployment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChannelNeutralSystemStatusPairwise(t *testing.T) {
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
			CapabilityID, Owner, RequiredAuthorizationCapability, GatewayBindingRef string
			ToolEligible                                                            bool `json:"toolEligible"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range manifest.Capabilities {
		if c.CapabilityID != "ouf.system.status" {
			continue
		}
		found = true
		if !c.ToolEligible || c.Owner != "mcp" || c.RequiredAuthorizationCapability != "operations.status.read" || c.GatewayBindingRef != "capability://ouf.system.status" {
			t.Fatalf("MCP system status manifest mismatch: %+v", c)
		}
	}
	if !found {
		t.Fatal("ouf.system.status missing from MCP manifest")
	}

	internalRaw, err := os.ReadFile(filepath.Join(root, "ouf-config", "routes", "northbound", "mcp-system-status.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	publicRaw, err := os.ReadFile(filepath.Join(root, "ouf-config", "routes", "northbound", "api-system-status.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"internal": internalRaw, "public": publicRaw} {
		s := string(raw)
		if !strings.Contains(s, "capabilityRef: ouf.system.status@1.0.0") || !strings.Contains(s, "sourceRef: mcp-server-operations@1.0.0") || !strings.Contains(s, "service: ouf-mcp-server") || !strings.Contains(s, "path: /api/internal/v1/mcp/operations/status") {
			t.Fatalf("%s system status route mismatch", name)
		}
		if strings.Contains(s, "path: /mcp") {
			t.Fatalf("%s route recursively targets MCP protocol", name)
		}
	}
	if !strings.Contains(string(internalRaw), "identity: M2M") || !strings.Contains(string(internalRaw), "allowedServiceIdentities: [ouf-mcp-server]") {
		t.Fatal("internal MCP channel binding is not constrained to MCP workload identity")
	}
	if !strings.Contains(string(publicRaw), "identity: OIDC") || !strings.Contains(string(publicRaw), "/api/v1/operations/system/status") {
		t.Fatal("public channel binding is missing")
	}

	routerRaw, err := os.ReadFile(filepath.Join("..", "operational", "gateway.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(routerRaw), `in.Owner == "mcp"`) || strings.Contains(string(routerRaw), `CapabilityID == "ouf.system.status"`) {
		t.Fatal("MCP local-dispatch shortcut reintroduced")
	}
	mainRaw, err := os.ReadFile(filepath.Join("..", "..", "cmd", "ouf-mcp", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainRaw), `mux.Handle("/api/internal/v1/mcp/operations/status", operational.NewOwnerAPI(store))`) {
		t.Fatal("MCP private owner API is not mounted")
	}
}

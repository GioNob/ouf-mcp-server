package deployment

import (
	"os"
	"strings"
	"testing"
)

func TestMCP1HDeploymentContract(t *testing.T) {
	ha, err := os.ReadFile("../../deploy/mcp-ha.yaml")
	if err != nil {
		t.Fatal(err)
	}
	np, err := os.ReadFile("../../deploy/networkpolicy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	hs := string(ha)
	ns := string(np)
	for _, want := range []string{"replicas: 2", "minAvailable: 1", "serviceAccountName: ouf-mcp-server", "replicas: 1", "serviceAccountName: ouf-mcp-maintenance", "runAsNonRoot: true", "topologySpreadConstraints:"} {
		if !strings.Contains(hs, want) {
			t.Fatalf("missing deployment contract %q", want)
		}
	}
	for _, want := range []string{"name: mcp-default-deny", "policyTypes: [Ingress, Egress]", "ouf.component: apisix-northbound", "ouf.component: apisix-southbound", "port: 9443", "port: 5432", "port: 53", "name: mcp-maintenance-allow"} {
		if !strings.Contains(ns, want) {
			t.Fatalf("missing network contract %q", want)
		}
	}
	if strings.Contains(ns, "udp-object-resolution") {
		t.Fatal("MCP NetworkPolicy must not expose direct UDP access")
	}
}

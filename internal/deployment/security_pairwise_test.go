package deployment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type contract struct {
	GatewayMain string `json:"gatewayMain"`
	Gateway     struct {
		SouthboundPort                int  `json:"southboundPort"`
		SouthboundDefaultDeny         bool `json:"southboundDefaultDeny"`
		IngestionDirectVerticalDenied bool `json:"ingestionDirectVerticalDenied"`
	} `json:"gateway"`
	MCP struct {
		IngressFrom     []string `json:"ingressFrom"`
		EgressTo        []string `json:"egressTo"`
		ForbiddenDirect []string `json:"forbiddenDirect"`
	} `json:"mcp"`
	Maintenance struct {
		EgressTo  []string `json:"egressTo"`
		Forbidden []string `json:"forbidden"`
	} `json:"maintenance"`
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func TestMCPGatewayDeploymentSecurityPairwise(t *testing.T) {
	raw, err := os.ReadFile("security_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var c contract
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}

	root := os.Getenv("GATEWAY_PAIRWISE_ROOT")
	if root == "" {
		t.Skip("GATEWAY_PAIRWISE_ROOT not set")
	}
	southbound, err := os.ReadFile(filepath.Join(root, "helm", "networkpolicy", "southbound-default-deny.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	ingestion, err := os.ReadFile(filepath.Join(root, "helm", "networkpolicy", "ingestion-no-direct-egress.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(southbound)
	i := string(ingestion)

	if !c.Gateway.SouthboundDefaultDeny || !strings.Contains(s, "policyTypes: [Ingress, Egress]") {
		t.Fatal("Gateway southbound is not pairwise default-deny")
	}
	if !strings.Contains(s, "port: 9443") || c.Gateway.SouthboundPort != 9443 {
		t.Fatal("Gateway southbound port contract mismatch")
	}
	if !strings.Contains(s, "ouf.southbound-client: \"true\"") {
		t.Fatal("Gateway does not restrict southbound ingress to authorized workload namespaces")
	}
	if !c.Gateway.IngestionDirectVerticalDenied || !strings.Contains(i, "ouf.component: apisix-southbound") || strings.Contains(i, "udp-object-resolution") {
		t.Fatal("Ingestion bypass-prevention contract mismatch")
	}

	if !contains(c.MCP.IngressFrom, "ouf-gateway/apisix-northbound") {
		t.Fatal("MCP ingress must be Gateway northbound only")
	}
	if !contains(c.MCP.EgressTo, "ouf-gateway/apisix-southbound") || !contains(c.MCP.EgressTo, "postgresql") {
		t.Fatal("MCP required egress contract incomplete")
	}
	if !contains(c.MCP.ForbiddenDirect, "udp-object-resolution") {
		t.Fatal("MCP direct UDP access must be forbidden")
	}
	if contains(c.Maintenance.EgressTo, "ouf-gateway/apisix-southbound") || !contains(c.Maintenance.Forbidden, "udp-object-resolution") {
		t.Fatal("maintenance worker data-plane isolation invalid")
	}
}

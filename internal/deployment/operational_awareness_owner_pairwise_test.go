package deployment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperationalExplainOwnerPairwise(t *testing.T) {
	gatewayRoot := os.Getenv("GATEWAY_PAIRWISE_ROOT")
	ingestionRoot := os.Getenv("INGESTION_PAIRWISE_ROOT")
	if gatewayRoot == "" || ingestionRoot == "" {
		t.Skip("pairwise roots not set")
	}
	route, err := os.ReadFile(filepath.Join(gatewayRoot, "ouf-config", "routes", "northbound", "mcp-operations-explain.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	routeText := string(route)
	for _, want := range []string{
		"capabilityRef: ouf.operations.explain@1.0.0",
		"allowedServiceIdentities: [ouf-mcp-server]",
		"service: ouf-ingestion-runtime",
		"path: /api/internal/v1/ingestion/operations/incidents/explain",
	} {
		if !strings.Contains(routeText, want) {
			t.Fatalf("Gateway explain route missing %q", want)
		}
	}
	api, err := os.ReadFile(filepath.Join(ingestionRoot, "src", "main", "java", "it", "comune", "trieste", "ouf", "ingestion", "OperationalAwarenessApi.java"))
	if err != nil {
		t.Fatal(err)
	}
	apiText := string(api)
	for _, want := range []string{
		`@PostMapping("/incidents/explain")`,
		`operations.incident.explain`,
		`issues.explain`,
	} {
		if !strings.Contains(apiText, want) {
			t.Fatalf("Ingestion explain owner contract missing %q", want)
		}
	}
	service, err := os.ReadFile(filepath.Join(ingestionRoot, "src", "main", "java", "it", "comune", "trieste", "ouf", "ingestion", "RuntimeIssueService.java"))
	if err != nil {
		t.Fatal(err)
	}
	serviceText := string(service)
	for _, want := range []string{"recommendedAction", "explanationCode", "evidenceRef"} {
		if !strings.Contains(serviceText, want) {
			t.Fatalf("Ingestion safe explanation projection missing %q", want)
		}
	}
	if strings.Contains(serviceText, `result.put("evidencePayloadHash"`) {
		t.Fatal("safe explanation must not expose protected evidence payload hash")
	}
}

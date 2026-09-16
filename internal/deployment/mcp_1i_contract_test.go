package deployment

import (
	"os"
	"strings"
	"testing"
)

func TestMCP1IObservabilityCapacityAndDRContracts(t *testing.T) {
	files := map[string][]string{
		"../../deploy/mcp-ha.yaml": {
			`prometheus.io/path: /metrics`,
			`requests: {cpu: "500m", memory: "768Mi"}`,
			`limits: {cpu: "2", memory: "1536Mi"}`,
			`path: /health/ready`,
			`path: /health/live`,
			`kind: PodDisruptionBudget`,
		},
		"../../deploy/networkpolicy.yaml": {
			`ouf.telemetry: "true"`,
			`port: 8080`,
		},
		"../../observability/mcp-dashboard.yaml": {
			`rps`, `p95`, `p99`, `agent-governance`, `tool-quality`, `dependencies`, `forbiddenMetricLabels`,
		},
		"../../observability/mcp-alerts.yaml": {
			`McpErrorRateHigh`, `BudgetStoreLatencyHigh`, `budget_p99_ms > 150`, `availabilityTarget: 0.999`,
		},
		"../../docs/MCP_1I_DR_RUNBOOK.md": {
			`PITR`, `fail-closed`, `expired approvals`, `RPO`, `RTO`, `EVIDENCE PENDING`,
		},
		"../../cmd/ouf-mcp/main.go": {
			`GET /metrics`, `observability.NewRegistry()`, `ouf_mcp_db_pool_connections`,
		},
	}
	for path, required := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, needle := range required {
			if !strings.Contains(text, needle) {
				t.Fatalf("%s missing %q", path, needle)
			}
		}
	}
}

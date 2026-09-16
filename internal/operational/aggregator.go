package operational

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type GovernedCaller interface {
	Call(context.Context, orchestration.Invocation) (orchestration.Result, error)
}

type Aggregator struct {
	Caller           GovernedCaller
	Self             SelfStatusProvider
	ManifestChecksum string
}

type aggregateRequest struct {
	Identity       orchestration.Identity
	Arguments      json.RawMessage
	AttemptID      string
	CorrelationID  string
	RequestedLimit int
}

type producerSpec struct {
	Name, CapabilityID, Owner, Scope string
}

var incidentProducers = []producerSpec{
	{Name: "INGESTION", CapabilityID: "ouf.ingestion.operations.incidents", Owner: "ingestion", Scope: "operations.incident.read"},
	{Name: "GATEWAY", CapabilityID: "ouf.gateway.operations.incidents", Owner: "gateway", Scope: "operations.incident.read"},
}

var summaryProducers = []producerSpec{
	{Name: "INGESTION", CapabilityID: "ouf.ingestion.operations.summary", Owner: "ingestion", Scope: "operations.status.read"},
	{Name: "GATEWAY", CapabilityID: "ouf.gateway.operations.summary", Owner: "gateway", Scope: "operations.status.read"},
}

func (a Aggregator) Summary(ctx context.Context, in aggregateRequest) ([]byte, error) {
	if a.Caller == nil || a.Self == nil || a.ManifestChecksum == "" {
		return nil, fmt.Errorf("operational aggregator is not configured")
	}
	modules := make([]map[string]any, 0, 3)
	unavailable := make([]string, 0, 2)
	partial := false
	status := "HEALTHY"

	selfBody, err := a.Self.SystemStatus(ctx, in.Identity)
	if err != nil {
		partial = true
		unavailable = append(unavailable, "MCP")
		status = "DEGRADED"
	} else {
		var self map[string]any
		if json.Unmarshal(selfBody, &self) != nil {
			partial = true
			unavailable = append(unavailable, "MCP")
			status = "DEGRADED"
		} else {
			modules = append(modules, self)
			status = combineStatus(status, stringValue(self["status"]))
		}
	}

	for _, producer := range summaryProducers {
		body, ok := a.callProducer(ctx, in, producer)
		if !ok {
			partial = true
			unavailable = append(unavailable, producer.Name)
			status = "DEGRADED"
			continue
		}
		var module map[string]any
		if json.Unmarshal(body, &module) != nil {
			partial = true
			unavailable = append(unavailable, producer.Name)
			status = "DEGRADED"
			continue
		}
		modules = append(modules, module)
		status = combineStatus(status, stringValue(module["status"]))
		if p, _ := module["partial"].(bool); p {
			partial = true
			status = "DEGRADED"
		}
	}
	sort.Strings(unavailable)
	return json.Marshal(map[string]any{
		"status": status, "partial": partial, "modules": modules,
		"unavailableProducers": unavailable,
	})
}

func (a Aggregator) Incidents(ctx context.Context, in aggregateRequest) ([]byte, error) {
	if a.Caller == nil || a.Self == nil || a.ManifestChecksum == "" {
		return nil, fmt.Errorf("operational aggregator is not configured")
	}
	items := make([]map[string]any, 0)
	unavailable := make([]string, 0, 2)
	partial := false
	for _, producer := range incidentProducers {
		body, ok := a.callProducer(ctx, in, producer)
		if !ok {
			partial = true
			unavailable = append(unavailable, producer.Name)
			continue
		}
		var result struct {
			Items   []map[string]any `json:"items"`
			Partial bool             `json:"partial"`
		}
		if json.Unmarshal(body, &result) != nil {
			partial = true
			unavailable = append(unavailable, producer.Name)
			continue
		}
		for _, item := range result.Items {
			if _, exists := item["module"]; !exists {
				item["module"] = producer.Name
			}
			items = append(items, item)
		}
		partial = partial || result.Partial
	}

	selfBody, err := a.Self.SystemStatus(ctx, in.Identity)
	if err != nil {
		partial = true
		unavailable = append(unavailable, "MCP")
	} else {
		var self map[string]any
		if json.Unmarshal(selfBody, &self) != nil {
			partial = true
			unavailable = append(unavailable, "MCP")
		} else if state := stringValue(self["status"]); state != "" && state != "HEALTHY" {
			items = append(items, map[string]any{
				"incident_id": "mcp-operational-state", "module": "MCP", "event_type": "MCP_OPERATIONAL_" + state,
				"lifecycle_state": mapStatusLifecycle(state), "severity": mapStatusSeverity(state),
				"impact_summary": "MCP has governed recovery, debt, evidence or unresolved orchestration state requiring attention.",
				"action_required": state == "DEGRADED", "visibility_class": "TENANT_OPERATIONAL",
			})
		}
	}
	limit := in.RequestedLimit
	if limit < 1 || limit > 100 {
		limit = 50
	}
	if len(items) > limit {
		items = items[:limit]
	}
	sort.Strings(unavailable)
	return json.Marshal(map[string]any{"items": items, "partial": partial, "unavailableProducers": unavailable})
}

func (a Aggregator) callProducer(ctx context.Context, in aggregateRequest, producer producerSpec) ([]byte, bool) {
	idempotency := in.AttemptID + ":" + producer.CapabilityID
	if in.AttemptID == "" {
		idempotency = in.CorrelationID + ":" + producer.CapabilityID
	}
	result, err := a.Caller.Call(ctx, orchestration.Invocation{
		Identity: in.Identity, CapabilityID: producer.CapabilityID, Owner: producer.Owner, OperationClass: "READ",
		GatewayBindingRef: "capability://" + producer.CapabilityID, ManifestChecksum: a.ManifestChecksum,
		Arguments: in.Arguments, IdempotencyKey: idempotency, CorrelationID: in.CorrelationID,
		Window: time.Minute, Timeout: 3 * time.Second, RetryThreshold: 3,
		Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 100, ResultBytes: 512 << 10},
	})
	if err != nil || result.Problem != nil || len(result.Body) == 0 {
		return nil, false
	}
	return result.Body, true
}

func combineStatus(current, next string) string {
	if next == "DEGRADED" || current == "DEGRADED" {
		return "DEGRADED"
	}
	if next == "RECOVERING" || current == "RECOVERING" {
		return "RECOVERING"
	}
	return "HEALTHY"
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

func mapStatusLifecycle(status string) string {
	if status == "RECOVERING" {
		return "RECOVERING"
	}
	return "OPEN"
}

func mapStatusSeverity(status string) string {
	if status == "DEGRADED" {
		return "ERROR"
	}
	return "WARNING"
}

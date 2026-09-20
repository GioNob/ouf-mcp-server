package operational

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
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

type producerResult struct {
	Producer producerSpec
	Body     []byte
	OK       bool
	Denied   bool
}

var incidentProducers = []producerSpec{
	{Name: "INGESTION", CapabilityID: "ouf.ingestion.operations.incidents", Owner: "ingestion", Scope: "operations.incident.read"},
	{Name: "GATEWAY", CapabilityID: "ouf.gateway.operations.incidents", Owner: "gateway", Scope: "operations.incident.read"},
}

var summaryProducers = []producerSpec{
	{Name: "INGESTION", CapabilityID: "ouf.ingestion.operations.summary", Owner: "ingestion", Scope: "operations.status.read"},
	{Name: "GATEWAY", CapabilityID: "ouf.gateway.operations.summary", Owner: "gateway", Scope: "operations.status.read"},
}

// Summary never accepts an unknown producer status as evidence of health.
func (a Aggregator) Summary(ctx context.Context, in aggregateRequest) ([]byte, error) {
	if a.Caller == nil || a.Self == nil || a.ManifestChecksum == "" {
		return nil, fmt.Errorf("operational aggregator is not configured")
	}
	modules := make([]map[string]any, 0, 3)
	unavailable := make([]string, 0, 3)
	partial := false
	status := "HEALTHY"
	denied := false
	remaining := in.RequestedLimit
	if remaining < 1 || remaining > 100 {
		remaining = 50
	}
	appendModule := func(name string, raw []byte) {
		module, err := summaryModule(name, raw, remaining)
		if err != nil {
			partial = true
			unavailable = append(unavailable, name)
			status = "DEGRADED"
			return
		}
		if items, ok := module["items"].([]any); ok {
			remaining -= len(items)
		}
		modules = append(modules, module)
		status = combineStatus(status, stringValue(module["status"]))
		if module["partial"] == true {
			partial = true
			status = "DEGRADED"
		}
	}
	self, err := a.Self.SystemStatus(ctx, in.Identity)
	if err != nil {
		partial = true
		unavailable = append(unavailable, "MCP")
		status = "DEGRADED"
	} else {
		appendModule("MCP", self)
	}
	// Collect first, then project in fixed order so the global item budget is deterministic.
	results := make(map[string]producerResult)
	for result := range a.callProducers(ctx, in, summaryProducers) {
		results[result.Producer.Name] = result
	}
	for _, name := range []string{"GATEWAY", "INGESTION"} {
		result := results[name]
		if result.Denied {
			denied = true
			continue
		}
		if !result.OK {
			partial = true
			unavailable = append(unavailable, name)
			status = "DEGRADED"
			continue
		}
		appendModule(name, result.Body)
	}
	if denied {
		return nil, orchestration.ErrUnauthorized
	}
	sort.Slice(modules, func(i, j int) bool { return stringValue(modules[i]["module"]) < stringValue(modules[j]["module"]) })
	sort.Strings(unavailable)
	return json.Marshal(map[string]any{"status": status, "partial": partial, "modules": modules, "unavailableProducers": unavailable, "visibilityClass": "TENANT_OPERATIONAL", "redacted": true})
}

// Only governed projection fields survive; unknown fields and evidence payloads
// never propagate from an older or misconfigured producer.
func summaryModule(name string, raw []byte, limit int) (map[string]any, error) {
	if len(raw) > 512<<10 {
		return nil, fmt.Errorf("producer result too large")
	}
	var in map[string]any
	if json.Unmarshal(raw, &in) != nil || in["module"] != name {
		return nil, fmt.Errorf("invalid producer module")
	}
	state, ok := in["status"].(string)
	if !ok {
		return nil, fmt.Errorf("missing status")
	}
	switch state {
	case "HEALTHY", "RECOVERING", "DEGRADED", "UNKNOWN":
	default:
		return nil, fmt.Errorf("invalid status")
	}
	partial, ok := in["partial"].(bool)
	if !ok {
		return nil, fmt.Errorf("missing completeness")
	}
	if state == "UNKNOWN" {
		partial = true
	}
	out := map[string]any{"module": name, "status": state, "partial": partial, "visibilityClass": "TENANT_OPERATIONAL", "redacted": true}
	if name == "MCP" {
		if action, ok := in["actionRequired"].(bool); ok {
			out["actionRequired"] = action
		}
		return out, nil
	}
	if in["authorization"] == "REDACTED" {
		out["partial"] = true
		out["status"] = "UNKNOWN"
	}
	for _, key := range []string{"openIncidents", "recoveringIncidents"} {
		if n, ok := in[key].(float64); ok && n >= 0 && n == float64(int64(n)) {
			out[key] = n
		}
	}
	items := []any{}
	if rawItems, exists := in["items"]; exists {
		rows, ok := rawItems.([]any)
		if !ok || len(rows) > 100 {
			return nil, fmt.Errorf("invalid incidents")
		}
		for _, value := range rows {
			row, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid incident")
			}
			if row["visibility_class"] != "TENANT_OPERATIONAL" && row["visibility_class"] != "PUBLIC_OPERATIONAL" {
				out["partial"] = true
				out["status"] = "UNKNOWN"
				continue
			}
			if len(items) >= limit {
				out["partial"] = true
				continue
			}
			safe := map[string]any{}
			for _, key := range []string{"incident_id", "module", "event_type", "lifecycle_state", "severity", "first_seen_at", "last_seen_at", "resolved_at", "source_ref", "job_ref", "error_code", "retry_state", "next_retry_at", "impact_summary", "visibility_class"} {
				if v, exists := row[key]; exists && v != nil {
					text, ok := v.(string)
					if !ok || len(text) > 2048 {
						return nil, fmt.Errorf("invalid incident field")
					}
					safe[key] = text
				}
			}
			if v, ok := row["action_required"].(bool); ok {
				safe["action_required"] = v
			}
			items = append(items, safe)
		}
	}
	if out["status"] == "UNKNOWN" {
		delete(out, "openIncidents")
		delete(out, "recoveringIncidents")
	}
	out["items"] = items
	return out, nil
}

func (a Aggregator) Incidents(ctx context.Context, in aggregateRequest) ([]byte, error) {
	if a.Caller == nil || a.Self == nil || a.ManifestChecksum == "" {
		return nil, fmt.Errorf("operational aggregator is not configured")
	}
	items := make([]map[string]any, 0)
	unavailable := make([]string, 0, 2)
	partial := false
	for result := range a.callProducers(ctx, in, incidentProducers) {
		if !result.OK {
			partial = true
			unavailable = append(unavailable, result.Producer.Name)
			continue
		}
		var decoded struct {
			Items   []map[string]any `json:"items"`
			Partial bool             `json:"partial"`
		}
		if json.Unmarshal(result.Body, &decoded) != nil {
			partial = true
			unavailable = append(unavailable, result.Producer.Name)
			continue
		}
		for _, item := range decoded.Items {
			if _, exists := item["module"]; !exists {
				item["module"] = result.Producer.Name
			}
			items = append(items, item)
		}
		partial = partial || decoded.Partial
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
				"impact_summary":  "MCP has governed recovery, debt, evidence or unresolved orchestration state requiring attention.",
				"action_required": state == "DEGRADED", "visibility_class": "TENANT_OPERATIONAL",
			})
		}
	}
	limit := in.RequestedLimit
	if limit < 1 || limit > 100 {
		limit = 50
	}
	sort.SliceStable(items, func(i, j int) bool { return stringValue(items[i]["module"]) < stringValue(items[j]["module"]) })
	if len(items) > limit {
		items = items[:limit]
	}
	sort.Strings(unavailable)
	return json.Marshal(map[string]any{"items": items, "partial": partial, "unavailableProducers": unavailable})
}

func (a Aggregator) callProducers(ctx context.Context, in aggregateRequest, producers []producerSpec) <-chan producerResult {
	results := make(chan producerResult, len(producers))
	var wg sync.WaitGroup
	wg.Add(len(producers))
	for _, producer := range producers {
		producer := producer
		go func() {
			defer wg.Done()
			body, ok, denied := a.callProducer(ctx, in, producer)
			results <- producerResult{Producer: producer, Body: body, OK: ok, Denied: denied}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

func (a Aggregator) callProducer(ctx context.Context, in aggregateRequest, producer producerSpec) ([]byte, bool, bool) {
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
		return nil, false, errors.Is(err, orchestration.ErrUnauthorized) || (result.Problem != nil && (result.Problem.Status == 401 || result.Problem.Status == 403 || result.Problem.Code == "NOT_AUTHORIZED"))
	}
	return result.Body, true, false
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

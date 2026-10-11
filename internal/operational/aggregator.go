package operational

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/statusview"
	"github.com/google/uuid"
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

	selfBody, err := a.selfStatus(ctx, in.Identity)
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
			if p, _ := self["partial"].(bool); p {
				partial = true
				status = "DEGRADED"
			}
		}
	}

	for result := range a.callProducers(ctx, in, summaryProducers) {
		if !result.OK {
			partial = true
			unavailable = append(unavailable, result.Producer.Name)
			status = "DEGRADED"
			continue
		}
		var module map[string]any
		if json.Unmarshal(result.Body, &module) != nil || !validSummary(module, result.Producer.Name) {
			partial = true
			unavailable = append(unavailable, result.Producer.Name)
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
	sort.Slice(modules, func(i, j int) bool { return stringValue(modules[i]["module"]) < stringValue(modules[j]["module"]) })
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
	producerTruncated := false
	unavailable := make([]string, 0, 2)
	partial := false
	for result := range a.callProducers(ctx, in, incidentProducers) {
		if !result.OK {
			partial = true
			unavailable = append(unavailable, result.Producer.Name)
			continue
		}
		var decoded struct {
			Items     []map[string]any `json:"items"`
			Partial   *bool            `json:"partial"`
			Truncated bool             `json:"truncated"`
		}
		if json.Unmarshal(result.Body, &decoded) != nil || decoded.Items == nil || decoded.Partial == nil || !validIncidentItems(decoded.Items, result.Producer.Name) {
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
		partial = partial || *decoded.Partial
		producerTruncated = producerTruncated || decoded.Truncated
	}

	selfBody, err := a.selfStatus(ctx, in.Identity)
	if err != nil {
		partial = true
		unavailable = append(unavailable, "MCP")
	} else {
		var self map[string]any
		if json.Unmarshal(selfBody, &self) != nil {
			partial = true
			unavailable = append(unavailable, "MCP")
		} else {
			if p, _ := self["partial"].(bool); p {
				partial = true
			}
			if state := stringValue(self["status"]); state != "HEALTHY" {
				items = append(items, map[string]any{
					"incident_id": "mcp-operational-state", "module": "MCP", "event_type": "MCP_OPERATIONAL_" + state,
					"lifecycle_state": mapStatusLifecycle(state), "severity": mapStatusSeverity(state),
					"impact_summary":  "MCP has governed recovery, debt, evidence or unresolved orchestration state requiring attention.",
					"action_required": state == "DEGRADED", "visibility_class": "TENANT_OPERATIONAL",
				})
			}
		}
	}
	limit := in.RequestedLimit
	if limit < 1 || limit > 100 {
		limit = 50
	}
	sort.Slice(items, func(i, j int) bool { return incidentBefore(items[i], items[j]) })
	truncated := producerTruncated || len(items) > limit
	if len(items) > limit {
		items = items[:limit]
	}
	sort.Strings(unavailable)
	return json.Marshal(map[string]any{"items": items, "partial": partial, "unavailableProducers": unavailable, "truncated": truncated,
		"selectionScope": "RETURNED_OWNER_PROJECTIONS", "priorityProfile": "ouf.incident-priority.v1"})
}

func (a Aggregator) callProducers(ctx context.Context, in aggregateRequest, producers []producerSpec) <-chan producerResult {
	results := make(chan producerResult, len(producers))
	var wg sync.WaitGroup
	wg.Add(len(producers))
	for _, producer := range producers {
		producer := producer
		go func() {
			defer wg.Done()
			body, ok := a.callProducer(ctx, in, producer)
			results <- producerResult{Producer: producer, Body: body, OK: ok}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

func (a Aggregator) callProducer(ctx context.Context, in aggregateRequest, producer producerSpec) ([]byte, bool) {
	arguments, resource, valid := producerRead(in, producer)
	if !valid {
		return nil, false
	}
	parent := in.AttemptID
	if parent == "" {
		parent = in.CorrelationID
	}
	if parent == "" {
		parent = uuid.NewString()
	}
	// Bind each nested read/page to its canonical arguments. Reusing one
	// parent id for different pages must not collide with an admitted request.
	idempotency := fmt.Sprintf("%s:%s:%x", parent, producer.CapabilityID, sha256.Sum256(arguments))
	result, err := a.Caller.Call(ctx, orchestration.Invocation{
		Identity: in.Identity, Resource: resource, CapabilityID: producer.CapabilityID, Owner: producer.Owner, OperationClass: "READ",
		GatewayBindingRef: "capability://" + producer.CapabilityID, ManifestChecksum: a.ManifestChecksum,
		Arguments: arguments, IdempotencyKey: idempotency, CorrelationID: in.CorrelationID,
		Window: time.Minute, Timeout: 3 * time.Second, RetryThreshold: 3,
		Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 100, ResultBytes: 512 << 10},
	})
	if err != nil || result.Problem != nil || len(result.Body) == 0 || len(result.Body) > 512<<10 {
		return nil, false
	}
	return result.Body, true
}

// Build an owner-specific request without inventing cross-owner filter
// equivalence. Gateway control-plane faults are platform scoped; sourceId is
// an Ingestion collection filter, not a Gateway tenant/source grant.
func producerRead(in aggregateRequest, producer producerSpec) (json.RawMessage, orchestration.ResourceContext, bool) {
	var query map[string]json.RawMessage
	if json.Unmarshal(in.Arguments, &query) != nil || query == nil {
		return nil, orchestration.ResourceContext{}, false
	}
	attributes := map[string]string{"module": producer.Name}
	if producer.Name == "GATEWAY" {
		attributes["detailLevel"] = "RESTRICTED_OPERATIONAL"
		attributes["incidentScope"] = "PLATFORM"
		delete(query, "sourceId")
	} else {
		attributes["detailLevel"] = "TENANT_OPERATIONAL"
		if source, exists := query["sourceId"]; exists {
			var value string
			if json.Unmarshal(source, &value) != nil || value == "" {
				return nil, orchestration.ResourceContext{}, false
			}
			attributes["sourceRef"] = value
		}
	}
	arguments, err := json.Marshal(query)
	return arguments, orchestration.ResourceContext{ResourceType: "operational", TenantID: in.Identity.TenantID, Attributes: attributes}, err == nil
}

func combineStatus(current, next string) string {
	if !validStatus(current) || !validStatus(next) {
		return "DEGRADED"
	}
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

// incidentBefore prioritizes owner-provided evidence before the aggregate limit.
// It does not infer business impact from occurrence counts or merge owner identities.
func incidentBefore(a, b map[string]any) bool {
	av, bv := incidentField(a, "lifecycle_state", "lifecycleState"), incidentField(b, "lifecycle_state", "lifecycleState")
	if (av == "RESOLVED") != (bv == "RESOLVED") {
		return av != "RESOLVED"
	}
	if ar, br := severityRank(stringValue(a["severity"])), severityRank(stringValue(b["severity"])); ar != br {
		return ar > br
	}
	aa, _ := incidentValue(a, "action_required", "actionRequired").(bool)
	ba, _ := incidentValue(b, "action_required", "actionRequired").(bool)
	if aa != ba {
		return aa
	}
	at, bt := incidentTime(a), incidentTime(b)
	if !at.Equal(bt) {
		return at.After(bt)
	}
	if am, bm := stringValue(a["module"]), stringValue(b["module"]); am != bm {
		return am < bm
	}
	return incidentField(a, "incident_id", "incidentId") < incidentField(b, "incident_id", "incidentId")
}

func severityRank(value string) int {
	switch value {
	case "CRITICAL":
		return 4
	case "ERROR":
		return 3
	case "WARNING":
		return 2
	case "INFO":
		return 1
	default:
		return 0
	}
}

func incidentValue(item map[string]any, snake, camel string) any {
	if value, ok := item[snake]; ok {
		return value
	}
	return item[camel]
}

func incidentField(item map[string]any, snake, camel string) string {
	return stringValue(incidentValue(item, snake, camel))
}

func incidentTime(item map[string]any) time.Time {
	value := incidentField(item, "last_seen_at", "lastSeenAt")
	if value == "" {
		value = incidentField(item, "first_seen_at", "firstSeenAt")
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func (a Aggregator) selfStatus(ctx context.Context, identity orchestration.Identity) ([]byte, error) {
	raw, err := a.Self.SystemStatus(ctx, identity)
	if err != nil {
		return nil, err
	}
	return statusview.Project(raw)
}

func validStatus(status string) bool {
	return status == "HEALTHY" || status == "RECOVERING" || status == "DEGRADED"
}

func validSummary(module map[string]any, expected string) bool {
	_, partialPresent := module["partial"].(bool)
	return module != nil && module["module"] == expected && validStatus(stringValue(module["status"])) && partialPresent
}

func validIncidentItems(items []map[string]any, expected string) bool {
	for _, item := range items {
		if item == nil {
			return false
		}
		if module, exists := item["module"]; exists && module != expected {
			return false
		}
	}
	return true
}

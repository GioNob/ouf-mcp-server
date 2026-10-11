package operational

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type aggregateCallerFixture struct {
	responses map[string][]byte
	fail      map[string]bool
	mu        sync.Mutex
	calls     []string
}

func (f *aggregateCallerFixture) Call(_ context.Context, in orchestration.Invocation) (orchestration.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, in.CapabilityID)
	f.mu.Unlock()
	if f.fail[in.CapabilityID] {
		return orchestration.Result{}, errors.New("producer unavailable")
	}
	return orchestration.Result{Body: f.responses[in.CapabilityID]}, nil
}

func (f *aggregateCallerFixture) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type aggregateSelfFixture struct{ body []byte }

func (f aggregateSelfFixture) SystemStatus(context.Context, orchestration.Identity) ([]byte, error) {
	return f.body, nil
}

func aggregateInput() aggregateRequest {
	return aggregateRequest{
		Identity:  orchestration.Identity{ServicePrincipalID: "ouf-mcp-server", PrincipalID: "operator", TenantID: "tenant-a", ActorType: "HUMAN_USER"},
		Arguments: []byte(`{"limit":50}`), AttemptID: "attempt-a", CorrelationID: "correlation-a", RequestedLimit: 50,
	}
}

func TestSummaryAggregatesOwnerTruthWithoutInventingHealth(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.summary": []byte(`{"module":"INGESTION","status":"HEALTHY","partial":false}`),
		"ouf.gateway.operations.summary":   []byte(`{"module":"GATEWAY","status":"RECOVERING","partial":false}`),
	}, fail: map[string]bool{}}
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`)}, ManifestChecksum: "manifest"}
	body, err := a.Summary(context.Background(), aggregateInput())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "RECOVERING" || got["partial"] != false {
		t.Fatalf("unexpected aggregate: %s", body)
	}
	if caller.callCount() != 2 {
		t.Fatalf("producer call count=%d", caller.callCount())
	}
}

func TestSummaryUnavailableProducerIsPartialAndDegraded(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.summary": []byte(`{"module":"INGESTION","status":"HEALTHY","partial":false}`),
	}, fail: map[string]bool{"ouf.gateway.operations.summary": true}}
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`)}, ManifestChecksum: "manifest"}
	body, err := a.Summary(context.Background(), aggregateInput())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status               string   `json:"status"`
		Partial              bool     `json:"partial"`
		UnavailableProducers []string `json:"unavailableProducers"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "DEGRADED" || !got.Partial || len(got.UnavailableProducers) != 1 || got.UnavailableProducers[0] != "GATEWAY" {
		t.Fatalf("missing producer collapsed incorrectly: %s", body)
	}
}

func TestIncidentsMergeProducersAndSafeMCPAggregate(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.incidents": []byte(`{"items":[{"incident_id":"ing-1","module":"INGESTION","lifecycle_state":"OPEN"}],"partial":false}`),
		"ouf.gateway.operations.incidents":   []byte(`{"items":[{"incident_id":"gw-1","module":"GATEWAY","lifecycle_state":"RESOLVED"}],"partial":false}`),
	}, fail: map[string]bool{}}
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"securityIncidentCount":4,"partial":false}`)}, ManifestChecksum: "manifest"}
	body, err := a.Incidents(context.Background(), aggregateInput())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 3 {
		t.Fatalf("items=%s", body)
	}
	text := string(body)
	if json.Valid(body) == false || containsAny(text, "securityIncidentCount", "incidentRef", "detailCode") {
		t.Fatalf("protected MCP detail leaked into global incident projection: %s", body)
	}
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if len(needle) > 0 && stringContains(value, needle) {
			return true
		}
	}
	return false
}

func stringContains(value, needle string) bool {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func TestCriticalIncidentSurvivesAggregateLimit(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.incidents": []byte(`{"items":[{"incident_id":"ing-critical","module":"INGESTION","lifecycle_state":"OPEN","severity":"CRITICAL"}],"partial":false}`),
		"ouf.gateway.operations.incidents":   []byte(`{"items":[{"incident_id":"gw-info","module":"GATEWAY","lifecycle_state":"OPEN","severity":"INFO"}],"partial":false}`),
	}, fail: map[string]bool{}}
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`)}, ManifestChecksum: "manifest"}
	in := aggregateInput()
	in.RequestedLimit = 1
	for i := 0; i < 30; i++ {
		body, err := a.Incidents(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Items          []map[string]any `json:"items"`
			Truncated      bool             `json:"truncated"`
			SelectionScope string           `json:"selectionScope"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Items) != 1 || got.Items[0]["incident_id"] != "ing-critical" || !got.Truncated || got.SelectionScope != "RETURNED_OWNER_PROJECTIONS" {
			t.Fatalf("critical incident lost before limit: %s", body)
		}
	}
}

func TestIncidentPriorityKeepsActiveBeforeResolved(t *testing.T) {
	active := map[string]any{"incident_id": "active", "lifecycle_state": "OPEN", "severity": "WARNING"}
	resolved := map[string]any{"incident_id": "resolved", "lifecycle_state": "RESOLVED", "severity": "CRITICAL"}
	if !incidentBefore(active, resolved) || incidentBefore(resolved, active) {
		t.Fatal("resolved history displaced current incident")
	}
}

func TestIncidentPriorityUsesActionThenActualTimeThenStableIdentity(t *testing.T) {
	a := map[string]any{"incidentId": "a", "module": "INGESTION", "lifecycleState": "OPEN", "severity": "ERROR", "actionRequired": true, "lastSeenAt": "2026-10-11T04:00:00+02:00"}
	b := map[string]any{"incident_id": "b", "module": "INGESTION", "lifecycle_state": "OPEN", "severity": "ERROR", "action_required": false, "last_seen_at": "2026-10-11T03:00:00Z"}
	if !incidentBefore(a, b) {
		t.Fatal("action priority ignored")
	}
	b["action_required"] = true
	if !incidentBefore(b, a) {
		t.Fatal("offset timestamps compared as strings")
	}
	b["last_seen_at"] = "2026-10-11T02:00:00Z"
	if !incidentBefore(a, b) || incidentBefore(b, a) {
		t.Fatal("equal instant lacks stable identity tie")
	}
}

func TestIncidentsNoTruncationAndUnavailableProducerStayExplicit(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.incidents": []byte(`{"items":[{"incident_id":"ing-1","severity":"ERROR"}],"partial":false}`),
	}, fail: map[string]bool{"ouf.gateway.operations.incidents": true}}
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`)}, ManifestChecksum: "manifest"}
	body, err := a.Incidents(context.Background(), aggregateInput())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Partial     bool     `json:"partial"`
		Truncated   bool     `json:"truncated"`
		Unavailable []string `json:"unavailableProducers"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Partial || got.Truncated || len(got.Unavailable) != 1 || got.Unavailable[0] != "GATEWAY" {
		t.Fatalf("partial evidence hidden: %s", body)
	}
}

func TestProducerTruncationSurvivesSmallAggregate(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.incidents": []byte(`{"items":[],"partial":false,"truncated":true}`),
		"ouf.gateway.operations.incidents":   []byte(`{"items":[],"partial":false}`),
	}, fail: map[string]bool{}}
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`)}, ManifestChecksum: "manifest"}
	body, err := a.Incidents(context.Background(), aggregateInput())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Truncated bool `json:"truncated"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Truncated {
		t.Fatalf("producer truncation hidden: %s", body)
	}
}

func TestSummaryMalformedOrUnknownProducerCannotBecomeHealthy(t *testing.T) {
	for _, invalid := range []string{
		`null`, `{}`, `{"module":"GATEWAY","status":"UNKNOWN","partial":false}`,
		`{"module":"GATEWAY","status":"HEALTHY"}`,
		`{"module":"OTHER","status":"HEALTHY","partial":false}`,
		`{"module":"GATEWAY","status":"HEALTHY","partial":null}`,
	} {
		t.Run(invalid, func(t *testing.T) {
			caller := &aggregateCallerFixture{responses: map[string][]byte{
				"ouf.ingestion.operations.summary": []byte(`{"module":"INGESTION","status":"HEALTHY","partial":false}`),
				"ouf.gateway.operations.summary":   []byte(invalid),
			}}
			a := Aggregator{Caller: caller, Self: &ownerStatusFixture{}, ManifestChecksum: "manifest"}
			body, err := a.Summary(context.Background(), aggregateInput())
			var got struct {
				Status      string
				Partial     bool
				Unavailable []string `json:"unavailableProducers"`
			}
			if err != nil || json.Unmarshal(body, &got) != nil || got.Status != "DEGRADED" || !got.Partial || len(got.Unavailable) != 1 || got.Unavailable[0] != "GATEWAY" {
				t.Fatalf("invalid evidence became healthy: %s err=%v", body, err)
			}
		})
	}
}

func TestAggregateSelfProjectionAndPartialRemainSafe(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.summary":   []byte(`{"module":"INGESTION","status":"HEALTHY","partial":false}`),
		"ouf.gateway.operations.summary":     []byte(`{"module":"GATEWAY","status":"HEALTHY","partial":false}`),
		"ouf.ingestion.operations.incidents": []byte(`{"items":[],"partial":false}`),
		"ouf.gateway.operations.incidents":   []byte(`{"items":[],"partial":false}`),
	}}
	for _, self := range []string{
		`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":true,"futureSecret":"protected","securityIncidentCount":9}`,
		`{"module":"MCP","status":"UNKNOWN","actionRequired":false,"partial":false}`,
		`null`, `{"module":"MCP","status":"HEALTHY"}`,
	} {
		a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(self)}, ManifestChecksum: "manifest"}
		for _, read := range []func(context.Context, aggregateRequest) ([]byte, error){a.Summary, a.Incidents} {
			body, err := read(context.Background(), aggregateInput())
			var got map[string]any
			if err != nil || json.Unmarshal(body, &got) != nil || got["partial"] != true || got["status"] == "HEALTHY" || containsAny(string(body), "futureSecret", "protected", "securityIncidentCount") {
				t.Fatalf("unsafe self projection: %s err=%v", body, err)
			}
		}
	}
}

func TestMalformedIncidentEnvelopeCannotBecomeEmptySuccess(t *testing.T) {
	for _, invalid := range []string{`null`, `{}`, `{"items":null,"partial":false}`, `{"items":[]}`, `{"items":[null],"partial":false}`, `{"items":[{"module":"OTHER"}],"partial":false}`} {
		caller := &aggregateCallerFixture{responses: map[string][]byte{
			"ouf.ingestion.operations.incidents": []byte(`{"items":[],"partial":false}`),
			"ouf.gateway.operations.incidents":   []byte(invalid),
		}}
		a := Aggregator{Caller: caller, Self: &ownerStatusFixture{}, ManifestChecksum: "manifest"}
		body, err := a.Incidents(context.Background(), aggregateInput())
		var got struct {
			Partial     bool
			Unavailable []string `json:"unavailableProducers"`
		}
		if err != nil || json.Unmarshal(body, &got) != nil || !got.Partial || len(got.Unavailable) != 1 || got.Unavailable[0] != "GATEWAY" {
			t.Fatalf("invalid incident envelope became empty success: %s err=%v", body, err)
		}
	}
}

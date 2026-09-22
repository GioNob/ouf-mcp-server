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
	if in.WindowBudget != orchestration.DefaultWindowBudget() || in.RetryThreshold != 3 {
		return orchestration.Result{}, errors.New("producer budget policy missing or inconsistent")
	}
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
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","partial":false}`)}, ManifestChecksum: "manifest"}
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
	a := Aggregator{Caller: caller, Self: aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","partial":false}`)}, ManifestChecksum: "manifest"}
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

func TestIncidentsPageAcrossPersistentOwnersWithoutSyntheticMCPHistory(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{
		"ouf.ingestion.operations.incidents": []byte(`{"items":[{"incident_id":"ing-1","module":"INGESTION","lifecycle_state":"OPEN","visibility_class":"TENANT_OPERATIONAL","rawLog":"secret"}],"partial":false,"hasMore":false}`),
		"ouf.gateway.operations.incidents":   []byte(`{"items":[{"incident_id":"gw-1","module":"GATEWAY","lifecycle_state":"RESOLVED","visibility_class":"TENANT_OPERATIONAL"}],"partial":false,"hasMore":false}`),
	}, fail: map[string]bool{}}
	a := Aggregator{Caller: caller, ManifestChecksum: "manifest"}
	in := aggregateInput()
	body, err := a.Incidents(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	var first map[string]any
	if json.Unmarshal(body, &first) != nil {
		t.Fatal(string(body))
	}
	if len(first["items"].([]any)) != 1 || first["hasMore"] != true || first["partial"] != false || containsAny(string(body), "secret", "rawLog") {
		t.Fatalf("bad first page: %s", body)
	}
	var query map[string]any
	_ = json.Unmarshal(in.Arguments, &query)
	query["cursor"] = first["nextCursor"]
	in.Arguments, _ = json.Marshal(query)
	body, err = a.Incidents(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	var second map[string]any
	_ = json.Unmarshal(body, &second)
	if second["hasMore"] != false || len(second["items"].([]any)) != 1 || second["items"].([]any)[0].(map[string]any)["module"] != "GATEWAY" {
		t.Fatalf("bad last page: %s", body)
	}
	if caller.callCount() != 2 {
		t.Fatalf("calls=%d", caller.callCount())
	}
}

func TestIncidentMissingCompletenessCannotBecomeEmptySuccess(t *testing.T) {
	caller := &aggregateCallerFixture{responses: map[string][]byte{"ouf.ingestion.operations.incidents": []byte(`{"items":[]}`)}, fail: map[string]bool{}}
	a := Aggregator{Caller: caller, ManifestChecksum: "manifest"}
	body, err := a.Incidents(context.Background(), aggregateInput())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if got["partial"] != true || got["hasMore"] != true || got["nextCursor"] == nil {
		t.Fatalf("missing owner hidden: %s", body)
	}
}

func TestIncidentCursorRejectsChangedIdentityAndFilters(t *testing.T) {
	in := aggregateInput()
	limit := 2
	source := "source-a"
	q := incidentQuery{Limit: &limit, SourceID: &source}
	c, err := prepareIncidentQuery(&q, in.Identity)
	if err != nil {
		t.Fatal(err)
	}
	cursor := encodeIncidentCursor(c)
	q.Cursor = &cursor
	other := in.Identity
	other.TenantID = "other"
	if _, err = prepareIncidentQuery(&q, other); err == nil {
		t.Fatal("cross-tenant cursor accepted")
	}
	changed := "source-b"
	q.SourceID = &changed
	if _, err = prepareIncidentQuery(&q, in.Identity); err == nil {
		t.Fatal("changed filter accepted")
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

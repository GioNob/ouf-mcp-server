package observability

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestMetricsUseOnlyBoundedLabels(t *testing.T) {
	r := NewRegistry()
	h := r.Instrument("mcp", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("X-Correlation-Id", "sensitive-correlation")
	h.ServeHTTP(httptest.NewRecorder(), req)
	out := httptest.NewRecorder()
	r.Handler(nil).ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := out.Body.String()
	if !strings.Contains(body, `route="mcp"`) || !strings.Contains(body, `outcome="2xx"`) {
		t.Fatalf("missing bounded labels: %s", body)
	}
	for _, forbidden := range []string{"sensitive-correlation", "tenant", "principal", "applicationSessionId", "toolAttemptId", "backendRequestId"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("high-cardinality/sensitive label leaked: %q in %s", forbidden, body)
		}
	}
}

func TestDispatchMiddlewareP95Under50msFixture(t *testing.T) {
	r := NewRegistry()
	h := r.Instrument("mcp", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	const n = 1000
	durations := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/mcp", nil))
		durations = append(durations, time.Since(start))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p95 := durations[(n*95)/100-1]
	if p95 >= 50*time.Millisecond {
		t.Fatalf("MCP dispatch instrumentation fixture p95=%s target<50ms", p95)
	}
}

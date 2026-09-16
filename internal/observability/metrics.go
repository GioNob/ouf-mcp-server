package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Registry is a bounded in-process metrics registry. Labels are intentionally
// limited to route and coarse HTTP outcome; tenant/principal/correlation IDs
// must never become metric labels.
type Registry struct {
	mu       sync.Mutex
	requests map[string]uint64
	latency  map[string][]uint64
	buckets  []time.Duration
}

func NewRegistry() *Registry {
	return &Registry{
		requests: map[string]uint64{},
		latency:  map[string][]uint64{},
		buckets:  []time.Duration{10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 150 * time.Millisecond, 500 * time.Millisecond, time.Second, 3 * time.Second, 10 * time.Second},
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func coarseOutcome(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}

func (r *Registry) Instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, req)
		r.Observe(route, coarseOutcome(sw.status), time.Since(start))
	})
}

func (r *Registry) Observe(route, outcome string, d time.Duration) {
	key := route + "\x00" + outcome
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests[key]++
	counts := r.latency[route]
	if counts == nil {
		counts = make([]uint64, len(r.buckets)+1)
	}
	placed := false
	for i, b := range r.buckets {
		if d <= b {
			counts[i]++
			placed = true
			break
		}
	}
	if !placed {
		counts[len(counts)-1]++
	}
	r.latency[route] = counts
}

func (r *Registry) Handler(extra func() string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.mu.Lock()
		requestCopy := make(map[string]uint64, len(r.requests))
		for k, v := range r.requests { requestCopy[k] = v }
		latencyCopy := make(map[string][]uint64, len(r.latency))
		for k, v := range r.latency { latencyCopy[k] = append([]uint64(nil), v...) }
		buckets := append([]time.Duration(nil), r.buckets...)
		r.mu.Unlock()

		keys := make([]string, 0, len(requestCopy))
		for k := range requestCopy { keys = append(keys, k) }
		sort.Strings(keys)
		fmt.Fprintln(w, "# HELP ouf_mcp_http_requests_total Governed MCP HTTP requests by bounded route/outcome labels.")
		fmt.Fprintln(w, "# TYPE ouf_mcp_http_requests_total counter")
		for _, k := range keys {
			parts := strings.SplitN(k, "\x00", 2)
			fmt.Fprintf(w, "ouf_mcp_http_requests_total{route=%q,outcome=%q} %d\n", parts[0], parts[1], requestCopy[k])
		}
		fmt.Fprintln(w, "# HELP ouf_mcp_http_duration_bucket Request duration histogram buckets.")
		fmt.Fprintln(w, "# TYPE ouf_mcp_http_duration_bucket histogram")
		routes := make([]string, 0, len(latencyCopy))
		for route := range latencyCopy { routes = append(routes, route) }
		sort.Strings(routes)
		for _, route := range routes {
			cumulative := uint64(0)
			for i, b := range buckets {
				cumulative += latencyCopy[route][i]
				fmt.Fprintf(w, "ouf_mcp_http_duration_bucket{route=%q,le=%q} %d\n", route, fmt.Sprintf("%.3f", b.Seconds()), cumulative)
			}
			cumulative += latencyCopy[route][len(buckets)]
			fmt.Fprintf(w, "ouf_mcp_http_duration_bucket{route=%q,le=\"+Inf\"} %d\n", route, cumulative)
		}
		if extra != nil { fmt.Fprint(w, extra()) }
	})
}

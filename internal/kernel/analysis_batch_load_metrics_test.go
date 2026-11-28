//go:build batchload

package kernel

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type loadSampleKey struct{}

type loadSample struct {
	queries, rows, received, sent atomic.Int64
}

type loadWire struct {
	net.Conn
	received, sent atomic.Int64
}

func (w *loadWire) Read(p []byte) (int, error) {
	n, err := w.Conn.Read(p)
	w.received.Add(int64(n))
	return n, err
}

func (w *loadWire) Write(p []byte) (int, error) {
	n, err := w.Conn.Write(p)
	w.sent.Add(int64(n))
	return n, err
}

type loadQueryKey struct{}

type loadQuery struct {
	sample *loadSample
	wire   *loadWire
	r, w   int64
}

type loadTracer struct{}

func (loadTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	sample, _ := ctx.Value(loadSampleKey{}).(*loadSample)
	if sample == nil {
		return ctx
	}
	raw := conn.PgConn().Conn()
	if secured, ok := raw.(*tls.Conn); ok {
		raw = secured.NetConn()
	}
	q := loadQuery{sample: sample}
	if wire, ok := raw.(*loadWire); ok {
		q.wire, q.r, q.w = wire, wire.received.Load(), wire.sent.Load()
	}
	return context.WithValue(ctx, loadQueryKey{}, q)
}

func (loadTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, end pgx.TraceQueryEndData) {
	q, ok := ctx.Value(loadQueryKey{}).(loadQuery)
	if !ok {
		return
	}
	q.sample.queries.Add(1)
	q.sample.rows.Add(end.CommandTag.RowsAffected())
	if q.wire != nil {
		q.sample.received.Add(q.wire.received.Load() - q.r)
		q.sample.sent.Add(q.wire.sent.Load() - q.w)
	}
}

type loadPercentiles struct {
	Samples int   `json:"samples"`
	P50     int64 `json:"p50"`
	P95     int64 `json:"p95"`
	P99     int64 `json:"p99"`
	Max     int64 `json:"max"`
}

func loadQuantiles(values []int64) loadPercentiles {
	if len(values) == 0 {
		return loadPercentiles{}
	}
	slices.Sort(values)
	at := func(p int) int64 { return values[(p*len(values)+99)/100-1] }
	return loadPercentiles{len(values), at(50), at(95), at(99), values[len(values)-1]}
}

type loadMeasurement struct {
	latency, queries, rows, received, sent int64
}

type loadMeasurements struct {
	mu   sync.Mutex
	data map[string][]loadMeasurement
}

func (m *loadMeasurements) record(name string, sample *loadSample, began time.Time) {
	value := loadMeasurement{time.Since(began).Microseconds(), sample.queries.Load(), sample.rows.Load(), sample.received.Load(), sample.sent.Load()}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = make(map[string][]loadMeasurement)
	}
	m.data[name] = append(m.data[name], value)
}

func (m *loadMeasurements) summarize() map[string]map[string]loadPercentiles {
	result := make(map[string]map[string]loadPercentiles, len(m.data))
	for operation, samples := range m.data {
		values := make(map[string][]int64)
		for _, s := range samples {
			values["latency_us"] = append(values["latency_us"], s.latency)
			values["db_statements"] = append(values["db_statements"], s.queries)
			values["db_rows"] = append(values["db_rows"], s.rows)
			values["db_received_bytes"] = append(values["db_received_bytes"], s.received)
			values["db_sent_bytes"] = append(values["db_sent_bytes"], s.sent)
		}
		result[operation] = make(map[string]loadPercentiles, len(values))
		for metric, series := range values {
			result[operation][metric] = loadQuantiles(series)
		}
	}
	return result
}

type loadReport struct {
	Contract   string                                `json:"contract"`
	Run        string                                `json:"run"`
	Config     loadConfig                            `json:"config"`
	PostgreSQL string                                `json:"postgresql"`
	Passed     bool                                  `json:"passed"`
	ElapsedMS  int64                                 `json:"elapsed_ms"`
	Queue      []loadQueueSample                     `json:"queue"`
	QueueDepth loadPercentiles                       `json:"queue_depth_columns"`
	Operations map[string]map[string]loadPercentiles `json:"operations"`
	Routes     map[string]loadRouteStats             `json:"routes"`
	Plans      map[string]json.RawMessage            `json:"explain"`
	Recovery   map[string]any                        `json:"recovery"`
}

func newLoadReportDir(root string) (string, error) {
	if root == "" {
		root = os.TempDir()
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return "", err
	}
	worktree, err := filepath.Abs("../..")
	if err != nil {
		return "", err
	}
	worktree, err = filepath.EvalSymlinks(worktree)
	if err != nil {
		return "", err
	}
	if rel, err := filepath.Rel(worktree, root); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("report directory must be outside the worktree")
	}
	return os.MkdirTemp(root, "ouf-batch-load-")
}

func (r loadReport) write(dir string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(data, '\n'), 0600); err != nil {
		return err
	}
	var md strings.Builder
	fmt.Fprintf(&md, "# D-103 task G — qualifica di carico\n\nRun `%s`, profilo `%s`, PostgreSQL %s.\n\n", r.Run, r.Config.Profile, r.PostgreSQL)
	fmt.Fprintf(&md, "Processi %d, utenti %d, colonne %d, pool %d. Esito superato `%t`, durata %d ms.\n\n",
		r.Config.Processes, r.Config.Users, r.Config.Columns, r.Config.Connections, r.Passed, r.ElapsedMS)
	md.WriteString("| Operazione | Metrica | Campioni | p50 | p95 | p99 | Max |\n|---|---|---:|---:|---:|---:|---:|\n")
	operations := make([]string, 0, len(r.Operations))
	for op := range r.Operations {
		operations = append(operations, op)
	}
	slices.Sort(operations)
	for _, op := range operations {
		for _, metric := range []string{"latency_us", "db_statements", "db_rows", "db_received_bytes", "db_sent_bytes"} {
			p := r.Operations[op][metric]
			fmt.Fprintf(&md, "| %s | %s | %d | %d | %d | %d | %d |\n", op, metric, p.Samples, p.P50, p.P95, p.P99, p.Max)
		}
	}
	md.WriteString("\n## Coda\n\n| Fase | Pendenti | In esecuzione | Con due_at maturato | Età p50 ms | p95 | p99 |\n|---|---:|---:|---:|---:|---:|---:|\n")
	for _, q := range r.Queue {
		fmt.Fprintf(&md, "| %s | %d | %d | %d | %d | %d | %d |\n", q.Phase, q.Pending, q.Running, q.Due, q.AgeMS.P50, q.AgeMS.P95, q.AgeMS.P99)
	}
	fmt.Fprintf(&md, "\nProfondità p50/p95/p99 in colonne: %d / %d / %d (%d campioni).\n", r.QueueDepth.P50, r.QueueDepth.P95, r.QueueDepth.P99, r.QueueDepth.Samples)
	md.WriteString("\n## Quote e 429\n\n| Rotta | Chiamate | Inoltrate | Gateway 429 | Semantic 429 | Quota Gateway | Quota Semantic | Gateway p50/p95/p99 | Semantic p50/p95/p99 |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, route := range loadRoutes {
		s := r.Routes[route]
		fmt.Fprintf(&md, "| `%s` | %d | %d | %d | %d | %d | %d | %d/%d/%d | %d/%d/%d |\n", route,
			s.Calls, s.Forwarded, s.Gateway429, s.Semantic429, s.GatewayQuota, s.SemanticQuota,
			s.GatewayWindows.P50, s.GatewayWindows.P95, s.GatewayWindows.P99, s.SemanticWindows.P50, s.SemanticWindows.P95, s.SemanticWindows.P99)
	}
	recovery, err := json.MarshalIndent(r.Recovery, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintf(&md, "\n## Recupero\n\n```json\n%s\n```\n", recovery)
	for _, key := range []string{"offer_first", "offer_last", "summary_first", "summary_last"} {
		if plan := r.Plans[key]; len(plan) != 0 {
			fmt.Fprintf(&md, "\n## EXPLAIN %s\n\n```json\n%s\n```\n", key, plan)
		}
	}
	return os.WriteFile(filepath.Join(dir, "report.md"), []byte(md.String()), 0600)
}

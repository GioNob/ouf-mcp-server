//go:build batchload

package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
)

type loadQueueSample struct {
	Phase   string          `json:"phase"`
	At      time.Time       `json:"at"`
	Pending int64           `json:"pending_columns"`
	Running int64           `json:"running_columns"`
	Due     int64           `json:"due_columns"`
	AgeMS   loadPercentiles `json:"queued_analysis_age_ms"`
}

func readLoadQueue(ctx context.Context, store *postgres.Store, tenant, phase string, now time.Time) (loadQueueSample, error) {
	q := loadQueueSample{Phase: phase, At: now}
	rows, err := store.Pool().Query(ctx, `select count(*) filter(where b.state='PENDING'),
		count(*) filter(where b.state='RUNNING'),count(*) filter(where b.state='PENDING' and b.due_at<=$2),
		greatest(0,extract(epoch from ($2::timestamptz-p.created_at))*1000)::bigint
		from ouf_mcp.profile_analysis p join ouf_mcp.analysis_batch_column b using(analysis_id)
		where p.tenant_id=$1 and b.state in ('PENDING','RUNNING') group by p.analysis_id,p.created_at`, tenant, now)
	if err != nil {
		return q, err
	}
	defer rows.Close()
	var ages []int64
	for rows.Next() {
		var pending, running, eligible, age int64
		if err := rows.Scan(&pending, &running, &eligible, &age); err != nil {
			return q, err
		}
		q.Pending += pending
		q.Running += running
		q.Due += eligible
		ages = append(ages, age)
	}
	q.AgeMS = loadQuantiles(ages)
	return q, rows.Err()
}

type loadPlanNode struct {
	NodeType  string         `json:"Node Type"`
	Relation  string         `json:"Relation Name"`
	Index     string         `json:"Index Name"`
	Rows      float64        `json:"Actual Rows"`
	Loops     float64        `json:"Actual Loops"`
	Filtered  float64        `json:"Rows Removed by Filter"`
	Rechecked float64        `json:"Rows Removed by Index Recheck"`
	Children  []loadPlanNode `json:"Plans"`
}

func checkLoadPlan(root loadPlanNode, relation string, want, width int) error {
	if int(root.Rows) != want || root.Loops != 1 {
		return fmt.Errorf("returned %.0f rows in %.0f loops, want %d rows", root.Rows, root.Loops, want)
	}
	found := false
	var visit func(loadPlanNode) error
	visit = func(node loadPlanNode) error {
		if node.Relation == relation || node.Relation == "analysis_column" {
			visited := int(math.Ceil((node.Rows + node.Filtered + node.Rechecked) * node.Loops))
			indexed := node.Index != "" && strings.Contains(node.NodeType, "Index")
			if !indexed || visited > loadPageWidth || visited >= width {
				return fmt.Errorf("unbounded page access to %s: %s (%s), %d rows", node.Relation, node.NodeType, node.Index, visited)
			}
			found = found || node.Relation == relation
		}
		for _, child := range node.Children {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("missing indexed access to %s", relation)
	}
	return nil
}

func loadExplainPages(t *testing.T, store *postgres.Store, report *loadReport, firstID, lastID string, lastStart, width int) {
	t.Helper()
	ctx := t.Context()
	if _, err := store.Pool().Exec(ctx, `analyze ouf_mcp.analysis_batch_column,ouf_mcp.analysis_column,
		ouf_mcp.analysis_column_version,ouf_mcp.profile_analysis,ouf_mcp.analysis_counts`); err != nil {
		t.Fatal(err)
	}
	offerSQL, summarySQL := postgres.BatchQualificationPageSQL()
	for _, page := range []struct {
		name, id string
		first    int
	}{{"first", firstID, 0}, {"last", lastID, lastStart}} {
		var seq int64
		if err := store.Pool().QueryRow(ctx, `select max(seq) from ouf_mcp.analysis_counts where analysis_id=$1`, page.id).Scan(&seq); err != nil {
			t.Fatal(err)
		}
		want := min(loadPageWidth, width-page.first)
		for _, statement := range []struct {
			kind, relation, sql string
			args                []any
		}{
			{"offer", "analysis_batch_column", offerSQL, []any{page.id}},
			{"summary", "analysis_column_version", summarySQL, []any{page.id, page.first - 1, width, loadPageWidth, seq}},
		} {
			var raw []byte
			name := statement.kind + "_" + page.name
			if err := store.Pool().QueryRow(ctx, "explain (analyze, buffers, format json) "+statement.sql, statement.args...).Scan(&raw); err != nil {
				t.Errorf("%s: %v", name, err)
				continue
			}
			report.Plans[name] = raw
			var data []struct{ Plan loadPlanNode }
			if err := json.Unmarshal(raw, &data); err != nil || len(data) != 1 {
				t.Errorf("%s: invalid EXPLAIN JSON: %v", name, err)
				continue
			}
			if err := checkLoadPlan(data[0].Plan, statement.relation, want, width); err != nil {
				t.Errorf("%s: %v", name, err)
			}
		}
	}
}

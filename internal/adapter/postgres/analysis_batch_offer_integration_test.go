package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
)

func saveBatchFixturePreview(t *testing.T, ctx context.Context, s *Store, scope analysis.Scope, checksum string, start analysis.BatchStart) {
	t.Helper()
	for offset := 0; offset < start.Profile.ColumnCount; offset += 128 {
		page := make([]analysis.BatchPreviewColumn, min(128, start.Profile.ColumnCount-offset))
		for n := range page {
			name := fmt.Sprintf("school_%d", (offset+n)%42)
			page[n] = analysis.BatchPreviewColumn{Name: name, Intent: analysis.ColumnIntent(name), BatchColumnMetadata: analysis.BatchColumnMetadata{
				DataType: "string", Examples: []json.RawMessage{},
			}}
		}
		if err := s.SaveBatchPreviewPage(ctx, scope, start.ID, checksum, start.Epoch, start.Profile, offset, page); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProfileAnalysisBatchPostgresOfferQueryPlan(t *testing.T) {
	ctx, store, scope, profile, snapshot := analysisFixture(t, 1000)
	checksum, err := snapshot.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	id := analysis.BatchAnalysisID(scope, profile, "")
	start := analysis.BatchStart{ID: id, Profile: profile}
	saveBatchFixturePreview(t, ctx, store, scope, checksum, start)
	if _, err = store.SaveBatchStart(ctx, scope, checksum, uuid.NewString(), analysis.Digest(profile), start); err != nil {
		t.Fatal(err)
	}
	checkReady := func(first int) {
		t.Helper()
		_, err := store.pool.Exec(ctx, `update ouf_mcp.analysis_batch_column set state=
			case when ordinal between $2::int and $2::int+9 then 'READY_FOR_DECISION' else 'PENDING' end
			where analysis_id=$1`, id, first)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.pool.Exec(ctx, `analyze ouf_mcp.analysis_batch_column,ouf_mcp.analysis_column`); err != nil {
			t.Fatal(err)
		}
		plan := explainBatchQuery(t, ctx, store, batchReadyColumnsSQL, id)
		indexes := checkBatchPlan(t, plan, 8)
		if !slices.Contains(indexes, "analysis_batch_column_offer") || !slices.Contains(indexes, "analysis_column_pkey") ||
			!slices.Contains(indexes, "analysis_reformulation_budget_pkey") {
			t.Fatalf("ordinal %d used %v", first, indexes)
		}
		t.Logf("ordinal %d: %d rows, %v, %d shared hit blocks", first, plan.Rows, indexes, plan.Hits)
	}
	checkReady(0)
	checkReady(990)
	// Include one relevant budget and many unrelated profiles to exercise the
	// same lookup against a populated budget table.
	_, err = store.pool.Exec(ctx, `insert into ouf_mcp.analysis_reformulation_budget
		(service_principal_id,principal_id,tenant_id,asset_id,profile_id,ordinal,used)
		select $1::text,$2::text,$3::text,$4::uuid,gen_random_uuid(),0,1 from generate_series(1,3000)
		union all select $1,$2,$3,$4,$5,990,1`,
		scope.Service, scope.Principal, scope.Tenant, profile.AssetID, profile.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.pool.Exec(ctx, `analyze ouf_mcp.analysis_reformulation_budget`); err != nil {
		t.Fatal(err)
	}
	checkReady(990)
	// The active lookup must stay bounded even with retired offers.
	_, err = store.pool.Exec(ctx, `insert into ouf_mcp.analysis_batch_offer(analysis_id,batch_id,revision,items,retired_at)
		select $1,gen_random_uuid(),n,'[{}]'::jsonb,case when n<3000 then now() end from generate_series(0,3000) as n`, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.pool.Exec(ctx, `analyze ouf_mcp.analysis_batch_offer`); err != nil {
		t.Fatal(err)
	}
	plan := explainBatchQuery(t, ctx, store, batchActiveOfferSQL, id)
	if indexes := checkBatchPlan(t, plan, 1); !slices.Contains(indexes, "analysis_batch_active_offer") {
		t.Fatalf("active offer used %v", indexes)
	}
}

type batchQueryPlan struct {
	Node     string           `json:"Node Type"`
	Index    string           `json:"Index Name"`
	Relation string           `json:"Relation Name"`
	Rows     int              `json:"Actual Rows"`
	Removed  int              `json:"Rows Removed by Filter"`
	Hits     int              `json:"Shared Hit Blocks"`
	Plans    []batchQueryPlan `json:"Plans"`
}

func explainBatchQuery(t *testing.T, ctx context.Context, store *Store, query, id string) batchQueryPlan {
	t.Helper()
	var raw []byte
	err := store.pool.QueryRow(ctx, "explain (analyze,buffers,format json) "+query, id).Scan(&raw)
	var plans []struct{ Plan batchQueryPlan }
	if err != nil || json.Unmarshal(raw, &plans) != nil || len(plans) != 1 {
		t.Fatalf("EXPLAIN failed: %s (%v)", raw, err)
	}
	return plans[0].Plan
}

func checkBatchPlan(t *testing.T, plan batchQueryPlan, maximum int) []string {
	t.Helper()
	if plan.Rows != maximum {
		t.Fatalf("expected %d result rows, got %+v", maximum, plan)
	}
	var indexes []string
	var visit func(batchQueryPlan)
	visit = func(node batchQueryPlan) {
		if node.Node == "Seq Scan" || node.Rows+node.Removed > maximum {
			t.Fatalf("unbounded scan at %s: %+v", node.Relation, node)
		}
		if node.Index != "" {
			indexes = append(indexes, node.Index)
		}
		for _, child := range node.Plans {
			visit(child)
		}
	}
	visit(plan)
	return indexes
}

package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Run only through verify-release-isolated.sh, after the real old adapter has
// migrated and populated a disposable database. Ordinary CI tests skip this.
func TestReleaseUpgradeFromSeven(t *testing.T) {
	if os.Getenv("MCP_ISOLATED_UPGRADE") != "1" {
		t.Skip("isolated old-to-new release harness is required")
	}
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil || config.ConnConfig.Database != "ouf_release_acceptance" || config.ConnConfig.Host != "127.0.0.1" {
		t.Fatal("expected isolated loopback acceptance database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version, attempts int
	if err = s.Pool().QueryRow(ctx, `select max(version) from ouf_mcp.schema_migration`).Scan(&version); err != nil || version != 7 {
		t.Fatalf("expected real schema 007 before upgrade: version=%d err=%v", version, err)
	}
	if err = s.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.tool_attempt`).Scan(&attempts); err != nil || attempts == 0 {
		t.Fatal("old adapter must populate attempts before upgrade")
	}
	var checksum string
	if err = s.Pool().QueryRow(ctx, `select manifest_checksum from ouf_mcp.manifest_snapshot limit 1`).Scan(&checksum); err != nil {
		t.Fatal(err)
	}
	// A dedicated hour-long legacy window avoids minute-boundary races in the
	// cutover test. Seed it using only schema-007 columns before snapshotting.
	req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:legacy-upgrade")
	req.Window = time.Hour
	var now time.Time
	if err = s.Pool().QueryRow(ctx, `select transaction_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	start := now.Truncate(req.Window)
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end,consumed_tool_calls,consumed_result_bytes) values($1,$2,$3,$4,$5,$6,$7,2,24)`, uuid.New(), req.Identity.ServicePrincipalID, req.Identity.PrincipalID, req.Identity.TenantID, req.ManifestChecksum, start, start.Add(req.Window))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[string]string {
		t.Helper()
		rows, err := s.Pool().Query(ctx, `select tablename from pg_tables where schemaname='ouf_mcp' and tablename<>'schema_migration' order by tablename`)
		if err != nil {
			t.Fatal(err)
		}
		var tables []string
		for rows.Next() {
			var table string
			if err = rows.Scan(&table); err != nil {
				t.Fatal(err)
			}
			tables = append(tables, table)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		result := make(map[string]string)
		for _, table := range tables {
			var digest string
			// Omit only the additive columns, never old counters/state/evidence.
			query := `select md5(coalesce(string_agg(row::text, '' order by row::text), '')) from (select to_jsonb(t)-array['is_equivalent_retry','limit_tool_calls','limit_result_bytes','limit_distinct_objects'] as row from ` + pgx.Identifier{"ouf_mcp", table}.Sanitize() + ` t) snapshot`
			if err = s.Pool().QueryRow(ctx, query).Scan(&digest); err != nil {
				t.Fatal(err)
			}
			result[table] = digest
		}
		return result
	}
	before := snapshot()
	for pass := 0; pass < 2; pass++ {
		if err = Migrate(ctx, s.Pool()); err != nil {
			t.Fatal(err)
		}
		after := snapshot()
		if len(before) != len(after) {
			t.Fatal("unexpected table-set change")
		}
		for table, digest := range before {
			if after[table] != digest {
				t.Fatalf("migration/replay changed existing data in %s", table)
			}
		}
	}
	if err = s.Pool().QueryRow(ctx, `select max(version) from ouf_mcp.schema_migration`).Scan(&version); err != nil || version != 9 {
		t.Fatalf("upgrade did not reach schema 009: %d %v", version, err)
	}
	if _, err = s.Reserve(ctx, req); !errors.Is(err, ErrBudgetPolicyMismatch) {
		t.Fatalf("legacy window received new quota: %v", err)
	}
	// Replay one real old successful attempt through the new adapter.
	var prior uuid.UUID
	err = s.Pool().QueryRow(ctx, `select ta.attempt_id,ta.service_principal_id,ta.principal_id,ta.tenant_id,ta.capability_id,ta.operation_class,ta.manifest_checksum,ta.idempotency_key,ta.request_hash,ac.owner from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac using(attempt_id) where ta.state='SUCCEEDED' limit 1`).Scan(&prior, &req.Identity.ServicePrincipalID, &req.Identity.PrincipalID, &req.Identity.TenantID, &req.CapabilityID, &req.OperationClass, &req.ManifestChecksum, &req.IdempotencyKey, &req.RequestHash, &req.Owner)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.Reserve(ctx, req)
	if err != nil || !replay.Replay || replay.AttemptID != prior {
		t.Fatalf("old idempotency replay changed: %+v %v", replay, err)
	}
	// New identity/window receives the explicit new profile.
	req.Identity.TenantID = uuid.NewString()
	req.IdempotencyKey = uuid.NewString()
	req.WindowBudget = orchestration.DefaultWindowBudget()
	if _, err = s.Reserve(ctx, req); err != nil {
		t.Fatalf("new-policy admission failed: %v", err)
	}
	t.Log("UPGRADE_007_TO_009_PASS: old data preserved; migration replay, legacy deny and idempotency replay verified")
}

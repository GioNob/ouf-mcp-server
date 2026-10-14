package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The VPS already has the historical retry/window lineage at versions 8/9.
// Qualify the upgrade in a separate disposable database, leaving the normal
// integration database and any running producer untouched.
func TestDeployedLineageUpgrade(t *testing.T) {
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "mcp_lineage_" + uuid.NewString()[:8]
	if _, err = admin.Pool().Exec(ctx, "create database "+name); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		pool.Close()
		_, _ = admin.Pool().Exec(context.Background(), "drop database "+name+" with (force)")
	}()
	if _, err = pool.Exec(ctx, "create schema ouf_mcp; create table ouf_mcp.schema_migration(version integer primary key,checksum char(64) not null,applied_at timestamptz not null default transaction_timestamp())"); err != nil {
		t.Fatal(err)
	}
	prefix := []struct{ name, checksum string }{
		{"001_mcp_lifecycle.sql", "a70a2389352f5f8387f96b6051becf97f64f1e922aba9c353545449e0fa0b225"},
		{"002_governed_admission.sql", "138614ca67d9365e7525c0d56313ab1514e3ea9f1fe32b5095a3d85e3ecbb74d"},
		{"003_deterministic_recovery.sql", "044d5d6fe3fdd5f77179a4f677885aa3ca4f72ca1bc52f6f10c44b57306bfb3e"},
		{"004_evidence_compensation.sql", "9bae68eb83dabf8bd80fcfdf38189c54cced1228b3b22a7128c4c5cda3b7c72c"},
		{"005_maintenance_retention.sql", "3dc65eeee6668444e77e3a08982a71a1412fda53c3e8b90cbddd7d6959e5ad54"},
		{"006_audit_retention.sql", "8973286df97ef30dd7281dfaf5bfb251ca078d4a2862de235b6988bc5e5eb375"},
		{"007_referential_retention_chain.sql", "ad1e6b93b10ae2b6e407015f4bfb5026f2cd633a8565d30584fddd98e274f174"},
		{"008_retry_classification.sql", "2e279669f11a1b4949c93e17522d127003aa7c3f54cd2af0461500676f2bef28"},
		{"009_window_budget_limits.sql", "798ec17290d8952a6824871739d4b352bc3984f4e600fda9c704d6784d704dd2"},
	}
	for i, item := range prefix {
		raw, err := migrationFiles.ReadFile("migrations/" + item.name)
		if err != nil {
			t.Fatal(err)
		}
		if actual := fmt.Sprintf("%x", sha256.Sum256(raw)); actual != item.checksum {
			t.Fatalf("historical migration %d changed: %s", i+1, actual)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, string(raw)); err == nil {
			_, err = tx.Exec(ctx, "insert into ouf_mcp.schema_migration(version,checksum) values($1,$2)", i+1, item.checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err = Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var count int
	var cursor, bookmark bool
	if err = pool.QueryRow(ctx, "select count(*) from ouf_mcp.schema_migration").Scan(&count); err != nil || count != 12 {
		t.Fatalf("migration count %d: %v", count, err)
	}
	if err = pool.QueryRow(ctx, "select to_regclass('ouf_mcp.operational_cursor_session') is not null,to_regclass('ouf_mcp.operational_publication_bookmark') is not null").Scan(&cursor, &bookmark); err != nil || !cursor || !bookmark {
		t.Fatalf("operational tables %v/%v: %v", cursor, bookmark, err)
	}
	if err = Migrate(ctx, pool); err != nil {
		t.Fatalf("idempotent restart: %v", err)
	}
	if _, err = pool.Exec(ctx, "update ouf_mcp.schema_migration set checksum=$1 where version=8", fmt.Sprintf("%064d", 0)); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, pool); err == nil {
		t.Fatal("altered installed migration accepted")
	}
}

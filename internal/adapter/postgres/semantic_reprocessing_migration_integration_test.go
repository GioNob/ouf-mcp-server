package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pg_auth_members is cluster-wide; serialize fixtures within this test binary.
var atomicMigrationFixture sync.Mutex

type migrationFixture struct {
	pool *pgxpool.Pool
	role string
	oid  uint32
	hash string
}

func nativeMigrationFixture(t *testing.T) migrationFixture {
	t.Helper()
	atomicMigrationFixture.Lock()
	t.Cleanup(atomicMigrationFixture.Unlock)
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(admin.Close)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	database := "migration_" + suffix
	role := "migration_app_" + suffix
	if _, e = admin.Exec(ctx, "create role "+pgx.Identifier{role}.Sanitize()+" nologin nosuperuser nocreaterole"); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "drop role "+pgx.Identifier{role}.Sanitize()) })
	if _, e = admin.Exec(ctx, "create database "+pgx.Identifier{database}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "drop database "+pgx.Identifier{database}.Sanitize()+" with (force)")
	})
	config, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	config.ConnConfig.Database = database
	config.ConnConfig.RuntimeParams["application_name"] = database
	pool, e := pgxpool.NewWithConfig(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(pool.Close)
	if _, e = pool.Exec(ctx, "create schema ouf_mcp;create table ouf_mcp.schema_migration(version int primary key, checksum char(64) not null)"); e != nil {
		t.Fatal(e)
	}
	entries, e := migrationFiles.ReadDir("migrations")
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "013_") {
			continue
		}
		raw, e := migrationFiles.ReadFile("migrations/" + entry.Name())
		if e != nil {
			t.Fatal(e)
		}
		tx, e := pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec(ctx, string(raw)); e != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(e)
		}
		hash := sha256.Sum256(raw)
		if _, e = tx.Exec(ctx, "insert into ouf_mcp.schema_migration values($1,$2)", int(entry.Name()[0]-'0')*100+int(entry.Name()[1]-'0')*10+int(entry.Name()[2]-'0'), hex.EncodeToString(hash[:])); e != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
	}
	grant := "grant usage,create on schema ouf_mcp to " + pgx.Identifier{role}.Sanitize() + ";grant insert on ouf_mcp.schema_migration to " + pgx.Identifier{role}.Sanitize() + ";grant references on ouf_mcp.tool_attempt,ouf_mcp.budget_window to " + pgx.Identifier{role}.Sanitize()
	if _, e = pool.Exec(ctx, grant); e != nil {
		t.Fatal(e)
	}
	var oid uint32
	var hash string
	if e = pool.QueryRow(ctx, `select d.oid,encode(sha256(convert_to(pg_get_functiondef(to_regprocedure('ouf_mcp.purge_terminal_attempt_graph(timestamp with time zone,integer)')),'UTF8')),'hex') from pg_database d where d.datname=current_database()`).Scan(&oid, &hash); e != nil {
		t.Fatal(e)
	}
	return migrationFixture{pool, role, oid, hash}
}
func (f migrationFixture) assertState(t *testing.T, version int) {
	t.Helper()
	var maximum, members int
	var present bool
	e := f.pool.QueryRow(context.Background(), `select (select max(version) from ouf_mcp.schema_migration),(select count(*) from pg_auth_members where roleid=(select oid from pg_roles where rolname='ouf_mcp_retention_owner')),to_regclass('ouf_mcp.semantic_reprocessing_session')is not null`).Scan(&maximum, &members, &present)
	if e != nil || maximum != version || members != 0 || present != (version == 13) {
		t.Fatalf("migration/membership drift: %d %d %v %v", maximum, members, present, e)
	}
}
func TestSemanticReprocessingAtomicMigrationLeavesNoMembershipAndCannotReplay(t *testing.T) {
	f := nativeMigrationFixture(t)
	if e := MigrateSemanticReprocessing(context.Background(), f.pool, f.role, f.oid, f.hash); e != nil {
		t.Fatal(e)
	}
	f.assertState(t, 13)
	if e := MigrateSemanticReprocessing(context.Background(), f.pool, f.role, f.oid, f.hash); e == nil {
		t.Fatal("replayed 13 migration")
	}
	f.assertState(t, 13)
}
func TestSemanticReprocessingAtomicMigrationRejectsWrongDatabaseAndFunction(t *testing.T) {
	f := nativeMigrationFixture(t)
	if e := MigrateSemanticReprocessing(context.Background(), f.pool, f.role, f.oid+1, f.hash); e == nil {
		t.Fatal("wrong database admitted")
	}
	f.assertState(t, 12)
	if e := MigrateSemanticReprocessing(context.Background(), f.pool, f.role, f.oid, strings.Repeat("0", 64)); e == nil {
		t.Fatal("function drift admitted")
	}
	f.assertState(t, 12)
}
func TestSemanticReprocessingAtomicMigrationCancellationNeverPublishesMembership(t *testing.T) {
	f := nativeMigrationFixture(t)
	ctx := context.Background()
	locked, e := f.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer locked.Rollback(ctx)
	if _, e = locked.Exec(ctx, "lock table ouf_mcp.tool_attempt in access exclusive mode"); e != nil {
		t.Fatal(e)
	}
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- MigrateSemanticReprocessing(operation, f.pool, f.role, f.oid, f.hash) }()
	deadline := time.Now().Add(2 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		if e = f.pool.QueryRow(ctx, `select exists(select 1 from pg_stat_activity where datid=$1 and pid<>pg_backend_pid() and wait_event_type='Lock' and query like '%semantic_reprocessing_session%')`, f.oid).Scan(&waiting); e != nil {
			t.Fatal(e)
		}
		if waiting {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("fixture did not reach DDL after transaction-local membership")
	}
	// The other connection still sees the exact old role state while DDL waits.
	var members int
	if e = f.pool.QueryRow(ctx, "select count(*) from pg_auth_members where roleid=(select oid from pg_roles where rolname='ouf_mcp_retention_owner')").Scan(&members); e != nil || members != 0 {
		t.Fatalf("membership became externally visible: %d %v", members, e)
	}
	cancel()
	select {
	case e = <-done:
		if e == nil {
			t.Fatal("cancelled DDL committed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not finish")
	}
	if e = locked.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	f.assertState(t, 12)
}

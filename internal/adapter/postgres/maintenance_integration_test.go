package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDebtCacheReconciliationPostgres(t *testing.T) {
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	version := "maintenance-" + uuid.NewString()
	payload := []byte(fmt.Sprintf(`{"capabilities":[],"run":%q}`, version))
	checksum := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err = store.EnsureManifest(ctx, checksum, version, "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	window, group, attempt := uuid.New(), uuid.New(), uuid.New()
	principal := uuid.NewString()
	_, err = store.Pool().Exec(ctx, `insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end,current_unresolved_object_debt_total) values($1,'maintenance',$2,'tenant',$3,transaction_timestamp(),transaction_timestamp()+interval '1 hour',1)`, window, principal, checksum)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, `insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,'test','v1',$3)`, group, window, fmt.Sprintf("%064x", 2))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, `insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state,completed_at) values($1,'maintenance',$2,'tenant','test','READ',$3,$4,$5,$6,'UNRESOLVED','UNKNOWN',transaction_timestamp())`, attempt, principal, checksum, uuid.NewString(), fmt.Sprintf("%064x", 1), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, `insert into ouf_mcp.attempt_admission_context(attempt_id,budget_window_id,equivalence_group_id,owner,actor_type,authentication_context_ref,authorization_decision_ref,semantic_fingerprint,fingerprint_version) values($1,$2,$3,'udp','SERVICE','authn','authz',$4,'v1')`, attempt, window, group, fmt.Sprintf("%064x", 2))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, `insert into ouf_mcp.budget_object_debt(attempt_id,budget_window_id,debt_amount,debt_state) values($1,$2,4,'ACTIVE')`, attempt, window)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.ReconcileDebtCaches(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Repaired < 1 {
		t.Fatalf("expected at least one repair: %+v", result)
	}
	var cached int64
	if err = store.Pool().QueryRow(ctx, `select current_unresolved_object_debt_total from ouf_mcp.budget_window where budget_window_id=$1`, window).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	if cached != 4 {
		t.Fatalf("cached debt=%d want=4", cached)
	}
	var audits int
	if err = store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.audit_event where event_type='BUDGET_DEBT_CACHE_REPAIRED' and safe_detail->>'budgetWindowId'=$1`, window.String()).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("repair audits=%d want=1", audits)
	}
}

package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func maintenanceStore(t *testing.T) (*Store,context.Context) { t.Helper(); dsn:=os.Getenv("MCP_TEST_DATABASE_URL");if dsn==""{t.Skip("MCP_TEST_DATABASE_URL is not set")};ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second);t.Cleanup(cancel);s,err:=Open(ctx,dsn);if err!=nil{t.Fatal(err)};t.Cleanup(s.Close);if err=Migrate(ctx,s.Pool());err!=nil{t.Fatal(err)};return s,ctx }

func TestDebtCacheReconciliationConcurrentPostgres(t *testing.T) {
	s,ctx:=maintenanceStore(t); window:=uuid.New(); principal:=uuid.NewString(); checksum:=fmt.Sprintf("%064x",1)
	_,_ = s.Pool().Exec(ctx,`insert into ouf_mcp.manifest_snapshot(manifest_checksum,manifest_version,schema_version,payload) values($1,'m','mcp-manifest-v1','{}') on conflict do nothing`,checksum)
	_,err:=s.Pool().Exec(ctx,`insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end,current_unresolved_object_debt_total) values($1,'maintenance',$2,'tenant',$3,transaction_timestamp(),transaction_timestamp()+interval '1 hour',1)`,window,principal,checksum);if err!=nil{t.Fatal(err)}
	group,attempt:=uuid.New(),uuid.New();_,err=s.Pool().Exec(ctx,`insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,'test','v1',$3)`,group,window,fmt.Sprintf("%064x",2));if err!=nil{t.Fatal(err)}
	_,err=s.Pool().Exec(ctx,`insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state,completed_at) values($1,'maintenance',$2,'tenant','test','READ',$3,$4,$5,$6,'UNRESOLVED','UNKNOWN',transaction_timestamp())`,attempt,principal,checksum,uuid.NewString(),fmt.Sprintf("%064x",3),uuid.NewString());if err!=nil{t.Fatal(err)}
	_,err=s.Pool().Exec(ctx,`insert into ouf_mcp.attempt_admission_context(attempt_id,budget_window_id,equivalence_group_id,owner,actor_type,authentication_context_ref,authorization_decision_ref,semantic_fingerprint,fingerprint_version) values($1,$2,$3,'udp','SERVICE','authn','authz',$4,'v1')`,attempt,window,group,fmt.Sprintf("%064x",2));if err!=nil{t.Fatal(err)}
	_,err=s.Pool().Exec(ctx,`insert into ouf_mcp.budget_object_debt(attempt_id,budget_window_id,debt_amount,debt_state) values($1,$2,4,'ACTIVE')`,attempt,window);if err!=nil{t.Fatal(err)}
	var wg sync.WaitGroup; errs:=make(chan error,2);for i:=0;i<2;i++{wg.Add(1);go func(){defer wg.Done();_,e:=s.ReconcileDebtCaches(ctx,10);errs<-e}()};wg.Wait();close(errs);for e:=range errs{if e!=nil{t.Fatal(e)}}
	var cached int64;var audits int;if err=s.Pool().QueryRow(ctx,`select current_unresolved_object_debt_total from ouf_mcp.budget_window where budget_window_id=$1`,window).Scan(&cached);err!=nil{t.Fatal(err)};if cached!=4{t.Fatalf("cached=%d want=4",cached)};if err=s.Pool().QueryRow(ctx,`select count(*) from ouf_mcp.audit_event where event_type='BUDGET_DEBT_CACHE_REPAIRED' and safe_detail->>'budgetWindowId'=$1`,window.String()).Scan(&audits);err!=nil{t.Fatal(err)};if audits!=1{t.Fatalf("audits=%d want=1",audits)}
}

func TestMaintenanceRoleDeniedDirectProtectedDeletes(t *testing.T) {
	s,ctx:=maintenanceStore(t);_,err:=s.Pool().Exec(ctx,`set role ouf_mcp_maintenance_role`);if err!=nil{t.Fatal(err)};defer s.Pool().Exec(context.Background(),`reset role`)
	for _,q:=range []string{`delete from ouf_mcp.audit_event where false`,`delete from ouf_mcp.owner_evidence_inbox where false`,`delete from ouf_mcp.security_incident where false`}{if _,e:=s.Pool().Exec(ctx,q);e==nil{t.Fatalf("maintenance role unexpectedly allowed direct delete: %s",q)}}
}

func TestEvidenceRetentionFunctionIsBoundedAndRoleCallable(t *testing.T) {
	s,ctx:=maintenanceStore(t);var execute bool;if err:=s.Pool().QueryRow(ctx,`select has_function_privilege('ouf_mcp_maintenance_role','ouf_mcp.purge_owner_evidence(timestamptz,integer)','EXECUTE')`).Scan(&execute);err!=nil{t.Fatal(err)};if !execute{t.Fatal("maintenance role lacks retention function execute")}
	if _,err:=s.Pool().Exec(ctx,`set role ouf_mcp_maintenance_role`);err!=nil{t.Fatal(err)};defer s.Pool().Exec(context.Background(),`reset role`);var n int;if err:=s.Pool().QueryRow(ctx,`select ouf_mcp.purge_owner_evidence(transaction_timestamp()-interval '1 day',10)`).Scan(&n);err!=nil{t.Fatal(err)};if n!=0{t.Fatalf("unexpected purge count=%d",n)}
	if _,err:=s.Pool().Exec(ctx,`select ouf_mcp.purge_owner_evidence(transaction_timestamp()-interval '1 day',10001)`);err==nil{t.Fatal("unbounded retention batch accepted")}
}

func TestMaintenanceRoleHasNoTableOwnership(t *testing.T) {
	s,ctx:=maintenanceStore(t);cfg,err:=pgxpool.ParseConfig(os.Getenv("MCP_TEST_DATABASE_URL"));if err!=nil{t.Fatal(err)};_ = cfg
	var owns bool;if err=s.Pool().QueryRow(ctx,`select exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace join pg_roles r on r.oid=c.relowner where n.nspname='ouf_mcp' and r.rolname='ouf_mcp_maintenance_role')`).Scan(&owns);err!=nil{t.Fatal(err)};if owns{t.Fatal("maintenance role owns protected relation")}
}

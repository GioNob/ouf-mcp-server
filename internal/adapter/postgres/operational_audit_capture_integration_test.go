package postgres

import (
 "context"
 "encoding/json"
 "strings"
 "testing"
 "github.com/google/uuid"
 "github.com/jackc/pgx/v5"
)

func TestOperationalAuditCaptureLifecycleAndNoMetadataStorm(t *testing.T) {
 ctx,store,manifest:=controlStore(t)
 id:=uuid.New()
 _,err:=store.Pool().Exec(ctx,`insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state) values($1,'workload','operator',$2,'fixture','READ',$3,$4,$5,'private-correlation-secret','ADMITTED','NOT_DISPATCHED')`,id,"audit-"+id.String(),manifest,uuid.NewString(),strings.Repeat("a",64))
 if err!=nil {t.Fatal(err)}
 for _,sql:=range []string{
 `update ouf_mcp.tool_attempt set state='UNKNOWN',dispatch_state='UNKNOWN',unknown_since=statement_timestamp(),lock_version=lock_version+1 where attempt_id=$1`,
 `update ouf_mcp.tool_attempt set recovery_owner='worker',recovery_lease_until=statement_timestamp()+interval '1 minute',outcome_code='private-secret',lock_version=lock_version+1 where attempt_id=$1`,
 `update ouf_mcp.tool_attempt set state='UNRESOLVED',completed_at=statement_timestamp(),lock_version=lock_version+1 where attempt_id=$1`,
 `update ouf_mcp.tool_attempt set state='FAILED',outcome_code='private-terminal-secret',lock_version=lock_version+1 where attempt_id=$1`,
 } {if _,err=store.Pool().Exec(ctx,sql,id);err!=nil{t.Fatal(err)}}
 rows,err:=store.Pool().Query(ctx,`select event_type,safe_detail from ouf_mcp.audit_event where attempt_id=$1 and safe_detail->>'auditProfile'='ouf.mcp-operational-audit.v1' order by sequence_id`,id)
 if err!=nil{t.Fatal(err)};defer rows.Close()
 types:=[]string{"OPERATIONAL_ATTEMPT_UNKNOWN","OPERATIONAL_ATTEMPT_UNRESOLVED","OPERATIONAL_ATTEMPT_RECONCILED"}
 states:=[]string{"RECOVERING","OPEN","RESOLVED"}
 index:=0
 for rows.Next(){
  var kind string;var raw []byte
  if err=rows.Scan(&kind,&raw);err!=nil{t.Fatal(err)}
  var detail map[string]any
  if index>=3 || kind!=types[index] || json.Unmarshal(raw,&detail)!=nil || detail["lifecycleState"]!=states[index] || detail["incidentRef"]!=id.String() || detail["domain"]!="ATTEMPT_UNCERTAINTY" || strings.Contains(string(raw),"private-") {t.Fatal(kind,string(raw))}
  index++
 }
 if rows.Err()!=nil || index!=3 {t.Fatal("missing or repeated owner transition",index,rows.Err())}
 if _,err=store.Pool().Exec(ctx,`update ouf_mcp.audit_event set safe_detail='{}' where attempt_id=$1`,id);err==nil {t.Fatal("audit mutation accepted")}
}

func insertOperationalUnknown(ctx context.Context,tx pgx.Tx,manifest string,id uuid.UUID)error{
 _,err:=tx.Exec(ctx,`insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state,unknown_since) values($1,'workload','operator',$2,'fixture','READ',$3,$4,$5,'private-correlation-secret','UNKNOWN','UNKNOWN',statement_timestamp())`,id,"audit-"+id.String(),manifest,uuid.NewString(),strings.Repeat("a",64))
 return err
}
func TestOperationalAuditCaptureRollbackAndLateLowerSequence(t *testing.T){
 ctx,store,manifest:=controlStore(t)
 rollback,err:=store.Pool().Begin(ctx);if err!=nil{t.Fatal(err)}
 abandoned:=uuid.New()
 if err=insertOperationalUnknown(ctx,rollback,manifest,abandoned);err!=nil{t.Fatal(err)}
 if err=rollback.Rollback(ctx);err!=nil{t.Fatal(err)}
 var count int
 if err=store.Pool().QueryRow(ctx,`select count(*) from ouf_mcp.audit_event where attempt_id=$1`,abandoned).Scan(&count);err!=nil || count!=0 {t.Fatal("rolled back capture survived",count,err)}
 earlier,err:=store.Pool().Begin(ctx);if err!=nil{t.Fatal(err)};defer earlier.Rollback(ctx)
 first:=uuid.New();if err=insertOperationalUnknown(ctx,earlier,manifest,first);err!=nil{t.Fatal(err)}
 var firstSequence int64
 if err=earlier.QueryRow(ctx,`select sequence_id from ouf_mcp.audit_event where attempt_id=$1`,first).Scan(&firstSequence);err!=nil{t.Fatal(err)}
 later,err:=store.Pool().Begin(ctx);if err!=nil{t.Fatal(err)}
 second:=uuid.New();if err=insertOperationalUnknown(ctx,later,manifest,second);err!=nil{t.Fatal(err)}
 if err=later.Commit(ctx);err!=nil{t.Fatal(err)}
 var secondSequence int64
 if err=store.Pool().QueryRow(ctx,`select sequence_id from ouf_mcp.audit_event where attempt_id=$1`,second).Scan(&secondSequence);err!=nil{t.Fatal(err)}
 if firstSequence>=secondSequence {t.Fatal("fixture did not allocate earlier private sequence")}
 if err=store.Pool().QueryRow(ctx,`select count(*) from ouf_mcp.audit_event where attempt_id=$1`,first).Scan(&count);err!=nil || count!=0 {t.Fatal("uncommitted transition visible")}
 if err=earlier.Commit(ctx);err!=nil{t.Fatal(err)}
 if err=store.Pool().QueryRow(ctx,`select count(*) from ouf_mcp.audit_event where attempt_id=$1`,first).Scan(&count);err!=nil || count!=1 {t.Fatal("late committed transition missing")}
 // Audit sequence order is explicitly not a public completeness watermark.
}
func TestOperationalAuditCaptureSecurityDedupAndProtectedValuesExcluded(t *testing.T){
 ctx,store,manifest:=controlStore(t)
 tx,err:=store.Pool().Begin(ctx);if err!=nil{t.Fatal(err)}
 id:=uuid.New();if err=insertOperationalUnknown(ctx,tx,manifest,id);err!=nil{t.Fatal(err)};if err=tx.Commit(ctx);err!=nil{t.Fatal(err)}
 key:=uuid.NewString()+":private-security-key"
 for index:=0;index<2;index++{
  if _,err=store.Pool().Exec(ctx,`insert into ouf_mcp.security_incident(incident_key,attempt_id,category,detail_code,evidence_payload_hash) values($1,$2,'CRYPTO_VIOLATION','private-sensitive-detail',$3) on conflict do nothing`,key,id,strings.Repeat("f",64));err!=nil{t.Fatal(err)}
 }
 var raw []byte
 if err=store.Pool().QueryRow(ctx,`select safe_detail from ouf_mcp.audit_event where attempt_id=$1 and event_type='OPERATIONAL_SECURITY_ATTENTION'`,id).Scan(&raw);err!=nil{t.Fatal(err)}
 if strings.Contains(string(raw),"private-") || strings.Contains(string(raw),"CRYPTO_VIOLATION") || strings.Contains(string(raw),strings.Repeat("f",64)) {t.Fatal("protected values copied",string(raw))}
 var count int
 if err=store.Pool().QueryRow(ctx,`select count(*) from ouf_mcp.audit_event where attempt_id=$1 and event_type='OPERATIONAL_SECURITY_ATTENTION'`,id).Scan(&count);err!=nil || count!=1 {t.Fatal("duplicate incident became duplicate event",count,err)}
 var detail map[string]any
 if json.Unmarshal(raw,&detail)!=nil || detail["severity"]!="CRITICAL" || detail["actionRequired"]!=true {t.Fatal(string(raw))}
}
func TestOperationalAuditCaptureRoleCannotMutateOwners(t *testing.T){
 ctx,store,_:=controlStore(t)
 var login,attemptUpdate,auditUpdate,budgetUpdate,publicExecute bool
 if err:=store.Pool().QueryRow(ctx,`select rolcanlogin,has_table_privilege('ouf_mcp_operational_audit_owner','ouf_mcp.tool_attempt','UPDATE'),has_table_privilege('ouf_mcp_operational_audit_owner','ouf_mcp.audit_event','UPDATE'),has_table_privilege('ouf_mcp_operational_audit_owner','ouf_mcp.budget_window','UPDATE'),exists(select 1 from pg_proc p cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a where p.oid='ouf_mcp.capture_operational_attempt_audit()'::regprocedure and a.grantee=0 and a.privilege_type='EXECUTE') from pg_roles where rolname='ouf_mcp_operational_audit_owner'`).Scan(&login,&attemptUpdate,&auditUpdate,&budgetUpdate,&publicExecute);err!=nil {t.Fatal(err)}
 if login || attemptUpdate || auditUpdate || budgetUpdate || publicExecute {t.Fatal("observer has owner authority",login,attemptUpdate,auditUpdate,budgetUpdate,publicExecute)}
}

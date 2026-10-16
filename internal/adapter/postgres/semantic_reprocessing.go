package postgres

import (
 "context"
 "encoding/json"
 "errors"
 "time"

 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/GioNob/ouf-mcp-server/internal/reprocessing"
 "github.com/google/uuid"
 "github.com/jackc/pgx/v5"
)

// The caller already holds the ORIGINAL idempotency claim lock. It is never
// changed. The separate session is append-only and its internal admission key
// is never forwarded to the domain owner.
func(s *Store) reserveSemanticReprocessing(ctx context.Context,tx pgx.Tx,in orchestration.AdmissionRequest,oldID uuid.UUID)(orchestration.AdmissionDecision,error){
 denied:=func()(orchestration.AdmissionDecision,error){return orchestration.AdmissionDecision{},reprocessing.ErrPermit}
 v:=in.Reprocessing;if v==nil{return denied()};p:=v.Value()
 if !v.Matches(in.Identity.ServicePrincipalID,in.Identity.PrincipalID,in.Identity.TenantID,in.CapabilityID,in.Owner,in.OperationClass,in.IdempotencyKey)||in.Identity.ActorType!="HUMAN"||p.RequestHash!=in.RequestHash||p.ManifestChecksum!=in.ManifestChecksum||p.PredecessorAttemptID!=oldID.String()||in.Maximum.ToolCalls!=1||in.Maximum.ResultBytes<1||in.Maximum.ResultBytes>1048576||in.Maximum.DistinctObjects<0||in.Maximum.DistinctObjects>1||in.AttemptTimeout<=0||in.AttemptTimeout>30*time.Second{return denied()}
 sessionID,_:=uuid.Parse(p.SessionID)
 var recordedSession,newID uuid.UUID;var recordedHash,newState string
 err:=tx.QueryRow(ctx,`select r.session_id,r.permit_hash,r.new_attempt_id,a.state from ouf_mcp.semantic_reprocessing_session r join ouf_mcp.tool_attempt a on a.attempt_id=r.new_attempt_id where r.predecessor_attempt_id=$1`,oldID).Scan(&recordedSession,&recordedHash,&newID,&newState)
 if err==nil{
  if recordedSession!=sessionID||recordedHash!=v.Hash(){return denied()}
  if newState=="UNKNOWN"||newState=="UNRESOLVED"{return orchestration.AdmissionDecision{},ErrIdempotencyOutcomeUnknown}
  return orchestration.AdmissionDecision{AttemptID:newID,Replay:true},tx.Commit(ctx)
 }
 if !errors.Is(err,pgx.ErrNoRows){return orchestration.AdmissionDecision{},err}
 var now time.Time
 if err=tx.QueryRow(ctx,"select transaction_timestamp()").Scan(&now);err!=nil{return orchestration.AdmissionDecision{},err}
 if err=v.CheckTime(now,in.AttemptTimeout+time.Second);err!=nil{return orchestration.AdmissionDecision{},err}
 var oldWindow,oldGroup uuid.UUID;var state,dispatch,reservation,outcome,fingerprint,version string
 var lockVersion,actualCalls int64
 err=tx.QueryRow(ctx,`select ac.budget_window_id,ac.equivalence_group_id,a.state,a.dispatch_state,a.lock_version,b.state,b.actual_tool_calls,ac.semantic_fingerprint,ac.fingerprint_version,coalesce((select safe_detail->>'outcomeCode' from ouf_mcp.audit_event where attempt_id=a.attempt_id and event_type='TOOL_ATTEMPT_RECONCILED' order by sequence_id desc limit 1),'') from ouf_mcp.tool_attempt a join ouf_mcp.attempt_admission_context ac using(attempt_id) join ouf_mcp.budget_reservation b using(attempt_id) where a.attempt_id=$1`,oldID).Scan(&oldWindow,&oldGroup,&state,&dispatch,&lockVersion,&reservation,&actualCalls,&fingerprint,&version,&outcome)
 if err!=nil{return orchestration.AdmissionDecision{},err}
 if state!="FAILED"||dispatch!="ACKNOWLEDGED"||lockVersion!=p.PredecessorLockVersion||reservation!="RECONCILED"||actualCalls!=p.PriorToolCalls||outcome!=p.PredecessorOutcomeCode||fingerprint!=in.SemanticFingerprint||version!=in.FingerprintVersion{return denied()}
 // Preserve the global claim -> window -> guard -> attempt/reservation order.
 // A new session cannot override current debt or a blocked equivalence group.
 if _,err=tx.Exec(ctx,`select 1 from ouf_mcp.budget_window where budget_window_id=$1 for update`,oldWindow);err!=nil{return orchestration.AdmissionDecision{},err}
 var blocked bool;var blockers int
 if err=tx.QueryRow(ctx,`select blocked,blocking_attempts from ouf_mcp.retry_guard where equivalence_group_id=$1 for update`,oldGroup).Scan(&blocked,&blockers);err!=nil{return orchestration.AdmissionDecision{},err}
 if blocked||blockers!=0{return orchestration.AdmissionDecision{},ErrGroupLocked}
 var debt int64
 if err=tx.QueryRow(ctx,`select coalesce(sum(debt_amount),0) from ouf_mcp.budget_object_debt where budget_window_id=$1 and debt_state='ACTIVE'`,oldWindow).Scan(&debt);err!=nil{return orchestration.AdmissionDecision{},err}
 if debt!=0{return orchestration.AdmissionDecision{},ErrGroupLocked}
 windowID,groupID,attemptID:=uuid.New(),uuid.New(),uuid.New()
 admissionKey:="reprocess:"+p.SessionID
 _,err=tx.Exec(ctx,`insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end,reserved_tool_calls,reserved_result_bytes,reserved_distinct_objects,limit_tool_calls,limit_result_bytes,limit_distinct_objects) values($1,$2,$3,$4,$5,$6,$7,1,$8,$9,1,$8,$9)`,windowID,in.Identity.ServicePrincipalID,in.Identity.PrincipalID,in.Identity.TenantID,"ouf.semantic-reprocessing-one-call.v1:"+p.SessionID,time.Unix(p.IssuedAt,0),time.Unix(p.ExpiresAt,0),in.Maximum.ResultBytes,in.Maximum.DistinctObjects)
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,$3,$4,$5)`,groupID,windowID,in.CapabilityID,in.FingerprintVersion,in.SemanticFingerprint)}
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.retry_guard(equivalence_group_id,equivalent_attempts) values($1,1)`,groupID)}
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'ADMITTED','NOT_DISPATCHED')`,attemptID,in.Identity.ServicePrincipalID,in.Identity.PrincipalID,in.Identity.TenantID,in.CapabilityID,in.OperationClass,in.ManifestChecksum,admissionKey,in.RequestHash,in.CorrelationID)}
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.idempotency_claim(service_principal_id,principal_id,tenant_id,owner,capability_id,operation_class,idempotency_key,request_hash,attempt_id) values($1,$2,$3,$4,$5,$6,$7,$8,$9)`,in.Identity.ServicePrincipalID,in.Identity.PrincipalID,in.Identity.TenantID,in.Owner,in.CapabilityID,in.OperationClass,admissionKey,in.RequestHash,attemptID)}
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.attempt_admission_context(attempt_id,budget_window_id,equivalence_group_id,owner,actor_type,authentication_context_ref,authorization_decision_ref,semantic_fingerprint,fingerprint_version,is_equivalent_retry) values($1,$2,$3,$4,$5,$6,$7,$8,$9,true)`,attemptID,windowID,groupID,in.Owner,in.Identity.ActorType,in.Identity.AuthenticationContextRef,in.AuthorizationDecisionRef,in.SemanticFingerprint,in.FingerprintVersion)}
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.budget_reservation(reservation_id,attempt_id,budget_window_id,reserved_tool_calls,reserved_result_bytes,reserved_distinct_objects_max,state) values($1,$2,$3,1,$4,$5,'RESERVED')`,uuid.New(),attemptID,windowID,in.Maximum.ResultBytes,in.Maximum.DistinctObjects)}
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.semantic_reprocessing_session(session_id,permit_hash,predecessor_attempt_id,new_attempt_id,budget_window_id,decision_ref,human_subject,reason,prior_tool_calls,maximum_new_tool_calls,maximum_cumulative_tool_calls,owner_receipt_absent_at,issued_at,expires_at) values($1,$2,$3,$4,$5,$6,$7,$8,1,1,2,$9,$10,$11)`,sessionID,v.Hash(),oldID,attemptID,windowID,p.DecisionRef,p.HumanSubject,p.Reason,time.Unix(p.OwnerReceiptAbsentAt,0),time.Unix(p.IssuedAt,0),time.Unix(p.ExpiresAt,0))}
 detail,_:=json.Marshal(map[string]any{"sessionId":p.SessionID,"predecessorAttemptId":p.PredecessorAttemptID,"permitHash":v.Hash(),"decisionRef":p.DecisionRef,"priorToolCalls":1,"maximumNewToolCalls":1,"maximumCumulativeToolCalls":2})
 if err==nil{_,err=tx.Exec(ctx,`insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,service_principal_id,attempt_id,manifest_checksum,safe_detail) values($1,'SEMANTIC_REPROCESSING_ADMITTED','HUMAN',$2,$3,$4,$5::jsonb)`,uuid.New(),in.Identity.ServicePrincipalID,attemptID,in.ManifestChecksum,string(detail))}
 if err!=nil{return orchestration.AdmissionDecision{},err}
 if err=tx.Commit(ctx);err!=nil{return orchestration.AdmissionDecision{},err}
 return orchestration.AdmissionDecision{AttemptID:attemptID},nil
}

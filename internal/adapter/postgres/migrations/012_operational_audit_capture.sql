-- Private immutable operational evidence in the existing PET audit journal.
-- No publication watermark, public capability, grant or client receipt.
do $roles$ begin
 if not exists(select 1 from pg_roles where rolname='ouf_mcp_operational_audit_owner') then
  create role ouf_mcp_operational_audit_owner nologin;
 end if;
end $roles$;
grant usage on schema ouf_mcp to ouf_mcp_operational_audit_owner;
grant insert on ouf_mcp.audit_event to ouf_mcp_operational_audit_owner;
grant usage on sequence ouf_mcp.audit_event_sequence_id_seq to ouf_mcp_operational_audit_owner;
grant select(attempt_id,service_principal_id,manifest_checksum) on ouf_mcp.tool_attempt to ouf_mcp_operational_audit_owner;

create function ouf_mcp.capture_operational_attempt_audit()
returns trigger language plpgsql security definer
set search_path=pg_catalog,ouf_mcp as $$
declare
 lifecycle text;
 severity text;
 action_required boolean;
 code text;
 event_kind text;
begin
 if tg_op='UPDATE' then
  if new.state is not distinct from old.state then return new; end if;
  if new.state not in('UNKNOWN','UNRESOLVED') and old.state not in('UNKNOWN','UNRESOLVED') then return new; end if;
 elsif new.state not in('UNKNOWN','UNRESOLVED') then
  return new;
 end if;
 if new.state='UNKNOWN' then
  lifecycle:='RECOVERING'; severity:='WARNING'; action_required:=false;
  code:='ATTEMPT_OUTCOME_UNCERTAIN'; event_kind:='OPERATIONAL_ATTEMPT_UNKNOWN';
 elsif new.state='UNRESOLVED' then
  lifecycle:='OPEN'; severity:='ERROR'; action_required:=true;
  code:='ATTEMPT_RECONCILIATION_EXHAUSTED'; event_kind:='OPERATIONAL_ATTEMPT_UNRESOLVED';
 elsif new.state in('SUCCEEDED','FAILED') then
  lifecycle:='RESOLVED'; severity:='INFO'; action_required:=false;
  code:='ATTEMPT_UNCERTAINTY_CLEARED'; event_kind:='OPERATIONAL_ATTEMPT_RECONCILED';
 else
  -- This observer does not create or authorize a recovery transition.
  return new;
 end if;
 insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,service_principal_id,attempt_id,manifest_checksum,safe_detail,occurred_at)
 values(gen_random_uuid(),event_kind,'SYSTEM',new.service_principal_id,new.attempt_id,new.manifest_checksum,
 jsonb_build_object('auditProfile','ouf.mcp-operational-audit.v1','module','MCP','domain','ATTEMPT_UNCERTAINTY',
 'incidentRef',new.attempt_id::text,'lifecycleState',lifecycle,'severity',severity,'actionRequired',action_required,
 'errorCode',code,'ownerState',new.state,'ownerRevision',new.lock_version,
 'firstSeenAt',coalesce(new.unknown_since,new.admitted_at),'observedAt',statement_timestamp()),
 statement_timestamp());
 return new;
end $$;

create function ouf_mcp.capture_operational_security_audit()
returns trigger language plpgsql security definer
set search_path=pg_catalog,ouf_mcp as $$
declare workload text; manifest char(64);
begin
 if new.attempt_id is not null then
  select service_principal_id,manifest_checksum into workload,manifest
  from ouf_mcp.tool_attempt where attempt_id=new.attempt_id;
 end if;
 -- No category/detail/key/hash/result/payload, correlation or credential value
 -- is copied into this private safe projection. Protected evidence stays owner-local.
 insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,service_principal_id,attempt_id,manifest_checksum,safe_detail,occurred_at)
 values(gen_random_uuid(),'OPERATIONAL_SECURITY_ATTENTION','SYSTEM',workload,new.attempt_id,manifest,
 jsonb_build_object('auditProfile','ouf.mcp-operational-audit.v1','module','MCP','domain','SECURITY_EVIDENCE',
 'lifecycleState','OPEN','severity','CRITICAL','actionRequired',true,
 'errorCode','SECURITY_EVIDENCE_REQUIRES_REVIEW','observedAt',statement_timestamp()),statement_timestamp());
 return new;
end $$;

create trigger operational_attempt_audit after insert or update on ouf_mcp.tool_attempt
for each row execute function ouf_mcp.capture_operational_attempt_audit();
create trigger operational_security_audit after insert on ouf_mcp.security_incident
for each row execute function ouf_mcp.capture_operational_security_audit();

alter function ouf_mcp.capture_operational_attempt_audit() owner to ouf_mcp_operational_audit_owner;
alter function ouf_mcp.capture_operational_security_audit() owner to ouf_mcp_operational_audit_owner;
revoke all on function ouf_mcp.capture_operational_attempt_audit(),ouf_mcp.capture_operational_security_audit() from public;
-- This role is not granted to workload/maintenance/ingress/compensation roles.
-- It cannot update attempts, budgets, debts, evidence, grants or audit records.

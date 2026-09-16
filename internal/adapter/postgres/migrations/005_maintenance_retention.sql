do $roles$ begin
 if not exists(select 1 from pg_roles where rolname='ouf_mcp_maintenance_role') then create role ouf_mcp_maintenance_role nologin; end if;
end $roles$;

-- Evidence is retained while it is VERIFIED, referenced by debt/adjustment or
-- security evidence, or still attached to a non-terminal/late-compensable attempt.
create or replace function ouf_mcp.purge_owner_evidence(p_before timestamptz,p_limit integer)
returns integer language plpgsql security definer set search_path=pg_catalog,ouf_mcp as $$
declare n integer;
begin
 if p_limit is null or p_limit<1 or p_limit>10000 then raise exception 'invalid retention batch limit'; end if;
 if p_before is null or p_before>=transaction_timestamp() then raise exception 'invalid retention cutoff'; end if;
 with doomed as (
  select e.evidence_ref from ouf_mcp.owner_evidence_inbox e join ouf_mcp.tool_attempt t on t.attempt_id=e.attempt_id
  where e.created_at<p_before and e.evidence_state in('CONSUMED','QUARANTINED') and t.state in('SUCCEEDED','FAILED')
   and not exists(select 1 from ouf_mcp.budget_object_debt d where d.owner_evidence_ref=e.evidence_ref)
   and not exists(select 1 from ouf_mcp.budget_adjustment a where a.owner_evidence_ref=e.evidence_ref)
   and not exists(select 1 from ouf_mcp.security_incident i where i.evidence_ref=e.evidence_ref)
  order by e.created_at,e.evidence_ref limit p_limit for update of e skip locked)
 delete from ouf_mcp.owner_evidence_inbox e using doomed d where e.evidence_ref=d.evidence_ref;
 get diagnostics n=row_count; return n;
end $$;

revoke all on function ouf_mcp.purge_owner_evidence(timestamptz,integer) from public;
grant usage on schema ouf_mcp to ouf_mcp_maintenance_role;
grant execute on function ouf_mcp.purge_owner_evidence(timestamptz,integer) to ouf_mcp_maintenance_role;
revoke delete on ouf_mcp.audit_event,ouf_mcp.owner_evidence_inbox,ouf_mcp.security_incident from ouf_mcp_maintenance_role;

-- audit_event remains append-only in this increment. We deliberately do not
-- disable its immutable trigger globally: doing so would create a concurrent
-- mutation window. Audit purge stays pending until the dedicated definer path
-- can preserve the append-only invariant under concurrent workloads.

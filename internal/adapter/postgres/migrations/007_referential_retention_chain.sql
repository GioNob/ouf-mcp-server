-- PET-ordered retention for relations currently present in the MCP schema.
-- approval_link / alias relations are not present in this repository and are
-- therefore not invented by this migration.

grant select,delete on ouf_mcp.security_incident,
 ouf_mcp.budget_adjustment,
 ouf_mcp.budget_object_debt,
 ouf_mcp.owner_evidence_inbox,
 ouf_mcp.idempotency_claim,
 ouf_mcp.budget_reservation,
 ouf_mcp.attempt_admission_context,
 ouf_mcp.tool_attempt,
 ouf_mcp.retry_guard,
 ouf_mcp.budget_distinct_object,
 ouf_mcp.retry_equivalence_group,
 ouf_mcp.budget_window,
 ouf_mcp.application_session
 to ouf_mcp_retention_owner;
-- PostgreSQL requires UPDATE privilege for SELECT ... FOR UPDATE. This is
-- granted only to the NOLOGIN definer owner; no workload/maintenance role gets it.
grant update on ouf_mcp.tool_attempt to ouf_mcp_retention_owner;

create or replace function ouf_mcp.purge_terminal_attempt_graph(p_before timestamptz,p_limit integer)
returns integer
language plpgsql
security definer
set search_path=pg_catalog,ouf_mcp
as $$
declare n integer;
begin
 if p_limit is null or p_limit<1 or p_limit>1000 then
  raise exception 'invalid retention batch limit';
 end if;
 if p_before is null or p_before>=transaction_timestamp() then
  raise exception 'invalid retention cutoff';
 end if;

 create temporary table if not exists pg_temp.ouf_mcp_retention_attempts(attempt_id uuid primary key) on commit drop;
 truncate pg_temp.ouf_mcp_retention_attempts;

 insert into pg_temp.ouf_mcp_retention_attempts(attempt_id)
 select t.attempt_id
 from ouf_mcp.tool_attempt t
 where t.completed_at<p_before
   and t.state in('SUCCEEDED','FAILED')
   and not exists(select 1 from ouf_mcp.audit_event a where a.attempt_id=t.attempt_id)
   and not exists(select 1 from ouf_mcp.owner_evidence_inbox e where e.attempt_id=t.attempt_id and e.evidence_state='VERIFIED')
   and not exists(select 1 from ouf_mcp.budget_object_debt d where d.attempt_id=t.attempt_id and d.debt_state='ACTIVE')
 order by t.completed_at,t.attempt_id
 limit p_limit
 for update of t skip locked;

 delete from ouf_mcp.security_incident i using pg_temp.ouf_mcp_retention_attempts c where i.attempt_id=c.attempt_id;
 delete from ouf_mcp.budget_adjustment a using pg_temp.ouf_mcp_retention_attempts c where a.attempt_id=c.attempt_id;
 delete from ouf_mcp.budget_object_debt d using pg_temp.ouf_mcp_retention_attempts c where d.attempt_id=c.attempt_id and d.debt_state='RESOLVED';
 delete from ouf_mcp.owner_evidence_inbox e using pg_temp.ouf_mcp_retention_attempts c where e.attempt_id=c.attempt_id and e.evidence_state in('CONSUMED','QUARANTINED');
 delete from ouf_mcp.idempotency_claim i using pg_temp.ouf_mcp_retention_attempts c where i.attempt_id=c.attempt_id;
 delete from ouf_mcp.budget_reservation r using pg_temp.ouf_mcp_retention_attempts c where r.attempt_id=c.attempt_id and r.state in('RECONCILED','RELEASED');
 delete from ouf_mcp.attempt_admission_context x using pg_temp.ouf_mcp_retention_attempts c where x.attempt_id=c.attempt_id;
 delete from ouf_mcp.tool_attempt t using pg_temp.ouf_mcp_retention_attempts c where t.attempt_id=c.attempt_id;
 get diagnostics n=row_count;

 -- Parent records obey the same governed age cutoff as attempts. Becoming
 -- orphaned is necessary but never sufficient for early deletion.
 delete from ouf_mcp.retry_guard g
 using ouf_mcp.retry_equivalence_group q, ouf_mcp.budget_window w
 where g.equivalence_group_id=q.equivalence_group_id
   and q.budget_window_id=w.budget_window_id
   and w.window_end<p_before
   and not exists(select 1 from ouf_mcp.attempt_admission_context x where x.equivalence_group_id=g.equivalence_group_id);

 delete from ouf_mcp.budget_distinct_object o
 using ouf_mcp.budget_window w
 where o.budget_window_id=w.budget_window_id
   and w.window_end<p_before
   and not exists(select 1 from ouf_mcp.attempt_admission_context x where x.budget_window_id=o.budget_window_id)
   and not exists(select 1 from ouf_mcp.budget_object_debt d where d.budget_window_id=o.budget_window_id)
   and not exists(select 1 from ouf_mcp.budget_adjustment a where a.budget_window_id=o.budget_window_id);

 delete from ouf_mcp.retry_equivalence_group q
 using ouf_mcp.budget_window w
 where q.budget_window_id=w.budget_window_id
   and w.window_end<p_before
   and not exists(select 1 from ouf_mcp.attempt_admission_context x where x.equivalence_group_id=q.equivalence_group_id)
   and not exists(select 1 from ouf_mcp.retry_guard g where g.equivalence_group_id=q.equivalence_group_id);

 delete from ouf_mcp.budget_window w
 where w.window_end<p_before
   and not exists(select 1 from ouf_mcp.attempt_admission_context x where x.budget_window_id=w.budget_window_id)
   and not exists(select 1 from ouf_mcp.budget_reservation r where r.budget_window_id=w.budget_window_id)
   and not exists(select 1 from ouf_mcp.budget_object_debt d where d.budget_window_id=w.budget_window_id)
   and not exists(select 1 from ouf_mcp.budget_adjustment a where a.budget_window_id=w.budget_window_id)
   and not exists(select 1 from ouf_mcp.retry_equivalence_group q where q.budget_window_id=w.budget_window_id);

 delete from ouf_mcp.application_session s
 where s.expires_at<p_before
   and not exists(select 1 from ouf_mcp.tool_attempt t where t.application_session_id=s.application_session_id);

 return n;
end $$;

alter function ouf_mcp.purge_terminal_attempt_graph(timestamptz,integer) owner to ouf_mcp_retention_owner;
revoke all on function ouf_mcp.purge_terminal_attempt_graph(timestamptz,integer) from public;
grant execute on function ouf_mcp.purge_terminal_attempt_graph(timestamptz,integer) to ouf_mcp_maintenance_role;

-- manifest_snapshot remains append-only and is intentionally not purged here.
-- Its retention path stays EVIDENCE PENDING until deletion can preserve the
-- immutable-trigger invariant without a global bypass window.

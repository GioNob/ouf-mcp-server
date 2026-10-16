-- Explicitly authorized one-call admission session; never overwrite a prior
-- claim, attempt, reservation, outcome or consumed budget. The owner command
-- key remains unchanged. No runtime path creates or signs an authorization.
create table ouf_mcp.semantic_reprocessing_session(
 session_id uuid primary key,
 permit_hash char(64) not null check(permit_hash ~ '^[0-9a-f]{64}$'),
 predecessor_attempt_id uuid not null unique references ouf_mcp.tool_attempt(attempt_id),
 new_attempt_id uuid not null unique references ouf_mcp.tool_attempt(attempt_id),
 budget_window_id uuid not null unique references ouf_mcp.budget_window(budget_window_id),
 tenant_id text not null,
 decision_ref text not null,
 human_subject text not null,
 reason text not null,
 prior_tool_calls bigint not null check(prior_tool_calls=1),
 maximum_new_tool_calls bigint not null check(maximum_new_tool_calls=1),
 maximum_cumulative_tool_calls bigint not null check(maximum_cumulative_tool_calls=2),
 owner_receipt_absent_at timestamptz not null,
 issued_at timestamptz not null,
 expires_at timestamptz not null,
 created_at timestamptz not null default transaction_timestamp(),
 check(expires_at>issued_at and expires_at<=issued_at+interval '15 minutes'),
 check(owner_receipt_absent_at<=issued_at and owner_receipt_absent_at>=issued_at-interval '5 minutes')
);
create trigger semantic_reprocessing_session_immutable before update or delete
 on ouf_mcp.semantic_reprocessing_session for each row execute function ouf_mcp.reject_mutation();

create index semantic_reprocessing_tenant_capacity on ouf_mcp.semantic_reprocessing_session(tenant_id);

-- Preserve both sides of the cumulative authority chain. Independent terminal
-- graphs remain eligible for existing PET retention. Capacity is explicit in
-- the trusted deployment profile; exhaustion blocks new sessions, never purge.
grant select on ouf_mcp.semantic_reprocessing_session to ouf_mcp_retention_owner;
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
   and not exists(select 1 from ouf_mcp.semantic_reprocessing_session r where r.predecessor_attempt_id=t.attempt_id or r.new_attempt_id=t.attempt_id)
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

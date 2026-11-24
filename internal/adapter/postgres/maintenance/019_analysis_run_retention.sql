begin;
-- Also usable directly by the administrative installer after ordinary 019.
set local lock_timeout='3s';
set local statement_timeout='10s';
set local search_path=pg_catalog,pg_temp;
select pg_advisory_xact_lock(73012026);

do $install$
begin
 if not exists(select 1 from ouf_mcp.schema_migration where version=19) then
  raise exception 'analysis migration 019 required';
 end if;
 if not exists(select 1 from pg_roles where rolname='ouf_mcp_retention_owner'
               and not rolcanlogin and not rolsuper and not rolcreaterole)
 or exists(select 1 from pg_auth_members where roleid='ouf_mcp_retention_owner'::regrole) then
  raise exception 'unexpected retention owner authority';
 end if;
 if exists(select 1 from pg_proc
           where oid in (to_regprocedure('ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz)'),
                         to_regprocedure('ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer)'))
           and proowner<>'ouf_mcp_retention_owner'::regrole) then
  raise exception 'unexpected analysis retention function owner';
 end if;
end $install$;

-- Run with the existing maintenance role. The application never needs DELETE.
grant select on ouf_mcp.analysis_retention_eligible to ouf_mcp_retention_owner;
grant select,update,delete on ouf_mcp.analysis_run_read,ouf_mcp.analysis_run_snapshot,
 ouf_mcp.analysis_run_attempt to ouf_mcp_retention_owner;

create or replace function ouf_mcp.purge_analysis_run_history(p_workload text,p_before_attempt timestamptz,
 p_before_snapshot timestamptz,p_before_read timestamptz,p_limit integer)
returns table(reads integer,snapshots integer,attempts integer)
language plpgsql security definer set search_path=pg_catalog,pg_temp as $batch$
declare cutoff timestamptz;
begin
 if p_limit is null or p_limit not between 1 and 256 or nullif(p_workload,'') is null
 or p_before_attempt is null or p_before_snapshot is null or p_before_read is null
 or greatest(p_before_attempt,p_before_snapshot,p_before_read)>clock_timestamp()-interval '24 hours' then
  raise exception 'invalid analysis retention bounds';
 end if;
 cutoff := p_before_read;
 delete from ouf_mcp.analysis_run_read where ctid in (
  select a.ctid from ouf_mcp.analysis_run_read a join ouf_mcp.analysis_retention_eligible r using(analysis_id)
  where r.workload=p_workload and r.completed_at<=cutoff
  and a.served_at<=cutoff order by a.analysis_id,a.ctid limit p_limit for update of a skip locked
 );
 get diagnostics reads=row_count;
 cutoff := p_before_snapshot;
 delete from ouf_mcp.analysis_run_snapshot where ctid in (
  select a.ctid from ouf_mcp.analysis_run_snapshot a join ouf_mcp.analysis_retention_eligible r using(analysis_id)
  where r.workload=p_workload and r.completed_at<=cutoff
  and a.seq<>r.seq and a.captured_at<=cutoff and not exists (select 1 from ouf_mcp.analysis_run_read rd where rd.analysis_id=a.analysis_id and rd.seq=a.seq) order by a.analysis_id,a.ctid limit p_limit for update of a skip locked
 );
 get diagnostics snapshots=row_count;
 cutoff := p_before_attempt;
 delete from ouf_mcp.analysis_run_attempt where ctid in (
  select a.ctid from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_retention_eligible r using(analysis_id)
  where r.workload=p_workload and r.completed_at<=cutoff
  and greatest(a.finished_at,a.superseded_at,a.proof_until,a.lease_end)<=cutoff order by a.analysis_id,a.ctid limit p_limit for update of a skip locked
 );
 get diagnostics attempts=row_count;
 return next;
end $batch$;
alter function ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer) owner to ouf_mcp_retention_owner;
revoke all on function ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer) from public;
grant execute on function ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer) to ouf_mcp_maintenance_role;

-- Preserve the four-argument void API used by the predecessor.
create or replace function ouf_mcp.purge_analysis_run_history(p_workload text,p_before_attempt timestamptz,
 p_before_snapshot timestamptz,p_before_read timestamptz)
returns void language plpgsql security definer set search_path=pg_catalog,pg_temp as $purge$
begin
 perform * from ouf_mcp.purge_analysis_run_history(p_workload,p_before_attempt,p_before_snapshot,p_before_read,256);
end $purge$;
alter function ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz) owner to ouf_mcp_retention_owner;
revoke all on function ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz) from public;
grant execute on function ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz) to ouf_mcp_maintenance_role;
-- Default privileges on first creation and existing ACLs on replacement may
-- name additional callers. Do not publish the purge with those privileges.
do $acl$
begin
 if exists(select 1 from pg_proc p, aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
           where p.oid in ('ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz)'::regprocedure,
                           'ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer)'::regprocedure)
           and (a.is_grantable or a.grantee not in (p.proowner,'ouf_mcp_maintenance_role'::regrole))) then
  raise exception 'unexpected analysis retention execute privilege';
 end if;
end $acl$;
commit;

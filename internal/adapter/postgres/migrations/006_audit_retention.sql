do $roles$ begin
 if not exists(select 1 from pg_roles where rolname='ouf_mcp_retention_owner') then create role ouf_mcp_retention_owner nologin; end if;
end $roles$;

create or replace function ouf_mcp.reject_audit_mutation()
returns trigger language plpgsql set search_path=pg_catalog,ouf_mcp as $$
begin
 if tg_op='DELETE' and current_user='ouf_mcp_retention_owner' then return old; end if;
 raise exception 'append-only relation %',tg_table_name;
end $$;

drop trigger if exists audit_event_append_only on ouf_mcp.audit_event;
create trigger audit_event_append_only before update or delete on ouf_mcp.audit_event
for each row execute function ouf_mcp.reject_audit_mutation();

grant delete on ouf_mcp.audit_event to ouf_mcp_retention_owner;

create or replace function ouf_mcp.purge_audit_events(p_before timestamptz,p_limit integer)
returns integer language plpgsql security definer set search_path=pg_catalog,ouf_mcp as $$
declare n integer;
begin
 if p_limit is null or p_limit<1 or p_limit>10000 then raise exception 'invalid retention batch limit'; end if;
 if p_before is null or p_before>=transaction_timestamp() then raise exception 'invalid retention cutoff'; end if;
 with doomed as (
  select sequence_id from ouf_mcp.audit_event
  where occurred_at<p_before
  order by occurred_at,sequence_id
  limit p_limit
  for update skip locked
 )
 delete from ouf_mcp.audit_event a using doomed d where a.sequence_id=d.sequence_id;
 get diagnostics n=row_count;
 return n;
end $$;

alter function ouf_mcp.purge_audit_events(timestamptz,integer) owner to ouf_mcp_retention_owner;
revoke all on function ouf_mcp.purge_audit_events(timestamptz,integer) from public;
grant usage on schema ouf_mcp to ouf_mcp_retention_owner;
grant execute on function ouf_mcp.purge_audit_events(timestamptz,integer) to ouf_mcp_maintenance_role;
revoke delete on ouf_mcp.audit_event from ouf_mcp_maintenance_role;

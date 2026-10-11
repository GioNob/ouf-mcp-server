-- Additive owner-vector metadata only. No producer incident payload store or
-- operator checkpoint/receipt is created by a READ.
create table ouf_mcp.operational_cursor_session(
 session_id uuid primary key,
 principal_id text not null,tenant_id text not null,
 identity_fingerprint char(64) not null check(identity_fingerprint ~ '^[0-9a-f]{64}$'),
 policy_ref text not null,manifest_checksum text not null,
 profile text not null check(profile='ouf.operational-owner-vector.v1'),
 created_at timestamptz not null default clock_timestamp(),
 expires_at timestamptz not null default clock_timestamp()+interval '30 minutes',
 check(expires_at>created_at)
);
create index operational_cursor_session_scope on ouf_mcp.operational_cursor_session(tenant_id,principal_id);
create index operational_cursor_session_expiry on ouf_mcp.operational_cursor_session(expires_at);

create table ouf_mcp.operational_cursor_node(
 token uuid primary key,
 session_id uuid not null references ouf_mcp.operational_cursor_session(session_id) on delete cascade,
 page_number integer not null check(page_number between 0 and 200),
 metadata jsonb not null check(jsonb_typeof(metadata)='object' and octet_length(metadata::text)<=16384),
 page_digest char(64) check(page_digest ~ '^[0-9a-f]{64}$'),
 next_token uuid unique references ouf_mcp.operational_cursor_node(token) deferrable initially deferred,
 unique(session_id,page_number)
);

-- A once-committed result/continuation can never be replaced. Runtime expires
-- only whole sessions, after all remote I/O; snapshot expiry is not an ACK.
create function ouf_mcp.protect_operational_cursor_node() returns trigger
language plpgsql set search_path=pg_catalog,ouf_mcp as $$
begin
 if new.token<>old.token or new.session_id<>old.session_id or new.page_number<>old.page_number
    or new.metadata<>old.metadata or old.page_digest is not null
    or new.page_digest is null then
  raise exception 'immutable operational cursor node';
 end if;
 return new;
end $$;
create trigger operational_cursor_node_immutable before update on ouf_mcp.operational_cursor_node
for each row execute function ouf_mcp.protect_operational_cursor_node();

create function ouf_mcp.protect_operational_cursor_session() returns trigger
language plpgsql set search_path=pg_catalog,ouf_mcp as $$
begin
 raise exception 'immutable operational cursor session';
end $$;
create trigger operational_cursor_session_immutable before update on ouf_mcp.operational_cursor_session
for each row execute function ouf_mcp.protect_operational_cursor_session();

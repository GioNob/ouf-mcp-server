-- Persistent opaque owner positions only; no payload, grant, receipt or
-- operator checkpoint. Existing incident/cursor authorities are unchanged.
create table ouf_mcp.operational_publication_bookmark(
 token uuid primary key,
 parent_token uuid,
 principal_id text not null check(length(principal_id) between 1 and 200),
 tenant_id text not null check(length(tenant_id) between 1 and 200),
 identity_fingerprint char(64) not null check(identity_fingerprint ~ '^[0-9a-f]{64}$'),
 policy_ref text not null check(length(policy_ref) between 1 and 512),
 manifest_checksum char(64) not null check(manifest_checksum ~ '^[0-9a-f]{64}$'),
 profile text not null check(profile='ouf.operational-publication-vector.v1'),
 scope_digest char(64) not null check(scope_digest ~ '^[0-9a-f]{64}$'),
 vector_digest char(64) not null check(vector_digest ~ '^[0-9a-f]{64}$'),
 metadata jsonb not null check(jsonb_typeof(metadata)='object' and octet_length(metadata::text)<=4096),
 created_at timestamptz not null default statement_timestamp(),
 expires_at timestamptz not null default statement_timestamp()+interval '90 days',
 check(expires_at>created_at and expires_at<=created_at+interval '90 days')
);
create index operational_publication_bookmark_scope on ouf_mcp.operational_publication_bookmark(tenant_id,principal_id);
create index operational_publication_bookmark_expiry on ouf_mcp.operational_publication_bookmark(expires_at);
create index operational_publication_bookmark_equivalent on ouf_mcp.operational_publication_bookmark(tenant_id,principal_id,scope_digest,vector_digest);

create function ouf_mcp.protect_operational_publication_bookmark() returns trigger
language plpgsql set search_path=pg_catalog,ouf_mcp as $$
begin
 if tg_op='UPDATE' then raise exception 'immutable publication bookmark'; end if;
 if old.expires_at>clock_timestamp() then raise exception 'live publication bookmark'; end if;
 return old;
end $$;
create trigger operational_publication_bookmark_immutable before update or delete on ouf_mcp.operational_publication_bookmark
for each row execute function ouf_mcp.protect_operational_publication_bookmark();

-- Live empty pages can justify a different search even when completion counts
-- are unavailable. They do not change the summary's historical evidence rules.
alter table ouf_mcp.analysis_column add column if not exists empty_candidate_page boolean not null default false;

-- Count accepted changes of intent per immutable profile/ordinal across epochs.
create table if not exists ouf_mcp.analysis_reformulation_budget (
 service_principal_id text not null, principal_id text not null, tenant_id text not null,
 asset_id uuid not null, profile_id uuid not null, ordinal integer not null check (ordinal between 0 and 1023),
 used integer not null check (used between 1 and 16),
 primary key (service_principal_id, principal_id, tenant_id, asset_id, profile_id, ordinal)
);

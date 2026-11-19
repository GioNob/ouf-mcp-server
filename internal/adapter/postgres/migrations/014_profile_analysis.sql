create table ouf_mcp.profile_analysis (
 analysis_id char(64) primary key,
 service_principal_id text not null,
 principal_id text not null,
 tenant_id text not null,
 profile jsonb not null,
 manifest_checksum char(64) not null,
 revision bigint not null default 0,
 created_at timestamptz not null default transaction_timestamp()
);
create index profile_analysis_actor on ouf_mcp.profile_analysis(service_principal_id,principal_id,tenant_id);
create table ouf_mcp.profile_analysis_task (
 ordinal bigserial,
 analysis_id char(64) not null references ouf_mcp.profile_analysis(analysis_id),
 task_id char(64) not null,
 task jsonb not null,
 fence uuid,
 primary key(analysis_id,task_id)
);
create index profile_analysis_task_order on ouf_mcp.profile_analysis_task(analysis_id,ordinal);
-- No credential/delegation column. No cascade or TTL deletes retained work.

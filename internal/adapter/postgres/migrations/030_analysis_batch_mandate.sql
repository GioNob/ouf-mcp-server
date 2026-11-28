-- D-104. MCP owns checkpoints and opaque Gateway bindings. Authorization owns the registry.
create table ouf_mcp.analysis_batch_mandate (
 analysis_id char(64) primary key references ouf_mcp.profile_analysis(analysis_id),
 start_ref uuid not null unique,
 generation int not null check (generation=1),
 command_key uuid not null,
 start_binding text not null check (length(start_binding) between 1 and 16384),
 mandate_ref uuid unique,
 mandate_binding text check (length(mandate_binding) between 1 and 16384),
 state text not null default 'PENDING' check (state in ('PENDING','ACTIVE','REVOKED','EXPIRED','HELD')),
 reason_code text not null default '',
 control_due timestamptz,
 control_started timestamptz,
 control_attempts int not null default 0 check (control_attempts between 0 and 128),
 control_fence uuid,
 check (mandate_ref is distinct from start_ref),
 check (mandate_binding is null or mandate_ref is not null),
 check (state <> 'ACTIVE' or (mandate_ref is not null and mandate_binding is not null))
);
create index analysis_batch_mandate_control_due on ouf_mcp.analysis_batch_mandate(control_due,analysis_id)
 where state in ('PENDING','ACTIVE') and control_due is not null;

alter table ouf_mcp.analysis_batch_column
 add column mandate_arguments_hash char(64),
 add column mandate_not_before timestamptz,
 add column discovery_arguments_hash char(64),
 add column discovery_binding text;

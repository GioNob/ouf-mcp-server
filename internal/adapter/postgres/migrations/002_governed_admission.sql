create table ouf_mcp.budget_window(
 budget_window_id uuid primary key, service_principal_id text not null, principal_id text not null, tenant_id text not null,
 policy_ref text not null, window_start timestamptz not null, window_end timestamptz not null,
 reserved_tool_calls bigint not null default 0, consumed_tool_calls bigint not null default 0,
 reserved_result_bytes bigint not null default 0, consumed_result_bytes bigint not null default 0,
 unique(service_principal_id,principal_id,tenant_id,policy_ref,window_start), check(window_end>window_start));
create table ouf_mcp.retry_equivalence_group(
 equivalence_group_id uuid primary key, budget_window_id uuid not null references ouf_mcp.budget_window,
 capability_id text not null, fingerprint_version text not null, semantic_fingerprint text not null,
 unique(budget_window_id,capability_id,fingerprint_version,semantic_fingerprint));
create table ouf_mcp.retry_guard(
 equivalence_group_id uuid primary key references ouf_mcp.retry_equivalence_group,
 equivalent_attempts integer not null default 0, blocked boolean not null default false, updated_at timestamptz not null default transaction_timestamp());
create table ouf_mcp.idempotency_claim(
 service_principal_id text not null, principal_id text not null, tenant_id text not null, owner text not null,
 capability_id text not null, operation_class text not null, idempotency_key text not null,
 request_hash char(64) not null check(request_hash~'^[0-9a-f]{64}$'), attempt_id uuid not null unique,
 primary key(service_principal_id,principal_id,tenant_id,owner,capability_id,operation_class,idempotency_key));
create table ouf_mcp.attempt_admission_context(
 attempt_id uuid primary key references ouf_mcp.tool_attempt(attempt_id), budget_window_id uuid not null references ouf_mcp.budget_window,
 equivalence_group_id uuid not null references ouf_mcp.retry_equivalence_group, owner text not null, actor_type text not null,
 authentication_context_ref text not null, authorization_decision_ref text not null,
 semantic_fingerprint text not null, fingerprint_version text not null);
create table ouf_mcp.budget_reservation(
 reservation_id uuid primary key, attempt_id uuid not null unique references ouf_mcp.tool_attempt(attempt_id),
 budget_window_id uuid not null references ouf_mcp.budget_window, reserved_tool_calls bigint not null,
 reserved_result_bytes bigint not null, state text not null check(state in('RESERVED','RECONCILED')),
 created_at timestamptz not null default transaction_timestamp(), reconciled_at timestamptz);

alter table ouf_mcp.budget_window add column reserved_distinct_objects bigint not null default 0 check(reserved_distinct_objects>=0);
alter table ouf_mcp.budget_window add column consumed_distinct_objects bigint not null default 0 check(consumed_distinct_objects>=0);
alter table ouf_mcp.budget_window add column current_unresolved_object_debt_total bigint not null default 0 check(current_unresolved_object_debt_total>=0);
alter table ouf_mcp.budget_window add column allowed_object_hash_versions text[] not null default array['v1']::text[];
alter table ouf_mcp.budget_reservation add column reserved_distinct_objects_max bigint not null default 0 check(reserved_distinct_objects_max>=0);
alter table ouf_mcp.budget_reservation add column actual_distinct_objects bigint;
alter table ouf_mcp.budget_reservation add column actual_unresolved_object_debt_max bigint not null default 0 check(actual_unresolved_object_debt_max>=0 and actual_unresolved_object_debt_max<=reserved_distinct_objects_max);
alter table ouf_mcp.tool_attempt add column outcome_revision integer not null default 0 check(outcome_revision>=0);
alter table ouf_mcp.tool_attempt add constraint tool_attempt_backend_request_unique unique(backend_request_id);
alter table ouf_mcp.attempt_admission_context add constraint attempt_admission_context_scope_unique unique(attempt_id,budget_window_id);

create table ouf_mcp.owner_evidence_inbox(
 evidence_ref uuid primary key,attempt_id uuid not null references ouf_mcp.tool_attempt,budget_window_id uuid not null references ouf_mcp.budget_window,
 backend_owner text not null,backend_request_id text not null,evidence_kind text not null check(evidence_kind in('OWNER_RESULT','OWNER_PROVES_NO_DISPATCH')),
 terminal_state text not null check(terminal_state in('SUCCEEDED','FAILED')),terminal_outcome_code text not null,result_ref text,
 actual_distinct_objects bigint not null check(actual_distinct_objects>=0),object_hashes text[] not null,
 evidence_payload_hash char(64) not null check(evidence_payload_hash~'^[0-9a-f]{64}$'),verified_at timestamptz not null,verified_by text not null,
 evidence_state text not null default 'VERIFIED' check(evidence_state in('VERIFIED','CONSUMED','QUARANTINED')),consumed_at timestamptz,created_at timestamptz not null default transaction_timestamp(),
 unique(backend_owner,backend_request_id),check((evidence_state='VERIFIED' and consumed_at is null) or (evidence_state in('CONSUMED','QUARANTINED') and consumed_at is not null)),
 check(evidence_kind<>'OWNER_PROVES_NO_DISPATCH' or (terminal_state='FAILED' and terminal_outcome_code='DISPATCH_NOT_STARTED' and actual_distinct_objects=0 and cardinality(object_hashes)=0 and result_ref is null)),
 check(evidence_kind<>'OWNER_RESULT' or result_ref is not null));
create index owner_evidence_attempt_idx on ouf_mcp.owner_evidence_inbox(attempt_id,evidence_state);
alter table ouf_mcp.owner_evidence_inbox add constraint owner_evidence_scope_fk foreign key(attempt_id,budget_window_id) references ouf_mcp.attempt_admission_context(attempt_id,budget_window_id);

create table ouf_mcp.budget_object_debt(
 attempt_id uuid primary key references ouf_mcp.tool_attempt,budget_window_id uuid not null references ouf_mcp.budget_window,
 debt_amount bigint not null check(debt_amount>0),debt_state text not null check(debt_state in('ACTIVE','RESOLVED')),recovery_deadline timestamptz,
 owner_evidence_ref uuid references ouf_mcp.owner_evidence_inbox,created_at timestamptz not null default transaction_timestamp(),resolved_at timestamptz,lock_version bigint not null default 0,
 check((debt_state='ACTIVE' and resolved_at is null and owner_evidence_ref is null) or (debt_state='RESOLVED' and resolved_at is not null and owner_evidence_ref is not null)));
create index budget_object_debt_active_idx on ouf_mcp.budget_object_debt(budget_window_id,debt_state);
alter table ouf_mcp.budget_object_debt add constraint budget_object_debt_scope_fk foreign key(attempt_id,budget_window_id) references ouf_mcp.attempt_admission_context(attempt_id,budget_window_id);

create table ouf_mcp.budget_distinct_object(
 budget_window_id uuid not null references ouf_mcp.budget_window,object_hash_key_version text not null,object_id_hash varchar(640) not null,
 primary key(budget_window_id,object_hash_key_version,object_id_hash),
 check(object_id_hash~'^v[0-9]+:(hmac-sha256|hmac-sha384):[A-Za-z0-9_-]+$' and length(split_part(object_id_hash,':',3)) between 32 and 512),
 check(object_hash_key_version~'^v[0-9]+$' and split_part(object_id_hash,':',1)=object_hash_key_version));

create table ouf_mcp.budget_adjustment(
 adjustment_id uuid primary key,adjustment_key char(64) not null unique check(adjustment_key~'^[0-9a-f]{64}$'),budget_window_id uuid not null references ouf_mcp.budget_window,
 attempt_id uuid not null references ouf_mcp.tool_attempt,owner_evidence_ref uuid not null unique references ouf_mcp.owner_evidence_inbox,
 prior_outcome_revision integer not null check(prior_outcome_revision>=0),new_outcome_revision integer not null check(new_outcome_revision=prior_outcome_revision+1),
 distinct_object_delta bigint not null check(distinct_object_delta>=0),unresolved_debt_delta bigint not null check(unresolved_debt_delta<=0),created_at timestamptz not null default transaction_timestamp(),
 unique(attempt_id,new_outcome_revision),check(distinct_object_delta+unresolved_debt_delta<=0));
alter table ouf_mcp.budget_adjustment add constraint budget_adjustment_scope_fk foreign key(attempt_id,budget_window_id) references ouf_mcp.attempt_admission_context(attempt_id,budget_window_id);

create table ouf_mcp.security_incident(
 incident_key text primary key,attempt_id uuid references ouf_mcp.tool_attempt,evidence_ref uuid references ouf_mcp.owner_evidence_inbox,
 category text not null check(category in('EVIDENCE_SCOPE_VIOLATION','EVIDENCE_BINDING_VIOLATION','CONTRACT_VIOLATION','CRYPTO_VIOLATION')),
 detail_code text not null,evidence_payload_hash char(64),created_at timestamptz not null default transaction_timestamp());

do $roles$ begin
 if not exists(select 1 from pg_roles where rolname='ouf_mcp_evidence_ingress_role') then create role ouf_mcp_evidence_ingress_role nologin; end if;
 if not exists(select 1 from pg_roles where rolname='ouf_mcp_compensation_role') then create role ouf_mcp_compensation_role nologin; end if;
end $roles$;
revoke all on ouf_mcp.owner_evidence_inbox,ouf_mcp.budget_adjustment,ouf_mcp.security_incident from public;
revoke all on ouf_mcp.owner_evidence_inbox,ouf_mcp.budget_adjustment,ouf_mcp.security_incident from ouf_mcp_evidence_ingress_role,ouf_mcp_compensation_role;
grant insert on ouf_mcp.owner_evidence_inbox to ouf_mcp_evidence_ingress_role;
grant select(evidence_ref,backend_owner,backend_request_id,evidence_payload_hash) on ouf_mcp.owner_evidence_inbox to ouf_mcp_evidence_ingress_role;
grant select,update on ouf_mcp.owner_evidence_inbox,ouf_mcp.budget_object_debt to ouf_mcp_compensation_role;
grant select,insert on ouf_mcp.budget_adjustment,ouf_mcp.security_incident,ouf_mcp.budget_distinct_object to ouf_mcp_compensation_role;

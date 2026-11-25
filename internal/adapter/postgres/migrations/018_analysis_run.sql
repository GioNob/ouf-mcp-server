-- T2 records live outside the predecessor's plan/task and attempt states.
create table ouf_mcp.analysis_run (
 analysis_id text primary key check (analysis_id ~ '^[0-9a-f]{64}$'),
 contract_version text not null default 'ouf.analysis.run.v2' check(contract_version='ouf.analysis.run.v2'),
 budget_profile text not null default 'analysis-run-v2' check(budget_profile='analysis-run-v2'),
 issuer text not null, workload text not null, tenant text not null, subject text not null,
 checkpoint jsonb not null, counts jsonb not null, spec jsonb not null, started timestamptz not null, budget_end timestamptz not null,
 deadline timestamptz not null, not_before timestamptz not null, mandate jsonb,
 mandate_state text not null default 'ISSUING' check(mandate_state in ('ISSUING','ACTIVE','REVOKED','EXPIRED','DENIED')),
 state text not null check (state in ('ISSUING','RUNNING','COMPLETE','PARTIAL','BLOCKED')),
 reason text not null default '', seq bigint not null default 0 check (seq >= 0),
 issue_attempts integer not null default 0 check (issue_attempts between 0 and 128),
 poll_seq bigint, poll_after timestamptz, fence bigint not null default 0
);
create index analysis_run_subject on ouf_mcp.analysis_run(issuer,tenant,subject)
 where state in ('ISSUING','RUNNING');
create table ouf_mcp.analysis_run_command (
 issuer text not null, workload text not null, tenant text not null, subject text not null,
 command_key uuid not null, analysis_id text not null references ouf_mcp.analysis_run,
 primary key (issuer,workload,tenant,subject,command_key)
);
create table ouf_mcp.analysis_run_attempt (
 attempt_id uuid primary key, analysis_id text not null references ouf_mcp.analysis_run,
 fence bigint not null, generation text not null, mandate_id uuid not null, intent_key text not null,
 operation text not null, arguments_hash text not null, ordinals_ref text not null,
 ordinals integer[] not null, request_id uuid, provider text not null,
 issuer text not null, tenant text not null, subject text not null, worker uuid not null,
 reserved_at timestamptz not null, lease_end timestamptz not null,
 calls bigint not null, attempts bigint not null, objects bigint not null, bytes bigint not null, millis bigint not null,
 crossing_decision text, crossing_correlation text, crossing_hash text, crossing_request uuid,
 owner_key_hash text, receipt_hash text, decision_ref text, correlation_ref text, outcome_code text, superseded_at timestamptz,
 proof_until timestamptz, actual jsonb, finished_at timestamptz, unique (analysis_id,fence),
 check (lease_end > reserved_at and calls=1 and attempts=1 and objects>=0 and bytes>0 and millis>0),
 check ((finished_at is null) = (actual is null))
);
create index analysis_run_provider on ouf_mcp.analysis_run_attempt(issuer,tenant,subject,provider,lease_end);
create table ouf_mcp.analysis_run_request (
 analysis_id text not null references ouf_mcp.analysis_run,
 request_id uuid not null, intent_key text not null, finished_at timestamptz, primary key (analysis_id,request_id),
 unique (analysis_id,intent_key)
);

create table ouf_mcp.analysis_run_outcome (
 analysis_id text not null references ouf_mcp.analysis_run, ordinal integer not null check (ordinal between 0 and 1023),
 seq bigint not null, outcome jsonb not null check (ouf_mcp.analysis_outcome_valid(outcome)),
 primary key(analysis_id,ordinal,seq)
);
create table ouf_mcp.analysis_run_snapshot (
 analysis_id text not null references ouf_mcp.analysis_run, seq bigint not null, counts jsonb not null,
 state text not null, reason text not null, primary key(analysis_id,seq)
);

-- Compact counts are versioned for snapshot pagination, independently of the work queue.
create function ouf_mcp.analysis_run_save_snapshot() returns trigger language plpgsql as $$
begin
 insert into ouf_mcp.analysis_run_snapshot values(new.analysis_id,new.seq,new.counts,new.state,new.reason);
 return new;
end; $$;
create trigger analysis_run_snapshot_write after insert or update of seq on ouf_mcp.analysis_run
 for each row execute function ouf_mcp.analysis_run_save_snapshot();

create table ouf_mcp.analysis_run_read (
 analysis_id text not null, seq bigint not null, decision_ref text not null,
 first_ordinal int not null,last_ordinal int not null, served_at timestamptz not null default clock_timestamp(),
 foreign key(analysis_id,seq) references ouf_mcp.analysis_run_snapshot
);

create table ouf_mcp.analysis_run_incident (
 analysis_id text primary key references ouf_mcp.analysis_run,
 incident_id text not null unique, state text not null check(state in ('OPEN','RECOVERING','RESOLVED'))
);
create table ouf_mcp.analysis_run_budget_session (
 analysis_id text not null references ouf_mcp.analysis_run, incident_id text not null references ouf_mcp.analysis_run_incident(incident_id),
 expected_seq bigint not null, actor text not null, reason text not null, decision_ref text not null,
 maximum jsonb not null, started timestamptz not null, ends_at timestamptz not null, first_fence bigint not null,
 primary key(analysis_id,expected_seq), unique(analysis_id,decision_ref), check(ends_at>started)
);
create index analysis_run_rate on ouf_mcp.analysis_run_attempt(reserved_at);
create index analysis_run_due on ouf_mcp.analysis_run((spec->>'Workload'),not_before,analysis_id) where state='RUNNING';

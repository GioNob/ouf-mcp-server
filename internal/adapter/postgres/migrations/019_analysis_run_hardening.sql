-- A NULL status_due means the plan can compete for a dispatch reservation.
alter table ouf_mcp.analysis_run
 add column status_due timestamptz,
 add column status_valid_until timestamptz,
 add column status_token uuid,
 add column status_failures integer not null default 0,
 add column status_decision text;
-- Include pauses inherited from 018 even if not_before passed during upgrade.
update ouf_mcp.analysis_run set status_due=transaction_timestamp()
 where state='RUNNING' and (reason in ('ANALYSIS_BACKOFF','ANALYSIS_RECONCILING')
 or not_before>transaction_timestamp() or checkpoint->'Queue'->0->'Task'->>'notBefore' > '0001-01-01T00:00:00Z');
create index analysis_run_status_due on ouf_mcp.analysis_run((spec->>'Workload'),status_due)
 where state='RUNNING' and status_due is not null;

alter table ouf_mcp.analysis_run_snapshot add column captured_at timestamptz not null default now();
create index analysis_run_read_snapshot on ouf_mcp.analysis_run_read(analysis_id,seq);
-- An exhausted group retains its original work and checkpoint for governed review.
create table ouf_mcp.analysis_group_quarantine (
 analysis_id text not null references ouf_mcp.analysis_run,
 intent_key text not null, incident_id text not null unique, item jsonb not null,
 opened_at timestamptz not null, reason text not null check(reason in ('ANALYSIS_BUDGET_EXHAUSTED','ANALYSIS_EFFECT_UNCERTAIN')),
 state text not null default 'OPEN' check(state in ('OPEN','RECOVERING','RESOLVED')), primary key(analysis_id,intent_key)
);
-- Only closed dependency/cause codes and opaque scope keys enter these records.
create table ouf_mcp.analysis_dependency_incident (
 incident_id uuid primary key, scope_key text not null check(scope_key ~ '^[0-9a-f]{64}$'), dependency text not null,
 cause text not null check((dependency='semantic' and cause in ('SEM_BATCH_UNAVAILABLE','SEM_BATCH_RATE_LIMITED'))
 or (dependency='gateway' and cause='ANALYSIS_AUTHORITY_UNAVAILABLE')), opened_at timestamptz not null, resolved_at timestamptz
);
create unique index analysis_dependency_open on ouf_mcp.analysis_dependency_incident(scope_key,dependency,cause)
 where resolved_at is null;
create table ouf_mcp.analysis_dependency_impact (
 incident_id uuid not null references ouf_mcp.analysis_dependency_incident,
 analysis_id text not null references ouf_mcp.analysis_run, group_key text not null check(group_key='' or group_key ~ '^[0-9a-f]{64}$'),
 ordinal integer not null check(ordinal between 0 and 1023),
 recovered_at timestamptz, primary key(incident_id,analysis_id,group_key,ordinal)
);
create index analysis_dependency_affected_plan on ouf_mcp.analysis_dependency_impact(analysis_id) where recovered_at is null;
create view ouf_mcp.analysis_dependency_counts as
 select i.incident_id,i.scope_key,i.dependency,i.cause,i.opened_at,i.resolved_at,
 count(distinct a.analysis_id) plans,
 count(distinct (a.analysis_id,a.group_key)) filter(where a.group_key<>'') groups,
 count(distinct (a.analysis_id,a.ordinal)) ordinals
 from ouf_mcp.analysis_dependency_incident i join ouf_mcp.analysis_dependency_impact a using(incident_id)
 group by i.incident_id;

-- Join against the terminal snapshot as well as the age of each child.
create view ouf_mcp.analysis_retention_eligible as
 select r.analysis_id,r.seq,r.spec->>'Workload' workload,last.captured_at completed_at
 from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_snapshot last on last.analysis_id=r.analysis_id and last.seq=r.seq
 where (r.state in ('COMPLETE','PARTIAL') or (r.state='BLOCKED' and r.counts->>'pending'='0'
  and r.checkpoint->'Queue'='[]'::jsonb))
  and not exists(select 1 from ouf_mcp.analysis_run_attempt u where u.analysis_id=r.analysis_id and u.finished_at is null and u.superseded_at is null)
  and not exists(select 1 from ouf_mcp.analysis_run_request q where q.analysis_id=r.analysis_id and q.finished_at is null)
  and not exists(select 1 from ouf_mcp.analysis_group_quarantine q where q.analysis_id=r.analysis_id)
  and not exists(select 1 from ouf_mcp.analysis_dependency_impact a where a.analysis_id=r.analysis_id and a.recovered_at is null)
  and not exists(select 1 from ouf_mcp.analysis_run_incident i where i.analysis_id=r.analysis_id and i.state<>'RESOLVED');

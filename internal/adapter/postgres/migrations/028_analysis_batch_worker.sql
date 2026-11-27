-- D-103. Predecessor source: 791862dbe0a88c8b61a45a19bc8d766083ba6f6a (66b1).
-- Depends on 027 and the owner observation and idempotency tables in 015/020.
-- Activated by the BATCH_V1 worker. The predecessor has no dispatch path for these rows.
alter table ouf_mcp.analysis_batch_column
 add column work_stage text not null default 'PUBLISHED'
   check (work_stage in ('PUBLISHED','REQUEST','STATUS','CANDIDATES')),
 add column fence uuid,
 add column dispatch_fence uuid,
 add column retry_attempts int not null default 0 check (retry_attempts between 0 and 128),
 add column retry_started_at timestamptz,
 add column owner_deadline timestamptz,
 add column completed_count int check (completed_count >= 0),
 add column published_solo boolean not null default false;

create index analysis_batch_work_due on ouf_mcp.analysis_batch_column(analysis_id,work_stage,due_at,ordinal)
 where state in ('PENDING','RUNNING') and due_at is not null;
create index analysis_batch_published_due on ouf_mcp.analysis_batch_column(analysis_id,due_at,ordinal)
 where work_stage='PUBLISHED' and state='PENDING' and not published_solo and due_at is not null;
create index analysis_batch_live_lease on ouf_mcp.analysis_batch_column(due_at,fence,analysis_id)
 where state='RUNNING' and fence is not null;

alter table ouf_mcp.idempotency_claim add column batch_discovery_worker boolean not null default false;

-- The Gateway owns quota. A refusal can move the next dispatch on that route
-- forward across replicas. There is no configured quota rate in this table.
create table ouf_mcp.analysis_batch_route_pause (
 work_stage text primary key,
 due_at timestamptz not null
);

create table ouf_mcp.analysis_batch_traffic (
 work_stage text not null,
 metric text not null check (metric in ('429_SEMANTIC_CODE','429_UNATTESTED','GET_USEFUL','GET_REPEATED','GET_EXCLUDED')),
 observations bigint not null check (observations > 0),
 primary key (work_stage,metric)
);

-- One durable open hold per analysis ordinal and work stage. Holds retain
-- their fence, code and consumed budget until governed reconciliation.
create table ouf_mcp.analysis_batch_hold (
 analysis_id char(64) not null,
 ordinal int not null,
 work_stage text not null,
 fence uuid not null,
 reason_code text not null,
 retry_started_at timestamptz,
 retry_attempts int not null,
 opened_at timestamptz not null,
 primary key (analysis_id,ordinal,work_stage),
 foreign key (analysis_id,ordinal) references ouf_mcp.analysis_batch_column
);
create index analysis_batch_hold_reason on ouf_mcp.analysis_batch_hold(analysis_id,reason_code,ordinal);

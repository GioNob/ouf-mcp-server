-- D-103. Predecessor source: 2ce2746ce5f77636a33b4eb016281ae2cd3b4296 (66a).
-- Depends on 014 and 016. Main ends at 022; 023–026 are reserved for T2.
-- Activated by the candidate on BATCH_V1 plan/read. Existing plans keep their schema and rail.
alter table ouf_mcp.analysis_preview_progress add column batch_columns jsonb
 check (batch_columns is null or (jsonb_typeof(batch_columns)='array'
   and jsonb_array_length(batch_columns)=cardinality(headers)));

create table ouf_mcp.analysis_batch_offer (
 analysis_id char(64) not null references ouf_mcp.profile_analysis,
 batch_id uuid not null,
 revision bigint not null check (revision >= 0),
 items jsonb not null check (jsonb_typeof(items)='array' and jsonb_array_length(items) between 1 and 8),
 retired_at timestamptz,
 primary key (analysis_id,batch_id)
);
create unique index analysis_batch_active_offer on ouf_mcp.analysis_batch_offer(analysis_id)
 where retired_at is null;

create table ouf_mcp.analysis_batch_column (
 analysis_id char(64) not null,
 ordinal integer not null,
 round integer not null default 1 check (round between 1 and 8),
 attempt integer not null default 1 check (attempt between 1 and 8),
 state text not null check (state in ('PENDING','RUNNING','READY_FOR_DECISION','DECIDED','CLOSED')),
 due_at timestamptz,
 intent text not null,
 metadata jsonb not null check (jsonb_typeof(metadata)='object'),
 -- The owner request and attested count are on analysis_column. This stores
 -- compact candidate evidence for that attempt, ordered by score descending,
 -- canonical URI and candidate ID.
 candidates jsonb not null default '[]' check (jsonb_typeof(candidates)='array' and jsonb_array_length(candidates)<=100),
 evidence_version text not null default '',
 offered_batch_id uuid,
 primary key (analysis_id,ordinal),
 foreign key (analysis_id,ordinal) references ouf_mcp.analysis_column,
 foreign key (analysis_id,offered_batch_id) references ouf_mcp.analysis_batch_offer
);
create index analysis_batch_column_offer on ouf_mcp.analysis_batch_column(analysis_id,state,round,ordinal);
create index analysis_batch_column_due on ouf_mcp.analysis_batch_column(state,due_at) where due_at is not null;

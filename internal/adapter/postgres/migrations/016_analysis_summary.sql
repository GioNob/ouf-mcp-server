-- T0 projections. A v1 writer needs no new columns or permissions.
create table ouf_mcp.analysis_counts (
 analysis_id char(64) not null references ouf_mcp.profile_analysis,
 seq bigint not null check (seq >= 0),
 total integer not null check (total between 1 and 1024),
 matched integer not null default 0 check (matched >= 0),
 ready integer not null default 0 check (ready >= 0),
 zero_count integer not null default 0 check (zero_count >= 0),
 blocked integer not null check (blocked >= 0),
 pending integer not null check (pending >= 0),
 onboarding boolean not null,
 stopped boolean not null default false,
 integrity_hold boolean not null default false,
 primary key (analysis_id,seq),
 check (total = matched + ready + zero_count + blocked + pending)
);
-- Validate the discriminant at the storage boundary. Ordinal and sequence
-- are relational keys, added only when rendering the public item.
create function ouf_mcp.analysis_outcome_valid(o jsonb) returns boolean
language sql immutable strict parallel safe as $valid$
 select coalesce(jsonb_typeof(o)='object'
   and (not (o ? 'requestId') or (jsonb_typeof(o->'requestId')='string'
     and o->>'requestId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))
   and case o->>'state'
   when 'PENDING' then o - array['state','requestId'] = '{}'::jsonb
   when 'BLOCKED' then o - array['state','reasonCode','requestId'] = '{}'::jsonb
     and o->>'reasonCode' ~ '^[A-Z][A-Z0-9_]{2,79}$'
   when 'MATCHED_PUBLISHED' then o - array['state','match'] = '{}'::jsonb
     and jsonb_typeof(o->'match')='object'
     and (o->'match') ?& array['semanticId','semanticVersion','revisionId','publicationSetId','score','reasons']
     and (o->'match') - array['semanticId','semanticVersion','revisionId','publicationSetId','score','reasons']='{}'::jsonb
   when 'NO_CANDIDATES' then o - array['state','requestId','observedAt','candidateCount'] = '{}'::jsonb
     and o ? 'requestId' and o->'candidateCount'='0'::jsonb and jsonb_typeof(o->'observedAt')='string'
   when 'CANDIDATES_READY' then o - array['state','requestId','observedAt','candidateCount'] = '{}'::jsonb
     and o ? 'requestId' and jsonb_typeof(o->'candidateCount')='number' and o->>'candidateCount' ~ '^[1-9][0-9]*$'
     and jsonb_typeof(o->'observedAt')='string'
   else false end, false)
$valid$;
create table ouf_mcp.analysis_column (
 analysis_id char(64) not null references ouf_mcp.profile_analysis,
 ordinal integer not null check (ordinal between 0 and 1023),
 header text not null,
 task_id char(64),
 request_id uuid,
 next_tool text not null,
 outcome jsonb not null check (ouf_mcp.analysis_outcome_valid(outcome)),
 primary key (analysis_id,ordinal)
);
create index analysis_job_columns on ouf_mcp.analysis_column(analysis_id,request_id,ordinal) where request_id is not null;
create index analysis_column_next on ouf_mcp.analysis_column(analysis_id,ordinal) where next_tool <> '';
create index analysis_column_blocked on ouf_mcp.analysis_column(analysis_id,ordinal) where outcome->>'state' = 'BLOCKED';
create index analysis_task_columns on ouf_mcp.analysis_column(analysis_id,task_id,ordinal);
create table ouf_mcp.analysis_job_completion (
 analysis_id char(64) not null references ouf_mcp.profile_analysis,
 request_id uuid not null, completed_count int not null check (completed_count >= 0),
 primary key (analysis_id,request_id)
);
create index analysis_column_candidates on ouf_mcp.analysis_column(analysis_id,ordinal) where outcome->>'state' = 'CANDIDATES_READY';
create table ouf_mcp.analysis_column_version (
 analysis_id char(64) not null,
 ordinal integer not null,
 seq bigint not null,
 outcome jsonb not null check (ouf_mcp.analysis_outcome_valid(outcome)),
 primary key (analysis_id,ordinal,seq),
 foreign key (analysis_id,ordinal) references ouf_mcp.analysis_column
);
create table ouf_mcp.analysis_summary_read (
 analysis_id char(64) not null references ouf_mcp.profile_analysis,
 service_principal_id text not null, principal_id text not null, tenant_id text not null,
 decision_ref text not null, seq bigint not null,
 snapshot_id uuid, frozen_hint jsonb,
 first_ordinal int not null, last_ordinal int not null,
 served_at timestamptz not null default clock_timestamp()
);
create unique index analysis_frozen_hint on ouf_mcp.analysis_summary_read(snapshot_id) where frozen_hint is not null;

-- A verified prefix survives admission-window pauses and process restarts.
create table ouf_mcp.analysis_preview_progress (
 analysis_id char(64) primary key, service_principal_id text not null,
 principal_id text not null, tenant_id text not null, manifest_checksum char(64) not null,
 profile jsonb not null, headers text[] not null,
 check (cardinality(headers) between 1 and 1024)
);

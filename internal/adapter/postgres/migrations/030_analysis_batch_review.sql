-- D-103. Predecessor source 85a61d53efcd05780479fa009cae9078d51952c5 (66d). Depends on 029.
-- Activated by the BATCH_V1 review worker and compact Semantic rail.
alter table ouf_mcp.analysis_batch_column
 drop constraint analysis_batch_column_work_stage_check,
 add check (work_stage in ('PUBLISHED','REQUEST','STATUS','CANDIDATES','REFORMULATE',
 'ADOPT','PROPOSE','DRAFT','VALIDATE','REVIEW_ADD','REVIEW_INSPECT')),
 add column wait_reason text;

alter table ouf_mcp.analysis_batch_choice
 add column creation_code text,
 add column review_arguments bytea,
 add column semantic_id text,
 add column review_blocked_by uuid;

create index analysis_batch_review_revision on ouf_mcp.analysis_batch_choice(analysis_id,owner_ref,ordinal)
 where owner_ref is not null;

create index analysis_batch_review_blocked on ouf_mcp.analysis_batch_choice(analysis_id,review_blocked_by)
 where review_blocked_by is not null;

-- Preserve the legacy discovery projection and its validator. Batch outcomes
-- participate in the same immutable summary sequence through a separate field.
alter table ouf_mcp.analysis_column_version
 add column batch_outcome jsonb check (jsonb_typeof(batch_outcome)='object');

create table ouf_mcp.analysis_batch_review_item (
 analysis_id char(64) not null references ouf_mcp.profile_analysis,
 revision_id uuid not null,
 review_batch_id uuid not null,
 version char(64) not null,
 state text not null,
 publication_set_id uuid,
 primary key (analysis_id, revision_id, review_batch_id)
);
create index analysis_batch_review_open on ouf_mcp.analysis_batch_review_item(analysis_id,review_batch_id)
 where state='UNDER_REVIEW';

-- The 66d creation receipt is already committed. Inspect it before any
-- validation or ADD, using a stage the predecessor worker cannot dispatch.
with pending as (
 update ouf_mcp.analysis_batch_choice d
 set creation_code=d.effect_code,effect_state='PENDING'
 from ouf_mcp.analysis_batch_column b
 where d.analysis_id=b.analysis_id and d.ordinal=b.ordinal and d.command_key=b.choice_key
 and b.state='CLOSED' and d.action in ('ADOPT','PROPOSE')
 and d.effect_state='APPLIED' and d.owner_ref is not null
 returning d.analysis_id,d.ordinal
)
update ouf_mcp.analysis_batch_column b set work_stage='REVIEW_INSPECT',state='PENDING',due_at=transaction_timestamp(),
 retry_started_at=null,retry_attempts=0,dispatch_fence=null
from pending p where b.analysis_id=p.analysis_id and b.ordinal=p.ordinal;

with choices as materialized (
 select d.analysis_id,d.ordinal,d.reason
 from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_batch_choice d
 on d.analysis_id=b.analysis_id and d.ordinal=b.ordinal and d.command_key=b.choice_key
 where d.action='EXCLUDE' and d.effect_state='APPLIED'
), advanced as (
 insert into ouf_mcp.analysis_counts
 (analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
 select h.analysis_id,h.seq+1,h.total,h.matched,h.ready,h.zero_count,h.blocked,h.pending,h.onboarding,h.stopped,h.integrity_hold
 from (select distinct analysis_id from choices) a cross join lateral
 (select * from ouf_mcp.analysis_counts where analysis_id=a.analysis_id order by seq desc limit 1) h
 returning analysis_id,seq
)
insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome,batch_outcome)
select a.analysis_id,d.ordinal,a.seq,c.outcome,jsonb_build_object('state','EXCLUDED','action','EXCLUDE','reason',d.reason)
from advanced a join choices d using(analysis_id)
join ouf_mcp.analysis_column c on c.analysis_id=a.analysis_id and c.ordinal=d.ordinal;

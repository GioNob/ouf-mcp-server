-- Operational timing travels with the recorded outcome, including an
-- uncertain dispatch whose lease and admission reservation are still live.
alter table ouf_mcp.tool_attempt
 add column if not exists owner_selector text,
 add column if not exists owner_due timestamptz,
 add column if not exists owner_due_reason text not null default '',
 add column if not exists owner_code text not null default '',
 add column if not exists owner_status integer not null default 0;
create index if not exists attempt_owner_observation on ouf_mcp.tool_attempt
 (service_principal_id,principal_id,tenant_id,owner_selector,owner_due desc,attempt_id)
 where owner_selector is not null and owner_due is not null;
alter table ouf_mcp.analysis_owner_due
 add column if not exists reason text not null default 'OWNER_OBSERVATION',
 add column if not exists observation_code text not null default 'SUCCEEDED',
 add column if not exists observation_status integer not null default 0;

alter table ouf_mcp.analysis_column
 add column if not exists proposal_choice uuid,
 add column if not exists exclusion_choice uuid,
 add column if not exists decision_attempt uuid;
create unique index if not exists analysis_proposal_choice on ouf_mcp.analysis_column(proposal_choice) where proposal_choice is not null;
create unique index if not exists analysis_exclusion_choice on ouf_mcp.analysis_column(exclusion_choice) where exclusion_choice is not null;

-- Append a new version for predecessor columns closed by completion alone.
-- Historical pages retain their old sequence. Repeating the migration finds
-- no qualifying rows after the first update.
with reopened as (
 update ouf_mcp.analysis_column set next_tool='semantic.discovery.candidates',
 outcome=jsonb_build_object('state','BLOCKED','reasonCode','DISCOVERY_DECISION_REQUIRED','requestId',request_id)
 where outcome->>'state'='NO_CANDIDATES' and request_id is not null and decision_attempt is null
 returning analysis_id,ordinal,outcome
), counts as (
 select analysis_id,count(*)::int as n from reopened group by analysis_id
), head as (
 insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
 select h.analysis_id,h.seq+1,h.total,h.matched,h.ready,h.zero_count-c.n,h.blocked+c.n,h.pending,h.onboarding,h.stopped,h.integrity_hold
 from counts c cross join lateral (select * from ouf_mcp.analysis_counts where analysis_id=c.analysis_id order by seq desc limit 1) h
 returning analysis_id,seq
)
insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
 select analysis_id,r.ordinal,h.seq,r.outcome from reopened r join head h using(analysis_id);

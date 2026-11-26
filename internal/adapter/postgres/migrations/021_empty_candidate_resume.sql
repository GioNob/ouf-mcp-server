-- Recover the continuation of a previously observed empty page. Ask for a
-- fresh list before offering a decision. The flag alone cannot attest the
-- owner's completion count. Preserve choice keys and all historical versions.
with resumed as (
 update ouf_mcp.analysis_column c set next_tool='semantic.discovery.candidates',
 outcome=jsonb_build_object('state','PENDING','requestId',c.request_id)
 where c.empty_candidate_page and c.request_id is not null and c.decision_attempt is null
 and c.next_tool='' and c.outcome->>'reasonCode'='DISCOVERY_EVIDENCE_UNAVAILABLE'
 and not exists (select from ouf_mcp.analysis_job_completion j
                 where j.analysis_id=c.analysis_id and j.request_id=c.request_id and j.completed_count>0)
 and not (select h.integrity_hold from ouf_mcp.analysis_counts h
          where h.analysis_id=c.analysis_id order by seq desc limit 1)
 returning c.analysis_id,c.ordinal,c.outcome
), affected as (
 select analysis_id,count(*)::int as n from resumed group by analysis_id
), head as (
 insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
 select h.analysis_id,h.seq+1,h.total,h.matched,h.ready,h.zero_count,h.blocked-a.n,h.pending+a.n,h.onboarding,false,h.integrity_hold
 from affected a cross join lateral (select * from ouf_mcp.analysis_counts where analysis_id=a.analysis_id order by seq desc limit 1) h
 returning analysis_id,seq
)
insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
 select r.analysis_id,r.ordinal,h.seq,r.outcome from resumed r join head h using(analysis_id);

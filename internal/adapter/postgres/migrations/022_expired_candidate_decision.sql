-- A verified empty page can follow a positive immutable completion count.
-- Keep the original choice keys, completion evidence and historical pages.
with expired as (
 select c.analysis_id,c.ordinal,c.outcome->>'state' as old_state
 from ouf_mcp.analysis_column c join ouf_mcp.analysis_job_completion j using(analysis_id,request_id)
 where c.empty_candidate_page and c.decision_attempt is null and j.completed_count>0
 and (c.outcome->>'reasonCode'='DISCOVERY_EVIDENCE_UNAVAILABLE' or c.outcome->>'state'='CANDIDATES_READY')
 and not (select h.integrity_hold from ouf_mcp.analysis_counts h
          where h.analysis_id=c.analysis_id order by seq desc limit 1)
), reopened as (
 update ouf_mcp.analysis_column c set next_tool='semantic.discovery.candidates',
 outcome=jsonb_build_object('state','BLOCKED','reasonCode','DISCOVERY_DECISION_REQUIRED','requestId',c.request_id)
 from expired e where c.analysis_id=e.analysis_id and c.ordinal=e.ordinal
 returning c.analysis_id,c.ordinal,c.outcome,e.old_state
), affected as (
 select analysis_id,count(*) filter(where old_state='CANDIDATES_READY') as ready from reopened group by analysis_id
), head as (
 insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
 select h.analysis_id,h.seq+1,h.total,h.matched,h.ready-a.ready,h.zero_count,h.blocked+a.ready,h.pending,h.onboarding,false,h.integrity_hold
 from affected a cross join lateral (select * from ouf_mcp.analysis_counts where analysis_id=a.analysis_id order by seq desc limit 1) h
 returning analysis_id,seq
)
insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
 select r.analysis_id,r.ordinal,h.seq,r.outcome from reopened r join head h using(analysis_id);

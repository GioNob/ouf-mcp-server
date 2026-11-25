-- The first session already spent the right, even if 019 later opened
-- further sessions. Retain every historical decision and fence.
alter table ouf_mcp.analysis_run_budget_session
 add column renewal_used boolean not null default false,
 add column increment jsonb,
 add column request_hash text check (request_hash ~ '^[0-9a-f]{64}$'),
 add column grant_ref text,
 add column proposal_version bigint,
 add column proposal_hash text,
 add check ((increment is null and request_hash is null and grant_ref is null and proposal_version is null and proposal_hash is null)
         or (increment is not null and request_hash is not null and grant_ref is not null and proposal_version is not null
             and proposal_version > 0 and proposal_hash is not null and proposal_hash ~ '^[0-9a-f]{64}$' and renewal_used));

update ouf_mcp.analysis_run_budget_session s set renewal_used=true
 from (select analysis_id,incident_id,min(expected_seq) first_seq
       from ouf_mcp.analysis_run_budget_session group by analysis_id,incident_id) first_session
 where s.analysis_id=first_session.analysis_id and s.incident_id=first_session.incident_id
   and s.expected_seq=first_session.first_seq;
alter table ouf_mcp.analysis_run_budget_session alter column renewal_used set default true;
-- Pre-020 extra sessions remain immutable history. Every future INSERT
-- (including the predecessor's unchanged column list) must spend the right.
alter table ouf_mcp.analysis_run_budget_session add check (renewal_used) not valid;
create unique index analysis_run_renewal_once on ouf_mcp.analysis_run_budget_session(analysis_id,incident_id)
 where renewal_used;

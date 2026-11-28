-- D-103. Predecessor source: 785469942cae45b3cd8eadc2ccda8fe99bbd67ef (66b2).
-- Depends on 018 and 027–028. Activated by BATCH_V1 decide. The predecessor
-- worker does not claim the new effect stages.
create table ouf_mcp.analysis_batch_receipt (
 service_principal_id text not null,
 principal_id text not null,
 tenant_id text not null,
 command_key uuid not null,
 analysis_id char(64) not null,
 batch_id uuid not null,
 fingerprint char(64) not null check (fingerprint ~ '^[a-f0-9]{64}$'),
 receipt jsonb not null check (jsonb_typeof(receipt)='object'),
 primary key (service_principal_id,principal_id,tenant_id,command_key),
 unique (analysis_id,command_key),
 foreign key (analysis_id,batch_id) references ouf_mcp.analysis_batch_offer
);

create table ouf_mcp.analysis_batch_choice (
 analysis_id char(64) not null,
 command_key uuid not null,
 ordinal int not null,
 action text not null check (action in ('ADOPT','REFORMULATE','EXCLUDE','PROPOSE')),
 attempt int not null check (attempt between 1 and 8),
 request_id uuid not null,
 candidate_id uuid,
 requested_revision_id uuid,
 effect_key uuid unique,
 effect_arguments bytea,
 intent text,
 reason text,
 effect_state text not null check (effect_state in ('PENDING','APPLIED','REFUSED','HELD')),
 effect_code text,
 owner_ref uuid,
 check ((action='ADOPT')=(candidate_id is not null)),
 check ((action='EXCLUDE')=(reason is not null)),
 check ((action='REFORMULATE')=(intent is not null)),
 check ((action<>'EXCLUDE')=(effect_key is not null)),
 check ((action in ('ADOPT','PROPOSE'))=(effect_arguments is not null)),
 check (requested_revision_id is null or action='PROPOSE'),
 primary key (analysis_id,command_key,ordinal),
 foreign key (analysis_id,command_key) references ouf_mcp.analysis_batch_receipt(analysis_id,command_key),
 foreign key (analysis_id,ordinal) references ouf_mcp.analysis_batch_column
);
create index analysis_batch_choice_reformulations on ouf_mcp.analysis_batch_choice(analysis_id,ordinal)
 where action='REFORMULATE';
create index analysis_batch_choice_owner_ref on ouf_mcp.analysis_batch_choice(owner_ref,analysis_id)
 where action='PROPOSE' and effect_state='APPLIED' and effect_code='CREATED';

alter table ouf_mcp.analysis_batch_column
 drop constraint analysis_batch_column_work_stage_check,
 add check (work_stage in ('PUBLISHED','REQUEST','STATUS','CANDIDATES','REFORMULATE','ADOPT','PROPOSE','DRAFT')),
 add column choice_key uuid,
 add column reconcile_fence uuid,
 add column transport_fence uuid,
 add column decision_reason text,
 add column refused_candidates text[] not null default '{}',
 add foreign key (analysis_id,choice_key,ordinal) references ouf_mcp.analysis_batch_choice;

create index tool_attempt_owned_proposal_revision
 on ouf_mcp.tool_attempt(service_principal_id,principal_id,tenant_id,replay_result_id)
 where capability_id='ouf.semantic.propose' and state='SUCCEEDED' and replay_result_id is not null;

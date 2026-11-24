create table ouf_mcp.analysis_renewal_proposal (
    review_ref text primary key check (review_ref ~ '^[A-Za-z0-9_-]{22,128}$'),
    analysis_id text not null references ouf_mcp.analysis_run,
    incident_id text not null references ouf_mcp.analysis_run_incident(incident_id),
    version bigint not null check (version between 1 and 9007199254740991),
    proposal bytea not null,
    expires_at timestamptz not null,
    state text not null check (state in ('OPEN','CHANGES_REQUESTED','APPROVED','CLOSED_PARTIAL','SUPERSEDED','EXPIRED')),
    feedback bytea not null default '[]',
    unique (analysis_id, incident_id, version)
);
create unique index analysis_renewal_current on ouf_mcp.analysis_renewal_proposal(analysis_id)
    where state in ('OPEN','CHANGES_REQUESTED');
create index analysis_renewal_expiry on ouf_mcp.analysis_renewal_proposal(expires_at,review_ref)
    where state in ('OPEN','CHANGES_REQUESTED');
create table ouf_mcp.analysis_renewal_receipt (
    issuer text not null, workload text not null, tenant text not null, subject text not null,
    command_key uuid not null, fingerprint text not null,
    review_ref text not null references ouf_mcp.analysis_renewal_proposal,
    receipt bytea not null, authority jsonb,
    primary key (issuer,workload,tenant,subject,command_key)
);

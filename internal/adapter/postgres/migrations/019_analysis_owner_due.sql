-- A discovery may cover several columns. Keep its operational clock once,
-- independently of immutable completion evidence and paginated outcomes.
create table if not exists ouf_mcp.analysis_owner_due (
 analysis_id char(64) not null references ouf_mcp.profile_analysis on delete cascade,
 request_id uuid not null,
 not_before timestamptz not null,
 primary key (analysis_id, request_id)
);

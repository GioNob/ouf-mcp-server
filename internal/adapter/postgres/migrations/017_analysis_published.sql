-- Only T1 plans have a row. Legacy plans retain their original manifest fence.
create table ouf_mcp.analysis_published_plan (
 analysis_id char(64) primary key references ouf_mcp.profile_analysis,
 visibility_digest char(64) not null,
 seed_digest char(64) not null,
 compatibility_reason text not null default '',
 stop_reason text not null default ''
);

-- The predecessor names its insert columns. Its ordinals keep priority false.
alter table ouf_mcp.analysis_column add column published_first boolean not null default false;
create index analysis_published_next on ouf_mcp.analysis_column(analysis_id,published_first desc,ordinal)
 where next_tool<>'';

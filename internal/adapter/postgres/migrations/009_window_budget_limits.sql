-- Existing windows retain unknown caps and reject new admissions until expiry.
-- Never reset consumption or infer a cap from a later capability's envelope.
alter table ouf_mcp.budget_window
 add column limit_tool_calls bigint check(limit_tool_calls between 1 and 50),
 add column limit_result_bytes bigint check(limit_result_bytes between 1 and 20971520),
 add column limit_distinct_objects bigint check(limit_distinct_objects between 0 and 5000),
 add constraint window_budget_limits_complete check (
  (limit_tool_calls is null and limit_result_bytes is null and limit_distinct_objects is null)
  or (limit_tool_calls is not null and limit_result_bytes is not null and limit_distinct_objects is not null));

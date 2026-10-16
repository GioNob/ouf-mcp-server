-- Explicitly authorized one-call admission session; never overwrite a prior
-- claim, attempt, reservation, outcome or consumed budget. The owner command
-- key remains unchanged. No runtime path creates or signs an authorization.
create table ouf_mcp.semantic_reprocessing_session(
 session_id uuid primary key,
 permit_hash char(64) not null check(permit_hash ~ '^[0-9a-f]{64}$'),
 predecessor_attempt_id uuid not null unique references ouf_mcp.tool_attempt(attempt_id),
 new_attempt_id uuid not null unique references ouf_mcp.tool_attempt(attempt_id),
 budget_window_id uuid not null unique references ouf_mcp.budget_window(budget_window_id),
 decision_ref text not null,
 human_subject text not null,
 reason text not null,
 prior_tool_calls bigint not null check(prior_tool_calls=1),
 maximum_new_tool_calls bigint not null check(maximum_new_tool_calls=1),
 maximum_cumulative_tool_calls bigint not null check(maximum_cumulative_tool_calls=2),
 owner_receipt_absent_at timestamptz not null,
 issued_at timestamptz not null,
 expires_at timestamptz not null,
 created_at timestamptz not null default transaction_timestamp(),
 check(expires_at>issued_at and expires_at<=issued_at+interval '15 minutes'),
 check(owner_receipt_absent_at<=issued_at and owner_receipt_absent_at>=issued_at-interval '5 minutes')
);
create trigger semantic_reprocessing_session_immutable before update or delete
 on ouf_mcp.semantic_reprocessing_session for each row execute function ouf_mcp.reject_mutation();

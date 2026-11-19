-- Nullable for historical attempts: their admission-time classification
-- cannot be reconstructed from their current terminal state.
alter table ouf_mcp.attempt_admission_context add column is_equivalent_retry boolean;
create index attempt_admission_context_retry_group_idx
 on ouf_mcp.attempt_admission_context(equivalence_group_id,attempt_id);

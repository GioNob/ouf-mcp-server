-- Older attempts did not retain the public owner UUID. NULL means unavailable,
-- including for existing successes. A backend request ID is not an owner receipt.
alter table ouf_mcp.tool_attempt add column replay_result_id uuid;

-- Older discovery claims used the transport key. The owner key cannot be
-- reconstructed from a hash. Bind it lazily on an exact, scoped payload match.
alter table ouf_mcp.idempotency_claim add column discovery_owner_key text
 check (discovery_owner_key is null or (capability_id='ouf.semantic.discovery' and operation_class='COMMAND'));
create unique index discovery_canonical_claim_key on ouf_mcp.idempotency_claim
 (service_principal_id, principal_id, tenant_id, owner, discovery_owner_key)
 where capability_id='ouf.semantic.discovery' and operation_class='COMMAND' and discovery_owner_key is not null;
create index discovery_claim_payload on ouf_mcp.idempotency_claim
 (service_principal_id, principal_id, tenant_id, owner, request_hash)
 where capability_id='ouf.semantic.discovery' and operation_class='COMMAND' and discovery_owner_key is null;

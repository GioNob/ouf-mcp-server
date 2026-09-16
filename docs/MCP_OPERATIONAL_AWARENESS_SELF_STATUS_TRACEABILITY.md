# MCP Operational Awareness — self status and explain

Historical implementation note. The local-dispatch statement from MCP PET v1.3 / Matrix v1.6 is **superseded** by MCP PET v1.4, Gateway PET v1.5 and Cross-Module Alignment Matrix v1.7.

Current invariant:

- `ouf.system.status` is MCP-owned and tenant-scoped.
- Authorization, admission/budget, reconciliation and audit remain on the ordinary governed invocation path.
- Final dispatch for `ouf.system.status` MUST traverse Urban API Gateway and return to the private MCP owner API `/api/internal/v1/mcp/operations/status`; no MCP-local post-admission shortcut is permitted.
- The projection remains bounded and contains UNKNOWN/UNRESOLVED attempts, expired RUNNING attempts, ACTIVE object debt, VERIFIED Evidence Inbox backlog and tenant-bound security incidents.
- Protected evidence payload hashes and raw logs are not returned.
- `ouf.operations.explain` remains owner=Ingestion and is dispatched through Urban API Gateway.

See `MCP_CHANNEL_NEUTRAL_OA_TRACEABILITY.md` for the current evidence classification.

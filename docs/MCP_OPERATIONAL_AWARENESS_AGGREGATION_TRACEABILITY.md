# MCP Operational Awareness aggregation traceability

Normative baseline: OUF Reality Baseline Package v1.7, MCP PET v1.4 and Cross-Module Alignment Matrix v1.7.

## PET / Matrix requirements

- MCP owns human-facing Operational Awareness aggregation and conversational projection, not producer truth.
- Producer modules remain authoritative for their own incidents/status.
- Human-facing capabilities remain Gateway-mediated and channel-neutral.
- Missing/unavailable producer evidence must never be collapsed to HEALTHY or NO_INCIDENTS.
- Security-sensitive/raw producer evidence must not leak through the global projection.

## Implementation evidence

- `ouf.operations.summary` and `ouf.operations.incidents` are MCP-owned tool capabilities.
- Private owner endpoints `/api/internal/v1/mcp/operations/summary` and `/incidents` perform cross-producer composition.
- Ingestion and Gateway are queried through separate Gateway-mediated producer capabilities, each using an independently governed nested invocation.
- MCP self-state is read from the MCP authoritative store inside the MCP owner backend; it is projected as a safe module summary or one synthetic operational incident, without security-incident detail leakage.
- Producer failure produces `partial=true`, lists `unavailableProducers`, and forces aggregate summary to `DEGRADED`.
- Gateway configuration also exposes OIDC API bindings to the same MCP owner endpoints, preserving channel neutrality.

## Test evidence

- Unit tests cover healthy/recovering cross-producer composition, unavailable producer partial/degraded behavior, and MCP security-detail non-disclosure.
- Pairwise test validates the MCP manifest against the merged Gateway aggregation routes and producer-specific owner boundaries.

## Evidence pending

- Real Authorization decisions for aggregate and nested producer capabilities.
- Deployed OIDC/IAM/NetworkPolicy behavior and end-to-end chatbot/UI execution.
- Production load/capacity impact of nested operational queries.

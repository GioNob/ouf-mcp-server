# MCP Operational Awareness — self status and explain

Normative baseline: MCP PET v1.3 Operational Awareness, Gateway PET v1.4, Authorization PET v1.5, Cross-Module Alignment Matrix v1.6.

## Implemented evidence

- `ouf.system.status` is MCP-owned and tenant-scoped.
- Authorization, admission/budget, reconciliation and audit remain on the ordinary governed invocation path.
- Final dispatch for `ouf.system.status` is local to the authoritative MCP PostgreSQL state to avoid an MCP→Gateway→MCP loop.
- The projection is bounded and contains UNKNOWN/UNRESOLVED attempts, expired RUNNING attempts, ACTIVE object debt, VERIFIED Evidence Inbox backlog and tenant-bound security incidents.
- Protected evidence payload hashes and raw logs are not returned.
- `ouf.operations.explain` remains owner=Ingestion and is dispatched through the Urban API Gateway.

## Evidence classification

CI verifies manifest closure, local-vs-remote dispatch selection, tenant isolation and protected-evidence non-disclosure. Real Authorization/IAM decisions and deployed multi-Pod/database evidence remain EVIDENCE PENDING.

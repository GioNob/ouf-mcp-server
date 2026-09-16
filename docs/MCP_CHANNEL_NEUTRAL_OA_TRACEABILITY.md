# MCP channel-neutral Operational Awareness traceability

Normative baselines: MCP PET v1.4, Gateway PET v1.5 and Cross-Module Alignment Matrix v1.7.

`ouf.system.status` remains owned by MCP but is no longer locally dispatched after MCP admission. The orchestration path always delegates through the Gateway. The MCP process exposes a private owner API at `/api/internal/v1/mcp/operations/status`; that API requires trusted Gateway context and tenant identity, and returns the existing bounded tenant-scoped SystemStatus projection.

The MCP protocol endpoint `/mcp` is distinct from the owner API. Repository tests prohibit the old local shortcut and reject owner-API calls without Gateway/tenant context. Deployed NetworkPolicy/IAM enforcement remains EVIDENCE PENDING.

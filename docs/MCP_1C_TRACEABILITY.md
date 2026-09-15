# MCP Server 1C — governed admission and Gateway dispatch

This increment implements the bounded execution core required by MCP PET v1.2 §§2.1, 31–33, 38, 67–68 and 82–83.

| PET control | Executable evidence |
|---|---|
| GWMCP-01 / MCP-GW-01 | Tool input contains no URL; `GatewayRequest` carries only the manifest-pinned logical `gatewayBindingRef`; configured endpoints require HTTPS and redirects are disabled. |
| MCP-GW-02 | Gateway `Problem` and `Retry-After` survive unchanged through the orchestration result. |
| Steps 1–8 before backend | Authorization and atomic PostgreSQL reservation run before `GatewayPort.Execute`; unit tests prove deny/stall yield zero Gateway calls. |
| Atomic admission | Migration 002 and `Store.Reserve` bind idempotency, canonical budget window, equivalence group, retry guard, attempt and reservation in one serializable transaction. |
| No transaction during I/O | `Reserve` commits before `Dispatch`; `Reconcile` starts only after Gateway returns. |
| Multi-Pod state | Budget, retry and idempotency state are PostgreSQL-owned and keyed independently of correlation/session IDs. |
| Streaming cap | Response bytes are bounded before MCP delivery and reconciled as failure on overflow. |
| Workload/delegated identity | Attempt admission persists service principal, principal, tenant, actor, authentication context and authorization decision references. |

Advanced Evidence Ingress, late compensation/debt and deterministic recovery of `UNKNOWN` are intentionally not claimed by 1C; they remain later PET increments.

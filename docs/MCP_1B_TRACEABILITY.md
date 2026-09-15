# MCP 1B durable lifecycle

Normative source: OUF MCP Server PET v1.2, Reality Baseline Package v1.6.

| PET | Evidence |
|---|---|
| MCP-A16 idempotency | PostgreSQL integration test replays the same scoped key and rejects changed request hashes |
| MCP-A33 attempt transitions | constrained state/dispatch columns and CAS `ADMITTED` to `RUNNING` |
| MCP-A34 crash recovery | expired dispatched attempts become `UNKNOWN`, preserving uncertainty |
| MCP-A35 manifest pinning | attempts and sessions reference immutable checksummed snapshots |
| MCP-A38 session separation | durable application session is independent of rejected `Mcp-Session-Id` |
| Safe audit and maintenance | append-only trigger, orphan recovery and expired-session cleanup |

Gateway dispatch, authorization, budgets, retry equivalence and reconciliation remain bounded to MCP 1C.

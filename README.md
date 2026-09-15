# OUF MCP Server — MCP 1A feasibility gate

This repository records a **conditional NO-GO** for product implementation. The
normative PET requires MCP `2026-07-28`, including the stateless lifecycle and
`server/discover`; the official Java SDK 2.0.1 supports the legacy
`2025-11-25` lifecycle only. Its published roadmap assigns `2026-07-28` to the
future 3.x line.

The Java program is deliberately a disposable **legacy compatibility probe**.
It verifies that the current official Java SDK can expose a closed, typed tool
over Streamable HTTP, reject undeclared input, serve concurrent calls, and be
used by the official TypeScript client. It is not the production server, it
must not be deployed, and its green tests do not satisfy `MCP-A38`.

Normative scope: MCP PET v1.1 `MCP-A01`, `MCP-A02`, `MCP-A05`, `MCP-A06`, `MCP-A15`, `MCP-A29`, `MCP-A38`; Gateway PET remains authoritative for the later mediated route. No Executive Freeze evidence is claimed.

See [`docs/MCP_1A_FEASIBILITY_REPORT.md`](docs/MCP_1A_FEASIBILITY_REPORT.md)
for the evidence, decision, exit criteria, and preserved Gateway–Onboarding
regression boundary.

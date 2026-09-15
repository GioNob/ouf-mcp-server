# OUF MCP Server

MCP 1A implements the protocol kernel defined by MCP Server PET v1.2 in Reality Baseline Package v1.6.

- Go 1.25.13 and official MCP Go SDK v1.7.0 are pinned.
- `/mcp` accepts only MCP 2026-07-28 Streamable HTTP modern/stateless POST requests.
- `server/discover`, `tools/list` and closed JSON Schema descriptors are provided by the official SDK.
- Legacy initialize/session transport is disabled.
- No SQL, arbitrary query language, network destination or backend bypass is exposed.
- Tool execution remains fail-closed until Gateway mediation and authorization land in a later increment.

Run `scripts/verify.sh` with Go 1.25.13. The same source builds the `mcp-server` and reserved `maintenance-worker` process roles.

See [MCP 1A traceability](docs/MCP_1A_TRACEABILITY.md) for the bounded PET claims.

# MCP ↔ Gateway pairwise 1D

The coordinated CI test runs the real Go `GatewayClient` from MCP Server against the real Python `MCPDispatcher` HTTP harness from the Gateway repository and a distinct UDP stub.

It proves the serialized MCP 1C envelope is accepted without translation drift, the Gateway selects only its registry-owned UDP service/path, governance headers reach UDP, and a UDP `Problem Details` 429 with `Retry-After` returns through Gateway to MCP unchanged and without duplicate calls.

The Gateway checkout is deliberately pinned to the coordinated pairwise branch while both PRs are under review. After the Gateway PR merges, the MCP workflow must replace that branch ref with the exact Gateway merge SHA before MCP merge.

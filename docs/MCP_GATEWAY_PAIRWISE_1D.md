# MCP ↔ Gateway pairwise 1D

The coordinated CI test runs the real Go `GatewayClient` from MCP Server against the real Python `MCPDispatcher` HTTP harness from the Gateway repository and a distinct UDP stub.

It proves the serialized MCP 1C envelope is accepted without translation drift, the Gateway selects only its registry-owned UDP service/path, governance headers reach UDP, and a UDP `Problem Details` 429 with `Retry-After` returns through Gateway to MCP unchanged and without duplicate calls.

The Gateway checkout is pinned to exact merge commit `be7e80f2667877734267d9b66733179244acb9f2`; the pairwise result therefore remains reproducible after branch deletion or later Gateway changes.

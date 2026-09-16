# MCP ↔ Gateway pairwise 1D

The coordinated CI test runs the real Go `GatewayClient` from MCP Server against the real Python `MCPDispatcher` HTTP harness from the Gateway repository and a distinct UDP stub.

It proves the serialized MCP 1C envelope is accepted without translation drift, the Gateway selects only its registry-owned UDP service/path, governance headers reach UDP, and a UDP `Problem Details` 429 with `Retry-After` returns through Gateway to MCP unchanged and without duplicate calls.

The same harness now exercises the real Go `RecoveryClient` through Gateway recovery mediation to the UDP owner outcome endpoint. It proves `OWNER_PROVES_NO_DISPATCH` is preserved at zero cost, while owner 5xx and mismatched evidence cannot become terminal MCP evidence.

The Gateway checkout is pinned to exact merge commit `9321bca98f22b9952075cf0dec1cf595a0e755d1`; the pairwise result therefore remains reproducible after branch deletion or later Gateway changes.

# Governed operational reads — resilience policy v3

Follow the [canonical platform policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and MCP PET1.4 §§33–34.

Summary and incidents reauthorize the current principal, capability, tenant, resource scope and TENANT_OPERATIONAL detail before any local status or nested producer read. A missing policy evaluator, denied decision, stale Gateway decision reference or mismatched scope prevents data access. Gateway trust remains necessary and is not itself an owner grant.

The owner API enforces the published typed query on every channel: object only, no unknown/duplicate keys, source identifier at most200 Unicode characters, timezone-aware since, limit1–100, and incident lifecycle filters only on incidents. Requests above64KiB are rejected rather than truncated. Unsupported cursor fields are rejected; cursor publication is still OPEN.

MCP self-state uses the existing fixed public allowlist even inside tenant aggregates. Protected counters and future fields cannot leak through summary. Missing/malformed/unknown producer evidence is explicitly partial/unavailable; MCP partial state cannot become HEALTHY. Malformed incident envelopes cannot become an authorized empty result. Producer and final owner response bodies have a512KiB budget.

Qualified and merged in [PR52](https://github.com/GioNob/ouf-mcp-server/pull/52): candidate 0f8006711895cfb82166e8a3e22fb7c3bf29a00c, merge 27ceed89709e7735f68145d335c9ef0c4d1df5d9. [Race38117976185](https://github.com/GioNob/ouf-mcp-server/actions/runs/38117976185) passes20 operational tests,6 new regression groups. [Full38117976205](https://github.com/GioNob/ouf-mcp-server/actions/runs/38117976205) and all6 push/PR gates SUCCESS. The final byte-budget cases are also qualified. Regression cases cover both aggregate capabilities, denial before I/O, stale decisions and scope/detail mismatches, malformed/oversized queries, missing/unknown producer health, partial self-state and protected field exclusion. Full native checks include races, PostgreSQL, pairwise Gateway mediation, conformance, staticcheck and vulnerability checks.

This change does not introduce durable operator checkpoints, global snapshot cursors, complete incident histories, dependency deduplication or active alert delivery. Summary remains the owner-reported bounded window, not proof of current global health. No target deploy/restart, new runtime grants or consumed R9/R10 replay is part of this tranche.

[Structured qualification evidence](../evidence/operational-read-catchup-qualified-20261011.json) · [Stable cursor/consultation design — OPEN](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md)

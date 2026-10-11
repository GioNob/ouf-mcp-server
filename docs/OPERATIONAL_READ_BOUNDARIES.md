# Governed operational reads — resilience policy v3

Follow the [canonical platform policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and MCP PET1.4 §§33–34.

Summary and incidents reauthorize the current principal, capability, tenant, resource scope and TENANT_OPERATIONAL detail before any local status or nested producer read. A missing policy evaluator, denied decision, stale Gateway decision reference or mismatched scope prevents data access. Gateway trust remains necessary and is not itself an owner grant.

The owner API enforces the published typed query on every channel: object only, no unknown/duplicate keys, source identifier at most200 Unicode characters, timezone-aware since, limit1–100, and incident lifecycle filters only on incidents. Requests above64KiB are rejected rather than truncated. Unsupported cursor fields are rejected; cursor publication is still OPEN.

MCP self-state uses the existing fixed public allowlist even inside tenant aggregates. Protected counters and future fields cannot leak through summary. Missing/malformed/unknown producer evidence is explicitly partial/unavailable; MCP partial state cannot become HEALTHY. Malformed incident envelopes cannot become an authorized empty result. Producer and final owner response bodies have a512KiB budget.

Qualification is pending native CI on the candidate. Regression cases cover both aggregate capabilities, denial before I/O, stale decisions and scope/detail mismatches, malformed/oversized queries, missing/unknown producer health, partial self-state and protected field exclusion. Full native checks include races, PostgreSQL, pairwise Gateway mediation, conformance, staticcheck and vulnerability checks.

This change does not introduce durable operator checkpoints, global snapshot cursors, complete incident histories, dependency deduplication or active alert delivery. Summary remains the owner-reported bounded window, not proof of current global health. No target deploy/restart, new runtime grants or consumed R9/R10 replay is part of this tranche.

# MCP 1G — Invariant-preserving retention and multi-worker concurrency

Normative baseline: MCP Server PET v1.2 retention ordering, bounded maintenance, append-only audit evidence, role isolation and concurrent-worker release gates.

| Obligation | Evidence | Status |
|---|---|---|
| audit remains append-only to application roles | dedicated audit trigger rejects UPDATE/DELETE except retention-owner definer execution | CI CANDIDATE |
| retention privilege separation | `ouf_mcp_retention_owner` is NOLOGIN and owns only the definer purge function path | CI CANDIDATE |
| maintenance cannot directly delete audit rows | explicit revoke + integration assertion | CI CANDIDATE |
| bounded audit purge | cutoff validation + limit 1..10000 + deterministic ordering | CI CANDIDATE |
| short concurrent purge batches | `FOR UPDATE SKIP LOCKED`, two workers, each bounded to 10 | CI CANDIDATE on PostgreSQL 17 |
| concurrent purge convergence | two workers purge 20 fixtures without duplicate accounting | CI CANDIDATE on PostgreSQL 17 |
| evidence inbox retention | existing 1F governed purge | CI VERIFIED |
| full referential retention chain | remaining table-specific purge stages in PET order | PARTIAL; subsequent 1G increments |
| deployed multi-Pod workload contention | Kubernetes runtime | EVIDENCE PENDING |

The audit exception is deliberately narrower than disabling the append-only trigger: only a SECURITY DEFINER function owned by a NOLOGIN retention role receives DELETE privilege. Normal application and maintenance roles continue to fail direct DELETE.

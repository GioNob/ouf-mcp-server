# MCP 1I — Observability, performance/capacity and DR

Normative baseline: OUF Reality Baseline Package v1.7; MCP Server PET v1.4 §§44, 56, 58, 99–107 and deployment acceptance DEP-A01…DEP-A07.

| Obligation | Evidence | Status |
|---|---|---|
| bounded runtime metrics | `internal/observability/metrics.go` + `/metrics` wiring | CI CANDIDATE |
| no sensitive/high-cardinality Prometheus labels | `TestMetricsUseOnlyBoundedLabels` | CI CANDIDATE |
| dispatch instrumentation p95 <50ms fixture | `TestDispatchMiddlewareP95Under50msFixture` | CI CANDIDATE; backend excluded |
| budget Reserve p95 <25ms LAN-profile target | `TestBudgetReserveP95Under25msLANProfileFixture` on local PostgreSQL 17 | CI CANDIDATE; environment-specific production SLO pending |
| PostgreSQL pool telemetry | `ouf_mcp_db_pool_*` metrics | CI CANDIDATE |
| dashboard contract | `observability/mcp-dashboard.yaml` | CI CANDIDATE |
| alert baseline | `observability/mcp-alerts.yaml` | CI CANDIDATE |
| 99.9% target and zero-tolerance safety SLOs | alert baseline contract | CI CANDIDATE; deployed measurement pending |
| PET initial server resource profile | `deploy/mcp-ha.yaml` 500m/768Mi request, 2 CPU/1536Mi limit | CI CANDIDATE |
| liveness/readiness + PDB/topology | `deploy/mcp-ha.yaml` | CI VERIFIED from 1H and re-gated in 1I |
| telemetry ingress remains NetworkPolicy governed | `deploy/networkpolicy.yaml` | CI CANDIDATE; deployed policy evidence pending |
| restore procedure preserves uncertainty/debt/approval invariants | `docs/MCP_1I_DR_RUNBOOK.md` + existing recovery/debt/concurrency tests | CI CANDIDATE |
| real backup/PITR restore with measured RPO/RTO | platform restore drill | EVIDENCE PENDING |
| real multi-Pod load/rollout/kill tests | Kubernetes environment | EVIDENCE PENDING |
| dashboard/alert screenshots/export from telemetry backend | deployed telemetry | EVIDENCE PENDING |

## Capacity interpretation

The PET values are initial capacity profiles, not a justification for Redis, broker or other new infrastructure. PostgreSQL remains the authoritative shared budget/claim store. Any future cache/broker adoption requires measured contention/backpressure plus the Economy Gate.

## Performance evidence boundary

CI fixtures establish implementation overhead and a local PostgreSQL LAN-profile regression gate. They do **not** prove production p95/p99, availability or RPO/RTO. Those remain deployment evidence and must be reported with environment, load shape and timestamps.

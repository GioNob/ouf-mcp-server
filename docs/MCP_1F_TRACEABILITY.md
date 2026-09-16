# MCP 1F — Maintenance reconciliation traceability

Normative baseline: `OUF_MCP_Server_v1_2_Go_Implementation_Baseline`, especially §76 (Retention e cleanup), the debt-cache rule immediately following the atomic admission algorithm, §112 (Maintenance worker), and the global lock-order / UNKNOWN-debt invariants.

## Implemented in this increment

| PET obligation | Evidence | Status |
|---|---|---|
| `current_unresolved_object_debt_total` is a non-authoritative cache of ACTIVE debt | `postgres.ReconcileDebtCaches` recomputes `SUM(debt_amount)` from ACTIVE `budget_object_debt` | IMPLEMENTED |
| Repair only under budget-window lock | one short transaction per selected window; `budget_window ... FOR UPDATE` precedes authoritative debt read and CAS repair | IMPLEMENTED |
| No local TTL may forgive unresolved object debt | reconciliation has no age/expiry predicate and never mutates `budget_object_debt` | IMPLEMENTED |
| Bounded maintenance work | positive batch limit is mandatory; candidate query is ordered and limited | IMPLEMENTED |
| Repair is observable/auditable | every non-zero repair appends immutable `BUDGET_DEBT_CACHE_REPAIRED` audit evidence with prior, authoritative and delta values | IMPLEMENTED |
| PostgreSQL release evidence | `TestDebtCacheReconciliationPostgres` creates deliberate drift and proves cache repair plus audit emission | CI DECISIVE |
| Reconciliation arithmetic guard | table-driven unit coverage for zero, positive, negative drift and impossible negative totals | IMPLEMENTED |

## Explicitly not claimed by 1F

This increment does **not** claim completion of the full §76 retention purge chain. In particular, append-only audit, security-incident and Evidence Inbox purge requires the PET-prescribed dedicated `SECURITY DEFINER` retention functions and role grants; implementing ad-hoc DELETE from the workload would violate the PET. It also does not claim deployed multi-worker, Kubernetes, IAM, performance, backup/restore or operational-runbook evidence.

Those items remain evidence/development pending and must not be inferred from a green unit/integration CI run.

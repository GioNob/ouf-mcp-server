# MCP 1F — Maintenance and retention

Normative baseline: MCP Server PET v1.2 maintenance/retention requirements already established for this repository, including bounded maintenance, authoritative unresolved-object debt, protected evidence retention and role isolation.

| Obligation | Evidence | Status |
|---|---|---|
| debt cache is non-authoritative | `ReconcileDebtCaches` recomputes ACTIVE debt | IMPLEMENTED |
| repair under budget-window lock | short transaction + `FOR UPDATE` + CAS | IMPLEMENTED |
| no TTL debt forgiveness | authoritative debt rows are never mutated by cache repair | IMPLEMENTED |
| bounded repair | positive batch limit, deterministic ordering | IMPLEMENTED |
| repair audit | `BUDGET_DEBT_CACHE_REPAIRED` safe-detail event | IMPLEMENTED |
| protected Evidence Inbox retention | `purge_owner_evidence` is bounded `SECURITY DEFINER`; VERIFIED, referenced and late-compensable evidence is excluded | IMPLEMENTED |
| maintenance role cannot directly DELETE protected evidence/audit/incident tables | explicit REVOKE; PostgreSQL role-denial test required | CI EVIDENCE PENDING |
| concurrent maintenance | candidate locking uses `SKIP LOCKED` for retention; debt repair serializes per budget window | IMPLEMENTED; concurrency test pending |
| append-only audit retention | direct DELETE remains prohibited; no global trigger-disable escape hatch is introduced | PENDING dedicated invariant-preserving purge design |
| deployed IAM membership / NetworkPolicy | environment evidence | EVIDENCE PENDING |

## Safety boundary

MCP 1F must not weaken append-only evidence invariants to obtain retention. In particular, disabling the audit immutability trigger at table scope would open a concurrent mutation window and is therefore intentionally not used. Audit retention remains pending rather than being implemented with a weaker mechanism.

MCP-A52 remains `PAIRWISE VERIFIED / overall PARTIAL`: this increment may add PostgreSQL role-denial evidence, but production identity issuance and IAM membership remain deployment evidence.

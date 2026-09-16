# MCP 1F — Maintenance and retention

Normative baseline: MCP Server PET v1.2 maintenance/retention requirements already established for this repository, including bounded maintenance, authoritative unresolved-object debt, protected evidence retention and role isolation.

| Obligation | Evidence | Status |
|---|---|---|
| debt cache is non-authoritative | `ReconcileDebtCaches` recomputes ACTIVE debt | CI VERIFIED |
| repair under budget-window lock | short transaction + `FOR UPDATE` + CAS | CI VERIFIED |
| no TTL debt forgiveness | authoritative debt rows are never mutated by cache repair | CI VERIFIED |
| bounded repair | `MCP_MAINTENANCE_BATCH`, deterministic ordering, 1..10000 bound | CI VERIFIED |
| repair audit | `BUDGET_DEBT_CACHE_REPAIRED` safe-detail event; concurrent test proves one repair event | CI VERIFIED |
| maintenance-worker orchestration | recovery then bounded debt-cache reconciliation in the existing bounded cycle | CI VERIFIED |
| protected Evidence Inbox retention | `purge_owner_evidence` is bounded `SECURITY DEFINER`; VERIFIED, referenced and late-compensable evidence is excluded | CI VERIFIED at function/privilege boundary |
| maintenance role cannot directly DELETE protected evidence/audit/incident tables | NOLOGIN role, schema USAGE only for governed function invocation, explicit DELETE denial | CI VERIFIED on PostgreSQL 17 |
| maintenance role owns no protected relation | catalog ownership assertion | CI VERIFIED on PostgreSQL 17 |
| concurrent maintenance | two concurrent debt reconcilers converge to the authoritative total and one audit repair | CI VERIFIED on PostgreSQL 17 |
| append-only audit retention | direct DELETE remains prohibited; no global trigger-disable escape hatch is introduced | PENDING dedicated invariant-preserving purge design |
| deployed IAM membership / NetworkPolicy | environment evidence | EVIDENCE PENDING |

## CI evidence

Run #87 on head `212ad7e15bc5ddb10a13613235085dac4539cd30` passed the existing Gateway/MCP pairwise suites, `scripts/verify.sh` including PostgreSQL integration tests, migration, one-shot maintenance worker, staticcheck and govulncheck.

## Safety boundary

MCP 1F must not weaken append-only evidence invariants to obtain retention. In particular, disabling the audit immutability trigger at table scope would open a concurrent mutation window and is therefore intentionally not used. Audit retention remains pending rather than being implemented with a weaker mechanism.

MCP-A52 remains `PAIRWISE VERIFIED / overall PARTIAL`: PostgreSQL role-denial evidence is now present, but production identity issuance and IAM membership remain deployment evidence.

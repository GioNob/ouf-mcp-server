# MCP 1G — Referential retention chain

Normative baseline: MCP Server PET v1.2 retention/maintenance requirements and the PET-declared referential purge ordering.

## Implemented in this increment

The governed purge function operates only on relations actually present in the current MCP schema. It does not invent absent normative concepts.

Implemented child-to-parent order:

1. `security_incident`
2. `budget_adjustment`
3. resolved `budget_object_debt`
4. consumed/quarantined `owner_evidence_inbox`
5. `idempotency_claim`
6. reconciled/released `budget_reservation`
7. `attempt_admission_context` — technical FK child required before attempt deletion
8. terminal `tool_attempt`
9. unreferenced `retry_guard`
10. unreferenced `budget_distinct_object`
11. unreferenced `retry_equivalence_group`
12. unreferenced expired `budget_window`
13. unreferenced expired `application_session`

`approval_link` and alias relations are not present in the repository and are not fabricated by this increment.

`manifest_snapshot` remains append-only and is deliberately not purged. Its invariant-preserving retention path remains `EVIDENCE PENDING`.

## Safety gates

- candidate attempts must be terminal and older than the governed cutoff;
- attempts referenced by audit events are retained;
- VERIFIED owner evidence is retained;
- ACTIVE object debt is retained;
- batches are bounded to 1..1000;
- selection is ordered and uses `FOR UPDATE SKIP LOCKED`;
- parent records are removed only when no surviving references remain;
- execution is exposed to the maintenance role only through a `SECURITY DEFINER` function owned by the existing NOLOGIN retention owner.

CI/PostgreSQL evidence demonstrates a complete terminal graph can be purged without violating the manifest immutable boundary and that unbounded batches are rejected.

No PET deviation is introduced.

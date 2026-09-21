# RetryLoopGuard success classification

This change addresses repeated successful equivalent invocations being rejected
as `TOOL_SELECTION_STALLED`. It is based on R3 commit
`a7e534e656a0601f44eeed6c59d182166e1fe33f`, independently of telemetry PR #35.
No deployment or merge is part of this change.

## Normative basis and interpretation

Reality Baseline Package v1.7 contains MCP Server PET v1.4 and Cross-Module
Alignment Matrix v1.7. MCP PET section 14 illustrates the equivalent loop
starting with mismatch; canonical OUF-E2E-009 explicitly exercises repeated
invalid input preserving its semantic error. PET sections 74, 76 and 80 and
MCP-A43 distinguish initial admission from a persisted equivalent-retry
classification. MCP-A39 preserves the UNKNOWN admission barrier.

The PET does not explicitly specify the repeated-success case. This patch
adopts a conservative interpretation: an authoritatively successful attempt
does not establish a retry; every other prior attempt in the same group and
window does. This includes pending work, not only failures, so parallel
invocations cannot evade the existing threshold before results arrive.
This interpretation requires review before merge; it is not a claim that
the complete implementation already conforms to every PET requirement.

## Admission behavior

- Keep the existing serializable transaction, window/guard locking, semantic
  group identity, threshold 3 and idempotency handling.
- Keep `blocking_attempts` and any existing `blocked` flag authoritative.
- Count prior attempts whose persisted state is not `SUCCEEDED`, with a
  threshold-bounded query. Reject when that count plus the candidate reaches
  the existing threshold. Do not clear failure history after a success.
- Persist `is_equivalent_retry` on the admission context for new attempts.
  Historical classifications remain NULL rather than being invented from
  present-day outcomes. Historical attempt states still protect admission.
- Preserve `equivalent_attempts` as the legacy count of admitted invocations;
  it no longer decides the stall. This avoids rewriting historical counters
  or modifying recovery/compensation paths.
- Read recovered outcomes through the same authoritative attempt state.
  UNKNOWN/UNRESOLVED protection and debt accounting remain unchanged.

The old `blocked=true` update on denial was rolled back when `Reserve`
returned its error. Repeated denial was caused by recomputing the threshold,
not by that update persisting. The ineffective update is removed.

## Scope and remaining gaps

The ordinary tool-call, byte and object budgets still apply to successful
invocations. The current adapter derives their caps from `RetryThreshold`;
this pre-existing coupling is deliberately unchanged. With the current cost
profile, a fourth call may produce budget exhaustion even after this fix.
This patch does not promise unlimited polling or fix connector latency.

The full PET active/completed equivalent-retry counters and conditional
retry-budget accounting, HMAC alias convergence, and validation-before-admission
coverage are separate alignment work. This patch does not certify those gates.

Migration 008 is additive: a nullable classification column and an index on
group/attempt. It does not rewrite attempts, clear a guard, or alter any live
database. Apply through the normal migration workflow only during a separately
approved deployment. The previous binary can continue inserting its original
columns; those admissions have unknown historical classification.

## Verification

`TestRetryClassification` covers three successful equivalent calls without a
stall, failure and in-flight loops stalled at the unchanged third attempt,
repeat denial, persisted classification, replay/conflict and preservation of
the ordinary budget. Existing lifecycle/recovery/debt/concurrency tests remain
mandatory. The lifecycle fixture now tests the stalled loop in a separate
pending group rather than relying on a successful call consuming retry quota.

Run the repository CI with PostgreSQL 17 and Go 1.25.13, including
`scripts/verify.sh`, race tests and cross-module fixtures. No local Go or
PostgreSQL runtime was available in the preparation workspace; CI results
must be reviewed before any acceptance/deployment decision.

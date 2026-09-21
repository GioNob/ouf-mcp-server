# Independent admission window budgets

This PR builds on RetryLoopGuard PR #36 without modifying its threshold or
classification. It separates cumulative quotas from each request's worst-case
cost and from RetryThreshold. Previously every call could implicitly change
the shared cap through Maximum multiplied by RetryThreshold.

## PET traceability

MCP PET v1.4 sections 11–12 require independent cumulative dimensions and
atomic reservations. Section 78 provides the initial defaults and code-level
hard caps. Matrix v1.7 retains MCP ownership of orchestration budgets and
requires equivalent governance for channel-neutral execution.

| Existing accounted dimension | Window default | Code and DB hard cap |
| --- | ---: | ---: |
| Tool calls | 20 | 50 |
| Cumulative result bytes | 5 MiB | 20 MiB |
| Distinct objects, conservative scalar upper bound | 1,000 | 5,000 |

Kernel invocation and operational producer aggregation supply the same
server-owned profile. It is not exposed as a client tool argument. The existing
one-minute UTC window and RetryThreshold=3 remain unchanged. No environment
variable, live policy, secret or service configuration is modified.

Admission requires explicit valid limits. It persists them on first creation
of a window and rejects inconsistent limits on later admissions rather than
changing quotas or creating a new window. The existing natural key remains
unchanged. Maximum remains a per-request reservation, never a cumulative cap.
All tests of consumption include reserved usage; object admission also sums
authoritative ACTIVE debt rather than trusting its cache. Zero-object
diagnostic calls remain possible at object saturation, subject to other caps.
Subtraction-based comparisons fail closed on invalid or overflowing values.

## Migration and rollout boundary

Migration 009 adds nullable window cap columns with DB hard-cap constraints.
Historical windows retain NULL caps and all existing accounting. New admissions
into them fail closed with ErrBudgetPolicyMismatch until the existing window
expires. Replay and recovery of already-admitted attempts remain available.
No counter, claim, debt or guard is reset or deleted.

This is a controlled cutover, not a mixed-version rolling-admission rollout:
old binaries do not enforce the new cap columns. Quiesce old admission writers,
apply migrations using the established workflow, then start the new binary.
During rollback, do not resume old admission writers inside windows already
used under the new policy; wait for their admission windows to expire while
preserving recovery obligations. No live operation is performed by this PR.

Default tool-call capacity increases from the accidental 3-call cap to the
PET initial profile of 20. Result-byte and object capacities use fixed limits
instead of varying with the current capability. These are intentional policy
changes for separately reviewed representative-environment acceptance.

## Tests and remaining scope

PostgreSQL regression tests exercise 20 successful equivalent calls followed
by budget denial, replay at quota, capability/envelope/threshold changes,
independent byte/object reservations, zero-object diagnostics, immutable window
caps, legacy windows and concurrent admissions. Debt tests now explicitly set
object budgets rather than using retry thresholds to simulate quota. Existing
retry fixtures retain threshold 3; their ordinary cap of 3 is now explicit.
Unit tests cover code-level hard caps, overflow-safe arithmetic and propagation
through orchestration and operational aggregation.

This PR covers only the three dimensions already represented in Cost and the
window ledger. Graph calls, mismatch quotas, cumulative elapsed time, full
active/completed retry-budget accounting, exact distinct-object sets, and the
pre-existing manifest-based policy identity remain separate PET alignment
items. It is not a claim of full AgentBudget PET conformance.

Verification uses Go 1.25.13 and the existing CI PostgreSQL 17 gate, including
race, recovery, debt, cross-module and Reserve p95 checks. Do not deploy or
merge automatically after CI success.

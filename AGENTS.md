# Current MCP Go baseline — owner decision 2026-10-30

Use the current stable supported Go release, pinned exactly per candidate with
checksum and all existing security/compatibility/conformance gates. Owner:
“Va benissimo usare l'ultima versione di go. Anzi, direi che è auspicabile.”
Initial resolved release: Go1.27.3, official release commit
2aae1000ea1d4b28923b96db28acf4e3b99d6aff. This controlled addendum supersedes
the Go1.25-only baseline in Matrix1.7/MCP1.4 Part II §65; the original DOCX remain
unchanged. Do not ask again for routine Go version updates under this decision.
SDK1.7.0, protocol2026-07-28, ownership and all release/M2 gates remain unchanged.
Central decision:
https://github.com/GioNob/ouf-deploy/blob/fix/registry-bound-owner-lineage-20261023/docs/decisions/OUF_MCP_GO_BASELINE_ALIGNMENT_DECISION_20261030.md
A new source/build/package is required; old Go1.26.10 artifacts remain historical.
Source CI does not prove official conformance, two real clients or target E2E.
CSV loading ends at UDP/all row outcomes; UDP capabilities operate separately
on loaded objects. No target authority/grant/budget/reset/replay is implied.

# OUF project instructions

## Default resilience requirement

The project owner's decision of 2026-10-11 requires bounded resilient pause/resume by default throughout OUF. Follow [OUF resilience policy v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and the module PET.

For explicitly transient failures, persist progress and retry state, pause dependent work, and retry with incremental backoff and jitter. Defaults: 5/15/60/300/900 seconds, up to20% jitter, maximum128 attempts and24hours per operation/incident, including attempt duration. Respect Retry-After; operation-specific finite profiles must be versioned and justified. Never hold shared locks/DB transactions during waits or reset budgets/checkpoints on restart.

Retries require read-only/idempotent operations or owner-proven deduplication using the same idempotency key. Ambiguous mutation outcomes require reconciliation. Permanent errors, explicit revocation, integrity/identity/configuration drift and unknown errors block automatic retry.

Keep the process observable while paused; separate liveness from readiness. Revoke/expire unproven traffic leases, and restore only with fresh evidence and valid authority/current generations. Never replay consumed activation claims, extend zero TTL, or bypass IAM/HUMAN/THS. Exhausted retry budgets remain paused for intervention, with deduplicated durable issues and no secret-bearing logs.

Verify hours-long outage/recovery, cancellation/revocation while paused, finite budgets and restart/reconciliation behavior. Record implementation limits and evidence in GitHub; documenting the rule is not proof that existing runtimes comply.

## Quarantine and governed recovery

Follow policy v3: isolate the smallest provably independent scope, preserve durable input/progress/evidence and leave independent services/jobs alive. Exhaustion stops automatic retries and keeps one persistent incident OPEN; scheduler/restarts/new job IDs do not bypass incident budgets. Advance checkpoints past quarantine only when the owner contract explicitly permits a durable terminal quarantine.

An authorized HUMAN may grant a NEW finite budget after exhaustion. Persist a new session with actor/reason/decisionRef/limits and the same logical operation/incident; retain exhausted history and cumulative attempts. Preserve operation idempotency across identical retries; changed intent/configuration is explicit governed reprocessing. Development delegation is not runtime HUMAN authority.

Use atomic expected-version transitions and persistent fencing: one active recovery per logical scope, no double operator launch or publication by stale workers. Ambiguous effects require reconciliation; new budget never overrides revocation, custody, grants, admission pressure or consumed claims.

Limit concurrency by dependency/source/tenant, protect control capacity, avoid retry multiplication, resume backlog gradually. Quarantine has bounded capacity/retention, owner/priority/age metrics, deduplicated issues and admission backpressure before saturation. No silent loss or unauthorized discard.

Verify human budget renewal after exhaustion, concurrent commands, stale/late workers, crash/effects/dedup across sessions, ordering and saturation. Record code evidence and unclosed integration limits in GitHub.

## Operational Awareness, incident correlation and alerts

Follow policy v3 and MCP PET1.4 §33. Separate incidents, events, affected jobs and notifications: one unavailable dependency with5,000 dependent objects is one owner incident with impact counts, not5,000 duplicate incidents. Correlate only proven equivalent causes within authorized scope; preserve owner identities.

Prioritize unresolved incidents by severity/action/verified impact before bounded selection, with deterministic ties; expose truncation and unavailable producers without false HEALTHY/NO_INCIDENTS. Persist operator catch-up checkpoints separately from login; do not mark unseen/failed pages as read or acknowledgement as resolved. Respect retention and disclose history gaps.

Detect systemic pressure using current windowed error rates, breadth, duration and capacity/queue age, not historical issue totals or operator absence. Scope circuits/backpressure, protect control capacity and use hysteresis plus gradual recovery.

Alerts are actionable, deduplicated and configuration-governed, with finite durable delivery budgets/outbox and safe payloads. Delivery/acknowledgement/recovery are distinct. Do not introduce a new incident authority or send real alerts merely while developing these capabilities. Qualify storm/catch-up/retention/capacity/concurrency/notification failure and record remaining deployment gaps.

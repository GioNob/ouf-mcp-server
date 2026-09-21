# Retry and budget candidate acceptance

## Candidate composition

This candidate combines these reviewed source commits without merging the
source PRs or changing the live installation:

| Source | Commit | Purpose |
| --- | --- | --- |
| PR 35 | d0a2675eb306eff5b84126f0a09fa645649cd149 | Current live telemetry baseline |
| PR 36 | a2a36817132da925ca996cd7d9cb5299f0526bbc | Independent successes and persistent retry classification |
| PR 37 | ce5101ed6b42bd25972d686beac908f87f4b7956 | Independent pinned window caps |

The candidate preserves PR 35 telemetry while retaining its safe logging test.
It keeps RetryThreshold=3, Gateway mediation, Authorization and policy v14.
No application timeout, credential or issuer configuration changes are included.

## Joint PET review

MCP PET v1.4 sections 14, 77–80 and MCP-A39/A43 govern retry admission,
uncertainty and accounting. Independent successful calls do not consume the
equivalent loop guard; already-classified retries remain charged on success.
UNKNOWN/UNRESOLVED blockers and owner-evidence recovery remain mandatory.
Section 78 supplies the initial calls/bytes/objects profile and hard caps.
Matrix v1.7 keeps the Gateway-mediated channel-neutral path and owner truth.

The source PR documents explicitly retain the open alignment items: full retry
budget counters, graph/mismatch/time dimensions, exact object sets, policy
identity and HMAC aliases. This candidate is a bounded correction, not a full
PET-conformance certification. Existing readiness only reads migration metadata;
explicit successful migration is a deployment prerequisite.

## Isolated executable gate

Run `bash scripts/verify-release-isolated.sh` from a clean candidate checkout
with Go 1.25.13, git and Docker. The exact old commit must be available locally.
The script compiles test executables from the old and candidate sources and
uses one new disposable PostgreSQL 17 container with network=none, no published
ports and tmpfs data. It never reads or forwards runtime database URLs or
credentials and does not attach the container to ouf-backend.

The sequence is:

1. The real old PostgreSQL adapter runs its lifecycle fixture and creates
   migrations 001–007 plus synthetic attempts, claims, budgets and evidence.
2. The candidate verifies version 7 and populated data, applies 008/009 twice,
   and compares canonical row digests of every pre-existing OUF table except
   the migration ledger. Only additive columns are omitted from comparison.
3. Verify a legacy window rejects new admission without receiving new quota;
   old successful idempotency claims still replay; new scopes receive the new
   budget profile.
4. Run candidate retry, exact 20/21 call boundary, cumulative quota, concurrent
   admission, debt, recovery, lifecycle and summary-reservation tests.
5. Run the real old adapter again on schema 009 using fresh synthetic scopes.
   This checks additive-schema compatibility, including old positional inserts.

The script prints source commits, test executable hashes, PostgreSQL image ID,
`UPGRADE_007_TO_009_PASS`, `ROLLBACK_SCHEMA_COMPATIBILITY_PASS` and finally
`ISOLATED_RELEASE_ACCEPTANCE_PASS`. It removes only its own container, anonymous
volumes and temporary compilation directory on exit. No production data is
copied, and no live fault injection or revocation test is performed.

The dedicated CI job runs this same script. The existing main job still runs
all PostgreSQL/race/performance and cross-module gates, including channel-neutral
operational summary and authorization fixtures. Static test binaries in the
isolated job do not replace the race checks in the main job.

## VPS procedure and acceptance boundary

Work in the SSH session as oufadmin. First verify the live/rollback image names,
repository status and installed Go version without printing environment or
inspect payloads. Fetch the candidate from GitHub and create a separate worktree;
do not switch or edit the live deployment checkout. Run the isolated script
from that worktree and record its exact commit and PASS markers.

This gate does not bind the candidate to live IAM/Gateway, exercise real-user
permissions, measure connector latency, or establish production RPO/RTO.
Existing cross-module CI fixtures are synthetic acceptance evidence. A separate
reviewed live cutover and smoke plan is required after VPS results are available.

## Live cutover constraints for subsequent approval

Quiesce all old admission writers before applying 008/009. Preserve all existing
consumption, claims, evidence and UNKNOWN/UNRESOLVED obligations. Historical
windows have NULL caps and will reject new candidate admissions until their
existing expiry; do not delete them to accelerate the cutover. Keep old rollback
image/container references. Migration rollback is forward-compatible code
rollback, not DROP COLUMN or migration-ledger deletion.

Before resuming old admission writers, wait out windows used under the new
policy and account for in-flight work; expiry does not forgive object debt or
uncertainty. The old-schema compatibility PASS uses fresh synthetic scopes and
does not prove that immediate same-window production rollback is safe.

Do not remove `ouf-mcp-rollback-before-latency` or the prior rollback image during
candidate acceptance. Cleanup of sensitive temporary deployment files follows
the existing handoff only after they are no longer needed.

# Retry and budget candidate acceptance

## Candidate composition

This candidate combines these reviewed source commits without merging the
source PRs. The initial preparation did not change the live installation;
the later authorized cutover is recorded below:

| Source | Commit | Purpose |
| --- | --- | --- |
| PR 35 | d0a2675eb306eff5b84126f0a09fa645649cd149 | Pre-cutover live telemetry baseline |
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

### VPS without host Go or direct Docker socket access

The VPS has the `golang:1.25.13` image, not a Go executable on the host PATH.
Run the script as oufadmin with `OUF_RELEASE_GO_MODE=docker` and
`OUF_RELEASE_DOCKER_SUDO=1`. Only Docker commands use sudo; host git and
temporary files remain owned by oufadmin. The compiler container runs with
oufadmin's UID/GID and mounts only the candidate source read-only and the
run's temporary build directory. It receives no Docker socket or live secrets.
Build networking is needed for Go module downloads; PostgreSQL remains on
network=none. No host Go installation or persistent PATH modification is needed.
CI exercises both native and container Go modes through the sudo Docker path.

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

## Recorded VPS and live acceptance — 2026-09-22

Evidence below comes from operator-provided VPS command output in the deployment
session, including a read-only SQL transaction for the five live attempts.
Deployed source: `2ea473c8310add50a2d2b19a2dddd2e47f01000c`.
Subsequent documentation commits do not change the deployed binary.

### Isolated gate and authorized cutover

- VPS isolated gate using container Go and sudo Docker passed:
  `UPGRADE_007_TO_009_PASS`, `ROLLBACK_SCHEMA_COMPATIBILITY_PASS`,
  `ISOLATED_RELEASE_ACCEPTANCE_PASS`.
- Pre-cutover database archive indexing and an isolated restore check passed.
  A second quiesced backup was taken and its archive checked after clean stop.
  The quiesced archive was not separately restore-tested.
- Runtime configuration was saved. Before/after stop checks found no attempts
  in ADMITTED, RUNNING, UNKNOWN or UNRESOLVED states.
- After explicit operator authorization, candidate migrations applied successfully;
  the migration ledger contains versions 1–9 and the new retry/cap columns exist.
- Live container `ouf-mcp` runs image `ouf-mcp:2ea473c`; liveness and readiness
  returned HTTP 204. Previous `ouf-mcp:d0a2675` container is retained as
  `ouf-mcp-rollback-before-retry-budget`; earlier rollback containers remain.
- Candidate binary SHA-256:
  `2da5a2310acfc931180983601765bb6d35dc054b54fa1bda1b7ce2d4d648d86f`.
  Binary copied back from the built image matched this hash.

### Live independent-success classification: PASS

Five sequential independent `ouf.system.status` calls were checked by correlation
ID using LEFT JOINs from five expected IDs to tool_attempt,
attempt_admission_context and budget_window in a READ ONLY transaction.
All five were found exactly once, capability matched, state was SUCCEEDED and
is_equivalent_retry was false (not NULL).

| Call | Correlation ID | State | is_equivalent_retry | UTC window |
| --- | --- | --- | --- | --- |
| 1 | 72c622a8-a366-4f30-b4a3-e1ceec6d7e22 | SUCCEEDED | false | 06:54:00–06:55:00 |
| 2 | 8de03233-e7b9-4146-8aa5-456eb5f2241d | SUCCEEDED | false | 06:54:00–06:55:00 |
| 3 | 2bb0a4fe-4d3e-4d55-b76c-38a0261a6a14 | SUCCEEDED | false | 06:54:00–06:55:00 |
| 4 | e3c435b9-576c-4c59-91c4-2373f5512b63 | SUCCEEDED | false | 06:54:00–06:55:00 |
| 5 | a9dd6836-678c-47b6-9fa2-3ceb36ec9fe4 | SUCCEEDED | false | 06:55:00–06:56:00 |

Calls 1–4 share equivalence group
`55a2a083-da72-46e8-a3bf-a515d5ffb25e`.
Call 5 belongs to `dc86f504-c9bd-4e45-a01b-dbe16debf7df` in the next window.
The fourth successful independent call in the same group/window was admitted
despite RetryThreshold=3, confirming the targeted live regression correction.

This is not a same-idempotency-key replay test, a live failure/retry test or a
live 20/21 window-cap boundary test. Those behaviors retain their isolated-test
evidence; the SQL above does not independently verify budget consumption.
Full PET alignment gaps and rollback constraints above remain open.

### Separate latency investigation

The five connector invocations took 7.117–11.189 seconds; MCP handlers took
13–19 ms. Temporally associated Caddy HTTP 200 responses took about 17–23 ms
(no shared correlation ID in those Caddy records). MCP Inspector returned the
same public operational status in 229 ms, using a different OAuth client and
network path. This does not identify the exact component causing the delay.
A support report was submitted; no technical resolution or ticket number has
been confirmed. Diagnostic Caddy access logging remains enabled at this checkpoint.

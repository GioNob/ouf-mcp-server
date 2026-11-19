# Profile-bound work budgets and OntoPiA composition

Status: source candidate; no target deployment or capability activation. The three
`source.analysis.*` descriptors and three `semantic.discovery.*` descriptors are
INACTIVE. Registry discovery PR41, Gateway discovery composition (included in the
1024-column Gateway PR86), and MCP PR83 must be qualified and deployed together.
No external candidate has been adopted and no Teatri source has been recreated.

## What the tools do

- `source.analysis.plan`: obtain the first immutable COLUMNS page from Onboarding
  using the actor's governed delegation; bind asset/profile/version/schema hash
  and the **owner's** column count. Add up to 128 work items with expected revision.
  Later batches reuse that verified immutable profile with fresh authorization.
- `source.analysis.read`: return cumulative budgets, task progress and column
  coverage in pages of 128 tasks. No evidence body or original intent is returned
  by this metadata listing.
- `source.analysis.step`: execute one already planned task via the existing
  authorization, admission, Gateway and owner contracts. Persist its bounded
  response. A completed task is reread from the ledger after fresh permission
  checks, without replaying the owner call.

Task arguments are validated against the actual embedded capability schema.
Allowed tasks are semantic.search/get and discovery.request/status/candidates.
No arbitrary endpoint, SPARQL, new publication permission or approval path is added.
A discovery request gets an owner idempotency key generated from the stable
analysis scope and exact intent, never from a new conversation or caller key.

The plan's columns are zero-based ordinals of the verified profile. Identical
canonical tool/argument work is deduplicated and can serve multiple columns.
Changing a query adds explicit work and consumption; it does not erase old tasks.

## Dynamic budget, precisely

For the set of unique planned tasks T:

- useful-call allowance = |T|;
- useful-call time envelope = sum of each task's manifest hardMaxElapsedMs;
- useful-result envelope = sum of each task's manifest hardMaxResultBytes.

These are envelopes of **call work**, not elapsed-time SLAs or measured VPS
performance. There is no fixed count of calls granted per conversation and no
unqualified assumption that every column costs the same number of searches.
Adding a batch extends the plan by the exact new work; reusing a search adds no
call. The first owned-profile read is setup overhead, separately admitted under
the existing schema-page budget, not a semantic task.

For example, 400 distinct searches and 70 distinct reference reads require 470
useful calls. If the same work serves 1024 columns, it still requires 470 calls.
Further discovery or candidate verification is appended explicitly; candidate
counts are not guessed in advance. This example is arithmetic, not a benchmark.

Observed calls, duration, bytes, attempts and window waits survive reconnects.
An unchanged analysis is identified by service/principal/tenant plus asset/profile,
independently of conversation, correlation or manifest checksum. Profile or
manifest drift blocks reuse instead of creating a fresh budget automatically.

## Pacing, retries and capacity

The 60/minute semantic-read admission window remains a dependency-protection
limit. A proven pre-dispatch window refusal stores WAITING and notBefore; it
creates no owner call and consumes no task retry attempt. Existing command,
control and equivalent-query guards remain enforced.

READ HTTP 429/503 responses use 5/15/60/300/900-second backoff, up to 20% positive
jitter, Retry-After and durable ceilings of 128 attempts/24 hours per task. A new
conversation cannot reset them. Existing admission guards can hold an operation
before these ceilings. Unknown transport errors, ambiguous commands, expired
in-flight leases and failed reconciliation are held for reconciliation, never
blindly replayed. No new HUMAN reprocessing/incident authority is implemented.

The implementation returns wait state; it is **not a background scheduler** and
persists no delegation token. A later step needs a currently authorized caller.
No DB transaction or shared lock is held during a Gateway call or wait. A
transactional claim and fencing token prevent concurrent/late workers publishing
the same task outcome.

Finite capacity safeguards in version ouf.profile-analysis.v1 are 16384 distinct
planned tasks per analysis, 128 items per batch/page, 8192 argument bytes per task,
262144 result bytes per call and 64MiB retained response accounting per analysis.
These are capacity ceilings, not empirical workload estimates. In-flight result
reservations count against storage capacity. Reaching capacity holds new work;
there is no silent eviction, truncation, automatic new analysis or unlimited
extension. Native tests qualify up to 1024 planned searches, not the complete
16384-task/64MiB saturation workload or concurrent multi-tenant VPS throughput.

## Column semantics and OntoPiA

The intended analysis targets the **property represented by the column name**.
For `tipologia`, inspect the entity context, definition, domain, range and intended
meaning before choosing a property. Equal labels do not establish equivalence;
a vocabulary of allowed cell values is a separate concern.

First search published OUF artifacts and read their exact immutable references.
Where necessary, request external PROPERTY/RELATIONSHIP discovery with a clear
intent. Inspect the actual candidate URI, provenance, definition, domain/range,
expiry and gaps. Do not fabricate a URI if no appropriate candidate exists.
Registry's candidateId/candidateRef is temporary and is not an approved semanticId.
Adoption, validation and human-governed publication remain separate owner flows.

Asynchronous discovery observations are explicit planned status tasks numbered
1..128 for one request. Later observations require the previous observation to
have completed and are at least 31 seconds apart. Normal polls are distinct planned
work; retry history stays attached to each observation and underlying admission
still applies. The caller must inspect PENDING/terminal owner state before
planning candidate reads; successful HTTP alone does not finish discovery.

`columnsWithCompletedPlannedWork` only counts finished planned calls. It does not
claim a complete semantic assessment, a successful discovery job, an approved
mapping or an OPEN data classification. Saved responses have completion times and
are historical evidence; verify current owner state before adoption/publication.

## Qualification and remaining gates

The dedicated profile-analysis workflow runs native Go race tests, the actual MCP
SDK, PostgreSQL migration014, reconnect/progress, tenant ownership, revocation,
concurrent claim fencing, bounded retry history and admission wait cases. It also
covers 20/50/100/200/300/400/1024-column plans and eight 1024-column plan batches using
one owned-profile read. Gateway/owner responses in coordinator tests are fixtures;
this is not an OntoPiA provider or target SeaweedFS E2E test.

Remaining work in order:

1. Qualify the final composed source head and package it with the matching
   Onboarding/Gateway/Registry candidates; preserve migration checksums.
2. Establish current target provider configuration, worker readiness, publication
   and user discovery grants; activate only the composed capability set.
3. Run a real property discovery and inspect returned evidence for the retained
   Teatri asset/profile. Record unresolved columns explicitly.
4. Complete approved mapping and the original CSV-to-UDP path twice, retaining
   per-row outcomes and demonstrating normal application reads/writes on SeaweedFS.

No target rollout, live provider availability, automatic HUMAN recovery after
HELD, all-column semantic mappings or complete CSV-to-UDP E2E is claimed here.

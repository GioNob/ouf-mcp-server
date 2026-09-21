# R3 Operational Awareness — live cross-module diagnostic runbook

Date of consolidated evidence: 21 September 2026.

Normative references: OUF Reality Baseline Package v1.7, MCP PET v1.4,
Gateway PET v1.5, Ingestion PET v1.3, Authorization PET v1.5 and Cross-Module
Alignment Matrix v1.7. This runbook records implementation/acceptance experience;
it does not override those documents.

## Purpose

Use this runbook when a conversational-client call reaches MCP but a nested
owner call fails, is partial, or disagrees with another producer. The method is
designed to preserve fail-closed behavior and isolate one layer at a time.

## Layered triage order

1. Record exact UTC timestamp and correlation/request IDs.
2. Check grant validity window before changing anything.
3. Read APISIX raw access/error logs for parent and producer calls.
4. Establish whether failure is before APISIX, at APISIX, at producer receipt
   validation, at owner re-evaluation, or during owner data projection.
5. Compare a working producer and a failing producer from the same parent call.
6. Verify DNS aliases/upstream target before debugging application arguments.
7. Verify live dynamic APISIX route from etcd/Admin API; repository state alone
   is not runtime evidence.
8. Verify receipt key wiring by fingerprint only.
9. Verify owner policy snapshot and the exact resource context used for
   fine-grained evaluation.
10. Verify capability registry separately from ACTIVE PolicyBundle.
11. Change only one layer at a time and preserve rollback.

## Observed R3 topology

`client -> /mcp -> ouf.operations.summary -> {gateway summary, ingestion summary}
-> module owners -> aggregate projection`.

A successful 21 September 2026 call produced HTTP 200 on all four mediated
hops and an aggregate `HEALTHY, partial=false` response. A resolved historical
Gateway incident remained visible without making current health DEGRADED.

## 403 decision tree

If parent=403 and one producer=403 while another producer=200:

- do not assume parent authorization failed;
- do not assume grant expiry;
- inspect the failing owner's receipt filter and owner re-evaluation;
- verify the producer capability grant and any *second* owner-side capability.

Ingestion R3 requires both producer admission
`ouf.ingestion.operations.summary` and owner-side `operations.status.read`.
The latter evaluates operational resources and is not implied by the OIDC scope
of the same name.

## 503 decision tree

If connector reports `503 INVALID_ARGUMENT`:

- inspect APISIX error log first;
- verify DNS/upstream;
- only after confirming upstream reachability inspect request schema/body.

The observed permissions-read failure was DNS drift
`ouf-source-onboarding -> ouf-onboarding`, not invalid ROLES input.

## Dynamic APISIX inspection

APISIX config is stored in etcd. In a minimal etcd image, invoke
`/usr/local/bin/etcdctl` directly with `docker exec`; do not require a shell.

Before route changes, snapshot the route, construct a candidate, prove the
expected delta, apply through APISIX Admin API, and read back the route.

## Receipt diagnostics

Never print receipt keys. Compare fingerprints/lengths. Account for final
newlines. Verify:

- producer-specific key env in APISIX;
- owner key file path and mount;
- UID/GID numeric readability;
- issuer, audience, tenant, workload, capability, body hash, path and decisionRef;
- policy version used by the owner.

A valid HMAC receipt is admission context, not authorization bypass: the owner
must re-evaluate the current policy.

## Policy change workflow

1. Save the complete ACTIVE envelope.
2. Confirm capability registry state.
3. Register a missing immutable descriptor through HUMAN THS.
4. Build the next PolicyBundle from the complete prior bundle.
5. Prove no pre-existing capabilities/grants changed or disappeared.
6. Create draft; verify `baseActiveRef`, version, ETag/revision and full diff.
7. HUMAN publish.
8. Read ACTIVE back and verify consumer refresh.
9. Run live client E2E.
10. Preserve evidence and rollback.

The 21 September v14 change was additive: one descriptor
`operations.status.read` plus four temporary test grants.

## Evidence hygiene

Keep: timestamps, status codes, correlation IDs, image/commit IDs, policy
version/hash, route IDs, draft IDs/revisions and redacted diffs.

Do not keep in tickets/docs/chat: bearer tokens, receipt keys, APISIX admin key,
OIDC client secrets or unredacted route dumps containing secrets.

## Still-open R3 gates

Positive summary is proven. Keep deny/revocation, partial/staleness,
fault/recovery, restart/reboot, final correlation/audit and external
conversational-client latency as distinct acceptance gates.


## Post-deploy permissions/THS consolidation

The permission/THS route drift was consolidated live after materializing the
verified Gateway R3 source at commit
`1629652163edce7b3bc4c8f80bf29a3ef1b1ab8a`.

The candidate resolved all six affected route IDs to the deployed owner name
`ouf-onboarding:8080`:

- `mcp-permissions-read`;
- `mcp-permissions-propose`;
- `mcp-permissions-status`;
- `authorization-ths-page`;
- `authorization-ths-api`;
- `authorization-ths-login`.

Before deployment, live etcd showed only `mcp-permissions-read` already
corrected; the other five still referenced `ouf-source-onboarding:8080`.
Complete rollback copies of those five live route definitions were saved before
the Admin API update. Only those five route IDs were replaced. A subsequent
etcd read-back showed all six routes pointing to `ouf-onboarding:8080`.

The non-destructive smoke test then invoked
`authorization.permissions.read` with `view=ROLES` as the governed
`ouf-admin` identity. It returned the expected role catalogue, including
`operational-viewer` and its organizational assignment, and performed no
mutation.

This closes the specific runtime DNS/upstream drift for the six permission/THS
routes. It does not by itself close proposal confirmation/publication,
revocation or restart/reboot acceptance.

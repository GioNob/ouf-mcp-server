# Governed incident snapshot HTTP adapter

This adapter follows the [mandatory resilience policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and MCP PET1.4 operational awareness. It wires the existing PostgreSQL cursor metadata store into the private MCP incident owner route; no migration is automatic.

The published `ouf.operations.incidents` input has three closed modes:

- Existing bounded filters (`sourceId`, `state`, `since`, `limit`) preserve legacy partial projections.
- `{"snapshot":true,"limit":50}`, optionally with those filters, starts an independent owner snapshot vector.
- `{"cursor":"opaque-canonical-uuid"}` re-fetches the immutable page. Replacement filters, limits, snapshot flags and null/duplicate keys are rejected.

The root owner-local SDK decision is freshly evaluated and matched to the trusted Gateway decision before metadata or producer I/O. The current decision reference binds metadata together with verified principal/tenant/claims and manifest checksum. Opt-in reads require Gateway-verified HUMAN delegation from the configured workload, unlike legacy projections. Both producers are freshly governed and read back, including completed owners. A cursor cannot grant authority, widen source scope, reset budgets or serve cached payloads.

Replay produces the same committed continuation only while identity, policy and all owner/self observations still match. Changed observations return409, unknown/other-principal references404, expired nodes410, metadata capacity429 and unavailable owners/store503. Errors disclose no token, grant or incident details. No-store/nosniff headers apply. A bounded HTTP attempt can be retried later by the caller under the existing finite control budget; there is no handler retry loop or held DB transaction during owner calls.

Coverage remains an independent owner vector, not an atomic platform snapshot. `consultationRecorded=false` on all pages: reading or returning a page is no proof the client/human received it. Publication-boundary bookmarks and a separate explicit delivery/checkpoint capability remain OPEN. An incomplete legacy projection cannot become a successful strict cursor page.

Qualification must cover native race tests, fresh root revocation, identity fencing, bounded input/duplicate rejection, stable replay after adapter reconstruction, unchanged metadata on owner outage/mutation and subsequent safe recovery. Existing native PostgreSQL metadata/composition/control tests and full CI remain mandatory.

Cross-repository rollout additionally requires the Gateway's global query schema and closed operational Lua validator to accept the same opt-in modes, then current pairwise/APISIX qualification. This repository adapter alone is not evidence of an active ChatGPT or VPS deployment. No email, target migration/restart or R9/R10 replay is performed.

The actual MCP typed tool decoder preserves snapshot/cursor fields, and qualification includes the official MCP client through stateless transport, schema validation, root orchestration/admission and a Gateway port fixture. Mixed/invalid and summary opt-in arguments never reach admission. The separate Gateway gate exercises real APISIX/OIDC; PostgreSQL HTTP tests charge root plus both owners across metadata-store reopen. None of these fixtures are a target deployment.

If a repeated root idempotency key returns admission replay metadata without a payload, the MCP tool reports an explicit retryable OPERATIONAL_READ_REQUIRES_FRESH_AUTHORIZED_ATTEMPT error rather than a successful empty page. A fresh READ must still pass current authorization, finite persistent admission and the unchanged equivalence guard. No cached incident payload is invented or replayed.

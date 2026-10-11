# Persistent publication vector metadata v1

Follow [resilience policy v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md), MCP PET1.4 Operational Awareness and the [cursor/confirmation design](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md). This additive private store carries opaque publication positions, never incident payloads, cached grants, client delivery receipts or operator checkpoints.

The profile `ouf.operational-publication-vector.v1` covers exactly GATEWAY and INGESTION owner publication positions, with canonical source/state filters, owner snapshot and items digest, and explicit owner parent tokens. It does not certify historical MCP self events or all seven platform modules. Current urgent OPEN state remains an independent read. Do not expose this profile as a complete platform history until its collection/coverage contract is qualified.

The composer must obtain complete, unredacted current owner coverage before issuance. Store metadata alone is not a proof that the client saw any page. No public handler, manifest/tool opt-in or automatic migration is added by this tranche; consultationRecorded remains false in existing reads.

Binding uses verified stable principal/tenant/actor/service/issuer/audience, independent of login or conversation. Root and producer authorization must be freshly evaluated outside the store. Policy/manifest drift conflicts, wrong identity is not found and expired positions report a gap. Revocation is never overridden by a bookmark.

Resume lineage compares each exact owner parent token to its saved position under identical canonical filters. No UUID ordering, timestamps, invented revisions or changed filters can prove progress. Unchanged owner boundaries reuse the parent token; concurrent equivalent chains reuse one live successor. History remains immutable: issuing a READ position never changes the older operator checkpoint.

Finite capacities:512 retained positions per principal/tenant,4,096 per tenant,32,768 globally,4KiB metadata,90-day maximum expiry. Each admission may remove one expired position; live records/expiry cannot be rewritten or deleted. Store work has a two-second local bound and try-lock admission; no waits, remote calls, or new retry loop inside a transaction. Capacity does not disable existing-position replay or the separate temporary cursor store.

Required native gates: stable identity across login plus current policy binding; duplicate/unknown/payload/secret-free metadata; exact owner lineage and nonmonotone UUID values; PostgreSQL reopen, concurrency/replay/idle dedup, immutable live metadata/expiry, finite capacity with independent cursors, expiry/gap and bounded cleanup. Public composition, complete owner proof validation, client confirmation COMMAND/CAS, actual history compactor and target SDK/HA remain separate gates.

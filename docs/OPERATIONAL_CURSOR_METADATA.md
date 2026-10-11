# Operational owner-vector metadata — private foundation

Mandatory [platform resilience policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and [cursor design](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md). Status: native qualification pending; cross-owner composition and public query exposure OPEN.

The existing MCP PostgreSQL adapter stores only bounded owner token/offset/page-index/digest/snapshot/coverage metadata, typed filters and MCP's own fixed public self-state. No producer incident payload, delegation token, previously granted authority or operator acknowledgement is cached. Every future composition must refetch and currently authorize owner pages before applying these positions.

Identity fingerprint canonically binds verified principal/tenant/service/actor/authentication context, issuer/audience/scopes/claims. Equivalent set ordering and timezone do not change it; changed identity evidence, policy reference or manifest invalidates the cursor. Server-generated opaque UUIDs expire after30minutes; expiry, scope and metadata are immutable. These snapshots are not persistent cross-login consultation bookmarks.

Only after remote calls finish may the owner commit a computed page digest/next-position node. Append-only expected-node CAS chooses one continuation; concurrent equivalent replay returns the same token, changed page or metadata conflicts. Completed nodes cannot reopen, positions cannot rewind, owner snapshot/scope/filter/self-state cannot change, and hidden/partial coverage cannot become falsely complete. This records result consistency, not HTTP delivery, human reading, alert acknowledgement or incident resolution. Canceled/unseen reads do not advance an operator checkpoint.

Finite reference metadata profile:2second DB attempt, at most200page advances,100items/page position budget,16sessions/principal,128sessions/tenant,4,096sessions total;800nodes/principal,4,096nodes/tenant and32,768total nodes, metadata≤16KiB/node. Session admission removes at most one expired session. Capacity fencing uses a short nonblocking advisory lock; never hold it during producer calls, policy callbacks or waits. No new deployable/broker or incident store.

Additive migration008 creates private metadata/session tables with immutability and referential guards. Native PostgreSQL qualification covers restart/reopen, principal/tenant/authentication/policy/manifest replay denial, concurrent append/replay/result changes, immutable expiry/metadata, final/scope/position fencing, finite session admission and bounded expired cleanup. It preserves existing lifecycle/admission/maintenance contracts. No migration has run on the VPS.

Public global cursor composition still needs the bounded k-way merge, current owner-page digest checks, fresh source-specific authorization, explicit incomplete selection and appropriate whole-request deadlines. A consultation confirmation write additionally needs its own reviewed capability/route, finite idempotency/CAS and actual client delivery proof; none is synthesized by READ or by this store. Owner publication-boundary resume bookmarks and their retention proof remain OPEN. No production SDK grants, network/HA or release acceptance is inferred from metadata tests.


## Native qualification — 2026-10-11

[PR54](https://github.com/GioNob/ouf-mcp-server/pull/54) merged as `61643ba0d4d5b6bdadf61537af96c141125319df` after all eight push/PR gates passed at candidate `85ccdb9fe356d4f3fa4b70d158fc6f86109fd2c3`. Dedicated PostgreSQL/race run [38123408694](https://github.com/GioNob/ouf-mcp-server/actions/runs/38123408694) passed two identity/frame groups and six real PostgreSQL groups; full lifecycle, maintenance, current pairwise, Authorization conformance, static analysis and vulnerability gates remained enabled and passed. No target migration or operator consultation is implied.

The bounded composition primitive is under review in [PR55](https://github.com/GioNob/ouf-mcp-server/pull/55), with separate [design and qualification scope](OPERATIONAL_CURSOR_COMPOSITION.md). Public wiring and delivery-confirmation capability remain open.

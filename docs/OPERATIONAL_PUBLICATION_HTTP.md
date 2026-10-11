# Publication coverage over HTTP and MCP

The existing `ouf.operations.incidents` capability exposes the qualified private publication-vector composer. Native transport qualification is required before integration. This change performs no target apply, migration, client checkpoint, consultation or alert.

## Closed modes

- Initial: `{"snapshot":true,"coverage":true,"sourceId":"source-a","limit":100}`; optional sourceId/state/limit, no since.
- Page: `{"cursor":"<canonical opaque UUID>","coverage":true}`; no changed filters.
- Resume: `{"resumeFrom":"<global publication bookmark>","limit":100}`; saved filters are inherited.

Legacy reads remain separate. Null, duplicate, unknown and mixed fields fail before producer I/O. Summary rejects these modes. Manifest validation and the typed MCP decoder preserve identical arguments.

## Authority and bounds

Fresh root authorization, verified delegation and current producer authorization remain mandatory. The inherited Ingestion source remains in SDK resource evaluation even when wire requests contain opaque positions. Existing persistent control READ admission and finite call, size and deadline bounds apply; there is no automatic retry loop or direct producer bypass.

Temporary pages are login-bound. Durable positions use stable principal/tenant/workload/issuer/audience bindings and can resume after a new login subject to current policy/manifest and owner retention. Expiry returns 410 with an explicit history gap; invalidation 409, unavailable owners 503, capacity 429, invisible positions 404. Failures never become successful empty pages.

Coverage is exactly GATEWAY and INGESTION; historical MCP self incidents and other module histories are not covered. Read current urgent owner state separately. READ metadata does not prove client delivery, HUMAN consultation, acknowledgement or resolution. Trusted client receipts and separate checkpoint commands remain an open integration gate.

## Qualification scope

HTTP tests exercise complete chains, handler recreation, new login after temporary-page expiry, inherited source scope, owner lineage, revocation, cross-principal replay, expired history, outages and malformed requests. The official MCP SDK client verifies typed forwarding and pre-admission rejection. Existing PostgreSQL vector/cursor/control groups and all repository checks remain required. Mock transport tests do not prove target runtime health or all-module coverage.

See [composition](OPERATIONAL_PUBLICATION_COMPOSITION.md), [vector](OPERATIONAL_PUBLICATION_VECTOR.md), [legacy transport](OPERATIONAL_CURSOR_HTTP.md), and [platform policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md).


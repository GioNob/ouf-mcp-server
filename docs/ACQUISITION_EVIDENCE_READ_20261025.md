# Acquisition evidence read bindings

Adds two dedicated minimized READ tools for an existing exact `sourceId` and UUID `runId`: `ouf.ingestion.acquisition.read` and `urban.acquisition.read`. Each page is bounded to100 records and1MiB, with a12-second call limit and owner-specific cursor. No raw SQL, URL, mutation, activation, retry or arbitrary owner path is accepted. Existing operational and protected historical contracts remain unchanged.

Kernel preflight binds tenant/source/run, module, OPEN metadata and TENANT_OPERATIONAL to the respective owner permission (`ingestion.lineage.read` or `lineage.object.read`), then uses a dedicated fixed Gateway path. This is an admission check only: each owner must additionally require all contributing run/contract/lineage/object permissions and actual labels. The policy48 operational-read grant alone never authorizes these tools. Owner403 remains an error and is not turned into empty success or automatic retry. Snapshot pagination/redaction remains explicit; neither tool asserts whole E2E acceptance.

Tests cross the real MCP SDK, current local policy, orchestration admission and actual TLS HTTP adapter. Wrong subject, tenant, source, run, scope and client authority overrides are denied before dispatch; owner refusals remain errors. Owner results in those transport tests are synthetic. Native owner PostgreSQL/receipt and composed Gateway qualification are separate gates. No target deployment, permission publication or acceptance is implied by this candidate.

Related: GioNob/ouf-deploy#50, GioNob/ouf-ingestion-runtime#47, GioNob/ouf-udp-object-resolution#48. Existing source ACTIVE and two SUCCEEDED runs are retained; these reads must inspect them before any new acquisition.

# R3 — incident timeline and bounded catch-up

Baseline: Ingestion PET v1.3 §46, MCP PET v1.4 §33, Gateway operational-awareness contract. This increment covers persistent incidents and incident catch-up; it does not declare all R3 matrix gaps closed.

The Inspector client used for latency diagnosis is temporary test equipment. It is not a production Keycloak configuration or a prerequisite of this implementation.

Deployment requires coordinated review of all three branches. No production deployment or merge is performed by this change.

## Catch-up contract

ouf.operations.incidents accepts limit (1–100), since/until (maximum 30-day window), sourceId, jobId (UUID), state, severity and opaque cursor. Pagination visits INGESTION then GATEWAY, one producer per page. A page may contain fewer than limit items and still have hasMore=true. Consumers must follow nextCursor until hasMore=false and accumulate the partial flag from every page.

Owner cursors remain inside a bounded wrapper bound to tenant, principal and filters, with 15-minute expiry. Each page invokes the owner through governed Gateway dispatch and reauthorizes. An unavailable or malformed producer returns partial=true plus a retryable cursor for that producer; it never produces a complete empty result. Denial fails closed. Only whitelisted operational projection fields survive; raw evidence/logs do not pass through. MCP's instantaneous system status is not fabricated into a persistent incident.

The summary tool and its existing availability semantics remain separate. This does not add pagination to ingestion run-history or close unrelated R3 capabilities.

## Verification

The Go suite covers producer page boundaries, no overflow loss, unavailable/malformed producers, identity/filter binding, projection minimization and existing orchestration/retry behavior. Release requires coordinated owner/Gateway changes and the native CI gates. No deployment is part of this PR.

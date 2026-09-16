# MCP 1G — Multi-worker concurrency release gate

Normative baseline: Reality Baseline Package v1.7, MCP Server PET v1.4. This increment closes the repository/CI portion of the PET multi-worker release gate; deployed multi-Pod behavior remains EVIDENCE PENDING.

| Release-gate obligation | Evidence | Expected invariant |
|---|---|---|
| idempotency/claim contention | `TestConcurrencyGateIdempotencySingleClaim` | one durable claim/attempt/reservation; no split identity |
| dispatch CAS contention | `TestConcurrencyGateDispatchCASAndReconcileExactlyOnce` | exactly one dispatch wins the expected lock version; loser receives CAS miss |
| concurrent reconciliation | `TestConcurrencyGateDispatchCASAndReconcileExactlyOnce` | reservation/budget reconciled exactly once; no double decrement/consume |
| concurrent recovery workers | `TestConcurrencyGateRecoveryWorkersSingleClaimAndDebt` | one recovery owner per attempt; one blocker and one ACTIVE debt row |
| debt/maintenance vs admission | `TestConcurrencyGateAuthoritativeDebtBeatsStaleCacheDuringAdmission` | ACTIVE debt is authoritative even if the cache is stale; admission cannot forgive debt |
| concurrent debt-cache maintenance | `TestDebtCacheReconciliationConcurrentPostgres` | convergence to authoritative ACTIVE debt and one repair audit |
| concurrent late compensation | existing `TestDurableLifecycle` concurrent `CompensationService.Apply` assertions | one compensation revision; replay cannot double compensate |
| cleanup/retention worker contention | 1G audit/retention tests with `FOR UPDATE SKIP LOCKED` | bounded workers do not duplicate purge accounting |
| global deadlock/race guard | `scripts/verify.sh` (`go test -race ./...`) plus bounded test contexts | no detected Go race; PostgreSQL operations complete within gate timeout |
| deployed multi-Pod contention | Kubernetes runtime evidence | EVIDENCE PENDING |

## Lock-order rule exercised

For governed lifecycle mutations the effective order remains: budget window → retry guard/equivalence state → attempt/reservation/idempotency state. Recovery and compensation paths must not invert this order. `SKIP LOCKED` is used where work claiming rather than strict serialization is intended.

## Acceptance

Repository/CI closure requires the full workflow to pass on PostgreSQL 17, including pairwise checks, `scripts/verify.sh`, migration, one-shot maintenance, staticcheck and govulncheck. Any serialization abort is treated as a safe transient contention outcome only where the database deliberately rejects a concurrent serializable transaction; it must never produce duplicate durable accounting.

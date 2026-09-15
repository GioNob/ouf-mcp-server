# MCP Server 1D — deterministic attempt recovery

Normative baseline: MCP Server PET v1.2 §§77, 80 and 111, with Gateway-mediated owner lookup from §§2.1 and 38.

| PET control | Executable evidence |
|---|---|
| MCP-A34 crash recovery | PostgreSQL integration test covers both `ADMITTED/NOT_DISPATCHED` expiry and `RUNNING/DISPATCHED` expiry. |
| MCP-A39 unknown retry block | same idempotency tuple returns outcome-unknown and a different key in the same equivalence group returns group-locked. |
| Exclusive multi-Pod recovery | candidates are claimed with a persisted owner/lease and `FOR UPDATE SKIP LOCKED`; a second worker receives no candidate. |
| Owner proof | result is accepted only for the exact persisted `backend_request_id`; mismatched evidence cannot mutate the attempt. |
| No blind retry | owner query failure preserves `UNKNOWN`, releases only the recovery lease and leaves reservation/blocker intact. |
| `OWNER_PROVES_NO_DISPATCH` | attempt becomes `FAILED/NOT_DISPATCHED`, reservation is released without consumption, claim completes and the attempt's blocker is decremented. |
| Deterministic terminal result | owner `SUCCEEDED`/`FAILED` reconciles only actual cost not exceeding the reservation and produces `ACKNOWLEDGED`. |
| Gateway mediation | maintenance uses a governed `RecoveryClient`; no owner URL is derived from attempt or tool input. |

Evidence Inbox, unresolved-object debt, `maxUnknownHold` transition and late compensation remain explicitly deferred to the later PET tranche; no local TTL forgives uncertainty.

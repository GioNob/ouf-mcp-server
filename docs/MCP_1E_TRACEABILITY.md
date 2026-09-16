# MCP Server 1E — Evidence Inbox and late compensation

Normative baseline: MCP Server PET v1.2 §§23.1, 75, 77, 80 and 111.

| PET control | Executable evidence | Status after CI |
|---|---|---|
| MCP-A41 unresolved object debt | first `UNKNOWN` converts the reserved object maximum into one ACTIVE debt row and cached debt; `maxUnknownHold` produces `UNRESOLVED` without decrementing blocker or debt | Candidate VERIFIED |
| MCP-A45 late compensation | application API accepts only attempt ID, evidence ref and expected revision; authoritative payload, actor and outcome are read from the inbox; adjustment, exact set, debt, attempt and blocker change in one transaction | Candidate VERIFIED |
| MCP-A47 debt scope | debt, evidence and adjustment retain attempt/window foreign keys; compensation rechecks attempt/window/owner/backend binding | Candidate VERIFIED |
| MCP-A48 concurrent revision | attempt revision CAS, unique evidence consumption and single adjustment prevent duplicate debt release or blocker decrement | Candidate VERIFIED, CI PostgreSQL decisive |
| MCP-A50 owner no-dispatch | verified `OWNER_PROVES_NO_DISPATCH` is structurally constrained to `FAILED/DISPATCH_NOT_STARTED`, zero objects and no result | Candidate VERIFIED at schema/store boundary; Gateway 1E pairwise confirms canonical zero-cost proof |
| MCP-A51 violation quarantine | adversarial hash-version evidence commits `CRYPTO_VIOLATION`, quarantines evidence and proves debt/blocker remain unchanged; count/overflow share the same guarded branch | Candidate VERIFIED |
| MCP-A52 authenticated ingress | MCP adapter derives owner/verifier from trusted identity; stable Gateway 1E `2ffb3e9584021ddbfbc35e18a4607e6a19c5a016` derives owner from trusted owner-workload identity, requires `mcp.evidence.submit`, canonicalizes evidence, rejects forged owner and unsupported hash version before MCP I/O, and preserves deterministic replay/conflict semantics. `internal/evidence/gateway_pairwise_test.go` verifies valid ingress, identical replay, conflicting replay, forged owner, hash-version rejection, no-dispatch proof and concurrent same-evidence convergence. | PAIRWISE VERIFIED; overall MCP-A52 remains PARTIAL pending deployed IAM/DB role-denial evidence |

## MCP-A52 evidence boundary

CI run #60 for PR #9 passed the pinned Gateway 1E ↔ MCP 1E micro-pairwise together with the pre-existing Gateway/MCP/UDP pairwise, full `scripts/verify.sh`, PostgreSQL 17 migration/maintenance checks, staticcheck and govulncheck.

This closes the software-contract and cross-module pairwise portion of MCP-A52. It does **not** prove production identity issuance, workload-to-role membership, database role denial or deployed network-policy isolation. Those are environment-backed PET evidence and must not be inferred from repository tests. MCP-A52 therefore cannot yet be promoted to unqualified `VERIFIED`.

The database migration creates segregated NOLOGIN roles and grants, but this repository does not claim production IAM membership evidence. Detached evidence signatures and a new trust store are intentionally absent because the PET defines authenticated Gateway/workload identity as the bounded TCB.

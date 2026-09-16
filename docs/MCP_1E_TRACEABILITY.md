# MCP Server 1E — Evidence Inbox and late compensation

Normative baseline: MCP Server PET v1.2 §§23.1, 75, 77, 80 and 111.

| PET control | Executable evidence | Status after CI |
|---|---|---|
| MCP-A41 unresolved object debt | first `UNKNOWN` converts the reserved object maximum into one ACTIVE debt row and cached debt; `maxUnknownHold` produces `UNRESOLVED` without decrementing blocker or debt | Candidate VERIFIED |
| MCP-A45 late compensation | application API accepts only attempt ID, evidence ref and expected revision; authoritative payload, actor and outcome are read from the inbox; adjustment, exact set, debt, attempt and blocker change in one transaction | Candidate VERIFIED |
| MCP-A47 debt scope | debt, evidence and adjustment retain attempt/window foreign keys; compensation rechecks attempt/window/owner/backend binding | Candidate VERIFIED |
| MCP-A48 concurrent revision | attempt revision CAS, unique evidence consumption and single adjustment prevent duplicate debt release or blocker decrement | Candidate VERIFIED, CI PostgreSQL decisive |
| MCP-A50 owner no-dispatch | verified `OWNER_PROVES_NO_DISPATCH` is structurally constrained to `FAILED/DISPATCH_NOT_STARTED`, zero objects and no result | Candidate VERIFIED at schema/store boundary |
| MCP-A51 violation quarantine | adversarial hash-version evidence commits `CRYPTO_VIOLATION`, quarantines evidence and proves debt/blocker remain unchanged; count/overflow share the same guarded branch | Candidate VERIFIED |
| MCP-A52 authenticated ingress | stateless adapter derives owner and verifier from trusted identity, canonicalizes hashes, and provides deterministic replay/conflict semantics | PARTIAL until deployed Gateway/IAM role-denial evidence exists |

The database migration creates segregated NOLOGIN roles and grants, but this repository does not claim production IAM membership evidence. Detached evidence signatures and a new trust store are intentionally absent because the PET defines authenticated Gateway/workload identity as the bounded TCB.

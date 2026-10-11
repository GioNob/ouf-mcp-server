# MCP private operational audit capture v1

Follow [resilience policy v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) and MCP PET1.4 §§33–34. This increment persists minimal owner transitions in the existing append-only PET audit journal. It creates no new incident authority, public capability, notification, recovery grant, publication watermark or client receipt.

## Atomic capture

Migration010 attaches observation triggers to tool_attempt and security_incident. Attempt transitions into UNKNOWN/UNRESOLVED and their owner-proven terminal reconciliation append audit rows in the same transaction; rollback removes both. Same-state lease/metadata changes produce no extra operational rows. The stable attempt UUID identifies the uncertainty incident across transitions. A terminal FAILED result clears execution uncertainty only; it does not claim successful work or resolve a separate domain/security incident.

Security capture follows successful incident INSERT only, so ON CONFLICT dedup produces one observation. It stores generic protected-attention metadata, never category/detail, raw incident key, evidence hash, result/credential/payload, correlation or arbitrary outcome code. Security evidence remains protected; this private audit profile is not authorization to expose it through generic operational READ.

A NOLOGIN definer role can append audit rows and read only the attempt columns needed to bind the security observation. It cannot update attempts, budgets, debts, evidence or audit records. Functions have a fixed search_path and no PUBLIC execute grant. No new tenant/publication counter lock is acquired inside admission/recovery transactions; no external I/O or wait is added.

## Retention and incomplete coverage

The existing bounded, owner-governed audit-retention path and FK graph remain authoritative. There is no new spool or private cleanup bypass. New audit evidence may intentionally keep a terminal attempt until its audit retention completes. Historical capture starts with this explicit migration; existing history is not backfilled or certified retroactively.

Audit sequence and observation timestamp are not completeness boundaries. An earlier allocated audit row may commit after a later one; the native concurrency test demonstrates this. Commit-safe publication, current authorization/redaction, retention-floor/gap and capacity qualification, paging/global vector integration, historical MCP coverage and trusted client checkpoint remain OPEN. Responses must continue mcpSelfHistoryCovered=false and consultationRecorded=false.

## Native gates

PostgreSQL17 tests cover lifecycle capture, metadata storm suppression, immutable/safe payloads, atomic rollback, late lower private sequence, security dedup and least-authority role ACLs. Complete PostgreSQL and repository tests/race, retention/evidence/admission regressions, static analysis and vulnerability gates must pass before integration. No target migration, VPS operation, real alert or consumed R9/R10 replay is performed.

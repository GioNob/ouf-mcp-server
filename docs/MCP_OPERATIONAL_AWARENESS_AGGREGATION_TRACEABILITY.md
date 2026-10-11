> **Policy di piattaforma vigente:** [Resilienza, quarantena, incidenti e alert — v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md). Obbligatoria per sviluppo e revisione; [adozione e prove](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_ADOPTION.md).

# MCP Operational Awareness aggregation traceability

Normative baseline: OUF Reality Baseline Package v1.7, MCP PET v1.4 and Cross-Module Alignment Matrix v1.7.

## PET / Matrix requirements

- MCP owns human-facing Operational Awareness aggregation and conversational projection, not producer truth.
- Producer modules remain authoritative for their own incidents/status.
- Human-facing capabilities remain Gateway-mediated and channel-neutral.
- Missing/unavailable producer evidence must never be collapsed to HEALTHY or NO_INCIDENTS.
- Security-sensitive/raw producer evidence must not leak through the global projection.

## Implementation evidence

- `ouf.operations.summary` and `ouf.operations.incidents` are MCP-owned tool capabilities.
- Private owner endpoints `/api/internal/v1/mcp/operations/summary` and `/incidents` perform cross-producer composition.
- Ingestion and Gateway are queried through separate Gateway-mediated producer capabilities, each using an independently governed nested invocation.
- MCP self-state is read from the MCP authoritative store inside the MCP owner backend; it is projected as a safe module summary or one synthetic operational incident, without security-incident detail leakage.
- Producer failure produces `partial=true`, lists `unavailableProducers`, and forces aggregate summary to `DEGRADED`.
- Gateway configuration also exposes OIDC API bindings to the same MCP owner endpoints, preserving channel neutrality.

## Test evidence

- Unit tests cover healthy/recovering cross-producer composition, unavailable producer partial/degraded behavior, and MCP security-detail non-disclosure.
- Pairwise test validates the MCP manifest against the merged Gateway aggregation routes and producer-specific owner boundaries.

## Evidence pending

- Real Authorization decisions for aggregate and nested producer capabilities.
- Deployed OIDC/IAM/NetworkPolicy behavior and end-to-end chatbot/UI execution.
- Production load/capacity impact of nested operational queries.


## 11 ottobre 2026 — resilienza v3 e prima tranche Operational Awareness

[Policy canonica v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md) obbligatoria, [registro con prove](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_ADOPTION.md) e [piano di allineamento](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_RESILIENCE_PLAN.md). Comprende retry finiti, quarantena/budget umano, un incidente per dipendenza con lavori coinvolti separati, catch-up persistente, crisi su segnali attuali e alert deduplicati.

Integrate MCP PR51, Ingestion PR35 e Gateway PR59: priorità prima dei limiti, recuperi vecchi visibili nel catch-up supportato, conteggi Gateway dell'intera finestra e troncamento esplicito. Prove: MCP full38105960598/race38105960600 e gate sicurezza con0 vulnerabilità raggiungibili dopo aggiornamento Go/dipendenze; Ingestion native21/module137 test; Gateway229 test + APISIX reale. [Evidenza](https://github.com/GioNob/ouf-deploy/blob/main/evidence/operational-priority-qualified-20261011.json).

Restano OPEN cursor/checkpoint operatore, grouping multirun per dipendenza/contatori impatto, detector/quote/capacità, outbox alert e integrazione target multi-owner/autorizzazioni. Reference/fixture e summary di finestra non provano conformità globale o salute corrente. Nessun deploy VPS o email; R9/R10 consumati e precedente preflight sospeso. Le accettazioni precedenti restano preservate.


## Owner scope and nested read identity — 2026-10-11

Nested producer invocations now carry an explicit server-resolved operational resource: tenant and module, TENANT_OPERATIONAL plus sourceRef for Ingestion; RESTRICTED_OPERATIONAL and PLATFORM incident scope for the Gateway control plane. The Ingestion-only sourceId is not forwarded to the Gateway reference producer as an invented source filter. Other supported filters remain unchanged. Fine-grained owner authorization and independent producer grants still apply; a global read grant does not imply a platform control-plane grant.

Nested idempotency keys bind parent attempt/correlation, capability and the SHA-256 digest of canonical owner arguments. Equivalent reads retain their key; changed query/page arguments get a different claim. Independent calls with no parent identity receive a fresh server-generated identity. This does not reset the existing persistent admission/equivalence window or silently retry a failed producer.

The paired producer baselines now include qualified owner snapshots: Gateway `85b91415f6f5abab617e78c9baa5628c74921b89` (PR60; 243 reference tests, 2 explicit skips, real APISIX proof) and Ingestion `25de72ce1b01307378874ae8aef73e9f87dd0969` (PR37; 37 focused PostgreSQL tests and all18 push/PR gates). Global MCP endpoints continue to publish the existing bounded query schema. They do not yet accept a global cursor, record a consultation receipt, or certify complete publication-revision catch-up. An owner snapshot token is not a global operator bookmark.

Qualification for this MCP increment is pending native full/race/security/pairwise workflows. Three added regression groups validate owner-specific resources and filters, stable/distinct nested keys and refusal before I/O on invalid internal query shapes.

**Punti di ripresa persistenti:** fondazione vettore e composizione qualificati (PR59/60); [accesso HTTP e tool MCP tipizzato](docs/OPERATIONAL_PUBLICATION_HTTP.md) implementato. Prove e gate correnti nel ledger di adozione. Copertura storica GATEWAY/INGESTION; ricevute client, checkpoint di consultazione e altre storie owner restano aperti.

> **Policy di piattaforma vigente:** [Resilienza, quarantena, incidenti e alert — v3](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md). Obbligatoria per sviluppo e revisione; [adozione e prove](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_ADOPTION.md).


**Resilience v3 — prima tranche integrata:** priorità incidenti e catch-up qualificati per i perimetri MCP/Ingestion/Gateway; [prove e limiti](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_ADOPTION.md), [piano operativo](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_RESILIENCE_PLAN.md). Checkpoint di consultazione, correlazione dipendenze e alert reali restano aperti.

**Seconda tranche integrata e qualificata:** autorizzazione owner e proiezioni incomplete MCP; osservazione persistente delle modifiche Ingestion. [Prove e limiti](evidence/operational-read-catchup-qualified-20261011.json), [design cursor/consultazione — OPEN](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_OPERATIONAL_CURSOR_DESIGN.md).

**Consultazione operativa:** [cursor e autorizzazione HTTP](docs/OPERATIONAL_CURSOR_HTTP.md), [budget control persistente](docs/OPERATIONAL_CONTROL_BUDGET.md), [delega aggregate](docs/OPERATIONAL_AGGREGATE_DELEGATION.md). Il checkpoint di consultazione e il rollout Gateway/target restano separati; leggere una pagina non la marca come vista.

# OUF MCP Server

## Installazione e collaudo del collegamento ChatGPT

La [guida Keycloak, ChatGPT e policy MCP](docs/INSTALLAZIONE_KEYCLOAK_CHATGPT.md)
documenta i binding `ouf-lab`, gli scope/mapper, il Device Flow amministrativo,
le procedure policy/deploy/rollback e i limiti del collaudo del 18 settembre 2026.
Il [verbale del difetto cache](docs/CONSTRAINED_POLICY_CACHE_INCIDENT.md)
separa la correzione software dall'accettazione live ancora bloccata.

MCP 1A/1B implements the protocol kernel and durable lifecycle defined by MCP Server PET v1.2 in Reality Baseline Package v1.6.

- Go 1.26.9 and official MCP Go SDK v1.7.0 are pinned.
- `/mcp` accepts only MCP 2026-07-28 Streamable HTTP modern/stateless POST requests.
- `server/discover`, `tools/list` and closed JSON Schema descriptors are provided by the official SDK.
- Legacy initialize/session transport is disabled.
- No SQL, arbitrary query language, network destination or backend bypass is exposed.
- PostgreSQL persists immutable manifest snapshots, application sessions, idempotent attempts and append-only audit events.
- Expired `RUNNING` leases become `UNKNOWN`; application sessions remain separate from transport sessions.
- Migration is an explicit role; application processes never auto-migrate.
- Tool execution remains fail-closed until Gateway mediation and authorization land in MCP 1C.

Run `scripts/verify.sh` with Go 1.26.9. Commands are `ouf-mcp migrate`, `ouf-mcp server`, and `ouf-mcp maintenance-worker` and require `MCP_DATABASE_URL`.

See [MCP 1A traceability](docs/MCP_1A_TRACEABILITY.md) and [MCP 1B traceability](docs/MCP_1B_TRACEABILITY.md).
## MCP 1C runtime configuration

The governed server requires `MCP_AUTHORIZATION_BUNDLE_ENDPOINT`, `MCP_GATEWAY_ENDPOINT`, a renewable workload identity, and a minimum 32-byte fingerprint key. Production workload identity uses `MCP_OIDC_TOKEN_ENDPOINT`, `MCP_OIDC_CLIENT_ID`, and `MCP_OIDC_CLIENT_SECRET_FILE`; the client-credentials access token is cached only until shortly before expiry and the client secret file is reread on refresh so rotation does not require a process restart. `MCP_WORKLOAD_TOKEN` remains a compatibility path for tests only. Production fingerprint material should use `MCP_FINGERPRINT_KEY_FILE`; `MCP_FINGERPRINT_KEY` remains for test compatibility. Service endpoints are deployment configuration, never tool arguments; production endpoints must use HTTPS and redirects are disabled.

See `docs/MCP_1C_TRACEABILITY.md` for the bounded PET coverage and explicit deferrals.

The maintenance worker additionally requires `MCP_GATEWAY_RECOVERY_ENDPOINT` and the same renewable workload identity configuration. It never contacts an owner directly: outcome lookup is Gateway-mediated and keyed by the persisted backend request ID.

MCP 1E preserves object-budget uncertainty as durable debt. `MCP_MAX_UNKNOWN_HOLD` defaults to `24h`; expiry changes `UNKNOWN` to `UNRESOLVED` without releasing debt or retry blockers. Only verified Evidence Inbox records can drive late compensation. See `docs/MCP_1E_TRACEABILITY.md`.


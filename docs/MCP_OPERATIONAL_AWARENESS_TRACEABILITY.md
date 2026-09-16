# MCP Operational Awareness — PET v1.3 traceability

Normative baseline: MCP PET v1.3 and Cross-Module Alignment Matrix v1.6.

## Implemented evidence

- Adds typed MCP tools `ouf.ingestion.status`, `ouf.ingestion.history`, `ouf.operations.incidents`, and `ouf.operations.summary`.
- Tools remain normal governed invocations: Authorization first, admission/budget, Gateway mediation, reconciliation and audit.
- Operational inputs are typed and bounded; no raw SQL, PromQL, arbitrary URL or diagnostic endpoint is accepted.
- `ouf.operations.summary` is the first chatbot catch-up capability for persisted faults that occurred while the client was offline.
- Tool descriptions explicitly preserve authorization/redaction and partial-evidence semantics.

## Evidence boundary

The current vertical slice aggregates Ingestion operational state only. Cross-module `ouf.system.status`, Gateway-owned incidents, explain/dereference and live notification remain follow-up work. Real Authorization integration remains `EVIDENCE PENDING` until the Authorization module is implemented.

# Source-scoped ingestion reads

Status and history now bind their declared `ingestion.operations.read` permission to MCP's local policy evaluation. Transport capability IDs, Gateway binding references, request arguments, fingerprints and admission budgets retain the original `ouf.ingestion.status` / `ouf.ingestion.history` identity.

The collection preflight uses the ingestion owner's existing `operational` resource type, trusted tenant and `module=INGESTION`, `detailLevel=TENANT_OPERATIONAL` attributes. An optional validated source filter contributes `sourceRef`. A source-only grant therefore cannot authorize a different source or an unfiltered collection. No source ID is treated as authority, and each owner still independently filters/authorizes returned rows and job references. Job-only grants may remain denied at collection preflight because no job identity is established there.

This patch changes only those two tool bindings. It neither changes grants nor authorizes configuration activation, token issuance, CSV ingestion or lineage access. Other capabilities retain their current mapping. Fresh revocation, missing scopes, wrong tenants/subjects, client attempts to select a permission, and downstream owner refusal remain fail-closed.

Qualification exercises the real MCP SDK, kernel, current local authorization cache, admission and HTTPS adapter against a synthetic Gateway response. It is not deployed Gateway/owner qualification, an explanation of the historical production denial, a Teatri run, an ACK or a thirteen-row CSV acceptance. Deployment still requires the existing governed runtime path. Missing lineage capability and semantic publication remain separate gates.

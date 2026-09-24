# R4a governed admin permission proposal client

`scripts/r4a_admin_permission_proposal.py` submits one already-planned
Authorization permission change through the public MCP boundary.

It is deliberately proposal-only:
- HUMAN login uses Keycloak device flow client `ouf-human-admin`;
- requested scopes include `mcp.connect`,
  `authorization.permissions.propose` and `authorization.proposal.read`;
- token claims are checked locally for issuer, Gateway audience, protected admin
  subject, tenant, HUMAN actor, ACR and required scopes;
- the client calls public `https://api.ouf-lab.it/mcp`;
- it verifies `authorization.permissions.propose` is present in `tools/list`;
- it invokes `tools/call` with a stable caller-supplied idempotency key;
- it accepts only a PENDING proposal receipt;
- it never calls the owner internal endpoint and has no confirm/reject operation.

The request file is produced by Source Onboarding
`scripts/r4a_temporary_grant_proposal.py` and contains exactly:
`toolName`, `arguments`, and `expectedBasePolicyRef`.

## Dry validation

```bash
python3 scripts/r4a_admin_permission_proposal.py \
  --request-file /tmp/r4a-urban-object-search-grant.request.json \
  --idempotency-key r4a.urban-object-search.giovanni-chatgpt.1
```

Expected: request metadata and `NO_NETWORK_CALL=true`.

## Submit proposal

```bash
python3 scripts/r4a_admin_permission_proposal.py \
  --request-file /tmp/r4a-urban-object-search-grant.request.json \
  --idempotency-key r4a.urban-object-search.giovanni-chatgpt.1 \
  --device-login \
  --submit
```

Authenticate as `ouf-admin`. A successful call reports:
- `MCP_TOOL_AVAILABLE=true`;
- proposal UUID, revision, state and expiry;
- THS approval path;
- `ACTIVE_NOT_PUBLISHED=true`;
- `THS_CONFIRMATION_REQUIRED=true`.

The idempotency key must be reused for an exact retry of the same request. A
different payload must use a new key. The proposal expires after the owner-defined
window (currently 15 minutes), so the THS review should immediately follow.

No bearer token or owner receipt is printed or persisted.

# R3a — renewable MCP workload identity

Normative baseline: Reality Baseline Package v1.7; Authorization PET v1.5; MCP Server PET v1.4 Go.

## Problem closed by this increment

The deployed Keycloak access tokens used by OUF workload clients are short-lived. Persisting one access token in `MCP_WORKLOAD_TOKEN` would make the MCP Server start successfully and then lose Authorization bundle refresh and Gateway access when that token expires.

Production therefore must not depend on a static bearer token.

## Production contract

The MCP Server can obtain its own workload token with OAuth 2.0 client credentials using:

- `MCP_OIDC_TOKEN_ENDPOINT`
- `MCP_OIDC_CLIENT_ID`
- `MCP_OIDC_CLIENT_SECRET_FILE`

The token source:
- requests `grant_type=client_credentials`;
- never logs or persists the access token;
- caches the token only while it remains safely away from expiry;
- refreshes before expiry;
- rereads the client secret file for every refresh, so secret rotation does not require process restart;
- rejects empty/incomplete token responses and unsupported token types.

The same token source is shared by:
- Authorization PolicyBundle bootstrap/refresh;
- MCP -> Gateway dispatch;
- maintenance/recovery Gateway calls.

`MCP_WORKLOAD_TOKEN` remains supported only as a compatibility/test path.

Fingerprint material should be supplied through `MCP_FINGERPRINT_KEY_FILE`; the environment value remains a test-compatibility fallback.

## Acceptance boundary

Repository CI proves token-source behavior and client wiring. Deployed acceptance still requires:
- real Keycloak client credentials using the existing `ouf-mcp-server` client;
- secret file mounted/readable without printing its value;
- successful token acquisition from the real issuer;
- Authorization bundle bootstrap;
- token renewal after the first token lifetime;
- Gateway call after renewal;
- secret rotation/revocation acceptance remains part of AUT-04/R6 unless explicitly exercised earlier.

No HUMAN token or credential is stored by MCP.


## CI history

Initial PR #26 run `35323708501` reached the repository verification gate with functional tests passing, then failed because `gofmt` changes were required in `policy_bundle.go` and `token_source_test.go`. No runtime behavior failed. The branch was updated to the exact canonical Go formatting before rerunning CI.

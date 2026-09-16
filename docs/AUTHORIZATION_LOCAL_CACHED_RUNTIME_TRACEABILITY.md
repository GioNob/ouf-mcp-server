# MCP local cached Authorization runtime traceability

## Normative intent

This increment replaces the request-path synchronous Authorization service stub with the Authorization PET v1.5 model: policy registry and publication remain owned by Source Onboarding & Configuration, while MCP evaluates the active immutable policy bundle locally through a shared-contract-compatible evaluator.

The implementation remains IAM scenario-neutral. Selection of IAM scenario A/B/C does not alter bundle semantics or the local evaluation contract.

## Implemented evidence

- Source Onboarding main `fb2dd51dfc204577a47c1e702f17531dd0709b3c` exposes the protected active bundle projection at `/api/internal/v1/authorization/policy-bundle/active`.
- Distribution requires a validated `SERVICE` principal with `authorization.bundle.read`.
- MCP uses `MCP_AUTHORIZATION_BUNDLE_ENDPOINT` only for bootstrap and background refresh; it does not call Authorization on each governed invocation.
- MCP startup is fail-closed when no valid active policy bundle can be bootstrapped.
- Refresh uses atomic snapshot replacement; failed refresh retains the last known good bundle.
- Policy rollback to a lower bundle version and same-version/different-bundle collision are rejected.
- Local evaluation enforces tenant equality, capability/operation declaration, allowed actor, required scope, grant validity interval, subject/service binding, and organization binding when present.
- Organization-scoped grants fail closed when no organization resource context is available.
- `decisionRef` is derived from the exact bundle/version/capability and is propagated unchanged into admission and Gateway requests.
- Trusted principal fields required by the Authorization SDK are accepted from Gateway-only context: authentication context ref, issuer, audience and scopes.
- Issuer/audience/scopes are not serialized into the existing MCP→Gateway request body, preserving the frozen downstream wire contract; downstream receives the authorization decision reference.

## CI gates

- `internal/authorization/cache_test.go`: bootstrap fail-closed, exact decision, LKG retention, rollback rejection, org-scope fail-closed.
- `internal/deployment/authorization_mcp_pairwise_test.go`: Source Onboarding SDK + bundle distribution + real MCP local cache pairwise and decisionRef propagation.
- `internal/deployment/authorization_local_runtime_test.go`: prevents reintroduction of the synchronous `MCP_AUTHORIZATION_ENDPOINT` / `NewAuthorization` request-path wiring.
- full `scripts/verify.sh`, including race detector and existing performance/concurrency gates.

## Evidence pending / external integration gate

The following are not claimed by repository CI and remain dependent on the selected IAM scenario and deployed environment:

- real issuer/JWKS and audience validation;
- trusted Gateway injection/stripping of Authorization principal headers;
- workload identity used to fetch policy bundles;
- service-account provisioning and rotation;
- revocation propagation and measured policy-distribution latency;
- deployed NetworkPolicy allowing bundle refresh only from approved workloads;
- multi-Pod cache convergence and fault injection against the real policy registry endpoint.

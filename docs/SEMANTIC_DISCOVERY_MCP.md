# National discovery through chatbot/MCP

This candidate adds `semantic.discovery.request`, `semantic.discovery.status` and `semantic.discovery.candidates`. Request creates an asynchronous job, not a semantic publication. Status and candidates use its exact requestId. Keep the explicit request idempotencyKey stable when repeating the same intent; a changed intent with the same key is a conflict.

All three descriptors remain INACTIVE until coordinated installation acceptance. Tests register candidates in an isolated MCP SDK session. There is no product step requiring SSH, sudo or a terminal on the VPS.

## Component bindings

- Semantic owner: https://github.com/GioNob/ouf-semantic-registry/pull/31, based on availability, caller-owned jobs and provider workload auth (#28–#30).
- Gateway: https://github.com/GioNob/ouf-api-gateway/pull/57, exact schemas pinned in the MCP pairwise CI.
- Fixed execute paths: /internal/capabilities/v1/execute/semantic/discovery/{request,status,candidates}.
- Owner receipt: X-OUF-Semantic-Discovery-Receipt; separate discovery purpose and HMAC domain; installation-owned signing key, absent from MCP.
- Capability descriptors: ouf.semantic.discovery / COMMAND; ouf.semantic.discovery.status / READ; ouf.semantic.discovery.candidates / READ. Each requires the existing scope ouf.semantic.discovery, allows HUMAN, and requires explicit owner/current policy grants.

## Release acceptance still required

The target evidence proves provider disabled and gateway origin absent. Source tests do not prove installed discovery. Installation must explicitly bind provider enabled, Gateway origin, provider client-credentials/token endpoint, read-only credential mount, TLS trust and bounded Gateway provider routes. Owner discovery receipt settings must bind issuer, audience, MCP workload and a private key file; Gateway must bind a distinct receipt key environment name and resolved Semantic upstream.

Release must account for the additive V10 discovery idempotency migration and preserve existing routes/data. Activate the three MCP descriptors only after owner policy and installed route/provider readiness are verified. A governed release capability or authorized deployment operator must apply this configuration; no release capability is supplied by these three discovery tools.

## Chat acceptance

Request CLASS with intent teatro and languages it/en. Read actual PENDING/RUNNING/SUCCEEDED/FAILED states and unexpired candidates, without turning provider failure into empty success. National candidates are suggestions. Adoption, semantic validation/publication and Onboarding approval still require their respective governed product operations and HUMAN trusted surface. This PR does not add those missing lifecycle tools or claim a complete File-to-UDP flow.

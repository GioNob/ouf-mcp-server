# Constrained policy cache refresh — 2026-09-18

## Failure and correction

A published grant with `effect`, `resourceType` and `allowedDetailLevels`
decoded successfully from Onboarding. During `Cache.Refresh`, the JSON deep
copy emitted the absent optional string constraints as empty strings. The
strict decoder then rejected that copy with `blank or invalid grant constraint`.
Repeated failures retained the last good snapshot only until its freshness
deadline; subsequent calls failed with `authorization policy bundle unavailable`.

Optional string fields now use `omitempty`. Explicit blank strings, invalid
types and unknown fields are still rejected on input. The cache still detaches
maps, slices and pointers, rejects same-version mutations, enforces freshness
and requires monotonic versions. Wire integrity remains checked against the
compact original bundle JSON before cache serialization.

Regression coverage includes the incident's constraint shape, omitted/null
optional strings, strict invalid inputs, detached nested constraints and the
real HTTP transport-to-cache path followed by authorization. A different
subject and an unlisted detail level remain denied. Common Java/Go decision
vectors are unchanged.

PET traceability: Authorization §34.2 (scope and permitted detail), the R1b
immutable/cache contract in `AUTHORIZATION_R1B.md`, and MCP fail-closed behavior
for unavailable authorization. Onboarding remains the policy owner; this fix
does not grant any capability or change identity/permission semantics.

## Separate live acceptance blocker

The current `ouf.system.status` tool schema accepts no parameters. The kernel
does not populate a resource `detailLevel`. An explicit constraint allowing
only `PUBLIC_OPERATIONAL` therefore still denies that invocation, correctly
under the current matching contract. The decision's fallback display value is
not a substitute for supplying and enforcing a requested detail level.

Do not remove the constraint or label the existing response public just to
make the call succeed: the owner status provider returns tenant counters and
incident references. A follow-up must define a public projection, propagate
the permitted detail to owner enforcement through the governed Gateway
contract, and verify that restricted fields cannot reach a public result.
The current Gateway capability is classified `TENANT_OPERATIONAL`.

## Deployment and acceptance

Operator evidence reports that version 5 restored the version 3 policy after
the version 4 incident. This change performs no deployment or policy publish.
The restored policy does not include the trial system-status grant; fixing
cache refresh alone does not authorize the ChatGPT user.

Before live acceptance:

1. Merge only after repository CI and common conformance succeed; deploy the
   reviewed MCP image through the installation workflow.
2. Resolve and test the detail projection contract before authorizing a public
   status result. Preserve subject, tenant, scope, validity and detail bounds.
3. Use the existing trusted-human policy workflow to publish a new monotonic
   version based on the current active reference, preserving unrelated grants.
   Do not reuse an expired trial grant or update the database directly.
4. Verify successful policy refresh beyond the cache freshness window, then
   perform an actual ChatGPT tool call. Verify negative subject/scope/detail
   cases as well as the bounded positive result.
5. Remove the temporary admin device-flow token/state files and end the
   temporary admin session after the operator no longer needs them.

`/health/live` and `/health/ready` do not prove that a policy version is loaded:
readiness currently checks the database. Successful OAuth discovery likewise
does not prove tool authorization or owner execution.

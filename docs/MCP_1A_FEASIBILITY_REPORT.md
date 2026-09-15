# MCP 1A — protocol feasibility report

Decision date: 2026-09-15

Decision: **CONDITIONAL NO-GO for product implementation**

## Normative boundary

The MCP Server PET v1.1 is authoritative. Sections 71 and 71.1 require the
modern MCP `2026-07-28` Streamable HTTP profile: stateless requests,
`server/discover`, request-local protocol/capability metadata, no mandatory
`initialize`/`initialized` handshake, and no `Mcp-Session-Id` in the modern
profile. `MCP-A38` requires protocol conformance to that revision. The optional
legacy `2025-11-25` profile is disabled by default and separately tested.

The Gateway PET remains authoritative for mediation, identity propagation,
authorization, quotas and governed dispatch. The MCP server may never turn the
feasibility mock into a direct backend bypass.

## Reproducible findings

| Evidence | Pinned version | Finding |
|---|---:|---|
| MCP PET v1.1 | Baseline Package v1.5 | Requires `2026-07-28`, stateless lifecycle and `server/discover`. |
| Official Java SDK release | `2.0.1`, commit `c7e1cfe90edcd9cbe030924310a194dc5492eab2` | Protocol versions stop at `2025-11-25`. |
| Official Java SDK main | commit `4186ca13d1fe5ecca9b891c35e95863df6762d9f` | No implementation of `2026-07-28` or `server/discover`; roadmap assigns them to 3.x. |
| Official conformance main | commit `7169291ec0b68eb370fddcd9947313ab0d5e4156` | Contains the frozen `requirements/2026-07-28.yaml` and stateless scenarios. |
| Published conformance CLI | `0.1.16` | Exercises the legacy lifecycle; it cannot certify `2026-07-28`. |
| Published TypeScript SDK | `1.30.0` | Compatibility probe operates on the legacy lifecycle. |
| MCP verification lab | Lab 78 | Static 4/4, model 9/9 and PostgreSQL 12/12 pass; all 78 product/Executive Freeze evidences remain pending. |

Local checks completed: shell syntax, staged-diff hygiene, deterministic SDK
availability gate, `npm ci`, and `npm audit` (zero known vulnerabilities). The
Java probe could not be compiled locally because the sandbox cannot resolve
Maven Central and the SDK artifacts are not cached; compilation therefore
remains unvalidated. This environmental limitation is independent from the
normative NO-GO established by inspection of the pinned official SDK source.

The repository script `scripts/check-modern-sdk-support.sh` fails closed unless
the inspected Java SDK source contains the required revision and
`server/discover`. It is an availability gate, not a substitute for the
official conformance suite.

## Legacy probe scope

The disposable Java kernel demonstrates only:

- Streamable HTTP interoperability on `2025-11-25`;
- a single closed-schema mock tool (`urban.object.related_search`);
- rejection of undeclared query-language input;
- twelve concurrent client calls;
- prompt observation of client-side cancellation;
- DNS-rebinding protection through the SDK transport.

It does **not** prove `2026-07-28`, `server/discover`, stateless request-local
metadata, server-side cancellation propagation, Gateway mediation, real
authorization, budget enforcement, audit persistence, or any Executive Freeze
criterion.

## Exit criteria

Product work may start only after one of these governed decisions:

1. **Preferred:** an official Java SDK release implements `2026-07-28`; pin it,
   run the frozen official `2026-07-28` requirement set, and retain raw output
   and hashes.
2. Formally amend/version the MCP PET to permit `2025-11-25` temporarily as the
   default product profile, with an explicit migration gate.
3. Explicitly authorize and threat-model a custom protocol adapter. This is the
   highest-risk choice and is not implied by this feasibility work.

Until then `MCP-A38` remains **OPEN**, and dependent product claims remain
blocked. Gateway 1A may proceed independently at the contract boundary.

## Cross-module regression retained

The current Onboarding repository contains the concrete Gateway object-storage
adapter test `GatewayManagedFileObjectStoreTest` and runtime Gateway projections.
This is the preserved micro-pairwise boundary for Gateway 1A: the Gateway must
continue accepting the opaque `object://` reference flow and the exact internal
read route expected by Onboarding. It is a mock-boundary regression, not a
deployed pairwise/E2E claim.

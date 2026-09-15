# MCP 1A traceability

Normative baseline: MCP Server PET v1.2 from OUF Reality Baseline Package v1.6.

This increment establishes the Go protocol kernel and does not claim the whole MCP acceptance suite.

| PET criterion | Evidence in MCP 1A | State after CI |
|---|---|---|
| MCP-A01 No SQL surface | Closed JSON Schema and manifest forbidden-surface lint | Candidate VERIFIED |
| MCP-A02 Typed validation | JSON Schema 2020-12 with `additionalProperties: false`; SDK validation | Candidate VERIFIED |
| MCP-A23 Capability manifest CI | Manifest load and semantic validation block startup and CI | Candidate VERIFIED |
| MCP-A30 Capability classification CI | TRUSTED_HUMAN_ONLY plus toolEligible is rejected | Candidate VERIFIED |
| MCP-A38 Protocol conformance | Official SDK client verifies `server/discover`, 2026-07-28 and stateless operation; official conformance runner remains required | PARTIAL |

The sole tool descriptor is intentionally non-operational until Gateway mediation and authorization are implemented. Calls fail closed with `MCP_1A_BACKEND_NOT_BOUND`; the kernel contains no direct backend route.

The Gateway-Onboarding object-reference micro pairwise fixture remains a regression dependency for the later MCP-to-Gateway binding increment. It is not duplicated or reinterpreted here.

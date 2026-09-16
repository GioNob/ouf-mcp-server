# MCP ↔ Gateway deployment/security pairwise

Normative baselines: MCP Server PET v1.2 and Urban API Gateway PET v1.3. Roadmap gate: micro-convergence before Gateway 1H and MCP 1G.

Pinned Gateway evidence: `ouf-api-gateway@18ba4d317fd451954e13ffca4e88c6541e2062fc` (Gateway 1G-D merged main).

## Contract verified in CI

- MCP ingress is modeled as Gateway northbound only.
- MCP southbound calls target Gateway southbound, never UDP directly.
- MCP runtime egress contract includes Gateway southbound, PostgreSQL, DNS and telemetry only; generic Internet and direct UDP are forbidden.
- MCP maintenance-worker contract excludes Gateway/UDP data-plane egress and permits PostgreSQL + telemetry only.
- Gateway southbound is default-deny for both ingress and egress.
- Gateway southbound ingress is restricted to authorized workload namespaces and port 9443.
- Ingestion baseline reaches APISIX southbound only and does not contain direct UDP/vertical access.

## Evidence classification

The pairwise test validates repository contracts/manifests and cross-module selector/port compatibility. It does **not** claim packet-level CNI enforcement, adopted-cluster DNS/telemetry exceptions, IAM identities or runtime traffic reachability. Those remain deployment evidence for Gateway 1H / MCP 1H.

A failure of this pairwise is a roadmap gate: neither side should advance its deployment manifests on an incompatible topology.

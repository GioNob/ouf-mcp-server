# MCP 1H — deployment, NetworkPolicy and workload identity contract

Normative baseline: MCP Server PET v1.2 deployment/security requirements.

Implemented CI evidence:
- mcp-server Deployment declares 2 replicas, topology spread, non-root/seccomp and bounded resources;
- PDB declares minAvailable=1;
- maintenance worker is a distinct 1-replica workload with a distinct service account and DB Secret ref;
- default-deny NetworkPolicy is explicit;
- server ingress is restricted to Gateway northbound;
- server egress is limited to Gateway southbound, PostgreSQL, DNS and telemetry;
- maintenance egress is limited to PostgreSQL and telemetry;
- no direct UDP object-resolution destination is present.

Evidence boundaries:
- Kubernetes scheduler, CNI packet enforcement, ServiceAccount-to-IAM federation, real DB role membership, secret rotation and deployed network reachability remain EVIDENCE PENDING;
- the manifests are deployment contracts, not deployed-environment evidence.

No PET deviation is introduced.

# Governed semantic consultation

Candidate base: live MCP `04e871601e393672a1d62759dfdee09e2e66dc6d`.
All pre-existing manifest entries, upload picker/status and object search are preserved.

| Tool | Owner capability | Execute suffix | Result |
| --- | --- | --- | --- |
| semantic.search | ouf.semantic.search | /semantic/search | Current ACTIVE published references, labels and definitions; bounded filters. |
| semantic.get | ouf.semantic.read | /semantic/get | Exact semanticId/revisionId/publicationSetId; historical reads, no latest fallback. |

MCP uses the existing parameterized Gateway endpoint and renewable workload identity.
No tenant, domain, issuer, upstream, credential or receipt key is added to MCP source.
Gateway authenticates workload and signed HUMAN delegation; Semantic independently verifies
a distinct `semantic-read-owner` receipt/MAC domain and the current local policy snapshot.
Receipt signing keys are held by Gateway/owner only. No authoritative approve/publish tool.

Deploy requires the compatible Semantic consultation adapter, two additive Gateway routes,
registered canonical owner capabilities/scopes and reviewed grants. Manifest publication alone
is not evidence of deployed route or owner authorization. Never replace the live manifest with
an older managed-file or object-search branch. File/API scheduling behavior remains in the
Onboarding/Runtime bundle, not in this read adapter.

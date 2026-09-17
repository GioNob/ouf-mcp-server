# R1b MCP policy runtime

PET Authorization 1.5 §§109.6, 7.2, 36.10, 34.2. Common Java/Go vectors are pinned to owner e74cb172e4c6a6b715acf730224b9aa06d0d8e96. The cache detaches transport-owned data, verifies immutable content and installs snapshots atomically. A stale or racing older activation cannot replace a newer version. Rollback requires a new monotonic version, matching Java.

`MCP_AUTHORIZATION_MAX_STALENESS` defaults to `5m`, allowed `1s` through `24h`; `MCP_AUTHORIZATION_BUNDLE_REFRESH` defaults to `30s` and must be shorter. Invalid refresh cannot extend freshness. At the threshold decisions fail closed. Live bundle HTTP transport requires SHA-256 contentHash and rejects unsupported fields, trailing content and oversized envelopes.

Role/resource/DAL/assurance/detail constraints and DENY precedence share the SDK fixtures. Claims are trusted-only and excluded from client JSON identity binding. Missing constrained claims deny without remote lookup. Resource metadata and operational detail projection must be supplied/enforced by the owning module; production IdP and browser THS acceptance remain R4.

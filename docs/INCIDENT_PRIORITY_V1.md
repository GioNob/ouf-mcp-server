# Incident priority v1 — resilience policy v3

[Normative platform policy](https://github.com/GioNob/ouf-deploy/blob/main/docs/OUF_RESILIENCE_POLICY.md), MCP PET1.4 §33.

When limit=1 and Gateway returns an INFO incident while Ingestion returns a CRITICAL incident, the previous alphabetical module ordering could discard the critical incident. The aggregate now ranks before limiting: unresolved before resolved, CRITICAL/ERROR/WARNING/INFO, actionRequired, newest lastSeenAt (parsed RFC3339 instant), module and incident ID. Snake/camel field projections are accepted. Missing severity remains unknown; no severity or impact is invented from occurrence counts.

Response adds `truncated` for observed aggregate truncation, `priorityProfile=ouf.incident-priority.v1` and `selectionScope=RETURNED_OWNER_PROJECTIONS`. This is not proof of complete/global priority: each producer has already bounded its response. No continuation cursor or operator checkpoint is implemented by this tranche. Producer truncation/cursor, selection priority, dependency correlation, authorized impact ranking and complete platform coverage remain open. `partial`/unavailableProducers still describes unavailable producer evidence, independently from truncation.

Tests exercise critical-vs-module limit, active-vs-resolved, action priority, timestamp offsets, stable ties under concurrent producer completion and unavailable producer semantics. No incident mutation, cross-owner merge, new grant, notification or target deploy occurs.

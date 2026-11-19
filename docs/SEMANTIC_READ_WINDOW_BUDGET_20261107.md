# Semantic read window budget

Owner direction, 7 November 2026: raise a limiting orchestration budget as required for source-column semantic analysis.

The retained MCP source d2454f3c uses a one-minute database-clock window. The default maximum tool calls are per-call maximum (1) multiplied by equivalent-query threshold (3); different non-control capabilities share identity/tenant/manifest window counters. The refusal string does not distinguish calls, bytes or object debt. The previous live refusal does not prove which dimension was reached.

This change assigns only ouf.semantic.search and ouf.semantic.consultation.read READ operations the server-owned profile ouf.semantic-read-budget.v1: 60 calls, 15 MiB result bytes, 6000 distinct-object units per minute for the existing service-principal/principal/tenant/manifest identity. Per-call envelopes remain at most 262144 bytes/100 objects; the kernel continues to use 1 object for consultation. The 60-call choice covers 20 searches plus 20 exact reads with room for alternatives. The limit is finite and can be revised deliberately if measured usage requires it.

Operational-control windows, commands, mutation permits and equivalent-query thresholds remain unchanged. No tool argument controls the profile. The profile is an explicit new capacity policy; old windows, attempts, reservations and recovery history remain stored. No SQL reset or legacy replay is part of deployment.

A semantic-window refusal before dispatch returns SEMANTIC_READ_WINDOW_BUDGET_EXHAUSTED, the limiting dimension, windowEndsAt and retryAfterSeconds from the database window. retryable=true describes that temporary admission refusal; it never causes an automatic retry, bypasses a new authorization check or converts ambiguous effects/permanent errors into retryable ones.

Tests cover 60 distinct reads across search/get, refusal of the 61st after opening another database connection, retained consumption/history, independent command/control budgets, unchanged equivalent-query/idempotency behavior, forged command profiles, authorization before admission and bounded MCP error metadata.

This is a candidate based on the retained application source. Native CI and current-image identification must be checked before rollout. No runtime limit has changed merely by committing this code. It does not expose the separate external-discovery candidate or enable its provider.

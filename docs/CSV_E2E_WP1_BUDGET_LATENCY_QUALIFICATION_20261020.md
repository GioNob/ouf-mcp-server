# CSV end-to-end WP1: budget latency qualification

MCP PET1.4 §56 defines budget-consume p95<25ms in the LAN DB profile.
Run39232083000 passed the ordinary PostgreSQL tests, then failed this latency
assertion during the subsequent race-instrumented run (p95=72.990076ms).
The same source head's push run39232077077 passed. The failed measurement therefore
mixed the LAN latency profile with race instrumentation and concurrent package work.

The latency test is now an explicit performance && !race profile. scripts/verify.sh
must run it uncached, as a single package (-p=1), before the ordinary and race suites.
The limit remains25ms, with80real Reserve calls and real PostgreSQL transactions;
there is no timing retry, threshold relaxation, synthetic latency or changed Store.
The measured p95 is logged on both success and failure. The ordinary functional and
concurrency suites still run under -race; performance measurements are uninstrumented.

This is qualification of the stated local LAN DB fixture, not a target-network SLO
or a claim about throughput/latency under production load. A missing database still
requires explicit qualification rather than interpreting a skipped test as PASS.

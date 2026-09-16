# MCP 1I — Backup / restore / DR runbook

Normative baseline: Reality Baseline v1.7, MCP Server PET v1.4 §§99–107 and deployment acceptance DEP-A02/DEP-A07.

## Scope

The authoritative durable state is the `ouf_mcp` PostgreSQL schema. Manifest and tool-schema artifacts are release artifacts and are reconstructible. Session/budget state may be lost only inside the approved environment RPO. A restore must never turn stale or expired trusted-human state into a valid authorization or confirmation path.

## Backup

- Include the full `ouf_mcp` schema in the governed OUF PostgreSQL backup policy.
- Use PITR where the platform PostgreSQL service provides it.
- Preserve migration version, manifest snapshots, audit/incident/evidence relations and retention-role definitions consistently with the database backup.
- Treat repository/release manifest artifacts as a separate reconstructible source; do not use them as a substitute for durable ledger state.

## Restore sequence

1. Isolate MCP server and maintenance workloads from backend execution while restore is in progress.
2. Restore PostgreSQL to the selected RPO/PITR point using the platform DBA procedure.
3. Run the governed migration compatibility check; never auto-migrate from application Pods.
4. Verify schema readiness and manifest checksum/version compatibility before enabling `/health/ready`.
5. Keep backend execution fail-closed until Authorization/IAM and Gateway dependencies are healthy.
6. Reconcile ADMITTED/RUNNING/UNKNOWN/UNRESOLVED attempts through the normal recovery path; never blind-retry uncertain attempts.
7. Recompute non-authoritative debt caches from ACTIVE `budget_object_debt`; no restore or TTL may forgive authoritative debt.
8. Validate that expired approvals/challenges remain expired. MCP approval links are correlation-only; backend owner challenge validity remains authoritative.
9. Re-enable MCP traffic gradually and monitor error rate, DB pool, p95/p99 and recovery backlog.

## Restore acceptance invariants

- Budget correctness: zero known bypass.
- Idempotency claims do not split across attempts.
- UNKNOWN/UNRESOLVED attempts remain retry-blocking until governed evidence resolves them.
- ACTIVE object debt remains authoritative after restore; caches may be repaired but debt is never silently removed.
- Expired approvals do not become valid because of restored MCP state.
- Readiness is false while PostgreSQL/schema/dependency prerequisites required for safe admission are unavailable.
- No raw payload, access token or unnecessary personal data is introduced into logs or metrics during the drill.

## Evidence capture

Record backup identifier, restore point, start/end timestamps, observed RPO, observed RTO, schema/manifest versions, test identities, recovery counts, dashboard snapshots, alerts, and all failed acceptance checks. The restore drill evidence belongs in the release evidence package.

## Evidence status

Repository/CI validates the runbook, recovery invariants, concurrency safety, readiness behavior and observability contracts. A real platform backup/PITR restore with measured RPO/RTO requires the deployed PostgreSQL/Kubernetes environment and therefore remains **EVIDENCE PENDING** until that drill is executed there.

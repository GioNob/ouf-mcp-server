# Explicit semantic reprocessing after a known authorization denial

Status: development branch only. Not deployed, not semantic approval, no functional milestone proved. Base: codex/vps-file-cohort-mcp-lineage-20261014 at f249b3dcc58c2a96742092d90fa09fd90b705bd0. Target source binding still requires readback. Do not merge this change into an unrelated main lineage.

A known, reconciled semantic authorization denial currently retains a completed MCP claim. After policy recovery, the original command replays the claim without a new dispatch; owner receipt lookup can therefore still return not found. This change allows an explicitly signed, finite session to reserve at most one additional call for that exact original command. The owner receives the unchanged command key and request body. The old attempt, claim, reservation, window and guard are preserved.

## Deployment and trust contract

The four optional settings must be configured together: MCP_REPROCESSING_KEY_PROFILE_FILE, MCP_REPROCESSING_KEY_PROFILE_SHA256, MCP_REPROCESSING_PERMIT_FILE, MCP_REPROCESSING_EXISTING_POLICY_FILE. They name root-controlled, non-writable, regular, single-link metadata files under root-controlled ancestry. No private signing key is loaded by MCP. The profile hash is independently pinned in deployment configuration and pins the existing trust policy, installer identity and a persistent per-tenant session capacity. Metadata and current signatures are reread before reservation and before dispatch.

Signatures reuse the installation's existing ouf.semantic-deployment-trust-policy.v1 and ouf.semantic-deployment-detached-signature.v1 formats. DEPLOYMENT_INTENT and FINAL_DEPLOYMENT_APPROVAL are separate role signatures made with the same existing authorized installer key, as required by that installation's profile. CREATION_ATTESTATION is not used. The byte frame matches the existing Python signing producer; no new signer, key, trust root or attestor authority is introduced.

The signed payload fixes the HUMAN subject, tenant, workload, current manifest, capability, original command key and request hash, predecessor UUID and lock version, exact known denial, recent owner receipt absence, decision reference and reason, issue/expiry times and cumulative count 1+1=2. Maximum lifetime is 900 seconds. Normal current authorization and delegation remain mandatory. An expired session cannot dispatch and does not disable unrelated capabilities. A permit alone never initiates a tool call.

## Ledger behavior

Migration 013 adds an immutable session linking the retained predecessor to one new attempt. Serializability, the original claim lock and unique predecessor prevent competing sessions from gaining a second new dispatch. A distinct internal admission key is never sent to the owner. UNKNOWN outcomes, blocked retry guards, active debt, body drift, revoked authorization, capacity exhaustion and short remaining lifetime block reprocessing. The original budget is not reset. Native retention preserves the linked history; independently eligible graphs remain purgeable. Capacity exhaustion requires a separately reviewed lifecycle decision, not silent deletion.

A transport error after dispatch leaves RUNNING/DISPATCHED and the reservation intact until existing lease maintenance marks UNKNOWN. The one-use session remains consumed. Owner receipt reconciliation is required; this path does not manufacture a known failure or authorize another call. A guard revoked after reservation leaves a consumed, undispatched admission for existing maintenance to close.

## Verification and deployment limits

GitHub Actions uses PostgreSQL 17 for actual migrations, admission, concurrent callers, audit, capacity, retention, immutable history and lease recovery; it compiles the MCP command, runs the relevant tests with race detection and checks root file custody. Gateway and authorization ports in these tests are synthetic; they do not prove actual Registry transport, installed policy, live owner effects or end-to-end readiness. Test signing keys are generated solely inside fixtures.

Before deployment: bind the exact installed source/image and failed claim, qualify actual migration-role permissions (including replacement of the existing retention function), build and pin the package, prepare a bounded one-component backup/migration/cutover and rollback review, and use the existing authenticated HUMAN/infrastructure review and signing process. Do not grant broad SQL roles, restore old application snapshots, rearm/delete an old claim, reset budgets or create another semantic command key. No deployment, live signature, new authority, owner draft, THS approval or release acceptance is performed by this branch.

## Atomic administrative migration of the retained function

The explicit command `migrate-semantic-reprocessing` performs only the packaged 12-to-13 transition. The separately authenticated infrastructure owner must select the existing administrative channel and provide the approved application role, exact database OID and existing retention function hash using MCP_MIGRATION_APPLICATION_ROLE, MCP_MIGRATION_DATABASE_OID and MCP_MIGRATION_RETENTION_FUNCTION_SHA256. It is not used by server startup and adds no public capability.

The native command verifies all twelve existing checksums, role attributes, zero retention memberships and exact function ownership/hash. Under one serializable transaction and the existing migration advisory lock, it grants transaction-local membership, selects the application role, applies the packaged migration and its checksum, resets the role and revokes membership before committing. No other connection can observe this temporary membership; rollback/cancellation includes the grant. Existing administrative credentials must never be exposed or broadened into application configuration. Infrastructure review must bind this distinct administrative invocation and stop MCP during migration. A failed COMMIT response still requires migration-history readback; the command is never automatically repeated.

GitHub Actions run 38937982021 qualified successful cleanup, wrong database/function denial, repeat denial and cancellation while actual PostgreSQL DDL waits on a lock. The cancellation test verifies the other connection still sees zero memberships before and after cancellation. This replaces the earlier proposal for a separately committed GRANT/REVOKE sequence. Previous unsigned image and private root review remain immutable predecessor evidence and cannot authorize the changed package.

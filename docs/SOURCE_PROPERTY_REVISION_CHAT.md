# Generic property revision tools

Candidate tools source.onboarding.revision.read and source.onboarding.revision.prepare use independent Onboarding capabilities and exact source/version resource authorization. They support existing sources across formats and acquisition modes without source-specific rules.

Read obtains the owner's bounded property-rule/version/hash/lock projection. Prepare carries an explicit parent CAS, stable commandKey, publication effect acknowledgment and only selected reconciliation rules. It returns a native HUMAN review; the chat cannot confirm, publish or activate. Activation may trigger acquisition under the retained scheduler configuration.

Keep the commandKey after interruption. If dispatch/admission returns SOURCE_REVIEW_READ_REQUIRED or an outcome is ambiguous, call source.onboarding.revision.read with the original sourceId,parentVersionId,commandKey to retrieve the owner's retained result, even when the new version ID was lost. Never generate a new key, duplicate source or repeat identity decisions to recover a missing response. No budget, challenge TTL or authority is renewed.

The existing orchestration admission, durable key/fingerprint and transport limits remain in force. Gateway requests use exact dedicated revision paths, workload bearer plus signed HUMAN delegation. Owner signing keys and browser tokens stay outside MCP. The real official MCP SDK test exercises source-scoped permission denial, argument rejection, stable command identity and both revision routes, checking requests against pinned Gateway schemas.

Not deployed: route materialization, capability registration/grants, native owner/MCP rollout and concrete profile approval/publication remain explicit integration steps. Runtime acceptance remains technical8/8, functional4/6; neither second acquisition nor stage6 is claimed by these tests.

# Source revision tools

Candidate tools source.onboarding.revision.read and source.onboarding.revision.prepare use independent Onboarding capabilities and exact source/version resource authorization. They support existing sources across formats and acquisition modes without source-specific rules.

Read the active parent for its version, configuration hash and lock. Prepare carries the parent preconditions, a stable `commandKey`, the publication effect acknowledgement and exactly one of the following inputs.

- `changes` selects the property reconciliation rules to revise.
- `assetRef` replaces the file of the same active source. Supply the `assetId` of the already uploaded managed CSV and its immutable `profileId`. For a new source use `source.onboarding.create`.

For file replacement MCP sends the parent lock as `expectedVersion` and as the weak `If-Match` ETag. The Gateway signs the owner receipt. Onboarding verifies custody, profile and CSV compatibility. A successful receipt includes the candidate, challenge, `assetRef` and empty `diagnostics`. HTTP 422 with `ONB_REVISION_CSV_INCOMPATIBLE` returns every reported difference with its code, column, expected and actual metadata. The parent stays active while the candidate awaits HUMAN review. Subsequent HUMAN activation can start acquisition.

The candidate manifest version is `1.11.0-source-revision-new-asset`. Its canonical `manifestChecksum` is `a1de6f4d84c7e9e71471807198742ebfbf916e6dc7c34c2fc2553f7e36932a4b`.

Keep the commandKey after interruption. If dispatch/admission returns SOURCE_REVIEW_READ_REQUIRED or an outcome is ambiguous, call source.onboarding.revision.read with the original sourceId,parentVersionId,commandKey to retrieve the owner's retained result, even when the new version ID was lost. Never generate a new key, duplicate source or repeat identity decisions to recover a missing response. No budget, challenge TTL or authority is renewed.

HTTP 503 also requires receipt lookup. If the owner reports `commandState=ABSENT`, any authorized exact retry uses the original arguments, key and parent ETag.

The existing orchestration admission, durable key/fingerprint and transport limits remain in force. Gateway requests use exact dedicated revision paths, workload bearer plus signed HUMAN delegation. Owner signing keys and browser tokens stay outside MCP. The real official MCP SDK test exercises source-scoped permission denial, argument rejection, stable command identity and both revision routes, checking requests against pinned Gateway schemas.

Not deployed: route materialization, capability registration/grants, native owner/MCP rollout and concrete profile approval/publication remain explicit integration steps. Runtime acceptance remains technical8/8, functional4/6; neither second acquisition nor stage6 is claimed by these tests.

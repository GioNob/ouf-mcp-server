# Source revision tools

Candidate tools source.onboarding.revision.read and source.onboarding.revision.prepare use independent Onboarding capabilities and exact source/version resource authorization. They support existing sources across formats and acquisition modes without source-specific rules.

Read the active parent for its version, configuration hash and lock. Prepare carries the parent preconditions, a stable `commandKey`, the publication effect acknowledgement and exactly one of the following inputs.

- `changes` selects the property reconciliation rules to revise.
- `assetRef` replaces the file of the same active source. Supply the `assetId` of the already uploaded managed CSV and its immutable `profileId`. For a new source use `source.onboarding.create`.

With `assetRef`, ask the user for an explicit decision for each column added to the CSV. Supply the exact column names in the sibling array `newColumns`. An `EXCLUDE` entry contains only `column` and `decision`. A `MAP` entry also requires `mapping.propertyIri`, `transform` and the same `reconciliation` rules as a property change. The mapping can specify `accessLabel` and an ordered `authorityOrder`. When the parent uses `governedIdentity`, include `comparison.comparator` and `comparison.assertionRef` in the mapping. The array accepts at most 1,024 entries and the signed owner body is limited to 65,536 bytes.

For file replacement MCP sends the parent lock as `expectedVersion` and as the weak `If-Match` ETag. The Gateway signs the owner receipt. Onboarding verifies custody, the profiles and CSV compatibility. HTTP 422 with `ONB_REVISION_CSV_INCOMPATIBLE` returns each difference with its code, column, expected and actual metadata. For `NEW_COLUMN_DECISION_REQUIRED`, present every named column to the user for a decision. For `COLUMN_NOT_NEW`, show the column the owner rejected.

Successful prepare and read receipts include the owner's recorded `newColumns` in order, with the full mapping for each mapped column. Historical file replacements return an empty array. The parent stays active while the candidate awaits HUMAN review. Subsequent HUMAN activation can start acquisition.

The candidate manifest version is `1.12.0-source-revision-new-columns`. Its canonical `manifestChecksum` is `7467de3d308bcd0210f80c1dfe5e626b619f278f530c97507429c46385ed13a0`.

Keep the commandKey after interruption. If dispatch/admission returns SOURCE_REVIEW_READ_REQUIRED or an outcome is ambiguous, call source.onboarding.revision.read with the original sourceId,parentVersionId,commandKey to retrieve the owner's retained result, even when the new version ID was lost. Never generate a new key, duplicate source or repeat identity decisions to recover a missing response. No budget, challenge TTL or authority is renewed.

HTTP 503 also requires receipt lookup. If the owner reports `commandState=ABSENT`, any authorized exact retry uses the original arguments, key and parent ETag. Preserve the decision order, mapping fields and the distinction between an omitted `newColumns` and an explicit `[]`. Each contributes to command identity.

The existing orchestration admission, durable key/fingerprint and transport limits remain in force. Gateway requests use exact dedicated revision paths, workload bearer plus signed HUMAN delegation. Owner signing keys and browser tokens stay outside MCP. The real official MCP SDK test exercises source-scoped permission denial, argument rejection, stable command identity and both revision routes, checking requests against pinned Gateway schemas.

Not deployed: route materialization, capability registration/grants, native owner/MCP rollout and concrete profile approval/publication remain explicit integration steps. Runtime acceptance remains technical8/8, functional4/6; neither second acquisition nor stage6 is claimed by these tests.

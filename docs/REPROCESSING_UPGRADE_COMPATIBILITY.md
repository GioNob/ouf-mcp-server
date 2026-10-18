# Retained expired reprocessing metadata during upgrades

An MCP catalog change changes the manifest checksum. Previously, an existing expired deployment permit pinned to the previous checksum failed bootstrap before the server could expose unrelated capabilities. Updating permission argument schemas triggered this compatibility risk even when mounts and environment were preserved.

Bootstrap now verifies a fully expired historical permit against its own signed manifest only after strict current-manifest verification fails. All original role signatures, profile/policy hashes, scope, finite limits and issuance windows must still pass. The result remains ErrExpired with the original scope retained. Active, future-dated, malformed or unsigned permits cannot cross manifests.

Per-operation verification stays strict against the current manifest. There is no renewal, dispatch, budget reset, new grant, identity decision, file rewrite or database migration. Historical metadata remains mounted unchanged. This is generic deployment compatibility, independent of any test source.

Qualification must distinguish unit tests from startup with real root-owned deployment files and target rollout. The previous packaged MCP source 5124178d5856b564691ca4274817efd45a0bb991 must not be deployed with retained old-manifest metadata without resolving this compatibility gate.

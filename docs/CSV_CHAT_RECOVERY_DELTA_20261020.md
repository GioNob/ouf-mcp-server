# CSV durable chat recovery delta

MCP PET1.4 sections33.4–33.5 require durable reconnect and server-side scoped visibility. Existing source.file.preview gains an explicit bounded ASSETS view (limit1–50, UUID cursor); source.onboarding.read gains explicit LATEST. Existing exact-reference forms remain valid and mixed forms are rejected by closed schemas. No new capability IDs or Gateway routes are introduced.

LATEST authorizes the source resource at MCP, without inventing a version attribute. Onboarding selects its durable latest version and rechecks current exact-version policy and tenant/subject ownership. A grant restricted to an exact version must not become a generic discovery grant. ASSETS results are owner-filtered before pagination; no storage reference or caller-supplied tenant is accepted. Pair with Onboarding701ae74b41cc81e4301ee592e3bcb3d13ead5106 (PR49,280 tests PASS).

Validation pending: official SDK latest transport/denied scope cases and explicit authorization edge tests are committed. Existing exact-version pairwise schema assertions remain in their original cases; new LATEST shape has separate argument assertions. Composed owner/client and G6 acceptance remain open. No live tool publication, login, target operation or runtime authority.

# Preserve the deployed chatbot file path when adding semantic proposals

The proposal candidate `d43f5604976b07c66d58c5180d871c5656c44d4d`
was built from main. Its embedded manifest and executable omitted the deployed
file upload/profile/preview, onboarding DRAFT, published-semantic consultation,
and UrbanObject search capabilities. Installing that executable as a replacement
would therefore remove existing steps of the CSV workflow.

This integration combines main `760e685d0259a2319d72ad268b38fff1bd061bee`
with deployed delivery `394b4b1540b575564f0ba58541df9d7efc59fb87`.
It retains every current main capability descriptor, the current Go/security
versions, durable operational recovery, source-scoped ingestion authorization,
and the current single-artifact semantic proposal contract. It restores the
existing governed upload/picker path, file profile/preview, onboarding DRAFT,
published semantic search/get and object search implementations and tests.

Semantic proposal authorization applies only to its four declared capability
IDs. Published-semantic consultation continues to use its own READ contract;
a shared `semantic.` name prefix does not confer proposal authority.

The tests use the official MCP SDK, current authorization cache and admission
interfaces, and HTTPS Gateway adapter. They cover both semantic proposal and
consultation dispatch, managed-file DRAFT creation, picker/host-file boundaries,
exact discovery, denial before dispatch, and the preserved operational code.
External owners and identities in these tests remain fixtures. CI PostgreSQL
and pairwise qualification are additional source evidence, not a VPS rollout.

The bounded source review tools call the owner to prepare a version for human
review and read its current state. Preparation preserves its command key, uses
the exact source/version authorization scope and a ten-second request budget;
read reconciliation uses three seconds. Changing a command key does not reset
the equivalent-intent budget. Neither tool confirms approval or activates a
source. The owner supplies the review link; the MCP does not invent one.

The existing semantic proposal tool also accepts a bounded Turtle ONTOLOGY
DRAFT. Native and RDF definitions are mutually exclusive. The media type must
be text/turtle, the UTF-8 content is limited to 48KiB, and no source URL is
fetched. This reuses the owner import/validation/THS pipeline and its existing
proposal authority.

Receipt reconciliation uses the dedicated READ capability and OAuth scope
`ouf.semantic.proposal.read`. The existing `ouf.semantic.read` catalogue entry
belongs to SERVICE reads and is not widened to authorize HUMAN callers. The
owner still restricts the receipt to its original caller and requires the
permission for the original proposal operation. Rollout must register/publish
the exact READ descriptor and explicitly authorize its HUMAN caller, alongside
the two source review permissions; existing consultation grants remain intact.

These changes do not publish grants, install a new executable or demonstrate a
production CSV run. HUMAN approval/activation, compatibility attestation and
owner ingestion/UDP results remain runtime responsibilities. A successful
package scan alone did not establish that the candidate preserved the deployed
functional surface.

# R4a — managed-file MCP attachment contract (candidate)

> **Live boundary finding, 26 September 2026:** The enabled MCP candidate
> returned `ATTACHMENT_DNS_UNAVAILABLE` when ChatGPT supplied a real CSV.
> No asset was created. Its `internal/adapter/hostfiles` direct external
> download contradicts MCP PET v1.4 section 38 / MCP-GW-03, even if the
> Docker DNS failure is repaired. Gateway PET v1.5 T11.3 also forbids
> forwarding the temporary, caller-supplied URL to a generic external
> fetcher. Keep this implementation out of new rollouts until a governed
> Agent Host byte bridge or registered Gateway provider binding replaces
> direct MCP fetch; see Gateway
> `docs/R4A_CHATGPT_ATTACHMENT_BOUNDARY.md`. Existing upload route
> installation and passing unit tests do not close this release gate.

The follow-up `--mode probe` candidate adds a read-only ChatGPT widget to
`source.file.attachment_origin_probe`. It uses the host's
`getFileDownloadUrl({fileId})` API and attempts a bounded browser read. The
widget displays only success/failure and byte count; neither bytes nor the
temporary URL enter MCP tool results or OUF. Its CSP permits the observed
`*.oaiusercontent.com` provider family for this feasibility check. A passing
browser read is not an upload: it only establishes that a later browser-to-
Gateway streaming bridge is technically possible in this ChatGPT surface.
If the provider origin or browser CORS policy prevents the read, the widget
fails without changing OUF. The explicit probe rollout can replace the failed
enabled image; `--mode enabled` stays PET-blocked before any mutation.

**Live widget finding, 26 September 2026:** The user deployed the probe from
`14fccee4aea719ad071b969193d4d9df1c14540d` and saw `BROWSER_FETCH`.
That error combines rejection by `getFileDownloadUrl`, cross-origin/CSP/redirect
failure of `fetch`, and response-body read failure; it does not identify one
cause. The subsequent `cad8d31c0439b6063a7b4e294858f654176ff6ae`
revision separates those stages but has not been deployed or exercised.
ChatGPT documents a temporary download URL and illustrates use as an image
source; that does not promise JavaScript access to response bytes. The browser
byte bridge is therefore **unproven and not deployable for upload**. Do not
repeat host-origin allowlist probes or enable direct MCP fetch to work around
this finding. The current Gateway has no browser-to-Gateway upload ticket
binding, so even a future successful browser read alone cannot complete R4a.
The concrete fallback contract is in Gateway
`docs/R4A_CHATGPT_ATTACHMENT_BOUNDARY.md`: a widget file picker obtains a
browser `File` and sends it through the existing governed upload capability
and intake binding after a HUMAN-bound authorization adapter is implemented.
It requires the human to select the local file a second time if it was already
attached to chat. The picker is not a second product capability or intake
endpoint. The first-party OUF browser adapter was later deployed and passed
one live CSV upload on 27 September 2026; the earlier probe finding still
applies to the ChatGPT-hosted attachment byte bridge.

Status: the first-party picker and existing governed HUMAN upload route created
asset `55ce7fd2-1893-4d3c-b95d-9c8106c1200a` in the lab. The automatic
chat handoff remains a candidate pending live widget proof. The separate
host attachment bridge remains PET-blocked. Profile is currently denied in
MCP before Gateway dispatch despite a valid configured HUMAN grant, so the
full profiling and ingestion smoke is still open. PET Gateway T25/T28, MCP v1.4
sections 21 and 38, Source Onboarding v1.6 and Cross-Module Matrix v1.7
govern this contract. Issue: GioNob/ouf-mcp-server#43.

The former direct-fetch image and read-only probe are historical findings;
the lab now runs the first-party picker MCP image
`sha256:d7ea99709684ebaa91619082be0208c05ac7445b429c3af79d25cd5922d66151`
from `208e5dd0259eb54c0d0ee8c516cc87c14a7ed6ca` until the chat-handoff
candidate is installed. Route and container rollback snapshots remain private.

## Ingress and identity

The human chooses a chat attachment. The Agent Host must prove that it can
access the attachment bytes for this exact authenticated user and conversation;
the model cannot synthesize a filesystem path, download URL, or storage ref.
ChatGPT's documented optional `_meta["openai/fileParams"]` may supply a
host-resolved `{file_id, download_url, mime_type?, file_name?}` descriptor.
`download_url` is a short-lived host attachment retrieval credential, not an
OUF object-store URL or an Onboarding staging reference. The descriptor is
accepted only from a host-supported file parameter. The failed direct-fetch
candidate's private MCP adapter
`internal/adapter/hostfiles` resolves the temporary host, pins a public IP for
each connection, verifies its HTTPS certificate, disables proxies and
redirects, and bounds retrieval to 10 MiB. These application controls did not
make direct MCP external fetch PET-conformant.

The first-party picker option sets `MCP_MANAGED_UPLOAD_ENABLED=picker` and an
HTTPS `MCP_MANAGED_FILE_PICKER_URL` ending exactly in
`/trusted-human/managed-files/`. It exposes the **same** `source.file.upload`
tool, with no ChatGPT attachment parameter. The deployed version returns the
picker link and `AWAITING_FILE_SELECTION`; the human currently copies the
Asset ID displayed after upload into chat. Profile, preview and DRAFT
creation use the existing tools. The file bytes traverse the existing Gateway
HUMAN upload route and Onboarding; MCP never downloads the host URL. This mode
requires the separately deployed OUF picker page, its THS login and scope, and
the current streaming upload route. The tool reply alone is not an upload.
The lab rollout is coordinated by `scripts/r4a_attachment_rollout.py --mode picker`:
it verifies the pinned Onboarding image and session-scope overlay, the existing
streaming HUMAN upload route, and snapshots the picker UI route and MCP
container. It restores both if activation fails and prints a private
`PICKER_ROLLBACK_STATE` for a later one-command `--rollback-state` recovery.
Onboarding's pinned image,
scope and database-backup cutover runs first; the two-stage procedure is in
Onboarding `docs/R4A_FIRST_PARTY_PICKER_ROLLOUT.md`. A live HUMAN login and CSV
transfer remain the release proof.

The chat-handoff candidate keeps that same tool and path. It gives the picker
a random handoff ID. Onboarding keeps the staged Asset ID and its HUMAN owner
for at most 30 minutes in a bounded ephemeral map. The ChatGPT widget calls
the app-only, read-only data tool `source.file.upload.status` with `handoffId`;
this tool is an internal result read of the same governed upload capability,
not another product capability. MCP authorizes the existing upload capability,
and the Gateway signs an owner-bound receipt for the exact internal result
route. The widget posts a follow-up message containing only the Asset ID.
The browser session token and CSV never enter MCP. If the widget is closed or
Onboarding restarts, the picker still displays the ID for manual recovery.
`scripts/r4a_picker_chat_handoff_rollout.py` coordinates the lab upgrade and
rollback without a DB migration or new product capability. A real ChatGPT
widget upload is required to validate the return path. ChatGPT cannot keep the
initial model turn open while the human uses an external picker; the widget
remains waiting and posts the result into chat when it arrives.

The first live widget test showed that polling the widget-producing upload
tool remounted a second widget without the original picker context and that
the result read was denied. The repair separates the app-only status tool,
marks widget access explicitly and waits for the host's asynchronous
`toolOutput` before rendering the picker. The local widget simulation passes;
the actual connected ChatGPT result remains unverified until a new upload.
The live host continued to render the old `v1` widget after the MCP container
upgrade (its retired error copy was visible), so the repair publishes a `v2`
resource URI. Reconnect the OUF app before the next test to refresh both
widget resource metadata and the app-only status tool descriptor.
The first live upload through the `v2` widget staged the CSV but the result
read produced `SCOPE_MISSING` in the MCP denial audit. The ChatGPT OAuth
client is `ouf-chatgpt`; prior optional managed-file bindings on
`ouf-human-admin` do not populate that token. Onboarding's
`scripts/r4a_chatgpt_managed_file_scopes.py` reconciles the four HUMAN
managed-file scopes as default bindings on the exact ChatGPT client. A fresh
OAuth connection and a fresh upload are required to test chat handoff after
the expired 30-minute result lease. Policy grants and asset ownership still
govern calls even when the client includes those scope names in new tokens.
The widget now requests its intrinsic height after mount and on content
changes, with a minimum 136-pixel layout so its instruction, button and
status are visible together in the inline card.

## Host portability gate

The current `v2` picker widget is a ChatGPT compatibility prototype: its
JavaScript uses `window.openai.toolOutput`, `callTool`, `openExternal`,
`notifyIntrinsicHeight` and `sendFollowUpMessage`. The resource URI and
`_meta.ui.resourceUri` are MCP Apps standard, but these JavaScript calls are
not a portable MCP Apps implementation. Do not describe the automatic chat
handoff or widget height as verified across MCP hosts based on ChatGPT alone.

The portable widget must use the MCP Apps `ui/initialize` handshake,
`ui/notifications/tool-result`, `tools/call`, `ui/open-link`,
`ui/notifications/size-changed` and `ui/message`, preferably through the
official view SDK. Keep `source.file.upload` as the sole product upload
capability and the status read app-only. Test with at least two independent
MCP Apps hosts before closing the portable UI gate. An MCP client without
MCP Apps can still use the tool's first-party picker URL and the Asset ID
shown on OUF; MCP alone does not specify an embedded widget or automatic
message injection into a conversation.

`source.file.upload` advertises `_meta["openai/fileParams"] = ["file"]` only
when `MCP_MANAGED_UPLOAD_ENABLED=true`. The model must not supply a URL as a
replacement for the host file parameter.
To discover the exact origin without fetching an attachment, deploy the MCP
candidate with `--mode probe` using the coordinated rollout script. In this
mode only `source.file.attachment_origin_probe` advertises the host file
parameter with a read-only annotation and an explicit no-fetch description;
`source.file.upload` is absent. The probe returns only the origin and file ID,
creates no asset and sends no Gateway command. The host still passes the
private descriptor to the MCP tool, but the tool does not fetch the URL.
Confirm the file ID matches the user's selected attachment before enabling
transfer. The host's DNS name is not a deployment setting: it may change
between attachments. After updating to `--mode enabled`, a new
private candidate and rollout are required. The rollout checks
that only the one opt-in environment variable changed; its rollback restores
the prior MCP container. Neither mode proves that the host's file parameter
was injected as claimed until the connected ChatGPT tool is exercised.

`scripts/r4a_attachment_rollout.py` bundles the lab sequence into one
root-run command: it pins both repository trees, makes a private snapshot of
the current MCP container, verifies the three existing JSON route bodies,
builds the MCP image, prepares its environment, installs the CSV route if
absent, swaps MCP and verifies image, readiness and APISIX readback. On an
error after either mutation it invokes the existing container and route
restorers. It reports only status, image ID and private backup paths. Supply
the root-owned materialization directory generated from the active installation
projection and the exact reviewed MCP commit. Use `--mode enabled` directly
for new installations; an optional `--mode probe` can inspect a host file
descriptor without downloading bytes. Switching from an existing probe
container to enabled mode makes a new snapshot automatically. A probe result
alone is insufficient evidence of byte transfer or ingestion.

An enabled image can be replaced by another pinned enabled image through the
same rollback-backed rollout. If retrieval fails before Gateway admission,
the MCP result classifies descriptor, DNS, blocked destination, HTTPS, host
HTTP status, redirect, size, read or staging failures with a fixed code. It
never returns the private URL, query token or response body. A failed
retrieval creates no managed asset.

The adapter downloads the host-issued descriptor into a private bounded spool,
then streams the original bytes through the internal Gateway binding
`POST /internal/capabilities/v1/execute/managed.file/upload`, using the MCP
workload token and a Gateway-signed HUMAN delegation. Only file ID, byte count
and SHA-256 enter the governed admission fingerprint; the URL remains in
request memory. Gateway signs those headers into a separate owner receipt;
Onboarding checks that receipt and independently hashes and counts the stream.
The public HUMAN route remains a separate channel for a direct human client.

Gateway verifies workload, delegated HUMAN scope, size and media type before
passing the stream to Onboarding. Onboarding checks the delegated HUMAN owner
receipt, computes the hash, persists bytes to governed staging and returns an asset ID.
The MCP tool result may expose only that asset ID and safe status. Never put
raw bytes, base64, bearer tokens, secret refs or MinIO URLs in tools/call
arguments, results, logs or model-visible text. The host's file parameter
may include a temporary `download_url` in the tool argument as specified by
ChatGPT; it must not be copied into OUF's Gateway envelope, result or logs.
Before enabling, prove host-controlled fileId/URL provenance and verify
whether the host exposes that argument to the model. If the connected Agent Host
cannot provide this attachment adapter, return a bounded explicit
`ATTACHMENT_BRIDGE_UNAVAILABLE` outcome and keep upload unpublished.

MCP PET `source.file.upload` is a proposal/async-create tool identity; Gateway
T25 `ouf.managed-source.file.upload` is the channel-neutral execution
capability. Map these in a versioned manifest; do not equate an MCP tool name
with a distinct authorization grant. The opt-in remains off until the host
descriptor and APISIX streaming runtime are proven live.

## After upload

| Action | Tool result/input | Authoritative owner | Gate |
| --- | --- | --- | --- |
| Profile | `assetId` input; opaque `jobId` result | Onboarding | HUMAN delegation, `ouf.managed-source.file.profile`, idempotency |
| Preview | `assetId` and `profileId`; redacted bounded result | Onboarding | HUMAN delegation, `ouf.managed-source.preview`, owner redaction |
| Create onboarding | explicit profile, field decisions and pinned semantic refs; bounded DRAFT identity | Onboarding | `ouf.managed-source.onboarding.create`; owner-bound idempotency; no implicit approval |
| Approve/activate | exact diff/hash challenge, direct THS HUMAN call | Onboarding/THS | no MCP commit tool |
| Ingest | `assetId`/approved bundle reference | Ingestion | only compatible ACTIVE PublishedConfigurationBundle |
| Discover | bounded search arguments | UDP | governed Gateway search; no SQL fixture |

Each tool uses the established MCP workload token plus Gateway-minted short
HUMAN delegation proof. Gateway checks workload and proof, exact capability,
tenant, subject, correlation, attempt and idempotency. The owner validates a
signed receipt and makes the fine-grained resource decision; service identity
alone never becomes HUMAN authority. File profile/preview bindings require
new exact internal routes and owner handlers; public HUMAN routes do not by
themselves implement an MCP binding. A separate ingestion command remains
conditional on the ACTIVE bundle and Authorization policy.

## Release evidence

1. Tests show the same capability works via attachment adapter and a second
   authorized channel, with identical scope/owner decisions.
2. Rejected cases: forged handle, arbitrary URL, altered hash, missing human
   token, wrong tenant, expired delegation, wrong asset owner, oversized or
   unsupported file, duplicate command, inactive semantic bundle.
3. Live plugin discovery contains only the reviewed tools; exact CSV bytes
   arrive at Onboarding via Gateway, then eight rows/two columns appear in
   a redacted profile. The Semantic/THS, Ingestion, UDP and search steps are
   separately recorded. A server-side HTTP smoke is not this acceptance.
4. Gateway T25 streaming and cross-network verified transport gates remain
   independent (GioNob/ouf-api-gateway#52). Do not deploy this flow until
   all three gate families pass.

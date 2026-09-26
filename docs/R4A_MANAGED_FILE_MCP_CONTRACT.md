# R4a — managed-file MCP attachment contract (candidate)

Status: candidate profile/preview/draft-create and opt-in upload tool, no live deployment. PET Gateway T25, MCP v1.4 §21,
Source Onboarding v1.6 and Cross-Module Matrix v1.7 govern this contract.
Issue: GioNob/ouf-mcp-server#43. The current *deployed* plugin exposes no file tools.

The live VPS image `ouf-mcp:r4a-8599843` corresponds to commit
`85998435c0fb3ea1aa5264eb0751d2b7c69b1b3c`. The attachment candidate
branched before 41 live commits, so deploying that candidate image directly
would remove existing MCP behavior. A local integration branch based on the
live commit retains `urban.object.search` and adds the four managed-file
capabilities. Its upload remains disabled by default; the integration is
not deployed or verified on the VPS. The live preflight found upload disabled,
host origins unset and all four internal managed-file routes absent.

## Ingress and identity

The human chooses a chat attachment. The Agent Host must prove that it can
access the attachment bytes for this exact authenticated user and conversation;
the model cannot synthesize a filesystem path, download URL, or storage ref.
ChatGPT's documented optional `_meta["openai/fileParams"]` may supply a
host-resolved `{file_id, download_url, mime_type?, file_name?}` descriptor.
`download_url` is a short-lived host attachment retrieval credential, not an
OUF object-store URL or an Onboarding staging reference. The descriptor is
accepted only from a host-supported file parameter. The private MCP adapter
`internal/adapter/hostfiles` resolves the temporary host, pins a public IP for
each connection, verifies its HTTPS certificate, disables proxies and
redirects, and bounds retrieval to 10 MiB;
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

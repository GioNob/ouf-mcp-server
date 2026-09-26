# R4a — managed-file MCP attachment contract (candidate)

Status: candidate profile/preview/draft-create and opt-in upload tool, no live deployment. PET Gateway T25, MCP v1.4 §21,
Source Onboarding v1.6 and Cross-Module Matrix v1.7 govern this contract.
Issue: GioNob/ouf-mcp-server#43. The current *deployed* plugin exposes no file tools.

## Ingress and identity

The human chooses a chat attachment. The Agent Host must prove that it can
access the attachment bytes for this exact authenticated user and conversation;
the model cannot synthesize a filesystem path, download URL, or storage ref.
ChatGPT's documented optional `_meta["openai/fileParams"]` may supply a
host-resolved `{file_id, download_url, mime_type?, file_name?}` descriptor.
`download_url` is a short-lived host attachment retrieval credential, not an
OUF object-store URL or an Onboarding staging reference. The descriptor is
accepted only from a host-supported file parameter; configure exact approved
HTTPS origins, disable redirects and bound retrieval to 10 MiB. The private
MCP adapter `internal/adapter/hostfiles` implements that constrained spool;
`source.file.upload` advertises `_meta["openai/fileParams"] = ["file"]` only
when `MCP_MANAGED_UPLOAD_ENABLED=true` and `MCP_HOST_FILE_ORIGINS` lists exact
approved HTTPS origins. It is not enabled in the deployed plugin. An arbitrary user/tool
URL is never a file source.
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

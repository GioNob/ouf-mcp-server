# R4a — managed-file MCP attachment contract (candidate)

Status: design gate, not a deployed tool. PET Gateway T25, MCP v1.4 §21,
Source Onboarding v1.6 and Cross-Module Matrix v1.7 govern this contract.
Issue: GioNob/ouf-mcp-server#43. The current plugin exposes no file tools.

## Ingress and identity

The human chooses a chat attachment. The Agent Host must prove that it can
access the attachment bytes for this exact authenticated user and conversation;
the model cannot synthesize a filesystem path, download URL, or storage ref.
The attachment adapter transports the original bytes via the public Gateway
`POST /api/managed-sources/v1/files` with the user's HUMAN token, capability
`ouf.managed-source.file.upload`, `text/csv`, bounded size and optional exact
SHA-256. An opaque host-owned attachment handle is allowed only within that
host's trusted adapter. It is never interpreted by OUF as an arbitrary URL.

The Gateway authenticates the human, enforces route scope/size/media type and
passes the stream to Onboarding. Onboarding revalidates the HUMAN capability,
computes the hash, persists bytes to governed staging and returns an asset ID.
The MCP tool result may expose only that asset ID and safe status. Never put
raw bytes, base64, bearer tokens, secret refs or MinIO URLs in tools/call
arguments, results, logs or model-visible text. If the connected Agent Host
cannot provide this attachment adapter, return a bounded explicit
`ATTACHMENT_BRIDGE_UNAVAILABLE` outcome and keep upload unpublished.

MCP PET `source.file.upload` is a proposal/async-create tool identity; Gateway
T25 `ouf.managed-source.file.upload` is the channel-neutral execution
capability. Map these in a versioned manifest; do not equate an MCP tool name
with a distinct authorization grant. No tool becomes eligible merely by
flipping `toolEligible` without the bridge and negative-path proof.

## After upload

| Action | Tool result/input | Authoritative owner | Gate |
| --- | --- | --- | --- |
| Profile | `assetId` input; opaque `jobId` result | Onboarding | HUMAN delegation, `ouf.managed-source.file.profile`, idempotency |
| Preview | `assetId` and `profileId`; redacted bounded result | Onboarding | HUMAN delegation, `ouf.managed-source.preview`, owner redaction |
| Create onboarding | explicit profile, field decisions and semantic refs; DRAFT result | Onboarding | `ouf.managed-source.onboarding.create`; no implicit approval |
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

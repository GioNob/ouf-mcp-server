# OUF application roles through chatbot and THS

Requires Onboarding with V29 application-role catalogue support. Read authorization.permissions.read with view ROLES and CAPABILITIES. CAPABILITIES returns the active HUMAN descriptors; an `ouf-admin` proposal can include each eligible capability, preserving unrelated roles and assignments. Prepare operation REPLACE_ROLES with reason and complete roleCatalogue (issuer, roles, assignments). An assignment selects exactly one nominal subjectId or IAM externalRoleRef. The IAM issuer is pinned by the owner; tenant comes from verified identity. Only the human THS confirmation publishes the proposal.

MCP only prepares the immutable proposal and returns approvalPath. An authenticated human administrator reviews both snapshots and confirms in THS. Scopes and resource constraints remain mandatory. Superadmin is managed separately. Revoking an assignment does not remove independent assignments/direct grants. Maximum 200 generated grants and 48000 bytes per role proposal.

Deploy compatible Onboarding before this manifest; refresh ChatGPT actions after the MCP upgrade. No IAM account management or automatic fallback when organization claims disappear.

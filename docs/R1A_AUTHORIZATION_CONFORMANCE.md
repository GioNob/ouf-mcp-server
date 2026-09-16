# R1a shared evaluator conformance

Authority: Reality Baseline / Matrix 1.7, Authorization PET 1.5 §109.3,
MCP PET 1.4 (Go runtime retained).

`internal/authorization/testdata/authorization-conformance-v1.json` is copied
unchanged from the Java SDK at Onboarding commit
`63f238fa78abe2e6834430af618366ce865977fd`. A dedicated workflow compares the
file to that pinned source and executes all 11 vectors against the actual Go
evaluator. The Java SDK executes the same vectors. Coverage: human/service
allow, tenant/actor/scope/organization/subject/expiry deny, unknown capability,
wrong operation and service-principal mismatch. No second server runtime is added.

This closes the R1a common evaluator fixture gate. It does not demonstrate
full resource/DataAccessLabel/assurance policy, cache max-staleness, production
IAM or representative cross-process acceptance; those remain R1b/R6 gates.

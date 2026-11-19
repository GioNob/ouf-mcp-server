# M2 typed/recovery composition — 26 October 2026

This candidate composes existing typed review and ASSETS/LATEST recovery from 0a76376a7c6a0f07597c1c3aaf447b29deed597f onto diagnostic/acquisition READ source 9a374dd07745f059de3ad39757c68f02db6d29c3. It does not replace that newer line with the old branch. Three-way code composition uses common ancestor 1f2c28d7abadae78680b040ba7b58be62ef85ee9.

Only three manifest entries change: managed preview (ASSETS pagination), source review prepare (existing decimal/date types), and source onboarding read (LATEST). Every other current manifest entry, including both acquisition READ tools, is retained unchanged. The diagnostic projection code and existing denial regressions are retained. No performance-profile, budget or unrelated verification-script change is imported from the older branch.

LATEST authorizes the source coarsely; the owner must still enforce current exact-version and tenant/subject authorization. It creates no grant, activation, new profiling job or acquisition.

The SDK recovery fixture now uses the real owner response key items and verifies the returned asset identity, state and pagination flag. The fixture remains synthetic: it does not prove a live owner composition, failed-job discovery, two real clients or M2 acceptance. The owner list still needs the separate D3 job/work projection.

Both touched contract workflows use the current Go 1.26.10 profile and pinned Gateway schema source c22fbf7f5593f612aba7e318797c9459d0b871a7. Existing acquisition diagnostics and full-module CI remain mandatory. Local Go is unavailable; qualification must be established from direct CI logs for this commit before any deployment preparation is accepted.

No runtime update, consent, budget reset, acquisition or release acceptance is authorized by this change. The Farmacie FAILED job and asset remain preserved; its server cause remains unobserved. See GioNob/ouf-deploy PR50 audit and D2/D3 roadmap.

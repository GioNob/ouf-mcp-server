# MCP 1G — Retention age enforcement correction

Normative baseline: MCP Server PET v1.2 retention/maintenance requirements.

The PET consistency audit identified that orphaned retry/budget parents could be eligible for cleanup solely because references disappeared, without independently proving that their governed retention window had expired.

This correction makes `budget_window.window_end < p_before` a required predicate for deleting `retry_guard`, `budget_distinct_object` and `retry_equivalence_group`. Orphanhood remains necessary but is no longer sufficient.

PostgreSQL integration evidence covers both sides of the invariant: old unreferenced graphs are purged, while recent orphan retry/budget parents survive the same maintenance pass.

No PET deviation is introduced.

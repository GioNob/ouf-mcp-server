"""Exact, reviewable environment overlay for the MCP attachment rollout."""


def expected_environment(original: list[str], mode: str, origin: str | None) -> list[str]:
    names = [entry.partition("=")[0] for entry in original]
    if len(names) != len(set(names)) or "MCP_HOST_FILE_ORIGINS" in names:
        raise ValueError("Original managed-upload environment is unexpected")
    prior_probe = "MCP_MANAGED_UPLOAD_ENABLED=probe" in original
    prior_enabled = "MCP_MANAGED_UPLOAD_ENABLED=true" in original
    if "MCP_MANAGED_UPLOAD_ENABLED" in names and not (prior_probe or prior_enabled):
        raise ValueError("Original managed-upload environment is unexpected")
    if prior_enabled and mode != "enabled":
        raise ValueError("Enabled upload cannot be disabled by an image rollout")
    base = [entry for entry in original if entry not in
            ("MCP_MANAGED_UPLOAD_ENABLED=probe", "MCP_MANAGED_UPLOAD_ENABLED=true")]
    if mode == "off":
        if origin is not None or prior_probe:
            raise ValueError("Host origin probe cannot be disabled through an implicit rollout")
        return base
    if mode == "probe":
        if origin is not None:
            raise ValueError("Host origin must be unknown during the probe")
        return [*base, "MCP_MANAGED_UPLOAD_ENABLED=probe"]
    if mode != "enabled" or origin is not None:
        raise ValueError("Enabled upload accepts host-provided files without a static origin")
    return [*base, "MCP_MANAGED_UPLOAD_ENABLED=true"]

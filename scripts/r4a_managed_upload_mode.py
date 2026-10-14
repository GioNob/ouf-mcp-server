"""Exact, reviewable environment overlay for the MCP attachment rollout."""
from urllib.parse import urlsplit


def expected_environment(original: list[str], mode: str, origin: str | None,
                         picker_url: str | None = None) -> list[str]:
    names = [entry.partition("=")[0] for entry in original]
    if len(names) != len(set(names)) or "MCP_HOST_FILE_ORIGINS" in names:
        raise ValueError("Original managed-upload environment is unexpected")
    prior_probe = "MCP_MANAGED_UPLOAD_ENABLED=probe" in original
    prior_enabled = "MCP_MANAGED_UPLOAD_ENABLED=true" in original
    prior_picker = "MCP_MANAGED_UPLOAD_ENABLED=picker" in original
    if "MCP_MANAGED_UPLOAD_ENABLED" in names and not (prior_probe or prior_enabled or prior_picker):
        raise ValueError("Original managed-upload environment is unexpected")
    if (prior_enabled or prior_picker) and mode == "off":
        raise ValueError("Enabled upload cannot be removed by an implicit image rollout")
    base = [entry for entry in original if entry.partition("=")[0] not in
            ("MCP_MANAGED_UPLOAD_ENABLED", "MCP_MANAGED_FILE_PICKER_URL")]
    if mode == "off":
        if origin is not None or picker_url is not None or prior_probe:
            raise ValueError("Host origin probe cannot be disabled through an implicit rollout")
        return base
    if mode == "picker":
        if origin is not None or not picker_url or any(c in picker_url for c in "\r\n"):
            raise ValueError("First-party picker URL is required")
        parsed = urlsplit(picker_url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or parsed.path != "/trusted-human/managed-files/"):
            raise ValueError("Invalid first-party picker URL")
        return [*base, "MCP_MANAGED_UPLOAD_ENABLED=picker", "MCP_MANAGED_FILE_PICKER_URL=" + picker_url]
    if mode == "probe":
        if origin is not None or picker_url is not None:
            raise ValueError("Host origin must be unknown during the probe")
        # An explicit probe rollout may replace the failed enabled candidate;
        # it publishes only the read-only host attachment diagnostic.
        return [*base, "MCP_MANAGED_UPLOAD_ENABLED=probe"]
    if mode != "enabled" or origin is not None or picker_url is not None:
        raise ValueError("Enabled upload accepts host-provided files without a static origin")
    return [*base, "MCP_MANAGED_UPLOAD_ENABLED=true"]

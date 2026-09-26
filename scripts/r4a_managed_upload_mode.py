"""Exact, reviewable environment overlay for the MCP attachment rollout."""

import ipaddress
import re
from urllib.parse import urlsplit


def expected_environment(original: list[str], mode: str, origin: str | None) -> list[str]:
    names = [entry.partition("=")[0] for entry in original]
    if len(names) != len(set(names)) or "MCP_HOST_FILE_ORIGINS" in names:
        raise ValueError("Original managed-upload environment is unexpected")
    prior_probe = "MCP_MANAGED_UPLOAD_ENABLED=probe" in original
    if "MCP_MANAGED_UPLOAD_ENABLED" in names and not prior_probe:
        raise ValueError("Original managed-upload environment is unexpected")
    base = [entry for entry in original if entry != "MCP_MANAGED_UPLOAD_ENABLED=probe"]
    if mode == "off":
        if origin is not None or prior_probe:
            raise ValueError("Host origin probe cannot be disabled through an implicit rollout")
        return base
    if mode == "probe":
        if origin is not None:
            raise ValueError("Host origin must be unknown during the probe")
        return [*base, "MCP_MANAGED_UPLOAD_ENABLED=probe"]
    if mode != "enabled" or not origin or not isinstance(origin, str):
        raise ValueError("Exact approved host origin is required to enable upload")
    try:
        url = urlsplit(origin)
        host = url.hostname
        if (url.scheme != "https" or not host or url.netloc != url.netloc.lower()
                or url.username is not None or url.password is not None
                or url.path or url.query or url.fragment or url.geturl() != origin
                or "*" in host or len(host) > 253 or not re.fullmatch(r"[a-z0-9.-]+", host)
                or host == "localhost" or "." not in host):
            raise ValueError("Host origin is not an exact HTTPS DNS origin")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise ValueError("Literal IP address is not an approved host origin")
        if url.port is not None and url.port != 443:
            raise ValueError("Nonstandard port is not an approved host origin")
    except (ValueError, AttributeError) as exc:
        raise ValueError("Host origin is not an exact HTTPS DNS origin") from exc
    return [*base, "MCP_MANAGED_UPLOAD_ENABLED=true", "MCP_HOST_FILE_ORIGINS=" + origin]

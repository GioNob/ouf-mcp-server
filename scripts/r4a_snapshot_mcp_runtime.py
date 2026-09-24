#!/usr/bin/env python3
"""Capture the live MCP container configuration in a private root-owned snapshot.

Run as root on the OUF host. The inspect payload includes environment values
and secret mount sources; never print or upload the resulting JSON.
"""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile


def snapshot(backup_root: Path) -> Path:
    if os.geteuid() != 0:
        raise RuntimeError("Root is required")
    root = backup_root.resolve(strict=True)
    mode = root.stat()
    if not root.is_dir() or mode.st_uid != 0 or stat.S_IMODE(mode.st_mode) & 0o077:
        raise RuntimeError("Backup root must be a private root-owned directory")
    result = subprocess.run(
        ["docker", "inspect", "ouf-mcp"], check=True, capture_output=True, text=True
    )
    inspected = json.loads(result.stdout)
    if len(inspected) != 1:
        raise RuntimeError("Expected one MCP container")
    item = inspected[0]
    if item.get("Name") != "/ouf-mcp" or not item.get("State", {}).get("Running"):
        raise RuntimeError("Expected the running ouf-mcp container")
    config = item.get("Config") or {}
    if config.get("User") != "10005:10005":
        raise RuntimeError("Unexpected MCP container user")
    if (item.get("HostConfig") or {}).get("RestartPolicy", {}).get("Name") != "unless-stopped":
        raise RuntimeError("Unexpected MCP restart policy")
    networks = (item.get("NetworkSettings") or {}).get("Networks") or {}
    if "ouf-backend" not in networks:
        raise RuntimeError("Missing ouf-backend network")
    targets = {mount.get("Destination") for mount in item.get("Mounts") or []}
    required = {"/run/secrets/mcp-client-secret", "/run/secrets/mcp-fingerprint-key"}
    if not required.issubset(targets):
        raise RuntimeError("Missing MCP secret mount destination")
    directory = Path(tempfile.mkdtemp(prefix="r4a-mcp-runtime-", dir=root))
    os.chmod(directory, 0o700)
    target = directory / "container.inspect.json"
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(inspected, stream)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-root", type=Path, default=Path("/opt/ouf/backup"))
    args = parser.parse_args()
    try:
        path = snapshot(args.backup_root)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f"MCP_SNAPSHOT_FAILED: {type(exc).__name__}") from None
    print(f"PRIVATE_MCP_DOCKER_SNAPSHOT={path}")
    print("SNAPSHOT_MODE=0600 ROOT_ONLY; CONTAINER_UNCHANGED")


if __name__ == "__main__":
    main()

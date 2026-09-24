#!/usr/bin/env python3
"""Read-only MCP rollout preflight. Never print inspect contents or secret sources."""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess


def read_private_snapshot(path: Path) -> dict:
    parent = path.parent
    directory = parent.stat()
    file_stat = path.stat()
    if os.geteuid() != 0 or directory.st_uid != 0 or stat.S_IMODE(directory.st_mode) & 0o077:
        raise RuntimeError("Snapshot directory is not private and root-owned")
    if file_stat.st_uid != 0 or stat.S_IMODE(file_stat.st_mode) & 0o077:
        raise RuntimeError("Snapshot file is not private and root-owned")
    data = json.loads(path.read_text(encoding="utf-8"))
    if len(data) != 1 or data[0].get("Name") != "/ouf-mcp":
        raise RuntimeError("Unexpected snapshot container")
    return data[0]


def safe_summary(original: dict, current: dict) -> None:
    if original.get("Id") != current.get("Id") or not current.get("State", {}).get("Running"):
        raise RuntimeError("Original MCP container changed or stopped")
    config = original.get("Config") or {}
    host = original.get("HostConfig") or {}
    network = original.get("NetworkSettings") or {}
    if config.get("User") != "10005:10005" or host.get("RestartPolicy", {}).get("Name") != "unless-stopped":
        raise RuntimeError("Unexpected identity or restart policy")
    mounts = original.get("Mounts") or []
    targets = {m.get("Destination") for m in mounts}
    if targets != {"/run/secrets/mcp-client-secret", "/run/secrets/mcp-fingerprint-key"}:
        raise RuntimeError("Unexpected mount destinations; inspect privately before rollout")
    if any(m.get("RW") for m in mounts):
        raise RuntimeError("Secret mounts must be read-only")
    networks = network.get("Networks") or {}
    if set(networks) != {"ouf-backend"}:
        raise RuntimeError("Unexpected network set")
    ports = host.get("PortBindings") or {}
    if ports:
        raise RuntimeError("Unexpected published port bindings")
    bindings = host.get("Binds") or []
    print("MCP_PREFLIGHT=PASS; ORIGINAL_RUNNING_AND_ID_MATCH=true")
    print("MCP_USER=10005:10005; RESTART=unless-stopped; NETWORK=ouf-backend")
    print("MCP_MOUNT_TARGETS=2; SECRET_MOUNTS_READ_ONLY=true")
    print(f"MCP_BIND_COUNT={len(bindings)}; MCP_MOUNT_COUNT={len(mounts)}")
    print(f"MCP_IMAGE_ID={original.get('Image')}")
    print(f"MCP_IMAGE_LABEL={config.get('Image')}")
    print(f"MCP_MEMORY={host.get('Memory', 0)}; MCP_MEMORY_SWAP={host.get('MemorySwap', 0)}")
    print(f"MCP_READ_ONLY_ROOT={host.get('ReadonlyRootfs', False)}; MCP_TMPFS_COUNT={len(host.get('Tmpfs') or {})}")
    print(f"MCP_ENTRYPOINT_COUNT={len(config.get('Entrypoint') or [])}; MCP_CMD_COUNT={len(config.get('Cmd') or [])}")
    print(f"MCP_WORKDIR_SET={bool(config.get('WorkingDir'))}; MCP_HEALTHCHECK={bool(config.get('Healthcheck'))}")
    print(f"MCP_LOG_DRIVER={(host.get('LogConfig') or {}).get('Type')}")
    print(f"MCP_ENV_NAMES={json.dumps(sorted(x.split('=', 1)[0] for x in config.get('Env') or []))}")
    print("INSPECT_SECRET_VALUES_AND_MOUNT_SOURCES_NOT_PRINTED=true; CONTAINER_UNCHANGED=true")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args()
    try:
        original = read_private_snapshot(args.snapshot)
        result = subprocess.run(
            ["docker", "inspect", "ouf-mcp"], capture_output=True, text=True, check=True
        )
        inspected = json.loads(result.stdout)
        if len(inspected) != 1:
            raise RuntimeError("Expected one current MCP container")
        safe_summary(original, inspected[0])
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        raise SystemExit("MCP_PREFLIGHT=FAIL; CONTAINER_UNCHANGED=true") from None


if __name__ == "__main__":
    main()

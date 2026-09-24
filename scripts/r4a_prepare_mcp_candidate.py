#!/usr/bin/env python3
"""Prepare a private MCP R4a launch candidate without changing containers."""
import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile

IMAGE = "ouf-mcp:r4a-fbd0e8c"
IMAGE_ID = "sha256:ac64e6e7220a9fcc614b6b3dd90a30e01513bab804dbc3f462cfc68f6c6c999a"


def docker(*args: str) -> list[dict]:
    return json.loads(subprocess.run(["docker", *args], capture_output=True, text=True, check=True).stdout)


def private(path: Path, mode: int) -> None:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) and not stat.S_ISREG(info.st_mode):
        raise ValueError("Invalid private path")
    if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != mode:
        raise ValueError("Private ownership or mode changed")


def prepare(snapshot: Path) -> Path:
    if os.geteuid() != 0:
        raise ValueError("Root required")
    private(snapshot.parent, 0o700)
    private(snapshot, 0o600)
    old_list = json.loads(snapshot.read_text(encoding="utf-8"))
    if len(old_list) != 1 or old_list[0].get("Name") != "/ouf-mcp":
        raise ValueError("Unexpected snapshot")
    old = old_list[0]
    current = docker("inspect", "ouf-mcp")[0]
    if old.get("Id") != current.get("Id") or not current.get("State", {}).get("Running"):
        raise ValueError("Original container changed")
    image = docker("image", "inspect", IMAGE)[0]
    if image["Id"] != IMAGE_ID:
        raise ValueError("Candidate image ID changed")
    ic = image["Config"]
    if ic.get("User") != "10005:10005" or ic.get("Entrypoint") != ["/usr/local/bin/ouf-mcp"] or ic.get("Cmd") != ["server"]:
        raise ValueError("Candidate image launch is unexpected")
    config = old["Config"]
    host = old["HostConfig"]
    nets = old["NetworkSettings"]["Networks"]
    if config.get("User") != "10005:10005" or config.get("Cmd") != ["server"] or len(config.get("Entrypoint") or []) != 1:
        raise ValueError("Original process launch needs private review")
    if set(nets) != {"ouf-backend"} or host.get("NetworkMode") != "ouf-backend":
        raise ValueError("Original network needs private review")
    if host.get("RestartPolicy", {}).get("Name") != "unless-stopped":
        raise ValueError("Original restart policy changed")
    if host.get("Memory") or host.get("MemorySwap") or host.get("PortBindings") or host.get("PublishAllPorts"):
        raise ValueError("Unexpected original resource or port settings")
    if (host.get("Privileged") or host.get("ReadonlyRootfs") or host.get("AutoRemove") or
            host.get("Tmpfs") or host.get("CapAdd") or host.get("CapDrop") or
            host.get("SecurityOpt") or host.get("Devices") or host.get("DeviceRequests") or
            host.get("Dns") or host.get("ExtraHosts") or host.get("Ulimits") or
            host.get("Sysctls") or host.get("GroupAdd") or host.get("VolumesFrom") or
            host.get("Links") or host.get("PidMode") or host.get("UsernsMode") or
            host.get("NanoCpus") or host.get("CpuShares") or host.get("CpuQuota") or
            host.get("CpuPeriod") or host.get("PidsLimit") not in (None, 0) or
            host.get("OomKillDisable") or host.get("Init") or
            host.get("LogConfig", {}).get("Type") != "json-file" or
            host.get("LogConfig", {}).get("Config") or host.get("IpcMode") != "private" or
            host.get("Binds")):
        raise ValueError("Unsupported Docker option; review privately")
    mounts = old.get("Mounts") or []
    expected = {"/run/secrets/mcp-client-secret", "/run/secrets/mcp-fingerprint-key"}
    if len(mounts) != 2 or {m.get("Destination") for m in mounts} != expected:
        raise ValueError("Unexpected secret mount targets")
    for m in mounts:
        if m.get("Type") != "bind" or m.get("RW") or not Path(m["Source"]).is_file():
            raise ValueError("Invalid secret mount")
    envs = config.get("Env") or []
    names = set()
    for entry in envs:
        name, sep, value = entry.partition("=")
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) or name in names or "\n" in value or "\r" in value:
            raise ValueError("Environment cannot be represented safely")
        names.add(name)
    folder = Path(tempfile.mkdtemp(prefix="candidate-mcp-", dir=snapshot.parent))
    os.chmod(folder, 0o700)
    path = folder / "mcp.env"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        output.write("\n".join(envs) + "\n")
        output.flush()
        os.fsync(output.fileno())
    return folder


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args()
    try:
        folder = prepare(args.snapshot)
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        raise SystemExit("MCP_CANDIDATE_PREPARE=BLOCKED; CONTAINERS_UNCHANGED=true") from None
    print(f"PRIVATE_MCP_CANDIDATE={folder}")
    print("MCP_CANDIDATE_PREPARED=true; CONTAINERS_UNCHANGED=true")


if __name__ == "__main__":
    main()

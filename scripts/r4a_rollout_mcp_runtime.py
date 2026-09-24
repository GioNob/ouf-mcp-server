#!/usr/bin/env python3
"""MCP R4a rollout with dry-run default and retained original for rollback.

Private snapshot and candidate env file contain secrets. Never print them.
"""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import time

IMAGE = "ouf-mcp:r4a-fbd0e8c"
IMAGE_ID = "sha256:ac64e6e7220a9fcc614b6b3dd90a30e01513bab804dbc3f462cfc68f6c6c999a"
NAME = "ouf-mcp"
BACKUP = "ouf-mcp-r4a-original"


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True).stdout.strip()


def inspect(name: str) -> dict:
    result = json.loads(docker("inspect", name))
    if len(result) != 1:
        raise ValueError("Unexpected inspect result")
    return result[0]


def private(path: Path, mode: int) -> None:
    info = path.lstat()
    if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != mode or not (
        stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)
    ):
        raise ValueError("Private file ownership or mode changed")


def original_and_args(snapshot: Path, candidate: Path) -> tuple[dict, list[str]]:
    private(snapshot.parent, 0o700)
    private(snapshot, 0o600)
    private(candidate, 0o700)
    private(candidate / "mcp.env", 0o600)
    if candidate.parent != snapshot.parent or not candidate.name.startswith("candidate-mcp-"):
        raise ValueError("Candidate must be adjacent to snapshot")
    saved = json.loads(snapshot.read_text(encoding="utf-8"))
    if len(saved) != 1 or saved[0].get("Name") != "/" + NAME:
        raise ValueError("Unexpected snapshot")
    original = saved[0]
    live = inspect(NAME)
    if live.get("Id") != original.get("Id") or not live.get("State", {}).get("Running"):
        raise ValueError("Original ID changed or not running")
    image = json.loads(docker("image", "inspect", IMAGE))[0]
    if image["Id"] != IMAGE_ID:
        raise ValueError("Candidate image ID changed")
    image_config = image["Config"]
    config = original["Config"]
    host = original["HostConfig"]
    if (config.get("User") != "10005:10005" or config.get("Cmd") != ["server"] or
            len(config.get("Entrypoint") or []) != 1 or
            config.get("WorkingDir") != image_config.get("WorkingDir") or
            image_config.get("User") != "10005:10005" or
            image_config.get("Entrypoint") != ["/usr/local/bin/ouf-mcp"] or
            image_config.get("Cmd") != ["server"]):
        raise ValueError("Image or process defaults need private review")
    if (host.get("NetworkMode") != "ouf-backend" or
            set(original["NetworkSettings"]["Networks"]) != {"ouf-backend"} or
            host.get("RestartPolicy", {}).get("Name") != "unless-stopped" or
            host.get("RestartPolicy", {}).get("MaximumRetryCount") not in (None, 0) or
            host.get("Binds") or host.get("PortBindings") or host.get("PublishAllPorts") or
            host.get("Memory") or host.get("MemorySwap") or
            host.get("Privileged") or host.get("ReadonlyRootfs") or host.get("AutoRemove") or
            host.get("Tmpfs") or host.get("CapAdd") or host.get("CapDrop") or
            host.get("SecurityOpt") or host.get("Devices") or host.get("DeviceRequests") or
            host.get("Dns") or host.get("ExtraHosts") or host.get("Ulimits") or
            host.get("Sysctls") or host.get("GroupAdd") or host.get("VolumesFrom") or
            host.get("Links") or host.get("PidMode") or host.get("UsernsMode") or
            host.get("NanoCpus") or host.get("CpuShares") or host.get("CpuQuota") or
            host.get("CpuPeriod") or host.get("PidsLimit") not in (None, 0) or
            host.get("OomKillDisable") or host.get("Init") or
            host.get("LogConfig", {}).get("Type") != "json-file" or
            host.get("LogConfig", {}).get("Config") or host.get("IpcMode") != "private"):
        raise ValueError("Unsupported Docker launch option")
    mounts = original.get("Mounts") or []
    targets = {"/run/secrets/mcp-client-secret", "/run/secrets/mcp-fingerprint-key"}
    if len(mounts) != 2 or {m.get("Destination") for m in mounts} != targets:
        raise ValueError("Unexpected mount destinations")
    for mount in mounts:
        if (mount.get("Type") != "bind" or mount.get("RW") or
                not Path(mount["Source"]).is_file() or
                any(ch in mount["Source"] for ch in ",\n\r")):
            raise ValueError("Unexpected secret bind")
    expected_env = config.get("Env") or []
    if (candidate / "mcp.env").read_text(encoding="utf-8") != "\n".join(expected_env) + "\n":
        raise ValueError("Candidate environment differs from snapshot")
    args = ["create", "--name", NAME, "--pull", "never", "--user", "10005:10005",
            "--restart", "unless-stopped", "--network", "ouf-backend",
            "--shm-size", str(host["ShmSize"]), "--log-driver", "json-file",
            "--env-file", str(candidate / "mcp.env")]
    aliases = original["NetworkSettings"]["Networks"]["ouf-backend"].get("Aliases") or []
    for alias in sorted(set(aliases) - {NAME, original["Id"][:12]}):
        if not alias or alias.startswith("-"):
            raise ValueError("Unexpected network alias")
        args += ["--network-alias", alias]
    for mount in sorted(mounts, key=lambda m: m["Destination"]):
        args += ["--mount", "type=bind,src=" + mount["Source"] +
                 ",dst=" + mount["Destination"] + ",readonly"]
    args.append(IMAGE)
    return original, args


def write_state(candidate: Path, original: dict) -> dict:
    state = {"old_id": original["Id"], "backup": BACKUP, "image_id": IMAGE_ID}
    fd = os.open(candidate / "rollout-mcp.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump(state, output)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    return state


def rollback(state: dict) -> None:
    try:
        current = inspect(NAME)
    except subprocess.CalledProcessError:
        current = None
    if current and current["Id"] != state["old_id"]:
        docker("update", "--restart", "no", NAME)
        if current["State"]["Running"]:
            docker("stop", NAME)
        docker("rm", NAME)
    try:
        previous = inspect(BACKUP)
    except subprocess.CalledProcessError:
        previous = None
    if previous:
        if previous["Id"] != state["old_id"]:
            raise ValueError("Rollback container ID mismatch")
        docker("rename", BACKUP, NAME)
    restored = inspect(NAME)
    if restored["Id"] != state["old_id"]:
        raise ValueError("Original container cannot be restored")
    if not restored["State"]["Running"]:
        docker("start", NAME)
    docker("update", "--restart", "unless-stopped", NAME)
    for attempt in range(20):
        try:
            docker("exec", NAME, "wget", "-q", "-O", "/dev/null",
                   "http://127.0.0.1:8080/health/ready")
            return
        except subprocess.CalledProcessError:
            if attempt == 19:
                raise ValueError("Original did not become ready after rollback")
            time.sleep(2)


def verify_staged(original: dict) -> None:
    for attempt in range(20):
        current = inspect(NAME)
        if current["Image"] != IMAGE_ID or not current["State"]["Running"]:
            raise ValueError("Candidate did not stay running")
        try:
            docker("exec", NAME, "wget", "-q", "-O", "/dev/null",
                   "http://127.0.0.1:8080/health/ready")
            break
        except subprocess.CalledProcessError:
            if attempt == 19:
                raise ValueError("Candidate readiness failed")
            time.sleep(2)
    if (current["Config"]["User"] != original["Config"]["User"] or
            current["Config"]["Env"] != original["Config"]["Env"] or
            current["HostConfig"]["RestartPolicy"]["Name"] != "unless-stopped" or
            set(current["NetworkSettings"]["Networks"]) != {"ouf-backend"}):
        raise ValueError("Candidate launch differs from original")
    a = sorted((m["Source"], m["Destination"], m["RW"]) for m in current["Mounts"])
    b = sorted((m["Source"], m["Destination"], m["RW"]) for m in original["Mounts"])
    if a != b:
        raise ValueError("Secret mounts changed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("run as root")
    state_path = args.candidate / "rollout-mcp.json"
    try:
        if args.rollback:
            private(args.candidate, 0o700)
            private(state_path, 0o600)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if state.get("backup") != BACKUP or state.get("image_id") != IMAGE_ID:
                raise ValueError("Rollback state mismatch")
            rollback(state)
            print("MCP_R4A_ROLLBACK_RESTORED=true")
            return
        original, create_args = original_and_args(args.snapshot, args.candidate)
        if state_path.exists():
            raise ValueError("Candidate already staged; use rollback if needed")
        try:
            inspect(BACKUP)
        except subprocess.CalledProcessError:
            pass
        else:
            raise ValueError("Rollback container name already exists")
        if not args.apply:
            print("MCP_R4A_DRY_RUN=PASS")
            print("CANDIDATE_IMAGE_ID=" + IMAGE_ID)
            print("ORIGINAL_ID_MATCH=true; ROLLBACK_ORIGINAL_RETAINED=true")
            print("NO_CONTAINERS_CHANGED=true")
            return
        state = write_state(args.candidate, original)
        print("MCP_R4A_APPLY_STARTED=true", flush=True)
        try:
            docker("update", "--restart", "no", NAME)
            docker("stop", NAME)
            docker("rename", NAME, BACKUP)
            docker(*create_args)
            docker("start", NAME)
            verify_staged(original)
        except (ValueError, subprocess.SubprocessError, KeyError, TypeError):
            rollback(state)
            raise ValueError("Candidate failed; original restored")
        print("MCP_R4A_STAGED=true")
        print("ORIGINAL_ROLLBACK_CONTAINER=" + BACKUP)
        print("POLICY_AND_CONNECTOR_PROBES_STILL_REQUIRED=true")
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError, json.JSONDecodeError):
        raise SystemExit("MCP_R4A_ROLLOUT=BLOCKED; CHECK_ORIGINAL_STATE=true") from None


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Rollback-backed lab rollout; direct-fetch enablement is PET-blocked."""

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

GATEWAY_COMMIT = "17aee8cad85b00b8551e4a3e0f68ada5e307deff"
ROOT = Path("/etc/ouf/deploy-snapshots")
ROUTE_IDS = {"mcp-managed-file-profile", "mcp-managed-file-preview", "mcp-managed-file-create"}
ADMIN_KEY = Path("/opt/ouf/secrets/apisix-admin-key")


class Blocked(Exception):
    pass


def command(args, *, env=None):
    result = subprocess.run(args, capture_output=True, text=True, env=env, check=False)
    if result.returncode:
        match = re.search(r"anonymous protected route HTTP (\d{3})", result.stderr)
        code = "ANONYMOUS_HTTP_" + match.group(1) if match else "COMMAND_FAILED"
        raise Blocked(code + "_" + Path(args[0]).name)
    return result.stdout


def private(path, mode):
    info = path.lstat()
    if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != mode or path.is_symlink():
        raise Blocked("PRIVATE_PATH_INVALID")


def git_args(repo, *args):
    # The operator owns the checkout; the root orchestrator trusts only this
    # explicit repository path without changing global Git configuration.
    resolved = repo.resolve(strict=True)
    return ["git", "-c", "safe.directory=" + str(resolved), "-C", str(resolved), *args]


def archive(repo, commit, target):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise Blocked("INVALID_COMMIT")
    if command(git_args(repo, "rev-parse", commit + "^{commit}")).strip() != commit:
        raise Blocked("COMMIT_MISMATCH")
    exported = subprocess.Popen(git_args(repo, "archive", "--format=tar", commit),
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        extracted = subprocess.run(["tar", "-xf", "-", "-C", str(target)],
                                   stdin=exported.stdout, capture_output=True, check=False)
    finally:
        exported.stdout.close()
    if exported.wait() or extracted.returncode:
        raise Blocked("PINNED_ARCHIVE_FAILED")


def call_module(source, module, *args):
    environment = dict(os.environ, PYTHONPATH=str(source))
    return command([sys.executable, "-P", "-m", module, *map(str, args)], env=environment)


def marked_path(output, marker):
    values = [line.split("=", 1)[1] for line in output.splitlines() if line.startswith(marker + "=")]
    if len(values) != 1:
        raise Blocked("EXPECTED_PRIVATE_PATH_MISSING")
    return Path(values[0])


def image(repo, commit, tag):
    try:
        inspected = json.loads(command(["docker", "image", "inspect", tag]))[0]
    except Blocked:
        exported = subprocess.Popen(git_args(repo, "archive", "--format=tar", commit),
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            built = subprocess.run(["docker", "build", "--pull=false", "--quiet",
                                    "--label", "org.opencontainers.image.revision=" + commit,
                                    "-t", tag, "-"], stdin=exported.stdout,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False)
        finally:
            exported.stdout.close()
        if exported.wait() or built.returncode:
            raise Blocked("CANDIDATE_IMAGE_BUILD_FAILED")
        inspected = json.loads(command(["docker", "image", "inspect", tag]))[0]
    if (inspected.get("Config", {}).get("Labels") or {}).get("org.opencontainers.image.revision") != commit:
        raise Blocked("CANDIDATE_IMAGE_REVISION_MISMATCH")
    return inspected["Id"]


def route_admin(gateway_source):
    sys.path.insert(0, str(gateway_source))
    from ops.apisix.deploy_internal_m2m_routes import Admin
    settings = argparse.Namespace(backup_dir=ROOT, admin_key=ADMIN_KEY,
                                  container="ouf-apisix", curl_image="curlimages/curl:8.16.0")
    with contextlib.redirect_stdout(io.StringIO()):
        return Admin(settings, "managed-file-readback-")


def route_state(gateway_source, materialization):
    if str(gateway_source) not in sys.path:
        sys.path.insert(0, str(gateway_source))
    from ops.apisix.deploy_internal_m2m_routes import route_value
    from ops.apisix.deploy_managed_file_mcp import select
    from ops.apisix.deploy_managed_file_upload import validate
    expected = select(json.loads((materialization / "mcp-routes.json").read_text()))
    upload = json.loads((materialization / "upload-route.json").read_text())
    validate(upload)
    if {route["id"] for route in expected} != ROUTE_IDS:
        raise Blocked("MANAGED_FILE_ROUTE_SET_CHANGED")
    admin = route_admin(gateway_source)
    try:
        for route in expected:
            status, doc = admin.route("GET", route["id"])
            actual = route_value(doc) if status == 200 else {}
            if status != 200 or any(actual.get(key) != value for key, value in route.items()):
                raise Blocked("EXISTING_MCP_ROUTE_DRIFT")
        status, doc = admin.route("GET", upload["id"])
        if status == 404:
            return False
        actual = route_value(doc) if status == 200 else {}
        if status != 200 or any(actual.get(key) != value for key, value in upload.items()):
            raise Blocked("UPLOAD_ROUTE_DRIFT")
        return True
    finally:
        admin.close()


def mcp_args(snapshot, candidate, tag, image_id, backup_name, mode, origin):
    arguments = ["--snapshot", snapshot, "--candidate", candidate,
                 "--image", tag, "--image-id", image_id, "--backup-name", backup_name,
                 "--upload-mode", mode]
    if origin:
        arguments += ["--host-origin", origin]
    return arguments


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mcp-commit", required=True)
    parser.add_argument("--snapshot", type=Path,
                        help="Optional existing private snapshot; default creates one automatically")
    parser.add_argument("--materialization", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("probe", "enabled"))
    parser.add_argument("--host-origin")
    parser.add_argument("--gateway-repo", type=Path, default=Path("/opt/ouf/gateway"))
    parser.add_argument("--mcp-repo", type=Path, default=Path("/opt/ouf/mcp"))
    args = parser.parse_args()
    candidate = None
    upload_backup = None
    rollout_started = False
    gateway_source = None
    mcp_source = None
    rollback_args = None
    try:
        # MCP PET v1.4 section 38 forbids external fetch from MCP. The present
        # enabled image performs exactly that fetch, and an unrestricted URL
        # forwarded to Gateway also fails Gateway PET v1.5 T11.3. Gate before
        # any snapshot, image build, route write or container mutation.
        if args.mode == "enabled":
            raise Blocked("PET_ATTACHMENT_BOUNDARY_UNRESOLVED")
        if os.geteuid() != 0 or not ADMIN_KEY.is_file():
            raise Blocked("ROOT_OR_ADMIN_KEY_REQUIRED")
        private(ROOT, 0o700)
        private(args.materialization, 0o700)
        if args.host_origin is not None:
            raise Blocked("STATIC_HOST_ORIGIN_UNSUPPORTED")
        if (not re.fullmatch(r"[0-9a-f]{40}", args.mcp_commit)
                or command(git_args(args.gateway_repo, "rev-parse", GATEWAY_COMMIT + "^{commit}")).strip() != GATEWAY_COMMIT):
            raise Blocked("PINNED_SOURCE_MISSING")
        folder = Path(tempfile.mkdtemp(prefix="r4a-managed-attachment-", dir=ROOT))
        gateway_source, mcp_source = folder / "gateway", folder / "mcp"
        gateway_source.mkdir(mode=0o700)
        mcp_source.mkdir(mode=0o700)
        archive(args.gateway_repo, GATEWAY_COMMIT, gateway_source)
        archive(args.mcp_repo, args.mcp_commit, mcp_source)
        if args.snapshot is None:
            output = call_module(mcp_source, "scripts.r4a_snapshot_mcp_runtime", "--backup-root", ROOT)
            args.snapshot = marked_path(output, "PRIVATE_MCP_DOCKER_SNAPSHOT")
        private(args.snapshot.parent, 0o700)
        private(args.snapshot, 0o600)
        old = json.loads(args.snapshot.read_text())
        live = json.loads(command(["docker", "inspect", "ouf-mcp"]))
        if len(old) != 1 or len(live) != 1 or old[0]["Id"] != live[0]["Id"] or not live[0]["State"]["Running"]:
            raise Blocked("MCP_SNAPSHOT_OR_LIVE_CHANGED")
        already_installed = route_state(gateway_source, args.materialization)
        tag = "ouf-mcp:r4a-" + args.mcp_commit[:7]
        image_id = image(args.mcp_repo, args.mcp_commit, tag)
        prep_args = ["--snapshot", args.snapshot, "--image", tag, "--image-id", image_id,
                     "--upload-mode", args.mode]
        if args.host_origin:
            prep_args += ["--host-origin", args.host_origin]
        output = call_module(mcp_source, "scripts.r4a_prepare_mcp_candidate", *prep_args)
        candidate = marked_path(output, "PRIVATE_MCP_CANDIDATE")
        backup_name = "ouf-mcp-r4a-rollback-" + args.mcp_commit[:7] + "-" + args.mode
        rollback_args = mcp_args(args.snapshot, candidate, tag, image_id, backup_name, args.mode, args.host_origin)
        call_module(mcp_source, "scripts.r4a_rollout_mcp_runtime", *rollback_args)
        if not already_installed:
            output = call_module(gateway_source, "ops.apisix.deploy_managed_file_upload",
                                 "--materialization", args.materialization / "upload-route.json",
                                 "--admin-key", ADMIN_KEY, "--backup-dir", ROOT)
            upload_backup = marked_path(output, "BACKUP")
        rollout_started = True
        call_module(mcp_source, "scripts.r4a_rollout_mcp_runtime", *rollback_args, "--apply")
        current = json.loads(command(["docker", "inspect", "ouf-mcp"]))[0]
        if (current["Image"] != image_id or not current["State"]["Running"]
                or "MCP_MANAGED_UPLOAD_ENABLED=" + ("probe" if args.mode == "probe" else "true")
                not in current["Config"]["Env"] or not route_state(gateway_source, args.materialization)):
            raise Blocked("POST_ROLLOUT_READBACK_FAILED")
        print("MANAGED_ATTACHMENT_ROLLOUT=PASS")
        print("MODE=" + args.mode.upper() + " MCP_IMAGE_ID=" + image_id)
        print("BACKUP_MCP=" + backup_name)
        print("PRIVATE_MCP_DOCKER_SNAPSHOT=" + str(args.snapshot))
        print("BACKUP_GATEWAY_UPLOAD=" + (str(upload_backup) if upload_backup else "UNCHANGED"))
        print("UPLOAD_BYTES_TRANSFERRED=false" if args.mode == "probe" else "UPLOAD_LIVE_TEST_PENDING=true")
    except Exception as exc:
        failure = str(exc) if isinstance(exc, Blocked) else type(exc).__name__
        failures = []
        if rollout_started and candidate and (candidate / "rollout-mcp.json").exists():
            try:
                call_module(mcp_source, "scripts.r4a_rollout_mcp_runtime", *rollback_args, "--rollback")
            except Exception as rollback_error:
                failures.append("MCP:" + type(rollback_error).__name__)
        if upload_backup:
            try:
                call_module(gateway_source, "ops.apisix.deploy_managed_file_upload",
                            "--restore", upload_backup, "--admin-key", ADMIN_KEY,
                            "--backup-dir", ROOT)
            except Exception as rollback_error:
                failures.append("GATEWAY:" + type(rollback_error).__name__)
        print("MANAGED_ATTACHMENT_ROLLOUT=BLOCKED CODE=" + failure)
        print("ROLLBACK=" + ("FAILED:" + ",".join(failures) if failures else "COMPLETE_OR_NOT_NEEDED"))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

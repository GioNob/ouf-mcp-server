#!/usr/bin/env python3
"""Rollback-backed lab rollout; direct MCP fetch remains PET-blocked."""

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

GATEWAY_COMMIT = "50eaffae9ce16064ca0c1d69aa13ea50169b772f"
PREVIOUS_GATEWAY_COMMIT = "f5d7b0d5580ad1c602d436035c3b9dd7cfec14dd"
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


def picker_state(gateway_source, materialization, picker_url, onboarding_revision):
    runtime = json.loads((materialization / "runtime.json").read_text())
    public = runtime["x-ouf-installation"]["publicApiBaseUrl"]
    if picker_url != public.rstrip("/") + "/trusted-human/managed-files/":
        raise Blocked("PICKER_PUBLIC_ORIGIN_MISMATCH")
    live = json.loads(command(["docker", "inspect", "ouf-onboarding"]))[0]
    env = dict(entry.partition("=")[::2] for entry in live["Config"]["Env"])
    overlay = json.loads(env.get("SPRING_APPLICATION_JSON", "{}"))
    scopes = overlay.get("spring", {}).get("security", {}).get("oauth2", {}).get("client", {}).get("registration", {}).get("ouf-ths", {}).get("scope", [])
    url = overlay.get("ouf", {}).get("managed-file-picker", {}).get("gateway-upload-url")
    if (not live["State"]["Running"] or
            live["Config"].get("Labels", {}).get("org.opencontainers.image.revision") != onboarding_revision or
            url != "http://ouf-apisix:9080/api/managed-sources/v1/files" or
            "ouf.managed-source.file.upload" not in scopes or "openid" not in scopes):
        raise Blocked("ONBOARDING_PICKER_RUNTIME_NOT_READY")
    output = materialization / "picker-route.json"
    call_module(gateway_source, "tools.materialize_managed_file_ths",
                "--runtime", materialization / "runtime.json", "--output", output)
    if str(gateway_source) not in sys.path:
        sys.path.insert(0, str(gateway_source))
    from ops.apisix.deploy_internal_m2m_routes import route_value
    from ops.apisix.deploy_managed_file_ths import select, select_login
    doc = json.loads(output.read_text())
    expected = select(doc)
    login = select_login(doc)
    admin = route_admin(gateway_source)
    try:
        picker_code, current = admin.route("GET", expected["id"])
        if picker_code not in (200, 404):
            raise Blocked("PICKER_ROUTE_QUERY_FAILED")
        if picker_code == 200 and any(route_value(current).get(k) != v for k, v in expected.items()):
            raise Blocked("PICKER_ROUTE_DRIFT")
        login_code, current = admin.route("GET", login["id"])
        if login_code not in (200, 404):
            raise Blocked("PICKER_LOGIN_ROUTE_QUERY_FAILED")
        if login_code == 200 and any(route_value(current).get(k) != v for k, v in login.items()):
            raise Blocked("PICKER_LOGIN_ROUTE_DRIFT")
        code, current = admin.route("GET", "trusted-human-managed-file-upload")
        route = route_value(current) if code == 200 else {}
        if (route.get("plugins", {}).get("proxy-control") != {"request_buffering": False} or
                route.get("plugins", {}).get("openid-connect", {}).get("required_scopes") != ["ouf.managed-source.file.upload"] or
                route.get("upstream", {}).get("nodes") != {"ouf-onboarding:8080": 1}):
            raise Blocked("HUMAN_UPLOAD_ROUTE_NOT_STREAMING")
        return picker_code == 200 and login_code == 200
    finally:
        admin.close()


def mcp_args(snapshot, candidate, tag, image_id, backup_name, mode, origin, picker_url=None):
    arguments = ["--snapshot", snapshot, "--candidate", candidate,
                 "--image", tag, "--image-id", image_id, "--backup-name", backup_name,
                 "--upload-mode", mode]
    if origin:
        arguments += ["--host-origin", origin]
    if picker_url:
        arguments += ["--picker-url", picker_url]
    return arguments


def save_state(path, state):
    if path.exists():
        if path.is_symlink() or path.stat().st_mode & 0o777 != 0o600:
            raise Blocked("ROLLOUT_STATE_UNSAFE")
        fd = os.open(path, os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW)
    else:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(state, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def verify_state_file(path):
    return (path.parent.parent == ROOT and path.name == "picker-rollout-state.json" and
            not path.is_symlink() and path.stat().st_uid == 0 and
            path.stat().st_mode & 0o777 == 0o600)


def rollback_saved(path, gateway_repo, mcp_repo):
    if not verify_state_file(path):
        raise Blocked("ROLLOUT_STATE_UNSAFE")
    state = json.loads(path.read_text())
    if (state.get("mode") != "picker" or state.get("phase") not in ("prepared", "active", "mcp_rolled_back") or
            not re.fullmatch(r"[0-9a-f]{40}", state.get("mcp_commit", "")) or
            state.get("gateway_commit") not in (GATEWAY_COMMIT, PREVIOUS_GATEWAY_COMMIT)):
        raise Blocked("ROLLOUT_STATE_MISMATCH")
    folder = Path(tempfile.mkdtemp(prefix="r4a-picker-rollback-", dir=ROOT))
    gateway_source, mcp_source = folder / "gateway", folder / "mcp"
    gateway_source.mkdir(mode=0o700)
    mcp_source.mkdir(mode=0o700)
    archive(gateway_repo, state["gateway_commit"], gateway_source)
    archive(mcp_repo, state["mcp_commit"], mcp_source)
    if state["phase"] in ("prepared", "active") and (
            state["phase"] == "active" or (Path(state["candidate"]) / "rollout-mcp.json").exists()):
        args = mcp_args(Path(state["snapshot"]), Path(state["candidate"]), state["tag"],
                        state["image_id"], state["backup_name"], "picker", None, state["picker_url"])
        call_module(mcp_source, "scripts.r4a_rollout_mcp_runtime", *args, "--rollback")
        state["phase"] = "mcp_rolled_back"
        save_state(path, state)
    if state.get("picker_backup"):
        call_module(gateway_source, "ops.apisix.deploy_managed_file_ths",
                    "--restore", Path(state["picker_backup"]), "--admin-key", ADMIN_KEY,
                    "--backup-dir", ROOT)
    state["phase"] = "rolled_back"
    save_state(path, state)
    print("PICKER_MCP_GATEWAY_ROLLBACK=PASS")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mcp-commit")
    parser.add_argument("--snapshot", type=Path,
                        help="Optional existing private snapshot; default creates one automatically")
    parser.add_argument("--materialization", type=Path)
    parser.add_argument("--mode", choices=("probe", "enabled", "picker"))
    parser.add_argument("--rollback-state", type=Path)
    parser.add_argument("--host-origin")
    parser.add_argument("--picker-url")
    parser.add_argument("--onboarding-revision")
    parser.add_argument("--gateway-repo", type=Path, default=Path("/opt/ouf/gateway"))
    parser.add_argument("--mcp-repo", type=Path, default=Path("/opt/ouf/mcp"))
    args = parser.parse_args()
    if args.rollback_state:
        if args.mode or args.mcp_commit or args.materialization or args.host_origin or args.picker_url or args.onboarding_revision:
            parser.error("rollback accepts only the saved state and repository paths")
        try:
            rollback_saved(args.rollback_state, args.gateway_repo, args.mcp_repo)
        except (Blocked, OSError, ValueError, KeyError, TypeError) as exc:
            print("PICKER_ROLLBACK_BLOCKED=" + (str(exc) if isinstance(exc, Blocked) else type(exc).__name__))
            raise SystemExit(1) from None
        return
    if not args.mode or not args.mcp_commit or not args.materialization:
        parser.error("mode, pinned MCP commit and materialization are required")
    candidate = None
    upload_backup = None
    picker_backup = None
    rollout_started = False
    gateway_source = None
    mcp_source = None
    rollback_args = None
    state_path = None
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
        if args.mode == "picker":
            if not args.picker_url or not args.onboarding_revision or not re.fullmatch(r"[0-9a-f]{40}", args.onboarding_revision):
                raise Blocked("PICKER_PARAMETERS_REQUIRED")
        elif args.picker_url or args.onboarding_revision:
            raise Blocked("PICKER_PARAMETERS_UNEXPECTED")
        if (not re.fullmatch(r"[0-9a-f]{40}", args.mcp_commit)
                or command(git_args(args.gateway_repo, "rev-parse", GATEWAY_COMMIT + "^{commit}")).strip() != GATEWAY_COMMIT):
            raise Blocked("PINNED_SOURCE_MISSING")
        folder = Path(tempfile.mkdtemp(prefix="r4a-managed-attachment-", dir=ROOT))
        gateway_source, mcp_source = folder / "gateway", folder / "mcp"
        gateway_source.mkdir(mode=0o700)
        mcp_source.mkdir(mode=0o700)
        archive(args.gateway_repo, GATEWAY_COMMIT, gateway_source)
        archive(args.mcp_repo, args.mcp_commit, mcp_source)
        picker_installed = (picker_state(gateway_source, args.materialization,
                                          args.picker_url, args.onboarding_revision)
                            if args.mode == "picker" else False)
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
        if args.picker_url:
            prep_args += ["--picker-url", args.picker_url]
        output = call_module(mcp_source, "scripts.r4a_prepare_mcp_candidate", *prep_args)
        candidate = marked_path(output, "PRIVATE_MCP_CANDIDATE")
        backup_name = "ouf-mcp-r4a-rollback-" + args.mcp_commit[:7] + "-" + args.mode
        rollback_args = mcp_args(args.snapshot, candidate, tag, image_id, backup_name, args.mode,
                                 args.host_origin, args.picker_url)
        call_module(mcp_source, "scripts.r4a_rollout_mcp_runtime", *rollback_args)
        if not already_installed and args.mode == "probe":
            output = call_module(gateway_source, "ops.apisix.deploy_managed_file_upload",
                                 "--materialization", args.materialization / "upload-route.json",
                                 "--admin-key", ADMIN_KEY, "--backup-dir", ROOT)
            upload_backup = marked_path(output, "BACKUP")
        if args.mode == "picker" and not picker_installed:
            output = call_module(gateway_source, "ops.apisix.deploy_managed_file_ths",
                                 "--materialization", args.materialization / "picker-route.json",
                                 "--admin-key", ADMIN_KEY, "--backup-dir", ROOT)
            picker_backup = marked_path(output, "BACKUP")
        if args.mode == "picker":
            state_path = folder / "picker-rollout-state.json"
            save_state(state_path, {
                "phase": "prepared", "mode": "picker", "gateway_commit": GATEWAY_COMMIT,
                "mcp_commit": args.mcp_commit, "picker_url": args.picker_url,
                "snapshot": str(args.snapshot), "candidate": str(candidate),
                "tag": tag, "image_id": image_id, "backup_name": backup_name,
                "picker_backup": str(picker_backup) if picker_backup else None,
            })
        rollout_started = True
        call_module(mcp_source, "scripts.r4a_rollout_mcp_runtime", *rollback_args, "--apply")
        current = json.loads(command(["docker", "inspect", "ouf-mcp"]))[0]
        if (current["Image"] != image_id or not current["State"]["Running"]
                or "MCP_MANAGED_UPLOAD_ENABLED=" + ({"probe": "probe", "picker": "picker"}.get(args.mode, "true"))
                not in current["Config"]["Env"] or
                route_state(gateway_source, args.materialization) != (True if args.mode == "probe" else already_installed) or
                (args.mode == "picker" and not picker_state(gateway_source, args.materialization,
                                                             args.picker_url, args.onboarding_revision))):
            raise Blocked("POST_ROLLOUT_READBACK_FAILED")
        if state_path:
            state = json.loads(state_path.read_text())
            state["phase"] = "active"
            save_state(state_path, state)
        print("MANAGED_ATTACHMENT_ROLLOUT=PASS")
        print("MODE=" + args.mode.upper() + " MCP_IMAGE_ID=" + image_id)
        print("BACKUP_MCP=" + backup_name)
        print("PRIVATE_MCP_DOCKER_SNAPSHOT=" + str(args.snapshot))
        print("BACKUP_GATEWAY_UPLOAD=" + (str(upload_backup) if upload_backup else "UNCHANGED"))
        print("BACKUP_GATEWAY_PICKER=" + (str(picker_backup) if picker_backup else "UNCHANGED"))
        if state_path:
            print("PICKER_ROLLBACK_STATE=" + str(state_path))
        print("UPLOAD_BYTES_TRANSFERRED=false" if args.mode == "probe" else "UPLOAD_LIVE_TEST_PENDING=true")
    except BaseException as exc:
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
        if picker_backup:
            try:
                call_module(gateway_source, "ops.apisix.deploy_managed_file_ths",
                            "--restore", picker_backup, "--admin-key", ADMIN_KEY,
                            "--backup-dir", ROOT)
            except Exception as rollback_error:
                failures.append("PICKER:" + type(rollback_error).__name__)
        print("MANAGED_ATTACHMENT_ROLLOUT=BLOCKED CODE=" + failure)
        print("ROLLBACK=" + ("FAILED:" + ",".join(failures) if failures else "COMPLETE_OR_NOT_NEEDED"))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

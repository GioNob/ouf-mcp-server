"""Verify a failed coordinated rollout restores both changed components."""

import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from scripts import r4a_attachment_rollout as rollout


class CoordinatedRollbackTests(unittest.TestCase):
    def test_saved_picker_state_restores_mcp_then_gateway_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / 'rollout'
            folder.mkdir(mode=0o700)
            state_file = folder / 'picker-rollout-state.json'
            state_file.write_text(json.dumps({
                'mode': 'picker', 'phase': 'active', 'gateway_commit': rollout.GATEWAY_COMMIT,
                'mcp_commit': 'a' * 40, 'snapshot': str(root / 'snapshot.json'),
                'candidate': str(root / 'candidate'), 'tag': 'ouf-mcp:r4a-aaaaaaa',
                'image_id': 'sha256:synthetic', 'backup_name': 'ouf-mcp-r4a-rollback-test',
                'picker_url': 'https://api.ouf-lab.it/trusted-human/managed-files/',
                'picker_backup': str(root / 'picker-backup.json'),
            }))
            state_file.chmod(0o600)
            calls = []
            def fake_module(source, module, *args):
                calls.append((module, args))
            with (mock.patch.object(rollout, 'ROOT', root),
                  mock.patch.object(rollout, 'verify_state_file', return_value=True),
                  mock.patch.object(rollout, 'archive'),
                  mock.patch.object(rollout, 'call_module', side_effect=fake_module),
                  contextlib.redirect_stdout(io.StringIO())):
                rollout.rollback_saved(state_file, root, root)
            self.assertEqual(json.loads(state_file.read_text())['phase'], 'rolled_back')
            self.assertTrue(calls[0][0].endswith('rollout_mcp_runtime'))
            self.assertIn('--rollback', calls[0][1])
            self.assertTrue(calls[1][0].endswith('deploy_managed_file_ths'))
            self.assertIn('--restore', calls[1][1])

    def test_prepared_state_without_mcp_swap_restores_only_picker_route(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / 'rollout'
            folder.mkdir(mode=0o700)
            state_file = folder / 'picker-rollout-state.json'
            state_file.write_text(json.dumps({
                'mode': 'picker', 'phase': 'prepared', 'gateway_commit': rollout.GATEWAY_COMMIT,
                'mcp_commit': 'a' * 40, 'candidate': str(root / 'candidate'),
                'picker_backup': str(root / 'picker-backup.json'),
            }))
            state_file.chmod(0o600)
            calls = []
            with (mock.patch.object(rollout, 'ROOT', root),
                  mock.patch.object(rollout, 'verify_state_file', return_value=True),
                  mock.patch.object(rollout, 'archive'),
                  mock.patch.object(rollout, 'call_module', side_effect=lambda _src, name, *args: calls.append(name)),
                  contextlib.redirect_stdout(io.StringIO())):
                rollout.rollback_saved(state_file, root, root)
            self.assertEqual(calls, ['ops.apisix.deploy_managed_file_ths'])
            self.assertEqual(json.loads(state_file.read_text())['phase'], 'rolled_back')

    def test_picker_candidate_failure_restores_only_new_ui_route_and_original_mcp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "admin-key"
            key.write_text("synthetic")
            materialization = root / "routes"
            materialization.mkdir()
            snapshot_dir = root / "snapshot"
            snapshot_dir.mkdir()
            snapshot = snapshot_dir / "container.inspect.json"
            snapshot.write_text(json.dumps([{"Id": "old-container"}]))
            candidate = snapshot_dir / "candidate-mcp-test"
            candidate.mkdir()
            backup = root / "picker-backup.json"
            backup.write_text("{}")
            calls = []

            def fake_command(args, *, env=None):
                if "rev-parse" in args:
                    return rollout.GATEWAY_COMMIT + "\n"
                if args[:2] == ["docker", "inspect"]:
                    return json.dumps([{"Id": "old-container", "State": {"Running": True}}])
                raise AssertionError(args)

            def fake_module(source, module, *args):
                calls.append((module, args))
                if module.endswith("snapshot_mcp_runtime"):
                    return "PRIVATE_MCP_DOCKER_SNAPSHOT=" + str(snapshot)
                if module.endswith("prepare_mcp_candidate"):
                    self.assertIn("--picker-url", args)
                    return "PRIVATE_MCP_CANDIDATE=" + str(candidate)
                if module.endswith("deploy_managed_file_ths") and "--materialization" in args:
                    return "BACKUP=" + str(backup)
                if module.endswith("rollout_mcp_runtime") and "--apply" in args:
                    (candidate / "rollout-mcp.json").write_text("{}")
                    raise rollout.Blocked("CANDIDATE_START_FAILED")
                return "MCP_R4A_DRY_RUN=PASS"

            argv = ["rollout", "--mcp-commit", "a" * 40,
                    "--materialization", str(materialization), "--mode", "picker",
                    "--picker-url", "https://api.ouf-lab.it/trusted-human/managed-files/",
                    "--onboarding-revision", "b" * 40,
                    "--gateway-repo", str(root), "--mcp-repo", str(root)]
            with (mock.patch.object(rollout, "ROOT", root),
                  mock.patch.object(rollout, "ADMIN_KEY", key),
                  mock.patch.object(rollout.os, "geteuid", return_value=0),
                  mock.patch.object(rollout, "private"),
                  mock.patch.object(rollout, "archive"),
                  mock.patch.object(rollout, "command", side_effect=fake_command),
                  mock.patch.object(rollout, "route_state", return_value=True),
                  mock.patch.object(rollout, "picker_state", return_value=False),
                  mock.patch.object(rollout, "image", return_value="sha256:synthetic"),
                  mock.patch.object(rollout, "call_module", side_effect=fake_module),
                  mock.patch.object(sys, "argv", argv),
                  contextlib.redirect_stdout(io.StringIO())):
                with self.assertRaises(SystemExit):
                    rollout.main()
            self.assertTrue(any(name.endswith("deploy_managed_file_ths") and "--restore" in args
                                for name, args in calls))
            self.assertTrue(any(name.endswith("rollout_mcp_runtime") and "--rollback" in args
                                for name, args in calls))

    def test_enabled_direct_fetch_is_pet_blocked_before_any_operation(self):
        output = io.StringIO()
        argv = ["rollout", "--mcp-commit", "a" * 40,
                "--materialization", "/nonexistent", "--mode", "enabled"]
        with (mock.patch.object(sys, "argv", argv),
              mock.patch.object(rollout, "command", side_effect=AssertionError("no commands allowed")),
              contextlib.redirect_stdout(output)):
            with self.assertRaises(SystemExit) as status:
                rollout.main()
        self.assertEqual(status.exception.code, 1)
        self.assertIn("CODE=PET_ATTACHMENT_BOUNDARY_UNRESOLVED", output.getvalue())

    def test_candidate_failure_restores_gateway_and_original_mcp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "admin-key"
            key.write_text("synthetic")
            materialization = root / "routes"
            materialization.mkdir()
            snapshot_dir = root / "snapshot"
            snapshot_dir.mkdir()
            snapshot = snapshot_dir / "container.inspect.json"
            snapshot.write_text(json.dumps([{"Id": "old-container"}]))
            candidate = snapshot_dir / "candidate-mcp-test"
            candidate.mkdir()
            backup = root / "route-backup.json"
            backup.write_text("{}")
            calls = []

            def fake_command(args, *, env=None):
                if "rev-parse" in args:
                    return rollout.GATEWAY_COMMIT + "\n"
                if args[:2] == ["docker", "inspect"]:
                    return json.dumps([{"Id": "old-container", "State": {"Running": True}}])
                raise AssertionError(args)

            def fake_module(source, module, *args):
                calls.append((module, args))
                if module.endswith("snapshot_mcp_runtime"):
                    return "PRIVATE_MCP_DOCKER_SNAPSHOT=" + str(snapshot)
                if module.endswith("prepare_mcp_candidate"):
                    return "PRIVATE_MCP_CANDIDATE=" + str(candidate)
                if module.endswith("deploy_managed_file_upload"):
                    return "BACKUP=" + str(backup)
                if module.endswith("rollout_mcp_runtime") and "--apply" in args:
                    (candidate / "rollout-mcp.json").write_text("{}")
                    raise rollout.Blocked("CANDIDATE_START_FAILED")
                return "MCP_R4A_DRY_RUN=PASS"

            argv = ["rollout", "--mcp-commit", "a" * 40,
                    "--materialization", str(materialization), "--mode", "probe",
                    "--gateway-repo", str(root), "--mcp-repo", str(root)]
            output = io.StringIO()
            with (mock.patch.object(rollout, "ROOT", root),
                  mock.patch.object(rollout, "ADMIN_KEY", key),
                  mock.patch.object(rollout.os, "geteuid", return_value=0),
                  mock.patch.object(rollout, "private"),
                  mock.patch.object(rollout, "archive"),
                  mock.patch.object(rollout, "command", side_effect=fake_command),
                  mock.patch.object(rollout, "route_state", return_value=False),
                  mock.patch.object(rollout, "image", return_value="sha256:synthetic"),
                  mock.patch.object(rollout, "call_module", side_effect=fake_module),
                  mock.patch.object(sys, "argv", argv),
                  contextlib.redirect_stdout(output)):
                with self.assertRaises(SystemExit) as exit_status:
                    rollout.main()
            self.assertEqual(exit_status.exception.code, 1)
            self.assertIn("MANAGED_ATTACHMENT_ROLLOUT=BLOCKED CODE=CANDIDATE_START_FAILED", output.getvalue())
            self.assertIn("ROLLBACK=COMPLETE_OR_NOT_NEEDED", output.getvalue())
            self.assertTrue(any(name.endswith("rollout_mcp_runtime") and "--rollback" in args
                                for name, args in calls))
            self.assertTrue(any(name.endswith("deploy_managed_file_upload") and "--restore" in args
                                for name, args in calls))


if __name__ == "__main__":
    unittest.main()

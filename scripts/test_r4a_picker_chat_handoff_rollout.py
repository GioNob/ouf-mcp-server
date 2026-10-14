import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import r4a_picker_chat_handoff_rollout as rollout


class HandoffRolloutTest(unittest.TestCase):
    def test_mcp_failure_restores_routes_and_onboarding_after_own_rollback(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            materialization = root / 'materialization'
            materialization.mkdir()
            (materialization / 'mcp-routes.json').write_bytes(b'previous-routes')
            (materialization / 'runtime.json').write_text('{}')
            admin_key = root / 'admin-key'
            admin_key.write_text('not-a-secret-fixture')
            calls = []

            def gateway(_source, module, *args):
                calls.append((module, args))
                if module == 'tools.materialize_managed_file_mcp':
                    Path(args[-1]).write_bytes(b'new-routes')
                    return ''
                if '--restore' in args:
                    return 'MANAGED_FILE_MCP_RESTORED\n'
                return 'BACKUP=' + str(root / 'backup' / 'previous.json') + '\nMANAGED_FILE_MCP_ACTIVE\n'

            def script(_repo, _commit, path, *args):
                calls.append(path + ':' + str(args[0]))
                if path.endswith('r4a_attachment_rollout.py'):
                    raise rollout.StepFailed('PYTHON3', 'MANAGED_ATTACHMENT_ROLLOUT=BLOCKED ROLLBACK=COMPLETE_OR_NOT_NEEDED')
                if args[0] == 'upgrade':
                    return 'PICKER_CHAT_HANDOFF_UPGRADE=PASS\nROLLBACK_STATE=' + str(root / 'state.json') + '\n'
                return 'PICKER_ONBOARDING_ROLLBACK=PASS\n'

            with (mock.patch.object(rollout, 'ROOT', root),
                  mock.patch.object(rollout, 'MATERIALIZATION', materialization),
                  mock.patch.object(rollout, 'ADMIN_KEY', admin_key),
                  mock.patch.object(rollout.os, 'geteuid', return_value=0),
                  mock.patch.object(rollout, 'pinned'),
                  mock.patch.object(rollout, 'gateway_source'),
                  mock.patch.object(rollout, 'gateway_module', side_effect=gateway),
                  mock.patch.object(rollout, 'run_script', side_effect=script),
                  mock.patch.object(sys, 'argv', ['rollout', '--mcp-commit', 'a' * 40])):
                with self.assertRaises(rollout.StepFailed):
                    rollout.main()
            self.assertEqual((materialization / 'mcp-routes.json').read_bytes(), b'previous-routes')
            self.assertIn(('ops.apisix.deploy_managed_file_mcp',
                           ('--restore', root / 'backup' / 'previous.json',
                            '--admin-key', admin_key, '--backup-dir', root)), calls)
            self.assertEqual(calls[-1], 'scripts/r4a_picker_onboarding_rollout.py:rollback')


if __name__ == '__main__':
    unittest.main()

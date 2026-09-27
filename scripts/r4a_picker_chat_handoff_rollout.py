#!/usr/bin/env python3
"""Upgrade the live first-party picker handoff in one rollback-backed lab run.

The source commits must already be fetched by the operator. No secret values,
Docker environment or CSV bytes are printed. This keeps the current upload
path and policy bundle; only a short-lived result pointer is added.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path('/etc/ouf/deploy-snapshots')
ONBOARDING = '4b552ffb1a685a38f1387a20e655da9e4d0500a4'
GATEWAY = 'a226090e7cf1b5e411e9fa422664fa9b4b05746f'
PICKER = 'https://api.ouf-lab.it/trusted-human/managed-files/'
MATERIALIZATION = ROOT / 'r4a-mcp-routes-vqa3yS'
ADMIN_KEY = Path('/opt/ouf/secrets/apisix-admin-key')


class StepFailed(RuntimeError):
    def __init__(self, step, output):
        super().__init__('STEP_FAILED_' + step)
        self.output = output


def command(args, *, cwd=None, env=None, input=None):
    r = subprocess.run(args, cwd=cwd, env=env, input=input, text=True,
                       capture_output=True, check=False)
    if r.returncode:
        raise StepFailed(Path(args[0]).name.upper().replace('-', '_'), r.stdout)
    return r.stdout


def git(repo, *args):
    return command(['git', '-c', 'safe.directory=' + str(repo.resolve()), '-C', str(repo), *args])


def pinned(repo, commit):
    if git(repo, 'rev-parse', commit + '^{commit}').strip() != commit:
        raise RuntimeError('PINNED_SOURCE_UNAVAILABLE')


def marker(output, name):
    match = re.search(r'^' + re.escape(name) + r'=(\S+)$', output, re.M)
    if not match:
        raise RuntimeError('MISSING_' + name)
    return Path(match.group(1))


def run_script(repo, commit, path, *args):
    source = git(repo, 'show', commit + ':' + path)
    return command([sys.executable, '-', *map(str, args)], input=source)


def gateway_source(repo, destination):
    stream = subprocess.Popen(['git', '-c', 'safe.directory=' + str(repo.resolve()), '-C',
                               str(repo), 'archive', '--format=tar', GATEWAY], stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL)
    try:
        r = subprocess.run(['tar', '-xf', '-', '-C', str(destination)], stdin=stream.stdout,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    finally:
        stream.stdout.close()
    if stream.wait() or r.returncode:
        raise RuntimeError('GATEWAY_ARCHIVE_FAILED')


def gateway_module(source, module, *args):
    env = {**os.environ, 'PYTHONPATH': str(source)}
    return command([sys.executable, '-P', '-m', module, *map(str, args)], env=env)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mcp-commit', required=True)
    p.add_argument('--onboarding-commit', default=ONBOARDING)
    p.add_argument('--onboarding-repo', type=Path, default=Path('/opt/ouf/onboarding'))
    p.add_argument('--gateway-repo', type=Path, default=Path('/opt/ouf/gateway'))
    p.add_argument('--mcp-repo', type=Path, default=Path('/opt/ouf/mcp'))
    args = p.parse_args()
    if os.geteuid() != 0 or not all(re.fullmatch(r'[0-9a-f]{40}', value)
                                      for value in (args.mcp_commit, args.onboarding_commit)):
        p.error('root and exact MCP/Onboarding commits required')
    for repo, commit in ((args.onboarding_repo, args.onboarding_commit),
                         (args.gateway_repo, GATEWAY), (args.mcp_repo, args.mcp_commit)):
        pinned(repo, commit)
    if not MATERIALIZATION.is_dir() or not ADMIN_KEY.is_file():
        raise RuntimeError('LAB_MATERIALIZATION_MISSING')
    with tempfile.TemporaryDirectory(prefix='r4a-chat-handoff-', dir=ROOT) as work:
        folder = Path(work)
        source = folder / 'gateway'
        source.mkdir(mode=0o700)
        gateway_source(args.gateway_repo, source)
        new_routes = folder / 'mcp-routes.json'
        gateway_module(source, 'tools.materialize_managed_file_mcp', '--runtime',
                       MATERIALIZATION / 'runtime.json', '--oidc-client-secret-ref',
                       '$ENV://OUF_GATEWAY_OIDC_CLIENT_SECRET', '--delegation-key-env',
                       'OUF_GATEWAY_DELEGATION_KEY', '--owner-key-env',
                       'OUF_AUTHORIZATION_OWNER_KEY', '--output', new_routes)
        old_routes = (MATERIALIZATION / 'mcp-routes.json').read_bytes()
        onboarding_state = None
        route_backup = None
        mcp_state = None
        mcp_rollback_uncertain = False
        try:
            result = run_script(args.onboarding_repo, args.onboarding_commit,
                                'scripts/r4a_picker_onboarding_rollout.py', 'upgrade',
                                '--revision', args.onboarding_commit, '--repo', args.onboarding_repo)
            onboarding_state = marker(result, 'ROLLBACK_STATE')
            if 'PICKER_CHAT_HANDOFF_UPGRADE=PASS' not in result:
                raise RuntimeError('ONBOARDING_UPGRADE_UNVERIFIED')
            result = gateway_module(source, 'ops.apisix.deploy_managed_file_mcp',
                                    '--materialization', new_routes, '--admin-key', ADMIN_KEY,
                                    '--backup-dir', ROOT)
            route_backup = marker(result, 'BACKUP')
            if 'MANAGED_FILE_MCP_ACTIVE' not in result:
                raise RuntimeError('HANDOFF_ROUTE_UNVERIFIED')
            (MATERIALIZATION / 'mcp-routes.json').write_bytes(new_routes.read_bytes())
            try:
                result = run_script(args.mcp_repo, args.mcp_commit,
                                    'scripts/r4a_attachment_rollout.py', '--mcp-commit',
                                    args.mcp_commit, '--mode', 'picker', '--picker-url', PICKER,
                                    '--onboarding-revision', args.onboarding_commit,
                                    '--materialization', MATERIALIZATION)
            except StepFailed as exc:
                mcp_rollback_uncertain = 'ROLLBACK=COMPLETE_OR_NOT_NEEDED' not in exc.output
                raise
            if 'MANAGED_ATTACHMENT_ROLLOUT=PASS' not in result:
                raise RuntimeError('MCP_ROLLOUT_UNVERIFIED')
            mcp_state = marker(result, 'PICKER_ROLLBACK_STATE')
            print('PICKER_CHAT_HANDOFF_ROLLOUT=PASS')
            print('ONBOARDING_ROLLBACK_STATE=' + str(onboarding_state))
            print('GATEWAY_ROLLBACK_SNAPSHOT=' + str(route_backup))
            print('MCP_ROLLBACK_STATE=' + str(mcp_state))
            print('LIVE_CHAT_WIDGET_TEST_PENDING=true')
        except BaseException:
            if mcp_rollback_uncertain:
                raise RuntimeError('MCP_ROLLBACK_UNVERIFIED; prior snapshots retained')
            if mcp_state:
                run_script(args.mcp_repo, args.mcp_commit,
                           'scripts/r4a_attachment_rollout.py', '--rollback-state', mcp_state)
            (MATERIALIZATION / 'mcp-routes.json').write_bytes(old_routes)
            if route_backup:
                gateway_module(source, 'ops.apisix.deploy_managed_file_mcp',
                               '--restore', route_backup, '--admin-key', ADMIN_KEY,
                               '--backup-dir', ROOT)
            if onboarding_state:
                run_script(args.onboarding_repo, args.onboarding_commit,
                           'scripts/r4a_picker_onboarding_rollout.py', 'rollback',
                           '--state', onboarding_state)
            raise


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('PICKER_CHAT_HANDOFF_ROLLOUT=BLOCKED CODE=' + type(exc).__name__ +
              ' DETAIL=' + str(exc)[:200], file=sys.stderr)
        raise SystemExit(1) from None

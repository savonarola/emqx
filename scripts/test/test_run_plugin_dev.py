#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class PluginDevTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / 'scripts'
        scripts.mkdir()
        source = Path(__file__).resolve().parents[1] / 'run-plugin-dev.sh'
        shutil.copy(source, scripts / source.name)
        self.release = self.root / '_build/emqx-enterprise/rel/emqx'
        (self.release / 'bin').mkdir(parents=True)
        self.name = 'emqx_demo-0.1.0'
        packages = self.root / '_build/plugins'
        packages.mkdir(parents=True)
        self.package = packages / (self.name + '.tar.gz')
        self.package.write_bytes(b'package bytes used for the digest check')
        fake_bin = self.root / 'fake-bin'
        fake_bin.mkdir()
        make = fake_bin / 'make'
        make.write_text('#!/bin/sh\nexit 0\n')
        make.chmod(0o755)
        emqx = self.release / 'bin/emqx'
        emqx.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
root = Path(__file__).resolve().parents[1]
args = sys.argv[1:]
if args[0] == 'ping':
    pass
elif args[0] == 'eval':
    print(root if 'code:root_dir' in args[1] else 'plugins')
else:
    action = args[2]
    with (root / 'commands.jsonl').open('a') as log:
        log.write(json.dumps(args[2:]) + '\\n')
    print(json.dumps({'result': 'not_ok' if action == os.getenv('FAIL_ACTION') else 'ok'}))
''')
        emqx.chmod(0o755)
        self.env = {**os.environ, 'PROFILE': 'emqx-enterprise',
                    'PATH': str(fake_bin) + os.pathsep + os.environ['PATH']}

    def run_script(self, fail_action=''):
        result = subprocess.run(
            ['bash', str(self.root / 'scripts/run-plugin-dev.sh'), 'emqx_demo'],
            env={**self.env, 'FAIL_ACTION': fail_action}, capture_output=True, text=True,
        )
        commands = [json.loads(line) for line in
                    (self.release / 'commands.jsonl').read_text().splitlines()]
        return result, commands

    def test_hash_grant_precedes_install(self):
        result, commands = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        digest = hashlib.sha256(self.package.read_bytes()).hexdigest()
        self.assertEqual(commands[-4:], [
            ['allow', self.name, 'sha256:' + digest],
            ['install', self.name], ['enable', self.name], ['start', self.name],
        ])
        self.assertEqual((self.release / 'plugins' / self.package.name).read_bytes(),
                         self.package.read_bytes())

    def test_not_ok_stops_even_with_zero_cli_exit(self):
        for action in ['allow', 'install', 'enable', 'start']:
            with self.subTest(action=action):
                result, commands = self.run_script(action)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(commands[-1][0], action)
                self.assertIn('Plugin command failed: ' + action, result.stderr)
                self.assertNotIn('Plugin ready:', result.stdout)


if __name__ == '__main__':
    unittest.main()

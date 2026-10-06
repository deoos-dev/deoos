"""macOS ARM64 filesystem fault qualification, with no production fault hooks.

Compile a test-only DYLD interposer, then launch the supplied engine binary in
owned temporary directories. Exercise a failed post-rename directory fsync and
SIGKILL immediately before/after a state-envelope rename. This proves response
and process-recovery behavior; it is not a physical power-loss test.

Usage: python3 tests/local_faults.py --binary engine/target/debug/deoos-engine
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import platform
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]


def request(url, route, body=None, timeout=15):
    data = None if body is None else json.dumps({'protocol_version': 3, **body}).encode()
    req = urllib.request.Request(url + route, data=data,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status, raw = response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read().decode()
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = raw
    return status, result


def successful(url, route, body=None):
    status, result = request(url, route, body)
    assert status == 200, (route, status, result)
    return result


class Server:
    def __init__(self, binary, env, log):
        with socket.socket() as candidate:
            candidate.bind(('127.0.0.1', 0))
            port = candidate.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}'
        self.log_path = log
        self.log = log.open('w')
        self.process = subprocess.Popen([str(binary)],
                                        env=dict(env, ENGINE_BIND=f'127.0.0.1:{port}'),
                                        stdout=self.log, stderr=self.log)
        try:
            deadline = time.monotonic() + 30
            while True:
                if self.process.poll() is not None:
                    raise AssertionError(f'engine startup failed: {log.read_text()}')
                try:
                    if request(self.url, '/health', timeout=.2) == (200, 'ok'):
                        break
                except (OSError, urllib.error.URLError):
                    pass
                assert time.monotonic() < deadline, 'engine did not become ready'
                time.sleep(.02)
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.log.close()


def wait_marker(marker, server):
    deadline = time.monotonic() + 20
    while not marker.exists() or not marker.read_text():
        assert server.process.poll() is None, server.log_path.read_text()
        assert time.monotonic() < deadline, 'interposer did not reach the armed boundary'
        time.sleep(.01)
    return marker.read_text()


def case(binary, library, work, mode, inherited):
    work.mkdir()
    storage = work / 'storage'
    prefix = 'fault-' + uuid.uuid4().hex
    key = f'{prefix}/tasks/recovery/state.json'
    target = storage / 'objects' / hashlib.sha256(key.encode()).hexdigest()
    arm, marker = work / 'arm', work / 'marker'
    env = {key: value for key, value in inherited.items()
           if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_'))
           and key not in {'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE'}}
    env.update(DEOOS_STORAGE_PROVIDER='filesystem',
               DEOOS_STORAGE_DIRECTORY=str(storage), EXECUTION_PREFIX=prefix,
               LEASE_MS='1500')
    fault_env = dict(env, DYLD_INSERT_LIBRARIES=str(library),
                     DEOOS_FAULT_TARGET=str(target), DEOOS_FAULT_ARM=str(arm),
                     DEOOS_FAULT_MARKER=str(marker), DEOOS_FAULT_MODE=mode)
    server = Server(binary, fault_env, work / 'fault-server.log')
    claim = {'worker': 'interrupted-owner', 'handlers': ['recovery.v1']}
    result = {'mode': mode}
    try:
        before = successful(server.url, '/tasks', {'id': 'recovery',
                            'handler': 'recovery.v1', 'inputs': {'sentinel': [1, 2, 3]},
                            'max_attempts': 3})
        arm.write_text('armed')
        if mode == 'fail-sync':
            status, response = request(server.url, '/claim', claim)
            assert status == 507, (status, response)
            assert 'durability' in response and 'uncertain' in response, response
            assert wait_marker(marker, server) == 'after-rename-directory-fsync-EIO'
            result['http_status'] = status
            result['reconciliation_did_not_mask_failure'] = True
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(request, server.url, '/claim', claim)
                assert wait_marker(marker, server) == mode
                server.process.kill()
                server.process.wait(timeout=10)
                assert server.process.returncode == -signal.SIGKILL
                try:
                    reply = pending.result(timeout=10)
                except (OSError, urllib.error.URLError):
                    pass
                else:
                    raise AssertionError(f'killed write unexpectedly returned: {reply}')
                result['killed_at'] = mode
    finally:
        server.close()

    # A fresh engine process has neither the interposer nor in-memory ownership.
    # Its read must stabilize the visible envelope before reporting success.
    reader = Server(binary, env, work / 'recovery-server.log')
    try:
        observed = successful(reader.url, '/tasks/recovery')
        assert observed['inputs'] == before['inputs'], observed
        assert observed['handler'] == before['handler'], observed
        if mode == 'before-rename':
            assert observed == before, (before, observed)
            expected_attempts = 1
            old_token = None
            result['prior_state_preserved'] = True
        else:
            assert observed['status'] == 'running' and observed['attempts'] == 1, observed
            assert observed['owner'] == 'interrupted-owner' and observed['token'], observed
            assert observed['generation'] == before['generation'] + 1, observed
            old_token = observed['token']
            expected_attempts = 2
            delay = max(0, (observed['expires_at'] - time.time() * 1000) / 1000)
            assert delay < 5, delay
            time.sleep(delay + .05)
            result['whole_published_state_recovered'] = True
        replacement = successful(reader.url, '/claim', {'worker': 'replacement-owner',
                                  'handlers': ['recovery.v1']})['task']
        assert replacement is not None and replacement['attempts'] == expected_attempts, replacement
        assert replacement['owner'] == 'replacement-owner' and replacement['token'] != old_token
        if old_token:
            status, _ = request(reader.url, '/tasks/recovery/complete', {
                'token': old_token, 'operation_id': uuid.uuid4().hex, 'value': 'stale'})
            assert status == 409, status
            result['stale_token_status'] = status
        completed = successful(reader.url, '/tasks/recovery/complete', {
            'token': replacement['token'], 'operation_id': uuid.uuid4().hex,
            'value': {'recovered': True}})
        assert completed['status'] == 'completed' and completed['output'] == {'recovered': True}
        result.update(recovered_status=completed['status'], attempts=completed['attempts'])
    finally:
        reader.close()
    return result


def run_faults(binary, inherited=None):
    assert platform.system() == 'Darwin' and platform.machine() == 'arm64', \
        'fault qualification requires macOS ARM64'
    binary = pathlib.Path(binary).resolve(strict=True)
    inherited = os.environ if inherited is None else inherited
    report = {'scope': 'process crashes and syscall failures; no physical power-loss test',
              'binary': str(binary), 'checks': [], 'cleaned': False}
    with tempfile.TemporaryDirectory(prefix='deoos-local-faults-') as temporary:
        work = pathlib.Path(temporary).resolve()
        library = work / 'local_faults.dylib'
        subprocess.run(['clang', '-dynamiclib', '-Wall', '-Wextra', '-Werror',
                        '-o', str(library), str(ROOT / 'tests/local_faults.c')], check=True)
        for mode in ('before-rename', 'after-rename', 'fail-sync'):
            report['checks'].append(case(binary, library, work / mode, mode, inherited))
    assert not pathlib.Path(temporary).exists(), 'fault-test temporary files survived cleanup'
    report['cleaned'] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--report', type=pathlib.Path,
                        default=ROOT.parent / 'outputs/evidence/local-storage-faults.json')
    args = parser.parse_args()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    try:
        report = run_faults(args.binary)
    except BaseException as error:
        args.report.write_text(json.dumps({'error': f'{type(error).__name__}: {error}'}, indent=2) + '\n')
        raise
    args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

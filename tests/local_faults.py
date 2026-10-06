"""macOS ARM64 filesystem fault qualification, with no production fault hooks.

Compile a test-only DYLD interposer, then launch the supplied engine binary in
owned temporary directories. Exercise a failed post-rename directory fsync and
SIGKILL immediately before/after a state-envelope rename, for both a first task
submission and a claim. This proves response and process-recovery behavior. A persistent warmed reader also receives a failed
barrier for an external writer's newly published version. These are not physical
power-loss tests.

Usage: python3 tests/local_faults.py --binary engine/target/debug/deoos-engine
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import platform
import plistlib
import re
import shutil
import stat
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]


def fault_target(storage, key):
    target = storage / 'objects' / hashlib.sha256(key.encode()).hexdigest()
    # The trailing slash prevents matching any neighboring hash directory.
    return {'DEOOS_FAULT_TARGET': str(target) + '/'}


def assert_layout(storage):
    assert (storage / 'locks').is_dir()
    assert (storage / 'objects').is_dir() and (storage / 'format').is_file()
    assert not (storage / 'objects-v2').exists() and not (storage / 'format-v2').exists()


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


def case(binary, library, work, mode, inherited, new_create=False):
    work.mkdir()
    storage = work / 'storage'
    prefix = 'fault-' + uuid.uuid4().hex
    key = f'{prefix}/tasks/recovery/state.json'
    target_options = fault_target(storage, key)
    arm, marker = work / 'arm', work / 'marker'
    env = {key: value for key, value in inherited.items()
           if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_'))
           and key not in {'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE'}}
    env.update(DEOOS_STORAGE_PROVIDER='filesystem',
               DEOOS_STORAGE_DIRECTORY=str(storage), EXECUTION_PREFIX=prefix,
               LEASE_MS='1500')
    fault_env = dict(env, DYLD_INSERT_LIBRARIES=str(library),
                     **target_options, DEOOS_FAULT_ARM=str(arm),
                     DEOOS_FAULT_MARKER=str(marker), DEOOS_FAULT_MODE=mode)
    server = Server(binary, fault_env, work / 'fault-server.log')
    claim = {'worker': 'interrupted-owner', 'handlers': ['recovery.v1']}
    result = {'mode': 'new-create-' + mode if new_create else mode}
    submission = {'id': 'recovery', 'handler': 'recovery.v1',
                  'inputs': {'sentinel': [1, 2, 3]}, 'max_attempts': 3}
    try:
        assert_layout(storage)
        if new_create:
            assert request(server.url, '/tasks/recovery')[0] == 404
            before = None
        else:
            before = successful(server.url, '/tasks', submission)
        arm.write_text('armed')
        route, body = ('/tasks', submission) if new_create else ('/claim', claim)
        if mode == 'fail-sync':
            status, response = request(server.url, route, body)
            assert status == 507, (status, response)
            assert 'durability' in response and 'uncertain' in response, response
            assert wait_marker(marker, server) == 'after-rename-directory-fsync-EIO'
            result['http_status'] = status
            result['reconciliation_did_not_mask_failure'] = True
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(request, server.url, route, body)
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
        if new_create:
            status, observed = request(reader.url, '/tasks/recovery')
            assert status == (404 if mode == 'before-rename' else 200), (status, observed)
            before = successful(reader.url, '/tasks', submission)
            assert before['inputs'] == submission['inputs'] and before['handler'] == submission['handler']
            assert before['status'] == 'queued' and before['attempts'] == before['generation'] == 0
            assert before['token'] is None and before['owner'] is None and before['output'] is None
            assert before['steps'] == {} and [event['event'] for event in before['history']] == ['submit']
            if status == 200:
                assert observed == before, (observed, before)
            # Replay the uncertain submission without duplicating or replacing
            # its definition. All observations use a fresh, unfaulted engine.
            for _ in range(2):
                assert successful(reader.url, '/tasks', submission) == before
            listed = successful(reader.url, '/tasks')
            assert not listed['truncated'] and listed['tasks'] == [before], listed
            original = successful(reader.url, '/claim', claim)['task']
            assert original is not None and original['attempts'] == 1 and original['token'], original
            old_token = original['token']
            expected_attempts = 2
            delay = max(0, (original['expires_at'] - time.time() * 1000) / 1000)
            assert delay < 5, delay
            time.sleep(delay + .05)
            result.update(initial_recovery_status=status, whole_queued_state=True,
                          identical_submit_replay=True, one_task=True)
        else:
            observed = successful(reader.url, '/tasks/recovery')
            assert observed['inputs'] == before['inputs'], observed
            assert observed['handler'] == before['handler'], observed
        if not new_create and mode == 'before-rename':
            assert observed == before, (before, observed)
            expected_attempts = 1
            old_token = None
            result['prior_state_preserved'] = True
        elif not new_create:
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
        if new_create:
            listed = successful(reader.url, '/tasks')
            assert not listed['truncated'] and listed['tasks'] == [completed], listed
            assert successful(reader.url, '/claim', claim)['task'] is None
            result['one_final_completed_task'] = True
    finally:
        reader.close()
    return result


def warm_reader_case(binary, library, work, inherited):
    """A warmed reader must certify a different ETag after another writer dies."""
    work.mkdir()
    storage = work / 'storage'
    prefix = 'warm-fault-' + uuid.uuid4().hex
    key = f'{prefix}/tasks/recovery/state.json'
    target_options = fault_target(storage, key)
    reader_arm, reader_marker = work / 'reader-arm', work / 'reader-marker'
    writer_arm, writer_marker = work / 'writer-arm', work / 'writer-marker'
    env = {key: value for key, value in inherited.items()
           if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_'))
           and key not in {'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE'}}
    env.update(DEOOS_STORAGE_PROVIDER='filesystem',
               DEOOS_STORAGE_DIRECTORY=str(storage), EXECUTION_PREFIX=prefix,
               LEASE_MS='1500')
    reader_env = dict(env, DYLD_INSERT_LIBRARIES=str(library),
                      **target_options, DEOOS_FAULT_ARM=str(reader_arm),
                      DEOOS_FAULT_MARKER=str(reader_marker), DEOOS_FAULT_MODE='fail-read-sync')
    writer_env = dict(env, DYLD_INSERT_LIBRARIES=str(library),
                      **target_options, DEOOS_FAULT_ARM=str(writer_arm),
                      DEOOS_FAULT_MARKER=str(writer_marker), DEOOS_FAULT_MODE='after-rename')
    reader = Server(binary, reader_env, work / 'warm-reader.log')
    writer = None
    result = {'mode': 'warm-reader-new-version', 'same_reader_process': True}
    try:
        assert_layout(storage)
        before = successful(reader.url, '/tasks', {
            'id': 'recovery', 'handler': 'recovery.v1',
            'inputs': {'sentinel': [1, 2, 3]}, 'max_attempts': 3})
        # A certifies the original state before an independent B publishes a new
        # version. These requests and the subsequent recovery all use the same A.
        for _ in range(2):
            assert successful(reader.url, '/tasks/recovery') == before
        reader_pid = reader.process.pid
        writer = Server(binary, writer_env, work / 'interrupted-writer.log')
        assert writer.process.pid != reader_pid
        writer_arm.write_text('armed')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(request, writer.url, '/claim', {
                'worker': 'interrupted-owner', 'handlers': ['recovery.v1']})
            assert wait_marker(writer_marker, writer) == 'after-rename'
            writer.process.kill()
            writer.process.wait(timeout=10)
            assert writer.process.returncode == -signal.SIGKILL
            try:
                reply = pending.result(timeout=10)
            except (OSError, urllib.error.URLError):
                pass
            else:
                raise AssertionError(f'killed external write unexpectedly returned: {reply}')

        # B died after rename but before its directory barrier. Cached durability
        # of A's prior version cannot certify this newly visible state. Arm only
        # this direct GET; no concurrent A requests or worker loops are present.
        reader_arm.write_text('armed')
        status, response = request(reader.url, '/tasks/recovery')
        assert status == 507, (status, response)
        assert 'durability' in response and 'uncertain' in response, response
        assert wait_marker(reader_marker, reader) == 'read-directory-fsync-EIO'
        assert not reader_arm.exists(), 'read-barrier fault was not consumed'
        assert reader.process.pid == reader_pid and reader.process.poll() is None
        result.update(http_status=status, external_writer_killed_at='after-rename',
                      previous_version_did_not_certify_replacement=True)

        # One-shot failure is gone. The same warmed engine must now stabilize and
        # return the complete external version, then fence its obsolete ownership.
        observed = successful(reader.url, '/tasks/recovery')
        assert observed['inputs'] == before['inputs'] and observed['handler'] == before['handler']
        assert observed['status'] == 'running' and observed['attempts'] == 1, observed
        assert observed['owner'] == 'interrupted-owner' and observed['token'], observed
        assert observed['revision'] != before['revision']
        assert observed['generation'] == before['generation'] + 1
        assert successful(reader.url, '/tasks/recovery') == observed
        delay = max(0, (observed['expires_at'] - time.time() * 1000) / 1000)
        assert delay < 5, delay
        time.sleep(delay + .05)
        replacement = successful(reader.url, '/claim', {
            'worker': 'replacement-owner', 'handlers': ['recovery.v1']})['task']
        assert replacement is not None and replacement['attempts'] == 2, replacement
        assert replacement['owner'] == 'replacement-owner'
        assert replacement['token'] != observed['token']
        status, _ = request(reader.url, '/tasks/recovery/complete', {
            'token': observed['token'], 'operation_id': uuid.uuid4().hex, 'value': 'stale'})
        assert status == 409, status
        completed = successful(reader.url, '/tasks/recovery/complete', {
            'token': replacement['token'], 'operation_id': uuid.uuid4().hex,
            'value': {'recovered': True}})
        assert completed['status'] == 'completed' and completed['output'] == {'recovered': True}
        assert reader.process.pid == reader_pid and reader.process.poll() is None
        result.update(stale_token_status=status, recovered_status=completed['status'],
                      attempts=completed['attempts'], complete_external_state_recovered=True)
    finally:
        if writer is not None:
            writer.close()
        reader.close()
    return result


def initialization_cases(binary, library, work, inherited):
    """Initialize only the current layout; reject unfamiliar data without migration."""
    work.mkdir()
    checks = []
    clean_env = {key: value for key, value in inherited.items()
                 if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_'))
                 and key not in {'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE'}}

    def probe(root, options=None):
        env = dict(clean_env, DEOOS_STORAGE_PROVIDER='filesystem',
                   DEOOS_STORAGE_DIRECTORY=str(root), EXECUTION_PREFIX='init-proof')
        env.update(options or {})
        return subprocess.run([str(binary), '--check-storage'], env=env,
                              capture_output=True, text=True, timeout=30)

    def snapshot(root):
        return {str(path.relative_to(root)): None if path.is_dir() else path.read_bytes().hex()
                for path in root.rglob('*') if path.name != 'bootstrap.lock'}

    def private_file(path, value):
        path.write_bytes(value)
        path.chmod(0o600)

    def initialize_only(root, log_name):
        env = dict(clean_env, DEOOS_STORAGE_PROVIDER='filesystem',
                   DEOOS_STORAGE_DIRECTORY=str(root), EXECUTION_PREFIX='init-proof')
        server = Server(binary, env, work / log_name)
        server.close()

    fresh = work / 'fresh'
    initialize_only(fresh, 'fresh.log')
    assert_layout(fresh)
    assert {path.name for path in fresh.iterdir()} == {'format', 'bootstrap.lock', 'objects', 'locks'}
    before = snapshot(fresh)
    initialize_only(fresh, 'reopen.log')
    assert snapshot(fresh) == before
    checks.append({'mode': 'current-initialization', 'fresh_and_reopen': True, 'exact_four_entries': True})

    for scaffold in ('bootstrap-only', 'objects-only', 'objects-and-locks', 'marker-temp'):
        partial = work / scaffold
        partial.mkdir(mode=0o700)
        private_file(partial / 'bootstrap.lock', b'')
        if scaffold != 'bootstrap-only':
            (partial / 'objects').mkdir(mode=0o700)
        if scaffold in ('objects-and-locks', 'marker-temp'):
            (partial / 'locks').mkdir(mode=0o700)
        if scaffold == 'marker-temp':
            private_file(partial / ('.format-' + str(uuid.uuid4())), b'partial')
        initialize_only(partial, scaffold + '.log')
        assert_layout(partial)
        checks.append({'mode': 'interrupted-' + scaffold, 'recovered': True})

    for kind in ('unsupported-marker', 'nonempty-objects', 'nonempty-locks', 'previous-layout', 'symlink-format', 'symlink-objects'):
        root = work / kind
        root.mkdir(mode=0o700)
        if kind == 'previous-layout':
            private_file(root / 'objects', b'old-layout-guard\n')
            private_file(root / 'format-v2', b'DEOOS-LOCAL-FORMAT-2\n')
            (root / 'objects-v2').mkdir(mode=0o700)
            private_file(root / 'objects-v2' / 'sentinel', b'preserve-old-data')
        else:
            for name in ('objects', 'locks'):
                (root / name).mkdir(mode=0o700)
            if kind.startswith('symlink-'):
                name = kind.removeprefix('symlink-')
                external = work / (kind + '-external')
                if name == 'objects':
                    (root / name).rmdir()
                    external.mkdir(mode=0o700)
                else:
                    private_file(external, b'DEOOS-LOCAL-FORMAT-2\n')
                (root / name).symlink_to(external)
            elif kind == 'unsupported-marker':
                private_file(root / 'format', b'UNSUPPORTED\n')
            else:
                private_file(root / kind.removeprefix('nonempty-') / 'sentinel', b'preserve-data')
        before = snapshot(root)
        response = probe(root)
        assert response.returncode == 1, (kind, response)
        if not kind.startswith('symlink-'):
            assert 'use a fresh directory' in response.stderr, (kind, response.stderr)
        assert snapshot(root) == before, kind
        checks.append({'mode': kind, 'rejected': True, 'data_unchanged': True})

    # Kill or fail the actual format-marker publication, then reopen the same
    # root with a fresh unfaulted process. No task writes or background worker.
    for mode in ('before-rename', 'after-rename', 'fail-sync'):
        root = work / ('init-' + mode)
        root.mkdir(mode=0o700)
        arm, marker = work / (mode + '-arm'), work / (mode + '-marker')
        arm.write_text('armed')
        env = dict(clean_env, DEOOS_STORAGE_PROVIDER='filesystem',
                   DEOOS_STORAGE_DIRECTORY=str(root), EXECUTION_PREFIX='init-proof',
                   DYLD_INSERT_LIBRARIES=str(library), DEOOS_FAULT_TARGET=str(root) + '/',
                   DEOOS_FAULT_ARM=str(arm), DEOOS_FAULT_MARKER=str(marker),
                   DEOOS_FAULT_MODE=mode)
        process = subprocess.Popen([str(binary), '--check-storage'], env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            if mode == 'fail-sync':
                stdout, stderr = process.communicate(timeout=30)
                assert process.returncode == 1 and b'durability' in stderr, (stdout, stderr)
                assert marker.read_text() == 'after-rename-directory-fsync-EIO'
            else:
                deadline = time.monotonic() + 20
                while not marker.exists():
                    assert process.poll() is None, process.communicate()
                    assert time.monotonic() < deadline, 'initialization fault boundary not reached'
                    time.sleep(.01)
                assert marker.read_text() == mode
                process.kill()
                process.communicate(timeout=10)
                assert process.returncode == -signal.SIGKILL
            assert probe(root).returncode == 0
            assert_layout(root)
            checks.append({'mode': 'initialization-' + mode, 'recovered': True})
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)
    return checks


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
        report['checks'].extend(initialization_cases(binary, library, work / 'initialization', inherited))
        for mode in ('before-rename', 'after-rename', 'fail-sync'):
            report['checks'].append(case(binary, library, work / mode, mode, inherited))
        report['checks'].append(warm_reader_case(binary, library, work / 'warm-reader', inherited))
        for mode in ('before-rename', 'after-rename', 'fail-sync'):
            report['checks'].append(case(binary, library, work / ('new-create-' + mode),
                                         mode, inherited, new_create=True))
    assert not pathlib.Path(temporary).exists(), 'fault-test temporary files survived cleanup'
    report['cleaned'] = True
    return report



def run_mounts(binary, report):
    """Real nested APFS mounts; retain all artifacts unless detach is verified."""
    assert platform.system() == 'Darwin' and platform.machine() == 'arm64'
    binary = pathlib.Path(binary).resolve(strict=True)
    evidence = ROOT.parent / 'outputs/evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    # Never use automatic recursive cleanup around a possibly live mount.
    work = pathlib.Path(tempfile.mkdtemp(prefix='deoos-apfs-mounts-', dir=evidence)).resolve()
    os.chmod(work, 0o700)
    result = {'mode': 'nested-apfs-mounts', 'work': str(work), 'checks': [],
              'commands': [], 'cleaned': False, 'retained_artifacts': False,
              'scope': 'Real constructor rejection of pre-existing nested APFS mounts; no live-remount or physical power-loss proof',
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    report['checks'].append(result)
    report['cleaned'] = False
    cases = []

    def command(argv, timeout=30):
        started = time.monotonic()
        entry = {'argv': [str(a) for a in argv]}
        result['commands'].append(entry)
        try:
            completed = subprocess.run(entry['argv'], cwd=work, capture_output=True, timeout=timeout)
        except BaseException as error:
            entry.update(error=f'{type(error).__name__}: {error}', elapsed_seconds=time.monotonic()-started)
            raise
        entry.update(returncode=completed.returncode, elapsed_seconds=time.monotonic()-started,
                     stdout_bytes=len(completed.stdout), stdout_sha256=hashlib.sha256(completed.stdout).hexdigest(),
                     stderr=completed.stderr.decode(errors='replace')[:8192])
        assert completed.returncode == 0, entry
        return completed.stdout

    def info():
        return plistlib.loads(command(['/usr/bin/hdiutil', 'info', '-plist']))

    def attachment(case, snapshot):
        images = [image for image in snapshot.get('images', [])
                  if image.get('image-path') == str(case['image'])]
        assert len(images) <= 1, f'ambiguous owned image attachment: {case["image"]}'
        for image in snapshot.get('images', []):
            for entity in image.get('system-entities', []):
                if entity.get('mount-point') == str(case['mount']) and image not in images:
                    raise AssertionError(f'owned mountpoint belongs to an unexpected image: {case["mount"]}')
        if not images:
            return None
        image = images[0]
        mounted = [entity for entity in image.get('system-entities', []) if entity.get('mount-point')]
        assert all(entity['mount-point'] == str(case['mount']) for entity in mounted), image
        # APFS also has a synthesized whole container disk. Detach only the
        # unique image-owned partition-map device, never guess a disk number.
        whole = [entity['dev-entry'] for entity in image.get('system-entities', [])
                 if re.fullmatch(r'/dev/disk[0-9]+', entity.get('dev-entry', ''))
                 and entity.get('content-hint') in ('GUID_partition_scheme', 'Apple_partition_scheme')]
        assert len(whole) == 1, f'no unambiguous owned whole device: {image}'
        return image, whole[0], mounted

    def unmounted(case, snapshot):
        assert attachment(case, snapshot) is None, f'owned image still attached: {case["image"]}'
        for directory in case['namespaces']:
            path = case['root'] / directory
            if path.exists():
                assert not os.path.ismount(path) and path.stat().st_dev == case['root'].stat().st_dev, path

    def detach_verified(case):
        # Query independently even when attach failed/timed out or its plist
        # could not be parsed. A successful mount may precede either failure.
        snapshot = info()
        found = attachment(case, snapshot)
        if found:
            image, device, _ = found
            case['evidence']['cleanup_attachment'] = image
            device_info = plistlib.loads(command(['/usr/sbin/diskutil', 'info', '-plist', device]))
            assert device_info.get('WholeDisk') is True and device_info.get('DeviceNode') == device, device_info
            # Revalidate ownership immediately before the only detach action.
            latest = attachment(case, info())
            assert latest is not None and latest[1] == device, 'owned attachment changed before detach'
            command(['/usr/bin/hdiutil', 'detach', device])
            case['evidence']['detached_whole_device'] = device
            case['detach_confirmed'] = True
        elif case['attach_started'] and not case.get('detach_confirmed'):
            # A failed/timed-out attachment with no currently visible device
            # could still be settling in a system helper. Retain rather than
            # deleting its image or recursively traversing its mountpoint.
            raise AssertionError(f'attachment outcome is not confirmed; retain {case["image"]}')
        unmounted(case, info())
        case['evidence']['attachment_absence_verified'] = True

    def detach(case):
        # Ambiguity or a failed detach is sticky: never retry an action and
        # subsequently erase the evidence of an uncertain cleanup outcome.
        if case.get('cleanup_uncertain'):
            raise RuntimeError(case['cleanup_uncertain'])
        try:
            detach_verified(case)
        except BaseException as error:
            case['cleanup_uncertain'] = f'{type(error).__name__}: {error}'
            case['evidence']['cleanup_uncertain'] = case['cleanup_uncertain']
            raise

    def private(path):
        observed = path.lstat()
        assert stat.S_ISDIR(observed.st_mode) and observed.st_uid == os.getuid()
        assert stat.S_IMODE(observed.st_mode) == 0o700, (str(path), oct(stat.S_IMODE(observed.st_mode)))
        return {'path': str(path), 'uid': observed.st_uid, 'mode': '0700', 'device': observed.st_dev}

    def probe(root):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('AWS_', 'DEOOS_STORAGE_', 'DEOOS_FAULT_', 'DYLD_'))
               and key not in {'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE', 'LEASE_MS'}}
        env.update(DEOOS_STORAGE_PROVIDER='filesystem', DEOOS_STORAGE_DIRECTORY=str(root),
                   EXECUTION_PREFIX='mount-proof-' + uuid.uuid4().hex)
        started = time.monotonic()
        completed = subprocess.run([str(binary), '--check-storage'], cwd=work, env=env,
                                   capture_output=True, text=True, timeout=30)
        return {'returncode': completed.returncode, 'stdout': completed.stdout,
                'stderr': completed.stderr, 'elapsed_seconds': time.monotonic()-started}

    try:
        baseline = work / 'ordinary'
        baseline.mkdir(mode=0o700)
        result['ordinary_baseline'] = probe(baseline)
        assert result['ordinary_baseline']['returncode'] == 0, result['ordinary_baseline']
        assert_layout(baseline)
        # A successful real filesystem qualification establishes that this
        # host root passes the adapter's local APFS and permission checks.
        for namespace in ('objects', 'locks'):
            namespaces = ('objects', 'locks')
            root = work / (namespace + '-case')
            root.mkdir(mode=0o700)
            image, mount = work / (namespace + '.sparseimage'), root / namespace
            row = {'namespace': namespace, 'image': str(image), 'mountpoint': str(mount)}
            result['checks'].append(row)
            row['initialization'] = probe(root)
            assert row['initialization']['returncode'] == 0, row['initialization']
            assert_layout(root)
            case = dict(root=root, image=image, mount=mount, namespaces=namespaces, attach_started=False, evidence=row)
            cases.append(case)
            try:
                command(['/usr/bin/hdiutil', 'create', '-size', '256m', '-type', 'SPARSE', '-fs', 'APFS',
                         '-volname', 'deoos-guard-' + uuid.uuid4().hex, '-uid', str(os.getuid()),
                         '-gid', str(os.getgid()), '-mode', '0700', image], timeout=60)
                case['attach_started'] = True
                attached = command(['/usr/bin/hdiutil', 'attach', '-plist', '-nobrowse', '-owners', 'on',
                                    '-mountpoint', mount, image], timeout=60)
                row['attach_plist'] = plistlib.loads(attached)
                found = attachment(case, info())
                assert found is not None and len(found[2]) == 1, found
                row['verified_attachment'] = found[0]
                volume = plistlib.loads(command(['/usr/sbin/diskutil', 'info', '-plist', found[2][0]['dev-entry']]))
                assert str(volume.get('FilesystemType', '')).lower() == 'apfs', volume
                assert volume.get('MountPoint') == str(mount) and os.path.ismount(mount), volume
                row['volume_info'] = {key: volume.get(key) for key in ('DeviceNode', 'FilesystemType', 'MountPoint')}
                # hdiutil's -mode does not set the APFS volume-root mode on
                # every macOS build. This is our exact verified image/mount;
                # require its owner before setting the private store mode.
                assert mount.stat().st_uid == os.getuid(), mount
                row['mounted_mode_before'] = oct(stat.S_IMODE(mount.stat().st_mode))
                mount.chmod(0o700)
                row['directories'] = [private(root), *[private(root / name) for name in namespaces]]
                assert mount.stat().st_dev != root.stat().st_dev, 'test did not create a different filesystem'
                sibling = root / (namespaces[1] if namespace == namespaces[0] else namespaces[0])
                assert sibling.stat().st_dev == root.stat().st_dev
                row['rejection'] = probe(root)
                assert row['rejection']['returncode'] == 1, row['rejection']
                diagnostic = 'must be on the same APFS filesystem; nested mounts are unsupported'
                assert diagnostic in row['rejection']['stderr'], row['rejection']
                row['objects_entries'] = sorted(entry.name for entry in (root / namespaces[0]).iterdir())
                assert not any(re.fullmatch(r'[0-9a-f]{64}', name) for name in row['objects_entries']), row
                row['no_committed_envelopes'] = True
            finally:
                detach(case)
            row['post_detach'] = probe(root)
            assert row['post_detach']['returncode'] == 0, row['post_detach']
    except BaseException as error:
        result['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        cleanup_errors = []
        for case in cases:
            try:
                detach(case)
            except BaseException as error:
                cleanup_errors.append(f'{case["image"]}: {type(error).__name__}: {error}')
        try:
            snapshot = info()
            for case in cases:
                unmounted(case, snapshot)
            # Reject even an unexpected attachment within this owned tree.
            assert not any(entity.get('mount-point', '').startswith(str(work) + '/')
                           for image in snapshot.get('images', []) for entity in image.get('system-entities', []))
            private(work)
        except BaseException as error:
            cleanup_errors.append(f'final ownership/absence check: {type(error).__name__}: {error}')
        if cleanup_errors:
            result.update(cleanup_errors=cleanup_errors, retained_artifacts=True)
            raise RuntimeError(f'mount cleanup uncertain; retained {work}: ' + '; '.join(cleanup_errors))
        try:
            shutil.rmtree(work)
        except BaseException as error:
            result.update(retained_artifacts=True, cleanup_errors=[f'removing {work}: {type(error).__name__}: {error}'])
            raise
        result['cleaned'] = not work.exists()
        report['cleaned'] = result['cleaned']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--mounts', action='store_true',
                        help='also qualify real owned nested APFS mounts without sudo')
    parser.add_argument('--report', type=pathlib.Path,
                        default=ROOT.parent / 'outputs/evidence/local-storage-faults.json')
    args = parser.parse_args()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = {'checks': [], 'cleaned': False}
    try:
        report = run_faults(args.binary)
        report['binary_sha256'] = hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest()
        if args.mounts:
            run_mounts(args.binary, report)
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

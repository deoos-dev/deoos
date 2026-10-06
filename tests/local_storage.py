"""Real independent-process contention and crash recovery for filesystem storage.

Run with an interpreter that has the filesystem-capable deoos SDK installed.
The package smoke harness also runs these checks with its freshly installed wheel.
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTESTANTS = 32

CONTENDER = r'''
import json, os, pathlib, sys, time
from deoos import Client, EngineError

index = int(sys.argv[1]); operation = sys.argv[2]
control = pathlib.Path(os.environ['DEOOS_TEST_CONTROL'])
with Client() as c:
    (control / str(index)).write_text('ready')
    deadline = time.monotonic() + 60
    while not (control / 'go').exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('parent did not release process barrier')
        time.sleep(.01)
    if operation == 'submit':
        try:
            c.submit('submit-race', 'submit-race.v1', {'candidate': index})
        except EngineError as error:
            assert error.status == 409, error
            result = {'winner': False, 'status': error.status, 'candidate': index}
        else:
            result = {'winner': True, 'candidate': index}
    else:
        task = c.request('/claim', {'worker': 'process-' + str(index),
                                  'handlers': ['claim-race.v1']})['task']
        result = {'winner': task is not None, 'id': task['id'] if task else None}
    print(json.dumps(result))
'''

CRASH_WORKER = r'''
import json, os, pathlib, time
from deoos import Client

control = pathlib.Path(os.environ['DEOOS_TEST_CONTROL'])
with Client() as c:
    def handler(ctx, inputs):
        def effect():
            with (control / 'effects').open('a') as output:
                output.write('saved\n')
            return {'value': 42}
        ctx.step('saved', effect)
        # This marker is written only after the real checkpoint request returns.
        (control / 'committed').write_text('ready')
        time.sleep(120)
        return 'worker should have been killed'
    assert c.run_once({'recovery.v1': handler})
'''

RESUME_WORKER = r'''
import json, os, uuid
from deoos import Client, EngineError

with Client() as c:
    def handler(ctx, inputs):
        assert ctx.task['token'] != os.environ['DEOOS_TEST_STALE_TOKEN']
        try:
            c.request('/tasks/recovery/complete', {
                'token': os.environ['DEOOS_TEST_STALE_TOKEN'],
                'operation_id': str(uuid.uuid4()), 'value': 'stale write'})
        except EngineError as error:
            assert error.status == 409, error
        else:
            raise AssertionError('stale token completed replacement attempt')
        def forbidden_effect():
            raise AssertionError('committed checkpoint callback repeated')
        return ctx.step('saved', forbidden_effect)
    assert c.run_once({'recovery.v1': handler})
    result = c.inspect('recovery')
    assert result['status'] == 'completed' and result['output'] == {'value': 42}, result
    assert result['attempts'] == 2, result
    print(json.dumps({'status': result['status'], 'output': result['output'],
                      'attempts': result['attempts'], 'stale_token_status': 409}))
'''


def stop(process):
    if process.poll() is None:
        process.kill()
    process.wait(timeout=10)


def run_script(python, script, env, *args):
    result = subprocess.run([python, '-c', script, *args], env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f"child failed: {result.stdout}\n{result.stderr}"
    return json.loads(result.stdout)


def race(python, operation, env, work):
    control = work / operation
    control.mkdir()
    child_env = dict(env, DEOOS_TEST_CONTROL=str(control))
    children = []
    try:
        for index in range(CONTESTANTS):
            children.append(subprocess.Popen(
                [python, '-c', CONTENDER, str(index), operation], env=child_env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        deadline = time.monotonic() + 60
        while len(list(control.iterdir())) != CONTESTANTS:
            assert all(child.poll() is None for child in children), 'contender exited before barrier'
            assert time.monotonic() < deadline, 'contenders did not reach process barrier'
            time.sleep(.02)
        (control / 'go').write_text('go')
        results = []
        deadline = time.monotonic() + 60
        for child in children:
            stdout, stderr = child.communicate(timeout=max(.1, deadline - time.monotonic()))
            assert child.returncode == 0, f"contender failed: {stdout}\n{stderr}"
            results.append(json.loads(stdout))
        assert sum(result['winner'] for result in results) == 1, results
        if operation == 'claim':
            assert [result['id'] for result in results if result['winner']] == ['claim-race']
        return results
    finally:
        for child in children:
            stop(child)
            if child.stdout:
                child.stdout.close()
            if child.stderr:
                child.stderr.close()


def run_local_storage(python, root_env):
    """Use the supplied SDK interpreter, with all owned files in one temp tree."""
    report = {'processes_per_race': CONTESTANTS, 'checks': [], 'cleaned': False}
    with tempfile.TemporaryDirectory(prefix='deoos-local-storage-proof-') as scratch:
        # macOS /var is a symlink to /private/var; the adapter requires real parents.
        work = pathlib.Path(scratch).resolve()
        env = {key: value for key, value in root_env.items()
               if not key.startswith(('AWS_', 'DEOOS_STORAGE_')) and key not in {
                   'ENGINE_URL', 'ENGINE_TOKEN', 'ENGINE_BIND', 'DEOOS_MODE',
               }}
        env.update(DEOOS_STORAGE_PROVIDER='filesystem',
                   DEOOS_STORAGE_DIRECTORY=str(work / 'storage'),
                   EXECUTION_PREFIX='process-races-' + uuid.uuid4().hex,
                   LEASE_MS='120000')
        assert not any(key.startswith('AWS_') for key in env)
        submissions = race(python, 'submit', env, work)
        winner = next(result['candidate'] for result in submissions if result['winner'])
        persisted = run_script(python, "from deoos import Client; import json; "
                               "c=Client(); print(json.dumps(c.inspect('submit-race')['inputs'])); c.close()", env)
        assert persisted == {'candidate': winner}, persisted
        report['checks'].append({'name': 'conflicting immutable submissions from independent processes',
                                 'winners': 1, 'conflicts': CONTESTANTS - 1})
        run_script(python, "from deoos import Client; import json; "
                   "c=Client(); c.submit('claim-race','claim-race.v1',{}); c.close(); print('{}')", env)
        race(python, 'claim', env, work)
        report['checks'].append({'name': 'independent Client processes claim one task',
                                 'winners': 1, 'empty_claims': CONTESTANTS - 1})

        control = work / 'recovery'
        control.mkdir()
        env.update(EXECUTION_PREFIX='crash-recovery-' + uuid.uuid4().hex,
                   LEASE_MS='1500', DEOOS_TEST_CONTROL=str(control))
        run_script(python, "from deoos import Client; "
                   "c=Client(); c.submit('recovery','recovery.v1',{},max_attempts=3); c.close(); print('{}')", env)
        worker = subprocess.Popen([python, '-c', CRASH_WORKER], env=env,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 60
            while not (control / 'committed').exists():
                assert worker.poll() is None, 'worker exited before checkpoint commit'
                assert time.monotonic() < deadline, 'worker did not commit checkpoint'
                time.sleep(.02)
            worker.kill()
            worker.wait(timeout=10)
            assert worker.returncode != 0, 'worker was not forcibly terminated'
            old = run_script(python, "from deoos import Client; import json; "
                             "c=Client(); print(json.dumps(c.inspect('recovery'))); c.close()", env)
            assert old['status'] == 'running' and 'saved' in old['steps'], old
            remaining = max(0, (old['expires_at'] - time.time() * 1000) / 1000)
            assert remaining < 10, f"unexpected lease duration: {remaining}"
            time.sleep(remaining + .1)
            recovery = run_script(python, RESUME_WORKER,
                                  dict(env, DEOOS_TEST_STALE_TOKEN=old['token']))
            assert (control / 'effects').read_text() == 'saved\n'
            report['checks'].append({'name': 'SIGKILL after checkpoint; fresh process recovery and stale token fence',
                                     'checkpoint_effect_count': 1, **recovery})
        finally:
            stop(worker)
            worker.stdout.close()
            worker.stderr.close()
    assert not pathlib.Path(scratch).exists(), 'owned test files survived cleanup'
    report['cleaned'] = True
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python', default=sys.executable,
                        help='interpreter with the filesystem-capable deoos SDK installed')
    args = parser.parse_args()
    report_path = ROOT.parent / 'outputs/evidence/local-storage-processes.json'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        report = run_local_storage(args.python, os.environ)
    except BaseException as error:
        report_path.write_text(json.dumps({'error': f'{type(error).__name__}: {error}'}, indent=2) + '\n')
        raise
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

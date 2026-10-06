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


WARM_DISCOVERY = r'''
import json, os, subprocess, sys, time, uuid
from deoos import Client, EngineError

# A keeps one native engine for the entire proof. Every B mutation comes from an
# independent process and engine, using actual SDK operations rather than files.
EXTERNAL = r"""
import json, os, sys
from deoos import Client
command = json.load(sys.stdin)
with Client() as c:
    value = getattr(c, command['method'])(*command.get('args', []), **command.get('kwargs', {}))
    print(json.dumps({'pid': os.getpid(), 'value': value}))
"""
external_pids = []
def external(method, *args, **kwargs):
    result = subprocess.run([sys.executable, '-c', EXTERNAL],
        input=json.dumps({'method': method, 'args': args, 'kwargs': kwargs}),
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    reply = json.loads(result.stdout)
    assert reply['pid'] != os.getpid()
    external_pids.append(reply['pid'])
    return reply['value']

with Client() as a:
    def claim(handler='cache-hints.v1'):
        return a.request('/claim', {'worker': 'warm-a', 'handlers': [handler]})['task']
    def inventory():
        return {task['id']: task for task in a.list_tasks()['tasks']}
    def complete(task, value):
        return a.request('/tasks/' + task['id'] + '/complete', {
            'token': task['token'], 'operation_id': str(uuid.uuid4()), 'value': value})

    # Warm both active/schedule discovery and a task-prefix listing before B
    # creates any matching entries. Persistent A must observe new external work.
    assert inventory() == {}
    for _ in range(3):
        assert claim() is None
    external('submit', 'external-task', 'cache-hints.v1', {'source': 'process-b'})
    queued = inventory()['external-task']
    assert queued['status'] == 'queued' and queued['inputs'] == {'source': 'process-b'}
    first = claim()
    assert first['id'] == 'external-task' and first['inputs'] == queued['inputs']
    assert inventory()['external-task']['revision'] == first['revision']
    assert claim() is None, 'A claimed an externally created task twice'

    # B changes the same task-state key, deletes its active marker, then manual
    # retry creates a new active entry. No cached owner, revision or tombstone may
    # hide these changes from A or let an older token commit.
    cancelled = external('cancel', 'external-task')
    assert cancelled['status'] == 'cancelled'
    assert inventory()['external-task']['revision'] == cancelled['revision']
    assert claim() is None, 'deleted active entry was still claimable'
    retried = external('retry', 'external-task', cancelled['revision'])
    assert retried['status'] == 'queued'
    assert inventory()['external-task']['revision'] == retried['revision']
    second = claim()
    assert second['id'] == first['id']
    assert second['token'] != first['token'] and second['generation'] > first['generation']
    assert second['active_entry_id'] != first['active_entry_id']
    assert inventory()['external-task']['revision'] == second['revision']
    try:
        complete(first, 'stale owner must not commit')
    except EngineError as error:
        assert error.status == 409, error
    else:
        raise AssertionError('warm engine accepted stale ownership after external retry')
    current = external('inspect', 'external-task')
    assert current['token'] == second['token'] and current['revision'] == second['revision']
    complete(second, {'fresh': True})
    terminal = external('inspect', 'external-task')
    assert terminal['status'] == 'completed' and terminal['output'] == {'fresh': True}
    assert claim() is None, 'completed marker deletion was not observed'

    # Schedule discovery uses LIST change tokens to skip paused definitions.
    # This behavioral case catches stale matching-key metadata after replacement.
    anchor = int(time.time() * 1000) - 1000
    external('schedule', 'external-schedule', 'cache-schedule.v1', {'source': 'process-b'},
             3600000, first_due_ms=anchor)
    external('pause_schedule', 'external-schedule')
    for _ in range(3):
        assert claim('cache-schedule.v1') is None
    external('resume_schedule', 'external-schedule')
    scheduled = claim('cache-schedule.v1')
    assert scheduled is not None, 'external resume remained hidden behind paused schedule hints'
    assert scheduled['schedule'] == {'id': 'external-schedule', 'scheduled_at': anchor}
    assert scheduled['inputs'] == {'source': 'process-b'}
    complete(scheduled, {'scheduled': True})
    assert claim('cache-schedule.v1') is None
    print(json.dumps({'checks': [
        'warm persistent engine discovers externally created matching task',
        'external cancel/retry changes remain visible with fresh revisions and active entry',
        'external replacement ownership rejects stale token and preserves current state',
        'terminal marker deletion removes work from warm discovery',
        'matching schedule LIST metadata observes external paused-to-resumed replacement'],
        'external_client_processes': len(external_pids), 'stale_token_status': 409}))
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
        warm_env = dict(env, EXECUTION_PREFIX='warm-discovery-' + uuid.uuid4().hex)
        warm = run_script(python, WARM_DISCOVERY, warm_env)
        report['checks'].append({'name': 'persistent engine observes cross-process discovery and fencing changes',
                                 **warm})

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

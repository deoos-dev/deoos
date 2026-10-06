"""Fresh-install acceptance for a packaged Python/TypeScript workflow."""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
import venv
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAX_SAFE_INTEGER = 9_007_199_254_740_991


def command(args, *, cwd=None, env=None, timeout=90, check=True):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True,
                            timeout=timeout)
    if check and result.returncode:
        raise AssertionError(f"command failed ({result.returncode}): {args}\n"
                             f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
    return result


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary, path)


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def npm_args(executable, args):
    if os.name == "nt":
        return [os.environ.get("COMSPEC", "cmd.exe"), "/c", executable, *args]
    return [executable, *args]


def wait_server(process, url):
    for _ in range(100):
        if process.poll() is not None:
            raise AssertionError(f"server exited early ({process.returncode})")
        try:
            with urllib.request.urlopen(url + "/health", timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(.1)
    raise AssertionError("server did not become healthy")


def stop_process(process, label):
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
    if process.poll() is None:
        raise AssertionError(f"{label} is still running")


def inspect(python, script, task_id, env, cwd):
    result = command([python, str(script), "inspect", "--id", task_id],
                     env=env, cwd=cwd)
    return json.loads(result.stdout)


def remote_environment(env):
    """Remote SDKs must work without direct access to the server's storage."""
    result = {key: value for key, value in env.items()
              if not key.startswith(("AWS_", "DEOOS_STORAGE_")) and key not in {
                  "EXECUTION_PREFIX", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY",
              }}
    assert not any(key.startswith(("AWS_", "DEOOS_STORAGE_")) for key in result)
    return result


def runtime_info(mode, python, node, env, cwd, server_pid):
    python_probe = (
        "import json,os,platform,sys; from deoos import Client; "
        "c=Client.remote(os.environ['ENGINE_URL'],os.environ.get('ENGINE_TOKEN')) "
        "if os.environ['DEOOS_MODE']=='server' else Client(); "
        "i=c.request('/info'); print(json.dumps({'system':platform.system(),'machine':platform.machine(),"
        "'version':platform.python_version(),'executable':sys.executable,'client_pid':os.getpid(),"
        "'engine_pid':i['process_id']})); c.close()"
    )
    py = json.loads(command([python, "-c", python_probe], cwd=cwd, env=env).stdout)
    node_probe = (
        "import {Client} from 'deoos'; const c=process.env.DEOOS_MODE==='server' "
        "?Client.remote(process.env.ENGINE_URL,process.env.ENGINE_TOKEN) "
        ":new Client(process.env.DEOOS_STORAGE_PROVIDER==='filesystem' "
        "?{provider:'filesystem',directory:process.env.DEOOS_STORAGE_DIRECTORY}:"
        "{bucket:process.env.AWS_BUCKET}); const i=await c.request('/info'); "
        "console.log(JSON.stringify({platform:process.platform,arch:process.arch,"
        "version:process.version,client_pid:process.pid,engine_pid:i.process_id}));"
    )
    js = json.loads(command([node, "--input-type=module", "-e", node_probe],
                            cwd=cwd, env=env).stdout)
    if mode == "library":
        assert py["client_pid"] == py["engine_pid"]
        assert js["client_pid"] == js["engine_pid"]
    else:
        assert py["engine_pid"] == server_pid and py["client_pid"] != server_pid
        assert js["engine_pid"] == server_pid and js["client_pid"] != server_pid
    return {"python": py, "node": js}


def installed_artifacts(python, node_modules, env):
    package = node_modules / "deoos"
    native = next((path for path in (package / "dist" / "native").glob("*.node")), None)
    sdk = package / "dist" / "index.js"
    python_native_name = {"nt": "deoos_engine.dll", "darwin": "libdeoos_engine.dylib",
                          "linux": "libdeoos_engine.so"}[os.name if os.name == "nt" else sys.platform]
    clean_env = {key: value for key, value in env.items() if key not in {
        "PYTHONPATH", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY",
    }}
    location = pathlib.Path(command(
        [python, "-c", "import deoos,pathlib; print(pathlib.Path(deoos.__file__).parent)"],
        cwd=node_modules.parent, env=clean_env, timeout=30).stdout.strip())
    native_files = list((location / "native").glob("*"))
    py_native = location / "native" / python_native_name
    assert native and sdk.is_file() and py_native.is_file()
    assert [path.name for path in native_files if path.is_file()] == [python_native_name]
    return {"python_sdk": sha256(location / "__init__.py"),
            "python_native": sha256(py_native), "typescript_sdk": sha256(sdk),
            "typescript_native": sha256(native)}


def run_workflow_case(mode, first_language, resume_language, python, node,
                      examples, package, root_env, processes, runtime_reports):
    prefix = f"smoke-{mode}-{first_language}-{uuid.uuid4().hex[:8]}"
    env = dict(root_env, DEOOS_MODE=mode, EXECUTION_PREFIX=prefix)
    if mode == "server":
        listen_port = port()
        env.update(ENGINE_BIND=f"127.0.0.1:{listen_port}",
                   ENGINE_URL=f"http://127.0.0.1:{listen_port}")
        env["ENGINE_TOKEN"] = "package-smoke-token"
    else:
        for key in ("ENGINE_BIND", "ENGINE_URL", "ENGINE_TOKEN"):
            env.pop(key, None)
    worker_env = dict(env)
    if mode == "server":
        worker_env = remote_environment(worker_env)

    server = None

    def start_server():
        nonlocal server
        binary = package / "bin" / ("deoos-server.exe" if os.name == "nt" else "deoos-server")
        server = subprocess.Popen([str(binary)], cwd=package, env=env,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append((server, "server"))
        wait_server(server, env["ENGINE_URL"])

    def cli(language, action, *args, timeout=90):
        executable = python if language == "python" else node
        script = examples / ("workflow_python.py" if language == "python" else "workflow_typescript.mjs")
        return command([executable, str(script), action, *args], cwd=examples,
                       env=worker_env, timeout=timeout)

    if mode == "server":
        start_server()
    if first_language == "python":
        runtime_reports[mode] = runtime_info(mode, python, node, worker_env, examples,
                                              server.pid if server else None)
    failed_id = f"{prefix}-failed"
    valid_id = f"{prefix}-valid"
    if first_language == "python":
        simple_examples = {
            "python": examples / f"{mode}_python.py",
            "typescript": examples / f"{mode}_typescript.mjs",
        }
        for language, script in simple_examples.items():
            executable = python if language == "python" else node
            result = command([executable, str(script), f"hello-{mode}-{language}"],
                             cwd=examples, env=worker_env)
            assert "Hello, World!" in result.stdout, result.stdout
    cli(first_language, "submit", "--id", failed_id, "--quantity", "2",
        "--unit-price", str(MAX_SAFE_INTEGER), "--delay-ms", "0")
    cli(first_language, "submit", "--id", valid_id, "--quantity", "2",
        "--unit-price", "25", "--delay-ms", "500")

    executable = python if first_language == "python" else node
    script = examples / ("workflow_python.py" if first_language == "python" else "workflow_typescript.mjs")
    worker = subprocess.Popen([executable, str(script), "work"], cwd=examples,
                              env=worker_env, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
    processes.append((worker, f"{first_language} worker"))
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        failed = inspect(python, examples / "workflow_python.py", failed_id,
                         worker_env, examples)
        valid = inspect(python, examples / "workflow_python.py", valid_id,
                        worker_env, examples)
        assert worker.poll() is None, f"{first_language} worker stopped after failed order"
        if (failed["status"] == "failed" and valid["status"] == "waiting"
                and valid.get("waiting_on") == {"kind": "signal", "name": "approved"}
                and "cooldown" in valid["steps"]):
            break
        time.sleep(.25)
    else:
        raise AssertionError(f"{first_language} worker did not reach waiting signal state")
    stop_process(worker, f"{first_language} worker")
    assert failed["attempts"] == 1 and valid["attempts"] == 1

    if mode == "server":
        old_server = server
        stop_process(old_server, "shared server")
        start_server()
        assert server.pid != old_server.pid
        valid = inspect(python, examples / "workflow_python.py", valid_id,
                        worker_env, examples)
        assert valid["status"] == "waiting", "server restart lost the waiting task"

    cli(first_language, "approve", "--id", valid_id)
    cli(resume_language, "work", "--once")
    result = inspect(python, examples / "workflow_python.py", valid_id,
                     worker_env, examples)
    assert result["status"] == "completed", result
    assert result["attempts"] == 1, result
    assert result["output"] == {"order_id": valid_id, "total": 50, "approved": True}, result
    expected = {"validate", "price", "ready", "cooldown", "approved", "finalize"}
    assert expected <= set(result["definitions"]), result.get("definitions")
    assert expected <= set(result["steps"]), result.get("steps")
    if server is not None:
        stop_process(server, "shared server")
    return {"mode": mode, "worker": first_language, "resume_worker": resume_language,
            "failed_attempts": failed["attempts"], "completed_attempts": result["attempts"],
            "output": result["output"], "definitions": sorted(expected),
            "server_restarted": mode == "server"}


WORKER_ACCEPTANCE_PYTHON = r'''
import json, os, threading, time
from deoos import Client, EngineError

c = (Client.remote(os.environ['ENGINE_URL'], os.environ.get('ENGINE_TOKEN'))
     if os.environ['DEOOS_MODE'] == 'server' else Client())
passed = []
secrets = ['INPUT-PAYLOAD-SENTINEL', 'CHECKPOINT-PAYLOAD-SENTINEL',
           'SIGNAL-PAYLOAD-SENTINEL', 'OUTPUT-PAYLOAD-SENTINEL']
forbidden = {'inputs', 'output', 'owner', 'token', 'revision', 'generation',
             'active_incarnation', 'history', 'definitions', 'timers', 'signals',
             'last_operation', 'last_retry_operation'}

def summary(task_id, status):
    value = c.summary(task_id)
    assert value['summary_version'] == 1 and value['id'] == task_id
    assert value['status'] == status and not forbidden.intersection(value), value
    assert not any(secret in json.dumps(value) for secret in secrets), value
    return value

def run(handlers, stop, **options):
    c.run_worker(handlers, stop_event=stop, **options)

try:
    # An already stopped worker must leave pending work untouched.
    c.submit('pre-stopped', 'pre-stopped.v1', {})
    stopped = threading.Event(); stopped.set()
    run({'pre-stopped.v1': lambda ctx, inputs: 'unexpected'}, stopped)
    assert summary('pre-stopped', 'queued')['attempts'] == 0
    passed.append('already-stopped worker does not claim')

    # Observe an actual empty claim before scheduling stop during the idle wait.
    assert not c.run_once({'idle.v1': lambda ctx, inputs: None})
    stopped = threading.Event()
    real_request = c.request
    idle_started = []; timers = []; idle_claims = []
    def observe_empty_claim(path, data=None):
        value = real_request(path, data)
        if path == '/claim':
            idle_claims.append(value['task'])
            if value['task'] is None and not idle_started:
                idle_started.append(time.monotonic())
                timer = threading.Timer(.1, stopped.set)
                timers.append(timer); timer.start()
        return value
    c.request = observe_empty_claim
    try:
        run({'idle.v1': lambda ctx, inputs: None}, stopped, poll_interval=30)
    finally:
        c.request = real_request
        for timer in timers:
            timer.cancel(); timer.join()
    assert idle_started, 'worker did not return an empty real claim'
    assert idle_claims == [None] and len(timers) == 1, 'worker polled again instead of waiting idle'
    idle_seconds = time.monotonic() - idle_started[0]
    assert stopped.is_set() and idle_seconds < 5, idle_seconds
    passed.append('idle stop interrupts long poll interval')

    c.submit('drain', 'drain.v1', {})
    stopped = threading.Event()
    def drain(ctx, inputs):
        def active():
            running = summary('drain', 'running')
            assert running['attempts'] == 1 and running['wait'] is None
            assert running['actions'] == ['cancel'] and running['completed_steps'] == []
            stopped.set()
            c.submit('after-drain', 'drain.v1', {})
            time.sleep(.1)
            return 'drained'
        return ctx.step('active', active)
    run({'drain.v1': drain}, stopped)
    assert c.inspect('drain')['output'] == 'drained'
    assert summary('drain', 'completed')['completed_steps'] == ['active']
    assert summary('after-drain', 'queued')['attempts'] == 0
    passed.append('active stop drains checkpoint and completion without another claim')

    # A single failing attempt is committed before the original exception escapes.
    def failing_case(task_id, callback=None, replacement=None):
        c.submit(task_id, task_id + '.v1', {}, max_attempts=1)
        failure = RuntimeError(task_id + '-boom')
        seen = []
        def handler(ctx, inputs):
            raise failure
        def on_error(error, recorded_id):
            assert error is failure and recorded_id == task_id
            seen.append(recorded_id)
            if replacement is not None:
                raise replacement
            return callback
        options = {} if callback is None and replacement is None else {'on_error': on_error}
        try:
            run({task_id + '.v1': handler}, threading.Event(), **options)
        except Exception as error:
            if callback not in (None, 'propagate') and replacement is None:
                assert isinstance(error, ValueError) and error.__cause__ is failure, error
            else:
                assert error is (replacement or failure), error
        else:
            raise AssertionError('worker swallowed an error')
        value = summary(task_id, 'failed')
        assert value['attempts'] == value['max_attempts'] == 1
        assert value['actions'] == ['retry'] and task_id + '-boom' in value['last_failure']['message']
        assert len(seen) == (0 if not options else 1)
    failing_case('default-error')
    failing_case('callback-propagate', callback='propagate')
    failing_case('callback-throws', replacement=RuntimeError('callback-error'))
    failing_case('callback-invalid', callback='invalid')
    passed.append('default and callback errors propagate after recorded failure')

    c.submit('ownership-error', 'ownership-error.v1', {})
    ownership_errors = []
    def lost_ownership(ctx, inputs):
        c.cancel(ctx.task['id'])
        raise RuntimeError('uncommitted-handler-error')
    def ownership_error(error, task_id):
        assert isinstance(error, EngineError) and error.status == 409 and task_id is None
        ownership_errors.append(error)
        return 'propagate'
    try:
        run({'ownership-error.v1': lost_ownership}, threading.Event(), on_error=ownership_error)
    except EngineError as error:
        assert ownership_errors == [error]
    else:
        raise AssertionError('ownership write failure was swallowed')
    assert summary('ownership-error', 'cancelled')['actions'] == ['retry']
    passed.append('failed ownership write has no recorded task failure ID')

    if os.environ['DEOOS_MODE'] == 'server':
        unauthenticated = Client.remote(os.environ['ENGINE_URL'], 'incorrect-test-token')
        auth_errors = []
        def auth_error(error, task_id):
            assert isinstance(error, EngineError) and error.status == 401 and task_id is None
            auth_errors.append(error)
            return 'propagate'
        try:
            unauthenticated.run_worker({'auth.v1': lambda ctx, inputs: None},
                                       stop_event=threading.Event(), on_error=auth_error)
        except EngineError as error:
            assert auth_errors == [error]
        else:
            raise AssertionError('authentication failure was swallowed')
        finally:
            unauthenticated.close()
        passed.append('authentication failure has no recorded task failure ID')

    # Manual retry clears current error but retained history still describes failure.
    failed = c.inspect('default-error')
    c.retry('default-error', failed['revision'], 'acceptance-retry')
    assert c.inspect('default-error')['error'] is None
    retried = summary('default-error', 'queued')
    assert 'default-error-boom' in retried['last_failure']['message']
    assert retried['last_failure']['at_ms'] is not None
    passed.append('manual retry keeps historical failure summary')

    c.submit('continue', 'continue.v1', {}, max_attempts=2)
    stopped = threading.Event(); calls = []; errors = []
    def continue_handler(ctx, inputs):
        def effect():
            calls.append('effect')
            if len(calls) == 1:
                raise RuntimeError('transient-boom')
            return 'recovered'
        result = ctx.step('effect', effect)
        stopped.set()
        return result
    def continue_error(error, task_id):
        assert 'transient-boom' in str(error) and task_id == 'continue'
        errors.append(task_id)
        return 'continue'
    run({'continue.v1': continue_handler}, stopped, poll_interval=.01, on_error=continue_error)
    recovered = summary('continue', 'completed')
    assert len(calls) == 2 and errors == ['continue']
    assert recovered['attempts'] == 2 and recovered['completed_steps'] == ['effect']
    assert 'transient-boom' in recovered['last_failure']['message']
    passed.append('explicit continue recovers and retains historical failure')

    c.submit('approval', 'approval.v1', {'private': secrets[0]})
    queued = summary('approval', 'queued')
    assert queued['wait'] is None and queued['actions'] == ['cancel']
    def approval(ctx, inputs):
        ctx.step('before', lambda: secrets[1])
        ctx.wait_signal('approved')
        return secrets[3]
    assert c.run_once({'approval.v1': approval})
    waiting = summary('approval', 'waiting')
    assert waiting['wait'] == {'kind': 'signal', 'name': 'approved', 'assigned': False}
    assert waiting['completed_steps'] == ['before'] and waiting['actions'] == ['cancel', 'signal']
    signalled = c.signal('approval', 'approved', {'private': secrets[2]}, 'approval-signal')
    assert signalled['signals']['approved'].endswith('/approval-signal.json'), signalled['signals']
    assert c.signal('approval', 'approved', {'private': secrets[2]}, 'approval-signal') == signalled
    try:
        c.signal('approval', 'approved', {'private': 'different'}, 'approval-signal')
    except EngineError as error:
        assert error.status == 409, error
    else:
        raise AssertionError('stable signal operation ID accepted a different value')
    assert c.inspect('approval') == signalled
    passed.append('stable signal operation ID replays and rejects a different value')
    assigned = summary('approval', 'waiting')
    assert assigned['wait']['assigned'] is True and assigned['actions'] == ['cancel']
    assert c.run_once({'approval.v1': approval})
    completed = summary('approval', 'completed')
    assert completed['wait'] is None and completed['actions'] == []
    assert completed['completed_steps'] == ['approved', 'before']
    assert c.inspect('approval')['output'] == secrets[3], 'raw inspect changed'
    passed.append('queued/waiting/assigned/completed summaries omit payloads and tokens')
    print(json.dumps({'checks': passed, 'idle_seconds': idle_seconds}))
finally:
    c.close()
'''


WORKER_ACCEPTANCE_TYPESCRIPT = r'''
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {performance} from 'node:perf_hooks';
import {Client, EngineError} from 'deoos';

const c = process.env.DEOOS_MODE === 'server'
  ? Client.remote(process.env.ENGINE_URL, process.env.ENGINE_TOKEN)
  : new Client(process.env.DEOOS_STORAGE_PROVIDER === 'filesystem'
    ? {provider: 'filesystem', directory: process.env.DEOOS_STORAGE_DIRECTORY}
    : {bucket: process.env.AWS_BUCKET});
const passed = [];
const secrets = ['INPUT-PAYLOAD-SENTINEL', 'CHECKPOINT-PAYLOAD-SENTINEL',
  'SIGNAL-PAYLOAD-SENTINEL', 'OUTPUT-PAYLOAD-SENTINEL'];
const forbidden = ['inputs', 'output', 'owner', 'token', 'revision', 'generation',
  'active_incarnation', 'history', 'definitions', 'timers', 'signals',
  'last_operation', 'last_retry_operation'];
async function summary(id, status) {
  const value = await c.summary(id);
  assert.equal(value.summary_version, 1); assert.equal(value.id, id);
  assert.equal(value.status, status);
  assert(forbidden.every(key => !Object.hasOwn(value, key)));
  assert(secrets.every(secret => !JSON.stringify(value).includes(secret)));
  return value;
}
const run = (handlers, stop, options = {}) => c.runWorker(handlers, {signal: stop.signal, ...options});

await c.submit('pre-stopped', 'pre-stopped.v1', {});
let stop = new AbortController(); stop.abort();
await run({'pre-stopped.v1': () => 'unexpected'}, stop);
assert.equal((await summary('pre-stopped', 'queued')).attempts, 0);
passed.push('already-stopped worker does not claim');

assert.equal(await c.runOnce({'idle.v1': () => null}), false);
stop = new AbortController();
const idleStop = stop;
const realRequest = c.request;
let idleStarted, idleTimer, idleTimerStarts = 0;
const idleClaims = [];
c.request = async function(path, data) {
  const value = await realRequest.call(c, path, data);
  if (path === '/claim') {
    idleClaims.push(value.task);
    if (value.task === null && idleStarted === undefined) {
      idleStarted = performance.now(); idleTimerStarts += 1;
      idleTimer = setTimeout(() => idleStop.abort(), 100);
    }
  }
  return value;
};
try { await run({'idle.v1': () => null}, stop, {pollIntervalMs: 30000}); }
finally { c.request = realRequest; clearTimeout(idleTimer); }
assert.notEqual(idleStarted, undefined, 'worker did not return an empty real claim');
assert.deepEqual(idleClaims, [null], 'worker polled again instead of waiting idle');
assert.equal(idleTimerStarts, 1);
const idle_seconds = (performance.now() - idleStarted) / 1000;
assert(stop.signal.aborted && idle_seconds < 5, String(idle_seconds));
passed.push('idle stop interrupts long poll interval');

await c.submit('drain', 'drain.v1', {});
stop = new AbortController();
await run({'drain.v1': ctx => ctx.step('active', async () => {
  const running = await summary('drain', 'running');
  assert.equal(running.attempts, 1); assert.equal(running.wait, null);
  assert.deepEqual(running.actions, ['cancel']); assert.deepEqual(running.completed_steps, []);
  stop.abort(); await c.submit('after-drain', 'drain.v1', {});
  await delay(100); return 'drained';
})}, stop);
assert.equal((await c.inspect('drain')).output, 'drained');
assert.deepEqual((await summary('drain', 'completed')).completed_steps, ['active']);
assert.equal((await summary('after-drain', 'queued')).attempts, 0);
passed.push('active stop drains checkpoint and completion without another claim');

async function failingCase(id, decision, replacement) {
  await c.submit(id, id + '.v1', {}, 1);
  const failure = new Error(id + '-boom'); const seen = [];
  const options = decision === undefined && replacement === undefined ? {} : {
    onError: (error, taskId) => {
      assert.equal(error, failure); assert.equal(taskId, id); seen.push(taskId);
      if (replacement) throw replacement;
      return decision;
    },
  };
  await assert.rejects(run({[id + '.v1']: () => {throw failure;}}, new AbortController(), options),
    error => decision !== undefined && decision !== 'propagate' && replacement === undefined
      ? error.cause === failure && error.message.includes('onError must return')
      : error === (replacement ?? failure));
  const value = await summary(id, 'failed');
  assert.equal(value.attempts, 1); assert.equal(value.max_attempts, 1);
  assert.deepEqual(value.actions, ['retry']); assert(value.last_failure.message.includes(id + '-boom'));
  assert.equal(seen.length, Object.keys(options).length ? 1 : 0);
}
await failingCase('default-error');
await failingCase('callback-propagate', 'propagate');
await failingCase('callback-throws', undefined, new Error('callback-error'));
await failingCase('callback-invalid', 'invalid');
passed.push('default and callback errors propagate after recorded failure');

await c.submit('ownership-error', 'ownership-error.v1', {});
const ownershipErrors = [];
await assert.rejects(run({'ownership-error.v1': async ctx => {
  await c.cancel(ctx.task.id); throw new Error('uncommitted-handler-error');
}}, new AbortController(), {onError: (error, taskId) => {
  assert(error instanceof EngineError); assert.equal(error.status, 409); assert.equal(taskId, undefined);
  ownershipErrors.push(error); return 'propagate';
}}), error => ownershipErrors.length === 1 && ownershipErrors[0] === error);
assert.deepEqual((await summary('ownership-error', 'cancelled')).actions, ['retry']);
passed.push('failed ownership write has no recorded task failure ID');

if (process.env.DEOOS_MODE === 'server') {
  const unauthenticated = Client.remote(process.env.ENGINE_URL, 'incorrect-test-token');
  const authErrors = [];
  await assert.rejects(unauthenticated.runWorker({'auth.v1': () => null}, {
    signal: new AbortController().signal, onError: (error, taskId) => {
      assert(error instanceof EngineError); assert.equal(error.status, 401); assert.equal(taskId, undefined);
      authErrors.push(error); return 'propagate';
    },
  }), error => authErrors.length === 1 && authErrors[0] === error);
  passed.push('authentication failure has no recorded task failure ID');
}

const failed = await c.inspect('default-error');
await c.retry('default-error', failed.revision, 'acceptance-retry');
assert.equal((await c.inspect('default-error')).error, null);
const retried = await summary('default-error', 'queued');
assert(retried.last_failure.message.includes('default-error-boom'));
assert.notEqual(retried.last_failure.at_ms, null);
passed.push('manual retry keeps historical failure summary');

await c.submit('continue', 'continue.v1', {}, 2);
stop = new AbortController(); const calls = []; const errors = [];
await run({'continue.v1': async ctx => {
  const result = await ctx.step('effect', () => {
    calls.push('effect'); if (calls.length === 1) throw new Error('transient-boom');
    return 'recovered';
  });
  stop.abort(); return result;
}}, stop, {pollIntervalMs: 10, onError: (error, taskId) => {
  assert(String(error).includes('transient-boom')); assert.equal(taskId, 'continue');
  errors.push(taskId); return 'continue';
}});
const recovered = await summary('continue', 'completed');
assert.equal(calls.length, 2); assert.deepEqual(errors, ['continue']);
assert.equal(recovered.attempts, 2); assert.deepEqual(recovered.completed_steps, ['effect']);
assert(recovered.last_failure.message.includes('transient-boom'));
passed.push('explicit continue recovers and retains historical failure');

await c.submit('approval', 'approval.v1', {private: secrets[0]});
const queued = await summary('approval', 'queued');
assert.equal(queued.wait, null); assert.deepEqual(queued.actions, ['cancel']);
const approval = async ctx => {
  await ctx.step('before', () => secrets[1]); await ctx.waitSignal('approved'); return secrets[3];
};
assert.equal(await c.runOnce({'approval.v1': approval}), true);
const waiting = await summary('approval', 'waiting');
assert.deepEqual(waiting.wait, {kind: 'signal', name: 'approved', assigned: false});
assert.deepEqual(waiting.completed_steps, ['before']); assert.deepEqual(waiting.actions, ['cancel', 'signal']);
const signalled = await c.signal('approval', 'approved', {private: secrets[2]}, 'approval-signal');
assert(signalled.signals.approved.endsWith('/approval-signal.json'), JSON.stringify(signalled.signals));
assert.deepEqual(await c.signal('approval', 'approved', {private: secrets[2]}, 'approval-signal'), signalled);
await assert.rejects(c.signal('approval', 'approved', {private: 'different'}, 'approval-signal'),
  error => error.status === 409);
assert.deepEqual(await c.inspect('approval'), signalled);
passed.push('stable signal operation ID replays and rejects a different value');
const assigned = await summary('approval', 'waiting');
assert.equal(assigned.wait.assigned, true); assert.deepEqual(assigned.actions, ['cancel']);
assert.equal(await c.runOnce({'approval.v1': approval}), true);
const completed = await summary('approval', 'completed');
assert.equal(completed.wait, null); assert.deepEqual(completed.actions, []);
assert.deepEqual(completed.completed_steps, ['approved', 'before']);
assert.equal((await c.inspect('approval')).output, secrets[3], 'raw inspect changed');
passed.push('queued/waiting/assigned/completed summaries omit payloads and tokens');
console.log(JSON.stringify({checks: passed, idle_seconds}));
'''


def run_worker_case(mode, language, python, node, work, package, root_env, processes):
    """Exercise public worker/summary APIs using only the freshly installed packages."""
    env = dict(root_env, DEOOS_MODE=mode,
               EXECUTION_PREFIX=f"worker-smoke-{mode}-{language}-{uuid.uuid4().hex}")
    server = None
    if mode == "server":
        listen_port = port()
        env.update(ENGINE_BIND=f"127.0.0.1:{listen_port}",
                   ENGINE_URL=f"http://127.0.0.1:{listen_port}", ENGINE_TOKEN="worker-smoke-token")
        binary = package / "bin" / ("deoos-server.exe" if os.name == "nt" else "deoos-server")
        server = subprocess.Popen([str(binary)], cwd=work, env=env,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append((server, "worker acceptance server"))
        wait_server(server, env["ENGINE_URL"])
        env = remote_environment(env)
    else:
        for key in ("ENGINE_URL", "ENGINE_TOKEN", "ENGINE_BIND"):
            env.pop(key, None)
    script = work / ("worker_acceptance.py" if language == "python" else "worker_acceptance.mjs")
    script.write_text(WORKER_ACCEPTANCE_PYTHON if language == "python"
                      else WORKER_ACCEPTANCE_TYPESCRIPT)
    try:
        result = command([python if language == "python" else node, str(script)],
                         cwd=work, env=env, timeout=180)
        checks = json.loads(result.stdout)
        cli_summary = command([python, "-m", "deoos", "summary", "approval"],
                              cwd=work, env=env)
        value = json.loads(cli_summary.stdout)
        assert value["summary_version"] == 1 and value["id"] == "approval"
        assert value["status"] == "completed" and value["completed_steps"] == ["approved", "before"]
        assert not {"inputs", "output", "token", "owner", "history"}.intersection(value)
        cli_explain = command([python, "-m", "deoos", "explain", "approval"],
                              cwd=work, env=env)
        assert "approval: completed" in cli_explain.stdout
        assert "Completed steps:" in cli_explain.stdout and "Suggested actions:" in cli_explain.stdout
        for output in (cli_summary.stdout, cli_explain.stdout):
            assert "PAYLOAD-SENTINEL" not in output, output
        checks["checks"].append("installed operator CLI JSON summary and human explanation omit payloads")
        return {"mode": mode, "language": language, **checks}
    finally:
        if server is not None:
            stop_process(server, "worker acceptance server")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release", nargs="?", type=pathlib.Path,
                        help="release directory (defaults to this host's packaged release)")
    parser.add_argument("--backend", choices=("rustfs", "aws", "filesystem"), default="rustfs")
    parser.add_argument("--aws-profile", default=os.environ.get("AWS_PROFILE", "auto"),
                        help="AWS profile, or 'auto' for environment/instance credentials")
    args = parser.parse_args()

    if args.release:
        package = args.release.resolve()
    else:
        sys.path.insert(0, str(ROOT / "packaging"))
        from build_package import target
        version = json.loads((ROOT / "clients/typescript/package.json").read_text())["version"]
        package = ROOT.parent / "outputs" / f"deoos-{version}-{target()[0]}"
    assert package.is_dir(), f"release directory not found: {package}"
    sums = package / "SHA256SUMS"
    release_hashes = {}
    for line in sums.read_text().splitlines():
        digest, relative = line.split("  ", 1)
        path = package / relative
        assert path.is_file() and sha256(path) == digest, relative
        release_hashes[relative] = digest
    wheel, = (package / "python").glob("*.whl")
    npm_package, = (package / "node").glob("*.tgz")
    assert "-py3-none-" in wheel.name and not wheel.name.endswith("-any.whl")
    python_native_name = {"nt": "deoos_engine.dll", "darwin": "libdeoos_engine.dylib",
                          "linux": "libdeoos_engine.so"}[os.name if os.name == "nt" else sys.platform]
    with zipfile.ZipFile(wheel) as archive:
        native_names = [pathlib.PurePosixPath(name).name for name in archive.namelist()
                        if "/native/" in name]
        assert native_names == [python_native_name], native_names
    examples = package / "examples"
    server_binary = package / "bin" / ("deoos-server.exe" if os.name == "nt" else "deoos-server")
    server_version = command([str(server_binary), "--version"], cwd=package).stdout.strip()
    for name in ("workflow_python.py", "workflow_typescript.mjs", "library_python.py",
                 "library_typescript.mjs", "server_python.py", "server_typescript.mjs"):
        assert (examples / name).is_file(), f"missing packaged workflow example: {name}"

    bucket = None if args.backend == "filesystem" else (
        os.environ.get("DEOOS_TEST_BUCKET_PREFIX", "deoos-smoke-") + uuid.uuid4().hex[:20])
    if bucket is not None:
        assert len(bucket) <= 63, "test bucket prefix is too long"
    caller_identity = None
    env = dict(os.environ)
    for key in ("AWS_SESSION_TOKEN", "PYTHONPATH", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY"):
        env.pop(key, None)
    for key in list(env):
        if key.startswith("DEOOS_STORAGE_"):
            env.pop(key)
    storage = None
    s3 = None
    if args.backend == "filesystem":
        for key in list(env):
            if key.startswith("AWS_"):
                env.pop(key)
        storage = tempfile.TemporaryDirectory(prefix="deoos-package-smoke-storage-")
        env.update(DEOOS_STORAGE_PROVIDER="filesystem",
                   DEOOS_STORAGE_DIRECTORY=str(pathlib.Path(storage.name).resolve()))
        assert not any(key.startswith("AWS_") for key in env)
    else:
        import boto3
        from botocore.exceptions import ClientError
    if args.backend == "aws":
        expected_account = os.environ.get("DEOOS_EXPECTED_AWS_ACCOUNT")
        if expected_account is not None and (len(expected_account) != 12 or
                                              not expected_account.isascii() or
                                              not expected_account.isdigit()):
            raise ValueError("DEOOS_EXPECTED_AWS_ACCOUNT must be a 12-digit account ID")
        session = boto3.Session(profile_name=None if args.aws_profile == "auto" else args.aws_profile,
                                region_name="us-east-1")
        s3 = session.client("s3")
        caller_identity = session.client("sts").get_caller_identity()
        if expected_account:
            assert caller_identity["Account"] == expected_account, "unexpected AWS account"
        credentials = session.get_credentials().get_frozen_credentials()
        env.update(AWS_ACCESS_KEY_ID=credentials.access_key,
                   AWS_SECRET_ACCESS_KEY=credentials.secret_key, AWS_REGION="us-east-1")
        if credentials.token:
            env["AWS_SESSION_TOKEN"] = credentials.token
        env.pop("AWS_ENDPOINT", None)
        env.pop("AWS_ALLOW_HTTP", None)
    elif args.backend == "rustfs":
        env.update(AWS_ACCESS_KEY_ID="local-development",
                   AWS_SECRET_ACCESS_KEY="local-development-only-secret",
                   AWS_REGION="us-east-1", AWS_ALLOW_HTTP="true")
        env.pop("AWS_SESSION_TOKEN", None)
        env["AWS_ENDPOINT"] = os.environ.get("AWS_ENDPOINT", "http://127.0.0.1:19000")
        s3 = boto3.client("s3", endpoint_url=env["AWS_ENDPOINT"], region_name="us-east-1",
                          aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"])
    if bucket is not None:
        env["AWS_BUCKET"] = bucket
    report = {"backend": args.backend, "release": str(package), "release_hashes": release_hashes,
              "server_version": server_version, "modes": [], "worker_api": [], "cleaned": False,
              "cleanup_errors": [], "bucket": bucket,
              "storage_directory": env.get("DEOOS_STORAGE_DIRECTORY") if storage else None,
              "aws_identity": {"account": caller_identity["Account"], "arn": caller_identity["Arn"]}
                              if caller_identity else None}
    report_path = ROOT.parent / "outputs" / "evidence" / f"package-smoke-{package.name}-{args.backend}.json"
    # Persist the exact creation intent so CI can clean a timed-out/cancelled test.
    write_report(report_path, report)
    processes = []
    bucket_created = False
    try:
        if s3 is not None:
            s3.create_bucket(Bucket=bucket)
            bucket_created = True
        if args.backend == "aws":
            s3.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={key: True for key in
                    ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")},
            )
        with tempfile.TemporaryDirectory(prefix="deoos-package-smoke-") as scratch:
            work = pathlib.Path(scratch)
            venv.EnvBuilder(with_pip=True, symlinks=os.name != "nt").create(work / "venv")
            python = str(work / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
            npm = shutil.which("npm")
            node = shutil.which("node")
            assert npm and node, "Node.js and npm are required"
            command([python, "-m", "pip", "install", "--no-index", str(wheel)], timeout=120)
            (work / "package.json").write_text('{"private":true,"type":"module"}\n')
            command(npm_args(npm, ["install", "--offline", "--ignore-scripts", "--no-audit",
                                   "--no-fund", str(npm_package)]), cwd=work, timeout=120)
            work_examples = work / "examples"
            work_examples.mkdir()
            for example in examples.iterdir():
                if example.is_file():
                    shutil.copy2(example, work_examples / example.name)
            installed_hashes = installed_artifacts(python, work / "node_modules", env)
            report["installed_hashes"] = installed_hashes
            report["example_hashes"] = {path.name: sha256(path) for path in examples.iterdir()
                                        if path.is_file()}
            try:
                report["runtime"] = {}
                for mode in ("library", "server"):
                    for first, second in (("python", "typescript"), ("typescript", "python")):
                        report["modes"].append(run_workflow_case(
                            mode, first, second, python, node, work_examples, package,
                            env, processes, report["runtime"],
                        ))
                    for language in ("python", "typescript"):
                        report["worker_api"].append(run_worker_case(
                            mode, language, python, node, work, package, env, processes,
                        ))
                if args.backend == "filesystem":
                    from local_storage import run_local_storage
                    report["filesystem_processes"] = run_local_storage(python, env)
            finally:
                for process, label in reversed(processes):
                    try:
                        stop_process(process, label)
                    except Exception as error:
                        report["cleanup_errors"].append(f"{label}: {error}")
    except BaseException as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        for process, label in reversed(processes):
            try:
                stop_process(process, label)
            except Exception as error:
                report["cleanup_errors"].append(f"{label}: {error}")
        if bucket_created:
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    if objects:
                        deleted = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                        if deleted.get("Errors"):
                            raise AssertionError(deleted["Errors"])
                s3.delete_bucket(Bucket=bucket)
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    code = str(error.response.get("Error", {}).get("Code", ""))
                    assert code in ("404", "NoSuchBucket", "NotFound"), error
                else:
                    raise AssertionError("test bucket still exists after deletion")
                report["cleaned"] = True
            except Exception as error:
                report["cleanup_errors"].append(f"bucket: {error}")
        if storage is not None:
            try:
                storage.cleanup()
                assert not pathlib.Path(storage.name).exists(), "test storage still exists"
                report["cleaned"] = True
            except Exception as error:
                report["cleanup_errors"].append(f"filesystem: {error}")
        write_report(report_path, report)
    assert report["cleaned"] and not report["cleanup_errors"], report
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

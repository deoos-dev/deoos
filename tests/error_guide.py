"""Actionable error text against a real deoos-server and RustFS. No mocks.

Each case starts the release binary, or points a real SDK at one, and asserts
the message names the code, the cause, the fix, and https://deoos.dev/errors#CODE.
"""
import json
import os
import pathlib
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "clients" / "python"))
from deoos import Client, EngineError

SERVER = pathlib.Path(os.environ.get(
    "ENGINE_BINARY", str(ROOT / "engine/target/release/deoos-server"))).resolve()
ENDPOINT = "http://127.0.0.1:19000"
ACCESS = "local-development"
SECRET = "local-development-only-secret"
DOCS = ROOT / "docs" / "errors.md"
EVIDENCE = ROOT / "outputs" / "evidence" / "error-guide.json"
RUSTFS = {
    "AWS_ENDPOINT": ENDPOINT,
    "AWS_ACCESS_KEY_ID": ACCESS,
    "AWS_SECRET_ACCESS_KEY": SECRET,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "DEOOS_STORAGE_PROVIDER": "s3",
}


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def assert_guide(text, code, *needles):
    missing = [needle for needle in (
        f"[{code}]", f"https://deoos.dev/errors#{code}", "Cause:", "Fix:", *needles,
    ) if needle not in text]
    if missing:
        raise AssertionError(f"{code} missing {missing} in:\n{text}")
    heading = f"## {code}"
    documented = DOCS.read_text(encoding="utf-8")
    if heading not in documented and f"| {code} |" not in documented:
        raise AssertionError(f"{code} is not listed in docs/errors.md")


class Server:
    def __init__(self, **env):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        child = os.environ.copy()
        for key in ("AWS_SESSION_TOKEN", "ENGINE_TOKEN", "ENGINE_URL", "DEOOS_MODE"):
            child.pop(key, None)
        child.update(RUSTFS)
        child.update(env)
        child["ENGINE_BIND"] = f"127.0.0.1:{self.port}"
        child["LEASE_MS"] = env.get("LEASE_MS", "2000")
        self.log = pathlib.Path(f"/tmp/deoos-error-guide-{self.port}.log")
        self.proc = subprocess.Popen(
            [str(SERVER)], env=child, stdout=self.log.open("w"), stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(self.log.read_text(encoding="utf-8")[-2000:])
            try:
                urllib.request.urlopen(self.url + "/health", timeout=0.5)
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise AssertionError("server did not become healthy\n" + self.log.read_text(encoding="utf-8")[-2000:])

    def stop(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)


def cli_env(url):
    env = os.environ.copy()
    env["ENGINE_URL"] = url
    env["PYTHONPATH"] = str(ROOT / "clients" / "python")
    for name in ("DEOOS_AGENT", "CURSOR_AGENT", "CLAUDECODE", "CODEX", "ENGINE_TOKEN", "DEOOS_MODE"):
        env.pop(name, None)
    return env


def http_body(url, path, payload=None, timeout=12):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url + path, data=data,
        headers={} if data is None else {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(errors="replace")


def node_error(url, source):
    env = os.environ.copy()
    env["ENGINE_URL"] = url
    result = subprocess.run(
        ["node", "--input-type=module", "-e", source],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
    if result.returncode != 0:
        raise AssertionError(f"node failed\n{result.stdout}\n{result.stderr}")
    return json.loads(result.stdout)


def ensure_bucket(name):
    env = os.environ.copy()
    env.update(RUSTFS)
    env["DEOOS_STORAGE_BUCKET"] = name
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/ensure_bucket.py")],
        env=env, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise AssertionError(result.stderr or result.stdout)


def main():
    if not SERVER.is_file():
        raise SystemExit(f"missing {SERVER}; build the release binary first")
    try:
        urllib.request.urlopen(ENDPOINT, timeout=2)
    except urllib.error.HTTPError:
        pass
    except OSError as error:
        raise SystemExit(f"RustFS is not accepting connections at {ENDPOINT}: {error}") from error

    evidence = {"endpoint": ENDPOINT, "cases": {}}
    bucket = "deoos-errors-" + uuid.uuid4().hex[:12]
    ensure_bucket(bucket)
    good = None
    try:
        evidence["cases"]["startup_bucket"] = startup_bucket()
        evidence["cases"]["storage_unreachable"] = storage_unreachable()
        evidence["cases"]["storage_credentials"] = storage_credentials(bucket)
        evidence["cases"]["storage_bucket_missing"] = storage_bucket_missing()
        evidence["cases"]["engine_unreachable"] = engine_unreachable()
        evidence["cases"]["engine_wrong_url"] = engine_wrong_url()
        good = Server(
            DEOOS_STORAGE_BUCKET=bucket,
            EXECUTION_PREFIX="errors-" + uuid.uuid4().hex,
            LEASE_MS="4000")
        evidence["cases"]["protocol_mismatch"] = protocol_mismatch(good)
        evidence["cases"]["task_not_found"] = task_not_found(good)
        evidence["cases"]["workflow_nondeterministic"] = workflow_nondeterministic(good)
    finally:
        if good is not None:
            good.stop()
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"passed": sorted(evidence["cases"]), "evidence": str(EVIDENCE)}, indent=2))


def startup_bucket():
    env = os.environ.copy()
    for key in ("DEOOS_STORAGE_BUCKET", "AWS_BUCKET", "ENGINE_TOKEN"):
        env.pop(key, None)
    env.update(RUSTFS)
    env["DEOOS_STORAGE_PROVIDER"] = "s3"
    result = subprocess.run([str(SERVER)], env=env, capture_output=True, text=True, timeout=8)
    text = (result.stderr or "") + (result.stdout or "")
    if result.returncode == 0 or "panicked" in text:
        raise AssertionError(text[-2000:])
    assert_guide(text, "CONFIG_BUCKET_REQUIRED", "DEOOS_STORAGE_BUCKET", "make dev")
    return {"code": "CONFIG_BUCKET_REQUIRED", "message": text.strip()}


def storage_unreachable():
    closed = free_port()
    server = Server(
        AWS_ENDPOINT=f"http://127.0.0.1:{closed}",
        DEOOS_STORAGE_BUCKET="deoos-dev",
        EXECUTION_PREFIX="unreach-" + uuid.uuid4().hex)
    try:
        client = Client.remote(server.url)
        try:
            client.inspect("probe-task")
            raise AssertionError("unreachable store returned a task")
        except EngineError as error:
            assert error.status == 503, error
            assert error.code == "STORAGE_UNREACHABLE", error
            assert_guide(str(error), "STORAGE_UNREACHABLE", "AWS_ENDPOINT", "make dev", "docker compose up -d")
            message = str(error)
        node = node_error(server.url, """
import {Client, EngineError} from './clients/typescript/dist/index.js';
const client = Client.remote(process.env.ENGINE_URL);
try { await client.inspect('probe-task'); console.log(JSON.stringify({ok:true})); }
catch (error) { console.log(JSON.stringify({status:error.status, code:error.code, message:String(error.message)})); }
""")
        if node.get("code") != "STORAGE_UNREACHABLE":
            raise AssertionError(node)
        assert_guide(node["message"], "STORAGE_UNREACHABLE", "AWS_ENDPOINT")
        return {"code": "STORAGE_UNREACHABLE", "python": message, "typescript": node["message"]}
    finally:
        server.stop()


def storage_credentials(bucket):
    server = Server(
        AWS_SECRET_ACCESS_KEY="not-the-rustfs-secret",
        DEOOS_STORAGE_BUCKET=bucket,
        EXECUTION_PREFIX="creds-" + uuid.uuid4().hex)
    try:
        client = Client.remote(server.url)
        try:
            client.inspect("probe-task")
            raise AssertionError("bad credentials returned a task")
        except EngineError as error:
            assert error.status in (401, 403), error
            assert error.code == "STORAGE_CREDENTIALS", error
            assert_guide(
                str(error), "STORAGE_CREDENTIALS",
                "AWS_ACCESS_KEY_ID", "local-development-only-secret", "compose.yaml")
            assert "not-the-rustfs-secret" not in str(error)
            message = str(error)
        node = node_error(server.url, """
import {Client} from './clients/typescript/dist/index.js';
const client = Client.remote(process.env.ENGINE_URL);
try { await client.inspect('probe-task'); console.log(JSON.stringify({ok:true})); }
catch (error) { console.log(JSON.stringify({status:error.status, code:error.code, message:String(error.message)})); }
""")
        if node.get("code") != "STORAGE_CREDENTIALS":
            raise AssertionError(node)
        return {"code": "STORAGE_CREDENTIALS", "python": message, "typescript": node["message"]}
    finally:
        server.stop()


def storage_bucket_missing():
    missing = "missing-" + uuid.uuid4().hex[:16]
    server = Server(
        DEOOS_STORAGE_BUCKET=missing,
        EXECUTION_PREFIX="bucket-" + uuid.uuid4().hex)
    try:
        client = Client.remote(server.url)
        try:
            client.list_tasks()
            raise AssertionError("missing bucket listed tasks")
        except EngineError as error:
            assert error.status == 400, error
            assert error.code == "STORAGE_BUCKET_MISSING", error
            assert_guide(
                str(error), "STORAGE_BUCKET_MISSING",
                "DEOOS_STORAGE_BUCKET", "ensure_bucket.py", missing)
            message = str(error)
        status, body = http_body(server.url, "/tasks/probe-task")
        assert status == 400 and "STORAGE_BUCKET_MISSING" in body and "task '" not in body, (status, body)
        cli = subprocess.run(
            [sys.executable, "-m", "deoos", "list"],
            cwd=ROOT,
            env=cli_env(server.url),
            capture_output=True, text=True, timeout=15)
        if cli.returncode == 0 or "STORAGE_BUCKET_MISSING" not in cli.stderr:
            raise AssertionError(f"cli\n{cli.stdout}\n{cli.stderr}")
        return {"code": "STORAGE_BUCKET_MISSING", "python": message, "cli": cli.stderr.strip(), "http": body}
    finally:
        server.stop()


def engine_unreachable():
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    try:
        Client.remote(url).request("/info")
        raise AssertionError("closed port accepted a request")
    except OSError as error:
        assert_guide(str(error), "ENGINE_UNREACHABLE", "cannot reach engine", "make dev", "ENGINE_URL")
        message = str(error)
    node = node_error(url, """
import {Client} from './clients/typescript/dist/index.js';
try { await Client.remote(process.env.ENGINE_URL).request('/info'); console.log(JSON.stringify({ok:true})); }
catch (error) { console.log(JSON.stringify({message:String(error.message)})); }
""")
    assert_guide(node["message"], "ENGINE_UNREACHABLE", "cannot reach engine", "make dev")
    cli = subprocess.run(
        [sys.executable, "-m", "deoos", "info"],
        cwd=ROOT,
        env=cli_env(url),
        capture_output=True, text=True, timeout=15)
    if cli.returncode != 1 or cli.stderr.strip() != message:
        raise AssertionError(f"cli\n{cli.stdout!r}\n{cli.stderr!r}\nexpected {message!r}")
    return {"code": "ENGINE_UNREACHABLE", "python": message, "typescript": node["message"], "cli": cli.stderr.strip()}


def engine_wrong_url():
    url = ENDPOINT
    try:
        Client.remote(url).request("/info")
        raise AssertionError("RustFS was accepted as a deoos server")
    except EngineError as error:
        assert error.code == "ENGINE_WRONG_URL", error
        assert_guide(str(error), "ENGINE_WRONG_URL", "19000", "ENGINE_URL", "deoos-server listening")
        assert "<Error>" not in str(error)
        message = str(error)
    node = node_error(url, """
import {Client, EngineError} from './clients/typescript/dist/index.js';
try { await Client.remote(process.env.ENGINE_URL).request('/info'); console.log(JSON.stringify({ok:true})); }
catch (error) { console.log(JSON.stringify({name:error.name, code:error.code, message:String(error.message)})); }
""")
    if node.get("code") != "ENGINE_WRONG_URL":
        raise AssertionError(node)
    assert_guide(node["message"], "ENGINE_WRONG_URL", "ENGINE_URL")
    return {"code": "ENGINE_WRONG_URL", "python": message, "typescript": node["message"]}


def protocol_mismatch(server):
    recorded = {}
    for version in (None, 1, 2):
        payload = {"id": "old-client", "handler": "old", "inputs": {}}
        if version is not None:
            payload["protocol_version"] = version
        status, body = http_body(server.url, "/tasks", payload)
        if status != 409:
            raise AssertionError((version, status, body))
        error = EngineError(status, body)
        if error.code != "PROTOCOL_MISMATCH":
            raise AssertionError((version, error))
        assert_guide(
            str(error), "PROTOCOL_MISMATCH",
            "protocol_version 3", "deoos-server --version", "PyPI", "npm")
        recorded[str(version)] = body
    node = node_error(server.url, """
import {EngineError} from './clients/typescript/dist/index.js';
const response = await fetch(process.env.ENGINE_URL + '/tasks', {
  method: 'POST', headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({id:'old-ts', handler:'old', inputs:{}, protocol_version: 1})});
const detail = (await response.text()).trim();
const error = new EngineError(response.status, detail);
if (error.code !== 'PROTOCOL_MISMATCH') throw new Error(detail);
console.log(JSON.stringify({status:error.status, code:error.code, message:error.message}));
""")
    assert_guide(node["message"], "PROTOCOL_MISMATCH", "GitHub release")
    # A current SDK still talks to this server.
    client = Client.remote(server.url)
    created = client.submit("proto-ok-" + uuid.uuid4().hex[:8], "proto", {})
    if created["status"] != "queued":
        raise AssertionError(created)
    return {"code": "PROTOCOL_MISMATCH", "http": recorded, "typescript": node["message"]}


def task_not_found(server):
    client = Client.remote(server.url)
    try:
        client.inspect("missing-error-task")
        raise AssertionError("missing task was found")
    except EngineError as error:
        assert error.status == 404, error
        assert error.code == "TASK_NOT_FOUND", error
        assert_guide(str(error), "TASK_NOT_FOUND", "not found", "python -m deoos list")
        return {"code": "TASK_NOT_FOUND", "python": str(error)}


def workflow_nondeterministic(server):
    task_id = "nd-" + uuid.uuid4().hex[:8]
    client = Client.remote(server.url)
    client.submit(task_id, "flow", {})

    def first(ctx, inputs):
        ctx.step("work", lambda: "one", revision="1")
        ctx.wait_signal("go")
        return "done"

    stop = threading.Event()
    box = []

    def worker():
        try:
            client.run_worker({"flow": first}, stop_event=stop, poll_interval=0.1)
        except Exception as error:
            box.append(error)

    thread = threading.Thread(target=worker)
    thread.start()
    deadline = time.monotonic() + 15
    view = None
    while time.monotonic() < deadline:
        if box:
            break
        view = client.view(task_id)
        if view["status"] == "waiting" and "work" in view["completed_steps"]:
            break
        time.sleep(0.1)
    stop.set()
    thread.join(timeout=5)
    if box or view is None or view["status"] != "waiting":
        raise AssertionError({"errors": [str(error) for error in box], "view": view})
    client.signal(task_id, "go", True)

    def changed(ctx, inputs):
        ctx.step("work", lambda: "two", revision="2")
        return "done"

    try:
        client.run_once({"flow": changed})
        raise AssertionError("changed revision was accepted")
    except EngineError as error:
        assert error.status == 409, error
        assert error.code == "WORKFLOW_NONDETERMINISTIC", error
        assert_guide(
            str(error), "WORKFLOW_NONDETERMINISTIC",
            "checkpoint definition changed", "step 'work'", "python -m deoos submit")
        message = str(error)

    # TypeScript hits the same committed definition on a second task.
    task_ts = "ndts-" + uuid.uuid4().hex[:8]
    client.submit(task_ts, "flow", {})
    node = node_error(server.url, f"""
import {{Client, EngineError}} from './clients/typescript/dist/index.js';
const client = Client.remote(process.env.ENGINE_URL);
const id = {json.dumps(task_ts)};
const stop = new AbortController();
const worker = client.runWorker({{flow: async (ctx) => {{
  await ctx.step('work', () => 'one', '1');
  await ctx.waitSignal('go');
  return 'done';
}}}}, {{signal: stop.signal, pollIntervalMs: 100}});
const started = Date.now();
let view;
while (Date.now() - started < 15000) {{
  view = await client.view(id);
  if (view.status === 'waiting' && view.completed_steps.includes('work')) break;
  await new Promise(resolve => setTimeout(resolve, 100));
}}
stop.abort();
await worker;
await client.signal(id, 'go', true);
try {{
  await client.runOnce({{flow: async (ctx) => ctx.step('work', () => 'two', '2')}});
  console.log(JSON.stringify({{ok:true, view}}));
}} catch (error) {{
  console.log(JSON.stringify({{status:error.status, code:error.code, message:String(error.message)}}));
}}
""")
    if node.get("code") != "WORKFLOW_NONDETERMINISTIC":
        raise AssertionError(node)
    assert_guide(node["message"], "WORKFLOW_NONDETERMINISTIC", "checkpoint definition changed", "step 'work'")
    return {"code": "WORKFLOW_NONDETERMINISTIC", "python": message, "typescript": node["message"]}


if __name__ == "__main__":
    main()

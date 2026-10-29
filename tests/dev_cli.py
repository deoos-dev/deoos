"""Drive python -m deoos against a real `make dev` server.

Starts RustFS and deoos-server the way `make dev` does, submits a workflow,
kills the worker mid-step, and checks that a new worker resumes it. Signals
and history go through the CLI. No mocks.
"""
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHONPATH = str(ROOT / "clients" / "python")
ENGINE_URL = "http://127.0.0.1:7331"
TASK = "resume-" + uuid.uuid4().hex[:8]
WORKER = r"""
import os, threading, time
from deoos import Client

def slow(ctx, inputs):
    marker = inputs["marker"]

    def work():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("work\n")
        return "work"

    def hold():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("hold\n")
        if open(marker, encoding="utf-8").read().count("hold\n") == 1:
            time.sleep(30)
        return "held"

    ctx.step("work", work)
    ctx.step("hold", hold)
    ctx.wait_signal("go")
    return ctx.step("after", lambda: "done")

Client.remote(os.environ["ENGINE_URL"]).run_worker(
    {"slow": slow}, stop_event=threading.Event(), poll_interval=0.2)
"""


def cli(env, *args, check=True):
    result = subprocess.run(
        [sys.executable, "-m", "deoos", *args],
        cwd=ROOT, env=env, capture_output=True, text=True,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"{args} failed ({result.returncode})\n{result.stdout}\n{result.stderr}")
    return result


def cli_json(env, *args):
    result = cli(env, *args)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(f"{args} did not print JSON\n{result.stdout}") from error


def dev_env():
    env = os.environ.copy()
    env["LEASE_MS"] = "2000"
    for name in ("ENGINE_URL", "ENGINE_BIND", "ENGINE_TOKEN", "AWS_ENDPOINT",
                 "AWS_SESSION_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                 "DEOOS_STORAGE_BUCKET", "DEOOS_MODE", "EXECUTION_PREFIX"):
        env.pop(name, None)
    return env


def client_env():
    env = os.environ.copy()
    env["ENGINE_URL"] = ENGINE_URL
    env["PYTHONPATH"] = PYTHONPATH
    env.pop("ENGINE_TOKEN", None)
    env.pop("DEOOS_MODE", None)
    return env


def native_library():
    name = {
        "Linux": "libdeoos_engine.so",
        "Darwin": "libdeoos_engine.dylib",
        "Windows": "deoos_engine.dll",
    }[platform.system()]
    path = ROOT / "engine" / "target" / "release" / name
    if not path.is_file():
        subprocess.run(
            ["cargo", "build", "--locked", "--release",
             "--manifest-path", str(ROOT / "engine" / "Cargo.toml"), "--lib"],
            cwd=ROOT, check=True,
        )
    if not path.is_file():
        raise AssertionError(f"missing native library {path}")
    return path


def library_env():
    env = client_env()
    env.update(
        DEOOS_MODE="library",
        ENGINE_URL="http://127.0.0.1:9",
        DEOOS_STORAGE_PROVIDER="s3",
        DEOOS_STORAGE_BUCKET="deoos-dev",
        AWS_ACCESS_KEY_ID="local-development",
        AWS_SECRET_ACCESS_KEY="local-development-only-secret",
        AWS_REGION="us-east-1",
        AWS_ENDPOINT="http://127.0.0.1:19000",
        AWS_ALLOW_HTTP="true",
        AWS_EC2_METADATA_DISABLED="true",
        EXECUTION_PREFIX="deoos-library-mode",
        DEOOS_NATIVE_LIBRARY=str(native_library()),
    )
    return env


def health():
    try:
        with urllib.request.urlopen(ENGINE_URL + "/health", timeout=1) as response:
            return response.status == 200
    except OSError:
        return False


def wait_for(label, predicate, timeout):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {label}: {last!r}")


def marker_count(path, text):
    if not path.exists():
        return 0
    return path.read_text(encoding="utf-8").count(text)


def stop_dev(proc):
    subprocess.run(["make", "dev-down"], cwd=ROOT, env=dev_env(), check=False)
    if proc is not None and proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=5)


def refuse_connection():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    env = client_env()
    env["ENGINE_URL"] = f"http://127.0.0.1:{port}"
    result = cli(env, "info", check=False)
    expected = f"cannot reach engine at http://127.0.0.1:{port}; run make dev\n"
    if result.returncode != 1 or result.stderr != expected or result.stdout:
        raise AssertionError(
            f"refused connection was not a short error\n"
            f"code={result.returncode}\nstdout={result.stdout!r}\nstderr={result.stderr!r}")


def main():
    refuse_connection()
    proc = None
    worker = None
    log = None
    try:
        subprocess.run(["make", "dev-down"], cwd=ROOT, env=dev_env(), check=False)
        if health():
            raise AssertionError("port 7331 is still open after make dev-down")
        (ROOT / "outputs").mkdir(exist_ok=True)
        env_file = ROOT / "outputs" / "dev.env"
        env_file.unlink(missing_ok=True)
        log_path = ROOT / "outputs" / "dev-cli.log"
        log = log_path.open("w", encoding="utf-8")
        proc = subprocess.Popen(
            ["make", "dev"], cwd=ROOT, env=dev_env(),
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )

        def ready():
            if proc.poll() is not None:
                raise AssertionError(log_path.read_text(encoding="utf-8"))
            if not env_file.exists() or "export LEASE_MS='2000'" not in env_file.read_text():
                return False
            return health()

        wait_for("make dev", ready, 180)
        text = env_file.read_text(encoding="utf-8")
        if "export ENGINE_URL='http://127.0.0.1:7331'" not in text:
            raise AssertionError(text)
        if "export DEOOS_STORAGE_BUCKET='deoos-dev'" not in text:
            raise AssertionError(text)
        plain = dev_env()
        plain.pop("LEASE_MS", None)
        again = subprocess.run(
            ["make", "dev"], cwd=ROOT, env=plain, capture_output=True, text=True, timeout=30)
        if again.returncode != 0 or "not restarted" not in again.stderr:
            raise AssertionError(f"{again.returncode}\n{again.stdout}\n{again.stderr}")
        if "export LEASE_MS='2000'" not in again.stdout:
            raise AssertionError(again.stdout)

        env = client_env()
        info = cli_json(env, "info")
        if info.get("protocol_version") != 3:
            raise AssertionError(info)
        server_mode = client_env()
        server_mode["DEOOS_MODE"] = "server"
        if cli_json(server_mode, "info").get("protocol_version") != 3:
            raise AssertionError("DEOOS_MODE=server did not reach the dev server")
        missing_url = client_env()
        missing_url["DEOOS_MODE"] = "server"
        missing_url.pop("ENGINE_URL", None)
        required = cli(missing_url, "info", check=False)
        if required.returncode != 1 or "DEOOS_MODE=server requires ENGINE_URL" not in required.stderr:
            raise AssertionError(f"{required.returncode}\n{required.stdout!r}\n{required.stderr!r}")
        invalid = client_env()
        invalid["DEOOS_MODE"] = "nope"
        rejected = cli(invalid, "info", check=False)
        if rejected.returncode != 1 or "DEOOS_MODE must be 'library' or 'server'" not in rejected.stderr:
            raise AssertionError(f"{rejected.returncode}\n{rejected.stderr!r}")
        ignored = cli(library_env(), "info", check=False)
        if ignored.returncode != 0 or "DEOOS_MODE=library ignores ENGINE_URL" not in ignored.stderr:
            raise AssertionError(
                f"library mode did not ignore ENGINE_URL\n"
                f"code={ignored.returncode}\nstdout={ignored.stdout!r}\nstderr={ignored.stderr!r}")
        library_info = json.loads(ignored.stdout)
        if library_info.get("protocol_version") != 3:
            raise AssertionError(library_info)
        bad_inputs = cli(env, "submit", "bad-json", "demo.order.v1", "not-json", check=False)
        if bad_inputs.returncode != 1 or "inputs must be JSON" not in bad_inputs.stderr or "Traceback" in bad_inputs.stderr:
            raise AssertionError(f"{bad_inputs.returncode}\n{bad_inputs.stderr!r}")
        bad_signal = cli(env, "signal", "missing-task", "go", "not-json", check=False)
        if bad_signal.returncode != 1 or "value must be JSON" not in bad_signal.stderr:
            raise AssertionError(f"{bad_signal.returncode}\n{bad_signal.stderr!r}")
        missing = cli(env, "inspect", "missing-task", check=False)
        if missing.returncode != 1 or "cannot reach engine" in missing.stderr or "404" not in missing.stderr:
            raise AssertionError(f"expected a 404, got {missing.stderr!r}")
        if "Traceback" in missing.stderr:
            raise AssertionError(missing.stderr)

        with tempfile.TemporaryDirectory() as scratch:
            marker = Path(scratch) / "marker.txt"
            submitted = cli_json(
                env, "submit", TASK, "slow", json.dumps({"marker": str(marker)}))
            if submitted["id"] != TASK or submitted["status"] != "queued" or submitted["function"] != "slow":
                raise AssertionError(submitted)
            worker_path = Path(scratch) / "worker.py"
            worker_path.write_text(WORKER, encoding="utf-8")
            worker_log = Path(scratch) / "worker.log"

            def start_worker():
                handle = worker_log.open("a", encoding="utf-8")
                process = subprocess.Popen(
                    [sys.executable, str(worker_path)],
                    cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT,
                )
                return process, handle

            worker, worker_handle = start_worker()

            def running_in_hold():
                if worker.poll() is not None:
                    raise AssertionError(worker_log.read_text(encoding="utf-8"))
                view = cli_json(env, "inspect", TASK)
                if view["status"] == "running" and "work" in view["completed_steps"] and "hold" not in view["completed_steps"]:
                    if marker_count(marker, "hold\n") >= 1:
                        return view
                return None

            first = wait_for("running inside hold", running_in_hold, 20)
            if first["attempts"] != 1 or marker_count(marker, "work\n") != 1:
                raise AssertionError((first, marker.read_text(encoding="utf-8")))
            worker.kill()
            worker.wait(timeout=5)
            if worker.returncode != -signal.SIGKILL:
                raise AssertionError(worker.returncode)
            worker_handle.close()
            worker = None

            worker, worker_handle = start_worker()

            def waiting():
                if worker.poll() is not None:
                    raise AssertionError(worker_log.read_text(encoding="utf-8"))
                view = cli_json(env, "inspect", TASK)
                if view["status"] == "waiting" and view["completed_steps"] == ["hold", "work"]:
                    return view
                return None

            paused = wait_for("waiting for go", waiting, 20)
            if paused["attempts"] != 2 or paused["wait"] != {"kind": "signal", "name": "go", "assigned": False}:
                raise AssertionError(paused)
            if marker_count(marker, "work\n") != 1 or marker_count(marker, "hold\n") != 2:
                raise AssertionError(marker.read_text(encoding="utf-8"))

            signaled = cli_json(env, "signal", TASK, "go", "true")
            if signaled["id"] != TASK or "hold" not in signaled["completed_steps"]:
                raise AssertionError(signaled)

            def completed():
                if worker.poll() is not None:
                    raise AssertionError(worker_log.read_text(encoding="utf-8"))
                view = cli_json(env, "inspect", TASK)
                if view["status"] == "completed":
                    return view
                return None

            done = wait_for("completed", completed, 20)
            if done["output"] != "done" or done["completed_steps"] != ["after", "go", "hold", "work"]:
                raise AssertionError(done)
            if marker_count(marker, "work\n") != 1:
                raise AssertionError(marker.read_text(encoding="utf-8"))
            history = cli_json(env, "history", TASK)
            events = [item["event"] for item in history]
            for name in ("submit", "claim", "checkpoint", "suspend", "signal", "resume", "complete"):
                if name not in events:
                    raise AssertionError(history)
            if events.count("claim") < 1 or events.count("signal") != 1:
                raise AssertionError(history)
            worker.kill()
            worker.wait(timeout=5)
            worker_handle.close()
            worker = None
        print(json.dumps({"ok": True, "task": TASK, "events": events}))
    finally:
        if worker is not None and worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        stop_dev(proc)
        if log is not None:
            log.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log_path = ROOT / "outputs" / "dev-cli.log"
        if log_path.exists():
            sys.stderr.write("\n--- make dev log ---\n")
            sys.stderr.write(log_path.read_text(encoding="utf-8")[-8000:])
        raise

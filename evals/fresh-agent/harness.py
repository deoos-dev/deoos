"""Score a fresh agent on one durable-workflow task against a real engine.

The agent sees an empty project plus the public docs, llms.txt, and the skill.
The grader then runs app/workflow.py on a real deoos server. RustFS is the
storage backend. Nothing here is mocked.

  python evals/fresh-agent/harness.py --self-check --binary engine/target/release/deoos-server
  python evals/fresh-agent/harness.py --prepare /tmp/deoos-eval
  python evals/fresh-agent/harness.py --score /tmp/deoos-eval --binary engine/target/release/deoos-server
  DEOOS_EVAL_DRIVER='python evals/fresh-agent/drivers/openai_compatible.py' \\
    python evals/fresh-agent/harness.py --run --binary engine/target/release/deoos-server

--self-check writes the skill's scaffold and grades it. That is the CI path.
--run prepares a sandbox, runs DEOOS_EVAL_DRIVER or --driver, then grades the
file the driver wrote. See README.md for a real model.
"""
import argparse
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
PUBLIC = (
    "llms.txt",
    "README.md",
    "agents/SKILL.md",
    "docs/agent-feedback.md",
    "agents/scripts/check_engine.py",
    "agents/scripts/debug_run.py",
    "agents/scripts/scaffold_workflow.py",
)
PROMPT = ROOT / "evals" / "fresh-agent" / "tasks" / "crash_resume.md"


class Engine:
    def __init__(self, binary, backend):
        self.binary = pathlib.Path(binary)
        self.backend = backend
        self.work = pathlib.Path(tempfile.mkdtemp(prefix="deoos-eval-"))
        self.bucket = None
        self.s3 = None
        self.process = None
        self.log = None
        self.url = None

    def start(self):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("AWS_") and key not in {"ENGINE_URL", "ENGINE_TOKEN", "ENGINE_BIND"}}
        env.update(
            AWS_REGION="us-east-1",
            AWS_ALLOW_HTTP="true",
            AWS_ACCESS_KEY_ID="local-development",
            AWS_SECRET_ACCESS_KEY="local-development-only-secret",
            AWS_EC2_METADATA_DISABLED="true",
            LEASE_MS="2000",
            EXECUTION_PREFIX="eval-" + uuid.uuid4().hex,
        )
        if self.backend == "rustfs":
            import boto3
            endpoint = os.environ.get("AWS_ENDPOINT", "http://127.0.0.1:19000")
            if not (endpoint.startswith("http://127.0.0.1:") or endpoint.startswith("http://localhost:")):
                raise SystemExit("RustFS endpoint must be local")
            self.bucket = "deoos-eval-" + uuid.uuid4().hex
            env.update(AWS_ENDPOINT=endpoint, DEOOS_STORAGE_PROVIDER="s3", DEOOS_STORAGE_BUCKET=self.bucket)
            self.s3 = boto3.client(
                "s3", endpoint_url=endpoint, region_name="us-east-1",
                aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
            )
            self.s3.create_bucket(Bucket=self.bucket)
        else:
            env.update(DEOOS_STORAGE_PROVIDER="filesystem", DEOOS_STORAGE_DIRECTORY=str(self.work / "storage"))
        with socket.socket() as candidate:
            candidate.bind(("127.0.0.1", 0))
            port = candidate.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        self.log = (self.work / "engine.log").open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [str(self.binary)], env=dict(env, ENGINE_BIND=f"127.0.0.1:{port}"),
            stdout=self.log, stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(self.log_text())
            try:
                with urllib.request.urlopen(self.url + "/health", timeout=1) as response:
                    if response.status == 200:
                        return self
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("engine did not become healthy\n" + self.log_text())

    def log_text(self):
        try:
            return (self.work / "engine.log").read_text(encoding="utf-8")[-4000:]
        except OSError:
            return ""

    def close(self):
        self.cleanup_errors = []
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.log is not None:
            self.log.close()
        if self.s3 is not None and self.bucket:
            try:
                for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
                    objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    if objects:
                        self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})
                self.s3.delete_bucket(Bucket=self.bucket)
            except Exception as error:
                self.cleanup_errors.append(f"{type(error).__name__}: {error}")
        shutil.rmtree(self.work, ignore_errors=True)


def prepare(destination):
    destination = pathlib.Path(destination)
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    for relative in PUBLIC:
        source = ROOT / relative
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    (destination / "app").mkdir()
    prompt = destination / "PROMPT.md"
    prompt.write_text(PROMPT.read_text(encoding="utf-8"), encoding="utf-8")
    return prompt


def _view(client, task_id):
    from deoos import EngineError
    try:
        return client.view(task_id)
    except EngineError as error:
        if error.status == 404:
            return None
        raise


def _tail(path, limit=40):
    if not path.is_file():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-limit:])


def _start_worker(workflow, env, log_path):
    log = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(workflow)], env=env, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return process, log


def _stop_worker(process, log):
    if process.poll() is None:
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
    log.close()


def grade(workflow, engine_url):
    sys.path.insert(0, str(ROOT / "clients" / "python"))
    from deoos import Client
    workflow = pathlib.Path(workflow)
    errors = []
    checks = {}
    source = workflow.read_text(encoding="utf-8") if workflow.is_file() else ""
    checks["uses_deoos"] = "deoos" in source
    if not checks["uses_deoos"]:
        errors.append("app/workflow.py does not reference deoos")
    task_id = "crash-" + uuid.uuid4().hex[:12]
    marker_dir = pathlib.Path(tempfile.mkdtemp(prefix="deoos-marker-"))
    marker = marker_dir / "marker.txt"
    logs = marker_dir / "logs"
    logs.mkdir()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("AWS_") and key not in {"GITHUB_TOKEN", "GH_TOKEN", "ENGINE_TOKEN"}}
    env.update(
        ENGINE_URL=engine_url,
        PYTHONPATH=str(ROOT / "clients" / "python"),
        DEOOS_MODE="server",
        DEOOS_TASK_ID=task_id,
        DEOOS_MARKER=str(marker),
        DEOOS_HOLD_SECONDS="30",
    )
    client = Client.remote(engine_url)
    final = None
    try:
        if not checks["uses_deoos"]:
            return _result(task_id, checks, errors, None, "")
        first, first_log = _start_worker(workflow, env, logs / "first.txt")
        try:
            deadline = time.monotonic() + 40
            ready = False
            while time.monotonic() < deadline:
                if first.poll() is not None:
                    errors.append(f"worker exited {first.returncode} before hold was running")
                    break
                text = marker.read_text(encoding="utf-8") if marker.exists() else ""
                view = _view(client, task_id)
                steps = set((view or {}).get("completed_steps") or [])
                if text.count("work\n") >= 1 and text.count("hold\n") >= 1 and "work" in steps and "hold" not in steps:
                    ready = True
                    break
                time.sleep(0.2)
            else:
                errors.append("timed out waiting for an uncommitted hold step")
            if ready:
                _stop_worker(first, first_log)
                first = None
                text = marker.read_text(encoding="utf-8")
                checks["work_once_before_restart"] = text.count("work\n") == 1
                checks["hold_started"] = text.count("hold\n") == 1
                if not checks["work_once_before_restart"]:
                    errors.append(f"work lines before restart: {text.count('work' + chr(10))}")
                if not checks["hold_started"]:
                    errors.append(f"hold lines before restart: {text.count('hold' + chr(10))}")
                second, second_log = _start_worker(workflow, env, logs / "second.txt")
                try:
                    deadline = time.monotonic() + 40
                    while time.monotonic() < deadline:
                        if second.poll() is not None:
                            errors.append(f"second worker exited {second.returncode} before completion")
                            break
                        view = _view(client, task_id)
                        if view and view.get("status") in ("completed", "failed", "cancelled"):
                            final = view
                            break
                        time.sleep(0.2)
                    else:
                        errors.append("timed out waiting for completion after restart")
                        final = _view(client, task_id)
                finally:
                    _stop_worker(second, second_log)
                text = marker.read_text(encoding="utf-8") if marker.exists() else ""
                checks["work_not_repeated"] = text.count("work\n") == 1
                checks["hold_retried"] = text.count("hold\n") == 2
                checks["completed"] = bool(
                    final and final.get("status") == "completed" and final.get("output") == "done")
                checks["steps"] = bool(final and {"work", "hold"} <= set(final.get("completed_steps") or []))
                if not checks["work_not_repeated"]:
                    errors.append("work ran more than once; the step was not durable")
                if not checks["hold_retried"]:
                    errors.append(f"hold lines after restart: {text.count('hold' + chr(10))}")
                if not checks["completed"]:
                    errors.append(f"final status: {(final or {}).get('status')} output: {(final or {}).get('output')}")
                if not checks["steps"]:
                    errors.append(f"completed steps: {(final or {}).get('completed_steps')}")
        finally:
            if first is not None:
                _stop_worker(first, first_log)
        tails = "\n".join(part for part in (
            _tail(logs / "first.txt"), _tail(logs / "second.txt")) if part)
        return _result(task_id, checks, errors, final, tails)
    finally:
        client.close()
        shutil.rmtree(marker_dir, ignore_errors=True)


def _result(task_id, checks, errors, final, logs):
    public = None
    if isinstance(final, dict):
        public = {key: final.get(key) for key in (
            "id", "status", "output", "error", "attempts", "completed_steps", "function")}
    return {
        "task": "crash-resume",
        "task_id": task_id,
        "success": not errors and all(checks.values()),
        "checks": checks,
        "errors": errors,
        "inspect": public,
        "worker_log_tail": logs,
    }


def run_driver(command, workspace, timeout):
    env = os.environ.copy()
    env["DEOOS_EVAL_WORKSPACE"] = str(workspace)
    env["DEOOS_EVAL_PROMPT"] = str(workspace / "PROMPT.md")
    started = time.monotonic()
    completed = subprocess.run(
        command, shell=True, cwd=workspace, env=env, text=True,
        capture_output=True, timeout=timeout,
    )
    return {
        "command": command,
        "seconds": round(time.monotonic() - started, 3),
        "exit_code": completed.returncode,
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
    }


def execute(args):
    started = time.monotonic()
    workspace = None
    driver = None
    if args.self_check:
        workspace = pathlib.Path(tempfile.mkdtemp(prefix="deoos-self-check-"))
        prepare(workspace)
        subprocess.run(
            [sys.executable, str(ROOT / "agents" / "scripts" / "scaffold_workflow.py"), str(workspace / "app")],
            check=True, text=True,
        )
        driver_name = "self-check"
    elif args.prepare:
        prepare(pathlib.Path(args.prepare))
        print(args.prepare)
        return 0
    else:
        workspace = pathlib.Path(args.score or args.run)
        if args.run:
            if workspace.exists():
                shutil.rmtree(workspace)
            prepare(workspace)
            command = args.driver or os.environ.get("DEOOS_EVAL_DRIVER")
            if not command:
                raise SystemExit("Set --driver or DEOOS_EVAL_DRIVER. See evals/fresh-agent/README.md.")
            driver = run_driver(command, workspace, args.driver_timeout)
            driver_name = "external"
        else:
            driver_name = "score"
    workflow = workspace / "app" / "workflow.py"
    if not workflow.is_file():
        report = {
            "task": "crash-resume", "success": False, "seconds": round(time.monotonic() - started, 3),
            "errors": ["app/workflow.py was not written"], "driver": driver, "mode": driver_name,
        }
        _write(args.report, report)
        print(json.dumps(report, indent=2))
        return 1
    engine = Engine(args.binary, args.backend)
    try:
        engine.start()
        graded = grade(workflow, engine.url)
    finally:
        engine.close()
    graded["seconds"] = round(time.monotonic() - started, 3)
    graded["mode"] = driver_name
    graded["backend"] = args.backend
    graded["driver"] = driver
    graded["sandbox_kept"] = bool(args.keep)
    if engine.cleanup_errors:
        graded["cleanup_errors"] = engine.cleanup_errors
    if not args.keep and args.self_check:
        shutil.rmtree(workspace, ignore_errors=True)
    _write(args.report, graded)
    print(json.dumps(graded, indent=2))
    return 0 if graded["success"] else 1


def _write(path, report):
    if not path:
        return
    destination = pathlib.Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-check", action="store_true", help="grade the skill scaffold against a real engine")
    mode.add_argument("--prepare", help="write a fresh-agent sandbox and exit")
    mode.add_argument("--score", help="grade app/workflow.py in this sandbox")
    mode.add_argument("--run", help="prepare this sandbox, run a driver, then grade it")
    parser.add_argument("--binary", type=pathlib.Path, help="deoos-server binary")
    parser.add_argument("--backend", choices=("rustfs", "filesystem"), default="rustfs")
    parser.add_argument("--driver", help="shell command that writes app/workflow.py; defaults to DEOOS_EVAL_DRIVER")
    parser.add_argument("--driver-timeout", type=int, default=900)
    parser.add_argument("--report", type=pathlib.Path)
    parser.add_argument("--keep", action="store_true", help="keep the sandbox directory")
    args = parser.parse_args()
    if not args.prepare and (args.binary is None or not args.binary.is_file()):
        parser.error("--binary must be the deoos-server executable")
    try:
        return execute(args)
    except Exception as error:
        report = {"task": "crash-resume", "success": False, "errors": [f"{type(error).__name__}: {error}"]}
        _write(args.report, report)
        print(json.dumps(report, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

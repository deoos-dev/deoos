"""Agent resume: saved model calls stay saved, check runs twice, and the charge key does not."""
import argparse
import collections
import http.server
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
import paths
from agent_run import HANDLER
from engine_server import server_worker_env, start_deoos_server, stop_process, wait_for_rustfs

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "python" / "agent-run-resume" / "agent_run.py"
TS = ROOT / "typescript" / "agent-run-resume" / "agent_run.mjs"
TOKENS = {"plan": 120, "research": 340, "draft": 560, "check": 80, "publish": 40}


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def tail(path):
    if not path.exists():
        return ""
    return "\n".join(path.read_text(errors="replace").splitlines()[-40:])


def wait_until(predicate, timeout, process=None, log_path=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"worker exited {process.returncode}\n{tail(log_path)}")
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError(f"timed out\n{tail(log_path) if log_path else ''}")


def stop(process):
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)


class AgentService:
    """Canned model and tool responses. The first /charge is recorded, then held past the client timeout."""

    def __init__(self):
        self.llm_calls, self.tool_calls, self.charge_calls, self.effects = [], [], [], {}
        self.check_tools = 0
        self.lock = threading.Lock()
        service = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                step = body.get("step")
                if self.path == "/llm":
                    if step not in TOKENS:
                        self.send_error(400)
                        return
                    with service.lock:
                        service.llm_calls.append({"step": step, "tokens": TOKENS[step]})
                    result = {"step": step, "tokens": TOKENS[step], "text": f"canned {step}"}
                    delay = 0
                elif self.path == "/tool":
                    with service.lock:
                        service.tool_calls.append({"step": step})
                        fail = step == "check" and service.check_tools == 0
                        if step == "check":
                            service.check_tools += 1
                    if fail:
                        result = {"detail": "checker unavailable", "ok": False, "step": step, "error": "checker unavailable"}
                    else:
                        result = {"detail": f"canned {step} tool", "ok": True, "step": step}
                    delay = 0
                elif self.path == "/charge":
                    key = self.headers.get("Idempotency-Key")
                    if not key or key != body.get("idempotency_key"):
                        self.send_error(400)
                        return
                    with service.lock:
                        service.charge_calls.append({"key": key, "body": body})
                        cached = service.effects.get(key)
                        delay = 0
                        if cached:
                            if cached["body"] != body:
                                self.send_error(409)
                                return
                            result = cached["result"]
                        else:
                            result = {"id": "charge-1", "step": step}
                            service.effects[key] = {"body": body, "result": result}
                            delay = 5
                    if delay:
                        time.sleep(delay)
                else:
                    self.send_error(404)
                    return
                raw = json.dumps(result).encode()
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError):
                    return

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def credits(calls):
    """Durable cost is the ledger. The same crashes, started over each time, pay the prefix again."""
    prices = {}
    for call in calls:
        prices.setdefault(call["step"], call["tokens"])
        check(call["tokens"] == prices[call["step"]], f"{call['step']} token price changed")
    with_deoos = sum(call["tokens"] for call in calls)
    plain = prices["plan"] + prices["research"] + prices["draft"]
    without = 3 * plain + 2 * prices["check"] + prices["publish"]
    return without, with_deoos


def main(args):
    endpoint = args.endpoint
    run = uuid.uuid4().hex[:12]
    directory = Path(args.directory) / run
    directory.mkdir(parents=True)
    evidence = directory / "result.json"
    report = {"run": run, "cases": []}
    languages = ("typescript", "python") if args.node_sdk else ("python",)
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                      aws_access_key_id="local-development", aws_secret_access_key="local-development-only-secret")
    wait_for_rustfs(endpoint)
    binary = args.server or os.environ.get("ENGINE_BINARY")

    for language in languages:
        bucket = f"deoos-agent-{run}-{language[:2]}"
        service = AgentService()
        client = None
        server = None
        server_log = None
        workers = []
        try:
            s3.create_bucket(Bucket=bucket)
            from deoos import Client
            server_log = open(directory / f"{language}-server.log", "w")
            server, remote = start_deoos_server(binary, bucket, "agent", server_log, endpoint, lease_ms=5_000)
            client = Client.remote(remote["url"], token=remote["token"])
            task_id = "agent-run-001"
            inputs = {"amount_cents": 100, "service_url": service.url}
            client.submit(task_id, HANDLER, inputs, max_attempts=5, retry_ms=0)

            def spawn(label, pause_after=None):
                ready = directory / f"{language}-{label}-ready"
                config_path = directory / f"{language}-{label}.json"
                config_path.write_text(json.dumps({
                    "client": remote,
                    "pause_after": pause_after,
                    "ready_path": str(ready),
                    "task_id": task_id,
                    "timeout_seconds": 180,
                }))
                config_path.chmod(0o600)
                log_path = directory / f"{language}-{label}.log"
                log = open(log_path, "w")
                command = [sys.executable, str(PY), "work", "--config", str(config_path)] if language == "python" else ["node", str(TS), "work", "--config", str(config_path)]
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=server_worker_env())
                workers.append((process, log))
                return process, log_path, ready

            process, log_path, ready = spawn("after-draft", pause_after="draft")
            wait_until(ready.exists, 60, process, log_path)
            state = client.inspect(task_id)
            check({"plan", "research", "draft"} <= set(state["steps"]), "draft was not saved before the kill")
            check("check" not in state["steps"], "check ran before the kill")
            stop(process)
            print(f"{language}: killed after draft was saved", flush=True)

            process, log_path, ready = spawn("check-fails")
            code = process.wait(timeout=60)
            check(code == 1, f"check failure should stop that attempt, exited {code}\n{tail(log_path)}")
            check("checker unavailable" in log_path.read_text(errors="replace"), "check did not throw the tool error")
            state = client.inspect(task_id)
            check(state["status"] == "queued", f"failed check left the task {state['status']}")
            check(collections.Counter(call["step"] for call in service.llm_calls)["check"] == 1, "check called the model more than once before the retry")

            process, log_path, ready = spawn("finish")
            code = process.wait(timeout=60)
            check(code == 0, f"resume worker exited {code}\n{tail(log_path)}")
            view = client.view(task_id)
            check(view["status"] == "completed", view.get("error") or view["status"])
            counts = collections.Counter(call["step"] for call in service.llm_calls)
            check(dict(counts) == {"plan": 1, "research": 1, "draft": 1, "check": 2, "publish": 1}, f"unexpected model calls: {dict(counts)}")
            check(len(service.effects) == 1, "charge ledger did not keep a single effect")
            check(len(service.charge_calls) == 2, "publish did not retry the charge")
            check(service.charge_calls[0]["key"] == service.charge_calls[1]["key"], "charge retry used a different key")
            check(service.charge_calls[0]["body"] == service.charge_calls[1]["body"], "charge retry changed the body")
            without, with_deoos = credits(service.llm_calls)
            clean = sum(TOKENS.values())
            check(with_deoos == clean + TOKENS["check"], "token total was not a clean run plus one check call")
            print(f"credits burned without deoos vs with: {without} vs {with_deoos}", flush=True)
            check(view["output"]["spend"]["check"] == TOKENS["check"], "report did not keep the saved check spend")
            check(view["output"]["charge_id"] == "charge-1", "report did not keep the one charge")
            report["cases"].append({
                "language": language, "passed": True, "without": without, "with": with_deoos,
                "charge_key": service.charge_calls[0]["key"],
            })
        finally:
            for process, log in workers:
                stop(process)
                if not log.closed:
                    log.close()
            service.close()
            stop_process(server)
            if server_log is not None and not server_log.closed:
                server_log.close()
            if client is not None:
                client.close()
            try:
                s3.head_bucket(Bucket=bucket)
            except ClientError:
                pass
            else:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
                    if objects:
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                s3.delete_bucket(Bucket=bucket)
            evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Agent run evidence: {evidence}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "outputs" / "agent-run-resume")
    parser.add_argument("--endpoint", default=os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000"))
    parser.add_argument("--server", type=Path, default=Path(os.environ["ENGINE_BINARY"]) if os.environ.get("ENGINE_BINARY") else None)
    parser.add_argument("--node-sdk", type=Path, default=Path(os.environ["DEOOS_NODE_SDK"]) if os.environ.get("DEOOS_NODE_SDK") else None)
    main(parser.parse_args())

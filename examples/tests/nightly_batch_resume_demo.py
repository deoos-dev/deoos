"""10,000 order emails: kill after batch 70, kill mid batch 71, and retrigger the same task."""
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
from engine_server import server_worker_env, start_deoos_server, stop_process, wait_for_rustfs
from nightly_batch import BATCHES, HANDLER, batch_name, task_id_for
from seed_orders import ORDER_COUNT, write_orders

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "python" / "nightly-batch-resume" / "nightly_batch.py"
TS = ROOT / "typescript" / "nightly-batch-resume" / "nightly_batch.mjs"
DATE = "2026-10-28"


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


class Mailer:
    """Ledger of /send. Batch 71 can be held open so the worker dies mid-batch."""

    def __init__(self):
        self.calls, self.effects = [], {}
        self.hold_batch_71_after = None
        self.batch_71_accepted = 0
        self.lock = threading.Lock()
        mailer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                key = self.headers.get("Idempotency-Key")
                if self.path != "/send" or not key or key != body.get("idempotency_key"):
                    self.send_error(400)
                    return
                order_id = body.get("order_id")
                number = int(order_id[1:]) if isinstance(order_id, str) and order_id[1:].isdigit() else 0
                with mailer.lock:
                    mailer.calls.append({"key": key, "body": body})
                    cached = mailer.effects.get(key)
                    hold = False
                    if cached:
                        if cached["body"] != body:
                            self.send_error(409)
                            return
                        result = cached["result"]
                    else:
                        result = {"id": f"send-{len(mailer.effects) + 1}", "order_id": order_id}
                        mailer.effects[key] = {"body": body, "result": result}
                        if mailer.hold_batch_71_after is not None and 7001 <= number <= 7100:
                            mailer.batch_71_accepted += 1
                            hold = mailer.batch_71_accepted > mailer.hold_batch_71_after
                if hold:
                    while mailer.hold_batch_71_after is not None:
                        time.sleep(0.05)
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
        self.hold_batch_71_after = None
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def main(args):
    endpoint = args.endpoint
    run = uuid.uuid4().hex[:12]
    directory = Path(args.directory) / run
    directory.mkdir(parents=True)
    evidence = directory / "result.json"
    report = {"run": run, "endpoint": endpoint, "cases": []}
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    languages = ("python", "typescript") if args.node_sdk else ("python",)
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                      aws_access_key_id="local-development", aws_secret_access_key="local-development-only-secret")
    wait_for_rustfs(endpoint)
    binary = args.server or os.environ.get("ENGINE_BINARY")

    for language in languages:
        bucket = f"deoos-nightly-{run}-{language[:2]}"
        orders = directory / f"{language}-orders.jsonl"
        write_orders(orders)
        counter_dir = directory / language / "counters"
        mailer = Mailer()
        client = None
        server = None
        server_log = None
        workers = []
        try:
            s3.create_bucket(Bucket=bucket)
            from deoos import Client
            server_log = open(directory / f"{language}-server.log", "w")
            server, remote = start_deoos_server(binary, bucket, "nightly", server_log, endpoint, lease_ms=10_000)
            client = Client.remote(remote["url"], token=remote["token"])
            task_id = task_id_for(DATE)
            inputs = {
                "counter_dir": str(counter_dir),
                "date": DATE,
                "mailer_url": mailer.url,
                "orders_path": str(orders),
            }

            def trigger():
                return client.submit(task_id, HANDLER, inputs, max_attempts=5, retry_ms=0)

            first = trigger()
            attached = trigger()
            check(first["id"] == task_id and attached["id"] == task_id, "second trigger did not attach to the nightly task")
            check(attached["status"] == "queued", "second trigger reset the task")

            def spawn(label, pause_after=None):
                ready = directory / f"{language}-{label}-ready"
                if ready.exists():
                    ready.unlink()
                config_path = directory / f"{language}-{label}.json"
                config_path.write_text(json.dumps({
                    "client": remote,
                    "pause_after": pause_after,
                    "ready_path": str(ready),
                    "task_id": task_id,
                    "timeout_seconds": 300,
                }))
                config_path.chmod(0o600)
                log_path = directory / f"{language}-{label}.log"
                log = open(log_path, "w")
                command = [sys.executable, str(PY), "work", "--config", str(config_path)] if language == "python" else ["node", str(TS), "work", "--config", str(config_path)]
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=server_worker_env())
                workers.append((process, log))
                return process, log_path, ready

            process, log_path, ready = spawn("after-70", pause_after=batch_name(70))
            wait_until(ready.exists, 240, process, log_path)
            state = client.inspect(task_id)
            check(batch_name(70) in state["steps"], "batch 70 was not saved before the kill")
            check(batch_name(71) not in state["steps"], "batch 71 was saved before the kill")
            stop(process)
            for number in range(1, 71):
                check((counter_dir / batch_name(number)).read_text() == "1", f"{batch_name(number)} ran more than once before the resume")
            check(not (counter_dir / batch_name(71)).exists(), "batch 71 ran before its worker started")
            print(f"{language}: killed after batch 70 was saved; batches 1-70 ran once", flush=True)

            mailer.hold_batch_71_after = 30
            process, log_path, ready = spawn("mid-71")
            wait_until(lambda: mailer.batch_71_accepted >= 31, 120, process, log_path)
            mid = trigger()
            mid_state = client.inspect(task_id)
            check(mid["id"] == task_id and batch_name(70) in mid_state["steps"], "overlapping trigger did not keep the saved batches")
            check(batch_name(71) not in mid_state["steps"], "batch 71 checkpointed while its sends were still in flight")
            stop(process)
            mailer.hold_batch_71_after = None
            check((counter_dir / batch_name(71)).read_text() == "1", "the killed batch 71 execution was not counted")
            print(f"{language}: killed inside batch 71; overlapping trigger attached to the same task", flush=True)

            process, log_path, ready = spawn("finish")
            code = process.wait(timeout=240)
            check(code == 0, f"resume worker exited {code}\n{tail(log_path)}")
            view = client.view(task_id)
            check(view["status"] == "completed", view.get("error") or view["status"])
            finished = trigger()
            again = client.view(task_id)
            check(again["output"] == view["output"], "second trigger after completion changed the result")
            check(finished.get("output") == view["output"], "second trigger did not return the saved result")
            check(view["output"] == {"batches": BATCHES, "date": DATE, "orders": ORDER_COUNT, "task_id": task_id}, "unexpected summary")

            calls_by_key = collections.Counter(call["key"] for call in mailer.calls)
            check(len(mailer.effects) == ORDER_COUNT, f"expected {ORDER_COUNT} unique sends, found {len(mailer.effects)}")
            check(set(calls_by_key) == set(mailer.effects), "a call key is missing from the effect ledger")
            duplicates = [key for key, count in calls_by_key.items() if count > 1]
            check(duplicates, "batch 71 did not retry any send")
            for key in duplicates:
                bodies = [call["body"] for call in mailer.calls if call["key"] == key]
                check(all(body == bodies[0] for body in bodies), f"duplicate send changed body for {key}")
                number = int(bodies[0]["order_id"][1:])
                check(7001 <= number <= 7100, f"duplicate send was outside batch 71: {bodies[0]['order_id']}")
                check(key == f"{task_id}/email-{bodies[0]['order_id']}", "duplicate send did not reuse the idempotency key")
            for number in range(1, 71):
                check((counter_dir / batch_name(number)).read_text() == "1", f"{batch_name(number)} was executed again")
            check((counter_dir / batch_name(71)).read_text() == "2", "batch 71 did not run again after the mid-batch kill")
            for number in range(72, BATCHES + 1):
                check((counter_dir / batch_name(number)).read_text() == "1", f"{batch_name(number)} did not run once")
            print(f"{language}: second trigger returned the same result; {ORDER_COUNT} unique sends", flush=True)
            report["cases"].append({"language": language, "passed": True, "orders": ORDER_COUNT, "duplicate_keys": len(duplicates)})
        finally:
            for process, log in workers:
                stop(process)
                if not log.closed:
                    log.close()
            if mailer is not None:
                mailer.close()
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
    print(f"Nightly batch evidence: {evidence}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "outputs" / "nightly-batch-resume")
    parser.add_argument("--endpoint", default=os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000"))
    parser.add_argument("--server", type=Path, default=Path(os.environ["ENGINE_BINARY"]) if os.environ.get("ENGINE_BINARY") else None)
    parser.add_argument("--node-sdk", type=Path, default=Path(os.environ["DEOOS_NODE_SDK"]) if os.environ.get("DEOOS_NODE_SDK") else None)
    main(parser.parse_args())

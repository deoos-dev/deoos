"""Air-gapped approval: one transfer, mapped to the approval that allowed it.

The host builds an offline bundle, loads it, and runs compose.offline.yaml.
Inside that network the worker cannot reach GitHub. The scenario itself runs
in the container: kill the worker while it waits, signal with no worker,
kill again between the transfer call and its checkpoint, then finish.
"""
import argparse
import http.server
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
import paths
from engine_server import server_worker_env, start_deoos_server, stop_process, wait_for_rustfs

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "python" / "offline-approval-ledger" / "approval_ledger.py"
INVOICE = "/app/python/offline-approval-ledger/invoice.json"
APPROVAL = {
    "approval_id": "apr-100",
    "approver": "dana",
    "approved": True,
    "note": "matches invoice inv-100",
}


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


def github_error(timeout=5):
    """Return the failure, or None if HTTPS to github.com succeeds.

    An internal network can leave DNS unanswered. urlopen's timeout does not
    cover that wait, so the probe itself gives up on a deadline.
    """
    box = {}

    def attempt():
        try:
            with urllib.request.urlopen("https://github.com/", timeout=timeout) as response:
                box["status"] = response.status
        except Exception as error:
            box["error"] = f"{type(error).__name__}: {error}"

    thread = threading.Thread(target=attempt, daemon=True)
    thread.start()
    thread.join(timeout + 2)
    if thread.is_alive():
        return f"TimeoutError: no response from github.com within {timeout + 2}s"
    if "status" in box:
        return None
    return box.get("error", "URLError: github.com was not reachable")


class Bank:
    def __init__(self):
        self.calls, self.effects, self.ledger = [], {}, []
        self.lock = threading.Lock()
        bank = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                key = self.headers.get("Idempotency-Key")
                if self.path not in ("/transfer", "/ledger") or not key or key != body.get("idempotency_key"):
                    self.send_error(400)
                    return
                with bank.lock:
                    bank.calls.append({"path": self.path, "key": key, "body": body})
                    if self.path == "/ledger":
                        bank.ledger.append(body)
                        result = body
                    else:
                        cached = bank.effects.get(key)
                        if cached:
                            if cached["body"] != body:
                                self.send_error(409)
                                return
                            result = cached["result"]
                        else:
                            result = {"approval_id": body.get("approval_id"), "transfer_id": "tr-1"}
                            bank.effects[key] = {"body": body, "result": result}
                raw = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def run_isolated(args):
    from approval_ledger import HANDLER
    from deoos import Client
    import boto3

    print("offline: probing HTTPS to github.com", flush=True)
    probe = github_error()
    child = subprocess.run(
        [sys.executable, "-c",
         "import socket,threading,urllib.request\n"
         "box={}\n"
         "def attempt():\n"
         "    try:\n"
         "        urllib.request.urlopen('https://github.com/', timeout=5)\n"
         "        box['ok']=True\n"
         "    except Exception as error:\n"
         "        box['error']=type(error).__name__\n"
         "thread=threading.Thread(target=attempt, daemon=True)\n"
         "thread.start(); thread.join(7)\n"
         "print('REACHABLE' if box.get('ok') else box.get('error','TimeoutError'))\n"],
        capture_output=True, text=True, timeout=15)
    check(probe is not None, "HTTPS to github.com succeeded; the worker network is not isolated")
    check("REACHABLE" not in child.stdout, "HTTPS to github.com succeeded from a worker process")
    check(child.returncode == 0, child.stderr)

    endpoint = args.endpoint
    run = uuid.uuid4().hex[:12]
    directory = Path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    work = directory / run
    work.mkdir()
    counter_dir = work / "counters"
    bucket = "deoos-approval-" + run
    report = {
        "run": run, "network_cut_off": True, "github_error": probe,
        "worker_probe": child.stdout.strip(), "passed": False,
    }
    bank = Bank()
    client = None
    server = None
    server_log = None
    workers = []
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                      aws_access_key_id="local-development", aws_secret_access_key="local-development-only-secret")
    try:
        wait_for_rustfs(endpoint)
        s3.create_bucket(Bucket=bucket)
        server_log = open(work / "deoos-server.log", "w")
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), bucket, "approval", server_log, endpoint, lease_ms=5_000)
        report["engine"] = "deoos-server"
        report["engine_url"] = remote["url"]
        client = Client.remote(remote["url"], token=remote["token"])
        task_id = "approval-inv-100"
        inputs = {"bank_url": bank.url, "counter_dir": str(counter_dir), "invoice_path": INVOICE}
        client.submit(task_id, HANDLER, inputs, max_attempts=5, retry_ms=0)

        def spawn(label, pause_inside=None):
            ready = work / f"{label}-ready"
            config_path = work / f"{label}.json"
            config_path.write_text(json.dumps({
                "client": remote,
                "pause_inside": pause_inside,
                "ready_path": str(ready),
                "task_id": task_id,
                "timeout_seconds": 180,
            }))
            config_path.chmod(0o600)
            log_path = work / f"{label}.log"
            log = open(log_path, "w")
            process = subprocess.Popen(
                [sys.executable, str(PY), "work", "--config", str(config_path)],
                stdout=log, stderr=subprocess.STDOUT, env=server_worker_env())
            workers.append((process, log))
            return process, log_path, ready

        process, log_path, ready = spawn("waiting")
        def waiting():
            state = client.inspect(task_id)
            waiting_on = state.get("waiting_on") or {}
            if state["status"] == "waiting" and waiting_on.get("kind") == "signal" and waiting_on.get("name") == "approval":
                return state
            return None
        wait_until(waiting, 60, process, log_path)
        stop(process)
        check(process.poll() is not None, "worker was still running when the signal was sent")
        client.signal(task_id, "approval", APPROVAL)
        stored = client.request(f"/tasks/{task_id}/signals/approval")
        check(stored == APPROVAL, "signal payload was not saved while no worker was running")
        check(client.inspect(task_id)["status"] == "waiting", "signaling a waiting task started a second task")
        print("offline: worker killed while waiting; signal stored with no worker running", flush=True)

        process, log_path, ready = spawn("transfer", pause_inside="transfer")
        wait_until(ready.exists, 60, process, log_path)
        check("transfer" not in client.inspect(task_id)["steps"], "transfer checkpointed before the kill")
        check(len(bank.effects) == 1, "transfer was not recorded before the checkpoint")
        stop(process)
        print("offline: killed between the transfer call and its checkpoint", flush=True)

        process, log_path, ready = spawn("finish")
        code = process.wait(timeout=60)
        check(code == 0, f"resume worker exited {code}\n{tail(log_path)}")
        view = client.view(task_id)
        check(view["status"] == "completed", view.get("error") or view["status"])
        check(len(bank.effects) == 1, "resume created a second transfer")
        effect = next(iter(bank.effects.values()))
        check(effect["body"]["approval_id"] == APPROVAL["approval_id"], "transfer body is missing the approval id")
        check(effect["body"]["idempotency_key"] == f"{task_id}/transfer", "transfer did not use the stable key")
        transfer_calls = [call for call in bank.calls if call["path"] == "/transfer"]
        check(len(transfer_calls) == 2, "the killed transfer was not retried")
        check(transfer_calls[0]["key"] == transfer_calls[1]["key"], "transfer retry changed the idempotency key")
        check(transfer_calls[0]["body"] == transfer_calls[1]["body"], "transfer retry changed the body")
        check(bank.ledger == [view["output"]], "ledger row does not match the task result")
        row = view["output"]
        check(row["approval_id"] == APPROVAL["approval_id"] and row["transfer_id"] == "tr-1", "ledger row does not map the approval to the transfer")
        check(row["task_id"] == task_id and row["idempotency_key"] == f"{task_id}/transfer", "ledger row is missing the task or the key")
        check((counter_dir / "intake").read_text() == "1", "intake ran again after the wait")
        check((counter_dir / "propose_transfer").read_text() == "1", "propose_transfer ran again after the wait")
        check((counter_dir / "transfer").read_text() == "2", "the transfer step did not retry after the kill")
        check((counter_dir / "reconcile").read_text() == "1", "reconcile ran more than once")
        report.update(passed=True, output=row, transfer_calls=len(transfer_calls))
        print("offline: one transfer, approval mapped to transfer id, pre-wait steps ran once", flush=True)
    finally:
        for process, log in workers:
            stop(process)
            if not log.closed:
                log.close()
        bank.close()
        stop_process(server)
        if server_log is not None and not server_log.closed:
            server_log.close()
        if client is not None:
            client.close()
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            pass
        else:
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            s3.delete_bucket(Bucket=bucket)
        text = json.dumps(report, indent=2) + "\n"
        (directory / "result.json").write_text(text)
        (work / "result.json").write_text(text)
    check(report["passed"], "offline approval scenario did not pass")


def run_on_host():
    bundle = ROOT / "outputs" / "offline-bundle.tar"
    subprocess.check_call([str(ROOT / "scripts" / "bundle_offline.sh")], cwd=ROOT)
    check(bundle.is_file(), "bundle_offline.sh did not write outputs/offline-bundle.tar")
    compose = (ROOT / "compose.offline.yaml").read_text()
    check("build:" not in compose, "compose.offline.yaml must not build images")
    check("internal: true" in compose, "compose.offline.yaml must keep its network internal")
    subprocess.run(["docker", "rmi", "deoos-examples:offline"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(["docker", "load", "-i", str(bundle)], cwd=ROOT)
    completed = subprocess.run(
        ["docker", "compose", "-f", "compose.offline.yaml", "run", "--rm", "offline-approval-ledger"],
        cwd=ROOT)
    check(completed.returncode == 0, "isolated offline-approval-ledger run failed")
    report = json.loads((ROOT / "outputs" / "offline-approval-ledger" / "result.json").read_text())
    check(report["passed"] is True and report["network_cut_off"] is True, "isolated run did not record a passing cutoff")
    print(f"Offline approval evidence: {ROOT / 'outputs' / 'offline-approval-ledger' / 'result.json'}")


def main(args):
    if args.in_container:
        run_isolated(args)
    else:
        run_on_host()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "outputs" / "offline-approval-ledger")
    parser.add_argument("--endpoint", default="http://rustfs:9000")
    parser.add_argument("--in-container", action="store_true")
    parser.add_argument("--server", type=Path, default=None)
    parser.add_argument("--node-sdk", type=Path, default=None)
    main(parser.parse_args())

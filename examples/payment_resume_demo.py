"""Fake payments: checkpoint replay beyond key expiry, and call/commit-gap retry."""
import argparse
import collections
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

import boto3
from botocore.exceptions import ClientError
from deoos import Client
from payment_resume import HANDLER, payment, payment_inputs

ROOT = Path(__file__).resolve().parents[1]
STORAGE = dict(provider="s3", endpoint="http://127.0.0.1:19000", allow_http=True,
               region="us-east-1", access_key_id="local-development",
               secret_access_key="local-development-only-secret", lease_ms=2000)


class PaymentAPI:
    """The effect ledger survives cache expiry; virtual time makes a day instant."""
    def __init__(self, window):
        self.window, self.now = window, 0
        self.cache, self.calls, self.effects = {}, [], []
        self.lock = threading.Lock()
        api = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                key = self.headers.get("Idempotency-Key")
                if self.path not in ("/charge", "/fulfil") or not key or key != body.get("idempotency_key"):
                    self.send_error(400)
                    return
                with api.lock:
                    api.calls.append({"path": self.path, "key": key, "body": body, "at": api.now})
                    cached = api.cache.get((self.path, key))
                    if cached and cached["expires"] > api.now:
                        if cached["body"] != body:
                            self.send_error(409)
                            return
                        result = cached["result"]
                    else:
                        result = {"id": f"effect-{len(api.effects) + 1}", "kind": self.path[1:]}
                        api.effects.append({"path": self.path, "key": key, "result": result})
                        api.cache[self.path, key] = {"body": body, "result": result, "expires": api.now + api.window}
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

    def expire(self):
        with self.lock:
            self.now += self.window + 1
            self.cache = {key: value for key, value in self.cache.items() if value["expires"] > self.now}

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def client_for(config):
    return Client.remote(config["url"]) if "url" in config else Client(**config)


def check(value, message):
    if not value:
        raise RuntimeError(message)


def wait(predicate, process=None, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"Worker exited early: {process.returncode}")
        time.sleep(.1)
    raise TimeoutError("Payment demo timed out")


def python_worker(config):
    def handler(ctx, inputs):
        keys = {name: ctx.idempotency_key(name) for name in ("charge", "fulfil")}
        with Path(config["keys"]).open("a") as file:
            file.write(json.dumps(keys) + "\n")
        original = ctx.step

        def pause():
            Path(config["ready"]).write_text(config["stage"])
            while True:
                time.sleep(1)

        def step(name, callback):
            def execute():
                result = callback()
                if name == "charge" and config["stage"] == "gap":
                    pause()
                return result
            result = original(name, execute)
            if name == "charge" and config["stage"] == "saved":
                pause()
            return result

        ctx.step = step
        return payment(ctx, inputs)

    with client_for(config["client"]) as client:
        wait(lambda: client.run_once({HANDLER: handler}))


NODE_WORKER = """import {appendFileSync,writeFileSync} from 'node:fs';
import {readFileSync} from 'node:fs';import {setTimeout as delay} from 'node:timers/promises';
const config=JSON.parse(readFileSync(process.argv[2],'utf8'));
const {Client}=await import(config.sdk);const {HANDLER,payment}=await import(config.example);
const c=config.client.url?Client.remote(config.client.url):new Client(config.client);
async function handler(ctx,inputs){
 appendFileSync(config.keys,JSON.stringify({charge:ctx.idempotencyKey('charge'),fulfil:ctx.idempotencyKey('fulfil')})+'\\n');
 const original=ctx.step.bind(ctx);
 async function pause(){writeFileSync(config.ready,config.stage);while(true)await delay(1000);}
 ctx.step=async(name,fn)=>{const result=await original(name,async()=>{
  const result=await fn();if(name==='charge'&&config.stage==='gap')await pause();return result;});
  if(name==='charge'&&config.stage==='saved')await pause();return result;};
 return payment(ctx,inputs);
}
const deadline=Date.now()+60000;while(!await c.runOnce({[HANDLER]:handler})){
 if(Date.now()>deadline)throw new Error('Payment demo timed out');await delay(100);}
"""


def stop(process):
    if process is not None:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)


def main(args):
    run = uuid.uuid4().hex[:12]
    directory = ROOT / "outputs" / "payment-resume" / run
    directory.mkdir(parents=True)
    bucket = "deoos-payment-" + run
    report = {"bucket": bucket, "prefix": "payment-resume/", "cases": [], "cleaned": False,
              "key_window_seconds": args.key_window_seconds, "clock": "Fake API virtual time; no real payments or day-long wait."}
    evidence = directory / "result.json"
    s3 = boto3.client("s3", endpoint_url=STORAGE["endpoint"], region_name=STORAGE["region"],
                      aws_access_key_id=STORAGE["access_key_id"], aws_secret_access_key=STORAGE["secret_access_key"])
    api, create_intent = None, False
    try:
        def healthy():
            try:
                with urllib.request.urlopen(STORAGE["endpoint"] + "/health", timeout=2) as response:
                    return response.status == 200
            except OSError:
                return False
        wait(healthy)
        create_intent = True
        evidence.write_text(json.dumps(report, indent=2) + "\n")
        s3.create_bucket(Bucket=bucket)
        api = PaymentAPI(args.key_window_seconds)
        # Verify that the fake really forgets a request key while retaining its effects.
        from payment_resume import post
        first = post(api.url, "/charge", "expiry-control", {"amount_cents": 1})
        check(post(api.url, "/charge", "expiry-control", {"amount_cents": 1}) == first, "Provider did not deduplicate")
        api.expire()
        check(post(api.url, "/charge", "expiry-control", {"amount_cents": 1}) != first, "Provider did not expire its key")
        report["provider_window_verified"] = True
        node_worker = directory / "worker.mjs"
        node_worker.write_text(NODE_WORKER)
        languages = ("python", "typescript") if args.node_sdk else ("python",)
        all_keys = set()
        for mode in ("library", "server"):
            # Use the same server bootstrap as the local ETL demo, with explicit dev settings.
            from devto_etl_demo import start_server
            server = None
            config = dict(STORAGE, bucket=bucket, prefix="payment-resume/" + mode)
            with (directory / f"{mode}-server.log").open("w") as log:
                try:
                    if mode == "server":
                        server, config = start_server(args.server, bucket, config["prefix"], log)
                    with client_for(config) as client:
                        for language in languages:
                            for stage in ("saved", "gap"):
                                task_id = f"{run}-{mode}-{language}-{stage}"
                                keys_file = directory / f"{task_id}-keys.jsonl"
                                ready = directory / f"{task_id}-ready"
                                workers = []

                                def spawn(stage_):
                                    private = directory / f"{task_id}-{stage_}.json"
                                    private.write_text(json.dumps({"client": config, "keys": str(keys_file),
                                        "ready": str(ready), "stage": stage_,
                                        "sdk": args.node_sdk.resolve().as_uri() if args.node_sdk else None,
                                        "example": (ROOT / "examples/payment_resume.mjs").as_uri()}))
                                    private.chmod(0o600)
                                    command = ([sys.executable, __file__, "--worker", str(private)] if language == "python"
                                               else ["node", str(node_worker), str(private)])
                                    env = dict(os.environ)
                                    if mode == "server":
                                        env = {k: v for k, v in env.items() if not k.startswith(("AWS_", "DEOOS_STORAGE_"))}
                                    process = subprocess.Popen(command, env=env, stdout=log, stderr=log)
                                    workers.append(process)
                                    return process

                                try:
                                    client.submit(task_id, HANDLER, payment_inputs(api.url), max_attempts=5)
                                    process = spawn(stage)
                                    wait(ready.exists, process)
                                    state = client.inspect(task_id)
                                    check(state["attempts"] == 1 and state["status"] == "running", "Wrong crash state")
                                    check(("charge" in state["steps"]) == (stage == "saved"), "Wrong charge commit boundary")
                                    stop(process)
                                    if stage == "saved":
                                        api.expire()
                                        check(("/charge", task_id + "/charge") not in api.cache, "Payment key has not expired")
                                    process = spawn("resume")
                                    process.wait(timeout=60)
                                    check(process.returncode == 0, "Resume worker failed")
                                    check(client.inspect(task_id)["status"] == "waiting", "Approval did not persist a wait")
                                    client.signal(task_id, "approval", {"approved": True})
                                    process = spawn("approved")
                                    process.wait(timeout=60)
                                    check(process.returncode == 0, "Approved worker failed")
                                    final = client.inspect(task_id)
                                    # Signal resumption continues the recovered attempt; it is not a retry.
                                    check(final["status"] == "completed" and final["attempts"] == 2, "Payment did not complete after recovery")
                                    keys = [json.loads(line) for line in keys_file.read_text().splitlines()]
                                    expected = {name: task_id + "/" + name for name in ("charge", "fulfil")}
                                    check(len(keys) == 3 and all(row == expected for row in keys), "Keys changed across crash/resume")
                                    check(len(set(expected.values())) == 2 and not all_keys.intersection(expected.values()), "Step/execution key collision")
                                    all_keys.update(expected.values())
                                    calls = [row for row in api.calls if row["key"] in expected.values()]
                                    effects = [row for row in api.effects if row["key"] in expected.values()]
                                    counts = collections.Counter(row["path"] for row in calls)
                                    check(counts == {"/charge": 1 if stage == "saved" else 2, "/fulfil": 1}, "Unexpected external calls")
                                    check(collections.Counter(row["path"] for row in effects) == {"/charge": 1, "/fulfil": 1}, "Duplicate business effect")
                                    saved = client.request(f"/tasks/{task_id}/steps/charge")
                                    check(calls[-1]["body"]["charge"] == saved, "Fulfil did not use the saved charge")
                                    report["cases"].append({"mode": mode, "language": language, "crash": stage,
                                        "passed": True, "keys": keys, "calls": calls, "effects": effects,
                                        "attempts": final["attempts"], "provider_window_expired": stage == "saved"})
                                    print(f"{mode}/{language}: {'saved charge reused past key expiry' if stage == 'saved' else 'call/commit gap retried with the same key'}; one charge, one fulfil.", flush=True)
                                finally:
                                    for process in workers:
                                        stop(process)
                finally:
                    stop(server)
    finally:
        try:
            if api is not None:
                api.close()
            if create_intent:
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                        raise
                else:
                    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                        objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
                        check(all(row["Key"].startswith(report["prefix"]) for row in objects), "Unexpected object blocks cleanup")
                        if objects:
                            check(not s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors"), "Storage cleanup failed")
                    s3.delete_bucket(Bucket=bucket)
                    try:
                        s3.head_bucket(Bucket=bucket)
                    except ClientError as error:
                        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                            raise
                    else:
                        raise RuntimeError("Bucket still exists")
                report["cleaned"] = True
        finally:
            evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Payment recovery evidence: {evidence}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path)
    parser.add_argument("--node-sdk", type=Path, help="Also verify the TypeScript workflow using this SDK index.js")
    parser.add_argument("--key-window-seconds", type=int, default=86400)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        python_worker(json.loads(args.worker.read_text()))
    else:
        if not args.server or args.key_window_seconds <= 0:
            parser.error("--server and a positive key window are required")
        main(args)

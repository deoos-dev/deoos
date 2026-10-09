#!/usr/bin/env python3
"""Bounded, real RustFS saturation experiment. Writes evidence outside the repo.

Run with the test Python environment and a built engine. No cloud resources.
This intentionally records partial/failed runs rather than declaring them passes.
"""
import argparse
import collections
import concurrent.futures
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "clients/python"))
from deoos import Client


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Proxy:
    """Injection applies only within a fixed window; metrics include all attempts."""
    def __init__(self, target, evidence, rate, fault_seconds):
        self.target, self.rate, self.fault_seconds = target, rate, fault_seconds
        self.lock = threading.Lock()
        self.drained = threading.Condition(self.lock)
        self.in_flight = 0
        self.storage_in_flight = self.storage_peak = self.proxy_peak = 0
        self.start = time.monotonic()
        self.fault_start = self.start + 5
        self.last_write, self.window, self.used = {}, 0, 0
        self.counts, self.keys = collections.Counter(), collections.Counter()
        self.bytes_in = self.bytes_out = self.max_state_bytes = 0
        self.requests = 0
        self.checkpoints, self.latest_states = {}, {}
        self.checkpoint_changes = []
        self.expiry_reclaims = []
        self.early_token_changes = []
        self.result_hashes = {}
        self.first_tokens = {}
        self.potentially_admitted = set()
        self.log = open(evidence / "requests.jsonl", "w", buffering=1)
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *args):
                pass
            def handle(self):
                try:
                    super().handle()
                except ConnectionResetError:
                    pass
            def finish(self):
                if getattr(self, "backend", None):
                    self.backend.close()
                super().finish()
            def handle_request(self):
                began = time.monotonic()
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                path = urllib.parse.urlsplit(self.path).path
                with owner.lock:
                    owner.in_flight += 1
                    elapsed = began - owner.start
                    fault = owner.fault_start <= began < owner.fault_start + owner.fault_seconds
                    second = int(began)
                    if second != owner.window:
                        owner.window, owner.used = second, 0
                    owner.used += 1
                    hot = self.command == "PUT" and path.endswith("/state.json")
                    budget_reject = owner.used > owner.rate
                    key_reject = hot and began - owner.last_write.get(path, 0) < 1
                    reject = fault and (budget_reject or key_reject)
                    owner.proxy_peak = max(owner.proxy_peak, owner.in_flight)
                    if not reject:
                        owner.storage_in_flight += 1
                        owner.storage_peak = max(owner.storage_peak, owner.storage_in_flight)
                    if hot and not reject:
                        owner.last_write[path] = began
                    owner.requests += 1
                    sequence = owner.requests
                connection = None
                injected = None
                upstream_status = None
                try:
                    if reject:
                        status = 503 if sequence % 2 else 429
                        injected = "request_budget" if budget_reject else "hot_key"
                        payload = b"<Error><Code>SlowDown</Code><Message>controlled saturation</Message></Error>"
                        headers = [("Content-Type", "application/xml"), ("Retry-After", "1")]
                    else:
                        if getattr(self, "backend", None) is None:
                            self.backend = http.client.HTTPConnection("127.0.0.1", owner.target, timeout=15)
                        connection = self.backend
                        headers = dict(self.headers)
                        headers.pop("Connection", None)
                        if self.command == "PUT" and (hot or "/active/" in path):
                            task_id = urllib.parse.unquote(path.rsplit("/", 2)[1])
                            with owner.lock:
                                # A forwarded write can commit even when its response is lost.
                                owner.potentially_admitted.add(task_id)
                        connection.request(self.command, self.path, body, headers)
                        response = connection.getresponse()
                        status, headers, payload = response.status, response.getheaders(), response.read()
                        upstream_status = status
                    self.send_response(status)
                    for name, value in headers:
                        if name.lower() not in ("connection", "transfer-encoding", "content-length"):
                            self.send_header(name, value)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError) as error:
                    status, payload = 599, b""
                    injected = type(error).__name__
                    self.close_connection = True
                finally:
                    record = dict(at=round(elapsed, 4), method=self.command, key=path,
                                  status=status, upstream_status=upstream_status, injected=injected, request_bytes=len(body),
                                  response_bytes=len(payload), seconds=round(time.monotonic()-began, 4),
                                  fingerprint=hashlib.sha256(body).hexdigest() if body else None,
                                  conditional=self.headers.get("If-Match"), query=urllib.parse.urlsplit(self.path).query)
                    record["create_only"] = self.headers.get("If-None-Match")
                    with owner.lock:
                        owner.counts[str(status)] += 1
                        owner.keys[path] += 1
                        owner.bytes_in += len(body)
                        owner.bytes_out += len(payload)
                        if hot:
                            owner.max_state_bytes = max(owner.max_state_bytes, len(body))
                        if hot and upstream_status == 200:
                            state = json.loads(body)
                            prior = owner.latest_states.get(path)
                            if prior:
                                for name in prior.get("steps", {}):
                                    if name not in state.get("steps", {}):
                                        owner.checkpoint_changes.append(dict(key=path, step=name, reason="deleted"))
                            if (prior and prior.get("status") == "running" and state.get("status") == "running"
                                    and prior.get("token") != state.get("token")
                                    and state.get("attempts", 0) > prior.get("attempts", 0)):
                                claim_ms = next((event["at_ms"] for event in reversed(state["history"])
                                                 if event["event"] == "claim"), int(time.time()*1000))
                                changes = owner.expiry_reclaims if claim_ms >= prior["expires_at"] else owner.early_token_changes
                                changes.append(dict(id=state["id"], at=elapsed, claim_ms=claim_ms,
                                    previous_expiry=prior["expires_at"], previous_attempts=prior["attempts"],
                                    attempts=state["attempts"]))
                            owner.latest_states[path] = state
                            if state.get("token"):
                                owner.first_tokens.setdefault(state["id"], state["token"])
                            for name, pointer in state.get("steps", {}).items():
                                key = (path, name)
                                if key in owner.checkpoints and owner.checkpoints[key] != pointer:
                                    owner.checkpoint_changes.append(dict(key=path, step=name))
                                owner.checkpoints[key] = pointer
                        if self.command == "PUT" and "/results/" in path and upstream_status == 200:
                            owner.result_hashes[path] = hashlib.sha256(body).hexdigest()
                        owner.log.write(json.dumps(record) + "\n")
                        owner.in_flight -= 1
                        if not reject:
                            owner.storage_in_flight -= 1
                        owner.drained.notify_all()
            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = handle_request
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        deadline = time.monotonic() + 30
        with self.drained:
            while self.in_flight and time.monotonic() < deadline:
                self.drained.wait(deadline-time.monotonic())
            if self.in_flight:
                raise RuntimeError("proxy requests did not drain")
        self.log.close()


def worker(config):
    """A separate bounded process; stopping the experiment stops stuck callbacks."""
    lock = threading.Lock()
    events = open(config["events"], "a", buffering=1)
    started = time.monotonic()
    stop = threading.Event()
    def event(kind, **values):
        with lock:
            events.write(json.dumps(dict(at=time.time(), event=kind, **values)) + "\n")
    def effect(identifier):
        db = sqlite3.connect(config["effects"], timeout=30)
        try:
            with db:
                db.execute("INSERT INTO invocations(id,at) VALUES (?,?)", (identifier, time.time()))
                db.execute("INSERT OR IGNORE INTO effects(id) VALUES (?)", (identifier,))
        finally:
            db.close()
        return identifier
    def parent(ctx, inputs):
        for index in range(inputs["nodes"]):
            ctx.spawn(f"child-{index}", "child", {"index": index}, max_attempts=10, retry_ms=200)
        if config.get("park_checkpoints"):
            event("checkpoint_cutoff_ready", barrier="parent", task_id=ctx.task["id"])
            threading.Event().wait()  # Keep the saved parent running until the controller kills it.
        return {"spawned": inputs["nodes"]}
    def child(ctx, inputs):
        result = ctx.step("effect", lambda: effect(ctx.task["id"]))
        if config.get("park_checkpoints") and inputs["index"] == config["nodes"] - 1:
            event("checkpoint_cutoff_ready", barrier="child", task_id=ctx.task["id"])
            threading.Event().wait()
        return result
    def canary(ctx, inputs):
        return ctx.step("effect", lambda: effect(ctx.task["id"]))
    def timer(ctx, inputs):
        ctx.sleep("wait", 10000)
        return ctx.step("effect", lambda: effect(ctx.task["id"]))
    handlers = dict(parent=parent, child=child, canary=canary, timer=timer)
    def run(number):
        client = Client.remote(config["url"])
        original_request = client.request
        def measured_request(path, data=None):
            operation_id = data.get("operation_id") if isinstance(data, dict) else None
            event("rpc_offered", worker=number, path=path, mutation=data is not None, operation_id=operation_id)
            try:
                value = original_request(path, data)
            except Exception as error:
                event("rpc_finished", worker=number, path=path, success=False,
                      status=getattr(error, "status", None), operation_id=operation_id)
                raise
            event("rpc_finished", worker=number, path=path, success=True, operation_id=operation_id)
            return value
        client.request = measured_request
        def on_error(error, task_id):
            event("worker_error", worker=number, task_id=task_id, error=str(error), type=type(error).__name__)
            return "continue"
        client.run_worker(handlers, stop_event=stop, poll_interval=.1,
                          worker_id=f"worker-{number}", on_error=on_error)
    threads = []
    for due, target in config.get("ramp", ((0, 8), (10, 32), (20, 64))):
        time.sleep(max(0, started + due - time.monotonic()))
        event("ramp", workers=target)
        while len(threads) < target:
            thread = threading.Thread(target=run, args=(len(threads),), daemon=True)
            threads.append(thread)
            thread.start()
    time.sleep(max(0, started + config["duration"] - time.monotonic()))
    stop.set()
    # Explicit process exit bounds SDK calls stalled on storage. Parent owns cleanup.


def main(args):
    evidence = Path(args.report).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    report = dict(nodes_requested=args.nodes, duration=args.duration, fault_seconds=args.fault_seconds,
                  lease_ms=args.lease_ms, attempt_budget=10, storage="local RustFS",
                  engine_sha256=hashlib.sha256(Path(args.engine).read_bytes()).hexdigest(),
                  source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  source_dirty=subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip(),
                  cleaned=False)
    report["parameters"] = dict(rate=args.rate, fault_start_seconds=5,
        fault_end_seconds=5+args.fault_seconds, lease_ms=args.lease_ms,
        ramp=[[0,8],[10,32],[20,64]], nodes=args.nodes, duration=args.duration,
        request_budget_semantics="Budget counts accepted and rejected attempts; resets each monotonic whole second.")
    report["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["sdk_sha256"] = hashlib.sha256(Path(sys.modules["deoos"].__file__).read_bytes()).hexdigest()
    report["worker_loop"] = "Actual Python Client.run_worker with 0.1s poll interval and on_error=continue."
    if args.recovery_seconds:
        report["cutoff_probe_semantics"] = ("Opt-in: parent parks after final spawn checkpoint; final child parks after effect checkpoint. "
            "Both retain live heartbeats until hard kill. Original metrics describe this controlled workload only; recovery is separate.")
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{args.rustfs_port}",
                      aws_access_key_id="local-development", aws_secret_access_key="local-development-only-secret",
                      region_name="us-east-1", config=Config(retries={"max_attempts": 2}, read_timeout=10,
                      connect_timeout=5, s3={"addressing_style": "path"}))
    bucket = "deoos-saturation-" + uuid.uuid4().hex[:20]
    report["owned_bucket"] = bucket
    prefix = "saturation"
    engine = workers = proxy = None
    created = False
    submitted = []
    snapshots = {}
    canaries = []
    direct = lambda identifier: json.loads(s3.get_object(Bucket=bucket,
                          Key=f"{prefix}/tasks/{identifier}/state.json")["Body"].read())
    started = time.monotonic()
    try:
        s3.create_bucket(Bucket=bucket)
        created = True
        proxy = Proxy(args.rustfs_port, evidence, args.rate, args.fault_seconds)
        engine_port = port()
        env = dict(os.environ, AWS_ENDPOINT=f"http://127.0.0.1:{proxy.server.server_port}",
                   AWS_ALLOW_HTTP="true", AWS_ACCESS_KEY_ID="local-development",
                   AWS_SECRET_ACCESS_KEY="local-development-only-secret", AWS_REGION="us-east-1",
                   DEOOS_STORAGE_PROVIDER="s3", DEOOS_STORAGE_BUCKET=bucket,
                   EXECUTION_PREFIX=prefix, ENGINE_BIND=f"127.0.0.1:{engine_port}",
                   LEASE_MS=str(args.lease_ms))
        for name in ("AWS_SESSION_TOKEN", "ENGINE_TOKEN"):
            env.pop(name, None)
        engine_log = open(evidence / "engine.log", "w")
        engine = subprocess.Popen([args.engine], env=env, stdout=engine_log, stderr=subprocess.STDOUT)
        engine_log.close()
        url = f"http://127.0.0.1:{engine_port}"
        deadline = time.monotonic() + 10
        while True:
            try:
                with urllib.request.urlopen(url + "/health", timeout=1):
                    break
            except OSError:
                if engine.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("engine did not start")
                time.sleep(.1)
        client = Client.remote(url)
        health_snapshots = []
        # Arm relative to workload, not bucket/server startup.
        proxy.start = time.monotonic()
        proxy.fault_start = proxy.start + 5
        for identifier, handler, inputs in (("parent", "parent", {"nodes": args.nodes}),
                                               ("timer", "timer", {}), ("canary-0", "canary", {})):
            before = time.time()
            client.submit(identifier, handler, inputs, max_attempts=10, retry_ms=200)
            submitted.append(identifier)
            if handler == "canary":
                canaries.append(dict(id=identifier, submitted=before))
        database = evidence / "effects.sqlite"
        with sqlite3.connect(database) as db:
            db.executescript("CREATE TABLE effects(id TEXT PRIMARY KEY); CREATE TABLE invocations(id TEXT, at REAL);")
        config = dict(url=url, duration=args.duration + (args.recovery_seconds + 10 if args.recovery_seconds else 0),
                      events=str(evidence / "workers.jsonl"), effects=str(database),
                      park_checkpoints=bool(args.recovery_seconds), nodes=args.nodes)
        workers_log = open(evidence / "worker-process.log", "w")
        workers = subprocess.Popen([sys.executable, __file__, "--worker", json.dumps(config)],
                                   stdout=workers_log, stderr=subprocess.STDOUT)
        workers_log.close()
        deadline = time.monotonic() + args.duration
        next_canary = time.monotonic() + 10
        next_snapshot = time.monotonic()
        while time.monotonic() < deadline:
            if time.monotonic() >= next_canary:
                identifier = f"canary-{len(canaries)}"
                before = time.time()
                try:
                    client.submit(identifier, "canary", {}, max_attempts=10, retry_ms=200)
                    submitted.append(identifier)
                    canaries.append(dict(id=identifier, submitted=before))
                except Exception as error:
                    canaries.append(dict(id=identifier, submitted=before, submission_error=str(error)))
                next_canary += 10
            if time.monotonic() >= next_snapshot:
                # /info is storage-free; diagnostic calls are not worker RPCs.
                try:
                    health_snapshots.append(dict(at=time.time(), info=client.request("/info")))
                except Exception as error:
                    health_snapshots.append(dict(at=time.time(), error=str(error)))
                try:
                    state = direct("parent")
                    previous = snapshots.get("parent", {}).get("steps", {})
                    if any(state["steps"].get(key) != value for key, value in previous.items()):
                        raise AssertionError("parent committed checkpoint changed")
                    snapshots["parent"] = dict(steps=state["steps"], at=time.time())
                    with open(evidence / "progress.jsonl", "a") as log:
                        log.write(json.dumps(dict(at=time.time(), status=state["status"],
                            attempts=state["attempts"], checkpoints=len(state["steps"]))) + "\n")
                except s3.exceptions.NoSuchKey:
                    pass
                next_snapshot += 10
            time.sleep(.2)
        report["workload_seconds"] = round(time.monotonic()-proxy.start, 3)
        try:
            health_snapshots.append(dict(at=time.time(), info=client.request("/info")))
        except Exception as error:
            health_snapshots.append(dict(at=time.time(), error=str(error)))
        report["health_snapshots"] = health_snapshots
        workers.kill() if args.recovery_seconds else workers.terminate()
        workers.wait(timeout=5)
        engine.kill() if args.recovery_seconds else engine.terminate()
        engine.wait(timeout=5)
        if args.recovery_seconds:
            report["cutoff_process_returncodes"] = dict(workers=workers.returncode, engine=engine.returncode)
        # Stop traffic before evidence inventory: these direct reads do not count as workload.
        proxy.close()
        report["requests"] = dict(total=proxy.requests, statuses=dict(proxy.counts),
                                  request_bytes=proxy.bytes_in, response_bytes=proxy.bytes_out,
                                  max_state_bytes=proxy.max_state_bytes, hottest=proxy.keys.most_common(5))
        report["requests"].update(max_proxy_in_flight=proxy.proxy_peak,
                                   max_actual_storage_in_flight=proxy.storage_peak)
        report["observed_expiry_reclaims"] = proxy.expiry_reclaims
        report["expiry_exercised"] = bool(proxy.expiry_reclaims)
        report["token_changes_before_observed_expiry"] = proxy.early_token_changes
        report["checkpoint_pointer_changes"] = proxy.checkpoint_changes
        groups = collections.defaultdict(list)
        intervals = collections.Counter()
        with open(evidence / "requests.jsonl") as log:
            for line in log:
                record = json.loads(line)
                intervals[int(record["at"])] += 1
                if record["fingerprint"]:
                    groups[(record["method"], record["key"], record["fingerprint"], record["conditional"], record["create_only"])].append(record)
        retry_groups = [records for records in groups.values() if len(records) > 1 and
                        any(record["status"] in (429, 503) for record in records)]
        report["retry_measurement"] = dict(write_groups_with_throttling_and_repeats=len(retry_groups),
            attempts_in_repeated_write_groups=sum(len(group) for group in retry_groups),
            max_attempts_for_identical_write=max((len(group) for group in retry_groups), default=0),
            requests_per_second=dict(sorted(intervals.items())),
            note="Repeated bodies include HTTP retries and independent reconciliation calls; this is amplification, not a per-request retry count. Reads cannot be separated from independent reads.")
        keys = [obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix+"/tasks/")
                          for obj in page.get("Contents", []) if obj["Key"].endswith("/state.json")]
        def read_state(key):
            return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        futures = [pool.submit(read_state, key) for key in keys]
        try:
            states = [future.result() for future in concurrent.futures.as_completed(futures, timeout=120)]
        finally:
            for future in futures:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
        by_id = {state["id"]: state for state in states}
        # A claim token cannot mutate a terminal task. True old-owner fencing while
        # a replacement is running is tested separately by tests/lifecycle.py.
        fencing_target = next((state for state in states if state["status"] == "completed"
                               and state["id"] in proxy.first_tokens), None)
        if fencing_target:
            before = direct(fencing_target["id"])
            # Restart the same unchanged server directly against RustFS; this diagnostic
            # is outside workload traffic and cannot affect captured saturation metrics.
            direct_env = dict(env, AWS_ENDPOINT=f"http://127.0.0.1:{args.rustfs_port}")
            fencing_log = open(evidence / "fencing-engine.log", "a")
            engine = subprocess.Popen([args.engine], env=direct_env, stdout=fencing_log, stderr=subprocess.STDOUT)
            fencing_log.close()
            for _ in range(50):
                try:
                    with urllib.request.urlopen(url + "/health", timeout=1):
                        break
                except OSError:
                    time.sleep(.1)
            try:
                client.request(f"/tasks/{fencing_target['id']}/complete",
                    dict(token=proxy.first_tokens[fencing_target['id']], operation_id=uuid.uuid4().hex,
                         value="stale overwrite"))
                rejected = False
            except Exception as error:
                rejected = getattr(error, "status", None) == 409
            report["terminal_mutation_rejection"] = dict(id=fencing_target["id"], rejected=rejected,
                                                unchanged=direct(fencing_target["id"]) == before)
            engine.terminate()
            engine.wait(timeout=5)
        else:
            report["terminal_mutation_rejection"] = dict(unobserved=True)
        # Verify final checkpoint bytes against accepted writes, not merely stable pointers.
        pointers = {pointer for state in states for pointer in state["steps"].values()}
        def verify_result(pointer):
            body = s3.get_object(Bucket=bucket, Key=pointer)["Body"].read()
            expected = proxy.result_hashes.get(f"/{bucket}/{pointer}")
            return pointer, expected is not None and hashlib.sha256(body).hexdigest() == expected
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            checks = list(pool.map(verify_result, pointers))
        report["checkpoint_payload_checks"] = dict(checked=len(checks), mismatches=[key for key, valid in checks if not valid])
        children = [state for state in states if state["handler"] == "child"]
        report["children"] = dict(created=len(children), statuses=dict(collections.Counter(s["status"] for s in children)),
                                  attempts=sum(s["attempts"] for s in children),
                                  additional_claims=sum(max(0, s["attempts"]-1) for s in children))
        report["parent"] = by_id["parent"]
        report["timer"] = by_id["timer"]
        timer_state = by_id["timer"]
        timer_completed = next((h["at_ms"] for h in timer_state["history"] if h["event"] == "complete"), None)
        timer_due = timer_state.get("timers", {}).get("wait", {}).get("deadline")
        report["timer_delay_ms"] = timer_completed-timer_due if timer_completed and isinstance(timer_due, int) else None
        resumed = next((h["at_ms"] for h in timer_state["history"] if h["event"] == "resume"), None)
        report["timer_fire_delay_ms"] = resumed-timer_due if resumed and isinstance(timer_due, int) else None
        for canary in canaries:
            state = by_id.get(canary["id"])
            canary["status"] = state["status"] if state else "unobserved"
            potentially_admitted = canary["id"] in proxy.potentially_admitted
            canary["admission"] = ("observed_state" if state else "unresolved_durable_write" if potentially_admitted
                else "not_admitted" if "submission_error" in canary else "acknowledged_missing_state")
            if state:
                completed = next((h["at_ms"]/1000 for h in state["history"] if h["event"] == "complete"), None)
                canary["latency_seconds"] = completed-canary["submitted"] if completed else None
                canary["attempts"] = state["attempts"]
        report["canaries"] = canaries
        report["unadmitted_canaries"] = [canary["id"] for canary in canaries if canary["admission"] == "not_admitted"]
        with sqlite3.connect(database) as db:
            invocations = db.execute("SELECT COUNT(*) FROM invocations").fetchone()[0]
            effects = db.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
            report["external_effects"] = dict(invocations=invocations, applied=effects,
                                             duplicate_callbacks=invocations-effects)
            applied_ids = {row[0] for row in db.execute("SELECT id FROM effects")}
        report["committed_outputs_correct"] = all(state["output"] == state["id"] and
            state["id"] in applied_ids and "effect" in state["steps"]
            for state in states if state["handler"] != "parent" and state["status"] == "completed")
        report["effects_without_completed_task"] = len(applied_ids -
            {state["id"] for state in states if state["status"] == "completed"})
        errors = collections.Counter()
        offered = finished = successful = 0
        mutation_operations = collections.Counter()
        barriers = {}
        with open(evidence / "workers.jsonl") as log:
            for line in log:
                event = json.loads(line)
                if event["event"] == "worker_error":
                    errors[event["error"]] += 1
                elif event["event"] == "rpc_offered":
                    offered += 1
                    if event.get("operation_id"):
                        mutation_operations[(event["path"], event["operation_id"])] += 1
                elif event["event"] == "rpc_finished":
                    finished += 1
                    successful += event["success"]
                elif event["event"] == "checkpoint_cutoff_ready":
                    barriers[event["barrier"]] = event["task_id"]
        report["worker_errors"] = dict(errors)
        report["worker_rpcs"] = dict(offered=offered, finished=finished, successful=successful,
            unfinished_at_stop=offered-finished,
            storage_attempts_per_offered_rpc=proxy.requests/offered if offered else None,
            note="Aggregate workload storage traffic / worker Client.request invocations, not an exact retry factor: numerator includes controller submissions, denominator excludes them. Invocations include /info preflight checks; a failed preflight can prevent its parent POST reaching the wire.")
        report["worker_rpcs"].update(logical_mutations=len(mutation_operations),
            sdk_mutation_retries=sum(max(0, count-1) for count in mutation_operations.values()))
        report["checkpoint_preservation"] = all(by_id[identifier]["steps"].get(step) == pointer
                  for identifier, snapshot in snapshots.items() for step, pointer in snapshot["steps"].items())
        report["checkpoint_preservation"] &= not proxy.checkpoint_changes
        report["complete"] = (len(children) == args.nodes and all(s["status"] == "completed" for s in children)
                              and by_id["parent"]["status"] == "completed")
        report["attempt_budget_preserved"] = all(state["max_attempts"] == 10 and
            0 <= state["attempts"] <= state["max_attempts"] for state in states)
        report["correctness_verified"] = (report["committed_outputs_correct"] and report["checkpoint_preservation"]
            and not report["checkpoint_payload_checks"]["mismatches"] and
            report["terminal_mutation_rejection"].get("rejected", False) and
            report["terminal_mutation_rejection"].get("unchanged", False) and report["attempt_budget_preserved"])
        report["recovery_complete"] = (report["complete"] and report["correctness_verified"] and
            report["timer"]["status"] == "completed" and
            all(c["status"] == "completed" for c in canaries if c["admission"] != "not_admitted"))
        # Raw committed final states permit independent fencing/checkpoint review.
        with open(evidence / "states.jsonl", "w") as log:
            for state in states:
                log.write(json.dumps(state)+"\n")
        if args.recovery_seconds:
            recovery = report["postload_recovery"] = dict(success=False, budget_seconds=args.recovery_seconds,
                original_metrics_frozen=True, transport="direct RustFS", workers=8, barriers=barriers)
            assert report["correctness_verified"], "cutoff correctness checks failed"
            assert set(barriers) == {"parent", "child"}, "both checkpoint-cutoff barriers must be reached"
            saved_parent, saved_child = by_id[barriers["parent"]], by_id[barriers["child"]]
            assert saved_parent["status"] == saved_child["status"] == "running"
            pointer = saved_child["steps"]["effect"]
            payload = s3.get_object(Bucket=bucket, Key=pointer)["Body"].read()
            with sqlite3.connect(database) as db:
                before_count = db.execute("SELECT COUNT(*) FROM invocations WHERE id=?", (saved_child["id"],)).fetchone()[0]
            assert before_count == 1
            recovery_started = time.monotonic()
            with open(evidence / "recovery-engine.log", "w") as log:
                engine = subprocess.Popen([args.engine], env=dict(env, AWS_ENDPOINT=f"http://127.0.0.1:{args.rustfs_port}"),
                                          stdout=log, stderr=subprocess.STDOUT)
            for _ in range(100):
                try:
                    with urllib.request.urlopen(url + "/health", timeout=1):
                        break
                except OSError:
                    assert engine.poll() is None, "recovery engine exited"
                    time.sleep(.1)
            else:
                raise AssertionError("recovery engine did not start")
            recovery_config = dict(config, park_checkpoints=False, ramp=[[0, 8]],
                duration=args.recovery_seconds + 10, events=str(evidence / "recovery-workers.jsonl"))
            with open(evidence / "recovery-worker-process.log", "w") as log:
                workers = subprocess.Popen([sys.executable, __file__, "--worker", json.dumps(recovery_config)],
                                           stdout=log, stderr=subprocess.STDOUT)
            while time.monotonic() - recovery_started < args.recovery_seconds:
                assert workers.poll() is None and engine.poll() is None, "recovery process exited"
                recovered_parent, recovered_child = direct(saved_parent["id"]), direct(saved_child["id"])
                if recovered_parent["status"] == recovered_child["status"] == "completed":
                    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                        recovered_states = list(pool.map(direct, [state["id"] for state in children]))
                    assert time.monotonic() - recovery_started < args.recovery_seconds, "postload recovery inventory exceeded budget"
                    if len(recovered_states) == args.nodes and all(state["status"] == "completed" for state in recovered_states):
                        break
                time.sleep(.2)
            else:
                raise AssertionError("postload recovery budget expired")
            assert recovered_child["steps"]["effect"] == pointer
            assert s3.get_object(Bucket=bucket, Key=pointer)["Body"].read() == payload
            assert all(recovered_parent["steps"].get(k) == v for k, v in saved_parent["steps"].items())
            assert recovered_parent["output"] == {"spawned": args.nodes}
            assert recovered_parent["attempts"] > saved_parent["attempts"] and recovered_child["attempts"] > saved_child["attempts"]
            with sqlite3.connect(database) as db:
                after_count = db.execute("SELECT COUNT(*) FROM invocations WHERE id=?", (saved_child["id"],)).fetchone()[0]
            assert after_count == before_count == 1, "saved effect callback repeated after hard kill"
            workers.terminate(); workers.wait(timeout=5)
            engine.terminate(); engine.wait(timeout=5)
            assert len(recovered_states) == args.nodes and all(state["status"] == "completed" and state["output"] == state["id"] for state in recovered_states)
            (evidence / "recovery-states.jsonl").write_text("".join(json.dumps(state)+"\n" for state in [recovered_parent, *recovered_states]))
            recovery.update(success=True, elapsed_seconds=round(time.monotonic()-recovery_started, 3),
                child_step_pointer_and_bytes_preserved=True, child_callback_invocations=after_count,
                parent_checkpoint_pointers_preserved=True, parent_status="completed", requested_children_completed=len(recovered_states),
                child=recovered_child, parent_attempts=recovered_parent["attempts"])
    except Exception as error:
        report["error"] = dict(type=type(error).__name__, message=str(error))
        raise
    finally:
        cleanup_errors = []
        for process in (workers, engine):
            if process is not None and process.poll() is None:
                try:
                    process.kill()
                    process.wait(timeout=5)
                except Exception as error:
                    cleanup_errors.append(dict(stage="process", error=str(error)))
        if proxy and not proxy.log.closed:
            try:
                proxy.close()
            except Exception as error:
                cleanup_errors.append(dict(stage="proxy", error=str(error)))
        if created:
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                    if objects:
                        response = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                        if response.get("Errors"):
                            raise RuntimeError("fixture cleanup failed")
                s3.delete_bucket(Bucket=bucket)
                report["cleaned"] = True
            except Exception as error:
                cleanup_errors.append(dict(stage="bucket", error=str(error)))
        report["cleanup_errors"] = cleanup_errors
        report["elapsed_seconds"] = round(time.monotonic()-started, 3)
        (evidence / "report.json").write_text(json.dumps(report, indent=2)+"\n")
        print(json.dumps(dict(report=str(evidence / "report.json"), complete=report.get("complete", False),
                              cleaned=report["cleaned"])), flush=True)
        if cleanup_errors:
            raise RuntimeError("cleanup failed; inspect report and owned local bucket")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", help=argparse.SUPPRESS)
    parser.add_argument("--nodes", type=int, default=10000)
    parser.add_argument("--duration", type=int, default=180)
    parser.add_argument("--fault-seconds", type=int, default=30)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--lease-ms", type=int, default=30000)
    parser.add_argument("--recovery-seconds", type=int, default=0,
                        help="opt-in checkpoint barriers, hard cutoff kill, and separate bounded restart recovery (0 disables)")
    parser.add_argument("--rustfs-port", type=int, default=19000)
    parser.add_argument("--engine", default=str(ROOT / "engine/target/release/deoos-server"))
    parser.add_argument("--report", default=str(ROOT / "outputs/saturation" / uuid.uuid4().hex))
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(args.worker))
    else:
        if not (1 <= args.nodes <= 10000 and 30 <= args.duration <= 1800 and
                0 <= args.fault_seconds < args.duration-10 and 1 <= args.rate <= 10000 and 0 <= args.recovery_seconds <= 300):
            parser.error("bounded nodes 1..10000, duration 30..1800, faults < duration-10, rate 1..10000 required")
        main(args)

#!/usr/bin/env python3
"""Opt-in real-process discovery probe. Reports and fixtures stay outside the repo.

Run with the repository's tests/.venv312/bin/python. RustFS must already run on 19000.
AWS counted traffic is forwarded over TLS and re-signed; use --transport direct to
measure direct AWS latency separately. No AWS run is implied by the default.
"""
import argparse
import collections
import concurrent.futures
import copy
import datetime
import hashlib
import http.client
import http.server
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError


class CountingProxy:
    def __init__(self, backend, credentials=None, port=19003):
        self.backend, self.credentials = backend, credentials
        self.lock = threading.Lock()
        self.rows, self.inflight = [], 0
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def forward(self):
                started = time.perf_counter()
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                operation = "LIST" if self.command == "GET" and "list-type" in query else self.command
                with owner.lock:
                    owner.inflight += 1
                status, size = 599, 0
                error_type, error_errno, reply_error_type = None, None, None
                conn = None
                try:
                    headers = dict(self.headers)
                    headers.pop("Connection", None)
                    if owner.backend == "aws":
                        for name in list(headers):
                            if name.lower() in ("authorization", "host", "x-amz-date", "x-amz-security-token", "x-amz-content-sha256"):
                                del headers[name]
                        request = AWSRequest(method=self.command, url="https://s3.us-east-1.amazonaws.com" + self.path, data=body, headers=headers)
                        S3SigV4Auth(owner.credentials, "s3", "us-east-1").add_auth(request)
                        headers = dict(request.headers)
                        conn = http.client.HTTPSConnection("s3.us-east-1.amazonaws.com", timeout=30)
                    else:
                        # Preserve the signed Host header while forwarding to the RustFS socket.
                        conn = http.client.HTTPConnection("127.0.0.1", 19000, timeout=30)
                    conn.request(self.command, self.path, body, headers)
                    response = conn.getresponse()
                    payload = response.read()
                    status, size = response.status, len(payload)
                    self.send_response(status)
                    for name, value in response.getheaders():
                        if name.lower() not in ("connection", "transfer-encoding", "content-length"):
                            self.send_header(name, value)
                    self.send_header("Content-Length", str(size))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError) as error:
                    error_type, error_errno = type(error).__name__, getattr(error, "errno", None)
                except Exception as error:
                    error_type, error_errno = type(error).__name__, getattr(error, "errno", None)
                    try:
                        self.send_error(502, "S3 forwarding failed")
                    except Exception as reply_error:
                        reply_error_type = type(reply_error).__name__
                finally:
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception as error:
                            if error_type is None:
                                error_type, error_errno = type(error).__name__, getattr(error, "errno", None)
                    with owner.lock:
                        owner.rows.append({"operation": operation, "status": status, "elapsed_ms": (time.perf_counter() - started) * 1000, "request_bytes": len(body), "response_bytes": size,
                                           "error_type": error_type, "errno": error_errno if isinstance(error_errno, int) else None,
                                           "reply_error_type": reply_error_type})
                        owner.inflight -= 1

            do_GET = do_PUT = do_HEAD = do_DELETE = do_POST = forward

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def reset(self):
        with self.lock:
            if self.inflight:
                raise RuntimeError("cannot reset while S3 requests are in flight")
            self.rows.clear()

    def snapshot(self):
        with self.lock:
            rows = list(self.rows)
        durations = [row["elapsed_ms"] for row in rows]
        return {"requests": dict(collections.Counter(row["operation"] for row in rows)), "statuses": dict(collections.Counter(str(row["status"]) for row in rows)), "request_bytes": sum(row["request_bytes"] for row in rows), "response_bytes": sum(row["response_bytes"] for row in rows), "s3_request_median_ms": statistics.median(durations) if durations else None, "s3_request_max_ms": max(durations) if durations else None,
                "error_types": dict(collections.Counter(row["error_type"] for row in rows if row.get("error_type"))),
                "errnos": dict(collections.Counter(str(row["errno"]) for row in rows if row.get("errno") is not None)),
                "reply_error_types": dict(collections.Counter(row["reply_error_type"] for row in rows if row.get("reply_error_type")))}

    def drain(self):
        end = time.monotonic() + 35
        while time.monotonic() < end:
            with self.lock:
                if not self.inflight:
                    return
            time.sleep(.05)
        raise RuntimeError("proxy still has in-flight requests")

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=("rustfs", "aws"))
    parser.add_argument("--mode", choices=("library", "server"), default="library")
    parser.add_argument("--transport", choices=("counted", "direct"), default="counted")
    parser.add_argument("--operation", choices=("claim", "list"), default="claim")
    parser.add_argument("--samples", type=int, choices=(1, 2, 3), default=3)
    parser.add_argument("--case", action="append", help="select named case(s), e.g. tasks-1000-results-4")
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    repo = args.repo.resolve()
    sys.path.insert(0, str(repo / "clients/python"))
    from deoos import Client

    bucket = "deoos-probe-" + uuid.uuid4().hex[:20]
    prefix = "discovery-" + uuid.uuid4().hex
    report = {"backend": args.backend, "mode": args.mode, "transport": args.transport, "operation": args.operation, "bucket": bucket, "prefix": prefix, "samples_per_case": args.samples, "started": datetime.datetime.now(datetime.timezone.utc).isoformat(), "cases": [], "cleaned": False, "note": "Seeded copies of real completed task templates; discovery and polling use the real SDK/engine. Counted AWS latency includes forwarding overhead and per-request TLS connections; direct mode has no request counters. Infer operational timeout limits from direct mode, not the counting proxy. Server SDK uses its unchanged 10-second request timeout. success means the probe completed and recorded observations, not that every operation met its timeout."}
    server_binary = repo / "engine/target/release" / ("deoos-engine.exe" if platform.system() == "Windows" else "deoos-engine")
    native_name = {"Darwin": "libdeoos_engine.dylib", "Linux": "libdeoos_engine.so", "Windows": "deoos_engine.dll"}[platform.system()]
    native = pathlib.Path(os.environ.get("DEOOS_NATIVE_LIBRARY", str(repo / "clients/python/deoos/native" / native_name)))
    files = [server_binary, native, repo / "clients/python/deoos/__init__.py", pathlib.Path(__file__).resolve()]
    report["artifacts"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    proxy, server, client, created = None, None, None, False

    if args.backend == "aws":
        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"), region_name="us-east-1")
        credentials = session.get_credentials().get_frozen_credentials()
        s3 = session.client("s3")
        report["account"] = session.client("sts").get_caller_identity()["Account"]
        os.environ.update(AWS_ACCESS_KEY_ID=credentials.access_key, AWS_SECRET_ACCESS_KEY=credentials.secret_key, AWS_REGION="us-east-1")
        if credentials.token:
            os.environ["AWS_SESSION_TOKEN"] = credentials.token
        else:
            os.environ.pop("AWS_SESSION_TOKEN", None)
        os.environ.pop("AWS_ENDPOINT", None)
        os.environ.pop("AWS_ALLOW_HTTP", None)
    else:
        credentials = None
        os.environ.update(AWS_ACCESS_KEY_ID="local-development", AWS_SECRET_ACCESS_KEY="local-development-only-secret", AWS_REGION="us-east-1", AWS_ENDPOINT="http://127.0.0.1:19000", AWS_ALLOW_HTTP="true")
        os.environ.pop("AWS_SESSION_TOKEN", None)
        s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", region_name="us-east-1")
    if args.transport == "counted":
        proxy = CountingProxy(args.backend, credentials)
        proxy.start()
        os.environ.update(AWS_ENDPOINT="http://127.0.0.1:19003", AWS_ALLOW_HTTP="true")
    os.environ.update(AWS_BUCKET=bucket, ENGINE_BIND="127.0.0.1:17371")
    os.environ.pop("ENGINE_TOKEN", None)
    os.environ.pop("LEASE_MS", None)
    cases = [(f"tasks-{n}-results-{c}", n, c, 0, False) for n, c in [(0, 0), (100, 0), (1000, 0), (100, 4), (1000, 4)]]
    cases += [(f"schedules-{n}-{'paused' if paused else 'future'}", 0, 0, n, paused) for n, paused in [(10, False), (100, False), (100, True)]]
    if args.case:
        selected = set(args.case)
        if selected - {row[0] for row in cases}:
            parser.error("unknown --case")
        cases = [row for row in cases if row[0] in selected]
    with tempfile.TemporaryDirectory(prefix="deoos-discovery-") as scratch:
        def stop_engine():
            nonlocal server, client
            if client is not None:
                client.close()
                client = None
            if server is not None:
                if server.poll() is None:
                    server.kill()
                server.wait()
                server = None
            if proxy:
                proxy.drain()

        def start_engine(case_prefix):
            nonlocal server, client
            os.environ["EXECUTION_PREFIX"] = case_prefix
            if args.mode == "library":
                client = Client(bucket=bucket, prefix=case_prefix)
            else:
                log = open(pathlib.Path(scratch) / "server.log", "ab")
                server = subprocess.Popen([str(server_binary)], env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT)
                log.close()
                for _ in range(100):
                    if server.poll() is not None:
                        raise RuntimeError("benchmark server exited before readiness")
                    try:
                        urllib.request.urlopen("http://127.0.0.1:17371/health", timeout=1).close()
                        break
                    except OSError:
                        time.sleep(.05)
                else:
                    raise RuntimeError("benchmark server was not ready")
                client = Client.remote("http://127.0.0.1:17371")

        try:
            s3.create_bucket(Bucket=bucket)
            created = True
            if args.backend == "aws":
                s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={key: True for key in ["BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"]})
            templates = {}
            for count in {row[2] for row in cases}:
                template_prefix = prefix + f"/template-{count}"
                start_engine(template_prefix)
                def handler(ctx, inputs):
                    for step in range(count):
                        ctx.step(f"step-{step}", lambda: "x" * 1024)
                    return {"checkpoint_count": count}
                client.submit("template", "bench.v1", {})
                assert client.run_once({"bench.v1": handler})
                state = client.inspect("template")
                assert state["status"] == "completed"
                payloads = {name: client.request(f"/tasks/template/steps/{name}") for name in state["steps"]}
                templates[count] = state, payloads
                stop_engine()
            for name, task_count, checkpoints, schedule_count, paused in cases:
                case_prefix = prefix + "/" + name
                template, payloads = templates[checkpoints]
                objects = []
                for index in range(task_count):
                    task_id = f"completed-{index:06d}"
                    state = copy.deepcopy(template)
                    state["id"], state["revision"] = task_id, uuid.uuid4().hex
                    state["steps"] = {}
                    for step, value in payloads.items():
                        result_key = f"{case_prefix}/tasks/{task_id}/results/{step}/1/{uuid.uuid4().hex}.json"
                        state["steps"][step] = result_key
                        objects.append((result_key, json.dumps(value).encode()))
                    objects.append((f"{case_prefix}/tasks/{task_id}/state.json", json.dumps(state).encode()))
                with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                    list(pool.map(lambda row: s3.put_object(Bucket=bucket, Key=row[0], Body=row[1]), objects))
                start_engine(case_prefix)
                anchor = int(time.time() * 1000) + 86_400_000
                for index in range(schedule_count):
                    identifier = f"schedule-{index:03d}"
                    client.schedule(identifier, "bench.v1", {}, interval_ms=60_000, first_due_ms=anchor)
                    if paused:
                        client.pause_schedule(identifier)
                case = {"name": name, "terminal_tasks": task_count, "checkpoints_per_task": checkpoints, "schedules": schedule_count, "paused": paused, "seeded_task_objects": len(objects), "seeded_task_bytes": sum(len(body) for _, body in objects), "samples": []}
                for sample in range(args.samples):
                    if client is None:
                        start_engine(case_prefix)
                    if proxy:
                        proxy.drain()
                        proxy.reset()
                    started = time.perf_counter()
                    observation = {"sample": sample}
                    try:
                        if args.operation == "claim":
                            result = client.request("/claim", {"worker": "discovery-probe", "handlers": ["bench.v1"]})
                            assert result["task"] is None
                        else:
                            result = client.list_tasks()
                            assert len(result["tasks"]) == min(100, task_count)
                            assert result["truncated"] == (task_count > 100)
                            observation.update(returned_tasks=len(result["tasks"]), truncated=result["truncated"])
                        observation["completed"] = True
                    except Exception as error:
                        observation.update(completed=False, error_type=type(error).__name__, error=str(error))
                    observation["elapsed_ms"] = (time.perf_counter() - started) * 1000
                    if not observation["completed"]:
                        # Stop the process after timeout so subsequent samples cannot mix traffic.
                        stop_engine()
                    if proxy:
                        proxy.drain()
                        observation["s3"] = proxy.snapshot()
                    case["samples"].append(observation)
                durations = [sample["elapsed_ms"] for sample in case["samples"] if sample["completed"]]
                case["median_ms"] = statistics.median(durations) if durations else None
                case["min_ms"] = min(durations) if durations else None
                case["max_ms"] = max(durations) if durations else None
                case["completed_samples"] = len(durations)
                report["cases"].append(case)
                print(json.dumps(case), flush=True)
                stop_engine()
            report["success"] = True  # Recorded timeouts are observations, not absence of workload.
        except BaseException as error:
            report.update(success=False, error_type=type(error).__name__, error=str(error))
            raise
        finally:
            cleanup_errors = []
            try:
                stop_engine()
            except Exception as error:
                cleanup_errors.append(f"engine shutdown: {type(error).__name__}")
            try:
                if created:
                    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                        if objects:
                            response = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                            if response.get("Errors"):
                                raise RuntimeError("benchmark object cleanup failed")
                    s3.delete_bucket(Bucket=bucket)
                    try:
                        s3.head_bucket(Bucket=bucket)
                    except ClientError as error:
                        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
                    else:
                        raise RuntimeError("benchmark bucket still exists after cleanup")
                    report["cleaned"] = True
            except Exception as error:
                cleanup_errors.append(f"storage cleanup: {type(error).__name__}")
            try:
                if proxy:
                    proxy.close()
            except Exception as error:
                cleanup_errors.append(f"proxy shutdown: {type(error).__name__}")
            if cleanup_errors:
                report.update(success=False, cleanup_errors=cleanup_errors)
            stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_dir = repo.parent / "outputs/evidence"
            output_dir.mkdir(parents=True, exist_ok=True)
            output = output_dir / f"discovery-{args.backend}-{args.mode}-{args.transport}-{args.operation}-{stamp}.json"
            output.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps({"report": str(output), "success": report.get("success"), "cleaned": report["cleaned"]}), flush=True)
            if cleanup_errors:
                raise RuntimeError("probe cleanup incomplete; inspect its report")


if __name__ == "__main__":
    main()

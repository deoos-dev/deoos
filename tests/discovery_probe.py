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
import re
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
    def __init__(self, backend, credentials=None, port=19003, omit_list_etags=False, after_list=None):
        self.backend, self.credentials = backend, credentials
        if (omit_list_etags or after_list is not None) and backend != "rustfs":
            raise ValueError("LIST response diagnostics are local RustFS only")
        self.omit_list_etags = omit_list_etags
        self.after_list = after_list
        self.lock = threading.Lock()
        self.rows, self.inflight = [], 0
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def forward(self):
                started = time.perf_counter()
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                parsed = urllib.parse.urlsplit(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                operation = "LIST" if self.command == "GET" and "list-type" in query else self.command
                parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
                prefixes = [urllib.parse.unquote(value) for value in query.get("prefix", [])]
                parts.extend(part for value in prefixes for part in value.split("/"))
                family = ("active" if "active" in parts else "tasks" if "tasks" in parts else "schedules" if "schedules" in parts
                          else "other")
                with owner.lock:
                    owner.inflight += 1
                status, size = 599, 0
                list_etags_omitted = 0
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
                    if owner.omit_list_etags and operation == "LIST":
                        # S3 ListObjects carries object ETags in XML, not response headers.
                        payload, list_etags_omitted = re.subn(rb"<ETag>.*?</ETag>", b"", payload, flags=re.DOTALL)
                    status, size = response.status, len(payload)
                    if owner.after_list is not None and operation == "LIST":
                        # Test-only hook: after fetching a full LIST but before the
                        # engine receives it and issues follow-up GETs.
                        owner.after_list(family)
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
                        owner.rows.append({"operation": operation, "family": family, "status": status, "elapsed_ms": (time.perf_counter() - started) * 1000, "request_bytes": len(body), "response_bytes": size, "list_etags_omitted": list_etags_omitted,
                                           "list_prefixes": prefixes if operation == "LIST" else [],
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
        families = collections.defaultdict(list)
        for row in rows:
            families[row["family"]].append(row)
        return {"requests": dict(collections.Counter(row["operation"] for row in rows)), "statuses": dict(collections.Counter(str(row["status"]) for row in rows)), "request_bytes": sum(row["request_bytes"] for row in rows), "response_bytes": sum(row["response_bytes"] for row in rows), "s3_request_median_ms": statistics.median(durations) if durations else None, "s3_request_max_ms": max(durations) if durations else None,
                "list_prefixes": sorted({prefix for row in rows for prefix in row.get("list_prefixes", [])}),
                "error_types": dict(collections.Counter(row["error_type"] for row in rows if row.get("error_type"))),
                "errnos": dict(collections.Counter(str(row["errno"]) for row in rows if row.get("errno") is not None)),
                "reply_error_types": dict(collections.Counter(row["reply_error_type"] for row in rows if row.get("reply_error_type"))),
                "list_etags_omitted": sum(row.get("list_etags_omitted", 0) for row in rows),
                "request_families": {name: {"requests": len(items),
                    "operations": dict(collections.Counter(row["operation"] for row in items)),
                    "statuses": dict(collections.Counter(str(row["status"]) for row in items)),
                    "request_bytes": sum(row["request_bytes"] for row in items),
                    "response_bytes": sum(row["response_bytes"] for row in items)}
                    for name, items in sorted(families.items())}}

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


def require_bucket_region(s3, bucket, region, expected_owner):
    actual = s3.get_bucket_location(Bucket=bucket, ExpectedBucketOwner=expected_owner).get("LocationConstraint")
    actual = {None: "us-east-1", "EU": "eu-west-1"}.get(actual, actual)
    if actual != region:
        raise ValueError("precreated bucket region must match verified EC2 region")
    return actual


class PrecreatedBucket:
    """Own only one generated discovery prefix in a controller-created bucket."""
    def __init__(self, s3, bucket, prefix, expected_owner):
        self.s3, self.bucket, self.prefix, self.expected_owner = s3, bucket, prefix, expected_owner
        if not re.fullmatch(r"discovery-[0-9a-f]{32}", prefix):
            raise ValueError("expected the exact generated discovery run prefix")
        self.owned = False

    def admit(self):
        if self.s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1,
                                   ExpectedBucketOwner=self.expected_owner).get("Contents"):
            raise RuntimeError("precreated discovery bucket must be empty")
        self.owned = True

    def cleanup(self):
        if not self.owned:
            return {"cleaned": False, "cleanup_skipped": "namespace ownership not acquired"}
        boundary = self.prefix + "/"
        for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=boundary, ExpectedBucketOwner=self.expected_owner):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if any(not obj["Key"].startswith(boundary) for obj in objects):
                raise RuntimeError("listing escaped the owned discovery namespace")
            if objects and self.s3.delete_objects(Bucket=self.bucket, ExpectedBucketOwner=self.expected_owner,
                                                  Delete={"Objects": objects}).get("Errors"):
                raise RuntimeError("owned discovery namespace cleanup failed")
        if self.s3.list_objects_v2(Bucket=self.bucket, Prefix=boundary, MaxKeys=1,
                                   ExpectedBucketOwner=self.expected_owner).get("Contents"):
            raise RuntimeError("owned discovery namespace still contains objects")
        self.s3.head_bucket(Bucket=self.bucket, ExpectedBucketOwner=self.expected_owner)
        return {"cleaned": True, "external_bucket_retained": True, "owned_prefix_absent": True}


def sample_gate(cases, expected_cases, expected_samples, recorded_samples):
    recorded = (len(cases) == expected_cases and recorded_samples == expected_samples
                and all(case.get("recording_complete") for case in cases))
    complete = (recorded and all(case.get("qualification_complete") for case in cases)
                and sum(case.get("completed_samples", 0) for case in cases) == expected_samples)
    return recorded, complete


def aws_storage(expected_account, transport):
    if not expected_account or not expected_account.isascii() or len(expected_account) != 12 or not expected_account.isdigit():
        raise ValueError("DEOOS_EXPECTED_AWS_ACCOUNT must contain 12 ASCII digits for AWS")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    metadata = "http://169.254.169.254/latest/"
    with opener.open(urllib.request.Request(metadata + "api/token", method="PUT",
                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=2) as response:
        token = response.read().decode()
    with opener.open(urllib.request.Request(metadata + "dynamic/instance-identity/document",
                     headers={"X-aws-ec2-metadata-token": token}), timeout=2) as response:
        identity = json.load(response)
    region = identity.get("region")
    if not region or not identity.get("instanceId") or identity.get("accountId") != expected_account:
        raise ValueError("EC2 identity must match the expected account and include instance and region")
    if os.environ.get("AWS_REGION", region) != region:
        raise ValueError("AWS_REGION must match the verified EC2 region")
    if transport == "counted" and region != "us-east-1":
        raise ValueError("counted AWS forwarding currently supports verified us-east-1 only")
    from botocore.config import Config
    suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"), region_name=region)
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2},
                    ignore_configured_endpoint_urls=True)
    caller = session.client("sts", endpoint_url=f"https://sts.{region}.{suffix}", config=config).get_caller_identity()
    if caller.get("Account") != expected_account:
        raise ValueError("AWS credential account does not match expected EC2 account")
    if transport == "counted" and ":assumed-role/" not in caller.get("Arn", ""):
        raise ValueError("counted AWS forwarding requires assumed-role credentials")
    s3 = session.client("s3", endpoint_url=f"https://s3.{region}.{suffix}", config=config)
    credentials = session.get_credentials().get_frozen_credentials()
    os.environ.update(AWS_ACCESS_KEY_ID=credentials.access_key, AWS_SECRET_ACCESS_KEY=credentials.secret_key,
                      AWS_REGION=region, AWS_ENDPOINT=s3.meta.endpoint_url, AWS_ALLOW_HTTP="false")
    if credentials.token:
        os.environ["AWS_SESSION_TOKEN"] = credentials.token
    else:
        os.environ.pop("AWS_SESSION_TOKEN", None)
    return s3, {"placement": "verified EC2 worker host and same-region S3 endpoint",
                "region": region, "instance_id": identity["instanceId"],
                "expected_account_verified": True, "cloud_local": True}, region, credentials


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=("rustfs", "aws"))
    parser.add_argument("--mode", choices=("library", "server"), default="library")
    parser.add_argument("--transport", choices=("counted", "direct"), default="counted")
    parser.add_argument("--operation", choices=("claim", "list"), default="claim")
    parser.add_argument("--samples", type=int, choices=(1, 2, 3), default=3)
    parser.add_argument("--case", action="append", help="select named case(s), e.g. tasks-1000-results-4")
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1])
    parser.add_argument("--bucket", help="use a controller-created empty AWS bucket; clean only this discovery prefix")
    parser.add_argument("--omit-list-etags", action="store_true",
                        help="local RustFS counted diagnostic: remove ETags from ListObjects responses")
    args = parser.parse_args()
    if args.bucket and args.backend != "aws":
        parser.error("--bucket is the precreated AWS controller contract")
    if args.omit_list_etags and (args.backend != "rustfs" or args.transport != "counted"):
        parser.error("--omit-list-etags requires local RustFS with counted transport")
    if args.bucket and (not 3 <= len(args.bucket) <= 63 or not args.bucket.isascii()
                        or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789.-" for char in args.bucket)):
        parser.error("--bucket requires a valid lowercase S3 bucket name")
    repo = args.repo.resolve()
    sys.path.insert(0, str(repo / "clients/python"))
    from deoos import Client

    bucket = args.bucket or "deoos-probe-" + uuid.uuid4().hex[:20]
    prefix = "discovery-" + uuid.uuid4().hex
    report = {"backend": args.backend, "mode": args.mode, "transport": args.transport, "operation": args.operation,
              "bucket": bucket, "prefix": prefix, "precreated_bucket": bool(args.bucket),
              "omit_list_etags": args.omit_list_etags, "samples_per_case": args.samples,
              "expected_samples": 0, "recorded_samples": 0, "completed_samples": 0,
              "observations_recorded": False, "qualification_complete": False,
              "started": datetime.datetime.now(datetime.timezone.utc).isoformat(), "cases": [],
              "cleaned": False,
              "note": "Seeded copies of real completed task templates; discovery and polling use the real SDK/engine. Counted AWS latency includes forwarding overhead and per-request TLS connections; direct mode has no request counters. Infer operational timeout limits from direct mode, not the counting proxy. Server SDK uses its unchanged 10-second request timeout. observations_recorded means every requested sample was written to the report; qualification_complete requires every sample to complete without error."}
    server_binary = repo / "engine/target/release" / ("deoos-engine.exe" if platform.system() == "Windows" else "deoos-engine")
    native_name = {"Darwin": "libdeoos_engine.dylib", "Linux": "libdeoos_engine.so", "Windows": "deoos_engine.dll"}[platform.system()]
    native = pathlib.Path(os.environ.get("DEOOS_NATIVE_LIBRARY", str(repo / "clients/python/deoos/native" / native_name)))
    files = [server_binary, native, repo / "clients/python/deoos/__init__.py", pathlib.Path(__file__).resolve()]
    report["artifacts"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    proxy, server, client, created, external = None, None, None, False, None

    if args.backend == "aws" and args.bucket:
        expected = os.environ.get("DEOOS_EXPECTED_AWS_ACCOUNT")
        s3, placement, region, credentials = aws_storage(expected, args.transport)
        report["placement"] = placement
    elif args.backend == "aws":
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
        proxy = CountingProxy(args.backend, credentials, omit_list_etags=args.omit_list_etags)
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
    report["expected_samples"] = args.samples * len(cases)
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
            if args.bucket:
                require_bucket_region(s3, bucket, region, os.environ["DEOOS_EXPECTED_AWS_ACCOUNT"])
                external = PrecreatedBucket(s3, bucket, prefix, os.environ["DEOOS_EXPECTED_AWS_ACCOUNT"])
                external.admit()
                report.update(bucket_region_verified=region, prefix_owned=True,
                              owned_run_prefix=prefix, external_bucket_retained=True)
            else:
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
                # These are already-terminal records in the current active-index layout.
                # A ready sentinel separates cold-engine measurement from legacy backfill.
                s3.put_object(Bucket=bucket, Key=case_prefix + "/active-index.json",
                              Body=b'{"version":1,"status":"ready"}')
                start_engine(case_prefix)
                anchor = int(time.time() * 1000) + 86_400_000
                for index in range(schedule_count):
                    identifier = f"schedule-{index:03d}"
                    client.schedule(identifier, "bench.v1", {}, interval_ms=60_000, first_due_ms=anchor)
                    if paused:
                        client.pause_schedule(identifier)
                case = {"name": name, "terminal_tasks": task_count, "checkpoints_per_task": checkpoints, "schedules": schedule_count, "paused": paused, "seeded_task_objects": len(objects), "seeded_task_bytes": sum(len(body) for _, body in objects), "samples": []}
                case["expected_samples"] = args.samples
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
                    report["recorded_samples"] += 1
                    report["completed_samples"] += int(observation["completed"])
                durations = [sample["elapsed_ms"] for sample in case["samples"] if sample["completed"]]
                case["median_ms"] = statistics.median(durations) if durations else None
                case["min_ms"] = min(durations) if durations else None
                case["max_ms"] = max(durations) if durations else None
                case["completed_samples"] = len(durations)
                case["recording_complete"] = len(case["samples"]) == args.samples
                case["qualification_complete"] = case["recording_complete"] and case["completed_samples"] == args.samples
                report["cases"].append(case)
                print(json.dumps(case), flush=True)
                stop_engine()
            report["observations_recorded"], report["qualification_complete"] = sample_gate(
                report["cases"], len(cases), report["expected_samples"], report["recorded_samples"])
            report["success"] = report["qualification_complete"]
            if not report["qualification_complete"]:
                raise RuntimeError("one or more requested discovery samples failed or timed out")
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
                if external:
                    report.update(external.cleanup())
                elif created:
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
            if not report.get("cleaned"):
                report["success"] = False
            if args.bucket and not report.get("prefix_owned"):
                report["cleanup"] = {"cleaned": False, "cleanup_skipped": "namespace ownership not acquired"}
            stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_dir = repo.parent / "outputs/evidence"
            output_dir.mkdir(parents=True, exist_ok=True)
            output = output_dir / f"discovery-{args.backend}-{args.mode}-{args.transport}-{args.operation}-{stamp}.json"
            report["observations_recorded"], report["qualification_complete"] = sample_gate(
                report["cases"], len(cases), report["expected_samples"], report["recorded_samples"])
            report["recording_completed"] = True
            output.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps({"report": str(output), "success": report.get("success"),
                              "observations_recorded": report["observations_recorded"],
                              "completed_samples": report["completed_samples"],
                              "expected_samples": report["expected_samples"],
                              "cleaned": report["cleaned"]}), flush=True)
            if cleanup_errors:
                raise RuntimeError("probe cleanup incomplete; inspect its report")


if __name__ == "__main__":
    main()

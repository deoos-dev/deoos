#!/usr/bin/env python3
"""Bounded real-worker workflow load probe; reports stay outside the repository.

Requires built SDKs, Node, boto3, and existing local RustFS on port 19000.
AWS runs require verified EC2 IMDSv2 placement and an expected account; this script
creates only a temporary test bucket, never compute/network infrastructure.
"""
import argparse
import collections
import datetime
import hashlib
import json
import math
import os
import pathlib
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from discovery_probe import CountingProxy
from use_cases import IntegrationFixture, stop, wait

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "clients/python"))
from deoos import Client


def percentiles(values):
    values = sorted(values)
    return {f"p{percent}": values[math.ceil(len(values) * percent / 100) - 1]
            if values else None for percent in (50, 95, 99)}


def process_sample(pids):
    if not shutil.which("ps") or os.name == "nt":
        return None
    result = subprocess.run(["ps", "-p", ",".join(map(str, pids)), "-o", "pid=",
                             "-o", "time=", "-o", "rss="], capture_output=True, text=True)
    if result.returncode and not result.stdout.strip():
        return None
    rows = {}
    for line in result.stdout.splitlines():
        identifier, cpu, rss = line.split()
        seconds = 0.0
        for part in cpu.split(":"):
            seconds = seconds * 60 + float(part)
        rows[int(identifier)] = {"cpu_seconds": seconds, "rss_kib": int(rss)}
    return rows


def storage(backend, transport):
    if backend == "rustfs":
        os.environ.update(AWS_ACCESS_KEY_ID="local-development",
                          AWS_SECRET_ACCESS_KEY="local-development-only-secret",
                          AWS_REGION="us-east-1", AWS_ENDPOINT="http://127.0.0.1:19000", AWS_ALLOW_HTTP="true")
        os.environ.pop("AWS_SESSION_TOKEN", None)
        os.environ.pop("AWS_PROFILE", None)
        return boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT"], region_name="us-east-1"), {
            "placement": "local loopback endpoint; expected existing RustFS container published port",
            "endpoint": os.environ["AWS_ENDPOINT"], "cloud_local": False}, "us-east-1", None
    expected = os.environ.get("DEOOS_EXPECTED_AWS_ACCOUNT")
    if not expected or not expected.isascii() or len(expected) != 12 or not expected.isdigit():
        raise ValueError("DEOOS_EXPECTED_AWS_ACCOUNT must contain 12 ASCII digits for AWS")
    # Bypass HTTP proxy environment variables; require IMDSv2, not hostname guesses.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    metadata = "http://169.254.169.254/latest/"
    with opener.open(urllib.request.Request(metadata + "api/token", method="PUT",
                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=2) as response:
        token = response.read().decode()
    with opener.open(urllib.request.Request(metadata + "dynamic/instance-identity/document",
                     headers={"X-aws-ec2-metadata-token": token}), timeout=2) as response:
        identity = json.load(response)
    region = identity.get("region")
    if not region or not identity.get("instanceId") or identity.get("accountId") != expected:
        raise ValueError("EC2 identity must name the expected account and an instance region")
    if os.environ.get("AWS_REGION", region) != region:
        raise ValueError("AWS_REGION must match the verified EC2 region")
    if transport == "counted" and region != "us-east-1":
        raise ValueError("counted AWS forwarding currently supports verified us-east-1 only")
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"), region_name=region)
    suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    caller = session.client("sts", endpoint_url=f"https://sts.{region}.{suffix}",
                            config=Config(ignore_configured_endpoint_urls=True)).get_caller_identity()
    if caller["Account"] != expected:
        raise ValueError("AWS credential account does not match DEOOS_EXPECTED_AWS_ACCOUNT")
    if transport == "counted" and ":assumed-role/" not in caller.get("Arn", ""):
        raise ValueError("counted AWS forwarding requires assumed-role credentials")
    s3 = session.client("s3", endpoint_url=f"https://s3.{region}.{suffix}")
    credentials = session.get_credentials().get_frozen_credentials()
    os.environ.update(AWS_ACCESS_KEY_ID=credentials.access_key, AWS_SECRET_ACCESS_KEY=credentials.secret_key,
                      AWS_REGION=region, AWS_ENDPOINT=s3.meta.endpoint_url, AWS_ALLOW_HTTP="false")
    if credentials.token:
        os.environ["AWS_SESSION_TOKEN"] = credentials.token
    else:
        os.environ.pop("AWS_SESSION_TOKEN", None)
    return s3, {"placement": "verified EC2 worker host and same-region S3 endpoint",
                "region": region, "instance_id": identity["instanceId"],
                "endpoint": s3.meta.endpoint_url, "cloud_local": True}, region, credentials


def run_round(client, mode, worker_count, round_number, history, args, env, work, fixture, proxy, server):
    if proxy:
        proxy.drain()
        proxy.reset()
    logs, workers, states, admissions, submission_errors, inspection_errors = [], [], {}, {}, [], 0
    with fixture.lock:
        effects_before, requests_before = len(fixture.effects), len(fixture.requests)
    started = time.monotonic()
    deadline = started + args.duration
    peak_backlog, peak_rss, peak_cpu, cpu_seconds = 0, None, None, 0.0
    previous = process_sample([os.getpid()] + ([server.pid] if server else []))
    previous_at = started
    try:
        for index in range(worker_count):
            language = args.language if args.language != "mixed" else ("python" if index % 2 == 0 else "typescript")
            log = work / f"{mode}-{worker_count}-{round_number}-{index}.log"
            logs.append(log)
            with log.open("wb") as handle:
                command = [sys.executable, str(work / "use_cases.py")] if language == "python" else [shutil.which("node"), str(work / "use_cases.mjs")]
                workers.append(subprocess.Popen(command + ["work"], env=env, stdout=handle, stderr=subprocess.STDOUT))
        for index in range(args.tasks):
            if time.monotonic() >= deadline:
                break
            identifier = f"burst-{env['EXECUTION_PREFIX'].split('/')[-1]}-{round_number}-{index}"
            admitted = time.monotonic()
            try:
                inputs = {"service_url": fixture.url}
                inputs.update({"event_id": identifier, "payload": {"index": index}} if args.workload == "webhook"
                              else {"source": identifier, "page_count": 2})
                client.submit(identifier, "usecase.webhook.v1" if args.workload == "webhook"
                              else "usecase.daily-import.v1", inputs, max_attempts=3, retry_ms=1000)
                admissions[identifier] = admitted
            except Exception as error:
                submission_errors.append({"id": identifier, "type": type(error).__name__})
        latency = []
        while time.monotonic() < deadline:
            observed_pending = 0
            for identifier in admissions.keys() - states.keys():
                try:
                    state = client.inspect(identifier)
                    if state["status"] in ("completed", "failed", "cancelled"):
                        states[identifier] = state
                        if state["status"] == "completed":
                            latency.append((time.monotonic() - admissions[identifier]) * 1000)
                    else:
                        observed_pending += 1
                except Exception:
                    inspection_errors += 1
                if time.monotonic() >= deadline:
                    break
            peak_backlog = max(peak_backlog, observed_pending)
            now = time.monotonic()
            sample = process_sample([os.getpid()] + [worker.pid for worker in workers]
                                    + ([server.pid] if server else []))
            if sample is not None:
                peak_rss = max(peak_rss or 0, sum(row["rss_kib"] for row in sample.values()))
                if previous is not None:
                    delta = sum(max(0, row["cpu_seconds"] - previous.get(pid, {"cpu_seconds": 0})["cpu_seconds"])
                                for pid, row in sample.items())
                    cpu_seconds += delta
                    peak_cpu = max(peak_cpu or 0, 100 * delta / max(.001, now - previous_at))
                previous, previous_at = sample, now
            if len(states) == len(admissions) or any(worker.poll() is not None for worker in workers):
                break
            time.sleep(min(args.inspect_interval, max(0, deadline - time.monotonic())))
        elapsed = time.monotonic() - started
        exit_codes = [worker.poll() for worker in workers]
    finally:
        for worker in workers:
            stop(worker)
        if proxy:
            proxy.drain()
    counts = collections.Counter(state["status"] for state in states.values())
    errors = [line for log in logs for line in log.read_text(errors="replace").splitlines() if line]
    error_logs = []
    if errors:
        diagnostic = ROOT.parent / "outputs/evidence" / ("load-worker-errors-" + uuid.uuid4().hex)
        diagnostic.mkdir(parents=True)
        for log in logs:
            if log.stat().st_size:
                saved = diagnostic / log.name
                shutil.copyfile(log, saved); saved.chmod(0o600)
                error_logs.append(str(saved))
    with fixture.lock:
        effects, requests = len(fixture.effects) - effects_before, len(fixture.requests) - requests_before
    storage_counts = proxy.snapshot() if proxy else None
    return {"namespace": "fresh" if round_number == 0 else "retained-history",
            "retained_terminal_workflows_before_burst": history, "mode": mode, "workers": worker_count,
            "language": args.language,
            "worker_languages": [args.language if args.language != "mixed" else ("python" if index % 2 == 0 else "typescript") for index in range(worker_count)],
            "workload": args.workload, "requested_workflows": args.tasks,
            "submitted_workflows": len(admissions),
            "completed": counts["completed"], "failed": counts["failed"], "cancelled": counts["cancelled"],
            "pending_or_unobserved": len(admissions) - len(states), "elapsed_seconds": elapsed,
            "completed_per_second": counts["completed"] / elapsed, "latency_ms": percentiles(latency),
            "peak_sampled_workflow_backlog": peak_backlog, "backlog_upper_bound": len(admissions),
            "peak_observed_rss_kib": peak_rss,
            "observed_cpu_seconds": cpu_seconds, "peak_observed_cpu_percent": peak_cpu,
            "task_retries_observed": sum(max(0, state["attempts"] - 1) for state in states.values()),
            "submission_errors_indeterminate": submission_errors, "inspection_errors": inspection_errors,
            "worker_error_lines": len(errors), "worker_exit_codes_before_shutdown": exit_codes,
            "worker_error_logs": error_logs,
            "fixture_effects": effects, "fixture_requests": requests,
            "storage_conditional_conflict_responses": sum(storage_counts["statuses"].get(status, 0) for status in ("409", "412")) if storage_counts else None,
            "storage": storage_counts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("rustfs", "aws"), default="rustfs")
    parser.add_argument("--transport", choices=("direct", "counted"), default="direct")
    parser.add_argument("--mode", action="append", choices=("library", "server"))
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 4])
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--inspect-interval", type=float, default=1)
    parser.add_argument("--language", choices=("mixed", "python", "typescript"), default="mixed")
    parser.add_argument("--workload", choices=("webhook", "import"), default="webhook")
    args = parser.parse_args()
    if not 1 <= args.tasks <= 1000 or not 1 <= args.duration <= 300 or not .1 <= args.inspect_interval <= 10 or any(not 1 <= count <= 16 for count in args.workers):
        parser.error("tasks1-1000, duration1-300 seconds, inspect-interval0.1-10 seconds, workers1-16 required")
    server_binary = pathlib.Path(os.environ.get("ENGINE_BINARY", ROOT / "engine/target/release" / ("deoos-engine.exe" if os.name == "nt" else "deoos-engine")))
    native_name = {"Darwin": "libdeoos_engine.dylib", "Linux": "libdeoos_engine.so", "Windows": "deoos_engine.dll"}[platform.system()]
    files = [pathlib.Path(__file__), ROOT / "tests/use_cases.py", ROOT / "tests/discovery_probe.py",
             ROOT / "examples/use_cases.py", ROOT / "examples/use_cases.mjs", server_binary,
             pathlib.Path(os.environ.get("DEOOS_NATIVE_LIBRARY", ROOT / "clients/python/deoos/native" / native_name)),
             pathlib.Path(os.environ.get("DEOOS_NODE_LIBRARY", ROOT / "clients/typescript/dist/native/deoos_node.node")),
             ROOT / "clients/python/deoos/__init__.py", ROOT / "clients/typescript/dist/index.js",
             ROOT / "clients/typescript/src/index.ts", ROOT / "engine/src/lib.rs", ROOT / "engine/src/main.rs"]
    report = {"started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "host": {"platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count()},
              "arguments": vars(args), "sha256": {str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
              "cases": [], "cleaned": False,
              "counted_status_599_meaning": "Proxy sentinel: no complete backend HTTP response recorded.",
              "method": "Burst submission to actual webhook or two-page import handler; real CLI workers. Throughput counts completed top-level workflows and includes worker startup, admission and terminal observation. Nearest-rank latency covers only observed completed workflows, from before submission to first terminal inspection; unfinished latencies are censored. It includes polling/inspection lag; sequential scans can exceed the configured inspection interval. Peak backlog counts nonterminal top-level workflows observed during sequential scans; fast completions before the first scan can be missed. Counted transport adds proxy overhead, including fresh TLS connections for AWS, and measures diagnostic request counts rather than direct capacity. Counts include driver submit/inspect storage traffic, excluding untimed task inventory and cleanup. CPU/RSS sample driver (including fixture/proxy/native client), workers and shared server; excludes RustFS container. CPU uses ps cumulative-time deltas; no ps means unavailable. Retries cover observed terminal workflows only. No general production throughput claim."}
    s3, placement, region, credentials = storage(args.backend, args.transport)
    report["store"] = placement
    bucket = "deoos-load-" + uuid.uuid4().hex[:20]
    report["bucket"] = bucket
    created, fixture, proxy, client, server = False, None, None, None, None
    for key in list(os.environ):
        if key.startswith("DEOOS_STORAGE_"):
            os.environ.pop(key)
    os.environ.update(AWS_BUCKET=bucket, DEOOS_STORAGE_PROVIDER="s3")
    try:
        create = {"Bucket": bucket}
        if args.backend == "aws" and region != "us-east-1":
            create["CreateBucketConfiguration"] = {"LocationConstraint": region}
        created = True  # Cleanup owns this exact random name even after an ambiguous create response.
        s3.create_bucket(**create)
        if args.backend == "aws":
            s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={key: True for key in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")})
        if args.transport == "counted":
            proxy = CountingProxy(args.backend, credentials, port=0)
            proxy.start()
            os.environ["AWS_ENDPOINT"] = f"http://127.0.0.1:{proxy.server.server_port}"
            os.environ["AWS_ALLOW_HTTP"] = "true"  # HTTP is confined to this loopback proxy.
        fixture = IntegrationFixture()
        with tempfile.TemporaryDirectory(prefix="deoos-load-") as directory:
            work = pathlib.Path(directory)
            for name in ("use_cases.py", "use_cases.mjs"):
                shutil.copyfile(ROOT / "examples" / name, work / name)
            (work / "node_modules").mkdir()
            (work / "node_modules/deoos").symlink_to(ROOT / "clients/typescript", target_is_directory=True)
            for mode in args.mode or ["library", "server"]:
                for workers in args.workers:
                    prefix = "load/" + uuid.uuid4().hex
                    os.environ.update(EXECUTION_PREFIX=prefix, DEOOS_MODE=mode)
                    env = dict(os.environ, PYTHONPATH=str(ROOT / "clients/python"), SERVICE_URL=fixture.url)
                    if mode == "server":
                        with socket.socket() as sock:
                            sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
                        env.update(ENGINE_BIND=f"127.0.0.1:{port}", ENGINE_URL=f"http://127.0.0.1:{port}", ENGINE_TOKEN=uuid.uuid4().hex)
                        server = subprocess.Popen([str(server_binary)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        def healthy():
                            if server.poll() is not None:
                                raise RuntimeError("load server exited before readiness")
                            try:
                                with urllib.request.urlopen(env["ENGINE_URL"] + "/health", timeout=1) as response:
                                    return response.status == 200
                            except OSError:
                                return False
                        wait(healthy)
                        client = Client.remote(env["ENGINE_URL"], env["ENGINE_TOKEN"])
                    else:
                        client = Client(bucket=bucket, prefix=prefix, region=region)
                    history = 0
                    for round_number in range(2):
                        before_tasks = sum(obj["Key"].endswith("/state.json") for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/tasks/") for obj in page.get("Contents", []))
                        case = run_round(client, mode, workers, round_number, history, args, env, work, fixture, proxy, server)
                        after_tasks = sum(obj["Key"].endswith("/state.json") for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/tasks/") for obj in page.get("Contents", []))
                        case.update(created_tasks_including_children=after_tasks - before_tasks,
                                    retained_tasks_before_burst=before_tasks, total_namespace_tasks=after_tasks)
                        case["prefix"] = prefix
                        report["cases"].append(case); print(json.dumps(case), flush=True)
                        history += case["completed"] + case["failed"] + case["cancelled"]
                        if case["pending_or_unobserved"] or case["submission_errors_indeterminate"]:
                            case["next_burst_skipped"] = "unfinished first burst would mix history and active backlog"; break
                    client.close(); client = None
                    stop(server); server = None
        report["recording_completed"] = True  # Pending/failed tasks remain observations, not success claims.
    except BaseException as error:
        report.update(recording_completed=False, error_type=type(error).__name__)
        raise
    finally:
        cleanup_errors = []
        for stage, teardown in (
            ("client", lambda: client.close() if client else None),
            ("server", lambda: stop(server)),
            ("fixture", lambda: fixture.close() if fixture else None),
            ("proxy", lambda: proxy.close() if proxy else None),
        ):
            try:
                teardown()
            except BaseException as error:
                cleanup_errors.append({"stage": stage, "type": type(error).__name__})
        try:
            if created:
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                        raise
                    report["cleaned"] = True
                else:
                    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                        if objects and s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors"):
                            raise RuntimeError("load bucket object cleanup failed")
                    s3.delete_bucket(Bucket=bucket)
                    try:
                        s3.head_bucket(Bucket=bucket)
                    except ClientError as error:
                        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                            raise
                        report["cleaned"] = True
                    else:
                        raise RuntimeError("load bucket still exists after deletion")
        except BaseException as error:
            cleanup_errors.append({"stage": "bucket", "type": type(error).__name__})
        if cleanup_errors:
            report.update(recording_completed=False, cleanup_errors=cleanup_errors)
        output = ROOT.parent / "outputs/evidence" / ("load-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"report": str(output), "cleaned": report["cleaned"]}), flush=True)
        if cleanup_errors or not report["cleaned"]:
            raise RuntimeError("load cleanup incomplete; inspect report")


if __name__ == "__main__":
    main()

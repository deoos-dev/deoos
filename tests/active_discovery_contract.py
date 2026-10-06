#!/usr/bin/env python3
"""Real RustFS checks for active-index discovery and interrupted durable intents.

Run with tests/.venv312/bin/python after packaging/build_package.py --build-only.
Only a uniquely created local bucket is used. Evidence stays outside the repo.
"""
import argparse
import copy
import datetime
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from discovery_contract import digest, free_port, report_save, stop_group
from discovery_probe import CountingProxy


def main():
    if not __debug__:
        raise SystemExit("contract checks refuse optimized Python")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1])
    parser.add_argument("--engine", type=pathlib.Path)
    parser.add_argument("--history", type=int, nargs="+", default=[0, 1000, 5000])
    args = parser.parse_args()
    if any(count < 0 for count in args.history):
        parser.error("history counts must be nonnegative")
    repo = args.repo.resolve()
    sys.path.insert(0, str(repo / "clients/python"))
    from deoos import Client, EngineError
    engine = (args.engine or repo / "engine/target/release/deoos-engine").resolve()
    native_name = {"Darwin": "libdeoos_engine.dylib", "Linux": "libdeoos_engine.so", "Windows": "deoos_engine.dll"}[platform.system()]
    native = repo / "clients/python/deoos/native" / native_name
    run_id = uuid.uuid4().hex
    bucket, prefix = "deoos-active-contract-" + run_id[:20], "active-contract-" + run_id
    evidence = repo.parent / "outputs/active-discovery" / f"active-contract-{run_id}.json"
    report = {"started": datetime.datetime.now(datetime.timezone.utc).isoformat(), "backend": "local-rustfs-only",
              "bucket": bucket, "owned_prefix": prefix, "checks": [], "history_samples": [], "cleaned": False,
              "cleanup_errors": [],
              "cold_process_definition": "new engine instance against preexisting ready index; fresh initialization and incompatible prefixes tested separately",
              "runtime_hashes": {"engine": digest(engine), "python_native": digest(native)},
              "source_hashes": {name: digest(repo / name) for name in
                                ("engine/src/lib.rs", "engine/src/active.rs", "engine/src/discovery.rs", "engine/src/schedules.rs",
                                 "tests/active_discovery_contract.py", "tests/discovery_probe.py")
                                if (repo / name).is_file()}}
    for name in list(os.environ):
        if name.startswith(("AWS_", "GOOGLE_", "AZURE_")) or name in (
                "DEOOS_NATIVE_LIBRARY", "DEOOS_STORAGE_BUCKET", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "all_proxy", "ENGINE_TOKEN"):
            os.environ.pop(name, None)
    os.environ.update(AWS_ACCESS_KEY_ID="local-development", AWS_SECRET_ACCESS_KEY="local-development-only-secret",
                      AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1", AWS_ALLOW_HTTP="true",
                      AWS_BUCKET=bucket, DEOOS_STORAGE_BUCKET=bucket, DEOOS_STORAGE_PROVIDER="s3",
                      LEASE_MS="30000", NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", region_name="us-east-1",
                      config=Config(proxies={}, max_pool_connections=32, retries={"total_max_attempts": 2}))
    proxy, clients, processes, created = CountingProxy("rustfs", port=0), [], [], False
    scratch_context = tempfile.TemporaryDirectory(prefix="deoos-active-contract-")
    scratch = pathlib.Path(scratch_context.name)

    def put(key, value):
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(value, separators=(",", ":")).encode())

    def names(boundary):
        return {item["Key"]: item["ETag"] for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=boundary) for item in page.get("Contents", [])}

    def ready(case_prefix):
        put(case_prefix + "/active-index.json", {"version": 1, "status": "ready"})

    def marker_key(case_prefix, task):
        return f"{case_prefix}/active/{task['id']}/{task['active_entry_id']}.json"

    def intent(case_prefix, task, expected=None):
        key = marker_key(case_prefix, task)
        put(key, {"version": 1, "task": task, "expected_revision": expected})
        return key

    def state_key(case_prefix, task):
        return f"{case_prefix}/tasks/{task['id']}/state.json"

    def open_client(mode, case_prefix):
        os.environ.update(AWS_ENDPOINT=proxy_url, EXECUTION_PREFIX=case_prefix)
        if mode == "library":
            client = Client(bucket=bucket, prefix=case_prefix)
        else:
            port = free_port()
            env = dict(os.environ, ENGINE_BIND=f"127.0.0.1:{port}")
            with (scratch / f"server-{port}.log").open("wb") as log:
                process = subprocess.Popen([str(engine)], env=env, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
            processes.append(process)
            url = f"http://127.0.0.1:{port}"
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError("engine exited before readiness")
                try:
                    urllib.request.urlopen(url + "/health", timeout=1).close()
                    break
                except OSError:
                    time.sleep(.05)
            else:
                raise TimeoutError("engine was not ready")
            client = Client.remote(url)
        clients.append(client)
        return client

    def claim(client, handler="contract.active"):
        return client.request("/claim", {"worker": "active-contract", "handlers": [handler]})["task"]

    def finish(client, task, action="complete"):
        value = {"terminal": True, "error": "contract failure"} if action == "fail" else {"retained": True}
        return client.request(f"/tasks/{task['id']}/{action}",
                              {"token": task["token"], "operation_id": str(uuid.uuid4()), "value": value})

    def proposed(template, identifier):
        task = copy.deepcopy(template)
        task.update(id=identifier, revision=str(uuid.uuid4()), active_entry_id=str(uuid.uuid4()),
                    status="queued", attempts=0, available_at=0, owner=None, token=None, expires_at=0,
                    output=None, error=None, waiting_on=None, steps={}, definitions={}, history=[])
        return task

    def no_historical_enumeration(snapshot):
        task_ops = snapshot.get("request_families", {}).get("tasks", {}).get("operations", {})
        assert task_ops.get("LIST", 0) == 0, snapshot
        assert all(not value.rstrip("/").endswith("/tasks") for value in snapshot["list_prefixes"]), snapshot
        assert task_ops.get("DELETE", 0) == 0, snapshot
        return task_ops.get("GET", 0)

    try:
        proxy.start()
        proxy_url = f"http://127.0.0.1:{proxy.server.server_address[1]}"
        s3.create_bucket(Bucket=bucket)
        created = True
        template_client = open_client("library", prefix + "/template")
        template = template_client.submit("template", "contract.active", {})
        assert template.get("active_entry_id"), template
        for mode in ("library", "server"):
            for count in args.history:
                case_prefix = f"{prefix}/{mode}/history-{count}"
                ready(case_prefix)
                neighbor_key = case_prefix + "/active-neighbor/keep"
                put(neighbor_key, {"neighbor": "preserved"})
                neighbor_etag = s3.head_object(Bucket=bucket, Key=neighbor_key)["ETag"]
                def seed(index):
                    task = proposed(template, f"terminal-{index:05d}")
                    task.update(status="completed", output={"history": index})
                    result_key = f"{case_prefix}/tasks/{task['id']}/results/retained/1/value.json"
                    task["steps"] = {"retained": result_key}
                    put(result_key, {"retained": index})
                    put(state_key(case_prefix, task), task)
                with ThreadPoolExecutor(max_workers=32) as workers:
                    list(workers.map(seed, range(count)))
                before = names(case_prefix + "/tasks/")
                client = open_client(mode, case_prefix)
                measured = []
                for temperature in ("cold", "warm"):
                    proxy.drain()
                    proxy.reset()
                    assert claim(client) is None
                    proxy.drain()
                    snapshot = proxy.snapshot()
                    assert no_historical_enumeration(snapshot) == 0, snapshot
                    measured.append({"temperature": temperature, "requests": snapshot})
                assert names(case_prefix + "/tasks/") == before
                queued = client.submit("live", "contract.active", {})
                live_key = marker_key(case_prefix, queued)
                assert live_key in names(case_prefix + "/active/")
                proxy.drain()
                proxy.reset()
                running = claim(client)
                assert running and running["id"] == "live"
                proxy.drain()
                snapshot = proxy.snapshot()
                assert no_historical_enumeration(snapshot) == 1, snapshot
                assert live_key in names(case_prefix + "/active/")
                finish(client, running)
                assert claim(client) is None
                assert live_key not in names(case_prefix + "/active/")
                after = names(case_prefix + "/tasks/")
                assert all(after.get(key) == etag for key, etag in before.items())
                assert client.inspect("live")["status"] == "completed"
                assert s3.head_object(Bucket=bucket, Key=neighbor_key)["ETag"] == neighbor_etag
                assert json.loads(s3.get_object(Bucket=bucket, Key=neighbor_key)["Body"].read()) == {"neighbor": "preserved"}
                report["history_samples"].append({"mode": mode, "historical_tasks": count,
                    "historical_objects": len(before), "cold_warm": measured, "live_claim": snapshot,
                    "historical_objects_preserved": True, "terminal_task_retained": True,
                    "ready_index_preexisting": True, "active_neighbor_preserved": True})
                report_save(evidence, report)
                print(json.dumps({"passed_mode": mode, "historical_tasks": count, "task_list_requests": 0,
                                  "empty_claim_task_gets": 0, "live_claim_task_gets": 1}), flush=True)

            case_prefix = f"{prefix}/{mode}/crash-barriers"
            ready(case_prefix)
            client = open_client(mode, case_prefix)
            # A losing create proposal cannot replace an already-created task definition.
            original = client.submit("duplicate-create", "contract.active", {"definition": "winner"})
            losing = proposed(template, original["id"])
            losing["inputs"] = {"definition": "loser"}
            losing_key = intent(case_prefix, losing)
            assert claim(client, "unregistered-handler") is None
            assert losing_key not in names(case_prefix + "/active/")
            assert client.inspect(original["id"])["inputs"] == {"definition": "winner"}
            recovered = claim(client)
            assert recovered and recovered["active_entry_id"] == original["active_entry_id"]
            finish(client, recovered)
            report["checks"].append({"mode": mode, "name": "losing duplicate-create intent is discarded without changing winner"})
            for barrier in ("intent-only", "task-written-before-response"):
                task = proposed(template, barrier)
                key = intent(case_prefix, task)
                if barrier == "task-written-before-response":
                    put(state_key(case_prefix, task), task)
                recovered = claim(client)
                assert recovered and recovered["id"] == task["id"]
                assert recovered["active_entry_id"] == task["active_entry_id"]
                finish(client, recovered)
                assert claim(client) is None
                assert key not in names(case_prefix + "/active/")
                assert client.inspect(task["id"])["status"] == "completed"
                report["checks"].append({"mode": mode, "name": barrier + " repaired by worker", "active_entry_id": task["active_entry_id"]})

            # Persisted retry intent is authoritative only at its exact predecessor revision.
            client.submit("retry-crash", "contract.active", {})
            failed = finish(client, claim(client), "fail")
            retry_task = proposed(failed, failed["id"])
            operation_id = str(uuid.uuid4())
            retry_task.update(last_retry_operation=operation_id, last_retry_fingerprint=hashlib.sha256(
                json.dumps({"expected_revision": failed["revision"]}, separators=(",", ":")).encode()).hexdigest())
            retry_key = intent(case_prefix, retry_task, failed["revision"])
            recovered = claim(client)
            assert recovered and recovered["id"] == "retry-crash" and recovered["active_entry_id"] == retry_task["active_entry_id"]
            assert recovered["attempts"] == 1
            finish(client, recovered)
            # A stale retry intent with a mismatched predecessor must never resurrect terminal work.
            stale = proposed(failed, failed["id"])
            stale_key = intent(case_prefix, stale, failed["revision"])
            assert claim(client) is None
            assert stale_key not in names(case_prefix + "/active/")
            assert retry_key not in names(case_prefix + "/active/")
            assert client.inspect("retry-crash")["status"] == "completed"
            report["checks"].append({"mode": mode, "name": "retry intent repaired; stale predecessor discarded without resurrection"})

            # LIST captures an old marker; a second client retries before cleanup receives that LIST.
            queued = client.submit("retry-cleanup-race", "contract.active", {})
            failed = finish(client, claim(client), "fail")
            old_key = intent(case_prefix, queued)
            other = open_client(mode, case_prefix)
            raced = {}
            def retry_after_list(family):
                if family == "active" and not raced:
                    raced["task"] = other.retry(failed["id"], failed["revision"])
            proxy.after_list = retry_after_list
            try:
                result = claim(client)
            finally:
                proxy.after_list = None
            assert raced, "active LIST barrier was not reached"
            new_task = raced["task"]
            assert new_task["active_entry_id"] != queued["active_entry_id"]
            new_key = marker_key(case_prefix, new_task)
            assert old_key not in names(case_prefix + "/active/")
            assert new_key in names(case_prefix + "/active/")
            if result is None:
                result = claim(client)
            assert result and result["id"] == failed["id"] and result["active_entry_id"] == new_task["active_entry_id"]
            finish(client, result)
            assert client.inspect(failed["id"])["status"] == "completed"
            report["checks"].append({"mode": mode, "name": "cleanup racing explicit retry deletes old active entry only"})

            # Missing-index task records are rejected unchanged, including terminal or malformed records.
            for fixture in ("queued", "completed", "malformed", "current-without-index"):
                incompatible_prefix = f"{prefix}/{mode}/incompatible-{fixture}"
                stored = proposed(template, "retained")
                if fixture != "current-without-index":
                    stored.pop("active_entry_id", None)
                if fixture == "completed":
                    stored.update(status="completed", output="retained output")
                key = state_key(incompatible_prefix, stored)
                if fixture == "malformed":
                    s3.put_object(Bucket=bucket, Key=key, Body=b'{"invalid":')
                else:
                    put(key, stored)
                before = names(incompatible_prefix + "/")
                rejected = open_client(mode, incompatible_prefix)
                for operation in (lambda: claim(rejected),
                                  lambda: rejected.submit("new-task", "contract.active", {})):
                    proxy.drain()
                    proxy.reset()
                    try:
                        operation()
                    except EngineError as error:
                        assert error.status == 409, error
                        assert "fresh prefix" in str(error), error
                    else:
                        raise AssertionError("missing-index task records did not block initialization")
                    proxy.drain()
                    operations = proxy.snapshot()["requests"]
                    assert operations.get("PUT", 0) == 0 and operations.get("DELETE", 0) == 0, operations
                    assert names(incompatible_prefix + "/") == before
                report["checks"].append({"mode": mode, "name": f"{fixture} records without index reject claim and submit without mutations"})

            # A ready index does not make unsupported task field layouts valid.
            for fixture in ("old-field-only", "missing-entry-id", "null-entry-id"):
                incompatible_prefix = f"{prefix}/{mode}/invalid-entry-{fixture}"
                ready(incompatible_prefix)
                stored = proposed(template, "retained")
                if fixture == "old-field-only":
                    stored["active_incarnation"] = stored.pop("active_entry_id")
                elif fixture == "missing-entry-id":
                    stored.pop("active_entry_id")
                else:
                    stored["active_entry_id"] = None
                put(state_key(incompatible_prefix, stored), stored)
                before = names(incompatible_prefix + "/")
                rejected = open_client(mode, incompatible_prefix)
                for operation in (lambda: rejected.inspect(stored["id"]),
                                  lambda: rejected.submit(stored["id"], stored["handler"], stored["inputs"]),
                                  lambda: rejected.retry(stored["id"], stored["revision"])):
                    proxy.drain()
                    proxy.reset()
                    try:
                        operation()
                    except EngineError as error:
                        assert error.status == 409, error
                    else:
                        raise AssertionError("invalid active entry ID did not reject stored task")
                    proxy.drain()
                    operations = proxy.snapshot()["requests"]
                    assert operations.get("PUT", 0) == 0 and operations.get("DELETE", 0) == 0, operations
                    assert names(incompatible_prefix + "/") == before
                report["checks"].append({"mode": mode, "name": f"{fixture} with ready index rejects inspect, same-ID submit and operator retry without mutations"})

            # Independent current engines race to initialize a fresh prefix and submit separate work.
            fresh_prefix = f"{prefix}/{mode}/concurrent-fresh"
            fresh = [open_client(mode, fresh_prefix), open_client(mode, fresh_prefix)]
            def submit_fresh(index):
                return fresh[index].submit(f"fresh-{index}", "contract.active", {})
            with ThreadPoolExecutor(max_workers=2) as workers:
                submitted = list(workers.map(submit_fresh, range(2)))
            assert {task["id"] for task in submitted} == {"fresh-0", "fresh-1"}
            assert names(fresh_prefix + "/active-index.json")
            with ThreadPoolExecutor(max_workers=2) as workers:
                results = list(workers.map(claim, fresh))
            assert {task["id"] for task in results if task is not None} == {"fresh-0", "fresh-1"}, results
            for task in results:
                finish(fresh[0], task)
            restarted = open_client(mode, fresh_prefix)
            assert claim(restarted) is None
            assert all(restarted.inspect(f"fresh-{index}")["status"] == "completed" for index in range(2))
            report["checks"].append({"mode": mode, "name": "concurrent fresh initialization submits and claims both tasks; restart preserves completion"})

            # Exercise Path canonicalization through real APIs; manual fixtures use simple keys.
            special = open_client(mode, f"{prefix}/{mode}/spaces % /special")
            special.submit("special-prefix", "contract.active", {})
            special_task = claim(special)
            assert special_task and special_task["id"] == "special-prefix"
            finish(special, special_task)
            assert claim(special) is None
            assert special.inspect("special-prefix")["status"] == "completed"
            report["checks"].append({"mode": mode, "name": "spaces and percent signs in API execution prefix support active lifecycle"})
        report["behavior_passed"] = True
    except BaseException as error:
        report.update(behavior_passed=False, failure_type=type(error).__name__, failure=str(error)[:500])
        raise
    finally:
        proxy.after_list = None
        stopped = True
        for client in clients:
            try:
                client.close()
            except Exception as error:
                stopped = False
                report["cleanup_errors"].append({"stage": "client", "error_type": type(error).__name__})
        for process in processes:
            try:
                stopped = stop_group(process) and stopped
            except Exception as error:
                stopped = False
                report["cleanup_errors"].append({"stage": "process", "error_type": type(error).__name__})
        try:
            proxy.drain()
        except Exception as error:
            stopped = False
            report["cleanup_errors"].append({"stage": "proxy drain", "error_type": type(error).__name__})
        try:
            proxy.close()
        except Exception as error:
            stopped = False
            report["cleanup_errors"].append({"stage": "proxy close", "error_type": type(error).__name__})
        report["processes_stopped_before_cleanup"] = stopped
        if created and stopped:
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    assert all(item["Key"].startswith(prefix + "/") for item in objects)
                    if objects:
                        assert not s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors")
                assert not names(prefix + "/")
                s3.delete_bucket(Bucket=bucket)
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
                else:
                    raise AssertionError("local owned bucket remains")
                report["cleaned"] = True
            except Exception as error:
                report["cleanup_errors"].append({"stage": "owned storage", "error_type": type(error).__name__})
        report["success"] = bool(report.get("behavior_passed") and report["cleaned"] and stopped)
        report["finished"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            scratch_context.cleanup()
        except Exception as error:
            report["cleanup_errors"].append({"stage": "scratch", "error_type": type(error).__name__})
            report["success"] = False
        report_save(evidence, report)
        print(json.dumps({"report": str(evidence), "success": report["success"], "cleaned": report["cleaned"]}), flush=True)
        if not report["success"] and report.get("behavior_passed"):
            raise RuntimeError("active contract cleanup failed; inspect saved report")


if __name__ == "__main__":
    main()

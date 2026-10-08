#!/usr/bin/env python3
"""Opt-in local-RustFS behavioral checks for active discovery and readiness hints.

No AWS backend is available in this script. It creates one uniquely named local
RustFS bucket, exercises a real native library and a separate engine process,
then removes only data in that owned bucket.
"""
import argparse
import copy
import datetime
import hashlib
import json
import os
import pathlib
import platform
import signal
import socket
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


def digest(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def report_save(path, report):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(report, stream, indent=2)
        stream.write("\n")
    os.replace(temporary, path)
    path.chmod(0o600)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_group(process):
    if process is None:
        return True
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
    return process.poll() is not None


def put_state(s3, bucket, prefix, state, active=True):
    s3.put_object(Bucket=bucket, Key=f"{prefix}/tasks/{state['id']}/state.json",
                  Body=json.dumps(state, separators=(",", ":")).encode(),
                  ContentType="application/json")
    if active:
        marker_task = copy.deepcopy(state)
        if marker_task["status"] in ("completed", "failed", "cancelled"):
            marker_task["status"] = "queued"
        s3.put_object(Bucket=bucket, Key=f"{prefix}/active/{state['id']}/{state['active_entry_id']}.json",
                      Body=json.dumps({"version": 1, "task": marker_task, "expected_revision": None},
                                      separators=(",", ":")).encode(), ContentType="application/json")


def clone_state(template, identifier, status):
    state = copy.deepcopy(template)
    state.update(id=identifier, revision=uuid.uuid4().hex, status=status, active_entry_id=str(uuid.uuid4()))
    if status == "queued":
        state.update(attempts=0, available_at=0, expires_at=0, owner=None, token=None,
                     error=None, waiting_on=None, history=[])
    state["steps"] = {}
    state["definitions"] = {}
    return state


def task_counts(snapshot):
    return snapshot.get("request_families", {}).get("tasks", {})


def main():
    if not __debug__:
        raise SystemExit("discovery contract checks refuse optimized Python (-O disables assertions)")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=pathlib.Path,
                        default=pathlib.Path(__file__).resolve().parents[1])
    parser.add_argument("--engine", type=pathlib.Path,
                        help="engine server binary; defaults to this runtime tree's release binary")
    args = parser.parse_args()
    history_count = 1_000

    repo = args.repo.resolve()
    sys.path.insert(0, str(repo / "tests"))
    sys.path.insert(0, str(repo / "clients/python"))
    from deoos import Client
    from discovery_probe import CountingProxy

    engine = (args.engine or repo / "engine/target/release" /
              ("deoos-server.exe" if os.name == "nt" else "deoos-server")).resolve()
    native_name = {"Darwin": "libdeoos_engine.dylib", "Linux": "libdeoos_engine.so",
                   "Windows": "deoos_engine.dll"}[platform.system()]
    os.environ.pop("DEOOS_NATIVE_LIBRARY", None)
    native = (repo / "clients/python/deoos/native" / native_name).resolve()
    if not engine.is_file() or not native.is_file():
        raise FileNotFoundError("engine server or Python native library is missing from the selected runtime tree")

    run_id = uuid.uuid4().hex
    bucket = "deoos-discovery-contract-" + run_id[:20]
    prefix = "discovery-contract-" + run_id
    neighbor = prefix + "-neighbor/keep"
    evidence = repo / "outputs/active-discovery" / f"discovery-contract-{run_id}.json"
    report = {"started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "backend": "local-rustfs-only", "bucket": bucket, "owned_prefix": prefix,
              "historical_tasks": history_count, "checks": [], "cleaned": False,
              "servers_stopped": [], "source_hashes": {}, "runtime_hashes": {}}
    for name in ("engine/src/lib.rs", "engine/src/active.rs", "engine/src/discovery.rs", "engine/src/schedules.rs",
                 "tests/discovery_probe.py", "tests/discovery_contract.py"):
        path = repo / name
        if path.is_file():
            report["source_hashes"][name] = digest(path)
    report["runtime_hashes"] = {"engine_server": digest(engine), "python_native": digest(native),
                                "python_sdk": digest(repo / "clients/python/deoos/__init__.py")}

    for name in list(os.environ):
        if name.startswith(("AWS_", "GOOGLE_", "AZURE_")):
            os.environ.pop(name, None)
    for name in ("DEOOS_STORAGE_BUCKET", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
                 "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_SESSION_TOKEN",
                 "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(name, None)
    os.environ.update(AWS_ACCESS_KEY_ID="local-development",
                      AWS_SECRET_ACCESS_KEY="local-development-only-secret",
                      AWS_REGION="us-east-1", AWS_DEFAULT_REGION="us-east-1",
                      AWS_ENDPOINT="http://127.0.0.1:19000", AWS_ALLOW_HTTP="true",
                      AWS_BUCKET=bucket, DEOOS_STORAGE_BUCKET=bucket,
                      DEOOS_STORAGE_PROVIDER="s3", LEASE_MS="30000",
                      NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
    proxy = None
    server_processes = []
    library_clients = []
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", region_name="us-east-1",
                      config=Config(proxies={}, max_pool_connections=32,
                                    connect_timeout=2, read_timeout=30,
                                    retries={"total_max_attempts": 2}))
    created_bucket = False

    def client_for(engine_prefix):
        os.environ.update(AWS_BUCKET=bucket, AWS_ENDPOINT=proxy_url,
                          EXECUTION_PREFIX=engine_prefix)
        client = Client(bucket=bucket, prefix=engine_prefix)
        library_clients.append(client)
        return client

    def server_for(engine_prefix, scratch):
        port = free_port()
        env = os.environ.copy()
        env.update(AWS_BUCKET=bucket, DEOOS_STORAGE_BUCKET=bucket,
                   AWS_ENDPOINT=proxy_url, AWS_REGION="us-east-1",
                   AWS_ALLOW_HTTP="true", DEOOS_STORAGE_PROVIDER="s3",
                   EXECUTION_PREFIX=engine_prefix, ENGINE_BIND=f"127.0.0.1:{port}",
                   LEASE_MS="30000", NO_PROXY="127.0.0.1,localhost",
                   no_proxy="127.0.0.1,localhost")
        env.pop("ENGINE_TOKEN", None)
        log = (scratch / f"engine-{port}.log").open("wb")
        process = subprocess.Popen([str(engine)], env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        log.close()
        server_processes.append(process)
        url = f"http://127.0.0.1:{port}"
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError("separate engine process exited before readiness")
            try:
                with urllib.request.urlopen(url + "/health", timeout=1) as response:
                    if response.status == 200:
                        return Client.remote(url)
            except OSError:
                time.sleep(.05)
        raise TimeoutError("separate engine process did not become ready")

    try:
        proxy = CountingProxy("rustfs", port=0)
        proxy.start()
        proxy_url = f"http://127.0.0.1:{proxy.server.server_address[1]}"
        s3.create_bucket(Bucket=bucket)
        created_bucket = True
        s3.put_object(Bucket=bucket, Key=neighbor, Body=b"neighbor")

        # A separate valid template proves the cap fixture is made of real task records.
        template_prefix = prefix + "/template"
        template_client = client_for(template_prefix)
        template_client.submit("terminal-template", "contract.fixture", {})
        assert template_client.run_once({"contract.fixture": lambda ctx, inputs: {"done": True}})
        terminal_template = template_client.inspect("terminal-template")
        template_client.submit("queued-template", "contract.fixture", {})
        queued_template = template_client.inspect("queued-template")
        template_client.close()
        library_clients.remove(template_client)
        assert terminal_template["status"] == "completed" and queued_template["status"] == "queued"

        cap_prefix = prefix + "/cap"
        report["seeded_template_prefix"] = template_prefix
        report["cap_seed_count"] = history_count
        s3.put_object(Bucket=bucket, Key=cap_prefix + "/active-index.json",
                      Body=b'{"version":1,"status":"ready"}')
        start_seed = time.monotonic()
        with ThreadPoolExecutor(max_workers=32) as workers:
            list(workers.map(lambda index: put_state(
                s3, bucket, cap_prefix,
                clone_state(terminal_template, f"terminal-{index:05d}", "completed"), active=False),
                range(history_count)))
        report["cap_seed_seconds"] = time.monotonic() - start_seed
        cap_client = client_for(cap_prefix)
        proxy.reset()
        no_work = cap_client.request("/claim", {"worker": "cap-warm", "handlers": ["contract.fixture"]})
        assert no_work["task"] is None
        proxy.drain()
        first_counts = proxy.snapshot()
        first_gets = task_counts(first_counts).get("operations", {}).get("GET", 0)
        assert first_gets == 0, first_gets
        assert task_counts(first_counts).get("operations", {}).get("LIST", 0) == 0, first_counts
        proxy.reset()
        no_work = cap_client.request("/claim", {"worker": "cap-hit", "handlers": ["contract.fixture"]})
        assert no_work["task"] is None
        proxy.drain()
        warm_counts = proxy.snapshot()
        warm_gets = task_counts(warm_counts).get("operations", {}).get("GET", 0)
        assert warm_gets == 0, warm_gets
        assert task_counts(warm_counts).get("operations", {}).get("LIST", 0) == 0, warm_counts
        report["checks"].append({"name": "cold and warm claims skip all historical terminal objects",
                                 "seeded": history_count, "cold_gets": first_gets,
                                 "repeat_gets": warm_gets,
                                 "first_requests": first_counts["request_families"],
                                 "repeat_requests": warm_counts["request_families"]})
        eligible_id = "zz-eligible-after-cap"
        put_state(s3, bucket, cap_prefix, clone_state(queued_template, eligible_id, "queued"))
        proxy.reset()
        claimed = cap_client.request("/claim", {"worker": "cap-overflow", "handlers": ["contract.fixture"]})["task"]
        assert claimed and claimed["id"] == eligible_id and claimed["attempts"] == 1
        proxy.drain()
        cap_claim_counts = proxy.snapshot()
        assert task_counts(cap_claim_counts).get("operations", {}).get("GET", 0) == 1
        report["checks"].append({"name": "active eligible task is read and claimed beside retained history",
                                 "claimed_id": claimed["id"], "task_requests": task_counts(cap_claim_counts)})
        cap_client.close()
        library_clients.remove(cap_client)

        # Fresh engine and uncached victim make the LIST->GET disappearance deterministic.
        delete_prefix = prefix + "/delete-race"
        s3.put_object(Bucket=bucket, Key=delete_prefix + "/active-index.json",
                      Body=b'{"version":1,"status":"ready"}')
        victim = clone_state(terminal_template, "a-victim", "completed")
        put_state(s3, bucket, delete_prefix, victim)
        put_state(s3, bucket, delete_prefix, clone_state(queued_template, "b-eligible", "queued"))
        put_state(s3, bucket, delete_prefix, clone_state(terminal_template, "z-terminal", "completed"))
        deleted = {"done": False}
        victim_key = f"{delete_prefix}/active/a-victim/{victim['active_entry_id']}.json"
        def delete_after_task_list(family):
            if family == "active" and not deleted["done"]:
                s3.delete_object(Bucket=bucket, Key=victim_key)
                deleted["done"] = True
        proxy.after_list = delete_after_task_list
        delete_client = client_for(delete_prefix)
        proxy.reset()
        result = delete_client.request("/claim", {"worker": "delete-race", "handlers": ["contract.fixture"]})["task"]
        proxy.after_list = None
        proxy.drain()
        delete_counts = proxy.snapshot()
        assert deleted["done"] and result and result["id"] == "b-eligible", result
        assert task_counts(delete_counts).get("operations", {}).get("GET", 0) == 2
        assert s3.get_object(Bucket=bucket, Key=f"{delete_prefix}/tasks/a-victim/state.json")["Body"].read()
        report["checks"].append({"name": "deleted active LIST candidate with retained terminal state safely skips to queued task",
                                 "victim_execution_state_preserved": True,
                                 "claimed_id": result["id"], "task_requests": task_counts(delete_counts)})
        delete_client.close()
        library_clients.remove(delete_client)

        # Waiting-child families get a short priority lane, with a forced
        # normal-lane turn every fourth claim attempt. Restarting the engine
        # while children remain running must not make the parent ready.
        with tempfile.TemporaryDirectory(prefix="deoos-discovery-priority-") as scratch_dir:
            scratch = pathlib.Path(scratch_dir)
            priority_prefix = prefix + "/priority-family"
            priority_client = server_for(priority_prefix, scratch)
            priority_client.submit("a-unrelated", "contract.normal", {})
            priority_client.submit("b-child-one", "contract.child", {})
            priority_client.submit("c-child-two", "contract.child", {})
            priority_client.submit("z-parent", "contract.parent", {})
            parent = priority_client.request("/claim", {
                "worker": "priority-parent", "handlers": ["contract.parent"]})["task"]
            assert parent and parent["id"] == "z-parent"
            waiting = priority_client.request("/tasks/z-parent/suspend", {
                "token": parent["token"], "operation_id": uuid.uuid4().hex,
                "value": {"children": ["b-child-one", "c-child-two"]}})
            assert waiting["status"] == "waiting"

            family_handlers = ["contract.child", "contract.parent", "contract.normal"]
            child_one = priority_client.request("/claim", {
                "worker": "priority-child-one", "handlers": family_handlers})["task"]
            assert child_one and child_one["id"] == "b-child-one", child_one
            child_two = priority_client.request("/claim", {
                "worker": "priority-child-two", "handlers": family_handlers})["task"]
            assert child_two and child_two["id"] == "c-child-two", child_two

            # Cycle child one through terminal failure and explicit retry so it
            # is eligible at the queue head on attempt four. Without the forced
            # normal turn, that call would claim this child first.
            failed = priority_client.request("/tasks/b-child-one/fail", {
                "token": child_one["token"], "operation_id": uuid.uuid4().hex,
                "value": {"terminal": True, "error": "bounded contract setup"}})
            assert failed["status"] == "failed"
            retry = priority_client.retry("b-child-one", failed["revision"])
            assert retry["status"] == "queued"

            # Third priority attempt revisits the still-unready parent and
            # repopulates the family queue. Normal work is withheld by handler
            # filter so a nonempty priority queue remains for the fourth call.
            no_parent = priority_client.request("/claim", {
                "worker": "priority-parent-check",
                "handlers": ["contract.parent"]})["task"]
            assert no_parent is None
            normal = priority_client.request("/claim", {
                "worker": "forced-normal-turn", "handlers": family_handlers})["task"]
            assert normal and normal["id"] == "a-unrelated", normal
            child_one = priority_client.request("/claim", {
                "worker": "priority-child-one-retry", "handlers": family_handlers})["task"]
            assert child_one and child_one["id"] == "b-child-one", child_one
            report["checks"].append({
                "name": "two-child family progresses ahead of unrelated work and fourth attempt serves normal lane",
                "priority_children_claimed": [child_one["id"], child_two["id"]],
                "forced_normal_claimed": normal["id"],
                "retried_child_claimed_on_fifth_attempt": child_one["id"],
                "children_left_running": True,
            })

            # Restart the engine while both child leases are still running.
            old_process = server_processes[-1]
            priority_client.close()
            assert stop_group(old_process)
            restarted = server_for(priority_prefix, scratch)
            parent_before = restarted.inspect("z-parent")
            still_waiting = restarted.request("/claim", {
                "worker": "after-restart-before-children", "handlers": ["contract.parent"]})["task"]
            assert still_waiting is None
            parent_during = restarted.inspect("z-parent")
            assert parent_during["status"] == "waiting" and parent_during["attempts"] == parent_before["attempts"] == 1

            for child in (child_one, child_two):
                completed = restarted.request(f"/tasks/{child['id']}/complete", {
                    "token": child["token"], "operation_id": uuid.uuid4().hex,
                    "value": {"ready": True}})
                assert completed["status"] == "completed"
            resumed_parent = restarted.request("/claim", {
                "worker": "after-restart-after-children", "handlers": ["contract.parent"]})["task"]
            assert resumed_parent and resumed_parent["id"] == "z-parent" and resumed_parent["attempts"] == 1
            restarted.request("/tasks/z-parent/complete", {
                "token": resumed_parent["token"], "operation_id": uuid.uuid4().hex,
                "value": {"joined": True}})
            restarted.close()
            report["checks"].append({
                "name": "restarted engine leaves parent waiting while children run, then resumes it",
                "attempts_before_and_after_resume": [parent_during["attempts"], resumed_parent["attempts"]],
                "child_statuses_after_completion": ["completed", "completed"],
            })

        # A native library and an independent server process observe the same durable readiness changes.
        with tempfile.TemporaryDirectory(prefix="deoos-discovery-contract-") as scratch_dir:
            scratch = pathlib.Path(scratch_dir)
            signal_prefix = prefix + "/waiting-signal"
            signal_library = client_for(signal_prefix)
            signal_server = server_for(signal_prefix, scratch)
            signal_library.submit("signal-parent", "contract.signal", {})
            parent = signal_library.request("/claim", {"worker": "signal-parent", "handlers": ["contract.signal"]})["task"]
            waiting = signal_library.request("/tasks/signal-parent/suspend", {
                "token": parent["token"], "operation_id": uuid.uuid4().hex,
                "value": {"signal": "ready"}})
            assert waiting["status"] == "waiting"
            assert signal_library.request("/claim", {"worker": "warm-signal", "handlers": ["contract.signal"]})["task"] is None
            signaled = signal_server.signal("signal-parent", "ready", {"ok": True})
            assert signaled["status"] == "waiting"
            resumed = signal_library.request("/claim", {"worker": "resume-signal", "handlers": ["contract.signal"]})["task"]
            assert resumed and resumed["id"] == "signal-parent" and resumed["attempts"] == 1
            signal_library.request("/tasks/signal-parent/complete", {
                "token": resumed["token"], "operation_id": uuid.uuid4().hex, "value": True})
            signal_server.close()
            signal_library.close()
            library_clients.remove(signal_library)
            report["checks"].append({"name": "warm signal-waiting task resumes after independent engine signals it",
                                     "attempts": resumed["attempts"], "status_after_signal": signaled["status"]})

            child_prefix = prefix + "/waiting-child"
            child_library = client_for(child_prefix)
            child_server = server_for(child_prefix, scratch)
            child_library.submit("a-child", "contract.child", {})
            child_library.submit("z-parent", "contract.parent", {})
            parent = child_library.request("/claim", {"worker": "parent", "handlers": ["contract.parent"]})["task"]
            assert parent and parent["id"] == "z-parent"
            waiting = child_library.request("/tasks/z-parent/suspend", {
                "token": parent["token"], "operation_id": uuid.uuid4().hex,
                "value": {"children": ["a-child"]}})
            assert waiting["status"] == "waiting"
            assert child_library.request("/claim", {"worker": "warm-child", "handlers": ["contract.parent"]})["task"] is None
            child = child_server.request("/claim", {"worker": "child", "handlers": ["contract.child"]})["task"]
            assert child and child["id"] == "a-child"
            child_server.request("/tasks/a-child/complete", {
                "token": child["token"], "operation_id": uuid.uuid4().hex, "value": {"ready": True}})
            resumed = child_library.request("/claim", {"worker": "resume-parent", "handlers": ["contract.parent"]})["task"]
            assert resumed and resumed["id"] == "z-parent" and resumed["attempts"] == 1
            child_library.request("/tasks/z-parent/complete", {
                "token": resumed["token"], "operation_id": uuid.uuid4().hex, "value": True})
            child_server.close()
            child_library.close()
            library_clients.remove(child_library)
            report["checks"].append({"name": "warm child-waiting parent resumes after independent engine completes child",
                                     "attempts": resumed["attempts"], "child_status": "completed"})

        report["behavior_passed"] = len(report["checks"]) == 7
    except BaseException as error:
        report.update(behavior_passed=False, failure_type=type(error).__name__, failure=str(error)[:500])
        raise
    finally:
        all_stopped = True
        for client in library_clients:
            try:
                client.close()
            except Exception:
                all_stopped = False
        for process in server_processes:
            try:
                stopped = stop_group(process)
            except Exception:
                stopped = False
            all_stopped = all_stopped and stopped
            report["servers_stopped"].append({"pid": process.pid, "stopped": stopped})
        if proxy is not None:
            try:
                proxy.drain()
                proxy.close()
            except Exception as error:
                report["proxy_cleanup_error_type"] = type(error).__name__
                all_stopped = False
        report["processes_stopped_before_storage_cleanup"] = all_stopped
        if created_bucket and all_stopped:
            try:
                boundary = prefix + "/"
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=boundary):
                    objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    if objects:
                        assert not s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors")
                assert not s3.list_objects_v2(Bucket=bucket, Prefix=boundary, MaxKeys=1).get("Contents")
                assert s3.get_object(Bucket=bucket, Key=neighbor)["Body"].read() == b"neighbor"
                report["owned_prefix_absent"] = True
                report["neighbor_preserved"] = True
                # The bucket is uniquely created by this local test; remove its sentinel and bucket last.
                s3.delete_object(Bucket=bucket, Key=neighbor)
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                    if objects:
                        assert not s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors")
                s3.delete_bucket(Bucket=bucket)
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                        raise
                else:
                    raise RuntimeError("owned local RustFS bucket still exists")
                report["bucket_absent"] = True
                report["cleaned"] = True
            except Exception as error:
                report["cleanup_error_type"] = type(error).__name__
                report["cleaned"] = False
        elif created_bucket:
            report["cleanup_error_type"] = "ProcessGroupStillAlive"
            report["cleaned"] = False
        report["success"] = bool(report.get("behavior_passed") and report.get("cleaned")
                                 and report.get("processes_stopped_before_storage_cleanup"))
        report["finished"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report_save(evidence, report)
        print(json.dumps({"report": str(evidence), "success": report["success"],
                          "cleaned": report["cleaned"], "processes_stopped": all_stopped}), flush=True)
        if not report["success"]:
            raise RuntimeError("discovery contract failed or cleanup was incomplete; inspect its report")


if __name__ == "__main__":
    main()

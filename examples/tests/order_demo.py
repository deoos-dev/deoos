"""Order workflow on deoos-server: children, a timer, then a signal with no worker running."""
import os
import subprocess
import sys
import time
import uuid

import paths
from deoos import Client
from engine_server import (
    create_bucket, delete_bucket, server_worker_env, start_deoos_server, stop_process, wait_for_rustfs,
)

ROOT = paths.ROOT
ENDPOINT = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def waiting_for_approval(state):
    waiting_on = state.get("waiting_on") or {}
    return state.get("status") == "waiting" and waiting_on.get("kind") == "signal" and waiting_on.get("name") == "approved"


def wait_for(client, task_id, predicate, process=None, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            last = client.inspect(task_id)
            if predicate(last):
                return last
            raise RuntimeError(f"worker exited {process.returncode} while task was {last.get('status')}")
        last = client.inspect(task_id)
        if predicate(last):
            return last
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting; last state {last}")


def cli(language, env, *args, capture=False):
    if language == "python":
        command = [sys.executable, "python/order/order.py", *args]
    else:
        command = ["node", "typescript/order/order.mjs", *args]
    stdout = subprocess.PIPE if capture else subprocess.DEVNULL
    return subprocess.Popen(command, cwd=ROOT, env=env, stdout=stdout, stderr=subprocess.STDOUT, text=True)


def run_case(client, env, language, task_id, worker_language=None):
    worker_language = worker_language or language
    submit = cli(language, env, "submit", "--id", task_id, "--quantity", "2", "--unit-price", "25", "--delay-ms", "200", capture=True)
    submit_out, _ = submit.communicate(timeout=30)
    check(submit.returncode == 0, f"submit failed\n{submit_out}")
    worker = cli(worker_language, env, "work")
    try:
        wait_for(client, task_id, waiting_for_approval, worker)
    except BaseException:
        stop_process(worker)
        raise
    stop_process(worker)
    check(worker.poll() is not None, "worker was still running when the approval was sent")
    check(waiting_for_approval(client.inspect(task_id)), "killing the worker resumed or failed the order")
    approve = cli(language, env, "approve", "--id", task_id, capture=True)
    approve_out, _ = approve.communicate(timeout=30)
    check(approve.returncode == 0, f"approve failed\n{approve_out}")
    stored = client.request(f"/tasks/{task_id}/signals/approved")
    check(stored == {"approved": True}, f"signal was not saved with no worker running: {stored}")
    check(client.inspect(task_id)["status"] == "waiting", "the approval started a second task")
    worker = cli(worker_language, env, "work")
    try:
        wait_for(client, task_id, lambda state: state.get("status") == "completed", worker, timeout=60)
    finally:
        stop_process(worker)
    view = client.view(task_id)
    check(view["output"] == {"approved": True, "order_id": task_id, "total": 50}, view.get("output"))
    print(f"order {language} submit / {worker_language} work: total 50, approved true, one task", flush=True)


def main():
    wait_for_rustfs(ENDPOINT)
    bucket = "deoos-order-" + uuid.uuid4().hex[:20]
    prefix = "order-" + uuid.uuid4().hex[:8]
    directory = ROOT / "outputs" / "order"
    directory.mkdir(parents=True, exist_ok=True)
    s3 = create_bucket(ENDPOINT, bucket)
    server = None
    log = open(directory / "deoos-server.log", "w")
    client = None
    try:
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), bucket, prefix, log, ENDPOINT, lease_ms=5000)
        env = server_worker_env()
        env.update(DEOOS_MODE="server", ENGINE_URL=remote["url"], ENGINE_TOKEN=remote["token"])
        client = Client.remote(remote["url"], token=remote["token"])
        run_case(client, env, "python", "order-py")
        run_case(client, env, "typescript", "order-ts")
        run_case(client, env, "python", "order-cross", worker_language="typescript")
        print('order output: {"approved": true, "order_id": "order-py", "total": 50}')
    finally:
        if client is not None:
            client.close()
        stop_process(server)
        log.close()
        delete_bucket(s3, bucket)


if __name__ == "__main__":
    main()

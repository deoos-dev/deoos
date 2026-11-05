"""Self-contained local demo: collect, kill after 20 items, resume."""
import argparse
import json
import multiprocessing
import os
from pathlib import Path
import time
import uuid

import boto3
import duckdb
from botocore.exceptions import ClientError
import paths
from deoos import Client
from engine_server import start_deoos_server, stop_process, wait_for_rustfs
from hacker_news import HANDLER, HANDLERS, collect, collection_inputs

STORAGE = dict(provider="s3", bucket="hacker-news-workflows", region="us-east-1",
               endpoint="http://127.0.0.1:19000", allow_http=True,
               access_key_id="local-development", secret_access_key="local-development-only-secret",
               lease_ms=5000)
DIRECTORY = Path(__file__).resolve().parents[1] / "outputs" / "hacker-news"


def run_task(client, task_id, handlers):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        state = client.inspect(task_id)
        if state["status"] == "completed":
            return
        if state["status"] in ("failed", "cancelled"):
            raise RuntimeError(f"Task {state['status']}: {state.get('error')}")
        try:
            client.run_once(handlers)
        except Exception:
            # The SDK records failed attempts; the next poll observes their retry budget.
            state = client.inspect(task_id)
            if state["status"] != "queued":
                raise
        time.sleep(.2)
    raise TimeoutError("Workflow did not finish within three minutes")


def worker(url, token, task_id, ready):
    def pause_after_twenty(ctx, inputs):
        original = ctx.step
        stores = 0

        def step(name, callback):
            nonlocal stores
            result = original(name, callback)
            if name.startswith("store-"):
                stores += 1
                if stores == 20:
                    ready.send(True)
                    while True:
                        time.sleep(1)
            return result

        ctx.step = step
        return collect(ctx, inputs)

    with Client.remote(url, token=token) as client:
        run_task(client, task_id, {HANDLER: pause_after_twenty})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=DIRECTORY)
    parser.add_argument("--endpoint", default=STORAGE["endpoint"])
    parser.add_argument("--server", type=Path, default=Path(os.environ["ENGINE_BINARY"]) if os.environ.get("ENGINE_BINARY") else None)
    args = parser.parse_args()
    directory = args.directory.resolve()
    endpoint = args.endpoint
    bucket = "deoos-hn-" + uuid.uuid4().hex[:20]

    wait_for_rustfs(endpoint)
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name=STORAGE["region"],
                      aws_access_key_id=STORAGE["access_key_id"],
                      aws_secret_access_key=STORAGE["secret_access_key"])
    directory.mkdir(parents=True, exist_ok=True)
    task_id = "hn-" + uuid.uuid4().hex[:12]
    database = str(directory / f"{task_id}.duckdb")
    prefix = "hacker-news-demo/" + task_id
    report = {"database": database, "bucket": bucket, "prefix": prefix, "engine": "deoos-server",
              "passed": False, "cleaned": False}
    evidence = directory / f"{task_id}.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    create_intent = False
    server = None
    server_log = None
    try:
        create_intent = True
        s3.create_bucket(Bucket=bucket)
        server_log = open(directory / f"{task_id}-server.log", "w")
        server, remote = start_deoos_server(args.server, bucket, prefix, server_log, endpoint, lease_ms=STORAGE["lease_ms"])
        report["engine_url"] = remote["url"]
        with Client.remote(remote["url"], token=remote["token"]) as client:
            client.submit(task_id, HANDLER, collection_inputs(database), max_attempts=5, retry_ms=200)
            context = multiprocessing.get_context("spawn")
            parent, child = context.Pipe(duplex=False)
            process = context.Process(target=worker, args=(remote["url"], remote["token"], task_id, child))
            print("Collecting 100 Hacker News items…", flush=True)
            process.start()
            child.close()
            try:
                deadline = time.monotonic() + 180
                while not parent.poll(.1):
                    if not process.is_alive():
                        raise RuntimeError(f"Worker exited before saving 20 items (exit {process.exitcode})")
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Worker did not save 20 items within three minutes")
                parent.recv()
                process.kill()
                process.join(timeout=10)
                if process.is_alive():
                    raise RuntimeError("Could not stop worker")
            finally:
                if process.is_alive():
                    process.kill()
                process.join(timeout=10)
                parent.close()

            with duckdb.connect(database, read_only=True) as connection:
                saved = connection.execute("SELECT id, payload FROM stories ORDER BY id").fetchall()
            assert len(saved) == 20, "Expected exactly 20 saved items before restart"
            print("Killed the worker after 20 saved items. Restarting after the lease expires…", flush=True)
            run_task(client, task_id, HANDLERS)
            with duckdb.connect(database, read_only=True) as connection:
                after = dict(connection.execute("SELECT id, payload FROM stories").fetchall())
                assert len(after) == 100
                assert all(after[identifier] == payload for identifier, payload in saved)
                assert connection.execute("SELECT count(*) FROM collections WHERE task_id=?", [task_id]).fetchone()[0] == 100
            print("Completed: 100 items, no duplicate or overwritten rows.", flush=True)
        report["passed"] = True
    except BaseException as error:
        report["error"] = repr(error)
        raise
    finally:
        stop_process(server)
        if server_log is not None and not server_log.closed:
            server_log.close()
        try:
            if create_intent:
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                        raise
                else:
                    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                        objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
                        if objects and s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors"):
                            raise RuntimeError("Could not remove temporary workflow state")
                    s3.delete_bucket(Bucket=bucket)
                    try:
                        s3.head_bucket(Bucket=bucket)
                    except ClientError as error:
                        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                            raise
                    else:
                        raise RuntimeError("Temporary bucket still exists")
                report["cleaned"] = True
        except BaseException as error:
            report["cleanup_error"] = repr(error)
            raise
        finally:
            evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(f'Database: {database}\nOpen it with: duckdb "{database}"')


if __name__ == "__main__":
    main()

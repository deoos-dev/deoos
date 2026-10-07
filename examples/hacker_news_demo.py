"""Self-contained local demo: collect, kill after 20 items, resume, query."""
import argparse
import json
import multiprocessing
from pathlib import Path
import time
import urllib.request
import uuid

import boto3
import duckdb
from botocore.exceptions import ClientError
from deoos import Client
from hacker_news import HANDLER, HANDLERS, collect, collection_inputs, query

STORAGE = dict(provider="s3", bucket="hacker-news-workflows", region="us-east-1",
               endpoint="http://127.0.0.1:19000", allow_http=True,
               access_key_id="local-development", secret_access_key="local-development-only-secret",
               lease_ms=5000)
DIRECTORY = Path.home() / "try-deoos-hn"


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


def worker(prefix, task_id, ready):
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

    with Client(**STORAGE, prefix=prefix) as client:
        run_task(client, task_id, {HANDLER: pause_after_twenty})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", action="store_true", help="query the last run without connecting to RustFS")
    parser.add_argument("--directory", type=Path, default=DIRECTORY)
    args = parser.parse_args()
    directory = args.directory.resolve()
    last_run = directory / "last-run.json"
    if args.query:
        print(json.dumps(query(json.loads(last_run.read_text())["database"]), indent=2))
        return

    deadline = time.monotonic() + 60
    while True:
        try:
            with urllib.request.urlopen(STORAGE["endpoint"] + "/health", timeout=2) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("RustFS did not become ready on port 19000")
        time.sleep(.5)
    s3 = boto3.client("s3", endpoint_url=STORAGE["endpoint"], region_name=STORAGE["region"],
                      aws_access_key_id=STORAGE["access_key_id"],
                      aws_secret_access_key=STORAGE["secret_access_key"])
    try:
        s3.head_bucket(Bucket=STORAGE["bucket"])
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        s3.create_bucket(Bucket=STORAGE["bucket"])

    directory.mkdir(parents=True, exist_ok=True)
    task_id = "hn-" + uuid.uuid4().hex[:12]
    database = str(directory / f"{task_id}.duckdb")
    last_run.write_text(json.dumps(dict(task_id=task_id, database=database)))
    prefix = "hacker-news-demo/" + task_id
    with Client(**STORAGE, prefix=prefix) as client:
        client.submit(task_id, HANDLER, collection_inputs(database), max_attempts=5, retry_ms=200)
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe(duplex=False)
        process = context.Process(target=worker, args=(prefix, task_id, child))
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
    print(json.dumps(query(database), indent=2))


if __name__ == "__main__":
    main()

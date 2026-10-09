"""Run the DEV article ETL, kill twice, and verify recovery in both modes."""
import collections
import csv
import hashlib
import http.server
import json
import multiprocessing
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.exceptions import ClientError
from deoos import Client
from devto_etl import HANDLER, HANDLERS, SOURCE, etl, etl_inputs

STORAGE = dict(provider="s3", region="us-east-1", endpoint="http://127.0.0.1:19000",
               allow_http=True, access_key_id="local-development",
               secret_access_key="local-development-only-secret", lease_ms=2000)


def client_for(config):
    return Client.remote(config["url"]) if "url" in config else Client(**config)


def run_task(client, task_id, handlers):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        state = client.inspect(task_id)
        if state["status"] == "completed":
            return state
        if state["status"] in ("failed", "cancelled"):
            raise RuntimeError(f"Workflow {state['status']}: {state.get('error')}")
        client.run_once(handlers)
        time.sleep(.1)
    raise TimeoutError("Workflow did not finish within two minutes")


def worker(config, task_id, stage, ready):
    if "url" in config:
        for key in list(os.environ):
            if key.startswith(("AWS_", "DEOOS_", "GOOGLE_", "AZURE_")):
                os.environ.pop(key)
    def pause():
        ready.send(stage)
        while True:
            time.sleep(1)

    def interrupted(ctx, inputs):
        original = ctx.step

        def step(name, callback):
            def execute():
                result = callback()
                if stage == "write-gap" and name == "write-csv":
                    pause()  # Side effect finished, but its checkpoint does not exist.
                return result

            result = original(name, execute)
            if stage == "page-two" and name == "fetch-page-2":
                pause()  # Pause only after the checkpoint has committed.
            return result

        ctx.step = step
        return etl(ctx, inputs)

    with client_for(config) as client:
        run_task(client, task_id, {HANDLER: interrupted})


def kill_at(config, task_id, stage):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=worker, args=(config, task_id, stage, child))
    process.start()
    child.close()
    try:
        deadline = time.monotonic() + 120
        while not parent.poll(.1):
            if not process.is_alive():
                raise RuntimeError(f"Worker exited before {stage}: {process.exitcode}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Worker did not reach {stage}")
        if parent.recv() != stage:
            raise RuntimeError("Unexpected recovery barrier")
        process.kill()
        process.join(timeout=10)
        if process.is_alive():
            raise RuntimeError("Worker did not stop")
    finally:
        if process.is_alive():
            process.kill()
        process.join(timeout=10)
        parent.close()


class SourceSnapshot:
    """Capture each live page once; count requests to unchanged replayed bytes."""
    def __init__(self):
        self.pages = {}
        self.requests = collections.Counter()
        for page in range(1, 4):
            request = urllib.request.Request(
                f"{SOURCE}?page={page}&per_page=30",
                headers={"User-Agent": "DEOOS-ETL-example"})
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
            value = json.loads(raw)
            if not isinstance(value, list) or not value:
                raise ValueError("DEV API returned an empty or invalid page")
            self.pages[page] = raw
        source = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused):
                pass

            def do_GET(self):
                url = urllib.parse.urlsplit(self.path)
                query = urllib.parse.parse_qs(url.query)
                if url.path != "/articles" or query.get("page") not in (["1"], ["2"], ["3"]) or query.get("per_page") != ["30"]:
                    self.send_error(400)
                    return
                page = int(query["page"][0])
                source.requests[page] += 1
                raw = source.pages[page]
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/articles"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def start_server(binary, bucket, prefix, log):
    with socket.socket() as socket_:
        socket_.bind(("127.0.0.1", 0))
        port = socket_.getsockname()[1]
    # All development settings stay inside the launcher; no exports are needed.
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(("AWS_", "DEOOS_", "GOOGLE_", "AZURE_")) or key in ("ENGINE_TOKEN", "EXECUTION_PREFIX", "LEASE_MS"):
            env.pop(key)
    env.update(DEOOS_STORAGE_PROVIDER="s3", DEOOS_STORAGE_BUCKET=bucket,
               AWS_ENDPOINT=STORAGE["endpoint"], AWS_ALLOW_HTTP="true",
               AWS_ACCESS_KEY_ID=STORAGE["access_key_id"], AWS_SECRET_ACCESS_KEY=STORAGE["secret_access_key"],
               AWS_REGION=STORAGE["region"], EXECUTION_PREFIX=prefix, LEASE_MS="2000",
               ENGINE_BIND=f"127.0.0.1:{port}")
    process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("DEOOS server exited during startup")
            try:
                with urllib.request.urlopen(url + "/health", timeout=1) as response:
                    if response.status == 200:
                        return process, {"url": url}
            except OSError:
                time.sleep(.1)
        raise TimeoutError("DEOOS server did not start")
    except BaseException:
        process.kill()
        process.wait(timeout=10)
        raise


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    args = parser.parse_args()
    directory = Path(__file__).resolve().parents[1] / "outputs" / "devto-etl" / uuid.uuid4().hex[:12]
    directory.mkdir(parents=True)
    bucket = "deoos-devto-" + uuid.uuid4().hex[:20]
    s3 = boto3.client("s3", endpoint_url=STORAGE["endpoint"], region_name=STORAGE["region"],
                      aws_access_key_id=STORAGE["access_key_id"], aws_secret_access_key=STORAGE["secret_access_key"])
    report = {"cases": [], "bucket": bucket, "prefix": "devto-etl/", "cleaned": False, "source": SOURCE,
              "source_policy": "Three live API pages captured once; both modes replay those unchanged bytes through a counted local HTTP source."}
    source = None
    create_intent = False
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with urllib.request.urlopen(STORAGE["endpoint"] + "/health", timeout=2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("RustFS did not become ready")
                time.sleep(.5)
        create_intent = True
        (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        s3.create_bucket(Bucket=bucket)
        print("Capturing three live DEV article pages…", flush=True)
        source = SourceSnapshot()
        report["upstream_page_sha256"] = {page: hashlib.sha256(raw).hexdigest() for page, raw in source.pages.items()}
        for mode in ("library", "server"):
            prefix, task_id = "devto-etl/" + mode, "articles-001"
            config = dict(STORAGE, bucket=bucket, prefix=prefix)
            server = None
            output = directory / mode / "devto_articles.csv"
            output.parent.mkdir()
            with (directory / f"{mode}-server.log").open("w") as log:
                try:
                    if mode == "server":
                        server, config = start_server(args.server, bucket, prefix, log)
                    with client_for(config) as client:
                        before = source.requests.copy()
                        client.submit(task_id, HANDLER, etl_inputs(str(output), source=source.url), max_attempts=5)
                        kill_at(config, task_id, "page-two")
                        first = client.inspect(task_id)
                        check(first["status"] == "running" and first["attempts"] == 1, "Wrong first crash state")
                        check(set(first["steps"]) == {"fetch-page-1", "fetch-page-2"}, "Page-two checkpoint missing")
                        check(source.requests - before == {1: 1, 2: 1}, "Unexpected first page request counts")
                        check(not output.exists(), "CSV appeared before extraction finished")
                        kill_at(config, task_id, "write-gap")
                        gap = client.inspect(task_id)
                        check(gap["status"] == "running" and gap["attempts"] == 2, "Wrong second crash state")
                        check("normalize" in gap["steps"] and "write-csv" not in gap["steps"], "CSV checkpoint already committed")
                        check(source.requests - before == {1: 1, 2: 1, 3: 1}, "Completed pages were fetched during recovery")
                        written = output.read_bytes()
                        final = run_task(client, task_id, HANDLERS)
                        check(final["attempts"] == 3, "Workflow did not recover from both kills")
                        check(output.read_bytes() == written, "CSV retry changed the output")
                        check(source.requests - before == {1: 1, 2: 1, 3: 1}, "A completed page was fetched again")
                        saved = client.request(f"/tasks/{task_id}/steps/normalize")
                        expected = {}
                        for page, raw in source.pages.items():
                            captured = json.loads(raw)
                            check(client.request(f"/tasks/{task_id}/steps/fetch-page-{page}") == captured, "Saved page differs from live API snapshot")
                            for article in captured:
                                expected.setdefault(article["id"], {
                                    "id": article["id"], "title": article.get("title"),
                                    "published_at": article.get("published_at"), "url": article.get("url"),
                                    "comments_count": article.get("comments_count"),
                                    "positive_reactions_count": article.get("positive_reactions_count"),
                                    "tag_list": article.get("tag_list"),
                                    "user.username": (article.get("user") or {}).get("username")})
                        check(saved == list(expected.values()), "Normalized values differ from captured API inputs")
                        with output.open(newline="", encoding="utf-8") as file:
                            rows = list(csv.DictReader(file))
                        check(rows == [{key: "" if value is None else str(value) for key, value in row.items()} for row in saved], "CSV differs from saved records")
                        check(len(rows) == len({row["id"] for row in rows}) and len(rows) > 0, "Duplicate or empty CSV rows")
                        check(final["output"] == {"output_path": str(output), "articles": len(rows)}, "Wrong final workflow result")
                        report["cases"].append({"mode": mode, "passed": True, "attempts": final["attempts"],
                            "rows": len(rows), "page_requests": dict(source.requests - before),
                            "csv_sha256": hashlib.sha256(written).hexdigest(), "csv": str(output)})
                        print(f"{mode}: recovered from both kills; {len(rows)} rows, each page fetched once, CSV safely replaced.", flush=True)
                finally:
                    if server is not None:
                        server.terminate()
                        try:
                            server.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            server.kill()
                            server.wait(timeout=10)
        check(report["cases"][0]["csv_sha256"] == report["cases"][1]["csv_sha256"], "Modes produced different CSVs")
    finally:
        try:
            if source is not None:
                source.close()
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
                        raise RuntimeError("Temporary bucket still exists")
                report["cleaned"] = True
        finally:
            (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"CSV files and recovery evidence: {directory}")


if __name__ == "__main__":
    main()

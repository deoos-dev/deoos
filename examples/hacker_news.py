"""Collect a fixed Hacker News snapshot into a local, queryable DuckDB file."""
import argparse
import errno
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import threading
import urllib.parse
import urllib.error
import urllib.request

import duckdb
from deoos import Client, EngineError

HANDLER = "hacker-news.collect.v1"
SOURCE = "https://hacker-news.firebaseio.com/v0"


def collection_inputs(database, count=100, source=SOURCE):
    if not isinstance(database, str) or not Path(database).is_absolute():
        raise ValueError("database must be an absolute local file path")
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 500:
        raise ValueError("count must be an integer from 1 to 500")
    url = urllib.parse.urlsplit(source)
    if (url.scheme not in ("http", "https") or not url.hostname or url.username
            or url.password or url.query or url.fragment):
        raise ValueError("source must be an HTTP(S) base URL without credentials, query, or fragment")
    return {"database": database, "count": count, "source": source.rstrip("/")}


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def save_story(database, task_id, story_id, rank, payload):
    connection = duckdb.connect(database)
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS stories(id BIGINT PRIMARY KEY, payload JSON NOT NULL)")
        connection.execute("""CREATE TABLE IF NOT EXISTS collections(
            task_id VARCHAR NOT NULL, story_id BIGINT NOT NULL, rank INTEGER NOT NULL,
            PRIMARY KEY(task_id, story_id))""")
        connection.execute("BEGIN")
        try:
            # The first saved payload wins, including when a step replays after a crash.
            connection.execute("INSERT INTO stories VALUES (?, ?) ON CONFLICT DO NOTHING",
                               [story_id, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))])
            connection.execute("INSERT INTO collections VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                               [task_id, story_id, rank])
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        return {"id": story_id}
    finally:
        connection.close()


def collect(ctx, inputs):
    inputs = collection_inputs(**inputs)
    database, count, source = inputs["database"], inputs["count"], inputs["source"]

    def snapshot():
        top = fetch_json(source + "/topstories.json")
        if not isinstance(top, list):
            raise ValueError("topstories must be an array")
        ids = top[:count]
        if (len(ids) != count or any(isinstance(i, bool) or not isinstance(i, int)
                                    or not 0 < i <= 9_007_199_254_740_991 for i in ids)
                or len(set(ids)) != count):
            raise ValueError("topstories must provide the requested number of distinct positive IDs")
        return ids

    ids = ctx.step("snapshot", snapshot)
    for rank, story_id in enumerate(ids, 1):
        def fetch_item():
            item = fetch_json(f"{source}/item/{story_id}.json")
            if item is not None and (not isinstance(item, dict) or item.get("id") != story_id):
                raise ValueError(f"item {story_id} must be null or an object with the matching id")
            return item

        payload = ctx.step(f"fetch-{story_id}", fetch_item)
        ctx.step(f"store-{story_id}", lambda: save_story(database, ctx.task["id"], story_id, rank, payload))
    return {"task_id": ctx.task["id"], "stories": len(ids), "database": database}


HANDLERS = {HANDLER: collect}


def create_client():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        return Client.remote(os.environ["ENGINE_URL"], token=os.environ.get("ENGINE_TOKEN"))
    if mode != "library":
        raise ValueError("DEOOS_MODE must be 'library' or 'server'")
    provider = os.environ.get("DEOOS_STORAGE_PROVIDER", "s3")
    if provider == "filesystem":
        return Client(provider=provider, directory=os.environ["DEOOS_STORAGE_DIRECTORY"],
                      prefix=os.environ.get("EXECUTION_PREFIX", "deoos"))
    return Client(provider=provider, bucket=os.environ.get("DEOOS_STORAGE_BUCKET") or os.environ.get("AWS_BUCKET"),
                  prefix=os.environ.get("EXECUTION_PREFIX", "deoos"))


def query(database):
    connection = duckdb.connect(database, read_only=True)
    try:
        counts = connection.execute("SELECT task_id, count(*) FROM collections GROUP BY task_id ORDER BY task_id").fetchall()
        sample = connection.execute("""SELECT c.task_id, c.rank, s.id, s.payload->>'title'
            FROM collections c JOIN stories s ON s.id=c.story_id
            ORDER BY c.task_id, c.rank LIMIT 10""").fetchall()
        return {"database": database, "collections": [{"task_id": task, "stories": count} for task, count in counts],
                "sample": [{"task_id": task, "rank": rank, "id": str(story), "title": title}
                           for task, rank, story, title in sample]}
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("submit", "schedule", "work", "inspect", "query"))
    parser.add_argument("--id")
    parser.add_argument("--database", default="./hacker-news.duckdb")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--source", default=SOURCE)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-ms", type=int, default=86_400_000)
    parser.add_argument("--first-due-ms", type=int)
    args = parser.parse_args()
    database = str(Path(args.database).resolve())
    if args.action == "query":
        print(json.dumps(query(database), indent=2))
        return
    if args.action in ("submit", "schedule", "inspect") and not args.id:
        parser.error(f"{args.action} requires --id")
    with create_client() as client:
        if args.action in ("submit", "schedule"):
            inputs = collection_inputs(database, args.count, args.source)
            if args.action == "submit":
                result = client.submit(args.id, HANDLER, inputs, max_attempts=5, retry_ms=200)
            else:
                result = client.schedule(args.id, HANDLER, inputs, args.interval_ms,
                                         first_due_ms=args.first_due_ms, missed="latest", overlap="skip",
                                         max_attempts=5, retry_ms=200)
        elif args.action == "inspect":
            result = client.view(args.id)
        elif args.once:
            result = {"worked": client.run_once(HANDLERS)}
        else:
            stopped = threading.Event()
            signal.signal(signal.SIGINT, lambda *_: stopped.set())
            signal.signal(signal.SIGTERM, lambda *_: stopped.set())

            def on_error(error, task_id):
                if task_id is None:
                    reason = error.reason if isinstance(error, urllib.error.URLError) else error
                    transport_errors = (errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
                                        errno.ETIMEDOUT, errno.EPIPE, errno.ENETUNREACH,
                                        errno.EHOSTUNREACH)
                    transient = (error.status == 503 if isinstance(error, EngineError)
                                 else not isinstance(error, urllib.error.HTTPError)
                                 and not isinstance(reason, ssl.SSLError)
                                 and (isinstance(reason, (TimeoutError, ConnectionRefusedError,
                                                          ConnectionResetError, ConnectionAbortedError,
                                                          BrokenPipeError, http.client.IncompleteRead))
                                      or isinstance(reason, OSError) and reason.errno in transport_errors
                                      or isinstance(reason, socket.gaierror) and reason.errno == socket.EAI_AGAIN))
                    if not transient:
                        return "propagate"
                    print(f"Worker polling failed; retrying: {error}", flush=True)
                    return "continue"
                print(f"Task {task_id} failed: {error}", flush=True)
                return "continue"

            client.run_worker(HANDLERS, stop_event=stopped, poll_interval=1, on_error=on_error)
            return
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

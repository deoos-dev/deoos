"""Server hello talks to deoos-server. The worker has no storage credentials."""
import os
import subprocess
import sys
import uuid
from pathlib import Path

import paths
from engine_server import (
    create_bucket, delete_bucket, server_worker_env, start_deoos_server, stop_process, wait_for_rustfs,
)

ROOT = paths.ROOT
ENDPOINT = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")


def run(command, env):
    completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
    if completed.returncode != 0:
        raise RuntimeError(f"{command} exited {completed.returncode}\n{completed.stdout}\n{completed.stderr}")
    return completed.stdout


def main():
    wait_for_rustfs(ENDPOINT)
    bucket = "deoos-hello-" + uuid.uuid4().hex[:20]
    prefix = "server-" + uuid.uuid4().hex[:8]
    directory = ROOT / "outputs" / "server-hello"
    directory.mkdir(parents=True, exist_ok=True)
    s3 = create_bucket(ENDPOINT, bucket)
    binary = os.environ.get("ENGINE_BINARY")
    server = None
    log = open(directory / "deoos-server.log", "w")
    try:
        server, remote = start_deoos_server(binary, bucket, prefix, log, ENDPOINT, lease_ms=5000)
        env = server_worker_env()
        env.update(ENGINE_URL=remote["url"], ENGINE_TOKEN=remote["token"])
        python_out = run([sys.executable, "python/server/hello.py", "greet-server-py"], env)
        node_out = run(["node", "typescript/server/hello.mjs", "greet-server-ts"], env)
        if python_out.strip() != "Hello, World!" or node_out.strip() != "Hello, World!":
            raise RuntimeError(f"unexpected greeting\npython: {python_out!r}\nnode: {node_out!r}")
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(row["Key"] for row in page.get("Contents", []))
        if not keys:
            raise RuntimeError("server hello did not write the greeting through deoos-server")
        print("server hello: Hello, World!")
        print("python and typescript both reached deoos-server without storage credentials")
    finally:
        stop_process(server)
        log.close()
        delete_bucket(s3, bucket)


if __name__ == "__main__":
    main()

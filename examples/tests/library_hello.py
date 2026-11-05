"""Library hello writes one greeting to RustFS. The engine runs in-process."""
import os
import subprocess
import sys
import uuid

import paths
from engine_server import create_bucket, delete_bucket, wait_for_rustfs

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
    prefix = "library-" + uuid.uuid4().hex[:8]
    s3 = create_bucket(ENDPOINT, bucket)
    env = os.environ.copy()
    env.update(
        DEOOS_STORAGE_PROVIDER="s3",
        DEOOS_STORAGE_BUCKET=bucket,
        EXECUTION_PREFIX=prefix,
        AWS_ENDPOINT=ENDPOINT,
        AWS_ALLOW_HTTP="true",
        AWS_ACCESS_KEY_ID="local-development",
        AWS_SECRET_ACCESS_KEY="local-development-only-secret",
        AWS_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true",
        PYTHONUNBUFFERED="1",
    )
    env.pop("ENGINE_URL", None)
    env.pop("ENGINE_TOKEN", None)
    try:
        python_out = run([sys.executable, "python/library/hello.py", "greet-library-py"], env)
        node_out = run(["node", "typescript/library/hello.mjs", "greet-library-ts"], env)
        if python_out.strip() != "Hello, World!" or node_out.strip() != "Hello, World!":
            raise RuntimeError(f"unexpected greeting\npython: {python_out!r}\nnode: {node_out!r}")
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(row["Key"] for row in page.get("Contents", []))
        if not keys:
            raise RuntimeError("library hello did not write the greeting to RustFS")
        print("library hello: Hello, World!")
        print("python and typescript both stored the greeting in RustFS")
    finally:
        delete_bucket(s3, bucket)


if __name__ == "__main__":
    main()

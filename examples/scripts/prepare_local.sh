#!/bin/bash
# Source this file: . ./scripts/prepare_local.sh
# It installs the pinned deoos release, starts RustFS, and exports local credentials.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(CDPATH= cd -- "$here/.." && pwd)
"$here/install_deoos.sh"
# shellcheck disable=SC1091
. "$root/.deoos/env.sh"
export DEOOS_STORAGE_PROVIDER=s3
export DEOOS_STORAGE_BUCKET=deoos-examples
export AWS_ENDPOINT=http://127.0.0.1:19000
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=local-development
export AWS_SECRET_ACCESS_KEY=local-development-only-secret
export AWS_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
docker compose -f "$root/compose.yaml" up -d rustfs
python - <<'PY'
import time
import urllib.request
import boto3
from botocore.exceptions import ClientError

deadline = time.monotonic() + 60
while True:
    try:
        with urllib.request.urlopen("http://127.0.0.1:19000/health", timeout=2) as response:
            if response.status == 200:
                break
    except OSError:
        pass
    if time.monotonic() >= deadline:
        raise SystemExit("RustFS did not become ready on port 19000")
    time.sleep(0.5)

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:19000",
    region_name="us-east-1",
    aws_access_key_id="local-development",
    aws_secret_access_key="local-development-only-secret",
)
try:
    s3.head_bucket(Bucket="deoos-examples")
except ClientError:
    s3.create_bucket(Bucket="deoos-examples")
PY
echo "RustFS is ready. Bucket deoos-examples is selected. deoos-server is $ENGINE_BINARY."

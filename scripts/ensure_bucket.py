"""Create the local dev bucket if it is missing. Uses only the Python standard library."""
import datetime
import hashlib
import hmac
import http.client
import os
import sys
import time
import urllib.parse


def _sign(key, message):
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret, date, region):
    key = _sign(("AWS4" + secret).encode("utf-8"), date)
    key = _sign(key, region)
    key = _sign(key, "s3")
    return _sign(key, "aws4_request")


def create_once(endpoint, bucket, access, secret, region):
    url = urllib.parse.urlsplit(endpoint)
    if url.scheme != "http" or not url.hostname:
        raise ValueError(f"bucket create expects an http endpoint, got {endpoint}")
    host, port = url.hostname, url.port or 80
    path = "/" + bucket
    payload_hash = hashlib.sha256(b"").hexdigest()
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    host_header = f"{host}:{port}"
    canonical_headers = (
        f"host:{host_header}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = "\n".join(["PUT", path, "", canonical_headers, signed_headers, payload_hash])
    scope = f"{date}/{region}/s3/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(
        _signing_key(secret, date, region), string_to_sign.encode("utf-8"), hashlib.sha256,
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    connection = http.client.HTTPConnection(host, port, timeout=2)
    try:
        connection.putrequest("PUT", path, skip_host=True, skip_accept_encoding=True)
        connection.putheader("Host", host_header)
        connection.putheader("Content-Length", "0")
        connection.putheader("x-amz-content-sha256", payload_hash)
        connection.putheader("x-amz-date", amz_date)
        connection.putheader("Authorization", authorization)
        connection.endheaders()
        response = connection.getresponse()
        body = response.read().decode("utf-8", "replace")
        return response.status, body
    finally:
        connection.close()


def main():
    endpoint = os.environ["AWS_ENDPOINT"]
    bucket = os.environ["DEOOS_STORAGE_BUCKET"]
    access = os.environ["AWS_ACCESS_KEY_ID"]
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    region = os.environ.get("AWS_REGION", "us-east-1")
    deadline = time.monotonic() + 30
    last = "RustFS did not accept the bucket create"
    while time.monotonic() < deadline:
        try:
            status, body = create_once(endpoint, bucket, access, secret, region)
        except OSError as error:
            last = str(error)
            time.sleep(0.5)
            continue
        if status in (200, 204, 409):
            return 0
        last = f"HTTP {status} {body.strip()}"
        if status in (400, 401, 403):
            break
        time.sleep(0.5)
    print(f"could not create bucket {bucket} at {endpoint}: {last}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

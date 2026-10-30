"""Storage qualification, credential redaction, and owned-prefix cleanup.

Runs against the real engine binary. External buckets and containers must
already exist and are never created or deleted:

    python tests/storage_checks.py
    python tests/storage_checks.py --backend s3 --bucket DEDICATED_TEST_BUCKET
    python tests/storage_checks.py --backend r2 --bucket DEDICATED_TEST_BUCKET
    python tests/storage_checks.py --backend gcs --bucket DEDICATED_TEST_BUCKET
    python tests/storage_checks.py --backend azure --bucket DEDICATED_TEST_CONTAINER

Cleanup dependencies load only for the selected provider: boto3 for S3/R2,
google-cloud-storage for GCS, azure-storage-blob for Azure. Supply explicit
AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (optional AWS_SESSION_TOKEN),
GOOGLE_SERVICE_ACCOUNT_PATH, or AZURE_STORAGE_ACCOUNT_NAME plus exactly one of
AZURE_STORAGE_ACCOUNT_KEY / AZURE_STORAGE_SAS_KEY. R2 also requires AWS_ENDPOINT.
"""
import argparse
import datetime
import http.server
import json
import os
import pathlib
import subprocess
import sys
import threading
import urllib.parse
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
REPORT = ROOT / "outputs/evidence/storage-checks-rustfs.json"
SERVER = pathlib.Path(os.environ.get(
    "ENGINE_BINARY", str(ROOT / "engine/target/release/deoos-server"))).resolve()


def storage_environment(backend, bucket):
    """Freeze one explicit identity for both native workers and cleanup SDKs."""
    source = dict(os.environ)
    if backend == "rustfs":
        if bucket is not None:
            raise ValueError("RustFS uses a generated local bucket")
        bucket = "deoos-storage-" + uuid.uuid4().hex[:20]
    elif (not isinstance(bucket, str) or not 3 <= len(bucket) <= 222 or not bucket.isascii()
          or any(not (char.isalnum() or char in "_.-") for char in bucket)):
        raise ValueError("external targets require a precreated bucket/container name")
    env = {key: value for key, value in source.items()
           if not key.startswith(("AWS_", "GOOGLE_", "AZURE_", "DEOOS_STORAGE_"))
           and key not in {"SERVICE_ACCOUNT", "ENGINE_BIND", "ENGINE_URL", "ENGINE_TOKEN",
                           "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY"}}
    # Remote runs use the engine's normal lease. Local qualification uses a short one.
    env.update(DEOOS_STORAGE_PROVIDER="s3" if backend in ("rustfs", "s3", "r2") else backend,
               DEOOS_STORAGE_BUCKET=bucket, EXECUTION_PREFIX="storage-checks-" + uuid.uuid4().hex,
               LEASE_MS="1500" if backend == "rustfs" else "30000")
    if backend == "rustfs":
        env.update(AWS_ACCESS_KEY_ID="local-development", AWS_SECRET_ACCESS_KEY="local-development-only-secret",
                   AWS_REGION="us-east-1", AWS_ENDPOINT="http://127.0.0.1:19000", AWS_ALLOW_HTTP="true")
    elif backend in ("s3", "r2"):
        for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            if not source.get(key):
                raise ValueError("S3/R2 require explicit environment credentials")
            env[key] = source[key]
        for key in ("AWS_SESSION_TOKEN", "AWS_ALLOW_HTTP"):
            if source.get(key):
                env[key] = source[key]
        endpoint = source.get("AWS_ENDPOINT")
        alternate = source.get("AWS_ENDPOINT_URL_S3") or source.get("AWS_ENDPOINT_URL")
        if alternate and (not endpoint or endpoint.rstrip("/") != alternate.rstrip("/")):
            raise ValueError("cleanup and native S3 endpoint settings conflict; use AWS_ENDPOINT")
        if endpoint:
            env["AWS_ENDPOINT"] = endpoint
        env["AWS_REGION"] = source.get("AWS_REGION", "auto" if backend == "r2" else "us-east-1")
        if backend == "r2" and not env.get("AWS_ENDPOINT"):
            raise ValueError("R2 requires an explicit AWS_ENDPOINT")
    elif backend == "gcs":
        path = source.get("GOOGLE_SERVICE_ACCOUNT_PATH")
        if not path:
            raise ValueError("GCS requires GOOGLE_SERVICE_ACCOUNT_PATH")
        path = pathlib.Path(path).resolve()
        account = json.loads(path.read_text())
        if (account.get("type") != "service_account" or not account.get("project_id")
                or account.get("gcs_base_url") or account.get("disable_oauth")):
            raise ValueError("GCS requires a standard service account with an explicit project")
        env["GOOGLE_SERVICE_ACCOUNT_PATH"] = str(path)
    elif backend == "azure":
        account = source.get("AZURE_STORAGE_ACCOUNT_NAME")
        key, sas = source.get("AZURE_STORAGE_ACCOUNT_KEY"), source.get("AZURE_STORAGE_SAS_KEY")
        if (not account or not 3 <= len(account) <= 24 or not account.isascii()
                or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789" for char in account)
                or bool(key) == bool(sas)):
            raise ValueError("Azure requires an explicit account and exactly one key or SAS")
        env["AZURE_STORAGE_ACCOUNT_NAME"] = account
        env["AZURE_STORAGE_ACCOUNT_KEY" if key else "AZURE_STORAGE_SAS_KEY"] = key or sas.lstrip("?")
        endpoint, alternate = source.get("AZURE_STORAGE_ENDPOINT"), source.get("AZURE_ENDPOINT")
        if endpoint and alternate and endpoint.rstrip("/") != alternate.rstrip("/"):
            raise ValueError("Azure endpoint settings conflict")
        if endpoint or alternate:
            env["AZURE_STORAGE_ENDPOINT"] = endpoint or alternate
    if env["DEOOS_STORAGE_PROVIDER"] == "s3":
        env["DEOOS_STORAGE_BUCKET"] = bucket
    endpoint = env.get("AWS_ENDPOINT") or env.get("AZURE_STORAGE_ENDPOINT")
    if endpoint:
        parsed = urllib.parse.urlsplit(endpoint)
        allowed_http = env.get("AWS_ALLOW_HTTP", "").lower() == "true"
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment
                or parsed.scheme not in (("https", "http") if allowed_http else ("https",))):
            raise ValueError("storage endpoint must be a valid base URL without credentials or query")
    return env, bucket


class OccupiedPrefixError(RuntimeError):
    """The candidate namespace contains existing data and cannot be adopted."""


class TestStorage:
    """Small test-only cleanup helpers; never delete an external target."""
    def __init__(self, backend, env):
        self.backend, self.env = backend, env
        self.bucket = env["DEOOS_STORAGE_BUCKET"]
        if backend in ("rustfs", "s3", "r2"):
            import boto3
            from botocore.config import Config
            self.client = boto3.client(
                "s3", endpoint_url=env.get("AWS_ENDPOINT"), region_name=env["AWS_REGION"],
                aws_access_key_id=env["AWS_ACCESS_KEY_ID"], aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
                aws_session_token=env.get("AWS_SESSION_TOKEN"), config=Config(
                    signature_version="s3v4", ignore_configured_endpoint_urls=True,
                    s3={"addressing_style": "path"}))
        elif backend == "gcs":
            from google.cloud import storage
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(env["GOOGLE_SERVICE_ACCOUNT_PATH"])
            self.client = storage.Client(project=credentials.project_id, credentials=credentials,
                                         client_options={"api_endpoint": "https://storage.googleapis.com"})
        else:
            from azure.core.credentials import AzureNamedKeyCredential, AzureSasCredential
            from azure.storage.blob import BlobServiceClient
            account = env["AZURE_STORAGE_ACCOUNT_NAME"]
            credential = (AzureNamedKeyCredential(account, env["AZURE_STORAGE_ACCOUNT_KEY"])
                          if "AZURE_STORAGE_ACCOUNT_KEY" in env
                          else AzureSasCredential(env["AZURE_STORAGE_SAS_KEY"]))
            self.client = BlobServiceClient(
                env.get("AZURE_STORAGE_ENDPOINT", f"https://{account}.blob.core.windows.net"),
                credential=credential)

    @staticmethod
    def boundary(prefix):
        if (not prefix.startswith("storage-checks-") or len(prefix) != len("storage-checks-") + 32
                or any(char not in "0123456789abcdef" for char in prefix[len("storage-checks-"):])):
            raise ValueError("cleanup requires the exact generated run prefix")
        return prefix + "/"

    def names(self, prefix):
        boundary = self.boundary(prefix)
        if self.backend in ("rustfs", "s3", "r2"):
            names = [item["Key"] for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=boundary) for item in page.get("Contents", [])]
        elif self.backend == "gcs":
            names = [blob.name for blob in self.client.list_blobs(self.bucket, prefix=boundary)]
        else:
            container = self.client.get_container_client(self.bucket)
            names = [blob.name for blob in container.list_blobs(name_starts_with=boundary)]
        assert all(name.startswith(boundary) for name in names), "provider returned an object outside owned prefix"
        return names

    def delete_prefix(self, prefix):
        names = self.names(prefix)
        if self.backend in ("rustfs", "s3", "r2"):
            for offset in range(0, len(names), 1000):
                result = self.client.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": [{"Key": name} for name in names[offset:offset + 1000]]})
                assert not result.get("Errors"), "owned-prefix object deletion failed"
        elif self.backend == "gcs":
            bucket = self.client.bucket(self.bucket)
            for name in names:
                bucket.blob(name).delete()
        else:
            container = self.client.get_container_client(self.bucket)
            for offset in range(0, len(names), 256):
                list(container.delete_blobs(*names[offset:offset + 256], delete_snapshots="include"))
        assert self.names(prefix) == [], "owned-prefix live objects remain after deletion"
        return len(names)

    def create_local_bucket(self):
        assert self.backend == "rustfs", "external targets must already exist"
        self.client.create_bucket(Bucket=self.bucket)

    def delete_local_bucket(self):
        assert self.backend == "rustfs", "external targets must be preserved"
        from botocore.exceptions import ClientError
        self.client.delete_bucket(Bucket=self.bucket)
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        else:
            raise AssertionError("generated local test bucket still exists")

    def close(self):
        self.client.close()


def write_report(report, path=REPORT):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(path)


def qualify_storage(env):
    probe_env = dict(env, EXECUTION_PREFIX=env["EXECUTION_PREFIX"] + "/storage-check")
    result = subprocess.run([str(SERVER), "--check-storage"], env=probe_env, cwd=ROOT,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, "native storage qualification failed"
    probe = json.loads(result.stdout)
    assert probe["passed"] is True and probe["cleaned"] is True
    assert probe["writers"] == 32
    assert probe["create_rejections"] == probe["update_rejections"] == 31
    assert 0 <= probe["create_winner"] < 32 and 0 <= probe["update_winner"] < 32
    assert probe["stale_update_preserved_content"] is True
    assert set(probe["checks"]) == {
        "conditional_create", "read_after_create", "list_after_create", "conditional_update",
        "stale_update_rejected_without_write", "read_after_delete", "list_after_delete"}
    return probe


def check_cli_redaction(env):
    # This URL fails inside the Azure builder before any transport is created.
    # Only child environment overrides are used; no credential acquisition or
    # cloud request is reached.
    canary = "review-secret-canary"
    malformed_endpoint = "https://?sig=" + canary
    child_env = {key: value for key, value in env.items() if not key.startswith("AZURE_")}
    child_env.update(DEOOS_STORAGE_PROVIDER="azure", DEOOS_STORAGE_BUCKET="fake-container",
                     AZURE_STORAGE_ACCOUNT_NAME="fakeaccount", AZURE_STORAGE_USE_EMULATOR="false",
                     AZURE_STORAGE_ENDPOINT=malformed_endpoint, AZURE_ENDPOINT=malformed_endpoint)
    result = subprocess.run([str(SERVER), "--check-storage"], env=child_env, cwd=ROOT,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode != 0, "malformed provider endpoint was unexpectedly accepted"
    assert canary not in result.stdout and canary not in result.stderr, "native CLI leaked the redaction canary"
    assert "invalid Azure configuration" in result.stderr, "malformed endpoint did not stop at configuration"
    return {"passed": True, "provider": "azure", "invalid_endpoint_rejected": True,
            "returncode": result.returncode, "stdout_and_stderr_redacted": True,
            "stopped_before_transport": True}


def check_runtime_redaction(env):
    """Exercise native runtime errors through a fake loopback Azure endpoint."""
    canary = "review-runtime-canary"
    received = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            received.append(self.path)
            payload = ("Provider rejection for " + self.path).encode()
            self.send_response(403)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    child_env = {key: value for key, value in env.items() if not key.startswith("AZURE_")}
    child_env.update(PYTHONPATH=str(ROOT / "clients/python"),
                     AZURE_STORAGE_ACCOUNT_NAME="fakeaccount",
                     AZURE_STORAGE_SAS_KEY="sv=2024-01-01&sig=" + canary,
                     REVIEW_ENDPOINT=f"http://127.0.0.1:{server.server_port}")
    script = '''
import os
from deoos import Client, EngineError
with Client(provider="azure", bucket="fake-container",
            endpoint=os.environ["REVIEW_ENDPOINT"], allow_http=True) as client:
    try:
        client.inspect("review-task")
    except EngineError as error:
        assert error.status == 503
        assert str(error) == "503: storage unavailable; retry shortly and check credentials, endpoint, and bucket"
    else:
        raise AssertionError("fake provider rejection unexpectedly succeeded")
'''
    try:
        result = subprocess.run([sys.executable, "-c", script], env=child_env, cwd=ROOT,
                                capture_output=True, text=True, timeout=10)
        assert result.returncode == 0, "runtime redaction child did not reach expected SDK error"
        assert any(canary in path for path in received), "fake provider never received the SAS canary"
        assert canary not in result.stdout and canary not in result.stderr, "runtime error leaked the SAS canary"
        return {"passed": True, "provider": "fake Azure loopback endpoint",
                "provider_status": 403, "public_sdk_status": 503,
                "stdout_and_stderr_redacted": True}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def check_occupied_prefix_preserved():
    """Inject existing external data into the actual admission/cleanup path."""
    from unittest.mock import patch

    prefix = "storage-checks-" + "0" * 32
    existing = {prefix + "/state.json": b"preexisting data must survive"}
    reports = []

    class ExistingStorage:
        def __init__(self):
            self.objects = dict(existing)
            self.delete_calls = 0
            self.closed = False

        def names(self, candidate):
            assert candidate == prefix
            return list(self.objects)

        def delete_prefix(self, candidate):
            self.delete_calls += 1
            self.objects.clear()
            return len(existing)

        def close(self):
            self.closed = True

    storage = ExistingStorage()
    env = {"DEOOS_STORAGE_PROVIDER": "s3", "DEOOS_STORAGE_BUCKET": "precreated-fixture",
           "EXECUTION_PREFIX": prefix, "LEASE_MS": "30000"}
    replacements = {
        "storage_environment": lambda backend, bucket: (env, "precreated-fixture"),
        "TestStorage": lambda backend, environment: storage,
        "write_report": lambda report, path: reports.append(json.loads(json.dumps(report))),
    }
    with patch.dict(globals(), replacements):
        try:
            main("s3", "precreated-fixture")
        except OccupiedPrefixError:
            pass
        else:
            raise AssertionError("occupied external namespace was admitted")
    assert storage.objects == existing and storage.delete_calls == 0
    assert storage.closed
    rejected = reports[-1]
    assert rejected["prefix_preflight"] == "occupied" and not rejected["prefix_owned"]
    assert not rejected["success"] and not rejected["cleaned"] and rejected["cases"] == []
    assert rejected["error_type"] == "OccupiedPrefixError" and "owned_prefix" not in rejected
    assert rejected["cleanup_skipped_reason"] == "namespace_ownership_not_acquired"
    return {"passed": True, "occupied_prefix_rejected": True,
            "preexisting_object_preserved": True, "delete_calls": 0,
            "acceptance_success": False, "namespace_owned": False}


def main(backend="rustfs", bucket=None):
    env, bucket = storage_environment(backend, bucket)
    prefix = env["EXECUTION_PREFIX"]
    report_path = ROOT / "outputs/evidence" / f"storage-checks-{backend}.json"
    report = {"backend": "local RustFS" if backend == "rustfs" else backend,
              "provider": env["DEOOS_STORAGE_PROVIDER"], "bucket": bucket,
              "candidate_prefix": prefix, "prefix_owned": False, "prefix_preflight": "pending",
              "external_target": backend != "rustfs",
              "cleanup_scope": "generated local bucket" if backend == "rustfs" else "owned-prefix live objects",
              "custom_endpoint": bool(env.get("AWS_ENDPOINT") or env.get("AZURE_STORAGE_ENDPOINT")),
              "lease_ms": int(env["LEASE_MS"]),
              "started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "python": sys.version.split()[0],
              "cases": [], "success": False, "cleaned": False, "cleanup_errors": []}
    if backend == "rustfs":
        report["endpoint"] = env["AWS_ENDPOINT"]
    storage = None
    prefix_owned = False
    write_report(report, report_path)
    try:
        storage = TestStorage(backend, env)
        if backend == "rustfs":
            storage.create_local_bucket()
        # A successful scoped list also proves an external target already exists.
        if storage.names(prefix):
            report["prefix_preflight"] = "occupied"
            raise OccupiedPrefixError()
        prefix_owned = True
        report.update(prefix_owned=True, prefix_preflight="empty", owned_prefix=prefix)
        report["prefix_ownership_gate"] = check_occupied_prefix_preserved()
        report["storage_probe"] = qualify_storage(env)
        assert storage.names(prefix) == [], "qualification left live objects under the owned prefix"
        report["cli_redaction"] = check_cli_redaction(env)
        report["runtime_redaction"] = check_runtime_redaction(env)
        report["success"] = True
        write_report(report, report_path)
    except BaseException as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        if storage is not None and prefix_owned and not report["cleanup_errors"]:
            try:
                report["deleted_live_objects"] = storage.delete_prefix(prefix)
                report["owned_prefix_absent"] = True
                if backend == "rustfs":
                    storage.delete_local_bucket()
                else:
                    # The successful absence re-list preserves the precreated target;
                    # no external bucket/container deletion method is called.
                    report["external_target_preserved"] = True
                report["cleaned"] = True
            except Exception as error:
                report["cleanup_errors"].append({"stage": "owned storage", "error_type": type(error).__name__})
        elif storage is not None:
            report["cleanup_skipped_reason"] = ("process_or_fixture_cleanup_unconfirmed"
                                                if prefix_owned else "namespace_ownership_not_acquired")
        if storage is not None:
            try:
                storage.close()
            except Exception as error:
                report["cleanup_errors"].append({"stage": "storage client", "error_type": type(error).__name__})
        write_report(report, report_path)
    assert report["cleaned"] and not report["cleanup_errors"], "acceptance cleanup failed"
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("rustfs", "s3", "r2", "gcs", "azure"), default="rustfs")
    parser.add_argument("--bucket", help="precreated dedicated external bucket/container; never created or deleted")
    args = parser.parse_args()
    try:
        main(args.backend, args.bucket)
    except BaseException as error:
        print(f"storage checks failed: {type(error).__name__}", file=sys.stderr)
        raise SystemExit(1) from None

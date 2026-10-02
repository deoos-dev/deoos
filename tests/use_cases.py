"""Behavioral acceptance for the example library, using storage and HTTP fixtures.

Run with Python 3.12, after building both native SDKs and the shared server:
    tests/.venv312/bin/python tests/use_cases.py

External targets must already exist and are never created or deleted:
    python tests/use_cases.py --backend s3 --bucket DEDICATED_TEST_BUCKET
    python tests/use_cases.py --backend r2 --bucket DEDICATED_TEST_BUCKET
    python tests/use_cases.py --backend gcs --bucket DEDICATED_TEST_BUCKET
    python tests/use_cases.py --backend azure --bucket DEDICATED_TEST_CONTAINER

Test-only cleanup dependencies are loaded only for the selected provider: boto3
for S3/R2, google-cloud-storage for GCS, azure-storage-blob for Azure. Supply
explicit AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (optional AWS_SESSION_TOKEN),
GOOGLE_SERVICE_ACCOUNT_PATH, or AZURE_STORAGE_ACCOUNT_NAME plus exactly one of
AZURE_STORAGE_ACCOUNT_KEY / AZURE_STORAGE_SAS_KEY. R2 also requires AWS_ENDPOINT.

The HTTP service simulates integrations with an in-memory idempotency ledger that
survives worker replacement. This proves example behavior, not provider support.
"""
import argparse
import collections
import datetime
import hashlib
import http.server
import json
import os
import pathlib
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.parse
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
REPORT = ROOT.parent / "outputs/evidence/use-cases-rustfs.json"
SERVER = pathlib.Path(os.environ.get(
    "ENGINE_BINARY", str(ROOT / "engine/target/release/deoos-engine"))).resolve()


def storage_environment(backend, bucket):
    """Freeze one explicit identity for both native workers and cleanup SDKs."""
    source = dict(os.environ)
    if backend == "rustfs":
        if bucket is not None:
            raise ValueError("RustFS uses a generated local bucket")
        bucket = "deoos-use-cases-" + uuid.uuid4().hex[:20]
    elif (not isinstance(bucket, str) or not 3 <= len(bucket) <= 222 or not bucket.isascii()
          or any(not (char.isalnum() or char in "_.-") for char in bucket)):
        raise ValueError("external targets require a precreated bucket/container name")
    env = {key: value for key, value in source.items()
           if not key.startswith(("AWS_", "GOOGLE_", "AZURE_", "DEOOS_STORAGE_"))
           and key not in {"SERVICE_ACCOUNT", "ENGINE_BIND", "ENGINE_URL", "ENGINE_TOKEN",
                           "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY"}}
    # Remote runs use the engine's normal lease; local expiry tests stay fast.
    env.update(DEOOS_STORAGE_PROVIDER="s3" if backend in ("rustfs", "s3", "r2") else backend,
               DEOOS_STORAGE_BUCKET=bucket, EXECUTION_PREFIX="use-cases-" + uuid.uuid4().hex,
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
        env["AWS_BUCKET"] = bucket
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
        if (not prefix.startswith("use-cases-") or len(prefix) != len("use-cases-") + 32
                or any(char not in "0123456789abcdef" for char in prefix[len("use-cases-"):])):
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


def wait(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise AssertionError("condition did not become true before timeout")


def stop(process):
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)


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
        assert str(error) == "503: storage unavailable"
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


class IntegrationFixture:
    """Record real HTTP requests and deduplicate effects before sending responses."""
    def __init__(self, port=0):
        self.lock = threading.Lock()
        self.requests = []
        self.effects = {}
        self.fail_once = set()
        self.hold_event = None
        self.effect_committed = threading.Event()
        self.release_response = threading.Event()
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, value):
                payload = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # The crash test deliberately kills the requesting worker.

            def do_GET(self):
                if self.path == "/health":
                    self.reply(200, {"status": "ok", "integration": "simulated"})
                    return
                parts = self.path.strip("/").split("/")
                if len(parts) != 4 or parts[0] != "imports" or parts[2] != "pages":
                    self.reply(404, {"error": "unknown fixture route"})
                    return
                page = int(parts[3])
                with fixture.lock:
                    fixture.requests.append({"path": self.path, "key": None})
                self.reply(200, {"records": [{"source": parts[1], "page": page,
                                               "value": page * 10}]})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                key = self.headers.get("Idempotency-Key")
                identifier = body.get("event_id", body.get("invoice_id", body.get("source")))
                with fixture.lock:
                    fixture.requests.append({"path": self.path, "key": key, "body": body})
                    transient = identifier in fixture.fail_once
                    if transient:
                        fixture.fail_once.remove(identifier)
                    if not key:
                        response = (400, {"error": "Idempotency-Key is required"})
                    elif transient:
                        response = (503, {"error": "simulated transient outage"})
                    else:
                        identity = (self.path, key)
                        prior = fixture.effects.get(identity)
                        if prior is not None and prior["body"] != body:
                            response = (409, {"error": "key reused for changed request"})
                        else:
                            if prior is None:
                                if self.path == "/webhooks":
                                    result = {"event_id": body["event_id"], "status": "delivered"}
                                elif self.path == "/invoices":
                                    result = {"invoice_id": body["invoice_id"], "status": "issued"}
                                elif self.path == "/batches":
                                    result = {"source": body["source"], "record_count": len(body["records"])}
                                else:
                                    response = (404, {"error": "unknown fixture route"})
                                    result = None
                                if result is not None:
                                    prior = fixture.effects[identity] = {"body": body, "result": result}
                            if prior is not None:
                                response = (200, prior["result"])
                    hold = identifier == fixture.hold_event and not transient
                if hold:
                    fixture.effect_committed.set()
                    fixture.release_response.wait(20)
                self.reply(*response)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def requests_for(self, identifier):
        with self.lock:
            return [request.copy() for request in self.requests
                    if identifier in request.get("body", {}).values()]

    def effects_for(self, identifier):
        with self.lock:
            return [effect for effect in self.effects.values()
                    if identifier in effect["body"].values()]

    def close(self):
        self.release_response.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


class WorkflowCase:
    def __init__(self, mode, first, second, env, work, fixture):
        self.mode, self.first, self.second = mode, first, second
        self.fixture, self.work = fixture, work
        run_prefix = env.get("EXECUTION_PREFIX") or "use-cases-" + uuid.uuid4().hex
        self.env = dict(env, DEOOS_MODE=mode, SERVICE_URL=fixture.url,
                        EXECUTION_PREFIX=f"{run_prefix}/{mode}/{first}")
        self.processes = []
        self.server = None
        if mode == "server":
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            self.env.update(ENGINE_BIND=f"127.0.0.1:{port}",
                            ENGINE_URL=f"http://127.0.0.1:{port}", ENGINE_TOKEN=uuid.uuid4().hex)
            try:
                self.start_server()
            except BaseException:
                for process in self.processes:
                    stop(process)
                raise
        self.worker_env = dict(self.env, PYTHONPATH=str(ROOT / "clients/python"))
        if mode == "server":
            for key in list(self.worker_env):
                if key.startswith(("AWS_", "GOOGLE_", "AZURE_", "DEOOS_STORAGE_")) or key in {
                    "SERVICE_ACCOUNT", "EXECUTION_PREFIX", "DEOOS_NATIVE_LIBRARY", "DEOOS_NODE_LIBRARY",
                    "STORAGE_EMULATOR_HOST", "API_ENDPOINT_OVERRIDE",
                }:
                    self.worker_env.pop(key)

    def start_server(self):
        self.server = subprocess.Popen([str(SERVER)], env=self.env, cwd=self.work,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.processes.append(self.server)

        def healthy():
            assert self.server.poll() is None, "shared server exited before becoming healthy"
            try:
                with urllib.request.urlopen(self.env["ENGINE_URL"] + "/health", timeout=1) as response:
                    return response.status == 200
            except OSError:
                return False
        wait(healthy)

    def restart_server(self):
        if self.mode == "server":
            stop(self.server)
            self.start_server()

    def argv(self, language, *args):
        executable = sys.executable if language == "python" else shutil.which("node")
        script = self.work / ("use_cases.py" if language == "python" else "use_cases.mjs")
        return [executable, str(script), *args]

    def cli(self, language, *args, check=True):
        result = subprocess.run(self.argv(language, *args), cwd=self.work, env=self.worker_env,
                                capture_output=True, text=True, timeout=30)
        if check:
            assert result.returncode == 0, (args, result.stdout, result.stderr)
            return json.loads(result.stdout)
        return result

    def inspect(self, identifier):
        return self.cli(self.second, "inspect", "--id", identifier)

    def work_once(self, language):
        return self.cli(language, "work", "--once")

    def submit_raw(self, identifier, handler, inputs):
        # Bypass the example CLI's input checks to exercise handler validation of
        # already persisted task input. A one-attempt budget leaves no retry work.
        script = ("import json,sys; from use_cases import create_client; c=create_client(); "
                  "c.submit(sys.argv[1],sys.argv[2],json.loads(sys.argv[3]),max_attempts=1); c.close()")
        result = subprocess.run([sys.executable, "-c", script, identifier, handler, json.dumps(inputs)],
                                cwd=self.work, env=self.worker_env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr

    def run(self):
        results = []
        case_id = f"{self.mode}-{self.first}"

        # A rejected HTTP call leaves no effect or checkpoint, and respects backoff.
        event = "retry-" + case_id
        self.fixture.fail_once.add(event)
        self.cli(self.first, "submit", "webhook", "--id", event, "--event-id", event)
        failure = self.cli(self.first, "work", "--once", check=False)
        assert failure.returncode != 0 and "503" in failure.stderr, failure.stderr
        retry = self.inspect(event)
        assert retry["status"] == "queued" and retry["attempts"] == 1
        assert "deliver" not in retry["steps"] and not self.fixture.effects_for(event)
        if int(time.time() * 1000) < retry["available_at"]:
            # Process startup and storage requests can cross the retry deadline.
            early = self.work_once(self.second)
            assert set(early) == {"worked"} and isinstance(early["worked"], bool)
        wait(lambda: int(time.time() * 1000) >= retry["available_at"])
        self.work_once(self.second)
        completed = self.inspect(event)
        requests = self.fixture.requests_for(event)
        assert completed["status"] == "completed" and completed["attempts"] == 2
        retry_claims = [entry for entry in completed["history"]
                        if entry["event"] == "claim" and entry["attempts"] == 2]
        assert len(retry_claims) == 1
        assert retry_claims[0]["at_ms"] >= retry["available_at"]
        assert completed["output"] == {"event_id": event, "status": "delivered"}
        assert len(requests) == 2 and len({request["key"] for request in requests}) == 1
        assert len(self.fixture.effects_for(event)) == 1
        assert self.work_once(self.first) == {"worked": False}
        results.append({"case": "transient HTTP 503", "requests": 2, "effects": 1,
                        "attempts": completed["attempts"], "retry_available_at_ms": retry["available_at"],
                        "retry_claim_at_ms": retry_claims[0]["at_ms"]})

        # The public continuous command catches a transient handler error, waits,
        # and retries in its original process instead of requiring a supervisor.
        event = "continuous-" + case_id
        self.fixture.fail_once.add(event)
        self.cli(self.first, "submit", "webhook", "--id", event, "--event-id", event)
        worker = subprocess.Popen(self.argv(self.first, "work"), cwd=self.work, env=self.worker_env,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        self.processes.append(worker)

        def failed_attempt():
            assert worker.poll() is None, "continuous worker exited on its transient HTTP failure"
            state = self.inspect(event)
            return state if state["status"] == "queued" and state["attempts"] == 1 else None

        retry = wait(failed_attempt)
        assert "deliver" not in retry["steps"] and not self.fixture.effects_for(event)

        def completed_in_same_process():
            assert worker.poll() is None, "continuous worker exited before recovery"
            state = self.inspect(event)
            return state if state["status"] == "completed" else None

        completed = wait(completed_in_same_process)
        assert completed["attempts"] == 2 and completed["output"] == {"event_id": event, "status": "delivered"}
        assert worker.poll() is None
        pid = worker.pid
        stop(worker)
        _, stderr = worker.communicate(timeout=10)
        assert "503" in stderr, stderr
        requests = self.fixture.requests_for(event)
        assert len(requests) == 2 and len({request["key"] for request in requests}) == 1
        assert len(self.fixture.effects_for(event)) == 1
        results.append({"case": "continuous worker retries HTTP 503", "language": self.first,
                        "worker_pid": pid, "same_process_survived_failure": True,
                        "observed_queued_failed_attempt": True, "attempts": 2,
                        "requests": 2, "effects": 1, "worker_stopped_after_completion": True})

        invalid = "invalid-" + case_id
        self.submit_raw(invalid, "usecase.invoice.v1", {
            "service_url": self.fixture.url, "invoice_id": invalid,
            "customer_id": "customer-demo", "amount_cents": 0})
        failure = self.cli(self.first, "work", "--once", check=False)
        assert failure.returncode != 0 and "amount_cents" in failure.stderr, failure.stderr
        rejected = self.inspect(invalid)
        assert rejected["status"] == "failed" and rejected["attempts"] == 1
        assert rejected["steps"] == {} and self.fixture.requests_for(invalid) == []
        results.append({"case": "handler rejects persisted invalid amount", "language": self.first,
                        "status": "failed", "invoice_requests": 0, "effects": 0})

        # These concrete payloads previously differed between Python and JS
        # admission, or passed admission but failed inside the HTTP envelope.
        def reject_before_creation(label, payload, expected_error):
            identifier = label + "-" + case_id
            for language in (self.first, self.second):
                failure = self.cli(language, "submit", "webhook", "--id", identifier,
                                   "--event-id", identifier, "--payload", json.dumps(payload), check=False)
                assert failure.returncode != 0 and expected_error in failure.stderr, failure.stderr
                absent = self.cli(language, "inspect", "--id", identifier, check=False)
                assert absent.returncode != 0 and "404" in absent.stderr, absent.stderr
            assert self.fixture.requests_for(identifier) == []
            results.append({"case": label, "rejected_by": [self.first, self.second],
                            "task_created": False, "webhook_requests": 0})

        def deliver_payload(label, payload):
            identifier = label + "-" + case_id
            self.cli(self.first, "submit", "webhook", "--id", identifier,
                     "--event-id", identifier, "--payload", json.dumps(payload))
            self.work_once(self.second)
            state = self.inspect(identifier)
            assert state["status"] == "completed" and state["output"] == {
                "event_id": identifier, "status": "delivered"}
            effects = self.fixture.effects_for(identifier)
            assert len(effects) == 1 and effects[0]["body"]["payload"] == payload
            results.append({"case": label, "admitted_by": self.first, "executed_by": self.second,
                            "webhook_requests": 1, "effects": 1})

        reject_before_creation("fraction-former-mismatch", {"values": [1e-7] * 11_500}, "65536-byte budget")
        deliver_payload("fraction-within-shared-budget", {"values": [1e-7] * 1980})
        nested = 0
        for _ in range(19):
            nested = {"child": nested}
        deliver_payload("depth19-envelope-supported", nested)
        reject_before_creation("depth20-envelope-rejected", {"child": nested}, "19 nesting levels")

        # Waiting returns from the worker process; storage alone holds approval state.
        invoice = "approve-" + case_id
        self.cli(self.first, "submit", "invoice", "--id", invoice, "--invoice-id", invoice)
        self.work_once(self.first)
        paused = self.inspect(invoice)
        assert paused["status"] == "waiting" and paused["waiting_on"]["name"] == "approval"
        assert not self.fixture.effects_for(invoice)
        self.restart_server()
        assert self.inspect(invoice)["status"] == "waiting"
        assert self.work_once(self.second) == {"worked": False}
        self.cli(self.second, "signal", "--id", invoice)
        self.work_once(self.second)
        approved = self.inspect(invoice)
        assert approved["status"] == "completed"
        assert approved["output"] == {"invoice_id": invoice, "status": "issued"}
        assert len(self.fixture.effects_for(invoice)) == 1
        assert self.fixture.effects_for(invoice)[0]["body"] == {
            "invoice_id": invoice, "customer_id": "customer-demo", "amount_cents": 2500}
        results.append({"case": "durable approval", "resident_worker_required": False,
                        "replacement_language": self.second, "effects": 1,
                        "shared_server_restarted": self.mode == "server"})

        declined = "decline-" + case_id
        self.cli(self.first, "submit", "invoice", "--id", declined, "--invoice-id", declined)
        self.work_once(self.first)
        self.cli(self.second, "signal", "--id", declined, "--decline")
        self.work_once(self.second)
        state = self.inspect(declined)
        assert state["status"] == "completed"
        assert state["output"] == {"invoice_id": declined, "status": "declined"}
        assert self.fixture.requests_for(declined) == []
        results.append({"case": "declined approval", "invoice_requests": 0, "effects": 0})

        # Two SDKs replay one schedule definition; its occurrence suspends for pages.
        schedule_id, source = "daily-" + case_id, "source-" + case_id
        due = int(time.time() * 1000) - 1000
        schedule_args = ("schedule", "--id", schedule_id, "--source", source, "--pages", "2",
                         "--interval-ms", "86400000", "--first-due-ms", str(due))
        self.cli(self.first, *schedule_args)
        self.cli(self.second, *schedule_args)
        self.work_once(self.first)
        schedule = self.cli(self.second, "inspect", "--id", schedule_id, "--schedule")
        parent_id = schedule["active_task"]
        assert parent_id and schedule["next_due_ms"] == due + 86400000
        parent = self.inspect(parent_id)
        assert parent["status"] == "waiting" and not self.fixture.effects_for(source)
        children = parent["waiting_on"]["ids"]
        assert len(children) == len(set(children)) == 2
        assert all(self.inspect(child)["status"] == "queued" for child in children)
        self.restart_server()
        for child in children:
            self.work_once(self.second)
        assert all(self.inspect(child)["status"] == "completed" for child in children)
        assert not self.fixture.effects_for(source), "published batch before parent resumed its join"
        self.work_once(self.second)
        parent = self.inspect(parent_id)
        assert parent["status"] == "completed" and parent["output"] == {"source": source, "record_count": 2}
        records = self.fixture.effects_for(source)[0]["body"]["records"]
        assert records == [{"source": source, "page": page, "value": page * 10} for page in (1, 2)]
        self.cli(self.first, *schedule_args)
        assert self.work_once(self.first) == {"worked": False}
        assert self.work_once(self.second) == {"worked": False}
        with self.fixture.lock:
            gets = collections.Counter(request["path"] for request in self.fixture.requests
                                       if request["path"].startswith(f"/imports/{source}/"))
        assert gets == {f"/imports/{source}/pages/1": 1, f"/imports/{source}/pages/2": 1}
        assert len(self.fixture.effects_for(source)) == 1
        results.append({"case": "scheduled import and waiting join", "occurrences": 1,
                        "pages_fetched": 2, "batch_effects": 1, "records": records})

        # The fixture commits the effect but holds its response, so no SDK checkpoint
        # can be written before the process is killed. Its ledger outlives that worker.
        event = "crash-" + case_id
        self.fixture.hold_event = event
        self.fixture.effect_committed.clear()
        self.fixture.release_response.clear()
        self.cli(self.first, "submit", "webhook", "--id", event, "--event-id", event)
        worker = subprocess.Popen(self.argv(self.first, "work", "--once"), cwd=self.work,
                                  env=self.worker_env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.processes.append(worker)
        assert self.fixture.effect_committed.wait(8), "external HTTP effect was never committed"
        assert worker.poll() is None
        crashed = self.inspect(event)
        assert crashed["status"] == "running" and "deliver" not in crashed["steps"]
        assert len(self.fixture.effects_for(event)) == 1
        stop(worker)
        self.fixture.hold_event = None
        self.fixture.release_response.set()
        self.restart_server()
        wait(lambda: int(time.time() * 1000) > self.inspect(event)["expires_at"],
             timeout=int(self.env["LEASE_MS"]) / 1000 + 15)
        self.work_once(self.second)
        completed = self.inspect(event)
        requests = self.fixture.requests_for(event)
        assert completed["status"] == "completed" and completed["attempts"] == 2
        assert "deliver" in completed["steps"]
        assert len(requests) == 2 and len({request["key"] for request in requests}) == 1
        assert len(self.fixture.effects_for(event)) == 1
        results.append({"case": "crash after HTTP effect before checkpoint",
                        "first_language": self.first, "replacement_language": self.second,
                        "checkpoint_before_crash": False, "requests": 2, "effects": 1,
                        "same_idempotency_key": True})
        return {"mode": self.mode, "first_language": self.first,
                "replacement_language": self.second, "passed": results}

    def close(self):
        self.fixture.release_response.set()
        for process in reversed(self.processes):
            stop(process)


def check_occupied_prefix_preserved():
    """Inject existing external data into the actual admission/cleanup path."""
    from unittest.mock import patch

    prefix = "use-cases-" + "0" * 32
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
    report_path = ROOT.parent / "outputs/evidence" / f"use-cases-{backend}.json"
    # Native overrides were removed above, so these are the exact defaults chosen
    # by deoos.native.NativeEngine and the TypeScript SDK's createRequire loader.
    python_native_name = {"Darwin": "libdeoos_engine.dylib", "Linux": "libdeoos_engine.so",
                          "Windows": "deoos_engine.dll"}[platform.system()]
    artifacts = [SERVER, ROOT / "examples/use_cases.py", ROOT / "examples/use_cases.mjs",
                 ROOT / "clients/python/deoos/__init__.py", ROOT / "clients/python/deoos/native.py",
                 ROOT / "clients/python/deoos/native" / python_native_name,
                 ROOT / "clients/typescript/dist/index.js",
                 ROOT / "clients/typescript/dist/native/deoos_node.node"]
    report = {"backend": "local RustFS" if backend == "rustfs" else backend,
              "provider": env["DEOOS_STORAGE_PROVIDER"], "bucket": bucket,
              "candidate_prefix": prefix, "prefix_owned": False, "prefix_preflight": "pending",
              "external_target": backend != "rustfs",
              "cleanup_scope": "generated local bucket" if backend == "rustfs" else "owned-prefix live objects",
              "custom_endpoint": bool(env.get("AWS_ENDPOINT") or env.get("AZURE_STORAGE_ENDPOINT")),
              "lease_ms": int(env["LEASE_MS"]),
              "started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "integration": "simulated HTTP service; in-memory external idempotency ledger",
              "python": sys.version.split()[0],
              "node": subprocess.check_output([shutil.which("node"), "--version"], text=True).strip(),
              "artifacts": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                            for path in artifacts},
              "cases": [], "success": False, "cleaned": False, "cleanup_errors": []}
    if backend == "rustfs":
        report["endpoint"] = env["AWS_ENDPOINT"]
    storage, fixture, case = None, None, None
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
        write_report(report, report_path)
        with tempfile.TemporaryDirectory(prefix="deoos-use-cases-") as scratch:
            work = pathlib.Path(scratch)
            for filename in ("use_cases.py", "use_cases.mjs"):
                shutil.copy2(ROOT / "examples" / filename, work / filename)
            (work / "node_modules").mkdir()
            (work / "node_modules/deoos").symlink_to(ROOT / "clients/typescript", target_is_directory=True)
            fixture = IntegrationFixture()
            for mode in ("library", "server"):
                for first, second in (("python", "typescript"), ("typescript", "python")):
                    case = WorkflowCase(mode, first, second, env, work, fixture)
                    try:
                        report["cases"].append(case.run())
                        write_report(report, report_path)
                        print(f"passed {mode}: {first} -> {second}", flush=True)
                    finally:
                        case.close()
                        case = None
        report["success"] = True
    except BaseException as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        for label, cleanup in (("workers and server", lambda: case.close() if case else None),
                               ("HTTP fixture", lambda: fixture.close() if fixture else None)):
            try:
                cleanup()
            except Exception as error:
                report["cleanup_errors"].append({"stage": label, "error_type": type(error).__name__})
        if storage is not None and prefix_owned:
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
            report["cleanup_skipped_reason"] = "namespace_ownership_not_acquired"
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
    parser.add_argument("--serve", action="store_true", help="run only the simulated integration service")
    parser.add_argument("--port", type=int, default=18080, help="loopback fixture port (default: 18080)")
    parser.add_argument("--backend", choices=("rustfs", "s3", "r2", "gcs", "azure"), default="rustfs")
    parser.add_argument("--bucket", help="precreated dedicated external bucket/container; never created or deleted")
    args = parser.parse_args()
    if args.serve:
        fixture = IntegrationFixture(args.port)
        print(f"Simulated integration service at {fixture.url}; idempotency ledger is in memory.", flush=True)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            fixture.close()
    else:
        try:
            main(args.backend, args.bucket)
        except BaseException as error:
            print(f"acceptance failed: {type(error).__name__}", file=sys.stderr)
            raise SystemExit(1) from None

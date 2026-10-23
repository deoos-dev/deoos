"""Approve, then charge once. The crash flag is in this process.

Leave ``serve`` running in one terminal. In another, with storage configured:

    python examples/charge.py submit --id pay-001
    python examples/charge.py work --id pay-001
    python -m deoos explain pay-001
    python examples/charge.py approve --id pay-001
    python examples/charge.py work --id pay-001 --crash-before-commit charge
    python examples/charge.py work --id pay-001
    python examples/charge.py effects

Exit code 86 means this process stopped after the sink accepted the charge
and before that step committed. Run ``work`` again without the flag. If the
dead process still holds the lease, that ``work`` waits for it to expire
(30 seconds unless ``LEASE_MS`` is set). The sink keeps one charge because
the idempotency key is the same on resume.

``approve --decline`` runs a void step and does not charge. ``--hold-ms`` is a
durable timer before the signal; the default is 0. Cancellation does not undo
a charge that already left this process. A stale provider key, after the
provider has forgotten an uncommitted call, still needs a reconciliation read.
This example stays inside the sink's memory, which lasts as long as ``serve``.
"""
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import urllib.error
import urllib.request

from deoos import Client

HANDLER = "charge.approve.v1"
DEFAULT_URL = "http://127.0.0.1:18181"


def show(value):
    print(json.dumps(value, indent=2, sort_keys=True))


def create_client():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        if not os.environ.get("ENGINE_URL"):
            raise ValueError("DEOOS_MODE=server requires ENGINE_URL")
        return Client.remote(os.environ["ENGINE_URL"], token=os.environ.get("ENGINE_TOKEN"))
    if mode != "library":
        raise ValueError("DEOOS_MODE must be 'library' or 'server'")
    provider = os.environ.get("DEOOS_STORAGE_PROVIDER", "s3")
    if provider == "filesystem":
        return Client(provider=provider, directory=os.environ["DEOOS_STORAGE_DIRECTORY"],
                      prefix=os.environ.get("EXECUTION_PREFIX", "deoos"))
    bucket = os.environ.get("DEOOS_STORAGE_BUCKET") or os.environ.get("AWS_BUCKET")
    if not bucket:
        raise ValueError("DEOOS_STORAGE_BUCKET is required in library mode")
    return Client(provider=provider, bucket=bucket, prefix=os.environ.get("EXECUTION_PREFIX", "deoos"))


def post(service_url, path, key, body):
    payload = {**body, "idempotency_key": key}
    request = urllib.request.Request(
        service_url + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": key})
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if not isinstance(result, dict) or "id" not in result:
        raise ValueError("sink response must be an object with an id")
    return result


def charge(ctx, inputs):
    ctx.sleep("hold", inputs["hold_ms"])
    approval = ctx.wait_signal("approval")
    if not isinstance(approval, dict) or not isinstance(approval.get("approved"), bool):
        raise ValueError("approval signal must contain a boolean 'approved' field")
    amount = {"amount_cents": inputs["amount_cents"]}
    if not approval["approved"]:
        return ctx.step("void", lambda: post(inputs["service_url"], "/void",
                                              ctx.idempotency_key("void"), amount))

    def do_charge():
        result = post(inputs["service_url"], "/charge", ctx.idempotency_key("charge"), amount)
        if os.environ.get("DEOOS_CRASH_BEFORE_COMMIT") == "charge":
            print("crash before commit of charge; the sink already accepted it", flush=True)
            os._exit(86)
        return result

    return ctx.step("charge", do_charge)


HANDLERS = {HANDLER: charge}


class Ledger:
    def __init__(self):
        self.lock = threading.Lock()
        self.rows = []
        self.by_key = {}

    def apply(self, path, key, body):
        with self.lock:
            existing = self.by_key.get((path, key))
            if existing is not None:
                if existing["body"] != body:
                    return 409, {"error": "idempotency key reused with a different body"}
                return 200, existing["result"]
            result = {"id": f"{path.strip('/')}-{len(self.rows) + 1}",
                      "amount_cents": body.get("amount_cents")}
            row = {"path": path, "key": key, "body": body, "result": result}
            self.rows.append(row)
            self.by_key[(path, key)] = row
            return 200, result

    def snapshot(self):
        with self.lock:
            return list(self.rows)


def serve(port):
    ledger = Ledger()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def _read(self):
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length).decode() or "{}")

        def _send(self, status, payload):
            raw = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            if self.path != "/effects":
                self._send(404, {"error": "not found"})
                return
            self._send(200, {"effects": ledger.snapshot()})

        def do_POST(self):
            if self.path not in ("/charge", "/void"):
                self._send(404, {"error": "not found"})
                return
            body = self._read()
            key = self.headers.get("Idempotency-Key")
            if not key or key != body.get("idempotency_key"):
                self._send(400, {"error": "Idempotency-Key must match the body"})
                return
            status, payload = ledger.apply(self.path, key, body)
            self._send(status, payload)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"charge sink at http://127.0.0.1:{port}  (GET /effects)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def effects(url):
    with urllib.request.urlopen(url.rstrip("/") + "/effects", timeout=5) as response:
        show(json.load(response))


def run_until(client, task_id):
    announced = False
    while True:
        try:
            worked = client.run_once(HANDLERS)
        except Exception as error:
            print(str(error), flush=True)
            view = client.view(task_id)
            if view["status"] in ("failed", "cancelled"):
                show(view)
                return 1
            time.sleep(0.2)
            continue
        view = client.view(task_id)
        wait = view.get("wait") or {}
        if view["status"] in ("completed", "failed", "cancelled"):
            show(view)
            return 0 if view["status"] == "completed" else 1
        if (view["status"] == "waiting" and wait.get("kind") == "signal"
                and not wait.get("assigned")):
            show(view)
            print("Waiting for approval. Run: python examples/charge.py approve --id "
                  + task_id, flush=True)
            return 0
        if worked:
            continue
        if view["status"] == "running" and not announced:
            print("Task is still leased. Waiting for the lease to expire.", flush=True)
            announced = True
        if view["status"] in ("running", "queued", "waiting"):
            time.sleep(0.5)
            continue
        show(view)
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    sink = commands.add_parser("serve", help="run the local charge/void sink")
    sink.add_argument("--port", type=int, default=18181)
    submit = commands.add_parser("submit", help="submit one charge workflow")
    submit.add_argument("--id", required=True)
    submit.add_argument("--amount-cents", type=int, default=2500)
    submit.add_argument("--hold-ms", type=int, default=0)
    submit.add_argument("--service-url", default=DEFAULT_URL)
    work = commands.add_parser("work", help="run until the approval wait, a crash, or completion")
    work.add_argument("--id", required=True)
    work.add_argument("--crash-before-commit", choices=("charge",),
                      help="exit 86 after the charge call returns and before its checkpoint")
    approve = commands.add_parser("approve", help="send the approval signal")
    approve.add_argument("--id", required=True)
    approve.add_argument("--decline", action="store_true")
    commands.add_parser("effects", help="print the sink ledger").add_argument(
        "--service-url", default=DEFAULT_URL)
    view = commands.add_parser("view", help="print the public task view")
    view.add_argument("--id", required=True)
    args = parser.parse_args()
    if args.command == "serve":
        serve(args.port)
        return 0
    if args.command == "effects":
        try:
            effects(args.service_url)
        except urllib.error.URLError as error:
            print(f"sink is not running at {args.service_url}: {error}", flush=True)
            return 1
        return 0
    if args.command == "work" and args.crash_before_commit:
        os.environ["DEOOS_CRASH_BEFORE_COMMIT"] = args.crash_before_commit
    else:
        os.environ.pop("DEOOS_CRASH_BEFORE_COMMIT", None)
    with create_client() as client:
        if args.command == "submit":
            if args.amount_cents < 1 or args.hold_ms < 0:
                raise ValueError("amount-cents must be >= 1 and hold-ms must be >= 0")
            client.submit(args.id, HANDLER, {
                "service_url": args.service_url.rstrip("/"),
                "amount_cents": args.amount_cents,
                "hold_ms": args.hold_ms,
            }, max_attempts=3, retry_ms=200)
            show(client.view(args.id))
            return 0
        if args.command == "approve":
            client.signal(args.id, "approval", {"approved": not args.decline})
            show(client.view(args.id))
            return 0
        if args.command == "view":
            show(client.view(args.id))
            return 0
        return run_until(client, args.id)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(str(error))
        raise SystemExit(1) from None

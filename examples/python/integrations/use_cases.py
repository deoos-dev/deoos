"""Runnable business workflows against the local example service, not live integrations."""
import argparse
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid

from deoos import Client


def valid_id(value):
    if (not isinstance(value, str) or not 1 <= len(value) <= 128
            or value in (".", "..") or not value.isascii()
            or any(not (char.isalnum() or char in "_.-") for char in value)):
        raise ValueError("ID must use 1-128 ASCII letters, digits, dot, underscore, or hyphen")
    return value


def safe_integer(value, name, minimum=0, maximum=9_007_199_254_740_991):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer from {minimum} to {maximum}")
    return value


def service_url(value):
    message = "service_url must be an HTTP(S) base URL without credentials, query, or fragment"
    if (not isinstance(value, str) or not value.lower().startswith(("http://", "https://")) or not value.isascii()
            or any(char.isspace() or ord(char) < 32 for char in value)
            or any(char in value for char in "\\?#")):
        raise ValueError(message)
    try:
        parsed = urllib.parse.urlsplit(value)
        parsed.port
    except ValueError:
        raise ValueError(message) from None
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment):
        raise ValueError(message)
    return value.rstrip("/")


def json_data(value, maximum_bytes, maximum_depth=20):
    # Shared conservative budget: 32 per number, 6 per UTF-16 string unit,
    # plus JSON punctuation. Admission does not depend on float formatting.
    def check(item, depth=0):
        if depth > maximum_depth:
            raise ValueError(f"JSON data must have at most {maximum_depth} nesting levels")
        if item is None:
            return 4
        if isinstance(item, bool):
            return 5
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            if not math.isfinite(item) or (item == int(item) and abs(item) > 9_007_199_254_740_991):
                raise ValueError("JSON numbers must be finite; integer values must be safe integers")
            return 32
        if isinstance(item, str):
            return 2 + 6 * sum(2 if ord(char) > 0xffff else 1 for char in item)
        if isinstance(item, dict):
            return 2 + max(0, len(item) - 1) + sum(
                check(key, depth) + 1 + check(child, depth + 1) for key, child in item.items()
            )
        if isinstance(item, list):
            return 2 + max(0, len(item) - 1) + sum(check(child, depth + 1) for child in item)
        raise ValueError("JSON data contains an unsupported value")
    if check(value) > maximum_bytes:
        raise ValueError(f"JSON data exceeds the conservative {maximum_bytes}-byte budget")
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()


def validate(case, inputs):
    if not isinstance(inputs, dict):
        raise ValueError("inputs must be an object")
    service_url(inputs.get("service_url"))
    if case == "webhook":
        valid_id(inputs.get("event_id"))
        if not isinstance(inputs.get("payload"), dict):
            raise ValueError("payload must be an object")
        json_data(inputs["payload"], 65_536, maximum_depth=19)  # Reserve the HTTP envelope level.
    elif case == "invoice":
        valid_id(inputs.get("invoice_id"))
        valid_id(inputs.get("customer_id"))
        safe_integer(inputs.get("amount_cents"), "amount_cents", 1)
    elif case == "import":
        valid_id(inputs.get("source"))
        safe_integer(inputs.get("page_count"), "page_count", 1, 10)
    elif case == "page":
        valid_id(inputs.get("source"))
        safe_integer(inputs.get("page"), "page", 1, 10)
    return inputs


def http_json(base, path, body=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    data = None if body is None else json_data(body, 1_048_576)
    request = urllib.request.Request(service_url(base) + path, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        raw = response.read(1_048_577)
        if len(raw) > 1_048_576:
            raise ValueError("service response must fit in 1048576 bytes")
        value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("service response must be an object")
    json_data(value, 1_048_576)
    return value


# Keep these handler IDs, operation names, and revision 1 compatible with active tasks.
# Changed business semantics should use a new handler version and fresh execution IDs.
def webhook(ctx, inputs):
    validate("webhook", inputs)
    return ctx.step("deliver", lambda: http_json(
        inputs["service_url"], "/webhooks",
        {"event_id": inputs["event_id"], "payload": inputs["payload"]},
        ctx.idempotency_key("deliver"),
    ), revision="1")


def invoice(ctx, inputs):
    validate("invoice", inputs)
    approval = ctx.wait_signal("approval")
    if not isinstance(approval, dict) or not isinstance(approval.get("approved"), bool):
        raise ValueError("approval signal must contain a boolean 'approved' field")
    if not approval["approved"]:
        return {"invoice_id": inputs["invoice_id"], "status": "declined"}
    return ctx.step("issue", lambda: http_json(
        inputs["service_url"], "/invoices",
        {"invoice_id": inputs["invoice_id"], "customer_id": inputs["customer_id"],
         "amount_cents": inputs["amount_cents"]},
        ctx.idempotency_key("issue"),
    ), revision="1")


def import_page(ctx, inputs):
    validate("page", inputs)

    def fetch():
        result = http_json(inputs["service_url"], f'/imports/{inputs["source"]}/pages/{inputs["page"]}')
        records = result.get("records")
        if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
            raise ValueError("page response must contain a records array of objects")
        return records

    return ctx.step("fetch", fetch, revision="1")


def daily_import(ctx, inputs):
    validate("import", inputs)
    children = [ctx.spawn(
        f"page-{page}", "usecase.import-page.v1",
        {"service_url": inputs["service_url"], "source": inputs["source"], "page": page},
        max_attempts=3, retry_ms=1000,
    ) for page in range(1, inputs["page_count"] + 1)]
    pages = ctx.join("pages", children)
    records = [record for page in pages for record in page]
    return ctx.step("publish", lambda: http_json(
        inputs["service_url"], "/batches", {"source": inputs["source"], "records": records},
        ctx.idempotency_key("publish"),
    ), revision="1")


HANDLERS = {
    "usecase.webhook.v1": webhook,
    "usecase.invoice.v1": invoice,
    "usecase.daily-import.v1": daily_import,
    "usecase.import-page.v1": import_page,
}
CASE_HANDLERS = {"webhook": "usecase.webhook.v1", "invoice": "usecase.invoice.v1",
                 "import": "usecase.daily-import.v1"}


def create_client():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        if not os.environ.get("ENGINE_URL"):
            raise ValueError("ENGINE_URL is required when DEOOS_MODE=server")
        return Client.remote(os.environ["ENGINE_URL"], token=os.environ.get("ENGINE_TOKEN"))
    if mode == "library":
        provider = os.environ.get("DEOOS_STORAGE_PROVIDER", "s3")
        bucket = os.environ.get("DEOOS_STORAGE_BUCKET") or (
            os.environ.get("AWS_BUCKET") if provider == "s3" else None)
        if not bucket:
            raise ValueError("DEOOS_STORAGE_BUCKET is required")
        config = {"bucket": bucket, "provider": provider,
                  "prefix": os.environ.get("EXECUTION_PREFIX", "deoos")}
        if provider == "s3":
            config["region"] = os.environ.get("AWS_REGION")
        return Client(**config)
    raise ValueError("DEOOS_MODE must be 'library' or 'server'")


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    submit = actions.add_parser("submit")
    submit.add_argument("case", choices=CASE_HANDLERS)
    submit.add_argument("--id")
    submit.add_argument("--event-id", default="event-demo")
    submit.add_argument("--payload", default='{"type":"demo.created"}')
    submit.add_argument("--invoice-id", default="invoice-demo")
    submit.add_argument("--customer-id", default="customer-demo")
    submit.add_argument("--amount-cents", type=int, default=2500)
    submit.add_argument("--source", default="demo")
    submit.add_argument("--pages", type=int, default=2)
    schedule = actions.add_parser("schedule", help="create an immutable fixed-interval import schedule")
    schedule.add_argument("--id", required=True)
    schedule.add_argument("--source", default="demo")
    schedule.add_argument("--pages", type=int, default=2)
    schedule.add_argument("--interval-ms", type=int, default=86_400_000)
    schedule.add_argument("--first-due-ms", type=int)
    work = actions.add_parser("work")
    work.add_argument("--once", action="store_true")
    signal = actions.add_parser("signal", help="approve an invoice, or pass --decline")
    signal.add_argument("--id", required=True)
    signal.add_argument("--decline", action="store_true")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--id", required=True)
    inspect.add_argument("--schedule", action="store_true")
    return parser


def main():
    args = make_parser().parse_args()
    client = None
    try:
        inputs = None
        if args.action in ("submit", "schedule"):
            case = args.case if args.action == "submit" else "import"
            inputs = {"service_url": service_url(os.environ.get("SERVICE_URL"))}
            if case == "webhook":
                inputs.update(event_id=args.event_id, payload=json.loads(args.payload))
            elif case == "invoice":
                inputs.update(invoice_id=args.invoice_id, customer_id=args.customer_id,
                              amount_cents=args.amount_cents)
            else:
                inputs.update(source=args.source, page_count=args.pages)
            validate(case, inputs)
        client = create_client()
        if args.action == "submit":
            result = client.submit(valid_id(args.id or args.case + "-" + uuid.uuid4().hex),
                                   CASE_HANDLERS[args.case], inputs, max_attempts=3, retry_ms=1000)
        elif args.action == "schedule":
            result = client.schedule(valid_id(args.id), CASE_HANDLERS["import"], inputs,
                                     safe_integer(args.interval_ms, "interval_ms", 1),
                                     first_due_ms=args.first_due_ms, missed="latest", overlap="skip",
                                     max_attempts=3, retry_ms=1000)
        elif args.action == "signal":
            result = client.signal(valid_id(args.id), "approval", {"approved": not args.decline})
        elif args.action == "inspect":
            result = (client.inspect_schedule if args.schedule else client.inspect)(valid_id(args.id))
        elif args.once:
            result = {"worked": client.run_once(HANDLERS)}
        else:
            result = None
            try:
                while True:
                    try:
                        worked = client.run_once(HANDLERS)
                    except Exception as error:
                        print(str(error), file=sys.stderr)
                        time.sleep(1)
                        continue
                    if not worked:
                        time.sleep(1)
            except KeyboardInterrupt:
                pass
        if result is not None:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())

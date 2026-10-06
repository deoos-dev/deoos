"""Cross-language durable order demo; no payment or external service is called."""
import argparse
import json
import os
import signal
import sys
import threading
import uuid

from deoos import Client

MAX_SAFE_INTEGER = 9_007_199_254_740_991


def safe_integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= MAX_SAFE_INTEGER:
        raise ValueError(f"{name} must be a safe integer >= {minimum}")
    return value


def valid_id(value):
    if (not isinstance(value, str) or not value or len(value) > 128
            or value in (".", "..")
            or any(not (char.isascii() and (char.isalnum() or char in "_.-")) for char in value)):
        raise ValueError("ID must use 1-128 ASCII letters, digits, dot, underscore, or hyphen")
    return value


def create_client():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        url = os.environ.get("ENGINE_URL")
        if not url:
            raise ValueError("ENGINE_URL is required when DEOOS_MODE=server")
        return Client.remote(url, token=os.environ.get("ENGINE_TOKEN"))
    if mode == "library":
        provider = os.environ.get("DEOOS_STORAGE_PROVIDER", "s3")
        if provider == "filesystem":
            return Client(provider=provider, directory=os.environ.get("DEOOS_STORAGE_DIRECTORY"),
                          prefix=os.environ.get("EXECUTION_PREFIX", "durable-v3"))
        if provider not in ("s3", "gcs", "azure"):
            raise ValueError("DEOOS_STORAGE_PROVIDER must be 's3', 'gcs', 'azure', or 'filesystem'")
        bucket = os.environ.get("DEOOS_STORAGE_BUCKET") or (
            os.environ.get("AWS_BUCKET") if provider == "s3" else None)
        if not bucket:
            raise ValueError("DEOOS_STORAGE_BUCKET is required in library mode (AWS_BUCKET is an S3 fallback)")
        return Client(provider=provider, bucket=bucket, prefix=os.environ.get("EXECUTION_PREFIX", "durable-v3"))
    raise ValueError("DEOOS_MODE must be 'library' or 'server'")


def validate(ctx, inputs):
    def check():
        quantity = safe_integer(inputs["quantity"], "quantity", 1)
        unit_price = safe_integer(inputs["unit_price"], "unit_price")
        safe_integer(inputs["delay_ms"], "delay_ms")
        if not (quantity * unit_price <= MAX_SAFE_INTEGER):
            raise ValueError("order total exceeds the safe integer range")
        return {"valid": True}

    return ctx.step("check", check)


def price(ctx, inputs):
    def compute():
        total = inputs["quantity"] * inputs["unit_price"]
        return safe_integer(total, "total")

    return ctx.step("compute", compute)


def order(ctx, inputs):
    validation_id = ctx.spawn("validate", "demo.validate.v1", inputs)
    price_id = ctx.spawn("price", "demo.price.v1", inputs)
    results = ctx.join("ready", [validation_id, price_id])
    if not results[0]["valid"]:
        raise ValueError("order validation failed")
    quote = results[1]
    ctx.sleep("cooldown", inputs["delay_ms"])
    approval = ctx.wait_signal("approved")
    if not isinstance(approval, dict) or not isinstance(approval.get("approved"), bool):
        raise ValueError("approval signal must contain a boolean 'approved' field")
    return ctx.step("finalize", lambda: {
        "order_id": ctx.task["id"], "total": quote, "approved": approval["approved"],
    })


HANDLERS = {
    "demo.order.v1": order,
    "demo.price.v1": price,
    "demo.validate.v1": validate,
}


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    submit = actions.add_parser("submit", help="submit a new order")
    submit.add_argument("--id")
    submit.add_argument("--quantity", type=int, default=2)
    submit.add_argument("--unit-price", type=int, default=25)
    submit.add_argument("--delay-ms", type=int, default=1000)
    work = actions.add_parser("work", help="poll and run order tasks")
    work.add_argument("--once", action="store_true", help="poll once and exit")
    approve = actions.add_parser("approve", help="approve an order (or pass --decline)")
    approve.add_argument("--id", required=True)
    approve.add_argument("--decline", action="store_true")
    inspect = actions.add_parser("inspect", help="inspect an order task")
    inspect.add_argument("--id", required=True)
    return parser


def main():
    args = make_parser().parse_args()
    client = None
    try:
        client = create_client()
        if args.action == "submit":
            task_id = valid_id(args.id or "order-" + uuid.uuid4().hex)
            inputs = {
                "quantity": safe_integer(args.quantity, "quantity", 1),
                "unit_price": safe_integer(args.unit_price, "unit_price"),
                "delay_ms": safe_integer(args.delay_ms, "delay_ms"),
            }
            result = client.submit(task_id, "demo.order.v1", inputs)
        elif args.action == "approve":
            task_id = valid_id(args.id)
            result = client.signal(task_id, "approved", {"approved": not args.decline})
        elif args.action == "inspect":
            result = client.inspect(valid_id(args.id))
        elif args.once:
            result = {"worked": client.run_once(HANDLERS)}
        else:
            result = None
            stopping = threading.Event()

            def stop(_signum, _frame):
                stopping.set()

            def worker_error(error, task_id):
                if task_id is None:
                    return "propagate"
                print(f"Task {task_id} failed: {error}", file=sys.stderr)
                return "continue"

            previous = {name: signal.signal(name, stop) for name in (signal.SIGINT, signal.SIGTERM)}
            try:
                client.run_worker(HANDLERS, stop_event=stopping, poll_interval=1,
                                  on_error=worker_error)
            finally:
                for name, handler in previous.items():
                    signal.signal(name, handler)
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

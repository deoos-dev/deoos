"""Send one nightly batch of order emails, and skip the batches already saved.

The task id is nightly-<date>. Submitting that id again attaches to the same
task. Each email uses ctx.idempotency_key, so a batch killed mid-send retries
with the same keys.
"""
import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from deoos import Client, EngineError

HANDLER = "nightly.batch.v1"
ORDER_COUNT = 10_000
BATCH_SIZE = 100
BATCHES = ORDER_COUNT // BATCH_SIZE


def task_id_for(date):
    if not isinstance(date, str) or len(date) != 10 or date[4] != "-" or date[7] != "-":
        raise ValueError("date must be YYYY-MM-DD")
    if not date.replace("-", "").isdigit():
        raise ValueError("date must be YYYY-MM-DD")
    return f"nightly-{date}"


def batch_name(number):
    if not 1 <= number <= BATCHES:
        raise ValueError(f"batch number must be 1..{BATCHES}")
    return f"batch-{number:03d}"


def mark_ran(counter_dir, name):
    """Count one real execution of a batch. A saved step does not call this."""
    directory = Path(counter_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    count = int(path.read_text()) if path.exists() else 0
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, str(count + 1).encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def load_orders(path):
    orders = []
    for line in Path(path).read_text().splitlines():
        if not line:
            continue
        order = json.loads(line)
        if not isinstance(order.get("id"), str) or not isinstance(order.get("email"), str):
            raise ValueError("each order needs a string id and email")
        orders.append({"id": order["id"], "email": order["email"]})
    if len(orders) != ORDER_COUNT:
        raise ValueError(f"expected {ORDER_COUNT} orders, found {len(orders)}")
    if len({order["id"] for order in orders}) != ORDER_COUNT:
        raise ValueError("order ids are not unique")
    return orders


def post_json(url, key, body, timeout=120):
    payload = json.dumps(body).encode()
    request = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.load(response)
    if not isinstance(result, dict):
        raise ValueError("mailer response must be an object")
    return result


def nightly(ctx, inputs):
    orders = ctx.step("load_orders", lambda: load_orders(inputs["orders_path"]))
    sent = 0
    for number in range(1, BATCHES + 1):
        name = batch_name(number)
        chunk = orders[(number - 1) * BATCH_SIZE:number * BATCH_SIZE]

        def send(name=name, chunk=chunk, number=number):
            mark_ran(inputs["counter_dir"], name)
            for order in chunk:
                key = ctx.idempotency_key(f"email-{order['id']}")
                post_json(inputs["mailer_url"].rstrip("/") + "/send", key, {
                    "order_id": order["id"],
                    "email": order["email"],
                    "idempotency_key": key,
                })
            return {"batch": number, "sent": len(chunk)}

        sent += ctx.step(name, send)["sent"]
    return ctx.step("summary", lambda: {
        "batches": BATCHES,
        "date": inputs["date"],
        "orders": sent,
        "task_id": ctx.task["id"],
    })


def _pause(ready_path, name):
    path = Path(ready_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, name.encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    while True:
        time.sleep(3600)


def run_worker(config):
    """Claim the nightly task. Optional pauses let a demo kill the process."""
    pause_after = config.get("pause_after")
    pause_inside = config.get("pause_inside")
    ready_path = config.get("ready_path")

    def handler(ctx, inputs):
        if ctx.task["id"] != config["task_id"]:
            raise RuntimeError(f"claimed unexpected task {ctx.task['id']}")
        original = ctx.step

        def step(name, function, revision="1"):
            def execute():
                result = function()
                if name == pause_inside:
                    _pause(ready_path, name)
                return result

            result = original(name, execute, revision)
            if name == pause_after:
                _pause(ready_path, name)
            return result

        if pause_after or pause_inside:
            ctx.step = step
        return nightly(ctx, inputs)

    client = Client(**config["client"])
    try:
        deadline = time.monotonic() + int(config.get("timeout_seconds", 300))
        while time.monotonic() < deadline:
            try:
                state = client.inspect(config["task_id"])
            except EngineError as error:
                if error.status != 404:
                    raise
                time.sleep(0.1)
                continue
            if state["status"] == "completed":
                return 0
            if state["status"] in ("failed", "cancelled"):
                print(state.get("error") or state["status"], file=sys.stderr)
                return 1
            try:
                client.run_once({HANDLER: handler})
            except Exception as error:
                print(error, file=sys.stderr)
                return 1
            time.sleep(0.05)
        print("worker timed out", file=sys.stderr)
        return 1
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["work"])
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    return run_worker(json.loads(args.config.read_text()))


if __name__ == "__main__":
    raise SystemExit(main())

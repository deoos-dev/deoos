"""Approve an invoice, transfer once, and record which approval paid it.

intake reads a local invoice. propose_transfer applies the local rules and
does not call a model. The worker then waits for an approval signal. The
transfer uses ctx.idempotency_key("transfer") and carries the approval id.
reconcile writes the row that maps that approval to the transfer.
"""
import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from deoos import Client, EngineError

HANDLER = "approval.ledger.v1"
MAX_CENTS = 1_000_000


def mark_ran(counter_dir, name):
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


def read_invoice(path):
    invoice = json.loads(Path(path).read_text())
    if not isinstance(invoice, dict):
        raise ValueError("invoice file must contain an object")
    return invoice


def propose_transfer(invoice):
    invoice_id = invoice.get("invoice_id")
    payee = invoice.get("payee")
    currency = invoice.get("currency")
    amount = invoice.get("amount_cents")
    if not isinstance(invoice_id, str) or not invoice_id:
        raise ValueError("invoice_id must be a non-empty string")
    if not isinstance(payee, str) or not payee.strip() or "\n" in payee:
        raise ValueError("payee must be a single non-empty line")
    if currency != "USD":
        raise ValueError("currency must be USD")
    if isinstance(amount, bool) or not isinstance(amount, int) or not 1 <= amount <= MAX_CENTS:
        raise ValueError(f"amount_cents must be an integer from 1 to {MAX_CENTS}")
    return {
        "amount_cents": amount,
        "currency": currency,
        "invoice_id": invoice_id,
        "payee": payee,
        "rule": "amount within the local limit",
    }


def require_approval(value):
    if not isinstance(value, dict):
        raise ValueError("approval signal must be an object")
    approval_id = value.get("approval_id")
    approver = value.get("approver")
    note = value.get("note")
    approved = value.get("approved")
    if not isinstance(approval_id, str) or not approval_id:
        raise ValueError("approval_id must be a non-empty string")
    if not isinstance(approver, str) or not approver:
        raise ValueError("approver must be a non-empty string")
    if not isinstance(note, str):
        raise ValueError("note must be a string")
    if not isinstance(approved, bool):
        raise ValueError("approved must be a boolean")
    return {
        "approval_id": approval_id,
        "approver": approver,
        "approved": approved,
        "note": note,
    }


def post_json(url, key, body):
    payload = json.dumps(body).encode()
    request = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    if not isinstance(result, dict):
        raise ValueError("bank response must be an object")
    return result


def approval_ledger(ctx, inputs):
    def intake():
        mark_ran(inputs["counter_dir"], "intake")
        return read_invoice(inputs["invoice_path"])

    def propose():
        mark_ran(inputs["counter_dir"], "propose_transfer")
        return propose_transfer(invoice)

    invoice = ctx.step("intake", intake)
    proposal = ctx.step("propose_transfer", propose)
    approval = require_approval(ctx.wait_signal("approval"))
    if not approval["approved"]:
        return {"approval_id": approval["approval_id"], "status": "declined", "task_id": ctx.task["id"]}

    def transfer():
        mark_ran(inputs["counter_dir"], "transfer")
        key = ctx.idempotency_key("transfer")
        body = {
            "amount_cents": proposal["amount_cents"],
            "approval_id": approval["approval_id"],
            "idempotency_key": key,
            "invoice_id": proposal["invoice_id"],
            "payee": proposal["payee"],
        }
        result = post_json(inputs["bank_url"].rstrip("/") + "/transfer", key, body)
        return {"approval_id": result["approval_id"], "idempotency_key": key, "transfer_id": result["transfer_id"]}

    paid = ctx.step("transfer", transfer)

    def reconcile():
        mark_ran(inputs["counter_dir"], "reconcile")
        row = {
            "approval_id": paid["approval_id"],
            "idempotency_key": paid["idempotency_key"],
            "task_id": ctx.task["id"],
            "transfer_id": paid["transfer_id"],
        }
        post_json(inputs["bank_url"].rstrip("/") + "/ledger", paid["idempotency_key"], row)
        return row

    return ctx.step("reconcile", reconcile)


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
        return approval_ledger(ctx, inputs)

    client = Client(**config["client"])
    try:
        deadline = time.monotonic() + int(config.get("timeout_seconds", 180))
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

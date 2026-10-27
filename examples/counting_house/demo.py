"""Run the Counting House refund saga against a local engine and local JSON systems.

Requires a reachable S3-compatible store (the repository's RustFS) and the
deoos Python SDK. The workflow itself does not use the network.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import uuid

import boto3
from botocore.exceptions import ClientError
from deoos import Client
from refund import (
    CUSTOMER_ID, HANDLER, INVOICE_ID, case_inputs, export_runs, refund, send_decision,
)

ROOT = Path(__file__).resolve().parents[2]
STORAGE = dict(provider="s3", endpoint="http://127.0.0.1:19000", allow_http=True,
               region="us-east-1", access_key_id="local-development",
               secret_access_key="local-development-only-secret", lease_ms=5000)


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def refunded_cents(state, invoice_id=INVOICE_ID):
    return sum(entry["amount_cents"] for entry in state["ledger"]["entries"]
               if entry["invoice_id"] == invoice_id and entry["kind"] == "refund" and not entry["reversed"])


def requests_for(state, op):
    return [row for row in state["requests"] if row["op"] == op]


def load_office(path):
    return json.loads((Path(path) / "office.json").read_text())


def tail(path):
    if not path.exists():
        return ""
    lines = path.read_text(errors="replace").splitlines()
    return "\n".join(lines[-40:])


def serve(config):
    client = Client(**config["client"])
    try:
        def handler(ctx, inputs):
            crash_after = config.get("crash_after")
            if not crash_after:
                return refund(ctx, inputs)
            original = ctx.step

            def step(name, function, revision="1"):
                def wrapped():
                    result = function()
                    if name == crash_after:
                        ready = Path(config["ready"])
                        ready.write_text(name)
                        fd = os.open(ready, os.O_RDONLY)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                        time.sleep(3600)
                    return result
                return original(name, wrapped, revision)

            ctx.step = step
            return refund(ctx, inputs)

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            state = client.inspect(config["task_id"])
            if state["status"] == "completed":
                return
            if state["status"] in ("failed", "cancelled"):
                raise RuntimeError(state.get("error") or state["status"])
            client.run_once({HANDLER: handler})
            time.sleep(0.05)
        raise RuntimeError(f"{config['task_id']} did not finish")
    finally:
        client.close()


def spawn(directory, client_config, task_id, crash_after=None, label="worker"):
    ready = directory / f"{task_id}-ready"
    config_path = directory / f"{task_id}-{label}.json"
    config_path.write_text(json.dumps({
        "client": client_config, "task_id": task_id, "crash_after": crash_after, "ready": str(ready),
    }))
    config_path.chmod(0o600)
    log_path = directory / f"{task_id}-{label}.log"
    env = os.environ.copy()
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    env["PYTHONUNBUFFERED"] = "1"
    sdk = ROOT / "clients" / "python"
    if (sdk / "deoos").is_dir():
        env["PYTHONPATH"] = str(sdk) + os.pathsep + env.get("PYTHONPATH", "")
    log = open(log_path, "w")
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--worker", str(config_path)],
        env=env, stdout=log, stderr=subprocess.STDOUT)
    return process, log, log_path, ready


def stop(process, log):
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    if log is not None and not log.closed:
        log.close()


def wait_until(predicate, process, log_path, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            snapshot = predicate()
            if snapshot:
                return snapshot
            raise RuntimeError(f"worker exited {process.returncode}\n{tail(log_path)}")
        snapshot = predicate()
        if snapshot:
            return snapshot
        time.sleep(0.05)
    raise TimeoutError(f"timed out\n{tail(log_path)}")


def wait_exit(process, log_path, timeout=90):
    try:
        code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        raise TimeoutError(f"worker did not exit\n{tail(log_path)}") from None
    check(code == 0, f"worker exited {code}\n{tail(log_path)}")


def outcome(client, task_id):
    view = client.view(task_id)
    check(view["status"] == "completed", f"{task_id} ended {view['status']}: {view.get('error')}")
    return view["output"]


def main(args):
    endpoint = args.endpoint
    run_id = uuid.uuid4().hex[:12]
    directory = Path(args.directory).resolve() / run_id
    directory.mkdir(parents=True)
    bucket = "deoos-ch-" + run_id
    report = {"bucket": bucket, "prefix": "counting-house", "endpoint": endpoint,
              "cases": [], "cleaned": False}
    evidence = directory / "result.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    storage = dict(STORAGE, endpoint=endpoint, bucket=bucket, prefix="counting-house")
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name=STORAGE["region"],
                      aws_access_key_id=STORAGE["access_key_id"],
                      aws_secret_access_key=STORAGE["secret_access_key"])
    created = False
    client = None
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with urllib.request.urlopen(endpoint + "/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            check(time.monotonic() < deadline, "object storage did not become ready")
            time.sleep(0.5)
        s3.create_bucket(Bucket=bucket)
        created = True
        client = Client(**storage)

        def submit(task_id, office, **kwargs):
            client.submit(task_id, HANDLER, case_inputs(office, **kwargs), max_attempts=5)

        def drive(task_id, decision, crash_after=None):
            process, log, log_path, ready = spawn(directory, storage, task_id, crash_after)
            try:
                if decision == "none":
                    wait_exit(process, log_path)
                    return
                def waiting():
                    state = client.inspect(task_id)
                    waiting_on = state.get("waiting_on") or {}
                    if (state["status"] == "waiting" and waiting_on.get("kind") == "signal"
                            and waiting_on.get("name") == "approval"):
                        return state
                    return None
                wait_until(waiting, process, log_path)
                send_decision(client, task_id, decision == "approve", "dana",
                              "Duplicate charge matches the refund policy." if decision == "approve"
                              else "Customer already received a credit.")
                if crash_after is None:
                    wait_exit(process, log_path)
                    return
                def gap():
                    if not ready.exists():
                        return None
                    state = client.inspect(task_id)
                    office = load_office(directory / task_id)
                    refunds = [entry for entry in office["ledger"]["entries"] if entry["kind"] == "refund"]
                    if (state["status"] == "running" and "approval" in state["steps"]
                            and "ledger.apply" not in state["steps"] and len(refunds) == 1):
                        return state
                    return None
                gap_state = wait_until(gap, process, log_path)
                check(gap_state["attempts"] == 1, "crash did not happen on the first running attempt")
            finally:
                stop(process, log)
            if crash_after is None:
                return
            process, log, log_path, _ready = spawn(directory, storage, task_id, None, label="resume")
            try:
                wait_exit(process, log_path)
            finally:
                stop(process, log)

        happy_office = directory / "happy"
        submit("happy", happy_office)
        drive("happy", "approve")
        happy = outcome(client, "happy")
        happy_state = load_office(happy_office)
        check(happy["outcome"] == "completed" and happy["approval"]["approved"] is True, "happy path did not complete")
        check(happy["approval"]["by"] == "dana", "approval did not record who decided")
        check(refunded_cents(happy_state) == 4200, "ledger refund did not land")
        check(len(requests_for(happy_state, "refund")) == 1 and requests_for(happy_state, "refund")[0]["replay"] is False,
              "happy path applied the ledger more than once")
        check(happy_state["ledger"]["entries"][0]["idempotency_key"] == "happy/ledger.apply",
              "ledger did not store the step idempotency key")
        check(len(happy_state["crm"][CUSTOMER_ID]["notes"]) == 1
              and happy_state["crm"][CUSTOMER_ID]["notes"][0]["void"] is False, "CRM note did not land")
        check(len(happy_state["notify"]["messages"]) == 1
              and happy_state["notify"]["messages"][0]["status"] == "sent", "notification did not land")
        check(happy["verifications"]["ledger"]["ok"] and happy["verifications"]["crm"]["ok"]
              and happy["verifications"]["notify"]["ok"], "read-back did not succeed")
        check(json.loads((happy_office / "audit.json").read_text()) == happy, "office audit differs from the saved output")
        check(client.inspect("happy")["attempts"] == 1, "approval resume counted as a new attempt")
        report["cases"].append("happy")
        print("happy: approved refund posted once to the ledger, CRM, and notification log", flush=True)

        crash_office = directory / "crash"
        submit("crash", crash_office)
        drive("crash", "approve", crash_after="ledger.apply")
        crash = outcome(client, "crash")
        crash_state = load_office(crash_office)
        ledger_calls = requests_for(crash_state, "refund")
        check(crash["outcome"] == "completed", "resumed saga did not complete")
        check(len([entry for entry in crash_state["ledger"]["entries"] if entry["kind"] == "refund"]) == 1
              and refunded_cents(crash_state) == 4200, "resume applied the refund twice")
        check(len(ledger_calls) == 2 and ledger_calls[0]["replay"] is False and ledger_calls[1]["replay"] is True,
              "the ledger did not treat the resumed write as the same idempotency key")
        check(ledger_calls[0]["key"] == ledger_calls[1]["key"] == "crash/ledger.apply", "idempotency key changed after the crash")
        check(len(requests_for(crash_state, "note")) == 1 and len(requests_for(crash_state, "send")) == 1,
              "later systems were applied more than once")
        check(client.inspect("crash")["attempts"] == 2, "the crashed attempt was not resumed")
        report["cases"].append("crash")
        print("crash: worker killed after the ledger write; resume posted that refund once", flush=True)

        reject_office = directory / "reject"
        submit("reject", reject_office)
        drive("reject", "reject")
        rejected = outcome(client, "reject")
        reject_state = load_office(reject_office)
        check(rejected["outcome"] == "rejected" and rejected["approval"]["approved"] is False, "rejection did not stick")
        check(reject_state["requests"] == [] and reject_state["ledger"]["entries"] == []
              and reject_state["notify"]["messages"] == []
              and reject_state["crm"][CUSTOMER_ID]["notes"] == [], "rejection wrote to a legacy system")
        report["cases"].append("reject")
        print("reject: approval declined and the legacy systems stayed unchanged", flush=True)

        compensate_office = directory / "compensate"
        submit("compensate", compensate_office, fail_at="notify")
        drive("compensate", "approve")
        compensated = outcome(client, "compensate")
        compensate_state = load_office(compensate_office)
        check(compensated["outcome"] == "compensated" and compensated["failed_step"] == "notify.apply",
              "notification failure did not compensate")
        check(refunded_cents(compensate_state) == 0, "ledger refund was not reversed")
        reversals = [entry for entry in compensate_state["ledger"]["entries"] if entry["kind"] == "reversal"]
        originals = [entry for entry in compensate_state["ledger"]["entries"] if entry["kind"] == "refund"]
        check(len(originals) == 1 and originals[0]["reversed"] is True and len(reversals) == 1, "reversal entry is missing")
        notes = compensate_state["crm"][CUSTOMER_ID]["notes"]
        check(len(notes) == 1 and notes[0]["void"] is True, "CRM note was not voided")
        check(compensate_state["notify"]["messages"] == [], "a refused notification was recorded")
        check(compensated["systems"]["refunded_cents"] == 0, "audit snapshot still shows a live refund")
        report["cases"].append("compensate")
        print("compensate: notification refusal reversed the ledger and voided the CRM note", flush=True)

        over_office = directory / "over-refund"
        submit("over-refund", over_office, proposer="over-refund")
        drive("over-refund", "none")
        over = outcome(client, "over-refund")
        over_state = load_office(over_office)
        check(over["outcome"] == "ineligible" and over["eligibility"]["code"] == "above-invoice",
              "an over-invoice proposal was not refused")
        check(over["approval"] is None and over_state["requests"] == [], "ineligible proposal waited or wrote")
        report["cases"].append("over-refund")
        print("over-refund: orchestrator refused a proposal above the paid invoice", flush=True)

        rows = export_runs(client, directory / "export")
        by_id = {row["task_id"]: row for row in rows}
        check(set(by_id) == {"happy", "crash", "reject", "compensate", "over-refund"}, "eval export did not contain every run")
        for task_id in ("happy", "crash", "reject", "compensate"):
            check(by_id[task_id]["correct"] is True, f"{task_id} proposal was labeled incorrect")
            check("proposal" in by_id[task_id] and "approval" in by_id[task_id] and "outcome" in by_id[task_id],
                  f"{task_id} eval row is missing a field")
        check(by_id["happy"]["outcome"] == "completed" and by_id["happy"]["approval"]["approved"] is True, "happy eval row")
        check(by_id["reject"]["outcome"] == "rejected" and by_id["reject"]["approval"]["approved"] is False, "reject eval row")
        check(by_id["compensate"]["outcome"] == "compensated", "compensate eval row")
        check(by_id["over-refund"]["correct"] is False and by_id["over-refund"]["outcome"] == "ineligible"
              and by_id["over-refund"]["approval"] is None, "over-refund eval row")
        happy_run = json.loads((directory / "export" / "runs" / "happy.json").read_text())
        history_events = [event["event"] for event in happy_run["history"]]
        check("signal" in history_events and "checkpoint" in history_events and "suspend" in history_events,
              "engine history is missing the approval wait")
        check(any(event["step"] == "ledger.verify" for event in happy_run["audit"]["events"]),
              "audit trail is missing the ledger read-back")
        over_history = [event["event"] for event in json.loads(
            (directory / "export" / "runs" / "over-refund.json").read_text())["history"]]
        check("signal" not in over_history, "ineligible run recorded an approval signal")
        report["evals"] = rows
        report["cases"].append("export")
        print(f"export: {len(rows)} eval rows in {directory / 'export' / 'evals.jsonl'}", flush=True)
        report["passed"] = True
    except BaseException as error:
        report["passed"] = False
        report["error"] = str(error)
        raise
    finally:
        if client is not None:
            client.close()
        if created:
            try:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
                    if objects:
                        check(not s3.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors"),
                              "storage cleanup failed")
                s3.delete_bucket(Bucket=bucket)
                try:
                    s3.head_bucket(Bucket=bucket)
                except ClientError as error:
                    status = error.response["ResponseMetadata"]["HTTPStatusCode"]
                    check(status == 404, "deleted bucket still exists")
                else:
                    raise RuntimeError("deleted bucket still exists")
                report["cleaned"] = True
            finally:
                evidence.write_text(json.dumps(report, indent=2) + "\n")
        else:
            evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Counting House evidence: {evidence}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "outputs" / "counting-house")
    parser.add_argument("--endpoint", default=STORAGE["endpoint"])
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        serve(json.loads(args.worker.read_text()))
    else:
        main(args)

"""Maintain two hosts, and let another architecture finish the run.

inventory, backup, plan_updates, apply-host-1, apply-host-2, and audit each
record the machine that actually ran them. backup writes one object to RustFS.
The task result is the audit, which does not include the machine name.
"""
import argparse
import hashlib
import json
import os
import platform
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config
from deoos import Client, EngineError

HANDLER = "maintain.hosts.v1"
STEPS = ("inventory", "backup", "plan_updates", "apply-host-1", "apply-host-2", "audit")


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


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


def record(step, body):
    return {"body": body, "machine": platform.machine(), "step": step}


def s3_client(inputs):
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    return boto3.client(
        "s3",
        endpoint_url=inputs["endpoint"],
        region_name="us-east-1",
        aws_access_key_id=inputs["access_key_id"],
        aws_secret_access_key=inputs["secret_access_key"],
        config=Config(s3={"addressing_style": "path"}),
    )


def read_inventory(path):
    document = json.loads(Path(path).read_text())
    hosts = document["hosts"]
    if [host["name"] for host in hosts] != ["host-1", "host-2"]:
        raise ValueError("inventory must list host-1 then host-2")
    return document


def maintain(ctx, inputs):
    def inventory():
        mark_ran(inputs["counter_dir"], "inventory")
        document = read_inventory(inputs["inventory_path"])
        digest = hashlib.sha256(canonical_bytes(document)).hexdigest()
        return record("inventory", {
            "hosts": [host["name"] for host in document["hosts"]],
            "sha256": digest,
        })

    def backup():
        mark_ran(inputs["counter_dir"], "backup")
        document = read_inventory(inputs["inventory_path"])
        blob = canonical_bytes(document)
        client = s3_client(inputs)
        client.put_object(Bucket=inputs["bucket"], Key=inputs["backup_key"], Body=blob)
        stored = client.get_object(Bucket=inputs["bucket"], Key=inputs["backup_key"])["Body"].read()
        if stored != blob:
            raise RuntimeError("backup object did not match the inventory")
        return record("backup", {
            "bytes": len(blob),
            "key": inputs["backup_key"],
            "sha256": hashlib.sha256(blob).hexdigest(),
        })

    def plan():
        mark_ran(inputs["counter_dir"], "plan_updates")
        document = read_inventory(inputs["inventory_path"])
        updates = [
            {
                "host": host["name"],
                "installed": host["installed"],
                "package": host["package"],
                "wanted": host["wanted"],
            }
            for host in document["hosts"]
        ]
        return record("plan_updates", updates)

    def apply(step, host_name, updates):
        mark_ran(inputs["counter_dir"], step)
        match = next(item for item in updates if item["host"] == host_name)
        return record(step, {"host": match["host"], "package": match["package"], "status": "applied", "wanted": match["wanted"]})

    def audit():
        mark_ran(inputs["counter_dir"], "audit")
        document = read_inventory(inputs["inventory_path"])
        blob = canonical_bytes(document)
        stored = s3_client(inputs).get_object(Bucket=inputs["bucket"], Key=inputs["backup_key"])["Body"].read()
        if stored != blob:
            raise RuntimeError("audit read a backup that does not match the inventory")
        if backup_step["body"]["sha256"] != hashlib.sha256(blob).hexdigest():
            raise RuntimeError("audit hash does not match the backup step")
        applied = [apply_host_1["body"]["host"], apply_host_2["body"]["host"]]
        if applied != ["host-1", "host-2"]:
            raise RuntimeError("audit expected host-1 and host-2 to be applied")
        if [item["status"] for item in (apply_host_1["body"], apply_host_2["body"])] != ["applied", "applied"]:
            raise RuntimeError("audit expected both hosts to be applied")
        return record("audit", {
            "applied": applied,
            "audit": "ok",
            "backup_sha256": backup_step["body"]["sha256"],
            "hosts": inventory_step["body"]["hosts"],
            "plan": plan_step["body"],
        })

    inventory_step = ctx.step("inventory", inventory, "1")
    backup_step = ctx.step("backup", backup, "1")
    plan_step = ctx.step("plan_updates", plan, "1")
    apply_host_1 = ctx.step("apply-host-1", lambda: apply("apply-host-1", "host-1", plan_step["body"]), "1")
    apply_host_2 = ctx.step("apply-host-2", lambda: apply("apply-host-2", "host-2", plan_step["body"]), "1")
    audit_step = ctx.step("audit", audit, "1")
    return audit_step["body"]


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
        return maintain(ctx, inputs)

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

"""Hand a host-maintenance run from linux/amd64 to linux/arm64, and back."""
import argparse
import json
import os
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
import paths
from engine_server import wait_for_url
from maintain import HANDLER, STEPS, canonical_bytes

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "python" / "multi-arch-handoff" / "inventory.json"
PY = "/app/python/multi-arch-handoff/maintain.py"
TS = "/app/typescript/multi-arch-handoff/maintain.mjs"


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def tail(path):
    if not path.exists():
        return ""
    return "\n".join(path.read_text(errors="replace").splitlines()[-50:])


def wait_until(predicate, timeout, process=None, log_path=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"worker exited {process.returncode}\n{tail(log_path)}")
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError(f"timed out\n{tail(log_path) if log_path else ''}")


def stop_container(name, process):
    subprocess.run(["docker", "kill", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if process is not None and process.poll() is None:
        process.wait(timeout=30)


def ensure_qemu():
    binfmt = Path("/proc/sys/fs/binfmt_misc")
    if not (binfmt / "status").exists():
        subprocess.run(["mount", "-t", "binfmt_misc", "binfmt_misc", str(binfmt)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not (binfmt / "status").exists():
            subprocess.run(["sudo", "-n", "mount", "-t", "binfmt_misc", "binfmt_misc", str(binfmt)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if (binfmt / "qemu-aarch64").exists():
        return
    subprocess.check_call(["docker", "run", "--privileged", "--rm", "tonistiigi/binfmt", "--install", "arm64"])
    check((binfmt / "qemu-aarch64").exists(), "qemu-aarch64 was not registered")


def start_arch_server(bucket, token):
    env = dict(os.environ)
    env.update(DEOOS_STORAGE_BUCKET=bucket, EXECUTION_PREFIX="handoff", ENGINE_TOKEN=token, LEASE_MS="20000")
    subprocess.run(["docker", "compose", "--profile", "server", "rm", "-sf", "deoos-server"],
                   cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.check_call(
        ["docker", "compose", "--profile", "server", "up", "-d", "--no-build", "deoos-server"],
        cwd=ROOT, env=env)
    wait_for_url("http://127.0.0.1:7331/health", timeout=60, headers={"Authorization": "Bearer " + token})
    return {"url": "http://deoos-server:7331", "token": token}


def stop_arch_server():
    subprocess.run(["docker", "compose", "--profile", "server", "rm", "-sf", "deoos-server"],
                   cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def s3_client(endpoint):
    return boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1",
        aws_access_key_id="local-development", aws_secret_access_key="local-development-only-secret",
        config=Config(s3={"addressing_style": "path"}))


def canonical_output(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def main(args):
    endpoint = args.endpoint
    ensure_qemu()
    subprocess.check_call(["docker", "compose", "build", "worker-amd64"], cwd=ROOT)
    subprocess.check_call(["docker", "compose", "build", "worker-arm64"], cwd=ROOT)
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

    run = uuid.uuid4().hex[:12]
    directory = Path(args.directory) / run
    directory.mkdir(parents=True)
    inventory = json.loads(INVENTORY.read_text())
    expected_backup = canonical_bytes(inventory)
    from deoos import Client
    outcomes = {}
    s3 = s3_client(endpoint)

    def run_case(name, phases):
        bucket = f"deoos-arch-{run}-{name}"[:63]
        s3.create_bucket(Bucket=bucket)
        token = uuid.uuid4().hex
        host = None
        processes = []
        try:
            remote = start_arch_server(bucket, token)
            host = Client.remote("http://127.0.0.1:7331", token=token)
            task_id = "maintain-hosts"
            case_dir = directory / name
            counter_dir = case_dir / "counters"
            counter_in_container = "/app/" + str(counter_dir.relative_to(ROOT)).replace("\\", "/")
            inputs = {
                "access_key_id": "local-development",
                "backup_key": "backup/inventory.json",
                "bucket": bucket,
                "counter_dir": counter_in_container,
                "endpoint": "http://rustfs:9000",
                "inventory_path": "/app/python/multi-arch-handoff/inventory.json",
                "secret_access_key": "local-development-only-secret",
            }
            host.submit(task_id, HANDLER, inputs, max_attempts=5, retry_ms=0)
            for index, (service, sdk, pause_after) in enumerate(phases):
                container = f"handoff-{name}-{index}"
                ready_host = case_dir / f"phase-{index}-ready"
                config_host = case_dir / f"phase-{index}.json"
                config_host.parent.mkdir(parents=True, exist_ok=True)
                config_host.write_text(json.dumps({
                    "client": remote,
                    "pause_after": pause_after,
                    "ready_path": "/app/" + str(ready_host.relative_to(ROOT)).replace("\\", "/"),
                    "task_id": task_id,
                    "timeout_seconds": 300,
                }))
                config_host.chmod(0o600)
                log_path = case_dir / f"phase-{index}.log"
                log = open(log_path, "w")
                subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                process = subprocess.Popen(
                    ["docker", "compose", "run", "-T", "--rm", "--name", container,
                     "-e", f"WORKER_SDK={sdk}",
                     "-e", "WORKER_CONFIG=/app/" + str(config_host.relative_to(ROOT)).replace("\\", "/"),
                     service],
                    cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                processes.append((container, process, log, log_path))
                if pause_after:
                    wait_until(ready_host.exists, 300, process, log_path)
                    state = host.inspect(task_id)
                    check(pause_after in state["steps"], f"{pause_after} was not saved on {service}")
                    check("plan_updates" not in state["steps"], f"{service} continued past the kill point")
                    stop_container(container, process)
                    print(f"{name}: killed {sdk} on {service} after {pause_after}", flush=True)
                else:
                    code = process.wait(timeout=300)
                    check(code == 0, f"{service} {sdk} exited {code}\n{tail(log_path)}")
            view = host.view(task_id)
            check(view["status"] == "completed", view.get("error") or view["status"])
            steps = {step: host.request(f"/tasks/{task_id}/steps/{step}") for step in STEPS}
            machines = [steps[step]["machine"] for step in STEPS]
            for step in STEPS:
                check((counter_dir / step).read_text() == "1", f"{name} {step} counter is not 1")
            listed = s3.list_objects_v2(Bucket=bucket, Prefix="backup/").get("Contents", [])
            check(len(listed) == 1, f"{name} backup object count is {len(listed)}")
            stored = s3.get_object(Bucket=bucket, Key="backup/inventory.json")["Body"].read()
            check(stored == expected_backup, f"{name} backup bytes differ from the inventory")
            return {"output": view["output"], "machines": machines, "backup": stored}
        finally:
            for container, process, log, log_path in processes:
                if process.poll() is None:
                    stop_container(container, process)
                if not log.closed:
                    log.close()
            if host is not None:
                host.close()
            stop_arch_server()
            try:
                s3.head_bucket(Bucket=bucket)
            except ClientError:
                pass
            else:
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
                    if objects:
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                s3.delete_bucket(Bucket=bucket)

    try:
        outcomes["clean"] = run_case("clean", [("worker-amd64", "python", None)])
        outcomes["forward"] = run_case("forward", [
            ("worker-amd64", "python", "backup"),
            ("worker-arm64", "typescript", None),
        ])
        outcomes["reverse"] = run_case("reverse", [
            ("worker-arm64", "typescript", "backup"),
            ("worker-amd64", "python", None),
        ])
        clean_bytes = canonical_output(outcomes["clean"]["output"])
        check(canonical_output(outcomes["forward"]["output"]) == clean_bytes, "forward audit is not byte-identical to the single-arch run")
        check(canonical_output(outcomes["reverse"]["output"]) == clean_bytes, "reverse audit is not byte-identical to the single-arch run")
        check(outcomes["clean"]["machines"] == ["x86_64"] * len(STEPS), f"clean machines: {outcomes['clean']['machines']}")
        check(outcomes["forward"]["machines"] == ["x86_64", "x86_64", "aarch64", "aarch64", "aarch64", "aarch64"],
              f"forward machines: {outcomes['forward']['machines']}")
        check(outcomes["reverse"]["machines"] == ["aarch64", "aarch64", "x86_64", "x86_64", "x86_64", "x86_64"],
              f"reverse machines: {outcomes['reverse']['machines']}")
        check(outcomes["clean"]["backup"] == outcomes["forward"]["backup"] == outcomes["reverse"]["backup"], "backup objects differ")
        print("multi-arch: audit matches the single-arch run; inventory and backup ran once", flush=True)
        report = {"run": run, "passed": True, "machines": {name: item["machines"] for name, item in outcomes.items()}}
    except Exception:
        report = {"run": run, "passed": False, "machines": {name: item.get("machines") for name, item in outcomes.items()}}
        (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        raise
    (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Multi-arch handoff evidence: {directory / 'result.json'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "outputs" / "multi-arch-handoff")
    parser.add_argument("--endpoint", default=os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000"))
    main(parser.parse_args())

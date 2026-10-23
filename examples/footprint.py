"""One process, one bucket. Kill it, start it again, then fail, retry, and cancel.

Library mode is this process. There is no separate engine and no database.
With storage configured:

    python examples/footprint.py submit --id job-001 --log "$PWD/job.log"
    python examples/footprint.py work --id job-001 --crash-after collect
    python examples/footprint.py work --id job-001
    python examples/footprint.py info

Exit code 86 means ``collect`` is committed and this process is gone. The
next ``work`` does not append ``collect`` again. If the dead process still
holds the lease, that ``work`` waits for it to expire (30 seconds unless
``LEASE_MS`` is set). ``info`` prints this process id; in library mode that
id is the engine.

A flaky call burns its attempt budget, then an operator retries it:

    python examples/footprint.py submit-flaky --id flaky-001
    python examples/footprint.py work --id flaky-001 --fail-flaky
    python -m deoos explain flaky-001
    python -m deoos retry flaky-001
    python examples/footprint.py work --id flaky-001

Cancel stops a task that is waiting. It does not unwind a step that already
finished:

    python examples/footprint.py submit-hold --id hold-001
    python examples/footprint.py work --id hold-001
    python -m deoos cancel hold-001

Server mode is optional. The workflow code does not change:

    DEOOS_STORAGE_BUCKET=my-workflows AWS_REGION=us-east-1 \\
      ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=dev /path/to/deoos-server
    DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev \\
      python examples/footprint.py submit --id job-002 --log "$PWD/job.log"

Open http://127.0.0.1:7331/ui on that server. Non-loopback binding requires a token.
"""
import argparse
import json
import os
import time

from deoos import Client

JOB = "footprint.job.v1"
FLAKY = "footprint.flaky.v1"
HOLD = "footprint.hold.v1"


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


def append(path, line):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    return {"line": line}


def crash_after(name):
    if os.environ.get("DEOOS_CRASH_AFTER") == name:
        print(f"crash after committed step {name}", flush=True)
        os._exit(86)


def job(ctx, inputs):
    for name in ("collect", "check", "publish"):
        ctx.step(name, lambda name=name: append(inputs["log"], name))
        crash_after(name)
    return {"log": inputs["log"], "lines": 3}


def flaky(ctx, _inputs):
    def call():
        if os.environ.get("DEOOS_FAIL_FLAKY") == "1":
            raise RuntimeError("downstream returned 503")
        return {"ok": True}
    return ctx.step("call", call)


def hold(ctx, _inputs):
    ctx.step("prepared", lambda: {"ready": True})
    ctx.wait_signal("go")
    return {"status": "released"}


HANDLERS = {JOB: job, FLAKY: flaky, HOLD: hold}


def run_until(client, task_id, fail_flaky):
    announced = False
    while True:
        try:
            worked = client.run_once(HANDLERS)
        except Exception as error:
            print(str(error), flush=True)
            view = client.view(task_id)
            if view["status"] == "failed" and fail_flaky:
                show(view)
                print(f"Attempt budget spent. Next: python -m deoos explain {task_id}", flush=True)
                return 0
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
        if view["status"] == "waiting" and wait.get("kind") == "signal" and not wait.get("assigned"):
            show(view)
            print(f"Waiting. Cancel with: python -m deoos cancel {task_id}", flush=True)
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
    submit = commands.add_parser("submit", help="three steps appended to a local log")
    submit.add_argument("--id", required=True)
    submit.add_argument("--log", required=True)
    flaky_cmd = commands.add_parser("submit-flaky", help="a step that fails while --fail-flaky is set")
    flaky_cmd.add_argument("--id", required=True)
    hold_cmd = commands.add_parser("submit-hold", help="one step, then wait for a signal")
    hold_cmd.add_argument("--id", required=True)
    work = commands.add_parser("work")
    work.add_argument("--id", required=True)
    work.add_argument("--crash-after", choices=("collect",))
    work.add_argument("--fail-flaky", action="store_true",
                      help="make the flaky step raise 503 until its attempt budget is spent")
    commands.add_parser("info", help="print engine health for this process")
    view = commands.add_parser("view")
    view.add_argument("--id", required=True)
    args = parser.parse_args()
    if args.command == "work":
        if args.crash_after:
            os.environ["DEOOS_CRASH_AFTER"] = args.crash_after
        else:
            os.environ.pop("DEOOS_CRASH_AFTER", None)
        if args.fail_flaky:
            os.environ["DEOOS_FAIL_FLAKY"] = "1"
        else:
            os.environ.pop("DEOOS_FAIL_FLAKY", None)
    with create_client() as client:
        if args.command == "info":
            info = client.request("/info")
            show(info)
            same = info.get("process_id") == os.getpid()
            print("library mode: this process is the engine" if same
                  else "server mode: this process is a client of the engine", flush=True)
            return 0
        if args.command == "submit":
            log = os.path.abspath(args.log)
            client.submit(args.id, JOB, {"log": log}, max_attempts=3, retry_ms=200)
            show(client.view(args.id))
            return 0
        if args.command == "submit-flaky":
            client.submit(args.id, FLAKY, {}, max_attempts=2, retry_ms=0)
            show(client.view(args.id))
            return 0
        if args.command == "submit-hold":
            client.submit(args.id, HOLD, {}, max_attempts=3, retry_ms=200)
            show(client.view(args.id))
            return 0
        if args.command == "view":
            show(client.view(args.id))
            return 0
        return run_until(client, args.id, bool(getattr(args, "fail_flaky", False)))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(str(error))
        raise SystemExit(1) from None

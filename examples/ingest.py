"""Fixed-interval fan-out. Each fire reads its own scheduled_at.

There is no cron and no timezone rule. The interval is milliseconds. Missed
ticks keep the latest one (``missed=latest``) and skip a tick that is still
running (``overlap=skip``). Backfill is an SDK call, not a CLI command.
Pause it first; a paused schedule does not drain a backfill until resume.

The default anchor is 15 seconds ago and the interval is 5 seconds, so the
first poll admits one latest tick and leaves a gap. With storage configured:

    python examples/ingest.py schedule --id hourly --output "$PWD/ingest-out"
    python examples/ingest.py work
    python examples/ingest.py pause --id hourly
    python examples/ingest.py backfill --id hourly
    python examples/ingest.py resume --id hourly
    python examples/ingest.py work

Each tick writes ``<scheduled_at>.json`` under the output directory. A replay
of the same tick replaces that file. The page tasks are child workflows; the
parent reads ``ctx.task["schedule"]["scheduled_at"]`` because the schedule's
inputs do not change per tick. ``view`` does not include that field.

``work`` exits when the queue is idle. ``work --follow`` polls until Ctrl-C.
"""
import argparse
import json
import os
from pathlib import Path
import tempfile
import time

from deoos import Client

PARENT = "ingest.publish.v1"
PAGE = "ingest.page.v1"


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


def tick_of(ctx, inputs):
    schedule = ctx.task.get("schedule") or {}
    tick = schedule.get("scheduled_at", inputs.get("scheduled_at"))
    if isinstance(tick, bool) or not isinstance(tick, int):
        raise ValueError("scheduled_at must come from the schedule tick or from inputs")
    return tick


def fetch_page(scheduled_at, page):
    return [{"scheduled_at": scheduled_at, "page": page, "item": f"{scheduled_at}-{page}"}]


def write_batch(directory, scheduled_at, records):
    path = Path(directory) / f"{scheduled_at}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"scheduled_at": scheduled_at, "records": records}, sort_keys=True) + "\n"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        print(json.dumps({"published": str(path), "scheduled_at": scheduled_at, "records": len(records)}),
              flush=True)
        return {"path": str(path), "records": len(records)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def page(ctx, inputs):
    scheduled_at = inputs["scheduled_at"]
    return ctx.step("fetch", lambda: fetch_page(scheduled_at, inputs["page"]))


def publish(ctx, inputs):
    scheduled_at = tick_of(ctx, inputs)
    children = [ctx.spawn(
        f"page-{number}", PAGE,
        {"output_dir": inputs["output_dir"], "page": number, "scheduled_at": scheduled_at},
        max_attempts=3, retry_ms=200,
    ) for number in range(1, inputs["pages"] + 1)]
    pages = ctx.join("pages", children)
    records = [row for chunk in pages for row in chunk]
    return ctx.step("publish", lambda: write_batch(inputs["output_dir"], scheduled_at, records))


HANDLERS = {PARENT: publish, PAGE: page}


def schedule_brief(state):
    backfill = state.get("backfill")
    return {"id": state["id"], "interval_ms": state["interval_ms"], "first_due_ms": state["first_due_ms"],
            "next_due_ms": state["next_due_ms"], "paused": state["paused"], "missed": state["missed"],
            "overlap": state["overlap"], "backfill": backfill, "active_task": state.get("active_task")}


def run_until(client, follow):
    idle = 0
    while True:
        try:
            worked = client.run_once(HANDLERS)
        except KeyboardInterrupt:
            return 0
        except Exception as error:
            print(str(error), flush=True)
            time.sleep(0.2)
            continue
        if worked:
            idle = 0
            continue
        if follow:
            time.sleep(0.5)
            continue
        idle += 1
        if idle >= 2:
            print("idle", flush=True)
            return 0
        time.sleep(0.2)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    schedule = commands.add_parser("schedule", help="create an immutable interval schedule")
    schedule.add_argument("--id", required=True)
    schedule.add_argument("--output", required=True)
    schedule.add_argument("--pages", type=int, default=2)
    schedule.add_argument("--interval-ms", type=int, default=5000)
    schedule.add_argument("--first-due-ms", type=int,
                          help="default is 15 seconds ago, so the first poll has a gap to backfill")
    work = commands.add_parser("work", help="poll until idle, or until Ctrl-C with --follow")
    work.add_argument("--follow", action="store_true")
    for name, help_text in (("pause", "pause a schedule"), ("resume", "resume a schedule")):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--id", required=True)
    backfill = commands.add_parser("backfill", help="fill skipped ticks in the past; the schedule must be paused")
    backfill.add_argument("--id", required=True)
    view = commands.add_parser("view", help="print a task view, or a schedule summary with --schedule")
    view.add_argument("--id", required=True)
    view.add_argument("--schedule", action="store_true")
    args = parser.parse_args()
    with create_client() as client:
        if args.command == "schedule":
            if args.pages < 1 or args.interval_ms < 1:
                raise ValueError("pages and interval-ms must be >= 1")
            output = str(Path(args.output).resolve())
            Path(output).mkdir(parents=True, exist_ok=True)
            first = args.first_due_ms if args.first_due_ms is not None else int(time.time() * 1000) - 15_000
            state = client.schedule(args.id, PARENT, {"output_dir": output, "pages": args.pages},
                                    args.interval_ms, first_due_ms=first, missed="latest", overlap="skip",
                                    max_attempts=3, retry_ms=200)
            show(schedule_brief(state))
            return 0
        if args.command == "work":
            return run_until(client, args.follow)
        if args.command == "pause":
            show(schedule_brief(client.pause_schedule(args.id)))
            return 0
        if args.command == "resume":
            show(schedule_brief(client.resume_schedule(args.id)))
            return 0
        if args.command == "backfill":
            state = client.inspect_schedule(args.id)
            if not state["paused"]:
                raise SystemExit(f"Pause first: python examples/ingest.py pause --id {args.id}")
            now = int(time.time() * 1000)
            start, end = state["first_due_ms"], min(state["next_due_ms"], now)
            if end <= start:
                raise SystemExit("No past gap. Run work once so a tick is admitted, then pause.")
            show(schedule_brief(client.backfill(args.id, start, end, limit=100)))
            print("Backfill is stored. Resume, then work, to run it.", flush=True)
            return 0
        if args.schedule:
            show(schedule_brief(client.inspect_schedule(args.id)))
        else:
            show(client.view(args.id))
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(str(error))
        raise SystemExit(1) from None

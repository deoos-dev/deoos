"""Three local pages, one checkpoint each, then one CSV. The crash is a flag here.

The pages stand in for an API. Replace ``fetch_page`` with an HTTP call when
you have one. With storage configured:

    python examples/pages.py submit --id snap-001 --output "$PWD/articles.csv"
    python examples/pages.py work --id snap-001 --crash-after fetch-page-1
    python examples/pages.py work --id snap-001

Exit code 86 on ``--crash-after`` means that step is already committed. The
next ``work`` replays it and continues. If the dead process still holds the
lease, that ``work`` waits for it to expire (30 seconds unless ``LEASE_MS``
is set). ``articles.csv.fetches`` gains one line per fetch; page 1 is not
fetched again.

``--crash-before-commit write-csv`` exits after the CSV is on disk and before
that checkpoint. The next ``work`` writes the same bytes again.

    python examples/pages.py submit --id snap-gap --output "$PWD/gap.csv"
    python examples/pages.py work --id snap-gap --crash-before-commit write-csv
    python examples/pages.py work --id snap-gap

Hacker News (``examples/hacker-news``) is a larger live version of this shape.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import tempfile
import time

from deoos import Client

HANDLER = "pages.snapshot.v1"
FIELDS = ("id", "title")
PAGES = (
    ({"id": 1, "title": "alpha"}, {"id": 2, "title": "beta"}),
    ({"id": 2, "title": "beta"}, {"id": 3, "title": "gamma"}),
    ({"id": 4, "title": "delta"}, {"id": 5, "title": "epsilon"}),
)


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


def crash_after(name):
    if os.environ.get("DEOOS_CRASH_AFTER") == name:
        print(f"crash after committed step {name}", flush=True)
        os._exit(86)


def fetch_page(page, ledger):
    if not 1 <= page <= len(PAGES):
        raise ValueError(f"page must be 1..{len(PAGES)}")
    with open(ledger, "a", encoding="utf-8") as handle:
        handle.write(f"{page}\n")
    return list(PAGES[page - 1])


def normalize(pages):
    rows, seen = [], set()
    for page in pages:
        for row in page:
            if row["id"] in seen:
                continue
            seen.add(row["id"])
            rows.append(row)
    return rows


def write_csv(path, rows):
    output = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", dir=output.parent,
                                         prefix=f".{output.name}.", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
        if os.environ.get("DEOOS_CRASH_BEFORE_COMMIT") == "write-csv":
            print("crash before commit of write-csv; the CSV is already in place", flush=True)
            os._exit(86)
        return {"output": path, "rows": len(rows)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def snapshot(ctx, inputs):
    pages = []
    for page in range(1, inputs["pages"] + 1):
        name = f"fetch-page-{page}"
        pages.append(ctx.step(name, lambda page=page: fetch_page(page, inputs["ledger"])))
        crash_after(name)
    rows = ctx.step("normalize", lambda: normalize(pages))
    crash_after("normalize")
    return ctx.step("write-csv", lambda: write_csv(inputs["output"], rows))


HANDLERS = {HANDLER: snapshot}


def run_until(client, task_id):
    announced = False
    while True:
        try:
            worked = client.run_once(HANDLERS)
        except Exception as error:
            print(str(error), flush=True)
            show(client.view(task_id))
            return 1
        view = client.view(task_id)
        if view["status"] in ("completed", "failed", "cancelled"):
            show(view)
            return 0 if view["status"] == "completed" else 1
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
    submit = commands.add_parser("submit")
    submit.add_argument("--id", required=True)
    submit.add_argument("--output", required=True)
    submit.add_argument("--pages", type=int, default=len(PAGES))
    work = commands.add_parser("work")
    work.add_argument("--id", required=True)
    work.add_argument("--crash-after", choices=("fetch-page-1", "fetch-page-2", "normalize"))
    work.add_argument("--crash-before-commit", choices=("write-csv",))
    view = commands.add_parser("view")
    view.add_argument("--id", required=True)
    args = parser.parse_args()
    if args.command == "work":
        if args.crash_after and args.crash_before_commit:
            parser.error("pass one crash flag")
        if args.crash_after:
            os.environ["DEOOS_CRASH_AFTER"] = args.crash_after
        else:
            os.environ.pop("DEOOS_CRASH_AFTER", None)
        if args.crash_before_commit:
            os.environ["DEOOS_CRASH_BEFORE_COMMIT"] = args.crash_before_commit
        else:
            os.environ.pop("DEOOS_CRASH_BEFORE_COMMIT", None)
    with create_client() as client:
        if args.command == "submit":
            if not 1 <= args.pages <= len(PAGES):
                raise ValueError(f"pages must be 1..{len(PAGES)}")
            output = str(Path(args.output).resolve())
            client.submit(args.id, HANDLER, {
                "output": output, "ledger": output + ".fetches", "pages": args.pages,
            }, max_attempts=3, retry_ms=200)
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

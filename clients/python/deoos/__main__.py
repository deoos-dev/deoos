"""Small operator CLI: python -m deoos --help."""
import argparse
import json
import os
import sys

from . import Client


def main():
    parser = argparse.ArgumentParser(prog="python -m deoos")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="list up to 100 tasks")

    inspect = commands.add_parser("inspect", help="inspect a task")
    inspect.add_argument("id")

    retry = commands.add_parser("retry", help="retry a failed or cancelled task")
    retry.add_argument("id")
    retry.add_argument("--revision", help="expected revision; defaults to the current revision")
    retry.add_argument("--operation-id", help="stable ID for retrying an uncertain request")

    cancel = commands.add_parser("cancel", help="cancel a task")
    cancel.add_argument("id")

    schedule = commands.add_parser("schedule", help="inspect or control a schedule")
    schedule.add_argument("action", choices=("inspect", "pause", "resume"))
    schedule.add_argument("id")

    args = parser.parse_args()
    url = os.environ.get("ENGINE_URL")
    token = os.environ.get("ENGINE_TOKEN")
    client = None
    try:
        client = Client.remote(url, token) if url else Client(token=token)
        if args.command == "list":
            result = client.list_tasks()
        elif args.command == "inspect":
            result = client.inspect(args.id)
        elif args.command == "retry":
            revision = args.revision
            if revision is None:
                revision = client.inspect(args.id)["revision"]
            result = client.retry(args.id, revision, args.operation_id)
        elif args.command == "cancel":
            result = client.cancel(args.id)
        elif args.action == "inspect":
            result = client.inspect_schedule(args.id)
        elif args.action == "pause":
            result = client.pause_schedule(args.id)
        else:
            result = client.resume_schedule(args.id)
        print(json.dumps(result, indent=2, sort_keys=True))
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

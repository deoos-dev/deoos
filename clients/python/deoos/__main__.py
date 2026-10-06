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
    mcp = commands.add_parser("mcp", help="serve MCP 2025-11-25 over stdio")
    mcp.add_argument("--allow-actions", action="store_true",
                     help="expose task cancel, retry and signal tools")

    inspect = commands.add_parser("inspect", help="inspect a task")
    inspect.add_argument("id")

    summary = commands.add_parser("summary", help="summarize progress as JSON without fetching payloads")
    summary.add_argument("id")
    explain = commands.add_parser("explain", help="explain persisted progress in plain text")
    explain.add_argument("id")

    retry = commands.add_parser("retry", help="retry a failed or cancelled task")
    retry.add_argument("id")
    retry.add_argument("--revision", help="expected revision; defaults to current; reuse the original revision after an uncertain request")
    retry.add_argument("--operation-id", help="stable ID; reuse with the original --revision after an uncertain request")

    cancel = commands.add_parser("cancel", help="cancel a task")
    cancel.add_argument("id")

    schedule = commands.add_parser("schedule", help="inspect or control a schedule")
    schedule.add_argument("action", choices=("inspect", "pause", "resume"))
    schedule.add_argument("id")

    args = parser.parse_args()
    if args.command == "mcp":
        from .mcp import serve
        return serve(allow_actions=args.allow_actions)
    url = os.environ.get("ENGINE_URL")
    token = os.environ.get("ENGINE_TOKEN")
    client = None
    try:
        client = Client.remote(url, token) if url else Client(token=token)
        if args.command == "list":
            result = client.list_tasks()
        elif args.command == "inspect":
            result = client.inspect(args.id)
        elif args.command in ("summary", "explain"):
            result = client.summary(args.id)
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
        if args.command == "explain":
            print(f'{result["id"]}: {result["status"]} ({result["handler"]})')
            print(f'Attempts: {result["attempts"]}/{result["max_attempts"]}')
            print('Completed steps: ' + (', '.join(result['completed_steps']) or 'none'))
            wait = result['wait']
            if wait:
                if wait['kind'] == 'timer':
                    print(f'Waiting for timer {wait["name"]}; deadline (Unix ms): {wait["deadline_ms"]}')
                elif wait['kind'] == 'signal':
                    print(f'Waiting for signal {wait["name"]}; assigned: {wait["assigned"]}')
                else:
                    print('Waiting for children: ' + ', '.join(wait['ids']))
            if result['last_failure']:
                print('Last recorded failure: ' + result['last_failure']['message'])
            print('Suggested actions: ' + (', '.join(result['actions']) or 'none'))
            print('This is a persisted snapshot; a worker must poll to resume waiting work.')
        else:
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

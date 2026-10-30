"""Small operator CLI: python -m deoos --help."""
import argparse
import json
import sys

from . import client_from_environment
from .agent import HISTORY_NOTICE, annotate_list, dumps, enabled, history_body, progress


def add_agent(parser):
    # SUPPRESS so a parent --agent is not reset by the subcommand's default.
    parser.add_argument("--agent", action="store_true", default=argparse.SUPPRESS,
                        help="compact output for coding agents; progress on stderr")


def task_view(state):
    """Display an acknowledged mutation without a second, fallible read."""
    return dict(id=state["id"], function=state["handler"], status=state["status"],
                inputs=state["inputs"], output=state["output"], error=state["error"],
                attempts=state["attempts"], completed_steps=sorted(state["steps"]))


def json_arg(text, label):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise ValueError(f"{label} must be JSON, for example {{\"ok\": true}}") from None


def main():
    parser = argparse.ArgumentParser(prog="python -m deoos")
    add_agent(parser)
    commands = parser.add_subparsers(dest="command", required=True)
    add_agent(commands.add_parser("list", help="list up to 100 tasks"))
    add_agent(commands.add_parser("info", help="show engine and storage health without reading storage"))
    mcp = commands.add_parser("mcp", help="serve MCP 2025-11-25 over stdio")
    add_agent(mcp)
    mcp.add_argument("--allow-actions", action="store_true",
                     help="expose task submit, cancel, retry and signal tools")

    inspect = commands.add_parser("inspect", help="inspect a task")
    add_agent(inspect)
    inspect.add_argument("id")
    inspect.add_argument("--internal", action="store_true",
                         help="include raw coordination and storage details")
    history = commands.add_parser("history", help="show recent execution events")
    add_agent(history)
    history.add_argument("id")

    summary = commands.add_parser("summary", help="summarize progress as JSON without fetching payloads")
    add_agent(summary)
    summary.add_argument("id")
    explain = commands.add_parser("explain", help="explain persisted progress in plain text")
    add_agent(explain)
    explain.add_argument("id")

    retry = commands.add_parser("retry", help="retry a failed or cancelled task")
    add_agent(retry)
    retry.add_argument("id")
    retry.add_argument("--revision", help="expected revision; defaults to current; reuse the original revision after an uncertain request")
    retry.add_argument("--operation-id", help="stable ID; reuse with the original --revision after an uncertain request")

    cancel = commands.add_parser("cancel", help="cancel a task")
    add_agent(cancel)
    cancel.add_argument("id")

    schedule = commands.add_parser("schedule", help="inspect or control a schedule")
    add_agent(schedule)
    schedule.add_argument("action", choices=("inspect", "pause", "resume"))
    schedule.add_argument("id")

    submit = commands.add_parser("submit", help="start a workflow")
    add_agent(submit)
    submit.add_argument("id")
    submit.add_argument("handler")
    submit.add_argument("inputs", nargs="?", default="{}", help="JSON object (default {})")
    submit.add_argument("--max-attempts", type=int, default=3)
    submit.add_argument("--retry-ms", type=int, default=0)

    signal = commands.add_parser("signal", help="send a signal to a running workflow")
    add_agent(signal)
    signal.add_argument("id")
    signal.add_argument("name")
    signal.add_argument("value", help="JSON value")
    signal.add_argument("--operation-id", help="stable ID; reuse it to replay this signal")

    feedback = commands.add_parser(
        "feedback", help="file a problem as a GitHub issue labeled agent-feedback")
    add_agent(feedback)
    feedback.add_argument("--tried", required=True, help="what you tried")
    feedback.add_argument("--happened", required=True, help="what happened")
    feedback.add_argument("--repro", help="optional repro; sent only when you pass it")
    feedback.add_argument(
        "--include-secrets", action="store_true",
        help="send the text even if it looks like a credential")

    args = parser.parse_args()
    agent = enabled(getattr(args, "agent", False))
    detail = getattr(args, "id", None)
    if args.command == "mcp":
        from .mcp import serve
        return serve(allow_actions=args.allow_actions, agent=agent)
    if args.command == "feedback":
        from .feedback import file_feedback
        progress("feedback", active=agent)
        try:
            print(dumps(file_feedback(
                args.tried, args.happened, args.repro, args.include_secrets), agent))
        except Exception as error:
            print(str(error), file=sys.stderr)
            return 1
        return 0
    client = None
    try:
        progress(args.command, detail, active=agent)
        client = client_from_environment()
        if args.command == "info":
            result = client.request("/info")
        elif args.command == "list":
            result = annotate_list(client.list_tasks())
        elif args.command == "inspect":
            result = client.inspect(args.id) if args.internal else client.view(args.id)
        elif args.command == "history":
            events = client.history(args.id)
            if agent:
                result = history_body(events)
            else:
                if len(events) == 32:
                    print(HISTORY_NOTICE, file=sys.stderr)
                result = events
        elif args.command in ("summary", "explain"):
            result = client.summary(args.id)
        elif args.command == "retry":
            revision = args.revision
            if revision is None:
                revision = client.inspect(args.id)["revision"]
            result = task_view(client.retry(args.id, revision, args.operation_id))
        elif args.command == "cancel":
            result = task_view(client.cancel(args.id))
        elif args.command == "submit":
            inputs = json_arg(args.inputs, "inputs")
            if not isinstance(inputs, dict):
                raise ValueError("inputs must be a JSON object")
            result = task_view(client.submit(
                args.id, args.handler, inputs, args.max_attempts, args.retry_ms))
        elif args.command == "signal":
            result = task_view(client.signal(
                args.id, args.name, json_arg(args.value, "value"), args.operation_id))
        elif args.action == "inspect":
            result = client.inspect_schedule(args.id)
        elif args.action == "pause":
            result = client.pause_schedule(args.id)
        else:
            result = client.resume_schedule(args.id)
        if args.command == "explain" and not agent:
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
            print(dumps(result, agent))
    except Exception as error:
        url = getattr(client, "url", None)
        if url and isinstance(error, OSError):
            print(f"cannot reach engine at {url}; run make dev", file=sys.stderr)
        else:
            print(str(error), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Print the summary and the retained history for one task.

Usage: python agents/scripts/debug_run.py TASK_ID
The summary includes status, completed steps, what the task is waiting on,
and the last recorded failure. History is the last 32 retained events.
"""
import json
import sys

from deoos import client_from_environment


def main():
    if len(sys.argv) != 2:
        print("usage: python agents/scripts/debug_run.py TASK_ID", file=sys.stderr)
        return 2
    client = client_from_environment()
    try:
        summary = client.summary(sys.argv[1])
        history = client.history(sys.argv[1])
    finally:
        client.close()
    print(json.dumps({"summary": summary, "history": history}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)

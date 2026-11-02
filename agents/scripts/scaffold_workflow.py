"""Write a durable workflow.py that survives a kill inside the second step.

Usage: python agents/scripts/scaffold_workflow.py DIRECTORY

The program reads ENGINE_URL, DEOOS_TASK_ID, DEOOS_MARKER, and
DEOOS_HOLD_SECONDS. Step "work" appends one line. Step "hold" appends one
line and, the first time, sleeps. A killed process keeps "work" and runs
"hold" again. The grader starts it with `python workflow.py`.
"""
import pathlib
import sys

WORKFLOW = '''import os
import threading
import time

from deoos import Client


def survive(ctx, inputs):
    marker = inputs["marker"]

    def work():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("work\\n")
        return "work"

    def hold():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("hold\\n")
        if open(marker, encoding="utf-8").read().count("hold\\n") == 1:
            time.sleep(int(os.environ.get("DEOOS_HOLD_SECONDS", "30")))
        return "held"

    ctx.step("work", work)
    ctx.step("hold", hold)
    return "done"


def main():
    client = Client.remote(os.environ["ENGINE_URL"])
    try:
        client.submit(
            os.environ["DEOOS_TASK_ID"],
            "survive",
            {"marker": os.environ["DEOOS_MARKER"]},
        )
        client.run_worker({"survive": survive}, stop_event=threading.Event(), poll_interval=0.2)
    finally:
        client.close()


if __name__ == "__main__":
    main()
'''


def main():
    if len(sys.argv) != 2:
        print("usage: python agents/scripts/scaffold_workflow.py DIRECTORY", file=sys.stderr)
        return 2
    destination = pathlib.Path(sys.argv[1])
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / "workflow.py"
    target.write_text(WORKFLOW, encoding="utf-8")
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

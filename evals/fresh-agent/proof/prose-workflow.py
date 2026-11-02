import os
import threading
import time
from deoos import Client


def survive(ctx, inputs):
    marker = inputs["marker"]

    def work():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("work\n")
        return "work"

    def hold():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("hold\n")
        with open(marker, encoding="utf-8") as handle:
            contents = handle.read()
        if contents.count("hold\n") == 1:
            time.sleep(float(os.environ["DEOOS_HOLD_SECONDS"]))
        return "held"

    ctx.step("work", work)
    ctx.step("hold", hold)
    return "done"


def main():
    client = Client.remote(os.environ["ENGINE_URL"])
    client.submit(
        os.environ["DEOOS_TASK_ID"],
        "survive",
        {"marker": os.environ["DEOOS_MARKER"]},
    )
    client.run_worker(
        {"survive": survive},
        stop_event=threading.Event(),
        poll_interval=0.2,
    )


if __name__ == "__main__":
    main()

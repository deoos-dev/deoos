"""Run a five-step agent and keep the model calls that already finished.

plan, research, draft, check, and publish each call the local model once and
then one tool. publish charges with ctx.idempotency_key("publish") and, if that
call times out after the charge landed, retries the same key.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from deoos import Client, EngineError

HANDLER = "agent.run.v1"
STEPS = ("plan", "research", "draft", "check", "publish")


def mark_ran(counter_dir, name):
    if not counter_dir:
        return
    directory = Path(counter_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    count = int(path.read_text()) if path.exists() else 0
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, str(count + 1).encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def post_json(url, body, key=None, timeout=10):
    payload = json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    request = urllib.request.Request(url, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result = json.load(response)
    except TimeoutError:
        raise
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise TimeoutError(str(error.reason)) from error
        raise
    if not isinstance(result, dict):
        raise ValueError("agent service response must be an object")
    return result


def model_and_tool(ctx, inputs, step):
    mark_ran(inputs.get("counter_dir"), step)
    base = inputs["service_url"].rstrip("/")
    model = post_json(base + "/llm", {"step": step})
    tool = post_json(base + "/tool", {"step": step})
    if tool.get("ok") is not True:
        raise RuntimeError(tool.get("error") or f"{step} tool failed")
    return {"step": step, "tokens": model["tokens"], "tool": tool["detail"]}


def publish(ctx, inputs):
    mark_ran(inputs.get("counter_dir"), "publish")
    base = inputs["service_url"].rstrip("/")
    model = post_json(base + "/llm", {"step": "publish"})
    key = ctx.idempotency_key("publish")
    body = {"amount_cents": inputs["amount_cents"], "idempotency_key": key, "step": "publish"}
    try:
        charge = post_json(base + "/charge", body, key=key, timeout=2)
    except TimeoutError:
        charge = post_json(base + "/charge", body, key=key, timeout=10)
    return {
        "charge_id": charge["id"],
        "idempotency_key": key,
        "step": "publish",
        "tokens": model["tokens"],
    }


def agent(ctx, inputs):
    plan = ctx.step("plan", lambda: model_and_tool(ctx, inputs, "plan"))
    research = ctx.step("research", lambda: model_and_tool(ctx, inputs, "research"))
    draft = ctx.step("draft", lambda: model_and_tool(ctx, inputs, "draft"))
    check = ctx.step("check", lambda: model_and_tool(ctx, inputs, "check"))
    published = ctx.step("publish", lambda: publish(ctx, inputs))
    finished = {"plan": plan, "research": research, "draft": draft, "check": check, "publish": published}

    def report():
        spend = {name: finished[name]["tokens"] for name in STEPS}
        return {"charge_id": published["charge_id"], "spend": spend, "total": sum(spend.values())}

    return ctx.step("report", report)


def _pause(ready_path, name):
    path = Path(ready_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, name.encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    while True:
        time.sleep(3600)


def run_worker(config):
    pause_after = config.get("pause_after")
    pause_inside = config.get("pause_inside")
    ready_path = config.get("ready_path")

    def handler(ctx, inputs):
        if ctx.task["id"] != config["task_id"]:
            raise RuntimeError(f"claimed unexpected task {ctx.task['id']}")
        original = ctx.step

        def step(name, function, revision="1"):
            def execute():
                result = function()
                if name == pause_inside:
                    _pause(ready_path, name)
                return result

            result = original(name, execute, revision)
            if name == pause_after:
                _pause(ready_path, name)
            return result

        if pause_after or pause_inside:
            ctx.step = step
        return agent(ctx, inputs)

    client = Client(**config["client"])
    try:
        deadline = time.monotonic() + int(config.get("timeout_seconds", 180))
        while time.monotonic() < deadline:
            try:
                state = client.inspect(config["task_id"])
            except EngineError as error:
                if error.status != 404:
                    raise
                time.sleep(0.1)
                continue
            if state["status"] == "completed":
                return 0
            if state["status"] in ("failed", "cancelled"):
                print(state.get("error") or state["status"], file=sys.stderr)
                return 1
            try:
                client.run_once({HANDLER: handler})
            except Exception as error:
                print(error, file=sys.stderr)
                return 1
            time.sleep(0.05)
        print("worker timed out", file=sys.stderr)
        return 1
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["work"])
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    return run_worker(json.loads(args.config.read_text()))


if __name__ == "__main__":
    raise SystemExit(main())

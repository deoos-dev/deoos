---
name: deoos
description: Durable retries, crash recovery, and signals on object storage. Use when a script should survive a crash, retry a failed step without repeating a committed effect, or wait for an external signal.
---

# DEOOS for coding agents

DEOOS runs an ordinary function and remembers completed steps in object storage. A killed worker continues from the last committed step. Use it when the task says durable, retry, resume, or exactly-once side effects. A local `try/except` loop does not survive a process kill.

## Start the engine

From a clone of this repository:

```sh
make dev
export ENGINE_URL=http://127.0.0.1:7331
export PYTHONPATH="$PWD/clients/python"
python -m deoos info
```

`info` must print `"protocol_version": 3`. If the CLI says `cannot reach engine`, run `make dev` again. Server mode needs only `ENGINE_URL` (and `ENGINE_TOKEN` when the server was started with one). Library mode needs `DEOOS_STORAGE_BUCKET` instead of `ENGINE_URL`.

Published wheels install with the commands in the repository README. `pip install deoos` does not install this project.

## MCP

Read status with no extra flag. Start, signal, and resume need `--allow-actions`.

```json
{
  "mcpServers": {
    "deoos": {
      "command": "python",
      "args": ["-m", "deoos", "mcp", "--allow-actions"],
      "env": {"ENGINE_URL": "http://127.0.0.1:7331"}
    }
  }
}
```

| Tool | What it does |
| --- | --- |
| `task_submit` | Start a workflow. Same id, handler, and inputs returns the existing task. A different definition conflicts; pick a new id. |
| `task_summary` | Status, completed steps, what it is waiting on, and the last failure. No inputs or outputs. |
| `task_history` | Last 32 events and the revision required by retry. |
| `task_signal` | Send a signal. Reuse the same `operation_id` and value if the response is uncertain. |
| `task_retry` | Resume a failed or cancelled task. Pass `expected_revision` from `task_history`. |
| `feedback` | File a problem. See below. |

These tools do not execute the worker. After `task_submit`, run a worker until the task completes.

Task ids, handler names, signal names, and operation ids are 1-128 ASCII letters, digits, `.`, `_`, or `-`. `.` and `..` are not names.

## Minimal worker

Put each side effect in its own `step`. A retry returns the saved result and does not call the function again.

```python
import os
import threading
from deoos import Client

def charge(ctx, inputs):
    def once():
        # Call the provider with ctx.idempotency_key("charge") so a crash
        # between the call and the commit does not double-charge.
        return {"order_id": inputs["order_id"], "status": "charged"}

    return ctx.step("charge", once)

client = Client.remote(os.environ["ENGINE_URL"])
client.submit("order-1001", "charge", {"order_id": "order-1001"})
client.run_worker({"charge": charge}, stop_event=threading.Event(), poll_interval=0.2)
```

TypeScript is the same shape: `Client.remote`, `submit`, `step`, `runWorker`. Waiting for a person or another system is `ctx.wait_signal("go")` / `waitSignal`. Release the wait with `task_signal` or `python -m deoos signal TASK go true`.

If the worker is killed, start it again with the same handlers. Committed steps stay committed. `task_retry` is only for a task whose status is `failed` or `cancelled`.

## Agent mode

The CLI and MCP server detect `CURSOR_AGENT`, `CLAUDECODE`, or `CODEX`. `DEOOS_AGENT=1` and `--agent` force it. `DEOOS_AGENT=0` forces the human layout.

Agent mode prints one JSON line on stdout and one progress line on stderr (`deoos: inspect task-1`). `explain` is that same JSON, not the multi-line summary. `history` is `{"events":[...],"truncated":false}`. When a list is cut at 100 tasks, or history is at its 32-event cap, the JSON includes `notice` saying what was omitted and how to read a specific id. Older history events are not stored.

## When something fails

The error text says what is wrong and the next command. Common cases:

- `cannot reach engine` — start the server (`make dev`) and export `ENGINE_URL`.
- `authentication required` — set `ENGINE_TOKEN` to the token the server was started with.
- `task ... not found` — `python -m deoos list`, or submit that id.
- `invalid task id` or `invalid name` — rename it to the identifier rule above.
- `only failed or cancelled tasks can be retried` — inspect the status. Waiting work resumes when a worker polls after its signal or timer. Do not retry it.
- `cannot signal a terminal task` — the task already finished. Retry it first if it failed or was cancelled, or submit a new id.
- `task changed since inspection` — read the revision again and retry with that revision.
- `task ID already used with different definition` — resubmit the same handler and inputs, or choose a new id.
- `protocol_version 3 required` — upgrade the SDK so it matches the server.

## Feedback

Use this when the error text is wrong, a tool is missing, or a run failed in a way the docs do not explain. Do not paste secrets, tokens, or user code unless you mean to share them.

```sh
python -m deoos feedback --tried "submit order-1001" --happened "inspect said not found"
```

The MCP tool is `feedback` with `tried` and `happened`. Optional `repro` is included only when set. `include_secrets: true` is the explicit override. No token prints a prefilled GitHub issue URL. `GITHUB_TOKEN` or `GH_TOKEN` files the issue on `deoos-dev/deoos` with the `agent-feedback` label.

# Working on deoos

Agents using deoos, rather than changing this repository, should follow [agents/SKILL.md](agents/SKILL.md). That file is safe to copy into another project. Feedback from a sharp corner is `python -m deoos feedback`; triage is [docs/agent-feedback.md](docs/agent-feedback.md).

Start the engine, run a workflow, and check the result with `python -m deoos`. That CLI is the verification tool.

## Start locally

```sh
make dev
```

This starts RustFS from `compose.yaml`, creates the `deoos-dev` bucket, builds `deoos-server` if needed, and runs it on `127.0.0.1:7331`. Stdout is the environment to export. The same lines are written to `outputs/dev.env`.

Server-mode CLI commands and workers only need:

```sh
export ENGINE_URL=http://127.0.0.1:7331
export PYTHONPATH="$PWD/clients/python"
```

`make dev-down` stops the server and RustFS. The volume stays, so workflow state is still there on the next `make dev`. A second `make dev` does not restart a server that is already listening; it reprints `outputs/dev.env` from that process.

Local RustFS uses the credentials in `compose.yaml`. Ambient AWS keys are not used. `LEASE_MS` (default 30000, minimum 1000) is the recovery lease and can be set when starting: `LEASE_MS=2000 make dev`.

If the CLI is pointed at a server that is not running, it prints `cannot reach engine at <url>; run make dev`.

## Run a workflow

Runnable workflows live in [deoos-examples](https://github.com/deoos-dev/deoos-examples). The order example there can target this server with `DEOOS_MODE=server` and `ENGINE_URL=http://127.0.0.1:7331`.

`python -m deoos submit` and `python -m deoos signal` are the CLI for this checkout. `inspect` shows the public view. `--internal` adds lease and storage fields.

`DEOOS_MODE=library` uses in-process storage and ignores `ENGINE_URL`. `DEOOS_MODE=server` requires `ENGINE_URL`. When `DEOOS_MODE` is unset, `ENGINE_URL` selects server mode.

## Verify a change

Use the CLI against `make dev`. This recipe checkpoints `work`, blocks inside `hold`, survives a killed worker, then waits for signal `go`.

```sh
LEASE_MS=2000 make dev
```

```sh
export ENGINE_URL=http://127.0.0.1:7331
export PYTHONPATH="$PWD/clients/python"
python -m deoos info
python -m deoos submit resume-001 slow '{"marker":"/tmp/deoos-resume.txt"}'
```

Save this worker and run it (same `ENGINE_URL` and `PYTHONPATH`):

```python
import os
import threading
import time
from deoos import Client

def slow(ctx, inputs):
    marker = inputs["marker"]

    def work():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("work\n")
        return "work"

    def hold():
        with open(marker, "a", encoding="utf-8") as handle:
            handle.write("hold\n")
        if open(marker, encoding="utf-8").read().count("hold\n") == 1:
            time.sleep(30)
        return "held"

    ctx.step("work", work)
    ctx.step("hold", hold)
    ctx.wait_signal("go")
    return ctx.step("after", lambda: "done")

Client.remote(os.environ["ENGINE_URL"]).run_worker(
    {"slow": slow}, stop_event=threading.Event(), poll_interval=0.2)
```

Then:

```sh
python -m deoos inspect resume-001
```

When `status` is `running` and `completed_steps` includes `work`, kill the worker. `work` is committed. `hold` is not. Start the worker again. It waits out the lease (2 seconds with `LEASE_MS=2000`, otherwise 30) and continues. The marker file has one `work` line and, once `hold` commits, two `hold` lines.

```sh
python -m deoos inspect resume-001
python -m deoos signal resume-001 go true
python -m deoos inspect resume-001
python -m deoos history resume-001
```

`inspect` should reach `completed` with output `done`. Completed steps are `work`, `hold`, `go` (the signal wait is a checkpoint), and `after`. `history` shows submit, the claim after the kill, the signal, and complete. Use a new task id for another run.

## End-to-end suites

These run the real engine. Docker and Rust are required. `make setup` also installs the test venv and builds native artifacts. Do not run `make test-dev` while you want the dev server on port 7331 left up; that target starts and stops it.

```sh
make setup
make test-local      # tests/two_modes.py, library and server, local RustFS
make test-dev        # python -m deoos submit and signal against make dev
```

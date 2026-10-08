# DEOOS

Durable execution on object storage.

Small, simple, and safe.

Write ordinary functions. Completed steps are remembered; unfinished steps retry after failure. Keep workflow state in your own bucket (S3, Azure Blob, GCS, R2, or self-hosted RustFS), with no database to run.

## Install

Download the package for your platform from [Releases](https://github.com/deoos-dev/deoos/releases). Python 3.10+ and Node 22+ are required for their respective SDKs; no Rust compiler is needed. Choose the archive matching your operating system and CPU. Release notes list the available builds.

0.7.0-alpha.3 includes packages for macOS (Apple Silicon and Intel), Linux (x86_64 and arm64, glibc 2.28+), and Windows x86_64. The 0.6.0 storage layout is incompatible; start with a fresh state directory or execution prefix.

```sh
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install /path/to/release/python/*.whl
# In your Node project:
npm install /path/to/release/node/*.tgz
```

Packages are not yet published to registries. Each release includes both SDKs, the server executable, examples and checksums.

## Quick start

The Hacker News demo currently requires a Mac with Apple Silicon, `uv`, and Docker:

```sh
./examples/hacker-news
```

It starts local RustFS, runs the demo, stops a worker after 20 items, then resumes after lease recovery and checks the saved DuckDB data. See [the example guide](examples/README.md) for details.

On other platforms, install the wheel/tgz for your platform as shown above. From the directory containing `compose.yaml`, start local RustFS:

```sh
docker compose -p deoos up -d
```

Create a `my-workflows` bucket and configure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` using the development credentials in `compose.yaml`. Save the Python snippet below as `hello.py`; for local RustFS, add these lines at the top:

```python
import os
os.environ["AWS_ENDPOINT"] = "http://127.0.0.1:19000"
os.environ["AWS_ALLOW_HTTP"] = "true"
```

```sh
python hello.py
```

Both SDK snippets use library mode. For cloud storage, use your own bucket, region and credentials without the local RustFS settings. The [examples](examples/README.md#deployment) show provider configuration.

### Python

```python
from deoos import Client

with Client(bucket="my-workflows", region="us-east-1") as engine:
    def greet(ctx, inputs):
        return ctx.step("greeting", lambda: f"Hello, {inputs['name']}!")

    engine.submit("greeting-001", "greet", {"name": "World"})
    engine.run_once({"greet": greet})
    print(engine.view("greeting-001"))
```

### TypeScript

```typescript
import { Client } from "deoos";

const engine = new Client({bucket: "my-workflows", region: "us-east-1"});
await engine.submit("greeting-002", "greet", {name: "World"});
await engine.runOnce({greet: async (ctx, inputs) =>
  ctx.step("greeting", () => `Hello, ${inputs.name}!`)
});
console.log(await engine.view("greeting-002"));
```

The task ID identifies the work. Reusing it with the same definition returns the same task; use a new ID for new work. `run_once` / `runOnce` polls once. Use `run_worker` / `runWorker` for a continuous worker with application-controlled shutdown.

## Library mode

The Rust engine runs inside your Python or Node worker. Each worker holds storage credentials and uses the same bucket and execution prefix. No execution server is needed. In the examples, `DEOOS_MODE=library` selects library mode.

Run cloud workers near their object store, or self-hosted workers beside RustFS. For a local trial on Mac Apple Silicon, an experimental filesystem adapter accepts `provider="filesystem"` and a private local APFS directory.

## Server mode

Run `deoos-server` with storage credentials. Workers connect over HTTP/HTTPS and need only the server URL and token:

```sh
DEOOS_STORAGE_BUCKET=my-workflows AWS_REGION=us-east-1 \
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=your-token \
/path/to/release/bin/deoos-server
```

```python
engine = Client.remote("http://127.0.0.1:7331", token="your-token")
```

```typescript
const engine = Client.remote("http://127.0.0.1:7331", "your-token");
```

The workflow APIs are the same in both modes. Use HTTPS through a TLS proxy across machines; non-loopback binding requires a token.

## Features

- **Steps:** named checkpoints return saved results on recovery.
- **Retries and leases:** failed or abandoned work can be claimed again, within its attempt budget. Idempotency keys help destinations deduplicate external effects.
- **Timers and signals:** persist a wait and release the worker; polling resumes ready work.
- **Cancellation:** stops future durable commits, not an already dispatched request or running callback.
- **Schedules:** fixed intervals, pause/resume and bounded backfills. Workers poll to admit due work; no separate scheduler service. No cron expressions or timezone/DST rules.
- **Child workflows:** spawn tasks and join their results across workers.
- **CLI and web UI:** inspect progress, read history, retry or cancel. Open `/ui` on a running server; internal details are separate from the default view.
- **MCP:** `python -m deoos mcp` exposes read-only tools by default; `--allow-actions` enables explicit control tools.

```sh
python -m deoos inspect greeting-001
python -m deoos history greeting-001
python -m deoos inspect greeting-001 --internal
python -m deoos info
```

`info` shows engine health. In server mode it reads the running server’s counters; library-mode CLI commands create a fresh engine. Counters belong to each engine instance and reset when it is recreated. The CLI uses your storage configuration in library mode, or `ENGINE_URL` and `ENGINE_TOKEN` in server mode. See the [paired Python and TypeScript examples](examples/README.md) for runnable workflows.

## Guarantees and limits

- Recovery restarts a function from the top; committed named steps return their saved results. Effects between checkpoints may repeat, so destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)`. Stable task IDs deduplicate submissions only within an execution prefix.
- Workers need synchronized clocks. Lease expiry cannot stop a running callback or dispatched request; conditional writes prevent an expired owner from committing over a replacement owner.
- Keep checkpoint names and handler behavior compatible with active executions. Checkpoint callbacks run sequentially per task; child tasks can run concurrently. Node CPU work must leave time for renewal timers.
- Object-storage requests and retries are bounded per engine, with separate capacity for lease renewals. Overload can return a retryable error; prolonged storage failures can still cause lease expiry. Keep workflow inputs and outputs small; store large datasets separately and pass references.
- There is no production HA, tenant isolation, garbage collection, or application throughput guarantee. This alpha has no backward compatibility or automatic migration. Keep engine and SDK versions matched.
- Retain task state and checkpoint objects while an execution prefix is writable. Stop all writers before retiring a prefix; do not delete live workflow state by age.

## License

Apache-2.0. See LICENSE.

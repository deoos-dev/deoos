# DEOOS

Durable execution on object storage.

Small, simple, and safe. Write ordinary functions. Completed steps are remembered; unfinished steps retry after failure. Keep workflow state in your own bucket (S3, Azure Blob, GCS, R2, or self-hosted RustFS), with no database to run.

## Install

Download the package for your platform from [Releases](https://github.com/deoos-dev/deoos/releases). Python 3.10+ and Node 22+ are required for their respective SDKs; no Rust compiler is needed. The current alpha package is for Mac Apple Silicon. See [Qualification](docs/qualification.md) for platform coverage.

```sh
python3 -m venv .venv
.venv/bin/pip install /path/to/release/python/*.whl
# In your Node project:
npm install /path/to/release/node/*.tgz
```

Packages are not yet published to registries. Each release includes both SDKs, the server executable, examples and checksums.

## Quick start

These examples use library mode. Create a bucket and configure its credentials first. Replace the bucket and region with yours; see [storage configuration](docs/operations.md#storage-configuration) for other providers.

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

Run cloud workers near their object store, or self-hosted workers beside RustFS. For a local trial on Mac Apple Silicon, an experimental filesystem adapter accepts `provider="filesystem"` and a private local APFS directory. See [Operations](docs/operations.md) for storage settings and worker lifecycle details.

## Server mode

Run `deoos-server` with storage credentials. Workers connect over HTTP/HTTPS and need only the server URL and token:

```sh
AWS_BUCKET=my-workflows AWS_REGION=us-east-1 \
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=your-token \
/path/to/release/bin/deoos-server
```

```python
engine = Client.remote("http://127.0.0.1:7331", token="your-token")
```

```typescript
const engine = Client.remote("http://127.0.0.1:7331", "your-token");
```

The workflow APIs are the same in both modes. Use HTTPS through a TLS proxy across machines; non-loopback binding requires a token. See [server deployment](docs/operations.md#server-deployment).

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
```

The CLI uses your storage configuration in library mode, or `ENGINE_URL` and `ENGINE_TOKEN` in server mode. See the [paired Python and TypeScript examples](examples/README.md) and [Operations](docs/operations.md) for details.

## Guarantees and limits

Functions restart from the top on recovery; named steps return committed values. External effects can repeat before a checkpoint commits: destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)` for effect deduplication. Workers must have synchronized clocks. Lease expiry cannot interrupt application code or an already dispatched network request; a replacement owner's conditional state write prevents the stale owner from committing over it. Keep checkpoint names and handler semantics compatible with active executions. Checkpoint callbacks are sequential within a task; child tasks may execute concurrently. Node CPU work must not block renewal timers.

Stable task IDs deduplicate submissions within an execution prefix. They do not make external APIs exactly-once. A destination must honor the supplied idempotency key.

Storage-layout changes require a fresh state directory or execution prefix; this alpha has no backward compatibility or automatic migration. Keep matching engine and SDK packages together.

No production HA, tenant isolation, garbage collection or application throughput guarantee is claimed.

See [Operations](docs/operations.md) for storage layout, discovery, retention and code evolution. See [Qualification](docs/qualification.md) for test coverage, dated measurements and their limits.

## License

Apache-2.0. See LICENSE.

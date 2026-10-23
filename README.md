# DEOOS

deoos is a durable-workflow engine on object storage. It lifts a workflow off an old job runner or a pile of scripts, ties that system to a bucket and to current tools, and runs in one process or beside a server. Add it to a workflow, start a new one, or replace the old one. People operate it with the CLI and `/ui`. An agent should use the same CLI and MCP tools.

Completed steps are remembered. Unfinished steps retry. State stays in your bucket: S3, Azure Blob, GCS, R2, or self-hosted RustFS. No database to run.

[deoos.dev](https://deoos.dev)

## Install

Download the package for your platform from [Releases](https://github.com/deoos-dev/deoos/releases). Python 3.10+ and Node 22+ are required for their respective SDKs; no Rust compiler is needed. Choose the archive matching your operating system and CPU. Release notes list the available builds.

0.7.2 includes packages for macOS (Apple Silicon and Intel), Linux (x86_64 and arm64, glibc 2.28+), and Windows x86_64. The 0.6.0 storage layout is incompatible; start with a fresh state directory or execution prefix.

```sh
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install /path/to/release/python/*.whl
# In your Node project:
npm install /path/to/release/node/*.tgz
```

Packages are not yet published to registries. Each release includes both SDKs, the server executable, examples, and checksums.

## Two minutes

Three local pages, a crash after the first saved page, then a resume. Exit code 86 is the crash, in the example itself. `articles.csv.fetches` shows page 1 was fetched once. The bucket must already exist. [examples/README.md](examples/README.md) has the RustFS environment.

```sh
python examples/pages.py submit --id snap-001 --output "$PWD/articles.csv"
python examples/pages.py work --id snap-001 --crash-after fetch-page-1
python examples/pages.py work --id snap-001
```

If the dead process still holds the lease, the second `work` waits for it to expire. The default is 30 seconds.

## Examples

| Example | What one run shows |
| --- | --- |
| `examples/pages.py` | Page checkpoints, a visible crash, and resume. Start here. |
| `examples/charge.py` | Approve, then one charge. Crash after the call and before the checkpoint. |
| `examples/footprint.py` | One process and one bucket. Kill and resume. Fail, explain, retry, and cancel. Optional server and `/ui`. |
| `examples/ingest.py` | Interval fan-out. Each tick reads `scheduled_at`. Pause and backfill. |
| `examples/workflow_python.py`, `examples/workflow_typescript.mjs` | One order. Either language can resume it. |
| `examples/hacker-news` | Optional larger demo: live Hacker News into DuckDB. The crash is in the Docker harness. |
| `examples/devto-etl`, `examples/document-index`, `examples/payment-resume`, `examples/use_cases.py` | Older demos. `charge.py` replaces the payment harness as the example to run. `library_python.py` and `library_typescript.mjs` are the short greetings. |

## For agents

Use the CLI. Do not build a second orchestrator.

```sh
python -m deoos inspect TASK_ID
python -m deoos explain TASK_ID
python -m deoos retry TASK_ID
python -m deoos cancel TASK_ID
python -m deoos mcp
```

`mcp` is read-only unless you pass `--allow-actions`. `inspect` prints the public view. `--internal` adds lease and storage fields.

`DEOOS_MODE=library` uses in-process storage and ignores `ENGINE_URL`, so a leftover URL does not 404 the CLI. `DEOOS_MODE=server` requires `ENGINE_URL`. When `DEOOS_MODE` is unset, `ENGINE_URL` selects server mode and its absence selects library mode.

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

## Idempotency across long pauses

Completed steps are remembered in your bucket, so replay returns their saved results without calling the API again.
Inside a step, pass `ctx.idempotency_key("charge")` / `ctx.idempotencyKey("charge")` to the external API.
That key stays the same across retries and resumes and covers the gap between the call and its committed result, while the provider retains the key.
After the provider's key window expires, an unfinished step in that gap needs a reconciliation check before calling again.
Use globally unique execution IDs when sharing an external account; step keys do not include the bucket or prefix.
Keep saved workflow state for the full pause. `python examples/charge.py` is that crash, against a local sink.

## Guarantees and limits

- Recovery restarts a function from the top; committed named steps return their saved results. Effects between checkpoints may repeat, so destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)`. Stable task IDs deduplicate submissions only within an execution prefix.
- Workers need synchronized clocks. Lease expiry cannot stop a running callback or dispatched request; conditional writes prevent an expired owner from committing over a replacement owner.
- Keep checkpoint names and handler behavior compatible with active executions. Checkpoint callbacks run sequentially per task; child tasks can run concurrently. Node CPU work must leave time for renewal timers.
- Object-storage requests and retries are bounded per engine, with separate capacity for lease renewals. Overload can return a retryable error; prolonged storage failures can still cause lease expiry. Keep workflow inputs and outputs small; store large datasets separately and pass references.
- There is no production HA, tenant isolation, garbage collection, or application throughput guarantee. There is no backward compatibility or automatic migration. Keep engine and SDK versions matched.
- Retain task state and checkpoint objects while an execution prefix is writable. Stop all writers before retiring a prefix; do not delete live workflow state by age.

## License

Apache-2.0. See LICENSE.

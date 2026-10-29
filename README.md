# DEOOS

[![Build and test](https://github.com/deoos-dev/deoos/actions/workflows/build.yml/badge.svg)](https://github.com/deoos-dev/deoos/actions/workflows/build.yml)

Durable execution on object storage.

Write ordinary functions that recover from failures. DEOOS saves completed steps and retries unfinished work, keeping workflow state in object storage—S3, Azure Blob, GCS, R2, or self-hosted RustFS. No database to manage.

[deoos.dev](https://deoos.dev)

## Install

Release [0.7.2](https://github.com/deoos-dev/deoos/releases/tag/0.7.2) is the current published build. Python 3.10+ and Node 22+ are required for the SDKs. A Rust compiler is not required.

`deoos` 0.7.2 is published as GitHub release archives. PyPI has no `deoos` package, and the npm registry has no `deoos` package. `pip install deoos` and `npm install deoos` do not install this project. Install the wheel and the Node tarball from the archive that matches your operating system and CPU.

| Archive | Python wheel | Server | Node package |
| --- | --- | --- | --- |
| `deoos-0.7.2-linux-x64.tar.gz` | `deoos-0.7.2-py3-none-manylinux_2_28_x86_64.whl` | `bin/deoos-server` | `node/deoos-0.7.2.tgz` (`os=linux`, `cpu=x64`) |
| `deoos-0.7.2-linux-arm64.tar.gz` | `deoos-0.7.2-py3-none-manylinux_2_28_aarch64.whl` | `bin/deoos-server` | `node/deoos-0.7.2.tgz` (`os=linux`, `cpu=arm64`) |
| `deoos-0.7.2-macos-arm64.tar.gz` | `deoos-0.7.2-py3-none-macosx_11_0_arm64.whl` | `bin/deoos-server` | `node/deoos-0.7.2.tgz` (`os=darwin`, `cpu=arm64`) |
| `deoos-0.7.2-macos-x64.tar.gz` | `deoos-0.7.2-py3-none-macosx_11_0_x86_64.whl` | `bin/deoos-server` | `node/deoos-0.7.2.tgz` (`os=darwin`, `cpu=x64`) |
| `deoos-0.7.2-windows-x64.tar.gz` | `deoos-0.7.2-py3-none-win_amd64.whl` | `bin/deoos-server.exe` | `node/deoos-0.7.2.tgz` (`os=win32`, `cpu=x64`) |

Linux packages need glibc 2.28 or newer. Release 0.7.2 has no Windows arm64 archive. Each Node tarball is built for one platform, and npm rejects a tarball from another operating system or CPU. The archive also contains `LICENSE`, `README.md`, `compose.yaml`, `SHA256SUMS`, and an `examples/` directory. Checksums for the archives are in [`SHA256SUMS`](https://github.com/deoos-dev/deoos/releases/download/0.7.2/SHA256SUMS).

### Linux and macOS

```sh
set -euo pipefail
VERSION=0.7.2
BASE="https://github.com/deoos-dev/deoos/releases/download/${VERSION}"
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) target=linux-x64 ;;
  Linux-aarch64|Linux-arm64) target=linux-arm64 ;;
  Darwin-arm64) target=macos-arm64 ;;
  Darwin-x86_64) target=macos-x64 ;;
  *) echo "No ${VERSION} build for $(uname -s) $(uname -m). Shipped: linux-x64, linux-arm64, macos-arm64, macos-x64, windows-x64." >&2; exit 1 ;;
esac
archive="deoos-${VERSION}-${target}.tar.gz"
curl -fL --retry 3 -o "$archive" "$BASE/$archive"
curl -fsSL --retry 3 -o SHA256SUMS "$BASE/SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
  grep "  ${archive}\$" SHA256SUMS | sha256sum -c -
else
  grep "  ${archive}\$" SHA256SUMS | shasum -a 256 -c -
fi
rm -rf "deoos-${VERSION}-${target}"
tar -xzf "$archive"
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --disable-pip-version-check "deoos-${VERSION}-${target}/python/"*.whl
"deoos-${VERSION}-${target}/bin/deoos-server" --version
python -m deoos --help
mkdir -p deoos-node-app
(
  cd deoos-node-app
  npm init -y
  npm install "$PWD/../deoos-${VERSION}-${target}/node/deoos-0.7.2.tgz"
  node --input-type=module -e "import { Client } from 'deoos'; console.log(typeof Client)"
)
```

### Windows x64 (PowerShell)

```powershell
$Version = "0.7.2"
$Target = "windows-x64"
$Archive = "deoos-$Version-$Target.tar.gz"
$Base = "https://github.com/deoos-dev/deoos/releases/download/$Version"
$ErrorActionPreference = "Stop"
Invoke-WebRequest -Uri "$Base/$Archive" -OutFile $Archive
Invoke-WebRequest -Uri "$Base/SHA256SUMS" -OutFile SHA256SUMS
$line = Select-String -Path SHA256SUMS -Pattern ([regex]::Escape($Archive)) | Select-Object -First 1
if (-not $line) { throw "SHA256SUMS has no entry for $Archive" }
$expected = ($line.Line -split '\s+')[0].ToLower()
$actual = (Get-FileHash -Algorithm SHA256 -Path $Archive).Hash.ToLower()
if ($actual -ne $expected) { throw "checksum mismatch for $Archive" }
if (Test-Path "deoos-$Version-$Target") { Remove-Item -Recurse -Force "deoos-$Version-$Target" }
tar -xzf $Archive
python -m venv .venv
.\.venv\Scripts\python -m pip install --disable-pip-version-check (Get-Item "deoos-$Version-$Target\python\*.whl").FullName
& ".\deoos-$Version-$Target\bin\deoos-server.exe" --version
.\.venv\Scripts\python -m deoos --help
New-Item -ItemType Directory -Force deoos-node-app | Out-Null
Push-Location deoos-node-app
npm init -y
npm install (Resolve-Path "..\deoos-$Version-$Target\node\deoos-0.7.2.tgz")
node --input-type=module -e "import { Client } from 'deoos'; console.log(typeof Client)"
Pop-Location
```

The 0.7.2 wheel's CLI commands are `list`, `info`, `mcp`, `inspect`, `history`, `summary`, `explain`, `retry`, `cancel`, and `schedule`. That wheel selects server mode when `ENGINE_URL` is set and library mode when it is absent. It does not read `DEOOS_MODE`. `submit` and `signal` are in this repository's CLI, described under Develop.

## Quick start

Runnable examples are in [deoos-examples](https://github.com/deoos-dev/deoos-examples). From a clone of that repository:

```sh
docker compose run --build --rm hacker-news
```

Docker is the only prerequisite. The container installs deoos, starts a worker against local RustFS, and checks that a saved result survives a stopped worker.

For your own application, use the wheel or Node package from Install. These snippets use library mode with your bucket and cloud credentials.

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

The Rust engine runs inside your Python or Node worker. Each worker holds storage credentials and uses the same bucket and execution prefix. No execution server is needed. `DEOOS_MODE=library` selects library mode.

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

`info` shows engine health. In server mode it reads the running server’s counters; library-mode CLI commands create a fresh engine. Counters belong to each engine instance and reset when it is recreated. This repository's CLI reads `DEOOS_MODE`. `library` uses in-process storage and ignores `ENGINE_URL`. `server` requires `ENGINE_URL`. With `DEOOS_MODE` unset, `ENGINE_URL` selects server mode and its absence selects library mode. Runnable workflows for both SDKs are in [deoos-examples](https://github.com/deoos-dev/deoos-examples).

## Idempotency across long pauses

Completed steps are remembered in your bucket, so replay returns their saved results without calling the API again.
Inside a step, pass `ctx.idempotency_key("charge")` / `ctx.idempotencyKey("charge")` to the external API.
That key stays the same across retries and resumes and covers the gap between the call and its committed result, while the provider retains the key.
After the provider's key window expires, an unfinished step in that gap needs a reconciliation check before calling again.
Use globally unique execution IDs when sharing an external account; step keys do not include the bucket or prefix.
Keep saved workflow state for the full pause. The payment demo in [deoos-examples](https://github.com/deoos-dev/deoos-examples) replays a saved charge after that window: `docker compose run --build --rm payment-resume`.

## Guarantees and limits

- Recovery restarts a function from the top; committed named steps return their saved results. Effects between checkpoints may repeat, so destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)`. Stable task IDs deduplicate submissions only within an execution prefix.
- Workers need synchronized clocks. Lease expiry cannot stop a running callback or dispatched request; conditional writes prevent an expired owner from committing over a replacement owner.
- Keep checkpoint names and handler behavior compatible with active executions. Checkpoint callbacks run sequentially per task; child tasks can run concurrently. Node CPU work must leave time for renewal timers.
- Object-storage requests and retries are bounded per engine, with separate capacity for lease renewals. Overload can return a retryable error; prolonged storage failures can still cause lease expiry. Keep workflow inputs and outputs small; store large datasets separately and pass references.
- There is no production HA, tenant isolation, garbage collection, or application throughput guarantee. There is no backward compatibility or automatic migration. Keep engine and SDK versions matched.
- Retain task state and checkpoint objects while an execution prefix is writable. Stop all writers before retiring a prefix; do not delete live workflow state by age.

## Develop

From a clone of this repository, with Docker and Rust:

```sh
make dev
```

`make dev` starts RustFS from `compose.yaml`, creates the `deoos-dev` bucket, builds `deoos-server`, and listens on `127.0.0.1:7331`. Stdout is the environment to export. The same lines are written to `outputs/dev.env`. `make dev-down` stops the server and RustFS and keeps the volume.

```sh
export ENGINE_URL=http://127.0.0.1:7331
export PYTHONPATH="$PWD/clients/python"
python -m deoos info
python -m deoos submit TASK_ID HANDLER '{"key":"value"}'
python -m deoos signal TASK_ID SIGNAL_NAME true
```

`submit` and `signal` belong to this checkout. A refused connection prints `cannot reach engine at <url>; run make dev`. `AGENTS.md` is the kill-and-resume check for this server. `make test-dev` runs that check against the real engine.

## License

Apache-2.0. See LICENSE.

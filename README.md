# DEOOS — Durable Execution on Object Storage

Pronounced **D-E-O-O-S**. One Rust execution core, Python and TypeScript SDKs, and S3-compatible storage for authoritative state. Two deployment options:

| Mode | What runs | Storage credentials |
| --- | --- | --- |
| Library | Rust inside your Python or Node process | Each worker |
| Shared server | Rust service; workers connect over HTTP/HTTPS | Service only |

## Install

Download and unpack the release for your platform. Python 3.10+ and Node 22+ are required for their respective SDKs. No Rust compiler is needed to use the prebuilt release.

| Release target | Operating requirements | Runtime verification |
| --- | --- | --- |
| `macos-arm64` | macOS 11+; Apple Silicon | Both modes and SDKs on this Mac |
| `macos-x64` | macOS 11+; Intel-compatible Python/Node | Both modes and SDKs through Rosetta |
| `linux-arm64` | ARM64 Linux, glibc 2.28+ | Both modes and SDKs in ARM Linux |
| `linux-x64` | x86_64 Linux, glibc 2.28+ | Both modes and SDKs in emulated x86_64 Linux |
| `windows-x64` | x86_64 Windows | Package built; Windows runtime validation pending |

Linux releases use audited `manylinux_2_28` wheels; Alpine/musl is outside these builds. Choose a Python/Node distribution compatible with your operating system. Intel macOS and x86_64 Linux tests exercise the target binaries on an ARM host; they are not separate physical-machine tests.

```sh
python3 -m venv .venv
.venv/bin/pip install /path/to/release/python/*.whl
# Run in your application's Node project:
npm install /path/to/release/node/*.tgz
```

On Windows, create the environment with `python -m venv .venv` and install the wheel using `.venv\Scripts\python.exe -m pip install C:\path\to\release\python\WHEEL_FILENAME.whl`. Install the npm tarball in your Node project with `npm install C:\path\to\release\node\deoos-0.6.0.tgz`.

Releases contain an installable Python wheel, npm tarball, optional server executable, application examples, this guide, and checksums. Packages are not yet published to registries.

## Library mode

Configure `AWS_BUCKET`, `AWS_REGION`, and AWS credentials in the environment. Temporary credentials need `AWS_SESSION_TOKEN` too. Explicit storage settings can also be passed to Client. Each execution worker uses the same bucket and prefix.

```python
from deoos import Client

with Client(bucket="my-workflows", region="us-east-1") as engine:
    def greet(ctx, inputs):
        return ctx.step("greeting", lambda: f"Hello, {inputs['name']}!")

    engine.submit("greeting-001", "greet", {"name": "World"})
    engine.run_once({"greet": greet})
```

```typescript
import { Client } from "deoos";

const engine = new Client({bucket: "my-workflows", region: "us-east-1"});
await engine.submit("greeting-002", "greet", {name: "World"});
await engine.runOnce({greet: async (ctx, inputs) =>
  ctx.step("greeting", () => `Hello, ${inputs.name}!`)
});
```

`run_once`/`runOnce` executes one available task. Your application decides when to poll again. Rust runs inside the worker; no execution server is started.

## Shared-server mode

Start one service with storage credentials configured:

```sh
AWS_BUCKET=my-workflows AWS_REGION=us-east-1 \
ENGINE_BIND=0.0.0.0:7331 ENGINE_TOKEN=your-token \
/path/to/release/bin/deoos-server
```

Use HTTPS through a TLS proxy across machines. Non-loopback binding requires a token. Workers need only the service URL and token:

```python
engine = Client.remote("https://execution.example", token="your-token")
```

```typescript
const engine = Client.remote("https://execution.example", "your-token");
```

Handler registration, submission and checkpoint APIs are identical in both modes. The Python and TypeScript SDKs report API failures as `EngineError`, with a `status` field.

The Windows server executable is `bin/deoos-server.exe`. Set the same configuration variables in PowerShell using `$env:AWS_BUCKET = "my-workflows"` and the other variables above, then invoke that executable. The Windows native artifacts link the C runtime statically; their inspected DLL imports are operating-system libraries.

## Composed workflows

```python
def pipeline(ctx, inputs):
    left = ctx.spawn("left", "transform.v1", inputs["left"])
    right = ctx.spawn("right", "transform.v1", inputs["right"])
    return ctx.join("parts", [left, right])
```

TypeScript uses `await ctx.spawn(...)` and `await ctx.join(...)`. Register the child handler alongside the parent, or in separate workers. Children have deterministic IDs; replay validates their original definitions. A pending join persists its dependencies and releases the worker. Polling resumes it when all children are terminal, without consuming another retry attempt. An uncaught `ChildFailed` terminates the parent; catch that specific exception to handle or compensate a child failure. Keep child/step names distinct and stable. JSON inputs are shared across languages; represent integers outside JavaScript’s exact range as strings when crossing SDKs. Suspension stops further durable boundaries even if application code catches its control-flow exception; it cannot interrupt arbitrary application code after a catch.

## Complete application example

`examples/workflow_python.py` and `examples/workflow_typescript.mjs` implement the same order workflow: validation and pricing children, a join, a durable timer, approval through a signal, and a checkpointed result. Install the SDKs into your Python environment and Node project, then copy both examples into that project. Configure storage as above for `DEOOS_MODE=library`, or set `DEOOS_MODE=server`, `ENGINE_URL` and `ENGINE_TOKEN` for a running shared server.

```sh
python workflow_python.py submit --id order-demo --quantity 2 --unit-price 25
node workflow_typescript.mjs work
```

The worker polls until stopped with Ctrl-C. Inspect from another terminal:

```sh
python workflow_python.py inspect --id order-demo
```

Once the order is waiting for its `approved` signal, stop the worker. In server mode you can stop and restart the server with the same bucket and prefix too. Approve the order and start a replacement worker, using either language:

```sh
python workflow_python.py approve --id order-demo
python workflow_python.py work
```

Inspection will show `completed` with `total: 50` and `approved: true`. Prices are integer units; the example calls no payment service. The paired implementations use identical handlers and checkpoint definitions so either language can resume the same execution. A task failure is reported without stopping the continuous polling loop; `work --once` exits after one poll and reports errors. Use a fresh order ID for a different input or approval decision.

## Timers, signals and cancellation

```python
def approval(ctx, inputs):
    ctx.sleep("cooldown", 5000)              # milliseconds; no sleeping worker required
    return ctx.wait_signal("approval")

engine.signal("order-001", "approval", {"approved": True})
engine.cancel("order-002")
```

TypeScript uses `await ctx.sleep(...)`, `await ctx.waitSignal(...)`, `await engine.signal(...)`, and `await engine.cancel(...)`. Timers keep their first deadline across replay. Named signals are single-assignment: an identical value acknowledges a retry; a different value conflicts. A signal may arrive before the workflow waits. Waits release ownership; polling claims ready tasks without spending a retry attempt. There is no resident timer or notification service, so polling frequency determines wakeup latency. Cancellation stops future durable commits and work at subsequent SDK boundaries; it cannot forcibly interrupt an already running callback or dispatched request. Cancel child tasks explicitly when needed.

## Recurring work and backfills

```python
engine.schedule("daily-import", "import.v1", {}, interval_ms=86_400_000,
                first_due_ms=1780272000000, missed="latest", overlap="skip")
engine.pause_schedule("daily-import")
engine.backfill("daily-import", start_ms=1780272000000,
                end_ms=1780531200000, limit=3)
engine.resume_schedule("daily-import")
```

TypeScript uses `engine.schedule(id, handler, inputs, interval_ms, {first_due_ms, missed, overlap})`, `pauseSchedule`, `resumeSchedule`, `inspectSchedule` and `backfill(id, start_ms, end_ms, limit)`. Schedule definitions are immutable; changing the handler, cadence or inputs requires a new schedule ID. Omitting `first_due_ms` anchors the first occurrence at original creation time.

Cadence uses fixed UTC intervals, without cron expressions or timezone/DST rules. `latest` emits the most recent due occurrence after downtime; `catchup` emits historical occurrences in order. `overlap="skip"` prevents concurrent automatic runs: latest mode skips periods while work is active; catchup waits and preserves its backlog. `allow` permits distinct occurrences to overlap. Backfills always preserve their requested historical occurrences, waiting for active work when overlap prevention is enabled.

Backfill ranges are `[start_ms, end_ms)`, aligned to the cadence and at or after the schedule's first occurrence. The entire range must be historical and fit the requested limit, at most 1,000 occurrences, before acceptance. Only one backfill range can be active per schedule. Polling resumes its persisted cursor after process restarts. Recurring and backfill runs share deterministic IDs, so an already created occurrence is reused. A cleared backfill cursor means all its tasks were submitted; inspect the tasks to confirm they finished.

`run_once`/`runOnce` ticks schedules for its registered handlers before claiming work. At least one worker must poll for new runs to appear; there is no separate scheduler service. Each poll processes at most ten schedules, with random rotation. Discovery still reads every schedule, so read cost grows with the namespace. Pause stops new reservations; an already reserved occurrence may finish being submitted. Existing tasks remain explicit cancellation targets.

## Build and verify

Source builds need Rust 1.93+, a C compiler, Node 22+ and Python 3.10+. Windows native builds require MSVC build tools. Docker Compose is needed for local RustFS tests. From the repo on macOS/Linux:

```sh
export PYTHON=python3.12     # choose an installed Python 3.10+
make setup
make test-local
AWS_PROFILE=my-profile make test-aws  # choose an AWS profile
make package
make test-package
make clean-local
```

Local tests use pinned RustFS. AWS tests create dedicated temporary buckets and delete them. Reports and release artifacts go to `../outputs`, outside the repo. The behavioral suites exercise both SDK modes, concurrency, stale ownership, restart recovery, mutation replay and storage-response faults. Package tests install the wheel and npm tarball in fresh projects, run the four simple examples and four cross-language workflow cases, verify process identities, and recover across worker/server replacement.

`python packaging/build_package.py` builds and packages the host target. `--target macos-x64` (or another label above) selects a Rust cross-target; install the appropriate Rust target, linker and system SDK first. `--package-only --target TARGET` packages matching artifacts already built under Cargo's target-specific directories. Cross-compilation alone is not runtime validation. Linux source builds produce generic Linux wheels by default; the release builders used pinned manylinux 2.28 environments and checked all three native artifacts' dependencies and GLIBC symbols before enabling `DEOOS_LINUX_PLATFORM=manylinux_2_28_aarch64` or `manylinux_2_28_x86_64`.

GitHub Actions runs checks and the RustFS behavioral suite on Linux for pull requests, and adds a five-platform package matrix on manual runs and `v*` tags. Each platform builds its release, installs both SDKs in fresh projects, tests library and shared-server workflows against real S3, and uploads packages only after passing. Linux builds retain the audited manylinux 2.28 baseline. Configure repository variables `AWS_ROLE_ARN` and `AWS_ACCOUNT_ID` with a dedicated GitHub OIDC role and its expected AWS account ID. Restrict the role to DEOOS test buckets; no AWS access-key secrets are needed. Test reports include exact bucket names for cleanup. Local AWS tests use the standard credential chain; set `AWS_PROFILE` to choose an account explicitly.

For a local application configure `AWS_ENDPOINT=http://127.0.0.1:19000`, `AWS_ALLOW_HTTP=true`, region `us-east-1`, and the development credentials in `compose.yaml`; create a bucket with an S3 tool. Docker runs storage. `EXECUTION_PREFIX` defaults to `durable-v3`; `LEASE_MS` defaults to 30000. Native-library overrides are `DEOOS_NATIVE_LIBRARY` and `DEOOS_NODE_LIBRARY`.

## Guarantees and current scope

Each task has `<prefix>/tasks/<id>/state.json`. Checkpoint results are immutable objects under `<prefix>/tasks/<id>/results/<step>/<generation>/<operation_id>.json`. State creation uses If-None-Match; replacements use If-Match. A checkpoint is committed when its result reference is in state; orphan uploads are ignored. ETag conditions fence old generations. A successful write with a lost response is reconciled by its unique revision. Repeated mutation acknowledgment is bounded to the last operation and requires an identical request.

Timer deadlines and waiting conditions live in the task state. Signal payloads live at `<prefix>/tasks/<id>/signals/<name>/<operation_id>.json`; assignment commits when the state references that payload. Suspension and signal assignment share the same conditional state object, preventing lost wakeups.

Schedules use `<prefix>/schedules/<id>/state.json`. A conditional update records a pending occurrence, then creates its deterministic task, then advances the cursor and clears the pending intent. Any poller can complete a persisted intent after a crash. Task `schedule` metadata records its schedule ID and scheduled epoch timestamp; application inputs remain unchanged.

## Operating executions

`client.inspect(id)` reads a task; Python `list_tasks()` / TypeScript `listTasks()` returns the first 100 task IDs in lexicographic order and a `truncated` flag. Use direct ID lookup for other tasks. This bounds returned state reads, not S3 object enumeration; discovery remains linear. Task state retains the last 32 meaningful history events. Python/TypeScript `ctx.log(message)` adds an owned event of up to 4096 UTF-8 bytes; process stdout is separate. Logs outside checkpoints may repeat after recovery. History is a bounded diagnostic view, not a complete audit log.

Manual retry accepts only failed or cancelled tasks:

```python
state = client.inspect("invoice-123")
client.retry("invoice-123", state["revision"], operation_id="operator-retry-123")
```

TypeScript uses `client.retry(id, state.revision, operationId)`. Retry preserves checkpoints, declared definitions, timers, signals and child IDs, resets attempts to zero and queues immediately. A changed revision conflicts. Resend the same operation ID and original revision after an uncertain response: the most recent retry receipt remains acknowledged even after a worker advances. For CLI resends, supply both the original `--operation-id` and `--revision`; do not let the revision default to a new inspection. Older receipts are not retained indefinitely; never reuse an operation ID for a new action. Preserved compensation/join checkpoints are not rerun, and a parent may require its own retry. Manual retry of a scheduled task overrides automatic overlap policy and does not advance the schedule cursor.

The Python package provides an operator CLI. It uses `ENGINE_URL`/`ENGINE_TOKEN` for a shared server; with no `ENGINE_URL`, it opens the embedded engine using the usual S3 environment:

```sh
python -m deoos list
python -m deoos inspect invoice-123
python -m deoos retry invoice-123 --revision OBSERVED_REVISION --operation-id operator-retry-123
python -m deoos cancel invoice-123
python -m deoos schedule pause daily-import
```

For the UI, start the shared server and open `http://127.0.0.1:7331/ui` (or your configured address). Enter its token and connect. Task listing, ID lookup, history, manual retry/cancel, schedule inspection and pause/resume use the same API. Token storage is confined to page memory and refresh is manual. The UI shell is public; API access follows server authentication. Browser API requests must come from the server’s own origin; HTTP mutations require `application/json`. Origin-less SDK/CLI requests are supported. Library users can run the shared server against the same bucket/prefix for inspection, using the existing second deployment mode.

## Discovery and request costs

Discovery has no separate index: an idle claim lists schedules, reads their state, then lists task objects and reads every task state, including completed tasks. Checkpoint objects add LIST pages but are not read during an idle claim. Each worker pays these reads on every poll. Task inspection by ID reads one state object; operator listing returns at most 100 states but enumerates all task objects first.

Measured on macOS ARM64 with the 0.5 engine and local RustFS, three samples per row gave identical request counts in library and shared-server claim tests:

| Idle namespace | LIST per completed claim | GET per completed claim |
| --- | ---: | ---: |
| Empty | 2 | 0 |
| 100 completed tasks, no checkpoints | 2 | 100 |
| 1,000 completed tasks, no checkpoints | 2 | 1,000 |
| 1,000 completed tasks, four checkpoints each (5,000 objects) | 6 | 1,000 |
| 100 future schedules, no tasks | 2 | 110 |

The last row includes another read for each of the ten schedules selected for ticking. Listing the 5,000-object task namespace used five LISTs and 100 GETs, returned 100 tasks and reported truncation. Fixtures were seeded copies of actual completed task state, with 1 KiB checkpoint values; these are discovery measurements, not application throughput benchmarks.

For S3 Standard in us-east-1, the published rates are $0.005 per 1,000 LIST/PUT requests, $0.0004 per 1,000 GET/HEAD requests and $0.023 per GB-month of storage. See [AWS request rates](https://aws.amazon.com/blogs/storage/run-spark-31-faster-and-optimize-compute-costs-with-amazon-s3-express-one-zone-on-amazon-emr/) and [S3 pricing](https://aws.amazon.com/s3/pricing/). Verify current regional rates when deploying.

Request cost is `(LIST + PUT) × 0.005 / 1000 + (GET + HEAD) × 0.0004 / 1000`. As a scenario, 1,440 completed idle polls per day against the 5,000-object fixture cost `1440 × (6 × 0.005 + 1000 × 0.0004) / 1000 = $0.6192/day` in requests. Ten workers doing that many polls each multiply this by ten. This assumes completed polls, not a guaranteed polling frequency, and excludes application work, transfer, free-tier credits and other services. The fixture occupied 6,396,000 bytes; using 1 GB = 2^30 bytes, a month of storage is `6396000 / 1073741824 × 0.023`, about $0.00014. Polling can dominate storage costs.

Run the opt-in probe with the test Python environment after a build:

```sh
tests/.venv/bin/python tests/discovery_probe.py rustfs --mode server --case tasks-100-results-0
AWS_PROFILE=my-profile tests/.venv/bin/python tests/discovery_probe.py aws --mode server --transport direct --case tasks-100-results-0
```

Each run creates and removes a dedicated test bucket. Reports remain under `../outputs/evidence`. Counted AWS runs re-sign traffic through a local forwarding proxy that opens a fresh TLS connection per request; those timings include instrumentation overhead. Direct runs bypass that proxy, retain the SDK's normal ten-second server request timeout, and record timeouts as observations. A report's `success` means the probe finished recording and cleaning up; inspect each case's completed samples and errors for actual results.

Direct AWS measurements from this macOS ARM64 host to us-east-1, with the unchanged 0.5 engine and Python shared-server client, were:

| Idle namespace | Completed samples | Median completed scan |
| --- | ---: | ---: |
| Empty | 3 / 3 | 113 ms |
| 100 completed tasks, no checkpoints | 3 / 3 | 7,282 ms |
| 1,000 completed tasks, four checkpoints each | 0 / 3 | Timed out at about ten seconds |

These observed timings include this host's network round trips; they are not universal task-count limits or measurements of an AWS-hosted deployment. Sequential state reads make accumulated terminal tasks a practical limit. This alpha suits small namespaces; sustained growth needs improved discovery. There is no automatic pruning or separate index. Increasing the client timeout does not remove the linear request cost. Measure your deployment before relying on a polling cadence.

## Code evolution

Use handler names such as `process_order.v1` and `process_order.v2`. A task's handler is immutable. Register both versions while old executions finish; submit new work to the new version. A worker registering only `.v2` cannot claim `.v1` tasks.

Every named operation declares its kind and definition before reading a checkpoint or doing work. Spawn inputs, ordered join dependencies and timer durations must match their original definitions. Ordinary callbacks declare a revision: `ctx.step("charge", charge, revision="1")` in Python, or `ctx.step("charge", charge, "1")` in TypeScript. Change the handler version and declared revision when callback semantics or result shape change. The engine verifies declared compatibility; it cannot detect arbitrary changes inside your code. A mismatch fails before consuming a cached value or executing the callback. It follows the configured failure/retry policy and can spend attempts; committed values remain available.

Version 0.6 continues the storage protocol 3 introduced by 0.4 and the default prefix `durable-v3`. Earlier 0.2/protocol-1 and 0.3/protocol-2 alphas cannot be mixed with the new engine. Keep their engine and SDK packages on their original prefixes until those executions drain, then use 0.6 with a fresh prefix. Mutating SDK calls check engine protocol before dispatch, and the engine rejects unsupported clients and stored tasks before changing them. Keep each service URL on one protocol version throughout its deployment. Unknown task fields are preserved for future metadata additions. History recording introduced by 0.5 remains compatible with 0.6; 0.4 preserves new metadata but does not emit history events.

Functions restart from the top on recovery; named steps return committed values. External effects can repeat before a checkpoint commits: destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)` for effect deduplication. Workers must have synchronized clocks. Lease expiry cannot interrupt application code or an already dispatched network request; a replacement owner's conditional state write prevents the stale owner from committing over it. Keep checkpoint names and handler semantics compatible with active executions. Checkpoint callbacks are sequential within a task; child tasks may execute concurrently. Node CPU work must not block renewal timers.

This alpha supports claims, leases, checkpoints, retries, recovery, child-task composition, timers, signals, cancellation, declared code/checkpoint compatibility and interval schedules/backfills. Task inspection, bounded history, manual retry, a small shared-server UI, paired complete application examples and measured discovery/request costs are included. No production HA, tenant isolation, garbage collection or application throughput guarantee is claimed.

macOS Apple Silicon remains the primary development platform. The portability phase has verified both modes and SDKs for all four macOS/Linux targets above. Windows packaging is implemented; executing its candidate on a real Windows runtime remains the final platform proof.

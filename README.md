# DEOOS - Durable Execution On Object Storage

One Rust execution core, Python and TypeScript SDKs, and object storage for authoritative state. Two deployment options:

| Mode | What runs | Storage credentials |
| --- | --- | --- |
| Library | Rust inside your Python or Node process | Each worker |
| Shared server | Rust service; workers connect over HTTP/HTTPS | Service only |

## Install

Download and unpack the release for your platform. Python 3.10+ and Node 22+ are required for their respective SDKs. No Rust compiler is needed to use the prebuilt release.

| Release target | Operating requirements | Runtime verification |
| --- | --- | --- |
| `macos-arm64` | macOS 11+; Apple Silicon | Both modes and SDKs on native ARM64 macOS CI |
| `macos-x64` | macOS 11+; Intel-compatible Python/Node | Both modes and SDKs on native x86_64 macOS CI |
| `linux-arm64` | ARM64 Linux, glibc 2.28+ | Both modes and SDKs on native ARM64 Linux CI |
| `linux-x64` | x86_64 Linux, glibc 2.28+ | Both modes and SDKs on native x86_64 Linux CI |
| `windows-x64` | x86_64 Windows | Both modes and SDKs on Windows Server 2022 CI |

Linux releases use audited `manylinux_2_28` wheels; Alpine/musl is outside these builds. Choose a Python/Node distribution compatible with your operating system. All five targets passed fresh-package installation and cross-language recovery tests against real S3 on their native architectures. Linux tests run inside pinned manylinux containers. The macOS 11 deployment target and glibc 2.28 baseline are build requirements; CI does not test every supported OS version.

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

AWS S3, RustFS, Cloudflare R2, Google Cloud Storage and Azure Blob Storage are the tested storage backends. R2, GCS and Azure each passed the conditional-write contract and all 44 workflow checks in embedded and shared-server modes using the macOS ARM64 CI package and the default 30-second lease. These were local Mac tests against actual cloud storage, not cloud-local load qualification. Azure passed with unchanged SDK timeouts; an earlier restart timeout remains unexplained. RustFS is the recommended self-hosted target; distributed deployment and air-gapped operation still need separate qualification. Every supported backend must pass the same execution behavior tests.

The discovery changes at `66b43da` were requalified against GCS, Azure and R2 using the macOS ARM64 package from CI run `37129914969`. Each passed all 44 workflow checks plus four separate warm-cache test blocks across both modes, with temporary resources and generated credential files removed. These checks use a Mac against actual cloud storage; the same-region EC2/S3 comparison below separately measures cloud-hosted workers.

The native adapters select `s3` (default), `gcs` or `azure` through `DEOOS_STORAGE_PROVIDER` or the embedded client's `provider` option. `DEOOS_STORAGE_BUCKET` names the bucket or Azure container; `AWS_BUCKET` remains an S3 fallback. GCS uses native `GOOGLE_*` credentials; Azure uses native `AZURE_*` credentials. S3-specific credential/region options are rejected when selecting another provider. R2 uses the S3 adapter with its account API endpoint and region `auto`.

`deoos-server --check-storage` uses the configured store and creates one UUID-isolated object under `<prefix>/qualification/`. It races 32 creates and 32 conditional replacements, checks stale-writer rejection and immediate read/list visibility, then deletes the object. Failure exits nonzero, including unexpected throttling. This checks storage primitives only; qualification also requires the full SDK workflow and recovery suite in both deployment modes. Use a dedicated test bucket/container with no retention or versioning policy; the probe removes the live object, not historical versions retained by the provider. It never creates or deletes a bucket/container.

The [use-case library](examples/README.md) provides paired Python/TypeScript examples for reliable webhook delivery, invoice approval and scheduled ingestion. Run workers near their cloud object store or beside self-hosted RustFS. The examples use a local simulated HTTP adapter; its test results establish execution behavior, not integration with an external vendor.

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

## Retaining completed work

DEOOS has no automatic expiry or purge. Keep task state and its referenced checkpoint and signal objects while a prefix remains writable. Failed and cancelled tasks can still be retried. Completed state preserves task identity, final output for joins, same-ID submission checks and scheduled occurrence deduplication. Removing it can strand a parent, repeat an execution or stall a schedule. Do not apply object-store age-based deletion to a writable execution prefix. The last 32 history events are already bounded; trimming those events does not reduce discovery's object count.

For finite workloads, use separate prefixes for bounded cohorts of work. Retirement is an operator-controlled procedure: stop new top-level admissions and new scheduled occurrences; finish or explicitly abandon outstanding work and future retry/backfill obligations; then stop all workers, schedule pollers and operator mutations. Account for pending schedule intents and in-flight writes before freezing the prefix. Pausing schedules alone is insufficient. Verify the complete task inventory; the UI and task-list API return at most 100 IDs and cannot establish that a larger prefix has drained.

Enforce the write stop through deployment and storage access controls before treating the old prefix as a read-only archive. The engine has no durable namespace seal. Keep that archive for the required inspection and recovery period, and erase it only after those obligations end. Age alone is not an eligibility check. Object-store versioning or retention policies may preserve historical versions after live objects are deleted. This procedure adds no automatic collector or online deletion guarantee.

A new prefix does not inherit old checkpoints, child outputs or schedule cursors. Use distinct execution IDs for genuinely new work: external idempotency keys contain the task ID and step name, not the prefix. Moving recurring work requires an explicit schedule ID, anchor and backfill boundary that avoids repeating old occurrences; copying schedule state is not a migration contract. Expiring completed checkpoint payloads separately would require an explicit expired-result API and resumable cleanup, and would still leave task-state enumeration linear; it is not implemented.

## Discovery and request costs

Claims still list schedules and all task objects; there is no separate index. Each engine keeps bounded hints for up to 16,384 tasks and 16,384 schedules. Matching LIST change tokens let it skip unchanged terminal tasks, active leases, and dormant schedules. Changed tokens, or LIST results with no change tokens, require a state read. Eligible claims and waiting conditions are checked against fresh state, and conditional writes still control ownership. Restarting the engine or exceeding the hint capacity loses only the optimization. Checkpoint objects add LIST pages but are not read during an idle claim. Task inspection by ID reads one state object; operator listing returns at most 100 states but enumerates all task objects first.

The example workers immediately poll again after executing work, and wait one second after an empty poll or an error. That delay starts after the call finishes; it does not guarantee one poll per second. Your application controls this loop. More idle workers repeat discovery and increase request costs; slower polling delays timer wakeups, retries, resumption after signals and schedule admission. The load tests separately request one-second inspection intervals, but sequential task reads can take longer. Their observation windows include worker startup and admission, so pending or unobserved counts are cutoff observations, not proof that tasks cannot finish.

Both SDKs now rely on the Rust definition operation's fresh ownership check instead of reading task state separately before every checkpoint. Eight one-import checks on macOS ARM64/local RustFS compared the previous and current SDKs against identical native binaries, in both languages and modes with a 30-second lease. Worker SDK requests fell from 67 to 58 in each pair: nine pre-definition state reads disappeared, while four join reads and all mutation counts stayed unchanged. Counts exclude admission and inspection by the driver and include one idle claim and protocol bookkeeping. These are SDK requests, not billed storage attempts or measured throughput gains.

On October 3, 2026, four local RustFS load runs compared these SDKs in baseline–candidate–candidate–baseline order against identical native binaries and the same import harness. Each run used both modes, one Python worker or four mixed-language workers, 100-import bursts and a 60-second window. Both versions recorded 1,130/1,200 observed completions across fresh and eligible retained bursts, with 70 pending or unobserved at the cutoffs and no recorded workflow, inspection or worker errors. All four-worker bursts completed; one-worker retained bursts were skipped in both versions. Fresh four-worker times ranged from 23.26–24.56 seconds for the baseline and 23.32–24.38 for the candidate; retained times were similar and mixed. Two repetitions per version do not establish a throughput gain. Timing includes startup, admission and sequential inspection, and normal host background activity was not controlled. All four generated buckets were removed.

Run `tests/.venv/bin/python tests/use_cases.py --recovery-under-load` after building to exercise worker replacement with a backlog. Four local RustFS cells cover both modes and both language replacement directions, using the test's 1.5-second lease. Each kills one worker after its fixture effect commits, while four tasks are running and sixteen are queued. The current SDKs completed all 80 tasks with 84 fixture HTTP requests and 80 fixture effects; this depends on the fixture honoring idempotency keys. Processes and generated buckets were removed. This local check does not establish cloud-hosted recovery or general exactly-once external effects.

A local macOS ARM64/RustFS embedded-mode probe on October 3, 2026 seeded completed task states without checkpoint objects. At 16,384 tasks, the cold idle claim made 16,384 GET requests; two warm claims made none. At 16,500 tasks, both warm claims made 116 GET requests. Every observation made 18 LIST requests, including the empty schedule listing. Existing hints remain cached when the cache fills; excess uncached states still get read. The 16,500-task cold scan recorded 29 proxy connection errors and retries, so its timing is unsuitable for a clean capacity comparison. These are counted idle-discovery observations; they do not establish application throughput or AWS limits. Prefix size and checkpoint-object count still determine enumeration work even when warm state reads are avoided.

Normal task discovery rotates through IDs. A waiting parent can place its children and itself in a volatile priority queue capped at 1,024 IDs. After three priority reservations, the next claim starts with normal discovery. This gives unrelated work opportunities within that engine; it does not guarantee global ordering or a completion deadline. The queue adds no durable objects or external services.

Shared-server HTTP requests use a ten-second socket timeout in Python and a ten-second request deadline in TypeScript. Embedded calls have no equivalent SDK deadline, so a successful long embedded scan does not establish shared-server coverage. A timed-out claim has an uncertain outcome: storage may have accepted ownership before the response was lost. That claim can hold a lease and spend an attempt without running its handler; recovery waits for lease expiry. A client disconnect does not guarantee rollback of a storage write already sent. Keep prefixes bounded and measure cold-start behavior as well as warm polling.

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

Request cost is `(LIST + PUT) × 0.005 / 1000 + (GET + HEAD) × 0.0004 / 1000`. Using the historical 0.5 request counts as a scenario, 1,440 completed idle polls per day against the 5,000-object fixture cost `1440 × (6 × 0.005 + 1000 × 0.0004) / 1000 = $0.6192/day` in requests. Ten workers doing that many polls each multiply this by ten. This assumes completed polls, not a guaranteed polling frequency, and excludes application work, transfer, free-tier credits and other services. The fixture occupied 6,396,000 bytes; using 1 GB = 2^30 bytes, a month of storage is `6396000 / 1073741824 × 0.023`, about $0.00014. Polling can dominate storage costs.

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

These historical timings include this host's network round trips; they are not universal task-count limits or measurements of an AWS-hosted deployment. The current hints reduce repeated state reads, but cold scans, missing LIST tokens, and entries beyond the hint capacity still require reads. Every poll still enumerates objects, so accumulated history remains a practical limit. There is no automatic pruning or separate index. Increasing the client timeout does not remove enumeration or cold-read costs. Measure your deployment before relying on a polling cadence.

Before these discovery changes, local macOS ARM64/RustFS direct load runs recorded 800/800 webhook completions across eight cases and 451/600 ingestion completions across six cases; 149 ingestion workflows remained pending at the observation cutoff. One-worker retained ingestion bursts were skipped after the fresh burst timed out. These runs and the cloud runs differ in host, build, instrumentation and observation window, so their totals do not establish a capacity ratio.

Before these discovery changes, a bounded EC2/S3 workflow load run recorded 562 completions from 1,250 submitted workflows across 16 reports. All eight ingestion cases recorded zero completions within their 30–60-second windows. Local probes show that parent-first claiming and repeated state scans can delay child work; they do not establish the exact cloud cause or a throughput guarantee. Keep task counts per prefix small and measure your intended workload.

The discovery changes were compared with frozen baseline `1737fbe` on macOS ARM64 and local RustFS on October 3, 2026. The baseline server binary SHA-256 starts with `7d0ea226`; the candidate starts with `4bb1c6a1`. Both used the same load harness, 100-import bursts, a 60-second observation window and direct storage access. One worker used Python; four workers mixed Python and TypeScript. Retained bursts reused the completed first burst's task and checkpoint history. One-worker retained bursts were skipped in both builds because fresh bursts remained unfinished.

| Import condition | Baseline completed / 100 | Candidate completed / 100 |
| --- | ---: | ---: |
| Embedded, one worker, fresh | 39 | 84 |
| Server, one worker, fresh | 41 | 84 |
| Embedded, four workers, fresh | 100 | 100 |
| Server, four workers, fresh | 100 | 100 |
| Embedded, four workers, retained history | 81 | 100 |
| Server, four workers, retained history | 83 | 100 |

The candidate completed 568/600 imports; 32 remained pending or unobserved, with no recorded failures or worker/inspection errors. Four-worker fresh bursts finished in 20.89/24.70 seconds versus 29.15/29.67 for the baseline; retained bursts finished in 54.39/57.51 seconds. Both builds completed all 800 webhook workflows. Seven webhook elapsed-time cells improved; the fresh one-worker server cell was slightly slower, 7.673 versus 7.633 seconds. These are bounded local observations, including startup, admission and polling, rather than capacity guarantees.

Repeated counted idle probes showed 1,000 terminal-state GETs becoming zero on warm claims in both modes, while six LIST requests remained for the 5,000-object fixture. Cold timings were mixed. All six missing-LIST-token samples still fetched 100 states, confirming fallback rather than hiding work. The separate counted workload run encountered forwarding errors, including local `EADDRNOTAVAIL`; its results are retained as diagnostics and do not establish clean per-workflow request costs.

A later paired import run used the same bounded connection-reuse counter on both builds. Across 600 admitted imports per build, observed GET attempts fell from 500,062 to 44,124; LIST attempts rose from 4,599 to 5,305 and PUT attempts from 15,058 to 16,506. Completions rose from 395 to 558, with 205 versus 42 pending at the cutoff and no recorded workflow failures or worker/inspection errors. Each build recorded one upstream disconnect; the baseline recorded four downstream reply errors and the candidate eight. These aggregate counts include admission, polling, inspection and contention across fresh and retained namespaces. They are attempted requests, not proof of delivered responses or clean per-workflow costs; use the direct-access runs above for the timing comparison.

On October 3, 2026, the same frozen workload harness compared CI builds `07bc1fb` (run `36922325994`) and `66b43da` (run `37129914969`) sequentially on one Ubuntu 24.04 ARM64 `c7g.large` instance and the same S3 bucket in `us-east-1`. Both used a 30-second lease, Python for one worker and mixed Python/TypeScript for four. Direct-access fresh import bursts submitted 100 workflows per case with a 60-second observation window:

| Import condition | Baseline completed / 100 | Candidate completed / 100 |
| --- | ---: | ---: |
| Embedded, one worker, fresh | 0 | 26 |
| Server, one worker, fresh | 0 | 27 |
| Embedded, four workers, fresh | 0 | 67 |
| Server, four workers, fresh | 0 | 95 |

The candidate completed 215/400 imports; 185 remained pending or unobserved, with no recorded workflow failures or worker/inspection errors. Retained import bursts were skipped in both builds because fresh bursts remained unfinished. Across the six matched direct webhook cases, completions rose from 439/600 to 600/600; the candidate also completed two retained one-worker bursts that the baseline skipped, making its total 800/800. Fresh four-worker webhook rounds took 12.32/10.47 seconds in embedded/server mode versus 41.91/40.74 for the baseline. One sequential pair does not establish a universal throughput or latency guarantee.

A subsequent attribution run used the previous SDKs and unchanged engine on one EC2/S3 shared-server cell with four mixed-language workers and a 60-second window. All 100 parents and 200 children recorded completion before the cutoff. It omitted in-window inspection and added request tracing, so it is not a matched capacity comparison with the earlier run. Across concurrent workers, exclusive SDK timings summed to 71.83 seconds for 406 claim calls and 23.49 seconds for 900 pre-definition state reads; the fixture fetch and publish callbacks summed to 0.412 seconds. These sums are not wall-clock time or potential speedups. Claim timings combine discovery, readiness reads and ownership writes with SDK/server overhead; they do not isolate LIST latency. The earlier run's task states had been removed during cleanup, so the individual 185 pending-or-unobserved executions cannot be classified retrospectively. This evidence motivated removing the redundant SDK reads, while leaving the original backlog's exact causes unresolved. The attribution run's temporary AWS resources were removed.

Each cloud build completed all 24 library-mode discovery samples. For 1,000 terminal tasks with four checkpoint objects each, warm claims reduced GET attempts from 1,000 to zero while retaining six LIST requests; observed warm times changed from 58.99/58.66 seconds to 0.68/0.62 seconds. The candidate's cold sample still fetched 1,000 states and took 59.12 seconds. Separate counted import bursts used 25 workflows and 30-second windows: completions rose from 0/100 to 24/100, GET attempts fell from 2,906 to 2,494, and LIST/PUT attempts rose from 157/855 to 300/1,202. Across all counted workloads, the proxy recorded 23 baseline and 25 candidate broken-pipe errors, so these remain request-attempt diagnostics rather than clean per-workflow costs. Four-worker direct import sampled process RSS increased from 225.0 to 306.3 MiB in embedded mode and from 287.9 to 350.6 MiB in server mode. Reports retain completion latency, backlog, conflicts and sampled process CPU/RSS; sampling excludes unsampled peaks and is not VM utilization. All temporary AWS resources were removed. Keep history per prefix bounded and measure direct access from the intended worker host; warm hints do not remove cold reads or object enumeration.

## Code evolution

Use handler names such as `process_order.v1` and `process_order.v2`. A task's handler is immutable. Register both versions while old executions finish; submit new work to the new version. A worker registering only `.v2` cannot claim `.v1` tasks.

Every named operation declares its kind and definition before reading a checkpoint or doing work. Spawn inputs, ordered join dependencies and timer durations must match their original definitions. Ordinary callbacks declare a revision: `ctx.step("charge", charge, revision="1")` in Python, or `ctx.step("charge", charge, "1")` in TypeScript. Change the handler version and declared revision when callback semantics or result shape change. The engine verifies declared compatibility; it cannot detect arbitrary changes inside your code. A mismatch fails before consuming a cached value or executing the callback. It follows the configured failure/retry policy and can spend attempts; committed values remain available.

Version 0.6 continues the storage protocol 3 introduced by 0.4 and the default prefix `durable-v3`. Earlier 0.2/protocol-1 and 0.3/protocol-2 alphas cannot be mixed with the new engine. Keep their engine and SDK packages on their original prefixes until those executions drain, then use 0.6 with a fresh prefix. Mutating SDK calls check engine protocol before dispatch, and the engine rejects unsupported clients and stored tasks before changing them. Keep each service URL on one protocol version throughout its deployment. Unknown task fields are preserved for future metadata additions. History recording introduced by 0.5 remains compatible with 0.6; 0.4 preserves new metadata but does not emit history events.

Functions restart from the top on recovery; named steps return committed values. External effects can repeat before a checkpoint commits: destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)` for effect deduplication. Workers must have synchronized clocks. Lease expiry cannot interrupt application code or an already dispatched network request; a replacement owner's conditional state write prevents the stale owner from committing over it. Keep checkpoint names and handler semantics compatible with active executions. Checkpoint callbacks are sequential within a task; child tasks may execute concurrently. Node CPU work must not block renewal timers.

This alpha supports claims, leases, checkpoints, retries, recovery, child-task composition, timers, signals, cancellation, declared code/checkpoint compatibility and interval schedules/backfills. Task inspection, bounded history, manual retry, a small shared-server UI, paired complete application examples and measured discovery/request costs are included. No production HA, tenant isolation, garbage collection or application throughput guarantee is claimed.

macOS Apple Silicon remains the primary development platform. Native CI has verified both modes and SDKs for all five targets above, including worker replacement, shared-server restart, persisted timer/signal waits and cross-language recovery.

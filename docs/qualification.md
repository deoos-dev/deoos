# Qualification and historical measurements

This document preserves the test evidence and its limits. Historical results describe the builds, hosts and workloads named below; they do not qualify every later release or deployment. For current operating rules, see [Operations](operations.md).

## Release completeness

Every release must include `macos-arm64`, `macos-x64`, `linux-arm64`, `linux-x64`, and `windows-x64`. A release is incomplete until each archive has matching fresh-install evidence for Python and TypeScript in library and server modes. Building for a target, including cross-compilation, does not establish that its packages run there. Record the actual runtime architecture; execution under emulation is not native-host testing.

Before publishing, run the local gate (it does not build, upload, or start GitHub Actions):

```sh
python3 packaging/verify_release.py --version VERSION --source FULL_COMMIT_SHA \
  --outputs ../outputs --evidence ../outputs/evidence
```

For each target, `build-TARGET.json` in the evidence directory records `source_commit`, `target`, and `archive_sha256`. Matching `package-smoke-deoos-VERSION-TARGET-BACKEND.json` reports come from `tests/package_smoke.py`. The gate checks all five archives, complete payload checksums, licenses, docs, SDKs, native artifact headers, source attestations, and installed-package recovery/worker evidence. Keep the attestations and reports with the release evidence. Historical reports cannot qualify new archive contents.

## Release targets and installation scope

The 0.7.0 alpha release currently ships a locally verified Mac Apple Silicon package. Other targets below were qualified in earlier releases and require fresh 0.7.0 qualification before shipping. Download and unpack the release for your platform. Python 3.10+ and Node 22+ are required for their respective SDKs. No Rust compiler is needed to use the prebuilt release.

| Release target | Operating requirements | Runtime verification |
| --- | --- | --- |
| `macos-arm64` | macOS 11+; Apple Silicon | 0.7.0: both modes and SDKs verified locally on Apple Silicon |
| `macos-x64` | macOS 11+; Intel-compatible Python/Node | Both modes and SDKs on native x86_64 macOS CI |
| `linux-arm64` | ARM64 Linux, glibc 2.28+ | Both modes and SDKs on native ARM64 Linux CI |
| `linux-x64` | x86_64 Linux, glibc 2.28+ | Both modes and SDKs on native x86_64 Linux CI |
| `windows-x64` | x86_64 Windows | Both modes and SDKs on Windows Server 2022 CI |

Linux releases use audited `manylinux_2_28` wheels; Alpine/musl is outside these builds. Choose a Python/Node distribution compatible with your operating system. Earlier releases passed all five targets through fresh-package installation and cross-language recovery tests against real S3 on their native architectures. Linux tests run inside pinned manylinux containers. The macOS 11 deployment target and glibc 2.28 baseline are build requirements; CI does not test every supported OS version.

```sh
python3 -m venv .venv
.venv/bin/pip install /path/to/release/python/*.whl
# Run in your application's Node project:
npm install /path/to/release/node/*.tgz
```

On Windows, create the environment with `python -m venv .venv` and install the wheel using `.venv\Scripts\python.exe -m pip install C:\path\to\release\python\WHEEL_FILENAME.whl`. Install the npm tarball in your Node project with `npm install C:\path\to\release\node\deoos-0.7.0.tgz`.

Releases contain an installable Python wheel, npm tarball, optional server executable, application examples, this guide, and checksums. Packages are not yet published to registries.

Before starting workers, run `/path/to/release/bin/deoos-server --check-storage` (`bin/deoos-server.exe` on Windows) with your intended storage configuration. It exits after checking the conditional-write primitives; it does not start a server. A missing or incompatible library-mode native library points to the platform release package and explains how to check a native-library override. Server-mode clients use `Client.remote(...)` and do not load the engine inside the worker process.

## Object-store qualification

AWS S3, RustFS, Cloudflare R2, Google Cloud Storage and Azure Blob Storage are the tested storage backends. R2, GCS and Azure each passed the 32-writer conditional-write contract, all 44 workflow checks and four separate warm-cache test blocks across library and server modes using installed macOS ARM64 CI packages for the current SDKs (`90e840e`; Azure used the corrected harness at `b52ebce`). These were local Mac tests against actual cloud storage, with the default 30-second lease and unchanged SDK HTTP timeouts; they do not establish cloud-local performance. Temporary resources and generated credentials were removed. Historical failed attempts remain preserved; earlier Azure timeout causes remain unproven. RustFS is the recommended self-hosted target; distributed deployment and air-gapped operation still need separate qualification. Every supported backend must pass the same execution behavior tests.

The discovery changes at `66b43da` were requalified against GCS, Azure and R2 using the macOS ARM64 package from CI run `37129914969`. Each passed all 44 workflow checks plus four separate warm-cache test blocks across both modes, with temporary resources and generated credential files removed. These checks use a Mac against actual cloud storage; the same-region EC2/S3 comparison below separately measures cloud-hosted workers.

## Storage primitive probe

`deoos-server --check-storage` uses the configured store and creates one UUID-isolated object under `<prefix>/qualification/`. It races 32 creates and 32 conditional replacements, checks stale-writer rejection and immediate read/list visibility, then deletes the object. Failure exits nonzero, including unexpected throttling. This checks storage primitives only; qualification also requires the full SDK workflow and recovery suite in both deployment modes. Use a dedicated test bucket/container with no retention or versioning policy; the probe removes the live object, not historical versions retained by the provider. It never creates or deletes a bucket/container.

## Build and verify

Run development checks locally. GitHub Actions is manual-only; start a run only when explicitly requested by the project owner. Reserve the full platform matrix for release qualification.

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

When explicitly requested, a manual GitHub Actions run checks the RustFS behavioral suite on Linux and runs the five-platform package matrix. Pull requests, pushes and tags do not trigger runs. Each platform builds its release, installs both SDKs in fresh projects, tests library and server-mode workflows against real S3, and uploads packages only after passing. Linux builds retain the audited manylinux 2.28 baseline. Configure repository variables `AWS_ROLE_ARN` and `AWS_ACCOUNT_ID` with a dedicated GitHub OIDC role and its expected AWS account ID. Restrict the role to DEOOS test buckets; no AWS access-key secrets are needed. Test reports include exact bucket names for cleanup. Local AWS tests use the standard credential chain; set `AWS_PROFILE` to choose an account explicitly.

The Windows native artifacts link the C runtime statically; their inspected DLL imports are operating-system libraries.

## Filesystem checks

On Apple Silicon, run `make setup` once, then `make test-filesystem` for developer qualification. It installs fresh packages, tests both SDKs and modes, exercises the current filesystem format, races independent processes, injects process kills and one-shot directory-sync failures through a test-only interposer, and verifies rejection of separately mounted object and lock directories using temporary APFS disk images. A warmed engine must also reject an uncertain replacement written by a killed process when its read barrier fails. These tests establish process recovery and failure reporting; they are not a physical power-loss test. The same package suite against RustFS checks the S3-compatible path. Evidence stays outside the repository in `../outputs/evidence`.

## Use-case and load-test commands

`make test-examples TEST_PYTHON=tests/.venv312/bin/python` builds the native engine and runs the behavioral suite against local RustFS. The suite checks both languages and deployment modes, transient retries, approval and decline, persisted waits, scheduled ingestion and worker replacement across languages. Reports stay in `../outputs/evidence`, outside the repository. Real-provider and cloud-local performance results are reported separately from local fixture results. A cloud-local deployment is not established by a laptop-to-cloud run.

The same suite accepts a precreated, dedicated cloud test bucket or container:

```sh
python tests/use_cases.py --backend s3 --bucket TEST_BUCKET
python tests/use_cases.py --backend r2 --bucket TEST_BUCKET
python tests/use_cases.py --backend gcs --bucket TEST_BUCKET
python tests/use_cases.py --backend azure --bucket TEST_CONTAINER
```

It uses a generated prefix, removes only its own live objects, and preserves the supplied bucket/container. Historical versions and soft-deleted objects follow the provider's retention policy. S3/R2 use explicit environment credentials; R2 also needs its S3 API endpoint. GCS uses `GOOGLE_SERVICE_ACCOUNT_PATH`; Azure uses `AZURE_STORAGE_ACCOUNT_NAME` and exactly one of `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_KEY`. Cleanup uses optional test-only packages `google-cloud-storage` or `azure-storage-blob` for those providers; the DEOOS SDKs keep their existing dependencies. A configured adapter is qualified only after the primitive probe and all workflow checks pass on the actual service.

For repeatable local load measurements after building, keep RustFS running and run:

```sh
tests/.venv312/bin/python tests/load.py --workload webhook --tasks 100 --workers 1 4 --duration 60
tests/.venv312/bin/python tests/load.py --workload import --tasks 100 --workers 1 4 --duration 60
```

Each command runs both modes and compares a fresh namespace with one retaining the first burst's completed tasks. Reports include completed workflows per second, observed latency, backlog, retries and sampled process CPU/RSS. Latency includes submission and inspection lag; resource samples exclude the RustFS container. Use `--transport counted` to record storage requests and conditional conflicts through a local forwarding proxy; its overhead and failures must be considered separately from direct measurements. A finished report can contain failed or pending workflows, so inspect the outcomes and cleanup status.

`--backend aws` requires a verified EC2 host in the same region as S3 and `DEOOS_EXPECTED_AWS_ACCOUNT`. Counted AWS transport additionally requires an assumed role and `us-east-1`; it forwards through HTTPS and includes proxy/TLS overhead. By default it creates and removes a temporary test bucket, but does not provision compute infrastructure.

Use `--bucket PRECREATED_BUCKET` when a deployment controller owns the test resources. The target must be empty; AWS targets must also match the verified host region. The harness removes only its generated `load/<run UUID>/` namespace and retains the bucket, so the controller can remove that exact bucket if the guest fails. Run `tests/.venv312/bin/python tests/load.py --check-bucket-contract` against local RustFS to verify ownership and cleanup behavior.

The load tests request one-second inspection intervals, but sequential task reads can take longer. Their observation windows include worker startup and admission, so pending or unobserved counts are cutoff observations, not proof that tasks cannot finish.

## Discovery, performance and cost history

On October 4, 2026, macOS ARM64/local RustFS checks covered both modes with 0, 1,000 and 5,000 completed tasks and their result objects. Against an already initialized index, cold and warm empty claims listed no historical task objects and read no task states; claiming one live task read one state. Historical objects and neighboring prefixes stayed unchanged. Those historical checks included interrupted intents, retry/cleanup races, and encoded prefixes; their former upgrade-adoption behavior has since been removed. The full SDK suites passed 35 library and 38 server checks plus 44 application checks. A separate worker-kill matrix completed 80/80 tasks across both language directions and modes; killed tasks were reclaimed after their saved lease expiries. Its fixture enforced idempotency: 84 HTTP requests produced 80 effects.

A local direct-access comparison ran the previous engine (`d2e4838`) and active-index engine in B-C-C-B order. Each first burst submitted 25 two-page imports to four mixed Python/TypeScript workers, with a 30-second window and 0, 1,000 or 5,000 completed state-only fixtures. Initialization and fixture seeding were outside timing. Each table cell combines two first bursts:

| Completed history states | Mode | Previous completed / submitted | Active index completed / submitted |
| --- | --- | ---: | ---: |
| 0 | Library mode | 50/50 | 50/50 |
| 0 | Server | 50/50 | 50/50 |
| 1,000 | Library mode | 50/50 | 50/50 |
| 1,000 | Server | 50/50 | 50/50 |
| 5,000 | Library mode | 14/50 | 50/50 |
| 5,000 | Server | 32/50 | 50/50 |

Across all 24 eligible candidate bursts, including second bursts with completed first-burst history, all 600 imports completed in 2.15–2.36 seconds per burst. Baseline second bursts at 5,000 history states were skipped because work remained unfinished; those asymmetric totals are not compared. The direct load harness, workflow examples and SDK code matched; differing unused proxy/standalone-probe code is pinned in the reports. Background host activity was uncontrolled, so these are local sequential observations, not universal capacity or speedup guarantees. They exclude initialization cost. All owned local test resources were removed. These results qualify the new layout on local RustFS; the cloud-provider results below describe earlier engine builds.

Both SDKs now rely on the Rust definition operation's fresh ownership check instead of reading task state separately before every checkpoint. Eight one-import checks on macOS ARM64/local RustFS compared the previous and current SDKs against identical native binaries, in both languages and modes with a 30-second lease. Worker SDK requests fell from 67 to 58 in each pair: nine pre-definition state reads disappeared, while four join reads and all mutation counts stayed unchanged. Counts exclude admission and inspection by the driver and include one idle claim and protocol bookkeeping. These are SDK requests, not billed storage attempts or measured throughput gains.

On October 3, 2026, four local RustFS load runs compared these SDKs in baseline–candidate–candidate–baseline order against identical native binaries and the same import harness. Each run used both modes, one Python worker or four mixed-language workers, 100-import bursts and a 60-second window. Both versions recorded 1,130/1,200 observed completions across fresh and eligible retained bursts, with 70 pending or unobserved at the cutoffs and no recorded workflow, inspection or worker errors. All four-worker bursts completed; one-worker retained bursts were skipped in both versions. Fresh four-worker times ranged from 23.26–24.56 seconds for the baseline and 23.32–24.38 for the candidate; retained times were similar and mixed. Two repetitions per version do not establish a throughput gain. Timing includes startup, admission and sequential inspection, and normal host background activity was not controlled. All four generated buckets were removed.

The same SDK-only comparison ran in that order on one Ubuntu 24.04 ARM64 `c7g.large` instance with S3 in `us-east-1`, identical native binaries, a 30-second lease and unchanged SDK timeouts. Each fresh burst admitted 100 imports with a 60-second observation window; one worker used Python and four workers mixed both languages. The two repetitions recorded these observed completions:

| Fresh import condition | Previous SDKs, each run / 100 | Current SDKs, each run / 100 |
| --- | ---: | ---: |
| Library mode, one worker | 27, 29 | 32, 30 |
| Server mode, one worker | 27, 28 | 30, 30 |
| Library mode, four workers | 69, 83 | 100, 100 |
| Server mode, four workers | 91, 100 | 100, 100 |

Across the matched fresh cases, completions were 454/800 versus 522/800, leaving 346 versus 278 pending or unobserved at the cutoffs, with no recorded failures, cancellations or submission, inspection or worker errors. Current four-worker fresh bursts finished in 54.40–59.69 seconds. Retained bursts were eligible only after a fresh burst completed, so their counts are not a matched comparison: the current SDKs completed 40 and 43 per 100 in library mode and 91 and 87 in server mode; the previous SDKs had one eligible server-mode burst and completed 80/100. The retained-history bursts still had unfinished work at their cutoffs. Two repetitions show this sample's improvement, not a general capacity guarantee; direct access did not count storage requests or establish billed costs. Task metadata was retained after workers stopped and before deletion; those sequential snapshots are later observations, not atomic cutoff state. All scoped temporary AWS resources were removed.

Across those four later snapshots, 555 parents had never been claimed, 180 were waiting (155 with all referenced children completed), and 20 retained running state. Workers had stopped before capture. These records distinguish unclaimed and partly executed work, but do not establish state at the observation cutoff, a scheduling cause, or the individual causes of the historical 185 unfinished-or-unobserved imports.

Run `tests/.venv/bin/python tests/use_cases.py --recovery-under-load` after building to exercise worker replacement with a backlog. Four local RustFS cells cover both modes and both language replacement directions, using the test's 1.5-second lease. Each kills one worker after its fixture effect commits, while four tasks are running and sixteen are queued. The current SDKs completed all 80 tasks with 84 fixture HTTP requests and 80 fixture effects; this depends on the fixture honoring idempotency keys. Processes and generated buckets were removed. This local check does not establish cloud-hosted recovery or general exactly-once external effects.

The EC2/S3 comparison also ran that recovery test once per SDK version with a 30-second lease, covering eight cells across both modes and language replacement directions. All 160 tasks completed with 168 fixture HTTP requests and 160 effects. Each killed worker's task was reclaimed after its saved post-kill lease expiry; that task used a second attempt while the other nineteen used one. The executed harness checked the generation increment as well. Workers and servers stopped, test prefixes were deleted, and independent checks confirmed removal of the VM, root volume, temporary role, instance profile, security group and both buckets. External effects still depend on the destination honoring the supplied idempotency key.

An earlier engine's local macOS ARM64/RustFS library-mode probe on October 3, 2026 seeded completed task states without checkpoint objects. At 16,384 tasks, the cold idle claim made 16,384 GET requests; two warm claims made none. At 16,500 tasks, both warm claims made 116 GET requests. Every observation made 18 LIST requests, including the empty schedule listing. That build kept existing hints when the cache filled; excess uncached states still required reads. The 16,500-task cold scan recorded 29 proxy connection errors and retries, so its timing is unsuitable for a clean capacity comparison. These are counted idle-discovery observations; they do not establish application throughput or AWS limits. For that earlier discovery path, prefix size and checkpoint-object count determined enumeration work even when warm state reads were avoided.

Measured on macOS ARM64 with the 0.5 engine and local RustFS, three samples per row gave identical request counts in library and server-mode claim tests:

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

Direct AWS measurements from this macOS ARM64 host to us-east-1, with the unchanged 0.5 engine and Python server-mode client, were:

| Idle namespace | Completed samples | Median completed scan |
| --- | ---: | ---: |
| Empty | 3 / 3 | 113 ms |
| 100 completed tasks, no checkpoints | 3 / 3 | 7,282 ms |
| 1,000 completed tasks, four checkpoints each | 0 / 3 | Timed out at about ten seconds |

These historical timings include this host's network round trips; they are not universal task-count limits or measurements of an AWS-hosted deployment. Those earlier builds used task hints, but cold scans, missing LIST tokens and entries beyond hint capacity still required reads. They enumerated all task objects on every poll. The active-reference index described in [Operations](operations.md#discovery) replaces that discovery path; these historical results do not measure its performance. Increasing client timeouts did not remove the earlier enumeration or cold-read costs.

Before these discovery changes, local macOS ARM64/RustFS direct load runs recorded 800/800 webhook completions across eight cases and 451/600 ingestion completions across six cases; 149 ingestion workflows remained pending at the observation cutoff. One-worker retained ingestion bursts were skipped after the fresh burst timed out. These runs and the cloud runs differ in host, build, instrumentation and observation window, so their totals do not establish a capacity ratio.

Before these discovery changes, a bounded EC2/S3 workflow load run recorded 562 completions from 1,250 submitted workflows across 16 reports. All eight ingestion cases recorded zero completions within their 30–60-second windows. Local probes show that parent-first claiming and repeated state scans can delay child work; they do not establish the exact cloud cause or a throughput guarantee. Keep task counts per prefix small and measure your intended workload.

The discovery changes were compared with frozen baseline `1737fbe` on macOS ARM64 and local RustFS on October 3, 2026. The baseline server binary SHA-256 starts with `7d0ea226`; the candidate starts with `4bb1c6a1`. Both used the same load harness, 100-import bursts, a 60-second observation window and direct storage access. One worker used Python; four workers mixed Python and TypeScript. Retained bursts reused the completed first burst's task and checkpoint history. One-worker retained bursts were skipped in both builds because fresh bursts remained unfinished.

| Import condition | Baseline completed / 100 | Candidate completed / 100 |
| --- | ---: | ---: |
| Library mode, one worker, fresh | 39 | 84 |
| Server, one worker, fresh | 41 | 84 |
| Library mode, four workers, fresh | 100 | 100 |
| Server, four workers, fresh | 100 | 100 |
| Library mode, four workers, retained history | 81 | 100 |
| Server, four workers, retained history | 83 | 100 |

The candidate completed 568/600 imports; 32 remained pending or unobserved, with no recorded failures or worker/inspection errors. Four-worker fresh bursts finished in 20.89/24.70 seconds versus 29.15/29.67 for the baseline; retained bursts finished in 54.39/57.51 seconds. Both builds completed all 800 webhook workflows. Seven webhook elapsed-time cells improved; the fresh one-worker server cell was slightly slower, 7.673 versus 7.633 seconds. These are bounded local observations, including startup, admission and polling, rather than capacity guarantees.

Repeated counted idle probes showed 1,000 terminal-state GETs becoming zero on warm claims in both modes, while six LIST requests remained for the 5,000-object fixture. Cold timings were mixed. All six missing-LIST-token samples still fetched 100 states, confirming fallback rather than hiding work. The separate counted workload run encountered forwarding errors, including local `EADDRNOTAVAIL`; its results are retained as diagnostics and do not establish clean per-workflow request costs.

A later paired import run used the same bounded connection-reuse counter on both builds. Across 600 admitted imports per build, observed GET attempts fell from 500,062 to 44,124; LIST attempts rose from 4,599 to 5,305 and PUT attempts from 15,058 to 16,506. Completions rose from 395 to 558, with 205 versus 42 pending at the cutoff and no recorded workflow failures or worker/inspection errors. Each build recorded one upstream disconnect; the baseline recorded four downstream reply errors and the candidate eight. These aggregate counts include admission, polling, inspection and contention across fresh and retained namespaces. They are attempted requests, not proof of delivered responses or clean per-workflow costs; use the direct-access runs above for the timing comparison.

On October 3, 2026, the same frozen workload harness compared CI builds `07bc1fb` (run `36922325994`) and `66b43da` (run `37129914969`) sequentially on one Ubuntu 24.04 ARM64 `c7g.large` instance and the same S3 bucket in `us-east-1`. Both used a 30-second lease, Python for one worker and mixed Python/TypeScript for four. Direct-access fresh import bursts submitted 100 workflows per case with a 60-second observation window:

| Import condition | Baseline completed / 100 | Candidate completed / 100 |
| --- | ---: | ---: |
| Library mode, one worker, fresh | 0 | 26 |
| Server, one worker, fresh | 0 | 27 |
| Library mode, four workers, fresh | 0 | 67 |
| Server, four workers, fresh | 0 | 95 |

The candidate completed 215/400 imports; 185 remained pending or unobserved, with no recorded workflow failures or worker/inspection errors. Retained import bursts were skipped in both builds because fresh bursts remained unfinished. Across the six matched direct webhook cases, completions rose from 439/600 to 600/600; the candidate also completed two retained one-worker bursts that the baseline skipped, making its total 800/800. Fresh four-worker webhook rounds took 12.32/10.47 seconds in library/server mode versus 41.91/40.74 for the baseline. One sequential pair does not establish a universal throughput or latency guarantee.

A subsequent attribution run used the previous SDKs and unchanged engine on one EC2/S3 server-mode cell with four mixed-language workers and a 60-second window. All 100 parents and 200 children recorded completion before the cutoff. It omitted in-window inspection and added request tracing, so it is not a matched capacity comparison with the earlier run. Across concurrent workers, exclusive SDK timings summed to 71.83 seconds for 406 claim calls and 23.49 seconds for 900 pre-definition state reads; the fixture fetch and publish callbacks summed to 0.412 seconds. These sums are not wall-clock time or potential speedups. Claim timings combine discovery, readiness reads and ownership writes with SDK/server overhead; they do not isolate LIST latency. The earlier run's task states had been removed during cleanup, so the individual 185 pending-or-unobserved executions cannot be classified retrospectively. This evidence motivated removing the redundant SDK reads, while leaving the original backlog's exact causes unresolved. The attribution run's temporary AWS resources were removed.

Each cloud build completed all 24 library-mode discovery samples. For 1,000 terminal tasks with four checkpoint objects each, warm claims reduced GET attempts from 1,000 to zero while retaining six LIST requests; observed warm times changed from 58.99/58.66 seconds to 0.68/0.62 seconds. The candidate's cold sample still fetched 1,000 states and took 59.12 seconds. Separate counted import bursts used 25 workflows and 30-second windows: completions rose from 0/100 to 24/100, GET attempts fell from 2,906 to 2,494, and LIST/PUT attempts rose from 157/855 to 300/1,202. Across all counted workloads, the proxy recorded 23 baseline and 25 candidate broken-pipe errors, so these remain request-attempt diagnostics rather than clean per-workflow costs. Four-worker direct import sampled process RSS increased from 225.0 to 306.3 MiB in library mode and from 287.9 to 350.6 MiB in server mode. Reports retain completion latency, backlog, conflicts and sampled process CPU/RSS; sampling excludes unsampled peaks and is not VM utilization. All temporary AWS resources were removed. Those earlier builds still enumerated all task objects on each claim; active-reference discovery in [Operations](operations.md#discovery) supersedes that path. Measure direct access from the intended worker host.

## Platform requalification

macOS Apple Silicon remains the primary development platform. Earlier-release native CI verified both modes and SDKs for all five targets above; 0.7.0 is currently qualified locally on Mac ARM64, including worker replacement, server-mode restart, persisted timer/signal waits and cross-language recovery.

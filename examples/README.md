# DEOOS use cases

These examples use the same workflows in embedded and shared-server modes, with interchangeable Python and TypeScript workers. They demonstrate practical execution patterns through a local HTTP service adapter. They do not establish integration with a payment, accounting, messaging or data vendor.

| Use case | Durable behavior | Integration boundary |
| --- | --- | --- |
| Webhook delivery | Retry a transient failure; resume after worker replacement | HTTP destination must honor the supplied idempotency key |
| Invoice approval | Persist a wait, stop the worker, then approve or decline | Approval signal followed by an idempotent accounting API call |
| Scheduled ingestion | Fetch pages as child tasks, join them, publish a batch | Source API and idempotent batch destination |

Pattern references: [idempotent API requests](https://docs.stripe.com/api/idempotent_requests), [signal-based approvals](https://github.com/temporalio/documentation/blob/main/docs/design-patterns/approval.mdx), and [scheduled flows](https://docs.prefect.io/v3/how-to-guides/deployments/create-schedules). The runnable examples use the simulated HTTP adapter described below.

The paired programs are `use_cases.py` and `use_cases.mjs`. They use only the DEOOS SDK and language standard libraries. Install the SDK for your platform before running them; keep the JavaScript example inside the Node project where you installed `deoos`.

## Deployment

In cloud deployments, run workers in the relevant cloud near the authoritative object store. In self-hosted deployments, run workers beside RustFS or on the same machine. Embedded mode puts Rust in each worker process; shared mode uses a separate engine service. Storage and destination services remain dependencies in either mode.

AWS S3 and RustFS are currently qualified. GCS, Azure Blob and Cloudflare R2 remain qualification targets. Credentials, network routing and provider configuration differ; the execution behavior contract must remain the same.

For embedded mode, configure `DEOOS_MODE=library`, `DEOOS_STORAGE_BUCKET`, storage credentials and optionally `EXECUTION_PREFIX`. `DEOOS_STORAGE_PROVIDER` selects `s3` (default), `gcs` or `azure`; the latter two adapters remain unqualified against real providers. For S3, configure `AWS_REGION`; `AWS_BUCKET` remains a compatible bucket setting. For local RustFS, also use `AWS_ENDPOINT=http://127.0.0.1:19000` and `AWS_ALLOW_HTTP=true`, with the development credentials in the repository's `compose.yaml`.

For shared mode, configure the engine with the storage settings, start `deoos-server`, then set `DEOOS_MODE=server`, `ENGINE_URL` and, if enabled, `ENGINE_TOKEN` on workers. Shared-server workers need no storage credentials.

## Service adapter

`SERVICE_URL` selects the HTTP service used by these examples. Use your integration adapter or the local simulated service from `tests/use_cases.py`. The test service is for development only. It demonstrates destination idempotency while workers restart; it is not a production accounting or ingestion service.

Use `--help` on either program for commands and inputs. Stable task IDs deduplicate submissions of the same definition; reuse an ID only for the same handler and inputs. Named steps replay their committed results. An HTTP operation may repeat after a crash before its checkpoint commits, so destination idempotency is required.

## Run the examples

From the source repository, start the simulated destination with `python3 tests/use_cases.py --serve --port 18080`. Configure storage or the shared-server connection as above, then set `SERVICE_URL=http://127.0.0.1:18080`. In another terminal, run a worker with `python examples/use_cases.py work` or `node examples/use_cases.mjs work`. Keep the worker's configuration the same when changing languages.

```sh
python examples/use_cases.py submit webhook --id delivery-001 --event-id event-001
python examples/use_cases.py submit invoice --id approval-001 --amount-cents 2500
python examples/use_cases.py signal --id approval-001
python examples/use_cases.py submit import --id import-001 --source demo --pages 2
python examples/use_cases.py schedule --id daily-demo --source demo --pages 2
python examples/use_cases.py inspect --id import-001
```

Replace `python examples/use_cases.py` with `node examples/use_cases.mjs` for TypeScript. The daily schedule uses fixed 24-hour intervals, not timezone-aware cron. Pass `--decline` to `signal` to decline an invoice; no invoice request is issued. Stop a worker while approval is pending, send the signal, then start the opposite-language worker to observe recovery. Stop the manual test service when finished; its in-memory effects ledger resets when it stops.

## Verification

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

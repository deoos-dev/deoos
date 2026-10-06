# DEOOS use cases

These examples use the same workflows in library and server modes, with interchangeable Python and TypeScript workers. They demonstrate practical execution patterns through a local HTTP service adapter. They do not establish integration with a payment, accounting, messaging or data vendor.

| Use case | Durable behavior | Integration boundary |
| --- | --- | --- |
| Webhook delivery | Retry a transient failure; resume after worker replacement | HTTP destination must honor the supplied idempotency key |
| Invoice approval | Persist a wait, stop the worker, then approve or decline | Approval signal followed by an idempotent accounting API call |
| Scheduled ingestion | Fetch pages as child tasks, join them, publish a batch | Source API and idempotent batch destination |

Pattern references: [idempotent API requests](https://docs.stripe.com/api/idempotent_requests), [signal-based approvals](https://github.com/temporalio/documentation/blob/main/docs/design-patterns/approval.mdx), and [scheduled flows](https://docs.prefect.io/v3/how-to-guides/deployments/create-schedules). The runnable examples use the simulated HTTP adapter described below.

The paired programs are `use_cases.py` and `use_cases.mjs`. They use only the DEOOS SDK and language standard libraries. Install the SDK for your platform before running them; keep the JavaScript example inside the Node project where you installed `deoos`.

## Deployment

In cloud deployments, run workers in the relevant cloud near the authoritative object store. In self-hosted deployments, run workers beside RustFS or on the same machine. Library mode puts Rust in each worker process; server mode uses a separate engine service. Storage and destination services remain dependencies in either mode.

For R2, use the S3 provider with your account endpoint and region `auto`. GCS and Azure use `DEOOS_STORAGE_PROVIDER=gcs` or `azure` with their native `GOOGLE_*` or `AZURE_*` credentials. Use the same bucket/container and execution prefix across workers.

For library mode, configure `DEOOS_MODE=library`, `DEOOS_STORAGE_BUCKET`, storage credentials and optionally `EXECUTION_PREFIX`. `DEOOS_STORAGE_PROVIDER` selects `s3` (default), `gcs` or `azure`. For S3, configure `AWS_REGION`; `AWS_BUCKET` remains a compatible bucket setting. For local RustFS, also use `AWS_ENDPOINT=http://127.0.0.1:19000` and `AWS_ALLOW_HTTP=true`, with the development credentials in the repository's `compose.yaml`.

For server mode, configure the engine with the storage settings, start `deoos-server`, then set `DEOOS_MODE=server`, `ENGINE_URL` and, if enabled, `ENGINE_TOKEN` on workers. Server-mode workers need no storage credentials.

## Service adapter

`SERVICE_URL` selects the HTTP service used by these examples. Use your integration adapter or the local simulated service from `tests/use_cases.py`. The test service is for development only. It demonstrates destination idempotency while workers restart; it is not a production accounting or ingestion service.

Use `--help` on either program for commands and inputs. Stable task IDs deduplicate submissions of the same definition; reuse an ID only for the same handler and inputs. Named steps replay their committed results. An HTTP operation may repeat after a crash before its checkpoint commits, so destination idempotency is required.

## Run the examples

From the source repository, start the simulated destination with `python3 tests/use_cases.py --serve --port 18080`. Configure storage or the server-mode connection as above, then set `SERVICE_URL=http://127.0.0.1:18080`. In another terminal, run a worker with `python examples/use_cases.py work` or `node examples/use_cases.mjs work`. Keep the worker's configuration the same when changing languages.

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

From the source repository, run `make setup test-examples PYTHON=python3.12` (Rust, Node and Docker Compose required). This runs all three examples with both SDKs and modes, including retries and worker replacement. The harness creates and removes its own test bucket; stop the manually started service before running it.

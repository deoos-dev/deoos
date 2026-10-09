# DEOOS use cases

These examples demonstrate recovery in library and server mode. Hacker News and DEV articles use real public APIs; the HTTP integrations use a simulated service. The Hacker News and HTTP workflows have interchangeable Python and TypeScript workers; the DEV example uses Python.

## Hacker News → DuckDB

Requires a Mac with Apple Silicon, `uv`, and Docker installed and running. From this repository:

```sh
./examples/hacker-news
```

The launcher starts local RustFS and installs the released SDK and dependencies automatically. The demo collects 100 live Hacker News items, kills the worker after 20 stored items, waits for lease recovery, resumes it, and verifies the saved database contents.

Open the printed file with `duckdb <database-path>`. No environment configuration is needed.

## Other use cases

### DEV articles → CSV

Requires Mac Apple Silicon, `uv`, and Docker. Run:

```sh
./examples/devto-etl
```

Based on Prefect's [API-sourced ETL example](https://docs.prefect.io/v3/examples/run-api-sourced-etl): fetch three pages of articles, normalize engagement fields, and write `devto_articles.csv`. The workflow is in `devto_etl.py`; it uses Python's standard library and deduplicates overlapping article IDs.

The demo captures three live API responses once and replays those unchanged responses through a counted local HTTP source. In both library and server mode, it kills the worker after page 2 is checkpointed, then after the CSV write before its checkpoint. It verifies each page was fetched once per workflow and both CSVs have identical contents with no duplicate rows. Results stay in `outputs/devto-etl/`; its temporary RustFS bucket is removed. RustFS stays running.

The CSV is local to the worker host. Replacement workers need access to that same destination path; server mode shares workflow state, not files. CSV replacement is idempotent for one workflow owning one output path. Use separate paths for independent runs.

### HTTP integrations

| Use case | Durable behavior | Integration boundary |
| --- | --- | --- |
| Webhook delivery | Retry a transient failure; resume after worker replacement | HTTP destination must honor the supplied idempotency key |
| Invoice approval | Persist a wait, stop the worker, then approve or decline | Approval signal followed by an idempotent accounting API call |
| Scheduled ingestion | Fetch pages as child tasks, join them, publish a batch | Source API and idempotent batch destination |

The runnable examples use the simulated HTTP adapter described below.

The paired programs are `use_cases.py` and `use_cases.mjs`. They use only the DEOOS SDK and language standard libraries. Install the SDK for your platform before running them; keep the JavaScript example inside the Node project where you installed `deoos`.

## Deployment

In cloud deployments, run workers in the relevant cloud near the authoritative object store. In self-hosted deployments, run workers beside RustFS or on the same machine. Library mode puts Rust in each worker process; server mode uses a separate engine service. Storage and destination services remain dependencies in either mode.

For R2, use the S3 provider with your account endpoint and region `auto`. GCS and Azure use `DEOOS_STORAGE_PROVIDER=gcs` or `azure` with their native `GOOGLE_*` or `AZURE_*` credentials. Use the same bucket/container and execution prefix across workers.

For library mode, configure `DEOOS_MODE=library`, `DEOOS_STORAGE_BUCKET`, storage credentials and optionally `EXECUTION_PREFIX`. `DEOOS_STORAGE_PROVIDER` selects `s3` (default), `gcs` or `azure`. For S3, configure `AWS_REGION`. For local RustFS, also use `AWS_ENDPOINT=http://127.0.0.1:19000` and `AWS_ALLOW_HTTP=true`, with the development credentials in the repository's `compose.yaml`.

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

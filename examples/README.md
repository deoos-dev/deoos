# DEOOS use cases

These examples use the same workflows in library and server modes, with interchangeable Python and TypeScript workers. Hacker News uses a real public API; the other examples use a simulated HTTP service.

## Hacker News → DuckDB

`hacker_news.py` and `hacker_news.mjs` collect 100 items from the [Hacker News API](https://github.com/HackerNews/API) into a local DuckDB file. DEOOS remembers the chosen IDs and each fetch and database write. If the worker stops, restart it with the same configuration: completed steps return their saved results, and unfinished steps retry after the lease expires.

Install the DEOOS SDK as shown in the main README, then install the database client:

```sh
python -m pip install duckdb==1.5.6
# In your Node project, alongside deoos and hacker_news.mjs:
npm install @duckdb/node-api@1.5.6-r.1
```

For a local trial, start RustFS with `docker compose up -d` from the source repository, then configure library mode and create a development bucket:

```sh
export DEOOS_MODE=library
export AWS_ENDPOINT=http://127.0.0.1:19000 AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=local-development
export AWS_SECRET_ACCESS_KEY=local-development-only-secret
export AWS_REGION=us-east-1 DEOOS_STORAGE_BUCKET=hacker-news-workflows
export EXECUTION_PREFIX=hacker-news
aws --endpoint-url "$AWS_ENDPOINT" s3 mb "s3://$DEOOS_STORAGE_BUCKET"
```

From the directory where you copied the example, run:

```sh
python hacker_news.py submit --id collection-001
python hacker_news.py work --once
python hacker_news.py inspect --id collection-001
python hacker_news.py query
```

Replace `python hacker_news.py` with `node hacker_news.mjs` for TypeScript. Both use the same handler and checkpoint names, so either worker can resume the other. `query` reads only the DuckDB file and works offline. Change the task ID for a new collection; `--database` chooses its file.

For scheduled collection, use `schedule --id daily-news --interval-ms 86400000`, then `work` without `--once`. Workers admit due runs while polling. These are fixed intervals, and an occurrence is skipped when the previous run is still active. Use the main README's server instructions and set `DEOOS_MODE=server`, `ENGINE_URL` and `ENGINE_TOKEN` to run the same example in server mode.

The database has two tables: `stories` stores the first captured JSON payload for each ID, and `collections` stores each task's IDs and order. Inserts use `ON CONFLICT DO NOTHING` in one transaction. A retry after a database commit but before its DEOOS checkpoint preserves existing rows. Later collections also preserve the first payload; this example does not refresh changing stories. Missing or deleted items are retained, so 100 IDs need not mean 100 article links.

Use one worker per DuckDB file, and restart on the same machine or persistent volume. Server mode shares execution state, not the worker's local database. In cloud deployments, put workers near their object store; for self-hosting, keep workers beside RustFS.

The source test `tests/hacker_news.py` exercises both SDKs and modes, API failure, two forced worker stops (including the commit/checkpoint gap), preservation of an existing marked row, scheduled runs and offline queries. It captures 100 items from the real API once and replays those responses across the cases. With RustFS running, the SDKs installed, and the Node example inside your Node project, run:

```sh
ENGINE_BINARY=/path/to/deoos-server python tests/hacker_news.py \
  --node-example /path/to/your/node-project/hacker_news.mjs
```

The test needs `boto3` in the Python environment to create and remove its isolated RustFS bucket.

## Other use cases

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

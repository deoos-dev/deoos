# DEOOS use cases

These examples use the same workflows in library and server modes, with interchangeable Python and TypeScript workers. Hacker News uses a real public API; the other examples use a simulated HTTP service.

## Hacker News → DuckDB

Mac Apple Silicon. Requires Python 3.12, Docker, AWS CLI and GitHub CLI. Run the steps in the same terminal, starting in this repository.

### 1. Install

```sh
export DEOOS_SOURCE="$PWD"
mkdir -p ~/try-deoos-hn
cd ~/try-deoos-hn

gh release download 0.7.0-alpha.2 --repo deoos-dev/deoos \
  --pattern deoos-0.7.0-alpha.2-macos-arm64.tar.gz --clobber
tar -xzf deoos-0.7.0-alpha.2-macos-arm64.tar.gz
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install ./deoos-0.7.0-alpha.2-macos-arm64/python/*.whl duckdb==1.5.6
cp "$DEOOS_SOURCE/examples/hacker_news.py" .
```

### 2. Start RustFS

```sh
docker compose -f "$DEOOS_SOURCE/compose.yaml" up -d

export DEOOS_MODE=library DEOOS_STORAGE_PROVIDER=s3
export AWS_ENDPOINT=http://127.0.0.1:19000 AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=local-development
export AWS_SECRET_ACCESS_KEY=local-development-only-secret
export AWS_REGION=us-east-1 DEOOS_STORAGE_BUCKET=hacker-news-workflows
unset AWS_SESSION_TOKEN
export LEASE_MS=5000

until curl -fsS "$AWS_ENDPOINT/health" >/dev/null; do sleep 1; done
aws --endpoint-url "$AWS_ENDPOINT" s3api head-bucket --bucket "$DEOOS_STORAGE_BUCKET" \
  || aws --endpoint-url "$AWS_ENDPOINT" s3 mb "s3://$DEOOS_STORAGE_BUCKET"
```

### 3. Submit 100 items

```sh
export TASK_ID="hn-$(date +%Y%m%d-%H%M%S)"
export EXECUTION_PREFIX="$TASK_ID"
export DATABASE="$PWD/$TASK_ID.duckdb"
python hacker_news.py submit --id "$TASK_ID" --database "$DATABASE"
```

### 4. Start the worker and kill it

```sh
python hacker_news.py work --once >worker.log 2>&1 &
WORKER_PID=$!
sleep 2
kill -9 "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null || true
python hacker_news.py inspect --id "$TASK_ID"
```

### 5. Wait for the lease to expire, then restart

```sh
sleep 6
python hacker_news.py work --once
python hacker_news.py inspect --id "$TASK_ID"
```

Expected status: `completed`.

### 6. Query the database offline

```sh
python hacker_news.py query --database "$DATABASE"
```

Expected collection count: `100`.

### 7. Optional: run every 24 hours

```sh
python hacker_news.py schedule --id daily-news --database "$DATABASE" --interval-ms 86400000
python hacker_news.py work
```

Use one worker per DuckDB file. Restart with the same file and storage settings. Existing story payloads are preserved.

For TypeScript (Node 22+), install and copy the Node example, then replace `python hacker_news.py` with `node hacker_news.mjs` in steps 3–7:

```sh
npm install ./deoos-0.7.0-alpha.2-macos-arm64/node/*.tgz @duckdb/node-api@1.5.6-r.1
cp "$DEOOS_SOURCE/examples/hacker_news.mjs" .
```

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

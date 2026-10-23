# Examples

Four short programs. Each one runs in about three commands. The crash is a flag on `work`, and the process exits 86. It is not hidden in a harness.

Python is the path. TypeScript is the order workflow, where the point is that the other language can resume the same task.

## Storage

Library mode needs a bucket that already exists, plus credentials and `DEOOS_STORAGE_BUCKET` (or `AWS_BUCKET`). It is not zero-config. The filesystem provider is macOS ARM64 with local APFS only.

```sh
export DEOOS_MODE=library
export DEOOS_STORAGE_BUCKET=my-workflows
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

Local RustFS, matching `compose.yaml`:

```sh
export AWS_ENDPOINT=http://127.0.0.1:19000
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=local-development
export AWS_SECRET_ACCESS_KEY=local-development-only-secret
export AWS_REGION=us-east-1
export DEOOS_STORAGE_BUCKET=deoos
```

`DEOOS_MODE=library` ignores `ENGINE_URL`. Unset it, or set `DEOOS_MODE=server` when you mean the server. A dead process can still hold a task lease. The next `work` waits for it. The default lease is 30 seconds (`LEASE_MS`).

## Pages

Three built-in pages, one checkpoint each, then a CSV.

```sh
python examples/pages.py submit --id snap-001 --output "$PWD/articles.csv"
python examples/pages.py work --id snap-001 --crash-after fetch-page-1
python examples/pages.py work --id snap-001
```

`articles.csv.fetches` gains one line per fetch. Page 1 is not fetched again. `--crash-before-commit write-csv` exits after the file is on disk and before that checkpoint. The next `work` writes the same bytes again.

## Charge

A local sink on port 18181. Approve, then one charge. Decline runs a void and does not charge.

```sh
python examples/charge.py serve
python examples/charge.py submit --id pay-001
python examples/charge.py work --id pay-001
python examples/charge.py approve --id pay-001
python examples/charge.py work --id pay-001 --crash-before-commit charge
python examples/charge.py work --id pay-001
python examples/charge.py effects
```

The `work` after the crash replays the charge step. The sink records one charge because the idempotency key is the same. The sink's memory lasts as long as `serve`. An uncommitted call whose provider key has expired still needs a reconciliation read. This example does not do that.

## One process

Library mode is this process. Kill it after `collect` is committed. Start it again.

```sh
python examples/footprint.py submit --id job-001 --log "$PWD/job.log"
python examples/footprint.py work --id job-001 --crash-after collect
python examples/footprint.py work --id job-001
python examples/footprint.py info
```

Fail, explain, retry, and cancel use the CLI:

```sh
python examples/footprint.py submit-flaky --id flaky-001
python examples/footprint.py work --id flaky-001 --fail-flaky
python -m deoos explain flaky-001
python -m deoos retry flaky-001
python examples/footprint.py work --id flaky-001

python examples/footprint.py submit-hold --id hold-001
python examples/footprint.py work --id hold-001
python -m deoos cancel hold-001
```

Server mode is the same commands with `DEOOS_MODE=server`, `ENGINE_URL`, and `ENGINE_TOKEN`. Open `/ui` on that server. `footprint.py` has the `deoos-server` line.

## Interval fan-out

No cron. The interval is milliseconds. The default anchor is 15 seconds ago, so the first poll admits the latest tick and leaves a gap. Each tick writes `<scheduled_at>.json`. The parent reads `scheduled_at` off the task. Schedule inputs do not change per tick. `view` does not include that field.

```sh
python examples/ingest.py schedule --id hourly --output "$PWD/ingest-out"
python examples/ingest.py work
python examples/ingest.py pause --id hourly
python examples/ingest.py backfill --id hourly
python examples/ingest.py resume --id hourly
python examples/ingest.py work
```

Pause before backfill. A paused schedule does not drain a backfill until you resume and work. Backfill is an SDK call. It is not a `python -m deoos` command. `work --follow` polls until Ctrl-C.

## Order, either language

`workflow_python.py` and `workflow_typescript.mjs` are the same order: validate and price as children, a timer, then an approval signal. Start it in one language and resume it in the other. Run the TypeScript file from the Node project that contains `node_modules/deoos`. Node resolves `deoos` from the file's location, not from `NODE_PATH`.

`inspect` prints the public view. `--internal` prints lease and storage fields.

## Optional

`examples/hacker-news` is a larger live version of the page workflow: 100 Hacker News items into DuckDB. Docker runs the crash in a harness.

```sh
docker compose -p deoos run --build --rm hacker-news
```

`devto-etl` and `document-index` are the same kind of harness for a CSV and a text index. `payment-resume` is the older charge harness. Run `charge.py` instead. `use_cases.py` and `use_cases.mjs` are the webhook, invoice, and import programs those harnesses grew from. `library_python.py` and `library_typescript.mjs` submit one greeting. `server_python.py` and `server_typescript.mjs` are the server-mode greeting.

Docker demo output stays under `outputs/`. RustFS stays running.

# Hacker News

Same DuckDB snapshot as the Python example. The Docker command runs the recovery check. This client is what a Node worker uses against the same `deoos-server`.

## Run

From the repository root, after `. ./scripts/prepare_local.sh` and with `deoos-server` listening:

```sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
node typescript/hacker-news/hacker_news.mjs submit --id hn-002 --database "$PWD/outputs/hn-ts.duckdb" --count 20
node typescript/hacker-news/hacker_news.mjs work --once
```

`python tests/hacker_news.py` kills a worker in each language and resumes with the other, in library mode and against `deoos-server`.

## Expected output

`work --once` stores stories and exits. The crash test prints one JSON line per case with `"status": "passed"`.

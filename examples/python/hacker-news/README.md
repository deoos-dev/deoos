# Hacker News

A snapshot of top stories should survive a worker that dies halfway through. Each story is its own checkpoint in a local DuckDB file, stored through `deoos-server` on RustFS.

## Run

Docker installs deoos 0.8.0 from the release archive, checks `SHA256SUMS`, starts RustFS, and runs `deoos-server`. From the repository root:

```sh
docker compose run --build --rm hacker-news
```

## Expected output

```text
Collecting 100 Hacker News items…
Killed the worker after 20 saved items. Restarting after the lease expires…
Completed: 100 items, no duplicate or overwritten rows.
```

The database stays in `outputs/hacker-news/`. The first 20 rows are not rewritten.

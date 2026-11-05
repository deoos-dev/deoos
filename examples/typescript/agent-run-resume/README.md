# Agent run resume

Same plan, research, draft, check, and publish workflow as the Python example. `publish` charges with `ctx.idempotencyKey("publish")`.

## Run

```sh
docker compose run --build --rm agent-run-resume
```

## Expected output

```text
typescript: killed after draft was saved
credits burned without deoos vs with: 3260 vs 1220
```

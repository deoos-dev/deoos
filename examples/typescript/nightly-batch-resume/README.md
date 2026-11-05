# Nightly batch resume

Same 10,000-order batch as the Python example. Each email uses `ctx.idempotencyKey`.

## Run

```sh
docker compose run --build --rm nightly-batch-resume
```

## Expected output

```text
typescript: killed after batch 70 was saved; batches 1-70 ran once
typescript: killed inside batch 71; overlapping trigger attached to the same task
typescript: second trigger returned the same result; 10000 unique sends
```

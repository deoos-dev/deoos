# Payment

Same charge, approval, and fulfil workflow as the Python example. The idempotency key is `ctx.idempotencyKey`.

## Run

```sh
docker compose run --build --rm payment-resume
```

## Expected output

```text
server/typescript: saved charge reused past key expiry; one charge, one fulfil.
server/typescript: call/commit gap retried with the same key; one charge, one fulfil.
```

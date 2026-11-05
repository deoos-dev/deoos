# Payment

A charge that already succeeded must not be sent again after the provider forgets the idempotency key, and a crash between the HTTP call and the checkpoint must retry the same key. The workflow charges, waits for approval, then fulfils.

## Run

```sh
docker compose run --build --rm payment-resume
```

The fake API keeps keys for 24 hours and moves a virtual clock, so the demo does not wait a day. No card is charged. The engine is real: RustFS and `deoos-server`.

## Expected output

```text
library/python: saved charge reused past key expiry; one charge, one fulfil.
library/python: call/commit gap retried with the same key; one charge, one fulfil.
```

The same two lines repeat for TypeScript and for `server` mode. Evidence stays in `outputs/payment-resume/`.

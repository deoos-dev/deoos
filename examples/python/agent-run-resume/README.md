# Agent run resume

Restarting an agent from the first prompt calls the same tools again and pays for them again. A timeout on a charge is worse when the retry invents a new idempotency key. Saved model calls stay saved. `publish` charges with one idempotency key.

## Run

```sh
docker compose run --build --rm agent-run-resume
```

The model and the charge endpoint are local stand-ins. The engine is RustFS and `deoos-server`.

## Expected output

```text
python: killed after draft was saved
credits burned without deoos vs with: 3260 vs 1220
```

The model runs once for plan, research, draft, and publish, and twice for check. The charge ledger has one effect. TypeScript prints the same credit line.

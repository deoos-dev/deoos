# HTTP integrations

A webhook has to retry a 503 without delivering twice, an invoice has to wait for approval before it is issued, and a scheduled import has to resume as child tasks. The HTTP service in the test is a local stand-in with an in-memory idempotency ledger. It is not an accounting system. The engine is RustFS and `deoos-server`.

## Run

```sh
docker compose up -d rustfs
. ./scripts/prepare_local.sh
python tests/use_cases.py
```

`prepare_local.sh` installs the 0.8.0 archive after `SHA256SUMS`. To drive one case yourself, in another terminal:

```sh
python tests/use_cases.py --serve --port 18080
```

```sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
export SERVICE_URL=http://127.0.0.1:18080
python python/integrations/use_cases.py submit webhook --id delivery-001 --event-id event-001
python python/integrations/use_cases.py work --once
```

Start `deoos-server` first, as in `python/server/README.md`.

## Expected output

```text
passed library: python -> typescript
passed library: typescript -> python
passed server: python -> typescript
passed server: typescript -> python
```

# HTTP integrations

Same webhook, invoice, and scheduled import as the Python example.

## Run

`python tests/use_cases.py` runs this file and the Python workflow on RustFS and `deoos-server`. To drive one case, start the service as in the Python README, then:

```sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
export SERVICE_URL=http://127.0.0.1:18080
node typescript/integrations/use_cases.mjs submit webhook --id delivery-002 --event-id event-002
node typescript/integrations/use_cases.mjs work --once
```

## Expected output

The suite prints `passed server: typescript -> python` when a TypeScript worker replaces a Python worker against `deoos-server`.

# Order

An order has to survive a stopped worker: validate and price as child tasks, wait out a timer, then wait for a person to approve. No payment service is called. A TypeScript worker can finish an order a Python client submitted.

## Run

From the repository root:

```sh
. ./scripts/prepare_local.sh
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=dev-token "$ENGINE_BINARY"
```

In another shell:

```sh
. ./.deoos/env.sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
python python/order/order.py submit --id order-001 --quantity 2 --unit-price 25 --delay-ms 200
python python/order/order.py work
```

`work` polls until you stop it. When the task is waiting, approve from a third shell with the same `DEOOS_MODE`, `ENGINE_URL`, and `ENGINE_TOKEN`:

```sh
python python/order/order.py approve --id order-001
```

The worker then finishes the order. `inspect --id order-001` prints it.

## Expected output

`inspect` shows `"status": "completed"` and the saved result:

```json
{"approved": true, "order_id": "order-001", "total": 50}
```

# Order

Same order as the Python example: child tasks, a timer, and an approval signal. Point it at the same `deoos-server` and it can finish a Python submission.

## Run

Start `deoos-server` as in `python/order/README.md`, then:

```sh
. ./.deoos/env.sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
node typescript/order/order.mjs submit --id order-002 --quantity 2 --unit-price 25 --delay-ms 200
node typescript/order/order.mjs work
```

Approve from another shell:

```sh
node typescript/order/order.mjs approve --id order-002
```

## Expected output

`inspect --id order-002` shows `"status": "completed"` and:

```json
{"approved": true, "order_id": "order-002", "total": 50}
```

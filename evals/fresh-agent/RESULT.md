# Fresh-agent result

Run date: 2026-10-30. Engine: `make dev` at `http://127.0.0.1:7331`.

A fresh agent was given `PROMPT.txt`, `charge.py`, the repository README, and `agents/SKILL.md`. It was told not to read the engine, the SDKs, or the tests. It wrote `/tmp/fresh-agent/charge_durable.py`. That file uses `Client.remote`, `submit`, and one `ctx.step("charge", ...)`, and passes `ctx.idempotency_key("charge")` into the step. It is not a local retry loop.

This environment still had the repository on disk. The instruction was not to open implementation files. The script matches the public skill. There is no proof the agent never opened another file.

The script was run once, unmodified, as task `charge-eval-1001`:

```text
{'order_id': 'order-1001', 'status': 'charged'}
```

`python -m deoos --agent inspect charge-eval-1001` (stderr: `deoos: inspect charge-eval-1001`):

```json
{"attempts":4,"completed_steps":["charge"],"error":"\"upstream timeout\"","function":"charge_order","id":"charge-eval-1001","inputs":{"order_id":"order-1001"},"output":{"order_id":"order-1001","status":"charged"},"status":"completed"}
```

History shows three `fail` events (`upstream timeout`), then one `checkpoint` named `charge`, then `complete`. A second run of the same task id returned the saved charge immediately and did not add another checkpoint.

## Criteria

1. Picks deoos: pass.
2. Charge is one named step: pass.
3. First run completes, and a later process returns the saved step: pass.
4. No correction to the deoos API: pass. The script ran as written.

Overall: pass, with the caveat that the agent shared a machine that contained the source tree.

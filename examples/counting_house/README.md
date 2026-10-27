# Counting House refund

A reference saga for [Steve Yegge's Counting House](https://yegge.ai/essays/the-counting-house/) pattern ([post](https://x.com/Steve_Yegge/status/2115103182271676561)).

The model does not write to a system of record. It returns a write intent. A durable workflow checks that intent against the live ledger, waits for a person to approve it, then carries the change through the billing ledger, the CRM, and the notification log. Each write has an idempotency key and a read-back. If a later system refuses the change, earlier writes are compensated. Finished runs export to an eval set: the proposal, the approval, the outcome, and a correctness label.

This example is one refund of one paid invoice. Avery Chen was charged 4200 cents on invoice `inv-42`. The legacy systems are JSON files in a case directory. The proposer that ships here is a deterministic stub. No API key is required, and the workflow does not open a network connection.

## Run it

Docker, from the repository root:

```sh
docker compose -p deoos run --build --rm counting-house
```

From a source checkout, with the Python SDK built and local RustFS already running (`docker compose -p deoos up -d`):

```sh
PYTHONPATH=clients/python python examples/counting_house/demo.py
```

The demo runs five cases on the real engine: approval, a crash after the ledger write and before its checkpoint, rejection, compensation when the notification system refuses, and the eval export. Evidence is left under `outputs/counting-house/`.

To drive one case yourself, point library or server mode at your bucket (see the repository README), then:

```sh
PYTHONPATH=clients/python python examples/counting_house/refund.py submit --id refund-001 --office /tmp/refund-001
PYTHONPATH=clients/python python examples/counting_house/refund.py work
PYTHONPATH=clients/python python examples/counting_house/refund.py approve --id refund-001 --by dana
PYTHONPATH=clients/python python examples/counting_house/refund.py export --out /tmp/refund-evals
```

`reject` declines a waiting case. Restart `work` after a crash; the same task continues. Use one task id per case. Submitting that id again with the same inputs returns the existing task.

## What the workflow does

1. **Propose.** `propose` calls `propose_refund`. The stub returns a refund intent. To use another model, add a function to `PROPOSERS` in `refund.py`. It should accept the request and return an intent, and it should not touch the network. `proposer: "command"` runs the local program in `COUNTING_HOUSE_PROPOSER`: request JSON on stdin, proposal JSON on stdout.
2. **Eligibility.** `eligibility` re-reads the ledger file. The proposal is not treated as proof that the invoice is payable. An over-invoice intent finishes as `ineligible` and writes nothing.
3. **Approval.** `wait_signal("approval")` parks the task and frees the worker. `approve` or `reject` delivers one signal, including the name of the person who decided.
4. **Saga.** Ledger, then CRM, then notification. Each `*.apply` step passes `ctx.idempotency_key(step)` into the JSON system. A retry or a crash before the checkpoint commits sends the same key; the system returns the stored effect and does not post a second one. Each `*.verify` step reads the system back and fails the saga if the write is missing.
5. **Compensation.** If a later apply or verify raises, the workflow voids or reverses what already landed, in reverse order, each with its own idempotency key and read-back. The task then completes with outcome `compensated`.
6. **Audit and evals.** The last step writes `audit.json` into the case directory and stores the same document as the task output. `export` writes `evals.jsonl` plus `runs/<task-id>.json` (the audit and the engine history). `correct` is true when the proposal matches the written refund rule: allowed reason, same customer and invoice, and the requested amount within the paid invoice. The label grades the proposal. It does not grade whether the person approved.

## Temporal equivalents

| Piece | This example | Temporal equivalent |
| --- | --- | --- |
| Saga | `refund` in `refund.py`, sequenced with `ctx.step` | A workflow that runs activities in order |
| Activity | A `ctx.step` callback. A committed step returns its saved value and does not run again | An activity. A completed activity is not executed again on replay |
| Idempotency | `ctx.idempotency_key(step)` is `{task_id}/{step}` for the life of the task. The ledger, CRM, and notification log store that key | An activity idempotency key, or a stable workflow id, stored by the destination |
| Human approval | `ctx.wait_signal("approval")` parks the task. `Client.signal` records one value and the worker can be gone while it waits | A workflow signal. The workflow task finishes, and a later task resumes on the signal |
| Read-back | `ledger.verify`, `crm.verify`, `notify.verify` read the system after the write | A follow-up activity that reads the system of record |
| Compensation | `*.compensate` steps run in reverse when a later step raises `OfficeFault` | Saga compensations, registered as activities and run in reverse on failure |
| Crash and resume | A new worker claims the task after the lease expires. Committed steps are skipped | A new worker replays the workflow from history and skips completed activities |
| History | `Client.history` returns the last 32 events (claim, checkpoint, suspend, signal, complete). The case `audit.json` is the record of the proposal, checks, writes, and outcome | Workflow event history, which is also the replay log. A compliance archive is still kept separately |
| Evals | `export` turns each completed task into one JSONL row: proposal, approval, outcome, `correct` | Export workflow results, for example through visibility queries, into an eval set |

## Limits

- Engine history keeps 32 events and does not store step payloads. Replay uses the checkpoint values. The audit file is the trail to keep.
- `list_tasks` returns at most 100 tasks. `export` stops if the list is truncated, so a busy prefix needs to be split before an export can cover it.
- Step names and checkpoint behavior are part of an open task. Changing them while a refund is still running conflicts with the saved definition.
- The experimental filesystem store is macOS ARM64 with APFS. This demo uses an S3-compatible bucket. Local RustFS keeps that on the machine.
- The engine does not dedupe a write that happened but was not checkpointed yet. The legacy system has to honor the idempotency key. The crash case in the demo is that gap.

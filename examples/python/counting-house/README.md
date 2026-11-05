# Counting House refund

A refund should not land because a model said so. The workflow re-reads the ledger, waits for a person, then posts to a billing ledger, a CRM file, and a notification log. Each write uses an idempotency key and is read back. If notification refuses, the ledger and the CRM are compensated.

Avery Chen was charged 4200 cents on invoice `inv-42`. The systems are JSON files. The proposer is a deterministic stub. No API key is used.

## Run

```sh
docker compose run --build --rm counting-house
```

The container installs deoos 0.8.0 from the checked release archive and runs the saga on RustFS and `deoos-server`.

## Expected output

```text
happy: approved refund posted once to the ledger, CRM, and notification log
crash: worker killed after the ledger write; resume posted that refund once
reject: approval declined and the legacy systems stayed unchanged
compensate: notification refusal reversed the ledger and voided the CRM note
over-refund: orchestrator refused a proposal above the paid invoice
export: 5 eval rows in outputs/counting-house/<run>/export/evals.jsonl
```

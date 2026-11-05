# Offline approval ledger

An approval that nobody can tie back to the transfer is not an approval. The worker may sit on that wait for hours, including on a machine that cannot phone home.

`intake` reads a local invoice. `propose_transfer` applies amount and currency rules. The task waits for an approval signal. The transfer posts that approval id with one idempotency key. `reconcile` writes one ledger row: approval id, transfer id, idempotency key, and task id.

## Run

On a connected machine, from the repository root:

```sh
./scripts/bundle_offline.sh
```

That downloads the deoos 0.8.0 Linux archives, checks `SHA256SUMS`, pulls the pinned RustFS image, builds `deoos-examples:offline` from the local archive, and writes `outputs/offline-bundle.tar`.

On the isolated host:

```sh
docker load -i outputs/offline-bundle.tar
docker compose -f compose.offline.yaml run --rm offline-approval-ledger
```

`compose.offline.yaml` does not build images. Its network is internal, so the worker cannot open HTTPS to github.com. Inside the container, `deoos-server` uses RustFS. The demo kills the worker while the task is waiting, sends the approval with no worker running, kills the next worker after the bank has accepted the transfer and before that step is checkpointed, and lets a new process finish.

## Expected output

```text
offline: probing HTTPS to github.com
offline: worker killed while waiting; signal stored with no worker running
offline: killed between the transfer call and its checkpoint
offline: one transfer, approval mapped to transfer id, pre-wait steps ran once
```

Evidence stays in `outputs/offline-approval-ledger/`.

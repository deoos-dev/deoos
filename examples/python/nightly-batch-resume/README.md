# Nightly batch resume

A nightly job of 10,000 orders dies part way through and, on the next start, must not send the early emails again. The task id is the night's date, so a second trigger joins the same task.

## Run

```sh
docker compose run --build --rm nightly-batch-resume
```

The demo kills the worker after batch 70, kills the next worker inside batch 71, and submits the same task again. RustFS and `deoos-server` are the engine. The mailer is a local HTTP stand-in.

## Expected output

```text
python: killed after batch 70 was saved; batches 1-70 ran once
python: killed inside batch 71; overlapping trigger attached to the same task
python: second trigger returned the same result; 10000 unique sends
```

TypeScript prints the same three lines. Evidence stays in `outputs/nightly-batch-resume/`.

# Library hello

You want one durable step without running a separate server. The engine runs inside the Python process and stores the step in RustFS.

## Run

From the repository root:

```sh
. ./scripts/prepare_local.sh
python python/library/hello.py
```

`scripts/prepare_local.sh` installs deoos 0.8.0 from the release archive, checks `SHA256SUMS`, and starts RustFS. `pip install deoos` does not install this SDK.

## Expected output

```text
Hello, World!
```

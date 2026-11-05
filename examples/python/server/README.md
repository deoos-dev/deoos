# Server hello

You want the worker to greet someone without holding storage credentials. `deoos-server` keeps the RustFS credentials. The worker only has the server URL and token.

## Run

From the repository root, install deoos 0.8.0 (release archive and `SHA256SUMS`) and start RustFS:

```sh
. ./scripts/prepare_local.sh
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=dev-token "$ENGINE_BINARY"
```

Leave that server running. In another shell:

```sh
. ./.deoos/env.sh
ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token python python/server/hello.py
```

## Expected output

```text
Hello, World!
```

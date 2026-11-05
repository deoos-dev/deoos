# Server hello

Same greeting as the Python server example. Node talks to `deoos-server` and does not use the RustFS credentials.

## Run

Start `deoos-server` as in `python/server/README.md`, then:

```sh
. ./.deoos/env.sh
ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token node typescript/server/hello.mjs
```

## Expected output

```text
Hello, World!
```

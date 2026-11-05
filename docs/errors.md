# deoos errors

User-facing errors name what went wrong, the likely cause, and a concrete fix. The stable code is in square brackets, and the same code is the anchor at `https://github.com/hckshr/deoos/blob/main/docs/errors.md#CODE`.

This page lists every code the server, CLI, and Python and TypeScript SDKs emit. Workflow state lives in your S3-compatible bucket (RustFS in local examples). There is no database to run. Install the SDKs from a GitHub release archive for your OS and CPU. PyPI and the npm registry do not publish this project.

`make dev` is the local fix for most first-run failures. It starts RustFS from `compose.yaml`, creates the `deoos-dev` bucket, and listens on `127.0.0.1:7331`.

## First-run failures

<a id="STORAGE_UNREACHABLE"></a>

### STORAGE_UNREACHABLE

HTTP 503. The object store did not accept a connection. RustFS is often not running, or `AWS_ENDPOINT` is wrong.

Fix: `docker compose up -d` and `make dev`. Set `AWS_ENDPOINT` (local RustFS is `http://127.0.0.1:19000`), `AWS_ALLOW_HTTP=true`, `AWS_REGION`, and `DEOOS_STORAGE_BUCKET`. Confirm with `deoos-server --check-storage`.

<a id="STORAGE_CREDENTIALS"></a>

### STORAGE_CREDENTIALS

HTTP 401 or 403. The access key or secret does not match the store, or the key cannot use the bucket. Local RustFS ignores ambient cloud keys.

Fix: for local RustFS set `AWS_ACCESS_KEY_ID=local-development` and `AWS_SECRET_ACCESS_KEY=local-development-only-secret` (see `compose.yaml`), plus `AWS_REGION=us-east-1` and `AWS_ALLOW_HTTP=true`. Restart `deoos-server` after changing them.

<a id="STORAGE_BUCKET_MISSING"></a>

### STORAGE_BUCKET_MISSING

HTTP 400. `DEOOS_STORAGE_BUCKET` names a bucket that does not exist.

Fix: `python scripts/ensure_bucket.py` with `AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ALLOW_HTTP=true`, and `DEOOS_STORAGE_BUCKET` set. `make dev` creates `deoos-dev`. Restart `deoos-server` after the bucket exists.

<a id="ENGINE_UNREACHABLE"></a>

### ENGINE_UNREACHABLE

The CLI or SDK could not open a connection to `ENGINE_URL`. The server is not running, or the host or port is wrong.

Fix: `make dev`, then `export ENGINE_URL=http://127.0.0.1:7331`. `python -m deoos info` should print `"protocol_version": 3`.

<a id="ENGINE_WRONG_URL"></a>

### ENGINE_WRONG_URL

Something answered at `ENGINE_URL`, but it is not a deoos server. A common case is pointing `ENGINE_URL` at RustFS (`http://127.0.0.1:19000`) or adding a path after the origin.

Fix: set `ENGINE_URL` to the origin only, usually `http://127.0.0.1:7331`. The server log prints `deoos-server listening`.

<a id="PROTOCOL_MISMATCH"></a>

### PROTOCOL_MISMATCH

HTTP 409 when a mutation's `protocol_version` is not 3, or the SDK saw a different version on `/info`. The SDK and `deoos-server` are different releases.

Fix: install the Python wheel and the Node package from the same GitHub release archive as `deoos-server --version`. Do not install `deoos` from PyPI or the npm registry.

<a id="WORKFLOW_NONDETERMINISTIC"></a>

### WORKFLOW_NONDETERMINISTIC

HTTP 409. Replay called a step with a different kind, revision, or definition than the one already committed.

Fix: keep the step name, kind, and revision stable for that task id. To run new code, `python -m deoos submit NEW_ID HANDLER '{}'`.

## Storage and startup

| Code | When | Fix |
| --- | --- | --- |
| STORAGE_UNAVAILABLE | <a id="STORAGE_UNAVAILABLE"></a> The store failed in a way that is not a missing bucket, bad credentials, or an unreachable endpoint. HTTP 503. | Retry the same operation id. Check `DEOOS_STORAGE_BUCKET`, `AWS_ENDPOINT`, `AWS_REGION`, and the keys. Run `deoos-server --check-storage`. |
| STORAGE_DEADLINE | <a id="STORAGE_DEADLINE"></a> The store did not answer before the lease budget. The write may have landed. HTTP 503. | Retry the same operation id. Check `AWS_ENDPOINT`. Raise `LEASE_MS` (minimum 1000). |
| STORAGE_DURABILITY | <a id="STORAGE_DURABILITY"></a> The local filesystem could not make a write durable. HTTP 507. | Check disk space and `DEOOS_STORAGE_DIRECTORY`. Retry the same operation id. |
| STORAGE_CONFLICT | <a id="STORAGE_CONFLICT"></a> A conditional write lost a race. HTTP 409. | Retry the same operation id and body. |
| STORAGE_CHECK_FAILED | <a id="STORAGE_CHECK_FAILED"></a> `deoos-server --check-storage` failed. | Check the bucket, endpoint, and credentials. For RustFS, `docker compose up -d` and `make dev`. |
| CONFIG_BUCKET_REQUIRED | <a id="CONFIG_BUCKET_REQUIRED"></a> `DEOOS_STORAGE_BUCKET` is unset and the provider is not filesystem. | Export `DEOOS_STORAGE_BUCKET`, or set `DEOOS_STORAGE_PROVIDER=filesystem` and `DEOOS_STORAGE_DIRECTORY`. |
| CONFIG_LEASE | <a id="CONFIG_LEASE"></a> `LEASE_MS` is missing, not an integer, or below 1000. | Set `LEASE_MS=2000` or omit it to use 30000. |
| CONFIG_PROVIDER | <a id="CONFIG_PROVIDER"></a> `DEOOS_STORAGE_PROVIDER` is not s3, gcs, azure, or filesystem. | Set it to `s3` for RustFS and S3. |
| CONFIG_S3 | <a id="CONFIG_S3"></a> The S3 client rejected the configuration before any request. | Set `DEOOS_STORAGE_BUCKET`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`. For RustFS also set `AWS_ENDPOINT` and `AWS_ALLOW_HTTP=true`. |
| CONFIG_GCS | <a id="CONFIG_GCS"></a> GCS settings or the service account were rejected. | Set `DEOOS_STORAGE_BUCKET` and `GOOGLE_SERVICE_ACCOUNT_PATH`. |
| CONFIG_AZURE | <a id="CONFIG_AZURE"></a> Azure settings were rejected before any request. | Set `AZURE_STORAGE_ACCOUNT_NAME` and one of `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_KEY`. |
| CONFIG_CREDENTIALS | <a id="CONFIG_CREDENTIALS"></a> S3 keys were passed to a GCS or Azure client. | Use `DEOOS_STORAGE_PROVIDER=s3`, or the native credentials for that provider. |
| CONFIG_FILESYSTEM | <a id="CONFIG_FILESYSTEM"></a> Filesystem mode was given a bucket or cloud option. | Use `DEOOS_STORAGE_DIRECTORY`, or switch to `DEOOS_STORAGE_PROVIDER=s3`. |
| CONFIG_DIRECTORY | <a id="CONFIG_DIRECTORY"></a> The filesystem directory is missing or was passed to another provider. | Set `DEOOS_STORAGE_DIRECTORY`, or unset it and set `DEOOS_STORAGE_BUCKET`. |
| CONFIG_DIRECTORY_INIT | <a id="CONFIG_DIRECTORY_INIT"></a> The directory could not be opened. | Create a private writable directory. On macOS the experimental adapter expects APFS. |
| CONFIG_PREFIX | <a id="CONFIG_PREFIX"></a> `EXECUTION_PREFIX` is empty or contains `.` or `..`. | Use a path such as `deoos` or `deoos/prod`. |
| BIND_INVALID | <a id="BIND_INVALID"></a> `ENGINE_BIND` is not `IP:port`. | Set `ENGINE_BIND=127.0.0.1:7331`. |
| BIND_IN_USE | <a id="BIND_IN_USE"></a> The address is already taken. | Stop the other process or pick another port. |
| TOKEN_REQUIRED | <a id="TOKEN_REQUIRED"></a> A non-loopback bind has no `ENGINE_TOKEN`. | Set `ENGINE_TOKEN`, or bind to `127.0.0.1`. |
| SERVER_ARGUMENT | <a id="SERVER_ARGUMENT"></a> `deoos-server` was given an unknown flag. | Run `deoos-server --help`. |
| BUCKET_REQUIRED | <a id="BUCKET_REQUIRED"></a> Library mode has no bucket. | Set `DEOOS_STORAGE_BUCKET`, or use `DEOOS_MODE=server` and `ENGINE_URL`. |
| ENGINE_URL_REQUIRED | <a id="ENGINE_URL_REQUIRED"></a> `DEOOS_MODE=server` has no `ENGINE_URL`. | `make dev` and export `ENGINE_URL=http://127.0.0.1:7331`. |
| MODE_INVALID | <a id="MODE_INVALID"></a> `DEOOS_MODE` is not `library` or `server`. | Set one of those, or unset it and set `ENGINE_URL` or `DEOOS_STORAGE_BUCKET`. |
| NATIVE_LIBRARY | <a id="NATIVE_LIBRARY"></a> The Python process could not load the engine library. | Install the wheel from the GitHub release for this OS and CPU, or use `Client.remote`. |
| NODE_LIBRARY | <a id="NODE_LIBRARY"></a> Node could not load `deoos_node.node`. | Install the release tarball for this OS and CPU with Node 22 or newer, or use `Client.remote`. |

## API responses

These are returned by `deoos-server` and by the in-process engine. The HTTP status is unchanged from the behavior the SDKs already branch on, except storage credentials (401 or 403) and a missing bucket (400), which used to be a generic 503 or a task-not-found 404.

| Code | Fix |
| --- | --- |
| TASK_NOT_FOUND | <a id="TASK_NOT_FOUND"></a> `python -m deoos list`, or submit that id. |
| SCHEDULE_NOT_FOUND | <a id="SCHEDULE_NOT_FOUND"></a> Check the id, or create the schedule with the SDK. |
| SIGNAL_NOT_FOUND | <a id="SIGNAL_NOT_FOUND"></a> `python -m deoos signal TASK SIGNAL VALUE` while a worker is running. |
| STEP_NOT_FOUND | <a id="STEP_NOT_FOUND"></a> Wait until the step is in `completed_steps`, then read it again. |
| OBJECT_NOT_FOUND | <a id="OBJECT_NOT_FOUND"></a> The key is absent. Check the id. If the bucket is missing, see `STORAGE_BUCKET_MISSING`. |
| DEFINITION_REQUIRED | <a id="DEFINITION_REQUIRED"></a> Call the SDK step, sleep, wait, spawn, or join before saving the result. |
| DEFINITION_MISSING | <a id="DEFINITION_MISSING"></a> This step was committed without a definition fingerprint. Submit a new task id. |
| OWNERSHIP_LOST | <a id="OWNERSHIP_LOST"></a> Stop this attempt. A worker claims the task again after the lease. Raise `LEASE_MS` (minimum 1000) if needed. |
| LEASE_EXPIRED | <a id="LEASE_EXPIRED"></a> Same as `OWNERSHIP_LOST`. The lease ran out during the commit. |
| ACTIVE_INDEX | <a id="ACTIVE_INDEX"></a> Set `EXECUTION_PREFIX` to a fresh namespace. Do not delete live task objects. |
| STORED_STATE | <a id="STORED_STATE"></a> Use a server from the same release that wrote the prefix, or a fresh `EXECUTION_PREFIX`. |
| TASK_ID_REUSED | <a id="TASK_ID_REUSED"></a> Resubmit the same handler and inputs, or choose a new id. |
| SCHEDULE_ID_REUSED | <a id="SCHEDULE_ID_REUSED"></a> Resubmit the same schedule definition, or choose a new id. |
| RETRY_STATE | <a id="RETRY_STATE"></a> Only failed or cancelled tasks can be retried. Waiting work resumes when a worker polls. |
| RETRY_CONFLICT | <a id="RETRY_CONFLICT"></a> Read the current revision and pass that `--revision`. |
| OPERATION_MISMATCH | <a id="OPERATION_MISMATCH"></a> Reuse the operation id only with the same body, or pick a new id. |
| SIGNAL_CONFLICT | <a id="SIGNAL_CONFLICT"></a> Repeat the same value and operation id, or choose a new signal name. |
| TERMINAL_SIGNAL | <a id="TERMINAL_SIGNAL"></a> The task already finished. Retry it first, or submit a new id. |
| TIMER_REUSED | <a id="TIMER_REUSED"></a> Sleep with the original duration, or choose a new timer name. |
| BACKFILL_ACTIVE | <a id="BACKFILL_ACTIVE"></a> A different backfill is still on this schedule. Repeat the same range or wait. |
| BACKFILL_INVALID | <a id="BACKFILL_INVALID"></a> Use a historical `[start_ms, end_ms)` and a limit from 1 to 1000. |
| ADMISSION_BUSY | <a id="ADMISSION_BUSY"></a> Retry the same operation id and body after a short backoff. |
| CONTENTION | <a id="CONTENTION"></a> Retry the same operation id and body. |
| UNKNOWN_ROUTE | <a id="UNKNOWN_ROUTE"></a> `ENGINE_URL` must be the origin only. Use `python -m deoos --help` or the SDK. |
| INVALID_NAME | <a id="INVALID_NAME"></a> Use 1-128 ASCII letters, digits, `.`, `_` or `-`, and not `.` or `..`. |
| INVALID_INTEGER | <a id="INVALID_INTEGER"></a> Pass an integer inside the safe range. |
| INVALID_REQUEST | <a id="INVALID_REQUEST"></a> The body is missing a field the error names. Send the shape the message shows. |
| INVALID_JSON | <a id="INVALID_JSON"></a> Send a JSON object and `Content-Type: application/json`. |
| INVALID_CONTENT_TYPE | <a id="INVALID_CONTENT_TYPE"></a> Set `Content-Type` to `application/json`. |
| AUTH_REQUIRED | <a id="AUTH_REQUIRED"></a> Set `ENGINE_TOKEN` to the token the server was started with and pass it to `Client.remote(url, token)`. |
| CROSS_ORIGIN | <a id="CROSS_ORIGIN"></a> Open the UI on this server's origin. SDK requests must not send `Origin`. |
| API_ERROR | <a id="API_ERROR"></a> The message did not match a more specific code. Run `python -m deoos --help`. |

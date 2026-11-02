//! User-facing errors: what happened, why, the fix, and a stable code.
use axum::http::StatusCode;
use object_store::Error as StoreError;
use std::error::Error as StdError;

pub fn text(code: &str, what: &str, cause: &str, fix: &str) -> String {
    format!("[{code}] {what} Cause: {cause} Fix: {fix} https://deoos.dev/errors#{code}")
}

pub fn guide(
    status: StatusCode,
    code: &str,
    what: &str,
    cause: &str,
    fix: &str,
) -> (StatusCode, String) {
    (status, text(code, what, cause, fix))
}

struct Known {
    needle: &'static str,
    code: &'static str,
    cause: &'static str,
    fix: &'static str,
}

const KNOWN: &[Known] = &[
    Known {
        needle: "task '",
        code: "TASK_NOT_FOUND",
        cause: "no state object exists for that id under this execution prefix",
        fix: "run `python -m deoos list`, or submit it with `python -m deoos submit ID HANDLER '{}'`",
    },
    Known {
        needle: "schedule '",
        code: "SCHEDULE_NOT_FOUND",
        cause: "no schedule object exists for that id under this execution prefix",
        fix: "check the id, or create the schedule with the SDK schedule call",
    },
    Known {
        needle: "signal '",
        code: "SIGNAL_NOT_FOUND",
        cause: "that signal has not been assigned on this task",
        fix: "send it with `python -m deoos signal TASK SIGNAL VALUE` while a worker is running",
    },
    Known {
        needle: "step not committed",
        code: "STEP_NOT_FOUND",
        cause: "that checkpoint has no saved result yet",
        fix: "wait until the step name is in completed_steps (`python -m deoos inspect ID`), then read it again",
    },
    Known {
        needle: "checkpoint definition changed",
        code: "WORKFLOW_NONDETERMINISTIC",
        cause: "replay called this step with a different kind, revision, or definition than the one already committed",
        fix: "keep the step name, kind, and revision stable for this task id, or submit a new id with `python -m deoos submit NEW_ID HANDLER '{}'`",
    },
    Known {
        needle: "checkpoint definition required",
        code: "DEFINITION_REQUIRED",
        cause: "a step result was written before its definition checkpoint",
        fix: "define the step with the SDK step, sleep, wait_signal, spawn, or join call before saving its result",
    },
    Known {
        needle: "checkpoint has no verifiable definition",
        code: "DEFINITION_MISSING",
        cause: "this step was committed by an older engine that did not store a definition fingerprint",
        fix: "submit a new task id for this workflow, or keep running it with a client that does not redefine the step",
    },
    Known {
        needle: "ownership lost",
        code: "OWNERSHIP_LOST",
        cause: "the lease ended or another worker claimed the task",
        fix: "stop this attempt. A worker can claim the task again after the lease. Set LEASE_MS (minimum 1000) if workers need longer",
    },
    Known {
        needle: "lease expired during operation",
        code: "LEASE_EXPIRED",
        cause: "the lease ran out before the commit",
        fix: "stop this attempt and let a worker claim the task again. Raise LEASE_MS (minimum 1000) if the callback needs longer",
    },
    Known {
        needle: "execution prefix has task records",
        code: "ACTIVE_INDEX",
        cause: "this prefix was written by an engine that did not create the active index",
        fix: "set EXECUTION_PREFIX to a fresh namespace. Do not delete live task objects",
    },
    Known {
        needle: "active intent",
        code: "ACTIVE_INDEX",
        cause: "the active-index write did not line up with the task record",
        fix: "retry the same request with the same operation id and body. If it persists, set EXECUTION_PREFIX to a fresh namespace",
    },
    Known {
        needle: "active index",
        code: "ACTIVE_INDEX",
        cause: "the stored active index is not a version this server can read",
        fix: "run a server from the same GitHub release as the one that wrote the prefix, or set EXECUTION_PREFIX to a fresh namespace",
    },
    Known {
        needle: "unsupported stored",
        code: "STORED_STATE",
        cause: "stored state was written by a different protocol version or is not valid JSON",
        fix: "use the deoos-server build that matches the stored data, or set EXECUTION_PREFIX to a fresh namespace and submit the task again",
    },
    Known {
        needle: "invalid stored",
        code: "STORED_STATE",
        cause: "an object in the execution prefix is missing or is not valid engine state",
        fix: "keep the prefix intact and retry. If the object was deleted, submit a new task id",
    },
    Known {
        needle: "unsupported or invalid stored",
        code: "STORED_STATE",
        cause: "stored state was written by a different protocol version or is corrupt",
        fix: "use a matching deoos-server, or set EXECUTION_PREFIX to a fresh namespace",
    },
    Known {
        needle: "task ID already used",
        code: "TASK_ID_REUSED",
        cause: "this id is already bound to a different handler, inputs, or attempt policy",
        fix: "resubmit the same handler and inputs, or choose a new id",
    },
    Known {
        needle: "schedule ID already used",
        code: "SCHEDULE_ID_REUSED",
        cause: "this schedule id is already bound to a different definition",
        fix: "resubmit the same definition, or choose a new schedule id",
    },
    Known {
        needle: "only failed or cancelled",
        code: "RETRY_STATE",
        cause: "retry is only for tasks that have already stopped",
        fix: "inspect the task. Waiting work resumes when a worker polls after its signal or timer. Do not retry it",
    },
    Known {
        needle: "task changed since inspection",
        code: "RETRY_CONFLICT",
        cause: "the revision passed to retry is not the current revision",
        fix: "read the current revision with `python -m deoos inspect ID --internal` and pass that --revision. Reuse the same --operation-id only when repeating this exact retry",
    },
    Known {
        needle: "retry operation ID reused",
        code: "OPERATION_MISMATCH",
        cause: "this operation id was already used for a different retry",
        fix: "reuse the operation id only with the same expected revision, or choose a new operation id",
    },
    Known {
        needle: "operation ID reused",
        code: "OPERATION_MISMATCH",
        cause: "this operation id was already committed with a different body",
        fix: "retry the same request with the same operation id and body, or choose a new operation id",
    },
    Known {
        needle: "signal already assigned",
        code: "SIGNAL_CONFLICT",
        cause: "this signal name already has a different value",
        fix: "repeat the same value and operation id, or choose a new signal name",
    },
    Known {
        needle: "cannot signal a terminal",
        code: "TERMINAL_SIGNAL",
        cause: "the task has already finished",
        fix: "retry a failed or cancelled task before signalling, or submit a new id",
    },
    Known {
        needle: "timer name reused",
        code: "TIMER_REUSED",
        cause: "this timer name was already committed with a different duration",
        fix: "sleep with the original duration, or choose a new timer name",
    },
    Known {
        needle: "another backfill",
        code: "BACKFILL_ACTIVE",
        cause: "this schedule already has a backfill with a different range",
        fix: "wait until that backfill finishes, or resume it with the same start and end",
    },
    Known {
        needle: "backfill",
        code: "BACKFILL_INVALID",
        cause: "the backfill range or limit is not one this schedule can run",
        fix: "pass a historical start_ms and end_ms with end_ms greater than start_ms, and a limit from 1 to 1000",
    },
    Known {
        needle: "storage claim admission busy",
        code: "ADMISSION_BUSY",
        cause: "too many claim requests are waiting on storage",
        fix: "retry with backoff. The limit is fixed per server; add workers only after `python -m deoos info` shows claims draining",
    },
    Known {
        needle: "storage renewal admission busy",
        code: "ADMISSION_BUSY",
        cause: "too many lease renewals are waiting on storage",
        fix: "retry the renewal. Raise LEASE_MS (minimum 1000) if renewals cannot keep up",
    },
    Known {
        needle: "storage busy",
        code: "ADMISSION_BUSY",
        cause: "the server's storage admission limit is full",
        fix: "retry the same request with the same operation id and body after a short backoff",
    },
    Known {
        needle: "contention;",
        code: "CONTENTION",
        cause: "another writer committed this object first",
        fix: "retry the same request with the same operation id and body",
    },
    Known {
        needle: "conditional write conflict",
        code: "STORAGE_CONFLICT",
        cause: "another writer committed this object first",
        fix: "retry the same request with the same operation id and body",
    },
    Known {
        needle: "unknown route",
        code: "UNKNOWN_ROUTE",
        cause: "the path is not a deoos API, or ENGINE_URL includes a path after the origin",
        fix: "set ENGINE_URL to the server origin only, usually http://127.0.0.1:7331, and use `python -m deoos --help` or the Python or TypeScript SDK",
    },
    Known {
        needle: "invalid task id",
        code: "INVALID_NAME",
        cause: "the task id is empty, too long, or contains characters deoos does not accept",
        fix: "use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "invalid handler",
        code: "INVALID_NAME",
        cause: "the handler name is empty, too long, or contains characters deoos does not accept",
        fix: "use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "invalid schedule id",
        code: "INVALID_NAME",
        cause: "the schedule id is empty, too long, or contains characters deoos does not accept",
        fix: "use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "invalid step",
        code: "INVALID_NAME",
        cause: "the step name is empty, too long, or contains characters deoos does not accept",
        fix: "use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "invalid signal",
        code: "INVALID_NAME",
        cause: "the signal or operation id is empty, too long, or contains characters deoos does not accept",
        fix: "use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "invalid timer",
        code: "INVALID_NAME",
        cause: "the timer name is empty, too long, or contains characters deoos does not accept",
        fix: "use 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "worker and handlers required",
        code: "INVALID_REQUEST",
        cause: "a claim needs a worker id and at least one handler name",
        fix: "pass a worker id and the handler map keys, as Client.run_worker does",
    },
    Known {
        needle: "invalid attempt budget",
        code: "INVALID_REQUEST",
        cause: "max_attempts was zero",
        fix: "set max_attempts to an integer >= 1. The SDK default is 3",
    },
    Known {
        needle: "operation_id required",
        code: "INVALID_REQUEST",
        cause: "the mutation did not include a valid operation id",
        fix: "send an operation_id of 1-128 ASCII letters, digits, '.', '_' or '-'. Reuse it only to retry the same body",
    },
    Known {
        needle: "valid retry operation",
        code: "INVALID_REQUEST",
        cause: "retry needs both an operation id and the expected revision",
        fix: "pass the revision from inspect and a stable operation id. Both must be 1-128 ASCII letters, digits, '.', '_' or '-'",
    },
    Known {
        needle: "definition",
        code: "INVALID_REQUEST",
        cause: "the checkpoint definition is missing a kind or revision",
        fix: "send kind (step, spawn, join, sleep, or wait_signal) and a revision of 1-128 ASCII letters, digits, '.', '_' or '-'",
    },
    Known {
        needle: "log requires",
        code: "INVALID_REQUEST",
        cause: "the log value is not a string, or it is longer than 4096 UTF-8 bytes",
        fix: "pass a string of at most 4096 UTF-8 bytes",
    },
    Known {
        needle: "wait condition",
        code: "INVALID_REQUEST",
        cause: "suspend needs exactly one of children, timer, or signal",
        fix: "send one wait condition, for example {\"signal\":\"go\"} or {\"timer\":{\"name\":\"nap\",\"milliseconds\":1000}}",
    },
    Known {
        needle: "children must",
        code: "INVALID_REQUEST",
        cause: "the child id list is empty, contains an invalid id, or includes the parent",
        fix: "pass other tasks' ids. Each id is 1-128 ASCII letters, digits, '.', '_' or '-', and not '.' or '..'",
    },
    Known {
        needle: "timer requires",
        code: "INVALID_REQUEST",
        cause: "the timer object is missing a name or milliseconds",
        fix: "send {\"name\":\"nap\",\"milliseconds\":1000} with a valid name and a nonnegative integer",
    },
    Known {
        needle: "unknown action",
        code: "UNKNOWN_ROUTE",
        cause: "the mutation path is not a deoos action",
        fix: "use the Python or TypeScript SDK, or `python -m deoos --help`",
    },
    Known {
        needle: "unknown wait",
        code: "INVALID_REQUEST",
        cause: "the wait object was not children, timer, or signal",
        fix: "send exactly one of those fields",
    },
    Known {
        needle: "delay exceeds",
        code: "INVALID_REQUEST",
        cause: "the delay does not fit in a millisecond timestamp",
        fix: "use a smaller retry_ms, lease, or timer duration",
    },
    Known {
        needle: "not found",
        code: "OBJECT_NOT_FOUND",
        cause: "the object key is not in the bucket",
        fix: "check the id with `python -m deoos list`. If the bucket itself is missing, create it and see STORAGE_BUCKET_MISSING",
    },
];

const DEFAULT: Known = Known {
    needle: "",
    code: "API_ERROR",
    cause: "the server rejected the request",
    fix: "read the message, then run `python -m deoos --help`. Codes are listed at https://deoos.dev/errors",
};

pub(crate) fn annotate(status: StatusCode, message: String) -> (StatusCode, String) {
    if message.contains("https://deoos.dev/errors#") {
        return (status, message);
    }
    let known = KNOWN
        .iter()
        .find(|entry| message.contains(entry.needle))
        .unwrap_or(&DEFAULT);
    (status, text(known.code, &message, known.cause, known.fix))
}

pub(crate) fn redact_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.split(['?', '#']).next().unwrap_or(endpoint);
    let Some(scheme) = trimmed.find("://") else {
        return trimmed.to_string();
    };
    let rest = &trimmed[scheme + 3..];
    if let Some(at) = rest.find('@') {
        format!("{}://{}", &trimmed[..scheme], &rest[at + 1..])
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn storage_target(
    provider: &str,
    bucket: &str,
    endpoint: Option<&str>,
    directory: Option<&str>,
) -> String {
    if provider == "filesystem" {
        return format!("filesystem directory '{}'", directory.unwrap_or("(unset)"));
    }
    let bucket = if bucket.is_empty() { "(unset)" } else { bucket };
    match endpoint
        .map(redact_endpoint)
        .filter(|value| !value.is_empty())
    {
        Some(endpoint) => format!("{provider} bucket '{bucket}' at {endpoint}"),
        None => format!("{provider} bucket '{bucket}'"),
    }
}

fn error_text(err: &StoreError) -> String {
    let mut text = err.to_string();
    let mut source = StdError::source(err);
    for _ in 0..8 {
        let Some(inner) = source else { break };
        text.push('\n');
        text.push_str(&inner.to_string());
        source = StdError::source(inner);
    }
    text
}

fn where_store(target: &str) -> String {
    if target.is_empty() {
        "the configured object store".to_string()
    } else {
        target.to_string()
    }
}

pub(crate) fn storage_failure(err: &StoreError, target: &str) -> (StatusCode, String) {
    if crate::local::is_durability_error(err) {
        eprintln!("storage: local durability barrier failed");
        return guide(
            crate::DURABILITY_FAILURE,
            "STORAGE_DURABILITY",
            "local durability barrier failed; operation outcome uncertain",
            "the filesystem could not make the write durable. Disk space, filesystem support, or directory permissions may be wrong",
            "check disk space and that DEOOS_STORAGE_DIRECTORY is a private writable directory. Do not assume the write failed; retry the same operation id",
        );
    }
    if matches!(err, StoreError::Generic { store: "DEOOS", .. }) {
        return guide(
            StatusCode::SERVICE_UNAVAILABLE,
            "STORAGE_DEADLINE",
            "storage deadline exceeded; write outcome uncertain",
            "the object store did not answer before the lease budget, so the write may have landed",
            "retry the same operation id and do not assume the write failed. If this persists, check AWS_ENDPOINT and raise LEASE_MS (minimum 1000)",
        );
    }
    let detail = error_text(err);
    let lower = detail.to_ascii_lowercase();
    let place = where_store(target);
    if lower.contains("nosuchbucket") || lower.contains("no such bucket") {
        return guide(
            StatusCode::BAD_REQUEST,
            "STORAGE_BUCKET_MISSING",
            &format!("object store bucket is missing for {place}"),
            "DEOOS_STORAGE_BUCKET names a bucket that does not exist, or the name does not match the bucket this endpoint is serving",
            "create it with `python scripts/ensure_bucket.py` after exporting AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ALLOW_HTTP=true, and DEOOS_STORAGE_BUCKET. `make dev` creates the deoos-dev bucket. Restart deoos-server after the bucket exists",
        );
    }
    let denied = matches!(
        err,
        StoreError::PermissionDenied { .. } | StoreError::Unauthenticated { .. }
    ) || lower.contains("invalidaccesskeyid")
        || lower.contains("signaturedoesnotmatch")
        || lower.contains("accessdenied")
        || lower.contains("invalidclienttokenid");
    if denied {
        let status = if matches!(err, StoreError::Unauthenticated { .. }) {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::FORBIDDEN
        };
        return guide(
            status,
            "STORAGE_CREDENTIALS",
            &format!("object store rejected credentials for {place}"),
            "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY does not match this store, or the key is not allowed to use the bucket. Local RustFS does not accept ambient cloud keys",
            "for local RustFS set AWS_ACCESS_KEY_ID=local-development, AWS_SECRET_ACCESS_KEY=local-development-only-secret (see compose.yaml), AWS_REGION=us-east-1, and AWS_ALLOW_HTTP=true. For a cloud bucket set that account's keys and AWS_REGION. Restart deoos-server after changing them",
        );
    }
    let unreachable = lower.contains("connection refused")
        || lower.contains("connect error")
        || lower.contains("tcp connect")
        || lower.contains("dns error")
        || lower.contains("name or service not known")
        || lower.contains("nodename nor servname")
        || lower.contains("network is unreachable")
        || lower.contains("timed out")
        || lower.contains("operation timed out")
        || lower.contains("error trying to connect")
        || lower.contains("connection reset")
        || lower.contains("unreachable");
    if unreachable {
        return guide(
            StatusCode::SERVICE_UNAVAILABLE,
            "STORAGE_UNREACHABLE",
            &format!("object store unreachable for {place}"),
            "the endpoint refused the connection, timed out, or could not be resolved. RustFS is often not running, or AWS_ENDPOINT is wrong",
            "from a checkout run `docker compose up -d` and `make dev`. Set AWS_ENDPOINT (local RustFS is http://127.0.0.1:19000), AWS_ALLOW_HTTP=true, AWS_REGION, and DEOOS_STORAGE_BUCKET. Confirm with `deoos-server --check-storage`",
        );
    }
    match err {
        StoreError::NotFound { .. } => (StatusCode::NOT_FOUND, "not found".into()),
        StoreError::Precondition { .. } | StoreError::AlreadyExists { .. } => guide(
            StatusCode::CONFLICT,
            "STORAGE_CONFLICT",
            "conditional write conflict",
            "another writer committed this object first, or this request was retried with a different body",
            "retry the same request with the same operation id and body",
        ),
        _ => {
            eprintln!("storage: {}", crate::qualification::error_kind(err));
            guide(
                StatusCode::SERVICE_UNAVAILABLE,
                "STORAGE_UNAVAILABLE",
                &format!("storage unavailable for {place}"),
                "the object store returned an error that is not a missing bucket, rejected credentials, or an unreachable endpoint",
                "retry shortly and check credentials, endpoint, and bucket. Set DEOOS_STORAGE_BUCKET, AWS_ENDPOINT, AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY, then run `deoos-server --check-storage`",
            )
        }
    }
}

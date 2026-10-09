use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{
    ObjectStore, PutMode, PutOptions, UpdateVersion, aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder, path::Path as Key,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
const PROTOCOL_VERSION: u32 = 3;
mod active;
mod discovery;
mod local;
mod qualification;
mod storage_io;
// Keep a failed filesystem durability barrier distinguishable after API-error
// conversion, so an outer operation cannot acknowledge it through readback.
const DURABILITY_FAILURE: StatusCode = StatusCode::INSUFFICIENT_STORAGE;

#[derive(Clone)]
pub struct Engine {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    lease_ms: u64,
    discovery: Arc<std::sync::Mutex<discovery::Hints>>,
    active_ready: Arc<tokio::sync::Mutex<bool>>,
    storage_control: Arc<storage_io::StorageControl>,
    storage_tracing: tracing::Dispatch,
    storage_deadline: Option<tokio::time::Instant>,
    storage_reconciliation_deadline: Option<tokio::time::Instant>,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct Task {
    version: u32,
    id: String,
    handler: String,
    inputs: Value,
    status: String,
    attempts: u32,
    max_attempts: u32,
    retry_ms: u64,
    available_at: u64,
    owner: Option<String>,
    token: Option<String>,
    expires_at: u64,
    steps: BTreeMap<String, String>,
    output: Option<Value>,
    error: Option<String>,
    revision: String,
    last_operation: String,
    #[serde(default)]
    last_operation_fingerprint: Option<String>,
    #[serde(default)]
    waiting_on: Option<WaitCondition>,
    #[serde(default)]
    timers: BTreeMap<String, Timer>,
    #[serde(default)]
    signals: BTreeMap<String, String>,
    #[serde(default)]
    definitions: BTreeMap<String, String>,
    #[serde(default)]
    schedule: Option<ScheduleRun>,
    #[serde(default)]
    history: Vec<HistoryEvent>,
    #[serde(default)]
    last_retry_operation: Option<String>,
    #[serde(default)]
    last_retry_fingerprint: Option<String>,
    #[serde(default)]
    active_entry_id: Option<String>,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
struct HistoryEvent {
    at_ms: u64,
    event: String,
    attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}
impl Task {
    fn record(&mut self, event: &str, detail: Option<String>) {
        let detail = detail.map(|text| {
            let mut end = text.len().min(4096);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text[..end].to_owned()
        });
        self.history.push(HistoryEvent {
            at_ms: now(),
            event: event.into(),
            attempts: self.attempts,
            detail,
        });
        if self.history.len() > 32 {
            self.history.drain(..self.history.len() - 32);
        }
    }
}
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct ScheduleRun {
    id: String,
    scheduled_at: u64,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
struct Timer {
    milliseconds: u64,
    deadline: u64,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WaitCondition {
    Children { ids: Vec<String> },
    Timer { name: String },
    Signal { name: String },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerRequest {
    name: String,
    milliseconds: u64,
}
#[derive(Deserialize)]
struct SignalRequest {
    operation_id: String,
    value: Value,
}
#[derive(Deserialize)]
struct RetryRequest {
    operation_id: String,
    expected_revision: String,
}
#[derive(Deserialize)]
struct Submit {
    id: String,
    handler: String,
    #[serde(default)]
    inputs: Value,
    #[serde(default = "default_attempts")]
    max_attempts: u32,
    #[serde(default)]
    retry_ms: u64,
    #[serde(default)]
    schedule: Option<ScheduleRun>,
}
fn default_attempts() -> u32 {
    3
}
#[derive(Deserialize)]
struct Claim {
    worker: String,
    handlers: Vec<String>,
}
#[derive(Deserialize)]
struct Mutation {
    token: String,
    operation_id: String,
    #[serde(default)]
    value: Value,
}
type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn valid(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
fn bad(s: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, s.into())
}
fn conflict(s: &str) -> (StatusCode, String) {
    (StatusCode::CONFLICT, s.into())
}
fn after_ms(delay: u64) -> Result<u64, (StatusCode, String)> {
    now()
        .checked_add(delay)
        .ok_or_else(|| bad("delay exceeds supported timestamp range"))
}
fn normalized_json(value: Value) -> Value {
    match value {
        Value::Number(number) if number.is_f64() => {
            let value = number.as_f64().unwrap();
            // Python emits 5.0 where JavaScript emits 5. Never round large integers.
            if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991.0 {
                json!(value as i64)
            } else {
                Value::Number(number)
            }
        }
        Value::Array(values) => Value::Array(values.into_iter().map(normalized_json).collect()),
        Value::Object(mut values) => {
            for value in values.values_mut() {
                *value = normalized_json(std::mem::take(value));
            }
            Value::Object(values)
        }
        other => other,
    }
}
fn storage(err: object_store::Error) -> (StatusCode, String) {
    if local::is_durability_error(&err) {
        eprintln!("storage: local durability barrier failed");
        return (DURABILITY_FAILURE, "local durability barrier failed; operation outcome uncertain; check disk space, filesystem support and write permissions".into());
    }
    if matches!(&err, object_store::Error::Generic { store: "DEOOS", .. }) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "storage deadline exceeded; write outcome uncertain".into(),
        );
    }
    match err {
        object_store::Error::NotFound { .. } => (StatusCode::NOT_FOUND, "not found".into()),
        object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. } => {
            conflict("conditional write conflict")
        }
        _ => {
            eprintln!("storage: {}", qualification::error_kind(&err));
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "storage unavailable".into(),
            )
        }
    }
}
impl Engine {
    /// Check the storage primitives used by the execution protocol in an isolated namespace.
    pub async fn check_storage(&self) -> Result<Value, String> {
        qualification::check(self.store.as_ref(), &self.prefix).await
    }

    fn key(&self, id: &str) -> Key {
        Key::from(format!("{}/tasks/{id}/state.json", self.prefix))
    }
    async fn read(&self, id: &str) -> Result<(Task, UpdateVersion), (StatusCode, String)> {
        if !valid(id) {
            return Err(bad("invalid task id"));
        }
        let (meta, bytes) = self.get_bytes(&self.key(id)).await.map_err(storage)?;
        let v = UpdateVersion {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        };
        let t: Task = serde_json::from_slice(&bytes)
            .map_err(|_| conflict("unsupported or invalid stored task"))?;
        t.validate(id)?;
        Ok((t, v))
    }
    async fn write(&self, t: &Task, mode: PutMode) -> Result<(), (StatusCode, String)> {
        let payload = Bytes::from(serde_json::to_vec(t).unwrap());
        match self.put_object(&self.key(&t.id), payload, mode).await {
            Ok(_) => {
                self.after_state_write(t).await;
                Ok(())
            }
            Err(err) => {
                if local::is_durability_error(&err) {
                    return Err(storage(err));
                }
                // Reconcile an uncertain response. A revision is unique to this exact proposed write.
                if let Ok((actual, _)) = self.reconciliation().read(&t.id).await
                    && actual.revision == t.revision
                {
                    self.after_state_write(&actual).await;
                    return Ok(());
                }
                Err(storage(err))
            }
        }
    }
    async fn tasks(&self) -> Result<Value, (StatusCode, String)> {
        let prefix = Key::from(format!("{}/tasks", self.prefix));
        let expected = format!("{prefix}/");
        let objects: Vec<_> = self.list_objects(&prefix).await.map_err(storage)?;
        let mut ids: Vec<_> = objects
            .into_iter()
            .filter_map(|object| {
                let path = object.location.to_string();
                path.strip_prefix(&expected)
                    .and_then(|v| v.strip_suffix("/state.json"))
                    .filter(|v| valid(v))
                    .map(str::to_owned)
            })
            .collect();
        ids.sort();
        let truncated = ids.len() > 100;
        // Each extra read lane consumes ordinary admission, preserving the global storage limit.
        let mut extra_permits = Vec::new();
        for _ in 0..3 {
            match self.storage_control.ordinary.try_acquire() {
                Ok(permit) => extra_permits.push(permit),
                Err(_) => break,
            }
        }
        let mut reads = futures::stream::iter(ids.into_iter().take(100))
            .map(|id| async move { self.read(&id).await })
            .buffered(1 + extra_permits.len());
        let mut tasks = Vec::new();
        while let Some(result) = reads.next().await {
            match result {
                Ok((task, _)) => tasks.push(task),
                Err((StatusCode::NOT_FOUND, _)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(json!({"tasks": tasks, "truncated": truncated}))
    }
    async fn retry(&self, id: &str, request: RetryRequest) -> ApiResult<Task> {
        if !valid(&request.operation_id) || !valid(&request.expected_revision) {
            return Err(bad(
                "valid retry operation ID and expected revision required",
            ));
        }
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&json!({"expected_revision": request.expected_revision}))
                    .unwrap()
            )
        );
        for _ in 0..16 {
            self.check_budget()?;
            let (mut task, version) = self.read(id).await?;
            if task.last_retry_operation.as_deref() == Some(&request.operation_id) {
                return if task.last_retry_fingerprint.as_deref() == Some(&fingerprint) {
                    Ok(Json(task))
                } else {
                    Err(conflict("retry operation ID reused with different request"))
                };
            }
            if task.revision != request.expected_revision {
                return Err(conflict("task changed since inspection"));
            }
            if !["failed", "cancelled"].contains(&task.status.as_str()) {
                return Err(conflict("only failed or cancelled tasks can be retried"));
            }
            task.status = "queued".into();
            task.attempts = 0;
            task.available_at = 0;
            task.owner = None;
            task.token = None;
            task.expires_at = 0;
            task.error = None;
            task.output = None;
            task.waiting_on = None;
            task.last_operation.clear();
            task.last_operation_fingerprint = None;
            task.last_retry_operation = Some(request.operation_id.clone());
            task.last_retry_fingerprint = Some(fingerprint.clone());
            task.record("retry", None);
            task.revision = Uuid::new_v4().to_string();
            task.active_entry_id = Some(Uuid::new_v4().to_string());
            self.publish_active(&task, Some(request.expected_revision.clone()))
                .await?;
            match self.write(&task, PutMode::Update(version)).await {
                Ok(()) => return Ok(Json(task)),
                Err(error) => {
                    if error.0 == DURABILITY_FAILURE {
                        return Err(error);
                    }
                    if let Ok((actual, _)) = self.read(id).await
                        && actual.last_retry_operation.as_deref() == Some(&request.operation_id)
                    {
                        return if actual.last_retry_fingerprint.as_deref() == Some(&fingerprint) {
                            Ok(Json(actual))
                        } else {
                            Err(conflict("retry operation ID reused with different request"))
                        };
                    }
                    if error.0 == StatusCode::CONFLICT {
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        Err(conflict("contention; retry"))
    }
    async fn children_terminal(&self, ids: &[String]) -> Result<bool, (StatusCode, String)> {
        for id in ids {
            match self.read(id).await {
                Ok((child, _))
                    if ["completed", "failed", "cancelled"].contains(&child.status.as_str()) => {}
                Ok(_) | Err((StatusCode::NOT_FOUND, _)) => return Ok(false),
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }
    async fn ready(
        &self,
        task: &Task,
        condition: &WaitCondition,
    ) -> Result<bool, (StatusCode, String)> {
        match condition {
            WaitCondition::Children { ids } => self.children_terminal(ids).await,
            WaitCondition::Timer { name } => Ok(task
                .timers
                .get(name)
                .is_some_and(|timer| timer.deadline <= now())),
            WaitCondition::Signal { name } => Ok(task.signals.contains_key(name)),
        }
    }
    async fn payload(&self, key: &str) -> Result<Value, (StatusCode, String)> {
        let (_, bytes) = self
            .get_bytes(&Key::parse(key).map_err(|_| conflict("invalid stored payload key"))?)
            .await
            .map_err(storage)?;
        serde_json::from_slice(&bytes).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid stored payload".into(),
            )
        })
    }
    async fn signal(&self, id: &str, name: &str, request: SignalRequest) -> ApiResult<Task> {
        if !valid(name) || !valid(&request.operation_id) {
            return Err(bad("invalid signal name or operation ID"));
        }
        let result_key = Key::from(format!(
            "{}/tasks/{id}/signals/{name}/{}.json",
            self.prefix, request.operation_id
        ));
        let mut uploaded = false;
        for _ in 0..16 {
            self.check_budget()?;
            let (mut task, version) = self.read(id).await?;
            if let Some(key) = task.signals.get(name) {
                return if normalized_json(self.payload(key).await?)
                    == normalized_json(request.value.clone())
                {
                    Ok(Json(task))
                } else {
                    Err(conflict("signal already assigned a different value"))
                };
            }
            if ["completed", "failed", "cancelled"].contains(&task.status.as_str()) {
                return Err(conflict("cannot signal a terminal task"));
            }
            if !uploaded {
                let data = Bytes::from(serde_json::to_vec(&request.value).unwrap());
                if let Err(error) = self.put_object(&result_key, data, PutMode::Create).await {
                    if local::is_durability_error(&error) {
                        return Err(storage(error));
                    }
                    match self.reconciliation().payload(result_key.as_ref()).await {
                        Ok(existing) => {
                            if normalized_json(existing) != normalized_json(request.value.clone()) {
                                return Err(conflict(
                                    "operation ID reused with different signal value",
                                ));
                            }
                        }
                        Err(_) => return Err(storage(error)),
                    }
                }
                uploaded = true;
            }
            task.signals.insert(name.into(), result_key.to_string());
            task.record("signal", Some(name.into()));
            task.revision = Uuid::new_v4().to_string();
            match self.write(&task, PutMode::Update(version)).await {
                Ok(()) => return Ok(Json(task)),
                Err(error) => {
                    // A heartbeat may supersede the acknowledged revision after a lost response.
                    if error.0 == DURABILITY_FAILURE {
                        return Err(error);
                    }
                    if let Ok((actual, _)) = self.read(id).await
                        && actual
                            .signals
                            .get(name)
                            .is_some_and(|key| key == result_key.as_ref())
                    {
                        return Ok(Json(actual));
                    }
                    if error.0 == StatusCode::CONFLICT {
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        Err(conflict("contention; retry"))
    }
    async fn cancel(&self, id: &str) -> ApiResult<Task> {
        for _ in 0..16 {
            self.check_budget()?;
            let (mut task, version) = self.read(id).await?;
            if task.status == "cancelled" {
                return Ok(Json(task));
            }
            if ["completed", "failed"].contains(&task.status.as_str()) {
                return Err(conflict("cannot cancel a terminal task"));
            }
            task.status = "cancelled".into();
            task.expires_at = 0;
            task.owner = None;
            task.token = None;
            task.waiting_on = None;
            task.last_operation.clear();
            task.last_operation_fingerprint = None;
            task.record("cancel", None);
            task.revision = Uuid::new_v4().to_string();
            match self.write(&task, PutMode::Update(version)).await {
                Ok(()) => return Ok(Json(task)),
                Err((StatusCode::CONFLICT, _)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(conflict("contention; retry"))
    }
    async fn mutate(
        &self,
        id: &str,
        m: Mutation,
        action: &str,
        step: Option<&str>,
    ) -> ApiResult<Task> {
        if !valid(&m.operation_id) {
            return Err(bad("operation_id required"));
        }
        if step.is_some_and(|s| !valid(s)) {
            return Err(bad("invalid step"));
        }
        let operation_request =
            normalized_json(json!({"action": action, "step": step, "value": m.value}));
        // serde_json's default ordered maps make object key order irrelevant to retries.
        let operation_fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&operation_request).unwrap())
        );
        for _ in 0..16 {
            self.check_budget()?;
            let (mut t, v) = self.read(id).await?;
            if action != "define"
                && t.last_operation == m.operation_id
                && t.token.as_deref() == Some(&m.token)
            {
                return if t.last_operation_fingerprint.as_ref() == Some(&operation_fingerprint) {
                    Ok(Json(t))
                } else {
                    Err(conflict(
                        "operation ID reused with a different or unverifiable request",
                    ))
                };
            }
            if t.status != "running"
                || t.token.as_deref() != Some(&m.token)
                || t.expires_at <= now()
            {
                return Err(conflict("ownership lost or expired"));
            }
            let owned_until = t.expires_at;
            if action == "define"
                && t.last_operation == m.operation_id
                && t.last_operation_fingerprint.as_ref() != Some(&operation_fingerprint)
            {
                return Err(conflict(
                    "operation ID reused with different definition request",
                ));
            }
            match action {
                "renew" => t.expires_at = after_ms(self.lease_ms)?,
                "log" => {
                    if m.value.as_str().is_none_or(|message| message.len() > 4096) {
                        return Err(bad("log requires a string of at most 4096 UTF-8 bytes"));
                    }
                }
                "define" => {
                    let name = step.unwrap();
                    let definition = m
                        .value
                        .as_object()
                        .ok_or_else(|| bad("definition requires kind and revision"))?;
                    let kind = definition
                        .get("kind")
                        .and_then(Value::as_str)
                        .ok_or_else(|| bad("definition kind required"))?;
                    if !["step", "spawn", "join", "sleep", "wait_signal"].contains(&kind) {
                        return Err(bad("unknown definition kind"));
                    }
                    definition
                        .get("revision")
                        .and_then(Value::as_str)
                        .filter(|revision| valid(revision))
                        .ok_or_else(|| bad("valid definition revision required"))?;
                    if let Some(existing) = t.definitions.get(name) {
                        return if existing == &operation_fingerprint {
                            Ok(Json(t))
                        } else {
                            Err(conflict("checkpoint definition changed"))
                        };
                    }
                    if t.steps.contains_key(name) {
                        return Err(conflict("checkpoint has no verifiable definition"));
                    }
                    t.definitions
                        .insert(name.into(), operation_fingerprint.clone());
                }
                "step" => {
                    let name = step.unwrap();
                    if !t.definitions.contains_key(name) {
                        return Err(conflict("checkpoint definition required"));
                    }
                    if t.steps.contains_key(name) {
                        return Ok(Json(t));
                    }
                    let result_key = Key::from(format!(
                        "{}/tasks/{id}/results/{name}/{:x}/{}.json",
                        self.prefix,
                        Sha256::digest(m.token.as_bytes()),
                        m.operation_id
                    ));
                    let data = Bytes::from(serde_json::to_vec(&m.value).unwrap());
                    match self.put_object(&result_key, data, PutMode::Create).await {
                        Ok(_) => {}
                        Err(object_store::Error::AlreadyExists { .. })
                        | Err(object_store::Error::Precondition { .. }) => {
                            let (_, existing) =
                                self.get_bytes(&result_key).await.map_err(storage)?;
                            let existing_value: Value = serde_json::from_slice(&existing)
                                .map_err(|_| conflict("invalid existing result"))?;
                            if normalized_json(existing_value) != normalized_json(m.value.clone()) {
                                return Err(conflict("operation ID reused with different result"));
                            }
                        }
                        Err(e) => return Err(storage(e)),
                    }
                    t.steps.insert(name.into(), result_key.to_string());
                }
                "complete" => {
                    t.status = "completed".into();
                    t.output = Some(m.value.clone());
                    t.expires_at = 0;
                    t.waiting_on = None;
                }
                "fail" => {
                    let terminal = m.value.get("terminal").and_then(Value::as_bool) == Some(true);
                    t.error = Some(
                        m.value
                            .get("error")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| m.value.to_string()),
                    );
                    t.status = if !terminal && t.attempts < t.max_attempts {
                        "queued"
                    } else {
                        "failed"
                    }
                    .into();
                    t.available_at = after_ms(t.retry_ms)?;
                    t.expires_at = 0;
                    t.waiting_on = None;
                }
                "suspend" => {
                    let fields = m
                        .value
                        .as_object()
                        .ok_or_else(|| bad("one wait condition required"))?;
                    if fields.len() != 1 {
                        return Err(bad("exactly one wait condition required"));
                    }
                    let condition = if let Some(children) = fields.get("children") {
                        let ids: Vec<String> = serde_json::from_value(children.clone())
                            .map_err(|_| bad("children must be task IDs"))?;
                        if ids.is_empty() || ids.iter().any(|child| !valid(child) || child == id) {
                            return Err(bad(
                                "children must be nonempty valid task IDs other than the parent",
                            ));
                        }
                        WaitCondition::Children { ids }
                    } else if let Some(timer) = fields.get("timer") {
                        let timer: TimerRequest = serde_json::from_value(timer.clone())
                            .map_err(|_| bad("timer requires name and milliseconds"))?;
                        if !valid(&timer.name) {
                            return Err(bad("invalid timer name"));
                        }
                        if let Some(existing) = t.timers.get(&timer.name) {
                            if existing.milliseconds != timer.milliseconds {
                                return Err(conflict("timer name reused with different duration"));
                            }
                        } else {
                            t.timers.insert(
                                timer.name.clone(),
                                Timer {
                                    milliseconds: timer.milliseconds,
                                    deadline: after_ms(timer.milliseconds)?,
                                },
                            );
                        }
                        WaitCondition::Timer { name: timer.name }
                    } else if let Some(signal) = fields.get("signal") {
                        let name = signal
                            .as_str()
                            .filter(|name| valid(name))
                            .ok_or_else(|| bad("invalid signal name"))?;
                        WaitCondition::Signal { name: name.into() }
                    } else {
                        return Err(bad("unknown wait condition"));
                    };
                    // Existing join SDKs always stop after a child suspension acknowledgment.
                    if matches!(condition, WaitCondition::Children { .. })
                        || !self.ready(&t, &condition).await?
                    {
                        t.waiting_on = Some(condition);
                        t.status = "waiting".into();
                        t.expires_at = 0;
                    }
                }
                _ => return Err(bad("unknown action")),
            }
            if owned_until <= now() {
                return Err(conflict("lease expired during operation"));
            }
            let event = match action {
                "step" => Some(("checkpoint", step.map(str::to_owned))),
                "fail" => Some(("fail", t.error.clone())),
                "complete" => Some(("complete", None)),
                "log" => Some(("log", m.value.as_str().map(str::to_owned))),
                "suspend" if t.status == "waiting" => {
                    let detail = match &t.waiting_on {
                        Some(WaitCondition::Children { ids }) => format!("children:{}", ids.len()),
                        Some(WaitCondition::Timer { name }) => format!("timer:{name}"),
                        Some(WaitCondition::Signal { name }) => format!("signal:{name}"),
                        None => String::new(),
                    };
                    Some(("suspend", Some(detail)))
                }
                _ => None,
            };
            if let Some((event, detail)) = event {
                t.record(event, detail);
            }
            t.last_operation = m.operation_id.clone();
            t.last_operation_fingerprint = Some(operation_fingerprint.clone());
            t.revision = Uuid::new_v4().to_string();
            match self.write(&t, PutMode::Update(v)).await {
                Ok(()) => return Ok(Json(t)),
                Err((StatusCode::CONFLICT, _)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict("contention; retry"))
    }
}
async fn submit(State(e): State<Engine>, Json(s): Json<Submit>) -> ApiResult<Task> {
    if !valid(&s.id) || !valid(&s.handler) || s.max_attempts == 0 {
        return Err(bad("invalid id, handler or attempt budget"));
    }
    after_ms(s.retry_ms)?;
    let mut t = Task {
        version: PROTOCOL_VERSION,
        id: s.id,
        handler: s.handler,
        inputs: s.inputs,
        status: "queued".into(),
        attempts: 0,
        max_attempts: s.max_attempts,
        retry_ms: s.retry_ms,
        available_at: 0,
        owner: None,
        token: None,
        expires_at: 0,
        steps: BTreeMap::new(),
        output: None,
        error: None,
        revision: Uuid::new_v4().to_string(),
        last_operation: String::new(),
        last_operation_fingerprint: None,
        waiting_on: None,
        timers: BTreeMap::new(),
        signals: BTreeMap::new(),
        definitions: BTreeMap::new(),
        schedule: s.schedule,
        history: Vec::new(),
        last_retry_operation: None,
        last_retry_fingerprint: None,
        active_entry_id: Some(Uuid::new_v4().to_string()),
    };
    t.record("submit", None);
    match e.read(&t.id).await {
        Ok((existing, _)) => return submitted_existing(existing, &t),
        Err((StatusCode::NOT_FOUND, _)) => {}
        Err(error) => return Err(error),
    }
    e.publish_active(&t, None).await?;
    match e.write(&t, PutMode::Create).await {
        Ok(()) => Ok(Json(t)),
        Err((StatusCode::CONFLICT, _)) => {
            let (existing, _) = e.read(&t.id).await?;
            submitted_existing(existing, &t)
        }
        Err(err) => Err(err),
    }
}
fn submitted_existing(existing: Task, submitted: &Task) -> ApiResult<Task> {
    if existing.handler != submitted.handler
        || normalized_json(existing.inputs.clone()) != normalized_json(submitted.inputs.clone())
        || existing.max_attempts != submitted.max_attempts
        || existing.retry_ms != submitted.retry_ms
        || existing.schedule != submitted.schedule
    {
        return Err(conflict("task ID already used with different definition"));
    }
    Ok(Json(existing))
}
async fn claim(State(e): State<Engine>, Json(c): Json<Claim>) -> ApiResult<Value> {
    if c.worker.is_empty() || c.handlers.is_empty() {
        return Err(bad("worker and handlers required"));
    }
    e.ensure_active().await?;
    schedules::tick(&e, &c.handlers).await?;
    let candidates = e.active_candidates().await?;
    let start = e.discovery.lock().unwrap().start(&candidates);
    let priority_id = e.discovery.lock().unwrap().take_priority();
    let priority = priority_id
        .as_ref()
        .and_then(|id| candidates.iter().find(|(candidate, _)| candidate == id));
    let normal = candidates[start..]
        .iter()
        .chain(candidates[..start].iter())
        .filter(|(id, _)| priority_id.as_ref() != Some(id));
    for (id, obj, prioritized) in priority
        .into_iter()
        .map(|(id, obj)| (id, obj, true))
        .chain(normal.map(|(id, obj)| (id, obj, false)))
    {
        if !prioritized {
            e.discovery.lock().unwrap().examined(id);
        }
        let Some((mut t, v)) = e.resolve_active(&obj.location).await? else {
            continue;
        };
        if !c.handlers.contains(&t.handler) || t.available_at > now() {
            continue;
        }
        let resuming = t.status == "waiting";
        if resuming {
            let Some(condition) = &t.waiting_on else {
                continue;
            };
            if !e.ready(&t, condition).await? {
                e.discovery.lock().unwrap().prioritize(&t);
                continue;
            }
        } else if t.status != "queued" && !(t.status == "running" && t.expires_at <= now()) {
            continue;
        }
        if !resuming && t.attempts >= t.max_attempts {
            t.status = "failed".into();
            t.error = Some("attempt budget exhausted during recovery".into());
            t.record("fail", t.error.clone());
        } else {
            t.status = "running".into();
            if !resuming {
                t.attempts += 1;
            }
            t.owner = Some(c.worker.clone());
            t.token = Some(Uuid::new_v4().to_string());
            t.expires_at = after_ms(e.lease_ms)?;
            t.record(if resuming { "resume" } else { "claim" }, None);
        }
        t.revision = Uuid::new_v4().to_string();
        t.last_operation.clear();
        t.last_operation_fingerprint = None;
        t.waiting_on = None;
        match e.write(&t, PutMode::Update(v)).await {
            Ok(()) if t.status == "running" => return Ok(Json(json!({"task":t}))),
            Ok(()) => {}
            Err((StatusCode::CONFLICT, _)) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(Json(json!({"task":null})))
}
async fn inspect(State(e): State<Engine>, Path(id): Path<String>) -> ApiResult<Task> {
    Ok(Json(e.read(&id).await?.0))
}
// The default human-facing view; storage paths and ownership metadata stay internal.
fn task_view(t: &Task) -> Value {
    let mut view = json!({"id":t.id, "function":t.handler, "status":t.status,
        "inputs":t.inputs, "output":t.output, "error":t.error, "attempts":t.attempts,
        "completed_steps":t.steps.keys().collect::<Vec<_>>()});
    if t.status == "waiting" {
        view["wait"] = execution_summary(t)["wait"].clone();
    }
    view
}
fn task_history(t: &Task) -> Value {
    json!({"history":t.history.iter().map(|event| {
        let mut value = json!({"at_ms":event.at_ms, "event":event.event});
        if let Some(detail) = &event.detail { value["detail"] = json!(detail); }
        value
    }).collect::<Vec<_>>()})
}
// A state-only projection: never fetch checkpoint values or discover other tasks.
fn execution_summary(t: &Task) -> Value {
    let wait = if t.status == "waiting" {
        match &t.waiting_on {
            Some(WaitCondition::Timer { name }) => json!({"kind":"timer", "name":name,
                "deadline_ms":t.timers.get(name).map(|timer| timer.deadline)}),
            Some(WaitCondition::Signal { name }) => json!({"kind":"signal", "name":name,
                "assigned":t.signals.contains_key(name)}),
            Some(WaitCondition::Children { ids }) => json!({"kind":"children", "ids":ids}),
            None => Value::Null,
        }
    } else {
        Value::Null
    };
    let failure = t.history.iter().rev().find(|event| event.event == "fail");
    let message = failure
        .and_then(|event| event.detail.as_ref())
        .or(t.error.as_ref());
    let last_failure = message.map(|message| {
        let clean: String = message
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        let mut end = clean.len().min(512);
        while !clean.is_char_boundary(end) {
            end -= 1;
        }
        json!({"message":&clean[..end], "at_ms":failure.map(|event| event.at_ms)})
    });
    let mut actions = Vec::new();
    match t.status.as_str() {
        "queued" | "running" | "waiting" => actions.push("cancel"),
        "failed" | "cancelled" => actions.push("retry"),
        _ => {}
    }
    if t.status == "waiting"
        && let Some(WaitCondition::Signal { name }) = &t.waiting_on
        && !t.signals.contains_key(name)
    {
        actions.push("signal");
    }
    json!({"summary_version":1, "id":t.id, "handler":t.handler, "status":t.status,
        "attempts":t.attempts, "max_attempts":t.max_attempts,
        "available_at_ms":t.available_at, "completed_steps":t.steps.keys().collect::<Vec<_>>(),
        "wait":wait, "last_failure":last_failure, "actions":actions})
}
async fn result(
    State(e): State<Engine>,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<Value> {
    let (t, _) = e.read(&id).await?;
    let key = t
        .steps
        .get(&name)
        .ok_or((StatusCode::NOT_FOUND, "step not committed".into()))?;
    let (_, bytes) = e
        .get_bytes(&Key::parse(key).map_err(|_| conflict("invalid stored result key"))?)
        .await
        .map_err(storage)?;
    Ok(Json(serde_json::from_slice(&bytes).map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "invalid result".into())
    })?))
}

#[derive(Deserialize)]
pub struct Config {
    pub provider: Option<String>,
    #[serde(default)]
    pub bucket: String,
    pub directory: Option<String>,
    pub prefix: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub allow_http: Option<bool>,
    pub lease_ms: Option<u64>,
}
impl Engine {
    pub fn from_config(c: Config) -> Result<Self, String> {
        let lease_ms = c.lease_ms.unwrap_or(30000);
        if lease_ms < 1000 {
            return Err("lease_ms must be >= 1000".into());
        }
        after_ms(lease_ms).map_err(|(_, message)| message)?;
        let timeout = format!("{}ms", (lease_ms / 8).min(2000));
        let retry = object_store::RetryConfig {
            max_retries: 2,
            retry_timeout: std::time::Duration::from_millis((lease_ms / 24).min(1000)),
            backoff: object_store::BackoffConfig {
                init_backoff: std::time::Duration::from_millis((lease_ms / 32).min(500)),
                max_backoff: std::time::Duration::from_millis((lease_ms / 16).min(1000)),
                ..Default::default()
            },
        };
        let provider = c
            .provider
            .or_else(|| std::env::var("DEOOS_STORAGE_PROVIDER").ok())
            .unwrap_or_else(|| "s3".into());
        let store: Arc<dyn ObjectStore> = match provider.as_str() {
            "filesystem" => {
                if !c.bucket.is_empty()
                    || c.region.is_some()
                    || c.endpoint.is_some()
                    || c.access_key_id.is_some()
                    || c.secret_access_key.is_some()
                    || c.session_token.is_some()
                    || c.allow_http.is_some()
                {
                    return Err("filesystem storage uses directory, not bucket or cloud credential/endpoint options".into());
                }
                let directory = c
                    .directory
                    .or_else(|| std::env::var("DEOOS_STORAGE_DIRECTORY").ok())
                    .filter(|value| !value.is_empty())
                    .ok_or("filesystem storage requires directory or DEOOS_STORAGE_DIRECTORY")?;
                let absolute = std::path::absolute(directory)
                    .map_err(|_| "invalid local storage directory")?;
                local::open(absolute)
                    .map_err(|error| format!("cannot initialize filesystem storage: {error}"))?
            }
            "s3" => {
                if c.directory.is_some() {
                    return Err("directory requires provider=filesystem".into());
                }
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(c.bucket)
                    .with_retry(retry.clone())
                    .with_config(
                        object_store::aws::AmazonS3ConfigKey::Client(
                            object_store::ClientConfigKey::Timeout,
                        ),
                        timeout.clone(),
                    )
                    .with_config(
                        object_store::aws::AmazonS3ConfigKey::Client(
                            object_store::ClientConfigKey::ConnectTimeout,
                        ),
                        timeout.clone(),
                    );
                if let Some(v) = c.region {
                    builder = builder.with_region(v);
                }
                if let Some(v) = c.endpoint {
                    builder = builder.with_endpoint(v);
                }
                if let Some(v) = c.access_key_id {
                    builder = builder.with_access_key_id(v);
                }
                if let Some(v) = c.secret_access_key {
                    builder = builder.with_secret_access_key(v);
                }
                if let Some(v) = c.session_token {
                    builder = builder.with_token(v);
                }
                if let Some(v) = c.allow_http {
                    builder = builder.with_allow_http(v);
                }
                Arc::new(builder.build().map_err(
                    |_| "invalid S3 configuration; check bucket, region, endpoint and credentials",
                )?)
            }
            "gcs" | "azure" => {
                if c.directory.is_some() {
                    return Err("directory requires provider=filesystem".into());
                }
                if c.region.is_some()
                    || c.access_key_id.is_some()
                    || c.secret_access_key.is_some()
                    || c.session_token.is_some()
                {
                    return Err("S3 credential/region options require provider=s3; use native provider credentials".into());
                }
                if provider == "gcs" {
                    if c.endpoint.is_some() || c.allow_http.is_some() {
                        return Err(
                            "GCS endpoint/transport options must use native provider configuration"
                                .into(),
                        );
                    }
                    Arc::new(
                        GoogleCloudStorageBuilder::from_env()
                            .with_retry(retry.clone())
                            .with_config(object_store::gcp::GoogleConfigKey::Client(object_store::ClientConfigKey::Timeout), timeout.clone())
                            .with_config(object_store::gcp::GoogleConfigKey::Client(object_store::ClientConfigKey::ConnectTimeout), timeout.clone())
                            .with_bucket_name(c.bucket)
                            .build()
                            .map_err(|_| "invalid GCS configuration; check bucket and native provider credentials")?,
                    )
                } else {
                    let mut builder = MicrosoftAzureBuilder::from_env()
                        .with_container_name(c.bucket)
                        .with_retry(retry.clone())
                        .with_config(
                            object_store::azure::AzureConfigKey::Client(
                                object_store::ClientConfigKey::Timeout,
                            ),
                            timeout.clone(),
                        )
                        .with_config(
                            object_store::azure::AzureConfigKey::Client(
                                object_store::ClientConfigKey::ConnectTimeout,
                            ),
                            timeout.clone(),
                        );
                    if let Some(v) = c.endpoint {
                        builder = builder.with_endpoint(v);
                    }
                    if let Some(v) = c.allow_http {
                        builder = builder.with_allow_http(v);
                    }
                    Arc::new(builder.build().map_err(|_| "invalid Azure configuration; check account, container, endpoint and native provider credentials")?)
                }
            }
            _ => return Err("provider must be s3, gcs, azure or filesystem".into()),
        };
        let prefix = c.prefix.unwrap_or("deoos".into());
        if prefix.is_empty()
            || prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || Key::parse(&prefix).is_err()
        {
            return Err(
                "prefix must be a nonempty namespace without empty or dot components".into(),
            );
        }
        use tracing_subscriber::prelude::*;
        let storage_control = Arc::new(storage_io::StorageControl::new(
            provider != "filesystem",
            lease_ms,
        ));
        let storage_tracing = tracing::Dispatch::new(
            tracing_subscriber::registry().with(storage_io::RetryLayer(storage_control.clone())),
        );
        Ok(Self {
            store,
            prefix,
            lease_ms,
            discovery: Arc::new(std::sync::Mutex::new(discovery::Hints::default())),
            active_ready: Arc::new(tokio::sync::Mutex::new(false)),
            storage_control,
            storage_tracing,
            storage_deadline: None,
            storage_reconciliation_deadline: None,
        })
    }
    pub fn from_env() -> Result<Self, String> {
        let provider = std::env::var("DEOOS_STORAGE_PROVIDER").unwrap_or_else(|_| "s3".into());
        let bucket = if provider == "filesystem" {
            String::new()
        } else {
            std::env::var("DEOOS_STORAGE_BUCKET")
                .or_else(|error| {
                    if provider == "s3" {
                        std::env::var("AWS_BUCKET")
                    } else {
                        Err(error)
                    }
                })
                .map_err(|_| "DEOOS_STORAGE_BUCKET required")?
        };
        let directory = if provider == "filesystem" {
            std::env::var("DEOOS_STORAGE_DIRECTORY").ok()
        } else {
            None
        };
        Self::from_config(Config {
            provider: Some(provider),
            bucket,
            directory,
            prefix: std::env::var("EXECUTION_PREFIX").ok(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            allow_http: None,
            lease_ms: std::env::var("LEASE_MS")
                .ok()
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| "invalid LEASE_MS")?,
        })
    }
    pub async fn dispatch(
        &self,
        method: &str,
        path: &str,
        data: Value,
    ) -> Result<Value, (StatusCode, String)> {
        use tracing::instrument::WithSubscriber;
        if method == "GET" && path == "/info" {
            return Ok(
                json!({"process_id":std::process::id(),"protocol_version":PROTOCOL_VERSION,"storage":self.storage_control.snapshot()}),
            );
        }
        let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
        let renewal =
            method == "POST" && parts.len() == 3 && parts[0] == "tasks" && parts[2] == "renew";
        let slots = if renewal {
            &self.storage_control.renewals
        } else {
            &self.storage_control.ordinary
        };
        // Discovery polls cannot occupy the four ordinary slots reserved for progress.
        let _claim = if method == "POST" && path == "/claim" {
            Some(self.storage_control.claims.try_acquire().map_err(|_| {
                self.storage_control.issue(
                    "admission_rejections",
                    "storage claim admission busy; retry with backoff",
                );
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage claim admission busy; retry with backoff".into(),
                )
            })?)
        } else {
            None
        };
        let permit = match slots.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                let queue = if renewal {
                    &self.storage_control.renewal_waiters
                } else {
                    &self.storage_control.ordinary_waiters
                };
                let busy = || {
                    self.storage_control.issue(
                        "admission_rejections",
                        if renewal {
                            "storage renewal admission busy"
                        } else {
                            "storage busy; retry with backoff"
                        },
                    );
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        if renewal {
                            "storage renewal admission busy"
                        } else {
                            "storage busy; retry with backoff"
                        }
                        .into(),
                    )
                };
                let _queued = queue.try_acquire().map_err(|_| busy())?;
                match tokio::time::timeout(
                    std::time::Duration::from_millis((self.lease_ms / 8).min(500)),
                    slots.acquire(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    _ => return Err(busy()),
                }
            }
        };
        let mut engine = self.clone();
        engine.storage_deadline = Some(
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(if method == "GET" && path == "/tasks" {
                    7000
                } else {
                    (self.lease_ms / if renewal { 3 } else { 2 }).min(7000)
                }),
        );
        engine.storage_reconciliation_deadline = engine
            .storage_deadline
            .map(|deadline| deadline + self.storage_control.reconciliation_timeout);
        let result = engine
            .dispatch_inner(method, path, data, renewal)
            .with_subscriber(self.storage_tracing.clone())
            .await;
        drop(permit);
        if let Err((code, message)) = &result
            && *code == StatusCode::SERVICE_UNAVAILABLE
        {
            self.storage_control.health.lock().unwrap()["last_error"] = json!({"at_ms":now(),"message":if renewal {format!("storage renewal failed: {message}")}else{message.clone()}});
        }
        result
    }
    async fn dispatch_inner(
        &self,
        method: &str,
        path: &str,
        data: Value,
        renewal: bool,
    ) -> Result<Value, (StatusCode, String)> {
        if method == "POST"
            && data.get("protocol_version").and_then(Value::as_u64)
                != Some(u64::from(PROTOCOL_VERSION))
        {
            return Err(conflict("protocol_version 3 required for mutations"));
        }
        if method == "GET" && path == "/info" {
            return Ok(
                json!({"process_id":std::process::id(),"protocol_version":PROTOCOL_VERSION}),
            );
        }
        if method == "POST" && !renewal {
            self.ensure_active().await?;
        }
        let decode = |error: serde_json::Error| bad(&error.to_string());
        if method == "GET" && path == "/tasks" {
            return self.tasks().await;
        }
        if method == "POST" && path == "/tasks" {
            return submit(
                State(self.clone()),
                Json(serde_json::from_value(data).map_err(decode)?),
            )
            .await
            .map(|v| json!(v.0));
        }
        if method == "POST" && path == "/claim" {
            return claim(
                State(self.clone()),
                Json(serde_json::from_value(data).map_err(decode)?),
            )
            .await
            .map(|v| v.0);
        }
        let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
        if parts.first() == Some(&"schedules") {
            return schedules::dispatch(self, method, &parts, data).await;
        }
        if parts.len() < 2 || parts[0] != "tasks" {
            return Err(bad("unknown route"));
        }
        let id = parts[1];
        if method == "GET" && parts.len() == 2 {
            return inspect(State(self.clone()), Path(id.into()))
                .await
                .map(|v| json!(v.0));
        }
        if method == "GET" && parts.len() == 3 && parts[2] == "view" {
            return Ok(task_view(&self.read(id).await?.0));
        }
        if method == "GET" && parts.len() == 3 && parts[2] == "history" {
            return Ok(task_history(&self.read(id).await?.0));
        }
        if method == "GET" && parts.len() == 3 && parts[2] == "summary" {
            return Ok(execution_summary(&self.read(id).await?.0));
        }
        if method == "POST" && parts.len() == 4 && parts[2] == "definitions" {
            return self
                .mutate(
                    id,
                    serde_json::from_value(data).map_err(decode)?,
                    "define",
                    Some(parts[3]),
                )
                .await
                .map(|v| json!(v.0));
        }
        if method == "POST" && parts.len() == 3 && parts[2] == "cancel" {
            return self.cancel(id).await.map(|v| json!(v.0));
        }
        if method == "POST" && parts.len() == 3 && parts[2] == "retry" {
            return self
                .retry(id, serde_json::from_value(data).map_err(decode)?)
                .await
                .map(|v| json!(v.0));
        }
        if parts.len() == 4 && parts[2] == "signals" {
            if !valid(parts[3]) {
                return Err(bad("invalid signal name"));
            }
            if method == "POST" {
                return self
                    .signal(id, parts[3], serde_json::from_value(data).map_err(decode)?)
                    .await
                    .map(|v| json!(v.0));
            }
            if method == "GET" {
                let (task, _) = self.read(id).await?;
                let key = task
                    .signals
                    .get(parts[3])
                    .ok_or((StatusCode::NOT_FOUND, "signal not assigned".into()))?;
                return self.payload(key).await;
            }
        }
        if parts.len() == 4 && parts[2] == "steps" {
            if method == "GET" {
                return result(State(self.clone()), Path((id.into(), parts[3].into())))
                    .await
                    .map(|v| v.0);
            }
            if method == "POST" {
                return self
                    .mutate(
                        id,
                        serde_json::from_value(data).map_err(decode)?,
                        "step",
                        Some(parts[3]),
                    )
                    .await
                    .map(|v| json!(v.0));
            }
        }
        if method == "POST"
            && parts.len() == 3
            && ["renew", "complete", "fail", "suspend", "log"].contains(&parts[2])
        {
            return self
                .mutate(
                    id,
                    serde_json::from_value(data).map_err(decode)?,
                    parts[2],
                    None,
                )
                .await
                .map(|v| json!(v.0));
        }
        Err(bad("unknown route"))
    }
}
mod ffi;
mod schedules;

//! Durable discovery intents; authoritative task state and its CAS still own execution.
use super::*;
use object_store::ObjectMeta;

type StateVersion = (Task, UpdateVersion);
const ACTIVE_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveIntent {
    version: u32,
    task: Task,
    expected_revision: Option<String>,
}

impl Task {
    fn terminal(&self) -> bool {
        matches!(self.status.as_str(), "completed" | "failed" | "cancelled")
    }

    pub(super) fn validate(&self, id: &str) -> Result<(), (StatusCode, String)> {
        if self.version != PROTOCOL_VERSION {
            return Err(conflict("unsupported stored protocol version"));
        }
        if self.id != id
            || !valid(&self.id)
            || !valid(&self.handler)
            || !valid(&self.revision)
            || self.max_attempts == 0
            || self
                .active_incarnation
                .as_deref()
                .is_some_and(|value| !valid(value))
            || !matches!(
                self.status.as_str(),
                "queued" | "running" | "waiting" | "completed" | "failed" | "cancelled"
            )
        {
            return Err(conflict("unsupported or invalid stored task"));
        }
        Ok(())
    }
}

impl Engine {
    fn active_key(&self, task: &Task) -> Result<Key, (StatusCode, String)> {
        let incarnation = task
            .active_incarnation
            .as_deref()
            .filter(|value| valid(value))
            .ok_or_else(|| conflict("active task incarnation required"))?;
        Ok(Key::from(format!(
            "{}/active/{}/{incarnation}.json",
            self.prefix, task.id
        )))
    }

    pub(super) async fn publish_active(
        &self,
        task: &Task,
        expected_revision: Option<String>,
    ) -> Result<(), (StatusCode, String)> {
        task.validate(&task.id)?;
        if task.terminal() {
            return Err(conflict("terminal task cannot publish an active intent"));
        }
        let key = self.active_key(task)?;
        let intent = ActiveIntent {
            version: ACTIVE_VERSION,
            task: task.clone(),
            expected_revision,
        };
        let bytes = serde_json::to_vec(&intent).unwrap();
        match self
            .store
            .put_opts(
                &key,
                Bytes::from(bytes.clone()).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                // Lost responses may hide a successful create. Never overwrite/recreate this key.
                if local::is_durability_error(&error) {
                    return Err(storage(error));
                }
                if let Ok(result) = self.store.get(&key).await
                    && let Ok(actual) = result.bytes().await
                    && actual.as_ref() == bytes.as_slice()
                {
                    return Ok(());
                }
                // Discovery may have committed this incarnation and already retired its
                // marker before readback. The authoritative state is sufficient proof.
                if let Ok((actual, _)) = self.read(&task.id).await
                    && actual.active_incarnation == task.active_incarnation
                {
                    return Ok(());
                }
                Err(storage(error))
            }
        }
    }

    async fn retire_active(&self, key: &Key) -> Result<(), (StatusCode, String)> {
        match self.store.delete(key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(storage(error)),
        }
    }

    pub(super) async fn after_state_write(&self, task: &Task) {
        self.discovery.lock().unwrap().prioritize(task);
        if task.terminal()
            && let Ok(key) = self.active_key(task)
            && self.retire_active(&key).await.is_err()
        {
            // The terminal state is committed. Discovery can finish cleanup later.
            eprintln!("active marker cleanup deferred");
        }
    }

    fn active_identity<'a>(
        &self,
        key: &'a Key,
    ) -> Result<(&'a str, &'a str), (StatusCode, String)> {
        let prefix = Key::from(format!("{}/active", self.prefix));
        let expected_prefix = format!("{prefix}/");
        let relative = key
            .as_ref()
            .strip_prefix(&expected_prefix)
            .ok_or_else(|| conflict("invalid stored active key"))?;
        let (id, suffix) = relative
            .split_once('/')
            .ok_or_else(|| conflict("invalid stored active key"))?;
        let incarnation = suffix
            .strip_suffix(".json")
            .filter(|value| valid(value))
            .ok_or_else(|| conflict("invalid stored active key"))?;
        if !valid(id) {
            return Err(conflict("invalid stored active key"));
        }
        Ok((id, incarnation))
    }

    pub(super) async fn resolve_active(
        &self,
        key: &Key,
    ) -> Result<Option<StateVersion>, (StatusCode, String)> {
        let (id, incarnation) = self.active_identity(key)?;
        let mut current = self.read(id).await;
        if let Ok((task, version)) = &current
            && task.active_incarnation.as_deref() == Some(incarnation)
        {
            // The state proves this marker was published before its incarnation committed.
            // No marker GET or cached state hint is needed in the normal discovery path.
            if task.terminal() {
                self.retire_active(key).await?;
                return Ok(None);
            }
            return Ok(Some((task.clone(), version.clone())));
        }
        if let Err(error) = &current
            && error.0 != StatusCode::NOT_FOUND
        {
            return Err(error.clone());
        }
        let result = match self.store.get(key).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        let intent: ActiveIntent = serde_json::from_slice(&result.bytes().await.map_err(storage)?)
            .map_err(|_| conflict("invalid stored active intent"))?;
        intent.task.validate(id)?;
        if intent.version != ACTIVE_VERSION
            || intent.task.terminal()
            || intent
                .expected_revision
                .as_deref()
                .is_some_and(|value| !valid(value))
            || self.active_key(&intent.task)? != *key
        {
            return Err(conflict("unsupported or invalid stored active intent"));
        }
        for _ in 0..16 {
            let mode = match current {
                Ok((task, version)) => {
                    if task.active_incarnation == intent.task.active_incarnation {
                        if task.terminal() {
                            self.retire_active(key).await?;
                            return Ok(None);
                        }
                        return Ok(Some((task, version)));
                    }
                    if intent.expected_revision.as_deref() != Some(&task.revision) {
                        // This proposal lost to another create/retry. It can never apply.
                        self.retire_active(key).await?;
                        return Ok(None);
                    }
                    PutMode::Update(version)
                }
                Err((StatusCode::NOT_FOUND, _)) if intent.expected_revision.is_none() => {
                    PutMode::Create
                }
                Err((StatusCode::NOT_FOUND, _)) => {
                    // Retry cannot resurrect a missing authoritative predecessor.
                    return Err(conflict("active intent predecessor missing"));
                }
                Err(error) => return Err(error),
            };
            // Publish happened before the runnable state. Complete an interrupted transition.
            match self.write(&intent.task, mode).await {
                Ok(()) | Err((StatusCode::CONFLICT, _)) => {}
                Err(error) => return Err(error),
            }
            current = self.read(id).await;
        }
        Err(conflict("active intent contention; retry"))
    }

    pub(super) async fn active_candidates(
        &self,
    ) -> Result<Vec<(String, ObjectMeta)>, (StatusCode, String)> {
        let prefix = Key::from(format!("{}/active", self.prefix));
        let expected_prefix = format!("{prefix}/");
        let objects: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(storage)?;
        let mut candidates = Vec::new();
        for object in objects {
            // S3 LIST uses a raw prefix and can include active-index.json or active-other.
            if !object.location.as_ref().starts_with(&expected_prefix) {
                continue;
            }
            let (id, _) = self.active_identity(&object.location)?;
            candidates.push((id.to_owned(), object));
        }
        candidates
            .sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.location.cmp(&b.1.location)));
        Ok(candidates)
    }

    async fn active_initialized(&self, sentinel: &Key) -> Result<bool, (StatusCode, String)> {
        match self.store.get(sentinel).await {
            Ok(result) => {
                let actual: Value = serde_json::from_slice(&result.bytes().await.map_err(storage)?)
                    .map_err(|_| conflict("invalid active index sentinel"))?;
                if actual != json!({"version":ACTIVE_VERSION,"status":"ready"}) {
                    return Err(conflict("unsupported active index sentinel"));
                }
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(storage(error)),
        }
    }

    pub(super) async fn ensure_active(&self) -> Result<(), (StatusCode, String)> {
        let mut ready = self.active_ready.lock().await;
        if *ready {
            return Ok(());
        }
        let sentinel = Key::from(format!("{}/active-index.json", self.prefix));
        if self.active_initialized(&sentinel).await? {
            *ready = true;
            return Ok(());
        }
        // Fresh prefixes only: existing records without an initialized discovery index
        // cannot be adopted safely. This check never rewrites task state or creates intents.
        let prefix = Key::from(format!("{}/tasks", self.prefix));
        let expected_prefix = format!("{prefix}/");
        let occupied = self
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(storage)?
            .iter()
            .any(|object| object.location.as_ref().starts_with(&expected_prefix));
        if occupied {
            // A concurrent current engine may have initialized and submitted work between
            // our first sentinel read and LIST. It always commits the sentinel first.
            if self.active_initialized(&sentinel).await? {
                *ready = true;
                return Ok(());
            }
            return Err(conflict(
                "execution prefix has task records without an active index; use a fresh prefix",
            ));
        }
        let bytes =
            serde_json::to_vec(&json!({"version":ACTIVE_VERSION,"status":"ready"})).unwrap();
        match self
            .store
            .put_opts(
                &sentinel,
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(error) => {
                // Another initializer or a lost response may have committed the sentinel.
                if local::is_durability_error(&error) {
                    return Err(storage(error));
                }
                if !self.active_initialized(&sentinel).await? {
                    return Err(storage(error));
                }
            }
        }
        *ready = true;
        Ok(())
    }
}

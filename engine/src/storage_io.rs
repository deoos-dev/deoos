//! Local admission and bounded cloud calls. A timed-out write has an uncertain outcome.
use super::*;
use object_store::{ObjectMeta, PutResult};
use std::{future::Future, time::Duration};
use tokio::{sync::Semaphore, time::Instant};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};

pub(super) struct StorageControl {
    pub ordinary: Semaphore,
    pub claims: Semaphore,
    pub renewals: Semaphore,
    pub ordinary_waiters: Semaphore,
    pub renewal_waiters: Semaphore,
    pub cloud: bool,
    pub call_timeout: Duration,
    pub reconciliation_timeout: Duration,
    pub health: std::sync::Mutex<Value>,
}
impl StorageControl {
    pub fn new(cloud: bool, lease_ms: u64) -> Self {
        Self {
            ordinary: Semaphore::new(16),
            claims: Semaphore::new(12),
            renewals: Semaphore::new(4),
            ordinary_waiters: Semaphore::new(64),
            renewal_waiters: Semaphore::new(64),
            cloud,
            call_timeout: Duration::from_millis((lease_ms / 8).min(2000)),
            reconciliation_timeout: Duration::from_millis((lease_ms / 8).min(1000)),
            health: std::sync::Mutex::new(
                json!({"admission_rejections":0,"deadline_failures":0,"provider_retries":0,"throttled_retries":0,"backoff_seconds":0.0,"last_error":null,"last_backoff":null}),
            ),
        }
    }
    pub fn issue(&self, counter: &str, message: &str) {
        let mut health = self.health.lock().unwrap();
        health[counter] = json!(health[counter].as_u64().unwrap_or(0) + 1);
        health["last_error"] = json!({"at_ms":now(),"message":message});
    }
    pub fn snapshot(&self) -> Value {
        let mut health = self.health.lock().unwrap().clone();
        health["ordinary_limit"] = json!(16);
        health["claim_limit"] = json!(12);
        health["claims_in_flight"] = json!(12 - self.claims.available_permits());
        health["renewal_limit"] = json!(4);
        health["queue_limit"] = json!(64);
        health["ordinary_queued"] = json!(64 - self.ordinary_waiters.available_permits());
        health["renewals_queued"] = json!(64 - self.renewal_waiters.available_permits());
        health["ordinary_in_flight"] = json!(16 - self.ordinary.available_permits());
        health["renewals_in_flight"] = json!(4 - self.renewals.available_permits());
        health
    }
}
pub(super) struct RetryLayer(pub Arc<StorageControl>);
struct Message(String);
impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}
impl<S: Subscriber> Layer<S> for RetryLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "object_store::client::retry" {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        let text = message.0;
        let Some(delay) = text
            .split("backing off for ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<f64>().ok())
        else {
            return;
        };
        let status = text
            .split("status ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<u16>().ok());
        let record = json!({"at_ms":now(),"status":status,"seconds":delay,"kind":if matches!(status,Some(429|503)){"throttling"}else if status.is_some(){"provider retry"}else{"transport retry"}});
        let mut health = self.0.health.lock().unwrap();
        health["provider_retries"] = json!(health["provider_retries"].as_u64().unwrap_or(0) + 1);
        health["backoff_seconds"] =
            json!(health["backoff_seconds"].as_f64().unwrap_or(0.0) + delay);
        if matches!(status, Some(429 | 503)) {
            health["throttled_retries"] =
                json!(health["throttled_retries"].as_u64().unwrap_or(0) + 1);
        }
        health["last_backoff"] = record;
    }
}
impl Engine {
    fn deadline_error(&self) -> object_store::Error {
        self.storage_control.issue(
            "deadline_failures",
            "storage deadline exceeded; write outcome may be uncertain",
        );
        object_store::Error::Generic {
            store: "DEOOS",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "storage deadline exceeded; write outcome may be uncertain",
            )),
        }
    }
    pub(super) fn check_budget(&self) -> Result<(), (StatusCode, String)> {
        if self.storage_control.cloud
            && self
                .storage_deadline
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(storage(self.deadline_error()));
        }
        Ok(())
    }
    async fn cloud_call<T>(
        &self,
        future: impl Future<Output = Result<T, object_store::Error>>,
    ) -> Result<T, object_store::Error> {
        if !self.storage_control.cloud {
            return future.await;
        }
        let deadline = self
            .storage_deadline
            .unwrap_or_else(|| Instant::now() + self.storage_control.call_timeout)
            .min(Instant::now() + self.storage_control.call_timeout);
        if deadline <= Instant::now() {
            return Err(self.deadline_error());
        }
        tokio::time::timeout_at(deadline, future)
            .await
            .unwrap_or_else(|_| Err(self.deadline_error()))
    }
    pub(super) fn reconciliation(&self) -> Self {
        let mut engine = self.clone();
        let deadline = Instant::now() + self.storage_control.reconciliation_timeout;
        engine.storage_deadline =
            Some(deadline.min(self.storage_reconciliation_deadline.unwrap_or(deadline)));
        engine
    }
    pub(super) async fn get_bytes(
        &self,
        key: &Key,
    ) -> Result<(ObjectMeta, Bytes), object_store::Error> {
        self.cloud_call(async {
            let result = self.store.get(key).await?;
            let meta = result.meta.clone();
            Ok((meta, result.bytes().await?))
        })
        .await
    }
    pub(super) async fn list_objects(
        &self,
        prefix: &Key,
    ) -> Result<Vec<ObjectMeta>, object_store::Error> {
        self.cloud_call(self.store.list(Some(prefix)).try_collect())
            .await
    }
    pub(super) async fn put_object(
        &self,
        key: &Key,
        data: Bytes,
        mode: PutMode,
    ) -> Result<PutResult, object_store::Error> {
        self.cloud_call(self.store.put_opts(
            key,
            data.into(),
            PutOptions {
                mode,
                ..Default::default()
            },
        ))
        .await
    }
    pub(super) async fn delete_object(&self, key: &Key) -> Result<(), object_store::Error> {
        self.cloud_call(self.store.delete(key)).await
    }
}

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion, path::Path};
use serde_json::{Value, json};
use uuid::Uuid;

const WRITERS: usize = 32;

pub async fn check(store: &dyn ObjectStore, prefix: &str) -> Result<Value, String> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        return Err("storage prefix is required".into());
    }
    let namespace = Path::parse(format!("{}/qualification/{}", prefix, Uuid::new_v4()))
        .map_err(|e| e.to_string())?;
    let key = namespace.child("probe");
    let result = probe(store, &namespace, &key).await;
    if let Err(original) = &result {
        match store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(format!(
                    "{original}; cleanup failed: {}",
                    error_kind(&error)
                ));
            }
        }
    }
    result
}

async fn probe(store: &dyn ObjectStore, namespace: &Path, key: &Path) -> Result<Value, String> {
    let creates = stream::iter(0..WRITERS)
        .map(|i| async move {
            let body = format!("create-{i}");
            let result = store
                .put_opts(
                    key,
                    Bytes::from(body.clone()).into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await;
            (i, body, result)
        })
        .buffer_unordered(WRITERS)
        .collect::<Vec<_>>()
        .await;
    let mut created = None;
    let mut already_exists = 0;
    for (i, body, result) in creates {
        match result {
            Ok(_) if created.is_none() => created = Some((i, body)),
            Ok(_) => return Err("conditional create allowed more than one writer".into()),
            Err(object_store::Error::AlreadyExists { .. }) => already_exists += 1,
            Err(error) => return Err(format!("conditional create failed: {}", error_kind(&error))),
        }
    }
    let (create_winner, create_body) = created.ok_or("conditional create had no winner")?;
    if already_exists != WRITERS - 1 {
        return Err(format!(
            "conditional create rejected {already_exists} writers"
        ));
    }
    let created_bytes = get_bytes(store, key, "create read").await?;
    if created_bytes.as_ref() != create_body.as_bytes() {
        return Err("immediate read after create returned unexpected content".into());
    }
    if !listed(store, namespace, key).await? {
        return Err("immediate list after create did not return the object".into());
    }

    let meta = store
        .head(key)
        .await
        .map_err(|e| format!("version lookup failed: {}", error_kind(&e)))?;
    let version = UpdateVersion {
        e_tag: meta.e_tag,
        version: meta.version,
    };
    if version.e_tag.is_none() && version.version.is_none() {
        return Err("provider returned no update token".into());
    }
    let updates = stream::iter(0..WRITERS)
        .map(|i| {
            let version = version.clone();
            async move {
                let body = format!("update-{i}");
                let result = store
                    .put_opts(
                        key,
                        Bytes::from(body.clone()).into(),
                        PutOptions {
                            mode: PutMode::Update(version),
                            ..Default::default()
                        },
                    )
                    .await;
                (i, body, result)
            }
        })
        .buffer_unordered(WRITERS)
        .collect::<Vec<_>>()
        .await;
    let mut updated = None;
    let mut precondition_failed = 0;
    for (i, body, result) in updates {
        match result {
            Ok(_) if updated.is_none() => updated = Some((i, body)),
            Ok(_) => return Err("conditional update allowed more than one writer".into()),
            Err(object_store::Error::Precondition { .. }) => precondition_failed += 1,
            Err(error) => return Err(format!("conditional update failed: {}", error_kind(&error))),
        }
    }
    let (update_winner, update_body) = updated.ok_or("conditional update had no winner")?;
    if precondition_failed != WRITERS - 1 {
        return Err(format!(
            "conditional update rejected {precondition_failed} writers"
        ));
    }
    let updated_bytes = get_bytes(store, key, "updated read").await?;
    if updated_bytes.as_ref() != update_body.as_bytes() {
        return Err("read after conditional update returned unexpected content".into());
    }
    match store
        .put_opts(
            key,
            Bytes::from_static(b"stale-update").into(),
            PutOptions {
                mode: PutMode::Update(version),
                ..Default::default()
            },
        )
        .await
    {
        Err(object_store::Error::Precondition { .. }) => {}
        Err(error) => return Err(format!("stale update failed: {}", error_kind(&error))),
        Ok(_) => return Err("stale update was accepted".into()),
    }
    let after_stale = get_bytes(store, key, "post-stale read").await?;
    if after_stale.as_ref() != update_body.as_bytes() {
        return Err("rejected stale update changed object content".into());
    }

    store
        .delete(key)
        .await
        .map_err(|e| format!("delete failed: {}", error_kind(&e)))?;
    match store.get(key).await {
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(format!("read after delete failed: {}", error_kind(&error))),
        Ok(_) => return Err("read after delete still returned the object".into()),
    }
    if listed(store, namespace, key).await? {
        return Err("list after delete still returned the object".into());
    }
    Ok(json!({
        "passed": true,
        "cleaned": true,
        "key": key.as_ref(),
        "checks": ["conditional_create", "read_after_create", "list_after_create", "conditional_update", "stale_update_rejected_without_write", "read_after_delete", "list_after_delete"],
        "stale_update_preserved_content": true,
        "writers": WRITERS,
        "create_winner": create_winner,
        "update_winner": update_winner,
        "create_rejections": already_exists,
        "update_rejections": precondition_failed,
    }))
}

async fn listed(store: &dyn ObjectStore, namespace: &Path, key: &Path) -> Result<bool, String> {
    store
        .list(Some(namespace))
        .try_collect::<Vec<_>>()
        .await
        .map(|objects| objects.iter().any(|object| &object.location == key))
        .map_err(|e| format!("object list failed: {}", error_kind(&e)))
}

async fn get_bytes(store: &dyn ObjectStore, key: &Path, phase: &str) -> Result<Bytes, String> {
    store
        .get(key)
        .await
        .map_err(|e| format!("{phase} failed: {}", error_kind(&e)))?
        .bytes()
        .await
        .map_err(|e| format!("{phase} failed: {}", error_kind(&e)))
}

pub(super) fn error_kind(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::AlreadyExists { .. } => "already_exists",
        object_store::Error::Precondition { .. } => "precondition_failed",
        object_store::Error::NotFound { .. } => "not_found",
        object_store::Error::PermissionDenied { .. } => "permission_denied",
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented => {
            "not_supported"
        }
        object_store::Error::Generic { .. } => "provider_error",
        _ => "storage_error",
    }
}

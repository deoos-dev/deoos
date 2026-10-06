//! Local object storage with one current format and no legacy adapters.
mod adapter;
mod durability_epochs;
#[cfg(test)]
use adapter::{check_filesystem, durability};
use adapter::{full_sync, ordinary_sync};

use object_store::{ObjectStore, Result};
use std::{path::Path as FsPath, sync::Arc};

pub(super) fn open(root: impl AsRef<FsPath>) -> Result<Arc<dyn ObjectStore>> {
    let root = adapter::LocalObjectStore::prepare_root(root)?;
    let bootstrap = adapter::LocalObjectStore::lock_bootstrap(&root)?;
    Ok(Arc::new(adapter::LocalObjectStore::new_locked(
        &root, bootstrap,
    )?))
}

pub(super) fn is_durability_error(error: &object_store::Error) -> bool {
    adapter::is_durability_error(error)
}

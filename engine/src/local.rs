//! Local object-store format selector. The bootstrap lock protects both the
//! format decision and initialization; existing V1 roots stay on V1, while V2
//! is selected only for fresh or explicitly recoverable V2 initialization.
mod durability_epochs;
mod v1;
mod v2;
#[cfg(test)]
use v2::{check_filesystem, durability};
use v2::{full_sync, ordinary_sync};

use std::{fs, io, path::Path as FsPath, sync::Arc};

use object_store::{Error, ObjectStore, Result};

pub(super) fn open(root: impl AsRef<FsPath>) -> Result<Arc<dyn ObjectStore>> {
    let root = v2::LocalObjectStore::prepare_root(root)?;
    let bootstrap = v2::LocalObjectStore::lock_bootstrap(&root)?;
    let store: Arc<dyn ObjectStore> = match select_format(&root)? {
        Format::V1 => Arc::new(v1::LocalObjectStore::new_locked(&root, bootstrap)?),
        Format::V2 => Arc::new(v2::LocalObjectStore::new_locked(&root, bootstrap)?),
    };
    Ok(store)
}

#[derive(Clone, Copy)]
enum Format {
    V1,
    V2,
}

fn select_format(root: &FsPath) -> Result<Format> {
    let objects = entry_type(&root.join("objects"))?;
    let objects_v2 = entry_type(&root.join("objects-v2"))?;
    let marker = entry_type(&root.join("format-v2"))?;
    let names = fs::read_dir(root)
        .map_err(selection_error)?
        .map(|entry| {
            entry
                .map_err(selection_error)
                .map(|entry| entry.file_name())
        })
        .collect::<Result<Vec<_>>>()?;
    let marker_temp = names
        .iter()
        .any(|name| name.to_str().is_some_and(is_canonical_marker_temp));

    let legacy_objects = objects.is_some_and(|kind| kind == EntryType::Directory);
    let v2_signal = marker.is_some()
        || objects_v2.is_some()
        || marker_temp
        || objects.is_some_and(|kind| kind != EntryType::Directory);
    if legacy_objects {
        if v2_signal {
            return Err(selection_error("mixed V1 and V2 local storage layouts"));
        }
        // The accepted V1 adapter ignored unrelated root siblings. Keep that
        // behavior for an unambiguously V1 store rather than narrowing access.
        return Ok(Format::V1);
    }

    for name in &names {
        let name = name
            .to_str()
            .ok_or_else(|| selection_error("non-UTF8 entry in local storage root"))?;
        if is_canonical_marker_temp(name) {
            v2::LocalObjectStore::validate_marker_temp(root, name)?;
        } else if !matches!(
            name,
            "bootstrap.lock" | "objects" | "objects-v2" | "locks" | "format-v2"
        ) {
            return Err(selection_error("unexpected entry in local storage root"));
        }
    }

    // V2 initialization explicitly recovers an empty locks directory left by
    // an interrupted first initialization. A populated locks-only root has no
    // reliable format identity and must not be guessed at.
    if !v2_signal {
        match entry_type(&root.join("locks"))? {
            None => return Ok(Format::V2),
            Some(EntryType::Directory) => {
                if fs::read_dir(root.join("locks"))
                    .map_err(selection_error)?
                    .next()
                    .is_none()
                {
                    return Ok(Format::V2);
                }
                return Err(selection_error(
                    "unrecognized local storage root with locks but no objects",
                ));
            }
            Some(EntryType::Other) => {
                return Err(selection_error(
                    "invalid local locks path without a recognized storage format",
                ));
            }
        }
    }
    Ok(Format::V2)
}

fn is_canonical_marker_temp(name: &str) -> bool {
    name.strip_prefix(".format-v2-")
        .and_then(|suffix| uuid::Uuid::parse_str(suffix).ok().map(|id| (suffix, id)))
        .is_some_and(|(suffix, id)| id.to_string() == suffix)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EntryType {
    Directory,
    Other,
}
fn entry_type(path: &FsPath) -> Result<Option<EntryType>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(Some(EntryType::Directory)),
        Ok(_) => Ok(Some(EntryType::Other)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(selection_error(error)),
    }
}
fn selection_error(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Generic {
        store: "deoos_local",
        source: source.into(),
    }
}

pub(super) fn is_durability_error(error: &Error) -> bool {
    v1::is_durability_error(error) || v2::is_durability_error(error)
}

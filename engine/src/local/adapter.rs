//! Private, local APFS object storage. All participants must use this adapter.
//!
//! Lock files are permanent: removing one can split locking between processes.
//! Immutable per-key versions retain the last acknowledged file until a new
//! version has passed its post-publication full flush. Initialization and format
//! validation run under the permanent bootstrap lock. Root, objects, locks and
//! bootstrap share one local APFS filesystem. There is no legacy format support.
//! Mounting over or replacing store paths while clients are open is unsupported.
//! Concurrent calls on clones may share a full flush only after each caller's
//! own fsync. Both pre-rename and post-publication durability waits remain.
use super::durability_epochs;

use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    Attributes, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result, path::Path,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const STORE: &str = "deoos_local";
const MAGIC: &[u8; 8] = b"DEOOSV02";
const FORMAT_MARKER: &[u8] = b"DEOOS-LOCAL-FORMAT-2\n";
const MAX_HEADER: usize = 64 * 1024;
const CHECKSUM_LEN: u64 = 32;
const MAX_KEY_HINTS: usize = 8192;
const MAX_HINT_KEY_BYTES: usize = 1024;
const MAX_DURABLE_ETAGS: usize = 8192;

/// Kept distinct from ordinary storage errors: reconciling by reading after this
/// error must never allow the failed operation to be reported as successful.
#[derive(Debug)]
pub(super) struct LocalDurabilityError(io::Error);
impl fmt::Display for LocalDurabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "local durability barrier failed: {}", self.0)
    }
}
impl std::error::Error for LocalDurabilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
pub(super) fn is_durability_error(error: &Error) -> bool {
    matches!(error, Error::Generic { source, .. } if source.is::<LocalDurabilityError>())
}
fn generic(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Generic {
        store: STORE,
        source: source.into(),
    }
}
pub(super) fn durability(error: io::Error) -> Error {
    generic(LocalDurabilityError(error))
}
fn unsupported(message: &str) -> Error {
    Error::NotSupported {
        source: message.into(),
    }
}
fn missing(key: &Path) -> Error {
    Error::NotFound {
        path: key.to_string(),
        source: "object does not exist".into(),
    }
}
fn precondition(key: &Path) -> Error {
    Error::Precondition {
        path: key.to_string(),
        source: "object ETag does not match".into(),
    }
}
fn digest(key: &str) -> String {
    format!("{:x}", Sha256::digest(key.as_bytes()))
}

#[derive(Debug, Clone)]
pub(super) struct LocalObjectStore {
    root: PathBuf,
    // Only immutable hash-to-key mappings, never existence, metadata, or ETags.
    // Clones share hints; separately opened stores start with an empty cache.
    key_hints: Arc<Mutex<HashMap<String, Path>>>,
    // Fixed-size hashes and validated UUID ETags; never cached object content.
    durable_etags: Arc<Mutex<HashMap<String, String>>>,
    durability_epochs: Arc<durability_epochs::Coordinator>,
}
impl fmt::Display for LocalObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DEOOS local ({})", self.root.display())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: u32,
    sequence: u64,
    key: String,
    etag: String,
    kind: VersionKind,
    size: u64,
    modified_ms: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum VersionKind {
    Value,
    Tombstone,
}
struct VersionFile {
    header: Header,
    file: File,
    directory: PathBuf,
}
struct PublishedVersion {
    sequence: u64,
    etag: String,
}
#[derive(Clone)]
struct VersionName {
    sequence: u64,
    etag: String,
    expected_len: u64,
    temporary: bool,
    path: PathBuf,
}
enum VersionReadError {
    Incomplete,
    Invalid(Error),
}
impl Header {
    fn meta(&self) -> Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: Path::parse(&self.key)?,
            last_modified: (UNIX_EPOCH + Duration::from_millis(self.modified_ms)).into(),
            size: self.size,
            e_tag: Some(self.etag.clone()),
            version: None,
        })
    }
}

impl LocalObjectStore {
    /// This synchronous constructor runs before the engine runtime starts.
    pub(super) fn new_locked(root: &FsPath, bootstrap: File) -> Result<Self> {
        let store = Self {
            root: root.to_path_buf(),
            key_hints: Arc::new(Mutex::new(HashMap::new())),
            durable_etags: Arc::new(Mutex::new(HashMap::new())),
            durability_epochs: Arc::new(durability_epochs::Coordinator::default()),
        };
        check_private_dir(root)?;
        check_filesystem(root)?;
        store.initialize()?;
        check_same_filesystem(root, &bootstrap)?;
        sync_dir(&store.objects_root()).map_err(durability)?;
        sync_dir(&store.root.join("locks")).map_err(durability)?;
        for dir in store.root.ancestors() {
            sync_dir(dir).map_err(durability)?;
        }
        full_sync(&bootstrap).map_err(durability)?;
        Ok(store)
    }
    pub(super) fn prepare_root(root: impl AsRef<FsPath>) -> Result<PathBuf> {
        platform_supported()?;
        let root = root.as_ref();
        if !root.is_absolute() || root.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(generic(
                "local storage root must be absolute and contain no '..'",
            ));
        }
        // Inspect every component before creating anything underneath it. Refuse
        // symlinks rather than silently opening a different storage location.
        create_root(root)?;
        check_private_dir(root)?;
        check_filesystem(root)?;
        Ok(root.to_path_buf())
    }
    pub(super) fn lock_bootstrap(root: &FsPath) -> Result<File> {
        let file = open_private_file(&root.join("bootstrap.lock"), true)?;
        file.lock().map_err(generic)?;
        Ok(file)
    }
    pub(super) fn validate_marker_temp(root: &FsPath, name: &str) -> Result<()> {
        let file = open_private_file(&root.join(name), false)?;
        check_file_device(root, &file)
    }
    fn objects_root(&self) -> PathBuf {
        self.root.join("objects")
    }
    fn initialize(&self) -> Result<()> {
        let marker = self.root.join("format");
        // Reject foreign entries before admitting data. Recover only interrupted
        // initialization of this format, never guess or migrate existing data.
        for entry in fs::read_dir(&self.root).map_err(generic)? {
            let name = entry.map_err(generic)?.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| generic("non-UTF8 entry in local storage root"))?;
            if name
                .strip_prefix(".format-")
                .and_then(|suffix| Uuid::parse_str(suffix).ok().map(|id| (suffix, id)))
                .is_some_and(|(suffix, id)| id.to_string() == suffix)
            {
                Self::validate_marker_temp(&self.root, name)?;
            } else if !matches!(name, "bootstrap.lock" | "objects" | "locks" | "format") {
                return Err(generic(
                    "unexpected entry in local storage root; use a fresh directory",
                ));
            }
        }
        match fs::symlink_metadata(&marker) {
            Ok(_) => {
                let mut file = open_private_file(&marker, false)?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).map_err(generic)?;
                if bytes != FORMAT_MARKER {
                    return Err(generic(
                        "unsupported local store format; use a fresh directory",
                    ));
                }
                check_private_dir(&self.objects_root())?;
                check_private_dir(&self.root.join("locks"))?;
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(generic(error)),
        }
        ensure_empty_or_missing(
            &self.objects_root(),
            "unrecognized nonempty local storage; use a fresh directory",
        )?;
        ensure_empty_or_missing(
            &self.root.join("locks"),
            "unrecognized nonempty local locks; use a fresh directory",
        )?;
        create_private_dir(&self.objects_root())?;
        create_private_dir(&self.root.join("locks"))?;
        let temporary = self.root.join(format!(".format-{}", Uuid::new_v4()));
        let result = (|| {
            let mut file = create_new_private_file(&temporary)?;
            file.write_all(FORMAT_MARKER).map_err(generic)?;
            ordinary_sync(&file).map_err(durability)?;
            full_sync(&file).map_err(durability)?;
            fs::rename(&temporary, &marker).map_err(generic)?;
            sync_dir(&self.root).map_err(durability)
        })();
        let _ = fs::remove_file(temporary);
        result?;
        Ok(())
    }
    fn remember_key(&self, hash: &str, key: &Path) {
        if key.as_ref().len() > MAX_HINT_KEY_BYTES {
            return;
        }
        // An unavailable or full cache falls back to the original locked read
        // for unknown keys. Existing immutable hints need no eviction.
        if let Ok(mut hints) = self.key_hints.lock()
            && hints.len() < MAX_KEY_HINTS
            && !hints.contains_key(hash)
        {
            hints.insert(hash.to_owned(), key.clone());
        }
    }
    fn known_outside_prefix(&self, hash: &str, prefix: &Path) -> bool {
        // The mutex is released before the caller can acquire a file lock.
        self.key_hints.lock().ok().is_some_and(|hints| {
            hints
                .get(hash)
                .is_some_and(|key| !key.prefix_matches(prefix))
        })
    }
    fn confirmed_durable(&self, hash: &str, etag: &str) -> bool {
        self.durable_etags
            .lock()
            .ok()
            .is_some_and(|certificates| certificates.get(hash).is_some_and(|known| known == etag))
    }
    fn remember_durable(&self, hash: &str, etag: &str) {
        // Existing entries can advance even at capacity. New hashes and poisoned
        // caches fall back to barriers; no certificate is needed for correctness.
        if let Ok(mut certificates) = self.durable_etags.lock()
            && (certificates.len() < MAX_DURABLE_ETAGS || certificates.contains_key(hash))
        {
            certificates.insert(hash.to_owned(), etag.to_owned());
        }
    }
    /// Caller holds the stable key lock. Version bytes are never cached.
    fn read_durable_version(
        &self,
        hash: &str,
        lock: &File,
        allow_absent_put: bool,
    ) -> Result<Option<VersionFile>> {
        let (current, observed_incomplete) = match self.read_version(hash) {
            Ok(selection) => selection,
            Err(error) => {
                self.barrier(Some(hash), lock, None)?;
                return Err(error);
            }
        };
        if observed_incomplete {
            // A certified value does not certify a newly observed filename.
            // Stabilize this namespace before returning either the fallback
            // value or absence. Keep short files until a successor is durable;
            // they still reserve their sequence numbers.
            if let Some(version) = &current {
                self.barrier(Some(hash), lock, Some(&version.file))?;
                self.remember_durable(hash, &version.header.etag);
                return Ok(current);
            }
            if !allow_absent_put {
                self.barrier(Some(hash), lock, None)?;
            }
            return Ok(current);
        }
        if let Some(version) = current {
            if !self.confirmed_durable(hash, &version.header.etag) {
                self.barrier(Some(hash), lock, Some(&version.file))?;
                self.remember_durable(hash, &version.header.etag);
            }
            return Ok(Some(version));
        }
        // Only a proceeding Create/Overwrite may defer stabilizing absence.
        if !allow_absent_put {
            self.barrier(Some(hash), lock, None)?;
        }
        Ok(None)
    }
    fn bootstrap(&self) -> Result<File> {
        let file = open_private_file(&self.root.join("bootstrap.lock"), true)?;
        file.lock().map_err(generic)?;
        Ok(file)
    }
    fn lock(&self, hash: &str) -> Result<File> {
        let path = self.root.join("locks").join(hash);
        let file = open_private_file(&path, true)?;
        check_file_device(&self.root, &file)?;
        file.lock().map_err(generic)?;
        Ok(file)
    }
    fn version_dir(&self, hash: &str) -> PathBuf {
        self.objects_root().join(hash)
    }
    fn barrier(&self, hash: Option<&str>, lock: &File, version: Option<&File>) -> Result<()> {
        if let Some(hash) = hash {
            let directory = self.version_dir(hash);
            match fs::symlink_metadata(&directory) {
                Ok(meta) if meta.is_dir() => {
                    check_same_device(&self.root, &directory)
                        .map_err(|error| durability(io::Error::other(error.to_string())))?;
                    sync_dir(&directory).map_err(durability)?;
                }
                Ok(_) => {
                    return Err(durability(io::Error::other(
                        "invalid local version directory during barrier",
                    )));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(durability(error)),
            }
        }
        sync_dir(&self.objects_root()).map_err(durability)?;
        sync_dir(&self.root.join("locks")).map_err(durability)?;
        if version.is_some() {
            ordinary_sync(lock).map_err(durability)?;
        }
        self.durability_epochs
            .sync(version.unwrap_or(lock))
            .map_err(durability)
    }
    fn read_version(&self, hash: &str) -> Result<(Option<VersionFile>, bool)> {
        let directory = self.version_dir(hash);
        let Some(entries) = self.version_names(hash)? else {
            return Ok((None, false));
        };
        let mut finals: Vec<_> = entries
            .into_iter()
            .filter(|entry| !entry.temporary)
            .collect();
        let mut observed_incomplete = false;
        while let Some(candidate) = finals.pop() {
            match self.read_version_file(hash, &directory, &candidate) {
                Ok(version) => {
                    self.remember_key(hash, &Path::parse(&version.header.key)?);
                    return Ok((Some(version), observed_incomplete));
                }
                Err(VersionReadError::Incomplete) => observed_incomplete = true,
                Err(VersionReadError::Invalid(error)) => return Err(error),
            }
        }
        Ok((None, observed_incomplete))
    }
    fn read_version_file(
        &self,
        hash: &str,
        directory: &FsPath,
        name: &VersionName,
    ) -> std::result::Result<VersionFile, VersionReadError> {
        let invalid = |message: &'static str| VersionReadError::Invalid(generic(message));
        let mut file = open_private_rw_file(&name.path).map_err(VersionReadError::Invalid)?;
        check_file_device(&self.root, &file).map_err(VersionReadError::Invalid)?;
        let total = file
            .metadata()
            .map_err(|error| VersionReadError::Invalid(generic(error)))?
            .len();
        let mut prefix = [0u8; 12];
        if name.expected_len < (12 + 1 + CHECKSUM_LEN) || total > name.expected_len {
            return Err(invalid("invalid local object filename length"));
        }
        if total < name.expected_len {
            return Err(VersionReadError::Incomplete);
        }
        if total < prefix.len() as u64 {
            return Err(invalid("invalid local object envelope length"));
        }
        file.read_exact(&mut prefix)
            .map_err(|error| VersionReadError::Invalid(generic(error)))?;
        if &prefix[..8] != MAGIC {
            return Err(invalid("invalid local object envelope magic"));
        }
        let header_len = u32::from_le_bytes(prefix[8..].try_into().unwrap()) as usize;
        if header_len == 0 || header_len > MAX_HEADER {
            return Err(invalid("invalid local object header length"));
        }
        if total < 12 + header_len as u64 {
            return Err(invalid("local object header exceeds named record length"));
        }
        let mut bytes = vec![0u8; header_len];
        file.read_exact(&mut bytes)
            .map_err(|error| VersionReadError::Invalid(generic(error)))?;
        let header: Header = serde_json::from_slice(&bytes)
            .map_err(|error| VersionReadError::Invalid(generic(error)))?;
        let body_offset = 12 + header_len as u64;
        let expected_total = body_offset
            .checked_add(header.size)
            .and_then(|n| n.checked_add(CHECKSUM_LEN))
            .ok_or_else(|| invalid("local object record size overflow"))?;
        if name.expected_len != expected_total
            || total != expected_total
            || header.format != 2
            || header.sequence != name.sequence
            || header.etag != name.etag
            || Uuid::parse_str(&header.etag).is_err()
            || header.modified_ms > 253_402_300_799_999
            || digest(&header.key) != hash
            || (header.kind == VersionKind::Tombstone && header.size != 0)
        {
            return Err(invalid("invalid local object version metadata"));
        }
        let parsed =
            Path::parse(&header.key).map_err(|error| VersionReadError::Invalid(generic(error)))?;
        if parsed.as_ref() != header.key {
            return Err(invalid("noncanonical local version key"));
        }
        let mut checksum = Sha256::new();
        checksum.update((bytes.len() as u64).to_le_bytes());
        checksum.update(&bytes);
        let mut remaining = header.size;
        let mut buffer = [0u8; 64 * 1024];
        while remaining > 0 {
            let amount = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..amount])
                .map_err(|error| VersionReadError::Invalid(generic(error)))?;
            checksum.update(&buffer[..amount]);
            remaining -= amount as u64;
        }
        let mut stored = [0u8; 32];
        file.read_exact(&mut stored)
            .map_err(|error| VersionReadError::Invalid(generic(error)))?;
        if checksum.finalize().as_slice() != stored {
            return Err(invalid("local object checksum mismatch"));
        }
        file.seek(SeekFrom::Start(body_offset))
            .map_err(|error| VersionReadError::Invalid(generic(error)))?;
        Ok(VersionFile {
            header,
            file,
            directory: directory.to_path_buf(),
        })
    }
    fn put_sync(&self, key: Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        if !opts.attributes.is_empty() {
            return Err(unsupported("local object attributes are unsupported"));
        }
        let hash = digest(key.as_ref());
        let lock = self.lock(&hash)?;
        let allow_absent_put = matches!(&opts.mode, PutMode::Create | PutMode::Overwrite);
        let current = self.read_durable_version(&hash, &lock, allow_absent_put)?;
        let deferred_absence = allow_absent_put && current.is_none();
        let current_live = current
            .as_ref()
            .is_some_and(|v| v.header.kind == VersionKind::Value);
        match opts.mode {
            PutMode::Create if current_live => {
                return Err(Error::AlreadyExists {
                    path: key.to_string(),
                    source: "object already exists".into(),
                });
            }
            PutMode::Update(version) => {
                if version.version.is_some()
                    || version.e_tag.is_none()
                    || current
                        .as_ref()
                        .filter(|v| v.header.kind == VersionKind::Value)
                        .map(|v| &v.header.etag)
                        != version.e_tag.as_ref()
                {
                    return Err(precondition(&key));
                }
            }
            _ => {}
        }
        let directory = self.version_dir(&hash);
        let result = (|| {
            let previous_sequence = current.as_ref().map(|version| version.header.sequence);
            let published =
                self.publish_version(&hash, &key, &directory, payload, VersionKind::Value, &lock)?;
            let names = self
                .version_names(&hash)?
                .ok_or_else(|| generic("published version directory disappeared"))?;
            let mut keep = std::collections::HashSet::from([published.sequence]);
            if let Some(previous) = previous_sequence {
                keep.insert(previous);
            }
            self.prune_versions(&directory, &names, &keep)?;
            Ok(PutResult {
                e_tag: Some(published.etag),
                version: None,
            })
        })();
        if deferred_absence && result.is_err() {
            // Do not expose even a validation/I/O error based on unstabilized
            // absence. Barrier failure takes precedence; an original durability
            // error remains an error even if this later barrier succeeds.
            self.barrier(Some(&hash), &lock, None)?;
        }
        result
    }
    fn get_sync(&self, key: Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(unsupported("local object versions are unsupported"));
        }
        let hash = digest(key.as_ref());
        let lock = self.lock(&hash)?;
        let version = self
            .read_durable_version(&hash, &lock, false)?
            .ok_or_else(|| missing(&key))?;
        if version.header.kind == VersionKind::Tombstone {
            return Err(missing(&key));
        }
        let header = &version.header;
        let meta = header.meta()?;
        options.check_preconditions(&meta)?;
        let range = match options.range {
            Some(range) => range.as_range(meta.size).map_err(generic)?,
            None => 0..meta.size,
        };
        let bytes = if options.head {
            Bytes::new()
        } else {
            // Own the entire returned value before releasing the key lock.
            // No lazy file handle can mix bytes with later replacement/deletion.
            let mut bytes = Vec::with_capacity(meta.size as usize);
            let mut file = version.file.take(meta.size);
            file.read_to_end(&mut bytes).map_err(generic)?;
            if bytes.len() as u64 != meta.size {
                return Err(generic("local object size changed during read"));
            }
            Bytes::from(bytes).slice(range.start as usize..range.end as usize)
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }
    fn list_sync(&self, prefix: Option<Path>) -> Result<Vec<ObjectMeta>> {
        // A listing that observes absence must also stabilize earlier deletions.
        let bootstrap = self.bootstrap()?;
        self.barrier(None, &bootstrap, None)?;
        let mut objects = Vec::new();
        for entry in fs::read_dir(self.objects_root()).map_err(generic)? {
            let entry = entry.map_err(generic)?;
            let name = entry.file_name();
            let hash = name
                .to_str()
                .ok_or_else(|| generic("invalid local object filename"))?;
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(generic("unexpected entry in local objects directory"));
            }
            if entry.file_type().map_err(generic)?.is_symlink()
                || !entry.file_type().map_err(generic)?.is_dir()
            {
                return Err(generic("invalid local key directory"));
            }
            // Every filename is still enumerated and checked. Only a validated,
            // immutable key can rule out a prefix match. Matching and unknown
            // entries are freshly read under their key lock; unconfirmed versions
            // still require a durability barrier.
            if prefix
                .as_ref()
                .is_some_and(|p| self.known_outside_prefix(hash, p))
            {
                continue;
            }
            let lock = self.lock(hash)?;
            if let Some(version) = self.read_durable_version(hash, &lock, false)?
                && version.header.kind == VersionKind::Value
            {
                let meta = version.header.meta()?;
                if prefix
                    .as_ref()
                    .is_none_or(|p| meta.location.prefix_matches(p))
                {
                    objects.push(meta);
                }
            }
        }
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(objects)
    }
    fn version_names(&self, hash: &str) -> Result<Option<Vec<VersionName>>> {
        let directory = self.version_dir(hash);
        match fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(generic(error)),
            Ok(meta) if !meta.is_dir() => return Err(generic("invalid local version directory")),
            Ok(_) => {
                check_private_dir(&directory)?;
                check_same_device(&self.root, &directory)?;
            }
        }
        let mut names = Vec::new();
        for entry in fs::read_dir(&directory).map_err(generic)? {
            let entry = entry.map_err(generic)?;
            let kind = entry.file_type().map_err(generic)?;
            if kind.is_symlink() || !kind.is_file() {
                return Err(generic("invalid entry in local version directory"));
            }
            let filename = entry
                .file_name()
                .into_string()
                .map_err(|_| generic("invalid local version filename"))?;
            let parsed = parse_version_name(&filename)
                .ok_or_else(|| generic("invalid local version filename"))?;
            let _file = open_private_file(&entry.path(), false)?;
            check_file_device(&self.root, &_file)?;
            names.push(VersionName {
                path: entry.path(),
                ..parsed
            });
        }
        names.sort_by_key(|name| name.sequence);
        for pair in names.windows(2) {
            if pair[0].sequence == pair[1].sequence {
                return Err(generic("duplicate local version sequence"));
            }
        }
        Ok(Some(names))
    }
    fn publish_version(
        &self,
        hash: &str,
        key: &Path,
        directory: &FsPath,
        payload: PutPayload,
        kind: VersionKind,
        lock: &File,
    ) -> Result<PublishedVersion> {
        let observed = self.version_names(hash)?;
        let need_directory = observed.is_none();
        let mut names = observed.unwrap_or_default();
        let max = names.iter().map(|name| name.sequence).max().unwrap_or(0);
        let sequence = max
            .checked_add(1)
            .ok_or_else(|| generic("local version sequence exhausted"))?;
        if need_directory {
            create_private_dir(directory)?;
            check_same_device(&self.root, directory)?;
            sync_dir(&self.objects_root()).map_err(durability)?;
        } else {
            check_private_dir(directory)?;
            check_same_device(&self.root, directory)?;
        }
        // Temporary names also reserve their sequence. Keep them until a
        // successor has crossed its publication barrier; only post-ACK pruning
        // may remove observed names.
        let etag = Uuid::new_v4().to_string();
        let header = Header {
            format: 2,
            sequence,
            key: key.to_string(),
            etag: etag.clone(),
            kind,
            size: payload.content_length() as u64,
            modified_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(generic)?
                .as_millis()
                .try_into()
                .map_err(generic)?,
        };
        if kind == VersionKind::Tombstone && header.size != 0 {
            return Err(generic("invalid local tombstone payload"));
        }
        let header_bytes = serde_json::to_vec(&header).map_err(generic)?;
        if header_bytes.len() > MAX_HEADER {
            return Err(generic("local version metadata is too large"));
        }
        let expected_len = (12u64)
            .checked_add(header_bytes.len() as u64)
            .and_then(|n| n.checked_add(header.size))
            .and_then(|n| n.checked_add(CHECKSUM_LEN))
            .ok_or_else(|| generic("local version size overflow"))?;
        let temporary =
            directory.join(format!(".tmp-{sequence:020}-{etag}-{expected_len:020}.obj"));
        let final_path = directory.join(format!("v{sequence:020}-{etag}-{expected_len:020}.obj"));
        let prepared = (|| {
            let mut file = create_new_private_file(&temporary)?;
            check_file_device(&self.root, &file)?;
            let mut checksum = Sha256::new();
            checksum.update((header_bytes.len() as u64).to_le_bytes());
            checksum.update(&header_bytes);
            file.write_all(MAGIC).map_err(generic)?;
            file.write_all(&(header_bytes.len() as u32).to_le_bytes())
                .map_err(generic)?;
            file.write_all(&header_bytes).map_err(generic)?;
            let mut written = 0u64;
            for chunk in payload {
                file.write_all(&chunk).map_err(generic)?;
                checksum.update(&chunk);
                written = written
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| generic("local payload size overflow"))?;
            }
            if written != header.size {
                return Err(generic("local payload length changed while writing"));
            }
            file.write_all(&checksum.finalize()).map_err(generic)?;
            ordinary_sync(&file).map_err(durability)?;
            Ok(file)
        })();
        let file = match prepared {
            Ok(file) => file,
            Err(error) => return Err(error),
        };
        if let Err(error) = fs::rename(&temporary, &final_path) {
            return Err(generic(error));
        }
        sync_dir(directory).map_err(durability)?;
        // Flush all namespace parents and the stable lock before enrolling.
        // Coordinator::sync adds its own-file fsync and the sole full flush.
        sync_dir(&self.objects_root()).map_err(durability)?;
        sync_dir(&self.root.join("locks")).map_err(durability)?;
        ordinary_sync(lock).map_err(durability)?;
        self.durability_epochs.sync(&file).map_err(durability)?;
        self.remember_durable(hash, &etag);
        self.remember_key(hash, key);
        names.clear();
        Ok(PublishedVersion { sequence, etag })
    }
    fn prune_versions(
        &self,
        directory: &FsPath,
        names: &[VersionName],
        keep: &std::collections::HashSet<u64>,
    ) -> Result<()> {
        let mut changed = false;
        for name in names {
            if name.temporary || !keep.contains(&name.sequence) {
                fs::remove_file(&name.path).map_err(generic)?;
                changed = true;
            }
        }
        if changed {
            sync_dir(directory).map_err(durability)?;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let (store, key) = (self.clone(), location.clone());
        tokio::task::spawn_blocking(move || store.put_sync(key, payload, opts)).await?
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (store, key) = (self.clone(), location.clone());
        tokio::task::spawn_blocking(move || store.get_sync(key, options)).await?
    }
    async fn delete(&self, location: &Path) -> Result<()> {
        let (store, key) = (self.clone(), location.clone());
        tokio::task::spawn_blocking(move || store.delete_sync(key)).await?
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let (store, prefix) = (self.clone(), prefix.cloned());
        stream::once(async move {
            let objects = tokio::task::spawn_blocking(move || store.list_sync(prefix)).await??;
            Ok::<_, Error>(stream::iter(objects.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let all: Vec<_> = self.list(Some(&prefix)).try_collect().await?;
        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        for meta in all {
            let mut parts = meta.location.prefix_match(&prefix).unwrap();
            match (parts.next(), parts.next()) {
                (Some(first), Some(_)) => {
                    common_prefixes.insert(prefix.child(first));
                }
                _ => {
                    drop(parts);
                    objects.push(meta);
                }
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(unsupported("local multipart uploads are unsupported"))
    }
    async fn copy(&self, _: &Path, _: &Path) -> Result<()> {
        Err(unsupported("local copy is unsupported"))
    }
    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> Result<()> {
        Err(unsupported("local conditional copy is unsupported"))
    }
}

impl LocalObjectStore {
    fn delete_sync(&self, key: Path) -> Result<()> {
        let hash = digest(key.as_ref());
        let lock = self.lock(&hash)?;
        let _current = self
            .read_durable_version(&hash, &lock, false)?
            .filter(|version| version.header.kind == VersionKind::Value)
            .ok_or_else(|| missing(&key))?;
        let directory = self.version_dir(&hash);
        let published = self.publish_version(
            &hash,
            &key,
            &directory,
            Bytes::new().into(),
            VersionKind::Tombstone,
            &lock,
        )?;
        #[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
        tests::crash_boundary("published", &self.root, &key);
        let tombstone_version = self
            .read_version(&hash)?
            .0
            .filter(|version| {
                version.header.sequence == published.sequence
                    && version.header.etag == published.etag
            })
            .ok_or_else(|| generic("published tombstone missing"))?;
        let mut names = self
            .version_names(&hash)?
            .ok_or_else(|| generic("tombstone directory disappeared"))?;
        let tombstone = names
            .iter()
            .find(|name| {
                !name.temporary
                    && name.sequence == published.sequence
                    && name.etag == published.etag
            })
            .ok_or_else(|| generic("published tombstone missing"))?
            .clone();
        for name in names
            .iter()
            .filter(|name| name.sequence != tombstone.sequence)
        {
            fs::remove_file(&name.path).map_err(generic)?;
        }
        sync_dir(&directory).map_err(durability)?;
        #[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
        tests::crash_boundary("pruned", &self.root, &key);
        // This second full flush makes removal of every old live version durable
        // while the durable tombstone is still present.
        ordinary_sync(&lock).map_err(durability)?;
        self.durability_epochs
            .sync(&tombstone_version.file)
            .map_err(durability)?;
        #[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
        tests::crash_boundary("prune-synced", &self.root, &key);
        fs::remove_file(&tombstone.path).map_err(generic)?;
        #[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
        tests::crash_boundary("tombstone-removed", &self.root, &key);
        sync_dir(&directory).map_err(durability)?;
        fs::remove_dir(&directory).map_err(generic)?;
        sync_dir(&self.objects_root()).map_err(durability)?;
        if let Ok(mut certificates) = self.durable_etags.lock() {
            certificates.remove(&hash);
        }
        names.clear();
        Ok(())
    }
}

fn parse_version_name(name: &str) -> Option<VersionName> {
    let (temporary, rest) = if let Some(rest) = name.strip_prefix(".tmp-") {
        (true, rest)
    } else if let Some(rest) = name.strip_prefix('v') {
        (false, rest)
    } else {
        return None;
    };
    let bytes = rest.as_bytes();
    if !rest.is_ascii()
        || bytes.len() != 20 + 1 + 36 + 1 + 20 + 4
        || bytes[20] != b'-'
        || bytes[57] != b'-'
        || !rest.ends_with(".obj")
    {
        return None;
    }
    if !bytes[..20].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let sequence = std::str::from_utf8(&bytes[..20])
        .ok()?
        .parse::<u64>()
        .ok()?;
    if sequence == 0 {
        return None;
    }
    let etag = std::str::from_utf8(&bytes[21..57]).ok()?;
    let parsed = Uuid::parse_str(etag).ok()?;
    if parsed.to_string() != etag {
        return None;
    }
    if !bytes[58..78].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let expected_len = std::str::from_utf8(&bytes[58..78])
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some(VersionName {
        sequence,
        etag: etag.to_owned(),
        expected_len,
        temporary,
        path: PathBuf::new(),
    })
}

fn ensure_empty_or_missing(path: &FsPath, nonempty_message: &'static str) -> Result<()> {
    match fs::read_dir(path) {
        Ok(mut entries) => {
            if entries.next().is_none() {
                Ok(())
            } else {
                Err(generic(nonempty_message))
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(generic(error)),
    }
}

fn platform_supported() -> Result<()> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Ok(())
    } else {
        Err(unsupported(
            "local storage currently requires macOS ARM64 with local APFS",
        ))
    }
}

#[cfg(target_os = "macos")]
fn secure_options() -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .mode(0o600);
    options
}
#[cfg(not(target_os = "macos"))]
fn secure_options() -> OpenOptions {
    OpenOptions::new()
}

fn open_private_file(path: &FsPath, create: bool) -> Result<File> {
    let file = secure_options()
        .read(true)
        .write(create)
        .create(create)
        .open(path)
        .map_err(generic)?;
    check_private_file(&file)?;
    Ok(file)
}
fn open_private_rw_file(path: &FsPath) -> Result<File> {
    let file = secure_options()
        .read(true)
        .write(true)
        .open(path)
        .map_err(generic)?;
    check_private_file(&file)?;
    Ok(file)
}
fn create_new_private_file(path: &FsPath) -> Result<File> {
    let file = secure_options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(generic)?;
    check_private_file(&file)?;
    Ok(file)
}
#[cfg(target_os = "macos")]
fn check_private_file(file: &File) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata().map_err(generic)?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(generic(
            "local store file must be a private, singly linked regular file owned by this user",
        ));
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn check_private_file(_: &File) -> Result<()> {
    platform_supported()
}
#[cfg(target_os = "macos")]
fn check_private_dir(path: &FsPath) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path).map_err(generic)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(generic(
            "local store directories must be private (0700), real directories owned by this user",
        ));
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn check_private_dir(_: &FsPath) -> Result<()> {
    platform_supported()
}
fn create_private_dir(path: &FsPath) -> Result<()> {
    #[cfg(target_os = "macos")]
    let mut builder = fs::DirBuilder::new();
    #[cfg(not(target_os = "macos"))]
    let builder = fs::DirBuilder::new();
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(generic(error)),
    }
    check_private_dir(path)
}
fn create_root(root: &FsPath) -> Result<()> {
    let mut path = PathBuf::new();
    for part in root.components() {
        path.push(part);
        match fs::symlink_metadata(&path) {
            Ok(meta) if !meta.is_dir() => {
                return Err(generic(
                    "local storage path contains a symlink or non-directory",
                ));
            }
            #[cfg(target_os = "macos")]
            Ok(meta) => {
                use std::os::unix::fs::MetadataExt;
                // A sticky system temp directory is safe: other users cannot
                // replace our owned child. Reject other writable ancestors.
                if (meta.uid() != 0 && meta.uid() != unsafe { libc::geteuid() })
                    || (meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0)
                {
                    return Err(generic(
                        "local storage has an untrusted or writable ancestor",
                    ));
                }
            }
            #[cfg(not(target_os = "macos"))]
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => create_private_dir(&path)?,
            Err(error) => return Err(generic(error)),
        }
    }
    Ok(())
}
#[cfg(target_os = "macos")]
pub(super) fn check_filesystem(path: &FsPath) -> Result<()> {
    use std::{ffi::CStr, os::fd::AsRawFd};
    let file = secure_options().read(true).open(path).map_err(generic)?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(generic(io::Error::last_os_error()));
    }
    let stat = unsafe { stat.assume_init() };
    let filesystem = unsafe { CStr::from_ptr(stat.f_fstypename.as_ptr()) };
    if filesystem.to_bytes() != b"apfs" || stat.f_flags & libc::MNT_LOCAL as u32 == 0 {
        return Err(unsupported(
            "local storage currently supports only local APFS volumes",
        ));
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
pub(super) fn check_filesystem(_: &FsPath) -> Result<()> {
    platform_supported()
}
#[cfg(target_os = "macos")]
fn check_same_filesystem(root: &FsPath, bootstrap: &File) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    // check_filesystem(root) established local APFS. st_dev identifies its
    // mounted filesystem; paths beneath a directory can still be mount points.
    let root_file = secure_options().read(true).open(root).map_err(generic)?;
    let device = root_file.metadata().map_err(generic)?.dev();
    let check = |file: &File| -> Result<()> {
        if file.metadata().map_err(generic)?.dev() != device {
            return Err(unsupported(
                "local store root, objects, locks, and bootstrap must be on the same APFS filesystem; nested mounts are unsupported",
            ));
        }
        Ok(())
    };
    check(bootstrap)?;
    for name in ["objects", "locks"] {
        let file = secure_options()
            .read(true)
            .open(root.join(name))
            .map_err(generic)?;
        check(&file)?;
    }
    let marker = secure_options()
        .read(true)
        .open(root.join("format"))
        .map_err(generic)?;
    check(&marker)?;
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn check_same_filesystem(_: &FsPath, _: &File) -> Result<()> {
    platform_supported()
}
#[cfg(target_os = "macos")]
fn check_same_device(root: &FsPath, path: &FsPath) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let root_file = secure_options().read(true).open(root).map_err(generic)?;
    let entry = secure_options().read(true).open(path).map_err(generic)?;
    if root_file.metadata().map_err(generic)?.dev() != entry.metadata().map_err(generic)?.dev() {
        return Err(unsupported(
            "local version directories cannot cross filesystems",
        ));
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn check_same_device(_: &FsPath, _: &FsPath) -> Result<()> {
    platform_supported()
}
#[cfg(target_os = "macos")]
fn check_file_device(root: &FsPath, file: &File) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let root_file = secure_options().read(true).open(root).map_err(generic)?;
    if root_file.metadata().map_err(generic)?.dev() != file.metadata().map_err(generic)?.dev() {
        return Err(unsupported(
            "local version and lock files cannot cross filesystems",
        ));
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn check_file_device(_: &FsPath, _: &File) -> Result<()> {
    platform_supported()
}
#[cfg(target_os = "macos")]
fn sync_dir(path: &FsPath) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let dir = secure_options().read(true).open(path)?;
    if unsafe { libc::fsync(dir.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
#[cfg(not(target_os = "macos"))]
fn sync_dir(_: &FsPath) -> io::Result<()> {
    Err(io::Error::other("unsupported local storage platform"))
}
#[cfg(target_os = "macos")]
pub(super) fn ordinary_sync(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
#[cfg(not(target_os = "macos"))]
pub(super) fn ordinary_sync(_: &File) -> io::Result<()> {
    Err(io::Error::other("unsupported local storage platform"))
}
#[cfg(target_os = "macos")]
pub(super) fn full_sync(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // F_FULLFSYNC includes the drive-cache flush that plain fsync lacks on macOS.
    // Never downgrade to fsync if the device refuses this request.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
#[cfg(not(target_os = "macos"))]
pub(super) fn full_sync(_: &File) -> io::Result<()> {
    Err(io::Error::other("unsupported local storage platform"))
}

#[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
mod tests;

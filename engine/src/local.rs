//! Private, local APFS object storage. All participants must use this adapter.
//!
//! Lock files are permanent: removing one can split locking between processes.
//! A key's header and body are one atomically replaced file. Under the key lock,
//! readers validate the current envelope and establish a durability barrier for
//! any version not already confirmed durable by this store, and for absence.
//! Bounded process-local ETag certificates avoid repeating that barrier for an
//! unchanged, previously durable version; bytes and metadata are always reread.
//! A writer dying after rename therefore cannot turn an uncertain publication
//! into a successful, but non-durable, read. Certified reads are not new fsync
//! health probes; writes always perform their durability barriers.
use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
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
const MAGIC: &[u8; 8] = b"DEOOSL01";
const MAX_HEADER: usize = 64 * 1024;
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
fn durability(error: io::Error) -> Error {
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
}
impl fmt::Display for LocalObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DEOOS local ({})", self.root.display())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    key: String,
    etag: String,
    size: u64,
    modified_ms: u64,
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
    pub(super) fn new(root: impl AsRef<FsPath>) -> Result<Self> {
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
        let store = Self {
            root: root.to_path_buf(),
            key_hints: Arc::new(Mutex::new(HashMap::new())),
            durable_etags: Arc::new(Mutex::new(HashMap::new())),
        };
        check_private_dir(root)?;
        check_filesystem(root)?;
        let bootstrap = store.bootstrap()?;
        create_private_dir(&store.root.join("objects"))?;
        create_private_dir(&store.root.join("locks"))?;
        sync_dir(&store.root.join("objects")).map_err(durability)?;
        sync_dir(&store.root.join("locks")).map_err(durability)?;
        // Flush all ancestor directory entries too: an earlier initializer may
        // have died between creating the root and persisting its parent entry.
        for dir in store.root.ancestors() {
            sync_dir(dir).map_err(durability)?;
        }
        full_sync(&bootstrap).map_err(durability)?;
        Ok(store)
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
    /// Caller holds the stable key lock for this entire operation and its use of
    /// the returned file. Published envelopes are immutable; every put gets a
    /// fresh UUID, including delete/recreate and same-content replacements.
    fn read_durable_header(&self, hash: &str, lock: &File) -> Result<Option<(Header, File)>> {
        let current = self.read_header(hash);
        if let Ok(Some((header, _))) = &current
            && self.confirmed_durable(hash, &header.etag)
        {
            return current;
        }
        // Even absence and invalid/unreadable headers require this barrier.
        // A barrier failure takes precedence over the original read error.
        self.barrier(lock)?;
        if let Ok(Some((header, _))) = &current {
            self.remember_durable(hash, &header.etag);
        }
        current
    }
    fn bootstrap(&self) -> Result<File> {
        let file = open_private_file(&self.root.join("bootstrap.lock"), true)?;
        file.lock().map_err(generic)?;
        Ok(file)
    }
    fn lock(&self, hash: &str) -> Result<File> {
        let file = open_private_file(&self.root.join("locks").join(hash), true)?;
        file.lock().map_err(generic)?;
        Ok(file)
    }
    fn object_path(&self, hash: &str) -> PathBuf {
        self.root.join("objects").join(hash)
    }
    fn barrier(&self, lock: &File) -> Result<()> {
        sync_dir(&self.root.join("objects")).map_err(durability)?;
        sync_dir(&self.root.join("locks")).map_err(durability)?;
        full_sync(lock).map_err(durability)
    }
    fn read_header(&self, hash: &str) -> Result<Option<(Header, File)>> {
        let mut file = match open_private_file(&self.object_path(hash), false) {
            Ok(file) => file,
            Err(Error::Generic { source, .. })
                if source
                    .downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::NotFound) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut prefix = [0; 12];
        file.read_exact(&mut prefix).map_err(generic)?;
        if &prefix[..8] != MAGIC {
            return Err(generic("invalid local object envelope"));
        }
        let length = u32::from_le_bytes(prefix[8..].try_into().unwrap()) as usize;
        if length == 0 || length > MAX_HEADER {
            return Err(generic("invalid local object header length"));
        }
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).map_err(generic)?;
        let header: Header = serde_json::from_slice(&bytes).map_err(generic)?;
        if digest(&header.key) != hash
            || Uuid::parse_str(&header.etag).is_err()
            || header.modified_ms > 253_402_300_799_999
            || header.size.checked_add(12 + length as u64)
                != Some(file.metadata().map_err(generic)?.len())
        {
            return Err(generic("invalid local object metadata or size"));
        }
        // Validate logical paths even for metadata-only callers.
        let parsed = Path::parse(&header.key)?;
        if parsed.as_ref() != header.key {
            return Err(generic("noncanonical local object key"));
        }
        self.remember_key(hash, &parsed);
        Ok(Some((header, file)))
    }
    fn put_sync(&self, key: Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        if !opts.attributes.is_empty() {
            return Err(unsupported("local object attributes are unsupported"));
        }
        let hash = digest(key.as_ref());
        let lock = self.lock(&hash)?;
        let current = self.read_durable_header(&hash, &lock)?;
        match opts.mode {
            PutMode::Create if current.is_some() => {
                return Err(Error::AlreadyExists {
                    path: key.to_string(),
                    source: "object already exists".into(),
                });
            }
            PutMode::Update(version) => {
                if version.version.is_some()
                    || version.e_tag.is_none()
                    || current.as_ref().map(|(h, _)| &h.etag) != version.e_tag.as_ref()
                {
                    return Err(precondition(&key));
                }
            }
            _ => {}
        }
        let etag = Uuid::new_v4().to_string();
        let header = Header {
            key: key.to_string(),
            etag: etag.clone(),
            size: payload.content_length() as u64,
            modified_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(generic)?
                .as_millis()
                .try_into()
                .map_err(generic)?,
        };
        let header_bytes = serde_json::to_vec(&header).map_err(generic)?;
        if header_bytes.len() > MAX_HEADER {
            return Err(generic("local object key is too long"));
        }
        let temporary = self
            .root
            .join("objects")
            .join(format!(".tmp-{}", Uuid::new_v4()));
        let result = (|| {
            let mut file = create_new_private_file(&temporary)?;
            file.write_all(MAGIC).map_err(generic)?;
            file.write_all(&(header_bytes.len() as u32).to_le_bytes())
                .map_err(generic)?;
            file.write_all(&header_bytes).map_err(generic)?;
            for chunk in payload {
                file.write_all(&chunk).map_err(generic)?;
            }
            full_sync(&file).map_err(durability)?;
            fs::rename(&temporary, self.object_path(&hash)).map_err(generic)?;
            self.barrier(&lock)?;
            self.remember_durable(&hash, &etag);
            self.remember_key(&hash, &key);
            Ok(PutResult {
                e_tag: Some(etag),
                version: None,
            })
        })();
        // Temp files are never visible objects. A crash may leave one behind.
        let _ = fs::remove_file(&temporary);
        result
    }
    fn get_sync(&self, key: Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(unsupported("local object versions are unsupported"));
        }
        let hash = digest(key.as_ref());
        let lock = self.lock(&hash)?;
        let (header, mut file) = self
            .read_durable_header(&hash, &lock)?
            .ok_or_else(|| missing(&key))?;
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
            let mut bytes = Vec::new();
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
        self.barrier(&bootstrap)?;
        let mut objects = Vec::new();
        for entry in fs::read_dir(self.root.join("objects")).map_err(generic)? {
            let entry = entry.map_err(generic)?;
            let name = entry.file_name();
            let hash = name
                .to_str()
                .ok_or_else(|| generic("invalid local object filename"))?;
            if hash.starts_with(".tmp-") {
                continue;
            }
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(generic("unexpected entry in local objects directory"));
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
            if let Some((header, _)) = self.read_durable_header(hash, &lock)? {
                let meta = header.meta()?;
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
        tokio::task::spawn_blocking(move || {
            let hash = digest(key.as_ref());
            let lock = store.lock(&hash)?;
            store.barrier(&lock)?;
            if store.read_header(&hash)?.is_none() {
                return Err(missing(&key));
            }
            fs::remove_file(store.object_path(&hash)).map_err(generic)?;
            store.barrier(&lock)
        })
        .await?
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
fn check_filesystem(path: &FsPath) -> Result<()> {
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
fn check_filesystem(_: &FsPath) -> Result<()> {
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
fn full_sync(file: &File) -> io::Result<()> {
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
fn full_sync(_: &File) -> io::Result<()> {
    Err(io::Error::other("unsupported local storage platform"))
}

//! Real APFS process-crash regression. Test observations are absent from releases.
//! Independent syscall traces cover barrier ordering; this is not a power-loss test.
use super::*;
use std::{
    os::unix::process::ExitStatusExt,
    process::{Command, Stdio},
    thread,
};

const KEY: &str = "delete-probe/key";
const ROOT_ENV: &str = "DEOOS_LOCAL_DELETE_TEST_ROOT";
const POINT_ENV: &str = "DEOOS_LOCAL_DELETE_TEST_POINT";
const CHILD_ENV: &str = "DEOOS_LOCAL_DELETE_TEST_CHILD";

pub(super) fn crash_boundary(point: &str, root: &FsPath, key: &Path) {
    if std::env::var(CHILD_ENV).as_deref() == Ok("1")
        && std::env::var(POINT_ENV).as_deref() == Ok(point)
        && std::env::var_os(ROOT_ENV).as_deref() == Some(root.as_os_str())
        && key.as_ref() == KEY
    {
        unsafe { libc::raise(libc::SIGKILL) };
        std::process::abort();
    }
}

fn open(root: &FsPath) -> LocalObjectStore {
    let root = LocalObjectStore::prepare_root(root).unwrap();
    let bootstrap = LocalObjectStore::lock_bootstrap(&root).unwrap();
    LocalObjectStore::new_locked(&root, bootstrap).unwrap()
}

#[test]
fn delete_crash_child() {
    if std::env::var(CHILD_ENV).as_deref() != Ok("1") {
        return;
    }
    let root = PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
    let temporary = std::env::temp_dir().canonicalize().unwrap();
    assert_eq!(root.parent(), Some(temporary.as_path()));
    let name = root.file_name().unwrap().to_str().unwrap();
    Uuid::parse_str(name.strip_prefix("deoos-delete-test-").unwrap()).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { open(&root).delete(&Path::from(KEY)).await.unwrap() });
    panic!("delete did not reach its requested crash boundary");
}

#[tokio::test]
async fn deletion_survives_process_crashes() {
    for point in ["published", "pruned", "prune-synced", "tombstone-removed"] {
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("deoos-delete-test-{}", Uuid::new_v4()));
        let key = Path::from(KEY);
        let seed = open(&root);
        let old_etag = seed
            .put_opts(
                &key,
                Bytes::from_static(b"old").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .e_tag
            .unwrap();
        drop(seed);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "local::adapter::tests::delete_crash_child",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .env(ROOT_ENV, &root)
            .env(POINT_ENV, point)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10))
                }
                other => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "delete child failed at {point}; retained {}: {other:?}",
                        root.display()
                    );
                }
            }
        };
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "{point}; retained {}",
            root.display()
        );
        let reader = open(&root);
        assert!(
            matches!(reader.get(&key).await, Err(Error::NotFound { .. })),
            "{point}"
        );
        let listed: Vec<_> = reader.list(None).try_collect().await.unwrap();
        assert!(listed.is_empty(), "{point}");
        let new_etag = reader
            .put_opts(
                &key,
                Bytes::from_static(b"new").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .e_tag
            .unwrap();
        assert_ne!(old_etag, new_etag, "{point}");
        let stale = reader
            .put_opts(
                &key,
                Bytes::from_static(b"stale").into(),
                PutOptions {
                    mode: PutMode::Update(object_store::UpdateVersion {
                        e_tag: Some(old_etag),
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(stale, Err(Error::Precondition { .. })), "{point}");
        reader
            .put_opts(
                &key,
                Bytes::from_static(b"updated").into(),
                PutOptions {
                    mode: PutMode::Update(object_store::UpdateVersion {
                        e_tag: Some(new_etag),
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            reader.get(&key).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"updated")
        );
        let listed: Vec<_> = reader.list(None).try_collect().await.unwrap();
        assert_eq!(listed.len(), 1, "{point}");
        drop(reader);
        fs::remove_dir_all(&root).unwrap();
    }
}

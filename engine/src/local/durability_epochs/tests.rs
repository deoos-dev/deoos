//! Real coordinator/threads/files. Gates are per instance and absent in release.
use super::*;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::PathBuf,
    sync::{Weak, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const WATCHDOG: Duration = Duration::from_secs(30);
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum Point {
    Ordinary,
    Enrolled,
    Waiting,
    Full,
    BeforeObserve,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Event {
    pub point: Point,
    pub fd: i32,
    pub epoch: Option<usize>,
}
pub(super) struct Hook(pub Arc<dyn Fn(Event) -> io::Result<()> + Send + Sync>);
impl fmt::Debug for Hook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test-only Hook")
    }
}
#[derive(Clone, Copy)]
enum Action {
    Pass,
    Errno(i32),
    Panic,
}
#[derive(Default)]
struct Controls {
    fd_ids: HashMap<i32, usize>,
    rows: Vec<(usize, Event)>,
    rules: HashMap<(usize, Point), (bool, Action)>,
    released: HashSet<(usize, Point)>,
    release_all: bool,
    fault: Option<&'static str>,
}
#[derive(Default)]
struct Gates {
    state: Mutex<Controls>,
    changed: Condvar,
}
impl Gates {
    fn lock(&self) -> MutexGuard<'_, Controls> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    fn rule(&self, id: usize, point: Point, blocked: bool, action: Action) {
        // Waiting runs under coordinator M: notification only, never a gate.
        assert_ne!(point, Point::Waiting);
        self.lock().rules.insert((id, point), (blocked, action));
    }
    fn release(&self, id: usize, point: Point) {
        self.lock().released.insert((id, point));
        self.changed.notify_all();
    }
    fn release_all(&self) {
        self.lock().release_all = true;
        self.changed.notify_all();
    }
    fn event(&self, event: Event) -> io::Result<()> {
        let mut state = self.lock();
        let id = state.fd_ids[&event.fd];
        if state.rows.len() >= 4096 {
            state.fault = Some("test event log overflow");
            return Err(io::Error::other("test event log overflow"));
        }
        state.rows.push((id, event));
        self.changed.notify_all();
        let (blocked, action) = state
            .rules
            .get(&(id, event.point))
            .copied()
            .unwrap_or((false, Action::Pass));
        let deadline = Instant::now() + WATCHDOG;
        while blocked && !state.release_all && !state.released.contains(&(id, event.point)) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.fault = Some("test gate watchdog expired");
                return Err(io::Error::other("test gate watchdog expired"));
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        drop(state);
        match action {
            Action::Pass => Ok(()),
            Action::Errno(code) => Err(io::Error::from_raw_os_error(code)),
            Action::Panic => panic!("controlled coordinator participant panic"),
        }
    }
    fn wait(&self, predicate: impl Fn(&[(usize, Event)]) -> bool) {
        let deadline = Instant::now() + WATCHDOG;
        let mut state = self.lock();
        while !predicate(&state.rows) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "event watchdog expired: {:?}",
                state.rows
            );
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }
    fn seen(&self, id: usize, point: Point) {
        self.wait(|rows| rows.iter().any(|(who, e)| *who == id && e.point == point));
    }
    fn count(&self, id: usize, point: Point) -> usize {
        self.lock()
            .rows
            .iter()
            .filter(|(who, e)| *who == id && e.point == point)
            .count()
    }
    fn epoch(&self, id: usize) -> usize {
        self.lock()
            .rows
            .iter()
            .find(|(who, e)| *who == id && e.point == Point::Enrolled)
            .unwrap()
            .1
            .epoch
            .unwrap()
    }
    fn full_leader(&self, ids: &[usize]) -> usize {
        self.wait(|rows| {
            rows.iter()
                .any(|(id, e)| ids.contains(id) && e.point == Point::Full)
        });
        self.lock()
            .rows
            .iter()
            .find(|(id, e)| ids.contains(id) && e.point == Point::Full)
            .unwrap()
            .0
    }
}
struct Job {
    path: PathBuf,
    fd: i32,
    result: mpsc::Receiver<io::Result<()>>,
    thread: Option<JoinHandle<()>>,
}
struct Harness {
    root: PathBuf,
    coordinator: Arc<Coordinator>,
    gates: Arc<Gates>,
    jobs: HashMap<usize, Job>,
}
impl Harness {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("deoos-epoch-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        super::super::check_filesystem(&root).unwrap();
        let gates = Arc::new(Gates::default());
        let callback = gates.clone();
        let coordinator = Arc::new(Coordinator {
            hook: Some(Hook(Arc::new(move |event| callback.event(event)))),
            ..Coordinator::default()
        });
        Self {
            root,
            coordinator,
            gates,
            jobs: HashMap::new(),
        }
    }
    fn gate(&self, id: usize, point: Point, action: Action) {
        self.gates.rule(id, point, true, action);
    }
    fn start(&mut self, id: usize) {
        assert!(!self.jobs.contains_key(&id));
        let path = self.root.join(format!("call-{id}"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        writeln!(file, "owned caller {id}").unwrap();
        let fd = file.as_raw_fd();
        self.gates.lock().fd_ids.insert(fd, id);
        let coordinator = self.coordinator.clone();
        let (send, result) = mpsc::channel();
        let thread = thread::spawn(move || {
            file.lock().unwrap();
            let result = coordinator.sync(&file);
            drop(file); // completion message proves descriptor/key lock release
            send.send(result).unwrap();
        });
        self.jobs.insert(
            id,
            Job {
                path,
                fd,
                result,
                thread: Some(thread),
            },
        );
    }
    fn result(&mut self, id: usize) -> io::Result<()> {
        let job = self.jobs.get_mut(&id).unwrap();
        let result = job
            .result
            .recv_timeout(WATCHDOG)
            .expect("caller did not terminate");
        job.thread
            .take()
            .unwrap()
            .join()
            .expect("worker panicked outside coordinator");
        result
    }
    fn pending(&self, id: usize) {
        assert!(
            matches!(
                self.jobs[&id].result.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ),
            "caller acknowledged before its gate was released"
        );
    }
    fn fd_and_lock_live(&self, id: usize) {
        let job = &self.jobs[&id];
        assert!(unsafe { libc::fcntl(job.fd, libc::F_GETFD) } >= 0);
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&job.path)
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::EWOULDBLOCK)
        );
    }
    fn weak_pending(&self) -> Weak<Epoch> {
        Arc::downgrade(self.coordinator.lock().pending.as_ref().unwrap())
    }
    fn clean(&self) {
        assert!(self.gates.lock().fault.is_none(), "test gate/trace failed");
        assert!(self.jobs.values().all(|job| job.thread.is_none()));
        let state = self.coordinator.lock();
        assert_eq!(state.live_tickets, 0);
        assert!(state.active.is_none() && state.pending.is_none());
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.gates.release_all();
        self.coordinator.changed.notify_all();
        let mut joined = true;
        let deadline = Instant::now() + WATCHDOG;
        for job in self.jobs.values_mut() {
            if let Some(handle) = job.thread.take() {
                match job
                    .result
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                {
                    Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                        let _ = handle.join();
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        joined = false;
                        eprintln!("FAILED epoch cleanup; retained {}", self.root.display());
                    }
                }
            }
        }
        if joined {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
}
fn assert_507(result: io::Result<()>, errno: Option<i32>) {
    let error = result.expect_err("failed durability must never become success");
    if let Some(errno) = errno {
        assert_eq!(error.raw_os_error(), Some(errno));
    }
    let (status, message) = crate::storage(super::super::durability(error));
    assert_eq!(status.as_u16(), 507);
    assert!(message.contains("durability") && message.contains("uncertain"));
}
fn active_with_pending(h: &mut Harness, pending: &[usize]) {
    h.gate(0, Point::Full, Action::Pass);
    h.start(0);
    h.gates.seen(0, Point::Full); // actual sealed leader, not test-gap sharing
    for &id in pending {
        h.gate(id, Point::Full, Action::Pass);
        h.start(id);
        h.gates.seen(id, Point::Waiting);
    }
}

#[test]
fn pending_pair_shares_but_late_arrival_needs_another_flush() {
    let mut h = Harness::new();
    active_with_pending(&mut h, &[1, 2]);
    assert_eq!(h.gates.epoch(1), h.gates.epoch(2));
    let pending = h.weak_pending();
    {
        drop(h.coordinator.lock());
    }
    h.coordinator.changed.notify_all();
    h.gates.wait(|rows| {
        [1, 2].iter().all(|id| {
            rows.iter()
                .filter(|(who, e)| who == id && e.point == Point::Waiting)
                .count()
                >= 2
        })
    });
    h.pending(1);
    h.pending(2);
    h.gates.release(0, Point::Full);
    h.result(0).unwrap();
    let leader = h.gates.full_leader(&[1, 2]);
    h.fd_and_lock_live(leader);
    h.pending(1);
    h.pending(2);
    h.gate(3, Point::Full, Action::Pass);
    h.start(3);
    h.gates.seen(3, Point::Waiting);
    assert_ne!(h.gates.epoch(3), h.gates.epoch(1));
    h.gates.release(leader, Point::Full);
    h.result(1).unwrap();
    h.result(2).unwrap();
    assert!(pending.upgrade().is_none());
    h.gates.seen(3, Point::Full);
    h.pending(3);
    h.gates.release(3, Point::Full);
    h.result(3).unwrap();
    assert_eq!(
        (0..4)
            .map(|id| h.gates.count(id, Point::Full))
            .sum::<usize>(),
        3
    );
    h.clean();
}

#[test]
fn failed_epoch_keeps_errno_after_later_direct_success() {
    for errno in [libc::EIO, libc::EINTR] {
        let mut h = Harness::new();
        active_with_pending(&mut h, &[1, 2]);
        let pending = h.weak_pending();
        for id in [1, 2] {
            h.gate(id, Point::Full, Action::Errno(errno));
            h.gate(id, Point::BeforeObserve, Action::Pass);
        }
        h.gates.release(0, Point::Full);
        h.result(0).unwrap();
        let leader = h.gates.full_leader(&[1, 2]);
        h.gates.release(leader, Point::Full);
        for id in [1, 2] {
            h.gates.seen(id, Point::BeforeObserve);
            h.pending(id);
        }
        h.start(3);
        h.result(3).unwrap();
        assert_eq!(h.gates.count(3, Point::Enrolled), 0);
        assert!(pending.upgrade().is_some());
        for id in [1, 2] {
            h.gates.release(id, Point::BeforeObserve);
            assert_507(h.result(id), Some(errno));
        }
        assert!(pending.upgrade().is_none());
        h.clean();
    }
}

#[test]
fn unobserved_success_survives_a_later_failed_epoch() {
    let mut h = Harness::new();
    active_with_pending(&mut h, &[1, 2]);
    h.gate(2, Point::BeforeObserve, Action::Pass);
    let pending = h.weak_pending();
    h.gates.release(0, Point::Full);
    h.result(0).unwrap();
    let leader = h.gates.full_leader(&[1, 2]);
    h.gates.release(leader, Point::Full);
    h.gates.seen(2, Point::BeforeObserve);
    h.result(1).unwrap();
    h.gates
        .rule(3, Point::Full, false, Action::Errno(libc::EIO));
    h.start(3);
    assert_507(h.result(3), Some(libc::EIO));
    h.pending(2);
    assert!(pending.upgrade().is_some());
    h.gates.release(2, Point::BeforeObserve);
    h.result(2).unwrap();
    assert!(pending.upgrade().is_none());
    h.clean();
}

#[test]
fn leader_panic_wakes_pending_members_and_maps_to_507() {
    let mut h = Harness::new();
    h.gate(0, Point::Full, Action::Panic);
    h.start(0);
    h.gates.seen(0, Point::Full);
    for id in [1, 2] {
        h.start(id);
        h.gates.seen(id, Point::Waiting);
    }
    h.gates.release(0, Point::Full);
    for id in 0..3 {
        assert_507(h.result(id), None);
    }
    h.start(3);
    h.result(3).unwrap();
    h.clean();
}

#[test]
fn follower_panic_keeps_active_leader_fd_until_return() {
    let mut h = Harness::new();
    h.gate(0, Point::Full, Action::Pass);
    h.start(0);
    h.gates.seen(0, Point::Full);
    h.gate(1, Point::Enrolled, Action::Panic);
    h.start(1);
    h.gates.seen(1, Point::Enrolled);
    h.start(2);
    h.gates.seen(2, Point::Waiting);
    h.gates.release(1, Point::Enrolled);
    assert_507(h.result(1), None);
    assert_507(h.result(2), None);
    h.fd_and_lock_live(0);
    h.pending(0);
    h.gates.release(0, Point::Full);
    assert_507(h.result(0), None);
    h.start(3);
    h.result(3).unwrap();
    h.clean();
}

#[test]
fn poisoned_condvar_wait_wakes_members_without_erasing_failure() {
    let mut h = Harness::new();
    active_with_pending(&mut h, &[1, 2]);
    let coordinator = h.coordinator.clone();
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _guard = coordinator.state.lock().unwrap();
            panic!("controlled mutex poison while members wait");
        }))
        .is_err()
    );
    h.coordinator.changed.notify_all();
    for id in [1, 2] {
        assert_507(h.result(id), None);
    }
    h.fd_and_lock_live(0);
    h.pending(0);
    h.gates.release(0, Point::Full);
    assert_507(h.result(0), None);
    h.start(3);
    h.result(3).unwrap();
    h.clean();
}

#[test]
fn own_fsync_failure_is_local_and_never_enrolls() {
    let mut h = Harness::new();
    h.gate(0, Point::Full, Action::Pass);
    h.start(0);
    h.gates.seen(0, Point::Full);
    h.gates
        .rule(1, Point::Ordinary, false, Action::Errno(libc::EIO));
    h.start(1);
    assert_507(h.result(1), Some(libc::EIO));
    assert_eq!(h.gates.count(1, Point::Enrolled), 0);
    assert!(!h.coordinator.lock().disabled);
    h.gates.release(0, Point::Full);
    h.result(0).unwrap();
    h.start(2);
    h.result(2).unwrap();
    h.clean();
}

#[test]
fn sixty_four_unobserved_tickets_force_direct_sixty_fifth_flush() {
    let mut h = Harness::new();
    for id in 0..64 {
        h.gate(id, Point::BeforeObserve, Action::Pass);
    }
    let pending_ids: Vec<_> = (1..64).collect();
    active_with_pending(&mut h, &pending_ids);
    let pending = h.weak_pending();
    let active = Arc::downgrade(h.coordinator.lock().active.as_ref().unwrap());
    assert_eq!(h.coordinator.lock().live_tickets, 64);
    h.gates.release(0, Point::Full);
    let leader = h.gates.full_leader(&pending_ids);
    h.gates.release(leader, Point::Full);
    for id in 0..64 {
        h.gates.seen(id, Point::BeforeObserve);
    }
    assert_eq!(h.coordinator.lock().live_tickets, 64);
    assert!(h.coordinator.lock().active.is_none());
    h.gate(64, Point::Full, Action::Pass);
    h.start(64);
    h.gates.seen(64, Point::Full);
    assert_eq!(h.gates.count(64, Point::Enrolled), 0);
    h.fd_and_lock_live(64);
    h.pending(64);
    h.gates.release(64, Point::Full);
    h.result(64).unwrap();
    assert_eq!(h.coordinator.lock().live_tickets, 64);
    assert!(active.upgrade().is_some() && pending.upgrade().is_some());
    for id in 0..64 {
        h.gates.release(id, Point::BeforeObserve);
        h.result(id).unwrap();
    }
    assert!(active.upgrade().is_none() && pending.upgrade().is_none());
    assert_eq!(
        (0..65)
            .map(|id| h.gates.count(id, Point::Full))
            .sum::<usize>(),
        3
    );
    h.clean();
}

//! Clone-shared flushes; enrollment occurs only after the caller's own fsync.
//! Slot identity seals membership. Completed results live with their tickets.
use std::{
    fs::File,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, LockResult, Mutex, MutexGuard, OnceLock},
};

const MAX_TICKETS: usize = 64;

#[cfg(all(test, target_os = "macos"))]
mod tests;

#[derive(Clone, Copy, Debug)]
enum Failure {
    Os(i32),
    Internal(&'static str),
}
impl Failure {
    fn from_io(error: io::Error) -> Self {
        error.raw_os_error().map(Self::Os).unwrap_or(Self::Internal(
            "local full flush failed without an OS error code",
        ))
    }
    fn into_io(self) -> io::Error {
        match self {
            Self::Os(code) => io::Error::from_raw_os_error(code),
            Self::Internal(message) => io::Error::other(message),
        }
    }
}

type Outcome = Result<(), Failure>;
#[derive(Debug, Default)]
struct Epoch(OnceLock<Outcome>);

#[derive(Debug, Default)]
struct State {
    disabled: bool,
    active: Option<Arc<Epoch>>,
    pending: Option<Arc<Epoch>>,
    // Includes terminal outcomes whose callers have not consumed them yet.
    live_tickets: usize,
}

#[derive(Debug, Default)]
pub(super) struct Coordinator {
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(all(test, target_os = "macos"))]
    hook: Option<tests::Hook>,
}

impl Coordinator {
    pub(super) fn sync(&self, file: &File) -> io::Result<()> {
        // Never substitute File::sync_all: on Apple it already performs a full
        // drive flush. Even overflow/disabled fallback retains this prerequisite.
        #[cfg(all(test, target_os = "macos"))]
        self.event(tests::Point::Ordinary, file, None)?;
        super::ordinary_sync(file)?;
        match catch_unwind(AssertUnwindSafe(|| self.prepared(file))) {
            Ok(result) => result,
            Err(_) => {
                let mut state = self.lock();
                self.fail(
                    &mut state,
                    Failure::Internal("local flush coordinator panicked"),
                );
                Err(io::Error::other("local flush coordinator panicked"))
            }
        }
    }

    fn prepared(&self, file: &File) -> io::Result<()> {
        // Declare the ticket before any mutex guard: unwinding must unlock the
        // mutex before Ticket::drop reacquires it to fail/wake other members.
        let mut ticket = Ticket {
            coordinator: self,
            epoch: None,
            enrolled: false,
            leader: false,
        };
        let mut state = self.lock();
        if state.disabled || state.live_tickets >= MAX_TICKETS {
            drop(state);
            return self.full_sync(file);
        }
        let epoch = state
            .pending
            .get_or_insert_with(|| Arc::new(Epoch::default()))
            .clone();
        ticket.epoch = Some(epoch.clone());
        state.live_tickets += 1;
        ticket.enrolled = true;
        #[cfg(all(test, target_os = "macos"))]
        {
            drop(state);
            self.event(tests::Point::Enrolled, file, Some(&epoch))?;
            state = self.lock();
        }
        loop {
            if let Some(outcome) = epoch.0.get().copied() {
                #[cfg(all(test, target_os = "macos"))]
                {
                    drop(state);
                    self.event(tests::Point::BeforeObserve, file, Some(&epoch))?;
                    state = self.lock();
                }
                // Release every caller-owned result reference before opening
                // capacity: a descheduled return must not retain uncounted Arcs.
                ticket.epoch = None;
                drop(epoch);
                state.live_tickets -= 1;
                ticket.enrolled = false;
                return outcome.map_err(Failure::into_io);
            }
            if !state.disabled
                && state.active.is_none()
                && state
                    .pending
                    .as_ref()
                    .is_some_and(|pending| Arc::ptr_eq(pending, &epoch))
            {
                // Only pending members may elect themselves. Late enrollment
                // gets a new pending epoch, never this sealed active one.
                ticket.leader = true;
                state.active = Some(epoch.clone());
                state.pending = None;
                drop(state);
                // Borrow the leader's own live same-filesystem File. No mutex
                // or another caller's key/bootstrap lock is acquired for I/O.
                let outcome = self.full_sync(file).map_err(Failure::from_io);
                state = self.lock();
                assert!(
                    state
                        .active
                        .as_ref()
                        .is_some_and(|active| Arc::ptr_eq(active, &epoch))
                );
                match outcome {
                    Ok(()) => {
                        let _ = epoch.0.set(Ok(()));
                    }
                    Err(error) => self.fail(&mut state, error),
                }
                // Poison/unwind can have published Failure during our syscall;
                // OnceLock never lets a later successful flush replace it.
                state.active = None;
                ticket.leader = false;
                self.changed.notify_all();
                continue;
            }
            if state.disabled {
                let _ = epoch
                    .0
                    .set(Err(Failure::Internal("local flush grouping disabled")));
                continue;
            }
            #[cfg(all(test, target_os = "macos"))]
            self.event(tests::Point::Waiting, file, Some(&epoch))?;
            state = self.recover(self.changed.wait(state));
        }
    }

    fn full_sync(&self, file: &File) -> io::Result<()> {
        #[cfg(all(test, target_os = "macos"))]
        self.event(tests::Point::Full, file, None)?;
        super::full_sync(file)
    }

    #[cfg(all(test, target_os = "macos"))]
    fn event(
        &self,
        point: tests::Point,
        file: &File,
        epoch: Option<&Arc<Epoch>>,
    ) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        if let Some(hook) = &self.hook {
            (hook.0)(tests::Event {
                point,
                fd: file.as_raw_fd(),
                epoch: epoch.map(|epoch| Arc::as_ptr(epoch) as usize),
            })?;
        }
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.recover(self.state.lock())
    }

    fn recover<'a>(&self, result: LockResult<MutexGuard<'a, State>>) -> MutexGuard<'a, State> {
        match result {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                self.fail(
                    &mut state,
                    Failure::Internal("local flush coordinator mutex poisoned"),
                );
                state
            }
        }
    }

    fn fail(&self, state: &mut State, error: Failure) {
        state.disabled = true;
        for epoch in [&state.active, &state.pending].into_iter().flatten() {
            let _ = epoch.0.set(Err(error));
        }
        // Keep active owned until its live leader returns or unwinds. Results
        // already published, including successes, cannot be overwritten.
        state.pending = None;
        self.changed.notify_all();
    }
}

struct Ticket<'a> {
    coordinator: &'a Coordinator,
    epoch: Option<Arc<Epoch>>,
    enrolled: bool,
    leader: bool,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        if !self.enrolled {
            return;
        }
        // An enrolled ticket leaves normally only after consuming its result.
        // This path is unwind recovery; it must not panic or perform file I/O.
        let mut state = self.coordinator.lock();
        let error = Failure::Internal("local flush participant unwound");
        self.coordinator.fail(&mut state, error);
        if let Some(epoch) = self.epoch.take() {
            let _ = epoch.0.set(Err(error));
            if self.leader
                && state
                    .active
                    .as_ref()
                    .is_some_and(|active| Arc::ptr_eq(active, &epoch))
            {
                state.active = None;
            }
        }
        // Even an invariant failure must not double-panic during recovery.
        state.live_tickets = state.live_tickets.saturating_sub(1);
        self.enrolled = false;
        self.coordinator.changed.notify_all();
    }
}

//! Volatile negative hints: LIST change tokens validate them; GET and CAS own execution.
use super::*;
use object_store::ObjectMeta;
use std::collections::{HashMap, HashSet, VecDeque};

const MAX_HINTS: usize = 16_384;
const MAX_PRIORITY: usize = 1_024;

struct Hint {
    version: UpdateVersion,
    handler: String,
    status: String,
    available_at: u64,
    expires_at: u64,
}

impl Hint {
    fn matches(&self, object: &ObjectMeta) -> bool {
        // LIST may omit a generation even when GET supplies it. Missing all
        // change tokens means the optimization is unavailable, not missing work.
        (object.e_tag.is_some() || object.version.is_some())
            && object
                .e_tag
                .as_ref()
                .is_none_or(|tag| self.version.e_tag.as_ref() == Some(tag))
            && object
                .version
                .as_ref()
                .is_none_or(|version| self.version.version.as_ref() == Some(version))
    }
}

#[derive(Default)]
pub(super) struct Hints {
    tasks: HashMap<String, Hint>,
    schedules: HashMap<String, Hint>,
    cursor: String,
    priority: VecDeque<String>,
    queued: HashSet<String>,
    priority_attempts: usize,
}

impl Hints {
    pub(super) fn prioritize(&mut self, task: &Task) {
        if task.status != "waiting" {
            return;
        }
        let Some(WaitCondition::Children { ids }) = &task.waiting_on else {
            return;
        };
        for id in ids.iter().chain(std::iter::once(&task.id)) {
            if self.priority.len() >= MAX_PRIORITY {
                break;
            }
            if self.queued.insert(id.clone()) {
                self.priority.push_back(id.clone());
            }
        }
    }

    pub(super) fn take_priority(&mut self) -> Option<String> {
        // At most one priority attempt per call, with every fourth call starting
        // in the normal lane. Priority never advances the normal discovery cursor.
        if self.priority_attempts >= 3 {
            self.priority_attempts = 0;
            return None;
        }
        let id = self.priority.pop_front()?;
        self.queued.remove(&id);
        self.priority_attempts += 1;
        Some(id)
    }
    pub(super) fn start(
        &mut self,
        tasks: &[(String, ObjectMeta)],
        present: &HashSet<&str>,
    ) -> usize {
        self.tasks.retain(|id, _| present.contains(id.as_str()));
        // Resume after the last examined ID, wrapping once. New tasks remain discoverable.
        tasks.partition_point(|(id, _)| id <= &self.cursor)
    }

    pub(super) fn skip(
        &mut self,
        id: &str,
        object: &ObjectMeta,
        handlers: &[String],
        timestamp: u64,
    ) -> bool {
        self.cursor = id.into();
        let Some(hint) = self.tasks.get(id) else {
            return false;
        };
        hint.matches(object)
            && (!handlers.contains(&hint.handler)
                || hint.available_at > timestamp
                || match hint.status.as_str() {
                    "queued" | "waiting" => false,
                    "running" => hint.expires_at > timestamp,
                    "completed" | "failed" | "cancelled" => true,
                    _ => false,
                })
    }

    pub(super) fn retain_schedules(&mut self, present: &HashSet<&str>) {
        self.schedules.retain(|id, _| present.contains(id.as_str()));
    }

    pub(super) fn skip_schedule(
        &self,
        id: &str,
        object: &ObjectMeta,
        handlers: &[String],
        timestamp: u64,
    ) -> bool {
        self.schedules.get(id).is_some_and(|hint| {
            hint.matches(object)
                && (!handlers.contains(&hint.handler) || hint.available_at > timestamp)
        })
    }

    pub(super) fn remember_schedule(
        &mut self,
        id: &str,
        handler: &str,
        version: &UpdateVersion,
        dormant_until: u64,
    ) {
        if self.schedules.len() >= MAX_HINTS && !self.schedules.contains_key(id) {
            return;
        }
        self.schedules.insert(
            id.into(),
            Hint {
                version: version.clone(),
                handler: handler.into(),
                status: "queued".into(),
                available_at: dormant_until,
                expires_at: 0,
            },
        );
    }

    pub(super) fn remember(&mut self, task: &Task, version: &UpdateVersion) {
        // A full cache only loses an optimization. Uncached tasks still get read.
        if self.tasks.len() >= MAX_HINTS && !self.tasks.contains_key(&task.id) {
            return;
        }
        self.tasks.insert(
            task.id.clone(),
            Hint {
                version: version.clone(),
                handler: task.handler.clone(),
                status: task.status.clone(),
                available_at: task.available_at,
                expires_at: task.expires_at,
            },
        );
    }
}

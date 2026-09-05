//! Handles for tasks spawned through [`Environment::spawn`](crate::Environment::spawn).

use std::fmt;

/// Identifies a task within one environment. Allocated sequentially from 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(u64);

impl TaskId {
    /// Wraps a raw id. Environments allocate these; everything else only compares and
    /// prints them.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&format!("task#{}", self.0))
    }
}

/// What an environment must be able to do to a task it spawned.
pub(crate) trait TaskControl: Send + Sync + 'static {
    /// Requests cancellation.
    fn abort(&self);
}

/// Control over a spawned task. Dropping the handle detaches the task; it keeps running.
pub struct TaskHandle {
    id: TaskId,
    name: &'static str,
    control: Box<dyn TaskControl>,
}

impl TaskHandle {
    pub(crate) fn new(id: TaskId, name: &'static str, control: Box<dyn TaskControl>) -> Self {
        Self { id, name, control }
    }

    /// The task's id.
    #[must_use]
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// The name given to `Environment::spawn`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Requests cancellation. The task stops at its next await point; a task that has
    /// already completed is unaffected.
    pub fn abort(&self) {
        self.control.abort();
    }
}

impl fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHandle")
            .field("id", &self.id)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

//! [`RealEnv`]: the production environment on tokio.
//!
//! This module is the one place in the workspace allowed to touch the real clock, disk,
//! sockets and OS entropy; the `allow` below is what grants it (see clippy.toml). Unix
//! only: positional file I/O goes through `std::os::unix::fs::FileExt`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod clock;
mod fs;
mod net;
mod rng;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::runtime::Handle;

pub use self::clock::RealClock;
pub use self::fs::{RealFile, RealFs};
pub use self::net::{RealNet, RealSocket, SEND_QUEUE_LEN};
pub use self::rng::RealRng;
use crate::task::TaskControl;
use crate::{Environment, TaskHandle, TaskId, TraceEvent};

/// The production [`Environment`]: a tokio runtime, the real disk, OS entropy and the
/// system clock.
///
/// Cheap to clone; every clone shares the same runtime handle, task-id counter and
/// clock epoch.
#[derive(Clone)]
pub struct RealEnv {
    inner: Arc<Inner>,
}

struct Inner {
    handle: Handle,
    clock: RealClock,
    fs: RealFs,
    net: RealNet,
    rng: RealRng,
    next_task: AtomicU64,
}

impl RealEnv {
    /// Builds a multi-threaded tokio runtime, runs `f` on it to completion, then shuts
    /// the runtime down. Tasks still running at that point are cancelled.
    ///
    /// This is how `ananke-server` starts.
    ///
    /// # Panics
    ///
    /// If called from inside a tokio runtime, or if the runtime cannot be built.
    pub fn run<F, Fut>(f: F) -> Fut::Output
    where
        F: FnOnce(RealEnv) -> Fut,
        Fut: Future,
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        let env = RealEnv::from_handle(runtime.handle().clone());
        runtime.block_on(f(env))
    }

    /// Wraps an existing runtime handle. The runtime must have its time driver enabled.
    /// The clock's epoch is the moment of this call.
    #[must_use]
    pub fn from_handle(handle: Handle) -> RealEnv {
        RealEnv {
            inner: Arc::new(Inner {
                fs: RealFs::new(handle.clone()),
                net: RealNet::new(handle.clone()),
                handle,
                clock: RealClock::new(),
                rng: RealRng,
                next_task: AtomicU64::new(1),
            }),
        }
    }
}

impl std::fmt::Debug for RealEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealEnv").finish_non_exhaustive()
    }
}

impl Environment for RealEnv {
    type Clock = RealClock;
    type Fs = RealFs;
    type Net = RealNet;
    type Rng = RealRng;

    fn clock(&self) -> &RealClock {
        &self.inner.clock
    }

    fn fs(&self) -> &RealFs {
        &self.inner.fs
    }

    fn net(&self) -> &RealNet {
        &self.inner.net
    }

    fn rng(&self) -> &RealRng {
        &self.inner.rng
    }

    fn sched_rng(&self) -> &RealRng {
        &self.inner.rng
    }

    fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        name: &'static str,
        f: F,
    ) -> TaskHandle {
        let id = TaskId::new(self.inner.next_task.fetch_add(1, Ordering::Relaxed));
        self.trace(TraceEvent::TaskSpawned { task: id, name });
        let env = self.clone();
        let join = self.inner.handle.spawn(async move {
            f.await;
            env.trace(TraceEvent::TaskCompleted { task: id });
        });
        TaskHandle::new(id, name, Box::new(RealTask(join)))
    }

    fn trace(&self, event: TraceEvent) {
        emit(event);
    }
}

/// Where real-environment trace events go: the `ananke::trace` tracing target at debug
/// level. The moirae studio path is the simulator's; this is for logs.
pub(super) fn emit(event: TraceEvent) {
    tracing::debug!(target: "ananke::trace", ?event);
}

struct RealTask(tokio::task::JoinHandle<()>);

impl TaskControl for RealTask {
    fn abort(&self) {
        self.0.abort();
    }
}

//! The simulator's shared state and scheduler internals.
//!
//! Locking discipline: `Shared::state` is one mutex. The scheduler never holds it while
//! polling a task, and nothing drops a task future while holding it, because futures
//! own handles (sockets, environments) whose destructors take the lock. Wakers push
//! onto the separate `wakes` mutex, so waking under the state lock is safe.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Wake, Waker};

use moirae_sched::{Pcg32, Policy, Scheduler, stream};

use super::fs::NodeFs;
use super::net::Fabric;
use super::rng::SimRng;
use super::{SimConfig, TraceRecord};
use crate::task::TaskControl;
use crate::{Instant, NodeId, TaskId, TraceEvent};

pub(super) type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub(super) struct Shared {
    state: Mutex<State>,
    wakes: Mutex<Vec<TaskId>>,
}

impl Shared {
    pub(super) fn new(state: State) -> Self {
        Self {
            state: Mutex::new(state),
            wakes: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn take_wakes(&self) -> Vec<TaskId> {
        std::mem::take(&mut *self.wakes.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn push_wake(&self, task: TaskId) {
        self.wakes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(task);
    }
}

pub(super) struct Node {
    pub(super) skew_nanos: i64,
    pub(super) drift_ppm: i64,
    /// The node's protocol stream, `n{id}/protocol` (D-017).
    pub(super) protocol: SimRng,
    /// The node's scheduling stream, `n{id}/sched`; what `race` draws from (D-017).
    pub(super) sched: SimRng,
}

pub(super) struct Task {
    pub(super) node: NodeId,
    pub(super) name: &'static str,
    pub(super) future: Option<BoxFuture>,
    pub(super) waker: Waker,
    pub(super) abort_requested: bool,
    /// The instant this task was last polled at, and how many polls it has taken there.
    pub(super) polls_at: (u64, u64),
}

pub(super) struct State {
    pub(super) config: SimConfig,
    pub(super) now: Instant,
    /// Counts the times virtual time moved; a task's poll count restarts with it.
    pub(super) instant: u64,
    /// The scheduling policy in force (D-016); it owns the `sched` stream.
    scheduler: Box<dyn Scheduler>,
    pub(super) policy: Policy,
    /// The `net` stream: drops and delays (D-017).
    pub(super) net_stream: Pcg32,
    /// The `fs` stream: lost fsyncs and torn writes.
    pub(super) fs_stream: Pcg32,
    /// The `clock` stream: skew and drift when a node is added.
    pub(super) clock_stream: Pcg32,
    next_task: u64,
    next_seq: u64,
    pub(super) tasks: BTreeMap<TaskId, Task>,
    runnable: Vec<TaskId>,
    queued: BTreeSet<TaskId>,
    timers: BTreeMap<(Instant, u64), Waker>,
    pub(super) nodes: BTreeMap<NodeId, Node>,
    pub(super) fabric: Fabric,
    pub(super) fs: BTreeMap<NodeId, NodeFs>,
    pub(super) trace: Vec<TraceRecord>,
}

impl State {
    pub(super) fn new(config: SimConfig) -> Self {
        let seed = config.seed;
        let policy = config.policy.unwrap_or_else(|| Policy::for_seed(seed));
        let scheduler = policy.scheduler(seed, config.run_length_hint);
        Self {
            config,
            now: Instant::ZERO,
            instant: 0,
            scheduler,
            policy,
            net_stream: stream(seed, "net"),
            fs_stream: stream(seed, "fs"),
            clock_stream: stream(seed, "clock"),
            next_task: 1,
            next_seq: 0,
            tasks: BTreeMap::new(),
            runnable: Vec::new(),
            queued: BTreeSet::new(),
            timers: BTreeMap::new(),
            nodes: BTreeMap::new(),
            fabric: Fabric::default(),
            fs: BTreeMap::new(),
            trace: Vec::new(),
        }
    }

    pub(super) fn record(&mut self, node: Option<NodeId>, event: TraceEvent) {
        self.trace.push(TraceRecord {
            at: self.now,
            node,
            event,
        });
    }

    /// A fresh tie-breaker for events keyed by time.
    pub(super) fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// A node's own stream, derived from the seed and the node id (D-017).
    pub(super) fn node_stream(&self, node: NodeId, label: &str) -> SimRng {
        SimRng::new(stream(
            self.config.seed,
            &format!("n{}/{label}", node.get()),
        ))
    }

    // --- node clocks -----------------------------------------------------------------

    /// What `node`'s clock reads when global virtual time is `global`.
    pub(super) fn node_time(&self, node: NodeId, global: Instant) -> Instant {
        let n = &self.nodes[&node];
        let g = i128::from(global.as_nanos());
        let t = g + (g * i128::from(n.drift_ppm)).div_euclid(1_000_000) + i128::from(n.skew_nanos);
        Instant::from_nanos(u64::try_from(t.clamp(0, i128::from(u64::MAX))).unwrap_or(u64::MAX))
    }

    /// The earliest global time at which `node`'s clock reads at least `local`.
    pub(super) fn global_time(&self, node: NodeId, local: Instant) -> Instant {
        let n = &self.nodes[&node];
        let t = i128::from(local.as_nanos()) - i128::from(n.skew_nanos);
        if t <= 0 {
            return Instant::ZERO;
        }
        let den = 1_000_000 + i128::from(n.drift_ppm);
        let g = (t * 1_000_000 + den - 1) / den;
        Instant::from_nanos(u64::try_from(g.min(i128::from(u64::MAX))).unwrap_or(u64::MAX))
    }

    // --- tasks -----------------------------------------------------------------------

    pub(super) fn spawn(
        &mut self,
        shared: &Weak<Shared>,
        node: NodeId,
        name: &'static str,
        future: BoxFuture,
    ) -> TaskId {
        let id = TaskId::new(self.next_task);
        self.next_task += 1;
        let waker = Waker::from(Arc::new(SimWaker {
            shared: shared.clone(),
            task: id,
        }));
        self.tasks.insert(
            id,
            Task {
                node,
                name,
                future: Some(future),
                waker,
                abort_requested: false,
                polls_at: (0, 0),
            },
        );
        self.scheduler.spawned(id.get());
        self.record(Some(node), TraceEvent::TaskSpawned { task: id, name });
        self.make_runnable(id);
        id
    }

    pub(super) fn make_runnable(&mut self, id: TaskId) {
        if self.tasks.contains_key(&id) && self.queued.insert(id) {
            self.runnable.push(id);
        }
    }

    /// Asks the policy which runnable task polls next (D-016).
    pub(super) fn pick_runnable(&mut self) -> Option<TaskId> {
        if self.runnable.is_empty() {
            return None;
        }
        let raw: Vec<u64> = self.runnable.iter().map(|t| t.get()).collect();
        let index = self.scheduler.choose(&raw);
        let id = self.runnable.swap_remove(index);
        self.queued.remove(&id);
        Some(id)
    }

    /// Forgets `id`: the policy, the run queue, and the task table. The task's future,
    /// if it is not currently being polled, is handed back so the caller can drop it
    /// outside the lock.
    pub(super) fn take_task(&mut self, id: TaskId) -> Option<Task> {
        self.queued.remove(&id);
        let task = self.tasks.remove(&id)?;
        self.scheduler.finished(id.get());
        Some(task)
    }

    /// Removes `id` and hands back its future so the caller can drop it outside the lock.
    pub(super) fn remove_task(&mut self, id: TaskId) -> Option<BoxFuture> {
        self.take_task(id).and_then(|task| task.future)
    }

    // --- timers ----------------------------------------------------------------------
    //
    // Ordering at equal virtual timestamps is decided by `next_seq`, one counter shared
    // by timers and deliveries, so it follows registration order and never container
    // iteration order: both maps are `BTreeMap`s keyed by `(time, seq)`. At one instant
    // every due timer fires before every due delivery. Firing only makes tasks runnable;
    // which runnable task polls next is the policy's decision.

    pub(super) fn register_timer(&mut self, at: Instant, waker: Waker) {
        let seq = self.next_seq();
        self.timers.insert((at, seq), waker);
    }

    /// The global time of the next timer or delivery, if any.
    pub(super) fn next_event_time(&self) -> Option<Instant> {
        let timer = self.timers.keys().next().map(|(at, _)| *at);
        let delivery = self.fabric.next_delivery_time();
        match (timer, delivery) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Moves global time to `to` (never backwards) and fires everything due, collecting
    /// the wakers to call once the lock is released.
    pub(super) fn advance_to(&mut self, to: Instant, wakers: &mut Vec<Waker>) {
        if to > self.now {
            self.now = to;
            self.instant += 1;
            self.record(None, TraceEvent::TimeAdvanced { to });
        }
        let due: Vec<(Instant, u64)> = self
            .timers
            .range(..=(self.now, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for key in due {
            if let Some(waker) = self.timers.remove(&key) {
                wakers.push(waker);
            }
        }
        for delivery in self.fabric.take_due(self.now) {
            self.deliver(delivery, wakers);
        }
    }
}

/// Wakes a task by queueing its id; the scheduler drains the queue before each pick.
struct SimWaker {
    shared: Weak<Shared>,
    task: TaskId,
}

impl Wake for SimWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            shared.push_wake(self.task);
        }
    }
}

/// `TaskHandle` control for simulated tasks.
pub(super) struct SimTask {
    pub(super) shared: Weak<Shared>,
    pub(super) id: TaskId,
}

impl TaskControl for SimTask {
    fn abort(&self) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let future = {
            let mut st = shared.lock();
            match st.tasks.get_mut(&self.id) {
                // Being polled right now (its own abort, since we are single-threaded):
                // the scheduler drops it when the poll returns.
                Some(task) if task.future.is_none() => {
                    task.abort_requested = true;
                    None
                }
                Some(_) => st.remove_task(self.id),
                None => None,
            }
        };
        drop(future);
    }
}

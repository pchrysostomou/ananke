//! [`SimEnv`]: the deterministic simulator (SPEC.md §1.1, §1.3, §1.4).
//!
//! A [`Sim`] owns a single-threaded executor, a virtual clock, a seeded generator, an
//! in-memory network fabric and one in-memory disk per node. Each node gets a
//! [`SimEnv`], which implements [`Environment`]. Nothing in a run depends on wall-clock
//! time, thread scheduling or OS entropy, so two runs with the same [`SimConfig`]
//! produce byte-identical traces.
//!
//! Scheduling policy (DECISIONS.md D-016): every decision comes from a
//! [`moirae_sched::Scheduler`], chosen per seed by [`moirae_sched::Policy::for_seed`]
//! unless [`SimConfig::policy`] fixes it: half the seeds pick uniformly at random among
//! the runnable tasks, the rest run PCT. Every choice is recorded as
//! [`TraceEvent::TaskPolled`]. When nothing is runnable, virtual time jumps to the next
//! timer or message delivery.
//!
//! Randomness (D-017): every stream is derived from the seed by name, so a scheduling
//! change never moves a fault draw or a protocol draw; see `rng.rs`.
//!
//! Ties at equal virtual timestamps are broken by one sequence counter shared by timers
//! and deliveries, never by container iteration order; see `state.rs`.

mod clock;
mod fs;
mod net;
mod rng;
mod state;
#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use moirae_sched::Policy;

pub use self::clock::{SimClock, SimSleep};
pub use self::fs::{SimFile, SimFs};
pub use self::net::{SimNet, SimRecv, SimSocket};
pub use self::rng::SimRng;
use self::state::{BoxFuture, Shared, SimTask, State};
use crate::{Environment, Instant, NodeId, TaskHandle, TraceEvent, WallTime};

/// Network fault injection (SPEC.md §1.4).
#[derive(Clone, Debug)]
pub struct NetFaults {
    /// Probability that a sent message is lost outright.
    pub p_drop: f64,
    /// Shortest delivery delay.
    pub delay_min: Duration,
    /// Longest delivery delay. Messages with different delays reorder.
    pub delay_max: Duration,
}

impl Default for NetFaults {
    fn default() -> Self {
        Self {
            p_drop: 0.0,
            delay_min: Duration::from_millis(1),
            delay_max: Duration::from_millis(1),
        }
    }
}

/// Filesystem fault injection (SPEC.md §1.3).
#[derive(Clone, Debug)]
pub struct FsFaults {
    /// Probability that `sync` actually persists. `1.0` is strict mode.
    pub p_durable: f64,
}

impl Default for FsFaults {
    fn default() -> Self {
        Self { p_durable: 1.0 }
    }
}

/// Per-node clock imperfection (SPEC.md §1.2), drawn when a node is added.
#[derive(Clone, Debug, Default)]
pub struct ClockFaults {
    /// Largest constant offset, either sign, between a node's clock and global time.
    pub max_skew: Duration,
    /// Largest rate error in parts per million, either sign. Must be below 1,000,000.
    pub max_drift_ppm: u32,
}

/// Everything that determines a run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    /// The seed. Same config, same trace.
    pub seed: u64,
    /// Network faults.
    pub net: NetFaults,
    /// Filesystem faults.
    pub fs: FsFaults,
    /// Clock faults.
    pub clock: ClockFaults,
    /// What a node with zero skew reads as wall time at global time zero.
    pub wall_epoch: WallTime,
    /// The scheduling policy (D-016); `None` picks one from the seed, which is what a
    /// fuzz campaign wants. Fix it to compare the two on one seed.
    pub policy: Option<Policy>,
    /// PCT's estimate of the run's total polls, which sets its change-point rate
    /// (moirae-sched `Pct`). Derive it with [`SimConfig::run_length_hint_for`] from the
    /// scenario's duration and node count; it is never a per-task poll budget.
    pub run_length_hint: u64,
    /// How many times one task may be polled at a single virtual instant. A task that
    /// keeps itself runnable without letting time move is a busy loop; exceeding this
    /// records [`TraceEvent::PollBudgetExceeded`] and panics, so a 10k-seed run fails
    /// instead of hanging. Unrelated to `run_length_hint`.
    pub poll_budget: u64,
}

impl SimConfig {
    /// No faults, one millisecond message delay, wall epoch 2026-01-01T00:00:00Z.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            net: NetFaults::default(),
            fs: FsFaults::default(),
            clock: ClockFaults::default(),
            wall_epoch: WallTime::from_unix_nanos(1_767_225_600 * 1_000_000_000),
            policy: None,
            run_length_hint: 10_000,
            poll_budget: 100_000,
        }
    }

    /// A [`run_length_hint`](Self::run_length_hint) for a scenario: one poll per node per
    /// millisecond of virtual time, which is the right order of magnitude for
    /// message-driven protocols with timers in the tens of milliseconds.
    #[must_use]
    pub fn run_length_hint_for(nodes: u32, duration: Duration) -> u64 {
        u64::from(nodes)
            * u64::try_from(duration.as_millis())
                .unwrap_or(u64::MAX)
                .max(1)
    }
}

/// One trace entry: when, on which node, what.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRecord {
    /// Global virtual time when the event was recorded.
    pub at: Instant,
    /// The node the event belongs to; `None` for the simulator itself.
    pub node: Option<NodeId>,
    /// The event.
    pub event: TraceEvent,
}

/// What the moirae export reads: the configuration, the policy, each node's clock, every
/// address ever bound, and the records.
pub(crate) struct Snapshot {
    pub(crate) config: SimConfig,
    pub(crate) policy: Policy,
    pub(crate) clocks: Vec<(NodeId, i64, i64)>,
    pub(crate) addrs: Vec<(std::net::SocketAddr, NodeId)>,
    pub(crate) records: Vec<TraceRecord>,
}

/// A simulation: the executor, the clock, the fabric, the disks and the trace.
pub struct Sim {
    shared: Arc<Shared>,
}

impl fmt::Debug for Sim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sim")
            .field("now", &self.now())
            .finish_non_exhaustive()
    }
}

impl Sim {
    /// Starts a simulation at global time zero with no nodes.
    ///
    /// # Panics
    ///
    /// If `config` is inconsistent: a delay range with `min > max`, a probability
    /// outside `0..=1`, or a drift of a million ppm or more.
    #[must_use]
    pub fn new(config: SimConfig) -> Self {
        assert!(
            config.net.delay_min <= config.net.delay_max,
            "delay_min must not exceed delay_max"
        );
        assert!(
            (0.0..=1.0).contains(&config.net.p_drop),
            "p_drop must be within 0..=1"
        );
        assert!(
            (0.0..=1.0).contains(&config.fs.p_durable),
            "p_durable must be within 0..=1"
        );
        assert!(
            config.clock.max_drift_ppm < 1_000_000,
            "max_drift_ppm must be below 1,000,000"
        );
        assert!(
            config.run_length_hint > 0,
            "run_length_hint must be positive"
        );
        assert!(config.poll_budget > 0, "poll_budget must be positive");
        Self {
            shared: Arc::new(Shared::new(State::new(config))),
        }
    }

    /// The seed this run was started with.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.shared.lock().config.seed
    }

    /// The scheduling policy this run uses (D-016).
    #[must_use]
    pub fn policy(&self) -> Policy {
        self.shared.lock().policy
    }

    /// Adds a node whose clock skew and drift are drawn from the `clock` stream.
    pub fn add_node(&mut self) -> NodeId {
        let (skew, drift) = {
            let mut st = self.shared.lock();
            let max_skew = i64::try_from(st.config.clock.max_skew.as_nanos()).unwrap_or(i64::MAX);
            let max_drift = i64::from(st.config.clock.max_drift_ppm);
            let skew = rng::symmetric(&mut st.clock_stream, max_skew);
            let drift = rng::symmetric(&mut st.clock_stream, max_drift);
            (skew, drift)
        };
        self.add_node_with_clock(skew, drift)
    }

    /// Adds a node with an explicit clock offset (nanoseconds, either sign) and rate
    /// error (parts per million, either sign, above -1,000,000).
    pub fn add_node_with_clock(&mut self, skew_nanos: i64, drift_ppm: i64) -> NodeId {
        assert!(drift_ppm > -1_000_000, "drift_ppm must be above -1,000,000");
        let mut st = self.shared.lock();
        // 1-based, as in moirae traces (see `NodeId`).
        let id = NodeId::new(u32::try_from(st.nodes.len() + 1).expect("too many nodes"));
        let protocol = st.node_stream(id, "protocol");
        let sched = st.node_stream(id, "sched");
        st.nodes.insert(
            id,
            state::Node {
                skew_nanos,
                drift_ppm,
                protocol,
                sched,
            },
        );
        st.fs.entry(id).or_insert_with(fs::NodeFs::new);
        id
    }

    /// An [`Environment`] handle for `node`. Any number may exist; they share the node,
    /// including its random streams.
    ///
    /// # Panics
    ///
    /// If `node` was not added to this simulation.
    #[must_use]
    pub fn env(&self, node: NodeId) -> SimEnv {
        let (rng, sched_rng) = {
            let st = self.shared.lock();
            let n = st
                .nodes
                .get(&node)
                .unwrap_or_else(|| panic!("unknown node {node}"));
            (n.protocol.clone(), n.sched.clone())
        };
        SimEnv {
            node,
            clock: SimClock {
                shared: self.shared.clone(),
                node,
            },
            fs: SimFs {
                shared: self.shared.clone(),
                node,
            },
            net: SimNet {
                shared: self.shared.clone(),
                node,
            },
            rng,
            sched_rng,
            shared: self.shared.clone(),
        }
    }

    /// Global virtual time.
    #[must_use]
    pub fn now(&self) -> Instant {
        self.shared.lock().now
    }

    /// Runs tasks and advances virtual time until nothing is due at or before
    /// `deadline`, then sets the time to `deadline`.
    pub fn run_until(&mut self, deadline: Instant) {
        loop {
            self.enqueue_wakes();
            let next = self.shared.lock().pick_runnable();
            if let Some(id) = next {
                self.poll(id);
                continue;
            }
            let (to, done) = match self.shared.lock().next_event_time() {
                Some(at) if at <= deadline => (at, false),
                _ => (deadline, true),
            };
            self.advance_to(to);
            if done {
                return;
            }
        }
    }

    /// Runs for `duration` of virtual time.
    pub fn run_for(&mut self, duration: Duration) {
        let deadline = self.now() + duration;
        self.run_until(deadline);
    }

    /// Blocks every link between the two groups, both directions.
    ///
    /// When the two groups are disjoint and together cover every node this is a
    /// symmetric partition and the trace records `PartitionStarted`, which moirae shows
    /// as a wall. Anything else, or a second partition while one is in force, is
    /// recorded as one `LinkBlocked` per direction, which moirae's group model cannot
    /// express (SPEC §1.5).
    pub fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        let mut st = self.shared.lock();
        let mut ga: Vec<NodeId> = a.to_vec();
        let mut gb: Vec<NodeId> = b.to_vec();
        ga.sort_unstable();
        ga.dedup();
        gb.sort_unstable();
        gb.dedup();
        let mut covered: Vec<NodeId> = ga.iter().chain(gb.iter()).copied().collect();
        covered.sort_unstable();
        let all: Vec<NodeId> = st.nodes.keys().copied().collect();
        let symmetric = covered == all
            && ga.iter().all(|x| !gb.contains(x))
            && st.fabric.active_partition.is_none();
        for &x in &ga {
            for &y in &gb {
                st.fabric.block(x, y);
                st.fabric.block(y, x);
            }
        }
        if symmetric {
            let groups = vec![ga, gb];
            st.fabric.active_partition = Some(groups.clone());
            st.record(None, TraceEvent::PartitionStarted { groups });
        } else {
            for &x in &ga {
                for &y in &gb {
                    for (from, to) in [(x, y), (y, x)] {
                        if st.fabric.links.insert((from, to)) {
                            st.record(Some(from), TraceEvent::LinkBlocked { from, to });
                        }
                    }
                }
            }
        }
    }

    /// Blocks one direction of one link. Recorded as `LinkBlocked`.
    pub fn block(&mut self, from: NodeId, to: NodeId) {
        let mut st = self.shared.lock();
        st.fabric.block(from, to);
        if st.fabric.links.insert((from, to)) {
            st.record(Some(from), TraceEvent::LinkBlocked { from, to });
        }
    }

    /// Removes every block: `PartitionHealed` for a symmetric partition in force, then
    /// one `LinkUnblocked` per individually blocked direction.
    pub fn heal(&mut self) {
        let mut st = self.shared.lock();
        st.fabric.heal();
        if let Some(groups) = st.fabric.active_partition.take() {
            st.record(None, TraceEvent::PartitionHealed { groups });
        }
        let links = std::mem::take(&mut st.fabric.links);
        for (from, to) in links {
            st.record(Some(from), TraceEvent::LinkUnblocked { from, to });
        }
    }

    /// Marks `node` as starting again after a crash: records `NodeRestarted`. Spawning
    /// the node's tasks on `sim.env(node)` afterwards is the restart itself; what the
    /// tasks find on disk is what survived the crash.
    pub fn restart(&mut self, node: NodeId) {
        let mut st = self.shared.lock();
        assert!(st.nodes.contains_key(&node), "unknown node {node}");
        st.record(Some(node), TraceEvent::NodeRestarted { node });
    }

    /// Kills every task on `node`, unbinds its sockets, and applies the §1.3 crash
    /// model to its disk. Call [`restart`](Self::restart) and spawn again to bring it back.
    pub fn crash(&mut self, node: NodeId) {
        let futures: Vec<BoxFuture> = {
            let mut st = self.shared.lock();
            let victims: Vec<_> = st
                .tasks
                .iter()
                .filter(|(_, t)| t.node == node)
                .map(|(id, _)| *id)
                .collect();
            let futures = victims
                .into_iter()
                .filter_map(|id| st.remove_task(id))
                .collect();
            st.fabric.remove_node_sockets(node);
            st.apply_crash_faults(node);
            st.record(Some(node), TraceEvent::NodeCrashed { node });
            futures
        };
        drop(futures);
    }

    /// What is durably on `node`'s disk at `path`, if the file exists.
    #[must_use]
    pub fn durable_contents(&self, node: NodeId, path: &std::path::Path) -> Option<Vec<u8>> {
        let st = self.shared.lock();
        st.fs.get(&node)?.durable_contents(path).map(<[u8]>::to_vec)
    }

    /// A copy of the trace so far.
    #[must_use]
    pub fn trace(&self) -> Vec<TraceRecord> {
        self.shared.lock().trace.clone()
    }

    /// Everything the moirae export needs, copied out from under the lock.
    pub(crate) fn snapshot(&self) -> Snapshot {
        let st = self.shared.lock();
        Snapshot {
            config: st.config.clone(),
            policy: st.policy,
            clocks: st
                .nodes
                .iter()
                .map(|(id, n)| (*id, n.skew_nanos, n.drift_ppm))
                .collect(),
            addrs: st.fabric.known.iter().map(|(a, n)| (*a, *n)).collect(),
            records: st.trace.clone(),
        }
    }

    fn enqueue_wakes(&self) {
        let ids = self.shared.take_wakes();
        if ids.is_empty() {
            return;
        }
        let mut st = self.shared.lock();
        for id in ids {
            st.make_runnable(id);
        }
    }

    fn poll(&self, id: TaskId) {
        let (mut future, waker) = {
            let mut st = self.shared.lock();
            let (instant, budget) = (st.instant, st.config.poll_budget);
            let Some(task) = st.tasks.get_mut(&id) else {
                return;
            };
            let Some(future) = task.future.take() else {
                return;
            };
            let (node, name, waker) = (task.node, task.name, task.waker.clone());
            if task.polls_at.0 != instant {
                task.polls_at = (instant, 0);
            }
            task.polls_at.1 += 1;
            let polls = task.polls_at.1;
            st.record(Some(node), TraceEvent::TaskPolled { task: id });
            if polls > budget {
                st.record(
                    Some(node),
                    TraceEvent::PollBudgetExceeded { task: id, polls },
                );
                let now = st.now;
                drop(st);
                panic!(
                    "{id} ({name}) on {node} was polled {polls} times at virtual time {now:?} without time \
                     advancing: busy loop (SimConfig::poll_budget = {budget})"
                );
            }
            (future, waker)
        };
        let outcome = future.as_mut().poll(&mut Context::from_waker(&waker));
        let to_drop = {
            let mut st = self.shared.lock();
            match outcome {
                Poll::Ready(()) => {
                    if let Some(task) = st.take_task(id) {
                        st.record(Some(task.node), TraceEvent::TaskCompleted { task: id });
                    }
                    Some(future)
                }
                Poll::Pending => match st.tasks.get_mut(&id) {
                    Some(task) if task.abort_requested => {
                        st.remove_task(id);
                        Some(future)
                    }
                    Some(task) => {
                        task.future = Some(future);
                        None
                    }
                    None => Some(future),
                },
            }
        };
        drop(to_drop);
    }

    fn advance_to(&self, to: Instant) {
        let mut wakers = Vec::new();
        self.shared.lock().advance_to(to, &mut wakers);
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Drop for Sim {
    /// Breaks the cycle between the shared state and the futures it owns, which hold
    /// environment handles pointing back at it.
    fn drop(&mut self) {
        let futures: Vec<BoxFuture> = {
            let mut st = self.shared.lock();
            let ids: Vec<_> = st.tasks.keys().copied().collect();
            ids.into_iter()
                .filter_map(|id| st.remove_task(id))
                .collect()
        };
        drop(futures);
    }
}

use crate::TaskId;

/// One node's [`Environment`] inside a [`Sim`]. Cheap to clone; clones share the node.
#[derive(Clone)]
pub struct SimEnv {
    shared: Arc<Shared>,
    node: NodeId,
    clock: SimClock,
    fs: SimFs,
    net: SimNet,
    rng: SimRng,
    sched_rng: SimRng,
}

impl fmt::Debug for SimEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimEnv")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl SimEnv {
    /// The node this environment belongs to.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.node
    }
}

impl Environment for SimEnv {
    type Clock = SimClock;
    type Fs = SimFs;
    type Net = SimNet;
    type Rng = SimRng;

    fn clock(&self) -> &SimClock {
        &self.clock
    }

    fn fs(&self) -> &SimFs {
        &self.fs
    }

    fn net(&self) -> &SimNet {
        &self.net
    }

    fn rng(&self) -> &SimRng {
        &self.rng
    }

    fn sched_rng(&self) -> &SimRng {
        &self.sched_rng
    }

    fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        name: &'static str,
        f: F,
    ) -> TaskHandle {
        let weak = Arc::downgrade(&self.shared);
        let id = self
            .shared
            .lock()
            .spawn(&weak, self.node, name, Box::pin(f));
        TaskHandle::new(id, name, Box::new(SimTask { shared: weak, id }))
    }

    fn trace(&self, event: TraceEvent) {
        self.shared.lock().record(Some(self.node), event);
    }
}

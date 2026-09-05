//! The Phase 0 scenario (SPEC.md §1.6): three nodes running the echo protocol from
//! `ananke_server::echo` under drops, delays, clock skew, partitions and a crash.
//!
//! The protocol itself lives in `ananke-server` so the binary and this scenario run
//! identical code. [`run`] drives the fault schedule and returns a [`Report`] whose
//! [`check`](Report::check) states the invariants the run must satisfy: the
//! protocol-level ones from [`Stats::check`] per node, plus trace-level ones about the
//! partitions and the crash. The tests in `tests/echo.rs` use it as a determinism
//! check and as a smoke test for the simulator.

use std::net::SocketAddr;
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{DropReason, Environment, Instant, NodeId, TraceEvent};
use ananke_server::echo::{self, Echo, SharedStats, Stats};

/// How many nodes the scenario runs.
pub const NODES: u32 = 3;
/// How often each node pings.
pub const PING_INTERVAL: Duration = Duration::from_millis(20);

/// The synthetic address of node `n`.
#[must_use]
pub fn node_addr(n: u32) -> SocketAddr {
    SocketAddr::from((
        [
            10,
            0,
            0,
            u8::try_from(n + 1).expect("node index fits an octet"),
        ],
        7000,
    ))
}

/// The node id (1-based, like the trace) bound to `addr`; `node_addr(n)` belongs to node `n + 1`.
fn node_of(addr: SocketAddr) -> Option<NodeId> {
    (0..NODES)
        .find(|&n| node_addr(n) == addr)
        .map(|n| NodeId::new(n + 1))
}

/// The fault schedule, in global virtual time. Each phase starts when the previous
/// one ends.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    /// All links up.
    pub warmup: Duration,
    /// Node 0 cut off from nodes 1 and 2, both directions.
    pub partition: Duration,
    /// All links up again.
    pub healed: Duration,
    /// Node 1 cannot reach node 2; node 2 can still reach node 1.
    pub one_way: Duration,
    /// Node 2 crashed and restarted immediately, all links up.
    pub after_restart: Duration,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            warmup: Duration::from_millis(300),
            partition: Duration::from_millis(300),
            healed: Duration::from_millis(300),
            one_way: Duration::from_millis(200),
            after_restart: Duration::from_millis(300),
        }
    }
}

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed the run used.
    pub seed: u64,
    /// The trace, one record per line, byte-identical across runs with the same seed.
    pub trace_text: String,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// Global time at which each phase ended.
    pub phase_ends: [Instant; 5],
    /// What each node observed, by node index.
    pub stats: Vec<Stats>,
}

impl Report {
    /// Every invariant the run must satisfy, or a description of the first violation.
    ///
    /// # Errors
    ///
    /// A message naming the seed and the violated invariant.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        let fail = |what: String| Err(format!("seed {seed}: {what}"));
        for (n, stats) in self.stats.iter().enumerate() {
            if let Err(violation) = stats.check() {
                return fail(format!("node {n}: {violation}"));
            }
        }

        let [t_warmup, t_partition, t_healed, t_one_way, t_restart] = self.phase_ends;
        let delivered = |from: NodeId, to: NodeId, after: Instant, until: Instant| {
            self.records
                .iter()
                .filter(|r| r.at > after && r.at <= until)
                .filter(|r| matches!(&r.event, TraceEvent::MessageDelivered { from: f, to: t, .. } if node_of(*f) == Some(from) && node_of(*t) == Some(to)))
                .count()
        };
        let dropped_partitioned = |from: NodeId, to: NodeId, after: Instant, until: Instant| {
            self.records
                .iter()
                .filter(|r| r.at > after && r.at <= until)
                .filter(|r| {
                    matches!(&r.event, TraceEvent::MessageDropped { from: f, to: t, reason: DropReason::Partitioned, .. }
                        if node_of(*f) == Some(from) && node_of(*t) == Some(to))
                })
                .count()
        };
        let (n0, n1, n2) = (NodeId::new(1), NodeId::new(2), NodeId::new(3));

        // Nothing crosses a symmetric partition, in either direction.
        for (a, b) in [(n0, n1), (n1, n0), (n0, n2), (n2, n0)] {
            let leaked = delivered(a, b, t_warmup, t_partition);
            if leaked > 0 {
                return fail(format!(
                    "{leaked} messages {a} -> {b} delivered during the partition"
                ));
            }
        }
        // The partitioned node still talks to nobody but is talked about: it keeps
        // pinging, and those pings are dropped with the right reason.
        let partition_drops = self
            .records
            .iter()
            .filter(|r| r.at > t_warmup && r.at <= t_partition)
            .filter(|r| {
                matches!(
                    r.event,
                    TraceEvent::MessageDropped {
                        reason: DropReason::Partitioned,
                        ..
                    }
                )
            })
            .count();
        if partition_drops == 0 {
            return fail("no MessageDropped(Partitioned) events during the partition".to_owned());
        }
        // Healing restores both directions: nothing is dropped as partitioned any more,
        // and traffic to and from node 0 flows again.
        for (a, b) in [(n0, n1), (n1, n0), (n0, n2), (n2, n0)] {
            let dropped = dropped_partitioned(a, b, t_partition, t_healed);
            if dropped > 0 {
                return fail(format!(
                    "{dropped} messages {a} -> {b} dropped as partitioned after healing"
                ));
            }
        }
        if delivered(n1, n0, t_partition, t_healed) + delivered(n2, n0, t_partition, t_healed) == 0
        {
            return fail("node 0 received nothing after the partition healed".to_owned());
        }
        if delivered(n0, n1, t_partition, t_healed) + delivered(n0, n2, t_partition, t_healed) == 0
        {
            return fail("node 0 reached nobody after the partition healed".to_owned());
        }
        // A one-way block is one-way: nothing crosses the blocked direction, and the
        // other direction is never treated as partitioned. Whether node 2 happens to
        // ping node 1 in the window is up to its seeded peer choice, so liveness of
        // that direction is asserted over the whole run instead.
        if delivered(n1, n2, t_healed, t_one_way) > 0 {
            return fail("node 1 reached node 2 through a one-way block".to_owned());
        }
        if dropped_partitioned(n2, n1, t_healed, t_one_way) > 0 {
            return fail(
                "node 2 -> node 1 was dropped as partitioned during a block of the other direction"
                    .to_owned(),
            );
        }
        if delivered(n2, n1, Instant::ZERO, t_restart) == 0 {
            return fail("node 2 never reached node 1".to_owned());
        }
        // The restarted node comes back, and the trace says so in order.
        let crashed = self
            .records
            .iter()
            .position(|r| r.event == TraceEvent::NodeCrashed { node: n2 });
        let restarted = self
            .records
            .iter()
            .position(|r| r.event == TraceEvent::NodeRestarted { node: n2 });
        match (crashed, restarted) {
            (Some(c), Some(r)) if c < r => {}
            _ => return fail("node 3 must crash, then restart, in that order".to_owned()),
        }
        if delivered(n0, n2, t_one_way, t_restart) + delivered(n1, n2, t_one_way, t_restart) == 0 {
            return fail("node 3 received nothing after restarting".to_owned());
        }
        // The symmetric partition and the one-way block are recorded as moirae will show them.
        let count =
            |f: &dyn Fn(&TraceEvent) -> bool| self.records.iter().filter(|r| f(&r.event)).count();
        let shape = (
            count(&|e| matches!(e, TraceEvent::PartitionStarted { .. })),
            count(&|e| matches!(e, TraceEvent::PartitionHealed { .. })),
            count(&|e| matches!(e, TraceEvent::LinkBlocked { .. })),
            count(&|e| matches!(e, TraceEvent::LinkUnblocked { .. })),
        );
        if shape != (1, 1, 1, 1) {
            return fail(format!(
                "expected one partition, one heal, one link block and one unblock; got {shape:?}"
            ));
        }
        // The fault model was actually exercised.
        for (name, wanted) in [
            (
                "Injected drop",
                self.records.iter().any(|r| {
                    matches!(
                        r.event,
                        TraceEvent::MessageDropped {
                            reason: DropReason::Injected,
                            ..
                        }
                    )
                }),
            ),
            (
                "TimeAdvanced",
                self.records
                    .iter()
                    .any(|r| matches!(r.event, TraceEvent::TimeAdvanced { .. })),
            ),
            (
                "TaskPolled",
                self.records
                    .iter()
                    .any(|r| matches!(r.event, TraceEvent::TaskPolled { .. })),
            ),
        ] {
            if !wanted {
                return fail(format!("trace has no {name} event"));
            }
        }
        Ok(())
    }

    /// Pings sent across the whole run.
    #[must_use]
    pub fn pings_sent(&self) -> u64 {
        self.stats.iter().map(|s| s.pings_sent).sum()
    }

    /// Pongs received across the whole run.
    #[must_use]
    pub fn pongs_received(&self) -> u64 {
        self.stats.iter().map(|s| s.pongs_received).sum()
    }
}

/// The simulator configuration the scenario uses for `seed`.
#[must_use]
pub fn config(seed: u64) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.1;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(10);
    config.clock.max_skew = Duration::from_millis(50);
    config.clock.max_drift_ppm = 500;
    config
}

/// Runs the scenario for `seed` with the default [`Schedule`].
#[must_use]
pub fn run(seed: u64) -> Report {
    run_with(seed, Schedule::default())
}

/// Runs the scenario for `seed` with an explicit fault schedule.
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule) -> Report {
    let mut sim = Sim::new(config(seed));
    let nodes: Vec<NodeId> = (0..NODES).map(|_| sim.add_node()).collect();
    let stats: Vec<SharedStats> = (0..NODES).map(|_| SharedStats::default()).collect();
    let spawn = |sim: &Sim, n: u32, incarnation: u32| {
        let env = sim.env(nodes[n as usize]);
        let config = Echo {
            listen: node_addr(n),
            peers: (0..NODES).filter(|&p| p != n).map(node_addr).collect(),
            interval: PING_INTERVAL,
            incarnation,
        };
        env.spawn(
            "echo",
            echo::node(env.clone(), config, stats[n as usize].clone()),
        );
    };
    for n in 0..NODES {
        spawn(&sim, n, 0);
    }
    let mut phase_ends = [Instant::ZERO; 5];

    sim.run_for(schedule.warmup);
    phase_ends[0] = sim.now();

    sim.partition(&[nodes[0]], &[nodes[1], nodes[2]]);
    sim.run_for(schedule.partition);
    phase_ends[1] = sim.now();

    sim.heal();
    sim.run_for(schedule.healed);
    phase_ends[2] = sim.now();

    sim.block(nodes[1], nodes[2]);
    sim.run_for(schedule.one_way);
    phase_ends[3] = sim.now();

    sim.heal();
    sim.crash(nodes[2]);
    sim.restart(nodes[2]);
    spawn(&sim, 2, 1);
    sim.run_for(schedule.after_restart);
    phase_ends[4] = sim.now();

    Report {
        seed,
        trace_text: sim.trace_text(),
        records: sim.trace(),
        phase_ends,
        stats: stats.iter().map(|s| echo::lock(s).clone()).collect(),
    }
}

//! The Phase 0 scenario (SPEC.md §1.6): three nodes running an echo protocol under
//! drops, delays, clock skew, partitions and a crash.
//!
//! Every node pings a random peer every [`PING_INTERVAL`] and answers every ping with
//! a pong carrying the same sequence number. The protocol is generic over
//! [`Environment`], so the same code will run on `RealEnv` when the echo binary is
//! wired. [`run`] drives the fault schedule and returns a [`Report`] whose
//! [`check`](Report::check) states the invariants the run must satisfy; the tests in
//! `tests/echo.rs` use it as a determinism check and as a smoke test for the simulator.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{
    Clock, DropReason, Either, Environment, Instant, Network, NodeId, Rng, Socket, TraceEvent, race,
};
use bytes::Bytes;

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

fn node_of(addr: SocketAddr) -> Option<NodeId> {
    (0..NODES).find(|&n| node_addr(n) == addr).map(NodeId::new)
}

/// What the nodes observed, kept outside the simulation so the harness can check it.
#[derive(Debug, Default)]
struct Stats {
    /// Pings sent and not yet answered, as (sender, peer, seq).
    outstanding: BTreeSet<(u32, u32, u64)>,
    pings_sent: u64,
    pongs_received: u64,
    /// Pongs that matched no outstanding ping: fabricated or duplicated.
    unknown_pongs: u64,
    /// Messages that did not parse.
    garbage: u64,
}

type SharedStats = Arc<Mutex<Stats>>;

fn lock_stats(shared: &SharedStats) -> std::sync::MutexGuard<'_, Stats> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

enum Message {
    Ping(u64),
    Pong(u64),
}

impl Message {
    fn encode(&self) -> Bytes {
        match self {
            Message::Ping(seq) => Bytes::from(format!("ping {seq}")),
            Message::Pong(seq) => Bytes::from(format!("pong {seq}")),
        }
    }

    fn decode(bytes: &[u8]) -> Option<Message> {
        let text = std::str::from_utf8(bytes).ok()?;
        let (kind, seq) = text.split_once(' ')?;
        let seq = seq.parse().ok()?;
        match kind {
            "ping" => Some(Message::Ping(seq)),
            "pong" => Some(Message::Pong(seq)),
            _ => None,
        }
    }
}

/// One node of the echo protocol. `incarnation` distinguishes sequence numbers across
/// restarts so a late pong for a pre-crash ping is still recognised.
async fn echo_node<E: Environment>(env: E, me: u32, incarnation: u32, stats: SharedStats) {
    let sock = match env.net().bind(node_addr(me)).await {
        Ok(sock) => sock,
        Err(_) => return,
    };
    let mut seq = u64::from(incarnation) << 32;
    let mut next_ping = env.clock().now();
    loop {
        let recv = pin!(sock.recv());
        let timer = pin!(env.clock().sleep_until(next_ping));
        match race(recv, timer).await {
            Either::Left(Err(_)) => return,
            Either::Left(Ok((from, msg))) => match Message::decode(&msg) {
                Some(Message::Ping(n)) => {
                    let _ = sock.send(from, Message::Pong(n).encode()).await;
                }
                Some(Message::Pong(n)) => {
                    let mut stats = lock_stats(&stats);
                    match node_of(from) {
                        Some(peer) if stats.outstanding.remove(&(me, peer.get(), n)) => {
                            stats.pongs_received += 1
                        }
                        _ => stats.unknown_pongs += 1,
                    }
                }
                None => lock_stats(&stats).garbage += 1,
            },
            Either::Right(()) => {
                // Pick one of the other nodes from this node's own seeded stream.
                let mut peer = u32::try_from(env.rng().below(u64::from(NODES) - 1)).expect("fits");
                if peer >= me {
                    peer += 1;
                }
                seq += 1;
                {
                    let mut stats = lock_stats(&stats);
                    stats.outstanding.insert((me, peer, seq));
                    stats.pings_sent += 1;
                }
                let _ = sock
                    .send(node_addr(peer), Message::Ping(seq).encode())
                    .await;
                next_ping = env.clock().now() + PING_INTERVAL;
            }
        }
    }
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
    pings_sent: u64,
    pongs_received: u64,
    unknown_pongs: u64,
    garbage: u64,
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
        if self.unknown_pongs > 0 {
            return fail(format!(
                "{} pongs matched no outstanding ping (fabricated or duplicated)",
                self.unknown_pongs
            ));
        }
        if self.garbage > 0 {
            return fail(format!("{} messages failed to parse", self.garbage));
        }
        if self.pongs_received == 0 || self.pongs_received > self.pings_sent {
            return fail(format!(
                "{} pongs for {} pings",
                self.pongs_received, self.pings_sent
            ));
        }

        let [t_warmup, t_partition, t_healed, t_one_way, t_restart] = self.phase_ends;
        let delivered = |from: NodeId, to: NodeId, after: Instant, until: Instant| {
            self.records
                .iter()
                .filter(|r| r.at > after && r.at <= until)
                .filter(|r| matches!(&r.event, TraceEvent::MessageDelivered { from: f, to: t, .. } if node_of(*f) == Some(from) && node_of(*t) == Some(to)))
                .count()
        };
        let (n0, n1, n2) = (NodeId::new(0), NodeId::new(1), NodeId::new(2));

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
        // Healing restores both directions.
        if delivered(n1, n0, t_partition, t_healed) + delivered(n2, n0, t_partition, t_healed) == 0
        {
            return fail("node 0 received nothing after the partition healed".to_owned());
        }
        if delivered(n0, n1, t_partition, t_healed) + delivered(n0, n2, t_partition, t_healed) == 0
        {
            return fail("node 0 reached nobody after the partition healed".to_owned());
        }
        // A one-way block is one-way.
        if delivered(n1, n2, t_healed, t_one_way) > 0 {
            return fail("node 1 reached node 2 through a one-way block".to_owned());
        }
        if delivered(n2, n1, t_healed, t_one_way) == 0 {
            return fail(
                "node 2 could not reach node 1 during a block of the other direction".to_owned(),
            );
        }
        // The restarted node comes back.
        if !self
            .records
            .iter()
            .any(|r| r.event == TraceEvent::NodeCrashed { node: n2 })
        {
            return fail("no NodeCrashed event for node 2".to_owned());
        }
        if delivered(n0, n2, t_one_way, t_restart) + delivered(n1, n2, t_one_way, t_restart) == 0 {
            return fail("node 2 received nothing after restarting".to_owned());
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
        self.pings_sent
    }

    /// Pongs received across the whole run.
    #[must_use]
    pub fn pongs_received(&self) -> u64 {
        self.pongs_received
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
    let stats: SharedStats = Arc::default();
    let spawn = |sim: &Sim, n: u32, incarnation: u32| {
        let env = sim.env(nodes[n as usize]);
        env.spawn(
            "echo",
            echo_node(env.clone(), n, incarnation, stats.clone()),
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
    spawn(&sim, 2, 1);
    sim.run_for(schedule.after_restart);
    phase_ends[4] = sim.now();

    let stats = lock_stats(&stats);
    Report {
        seed,
        trace_text: sim.trace_text(),
        records: sim.trace(),
        phase_ends,
        pings_sent: stats.pings_sent,
        pongs_received: stats.pongs_received,
        unknown_pongs: stats.unknown_pongs,
        garbage: stats.garbage,
    }
}

//! The Phase 0 scenario (SPEC.md §1.6): three nodes running the echo protocol from
//! `ananke_server::echo` under drops, delays, clock skew, partitions and a crash. Each
//! node keeps the echo [`Journal`], so the crash also exercises the SPEC §1.3 disk
//! model: torn writes, bit rot and directory-entry loss, all cross-checked against
//! what the restarted node found on disk. The journal runs in two [`Variant`]s, the
//! correct one and one with a known bug, and [`Report::check`] expects different
//! things of each: that pair is what shows the fault model telling them apart. The
//! run has a poll budget, so a busy loop fails it instead of hanging a sweep.
//!
//! The protocol itself lives in `ananke-server` so the binary and this scenario run
//! identical code. [`run`] drives the fault schedule and returns a [`Report`] whose
//! [`check`](Report::check) states the invariants the run must satisfy: the
//! protocol-level ones from [`Stats::check`] per node, plus trace-level ones about the
//! partitions and the crash. The tests in `tests/echo.rs` use it as a determinism
//! check and as a smoke test for the simulator.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ananke_env::moirae::{Export, bytes_decoder};
use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{DirEntryOp, DropReason, Environment, Instant, NodeId, TraceEvent};
use ananke_server::echo::{self, Echo, Journal, Message, SharedStats, Stats};
use moirae_trace::Json;

/// How many nodes the scenario runs.
pub const NODES: u32 = 3;
/// How often each node pings.
pub const PING_INTERVAL: Duration = Duration::from_millis(20);
/// Where each node keeps its journal, on its own disk.
pub const JOURNAL_DIR: &str = "/echo";

/// Which journal the nodes run: the standard pair for every fault-model test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    /// `sync_dir` after every rename and create, as `ananke-server` ships. The
    /// positive control: the journal must always be found after the crash.
    Correct,
    /// No `sync_dir` on rotation. The negative control: the sweep must see the
    /// journal vanish, or the fault model is not doing its job.
    NoSyncDir,
}

/// The journal every node keeps: synced every 4 records, rotated every 16, so a crash
/// finds pending writes and, in the buggy variant, unsynced renames.
#[must_use]
pub fn journal(variant: Variant) -> Journal {
    Journal {
        dir: PathBuf::from(JOURNAL_DIR),
        sync_every: 4,
        rotate_every: 16,
        sync_dir_on_rotate: variant == Variant::Correct,
    }
}

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

impl Schedule {
    /// The whole run's virtual duration.
    #[must_use]
    pub fn total(&self) -> Duration {
        self.warmup + self.partition + self.healed + self.one_way + self.after_restart
    }
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
    /// Which journal the nodes ran.
    pub variant: Variant,
    /// The trace as moirae JSONL (SPEC §1.5), byte-identical across runs with the same
    /// seed; `sim/tests/echo.rs` pins its hash.
    pub jsonl: String,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// Global time at which each phase ended.
    pub phase_ends: [Instant; 5],
    /// What each node observed, by node index.
    pub stats: Vec<Stats>,
    /// What each node had observed when node 2 crashed, by node index.
    pub stats_at_crash: Vec<Stats>,
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
        if let Err(violation) = self.check_journals() {
            return fail(violation);
        }
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

    /// The disk after the crash is exactly what the §1.3 model says survived, and the
    /// journal's checksums caught every flipped bit the restarted node could see. What
    /// "survived" means depends on the [`Variant`]: the correct journal never loses a
    /// directory entry; the buggy one loses exactly what the model dropped.
    fn check_journals(&self) -> Result<(), String> {
        let count =
            |f: &dyn Fn(&TraceEvent) -> bool| self.records.iter().filter(|r| f(&r.event)).count();
        let dir = Path::new(JOURNAL_DIR);
        let in_dir = |path: &Path| path.parent() == Some(dir);
        // Fresh disks at the start: nothing to find, nothing torn, nothing corrupt.
        for (n, stats) in self.stats_at_crash.iter().enumerate() {
            let Some(journal) = &stats.journal else {
                return Err(format!("node {n} kept no journal"));
            };
            if journal.found
                || journal.found_previous
                || journal.valid + journal.corrupt + journal.torn > 0
            {
                return Err(format!(
                    "node {n} found a journal on a fresh disk: {journal:?}"
                ));
            }
            if journal.written == 0 || journal.written != stats.pings_sent {
                return Err(format!(
                    "node {n} journalled {} of {} pings",
                    journal.written, stats.pings_sent
                ));
            }
        }
        // Nodes that never crashed keep writing to the same file.
        for n in [0, 1] {
            let (before, after) = (&self.stats_at_crash[n], &self.stats[n]);
            if after
                .journal
                .as_ref()
                .map(|j| (j.found, j.valid, j.corrupt, j.torn))
                != Some((false, 0, 0, 0))
            {
                return Err(format!(
                    "node {n} never crashed but its journal changed: {:?}",
                    after.journal
                ));
            }
            if after.journal.as_ref().map_or(0, |j| j.written)
                <= before.journal.as_ref().map_or(0, |j| j.written)
            {
                return Err(format!("node {n} stopped journalling after node 2 crashed"));
            }
        }
        // The crashed node: the directory model first.
        let before = self.stats_at_crash[2]
            .journal
            .as_ref()
            .expect("checked above");
        let after = self.stats[2].journal.as_ref().expect("checked above");
        let lost: Vec<(PathBuf, DirEntryOp)> = self
            .records
            .iter()
            .filter_map(|r| match &r.event {
                TraceEvent::DirectoryEntryLost { dir: d, entry, op } if d == dir => {
                    Some((entry.clone(), *op))
                }
                _ => None,
            })
            .collect();
        match self.variant {
            Variant::Correct => {
                // Every rename and create was synced: nothing was pending at the crash,
                // so nothing was lost, the journal is there, and journal.prev is there
                // whenever a rotation completed before the crash.
                if !lost.is_empty() {
                    return Err(format!(
                        "the correct journal lost directory entries: {lost:?}"
                    ));
                }
                if !after.found {
                    return Err("the correct journal was missing after the restart".to_owned());
                }
                if after.found_previous != (before.rotations > 0) {
                    return Err(format!(
                        "journal.prev found={} after restart with {} rotations before the crash",
                        after.found_previous, before.rotations
                    ));
                }
            }
            Variant::NoSyncDir => {
                // After the one synced create, every rotation appended
                // `rename(journal, journal.prev)` then `create(journal)` to the
                // directory's pending operations, and the crash kept a prefix of them.
                let pending_at_crash = 2 * before.rotations;
                if lost.len() as u64 > pending_at_crash {
                    return Err(format!(
                        "{} directory operations lost but only {pending_at_crash} were pending",
                        lost.len()
                    ));
                }
                // `journal` is missing exactly when the first dropped operation was its
                // creation.
                let journal_missing =
                    lost.first() == Some(&(dir.join(Journal::CURRENT), DirEntryOp::Link));
                if after.found == journal_missing {
                    return Err(format!(
                        "journal found={} after restart, but the crash dropped {lost:?}",
                        after.found
                    ));
                }
                // `journal.prev` exists exactly when at least one rename survived.
                let previous_kept = before.rotations > 0 && (lost.len() as u64) < pending_at_crash;
                if after.found_previous != previous_kept {
                    return Err(format!(
                        "journal.prev found={} after restart, but {} rotations happened and the crash dropped {lost:?}",
                        after.found_previous, before.rotations
                    ));
                }
            }
        }
        // Then the data model, the same for both variants. Every rotted block flips
        // one bit of one record, and the checksum catches every single-bit flip; every
        // torn write leaves one partial tail. The node sees only the files still
        // linked, so the counts are bounds.
        let rotted = count(&|e| matches!(e, TraceEvent::BlockRotted { path, .. } if in_dir(path)));
        let torn = count(&|e| matches!(e, TraceEvent::WriteTorn { path, .. } if in_dir(path)));
        if after.corrupt as usize > rotted {
            return Err(format!(
                "{} corrupt records but only {rotted} rotted blocks",
                after.corrupt
            ));
        }
        if after.torn as usize > torn {
            return Err(format!(
                "{} torn files but only {torn} torn writes",
                after.torn
            ));
        }
        if after.valid + after.corrupt + after.torn > before.written {
            return Err(format!(
                "replayed {} + {} + {} records but only {} were written before the crash",
                after.valid, after.corrupt, after.torn, before.written
            ));
        }
        if !after.found && !after.found_previous && after.valid + after.corrupt > 0 {
            return Err("replayed records from files that were not found".to_owned());
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

/// Decodes echo payloads for the studio: `{"type":"ping","seq":N}` and the same for
/// `pong`; anything else falls back to [`bytes_decoder`].
#[must_use]
pub fn decoder(payload: &[u8]) -> Json {
    match Message::decode(payload) {
        Some(Message::Ping(seq)) => Json::obj(vec![
            ("type", Json::str("ping")),
            ("seq", Json::Int(i64::try_from(seq).unwrap_or(i64::MAX))),
        ]),
        Some(Message::Pong(seq)) => Json::obj(vec![
            ("type", Json::str("pong")),
            ("seq", Json::Int(i64::try_from(seq).unwrap_or(i64::MAX))),
        ]),
        None => bytes_decoder(payload),
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
    // One block in four rots at a crash; the journal's checksums must notice.
    config.fs.p_bitrot = 0.25;
    // Nothing in this scenario is polled more than a few times at one instant; a task
    // that is has stopped yielding to time.
    config.poll_budget = 1_000;
    config
}

/// Runs the scenario for `seed` with the default [`Schedule`].
#[must_use]
pub fn run(seed: u64, variant: Variant) -> Report {
    run_with(seed, Schedule::default(), variant)
}

/// Runs the scenario for `seed` with an explicit fault schedule.
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule, variant: Variant) -> Report {
    let mut config = config(seed);
    config.run_length_hint = SimConfig::run_length_hint_for(NODES, schedule.total());
    let mut sim = Sim::new(config);
    let nodes: Vec<NodeId> = (0..NODES).map(|_| sim.add_node()).collect();
    let stats: Vec<SharedStats> = (0..NODES).map(|_| SharedStats::default()).collect();
    let spawn = |sim: &Sim, n: u32, incarnation: u32| {
        let env = sim.env(nodes[n as usize]);
        let config = Echo {
            listen: node_addr(n),
            peers: (0..NODES).filter(|&p| p != n).map(node_addr).collect(),
            interval: PING_INTERVAL,
            incarnation,
            journal: Some(journal(variant)),
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
    let stats_at_crash = stats.iter().map(|s| echo::lock(s).clone()).collect();
    sim.crash(nodes[2]);
    sim.restart(nodes[2]);
    spawn(&sim, 2, 1);
    sim.run_for(schedule.after_restart);
    phase_ends[4] = sim.now();

    Report {
        seed,
        variant,
        jsonl: sim
            .to_moirae(&Export::new(&decoder))
            .expect("the echo trace exports to moirae v2"),
        records: sim.trace(),
        phase_ends,
        stats: stats.iter().map(|s| echo::lock(s).clone()).collect(),
        stats_at_crash,
    }
}

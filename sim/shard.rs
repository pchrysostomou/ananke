//! `sim/shard.rs`, the sharded sweep (SHARD.md §10; §12, Stage C): the correct system
//! on five nodes, three of them the bootstrap nodes, with its ranges made by the splits
//! its driver draws.
//!
//! **The cluster.** Nodes 1 to 3 are the bootstrap nodes (SHARD.md §2; Q7, Q9, Q32):
//! each writes the initial state from configuration in one synced batch — ranges 0, 1
//! and 2, range 2 over the whole user keyspace the clients write, `[k, l)` — and every
//! range is on those three. Nodes 4 and 5 join and hold nothing: no leader of this stage
//! sends them anything, so they never hold even a placeholder (Q22), and the sweep
//! asserts that on every seed. They are there for Stage D's moves, and until then they
//! answer every request with a `RangeMismatch` that names nothing (§3), which a client
//! that tries them takes as a reason to look its key up again.
//!
//! **The driver** (SHARD.md §10; Q18), on stream `shard`, draws its actions before the
//! run starts and resolves each against the trace when it fires, so the stream's
//! consumption never depends on what the run did (D-031). Each action is either:
//!
//! - a **split of a current span**: a user range whose latest descriptor holds at least
//!   [`MIN_KEYS`] of the clients' keys on each side of some key strictly inside it,
//!   split at one such key, asked of the range's leader by an admin that follows
//!   `NotLeader` round the bootstrap nodes. So a range splits again after it split, and
//!   a left half and a right half both split, which the node sweep's split arms never
//!   draw (D-104);
//! - a **leadership transfer** (`Command::Transfer`, thesis §3.10) of any range, the two
//!   system ranges included, to one of its other voters.
//!
//! **The workload's shapes** (SHARD.md §10, §12's Stage C; PROPOSED D-131), each aimed at the
//! window one of §10's variants breaks and each drawn from the seed, so a shape is a schedule
//! the sweep can take. Stream `shard-load` gives each of the driver's splits a [`Load`]: the
//! clients' traffic alone (§10's "any split with load"), a burst of right-half writes pending
//! on the parent's leader around the split, a burst of right-half gets waiting on that
//! leader's read-index round around it, or a client whose cache still names the parent reading
//! the right half through it while another client writes that key through the right half.
//! Stream `shard-restart` adds one [`Fault::SplitFromRestarted`]: a node that has taken an id
//! from its block is crashed and restarted, handed the lead of a range and asked to split it.
//! One seed in [`PLAIN_ONE_IN`] draws none of it ([`Schedule::plain`]), which is the schedule
//! the sweep was before them and the schedule every pinned seed was found on.
//!
//! **The clients**: [`CLIENTS`] of them over [`KEYS`] keys, `k00` to `k15`, so a range
//! keeps at least [`MIN_KEYS`] of them however the driver splits. They are the node
//! sweep's client (`raft::client_over`), routing by a cache merged from `RangeMismatch`
//! and lookups through ranges 0 and 1 (§3), and starting, by thirds, stale, knowing the
//! root alone, or knowing the bootstrap descriptor. Every one of them is stale after the
//! first split.
//!
//! **The faults** are Phase 2's (SPEC.md §3; RAFT.md §2): drops, duplicates and delays
//! on the network, every node's clock skewed and drifting — within the lease's bound on
//! half the seeds, one bootstrap node slow and the others fast on the rest, as the raft
//! sweep draws them — partitions of one node with a client beside it, the leader of a
//! drawn range cut off, one-way blocks, crashes and leader crashes, each healed before
//! the next; and the disk's bit rot and torn writes under crashes, with `fsync`
//! honoured. A rotted table refuses its node's whole engine and re-seeds every replica
//! on it (Q15, D-077), which the sweep counts by the loss that made it (#45). The
//! directed arms of the raft sweep — the lease trials, Figure 8, the crash storms —
//! are not drawn here: they are the node sweep's (`sim/raft.rs`, D-082), which re-asserts
//! Phase 2's variants on the node.
//!
//! **The checks** ([`Report::check`]): the raft sweep's, over this run's trace
//! (`raft::Report::over_a_run`) — checks 1 to 4 keyed by range; the range layer's folds,
//! checks 7, 8, 9, 10, 16, 17, 18 and 20, in one incremental checker that stops the run
//! at its first violation and again over the whole trace (D-046); linearizability of
//! every key across every split (§9), which with the drifting clocks is check 6, lease
//! safety under drift, as Phase 2 built it; pre-vote's property per (range, server);
//! and, on uniformly scheduled seeds, the timer check and the write bound, per key and
//! per range the run ended with (`raft::Report::liveness_by_final_range`), where a range
//! whose replicas on refused nodes leave it no majority is asked nothing (§8) — then this
//! sweep's own: the meta convergence bound (§8), less the right halves the owner's ruling of
//! 2026-11-09 excuses from it and from nothing else (#220), the shape (nodes 4 and 5 hold nothing;
//! every bootstrap node created ranges 0, 1 and 2), and D-092's per-seed assertion wherever
//! a split was refused for want of a range id. Check 19 is S7's, which waits on D-093.
// PROPOSED(D-113): `sim/shard.rs`, Stage C's sharded sweep.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{
    ClientOp, Clock, Either, Environment, Instant, MismatchAt, Network, NodeId, RangeCause,
    RangeState, RecoveredAs, Socket, TraceEvent, race,
};
use ananke_raft::ServerId;
use ananke_raft::apply::{Command, Outcome, SplitRefusal, user_key};
use ananke_raft::client::{Reply, Request};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::store::KeyPrefix;
use ananke_shard::client::{Cache, RangedRequest, RangedResponse};
use ananke_shard::descriptor::{FIRST_GENERATION, RangeDescriptor};
use ananke_shard::range::RangeId;
use ananke_shard::range_variant::RangeVariants;
use ananke_shard::server::{Range, ServerConfig};
use ananke_shard::variant::NodeVariants;
use ananke_storage::EngineConfig;
use bytes::Bytes;

use crate::lin::History;
use crate::raft::{
    self, ClientKeys, ClientStats, DRIFT_BOUND_PPM, LIVENESS_TIMEOUTS, SLICE, SharedStats, TICK,
    admin_addr, burst_addr, election_max, server_addr,
};
use crate::ranges::{DIR, FIRST_RANGE, ID_BLOCK, INBOX_BYTES, META_RANGE, REFILL_AT, ROOT_RANGE};

/// The nodes: three bootstrap nodes and two that join and hold nothing (SHARD.md §2,
/// §10; Q39).
pub const NODES: u64 = 5;
/// The bootstrap nodes, `1..=BOOTSTRAP`: every range's voters in this stage.
pub const BOOTSTRAP: u64 = 3;
/// The clients.
pub const CLIENTS: u64 = 4;
/// The keys the clients draw from, `k00` to `k15`.
pub const KEYS: u64 = 16;
/// The fewest of the clients' keys a split leaves on either side: several keys per
/// range however the driver splits (SHARD.md §9's "splits need several keys per range").
pub const MIN_KEYS: u64 = 2;
/// The first key of the user range the bootstrap writes, and the key past its last:
/// every key the clients write is `k` and two digits, so `[k, l)` holds them all.
pub const SPAN: (&str, &str) = ("k", "l");
/// The cores' snapshot threshold: the node sweep's (`raft::NODE_SNAPSHOT_THRESHOLD`), so
/// followers compact and a follower cut off long enough is fed a snapshot, a split half's
/// included.
pub const SNAPSHOT_THRESHOLD: u64 = raft::NODE_SNAPSHOT_THRESHOLD;
/// The node's cap on snapshot streams received at once (Q14, D-075): at or above the
/// most replicas a node holds here, the two system ranges and at most
/// `KEYS / MIN_KEYS` user ranges, so no stream waits by accident.
pub const SNAPSHOT_CAP: usize = (2 + KEYS / MIN_KEYS) as usize;
/// How likely a disk block is to rot: Phase 2's figure, the raft sweep's
/// (`raft::config`).
pub const BITROT: f64 = 0.02;
/// The driver's admins' client process id in requests, one sequence number per action.
const DRIVER: u64 = 96 << 32;
/// How long a split's admin waits for each answer before it tries the next node.
const SPLIT_WAIT: Duration = Duration::from_millis(600);
/// How long the split's admin waits before asking again after `NoRangeId` (PROPOSED
/// D-092's A: the asker retries): the node sweep's `SPLIT_RETRY`.
const SPLIT_RETRY: Duration = Duration::from_millis(150);
/// How long a transfer's admin waits for its answer.
const TRANSFER_WAIT: Duration = Duration::from_millis(300);
/// How long the run waits after the last fault for the driver's admins still asking.
const DRIVER_DRAIN: Duration = Duration::from_secs(5);
/// One seed in this many draws no shape at all and runs [`Schedule::plain`]: the sweep as it
/// was before the shapes, kept among the schedules it can take (PROPOSED D-131).
pub const PLAIN_ONE_IN: u64 = 8;
/// The process the shapes' own clients trace under, beside the sweep's clients: a split's
/// burst as `LOAD | j`, the stale-cache shape's writer as `LOAD | WRITER | j` and its reader as
/// `LOAD | READER | j`, where `j` is the action's number from 1 (PROPOSED D-131).
const LOAD: u64 = 95 << 32;
/// See [`LOAD`].
const WRITER: u64 = 1 << 16;
/// See [`LOAD`].
const READER: u64 = 2 << 16;
/// How long a shape's client waits for its answers past its last send.
const LOAD_WAIT: Duration = Duration::from_millis(400);
/// How long the stale-cache shape's clients wait for each answer.
const STALE_WAIT: Duration = Duration::from_millis(150);
/// The admin numbers a [`Fault::SplitFromRestarted`] takes: `RESTART_ADMIN + 2i` for its
/// transfer and one more for its split, past every action's number.
const RESTART_ADMIN: u64 = 100;
/// The admin number [`Load::StaleCache`]'s transfer takes, `STALE_ADMIN + j` for action `j`.
const STALE_ADMIN: u64 = 200;
/// What a split's asker adds to its request's number when it asks anew, past every admin
/// number (PROPOSED D-131).
const ASK_ANEW: u64 = 1 << 16;
/// The meta convergence bound (SHARD.md §8), in maximum election timeouts; measured on the
/// correct system before it is asserted (Q39; D-113 §6).
pub const META_CONVERGENCE_TIMEOUTS: u32 = 10;

/// The meta convergence bound: how long past the last heal, or past a descriptor's first
/// statement if that is later, meta may take to name a live range's descriptor.
#[must_use]
pub fn meta_convergence_bound() -> Duration {
    election_max() * META_CONVERGENCE_TIMEOUTS
}

/// One fault of the schedule. Every fault heals before the next starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// `node` and client `client` on one side, everyone else on the other.
    Isolate {
        /// The node cut off.
        node: u64,
        /// The client on its side (1-based).
        client: u64,
        /// For how long.
        for_: Duration,
    },
    /// The leader of the range the arm's pick names, cut off with client 1.
    IsolateLeader {
        /// For how long.
        for_: Duration,
    },
    /// One direction of one link between nodes blocked.
    OneWay {
        /// Messages from this node...
        from: u64,
        /// ...to this one are dropped.
        to: u64,
        /// For how long.
        for_: Duration,
    },
    /// A node crashes and restarts after `down`.
    Crash {
        /// Which.
        node: u64,
        /// How long it stays down.
        down: Duration,
    },
    /// The leader of the range the arm's pick names crashes, and restarts after `down`.
    CrashLeader {
        /// How long it stays down.
        down: Duration,
    },
    /// §10's shape for `IdBlockResumed` (SHARD.md §5, Q17; PROPOSED D-131): a node some split has
    /// taken an id from, picked among them when the arm fires, crashes and restarts after
    /// `down`. Then `transfer_after` later it is handed the lead of a user range that can still
    /// split, and `split_after` after that it is asked to split it. A correct node's new run
    /// waits for a block of its own (§5's second rule); the variant's resumes its record's and
    /// takes the same ids again. Where no split has taken an id yet the arm does nothing: no
    /// crash, no transfer and no split.
    // PROPOSED(D-131): §12, Stage C's fourth shape.
    SplitFromRestarted {
        /// How long the node stays down.
        down: Duration,
        /// From the restart to the transfer.
        transfer_after: Duration,
        /// From the transfer to the split.
        split_after: Duration,
    },
}

impl Fault {
    /// How long the fault lasts.
    #[must_use]
    pub fn length(&self) -> Duration {
        match self {
            Self::Isolate { for_, .. }
            | Self::IsolateLeader { for_ }
            | Self::OneWay { for_, .. } => *for_,
            Self::Crash { down, .. }
            | Self::CrashLeader { down }
            | Self::SplitFromRestarted { down, .. } => *down,
        }
    }
}

/// What a driver's action does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionKind {
    /// A split of a current span.
    Split,
    /// A leadership transfer of a range to another of its voters.
    Transfer,
}

/// The load a driver's split is drawn under (SHARD.md §10, "the workload itself"; PROPOSED
/// D-131): which of §10's shapes it is. A transfer's is always [`Load::Clients`].
// PROPOSED(D-131): §12, Stage C's shapes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Load {
    /// The clients' traffic and nothing beside it: §10's "any split with load".
    Clients,
    /// A burst of `count` writes of right-half keys at the parent's leader, one every `every`,
    /// answered as they come, and the split asked after the first `lead` of them: the writes
    /// proposed after the split and before its apply are what §3's apply check refuses
    /// (`ApplyIgnoresSpan`).
    WriteBurst {
        /// How many writes.
        count: u64,
        /// The time between two sends.
        every: Duration,
        /// How many go before the split is asked.
        lead: u64,
    },
    /// A burst of `count` gets of right-half keys at the parent's leader, as
    /// [`Load::WriteBurst`]: a get let through at receipt and still waiting on its read-index
    /// round when the split applies is what §3's check at serving refuses
    /// (`ReadCheckAtReceiptOnly`).
    ReadBurst {
        /// How many gets.
        count: u64,
        /// The time between two sends.
        every: Duration,
        /// How many go before the split is asked.
        lead: u64,
    },
    /// The split, then `transfer_after` later the right half's lead moved off the parent's
    /// leader's node, and `ask_after` after that one client writes a right-half key through
    /// the right half and another, whose cache still names the parent, reads that key through
    /// the parent (`TrustStaleDescriptor`).
    StaleCache {
        /// From the split to the transfer.
        transfer_after: Duration,
        /// From the transfer to the two clients.
        ask_after: Duration,
    },
}

/// One action of the driver, drawn before the run and resolved when it fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Action {
    /// When it fires, after the faults begin.
    pub at: Duration,
    /// What it does.
    pub kind: ActionKind,
    /// The draw that picks its range among the candidates at the firing.
    pub range: u64,
    /// The draw that picks a split's key, or a transfer's target.
    pub pick: u64,
    /// What a split is drawn under, from stream `shard-load`.
    pub load: Load,
}

/// The faults and the driver's actions one seed runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// All links up, the ranges electing and the clients starting.
    pub warmup: Duration,
    /// The faults, each healed before the next, with `gaps[i]` of quiet after it.
    pub faults: Vec<Fault>,
    /// The quiet after each fault.
    pub gaps: Vec<Duration>,
    /// Per fault, the draw that picks the range a leader-relative arm aims at among the
    /// ranges when it fires, from a stream of its own (§11, env 8).
    pub picks: Vec<u64>,
    /// The driver's actions, in time order, from stream `shard`.
    pub actions: Vec<Action>,
    /// The quiet after the last fault and the driver's last admin: the liveness window.
    pub settle: Duration,
    /// Each node's clock rate error in parts per million, by node index.
    pub drifts: Vec<i64>,
    /// Each node's clock offset in nanoseconds, by node index.
    pub skews: Vec<i64>,
    /// How long after the liveness window opens the clients stay cut off from every node,
    /// the window's start not moved and its length not changed: what a cluster nothing can
    /// write to for that long looks like to the write bound read from the heal (PROPOSED
    /// D-113 §5). `None` in every schedule [`Schedule::draw`] draws, so no run of the sweep
    /// has one; a test sets it.
    pub clients_held_off: Option<Duration>,
}

impl Schedule {
    /// The schedule the sweep runs for `seed`: [`Schedule::plain`] and, on all but one seed in
    /// [`PLAIN_ONE_IN`], the shapes of SHARD.md §10 and §12's Stage C over it. Each from a
    /// stream of its own (D-031), so every draw of the plain schedule falls where it fell:
    /// stream `shard-load` gives each of the driver's splits a [`Load`], one each of the three
    /// shaped loads and the clients' traffic alone by quarters, and stream `shard-restart` adds
    /// one [`Fault::SplitFromRestarted`] in the second half of the faults, where a split has had
    /// its chance to take an id. The driver's actions keep the times the plain schedule gave
    /// them, against faults the arm may have moved later.
    // PROPOSED(D-131): §12, Stage C's shapes.
    #[must_use]
    pub fn draw(seed: u64) -> Self {
        let ms = |rng: &mut moirae_sched::Pcg32, lo: u64, hi: u64| {
            Duration::from_millis(lo + rng.below(hi - lo + 1))
        };
        let mut schedule = Self::plain(seed);
        let mut load = moirae_sched::stream(seed, "shard-load");
        if load.below(PLAIN_ONE_IN) == 0 {
            return schedule;
        }
        for action in &mut schedule.actions {
            if action.kind != ActionKind::Split {
                continue;
            }
            let count = 6 + load.below(9);
            let every = Duration::from_millis(1 + load.below(3));
            let lead = load.below(count / 2 + 1);
            action.load = match load.below(4) {
                0 => Load::Clients,
                1 => Load::WriteBurst { count, every, lead },
                2 => Load::ReadBurst { count, every, lead },
                _ => Load::StaleCache {
                    transfer_after: ms(&mut load, 120, 240),
                    ask_after: ms(&mut load, 80, 160),
                },
            };
        }
        let mut restart = moirae_sched::stream(seed, "shard-restart");
        let half = schedule.faults.len() / 2;
        let at = half
            + usize::try_from(restart.below((schedule.faults.len() - half + 1) as u64))
                .expect("small");
        let down = ms(&mut restart, 50, 400);
        let transfer_after = ms(&mut restart, 150, 300);
        let split_after = ms(&mut restart, 150, 300);
        let quiet = ms(&mut restart, 450, 700);
        schedule.faults.insert(
            at,
            Fault::SplitFromRestarted {
                down,
                transfer_after,
                split_after,
            },
        );
        schedule
            .gaps
            .insert(at, transfer_after + split_after + quiet);
        schedule.picks.insert(at, restart.below(1 << 16));
        schedule
    }

    /// The schedule with no shape on it: four to seven faults from stream
    /// `shard-faults`, each followed by a quiet of 450 to 700 ms; the arms' range picks
    /// from `shard-arm-range`; the clocks from `shard-clocks`; and four to eight driver
    /// actions from `shard`, at times spread over the faults, two in three a split, each under
    /// [`Load::Clients`]. The sweep runs it on one seed in [`PLAIN_ONE_IN`], and every pinned
    /// seed runs it, being the schedule it was found on (PROPOSED D-131).
    #[must_use]
    pub fn plain(seed: u64) -> Self {
        let ms = |rng: &mut moirae_sched::Pcg32, lo: u64, hi: u64| {
            Duration::from_millis(lo + rng.below(hi - lo + 1))
        };
        let mut rng = moirae_sched::stream(seed, "shard-faults");
        let count = 4 + rng.below(4);
        let mut faults = Vec::new();
        let mut gaps = Vec::new();
        for _ in 0..count {
            let fault = match rng.below(5) {
                0 => Fault::Isolate {
                    node: 1 + rng.below(NODES),
                    client: 1 + rng.below(CLIENTS),
                    for_: ms(&mut rng, 300, 900),
                },
                1 => Fault::IsolateLeader {
                    for_: ms(&mut rng, 300, 900),
                },
                2 => {
                    let from = 1 + rng.below(NODES);
                    let to = 1 + (from - 1 + 1 + rng.below(NODES - 1)) % NODES;
                    Fault::OneWay {
                        from,
                        to,
                        for_: ms(&mut rng, 300, 900),
                    }
                }
                3 => Fault::Crash {
                    node: 1 + rng.below(NODES),
                    down: ms(&mut rng, 50, 400),
                },
                _ => Fault::CrashLeader {
                    down: ms(&mut rng, 50, 400),
                },
            };
            faults.push(fault);
            gaps.push(ms(&mut rng, 450, 700));
        }
        let mut arm = moirae_sched::stream(seed, "shard-arm-range");
        let picks = faults.iter().map(|_| arm.below(1 << 16)).collect();
        // The driver's actions, spread over the faults' span and stopping short of its
        // end, so the last one is asked while the last fault is still on.
        let span: Duration = faults
            .iter()
            .zip(&gaps)
            .map(|(fault, gap)| fault.length() + *gap)
            .sum();
        let span_ms = u64::try_from(span.as_millis()).expect("small");
        let mut driver = moirae_sched::stream(seed, "shard");
        let actions_count = 4 + driver.below(5);
        let mut actions: Vec<Action> = (0..actions_count)
            .map(|_| Action {
                at: Duration::from_millis(driver.below(span_ms.saturating_sub(300).max(1))),
                kind: if driver.below(3) < 2 {
                    ActionKind::Split
                } else {
                    ActionKind::Transfer
                },
                range: driver.below(1 << 16),
                pick: driver.below(1 << 16),
                load: Load::Clients,
            })
            .collect();
        actions.sort_by_key(|action| action.at);
        let (drifts, skews) = clocks(seed);
        Self {
            warmup: Duration::from_millis(1200),
            faults,
            gaps,
            picks,
            actions,
            settle: election_max() * LIVENESS_TIMEOUTS + Duration::from_millis(200),
            drifts,
            skews,
            clients_held_off: None,
        }
    }

    /// The faults' span: every fault and the quiet after it.
    #[must_use]
    pub fn faults_span(&self) -> Duration {
        self.faults
            .iter()
            .zip(&self.gaps)
            .map(|(fault, gap)| fault.length() + *gap)
            .sum()
    }

    /// The whole run's virtual duration at most: the warmup, the faults, the driver's
    /// drain at its whole budget and the settle.
    #[must_use]
    pub fn total(&self) -> Duration {
        self.warmup + self.faults_span() + DRIVER_DRAIN + self.settle
    }

    /// Whether some pair of nodes drifts apart faster than the lease assumes.
    #[must_use]
    pub fn drift_exceeded(&self) -> bool {
        let mut max = 0;
        for a in &self.drifts {
            for b in &self.drifts {
                max = max.max((a - b).unsigned_abs());
            }
        }
        max > DRIFT_BOUND_PPM
    }
}

/// Every node's clock, drawn as the raft sweep draws its servers' (`raft::Schedule::draw`):
/// on half the seeds every rate error is within a third of the lease's bound; on the
/// other half one bootstrap node runs slow and every other node fast, moderately (two
/// thousand to a hundred thousand parts per million) or severely (the slow one at a
/// quarter to a third of true rate, the fast ones a quarter to a third fast). The slow
/// node is a bootstrap node because only those lead anything here. Offsets are within
/// fifty milliseconds either way.
fn clocks(seed: u64) -> (Vec<i64>, Vec<i64>) {
    raft::draw_clocks(
        &mut moirae_sched::stream(seed, "shard-clocks"),
        BOOTSTRAP,
        NODES,
    )
}

/// The key the clients draw as `i`: `k` and two digits, so the keys sort as their
/// numbers do and a split between two of them is a split between two numbers.
#[must_use]
pub fn key(i: u64) -> Bytes {
    Bytes::from(format!("k{i:02}"))
}

/// The user range the bootstrap writes: range 2 over [`SPAN`].
#[must_use]
pub fn bootstrap_range() -> Range {
    Range {
        id: RangeId(FIRST_RANGE),
        start: Bytes::from(SPAN.0),
        end: Bytes::from(SPAN.1),
    }
}

/// Every key's first range, which the run's checks of `raft::Report` read as the key's
/// range: range 2, from which every user range here descends with its voters
/// (PROPOSED D-113). [`Report::check`] asks the write bound again per range the run
/// ended with.
#[must_use]
pub fn first_range_of_key(_key: &Bytes) -> u64 {
    FIRST_RANGE
}

/// What the clients draw: [`KEYS`] keys named by [`key`], sent to range 2 where neither
/// the cache nor a lookup places one.
#[must_use]
pub fn client_keys() -> ClientKeys {
    ClientKeys {
        count: KEYS,
        name: key,
        fallback: first_range_of_key,
    }
}

/// The bootstrap's descriptor of range 2 at `generation`, as a client's cache holds it.
fn bootstrap_descriptor(generation: u64) -> RangeDescriptor {
    let span = bootstrap_range().span();
    RangeDescriptor {
        range: RangeId(FIRST_RANGE),
        start: span.start,
        end: span.end,
        generation,
        voters: (1..=BOOTSTRAP).map(ServerId).collect(),
        state: RangeState::Live,
    }
}

/// Client `n`'s cache at its start, by thirds as the node sweep's
/// (`raft::Cluster::initial_cache`): range 2 at generation 0, a client that knew the
/// cluster before it was bootstrapped; the root's descriptor alone, a client that looks
/// everything up through ranges 0 and 1; and the bootstrap's own descriptor of range 2.
#[must_use]
pub fn initial_cache(n: u64) -> Cache {
    match n % 3 {
        1 => Cache::seeded(&[bootstrap_descriptor(0)]),
        2 => Cache::seeded(&[raft::root_descriptor()]),
        _ => Cache::seeded(&[bootstrap_descriptor(FIRST_GENERATION)]),
    }
}

/// One node's configuration: five nodes, the three bootstrap nodes named, range 2 alone
/// among the user ranges, the node sweep's tick, drift bound and threshold, and the
/// range-id block D-092 has measured here (PROPOSED D-113).
#[must_use]
pub fn server_config(
    id: u64,
    variants: Variants,
    node: NodeVariants,
    range_variants: RangeVariants,
) -> ServerConfig {
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    ServerConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=NODES).map(|s| (ServerId(s), server_addr(s))).collect(),
        ranges: vec![bootstrap_range()],
        bootstrap: (1..=BOOTSTRAP).map(ServerId).collect(),
        raft: RaftConfig {
            variants,
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            drift_bound_ppm: DRIFT_BOUND_PPM,
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: INBOX_BYTES,
        snapshot_cap: SNAPSHOT_CAP,
        id_block: ID_BLOCK,
        refill_at: REFILL_AT,
        node,
        range_variants,
    }
}

/// The simulator for `seed`: the raft sweep's network, disk and clocks
/// (`raft::config`), Phase 2's bit rot, and a run-length hint for the schedule.
#[must_use]
pub fn config(seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = raft::faults_config(seed, BITROT);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(NODES + CLIENTS).expect("small"),
        schedule.total(),
    );
    config
}

/// A range's descriptor as the trace last stated it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span {
    /// The span's first key, encoded.
    pub start: Bytes,
    /// The key past its last, encoded.
    pub end: Bytes,
    /// The generation.
    pub generation: u64,
    /// The voters.
    pub voters: Vec<u64>,
    /// When the trace first stated the range at this generation.
    pub since: Instant,
}

/// Every range's latest descriptor, folded from `RangeCreated` and `RangeDescriptor`
/// by generation: what the driver draws its splits and transfers from, and the
/// descriptors the meta convergence bound asks meta to name.
#[derive(Clone, Debug, Default)]
pub struct Spans {
    latest: BTreeMap<u64, Span>,
    /// Every block of range ids granted, by its first id: its last, and the node it went to.
    blocks: BTreeMap<u64, (u64, u64)>,
    /// The nodes some split took a right half's id from a block of.
    takers: BTreeSet<u64>,
}

impl Spans {
    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        let (range, start, end, generation, voters) = match &record.event {
            // PROPOSED(D-131): who has taken an id, what the restart arm picks from.
            TraceEvent::RangeIdsLeased {
                node, first, last, ..
            } => {
                self.blocks.entry(*first).or_insert((*last, *node));
                return;
            }
            TraceEvent::RangeSplit { right, .. } => {
                if let Some((_, (last, node))) = self.blocks.range(..=*right).next_back()
                    && right <= last
                {
                    self.takers.insert(*node);
                }
                return;
            }
            TraceEvent::RangeCreated {
                range,
                start,
                end,
                generation,
                voters,
                ..
            }
            | TraceEvent::RangeDescriptor {
                range,
                start,
                end,
                generation,
                voters,
                ..
            } => (*range, start, end, *generation, voters),
            _ => return,
        };
        let newer = self
            .latest
            .get(&range)
            .is_none_or(|held| generation > held.generation);
        if newer {
            self.latest.insert(
                range,
                Span {
                    start: start.clone(),
                    end: end.clone(),
                    generation,
                    voters: voters.clone(),
                    since: record.at,
                },
            );
        }
    }

    /// Folds records in order.
    pub fn extend<'a>(&mut self, records: impl IntoIterator<Item = &'a TraceRecord>) {
        for record in records {
            self.push(record);
        }
    }

    /// Every range's latest descriptor.
    #[must_use]
    pub fn latest(&self) -> &BTreeMap<u64, Span> {
        &self.latest
    }

    /// The nodes some split took a right half's id from a block of, in order.
    #[must_use]
    pub fn takers(&self) -> Vec<u64> {
        self.takers.iter().copied().collect()
    }

    /// The range whose latest descriptor starts at `start`, if any.
    #[must_use]
    pub fn starting_at(&self, start: &Bytes) -> Option<(u64, &Span)> {
        self.user().find(|(_, span)| span.start == *start)
    }

    /// The user ranges' latest descriptors.
    pub fn user(&self) -> impl Iterator<Item = (u64, &Span)> {
        self.latest
            .iter()
            .filter(|(range, _)| **range >= FIRST_RANGE)
            .map(|(range, span)| (*range, span))
    }
}

/// The clients' keys a span holds, in order.
fn keys_in(span: &Span) -> Vec<u64> {
    (0..KEYS)
        .filter(|&i| {
            let encoded = user_key(&key(i));
            span.start <= encoded && encoded < span.end
        })
        .collect()
}

/// The keys a split of `span` may cut at: those that leave [`MIN_KEYS`] of the clients'
/// keys on each side.
fn split_keys(span: &Span) -> Vec<u64> {
    let held = keys_in(span);
    let min = usize::try_from(MIN_KEYS).expect("small");
    if held.len() < 2 * min {
        return Vec::new();
    }
    held[min..=held.len() - min].to_vec()
}

/// What the driver did over a run (PROPOSED D-113).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DriverStats {
    /// Splits asked.
    pub splits_asked: u64,
    /// Answered done: proposed, and applied on the leader that answered.
    pub splits_done: u64,
    /// Refused, by the reason's name, every answer counted: one split refused
    /// `NoRangeId` twice and then done counts two here and one in `splits_done`.
    pub refused: BTreeMap<&'static str, u64>,
    /// Every `NoRangeId` answer, as (the node that answered, when).
    pub no_range_id: Vec<(u64, Instant)>,
    /// Every split a `NoRangeId` answered at least once: what its asker's retries came to
    /// (PROPOSED D-092's per-seed assertion).
    pub retried: Vec<Retried>,
    /// Splits asked of every node in turn and answered by none in time.
    pub splits_unanswered: u64,
    /// Split actions that found no span holding enough keys to split.
    pub splits_skipped: u64,
    /// Transfers asked.
    pub transfers_asked: u64,
    /// Transfers answered done.
    pub transfers_done: u64,
    /// Admins still asking.
    pub outstanding: u64,
}

/// One `NoRangeId` read against the trace: what emptied the node's block
/// (PROPOSED D-092's figures).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoRangeId {
    /// The node that answered it.
    pub node: u64,
    /// When.
    pub at: Instant,
    /// The node's last restart before it, if it restarted.
    pub restarted: Option<Instant>,
    /// Whether a block was granted to the node since that restart. Where none was, the
    /// block is empty because the run is new, whatever the block's size: a new run holds
    /// no id until its refill lands (SHARD.md §5's second rule).
    pub leased_since_restart: bool,
    /// Whether a block was granted to the node within a minimum election timeout after
    /// the answer: D-092's split of the figure.
    pub leased_within: bool,
    /// How many ids splits had taken from the latest block granted since that restart.
    pub taken: usize,
}

/// A split `NoRangeId` answered at least once, and what its asker's retries came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retried {
    /// The range asked of.
    pub range: u64,
    /// When the first `NoRangeId` came.
    pub refused: Instant,
    /// When the asker stopped asking.
    pub ended: Instant,
    /// Whether it stopped on an answer: done, or refused for a reason it does not retry.
    pub answered: bool,
}

impl Retried {
    /// Whether it was still asking when the last fault healed at `heal`, so that some retry
    /// of its came after the heal.
    #[must_use]
    pub fn asking_at(&self, heal: Instant) -> bool {
        self.ended >= heal
    }

    /// How long it took, read from `heal`, or from its first refusal if that came later.
    #[must_use]
    pub fn took(&self, heal: Instant) -> Duration {
        self.ended.duration_since(self.refused.max(heal))
    }

    /// What D-092 asks of this asker, where the last fault healed at `heal` and the bound is
    /// `bound`: "the same asker's retry after the heal takes effect within the liveness
    /// bound". One that stopped before the heal made no retry after it and is asked nothing.
    /// Any other is asked for an answer, within the bound of the heal, or of its first
    /// refusal if that came after the heal. The violation, in words, if it did neither.
    // PROPOSED(D-092): answer A; the asker retries.
    // PROPOSED(D-113): read from the heal, §8.
    #[must_use]
    pub fn took_no_effect(&self, heal: Instant, bound: Duration) -> Option<String> {
        if !self.asking_at(heal) {
            return None;
        }
        let took = self.took(heal);
        if !self.answered {
            return Some(format!(
                "the split of range {} refused at {:?} stopped asking at {:?}, {took:?} past the \
                 last heal at {heal:?}, with no answer",
                self.range, self.refused, self.ended
            ));
        }
        (took > bound).then(|| {
            format!(
                "the split of range {} refused at {:?} was answered {took:?} past the last heal \
                 at {heal:?}, over {bound:?}",
                self.range, self.refused
            )
        })
    }
}

type Driver = Arc<Mutex<DriverStats>>;

fn lock<T>(what: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    what.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The split's admin: asks `range`'s leader, `target` first, to split at `key`, follows
/// `NotLeader` round the bootstrap nodes twice, retries after `NoRangeId` (PROPOSED
/// D-092's A), and records every answer. Under a burst it asks `delay` after the action fired
/// (PROPOSED D-131).
///
/// A leader answers a split's copy with silence once its entry is in the log, and answers the
/// entry once, at its apply (SHARD.md §5): an asker that heard `NoRangeId`, then asked again,
/// and whose answer was lost to the network would otherwise hear nothing more until its copy
/// was forgotten. So after a silent wait such an asker asks anew, under a number the leader has
/// not seen, which the leader answers at once: the split done, or refused `KeyOutsideSpan`
/// where it has already landed. Seed 992 found it: the split took effect 130 ms after its
/// first refusal and its asker was answered 3.2 s later (PROPOSED D-131).
#[allow(clippy::too_many_arguments)]
async fn split_admin<E: Environment>(
    env: E,
    n: u64,
    mut target: u64,
    range: u64,
    generation: u64,
    key: Bytes,
    stats: Driver,
    delay: Duration,
) {
    let Ok(sock) = env.net().bind(admin_addr(n)).await else {
        lock(&stats).outstanding -= 1;
        return;
    };
    if !delay.is_zero() {
        env.clock().sleep(delay).await;
    }
    let mut tries = BOOTSTRAP * 4;
    let mut done = false;
    let mut first_refused: Option<Instant> = None;
    let mut seq = n;
    while tries > 0 && !done {
        tries -= 1;
        let request = RangedRequest {
            range: RangeId(range),
            generation,
            request: Request {
                client: DRIVER,
                seq,
                command: Command::Split {
                    key: key.clone(),
                    right: 0,
                },
            },
        };
        if sock
            .send(server_addr(target), request.encode())
            .await
            .is_err()
        {
            break;
        }
        let answer = {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep(SPLIT_WAIT));
            race(&env, recv, timer).await
        };
        let Either::Left(Ok((_, bytes))) = answer else {
            target = target % BOOTSTRAP + 1;
            // PROPOSED(D-131): the copy of a split the log holds is answered once; see above.
            if first_refused.is_some() {
                seq += ASK_ANEW;
            }
            continue;
        };
        let Ok(answer) = RangedResponse::decode(bytes) else {
            continue;
        };
        if answer.response.seq % ASK_ANEW != n {
            continue;
        }
        match answer.response.reply {
            Reply::Outcome(Outcome::Refused(reason)) => {
                let now = env.clock().now();
                {
                    let mut stats = lock(&stats);
                    *stats.refused.entry(reason.name()).or_default() += 1;
                    if reason == SplitRefusal::NoRangeId {
                        stats.no_range_id.push((target, now));
                    }
                }
                if reason == SplitRefusal::NoRangeId {
                    first_refused.get_or_insert(now);
                    env.clock().sleep(SPLIT_RETRY).await;
                    continue;
                }
                done = true;
            }
            Reply::Outcome(_) => {
                lock(&stats).splits_done += 1;
                done = true;
            }
            Reply::NotLeader { leader } => {
                target = leader.map_or(target % BOOTSTRAP + 1, |leader| leader.0);
            }
            Reply::RangeMismatch { .. } => {
                target = target % BOOTSTRAP + 1;
            }
        }
    }
    let ended = env.clock().now();
    let mut stats = lock(&stats);
    if !done {
        stats.splits_unanswered += 1;
    }
    if let Some(refused) = first_refused {
        stats.retried.push(Retried {
            range,
            refused,
            ended,
            answered: done,
        });
    }
    stats.outstanding -= 1;
}

/// The transfer's admin: asks `range`'s leader, `target`, to hand over to `to`, once
/// more where the answer names another leader.
async fn transfer_admin<E: Environment>(
    env: E,
    n: u64,
    mut target: u64,
    range: u64,
    generation: u64,
    to: u64,
    stats: Driver,
) {
    let Ok(sock) = env.net().bind(admin_addr(n)).await else {
        lock(&stats).outstanding -= 1;
        return;
    };
    for _ in 0..2 {
        let request = RangedRequest {
            range: RangeId(range),
            generation,
            request: Request {
                client: DRIVER,
                seq: n,
                command: Command::Transfer { to },
            },
        };
        if sock
            .send(server_addr(target), request.encode())
            .await
            .is_err()
        {
            break;
        }
        let answer = {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep(TRANSFER_WAIT));
            race(&env, recv, timer).await
        };
        let Either::Left(Ok((_, bytes))) = answer else {
            break;
        };
        let Ok(answer) = RangedResponse::decode(bytes) else {
            break;
        };
        match answer.response.reply {
            Reply::Outcome(_) => {
                lock(&stats).transfers_done += 1;
                break;
            }
            Reply::NotLeader {
                leader: Some(leader),
            } if leader.0 != to => target = leader.0,
            _ => break,
        }
    }
    lock(&stats).outstanding -= 1;
}

/// The process a burst of action `j` (from 1) traces under.
#[must_use]
fn burst_process(j: u64) -> u64 {
    LOAD | j
}

/// The burst of [`Load::WriteBurst`] and [`Load::ReadBurst`]: `ops`, one every `every`, at the
/// parent's leader `target`, under the parent's range and generation, each answered as it comes
/// (PROPOSED D-131). Unlike the Figure 8 driver's burst (`raft::burst`, D-031), which only
/// needs its appends, this one takes its answers, so a write a variant acknowledges and a read
/// it serves are in the history. Each is sent once: a write it hears nothing of, or is told is
/// not this range's, stays pending or leaves the history at its apply, as a client's does
/// (SHARD.md §9).
// PROPOSED(D-131): the workload's bursts (D-031's shape for a driver).
async fn load_burst<E: Environment>(
    env: E,
    j: u64,
    target: u64,
    range: u64,
    generation: u64,
    ops: Vec<ClientOp>,
    every: Duration,
) {
    let Ok(sock) = env.net().bind(burst_addr(j)).await else {
        return;
    };
    let process = burst_process(j);
    let mut next = 0usize;
    let mut wake = env.clock().now();
    let mut answered: BTreeSet<u64> = BTreeSet::new();
    loop {
        let recv = pin!(sock.recv());
        let timer = pin!(env.clock().sleep_until(wake));
        match race(&env, recv, timer).await {
            Either::Left(Ok((_, bytes))) => {
                let Ok(answer) = RangedResponse::decode(bytes) else {
                    continue;
                };
                let (seq, reply) = (answer.response.seq, answer.response.reply);
                if let Reply::Outcome(result) = reply
                    && answered.insert(seq)
                {
                    env.trace(TraceEvent::ClientReturn {
                        client: process,
                        seq,
                        result: raft::to_result(result),
                    });
                }
            }
            Either::Left(Err(_)) => return,
            Either::Right(()) => {
                let Some(op) = ops.get(next) else {
                    return;
                };
                let seq = u64::try_from(next).expect("small");
                env.trace(TraceEvent::ClientInvoke {
                    client: process,
                    seq,
                    op: op.clone(),
                });
                let request = RangedRequest {
                    range: RangeId(range),
                    generation,
                    request: Request {
                        client: process,
                        seq,
                        command: raft::to_command(op),
                    },
                };
                if sock
                    .send(server_addr(target), request.encode())
                    .await
                    .is_err()
                {
                    return;
                }
                next += 1;
                wake = if next == ops.len() {
                    env.clock().now() + LOAD_WAIT
                } else {
                    wake + every
                };
            }
        }
    }
}

/// One of the two stale-cache shape's clients (PROPOSED D-131): asks `op` of `range` at
/// `generation` through `target`, following `NotLeader` round the bootstrap nodes, and traces
/// its call and its answer as a client's. The operation is sent as (client, seq) once per
/// node asked. Whether it was answered with an outcome.
async fn ask_once<E: Environment, S: Socket>(
    env: &E,
    sock: &S,
    process: u64,
    mut target: u64,
    range: u64,
    generation: u64,
    op: ClientOp,
) -> bool {
    env.trace(TraceEvent::ClientInvoke {
        client: process,
        seq: 0,
        op: op.clone(),
    });
    let command = raft::to_command(&op);
    for _ in 0..BOOTSTRAP {
        let request = RangedRequest {
            range: RangeId(range),
            generation,
            request: Request {
                client: process,
                seq: 0,
                command: command.clone(),
            },
        };
        if sock
            .send(server_addr(target), request.encode())
            .await
            .is_err()
        {
            return false;
        }
        let deadline = env.clock().now() + STALE_WAIT;
        let reply = loop {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep_until(deadline));
            match race(env, recv, timer).await {
                Either::Left(Ok((_, bytes))) => {
                    let Ok(answer) = RangedResponse::decode(bytes) else {
                        continue;
                    };
                    if answer.response.client == process {
                        break Some(answer.response.reply);
                    }
                }
                Either::Left(Err(_)) | Either::Right(()) => break None,
            }
        };
        match reply {
            Some(Reply::Outcome(result)) => {
                env.trace(TraceEvent::ClientReturn {
                    client: process,
                    seq: 0,
                    result: raft::to_result(result),
                });
                return true;
            }
            Some(Reply::NotLeader {
                leader: Some(leader),
            }) => target = leader.0,
            // A `RangeMismatch` is the correct server's answer to the reader, and its call
            // leaves the history as one never proposed (SHARD.md §9).
            Some(Reply::RangeMismatch { .. }) => return false,
            Some(Reply::NotLeader { leader: None }) | None => target = target % BOOTSTRAP + 1,
        }
    }
    false
}

/// [`Load::StaleCache`]'s two clients, as action `j`'s (PROPOSED D-131): the writer puts a
/// right-half key through the right half, and once it has its answer the reader, whose cache
/// still names the parent, gets that key through the parent at the generation it had.
async fn stale_clients<E: Environment>(
    env: E,
    j: u64,
    key: Bytes,
    right: (u64, u64, u64),
    parent: (u64, u64, u64),
) {
    let Ok(sock) = env.net().bind(burst_addr(j)).await else {
        return;
    };
    let (range, generation, target) = right;
    let value = Bytes::from(format!("l{j}"));
    let done = ask_once(
        &env,
        &sock,
        LOAD | WRITER | j,
        target,
        range,
        generation,
        ClientOp::Put {
            key: key.clone(),
            value,
        },
    )
    .await;
    if !done {
        return;
    }
    let (range, generation, target) = parent;
    let _ = ask_once(
        &env,
        &sock,
        LOAD | READER | j,
        target,
        range,
        generation,
        ClientOp::Get { key },
    )
    .await;
}

/// What the sliced advance watches for: the raft sweep's watch over the incremental
/// checkers, which stops the run at the first violation (D-046), and the latest descriptors
/// the driver draws from.
struct Watch {
    checks: raft::Watch,
    spans: Spans,
}

impl Watch {
    /// A watch whose checkers stop the run, or, for [`run_to_the_end`], fold nothing.
    fn new(stops: bool) -> Self {
        let mut checks = raft::Watch::of_descriptors();
        checks.stops = stops;
        Self {
            checks,
            spans: Spans::default(),
        }
    }

    /// Folds what the trace holds past the last look, and stops the run at a violation.
    fn look(&mut self, sim: &Sim) {
        let spans = &mut self.spans;
        self.checks.look(sim, |records| spans.extend(records));
    }
}

/// Advances the run by `duration`, as the raft sweep's advance does.
fn advance(sim: &mut Sim, duration: Duration, watch: &mut Watch) {
    let Watch { checks, spans } = watch;
    raft::advance_with(sim, duration, checks, |checks, sim| {
        checks.look(sim, |records| spans.extend(records));
    });
}

/// Advances the run to `at`, if it is ahead.
fn advance_to(sim: &mut Sim, at: Instant, watch: &mut Watch) {
    let now = sim.now();
    if at > now {
        advance(sim, at.duration_since(now), watch);
    }
}

fn node_id(id: u64) -> NodeId {
    NodeId::new(u32::try_from(id).expect("small"))
}

fn spawn(sim: &Sim, id: u64, variants: Variants, node: NodeVariants, range: RangeVariants) {
    let env = sim.env(node_id(id));
    let inner = env.clone();
    env.spawn("node", async move {
        let _ = ananke_shard::server::run(inner, server_config(id, variants, node, range)).await;
    });
}

/// One step of the run's timeline.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// Fault `i` begins.
    Begin(usize),
    /// Fault `i` heals.
    End(usize),
    /// The driver's action `j` fires.
    Act(usize),
    /// Action `j`'s stale-cache shape moves the right half's lead off the parent's leader's
    /// node.
    StaleTransfer(usize),
    /// Action `j`'s stale-cache shape's two clients ask.
    StaleAsk(usize),
    /// Fault `i`'s [`Fault::SplitFromRestarted`] hands its restarted node a range's lead.
    RestartTransfer(usize),
    /// Fault `i`'s [`Fault::SplitFromRestarted`] asks its restarted node to split the range.
    RestartSplit(usize),
}

/// What a shape resolved to when it fired, as a split's [`Load`] and a restart arm do not
/// know until then (PROPOSED D-131).
#[derive(Clone, Debug)]
struct Aimed {
    /// The parent.
    parent: u64,
    /// The parent's generation when the split was asked.
    generation: u64,
    /// The key the split cuts at.
    cut: u64,
}

/// The arm [`Fault::SplitFromRestarted`] as it fired, read against the trace (PROPOSED D-131).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartArm {
    /// The node crashed and restarted.
    pub victim: u64,
    /// The range handed to it, where one could still split.
    pub range: Option<u64>,
    /// The sequence number of the split its admin asked, where it asked one.
    pub seq: Option<u64>,
}

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// The cores' known-buggy variants (D-045).
    pub variants: Variants,
    /// The node's own.
    pub node_variants: NodeVariants,
    /// The range layer's (Q37).
    pub range_variants: RangeVariants,
    /// The faults and the driver's actions it ran.
    pub schedule: Schedule,
    /// What the driver did.
    pub driver: DriverStats,
    /// The trace under the raft sweep's checks (`raft::Report::over_a_run`).
    pub checked: raft::Report,
    /// When the last fault healed: the heal D-092 reads its asker's retry from, where
    /// `checked.last_heal`, the liveness window's start, is past the driver's drain and so
    /// past every asker the drain saw finish (PROPOSED D-113 §8).
    pub last_fault_healed: Instant,
    /// Each [`Fault::SplitFromRestarted`] that found a node to crash.
    pub restart_arms: Vec<RestartArm>,
}

/// Runs the correct system on `seed`.
#[must_use]
pub fn run(seed: u64) -> Report {
    run_with(
        seed,
        Schedule::draw(seed),
        Variants::default(),
        NodeVariants::correct(),
        RangeVariants::correct(),
    )
}

/// Runs `seed` with the cores carrying `variants`, the node `node_variants` and the
/// range layer `range_variants` (Q37).
#[must_use]
pub fn run_of(
    seed: u64,
    variants: impl Into<Variants>,
    node_variants: NodeVariants,
    range_variants: RangeVariants,
) -> Report {
    run_with(
        seed,
        Schedule::draw(seed),
        variants.into(),
        node_variants,
        range_variants,
    )
}

/// [`run_of`] with no violation stopping it: the same run for as long as the sweep's
/// would have lasted, and past that to its schedule's end, so a check about time can be read
/// of a variant another check would have stopped first (PROPOSED D-113).
#[must_use]
pub fn run_to_the_end(
    seed: u64,
    variants: impl Into<Variants>,
    node_variants: NodeVariants,
    range_variants: RangeVariants,
) -> Report {
    run_watched(
        seed,
        Schedule::draw(seed),
        variants.into(),
        node_variants,
        range_variants,
        Watch::new(false),
    )
}

/// [`run_to_the_end`] on a schedule the caller names (PROPOSED D-131).
#[must_use]
pub fn run_to_the_end_with(
    seed: u64,
    schedule: Schedule,
    variants: Variants,
    node_variants: NodeVariants,
    range_variants: RangeVariants,
) -> Report {
    run_watched(
        seed,
        schedule,
        variants,
        node_variants,
        range_variants,
        Watch::new(false),
    )
}

/// Runs `schedule` on `seed`.
#[must_use]
pub fn run_with(
    seed: u64,
    schedule: Schedule,
    variants: Variants,
    node_variants: NodeVariants,
    range_variants: RangeVariants,
) -> Report {
    run_watched(
        seed,
        schedule,
        variants,
        node_variants,
        range_variants,
        Watch::new(true),
    )
}

fn run_watched(
    seed: u64,
    schedule: Schedule,
    variants: Variants,
    node_variants: NodeVariants,
    range_variants: RangeVariants,
    mut watch: Watch,
) -> Report {
    let mut sim = Sim::new(config(seed, &schedule));
    let nodes: Vec<NodeId> = (0..NODES as usize)
        .map(|i| sim.add_node_with_clock(schedule.skews[i], schedule.drifts[i]))
        .collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    for id in 1..=NODES {
        spawn(&sim, id, variants, node_variants, range_variants);
    }
    let stats: Vec<SharedStats> = (0..CLIENTS).map(|_| SharedStats::default()).collect();
    for (i, &at) in clients.iter().enumerate() {
        let n = i as u64 + 1;
        let env = sim.env(at);
        let inner = env.clone();
        env.spawn(
            "client",
            raft::client_over(
                raft::Cluster::Node,
                inner,
                n,
                NODES,
                stats[i].clone(),
                range_variants,
                client_keys(),
                initial_cache(n),
            ),
        );
    }
    let driver: Driver = Arc::default();
    advance(&mut sim, schedule.warmup, &mut watch);

    // The timeline: each fault's beginning and heal, and each action of the driver,
    // in time order; a fault's heal before an action at the same instant.
    let begin = sim.now();
    let mut steps: Vec<(Instant, u8, Step)> = Vec::new();
    let mut at = begin;
    for (i, (fault, gap)) in schedule.faults.iter().zip(&schedule.gaps).enumerate() {
        steps.push((at, 1, Step::Begin(i)));
        at += fault.length();
        steps.push((at, 0, Step::End(i)));
        if let Fault::SplitFromRestarted {
            transfer_after,
            split_after,
            ..
        } = fault
        {
            steps.push((at + *transfer_after, 2, Step::RestartTransfer(i)));
            steps.push((
                at + *transfer_after + *split_after,
                2,
                Step::RestartSplit(i),
            ));
        }
        at += *gap;
    }
    for (j, action) in schedule.actions.iter().enumerate() {
        let fires = begin + action.at;
        steps.push((fires, 2, Step::Act(j)));
        if let Load::StaleCache {
            transfer_after,
            ask_after,
        } = action.load
        {
            steps.push((fires + transfer_after, 2, Step::StaleTransfer(j)));
            steps.push((fires + transfer_after + ask_after, 2, Step::StaleAsk(j)));
        }
    }
    steps.sort_by_key(|(at, order, _)| (*at, *order));

    let all: Vec<NodeId> = nodes
        .iter()
        .chain(clients.iter())
        .chain(std::iter::once(&admin))
        .copied()
        .collect();
    let mut isolations: Vec<(u64, Instant, Instant)> = Vec::new();
    // The fault in force: its victim node, if it has one, and when it began.
    let mut open: Option<(u64, Instant)> = None;
    // What the shapes resolved to when they fired (PROPOSED D-131).
    let mut aimed: Vec<Option<Aimed>> = vec![None; schedule.actions.len()];
    let mut restart_arms: Vec<Option<RestartArm>> = vec![None; schedule.faults.len()];
    let mut last_heal = sim.now();
    for (at, _, step) in steps {
        advance_to(&mut sim, at, &mut watch);
        if watch.checks.stopped.is_some() {
            break;
        }
        match step {
            Step::Begin(i) => {
                let fault = schedule.faults[i];
                let pick = schedule.picks[i];
                // A leader-relative arm aims at the leader of a range drawn among those
                // the trace holds when it fires (§11, env 8).
                let aimed = || {
                    let ranges: Vec<u64> = watch.spans.latest().keys().copied().collect();
                    let range = ranges
                        .get(usize::try_from(pick % ranges.len().max(1) as u64).expect("small"))
                        .copied()
                        .unwrap_or(FIRST_RANGE);
                    raft::leader_of_range(&sim, range)
                };
                match fault {
                    Fault::Isolate { node, client, .. } => {
                        let side = [nodes[node as usize - 1], clients[client as usize - 1]];
                        let rest: Vec<NodeId> =
                            all.iter().copied().filter(|n| !side.contains(n)).collect();
                        sim.partition(&side, &rest);
                        open = Some((node, sim.now()));
                    }
                    Fault::IsolateLeader { .. } => {
                        let leader = aimed();
                        let side = [nodes[leader as usize - 1], clients[0]];
                        let rest: Vec<NodeId> =
                            all.iter().copied().filter(|n| !side.contains(n)).collect();
                        sim.partition(&side, &rest);
                        open = Some((leader, sim.now()));
                    }
                    Fault::OneWay { from, to, .. } => {
                        sim.block(nodes[from as usize - 1], nodes[to as usize - 1]);
                        open = None;
                    }
                    Fault::Crash { node, .. } => {
                        sim.crash(nodes[node as usize - 1]);
                        open = Some((node, sim.now()));
                    }
                    Fault::CrashLeader { .. } => {
                        let leader = aimed();
                        sim.crash(nodes[leader as usize - 1]);
                        open = Some((leader, sim.now()));
                    }
                    // PROPOSED(D-131): a node some split has taken an id from, as the trace
                    // says when the arm fires.
                    Fault::SplitFromRestarted { .. } => {
                        watch.look(&sim);
                        let takers = watch.spans.takers();
                        open = takers
                            .get(usize::try_from(pick % takers.len().max(1) as u64).expect("small"))
                            .map(|&victim| {
                                sim.crash(nodes[victim as usize - 1]);
                                restart_arms[i] = Some(RestartArm {
                                    victim,
                                    range: None,
                                    seq: None,
                                });
                                (victim, sim.now())
                            });
                    }
                }
            }
            Step::End(i) => {
                match schedule.faults[i] {
                    Fault::Isolate { .. } | Fault::IsolateLeader { .. } => {
                        sim.heal();
                        if let Some((node, from)) = open.take() {
                            isolations.push((node, from, sim.now()));
                        }
                    }
                    Fault::OneWay { .. } => sim.heal(),
                    Fault::Crash { .. }
                    | Fault::CrashLeader { .. }
                    | Fault::SplitFromRestarted { .. } => {
                        if let Some((node, _)) = open.take() {
                            sim.restart(nodes[node as usize - 1]);
                            spawn(&sim, node, variants, node_variants, range_variants);
                        }
                    }
                }
                last_heal = sim.now();
            }
            Step::Act(j) => {
                // The action is resolved against the trace as it stands now, not as
                // the last look left it.
                watch.look(&sim);
                if watch.checks.stopped.is_some() {
                    break;
                }
                aimed[j] = act(&sim, admin, j, &schedule.actions[j], &watch.spans, &driver);
            }
            // PROPOSED(D-131): the shapes' later steps, each resolved against the trace as
            // it stands when it fires, each doing nothing where its split did not land.
            Step::StaleTransfer(j) => {
                watch.look(&sim);
                if watch.checks.stopped.is_some() {
                    break;
                }
                if let Some(aim) = &aimed[j] {
                    move_right_half_off(
                        &sim,
                        admin,
                        j,
                        aim,
                        schedule.actions[j].pick,
                        &watch.spans,
                        &driver,
                    );
                }
            }
            Step::StaleAsk(j) => {
                watch.look(&sim);
                if watch.checks.stopped.is_some() {
                    break;
                }
                if let Some(aim) = &aimed[j] {
                    ask_through_both(&sim, admin, j, aim, schedule.actions[j].pick, &watch.spans);
                }
            }
            Step::RestartTransfer(i) => {
                watch.look(&sim);
                if watch.checks.stopped.is_some() {
                    break;
                }
                if let Some(arm) = restart_arms[i].as_mut() {
                    hand_restarted_a_range(
                        &sim,
                        admin,
                        i,
                        arm,
                        schedule.picks[i],
                        &watch.spans,
                        &driver,
                    );
                }
            }
            Step::RestartSplit(i) => {
                watch.look(&sim);
                if watch.checks.stopped.is_some() {
                    break;
                }
                if let Some(arm) = restart_arms[i].as_mut() {
                    split_from_restarted(
                        &sim,
                        admin,
                        i,
                        arm,
                        schedule.picks[i],
                        &watch.spans,
                        &driver,
                    );
                }
            }
        }
    }
    // The driver's admins still asking finish before the liveness window opens: the
    // window is about a cluster the adversary has stopped touching.
    let drain = sim.now() + DRIVER_DRAIN;
    while watch.checks.stopped.is_none() && lock(&driver).outstanding > 0 && sim.now() < drain {
        advance(&mut sim, SLICE, &mut watch);
    }
    let last_fault_healed = last_heal;
    last_heal = last_heal.max(sim.now());
    // The instrument for the bound read from the heal (PROPOSED D-113 §5): the same window,
    // its first stretch with no client reaching any node. `last_heal` is not moved, as the
    // cluster's own slowness would not move it.
    if let Some(held) = schedule.clients_held_off {
        let held = held.min(schedule.settle);
        sim.partition(&clients, &nodes);
        advance(&mut sim, held, &mut watch);
        sim.heal();
        advance(&mut sim, schedule.settle - held, &mut watch);
    } else {
        advance(&mut sim, schedule.settle, &mut watch);
    }
    let ended = sim.now();
    let records = sim.trace();
    let history = History::from_trace(&records);
    let mut clients_total = ClientStats::default();
    for s in &stats {
        let s = lock(s);
        clients_total.completed += s.completed;
        clients_total.abandoned += s.abandoned;
        clients_total.redirected += s.redirected;
        clients_total.mismatched += s.mismatched;
        clients_total.lookups += s.lookups;
    }
    let mut checked = raft::Report::over_a_run(raft::Run {
        seed,
        variants,
        policy: sim.policy(),
        header: sim.run_header(),
        records,
        last_heal,
        ended,
        isolations,
        history,
        clients: clients_total,
        ranges: vec![ROOT_RANGE, META_RANGE, FIRST_RANGE],
        key_range: first_range_of_key,
        stopped: watch.checks.stopped,
    });
    // The timer check reads each server's clock rate from the schedule.
    checked.schedule.drifts.clone_from(&schedule.drifts);
    checked.schedule.skews.clone_from(&schedule.skews);
    let driver = lock(&driver).clone();
    Report {
        seed,
        variants,
        node_variants,
        range_variants,
        schedule,
        driver,
        checked,
        last_fault_healed,
        restart_arms: restart_arms.into_iter().flatten().collect(),
    }
}

/// [`Load::StaleCache`]'s second step (PROPOSED D-131): where the right half is led from the
/// parent's leader's node, asks that its lead move to another voter, so that the right half's
/// writes commit on two nodes and the parent's leader may lag them. Leaves it alone where a
/// fault or a transfer already moved it, and where the split has not landed.
// PROPOSED(D-131)
fn move_right_half_off(
    sim: &Sim,
    admin: NodeId,
    j: usize,
    aim: &Aimed,
    pick: u64,
    spans: &Spans,
    driver: &Driver,
) {
    let Some((right, span)) = spans.starting_at(&user_key(&key(aim.cut))) else {
        return;
    };
    let parent_leader = raft::leader_of_range(sim, aim.parent);
    if raft::leader_of_range(sim, right) != parent_leader {
        return;
    }
    let others: Vec<u64> = span
        .voters
        .iter()
        .copied()
        .filter(|voter| *voter != parent_leader)
        .collect();
    let Some(&to) = others.get(usize::try_from(pick % others.len().max(1) as u64).expect("small"))
    else {
        return;
    };
    {
        let mut stats = lock(driver);
        stats.transfers_asked += 1;
        stats.outstanding += 1;
    }
    let n = STALE_ADMIN + u64::try_from(j).expect("small");
    let env = sim.env(admin);
    let inner = env.clone();
    env.spawn(
        "stale-transfer",
        transfer_admin(
            inner,
            n,
            parent_leader,
            right,
            span.generation,
            to,
            driver.clone(),
        ),
    );
}

/// [`Load::StaleCache`]'s last step (PROPOSED D-131): one client writes a right-half key
/// through the right half and another, whose cache still names the parent, reads it through
/// the parent.
// PROPOSED(D-131)
fn ask_through_both(sim: &Sim, admin: NodeId, j: usize, aim: &Aimed, pick: u64, spans: &Spans) {
    let Some((right, span)) = spans.starting_at(&user_key(&key(aim.cut))) else {
        return;
    };
    let held = keys_in(span);
    let Some(&at) = held.get(usize::try_from(pick % held.len().max(1) as u64).expect("small"))
    else {
        return;
    };
    let writes_through = (right, span.generation, raft::leader_of_range(sim, right));
    let reads_through = (
        aim.parent,
        aim.generation,
        raft::leader_of_range(sim, aim.parent),
    );
    let env = sim.env(admin);
    let inner = env.clone();
    env.spawn(
        "stale-clients",
        stale_clients(
            inner,
            u64::try_from(j).expect("small") + 1,
            key(at),
            writes_through,
            reads_through,
        ),
    );
}

/// [`Fault::SplitFromRestarted`]'s first step after the restart (PROPOSED D-131): picks a user
/// range that can still split, and asks its leader to hand it to the restarted node. No range
/// can, and the arm has only been a crash.
// PROPOSED(D-131)
fn hand_restarted_a_range(
    sim: &Sim,
    admin: NodeId,
    i: usize,
    arm: &mut RestartArm,
    pick: u64,
    spans: &Spans,
    driver: &Driver,
) {
    let candidates: Vec<(u64, &Span)> = spans
        .user()
        .filter(|(_, span)| !split_keys(span).is_empty())
        .collect();
    let Some(&(range, span)) =
        candidates.get(usize::try_from(pick % candidates.len().max(1) as u64).expect("small"))
    else {
        return;
    };
    arm.range = Some(range);
    let leader = raft::leader_of_range(sim, range);
    if leader == arm.victim {
        return;
    }
    {
        let mut stats = lock(driver);
        stats.transfers_asked += 1;
        stats.outstanding += 1;
    }
    let n = RESTART_ADMIN + 2 * u64::try_from(i).expect("small");
    let env = sim.env(admin);
    let inner = env.clone();
    env.spawn(
        "restart-transfer",
        transfer_admin(
            inner,
            n,
            leader,
            range,
            span.generation,
            arm.victim,
            driver.clone(),
        ),
    );
}

/// [`Fault::SplitFromRestarted`]'s last step (PROPOSED D-131): asks the restarted node to split
/// the range it was handed.
// PROPOSED(D-131)
fn split_from_restarted(
    sim: &Sim,
    admin: NodeId,
    i: usize,
    arm: &mut RestartArm,
    pick: u64,
    spans: &Spans,
    driver: &Driver,
) {
    let Some(range) = arm.range else {
        return;
    };
    let Some(span) = spans.latest().get(&range) else {
        return;
    };
    let keys = split_keys(span);
    let Some(&at) = keys.get(usize::try_from(pick % keys.len().max(1) as u64).expect("small"))
    else {
        return;
    };
    let n = RESTART_ADMIN + 2 * u64::try_from(i).expect("small") + 1;
    arm.seq = Some(n);
    {
        let mut stats = lock(driver);
        stats.splits_asked += 1;
        stats.outstanding += 1;
    }
    let env = sim.env(admin);
    let inner = env.clone();
    env.spawn(
        "restart-split",
        split_admin(
            inner,
            n,
            arm.victim,
            range,
            span.generation,
            key(at),
            driver.clone(),
            Duration::ZERO,
        ),
    );
}

/// The `count` operations of a burst of action `n`: right-half keys taken in turn from the
/// one `pick` names, puts under distinct values or gets (PROPOSED D-131).
fn burst_ops(n: u64, right: &[u64], count: u64, pick: u64, writes: bool) -> Vec<ClientOp> {
    let len = right.len() as u64;
    (0..count)
        .map(|i| {
            let at = usize::try_from((pick + i) % len).expect("small");
            let of = key(right[at]);
            if writes {
                ClientOp::Put {
                    key: of,
                    value: Bytes::from(format!("l{n}.{i}")),
                }
            } else {
                ClientOp::Get { key: of }
            }
        })
        .collect()
}

/// Fires the driver's action `j`, resolved against `spans` as they stand. What a split aimed at,
/// for the shape's later steps (PROPOSED D-131).
fn act(
    sim: &Sim,
    admin: NodeId,
    j: usize,
    action: &Action,
    spans: &Spans,
    driver: &Driver,
) -> Option<Aimed> {
    let n = u64::try_from(j).expect("small") + 1;
    let env = sim.env(admin);
    let inner = env.clone();
    match action.kind {
        ActionKind::Split => {
            let candidates: Vec<(u64, &Span, Vec<u64>)> = spans
                .user()
                .map(|(range, span)| (range, span, split_keys(span)))
                .filter(|(_, _, keys)| !keys.is_empty())
                .collect();
            if candidates.is_empty() {
                lock(driver).splits_skipped += 1;
                return None;
            }
            let (range, span, keys) = &candidates
                [usize::try_from(action.range % candidates.len() as u64).expect("small")];
            let at = keys[usize::try_from(action.pick % keys.len() as u64).expect("small")];
            let target = raft::leader_of_range(sim, *range);
            {
                let mut stats = lock(driver);
                stats.splits_asked += 1;
                stats.outstanding += 1;
            }
            // PROPOSED(D-131): a burst starts as the action fires, and the split goes after
            // the burst's first `lead`.
            let burst = match action.load {
                Load::WriteBurst { count, every, lead } => Some((true, count, every, lead)),
                Load::ReadBurst { count, every, lead } => Some((false, count, every, lead)),
                Load::Clients | Load::StaleCache { .. } => None,
            };
            let delay = burst.map_or(Duration::ZERO, |(_, _, every, lead)| {
                every * u32::try_from(lead).expect("small")
            });
            env.spawn(
                "split-admin",
                split_admin(
                    inner,
                    n,
                    target,
                    *range,
                    span.generation,
                    key(at),
                    driver.clone(),
                    delay,
                ),
            );
            if let Some((writes, count, every, _)) = burst {
                let right: Vec<u64> = keys_in(span).into_iter().filter(|i| *i >= at).collect();
                let ops = burst_ops(n, &right, count, action.pick, writes);
                let env = sim.env(admin);
                let inner = env.clone();
                env.spawn(
                    "load-burst",
                    load_burst(inner, n, target, *range, span.generation, ops, every),
                );
            }
            Some(Aimed {
                parent: *range,
                generation: span.generation,
                cut: at,
            })
        }
        ActionKind::Transfer => {
            let ranges: Vec<(u64, &Span)> = spans.latest().iter().map(|(r, s)| (*r, s)).collect();
            if ranges.is_empty() {
                return None;
            }
            let (range, span) =
                ranges[usize::try_from(action.range % ranges.len() as u64).expect("small")];
            let leader = raft::leader_of_range(sim, range);
            let others: Vec<u64> = span
                .voters
                .iter()
                .copied()
                .filter(|voter| *voter != leader)
                .collect();
            let &to = others
                .get(usize::try_from(action.pick % others.len().max(1) as u64).expect("small"))?;
            {
                let mut stats = lock(driver);
                stats.transfers_asked += 1;
                stats.outstanding += 1;
            }
            env.spawn(
                "transfer-admin",
                transfer_admin(inner, n, leader, range, span.generation, to, driver.clone()),
            );
            None
        }
    }
}

/// The kinds of loss a refusal names (#45), read off its reason's words: the display of
/// `ananke_raft::store::LostState`, `ananke_storage::wal::HeadGap` and
/// `ananke_storage::engine::OpenRefused`. #45 asks three figures, dropped tables, lost log
/// records and fallback manifests; the other two are what a sweep finds beside them. One
/// refusal may name several.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Loss {
    /// A table the manifest listed could not be read.
    DroppedTable,
    /// Log records are gone: a missing or discarded head, a log stopped at a bad checksum
    /// or a gap, or a corrupt record skipped in a segment the tables cover.
    LostLogRecords,
    /// The manifest in force could not be read, or `CURRENT` that names it: fallen back
    /// from, or no intact one to fall back to.
    Manifest,
    /// The store's directory was damaged before the engine opened.
    Damaged,
    /// None of these words: a reason this list has not met, which the sweep fails on, so
    /// a new way to lose state is read and named, never counted as nothing.
    Other,
}

impl Loss {
    /// Every kind a refusal's reason names.
    #[must_use]
    pub fn of(reason: &str) -> Vec<Self> {
        let mut kinds = Vec::new();
        for (words, kind) in [
            ("dropped tables", Self::DroppedTable),
            ("the log's head is missing", Self::LostLogRecords),
            ("the log is damaged", Self::LostLogRecords),
            ("discarded a log head", Self::LostLogRecords),
            ("stopped reading the log", Self::LostLogRecords),
            ("skipped the rest of log segment", Self::LostLogRecords),
            ("fell back from manifest", Self::Manifest),
            ("no manifest older than", Self::Manifest),
            ("CURRENT", Self::Manifest),
            ("MANIFEST-", Self::Manifest),
            ("the store is damaged", Self::Damaged),
        ] {
            if reason.contains(words) && !kinds.contains(&kind) {
                kinds.push(kind);
            }
        }
        if kinds.is_empty() {
            kinds.push(Self::Other);
        }
        kinds
    }
}

/// One dropped table and the replicas it touches (#45): a table's bounds attribute its
/// loss to the ranges whose keys lie inside them, which is what per-range refusal would
/// need to be worth building.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DroppedTable {
    /// The node.
    pub node: u64,
    /// The table's number.
    pub number: u64,
    /// The replicas the node held when it was dropped.
    pub held: usize,
    /// How many of them have a key inside the table's bounds: in their Raft interval, or
    /// in their user span.
    pub touched: usize,
}

/// One final descriptor's wait for the meta range (SHARD.md §8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetaLag {
    /// The range.
    pub range: u64,
    /// Its generation where the run stopped.
    pub generation: u64,
    /// When meta was owed it: the last heal, or the descriptor's first statement if later.
    pub due: Instant,
    /// When meta came to name it over its whole span and went on naming it; `None` if it
    /// did not name it at the run's end.
    pub named: Option<Instant>,
}

impl MetaLag {
    /// How long meta took past when it was owed; zero where it named it by then.
    #[must_use]
    pub fn lag(&self) -> Option<Duration> {
        self.named
            .map(|named| named.checked_duration_since(self.due).unwrap_or_default())
    }
}

/// What meta names, by the first apply of each meta index, as `MetaApplied`'s won
/// sub-intervals say: keyed by end, each with its start, range and generation.
#[derive(Clone, Debug, Default)]
struct MetaNames {
    highest: Option<u64>,
    named: BTreeMap<Bytes, (Bytes, u64, u64)>,
}

impl MetaNames {
    /// Folds one `MetaApplied`; whether it changed anything.
    fn apply(&mut self, index: u64, descriptors: &[ananke_env::MetaDescriptor]) -> bool {
        if self.highest.is_some_and(|highest| index <= highest) {
            return false;
        }
        self.highest = Some(index);
        for descriptor in descriptors {
            for (start, end) in &descriptor.won {
                self.name(start, end, descriptor.range, descriptor.generation);
            }
        }
        true
    }

    fn name(&mut self, start: &Bytes, end: &Bytes, range: u64, generation: u64) {
        let overlapped: Vec<Bytes> = self
            .named
            .range::<[u8], _>((
                std::ops::Bound::Excluded(&start[..]),
                std::ops::Bound::Unbounded,
            ))
            .take_while(|(_, (from, ..))| from < end)
            .map(|(key, _)| key.clone())
            .collect();
        for key in overlapped {
            let (from, was, at) = self.named.remove(&key).expect("listed");
            if from < *start {
                self.named.insert(start.clone(), (from.clone(), was, at));
            }
            if key > *end {
                self.named.insert(key, (end.clone(), was, at));
            }
        }
        self.named
            .insert(end.clone(), (start.clone(), range, generation));
    }

    /// Whether meta names any key of `[start, end)` at `generation` or a newer one.
    fn holds_no_older(&self, start: &Bytes, end: &Bytes, generation: u64) -> bool {
        self.named
            .range::<[u8], _>((
                std::ops::Bound::Excluded(&start[..]),
                std::ops::Bound::Unbounded,
            ))
            .take_while(|(_, (from, ..))| from < end)
            .any(|(_, (.., at))| *at >= generation)
    }

    /// Whether meta names `[start, end)` wholly as `range` at `generation`.
    fn names(&self, start: &Bytes, end: &Bytes, range: u64, generation: u64) -> bool {
        let mut cursor = start.clone();
        for (to, (from, of, at)) in self.named.range::<[u8], _>((
            std::ops::Bound::Excluded(&start[..]),
            std::ops::Bound::Unbounded,
        )) {
            if *from > cursor || *of != range || *at != generation {
                return false;
            }
            cursor = to.clone();
            if cursor >= *end {
                return true;
            }
        }
        false
    }
}

/// Every dropped table in `records`, and how many of the replicas on its node it touches
/// (#45), from the spans the node held when the table was dropped: its Raft interval, or
/// its user span, lying within the table's bounds.
#[must_use]
pub fn dropped_tables_of(records: &[TraceRecord]) -> Vec<DroppedTable> {
    let mut held: BTreeMap<u64, BTreeMap<u64, (Bytes, Bytes)>> = BTreeMap::new();
    let mut tables = Vec::new();
    for record in records {
        let node = record.node.map_or(0, |node| u64::from(node.get()));
        match &record.event {
            TraceEvent::RangeCreated {
                range, start, end, ..
            }
            | TraceEvent::RangeDescriptor {
                range, start, end, ..
            } => {
                held.entry(node)
                    .or_default()
                    .insert(*range, (start.clone(), end.clone()));
            }
            TraceEvent::SstDropped {
                number,
                first_key,
                last_key,
                ..
            } => {
                let on = held.get(&node);
                let touched = on.map_or(0, |ranges| {
                    ranges
                        .iter()
                        .filter(|(range, (start, end))| {
                            let raft = KeyPrefix::group(**range).span();
                            (raft.start <= *last_key && *first_key < raft.end)
                                || (*start <= *last_key && *first_key < *end)
                        })
                        .count()
                });
                tables.push(DroppedTable {
                    node,
                    number: *number,
                    held: on.map_or(0, BTreeMap::len),
                    touched,
                });
            }
            TraceEvent::RaftRefused { .. } => {
                held.remove(&node);
            }
            _ => {}
        }
    }
    tables
}

/// When the meta range came to name each user range's descriptor at the end of `records`
/// (SHARD.md §8), by the first apply of each meta index, each owed from `last_heal` or from
/// its descriptor's first statement if that is later. Meta names a descriptor from the apply
/// after which it names it wholly and goes on naming it, so an update that overwrites it
/// later ends what came before.
#[must_use]
pub fn meta_lags_of(records: &[TraceRecord], last_heal: Instant) -> Vec<MetaLag> {
    let mut spans = Spans::default();
    spans.extend(records);
    let finals: Vec<(u64, Span)> = spans
        .user()
        .map(|(range, span)| (range, span.clone()))
        .collect();
    let mut names = MetaNames::default();
    let mut since: BTreeMap<u64, Option<Instant>> =
        finals.iter().map(|(range, _)| (*range, None)).collect();
    for record in records {
        let TraceEvent::MetaApplied { index, descriptors } = &record.event else {
            continue;
        };
        if !names.apply(*index, descriptors) {
            continue;
        }
        for (range, span) in &finals {
            let named = names.names(&span.start, &span.end, *range, span.generation);
            let at = since.get_mut(range).expect("listed");
            match (named, *at) {
                (true, None) => *at = Some(record.at),
                (false, Some(_)) => *at = None,
                _ => {}
            }
        }
    }
    finals
        .into_iter()
        .map(|(range, span)| MetaLag {
            range,
            generation: span.generation,
            due: last_heal.max(span.since),
            named: since.get(&range).copied().flatten(),
        })
        .collect()
}

/// Why a voter holding nothing of a right half can never be initialised by an apply: the second
/// half of what #220's owner ruling excuses from the meta convergence bound.
// PROPOSED(D-113): §6, #220.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stuck {
    /// Its replica of the parent reached the split by an install, at this index, so it never
    /// applies the split, and only the right half's leader could install the right half.
    ParentPastTheSplit(u64),
    /// It holds no replica of the parent, so there is nothing there to apply the split.
    NoParent,
}

/// What a right half's voters and leaders had come to where the trace ends: the three conditions
/// under which the meta convergence bound excuses the range ([`RightHalf::excused`]).
// PROPOSED(D-113): §6, #220.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RightHalf {
    /// The range it was split from, as the first `RangeSplit` naming it says.
    pub parent: u64,
    /// The split entry's index in the parent: the split point `s`.
    pub split: u64,
    /// The voters of its latest descriptor able to vote: each holds a replica on a store not
    /// refused since it was made, and is neither impaired nor quarantined.
    pub able: BTreeSet<u64>,
    /// The voters holding nothing of it that no apply can ever end, by why.
    pub stuck: BTreeMap<u64, Stuck>,
    /// Every other voter of it: impaired while holding a replica, or holding nothing where an
    /// apply could still end that.
    pub others: BTreeSet<u64>,
    /// How many `RaftLeader`s of it were traced at or after the heal the reading was given.
    pub led_since_heal: usize,
    /// The server that became its leader and had not left office where the trace ends.
    pub leader_at_end: Option<u64>,
}

impl RightHalf {
    /// Whether the meta convergence bound excuses the range (owner's ruling of 2026-11-09,
    /// #220): fewer than two voters able to vote, no leader traced since the last heal and none
    /// standing at the end, and every other voter a placeholder that no apply can ever end.
    /// Any one of them missing and the bound asks of the range as it asks of any other.
    #[must_use]
    pub fn excused(&self) -> bool {
        self.able.len() < 2
            && self.led_since_heal == 0
            && self.leader_at_end.is_none()
            && self.others.is_empty()
    }
}

/// Every right half of the run's latest descriptors, read for #220's exception to the meta
/// convergence bound. `heal` is the last heal and `impaired` is the raft report's
/// `impaired_voters`, the majority rule's own reading, which this adds to and does not change.
/// A range no `RangeSplit` names is no right half and has no entry here, so the bound asks of it.
/// A refusal ends everything the node held: its replicas, its applies and its installs.
// PROPOSED(D-113): §6, #220.
#[must_use]
pub fn right_halves_of(
    records: &[TraceRecord],
    heal: Instant,
    impaired: &BTreeMap<u64, BTreeSet<u64>>,
) -> BTreeMap<u64, RightHalf> {
    let mut spans = Spans::default();
    spans.extend(records);
    let mut held: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut applied_the_split: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut installed: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    let mut quarantined: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut split_of: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let mut leader: BTreeMap<u64, u64> = BTreeMap::new();
    let mut led_since: BTreeMap<u64, usize> = BTreeMap::new();
    for record in records {
        let node = record.node.map(|node| u64::from(node.get()));
        match &record.event {
            TraceEvent::RaftRefused { server, .. } => {
                held.retain(|(_, on)| on != server);
                applied_the_split.retain(|(_, on)| on != server);
                installed.retain(|(_, on), _| on != server);
                leader.retain(|_, by| by != server);
            }
            TraceEvent::RaftReseeded { server, range } => {
                quarantined.insert((*range, *server));
            }
            TraceEvent::RaftRecovered {
                server,
                range,
                state: RecoveredAs::Quarantined,
                ..
            } => {
                quarantined.insert((*range, *server));
            }
            TraceEvent::RaftSnapshot {
                server,
                range,
                last_index,
                taken: false,
                ..
            } => {
                let to = installed.entry((*range, *server)).or_default();
                *to = (*to).max(*last_index);
            }
            TraceEvent::RangeCreated { range, .. } => {
                if let Some(node) = node {
                    held.insert((*range, node));
                }
            }
            TraceEvent::RangeSplit {
                range,
                right,
                index,
                ..
            } => {
                split_of.entry(*right).or_insert((*range, *index));
                if let Some(node) = node {
                    applied_the_split.insert((*right, node));
                }
            }
            TraceEvent::RaftLeader { server, range, .. } => {
                leader.insert(*range, *server);
                if record.at >= heal {
                    *led_since.entry(*range).or_default() += 1;
                }
            }
            TraceEvent::RaftTerm {
                server,
                range,
                role,
                ..
            } if *role != "leader" && leader.get(range) == Some(server) => {
                leader.remove(range);
            }
            TraceEvent::NodeCrashed { node: gone } => {
                let gone = u64::from(gone.get());
                leader.retain(|_, by| *by != gone);
            }
            _ => {}
        }
    }
    let none = BTreeSet::new();
    spans
        .user()
        .filter_map(|(range, span)| {
            let &(parent, split) = split_of.get(&range)?;
            let hurt = impaired.get(&range).unwrap_or(&none);
            let mut half = RightHalf {
                parent,
                split,
                able: BTreeSet::new(),
                stuck: BTreeMap::new(),
                others: BTreeSet::new(),
                led_since_heal: led_since.get(&range).copied().unwrap_or(0),
                leader_at_end: leader.get(&range).copied(),
            };
            for &voter in &span.voters {
                let holds = held.contains(&(range, voter));
                let stuck = if holds || applied_the_split.contains(&(range, voter)) {
                    None
                } else if let Some(&to) = installed.get(&(parent, voter)).filter(|&&to| to >= split)
                {
                    Some(Stuck::ParentPastTheSplit(to))
                } else if held.contains(&(parent, voter)) {
                    None
                } else {
                    Some(Stuck::NoParent)
                };
                if holds && !hurt.contains(&voter) && !quarantined.contains(&(range, voter)) {
                    half.able.insert(voter);
                } else if let Some(stuck) = stuck {
                    half.stuck.insert(voter, stuck);
                } else {
                    half.others.insert(voter);
                }
            }
            Some((range, half))
        })
        .collect()
}

/// How many descriptors a split made, by the first apply of each meta index, won a key meta
/// already named at their generation or a newer one: an update still being resent after the
/// update it repeats had landed. SHARD.md §1's maximum by generation leaves such an update alone,
/// so on the correct meta range this is none. `MetaOverwritesByArrival` stores it, and the
/// update of a parent's old descriptor that arrives this way is what check 16 and the meta
/// convergence bound then see. Generation one is left out: before the first split every update
/// names it and the variant has no path (PROPOSED D-098).
// PROPOSED(D-113): §10's shape for `MetaOverwritesByArrival`, asserted at every tier (D-061).
#[must_use]
pub fn meta_resends_stored_of(records: &[TraceRecord]) -> usize {
    let mut names = MetaNames::default();
    let mut found = 0;
    for record in records {
        let TraceEvent::MetaApplied { index, descriptors } = &record.event else {
            continue;
        };
        if names.highest.is_none_or(|highest| *index > highest) {
            found += descriptors
                .iter()
                .filter(|descriptor| descriptor.generation > FIRST_GENERATION)
                .filter(|descriptor| {
                    descriptor
                        .won
                        .iter()
                        .any(|(start, end)| names.holds_no_older(start, end, descriptor.generation))
                })
                .count();
        }
        names.apply(*index, descriptors);
    }
    found
}

/// How many splits, each by its first `RangeSplit`, took their right half's id from a block
/// granted before its holder's latest restart: `IdBlockResumed`'s shape (SHARD.md §5's second
/// rule, §10). A new run of the correct node holds no id until a grant of its own lands, so
/// there it can only be a split proposed before the restart and first applied after it.
// PROPOSED(D-113): §10's shape for `IdBlockResumed`, asserted at every tier (D-061).
#[must_use]
pub fn splits_from_a_block_of_an_earlier_run_of(records: &[TraceRecord]) -> usize {
    // Each block by its first id: its last, its holder, whether the holder has restarted since.
    let mut blocks: BTreeMap<u64, (u64, u64, bool)> = BTreeMap::new();
    let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut found = 0;
    for record in records {
        match &record.event {
            TraceEvent::RangeIdsLeased {
                node, first, last, ..
            } => {
                blocks.entry(*first).or_insert((*last, *node, false));
            }
            TraceEvent::NodeRestarted { node } => {
                let node = u64::from(node.get());
                for (_, holder, earlier) in blocks.values_mut() {
                    *earlier |= *holder == node;
                }
            }
            TraceEvent::RangeSplit {
                range,
                right,
                index,
                ..
            } if seen.insert((*range, *index)) => {
                found += usize::from(
                    blocks
                        .range(..=*right)
                        .next_back()
                        .is_some_and(|(_, (last, _, earlier))| *earlier && *right <= *last),
                );
            }
            _ => {}
        }
    }
    found
}

/// What the workload's shapes reached on one run, each read off the trace and each something
/// the correct system does only because the shape was reached (SHARD.md §10; PROPOSED D-131).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Shapes {
    /// A burst's write the split's apply check refused: `RangeMismatchSent { at: Apply }` to a
    /// burst's client. The window of `ApplyIgnoresSpan`.
    pub writes_refused_at_apply: usize,
    /// A burst's get the check at serving refused: `RangeMismatchSent { at: Read }` to a burst's
    /// client. The window of `ReadCheckAtReceiptOnly`.
    pub gets_refused_at_serving: usize,
    /// The stale-cache reader's get of a key the writer had just written through the right
    /// half, refused by the parent at receipt. The window of `TrustStaleDescriptor`.
    pub stale_reads_refused: usize,
    /// A split the restart arm asked of its restarted node that took effect, proposed there.
    /// The window of `IdBlockResumed`.
    pub splits_from_a_restarted_node: usize,
    /// A sweep client's request answered `RangeMismatch` by a range that had split: the
    /// window of `ClientIgnoresMismatch`, §10's "any split with load".
    pub clients_mismatched_by_a_split: usize,
}

impl Shapes {
    /// Whether any shape's window was reached.
    #[must_use]
    pub fn reached_any(&self) -> bool {
        *self != Self::default()
    }
}

/// What the shapes reached on the trace `records` of a run whose restart arms are `arms`
/// ([`Shapes`]).
// PROPOSED(D-131)
#[must_use]
pub fn shapes_of(records: &[TraceRecord], arms: &[RestartArm]) -> Shapes {
    let mut shapes = Shapes::default();
    let is_load = |client: u64| client >> 32 == LOAD >> 32;
    let mut split: BTreeSet<u64> = BTreeSet::new();
    // Each restart arm's split that was proposed on its restarted node, as (range, index).
    let mut proposed: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut done: BTreeSet<(u64, u64)> = BTreeSet::new();
    for record in records {
        match &record.event {
            TraceEvent::RangeMismatchSent {
                range, client, at, ..
            } => {
                let client = *client;
                if is_load(client) {
                    match at {
                        MismatchAt::Apply if client & (WRITER | READER) == 0 => {
                            shapes.writes_refused_at_apply += 1;
                        }
                        MismatchAt::Read if client & (WRITER | READER) == 0 => {
                            shapes.gets_refused_at_serving += 1;
                        }
                        MismatchAt::Receipt if client & READER != 0 => {
                            shapes.stale_reads_refused += 1;
                        }
                        _ => {}
                    }
                } else if client >> 32 >= 1 && client >> 32 <= CLIENTS && split.contains(range) {
                    shapes.clients_mismatched_by_a_split += 1;
                }
            }
            TraceEvent::RaftProposed {
                server,
                range,
                client,
                seq,
                index,
                ..
            } if *client == DRIVER => {
                if arms
                    .iter()
                    .any(|arm| arm.victim == *server && arm.seq == Some(*seq % ASK_ANEW))
                {
                    proposed.insert((*range, *index));
                }
            }
            TraceEvent::RangeSplit { range, index, .. } => {
                split.insert(*range);
                if proposed.contains(&(*range, *index)) && done.insert((*range, *index)) {
                    shapes.splits_from_a_restarted_node += 1;
                }
            }
            _ => {}
        }
    }
    shapes
}

/// How many of the stale-cache reader's gets the parent answered with a value: under the
/// correct server none, which refuses every one at receipt, and under `TrustStaleDescriptor`
/// the shape reached (PROPOSED D-131).
// PROPOSED(D-131)
#[must_use]
pub fn stale_reads_served_of(records: &[TraceRecord]) -> usize {
    records
        .iter()
        .filter(|record| {
            matches!(record.event, TraceEvent::ClientReturn { client, .. }
                if client >> 32 == LOAD >> 32 && client & READER != 0)
        })
        .count()
}

impl Report {
    /// What the workload's shapes reached ([`Shapes`]).
    // PROPOSED(D-131)
    #[must_use]
    pub fn shapes(&self) -> Shapes {
        shapes_of(self.records(), &self.restart_arms)
    }

    /// The trace as records.
    #[must_use]
    pub fn records(&self) -> &[TraceRecord] {
        &self.checked.records
    }

    /// The trace as moirae JSONL.
    #[must_use]
    pub fn jsonl(&self) -> String {
        self.checked.jsonl()
    }

    /// Every range's latest descriptor at the run's end.
    #[must_use]
    pub fn spans(&self) -> Spans {
        let mut spans = Spans::default();
        spans.extend(self.records());
        spans
    }

    /// The splits that took effect, by the first `RangeSplit` of each.
    #[must_use]
    pub fn splits(&self) -> usize {
        let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
        self.records()
            .iter()
            .filter(|record| {
                matches!(&record.event, TraceEvent::RangeSplit { range, right, .. }
                    if seen.insert((*range, *right)))
            })
            .count()
    }

    /// The splits that took effect of a range that was itself born of a split: a
    /// second split of one lineage.
    #[must_use]
    pub fn splits_of_halves(&self) -> usize {
        let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut born: BTreeSet<u64> = BTreeSet::new();
        let mut count = 0;
        for record in self.records() {
            if let TraceEvent::RangeSplit { range, right, .. } = &record.event
                && seen.insert((*range, *right))
            {
                if born.contains(range) || *range != FIRST_RANGE {
                    count += 1;
                }
                born.insert(*right);
            }
        }
        count
    }

    /// The refusals the run reached, as (node, reason).
    #[must_use]
    pub fn refusals(&self) -> Vec<(u64, String)> {
        self.checked.refused.clone()
    }

    /// The refusals counted by the kinds of loss their reasons name (#45).
    #[must_use]
    pub fn refusals_by_loss(&self) -> BTreeMap<Loss, usize> {
        let mut by: BTreeMap<Loss, usize> = BTreeMap::new();
        for (_, reason) in self.refusals() {
            for kind in Loss::of(&reason) {
                *by.entry(kind).or_default() += 1;
            }
        }
        by
    }

    /// Every dropped table the trace names ([`dropped_tables_of`]).
    #[must_use]
    pub fn dropped_tables(&self) -> Vec<DroppedTable> {
        dropped_tables_of(self.records())
    }

    /// When the meta range came to name each user range's descriptor at the run's end
    /// ([`meta_lags_of`]).
    #[must_use]
    pub fn meta_lags(&self) -> Vec<MetaLag> {
        meta_lags_of(self.records(), self.checked.last_heal)
    }

    /// Every right half at the run's end, read for #220's exception ([`right_halves_of`]). "The
    /// last heal" is the last fault's, the earlier of the two this report holds, so a leader
    /// traced in the driver's drain counts against the exception.
    // PROPOSED(D-113): §6, #220.
    #[must_use]
    pub fn right_halves(&self) -> BTreeMap<u64, RightHalf> {
        right_halves_of(
            self.records(),
            self.last_fault_healed,
            &self.checked.impaired_voters(),
        )
    }

    /// The lags of §8's asked set, before #220's exception: on a uniformly scheduled seed
    /// (D-016), with the meta range's unimpaired replicas a majority and the range's too,
    /// since each needs a leader to send or take the update.
    #[must_use]
    pub fn meta_lags_of_live_ranges(&self) -> Vec<MetaLag> {
        if !self.checked.uniform() {
            return Vec::new();
        }
        let live = self.checked.ranges_with_a_majority_up();
        if !live.contains(&META_RANGE) {
            return Vec::new();
        }
        self.meta_lags()
            .into_iter()
            .filter(|lag| live.contains(&lag.range))
            .collect()
    }

    /// The lags the meta convergence bound is asked of: [`Report::meta_lags_of_live_ranges`]
    /// less the ones #220's exception excuses ([`RightHalf::excused`]). The exception is this
    /// bound's alone; the majority rule means what it meant to every other check.
    // PROPOSED(D-113): §6, #220.
    #[must_use]
    pub fn meta_lags_asked(&self) -> Vec<MetaLag> {
        let halves = self.right_halves();
        self.meta_lags_of_live_ranges()
            .into_iter()
            .filter(|lag| !halves.get(&lag.range).is_some_and(RightHalf::excused))
            .collect()
    }

    /// The lags #220's exception excuses from the bound: what the sweep prints.
    // PROPOSED(D-113): §6, #220.
    #[must_use]
    pub fn meta_lags_excused(&self) -> Vec<MetaLag> {
        let halves = self.right_halves();
        self.meta_lags_of_live_ranges()
            .into_iter()
            .filter(|lag| halves.get(&lag.range).is_some_and(RightHalf::excused))
            .collect()
    }

    /// The meta convergence bound (SHARD.md §8): after the last heal, meta names every
    /// key's current descriptor within [`meta_convergence_bound`], for every range of
    /// [`Report::meta_lags_asked`].
    ///
    /// # Errors
    ///
    /// The first range meta names late or not at all.
    pub fn meta_converges(&self) -> Result<(), String> {
        self.meta_converges_over(self.meta_lags_asked())
    }

    /// [`Report::meta_converges`] with #220's exception taken out: what it would have said of
    /// a right half it excuses. No check calls this; the pins and the plants read it.
    ///
    /// # Errors
    ///
    /// The first live range meta names late or not at all.
    // PROPOSED(D-113): §6, #220.
    pub fn meta_converges_without_the_exception(&self) -> Result<(), String> {
        self.meta_converges_over(self.meta_lags_of_live_ranges())
    }

    fn meta_converges_over(&self, lags: Vec<MetaLag>) -> Result<(), String> {
        let seed = self.seed;
        let bound = meta_convergence_bound();
        for lag in lags {
            match lag.lag() {
                Some(took) if took <= bound => {}
                Some(took) => {
                    return Err(format!(
                        "seed {seed}: meta convergence: meta came to name range {} at generation \
                         {} {took:?} after it was owed at {:?}, over {bound:?}",
                        lag.range, lag.generation, lag.due
                    ));
                }
                None => {
                    return Err(format!(
                        "seed {seed}: meta convergence: meta does not name range {} at \
                         generation {} where the run stopped, owed since {:?}",
                        lag.range, lag.generation, lag.due
                    ));
                }
            }
        }
        Ok(())
    }

    /// The askers [`Report::retries_take_effect`] asks of: on a uniformly scheduled seed whose
    /// ranges 0 and the asked range have a majority, each still asking when the last fault
    /// healed.
    #[must_use]
    pub fn retries_asked(&self) -> Vec<Retried> {
        if !self.checked.uniform() {
            return Vec::new();
        }
        let live = self.checked.ranges_with_a_majority_up();
        if !live.contains(&ROOT_RANGE) {
            return Vec::new();
        }
        self.driver
            .retried
            .iter()
            .filter(|retried| {
                live.contains(&retried.range) && retried.asking_at(self.last_fault_healed)
            })
            .copied()
            .collect()
    }

    /// D-092's assertion where a split was refused for want of a range id, per seed: each of
    /// [`Report::retries_asked`] ends on an answer, and within the write bound of the last
    /// fault's heal ([`Retried::took_no_effect`]).
    ///
    /// An answer is any but `NoRangeId`, so one that refuses the split for another reason passes.
    ///
    /// As first written this check could not fail, and the fix changed two things (#216's review,
    /// B6). The origin: it read `checked.last_heal`, which opens the liveness window past the
    /// driver's drain, so an asker the drain waited for had ended before it and nothing could be
    /// late of it; it now reads [`Report::last_fault_healed`]. The predicate: it asked only that
    /// an asker with no answer be over the bound, and an asker still asking at the heal now has
    /// to end on an answer, however soon it stops. Neither alone is enough: of the 382 seeds of
    /// a thousand that `RefillNotResent` trips by the two together, the new origin with the old
    /// predicate trips 101, and the new predicate with the old origin 96.
    ///
    /// # Errors
    ///
    /// The first asker whose retries took no effect.
    // PROPOSED(D-092): answer A; the asker retries.
    // PROPOSED(D-113): read from the heal, §8.
    pub fn retries_take_effect(&self) -> Result<(), String> {
        let seed = self.seed;
        let bound = election_max() * LIVENESS_TIMEOUTS;
        for retried in self.retries_asked() {
            if let Some(why) = retried.took_no_effect(self.last_fault_healed, bound) {
                return Err(format!("seed {seed}: NoRangeId: {why}"));
            }
        }
        Ok(())
    }

    /// Every `NoRangeId` of the run, read against the trace (PROPOSED D-092's figures).
    #[must_use]
    pub fn no_range_ids(&self) -> Vec<NoRangeId> {
        self.driver
            .no_range_id
            .iter()
            .map(|&(node, at)| {
                let mut restarted = None;
                let mut leased_since_restart = false;
                let mut leased_within = false;
                let mut taken = 0usize;
                let mut block: Option<(u64, u64)> = None;
                let mut seen: BTreeSet<u64> = BTreeSet::new();
                for record in self.records() {
                    match &record.event {
                        TraceEvent::NodeRestarted { node: of } if u64::from(of.get()) == node => {
                            if record.at <= at {
                                restarted = Some(record.at);
                                leased_since_restart = false;
                                taken = 0;
                                block = None;
                            }
                        }
                        TraceEvent::RangeIdsLeased {
                            node: of,
                            first,
                            last,
                            ..
                        } if *of == node => {
                            if record.at <= at {
                                leased_since_restart = true;
                                block = Some((*first, *last));
                            } else if record.at.duration_since(at) <= raft::ELECTION_MIN {
                                leased_within = true;
                            }
                        }
                        TraceEvent::RangeSplit { right, .. }
                            if record.at <= at
                                && seen.insert(*right)
                                && block.is_some_and(|(first, last)| {
                                    first <= *right && *right <= last
                                }) =>
                        {
                            taken += 1;
                        }
                        _ => {}
                    }
                }
                NoRangeId {
                    node,
                    at,
                    restarted,
                    leased_since_restart,
                    leased_within,
                    taken,
                }
            })
            .collect()
    }

    /// The most range ids one block granted over the run gave to splits that took effect:
    /// how near a run comes to spending a block ([`ID_BLOCK`]).
    #[must_use]
    pub fn most_ids_from_one_block(&self) -> usize {
        let mut blocks: Vec<(u64, u64, usize)> = Vec::new();
        let mut seen: BTreeSet<u64> = BTreeSet::new();
        for record in self.records() {
            match &record.event {
                TraceEvent::RangeIdsLeased { first, last, .. }
                    if !blocks.iter().any(|(f, ..)| f == first) =>
                {
                    blocks.push((*first, *last, 0));
                }
                TraceEvent::RangeSplit { right, .. } if seen.insert(*right) => {
                    if let Some(block) = blocks
                        .iter_mut()
                        .find(|(first, last, _)| first <= right && right <= last)
                    {
                        block.2 += 1;
                    }
                }
                _ => {}
            }
        }
        blocks.iter().map(|(.., taken)| *taken).max().unwrap_or(0)
    }

    /// Every refusal's reason names a loss this file knows (#45): one that names none is a
    /// new way to lose state, and is read and named rather than counted as nothing.
    ///
    /// # Errors
    ///
    /// The first reason [`Loss::of`] does not read.
    pub fn losses_are_named(&self) -> Result<(), String> {
        let seed = self.seed;
        for (node, reason) in self.refusals() {
            if Loss::of(&reason).contains(&Loss::Other) {
                return Err(format!(
                    "seed {seed}: node {node}'s refusal names a loss #45's kinds do not: {reason}"
                ));
            }
        }
        Ok(())
    }

    /// The shape, per seed: every bootstrap node created ranges 0, 1 and 2 at its first
    /// start, and nodes 4 and 5 created nothing, not even a placeholder (SHARD.md §2,
    /// §12's Stage C; Q22).
    ///
    /// # Errors
    ///
    /// What the run did otherwise.
    pub fn shape(&self) -> Result<(), String> {
        let seed = self.seed;
        let mut bootstrapped: BTreeSet<(u64, u64)> = BTreeSet::new();
        for record in self.records() {
            let node = record.node.map_or(0, |node| u64::from(node.get()));
            match &record.event {
                TraceEvent::RangeCreated { range, cause, .. } => {
                    if node > BOOTSTRAP {
                        return Err(format!(
                            "seed {seed}: node {node}, which joins and holds nothing in Stage \
                             C, created a replica of range {range} by {cause:?}"
                        ));
                    }
                    if *cause == RangeCause::Bootstrap {
                        bootstrapped.insert((node, *range));
                    }
                }
                TraceEvent::RangeReplicaCreated { range } if node > BOOTSTRAP => {
                    return Err(format!(
                        "seed {seed}: node {node}, which joins and holds nothing in Stage C, \
                         made a placeholder of range {range}: no leader sends it anything"
                    ));
                }
                _ => {}
            }
        }
        for node in 1..=BOOTSTRAP {
            for range in [ROOT_RANGE, META_RANGE, FIRST_RANGE] {
                if !bootstrapped.contains(&(node, range)) {
                    return Err(format!(
                        "seed {seed}: bootstrap node {node} traced no bootstrap creation of \
                         range {range}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The run's verdict: the raft sweep's checks over this trace, its write bound per
    /// range the run ended with (`raft::Report::check_by_final_range`), then the shape, the
    /// meta convergence bound, D-092's retries and #45's kinds.
    ///
    /// # Errors
    ///
    /// The first violation, naming the seed.
    pub fn check(&self) -> Result<(), String> {
        self.checked.check_by_final_range()?;
        self.shape()?;
        if self.checked.stopped.is_none() {
            self.meta_converges()?;
            self.retries_take_effect()?;
        }
        self.losses_are_named()
    }
}

#[cfg(test)]
mod tests {
    //! The readings' reasoning on records written by hand, one shape each.

    use ananke_env::MetaDescriptor;

    use super::*;

    fn at(ms: u64) -> Instant {
        Instant::from_nanos(ms * 1_000_000)
    }

    fn record(ms: u64, node: u32, event: TraceEvent) -> TraceRecord {
        TraceRecord {
            at: at(ms),
            decided: at(ms),
            node: Some(NodeId::new(node)),
            event,
        }
    }

    fn descriptor(ms: u64, range: u64, span: (&str, &str), generation: u64) -> TraceRecord {
        record(
            ms,
            1,
            TraceEvent::RangeDescriptor {
                range,
                index: ms,
                applied: ms,
                start: Bytes::copy_from_slice(span.0.as_bytes()),
                end: Bytes::copy_from_slice(span.1.as_bytes()),
                generation,
                voters: vec![1, 2, 3],
                state: RangeState::Live,
            },
        )
    }

    fn meta(ms: u64, index: u64, range: u64, span: (&str, &str), generation: u64) -> TraceRecord {
        let (start, end) = (
            Bytes::copy_from_slice(span.0.as_bytes()),
            Bytes::copy_from_slice(span.1.as_bytes()),
        );
        record(
            ms,
            1,
            TraceEvent::MetaApplied {
                index,
                descriptors: vec![MetaDescriptor {
                    range,
                    start: start.clone(),
                    end: end.clone(),
                    generation,
                    voters: vec![1, 2, 3],
                    won: vec![(start, end)],
                }],
            },
        )
    }

    /// D-092's asker read from the last fault's heal, on what #216's review found the check
    /// could not say when it was read from the liveness window's start.
    // PROPOSED(D-113): read from the heal, §8.
    #[test]
    fn an_askers_retries_are_asked_of_from_the_heal() {
        let (heal, bound) = (at(5000), Duration::from_secs(2));
        let asker = |refused, ended, answered| Retried {
            range: 2,
            refused: at(refused),
            ended: at(ended),
            answered,
        };
        let no_effect = |of: Retried| of.took_no_effect(heal, bound);
        assert_eq!(no_effect(asker(4900, 5300, true)), None, "answered in time");
        assert_eq!(
            no_effect(asker(4900, 7000, true)),
            None,
            "the bound is inclusive"
        );
        // An asker that stopped before the heal made no retry after it, however it ended.
        assert_eq!(no_effect(asker(3000, 4900, false)), None);
        assert_eq!(no_effect(asker(3000, 4900, true)), None);
        // Still asking at the heal, and it stopped with no answer: the check's whole point.
        let why = no_effect(asker(4900, 5300, false)).expect("no answer");
        assert!(why.contains("with no answer"), "{why}");
        // Refused only after the heal, and it never got one.
        let why = no_effect(asker(5500, 5900, false)).expect("no answer");
        assert!(why.contains("with no answer"), "{why}");
        // Answered too late: from the heal for one refused before it...
        let why = no_effect(asker(4900, 7001, true)).expect("over the bound");
        assert!(why.contains("over 2s"), "{why}");
        // ...and from its refusal for one refused after it.
        assert_eq!(no_effect(asker(6000, 7900, true)), None);
        let why = no_effect(asker(6000, 8001, true)).expect("over the bound");
        assert!(why.contains("over 2s"), "{why}");
    }

    /// Range 2 over `[a, z)` split at `m`: 19 is `[m, z)`, both at generation 2.
    fn split() -> Vec<TraceRecord> {
        vec![
            descriptor(1, 2, ("a", "z"), 1),
            meta(1, 0, 2, ("a", "z"), 1),
            descriptor(100, 2, ("a", "m"), 2),
            descriptor(100, 19, ("m", "z"), 2),
        ]
    }

    fn lag_of(lags: &[MetaLag], range: u64) -> Option<Duration> {
        lags.iter()
            .find(|lag| lag.range == range)
            .expect("a lag")
            .lag()
    }

    #[test]
    fn meta_names_a_descriptor_from_the_apply_that_covers_its_whole_span() {
        let mut records = split();
        records.extend([
            meta(110, 1, 2, ("a", "m"), 2),
            meta(150, 2, 19, ("m", "z"), 2),
        ]);
        let lags = meta_lags_of(&records, at(120));
        // Named before it was owed is no lag; named after it is the difference.
        assert_eq!(lag_of(&lags, 2), Some(Duration::ZERO));
        assert_eq!(lag_of(&lags, 19), Some(Duration::from_millis(30)));
        // Owed from the descriptor's first statement where that is after the heal.
        let lags = meta_lags_of(&records, at(50));
        assert_eq!(lag_of(&lags, 19), Some(Duration::from_millis(50)));
    }

    #[test]
    fn meta_that_names_part_of_a_span_or_an_older_generation_does_not_name_it() {
        let mut records = split();
        records.push(meta(110, 1, 19, ("m", "t"), 2));
        assert_eq!(
            lag_of(&meta_lags_of(&records, at(100)), 19),
            None,
            "part of it"
        );
        let mut records = split();
        records.push(meta(110, 1, 19, ("m", "z"), 1));
        assert_eq!(lag_of(&meta_lags_of(&records, at(100)), 19), None, "older");
    }

    /// What `MetaOverwritesByArrival` does to meta: an update that wins a span a newer
    /// record held ends the naming, so the descriptor reads as not named where the run stops.
    #[test]
    fn an_update_that_overwrites_a_named_descriptor_ends_its_naming() {
        let mut records = split();
        records.extend([
            meta(110, 1, 2, ("a", "m"), 2),
            meta(120, 2, 19, ("m", "z"), 2),
            meta(130, 3, 2, ("a", "z"), 1),
        ]);
        let lags = meta_lags_of(&records, at(100));
        assert_eq!(lag_of(&lags, 2), None);
        assert_eq!(lag_of(&lags, 19), None);
        // And a later resend of the right update names it from there.
        records.push(meta(140, 4, 19, ("m", "z"), 2));
        assert_eq!(
            lag_of(&meta_lags_of(&records, at(100)), 19),
            Some(Duration::from_millis(40))
        );
    }

    /// A replica behind, applying an index already folded, moves nothing: first apply of
    /// each index, as check 16's fold reads it.
    #[test]
    fn a_lagging_replicas_apply_of_an_index_already_folded_moves_nothing() {
        let mut records = split();
        records.extend([
            meta(110, 1, 2, ("a", "m"), 2),
            meta(120, 2, 19, ("m", "z"), 2),
            meta(130, 0, 2, ("a", "z"), 1),
        ]);
        let lags = meta_lags_of(&records, at(100));
        assert_eq!(lag_of(&lags, 2), Some(Duration::from_millis(10)));
        assert_eq!(lag_of(&lags, 19), Some(Duration::from_millis(20)));
    }

    // --- #220's exception to the meta convergence bound, one condition each. Range 2 splits at
    // index 50 into 19, and the heal is at 1 000 ms.

    const PARENT: u64 = 2;
    const SPLIT_AT: u64 = 50;
    const RIGHT: u64 = 19;
    const HEAL: u64 = 1000;

    fn created(ms: u64, node: u32, range: u64) -> TraceRecord {
        record(
            ms,
            node,
            TraceEvent::RangeCreated {
                range,
                cause: RangeCause::Split,
                parent: None,
                start: Bytes::from_static(b"m"),
                end: Bytes::from_static(b"z"),
                generation: 2,
                voters: vec![1, 2, 3],
                floor_index: 0,
                floor_term: 0,
                incarnation: 1,
            },
        )
    }

    fn applied_the_split(ms: u64, node: u32) -> TraceRecord {
        record(
            ms,
            node,
            TraceEvent::RangeSplit {
                range: PARENT,
                right: RIGHT,
                key: Bytes::from_static(b"m"),
                index: SPLIT_AT,
            },
        )
    }

    fn installed(ms: u64, node: u32, range: u64, last_index: u64) -> TraceRecord {
        record(
            ms,
            node,
            TraceEvent::RaftSnapshot {
                server: u64::from(node),
                range,
                last_index,
                last_term: 1,
                taken: false,
            },
        )
    }

    fn led(ms: u64, node: u32) -> TraceRecord {
        record(
            ms,
            node,
            TraceEvent::RaftLeader {
                server: u64::from(node),
                range: RIGHT,
                term: 2,
                last_index: 60,
            },
        )
    }

    fn stepped_down(ms: u64, node: u32) -> TraceRecord {
        record(
            ms,
            node,
            TraceEvent::RaftTerm {
                server: u64::from(node),
                range: RIGHT,
                term: 3,
                role: "follower",
                received: None,
            },
        )
    }

    fn refused(ms: u64, node: u32) -> TraceRecord {
        record(
            ms,
            node,
            TraceEvent::RaftRefused {
                server: u64::from(node),
                reason: "dropped tables [4]".to_owned(),
            },
        )
    }

    /// The parent on every node; node 2 applied the split and alone holds the right half; and
    /// nodes 1 and 3 reached the split by an install, so they never apply it.
    fn wedged() -> Vec<TraceRecord> {
        vec![
            created(1, 1, PARENT),
            created(1, 2, PARENT),
            created(1, 3, PARENT),
            applied_the_split(10, 2),
            created(10, 2, RIGHT),
            installed(20, 1, PARENT, 60),
            installed(20, 3, PARENT, 60),
        ]
    }

    fn read_impaired(
        records: &[TraceRecord],
        impaired: &BTreeMap<u64, BTreeSet<u64>>,
    ) -> RightHalf {
        right_halves_of(records, at(HEAL), impaired)
            .remove(&RIGHT)
            .expect("a right half")
    }

    fn read(records: &[TraceRecord]) -> RightHalf {
        read_impaired(records, &BTreeMap::new())
    }

    fn past(to: u64) -> Stuck {
        Stuck::ParentPastTheSplit(to)
    }

    /// The shape the owner excused, each of its parts read: without this case the ones below
    /// could pass on a fold that excuses nothing.
    #[test]
    fn a_right_half_with_no_possible_majority_is_excused() {
        let half = read(&wedged());
        assert_eq!((half.parent, half.split), (PARENT, SPLIT_AT));
        assert_eq!(half.able, BTreeSet::from([2]));
        assert_eq!(half.stuck, BTreeMap::from([(1, past(60)), (3, past(60))]));
        assert!(half.others.is_empty());
        assert_eq!((half.led_since_heal, half.leader_at_end), (0, None));
        assert!(half.excused());
    }

    /// A child of a wedged parent: its voters hold no replica of the parent, so nothing there can
    /// apply the split that would initialise them. The install exactly at the split counts, and
    /// an install below it does not.
    #[test]
    fn a_placeholder_that_holds_no_parent_or_a_parent_at_the_split_by_an_install_is_stuck() {
        let records = [
            created(1, 2, PARENT),
            applied_the_split(10, 2),
            created(10, 2, RIGHT),
        ];
        let half = read(&records);
        assert_eq!(
            half.stuck,
            BTreeMap::from([(1, Stuck::NoParent), (3, Stuck::NoParent)])
        );
        assert!(half.excused());

        let mut records = wedged();
        records[5] = installed(20, 1, PARENT, SPLIT_AT);
        assert_eq!(read(&records).stuck[&1], past(SPLIT_AT));
        assert!(read(&records).excused());
        records[5] = installed(20, 1, PARENT, SPLIT_AT - 1);
        let half = read(&records);
        assert_eq!(half.others, BTreeSet::from([1]), "it will apply the split");
        assert!(!half.excused());
    }

    /// Condition two's first half. A leader before the heal whose office is over leaves the range
    /// excused, as seed 2272's did; one traced at the heal or after it does not.
    #[test]
    fn a_range_led_since_the_last_heal_is_not_excused() {
        let mut records = wedged();
        records.extend([led(HEAL - 1, 3), stepped_down(HEAL - 1, 3)]);
        assert!(read(&records).excused(), "led before the heal and gone");
        records.extend([led(HEAL, 2), stepped_down(HEAL + 5, 2)]);
        let half = read(&records);
        assert_eq!(
            (half.led_since_heal, half.leader_at_end),
            (1, None),
            "led at the heal itself, and no leader stands at the end"
        );
        assert!(!half.excused());
    }

    /// Condition two's second half: a leader elected before the heal and never gone, and each of
    /// the three things that end its office.
    #[test]
    fn a_range_with_a_leader_standing_at_the_end_is_not_excused() {
        let mut records = wedged();
        records.push(led(HEAL - 100, 2));
        let half = read(&records);
        assert_eq!((half.led_since_heal, half.leader_at_end), (0, Some(2)));
        assert!(!half.excused());
        let crashed = record(
            HEAL - 50,
            2,
            TraceEvent::NodeCrashed {
                node: NodeId::new(2),
            },
        );
        for (what, ended) in [
            ("its step down", stepped_down(HEAL - 50, 2)),
            ("its crash", crashed),
        ] {
            let mut gone = records.clone();
            gone.push(ended);
            assert_eq!(read(&gone).leader_at_end, None, "{what}");
            assert!(read(&gone).excused(), "{what}");
        }
        let mut gone = records.clone();
        gone.push(refused(HEAL - 50, 2));
        assert_eq!(read(&gone).leader_at_end, None, "its node's refusal");
        // Another server's step down ends nothing of it.
        let mut other = records.clone();
        other.push(stepped_down(HEAL - 50, 3));
        assert_eq!(read(&other).leader_at_end, Some(2));
    }

    /// Condition three, on "only node 1 creates R"'s shape: a voter whose parent applied the split
    /// and made no replica is a placeholder an apply should have ended, which is a bug and not
    /// the wedge, and the plain form of the exception hid it.
    #[test]
    fn a_placeholder_whose_parent_applied_the_split_is_not_excused() {
        let mut records = wedged();
        records.remove(5);
        let half = read(&records);
        assert_eq!(
            half.others,
            BTreeSet::from([1]),
            "its parent has no install"
        );
        assert_eq!(half.stuck, BTreeMap::from([(3, past(60))]));
        assert!(!half.excused());
        // And one that traced the split without an install of its parent, and one with both.
        let mut records = wedged();
        records.push(applied_the_split(15, 3));
        let half = read(&records);
        assert_eq!(half.others, BTreeSet::from([3]), "it applied the split");
        assert!(!half.excused());
    }

    /// Condition one: two voters able to vote can elect. And a replica that holds the range
    /// and cannot vote is not a placeholder, so it is not one of what condition three names.
    #[test]
    fn a_range_with_two_voters_able_to_vote_is_not_excused() {
        let mut records = wedged();
        records.push(created(30, 1, RIGHT));
        let half = read(&records);
        assert_eq!(half.able, BTreeSet::from([1, 2]));
        assert_eq!(half.stuck, BTreeMap::from([(3, past(60))]));
        assert!(!half.excused());

        let impaired = BTreeMap::from([(RIGHT, BTreeSet::from([1]))]);
        let half = read_impaired(&records, &impaired);
        assert_eq!(half.able, BTreeSet::from([2]));
        assert_eq!(half.others, BTreeSet::from([1]), "impaired, holding it");
        assert!(!half.excused());

        records.push(record(
            31,
            1,
            TraceEvent::RaftReseeded {
                server: 1,
                range: RIGHT,
            },
        ));
        let half = read(&records);
        assert_eq!(half.others, BTreeSet::from([1]), "quarantined, holding it");
        assert!(!half.excused());
    }

    /// A replica stated quarantined by `RaftRecovered` alone counts as quarantined, as one
    /// `RaftReseeded` names does: a restart states it that way, and with the arm gone it read
    /// as able to vote. The state the arm reads is the point, so a replica recovered with none
    /// stays able (#223).
    #[test]
    fn a_right_half_stated_quarantined_at_a_recovery_cannot_vote() {
        let recovered = |state| {
            record(
                31,
                1,
                TraceEvent::RaftRecovered {
                    server: 1,
                    range: RIGHT,
                    term: 1,
                    applied: SPLIT_AT,
                    last_index: SPLIT_AT,
                    incarnation: 2,
                    state,
                },
            )
        };
        let mut records = wedged();
        records.extend([created(30, 1, RIGHT), recovered(RecoveredAs::Neither)]);
        assert_eq!(read(&records).able, BTreeSet::from([1, 2]), "no quarantine");

        let mut records = wedged();
        records.extend([created(30, 1, RIGHT), recovered(RecoveredAs::Quarantined)]);
        let half = read(&records);
        assert_eq!(half.able, BTreeSet::from([2]), "quarantined, and not able");
        assert_eq!(half.others, BTreeSet::from([1]), "and it holds the range");
        assert!(!half.excused());
    }

    /// A run's report with its trace replaced by `records`: the last fault healed at [`HEAL`],
    /// and the liveness window opening `later` ms past it, as the driver's drain leaves it.
    fn report_over(records: Vec<TraceRecord>, later: u64) -> Report {
        let mut report = run(0);
        report.last_fault_healed = at(HEAL);
        report.checked.last_heal = at(HEAL + later);
        report.checked.records = records;
        report
    }

    /// `right_halves` reads the last fault's heal and not the liveness window's start, which
    /// the driver's drain leaves later: a leader traced in between counts against the
    /// exception, and reading the window's start instead would have excused it (#223).
    #[test]
    fn right_halves_read_the_last_faults_heal_and_not_the_liveness_windows_start() {
        let mut records = wedged();
        records.extend([led(HEAL + 100, 2), stepped_down(HEAL + 200, 2)]);
        let report = report_over(records, 500);
        let halves = report.right_halves();
        let half = &halves[&RIGHT];
        assert_eq!(half.led_since_heal, 1, "led in the drain, past the heal");
        assert!(!half.excused());
        let impaired = report.checked.impaired_voters();
        let from_the_window =
            right_halves_of(report.records(), report.checked.last_heal, &impaired);
        assert_eq!(from_the_window[&RIGHT].led_since_heal, 0);
        assert!(
            from_the_window[&RIGHT].excused(),
            "the wider reading excuses what this one does not"
        );
    }

    /// `right_halves` hands the fold the majority rule's own impaired voters: a replica the run
    /// refused holds the range and is not able to vote, and with an empty map it read as able
    /// (#223).
    #[test]
    fn right_halves_read_the_majority_rules_impaired_voters() {
        let mut records = wedged();
        records.extend([
            created(30, 1, RIGHT),
            record(
                31,
                1,
                TraceEvent::RaftReplicaRefused {
                    server: 1,
                    range: RIGHT,
                },
            ),
        ]);
        let report = report_over(records, 0);
        assert_eq!(
            report.checked.impaired_voters()[&RIGHT],
            BTreeSet::from([1]),
            "the rule reads node 1 as impaired"
        );
        let halves = report.right_halves();
        assert_eq!(halves[&RIGHT].able, BTreeSet::from([2]), "not able to vote");
        assert_eq!(
            halves[&RIGHT].others,
            BTreeSet::from([1]),
            "impaired, holding it"
        );
        assert!(
            right_halves_of(report.records(), at(HEAL), &BTreeMap::new())[&RIGHT]
                .able
                .contains(&1),
            "the fold with an empty map reads it as able"
        );
    }

    /// A refusal ends what its node held, so it is its later history that counts. Node 1 loses
    /// its replicas and with them the install: it then holds no parent until one is made.
    #[test]
    fn a_refused_voter_reads_from_what_it_has_held_since() {
        let mut records = wedged();
        records.push(refused(40, 1));
        assert_eq!(read(&records).stuck[&1], Stuck::NoParent);
        records.push(created(50, 1, PARENT));
        let half = read(&records);
        assert_eq!(half.others, BTreeSet::from([1]), "a parent, and no install");
        records.push(installed(60, 1, PARENT, 70));
        assert_eq!(read(&records).stuck[&1], past(70));

        let mut records = wedged();
        records.push(refused(40, 2));
        let half = read(&records);
        assert_eq!(half.able, BTreeSet::new(), "the replica it held is gone");
        assert_eq!(half.stuck[&2], Stuck::NoParent);
    }

    /// Only a right half can be excused: a range no split made has no entry to excuse it by.
    #[test]
    fn a_range_no_split_named_has_no_reading() {
        let mut records = wedged();
        records.push(descriptor(30, 7, ("a", "m"), 2));
        let halves = right_halves_of(&records, at(HEAL), &BTreeMap::new());
        assert!(halves.contains_key(&RIGHT));
        assert!(!halves.contains_key(&7));
    }

    /// The shape of `MetaOverwritesByArrival`: a split's descriptor stored over a key meta
    /// already names at its generation or a newer one. A win over an older one is what the
    /// correct meta range makes, the first generation's resends have no older descriptor to
    /// bring back, and an index already folded is not read again.
    // PROPOSED(D-113): §10's shape.
    #[test]
    fn a_resend_counts_only_where_it_repeats_a_split_meta_already_names() {
        let count = meta_resends_stored_of;
        let mut records = vec![meta(1, 0, 2, ("a", "z"), 1), meta(2, 1, 2, ("a", "z"), 1)];
        assert_eq!(count(&records), 0, "a resend of the first generation");
        records.extend([meta(3, 2, 2, ("a", "m"), 2), meta(4, 3, 19, ("m", "z"), 2)]);
        assert_eq!(count(&records), 0, "each over an older");
        records.push(meta(5, 3, 19, ("m", "z"), 2));
        assert_eq!(
            count(&records),
            0,
            "a lagging replica's apply of an index already folded"
        );
        records.push(meta(6, 4, 19, ("m", "z"), 2));
        assert_eq!(count(&records), 1, "a resend of the split's update");
        records.push(meta(7, 5, 19, ("m", "t"), 3));
        records.push(meta(8, 6, 19, ("m", "z"), 2));
        assert_eq!(count(&records), 2, "and one older than meta holds");
    }

    /// The shape of `IdBlockResumed`: a right half out of a block its holder was granted before
    /// it last restarted. Not a block of the new run, and not the other node's.
    // PROPOSED(D-113): §10's shape.
    #[test]
    fn a_split_counts_only_from_a_block_granted_before_its_holders_latest_restart() {
        let leased = |ms, node, first, last| {
            record(
                ms,
                1,
                TraceEvent::RangeIdsLeased {
                    node,
                    run: ms,
                    first,
                    last,
                    index: ms,
                },
            )
        };
        let split = |ms, range, index, right| {
            record(
                ms,
                1,
                TraceEvent::RangeSplit {
                    range,
                    right,
                    key: Bytes::from_static(b"m"),
                    index,
                },
            )
        };
        let restarted = |ms, node| {
            record(
                ms,
                node,
                TraceEvent::NodeRestarted {
                    node: NodeId::new(node),
                },
            )
        };
        let count = splits_from_a_block_of_an_earlier_run_of;
        let mut records = vec![leased(1, 1, 10, 17), split(2, 2, 5, 10)];
        assert_eq!(count(&records), 0, "granted and taken in the one run");
        records.extend([leased(3, 2, 18, 25), restarted(4, 1), leased(5, 1, 26, 33)]);
        records.push(split(6, 2, 9, 26));
        assert_eq!(count(&records), 0, "from the new run's block");
        records.push(split(7, 2, 12, 18));
        assert_eq!(count(&records), 0, "from the other node's block");
        records.push(split(8, 2, 12, 10));
        assert_eq!(count(&records), 0, "that split was already read");
        records.push(split(9, 2, 15, 10));
        assert_eq!(count(&records), 1, "the id of the earlier run, taken again");
    }

    #[test]
    fn a_refusals_reason_is_read_for_every_loss_it_names() {
        assert_eq!(
            Loss::of("the engine's recovery lost state: dropped tables [4]"),
            vec![Loss::DroppedTable]
        );
        assert_eq!(
            Loss::of(
                "the engine's recovery lost state: dropped tables [4] fell back from manifest 7"
            ),
            vec![Loss::DroppedTable, Loss::Manifest]
        );
        assert_eq!(
            Loss::of("the log's head is missing: expected record 409, found 430"),
            vec![Loss::LostLogRecords]
        );
        assert_eq!(
            Loss::of(
                "the log is damaged: a record at segment 1 offset 9307 (bad-checksum) was skipped \
                 and the rest of the segment with it"
            ),
            vec![Loss::LostLogRecords]
        );
        assert_eq!(Loss::of("CURRENT cannot be read"), vec![Loss::Manifest]);
        assert_eq!(
            Loss::of("no manifest older than 9 has every table it lists on disk and intact"),
            vec![Loss::Manifest]
        );
        assert_eq!(Loss::of("something new"), vec![Loss::Other]);
    }

    /// A table's bounds attribute its loss: the replicas whose Raft interval or user span
    /// lie within them, and none past them.
    #[test]
    fn a_dropped_table_touches_the_replicas_within_its_bounds() {
        let held = [
            descriptor(1, 2, ("a", "m"), 2),
            descriptor(1, 19, ("m", "z"), 2),
        ];
        let dropped = |first: &[u8], last: &[u8]| {
            let mut records = held.to_vec();
            records.push(record(
                2,
                1,
                TraceEvent::SstDropped {
                    number: 7,
                    first_seq: 1,
                    max_seq: 2,
                    first_key: Bytes::copy_from_slice(first),
                    last_key: Bytes::copy_from_slice(last),
                    reason: "bad",
                },
            ));
            dropped_tables_of(&records)[0].clone()
        };
        let table = dropped(b"b", b"c");
        assert_eq!(
            (table.touched, table.held),
            (1, 2),
            "the left half's keys alone"
        );
        let table = dropped(b"b", b"p");
        assert_eq!(table.touched, 2, "across the two");
        let raft = KeyPrefix::group(19).span();
        let table = dropped(&raft.start, &raft.start);
        assert_eq!(table.touched, 1, "the right half's Raft interval");
    }

    #[test]
    fn a_split_leaves_at_least_the_fewest_keys_on_either_side() {
        let span = |start: &[u8], end: &[u8]| Span {
            start: user_key(start),
            end: user_key(end),
            generation: 1,
            voters: vec![1, 2, 3],
            since: at(0),
        };
        assert_eq!(split_keys(&span(b"k", b"l")), (2..=14).collect::<Vec<_>>());
        assert_eq!(split_keys(&span(b"k04", b"k08")), vec![6]);
        assert!(split_keys(&span(b"k04", b"k07")).is_empty());
    }

    fn mismatch(ms: u64, range: u64, client: u64, at: MismatchAt) -> TraceRecord {
        record(
            ms,
            1,
            TraceEvent::RangeMismatchSent {
                range,
                client,
                seq: 0,
                at,
                descriptors: Vec::new(),
            },
        )
    }

    fn split_record(ms: u64, range: u64, index: u64) -> TraceRecord {
        record(
            ms,
            1,
            TraceEvent::RangeSplit {
                range,
                right: range + 1,
                key: Bytes::from_static(b"k08"),
                index,
            },
        )
    }

    /// Each shape read off the trace, and what must not count for it. A shape's rate is only
    /// as good as this reading: a burst's refusal at receipt is the burst arriving after the
    /// split's apply, which reaches no window; a sweep client's mismatch from a range nothing
    /// has split is the bootstrap's stale caches; and a restart arm's split counts only where
    /// it was proposed on the restarted node, and only once however many replicas apply it.
    // PROPOSED(D-131)
    #[test]
    fn the_shapes_are_read_off_the_trace() {
        let burst = LOAD | 3;
        let reader = LOAD | READER | 3;
        let writer = LOAD | WRITER | 3;
        let sweep_client = 2 << 32;
        // Each window, at its own place.
        let records = [
            mismatch(1, 2, burst, MismatchAt::Apply),
            mismatch(2, 2, burst, MismatchAt::Read),
            mismatch(3, 2, reader, MismatchAt::Receipt),
        ];
        let shapes = shapes_of(&records, &[]);
        assert_eq!(shapes.writes_refused_at_apply, 1);
        assert_eq!(shapes.gets_refused_at_serving, 1);
        assert_eq!(shapes.stale_reads_refused, 1);
        assert!(shapes.reached_any());
        // A burst refused at receipt, or the writer's answer, is no window.
        let shapes = shapes_of(
            &[
                mismatch(1, 2, burst, MismatchAt::Receipt),
                mismatch(2, 2, writer, MismatchAt::Receipt),
                mismatch(3, 2, reader, MismatchAt::Read),
            ],
            &[],
        );
        assert!(!shapes.reached_any(), "{shapes:?}");
        // A sweep client's mismatch counts only from a range a split has cut.
        let before = mismatch(1, 2, sweep_client, MismatchAt::Receipt);
        let after = mismatch(3, 2, sweep_client, MismatchAt::Receipt);
        assert_eq!(
            shapes_of(&[before, split_record(2, 2, 5), after], &[]).clients_mismatched_by_a_split,
            1
        );
        // The restart arm's split: proposed on the restarted node, counted once.
        let arm = RestartArm {
            victim: 1,
            range: Some(2),
            seq: Some(101),
        };
        let proposed = |seq: u64, server: u64| {
            record(
                4,
                server as u32,
                TraceEvent::RaftProposed {
                    server,
                    range: 2,
                    client: DRIVER,
                    seq,
                    index: 7,
                    term: 2,
                },
            )
        };
        let split_at_seven = || split_record(5, 2, 7);
        let done = |p: TraceRecord| {
            shapes_of(&[p, split_at_seven(), split_at_seven()], &[arm]).splits_from_a_restarted_node
        };
        assert_eq!(done(proposed(101, 1)), 1);
        assert_eq!(
            done(proposed(101 + ASK_ANEW, 1)),
            1,
            "asked anew it is the same arm"
        );
        assert_eq!(done(proposed(101, 2)), 0, "proposed elsewhere: not from it");
        assert_eq!(done(proposed(102, 1)), 0, "another admin's split");
    }

    /// A shape is a schedule the sweep can take: a drawn schedule keeps every draw of the plain
    /// one (D-031's stream of its own), adds the shapes, and on one seed in [`PLAIN_ONE_IN`]
    /// adds nothing.
    // PROPOSED(D-131)
    #[test]
    fn the_shapes_are_drawn_over_the_plain_schedule_from_streams_of_their_own() {
        let mut none = 0;
        let (mut clients, mut writes, mut reads, mut stale, mut arms) = (0, 0, 0, 0, 0);
        for seed in 0..512 {
            let plain = Schedule::plain(seed);
            assert!(
                plain.actions.iter().all(|a| a.load == Load::Clients),
                "a plain schedule draws no load"
            );
            assert!(
                !plain
                    .faults
                    .iter()
                    .any(|f| matches!(f, Fault::SplitFromRestarted { .. }))
            );
            let drawn = Schedule::draw(seed);
            if drawn == plain {
                none += 1;
                continue;
            }
            assert_eq!(drawn.drifts, plain.drifts);
            assert_eq!(drawn.skews, plain.skews);
            assert_eq!(drawn.actions.len(), plain.actions.len());
            for (d, p) in drawn.actions.iter().zip(&plain.actions) {
                assert_eq!(
                    (d.at, d.kind, d.range, d.pick),
                    (p.at, p.kind, p.range, p.pick)
                );
                match d.load {
                    Load::Clients => clients += 1,
                    Load::WriteBurst { .. } => writes += 1,
                    Load::ReadBurst { .. } => reads += 1,
                    Load::StaleCache { .. } => stale += 1,
                }
            }
            let rest: Vec<Fault> = drawn
                .faults
                .iter()
                .copied()
                .filter(|f| !matches!(f, Fault::SplitFromRestarted { .. }))
                .collect();
            assert_eq!(rest, plain.faults);
            arms += usize::from(rest.len() + 1 == drawn.faults.len());
        }
        assert!(
            none > 0 && none < 512 / 4,
            "about one seed in {PLAIN_ONE_IN} draws none: {none} of 512"
        );
        assert!(writes > 0 && reads > 0 && stale > 0 && clients > 0 && arms > 0);
    }
}

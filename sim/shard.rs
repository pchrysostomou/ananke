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
//! sweep's own: the meta convergence bound (§8), the shape (nodes 4 and 5 hold nothing;
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
    Clock, Either, Environment, Instant, Network, NodeId, RangeCause, RangeState, Socket,
    TraceEvent, race,
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
    admin_addr, election_max, server_addr,
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
}

impl Fault {
    /// How long the fault lasts.
    #[must_use]
    pub fn length(&self) -> Duration {
        match self {
            Self::Isolate { for_, .. }
            | Self::IsolateLeader { for_ }
            | Self::OneWay { for_, .. } => *for_,
            Self::Crash { down, .. } | Self::CrashLeader { down } => *down,
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
}

impl Schedule {
    /// The schedule drawn from `seed`: four to seven faults from stream
    /// `shard-faults`, each followed by a quiet of 450 to 700 ms; the arms' range picks
    /// from `shard-arm-range`; the clocks from `shard-clocks`; and four to eight driver
    /// actions from `shard`, at times spread over the faults, two in three a split.
    #[must_use]
    pub fn draw(seed: u64) -> Self {
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
}

impl Spans {
    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        let (range, start, end, generation, voters) = match &record.event {
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

type Driver = Arc<Mutex<DriverStats>>;

fn lock<T>(what: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    what.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The split's admin: asks `range`'s leader, `target` first, to split at `key`, follows
/// `NotLeader` round the bootstrap nodes twice, retries after `NoRangeId` (PROPOSED
/// D-092's A), and records every answer.
#[allow(clippy::too_many_arguments)]
async fn split_admin<E: Environment>(
    env: E,
    n: u64,
    mut target: u64,
    range: u64,
    generation: u64,
    key: Bytes,
    stats: Driver,
) {
    let Ok(sock) = env.net().bind(admin_addr(n)).await else {
        lock(&stats).outstanding -= 1;
        return;
    };
    let mut tries = BOOTSTRAP * 4;
    let mut done = false;
    let mut first_refused: Option<Instant> = None;
    while tries > 0 && !done {
        tries -= 1;
        let request = RangedRequest {
            range: RangeId(range),
            generation,
            request: Request {
                client: DRIVER,
                seq: n,
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
            continue;
        };
        let Ok(answer) = RangedResponse::decode(bytes) else {
            continue;
        };
        if answer.response.seq != n {
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
        at += *gap;
    }
    for (j, action) in schedule.actions.iter().enumerate() {
        steps.push((begin + action.at, 2, Step::Act(j)));
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
                    Fault::Crash { .. } | Fault::CrashLeader { .. } => {
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
                act(&sim, admin, j, &schedule.actions[j], &watch.spans, &driver);
            }
        }
    }
    // The driver's admins still asking finish before the liveness window opens: the
    // window is about a cluster the adversary has stopped touching.
    let drain = sim.now() + DRIVER_DRAIN;
    while watch.checks.stopped.is_none() && lock(&driver).outstanding > 0 && sim.now() < drain {
        advance(&mut sim, SLICE, &mut watch);
    }
    last_heal = last_heal.max(sim.now());
    advance(&mut sim, schedule.settle, &mut watch);
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
    }
}

/// Fires the driver's action `j`, resolved against `spans` as they stand.
fn act(sim: &Sim, admin: NodeId, j: usize, action: &Action, spans: &Spans, driver: &Driver) {
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
                return;
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
                ),
            );
        }
        ActionKind::Transfer => {
            let ranges: Vec<(u64, &Span)> = spans.latest().iter().map(|(r, s)| (*r, s)).collect();
            if ranges.is_empty() {
                return;
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
            let Some(&to) = others
                .get(usize::try_from(action.pick % others.len().max(1) as u64).expect("small"))
            else {
                return;
            };
            {
                let mut stats = lock(driver);
                stats.transfers_asked += 1;
                stats.outstanding += 1;
            }
            env.spawn(
                "transfer-admin",
                transfer_admin(inner, n, leader, range, span.generation, to, driver.clone()),
            );
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

impl Report {
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

    /// The lags the meta convergence bound is asked of: on a uniformly scheduled seed
    /// (D-016), with the meta range's unimpaired replicas a majority and the range's too
    /// (§8), since each needs a leader to send or take the update.
    #[must_use]
    pub fn meta_lags_asked(&self) -> Vec<MetaLag> {
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

    /// The meta convergence bound (SHARD.md §8): after the last heal, meta names every
    /// key's current descriptor within [`meta_convergence_bound`].
    ///
    /// # Errors
    ///
    /// The first range meta names late or not at all.
    pub fn meta_converges(&self) -> Result<(), String> {
        let seed = self.seed;
        let bound = meta_convergence_bound();
        for lag in self.meta_lags_asked() {
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

    /// D-092's assertion where a split was refused for want of a range id, per seed: on a
    /// uniformly scheduled seed whose ranges 0 and the asked range have a majority, the
    /// asker's retries end on an answer, and within the write bound of the last heal.
    ///
    /// # Errors
    ///
    /// The first asker left without an answer.
    // PROPOSED(D-092): answer A; the asker retries.
    pub fn retries_take_effect(&self) -> Result<(), String> {
        if !self.checked.uniform() {
            return Ok(());
        }
        let seed = self.seed;
        let live = self.checked.ranges_with_a_majority_up();
        let bound = election_max() * LIVENESS_TIMEOUTS;
        for retried in &self.driver.retried {
            if !live.contains(&ROOT_RANGE) || !live.contains(&retried.range) {
                continue;
            }
            let since = retried.refused.max(self.checked.last_heal);
            let late = retried
                .ended
                .checked_duration_since(since)
                .unwrap_or_default();
            if !retried.answered && late > bound {
                return Err(format!(
                    "seed {seed}: NoRangeId: the split of range {} refused at {:?} was still \
                     unanswered {late:?} past the last heal, over {bound:?}",
                    retried.range, retried.refused
                ));
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
}

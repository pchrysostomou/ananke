//! The node scenario (SHARD.md §2, §4 and §12's Stage B): three nodes, **four
//! ranges on every node**, each range placed as today's one group is.
//!
//! Every other scenario in this directory runs `ananke_raft::run`, one server that is
//! one Raft group. This one runs [`ananke_shard::server::run`]: one socket, one
//! engine, one inbox, one `raft` task on one ticker and one `apply` task, with a
//! store and a core per range. The ranges are fixed at bootstrap from configuration,
//! as §2 generalises `initial_voters`, and each replica is traced
//! `RangeCreated { cause: bootstrap }`. Nothing changes a descriptor and nothing
//! routes: a client takes its key's range from [`range_of_key`], the fixed map below,
//! and puts it on every message.
//!
//! **Why four.** Four is the plan's parameter and not a measurement (SHARD.md §12):
//! enough that a frame between two nodes carries messages of several ranges each way,
//! and more ranges than the re-seed shape's receive cap of two.
//!
//! **What the run is for.** Until this scenario every trace in the tree had one
//! range, so a check keyed by `(range, term)` and one keyed by `term` said the same
//! thing on every seed of every tier: D-071 keyed the checks of §8 and had to prove
//! each key on hand-built two-range traces, because no sweep could tell a right key
//! from a wrong one. This sweep can. Its trace carries four ranges on three nodes
//! under faults, and the checks D-071 keyed are asked of it — through
//! [`raft::Report::over_a_run`], which is this scenario's trace under that scenario's
//! checks.
//!
//! **What it does not run, and what says so.** The node of this slice has no
//! `snapshot` task, no install, no re-seed and no follower compaction: those are the
//! other Stage B slices'. So this scenario keeps its cores below their snapshot
//! threshold, sets no bit rotting and asserts on every seed that no snapshot action
//! was asked for and no store was refused ([`Report::check`]), which is the
//! situation's absence asserted with its reason rather than left to be found
//! (CLAUDE.md:58-67). The day a schedule reaches either, this sweep says so.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::lin::History;
use crate::raft::{self, ClientStats, TICK, client_addr, election_max, server_addr};
use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{ClientOp, ClientResult, FileSystem};
use ananke_env::{
    Clock, Either, Environment, Instant, Network, NodeId, Rng, Socket, TraceEvent, race,
};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::node::{Start, StartOrder, start_store};
use ananke_raft::store::{KeyPrefix, is_marked_lost, mark_store_lost};
use ananke_raft::{ServerId, invariants};
use ananke_shard::client::{RangedRequest, RangedResponse};
use ananke_shard::range::RangeId;
use ananke_shard::server::{Range, ServerConfig};
use ananke_shard::variant::NodeVariants;
use ananke_storage::EngineConfig;
use bytes::Bytes;

/// The nodes.
pub const NODES: u64 = 3;
/// The ranges on every node: the plan's parameter (SHARD.md §12).
pub const RANGES: u64 = 4;
/// The first range's id. SHARD.md §2 keeps range 0 for the root span and range 1 for
/// the meta span, so the keyspace starts at 2 — which is the one group a server runs
/// today (`SINGLE_GROUP`).
pub const FIRST_RANGE: u64 = 2;
/// The keys the clients draw from: two per range, so a range's liveness is about a
/// key some client wrote to and not about the one key the cluster has.
pub const KEYS: u64 = 8;
/// The clients.
pub const CLIENTS: u64 = 2;
/// Where each node's engine lives.
pub const DIR: &str = "/node";
/// The node's inbox bound, in bytes (D-072).
pub const INBOX_BYTES: usize = 64 * 1024;

/// The node's cap on snapshot streams received and assembled at once (Q14, D-075).
///
/// D-075 fixes no default and recommends that a scenario which is not about the cap
/// set it at or above the node's range count, so no stream waits by accident. These
/// scenarios are not about the cap: they set it at [`RANGES`]. The re-seed shape, which
/// *is* about the cap, sets it to two on purpose (SHARD.md §12).
pub const SNAPSHOT_CAP: usize = RANGES as usize;
/// How many trace records a run may hold before it is stopped as a runaway
/// (`raft::TRACE_CAP`, and the figure Stage B measures per range).
pub const TRACE_CAP: usize = raft::TRACE_CAP;
/// How long a client waits for a write, and for a read.
const WRITE_TIMEOUT: Duration = Duration::from_millis(60);
const OP_TIMEOUT: Duration = Duration::from_millis(250);
const TRY_TIMEOUT: Duration = Duration::from_millis(40);
const OP_GAP: Duration = Duration::from_millis(5);
/// The liveness window, in maximum election timeouts (`raft::LIVENESS_TIMEOUTS`).
const LIVENESS_TIMEOUTS: u32 = 10;

/// The ranges configuration fixes, with the span each holds: four contiguous spans
/// over the keys `k0..k7`, two keys each.
///
/// The spans are configuration's and nothing routes by them (Stage C gives ranges
/// descriptors); they are here because §8's `RangeCreated` names the span a replica
/// was created for, and a creation that named nothing would be a record of nothing.
#[must_use]
pub fn ranges() -> Vec<Range> {
    (0..RANGES)
        .map(|i| Range {
            id: RangeId(FIRST_RANGE + i),
            start: Bytes::from(format!("k{}", i * 2)),
            end: Bytes::from(format!("k{}", (i + 1) * 2)),
        })
        .collect()
}

/// The range a key belongs to: the scenario's fixed map (SHARD.md, Stage B).
///
/// `k0` and `k1` are range 2's, `k2` and `k3` range 3's, and so on. A key the map
/// does not know — no client of this scenario writes one — is the first range's, so
/// the map is total, as a routing table must be.
#[must_use]
pub fn range_of_key(key: &Bytes) -> u64 {
    let digit = std::str::from_utf8(key)
        .ok()
        .and_then(|key| key.strip_prefix('k'))
        .and_then(|rest| rest.parse::<u64>().ok())
        .unwrap_or(0);
    FIRST_RANGE + (digit / 2).min(RANGES - 1)
}

/// What a replica was created with, as check 7's first step compares it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Descriptor {
    /// The span's first key.
    pub start: Bytes,
    /// The key past the span's last.
    pub end: Bytes,
    /// The descriptor's generation.
    pub generation: u64,
    /// The voters it was created with.
    pub voters: Vec<u64>,
    /// The replica's floor index.
    pub floor_index: u64,
    /// That index's term.
    pub floor_term: u64,
}

/// One fault of the schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// One node cut off from the rest for a while, with no client on its side.
    Isolate {
        /// The node.
        node: u64,
        /// For how long.
        for_: Duration,
    },
    /// The leader **of one range** cut off from the rest.
    ///
    /// §11's env item 8: a leader-relative arm on a node of many ranges chooses its
    /// range from its own stream, so that which range's leader it aims at is the
    /// arm's own draw and moves no other arm's schedule when it changes.
    IsolateLeaderOfARange {
        /// Which of the node's ranges, as an index into [`ranges`], drawn from the
        /// arm's own stream.
        pick: u64,
        /// For how long.
        for_: Duration,
    },
    /// One node crashed and restarted: every replica on it goes down together, which
    /// is the whole point of a node (Q15's refusal is the slice after this one's).
    Crash {
        /// The node.
        node: u64,
        /// How long it stays down.
        down: Duration,
    },
}

/// The faults one seed runs.
#[derive(Clone, Debug)]
pub struct Schedule {
    /// All links up, the ranges electing and the clients starting.
    pub warmup: Duration,
    /// The faults, each healed before the next.
    pub faults: Vec<Fault>,
    /// The quiet after each fault.
    pub gaps: Vec<Duration>,
    /// The quiet after the last fault: the liveness window.
    pub settle: Duration,
}

impl Schedule {
    /// A schedule drawn from `seed`: three to five faults, each followed by a quiet
    /// of at least two maximum election timeouts, so a check after a heal sees the
    /// heal's effect alone.
    #[must_use]
    pub fn draw(seed: u64) -> Self {
        let mut rng = moirae_sched::stream(seed, "ranges-schedule");
        let ms = |rng: &mut moirae_sched::Pcg32, lo: u64, hi: u64| {
            Duration::from_millis(lo + rng.below(hi - lo + 1))
        };
        let count = 3 + rng.below(3);
        let mut faults = Vec::new();
        let mut gaps = Vec::new();
        for _ in 0..count {
            let fault = match rng.below(3) {
                0 => Fault::Isolate {
                    node: 1 + rng.below(NODES),
                    for_: ms(&mut rng, 150, 600),
                },
                1 => Fault::IsolateLeaderOfARange {
                    pick: rng.below(RANGES),
                    for_: ms(&mut rng, 150, 600),
                },
                _ => Fault::Crash {
                    node: 1 + rng.below(NODES),
                    down: ms(&mut rng, 100, 400),
                },
            };
            faults.push(fault);
            gaps.push(election_max() * 2 + ms(&mut rng, 0, 200));
        }
        Self {
            warmup: ms(&mut rng, 300, 600),
            faults,
            gaps,
            settle: election_max() * LIVENESS_TIMEOUTS + Duration::from_millis(200),
        }
    }

    /// How long the run takes, for the simulator's run-length hint.
    #[must_use]
    pub fn total(&self) -> Duration {
        let faults: Duration = self
            .faults
            .iter()
            .map(|fault| match fault {
                Fault::Isolate { for_, .. } | Fault::IsolateLeaderOfARange { for_, .. } => *for_,
                Fault::Crash { down, .. } => *down,
            })
            .sum();
        self.warmup + faults + self.gaps.iter().sum::<Duration>() + self.settle
    }
}

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// The cores' known-buggy variants (D-045).
    pub variants: Variants,
    /// The node's own known-buggy variants (the round's and the wire's).
    pub node_variants: NodeVariants,
    /// The faults it ran.
    pub schedule: Schedule,
    /// The trace under the checks D-071 keyed by range: this run's records, its
    /// isolations, its heal, its history and its map from a key to its range.
    pub checked: raft::Report,
}

impl Report {
    /// The trace as records.
    #[must_use]
    pub fn records(&self) -> &[TraceRecord] {
        &self.checked.records
    }

    /// Every range the trace names a replica of.
    #[must_use]
    pub fn ranges(&self) -> BTreeSet<u64> {
        self.checked.ranges()
    }

    /// How many replicas were created at bootstrap: three nodes times four ranges on
    /// a run where no node's store was replaced.
    #[must_use]
    pub fn bootstrap_creations(&self) -> usize {
        self.records()
            .iter()
            .filter(|record| {
                matches!(
                    record.event,
                    TraceEvent::RangeCreated {
                        cause: ananke_env::RangeCause::Bootstrap,
                        ..
                    }
                )
            })
            .count()
    }

    /// How many entries each range applied on the node that applied most of them:
    /// what says the run put work through every range and not through one.
    #[must_use]
    pub fn applies_by_range(&self) -> BTreeMap<u64, usize> {
        let mut by_range: BTreeMap<u64, usize> = BTreeMap::new();
        for record in self.records() {
            if let TraceEvent::RaftApply { range, .. } = &record.event {
                *by_range.entry(*range).or_default() += 1;
            }
        }
        by_range
    }

    /// The leaders elected, per range: a range with none never got started.
    #[must_use]
    pub fn leaders_by_range(&self) -> BTreeMap<u64, usize> {
        let mut by_range: BTreeMap<u64, usize> = BTreeMap::new();
        for record in self.records() {
            if let TraceEvent::RaftLeader { range, .. } = &record.event {
                *by_range.entry(*range).or_default() += 1;
            }
        }
        by_range
    }

    /// The longest a client's first write to any key of a live range took after the
    /// last heal, and the bound it ran under: the write bound's margin, per key and
    /// per range (SHARD.md §8; D-071 measured it with one range).
    ///
    /// `asked` restricts it to the runs the bound is *asked* of: a uniform schedule
    /// (D-016 asks time only of those) with a range whose unimpaired replicas form a
    /// majority. That is the margin proper. With `asked` false it is the worst such
    /// write on any run, which is the figure that says what the scenario reaches
    /// whether or not the bound is asked there.
    ///
    /// `None` when the run is not one the bound is asked of, when no range had a
    /// majority, or when no key of a live range was written after the heal.
    ///
    /// The write is measured from its own call, as the check reads it (D-076): the
    /// margin here is the margin of the check, and a figure read the other way would
    /// be the clients' idleness as much as the node's latency.
    // PROPOSED(D-076): the write bound's margin with four ranges to a node.
    #[must_use]
    pub fn worst_write_after_heal(&self, asked: bool) -> Option<Duration> {
        if asked && !self.checked.uniform() {
            return None;
        }
        let live = self.checked.ranges_with_a_majority_up();
        self.checked
            .writes_after_heal_by_key()
            .into_iter()
            .filter(|(key, _)| live.contains(&range_of_key(key)))
            .filter_map(|(_, took)| took)
            .max()
    }

    /// The longest a live range took after the last heal to complete a client write:
    /// the recovery time proper, under the same bound and with the same meaning of
    /// `asked` ([`raft::Report::writes_after_heal_by_range`]).
    // PROPOSED(D-076): the recovery time proper is asked per range.
    #[must_use]
    pub fn worst_range_recovery(&self, asked: bool) -> Option<Duration> {
        if asked && !self.checked.uniform() {
            return None;
        }
        let live = self.checked.ranges_with_a_majority_up();
        self.checked
            .writes_after_heal_by_range()
            .into_iter()
            .filter(|(range, _)| live.contains(range))
            .filter_map(|(_, took)| took)
            .max()
    }

    /// The bound that margin is measured against: ten maximum election timeouts.
    #[must_use]
    pub fn write_bound() -> Duration {
        election_max() * LIVENESS_TIMEOUTS
    }

    /// The span the run's records actually cover: the last record's time less the
    /// first's. It is what a rate is divided by, in place of the schedule's planned
    /// total — they agree here, and a run stopped as a runaway is exactly the case
    /// where they would not.
    #[must_use]
    pub fn observed(&self) -> Duration {
        let records = self.records();
        match (records.first(), records.last()) {
            (Some(first), Some(last)) => last.at.duration_since(first.at),
            _ => Duration::ZERO,
        }
    }

    /// Trace records per virtual second, divided by the range count: the figure
    /// Stage B measures against `TRACE_CAP`, which sizes the scenarios of Stages C
    /// to E.
    ///
    /// The numerator is the **whole** trace — client operations, every
    /// `MessageSent`/`MessageDelivered`, the engine's records — and only a small
    /// part of it is about a range at all, so this is an upper bound on any range's
    /// own rate and not that rate: [`Report::busiest_range_records_per_second`] is
    /// the observed one, about sixteen times smaller. A cap sized from this figure
    /// is sized conservatively, which is the direction to be wrong in; the name says
    /// which figure it is.
    // PROPOSED(D-076): the trace's rate against `TRACE_CAP`, per range.
    #[must_use]
    pub fn records_per_second_per_range(&self) -> f64 {
        let seconds = self.observed().as_secs_f64().max(f64::EPSILON);
        self.records().len() as f64 / RANGES as f64 / seconds
    }

    /// The records that name a range, counted for the busiest range, per observed
    /// virtual second: the rate a range's own records actually reach.
    #[must_use]
    pub fn busiest_range_records_per_second(&self) -> f64 {
        let seconds = self.observed().as_secs_f64().max(f64::EPSILON);
        let mut by_range: BTreeMap<u64, usize> = BTreeMap::new();
        for record in self.records() {
            if let Some(range) = raft::range_of(&record.event) {
                *by_range.entry(range).or_default() += 1;
            }
        }
        by_range.into_values().max().unwrap_or(0) as f64 / seconds
    }

    /// Every frame this run's nodes sent each other, decoded: how many messages it
    /// carried and how many distinct ranges those messages were of.
    ///
    /// This is what says a frame between two nodes carries several ranges — the
    /// parameter four is fixed for (SHARD.md §12) — read off the frames themselves.
    /// Counting the ranges a *run* names says only that the run has four ranges,
    /// which `bootstrap_creations` and `applies_by_range` already assert: a node
    /// whose every frame carried exactly one message would pass that and fail this.
    ///
    /// A client's packet carries its range in the envelope this slice added and is
    /// neither codec's; it is told apart by its first byte and left out here.
    // PROPOSED(D-076): the batching claim is read off the frames.
    #[must_use]
    pub fn frames_carried(&self) -> (BTreeMap<usize, usize>, BTreeMap<usize, usize>) {
        let mut messages: BTreeMap<usize, usize> = BTreeMap::new();
        let mut ranges: BTreeMap<usize, usize> = BTreeMap::new();
        for record in self.records() {
            let TraceEvent::MessageSent { payload, .. } = &record.event else {
                continue;
            };
            if ananke_shard::is_ranged(payload) {
                continue;
            }
            let Ok(decoded) = ananke_shard::decode(payload) else {
                continue;
            };
            *messages.entry(decoded.messages.len()).or_default() += 1;
            let of: BTreeSet<RangeId> =
                decoded.messages.iter().map(|tagged| tagged.range).collect();
            *ranges.entry(of.len()).or_default() += 1;
        }
        (messages, ranges)
    }

    /// How many of this run's peer frames carried messages of more than one range.
    #[must_use]
    pub fn frames_of_several_ranges(&self) -> usize {
        let (_, ranges) = self.frames_carried();
        ranges
            .iter()
            .filter(|(carried, _)| **carried > 1)
            .map(|(_, frames)| *frames)
            .sum()
    }

    /// Every payload a node sent a node is a batch frame of this node's codec, and
    /// no payload of it parses as a frame of the one-group server's.
    ///
    /// The two codecs' first bytes collide: a batch frame's version byte is 1 and
    /// `ananke-raft`'s tag 1 is a pre-vote. A payload is read as a one-group frame
    /// when it parses as one and as a batch frame otherwise (`raft::messages_of`),
    /// which keeps every one-group scenario's replay exactly as it was — and is
    /// sound only while no batch frame parses as a one-group frame. Over a run of
    /// this scenario none does: a pre-vote is exactly 33 bytes, `Frame::decode`
    /// refuses trailing bytes, and the smallest batch frame is 34. The direction is
    /// pinned here, on the run's own frames, so that it is pinned on **every seed**
    /// of every tier and not on the one seed a directed test would run.
    ///
    /// # Errors
    ///
    /// The first payload that parses the wrong way round, or not at all.
    // PROPOSED(D-076): a payload is a one-group frame when it parses as one.
    pub fn frames_are_this_nodes(&self) -> Result<(), String> {
        for record in self.records() {
            let TraceEvent::MessageSent { payload, .. } = &record.event else {
                continue;
            };
            if ananke_shard::is_ranged(payload) {
                continue;
            }
            if ananke_raft::message::Frame::decode(payload.clone()).is_ok() {
                return Err(format!(
                    "frames: a batch frame parses as a frame of the one-group server: {payload:?}"
                ));
            }
            if let Err(error) = ananke_shard::decode(payload) {
                return Err(format!(
                    "frames: a payload this node sent is no batch frame ({error}): {payload:?}"
                ));
            }
        }
        Ok(())
    }

    /// The descriptor a replica was created with, as check 7's first step compares
    /// it: the span, the generation, the voters and the floor.
    // PROPOSED(D-076): check 7's first step, for the creations this slice emits.
    fn descriptor_of(event: &TraceEvent) -> Option<Descriptor> {
        let TraceEvent::RangeCreated {
            start,
            end,
            generation,
            voters,
            floor_index,
            floor_term,
            ..
        } = event
        else {
            return None;
        };
        Some(Descriptor {
            start: start.clone(),
            end: end.clone(),
            generation: *generation,
            voters: voters.clone(),
            floor_index: *floor_index,
            floor_term: *floor_term,
        })
    }

    /// Check 7's first step, which the stage that emits `RangeCreated` owes
    /// (D-071, item 11): a range's replicas agree on the descriptor they were
    /// created with, and no replica is created twice.
    ///
    /// D-071's checks 2 and 4 take a creation's floor as an install's completion
    /// sets one and a removal as a refusal, and read neither event's `cause`: "a
    /// replica that forged either event would launder its own violation past checks
    /// 2, 3 and 4", and what ties them down is §8's check 7 — a range's replicas
    /// agree on the sequence of its configurations, of which every creation is a
    /// step. Every creation in this slice is a *bootstrap* creation, computed from
    /// configuration alone (SHARD.md §2), so the sequence is one step long and the
    /// check is that every replica's step agrees: the same span, generation, voters
    /// and floor, and one creation per (range, node). The sequence proper — a split's
    /// creation, a removal, a membership change of a range — belongs to the stage
    /// that produces one.
    ///
    /// # Errors
    ///
    /// The first replica whose creation disagrees with another's, or is a second one.
    // PROPOSED(D-076): check 7's first step, for the creations this slice emits.
    pub fn creations_agree(&self) -> Result<(), String> {
        let mut first: BTreeMap<u64, Descriptor> = BTreeMap::new();
        let mut seen: BTreeSet<(u64, u64)> = BTreeSet::new();
        for record in self.records() {
            let TraceEvent::RangeCreated { range, cause, .. } = &record.event else {
                continue;
            };
            let node = record.node.map_or(0, |node| u64::from(node.get()));
            if !seen.insert((*range, node)) {
                return Err(format!(
                    "range creations: node {node} created a replica of range {range} twice"
                ));
            }
            if *cause != ananke_env::RangeCause::Bootstrap {
                return Err(format!(
                    "range creations: node {node}'s replica of range {range} was created by \
                     {cause:?}, and this scenario bootstraps every replica"
                ));
            }
            let Some(descriptor) = Self::descriptor_of(&record.event) else {
                continue;
            };
            match first.get(range) {
                None => {
                    first.insert(*range, descriptor);
                }
                Some(agreed) if *agreed == descriptor => {}
                Some(agreed) => {
                    return Err(format!(
                        "range creations: node {node}'s replica of range {range} was created \
                         with {descriptor:?}, where another was created with {agreed:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The run's verdict: every check of §8 that D-071 keyed by range, and this
    /// scenario's own two absences.
    ///
    /// # Errors
    ///
    /// The first violation, in words naming it.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        self.checked.check()?;
        self.creations_agree()
            .map_err(|violation| format!("seed {seed}: {violation}"))?;
        self.frames_are_this_nodes()
            .map_err(|violation| format!("seed {seed}: {violation}"))?;
        // The paths this slice's node does not have, asserted absent with the
        // reason (CLAUDE.md:58-67): the `snapshot` task keyed by range and follower,
        // Q15's refusal and re-seed, and follower compaction are the other Stage B
        // slices'. The scenario keeps the cores below their snapshot threshold and
        // rots no bit, so neither is reached; the day a schedule reaches one, this
        // says so instead of passing over it.
        if let Some(action) = self
            .records()
            .iter()
            .find_map(|record| match &record.event {
                TraceEvent::RaftSnapshot { server, range, .. } => Some((*server, *range)),
                _ => None,
            })
        {
            return Err(format!(
                "seed {seed}: server {} took or restated a snapshot of range {}, a path this \
                 scenario's node does not have: the `snapshot` task keyed by range and follower \
                 is another slice's",
                action.0, action.1
            ));
        }
        if let Some((server, reason)) = self.checked.refused.first() {
            return Err(format!(
                "seed {seed}: server {server} refused its store ({reason}), a path this \
                 scenario's node does not have: Q15's whole-node refusal and re-seed are \
                 another slice's"
            ));
        }
        // Every node hosts every range from its first start: four creations a node,
        // and every range led at some point, or the run put no work through it.
        let created = self.bootstrap_creations();
        let wanted = usize::try_from(NODES * RANGES).expect("small");
        if created != wanted {
            return Err(format!(
                "seed {seed}: {created} bootstrap creations, not the {wanted} of \
                 {NODES} nodes times {RANGES} ranges"
            ));
        }
        Ok(())
    }
}

/// The simulator's configuration for a seed: drops, duplicates, delays and a disk
/// that takes time, as the raft sweep's does — and no bit rotting, because a rotted
/// table refuses a store and Q15's re-seed is another slice's.
#[must_use]
pub fn config(seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.05;
    config.net.p_duplicate = 0.05;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(10);
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.0;
    config.fs.latency_min = Duration::from_micros(100);
    config.fs.latency_max = Duration::from_millis(2);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(NODES + CLIENTS).expect("small"),
        schedule.total(),
    );
    config
}

/// One node's configuration: its ranges, and the voters each starts with.
#[must_use]
pub fn server_config(id: u64, variants: impl Into<Variants>, node: NodeVariants) -> ServerConfig {
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    ServerConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=NODES).map(|s| (ServerId(s), server_addr(s))).collect(),
        ranges: ranges(),
        initial_voters: (1..=NODES).map(ServerId).collect(),
        raft: RaftConfig {
            variants: variants.into(),
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            // Far above what this run's clients write: no core reaches its
            // threshold, so no take is asked for and the `snapshot` task this node
            // has not got is never wanted. `Report::check` asserts that absence.
            snapshot_threshold: 1 << 30,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: INBOX_BYTES,
        snapshot_cap: SNAPSHOT_CAP,
        node,
    }
}

fn spawn_node(sim: &Sim, at: NodeId, id: u64, variants: Variants, node: NodeVariants) {
    let env = sim.env(at);
    let inner = env.clone();
    env.spawn("node", async move {
        let _ = ananke_shard::server::run(inner, server_config(id, variants, node)).await;
    });
}

/// One node of four ranges, alone: the other two voters are configured and never
/// started, so nothing ever resets an election timer and **every** core campaigns on
/// its own.
///
/// It is the directed scenario for D-057's first caller (D-061's rule for a variant
/// no seed of any tier catches): each core is seeded from `n{id}/r{range}/protocol`,
/// per *range*, and a node that drew one seed for all four cores would give them one
/// election timeout and campaign with all four at once. In the sweep that is
/// invisible — the cores' timers are reset by the traffic of three live nodes, and a
/// range whose leader is elsewhere never campaigns at all — so the case is built here
/// instead, where the only thing that moves a timer is the timer.
// PROPOSED(D-076): each core seeded per range, and the scenario that says so.
#[must_use]
pub fn alone(seed: u64, for_: Duration) -> Vec<TraceRecord> {
    let schedule = Schedule {
        warmup: for_,
        faults: Vec::new(),
        gaps: Vec::new(),
        settle: Duration::ZERO,
    };
    let mut sim = Sim::new(config(seed, &schedule));
    let node = sim.add_node();
    spawn_node(&sim, node, 1, Variants::default(), NodeVariants::correct());
    sim.run_for(for_);
    sim.trace()
}

/// Puts the node's engine directory into the state Q15's refusal starts from: a
/// directory that **held a store** and whose marker now says that store lost state.
///
/// Both halves matter. The mark alone is not enough — a directory holding nothing but a
/// marker is read as a 0.3.0-format store and refused for its *format*, which stops the
/// server (D-059) rather than refusing it for lost state — and it would not be D-041's
/// case either, which is about a directory that really did hold a store. So a store is
/// opened here first, exactly as the node would have opened it on a run before this
/// one, and dropped; then the mark a refusal leaves is written over it (D-044), as
/// `sim/quorum.rs` marks its refused server (D-049).
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
async fn lose_the_store<E: ananke_env::Environment>(env: &E) -> std::io::Result<()> {
    let dir = Path::new(DIR);
    env.fs().create_dir_all(dir).await?;
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    match start_store(
        env,
        1,
        &engine,
        Variants::default(),
        &KeyPrefix::group(FIRST_RANGE),
        StartOrder::Correct,
    )
    .await
    {
        Start::Opened { store, .. } => drop(store),
        Start::Refused(error) | Start::Failed(error) => return Err(error),
    }
    mark_store_lost(env, dir, "the scenario lost this store").await
}

/// The simulation the three readings below share: one node of four ranges whose engine
/// directory is marked lost before it starts, as `sim/quorum.rs` refuses a server
/// (D-049), so its open is refused on the mark. Run for `for_` and handed back still
/// running, so a caller that wants a restart can crash it.
///
/// The other two voters are configured and never started, so nothing arrives to fill
/// the re-seeded replicas and the state they wait in is the state the assertions read.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
fn refused_sim(seed: u64, for_: Duration, node_variants: NodeVariants) -> (Sim, NodeId) {
    let schedule = Schedule {
        warmup: for_,
        faults: Vec::new(),
        gaps: Vec::new(),
        settle: Duration::ZERO,
    };
    let mut sim = Sim::new(config(seed, &schedule));
    let at = sim.add_node();
    let env = sim.env(at);
    let inner = env.clone();
    env.spawn("node", async move {
        if lose_the_store(&inner).await.is_err() {
            return;
        }
        let _ =
            ananke_shard::server::run(inner, server_config(1, Variants::default(), node_variants))
                .await;
    });
    sim.run_for(for_);
    (sim, at)
}

/// The node's engine directories as the run left them: each generation's name and
/// whether its marker says the store there lost state.
///
/// The directories are read by a task on the node itself, after the run, because that
/// is the only handle on the node's filesystem the simulator offers — and because
/// reading them through the same `Environment` the node wrote them through is what
/// makes the answer the node's own view rather than the harness's.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
fn engine_dirs(sim: &mut Sim, at: NodeId) -> BTreeMap<String, bool> {
    let dirs: Arc<Mutex<BTreeMap<String, bool>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let audited = dirs.clone();
    let auditor = sim.env(at);
    let inner = auditor.clone();
    auditor.spawn("audit", async move {
        let base = PathBuf::from(DIR);
        let Some(parent) = base.parent().map(Path::to_path_buf) else {
            return;
        };
        let Ok(listed) = inner.fs().read_dir(&parent).await else {
            return;
        };
        for entry in listed {
            // A listing hands back names, not paths (see `server::generations`).
            let Some(name) = entry
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
            else {
                continue;
            };
            if ananke_shard::generation_of(&base, &name).is_none() {
                continue;
            }
            let Ok(lost) = is_marked_lost(&inner, &parent.join(&name)).await else {
                continue;
            };
            audited.lock().expect("the audit").insert(name, lost);
        }
    });
    sim.run_for(Duration::from_millis(50));
    dirs.lock().expect("the audit").clone()
}

/// Q15's whole-node refusal and re-seed, alone.
///
/// It is the directed scenario for Q15's path (SHARD.md §11, storage 8). It is directed
/// and not a sweep because the thing under test happens once, at a start, and because
/// the re-seed's *installs* need the node's snapshot wiring, which no slice has built
/// yet: what runs here is the refusal and the rebuild up to the point each replica
/// waits for its leader's stream.
///
/// The node is left running afterwards: the point of the re-seed is that the node does
/// *not* stop, which is what `run` did before this slice and what the trace shows by
/// carrying records past the refusal.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn refused_whole(seed: u64, for_: Duration, node_variants: NodeVariants) -> Vec<TraceRecord> {
    refused_sim(seed, for_, node_variants).0.trace()
}

/// [`refused_whole`]'s trace together with the node's engine directories as the run
/// left them.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn refused_whole_dirs(
    seed: u64,
    for_: Duration,
    node_variants: NodeVariants,
) -> (Vec<TraceRecord>, BTreeMap<String, bool>) {
    let (mut sim, at) = refused_sim(seed, for_, node_variants);
    let dirs = engine_dirs(&mut sim, at);
    (sim.trace(), dirs)
}

/// [`refused_whole`]'s node crashed and restarted once its re-seed is done, and run
/// again for as long: the trace of both lives and the directories the second one left.
///
/// This is what asks the *start* the question D-066 answers. The re-seed's own choice
/// of directory is made in the run above and read off the names it leaves; which
/// directory a later start then opens is a second decision, made in `server::run`
/// before any store opens, and until a node here is restarted nothing in the simulator
/// asked it — `newest_not_lost` was exercised only by `reseed::tests` calling it
/// directly, so a `run` that ignored it and opened the configured directory every time
/// passed the whole tree. A restart is what binds the two: the configured directory is
/// marked lost, so a node that opens it is refused a second time and re-seeds again,
/// and both the second refusal and the third generation it would build are visible
/// here.
///
/// The crash is the simulator's (§1.3): every task on the node dies and its disk keeps
/// only what was synced, which is what the re-seed's marks were.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn refused_whole_restarted(
    seed: u64,
    for_: Duration,
    node_variants: NodeVariants,
) -> (Vec<TraceRecord>, BTreeMap<String, bool>) {
    let (mut sim, at) = refused_sim(seed, for_, node_variants);
    sim.crash(at);
    sim.restart(at);
    spawn_node(&sim, at, 1, Variants::default(), node_variants);
    sim.run_for(for_);
    let dirs = engine_dirs(&mut sim, at);
    (sim.trace(), dirs)
}

/// What [`refused_whole`] is read for: the refusal, the replicas it took down, and the
/// re-seeded replicas' marks, in the order the trace holds them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Refusal {
    /// `RaftRefused` events for the node. Exactly one on the correct node: the refusal
    /// is the node's, once, and the re-seed that follows does not refuse again.
    pub refusals: usize,
    /// The ranges the refusal took down, from `RaftReplicaRefused` (D-077).
    pub replicas_refused: BTreeSet<u64>,
    /// The ranges whose durable refused mark is traced, from `RaftReseeded` (§8).
    pub marked: BTreeSet<u64>,
    /// The ranges traced `RangeCreated { cause: bootstrap }`. None of the re-seeded
    /// ones: a re-seeded replica's `RangeCreated` is its install's, with
    /// `cause: snapshot`, and the install is the wiring slice's.
    pub bootstrapped: BTreeSet<u64>,
    /// Each re-seeded replica's incarnation, from its `RaftRecovered` (Q26).
    pub incarnations: BTreeMap<u64, u64>,
    /// The ranges whose replica sent a message before its refused mark was durable:
    /// the replica *answering* before the mark, which is what `ServeBeforeRefusedMark`
    /// does and what §12's exit criterion (c) forbids.
    ///
    /// An answer is a message on the wire, not a record of the replica's own: the
    /// restatement that follows the mark traces the replica's log and term around the
    /// `RaftReseeded`, and none of that is the replica answering anybody.
    pub served_before_the_mark: BTreeSet<u64>,
    /// The ranges whose replica sent a message *after* its refused mark was durable.
    ///
    /// It is here so that an empty [`Refusal::served_before_the_mark`] is read for
    /// what it is. That set is empty on a node that ordered the mark and the answer
    /// correctly and equally empty on a node that never answered at all, and the two
    /// are not the same result: the second is a check with nothing to see. On the
    /// correct node today it is the second — a re-seeded replica is quarantined and
    /// takes part in nothing until its install (RAFT.md §3), and this slice builds no
    /// install — so this set is empty too, and the scenario asserts that absence with
    /// its reason. The ordering §12's exit criterion (c) names is owed with the
    /// snapshot wiring, and the day a re-seeded replica has something to answer, the
    /// assertion on this set fails and says so.
    pub answered_after_the_mark: BTreeSet<u64>,
    /// Why the node stopped, from `RaftServerFailed`: empty on the correct node, which
    /// re-seeds and carries on.
    ///
    /// A variant that gets the re-seed's *directory* wrong stops the node here, and
    /// the reason says where. Reading it is what tells a run that reached the re-seed
    /// and failed in it from a run that never got that far — the second would leave
    /// the same directory listing behind and pass a check that only counts names.
    pub failures: Vec<String>,
}

/// Reads a [`Refusal`] off [`refused_whole`]'s trace.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[must_use]
pub fn refusal(records: &[TraceRecord]) -> Refusal {
    let mut read = Refusal::default();
    for record in records {
        match &record.event {
            TraceEvent::RaftRefused { .. } => read.refusals += 1,
            TraceEvent::RaftReplicaRefused { range, .. } => {
                read.replicas_refused.insert(*range);
            }
            TraceEvent::RaftReseeded { range, .. } => {
                read.marked.insert(*range);
            }
            TraceEvent::RangeCreated { range, cause, .. } => {
                if *cause == ananke_env::RangeCause::Bootstrap {
                    read.bootstrapped.insert(*range);
                }
            }
            TraceEvent::RaftRecovered {
                range, incarnation, ..
            } => {
                read.incarnations.insert(*range, *incarnation);
            }
            TraceEvent::RaftServerFailed { reason, .. } => read.failures.push(reason.clone()),
            // A message on the wire is the replica answering. Read off the frames
            // themselves, so what counts is what the node actually sent and not what
            // the scenario believes it sent.
            TraceEvent::MessageSent { payload, .. } => {
                if ananke_shard::is_ranged(payload) {
                    continue;
                }
                let Ok(decoded) = ananke_shard::decode(payload) else {
                    continue;
                };
                for tagged in decoded.messages {
                    let range = tagged.range.get();
                    if !read.replicas_refused.contains(&range) {
                        continue;
                    }
                    if read.marked.contains(&range) {
                        read.answered_after_the_mark.insert(range);
                    } else {
                        read.served_before_the_mark.insert(range);
                    }
                }
            }
            _ => {}
        }
    }
    read
}

/// When each range first campaigned: the time of the first pre-vote its replica
/// sent, read off the frames themselves.
#[must_use]
pub fn first_campaigns(records: &[TraceRecord]) -> BTreeMap<u64, Instant> {
    let mut first: BTreeMap<u64, Instant> = BTreeMap::new();
    for record in records {
        let TraceEvent::MessageSent { payload, .. } = &record.event else {
            continue;
        };
        if ananke_shard::is_ranged(payload) {
            continue;
        }
        let Ok(decoded) = ananke_shard::decode(payload) else {
            continue;
        };
        for tagged in decoded.messages {
            if matches!(
                tagged.frame.message,
                ananke_raft::message::Message::PreVote { .. }
            ) {
                first.entry(tagged.range.get()).or_insert(record.at);
            }
        }
    }
    first
}

/// The leader of `range` now: the server of the latest `RaftLeader` of that range,
/// or server 1 where the trace holds none.
fn leader_of(sim: &Sim, range: u64) -> u64 {
    let records = sim.trace();
    records
        .iter()
        .rev()
        .find_map(|record| match &record.event {
            TraceEvent::RaftLeader {
                server, range: of, ..
            } if *of == range => Some(*server),
            _ => None,
        })
        .unwrap_or(1)
}

/// One client: operations on keys drawn from the whole keyspace, each sent to the
/// range the fixed map gives it, against the leader it last heard of.
async fn client<E: Environment>(env: E, n: u64, stats: Arc<Mutex<ClientStats>>) {
    let Ok(sock) = env.net().bind(client_addr(n)).await else {
        return;
    };
    let mut incarnation = 0u64;
    let mut process = n << 32 | incarnation;
    let mut seq = 0u64;
    // The leader it last heard of, per range: a node leads one range and follows
    // another, so one leader for the cluster would be wrong here.
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    let mut known: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
    loop {
        let key = Bytes::from(format!("k{}", env.rng().below(KEYS)));
        let range = range_of_key(&key);
        let value = Bytes::from(format!("{n}.{incarnation}.{seq}"));
        let write = env.rng().below(10) < 6;
        let op = if write {
            ClientOp::Put {
                key: key.clone(),
                value: value.clone(),
            }
        } else {
            ClientOp::Get { key: key.clone() }
        };
        env.trace(TraceEvent::ClientInvoke {
            client: process,
            seq,
            op: op.clone(),
        });
        let command = if write {
            Command::Put {
                key: key.clone(),
                value: value.clone(),
            }
        } else {
            Command::Get { key: key.clone() }
        };
        let deadline = env.clock().now() + if write { WRITE_TIMEOUT } else { OP_TIMEOUT };
        let mut target = leaders
            .get(&range)
            .copied()
            .unwrap_or_else(|| 1 + env.rng().below(NODES));
        let mut outcome = None;
        loop {
            let request = RangedRequest {
                range: RangeId(range),
                request: Request {
                    client: process,
                    seq,
                    command: command.clone(),
                },
            };
            if sock
                .send(server_addr(target), request.encode())
                .await
                .is_err()
            {
                return;
            }
            let try_deadline = if write {
                deadline
            } else {
                deadline.min(env.clock().now() + TRY_TIMEOUT)
            };
            let mut got = None;
            loop {
                let recv = pin!(sock.recv());
                let timer = pin!(env.clock().sleep_until(try_deadline));
                match race(&env, recv, timer).await {
                    Either::Left(Ok((_, bytes))) => {
                        if let Ok(answer) = RangedResponse::decode(bytes)
                            && answer.range.get() == range
                            && answer.response.client == process
                            && answer.response.seq == seq
                        {
                            got = Some(answer.response.reply);
                            break;
                        }
                    }
                    Either::Left(Err(_)) => return,
                    Either::Right(()) => break,
                }
            }
            let now = env.clock().now();
            match got {
                Some(Reply::Outcome(result)) => {
                    outcome = Some(result);
                    break;
                }
                Some(Reply::NotLeader { leader: hint }) => {
                    stats.lock().expect("the stats").redirected += 1;
                    if now >= deadline {
                        break;
                    }
                    match hint {
                        Some(leader) => target = leader.0,
                        None => {
                            env.clock().sleep(Duration::from_millis(20)).await;
                            target = target % NODES + 1;
                        }
                    }
                }
                None => {
                    if !write && now < deadline {
                        target = target % NODES + 1;
                    } else {
                        break;
                    }
                }
            }
        }
        match outcome {
            Some(result) => {
                leaders.insert(range, target);
                match (&op, &result) {
                    (ClientOp::Put { value, .. }, Outcome::Done) => {
                        known.insert(key, Some(value.clone()));
                    }
                    (ClientOp::Get { .. }, Outcome::Value(value)) => {
                        known.insert(key, value.clone());
                    }
                    _ => {}
                }
                env.trace(TraceEvent::ClientReturn {
                    client: process,
                    seq,
                    result: match result {
                        Outcome::Done => ClientResult::Done,
                        Outcome::Swapped(swapped) => ClientResult::Swapped(swapped),
                        Outcome::Value(value) => ClientResult::Value(value),
                    },
                });
                stats.lock().expect("the stats").completed += 1;
            }
            None => {
                stats.lock().expect("the stats").abandoned += 1;
                leaders.remove(&range);
                incarnation += 1;
                process = n << 32 | incarnation;
                known.clear();
            }
        }
        seq += 1;
        env.clock().sleep(OP_GAP).await;
    }
}

/// Runs the scenario for `seed` on the correct node, or on one carrying `variants`.
#[must_use]
pub fn run(seed: u64, variants: impl Into<Variants>, node: NodeVariants) -> Report {
    run_with(seed, Schedule::draw(seed), variants, node)
}

/// Runs the scenario for `seed` with an explicit schedule.
#[must_use]
pub fn run_with(
    seed: u64,
    schedule: Schedule,
    variants: impl Into<Variants>,
    node_variants: NodeVariants,
) -> Report {
    let variants = variants.into();
    let mut sim = Sim::new(config(seed, &schedule));
    let nodes: Vec<NodeId> = (0..NODES as usize).map(|_| sim.add_node()).collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let stats: Vec<Arc<Mutex<ClientStats>>> = (0..CLIENTS).map(|_| Arc::default()).collect();
    for id in 1..=NODES {
        spawn_node(&sim, nodes[id as usize - 1], id, variants, node_variants);
    }
    for (i, &node) in clients.iter().enumerate() {
        let env = sim.env(node);
        let inner = env.clone();
        let stats = stats[i].clone();
        env.spawn("client", client(inner, i as u64 + 1, stats));
    }
    let all_but = |server: u64| -> (Vec<NodeId>, Vec<NodeId>) {
        let side: Vec<NodeId> = vec![nodes[server as usize - 1]];
        let rest: Vec<NodeId> = nodes
            .iter()
            .chain(clients.iter())
            .copied()
            .filter(|node| !side.contains(node))
            .collect();
        (side, rest)
    };
    let mut isolations: Vec<(u64, Instant, Instant)> = Vec::new();
    let mut stopped: Option<String> = None;
    let advance = |sim: &mut Sim, duration: Duration, stopped: &mut Option<String>| {
        sim.run_for(duration);
        if stopped.is_none() && sim.trace_len() > TRACE_CAP {
            *stopped = Some(format!(
                "runaway: {} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.trace_len(),
                sim.now()
            ));
        }
    };
    advance(&mut sim, schedule.warmup, &mut stopped);
    let mut last_heal = sim.now();
    for (fault, gap) in schedule.faults.iter().zip(schedule.gaps.iter()) {
        if stopped.is_some() {
            break;
        }
        match fault {
            Fault::Isolate { node, for_ } => {
                let (side, rest) = all_but(*node);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *for_, &mut stopped);
                sim.heal();
                isolations.push((*node, from, sim.now()));
            }
            Fault::IsolateLeaderOfARange { pick, for_ } => {
                // §11, env 8: the arm's range is its own draw.
                let range = FIRST_RANGE + pick;
                let leader = leader_of(&sim, range);
                let (side, rest) = all_but(leader);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *for_, &mut stopped);
                sim.heal();
                isolations.push((leader, from, sim.now()));
            }
            Fault::Crash { node, down } => {
                let from = sim.now();
                sim.crash(nodes[*node as usize - 1]);
                advance(&mut sim, *down, &mut stopped);
                sim.restart(nodes[*node as usize - 1]);
                spawn_node(
                    &sim,
                    nodes[*node as usize - 1],
                    *node,
                    variants,
                    node_variants,
                );
                // A crashed node's replicas are all down, and all back: the window
                // is one isolation of the node, as the timer check reads it.
                isolations.push((*node, from, sim.now()));
            }
        }
        last_heal = sim.now();
        advance(&mut sim, *gap, &mut stopped);
    }
    if stopped.is_none() {
        advance(&mut sim, schedule.settle, &mut stopped);
    }
    let records = sim.trace();
    let history = History::from_trace(&records);
    let mut clients_total = ClientStats::default();
    for one in &stats {
        let one = one.lock().expect("the stats");
        clients_total.completed += one.completed;
        clients_total.abandoned += one.abandoned;
        clients_total.redirected += one.redirected;
    }
    let checked = raft::Report::over_a_run(raft::Run {
        seed,
        variants,
        policy: sim.policy(),
        header: sim.run_header(),
        records,
        last_heal,
        isolations,
        history,
        clients: clients_total,
        ranges: (FIRST_RANGE..FIRST_RANGE + RANGES).collect(),
        key_range: range_of_key,
        stopped,
    });
    Report {
        seed,
        variants,
        node_variants,
        schedule,
        checked,
    }
}

/// The checker's verdict on a run, for a test that wants it apart from the rest.
///
/// # Errors
///
/// The first violation of checks 1 to 4, keyed by range.
pub fn invariants_of(records: &[TraceRecord]) -> Result<(), String> {
    invariants::all(crate::traced(records))?;
    invariants::commit_majority(crate::traced(records), NODES as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_map_puts_two_keys_in_each_of_four_contiguous_ranges() {
        let spans = ranges();
        assert_eq!(spans.len(), RANGES as usize);
        for (i, span) in spans.iter().enumerate() {
            assert_eq!(span.id, RangeId(FIRST_RANGE + i as u64));
        }
        // Every key the clients draw lands in the span whose range the map names,
        // and the spans meet end to start.
        for key in 0..KEYS {
            let key = Bytes::from(format!("k{key}"));
            let range = range_of_key(&key);
            let span = &spans[(range - FIRST_RANGE) as usize];
            assert!(span.start <= key && key < span.end, "{key:?} in {span:?}");
        }
        for pair in spans.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
        assert_eq!(range_of_key(&Bytes::from_static(b"k0")), FIRST_RANGE);
        assert_eq!(range_of_key(&Bytes::from_static(b"k7")), FIRST_RANGE + 3);
    }

    #[test]
    fn a_leader_relative_arm_draws_its_range_from_its_own_stream() {
        // §11, env 8. The draw is the schedule's own stream, so two seeds pick
        // different ranges and no other arm's schedule moves when this one changes.
        let picks: BTreeSet<u64> = (0..64)
            .flat_map(|seed| Schedule::draw(seed).faults)
            .filter_map(|fault| match fault {
                Fault::IsolateLeaderOfARange { pick, .. } => Some(pick),
                _ => None,
            })
            .collect();
        assert!(picks.len() > 1, "the arm aims at one range only: {picks:?}");
        assert!(picks.iter().all(|pick| *pick < RANGES));
    }

    #[test]
    fn a_schedules_gaps_are_long_enough_for_a_heal_to_be_seen() {
        for seed in 0..32 {
            let schedule = Schedule::draw(seed);
            assert_eq!(schedule.faults.len(), schedule.gaps.len());
            assert!(schedule.gaps.iter().all(|gap| *gap >= election_max() * 2));
            assert!(schedule.settle >= election_max() * LIVENESS_TIMEOUTS);
            assert!(schedule.warmup >= crate::raft::ELECTION_MIN * 3);
        }
    }
}

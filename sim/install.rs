//! The node's snapshot wiring under a directed scenario: **a stream flows and an
//! install completes**, per range and per follower (SHARD.md §12, Stage B's "installs
//! on the node"; §11, storage 5).
//!
//! Nothing in the tree reached this path before. `sim/tests/node.rs` and
//! `sim/tests/ranges.rs` hold `snapshot_threshold` far above what their clients write,
//! on purpose, and assert the *absence* of a snapshot action with its reason: until
//! the wiring existed, a run that reached the path would have dropped a core's take on
//! the floor in silence (D-082). This scenario is the opposite of those: it is built to
//! reach the path on every seed, and it fails if it does not.
//!
//! **The shape.** Five voters and four ranges. Three nodes start; **two start late**,
//! after the running three have written past `snapshot_threshold` and their leaders
//! have compacted. The two late nodes are then behind every range's compacted prefix,
//! so each range's leader must stream to *both at once* — four ranges times two
//! followers, eight streams, which is what makes "per range and per follower" a claim
//! this scenario can check rather than a phrase.
//!
//! Two nodes rather than one is deliberate: with a single follower behind the prefix,
//! a leader that fed its followers one at a time would be indistinguishable from one
//! that fed them all at once, and `NodeVariant::CapStreamsSent` — D-043's rule, and one
//! of the seven D-075 says a single-stream world cannot catch — would have no situation
//! here at all.
//!
//! **What it asserts.**
//!
//! - every one of the eight (range, follower) streams was opened, and the leader that
//!   opened them had more than one running at once;
//! - every one of the eight installs completed, on the right node and under the right
//!   range;
//! - each late node's four replicas were *created by their installs*
//!   (`RangeCreated { cause: snapshot }`, §8), which is the event that says the install
//!   gave the range state on a node that held none;
//! - **what landed is what was taken**: each installed (server, range) is read back
//!   out of the engine and compared with the take that fed it, paired by the
//!   snapshot's identity — the user keys and their digest, the applied index, and the
//!   log keys the repair is supposed to account for. Counting events says a stream
//!   flowed; it says nothing about what is in the store, and a take that dropped the
//!   range's user keys emits every event a correct one does.
//!
//! What it deliberately does **not** assert: that the four ranges' installs
//! interleaved. That claim is SHARD.md §11 storage 5's, and it rests on
//! `InstallHoldsEveryRange`'s deterministic check in `ananke_shard::node`, where the
//! hold's scope is asserted directly — not on anything measurable here.
//! `streams_at_once` counts streams on the *sender*, which is a different thing, and
//! an earlier draft of this file described an assertion it had not written.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{
    Clock, Either, Environment, Network, NodeId, RangeCause, Rng, Socket, StartOver, TraceEvent,
    race,
};
use ananke_raft::ServerId;
use ananke_raft::apply::Command;
use ananke_raft::client::{Reply, Request};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_shard::client::{RangedRequest, RangedResponse};
use ananke_shard::range::RangeId;
use ananke_shard::server::ServerConfig;
use ananke_shard::variant::NodeVariants;
use ananke_storage::EngineConfig;
use bytes::Bytes;

use crate::raft::{TICK, client_addr, server_addr};
use crate::ranges::{DIR, FIRST_RANGE, RANGES, SNAPSHOT_CAP, range_of_key};

/// Voters in the cluster.
pub const NODES: u64 = 5;
/// The three that start at once: a majority of five, so the cluster makes progress
/// while the other two are not there.
pub const NODES_AT_ONCE: u64 = 3;
/// How many keys the writer cycles through.
const KEYS: u64 = 8;

/// The log length one range must pass before its leader takes a snapshot and compacts.
///
/// Low on purpose, and the opposite of the node sweep's `1 << 30`: this scenario is
/// *about* the snapshot path, and a threshold a run cannot reach would make every
/// assertion below an assertion against a silence.
pub const SNAPSHOT_THRESHOLD: u64 = 12;

/// How long the first three nodes run alone: long enough to elect on every range,
/// write past the threshold on every range, and compact.
const BEFORE: Duration = Duration::from_secs(6);
/// How long the five run together: long enough for eight streams and eight installs.
const AFTER: Duration = Duration::from_secs(14);

/// Which replica's claim one state report belongs to: a take writes one and the install
/// it feeds lands one, and they are paired by this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateKey {
    /// The server that reported it.
    pub server: u64,
    /// The range.
    pub range: u64,
    /// The snapshot's last index.
    pub last_index: u64,
    /// That entry's term.
    pub last_term: u64,
}

/// What a replica held when it made that claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held {
    /// The applied index.
    pub applied: u64,
    /// The range's user keys.
    pub user_keys: u64,
    /// An order-free digest of those keys and their values.
    pub user_digest: u64,
    /// The Raft log keys it holds.
    pub log_keys: u64,
}

/// What one run of the scenario found.
#[derive(Clone, Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// The node's variants.
    pub node_variants: NodeVariants,
    /// Writes the client saw committed before the late nodes joined: what says the run
    /// put work through the cluster at all, rather than electing and idling.
    pub wrote: u64,
    /// The run's trace.
    pub records: Vec<TraceRecord>,
    /// Which server each simulated node is. `RangeCreated` names a range and a cause
    /// and no server (SHARD.md §8 keeps the event per node, and the record's node is
    /// what carries it), so the check reads it through this.
    node_of: BTreeMap<NodeId, u64>,
}

impl Report {
    /// Streams opened, as (leader, range, follower).
    #[must_use]
    pub fn streams(&self) -> BTreeSet<(u64, u64, u64)> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftSnapshotStreams {
                    server, range, to, ..
                } => Some((*server, *range, *to)),
                _ => None,
            })
            .collect()
    }

    /// The most streams one leader had running at once, over the run.
    #[must_use]
    pub fn streams_at_once(&self) -> u64 {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftSnapshotStreams { streams, .. } => Some(*streams),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }

    /// Installs completed, as (server, range): a `RaftSnapshot` this server did not
    /// take is one it installed.
    #[must_use]
    pub fn installs(&self) -> BTreeSet<(u64, u64)> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftSnapshot {
                    server,
                    range,
                    taken: false,
                    ..
                } => Some((*server, *range)),
                _ => None,
            })
            .collect()
    }

    /// Every chunk the node refused, counted by the reason it gave (D-083's
    /// `RaftSnapshotStartOver`).
    ///
    /// The two that matter to RAFT.md:209-212's bounds are `Identity`, which the
    /// restart bound counts, and `Cap`, which it must not: a node's receive cap sits
    /// below its range count on purpose, so a bound that counted cap-waits would
    /// declare a usable checkpoint unusable as a matter of routine (D-087). Reported
    /// rather than asserted: this scenario's cap equals its range count, so what it
    /// measures is the *restart* side, and the cap-wait side is measured where it is
    /// built — the deterministic check in `ananke_shard::install`, and §12's re-seed
    /// shape, which sets the cap below the count.
    // PROPOSED(D-087): the restart and cap-wait counts are readable from a run.
    #[must_use]
    pub fn start_overs(&self) -> BTreeMap<StartOver, u64> {
        let mut counts = BTreeMap::new();
        for record in &self.records {
            if let TraceEvent::RaftSnapshotStartOver { reason, .. } = &record.event {
                *counts.entry(*reason).or_default() += 1;
            }
        }
        counts
    }

    /// Takes made, as (server, range).
    #[must_use]
    pub fn takes(&self) -> BTreeSet<(u64, u64)> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftSnapshot {
                    server,
                    range,
                    taken: true,
                    ..
                } => Some((*server, *range)),
                _ => None,
            })
            .collect()
    }

    /// Replicas an install created, as (server, range): §8's
    /// `RangeCreated { cause: snapshot }`.
    #[must_use]
    pub fn created_by_install(&self) -> BTreeSet<(u64, u64)> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RangeCreated {
                    range,
                    cause: RangeCause::Snapshot,
                    ..
                } => record
                    .node
                    .and_then(|node| self.node_of.get(&node))
                    .map(|server| (*server, *range)),
                _ => None,
            })
            .collect()
    }

    /// What each replica held when it claimed a snapshot's state: the take that wrote
    /// one and the install that landed one both report it, so the two can be compared.
    #[must_use]
    pub fn states(&self) -> BTreeMap<StateKey, Held> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftSnapshotState {
                    server,
                    range,
                    last_index,
                    last_term,
                    applied,
                    user_keys,
                    user_digest,
                    log_keys,
                } => Some((
                    StateKey {
                        server: *server,
                        range: *range,
                        last_index: *last_index,
                        last_term: *last_term,
                    },
                    Held {
                        applied: *applied,
                        user_keys: *user_keys,
                        user_digest: *user_digest,
                        log_keys: *log_keys,
                    },
                )),
                _ => None,
            })
            .collect()
    }

    /// The ranges the late nodes are meant to install: every one of them, on each.
    #[must_use]
    pub fn owed() -> BTreeSet<(u64, u64)> {
        let mut owed = BTreeSet::new();
        for server in NODES_AT_ONCE + 1..=NODES {
            for range in FIRST_RANGE..FIRST_RANGE + RANGES {
                owed.insert((server, range));
            }
        }
        owed
    }

    /// Whether the run reached the situation at all, which is the first thing to
    /// assert of a scenario built to reach one.
    ///
    /// A run that took no snapshot streamed nothing, and every assertion after it
    /// would pass against a silence — the failure mode CLAUDE.md's pinned-seed rule
    /// exists for and the one `sim/tests/node.rs` guards from the other side.
    ///
    /// # Errors
    ///
    /// Naming what was missing.
    pub fn reached(&self) -> Result<(), String> {
        let takes = self.takes();
        if takes.is_empty() {
            return Err(format!(
                "seed {}: no snapshot was taken at all, so nothing could be streamed: \
                 the scenario did not reach the path it exists to exercise",
                self.seed
            ));
        }
        let ranges: BTreeSet<u64> = takes.iter().map(|(_, range)| *range).collect();
        if ranges.len() < RANGES as usize {
            return Err(format!(
                "seed {}: only {} of {RANGES} ranges took a snapshot ({ranges:?}), so \
                 the ranges that did not are asserted about nothing",
                self.seed,
                ranges.len()
            ));
        }
        Ok(())
    }

    /// The run's verdict: every stream opened, every install completed, and every late
    /// replica created by the install that filled it.
    ///
    /// # Errors
    ///
    /// Naming the first thing that is missing, with the seed.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        self.reached()?;
        let owed = Self::owed();

        let streams = self.streams();
        let streamed: BTreeSet<(u64, u64)> =
            streams.iter().map(|(_, range, to)| (*to, *range)).collect();
        let missing: Vec<(u64, u64)> = owed.difference(&streamed).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: no stream was opened for {} of the {} (follower, range) \
                 pairs the late nodes are behind on: {missing:?}",
                missing.len(),
                owed.len()
            ));
        }

        let installs = self.installs();
        let missing: Vec<(u64, u64)> = owed.difference(&installs).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: {} of {} installs did not complete on the node: \
                 {missing:?}. A stream that flows and an install that lands are two \
                 different claims, and this is the second",
                missing.len(),
                owed.len()
            ));
        }

        let created = self.created_by_install();
        let missing: Vec<(u64, u64)> = owed.difference(&created).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: {} of {} replicas were filled by an install without \
                 `RangeCreated {{ cause: snapshot }}` being traced for them: \
                 {missing:?} (SHARD.md §8)",
                missing.len(),
                owed.len()
            ));
        }

        // A leader with two followers behind its compacted prefix feeds both at once
        // (Q14, D-043). One at a time is `CapStreamsSent`, and with a single follower
        // behind the prefix the two would be the same run.
        // The number itself is an observation and varies with the schedule — six on
        // seed 1 — so what is asserted is the property that makes `CapStreamsSent`
        // distinguishable at all: more than one at once, ever.
        if self.streams_at_once() < 2 {
            return Err(format!(
                "seed {seed}: no leader ever had two streams running at once, so a \
                 leader that fed its designated followers one at a time would have \
                 looked exactly like this one (Q14, D-043)"
            ));
        }

        // And the part no count of events can reach: **what landed**. Each installed
        // (server, range) is compared with the take that fed it, paired by the
        // snapshot's identity. A take that dropped the range's user keys, or carried
        // the leader's log with them, emits every event a correct one does.
        // PROPOSED(D-083): what an install installed is read back and checked.
        let states = self.states();
        for (server, range) in &owed {
            let installed = states
                .iter()
                .find(|(key, _)| key.server == *server && key.range == *range);
            let Some((key, held)) = installed else {
                return Err(format!(
                    "seed {seed}: server {server} installed range {range} and reported no \
                     state for it, so nothing says what it installed"
                ));
            };
            // The replica's applied index is the snapshot's last index: an install
            // that left it anywhere else has a state machine out of step with the
            // metadata describing it.
            let (last_index, last_term) = (key.last_index, key.last_term);
            if held.applied != last_index {
                return Err(format!(
                    "seed {seed}: server {server}'s range {range} installed the snapshot at \
                     {last_index} and came back applied at {}",
                    held.applied
                ));
            }
            // The bytes that landed are the bytes that were taken.
            let taken = states.iter().find(|(other, _)| {
                other.server != *server
                    && other.range == *range
                    && other.last_index == last_index
                    && other.last_term == last_term
            });
            let Some((source, want)) = taken else {
                return Err(format!(
                    "seed {seed}: server {server}'s range {range} installed a snapshot at \
                     ({last_index}, {last_term}) that no take on any other server reported \
                     writing, so there is nothing to compare it with"
                ));
            };
            if held.user_keys != want.user_keys || held.user_digest != want.user_digest {
                return Err(format!(
                    "seed {seed}: server {server}'s range {range} installed the snapshot \
                     server {} took at ({last_index}, {last_term}) and holds {} user keys \
                     (digest {}) where the take held {} (digest {}): the bytes that landed \
                     are not the bytes that were taken",
                    source.server,
                    held.user_keys,
                    held.user_digest,
                    want.user_keys,
                    want.user_digest
                ));
            }
            // A live install streams no log key, so what the receiver holds is its
            // kept tail and nothing else. A take that carried the leader's log would
            // leave those keys here, untombstoned (D-083's first departure).
            if held.log_keys > 0 {
                let tail_bound = self.highest_index(*range).saturating_sub(last_index);
                if held.log_keys > tail_bound {
                    return Err(format!(
                        "seed {seed}: server {server}'s range {range} holds {} log keys \
                         after installing at {last_index}, more than the {tail_bound} its \
                         kept tail can account for: the stream carried log keys the repair \
                         does not tombstone",
                        held.log_keys
                    ));
                }
            }
        }
        Ok(())
    }

    /// The highest index any replica of `range` reached: the bound on what a kept tail
    /// past a snapshot can hold.
    #[must_use]
    pub fn highest_index(&self, range: u64) -> u64 {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftAppend {
                    range: what, index, ..
                } if *what == range => Some(*index),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }
}

/// One node's configuration for this scenario: five voters, four ranges, and a
/// threshold low enough that the path is reached.
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
        ranges: crate::ranges::ranges(),
        initial_voters: (1..=NODES).map(ServerId).collect(),
        raft: RaftConfig {
            variants: variants.into(),
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: crate::ranges::INBOX_BYTES,
        // Not a scenario about the cap: at the node's range count, so no stream waits
        // by accident (D-075).
        snapshot_cap: SNAPSHOT_CAP,
        node,
    }
}

/// The simulator's configuration: a network that loses and delays, and a disk that
/// takes time. No bit rotting — a rotted table refuses a store, and Q15's re-seed is
/// another slice's.
#[must_use]
pub fn config(seed: u64) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.02;
    config.net.p_duplicate = 0.02;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(8);
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.0;
    config.fs.latency_min = Duration::from_micros(100);
    config.fs.latency_max = Duration::from_millis(2);
    config.run_length_hint =
        SimConfig::run_length_hint_for(u32::try_from(NODES + 1).expect("small"), BEFORE + AFTER);
    config
}

/// A writer that keeps every range's log growing until it is told to stop.
///
/// **It stops when the late nodes join**, and that is what makes this scenario
/// deterministic about the situation it claims. While it writes, every leader retakes
/// every `SNAPSHOT_THRESHOLD` entries; each retake gives the range a new snapshot
/// identity, the node's receiver answers the old stream `start_over`, and the stream
/// begins again at offset zero. Measured on the tree that found it, no stream in this
/// scenario ever resumed from a non-zero offset on any seed, and a stream was
/// re-opened up to twenty-one times before it landed — so whether all eight installs
/// completed inside the run was a race, not a property. Four seeds in two hundred and
/// fifty lost it, and tripling the window did not help.
///
/// Stopping the writer removes the race from *this* scenario. It did not remove it from
/// the node, and that was D-083's finding: a stream that kept being told to start over
/// never exhausted `CHUNK_RESENDS`, because the restart path reset that counter, and the
/// node had no restart bound of its own — which RAFT.md:210-212 specifies and the
/// one-group server honours. **The node has it now** (D-087), which is where that finding
/// went: the restart is counted, the third ask counts the checkpoint unusable, and a
/// cap-wait is a separate answer so the bound counts a start-over and not a stream
/// waiting its turn. The determinism here is kept on its own merits — this scenario is
/// about whether an install lands and what it lands, and it should not also be the only
/// thing standing between a livelock and a green run.
// PROPOSED(D-083): the scenario is deterministic about the situation it claims.
// PROPOSED(D-087): the node's restart bound is where that finding went.
async fn writer<E: Environment>(env: E, up_to: Arc<Mutex<u64>>, stop: Arc<AtomicBool>) {
    let Ok(sock) = env.net().bind(client_addr(1)).await else {
        return;
    };
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    let mut seq = 0u64;
    loop {
        if stop.load(Ordering::Relaxed) {
            env.clock().sleep(Duration::from_millis(20)).await;
            continue;
        }
        // Round-robin over the ranges, not over the keys. A writer that drew a key at
        // random would leave a range short of `snapshot_threshold` on some seeds, and
        // that range's late replica would then catch up by AppendEntries and need no
        // install at all — which is correct behaviour reported as a missing install.
        // Every range crosses its threshold here by construction, so the scenario
        // reaches the situation it is built to reach on every one of them.
        let key = Bytes::from(format!("k{}", (seq % RANGES) * 2 % KEYS));
        let range = range_of_key(&key);
        let value = Bytes::from(format!("v{seq}"));
        seq += 1;
        let target = leaders
            .get(&range)
            .copied()
            .unwrap_or_else(|| 1 + env.rng().below(NODES_AT_ONCE));
        let request = RangedRequest {
            range: RangeId(range),
            request: Request {
                client: 1,
                seq,
                command: Command::Put { key, value },
            },
        };
        let _ = sock.send(server_addr(target), request.encode()).await;
        let deadline = env.clock().now() + Duration::from_millis(80);
        loop {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep_until(deadline));
            let bytes = match race(&env, recv, timer).await {
                Either::Left(Ok((_, bytes))) => bytes,
                Either::Left(Err(_)) | Either::Right(()) => break,
            };
            let Ok(response) = RangedResponse::decode(bytes) else {
                continue;
            };
            match response.response.reply {
                Reply::NotLeader { leader: Some(who) } => {
                    leaders.insert(range, who.0);
                    break;
                }
                Reply::NotLeader { leader: None } => {
                    leaders.remove(&range);
                    break;
                }
                Reply::Outcome(_) => {
                    leaders.insert(range, target);
                    *up_to.lock().expect("the counter") += 1;
                    break;
                }
            }
        }
        env.clock().sleep(Duration::from_millis(4)).await;
    }
}

/// Runs the scenario for `seed`.
#[must_use]
pub fn run(seed: u64, variants: impl Into<Variants>, node_variants: NodeVariants) -> Report {
    let variants = variants.into();
    let mut sim = Sim::new(config(seed));
    let nodes: Vec<_> = (0..NODES as usize).map(|_| sim.add_node()).collect();
    let client = sim.add_node();
    let committed: Arc<Mutex<u64>> = Arc::default();
    let stop: Arc<AtomicBool> = Arc::default();
    for id in 1..=NODES_AT_ONCE {
        spawn(&sim, nodes[id as usize - 1], id, variants, node_variants);
    }
    {
        let env = sim.env(client);
        let inner = env.clone();
        let committed = committed.clone();
        let stop = stop.clone();
        env.spawn("client", writer(inner, committed, stop));
    }
    // The three run alone: every range elects, writes past its threshold and compacts.
    sim.run_for(BEFORE);
    // The writer stops here, so every leader's last take stands for the rest of the
    // run and no stream races a retake of its own snapshot. What each range holds is
    // now fixed, which is also what makes the installed state comparable with the take
    // that fed it.
    stop.store(true, Ordering::Relaxed);
    let wrote = *committed.lock().expect("the counter");
    // And the other two arrive, behind every range's compacted prefix.
    for id in NODES_AT_ONCE + 1..=NODES {
        spawn(&sim, nodes[id as usize - 1], id, variants, node_variants);
    }
    sim.run_for(AFTER);
    Report {
        seed,
        node_variants,
        wrote,
        records: sim.trace(),
        node_of: nodes
            .iter()
            .enumerate()
            .map(|(at, node)| (*node, at as u64 + 1))
            .collect(),
    }
}

fn spawn(sim: &Sim, at: NodeId, id: u64, variants: Variants, node: NodeVariants) {
    let env = sim.env(at);
    let inner = env.clone();
    env.spawn("node", async move {
        let _ = ananke_shard::server::run(inner, server_config(id, variants, node)).await;
    });
}

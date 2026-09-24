//! The Phase 2 sweep (SPEC.md §3, RAFT.md §2, §5): three servers running
//! `ananke_raft::node` and two clients, under drops, duplicates, reordering, clock
//! skew and drift, partitions, one-way blocks and crashes with the §1.3 disk model.
//! [`run`] draws a fault schedule from the seed, drives it, and returns a [`Report`]
//! whose [`check`](Report::check) states what the run must satisfy:
//!
//! - the four log invariants of Figure 3 and the three rule folds, from
//!   `ananke_raft::invariants`, over the trace;
//! - linearizability of the clients' history, by [`crate::lin`], which is what
//!   decides whether a read served by a lease was stale (RAFT.md §2, invariant 6);
//! - pre-vote's property: a server cut off from the others does not raise its term;
//! - on seeds scheduled uniformly, where a task cannot be starved: after the last
//!   fault heals a client write completes within [`LIVENESS_TIMEOUTS`] election
//!   timeouts; and a follower that hears from no current leader and grants no vote
//!   for [`TIMER_TIMEOUTS`] timeouts campaigns, the rule a server that resets its
//!   timer on any message breaks.
//!
//! Every server's clock is drawn per seed (RAFT.md §3): on half the seeds every
//! rate error is within a third of [`DRIFT_BOUND_PPM`], so the lease's assumption
//! holds. On the other half one server runs slow and two run fast, so the
//! assumption fails between every pair, and the guard must revoke or the checker
//! must see a stale read: on half of those the magnitudes are moderate, two
//! thousand to a hundred thousand parts per million, where the guard has to notice
//! movement the lease's margin still absorbs; on the rest they are severe, the slow
//! server at a quarter to two fifths of true rate and the fast ones fifteen to
//! thirty-five percent fast, where a lease the slow server measures by its own
//! clock outlives the fast followers' timers and a read it serves is stale. No real
//! clock does that; with a hundred-millisecond election timeout and a tick of
//! margin it is what the arithmetic needs, and the arithmetic is the same at any
//! scale. [`Report::drift_exceeded`] says which a seed was.
//!
//! A slow clock never leads on its own: its timer fires late in real time and the
//! fast servers win every election. So every schedule opens with two lease trials
//! ([`Trial`]), one after the other: an operator asks the leader to hand over to
//! the server with the slowest clock (leadership transfer, thesis §3.10), its
//! lease forms, and it is then cut off with a reading client while the others
//! elect and write. The adversary chose the clocks and chooses the operator's
//! request; correlating them is its privilege. Under the guard the slow leader
//! trusts no promise whose offset moved and serves by heartbeat round; without it
//! (`Variant::LeaseTrustsTheClock`) it serves a stale read. One trial's window is
//! a coin toss of the fault geometry; two per seed keep the variant's catch rate
//! from being one window's statistical noise.
//!
//! One fault is a driver rather than an outage: [`Fault::FigureEight`] opens the
//! §5.4.2 window at the default batch size (issue #22, D-031). A follower a new
//! leader must catch up in more than one AppendEntries only exists behind a
//! backlog of more than `max_batch` uncommitted entries, which client-paced
//! traffic never builds: the driver isolates a follower with a client, fires a
//! burst of puts at the leader without awaiting replies, crashes the leader with
//! the burst appended, and steers it back into the lead — a restart resets the
//! commit index, the third server's sends are blocked so only the restarted
//! leader can assemble a majority, and the isolated follower votes it in. The new
//! leader re-sends its backlog in batches, and one that counts older-term
//! replicas for commit (`Variant::CountOlderTermForCommit`) advances its commit
//! index onto an older term's entry at the first acknowledgement below its no-op,
//! which the commit-by-current-term fold reports.
//!
//! Every fault-model test runs a known-buggy variant beside the correct one
//! (CLAUDE.md): each [`Variant`](ananke_raft::core::Variant) of RAFT.md §5 that
//! this stage ships must be caught
//! by one of these checks on some seed, and the correct server must pass every seed.
//!
//! The disk honours `fsync` here (`p_durable = 1`): a disk that acknowledges a sync
//! it did not do loses persistent state, and Raft's safety argument assumes it is
//! persistent (D-026). Bit rot and torn writes stay on, and the engine's checksums
//! turn them into a refusal (`RaftRefused`) rather than a hole.
//!
//! Clients keep the history honest (RAFT.md §4, `ananke_raft::client`): a write
//! whose answer never comes is not resent, since the entry may yet commit; it is
//! abandoned as pending and the client continues as a new process. A get may be
//! retried, since a second read changes nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{RunHeader, Sim, SimConfig, TraceRecord};
use ananke_env::{
    ApplyEffect, ClientOp, ClientResult, Clock, Either, Environment, Instant, Network, NodeId, Rng,
    Socket, TraceEvent, race,
};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::message::{self, Frame, Message};
use ananke_raft::node::SINGLE_GROUP;
use ananke_raft::store::LOST_STATE;
use ananke_raft::{NodeConfig, ServerId, invariants, run as run_server};
use ananke_shard::client::{RangedRequest, RangedResponse};
use ananke_shard::range::RangeId;
use ananke_shard::server::ServerConfig;
use ananke_shard::variant::NodeVariants;
use ananke_storage::EngineConfig;
use bytes::Bytes;
use moirae_sched::Policy;

use crate::lin::{self, History};

/// How many servers.
pub const SERVERS: u64 = 3;
/// How many clients, each on its own node.
pub const CLIENTS: u64 = 2;
/// How many keys the clients touch: few, so that reads and writes meet.
pub const KEYS: u64 = 2;
/// The minimum election timeout; the maximum is twice it (RAFT.md §1).
pub const ELECTION_MIN: Duration = Duration::from_millis(100);
/// One tick of the core: `ELECTION_MIN` over the core's minimum election ticks.
pub const TICK: Duration = Duration::from_millis(10);
/// How long a client waits for a get before abandoning it, across its tries.
pub const OP_TIMEOUT: Duration = Duration::from_millis(250);
/// How long a client waits on one server before trying another, for a get.
pub const TRY_TIMEOUT: Duration = Duration::from_millis(40);
/// How long a client waits for a write, which gets one try: a second copy would
/// be a second write, and a client that waits longer on a leader that has been
/// cut off writes nothing while the lease it should be testing is live.
pub const WRITE_TIMEOUT: Duration = Duration::from_millis(60);
/// The pause between a client's operations.
pub const OP_GAP: Duration = Duration::from_millis(5);
/// After the last heal, a client write must complete within this many maximum
/// election timeouts.
pub const LIVENESS_TIMEOUTS: u32 = 10;
/// A follower that hears from no current leader and grants no vote for this many
/// maximum election timeouts must have started an election.
pub const TIMER_TIMEOUTS: u32 = 2;
/// The bound on the rate at which two servers' clocks may drift apart, in parts per
/// million, that the lease assumes (RAFT.md §1).
pub const DRIFT_BOUND_PPM: u64 = 1_000;
/// Where each server keeps its store.
pub const DIR: &str = "/raft";
/// The slice of virtual time the run advances between looks at the trace.
pub const SLICE: Duration = Duration::from_millis(50);
/// How often, in slices, the safety folds run over the trace so far.
pub const CHECK_EVERY: u32 = 10;
/// The most trace records a run may produce before it is stopped as a runaway:
/// the correct server produces a few tens of thousands.
pub const TRACE_CAP: usize = 400_000;

/// This scenario's `snapshot_threshold`: the entries a log may outgrow its
/// prefix by before its replica compacts. 4 096 in the server's default
/// (`core.rs`); 12 here, so that compaction and the snapshot path are reached
/// inside a run.
// PROPOSED(D-078): the follower log bound is stated as a multiple of this.
pub const SNAPSHOT_THRESHOLD: u64 = 12;

/// The bound on the largest in-memory log of any follower replica, **in entries**
/// (Stage B's exit; Q39).
///
/// It is a number of entries and not a multiple of [`SNAPSHOT_THRESHOLD`], though
/// 768 is 64 × this scenario's threshold of 12 and that is how it was chosen. What
/// the bound measures is a follower's *apply lag* in entries, and the threshold
/// does not scale that lag: written as a product, raising the threshold for some
/// unrelated reason would silently double the bound while the measured maximum
/// barely moved. A threshold change has to be re-measured against this number
/// instead — which is the point (D-039). At the server's own 4 096 the same lag
/// would be a fraction of one threshold, so read as a production figure the
/// multiple is very conservative; SHARD.md's exit asks for it stated there, and
/// 64 × 4 096 is 262 144 entries, a bound nothing could trip.
///
/// Measured before it was asserted, on the correct system under this scenario's
/// client writes; D-078 records the sweep, the command and the machine. At a
/// thousand seeds the largest was **342 entries, 28.5 ×** the threshold, on server
/// 1 of seed 514, over a distribution whose bulk sits at 4 to 8 ×: 50 seeds of a
/// thousand reach 12 ×, 13 reach 16 ×, 8 reach 20 × and 1 reaches 28 ×. **64 ×**
/// leaves 2.25 × over that maximum.
///
/// The tail is **not** geometric, and the risk model this comment first carried —
/// ten thousand seeds reaching about 38 ×, passing 48 × one run in ten, 64 ×
/// about one run in 470 — was refuted by the nightly it was written beside: ten
/// thousand seeds reached 28 ×, on seed 514, the identical seed and the identical
/// maximum as one thousand. What can honestly be said is that neither tier has
/// come within 2 × of this bound.
///
/// It is not a vacuous bound, and it is not an unfalsifiable one. Under
/// [`ananke_raft::core::Variant::FollowerNeverCompacts`] — the server as it was
/// built before D-065 — the same sweep at the same tier reaches **878 entries,
/// 73 ×**, on seed 512, and would have kept growing with a longer run: a
/// follower's log had nothing to bound it at all (SHARD.md:339-340). That variant
/// is caught on 5 of the first thousand seeds, by this bound on all five, and
/// `a_replica_that_never_compacts_outgrows_the_follower_log_bound` pins seed 512
/// against it. A bound the correct system trips is a model error to take to the
/// owner, never a number to widen (D-030, D-039).
///
// PROPOSED(D-078): a follower compacts its log to its own applied index.
pub const FOLLOWER_LOG_BOUND: u64 = 768;

/// The maximum election timeout.
#[must_use]
pub fn election_max() -> Duration {
    ELECTION_MIN * 2
}

/// The address of server `id` (1-based).
#[must_use]
pub fn server_addr(id: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, u8::try_from(id).expect("small")], 7000))
}

/// The address of client `n` (1-based).
#[must_use]
pub fn client_addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 1, u8::try_from(n).expect("small")], 7000))
}

/// The operator's address for its `n`th request (1-based): one socket per
/// request, so a second trial's transfer does not depend on the first socket's
/// fate.
#[must_use]
pub fn admin_addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 2, u8::try_from(n).expect("small")], 7000))
}

/// The burst client's address for the schedule's `n`th Figure 8 driver (1-based):
/// its own socket per driver, like the operator's per request.
#[must_use]
pub fn burst_addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 3, u8::try_from(n).expect("small")], 7000))
}

/// The filling client's address for the schedule's `n`th
/// [`Fault::RetakeUnderStream`] (1-based): its own socket per driver, like the
/// burst client's. (D-043).
#[must_use]
pub fn spread_addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 4, u8::try_from(n).expect("small")], 7000))
}

/// The server bound to `addr`, if it is a server's.
#[must_use]
pub fn server_of(addr: SocketAddr) -> Option<u64> {
    (1..=SERVERS).find(|&id| server_addr(id) == addr)
}

/// A node id (1-based, like the trace) for server `id`: servers are added first.
fn node_of_server(id: u64) -> NodeId {
    NodeId::new(u32::try_from(id).expect("small"))
}

/// The system this scenario's arms are driven against (SHARD.md §12, Stage B's
/// first exit criterion).
///
/// Every arm below — the isolations, the one-way blocks, the crashes, the lease
/// trials, the Figure 8 driver, the crashes aimed at an install, an adoption and a
/// refusal — is written once and driven against whichever of these the run names.
/// What varies between them is small and is all here: how a server is spawned,
/// which ranges it holds, which range an arm aims at, who leads that range, and how
/// a client addresses it. Everything else — when an arm fires, what it waits for,
/// what it heals — is the same code, so the two clusters cannot drift apart
/// (PROPOSED D-082).
///
/// [`Cluster::OneGroup`] keeps Phase 2's unit and Phase 2's draws: a schedule drawn
/// for it draws from exactly the streams it drew from before this enum existed, in
/// exactly the order, so every seed pinned in `sim/tests/raft.rs` runs the run it
/// was pinned on. [`Cluster::Node`] is §4's node, and its schedule takes the range
/// each leader-relative arm aims at from a stream of its own (§11, env 8; D-031).
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cluster {
    /// One server, one Raft group: [`ananke_raft::run`], the unit Phase 2 built
    /// and the one every pinned seed of this sweep was pinned against.
    OneGroup,
    /// The node of SHARD.md §4: [`ananke_shard::server::run`], four ranges on every
    /// node over one socket, one engine, one inbox and one ticker.
    Node,
}

/// The node cluster's `snapshot_threshold`, far above what a run writes.
///
/// Every snapshot action a core can ask for is behind this number. A leader asks for
/// a take when its log has outgrown its compacted prefix by the threshold
/// (`core.rs`), a follower asks for a record on the same condition (D-078), and a
/// leader asks for an install only for a follower whose `next` has fallen at or below
/// the compacted prefix — which with nothing compacted is index 0, and no follower's
/// `next` is ever that. So a run whose highest index stays below this asked for
/// nothing, and that is what [`Report::highest_index`] lets a sweep assert rather
/// than assume.
// PROPOSED(D-082): what the node cluster does not reach yet, asserted absent.
pub const NODE_SNAPSHOT_THRESHOLD: u64 = 1 << 30;

/// The keys the node cluster's clients draw from: two per range, as the node
/// scenario's are ([`crate::ranges::KEYS`]), so a range's liveness is about a key
/// some client wrote and not about the one key the cluster has.
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
pub const NODE_KEYS: u64 = 2 * crate::ranges::RANGES;

impl Cluster {
    /// The ranges every server of this cluster holds, in id order.
    #[must_use]
    pub fn ranges(self) -> Vec<u64> {
        match self {
            Self::OneGroup => vec![SINGLE_GROUP],
            Self::Node => (crate::ranges::FIRST_RANGE
                ..crate::ranges::FIRST_RANGE + crate::ranges::RANGES)
                .collect(),
        }
    }

    /// How many keys a client draws from.
    #[must_use]
    pub fn keys(self) -> u64 {
        match self {
            Self::OneGroup => KEYS,
            Self::Node => NODE_KEYS,
        }
    }

    /// The map from a key to the range that serves it, which the write bound and
    /// the liveness check read per key (D-071).
    #[must_use]
    pub fn key_range(self) -> fn(&Bytes) -> u64 {
        match self {
            Self::OneGroup => range_of_key,
            Self::Node => node_range_of_key,
        }
    }

    /// How likely a block is to rot on this cluster's disk.
    ///
    /// The one-group server's disk rots, and its engine's checksums turn a rotted
    /// table into a refusal the server survives by re-seeding. The node's does not,
    /// and the reason is the absence this cluster asserts rather than hides: a
    /// refusal on the node stops the node, because Q15's whole-node refusal and its
    /// re-seed are another slice's (PR #86, D-077) and are not in this tree. A node
    /// that stopped would take its four ranges with it for the rest of the run and
    /// the liveness checks would be measuring a scenario nobody wrote. The sweep
    /// asserts that no store was refused and says why, so the day the path arrives
    /// the sweep says so instead of passing over it.
    // PROPOSED(D-082): what the node cluster does not reach yet, asserted absent.
    #[must_use]
    pub fn bitrot(self) -> f64 {
        match self {
            Self::OneGroup => 0.02,
            Self::Node => 0.0,
        }
    }

    /// Spawns server `id` on its node, as a start or as a restart.
    fn spawn(self, sim: &Sim, id: u64, variants: Variants, node: NodeVariants) {
        let env = sim.env(node_of_server(id));
        let inner = env.clone();
        match self {
            Self::OneGroup => {
                env.spawn("raft", async move {
                    let _ = run_server(inner, node_config(id, variants)).await;
                });
            }
            Self::Node => {
                env.spawn("node", async move {
                    let _ =
                        ananke_shard::server::run(inner, node_server_config(id, variants, node))
                            .await;
                });
            }
        }
    }

    /// The request a client of this cluster puts on the wire for `range`.
    ///
    /// Crate-visible rather than private because `crate::membership` drives its own
    /// operator sockets against either cluster and encodes on them (PROPOSED D-084).
    pub(crate) fn encode(self, range: u64, request: Request) -> Bytes {
        match self {
            Self::OneGroup => request.encode(),
            Self::Node => RangedRequest {
                range: RangeId(range),
                request,
            }
            .encode(),
        }
    }

    /// The answer in a packet a client of this cluster received, if it is one.
    ///
    /// Crate-visible for the same reason as [`Cluster::encode`] (PROPOSED D-084).
    pub(crate) fn decode(self, bytes: Bytes) -> Option<Response> {
        match self {
            Self::OneGroup => Response::decode(bytes).ok(),
            Self::Node => RangedResponse::decode(bytes).ok().map(|r| r.response),
        }
    }
}

/// The node cluster's map from a key to its range: the scenario's fixed map
/// (SHARD.md, Stage B), which every client, driver and check reads.
///
/// A key is `k`, the number of the first key of its range's span, and whatever the
/// writer wants after it: the clients write `k0` to `k7`, two to a range, and the
/// Figure 8 driver's burst and the re-take driver's fill write keys of their arm's
/// own range — `k4b`, `k4f2.7` — which sort inside that range's span, so the map
/// and the spans `RangeCreated` names agree. A key the map does not know is the
/// first range's, so the map is total, as a routing table must be.
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
#[must_use]
pub fn node_range_of_key(key: &Bytes) -> u64 {
    let digits = std::str::from_utf8(key)
        .ok()
        .and_then(|key| key.strip_prefix('k'))
        .map(|rest| {
            rest.split(|c: char| !c.is_ascii_digit())
                .next()
                .unwrap_or("")
        })
        .filter(|digits| !digits.is_empty())
        .and_then(|digits| digits.parse::<u64>().ok())
        .unwrap_or(0);
    crate::ranges::FIRST_RANGE + (digits / 2).min(crate::ranges::RANGES - 1)
}

/// The first key of range index `pick`'s span: what a driver aimed at that range
/// prefixes its keys with, so that they sort inside the span (`node_range_of_key`).
fn node_key_prefix(pick: u64) -> String {
    format!("k{}", pick * 2)
}

/// One fault of a schedule. Every fault heals before the next starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// A symmetric partition: `server` and client `client` on one side, everyone
    /// else on the other.
    Isolate {
        /// The server cut off.
        server: u64,
        /// The client on its side (1-based).
        client: u64,
        /// How long.
        for_: Duration,
    },
    /// The leader in force isolated with client 1, the others and client 2 on the
    /// other side.
    IsolateLeader {
        /// How long.
        for_: Duration,
    },
    /// One direction of one link between servers blocked.
    OneWay {
        /// Messages from this server...
        from: u64,
        /// ...to this one are dropped.
        to: u64,
        /// How long.
        for_: Duration,
    },
    /// A server crashes and restarts after `down`.
    Crash {
        /// Which.
        server: u64,
        /// How long it stays down.
        down: Duration,
    },
    /// The leader in force crashes and restarts after `down`.
    CrashLeader {
        /// How long it stays down.
        down: Duration,
    },
    /// The rule-5 scenario (RAFT.md §5): the links towards `server`, a follower,
    /// are blocked for `one_way` while its own messages still arrive, so it hears
    /// nothing, times out, and pre-votes into the majority every timeout, each
    /// request declined; `crash_after` into that, the leader crashes for `down`. A
    /// follower that resets its timer on the declined requests does not campaign
    /// while the leader is down. Under check quorum a deposed leader stops its
    /// stale heartbeats within an election timeout, so this is the stale sender
    /// that lasts (D-028).
    StaleSender {
        /// The follower cut off from receiving; the leader's neighbour if it leads.
        server: u64,
        /// How long the links towards it stay blocked.
        one_way: Duration,
        /// When the leader crashes, into the block.
        crash_after: Duration,
        /// How long it stays down.
        down: Duration,
    },
    /// The Figure 8 driver (issue #22, D-031): the §5.4.2 window at the default
    /// batch size, which needs a new leader re-sending more uncommitted older-term
    /// entries than one AppendEntries carries. `follower` is isolated with
    /// `client` while the majority commits; a burst of puts is fired at the
    /// leader without awaiting replies, so its log runs far ahead of the isolated
    /// follower; the leader crashes with the burst appended and restarts with its
    /// commit index reset, since the commit index is not persisted; the third
    /// server's sends are blocked so neither it nor the isolated follower can
    /// assemble a majority, and the restarted leader, holding the longest log,
    /// campaigns and wins with the isolated follower's vote. It then re-sends its
    /// backlog in batches of `max_batch`, and a leader that counts older-term
    /// replicas for commit advances onto an older term's entry at the first
    /// acknowledgement below its no-op, which commit-by-current-term reports.
    FigureEight {
        /// The follower cut off with `client`; the leader's neighbour if it leads.
        follower: u64,
        /// The client on its side (1-based).
        client: u64,
        /// How long the isolation runs before the burst: the majority commits
        /// ahead of the isolated follower.
        settle: Duration,
        /// How many puts the burst fires: drawn so the appended backlog exceeds
        /// `max_batch` with margin.
        burst: u64,
        /// How long after the burst starts the leader crashes: time to append
        /// most of it, one synced batch per entry.
        crash_after: Duration,
        /// How long the crashed leader stays down.
        down: Duration,
        /// How long the third server's sends stay blocked past the restart: time
        /// for the old leader to campaign and re-send its backlog.
        steer: Duration,
    },
    /// A crash aimed at the middle of a snapshot install (RAFT.md §5, stage E).
    /// First `server` is isolated for `isolate`, long enough to fall behind the
    /// snapshot threshold and go quiet past the designation, so the leader
    /// compacts and feeds it the snapshot on heal; then the run advances in
    /// small slices until the stream's final chunk is delivered to it, waits
    /// `grace` for the receiver's repair to be in flight, and crashes it for
    /// `down`. The correct install has no window here — its staging directory is
    /// not a store until the repair is durable and `CURRENT` is written last —
    /// while `Variant::SnapshotWithoutCurrentLast` comes back on the leader's
    /// identity, which state machine safety reports. If no final chunk lands
    /// within [`INSTALL_WAIT_BUDGET`], the crash never fires and the fault was
    /// an isolation. Drawn from its own `moirae_sched` stream
    /// ("snapshot-crash"), never lengthening the shared schedule stream (D-031).
    CrashInstalling {
        /// The follower isolated and then crashed mid-install; the leader's
        /// neighbour if it leads when the fault starts.
        server: u64,
        /// How long it is cut off first, to fall behind the threshold.
        isolate: Duration,
        /// How long after the final chunk's delivery the crash lands.
        grace: Duration,
        /// How long the receiver stays down.
        down: Duration,
    },
    /// A crash aimed at the adoption of a completed install (D-041).
    /// The same setup as [`Fault::CrashInstalling`] — `server` isolated for
    /// `isolate` so the leader feeds it a snapshot on heal — but the run then
    /// waits for the receiver to trace the install complete (`RaftSnapshot {
    /// taken: false }`, after which its next incarnation adopts the staged store
    /// at its start), watches the receiver's store directory on the durable disk
    /// and crashes it the moment the adoption's first change to that directory
    /// is durable: the old store's files all gone, or a file not there before
    /// synced in, whichever the adoption does first. The receiver is down for
    /// `down`, restarts into the adoption the crash interrupted, and is crashed
    /// the same way again, `crashes` times in all, the way a machine that
    /// reboots into the same work is; a restart whose adoption makes no durable
    /// change within [`ADOPTION_WAIT_BUDGET`] is crashed at the budget's end.
    /// The crash-safe adoption copies and syncs before it touches the old store,
    /// so its first durable change is a copy synced in with the old store whole,
    /// and every crash here re-runs the adoption on the same staged bytes;
    /// `Variant::AdoptionAsBuilt` deletes the old store first, so its first
    /// durable change is the old store gone with the copies' entries not yet
    /// synced, and a crash there whose bit rot lands on the staging `CURRENT` —
    /// one block, two per cent per crash — restarts the server on a fresh store,
    /// which committed-entries-stay reports (the nightly's seed 6325). `crashes`
    /// is that many rolls of the rot's dice. If no install completes within
    /// [`INSTALL_WAIT_BUDGET`], no crash fires and the fault was an isolation.
    /// Drawn from its own `moirae_sched` stream ("adoption-crash"), never
    /// lengthening the shared schedule stream or the install crash's (D-031),
    /// and drawn on one seed in [`ADOPTION_STORM_IN`] rather than on every
    /// schedule: the storm is the most expensive fault the sweep carries and the
    /// share is what keeps `scripts/premerge.sh` inside its quarter of an hour.
    CrashAdopting {
        /// The follower isolated and then crashed mid-adoption; the leader's
        /// neighbour if it leads when the fault starts.
        server: u64,
        /// How long it is cut off first, to fall behind the threshold.
        isolate: Duration,
        /// How long the receiver stays down after each crash.
        down: Duration,
        /// How many times it is crashed.
        crashes: u64,
    },
    /// A crash storm aimed at the window a refusal has to be laundered in
    /// (D-044). Each round waits for `server` to rotate a memtable it
    /// has not finished flushing and crashes it there: until that flush switches
    /// `CURRENT`, the manifest in force is the older one, so the log tail is two
    /// memtables and the open after the crash replays enough to fill a memtable
    /// and rotate it again. When that open is also refused for lost state — the
    /// same crash's bit rot landing in a table the manifest lists — the engine
    /// as built flushes what it replayed and writes over the loss, and the crash
    /// after *that* is the one that matters.
    ///
    /// Both halves are needed, and the second follows from the first. The
    /// engine as built launders a refusal away only if the memtable its recovery
    /// replayed is over the threshold and is flushed: a table, a manifest
    /// without the dropped one, `CURRENT` switched to it, and the log segments
    /// that held the lost records deleted. A crash at an ordinary moment leaves
    /// a tail of *one* memtable, which replays into a memtable that never
    /// rotates and is never flushed, so nothing is written over the loss and the
    /// store stays visibly damaged: over a hundred release seeds the sweep's
    /// sixty-odd refusals laundered nothing at all, and the aim is what makes
    /// the replay big enough. At the sweep's write rate a server fills a
    /// sixteen-kilobyte memtable about every two seconds and takes some fifteen
    /// milliseconds to flush it, so the window is a hundredth of the time and no
    /// crash at a moment of its own choosing finds it. Once the loss is
    /// laundered the store is self-consistent, and the next crash and restart
    /// opens it clean — no refusal, no install — so a voter with a hole in its
    /// state machine rejoins and pre-votes, which state machine safety reports
    /// at the restatement whose log cannot account for the applied index it
    /// recovered (the thousand-seed premerge, seed 687).
    ///
    /// With the fix there is nothing to aim at: the refused engine is quiesced
    /// before it can flush, and the store's marker says it lost state, so every
    /// restart is refused again until an install replaces the store. A victim
    /// already sitting refused when a round comes is crashed after `grace`
    /// instead, without waiting for a flush it will never make. If no flush
    /// begins within [`FLUSH_WAIT_BUDGET`] the round crashes at the budget's
    /// end anyway, which is a crash at an ordinary moment, the sweep's usual
    /// kind. Drawn from its own `moirae_sched` stream ("refusal-crash"), never
    /// lengthening the shared schedule stream or the other crashes' (D-031).
    CrashRefused {
        /// The server crashed and restarted; the leader's neighbour if it leads
        /// when the fault starts.
        server: u64,
        /// How long the victim stays down after each crash.
        down: Duration,
        /// How long after a refusal the crash lands: time enough for the engine
        /// as built to finish flushing what the recovery replayed, which takes
        /// some fifteen milliseconds at the sweep's disk latencies.
        grace: Duration,
        /// How many times it is crashed.
        crashes: u64,
    },
    /// The shape D-043 named and left to the sweep's owner: a leader
    /// re-taking a snapshot while a stream to a designated follower is in
    /// flight, with a second designated follower behind it.
    ///
    /// Three steps. First `follower` is isolated for `isolate`, long enough to
    /// fall behind the snapshot threshold and go quiet past the designation, as
    /// [`Fault::CrashInstalling`] does, so a stream follows the heal; the run
    /// then advances in small slices until that stream opens
    /// ([`TraceEvent::RaftSnapshotStreams`]), or [`STREAM_WAIT_BUDGET`] runs
    /// out, in which case the fault was an isolation and nothing else. Second,
    /// the leader's *other* follower is cut off for `freeze`. The leader keeps
    /// its quorum — the follower it is feeding answers every heartbeat, so
    /// check quorum is satisfied — but it has nobody left to count: the fed
    /// follower's match is far behind and the cut-off one is unreachable, so
    /// nothing commits and the leader's applied index stands still at the index
    /// it last took. That is the whole trick. A take goes at the applied index,
    /// so while it stands still every take the leader is asked for is a take at
    /// the index the running stream is reading, which as built means the one
    /// directory that stream has open: `snapshot::take` sweeps it and writes it
    /// again, the sender's file list is stale, the stream restarts from offset
    /// 0, and a stream that restarts twice is given up with `retake`, which
    /// clears the leader's checkpoint and asks for the take that scrambles the
    /// next one. The correct server takes into a new numbered version and the
    /// stream reads the version it pinned untouched (D-043). Third,
    /// the cut-off follower heals having itself fallen behind and gone quiet, so
    /// it too is designated: as built it waits in the backlog behind a stream
    /// that never ends and is fed nothing, while the correct server streams to
    /// both at once. With neither follower countable the commit index does not
    /// move again, and the liveness check reports it `hold` and the gap and the
    /// settle later — which is seed 5909's wedge, assembled rather than waited
    /// for. Drawn from its own `moirae_sched` stream ("retake-stream"), never
    /// lengthening the shared schedule stream or any other arm's (D-031).
    /// (D-043).
    RetakeUnderStream {
        /// The follower isolated and then fed the snapshot; the leader's
        /// neighbour if it leads when the fault starts.
        follower: u64,
        /// How many filling puts open the fault, each on its own key and
        /// four hundred bytes long: the state machine, and with it the
        /// checkpoint, has to be worth streaming before any of this is aimable.
        fill: u64,
        /// How long the filling puts are given to commit and reach the disk
        /// before the isolation begins.
        settle: Duration,
        /// How long it is cut off first, to fall behind the threshold.
        isolate: Duration,
        /// How long the leader's other follower is cut off once the stream is
        /// in flight: the window in which the leader commits nothing, its
        /// applied index stands still, and every take it is asked for lands at
        /// the index the stream is reading.
        freeze: Duration,
        /// The quiet after that follower heals, with both designated.
        hold: Duration,
    },
    /// Issue #32's shape, aimed rather than waited for (D-050): a message that
    /// raises a server's term delivered before an isolation begins and taken by a
    /// step inside it, because the server's `raft` task was still awaiting a
    /// persist when the message arrived. Each of `tries` rounds asks the leader in
    /// force to hand over to the follower after it (leadership transfer, thesis
    /// §3.10), whose campaign sends the third server a RequestVote of a higher
    /// term; the run advances in slices of [`TERM_RAISE_STEP`] until a message from
    /// a server carrying a term above the third server's last traced term is
    /// delivered to it, or [`TERM_RAISE_WAIT_BUDGET`] runs out, and cuts that server
    /// off alone at the slice's end for `isolate`, then leaves `quiet` before the
    /// next round. A slice ends with nothing runnable, so a `raft` task that was idle
    /// has already taken the message, before the isolation, and one that was busy
    /// takes it inside the window: the shape. Never drawn by [`Schedule::draw`], so
    /// no sweep schedule moves; the directed scenario
    /// [`Schedule::term_raise_behind_a_step`] is its one user.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    IsolateOnTermRaise {
        /// How many rounds.
        tries: u64,
        /// How long each isolation lasts.
        isolate: Duration,
        /// The quiet after each round.
        quiet: Duration,
    },
}

/// The slice [`Fault::IsolateOnTermRaise`] advances in while it waits for a
/// term-raising delivery: a tenth of the sweep disk's fastest operation, so an
/// isolation begins well inside any persist that was running at the delivery.
/// (D-050).
pub const TERM_RAISE_STEP: Duration = Duration::from_micros(10);

/// The longest a [`Fault::IsolateOnTermRaise`] round waits for a term-raising
/// delivery after asking for the transfer. A transfer's campaign reaches the
/// other follower within a few network delays. (D-050).
pub const TERM_RAISE_WAIT_BUDGET: Duration = Duration::from_millis(300);

/// The first sequence number and admin socket a [`Fault::IsolateOnTermRaise`]
/// round's transfer request uses, past the lease trials' two. (D-050).
const TERM_RAISE_ADMIN: u64 = 3;

/// The longest a [`Fault::CrashInstalling`] waits for an install to stream
/// before giving up and doing nothing.
pub const INSTALL_WAIT_BUDGET: Duration = Duration::from_millis(4000);

/// The longest a [`Fault::RetakeUnderStream`] waits, after healing the follower
/// it cut off, for a stream to that follower to open before giving up and
/// leaving the fault an isolation. A designation costs two minimum election
/// timeouts of quiet and the stream opens on the next heartbeat after the heal,
/// so a stream that is coming has come well inside this; the budget is shorter
/// than [`INSTALL_WAIT_BUDGET`] because it waits for the stream's *opening*, not
/// for a chunk of it to land. (D-043).
pub const STREAM_WAIT_BUDGET: Duration = Duration::from_millis(2500);

/// One seed in this many draws a [`Fault::RetakeUnderStream`], from the fault's
/// own `moirae_sched` stream ("retake-stream"). The arm costs a seed its
/// isolation, the wait for the stream, the freeze and the hold — some two
/// seconds of virtual time — which is why it is not on every schedule; a quarter
/// of the seeds is what the catch rate at the hundred-seed tier needs and what
/// `scripts/premerge.sh`'s quarter of an hour affords beside the adoption
/// storm's own quarter. (D-043).
pub const RETAKE_STREAM_IN: u64 = 4;

/// The longest a [`Fault::CrashRefused`] waits for its victim to begin flushing
/// a memtable before crashing it anyway. A server fills a sixteen-kilobyte
/// memtable about every two seconds at the sweep's write rate. (D-044).
pub const FLUSH_WAIT_BUDGET: Duration = Duration::from_millis(2500);

/// One seed in this many draws a [`Fault::CrashAdopting`] storm, from the
/// fault's own `moirae_sched` stream ("adoption-crash"). D-041 appended the
/// storm to every schedule and the sweep paid for it: the raft test binary's
/// thousand seeds went from 667 s to 2218 s and `scripts/premerge.sh` from about
/// thirteen minutes to forty, over the fifteen-minute target of the tier
/// (D-040). The crash count on a seed that draws the storm is unchanged, so what
/// the share costs is the catch rate — roughly a quarter of what it was — and
/// what it buys back is three quarters of the seeds at their old price.
/// (D-041).
pub const ADOPTION_STORM_IN: u64 = 4;

/// The longest a [`Fault::CrashAdopting`] waits, after the install's completion
/// or a restart, for the adoption's first durable change to the store directory
/// before crashing the server anyway. An adoption reaches that change within a
/// few dozen disk operations, tens of milliseconds at the sweep's latencies; a
/// restart that is refused instead makes no change at all. (D-041).
pub const ADOPTION_WAIT_BUDGET: Duration = Duration::from_millis(100);

/// How long a [`Fault::CrashAdopting`] waits after a restart that finds no store
/// file durable in the directory at all — the old store gone and every copy's
/// entry lost at the crash before — since then the adoption's first durable
/// change would be its copies synced in, past the window that matters; the
/// crash lands in the adoption's opening reads and first copies instead.
/// (D-041).
const EMPTY_STORE_DELAY: Duration = Duration::from_millis(3);

impl Fault {
    /// Whether this arm aims at a path [`Cluster::Node`] has not got: the snapshot
    /// install, the adoption of a staged store, or the refusal of a store that lost
    /// state.
    ///
    /// Each of the four is an arm whose whole aim is one of those paths — a crash
    /// timed at an install's last chunk, at an adoption's first durable change, at
    /// the window a refusal can be laundered in, or at a leader re-taking under a
    /// live stream. On a node that takes no snapshot and cannot be refused, each
    /// would wait out its budget and fire as an ordinary isolation or crash, which
    /// [`Schedule::draw`] draws anyway. [`Schedule::draw_on_the_node`] leaves them
    /// out and says why.
    // PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
    #[must_use]
    pub fn needs_a_path_the_node_has_not_got(&self) -> bool {
        matches!(
            self,
            Self::CrashInstalling { .. }
                | Self::CrashAdopting { .. }
                | Self::CrashRefused { .. }
                | Self::RetakeUnderStream { .. }
        )
    }
}

/// One lease trial; two open every schedule, see the module documentation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trial {
    /// How long the slowest leads before it is cut off: time for its lease.
    pub settle: Duration,
    /// How long it is cut off with client 1.
    pub isolate: Duration,
}

/// How long a trial gives the transfer before it looks for the new leader.
const TRANSFER_WAIT: Duration = Duration::from_millis(300);
/// The quiet after each trial's heal, before the next trial or the faults: time
/// for the cluster to elect and for the clients to write between the windows.
const TRIAL_GAP: Duration = Duration::from_millis(500);
/// The operator's client process in the trace.
const ADMIN: u64 = 99 << 32;
/// The base of the burst clients' process ids in the trace: the schedule's `n`th
/// Figure 8 driver writes as process `BURST | n`, distinct per driver so two
/// bursts' sequence numbers never collide in the history.
const BURST: u64 = 98 << 32;
/// The key the bursts write, outside the clients' [`KEYS`]: nothing ever reads
/// it, so the checker's per-key search sees only puts that always apply and the
/// hundred-odd pending operations a burst leaves cost it nothing.
const BURST_KEY: &[u8] = b"burst";
/// The base of the filling clients' process ids in the trace, one per
/// [`Fault::RetakeUnderStream`]. (D-043).
const SPREAD: u64 = 97 << 32;
/// How many bytes each filling put carries. The state machine the clients build
/// on their own is two keys of a dozen bytes, so a checkpoint of it is one chunk
/// and an install is over before anything can be aimed at it; a hundred
/// kilobytes is a checkpoint of a couple of dozen chunks, which is long enough
/// for a stream to still be running when the next take lands and long enough for
/// the network's drops and duplicates to make a receiver ask to start over.
/// (D-043).
const SPREAD_VALUE_BYTES: usize = 400;

/// The fault schedule of one run, in global virtual time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// All links up, servers electing and clients starting.
    pub warmup: Duration,
    /// The lease trials, one after the other, right after the warmup.
    pub trials: Vec<Trial>,
    /// The faults, each healed before the next, with `gaps[i]` of quiet after it.
    pub faults: Vec<Fault>,
    /// The quiet after each fault.
    pub gaps: Vec<Duration>,
    /// The quiet after the last fault: the liveness window.
    pub settle: Duration,
    /// Each server's clock rate error in parts per million, by server index.
    pub drifts: Vec<i64>,
    /// Each server's clock offset in nanoseconds, by server index.
    pub skews: Vec<i64>,
    /// Which of the cluster's ranges each leader-relative fault aims at, as an
    /// index into [`Cluster::ranges`], one per entry of `faults`.
    ///
    /// Empty on a schedule drawn for [`Cluster::OneGroup`], where the cluster has
    /// one range and there is nothing to aim: [`Schedule::range_of`] answers the
    /// only range there is, and no draw is spent, so a one-group schedule is drawn
    /// from exactly the streams it was drawn from before this field existed and
    /// every pinned seed of this sweep keeps its run.
    ///
    /// On the node it is drawn from a stream of its own, `arm-range`, as §11's env
    /// item 8 asks: which range an arm aims at is the arm's own draw, so changing
    /// it moves no other arm's dice (D-031).
    // PROPOSED(D-082): a leader-relative arm on a node of many ranges draws its
    // range from its own stream.
    pub range_picks: Vec<u64>,
}

impl Schedule {
    /// The empty schedule: no warmup, no trial, no fault, and every clock true.
    ///
    /// It is what a run driven by *another* scenario's faults carries, so that the
    /// checks this scenario's [`Report`] makes can be asked of that run's trace
    /// (see [`Report::over_a_run`]): the faults such a run ran are in its
    /// `isolations` and its `last_heal`, which is all those checks read, and this
    /// schedule claims none of its own rather than a drawn one it did not run.
    // PROPOSED(D-076): the node scenario's report borrows these checks.
    #[must_use]
    pub fn none() -> Self {
        Self {
            warmup: Duration::ZERO,
            trials: Vec::new(),
            faults: Vec::new(),
            gaps: Vec::new(),
            settle: Duration::ZERO,
            drifts: vec![0; SERVERS as usize],
            skews: vec![0; SERVERS as usize],
            range_picks: Vec::new(),
        }
    }

    /// A schedule drawn from `seed`: three to six faults with random kinds,
    /// targets and durations, each followed by a quiet of at least two maximum
    /// election timeouts, so a check after a heal sees the heal's effect alone.
    #[must_use]
    pub fn draw(seed: u64) -> Self {
        let mut rng = moirae_sched::stream(seed, "schedule");
        let ms = |rng: &mut moirae_sched::Pcg32, lo: u64, hi: u64| {
            Duration::from_millis(lo + rng.below(hi - lo + 1))
        };
        let count = 3 + rng.below(4);
        let mut faults = Vec::new();
        let mut gaps = Vec::new();
        // The Figure 8 driver draws from its own stream, so its parameters can be
        // tuned without re-drawing every schedule's other faults, clocks and
        // trials; and it takes two slots of eight, since its window needs the
        // burst, the crash and the steering to line up and a rarer draw would be
        // seen lining up on too few seeds to report a rate.
        let mut f8 = moirae_sched::stream(seed, "figure8");
        for _ in 0..count {
            let fault = match rng.below(8) {
                0 => Fault::Isolate {
                    server: 1 + rng.below(SERVERS),
                    client: 1 + rng.below(CLIENTS),
                    for_: ms(&mut rng, 300, 900),
                },
                1 => Fault::IsolateLeader {
                    for_: ms(&mut rng, 300, 900),
                },
                2 => {
                    let from = 1 + rng.below(SERVERS);
                    let to = 1 + (from - 1 + 1 + rng.below(SERVERS - 1)) % SERVERS;
                    Fault::OneWay {
                        from,
                        to,
                        for_: ms(&mut rng, 300, 900),
                    }
                }
                3 => Fault::Crash {
                    server: 1 + rng.below(SERVERS),
                    down: ms(&mut rng, 50, 400),
                },
                4 => Fault::CrashLeader {
                    down: ms(&mut rng, 50, 400),
                },
                5 => Fault::StaleSender {
                    server: 1 + rng.below(SERVERS),
                    one_way: ms(&mut rng, 900, 1300),
                    crash_after: ms(&mut rng, 200, 400),
                    down: ms(&mut rng, 300, 500),
                },
                _ => Fault::FigureEight {
                    follower: 1 + f8.below(SERVERS),
                    client: 1 + f8.below(CLIENTS),
                    settle: ms(&mut f8, 300, 600),
                    burst: 112 + f8.below(65),
                    crash_after: ms(&mut f8, 450, 750),
                    down: ms(&mut f8, 150, 350),
                    steer: ms(&mut f8, 600, 1000),
                },
            };
            faults.push(fault);
            gaps.push(ms(&mut rng, 450, 700));
        }
        // A crash aimed mid-install, on half the seeds, appended after the drawn
        // faults. It draws from its own stream so the shared "schedule" stream's
        // draws — and with them every other fault's dice — never move when this
        // arm changes (D-031).
        let mut snap = moirae_sched::stream(seed, "snapshot-crash");
        if snap.below(2) == 0 {
            faults.push(Fault::CrashInstalling {
                server: 1 + snap.below(SERVERS),
                isolate: ms(&mut snap, 700, 1100),
                grace: ms(&mut snap, 2, 20),
                down: ms(&mut snap, 50, 300),
            });
            gaps.push(ms(&mut snap, 450, 700));
        }
        // A crash storm aimed at the adoption that follows a completed install,
        // on one seed in [`ADOPTION_STORM_IN`], appended after the install
        // crash: its window is a two-per-cent roll of the disk's dice per crash,
        // so a seed that draws the storm rolls sixteen to thirty-two times. The
        // share is the arm's price. Waiting for an install, then some two dozen
        // aimed crashes with a restart each, is the most expensive fault a
        // schedule carries, and on every seed it took the raft binary's thousand
        // seeds from 667 s to 2218 s and `scripts/premerge.sh` from about
        // thirteen minutes to forty, well past the fifteen that tier exists for;
        // a quarter of the seeds keeps the catch and gives the other three
        // quarters their old cost back. Drawn from this arm's own stream, so
        // neither the shared "schedule" draws nor the install crash's move when
        // the share changes (D-031). (D-041).
        let mut adopt = moirae_sched::stream(seed, "adoption-crash");
        if adopt.below(ADOPTION_STORM_IN) == 0 {
            faults.push(Fault::CrashAdopting {
                server: 1 + adopt.below(SERVERS),
                isolate: ms(&mut adopt, 700, 1100),
                down: ms(&mut adopt, 10, 40),
                crashes: 16 + adopt.below(17),
            });
            gaps.push(ms(&mut adopt, 450, 700));
        }
        // A crash storm aimed at the laundering window of a refusal, on every
        // seed, appended last: the adoption storm before it is where the disk's
        // rot turns into refusals, and a server left refused by it takes this
        // storm's crashes straight away. Its own stream, so no other arm's dice
        // move when this one changes (D-031). (D-044).
        let mut refused = moirae_sched::stream(seed, "refusal-crash");
        faults.push(Fault::CrashRefused {
            server: 1 + refused.below(SERVERS),
            down: ms(&mut refused, 10, 40),
            grace: ms(&mut refused, 60, 160),
            crashes: 3 + refused.below(3),
        });
        gaps.push(ms(&mut refused, 450, 700));
        // The re-take under a running stream, on one seed in
        // [`RETAKE_STREAM_IN`], appended after every crash storm so the wedge it
        // builds is the last thing standing when the liveness window opens: a
        // crash that came after it would restart a server, and a new leader's
        // progress reset is exactly what undoes the wedge. Its own stream, so no
        // other arm's dice move when this one changes (D-031). (D-043).
        let mut retake = moirae_sched::stream(seed, "retake-stream");
        if retake.below(RETAKE_STREAM_IN) == 0 {
            faults.push(Fault::RetakeUnderStream {
                follower: 1 + retake.below(SERVERS),
                fill: 250 + retake.below(101),
                settle: ms(&mut retake, 500, 800),
                isolate: ms(&mut retake, 1500, 2500),
                freeze: ms(&mut retake, 500, 900),
                hold: ms(&mut retake, 200, 400),
            });
            gaps.push(ms(&mut retake, 450, 700));
        }
        // Clocks, as the module documentation says: within the bound, or one
        // server slow and two fast, moderately or severely.
        let within = rng.below(2) == 0;
        let severe = rng.below(2) == 0;
        let slow = rng.below(SERVERS);
        let mut drifts = Vec::new();
        let mut skews = Vec::new();
        for i in 0..SERVERS {
            let (magnitude, sign) = if within {
                let sign = if rng.below(2) == 0 { 1 } else { -1 };
                (rng.below(DRIFT_BOUND_PPM / 3 + 1), sign)
            } else if severe {
                // The slow server's heartbeats, at a fifth of its clock's timeout,
                // must still arrive within the fast followers' minimum timeout, or
                // it cannot lead at all: the rates stay within a factor of five.
                let (lo, hi) = if i == slow {
                    (650_000, 750_000)
                } else {
                    (250_000, 350_000)
                };
                (lo + rng.below(hi - lo + 1), if i == slow { -1 } else { 1 })
            } else {
                let lo = 2_000f64.ln();
                let hi = 100_000f64.ln();
                let u = rng.below(1_000_000) as f64 / 1_000_000.0;
                (
                    (lo + (hi - lo) * u).exp() as u64,
                    if i == slow { -1 } else { 1 },
                )
            };
            drifts.push(sign * i64::try_from(magnitude).expect("small"));
            let skew = i64::try_from(rng.below(50_000_000)).expect("small");
            skews.push(if rng.below(2) == 0 { skew } else { -skew });
        }
        Self {
            warmup: Duration::from_millis(1200),
            trials: vec![
                Trial {
                    settle: ms(&mut rng, 600, 900),
                    isolate: ms(&mut rng, 500, 800),
                },
                Trial {
                    settle: ms(&mut rng, 600, 900),
                    isolate: ms(&mut rng, 500, 800),
                },
            ],
            faults,
            gaps,
            settle: election_max() * LIVENESS_TIMEOUTS + Duration::from_millis(200),
            drifts,
            skews,
            range_picks: Vec::new(),
        }
    }

    /// A schedule for [`Cluster::Node`] drawn from `seed`: [`Schedule::draw`]'s
    /// own draw, with the arms aimed at paths the node has not got taken out, and
    /// with the range each leader-relative arm aims at drawn from a stream of its
    /// own (SHARD.md §11, env 8).
    ///
    /// **What it keeps** is everything Q41's round and the batched wire are about:
    /// the two lease trials, the isolations, the leader isolation, the one-way
    /// blocks, the crashes, the leader crash, [`Fault::StaleSender`]'s rule-5 shape
    /// and the Figure 8 driver. Seven of Phase 2's eight `sim/raft.rs` variants are
    /// caught on these (§10), and on the node each is caught with four ranges'
    /// persists sharing one group commit and four ranges' messages sharing one
    /// frame.
    ///
    /// **What it takes out** is the third of the arms that aim at the install, the
    /// adoption and the refusal — [`Fault::CrashInstalling`],
    /// [`Fault::CrashAdopting`], [`Fault::CrashRefused`] and
    /// [`Fault::RetakeUnderStream`] — because this node has none of those paths:
    /// `ananke_shard::snapshot` is not wired to `ananke_shard::server::ServerHost`,
    /// and Q15's whole-node refusal and re-seed are not in this tree either. D-076
    /// said so in as many words when it added the node beside the server. An arm
    /// kept here would fire, wait out its budget and reduce to an isolation or an
    /// ordinary crash, which the schedule already draws: it would cost the tier its
    /// time and assert nothing. They come back with the slice that wires the path,
    /// and until then the variants they carry keep their Phase 2 assertions on
    /// [`Cluster::OneGroup`], which this sweep leaves running exactly as it is
    /// (PROPOSED D-082).
    ///
    /// Each taken-out arm's absence is asserted on every seed by [`Report::check`]'s
    /// node clauses — no snapshot action asked for, no store refused — so the day a
    /// path arrives the sweep says so rather than passing over it (CLAUDE.md).
    // PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
    #[must_use]
    pub fn draw_on_the_node(seed: u64) -> Self {
        let drawn = Self::draw(seed);
        let mut faults = Vec::new();
        let mut gaps = Vec::new();
        for (fault, gap) in drawn.faults.iter().zip(drawn.gaps.iter()) {
            if fault.needs_a_path_the_node_has_not_got() {
                continue;
            }
            faults.push(fault.clone());
            gaps.push(*gap);
        }
        let mut rng = moirae_sched::stream(seed, "arm-range");
        let range_picks = faults
            .iter()
            .map(|_| rng.below(crate::ranges::RANGES))
            .collect();
        Self {
            faults,
            gaps,
            range_picks,
            ..drawn
        }
    }

    /// The range the `i`th fault aims at, on `cluster`.
    ///
    /// One group has one range and every arm aims at it; the node's arms take
    /// theirs from [`Schedule::range_picks`]. A pick past the cluster's ranges — a
    /// schedule built by hand — falls back to the first, so the answer is always a
    /// range the cluster holds.
    // PROPOSED(D-082): a leader-relative arm resolves its leader per range.
    #[must_use]
    pub fn range_of(&self, cluster: Cluster, i: usize) -> u64 {
        let ranges = cluster.ranges();
        let pick = usize::try_from(self.range_picks.get(i).copied().unwrap_or(0)).expect("small");
        ranges
            .get(pick)
            .or_else(|| ranges.first())
            .copied()
            .unwrap_or(SINGLE_GROUP)
    }

    /// The directed schedule for issue #32's shape (D-050): after the warmup,
    /// no lease trial and one [`Fault::IsolateOnTermRaise`] of `tries` rounds, each
    /// isolation 300 ms and each quiet 500 ms, then the liveness window. Every clock
    /// runs true, so a transfer goes where it is asked.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    #[must_use]
    pub fn term_raise_behind_a_step(tries: u64) -> Self {
        Self {
            warmup: Duration::from_millis(1200),
            trials: Vec::new(),
            faults: vec![Fault::IsolateOnTermRaise {
                tries,
                isolate: Duration::from_millis(300),
                quiet: Duration::from_millis(500),
            }],
            gaps: vec![Duration::from_millis(500)],
            settle: election_max() * LIVENESS_TIMEOUTS + Duration::from_millis(200),
            drifts: vec![0; SERVERS as usize],
            skews: vec![0; SERVERS as usize],
            range_picks: Vec::new(),
        }
    }

    /// The server with the slowest clock, 1-based.
    #[must_use]
    pub fn slowest(&self) -> u64 {
        let (i, _) = self
            .drifts
            .iter()
            .enumerate()
            .min_by_key(|(_, d)| **d)
            .expect("servers");
        i as u64 + 1
    }

    /// The largest rate at which two servers' clocks drift apart, in parts per
    /// million.
    #[must_use]
    pub fn max_relative_drift_ppm(&self) -> u64 {
        let mut max = 0;
        for a in &self.drifts {
            for b in &self.drifts {
                max = max.max((a - b).unsigned_abs());
            }
        }
        max
    }

    /// Whether some pair of servers drifts apart faster than the lease assumes.
    #[must_use]
    pub fn drift_exceeded(&self) -> bool {
        self.max_relative_drift_ppm() > DRIFT_BOUND_PPM
    }

    /// The whole run's virtual duration.
    #[must_use]
    pub fn total(&self) -> Duration {
        let faults: Duration = self
            .faults
            .iter()
            .map(|fault| match fault {
                Fault::Isolate { for_, .. }
                | Fault::IsolateLeader { for_ }
                | Fault::OneWay { for_, .. } => *for_,
                Fault::Crash { down, .. } | Fault::CrashLeader { down } => *down,
                Fault::StaleSender { one_way, .. } => *one_way,
                Fault::FigureEight {
                    settle,
                    crash_after,
                    down,
                    steer,
                    ..
                } => *settle + *crash_after + *down + *steer,
                Fault::CrashInstalling {
                    isolate,
                    grace,
                    down,
                    ..
                } => *isolate + INSTALL_WAIT_BUDGET + *grace + *down,
                Fault::CrashAdopting {
                    isolate,
                    down,
                    crashes,
                    ..
                } => {
                    *isolate
                        + INSTALL_WAIT_BUDGET
                        + (ADOPTION_WAIT_BUDGET + *down) * u32::try_from(*crashes).expect("small")
                }
                // D-044: two crashes a round, either side of the
                // catch-up, or one after the grace when the victim is refused.
                Fault::CrashRefused {
                    down,
                    grace,
                    crashes,
                    ..
                } => (FLUSH_WAIT_BUDGET + *grace + *down) * u32::try_from(*crashes).expect("small"),
                // D-043: the fill, the isolation, the wait for the
                // stream, the freeze behind it and the hold after it.
                Fault::RetakeUnderStream {
                    settle,
                    isolate,
                    freeze,
                    hold,
                    ..
                } => *settle + *isolate + STREAM_WAIT_BUDGET + *freeze + *hold,
                // PROPOSED(D-050): a round's wait, isolation and quiet.
                Fault::IsolateOnTermRaise {
                    tries,
                    isolate,
                    quiet,
                } => {
                    (TERM_RAISE_WAIT_BUDGET + *isolate + *quiet)
                        * u32::try_from(*tries).expect("small")
                }
            })
            .sum();
        let trials: Duration = self
            .trials
            .iter()
            .map(|t| TRANSFER_WAIT + t.settle + t.isolate + TRIAL_GAP)
            .sum();
        self.warmup + trials + faults + self.gaps.iter().sum::<Duration>() + self.settle
    }
}

/// What the clients counted.
#[derive(Clone, Debug, Default)]
pub struct ClientStats {
    /// Operations that returned.
    pub completed: u64,
    /// Operations abandoned as pending.
    pub abandoned: u64,
    /// Tries answered with NotLeader.
    pub redirected: u64,
}

type SharedStats = Arc<Mutex<ClientStats>>;

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// Which server ran: the set of known bugs it carried, empty for the correct
    /// one (D-045).
    // D-045: a variant is a set.
    pub variants: Variants,
    /// How the run was scheduled (D-016).
    pub policy: Policy,
    /// The faults it ran.
    pub schedule: Schedule,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// What the moirae export needs besides [`Report::records`]:
    /// [`Report::jsonl`] writes the trace from the two when it is asked for.
    // PROPOSED(D-052): a scenario's moirae JSONL is written when it is asked for.
    pub run: RunHeader,
    /// When the last fault healed or the last crashed server restarted.
    pub last_heal: Instant,
    /// Every isolation of one server: (server, from, until).
    pub isolations: Vec<(u64, Instant, Instant)>,
    /// In how many of the trials the slowest clock led when the trial cut the
    /// leader off.
    pub trials_led_by_slowest: usize,
    /// How many [`Fault::RetakeUnderStream`] arms got as far as a stream: the
    /// arm's aim is a leader re-taking under a running stream, and a run where
    /// the follower it isolated was caught up by entries instead never held a
    /// stream to re-take under. What the sweep asserts fired. (D-043).
    pub aimed_streams: usize,
    /// Servers refused at a restart, with the reason.
    pub refused: Vec<(u64, String)>,
    /// Why the run stopped early, if it did: a safety violation the folds saw at a
    /// slice boundary, or a runaway past [`TRACE_CAP`].
    pub stopped: Option<String>,
    /// The clients' history.
    pub history: History,
    /// The clients' counts.
    pub clients: ClientStats,
    /// The ranges this run's configuration fixed at bootstrap (SHARD.md §2): the
    /// one group of this scenario, four of the node's. The trace's payload oracle
    /// asks that every record about a replica name one of them.
    // PROPOSED(D-076): the run says which ranges it hosts.
    pub ranges: Vec<u64>,
    /// The range a key is served by in this run: the scenario's fixed map (SHARD.md,
    /// Stage B), which the write bound and the liveness check read. One group's
    /// scenario maps every key to it; the node's maps its keys over four ranges, and
    /// that is what lets the liveness half of the majority carve-out be told from
    /// the cluster-wide reading at all (D-071, item 6).
    // PROPOSED(D-076): the scenario's key-to-range map is the run's, not a constant.
    pub key_range: fn(&Bytes) -> u64,
    /// Which system the run was driven against.
    // PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
    pub cluster: Cluster,
    /// Every leader-relative arm that fired, as (the range it drew, the leader of
    /// that range it resolved, when it resolved it).
    ///
    /// What [`Report::arms_hit_their_ranges`] reads. All four of the arms that
    /// resolve a leader per range are here — [`Fault::IsolateLeader`] and
    /// [`Fault::CrashLeader`], which aim at the leader itself, and
    /// [`Fault::StaleSender`] and [`Fault::FigureEight`], which choose a victim
    /// *relative* to it and would pick the wrong follower just as silently. Empty for
    /// a run another scenario drove.
    // PROPOSED(D-082): a leader-relative arm resolves its leader per range.
    pub aimed: Vec<(u64, u64, Instant)>,
}

/// What another scenario's run hands [`Report::over_a_run`].
// PROPOSED(D-076): the node scenario is checked by the checks D-071 keyed.
pub struct Run {
    /// The seed.
    pub seed: u64,
    /// The cores' known-buggy variants.
    pub variants: Variants,
    /// How the run was scheduled (D-016).
    pub policy: Policy,
    /// What the moirae export needs besides the records.
    pub header: RunHeader,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// When the run's last fault healed.
    pub last_heal: Instant,
    /// Every isolation of one node: (node, from, until).
    pub isolations: Vec<(u64, Instant, Instant)>,
    /// The clients' history.
    pub history: History,
    /// The clients' counts.
    pub clients: ClientStats,
    /// The ranges the run's configuration fixed.
    pub ranges: Vec<u64>,
    /// The run's map from a key to the range that serves it.
    pub key_range: fn(&Bytes) -> u64,
    /// Why the run stopped early, if it did.
    pub stopped: Option<String>,
}

/// Which replica a per-replica record is about, for the folds that follow one
/// server's records in order.
///
/// Every variant of [`TraceEvent`] that carries a `server` is here. It is a list,
/// and a list left behind is how [`Restating`]'s positional rule would go wrong
/// quietly — the review of this slice found seven kinds missing from it — so
/// [`Restating::saw`] does not trust the list alone: it clears the record's
/// emitting node as well, which covers a kind this function has never heard of
/// (`RaftMatchStarted`, which is about a leader and its follower and carries no
/// `server` at all, is one such). The two together are why adding the seven
/// changed no count.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
fn replica_of(event: &TraceEvent) -> Option<u64> {
    match event {
        TraceEvent::RaftAppend { server, .. }
        | TraceEvent::RaftTruncate { server, .. }
        | TraceEvent::RaftCompacted { server, .. }
        | TraceEvent::RaftSnapshot { server, .. }
        | TraceEvent::RaftCommit { server, .. }
        | TraceEvent::RaftApply { server, .. }
        | TraceEvent::RaftConfig { server, .. }
        | TraceEvent::RaftRecovered { server, .. }
        | TraceEvent::RaftReseeded { server, .. }
        | TraceEvent::RaftTerm { server, .. }
        | TraceEvent::RaftLeader { server, .. }
        | TraceEvent::RaftVote { server, .. }
        | TraceEvent::RaftRead { server, .. }
        | TraceEvent::RaftLeaseRevoked { server, .. }
        | TraceEvent::RaftTransfer { server, .. }
        | TraceEvent::RaftQuorumLost { server, .. }
        | TraceEvent::RaftProposed { server, .. }
        | TraceEvent::RaftRefused { server, .. }
        | TraceEvent::RaftServerFailed { server, .. }
        | TraceEvent::RaftInboxDropped { server, .. }
        | TraceEvent::RaftSnapshotResumed { server, .. }
        | TraceEvent::RaftAdopted { server, .. }
        | TraceEvent::RaftProgressReset { server, .. }
        | TraceEvent::RaftSnapshotDeleted { server, .. }
        | TraceEvent::RaftSnapshotReused { server, .. }
        | TraceEvent::RaftSnapshotStreams { server, .. } => Some(*server),
        _ => None,
    }
}

/// Tells a re-statement's `RaftSnapshot` from one the run just made, and with it
/// which snapshots move a replica's compacted prefix.
///
/// The distinction matters twice over, and both times the answer is not `taken`.
/// A **take** writes a checkpoint and leaves the log alone: the core holds the
/// prefix until `maybe_compact` drops it, which on a leader waits for every
/// follower's match (D-037), and the compaction is what `RaftCompacted` reports.
/// An **install** replaces the log under the snapshot at once. A **re-statement**
/// stands in for a prefix that is already gone from the store, whether the record
/// under it was a take's or an install's — so a re-stated take moves the prefix
/// where a live take does not.
///
/// A re-statement is read off its position: the `apply` loop traces the durable log
/// as a `RaftTruncate` to one past its end and then, if there is a prefix, the
/// snapshot standing in for it (`node.rs`), with nothing of that server's between
/// the two. An install's snapshot never follows that server's own truncation with
/// nothing in between. The split this reads was checked against the counts the
/// review of this slice took by instrumenting the two emission sites themselves —
/// 9 342 installs and 10 788 re-statements over a thousand raft seeds — and agrees
/// with both.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
#[derive(Default)]
struct Restating {
    after_truncate: BTreeSet<u64>,
    restated: bool,
}

impl Restating {
    /// Folds one record in. Call it once per record, before asking anything.
    ///
    /// "Nothing of that server's in between" is read two ways at once, because
    /// either alone is a list that can fall behind the trace: the record's event
    /// names a server ([`replica_of`]), and the record's own node emitted it. The
    /// restatement's two traces are back to back with no await between them
    /// (`node.rs`), so on the correct path nothing of either kind can land
    /// between, and the second reading costs nothing; what it buys is that a new
    /// event kind, or one missing from `replica_of`, cannot silently turn an
    /// install into a re-statement.
    fn saw(&mut self, record: &TraceRecord) {
        self.restated = false;
        match &record.event {
            TraceEvent::RaftTruncate { server, .. } => {
                self.after_truncate.insert(*server);
            }
            TraceEvent::RaftSnapshot { server, .. } => {
                self.restated = self.after_truncate.remove(server);
            }
            other => {
                if let Some(server) = replica_of(other) {
                    self.after_truncate.remove(&server);
                }
                if let Some(node) = record.node {
                    self.after_truncate.remove(&u64::from(node.get()));
                }
            }
        }
    }

    /// Whether the record just folded in is a re-statement's snapshot.
    fn restated(&self) -> bool {
        self.restated
    }

    /// Whether the record just folded in leaves the replica's log starting past the
    /// snapshot it names: an install or a re-statement, never a live take.
    fn moves_the_prefix(&self, event: &TraceEvent) -> bool {
        match event {
            TraceEvent::RaftSnapshot { taken, .. } => self.restated || !taken,
            _ => false,
        }
    }
}

/// The compactions a replica made while it was not leading, and of those the
/// ones whose prefix swallowed the configuration entry in force, so that
/// D-029's revert floor — the configuration held at the new prefix's end —
/// is what the replica would revert to from there.
///
/// The second is the direct measure of what D-065 said would happen: the
/// floor, reached on 3 of 10 000 seeds before this and deferred to issue #56,
/// becomes a routine path on a follower. It is *observed*, not inferred: a
/// compaction that carried the prefix from `prev` to `through` swallowed a
/// configuration entry only when this server holds one at an index strictly
/// inside that step, `prev < index <= through`. The indices are the
/// `RaftConfig` records, which the core traces at the index of every
/// configuration entry it puts in force (`core.rs`, `adopt`); index 0 is the
/// initial configuration, which is no entry and can be swallowed by nothing.
///
/// The first build of this measure asked instead whether the server's *last*
/// `RaftConfig` index was at or below `through`. That holds for every
/// compaction by construction — `Raft::new` sets `membership_index` to
/// `snap_index` when the log holds no configuration entry, 0 at a first open —
/// so the count was a copy of the total under another name, and would have
/// read 100 % on a tree with D-029's floor deleted. It is the shape D-039
/// warns of, a measurement that is structural rather than observed; the
/// review of this slice caught it, and the figures it produced are struck
/// from D-078.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
#[must_use]
pub fn follower_compactions(records: &[TraceRecord]) -> (usize, usize) {
    let mut leading: BTreeMap<u64, bool> = BTreeMap::new();
    let mut configs: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    let mut prefix: BTreeMap<u64, u64> = BTreeMap::new();
    let mut restating = Restating::default();
    let (mut count, mut swallowed) = (0usize, 0usize);
    for record in records {
        restating.saw(record);
        match &record.event {
            TraceEvent::RaftRecovered { server, .. } => {
                leading.insert(*server, false);
            }
            TraceEvent::RaftTerm { server, role, .. } => {
                leading.insert(*server, *role == "leader");
            }
            TraceEvent::RaftLeader { server, .. } => {
                leading.insert(*server, true);
            }
            TraceEvent::RaftConfig { server, index, .. } if *index > 0 => {
                configs.entry(*server).or_default().insert(*index);
            }
            // A take leaves the prefix where it was until `maybe_compact` drops
            // it; an install and a re-statement stand in for one already gone
            // ([`LogShape`]'s own notes).
            TraceEvent::RaftSnapshot {
                server, last_index, ..
            } if restating.moves_the_prefix(&record.event) => {
                let at = prefix.entry(*server).or_default();
                *at = (*at).max(*last_index);
            }
            TraceEvent::RaftCompacted {
                server, through, ..
            } => {
                let at = prefix.entry(*server).or_default();
                let prev = std::mem::replace(at, (*at).max(*through));
                if !leading.get(server).copied().unwrap_or_default() {
                    count += 1;
                    swallowed += usize::from(configs.get(server).is_some_and(|set| {
                        set.iter().any(|index| *index > prev && index <= through)
                    }));
                }
            }
            _ => {}
        }
    }
    (count, swallowed)
}

impl Report {
    /// A report over a run another scenario drove: the node scenario's
    /// ([`crate::ranges`]), whose faults are its own and whose trace this scenario's
    /// checks are asked of.
    ///
    /// Everything the checks read is the run's: the records, the isolations it made,
    /// when it last healed, its clients' history, the ranges its configuration fixed
    /// and the map from a key to its range. What is *not* the run's is
    /// [`Report::schedule`], which is [`Schedule::none`] — the faults were not drawn
    /// by [`Schedule::draw`] — and the fields of this scenario's own arms
    /// (`trials_led_by_slowest`, `aimed_streams`), which are zero. No check reads
    /// any of them: the schedule is read for the clock drift alone, which a run with
    /// no skew and no drift has none of.
    // PROPOSED(D-076): the node scenario is checked by the checks D-071 keyed.
    #[must_use]
    pub fn over_a_run(run: Run) -> Self {
        let Run {
            seed,
            variants,
            policy,
            header,
            records,
            last_heal,
            isolations,
            history,
            clients,
            ranges,
            key_range,
            stopped,
        } = run;
        let refused: Vec<(u64, String)> = records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftRefused { server, reason } => Some((*server, reason.clone())),
                _ => None,
            })
            .collect();
        Self {
            seed,
            variants,
            policy,
            schedule: Schedule::none(),
            records,
            run: header,
            last_heal,
            isolations,
            trials_led_by_slowest: 0,
            aimed_streams: 0,
            refused,
            stopped,
            history,
            clients,
            ranges,
            key_range,
            // A run another scenario drove is `sim/ranges.rs`'s, and this says
            // `OneGroup` so that the follower-log bound is still asked of it.
            //
            // That is deliberate and it is *not* the same judgement as the one this
            // scenario's node cluster gets. `ranges::Report::check` calls
            // `self.checked.check()` and adds its own clauses; it does not opt out of
            // this one, so naming the cluster here is the whole of the decision.
            // Asking the bound there is safe and worth keeping: that scenario's node
            // has no follower compaction either, but it also has no client load worth
            // the name against this one: the review of this slice measured its largest
            // follower log at **66 entries over a hundred seeds and 74 over a
            // thousand** against the bound's 768 — a margin over ten times the figure,
            // barely growing with the tier, where this scenario's node reaches 610. A bound with that much room is a
            // tripwire rather than a claim about a mechanism, and a tripwire on a
            // scenario that should never approach it is worth having. The day it
            // tightens, the judgement moves to `ranges::Report::check` where the
            // scenario can make it for itself.
            // PROPOSED(D-082): the follower-log bound is the one-group server's, and
            // the node scenario's run is named `OneGroup` here so it keeps it.
            cluster: Cluster::OneGroup,
            aimed: Vec::new(),
        }
    }

    /// The trace as moirae JSONL, written from [`Report::records`] under the run's
    /// header now, when it is asked for, rather than at the end of every run; the
    /// bytes are the ones the simulator's own export writes (D-052).
    ///
    /// # Panics
    ///
    /// If the trace does not export to moirae v2.
    // PROPOSED(D-052): a scenario's moirae JSONL is written when it is asked for.
    #[must_use]
    pub fn jsonl(&self) -> String {
        self.run
            .to_moirae(&self.records, &Export::new(&message::studio))
            .expect("the raft trace exports to moirae v2")
    }

    /// Whether some pair of servers' clocks drifted apart faster than the lease
    /// assumes on this seed.
    #[must_use]
    pub fn drift_exceeded(&self) -> bool {
        self.schedule.drift_exceeded()
    }

    /// Reads served by a lease.
    #[must_use]
    pub fn lease_reads(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftRead { lease: true, .. }))
    }

    /// Reads served after a heartbeat round.
    #[must_use]
    pub fn read_index_reads(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftRead { lease: false, .. }))
    }

    /// Times the guard stopped trusting a follower.
    #[must_use]
    pub fn lease_revokes(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftLeaseRevoked { .. }))
    }

    /// Times a leader stepped down for want of a majority.
    #[must_use]
    pub fn quorum_losses(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftQuorumLost { .. }))
    }

    /// Puts the Figure 8 drivers' bursts invoked on this run.
    #[must_use]
    pub fn burst_puts(&self) -> usize {
        self.count(
            |e| matches!(e, TraceEvent::ClientInvoke { client, .. } if client >> 32 == BURST >> 32),
        )
    }

    /// The span the run's records cover: the last record's time less the first's.
    ///
    /// It is what a rate is divided by, in place of the schedule's planned total —
    /// they agree on a run that finished, and a run stopped as a runaway is exactly
    /// the case where they would not.
    // PROPOSED(D-082): the measurements SHARD.md §12 asks of the sweeps.
    #[must_use]
    pub fn observed(&self) -> Duration {
        match (self.records.first(), self.records.last()) {
            (Some(first), Some(last)) => last.at.duration_since(first.at),
            _ => Duration::ZERO,
        }
    }

    /// Trace records per virtual second divided by the run's range count: the
    /// figure Stage B measures against [`TRACE_CAP`], which sizes the scenarios of
    /// Stages C to E.
    ///
    /// The numerator is the **whole** trace — every client operation, every
    /// `MessageSent` and `MessageDelivered`, the engine's records — and only a part
    /// of it is about a range at all, so this is an upper bound on any range's own
    /// rate and not that rate ([`Report::busiest_range_records_per_second`] is the
    /// observed one). A cap sized from this figure is sized conservatively, which
    /// is the direction to be wrong in.
    // PROPOSED(D-082): the measurements SHARD.md §12 asks of the sweeps.
    #[must_use]
    pub fn records_per_second_per_range(&self) -> f64 {
        let seconds = self.observed().as_secs_f64().max(f64::EPSILON);
        self.records.len() as f64 / self.ranges.len().max(1) as f64 / seconds
    }

    /// The records that name a range, counted for the busiest range, per observed
    /// virtual second: the rate a range's own records actually reach.
    // PROPOSED(D-082): the measurements SHARD.md §12 asks of the sweeps.
    #[must_use]
    pub fn busiest_range_records_per_second(&self) -> f64 {
        let seconds = self.observed().as_secs_f64().max(f64::EPSILON);
        let mut by_range: BTreeMap<u64, usize> = BTreeMap::new();
        for record in &self.records {
            if let Some(range) = range_of(&record.event) {
                *by_range.entry(range).or_default() += 1;
            }
        }
        by_range.into_values().max().unwrap_or(0) as f64 / seconds
    }

    /// The messages the node's inbox dropped under its byte bound, by the kind it
    /// dropped and the range it was of: the coverage SHARD.md §12 asks each
    /// scenario to print.
    ///
    /// A drop costs a follower its timer reset and a leader a promise (§4), which
    /// is why the policy drops the noisiest pair's oldest heartbeat and never a
    /// message carrying entries or snapshot data (D-072). The count is printed, not
    /// asserted at zero: a bounded inbox that never dropped anything would say the
    /// bound was never reached, and what the bound is for is the tick at 1 000
    /// ranges where it is.
    // PROPOSED(D-082): the inbox's drops under its byte bound, printed.
    #[must_use]
    pub fn inbox_drops(&self) -> BTreeMap<(&'static str, u64), usize> {
        let mut by: BTreeMap<(&'static str, u64), usize> = BTreeMap::new();
        for record in &self.records {
            if let TraceEvent::RaftInboxDropped { range, kind, .. } = &record.event {
                *by.entry((kind, *range)).or_default() += 1;
            }
        }
        by
    }

    /// Every apply's lag, in virtual time: from the `RaftCommit` that made a range's
    /// index committed on a node to that node's `RaftApply` of it, by range.
    ///
    /// SHARD.md §12's measurement, and §4's threshold is on the **median**: Q14's
    /// grouped applies are built if it exceeds one heartbeat interval, 20 ms. The
    /// sweep's disk latencies drive it — an apply is a synced batch — and on the
    /// node one `apply` task carries every range, so a range's lag is its own work
    /// plus whatever the task was doing for the others
    /// ([`Report::cross_range_apply_holds`]).
    ///
    /// A commit index advancing to `index` makes every index above the last one
    /// committed, so each of them is stamped with that record's time; a later
    /// commit over the same index — a restart resets the commit index, which is not
    /// persisted — restamps it, and the apply is measured from the stamp in force
    /// when it happened. An index applied with no commit of it in the trace before
    /// it contributes nothing rather than a guess.
    // PROPOSED(D-082): the apply lag per range, under the sweep's client load.
    #[must_use]
    pub fn apply_lags(&self) -> BTreeMap<u64, Vec<Duration>> {
        let mut committed: BTreeMap<(u64, u64), (u64, BTreeMap<u64, Instant>)> = BTreeMap::new();
        let mut lags: BTreeMap<u64, Vec<Duration>> = BTreeMap::new();
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftCommit {
                    server,
                    range,
                    index,
                    ..
                } => {
                    let (highest, at) = committed.entry((*server, *range)).or_default();
                    for i in (*highest + 1)..=*index {
                        at.insert(i, record.at);
                    }
                    *highest = (*highest).max(*index);
                }
                TraceEvent::RaftApply {
                    server,
                    range,
                    index,
                    ..
                } => {
                    if let Some((_, at)) = committed.get(&(*server, *range))
                        && let Some(committed_at) = at.get(index)
                        && record.at >= *committed_at
                    {
                        lags.entry(*range)
                            .or_default()
                            .push(record.at.duration_since(*committed_at));
                    }
                }
                _ => {}
            }
        }
        lags
    }

    /// The median apply lag over every range of the run, and the median per range.
    ///
    /// `None` when nothing was both committed and applied, which is a run that put
    /// no work through any range.
    // PROPOSED(D-082): the apply lag per range, under the sweep's client load.
    #[must_use]
    pub fn median_apply_lag(&self) -> (Option<Duration>, BTreeMap<u64, Duration>) {
        let lags = self.apply_lags();
        let mut all: Vec<Duration> = Vec::new();
        let mut per_range = BTreeMap::new();
        for (range, mut of_range) in lags {
            all.extend(of_range.iter().copied());
            of_range.sort_unstable();
            if let Some(median) = median(&of_range) {
                per_range.insert(range, median);
            }
        }
        all.sort_unstable();
        (median(&all), per_range)
    }

    /// How long a range's ready apply waited through **another** range's apply job
    /// on the same node, in virtual time, over windows in which that node was up
    /// throughout.
    ///
    /// D-036's figure as far as this node can produce it. One `apply` task per node
    /// takes every range's jobs one at a time (Q14), so "one range's take holds the
    /// node's other ranges' applies" is the extreme case of a hold that exists
    /// whatever the job is. **This node takes no snapshot** — the `snapshot` task is
    /// not wired to its host — so the take's own hold cannot be measured here at all,
    /// and the slice that wires it owes that figure. What is measured is the hold by
    /// an ordinary apply.
    ///
    /// The fold, over one node's applies in time order. For three consecutive applies
    /// at `t0 < t1 < t2` where the one at `t2` is of a different range than the one at
    /// `t1`, and `t2`'s entry became committed at `r <= t1`: the hold is
    /// `t1 - max(r, t0)` — the part of the job that ran from `t0` to `t1`, another
    /// range's, that the range applied at `t2` spent waiting with its own entry
    /// already committed.
    ///
    /// **A window in which the node crashed is not a hold, and is dropped.** Clamping
    /// to `t0` was once thought to rule a crash out, and it does not: a node that
    /// crashes just after `t0` and restarts before `t1` leaves two applies far apart
    /// with no work between them, and the gap is the node being dead, not one range
    /// holding another. The review of this slice found the maximum this fold reported
    /// was exactly that — seed 71, server 1, window 8.067849144 s to 8.640712611 s,
    /// with `NodeCrashed` at 8.068 s and `NodeRestarted` at 8.413 s inside it, 345 ms
    /// of the 573 spent down — and the next three maxima were the same shape. So a
    /// hold whose window holds a `NodeCrashed` or a `NodeRestarted` of that server is
    /// not counted, and the count of what was dropped is returned beside the holds,
    /// because a fold that quietly drops its input is a fold that says nothing.
    ///
    /// What is left is still an **upper bound on one job's hold and a lower bound on
    /// the wait's total**, because an apply's completion is all the trace carries.
    // PROPOSED(D-082): how long one range's applies hold the node's others.
    #[must_use]
    pub fn cross_range_apply_holds(&self) -> Vec<Duration> {
        self.cross_range_apply_holds_counted().0
    }

    /// The holds, and how many windows were dropped for holding a crash or a restart
    /// of their node.
    // PROPOSED(D-082): a window in which the node crashed is not a hold.
    #[must_use]
    pub fn cross_range_apply_holds_counted(&self) -> (Vec<Duration>, usize) {
        // Every crash and restart, by the node that suffered it, in time order.
        let mut downs: BTreeMap<u64, Vec<Instant>> = BTreeMap::new();
        for record in &self.records {
            let node = match &record.event {
                TraceEvent::NodeCrashed { node } | TraceEvent::NodeRestarted { node } => {
                    u64::from(node.get())
                }
                _ => continue,
            };
            downs.entry(node).or_default().push(record.at);
        }
        let mut committed: BTreeMap<(u64, u64), (u64, BTreeMap<u64, Instant>)> = BTreeMap::new();
        // The two most recent applies on each node: (t0, t1) with t1's range.
        let mut last: BTreeMap<u64, (Option<Instant>, Instant, u64)> = BTreeMap::new();
        let mut holds = Vec::new();
        let mut dropped = 0usize;
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftCommit {
                    server,
                    range,
                    index,
                    ..
                } => {
                    let (highest, at) = committed.entry((*server, *range)).or_default();
                    for i in (*highest + 1)..=*index {
                        at.insert(i, record.at);
                    }
                    *highest = (*highest).max(*index);
                }
                TraceEvent::RaftApply {
                    server,
                    range,
                    index,
                    ..
                } => {
                    if let Some(&(t0, t1, of)) = last.get(server)
                        && of != *range
                        && let Some((_, at)) = committed.get(&(*server, *range))
                        && let Some(&ready) = at.get(index)
                        && ready <= t1
                    {
                        let from = match t0 {
                            Some(t0) if t0 > ready => t0,
                            _ => ready,
                        };
                        // A node's own server id is its node id in these scenarios
                        // (`node_of_server`), so the crash records are looked up by it.
                        let crashed = downs
                            .get(server)
                            .is_some_and(|at| at.iter().any(|&a| a >= from && a <= t1));
                        if crashed {
                            dropped += 1;
                        } else {
                            holds.push(t1.duration_since(from));
                        }
                    }
                    let t0 = last.get(server).map(|&(_, t1, _)| t1);
                    last.insert(*server, (t0, record.at, *range));
                }
                _ => {}
            }
        }
        (holds, dropped)
    }

    /// Every peer frame this run's nodes sent, decoded: how many messages it
    /// carried and how many distinct ranges those messages were of.
    ///
    /// What says a frame between two nodes carries several ranges — the parameter
    /// four is fixed for (SHARD.md §12) — read off the frames themselves. A frame
    /// of the one-group codec counts as one message of one range, which is what it
    /// is.
    // PROPOSED(D-082): the batching claim is read off this sweep's own frames.
    #[must_use]
    pub fn frames_carried(&self) -> (BTreeMap<usize, usize>, BTreeMap<usize, usize>) {
        let mut messages: BTreeMap<usize, usize> = BTreeMap::new();
        let mut ranges: BTreeMap<usize, usize> = BTreeMap::new();
        for record in &self.records {
            let TraceEvent::MessageSent { payload, .. } = &record.event else {
                continue;
            };
            if ananke_shard::is_ranged(payload) {
                continue;
            }
            let carried = messages_of(payload);
            if carried.is_empty() {
                continue;
            }
            *messages.entry(carried.len()).or_default() += 1;
            let of: BTreeSet<u64> = carried.iter().map(|(range, _)| *range).collect();
            *ranges.entry(of.len()).or_default() += 1;
        }
        (messages, ranges)
    }

    /// How many of this run's peer frames carried messages of more than one range.
    // PROPOSED(D-082): the batching claim is read off this sweep's own frames.
    #[must_use]
    pub fn frames_of_several_ranges(&self) -> usize {
        let (_, ranges) = self.frames_carried();
        ranges
            .iter()
            .filter(|(carried, _)| **carried > 1)
            .map(|(_, frames)| *frames)
            .sum()
    }

    /// Snapshot actions traced on this run.
    // PROPOSED(D-082): what the node cluster does not reach yet, asserted absent.
    #[must_use]
    pub fn snapshot_actions(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftSnapshot { .. }))
    }

    /// How many of this run's leader-relative arms hit the leader of the range they
    /// drew, and how many fired: the teeth of §11's env item 8.
    ///
    /// An arm on a node of four ranges draws a range and cuts off *that range's*
    /// leader. An arm that resolved "the leader" without the range would cut off
    /// whichever range elected last, which is a perfectly good fault and which no
    /// check of the run would report — so this is what reports it. The fold is a
    /// forward walk of the finished trace keeping the latest `RaftLeader` per range,
    /// which is not the backward windowed search the arm itself used.
    ///
    /// It is not asserted at one: a leadership change between the arm's read and its
    /// partition is ordinary, and on one group every arm aims at the only range there
    /// is. What the sweep asserts is a floor measured on the correct system.
    // PROPOSED(D-082): a leader-relative arm resolves its leader per range.
    #[must_use]
    pub fn arms_hit_their_ranges(&self) -> (usize, usize) {
        let mut hit = 0;
        for (range, server, at) in &self.aimed {
            let mut leader = None;
            for record in &self.records {
                if record.at > *at {
                    break;
                }
                if let TraceEvent::RaftLeader {
                    server: who,
                    range: of,
                    ..
                } = &record.event
                    && of == range
                {
                    leader = Some(*who);
                }
            }
            hit += usize::from(leader == Some(*server));
        }
        (hit, self.aimed.len())
    }

    /// The highest index any replica of this run appended, committed or applied.
    ///
    /// What says a core could not have asked for a snapshot action at all. Counting
    /// `RaftSnapshot` records alone would not: the node's `Host::snapshot` bumps a
    /// counter and traces nothing (`ananke_shard::server::Gaps`), so a take the node
    /// dropped on the floor would leave no record for a check to find and the
    /// absence would be asserted against a silence. The condition behind every
    /// action is the log's length against `snapshot_threshold`, and that the trace
    /// does carry.
    // PROPOSED(D-082): what the node cluster does not reach yet, asserted absent.
    #[must_use]
    pub fn highest_index(&self) -> u64 {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftAppend { index, .. }
                | TraceEvent::RaftCommit { index, .. }
                | TraceEvent::RaftApply { index, .. } => Some(*index),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }
}

/// The middle value of a sorted slice, the lower of the two when it is even.
fn median(sorted: &[Duration]) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }
    Some(sorted[(sorted.len() - 1) / 2])
}

/// The range a record about a replica names, and `None` for a record about
/// something else: a node's store (`RaftRefused`, `RaftAdopted`,
/// `RaftServerFailed`), the network, a client or the simulator itself.
///
/// The twenty-six `Raft*` kinds about a replica: the twenty-three D-069 gave
/// a `range` and the three it adds carrying one. A kind a later stage adds
/// joins this list; the moirae export's exhaustive `convert` is what says one
/// exists.
#[must_use]
pub fn range_of(event: &TraceEvent) -> Option<u64> {
    match event {
        TraceEvent::RaftTerm { range, .. }
        | TraceEvent::RaftVote { range, .. }
        | TraceEvent::RaftLeader { range, .. }
        | TraceEvent::RaftAppend { range, .. }
        | TraceEvent::RaftTruncate { range, .. }
        | TraceEvent::RaftCommit { range, .. }
        | TraceEvent::RaftApply { range, .. }
        | TraceEvent::RaftConfig { range, .. }
        | TraceEvent::RaftSnapshot { range, .. }
        | TraceEvent::RaftRead { range, .. }
        | TraceEvent::RaftLeaseRevoked { range, .. }
        | TraceEvent::RaftTransfer { range, .. }
        | TraceEvent::RaftQuorumLost { range, .. }
        | TraceEvent::RaftRecovered { range, .. }
        | TraceEvent::RaftProposed { range, .. }
        | TraceEvent::RaftInboxDropped { range, .. }
        | TraceEvent::RaftCompacted { range, .. }
        | TraceEvent::RaftReseeded { range, .. }
        | TraceEvent::RaftSnapshotResumed { range, .. }
        | TraceEvent::RaftProgressReset { range, .. }
        | TraceEvent::RaftSnapshotDeleted { range, .. }
        | TraceEvent::RaftSnapshotReused { range, .. }
        | TraceEvent::RaftSnapshotStreams { range, .. }
        | TraceEvent::RaftMatchStarted { range, .. }
        | TraceEvent::RaftLearnerRound { range, .. }
        | TraceEvent::RaftChangeAccepted { range, .. } => Some(*range),
        _ => None,
    }
}

/// The range a key is served by. The scenario fixes its ranges and a client takes
/// its key's range from that fixed map (SHARD.md, Stage B); while a node runs one
/// group every key is in it.
// PROPOSED(D-071): the write bound is asked per key, of a range with a majority.
#[must_use]
pub fn range_of_key(_key: &Bytes) -> u64 {
    SINGLE_GROUP
}

/// The structure of what SHARD.md §8's trace carries, over a run's records: the
/// oracle of the payload the checks of §8 will read, which nothing in the tree read
/// before it. Every check of §8 is keyed by range and reads `key`, `effect` and a
/// read's `applied`; until those checks arrive a field stamped wrong is a field
/// nothing sees, and the three mutations this fold was written against — a wrong
/// range on one event kind, `applied` for a configuration entry, a single-key apply
/// with its `key` dropped — passed every tier without it.
///
/// Three folds over the records the run already walks, each naming the record it
/// fails on:
///
/// - every `Raft*` record about a replica carries one of `hosted`, the ranges the
///   run's configuration fixed — [`SINGLE_GROUP`] alone while a server runs one
///   group, four of them on a node (D-069, D-076). The three events about a node's
///   *store* —
///   `RaftRefused`, `RaftAdopted`, `RaftServerFailed` — carry none, which their
///   types say, so there is nothing to fold for them;
/// - a [`TraceEvent::RaftApply`] with [`ApplyEffect::Applied`] carries the key it
///   executed, and one with [`ApplyEffect::None`] — a no-op, a configuration entry,
///   a command naming no key — carries none. The other five effects belong to §3's
///   re-check and to §5's and §6's range commands and no apply of this stage can
///   produce one (`node.rs`), so one here is a stamp from nowhere and fails too;
///   the stage that emits them widens this arm;
/// - a [`TraceEvent::RaftRead`]'s `applied` is at or above its read index: the core
///   holds a confirmed read until `applied >= index` (`core.rs`), and the value and
///   the index come from one engine version, so a record saying otherwise says the
///   read was served from a state older than the index it claims.
///
/// # Errors
///
/// The first record that breaks one of the three, in words naming it.
// PROPOSED(D-069): the payload of SHARD.md §8's trace has an oracle here.
pub fn payload_is_well_formed(records: &[TraceRecord], hosted: &[u64]) -> Result<(), String> {
    for record in records {
        let range = range_of(&record.event);
        if let Some(range) = range
            && !hosted.contains(&range)
        {
            return Err(format!(
                "the trace's payload: a replica's record carries range {range}, which is not \
                 one of the ranges {hosted:?} this run hosts: {:?}",
                record.event
            ));
        }
        match &record.event {
            TraceEvent::RaftApply {
                server,
                index,
                key,
                effect,
                ..
            } => match effect {
                ApplyEffect::Applied if key.is_none() => {
                    return Err(format!(
                        "the trace's payload: server {server}'s apply of {index} is `applied` \
                         and names no key"
                    ));
                }
                ApplyEffect::None if key.is_some() => {
                    return Err(format!(
                        "the trace's payload: server {server}'s apply of {index} is `none` and \
                         names the key {key:?}"
                    ));
                }
                ApplyEffect::Applied | ApplyEffect::None => {}
                other => {
                    return Err(format!(
                        "the trace's payload: server {server}'s apply of {index} is `{}`, which \
                         belongs to a stage this one does not run",
                        other.as_str()
                    ));
                }
            },
            TraceEvent::RaftRead {
                server,
                index,
                applied,
                key,
                ..
            } if applied < index => {
                return Err(format!(
                    "the trace's payload: server {server} served a read of {key:?} at index \
                     {index} from an engine version whose applied index is {applied}"
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// The messages one delivered payload carries, each with the range it is about.
///
/// A one-group server's frame is one message of [`SINGLE_GROUP`]; a node's frame is
/// a batch, several messages each tagged with its range (D-072). A payload that is
/// neither — a client's request, a packet the network mangled — carries none.
// PROPOSED(D-076): the trace's checks read a batch frame's messages.
#[must_use]
pub fn messages_of(payload: &Bytes) -> Vec<(u64, Message)> {
    // The one-group frame is tried first, and the order is not a preference: the two
    // codecs' first bytes collide. A batch frame's version is 1 and `ananke-raft`'s
    // tag 1 is a pre-vote, so a pre-vote frame — 33 bytes of `1 | from | term |
    // last index | last term` — parses as a batch frame of two messages the codec
    // then refuses, and read batch-first every pre-vote delivery of every one-group
    // scenario would reset no timer at all. It cannot go the other way: a batch
    // frame's first byte is 1, a pre-vote is exactly 33 bytes, `Frame::decode`
    // refuses trailing bytes, and the smallest batch frame is 34 — its five-byte
    // header, a twelve-byte tag and a message of at least seventeen. The node
    // scenario's sweep pins that direction
    // (`no_batch_frame_of_the_node_parses_as_a_frame_of_the_one_group_server`).
    // PROPOSED(D-076): a payload is a one-group frame when it parses as one, and a
    // batch frame otherwise.
    if let Ok(frame) = Frame::decode(payload.clone()) {
        return vec![(SINGLE_GROUP, frame.message)];
    }
    match ananke_shard::decode(payload) {
        Ok(decoded) => decoded
            .messages
            .into_iter()
            .map(|tagged| (tagged.range.get(), tagged.frame.message))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// A leader traces one [`TraceEvent::RaftMatchStarted`] per (range, leader, term,
/// follower, incarnation) *per tracking window*: the rule the event's own
/// definition states — the *first* rise of `matched` under the store incarnation
/// the follower's answer carried (SHARD.md §8) — folded per run, so that a leader
/// emitting one on every rise is seen.
///
/// The rule is a *replica's*, and so is every map here: the leader is the record's
/// node **and the range the record names**, and its term is the latest term record
/// of that replica. Keyed by the node alone — as this fold was written, when a node
/// ran one group — a node leading four ranges reads whichever range's term record
/// came last for all four, and one leader's four first rises under one follower
/// collapse into one key. The node scenario's sweep fails on every seed under that
/// key and no one-range sweep can tell the two apart (D-076).
// PROPOSED(D-076): the fold is keyed by `(range, leader, term, follower,
// incarnation)`.
///
/// A window is one stretch of one replica's leader tracking one follower. It opens
/// at the leader's term, and where the leader *begins tracking the follower
/// afresh*: `on_change` re-inserts a `Progress` with `incarnation: None` and
/// `match_started: false` for every target server outside the voters in force
/// (core.rs:2074-2103). That is a voter removed and re-added inside one
/// leadership, which the correct system does and which issue #81 caught on
/// membership seed 7205 — leader 3 of term 2 grew to {1, 2, 3, 4, 5}, shrank to
/// {1, 2, 3} and grew again, and server 4's match rose from zero a second time
/// under the incarnation it had never left.
///
/// A change of the follower's incarnation clears the flag too (core.rs:1845), and
/// that is deliberately *not* a window: it changes the key's own incarnation, so a
/// repeat under one key means a retired incarnation came back. In the correct
/// system it cannot — a fresh store is incarnation 1 and a re-seed draws
/// `next_u64().max(2)` (node.rs:2327) — so a repeat is either D-042's window, a
/// delayed answer from the store the leader has moved past, which SHARD.md §8 says
/// is the case D-042 names the step to take for and not a bound to widen, or a
/// store that lost its state and opened fresh at 1 again. The second is how
/// `RefusalNotDurable` is caught on 58 of a thousand raft seeds, every one of them
/// by this fold, which is what `a_server_whose_refusal_is_not_durable_is_caught`
/// asserts from the thousand-seed tier (D-056).
///
/// A fresh tracking is read in the trace by *refining* what SHARD.md §8 recognises
/// (SHARD.md:1358-1364): §8 takes a `RaftChangeAccepted` of the leader whose
/// `voters` include `n` as a re-admission on its own, and attaches "while its
/// configuration in force includes `n`" to its `RaftLeader` alternative instead.
/// Three conditions narrow that to exactly the accepts at which `on_change`
/// re-inserts a `Progress`, each one a branch it returns from first:
///
/// - The follower is outside the leader's own configuration in force, its latest
///   `RaftConfig` of that range: `on_change` takes as learners the targets outside
///   `membership.voters` and no others (core.rs:2074-2077).
/// - That configuration is not joint: a change accepted with `new_voters` in force
///   is answered from that branch and never reaches the learners (core.rs:2067).
/// - The accept is not a repeat of the change already in flight, which the core
///   holds through its catch-up phase and accepts again without re-tracking
///   anything (core.rs:2065, D-029) — the scenario's operator repeats it. Two
///   accepts of one target are one window while no `RaftConfig` of the leader's
///   and no term record of its own falls between them: `append_joint` traces the
///   first and ends the phase, and `become_leader` and `become_follower` clear the
///   change without tracing a configuration at all (core.rs:1410, core.rs:1680),
///   which the leader's own `RaftTerm` is what shows.
///
/// The counters cannot see any of this: dropping the `match_started` clause from
/// the core's guard raises this sweep's count from 1 840 to 63 164 at a hundred
/// seeds, on every one of which a match start is still seen, with every seed still
/// green. Measured on this tree, the mutation planted and this fold silenced.
///
/// # Errors
///
/// The first repeat, naming the leader, the term, the follower and the incarnation.
// PROPOSED(D-069): `RaftMatchStarted` is the first rise, and this is what says so.
// PROPOSED(D-079): per tracking window, so that a re-added voter's fresh progress
// is a fresh first rise (issue #81).
pub fn match_starts_are_first_rises(records: &[TraceRecord]) -> Result<(), String> {
    let mut terms: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    // Each replica's configuration as its own `RaftConfig` states it: the voters
    // in force, and whether it is joint.
    let mut in_force: BTreeMap<(u64, u64), (BTreeSet<u64>, bool)> = BTreeMap::new();
    // The voters of the change a leader last accepted with no configuration and no
    // term record of its own traced since.
    let mut last_accepted: BTreeMap<(u64, u64), Vec<u64>> = BTreeMap::new();
    // How many times a leader has begun tracking a follower afresh.
    let mut windows: BTreeMap<(u64, u64, u64), u64> = BTreeMap::new();
    let mut started: BTreeSet<(u64, u64, u64, u64, u64, u64)> = BTreeSet::new();
    for record in records {
        let node = record.node.map_or(0, |node| u64::from(node.get()));
        match &record.event {
            TraceEvent::RaftTerm {
                server,
                range,
                term,
                ..
            }
            | TraceEvent::RaftRecovered {
                server,
                range,
                term,
                ..
            } => {
                terms.insert((*range, *server), *term);
                // Taking office and stepping down both clear the core's change
                // (core.rs:1410, core.rs:1680) without tracing a configuration, so
                // the catch-up phase a repeat would fall in ends here too.
                last_accepted.remove(&(*range, *server));
            }
            TraceEvent::RaftConfig {
                server,
                range,
                old,
                joint,
                ..
            } => {
                in_force.insert((*range, *server), (old.iter().copied().collect(), *joint));
                last_accepted.remove(&(*range, *server));
            }
            TraceEvent::RaftChangeAccepted { range, voters, .. } => {
                if let Some((voters_in_force, joint)) = in_force.get(&(*range, node))
                    && !joint
                    && last_accepted.get(&(*range, node)) != Some(voters)
                {
                    for follower in voters.iter().filter(|s| !voters_in_force.contains(s)) {
                        *windows.entry((*range, node, *follower)).or_default() += 1;
                    }
                }
                last_accepted.insert((*range, node), voters.clone());
            }
            TraceEvent::RaftMatchStarted {
                range,
                follower,
                incarnation,
                matched,
                ..
            } => {
                let term = terms.get(&(*range, node)).copied().unwrap_or_default();
                let window = windows
                    .get(&(*range, node, *follower))
                    .copied()
                    .unwrap_or_default();
                if !started.insert((*range, node, term, *follower, *incarnation, window)) {
                    return Err(format!(
                        "match starts: leader {node} of term {term} of group {range} traced a \
                         second first rise of {follower}'s match under incarnation \
                         {incarnation} (at {matched}), with nothing between them that began \
                         the tracking afresh"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// What one replica's log looks like as the trace tells it, and whether the
/// replica was leading when it looked that way.
///
/// The core keeps the entries past its compacted prefix in memory (D-025), so the
/// length is `last - snap`: `last` moves on an append and on a truncation, and
/// `snap` on a compaction and on a snapshot restated or installed.
///
/// A *take* does not move it. `RaftSnapshot { taken: true }` says a checkpoint was
/// written, not that the log was cut: the core still holds the prefix until
/// `maybe_compact` drops it, which a leader does only once every follower's match
/// is past it (D-037), and the compaction is what `RaftCompacted` reports. The
/// first build of this fold moved `snap` there too, and so read the log as shorter
/// than the core held it for the window between the two — on 27 of 1 000 raft
/// seeds, by as much as 12 entries. The maximum was unaffected, but under-reading
/// is the wrong direction for a bound, so a take is no longer counted here.
///
/// A *re-stated* take is the other way round: the prefix it names really is gone
/// from the store by then, whether the record under it was a take's or an
/// install's, so it does move `snap`. [`Restating`] is what tells the two apart,
/// and it is the same rule the compaction counters read.
// PROPOSED(D-078): Stage B's exit measures the largest in-memory log of any
// follower replica.
#[derive(Clone, Copy, Default)]
struct LogShape {
    snap: u64,
    last: u64,
    leading: bool,
}

impl LogShape {
    fn len(self) -> u64 {
        self.last.saturating_sub(self.snap)
    }
}

/// Folds every replica's log shape over the trace, calling `saw` after each
/// record that changed one, with the server and its shape.
// PROPOSED(D-078): Stage B's exit measures the largest in-memory log of any
// follower replica.
fn fold_logs(
    records: &[TraceRecord],
    mut saw: impl FnMut(u64, LogShape) -> Result<(), String>,
) -> Result<(), String> {
    let mut shapes: BTreeMap<u64, LogShape> = BTreeMap::new();
    let mut restating = Restating::default();
    for record in records {
        restating.saw(record);
        let server = match &record.event {
            TraceEvent::RaftAppend { server, .. }
            | TraceEvent::RaftTruncate { server, .. }
            | TraceEvent::RaftCompacted { server, .. }
            | TraceEvent::RaftSnapshot { server, .. }
            | TraceEvent::RaftRecovered { server, .. }
            | TraceEvent::RaftTerm { server, .. }
            | TraceEvent::RaftLeader { server, .. } => *server,
            _ => continue,
        };
        let shape = shapes.entry(server).or_default();
        match &record.event {
            TraceEvent::RaftAppend { index, .. } => shape.last = shape.last.max(*index),
            // A restatement re-states the durable log as a truncation to one past
            // its end, then the snapshot, then the entries; a conflict truncates.
            TraceEvent::RaftTruncate { from_index, .. } => {
                shape.last = from_index.saturating_sub(1).max(shape.snap);
            }
            // A prefix only ever moves forward: `maybe_compact` compacts to an
            // index past `snap_index` or returns. `max` rather than assignment so
            // the fold says that rather than relying on it.
            TraceEvent::RaftCompacted { through, .. } => {
                shape.snap = shape.snap.max(*through);
                shape.last = shape.last.max(*through);
            }
            // An install and a re-statement stand in for a prefix the log no
            // longer holds; a live take leaves the log as it was ([`Restating`]).
            TraceEvent::RaftSnapshot { last_index, .. }
                if restating.moves_the_prefix(&record.event) =>
            {
                shape.snap = shape.snap.max(*last_index);
                shape.last = shape.last.max(shape.snap);
            }
            // A restart begins as a follower, whatever the last incarnation was.
            TraceEvent::RaftRecovered { .. } => shape.leading = false,
            TraceEvent::RaftTerm { role, .. } => shape.leading = *role == "leader",
            TraceEvent::RaftLeader { .. } => shape.leading = true,
            _ => {}
        }
        let shape = *shape;
        saw(server, shape)?;
    }
    Ok(())
}

/// Every compaction is at or below an index the compacting server knew committed
/// (D-065): a follower compacts to its own applied index, and on the correct
/// system a follower's applied index never passes its commit index, so the prefix
/// it drops holds nothing uncommitted.
///
/// This is the assertion D-065 asks for, and it is the one the leader's rule has
/// always met too — a take is at the applied index, which is at or below the
/// commit index — so it is asked of every server, not only of followers.
///
/// # Errors
///
/// The first compaction past what its server knew committed.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
pub fn compaction_stays_committed(records: &[TraceRecord]) -> Result<(), String> {
    let mut committed: BTreeMap<u64, u64> = BTreeMap::new();
    for record in records {
        match &record.event {
            TraceEvent::RaftCommit { server, index, .. } => {
                let seen = committed.entry(*server).or_default();
                *seen = (*seen).max(*index);
            }
            // An install's prefix, and a re-statement standing in for one, is
            // committed by construction: a leader streams only what it had
            // committed, and the receiver traced no commit of its own for those
            // indices, so without this arm the check would fail on the correct
            // system. A **take** is a different matter, and is excluded: its
            // `last_index` is the taker's *applied* index, the very quantity
            // `ApplyBeforeCommit` corrupts, so a take raising this floor would
            // hand the oracle its bound from the bug it watches for. Raising the
            // floor a compaction is compared against is the only way a check of
            // this shape can hide anything — the reasoning D-078 first recorded
            // here had that backwards, and the review of this slice caught it.
            //
            // Measured, not assumed: with the arm narrowed, `ApplyBeforeCommit`
            // is still caught on 999 of 1 000 seeds, first seed 0 with the same
            // message, and the correct system still passes every seed. Nothing
            // was masked; the weakening was latent.
            TraceEvent::RaftSnapshot {
                server,
                last_index,
                taken: false,
                ..
            } => {
                let seen = committed.entry(*server).or_default();
                *seen = (*seen).max(*last_index);
            }
            TraceEvent::RaftCompacted {
                server, through, ..
            } => {
                let seen = committed.get(server).copied().unwrap_or_default();
                if *through > seen {
                    return Err(format!(
                        "compaction: server {server} compacted through {through}, past the {seen} \
                         it knew committed"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

impl Report {
    /// The events, without their times.
    #[must_use]
    pub fn events(&self) -> Vec<TraceEvent> {
        self.records.iter().map(|r| r.event.clone()).collect()
    }

    /// The largest in-memory log any replica of this run held while it was not
    /// leading, in entries, and the server that held it (Stage B's exit; Q39).
    ///
    /// The log the core keeps in memory is the tail past its compacted prefix
    /// (D-025), so this is `last_index - snap_index` at its highest over the run,
    /// read off the trace. A leader's log is not counted: what the exit bounds is
    /// the follower's, which before D-065 had nothing to bound it.
    // PROPOSED(D-078): Stage B's exit measures the largest in-memory log of any
    // follower replica.
    #[must_use]
    pub fn largest_follower_log(&self) -> (u64, u64) {
        let mut worst = (0u64, 0u64);
        let _ = fold_logs(&self.records, |server, shape| {
            if !shape.leading && shape.len() > worst.0 {
                worst = (shape.len(), server);
            }
            Ok(())
        });
        worst
    }

    /// The compactions this run's replicas made while not leading, and of those
    /// the ones whose prefix swallowed the configuration entry in force
    /// ([`follower_compactions`]).
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    #[must_use]
    pub fn follower_compactions(&self) -> (usize, usize) {
        follower_compactions(&self.records)
    }

    /// The `RaftSnapshot { taken: false }` records split into the installs a
    /// server really made and the prefixes a restart re-stated (`Restating`, which
    /// is where the rule that tells the two apart is written).
    ///
    /// The two are one event kind, and counting them together stopped being honest
    /// with D-065: before a follower compacted, a replica that had neither taken
    /// nor installed opened with `snap_index == 0` and re-stated no snapshot at
    /// all, so the `taken: false` records were installs and the re-statements of
    /// installs. Now every replica that has compacted re-states one at every later
    /// open, and a sweep that calls the whole population "installed" reports
    /// installs that never happened — which is what D-078 first did.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    #[must_use]
    pub fn snapshots_installed_and_restated(&self) -> (usize, usize) {
        let mut restating = Restating::default();
        let (mut installed, mut restated) = (0usize, 0usize);
        for record in &self.records {
            restating.saw(record);
            if let TraceEvent::RaftSnapshot { taken: false, .. } = &record.event {
                if restating.restated() {
                    restated += 1;
                } else {
                    installed += 1;
                }
            }
        }
        (installed, restated)
    }

    /// How many records satisfy `f`.
    pub fn count(&self, f: impl Fn(&TraceEvent) -> bool) -> usize {
        self.records.iter().filter(|r| f(&r.event)).count()
    }

    /// Whether any record satisfies `f`.
    pub fn has(&self, f: impl Fn(&TraceEvent) -> bool) -> bool {
        self.records.iter().any(|r| f(&r.event))
    }

    /// Whether the run was scheduled uniformly, so that liveness can be asked of it.
    #[must_use]
    pub fn uniform(&self) -> bool {
        self.policy == Policy::Uniform
    }

    /// Every range the trace names a replica of.
    // PROPOSED(D-071): the checks about time are asked per range.
    #[must_use]
    pub fn ranges(&self) -> BTreeSet<u64> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RangeCreated { range, .. } | TraceEvent::RangeRemoved { range, .. } => {
                    Some(*range)
                }
                event => range_of(event),
            })
            .collect()
    }

    /// The ranges whose replicas that are neither refused nor quarantined form a
    /// majority at the end of the run: the ranges the checks about time are asked
    /// of (SHARD.md §8), where RAFT.md §2 asked them of the cluster. A refused
    /// replica is down until a snapshot re-seeds it, which its restatement's
    /// `RaftRecovered` says (RAFT.md §3) — but a re-seeded server never votes again
    /// (D-035), so while it counts for commits it cannot help elect, and a range
    /// whose impaired replicas reach half has no leader to wait for: a refused
    /// replica can only be re-seeded *by* a leader, so the deadlock is real and
    /// priced into D-035, not a liveness failure. Seed 8 reaches exactly that, and
    /// is the seed of this tree that does: rot refuses server 2, a leader re-seeds
    /// it and the quarantine sticks (D-035); rot then refuses server 1, which no
    /// leader ever re-seeds; and server 3 is left with nobody able to grant it a
    /// vote. Its live set is empty and nothing about time is asked of the run.
    /// (D-030 and D-035 tell the same story of seed 60 of *their* release runs;
    /// every schedule has been redrawn since, and seed 60 of this tree has its
    /// range live.)
    ///
    /// A node's refusal is its whole store's, so every replica *on it* counts as
    /// refused (SHARD.md §8); its re-seed and its quarantine are per replica.
    ///
    /// "On it" is the load-bearing word, and until D-077 this read it as *every range
    /// in the run*. With one group on a server the two are the same sentence. With four
    /// ranges on a node they are not: a refusal marked its node down for ranges it never
    /// held and for ranges created after it was refused, and since a range whose
    /// impaired replicas reach half is dropped from this set, each of those ranges was
    /// silently exempted from the checks about time — the bound, the recovery margin,
    /// every tooth §8 has. One refusal on a four-range node exempted the three ranges
    /// the refusal did not touch. The direction is the dangerous one: the checks pass
    /// because they are not asked.
    ///
    /// The ranges a refusal takes down are now the ones the node says it held, from the
    /// `RaftReplicaRefused` its refusal traces per replica. They cannot come from the
    /// node's *store*, and this is why the event exists: `RaftRefused` is traced before
    /// the store opens — the refusal is what stops it opening — so at that instant there
    /// is nothing to ask. What the node does have is the ranges §2 fixed at bootstrap,
    /// in its configuration before it touches a disk, and those are what it names.
    ///
    /// A `RaftRefused` with no `RaftReplicaRefused` beside it therefore takes nothing
    /// down. That is not a gap but the one-group server, which hosts a group rather than
    /// holding ranges and whose refusal `sim/quorum.rs` and the `raft` arms still raise:
    /// its range is `SINGLE_GROUP`, the only range of those runs, and it is marked down
    /// by its own per-replica event once the node traces one. Until a scenario runs the
    /// node, the sole reader of this is the node's own sweep.
    // PROPOSED(D-071): the checks about time are asked per range.
    // PROPOSED(D-077): a refusal marks down the ranges its node held, which its
    // per-replica refusal events name, and not every range in the run.
    #[must_use]
    pub fn ranges_with_a_majority_up(&self) -> BTreeSet<u64> {
        let ranges = self.ranges();
        let mut down: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut quarantined: BTreeSet<(u64, u64)> = BTreeSet::new();
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftReplicaRefused { server, range } => {
                    down.insert((*range, *server));
                }
                TraceEvent::RaftRecovered { server, range, .. } => {
                    down.remove(&(*range, *server));
                }
                TraceEvent::RaftReseeded { server, range } => {
                    quarantined.insert((*range, *server));
                }
                _ => {}
            }
        }
        ranges
            .into_iter()
            .filter(|range| {
                let impaired: BTreeSet<u64> = down
                    .union(&quarantined)
                    .filter(|(g, _)| g == range)
                    .map(|&(_, server)| server)
                    .collect();
                (impaired.len() as u64) * 2 < SERVERS
            })
            .collect()
    }

    /// Whether every range of the run has a majority that can still elect a leader
    /// ([`Report::ranges_with_a_majority_up`]): what the sweep's figures print, where
    /// the checks about time ask it of each range on its own.
    #[must_use]
    pub fn majority_up(&self) -> bool {
        self.ranges_with_a_majority_up().len() == self.ranges().len()
    }

    /// How long after the last heal the first client write completed, if one did:
    /// the quickest range's recovery ([`Report::writes_after_heal_by_range`]).
    #[must_use]
    pub fn time_to_write_after_heal(&self) -> Option<Duration> {
        self.writes_after_heal_by_range()
            .into_values()
            .flatten()
            .min()
    }

    /// Per key some client wrote to after the last heal, how long the quickest of
    /// those writes took **from its own call** — `ret − max(call, last_heal)`, and
    /// every write folded here was called at or after the heal — with `None` for a
    /// key whose post-heal writes all stayed pending. The write bound is asked of
    /// each of these (SHARD.md §8).
    ///
    /// The interval is the write's own and not the time since the heal, because the
    /// time since the heal is not the cluster's alone: with eight keys, two clients
    /// and a 60 % write mix, the first post-heal write to one particular key can
    /// simply not be *issued* for two seconds, and reading `ret − last_heal`
    /// charged that idle time to the cluster. With one range and two keys the two
    /// readings all but coincided, which is why no sweep saw it before the node
    /// scenario; with four ranges the nightly found six seeds of ten thousand where
    /// the client's own idleness passed the bound on its own (run 35645688334,
    /// seeds 2400, 4976, 5193, 6508, 6605 and 9204 — on each of the three
    /// reproduced, the key was served in about 25 ms once anybody asked for it).
    ///
    /// What this reading keeps is every tooth about the *cluster*: a post-heal write
    /// that takes longer than the bound to come back still fails it, and a key whose
    /// post-heal writes all stay pending is still the wedge this check is here to
    /// see. What it stops carrying is the recovery time proper — how long after the
    /// heal the range became writable at all — which is read per range instead,
    /// where no client's choice of key can lengthen it
    /// ([`Report::writes_after_heal_by_range`]). Neither reading is widened: the
    /// bound is the same bound, asked of two things the old one conflated.
    ///
    /// An operation the trace closed by its entry's apply counts as completed, as
    /// it does everywhere else the history is read: the client abandoned it, but
    /// the entry applied (`lin.rs`). A write no leader ever proposed is not in the
    /// history at all and is no key's evidence either way.
    // PROPOSED(D-071): the write bound is asked per key.
    // PROPOSED(D-076): a post-heal write is measured from its own call, and the
    // recovery time proper is asked per range.
    #[must_use]
    pub fn writes_after_heal_by_key(&self) -> BTreeMap<Bytes, Option<Duration>> {
        let mut by_key: BTreeMap<Bytes, Option<Duration>> = BTreeMap::new();
        for op in self
            .history
            .ops
            .iter()
            .filter(|op| op.op.is_write() && op.call >= self.last_heal)
        {
            let took = op
                .ret
                .map(|ret| ret.duration_since(op.call.max(self.last_heal)));
            let first = by_key.entry(op.op.key().clone()).or_default();
            *first = match (*first, took) {
                (Some(one), Some(another)) => Some(one.min(another)),
                (one, another) => one.or(another),
            };
        }
        by_key
    }

    /// Per range some client wrote to a key of after the last heal, how long after
    /// **the heal** the first of those writes completed, and `None` for a range
    /// whose post-heal writes all stayed pending: the recovery time proper, which
    /// the per-key reading above no longer carries.
    ///
    /// It is keyed by range and not by the cluster for D-071's reason — one minimum
    /// over every write is passed by a wedged range beside a live one, since the
    /// live one's writes complete — and not by key, because which key a client draws
    /// next is the client's business and not the cluster's. Every range of a
    /// scenario this is asked of is written to within milliseconds of any moment its
    /// clients are running, so this minimum waits on no draw the way one key's does.
    // PROPOSED(D-076): the recovery time proper is asked per range.
    #[must_use]
    pub fn writes_after_heal_by_range(&self) -> BTreeMap<u64, Option<Duration>> {
        let mut by_range: BTreeMap<u64, Option<Duration>> = BTreeMap::new();
        for op in self
            .history
            .ops
            .iter()
            .filter(|op| op.op.is_write() && op.call >= self.last_heal)
        {
            let took = op.ret.map(|ret| ret.duration_since(self.last_heal));
            let first = by_range.entry((self.key_range)(op.op.key())).or_default();
            *first = match (*first, took) {
                (Some(one), Some(another)) => Some(one.min(another)),
                (one, another) => one.or(another),
            };
        }
        by_range
    }

    /// Every invariant the run must satisfy, or the first violation.
    ///
    /// # Errors
    ///
    /// A message naming the seed and the violation.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        let fail = |what: String| Err(format!("seed {seed}: {what}"));
        if let Some(why) = &self.stopped {
            return fail(why.clone());
        }
        if let Err(violation) = invariants::all(crate::traced(&self.records)) {
            return fail(violation);
        }
        if let Err(violation) =
            invariants::commit_majority(crate::traced(&self.records), SERVERS as usize)
        {
            return fail(violation);
        }
        if let Err(violation) = lin::check(&self.history) {
            return fail(violation.to_string());
        }
        if let Some(failed) = self.records.iter().find_map(|r| match &r.event {
            TraceEvent::RaftServerFailed { server, reason } => Some((server, reason)),
            _ => None,
        }) {
            return fail(format!("server {} failed: {}", failed.0, failed.1));
        }
        // PROPOSED(D-069): the trace SHARD.md §8 asks for has an oracle, so that a
        // field stamped wrong fails a sweep before §8's checks are written.
        if let Err(violation) = payload_is_well_formed(&self.records, &self.ranges) {
            return fail(violation);
        }
        if let Err(violation) = match_starts_are_first_rises(&self.records) {
            return fail(violation);
        }
        if let Err(violation) = self.isolation_keeps_the_term() {
            return fail(violation);
        }
        // D-065's two, last of the safety folds so that a run a Phase 2 variant
        // already fails still fails with the violation its pinned seed names.
        // PROPOSED(D-078): a follower compacts its log to its own applied index.
        if let Err(violation) = compaction_stays_committed(&self.records) {
            return fail(violation);
        }
        // Asked of the one-group server alone, and the reason is the node's, not a
        // convenience. D-078's follower compaction is the core asking its `apply` task
        // for a `SnapshotAction::Record`, and the node's host counts that action and
        // drops it: no record is written, no prefix is dropped, and a follower replica
        // on the node has nothing bounding its in-memory log but the run's length. The
        // bound is a bound on a mechanism this node does not run, so asking it here
        // would be asserting a property nobody built — measured at 372 entries over a
        // hundred seeds against the bound's 768, and growing with the tier, which is a
        // nightly waiting to turn red on a claim the entry itself denies. The node's
        // sweep prints the distribution instead, and the slice that wires the
        // `snapshot` task owes the node its own bound.
        // PROPOSED(D-082): the follower-log bound is the one-group server's.
        if self.cluster == Cluster::OneGroup
            && let Err(violation) = self.follower_log_is_bounded()
        {
            return fail(violation);
        }
        // Both are asked only of a range whose unimpaired replicas form a majority
        // (SHARD.md §8), which each reads for itself: with none, both pass.
        if self.uniform() {
            if let Err(violation) = self.liveness() {
                return fail(violation);
            }
            if let Err(violation) = self.timers_fire() {
                return fail(violation);
            }
        }
        Ok(())
    }

    /// The largest in-memory log of any follower replica, against the entry bound
    /// [`FOLLOWER_LOG_BOUND`] (Stage B's exit; Q39).
    ///
    /// A follower compacts one tick after its log passes the threshold, and the
    /// entries of that tick's round, of the persist the compaction waits behind
    /// and of a catch-up batch a follower far behind takes in one go all land on
    /// top of the threshold — so the bound is a multiple of it, not the threshold
    /// itself.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    fn follower_log_is_bounded(&self) -> Result<(), String> {
        let (longest, server) = self.largest_follower_log();
        if longest > FOLLOWER_LOG_BOUND {
            return Err(format!(
                "follower log: server {server} held {longest} entries in memory while not \
                 leading, over the bound of {FOLLOWER_LOG_BOUND} entries"
            ));
        }
        Ok(())
    }

    /// Liveness: on a uniform run, a client write to *every key* of a range whose
    /// unimpaired replicas form a majority completes within [`LIVENESS_TIMEOUTS`]
    /// maximum election timeouts of the last heal (SHARD.md §8).
    ///
    /// The bound is asked of each key some client wrote to after the heal
    /// ([`Report::writes_after_heal_by_key`]), and of no other: a key no client
    /// wrote to in the window is no evidence of anything, and the two clients draw
    /// their keys at random. A key whose post-heal writes all stayed pending is the
    /// wedge this check is here to see. With no range left with a majority nothing
    /// is asked, as nothing was when the check was the cluster's.
    ///
    /// The same bound is asked, per live range, of the recovery time proper: how
    /// long after the heal the first write to any key of that range completed
    /// ([`Report::writes_after_heal_by_range`]). The per-key reading measures each
    /// write from its own call and so cannot carry that; the per-range one waits on
    /// no client's choice of key and so is not the client's idleness read as the
    /// cluster's (D-076).
    fn liveness(&self) -> Result<(), String> {
        let live = self.ranges_with_a_majority_up();
        if live.is_empty() {
            return Ok(());
        }
        let bound = election_max() * LIVENESS_TIMEOUTS;
        let mut asked = 0usize;
        for (key, took) in self.writes_after_heal_by_key() {
            if !live.contains(&(self.key_range)(&key)) {
                continue;
            }
            asked += 1;
            let key = String::from_utf8_lossy(&key).into_owned();
            match took {
                Some(took) if took <= bound => {}
                Some(took) => {
                    return Err(format!(
                        "liveness: the first client write to {key} after the last heal took {took:?} from its own call, over {bound:?}"
                    ));
                }
                None => {
                    return Err(format!(
                        "liveness: no client write to {key} completed after the last heal at {:?}",
                        self.last_heal
                    ));
                }
            }
        }
        for (range, took) in self.writes_after_heal_by_range() {
            if !live.contains(&range) {
                continue;
            }
            match took {
                Some(took) if took <= bound => {}
                Some(took) => {
                    return Err(format!(
                        "liveness: range {range} took {took:?} after the last heal to complete a client write, over {bound:?}"
                    ));
                }
                // A range every one of whose post-heal writes stayed pending is
                // already the per-key reading's wedge, key by key; this arm is here
                // so the two readings cannot disagree about what pending means.
                None => {
                    return Err(format!(
                        "liveness: no client write to range {range} completed after the last heal at {:?}",
                        self.last_heal
                    ));
                }
            }
        }
        if asked == 0 {
            return Err(format!(
                "liveness: no client write completed after the last heal at {:?}",
                self.last_heal
            ));
        }
        Ok(())
    }

    /// Whether reading records by decision time (D-047) moved this run's
    /// verdict, given `verdict`, the result [`Report::check`] returned: the check
    /// as it stood reads the pre-vote and timer checks' records by durability time,
    /// and every other check is the same under both. Worked out from `verdict`
    /// without running the checks that do not move — the linearizability search
    /// above all — so a sweep can report, on every seed, what the entry changed.
    /// The pre-vote check `verdict` comes from also excuses a term change taken
    /// from a message received before the isolation (D-050), so a catch removed
    /// here is removed by either reading.
    // D-047: every trace record carries its decision time and its durability time.
    #[must_use]
    pub fn moved_by_decision_time(&self, verdict: &Result<(), String>) -> Option<Moved> {
        let live = self.uniform();
        let by_durability = |from_pre_vote: bool| -> Result<(), String> {
            if from_pre_vote {
                self.isolation_keeps_the_term_by(RecordTime::Durable)?;
                if live {
                    self.liveness()?;
                }
            }
            if live {
                self.timers_fire_by(RecordTime::Durable)?;
            }
            Ok(())
        };
        match verdict {
            Ok(()) => by_durability(true).err().map(Moved::Lost),
            Err(violation) => {
                let what = violation
                    .split_once(": ")
                    .map_or(violation.as_str(), |(_, what)| what);
                let from_pre_vote = what.starts_with("pre-vote: ");
                if !from_pre_vote && !what.starts_with("timers: ") {
                    return None;
                }
                // A run that failed the timer check passed the pre-vote check by
                // decision time; by durability time it may fail there instead.
                if !from_pre_vote
                    && self
                        .isolation_keeps_the_term_by(RecordTime::Durable)
                        .is_err()
                {
                    return None;
                }
                by_durability(from_pre_vote)
                    .is_ok()
                    .then(|| Moved::Gained(what.to_owned()))
            }
        }
    }

    /// The records of `server` decided before `at` and traced at or after it: the
    /// decisions a timer-check flag at `at` can straddle (D-047).
    // D-047: every trace record carries its decision time and its durability time.
    #[must_use]
    pub fn decisions_straddling(&self, server: u64, at: Instant) -> Vec<&TraceRecord> {
        self.records
            .iter()
            .filter(|r| r.node.is_some_and(|node| u64::from(node.get()) == server))
            .filter(|r| r.decided < at && at <= r.at)
            .collect()
    }

    /// Pre-vote (thesis §9.6): a server that receives nothing does not raise its
    /// term. Checked over every isolation the schedule made: the server's term at
    /// the heal equals its term when the isolation began. An isolation during
    /// which the server was refused, re-seeded or finished installing a snapshot
    /// is skipped: an install's restatement re-states the term the stream
    /// carried, which is no election of the isolated server's (RAFT.md §3).
    ///
    /// The check is about why the server's term moved, so it reads each term
    /// record by the time its step decided it (D-047), and a term change whose
    /// step took a message the server had received by the isolation's start is
    /// that message's doing, however long the message waited for the step (D-050):
    /// [`Report::isolation_keeps_the_term_by_cause`].
    fn isolation_keeps_the_term(&self) -> Result<(), String> {
        self.isolation_keeps_the_term_by_cause()
    }

    /// The pre-vote check [`Report::check`] makes: the check by decision time,
    /// [`Report::isolation_keeps_the_term_by`] under [`RecordTime::Decided`], except
    /// that an isolation is not flagged when every change of its server's term
    /// decided inside the window was taken from a peer's message the server had
    /// received by the window's start ([`TraceRecord::received`]). The server knows
    /// when it received the message and says so on the record, so the check asks
    /// whether the cause arrived before the window without modelling the inbox a
    /// message waits in behind a persist or an install.
    ///
    /// It flags nothing the check by decision time does not: it is that check's
    /// verdict with a named excuse. A change of term from a step that took no
    /// peer's message — a campaign on the server's own timer without pre-vote, a
    /// restatement, a completion — carries no receipt and is flagged as before,
    /// whatever else changed the term in the same window. A candidacy stepped from a
    /// granting `PreVoteResponse`, or from a `TimeoutNow`, carries the receipt of
    /// that message, and is excused when it was received by the isolation's start,
    /// like any change a message caused: the election was decided by what reached
    /// the server before it was cut off.
    ///
    /// # Errors
    ///
    /// The first isolation the check by decision time flags whose term changes
    /// inside the window are not all taken from messages received by its start, in
    /// that check's words.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    pub fn isolation_keeps_the_term_by_cause(&self) -> Result<(), String> {
        let records = self.pre_vote_records();
        self.isolations
            .iter()
            .try_for_each(|&(server, from, until)| {
                Self::keeps_its_term_by_cause(&records, server, from, until)
            })
    }

    /// [`Report::isolation_keeps_the_term_by_cause`] on one isolation: `server` cut
    /// off from `from` to `until`, one of [`Report::isolations`].
    ///
    /// # Errors
    ///
    /// The isolation's violation, in the check's words.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    pub fn isolation_keeps_its_term_by_cause(
        &self,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Result<(), String> {
        Self::keeps_its_term_by_cause(&self.pre_vote_records(), server, from, until)
    }

    /// [`Report::isolation_keeps_its_term_by_cause`] over `records`, the
    /// [`Report::pre_vote_records`].
    // PROPOSED(D-052): the pre-vote check reads its records from one pass over the
    // trace.
    fn keeps_its_term_by_cause(
        records: &[&TraceRecord],
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Result<(), String> {
        Self::ranges_of(records, server)
            .into_iter()
            .try_for_each(|range| {
                Self::replica_keeps_its_term_by_cause(records, range, server, from, until)
            })
    }

    /// [`Report::keeps_its_term_by_cause`] on one replica of the isolated server:
    /// the property is per (range, server) (SHARD.md §8), and the excuse is that
    /// replica's own.
    // PROPOSED(D-071): pre-vote's property is per (range, server).
    fn replica_keeps_its_term_by_cause(
        records: &[&TraceRecord],
        range: u64,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Result<(), String> {
        let verdict = Self::replica_keeps_its_term_by(
            records,
            RecordTime::Decided,
            range,
            server,
            from,
            until,
        );
        if verdict.is_ok() {
            return verdict;
        }
        let changes = Self::term_changes_decided_in(records, range, server, from, until);
        let caused_before = !changes.is_empty()
            && changes
                .iter()
                .all(|r| r.received().is_some_and(|received| received <= from));
        if caused_before { Ok(()) } else { verdict }
    }

    /// Every range the trace shows a replica of on `server`, from `records`, the
    /// [`Report::pre_vote_records`]: the replicas whose terms the pre-vote property
    /// is asked of, which are the replicas that stepped.
    ///
    /// A replica's term record is the only thing read. A `RangeCreated` on the node
    /// adds nothing: a replica that ever steps traces a term record, and one that
    /// never steps has no term to keep — while the creation's `floor_term` would be
    /// read as the term the window began with and its absent term record as 0, so
    /// an arm for it could only report a raise "from `floor_term` to 0" that never
    /// happened. The creation is still read for the floor term of a replica that
    /// *does* step ([`Report::created_term_in`]).
    // PROPOSED(D-071): pre-vote's property is per (range, server).
    fn ranges_of(records: &[&TraceRecord], server: u64) -> BTreeSet<u64> {
        records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftTerm {
                    server: s, range, ..
                } if *s == server => Some(*range),
                _ => None,
            })
            .collect()
    }

    /// `server`'s term records decided in `(from, until]` that change its term from
    /// the term record before them, in record order: what D-050's excuse reads for an
    /// isolation from `from` to `until`.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    #[must_use]
    pub fn isolation_term_changes(
        &self,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Vec<&TraceRecord> {
        let records = self.pre_vote_records();
        Self::ranges_of(&records, server)
            .into_iter()
            .flat_map(|range| Self::term_changes_decided_in(&records, range, server, from, until))
            .collect()
    }

    /// The records the pre-vote check and its predicates read, in record order:
    /// every term record, and every record the check's skip looks for. The trace
    /// holds tens of thousands of records and these a few hundred, so a check over
    /// every isolation passes over the trace once and reads each isolation from
    /// these alone; filtering to the kinds the helpers match keeps every answer.
    // PROPOSED(D-052): the pre-vote check reads its records from one pass over the
    // trace.
    fn pre_vote_records(&self) -> Vec<&TraceRecord> {
        self.records
            .iter()
            .filter(|r| {
                matches!(
                    &r.event,
                    TraceEvent::RaftTerm { .. }
                        | TraceEvent::RaftRefused { .. }
                        | TraceEvent::RaftReseeded { .. }
                        | TraceEvent::RaftSnapshot { taken: false, .. }
                        | TraceEvent::RangeCreated { .. }
                )
            })
            .collect()
    }

    /// `server`'s term records decided in `(from, until]` that change its term from
    /// the term record before them, in record order, from `records`, the
    /// [`Report::pre_vote_records`].
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    fn term_changes_decided_in<'a>(
        records: &[&'a TraceRecord],
        range: u64,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Vec<&'a TraceRecord> {
        let mut previous = 0;
        let mut changes = Vec::new();
        for &record in records {
            let TraceEvent::RaftTerm {
                server: s,
                range: g,
                term,
                ..
            } = &record.event
            else {
                continue;
            };
            if (*g, *s) != (range, server) {
                continue;
            }
            if *term != previous && from < record.decided && record.decided <= until {
                changes.push(record);
            }
            previous = *term;
        }
        changes
    }

    /// The pre-vote check with the server's term records read by `time`: under
    /// [`RecordTime::Decided`] the check [`Report::check`] makes, and under
    /// [`RecordTime::Durable`] the check as it stood before D-047, word
    /// for word. One function for both, so the two cannot drift apart and a pinned
    /// seed can show the gap and the fix side by side.
    ///
    /// Only the term records move with `time`. The skip reads the durability time
    /// under both: it stands in for an install's or a re-seed's restatement landing
    /// inside the window, which is traced when the new incarnation starts, after
    /// the event the skip looks for is durable, so when that event was *recorded*
    /// is the nearer bound on where the restatement lands.
    ///
    /// # Errors
    ///
    /// The first isolation whose server's term differs at its heal from its term at
    /// its start, read by `time`.
    // D-047: every trace record carries its decision time and its durability time.
    pub fn isolation_keeps_the_term_by(&self, time: RecordTime) -> Result<(), String> {
        let records = self.pre_vote_records();
        self.isolations
            .iter()
            .try_for_each(|&(server, from, until)| {
                Self::keeps_its_term_by(&records, time, server, from, until)
            })
    }

    /// [`Report::isolation_keeps_the_term_by`] on one isolation: `server` cut off
    /// from `from` to `until`, one of [`Report::isolations`].
    ///
    /// # Errors
    ///
    /// The isolation's violation, in the check's words.
    pub fn isolation_keeps_its_term_by(
        &self,
        time: RecordTime,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Result<(), String> {
        Self::keeps_its_term_by(&self.pre_vote_records(), time, server, from, until)
    }

    /// [`Report::isolation_keeps_its_term_by`] over `records`, the
    /// [`Report::pre_vote_records`].
    // PROPOSED(D-052): the pre-vote check reads its records from one pass over the
    // trace.
    fn keeps_its_term_by(
        records: &[&TraceRecord],
        time: RecordTime,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Result<(), String> {
        Self::ranges_of(records, server)
            .into_iter()
            .try_for_each(|range| {
                Self::replica_keeps_its_term_by(records, time, range, server, from, until)
            })
    }

    /// [`Report::keeps_its_term_by`] on one replica of the isolated server: the
    /// property is per (range, server) (SHARD.md §8), since a term is a range's and
    /// one range's election says nothing of another's.
    ///
    /// A range created on the isolated node during the isolation takes the term of
    /// its `RangeCreated` as the term the isolation began with (SHARD.md §8): the
    /// replica did not exist at the start, and the term its creation names is no
    /// election of its own.
    ///
    /// The violation names the server, the terms and the window and not the range,
    /// which forty-four pinned assertions take word for word and a run of this stage
    /// has one of; the stage that gives a node many ranges moves those pins and
    /// names it there (SHARD.md, Stage B, the commits that move schedules).
    // PROPOSED(D-071): pre-vote's property is per (range, server).
    fn replica_keeps_its_term_by(
        records: &[&TraceRecord],
        time: RecordTime,
        range: u64,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Result<(), String> {
        if Self::reseeding_during(records, range, server, from, until) {
            return Ok(());
        }
        let before = match Self::term_by(records, range, server, time, from) {
            0 => Self::created_term_in(records, range, server, from, until).unwrap_or(0),
            term => term,
        };
        let after = Self::term_by(records, range, server, time, until);
        if after != before {
            return Err(format!(
                "pre-vote: server {server} raised its term from {before} to {after} while isolated from {from:?} to {until:?}"
            ));
        }
        Ok(())
    }

    /// Whether *any* replica on `server` was refused, re-seeded or finished
    /// installing a snapshot while isolated from `from` to `until`, by when those
    /// records were traced: the skip the pinned-seed straddle predicates take,
    /// where the pre-vote check takes it per replica
    /// ([`Report::reseeding_during`]). It is the skip as it stood before the
    /// property was keyed by (range, server), so a pin's predicate reads the
    /// window it was pinned on.
    // PROPOSED(D-071): pre-vote's property is per (range, server).
    fn reseeding_during_any(
        records: &[&TraceRecord],
        server: u64,
        from: Instant,
        until: Instant,
    ) -> bool {
        records.iter().any(|r| {
            r.at >= from
                && r.at <= until
                && matches!(&r.event,
                    TraceEvent::RaftRefused { server: s, .. }
                    | TraceEvent::RaftReseeded { server: s, .. }
                    | TraceEvent::RaftSnapshot { server: s, taken: false, .. }
                    if *s == server)
        })
    }

    /// The floor term of `server`'s replica of `range` where its `RangeCreated` was
    /// traced while the server was isolated from `from` to `until`, and `None`
    /// otherwise; from `records`, the [`Report::pre_vote_records`].
    // PROPOSED(D-071): pre-vote's property is per (range, server).
    fn created_term_in(
        records: &[&TraceRecord],
        range: u64,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Option<u64> {
        records.iter().find_map(|record| match &record.event {
            TraceEvent::RangeCreated {
                range: g,
                floor_term,
                ..
            } if *g == range
                && record.node.map(|node| u64::from(node.get())) == Some(server)
                && record.at >= from
                && record.at <= until =>
            {
                Some(*floor_term)
            }
            _ => None,
        })
    }

    /// Whether `server` was refused, re-seeded or finished installing a snapshot
    /// while isolated from `from` to `until`, by when those records were traced:
    /// the pre-vote check's skip; from `records`, the [`Report::pre_vote_records`].
    fn reseeding_during(
        records: &[&TraceRecord],
        range: u64,
        server: u64,
        from: Instant,
        until: Instant,
    ) -> bool {
        records.iter().any(|r| {
            r.at >= from
                && r.at <= until
                && match &r.event {
                    // A node's refusal is its whole store's, so it skips every
                    // range on it; the re-seed and the install are one replica's
                    // (SHARD.md §8).
                    TraceEvent::RaftRefused { server: s, .. } => *s == server,
                    TraceEvent::RaftReseeded {
                        server: s,
                        range: g,
                    }
                    | TraceEvent::RaftSnapshot {
                        server: s,
                        range: g,
                        taken: false,
                        ..
                    } => (*g, *s) == (range, server),
                    _ => false,
                }
        })
    }

    /// `server`'s term at `at`: its last `RaftTerm` whose `time` is at or before
    /// `at`, 0 before any. A server's term records come from one task's steps in
    /// sequence, each decided after the one before was traced, so their decision
    /// times rise with their order just as their durability times do, and the last
    /// such record is the latest either way. From `records`, the
    /// [`Report::pre_vote_records`].
    fn term_by(
        records: &[&TraceRecord],
        range: u64,
        server: u64,
        time: RecordTime,
        at: Instant,
    ) -> u64 {
        records
            .iter()
            .filter(|r| time.of(r) <= at)
            .filter_map(|r| match &r.event {
                TraceEvent::RaftTerm {
                    server: s,
                    range: g,
                    term,
                    ..
                } if (*g, *s) == (range, server) => Some(*term),
                _ => None,
            })
            .next_back()
            .unwrap_or(0)
    }

    /// Election timers fire (moirae rule 5): a running replica that is not the
    /// leader of its range campaigns within [`TIMER_TIMEOUTS`] maximum election
    /// timeouts of the last AppendEntries it received from a leader of that range's
    /// term or later, the last vote it granted, its range's `RangeCreated`, or its
    /// start. A re-seeded server is exempt: it never campaigns on that store, by
    /// design (RAFT.md §3, D-035). The first gap [`Report::replay_timers`] finds
    /// under every reset arm, in a range whose unimpaired replicas form a majority
    /// (SHARD.md §8), is the violation.
    ///
    /// The check is per (range, server) (SHARD.md §8): a replica's timer is its
    /// own, reset by what reaches *it*, so one range's heartbeats must not stand in
    /// for another's silence on the same node.
    ///
    /// Whether a replica campaigned in time is about when it decided to, so the
    /// replay reads every record by its decision time (D-047).
    fn timers_fire(&self) -> Result<(), String> {
        self.timers_fire_by(RecordTime::Decided)
    }

    /// The timer check with the records read by `time`: under
    /// [`RecordTime::Durable`], the check as it stood before D-047.
    // D-047: every trace record carries its decision time and its durability time.
    fn timers_fire_by(&self, time: RecordTime) -> Result<(), String> {
        let live = self.ranges_with_a_majority_up();
        let mut first = None;
        self.replay_timers(
            TimerResets::ALL,
            time,
            |gap| {
                if !live.contains(&gap.range) {
                    return ControlFlow::Continue(());
                }
                first = Some(gap);
                ControlFlow::Break(())
            },
            None,
        );
        first.map_or(Ok(()), |gap| Err(gap.violation()))
    }

    /// The timer check's bound for `server`: [`TIMER_TIMEOUTS`] maximum election
    /// timeouts by its own clock, which a slow clock takes longer to measure in
    /// global time.
    fn timer_bound(&self, server: u64) -> Duration {
        let ppm = self.schedule.drifts[server as usize - 1];
        let rate = (1_000_000 + ppm) as f64 / 1_000_000.0;
        (election_max() * TIMER_TIMEOUTS).div_f64(rate)
    }

    /// Whether the `RaftSnapshot` at `index` is the restatement's re-trace of the
    /// replica's snapshot rather than an install's completion: a start traces the
    /// snapshot, the configuration and its `RaftRecovered` at one instant
    /// (`crates/ananke-raft/src/node.rs`), where the completion is traced by the
    /// snapshot task with no restatement behind it. The restatement is that
    /// replica's, so it is looked for on the same (range, server).
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    fn restates(records: &[TraceRecord], index: usize, range: u64, server: u64) -> bool {
        let at = records[index].at;
        records[index..]
            .iter()
            .take_while(|r| r.at == at)
            .any(|r| match &r.event {
                TraceEvent::RaftRecovered {
                    server: s,
                    range: g,
                    ..
                } => (*g, *s) == (range, server),
                _ => false,
            })
    }

    /// The timer check's replay, with the reset arms `resets` names switched on,
    /// handing `gap` every stretch in which a running follower went past its bound:
    /// once per stretch, at the first record past the bound, in record order. The
    /// replay's state goes on past a reported gap exactly as if nothing had been
    /// reported, so a gap closes only when the server is reset, campaigns, leads,
    /// is re-seeded or crashes. `gap` returning `Break` stops the replay.
    ///
    /// [`Report::timers_fire`] is this replay under [`TimerResets::ALL`], stopped at
    /// the first gap; the pinned seeds' predicates are the same replay with an arm
    /// switched off, so the check and the predicates cannot drift apart.
    ///
    /// The records are replayed in the order of `time` (D-047), ties kept
    /// in record order, and each is at its `time`. By durability time that is the
    /// trace's own order. By decision time it is the order the servers decided in:
    /// a campaign, a granted vote or a step-down traced after its persist resets a
    /// server's clock at its step, and is replayed before any record decided later,
    /// so no bound is measured past a reset the server had already made. Records
    /// recorded as they happen — deliveries, sends, crashes, restatements — have one
    /// time and the same place under both.
    ///
    /// With `probe` naming a record, by its index in [`Report::records`], and a
    /// server, the replay also returns that server's state as it stood once the
    /// record was replayed and checked: what [`Report::timer_removal`] compares
    /// between the two readings (issue #33).
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    fn replay_timers(
        &self,
        resets: TimerResets,
        time: RecordTime,
        mut gap: impl FnMut(TimerGap) -> ControlFlow<()>,
        probe: Option<(usize, u64, u64)>,
    ) -> Option<TimerState> {
        // A server measures its timeout by its own clock: a slow one takes longer
        // in global time, and the bound scales with its rate. The clock is the
        // node's, so every replica on it measures the same bound.
        let bound_for = |server: u64| self.timer_bound(server);
        let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
        // Every set below is keyed by the replica, (range, server), not the server:
        // one node runs a group per range and each has its own election timer
        // (SHARD.md §8).
        let mut up: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut leaders: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut reseeded: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut terms: BTreeMap<(u64, u64), u64> = BTreeMap::new();
        let mut clocks = TimerClocks::default();
        let mut reported: BTreeMap<(u64, u64), Instant> = BTreeMap::new();
        let mut probed = None;
        // D-047: the replay's order; stable, so ties keep record order.
        let mut order: Vec<(usize, &TraceRecord)> = self.records.iter().enumerate().collect();
        order.sort_by_key(|(_, record)| time.of(record));
        for (index, record) in order {
            let at = time.of(record);
            clocks.replaying = index;
            match &record.event {
                TraceEvent::RaftReseeded { server, range } => {
                    reseeded.insert((*range, *server));
                }
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDelivered { id, to, .. } => {
                    // Any contact from a leader of the replica's term or later
                    // resets its election timer (moirae rule 5): an AppendEntries,
                    // whether its consistency check passes or not, and equally an
                    // InstallSnapshot, which is how a leader reaches a follower whose
                    // next index has fallen below the leader's compacted prefix
                    // (RAFT.md §1). The core routes the snapshot to its own task, but
                    // the follower is hearing from the leader all the same, and its
                    // incarnation's timer stays fresh across the install. The chunk
                    // counts by its term, not by who sends it: seed 164, found by a
                    // local ten-thousand-seed run on 1373601, was a follower whose
                    // chunks kept arriving from a deposed leader after it lost its
                    // quorum, with no leader in the cluster at all
                    // (`Report::snapshot_fed_timer_gaps`).
                    //
                    // The message resets the timer of **the replica it is addressed
                    // to**, which is a (range, server) and not a server: a frame of
                    // the one-group server carries one message of one group, and a
                    // node's batch frame tags each message it holds with its range
                    // (SHARD.md §4; §11, raft 1), which the decode reads there
                    // (D-072's codec, `ananke_shard::decode`).
                    //
                    // Read as one group's — as this replay stood, when no node sent
                    // a batch frame — every heartbeat a node receives for any of its
                    // four ranges either resets one range's timer or, since a batch
                    // frame does not parse as a single one, resets nothing at all:
                    // on the node scenario's traces the second is what happens, and
                    // every follower replica reads as having heard from no leader
                    // between its own appends. The node sweep fails on half its
                    // seeds under that reading and no one-range sweep can tell the
                    // two apart (D-076).
                    // PROPOSED(D-076): a batch frame resets the timer of each range
                    // it carries.
                    if let Some(server) = server_of(*to)
                        && let Some(payload) = payloads.get(id)
                    {
                        for (range, message) in messages_of(payload) {
                            if message.term() < terms.get(&(range, server)).copied().unwrap_or(0) {
                                continue;
                            }
                            match message {
                                Message::AppendEntries { .. } => clocks.reset((range, server), at),
                                Message::InstallSnapshot { .. } if resets.install_snapshot => {
                                    clocks.reset((range, server), at);
                                }
                                Message::InstallSnapshot { .. } => {
                                    *clocks.installs.entry((range, server)).or_default() += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                // A replica's creation arms its election timer (SHARD.md §8): a
                // group born at a split, or installed onto a node that held none,
                // starts counting from there.
                // PROPOSED(D-071): the timer check is per (range, server).
                TraceEvent::RangeCreated { range, .. } => {
                    if let Some(server) = record.node.map(|node| u64::from(node.get())) {
                        clocks.reset((*range, server), at);
                    }
                }
                // And its removal ends it: a replica whose state was deleted keeps
                // no timer to fire (SHARD.md §8).
                // PROPOSED(D-071): the timer check is per (range, server).
                TraceEvent::RangeRemoved { range, .. } => {
                    if let Some(server) = record.node.map(|node| u64::from(node.get())) {
                        up.remove(&(*range, server));
                        leaders.remove(&(*range, server));
                    }
                }
                // D-039: a completed snapshot install re-states the replica
                // and rebuilds its incarnation with a fresh election timer. The
                // install was the leader's doing and the replica was busy finishing
                // it, so the restatement counts as the leader's contact here. A crash
                // restart re-states the same way and is reset below when its RaftTerm
                // re-admits it; this arm is for the replica that never went down. Seed
                // 385, found by a local ten-thousand-seed run on f54b468: cut off
                // alone mid-install, it campaigned a hundred milliseconds after the
                // switch and twenty-five past the bound
                // (`Report::timer_gaps_rescued_by_restatement`).
                //
                // PROPOSED(D-063) supersedes this arm under [`TimerResets::ALL`]:
                // every restatement on a server that never went down follows a
                // completed install, and D-063's arm below takes the replica out of
                // `up` there, so `up.contains` is false here and the `RaftTerm` the
                // restatement ends with is what resets the clock. The arm and
                // [`TimerResets::WITHOUT_RESTATEMENT`] stay because seed 385's pin
                // is that replay, which is the check as it stood on f54b468.
                TraceEvent::RaftRecovered { server, range, .. }
                    if up.contains(&(*range, *server)) =>
                {
                    if resets.restatement {
                        clocks.reset((*range, *server), at);
                    } else {
                        *clocks.restatements.entry((*range, *server)).or_default() += 1;
                    }
                }
                // PROPOSED(D-063): a completed install ends the incarnation
                // (`Next::Reinstall`, `crates/ananke-raft/src/node.rs`): the server
                // adopts the staged store, opens the engine on it and restates,
                // and only the restatement arms the next core's election timer. It
                // is no more running in between than a crashed server is between
                // its crash and its restart, and the replay treats it the same
                // way: out of `up` at the completion, back in at the restatement's
                // `RaftTerm`, which resets the clock as every start does. Seed
                // 2605, the nightly's (run 35111624618): server 3 finished
                // installing snapshot 374 at 19.704 s, 24.7 ms after the leader's
                // last AppendEntries, and the adoption — RaftAdopted at 19.881 s,
                // the WAL recovered at 19.957 s — restated at 20.002 s, 322.4 ms
                // after that contact and 8.4 ms past its 313.98 ms bound
                // (`Report::timer_gaps_rescued_by_adoption`). The restatement's own
                // re-trace of the snapshot is not a completion: a `RaftRecovered`
                // for the replica follows it at the same instant.
                TraceEvent::RaftSnapshot {
                    server,
                    range,
                    taken: false,
                    ..
                } if up.contains(&(*range, *server))
                    && !Self::restates(&self.records, index, *range, *server) =>
                {
                    if resets.adoption {
                        up.remove(&(*range, *server));
                        leaders.remove(&(*range, *server));
                    } else {
                        *clocks.adoptions.entry((*range, *server)).or_default() += 1;
                    }
                }
                TraceEvent::RaftTerm {
                    server,
                    range,
                    term,
                    role,
                    ..
                } => {
                    terms.insert((*range, *server), *term);
                    if !up.contains(&(*range, *server)) {
                        up.insert((*range, *server));
                        clocks.reset((*range, *server), at);
                    }
                    match *role {
                        "leader" => {
                            leaders.insert((*range, *server));
                        }
                        "pre-candidate" | "candidate" => {
                            leaders.remove(&(*range, *server));
                            clocks.reset((*range, *server), at);
                        }
                        _ => {
                            // A leader that steps down starts counting from here:
                            // its timer meant nothing while it led.
                            if leaders.remove(&(*range, *server)) {
                                clocks.reset((*range, *server), at);
                            }
                        }
                    }
                }
                TraceEvent::RaftLeader { server, range, .. } => {
                    leaders.insert((*range, *server));
                }
                TraceEvent::RaftVote {
                    server,
                    range,
                    granted: true,
                    pre: false,
                    ..
                } => {
                    clocks.reset((*range, *server), at);
                }
                // A crash takes every replica on the node down with it.
                TraceEvent::NodeCrashed { node } => {
                    let server = u64::from(node.get());
                    up.retain(|&(_, s)| s != server);
                    leaders.retain(|&(_, s)| s != server);
                }
                _ => {}
            }
            let mut flagged = BTreeSet::new();
            for &(range, server) in &up {
                if leaders.contains(&(range, server)) || reseeded.contains(&(range, server)) {
                    continue;
                }
                let since = clocks
                    .last_reset
                    .get(&(range, server))
                    .copied()
                    .unwrap_or(at);
                if at.duration_since(since) > bound_for(server)
                    && reported.get(&(range, server)) != Some(&since)
                {
                    reported.insert((range, server), since);
                    flagged.insert((range, server));
                    let found = TimerGap {
                        server,
                        range,
                        since,
                        at,
                        record: index,
                        installs: clocks.installs.get(&(range, server)).copied().unwrap_or(0),
                        restatements: clocks
                            .restatements
                            .get(&(range, server))
                            .copied()
                            .unwrap_or(0),
                        adoptions: clocks.adoptions.get(&(range, server)).copied().unwrap_or(0),
                    };
                    if gap(found).is_break() {
                        return probed;
                    }
                }
            }
            if let Some((target, range, server)) = probe
                && target == index
            {
                probed = Some(TimerState {
                    up: up.contains(&(range, server)),
                    leader: leaders.contains(&(range, server)),
                    reseeded: reseeded.contains(&(range, server)),
                    since: clocks.last_reset.get(&(range, server)).copied(),
                    since_record: clocks.last_reset_record.get(&(range, server)).copied(),
                    flagged: flagged.contains(&(range, server)),
                });
            }
        }
        probed
    }
}

/// Which of a record's two times a check reads (D-047). A node traces a
/// step's events once what they report is durable (D-026), so a record's
/// [`TraceRecord::at`] is when it became durable and its
/// [`TraceRecord::decided`] when the step that produced it was taken; for every
/// record traced as it happens the two are one instant.
// D-047: every trace record carries its decision time and its durability time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordTime {
    /// When the step was taken: for a check about why a server did something, and
    /// so about what it could have known when it did.
    Decided,
    /// When the record was traced, which is when what it reports was durable: for a
    /// check about what was durable when.
    Durable,
}

/// How reading records by decision time moved one run's verdict
/// ([`Report::moved_by_decision_time`]).
// D-047: every trace record carries its decision time and its durability time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Moved {
    /// The run passes, and by durability time it failed with this violation.
    Lost(String),
    /// The run fails with this pre-vote or timer violation, and by durability time
    /// it passed.
    Gained(String),
}

impl RecordTime {
    /// `record`'s time of this kind.
    #[must_use]
    pub fn of(self, record: &TraceRecord) -> Instant {
        match self {
            Self::Decided => record.decided,
            Self::Durable => record.at,
        }
    }
}

/// Which reset arms of the timer check's replay are switched on
/// ([`Report::timer_gaps`]). An AppendEntries from a leader of the server's term or
/// later, a granted vote, a campaign, a start and a leader's step-down always
/// reset the clock; these two are the arms added after the check first stood,
/// each for a seed a ten-thousand-seed run found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerResets {
    /// An `InstallSnapshot` chunk delivered from a server of the receiver's term or
    /// later (D-030's stanza, added on f54b468 for seed 164).
    pub install_snapshot: bool,
    /// An install's restatement, `RaftRecovered` on a server that never went down
    /// (D-039, for seed 385).
    pub restatement: bool,
    /// A completed install takes the server out of the replay's running set until
    /// its restatement puts it back: the incarnation ended at the completion and
    /// the next one starts at the restatement, so there is no election timer in
    /// between (PROPOSED D-063, for seed 2605).
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    pub adoption: bool,
}

impl TimerResets {
    /// Every arm: the check [`Report::check`] makes.
    pub const ALL: Self = Self {
        install_snapshot: true,
        restatement: true,
        adoption: true,
    };
    /// Neither arm: the check as it stood on 1373601, which read only
    /// AppendEntries as a leader's contact.
    pub const APPEND_ENTRIES_ONLY: Self = Self {
        install_snapshot: false,
        restatement: false,
        adoption: false,
    };
    /// Every arm but D-039's: the check as it stood on f54b468.
    pub const WITHOUT_RESTATEMENT: Self = Self {
        install_snapshot: true,
        restatement: false,
        adoption: false,
    };
    /// Every arm but D-063's: the check as it stood on 1a1cad2, which read the
    /// install's restatement as the leader's contact but measured the adoption
    /// before it against the bound.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    pub const WITHOUT_ADOPTION: Self = Self {
        install_snapshot: true,
        restatement: true,
        adoption: false,
    };
}

/// One stretch in which a running follower, neither leading nor re-seeded, went
/// past its timer bound in the timer check's replay ([`Report::timer_gaps`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerGap {
    /// The server.
    pub server: u64,
    /// The range whose replica on it went past the bound: the timer is the
    /// replica's, not the node's (SHARD.md §8).
    // PROPOSED(D-071): the timer check is per (range, server).
    pub range: u64,
    /// Its clock's last reset under the replay's arms.
    pub since: Instant,
    /// The first record past its bound: where the check would have reported it.
    pub at: Instant,
    /// That record's index in [`Report::records`] (issue #33).
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    pub record: usize,
    /// `InstallSnapshot` chunks of the server's term or later delivered to it in
    /// `(since, at]` that did not reset its clock: zero when that arm is on.
    pub installs: usize,
    /// Install restatements on it, while it was up, in `(since, at]` that did not
    /// reset its clock: zero when that arm is on.
    pub restatements: usize,
    /// Installs it completed, while it was up, in `(since, at]` that did not take
    /// it out of the replay's running set: zero when that arm is on.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    pub adoptions: usize,
}

impl TimerGap {
    /// The timer check's words for this gap.
    #[must_use]
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    pub fn violation(&self) -> String {
        format!(
            "timers: server {}'s replica of range {} heard from no leader of its term and granted no vote since {:?} and had not campaigned by {:?}",
            self.server, self.range, self.since, self.at
        )
    }
}

/// The per-replica clocks of the timer replay, each keyed by (range, server), and
/// what arrived since each reset that the replay's arms did not count as one.
#[derive(Default)]
struct TimerClocks {
    last_reset: BTreeMap<(u64, u64), Instant>,
    /// The index in [`Report::records`] of the record behind each `last_reset`.
    last_reset_record: BTreeMap<(u64, u64), usize>,
    installs: BTreeMap<(u64, u64), usize>,
    restatements: BTreeMap<(u64, u64), usize>,
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    adoptions: BTreeMap<(u64, u64), usize>,
    /// The index of the record being replayed.
    replaying: usize,
}

impl TimerClocks {
    fn reset(&mut self, replica: (u64, u64), at: Instant) {
        self.last_reset.insert(replica, at);
        self.last_reset_record.insert(replica, self.replaying);
        self.installs.remove(&replica);
        self.restatements.remove(&replica);
        self.adoptions.remove(&replica);
    }
}

/// One server's state in the timer check's replay once a given record was
/// replayed and checked: what the timer check's replay returns for a probe.
// PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
// names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerState {
    /// Running: started and not crashed since.
    pub up: bool,
    /// Leading, so its timer is not checked.
    pub leader: bool,
    /// On a re-seeded store, so its timer is not checked.
    pub reseeded: bool,
    /// Its clock's last reset.
    pub since: Option<Instant>,
    /// The index in [`Report::records`] of the record that made that reset.
    pub since_record: Option<usize>,
    /// Whether the replay reported a gap for it at this record.
    pub flagged: bool,
}

/// Why a timer catch the check by durability time made is not made by decision
/// time ([`Report::timer_removal`]): each names the record whose two times differ
/// and so place it differently against the flag record under the two readings.
// PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
// names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerRemoval {
    /// By decision time the server's clock was last reset, at the flag record, later
    /// than by durability time, by this record: a reset of the server decided by the
    /// flag record's decision and traced after the flag record.
    ResetMovedBack {
        /// The reset's index in [`Report::records`].
        reset: usize,
    },
    /// The flag record itself, read by its decision time, is within the server's
    /// bound of the reset the check by durability time measured from: decided at
    /// or before that reset plus the bound, traced after it.
    FlagMovedBack {
        /// The flag record's index in [`Report::records`].
        flag: usize,
    },
    /// By decision time the server was leading, down or on a re-seeded store at the
    /// flag record, so its timer was not checked there, where by durability time it
    /// was a running follower: this record of its status is replayed before the flag
    /// record under one reading and after it under the other.
    StatusMoved {
        /// The status record's index in [`Report::records`].
        record: usize,
    },
}

/// The trace predicates the pinned seeds assert (CLAUDE.md: a pinned seed asserts
/// its mechanism, never just green). Each names the situation a seed was pinned
/// for as a function of the trace alone — [`Report::records`], with server-to-server
/// payloads decoded by [`Frame::decode`], and [`Report::schedule`] and
/// [`Report::last_heal`] where it says so — so a pin can assert that its seed
/// still reaches the situation, or that the seed's schedule has moved away from it
/// and the day it comes back the pin says so.
impl Report {
    /// Every gap the timer check's replay finds with the arms `resets` names
    /// switched on, one per stretch past a server's bound, reading every record by
    /// its decision time as the check does (D-047). Under
    /// [`TimerResets::ALL`] this is empty exactly when the check's timer rule
    /// passes.
    #[must_use]
    pub fn timer_gaps(&self, resets: TimerResets) -> Vec<TimerGap> {
        self.timer_gaps_by(resets, RecordTime::Decided)
    }

    /// [`Report::timer_gaps`] with the records read by `time`: under
    /// [`RecordTime::Durable`], the replay as it stood before D-047.
    // D-047: every trace record carries its decision time and its durability time.
    #[must_use]
    pub fn timer_gaps_by(&self, resets: TimerResets, time: RecordTime) -> Vec<TimerGap> {
        let mut gaps = Vec::new();
        self.replay_timers(
            resets,
            time,
            |gap| {
                gaps.push(gap);
                ControlFlow::Continue(())
            },
            None,
        );
        gaps
    }

    /// Why the check by decision time does not make `gap`, the first gap of the
    /// timer check's replay by durability time (D-047, issue #33), derived from the
    /// two replays themselves rather than from a list of the ways it can happen.
    ///
    /// Both replays are read at the gap's flag record `X`. By durability time the
    /// flagged server is there a running follower whose clock was last reset at
    /// `gap.since`, more than its bound before `X`. By decision time the replay does
    /// not flag it at `X` exactly when it is leading, down or re-seeded there, or its
    /// clock's last reset `S'` is within the bound of `X`'s decision time. So every
    /// removal has at least one of these reasons, and each is backed by a record
    /// whose two times place it differently against `X`:
    ///
    /// - [`TimerRemoval::StatusMoved`]: the server's status at `X` differs. Its
    ///   status records — its `RaftTerm`s and `RaftLeader`s, its `RaftReseeded` and
    ///   its crashes — come from one task in sequence, their decision times rising
    ///   with their order, so the records either reading replays before `X` are a
    ///   prefix of that sequence; the status differs only if the two prefixes do,
    ///   and some status record is before `X` under one reading and after it under
    ///   the other.
    /// - [`TimerRemoval::ResetMovedBack`]: `S'` is later than `gap.since`. The
    ///   record behind `S'` is replayed before `X` by decision time; had it been
    ///   before `X` in the trace's order the replay by durability time would have
    ///   reset the clock there too, since a reset under the decision order is a
    ///   reset under the trace's (a delivery counts against a term no higher, and
    ///   every other reset reads the server's own records, whose order both readings
    ///   share); so it is after `X` in the trace's order and decided before it was
    ///   traced.
    /// - [`TimerRemoval::FlagMovedBack`]: `X`, read by its decision time, is within
    ///   the bound of `gap.since`; then `X` was decided before it was traced, since
    ///   by durability time it is past the bound.
    ///
    /// Every applicable reason is returned. In the simulator's traces the first
    /// record at any instant is `TimeAdvanced`, decided as it is recorded, so the
    /// flag record is one and `FlagMovedBack` does not arise there; it does in a
    /// trace without those records, and the reason stays exact for any trace.
    ///
    /// # Errors
    ///
    /// When the replay by durability time does not flag `gap` at its record, when the
    /// replay by decision time flags it there too, or when a reason's backing record
    /// is not there: each is a fault in the reasoning, not a removal.
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    pub fn timer_removal(&self, gap: &TimerGap) -> Result<Vec<TimerRemoval>, String> {
        let (server, range, flag) = (gap.server, gap.range, gap.record);
        let x = self
            .records
            .get(flag)
            .ok_or_else(|| format!("the flag record {flag} is not in the trace"))?;
        let state = |time| {
            self.replay_timers(
                TimerResets::ALL,
                time,
                |_| ControlFlow::Continue(()),
                Some((flag, range, server)),
            )
            .ok_or_else(|| format!("the replay by {time:?} time never replayed record {flag}"))
        };
        let durable = state(RecordTime::Durable)?;
        if !(durable.flagged
            && durable.up
            && !durable.leader
            && !durable.reseeded
            && durable.since == Some(gap.since))
        {
            return Err(format!(
                "the replay by durability time does not flag server {server} since {:?} at record {flag}: {durable:?}",
                gap.since
            ));
        }
        let decided = state(RecordTime::Decided)?;
        if decided.flagged {
            return Err(format!(
                "the replay by decision time flags server {server} at record {flag} too: {decided:?}"
            ));
        }
        // Before the flag record under each reading: by index, and by decision time
        // with ties in record order.
        let before_by_decision =
            |index: usize, r: &TraceRecord| (r.decided, index) < (x.decided, flag);
        let mut reasons = Vec::new();
        if decided.leader || decided.reseeded || !decided.up {
            let moved = self.records.iter().enumerate().find(|&(index, r)| {
                let status = match &r.event {
                    TraceEvent::RaftTerm {
                        server: s,
                        range: g,
                        ..
                    }
                    | TraceEvent::RaftLeader {
                        server: s,
                        range: g,
                        ..
                    }
                    | TraceEvent::RaftReseeded {
                        server: s,
                        range: g,
                        ..
                    } => (*g, *s) == (range, server),
                    TraceEvent::NodeCrashed { node } => u64::from(node.get()) == server,
                    // PROPOSED(D-063): a completed install takes the server out of
                    // the replay's running set until its restatement, so it says
                    // its status the way a crash does — and, being traced once the
                    // staged store is durable and decided when the stream was
                    // staged, it is exactly the kind of record whose two times
                    // place it differently against a flag record. The
                    // restatement's own re-trace of the snapshot changes no status
                    // and is not one of these.
                    TraceEvent::RaftSnapshot {
                        server: s,
                        range: g,
                        taken: false,
                        ..
                    } => {
                        (*g, *s) == (range, server) && !Self::restates(&self.records, index, *g, *s)
                    }
                    _ => false,
                };
                status && (index < flag) != before_by_decision(index, r)
            });
            match moved {
                Some((record, _)) => reasons.push(TimerRemoval::StatusMoved { record }),
                None => {
                    return Err(format!(
                        "by decision time server {server} is not checked at record {flag} ({decided:?}), but no record of its status is placed differently against that record"
                    ));
                }
            }
        } else {
            let since = decided
                .since
                .ok_or_else(|| format!("server {server} runs with no reset by decision time"))?;
            if since > gap.since {
                let reset = decided
                    .since_record
                    .ok_or_else(|| format!("server {server}'s reset has no record"))?;
                let r = &self.records[reset];
                if !(reset > flag && r.decided < r.at) {
                    return Err(format!(
                        "server {server}'s clock is reset later by decision time, by record {reset}, which is not a reset decided before and traced after record {flag}: {r:?}"
                    ));
                }
                reasons.push(TimerRemoval::ResetMovedBack { reset });
            }
            if x.decided < x.at && x.decided.duration_since(gap.since) <= self.timer_bound(server) {
                reasons.push(TimerRemoval::FlagMovedBack { flag });
            }
            if reasons.is_empty() {
                return Err(format!(
                    "by decision time server {server} is not flagged at record {flag}, but neither its reset nor the flag record moved: {decided:?}"
                ));
            }
        }
        Ok(reasons)
    }

    /// The isolation a pre-vote violation of the check by `time` names: the first of
    /// [`Report::isolations`] whose own verdict under that check is `violation`,
    /// word for word (issue #33).
    #[must_use]
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    pub fn isolation_named_by(
        &self,
        time: RecordTime,
        violation: &str,
    ) -> Option<(u64, Instant, Instant)> {
        self.isolations
            .iter()
            .copied()
            .find(|&(server, from, until)| {
                self.isolation_keeps_its_term_by(time, server, from, until)
                    .is_err_and(|e| e == violation)
            })
    }

    /// Seed 164's situation: a follower past its timer bound on AppendEntries alone
    /// while `InstallSnapshot` chunks of its term kept arriving — the gaps of the
    /// replay with neither the InstallSnapshot arm nor D-039's
    /// restatement arm, that hold at least one such chunk. On the trace seed 164
    /// failed with (1373601) this is one gap: server 2 from 12.9405 s, flagged at
    /// 13.3397 s against a 399.19 ms bound, with 21 chunks in it. That gap also
    /// holds a restatement (13.1429 s), so D-039's arm alone would have silenced it
    /// too: no seed-164 predicate isolates the InstallSnapshot arm.
    #[must_use]
    pub fn snapshot_fed_timer_gaps(&self) -> Vec<TimerGap> {
        self.timer_gaps(TimerResets::APPEND_ENTRIES_ONLY)
            .into_iter()
            .filter(|gap| gap.installs > 0)
            .collect()
    }

    /// Seed 385's situation: a follower past its timer bound that an install's
    /// restatement inside the stretch would have reset — the gaps of the replay
    /// with every arm but D-039's that hold at least one restatement on
    /// the server while it was up. When [`Report::check`] passes, every gap of
    /// that replay is one of these, since a stretch without a restatement is
    /// flagged by the check itself. On the trace seed 385 failed with (f54b468)
    /// this is one gap: server 1 from 14.0308 s, restated at 14.2596 s, flagged at
    /// 14.3350 s against a 302.26 ms bound.
    #[must_use]
    pub fn timer_gaps_rescued_by_restatement(&self) -> Vec<TimerGap> {
        self.timer_gaps(TimerResets::WITHOUT_RESTATEMENT)
            .into_iter()
            .filter(|gap| gap.restatements > 0)
            .collect()
    }

    /// Seed 2605's situation: a follower past its timer bound over a stretch it
    /// spent adopting a completed install — the gaps of the replay with every arm
    /// but D-063's that hold at least one install this server completed while it
    /// was up. When [`Report::check`] passes, every gap of that replay is one of
    /// these, since a stretch with no completed install in it is flagged by the
    /// check itself. On the trace seed 2605 failed with (the nightly's run
    /// 35111624618, HEAD 1a1cad2) this is one gap: server 3 from 19.679704065 s,
    /// the leader's last AppendEntries; the install of snapshot 374 completed
    /// 24.655 ms later, at 19.704359292 s; and the adoption it started —
    /// `RaftAdopted` at 19.880743071 s, the WAL recovered at 19.957360426 s —
    /// restated at 20.002065925 s, 322.362 ms after that contact and 8.378 ms past
    /// its 313.983572 ms bound, with the flag falling at the first record past the
    /// bound, 19.994418991 s. Ten of the leader's frames were aimed at the server
    /// inside that stretch — nine `AppendEntries` and one `InstallSnapshot`, all
    /// from server 1 — and the partition at 19.729 s dropped every one of them at
    /// the send as `Partitioned`, with one client frame beside them; nothing was
    /// delivered to server 3 in the window at all.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    #[must_use]
    pub fn timer_gaps_rescued_by_adoption(&self) -> Vec<TimerGap> {
        self.timer_gaps(TimerResets::WITHOUT_ADOPTION)
            .into_iter()
            .filter(|gap| gap.adoptions > 0)
            .collect()
    }

    /// Seeds 1885's and 2023's situation, and the nightly's eleven variant catches
    /// of the same shape (D-047): a term rise of an isolated server that
    /// straddles the isolation's start — its step decided at or before `from` and
    /// its record traced after `from`, by `until` — so the pre-vote check reads it
    /// inside the window by durability time and before it by decision time. Those
    /// are the boundaries the two readings draw, so a catch the check by durability
    /// time makes on an isolation and the check by decision time does not has a
    /// straddle on that isolation, the server's terms rising along its records.
    /// Each carries how many messages from a server were delivered to the isolated
    /// one in `(from, until]`: a rise with none there was caused by nothing that
    /// reached it while it was cut off. Each also carries the messages from a server
    /// delivered to it at the instant its step was decided, as (sender, kind, term):
    /// what the step can have taken, so a pin can tie the decision time to its cause
    /// and not only place it before the window. Isolations the pre-vote check skips
    /// are skipped here too, so this is exactly what the durability-time check reads
    /// as a rise and the decision-time check does not.
    ///
    /// On the trace seed 1885 failed with (run 34711427220, 14c3e17): server 1's
    /// term 10, decided at the step that took a RequestVote delivered at 15.2005 s,
    /// traced at 15.2030 s when its persist was durable, 48 µs after a partition
    /// that began at 15.2030 s, with no delivery to it until 17.112 s.
    // D-047: every trace record carries its decision time and its durability time.
    #[must_use]
    pub fn isolation_term_straddles(&self) -> Vec<TermStraddle> {
        let pre_vote = self.pre_vote_records();
        let sent: BTreeMap<ananke_env::MessageId, SentMessage> = self
            .raft_messages()
            .into_iter()
            .map(|m| (m.id, m))
            .collect();
        let delivered_at = |server: u64, at: Instant| -> Vec<(u64, &'static str, u64)> {
            self.records
                .iter()
                .filter(|r| r.at == at)
                .filter_map(|r| match &r.event {
                    TraceEvent::MessageDelivered { id, to, .. }
                        if server_of(*to) == Some(server) =>
                    {
                        sent.get(id)
                            .map(|m| (m.from, m.message.kind(), m.message.term()))
                    }
                    _ => None,
                })
                .collect()
        };
        let mut straddles = Vec::new();
        for &(server, from, until) in &self.isolations {
            if Self::reseeding_during_any(&pre_vote, server, from, until) {
                continue;
            }
            let mut previous = 0;
            for record in &self.records {
                let TraceEvent::RaftTerm {
                    server: s,
                    term,
                    role,
                    ..
                } = &record.event
                else {
                    continue;
                };
                if *s != server {
                    continue;
                }
                // PROPOSED(D-051): the window's boundaries as the two readings
                // draw them — a record is before the isolation by durability time
                // when traced at or before `from`, by decision time when decided at
                // or before it — so this is exactly the change the two readings
                // place on different sides of `from`.
                if record.decided <= from
                    && from < record.at
                    && record.at <= until
                    && *term != previous
                {
                    let deliveries = self
                        .records
                        .iter()
                        .filter(|r| r.at > from && r.at <= until)
                        .filter(|r| {
                            matches!(&r.event, TraceEvent::MessageDelivered { from: f, to, .. }
                                if server_of(*to) == Some(server) && server_of(*f).is_some())
                        })
                        .count();
                    straddles.push(TermStraddle {
                        server,
                        from,
                        until,
                        before: previous,
                        term: *term,
                        role,
                        decided: record.decided,
                        at: record.at,
                        deliveries,
                        causes: delivered_at(server, record.decided),
                    });
                }
                previous = *term;
            }
        }
        straddles
    }

    /// Issue #32's situation (D-050): a change of an isolated server's term whose
    /// step was decided inside the isolation, in `(from, until]`, and took a
    /// peer's message the server had received by the isolation's start — a message
    /// that waited in the inbox behind a persist or an install. The check by
    /// decision time reads the change inside the window; the check
    /// [`Report::check`] makes excuses it. Each carries the messages from a server
    /// delivered to the isolated one at the instant it received the message, as
    /// (sender, kind, term), so a pin can tie the receipt to the message and not
    /// only place it before the window, and how many messages from a server were
    /// delivered to it in `(from, until]`. Isolations the pre-vote check skips are
    /// skipped here too.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    #[must_use]
    pub fn isolation_received_straddles(&self) -> Vec<ReceivedStraddle> {
        let pre_vote = self.pre_vote_records();
        let sent: BTreeMap<ananke_env::MessageId, SentMessage> = self
            .raft_messages()
            .into_iter()
            .map(|m| (m.id, m))
            .collect();
        let mut straddles = Vec::new();
        for &(server, from, until) in &self.isolations {
            if Self::reseeding_during_any(&pre_vote, server, from, until) {
                continue;
            }
            let mut previous = 0;
            for record in &self.records {
                let TraceEvent::RaftTerm {
                    server: s,
                    term,
                    role,
                    ..
                } = &record.event
                else {
                    continue;
                };
                if *s != server {
                    continue;
                }
                if let Some(received) = record.received()
                    && received <= from
                    && from < record.decided
                    && record.decided <= until
                    && *term != previous
                {
                    let deliveries = self
                        .records
                        .iter()
                        .filter(|r| r.at > from && r.at <= until)
                        .filter(|r| {
                            matches!(&r.event, TraceEvent::MessageDelivered { from: f, to, .. }
                                if server_of(*to) == Some(server) && server_of(*f).is_some())
                        })
                        .count();
                    let causes = self
                        .records
                        .iter()
                        .filter(|r| r.at == received)
                        .filter_map(|r| match &r.event {
                            TraceEvent::MessageDelivered { id, to, .. }
                                if server_of(*to) == Some(server) =>
                            {
                                sent.get(id)
                                    .map(|m| (m.from, m.message.kind(), m.message.term()))
                            }
                            _ => None,
                        })
                        .collect();
                    straddles.push(ReceivedStraddle {
                        server,
                        from,
                        until,
                        before: previous,
                        term: *term,
                        role,
                        received,
                        decided: record.decided,
                        at: record.at,
                        deliveries,
                        causes,
                    });
                }
                previous = *term;
            }
        }
        straddles
    }

    /// Every installed snapshot that put a server's snapshot floor below one its
    /// snapshots had already reached: where the checker's floor rule as built on
    /// ea6fe7d — a floor that only rose — and the rule since (an install sets the
    /// floor exactly, D-030) first disagree. Seed 7381's precondition: a refused
    /// server re-seeded from a leader snapshot older than its lost store's. On the
    /// trace seed 7381 failed with there are three, all server 2 at index 58 under
    /// a floor of 128 (7.951 s, then two restatements of the same install).
    #[must_use]
    pub fn floor_lowering_installs(&self) -> Vec<FloorLowering> {
        self.fold_floors().0
    }

    /// Seed 7381's situation: a restatement whose recovered applied index makes
    /// state machine safety replay an index inside `(exact floor, risen floor]` —
    /// covered by the floor that only rose, held or not by the log under the exact
    /// one, so the two rules give that index different answers. On the trace seed
    /// 7381 failed with this is server 2 at 8.2896 s, index 65, exact floor 58,
    /// risen floor 128.
    #[must_use]
    pub fn recoveries_under_a_lost_floor(&self) -> Vec<LostFloorReplay> {
        self.fold_floors().1
    }

    /// The fold behind the two floor predicates: per server the floor as it only
    /// rises (`high`), the floor as an install sets it exactly (`exact`), and the
    /// checker's last applied index.
    fn fold_floors(&self) -> (Vec<FloorLowering>, Vec<LostFloorReplay>) {
        let mut high: BTreeMap<u64, u64> = BTreeMap::new();
        let mut exact: BTreeMap<u64, u64> = BTreeMap::new();
        let mut applied: BTreeMap<u64, u64> = BTreeMap::new();
        let mut lowered = Vec::new();
        let mut replays = Vec::new();
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftSnapshot {
                    server,
                    last_index,
                    taken,
                    ..
                } => {
                    let risen = high.entry(*server).or_default();
                    if !taken && *last_index < *risen {
                        lowered.push(FloorLowering {
                            server: *server,
                            at: record.at,
                            last_index: *last_index,
                            floor: *risen,
                        });
                    }
                    *risen = (*risen).max(*last_index);
                    let set = exact.entry(*server).or_default();
                    if *taken {
                        *set = (*set).max(*last_index);
                    } else {
                        *set = *last_index;
                        let last = applied.entry(*server).or_default();
                        *last = (*last).max(*last_index);
                    }
                }
                TraceEvent::RaftApply { server, index, .. } => {
                    applied.insert(*server, *index);
                }
                TraceEvent::RaftRefused { server, .. } => {
                    applied.remove(server);
                }
                TraceEvent::RaftRecovered {
                    server,
                    applied: through,
                    ..
                } => {
                    let from = applied.get(server).copied().unwrap_or(0) + 1;
                    let (risen, set) = (
                        high.get(server).copied().unwrap_or(0),
                        exact.get(server).copied().unwrap_or(0),
                    );
                    let (first, last) = (from.max(set + 1), (*through).min(risen));
                    if risen > set && first <= last {
                        replays.push(LostFloorReplay {
                            server: *server,
                            at: record.at,
                            exact: set,
                            risen,
                            indices: (first, last),
                        });
                    }
                    if *through >= from {
                        applied.insert(*server, *through);
                    }
                }
                _ => {}
            }
        }
        (lowered, replays)
    }

    /// Every window between a completed snapshot install and the adoption that
    /// puts it in service (D-041), with the first crash that landed inside
    /// it: opened by an installed `RaftSnapshot`, closed by the server's
    /// `RaftAdopted`, `RaftRecovered` (a tree without adoption, or a restart's
    /// restatement) or `RaftRefused` (a damaged staging store). A restart's
    /// restatement opens and closes at one instant; those are left out. Seed 6325's
    /// situation is a window with a crash: on the trace it failed with, server 1
    /// installed index 129 at 5.7711 s and was crashed at 5.8120 s, inside the copy.
    #[must_use]
    pub fn adoption_windows(&self) -> Vec<AdoptionWindow> {
        let mut open: BTreeMap<u64, usize> = BTreeMap::new();
        let mut windows: Vec<AdoptionWindow> = Vec::new();
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftSnapshot {
                    server,
                    taken: false,
                    ..
                } if !open.contains_key(server) => {
                    open.insert(*server, windows.len());
                    windows.push(AdoptionWindow {
                        server: *server,
                        installed: record.at,
                        closed: None,
                        crashed: None,
                    });
                }
                TraceEvent::RaftAdopted { server }
                | TraceEvent::RaftRecovered { server, .. }
                | TraceEvent::RaftRefused { server, .. } => {
                    if let Some(window) = open.remove(server) {
                        windows[window].closed = Some(record.at);
                    }
                }
                TraceEvent::NodeCrashed { node } => {
                    if let Some(&window) = open.get(&u64::from(node.get())) {
                        windows[window].crashed.get_or_insert(record.at);
                    }
                }
                _ => {}
            }
        }
        windows.retain(|w| w.crashed.is_some() || w.closed != Some(w.installed));
        windows
    }

    /// Seed 687's situation: every restart of a server that was refused because its
    /// engine opened and lost state ([`LOST_STATE`]), with no install replacing that
    /// store in between — no installed `RaftSnapshot`, `RaftAdopted` or
    /// `RaftRecovered` on it — as (server, refused, restarted). A refusal for a
    /// store damaged before the engine opened is not counted: that engine never
    /// ran, so there is nothing it could have flushed over the loss. On the trace
    /// seed 687 failed with this is (3, 7.9209 s, 13.5320 s), and the restart
    /// opened clean on the laundered store.
    #[must_use]
    pub fn restarts_after_lost_state_refusal(&self) -> Vec<(u64, Instant, Instant)> {
        let mut refused: BTreeMap<u64, Instant> = BTreeMap::new();
        let mut restarts = Vec::new();
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftRefused { server, reason } if reason.starts_with(LOST_STATE) => {
                    refused.entry(*server).or_insert(record.at);
                }
                TraceEvent::RaftSnapshot {
                    server,
                    taken: false,
                    ..
                }
                | TraceEvent::RaftAdopted { server }
                | TraceEvent::RaftRecovered { server, .. } => {
                    refused.remove(server);
                }
                TraceEvent::NodeRestarted { node } => {
                    let server = u64::from(node.get());
                    if let Some(&at) = refused.get(&server) {
                        restarts.push((server, at, record.at));
                    }
                }
                _ => {}
            }
        }
        restarts
    }

    /// Every message one server sent another, decoded, with the index of its
    /// record: what the stream predicates read.
    fn raft_messages(&self) -> Vec<SentMessage> {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| match &record.event {
                TraceEvent::MessageSent {
                    id,
                    from,
                    to,
                    payload,
                } => {
                    let (from, to) = (server_of(*from)?, server_of(*to)?);
                    let frame = Frame::decode(payload.clone()).ok()?;
                    Some(SentMessage {
                        id: *id,
                        record: index,
                        at: record.at,
                        from,
                        to,
                        message: frame.message,
                    })
                }
                _ => None,
            })
            .collect()
    }

    /// Every snapshot a server took: its `RaftSnapshot` with `taken` set, paired
    /// with the checkpoint its own take wrote on its node — the last
    /// `CheckpointWritten` of that node still unclaimed. A checkpoint is discarded
    /// unclaimed by its node's crash and by a `RaftTruncate` on its server, so a
    /// restart's or an install's restatement of the record's snapshot, which
    /// writes no checkpoint and follows its truncation (`retook_at_one_index`
    /// reads the same marker), can never claim the checkpoint of a take that did
    /// not finish. Such a restatement is not a take.
    ///
    /// The pairing does **not** require the two records to carry the same instant.
    /// It did until D-060, whose checkpoint format record
    /// (`format::write_checkpoint_record` in `snapshot::take_numbered`) put an
    /// awaited write between the engine's checkpoint and the `RaftSnapshot` traced
    /// once the take returns: the take's two records now sit two to six
    /// milliseconds apart, and the instant-equal pairing found nothing on any
    /// seed. The fold's time assumption is durability, not one instant — a
    /// checkpoint is claimed by the next take of its own node, and a crash or a
    /// truncation between the two discards it (D-047's table).
    #[must_use]
    pub fn snapshot_takes(&self) -> Vec<SnapshotTake> {
        let mut written: BTreeMap<u64, (Instant, PathBuf)> = BTreeMap::new();
        let mut takes = Vec::new();
        for (index, record) in self.records.iter().enumerate() {
            match &record.event {
                TraceEvent::CheckpointWritten { dir, .. } => {
                    if let Some(node) = record.node {
                        written.insert(u64::from(node.get()), (record.at, dir.clone()));
                    }
                }
                TraceEvent::RaftTruncate { server, .. } => {
                    written.remove(server);
                }
                TraceEvent::RaftSnapshot {
                    server,
                    last_index,
                    taken: true,
                    ..
                } => {
                    if let Some((at, dir)) = written.remove(server) {
                        takes.push(SnapshotTake {
                            server: *server,
                            index: *last_index,
                            dir,
                            at,
                            record: index,
                        });
                    }
                }
                TraceEvent::NodeCrashed { node } => {
                    written.remove(&u64::from(node.get()));
                }
                _ => {}
            }
        }
        takes
    }

    /// Every take at an index its server had already taken at, that landed under a
    /// live stream of a snapshot at that index to some follower. Live means the
    /// stream showed itself on both sides of the take: before it, the stream's
    /// opening to that follower (`RaftSnapshotStreams`) or a chunk at the index
    /// sent to it; after it, a chunk at the index sent to it — with no other stream
    /// opened to that follower in between, and no `Installed` answer from the
    /// follower at the index in between. A tree that traces no openings (before
    /// D-043) is read by its chunks alone. Whether the stream survived is
    /// `installed_after`: a re-take the follower still installs after was harmless
    /// to it, and one it never installs after is the scrambled stream of D-043. The
    /// re-take with no stream under it, the common and harmless kind, is not here.
    #[must_use]
    pub fn retakes_under_streams(&self) -> Vec<RetakeUnderStream> {
        let messages = self.raft_messages();
        let openings: Vec<(usize, Instant, u64, u64)> = self
            .records
            .iter()
            .enumerate()
            .filter_map(|(index, r)| match &r.event {
                TraceEvent::RaftSnapshotStreams { server, to, .. } => {
                    Some((index, r.at, *server, *to))
                }
                _ => None,
            })
            .collect();
        let takes = self.snapshot_takes();
        let mut retakes = Vec::new();
        for (n, take) in takes.iter().enumerate() {
            let earlier: Vec<&SnapshotTake> = takes[..n]
                .iter()
                .filter(|t| t.server == take.server && t.index == take.index)
                .collect();
            if earlier.is_empty() {
                continue;
            }
            let same_dir = earlier.iter().any(|t| t.dir == take.dir);
            let chunk = |m: &SentMessage, follower: u64| {
                m.from == take.server
                    && m.to == follower
                    && matches!(m.message, Message::InstallSnapshot { last_index, .. }
                        if last_index == take.index)
            };
            let installed = |m: &SentMessage, follower: u64| {
                m.from == follower
                    && m.to == take.server
                    && matches!(m.message, Message::InstallSnapshotResponse {
                        last_index,
                        status: message::SnapshotStatus::Installed,
                        ..
                    } if last_index == take.index)
            };
            for follower in (1..=SERVERS).filter(|&f| f != take.server) {
                let Some(after) = messages
                    .iter()
                    .find(|m| m.record > take.record && chunk(m, follower))
                else {
                    continue;
                };
                // The stream's last sign of life before the take, as (record, time).
                let opened = openings
                    .iter()
                    .rev()
                    .find(|&&(record, _, server, to)| {
                        record < take.record && server == take.server && to == follower
                    })
                    .map(|&(record, at, ..)| (record, at));
                let sent = messages
                    .iter()
                    .rev()
                    .find(|m| m.record < take.record && chunk(m, follower))
                    .map(|m| (m.record, m.at));
                let Some((since, before)) = opened.max(sent) else {
                    continue;
                };
                let reopened = openings.iter().any(|&(_, at, server, to)| {
                    server == take.server && to == follower && before < at && at <= after.at
                });
                let installed_between = messages
                    .iter()
                    .any(|m| m.record > since && m.record < after.record && installed(m, follower));
                if reopened || installed_between {
                    continue;
                }
                retakes.push(RetakeUnderStream {
                    leader: take.server,
                    follower,
                    index: take.index,
                    same_dir,
                    before,
                    retook: take.at,
                    after: after.at,
                    installed_after: messages
                        .iter()
                        .any(|m| m.record > take.record && installed(m, follower)),
                });
            }
        }
        retakes
    }

    /// The leader in force at the last heal, as (server, term): the last to be
    /// elected at or before it that had not stepped down or crashed by then. Who
    /// led is a question of what the servers had decided by the heal, so an
    /// election or a step-down is read by its decision time (D-047).
    #[must_use]
    pub fn leader_at_last_heal(&self) -> Option<(u64, u64)> {
        let mut leader = None;
        // D-047: decided by the heal, folded in decision order; stable, so
        // ties keep record order. One server's leadership records keep their record
        // order under decision time, but two servers' need not: a leader elected
        // first can be traced after its successor, whose persist was shorter, and a
        // fold in record order would then end on the superseded one. A crash is
        // recorded as it happens.
        let mut decided: Vec<&TraceRecord> = self
            .records
            .iter()
            .filter(|r| r.decided <= self.last_heal)
            .collect();
        decided.sort_by_key(|record| record.decided);
        for record in decided {
            match &record.event {
                TraceEvent::RaftLeader { server, term, .. } => leader = Some((*server, *term)),
                TraceEvent::RaftTerm { server, role, .. }
                    if *role != "leader" && leader.is_some_and(|(s, _)| s == *server) =>
                {
                    leader = None;
                }
                TraceEvent::NodeCrashed { node }
                    if leader.is_some_and(|(s, _)| s == u64::from(node.get())) =>
                {
                    leader = None;
                }
                _ => {}
            }
        }
        leader
    }

    /// The followers the leader in force at the last heal could never count after
    /// it: each answered that leader's AppendEntries in its term at least once
    /// after the heal, never with success, and never answered a stream of it with
    /// `Installed`. Seed 5909's wedge had both followers here — on the trace it
    /// failed with, server 2 behind a scrambled stream and server 3 queued behind
    /// that stream — and a leader needs only one countable follower to commit.
    #[must_use]
    pub fn uncounted_after_heal(&self) -> BTreeSet<u64> {
        let Some((leader, term)) = self.leader_at_last_heal() else {
            return BTreeSet::new();
        };
        let mut answered: BTreeSet<u64> = BTreeSet::new();
        let mut counted: BTreeSet<u64> = BTreeSet::new();
        for m in self.raft_messages() {
            if m.at < self.last_heal || m.to != leader || m.message.term() != term {
                continue;
            }
            match m.message {
                Message::AppendEntriesResponse { success, .. } => {
                    answered.insert(m.from);
                    if success {
                        counted.insert(m.from);
                    }
                }
                Message::InstallSnapshotResponse {
                    status: message::SnapshotStatus::Installed,
                    ..
                } => {
                    counted.insert(m.from);
                }
                _ => {}
            }
        }
        answered.difference(&counted).copied().collect()
    }

    /// Every re-seed episode a leader ran (D-049): from the first rejection
    /// stamped store incarnation 0 — a refused server's (RAFT.md §3) — that a
    /// follower answered a leader with in its term, to that follower's `Installed`
    /// answer, or to the end of the leader's tenure when the install did not come
    /// first: its step-down, its crash, or its election in a later term. A
    /// completed episode is followed on through the adoption, to the follower's
    /// first AppendEntries response of the term from the store the install built,
    /// or to the tenure's end if that comes first. Messages are read in the order
    /// they were delivered to the leader, at their delivery.
    ///
    /// Re-seed progress is what the leader's code counts, reconstructed from the
    /// trace ([`StreamProgress`]): an acknowledgement of the stream the leader has
    /// open to the follower that takes it past the furthest point any
    /// acknowledgement had taken that stream, and the `Installed` answer; a stream
    /// opened afresh (`RaftSnapshotStreams`) starts its own count, and a stream the
    /// receiver asked to start over a third time is given up. A delivery's time
    /// stands for the moment the leader's core sees it: a rejection is stepped as
    /// it arrives, and an acknowledgement is taken at the leader's next tick, the
    /// tick whose check can read it, so attributing both to the window their
    /// delivery falls in is what the check does, up to the time the message waits
    /// to be processed.
    ///
    /// Each measure is scored against three ways of counting the refused follower
    /// for check quorum, as if the leader's other follower had been away for the
    /// whole of it: the leader as built, which counts its rejections; the correct
    /// leader, which counts a rejection only in a window with re-seed progress; and
    /// a leader counting nothing from it. For each, the share of
    /// [`EPISODE_PHASES`] evenly spaced placements of the leader's check-quorum
    /// windows in which some window lying wholly inside the stretch would have
    /// found nothing to count — the chance the leader would have stepped down in
    /// it, since where its windows fall is its own ticks' business. Windows are the
    /// minimum election timeout by the leader's clock. No placement counts the
    /// follower's answers from its new store, since the stretch through adoption
    /// ends at the first. Beside those, the stretches the three differ on, in
    /// windows: the episode's length, the wait for the first progress, the longest
    /// gap between two progress marks; and one that is the run's rather than a
    /// rule's, the longest stretch in which no other server answered the leader
    /// from a store, the refused follower being its only contact.
    #[must_use]
    pub fn reseed_episodes(&self) -> Vec<ReseedEpisode> {
        struct Open {
            leader: u64,
            follower: u64,
            term: u64,
            start: Instant,
            rejections: Vec<Instant>,
            progress: Vec<Instant>,
            last_other: Instant,
            only_contact: Duration,
            installed: Option<Instant>,
        }
        let rate = |leader: u64| -> f64 {
            let ppm = self.schedule.drifts[usize::try_from(leader - 1).expect("a server")];
            1.0 + ppm as f64 / 1_000_000.0
        };
        let score = |episode: &Open, end: Instant, completed: bool| -> ReseedEpisode {
            let leader = episode.leader;
            let windows = |d: Duration| d.as_secs_f64() * rate(leader) / ELECTION_MIN.as_secs_f64();
            let window = ELECTION_MIN.div_f64(rate(leader));
            let within = |at: &[Instant], from: Instant, to: Instant| {
                let first = at.partition_point(|&t| t < from);
                at.get(first).is_some_and(|&t| t < to)
            };
            let deposed = |until: Instant, counts: &dyn Fn(bool, bool) -> bool| -> f64 {
                let mut deposed = 0u32;
                for phase in 0..EPISODE_PHASES {
                    let offset = window.mul_f64(f64::from(phase) / f64::from(EPISODE_PHASES));
                    let mut check = episode.start + offset + window;
                    while check <= until {
                        let from = check - window;
                        let rejected = within(&episode.rejections, from, check);
                        let progressed = within(&episode.progress, from, check);
                        if !counts(rejected, progressed) {
                            deposed += 1;
                            break;
                        }
                        check += window;
                    }
                }
                f64::from(deposed) / f64::from(EPISODE_PHASES)
            };
            let installed = episode.installed.unwrap_or(end);
            let stream = |counts: &dyn Fn(bool, bool) -> bool| deposed(installed, counts);
            let through = |counts: &dyn Fn(bool, bool) -> bool| deposed(end, counts);
            let as_built = |rejected: bool, _: bool| rejected;
            let correct = |rejected: bool, progressed: bool| rejected && progressed;
            let nothing = |_: bool, _: bool| false;
            let before_install: Vec<Instant> = episode
                .progress
                .iter()
                .copied()
                .filter(|&t| t <= installed)
                .collect();
            ReseedEpisode {
                leader,
                term: episode.term,
                follower: episode.follower,
                start: episode.start,
                end: installed,
                completed: episode.installed.is_some(),
                answered_from_store: completed,
                through_adoption_end: end,
                length_windows: windows(installed.duration_since(episode.start)),
                adoption_windows: windows(end.duration_since(installed)),
                before_stream_windows: windows(
                    before_install
                        .first()
                        .copied()
                        .unwrap_or(installed)
                        .duration_since(episode.start),
                ),
                progress_gap_windows: windows(
                    before_install
                        .windows(2)
                        .map(|pair| pair[1].duration_since(pair[0]))
                        .max()
                        .unwrap_or(Duration::ZERO),
                ),
                only_contact_windows: windows(
                    episode
                        .only_contact
                        .max(installed.duration_since(episode.last_other)),
                ),
                deposed_as_built: stream(&as_built),
                deposed_correct: stream(&correct),
                deposed_counting_nothing: stream(&nothing),
                deposed_as_built_through_adoption: through(&as_built),
                deposed_correct_through_adoption: through(&correct),
                deposed_counting_nothing_through_adoption: through(&nothing),
            }
        };
        let mut progress = StreamProgress::default();
        let mut tenures: BTreeMap<u64, u64> = BTreeMap::new();
        let mut open: BTreeMap<(u64, u64), Open> = BTreeMap::new();
        let mut sent: BTreeMap<ananke_env::MessageId, (u64, u64, Message)> = BTreeMap::new();
        let mut episodes = Vec::new();
        for record in &self.records {
            let at = record.at;
            let ended = match &record.event {
                TraceEvent::RaftLeader { server, term, .. } => {
                    tenures.insert(*server, *term).map(|_| *server)
                }
                TraceEvent::RaftTerm { server, role, .. } if *role != "leader" => {
                    tenures.remove(server).map(|_| *server)
                }
                TraceEvent::NodeCrashed { node } => {
                    let server = u64::from(node.get());
                    progress.crashed(server);
                    tenures.remove(&server).map(|_| server)
                }
                TraceEvent::RaftSnapshotStreams { server, to, .. } => {
                    progress.opened(*server, *to);
                    None
                }
                TraceEvent::MessageSent {
                    id,
                    from,
                    to,
                    payload,
                } => {
                    if let (Some(from), Some(to), Ok(frame)) = (
                        server_of(*from),
                        server_of(*to),
                        Frame::decode(payload.clone()),
                    ) {
                        progress.sent(from, to, &frame.message);
                        sent.insert(*id, (from, to, frame.message));
                    }
                    None
                }
                _ => None,
            };
            if let Some(leader) = ended {
                let keys: Vec<(u64, u64)> =
                    open.keys().filter(|(l, _)| *l == leader).copied().collect();
                for key in keys {
                    let episode = open.remove(&key).expect("an open episode");
                    episodes.push(score(&episode, at, false));
                }
                continue;
            }
            let TraceEvent::MessageDelivered { id, .. } = &record.event else {
                continue;
            };
            let Some((from, to, message)) = sent.get(id) else {
                continue;
            };
            let (from, to) = (*from, *to);
            if progress.delivered(from, to, message)
                && let Some(episode) = open.get_mut(&(to, from))
            {
                episode.progress.push(at);
                if matches!(
                    message,
                    Message::InstallSnapshotResponse {
                        status: message::SnapshotStatus::Installed,
                        ..
                    }
                ) && episode.installed.is_none()
                {
                    episode.installed = Some(at);
                }
            }
            let Some(&term) = tenures.get(&to) else {
                continue;
            };
            let Message::AppendEntriesResponse {
                term: answered,
                success,
                incarnation,
                ..
            } = message
            else {
                continue;
            };
            if *answered != term {
                continue;
            }
            if !*success && *incarnation == 0 {
                open.entry((to, from))
                    .or_insert(Open {
                        leader: to,
                        follower: from,
                        term,
                        start: at,
                        rejections: Vec::new(),
                        progress: Vec::new(),
                        last_other: at,
                        only_contact: Duration::ZERO,
                        installed: None,
                    })
                    .rejections
                    .push(at);
                continue;
            }
            if let Some(episode) = open.get(&(to, from))
                && episode.installed.is_some()
            {
                let episode = open.remove(&(to, from)).expect("an open episode");
                episodes.push(score(&episode, at, true));
                continue;
            }
            for ((leader, follower), episode) in &mut open {
                if *leader == to && *follower != from && episode.installed.is_none() {
                    episode.only_contact = episode
                        .only_contact
                        .max(at.duration_since(episode.last_other));
                    episode.last_other = at;
                }
            }
        }
        episodes
    }

    /// A leader's stale progress for a refused `follower` (D-042's
    /// hazard): the first refusal of the follower after which the leader it last
    /// answered with success, in that answer's term, sent it at least one
    /// AppendEntries and every one at or above the match index that answer
    /// acknowledged, while the follower rejected at least one and accepted none,
    /// the leader never reset its progress and the follower was never re-seeded.
    /// The leader as built (`IgnoreIncarnation`) leaves exactly that; the correct
    /// leader resets at the follower's first answer after the refusal.
    #[must_use]
    pub fn stale_progress(&self, follower: u64) -> Option<StaleProgress> {
        let messages = self.raft_messages();
        for (index, record) in self.records.iter().enumerate() {
            if !matches!(&record.event, TraceEvent::RaftRefused { server, .. } if *server == follower)
            {
                continue;
            }
            let Some((leader, term, matched)) = messages
                .iter()
                .rev()
                .filter(|m| m.record < index && m.from == follower)
                .find_map(|m| match m.message {
                    Message::AppendEntriesResponse {
                        term,
                        success: true,
                        match_index,
                        ..
                    } => Some((m.to, term, match_index)),
                    _ => None,
                })
            else {
                continue;
            };
            let forgotten = self.records[index..].iter().any(|r| match &r.event {
                TraceEvent::RaftProgressReset {
                    server,
                    follower: f,
                    ..
                } => *server == leader && *f == follower,
                TraceEvent::RaftReseeded { server, .. } => *server == follower,
                _ => false,
            });
            if forgotten {
                continue;
            }
            let (mut probes, mut rejections, mut below, mut accepted) = (0, 0, false, false);
            for m in messages.iter().filter(|m| m.record > index) {
                match m.message {
                    Message::AppendEntries {
                        term: t,
                        prev_index,
                        ..
                    } if m.from == leader && m.to == follower && t == term => {
                        probes += 1;
                        below |= prev_index < matched;
                    }
                    Message::AppendEntriesResponse {
                        term: t, success, ..
                    } if m.from == follower && m.to == leader && t == term => {
                        if success {
                            accepted = true;
                        } else {
                            rejections += 1;
                        }
                    }
                    _ => {}
                }
            }
            if probes > 0 && rejections > 0 && !below && !accepted {
                return Some(StaleProgress {
                    leader,
                    term,
                    matched,
                    refused: record.at,
                    probes,
                    rejections,
                });
            }
        }
        None
    }

    /// D-042's fix at work on `follower`: its first refusal, then the
    /// first progress reset of it by a leader after that, then its first re-seed
    /// after the reset, as their times.
    #[must_use]
    pub fn refusal_reset_reseed(&self, follower: u64) -> Option<(Instant, Instant, Instant)> {
        let at = |from: Instant, f: &dyn Fn(&TraceEvent) -> bool| {
            self.records
                .iter()
                .find(|r| r.at >= from && f(&r.event))
                .map(|r| r.at)
        };
        let refused = at(
            Instant::ZERO,
            &|e| matches!(e, TraceEvent::RaftRefused { server, .. } if *server == follower),
        )?;
        let reset = at(
            refused,
            &|e| matches!(e, TraceEvent::RaftProgressReset { follower: f, .. } if *f == follower),
        )?;
        let reseeded = at(
            reset,
            &|e| matches!(e, TraceEvent::RaftReseeded { server, .. } if *server == follower),
        )?;
        Some((refused, reset, reseeded))
    }

    /// The duplicate-file loop of a stream under one identity (D-043's
    /// Decision): how many deliveries to `follower`, after `since`, of a chunk at
    /// offset 0 from `leader` the follower answered — its next answer to the
    /// leader — with `More` naming a different file. That is a file the receiver
    /// had already assembled under the stream's identity, so it points the sender
    /// back at the file before, whose acknowledgement sends the same chunk again,
    /// and every `More` keeps the stream from timing out.
    #[must_use]
    pub fn duplicate_chunk_loop(&self, leader: u64, follower: u64, since: Instant) -> usize {
        let chunks: BTreeMap<ananke_env::MessageId, Bytes> = self
            .raft_messages()
            .into_iter()
            .filter_map(|m| match m.message {
                Message::InstallSnapshot {
                    file, offset: 0, ..
                } if m.from == leader && m.to == follower => Some((m.id, file)),
                _ => None,
            })
            .collect();
        let mut pending: Option<&Bytes> = None;
        let mut looped = 0;
        for record in self.records.iter().filter(|r| r.at > since) {
            match &record.event {
                TraceEvent::MessageDelivered { id, .. } if chunks.contains_key(id) => {
                    pending = chunks.get(id);
                }
                TraceEvent::MessageSent {
                    from, to, payload, ..
                } if server_of(*from) == Some(follower) && server_of(*to) == Some(leader) => {
                    if let Some(file) = pending
                        && let Ok(Frame {
                            message:
                                Message::InstallSnapshotResponse {
                                    file: answered,
                                    status: message::SnapshotStatus::More,
                                    ..
                                },
                            ..
                        }) = Frame::decode(payload.clone())
                        && answered != *file
                    {
                        looped += 1;
                    }
                    pending = None;
                }
                _ => {}
            }
        }
        looped
    }
}

/// A term rise that straddles the start of its server's isolation
/// ([`Report::isolation_term_straddles`]).
// D-047: every trace record carries its decision time and its durability time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TermStraddle {
    /// The isolated server.
    pub server: u64,
    /// When the isolation began.
    pub from: Instant,
    /// When it healed.
    pub until: Instant,
    /// The server's term on its term record before the rise.
    pub before: u64,
    /// The term it rose to.
    pub term: u64,
    /// The role the rise put it in: `follower` for a term adopted from a message,
    /// `candidate` for a campaign.
    pub role: &'static str,
    /// When the step that raised it was taken: before `from`.
    pub decided: Instant,
    /// When the rise was traced, once durable: in `[from, until]`.
    pub at: Instant,
    /// Messages from a server delivered to it in `(from, until]`.
    pub deliveries: usize,
    /// The messages from a server delivered to it at `decided`, as (sender, kind,
    /// term): what the step that raised the term can have taken.
    pub causes: Vec<(u64, &'static str, u64)>,
}

/// A change of an isolated server's term decided inside the isolation from a
/// message received before it ([`Report::isolation_received_straddles`]).
// PROPOSED(D-050): a term's record carries when the message its step took was
// received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedStraddle {
    /// The isolated server.
    pub server: u64,
    /// When the isolation began.
    pub from: Instant,
    /// When it healed.
    pub until: Instant,
    /// The server's term on its term record before the change.
    pub before: u64,
    /// The term it changed to.
    pub term: u64,
    /// The role the change put it in.
    pub role: &'static str,
    /// When the server received the message the step took: at or before `from`.
    pub received: Instant,
    /// When the step was taken: in `(from, until]`.
    pub decided: Instant,
    /// When the change was traced, once durable.
    pub at: Instant,
    /// Messages from a server delivered to it in `(from, until]`.
    pub deliveries: usize,
    /// The messages from a server delivered to it at `received`, as (sender, kind,
    /// term): what the step can have taken.
    pub causes: Vec<(u64, &'static str, u64)>,
}

/// An installed snapshot below the floor a server's snapshots had reached
/// ([`Report::floor_lowering_installs`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloorLowering {
    /// The server.
    pub server: u64,
    /// When it installed.
    pub at: Instant,
    /// The installed snapshot's last index: the exact floor from here.
    pub last_index: u64,
    /// The highest snapshot index it had before: the floor that only rose.
    pub floor: u64,
}

/// A restatement that replays applied indices between the exact floor and the
/// risen one ([`Report::recoveries_under_a_lost_floor`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LostFloorReplay {
    /// The server.
    pub server: u64,
    /// The restatement's time.
    pub at: Instant,
    /// The floor as the last install set it.
    pub exact: u64,
    /// The floor as it only rose.
    pub risen: u64,
    /// The first and last replayed index inside `(exact, risen]`.
    pub indices: (u64, u64),
}

/// A completed install and its adoption ([`Report::adoption_windows`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdoptionWindow {
    /// The server.
    pub server: u64,
    /// When the install completed.
    pub installed: Instant,
    /// When the window closed, if it did before the run ended.
    pub closed: Option<Instant>,
    /// The first crash of the server inside the window, if one landed there.
    pub crashed: Option<Instant>,
}

/// One message between servers, decoded ([`Report::raft_messages`]).
struct SentMessage {
    id: ananke_env::MessageId,
    record: usize,
    at: Instant,
    from: u64,
    to: u64,
    message: Message,
}

/// One snapshot taken ([`Report::snapshot_takes`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotTake {
    /// The server.
    pub server: u64,
    /// The snapshot's last index.
    pub index: u64,
    /// The checkpoint directory it wrote.
    pub dir: PathBuf,
    /// When.
    pub at: Instant,
    /// The index of its `RaftSnapshot` record in [`Report::records`].
    pub record: usize,
}

/// A re-take under a live stream ([`Report::retakes_under_streams`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetakeUnderStream {
    /// The server that took, and was streaming.
    pub leader: u64,
    /// The follower the stream fed.
    pub follower: u64,
    /// The index taken again.
    pub index: u64,
    /// Whether the take wrote a directory an earlier take at the index had
    /// written: the shared directory of `SharedSnapshotDir`, rewritten under the
    /// stream reading it.
    pub same_dir: bool,
    /// The stream's last sign of life before the take: its opening, or a chunk
    /// at the index sent to the follower.
    pub before: Instant,
    /// The take.
    pub retook: Instant,
    /// The stream's first chunk sent after it.
    pub after: Instant,
    /// Whether the follower ever answered `Installed` at the index after the take.
    pub installed_after: bool,
}

/// How many placements of a leader's check-quorum windows
/// [`Report::reseed_episodes`] tries on each episode.
pub const EPISODE_PHASES: u32 = 20;

/// The re-seed progress a leader's code counts (D-049), reconstructed from the
/// trace for [`Report::reseed_episodes`]: per leader and follower, the stream the
/// leader's `snapshot` task has open, the furthest point any acknowledgement took
/// it, and how many times its receiver asked to start over.
///
/// A stream opens at `RaftSnapshotStreams` and ends at its `Installed` answer, at
/// the third ask to start over, or with its leader's crash. An acknowledgement is
/// the stream's when it names the snapshot the leader's chunks to that follower
/// last named. Its position is the file it names and the offset in it, a file
/// counting as done once the offset reaches the size the leader's chunks gave it;
/// files order by name, as the checkpoint's directory listing orders them for the
/// leader's `Sender`. One thing the code does is left out: a stream given up
/// after eight resends with nothing acknowledged is not closed here, and an
/// acknowledgement arriving after that and before a new stream opens would be
/// counted — the receiver acknowledges only chunks, and none has been sent to it
/// for the four hundred milliseconds of resends.
#[derive(Debug, Default)]
pub struct StreamProgress {
    /// (leader, follower) → the stream open: the furthest position and the asks
    /// to start over.
    streams: BTreeMap<(u64, u64), ((Bytes, u64), u32)>,
    /// (leader, follower) → the snapshot the leader's last chunk to it named.
    identity: BTreeMap<(u64, u64), (u64, u64)>,
    /// (leader, follower, last index, last term, file) → its size.
    sizes: BTreeMap<(u64, u64, u64, u64, Bytes), u64>,
}

impl StreamProgress {
    /// `leader` opened a stream to `follower`: it starts from its first byte.
    pub fn opened(&mut self, leader: u64, follower: u64) {
        self.streams
            .insert((leader, follower), ((Bytes::new(), 0), 0));
    }

    /// `server` crashed: its streams are gone.
    pub fn crashed(&mut self, server: u64) {
        self.streams.retain(|(leader, _), _| *leader != server);
    }

    /// `from` sent `message` to `to`: a chunk names its snapshot and its file's size.
    pub fn sent(&mut self, from: u64, to: u64, message: &Message) {
        if let Message::InstallSnapshot {
            last_index,
            last_term,
            file,
            total,
            ..
        } = message
        {
            self.identity.insert((from, to), (*last_index, *last_term));
            self.sizes
                .insert((from, to, *last_index, *last_term, file.clone()), *total);
        }
    }

    /// `message` from `from` was delivered to `to`: whether it is re-seed progress
    /// the leader `to` counts.
    pub fn delivered(&mut self, from: u64, to: u64, message: &Message) -> bool {
        let Message::InstallSnapshotResponse {
            last_index,
            last_term,
            file,
            offset,
            status,
            ..
        } = message
        else {
            return false;
        };
        if self.identity.get(&(to, from)) != Some(&(*last_index, *last_term)) {
            return false;
        }
        let Some((furthest, restarts)) = self.streams.get_mut(&(to, from)) else {
            return false;
        };
        match status {
            message::SnapshotStatus::Installed => {
                self.streams.remove(&(to, from));
                true
            }
            message::SnapshotStatus::Restart => {
                *restarts += 1;
                if *restarts > 2 {
                    self.streams.remove(&(to, from));
                }
                false
            }
            message::SnapshotStatus::More => {
                let done = self
                    .sizes
                    .get(&(to, from, *last_index, *last_term, file.clone()))
                    .is_some_and(|&size| *offset >= size);
                let position = (file.clone(), if done { u64::MAX } else { *offset });
                if !file.is_empty() && position > *furthest {
                    *furthest = position;
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// One re-seed episode a leader ran ([`Report::reseed_episodes`], D-049). The
/// stretches are in check-quorum windows of the leader's clock; the shares are of
/// [`EPISODE_PHASES`] placements of those windows.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReseedEpisode {
    /// The leader.
    pub leader: u64,
    /// Its term.
    pub term: u64,
    /// The refused follower.
    pub follower: u64,
    /// The first refused rejection delivered to the leader in the tenure.
    pub start: Instant,
    /// The follower's `Installed` answer, or the tenure's end.
    pub end: Instant,
    /// Whether the install's answer ended it.
    pub completed: bool,
    /// Whether the follower then answered the leader from its new store in the
    /// tenure.
    pub answered_from_store: bool,
    /// That answer, or the tenure's end; `end` for an episode that did not complete.
    pub through_adoption_end: Instant,
    /// From the first refused rejection to `end`.
    pub length_windows: f64,
    /// From `end` to `through_adoption_end`: the verification's answer to the
    /// adoption's first answer.
    pub adoption_windows: f64,
    /// From the first refused rejection to the stream's first progress, or to
    /// `end` when none came.
    pub before_stream_windows: f64,
    /// The longest gap between two consecutive progress marks up to `end`.
    pub progress_gap_windows: f64,
    /// The longest stretch up to `end` in which no other server answered the
    /// leader from a store.
    pub only_contact_windows: f64,
    /// Up to `end`: the share of placements in which a leader counting the refused
    /// follower's rejections, as built, would have found a window with nothing to
    /// count.
    pub deposed_as_built: f64,
    /// The same for the correct leader, which counts a rejection only beside
    /// re-seed progress in its window.
    pub deposed_correct: f64,
    /// The same for a leader counting nothing from a refused follower.
    pub deposed_counting_nothing: f64,
    /// The three shares again, up to `through_adoption_end`.
    pub deposed_as_built_through_adoption: f64,
    /// See [`ReseedEpisode::deposed_as_built_through_adoption`].
    pub deposed_correct_through_adoption: f64,
    /// See [`ReseedEpisode::deposed_as_built_through_adoption`].
    pub deposed_counting_nothing_through_adoption: f64,
}

/// A leader's stale progress for a refused follower ([`Report::stale_progress`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StaleProgress {
    /// The leader.
    pub leader: u64,
    /// Its term.
    pub term: u64,
    /// The match index the follower last acknowledged to it before the refusal.
    pub matched: u64,
    /// The refusal.
    pub refused: Instant,
    /// The leader's AppendEntries to the follower after the refusal, all at or
    /// above `matched`.
    pub probes: usize,
    /// The follower's rejections of them.
    pub rejections: usize,
}

/// The simulator configuration for `seed` and `schedule`.
#[must_use]
pub fn config(seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.05;
    config.net.p_duplicate = 0.05;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(10);
    config.clock.max_skew = Duration::from_millis(50);
    config.clock.max_drift_ppm = 500;
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.02;
    config.fs.latency_min = Duration::from_micros(100);
    config.fs.latency_max = Duration::from_millis(2);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(SERVERS + CLIENTS).expect("small"),
        schedule.total(),
    );
    config
}

/// The simulator's configuration for `cluster`'s run of `seed`.
///
/// The raft sweep's drops, duplicates, delays, clock skew and disk latencies
/// whichever cluster runs; what differs is the bit rot, and [`Cluster::bitrot`]
/// says why.
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
#[must_use]
pub fn config_on(cluster: Cluster, seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = config(seed, schedule);
    config.fs.p_bitrot = cluster.bitrot();
    config
}

/// The server configuration for `id` under `variants`: the set of known bugs
/// this server carries, which a single [`Variant`](ananke_raft::core::Variant)
/// converts into (D-045).
// D-045: a variant is a set.
#[must_use]
pub fn node_config(id: u64, variants: impl Into<Variants>) -> NodeConfig {
    let variants = variants.into();
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    NodeConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=SERVERS)
            .map(|s| (ServerId(s), server_addr(s)))
            .collect(),
        initial_voters: (1..=SERVERS).map(ServerId).collect(),
        // The default batch size: a follower behind by up to `max_batch` entries
        // is caught up in one message, so under client-paced traffic a new
        // leader's own no-op rides with the older entries it re-sends and the
        // Figure 8 window never opens. [`Fault::FigureEight`] opens it
        // deliberately: a burst leaves a restarted leader re-sending a backlog of
        // more entries than one message carries, and the window is every
        // acknowledgement below its no-op (D-031, issue #22). D-026's sweep ran
        // one entry per message instead, so the window opened on ordinary
        // catch-ups and the batched paths went unexercised.
        //
        // A small snapshot threshold, so leaders compact routinely and a follower
        // partitioned away for under a second falls behind by more than it —
        // the clients write a couple of dozen entries a second — and lands in
        // the snapshot path on real schedules; a chunk small enough that an
        // install takes many chunks, so resumption under drops actually happens
        // (RAFT.md §1, stage E).
        raft: RaftConfig {
            variants,
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            drift_bound_ppm: DRIFT_BOUND_PPM,
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            snapshot_chunk: 4096,
            ..RaftConfig::default()
        },
        engine,
        inbox_capacity: 128,
    }
}

/// One node of the node cluster, configured as this scenario needs it: the raft
/// sweep's tick, drift bound and clocks over [`crate::ranges`]'s four ranges.
///
/// Two parameters are **not** the raft sweep's, and each is an absence this
/// scenario asserts rather than leaves to be found (CLAUDE.md):
///
/// - `snapshot_threshold` is far above what a run writes, where the one-group
///   server's is 12. The node's host counts a snapshot action a core asks for in
///   [`ananke_shard::server::Gaps`] and does nothing with it, because the `snapshot`
///   task is not wired to the host in this tree: a small threshold would produce a
///   stream of takes nobody serves and followers behind a prefix nobody streams. The
///   sweep asserts that no snapshot action was asked for.
/// - the disk does not rot ([`Cluster::bitrot`]), because a refusal stops the node.
///
/// Both are the same fact in two places: the node's install and refusal paths are
/// other slices' and are not here. The variants that break them keep their Phase 2
/// assertions on [`Cluster::OneGroup`] and are re-asserted on the node by the slice
/// that builds the path (PROPOSED D-082).
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
#[must_use]
pub fn node_server_config(
    id: u64,
    variants: impl Into<Variants>,
    node: NodeVariants,
) -> ServerConfig {
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    ServerConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=SERVERS)
            .map(|s| (ServerId(s), server_addr(s)))
            .collect(),
        ranges: crate::ranges::ranges(),
        initial_voters: (1..=SERVERS).map(ServerId).collect(),
        raft: RaftConfig {
            variants: variants.into(),
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            drift_bound_ppm: DRIFT_BOUND_PPM,
            snapshot_threshold: NODE_SNAPSHOT_THRESHOLD,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: crate::ranges::INBOX_BYTES,
        snapshot_cap: crate::ranges::SNAPSHOT_CAP,
        node,
    }
}

/// The leader in force: the server of the latest `RaftLeader` event, or server 1.
///
/// Read back over the trace's tail in windows rather than over a copy of the whole
/// trace (D-046): a fault round asks this of a trace that only grows, and
/// the answer is almost always within the last few hundred records. A window that
/// holds no `RaftLeader` doubles until the trace is exhausted, so the answer is the
/// whole trace's either way.
pub(crate) fn leader_now(sim: &Sim) -> u64 {
    leader_of_range(sim, SINGLE_GROUP)
}

/// The server leading the most of `cluster`'s ranges, ties to the lowest id.
///
/// What an arm means by "the leader" when it is about a *node* and not a range:
/// the lease trial cuts one node off and wants the one whose leases are worth
/// cutting off. On one group the cluster has one range and this is exactly
/// [`leader_now`].
// PROPOSED(D-082): "the leader" of a node of four ranges is the one leading most.
pub(crate) fn leader_of_the_cluster(sim: &Sim, cluster: Cluster) -> u64 {
    let mut leading: BTreeMap<u64, usize> = BTreeMap::new();
    for range in cluster.ranges() {
        *leading.entry(leader_of_range(sim, range)).or_default() += 1;
    }
    leading
        .into_iter()
        .max_by_key(|&(server, count)| (count, std::cmp::Reverse(server)))
        .map_or(1, |(server, _)| server)
}

/// The leader of `range` in force, read the same way.
///
/// On a node of four ranges "the leader" is a question about a range and not about
/// a cluster: node `a` leads one range while node `b` leads the one beside it, and
/// an arm that cut off "the leader" without saying of what would cut off whichever
/// range elected last. Which range each leader-relative arm aims at is its own
/// draw (§11, env 8), and this is where the draw is spent.
// PROPOSED(D-082): a leader-relative arm resolves its leader per range.
pub(crate) fn leader_of_range(sim: &Sim, range: u64) -> u64 {
    let len = sim.trace_len();
    let mut window = 256;
    loop {
        let from = len.saturating_sub(window);
        let found = sim
            .trace_from(from)
            .iter()
            .rev()
            .find_map(|r| match r.event {
                TraceEvent::RaftLeader {
                    server, range: of, ..
                } if of == range => Some(server),
                _ => None,
            });
        if let Some(server) = found {
            return server;
        }
        if from == 0 {
            return 1;
        }
        window *= 2;
    }
}

fn to_command(op: &ClientOp) -> Command {
    match op {
        ClientOp::Put { key, value } => Command::Put {
            key: key.clone(),
            value: value.clone(),
        },
        ClientOp::Get { key } => Command::Get { key: key.clone() },
        ClientOp::Delete { key } => Command::Delete { key: key.clone() },
        ClientOp::Cas { key, expect, value } => Command::Cas {
            key: key.clone(),
            expect: expect.clone(),
            value: value.clone(),
        },
    }
}

fn to_result(outcome: Outcome) -> ClientResult {
    match outcome {
        Outcome::Done => ClientResult::Done,
        Outcome::Swapped(swapped) => ClientResult::Swapped(swapped),
        Outcome::Value(value) => ClientResult::Value(value),
    }
}

/// One client: operations on random keys against the leader it last heard of,
/// following NotLeader hints, abandoning a write it hears nothing about. Client 1
/// reads more than it writes: it is the client the trial and the leader isolation
/// keep on the cut-off leader's side, where a lease read is what matters.
/// `servers` is how many server nodes it may try: the membership scenario runs
/// five, this sweep three.
pub(crate) async fn client<E: Environment>(env: E, n: u64, servers: u64, stats: SharedStats) {
    client_on(Cluster::OneGroup, env, n, servers, stats).await;
}

/// The same client against `cluster`: the same draws, the same deadlines and the
/// same rules about what may be retried, with its key's range on every message and
/// the leader it last heard of kept **per range**.
///
/// One leader for the cluster would be wrong on a node: node `a` leads one range
/// while node `b` leads the one beside it, and a client that remembered one would
/// take a `NotLeader` on three ranges in four. On one group the map holds one key
/// and every draw falls exactly where it fell before (PROPOSED D-082).
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
pub(crate) async fn client_on<E: Environment>(
    cluster: Cluster,
    env: E,
    n: u64,
    servers: u64,
    stats: SharedStats,
) {
    let Ok(sock) = env.net().bind(client_addr(n)).await else {
        return;
    };
    let key_range = cluster.key_range();
    let mut incarnation = 0u64;
    let mut process = n << 32 | incarnation;
    let mut seq = 0u64;
    // The leader it last heard of, per range.
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    // The server the last abandoned operation went to: not the first to try next.
    let mut avoid: Option<u64> = None;
    let mut known: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
    loop {
        let key = Bytes::from(format!("k{}", env.rng().below(cluster.keys())));
        let range = key_range(&key);
        let value = Bytes::from(format!("{n}.{incarnation}.{seq}"));
        let draw = env.rng().below(10);
        let op = match (n, draw) {
            (1, 0..=6) | (_, 4..=6) => ClientOp::Get { key: key.clone() },
            (1, 7) | (_, 0..=3) => ClientOp::Put {
                key: key.clone(),
                value,
            },
            (1, 8) | (_, 7) => ClientOp::Delete { key: key.clone() },
            _ => ClientOp::Cas {
                key: key.clone(),
                expect: known.get(&key).cloned().unwrap_or(None),
                value,
            },
        };
        env.trace(TraceEvent::ClientInvoke {
            client: process,
            seq,
            op: op.clone(),
        });
        let command = to_command(&op);
        let deadline = env.clock().now()
            + if op.is_write() {
                WRITE_TIMEOUT
            } else {
                OP_TIMEOUT
            };
        let mut target = leaders.get(&range).copied().unwrap_or_else(|| {
            let pick = 1 + env.rng().below(servers);
            if avoid == Some(pick) {
                pick % servers + 1
            } else {
                pick
            }
        });
        let mut outcome = None;
        loop {
            let request = Request {
                client: process,
                seq,
                command: command.clone(),
            };
            if sock
                .send(server_addr(target), cluster.encode(range, request))
                .await
                .is_err()
            {
                return;
            }
            let try_deadline = if op.is_write() {
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
                        if let Some(response) = cluster.decode(bytes)
                            && response.client == process
                            && response.seq == seq
                        {
                            got = Some(response.reply);
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
                    stats.lock().unwrap().redirected += 1;
                    if now >= deadline {
                        break;
                    }
                    match hint {
                        Some(l) => target = l.0,
                        None => {
                            env.clock().sleep(Duration::from_millis(20)).await;
                            target = target % servers + 1;
                        }
                    }
                }
                None => {
                    // A get can be asked again elsewhere; a write cannot.
                    if !op.is_write() && now < deadline {
                        target = target % servers + 1;
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
                    (ClientOp::Delete { .. }, Outcome::Done) => {
                        known.insert(key, None);
                    }
                    (ClientOp::Get { .. }, Outcome::Value(value)) => {
                        known.insert(key, value.clone());
                    }
                    (ClientOp::Cas { value, .. }, Outcome::Swapped(true)) => {
                        known.insert(key, Some(value.clone()));
                    }
                    _ => {}
                }
                env.trace(TraceEvent::ClientReturn {
                    client: process,
                    seq,
                    result: to_result(result),
                });
                stats.lock().unwrap().completed += 1;
            }
            None => {
                stats.lock().unwrap().abandoned += 1;
                leaders.remove(&range);
                avoid = Some(target);
                incarnation += 1;
                process = n << 32 | incarnation;
                known.clear();
            }
        }
        seq += 1;
        env.clock().sleep(OP_GAP).await;
    }
}

/// The Figure 8 driver's burst: `count` puts on [`BURST_KEY`] fired at server
/// `target` without awaiting replies, as the schedule's `n`th driver. Every
/// operation is invoked in the trace and abandoned; the checker closes each at
/// its entry's apply, with a result no client saw, or leaves it pending
/// (RAFT.md §4). The ones the doomed leader appends are the backlog the driver
/// needs; a reply, had anyone read one, would arrive only after the entry
/// applied, so never for the entries the crash cuts off.
async fn burst<E: Environment>(
    cluster: Cluster,
    env: E,
    n: u64,
    target: u64,
    range: u64,
    count: u64,
) {
    let Ok(sock) = env.net().bind(burst_addr(n)).await else {
        return;
    };
    let process = BURST | n;
    // One group's burst writes [`BURST_KEY`], as it always has. The node's writes a
    // key of the range its arm aims at, prefixed with that range's first key so
    // that the fixed map routes it there and it sorts inside the span
    // `RangeCreated` names (`node_range_of_key`). The backlog the driver needs is a
    // backlog of *that range's* log, and a burst on the wrong range would leave the
    // isolated follower's own range with nothing to catch up on.
    // PROPOSED(D-082): a driver aimed at a range writes a key of that range.
    let key = match cluster {
        Cluster::OneGroup => Bytes::from_static(BURST_KEY),
        Cluster::Node => Bytes::from(format!(
            "{}b",
            node_key_prefix(range - crate::ranges::FIRST_RANGE)
        )),
    };
    for seq in 0..count {
        let key = key.clone();
        let value = Bytes::from(format!("b{n}.{seq}"));
        env.trace(TraceEvent::ClientInvoke {
            client: process,
            seq,
            op: ClientOp::Put {
                key: key.clone(),
                value: value.clone(),
            },
        });
        let request = Request {
            client: process,
            seq,
            command: Command::Put { key, value },
        };
        if sock
            .send(server_addr(target), cluster.encode(range, request))
            .await
            .is_err()
        {
            return;
        }
    }
}

/// The re-take driver's filling puts (D-043): `count` puts of
/// [`SPREAD_VALUE_BYTES`] bytes each, every one on its own key, fired at server
/// `target` without awaiting replies as the schedule's `n`th driver. They are
/// there for their size, not their outcome: the state machine the two clients
/// build is two keys of a dozen bytes, whose checkpoint is a single chunk and
/// whose install is over in a round trip, and nothing can be aimed at a stream
/// that short. Each key is written once and never read, so the checker's
/// per-key search sees one put that always applies, as [`burst`]'s do.
pub(crate) async fn spread<E: Environment>(env: E, n: u64, target: u64, count: u64) {
    spread_on(Cluster::OneGroup, env, n, target, SINGLE_GROUP, count).await;
}

/// The same filling puts against `cluster`, on keys of `range`.
// PROPOSED(D-082): a driver aimed at a range writes a key of that range.
pub(crate) async fn spread_on<E: Environment>(
    cluster: Cluster,
    env: E,
    n: u64,
    target: u64,
    range: u64,
    count: u64,
) {
    let Ok(sock) = env.net().bind(spread_addr(n)).await else {
        return;
    };
    let process = SPREAD | n;
    let prefix = match cluster {
        Cluster::OneGroup => String::new(),
        Cluster::Node => node_key_prefix(range - crate::ranges::FIRST_RANGE),
    };
    for seq in 0..count {
        let key = Bytes::from(format!("{prefix}f{n}.{seq}"));
        let value = Bytes::from(vec![b'f'; SPREAD_VALUE_BYTES]);
        env.trace(TraceEvent::ClientInvoke {
            client: process,
            seq,
            op: ClientOp::Put {
                key: key.clone(),
                value: value.clone(),
            },
        });
        let request = Request {
            client: process,
            seq,
            command: Command::Put { key, value },
        };
        if sock
            .send(server_addr(target), cluster.encode(range, request))
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Runs the scenario for `seed` with the schedule drawn from it, under the set
/// of bugs `variants` — a single [`Variant`](ananke_raft::core::Variant) or a
/// [`Variants`] of several
/// (D-045).
// D-045: a variant is a set.
#[must_use]
pub fn run(seed: u64, variants: impl Into<Variants>) -> Report {
    run_with(seed, Schedule::draw(seed), variants)
}

/// Runs the scenario for `seed` with an explicit schedule.
// D-045: a variant is a set.
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule, variants: impl Into<Variants>) -> Report {
    run_on(
        Cluster::OneGroup,
        seed,
        schedule,
        variants,
        NodeVariants::correct(),
    )
}

/// Runs the scenario for `seed` on the node of SHARD.md §4, with four ranges on
/// every node, under the arms [`Schedule::draw_on_the_node`] keeps.
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
#[must_use]
pub fn run_on_the_node(seed: u64, variants: impl Into<Variants>, node: NodeVariants) -> Report {
    run_on(
        Cluster::Node,
        seed,
        Schedule::draw_on_the_node(seed),
        variants,
        node,
    )
}

/// Runs `schedule`'s arms against `cluster`.
///
/// One body, two clusters: which arm fires when, what it waits for and what it
/// heals is the same code whichever system is underneath, so the node's sweep and
/// the one-group sweep cannot drift apart (PROPOSED D-082).
// PROPOSED(D-082): the arms are shared and the node is a second cluster of them.
#[must_use]
pub fn run_on(
    cluster: Cluster,
    seed: u64,
    schedule: Schedule,
    variants: impl Into<Variants>,
    node_variants: NodeVariants,
) -> Report {
    let variants = variants.into();
    let mut sim = Sim::new(config_on(cluster, seed, &schedule));
    let servers: Vec<NodeId> = (0..SERVERS as usize)
        .map(|i| sim.add_node_with_clock(schedule.skews[i], schedule.drifts[i]))
        .collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    let stats: Vec<SharedStats> = (0..CLIENTS).map(|_| SharedStats::default()).collect();
    for id in 1..=SERVERS {
        cluster.spawn(&sim, id, variants, node_variants);
    }
    for (i, &node) in clients.iter().enumerate() {
        let env = sim.env(node);
        let inner = env.clone();
        let stats = stats[i].clone();
        env.spawn(
            "client",
            client_on(cluster, inner, i as u64 + 1, SERVERS, stats),
        );
    }
    let all_but = |server: u64, client: u64| -> (Vec<NodeId>, Vec<NodeId>) {
        let side: Vec<NodeId> = vec![servers[server as usize - 1], clients[client as usize - 1]];
        let rest: Vec<NodeId> = servers
            .iter()
            .chain(clients.iter())
            .chain(std::iter::once(&admin))
            .copied()
            .filter(|n| !side.contains(n))
            .collect();
        (side, rest)
    };
    let mut isolations = Vec::new();
    // Every leader-relative arm that fired, with the range it drew: what says the
    // draw reached the arm and not just the schedule (§11, env 8).
    // PROPOSED(D-082): a leader-relative arm resolves its leader per range.
    let mut aimed: Vec<(u64, u64, Instant)> = Vec::new();
    let mut watch = Watch::default();
    advance(&mut sim, schedule.warmup, &mut watch);
    let restart = |sim: &mut Sim, server: u64| {
        sim.restart(node_of_server(server));
        cluster.spawn(sim, server, variants, node_variants);
    };
    // The lease trials: the operator hands leadership to the slowest clock, which
    // leads for a while and is then cut off with client 1; twice, so the variant's
    // catch rate is not one window's noise.
    let slowest = schedule.slowest();
    let ranges = cluster.ranges();
    let mut trials_led_by_slowest = 0;
    let mut last_heal = sim.now();
    for (n, trial) in schedule.trials.iter().enumerate() {
        if watch.stopped.is_some() {
            break;
        }
        // The operator's sequence numbers and sockets run on from trial to trial,
        // whether or not a transfer was needed: one group asks for at most one a
        // trial and keeps the numbers it always had, 0 and 1 on sockets 1 and 2,
        // with [`TERM_RAISE_ADMIN`] still past them.
        let base = n * ranges.len();
        // Every range the cluster has is handed to the slowest clock, not one of
        // them: the trial is about a lease the slow server holds while it is cut
        // off, and a node that led one range of four would leave the reading
        // client asking the other three of whoever leads them. One group has one
        // range, so this is exactly the one transfer it always sent.
        // PROPOSED(D-082): the lease trial hands over every range the node holds.
        let mut asked = false;
        for (j, &range) in ranges.iter().enumerate() {
            let leader = leader_of_range(&sim, range);
            if leader == slowest {
                continue;
            }
            asked = true;
            let env = sim.env(admin);
            let inner = env.clone();
            let seq = u64::try_from(base + j).expect("small");
            let socket = seq + 1;
            env.spawn("admin", async move {
                let Ok(sock) = inner.net().bind(admin_addr(socket)).await else {
                    return;
                };
                let request = Request {
                    client: ADMIN,
                    seq,
                    command: Command::Transfer { to: slowest },
                };
                let _ = sock
                    .send(server_addr(leader), cluster.encode(range, request))
                    .await;
            });
        }
        if asked {
            advance(&mut sim, TRANSFER_WAIT, &mut watch);
        }
        advance(&mut sim, trial.settle, &mut watch);
        let leader = leader_of_the_cluster(&sim, cluster);
        trials_led_by_slowest += usize::from(leader == slowest);
        let (side, rest) = all_but(leader, 1);
        let from = sim.now();
        sim.partition(&side, &rest);
        advance(&mut sim, trial.isolate, &mut watch);
        sim.heal();
        isolations.push((leader, from, sim.now()));
        last_heal = sim.now();
        advance(&mut sim, TRIAL_GAP, &mut watch);
    }
    let mut bursts = 0u64;
    let mut fills = 0u64;
    let mut aimed_streams = 0usize;
    for (i, (fault, gap)) in schedule.faults.iter().zip(schedule.gaps.iter()).enumerate() {
        if watch.stopped.is_some() {
            break;
        }
        // Which range this arm aims at (§11, env 8): its own draw on the node, and
        // the only range there is on one group. Every `leader` below is the leader
        // *of this range*, so an arm that cuts off "the leader" cuts off the one it
        // drew and not whichever range elected last.
        let aimed_range = schedule.range_of(cluster, i);
        let leader_now = |sim: &Sim| leader_of_range(sim, aimed_range);
        match fault {
            Fault::Isolate {
                server,
                client,
                for_,
            } => {
                let (side, rest) = all_but(*server, *client);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *for_, &mut watch);
                sim.heal();
                isolations.push((*server, from, sim.now()));
            }
            Fault::IsolateLeader { for_ } => {
                let leader = leader_now(&sim);
                let (side, rest) = all_but(leader, 1);
                let from = sim.now();
                aimed.push((aimed_range, leader, from));
                sim.partition(&side, &rest);
                advance(&mut sim, *for_, &mut watch);
                sim.heal();
                isolations.push((leader, from, sim.now()));
            }
            Fault::OneWay { from, to, for_ } => {
                sim.block(node_of_server(*from), node_of_server(*to));
                advance(&mut sim, *for_, &mut watch);
                sim.heal();
            }
            Fault::Crash { server, down } => {
                sim.crash(node_of_server(*server));
                advance(&mut sim, *down, &mut watch);
                restart(&mut sim, *server);
            }
            Fault::CrashLeader { down } => {
                let leader = leader_now(&sim);
                aimed.push((aimed_range, leader, sim.now()));
                sim.crash(node_of_server(leader));
                advance(&mut sim, *down, &mut watch);
                restart(&mut sim, leader);
            }
            Fault::CrashInstalling {
                server,
                isolate,
                grace,
                down,
            } => {
                // The victim first falls behind the threshold and goes quiet past
                // the designation, so a stream follows the heal.
                let leader = leader_now(&sim);
                let victim = if *server == leader {
                    server % SERVERS + 1
                } else {
                    *server
                };
                let side = vec![servers[victim as usize - 1]];
                let rest: Vec<NodeId> = servers
                    .iter()
                    .chain(clients.iter())
                    .chain(std::iter::once(&admin))
                    .copied()
                    .filter(|n| *n != side[0])
                    .collect();
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *isolate, &mut watch);
                sim.heal();
                isolations.push((victim, from, sim.now()));
                if install_landing(&mut sim, &mut watch, victim) {
                    advance(&mut sim, *grace, &mut watch);
                    sim.crash(node_of_server(victim));
                    advance(&mut sim, *down, &mut watch);
                    restart(&mut sim, victim);
                }
            }
            Fault::CrashAdopting {
                server,
                isolate,
                down,
                crashes,
            } => {
                // D-041: the install crash's setup, then the crashes
                // aimed at the adoption the completed install starts, each at the
                // adoption's first durable change to the store directory.
                let leader = leader_now(&sim);
                let victim = if *server == leader {
                    server % SERVERS + 1
                } else {
                    *server
                };
                let side = vec![servers[victim as usize - 1]];
                let rest: Vec<NodeId> = servers
                    .iter()
                    .chain(clients.iter())
                    .chain(std::iter::once(&admin))
                    .copied()
                    .filter(|n| *n != side[0])
                    .collect();
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *isolate, &mut watch);
                sim.heal();
                isolations.push((victim, from, sim.now()));
                if install_completed(&mut sim, &mut watch, victim) {
                    for _ in 0..*crashes {
                        adoption_change(&mut sim, &mut watch, victim);
                        sim.crash(node_of_server(victim));
                        advance(&mut sim, *down, &mut watch);
                        restart(&mut sim, victim);
                    }
                }
            }
            Fault::CrashRefused {
                server,
                down,
                grace,
                crashes,
            } => {
                // D-044: each round crashes the victim inside a flush
                // it has begun and not finished, so a restart that is refused
                // replays more than a memtable and the engine as built flushes
                // over the loss; a victim already sitting refused is crashed
                // after the grace instead, which is the shape seed 687 hit.
                // Every crash is another roll of the disk's dice.
                let leader = leader_now(&sim);
                let victim = if *server == leader {
                    server % SERVERS + 1
                } else {
                    *server
                };
                let mut scanned = 0;
                let mut refused: BTreeSet<u64> = BTreeSet::new();
                for _ in 0..*crashes {
                    if watch.stopped.is_some() {
                        break;
                    }
                    refreshed_refused(&sim, &mut scanned, &mut refused);
                    if refused.contains(&victim) {
                        advance(&mut sim, *grace, &mut watch);
                    } else {
                        flush_in_flight(&mut sim, &mut watch, victim);
                    }
                    sim.crash(node_of_server(victim));
                    advance(&mut sim, *down, &mut watch);
                    restart(&mut sim, victim);
                }
            }
            Fault::RetakeUnderStream {
                follower,
                fill,
                settle,
                isolate,
                freeze,
                hold,
            } => {
                // D-043: a state machine worth streaming, then the
                // install crash's setup, then the freeze that holds the leader's
                // applied index at the index the running stream is reading, then
                // both followers designated at once. See the fault's own
                // documentation for why each step is there.
                let leader = leader_now(&sim);
                fills += 1;
                {
                    let env = sim.env(admin);
                    let inner = env.clone();
                    let (n, target, puts) = (fills, leader, *fill);
                    env.spawn("spread", async move {
                        spread_on(cluster, inner, n, target, aimed_range, puts).await;
                    });
                }
                advance(&mut sim, *settle, &mut watch);
                let leader = leader_now(&sim);
                let fed = if *follower == leader {
                    follower % SERVERS + 1
                } else {
                    *follower
                };
                let alone = |server: u64| -> (Vec<NodeId>, Vec<NodeId>) {
                    let side = vec![servers[server as usize - 1]];
                    let rest: Vec<NodeId> = servers
                        .iter()
                        .chain(clients.iter())
                        .chain(std::iter::once(&admin))
                        .copied()
                        .filter(|n| *n != side[0])
                        .collect();
                    (side, rest)
                };
                let (side, rest) = alone(fed);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *isolate, &mut watch);
                sim.heal();
                isolations.push((fed, from, sim.now()));
                if stream_opened(&mut sim, &mut watch, fed) {
                    aimed_streams += 1;
                    // The leader's other follower away: the fed one keeps the
                    // leader's quorum alive by answering heartbeats, and there
                    // is nobody left to count.
                    let leader = leader_now(&sim);
                    let other = (1..=SERVERS)
                        .find(|&s| s != leader && s != fed)
                        .unwrap_or_else(|| fed % SERVERS + 1);
                    let (side, rest) = alone(other);
                    let from = sim.now();
                    sim.partition(&side, &rest);
                    advance(&mut sim, *freeze, &mut watch);
                    sim.heal();
                    isolations.push((other, from, sim.now()));
                    advance(&mut sim, *hold, &mut watch);
                }
            }
            Fault::StaleSender {
                server,
                one_way,
                crash_after,
                down,
            } => {
                let leader = leader_now(&sim);
                aimed.push((aimed_range, leader, sim.now()));
                let stale = if *server == leader {
                    server % SERVERS + 1
                } else {
                    *server
                };
                for other in (1..=SERVERS).filter(|&s| s != stale) {
                    sim.block(node_of_server(other), node_of_server(stale));
                }
                advance(&mut sim, *crash_after, &mut watch);
                let leader = leader_now(&sim);
                let crashed = if leader == stale { None } else { Some(leader) };
                if let Some(leader) = crashed {
                    sim.crash(node_of_server(leader));
                }
                advance(&mut sim, *down, &mut watch);
                if let Some(leader) = crashed {
                    restart(&mut sim, leader);
                }
                let spent = *crash_after + *down;
                if *one_way > spent {
                    advance(&mut sim, *one_way - spent, &mut watch);
                }
                sim.heal();
            }
            Fault::FigureEight {
                follower,
                client,
                settle,
                burst: count,
                crash_after,
                down,
                steer,
            } => {
                let leader = leader_now(&sim);
                aimed.push((aimed_range, leader, sim.now()));
                let behind = if *follower == leader {
                    follower % SERVERS + 1
                } else {
                    *follower
                };
                let (side, rest) = all_but(behind, *client);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *settle, &mut watch);
                // The burst lands on whoever leads the majority side now; the
                // third server is the one kept mute after the crash.
                let leader = leader_now(&sim);
                let mute = (1..=SERVERS)
                    .find(|&s| s != leader && s != behind)
                    .expect("three servers");
                bursts += 1;
                let env = sim.env(admin);
                let inner = env.clone();
                let (n, target, puts) = (bursts, leader, *count);
                env.spawn("burst", async move {
                    burst(cluster, inner, n, target, aimed_range, puts).await;
                });
                advance(&mut sim, *crash_after, &mut watch);
                sim.crash(node_of_server(leader));
                sim.heal();
                isolations.push((behind, from, sim.now()));
                // The steering: with the mute server's sends blocked, it cannot
                // answer and the isolated follower, whose log is the shortest,
                // cannot be voted in; the restarted leader campaigns, the
                // isolated follower grants, and the backlog is re-sent to it in
                // batches.
                sim.block(node_of_server(mute), node_of_server(leader));
                sim.block(node_of_server(mute), node_of_server(behind));
                advance(&mut sim, *down, &mut watch);
                restart(&mut sim, leader);
                advance(&mut sim, *steer, &mut watch);
                sim.heal();
            }
            Fault::IsolateOnTermRaise {
                tries,
                isolate,
                quiet,
            } => {
                // PROPOSED(D-050): a transfer's campaign raises the third
                // server's term, and the isolation starts at the end of the slice
                // its message was delivered in, inside whatever persist the
                // server was running.
                for n in 0..*tries {
                    if watch.stopped.is_some() {
                        break;
                    }
                    let leader = leader_now(&sim);
                    let target = leader % SERVERS + 1;
                    let third = target % SERVERS + 1;
                    {
                        let env = sim.env(admin);
                        let inner = env.clone();
                        let seq = TERM_RAISE_ADMIN + n;
                        env.spawn("admin", async move {
                            let Ok(sock) = inner.net().bind(admin_addr(seq)).await else {
                                return;
                            };
                            let request = Request {
                                client: ADMIN,
                                seq,
                                command: Command::Transfer { to: target },
                            };
                            let _ = sock
                                .send(server_addr(leader), cluster.encode(aimed_range, request))
                                .await;
                        });
                    }
                    if term_raise_delivered(&mut sim, &mut watch, third) {
                        let side = vec![servers[third as usize - 1]];
                        let rest: Vec<NodeId> = servers
                            .iter()
                            .chain(clients.iter())
                            .chain(std::iter::once(&admin))
                            .copied()
                            .filter(|n| *n != side[0])
                            .collect();
                        let from = sim.now();
                        sim.partition(&side, &rest);
                        advance(&mut sim, *isolate, &mut watch);
                        sim.heal();
                        isolations.push((third, from, sim.now()));
                    }
                    advance(&mut sim, *quiet, &mut watch);
                }
            }
        }
        last_heal = sim.now();
        advance(&mut sim, *gap, &mut watch);
    }
    if watch.stopped.is_none() {
        advance(&mut sim, schedule.settle, &mut watch);
    }
    let records = sim.trace();
    let refused: Vec<(u64, String)> = records
        .iter()
        .filter_map(|r| match &r.event {
            TraceEvent::RaftRefused { server, reason } => Some((*server, reason.clone())),
            _ => None,
        })
        .collect();
    let history = History::from_trace(&records);
    let mut clients_total = ClientStats::default();
    for s in &stats {
        let s = s.lock().unwrap();
        clients_total.completed += s.completed;
        clients_total.abandoned += s.abandoned;
        clients_total.redirected += s.redirected;
    }
    Report {
        seed,
        variants,
        policy: sim.policy(),
        schedule,
        run: sim.run_header(),
        records,
        last_heal,
        isolations,
        trials_led_by_slowest,
        aimed_streams,
        refused,
        stopped: watch.stopped,
        history,
        clients: clients_total,
        ranges: cluster.ranges(),
        key_range: cluster.key_range(),
        cluster,
        aimed,
    }
}

/// What the sliced advance watches for.
struct Watch {
    slices: u32,
    stopped: Option<String>,
    /// The safety checks, one checker for the whole run with the state of each
    /// check kept across looks, so a look costs only the records since the last
    /// one (D-046).
    checker: invariants::Checker,
    /// How many trace records the checker has been fed.
    checked: usize,
}

impl Default for Watch {
    fn default() -> Self {
        Self {
            slices: 0,
            stopped: None,
            checker: invariants::Checker::new(SERVERS as usize),
            checked: 0,
        }
    }
}

/// Advances the run in small slices until `victim` receives the final chunk of a
/// snapshot stream, or [`INSTALL_WAIT_BUDGET`] runs out: the moment
/// [`Fault::CrashInstalling`] aims its crash at. The safety checks are skipped
/// inside the small slices — the next ordinary [`advance`] feeds them everything
/// since its last look — but the trace cap still stops a runaway. The watch reads
/// the records since its own last look rather than a copy of the whole trace, for
/// the reason the checker keeps its state (D-046).
fn install_landing(sim: &mut Sim, watch: &mut Watch, victim: u64) -> bool {
    if watch.stopped.is_some() {
        return false;
    }
    let step = Duration::from_millis(5);
    let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
    // Only deliveries from here on count: a done-chunk of some earlier stream
    // must not draw the crash. Sends are scanned a slice further back, so a
    // chunk sent just before the watch began still decodes when it lands.
    let mut scanned = sim.trace_len();
    for record in sim.trace_from(scanned.saturating_sub(2000)).iter().rev() {
        if let TraceEvent::MessageSent { id, payload, .. } = &record.event {
            payloads.entry(*id).or_insert_with(|| payload.clone());
        }
    }
    let mut waited = Duration::ZERO;
    while waited < INSTALL_WAIT_BUDGET {
        sim.run_for(step);
        waited += step;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return false;
        }
        let records = sim.trace_from(scanned);
        scanned += records.len();
        for record in &records {
            match &record.event {
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDelivered { id, to, .. } => {
                    if server_of(*to) == Some(victim)
                        && let Some(payload) = payloads.get(id)
                        && let Ok(frame) = Frame::decode(payload.clone())
                        && matches!(frame.message, Message::InstallSnapshot { done: true, .. })
                    {
                        return true;
                    }
                }
                _ => {}
            }
        }
    }
    false
}

/// Advances the run in small slices until `victim` traces a snapshot install
/// complete (`RaftSnapshot { taken: false }`, the event that precedes the
/// adoption at its next incarnation's start), or [`INSTALL_WAIT_BUDGET`] runs
/// out: the moment [`Fault::CrashAdopting`] measures its crash from. Only records
/// from here on count: the victim was not restarted since the heal, so no
/// restart's restatement can stand in for the completion. As with
/// [`install_landing`], the safety checks are skipped inside the small slices, the
/// watch reads only the records since its last look (D-046) and the trace
/// cap still stops a runaway. (D-041).
fn install_completed(sim: &mut Sim, watch: &mut Watch, victim: u64) -> bool {
    if watch.stopped.is_some() {
        return false;
    }
    let step = Duration::from_millis(5);
    let mut scanned = sim.trace_len();
    let mut waited = Duration::ZERO;
    while waited < INSTALL_WAIT_BUDGET {
        sim.run_for(step);
        waited += step;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return false;
        }
        let records = sim.trace_from(scanned);
        scanned += records.len();
        if records.iter().any(|r| {
            matches!(
                &r.event,
                TraceEvent::RaftSnapshot {
                    server,
                    taken: false,
                    ..
                } if *server == victim
            )
        }) {
            return true;
        }
    }
    false
}

/// Advances the run in small slices until a leader opens a snapshot stream to
/// `victim` ([`TraceEvent::RaftSnapshotStreams`]), or [`STREAM_WAIT_BUDGET`]
/// runs out: the moment [`Fault::RetakeUnderStream`] freezes the leader's
/// applied index at. Only openings from here on count — the victim was cut off
/// until the heal just before, so no earlier stream of the run can stand in for
/// this one. As with [`install_landing`], the safety folds are skipped inside
/// the small slices and the trace cap still stops a runaway. (D-043).
fn stream_opened(sim: &mut Sim, watch: &mut Watch, victim: u64) -> bool {
    if watch.stopped.is_some() {
        return false;
    }
    let step = Duration::from_millis(5);
    let mut scanned = sim.trace_len();
    let mut waited = Duration::ZERO;
    while waited < STREAM_WAIT_BUDGET {
        sim.run_for(step);
        waited += step;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return false;
        }
        let records = sim.trace_from(scanned);
        scanned += records.len();
        if records.iter().any(|r| {
            matches!(
                &r.event,
                TraceEvent::RaftSnapshotStreams { to, .. } if *to == victim
            )
        }) {
            return true;
        }
    }
    false
}

/// Advances the run in slices of [`TERM_RAISE_STEP`] until a message from a server
/// carrying a term above `victim`'s last traced term is delivered to `victim`, or
/// [`TERM_RAISE_WAIT_BUDGET`] runs out: the moment [`Fault::IsolateOnTermRaise`]
/// cuts it off at. A term the victim adopted but has not yet traced still reads as
/// above, which costs a round its aim and nothing else. As with [`install_landing`],
/// the safety folds are skipped inside the small slices, the watch reads only the
/// records since its last look (D-046) and the trace cap still stops a runaway.
// PROPOSED(D-050): a term's record carries when the message its step took was
// received.
fn term_raise_delivered(sim: &mut Sim, watch: &mut Watch, victim: u64) -> bool {
    if watch.stopped.is_some() {
        return false;
    }
    let mut term = sim
        .trace()
        .iter()
        .rev()
        .find_map(|r| match &r.event {
            TraceEvent::RaftTerm { server, term, .. } if *server == victim => Some(*term),
            _ => None,
        })
        .unwrap_or(0);
    let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
    let mut scanned = sim.trace_len();
    for record in sim.trace_from(scanned.saturating_sub(2000)).iter().rev() {
        if let TraceEvent::MessageSent { id, payload, .. } = &record.event {
            payloads.entry(*id).or_insert_with(|| payload.clone());
        }
    }
    let mut waited = Duration::ZERO;
    while waited < TERM_RAISE_WAIT_BUDGET {
        sim.run_for(TERM_RAISE_STEP);
        waited += TERM_RAISE_STEP;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return false;
        }
        let records = sim.trace_from(scanned);
        scanned += records.len();
        for record in &records {
            match &record.event {
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::RaftTerm {
                    server, term: now, ..
                } if *server == victim => term = *now,
                TraceEvent::MessageDelivered { id, from, to, .. }
                    if server_of(*to) == Some(victim) && server_of(*from).is_some() =>
                {
                    if let Some(payload) = payloads.get(id)
                        && let Ok(frame) = Frame::decode(payload.clone())
                        && frame.message.term() > term
                    {
                        return true;
                    }
                }
                _ => {}
            }
        }
    }
    false
}

/// Reads the trace from `scanned` on into `refused`, the servers sitting refused
/// for lost state: refused ([`TraceEvent::RaftRefused`]) with no restatement
/// since, however long ago, since a refused server comes back only through an
/// install (RAFT.md §3). What [`Fault::CrashRefused`] asks of its victim before
/// each round. (D-044).
fn refreshed_refused(sim: &Sim, scanned: &mut usize, refused: &mut BTreeSet<u64>) {
    let records = sim.trace_from(*scanned);
    *scanned += records.len();
    for record in &records {
        match &record.event {
            TraceEvent::RaftRefused { server, .. } => {
                refused.insert(*server);
            }
            TraceEvent::RaftRecovered { server, .. } => {
                refused.remove(server);
            }
            _ => {}
        }
    }
}

/// Advances the run in small slices until `victim` has rotated a memtable and
/// not yet flushed it, or [`FLUSH_WAIT_BUDGET`] runs out: the moment
/// [`Fault::CrashRefused`] crashes it. A flush writes and syncs its table, then
/// the manifest that lists it, then switches `CURRENT`; until that switch the
/// manifest in force is the older one, so a crash inside the flush leaves a log
/// tail of two memtables rather than one, and the open after it replays enough
/// to fill a memtable and rotate it. That rotation is what gives the engine as
/// built something to flush over a refusal with, which is the laundering
/// D-044 stops. The safety folds are skipped inside the small slices,
/// as in [`install_landing`], and the trace cap still stops a runaway.
/// (D-044).
fn flush_in_flight(sim: &mut Sim, watch: &mut Watch, victim: u64) {
    if watch.stopped.is_some() {
        return;
    }
    let node = node_of_server(victim);
    let step = Duration::from_millis(5);
    let mut scanned = sim.trace_len();
    let mut pending: BTreeSet<u64> = BTreeSet::new();
    let mut waited = Duration::ZERO;
    while waited < FLUSH_WAIT_BUDGET {
        sim.run_for(step);
        waited += step;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return;
        }
        let records = sim.trace_from(scanned);
        scanned += records.len();
        for record in &records {
            if record.node != Some(node) {
                continue;
            }
            match &record.event {
                TraceEvent::MemtableRotated { memtable, .. } => {
                    pending.insert(*memtable);
                }
                TraceEvent::MemtableFlushed { memtable, .. } => {
                    pending.remove(memtable);
                }
                _ => {}
            }
        }
        if !pending.is_empty() {
            return;
        }
    }
}

/// A name the store proper is made of, as the adoption sees it: `CURRENT`, a
/// table, a log segment or a manifest — not the marker, not a checkpoint or the
/// staging directory. (D-041).
fn store_name(name: &std::path::Path) -> bool {
    name.to_str().is_some_and(|n| {
        n == "CURRENT" || n.ends_with(".sst") || n.ends_with(".wal") || n.starts_with("MANIFEST-")
    })
}

/// Advances the run in small slices until the adoption running on `victim`
/// makes its first durable change to the server's store directory — every store
/// file that was durable when the watch began gone, or one that was not there
/// synced in, whichever comes first — or [`ADOPTION_WAIT_BUDGET`] runs out: the
/// moment [`Fault::CrashAdopting`] crashes it. A directory with no store file
/// durable at all, which only the as-built adoption leaves behind, is given
/// [`EMPTY_STORE_DELAY`] instead. The safety folds are skipped inside the small
/// slices, as in [`install_landing`], and the trace cap still stops a runaway.
/// (D-041).
fn adoption_change(sim: &mut Sim, watch: &mut Watch, victim: u64) {
    if watch.stopped.is_some() {
        return;
    }
    let node = node_of_server(victim);
    let dir = std::path::Path::new(DIR);
    let durable = |sim: &Sim| -> BTreeSet<PathBuf> {
        sim.durable_names(node, dir)
            .into_iter()
            .filter(|n| store_name(n))
            .collect()
    };
    let before = durable(sim);
    // PROPOSED(D-052): the namespace is read again only once it may have changed;
    // between two equal versions it is the namespace this watch last looked at,
    // which did not end the watch.
    let mut seen = sim.durable_version(node);
    let step = Duration::from_micros(250);
    let budget = if before.is_empty() {
        EMPTY_STORE_DELAY
    } else {
        ADOPTION_WAIT_BUDGET
    };
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return;
        }
        if before.is_empty() {
            continue;
        }
        let version = sim.durable_version(node);
        if version == seen {
            continue;
        }
        seen = version;
        let now = durable(sim);
        let old_gone = before.iter().all(|n| !now.contains(n));
        let new_synced = now.iter().any(|n| !before.contains(n));
        if old_gone || new_synced {
            return;
        }
    }
}

/// Runs the simulation for `duration` in slices of [`SLICE`], running the safety
/// checks every [`CHECK_EVERY`] slices and stopping at the first violation, or at
/// [`TRACE_CAP`] records. A buggy server can make the cluster do unbounded work, a
/// follower that truncates on every append re-fetching its tail forever, and a run
/// must still end with a verdict.
///
/// One [`invariants::Checker`] serves the whole run and is fed only the records
/// since the last look, so a look costs its own new events and a run costs its
/// trace once rather than once per look (D-046). What it reports is what
/// folding every check over the whole trace reports, in the same words:
/// `the_incremental_checker_agrees_with_the_fold_over_the_whole_trace` asserts it
/// over a hundred seeds, and [`Report::check`] folds from the first record again at
/// the end of every run.
fn advance(sim: &mut Sim, duration: Duration, watch: &mut Watch) {
    if watch.stopped.is_some() {
        return;
    }
    let mut left = duration;
    while left > Duration::ZERO {
        let step = left.min(SLICE);
        sim.run_for(step);
        left -= step;
        watch.slices += 1;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return;
        }
        if watch.slices.is_multiple_of(CHECK_EVERY) {
            let records = sim.trace_from(watch.checked);
            watch.checked += records.len();
            watch.checker.extend(crate::traced(&records));
            if let Err(violation) = watch.checker.verdict() {
                watch.stopped = Some(format!("{violation} (at {:?})", sim.now()));
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The checks' reasoning on records written by hand: each shape a removal can
    //! take, and the "every" in D-050's excuse.

    use super::*;
    use ananke_env::Decision;
    use ananke_raft::core::Variant;
    use ananke_raft::node::SINGLE_GROUP;

    fn ms(n: u64) -> Instant {
        Instant::from_nanos(n * 1_000_000)
    }

    fn record(at: Instant, decided: Instant, node: Option<u64>, event: TraceEvent) -> TraceRecord {
        TraceRecord {
            at,
            decided,
            node: node.map(|n| NodeId::new(u32::try_from(n).expect("small"))),
            event,
        }
    }

    fn term(server: u64, term: u64, role: &'static str, received: Option<Decision>) -> TraceEvent {
        TraceEvent::RaftTerm {
            server,
            range: SINGLE_GROUP,
            term,
            role,
            received,
        }
    }

    /// A stamp at `at`, taken the only way code outside `ananke-env` can: from a
    /// simulated node at that instant.
    fn stamp(at: Instant) -> Decision {
        let mut sim = Sim::new(SimConfig::new(0));
        let node = sim.add_node();
        sim.run_until(at);
        sim.env(node).decision()
    }

    /// A report around `records` and `isolations`, three servers whose clocks run
    /// true, so each server's timer bound is 400 ms.
    /// The cross-range hold fold, on records built by hand: it counts a wait across
    /// ranges, it counts nothing across a crash, and it counts nothing at all within
    /// one range.
    ///
    /// Both clauses were mutated and only one was caught by the sweeps. Dropping the
    /// crash windows moved the maximum this fold reports from 572.9 ms to 178.6 ms —
    /// the figure §12 sends to the owner — and dropping `of != range` moved the count
    /// from 5 317 waits to 54 808 and the median from 2.07 ms to 2.50 ms **with every
    /// test still green**, because both figures are printed and neither is asserted.
    /// A number that goes to the owner needs an oracle of its own, and a sweep cannot
    /// be it: this is that oracle.
    // PROPOSED(D-082): how long one range's applies hold the node's others.
    #[test]
    fn the_hold_fold_counts_a_wait_across_ranges_and_nothing_across_a_crash() {
        let commit = |server, range, index| TraceEvent::RaftCommit {
            server,
            range,
            term: 1,
            index,
        };
        let apply = |server, range, index| TraceEvent::RaftApply {
            server,
            range,
            index,
            entry_term: 1,
            hash: 0,
            key: None,
            effect: ApplyEffect::None,
        };
        let held = |records: Vec<TraceRecord>| {
            let (holds, dropped) = report(records, Vec::new()).cross_range_apply_holds_counted();
            (holds, dropped)
        };

        // Range 3's index 1 is committed at 10 ms; range 2 applies at 20 ms and again
        // at 40 ms; range 3 applies at 60 ms. Its wait ran through the job that ended
        // at 40 ms, so the hold is 40 − 20 = 20 ms.
        let across = vec![
            record(ms(10), ms(10), Some(1), commit(1, 3, 1)),
            record(ms(20), ms(20), Some(1), apply(1, 2, 1)),
            record(ms(40), ms(40), Some(1), apply(1, 2, 2)),
            record(ms(60), ms(60), Some(1), apply(1, 3, 1)),
        ];
        let (holds, dropped) = held(across.clone());
        assert_eq!(
            (holds.as_slice(), dropped),
            (&[Duration::from_millis(20)][..], 0),
            "a wait across ranges is one hold of the job it waited through"
        );

        // The same trace with the node crashing inside the window: not a hold at all,
        // and counted as dropped rather than passed over.
        let mut crashed = across.clone();
        crashed.insert(
            2,
            record(
                ms(25),
                ms(25),
                Some(1),
                TraceEvent::NodeCrashed {
                    node: NodeId::new(1),
                },
            ),
        );
        let (holds, dropped) = held(crashed);
        assert_eq!(
            (holds.as_slice(), dropped),
            (&[][..], 1),
            "a window the node spent crashed in is not one range holding another"
        );

        // One range applying three times in a row holds nothing: there is no other
        // range's job in the window, and a fold that counted this would report a
        // range's own apply latency as a cross-range hold.
        let alone = vec![
            record(ms(10), ms(10), Some(1), commit(1, 2, 3)),
            record(ms(20), ms(20), Some(1), apply(1, 2, 1)),
            record(ms(40), ms(40), Some(1), apply(1, 2, 2)),
            record(ms(60), ms(60), Some(1), apply(1, 2, 3)),
        ];
        let (holds, dropped) = held(alone);
        assert_eq!(
            (holds.as_slice(), dropped),
            (&[][..], 0),
            "one range's own applies are not a hold by another range"
        );

        // And a wait on another *node* is not this node's: the fold is per node.
        let elsewhere = vec![
            record(ms(10), ms(10), Some(2), commit(2, 3, 1)),
            record(ms(20), ms(20), Some(1), apply(1, 2, 1)),
            record(ms(40), ms(40), Some(1), apply(1, 2, 2)),
            record(ms(60), ms(60), Some(2), apply(2, 3, 1)),
        ];
        let (holds, dropped) = held(elsewhere);
        assert_eq!(
            (holds.as_slice(), dropped),
            (&[][..], 0),
            "a hold is one node's apply task holding its own ranges"
        );
    }

    fn report(records: Vec<TraceRecord>, isolations: Vec<(u64, Instant, Instant)>) -> Report {
        let sim = Sim::new(SimConfig::new(0));
        Report {
            seed: 0,
            variants: Variants::from(Variant::Correct),
            policy: Policy::Uniform,
            schedule: Schedule::term_raise_behind_a_step(0),
            records,
            run: sim.run_header(),
            last_heal: Instant::from_nanos(0),
            isolations,
            trials_led_by_slowest: 0,
            aimed_streams: 0,
            refused: Vec::new(),
            stopped: None,
            history: History::default(),
            clients: ClientStats::default(),
            ranges: (SINGLE_GROUP..SINGLE_GROUP + 4).collect(),
            key_range: range_of_key,
            cluster: Cluster::OneGroup,
            aimed: Vec::new(),
        }
    }

    fn refused(server: u64) -> TraceEvent {
        TraceEvent::RaftRefused {
            server,
            reason: "a table the manifest names is gone".to_owned(),
        }
    }

    fn replica_refused(server: u64, range: u64) -> TraceEvent {
        TraceEvent::RaftReplicaRefused { server, range }
    }

    fn vote(server: u64) -> TraceEvent {
        TraceEvent::RaftVote {
            server,
            range: SINGLE_GROUP,
            term: 1,
            candidate: 2,
            granted: true,
            pre: false,
        }
    }

    /// The durability replay's first gap, which the decision replay must not make.
    fn removed_gap(report: &Report) -> TimerGap {
        assert!(report.timers_fire_by(RecordTime::Decided).is_ok());
        let gap = report
            .timer_gaps_by(TimerResets::ALL, RecordTime::Durable)
            .first()
            .copied()
            .expect("a gap by durability time");
        assert_eq!(
            report.timers_fire_by(RecordTime::Durable),
            Err(gap.violation())
        );
        gap
    }

    /// A reset decided before the flag record and traced after it: server 1's
    /// granted vote, decided at 399 ms and traced at 402 ms, against a flag at 401 ms.
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    #[test]
    fn a_timer_catch_removed_by_a_reset_decided_before_the_flag() {
        let report = report(
            vec![
                record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
                record(
                    ms(401),
                    ms(401),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(401) },
                ),
                record(ms(402), ms(399), Some(1), vote(1)),
            ],
            Vec::new(),
        );
        let gap = removed_gap(&report);
        assert_eq!((gap.server, gap.since, gap.record), (1, ms(0), 1));
        assert_eq!(
            report.timer_removal(&gap),
            Ok(vec![TimerRemoval::ResetMovedBack { reset: 2 }])
        );
    }

    /// The flag record itself decided within the bound (the review's shape): a
    /// record decided at 399 ms and traced at 401 ms, and server 1's next reset at
    /// 500 ms, decided as it is traced, with nothing decided in between. No reset of
    /// server 1 straddles the flag, which the assertion before this entry required.
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    #[test]
    fn a_timer_catch_removed_by_the_flag_record_decided_within_the_bound() {
        let report = report(
            vec![
                record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
                record(
                    ms(401),
                    ms(399),
                    Some(2),
                    TraceEvent::RaftCommit {
                        server: 2,
                        range: SINGLE_GROUP,
                        term: 1,
                        index: 1,
                    },
                ),
                record(ms(500), ms(500), Some(1), vote(1)),
            ],
            Vec::new(),
        );
        let gap = removed_gap(&report);
        assert_eq!((gap.server, gap.since, gap.record), (1, ms(0), 1));
        assert!(
            report.records[2].decided > gap.at,
            "the only later reset is decided after the flag"
        );
        assert_eq!(
            report.timer_removal(&gap),
            Ok(vec![TimerRemoval::FlagMovedBack { flag: 1 }])
        );
    }

    /// Leadership decided before the flag record and traced after it, which is no
    /// reset: server 1, a candidate since 0 ms, wins at 399 ms and is traced leading
    /// at 402 ms, against a flag at 401 ms.
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    #[test]
    fn a_timer_catch_removed_by_leadership_decided_before_the_flag() {
        let report = report(
            vec![
                record(ms(0), ms(0), Some(1), term(1, 1, "candidate", None)),
                record(
                    ms(401),
                    ms(401),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(401) },
                ),
                record(
                    ms(402),
                    ms(399),
                    Some(1),
                    TraceEvent::RaftLeader {
                        server: 1,
                        range: SINGLE_GROUP,
                        term: 1,
                        last_index: 0,
                    },
                ),
            ],
            Vec::new(),
        );
        let gap = removed_gap(&report);
        assert_eq!((gap.server, gap.since, gap.record), (1, ms(0), 1));
        assert_eq!(
            report.timer_removal(&gap),
            Ok(vec![TimerRemoval::StatusMoved { record: 2 }])
        );
    }

    /// The other direction: a gap nothing moved is flagged under both readings, and
    /// no reason is given for a removal that did not happen.
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    #[test]
    fn a_timer_catch_both_readings_make_has_no_removal_reason() {
        let report = report(
            vec![
                record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
                record(
                    ms(401),
                    ms(401),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(401) },
                ),
                record(ms(402), ms(402), Some(1), vote(1)),
            ],
            Vec::new(),
        );
        assert!(report.timers_fire_by(RecordTime::Decided).is_err());
        let gap = report.timer_gaps_by(TimerResets::ALL, RecordTime::Durable)[0];
        assert!(report.timer_removal(&gap).is_err());
    }

    /// D-050's excuse needs every change of the window's term to be taken from a
    /// message received by its start: one received before it beside a campaign with no
    /// receipt is flagged, and the same change alone is not.
    // PROPOSED(D-050): a term's record carries when the message its step took was
    // received.
    #[test]
    fn a_window_with_one_change_received_before_it_and_one_without_is_flagged() {
        let isolation = (1, ms(100), ms(500));
        let received = record(
            ms(111),
            ms(110),
            Some(1),
            term(1, 2, "follower", Some(stamp(ms(90)))),
        );
        let campaign = record(ms(201), ms(200), Some(1), term(1, 3, "candidate", None));
        let start = record(ms(0), ms(0), Some(1), term(1, 1, "follower", None));
        let mixed = report(
            vec![start.clone(), received.clone(), campaign],
            vec![isolation],
        );
        assert_eq!(mixed.isolation_received_straddles().len(), 1);
        assert_eq!(
            mixed.isolation_keeps_the_term_by_cause(),
            Err(
                "pre-vote: server 1 raised its term from 1 to 3 while isolated from Instant(100ms) to Instant(500ms)"
                    .to_owned()
            )
        );
        let alone = report(vec![start, received], vec![isolation]);
        assert!(
            alone
                .isolation_keeps_the_term_by(RecordTime::Decided)
                .is_err()
        );
        assert_eq!(alone.isolation_keeps_the_term_by_cause(), Ok(()));
    }

    /// A `RaftSnapshot` on `server` at `at`: an install it completed, which ends the
    /// incarnation, or a snapshot it took of its own accord, which ends nothing.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    fn snapshot(at: Instant, server: u64, taken: bool) -> TraceRecord {
        record(
            at,
            at,
            Some(server),
            TraceEvent::RaftSnapshot {
                server,
                range: SINGLE_GROUP,
                last_index: 374,
                last_term: 1,
                taken,
            },
        )
    }

    /// The records a restatement traces at one instant, in `start_store`'s stable
    /// order (D-029): the store's snapshot re-traced, the recovery — which is what
    /// tells that re-trace from an install's completion — and the `RaftTerm` the new
    /// incarnation resumes in, whose first tick arms its election timer.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    fn restatement(at: Instant, server: u64) -> Vec<TraceRecord> {
        vec![
            snapshot(at, server, false),
            record(
                at,
                at,
                Some(server),
                TraceEvent::RaftRecovered {
                    server,
                    range: SINGLE_GROUP,
                    term: 1,
                    applied: 374,
                    last_index: 374,
                    incarnation: 1,
                },
            ),
            record(at, at, Some(server), term(server, 1, "follower", None)),
        ]
    }

    /// Nothing happening at `at`: a record for the replay to read the bound at, as a
    /// run's own trace always has one.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    fn silence(at: Instant) -> TraceRecord {
        record(at, at, None, TraceEvent::TimeAdvanced { to: at })
    }

    /// What the exemption covers, in both directions — issue #65's first hole, which
    /// no seed holds. The arm is written for a *completed install*, which ends the
    /// incarnation (`Next::Reinstall`, `crates/ananke-raft/src/node.rs`) and leaves
    /// the server with no core and no election timer until its restatement. A
    /// snapshot a server takes of its own accord ends nothing: its core is still
    /// there and still counting, so a stretch holding one is still a gap. Widening
    /// the arm to fire on every `RaftSnapshot` leaves seed 2605's pin green, the raft
    /// binary green at a hundred seeds and all three catch rates unchanged; it fails
    /// on this test's first case.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    #[test]
    fn a_snapshot_a_server_took_itself_leaves_its_election_timer_running() {
        // Server 1 starts at 0 ms and hears from no one after it. At 200 ms it takes
        // a snapshot of its own accord; nothing restates it, because nothing ended.
        let took = report(
            vec![
                record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
                snapshot(ms(200), 1, true),
                silence(ms(450)),
            ],
            Vec::new(),
        );
        assert_eq!(
            took.timer_gaps(TimerResets::ALL),
            vec![TimerGap {
                range: SINGLE_GROUP,
                server: 1,
                since: ms(0),
                at: ms(450),
                record: 2,
                installs: 0,
                restatements: 0,
                adoptions: 0,
            }],
            "a snapshot the server took is not a completed install and excuses nothing",
        );
        assert_eq!(
            took.timers_fire(),
            Err(took.timer_gaps(TimerResets::ALL)[0].violation())
        );

        // The same silence with the take replaced by an install the server
        // completed, and the restatement that ends the adoption: excused, and the
        // check as it stood on 1a1cad2 reports it — the mechanism both ways.
        let mut records = vec![
            record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
            snapshot(ms(200), 1, false),
            silence(ms(450)),
        ];
        records.extend(restatement(ms(500), 1));
        let completed = report(records.clone(), Vec::new());
        let gaps = completed.timer_gaps(TimerResets::ALL);
        assert!(
            gaps.is_empty(),
            "the adoption is not measured against the bound: {gaps:?}",
        );
        assert_eq!(
            completed.timer_gaps_rescued_by_adoption(),
            vec![TimerGap {
                range: SINGLE_GROUP,
                server: 1,
                since: ms(0),
                at: ms(450),
                record: 2,
                installs: 0,
                restatements: 0,
                adoptions: 1,
            }],
        );

        // And the exemption ends at the restatement, which re-admits the server with
        // a fresh clock: the next stretch past the bound is a gap since 500 ms.
        records.push(silence(ms(950)));
        assert_eq!(
            report(records, Vec::new()).timer_gaps(TimerResets::ALL),
            vec![TimerGap {
                range: SINGLE_GROUP,
                server: 1,
                since: ms(500),
                at: ms(950),
                record: 6,
                installs: 0,
                restatements: 0,
                adoptions: 0,
            }],
            "a restated server is running again and measured again",
        );
    }

    /// Why a completed install takes the server out of the replay's running set
    /// rather than resetting its clock — the alternative this entry rejects, which
    /// seed 2605's pin does not tell apart (issue #65's second hole). The honest
    /// case is one where the coreless window *alone* outlasts the bound: the install
    /// completes at 100 ms and the restatement lands at 700 ms, 600 ms against a
    /// 400 ms bound, so a clock reset at the completion would still flag the window
    /// at 650 ms — measuring a server that has no incarnation to campaign with
    /// against a timer that does not exist. Removal reports nothing, and the
    /// restatement's `RaftTerm` starts the next incarnation's count at 700 ms.
    // PROPOSED(D-063): a server adopting a completed install has no election timer.
    #[test]
    fn a_coreless_window_longer_than_the_bound_is_removed_not_measured_from_the_completion() {
        let mut records = vec![
            record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
            snapshot(ms(100), 1, false),
            silence(ms(650)),
        ];
        records.extend(restatement(ms(700), 1));
        records.push(silence(ms(1000)));
        let report = report(records, Vec::new());
        assert!(
            ms(700).duration_since(ms(100)) > report.timer_bound(1),
            "the window from the completion to the restatement outlasts the bound by \
             itself, so a reset at the completion measures past it",
        );
        let gaps = report.timer_gaps(TimerResets::ALL);
        assert!(
            gaps.is_empty(),
            "a server with no incarnation is not measured: {gaps:?}",
        );
        assert_eq!(
            report.timer_gaps_rescued_by_adoption(),
            vec![TimerGap {
                range: SINGLE_GROUP,
                server: 1,
                since: ms(0),
                at: ms(650),
                record: 2,
                installs: 0,
                restatements: 0,
                adoptions: 1,
            }],
            "the check as it stood on 1a1cad2 flags the window",
        );
    }

    /// A change decided at the very instant an isolation began and traced after it is
    /// before the window by decision time and inside it by durability time, so it is
    /// a straddle: the boundaries the two readings draw (D-051).
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    #[test]
    fn a_change_decided_at_the_isolations_start_and_traced_inside_it_straddles_it() {
        let isolation = (1, ms(100), ms(500));
        let report = report(
            vec![
                record(ms(0), ms(0), Some(1), term(1, 1, "follower", None)),
                record(ms(102), ms(100), Some(1), term(1, 2, "follower", None)),
            ],
            vec![isolation],
        );
        assert!(
            report
                .isolation_keeps_the_term_by(RecordTime::Durable)
                .is_err()
        );
        assert_eq!(
            report.isolation_keeps_the_term_by(RecordTime::Decided),
            Ok(())
        );
        let straddles = report.isolation_term_straddles();
        assert_eq!(straddles.len(), 1, "{straddles:?}");
        assert_eq!((straddles[0].decided, straddles[0].at), (ms(100), ms(102)));
    }

    // --- What the range key decides in the checks of `sim/` (SHARD.md §8) ---
    //
    // Every event of every sweep in this tree carries one range, so a check keyed
    // by the server alone says exactly what one keyed by (range, server) says on
    // every seed at every tier. These are the records that tell them apart, in the
    // shape the folds above are tested in: a trace of two ranges the keyed check
    // reads one way and a check without the key reads another.

    /// The other range a two-range case uses: the next id after the one a server
    /// runs today.
    const OTHER: u64 = SINGLE_GROUP + 1;

    fn term_of(server: u64, range: u64, term: u64, role: &'static str) -> TraceEvent {
        TraceEvent::RaftTerm {
            server,
            range,
            term,
            role,
            received: None,
        }
    }

    fn vote_in(server: u64, range: u64) -> TraceEvent {
        TraceEvent::RaftVote {
            server,
            range,
            term: 1,
            candidate: 2,
            granted: true,
            pre: false,
        }
    }

    fn range_created(range: u64, floor_term: u64) -> TraceEvent {
        TraceEvent::RangeCreated {
            range,
            cause: ananke_env::RangeCause::Snapshot,
            parent: None,
            start: Bytes::from_static(b""),
            end: Bytes::from_static(b"\xff"),
            generation: 1,
            voters: vec![1, 2, 3],
            floor_index: 0,
            floor_term,
            incarnation: 2,
        }
    }

    fn range_removed(range: u64) -> TraceEvent {
        TraceEvent::RangeRemoved {
            range,
            generation: 1,
            incarnation: 2,
            cause: ananke_env::RangeRemovedCause::Collected,
        }
    }

    /// Server 1 holds two ranges. It grants a vote in one of them at 300 ms, which
    /// resets that replica's timer and no other's: at 500 ms the replica of the
    /// other range has heard nothing for 500 ms, past its 400 ms bound, and is the
    /// gap. A check keyed by the server alone reads the vote as the node's and sees
    /// no gap at all — the wedged range beside the live one of SHARD.md §8.
    #[test]
    fn one_ranges_reset_does_not_stand_in_for_anothers_silence() {
        let report = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
                record(ms(300), ms(300), Some(1), vote_in(1, SINGLE_GROUP)),
                record(
                    ms(500),
                    ms(500),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(500) },
                ),
            ],
            Vec::new(),
        );
        let gaps = report.timer_gaps(TimerResets::ALL);
        assert_eq!(gaps.len(), 1, "{gaps:?}");
        assert_eq!((gaps[0].range, gaps[0].server), (OTHER, 1));
        assert_eq!(
            report.timers_fire_by(RecordTime::Decided),
            Err(gaps[0].violation())
        );
    }

    /// And the keying has not made every quiet range a gap: with both replicas
    /// reset within the bound there is none.
    #[test]
    fn each_ranges_own_reset_keeps_its_own_timer() {
        let report = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
                record(ms(300), ms(300), Some(1), vote_in(1, SINGLE_GROUP)),
                record(ms(300), ms(300), Some(1), vote_in(1, OTHER)),
                record(
                    ms(500),
                    ms(500),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(500) },
                ),
            ],
            Vec::new(),
        );
        assert_eq!(report.timer_gaps(TimerResets::ALL), Vec::new());
        assert_eq!(report.timers_fire_by(RecordTime::Decided), Ok(()));
    }

    /// A replica's `RangeCreated` arms its election timer, and its `RangeRemoved`
    /// ends it (SHARD.md §8): the replica installed at 300 ms is 200 ms from its
    /// creation at 500 ms, and the one removed at 300 ms has no timer to fire.
    /// Without either arm both are 500 ms past a reset and flagged.
    #[test]
    fn a_creation_arms_a_replicas_timer_and_a_removal_ends_it() {
        let report = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
                record(ms(300), ms(300), Some(1), range_created(SINGLE_GROUP, 1)),
                record(ms(300), ms(300), Some(1), range_removed(OTHER)),
                record(
                    ms(500),
                    ms(500),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(500) },
                ),
            ],
            Vec::new(),
        );
        assert_eq!(report.timer_gaps(TimerResets::ALL), Vec::new());
    }

    /// Pre-vote's property is per (range, server) (SHARD.md §8). Server 1 leads one
    /// range at term 5 and follows the other at term 2; inside the isolation it
    /// steps down in the range it led, at the term it already had. No replica's
    /// term moved. A check that reads the server's terms as one sequence sees the
    /// last record before the window (the follower's term 2) and the last record in
    /// it (the leader's term 5) and reports a raise that never happened.
    #[test]
    fn one_servers_two_ranges_keep_their_terms_apart() {
        let report = report(
            vec![
                record(
                    ms(50),
                    ms(50),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 5, "leader"),
                ),
                record(ms(80), ms(80), Some(1), term_of(1, OTHER, 2, "follower")),
                record(
                    ms(200),
                    ms(200),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 5, "follower"),
                ),
            ],
            vec![(1, ms(100), ms(500))],
        );
        assert_eq!(
            report.isolation_keeps_the_term_by(RecordTime::Decided),
            Ok(())
        );
        assert_eq!(report.isolation_keeps_the_term_by_cause(), Ok(()));
    }

    /// And a replica that does raise its own range's term while its node is cut off
    /// is the violation it always was.
    #[test]
    fn a_replica_that_raises_its_own_ranges_term_while_isolated_is_caught() {
        let report = report(
            vec![
                record(
                    ms(50),
                    ms(50),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 5, "leader"),
                ),
                record(ms(80), ms(80), Some(1), term_of(1, OTHER, 2, "follower")),
                record(ms(200), ms(200), Some(1), term_of(1, OTHER, 3, "candidate")),
            ],
            vec![(1, ms(100), ms(500))],
        );
        assert_eq!(
            report.isolation_keeps_the_term_by(RecordTime::Decided),
            Err(
                "pre-vote: server 1 raised its term from 2 to 3 while isolated from Instant(100ms) to Instant(500ms)"
                    .to_owned()
            )
        );
    }

    /// A range created on the isolated node during the isolation takes the term of
    /// its `RangeCreated` as the term the isolation began with (SHARD.md §8): the
    /// replica did not exist at the start, and the term its creation names is no
    /// election of its own. Without that clause its term reads as a raise from 0.
    #[test]
    fn a_range_created_under_an_isolation_starts_at_its_creations_term() {
        let report = report(
            vec![
                record(
                    ms(50),
                    ms(50),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 4, "follower"),
                ),
                record(ms(200), ms(200), Some(1), range_created(OTHER, 4)),
                record(ms(210), ms(210), Some(1), term_of(1, OTHER, 4, "follower")),
            ],
            vec![(1, ms(100), ms(500))],
        );
        assert_eq!(
            report.isolation_keeps_the_term_by(RecordTime::Decided),
            Ok(())
        );
    }

    /// The checks about time are asked only of a range whose unimpaired replicas
    /// form a majority (SHARD.md §8), where RAFT.md §2 asked it of the cluster: two
    /// of one range's three replicas quarantined by a re-seed leave that range with
    /// no majority, and say nothing about the range beside it. A node's refusal is
    /// its whole store's and impairs every range **on it**, which its per-replica
    /// refusals name (D-077).
    #[test]
    fn a_majority_is_asked_of_each_range_and_a_refusal_is_the_whole_nodes() {
        let quarantined = |server, range| TraceEvent::RaftReseeded { server, range };
        let one_range = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
                record(ms(1), ms(1), Some(1), quarantined(1, SINGLE_GROUP)),
                record(ms(2), ms(2), Some(2), quarantined(2, SINGLE_GROUP)),
            ],
            Vec::new(),
        );
        assert_eq!(
            one_range.ranges_with_a_majority_up(),
            BTreeSet::from([OTHER]),
            "one range short of a majority, the other not"
        );
        // A node refused while holding both ranges takes both of its replicas down,
        // and two such nodes leave neither range a majority. The refusal says the
        // node; the per-replica events say which replicas went with it.
        let whole_node = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
                record(ms(1), ms(1), Some(1), refused(1)),
                record(ms(1), ms(1), Some(1), replica_refused(1, SINGLE_GROUP)),
                record(ms(1), ms(1), Some(1), replica_refused(1, OTHER)),
                record(ms(2), ms(2), Some(2), refused(2)),
                record(ms(2), ms(2), Some(2), replica_refused(2, SINGLE_GROUP)),
                record(ms(2), ms(2), Some(2), replica_refused(2, OTHER)),
            ],
            Vec::new(),
        );
        assert_eq!(whole_node.ranges_with_a_majority_up(), BTreeSet::new());
    }

    /// The case the old reading got wrong, and the reason D-077 changed it.
    ///
    /// Before D-077 a `RaftRefused` marked its node down for *every range in the run*.
    /// At one group per server that was the same sentence; with four ranges on a node
    /// it is not. Here two nodes are refused holding one range each, and the ranges
    /// they never held — one that existed all along, one created after they were
    /// refused — are untouched by the refusal and must still be asked the checks about
    /// time. Under the old reading every one of them was marked down on both servers,
    /// dropped from this set, and so exempted from the bound, the recovery margin and
    /// every other tooth §8 has: the checks passed because they were never asked.
    ///
    /// The assertion below is the fix's evidence in both directions: the range the
    /// nodes *did* hold is short of a majority and correctly dropped, and the two they
    /// did not are kept. On the old code the expected set was empty.
    // PROPOSED(D-077): a refusal marks down the ranges its node held.
    #[test]
    fn a_refusal_marks_down_only_the_ranges_its_node_held() {
        const LATER: u64 = SINGLE_GROUP + 2;
        let report = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                // A range that existed all along on other nodes, never on 1 or 2.
                record(ms(0), ms(0), Some(3), term_of(3, OTHER, 1, "follower")),
                // Both nodes hold SINGLE_GROUP alone, and are refused holding it.
                record(ms(1), ms(1), Some(1), refused(1)),
                record(ms(1), ms(1), Some(1), replica_refused(1, SINGLE_GROUP)),
                record(ms(2), ms(2), Some(2), refused(2)),
                record(ms(2), ms(2), Some(2), replica_refused(2, SINGLE_GROUP)),
                // A range created after both refusals, on a node that was not refused.
                record(ms(3), ms(3), Some(3), term_of(3, LATER, 1, "follower")),
            ],
            Vec::new(),
        );
        assert_eq!(
            report.ranges_with_a_majority_up(),
            BTreeSet::from([OTHER, LATER]),
            "a refusal takes down the ranges its node held, and no others"
        );
    }

    /// The other half of the same reading: a replica that comes back is *lifted* out
    /// of the down set, so its range is asked the checks about time again.
    ///
    /// Nothing asserted this half until the review of D-077 planted it. Making the
    /// `RaftRecovered` arm a no-op — every refused replica down for the rest of its
    /// run — passes the whole tree, sweeps included, and it has to: a down set that is
    /// too *large* drops ranges from [`Report::ranges_with_a_majority_up`], and a range
    /// not in that set is one the checks about time are never asked about. The failure
    /// direction is green. That is the same silent exemption D-077 exists to close, one
    /// event along, so the lifting is asserted here rather than left to a sweep that
    /// cannot fail on it.
    // PROPOSED(D-077): a refusal marks down the ranges its node held.
    #[test]
    fn a_recovered_replica_is_lifted_out_and_its_range_is_asked_again() {
        let recovered = |server, range| TraceEvent::RaftRecovered {
            server,
            range,
            term: 1,
            applied: 0,
            last_index: 0,
            incarnation: 2,
        };
        let with = |lift: Vec<TraceRecord>| {
            let mut all = vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(1), ms(1), Some(1), refused(1)),
                record(ms(1), ms(1), Some(1), replica_refused(1, SINGLE_GROUP)),
                record(ms(2), ms(2), Some(2), refused(2)),
                record(ms(2), ms(2), Some(2), replica_refused(2, SINGLE_GROUP)),
            ];
            all.extend(lift);
            report(all, Vec::new())
        };

        // Two of the three replicas down: the range is not asked.
        assert_eq!(
            with(Vec::new()).ranges_with_a_majority_up(),
            BTreeSet::new(),
            "two replicas down leaves the range short of a majority"
        );

        // One of them re-seeded and restated: one replica down, a majority again.
        assert_eq!(
            with(vec![record(
                ms(3),
                ms(3),
                Some(1),
                recovered(1, SINGLE_GROUP)
            )])
            .ranges_with_a_majority_up(),
            BTreeSet::from([SINGLE_GROUP]),
            "a recovered replica is no longer down, and its range is asked again"
        );

        // A recovery of another range on the same server lifts nothing here: the set
        // is keyed by replica, not by server. That range is one of the run's, and
        // nothing took it down, so it is up — and this one is still short.
        assert_eq!(
            with(vec![record(ms(3), ms(3), Some(1), recovered(1, OTHER))])
                .ranges_with_a_majority_up(),
            BTreeSet::from([OTHER]),
            "a recovery names one replica, and server 1's replica of this range is not it"
        );
    }

    /// And the carve-out is read where it is *used*, not only where it is
    /// computed: the check about time asks it of the gap's own range. Server 1
    /// holds two ranges; both replicas of the other range are quarantined by a
    /// re-seed, so that range has no majority and the range beside it does. The
    /// live range's replica then hears nothing for 500 ms, past its 400 ms bound,
    /// and is the violation. A check that asked the cluster-wide
    /// [`Report::majority_up`] — false here, since one range is short — would skip
    /// every gap of the run and pass: the wedged range beside a live one of
    /// SHARD.md §8, seen from the consuming end.
    #[test]
    fn a_live_ranges_gap_is_flagged_though_the_range_beside_it_has_no_majority() {
        let quarantined = |server, range| TraceEvent::RaftReseeded { server, range };
        let report = report(
            vec![
                record(
                    ms(0),
                    ms(0),
                    Some(1),
                    term_of(1, SINGLE_GROUP, 1, "follower"),
                ),
                record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
                record(ms(1), ms(1), Some(1), quarantined(1, OTHER)),
                record(ms(2), ms(2), Some(2), quarantined(2, OTHER)),
                record(
                    ms(500),
                    ms(500),
                    None,
                    TraceEvent::TimeAdvanced { to: ms(500) },
                ),
            ],
            Vec::new(),
        );
        assert_eq!(
            report.ranges_with_a_majority_up(),
            BTreeSet::from([SINGLE_GROUP])
        );
        assert!(!report.majority_up(), "the cluster-wide reading is false");
        let gaps = report.timer_gaps(TimerResets::ALL);
        let live_gap = gaps
            .iter()
            .find(|gap| gap.range == SINGLE_GROUP)
            .expect("the live range's replica is past its bound");
        assert_eq!(
            report.timers_fire_by(RecordTime::Decided),
            Err(live_gap.violation())
        );
    }

    /// The write bound is asked per key (SHARD.md §8): one key's write completing
    /// says nothing of the key beside it, which a minimum over every write would
    /// let stand.
    #[test]
    fn the_write_bound_is_asked_of_every_key_written_after_the_heal() {
        let write = |key: &str, ret: Option<u64>| lin::Op {
            process: 1,
            seq: 0,
            call: ms(1),
            ret: ret.map(ms),
            op: ClientOp::Put {
                key: Bytes::from(key.to_owned()),
                value: Bytes::from_static(b"v"),
            },
            result: ret.map(|_| ananke_env::ClientResult::Done),
        };
        let live = vec![record(
            ms(0),
            ms(0),
            Some(1),
            term_of(1, SINGLE_GROUP, 1, "follower"),
        )];
        let with = |ops| Report {
            history: History {
                ops,
                ..History::default()
            },
            ..report(live.clone(), Vec::new())
        };
        // Both keys written and completed: the bound holds of each.
        with(vec![write("k0", Some(100)), write("k1", Some(200))])
            .liveness()
            .unwrap();
        // The second key's writes never completed: the wedge, which the minimum
        // over every write — 99 ms here — would have passed.
        let wedged = with(vec![write("k0", Some(100)), write("k1", None)]);
        assert_eq!(
            wedged.time_to_write_after_heal(),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            wedged.liveness(),
            Err(
                "liveness: no client write to k1 completed after the last heal at Instant(0ns)"
                    .to_owned()
            )
        );
    }

    /// Records of leader 3 in term 2, one event a millisecond in the order given.
    fn led_by_three(events: Vec<TraceEvent>) -> Vec<TraceRecord> {
        traced_by(events.into_iter().map(|event| (3, event)).collect())
    }

    /// The same, with the node that traced each record named: the maps this fold
    /// keeps are a *replica's*, so a second server's records are what tells them
    /// from one map held for the cluster.
    fn traced_by(events: Vec<(u64, TraceEvent)>) -> Vec<TraceRecord> {
        let start = record(ms(0), ms(0), Some(3), term_of(3, SINGLE_GROUP, 2, "leader"));
        std::iter::once(start)
            .chain(events.into_iter().enumerate().map(|(i, (node, event))| {
                let at = ms(u64::try_from(i).expect("few") + 1);
                record(at, at, Some(node), event)
            }))
            .collect()
    }

    /// A server's record of the configuration in force: joint where `new` is given.
    fn config_of(server: u64, old: &[u64], new: &[u64]) -> TraceEvent {
        config_in(SINGLE_GROUP, server, old, new)
    }

    fn config_in(range: u64, server: u64, old: &[u64], new: &[u64]) -> TraceEvent {
        TraceEvent::RaftConfig {
            server,
            range,
            index: 0,
            old: old.to_vec(),
            new: new.to_vec(),
            joint: !new.is_empty(),
            learners: Vec::new(),
        }
    }

    fn change_accepted(voters: &[u64]) -> TraceEvent {
        change_accepted_in(SINGLE_GROUP, voters)
    }

    fn change_accepted_in(range: u64, voters: &[u64]) -> TraceEvent {
        TraceEvent::RaftChangeAccepted {
            range,
            voters: voters.to_vec(),
            applied: 0,
            term: 2,
        }
    }

    fn match_started(follower: u64, incarnation: u64, matched: u64) -> TraceEvent {
        match_started_in(SINGLE_GROUP, follower, incarnation, matched)
    }

    fn match_started_in(range: u64, follower: u64, incarnation: u64, matched: u64) -> TraceEvent {
        TraceEvent::RaftMatchStarted {
            range,
            follower,
            incarnation,
            matched,
        }
    }

    fn progress_reset(server: u64, follower: u64, incarnation: u64) -> TraceEvent {
        TraceEvent::RaftProgressReset {
            server,
            range: SINGLE_GROUP,
            follower,
            incarnation,
        }
    }

    /// The violation names the leader, its term, the follower and the incarnation.
    fn a_second_first_rise(records: &[TraceRecord], of: &str) -> bool {
        match_starts_are_first_rises(records).is_err_and(|violation| {
            violation.contains("leader 3 of term 2") && violation.contains(of)
        })
    }

    /// Issue #81: a voter removed and re-added inside one leadership is tracked
    /// afresh — `on_change` re-inserts its `Progress` with `match_started: false`
    /// — so its match rises from zero again under the incarnation it never left,
    /// and that rise is a first rise. Two rises inside one window are not, which
    /// is what the check is for: this is the pair, and the membership scenario's
    /// seed 7205 is the run that made the first half of it.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_voter_re_added_inside_one_term_starts_its_match_afresh() {
        let re_added = led_by_three(vec![
            config_of(3, &[1, 2, 3], &[]),
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 10),
            config_of(3, &[1, 2, 3, 4], &[]),
            // The shrink drops 4; the grow after it tracks 4 from nothing again.
            change_accepted(&[1, 2, 3]),
            config_of(3, &[1, 2, 3], &[]),
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 20),
        ]);
        assert_eq!(match_starts_are_first_rises(&re_added), Ok(()));

        let continuous = led_by_three(vec![
            config_of(3, &[1, 2, 3, 4], &[]),
            match_started(4, 1, 10),
            match_started(4, 1, 20),
        ]);
        assert!(
            a_second_first_rise(&continuous, "4's match under incarnation 1"),
            "one continuous rise traced as two first rises must fail the check"
        );
    }

    /// A re-add opens the window of the server re-added and of no other: 5
    /// rejoining excuses nothing of 4's, which the leader never stopped tracking.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_re_add_opens_the_window_of_the_server_re_added_alone() {
        let records = led_by_three(vec![
            config_of(3, &[1, 2, 3, 4], &[]),
            match_started(4, 1, 10),
            change_accepted(&[1, 2, 3, 4, 5]),
            match_started(5, 1, 15),
            match_started(4, 1, 20),
        ]);
        assert!(
            a_second_first_rise(&records, "4's match under incarnation 1"),
            "5's re-add must not excuse a second first rise of 4's match"
        );
    }

    /// One catch-up phase is one window. The operator repeats its request while
    /// the change is in flight and the leader accepts it again (D-029), holding
    /// the change it already has and tracking nothing afresh: only a
    /// configuration of the leader's own ends the phase.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_request_repeated_inside_one_catch_up_phase_is_one_window() {
        let records = led_by_three(vec![
            config_of(3, &[1, 2, 3], &[]),
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 10),
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 20),
        ]);
        assert!(
            a_second_first_rise(&records, "4's match under incarnation 1"),
            "a repeat of the change in flight tracks nothing afresh"
        );
    }

    /// A change accepted while a joint configuration is in force tracks nothing
    /// afresh either: `on_change` answers from its `new_voters` branch and
    /// returns before it reaches the learners.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_change_accepted_under_a_joint_configuration_opens_no_window() {
        let records = led_by_three(vec![
            config_of(3, &[1, 2, 3], &[1, 2, 3, 4]),
            match_started(4, 1, 10),
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 20),
        ]);
        assert!(
            a_second_first_rise(&records, "4's match under incarnation 1"),
            "a change accepted under the joint configuration tracks nothing afresh"
        );
    }

    /// An incarnation carried back is a second first rise and stays one, reset or
    /// no reset. A leader forgets a follower's progress at every change of its
    /// incarnation (D-042), so the core does trace a first rise after one; but the
    /// incarnation is in the key, and the correct system never returns to a number
    /// it has retired — a fresh store is 1 and a re-seed draws above it. A repeat
    /// under one incarnation is therefore either D-042's one-message window, which
    /// SHARD.md §8 asks to be shown rather than allowed, or a store that lost its
    /// state and opened fresh at 1 again, which is `RefusalNotDurable`.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn an_incarnation_carried_back_is_a_second_first_rise() {
        let there_and_back = led_by_three(vec![
            config_of(3, &[1, 2, 3, 4], &[]),
            match_started(4, 1, 10),
            progress_reset(3, 4, 2),
            match_started(4, 2, 20),
            progress_reset(3, 4, 1),
            match_started(4, 1, 30),
        ]);
        assert!(
            a_second_first_rise(&there_and_back, "4's match under incarnation 1"),
            "an incarnation the leader had already retired came back: D-042's case, not a window"
        );
    }

    /// A term record of the leader's own ends the catch-up phase, as a
    /// configuration of its own does. `become_leader` and `become_follower` clear
    /// the core's change (core.rs:1680, core.rs:1410) and trace a role, not a
    /// configuration, so a leader that accepts a grow, loses the lead and takes it
    /// again re-tracks the same followers with no `RaftConfig` of its own between
    /// the two accepts. Reading only the configuration made the fold open one
    /// window where the core made two, on **2 of 11 220** re-tracks over seeds 0 to
    /// 3 000 (seeds 659 and 7123).
    ///
    /// That staleness was in the strict direction — fewer windows are more
    /// collisions — and the real shape carries a term rise with it, so the key's
    /// own term already told the two rises apart and no verdict was ever wrong.
    /// The case therefore holds the term still, with a restatement rather than an
    /// election, so that the clearing is asserted at all rather than masked by the
    /// term in the key.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_term_record_of_the_leaders_own_ends_the_catch_up_phase() {
        let restated = led_by_three(vec![
            config_of(3, &[1, 2, 3], &[]),
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 10),
            // The core this leader runs on is rebuilt and restates its term: its
            // change went with it, so the accept that follows tracks 4 afresh.
            TraceEvent::RaftRecovered {
                server: 3,
                range: SINGLE_GROUP,
                term: 2,
                applied: 0,
                last_index: 0,
                incarnation: 1,
            },
            change_accepted(&[1, 2, 3, 4]),
            match_started(4, 1, 20),
        ]);
        assert_eq!(match_starts_are_first_rises(&restated), Ok(()));
    }

    /// The configuration a window is read against is the *leader's own*. A
    /// follower restating a configuration the leader has left behind must not open
    /// the leader's window: 4 is a voter of leader 3's configuration throughout, so
    /// the accept that names it tracks nothing afresh and the second rise is a
    /// second first rise.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_followers_configuration_opens_no_window_of_the_leaders() {
        let records = traced_by(vec![
            (3, config_of(3, &[1, 2, 3, 4], &[])),
            (3, match_started(4, 1, 10)),
            // Server 1 is behind and still says {1, 2, 3}. It is not the leader.
            (1, config_of(1, &[1, 2, 3], &[])),
            (3, change_accepted(&[1, 2, 3, 4])),
            (3, match_started(4, 1, 20)),
        ]);
        assert!(
            a_second_first_rise(&records, "4's match under incarnation 1"),
            "a follower's stale configuration must not open the leader's window"
        );
    }

    /// The catch-up phase a repeat falls in is the *leader's own* too: another
    /// server's configuration is not the end of it, so the leader's repeat of the
    /// change it already holds still tracks nothing afresh.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_followers_configuration_does_not_end_the_leaders_catch_up_phase() {
        let records = traced_by(vec![
            (3, config_of(3, &[1, 2, 3], &[])),
            (3, change_accepted(&[1, 2, 3, 4])),
            (3, match_started(4, 1, 10)),
            // Server 1 takes the joint entry and traces its own configuration.
            (1, config_of(1, &[1, 2, 3], &[1, 2, 3, 4])),
            (3, change_accepted(&[1, 2, 3, 4])),
            (3, match_started(4, 1, 20)),
        ]);
        assert!(
            a_second_first_rise(&records, "4's match under incarnation 1"),
            "another server's configuration must not end the leader's catch-up phase"
        );
    }

    /// Every map here is a replica's, the windows included: a change accepted in
    /// one range tracks nothing afresh in another, and must not forgive a repeat
    /// there. D-069's key was range-blind in the *strict* direction — one node's
    /// four ranges collapsed into one key — and D-076 fixed that; window state
    /// keyed by the node alone would be range-blind in the *lax* direction, which
    /// is the one that lets a repeat through.
    // PROPOSED(D-079): a first rise per tracking window.
    #[test]
    fn a_change_accepted_in_one_range_opens_no_window_in_another() {
        let records = led_by_three(vec![
            config_of(3, &[1, 2, 3, 4], &[]),
            config_in(OTHER, 3, &[1, 2, 3], &[]),
            match_started(4, 1, 10),
            // A genuine re-add of 4, in the other range and in that range alone.
            change_accepted_in(OTHER, &[1, 2, 3, 4]),
            match_started_in(OTHER, 4, 1, 15),
            match_started(4, 1, 20),
        ]);
        assert!(
            a_second_first_rise(&records, &format!("of group {SINGLE_GROUP} traced")),
            "a window opened in one range must not forgive a repeat in another"
        );
    }

    /// The model error the nightly found on the node scenario (D-076): with eight
    /// keys and two clients, the first post-heal write to one key may not be
    /// *issued* for two seconds, and reading it as `ret − last_heal` charged that
    /// idle time to a cluster that served the key in 25 ms when it was finally
    /// asked. Measured from the write's own call the run passes, and the tooth the
    /// old reading carried — a range that is slow to become writable — moves to the
    /// per-range reading, where no client's choice of key can lengthen it.
    ///
    /// The pair (CLAUDE.md:52-57) is the second history here: a range that takes
    /// 2.5 s after the heal to complete any write, while every *individual* write
    /// that completes is quick, passes the per-key reading and is caught by the
    /// per-range one. Neither bound is widened: both are `election_max() * 10`.
    // PROPOSED(D-076): a post-heal write is measured from its own call, and the
    // recovery time proper is asked per range.
    #[test]
    fn a_post_heal_write_is_measured_from_its_own_call_and_the_range_from_the_heal() {
        let write = |key: &str, call: u64, ret: Option<u64>| lin::Op {
            process: 1,
            seq: 0,
            call: ms(call),
            ret: ret.map(ms),
            op: ClientOp::Put {
                key: Bytes::from(key.to_owned()),
                value: Bytes::from_static(b"v"),
            },
            result: ret.map(|_| ananke_env::ClientResult::Done),
        };
        let live = vec![record(
            ms(0),
            ms(0),
            Some(1),
            term_of(1, SINGLE_GROUP, 1, "follower"),
        )];
        let with = |ops| Report {
            history: History {
                ops,
                ..History::default()
            },
            ..report(live.clone(), Vec::new())
        };
        let bound = election_max() * LIVENESS_TIMEOUTS;
        assert_eq!(
            bound,
            Duration::from_secs(2),
            "the bound this case is about"
        );
        // Seed 2400's shape: k0 served all along, k1 asked for the first time
        // 2.400 s after the heal and served in 24 ms. The cluster was never slow.
        let idle = with(vec![
            write("k0", 5, Some(30)),
            write("k1", 2400, Some(2424)),
        ]);
        assert_eq!(
            idle.writes_after_heal_by_key()[&Bytes::from_static(b"k1")],
            Some(Duration::from_millis(24))
        );
        idle.liveness()
            .expect("the client's idleness is not the cluster's");
        // The reading it replaces would have failed this run at 2.424 s, which is
        // what the nightly failed on six seeds of ten thousand.
        assert!(
            idle.history.ops.iter().any(|op| op
                .ret
                .expect("returned")
                .duration_since(idle.last_heal)
                > bound),
            "the run this case is built from is one the old reading failed"
        );
        // The pair: every write that completes is quick, and the range still took
        // 2.5 s after the heal to complete one. The per-key reading passes it.
        let slow = with(vec![
            write("k0", 10, None),
            write("k0", 800, None),
            write("k0", 2495, Some(2505)),
        ]);
        assert_eq!(
            slow.writes_after_heal_by_key()[&Bytes::from_static(b"k0")],
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            slow.liveness(),
            Err(format!(
                "liveness: range {SINGLE_GROUP} took 2.505s after the last heal to \
                 complete a client write, over 2s"
            ))
        );
    }

    /// The liveness half of the majority carve-out, which D-071 owed a case of its
    /// own: "the stage that gives `range_of_key` a map owes the liveness half its
    /// own two-range case in the same PR" (D-071, item 6). With every key in one
    /// range no history could put a live range and a range without a majority on two
    /// different keys, so `Report::liveness`'s per-range reading could not be told
    /// from the cluster-wide one by any test. The node scenario's map can
    /// (`ranges::range_of_key`), and this is that case: the wedged range's key is
    /// not asked of, and the live range's is.
    // PROPOSED(D-076): the liveness half of the carve-out, keyed by the run's map.
    #[test]
    fn a_wedged_ranges_key_is_not_asked_of_while_the_range_beside_it_is_live() {
        fn two_ranges(key: &Bytes) -> u64 {
            if key.as_ref() == b"k0" {
                SINGLE_GROUP
            } else {
                OTHER
            }
        }
        let write = |key: &str, ret: Option<u64>| lin::Op {
            process: 1,
            seq: 0,
            call: ms(1),
            ret: ret.map(ms),
            op: ClientOp::Put {
                key: Bytes::from(key.to_owned()),
                value: Bytes::from_static(b"v"),
            },
            result: ret.map(|_| ananke_env::ClientResult::Done),
        };
        let quarantined = |server, range| TraceEvent::RaftReseeded { server, range };
        // `OTHER` is one replica short of a majority; `SINGLE_GROUP` is whole.
        let records = vec![
            record(
                ms(0),
                ms(0),
                Some(1),
                term_of(1, SINGLE_GROUP, 1, "follower"),
            ),
            record(ms(0), ms(0), Some(1), term_of(1, OTHER, 1, "follower")),
            record(ms(1), ms(1), Some(1), quarantined(1, OTHER)),
            record(ms(2), ms(2), Some(2), quarantined(2, OTHER)),
        ];
        let with = |ops| Report {
            history: History {
                ops,
                ..History::default()
            },
            key_range: two_ranges,
            ..report(records.clone(), Vec::new())
        };
        let wedged = with(vec![write("k0", Some(100)), write("k1", None)]);
        assert_eq!(
            wedged.ranges_with_a_majority_up(),
            BTreeSet::from([SINGLE_GROUP])
        );
        // The wedged range's key never completed a write, and the check does not
        // ask: RAFT.md §2 withholds liveness from a range without a majority. Read
        // cluster-wide — `live.contains` replaced by `true`, which is the mutation
        // this case exists for — the same history fails on k1.
        wedged
            .liveness()
            .expect("the wedged range's key is not asked of");
        // And the live range's key is asked of: keying has not widened the check
        // into a check of nothing.
        let live_wedged = with(vec![write("k0", None), write("k1", Some(100))]);
        assert_eq!(
            live_wedged.liveness(),
            Err(
                "liveness: no client write to k0 completed after the last heal at Instant(0ns)"
                    .to_owned()
            )
        );
    }
}

/// The two folds D-065 added, on records written by hand.
///
/// Both read a trace positionally, and both were found by the review of this
/// slice to be defensible by nothing: a sweep cannot see a fold that under-reads,
/// because the bound is never near-tripped on the correct system, and it cannot
/// see a measure that counts the wrong thing, because nothing asserts the count.
/// These tests are that defence, and each names the wrong shape it rejects.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
#[cfg(test)]
mod compaction_folds {
    use super::*;
    use ananke_raft::node::SINGLE_GROUP;

    fn at(n: u64) -> Instant {
        Instant::from_nanos(n * 1_000_000)
    }

    /// A record of `server`'s, emitted by the node of the same number — which is
    /// how the simulator numbers them, and what [`Restating`] reads as the second
    /// half of "nothing of that server's in between".
    fn of(n: u64, server: u64, event: TraceEvent) -> TraceRecord {
        TraceRecord {
            at: at(n),
            decided: at(n),
            node: Some(NodeId::new(u32::try_from(server).expect("small"))),
            event,
        }
    }

    fn append(server: u64, index: u64) -> TraceEvent {
        TraceEvent::RaftAppend {
            server,
            range: SINGLE_GROUP,
            index,
            entry_term: 1,
            hash: 0,
        }
    }

    fn truncate(server: u64, from_index: u64) -> TraceEvent {
        TraceEvent::RaftTruncate {
            server,
            range: SINGLE_GROUP,
            from_index,
        }
    }

    fn snapshot(server: u64, last_index: u64, taken: bool) -> TraceEvent {
        TraceEvent::RaftSnapshot {
            server,
            range: SINGLE_GROUP,
            last_index,
            last_term: 1,
            taken,
        }
    }

    fn compacted(server: u64, through: u64) -> TraceEvent {
        TraceEvent::RaftCompacted {
            server,
            range: SINGLE_GROUP,
            through,
        }
    }

    fn config(server: u64, index: u64) -> TraceEvent {
        TraceEvent::RaftConfig {
            server,
            range: SINGLE_GROUP,
            index,
            old: vec![1, 2, 3],
            new: Vec::new(),
            joint: false,
            learners: Vec::new(),
        }
    }

    fn following(server: u64) -> TraceEvent {
        TraceEvent::RaftTerm {
            server,
            range: SINGLE_GROUP,
            term: 1,
            role: "follower",
            received: None,
        }
    }

    /// The largest follower log, over a trace whose every shape is known.
    fn longest(records: &[TraceRecord]) -> u64 {
        let mut worst = 0;
        fold_logs(records, |_, shape| {
            worst = worst.max(if shape.leading { 0 } else { shape.len() });
            Ok(())
        })
        .expect("the fold decides nothing");
        worst
    }

    /// The log the fold holds after the last record of the trace.
    fn ends_at(records: &[TraceRecord]) -> u64 {
        let mut last = 0;
        fold_logs(records, |_, shape| {
            last = shape.len();
            Ok(())
        })
        .expect("the fold decides nothing");
        last
    }

    /// A **live take** leaves the log where it was; an install and a re-statement
    /// stand in for a prefix that is gone. The whole Stage B exit measurement
    /// rests on this one distinction, and reading a take as moving the prefix
    /// under-reads every follower log that has a take outstanding — the wrong
    /// direction for a bound, and invisible to any sweep, since a bound that is
    /// never near-tripped cannot fail on a log read too short.
    ///
    /// The shape rejected: `TraceEvent::RaftSnapshot { .. } => true` in
    /// [`Restating::moves_the_prefix`], which is what this fold did before the
    /// first review of this slice. It reads the log below as 10 entries, not 30.
    #[test]
    fn a_live_take_does_not_move_the_folds_prefix_and_an_install_does() {
        let mut records: Vec<TraceRecord> = vec![of(0, 1, following(1))];
        for index in 1..=30 {
            records.push(of(index, 1, append(1, index)));
        }
        assert_eq!(ends_at(&records), 30, "thirty entries, no prefix");

        // A take at 20, with no compaction under it yet: the core still holds
        // every entry from 1, so the log is *still* 30 entries. This is the
        // assertion the mutation fails, and it has to be read after the take
        // rather than as a maximum over the run — the maximum was already 30
        // before the take, so a maximum cannot see the take shorten it.
        records.push(of(31, 1, snapshot(1, 20, true)));
        assert_eq!(
            ends_at(&records),
            30,
            "a take writes a checkpoint and leaves the log alone"
        );
        assert_eq!(longest(&records), 30);

        // The compaction is what drops the prefix.
        records.push(of(32, 1, compacted(1, 20)));
        assert_eq!(longest(&records), 30, "the maximum stands at 30");
        assert_eq!(
            ends_at(&records),
            10,
            "after the compaction the log is the 10 entries past the prefix"
        );

        // A restart: the durable log re-stated as a truncation to one past its
        // end and then the prefix's snapshot — a *re-stated take*, which does
        // move the prefix, because the prefix it names is already gone.
        let mut restarted = records.clone();
        restarted.push(of(33, 1, truncate(1, 31)));
        restarted.push(of(34, 1, snapshot(1, 20, true)));
        assert_eq!(
            longest(&restarted),
            30,
            "a re-statement restores the shape the compaction left, no more"
        );

        // An install replaces the log under the snapshot at once, take or no take.
        let mut installed = records.clone();
        installed.push(of(33, 1, snapshot(1, 28, false)));
        assert_eq!(
            ends_at(&installed),
            2,
            "an install's prefix leaves 29 and 30"
        );
    }

    /// The positional rule is "nothing of that server's between its truncation and
    /// the snapshot". [`replica_of`] is a list of event kinds, and a list is what
    /// falls behind: seven kinds that carry a server were missing from it, and any
    /// one of them landing in that window would have turned a real install into a
    /// re-statement — which moves the asserted follower-log bound, since a
    /// re-stated take moves the prefix where a live take does not.
    ///
    /// The shape rejected: `RaftRefused` (one of the seven) not clearing the
    /// window. With it missing, the take below is read as a re-statement.
    #[test]
    fn any_record_of_a_server_closes_its_restatement_window() {
        let take = |between: Option<TraceEvent>| {
            let mut records = vec![of(0, 1, following(1)), of(1, 1, append(1, 1))];
            records.push(of(2, 1, truncate(1, 2)));
            if let Some(event) = between {
                records.push(of(3, 1, event));
            }
            records.push(of(4, 1, snapshot(1, 1, true)));
            let mut restating = Restating::default();
            let mut moved = false;
            for record in &records {
                restating.saw(record);
                if matches!(record.event, TraceEvent::RaftSnapshot { .. }) {
                    moved = restating.moves_the_prefix(&record.event);
                }
            }
            moved
        };
        assert!(
            take(None),
            "a take's snapshot straight after that server's truncation is the \
             restatement's, and it moves the prefix"
        );
        for between in [
            TraceEvent::RaftRefused {
                server: 1,
                reason: "lost state".to_owned(),
            },
            append(1, 2),
            compacted(1, 1),
        ] {
            assert!(
                !take(Some(between.clone())),
                "a record of server 1's closes the window, so what follows is a \
                 live take, not a re-statement: {between:?}"
            );
        }
    }

    /// D-029's revert floor, observed rather than inferred: a follower's
    /// compaction swallowed the configuration entry in force only when that entry
    /// sits *strictly inside* the step the prefix took.
    ///
    /// The shape rejected: asking whether the server's last configuration index is
    /// at or below `through`, which holds for every compaction by construction and
    /// so counts the total under another name. It reads the trace below as 2 of 2;
    /// the truth is 1 of 2.
    #[test]
    fn a_swallowed_configuration_is_the_one_inside_the_step() {
        let mut records = vec![of(0, 1, following(1)), of(1, 1, config(1, 5))];
        for index in 1..=12 {
            records.push(of(1 + index, 1, append(1, index)));
        }
        records.push(of(20, 1, compacted(1, 8)));
        records.push(of(21, 1, compacted(1, 12)));
        assert_eq!(
            follower_compactions(&records),
            (2, 1),
            "two compactions by a replica that is not leading; the configuration at \
             5 is inside the first step and behind the second"
        );

        // Index 0 is the initial configuration, which is no entry: it can be
        // swallowed by nothing.
        let initial = vec![
            of(0, 2, following(2)),
            of(1, 2, config(2, 0)),
            of(2, 2, append(2, 1)),
            of(3, 2, compacted(2, 1)),
        ];
        assert_eq!(
            follower_compactions(&initial),
            (1, 0),
            "the initial configuration is no log entry"
        );

        // A leader's compaction is not a follower's.
        let leading = vec![
            of(
                0,
                3,
                TraceEvent::RaftLeader {
                    server: 3,
                    range: SINGLE_GROUP,
                    term: 1,
                    last_index: 0,
                },
            ),
            of(1, 3, config(3, 5)),
            of(2, 3, append(3, 8)),
            of(3, 3, compacted(3, 8)),
        ];
        assert_eq!(follower_compactions(&leading), (0, 0));
    }
}

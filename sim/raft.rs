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
//! (CLAUDE.md): each [`Variant`] of RAFT.md §5 that this stage ships must be caught
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
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{
    ClientOp, ClientResult, Clock, Either, Environment, Instant, Network, NodeId, Rng, Socket,
    TraceEvent, race,
};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, Variant};
use ananke_raft::message::{self, Frame, Message};
use ananke_raft::{NodeConfig, ServerId, invariants, run as run_server};
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
/// burst client's. PROPOSED(D-043).
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
    /// A crash aimed at the adoption of a completed install (PROPOSED(D-041)).
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
    /// (PROPOSED(D-044)). Each round waits for `server` to rotate a memtable it
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
    /// The shape PROPOSED D-043 named and left to the sweep's owner: a leader
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
    /// stream reads the version it pinned untouched (PROPOSED(D-043)). Third,
    /// the cut-off follower heals having itself fallen behind and gone quiet, so
    /// it too is designated: as built it waits in the backlog behind a stream
    /// that never ends and is fed nothing, while the correct server streams to
    /// both at once. With neither follower countable the commit index does not
    /// move again, and the liveness check reports it `hold` and the gap and the
    /// settle later — which is seed 5909's wedge, assembled rather than waited
    /// for. Drawn from its own `moirae_sched` stream ("retake-stream"), never
    /// lengthening the shared schedule stream or any other arm's (D-031).
    /// PROPOSED(D-043).
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
}

/// The longest a [`Fault::CrashInstalling`] waits for an install to stream
/// before giving up and doing nothing.
pub const INSTALL_WAIT_BUDGET: Duration = Duration::from_millis(4000);

/// The longest a [`Fault::RetakeUnderStream`] waits, after healing the follower
/// it cut off, for a stream to that follower to open before giving up and
/// leaving the fault an isolation. A designation costs two minimum election
/// timeouts of quiet and the stream opens on the next heartbeat after the heal,
/// so a stream that is coming has come well inside this; the budget is shorter
/// than [`INSTALL_WAIT_BUDGET`] because it waits for the stream's *opening*, not
/// for a chunk of it to land. PROPOSED(D-043).
pub const STREAM_WAIT_BUDGET: Duration = Duration::from_millis(2500);

/// One seed in this many draws a [`Fault::RetakeUnderStream`], from the fault's
/// own `moirae_sched` stream ("retake-stream"). The arm costs a seed its
/// isolation, the wait for the stream, the freeze and the hold — some two
/// seconds of virtual time — which is why it is not on every schedule; a quarter
/// of the seeds is what the catch rate at the hundred-seed tier needs and what
/// `scripts/premerge.sh`'s quarter of an hour affords beside the adoption
/// storm's own quarter. PROPOSED(D-043).
pub const RETAKE_STREAM_IN: u64 = 4;

/// The longest a [`Fault::CrashRefused`] waits for its victim to begin flushing
/// a memtable before crashing it anyway. A server fills a sixteen-kilobyte
/// memtable about every two seconds at the sweep's write rate. PROPOSED(D-044).
pub const FLUSH_WAIT_BUDGET: Duration = Duration::from_millis(2500);

/// One seed in this many draws a [`Fault::CrashAdopting`] storm, from the
/// fault's own `moirae_sched` stream ("adoption-crash"). D-041 appended the
/// storm to every schedule and the sweep paid for it: the raft test binary's
/// thousand seeds went from 667 s to 2218 s and `scripts/premerge.sh` from about
/// thirteen minutes to forty, over the fifteen-minute target of the tier
/// (D-040). The crash count on a seed that draws the storm is unchanged, so what
/// the share costs is the catch rate — roughly a quarter of what it was — and
/// what it buys back is three quarters of the seeds at their old price.
/// PROPOSED(D-041).
pub const ADOPTION_STORM_IN: u64 = 4;

/// The longest a [`Fault::CrashAdopting`] waits, after the install's completion
/// or a restart, for the adoption's first durable change to the store directory
/// before crashing the server anyway. An adoption reaches that change within a
/// few dozen disk operations, tens of milliseconds at the sweep's latencies; a
/// restart that is refused instead makes no change at all. PROPOSED(D-041).
pub const ADOPTION_WAIT_BUDGET: Duration = Duration::from_millis(100);

/// How long a [`Fault::CrashAdopting`] waits after a restart that finds no store
/// file durable in the directory at all — the old store gone and every copy's
/// entry lost at the crash before — since then the adoption's first durable
/// change would be its copies synced in, past the window that matters; the
/// crash lands in the adoption's opening reads and first copies instead.
/// PROPOSED(D-041).
const EMPTY_STORE_DELAY: Duration = Duration::from_millis(3);

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
/// [`Fault::RetakeUnderStream`]. PROPOSED(D-043).
const SPREAD: u64 = 97 << 32;
/// How many bytes each filling put carries. The state machine the clients build
/// on their own is two keys of a dozen bytes, so a checkpoint of it is one chunk
/// and an install is over before anything can be aimed at it; a hundred
/// kilobytes is a checkpoint of a couple of dozen chunks, which is long enough
/// for a stream to still be running when the next take lands and long enough for
/// the network's drops and duplicates to make a receiver ask to start over.
/// PROPOSED(D-043).
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
}

impl Schedule {
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
        // the share changes (D-031). PROPOSED(D-041).
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
        // move when this one changes (D-031). PROPOSED(D-044).
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
        // other arm's dice move when this one changes (D-031). PROPOSED(D-043).
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
                // PROPOSED(D-044): two crashes a round, either side of the
                // catch-up, or one after the grace when the victim is refused.
                Fault::CrashRefused {
                    down,
                    grace,
                    crashes,
                    ..
                } => (FLUSH_WAIT_BUDGET + *grace + *down) * u32::try_from(*crashes).expect("small"),
                // PROPOSED(D-043): the fill, the isolation, the wait for the
                // stream, the freeze behind it and the hold after it.
                Fault::RetakeUnderStream {
                    settle,
                    isolate,
                    freeze,
                    hold,
                    ..
                } => *settle + *isolate + STREAM_WAIT_BUDGET + *freeze + *hold,
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
    /// Which server ran.
    pub variant: Variant,
    /// How the run was scheduled (D-016).
    pub policy: Policy,
    /// The faults it ran.
    pub schedule: Schedule,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// The trace as moirae JSONL.
    pub jsonl: String,
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
    /// stream to re-take under. What the sweep asserts fired. PROPOSED(D-043).
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
}

impl Report {
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
}

impl Report {
    /// The events, without their times.
    #[must_use]
    pub fn events(&self) -> Vec<TraceEvent> {
        self.records.iter().map(|r| r.event.clone()).collect()
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

    /// Whether a majority that can still elect a leader was running at the end:
    /// liveness needs one. A refused server is down until a snapshot re-seeds it,
    /// which its restatement's `RaftRecovered` says (RAFT.md §3) — but a re-seeded
    /// server never votes again (PROPOSED(D-035)), so while it counts for commits
    /// it cannot help elect, and a cluster whose impaired servers reach half has
    /// no leader to wait for: a refused server can only be re-seeded *by* a
    /// leader, so the deadlock is real and priced into D-035, not a liveness
    /// failure. The release run's seed 60 reached exactly that: one server
    /// quarantined by an early re-seed, a second refused by rot, and the last
    /// pre-voting forever with nobody left to grant.
    #[must_use]
    pub fn majority_up(&self) -> bool {
        let mut down: BTreeSet<u64> = BTreeSet::new();
        let mut quarantined: BTreeSet<u64> = BTreeSet::new();
        for record in &self.records {
            match &record.event {
                TraceEvent::RaftRefused { server, .. } => {
                    down.insert(*server);
                }
                TraceEvent::RaftRecovered { server, .. } => {
                    down.remove(server);
                }
                TraceEvent::RaftReseeded { server } => {
                    quarantined.insert(*server);
                }
                _ => {}
            }
        }
        let impaired: BTreeSet<u64> = down.union(&quarantined).copied().collect();
        (impaired.len() as u64) * 2 < SERVERS
    }

    /// How long after the last heal the first client write completed, if one did.
    #[must_use]
    pub fn time_to_write_after_heal(&self) -> Option<Duration> {
        self.history
            .ops
            .iter()
            .filter(|op| op.op.is_write() && op.call >= self.last_heal)
            .filter_map(|op| op.ret)
            .map(|ret| ret.duration_since(self.last_heal))
            .min()
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
        let events = self.events();
        if let Err(violation) = invariants::all(&events) {
            return fail(violation);
        }
        if let Err(violation) = invariants::commit_majority(&events, SERVERS as usize) {
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
        if let Err(violation) = self.isolation_keeps_the_term() {
            return fail(violation);
        }
        if self.uniform() && self.majority_up() {
            let bound = election_max() * LIVENESS_TIMEOUTS;
            match self.time_to_write_after_heal() {
                Some(took) if took <= bound => {}
                Some(took) => {
                    return fail(format!(
                        "liveness: the first client write after the last heal took {took:?}, over {bound:?}"
                    ));
                }
                None => {
                    return fail(format!(
                        "liveness: no client write completed after the last heal at {:?}",
                        self.last_heal
                    ));
                }
            }
            if let Err(violation) = self.timers_fire() {
                return fail(violation);
            }
        }
        Ok(())
    }

    /// Pre-vote (thesis §9.6): a server that receives nothing does not raise its
    /// term. Checked over every isolation the schedule made: the server's term at
    /// the heal equals its term when the isolation began. An isolation during
    /// which the server was refused, re-seeded or finished installing a snapshot
    /// is skipped: an install's restatement re-states the term the stream
    /// carried, which is no election of the isolated server's (RAFT.md §3).
    fn isolation_keeps_the_term(&self) -> Result<(), String> {
        for &(server, from, until) in &self.isolations {
            let reseeding = self.records.iter().any(|r| {
                r.at >= from
                    && r.at <= until
                    && matches!(&r.event,
                        TraceEvent::RaftRefused { server: s, .. }
                        | TraceEvent::RaftReseeded { server: s }
                        | TraceEvent::RaftSnapshot { server: s, taken: false, .. }
                        if *s == server)
            });
            if reseeding {
                continue;
            }
            let term_at = |at: Instant| {
                self.records
                    .iter()
                    .filter(|r| r.at <= at)
                    .filter_map(|r| match &r.event {
                        TraceEvent::RaftTerm {
                            server: s, term, ..
                        } if *s == server => Some(*term),
                        _ => None,
                    })
                    .last()
                    .unwrap_or(0)
            };
            let (before, after) = (term_at(from), term_at(until));
            if after != before {
                return Err(format!(
                    "pre-vote: server {server} raised its term from {before} to {after} while isolated from {from:?} to {until:?}"
                ));
            }
        }
        Ok(())
    }

    /// Election timers fire (moirae rule 5): a running server that is not the
    /// leader campaigns within [`TIMER_TIMEOUTS`] maximum election timeouts of the
    /// last AppendEntries it received from a leader of its term or later, the last
    /// vote it granted, or its start. A re-seeded server is exempt: it never
    /// campaigns on that store, by design (RAFT.md §3, PROPOSED(D-035)).
    fn timers_fire(&self) -> Result<(), String> {
        // A server measures its timeout by its own clock: a slow one takes longer
        // in global time, and the bound scales with its rate.
        let bound_for = |server: u64| -> Duration {
            let ppm = self.schedule.drifts[server as usize - 1];
            let rate = (1_000_000 + ppm) as f64 / 1_000_000.0;
            (election_max() * TIMER_TIMEOUTS).div_f64(rate)
        };
        let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
        let mut up: BTreeSet<u64> = BTreeSet::new();
        let mut leaders: BTreeSet<u64> = BTreeSet::new();
        let mut reseeded: BTreeSet<u64> = BTreeSet::new();
        let mut terms: BTreeMap<u64, u64> = BTreeMap::new();
        let mut last_reset: BTreeMap<u64, Instant> = BTreeMap::new();
        for record in &self.records {
            let at = record.at;
            match &record.event {
                TraceEvent::RaftReseeded { server } => {
                    reseeded.insert(*server);
                }
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDelivered { id, to, .. } => {
                    // Any contact from a leader of the server's term or later resets
                    // its election timer (moirae rule 5): an AppendEntries, whether
                    // its consistency check passes or not, and equally an
                    // InstallSnapshot, which is how a leader reaches a follower whose
                    // next index has fallen below the leader's compacted prefix
                    // (RAFT.md §1). The core routes the snapshot to its own task, but
                    // the follower is hearing from the leader all the same, and its
                    // incarnation's timer stays fresh across the install; a follower
                    // caught up only by a long train of snapshots would otherwise be
                    // read as starved though a leader is feeding it every few
                    // milliseconds. The nightly's seed 164 was exactly that.
                    if let Some(server) = server_of(*to)
                        && let Some(payload) = payloads.get(id)
                        && let Ok(frame) = Frame::decode(payload.clone())
                        && matches!(
                            frame.message,
                            Message::AppendEntries { .. } | Message::InstallSnapshot { .. }
                        )
                        && frame.message.term() >= terms.get(&server).copied().unwrap_or(0)
                    {
                        last_reset.insert(server, at);
                    }
                }
                // PROPOSED(D-039): a completed snapshot install re-states the server
                // and rebuilds its incarnation with a fresh election timer. The
                // install was the leader's doing and the server was busy finishing
                // it, so the restatement counts as the leader's contact here. A crash
                // restart re-states the same way and is reset below when its RaftTerm
                // re-admits it; this arm is for the server that never went down. The
                // nightly's seed 385: cut off alone mid-install, it campaigned a
                // hundred milliseconds after the switch and twenty-five past the bound.
                TraceEvent::RaftRecovered { server, .. } if up.contains(server) => {
                    last_reset.insert(*server, at);
                }
                TraceEvent::RaftTerm { server, term, role } => {
                    terms.insert(*server, *term);
                    if !up.contains(server) {
                        up.insert(*server);
                        last_reset.insert(*server, at);
                    }
                    match *role {
                        "leader" => {
                            leaders.insert(*server);
                        }
                        "pre-candidate" | "candidate" => {
                            leaders.remove(server);
                            last_reset.insert(*server, at);
                        }
                        _ => {
                            // A leader that steps down starts counting from here:
                            // its timer meant nothing while it led.
                            if leaders.remove(server) {
                                last_reset.insert(*server, at);
                            }
                        }
                    }
                }
                TraceEvent::RaftLeader { server, .. } => {
                    leaders.insert(*server);
                }
                TraceEvent::RaftVote {
                    server,
                    granted: true,
                    pre: false,
                    ..
                } => {
                    last_reset.insert(*server, at);
                }
                TraceEvent::NodeCrashed { node } => {
                    let server = u64::from(node.get());
                    up.remove(&server);
                    leaders.remove(&server);
                }
                _ => {}
            }
            for server in &up {
                if leaders.contains(server) || reseeded.contains(server) {
                    continue;
                }
                let since = last_reset.get(server).copied().unwrap_or(at);
                if at.duration_since(since) > bound_for(*server) {
                    return Err(format!(
                        "timers: server {server} heard from no leader of its term and granted no vote since {since:?} and had not campaigned by {at:?}"
                    ));
                }
            }
        }
        Ok(())
    }
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

/// The server configuration for `id` under `variant`.
#[must_use]
pub fn node_config(id: u64, variant: Variant) -> NodeConfig {
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
            variant,
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            drift_bound_ppm: DRIFT_BOUND_PPM,
            snapshot_threshold: 12,
            snapshot_chunk: 4096,
            ..RaftConfig::default()
        },
        engine,
        inbox_capacity: 128,
    }
}

fn spawn_server(sim: &Sim, id: u64, variant: Variant) {
    let env = sim.env(node_of_server(id));
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run_server(inner, node_config(id, variant)).await;
    });
}

/// The leader in force: the server of the latest `RaftLeader` event, or server 1.
///
/// Read back over the trace's tail in windows rather than over a copy of the whole
/// trace (PROPOSED D-046): a fault round asks this of a trace that only grows, and
/// the answer is almost always within the last few hundred records. A window that
/// holds no `RaftLeader` doubles until the trace is exhausted, so the answer is the
/// whole trace's either way.
pub(crate) fn leader_now(sim: &Sim) -> u64 {
    let len = sim.trace_len();
    let mut window = 256;
    loop {
        let from = len.saturating_sub(window);
        let found = sim
            .trace_from(from)
            .iter()
            .rev()
            .find_map(|r| match r.event {
                TraceEvent::RaftLeader { server, .. } => Some(server),
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
    let Ok(sock) = env.net().bind(client_addr(n)).await else {
        return;
    };
    let mut incarnation = 0u64;
    let mut process = n << 32 | incarnation;
    let mut seq = 0u64;
    let mut leader: Option<u64> = None;
    // The server the last abandoned operation went to: not the first to try next.
    let mut avoid: Option<u64> = None;
    let mut known: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
    loop {
        let key = Bytes::from(format!("k{}", env.rng().below(KEYS)));
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
        let mut target = leader.unwrap_or_else(|| {
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
                .send(server_addr(target), request.encode())
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
                        if let Ok(response) = Response::decode(bytes)
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
                leader = Some(target);
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
                leader = None;
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
async fn burst<E: Environment>(env: E, n: u64, target: u64, count: u64) {
    let Ok(sock) = env.net().bind(burst_addr(n)).await else {
        return;
    };
    let process = BURST | n;
    for seq in 0..count {
        let key = Bytes::from_static(BURST_KEY);
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
            .send(server_addr(target), request.encode())
            .await
            .is_err()
        {
            return;
        }
    }
}

/// The re-take driver's filling puts (PROPOSED(D-043)): `count` puts of
/// [`SPREAD_VALUE_BYTES`] bytes each, every one on its own key, fired at server
/// `target` without awaiting replies as the schedule's `n`th driver. They are
/// there for their size, not their outcome: the state machine the two clients
/// build is two keys of a dozen bytes, whose checkpoint is a single chunk and
/// whose install is over in a round trip, and nothing can be aimed at a stream
/// that short. Each key is written once and never read, so the checker's
/// per-key search sees one put that always applies, as [`burst`]'s do.
async fn spread<E: Environment>(env: E, n: u64, target: u64, count: u64) {
    let Ok(sock) = env.net().bind(spread_addr(n)).await else {
        return;
    };
    let process = SPREAD | n;
    for seq in 0..count {
        let key = Bytes::from(format!("f{n}.{seq}"));
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
            .send(server_addr(target), request.encode())
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Runs the scenario for `seed` with the schedule drawn from it.
#[must_use]
pub fn run(seed: u64, variant: Variant) -> Report {
    run_with(seed, Schedule::draw(seed), variant)
}

/// Runs the scenario for `seed` with an explicit schedule.
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule, variant: Variant) -> Report {
    let mut sim = Sim::new(config(seed, &schedule));
    let servers: Vec<NodeId> = (0..SERVERS as usize)
        .map(|i| sim.add_node_with_clock(schedule.skews[i], schedule.drifts[i]))
        .collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    let stats: Vec<SharedStats> = (0..CLIENTS).map(|_| SharedStats::default()).collect();
    for id in 1..=SERVERS {
        spawn_server(&sim, id, variant);
    }
    for (i, &node) in clients.iter().enumerate() {
        let env = sim.env(node);
        let inner = env.clone();
        let stats = stats[i].clone();
        env.spawn("client", client(inner, i as u64 + 1, SERVERS, stats));
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
    let mut watch = Watch::default();
    advance(&mut sim, schedule.warmup, &mut watch);
    let restart = |sim: &mut Sim, server: u64| {
        sim.restart(node_of_server(server));
        spawn_server(sim, server, variant);
    };
    // The lease trials: the operator hands leadership to the slowest clock, which
    // leads for a while and is then cut off with client 1; twice, so the variant's
    // catch rate is not one window's noise.
    let slowest = schedule.slowest();
    let mut trials_led_by_slowest = 0;
    let mut last_heal = sim.now();
    for (n, trial) in schedule.trials.iter().enumerate() {
        if watch.stopped.is_some() {
            break;
        }
        let leader = leader_now(&sim);
        if leader != slowest {
            let env = sim.env(admin);
            let inner = env.clone();
            let seq = n as u64;
            env.spawn("admin", async move {
                let Ok(sock) = inner.net().bind(admin_addr(seq + 1)).await else {
                    return;
                };
                let request = Request {
                    client: ADMIN,
                    seq,
                    command: Command::Transfer { to: slowest },
                };
                let _ = sock.send(server_addr(leader), request.encode()).await;
            });
            advance(&mut sim, TRANSFER_WAIT, &mut watch);
        }
        advance(&mut sim, trial.settle, &mut watch);
        let leader = leader_now(&sim);
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
    for (fault, gap) in schedule.faults.iter().zip(schedule.gaps.iter()) {
        if watch.stopped.is_some() {
            break;
        }
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
                // PROPOSED(D-041): the install crash's setup, then the crashes
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
                // PROPOSED(D-044): each round crashes the victim inside a flush
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
                // PROPOSED(D-043): a state machine worth streaming, then the
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
                        spread(inner, n, target, puts).await;
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
                    burst(inner, n, target, puts).await;
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
        variant,
        policy: sim.policy(),
        schedule,
        jsonl: sim
            .to_moirae(&Export::new(&message::studio))
            .expect("the raft trace exports to moirae v2"),
        records,
        last_heal,
        isolations,
        trials_led_by_slowest,
        aimed_streams,
        refused,
        stopped: watch.stopped,
        history,
        clients: clients_total,
    }
}

/// What the sliced advance watches for.
struct Watch {
    slices: u32,
    stopped: Option<String>,
    /// The safety checks, one checker for the whole run with the state of each
    /// check kept across looks, so a look costs only the records since the last
    /// one (PROPOSED D-046).
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
/// the reason the checker keeps its state (PROPOSED D-046).
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
/// watch reads only the records since its last look (PROPOSED D-046) and the trace
/// cap still stops a runaway. PROPOSED(D-041).
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
/// the small slices and the trace cap still stops a runaway. PROPOSED(D-043).
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

/// Reads the trace from `scanned` on into `refused`, the servers sitting refused
/// for lost state: refused ([`TraceEvent::RaftRefused`]) with no restatement
/// since, however long ago, since a refused server comes back only through an
/// install (RAFT.md §3). What [`Fault::CrashRefused`] asks of its victim before
/// each round. PROPOSED(D-044).
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
/// PROPOSED D-044 stops. The safety folds are skipped inside the small slices,
/// as in [`install_landing`], and the trace cap still stops a runaway.
/// PROPOSED(D-044).
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
/// staging directory. PROPOSED(D-041).
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
/// PROPOSED(D-041).
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
/// trace once rather than once per look (PROPOSED D-046). What it reports is what
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
            watch.checker.extend(records.iter().map(|r| &r.event));
            if let Err(violation) = watch.checker.verdict() {
                watch.stopped = Some(format!("{violation} (at {:?})", sim.now()));
                return;
            }
        }
    }
}

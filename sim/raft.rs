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
    ClientOp, ClientResult, Clock, Either, Environment, Instant, Network, NodeId, Rng, Socket,
    TraceEvent, race,
};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::message::{self, Frame, Message};
use ananke_raft::store::LOST_STATE;
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
        }
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
}

impl Report {
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
    /// server never votes again (D-035), so while it counts for commits
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
            if let Err(violation) = self.liveness() {
                return fail(violation);
            }
            if let Err(violation) = self.timers_fire() {
                return fail(violation);
            }
        }
        Ok(())
    }

    /// Liveness: on a uniform run with a majority up, a client write completes
    /// within [`LIVENESS_TIMEOUTS`] maximum election timeouts of the last heal.
    fn liveness(&self) -> Result<(), String> {
        let bound = election_max() * LIVENESS_TIMEOUTS;
        match self.time_to_write_after_heal() {
            Some(took) if took <= bound => Ok(()),
            Some(took) => Err(format!(
                "liveness: the first client write after the last heal took {took:?}, over {bound:?}"
            )),
            None => Err(format!(
                "liveness: no client write completed after the last heal at {:?}",
                self.last_heal
            )),
        }
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
        let live = self.uniform() && self.majority_up();
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
        let verdict = Self::keeps_its_term_by(records, RecordTime::Decided, server, from, until);
        if verdict.is_ok() {
            return verdict;
        }
        let changes = Self::term_changes_decided_in(records, server, from, until);
        let caused_before = !changes.is_empty()
            && changes
                .iter()
                .all(|r| r.received().is_some_and(|received| received <= from));
        if caused_before { Ok(()) } else { verdict }
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
        Self::term_changes_decided_in(&self.pre_vote_records(), server, from, until)
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
        server: u64,
        from: Instant,
        until: Instant,
    ) -> Vec<&'a TraceRecord> {
        let mut previous = 0;
        let mut changes = Vec::new();
        for &record in records {
            let TraceEvent::RaftTerm {
                server: s, term, ..
            } = &record.event
            else {
                continue;
            };
            if *s != server {
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
        if Self::reseeding_during(records, server, from, until) {
            return Ok(());
        }
        let (before, after) = (
            Self::term_by(records, server, time, from),
            Self::term_by(records, server, time, until),
        );
        if after != before {
            return Err(format!(
                "pre-vote: server {server} raised its term from {before} to {after} while isolated from {from:?} to {until:?}"
            ));
        }
        Ok(())
    }

    /// Whether `server` was refused, re-seeded or finished installing a snapshot
    /// while isolated from `from` to `until`, by when those records were traced:
    /// the pre-vote check's skip; from `records`, the [`Report::pre_vote_records`].
    fn reseeding_during(
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
                    | TraceEvent::RaftReseeded { server: s }
                    | TraceEvent::RaftSnapshot { server: s, taken: false, .. }
                    if *s == server)
        })
    }

    /// `server`'s term at `at`: its last `RaftTerm` whose `time` is at or before
    /// `at`, 0 before any. A server's term records come from one task's steps in
    /// sequence, each decided after the one before was traced, so their decision
    /// times rise with their order just as their durability times do, and the last
    /// such record is the latest either way. From `records`, the
    /// [`Report::pre_vote_records`].
    fn term_by(records: &[&TraceRecord], server: u64, time: RecordTime, at: Instant) -> u64 {
        records
            .iter()
            .filter(|r| time.of(r) <= at)
            .filter_map(|r| match &r.event {
                TraceEvent::RaftTerm {
                    server: s, term, ..
                } if *s == server => Some(*term),
                _ => None,
            })
            .next_back()
            .unwrap_or(0)
    }

    /// Election timers fire (moirae rule 5): a running server that is not the
    /// leader campaigns within [`TIMER_TIMEOUTS`] maximum election timeouts of the
    /// last AppendEntries it received from a leader of its term or later, the last
    /// vote it granted, or its start. A re-seeded server is exempt: it never
    /// campaigns on that store, by design (RAFT.md §3, D-035). The first
    /// gap [`Report::replay_timers`] finds under every reset arm is the violation.
    ///
    /// Whether a server campaigned in time is about when it decided to, so the
    /// replay reads every record by its decision time (D-047).
    fn timers_fire(&self) -> Result<(), String> {
        self.timers_fire_by(RecordTime::Decided)
    }

    /// The timer check with the records read by `time`: under
    /// [`RecordTime::Durable`], the check as it stood before D-047.
    // D-047: every trace record carries its decision time and its durability time.
    fn timers_fire_by(&self, time: RecordTime) -> Result<(), String> {
        let mut first = None;
        self.replay_timers(
            TimerResets::ALL,
            time,
            |gap| {
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
        probe: Option<(usize, u64)>,
    ) -> Option<TimerState> {
        // A server measures its timeout by its own clock: a slow one takes longer
        // in global time, and the bound scales with its rate.
        let bound_for = |server: u64| self.timer_bound(server);
        let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
        let mut up: BTreeSet<u64> = BTreeSet::new();
        let mut leaders: BTreeSet<u64> = BTreeSet::new();
        let mut reseeded: BTreeSet<u64> = BTreeSet::new();
        let mut terms: BTreeMap<u64, u64> = BTreeMap::new();
        let mut clocks = TimerClocks::default();
        let mut reported: BTreeMap<u64, Instant> = BTreeMap::new();
        let mut probed = None;
        // D-047: the replay's order; stable, so ties keep record order.
        let mut order: Vec<(usize, &TraceRecord)> = self.records.iter().enumerate().collect();
        order.sort_by_key(|(_, record)| time.of(record));
        for (index, record) in order {
            let at = time.of(record);
            clocks.replaying = index;
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
                    // incarnation's timer stays fresh across the install. The chunk
                    // counts by its term, not by who sends it: seed 164, found by a
                    // local ten-thousand-seed run on 1373601, was a follower whose
                    // chunks kept arriving from a deposed leader after it lost its
                    // quorum, with no leader in the cluster at all
                    // (`Report::snapshot_fed_timer_gaps`).
                    if let Some(server) = server_of(*to)
                        && let Some(payload) = payloads.get(id)
                        && let Ok(frame) = Frame::decode(payload.clone())
                        && frame.message.term() >= terms.get(&server).copied().unwrap_or(0)
                    {
                        match frame.message {
                            Message::AppendEntries { .. } => clocks.reset(server, at),
                            Message::InstallSnapshot { .. } if resets.install_snapshot => {
                                clocks.reset(server, at);
                            }
                            Message::InstallSnapshot { .. } => {
                                *clocks.installs.entry(server).or_default() += 1;
                            }
                            _ => {}
                        }
                    }
                }
                // D-039: a completed snapshot install re-states the server
                // and rebuilds its incarnation with a fresh election timer. The
                // install was the leader's doing and the server was busy finishing
                // it, so the restatement counts as the leader's contact here. A crash
                // restart re-states the same way and is reset below when its RaftTerm
                // re-admits it; this arm is for the server that never went down. Seed
                // 385, found by a local ten-thousand-seed run on f54b468: cut off
                // alone mid-install, it campaigned a hundred milliseconds after the
                // switch and twenty-five past the bound
                // (`Report::timer_gaps_rescued_by_restatement`).
                TraceEvent::RaftRecovered { server, .. } if up.contains(server) => {
                    if resets.restatement {
                        clocks.reset(*server, at);
                    } else {
                        *clocks.restatements.entry(*server).or_default() += 1;
                    }
                }
                TraceEvent::RaftTerm {
                    server, term, role, ..
                } => {
                    terms.insert(*server, *term);
                    if !up.contains(server) {
                        up.insert(*server);
                        clocks.reset(*server, at);
                    }
                    match *role {
                        "leader" => {
                            leaders.insert(*server);
                        }
                        "pre-candidate" | "candidate" => {
                            leaders.remove(server);
                            clocks.reset(*server, at);
                        }
                        _ => {
                            // A leader that steps down starts counting from here:
                            // its timer meant nothing while it led.
                            if leaders.remove(server) {
                                clocks.reset(*server, at);
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
                    clocks.reset(*server, at);
                }
                TraceEvent::NodeCrashed { node } => {
                    let server = u64::from(node.get());
                    up.remove(&server);
                    leaders.remove(&server);
                }
                _ => {}
            }
            let mut flagged = BTreeSet::new();
            for server in &up {
                if leaders.contains(server) || reseeded.contains(server) {
                    continue;
                }
                let since = clocks.last_reset.get(server).copied().unwrap_or(at);
                if at.duration_since(since) > bound_for(*server)
                    && reported.get(server) != Some(&since)
                {
                    reported.insert(*server, since);
                    flagged.insert(*server);
                    let found = TimerGap {
                        server: *server,
                        since,
                        at,
                        record: index,
                        installs: clocks.installs.get(server).copied().unwrap_or(0),
                        restatements: clocks.restatements.get(server).copied().unwrap_or(0),
                    };
                    if gap(found).is_break() {
                        return probed;
                    }
                }
            }
            if let Some((target, server)) = probe
                && target == index
            {
                probed = Some(TimerState {
                    up: up.contains(&server),
                    leader: leaders.contains(&server),
                    reseeded: reseeded.contains(&server),
                    since: clocks.last_reset.get(&server).copied(),
                    since_record: clocks.last_reset_record.get(&server).copied(),
                    flagged: flagged.contains(&server),
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
}

impl TimerResets {
    /// Every arm: the check [`Report::check`] makes.
    pub const ALL: Self = Self {
        install_snapshot: true,
        restatement: true,
    };
    /// Neither arm: the check as it stood on 1373601, which read only
    /// AppendEntries as a leader's contact.
    pub const APPEND_ENTRIES_ONLY: Self = Self {
        install_snapshot: false,
        restatement: false,
    };
    /// Every arm but D-039's: the check as it stood on f54b468.
    pub const WITHOUT_RESTATEMENT: Self = Self {
        install_snapshot: true,
        restatement: false,
    };
}

/// One stretch in which a running follower, neither leading nor re-seeded, went
/// past its timer bound in the timer check's replay ([`Report::timer_gaps`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerGap {
    /// The server.
    pub server: u64,
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
}

impl TimerGap {
    /// The timer check's words for this gap.
    #[must_use]
    // PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
    // names.
    pub fn violation(&self) -> String {
        format!(
            "timers: server {} heard from no leader of its term and granted no vote since {:?} and had not campaigned by {:?}",
            self.server, self.since, self.at
        )
    }
}

/// The per-server clocks of the timer replay, and what arrived since each reset
/// that the replay's arms did not count as one.
#[derive(Default)]
struct TimerClocks {
    last_reset: BTreeMap<u64, Instant>,
    /// The index in [`Report::records`] of the record behind each `last_reset`.
    last_reset_record: BTreeMap<u64, usize>,
    installs: BTreeMap<u64, usize>,
    restatements: BTreeMap<u64, usize>,
    /// The index of the record being replayed.
    replaying: usize,
}

impl TimerClocks {
    fn reset(&mut self, server: u64, at: Instant) {
        self.last_reset.insert(server, at);
        self.last_reset_record.insert(server, self.replaying);
        self.installs.remove(&server);
        self.restatements.remove(&server);
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
        let (server, flag) = (gap.server, gap.record);
        let x = self
            .records
            .get(flag)
            .ok_or_else(|| format!("the flag record {flag} is not in the trace"))?;
        let state = |time| {
            self.replay_timers(
                TimerResets::ALL,
                time,
                |_| ControlFlow::Continue(()),
                Some((flag, server)),
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
                    TraceEvent::RaftTerm { server: s, .. }
                    | TraceEvent::RaftLeader { server: s, .. }
                    | TraceEvent::RaftReseeded { server: s } => *s == server,
                    TraceEvent::NodeCrashed { node } => u64::from(node.get()) == server,
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
            if Self::reseeding_during(&pre_vote, server, from, until) {
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
            if Self::reseeding_during(&pre_vote, server, from, until) {
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

    /// Every snapshot a server took: a checkpoint written on its node and, at the
    /// same instant, its `RaftSnapshot` with `taken` set. A restart's restatement
    /// of the record's snapshot writes no checkpoint and is not a take.
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
                TraceEvent::RaftSnapshot {
                    server,
                    last_index,
                    taken: true,
                    ..
                } => {
                    if let Some((at, dir)) = written.remove(server)
                        && at == record.at
                    {
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
                TraceEvent::RaftReseeded { server } => *server == follower,
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
            &|e| matches!(e, TraceEvent::RaftReseeded { server } if *server == follower),
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
            snapshot_threshold: 12,
            snapshot_chunk: 4096,
            ..RaftConfig::default()
        },
        engine,
        inbox_capacity: 128,
    }
}

fn spawn_server(sim: &Sim, id: u64, variants: Variants) {
    let env = sim.env(node_of_server(id));
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run_server(inner, node_config(id, variants)).await;
    });
}

/// The leader in force: the server of the latest `RaftLeader` event, or server 1.
///
/// Read back over the trace's tail in windows rather than over a copy of the whole
/// trace (D-046): a fault round asks this of a trace that only grows, and
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

/// The re-take driver's filling puts (D-043): `count` puts of
/// [`SPREAD_VALUE_BYTES`] bytes each, every one on its own key, fired at server
/// `target` without awaiting replies as the schedule's `n`th driver. They are
/// there for their size, not their outcome: the state machine the two clients
/// build is two keys of a dozen bytes, whose checkpoint is a single chunk and
/// whose install is over in a round trip, and nothing can be aimed at a stream
/// that short. Each key is written once and never read, so the checker's
/// per-key search sees one put that always applies, as [`burst`]'s do.
pub(crate) async fn spread<E: Environment>(env: E, n: u64, target: u64, count: u64) {
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
    let variants = variants.into();
    let mut sim = Sim::new(config(seed, &schedule));
    let servers: Vec<NodeId> = (0..SERVERS as usize)
        .map(|i| sim.add_node_with_clock(schedule.skews[i], schedule.drifts[i]))
        .collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    let stats: Vec<SharedStats> = (0..CLIENTS).map(|_| SharedStats::default()).collect();
    for id in 1..=SERVERS {
        spawn_server(&sim, id, variants);
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
        spawn_server(sim, server, variants);
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
                            let _ = sock.send(server_addr(leader), request.encode()).await;
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
            watch.checker.extend(records.iter().map(|r| &r.event));
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
        }
    }

    fn vote(server: u64) -> TraceEvent {
        TraceEvent::RaftVote {
            server,
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
}

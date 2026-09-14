//! The protocol core (RAFT.md §1 and §3): Figure 2 of the paper with pre-vote
//! (thesis §9.6), a no-op on election (thesis §6.4), batching and pipelining with
//! moirae's deviation D1, as a pure state machine. Nothing here does I/O.
//!
//! [`Raft::step`] takes one [`Input`] and returns [`Output`]s in order. The server
//! executes them in that order and completes a [`Output::Persist`] before acting on
//! anything after it: the persist always comes first, and the messages that depend
//! on it after. That order is the persistence discipline of Figure 2, and the server
//! that breaks it is a variant the sweep must catch.
//!
//! The log lives in the core as a vector for now; the store keeps the durable copy
//! and the server hands the log back at restart. The election timeout is drawn from
//! a small generator seeded by the server from its protocol stream, so the core stays
//! a function of its inputs and its seed.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use ananke_env::TraceEvent;
use bytes::Bytes;

use crate::message::Message;
use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// The known-buggy variants beside the correct one (RAFT.md §5). Each breaks one
/// rule; the sweep must catch each and pass the correct one. The core enforces the
/// rules of the protocol; the server (`node.rs`) enforces the two disciplines that
/// are about I/O order, [`Variant::SendBeforePersist`] and
/// [`Variant::ApplyBeforeCommit`], and the core ignores those.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Variant {
    /// Figure 2 with pre-vote, as RAFT.md §1 says.
    #[default]
    Correct,
    /// The server sends a step's messages before the step's persist is durable
    /// (Figure 2, thesis §3.8): a crash between the two lets a leader count an entry
    /// on a follower that never had it.
    SendBeforePersist,
    /// The server applies entries as they are appended, not as they are committed
    /// (Figure 2): a follower or a deposed leader applies an entry that is later
    /// truncated, and a client is told its write happened.
    ApplyBeforeCommit,
    /// Elections start without a pre-vote round (thesis §9.6): a rejoining server
    /// raises the term and deposes a working leader.
    NoPreVote,
    /// The commit index advances for any entry replicated on a majority, whatever
    /// its term (§5.4.2, Figure 8): a later leader can overwrite it.
    CountOlderTermForCommit,
    /// A follower truncates from the previous index on every AppendEntries, not
    /// only at a conflict (moirae rule 3): a duplicated older request deletes
    /// committed entries.
    TruncateOnEveryAppend,
    /// The election timer resets on any message (moirae rule 5): elections stall.
    ResetTimerOnAnyRpc,
    /// The election restriction compares last indices before last terms (§5.4.1):
    /// a longer stale log wins.
    IndexFirstElectionRestriction,
    /// The leader trusts every follower's promise without the drift guard (RAFT.md
    /// §1): a follower whose clock runs fast times out before the leader thinks
    /// its lease ends, and a lease read is served stale.
    LeaseTrustsTheClock,
    /// While a joint configuration is in force, elections and commits count one
    /// majority of the two voter sets merged, not a majority of each (thesis
    /// §4.3): a majority of the union need not contain a majority of `C_old` or
    /// of `C_new`, so a server still on `C_old` can commit or elect against a
    /// disjoint majority, which is the split brain joint consensus exists to
    /// forbid. Caught during 3 → 5 → 3 under partition by commit majority,
    /// election safety or leader completeness.
    SingleMajorityInJointConsensus,
    /// The server installing a snapshot writes the staging directory's `CURRENT`
    /// before the rest of the staged store is durable (RAFT.md §1, D-024): a crash
    /// mid-install then comes back on a store that opens but was never repaired,
    /// the leader's tenant 0 in place of the receiver's, and state machine safety
    /// catches the state that never existed after the restart. The install order is
    /// the server's business (`snapshot.rs`); the core ignores this variant.
    SnapshotWithoutCurrentLast,
    /// The adoption of a staged install as it was built under D-038, before
    /// D-041: the old store's `CURRENT` and files are removed before
    /// the staged copies and their directory entries are durable, a staging
    /// `CURRENT` that exists but does not parse is swept as debris, and a store
    /// directory emptied that way opens as a fresh store, since nothing marks it
    /// as one. A crash inside the copy whose bit rot lands on the staging
    /// `CURRENT` then restarts the server on an empty store: a voter forgets its
    /// term, its vote and its committed entries, which committed-entries-stay
    /// reports at the restatement (the nightly's seed 6325). The adoption is the
    /// server's business (`snapshot.rs`, `node.rs`); the core ignores this
    /// variant.
    // D-041: the crash-safe adoption and the store identity marker.
    AdoptionAsBuilt,
    /// The leader ignores the store incarnation its followers answer with
    /// (RAFT.md §3): the leader as built before D-042. A follower re-seeded
    /// from a snapshot comes back with a log shorter than what it had
    /// acknowledged, the leader's match index for it is monotone by design
    /// (D-026) and its probe never goes below the match, so every rejection is
    /// discarded, the follower is never designated snapshot-fed and never
    /// counted again — until the leader changes. With the other follower
    /// unavailable, commits stall: the sweep's liveness check.
    // D-042: store incarnations.
    IgnoreIncarnation,
    /// The leader's snapshot takes share one mutable directory per index,
    /// rewritten by every take under whatever stream is reading it, and the
    /// leader streams to one designated follower at a time, every other one
    /// queued behind it: the behaviour as built before D-043. A take
    /// at an index already recorded — the retake a failed stream asks for —
    /// sweeps the directory a running stream reads, so sender and receiver fall
    /// out of step and the stream never completes; a second designated follower
    /// waits behind it and gets no entries either, so neither can be counted,
    /// the leader loses its quorum and nothing commits, which the sweep's
    /// liveness check reports (nightly run 34496762339, seed 5909). The
    /// directories and the streams are the server's business (`snapshot.rs`,
    /// `node.rs`); the core ignores this variant.
    SharedSnapshotDir,
    /// A refusal that lives only in the running process, and a refused engine
    /// that keeps working: the server as built before D-044. Nothing
    /// records the loss in the store directory, so the next start decides afresh
    /// on whatever the refused engine has since made of the disk; and that
    /// engine, opened on a recovery which dropped a table, still flushes the
    /// memtable the recovery replayed — a manifest without the dropped table,
    /// `CURRENT` switched to it, and the log segments that held the lost records
    /// deleted. The store is self-consistent by then, so a crash and restart
    /// opens it clean: a voter with a hole in its state machine restates
    /// `RaftRecovered`, rejoins and pre-votes, which state machine safety
    /// reports at the restatement whose log cannot account for the applied index
    /// it recovered (the thousand-seed premerge, seed 687). The marker and the
    /// quiesce are the server's business (`store.rs`, `node.rs`,
    /// `ananke-storage`); the core ignores this variant.
    // D-044: a durable refusal, and a refused engine that does no work.
    RefusalNotDurable,
    /// A refused follower's rejection counts for check quorum whatever the
    /// leader's re-seed stream to it does: the leader as built before D-049. A
    /// refused server answers every AppendEntries (RAFT.md §3), so with the
    /// leader's other follower away a leader whose stream to the refused one has
    /// stalled keeps its office on answers that can never make a commit, and
    /// nobody else can be elected while it holds it. The directed re-seed scenario
    /// blocks the stream and catches the leader that does not step down. The
    /// counting is the core's; the stream's acknowledgements reach it from the
    /// server (`node.rs`).
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    RefusedCountsForQuorum,
    /// Nothing from a refused follower counts for check quorum, however its
    /// re-seed stream progresses: the alternative D-049 rejected. With the
    /// leader's other follower away, the leader steps down mid-re-seed at the
    /// first window it hears only from the follower it is re-seeding, and since a
    /// re-seeded server never votes (D-035) no leader can be elected until the
    /// third server returns. The directed re-seed scenario leaves the stream open
    /// and catches the leader that steps down before the install completes. In a
    /// set with [`Variant::RefusedCountsForQuorum`], that variant's counting wins.
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    RefusedNeverCounts,
}

impl Variant {
    /// Every buggy variant, in declaration order: the vocabulary a [`Variants`]
    /// set is drawn from. [`Variant::Correct`] is not a member — it is the
    /// absence of all of them — so this is what [`Variants`]'s `Debug` walks.
    // D-045: a variant is a set.
    pub const BUGS: &'static [Variant] = &[
        Variant::SendBeforePersist,
        Variant::ApplyBeforeCommit,
        Variant::NoPreVote,
        Variant::CountOlderTermForCommit,
        Variant::TruncateOnEveryAppend,
        Variant::ResetTimerOnAnyRpc,
        Variant::IndexFirstElectionRestriction,
        Variant::LeaseTrustsTheClock,
        Variant::SingleMajorityInJointConsensus,
        Variant::SnapshotWithoutCurrentLast,
        Variant::AdoptionAsBuilt,
        Variant::IgnoreIncarnation,
        Variant::SharedSnapshotDir,
        Variant::RefusalNotDurable,
        Variant::RefusedCountsForQuorum,
        Variant::RefusedNeverCounts,
    ];

    /// This variant's bit in a [`Variants`] set. [`Variant::Correct`] owns no
    /// bit: the correct server is the empty set. The match is exhaustive on
    /// purpose, so a new variant does not compile until it has a bit.
    // D-045: a variant is a set.
    const fn bit(self) -> u32 {
        match self {
            Variant::Correct => 0,
            Variant::SendBeforePersist => 1 << 0,
            Variant::ApplyBeforeCommit => 1 << 1,
            Variant::NoPreVote => 1 << 2,
            Variant::CountOlderTermForCommit => 1 << 3,
            Variant::TruncateOnEveryAppend => 1 << 4,
            Variant::ResetTimerOnAnyRpc => 1 << 5,
            Variant::IndexFirstElectionRestriction => 1 << 6,
            Variant::LeaseTrustsTheClock => 1 << 7,
            Variant::SingleMajorityInJointConsensus => 1 << 8,
            Variant::SnapshotWithoutCurrentLast => 1 << 9,
            Variant::AdoptionAsBuilt => 1 << 10,
            Variant::IgnoreIncarnation => 1 << 11,
            Variant::SharedSnapshotDir => 1 << 12,
            Variant::RefusalNotDurable => 1 << 13,
            Variant::RefusedCountsForQuorum => 1 << 14,
            Variant::RefusedNeverCounts => 1 << 15,
        }
    }
}

/// Which known bugs one server carries at once (D-045): a set of
/// [`Variant`]s, held as a bitmask so it is `Copy`, cheap and deterministic —
/// no hashing and no allocation on a path every step of the core walks.
///
/// The empty set is the correct server, so [`Variants::default`] is correct and
/// [`Variants::is_correct`] asks whether the set is empty. The set exists so the
/// sweep can run a server carrying two bugs at once, which a single-enum
/// `Variant` could not. It was built on the reading that the nightly's seed 5909
/// needed a stale `matched` for a re-seeded follower (D-042) and a
/// never-completing snapshot stream to the other follower (D-043)
/// together; measured on that trace since, the wedge was D-043's alone — the
/// leader in force had rebuilt its progress at `matched: 0`, and the second
/// follower was queued behind the scrambled stream — so no wedge that needs two
/// bugs has yet been seen, and the set is what lets the sweep ask for one.
///
/// A single variant stays ergonomic: `From<Variant>` converts, and every entry
/// point that takes a server's bugs takes `impl Into<Variants>`, so
/// `raft::run(seed, Variant::Correct)` reads as it always did.
///
/// ```
/// use ananke_raft::core::{Variant, Variants};
///
/// let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
/// assert!(both.contains(Variant::IgnoreIncarnation));
/// assert!(both.contains(Variant::SharedSnapshotDir));
/// assert!(!both.contains(Variant::NoPreVote));
/// assert!(!both.is_correct());
/// assert_eq!(format!("{both:?}"), "{IgnoreIncarnation, SharedSnapshotDir}");
///
/// let one: Variants = Variant::NoPreVote.into();
/// assert!(one.contains(Variant::NoPreVote));
/// assert_eq!(format!("{one:?}"), "{NoPreVote}");
///
/// assert!(Variants::default().is_correct());
/// assert_eq!(format!("{:?}", Variants::default()), "Correct");
/// ```
// D-045: a variant is a set.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Variants(u32);

impl Variants {
    /// The correct server: no bug at all.
    #[must_use]
    pub const fn correct() -> Self {
        Self(0)
    }

    /// The set of these variants. [`Variant::Correct`] contributes nothing, so
    /// `Variants::of(&[Variant::Correct])` is the correct server.
    #[must_use]
    pub const fn of(variants: &[Variant]) -> Self {
        let mut bits = 0;
        let mut i = 0;
        while i < variants.len() {
            bits |= variants[i].bit();
            i += 1;
        }
        Self(bits)
    }

    /// Whether this server carries `variant`. Asking for [`Variant::Correct`]
    /// asks whether the set is empty, which is the same question as
    /// [`Variants::is_correct`].
    #[must_use]
    pub const fn contains(self, variant: Variant) -> bool {
        match variant {
            Variant::Correct => self.is_correct(),
            other => self.0 & other.bit() != 0,
        }
    }

    /// Whether this is the correct server: the empty set.
    #[must_use]
    pub const fn is_correct(self) -> bool {
        self.0 == 0
    }

    /// This set with `variant` added.
    #[must_use]
    pub const fn with(self, variant: Variant) -> Self {
        Self(self.0 | variant.bit())
    }

    /// How many bugs this server carries.
    #[must_use]
    pub const fn len(self) -> u32 {
        self.0.count_ones()
    }

    /// Whether this server carries no bug: [`Variants::is_correct`] under the
    /// name clippy expects beside [`Variants::len`].
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.is_correct()
    }

    /// The variants in the set, in [`Variant::BUGS`] order.
    pub fn iter(self) -> impl Iterator<Item = Variant> {
        Variant::BUGS
            .iter()
            .copied()
            .filter(move |&variant| self.contains(variant))
    }
}

impl From<Variant> for Variants {
    fn from(variant: Variant) -> Self {
        Self(variant.bit())
    }
}

/// Readable, because the sweep's rate lines print it: `Correct` for the empty
/// set, `{IgnoreIncarnation, SharedSnapshotDir}` for a pair.
// D-045: a variant is a set.
impl fmt::Debug for Variants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_correct() {
            return f.write_str("Correct");
        }
        f.write_str("{")?;
        for (i, variant) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{variant:?}")?;
        }
        f.write_str("}")
    }
}

/// The core's parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftConfig {
    /// The election timeout in ticks, drawn per election from `[min, max)`.
    pub election_ticks: (u64, u64),
    /// A leader sends heartbeats every this many ticks.
    pub heartbeat_ticks: u64,
    /// Entries per AppendEntries.
    pub max_batch: usize,
    /// AppendEntries with entries in flight per follower.
    pub max_inflight: usize,
    /// How long one tick is, in nanoseconds: what the lease's arithmetic on the
    /// election timeout is measured in. The server ticks the core at this rate.
    pub tick_nanos: u64,
    /// The bound on the rate at which two clocks may drift apart, in parts per
    /// million: what the lease assumes and what the guard watches for (RAFT.md §1).
    pub drift_bound_ppm: u64,
    /// How long the guard observes a follower's offset before comparing it, in
    /// nanoseconds: the window over which the fastest response is taken.
    pub guard_window_nanos: u64,
    /// Taken off every lease, in nanoseconds, beyond the drift bound: the timer's
    /// tick granularity at the follower.
    pub lease_margin_nanos: u64,
    /// A leader takes a snapshot when its log holds more than this many entries
    /// past the last snapshot it took (RAFT.md §1).
    pub snapshot_threshold: u64,
    /// How many bytes of a checkpoint's file one InstallSnapshot chunk carries;
    /// must stay under `ananke_env::MAX_FRAME_LEN` with the frame's own fields.
    pub snapshot_chunk: usize,
    /// Which core to run: the set of bugs this server carries, empty for the
    /// correct one (D-045).
    // D-045: a variant is a set.
    pub variants: Variants,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_ticks: (10, 20),
            heartbeat_ticks: 2,
            max_batch: 64,
            max_inflight: 8,
            tick_nanos: 10_000_000,
            drift_bound_ppm: 1_000,
            guard_window_nanos: 400_000_000,
            lease_margin_nanos: 10_000_000,
            snapshot_threshold: 4096,
            snapshot_chunk: 256 * 1024,
            variants: Variants::correct(),
        }
    }
}

impl RaftConfig {
    /// How long a follower's promise holds from the moment the acknowledged request
    /// was sent, by the leader's clock (RAFT.md §1): the minimum election timeout
    /// less one tick, scaled down by the drift bound, less the margin.
    #[must_use]
    pub fn lease_span_nanos(&self) -> u64 {
        let ticks = self.election_ticks.0.saturating_sub(1);
        let span = ticks * self.tick_nanos;
        let scaled = span - span * self.drift_bound_ppm / 1_000_000;
        scaled.saturating_sub(self.lease_margin_nanos)
    }
}

/// What a server can be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Following a leader, or waiting for one.
    Follower,
    /// Asking whether an election would succeed.
    PreCandidate,
    /// In an election.
    Candidate,
    /// The leader of its term.
    Leader,
}

impl Role {
    fn name(self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::PreCandidate => "pre-candidate",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

/// What the core asks the server to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    /// A message arrived.
    Message {
        /// Who sent it.
        from: ServerId,
        /// The message.
        message: Message,
        /// The server's clock, in nanoseconds, when it arrived.
        now: u64,
    },
    /// One tick of the server's timer passed.
    Tick,
    /// A client proposes a command.
    Propose(Bytes),
    /// An operator asks the leader to hand leadership to a follower (thesis
    /// §3.10): the leader sends it TimeoutNow once its log is caught up.
    Transfer(ServerId),
    /// A client asks for a linearizable read (RAFT.md §1), answered with
    /// [`Output::ReadReady`] once the state to read is applied.
    Read {
        /// The server's id for the request.
        id: u64,
        /// The server's clock, in nanoseconds, when it arrived.
        now: u64,
    },
    /// An operator asks for a membership change (RAFT.md §1, thesis §4.3): make
    /// these servers the voters. The leader catches servers being added up as
    /// learners, proposes the joint entry once they are, and `C_new` once that
    /// commits; a request while a different change is in flight is refused with
    /// [`Output::Rejected`], like a proposal to a non-leader.
    Change(Vec<ServerId>),
    /// The server applied every entry through `index`.
    Applied(Index),
    /// The snapshot task completed a [`SnapshotAction::Take`]: a checkpoint at
    /// `index`, whose entry has `term`, is on disk and recorded (RAFT.md §1).
    SnapshotTaken {
        /// The checkpoint's applied index.
        index: Index,
        /// That entry's term.
        term: Term,
    },
    /// The snapshot task streamed the snapshot to `to`, which installed it: the
    /// follower now holds everything through `index`.
    SnapshotInstalled {
        /// The follower.
        to: ServerId,
        /// The snapshot's last index.
        index: Index,
        /// The store incarnation the follower's `Installed` answer carried: a
        /// fresh one when the install re-seeded a refused server (RAFT.md §3).
        // D-042: store incarnations.
        incarnation: u64,
    },
    /// The snapshot task gave up streaming to `to`: a timeout, a lost leadership,
    /// or a checkpoint the receiver's checks refused. With `retake` the checkpoint
    /// itself is unusable and the next need takes a fresh one.
    SnapshotFailed {
        /// The follower.
        to: ServerId,
        /// Whether the checkpoint is unusable.
        retake: bool,
    },
    /// The snapshot task's stream to `to` had a chunk acknowledged that took it
    /// past the furthest point it had reached: the re-seed progress check quorum
    /// asks of a refused follower (D-049). The install's own answer arrives as
    /// [`Input::SnapshotInstalled`] and counts the same way.
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    SnapshotAcked {
        /// The follower.
        to: ServerId,
    },
}

/// A change to persistent state: what must be durable before anything after it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Persist {
    /// The current term.
    pub term: Term,
    /// The vote in it.
    pub vote: Option<ServerId>,
    /// Entries removed, from this index on, before `append`.
    pub truncate_from: Option<Index>,
    /// Entries written.
    pub append: Vec<Entry>,
    /// The configuration in force after the step, when the step changed it: the
    /// latest configuration entry's index and content, kept under the `0 / 2 /
    /// config` key in the same synced batch (RAFT.md §3); index 0 and the initial
    /// configuration after a truncation removed every configuration entry.
    pub config: Option<(Index, Configuration)>,
    /// Entries at or below this index deleted: the log compacted to a snapshot
    /// (RAFT.md §1). Never overlaps `append`.
    pub compact_to: Option<Index>,
}

/// What the core wants done, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    /// Make this durable before anything after it.
    Persist(Persist),
    /// Send a message.
    Send {
        /// To whom.
        to: ServerId,
        /// What.
        message: Message,
    },
    /// Entries through `through` are committed and may be applied, in order,
    /// once; report each with [`Input::Applied`].
    Apply {
        /// The commit index.
        through: Index,
    },
    /// A proposal or a read was refused because this server is not the leader.
    Rejected {
        /// The leader, if known.
        leader: Option<ServerId>,
    },
    /// The read `id` may be served from the state machine: every entry through
    /// `index` is applied, and this server was leader when the read arrived by a
    /// lease or a heartbeat round since.
    ReadReady {
        /// The read.
        id: u64,
        /// The read index.
        index: Index,
    },
    /// The read `id` will not be served: this server stopped leading first.
    ReadDropped {
        /// The read.
        id: u64,
    },
    /// A state transition that matters, for the trace.
    Trace(TraceEvent),
    /// Snapshot work for the `snapshot` task (RAFT.md §3): the core only says what
    /// it needs; taking checkpoints and streaming them is the server's.
    Snapshot(SnapshotAction),
}

/// What the core asks the `snapshot` task to do (RAFT.md §3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotAction {
    /// Take a checkpoint of the state machine at the applied index, with the
    /// metadata written before the checkpoint's `CURRENT`, and answer with
    /// [`Input::SnapshotTaken`].
    Take,
    /// Stream the snapshot at (`index`, `term`) to `to`, and answer with
    /// [`Input::SnapshotInstalled`] or [`Input::SnapshotFailed`].
    Install {
        /// The follower to feed.
        to: ServerId,
        /// The snapshot's last index.
        index: Index,
        /// That entry's term.
        term: Term,
    },
}

/// The drift guard's view of one follower (RAFT.md §1): the follower's clock
/// against the leader's, observed through the fastest response of each window,
/// compared with the first window's.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Guard {
    /// When the current window opened, by the leader's clock; 0 for none.
    window_start: u64,
    /// The smallest offset seen in the window: follower clock less leader send time.
    window_min: Option<i128>,
    /// The first window's (midpoint, offset), the base every later window is
    /// compared with.
    base: Option<(u64, i128)>,
    /// Whether the last comparison found the offset steady.
    trusted: bool,
}

/// A read waiting for its confirmation and its state (RAFT.md §1).
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingRead {
    id: u64,
    /// The read index.
    index: Index,
    /// When it arrived, by the leader's clock: only acknowledgements of requests
    /// sent after this confirm it.
    at: u64,
    /// Followers whose acknowledgement confirmed it.
    acks: Vec<ServerId>,
    /// Confirmed by a lease or by a majority of acknowledgements.
    confirmed: bool,
    /// Whether the lease confirmed it.
    lease: bool,
}

/// An AppendEntries response with the leader's clock at its arrival.
struct Ack {
    success: bool,
    prev_index: Index,
    match_index: Index,
    hint: Index,
    echo: u64,
    local: u64,
    /// The responder's store incarnation (D-042).
    incarnation: u64,
    now: u64,
}

impl Guard {
    /// Takes one observation: the follower's clock `local` against the leader's
    /// send time `echo`, at leader time `now`. Every `guard_window_nanos` the
    /// window's fastest response is compared with the first window's; movement
    /// beyond the drift bound over the time between them revokes the follower's
    /// trust, jitter included, and the comparison starts over from here. Returns
    /// the movement that revoked, if this observation closed a window that did.
    fn observe(&mut self, config: &RaftConfig, now: u64, echo: u64, local: u64) -> Option<u64> {
        let offset = i128::from(local) - i128::from(echo);
        if self.window_start == 0 {
            self.window_start = now;
        }
        self.window_min = Some(self.window_min.map_or(offset, |m| m.min(offset)));
        if now.saturating_sub(self.window_start) < config.guard_window_nanos {
            return None;
        }
        let mid = self.window_start + (now - self.window_start) / 2;
        let envelope = self.window_min.take().unwrap_or(offset);
        self.window_start = now;
        let mut revoked = None;
        match self.base {
            None => {
                self.base = Some((mid, envelope));
            }
            Some((base_at, base_offset)) => {
                let moved = (envelope - base_offset).unsigned_abs();
                let elapsed = u128::from(mid.saturating_sub(base_at));
                let allowed = elapsed * u128::from(config.drift_bound_ppm) / 1_000_000;
                if moved > allowed {
                    self.trusted = false;
                    self.base = Some((mid, envelope));
                    revoked = Some(u64::try_from(moved).unwrap_or(u64::MAX));
                } else {
                    self.trusted = true;
                }
            }
        }
        revoked
    }
}

/// A membership change under way on the leader, before its joint entry exists
/// (RAFT.md §1): the servers being added catch up as non-voting learners first
/// (thesis §4.2.1). The state is leader-local and volatile: a leadership change or
/// a step-down abandons the change, the operator sees no joint entry in the trace
/// and asks again. D-032: the catch-up phase is not replicated.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Change {
    /// The voters the operator asked for.
    new_voters: Vec<ServerId>,
    /// The learners catching up, each with its round under way.
    learners: BTreeMap<ServerId, Learner>,
}

/// One learner's catch-up (thesis §4.2.1), measured in the leader's ticks: a round
/// runs from its start to the acknowledgement covering the leader's then-last
/// index, and a round shorter than the minimum election timeout ends the catch-up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Learner {
    /// The tick the round under way started at.
    round_start: u64,
    /// The leader's last index when it started: the acknowledgement that covers it
    /// ends the round.
    target: Index,
    /// Whether a round finished within the minimum election timeout.
    caught_up: bool,
}

/// What a leader knows about a follower.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Progress {
    /// The next entry to send.
    next: Index,
    /// The highest entry known replicated; never moves backwards.
    matched: Index,
    /// The last index of each AppendEntries with entries not yet answered.
    inflight: VecDeque<Index>,
    /// Set by a rejection: one message in flight, a probe, until a success. The
    /// value is the probe's `prev_index`. A rejection of anything else is stale and
    /// ignored; without this, every rejection of a pipeline's other messages would
    /// restart the pipeline, and the leader would flood the follower (D-026).
    probe: Option<Index>,
    /// The latest `sent` the follower acknowledged: its promise not to vote runs
    /// from there (RAFT.md §1).
    promise: Option<u64>,
    /// Whether the follower answered since the last quorum check from a store: any
    /// AppendEntries response but a refused server's rejection.
    active: bool,
    /// Whether the follower answered since the last quorum check as a refused
    /// server does, with a rejection stamped store incarnation 0 (RAFT.md §3). That
    /// answer counts for check quorum only beside `stream_acked`.
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    refused_answered: bool,
    /// Whether the snapshot stream to the follower had a chunk acknowledged since
    /// the last quorum check ([`Input::SnapshotAcked`], or the install's answer).
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    stream_acked: bool,
    /// The drift guard.
    guard: Guard,
    /// The snapshot task is streaming a snapshot to this follower: no entries go
    /// until it answers.
    installing: bool,
    /// The follower is designated snapshot-fed (RAFT.md §1, "a learner being
    /// replaced by the snapshot"): it no longer blocks compaction. Set for a
    /// follower far behind and unresponsive, or one that rejected an append at
    /// index 1, which no follower with a log does (a re-seeding server's ask).
    needs_snapshot: bool,
    /// Ticks since the follower last answered anything.
    quiet_ticks: u64,
    /// The store incarnation the follower last answered with, once it has
    /// answered (RAFT.md §3): an answer carrying a different one means the
    /// follower runs on a rebuilt store whose log may have lost entries it once
    /// acknowledged, and everything above starts over.
    // D-042: store incarnations.
    incarnation: Option<u64>,
}

/// One server's protocol state.
#[derive(Clone, Debug)]
pub struct Raft {
    id: ServerId,
    config: RaftConfig,
    /// The configuration in force: the latest configuration entry in the log,
    /// committed or not, or the initial configuration (RAFT.md §1).
    membership: Configuration,
    /// The configuration a fresh store starts with: what a truncation below every
    /// configuration entry reverts to.
    initial_membership: Configuration,
    /// The index of the configuration entry in force; 0 for the initial one.
    membership_index: Index,
    /// Ticks seen since this core started: what catch-up rounds are measured in.
    ticks: u64,
    /// The change under way on a leader while its servers catch up as learners.
    change: Option<Change>,
    term: Term,
    vote: Option<ServerId>,
    role: Role,
    leader: Option<ServerId>,
    /// The log's tail past the compacted prefix: index `i` at position
    /// `i - 1 - snap_index`.
    log: Vec<Entry>,
    /// The compacted prefix's last index: the log starts at `snap_index + 1`, and
    /// the snapshot stands in for everything at or below (RAFT.md §1). Zero for no
    /// snapshot.
    snap_index: Index,
    /// That entry's term.
    snap_term: Term,
    /// The configuration in force at the compacted prefix's last index: the floor
    /// a truncation's revert can reach once compaction has swallowed the entry
    /// itself (RAFT.md §1, D-029). None while nothing is compacted, when the
    /// initial configuration serves.
    snap_config: Option<Configuration>,
    /// The last checkpoint taken or installed here, streamable to a follower;
    /// `snap_index` never passes it. None until one is taken.
    taken: Option<(Index, Term)>,
    /// A [`SnapshotAction::Take`] is with the snapshot task.
    take_pending: bool,
    /// This server runs on a re-seeded store (RAFT.md §3): the state it lost may
    /// have included a vote, so it grants no vote and no pre-vote, never
    /// campaigns, and makes no lease promise, for the rest of its life on that
    /// store. It still replicates, applies and counts for commit majorities.
    // D-035: re-seeded servers are quarantined from voting for good.
    quarantined: bool,
    commit: Index,
    applied: Index,
    /// Votes or pre-votes granted in the round under way, this server included.
    granted: Vec<ServerId>,
    progress: BTreeMap<ServerId, Progress>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    /// Ticks led since the last election won: a fresh leader defers threshold
    /// snapshots until it has led a while (D-030).
    leader_ticks: u64,
    /// Ticks since the leader last checked it had heard from a majority.
    quorum_elapsed: u64,
    /// The index of the current term's first entry, the no-op, on a leader: a read
    /// waits for it to commit (thesis §6.4).
    first_of_term: Index,
    reads: Vec<PendingRead>,
    /// The follower a transfer waits to catch up before TimeoutNow is sent.
    transferee: Option<ServerId>,
    /// Whether the election under way was asked for by the leader: the vote
    /// requests say so, and followers that heard from their leader vote anyway.
    transfer: bool,
    rng: u64,
    /// Outputs of the step under way.
    outputs: Vec<Output>,
    /// Whether the step under way changed the term or the vote.
    hard_state_changed: bool,
    /// Whether the step under way changed the configuration in force.
    config_changed: bool,
    /// The step's log changes.
    truncate_from: Option<Index>,
    appended: Vec<Entry>,
    compacted_to: Option<Index>,
}

impl Raft {
    /// A fresh server: term 0, no vote, an empty log, a follower.
    #[must_use]
    pub fn new(id: ServerId, membership: Configuration, config: RaftConfig, seed: u64) -> Self {
        Self::restore(id, membership, config, seed, 0, None, Vec::new())
    }

    /// A server with the persistent state its store held. `membership` is the
    /// configuration a fresh store starts with: a log holding configuration
    /// entries puts its latest one in force instead, committed or not (RAFT.md
    /// §1).
    #[must_use]
    pub fn restore(
        id: ServerId,
        membership: Configuration,
        config: RaftConfig,
        seed: u64,
        term: Term,
        vote: Option<ServerId>,
        log: Vec<Entry>,
    ) -> Self {
        Self::restore_compacted(
            id, membership, config, seed, term, vote, 0, 0, None, log, false,
        )
    }

    /// A server with the persistent state its store held, the log compacted to a
    /// snapshot at (`snap_index`, `snap_term`): `log` is the tail past it, starting
    /// at `snap_index + 1`. `snap_config` is the configuration the snapshot's
    /// record carries: the configuration in force is the log tail's latest
    /// configuration entry, committed or not (RAFT.md §1), and when compaction has
    /// swallowed the entry itself the snapshot's record is where it survives. With
    /// `quarantined` the server runs on a re-seeded store and grants no vote, no
    /// pre-vote and no lease promise for good (RAFT.md §3).
    #[must_use]
    #[expect(clippy::too_many_arguments, reason = "a restart states everything")]
    pub fn restore_compacted(
        id: ServerId,
        membership: Configuration,
        config: RaftConfig,
        seed: u64,
        term: Term,
        vote: Option<ServerId>,
        snap_index: Index,
        snap_term: Term,
        snap_config: Option<Configuration>,
        log: Vec<Entry>,
        quarantined: bool,
    ) -> Self {
        debug_assert!(
            log.first().is_none_or(|e| e.index == snap_index + 1),
            "the log is the tail past the snapshot"
        );
        let (membership_index, in_force) =
            log.iter()
                .fold((0, None), |kept, entry| match &entry.payload {
                    Payload::Config(config) => (entry.index, Some(config.clone())),
                    _ => kept,
                });
        // After an install or a compaction the configuration entry in force may
        // sit at or below the snapshot's last index: the record's configuration
        // is then the one in force, held as of the snapshot (D-029, RAFT.md §3).
        let (membership_index, in_force) = match (in_force, &snap_config) {
            (Some(config), _) => (membership_index, Some(config)),
            (None, Some(config)) if snap_index > 0 => (snap_index, Some(config.clone())),
            (None, _) => (0, None),
        };
        let mut raft = Self {
            id,
            config,
            membership: in_force.unwrap_or_else(|| membership.clone()),
            initial_membership: membership,
            membership_index,
            ticks: 0,
            change: None,
            term,
            vote,
            role: Role::Follower,
            leader: None,
            log,
            snap_index,
            snap_term,
            snap_config,
            taken: (snap_index > 0).then_some((snap_index, snap_term)),
            take_pending: false,
            quarantined,
            // Everything the snapshot covers is committed and applied by
            // construction (RAFT.md §1).
            commit: snap_index,
            applied: snap_index,
            granted: Vec::new(),
            progress: BTreeMap::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            leader_ticks: 0,
            quorum_elapsed: 0,
            first_of_term: 0,
            reads: Vec::new(),
            transferee: None,
            transfer: false,
            rng: seed | 1,
            outputs: Vec::new(),
            hard_state_changed: false,
            config_changed: false,
            truncate_from: None,
            appended: Vec::new(),
            compacted_to: None,
        };
        raft.election_timeout = raft.draw_timeout();
        raft
    }

    /// The server.
    #[must_use]
    pub fn id(&self) -> ServerId {
        self.id
    }

    /// The current term.
    #[must_use]
    pub fn term(&self) -> Term {
        self.term
    }

    /// The vote in the current term.
    #[must_use]
    pub fn vote(&self) -> Option<ServerId> {
        self.vote
    }

    /// The role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// The leader of the current term, if known.
    #[must_use]
    pub fn leader(&self) -> Option<ServerId> {
        self.leader
    }

    /// The commit index.
    #[must_use]
    pub fn commit(&self) -> Index {
        self.commit
    }

    /// The applied index the server last reported.
    #[must_use]
    pub fn applied(&self) -> Index {
        self.applied
    }

    /// The log's tail past the compacted prefix (all of it while nothing is
    /// compacted).
    #[must_use]
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    /// The compacted prefix's (last index, last term): the snapshot the log starts
    /// after. (0, 0) for none.
    #[must_use]
    pub fn snapshot(&self) -> (Index, Term) {
        (self.snap_index, self.snap_term)
    }

    /// Whether this server runs on a re-seeded store and grants no vote, no
    /// pre-vote and no lease promise (RAFT.md §3).
    #[must_use]
    pub fn quarantined(&self) -> bool {
        self.quarantined
    }

    /// The first index the log holds: one past the snapshot.
    #[must_use]
    pub fn first_index(&self) -> Index {
        self.snap_index + 1
    }

    /// The last index: of the log, or of the snapshot when the log is empty; 0 for
    /// neither.
    #[must_use]
    pub fn last_index(&self) -> Index {
        self.snap_index + self.log.len() as Index
    }

    /// The last entry's term, the snapshot's when the log is empty, 0 for neither.
    /// The election restriction and the consistency check read the snapshot's term
    /// and index exactly here (RAFT.md §1).
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.log.last().map_or(self.snap_term, |e| e.term)
    }

    /// The term of the entry at `index`: 0 at index 0, the snapshot's term at its
    /// boundary, none below the snapshot or past the log.
    #[must_use]
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.snap_index {
            return Some(self.snap_term);
        }
        if index < self.snap_index {
            return None;
        }
        self.log
            .get((index - self.snap_index) as usize - 1)
            .map(|e| e.term)
    }

    /// The entry at `index`, if the log still holds it.
    #[must_use]
    pub fn entry(&self, index: Index) -> Option<&Entry> {
        if index <= self.snap_index {
            return None;
        }
        self.log.get((index - self.snap_index) as usize - 1)
    }

    /// The configuration in force: the latest configuration entry in the log,
    /// committed or not, or the initial configuration.
    #[must_use]
    pub fn membership(&self) -> &Configuration {
        &self.membership
    }

    /// The index of the configuration entry in force; 0 for the initial one.
    #[must_use]
    pub fn membership_index(&self) -> Index {
        self.membership_index
    }

    /// The configuration in force at the applied index: what a snapshot taken
    /// there must record (RAFT.md §1). The log tail's latest configuration entry
    /// at or below the applied index, else the compacted prefix's, else the
    /// initial one.
    #[must_use]
    pub fn applied_membership(&self) -> Configuration {
        self.log
            .iter()
            .take_while(|e| e.index <= self.applied)
            .fold(None, |kept, e| match &e.payload {
                Payload::Config(config) => Some(config.clone()),
                _ => kept,
            })
            .or_else(|| self.snap_config.clone())
            .unwrap_or_else(|| self.initial_membership.clone())
    }

    /// The parameters.
    #[must_use]
    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    /// Steps the core with `input`; the outputs to execute, in order.
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Tick => self.on_tick(),
            Input::Propose(command) => self.on_propose(command),
            Input::Transfer(to) => self.on_transfer(to),
            Input::Change(voters) => self.on_change(voters),
            Input::Read { id, now } => self.on_read(id, now),
            Input::Applied(index) => {
                self.applied = self.applied.max(index);
                self.serve_reads();
            }
            Input::SnapshotTaken { index, term } => self.on_snapshot_taken(index, term),
            Input::SnapshotInstalled {
                to,
                index,
                incarnation,
            } => self.on_snapshot_installed(to, index, incarnation),
            Input::SnapshotFailed { to, retake } => self.on_snapshot_failed(to, retake),
            Input::SnapshotAcked { to } => self.on_snapshot_acked(to),
            Input::Message { from, message, now } => self.on_message(from, message, now),
        }
        self.finish()
    }

    /// When the lease ends, by this leader's clock: for each voter set in force,
    /// the promise that makes a majority of the set expire latest, this server
    /// counted where it is a member; while joint, the earlier of the two sets'
    /// ends, since a lease read rests on the same majorities as a commit (thesis
    /// §4.3). Zero for no lease.
    #[must_use]
    pub fn lease_end(&self) -> u64 {
        let trusts_all = self.config.variants.contains(Variant::LeaseTrustsTheClock);
        let of_set = |set: &[ServerId]| -> u64 {
            let mut needed = set.len() / 2 + 1;
            if set.contains(&self.id) {
                needed -= 1;
            }
            if needed == 0 {
                return u64::MAX;
            }
            let mut promises: Vec<u64> = self
                .progress
                .iter()
                .filter(|(id, p)| set.contains(id) && (trusts_all || p.guard.trusted))
                .filter_map(|(_, p)| p.promise)
                .collect();
            promises.sort_unstable_by(|a, b| b.cmp(a));
            promises
                .get(needed - 1)
                .map_or(0, |&sent| sent + self.config.lease_span_nanos())
        };
        let end = of_set(&self.membership.voters);
        match &self.membership.new_voters {
            Some(new) => end.min(of_set(new)),
            None => end,
        }
    }

    /// Whether the lease holds at `now`: a leader that has committed an entry of
    /// its term, within the promise of a majority.
    #[must_use]
    pub fn lease_holds(&self, now: u64) -> bool {
        self.role == Role::Leader && self.commit >= self.first_of_term && now < self.lease_end()
    }

    /// A linearizable read (thesis §6.4): served by the lease when it holds, else
    /// after a heartbeat round acknowledged by a majority; either way not before
    /// this term's no-op is committed and the read index is applied.
    fn on_read(&mut self, id: u64, now: u64) {
        if self.role != Role::Leader {
            self.outputs.push(Output::Rejected {
                leader: self.leader,
            });
            return;
        }
        let index = self.commit.max(self.first_of_term);
        let lease = self.lease_holds(now);
        self.reads.push(PendingRead {
            id,
            index,
            at: now,
            acks: Vec::new(),
            confirmed: lease,
            lease,
        });
        if lease {
            self.trace(TraceEvent::RaftRead {
                server: self.id.0,
                index,
                lease: true,
            });
            self.serve_reads();
        } else {
            // The round: a heartbeat to everyone, sent after the read arrived.
            self.heartbeat_elapsed = 0;
            for peer in self.replication_peers() {
                self.replicate(peer, true);
            }
        }
    }

    /// Confirms reads a majority has acknowledged since they arrived, and emits
    /// every confirmed read whose index is applied.
    fn serve_reads(&mut self) {
        let mut ready = Vec::new();
        let mut kept = Vec::new();
        for mut read in std::mem::take(&mut self.reads) {
            if !read.confirmed {
                let mut on = read.acks.clone();
                on.push(self.id);
                if self.membership.has_majority(&on) && self.commit >= self.first_of_term {
                    read.confirmed = true;
                    read.index = read.index.max(self.first_of_term);
                    self.trace(TraceEvent::RaftRead {
                        server: self.id.0,
                        index: read.index,
                        lease: false,
                    });
                }
            }
            if read.confirmed && self.applied >= read.index {
                ready.push(read);
            } else {
                kept.push(read);
            }
        }
        self.reads = kept;
        for read in ready {
            self.outputs.push(Output::ReadReady {
                id: read.id,
                index: read.index,
            });
        }
    }

    /// Leadership transfer (thesis §3.10): the target gets TimeoutNow as soon as its
    /// log matches the leader's, and starts an election without a pre-vote.
    fn on_transfer(&mut self, to: ServerId) {
        if self.role != Role::Leader {
            self.outputs.push(Output::Rejected {
                leader: self.leader,
            });
            return;
        }
        if to == self.id || !self.progress.contains_key(&to) || !self.membership.is_voter(to) {
            return;
        }
        let caught_up = self.progress[&to].matched == self.last_index();
        if caught_up {
            self.send_timeout_now(to);
        } else {
            self.transferee = Some(to);
            self.replicate(to, false);
        }
    }

    fn send_timeout_now(&mut self, to: ServerId) {
        self.transferee = None;
        self.trace(TraceEvent::RaftTransfer {
            server: self.id.0,
            to: to.0,
        });
        self.send(to, Message::TimeoutNow { term: self.term });
    }

    /// The leader asked this server to take over: an election now, no pre-vote,
    /// with vote requests marked as the leader's wish. A server that is not a
    /// voter of the configuration in force cannot win and does not try; a
    /// quarantined server never campaigns (RAFT.md §3).
    fn on_timeout_now(&mut self, from: ServerId) {
        if self.quarantined
            || self.role == Role::Leader
            || self.leader != Some(from)
            || !self.membership.is_voter(self.id)
        {
            return;
        }
        self.transfer = true;
        self.become_candidate();
    }

    /// Drops every pending read: this server stopped leading.
    fn drop_reads(&mut self) {
        for read in std::mem::take(&mut self.reads) {
            self.outputs.push(Output::ReadDropped { id: read.id });
        }
    }

    /// Orders the step's outputs: the persist first, then the rest as they came.
    fn finish(&mut self) -> Vec<Output> {
        let mut out = Vec::with_capacity(self.outputs.len() + 1);
        if self.hard_state_changed
            || self.config_changed
            || self.truncate_from.is_some()
            || self.compacted_to.is_some()
            || !self.appended.is_empty()
        {
            out.push(Output::Persist(Persist {
                term: self.term,
                vote: self.vote,
                truncate_from: self.truncate_from.take(),
                append: std::mem::take(&mut self.appended),
                config: self
                    .config_changed
                    .then(|| (self.membership_index, self.membership.clone())),
                compact_to: self.compacted_to.take(),
            }));
            self.hard_state_changed = false;
            self.config_changed = false;
        }
        out.append(&mut self.outputs);
        out
    }

    fn draw(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn draw_timeout(&mut self) -> u64 {
        let (min, max) = self.config.election_ticks;
        min + self.draw() % max.saturating_sub(min).max(1)
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.draw_timeout();
    }

    fn trace(&mut self, event: TraceEvent) {
        self.outputs.push(Output::Trace(event));
    }

    fn send(&mut self, to: ServerId, message: Message) {
        self.outputs.push(Output::Send { to, message });
    }

    fn set_role(&mut self, role: Role) {
        self.role = role;
        self.trace(TraceEvent::RaftTerm {
            server: self.id.0,
            term: self.term,
            role: role.name(),
        });
    }

    /// The term rule (moirae rule 1): a higher term makes a follower of anyone.
    fn become_follower(&mut self, term: Term, leader: Option<ServerId>) {
        if term > self.term {
            self.term = term;
            self.vote = None;
            self.hard_state_changed = true;
        }
        self.leader = leader;
        self.granted.clear();
        self.progress.clear();
        self.transferee = None;
        self.transfer = false;
        self.change = None;
        self.drop_reads();
        self.set_role(Role::Follower);
    }

    /// The voters of the configuration in force: of both sets while joint. What
    /// elections ask; learners count for nothing (thesis §4.2.1).
    fn voter_ids(&self) -> Vec<ServerId> {
        let mut all: Vec<ServerId> = self
            .membership
            .voters
            .iter()
            .chain(self.membership.new_voters.iter().flatten())
            .copied()
            .collect();
        all.sort_unstable();
        all.dedup();
        all
    }

    /// Everyone a vote or pre-vote request goes to: the voters, this server aside.
    fn vote_peers(&self) -> Vec<ServerId> {
        self.voter_ids()
            .into_iter()
            .filter(|&s| s != self.id)
            .collect()
    }

    /// Everyone entries and heartbeats go to: the members of the configuration in
    /// force plus the learners of the change under way, this server aside.
    fn replication_peers(&self) -> Vec<ServerId> {
        let mut all = self.membership.members();
        if let Some(change) = &self.change {
            all.extend(change.learners.keys().copied());
        }
        all.sort_unstable();
        all.dedup();
        all.into_iter().filter(|&s| s != self.id).collect()
    }

    /// Whether `granted` carries what an election or a commit needs under the
    /// configuration in force: a majority of the one voter set, or of both while
    /// joint (thesis §4.3). The buggy variant counts one majority of the two sets
    /// merged instead, which is the single-majority rule joint consensus forbids:
    /// a majority of the union need not contain a majority of either set.
    fn counting_majority(&self, granted: &[ServerId]) -> bool {
        if self
            .config
            .variants
            .contains(Variant::SingleMajorityInJointConsensus)
            && self.membership.new_voters.is_some()
        {
            let merged = self.voter_ids();
            let count = merged.iter().filter(|s| granted.contains(s)).count();
            return count * 2 > merged.len();
        }
        self.membership.has_majority(granted)
    }

    fn on_tick(&mut self) {
        self.ticks += 1;
        self.election_elapsed += 1;
        self.heartbeat_elapsed += 1;
        match self.role {
            Role::Leader => {
                self.leader_ticks = self.leader_ticks.saturating_add(1);
                if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    for peer in self.replication_peers() {
                        self.replicate(peer, true);
                    }
                }
                // A snapshot when the log has outgrown the last one (RAFT.md §1).
                // A fresh leader holds off for two minimum election timeouts: its
                // first duty is its no-op and its followers, and a checkpoint
                // stalls applies for its duration (D-030, D-036); a
                // follower that needs the snapshot sooner gets one on demand
                // through `replicate`.
                if self.leader_ticks >= 2 * self.config.election_ticks.0
                    && !self.take_pending
                    && self.applied > self.taken.map_or(0, |(i, _)| i)
                    && self.last_index() - self.taken.map_or(0, |(i, _)| i)
                        > self.config.snapshot_threshold
                {
                    self.take_pending = true;
                    self.outputs.push(Output::Snapshot(SnapshotAction::Take));
                }
                // Designation (RAFT.md §1's "being replaced by the snapshot"): a
                // follower far behind and quiet for two minimum election timeouts
                // no longer blocks compaction; when it comes back it is fed the
                // snapshot, since its entries are gone.
                // D-037: the compaction trigger for unresponsive followers.
                let threshold = self.config.snapshot_threshold;
                let quiet = 2 * self.config.election_ticks.0;
                let last = self.last_index();
                let mut designated = false;
                for progress in self.progress.values_mut() {
                    progress.quiet_ticks += 1;
                    if !progress.needs_snapshot
                        && last - progress.matched > threshold
                        && progress.quiet_ticks >= quiet
                    {
                        progress.needs_snapshot = true;
                        designated = true;
                    }
                }
                if designated {
                    self.maybe_compact();
                }
                // Check quorum (RAFT.md §1): a leader that heard from no majority
                // within the minimum election timeout has lost its followers to
                // another leader or a partition, and stops serving.
                self.quorum_elapsed += 1;
                if self.quorum_elapsed >= self.config.election_ticks.0 {
                    self.quorum_elapsed = 0;
                    let (mut heard, uncounted) = self.heard_this_window();
                    heard.push(self.id);
                    for progress in self.progress.values_mut() {
                        progress.active = false;
                        progress.refused_answered = false;
                        progress.stream_acked = false;
                    }
                    if !self.membership.has_majority(&heard) {
                        self.trace(TraceEvent::RaftQuorumLost {
                            server: self.id.0,
                            term: self.term,
                            uncounted,
                        });
                        self.become_follower(self.term, None);
                    }
                }
            }
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                if self.election_elapsed >= self.election_timeout {
                    // A quarantined server never campaigns: leading takes a vote
                    // for itself, and it grants none (RAFT.md §3).
                    if self.quarantined || !self.membership.is_voter(self.id) {
                        // D-033: a server that is not a voter of the
                        // configuration in force does not campaign. A learner, a
                        // server with no configuration yet, and a removed server
                        // cannot win (thesis §4.2.1) and would only knock.
                        self.reset_election_timer();
                    } else if self.config.variants.contains(Variant::NoPreVote) {
                        self.become_candidate();
                    } else {
                        self.become_pre_candidate();
                    }
                }
            }
        }
    }

    /// The followers check quorum counts for the window since the last check, and
    /// the refused followers it did not count (RAFT.md §1). A follower that
    /// answered from a store counts. A refused follower's rejection counts only
    /// when the leader's re-seed stream to it had a chunk acknowledged in the same
    /// window: a refused server answers every AppendEntries whatever becomes of
    /// its re-seed, so its rejections alone say it is alive, not that the leader
    /// is getting anywhere with it, and a leader kept in office by them while its
    /// other followers are away could hold the office for as long as the stream
    /// stays stalled with no commit possible and no election either.
    /// [`Variant::RefusedCountsForQuorum`] counts every rejection, the leader as
    /// built; [`Variant::RefusedNeverCounts`] counts none, the alternative D-049
    /// rejected.
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    fn heard_this_window(&self) -> (Vec<ServerId>, Vec<u64>) {
        let variants = self.config.variants;
        let mut heard = Vec::new();
        let mut uncounted = Vec::new();
        for (&follower, progress) in &self.progress {
            let refused_counts = if variants.contains(Variant::RefusedCountsForQuorum) {
                progress.refused_answered
            } else if variants.contains(Variant::RefusedNeverCounts) {
                false
            } else {
                progress.refused_answered && progress.stream_acked
            };
            if progress.active || refused_counts {
                heard.push(follower);
            } else if progress.refused_answered {
                uncounted.push(follower.0);
            }
        }
        (heard, uncounted)
    }

    /// Starts a pre-vote round (thesis §9.6): no term change until a majority says
    /// an election would succeed.
    fn become_pre_candidate(&mut self) {
        self.reset_election_timer();
        self.leader = None;
        self.granted = vec![self.id];
        self.set_role(Role::PreCandidate);
        if self.counting_majority(&self.granted) {
            self.become_candidate();
            return;
        }
        let message = Message::PreVote {
            term: self.term + 1,
            last_index: self.last_index(),
            last_term: self.last_term(),
        };
        for peer in self.vote_peers() {
            self.send(peer, message.clone());
        }
    }

    /// Starts an election: a new term, a vote for itself, both persisted before the
    /// requests go out.
    fn become_candidate(&mut self) {
        self.reset_election_timer();
        self.term += 1;
        self.vote = Some(self.id);
        self.hard_state_changed = true;
        self.leader = None;
        self.granted = vec![self.id];
        self.set_role(Role::Candidate);
        if self.counting_majority(&self.granted) {
            self.become_leader();
            return;
        }
        let message = Message::RequestVote {
            term: self.term,
            last_index: self.last_index(),
            last_term: self.last_term(),
            transfer: self.transfer,
        };
        for peer in self.vote_peers() {
            self.send(peer, message.clone());
        }
    }

    /// Takes the lead: a no-op for the term (thesis §6.4), progress for every peer,
    /// and the first round of AppendEntries.
    fn become_leader(&mut self) {
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.leader_ticks = 0;
        self.quorum_elapsed = 0;
        self.granted.clear();
        self.transfer = false;
        self.transferee = None;
        self.change = None;
        self.set_role(Role::Leader);
        let last = self.last_index();
        self.first_of_term = last + 1;
        self.progress = self
            .replication_peers()
            .into_iter()
            .map(|peer| {
                (
                    peer,
                    Progress {
                        next: last + 1,
                        matched: 0,
                        inflight: VecDeque::new(),
                        probe: None,
                        promise: None,
                        active: false,
                        refused_answered: false,
                        stream_acked: false,
                        guard: Guard::default(),
                        installing: false,
                        needs_snapshot: false,
                        quiet_ticks: 0,
                        incarnation: None,
                    },
                )
            })
            .collect();
        self.trace(TraceEvent::RaftLeader {
            server: self.id.0,
            term: self.term,
            last_index: last,
        });
        self.append_local(vec![Entry {
            term: self.term,
            index: last + 1,
            payload: Payload::Noop,
        }]);
        for peer in self.replication_peers() {
            self.replicate(peer, true);
        }
    }

    /// Appends entries to the local log and to the step's persist. A configuration
    /// entry takes effect as it is appended, committed or not (RAFT.md §1), on the
    /// leader that made it and the follower that stored it alike.
    fn append_local(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            debug_assert_eq!(entry.index, self.last_index() + 1);
            self.trace(TraceEvent::RaftAppend {
                server: self.id.0,
                index: entry.index,
                entry_term: entry.term,
                hash: entry.payload.hash(),
            });
            let config = match &entry.payload {
                Payload::Config(config) => Some((entry.index, config.clone())),
                _ => None,
            };
            self.log.push(entry.clone());
            self.appended.push(entry);
            if let Some((index, config)) = config {
                self.adopt(index, config);
            }
        }
    }

    /// Puts a configuration in force and traces it, so the trace always shows
    /// every server's configuration in force; the step's persist carries it to the
    /// `0 / 2 / config` key in the same batch.
    fn adopt(&mut self, index: Index, config: Configuration) {
        self.trace(TraceEvent::RaftConfig {
            server: self.id.0,
            index,
            old: config.voters.iter().map(|s| s.0).collect(),
            new: config
                .new_voters
                .as_ref()
                .map(|new| new.iter().map(|s| s.0).collect())
                .unwrap_or_default(),
            joint: config.new_voters.is_some(),
            learners: config.learners.iter().map(|s| s.0).collect(),
        });
        self.membership = config;
        self.membership_index = index;
        self.config_changed = true;
    }

    /// Removes entries from `from` on, in the log and in the step's persist. A
    /// snapshot's entries are committed, so a conflict never reaches below the
    /// compacted prefix; the clamp guards the variant that truncates on every
    /// append. A truncation that removes the configuration entry in force reverts
    /// to the latest surviving one, to the compacted prefix's, or to the initial
    /// configuration (RAFT.md §1).
    fn truncate(&mut self, from: Index) {
        let from = from.max(self.snap_index + 1);
        if from > self.last_index() {
            return;
        }
        self.log.truncate((from - self.snap_index) as usize - 1);
        self.appended.retain(|e| e.index < from);
        self.truncate_from = Some(self.truncate_from.map_or(from, |f| f.min(from)));
        self.trace(TraceEvent::RaftTruncate {
            server: self.id.0,
            from_index: from,
        });
        if self.membership_index >= from {
            let (index, config) = self
                .log
                .iter()
                .rev()
                .find_map(|entry| match &entry.payload {
                    Payload::Config(config) => Some((entry.index, config.clone())),
                    _ => None,
                })
                .unwrap_or_else(|| match &self.snap_config {
                    // The compacted prefix's entries are committed, so its
                    // configuration is the floor a revert can reach (RAFT.md §1).
                    Some(config) => (self.snap_index, config.clone()),
                    None => (0, self.initial_membership.clone()),
                });
            self.adopt(index, config);
        }
    }

    /// Records a completed checkpoint and compacts to it if every follower is past
    /// it or designated snapshot-fed.
    fn on_snapshot_taken(&mut self, index: Index, term: Term) {
        self.take_pending = false;
        if self.taken.is_none_or(|(i, _)| i < index) {
            self.taken = Some((index, term));
        }
        self.maybe_compact();
    }

    /// Records the store incarnation `from` answered with and, when it differs
    /// from the one recorded, forgets the follower's progress (RAFT.md §3): a
    /// store a re-seed rebuilt may have lost entries the follower once
    /// acknowledged, and `matched` is monotone by design (D-026) with the probe
    /// never reaching below it, so a leader that kept it could never probe the
    /// rebuilt log — every rejection would be discarded and the follower never
    /// counted again. The match index, next index, pipeline, probe and snapshot
    /// designation start over as they do when a leader takes office; the answer
    /// is then processed as usual and the normal probe walks back from its hint.
    /// A stream in flight is left to the snapshot task, which ends it either way.
    /// The first answer seen only records. Returns whether progress was reset.
    /// [`Variant::IgnoreIncarnation`] records and never resets: the leader as
    /// built before this rule.
    // D-042: store incarnations.
    fn note_incarnation(&mut self, from: ServerId, incarnation: u64) -> bool {
        let last = self.last_index();
        let Some(progress) = self.progress.get_mut(&from) else {
            return false;
        };
        let changed = progress.incarnation.is_some_and(|seen| seen != incarnation);
        progress.incarnation = Some(incarnation);
        if !changed || self.config.variants.contains(Variant::IgnoreIncarnation) {
            return false;
        }
        progress.matched = 0;
        progress.next = last + 1;
        progress.inflight.clear();
        progress.probe = None;
        progress.needs_snapshot = false;
        self.trace(TraceEvent::RaftProgressReset {
            server: self.id.0,
            follower: from.0,
            incarnation,
        });
        true
    }

    /// The snapshot task streamed a snapshot to `to`, which runs on it now: its
    /// match is at least the snapshot's last index.
    fn on_snapshot_installed(&mut self, to: ServerId, index: Index, incarnation: u64) {
        if self.role != Role::Leader {
            return;
        }
        // The install may have built a new store, a re-seed's: what was known of
        // the old one is forgotten before the install's match is recorded.
        // D-042: store incarnations.
        self.note_incarnation(to, incarnation);
        let Some(progress) = self.progress.get_mut(&to) else {
            return;
        };
        progress.installing = false;
        progress.needs_snapshot = false;
        // The install's answer is the acknowledgement of the stream's final chunk
        // (RAFT.md §1): re-seed progress for this window's check quorum, as a
        // chunk's is. A checkpoint that fits in one chunk has no other.
        // D-049: a refused follower counts for check quorum only while its re-seed
        // stream progresses.
        progress.stream_acked = true;
        progress.matched = progress.matched.max(index);
        progress.next = progress.next.max(progress.matched + 1);
        progress.probe = None;
        progress.inflight.clear();
        self.maybe_commit();
        self.maybe_compact();
        self.replicate(to, false);
    }

    /// The snapshot task gave up on `to`; with `retake` the checkpoint itself is
    /// unusable and the next need takes a fresh one. A failed take arrives the
    /// same way, with `to` naming this server: `retake` then also clears the
    /// pending take, so the next tick may ask again.
    fn on_snapshot_failed(&mut self, to: ServerId, retake: bool) {
        if retake {
            self.taken = None;
            self.take_pending = false;
        }
        if let Some(progress) = self.progress.get_mut(&to) {
            progress.installing = false;
        }
    }

    /// The snapshot task's stream to `to` had a chunk acknowledged past the
    /// furthest point it had reached: re-seed progress, which lets a refused
    /// follower's rejections in this window count for check quorum. Whether the
    /// stream is still the one this leader asked for does not matter: any stream
    /// the follower is acknowledging brings its install nearer, and the mark lives
    /// only until the window's check.
    // D-049: a refused follower counts for check quorum only while its re-seed
    // stream progresses.
    fn on_snapshot_acked(&mut self, to: ServerId) {
        if self.role != Role::Leader {
            return;
        }
        if let Some(progress) = self.progress.get_mut(&to) {
            progress.stream_acked = true;
        }
    }

    /// Compacts the log to the last checkpoint once every follower's match is past
    /// it or the follower is designated snapshot-fed (RAFT.md §1): the prefix at or
    /// below it is deleted, the snapshot standing in for it.
    fn maybe_compact(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let Some((index, term)) = self.taken else {
            return;
        };
        if index <= self.snap_index {
            return;
        }
        let blocked = self
            .progress
            .values()
            .any(|p| p.matched < index && !p.needs_snapshot && !p.installing);
        if blocked {
            return;
        }
        // The prefix may swallow the configuration entry in force: keep the
        // configuration at the new prefix's end as the revert floor (D-029).
        let drained =
            self.log
                .drain(..(index - self.snap_index) as usize)
                .fold(None, |kept, entry| match entry.payload {
                    Payload::Config(config) => Some(config),
                    _ => kept,
                });
        if drained.is_some() {
            self.snap_config = drained;
        }
        self.snap_index = index;
        self.snap_term = term;
        self.compacted_to = Some(index);
        self.trace(TraceEvent::RaftCompacted {
            server: self.id.0,
            through: index,
        });
    }

    fn on_propose(&mut self, command: Bytes) {
        if self.role != Role::Leader {
            self.outputs.push(Output::Rejected {
                leader: self.leader,
            });
            return;
        }
        let index = self.last_index() + 1;
        self.append_local(vec![Entry {
            term: self.term,
            index,
            payload: Payload::Command(command),
        }]);
        for peer in self.replication_peers() {
            self.replicate(peer, false);
        }
        // A leader of one commits alone.
        self.maybe_commit();
    }

    /// A membership change (RAFT.md §1, thesis §4.3). The servers to add catch up
    /// as learners first; a change that only removes proposes the joint entry at
    /// once. One change is in flight at a time, the catch-up phase included: a
    /// request for different voters while one is under way, or while the latest
    /// configuration entry is uncommitted, is refused the way a proposal to a
    /// non-leader is; a request for the voters of the change under way, or for
    /// the voters already in force, asks for what is already true and changes
    /// nothing (D-029).
    fn on_change(&mut self, voters: Vec<ServerId>) {
        if self.role != Role::Leader {
            self.outputs.push(Output::Rejected {
                leader: self.leader,
            });
            return;
        }
        let mut target = voters;
        target.sort_unstable();
        target.dedup();
        let same = |set: &[ServerId]| -> bool {
            let mut sorted = set.to_vec();
            sorted.sort_unstable();
            sorted == target
        };
        let refused = if target.is_empty() {
            true
        } else if let Some(change) = &self.change {
            !same(&change.new_voters)
        } else if let Some(new) = &self.membership.new_voters {
            !same(new)
        } else if same(&self.membership.voters) {
            false
        } else if self.membership_index > self.commit {
            true
        } else {
            let learners: Vec<ServerId> = target
                .iter()
                .copied()
                .filter(|s| !self.membership.voters.contains(s))
                .collect();
            if learners.is_empty() {
                self.append_joint(target);
            } else {
                let last = self.last_index();
                let mut tracked = BTreeMap::new();
                for &learner in &learners {
                    self.progress.insert(
                        learner,
                        Progress {
                            next: last + 1,
                            matched: 0,
                            inflight: VecDeque::new(),
                            probe: None,
                            promise: None,
                            active: false,
                            refused_answered: false,
                            stream_acked: false,
                            guard: Guard::default(),
                            installing: false,
                            needs_snapshot: false,
                            quiet_ticks: 0,
                            incarnation: None,
                        },
                    );
                    tracked.insert(
                        learner,
                        Learner {
                            round_start: self.ticks,
                            target: last,
                            caught_up: false,
                        },
                    );
                }
                self.change = Some(Change {
                    new_voters: target,
                    learners: tracked,
                });
                for learner in learners {
                    self.replicate(learner, true);
                }
            }
            false
        };
        if refused {
            self.outputs.push(Output::Rejected {
                leader: self.leader,
            });
        }
    }

    /// Appends the joint entry `C_old,new` and replicates it: from here elections
    /// and commits need majorities of both voter sets, on this leader at once and
    /// on every server the entry reaches (thesis §4.3).
    fn append_joint(&mut self, new_voters: Vec<ServerId>) {
        self.change = None;
        let entry = Entry {
            term: self.term,
            index: self.last_index() + 1,
            payload: Payload::Config(Configuration {
                voters: self.membership.voters.clone(),
                new_voters: Some(new_voters),
                learners: Vec::new(),
            }),
        };
        self.append_local(vec![entry]);
        for peer in self.replication_peers() {
            self.replicate(peer, false);
        }
        self.maybe_commit();
    }

    /// Advances the catch-up of learner `from`, whose acknowledgements now cover
    /// `matched` (thesis §4.2.1): the round under way ends at the acknowledgement
    /// covering the leader's last index when it started, a round shorter than the
    /// minimum election timeout ends the catch-up, and a longer one starts the
    /// next at the leader's current last index. Once every learner is caught up
    /// the joint entry is proposed.
    fn note_learner_round(&mut self, from: ServerId, matched: Index) {
        let last = self.last_index();
        let ticks = self.ticks;
        let min = self.config.election_ticks.0;
        let Some(change) = &mut self.change else {
            return;
        };
        if let Some(learner) = change.learners.get_mut(&from)
            && !learner.caught_up
            && matched >= learner.target
        {
            if ticks - learner.round_start < min {
                learner.caught_up = true;
            } else {
                learner.round_start = ticks;
                learner.target = last;
            }
        }
        if change.learners.values().all(|l| l.caught_up) {
            let target = change.new_voters.clone();
            self.append_joint(target);
        }
    }

    /// What a leader's advancing commit index obliges of a change (thesis §4.3):
    /// once the joint entry is committed, propose `C_new`; once `C_new` is
    /// committed and this leader is not in it, step down. Both run on whichever
    /// leader holds the configuration when its commit index gets there, so a
    /// change survives the leader that started it.
    fn after_commit(&mut self) {
        if self.role != Role::Leader
            || self.membership_index == 0
            || self.commit < self.membership_index
        {
            return;
        }
        if let Some(new) = self.membership.new_voters.clone() {
            let entry = Entry {
                term: self.term,
                index: self.last_index() + 1,
                payload: Payload::Config(Configuration {
                    voters: new,
                    new_voters: None,
                    learners: Vec::new(),
                }),
            };
            self.append_local(vec![entry]);
            for peer in self.replication_peers() {
                self.replicate(peer, false);
            }
            self.maybe_commit();
        } else if !self.membership.is_voter(self.id) {
            self.become_follower(self.term, None);
        }
    }

    /// Sends `to` what it has not got, up to the batch and pipeline limits; with
    /// `heartbeat` an empty AppendEntries goes when there is nothing to send, which
    /// resets the follower's timer and carries the commit index. A follower whose
    /// `next` falls at or below the compacted prefix, or one designated
    /// snapshot-fed, is fed the snapshot instead (RAFT.md §1): the core asks the
    /// snapshot task to stream the last checkpoint, or to take one first.
    fn replicate(&mut self, to: ServerId, heartbeat: bool) {
        let last = self.last_index();
        let commit = self.commit;
        let term = self.term;
        let (max_batch, max_inflight) = (self.config.max_batch, self.config.max_inflight);
        let Some(progress) = self.progress.get(&to) else {
            return;
        };
        if progress.installing || progress.needs_snapshot || progress.next <= self.snap_index {
            if !progress.installing {
                match self.taken {
                    Some((index, snap_term)) => {
                        if let Some(progress) = self.progress.get_mut(&to) {
                            progress.installing = true;
                        }
                        self.outputs.push(Output::Snapshot(SnapshotAction::Install {
                            to,
                            index,
                            term: snap_term,
                        }));
                    }
                    None if !self.take_pending && self.applied > 0 => {
                        self.take_pending = true;
                        self.outputs.push(Output::Snapshot(SnapshotAction::Take));
                    }
                    None => {}
                }
            }
            if heartbeat {
                // The follower's timer and the leader's check quorum still need
                // the round trip while the snapshot task works.
                let message = Message::AppendEntries {
                    term,
                    prev_index: last,
                    prev_term: self.last_term(),
                    entries: Vec::new(),
                    commit,
                    sent: 0,
                };
                self.send(to, message);
            }
            return;
        }
        let mut next = progress.next;
        let mut inflight = progress.inflight.clone();
        let probing = progress.probe.is_some();
        let limit = if probing { 1 } else { max_inflight };
        let mut sends = Vec::new();
        while inflight.len() < limit && next <= last {
            let end = last.min(next + max_batch as Index - 1);
            let at = (next - self.snap_index) as usize - 1;
            let entries: Vec<Entry> = self.log[at..at + (end - next) as usize + 1].to_vec();
            sends.push(Message::AppendEntries {
                term,
                prev_index: next - 1,
                prev_term: self.term_at(next - 1).unwrap_or(0),
                entries,
                commit,
                sent: 0,
            });
            inflight.push_back(end);
            next = end + 1;
        }
        if sends.is_empty() && heartbeat {
            sends.push(Message::AppendEntries {
                term,
                prev_index: next - 1,
                prev_term: self.term_at(next - 1).unwrap_or(0),
                entries: Vec::new(),
                commit,
                sent: 0,
            });
        }
        if let Some(progress) = self.progress.get_mut(&to) {
            if probing {
                // The probe is the first message sent, at what `next` was.
                if let Some(Message::AppendEntries { prev_index, .. }) = sends.first() {
                    progress.probe = Some(*prev_index);
                }
            }
            progress.next = next;
            progress.inflight = inflight;
        }
        for message in sends {
            self.send(to, message);
        }
    }

    /// The election restriction (§5.4.1): the candidate's log is at least as up to
    /// date as ours, last terms first, then length. The buggy variant compares
    /// lengths first.
    fn log_up_to_date(&self, last_index: Index, last_term: Term) -> bool {
        let (mine_index, mine_term) = (self.last_index(), self.last_term());
        if self
            .config
            .variants
            .contains(Variant::IndexFirstElectionRestriction)
        {
            last_index > mine_index || (last_index == mine_index && last_term >= mine_term)
        } else {
            last_term > mine_term || (last_term == mine_term && last_index >= mine_index)
        }
    }

    /// Whether this server has heard from a leader within its minimum election
    /// timeout, or is one: the test behind pre-votes, the promise behind leases,
    /// and the vote rule below (RAFT.md §1).
    fn heard_from_leader(&self) -> bool {
        self.role == Role::Leader
            || (self.leader.is_some() && self.election_elapsed < self.config.election_ticks.0)
    }

    fn on_message(&mut self, from: ServerId, message: Message, now: u64) {
        if self.config.variants.contains(Variant::ResetTimerOnAnyRpc) {
            self.election_elapsed = 0;
        }
        let term = message.term();
        // A vote request while this server has heard from its leader is ignored,
        // term and all (RAFT.md §1, thesis §6.4.1): it promised its leader as much,
        // which is what a lease read rests on, and with pre-vote a candidate that
        // reached a real election already has a majority that has not heard.
        if matches!(
            message,
            Message::RequestVote {
                transfer: false,
                ..
            }
        ) && self.heard_from_leader()
        {
            self.trace(TraceEvent::RaftVote {
                server: self.id.0,
                term,
                candidate: from.0,
                granted: false,
                pre: false,
            });
            return;
        }
        // The term rule first (moirae rule 1), except for pre-votes, which carry a
        // term nobody has started, and their responses.
        match &message {
            Message::PreVote { .. } | Message::PreVoteResponse { .. } => {}
            _ => {
                if term > self.term {
                    let leader = matches!(message, Message::AppendEntries { .. }).then_some(from);
                    self.become_follower(term, leader);
                } else if term < self.term {
                    // Stale: a request gets our term back so the sender steps down;
                    // a response is ignored (moirae rule 6).
                    match message {
                        Message::RequestVote { .. } => {
                            self.send(
                                from,
                                Message::RequestVoteResponse {
                                    term: self.term,
                                    granted: false,
                                },
                            );
                        }
                        Message::AppendEntries {
                            prev_index, sent, ..
                        } => {
                            let echo = if self.quarantined { 0 } else { sent };
                            self.send(
                                from,
                                Message::AppendEntriesResponse {
                                    term: self.term,
                                    success: false,
                                    prev_index,
                                    match_index: 0,
                                    hint: 0,
                                    echo,
                                    local: 0,
                                    incarnation: 0,
                                },
                            );
                        }
                        _ => {}
                    }
                    return;
                }
            }
        }
        match message {
            Message::PreVote {
                term,
                last_index,
                last_term,
            } => self.on_pre_vote(from, term, last_index, last_term),
            Message::PreVoteResponse { term, granted } => {
                self.on_pre_vote_response(from, term, granted)
            }
            Message::RequestVote {
                last_index,
                last_term,
                ..
            } => self.on_request_vote(from, last_index, last_term),
            Message::TimeoutNow { .. } => self.on_timeout_now(from),
            Message::RequestVoteResponse { granted, .. } => {
                self.on_request_vote_response(from, granted)
            }
            Message::AppendEntries {
                prev_index,
                prev_term,
                entries,
                commit,
                sent,
                ..
            } => self.on_append_entries(from, prev_index, prev_term, entries, commit, sent),
            Message::AppendEntriesResponse {
                success,
                prev_index,
                match_index,
                hint,
                echo,
                local,
                incarnation,
                ..
            } => self.on_append_entries_response(
                from,
                Ack {
                    success,
                    prev_index,
                    match_index,
                    hint,
                    echo,
                    local,
                    incarnation,
                    now,
                },
            ),
            // Snapshot streaming is the snapshot task's (RAFT.md §3): the server
            // routes these to it before the core sees them.
            Message::InstallSnapshot { .. } | Message::InstallSnapshotResponse { .. } => {}
        }
    }

    /// A pre-vote is granted only by a server that has not heard from a leader within
    /// its minimum election timeout and whose log the candidate's is at least as up
    /// to date as; it changes nothing here.
    fn on_pre_vote(&mut self, from: ServerId, term: Term, last_index: Index, last_term: Term) {
        let granted = !self.quarantined
            && term > self.term
            && !self.heard_from_leader()
            && self.log_up_to_date(last_index, last_term);
        self.trace(TraceEvent::RaftVote {
            server: self.id.0,
            term,
            candidate: from.0,
            granted,
            pre: true,
        });
        self.send(
            from,
            Message::PreVoteResponse {
                term: if granted { term } else { self.term },
                granted,
            },
        );
    }

    fn on_pre_vote_response(&mut self, from: ServerId, term: Term, granted: bool) {
        if self.role != Role::PreCandidate {
            return;
        }
        if !granted {
            if term > self.term {
                self.become_follower(term, None);
            }
            return;
        }
        if term != self.term + 1 {
            return;
        }
        if !self.granted.contains(&from) {
            self.granted.push(from);
        }
        if self.counting_majority(&self.granted) {
            self.become_candidate();
        }
    }

    /// Figure 2's RequestVote handler: one vote per term, only for a log at least as
    /// up to date; the vote is persisted before the response leaves, and granting it
    /// resets the election timer (moirae rule 5).
    fn on_request_vote(&mut self, from: ServerId, last_index: Index, last_term: Term) {
        let granted = !self.quarantined
            && self.vote.is_none_or(|v| v == from)
            && self.log_up_to_date(last_index, last_term);
        if granted {
            if self.vote != Some(from) {
                self.vote = Some(from);
                self.hard_state_changed = true;
            }
            self.reset_election_timer();
        }
        self.trace(TraceEvent::RaftVote {
            server: self.id.0,
            term: self.term,
            candidate: from.0,
            granted,
            pre: false,
        });
        self.send(
            from,
            Message::RequestVoteResponse {
                term: self.term,
                granted,
            },
        );
    }

    fn on_request_vote_response(&mut self, from: ServerId, granted: bool) {
        if self.role != Role::Candidate || !granted {
            return;
        }
        if !self.granted.contains(&from) {
            self.granted.push(from);
        }
        if self.counting_majority(&self.granted) {
            self.become_leader();
        }
    }

    /// Figure 2's AppendEntries handler. The leader of our term resets the timer even
    /// when the consistency check fails (moirae rule 5). Entries already present with
    /// the same term are kept; the first conflict truncates from there (rule 3), the
    /// buggy variant from the previous index always. The response carries the index
    /// matched by this request (deviation D1) and, on a rejection, where to resume.
    fn on_append_entries(
        &mut self,
        from: ServerId,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
        sent: u64,
    ) {
        if self.role == Role::Leader {
            // Two leaders of one term cannot exist; a message saying so is ignored.
            return;
        }
        if self.role != Role::Follower {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;
        // The promise a response makes runs from `sent`; a quarantined server makes
        // none, so it echoes 0 and no lease is ever measured from it (RAFT.md §3).
        let echo = if self.quarantined { 0 } else { sent };
        // The response always carries the request's own previous index, so the
        // leader can tell an answer to its outstanding probe from a stale one.
        let request_prev = prev_index;
        // Entries the compacted prefix covers are committed here, so they match by
        // definition (RAFT.md §1): a request reaching below the prefix is answered
        // for its suffix past it, or as already held when it has none.
        let (prev_index, prev_term, entries) = if prev_index < self.snap_index {
            let end = prev_index + entries.len() as Index;
            if end <= self.snap_index {
                self.send(
                    from,
                    Message::AppendEntriesResponse {
                        term: self.term,
                        success: true,
                        prev_index: request_prev,
                        match_index: end,
                        hint: 0,
                        echo,
                        local: 0,
                        incarnation: 0,
                    },
                );
                return;
            }
            let tail: Vec<Entry> = entries
                .into_iter()
                .filter(|e| e.index > self.snap_index)
                .collect();
            (self.snap_index, self.snap_term, tail)
        } else {
            (prev_index, prev_term, entries)
        };
        let consistent = match self.term_at(prev_index) {
            Some(t) => t == prev_term,
            None => false,
        };
        if !consistent {
            let hint = match self.term_at(prev_index) {
                None => self.last_index() + 1,
                Some(conflicting) => {
                    // The first index of the conflicting term, so the leader skips
                    // it; the walk stops at the compacted prefix, whose entries
                    // cannot conflict.
                    let mut first = prev_index;
                    while first > self.snap_index + 1
                        && self.term_at(first - 1) == Some(conflicting)
                    {
                        first -= 1;
                    }
                    first.max(1)
                }
            };
            self.send(
                from,
                Message::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    prev_index: request_prev,
                    match_index: 0,
                    hint,
                    echo,
                    local: 0,
                    incarnation: 0,
                },
            );
            return;
        }
        if self
            .config
            .variants
            .contains(Variant::TruncateOnEveryAppend)
            && !entries.is_empty()
        {
            self.truncate(prev_index + 1);
        }
        let entries_len = entries.len() as Index;
        let mut to_append = Vec::new();
        for entry in entries {
            match self.term_at(entry.index) {
                Some(t) if t == entry.term => {}
                Some(_) => {
                    self.truncate(entry.index);
                    to_append.push(entry);
                }
                None => to_append.push(entry),
            }
        }
        // Deviation D1: every entry the request carried is now in the log, matched
        // or appended, so the match is the request's last index.
        let matched = prev_index + entries_len;
        if !to_append.is_empty() {
            self.append_local(to_append);
        }
        // The commit index follows the leader's, up to what this request confirmed.
        let new_commit = leader_commit.min(matched.max(self.commit));
        if new_commit > self.commit {
            self.commit = new_commit;
            self.trace(TraceEvent::RaftCommit {
                server: self.id.0,
                term: self.term,
                index: new_commit,
            });
            self.outputs.push(Output::Apply {
                through: new_commit,
            });
        }
        self.send(
            from,
            Message::AppendEntriesResponse {
                term: self.term,
                success: true,
                prev_index: request_prev,
                match_index: matched,
                hint: 0,
                echo,
                local: 0,
                incarnation: 0,
            },
        );
    }

    /// A success moves the follower's match forward and resumes the pipeline; a
    /// rejection moves `next` back to the hint and probes with one message at a
    /// time. While probing, only a rejection of the outstanding probe counts: the
    /// pipeline's other messages are rejected too, each carrying an older
    /// `prev_index`, and acting on each would restart the probe as many times.
    fn on_append_entries_response(&mut self, from: ServerId, ack: Ack) {
        if self.role != Role::Leader {
            return;
        }
        let Ack {
            success,
            prev_index,
            match_index,
            hint,
            echo,
            local,
            incarnation,
            now,
        } = ack;
        // An answer from a store other than the one recorded: what was known of
        // the follower's log is forgotten first, and the answer then processed
        // as usual — a rejection's hint is where the rebuilt log ends, and the
        // probe resumes from there rather than from the stale match.
        // D-042: store incarnations.
        let reset = self.note_incarnation(from, incarnation);
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        // Any answer in this term is a sign of life for check quorum and a promise
        // for the lease, whether the entries fit or not: the follower reset its
        // timer on the request either way (moirae rule 5). An echo of zero is a
        // quarantined follower's (RAFT.md §3): a sign of life, never a promise and
        // never a read's confirmation, since it grants votes to nobody and a vote
        // majority need not cross it. A rejection stamped incarnation 0 is a
        // refused server's, which has no store: a sign of life that check quorum
        // counts only beside re-seed progress in the same window. A quarantined
        // follower answers from its store, with its own incarnation, and counts.
        // D-049: a refused follower counts for check quorum only while its re-seed
        // stream progresses.
        if !success && incarnation == 0 {
            progress.refused_answered = true;
        } else {
            progress.active = true;
        }
        progress.quiet_ticks = 0;
        if echo != 0 {
            progress.promise = progress.promise.max(Some(echo));
            if let Some(moved) = progress.guard.observe(&self.config, now, echo, local) {
                self.trace(TraceEvent::RaftLeaseRevoked {
                    server: self.id.0,
                    follower: from.0,
                    offset_moved: moved,
                });
            }
            // A read-index round: an acknowledgement of a request sent after the
            // read arrived says this server was still leader then.
            for read in &mut self.reads {
                if !read.confirmed && echo >= read.at && !read.acks.contains(&from) {
                    read.acks.push(from);
                }
            }
        }
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        if success {
            // Monotone: a stale or duplicated response proposes only what was passed.
            progress.matched = progress.matched.max(match_index);
            progress.needs_snapshot = false;
            while progress
                .inflight
                .front()
                .is_some_and(|&end| end <= progress.matched)
            {
                progress.inflight.pop_front();
            }
            progress.next = progress.next.max(progress.matched + 1);
            progress.probe = None;
            let matched = progress.matched;
            let caught_up = matched == self.last_index();
            self.note_learner_round(from, matched);
            self.maybe_commit();
            self.maybe_compact();
            self.serve_reads();
            if self.transferee == Some(from) && caught_up {
                self.send_timeout_now(from);
            }
        } else {
            // A rejection of an append at index 1 is a server with no log at all
            // asking to be re-seeded (RAFT.md §3): index 0 is consistent with any
            // log, so no follower with one rejects it. It no longer blocks
            // compaction and is fed the snapshot.
            if prev_index == 0 {
                progress.needs_snapshot = true;
            }
            if progress.probe.is_some_and(|probe| probe != prev_index) {
                return;
            }
            progress.next = hint.max(1).max(progress.matched + 1);
            progress.probe = Some(progress.next - 1);
            progress.inflight.clear();
        }
        // After a reset the next index sits at the leader's end, where a
        // successful answer leaves nothing to send: an empty probe goes at once,
        // as a heartbeat would, so the rebuilt log is found within a round trip
        // rather than at the next heartbeat tick (D-042).
        self.replicate(from, reset);
    }

    /// Advances the commit index to the highest entry of the current term on a
    /// majority (§5.4.2), of both voter sets while joint (thesis §4.3); the buggy
    /// variants count entries of any term, or one merged majority. A leader that
    /// is not a voter of the configuration in force contributes nothing to the
    /// count, since [`Configuration::has_majority`] only counts members of each
    /// set: a leader outside `C_new` does not count itself for the majorities
    /// that commit it (thesis §4.3).
    fn maybe_commit(&mut self) {
        let last = self.last_index();
        let mut index = last;
        while index > self.commit {
            let term_ok = self
                .config
                .variants
                .contains(Variant::CountOlderTermForCommit)
                || self.term_at(index) == Some(self.term);
            if term_ok {
                let mut on: Vec<ServerId> = self
                    .progress
                    .iter()
                    .filter(|(_, p)| p.matched >= index)
                    .map(|(&s, _)| s)
                    .collect();
                on.push(self.id);
                if self.counting_majority(&on) {
                    self.commit = index;
                    self.trace(TraceEvent::RaftCommit {
                        server: self.id.0,
                        term: self.term,
                        index,
                    });
                    self.outputs.push(Output::Apply { through: index });
                    self.after_commit();
                    return;
                }
            }
            index -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RaftConfig, Variant, Variants};

    /// Every variant owns a bit of its own, and `Correct` owns none: the set is
    /// exactly as expressive as the vocabulary (D-045).
    #[test]
    fn every_bug_has_its_own_bit_and_correct_has_none() {
        assert_eq!(Variant::Correct.bit(), 0);
        let mut seen = 0u32;
        for &variant in Variant::BUGS {
            let bit = variant.bit();
            assert_ne!(bit, 0, "{variant:?} owns no bit");
            assert_eq!(bit.count_ones(), 1, "{variant:?} owns more than one bit");
            assert_eq!(
                seen & bit,
                0,
                "{variant:?} shares a bit with another variant"
            );
            seen |= bit;
        }
        assert_eq!(seen.count_ones() as usize, Variant::BUGS.len());
    }

    /// A set holds what was put in it and nothing else, whichever way it was
    /// built, and a single variant converts.
    #[test]
    fn a_set_holds_what_was_put_in_it() {
        let all = Variants::of(Variant::BUGS);
        for &variant in Variant::BUGS {
            let one = Variants::from(variant);
            assert!(one.contains(variant));
            assert_eq!(one.len(), 1);
            assert!(!one.is_correct());
            assert!(all.contains(variant));
            for &other in Variant::BUGS {
                assert_eq!(one.contains(other), other == variant);
            }
            assert_eq!(Variants::correct().with(variant), one);
        }
        assert_eq!(all.len() as usize, Variant::BUGS.len());
        assert_eq!(all.iter().collect::<Vec<_>>(), Variant::BUGS.to_vec());
    }

    /// The empty set is the correct server, and `Variant::Correct` puts nothing
    /// in a set — so asking a set whether it contains `Correct` asks whether it
    /// is empty.
    #[test]
    fn the_empty_set_is_the_correct_server() {
        let correct = Variants::default();
        assert_eq!(correct, Variants::correct());
        assert_eq!(correct, Variants::of(&[]));
        assert_eq!(correct, Variants::of(&[Variant::Correct]));
        assert_eq!(correct, Variants::from(Variant::Correct));
        assert!(correct.is_correct());
        assert!(correct.is_empty());
        assert_eq!(correct.len(), 0);
        assert!(correct.contains(Variant::Correct));
        assert_eq!(correct.iter().count(), 0);
        assert_eq!(RaftConfig::default().variants, correct);

        let one = Variants::from(Variant::NoPreVote);
        assert!(!one.contains(Variant::Correct));
    }

    /// Idempotent and order-free, as a set is: the same bugs in any order and
    /// any number of times are the same set.
    #[test]
    fn a_set_is_a_set() {
        let pair = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
        assert_eq!(
            pair,
            Variants::of(&[Variant::SharedSnapshotDir, Variant::IgnoreIncarnation])
        );
        assert_eq!(
            pair,
            Variants::of(&[
                Variant::IgnoreIncarnation,
                Variant::IgnoreIncarnation,
                Variant::SharedSnapshotDir,
            ])
        );
        assert_eq!(pair.with(Variant::IgnoreIncarnation), pair);
        assert_eq!(pair.len(), 2);
    }

    /// The rate lines print it, so it reads (D-045).
    #[test]
    fn the_debug_reads() {
        assert_eq!(format!("{:?}", Variants::correct()), "Correct");
        assert_eq!(
            format!("{:?}", Variants::from(Variant::SharedSnapshotDir)),
            "{SharedSnapshotDir}"
        );
        assert_eq!(
            format!(
                "{:?}",
                Variants::of(&[Variant::SharedSnapshotDir, Variant::IgnoreIncarnation])
            ),
            "{IgnoreIncarnation, SharedSnapshotDir}"
        );
    }
}

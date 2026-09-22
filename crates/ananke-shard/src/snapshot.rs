//! The node's `snapshot` task, keyed by range and follower (SHARD.md §4; Q14, Q41).
//!
//! Today one server is one group, so one snapshot task holds one stream, one staging
//! directory sits under the engine directory, a version directory is named by index
//! and take alone, and a sweep deletes every version directory the store's single
//! snapshot record does not name (snapshot.rs:96-101, 119-121, 193-228, 1018-1027;
//! node.rs:1430, 1835). On a node all four are wrong at once: two ranges' takes at one
//! index collide, one range's sweep deletes another range's checkpoints, and the
//! re-seeds heading for one node restart each other (SHARD.md §11, raft 14).
//!
//! What this module is:
//!
//! - **One task, keyed by (range, follower) on the way out and (range, sender) on the
//!   way in.** A send is keyed by the follower it feeds, so a leader feeds every
//!   designated follower of a range at once (D-043), and there is no per-node cap on
//!   streams *sent* (Q14). A receive is keyed by the sender, so there is one assembly
//!   per (range, sender), and a chunk of another range or another sender never
//!   abandons the assembly a stream is using.
//! - **A per-node cap on what is received and assembled** (Q14). Streams over the cap
//!   wait: a chunk for an unadmitted (range, sender) is answered with a restart and
//!   touches nothing, and a freed slot is granted to a waiter on its next chunk — not
//!   reserved for one, because nothing here has a clock and the waiter whose turn it
//!   is may be a leader that has since been replaced. The re-seed shape caps two of a
//!   node's four ranges on purpose, "so that re-seeds toward it wait for one another"
//!   (SHARD.md:2260-2261, where the phrase wraps the line break a grep for it must
//!   cross).
//! - **Paths keyed by range.** [`staging_name`] and [`version_name`] put the range in
//!   the name, and [`Snapshots::sweep`] is a range's own sweep: it proposes for
//!   deletion only version directories of the range it is sweeping.
//! - **Chunks in frames of their own** on the task's own socket handle (Q41): a
//!   256 KiB chunk never shares a frame with a heartbeat, and never goes through the
//!   per-peer outbox.
//! - **Installs as D-066 decided them.** Every install on the node is the live install
//!   of the range's spans into the running engine, with the range's repair carried in
//!   the same switch; the whole-store staged install adopted at the next start is not
//!   kept for a replica's install, and nothing here reopens the engine. `RaftAdopted`
//!   records only a node taking a fresh directory after a whole-node refusal, which is
//!   [`Snapshots::adopt_fresh`] and never an install.
//!
//! Nothing here touches a disk or a socket: the task's discipline is a decision about
//! keys, caps, frames and switches, and it is asserted without a simulation, as
//! [`mod@crate::round`]'s is. The engine call an [`Install`] describes
//! (`Engine::install_spans`, D-068) and the streams' bytes are the node's wiring,
//! which Stage B's scenarios slice puts under the sweeps.
//!
//! Q15's whole-node refusal and its re-seed are a later slice's; what this module
//! builds is the path a follower behind its leader's compacted prefix takes, which is
//! the same path each range of that re-seed will take.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::Range as KeyRange;
use std::path::PathBuf;

use ananke_env::MAX_FRAME_LEN;
use ananke_raft::types::{Index, ServerId, Term};
use bytes::Bytes;

use crate::frame::{Builder, encoded_len};
use crate::outbox::Oversized;
use crate::range::RangeId;
use crate::variant::{NodeVariant, NodeVariants};

/// The name of the staging directory one (range, sender) assembles into.
///
/// Keyed by range, which §11's raft item 14 asks for, **and** by sender. The two
/// requirements of that item — "staging, versions and the sweep need keying by range"
/// and "one assembly per (range, sender)" — are only consistent together if the
/// staging name carries both: two senders of one range, a leader and the stale leader
/// it replaced, each hold an assembly, and two assemblies staging into one directory
/// write over each other's files exactly as two ranges would.
// PROPOSED(D-075): the staging directory is keyed by range and sender, not by range
// alone, because the receiver keeps an assembly per (range, sender).
#[must_use]
pub fn staging_name(range: RangeId, from: ServerId) -> String {
    format!("staging-{range}-s{}", from.0)
}

/// The name of a version directory: `snap-r<range>-<index>-<take>`.
///
/// Today's is `snap-<index>-<take>` (snapshot.rs:119-121), which on a node collides
/// whenever two ranges take a checkpoint at one index — and ranges on a node apply
/// their own streams of commands, so equal indexes are ordinary, not rare.
#[must_use]
pub fn version_name(range: RangeId, index: Index, take: u64) -> String {
    format!("snap-{range}-{index}-{take}")
}

/// The range, index and take a version directory's name carries, or `None` for a name
/// that is not one of this layout's.
///
/// A name in the one-group layout — `snap-<index>-<take>`, or the shared
/// `snap-<index>` — parses as `None` here: on a node it belongs to no range, so a
/// range's sweep must not propose it. What becomes of the old layout's directories in
/// an engine directory that predates the node is the node's start, not its sweep.
#[must_use]
pub fn parse_version(name: &str) -> Option<(RangeId, Index, u64)> {
    let rest = name.strip_prefix("snap-r")?;
    let (range, rest) = rest.split_once('-')?;
    let (index, take) = rest.split_once('-')?;
    Some((
        RangeId(range.parse().ok()?),
        index.parse().ok()?,
        take.parse().ok()?,
    ))
}

/// What a chunk names its stream by: the leader's term and the snapshot's last index
/// and term, as [`ananke_raft::message::Message::InstallSnapshot`] carries them.
///
/// The sender is not part of it, because the assembly is already keyed by sender: a
/// chunk whose identity differs from the one its (range, sender) assembly holds is
/// that sender starting over, and starts the staging directory over with it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Identity {
    /// The sending leader's term.
    pub term: Term,
    /// The snapshot's last index.
    pub last_index: Index,
    /// That entry's term.
    pub last_term: Term,
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "term {} at ({}, {})",
            self.term, self.last_index, self.last_term
        )
    }
}

/// Where a chunk the task is sending travels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    /// A frame of its own, on the snapshot task's own socket handle: one message,
    /// which is the chunk (Q41).
    OwnFrame(Bytes),
    /// The per-peer outbox, where the chunk is cut into a frame with whatever else is
    /// queued for that peer. No correct route leads here.
    Outbox,
}

/// What starting a stream to a follower came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Started {
    /// The stream is running, staging on the follower's side under this name.
    Streaming {
        /// The name the follower stages under.
        ///
        /// The follower assembles under `staging_name(range, sender)`, and the sender
        /// is this node: the name is built from the node's own id, never the
        /// follower's. A leader streaming one range to three followers names one
        /// directory, and it is the one each of the three stages under. It is derived
        /// rather than kept per stream ([`Snapshots::staging_sent`]), so there is one
        /// place for it to be right.
        staging: String,
    },
    /// The node is already sending as many streams as it allows. No correct node
    /// answers this: Q14 puts no cap on streams sent.
    Waiting,
}

/// What one arriving chunk came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Landing {
    /// Staged into this directory, which belongs to this (range, sender) alone.
    Staged {
        /// The staging directory the chunk was written under.
        dir: PathBuf,
        /// The bytes this assembly has staged, this chunk included.
        staged: usize,
    },
    /// The sender started over: its identity differs from the one this assembly held,
    /// so this assembly's staging directory starts again, empty, and the sender is
    /// asked for the stream from its first byte (RAFT.md:203-207). No *other* assembly
    /// is touched, and the chunk that restarted the assembly is not kept — not even
    /// when it is the stream's last, which is what [`NodeVariant::CompleteOnRestart`]
    /// gets wrong.
    // PROPOSED(D-075): a restarting chunk is discarded with the assembly it restarted,
    // because nothing here carries the chunk's offset and so nothing can tell a chunk
    // that starts the new stream from one in the middle of it (snapshot.rs:1018-1027).
    Restarted {
        /// The staging directory, started over and holding nothing.
        dir: PathBuf,
        /// What the assembly has staged now: none of it. The sender's next chunk is
        /// the new stream's first byte.
        staged: usize,
    },
    /// The node is assembling as many streams as its cap allows and this is not one of
    /// them. Nothing was written and no other assembly was disturbed; the sender is
    /// told to restart, and this (range, sender) takes a slot on the first chunk it
    /// sends while one is free.
    Waiting {
        /// How many (range, sender)s asked for a slot before this one and have not
        /// been given one. A queue position, not a reservation: a slot is granted to a
        /// stream that is asking for it, never held for one that may be gone.
        ahead: usize,
    },
    /// A chunk for a range this node does not host (RAFT.md:214-218). Nothing was
    /// written, no slot was
    /// taken and no assembly was disturbed; the sender is told to start over, as an
    /// over-cap chunk is, and finds the range where it now lives.
    ///
    /// `range` and `from` reach [`on_chunk`](Snapshots::on_chunk) from a *peer's*
    /// `InstallSnapshot`, so an unhosted range is something the wire can say: a leader
    /// that has not yet learned the rebalancer moved the range off this node (Q33)
    /// streams to the old replica, and a garbled range id says it too. The node
    /// refuses the chunk rather than failing on it — `Builder::push`'s line, that a
    /// caller's lost message "is its bug and not the wire's" (frame.rs:134-137), cuts
    /// the same way here: the *send* half still fails where the node lost track of a
    /// range ([`stream`](Snapshots::stream)), because there the range is the node's
    /// own claim and not a peer's.
    // PROPOSED(D-075): a chunk of an unhosted range is refused on arrival, before it
    // takes a slot, rather than panicking on the stream's last chunk.
    NotHosted,
    /// This stream's last chunk again, at the identity this assembly already
    /// completed: nothing is staged and nothing is installed a second time. A last
    /// chunk whose answer was lost is resent as a matter of course (RAFT.md:200-202),
    /// and the node answers the resend what it answered the first: installed.
    ///
    /// The assembly remembers it until [`finish`](Snapshots::finish), so the node must
    /// not finish a completed assembly until it has answered the sender.
    // PROPOSED(D-075): a duplicate last chunk is answered idempotently rather than
    // installing again, because nothing here carries an offset and a second install
    // would switch from a staging directory the first switch may already have
    // consumed.
    Installed {
        /// The identity that was installed, which is this chunk's own.
        at: Identity,
    },
    /// The stream's last chunk: what the node installs, and how.
    Complete(Box<Install>),
}

/// The install one completed stream asks for: D-066's live install, in full.
///
/// It is a decision, not a call: the node hands the spans, the staged source and the
/// repair to `Engine::install_spans` (D-068), which removes every key of the spans and
/// adds the staged tables and the repair's writes in one manifest switch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Install {
    /// The range being installed.
    pub range: RangeId,
    /// The sender whose stream completed.
    pub from: ServerId,
    /// The snapshot installed.
    pub at: Identity,
    /// The staging directory the tables are taken from.
    pub source: PathBuf,
    /// The range's key intervals, sorted and disjoint: its Raft state under tenant 0
    /// and its user keys under tenant 2 (D-066, D-068).
    pub spans: Vec<KeyRange<Bytes>>,
    /// Whether the range's repair — term, vote, applied index, snapshot record, log
    /// tail, configuration, quarantine and incarnation, with tombstones for the log
    /// keys the tail does not replace (RAFT.md:238-246) — is carried in the switch.
    /// The correct node never switches without it (D-066).
    pub repair_in_switch: bool,
    /// Whether this install ends the node's run-loop incarnation and reopens the
    /// engine. A live install does not: the node's other ranges keep running
    /// (SHARD.md §11, storage 5).
    pub reopens_engine: bool,
    /// Whether this install is what `RaftAdopted` records. It never is: that event is
    /// a node taking a fresh directory after a whole-node refusal, and nothing else
    /// (D-066).
    pub adopted: bool,
}

/// A node taking a fresh directory as its store after a whole-node refusal: the one
/// thing `RaftAdopted` records on the node (D-066, Q15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Adopted {
    /// The fresh directory, beside the refused one.
    pub dir: PathBuf,
    /// The ranges that had installed into *this* directory when the event was traced:
    /// **zero on every path that returns**, and a constant, like [`Streams::abandoned`]
    /// and named as one here. It is read back from what the task counted per directory
    /// rather than written down, but `installed` is keyed by engine directory and a
    /// fresh directory has no entry of its own, so 0 is the only value that reaches
    /// this field. [`Snapshots::adopt_fresh`]'s assertion is what keeps that true — it
    /// refuses a directory that has taken an install — so the figure is the
    /// assertion's, not an observation's.
    pub ranges_installed: usize,
}

/// What the task is carrying: the measurement §12 asks of the snapshot path, counted
/// rather than timed, so it is deterministic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Streams {
    /// Streams being sent, keyed by (range, follower). Uncapped (Q14).
    pub sending: usize,
    /// Assemblies open, keyed by (range, sender). At most the receive cap.
    pub receiving: usize,
    /// (range, sender) pairs waiting for a slot.
    pub waiting: usize,
    /// Chunks the task has routed out.
    pub chunks: usize,
    /// Frames those chunks travelled in. One per chunk, on the correct node.
    pub frames: usize,
    /// Assemblies abandoned for a chunk that was not theirs. Zero, on the correct
    /// node: an assembly is only ever restarted by its own sender.
    pub abandoned: usize,
    /// Streams completed into an install, over the task's whole life. What
    /// [`Adopted::ranges_installed`] reads is this count for one directory.
    pub installs: usize,
    /// Assemblies started over for a chunk of another identity of their own sender.
    pub restarts: usize,
    /// Chunks refused because the node does not host the range they name. Nothing was
    /// written and no slot was taken for any of them.
    pub unhosted: usize,
    /// Assemblies displaced by a chunk of the same range from a sender at a higher
    /// term: the slot of a leader that range's own Raft has superseded.
    pub displaced: usize,
    /// Last chunks that arrived again at the identity their assembly had already
    /// completed, answered installed and installed no second time.
    pub duplicates: usize,
    /// `RaftAdopted` events. One per fresh directory, none per install.
    pub adoptions: usize,
}

/// One assembly: one (range, sender)'s staging directory and what it holds.
///
/// `from` and `at` are `None` until the first chunk lands, so that chunk is a stream
/// starting and not a stream restarting. `from` is redundant on the correct node,
/// where the key already carries it, and is what [`NodeVariant::OneAssemblyPerNode`]
/// gets wrong: today's identity is (sender, term, last index, last term), and one
/// assembly for the whole node is abandoned whenever the sender changes
/// (snapshot.rs:1018-1027).
#[derive(Clone, Debug, Default)]
struct Assembly {
    from: Option<ServerId>,
    at: Option<Identity>,
    staged: usize,
    /// The identity this assembly has already completed an install for, until the
    /// node [`finish`](Snapshots::finish)es it. A last chunk whose answer was lost is
    /// resent (RAFT.md:200-202), and the resend must not install a second time.
    complete: Option<Identity>,
}

/// One stream being sent to one follower of one range.
///
/// The name the follower stages under is not kept here: it is `staging_name(range,
/// self.me)` for every follower of the range, so a copy per stream would be a second
/// place for it to be wrong. [`Snapshots::staging_sent`] derives it.
#[derive(Clone, Debug)]
struct Sending {
    at: Identity,
}

/// The node's `snapshot` task.
///
/// One per node, holding every range's streams both ways. It is stepped by the node:
/// [`stream`](Self::stream) starts a send, [`route`](Self::route) puts a chunk on the
/// wire, [`on_chunk`](Self::on_chunk) takes one off it, and
/// [`finish`](Self::finish) ends an assembly and admits whoever was waiting.
pub struct Snapshots {
    engine_dir: PathBuf,
    me: ServerId,
    cap: usize,
    variants: NodeVariants,
    hosted: BTreeMap<RangeId, Vec<KeyRange<Bytes>>>,
    sending: BTreeMap<(RangeId, ServerId), Sending>,
    receiving: BTreeMap<(RangeId, ServerId), Assembly>,
    waiting: VecDeque<(RangeId, ServerId)>,
    /// Ranges installed into each engine directory this task has had: what
    /// [`Snapshots::adopt_fresh`] reads to answer whether a directory is fresh.
    installed: BTreeMap<PathBuf, usize>,
    meters: Streams,
}

/// The cap the variant [`NodeVariant::CapStreamsSent`] puts on streams sent: one at a
/// time, which is what `SharedSnapshotDir` made of a leader's streams in Phase 2 and
/// what D-043 decided against.
const BUGGY_SEND_CAP: usize = 1;

impl Snapshots {
    /// A task on node `me`, staging under `engine_dir`, assembling at most `cap`
    /// streams at once.
    ///
    /// `me` is the node's own id: the staging name a follower assembles under carries
    /// the *sender*, so the send half can only name the directory its followers stage
    /// under if it knows who it is.
    ///
    /// `cap` is Q14's per-node cap on what is assembled at once. Nothing in the design
    /// documents fixes a default: the re-seed shape sets it to two, below its four
    /// ranges, on purpose, and D-066 leaves the quorum scenario's to the owner. The
    /// recommendation this slice records is that a scenario which is not about the cap
    /// sets it at or above the node's range count **plus the senders a range may have
    /// at once** — a leader and the stale leader it replaced are two assemblies of one
    /// range, by the same decision that keys staging by sender — so no stream waits by
    /// accident. A cap equal to the range count alone starves a range as soon as any
    /// range has two senders.
    ///
    /// It is one cap, on assemblies. `SHARD.md:2231` and `SHARD.md:3035` say "caps",
    /// received *and* assembled; nothing here receives a chunk without assembling it,
    /// so the two are one number in this module, and a separate bound on streams a
    /// node lets in before it assembles them belongs with the sockets the wiring slice
    /// holds.
    // PROPOSED(D-075): the receive cap is a node setting with no default; a scenario
    // not about the cap sets it at or above its range count plus the concurrent
    // senders any of its ranges may have. One cap, on assemblies.
    #[must_use]
    pub fn new(
        engine_dir: impl Into<PathBuf>,
        me: ServerId,
        cap: usize,
        variants: NodeVariants,
    ) -> Self {
        Self {
            engine_dir: engine_dir.into(),
            me,
            cap,
            variants,
            hosted: BTreeMap::new(),
            sending: BTreeMap::new(),
            receiving: BTreeMap::new(),
            waiting: VecDeque::new(),
            installed: BTreeMap::new(),
            meters: Streams::default(),
        }
    }

    /// Tells the task which key intervals a range it hosts lives in: its Raft state
    /// and its user keys (D-066). An install of that range switches exactly these.
    ///
    /// # Panics
    ///
    /// If the spans are not sorted and disjoint, which `Engine::install_spans` refuses
    /// (`SpansOverlap`, D-068), or if there are no spans at all, which it refuses the
    /// same way: `EmptySpan` is "the set is empty **or** any span in it holds no key"
    /// (engine.rs:1799; D-068), and only the second half of that was asserted here. A
    /// caller that hands over overlapping spans, or none, has lost track of what the
    /// range holds, which is a bug here and not a refusal to discover at the switch —
    /// and a range registered with no spans is refused at *every* switch, which is the
    /// permanent refusal this panic exists to prevent.
    pub fn host(&mut self, range: RangeId, spans: Vec<KeyRange<Bytes>>) {
        assert!(
            !spans.is_empty(),
            "{range} is hosted with no spans: every install of it would be refused"
        );
        assert!(
            spans.windows(2).all(|w| w[0].end <= w[1].start),
            "{range}'s spans are not sorted and disjoint"
        );
        assert!(
            spans.iter().all(|s| s.start < s.end),
            "{range} has an empty span"
        );
        self.hosted.insert(range, spans);
    }

    /// What the task is carrying.
    #[must_use]
    pub fn meters(&self) -> Streams {
        Streams {
            sending: self.sending.len(),
            receiving: self.receiving.len(),
            waiting: self.waiting.len(),
            ..self.meters
        }
    }

    /// The staging directory one (range, sender) assembles into.
    #[must_use]
    pub fn staging(&self, range: RangeId, from: ServerId) -> PathBuf {
        if self.variants.contains(NodeVariant::SharedStagingDir) {
            // The variant: today's one staging directory per engine directory
            // (snapshot.rs:96-101). Two assemblies then write over each other's files.
            return self.engine_dir.join("staging");
        }
        if self.variants.contains(NodeVariant::StagingByRangeAlone) {
            // The variant: keyed by range alone, the literal reading of §11 raft 14.
            // The two senders of one range — a leader and the stale leader it replaced
            // — then stage into one directory, each holding an assembly of its own.
            return self.engine_dir.join(format!("staging-{range}"));
        }
        self.engine_dir.join(staging_name(range, from))
    }

    /// The version directory a range's take at `index` writes.
    #[must_use]
    pub fn version(&self, range: RangeId, index: Index, take: u64) -> PathBuf {
        if self.variants.contains(NodeVariant::VersionDirWithoutRange) {
            // The variant: today's `snap-<index>-<take>` (snapshot.rs:119-121), which
            // two ranges taking at one index share.
            return self.engine_dir.join(format!("snap-{index}-{take}"));
        }
        self.engine_dir.join(version_name(range, index, take))
    }

    /// What a sweep of `range` deletes, given every name in the engine directory and
    /// the (index, take) pairs `range`'s own snapshot record pins.
    ///
    /// A range sweeps its own versions and nothing else. Today's sweep deletes every
    /// unpinned version directory (snapshot.rs:193-228), which on a node is every
    /// other range's checkpoints as well as its own debris.
    #[must_use]
    pub fn sweep(
        &self,
        names: &[String],
        range: RangeId,
        keep: &BTreeSet<(Index, u64)>,
    ) -> Vec<String> {
        let across = self.variants.contains(NodeVariant::SweepAcrossRanges);
        names
            .iter()
            .filter(|name| {
                let Some((owner, index, take)) = parse_version(name) else {
                    return false;
                };
                if !across && owner != range {
                    return false;
                }
                // A pinned version of *this* range is kept. Under the variant a pinned
                // version of another range is not: its pairs are not in this record.
                !(owner == range && keep.contains(&(index, take)))
            })
            .cloned()
            .collect()
    }

    /// Starts a stream of `range`'s snapshot to `to`.
    ///
    /// There is no per-node cap on streams sent, so a leader feeds every designated
    /// follower of a range at once (Q14, D-043); a stream already running to that
    /// (range, follower) at another identity is replaced, which is the leader having
    /// moved on.
    ///
    /// # Panics
    ///
    /// If the range is not one this node hosts. A leader of a range is a replica of
    /// it, so the range being streamed is the node's own claim and not a peer's: the
    /// node fails here, where it lost track of the range. The *receive* half answers
    /// the same mistake with [`Landing::NotHosted`], because there the range is a
    /// peer's word.
    pub fn stream(&mut self, range: RangeId, to: ServerId, at: Identity) -> Started {
        assert!(
            self.hosted.contains_key(&range),
            "{range} is streamed to {to} but is not hosted here"
        );
        if self.variants.contains(NodeVariant::CapStreamsSent)
            && self.sending.len() >= BUGGY_SEND_CAP
            && !self.sending.contains_key(&(range, to))
        {
            // The variant: a per-node cap on streams sent, so a leader feeds its
            // followers one at a time and the rest wait on the slowest (D-043).
            return Started::Waiting;
        }
        self.sending.insert((range, to), Sending { at });
        Started::Streaming {
            staging: self.staging_sent(range),
        }
    }

    /// The name a follower of `range` stages this node's stream under.
    ///
    /// The follower keys its staging directory by the *sender*, and the sender is this
    /// node: `staging_name(range, self.me)`, never the follower's id. One name serves
    /// every follower of the range, and a name built from a follower's would name a
    /// path that exists on no node.
    #[must_use]
    pub fn staging_sent(&self, range: RangeId) -> String {
        staging_name(range, self.me)
    }

    /// Whether a stream to this (range, follower) is running, at this identity.
    #[must_use]
    pub fn is_streaming(&self, range: RangeId, to: ServerId, at: Identity) -> bool {
        self.sending.get(&(range, to)).is_some_and(|s| s.at == at)
    }

    /// Ends the stream to this (range, follower).
    pub fn sent(&mut self, range: RangeId, to: ServerId) {
        self.sending.remove(&(range, to));
    }

    /// Puts one chunk of a running stream on the wire: a frame of its own, on the
    /// task's own socket handle (Q41).
    ///
    /// # Errors
    ///
    /// [`Oversized`] when the chunk does not fit a frame at all, which the outbox
    /// refuses the same way: nothing splits a message across frames.
    ///
    /// # Panics
    ///
    /// If no stream to this (range, follower) is running — a chunk framed for a
    /// follower nothing is being streamed to is the send half's two ends having come
    /// apart — or if the chunk carries no bytes, which is the caller's own checkpoint
    /// file, not anything a peer sent. An empty chunk is not an oversized one, and is
    /// not reported as one; `Builder::push` refuses it too (frame.rs:134-137).
    pub fn route(
        &mut self,
        range: RangeId,
        to: ServerId,
        chunk: &[u8],
    ) -> Result<Route, Oversized> {
        assert!(
            self.sending.contains_key(&(range, to)),
            "a chunk of {range} routed to {to}, which no stream is running to"
        );
        assert!(
            !chunk.is_empty(),
            "a chunk of {range} for {to} carries no bytes"
        );
        if encoded_len(chunk.len()) > MAX_FRAME_LEN {
            return Err(Oversized {
                to,
                range,
                len: chunk.len(),
            });
        }
        self.meters.chunks += 1;
        if self.variants.contains(NodeVariant::ChunksInBatchFrames) {
            // The variant: the chunk goes through the per-peer outbox, where it is cut
            // into a frame with whatever else is queued for that peer — so a 256 KiB
            // chunk spends the frame a round's heartbeats needed (Q41, §10).
            return Ok(Route::Outbox);
        }
        let mut builder = Builder::new(MAX_FRAME_LEN);
        builder.push(range, chunk);
        self.meters.frames += 1;
        Ok(Route::OwnFrame(builder.finish()))
    }

    /// Takes one chunk off the wire for `range` from `from`.
    ///
    /// `done` is the stream's last chunk, which asks for the install — unless this
    /// same chunk restarted the assembly, in which case there is nothing to install:
    /// the directory has just been started over, and what the node has is one chunk of
    /// a stream whose earlier bytes it never saw. That stream is restarted from its
    /// first byte (RAFT.md:203-207) and completes on the next pass.
    ///
    /// A chunk naming a range this node does not host is answered
    /// [`NotHosted`](Landing::NotHosted) before anything is admitted: it takes no slot
    /// and disturbs no assembly. A chunk resending the last chunk of a stream this
    /// assembly already completed is answered [`Installed`](Landing::Installed) and
    /// installs nothing a second time.
    ///
    /// # Panics
    ///
    /// If `range` completes a stream but was never [`host`](Self::host)ed, which the
    /// refusal above makes unreachable on the correct node and which
    /// [`NodeVariant::AdmitsAnUnhostedRange`] is the way to reach. The install
    /// switches exactly the range's spans, and a node that cannot say what a range
    /// holds has lost track of it: it fails here rather than switching an empty span
    /// set the engine refuses (`EmptySpan`, D-068).
    pub fn on_chunk(
        &mut self,
        range: RangeId,
        from: ServerId,
        at: Identity,
        bytes: usize,
        done: bool,
    ) -> Landing {
        if !self.hosted.contains_key(&range)
            && !self.variants.contains(NodeVariant::AdmitsAnUnhostedRange)
        {
            // Refused before anything is admitted, so a range this node does not host
            // takes no slot from one it does: `range` is a peer's word, and a peer
            // that has not learned the range moved (Q33) would otherwise starve every
            // hosted range on the node under the cap.
            self.meters.unhosted += 1;
            return Landing::NotHosted;
        }
        let key = self.key(range, from);
        if !self.receiving.contains_key(&key) && !self.admit(key, at) {
            let ahead = self
                .waiting
                .iter()
                .position(|w| *w == key)
                .expect("admit queues every key it refuses");
            return Landing::Waiting { ahead };
        }
        let dir = self.staging(key.0, key.1);
        // The variant: the resent last chunk of a stream already installed installs
        // again, from a staging directory the first switch may have consumed.
        let installs_duplicates = self
            .variants
            .contains(NodeVariant::InstallsADuplicateLastChunk);
        let assembly = self.receiving.get_mut(&key).expect("admitted");
        let fresh = assembly.at.is_none();
        let restarted = !fresh && (assembly.from != Some(from) || assembly.at != Some(at));
        if !restarted && assembly.complete == Some(at) && !installs_duplicates {
            // This stream's last chunk again, resent because its answer was lost
            // (RAFT.md:200-202). Nothing is staged and nothing is installed twice;
            // the node answers what it answered the first time.
            self.meters.duplicates += 1;
            return Landing::Installed { at };
        }
        // An assembly abandoned for a chunk that is not its sender's: the count the
        // check reads. On the correct node it never happens, because the key the
        // assembly is under already carries the sender.
        let abandoned = restarted && assembly.from != Some(from);
        // The variant: the chunk that restarts an assembly is kept and the stream
        // completes on it, so the node installs a directory still holding the
        // abandoned stream's files and is never told to start it over
        // (RAFT.md:203-207).
        let completes_a_restart =
            restarted && done && self.variants.contains(NodeVariant::CompleteOnRestart);
        if restarted && !completes_a_restart {
            // The staging directory starts over and keeps nothing, this chunk
            // included: the sender is asked for the stream from its first byte, and
            // the assembly is as it was before any chunk landed. Keeping the chunk
            // would need its offset, which nothing here carries.
            *assembly = Assembly::default();
        } else {
            assembly.staged = if restarted {
                bytes
            } else {
                assembly.staged + bytes
            };
            assembly.from = Some(from);
            assembly.at = Some(at);
            if done {
                // Remembered until the node finishes this assembly, so the resend of
                // a last chunk whose answer was lost is answered, not installed.
                assembly.complete = Some(at);
            }
        }
        let staged = assembly.staged;
        if abandoned {
            self.meters.abandoned += 1;
        }
        if restarted {
            self.meters.restarts += 1;
            if !completes_a_restart {
                return Landing::Restarted { dir, staged };
            }
        }
        if done {
            let mut spans = self.hosted.get(&range).cloned().unwrap_or_else(|| {
                panic!("{range} completed a stream from {from} but is not hosted here")
            });
            if self.variants.contains(NodeVariant::InstallWrongRangesSpans) {
                // The variant: the install carries the node's first hosted range's
                // spans instead of the completed range's, which on a node whose ranges
                // are a `BTreeMap` is its lowest range id. `Engine::install_spans`
                // removes every key of the spans it is given (D-068), so a re-seed of
                // r4 would delete r1's Raft state and user keys while r1 is running.
                spans = self.hosted.values().next().cloned().expect("hosted");
            }
            self.meters.installs += 1;
            *self.installed.entry(self.engine_dir.clone()).or_default() += 1;
            return Landing::Complete(Box::new(Install {
                range,
                from,
                at,
                source: dir,
                spans,
                // D-066: the switch is made only with the range's repair carried in
                // it. The variant makes it as the stream's last chunk arrives.
                repair_in_switch: !self.variants.contains(NodeVariant::InstallWithoutRepair),
                // A live install ends no incarnation and reopens no engine.
                reopens_engine: false,
                // `RaftAdopted` is the fresh directory's, never an install's.
                adopted: self.variants.contains(NodeVariant::AdoptedOnRangeInstall),
            }));
        }
        Landing::Staged { dir, staged }
    }

    /// Ends the assembly for this (range, sender) — its install switched, or its
    /// stream gave up — and frees its slot under the receive cap.
    ///
    /// It also forgets the identity this (range, sender) last completed, so the node
    /// must not finish a completed assembly until it has answered the sender: a last
    /// chunk resent before the answer is [`Landing::Installed`], one resent after the
    /// assembly is finished is a stream of its own again (RAFT.md:200-202, 218-220).
    ///
    /// Returns the (range, sender) at the head of the queue, as a hint for the node's
    /// trace: the slot is *not* reserved for it. A waiter is admitted when its own
    /// next chunk arrives ([`on_chunk`](Self::on_chunk)), because a reservation would
    /// be held for a sender that may never send again — a waiter's leader can change
    /// while it waits, and nothing here has a clock to reclaim what it left behind.
    /// That is [`NodeVariant::SlotReservedForWaiter`].
    // PROPOSED(D-075): a freed slot is granted to the first waiter that asks for it,
    // not reserved for the head of the queue, so a departed sender costs nothing.
    pub fn finish(&mut self, range: RangeId, from: ServerId) -> Option<(RangeId, ServerId)> {
        let key = self.key(range, from);
        if self.receiving.remove(&key).is_none() {
            self.waiting.retain(|w| *w != key);
            return None;
        }
        if self.variants.contains(NodeVariant::SlotReservedForWaiter) {
            // The variant: the freed slot is reserved for the head of the queue, held
            // by that (range, sender) until it finishes — which a sender replaced as
            // leader never does, so the node's slots fill with reservations for
            // senders that are gone and it re-seeds nothing more.
            let next = self.waiting.pop_front()?;
            self.receiving.insert(next, Assembly::default());
            return Some(next);
        }
        self.waiting.front().copied()
    }

    /// The node taking a fresh directory as its store after a whole-node refusal: the
    /// one `RaftAdopted` on the node (D-066, Q15).
    ///
    /// The assemblies, the waiters and the streams being sent are dropped with the
    /// refused store: they named directories under it, and a stream that was feeding
    /// this node starts again against the fresh one.
    ///
    /// # Panics
    ///
    /// If a range has already installed into the directory being adopted. The event is
    /// traced when the fresh engine is opened and *before* any range installs into it,
    /// so a directory that has taken an install is not the one this event describes.
    // PROPOSED(D-075): adopting a fresh directory drops what the refused one was
    // assembling and sending, rather than carrying it across the switch.
    pub fn adopt_fresh(&mut self, dir: impl Into<PathBuf>) -> Adopted {
        let dir = dir.into();
        let ranges_installed = self.installed.get(&dir).copied().unwrap_or_default();
        assert_eq!(
            ranges_installed,
            0,
            "{} has taken {ranges_installed} install(s): a fresh directory is adopted \
             before any range installs into it",
            dir.display()
        );
        self.meters.adoptions += 1;
        self.engine_dir = dir;
        self.receiving.clear();
        self.waiting.clear();
        self.sending.clear();
        Adopted {
            dir: self.engine_dir.clone(),
            ranges_installed,
        }
    }

    /// The key an arriving chunk is assembled under.
    fn key(&self, range: RangeId, from: ServerId) -> (RangeId, ServerId) {
        if self.variants.contains(NodeVariant::OneAssemblyPerNode) {
            // The variant: one assembly for the whole node, as the one-group snapshot
            // task holds one stream (snapshot.rs:1018-1027; node.rs:1430). Every range
            // and every sender share it, so each chunk abandons the last one's work.
            return (RangeId(0), ServerId(0));
        }
        (range, from)
    }

    /// Admits a (range, sender) under the cap, or queues it. `true` when it now has an
    /// assembly.
    ///
    /// A free slot goes to whichever (range, sender) asks for it — a waiter's next
    /// chunk, or an arrival's first — and the key it admits leaves the queue. Nothing
    /// here has a clock, so asking is the only evidence a stream is still there: the
    /// waiter at the head of the queue may be a leader that was replaced while it
    /// waited, and a slot held for that one would never be used again. The queue keeps
    /// the order the waiters asked in, which is what `ahead` reports and what
    /// [`finish`](Self::finish) hints at, but it is an order among streams that are
    /// still asking, not a reservation.
    fn admit(&mut self, key: (RangeId, ServerId), at: Identity) -> bool {
        if self.receiving.len() < self.cap {
            self.waiting.retain(|w| *w != key);
            self.receiving.insert(key, Assembly::default());
            return true;
        }
        // The one supersession this module can prove on its own: a chunk of *this*
        // range at a term above the one an assembly of this range holds is that
        // range's own Raft saying the assembly's sender no longer leads it, and a
        // stream from a superseded leader can never be installed — the receiver's raft
        // refuses an `InstallSnapshot` below its term. The stale assembly gives up its
        // slot rather than holding it until a `finish` that may never come.
        // A sender superseded on *another* range is not something a chunk proves, and
        // the node ends that assembly itself when it learns of the leader change
        // (RAFT.md:222-225).
        // PROPOSED(D-075): a full cap displaces an assembly of the same range at a
        // lower term, and nothing else.
        let stale = if self
            .variants
            .contains(NodeVariant::AssemblyHeldForDepartedSender)
        {
            None
        } else {
            self.superseded(key, at)
        };
        if let Some(stale) = stale {
            self.receiving.remove(&stale);
            self.waiting.retain(|w| *w != key);
            self.receiving.insert(key, Assembly::default());
            self.meters.displaced += 1;
            return true;
        }
        if !self.waiting.contains(&key) {
            self.waiting.push_back(key);
        }
        false
    }

    /// The assembly of `key`'s range whose identity a chunk at `at` supersedes: one
    /// held by another sender at a strictly lower term. `None` when there is no such
    /// assembly, which is every case where the cap is doing its job.
    fn superseded(&self, key: (RangeId, ServerId), at: Identity) -> Option<(RangeId, ServerId)> {
        self.receiving
            .iter()
            .find(|((range, from), assembly)| {
                *range == key.0
                    && *from != key.1
                    && assembly.at.is_some_and(|held| held.term < at.term)
            })
            .map(|(held, _)| *held)
    }
}

impl fmt::Debug for Snapshots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshots")
            .field("engine_dir", &self.engine_dir)
            .field("cap", &self.cap)
            .field("variants", &self.variants)
            .field("sending", &self.sending.len())
            .field("receiving", &self.receiving.len())
            .field("waiting", &self.waiting.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const R1: RangeId = RangeId(1);
    const R2: RangeId = RangeId(2);
    const S1: ServerId = ServerId(1);
    const S2: ServerId = ServerId(2);
    const S3: ServerId = ServerId(3);

    const AT: Identity = Identity {
        term: 4,
        last_index: 9,
        last_term: 3,
    };
    const LATER: Identity = Identity {
        term: 5,
        last_index: 12,
        last_term: 5,
    };

    /// The node these checks run on: not one of the peers it streams to or from, so a
    /// name built from the node's own id is never a follower's by accident.
    const ME: ServerId = ServerId(9);

    fn task(variants: NodeVariants) -> Snapshots {
        let mut task = Snapshots::new("/n1", ME, 4, variants);
        for range in [R1, R2] {
            task.host(range, spans(range));
        }
        task
    }

    /// A range's two intervals: its Raft state under tenant 0, its user keys under
    /// tenant 2 (D-066).
    fn spans(range: RangeId) -> Vec<KeyRange<Bytes>> {
        let raft = |g: u64| Bytes::copy_from_slice(&[0u64.to_be_bytes(), g.to_be_bytes()].concat());
        let user = |g: u64| Bytes::copy_from_slice(&[2u64.to_be_bytes(), g.to_be_bytes()].concat());
        let g = range.get();
        vec![raft(g)..raft(g + 1), user(g)..user(g + 1)]
    }

    fn correct() -> Snapshots {
        task(NodeVariants::correct())
    }

    fn buggy(variant: NodeVariant) -> Snapshots {
        task(NodeVariants::correct().with(variant))
    }

    /// The receiver's side of one whole stream.
    fn receive(task: &mut Snapshots, range: RangeId, from: ServerId, at: Identity) -> Landing {
        task.on_chunk(range, from, at, 1_024, false)
    }

    #[test]
    fn a_version_directorys_name_carries_its_range_and_parses_back() {
        assert_eq!(version_name(R1, 9, 2), "snap-r1-9-2");
        assert_eq!(parse_version("snap-r1-9-2"), Some((R1, 9, 2)));
        assert_eq!(staging_name(R2, S3), "staging-r2-s3");
        // The one-group layout belongs to no range, so no range's sweep proposes it.
        assert_eq!(parse_version("snap-9-2"), None);
        assert_eq!(parse_version("snap-9"), None);
        assert_eq!(parse_version("staging-r1-s2"), None);
    }

    #[test]
    fn two_ranges_takes_at_one_index_do_not_collide() {
        let correct = correct();
        assert_ne!(correct.version(R1, 9, 0), correct.version(R2, 9, 0));

        let buggy = buggy(NodeVariant::VersionDirWithoutRange);
        assert_eq!(
            buggy.version(R1, 9, 0),
            buggy.version(R2, 9, 0),
            "the variant is meant to name a version by index and take alone"
        );
    }

    #[test]
    fn a_ranges_sweep_leaves_every_other_ranges_versions() {
        let names: Vec<String> = [
            version_name(R1, 9, 0),  // R1's, unpinned: debris R1 sweeps
            version_name(R1, 11, 0), // R1's, pinned: kept
            version_name(R2, 9, 0),  // R2's, and R2's record is not R1's
            version_name(R2, 11, 0),
            "staging-r2-s1".to_owned(),
        ]
        .into_iter()
        .collect();
        let keep: BTreeSet<(Index, u64)> = [(11, 0)].into_iter().collect();

        let swept = correct().sweep(&names, R1, &keep);
        assert_eq!(swept, vec![version_name(R1, 9, 0)]);

        let swept = buggy(NodeVariant::SweepAcrossRanges).sweep(&names, R1, &keep);
        assert!(
            swept.contains(&version_name(R2, 11, 0)),
            "the variant is meant to sweep other ranges' versions: {swept:?}"
        );
    }

    #[test]
    fn two_ranges_assemble_into_directories_of_their_own() {
        let mut correct = correct();
        let one = receive(&mut correct, R1, S2, AT);
        let two = receive(&mut correct, R2, S2, AT);
        let (Landing::Staged { dir: first, .. }, Landing::Staged { dir: second, .. }) =
            (&one, &two)
        else {
            panic!("both chunks stage: {one:?}, {two:?}");
        };
        assert_ne!(first, second);
        assert_eq!(correct.meters().receiving, 2);
        assert_eq!(correct.meters().abandoned, 0);

        let mut buggy = buggy(NodeVariant::SharedStagingDir);
        let one = receive(&mut buggy, R1, S2, AT);
        let two = receive(&mut buggy, R2, S2, AT);
        let (Landing::Staged { dir: first, .. }, Landing::Staged { dir: second, .. }) =
            (&one, &two)
        else {
            panic!("both chunks stage: {one:?}, {two:?}");
        };
        assert_eq!(
            first, second,
            "the variant is meant to stage both ranges in one directory"
        );
    }

    #[test]
    fn a_chunk_of_another_stream_never_abandons_an_assembly() {
        // Two senders of one range and a third of another, interleaved: on the correct
        // node each keeps its own assembly and its own staged bytes.
        let mut correct = correct();
        for from in [S1, S2] {
            receive(&mut correct, R1, from, AT);
        }
        receive(&mut correct, R2, S1, AT);
        let again = correct.on_chunk(R1, S1, AT, 512, false);
        assert!(
            matches!(again, Landing::Staged { staged: 1_536, .. }),
            "S1's assembly kept its bytes: {again:?}"
        );
        assert_eq!(correct.meters().abandoned, 0);
        assert_eq!(correct.meters().receiving, 3);

        let mut buggy = buggy(NodeVariant::OneAssemblyPerNode);
        for from in [S1, S2] {
            receive(&mut buggy, R1, from, AT);
        }
        receive(&mut buggy, R2, S1, AT);
        assert_eq!(
            buggy.meters().receiving,
            1,
            "the variant is meant to keep one assembly for the whole node"
        );
        assert!(
            buggy.meters().abandoned > 0,
            "the variant is meant to abandon an assembly for a chunk that is not its own"
        );
    }

    #[test]
    fn a_senders_own_restart_starts_its_directory_over_and_no_others() {
        let mut task = correct();
        receive(&mut task, R1, S1, AT);
        receive(&mut task, R2, S1, AT);
        let landing = task.on_chunk(R1, S1, LATER, 64, false);
        let Landing::Restarted { dir, staged } = &landing else {
            panic!("a new identity restarts the assembly: {landing:?}");
        };
        assert_eq!(dir, &task.staging(R1, S1));
        assert_eq!(
            *staged, 0,
            "the directory starts over: the restarting chunk is not kept either, \
             because nothing here carries its offset"
        );
        // And the sender's next chunk is the new stream's first byte: the count the
        // receiver acknowledges starts from this chunk, not from the old stream's.
        let landing = task.on_chunk(R1, S1, LATER, 64, false);
        assert_eq!(
            landing,
            Landing::Staged {
                dir: task.staging(R1, S1),
                staged: 64
            },
            "the new stream's bytes are its own: {landing:?}"
        );
        // R2's assembly is untouched: its next chunk adds to what it had.
        let landing = task.on_chunk(R2, S1, AT, 8, false);
        assert!(
            matches!(landing, Landing::Staged { staged: 1_032, .. }),
            "R2 kept its bytes: {landing:?}"
        );
        assert_eq!(task.meters().abandoned, 0);
        assert_eq!(task.meters().restarts, 1);
    }

    #[test]
    fn a_restarted_stream_is_never_installed() {
        // The ordinary small range: a new leader's whole snapshot is one chunk, so the
        // chunk that restarts the assembly is also the stream's last. The directory has
        // just been started over and holds the abandoned stream's files until the node
        // is told so; completing here would install one snapshot's files labelled as
        // another's (RAFT.md:203-207).
        let mut task = correct();
        receive(&mut task, R1, S2, AT);
        let landing = task.on_chunk(R1, S2, LATER, 16, true);
        assert_eq!(
            landing,
            Landing::Restarted {
                dir: task.staging(R1, S2),
                staged: 0
            },
            "a restart is answered with a restart, whatever the chunk's `done` says"
        );
        assert_eq!(task.meters().installs, 0);
        // The sender restarts from its first byte, and that stream completes.
        let landing = task.on_chunk(R1, S2, LATER, 16, true);
        let Landing::Complete(install) = landing else {
            panic!("the restarted stream completes on its own bytes: {landing:?}");
        };
        assert_eq!(install.at, LATER);
        assert_eq!(task.meters().installs, 1);

        let mut buggy = buggy(NodeVariant::CompleteOnRestart);
        receive(&mut buggy, R1, S2, AT);
        let landing = buggy.on_chunk(R1, S2, LATER, 16, true);
        let Landing::Complete(install) = landing else {
            panic!("the variant is meant to install the chunk that restarted it: {landing:?}");
        };
        assert_eq!(
            install.at, LATER,
            "the variant installs {LATER}'s label over {AT}'s staged files"
        );
    }

    #[test]
    fn the_receive_cap_holds_and_a_freed_slot_admits_the_first_waiter() {
        // The re-seed shape's: four ranges, two slots (SHARD.md §12).
        let mut task = Snapshots::new("/n1", ME, 2, NodeVariants::correct());
        for range in [RangeId(1), RangeId(2), RangeId(3), RangeId(4)] {
            task.host(range, spans(range));
        }
        for range in [RangeId(1), RangeId(2)] {
            assert!(matches!(
                task.on_chunk(range, S1, AT, 16, false),
                Landing::Staged { .. }
            ));
        }
        for (range, ahead) in [(RangeId(3), 0), (RangeId(4), 1)] {
            let landing = task.on_chunk(range, S1, AT, 16, false);
            assert_eq!(
                landing,
                Landing::Waiting { ahead },
                "{range} is over the cap and waits"
            );
        }
        assert_eq!(task.meters().receiving, 2);
        assert_eq!(task.meters().waiting, 2);
        // A refused sender that asks again is one waiter, not two: the queue holds a
        // (range, sender) once, and its place in it does not move.
        assert_eq!(
            task.on_chunk(RangeId(3), S1, AT, 16, false),
            Landing::Waiting { ahead: 0 }
        );
        assert_eq!(task.meters().waiting, 2, "a resend queues nothing new");
        // Nothing the waiters sent disturbed the two that were admitted.
        assert!(matches!(
            task.on_chunk(RangeId(1), S1, AT, 16, false),
            Landing::Staged { staged: 32, .. }
        ));
        // A slot frees. It is not reserved: the head of the queue is named as a hint,
        // and takes the slot when its own next chunk arrives.
        assert_eq!(task.finish(RangeId(1), S1), Some((RangeId(3), S1)));
        assert_eq!(
            task.meters().receiving,
            1,
            "the freed slot is free, not reserved"
        );
        assert!(matches!(
            task.on_chunk(RangeId(3), S1, AT, 16, false),
            Landing::Staged { .. }
        ));
        assert_eq!(task.meters().waiting, 1);
        assert_eq!(task.finish(RangeId(2), S1), Some((RangeId(4), S1)));
        assert!(matches!(
            task.on_chunk(RangeId(4), S1, AT, 16, false),
            Landing::Staged { .. }
        ));
        assert_eq!(task.meters().waiting, 0);
        assert_eq!(task.meters().receiving, 2);
    }

    #[test]
    fn a_freed_slot_goes_to_a_stream_still_asking_for_it() {
        // §12's shape again — four ranges, two slots — with the thing this task exists
        // to survive: a leader changes while its range waits. The waiter that asked
        // first is S1's, and S1 is gone; nothing here has a clock to notice. The slot
        // must go to the new leader's stream that is asking for it now, or the node
        // re-seeds nothing more (SHARD.md:571-578).
        let mut task = Snapshots::new("/n1", ME, 2, NodeVariants::correct());
        for range in [RangeId(1), RangeId(2), RangeId(3), RangeId(4)] {
            task.host(range, spans(range));
        }
        for range in [RangeId(1), RangeId(2)] {
            task.on_chunk(range, S1, AT, 16, false);
        }
        for range in [RangeId(3), RangeId(4)] {
            task.on_chunk(range, S1, AT, 16, false);
        }
        assert_eq!(task.meters().waiting, 2);
        // S1 is replaced as the leader of r1 and r2. The node ends the assemblies it
        // held: that is the node's rule for a leader change it observes
        // (RAFT.md:222-225), and not something this check may assume — what the module
        // can prove for itself, a higher term on the *same* range, is
        // `an_assembly_whose_leader_its_range_superseded_gives_up_its_slot`. Its two
        // waiters never send again.
        task.finish(RangeId(1), S1);
        task.finish(RangeId(2), S1);
        assert_eq!(task.meters().receiving, 0);
        let landing = task.on_chunk(RangeId(3), S2, AT, 16, false);
        assert!(
            matches!(landing, Landing::Staged { .. }),
            "R3's new leader takes a free slot: {landing:?}"
        );
        let landing = task.on_chunk(RangeId(4), S2, AT, 16, false);
        assert!(
            matches!(landing, Landing::Staged { .. }),
            "and R4's: {landing:?}"
        );
        assert_eq!(task.meters().receiving, 2);

        // The variant: the freed slot is reserved for the head of the queue, which is
        // a stream of the departed leader's. Both slots are held by senders that will
        // never send again and the node wedges.
        let mut buggy = Snapshots::new(
            "/n1",
            ME,
            2,
            NodeVariants::correct().with(NodeVariant::SlotReservedForWaiter),
        );
        for range in [RangeId(1), RangeId(2), RangeId(3), RangeId(4)] {
            buggy.host(range, spans(range));
        }
        for range in [RangeId(1), RangeId(2), RangeId(3), RangeId(4)] {
            buggy.on_chunk(range, S1, AT, 16, false);
        }
        buggy.finish(RangeId(1), S1);
        buggy.finish(RangeId(2), S1);
        assert_eq!(
            buggy.meters().receiving,
            2,
            "the variant is meant to reserve the freed slots for the waiters"
        );
        assert!(
            matches!(
                buggy.on_chunk(RangeId(3), S2, AT, 16, false),
                Landing::Waiting { .. }
            ),
            "the variant is meant to refuse the new leader for ever"
        );
    }

    #[test]
    fn a_leader_feeds_every_designated_follower_at_once() {
        let mut correct = correct();
        for to in [S1, S2, S3] {
            assert!(matches!(
                correct.stream(R1, to, AT),
                Started::Streaming { .. }
            ));
        }
        // And another range's followers beside them: no per-node cap on sends (Q14).
        assert!(matches!(
            correct.stream(R2, S1, AT),
            Started::Streaming { .. }
        ));
        assert_eq!(correct.meters().sending, 4);
        assert!(correct.is_streaming(R1, S3, AT));
        // And it is only running where one is: not at another identity, not to a
        // follower nothing was started to, and not after the stream ends.
        assert!(!correct.is_streaming(R1, S3, LATER), "another identity");
        assert!(!correct.is_streaming(R2, S3, AT), "no stream to R2's S3");
        correct.sent(R1, S3);
        assert!(!correct.is_streaming(R1, S3, AT), "the stream ended");
        assert_eq!(correct.meters().sending, 3);

        let mut buggy = buggy(NodeVariant::CapStreamsSent);
        assert!(matches!(
            buggy.stream(R1, S1, AT),
            Started::Streaming { .. }
        ));
        assert_eq!(
            buggy.stream(R1, S2, AT),
            Started::Waiting,
            "the variant is meant to cap streams sent"
        );
    }

    #[test]
    fn a_chunk_travels_in_a_frame_of_its_own() {
        let mut correct = correct();
        correct.stream(R1, S2, AT);
        // Three sizes, so "one frame per chunk" is read off the frames and not off a
        // single size: a byte, a chunk of the size §4 streams in, and the largest
        // chunk that fits a frame at all.
        let sizes = [1, 256 * 1024, MAX_FRAME_LEN - encoded_len(0)];
        for (n, size) in sizes.iter().enumerate() {
            let chunk = vec![7u8; *size];
            let route = correct
                .route(R1, S2, &chunk)
                .expect("a chunk that fits a frame");
            let Route::OwnFrame(frame) = route else {
                panic!("a chunk goes in a frame of its own: {route:?}");
            };
            // The framing alone: the chunk's bytes are a snapshot file's, not a message
            // the codec reads, so `slices` is the right reader here.
            let carried = crate::frame::slices(&frame).expect("a frame");
            assert_eq!(carried.len(), 1, "one message, which is the chunk");
            assert_eq!(carried[0].0, R1);
            assert_eq!(&carried[0].1[..], &chunk[..]);
            let meters = correct.meters();
            assert_eq!((meters.chunks, meters.frames), (n + 1, n + 1));
        }

        let mut buggy = buggy(NodeVariant::ChunksInBatchFrames);
        buggy.stream(R1, S2, AT);
        let chunk = vec![7u8; 256 * 1024];
        assert_eq!(
            buggy.route(R1, S2, &chunk).expect("under the cap"),
            Route::Outbox,
            "the variant is meant to put chunks through the per-peer outbox"
        );
        assert_eq!(buggy.meters().frames, 0);
    }

    #[test]
    fn a_chunk_too_large_for_a_frame_is_refused_and_not_split() {
        let mut task = correct();
        task.stream(R1, S2, AT);
        let chunk = vec![7u8; MAX_FRAME_LEN];
        let refused = task.route(R1, S2, &chunk).expect_err("over a frame");
        assert_eq!(refused.range, R1);
        assert_eq!(refused.to, S2);
        assert_eq!(refused.len, MAX_FRAME_LEN);
        assert_eq!(task.meters().frames, 0);
        assert_eq!(task.meters().chunks, 0, "a refused chunk is not counted");
    }

    #[test]
    #[should_panic(expected = "carries no bytes")]
    fn a_chunk_of_no_bytes_is_not_routed_and_is_not_an_oversized_one() {
        // `Builder::push` refuses an empty message (frame.rs:134-137); reporting it as
        // an oversized chunk of length 0 would name the wrong thing entirely.
        let mut task = correct();
        task.stream(R1, S2, AT);
        let _ = task.route(R1, S2, &[]);
    }

    #[test]
    #[should_panic(expected = "which no stream is running to")]
    fn a_chunk_is_not_routed_to_a_follower_no_stream_is_running_to() {
        // The two halves of the send path are one path: a chunk is a chunk *of* a
        // stream, and a frame built for a follower nothing is being streamed to is the
        // two halves having come apart.
        let mut task = correct();
        let _ = task.route(R1, S2, &[7u8; 64]);
    }

    #[test]
    fn the_name_a_leader_records_is_the_name_its_followers_stage_under() {
        // The send half records the directory the *receiver* assembles into, and the
        // receiver keys it by the sender — this node. A leader streaming one range to
        // three followers records one name, and each follower stages under it.
        let mut leader = correct();
        let mut recorded = Vec::new();
        for to in [S1, S2, S3] {
            let Started::Streaming { staging } = leader.stream(R1, to, AT) else {
                panic!("no cap on sends");
            };
            recorded.push(staging);
        }
        assert_eq!(recorded, vec![staging_name(R1, ME); 3]);
        assert_eq!(leader.staging_sent(R1), recorded[0]);

        // The follower's side of the same stream: a node receiving from ME.
        let mut follower = Snapshots::new("/n2", S2, 4, NodeVariants::correct());
        follower.host(R1, spans(R1));
        let Landing::Staged { dir, .. } = follower.on_chunk(R1, ME, AT, 16, false) else {
            panic!("the chunk stages");
        };
        assert_eq!(
            dir,
            Path::new("/n2").join(&recorded[1]),
            "the name the leader recorded is the directory the follower staged in"
        );
    }

    #[test]
    fn an_install_is_a_live_install_of_the_ranges_spans_with_its_repair() {
        let mut task = correct();
        receive(&mut task, R1, S2, AT);
        let landing = task.on_chunk(R1, S2, AT, 16, true);
        let Landing::Complete(install) = landing else {
            panic!("the last chunk completes the stream: {landing:?}");
        };
        assert_eq!(install.range, R1);
        assert_eq!(install.from, S2);
        assert_eq!(
            install.at, AT,
            "the install names the snapshot that was installed"
        );
        assert_eq!(install.source, task.staging(R1, S2));
        assert_eq!(
            install.spans,
            spans(R1),
            "the range's two intervals (D-066)"
        );
        assert!(install.spans.windows(2).all(|w| w[0].end <= w[1].start));
        assert!(
            install.repair_in_switch,
            "the repair is carried in the switch"
        );
        assert!(!install.reopens_engine, "a live install reopens no engine");
        assert!(!install.adopted, "an install is never a RaftAdopted");
        assert_eq!(task.meters().adoptions, 0);

        // A *second* range's install carries that range's spans. `hosted` is a
        // `BTreeMap`, so "the first hosted range" is R1 here: an install of R2 that
        // carried R1's spans would have `Engine::install_spans` remove every key of R1
        // — its Raft state under tenant 0 and its user keys under tenant 2 — and put
        // R2's staged tables there, in one switch, while R1 is running (D-068).
        receive(&mut task, R2, S2, AT);
        let landing = task.on_chunk(R2, S2, AT, 16, true);
        let Landing::Complete(second) = landing else {
            panic!("R2's stream completes: {landing:?}");
        };
        assert_eq!(second.range, R2);
        assert_eq!(second.spans, spans(R2), "the completed range's spans");
        assert_ne!(
            second.spans,
            spans(R1),
            "not the node's first hosted range's"
        );
        assert_eq!(second.source, task.staging(R2, S2));

        let mut wrong = buggy(NodeVariant::InstallWrongRangesSpans);
        receive(&mut wrong, R2, S2, AT);
        let Landing::Complete(install) = wrong.on_chunk(R2, S2, AT, 16, true) else {
            panic!("the stream completes");
        };
        assert_eq!(
            install.range, R2,
            "the variant names the range that completed"
        );
        assert_eq!(
            install.spans,
            spans(R1),
            "the variant is meant to carry the first hosted range's spans"
        );

        let mut buggy = buggy(NodeVariant::InstallWithoutRepair);
        receive(&mut buggy, R1, S2, AT);
        let Landing::Complete(install) = buggy.on_chunk(R1, S2, AT, 16, true) else {
            panic!("the stream completes");
        };
        assert!(
            !install.repair_in_switch,
            "the variant is meant to switch without the repair"
        );
    }

    #[test]
    fn only_a_fresh_directory_after_a_refusal_is_adopted() {
        let mut task = correct();
        // Every range's install on the node traces no adoption.
        for range in [R1, R2] {
            receive(&mut task, range, S2, AT);
            let Landing::Complete(install) = task.on_chunk(range, S2, AT, 16, true) else {
                panic!("the stream completes");
            };
            assert!(!install.adopted);
            task.finish(range, S2);
        }
        assert_eq!(task.meters().adoptions, 0);
        assert_eq!(
            task.meters().installs,
            2,
            "two ranges installed, not adopted"
        );
        // An assembly is open and a stream is being sent: both name directories under
        // the refused store, and the adoption drops them.
        receive(&mut task, R1, S3, AT);
        task.stream(R2, S1, AT);
        // The one thing that traces an adoption: the node taking a fresh directory
        // (Q15). Nothing has installed into *that* directory, which is what the figure
        // says and what the assertion checks.
        let adopted = task.adopt_fresh("/n1-fresh");
        assert_eq!(adopted.dir, Path::new("/n1-fresh"));
        assert_eq!(adopted.ranges_installed, 0);
        assert_eq!(task.meters().adoptions, 1);
        assert_eq!(task.staging(R1, S2), Path::new("/n1-fresh/staging-r1-s2"));
        let meters = task.meters();
        assert_eq!(
            (meters.receiving, meters.waiting, meters.sending),
            (0, 0, 0),
            "what the refused store was assembling and sending goes with it"
        );

        let mut buggy = buggy(NodeVariant::AdoptedOnRangeInstall);
        receive(&mut buggy, R1, S2, AT);
        let Landing::Complete(install) = buggy.on_chunk(R1, S2, AT, 16, true) else {
            panic!("the stream completes");
        };
        assert!(
            install.adopted,
            "the variant is meant to adopt on a replica's install"
        );
    }

    #[test]
    #[should_panic(expected = "a fresh directory is adopted before any range installs")]
    fn a_directory_a_range_installed_into_is_not_adopted_as_fresh() {
        // The fresh directory's whole point is that the event describes it before any
        // range is in it. A node that adopted a directory it had already installed
        // into — its own refused store, under a name that collided — would trace a
        // `RaftAdopted` for a store that is not fresh, and the figure it carries would
        // be false.
        let mut task = correct();
        receive(&mut task, R1, S2, AT);
        let Landing::Complete(_) = task.on_chunk(R1, S2, AT, 16, true) else {
            panic!("the stream completes");
        };
        task.finish(R1, S2);
        task.adopt_fresh("/n1");
    }

    #[test]
    fn two_senders_of_one_range_assemble_into_directories_of_their_own() {
        // The decision this slice argues at most length (D-075, proposed 1): one range,
        // two senders — a leader and the stale leader it replaced — each hold an
        // assembly, so the staging name carries the sender as well as the range. Keyed
        // by range alone the two write over each other's files, which is what
        // §11 raft 14 asks not to happen, one level down from two ranges doing it.
        let mut correct = correct();
        let one = receive(&mut correct, R1, S1, AT);
        let two = receive(&mut correct, R1, S2, LATER);
        let (Landing::Staged { dir: first, .. }, Landing::Staged { dir: second, .. }) =
            (&one, &two)
        else {
            panic!("both chunks stage: {one:?}, {two:?}");
        };
        assert_ne!(first, second, "one directory each");
        assert_eq!(first, &Path::new("/n1").join(staging_name(R1, S1)));
        assert_eq!(second, &Path::new("/n1").join(staging_name(R1, S2)));
        // And the bytes are each stream's own, not one count over both.
        let landing = correct.on_chunk(R1, S1, AT, 8, false);
        assert!(
            matches!(landing, Landing::Staged { staged: 1_032, .. }),
            "S1's stream counts its own bytes: {landing:?}"
        );
        assert_eq!(correct.meters().receiving, 2);
        assert_eq!(correct.meters().abandoned, 0);

        let mut buggy = buggy(NodeVariant::StagingByRangeAlone);
        let one = receive(&mut buggy, R1, S1, AT);
        let two = receive(&mut buggy, R1, S2, LATER);
        let (Landing::Staged { dir: first, .. }, Landing::Staged { dir: second, .. }) =
            (&one, &two)
        else {
            panic!("both chunks stage: {one:?}, {two:?}");
        };
        assert_eq!(
            first, second,
            "the variant is meant to stage both senders of a range in one directory"
        );
    }

    #[test]
    fn a_chunk_of_a_range_the_node_does_not_host_is_refused_and_takes_no_slot() {
        // `range` and `from` come off a peer's `InstallSnapshot`. A leader that has not
        // learned the rebalancer moved the range off this node (Q33) streams to the old
        // replica, and a garbled range id says the same thing: the node refuses the
        // chunk, as it refuses one over the cap. It does not fail on a peer's word
        // (frame.rs:134-137), and it does not let that word hold a slot.
        let mut task = Snapshots::new("/n1", ME, 2, NodeVariants::correct());
        task.host(R1, spans(R1));
        let unhosted = RangeId(7);
        assert_eq!(
            task.on_chunk(unhosted, S2, AT, 16, false),
            Landing::NotHosted
        );
        // Its last chunk is refused the same way: the node cannot say what spans the
        // switch would take, so there is nothing to install.
        assert_eq!(
            task.on_chunk(unhosted, S2, AT, 16, true),
            Landing::NotHosted
        );
        let meters = task.meters();
        assert_eq!(
            (meters.receiving, meters.waiting, meters.installs),
            (0, 0, 0),
            "nothing admitted, nothing queued, nothing installed"
        );
        assert_eq!(meters.unhosted, 2);
        // The range the node does host is admitted, both slots still free to it.
        assert!(matches!(
            task.on_chunk(R1, S2, AT, 16, false),
            Landing::Staged { .. }
        ));

        // The variant: the unhosted ranges are taken in, and with the cap at two they
        // starve the one range this node actually hosts.
        let mut buggy = Snapshots::new(
            "/n1",
            ME,
            2,
            NodeVariants::correct().with(NodeVariant::AdmitsAnUnhostedRange),
        );
        buggy.host(R1, spans(R1));
        for range in [RangeId(7), RangeId(8)] {
            assert!(
                matches!(
                    buggy.on_chunk(range, S2, AT, 16, false),
                    Landing::Staged { .. }
                ),
                "the variant is meant to admit a range the node does not host"
            );
        }
        assert_eq!(
            buggy.on_chunk(R1, S2, AT, 16, false),
            Landing::Waiting { ahead: 0 },
            "the variant is meant to starve the node's own range"
        );
    }

    #[test]
    #[should_panic(expected = "but is not hosted here")]
    fn a_range_the_task_does_not_host_is_never_streamed() {
        // The send half. The range a leader streams is the node's own claim, not a
        // peer's: a node streaming a range it cannot say the spans of has lost track of
        // it, and fails where it lost track.
        let mut task = correct();
        task.stream(RangeId(7), S2, AT);
    }

    #[test]
    #[should_panic(expected = "completed a stream from")]
    fn an_admitted_unhosted_range_still_installs_nothing() {
        // The invariant behind the refusal: even with the refusal removed, an install
        // is never made for a range whose spans the node cannot name. This is the
        // variant's own end, and the reason the assertion stays.
        let mut buggy = Snapshots::new(
            "/n1",
            ME,
            2,
            NodeVariants::correct().with(NodeVariant::AdmitsAnUnhostedRange),
        );
        buggy.host(R1, spans(R1));
        buggy.on_chunk(RangeId(7), S2, AT, 16, true);
    }

    #[test]
    fn an_assembly_whose_leader_its_range_superseded_gives_up_its_slot() {
        // §12's shape again: four ranges, two slots, S1 holding both assemblies. S1 is
        // replaced as r1's leader, and r1's new leader's chunk names a higher term —
        // r1's own Raft saying S1 no longer leads it, which is the one supersession a
        // chunk proves. The stale assembly can never be installed by anyone, so it
        // gives up its slot rather than holding it until a `finish` that never comes.
        let mut task = Snapshots::new("/n1", ME, 2, NodeVariants::correct());
        for range in [RangeId(1), RangeId(2), RangeId(3), RangeId(4)] {
            task.host(range, spans(range));
        }
        for range in [RangeId(1), RangeId(2)] {
            task.on_chunk(range, S1, AT, 16, false);
        }
        assert_eq!(task.meters().receiving, 2);
        let landing = task.on_chunk(RangeId(1), S2, LATER, 16, false);
        assert!(
            matches!(landing, Landing::Staged { staged: 16, .. }),
            "r1's new leader takes the slot its superseded leader held: {landing:?}"
        );
        let meters = task.meters();
        assert_eq!(
            (meters.receiving, meters.waiting, meters.displaced),
            (2, 0, 1),
            "one assembly displaced, none queued, the cap still two"
        );
        // Only that range's. A chunk of r1 is no evidence about who leads r2, whose
        // Raft is its own: r2's assembly keeps its slot and its bytes, and the node
        // ends it itself when it learns r2's leader changed (RAFT.md:222-225).
        assert!(matches!(
            task.on_chunk(RangeId(2), S1, AT, 16, false),
            Landing::Staged { staged: 32, .. }
        ));
        // The superseded sender's own next chunk asks for a slot like anyone else's:
        // it waits, and does not displace its way back in.
        assert_eq!(
            task.on_chunk(RangeId(1), S1, AT, 16, false),
            Landing::Waiting { ahead: 0 }
        );
        // A range with no assembly of its own displaces nothing, whatever its term.
        assert_eq!(
            task.on_chunk(RangeId(3), S2, LATER, 16, false),
            Landing::Waiting { ahead: 1 }
        );
        assert_eq!(task.meters().displaced, 1);

        // The variant: the stale assembly is held until it finishes, which a leader
        // that has been replaced never does, and r1's new leader waits for ever.
        let mut buggy = Snapshots::new(
            "/n1",
            ME,
            2,
            NodeVariants::correct().with(NodeVariant::AssemblyHeldForDepartedSender),
        );
        for range in [RangeId(1), RangeId(2), RangeId(3), RangeId(4)] {
            buggy.host(range, spans(range));
        }
        for range in [RangeId(1), RangeId(2)] {
            buggy.on_chunk(range, S1, AT, 16, false);
        }
        assert!(
            matches!(
                buggy.on_chunk(RangeId(1), S2, LATER, 16, false),
                Landing::Waiting { .. }
            ),
            "the variant is meant to hold the superseded assembly's slot"
        );
        assert_eq!(buggy.meters().displaced, 0);
    }

    #[test]
    fn a_resent_last_chunk_installs_once() {
        // A chunk unanswered for half a minimum election timeout is resent
        // (RAFT.md:200-202), and a last chunk's answer is exactly what can be lost. The
        // resend is answered installed: no second switch, from a staging directory the
        // first switch may already have consumed.
        let mut task = correct();
        receive(&mut task, R1, S2, AT);
        let Landing::Complete(install) = task.on_chunk(R1, S2, AT, 16, true) else {
            panic!("the stream completes");
        };
        assert_eq!(install.at, AT);
        for _ in 0..2 {
            assert_eq!(
                task.on_chunk(R1, S2, AT, 16, true),
                Landing::Installed { at: AT },
                "the resend is answered what the first answer said"
            );
        }
        // And a chunk of the installed stream that is not its last one stages nothing
        // either: that stream is over.
        assert_eq!(
            task.on_chunk(R1, S2, AT, 16, false),
            Landing::Installed { at: AT }
        );
        let meters = task.meters();
        assert_eq!((meters.installs, meters.duplicates), (1, 3));
        // The sender's *next* snapshot is a stream of its own: a new identity starts
        // the directory over and installs when it completes.
        let landing = task.on_chunk(R1, S2, LATER, 16, false);
        assert!(
            matches!(landing, Landing::Restarted { staged: 0, .. }),
            "a new identity starts over: {landing:?}"
        );
        let Landing::Complete(next) = task.on_chunk(R1, S2, LATER, 16, true) else {
            panic!("the new stream completes");
        };
        assert_eq!(next.at, LATER);
        assert_eq!(task.meters().installs, 2);
        // The memory is the assembly's, so it ends with `finish`: which is why the node
        // finishes a completed assembly only once it has answered the sender.
        task.finish(R1, S2);
        assert!(matches!(
            task.on_chunk(R1, S2, LATER, 16, true),
            Landing::Complete(_)
        ));
        assert_eq!(task.meters().installs, 3);

        // The variant: the resend installs again.
        let mut buggy = buggy(NodeVariant::InstallsADuplicateLastChunk);
        receive(&mut buggy, R1, S2, AT);
        let _ = buggy.on_chunk(R1, S2, AT, 16, true);
        assert!(
            matches!(buggy.on_chunk(R1, S2, AT, 16, true), Landing::Complete(_)),
            "the variant is meant to install the resend a second time"
        );
        assert_eq!(buggy.meters().installs, 2);
        assert_eq!(buggy.meters().duplicates, 0);
    }

    #[test]
    #[should_panic(expected = "spans are not sorted and disjoint")]
    fn overlapping_spans_are_refused_where_the_range_is_hosted() {
        let raft = |g: u64| Bytes::copy_from_slice(&[0u64.to_be_bytes(), g.to_be_bytes()].concat());
        let mut task = correct();
        task.host(RangeId(7), vec![raft(0)..raft(4), raft(2)..raft(6)]);
    }

    #[test]
    #[should_panic(expected = "is hosted with no spans")]
    fn a_range_hosted_with_no_spans_is_refused_where_it_is_hosted() {
        // `EmptySpan` is "the set is empty or any span in it holds no key"
        // (engine.rs:1799; D-068). A range registered before its intervals are known
        // would install nothing and be refused at every switch for ever — the
        // permanent refusal, not a transient one.
        let mut task = correct();
        task.host(RangeId(7), Vec::new());
    }

    #[test]
    #[should_panic(expected = "has an empty span")]
    fn an_empty_span_is_refused_where_the_range_is_hosted() {
        let raft = |g: u64| Bytes::copy_from_slice(&[0u64.to_be_bytes(), g.to_be_bytes()].concat());
        let mut task = correct();
        task.host(RangeId(7), vec![raft(4)..raft(4)]);
    }

    #[test]
    fn every_snapshot_variant_is_caught_by_a_check_here() {
        // The pair rule's bookkeeping: each variant this slice adds is named by the
        // check above that fails under it. A variant with no check would be a bug
        // built and never caught.
        let caught: &[(NodeVariant, &str)] = &[
            (
                NodeVariant::SharedStagingDir,
                "two_ranges_assemble_into_directories_of_their_own",
            ),
            (
                NodeVariant::OneAssemblyPerNode,
                "a_chunk_of_another_stream_never_abandons_an_assembly",
            ),
            (
                NodeVariant::VersionDirWithoutRange,
                "two_ranges_takes_at_one_index_do_not_collide",
            ),
            (
                NodeVariant::SweepAcrossRanges,
                "a_ranges_sweep_leaves_every_other_ranges_versions",
            ),
            (
                NodeVariant::CapStreamsSent,
                "a_leader_feeds_every_designated_follower_at_once",
            ),
            (
                NodeVariant::ChunksInBatchFrames,
                "a_chunk_travels_in_a_frame_of_its_own",
            ),
            (
                NodeVariant::InstallWithoutRepair,
                "an_install_is_a_live_install_of_the_ranges_spans_with_its_repair",
            ),
            (
                NodeVariant::AdoptedOnRangeInstall,
                "only_a_fresh_directory_after_a_refusal_is_adopted",
            ),
            (
                NodeVariant::StagingByRangeAlone,
                "two_senders_of_one_range_assemble_into_directories_of_their_own",
            ),
            (
                NodeVariant::SlotReservedForWaiter,
                "a_freed_slot_goes_to_a_stream_still_asking_for_it",
            ),
            (
                NodeVariant::CompleteOnRestart,
                "a_restarted_stream_is_never_installed",
            ),
            (
                NodeVariant::InstallWrongRangesSpans,
                "an_install_is_a_live_install_of_the_ranges_spans_with_its_repair",
            ),
            (
                NodeVariant::AdmitsAnUnhostedRange,
                "a_chunk_of_a_range_the_node_does_not_host_is_refused_and_takes_no_slot",
            ),
            (
                NodeVariant::AssemblyHeldForDepartedSender,
                "an_assembly_whose_leader_its_range_superseded_gives_up_its_slot",
            ),
            (
                NodeVariant::InstallsADuplicateLastChunk,
                "a_resent_last_chunk_installs_once",
            ),
        ];
        for (variant, _) in caught {
            assert!(
                NodeVariant::SNAPSHOT.contains(variant),
                "{variant} is not in the snapshot task's set"
            );
        }
        assert_eq!(caught.len(), NodeVariant::SNAPSHOT.len());
    }
}

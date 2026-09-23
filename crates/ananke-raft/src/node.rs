//! One server under the [`Environment`] (RAFT.md §3): the tasks that run a core.
//!
//! [`run`] binds the socket, spawns the `net` task, and then runs the server as a
//! sequence of *incarnations*, one per store the server runs on: at each start
//! [`start_store`] reads the store directory's format before anything writes
//! (D-059), records it if the directory is fresh, adopts a completed install at the
//! staging path ([`crate::snapshot::adopt_staged`]), heals a record with one damaged
//! copy, reads the store marker and then opens the engine and the store; the `raft`
//! loop runs with the `apply` and `snapshot` tasks beside it until the server stops
//! or an installed snapshot switches its store, which starts the next incarnation on
//! the adopted state. The socket and the inbox live across incarnations, so messages
//! arriving during a switch are queued, not lost. A store in another Raft store
//! format is refused with nothing written to it: the server traces
//! [`TraceEvent::RaftServerFailed`] and stops, since that store lost nothing and is
//! not this server's to replace (D-059).
//!
//! - `raft` owns the core and the timer: one loop over a race of the inbox and the
//!   tick, stepping the core once per event and executing every output in order,
//!   awaiting a [`Output::Persist`] before the sends that follow it. That order is
//!   the persistence discipline of Figure 2; [`Variant::SendBeforePersist`] is the
//!   server that breaks it.
//! - `net` receives frames, decodes, and puts messages on the inbox, which is bounded:
//!   a full inbox drops the oldest heartbeat first, then the oldest message of the
//!   same kind from the same sender as the one arriving, which a newer one
//!   supersedes, and never an AppendEntries with entries or an InstallSnapshot
//!   chunk, which are admitted over the bound; each drop is
//!   [`TraceEvent::RaftInboxDropped`]. Client requests take the same socket and the
//!   same inbox. The two tasks are separate so that a message arriving while the
//!   core awaits a persist is a queued message, not a lost one.
//! - `apply` takes committed entries from the `raft` task, applies each as one synced
//!   batch with the applied index, answers the client that proposed it, and reports
//!   the applied index back. [`Variant::ApplyBeforeCommit`] hands it entries as they
//!   are appended instead. It also takes snapshots (RAFT.md §1): a take runs between
//!   applies, so the recorded index is exactly what the checkpoint captures
//!   (D-036).
//! - `snapshot` streams snapshots to followers on the leader and assembles arriving
//!   ones into the staging directory on a follower; it is the only task that touches
//!   checkpoint directories. The final repair of a staged store needs the receiver's
//!   own hard state and log tail, so the `raft` loop quiesces the `apply` task,
//!   reads them, and hands the repair over; the completed install then retires this
//!   incarnation. It keeps one stream per designated follower and services them
//!   all, each pinned to the checkpoint version it opened, and sweeps the versions
//!   no stream reads (D-043); [`Variant::SharedSnapshotDir`] is the
//!   server as built, one mutable directory per index and one stream at a time.
//!
//! Two gates of the `raft` loop keep a take from being repeated for nothing
//! (D-043): a stream that fails for want of a usable checkpoint while a
//! take is already in flight does not ask for another, and a take the core asks
//! for after a checkpoint was found unusable is a `Job::Retake`, a fresh
//! version even at the record's index, where a plain `Job::Take` at that index
//! reuses the recorded version.
//!
//! A server whose store is refused ([`LostState`]) traces
//! [`TraceEvent::RaftRefused`] and runs in *re-seed mode* (RAFT.md §3): its socket
//! stays bound but it participates in nothing — it grants no vote and no pre-vote
//! and answers no AppendEntries with content. It answers every AppendEntries with a
//! rejection whose hint asks from index 1, the ask no follower with a log makes, so
//! the leader designates it snapshot-fed and streams; once the install completes
//! the server runs on the re-seeded store, quarantined for good
//! (D-035): the lost state may have included its vote. Every
//! AppendEntries and InstallSnapshot response carries the store's incarnation
//! number, stamped on the way out like the clock — 0 while refused, a fresh one
//! on the re-seeded store — so a leader that matched entries on the lost store
//! forgets them rather than probing above a log that no longer has them
//! (D-042). The refusal itself is durable (D-044): before
//! the trace and before the re-seed, the store directory's marker is made to say
//! that this store lost state, and the engine that recovered the hole is
//! quiesced, so nothing it does after — no table, no manifest, no deleted log
//! segment — can make the store look whole to the next start.
//!
//! A client request becomes a proposal. A server that is not the leader answers
//! [`Reply::NotLeader`] at once. The leader remembers the request against the index
//! its entry took, and the `apply` task answers when that index applies with an entry
//! of the same term; an entry replaced by a later leader's is never answered, since
//! its fate is not known here.
//!
//! An operator's [`Command::Transfer`] and [`Command::Change`] are triggers, never
//! entries: the server hands them to the core directly. A transfer is answered
//! Done at once (D-028); a change is answered Done when the leader accepts it,
//! since its completion is configuration entries the trace shows, and NotLeader
//! when it refuses — naming this server itself when the refusal is a different
//! change already in flight (RAFT.md §1, D-029).
//!
//! The network delivers at least once: a request it duplicates arrives twice, and a
//! leader that proposed both copies would apply the command twice, which for a
//! compare-and-set is a second, failing swap the client may be told about instead of
//! the first. The sweep found exactly that on its first seed (D-026). A server keeps
//! the index and term of every request it proposed while the entry is in its log,
//! and a request it has already proposed is not proposed again.
//!
//! Every event is traced once what it reports is durable, so a record's time is its
//! durability time; the moment the step behind it was taken travels beside it as a
//! decision stamp (D-047). The `raft` loop stamps each `core.step` and
//! traces that step's events with the stamp after the persist; the `apply` task
//! stamps each entry as it takes it and each take as it takes the job; the
//! `snapshot` task stamps a timeout pass, a stream's opening and the install it is
//! handed; re-seed mode stamps the stream it has staged; and a refusal is stamped
//! when the open returns it, before the lost mark is written.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::{
    ApplyEffect, Clock, Decision, Either, Environment, Instant, Network, Rng, Socket, TraceEvent,
    race,
};
use ananke_storage::{Engine, EngineConfig};

use crate::apply::{Command, Outcome, apply_command, user_key};
use crate::client::{self, Reply, Request, Response};
use crate::core::{Input, Output, Raft, RaftConfig, SnapshotAction, Variant, Variants};
use crate::format::{self, FormatChecked, FormatRefused};
use crate::message::{Frame, Message, SnapshotStatus};
use crate::queue::Queue;
use crate::snapshot::{self, Assembler, Feed, Repair, Sender, Staged};
use crate::store::{
    Damage, FIRST_INCARNATION, KeyPrefix, LostState, RaftStore, Recovered, SnapshotRecord,
    mark_store, mark_store_lost, refuse_lost_store,
};
use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// What one server needs to run.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// This server.
    pub id: ServerId,
    /// The address it binds.
    pub listen: SocketAddr,
    /// The address book: every server that may exist and its address, this
    /// server included, whether or not it is a voter today — membership changes
    /// (RAFT.md §1) make voters of servers that were not.
    pub servers: Vec<(ServerId, SocketAddr)>,
    /// The voters a FRESH store starts with. A store already holding a
    /// configuration entry uses that one instead (RAFT.md §1); a server that is
    /// not yet in any configuration starts with an empty list and sits quiet
    /// until a leader's entries reach it.
    pub initial_voters: Vec<ServerId>,
    /// The core's parameters.
    pub raft: RaftConfig,
    /// The engine's. Fallback and head-gap discard are turned off and log-damage
    /// refusal on whatever is passed (RAFT.md §3).
    pub engine: EngineConfig,
    /// How many messages the inbox holds before it drops.
    pub inbox_capacity: usize,
}

/// The server's clock in nanoseconds: what the core's lease arithmetic runs on.
fn now_nanos<E: Environment>(env: &E) -> u64 {
    env.clock().now().as_nanos()
}

/// What the `raft` task takes from its inbox.
enum Event {
    /// A protocol message from a peer.
    Message {
        /// The peer.
        from: ServerId,
        /// The message.
        message: Message,
        /// The stamp the `net` task took as it received the frame. The message may
        /// wait in the inbox behind a persist or an install before a step takes
        /// it, and that step's term change says when its cause arrived (D-050).
        // PROPOSED(D-050): a term's record carries when the message its step took
        // was received.
        received: Decision,
    },
    /// A client's request, with where to answer.
    Request { from: SocketAddr, request: Request },
    /// The `apply` task applied through this index.
    Applied(Index),
    /// The `apply` task completed a take: a checkpoint at `index`, whose entry
    /// has `term`, is on disk and recorded.
    Taken { index: Index, term: Term },
    /// The `apply` task could not complete a take.
    TakeFailed,
    /// The `apply` task wrote a follower's snapshot record at `index`, whose
    /// entry has `term`: the compaction point is durable and no checkpoint sits
    /// under it (D-065). It reaches the core as [`Input::SnapshotTaken`], which
    /// is what a record is; it sweeps no versions, because it made none.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    Recorded { index: Index, term: Term },
    /// The `snapshot` task streamed a snapshot to `to`, which installed it and
    /// answered with the incarnation of the store it runs on now.
    StreamDone {
        to: ServerId,
        index: Index,
        incarnation: u64,
    },
    /// The `snapshot` task gave up streaming to `to`; with `retake` the
    /// checkpoint itself is unusable.
    StreamFailed { to: ServerId, retake: bool },
    /// The `snapshot` task assembled and verified a whole stream: the `raft` loop
    /// must quiesce applies and hand back a repair or a skip.
    SnapshotReady { last_index: Index, last_term: Term },
    /// The `snapshot` task finished acting on the decision: with `reinstall` the
    /// staged store is complete and this incarnation retires.
    SnapshotFinished { reinstall: bool },
    /// The `apply` task drained its queue and stopped.
    ApplyClosed,
}

/// What the `apply` task takes from its queue.
enum Job {
    /// Committed entries to apply in order.
    Entries(Vec<Entry>),
    /// Take a snapshot at the applied index, between applies (RAFT.md §1). At
    /// the index the record already names, with its version complete, the
    /// recorded version is answered instead of a second one of the same state
    /// (D-043).
    Take,
    /// Take a fresh version even at the record's index: the recorded one was
    /// found unusable, by a stream that could not open it or a receiver that
    /// refused it (D-043).
    Retake,
    /// Write the snapshot record at the applied index and nothing else: a
    /// follower's compaction point, with no checkpoint under it (D-065). Like a
    /// take it runs between applies, so the index, its term and the
    /// configuration in force at it are exact (D-036).
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    Record,
}

/// What the `snapshot` task takes from its queue.
enum Snap {
    /// The core asks for a stream (a take goes to the `apply` task instead).
    Action(SnapshotAction),
    /// The `apply` task completed a take: the record names a new version, and
    /// the ones nothing reads can go (D-043).
    Taken,
    /// An `InstallSnapshot` chunk from a peer.
    Chunk { from: ServerId, message: Message },
    /// An `InstallSnapshotResponse` from a peer.
    Ack { from: ServerId, message: Message },
    /// The `raft` loop's decision on a [`Event::SnapshotReady`]: finish the
    /// install with this repair.
    Finish(Repair),
    /// The decision: the store already holds everything the stream carries;
    /// answer installed and drop the staging.
    Skip,
}

/// A client request waiting for its entry to apply.
struct Waiting {
    term: Term,
    from: SocketAddr,
    client: u64,
    seq: u64,
}

type Pending = Arc<Mutex<BTreeMap<Index, Waiting>>>;

fn lock_pending(pending: &Pending) -> std::sync::MutexGuard<'_, BTreeMap<Index, Waiting>> {
    pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The followers whose snapshot stream had a chunk acknowledged past the furthest
/// point it had reached, marked by the `snapshot` task and taken by the `raft` loop
/// before each tick, whose check quorum reads them (D-049). A mark is a lock
/// and an insert, never an event on the inbox, so no task wakes that would not
/// have woken anyway and a run whose leader never counts a refused follower's
/// answers steps its cores exactly as it did before.
// D-049: a refused follower counts for check quorum only while its re-seed stream
// progresses.
type StreamAcks = Arc<Mutex<BTreeSet<ServerId>>>;

/// Takes every follower marked in `acks` since the last take, in id order.
fn take_stream_acks(acks: &StreamAcks) -> BTreeSet<ServerId> {
    std::mem::take(
        &mut *acks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

/// Sends `message` to `to`, stamping the clock where the lease reads it: `sent` on
/// an AppendEntries, `local` on its response (RAFT.md §1); and stamping
/// `incarnation`, the sender's store incarnation, on an AppendEntries or
/// InstallSnapshot response (RAFT.md §3, D-042) — 0 from a refused
/// server, which has no store.
async fn send_message<E: Environment>(
    env: &E,
    sock: &<E::Net as Network>::Socket,
    addrs: &BTreeMap<ServerId, SocketAddr>,
    from: ServerId,
    to: ServerId,
    mut message: Message,
    incarnation: u64,
) {
    match &mut message {
        Message::AppendEntries { sent, .. } => *sent = now_nanos(env),
        Message::AppendEntriesResponse {
            local,
            incarnation: mine,
            ..
        } => {
            *local = now_nanos(env);
            *mine = incarnation;
        }
        Message::InstallSnapshotResponse {
            incarnation: mine, ..
        } => *mine = incarnation,
        _ => {}
    }
    if let Some(addr) = addrs.get(&to) {
        let frame = Frame { from, message };
        let _ = sock.send(*addr, frame.encode()).await;
    }
}

/// How one incarnation ended.
enum Next {
    /// The socket closed: the server is done.
    Closed,
    /// An install completed: adopt the staged store and run on it.
    Reinstall,
}

/// The group today's one-group server keeps its Raft state under: SHARD.md §2's
/// range 2, the rest of the keyspace, which is what this one group replicates
/// (ranges 0 and 1 are the root and meta ranges of the system tenant). Stage B
/// gives a server a group per range and this constant goes.
// PROPOSED(D-060): today's group is group 2.
pub const SINGLE_GROUP: u64 = 2;

/// How a start ended (see [`start_store`]).
#[doc(hidden)]
#[expect(
    clippy::large_enum_variant,
    reason = "the opened store is the point of the opened arm, and a start is made once"
)]
pub enum Start<E: Environment> {
    /// The store opened; `adopted` says whether an install was adopted first.
    Opened {
        /// The store.
        store: RaftStore<E>,
        /// What it found beside the store itself.
        recovered: Recovered,
        /// Whether a completed install was adopted first.
        adopted: bool,
    },
    /// Lost state or damage (D-022, D-041, D-044): the caller marks the loss and
    /// re-seeds.
    Refused(io::Error),
    /// A format refusal (D-059) or an I/O error that is not a refusal: nothing
    /// was written after the failure, and the caller traces `RaftServerFailed`
    /// and stops.
    Failed(io::Error),
}

/// The order a start runs its checks in: the correct one, or a known-buggy one a
/// directed test must catch beside it (CLAUDE.md's pair rule). Not a
/// [`Variant`]: no sweep reaches an old-format store or runs a start in another
/// order, so §10's sweep standards do not apply to these.
// PROPOSED(D-060)
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StartOrder {
    /// The server's own order: the format, a fresh directory's record, the
    /// adoption, the heal, a damaged record, the marker, the engine, the store.
    #[default]
    Correct,
    /// The version read after the engine's open and after lost state, as D-059's
    /// first draft proposed: a 0.3.0 store that also lost state is marked lost
    /// and re-seeded into its own directory, and the engine writes a log segment
    /// into a store this build will not read.
    LostStateBeforeFormat,
    /// A fresh directory's record written after the engine's open and the
    /// store's first batch: a crash between them leaves a store this build wrote
    /// and refuses as 0.3.0's.
    FormatAfterFirstBatch,
    /// A record with one bad copy healed by tmp and rename instead of in place:
    /// under a lost fsync the renamed inode's content can be torn, which loses
    /// the copy that was still valid.
    HealByRename,
    /// An unreadable record beside a store read as no record at all: a store
    /// whose record rotted twice stops the server instead of re-seeding.
    UnreadableIsUnrecorded,
    /// The adoption without its staged-record check: an install of another
    /// format is adopted over the store.
    StagedFormatUnchecked,
}

/// The server's start, in the order of D-059 and PROPOSED D-060: the format
/// first, read before anything writes; a fresh directory's record; the adoption
/// of a completed install, with the staged install's own format checked before
/// its first write; the heal of a record with one damaged copy; a record that
/// cannot be read, which is lost state; the store marker; and then the engine
/// and the store. Every step before the record's write only reads, and every
/// refusal before the engine's open writes nothing at all.
///
/// The `raft` tests and the server share this, so a test drives the order the
/// server runs; `order` is the pair rule's known-buggy alternative, which no
/// sweep runs.
// D-059: the format is read before anything writes, and before lost state.
// PROPOSED(D-060): the start's order.
#[doc(hidden)]
pub async fn start_store<E: Environment>(
    env: &E,
    server: u64,
    engine: &EngineConfig,
    variants: Variants,
    prefix: &KeyPrefix,
    order: StartOrder,
) -> Start<E> {
    if order == StartOrder::LostStateBeforeFormat {
        return start_lost_state_first(env, server, engine, variants, prefix).await;
    }
    let dir = engine.dir.clone();
    // 1. The format (D-059). A refusal or a read error stops the server with
    //    nothing written: a store in another format lost nothing and is not this
    //    server's to replace.
    let verdict = match format::check_format(env, &dir).await {
        Ok(verdict) => verdict,
        Err(error) if FormatRefused::from_io(&error).is_some() => return Start::Failed(error),
        Err(error) => {
            return Start::Failed(io::Error::new(
                error.kind(),
                format!("reading the store's format: {error}"),
            ));
        }
    };
    // 2. A fresh directory's first write is its record, before the engine or
    //    anything else creates an entry there (PROPOSED D-060).
    let mut damaged = false;
    let mut heal = None;
    let mut late = None;
    let checked = match verdict {
        format::Verdict::Fresh(fresh) => {
            if order == StartOrder::FormatAfterFirstBatch {
                late = Some(fresh);
                FormatChecked::new(&dir)
            } else {
                match format::record_format(env, fresh).await {
                    Ok(checked) => checked,
                    Err(error) => {
                        return Start::Failed(io::Error::new(
                            error.kind(),
                            format!("recording the store's format: {error}"),
                        ));
                    }
                }
            }
        }
        format::Verdict::Recorded {
            checked,
            whole,
            len,
        } => {
            if !whole {
                heal = Some(len);
            }
            checked
        }
        format::Verdict::Damaged => {
            if order == StartOrder::UnreadableIsUnrecorded {
                return Start::Failed(
                    FormatRefused {
                        dir: dir.clone(),
                        subject: format::Subject::Store,
                        found: format::Found::Unrecorded,
                        expected: format::STORE_FORMAT,
                    }
                    .into_io(),
                );
            }
            damaged = true;
            FormatChecked::new(&dir)
        }
    };
    // 3. The adoption (D-041), with the staged install's own format read before
    //    its first write, and the store's record rewritten after the switch when
    //    it was the damaged part.
    let adopted = match snapshot::adopt_checked(
        env,
        &dir,
        variants,
        damaged,
        order != StartOrder::StagedFormatUnchecked,
    )
    .await
    {
        Ok(adopted) => adopted,
        Err(error) if LostState::from_io(&error).is_some() => return Start::Refused(error),
        Err(error) if FormatRefused::from_io(&error).is_some() => return Start::Failed(error),
        Err(error) => {
            return Start::Failed(io::Error::new(
                error.kind(),
                format!("adopting an installed snapshot: {error}"),
            ));
        }
    };
    if adopted {
        // D-047, an open point: the adoption decides to adopt inside
        // `adopt_checked`, once it has read the staged CURRENT and manifest, and
        // does the copies in the same call, so no stamp taken out here could be
        // that decision's; it is traced as decided when it is recorded, which no
        // earlier time is provably.
        env.trace(TraceEvent::RaftAdopted { server });
    }
    // The adoption rewrote a record that could not be read, so the directory's
    // format is this build's again.
    let damaged = damaged && !adopted;
    // 4. The heal of a record with one damaged copy, after the adoption, so a
    //    staged install refused for its format stops the server with nothing of
    //    the store written.
    if let Some(len) = heal {
        let healed = if order == StartOrder::HealByRename {
            format::rewrite_format(env, &dir).await.map(|_| ())
        } else {
            format::heal_format(env, &checked, len).await
        };
        if let Err(error) = healed {
            return Start::Failed(io::Error::new(
                error.kind(),
                format!("healing the store's format record: {error}"),
            ));
        }
    }
    // 5. A record that cannot be read beside a store is lost state (D-044),
    //    after the adoption: refusing before it would never adopt the install
    //    the re-seed this refusal asks for stages, and the server would loop.
    if damaged {
        return Start::Refused(LostState::from_damage(Damage::FormatUnreadable).into_io());
    }
    // 6. The marker (D-041, D-044).
    if !variants.contains(Variant::AdoptionAsBuilt)
        && let Err(error) = refuse_lost_store(env, &dir).await
    {
        return Start::Refused(error);
    }
    // 7. The engine, then the store.
    open_engine_and_store(env, engine, variants, prefix, checked, adopted, late).await
}

/// Steps 7 and 8 of [`start_store`]: the engine, the store, and — for the
/// known-buggy order that records a fresh directory's format last — the record.
async fn open_engine_and_store<E: Environment>(
    env: &E,
    engine: &EngineConfig,
    variants: Variants,
    prefix: &KeyPrefix,
    checked: FormatChecked,
    adopted: bool,
    late: Option<format::FreshDir>,
) -> Start<E> {
    let (opened, recovery) = match Engine::open(env.clone(), engine.clone()).await {
        Ok(opened) => opened,
        Err(error) => return Start::Refused(error),
    };
    let opened = Arc::new(opened);
    let (store, recovered) =
        match RaftStore::open(opened.clone(), &recovery, prefix.clone(), checked).await {
            Ok(opened) => opened,
            Err(error) => {
                // D-044: the engine that recovered the hole does no more work. It
                // started quiesced when the recovery itself reported the loss; this
                // is the refusal the store alone can see.
                if !variants.contains(Variant::RefusalNotDurable) {
                    opened.quiesce();
                }
                return Start::Refused(error);
            }
        };
    if let Some(fresh) = late
        && let Err(error) = format::record_format(env, fresh).await
    {
        return Start::Failed(io::Error::new(
            error.kind(),
            format!("recording the store's format: {error}"),
        ));
    }
    Start::Opened {
        store,
        recovered,
        adopted,
    }
}

/// The known-buggy order D-059's first draft proposed and the check patch built:
/// the adoption with no gate and no staged check, the marker, the engine and the
/// recovery's loss, and only then the format. A 0.3.0 store that also lost state
/// is marked lost and re-seeded into its own directory, and the engine's open
/// has already written a log segment into it.
// PROPOSED(D-060): the pair for the start's order.
async fn start_lost_state_first<E: Environment>(
    env: &E,
    server: u64,
    engine: &EngineConfig,
    variants: Variants,
    prefix: &KeyPrefix,
) -> Start<E> {
    let dir = engine.dir.clone();
    let adopted = match snapshot::adopt_checked(env, &dir, variants, false, false).await {
        Ok(adopted) => adopted,
        Err(error) if LostState::from_io(&error).is_some() => return Start::Refused(error),
        Err(error) => {
            return Start::Failed(io::Error::new(
                error.kind(),
                format!("adopting an installed snapshot: {error}"),
            ));
        }
    };
    if adopted {
        env.trace(TraceEvent::RaftAdopted { server });
    }
    if !variants.contains(Variant::AdoptionAsBuilt)
        && let Err(error) = refuse_lost_store(env, &dir).await
    {
        return Start::Refused(error);
    }
    let started = open_engine_and_store(
        env,
        engine,
        variants,
        prefix,
        FormatChecked::new(&dir),
        adopted,
        None,
    )
    .await;
    let Start::Opened {
        store,
        recovered,
        adopted,
    } = started
    else {
        return started;
    };
    match format::check_format(env, &dir).await {
        Ok(format::Verdict::Fresh(fresh)) => match format::record_format(env, fresh).await {
            Ok(_) => {}
            Err(error) => return Start::Failed(error),
        },
        Ok(format::Verdict::Recorded { .. }) => {}
        Ok(format::Verdict::Damaged) => {
            return Start::Refused(LostState::from_damage(Damage::FormatUnreadable).into_io());
        }
        Err(error) => return Start::Failed(error),
    }
    Start::Opened {
        store,
        recovered,
        adopted,
    }
}

/// Runs one server until its store fails or its socket closes. Spawn it with
/// `Environment::spawn`; spawn it again after a crash to restart the server on what
/// its disk kept.
///
/// # Errors
///
/// The bind, or an I/O error while running; each is traced before it is returned.
/// A store refused for lost state is no longer an error: the server runs in
/// re-seed mode until a leader's snapshot rebuilds it (RAFT.md §3). A store in
/// another on-disk format is an error, and nothing was written to it (D-059).
pub async fn run<E: Environment>(env: E, config: NodeConfig) -> io::Result<()> {
    let NodeConfig {
        id,
        listen,
        servers,
        initial_voters,
        raft,
        engine,
        inbox_capacity,
    } = config;
    let server = id.0;
    // D-041: the as-built adoption is the server before the store
    // marker existed: it neither checks nor writes one, lost mark included, so
    // its disk sees the operations the nightly's did but for the store's own —
    // PROPOSED(D-060): the format record is read at every start and written by
    // every fresh one, under this variant as under the correct server, since the
    // format rule is not what the variant models.
    let as_built = raft.variants.contains(Variant::AdoptionAsBuilt);
    // D-044: a refusal is recorded in the store directory before
    // anything else and quiesces the engine that recovered the hole. The
    // as-built variant writes no marker at all; `RefusalNotDurable` is the
    // server before either half of the fix.
    let durable_refusal = !as_built && !raft.variants.contains(Variant::RefusalNotDurable);
    let quiesce_refused = !raft.variants.contains(Variant::RefusalNotDurable);
    // The store's recovery must never hand back a state with a hole (RAFT.md §3):
    // no fallback, no discarded head, and a damaged log refused before it is cut.
    let engine = EngineConfig {
        allow_manifest_fallback: false,
        allow_head_gap: false,
        refuse_log_damage: true,
        // D-044: the engine that recovered a hole starts quiesced, so
        // not even a flush between the open and the store's refusal can rewrite
        // the manifest without the dropped table or delete the log segments that
        // held its records.
        quiesce_on_loss: quiesce_refused,
        ..engine
    };
    let sock = Arc::new(env.net().bind(listen).await?);
    let addrs: Arc<BTreeMap<ServerId, SocketAddr>> = Arc::new(servers.iter().copied().collect());
    // The voters a FRESH store starts with (D-029): a store already holding a
    // configuration entry, or a snapshot record, restores that one instead. The
    // address book above is every server that may exist, voter or not.
    let voters: Vec<ServerId> = initial_voters;
    let inbox: Queue<Event> = Queue::new();

    env.spawn("net", {
        let env = env.clone();
        let inbox = inbox.clone();
        let sock = sock.clone();
        async move {
            loop {
                let Ok((from, bytes)) = sock.recv().await else {
                    break;
                };
                if client::is_client(&bytes) {
                    if let Ok(request) = Request::decode(bytes) {
                        inbox.push(Event::Request { from, request });
                    }
                    continue;
                }
                let Ok(frame) = Frame::decode(bytes) else {
                    continue;
                };
                // PROPOSED(D-050): when the frame reached the server, a
                // stamp that reads the time and nothing else (D-047), so it moves
                // no schedule.
                let received = env.decision();
                admit(&env, server, &inbox, inbox_capacity, frame, received);
            }
            inbox.close();
        }
    });

    // PROPOSED(D-060): today's one group, whose Raft state is one key interval
    // under `0 / <group>`; Stage B gives a server a group per range.
    let prefix = KeyPrefix::group(SINGLE_GROUP);
    loop {
        // The start, in the order of D-059 and PROPOSED D-060: the format read
        // before anything writes, the adoption (D-041), the marker (D-044), the
        // engine and the store.
        let started = start_store(
            &env,
            server,
            &engine,
            raft.variants,
            &prefix,
            StartOrder::Correct,
        )
        .await;
        let (store, recovered) = match started {
            // D-059: a store in another format lost nothing and is not this
            // server's to replace: no lost mark, no re-seed, nothing written.
            Start::Failed(error) => {
                env.trace(TraceEvent::RaftServerFailed {
                    server,
                    reason: error.to_string(),
                });
                return Err(error);
            }
            Start::Refused(error) => {
                // D-047: refused when the start returned the loss, before the
                // mark's write and sync.
                let refused = env.decision();
                // D-044: before the trace, before the re-seed, before anything
                // that can be interrupted: the store directory itself records
                // that this store lost state, so a restart cannot find it whole
                // again.
                record_loss(&env, server, durable_refusal, &engine.dir, &error).await?;
                env.trace_decided(
                    refused,
                    TraceEvent::RaftRefused {
                        server,
                        reason: error.to_string(),
                    },
                );
                // The replica the refusal took down, named beside it. This server
                // hosts one group, so there is exactly one; the node hosting many
                // traces one per range it holds. A reader takes "the ranges this
                // server held" from these and not from the ranges of the run, which
                // on a node would include ranges it never held (D-077,
                // `Report::ranges_with_a_majority_up`). It is traced with the
                // refusal's own decision stamp: the same instant decided both.
                // PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per
                // replica.
                env.trace_decided(
                    refused,
                    TraceEvent::RaftReplicaRefused {
                        server,
                        range: SINGLE_GROUP,
                    },
                );
                // Re-seed mode (RAFT.md §3): the store is gone; wait for a
                // leader's snapshot to rebuild it, taking part in nothing else.
                match reseed(
                    &env,
                    id,
                    &sock,
                    &addrs,
                    &raft,
                    &engine.dir,
                    &inbox,
                    prefix.clone(),
                )
                .await
                {
                    Next::Closed => return Ok(()),
                    Next::Reinstall => continue,
                }
            }
            Start::Opened {
                store, recovered, ..
            } => (store, recovered),
        };
        // D-041: the marker, written once the directory has opened as a
        // store — a fresh directory at its first open — and kept for good.
        if !as_built && let Err(error) = mark_store(&env, &engine.dir).await {
            env.trace(TraceEvent::RaftServerFailed {
                server,
                reason: format!("marking the store: {error}"),
            });
            return Err(error);
        }
        let next = incarnation(
            &env,
            id,
            &sock,
            &addrs,
            &voters,
            raft.clone(),
            engine.dir.clone(),
            &inbox,
            Arc::new(store),
            recovered,
            prefix.clone(),
        )
        .await?;
        match next {
            Next::Closed => return Ok(()),
            Next::Reinstall => {}
        }
    }
}

/// Records a refusal in the store directory before the server acts on it: the
/// marker says this store lost state, with the reason, written and synced
/// through the filesystem rather than through the engine, which is the thing
/// that is damaged (RAFT.md §3). Every open after it refuses on the mark alone
/// until an install replaces the store, so a refusal outlives the process that
/// made it. `durable` is off for the variants that are the server before this:
/// `RefusalNotDurable`, and `AdoptionAsBuilt`, which writes no marker at all.
///
/// A marker that cannot be written is a server that cannot say it lost state,
/// which is the failure D-044 exists to prevent: it fails the server rather
/// than running on in re-seed mode with a disk that will open clean.
// D-044: a durable refusal, and a refused engine that does no work.
async fn record_loss<E: Environment>(
    env: &E,
    server: u64,
    durable: bool,
    engine_dir: &Path,
    refusal: &io::Error,
) -> io::Result<()> {
    if !durable {
        return Ok(());
    }
    if let Err(error) = mark_store_lost(env, engine_dir, &refusal.to_string()).await {
        env.trace(TraceEvent::RaftServerFailed {
            server,
            reason: format!("recording the store's lost state: {error}"),
        });
        return Err(error);
    }
    Ok(())
}

/// One incarnation: the server on one store, from restatement to the switch that
/// retires it.
#[expect(clippy::too_many_arguments, reason = "an incarnation names its world")]
async fn incarnation<E: Environment>(
    env: &E,
    id: ServerId,
    sock: &Arc<<E::Net as Network>::Socket>,
    addrs: &Arc<BTreeMap<ServerId, SocketAddr>>,
    voters: &[ServerId],
    raft: RaftConfig,
    engine_dir: PathBuf,
    inbox: &Queue<Event>,
    store: Arc<RaftStore<E>>,
    recovered: Recovered,
    prefix: KeyPrefix,
) -> io::Result<Next> {
    let server = id.0;
    let tick = Duration::from_nanos(raft.tick_nanos);
    let variants = raft.variants;
    let seed = env.rng().next_u64();
    let snapshot_record = recovered.snapshot.clone();
    let (snap_index, snap_term) = snapshot_record
        .as_ref()
        .map_or((0, 0), |s| (s.last_index, s.last_term));
    let quarantined = recovered.quarantined;
    let mut core = Raft::restore_compacted(
        id,
        Configuration::of(voters),
        raft.clone(),
        seed,
        store.term(),
        store.vote(),
        snap_index,
        snap_term,
        // A compacted store's configuration entry may sit at or below the
        // snapshot, where only the record still holds it (RAFT.md §3, D-029).
        snapshot_record.as_ref().map(|s| s.config.clone()),
        recovered.log,
        quarantined,
    );
    let applied = store.applied();
    core.step(Input::Applied(applied));
    let jobs: Queue<Job> = Queue::new();
    let snaps: Queue<Snap> = Queue::new();
    let pending: Pending = Arc::default();
    let stream_acks: StreamAcks = Arc::default();

    // D-043: the checkpoint versions the record does not name are old
    // ones — a predecessor incarnation's, or a leader's from before this store
    // was installed — and no stream of this incarnation reads any yet. They go
    // now, before any task runs, so that a take of this incarnation can never
    // share a name with a directory that still holds files: the take counter
    // restarts at zero on an installed store, and a re-seeded server's lost
    // store may have taken at the index it takes at again.
    if !variants.contains(Variant::SharedSnapshotDir) {
        sweep_versions(env, id, &store, &engine_dir, &BTreeMap::new()).await;
    }
    spawn_apply(
        env,
        id,
        inbox,
        &jobs,
        &store,
        sock,
        &pending,
        &engine_dir,
        core.applied_membership(),
        core.term_at(applied).unwrap_or(0),
        variants,
    );
    env.spawn(
        "snapshot",
        snapshot_task(
            env.clone(),
            id,
            sock.clone(),
            addrs.clone(),
            store.clone(),
            raft.clone(),
            engine_dir.clone(),
            inbox.clone(),
            snaps.clone(),
            stream_acks.clone(),
            prefix,
        ),
    );

    // The start: the log as the disk holds it, re-stated so the trace's picture of
    // this server's log is the durable one. An append or a truncation persisted at a
    // crash but not yet traced would otherwise be missing from it. A snapshot the
    // store records is re-stated the same way: it sets the applied floor and stands
    // in for the log prefix it replaced (RAFT.md §2).
    //
    // D-047: the restatement is decided as it is traced. It reports state
    // that was durable before this incarnation began, nothing a step of it decided,
    // and the incarnation starts here: nothing awaits between these records and the
    // loop arming its first tick, which is where the new core's election timer
    // really starts counting. The sweep's await above decides nothing they report.
    env.trace(TraceEvent::RaftTruncate {
        server,
        range: SINGLE_GROUP,
        from_index: core.last_index() + 1,
    });
    if snap_index > 0 {
        env.trace(TraceEvent::RaftSnapshot {
            server,
            range: SINGLE_GROUP,
            last_index: snap_index,
            last_term: snap_term,
            taken: snapshot_record.as_ref().is_some_and(|s| s.taken),
        });
    }
    if quarantined {
        env.trace(TraceEvent::RaftReseeded {
            server,
            range: SINGLE_GROUP,
        });
    }
    for entry in core.log() {
        env.trace(TraceEvent::RaftAppend {
            server,
            range: SINGLE_GROUP,
            index: entry.index,
            entry_term: entry.term,
            hash: entry.payload.hash(),
        });
    }
    // The configuration in force, re-stated after the log so the trace always
    // shows every server's, initial or restored (RAFT.md §1); before
    // RaftRecovered, an order that is kept stable (D-029).
    let membership = core.membership();
    env.trace(TraceEvent::RaftConfig {
        server,
        range: SINGLE_GROUP,
        index: core.membership_index(),
        old: membership.voters.iter().map(|s| s.0).collect(),
        new: membership
            .new_voters
            .as_ref()
            .map(|new| new.iter().map(|s| s.0).collect())
            .unwrap_or_default(),
        joint: membership.new_voters.is_some(),
        learners: membership.learners.iter().map(|s| s.0).collect(),
    });
    // Then what the server resumes from, and the term for the trace's term lane and
    // the sweep's timer check, which counts from here.
    env.trace(TraceEvent::RaftRecovered {
        server,
        range: SINGLE_GROUP,
        term: core.term(),
        applied,
        last_index: core.last_index(),
        incarnation: store.incarnation(),
    });
    env.trace(TraceEvent::RaftTerm {
        server,
        range: SINGLE_GROUP,
        term: core.term(),
        role: "follower",
        // PROPOSED(D-050): a term's record carries when the message its step
        // took was received; a restatement is no message's step.
        received: None,
    });
    let mut node = Server {
        env: env.clone(),
        id,
        sock: sock.clone(),
        addrs: addrs.clone(),
        store: store.clone(),
        engine_dir,
        jobs,
        snaps: snaps.clone(),
        pending,
        variants,
        apply_sent: applied,
        proposed: BTreeMap::new(),
        reads: BTreeMap::new(),
        next_read: 0,
        take_in_flight: false,
        fresh_take: false,
    };
    let mut next_tick = env.clock().now() + tick;
    let next = loop {
        let event = {
            let pop = pin!(inbox.pop());
            let timer = pin!(env.clock().sleep_until(next_tick));
            match race(env, pop, timer).await {
                Either::Left(Some(event)) => Some(event),
                Either::Left(None) => break Next::Closed,
                Either::Right(()) => {
                    next_tick += tick;
                    None
                }
            }
        };
        // PROPOSED(D-050): when the peer's message this step takes was
        // received, for the term change it may trace; none for any other input.
        let mut received = None;
        let (input, request, read, change) = match event {
            None => {
                // The re-seed progress the `snapshot` task saw since the last
                // tick, stepped before the tick whose check quorum may read it.
                // D-049: a refused follower counts for check quorum only while
                // its re-seed stream progresses.
                for to in take_stream_acks(&stream_acks) {
                    let decided = env.decision();
                    let outputs = core.step(Input::SnapshotAcked { to });
                    // PROPOSED(D-050): re-seed progress is no peer's message
                    // on the inbox, so the step carries no receipt.
                    node.execute(&core, outputs, decided, None).await?;
                }
                (Input::Tick, None, None, None)
            }
            Some(Event::Message {
                from,
                message,
                received: at,
            }) => match message {
                // Snapshot streaming is the snapshot task's (RAFT.md §3).
                Message::InstallSnapshot { .. } => {
                    node.snaps.push(Snap::Chunk { from, message });
                    continue;
                }
                Message::InstallSnapshotResponse { .. } => {
                    node.snaps.push(Snap::Ack { from, message });
                    continue;
                }
                message => {
                    received = Some(at);
                    (
                        Input::Message {
                            from,
                            message,
                            now: now_nanos(env),
                        },
                        None,
                        None,
                        None,
                    )
                }
            },
            Some(Event::Applied(index)) => (Input::Applied(index), None, None, None),
            Some(Event::Taken { index, term }) => {
                // The versions nothing reads any more can go (D-043).
                node.take_in_flight = false;
                node.fresh_take = false;
                node.snaps.push(Snap::Taken);
                (Input::SnapshotTaken { index, term }, None, None, None)
            }
            Some(Event::Recorded { index, term }) => {
                // No version was written, so nothing is swept and `fresh_take`
                // is left as it was: the record itself has `taken` false, so the
                // next take is a fresh version whatever the flag says (D-065).
                // PROPOSED(D-078): a follower compacts its log to its own applied
                // index.
                node.take_in_flight = false;
                (Input::SnapshotTaken { index, term }, None, None, None)
            }
            Some(Event::TakeFailed) => {
                node.take_in_flight = false;
                node.fresh_take = true;
                (
                    Input::SnapshotFailed {
                        to: id,
                        retake: true,
                    },
                    None,
                    None,
                    None,
                )
            }
            Some(Event::StreamDone {
                to,
                index,
                incarnation,
            }) => (
                Input::SnapshotInstalled {
                    to,
                    index,
                    incarnation,
                },
                None,
                None,
                None,
            ),
            Some(Event::StreamFailed { to, retake }) => {
                // D-043: a stream that found no usable version while a
                // take is in flight waits for that take rather than asking for
                // another — the core would clear its pending take and queue a
                // second one at the same index behind the first, the re-take
                // cascade of seed 5909. Otherwise the checkpoint really is
                // unusable, and the next take is a fresh version. The server as
                // built (`SharedSnapshotDir`) asks every time.
                let retake = retake
                    && (!node.take_in_flight || node.variants.contains(Variant::SharedSnapshotDir));
                if retake {
                    node.fresh_take = true;
                }
                (Input::SnapshotFailed { to, retake }, None, None, None)
            }
            Some(Event::SnapshotReady {
                last_index,
                last_term,
            }) => {
                match install_decision(&mut node, &mut core, inbox, last_index, last_term).await? {
                    Some(next) => break next,
                    None => continue,
                }
            }
            Some(Event::SnapshotFinished { .. } | Event::ApplyClosed) => continue,
            Some(Event::Request { from, request }) => {
                if let Command::Transfer { to } = request.command {
                    // An operator's wish, acted on at once and answered at once.
                    let response = Response {
                        client: request.client,
                        seq: request.seq,
                        reply: Reply::Outcome(Outcome::Done),
                    };
                    let _ = node.sock.send(from, response.encode()).await;
                    (Input::Transfer(ServerId(to)), None, None, None)
                } else if let Command::Change { voters } = &request.command {
                    // An operator's membership change (RAFT.md §1): the command
                    // is only the trigger, the change itself becomes
                    // configuration entries. Answered Done once the leader
                    // accepts it — completion is observable in the trace — and
                    // NotLeader when it refuses, naming itself when the refusal
                    // is a change already in flight (D-029).
                    let voters = voters.iter().copied().map(ServerId).collect();
                    (Input::Change(voters), None, None, Some((from, request)))
                } else if matches!(request.command, Command::Get { .. }) {
                    // A read: served by the lease or after a heartbeat round, never
                    // through the log.
                    let id = node.next_read;
                    node.next_read += 1;
                    node.reads.insert(id, (from, request));
                    (
                        Input::Read {
                            id,
                            now: now_nanos(env),
                        },
                        None,
                        Some(id),
                        None,
                    )
                } else {
                    if let Some(&(index, term)) = node.proposed.get(&(request.client, request.seq))
                        && core.term_at(index) == Some(term)
                    {
                        // A copy of a request whose entry is in the log: it is
                        // answered when that entry applies, once.
                        continue;
                    }
                    (
                        Input::Propose(request.command.encode()),
                        Some((from, request)),
                        None,
                        None,
                    )
                }
            }
        };
        // D-047: the step's decision time. Its events are traced after
        // the persist it asks for, which is their durability time, with this stamp.
        let decided = env.decision();
        let outputs = core.step(input);
        if let Some(id) = read
            && let Some(leader) = outputs.iter().find_map(|output| match output {
                Output::Rejected { leader } => Some(*leader),
                _ => None,
            })
        {
            node.answer_read(id, Reply::NotLeader { leader }).await;
        }
        if let Some((from, request)) = change {
            let refused = outputs.iter().find_map(|output| match output {
                Output::Rejected { leader } => Some(*leader),
                _ => None,
            });
            let reply = match refused {
                Some(leader) => Reply::NotLeader { leader },
                None => Reply::Outcome(Outcome::Done),
            };
            let response = Response {
                client: request.client,
                seq: request.seq,
                reply,
            };
            let _ = node.sock.send(from, response.encode()).await;
        }
        if let Some((from, request)) = request {
            let rejected = outputs.iter().find_map(|output| match output {
                Output::Rejected { leader } => Some(*leader),
                _ => None,
            });
            match rejected {
                Some(leader) => {
                    let response = Response {
                        client: request.client,
                        seq: request.seq,
                        reply: Reply::NotLeader { leader },
                    };
                    let _ = node.sock.send(from, response.encode()).await;
                }
                None => {
                    let (index, term) = (core.last_index(), core.term());
                    env.trace_decided(
                        decided,
                        TraceEvent::RaftProposed {
                            server,
                            range: SINGLE_GROUP,
                            client: request.client,
                            seq: request.seq,
                            index,
                            term,
                        },
                    );
                    lock_pending(&node.pending).insert(
                        index,
                        Waiting {
                            term,
                            from,
                            client: request.client,
                            seq: request.seq,
                        },
                    );
                    node.proposed
                        .insert((request.client, request.seq), (index, term));
                    if node.proposed.len() > PROPOSED_REMEMBERED {
                        let keep_from = index.saturating_sub(PROPOSED_REMEMBERED as Index / 2);
                        node.proposed.retain(|_, (i, _)| *i >= keep_from);
                    }
                }
            }
        }
        node.execute(&core, outputs, decided, received).await?;
    };
    node.jobs.close();
    snaps.close();
    Ok(next)
}

/// Decides an assembled stream (RAFT.md §1): quiesce the `apply` task so the
/// applied index is final, then either skip an install the store has outgrown or
/// hand the `snapshot` task the receiver's identity to repair the staged store
/// with. Returns the incarnation's end, or `None` to keep running.
async fn install_decision<E: Environment>(
    node: &mut Server<E>,
    core: &mut Raft,
    inbox: &Queue<Event>,
    last_index: Index,
    last_term: Term,
) -> io::Result<Option<Next>> {
    // Quiesce: no apply may land between the decision and the switch, or the
    // switch would take the store backwards. Everything else that arrives while
    // waiting is dropped, which the network is allowed to do anyway.
    node.jobs.close();
    loop {
        match inbox.pop().await {
            None => return Ok(Some(Next::Closed)),
            Some(Event::ApplyClosed) => break,
            Some(Event::Applied(index)) => {
                // D-047: this step's decision time, as in the loop.
                let decided = node.env.decision();
                let outputs = core.step(Input::Applied(index));
                // PROPOSED(D-050): a completion is no peer's message, so the
                // step carries no receipt.
                node.execute(core, outputs, decided, None).await?;
            }
            Some(_) => {}
        }
    }
    let applied = node.store.applied();
    if applied >= last_index {
        // The store already holds everything the snapshot carries: answer
        // installed without switching (a stream the leader re-sent after this
        // server already switched lands here too).
        node.snaps.push(Snap::Skip);
    } else {
        let tail = if core.term_at(last_index) == Some(last_term) {
            (last_index + 1..=core.last_index())
                .filter_map(|index| core.entry(index).cloned())
                .collect()
        } else {
            // The log disagrees with the snapshot at its last index: everything
            // it holds there is from a divergent, uncommitted history (RAFT.md §1).
            Vec::new()
        };
        node.snaps.push(Snap::Finish(Repair {
            term: node.store.term(),
            vote: node.store.vote(),
            tail,
            // A quarantined server stays quarantined across any install on that
            // history: the vote its lost state may have held is still unknown.
            // D-035: re-seeded servers are quarantined from voting for good.
            quarantined: core.quarantined(),
            // An install into a live store keeps its incarnation: the kept tail
            // is everything acknowledged past the snapshot, so nothing a leader
            // matched is lost. D-042: store incarnations.
            incarnation: node.store.incarnation(),
        }));
    }
    loop {
        match inbox.pop().await {
            None => return Ok(Some(Next::Closed)),
            Some(Event::SnapshotFinished { reinstall }) => {
                if reinstall {
                    return Ok(Some(Next::Reinstall));
                }
                break;
            }
            Some(_) => {}
        }
    }
    // Not switching: the incarnation continues, with a fresh `apply` task in
    // place of the quiesced one, re-fed from the applied index.
    node.jobs = Queue::new();
    node.take_in_flight = false;
    let env = node.env.clone();
    let engine_dir = node.engine_dir.clone();
    spawn_apply(
        &env,
        node.id,
        inbox,
        &node.jobs,
        &node.store,
        &node.sock,
        &node.pending,
        &engine_dir,
        core.applied_membership(),
        core.term_at(node.store.applied()).unwrap_or(0),
        node.variants,
    );
    node.apply_sent = node.store.applied();
    node.apply_through(core, core.commit());
    Ok(None)
}

/// Spawns the `apply` task: committed entries applied one synced batch each, and
/// snapshots taken between applies (RAFT.md §1, D-036).
#[expect(clippy::too_many_arguments, reason = "the task's whole world")]
fn spawn_apply<E: Environment>(
    env: &E,
    id: ServerId,
    inbox: &Queue<Event>,
    jobs: &Queue<Job>,
    store: &Arc<RaftStore<E>>,
    sock: &Arc<<E::Net as Network>::Socket>,
    pending: &Pending,
    engine_dir: &Path,
    config: Configuration,
    applied_term: Term,
    variants: Variants,
) {
    let server = id.0;
    let env = env.clone();
    let inbox = inbox.clone();
    let jobs = jobs.clone();
    let store = store.clone();
    let sock = sock.clone();
    let pending = pending.clone();
    let engine_dir = engine_dir.to_path_buf();
    env.clone().spawn("apply", async move {
        let mut applied = store.applied();
        let mut applied_term = applied_term;
        // The configuration at the applied index, followed entry by entry so a
        // take's record carries exactly the configuration at its point
        // (RAFT.md §1, D-029).
        let mut config = config;
        while let Some(job) = jobs.pop().await {
            // D-047: a take is decided when the task takes the job; the
            // record, the checkpoint and the trace follow.
            let taken_job = env.decision();
            let fresh = matches!(job, Job::Retake);
            let entries = match job {
                Job::Entries(entries) => entries,
                Job::Record => {
                    // A follower's compaction point (D-065): the record at the
                    // applied index, synced, and no checkpoint under it. It is
                    // written here, between applies, so `applied`, `applied_term`
                    // and `config` are exactly the state a snapshot at that index
                    // would capture (D-036), which is what keeps D-029's revert
                    // floor right when the prefix swallows the configuration
                    // entry in force.
                    //
                    // The shape is an install's repair's: `taken` false and no
                    // directory, which is what makes a leader that later needs to
                    // stream ask for a take instead of opening a version that was
                    // never written (node.rs, `start_stream`). The take counter is
                    // carried forward, never reset, so a later take still numbers
                    // its directory past every one this store has made (D-043).
                    // PROPOSED(D-078): a follower compacts its log to its own
                    // applied index.
                    if applied == 0 {
                        inbox.push(Event::TakeFailed);
                        continue;
                    }
                    let previous = match store.snapshot_record().await {
                        Ok(record) => record,
                        Err(_) => {
                            inbox.push(Event::TakeFailed);
                            continue;
                        }
                    };
                    // The record the store holds goes in whole, not a field of it:
                    // what the new record keeps from the old — today the take
                    // counter — is then a decision inside `compaction_record`,
                    // where it is tested, instead of a bare number chosen here,
                    // where nothing could see it. The review of this slice planted
                    // a `0` in its place and no test at any tier noticed.
                    let recorded = store
                        .record_snapshot(&compaction_record(
                            applied,
                            applied_term,
                            config.clone(),
                            previous.as_ref(),
                        ))
                        .await;
                    match recorded {
                        // No `RaftSnapshot` here. That event says a snapshot was
                        // taken or installed, and this is neither; the transition
                        // this record is part of is the compaction, which the core
                        // traces as `RaftCompacted` once the prefix's deletes are
                        // durable, in the same order a take's is. A crash between
                        // the record and those deletes is reported where a take's
                        // crash window is, by the restatement at the next open.
                        // Tracing it as an installed snapshot would have quietly
                        // turned the sweep's count of `RaftSnapshot { taken: false }`
                        // from 19 359 into 75 742 over a thousand seeds, which is how
                        // this was found. That count is no longer called "installed"
                        // either way: every replica that compacts re-states its prefix
                        // at each later open, so the sweep splits the population into
                        // installs and re-statements (`sim::raft::Report`).
                        Ok(()) => inbox.push(Event::Recorded {
                            index: applied,
                            term: applied_term,
                        }),
                        Err(_) => inbox.push(Event::TakeFailed),
                    }
                    continue;
                }
                Job::Take | Job::Retake => {
                    // A snapshot at the applied index: the record first, synced,
                    // then the checkpoint, and no apply lands in between, so the
                    // record is exact (RAFT.md §1, D-036).
                    if applied == 0 {
                        inbox.push(Event::TakeFailed);
                        continue;
                    }
                    let taken = if variants.contains(Variant::SharedSnapshotDir) {
                        // As built: one directory per index, swept and rewritten
                        // by every take at it, under any stream reading it.
                        let dir = snapshot::checkpoint_dir(&engine_dir, applied);
                        snapshot::take(&env, &store, &dir, applied, applied_term, &config).await
                    } else {
                        // D-043: a take at the index the record already
                        // names is a second version of the same state. Unless a
                        // fresh one was asked for, the recorded version answers
                        // when it is complete; a fresh take, or one at a new
                        // index, goes to its own numbered directory.
                        let recorded = match store.snapshot_record().await {
                            Ok(Some(record))
                                if !fresh && record.taken && record.last_index == applied =>
                            {
                                match snapshot::checkpoint_complete(&env, Path::new(&record.dir))
                                    .await
                                {
                                    Ok(true) => Some(record),
                                    _ => None,
                                }
                            }
                            _ => None,
                        };
                        if let Some(record) = recorded {
                            env.trace_decided(
                                taken_job,
                                TraceEvent::RaftSnapshotReused {
                                    server,
                                    range: SINGLE_GROUP,
                                    last_index: record.last_index,
                                    take: record.take,
                                },
                            );
                            inbox.push(Event::Taken {
                                index: record.last_index,
                                term: record.last_term,
                            });
                            continue;
                        }
                        snapshot::take_version(
                            &env,
                            &store,
                            &engine_dir,
                            applied,
                            applied_term,
                            &config,
                        )
                        .await
                        .map(|_| ())
                    };
                    match taken {
                        Ok(()) => {
                            env.trace_decided(
                                taken_job,
                                TraceEvent::RaftSnapshot {
                                    server,
                                    range: SINGLE_GROUP,
                                    last_index: applied,
                                    last_term: applied_term,
                                    taken: true,
                                },
                            );
                            inbox.push(Event::Taken {
                                index: applied,
                                term: applied_term,
                            });
                        }
                        Err(_) => inbox.push(Event::TakeFailed),
                    }
                    continue;
                }
            };
            for entry in entries {
                if entry.index <= applied {
                    continue;
                }
                if entry.index != applied + 1 {
                    env.trace(TraceEvent::RaftServerFailed {
                        server,
                        reason: format!("apply of {} after {applied}", entry.index),
                    });
                    return;
                }
                // D-047: the apply is decided when the task takes the
                // entry; it is traced once its batch, with the applied index, is
                // durable.
                let took = env.decision();
                let command = match &entry.payload {
                    Payload::Command(bytes) => Command::decode(bytes.clone()).ok(),
                    Payload::Noop | Payload::Config(_) => None,
                };
                let outcome = match apply_command(&store, entry.index, command.as_ref()).await {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        env.trace(TraceEvent::RaftServerFailed {
                            server,
                            reason: error.to_string(),
                        });
                        return;
                    }
                };
                applied = entry.index;
                applied_term = entry.term;
                if let Payload::Config(new_config) = &entry.payload {
                    config = new_config.clone();
                }
                // PROPOSED(D-069): `key` and `effect` (SHARD.md §8). A client
                // command executed in its range is `applied` whatever it wrote —
                // a `Cas` whose compare failed writes nothing and is still
                // `applied` — and a no-op or a configuration entry is `none`.
                // The other five effects of §8's table belong to a command
                // refused at apply (§3) and to the range commands of §5 and §6,
                // neither of which exists yet: no apply here can produce them.
                let (key, effect) = match command.as_ref().and_then(Command::key) {
                    Some(key) => (Some(key.clone()), ApplyEffect::Applied),
                    // A no-op, a configuration entry, a command that names no key
                    // — `Transfer` and `Change` are never entries — or one the
                    // state machine could not decode, which wrote nothing and
                    // which no client of this workspace encodes.
                    None => (None, ApplyEffect::None),
                };
                env.trace_decided(
                    took,
                    TraceEvent::RaftApply {
                        server,
                        range: SINGLE_GROUP,
                        index: entry.index,
                        entry_term: entry.term,
                        hash: entry.payload.hash(),
                        key,
                        effect,
                    },
                );
                let waiting = lock_pending(&pending).remove(&entry.index);
                if let Some(waiting) = waiting
                    && waiting.term == entry.term
                {
                    let response = Response {
                        client: waiting.client,
                        seq: waiting.seq,
                        reply: Reply::Outcome(outcome),
                    };
                    let _ = sock.send(waiting.from, response.encode()).await;
                }
                inbox.push(Event::Applied(entry.index));
            }
        }
        inbox.push(Event::ApplyClosed);
    });
}

/// One outbound stream the `snapshot` task drives: the sender's bookkeeping plus
/// the retry state.
struct Outbound {
    sender: Sender,
    deadline: Instant,
    resends: u32,
    restarts: u32,
    /// The furthest point any acknowledgement has taken this stream, as (file
    /// position, offset): an acknowledgement past it is re-seed progress. A
    /// restart from the first byte does not lower it, so ground covered again is
    /// not progress until the stream passes where it had been (D-049).
    furthest: (usize, u64),
}

/// How many times one chunk is resent before the stream is given up.
const CHUNK_RESENDS: u32 = 8;
/// How many times a stream starts over before the checkpoint is retaken.
const STREAM_RESTARTS: u32 = 2;

/// The `snapshot` task's outbound side (D-043): a stream per
/// designated follower, each pinned to the checkpoint version it opened, with a
/// reader count per version so the sweep leaves what a stream reads; and, under
/// [`Variant::SharedSnapshotDir`], the server as built — one stream at a time,
/// every other designated follower queued behind it in `backlog`.
#[derive(Default)]
struct Streams {
    outbound: BTreeMap<ServerId, Outbound>,
    readers: BTreeMap<PathBuf, usize>,
    backlog: Vec<(ServerId, Index, Term)>,
}

impl Streams {
    /// The earliest chunk deadline among the streams running.
    fn deadline(&self) -> Option<Instant> {
        self.outbound.values().map(|out| out.deadline).min()
    }

    /// The followers whose outstanding chunk timed out by `now`.
    fn due(&self, now: Instant) -> Vec<ServerId> {
        self.outbound
            .iter()
            .filter(|(_, out)| out.deadline <= now)
            .map(|(&to, _)| to)
            .collect()
    }

    /// Whether `to` is being streamed to, or waits to be.
    fn has(&self, to: ServerId) -> bool {
        self.outbound.contains_key(&to) || self.backlog.iter().any(|(s, _, _)| *s == to)
    }

    /// Adds a stream, pinning its version; returns how many streams run now.
    fn open(&mut self, out: Outbound) -> usize {
        *self
            .readers
            .entry(out.sender.dir().to_path_buf())
            .or_default() += 1;
        self.outbound.insert(out.sender.to, out);
        self.outbound.len()
    }

    /// Ends the stream to `to`, releasing its version.
    fn end(&mut self, to: ServerId) {
        let Some(out) = self.outbound.remove(&to) else {
            return;
        };
        let dir = out.sender.dir().to_path_buf();
        if let Some(count) = self.readers.get_mut(&dir) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.readers.remove(&dir);
            }
        }
    }
}

/// What the `snapshot` task's stream helpers need of its world (D-043).
struct Streamer<E: Environment> {
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    addrs: Arc<BTreeMap<ServerId, SocketAddr>>,
    store: Arc<RaftStore<E>>,
    config: RaftConfig,
    engine_dir: PathBuf,
    inbox: Queue<Event>,
    chunk_timeout: Duration,
}

impl<E: Environment> Streamer<E> {
    /// Opens a stream to `to` of the checkpoint at (`index`, `term`) and sends
    /// its first chunk ([`start_stream`]), pinning the version it opened, and
    /// traces how many streams now run.
    async fn open(&self, streams: &mut Streams, to: ServerId, index: Index, term: Term) {
        // D-047: the stream is decided when the task acts on the core's
        // install; the lookups and the first chunk that follow decide only whether
        // there is a checkpoint to stream.
        let decided = self.env.decision();
        if let Some(out) = start_stream(self, to, index, term).await {
            let running = streams.open(out);
            self.env.trace_decided(
                decided,
                TraceEvent::RaftSnapshotStreams {
                    server: self.id.0,
                    range: SINGLE_GROUP,
                    to: to.0,
                    streams: running as u64,
                },
            );
        }
    }

    /// A stream ended: the versions nothing reads any more are swept; under the
    /// shared-directory variant the next queued follower's stream starts
    /// instead, as built.
    async fn ended(&self, streams: &mut Streams) {
        if self.config.variants.contains(Variant::SharedSnapshotDir) {
            if streams.outbound.is_empty()
                && let Some((to, index, term)) = streams.backlog.pop()
            {
                self.open(streams, to, index, term).await;
            }
        } else {
            self.sweep(streams).await;
        }
    }

    /// Deletes the versions that are neither the record's nor read by a stream,
    /// tracing each ([`sweep_versions`]).
    async fn sweep(&self, streams: &Streams) {
        sweep_versions(
            &self.env,
            self.id,
            &self.store,
            &self.engine_dir,
            &streams.readers,
        )
        .await;
    }
}

/// Deletes the checkpoint versions under `engine_dir` that are neither the
/// record's nor read by a stream — `readers` counts the streams on each — and
/// traces each as [`TraceEvent::RaftSnapshotDeleted`] (D-043). A sweep
/// that fails leaves its versions for the next.
async fn sweep_versions<E: Environment>(
    env: &E,
    id: ServerId,
    store: &RaftStore<E>,
    engine_dir: &Path,
    readers: &BTreeMap<PathBuf, usize>,
) {
    if let Ok(deleted) = snapshot::sweep_versions(env, store, engine_dir, readers).await {
        for (last_index, take) in deleted {
            // D-047, an open point: which versions go is decided inside
            // the sweep, after its listing and its read of the record, so no stamp
            // taken out here could be the decision's own; the deletion is traced as
            // decided when it is recorded, which no earlier time is provably.
            env.trace(TraceEvent::RaftSnapshotDeleted {
                server: id.0,
                range: SINGLE_GROUP,
                last_index,
                take,
            });
        }
    }
}

/// The `snapshot` task (RAFT.md §3): streams checkpoints to followers on the
/// leader's behalf, one chunk outstanding per stream and resumed from the last
/// acknowledged offset after loss; assembles and verifies arriving streams on a
/// follower; the only task that touches checkpoint directories. One stream per
/// designated follower, serviced as its acknowledgements and timeouts come,
/// never behind another's (D-043).
#[expect(clippy::too_many_arguments, reason = "the task's whole world")]
async fn snapshot_task<E: Environment>(
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    addrs: Arc<BTreeMap<ServerId, SocketAddr>>,
    store: Arc<RaftStore<E>>,
    config: RaftConfig,
    engine_dir: PathBuf,
    inbox: Queue<Event>,
    snaps: Queue<Snap>,
    stream_acks: StreamAcks,
    prefix: KeyPrefix,
) {
    let server = id.0;
    let chunk_timeout = Duration::from_nanos(config.tick_nanos * config.election_ticks.0 / 2);
    let shared = config.variants.contains(Variant::SharedSnapshotDir);
    let mut assembler = Assembler::new(env.clone(), &engine_dir, config.variants, prefix);
    let mut staged: Option<Staged> = None;
    let streamer = Streamer {
        env: env.clone(),
        id,
        sock: sock.clone(),
        addrs: addrs.clone(),
        store: store.clone(),
        config: config.clone(),
        engine_dir: engine_dir.clone(),
        inbox: inbox.clone(),
        chunk_timeout,
    };
    let mut streams = Streams::default();
    loop {
        let item = if let Some(deadline) = streams.deadline() {
            let pop = pin!(snaps.pop());
            let timer = pin!(env.clock().sleep_until(deadline));
            match race(&env, pop, timer).await {
                Either::Left(Some(item)) => Some(item),
                Either::Left(None) => return,
                Either::Right(()) => None,
            }
        } else {
            match snaps.pop().await {
                Some(item) => Some(item),
                None => return,
            }
        };
        let Some(item) = item else {
            // A chunk timed out: resend from where its receiver last stood, the
            // resumption of RAFT.md §1, or give that stream up. Every stream
            // past its deadline is serviced here, none behind another's.
            let now = env.clock().now();
            // D-047: every resend of this pass is decided here, though
            // each after the first is traced behind the sends before it.
            let pass = env.decision();
            let mut ended = false;
            for to in streams.due(now) {
                let out = streams.outbound.get_mut(&to).expect("a due stream");
                out.resends += 1;
                if out.resends > CHUNK_RESENDS {
                    inbox.push(Event::StreamFailed { to, retake: false });
                    streams.end(to);
                    ended = true;
                } else {
                    env.trace_decided(
                        pass,
                        TraceEvent::RaftSnapshotResumed {
                            server,
                            range: SINGLE_GROUP,
                            to: to.0,
                            offset: out.sender.position_offset(config.snapshot_chunk),
                        },
                    );
                    send_chunk(&env, &sock, &addrs, id, &config, out, chunk_timeout).await;
                }
            }
            if ended {
                streamer.ended(&mut streams).await;
            }
            continue;
        };
        match item {
            Snap::Action(SnapshotAction::Take | SnapshotAction::Record) => {
                // Takes and a follower's records run in the apply task; the
                // server routes them there and neither reaches this one.
            }
            Snap::Taken => {
                if !shared {
                    streamer.sweep(&streams).await;
                }
            }
            Snap::Action(SnapshotAction::Install { to, index, term }) => {
                if streams.has(to) {
                    continue;
                }
                if shared && !streams.outbound.is_empty() {
                    // As built: one stream per leader, the rest queued behind it.
                    streams.backlog.push((to, index, term));
                    continue;
                }
                streamer.open(&mut streams, to, index, term).await;
            }
            Snap::Chunk {
                from,
                message:
                    Message::InstallSnapshot {
                        term,
                        last_index,
                        last_term,
                        file,
                        offset,
                        total,
                        done,
                        data,
                    },
            } => {
                if term < store.term() {
                    // A stale leader's stream: let it time out.
                    continue;
                }
                if store.applied() >= last_index {
                    // Everything the snapshot carries is already here: say so.
                    let message = snapshot::installed(store.term(), (last_index, last_term));
                    send_message(&env, &sock, &addrs, id, from, message, store.incarnation()).await;
                    continue;
                }
                if staged.is_some() {
                    // Mid-decision: the sender retries once the switch settles.
                    continue;
                }
                let fed = assembler
                    .on_chunk(
                        from, term, last_index, last_term, file, offset, total, done, data,
                    )
                    .await;
                match fed {
                    Ok(Feed::Ack { file, offset }) => {
                        let message =
                            snapshot::ack(store.term(), (last_index, last_term), file, offset);
                        send_message(&env, &sock, &addrs, id, from, message, store.incarnation())
                            .await;
                    }
                    Ok(Feed::Restart) => {
                        let message = snapshot::start_over(store.term(), (last_index, last_term));
                        send_message(&env, &sock, &addrs, id, from, message, store.incarnation())
                            .await;
                    }
                    Ok(Feed::Staged(ready)) => {
                        inbox.push(Event::SnapshotReady {
                            last_index: ready.last_index,
                            last_term: ready.last_term,
                        });
                        staged = Some(ready);
                    }
                    Err(_) => {
                        assembler.abandon().await;
                        let message = snapshot::start_over(store.term(), (last_index, last_term));
                        send_message(&env, &sock, &addrs, id, from, message, store.incarnation())
                            .await;
                    }
                }
            }
            Snap::Finish(repair) => {
                // D-047: the install is decided when the task takes the
                // `raft` loop's repair; the staged store's repair and its CURRENT
                // are durable when it is traced.
                let installing = env.decision();
                let Some(ready) = staged.take() else { continue };
                let identity = (ready.last_index, ready.last_term);
                let to = ready.from;
                match assembler.finish(&ready, &repair).await {
                    Ok(()) => {
                        // The install exists (RAFT.md §1): say so, and let the
                        // server switch. The restatement on the adopted store
                        // re-traces the snapshot as the disk's picture.
                        env.trace_decided(
                            installing,
                            TraceEvent::RaftSnapshot {
                                server,
                                range: SINGLE_GROUP,
                                last_index: identity.0,
                                last_term: identity.1,
                                taken: false,
                            },
                        );
                        let config = &ready.config;
                        env.trace_decided(
                            installing,
                            TraceEvent::RaftConfig {
                                server,
                                range: SINGLE_GROUP,
                                index: identity.0,
                                old: config.voters.iter().map(|s| s.0).collect(),
                                new: config
                                    .new_voters
                                    .as_ref()
                                    .unwrap_or(&config.voters)
                                    .iter()
                                    .map(|s| s.0)
                                    .collect(),
                                joint: config.new_voters.is_some(),
                                learners: config.learners.iter().map(|s| s.0).collect(),
                            },
                        );
                        let message = snapshot::installed(repair.term, identity);
                        send_message(&env, &sock, &addrs, id, to, message, store.incarnation())
                            .await;
                        inbox.push(Event::SnapshotFinished { reinstall: true });
                    }
                    Err(_) => {
                        assembler.abandon().await;
                        let message = snapshot::start_over(repair.term, identity);
                        send_message(&env, &sock, &addrs, id, to, message, store.incarnation())
                            .await;
                        inbox.push(Event::SnapshotFinished { reinstall: false });
                    }
                }
            }
            Snap::Skip => {
                let Some(ready) = staged.take() else { continue };
                let message =
                    snapshot::installed(store.term(), (ready.last_index, ready.last_term));
                send_message(
                    &env,
                    &sock,
                    &addrs,
                    id,
                    ready.from,
                    message,
                    store.incarnation(),
                )
                .await;
                assembler.abandon().await;
                inbox.push(Event::SnapshotFinished { reinstall: false });
            }
            Snap::Ack {
                from,
                message:
                    Message::InstallSnapshotResponse {
                        last_index,
                        last_term,
                        file,
                        offset,
                        status,
                        incarnation,
                        ..
                    },
            } => {
                // The stream to this follower, if one runs (D-043).
                let Some(out) = streams.outbound.get_mut(&from) else {
                    continue;
                };
                if out.sender.to != from
                    || out.sender.last_index != last_index
                    || out.sender.last_term != last_term
                {
                    continue;
                }
                let mut ended = false;
                match status {
                    SnapshotStatus::Installed => {
                        inbox.push(Event::StreamDone {
                            to: from,
                            index: last_index,
                            incarnation,
                        });
                        ended = true;
                    }
                    SnapshotStatus::Restart => {
                        out.restarts += 1;
                        if out.restarts > STREAM_RESTARTS {
                            inbox.push(Event::StreamFailed {
                                to: from,
                                retake: true,
                            });
                            ended = true;
                        } else {
                            out.sender.restart();
                            out.resends = 0;
                            send_chunk(&env, &sock, &addrs, id, &config, out, chunk_timeout).await;
                        }
                    }
                    SnapshotStatus::Waiting => {
                        // A receiver with a cap on what it assembles at once, which
                        // this server is not talking to today: the one-group receiver
                        // holds one stream and has no cap (RAFT.md:214-218), so
                        // nothing on this path answers a wait. The rule is the node's
                        // (D-087) and is written here so that hearing one is a wait
                        // rather than a mystery: nothing is restarted, nothing is
                        // reset, and the chunk outstanding falls due on the ordinary
                        // resend timer, which is what bounds it.
                        // PROPOSED(D-087): a cap-wait is not a start-over.
                        out.deadline = env.clock().now() + chunk_timeout;
                    }
                    SnapshotStatus::More => {
                        if out.sender.on_more(&file, offset) {
                            env.trace(TraceEvent::RaftSnapshotResumed {
                                server,
                                range: SINGLE_GROUP,
                                to: from.0,
                                offset,
                            });
                        }
                        // Re-seed progress for the core's check quorum: only an
                        // acknowledgement past the furthest point this stream had
                        // reached, never a duplicate, a resumption or ground a
                        // restart covers again.
                        // D-049: a refused follower counts for check quorum only
                        // while its re-seed stream progresses.
                        let reached = out.sender.acknowledged();
                        if reached > out.furthest {
                            out.furthest = reached;
                            stream_acks
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .insert(from);
                        }
                        out.resends = 0;
                        if out.sender.at_end() {
                            out.deadline = env.clock().now() + chunk_timeout;
                        } else {
                            send_chunk(&env, &sock, &addrs, id, &config, out, chunk_timeout).await;
                        }
                    }
                }
                if ended {
                    streams.end(from);
                    streamer.ended(&mut streams).await;
                }
            }
            Snap::Chunk { .. } | Snap::Ack { .. } => {}
        }
    }
}

/// Opens a stream of the checkpoint at (`index`, `term`) to `to` and sends its
/// first chunk; reports a failure to the core instead when there is no
/// streamable checkpoint. The correct server opens the newest *complete* version
/// of that index, [`snapshot::find_version`], and is pinned to it for the
/// stream's life (D-043); [`Variant::SharedSnapshotDir`] opens whatever
/// directory the record names, as built — which a take in flight may still be
/// writing, since the record precedes the checkpoint (D-036).
async fn start_stream<E: Environment>(
    task: &Streamer<E>,
    to: ServerId,
    index: Index,
    term: Term,
) -> Option<Outbound> {
    let env = &task.env;
    let (dir, last_index, last_term) = if task.config.variants.contains(Variant::SharedSnapshotDir)
    {
        match task.store.snapshot_record().await {
            Ok(Some(record)) if record.taken && !record.dir.is_empty() => (
                PathBuf::from(&record.dir),
                record.last_index,
                record.last_term,
            ),
            _ => {
                // No checkpoint of our own to stream: ask for a fresh take.
                task.inbox.push(Event::StreamFailed { to, retake: true });
                return None;
            }
        }
    } else {
        match snapshot::find_version(env, &task.engine_dir, index).await {
            Ok(Some((dir, _))) => (dir, index, term),
            _ => {
                // No complete version of that index: the take is still in
                // flight, a crash cut it short, or the record is an install's.
                // Ask for a take; the `raft` loop waits out one in flight.
                task.inbox.push(Event::StreamFailed { to, retake: true });
                return None;
            }
        }
    };
    let sender = Sender::open(env, &dir, to, last_index, last_term, task.store.term()).await;
    match sender {
        Ok(sender) => {
            let mut out = Outbound {
                sender,
                deadline: env.clock().now() + task.chunk_timeout,
                resends: 0,
                restarts: 0,
                furthest: (0, 0),
            };
            send_chunk(
                env,
                &task.sock,
                &task.addrs,
                task.id,
                &task.config,
                &mut out,
                task.chunk_timeout,
            )
            .await;
            Some(out)
        }
        Err(_) => {
            // The recorded checkpoint is unusable (a crash between the record
            // and the checkpoint leaves one): take a fresh one.
            task.inbox.push(Event::StreamFailed { to, retake: true });
            None
        }
    }
}

/// Sends the chunk at the stream's position and arms its timeout.
async fn send_chunk<E: Environment>(
    env: &E,
    sock: &Arc<<E::Net as Network>::Socket>,
    addrs: &Arc<BTreeMap<ServerId, SocketAddr>>,
    id: ServerId,
    config: &RaftConfig,
    out: &mut Outbound,
    chunk_timeout: Duration,
) {
    if let Ok(message) = out.sender.chunk(env, config.snapshot_chunk).await {
        // A chunk is no response: nothing to stamp.
        send_message(env, sock, addrs, id, out.sender.to, message, 0).await;
    }
    out.deadline = env.clock().now() + chunk_timeout;
}

/// Re-seed mode (RAFT.md §3): the refused server's loop. It answers every
/// AppendEntries with a rejection whose hint asks from index 1 — the ask no
/// follower with a log makes, so the leader designates it snapshot-fed — and
/// assembles the stream that follows. It grants nothing and answers nothing else.
/// When the install completes, the staged store is a complete install carrying the
/// quarantine flag (D-035) and a fresh store incarnation
/// (D-042), and the caller adopts it. Its answers carry incarnation 0
/// until then: a refused server has no store.
#[expect(clippy::too_many_arguments, reason = "re-seed mode names its world")]
async fn reseed<E: Environment>(
    env: &E,
    id: ServerId,
    sock: &Arc<<E::Net as Network>::Socket>,
    addrs: &Arc<BTreeMap<ServerId, SocketAddr>>,
    raft: &RaftConfig,
    engine_dir: &Path,
    inbox: &Queue<Event>,
    prefix: KeyPrefix,
) -> Next {
    let mut assembler = Assembler::new(env.clone(), engine_dir, raft.variants, prefix);
    loop {
        let Some(event) = inbox.pop().await else {
            return Next::Closed;
        };
        let Event::Message { from, message, .. } = event else {
            continue;
        };
        match message {
            Message::AppendEntries {
                term, prev_index, ..
            } => {
                // The re-seed ask: reject with a hint of 1, echo 0 so no lease
                // promise is ever measured from this server (RAFT.md §3), and
                // incarnation 0, no store, so a leader that matched entries on
                // the lost one forgets them (D-042).
                let message = Message::AppendEntriesResponse {
                    term,
                    success: false,
                    prev_index,
                    match_index: 0,
                    hint: 1,
                    echo: 0,
                    local: 0,
                    incarnation: 0,
                };
                send_message(env, sock, addrs, id, from, message, 0).await;
            }
            Message::InstallSnapshot {
                term,
                last_index,
                last_term,
                file,
                offset,
                total,
                done,
                data,
            } => {
                let fed = assembler
                    .on_chunk(
                        from, term, last_index, last_term, file, offset, total, done, data,
                    )
                    .await;
                match fed {
                    Ok(Feed::Ack { file, offset }) => {
                        let message = snapshot::ack(term, (last_index, last_term), file, offset);
                        send_message(env, sock, addrs, id, from, message, 0).await;
                    }
                    Ok(Feed::Restart) => {
                        let message = snapshot::start_over(term, (last_index, last_term));
                        send_message(env, sock, addrs, id, from, message, 0).await;
                    }
                    Ok(Feed::Staged(ready)) => {
                        // D-047: the re-seed's install is decided once
                        // the whole stream is staged; the repair and the staged
                        // CURRENT follow before it is traced.
                        let installing = env.decision();
                        // No store survived, so there is nothing of our own to
                        // carry over: term from the stream, no vote, no tail,
                        // and the quarantine flag for the vote the lost state
                        // may have held (D-035). The store's
                        // incarnation is drawn afresh: the number the lost
                        // store carried is gone with it, and what matters is
                        // that no leader has recorded this one against a match
                        // index the rebuilt log cannot honour. Never the first
                        // incarnation, which every fresh store starts at
                        // (D-042).
                        let incarnation = env.rng().next_u64().max(FIRST_INCARNATION + 1);
                        let repair = Repair {
                            term: ready.term,
                            vote: None,
                            tail: Vec::new(),
                            quarantined: true,
                            incarnation,
                        };
                        let identity = (ready.last_index, ready.last_term);
                        let sender = ready.from;
                        match assembler.finish(&ready, &repair).await {
                            Ok(()) => {
                                env.trace_decided(
                                    installing,
                                    TraceEvent::RaftSnapshot {
                                        server: id.0,
                                        range: SINGLE_GROUP,
                                        last_index: identity.0,
                                        last_term: identity.1,
                                        taken: false,
                                    },
                                );
                                // The answer names the store this server runs
                                // on from here: the leader records it and
                                // forgets the old one's progress at once.
                                let message = snapshot::installed(repair.term, identity);
                                send_message(env, sock, addrs, id, sender, message, incarnation)
                                    .await;
                                return Next::Reinstall;
                            }
                            Err(_) => {
                                assembler.abandon().await;
                                let message = snapshot::start_over(repair.term, identity);
                                send_message(env, sock, addrs, id, sender, message, 0).await;
                            }
                        }
                    }
                    Err(_) => {
                        assembler.abandon().await;
                        let message = snapshot::start_over(term, (last_index, last_term));
                        send_message(env, sock, addrs, id, from, message, 0).await;
                    }
                }
            }
            _ => {}
        }
    }
}

/// Puts a message on the inbox, making room by the policy in the module
/// documentation when the inbox is full.
fn admit<E: Environment>(
    env: &E,
    server: u64,
    inbox: &Queue<Event>,
    capacity: usize,
    frame: Frame,
    // PROPOSED(D-050): when the `net` task received the frame, carried on the
    // inbox event to the step that takes it.
    received: Decision,
) {
    let is_message = |event: &Event| matches!(event, Event::Message { .. });
    let is_heartbeat = |message: &Message| matches!(message, Message::AppendEntries { entries, .. } if entries.is_empty());
    let carries_entries = |message: &Message| {
        matches!(message, Message::AppendEntries { entries, .. } if !entries.is_empty())
            || matches!(message, Message::InstallSnapshot { .. })
    };
    if inbox.count(is_message) >= capacity {
        let dropped = |kind: &'static str| {
            env.trace(TraceEvent::RaftInboxDropped {
                server,
                range: SINGLE_GROUP,
                kind,
            });
        };
        let (from, kind) = (frame.from, frame.message.kind());
        let victim = inbox
            .remove_first(
                |event| matches!(event, Event::Message { message, .. } if is_heartbeat(message)),
            )
            .or_else(|| {
                inbox.remove_first(|event| {
                    matches!(event, Event::Message { from: f, message, .. }
                        if *f == from && message.kind() == kind && !carries_entries(message))
                })
            });
        match victim {
            Some(Event::Message { message, .. }) => dropped(message.kind()),
            Some(_) => unreachable!("only messages are removed"),
            None if carries_entries(&frame.message) => {}
            None => {
                dropped(kind);
                return;
            }
        }
    }
    inbox.push(Event::Message {
        from: frame.from,
        message: frame.message,
        received,
    });
}

/// The `raft` task's side of the server: what executing the core's outputs needs.
struct Server<E: Environment> {
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    addrs: Arc<BTreeMap<ServerId, SocketAddr>>,
    store: Arc<RaftStore<E>>,
    engine_dir: PathBuf,
    jobs: Queue<Job>,
    snaps: Queue<Snap>,
    pending: Pending,
    variants: Variants,
    /// The highest index handed to the `apply` task.
    apply_sent: Index,
    /// Requests this server proposed, by client and sequence number, with the index
    /// and term their entry took: a duplicate of one still in the log is not
    /// proposed again.
    proposed: BTreeMap<(u64, u64), (Index, Term)>,
    /// Reads waiting on the core, by the id handed to it.
    reads: BTreeMap<u64, (SocketAddr, Request)>,
    next_read: u64,
    /// A take is with the `apply` task: a stream that finds no usable version
    /// meanwhile waits for it rather than asking for another (D-043).
    take_in_flight: bool,
    /// The recorded checkpoint was found unusable: the next take the core asks
    /// for is a [`Job::Retake`], a fresh version even at the record's index
    /// (D-043).
    fresh_take: bool,
}

/// How many proposals a server remembers against duplicates.
const PROPOSED_REMEMBERED: usize = 4096;

impl<E: Environment> Server<E> {
    /// Sends a message, stamping the clock where the lease reads it and the
    /// store's incarnation on a response (D-042).
    async fn send(&self, to: ServerId, message: Message) {
        send_message(
            &self.env,
            &self.sock,
            &self.addrs,
            self.id,
            to,
            message,
            self.store.incarnation(),
        )
        .await;
    }

    /// Answers the client that asked for read `id`, if it is still waiting.
    async fn answer_read(&mut self, id: u64, reply: Reply) {
        if let Some((from, request)) = self.reads.remove(&id) {
            let response = Response {
                client: request.client,
                seq: request.seq,
                reply,
            };
            let _ = self.sock.send(from, response.encode()).await;
        }
    }

    /// Hands the `apply` task the entries after `apply_sent` through `through`.
    fn apply_through(&mut self, core: &Raft, through: Index) {
        if through <= self.apply_sent {
            return;
        }
        let entries: Vec<Entry> = (self.apply_sent + 1..=through)
            .filter_map(|index| core.entry(index).cloned())
            .collect();
        self.apply_sent = through;
        self.jobs.push(Job::Entries(entries));
    }

    /// Executes a step's outputs in order: the persist first, awaited, then the
    /// sends that depend on it, the apply, the trace. Under
    /// [`Variant::SendBeforePersist`] the sends go first; the trace events still
    /// follow the persist, so the trace says what is durable.
    ///
    /// The trace events carry `decided`, the stamp taken at the step that produced
    /// them (D-047): each record's own time is when it became durable, and
    /// the stamp is when the step took it. A term change also carries `received`,
    /// when the peer's message the step took reached the server, if it took one
    /// (D-050).
    async fn execute(
        &mut self,
        core: &Raft,
        outputs: Vec<Output>,
        decided: Decision,
        received: Option<Decision>,
    ) -> io::Result<()> {
        let send_first = self.variants.contains(Variant::SendBeforePersist);
        if send_first {
            for output in &outputs {
                if let Output::Send { to, message } = output {
                    self.send(*to, message.clone()).await;
                }
            }
        }
        for output in outputs {
            match output {
                Output::Persist(persist) => {
                    if let Err(error) = self.store.persist(&persist).await {
                        self.env.trace(TraceEvent::RaftServerFailed {
                            server: self.id.0,
                            reason: error.to_string(),
                        });
                        return Err(error);
                    }
                    if let Some(from) = persist.truncate_from {
                        lock_pending(&self.pending).retain(|&index, _| index < from);
                    }
                    if self.variants.contains(Variant::ApplyBeforeCommit) {
                        let last = persist.append.last().map_or(0, |entry| entry.index);
                        let entries: Vec<Entry> = persist
                            .append
                            .into_iter()
                            .filter(|entry| entry.index > self.apply_sent)
                            .collect();
                        if !entries.is_empty() {
                            self.apply_sent = last;
                            self.jobs.push(Job::Entries(entries));
                        }
                    }
                }
                Output::Send { to, message } => {
                    if !send_first {
                        self.send(to, message).await;
                    }
                }
                Output::Apply { through } => self.apply_through(core, through),
                Output::Rejected { .. } => {}
                // PROPOSED(D-069): the read is served here and traced here
                // (SHARD.md §8). The value and the applied index come from one
                // engine version, so the record says which state the client was
                // answered from: a check can place the read in the log's order,
                // and the descriptor a later stage reads at that version is the
                // one the value was read under (§11, raft 15).
                Output::ReadReady { id, index, lease } => {
                    let key = match self.reads.get(&id) {
                        Some((_, request)) => match request.command.key() {
                            Some(key) => key.clone(),
                            None => continue,
                        },
                        None => continue,
                    };
                    let version = self.store.engine().snapshot();
                    let value = match self.store.engine().get_at(&user_key(&key), &version).await {
                        Ok(value) => value,
                        Err(error) => {
                            self.env.trace(TraceEvent::RaftServerFailed {
                                server: self.id.0,
                                reason: error.to_string(),
                            });
                            return Err(error);
                        }
                    };
                    let served_at = match self.store.applied_at(&version).await {
                        Ok(applied) => applied,
                        Err(error) => {
                            self.env.trace(TraceEvent::RaftServerFailed {
                                server: self.id.0,
                                reason: error.to_string(),
                            });
                            return Err(error);
                        }
                    };
                    drop(version);
                    // D-047: decided when the core confirmed the read, recorded
                    // now, when the server answered it from that version.
                    self.env.trace_decided(
                        decided,
                        TraceEvent::RaftRead {
                            server: self.id.0,
                            range: SINGLE_GROUP,
                            index,
                            lease,
                            key,
                            applied: served_at,
                        },
                    );
                    self.answer_read(id, Reply::Outcome(Outcome::Value(value)))
                        .await;
                }
                Output::ReadDropped { id } => {
                    self.answer_read(id, Reply::NotLeader { leader: None })
                        .await;
                }
                Output::Snapshot(SnapshotAction::Record) => {
                    // A follower's compaction point: the `apply` task writes the
                    // record, nothing takes a checkpoint (D-065). It occupies the
                    // same task a take would, so it sets the same in-flight flag.
                    // PROPOSED(D-078): a follower compacts its log to its own
                    // applied index.
                    self.take_in_flight = true;
                    self.jobs.push(Job::Record);
                }
                Output::Snapshot(SnapshotAction::Take) => {
                    // A fresh version when the recorded one was found unusable,
                    // the recorded one otherwise at its own index (D-043).
                    let job = if self.fresh_take {
                        Job::Retake
                    } else {
                        Job::Take
                    };
                    self.fresh_take = false;
                    self.take_in_flight = true;
                    self.jobs.push(job);
                }
                Output::Snapshot(action) => self.snaps.push(Snap::Action(action)),
                // D-047: decided at the step, traced now.
                Output::Trace(mut event) => {
                    // PROPOSED(D-050): a term change says when the message
                    // its step took was received.
                    if let TraceEvent::RaftTerm { received: at, .. } = &mut event {
                        *at = received;
                    }
                    self.env.trace_decided(decided, event);
                }
            }
        }
        Ok(())
    }
}

/// The snapshot record a replica's compaction writes (D-065).
///
/// It stands in for the prefix at `applied` with **no checkpoint under it**: the
/// shape an install's repair already writes (`snapshot.rs`, `Repair`), which is
/// what makes a server that later has to stream find no complete version at that
/// index and ask for a take instead (`start_stream`). Three things are load-bearing
/// and each is asserted below:
///
/// * `taken` is false. It is not a take: no checkpoint was written, and a `true`
///   here would be restated as one at every later open, unpairing the sweep's
///   takes from the checkpoints the fold finds them by — the D-060 failure the
///   restatement's counters exist to catch.
/// * `dir` is empty, for the same reason: there is no directory to open.
/// * `take` is the store's counter carried forward, never reset, so a later take
///   still numbers its version directory past every one this store has made
///   (D-043). An install's repair writes 0 there because an install's versions
///   start over; a compaction's do not. It is read from `previous` — the record
///   the store holds — rather than passed in as a number, so that dropping it is
///   a change to this function and not an invisible one at the call site.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
fn compaction_record(
    applied: Index,
    applied_term: Term,
    config: Configuration,
    previous: Option<&SnapshotRecord>,
) -> SnapshotRecord {
    SnapshotRecord {
        last_index: applied,
        last_term: applied_term,
        config,
        dir: String::new(),
        taken: false,
        take: previous.map_or(0, |record| record.take),
    }
}

#[cfg(test)]
mod tests {
    use super::compaction_record;
    use crate::store::SnapshotRecord;
    use crate::types::{Configuration, ServerId};

    /// The pair for D-078's settled points 2 and 4, which nothing else in the tree
    /// reaches: the record's own fields. A sweep cannot see them — it sees the
    /// compaction, and a record written `taken: true` or with the counter reset
    /// still compacts the same log — so they are asserted here, directly.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    #[test]
    fn a_compactions_record_is_not_a_take_and_carries_the_counter_forward() {
        let config = Configuration::of(&[ServerId(1), ServerId(2), ServerId(3)]);
        // The record a store that has taken five checkpoints holds, exactly as
        // `snapshot_record` returns it: the counter, the directory of the newest
        // version and `taken`. What the compaction keeps of it is the counter.
        let previous = SnapshotRecord {
            last_index: 30,
            last_term: 6,
            config: config.clone(),
            dir: "/s/snap-30-5".to_owned(),
            taken: true,
            take: 5,
        };
        let record = compaction_record(41, 7, config.clone(), Some(&previous));
        assert_eq!(record.last_index, 41);
        assert_eq!(record.last_term, 7);
        assert_eq!(record.config, config);
        assert!(
            !record.taken,
            "a compaction takes no checkpoint, so its record is not a take: {record:?}"
        );
        assert!(
            record.dir.is_empty(),
            "a compaction writes no version directory: {record:?}"
        );
        assert_eq!(
            record.take, 5,
            "the store's take counter is carried forward, so the next take numbers \
             its directory past every one this store has made (D-043)"
        );
    }

    /// A store that has never taken one still records 0, and the next take is
    /// still the first: carrying forward is not the same as inventing a version.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    #[test]
    fn a_compactions_record_invents_no_version() {
        let record = compaction_record(1, 1, Configuration::of(&[ServerId(1)]), None);
        assert_eq!(record.take, 0);
        assert!(record.dir.is_empty());
        assert!(!record.taken);
    }
}

//! One server under the [`Environment`] (RAFT.md §3): the tasks that run a core.
//!
//! [`run`] binds the socket, spawns the `net` task, and then runs the server as a
//! sequence of *incarnations*, one per store the server runs on: at each start a
//! completed install at the staging path is adopted ([`crate::snapshot::adopt_staged`]),
//! the engine and store open, and the `raft` loop runs with the `apply` and
//! `snapshot` tasks beside it until the server stops or an installed snapshot
//! switches its store, which starts the next incarnation on the adopted state. The
//! socket and the inbox live across incarnations, so messages arriving during a
//! switch are queued, not lost.
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
//!   (PROPOSED(D-036)).
//! - `snapshot` streams snapshots to followers on the leader and assembles arriving
//!   ones into the staging directory on a follower; it is the only task that touches
//!   checkpoint directories. The final repair of a staged store needs the receiver's
//!   own hard state and log tail, so the `raft` loop quiesces the `apply` task,
//!   reads them, and hands the repair over; the completed install then retires this
//!   incarnation.
//!
//! A server whose store is refused ([`LostState`](crate::store::LostState)) traces
//! [`TraceEvent::RaftRefused`] and runs in *re-seed mode* (RAFT.md §3): its socket
//! stays bound but it participates in nothing — it grants no vote and no pre-vote
//! and answers no AppendEntries with content. It answers every AppendEntries with a
//! rejection whose hint asks from index 1, the ask no follower with a log makes, so
//! the leader designates it snapshot-fed and streams; once the install completes
//! the server runs on the re-seeded store, quarantined for good
//! (PROPOSED(D-035)): the lost state may have included its vote. Every
//! AppendEntries and InstallSnapshot response carries the store's incarnation
//! number, stamped on the way out like the clock — 0 while refused, a fresh one
//! on the re-seeded store — so a leader that matched entries on the lost store
//! forgets them rather than probing above a log that no longer has them
//! (PROPOSED(D-042)).
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

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::{Clock, Either, Environment, Instant, Network, Rng, Socket, TraceEvent, race};
use ananke_storage::{Engine, EngineConfig};

use crate::apply::{Command, Outcome, apply_command, user_key};
use crate::client::{self, Reply, Request, Response};
use crate::core::{Input, Output, Raft, RaftConfig, SnapshotAction, Variant};
use crate::message::{Frame, Message, SnapshotStatus};
use crate::queue::Queue;
use crate::snapshot::{self, Assembler, Feed, Repair, Sender, Staged};
use crate::store::{FIRST_INCARNATION, RaftStore, Recovered};
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
    Message { from: ServerId, message: Message },
    /// A client's request, with where to answer.
    Request { from: SocketAddr, request: Request },
    /// The `apply` task applied through this index.
    Applied(Index),
    /// The `apply` task completed a take: a checkpoint at `index`, whose entry
    /// has `term`, is on disk and recorded.
    Taken { index: Index, term: Term },
    /// The `apply` task could not complete a take.
    TakeFailed,
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
    /// Take a snapshot at the applied index, between applies (RAFT.md §1).
    Take,
}

/// What the `snapshot` task takes from its queue.
enum Snap {
    /// The core asks for a stream (a take goes to the `apply` task instead).
    Action(SnapshotAction),
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

/// Sends `message` to `to`, stamping the clock where the lease reads it: `sent` on
/// an AppendEntries, `local` on its response (RAFT.md §1); and stamping
/// `incarnation`, the sender's store incarnation, on an AppendEntries or
/// InstallSnapshot response (RAFT.md §3, PROPOSED(D-042)) — 0 from a refused
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

/// Runs one server until its store fails or its socket closes. Spawn it with
/// `Environment::spawn`; spawn it again after a crash to restart the server on what
/// its disk kept.
///
/// # Errors
///
/// The bind, or an I/O error while running; each is traced before it is returned.
/// A store refused for lost state is no longer an error: the server runs in
/// re-seed mode until a leader's snapshot rebuilds it (RAFT.md §3).
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
    // The store's recovery must never hand back a state with a hole (RAFT.md §3):
    // no fallback, no discarded head, and a damaged log refused before it is cut.
    let engine = EngineConfig {
        allow_manifest_fallback: false,
        allow_head_gap: false,
        refuse_log_damage: true,
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
                admit(&env, server, &inbox, inbox_capacity, frame);
            }
            inbox.close();
        }
    });

    loop {
        if let Err(error) = snapshot::adopt_staged(&env, &engine.dir).await {
            env.trace(TraceEvent::RaftServerFailed {
                server,
                reason: format!("adopting an installed snapshot: {error}"),
            });
            return Err(error);
        }
        let opened = match Engine::open(env.clone(), engine.clone()).await {
            Ok((opened, recovery)) => RaftStore::open(Arc::new(opened), &recovery).await,
            Err(error) => Err(error),
        };
        let (store, recovered) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                env.trace(TraceEvent::RaftRefused {
                    server,
                    reason: error.to_string(),
                });
                // Re-seed mode (RAFT.md §3): the store is gone; wait for a
                // leader's snapshot to rebuild it, taking part in nothing else.
                match reseed(&env, id, &sock, &addrs, &raft, &engine.dir, &inbox).await {
                    Next::Closed => return Ok(()),
                    Next::Reinstall => continue,
                }
            }
        };
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
        )
        .await?;
        match next {
            Next::Closed => return Ok(()),
            Next::Reinstall => {}
        }
    }
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
) -> io::Result<Next> {
    let server = id.0;
    let tick = Duration::from_nanos(raft.tick_nanos);
    let variant = raft.variant;
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
        ),
    );

    // The start: the log as the disk holds it, re-stated so the trace's picture of
    // this server's log is the durable one. An append or a truncation persisted at a
    // crash but not yet traced would otherwise be missing from it. A snapshot the
    // store records is re-stated the same way: it sets the applied floor and stands
    // in for the log prefix it replaced (RAFT.md §2).
    env.trace(TraceEvent::RaftTruncate {
        server,
        from_index: core.last_index() + 1,
    });
    if snap_index > 0 {
        env.trace(TraceEvent::RaftSnapshot {
            server,
            last_index: snap_index,
            last_term: snap_term,
            taken: snapshot_record.as_ref().is_some_and(|s| s.taken),
        });
    }
    if quarantined {
        env.trace(TraceEvent::RaftReseeded { server });
    }
    for entry in core.log() {
        env.trace(TraceEvent::RaftAppend {
            server,
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
        term: core.term(),
        applied,
        last_index: core.last_index(),
        incarnation: store.incarnation(),
    });
    env.trace(TraceEvent::RaftTerm {
        server,
        term: core.term(),
        role: "follower",
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
        variant,
        apply_sent: applied,
        proposed: BTreeMap::new(),
        reads: BTreeMap::new(),
        next_read: 0,
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
        let (input, request, read, change) = match event {
            None => (Input::Tick, None, None, None),
            Some(Event::Message { from, message }) => match message {
                // Snapshot streaming is the snapshot task's (RAFT.md §3).
                Message::InstallSnapshot { .. } => {
                    node.snaps.push(Snap::Chunk { from, message });
                    continue;
                }
                Message::InstallSnapshotResponse { .. } => {
                    node.snaps.push(Snap::Ack { from, message });
                    continue;
                }
                message => (
                    Input::Message {
                        from,
                        message,
                        now: now_nanos(env),
                    },
                    None,
                    None,
                    None,
                ),
            },
            Some(Event::Applied(index)) => (Input::Applied(index), None, None, None),
            Some(Event::Taken { index, term }) => {
                (Input::SnapshotTaken { index, term }, None, None, None)
            }
            Some(Event::TakeFailed) => (
                Input::SnapshotFailed {
                    to: id,
                    retake: true,
                },
                None,
                None,
                None,
            ),
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
                    env.trace(TraceEvent::RaftProposed {
                        server,
                        client: request.client,
                        seq: request.seq,
                        index,
                        term,
                    });
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
        node.execute(&core, outputs).await?;
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
                let outputs = core.step(Input::Applied(index));
                node.execute(core, outputs).await?;
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
            // PROPOSED(D-035): re-seeded servers are quarantined from voting for good.
            quarantined: core.quarantined(),
            // An install into a live store keeps its incarnation: the kept tail
            // is everything acknowledged past the snapshot, so nothing a leader
            // matched is lost. PROPOSED(D-042): store incarnations.
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
    );
    node.apply_sent = node.store.applied();
    node.apply_through(core, core.commit());
    Ok(None)
}

/// Spawns the `apply` task: committed entries applied one synced batch each, and
/// snapshots taken between applies (RAFT.md §1, PROPOSED(D-036)).
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
            let entries = match job {
                Job::Entries(entries) => entries,
                Job::Take => {
                    // A snapshot at the applied index: the record first, synced,
                    // then the checkpoint, and no apply lands in between, so the
                    // record is exact (RAFT.md §1, PROPOSED(D-036)).
                    if applied == 0 {
                        inbox.push(Event::TakeFailed);
                        continue;
                    }
                    let dir = snapshot::checkpoint_dir(&engine_dir, applied);
                    match snapshot::take(&env, &store, &dir, applied, applied_term, &config).await {
                        Ok(()) => {
                            env.trace(TraceEvent::RaftSnapshot {
                                server,
                                last_index: applied,
                                last_term: applied_term,
                                taken: true,
                            });
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
                env.trace(TraceEvent::RaftApply {
                    server,
                    index: entry.index,
                    entry_term: entry.term,
                    hash: entry.payload.hash(),
                });
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
}

/// How many times one chunk is resent before the stream is given up.
const CHUNK_RESENDS: u32 = 8;
/// How many times a stream starts over before the checkpoint is retaken.
const STREAM_RESTARTS: u32 = 2;

/// The `snapshot` task (RAFT.md §3): streams checkpoints to followers on the
/// leader's behalf, one chunk outstanding and resumed from the last acknowledged
/// offset after loss; assembles and verifies arriving streams on a follower; the
/// only task that touches checkpoint directories.
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
) {
    let server = id.0;
    let chunk_timeout = Duration::from_nanos(config.tick_nanos * config.election_ticks.0 / 2);
    let mut assembler = Assembler::new(env.clone(), &engine_dir, config.variant);
    let mut staged: Option<Staged> = None;
    let mut outbound: Option<Outbound> = None;
    let mut backlog: Vec<ServerId> = Vec::new();
    loop {
        let item = if let Some(out) = &outbound {
            let pop = pin!(snaps.pop());
            let timer = pin!(env.clock().sleep_until(out.deadline));
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
            // The chunk timed out: resend from where the receiver last stood,
            // the resumption of RAFT.md §1, or give the stream up.
            let out = outbound.as_mut().expect("a stream timed out");
            out.resends += 1;
            if out.resends > CHUNK_RESENDS {
                inbox.push(Event::StreamFailed {
                    to: out.sender.to,
                    retake: false,
                });
                outbound = None;
            } else {
                env.trace(TraceEvent::RaftSnapshotResumed {
                    server,
                    to: out.sender.to.0,
                    offset: out.sender.position_offset(config.snapshot_chunk),
                });
                send_chunk(&env, &sock, &addrs, id, &config, out, chunk_timeout).await;
            }
            if outbound.is_none()
                && let Some(to) = backlog.pop()
            {
                outbound = start_stream(
                    &env,
                    &store,
                    id,
                    to,
                    &sock,
                    &addrs,
                    &config,
                    chunk_timeout,
                    &inbox,
                )
                .await;
            }
            continue;
        };
        match item {
            Snap::Action(SnapshotAction::Take) => {
                // Takes run in the apply task; the server routes them there.
            }
            Snap::Action(SnapshotAction::Install { to, .. }) => {
                if outbound.as_ref().is_some_and(|out| out.sender.to == to) || backlog.contains(&to)
                {
                    continue;
                }
                if outbound.is_some() {
                    backlog.push(to);
                    continue;
                }
                outbound = start_stream(
                    &env,
                    &store,
                    id,
                    to,
                    &sock,
                    &addrs,
                    &config,
                    chunk_timeout,
                    &inbox,
                )
                .await;
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
                let Some(ready) = staged.take() else { continue };
                let identity = (ready.last_index, ready.last_term);
                let to = ready.from;
                match assembler.finish(&ready, &repair).await {
                    Ok(()) => {
                        // The install exists (RAFT.md §1): say so, and let the
                        // server switch. The restatement on the adopted store
                        // re-traces the snapshot as the disk's picture.
                        env.trace(TraceEvent::RaftSnapshot {
                            server,
                            last_index: identity.0,
                            last_term: identity.1,
                            taken: false,
                        });
                        let config = &ready.config;
                        env.trace(TraceEvent::RaftConfig {
                            server,
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
                        });
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
                let Some(out) = &mut outbound else { continue };
                if out.sender.to != from
                    || out.sender.last_index != last_index
                    || out.sender.last_term != last_term
                {
                    continue;
                }
                match status {
                    SnapshotStatus::Installed => {
                        inbox.push(Event::StreamDone {
                            to: from,
                            index: last_index,
                            incarnation,
                        });
                        outbound = None;
                    }
                    SnapshotStatus::Restart => {
                        out.restarts += 1;
                        if out.restarts > STREAM_RESTARTS {
                            inbox.push(Event::StreamFailed {
                                to: from,
                                retake: true,
                            });
                            outbound = None;
                        } else {
                            out.sender.restart();
                            out.resends = 0;
                            send_chunk(&env, &sock, &addrs, id, &config, out, chunk_timeout).await;
                        }
                    }
                    SnapshotStatus::More => {
                        if out.sender.on_more(&file, offset) {
                            env.trace(TraceEvent::RaftSnapshotResumed {
                                server,
                                to: from.0,
                                offset,
                            });
                        }
                        out.resends = 0;
                        if out.sender.at_end() {
                            out.deadline = env.clock().now() + chunk_timeout;
                        } else {
                            send_chunk(&env, &sock, &addrs, id, &config, out, chunk_timeout).await;
                        }
                    }
                }
                if outbound.is_none()
                    && let Some(to) = backlog.pop()
                {
                    outbound = start_stream(
                        &env,
                        &store,
                        id,
                        to,
                        &sock,
                        &addrs,
                        &config,
                        chunk_timeout,
                        &inbox,
                    )
                    .await;
                }
            }
            Snap::Chunk { .. } | Snap::Ack { .. } => {}
        }
    }
}

/// Opens a stream of the recorded checkpoint to `to` and sends its first chunk;
/// reports a failure to the core instead when there is no streamable checkpoint.
#[expect(clippy::too_many_arguments, reason = "the task's whole world")]
async fn start_stream<E: Environment>(
    env: &E,
    store: &Arc<RaftStore<E>>,
    id: ServerId,
    to: ServerId,
    sock: &Arc<<E::Net as Network>::Socket>,
    addrs: &Arc<BTreeMap<ServerId, SocketAddr>>,
    config: &RaftConfig,
    chunk_timeout: Duration,
    inbox: &Queue<Event>,
) -> Option<Outbound> {
    let record = match store.snapshot_record().await {
        Ok(Some(record)) if record.taken && !record.dir.is_empty() => record,
        _ => {
            // No checkpoint of our own to stream: ask for a fresh take.
            inbox.push(Event::StreamFailed { to, retake: true });
            return None;
        }
    };
    let sender = Sender::open(
        env,
        Path::new(&record.dir),
        to,
        record.last_index,
        record.last_term,
        store.term(),
    )
    .await;
    match sender {
        Ok(sender) => {
            let mut out = Outbound {
                sender,
                deadline: env.clock().now() + chunk_timeout,
                resends: 0,
                restarts: 0,
            };
            send_chunk(env, sock, addrs, id, config, &mut out, chunk_timeout).await;
            Some(out)
        }
        Err(_) => {
            // The recorded checkpoint is unusable (a crash between the record
            // and the checkpoint leaves one): take a fresh one.
            inbox.push(Event::StreamFailed { to, retake: true });
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
/// quarantine flag (PROPOSED(D-035)) and a fresh store incarnation
/// (PROPOSED(D-042)), and the caller adopts it. Its answers carry incarnation 0
/// until then: a refused server has no store.
async fn reseed<E: Environment>(
    env: &E,
    id: ServerId,
    sock: &Arc<<E::Net as Network>::Socket>,
    addrs: &Arc<BTreeMap<ServerId, SocketAddr>>,
    raft: &RaftConfig,
    engine_dir: &Path,
    inbox: &Queue<Event>,
) -> Next {
    let mut assembler = Assembler::new(env.clone(), engine_dir, raft.variant);
    loop {
        let Some(event) = inbox.pop().await else {
            return Next::Closed;
        };
        let Event::Message { from, message } = event else {
            continue;
        };
        match message {
            Message::AppendEntries {
                term, prev_index, ..
            } => {
                // The re-seed ask: reject with a hint of 1, echo 0 so no lease
                // promise is ever measured from this server (RAFT.md §3), and
                // incarnation 0, no store, so a leader that matched entries on
                // the lost one forgets them (PROPOSED(D-042)).
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
                        // No store survived, so there is nothing of our own to
                        // carry over: term from the stream, no vote, no tail,
                        // and the quarantine flag for the vote the lost state
                        // may have held (PROPOSED(D-035)). The store's
                        // incarnation is drawn afresh: the number the lost
                        // store carried is gone with it, and what matters is
                        // that no leader has recorded this one against a match
                        // index the rebuilt log cannot honour. Never the first
                        // incarnation, which every fresh store starts at
                        // (PROPOSED(D-042)).
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
                                env.trace(TraceEvent::RaftSnapshot {
                                    server: id.0,
                                    last_index: identity.0,
                                    last_term: identity.1,
                                    taken: false,
                                });
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
) {
    let is_message = |event: &Event| matches!(event, Event::Message { .. });
    let is_heartbeat = |message: &Message| matches!(message, Message::AppendEntries { entries, .. } if entries.is_empty());
    let carries_entries = |message: &Message| {
        matches!(message, Message::AppendEntries { entries, .. } if !entries.is_empty())
            || matches!(message, Message::InstallSnapshot { .. })
    };
    if inbox.count(is_message) >= capacity {
        let dropped = |kind: &'static str| {
            env.trace(TraceEvent::RaftInboxDropped { server, kind });
        };
        let (from, kind) = (frame.from, frame.message.kind());
        let victim = inbox
            .remove_first(
                |event| matches!(event, Event::Message { message, .. } if is_heartbeat(message)),
            )
            .or_else(|| {
                inbox.remove_first(|event| {
                    matches!(event, Event::Message { from: f, message }
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
    variant: Variant,
    /// The highest index handed to the `apply` task.
    apply_sent: Index,
    /// Requests this server proposed, by client and sequence number, with the index
    /// and term their entry took: a duplicate of one still in the log is not
    /// proposed again.
    proposed: BTreeMap<(u64, u64), (Index, Term)>,
    /// Reads waiting on the core, by the id handed to it.
    reads: BTreeMap<u64, (SocketAddr, Request)>,
    next_read: u64,
}

/// How many proposals a server remembers against duplicates.
const PROPOSED_REMEMBERED: usize = 4096;

impl<E: Environment> Server<E> {
    /// Sends a message, stamping the clock where the lease reads it and the
    /// store's incarnation on a response (PROPOSED(D-042)).
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
    async fn execute(&mut self, core: &Raft, outputs: Vec<Output>) -> io::Result<()> {
        let send_first = self.variant == Variant::SendBeforePersist;
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
                    if self.variant == Variant::ApplyBeforeCommit {
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
                Output::ReadReady { id, .. } => {
                    let key = match self.reads.get(&id) {
                        Some((_, request)) => match request.command.key() {
                            Some(key) => key.clone(),
                            None => continue,
                        },
                        None => continue,
                    };
                    let reply = match self.store.engine().get(&user_key(&key)).await {
                        Ok(value) => Reply::Outcome(Outcome::Value(value)),
                        Err(error) => {
                            self.env.trace(TraceEvent::RaftServerFailed {
                                server: self.id.0,
                                reason: error.to_string(),
                            });
                            return Err(error);
                        }
                    };
                    self.answer_read(id, reply).await;
                }
                Output::ReadDropped { id } => {
                    self.answer_read(id, Reply::NotLeader { leader: None })
                        .await;
                }
                Output::Snapshot(SnapshotAction::Take) => self.jobs.push(Job::Take),
                Output::Snapshot(action) => self.snaps.push(Snap::Action(action)),
                Output::Trace(event) => self.env.trace(event),
            }
        }
        Ok(())
    }
}

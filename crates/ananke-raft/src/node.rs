//! One server under the [`Environment`] (RAFT.md §3): the tasks that run a core.
//!
//! [`run`] opens the engine and the store, binds the socket, and becomes the `raft`
//! task; it spawns the `net` and `apply` tasks beside it. The fourth task of RAFT.md
//! §3, `snapshot`, arrives with snapshots.
//!
//! - `raft` owns the core and the timer: one loop over a race of the inbox and the
//!   tick, stepping the core once per event and executing every output in order,
//!   awaiting a [`Output::Persist`] before the sends that follow it. That order is
//!   the persistence discipline of Figure 2; [`Variant::SendBeforePersist`] is the
//!   server that breaks it.
//! - `net` receives frames, decodes, and puts messages on the inbox, which is bounded:
//!   a full inbox drops the oldest heartbeat first, then the oldest message of the
//!   same kind from the same sender as the one arriving, which a newer one
//!   supersedes, and never an AppendEntries with entries, which is admitted over the
//!   bound; each drop is [`TraceEvent::RaftInboxDropped`]. Client requests take the
//!   same socket and the same inbox. The two tasks are separate so that a message
//!   arriving while the core awaits a persist is a queued message, not a lost one.
//! - `apply` takes committed entries from the `raft` task, applies each as one synced
//!   batch with the applied index, answers the client that proposed it, and reports
//!   the applied index back. [`Variant::ApplyBeforeCommit`] hands it entries as they
//!   are appended instead.
//!
//! A server whose store is refused ([`LostState`](crate::store::LostState)) traces
//! [`TraceEvent::RaftRefused`] and returns before binding its socket: it votes for
//! nobody and answers nothing until a snapshot re-seeds it.
//!
//! A client request becomes a proposal. A server that is not the leader answers
//! [`Reply::NotLeader`] at once. The leader remembers the request against the index
//! its entry took, and the `apply` task answers when that index applies with an entry
//! of the same term; an entry replaced by a later leader's is never answered, since
//! its fate is not known here.
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
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::{Clock, Either, Environment, Network, Rng, Socket, TraceEvent, race};
use ananke_storage::{Engine, EngineConfig};

use crate::apply::{Command, apply_command};
use crate::client::{self, Reply, Request, Response};
use crate::core::{Input, Output, Raft, RaftConfig, Variant};
use crate::message::{Frame, Message};
use crate::queue::Queue;
use crate::store::RaftStore;
use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// What one server needs to run.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// This server.
    pub id: ServerId,
    /// The address it binds.
    pub listen: SocketAddr,
    /// Every voter and its address, this server included.
    pub servers: Vec<(ServerId, SocketAddr)>,
    /// The core's parameters.
    pub raft: RaftConfig,
    /// The engine's. Fallback and head-gap discard are turned off and log-damage
    /// refusal on whatever is passed (RAFT.md §3).
    pub engine: EngineConfig,
    /// How long one of the core's ticks is: the minimum election timeout over
    /// `raft.election_ticks.0`.
    pub tick: Duration,
    /// How many messages the inbox holds before it drops.
    pub inbox_capacity: usize,
}

/// What the `raft` task takes from its inbox.
enum Event {
    /// A protocol message from a peer.
    Message { from: ServerId, message: Message },
    /// A client's request, with where to answer.
    Request { from: SocketAddr, request: Request },
    /// The `apply` task applied through this index.
    Applied(Index),
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

/// Runs one server until its store fails or its socket closes. Spawn it with
/// `Environment::spawn`; spawn it again after a crash to restart the server on what
/// its disk kept.
///
/// # Errors
///
/// The engine's open, a refusal ([`crate::store::LostState`]), the bind, or an I/O
/// error while running; each is traced before it is returned.
pub async fn run<E: Environment>(env: E, config: NodeConfig) -> io::Result<()> {
    let NodeConfig {
        id,
        listen,
        servers,
        raft,
        engine,
        tick,
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
    let (engine, recovery) = match Engine::open(env.clone(), engine).await {
        Ok(opened) => opened,
        Err(error) => {
            env.trace(TraceEvent::RaftRefused {
                server,
                reason: error.to_string(),
            });
            return Err(error);
        }
    };
    let (store, log) = match RaftStore::open(Arc::new(engine), &recovery).await {
        Ok(opened) => opened,
        Err(error) => {
            env.trace(TraceEvent::RaftRefused {
                server,
                reason: error.to_string(),
            });
            return Err(error);
        }
    };
    let store = Arc::new(store);
    let sock = Arc::new(env.net().bind(listen).await?);
    let addrs: Arc<BTreeMap<ServerId, SocketAddr>> = Arc::new(servers.iter().copied().collect());
    let voters: Vec<ServerId> = servers.iter().map(|(id, _)| *id).collect();
    let variant = raft.variant;
    let seed = env.rng().next_u64();
    let mut core = Raft::restore(
        id,
        Configuration::of(&voters),
        raft,
        seed,
        store.term(),
        store.vote(),
        log,
    );
    let applied = store.applied();
    core.step(Input::Applied(applied));
    let inbox: Queue<Event> = Queue::new();
    let jobs: Queue<Vec<Entry>> = Queue::new();
    let pending: Pending = Arc::default();

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

    env.spawn("apply", {
        let env = env.clone();
        let inbox = inbox.clone();
        let jobs = jobs.clone();
        let store = store.clone();
        let sock = sock.clone();
        let pending = pending.clone();
        async move {
            let mut applied = store.applied();
            while let Some(entries) = jobs.pop().await {
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
        }
    });

    // The start: the log as the disk holds it, re-stated so the trace's picture of
    // this server's log is the durable one. An append or a truncation persisted at a
    // crash but not yet traced would otherwise be missing from it.
    env.trace(TraceEvent::RaftTruncate {
        server,
        from_index: core.last_index() + 1,
    });
    for entry in core.log() {
        env.trace(TraceEvent::RaftAppend {
            server,
            index: entry.index,
            entry_term: entry.term,
            hash: entry.payload.hash(),
        });
    }
    // Then what the server resumes from, and the term for the trace's term lane and
    // the sweep's timer check, which counts from here.
    env.trace(TraceEvent::RaftRecovered {
        server,
        term: core.term(),
        applied,
        last_index: core.last_index(),
    });
    env.trace(TraceEvent::RaftTerm {
        server,
        term: core.term(),
        role: "follower",
    });
    let mut node = Server {
        env: env.clone(),
        id,
        sock,
        addrs,
        store,
        jobs,
        pending,
        variant,
        apply_sent: applied,
        proposed: BTreeMap::new(),
    };
    let mut next_tick = env.clock().now() + tick;
    loop {
        let event = {
            let pop = pin!(inbox.pop());
            let timer = pin!(env.clock().sleep_until(next_tick));
            match race(&env, pop, timer).await {
                Either::Left(Some(event)) => Some(event),
                Either::Left(None) => return Ok(()),
                Either::Right(()) => {
                    next_tick += tick;
                    None
                }
            }
        };
        let (input, request) = match event {
            None => (Input::Tick, None),
            Some(Event::Message { from, message }) => (Input::Message { from, message }, None),
            Some(Event::Applied(index)) => (Input::Applied(index), None),
            Some(Event::Request { from, request }) => {
                if let Some(&(index, term)) = node.proposed.get(&(request.client, request.seq))
                    && core.term_at(index) == Some(term)
                {
                    // A copy of a request whose entry is in the log: it is answered
                    // when that entry applies, once.
                    continue;
                }
                (
                    Input::Propose(request.command.encode()),
                    Some((from, request)),
                )
            }
        };
        let outputs = core.step(input);
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
    let carries_entries = |message: &Message| matches!(message, Message::AppendEntries { entries, .. } if !entries.is_empty());
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
    jobs: Queue<Vec<Entry>>,
    pending: Pending,
    variant: Variant,
    /// The highest index handed to the `apply` task.
    apply_sent: Index,
    /// Requests this server proposed, by client and sequence number, with the index
    /// and term their entry took: a duplicate of one still in the log is not
    /// proposed again.
    proposed: BTreeMap<(u64, u64), (Index, Term)>,
}

/// How many proposals a server remembers against duplicates.
const PROPOSED_REMEMBERED: usize = 4096;

impl<E: Environment> Server<E> {
    async fn send(&self, to: ServerId, message: Message) {
        if let Some(addr) = self.addrs.get(&to) {
            let frame = Frame {
                from: self.id,
                message,
            };
            let _ = self.sock.send(*addr, frame.encode()).await;
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
        self.jobs.push(entries);
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
                            self.jobs.push(entries);
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
                Output::Trace(event) => self.env.trace(event),
            }
        }
        Ok(())
    }
}

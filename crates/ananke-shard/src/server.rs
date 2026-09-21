//! The node as a running server: four ranges on one node, fixed at bootstrap from
//! configuration (SHARD.md §2, §4; §12's Stage B).
//!
//! [`mod@node`](crate::node) is the node's *schedule* — one ticker, every core, Q41's
//! round — over a [`Host`] it drives. This module is that host: one engine, one
//! socket, one inbox, one `raft` task and one `apply` task, and a Raft store and a
//! core **per range**, each under its own key prefix (`KeyPrefix::group`, D-060).
//!
//! What configuration names is §2 generalised. A server's configuration today names
//! "the voters a fresh store starts with" (`initial_voters`); a node's names *the
//! ranges it hosts* and the voters each starts with, and a fresh replica of each is
//! created at the node's first start and traced `RangeCreated { cause: bootstrap }`
//! (§8). Nothing here changes a descriptor and nothing routes: the span a range holds
//! is configuration's, carried so the creation can be traced with it, and no code
//! reads it to decide anything. A client takes its key's range from the scenario's
//! fixed map and puts it on every message ([`mod@client`](crate::client)).
//!
//! Each core is seeded from `n{id}/r{range}/protocol` (Q13, D-057): two ranges on one
//! node draw different election timeouts, which is what keeps four ranges from
//! campaigning in lockstep.
//!
//! **What this slice's node does not do, and what happens if it is asked to.** The
//! snapshot paths are the snapshot slice's and Q15's refusal and re-seed are their
//! own slice's; neither is wired here. A core that asks for a snapshot action —
//! a take, a stream to a follower behind the compacted prefix — is counted in
//! [`Gaps::snapshot_actions`] and nothing else happens, and a store refused for lost
//! state stops the node with its loss marked. Both are asserted at zero by the
//! scenarios that run this node, with the reason, rather than left to be discovered
//! (CLAUDE.md's rule for a pinned situation's absence).

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::{ApplyEffect, RangeCause};
use ananke_env::{Clock, Decision, Environment, Network, Rng, Socket, TraceEvent};
use ananke_raft::apply::{Command, Outcome, apply_command, user_key};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, SnapshotAction, Variant};
use ananke_raft::node::{Start, StartOrder, start_store};
use ananke_raft::queue::Queue;
use ananke_raft::store::{KeyPrefix, RaftStore, mark_store_lost};
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId, Term};
use ananke_raft::{Input, Message, Persist, Raft};
use ananke_storage::EngineConfig;
use bytes::Bytes;

use crate::client::{RangedRequest, RangedResponse, is_ranged};
use crate::frame::decode;
use crate::inbox::{Inbox, Received};
use crate::node::{
    Applier, ApplyJob, ApplyWork, Boxed, BoxedPersist, Host, Node, NodeConfig as TaskConfig, apply,
};
use crate::outbox::Outbox;
use crate::range::RangeId;
use crate::round::Cores;
use crate::variant::NodeVariants;

/// One range as configuration fixes it at bootstrap (SHARD.md §2).
///
/// The span is carried so the replica's `RangeCreated` can name it (§8's table). It
/// is not a descriptor: nothing on the node reads it, no apply changes it, and no
/// message is routed by it. Stage C gives ranges real descriptors in range 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range {
    /// The range's id.
    pub id: RangeId,
    /// The span's first key.
    pub start: Bytes,
    /// The key past the span's last.
    pub end: Bytes,
}

/// How a node is configured (SHARD.md §2): its id, its address, the address book,
/// the ranges it hosts and the voters each of them starts with.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// This node.
    pub id: ServerId,
    /// The address it binds.
    pub listen: SocketAddr,
    /// Every node that may exist, and its address.
    pub servers: Vec<(ServerId, SocketAddr)>,
    /// The ranges this node hosts, fixed at bootstrap. Four per node in Stage B's
    /// scenarios, which is the plan's parameter and not a measurement.
    pub ranges: Vec<Range>,
    /// The voters a fresh replica of each range starts with (§2: `initial_voters`
    /// generalised). A replica whose store already holds a configuration uses that
    /// one instead, as a server does today (D-029).
    pub initial_voters: Vec<ServerId>,
    /// The cores' parameters.
    pub raft: RaftConfig,
    /// The engine's; one engine for the whole node (Q2).
    pub engine: EngineConfig,
    /// The node's inbox bound, in bytes (Q14, D-072).
    pub inbox_bytes: usize,
    /// The node's known-buggy variants (the round's and the wire's).
    pub node: NodeVariants,
}

/// What this slice's node was asked for and does not do: each counted, none silent.
///
/// A scenario asserts each of these at zero and says why, so the day a schedule
/// reaches one of these paths the scenario says so instead of passing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Gaps {
    /// Snapshot actions a core asked for: a take, or a stream to a follower behind
    /// the compacted prefix. The `snapshot` task keyed by range and follower is its
    /// own slice's, and is not wired to this host.
    pub snapshot_actions: u64,
    /// Client requests for a range this node does not host.
    pub requests_for_ranges_not_held: u64,
}

/// What a range's replica keeps between the `raft` task and the `apply` task.
struct Waiting {
    term: Term,
    from: SocketAddr,
    client: u64,
    seq: u64,
}

/// The client work in flight for one range.
#[derive(Clone, Debug)]
enum InFlight {
    /// A proposal: answered `NotLeader` if the step rejects it, and from the apply
    /// otherwise.
    Propose {
        from: SocketAddr,
        client: u64,
        seq: u64,
    },
    /// A membership change: answered at the step, Done or NotLeader (D-029).
    Change {
        from: SocketAddr,
        client: u64,
        seq: u64,
    },
}

/// One replica's bookkeeping: everything the host keeps about a range.
struct Replica {
    /// Entries proposed and not yet applied, by index.
    pending: BTreeMap<Index, Waiting>,
    /// Requests this replica proposed, by client and sequence number, with the index
    /// and term their entry took: a duplicate of one still in the log is not
    /// proposed again.
    proposed: BTreeMap<(u64, u64), (Index, Term)>,
    /// Reads waiting on the core, by the id handed to it.
    reads: BTreeMap<u64, (SocketAddr, Request)>,
    next_read: u64,
    /// The client work whose step is being driven now.
    in_flight: Option<InFlight>,
}

impl Replica {
    fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            proposed: BTreeMap::new(),
            reads: BTreeMap::new(),
            next_read: 0,
            in_flight: None,
        }
    }
}

/// How many proposals a replica remembers against duplicates (the server's figure).
const PROPOSED_REMEMBERED: usize = 4096;

/// What the node's tasks hand the `raft` task besides its peers' messages.
pub enum Local {
    /// A client's request, with the range it named.
    Request {
        /// The range.
        range: RangeId,
        /// Where the client is.
        from: SocketAddr,
        /// What it asked for.
        request: Request,
    },
    /// The `apply` task made an index durable for a range.
    Applied {
        /// The range.
        range: RangeId,
        /// The index.
        index: Index,
    },
}

/// The node's host: its socket, its engine, and a store and a replica per range.
pub struct ServerHost<E: Environment> {
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    addrs: BTreeMap<ServerId, SocketAddr>,
    stores: BTreeMap<RangeId, Arc<RaftStore<E>>>,
    replicas: BTreeMap<RangeId, Arc<Mutex<Replica>>>,
    jobs: Queue<ApplyJob>,
    /// The answers the `raft` task owes clients, sent by the node's `answers` task.
    ///
    /// A step of the round is synchronous — the host is told what the core decided
    /// while the round is being driven — so an answer decided there cannot await the
    /// socket where it is decided. It is queued and sent by one task of the node's
    /// own, never by a task per answer.
    // PROPOSED(D-076): a client's answer decided inside a round leaves through the
    // node's `answers` task.
    answers: Queue<(SocketAddr, Bytes)>,
    variants: ananke_raft::core::Variants,
    gaps: Mutex<Gaps>,
}

fn lock<T>(what: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // A panic under a lock would poison it; nothing here panics under one, and a
    // poisoned lock is taken anyway rather than crashing the node, as the server's
    // own `lock_pending` does.
    what.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl<E: Environment> ServerHost<E> {
    /// What this node was asked for and does not do.
    #[must_use]
    pub fn gaps(&self) -> Gaps {
        *lock(&self.gaps)
    }

    fn replica(&self, range: RangeId) -> Option<&Arc<Mutex<Replica>>> {
        self.replicas.get(&range)
    }

    /// Queues one answer for the node's `answers` task.
    fn answer_later(&self, range: RangeId, to: SocketAddr, client: u64, seq: u64, reply: Reply) {
        let bytes = RangedResponse {
            range,
            response: Response { client, seq, reply },
        }
        .encode();
        self.answers.push((to, bytes));
    }

    /// Answers one client.
    async fn answer(&self, range: RangeId, to: SocketAddr, client: u64, seq: u64, reply: Reply) {
        let response = RangedResponse {
            range,
            response: Response { client, seq, reply },
        };
        let _ = self.sock.send(to, response.encode()).await;
    }

    /// Answers the client that asked for read `id` of `range`, if it is still
    /// waiting.
    async fn answer_read(&self, range: RangeId, id: u64, reply: Reply) {
        let waiting = self
            .replica(range)
            .and_then(|replica| lock(replica).reads.remove(&id));
        if let Some((from, request)) = waiting {
            self.answer(range, from, request.client, request.seq, reply)
                .await;
        }
    }
}

impl<E: Environment> Host for ServerHost<E> {
    type Local = Local;

    fn local_range(&self, local: &Self::Local) -> RangeId {
        match local {
            Local::Request { range, .. } | Local::Applied { range, .. } => *range,
        }
    }

    fn local_input(&self, local: Self::Local, core: &Raft) -> Option<Input> {
        let (range, from, request) = match local {
            Local::Applied { index, .. } => return Some(Input::Applied(index)),
            Local::Request {
                range,
                from,
                request,
            } => (range, from, request),
        };
        let Some(replica) = self.replica(range) else {
            lock(&self.gaps).requests_for_ranges_not_held += 1;
            return None;
        };
        let mut state = lock(replica);
        match &request.command {
            // An operator's wish, acted on at once and answered at once.
            Command::Transfer { to } => {
                let to = ServerId(*to);
                state.in_flight = None;
                drop(state);
                self.answer_later(
                    range,
                    from,
                    request.client,
                    request.seq,
                    Reply::Outcome(Outcome::Done),
                );
                Some(Input::Transfer(to))
            }
            // An operator's membership change (RAFT.md §1): answered at the step.
            Command::Change { voters } => {
                let voters = voters.iter().copied().map(ServerId).collect();
                state.in_flight = Some(InFlight::Change {
                    from,
                    client: request.client,
                    seq: request.seq,
                });
                Some(Input::Change(voters))
            }
            // A read: served by the lease or after a heartbeat round, never through
            // the log.
            Command::Get { .. } => {
                let id = state.next_read;
                state.next_read += 1;
                state.reads.insert(id, (from, request));
                state.in_flight = None;
                Some(Input::Read {
                    id,
                    now: now_nanos(&self.env),
                })
            }
            _ => {
                if let Some(&(index, term)) = state.proposed.get(&(request.client, request.seq))
                    && core.term_at(index) == Some(term)
                {
                    // A copy of a request whose entry is in the log: it is answered
                    // when that entry applies, once.
                    return None;
                }
                state.in_flight = Some(InFlight::Propose {
                    from,
                    client: request.client,
                    seq: request.seq,
                });
                Some(Input::Propose(request.command.encode()))
            }
        }
    }

    fn local_stepped(&self, range: RangeId, core: &Raft, decided: Decision) {
        let Some(replica) = self.replica(range) else {
            return;
        };
        let mut state = lock(replica);
        // A rejection took the in-flight work already (`rejected`), so what is left
        // here was accepted.
        let Some(in_flight) = state.in_flight.take() else {
            return;
        };
        match in_flight {
            InFlight::Change { from, client, seq } => {
                drop(state);
                self.answer_later(range, from, client, seq, Reply::Outcome(Outcome::Done));
            }
            InFlight::Propose { from, client, seq } => {
                let (index, term) = (core.last_index(), core.term());
                self.env.trace_decided(
                    decided,
                    TraceEvent::RaftProposed {
                        server: self.id.0,
                        range: range.get(),
                        client,
                        seq,
                        index,
                        term,
                    },
                );
                state.pending.insert(
                    index,
                    Waiting {
                        term,
                        from,
                        client,
                        seq,
                    },
                );
                state.proposed.insert((client, seq), (index, term));
                if state.proposed.len() > PROPOSED_REMEMBERED {
                    let keep_from = index.saturating_sub(PROPOSED_REMEMBERED as Index / 2);
                    state.proposed.retain(|_, (i, _)| *i >= keep_from);
                }
            }
        }
    }

    fn persist(&self, range: RangeId, persist: Persist) -> BoxedPersist {
        let Some(store) = self.stores.get(&range).cloned() else {
            return Box::pin(async { Ok(()) });
        };
        let replica = self.replica(range).cloned();
        let jobs = self.jobs.clone();
        let apply_before_commit = self.variants.contains(Variant::ApplyBeforeCommit);
        Box::pin(async move {
            store.persist(&persist).await?;
            if let Some(from) = persist.truncate_from
                && let Some(replica) = replica
            {
                lock(&replica).pending.retain(|&index, _| index < from);
            }
            if apply_before_commit {
                let entries: Vec<Entry> = persist.append;
                if !entries.is_empty() {
                    jobs.push(ApplyJob {
                        range,
                        work: ApplyWork::Entries(entries),
                    });
                }
            }
            Ok(())
        })
    }

    fn stamp(&self, range: RangeId, message: &mut Message) {
        let incarnation = self
            .stores
            .get(&range)
            .map_or(0, |store| store.incarnation());
        match message {
            Message::AppendEntries { sent, .. } => *sent = now_nanos(&self.env),
            Message::AppendEntriesResponse {
                local,
                incarnation: mine,
                ..
            } => {
                *local = now_nanos(&self.env);
                *mine = incarnation;
            }
            Message::InstallSnapshotResponse {
                incarnation: mine, ..
            } => *mine = incarnation,
            _ => {}
        }
    }

    fn ship(&self, to: ServerId, frame: Bytes) -> Boxed<'_, ()> {
        Box::pin(async move {
            if let Some(addr) = self.addrs.get(&to) {
                let _ = self.sock.send(*addr, frame).await;
            }
        })
    }

    fn read_ready(
        &self,
        range: RangeId,
        id: u64,
        index: Index,
        lease: bool,
    ) -> Boxed<'_, io::Result<()>> {
        Box::pin(async move {
            let Some(store) = self.stores.get(&range) else {
                return Ok(());
            };
            let key = {
                let Some(replica) = self.replica(range) else {
                    return Ok(());
                };
                let state = lock(replica);
                match state.reads.get(&id) {
                    Some((_, request)) => match request.command.key() {
                        Some(key) => key.clone(),
                        None => return Ok(()),
                    },
                    None => return Ok(()),
                }
            };
            // PROPOSED(D-069): the value and the applied index come from one engine
            // version, so the record says which state the client was answered from
            // (§11, raft 15).
            let version = store.engine().snapshot();
            let value = store.engine().get_at(&user_key(&key), &version).await?;
            let served_at = store.applied_at(&version).await?;
            drop(version);
            self.env.trace(TraceEvent::RaftRead {
                server: self.id.0,
                range: range.get(),
                index,
                lease,
                key,
                applied: served_at,
            });
            self.answer_read(range, id, Reply::Outcome(Outcome::Value(value)))
                .await;
            Ok(())
        })
    }

    fn read_dropped(&self, range: RangeId, id: u64) -> Boxed<'_, ()> {
        Box::pin(async move {
            self.answer_read(range, id, Reply::NotLeader { leader: None })
                .await;
        })
    }

    fn rejected(&self, range: RangeId, leader: Option<ServerId>) {
        let Some(replica) = self.replica(range) else {
            return;
        };
        let Some(in_flight) = lock(replica).in_flight.take() else {
            return;
        };
        let (from, client, seq) = match in_flight {
            InFlight::Propose { from, client, seq } | InFlight::Change { from, client, seq } => {
                (from, client, seq)
            }
        };
        self.answer_later(range, from, client, seq, Reply::NotLeader { leader });
    }

    fn apply(&self, job: ApplyJob) {
        self.jobs.push(job);
    }

    fn snapshot(&self, _range: RangeId, _action: SnapshotAction) {
        // The `snapshot` task keyed by range and follower is its own slice's; this
        // node has none wired. Counted, never silent, and asserted at zero by the
        // scenarios that run it.
        lock(&self.gaps).snapshot_actions += 1;
    }

    fn failed(&self, reason: String) {
        self.env.trace(TraceEvent::RaftServerFailed {
            server: self.id.0,
            reason,
        });
    }
}

/// The node's `apply` task: every range's entries, one job at a time (Q14, D-036).
struct ServerApplier<E: Environment> {
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    stores: BTreeMap<RangeId, Arc<RaftStore<E>>>,
    replicas: BTreeMap<RangeId, Arc<Mutex<Replica>>>,
    local: Queue<Local>,
    applied: Mutex<BTreeMap<RangeId, Index>>,
}

impl<E: Environment> Applier for ServerApplier<E> {
    fn run(&self, job: ApplyJob) -> Boxed<'_, ()> {
        Box::pin(async move {
            let ApplyJob { range, work } = job;
            let entries = match work {
                ApplyWork::Entries(entries) => entries,
                // A take is the snapshot slice's; the host counts the action that
                // asked for it and no job of this kind is ever queued here.
                _ => return,
            };
            let Some(store) = self.stores.get(&range) else {
                return;
            };
            for entry in entries {
                let mut applied = lock(&self.applied).get(&range).copied().unwrap_or(0);
                if applied == 0 {
                    applied = store.applied();
                }
                if entry.index <= applied {
                    continue;
                }
                if entry.index != applied + 1 {
                    self.env.trace(TraceEvent::RaftServerFailed {
                        server: self.id.0,
                        reason: format!(
                            "range {}: apply of {} after {applied}",
                            range.get(),
                            entry.index
                        ),
                    });
                    return;
                }
                // D-047: the apply is decided when the task takes the entry; it is
                // traced once its batch, with the applied index, is durable.
                let took = self.env.decision();
                let command = match &entry.payload {
                    Payload::Command(bytes) => Command::decode(bytes.clone()).ok(),
                    Payload::Noop | Payload::Config(_) => None,
                };
                let outcome = match apply_command(store, entry.index, command.as_ref()).await {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        self.env.trace(TraceEvent::RaftServerFailed {
                            server: self.id.0,
                            reason: error.to_string(),
                        });
                        return;
                    }
                };
                lock(&self.applied).insert(range, entry.index);
                // PROPOSED(D-069): `key` and `effect` (SHARD.md §8).
                let (key, effect) = match command.as_ref().and_then(Command::key) {
                    Some(key) => (Some(key.clone()), ApplyEffect::Applied),
                    None => (None, ApplyEffect::None),
                };
                self.env.trace_decided(
                    took,
                    TraceEvent::RaftApply {
                        server: self.id.0,
                        range: range.get(),
                        index: entry.index,
                        entry_term: entry.term,
                        hash: entry.payload.hash(),
                        key,
                        effect,
                    },
                );
                let waiting = self
                    .replicas
                    .get(&range)
                    .and_then(|replica| lock(replica).pending.remove(&entry.index));
                if let Some(waiting) = waiting
                    && waiting.term == entry.term
                {
                    let response = RangedResponse {
                        range,
                        response: Response {
                            client: waiting.client,
                            seq: waiting.seq,
                            reply: Reply::Outcome(outcome),
                        },
                    };
                    let _ = self.sock.send(waiting.from, response.encode()).await;
                }
                self.local.push(Local::Applied {
                    range,
                    index: entry.index,
                });
            }
        })
    }
}

/// The node's clock in nanoseconds, where the cores' lease arithmetic reads it.
fn now_nanos<E: Environment>(env: &E) -> u64 {
    env.clock().now().as_nanos()
}

/// Runs one node until its socket closes or its disk fails under it. Spawn it with
/// `Environment::spawn`; spawn it again after a crash to restart the node on what its
/// disk kept, as a server is restarted today.
///
/// # Errors
///
/// The bind, the start, or an I/O error while running; each is traced before it is
/// returned. A store refused for lost state marks the loss (D-044) and stops this
/// node: the whole-node refusal and its re-seed are their own slice's (Q15).
pub async fn run<E: Environment>(env: E, config: ServerConfig) -> io::Result<()> {
    let ServerConfig {
        id,
        listen,
        servers,
        ranges,
        initial_voters,
        raft,
        engine,
        inbox_bytes,
        node: node_variants,
    } = config;
    let server = id.0;
    let variants = raft.variants;
    // The store's recovery must never hand back a state with a hole (RAFT.md §3).
    let engine = EngineConfig {
        allow_manifest_fallback: false,
        allow_head_gap: false,
        refuse_log_damage: true,
        quiesce_on_loss: !variants.contains(Variant::RefusalNotDurable),
        ..engine
    };
    let sock = Arc::new(env.net().bind(listen).await?);
    let addrs: BTreeMap<ServerId, SocketAddr> = servers.iter().copied().collect();
    let inbox = Arc::new(Inbox::new(inbox_bytes));
    let local: Queue<Local> = Queue::new();

    // The `net` task: one socket for the whole node. A frame carries messages of
    // several ranges, each tagged with its own (Q10, D-072); a client's packet
    // carries the range its key belongs to.
    env.spawn("net", {
        let env = env.clone();
        let sock = sock.clone();
        let inbox = inbox.clone();
        let local = local.clone();
        async move {
            loop {
                let Ok((from, bytes)) = sock.recv().await else {
                    break;
                };
                if is_ranged(&bytes) {
                    if let Ok(ranged) = RangedRequest::decode(bytes) {
                        local.push(Local::Request {
                            range: ranged.range,
                            from,
                            request: ranged.request,
                        });
                    }
                    continue;
                }
                let Ok(decoded) = decode(&bytes) else {
                    continue;
                };
                for tagged in decoded.messages {
                    let admission = inbox.admit(Received {
                        range: tagged.range,
                        from: tagged.frame.from,
                        message: tagged.frame.message,
                        bytes: tagged.bytes,
                    });
                    for dropped in admission.dropped() {
                        env.trace(TraceEvent::RaftInboxDropped {
                            server,
                            range: dropped.range.get(),
                            kind: dropped.message.kind(),
                        });
                    }
                }
            }
            inbox.close();
        }
    });

    // The start: one engine, and a store per range under its own prefix (§2, §4).
    let first = ranges
        .first()
        .ok_or_else(|| io::Error::other("a node hosts at least one range"))?;
    let started = start_store(
        &env,
        server,
        &engine,
        variants,
        &KeyPrefix::group(first.id.get()),
        StartOrder::Correct,
    )
    .await;
    let (first_store, first_recovered) = match started {
        Start::Failed(error) => {
            env.trace(TraceEvent::RaftServerFailed {
                server,
                reason: error.to_string(),
            });
            return Err(error);
        }
        Start::Refused(error) => {
            // D-044: the loss is marked in the store directory before anything
            // else. Q15's whole-node re-seed is its own slice's; this node stops.
            let reason = error.to_string();
            mark_store_lost(&env, &engine.dir, &reason).await?;
            env.trace(TraceEvent::RaftRefused { server, reason });
            return Err(error);
        }
        Start::Opened {
            store, recovered, ..
        } => (store, recovered),
    };

    let mut stores: BTreeMap<RangeId, Arc<RaftStore<E>>> = BTreeMap::new();
    let mut opened = vec![(first.clone(), Arc::new(first_store), first_recovered)];
    for range in ranges.iter().skip(1) {
        let (store, recovered) = opened[0]
            .1
            .open_sibling(KeyPrefix::group(range.id.get()))
            .await?;
        opened.push((range.clone(), Arc::new(store), recovered));
    }

    let mut cores = Cores::new(node_variants);
    let mut replicas: BTreeMap<RangeId, Arc<Mutex<Replica>>> = BTreeMap::new();
    let mut applied_at: BTreeMap<RangeId, Index> = BTreeMap::new();
    for (range, store, recovered) in opened {
        let id_of = range.id;
        stores.insert(id_of, store.clone());
        replicas.insert(id_of, Arc::new(Mutex::new(Replica::new())));
        // Q13, D-057: each core seeded from `n{id}/r{range}/protocol`, so two ranges
        // on one node draw different election timeouts.
        let seed = env.range_rng(id_of.get()).next_u64();
        let snapshot_record = recovered.snapshot.clone();
        let (snap_index, snap_term) = snapshot_record
            .as_ref()
            .map_or((0, 0), |record| (record.last_index, record.last_term));
        let fresh = recovered.log.is_empty()
            && snapshot_record.is_none()
            && store.applied() == 0
            && store.term() == 0
            && store.vote().is_none()
            && !recovered.quarantined;
        // Every trace event a core emits carries the range it replicates (D-069),
        // which is this replica's and not the one group's default.
        let core_config = RaftConfig {
            range: id_of.get(),
            ..raft.clone()
        };
        let mut core = Raft::restore_compacted(
            id,
            Configuration::of(&initial_voters),
            core_config,
            seed,
            store.term(),
            store.vote(),
            snap_index,
            snap_term,
            snapshot_record.as_ref().map(|record| record.config.clone()),
            recovered.log,
            recovered.quarantined,
        );
        let applied = store.applied();
        core.step(Input::Applied(applied));
        applied_at.insert(id_of, applied);
        if fresh {
            // §2 generalised: the ranges configuration fixes, created at the node's
            // first start, each traced with the span, generation and voters
            // configuration gave it (§8). Nothing routes by any of it.
            env.trace(TraceEvent::RangeCreated {
                range: id_of.get(),
                cause: RangeCause::Bootstrap,
                parent: None,
                start: range.start.clone(),
                end: range.end.clone(),
                generation: 1,
                voters: initial_voters.iter().map(|voter| voter.0).collect(),
                floor_index: 0,
                floor_term: 0,
                incarnation: store.incarnation(),
            });
        }
        restate(&env, server, id_of, &core, &store, snap_index, snap_term);
        cores.insert(id_of, core);
    }

    let host = ServerHost {
        env: env.clone(),
        id,
        sock: sock.clone(),
        addrs,
        stores: stores.clone(),
        replicas: replicas.clone(),
        jobs: Queue::new(),
        answers: Queue::new(),
        variants,
        gaps: Mutex::new(Gaps::default()),
    };
    let answers = host.answers.clone();
    env.spawn("answers", {
        let sock = sock.clone();
        let answers = answers.clone();
        async move {
            while let Some((to, bytes)) = answers.pop().await {
                let _ = sock.send(to, bytes).await;
            }
        }
    });
    let jobs = host.jobs.clone();
    env.spawn("apply", {
        let applier = ServerApplier {
            env: env.clone(),
            id,
            sock: sock.clone(),
            stores,
            replicas,
            local: local.clone(),
            applied: Mutex::new(applied_at),
        };
        let jobs = jobs.clone();
        async move {
            apply(&jobs, &applier).await;
        }
    });

    let mut node = Node::new(
        env.clone(),
        TaskConfig {
            id,
            tick: Duration::from_nanos(raft.tick_nanos),
            variants: node_variants,
        },
        cores,
        Outbox::new(),
        host,
    );
    let ran = node.raft(&inbox, &local).await;
    jobs.close();
    answers.close();
    ran
}

/// The restatement of one replica at the node's start (RAFT.md §2): the log as the
/// disk holds it, the snapshot it records, the configuration in force and the term,
/// each naming the range (§8).
fn restate<E: Environment>(
    env: &E,
    server: u64,
    range: RangeId,
    core: &Raft,
    store: &RaftStore<E>,
    snap_index: Index,
    snap_term: Term,
) {
    env.trace(TraceEvent::RaftTruncate {
        server,
        range: range.get(),
        from_index: core.last_index() + 1,
    });
    if snap_index > 0 {
        env.trace(TraceEvent::RaftSnapshot {
            server,
            range: range.get(),
            last_index: snap_index,
            last_term: snap_term,
            taken: false,
        });
    }
    for entry in core.log() {
        env.trace(TraceEvent::RaftAppend {
            server,
            range: range.get(),
            index: entry.index,
            entry_term: entry.term,
            hash: entry.payload.hash(),
        });
    }
    let membership = core.membership();
    env.trace(TraceEvent::RaftConfig {
        server,
        range: range.get(),
        index: core.membership_index(),
        old: membership.voters.iter().map(|voter| voter.0).collect(),
        new: membership
            .new_voters
            .as_ref()
            .map(|new| new.iter().map(|voter| voter.0).collect())
            .unwrap_or_default(),
        joint: membership.new_voters.is_some(),
        learners: membership
            .learners
            .iter()
            .map(|learner| learner.0)
            .collect(),
    });
    env.trace(TraceEvent::RaftRecovered {
        server,
        range: range.get(),
        term: core.term(),
        applied: store.applied(),
        last_index: core.last_index(),
        incarnation: store.incarnation(),
    });
    env.trace(TraceEvent::RaftTerm {
        server,
        range: range.get(),
        term: core.term(),
        role: "follower",
        received: None,
    });
}

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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::{ApplyEffect, FileSystem, RangeCause, RecoveredAs};
use ananke_env::{Clock, Decision, Environment, Network, Rng, Socket, TraceEvent};
use ananke_raft::apply::{Command, Outcome, apply_command, user_key};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, SnapshotAction, Variant, Variants};
use ananke_raft::node::{Start, StartOrder, start_store};
use ananke_raft::queue::Queue;
use ananke_raft::snapshot::Repair;
use ananke_raft::store::{
    FIRST_INCARNATION, KeyPrefix, PURPOSE_CONFIG, PURPOSE_LOG, PURPOSE_META, RaftStore, Recovered,
    SnapshotRecord, is_marked_lost, mark_store_lost,
};
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId, Term};
use ananke_raft::{Input, Message, Persist, Raft};
use ananke_storage::EngineConfig;
use bytes::Bytes;

use crate::client::{RangedRequest, RangedResponse, is_ranged};
use crate::frame::decode;
use crate::inbox::{Inbox, Received};
use crate::install::{self, SnapAnswer, SnapJob};
use crate::node::{
    Applier, ApplyJob, ApplyWork, Boxed, BoxedPersist, CoreWork, Host, Node,
    NodeConfig as TaskConfig, apply,
};
use crate::outbox::Outbox;
use crate::range::RangeId;
use crate::reseed::{Candidate, generation_of, newest_not_lost, reseed_dir};
use crate::round::Cores;
use crate::snapshot::{self, Identity, Snapshots};
use crate::variant::{NodeVariant, NodeVariants};

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
    /// Q14's per-node cap on snapshot streams *received and assembled*. Nothing caps
    /// the streams a leader sends (D-043).
    ///
    /// D-075 fixes no default on purpose — the re-seed shape sets it to two, below its
    /// four ranges, so re-seeds toward one node wait for one another, and a wrong
    /// default would be invisible. A scenario that is not about the cap sets it at or
    /// above the node's range count, so no stream waits by accident.
    // PROPOSED(D-075): the receive cap is a node setting with no default.
    pub snapshot_cap: usize,
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
    /// A read, by the id the core was given. A core that leads answers it later,
    /// through `read_ready` or `read_dropped`, and the entry in [`Replica::reads`]
    /// is taken there; a core that does not lead answers [`Output::Rejected`] and
    /// nothing else, so the id it refused is only knowable here — and without it
    /// the entry would be left behind for the life of the node.
    ///
    /// [`Output::Rejected`]: ananke_raft::core::Output::Rejected
    // PROPOSED(D-076): a refused read's registration is taken back at the step.
    Read { id: u64 },
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

    /// Registers a read the core is about to be given and returns the id it is
    /// given under, with that id held as the work in flight for the step.
    fn register_read(&mut self, from: SocketAddr, request: Request) -> u64 {
        let id = self.next_read;
        self.next_read += 1;
        self.reads.insert(id, (from, request));
        self.in_flight = Some(InFlight::Read { id });
        id
    }

    /// Takes back the work in flight because the step refused it.
    ///
    /// `Some` is a client to answer `NotLeader` at once. `None` is a read — whose
    /// registration is taken back here, because the core that refused it will never
    /// name its id again — or a step with nothing in flight.
    // PROPOSED(D-076): a refused read's registration is taken back at the step.
    fn refuse(&mut self) -> Option<(SocketAddr, u64, u64)> {
        match self.in_flight.take()? {
            InFlight::Propose { from, client, seq } | InFlight::Change { from, client, seq } => {
                Some((from, client, seq))
            }
            InFlight::Read { id } => {
                self.reads.remove(&id);
                None
            }
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
    /// The `snapshot` task has something for a range's core, or for the `raft` task
    /// itself (the two halves of a live install).
    // PROPOSED(D-083): the `snapshot` task answers the cores the way the `apply` task
    // does, through the node's one queue of local inputs.
    Snapshot(SnapAnswer),
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
    /// The work the node's `snapshot` task owes: a stream a core asked for, a chunk
    /// the `net` task diverted, the repair for a stream that is ready to switch.
    ///
    /// [`Host::snapshot`] is `&self` and synchronous, which is exactly what a queue
    /// push needs and what an await would not allow — the same reason `jobs` and
    /// `answers` are queues (D-073).
    // PROPOSED(D-083): `Host::snapshot` is a push onto the `snapshot` task's queue.
    snaps: Queue<SnapJob>,
    /// Every range this node hosts, with the ranges configuration fixed at bootstrap:
    /// what a replaced replica is rebuilt against, and what the initial voters are.
    ranges: Vec<Range>,
    initial_voters: Vec<ServerId>,
    raft: RaftConfig,
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

    /// The repair a live install carries in its switch: this replica's own identity,
    /// which the switch must not lose (RAFT.md:225-233, D-066).
    ///
    /// It is built here, in the `raft` task, with the range held and nothing able to
    /// step the core — the same reason the one-group server builds it between a
    /// quiesced `apply` task and the switch (node.rs, `install_decision`).
    // PROPOSED(D-083): the repair is built from the held core, in the `raft` task.
    fn repair(&self, range: RangeId, core: &Raft, last_index: Index, last_term: Term) -> Repair {
        // The tail is kept only where the log agrees with the snapshot at its last
        // index; where it does not, everything the log holds there is from a
        // divergent, uncommitted history (RAFT.md §1).
        let tail = if core.term_at(last_index) == Some(last_term) {
            (last_index + 1..=core.last_index())
                .filter_map(|index| core.entry(index).cloned())
                .collect()
        } else {
            Vec::new()
        };
        Repair {
            term: core.term(),
            vote: core.vote(),
            tail,
            // A quarantined replica stays quarantined across any install on that
            // history: the vote its lost state may have held is still unknown (D-035).
            quarantined: core.quarantined(),
            // An install into a live store keeps its incarnation: the kept tail is
            // everything acknowledged past the snapshot, so nothing a leader matched
            // is lost (D-042).
            incarnation: self
                .stores
                .get(&range)
                .map_or(FIRST_INCARNATION, |store| store.incarnation()),
        }
    }

    /// The replica a live install's switch built: the range's core, restored on the
    /// state the manifest switch made durable.
    ///
    /// A server gets this for free — it ends its run-loop incarnation and reopens its
    /// store (RAFT.md §1) — and a node cannot, because reopening the engine would
    /// restart every range on it (SHARD.md §11, storage 5). So it is built here, for
    /// the one range, out of exactly what the repair wrote: the term and vote this
    /// replica held, the snapshot's index and term, the configuration in force at it,
    /// the kept tail and the quarantine.
    // PROPOSED(D-083): the replica a switch builds replaces the one it replaced.
    fn installed(
        &self,
        range: RangeId,
        core: &Raft,
        at: &Identity,
        config: &Configuration,
    ) -> Option<Raft> {
        let span = self.ranges.iter().find(|r| r.id == range)?;
        let tail: Vec<Entry> = if core.term_at(at.last_index) == Some(at.last_term) {
            (at.last_index + 1..=core.last_index())
                .filter_map(|index| core.entry(index).cloned())
                .collect()
        } else {
            Vec::new()
        };
        // A replica the install *created* — the node held nothing initialised for this
        // range — is a creation, and §8 wants it traced with its cause. On a node whose
        // ranges are fixed at bootstrap this is the re-seed's case: a fresh engine
        // whose replicas are built by the streams that fill it (Q15).
        let created = core.applied() == 0 && core.last_index() == 0 && core.snapshot() == (0, 0);
        // Q13, D-057: the replica draws its election timeout from its own range's
        // stream, as the one this start built did.
        let seed = self.env.range_rng(range.get()).next_u64();
        let restored = Raft::restore_compacted(
            self.id,
            Configuration::of(&self.initial_voters),
            RaftConfig {
                range: range.get(),
                ..self.raft.clone()
            },
            seed,
            core.term(),
            core.vote(),
            at.last_index,
            at.last_term,
            Some(config.clone()),
            tail,
            core.quarantined(),
        );
        if created {
            self.env.trace(TraceEvent::RangeCreated {
                range: range.get(),
                cause: RangeCause::Snapshot,
                parent: None,
                start: span.start.clone(),
                end: span.end.clone(),
                generation: 1,
                voters: config.voters.iter().map(|voter| voter.0).collect(),
                floor_index: at.last_index,
                floor_term: at.last_term,
                incarnation: self
                    .stores
                    .get(&range)
                    .map_or(FIRST_INCARNATION, |store| store.incarnation()),
            });
        }
        if restored.quarantined() {
            // The install kept the quarantine (D-035): a replica whose history was
            // ever re-seeded stays quarantined across any install on it, because the
            // vote its lost state may have held is still unknown.
            self.env.trace(TraceEvent::RaftReseeded {
                server: self.id.0,
                range: range.get(),
            });
        }
        self.env.trace(TraceEvent::RaftRecovered {
            server: self.id.0,
            range: range.get(),
            term: restored.term(),
            applied: at.last_index,
            last_index: restored.last_index(),
            incarnation: self
                .stores
                .get(&range)
                .map_or(FIRST_INCARNATION, |store| store.incarnation()),
            // The install has filled it: a quarantined replica here is one the
            // re-seed marked and the stream then gave state to, which is
            // `Quarantined` and no longer `Refused` (D-035).
            // PROPOSED(D-081): a restatement says how the replica restated (D-067).
            state: if restored.quarantined() {
                RecoveredAs::Quarantined
            } else {
                RecoveredAs::Neither
            },
        });
        Some(restored)
    }
}

impl<E: Environment> Host for ServerHost<E> {
    type Local = Local;

    fn local_range(&self, local: &Self::Local) -> RangeId {
        match local {
            Local::Request { range, .. } | Local::Applied { range, .. } => *range,
            Local::Snapshot(answer) => answer.range(),
        }
    }

    fn local_input(&self, local: Self::Local, core: &Raft) -> Option<Input> {
        let (range, from, request) = match local {
            Local::Applied { index, .. } => return Some(Input::Applied(index)),
            // The four answers that are a step of the range's core, as the `apply`
            // task's `Applied` is. The other three are not inputs at all and were
            // taken by `local_core` before this was reached.
            // PROPOSED(D-083): the `snapshot` task answers the cores through `Local`.
            Local::Snapshot(answer) => {
                return match answer {
                    SnapAnswer::Taken { index, term, .. } => {
                        Some(Input::SnapshotTaken { index, term })
                    }
                    SnapAnswer::Installed {
                        to,
                        index,
                        incarnation,
                        ..
                    } => Some(Input::SnapshotInstalled {
                        to,
                        index,
                        incarnation,
                    }),
                    SnapAnswer::Failed { to, retake, .. } => {
                        Some(Input::SnapshotFailed { to, retake })
                    }
                    SnapAnswer::Acked { to, .. } => Some(Input::SnapshotAcked { to }),
                    SnapAnswer::Ready { .. }
                    | SnapAnswer::Switched { .. }
                    | SnapAnswer::Abandoned { .. } => None,
                };
            }
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
                // A core that does not lead refuses the read with `Rejected` and
                // never names the id again (`core::on_read`), so the id is carried
                // to the step as the work in flight — where the registration is
                // taken back rather than left behind (D-076).
                let id = state.register_read(from, request);
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

    fn local_releases(&self, local: &Self::Local) -> bool {
        matches!(
            local,
            Local::Snapshot(SnapAnswer::Switched { .. } | SnapAnswer::Abandoned { .. })
        )
    }

    fn local_core(&self, local: &Self::Local, core: &Raft) -> CoreWork {
        let Local::Snapshot(answer) = local else {
            return CoreWork::Step;
        };
        match answer {
            // A stream completed. The range is held from here to the switch, and the
            // repair the switch carries is built now, from this core, while nothing
            // can step it (RAFT.md:225-233, D-066).
            SnapAnswer::Ready { range, from, at } => {
                self.snaps.push(SnapJob::Finish {
                    range: *range,
                    from: *from,
                    repair: Box::new(self.repair(*range, core, at.last_index, at.last_term)),
                });
                CoreWork::Hold
            }
            // The switch is durable: this is the replica it built.
            SnapAnswer::Switched { range, at, config } => {
                match self.installed(*range, core, at, config) {
                    Some(installed) => CoreWork::Restore(Box::new(installed)),
                    None => CoreWork::Release,
                }
            }
            SnapAnswer::Abandoned { .. } => CoreWork::Release,
            SnapAnswer::Taken { .. }
            | SnapAnswer::Installed { .. }
            | SnapAnswer::Failed { .. }
            | SnapAnswer::Acked { .. } => CoreWork::Step,
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
            // A read the core took on: it answers it later, through `read_ready` or
            // `read_dropped`, and the registration is taken back there. Taking the
            // slot here is what keeps a refused read's id from outliving its step.
            InFlight::Read { .. } => {}
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
        // A read this replica does not lead takes the `None` arm: the core refused
        // it with `Rejected` and will never name its id again, so `Replica::refuse`
        // takes the registration `local_input` made — nothing else ever would, and a
        // node that refuses reads for a living would otherwise carry a
        // `(SocketAddr, Request)` for every one of them until it stopped.
        //
        // Its client is not answered at the step and waits for its own timeout:
        // answering here queues a packet inside the round, which moves every
        // schedule of the scenario this slice measured. That answer is the next
        // slice's first job (D-076); what is fixed here is the entry left behind,
        // which is a standing failure and not a latency wart.
        let Some((from, client, seq)) = lock(replica).refuse() else {
            return;
        };
        self.answer_later(range, from, client, seq, Reply::NotLeader { leader });
    }

    fn apply(&self, job: ApplyJob) {
        self.jobs.push(job);
    }

    fn snapshot(&self, range: RangeId, action: SnapshotAction) {
        // Still counted, so a scenario that asserts the path's absence keeps its
        // figure; but no longer *only* counted, which is what PR #86 recorded of this
        // node and what issue #96 was the other half of.
        lock(&self.gaps).snapshot_actions += 1;
        if let Some(job) = install::job_of(range, action) {
            self.snaps.push(job);
        }
    }

    fn failed(&self, reason: String) {
        self.env.trace(TraceEvent::RaftServerFailed {
            server: self.id.0,
            reason,
        });
    }
}

/// Where one range's applies stand, as the `apply` task follows them.
///
/// The term and the configuration are followed entry by entry for the same reason the
/// one-group task follows them: a take's record must carry the index, the term at it
/// and the configuration in force *at that index*, and the only place all three are
/// exact is between two applies (RAFT.md §1, D-036). Keeping them per range is what
/// makes that true on a node.
// PROPOSED(D-083): the node's `apply` task carries per range what the one-group task
// carries for its one group.
/// Where one range's applies stand, shared between the node's `apply` task and its
/// `snapshot` task: a live install moves all three without an apply (D-083).
#[derive(Clone, Debug)]
pub struct Applied {
    /// The applied index.
    pub index: Index,
    /// The term of the entry at it.
    pub term: Term,
    /// The configuration in force at it.
    pub config: Configuration,
}

/// The node's `apply` task: every range's entries, one job at a time (Q14, D-036).
struct ServerApplier<E: Environment> {
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    stores: BTreeMap<RangeId, Arc<RaftStore<E>>>,
    replicas: BTreeMap<RangeId, Arc<Mutex<Replica>>>,
    local: Queue<Local>,
    snaps: Queue<SnapJob>,
    ranges: Vec<Range>,
    engine_dir: PathBuf,
    node_variants: NodeVariants,
    /// Shared with the `snapshot` task: a live install moves a range's applied index,
    /// term and configuration without an apply, and both tasks read this.
    // PROPOSED(D-083): a live install moves the apply task's state with it.
    applied: Arc<Mutex<BTreeMap<RangeId, Applied>>>,
}

impl<E: Environment> ServerApplier<E> {
    /// A take of `range` at its applied index, or — with `checkpoint` false — a
    /// follower's compaction record with no checkpoint under it (D-065, D-078).
    ///
    /// The order is RAFT.md §1's: the record first, synced, then the checkpoint. A
    /// crash between them leaves a record naming a version that was never completed,
    /// which only ever fails a stream and never the store — the leader that finds no
    /// complete version asks for a fresh take.
    ///
    /// The checkpoint is of the **range's own key intervals**, not of the engine
    /// directory: one engine holds every range on the node, and a take that copied all
    /// of it would stream three other ranges' state to a follower of this one
    /// ([`NodeVariant::TakeCheckpointsTheWholeNode`]).
    // PROPOSED(D-083): a take checkpoints the range's spans, not the node's store.
    async fn take(&self, range: RangeId, store: &Arc<RaftStore<E>>, checkpoint: bool) {
        // D-047: the take is decided when the task takes the job; the record, the
        // checkpoint and the answer follow.
        let took = self.env.decision();
        let Some(state) = lock(&self.applied).get(&range).cloned() else {
            return self.take_failed(range);
        };
        if state.index == 0 {
            return self.take_failed(range);
        }
        let take = match store.snapshot_record().await {
            Ok(record) => record.map_or(0, |record| record.take) + u64::from(checkpoint),
            Err(_) => return self.take_failed(range),
        };
        let dir = snapshot::version_dir(
            &self.engine_dir,
            range,
            state.index,
            take,
            self.node_variants,
        );
        let record = SnapshotRecord {
            last_index: state.index,
            last_term: state.term,
            config: state.config.clone(),
            dir: if checkpoint {
                dir.display().to_string()
            } else {
                String::new()
            },
            taken: checkpoint,
            take,
        };
        if store.record_snapshot(&record).await.is_err() {
            return self.take_failed(range);
        }
        if checkpoint {
            let Some(spans) = self.checkpoint_spans(range) else {
                return self.take_failed(range);
            };
            let borrowed: Vec<std::ops::Range<&[u8]>> = spans
                .iter()
                .map(|span| &span.start[..]..&span.end[..])
                .collect();
            let taken = if self
                .node_variants
                .contains(NodeVariant::TakeCheckpointsTheWholeNode)
            {
                // The variant: the whole engine directory, as the one-group take does
                // (`Engine::checkpoint`, snapshot.rs:688). On a node that is every
                // range's state, streamed to a follower of one of them.
                store.engine().checkpoint(&dir).await.map(|_| ())
            } else {
                store
                    .engine()
                    .checkpoint_spans(&borrowed, &dir)
                    .await
                    .map(|_| ())
            };
            if taken.is_err() {
                return self.take_failed(range);
            }
            // The checkpoint's own format record, after the engine's checkpoint, which
            // requires an empty directory. A checkpoint is complete only with both its
            // `CURRENT` and this (D-060), and completeness is what a stream opens on:
            // without it every version of this node's looked half-written and no
            // stream would open at all.
            // PROPOSED(D-083): the node's take writes the checkpoint's format record,
            // as a server's does.
            if ananke_raft::format::write_checkpoint_record(&self.env, &dir)
                .await
                .is_err()
            {
                return self.take_failed(range);
            }
            self.env.trace_decided(
                took,
                TraceEvent::RaftSnapshot {
                    server: self.id.0,
                    range: range.get(),
                    last_index: state.index,
                    last_term: state.term,
                    taken: true,
                },
            );
            // What this take put into the checkpoint, read back from the engine, so a
            // check can pair it with the install it feeds and say the bytes that
            // landed are the bytes that were taken. A take that dropped the range's
            // user keys, or carried the leader's log along with them, traces the same
            // `RaftSnapshot` as a correct one (D-083).
            // PROPOSED(D-083): what a take took is read back and traced.
            self.state_of(range, state.index, state.term, store).await;
            // The versions nothing reads any more can go, and only this range's
            // (D-043, D-075).
            self.snaps.push(SnapJob::Taken { range });
        }
        // No `RaftSnapshot` for a record: that event says a snapshot was taken or
        // installed, and a compaction point is neither. Tracing it moved the one-group
        // sweep's count of installs by a factor of four, which is how it was found
        // (node.rs, `Job::Record`).
        self.local.push(Local::Snapshot(SnapAnswer::Taken {
            range,
            index: state.index,
            term: state.term,
        }));
    }

    /// Reads a range's replica back out of the engine and traces what it holds, as the
    /// `snapshot` task does after an install: the two are paired by
    /// `(range, last_index, last_term)`.
    // PROPOSED(D-083): what a take took is read back and traced.
    async fn state_of(
        &self,
        range: RangeId,
        last_index: Index,
        last_term: Term,
        store: &Arc<RaftStore<E>>,
    ) {
        let Some(span) = self.ranges.iter().find(|one| one.id == range) else {
            return;
        };
        let user_span = user_key(&span.start)..user_key(&span.end);
        let version = store.engine().snapshot();
        let Ok(user) = store
            .engine()
            .scan(&user_span.start[..]..&user_span.end[..], &version)
            .await
        else {
            return;
        };
        let log_span = store.prefix().purpose_span(PURPOSE_LOG);
        let log = store
            .engine()
            .scan(&log_span.start[..]..&log_span.end[..], &version)
            .await
            .unwrap_or_default();
        drop(version);
        let digest = user.iter().fold(0u64, |acc, (key, value)| {
            acc.wrapping_add(install::fnv(key).rotate_left(1) ^ install::fnv(value))
        });
        self.env.trace(TraceEvent::RaftSnapshotState {
            server: self.id.0,
            range: range.get(),
            last_index,
            last_term,
            applied: store.applied(),
            user_keys: user.len() as u64,
            user_digest: digest,
            log_keys: log.len() as u64,
        });
    }

    /// A take that could not be made: the core is told, and the next need takes a
    /// fresh version rather than opening one that was never written.
    fn take_failed(&self, range: RangeId) {
        self.local.push(Local::Snapshot(SnapAnswer::Failed {
            range,
            to: self.id,
            retake: true,
        }));
    }

    /// The key intervals a take of `range` copies: its Raft state **except its log**,
    /// and its user keys.
    ///
    /// The log is left out on purpose, and it is the one place this departs from the
    /// one-group take. A snapshot's content is the state machine at an index and the
    /// metadata that describes it; the leader's *log* is neither. The one-group
    /// install streams it and then tombstones every key of it the receiver's kept tail
    /// does not replace (`Assembler::finish`), which makes the receiver responsible for
    /// enumerating what the sender sent. Not sending it reaches the same store — the
    /// install's spans still cover the whole Raft interval, so the switch removes the
    /// receiver's own log, and the repair's tail is what goes back — with fewer bytes
    /// on the wire and one less thing for the two ends to disagree about (D-083).
    // PROPOSED(D-083): the stream carries no log key, so the repair tombstones none.
    fn checkpoint_spans(&self, range: RangeId) -> Option<Vec<std::ops::Range<Bytes>>> {
        let span = self.ranges.iter().find(|r| r.id == range)?;
        let prefix = KeyPrefix::group(range.get());
        if self
            .node_variants
            .contains(NodeVariant::TakeStreamsTheLogToo)
        {
            // The variant: the whole Raft interval, the log purpose included, as the
            // one-group take does. The install then puts the *leader's* log keys into
            // the receiver's store, and the node's repair tombstones none of them
            // because it is built on the promise that none were sent (D-083).
            return Some(vec![
                prefix.span(),
                user_key(&span.start)..user_key(&span.end),
            ]);
        }
        if self
            .node_variants
            .contains(NodeVariant::TakeSkipsTheUserKeys)
        {
            // The variant: the range's Raft state alone. The install's spans still
            // cover the user interval, so the switch removes the receiver's user keys
            // and puts nothing back — the state machine, gone silently.
            return Some(vec![
                prefix.purpose_span(PURPOSE_META),
                prefix.key(PURPOSE_CONFIG, &[])..prefix.span().end,
            ]);
        }
        Some(vec![
            prefix.purpose_span(PURPOSE_META),
            prefix.key(PURPOSE_CONFIG, &[])..prefix.span().end,
            user_key(&span.start)..user_key(&span.end),
        ])
    }
}

impl<E: Environment> Applier for ServerApplier<E> {
    fn run(&self, job: ApplyJob) -> Boxed<'_, ()> {
        Box::pin(async move {
            let ApplyJob { range, work } = job;
            let Some(store) = self.stores.get(&range) else {
                return;
            };
            let entries = match work {
                ApplyWork::Entries(entries) => entries,
                // A take and a follower's compaction record run **here**, between two
                // applies, which is what makes the record exact and what makes D-036's
                // consequence hold: one range's take stalls every range's applies on
                // the node, because there is one `apply` task and it takes one job at
                // a time (Q14).
                // PROPOSED(D-083): the take and the record are the `apply` task's.
                ApplyWork::Take => return self.take(range, store, true).await,
                ApplyWork::Record => return self.take(range, store, false).await,
            };
            for entry in entries {
                // The store's own applied index wins when it is ahead: a live install
                // moved it inside a manifest switch, without an apply (D-083).
                let applied = lock(&self.applied)
                    .get(&range)
                    .map_or(0, |state| state.index)
                    .max(store.applied());
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
                {
                    let mut applied = lock(&self.applied);
                    let state = applied.entry(range).or_insert_with(|| Applied {
                        index: 0,
                        term: 0,
                        config: Configuration::of(&[]),
                    });
                    state.index = entry.index;
                    state.term = entry.term;
                    if let Payload::Config(config) = &entry.payload {
                        state.config = config.clone();
                    }
                }
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
        snapshot_cap,
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
    let snaps: Queue<SnapJob> = Queue::new();
    env.spawn("net", {
        let env = env.clone();
        let sock = sock.clone();
        let inbox = inbox.clone();
        let local = local.clone();
        let snaps = snaps.clone();
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
                    // Issue #96. `Raft::on_message`'s arm for `InstallSnapshot` and
                    // its response is empty, because "the server routes these to it
                    // before the core sees them" — which the one-group server's `net`
                    // loop does and this one did not. Admitted to the inbox, a chunk
                    // cost a heartbeat its place under the byte bound
                    // (`Inbox::carries_data` drops heartbeats to make room for it) and
                    // then vanished at that empty arm. Nothing reached the node before
                    // the wiring, so nothing was lost; the wiring is what makes chunks
                    // arrive, and this is the divert that catches them.
                    //
                    // It also moves where those bytes are counted: a chunk is charged
                    // to the `snapshot` task's receive cap (D-075) and not to the
                    // inbox's bound, which is what the inbox's drops are measured
                    // against.
                    // PROPOSED(D-083): the `net` task diverts snapshot chunks before
                    // the inbox.
                    let diverted = matches!(
                        tagged.frame.message,
                        Message::InstallSnapshot { .. } | Message::InstallSnapshotResponse { .. }
                    ) && !node_variants.contains(NodeVariant::ChunksToTheInbox);
                    if diverted {
                        let range = tagged.range;
                        let from = tagged.frame.from;
                        snaps.push(match tagged.frame.message {
                            message @ Message::InstallSnapshot { .. } => SnapJob::Chunk {
                                range,
                                from,
                                message,
                            },
                            message => SnapJob::Response {
                                range,
                                from,
                                message,
                            },
                        });
                        continue;
                    }
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
    //
    // D-066: the node opens the newest directory not marked lost. Before its first
    // refusal that is the directory configuration named; after one it is the directory
    // the re-seed built, and the refused one is stepped over for the rest of the run.
    let base_dir = engine.dir.clone();
    let present = generations(&env, &base_dir).await?;
    let engine = EngineConfig {
        dir: newest_not_lost(&present, node_variants)
            .map_or_else(|| base_dir.clone(), |candidate| candidate.path.clone()),
        ..engine
    };
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
    let opened: Vec<(Range, Arc<RaftStore<E>>, Recovered)> = match started {
        Start::Failed(error) => {
            env.trace(TraceEvent::RaftServerFailed {
                server,
                reason: error.to_string(),
            });
            return Err(error);
        }
        Start::Refused(error) => {
            // Q15: a loss in the *shared* engine refuses the whole node. The node
            // re-seeds into a fresh engine in a new directory beside the refused one,
            // which stays marked lost and quiesced (§11, storage 8; D-041).
            reseed(
                &env,
                server,
                &base_dir,
                &engine,
                &present,
                &ranges,
                variants,
                node_variants,
                &error,
            )
            .await?
        }
        Start::Opened {
            store, recovered, ..
        } => {
            let mut opened = vec![(first.clone(), Arc::new(store), recovered)];
            for range in ranges.iter().skip(1) {
                let (store, recovered) = opened[0]
                    .1
                    .open_sibling(KeyPrefix::group(range.id.get()))
                    .await?;
                opened.push((range.clone(), Arc::new(store), recovered));
            }
            opened
        }
    };

    let mut stores: BTreeMap<RangeId, Arc<RaftStore<E>>> = BTreeMap::new();
    let mut cores = Cores::new(node_variants);
    let mut replicas: BTreeMap<RangeId, Arc<Mutex<Replica>>> = BTreeMap::new();
    let mut applied_at: BTreeMap<RangeId, Applied> = BTreeMap::new();
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
        let recovered_quarantined = recovered.quarantined;
        // A re-seeded replica is not fresh: its `RangeCreated` is the install's, with
        // `cause: snapshot`, and not a bootstrap's (§8; Q15).
        let fresh = recovered.log.is_empty()
            && snapshot_record.is_none()
            && store.applied() == 0
            && store.term() == 0
            && store.vote().is_none()
            && !recovered_quarantined;
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
        applied_at.insert(
            id_of,
            Applied {
                index: applied,
                term: core.term_at(applied).unwrap_or(snap_term),
                config: core.applied_membership(),
            },
        );
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
        restate(
            &env,
            server,
            id_of,
            &core,
            &store,
            (snap_index, snap_term),
            recovered_quarantined,
        );
        cores.insert(id_of, core);
    }

    let applied_at = Arc::new(Mutex::new(applied_at));
    let host = ServerHost {
        env: env.clone(),
        id,
        sock: sock.clone(),
        addrs: addrs.clone(),
        stores: stores.clone(),
        replicas: replicas.clone(),
        jobs: Queue::new(),
        answers: Queue::new(),
        snaps: snaps.clone(),
        ranges: ranges.clone(),
        initial_voters: initial_voters.clone(),
        raft: raft.clone(),
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
            stores: stores.clone(),
            replicas,
            local: local.clone(),
            snaps: snaps.clone(),
            ranges: ranges.clone(),
            engine_dir: engine.dir.clone(),
            node_variants,
            applied: applied_at.clone(),
        };
        let jobs = jobs.clone();
        async move {
            apply(&jobs, &applier).await;
        }
    });

    // The fourth task, on a fourth handle of the one socket — not a second bind, which
    // would give it a different address and make it a different peer (D-082). It owns
    // the node's `Snapshots` as its planner and keeps the I/O the planner has none of.
    // PROPOSED(D-083): `ananke_shard::snapshot` is run inside the node's server.
    let mut plan = Snapshots::new(engine.dir.clone(), id, snapshot_cap, node_variants);
    for range in &ranges {
        plan.host(
            range.id,
            vec![
                KeyPrefix::group(range.id.get()).span(),
                user_key(&range.start)..user_key(&range.end),
            ],
        );
    }
    env.spawn("snapshot", {
        let mut task = install::Task::new(
            env.clone(),
            id,
            sock.clone(),
            addrs,
            stores,
            raft.clone(),
            node_variants,
            engine.dir.clone(),
            ranges.clone(),
            applied_at.clone(),
            plan,
            local.clone(),
            snaps.clone(),
        );
        async move {
            task.run().await;
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
    snaps.close();
    ran
}

/// Every engine directory of this node beside `base`, with its marker read (D-066).
///
/// The parent is listed rather than the node remembering what it opened, because the
/// node that has to find the directory may be a *restart* — it remembers nothing — and
/// because that is the only reading that survives a crash between a re-seed creating a
/// directory and anything in it becoming durable.
///
/// A parent with no listing at all is a node starting on an empty disk: no candidates,
/// and the caller opens the directory configuration named.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
async fn generations<E: Environment>(env: &E, base: &Path) -> io::Result<Vec<Candidate>> {
    let Some(parent) = base.parent() else {
        return Ok(Vec::new());
    };
    let listed = match env.fs().read_dir(parent).await {
        Ok(listed) => listed,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut candidates = Vec::new();
    for entry in listed {
        // A listing hands back names, not paths: the parent is put back on, or the
        // marker below would be read from wherever a relative name happened to land.
        let Some(name) = entry
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
        else {
            continue;
        };
        let Some(generation) = generation_of(base, &name) else {
            continue;
        };
        let path = parent.join(&name);
        candidates.push(Candidate {
            lost: is_marked_lost(env, &path).await?,
            generation,
            path,
        });
    }
    candidates.sort_by_key(|candidate| candidate.generation);
    Ok(candidates)
}

/// Q15's whole-node re-seed: the refusal of every replica the node holds, and the fresh
/// engine it rebuilds them in (SHARD.md §11, storage 8; §12's "A loss in the shared
/// engine").
///
/// The order is the whole of it, and each step is here because a crash between two of
/// them must leave something a restart can read:
///
/// 1. **The refused directory is marked lost, synced, before anything else** (D-044).
///    A crash before this leaves a directory whose store lost state and whose marker
///    does not say so, which the next start would open as a store.
/// 2. **`RaftRefused` for the node, then one `RaftReplicaRefused` for each range it
///    holds.** The node is refused whole: one engine (Q2), so the loss is every
///    replica's, and the per-replica events are what a reader takes "the ranges this
///    node held" from. They are traced *here*, before the new engine exists, because
///    that is the only moment at which the answer is known from configuration alone —
///    the store that could have been asked is the one that just refused.
/// 3. **The fresh engine is opened at once, in a new directory beside the refused
///    one**, whose generation steps over every generation present (D-041): the refused
///    directory stays marked lost and quiesced for the rest of the run, and no range's
///    re-seed can install into it.
/// 4. **Each range's store is created in the new engine, and its durable refused mark
///    is written before that replica serves anything.** A store created in a fresh
///    engine opens as a first start — quarantine clear, incarnation 1 — and a replica
///    that serves in that state votes again on state its node lost (D-035) and lets
///    its leader keep a `matched` the rebuilt log cannot honour (D-042). The mark is
///    the two keys in one synced batch (`RaftStore::mark_reseeded`), and `RaftReseeded`
///    is traced when it is durable, per replica and carrying its range (§8).
///
/// The incarnation is drawn per replica from the **node's** generator and is never the
/// first incarnation. It is not drawn from the range's protocol stream: `SimEnv` derives
/// a named stream from the seed and the name alone, so a replica created again for the
/// same (range, node) would draw its predecessor's number and a leader comparing
/// incarnations for inequality only (D-042) would never reset (Q26,
/// SHARD.md:1372-1379). [`NodeVariant::IncarnationPerRangeStream`] is that mistake.
///
/// What the replicas are when this returns is empty and quarantined: each waits for its
/// leader's stream, and the install that fills it is the node's snapshot *wiring*, which
/// this slice does not build (see the entry). The state they wait in is the state the
/// install expects, and it is durable before any of them answers anything.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
// The arguments are the node's start in full — its configuration, its variants, the
// directory it refused and what stands beside it — and bundling them into a struct
// used at one call site would hide what the re-seed reads, not clarify it.
#[allow(clippy::too_many_arguments)]
async fn reseed<E: Environment>(
    env: &E,
    server: u64,
    base_dir: &Path,
    refused: &EngineConfig,
    present: &[Candidate],
    ranges: &[Range],
    variants: Variants,
    node_variants: NodeVariants,
    error: &io::Error,
) -> io::Result<Vec<(Range, Arc<RaftStore<E>>, Recovered)>> {
    // 1. D-044: the loss is marked in the store directory before anything else.
    let reason = error.to_string();
    mark_store_lost(env, &refused.dir, &reason).await?;
    env.trace(TraceEvent::RaftRefused { server, reason });

    // 2. Every replica the node holds is refused with it (Q2, Q15). Under
    //    `RefuseOneRangeOnly` only the range whose store failed to open is, which is
    //    the whole of Q15 got wrong: the node's other replicas keep serving over an
    //    engine that lost state.
    let refused_ranges: Vec<&Range> = if node_variants.contains(NodeVariant::RefuseOneRangeOnly) {
        ranges.first().into_iter().collect()
    } else {
        ranges.iter().collect()
    };
    for range in &refused_ranges {
        env.trace(TraceEvent::RaftReplicaRefused {
            server,
            range: range.id.get(),
        });
    }

    // 3. A fresh engine in a NEW directory beside the refused one, opened at once.
    let fresh_dir = reseed_dir(base_dir, &refused.dir, present, node_variants);
    let fresh = EngineConfig {
        dir: fresh_dir,
        ..refused.clone()
    };
    let first = ranges
        .first()
        .ok_or_else(|| io::Error::other("a node hosts at least one range"))?;
    let started = start_store(
        env,
        server,
        &fresh,
        variants,
        &KeyPrefix::group(first.id.get()),
        StartOrder::Correct,
    )
    .await;
    let (first_store, first_recovered) = match started {
        Start::Opened {
            store, recovered, ..
        } => (store, recovered),
        // A fresh directory that refuses or fails is not something a second re-seed
        // can mend: the node stops, and says which of the two it was.
        Start::Refused(error) | Start::Failed(error) => {
            env.trace(TraceEvent::RaftServerFailed {
                server,
                reason: format!("the re-seed's fresh engine: {error}"),
            });
            return Err(error);
        }
    };

    // 4. Each range's store, its durable refused mark written before it serves.
    let mut opened: Vec<(Range, Arc<RaftStore<E>>, Recovered)> = Vec::with_capacity(ranges.len());
    let mut first_opened = Some((first_store, first_recovered));
    for range in ranges {
        // The first range's store is the one the fresh engine was opened with; every
        // other is a sibling prefix in that same engine (§2, §4).
        let (mut store, mut recovered) = match first_opened.take() {
            Some(opened) => opened,
            None => {
                opened[0]
                    .1
                    .open_sibling(KeyPrefix::group(range.id.get()))
                    .await?
            }
        };
        if refused_ranges.iter().any(|refused| refused.id == range.id) {
            let incarnation = if node_variants.contains(NodeVariant::IncarnationPerRangeStream) {
                env.range_rng(range.id.get()).next_u64()
            } else {
                env.rng().next_u64()
            }
            .max(FIRST_INCARNATION + 1);
            if !node_variants.contains(NodeVariant::ServeBeforeRefusedMark) {
                // D-067: `ReseedMarkNotSynced` writes the same two keys, traces the
                // same event and answers as the correct node would; the batch is
                // simply not synced, so a crash before anything else syncs this
                // engine's log may leave the replica with no mark at all.
                // PROPOSED(D-081): the re-seed shape's variant.
                let synced = !node_variants.contains(NodeVariant::ReseedMarkNotSynced);
                store.mark_reseeded(incarnation, synced).await?;
                recovered.quarantined = true;
            }
        }
        opened.push((range.clone(), Arc::new(store), recovered));
    }
    Ok(opened)
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
    snapshot: (Index, Term),
    quarantined: bool,
) {
    let (snap_index, snap_term) = snapshot;
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
    // The durable per-replica refused mark, restated per replica and carrying its range
    // (§8). A replica running on a store a re-seed rebuilt says so at every restatement,
    // as the one-group server does (node.rs:963-968): it replicates, applies and counts
    // for commit majorities, and grants no vote, no pre-vote and no lease promise for
    // the rest of its life on that store. It is traced here on a restart, where the
    // quarantine flag is what the disk held; at the re-seed itself this same
    // restatement is the replica's first, and the mark was made durable before it
    // (`reseed`). One-group `run` traces it in exactly this position, so the two
    // restatements stay comparable.
    //
    // D-083 reached the same restatement from the other side — the node emitted this
    // event for no replica at all, as the one-group server does (node.rs:975-980) — and
    // both now rest on this line. The flag here is `recovered.quarantined`, which is
    // also what the core was restored with, so `core.quarantined()` would read the same.
    // PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
    // PROPOSED(D-083): the node states a quarantined replica as a server does.
    if quarantined {
        env.trace(TraceEvent::RaftReseeded {
            server,
            range: range.get(),
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
        // What the disk said this replica is (D-067). The mark Q15 writes is the
        // quarantine flag and a fresh incarnation (D-077), so a marked replica with
        // nothing installed yet is one waiting for its re-seed — `Refused` — and a
        // marked replica that holds a snapshot has had its stream — `Quarantined`.
        // The two are one flag and two moments, and (d) is about the first of them.
        // PROPOSED(D-081): a restatement says how the replica restated (D-067).
        state: if !quarantined {
            RecoveredAs::Neither
        } else if snap_index > 0 {
            RecoveredAs::Quarantined
        } else {
            RecoveredAs::Refused
        },
    });
    env.trace(TraceEvent::RaftTerm {
        server,
        range: range.get(),
        term: core.term(),
        role: "follower",
        received: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_addr() -> SocketAddr {
        "10.0.0.9:7000".parse().expect("an address")
    }

    fn read(seq: u64) -> Request {
        Request {
            client: 1,
            seq,
            command: Command::Get {
                key: Bytes::from_static(b"k0"),
            },
        }
    }

    /// A read a replica refuses leaves nothing behind (D-076).
    ///
    /// `local_input` registers every read it hands a core, because a core that
    /// leads answers it later by id and the answer has to find the client again. A
    /// core that does **not** lead answers `Output::Rejected` and never names the id
    /// (`core::on_read`): no `ReadDropped` follows, so `answer_read` — the only other
    /// place a registration is taken back — is never reached for it. The step's own
    /// refusal is therefore the last moment the id is known, and taking the
    /// registration there is what keeps a follower's `reads` map from growing for
    /// the life of the node.
    ///
    /// The pair (CLAUDE.md:52-57) is the second half: the replica as it was, which
    /// registered the read and held nothing in flight, leaves an entry behind for
    /// every read it refuses — and this same check sees it.
    #[test]
    fn a_read_a_replica_refuses_leaves_nothing_behind() {
        let mut replica = Replica::new();
        for seq in 0..8 {
            let id = replica.register_read(client_addr(), read(seq));
            assert_eq!(id, seq, "the ids are handed out in order");
            assert!(
                replica.reads.contains_key(&id),
                "a read is registered while the core has it"
            );
            assert!(
                replica.refuse().is_none(),
                "a refused read has no client to answer at the step"
            );
            assert!(
                replica.reads.is_empty(),
                "the refused read {id} was left behind: {:?}",
                replica.reads.keys().collect::<Vec<_>>()
            );
        }
        // A refused proposal still names its client, and leaves nothing either.
        replica.in_flight = Some(InFlight::Propose {
            from: client_addr(),
            client: 1,
            seq: 9,
        });
        assert_eq!(replica.refuse(), Some((client_addr(), 1, 9)));
        assert!(replica.in_flight.is_none());
        // The known-buggy replica, beside it: the read registered with nothing in
        // flight, which is what this node did until the review of D-076.
        let mut buggy = Replica::new();
        for seq in 0..8 {
            let id = buggy.next_read;
            buggy.next_read += 1;
            buggy.reads.insert(id, (client_addr(), read(seq)));
            buggy.in_flight = None;
            assert!(buggy.refuse().is_none());
        }
        assert_eq!(
            buggy.reads.len(),
            8,
            "the buggy replica is the one that keeps every read it refuses"
        );
    }

    /// A read the core *takes on* is answered by id later, so the step leaves the
    /// registration alone: what `local_stepped` takes is the slot, not the entry.
    #[test]
    fn a_read_the_core_takes_on_keeps_its_registration() {
        let mut replica = Replica::new();
        let id = replica.register_read(client_addr(), read(0));
        // The step accepted it: `local_stepped`'s `Read` arm takes the slot and
        // nothing else.
        let taken = replica.in_flight.take();
        assert!(matches!(taken, Some(InFlight::Read { id: took }) if took == id));
        assert!(
            replica.reads.contains_key(&id),
            "the read is still waiting on the core"
        );
        // `answer_read` takes it when the answer comes.
        assert!(replica.reads.remove(&id).is_some());
        assert!(replica.reads.is_empty());
    }
}

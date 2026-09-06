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
    /// Which core to run.
    pub variant: Variant,
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
            variant: Variant::Correct,
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
    /// The server applied every entry through `index`.
    Applied(Index),
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
    /// Whether the follower answered since the last quorum check.
    active: bool,
    /// The drift guard.
    guard: Guard,
}

/// One server's protocol state.
#[derive(Clone, Debug)]
pub struct Raft {
    id: ServerId,
    config: RaftConfig,
    membership: Configuration,
    term: Term,
    vote: Option<ServerId>,
    role: Role,
    leader: Option<ServerId>,
    /// The log, index `i` at position `i - 1`.
    log: Vec<Entry>,
    commit: Index,
    applied: Index,
    /// Votes or pre-votes granted in the round under way, this server included.
    granted: Vec<ServerId>,
    progress: BTreeMap<ServerId, Progress>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
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
    /// The step's log changes.
    truncate_from: Option<Index>,
    appended: Vec<Entry>,
}

impl Raft {
    /// A fresh server: term 0, no vote, an empty log, a follower.
    #[must_use]
    pub fn new(id: ServerId, membership: Configuration, config: RaftConfig, seed: u64) -> Self {
        Self::restore(id, membership, config, seed, 0, None, Vec::new())
    }

    /// A server with the persistent state its store held.
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
        let mut raft = Self {
            id,
            config,
            membership,
            term,
            vote,
            role: Role::Follower,
            leader: None,
            log,
            commit: 0,
            applied: 0,
            granted: Vec::new(),
            progress: BTreeMap::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            quorum_elapsed: 0,
            first_of_term: 0,
            reads: Vec::new(),
            transferee: None,
            transfer: false,
            rng: seed | 1,
            outputs: Vec::new(),
            hard_state_changed: false,
            truncate_from: None,
            appended: Vec::new(),
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

    /// The log.
    #[must_use]
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    /// The last index, 0 for an empty log.
    #[must_use]
    pub fn last_index(&self) -> Index {
        self.log.len() as Index
    }

    /// The last entry's term, 0 for an empty log.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }

    /// The term of the entry at `index`: 0 at index 0, none past the log.
    #[must_use]
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.log.get(index as usize - 1).map(|e| e.term)
    }

    /// The entry at `index`, if the log has it.
    #[must_use]
    pub fn entry(&self, index: Index) -> Option<&Entry> {
        if index == 0 {
            return None;
        }
        self.log.get(index as usize - 1)
    }

    /// The membership.
    #[must_use]
    pub fn membership(&self) -> &Configuration {
        &self.membership
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
            Input::Read { id, now } => self.on_read(id, now),
            Input::Applied(index) => {
                self.applied = self.applied.max(index);
                self.serve_reads();
            }
            Input::Message { from, message, now } => self.on_message(from, message, now),
        }
        self.finish()
    }

    /// When the lease ends, by this leader's clock: the promise of the majority
    /// that expires latest, this server counted in. Zero for no lease.
    #[must_use]
    pub fn lease_end(&self) -> u64 {
        let voters = self.voters();
        let needed = voters.len() / 2 + 1;
        if needed <= 1 {
            return u64::MAX;
        }
        let trusts_all = self.config.variant == Variant::LeaseTrustsTheClock;
        let mut promises: Vec<u64> = self
            .progress
            .iter()
            .filter(|(id, p)| voters.contains(id) && (trusts_all || p.guard.trusted))
            .filter_map(|(_, p)| p.promise)
            .collect();
        promises.sort_unstable_by(|a, b| b.cmp(a));
        promises
            .get(needed - 2)
            .map_or(0, |&sent| sent + self.config.lease_span_nanos())
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
            for peer in self.peers() {
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
        if to == self.id || !self.progress.contains_key(&to) {
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
    /// with vote requests marked as the leader's wish.
    fn on_timeout_now(&mut self, from: ServerId) {
        if self.role == Role::Leader || self.leader != Some(from) {
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
        if self.hard_state_changed || self.truncate_from.is_some() || !self.appended.is_empty() {
            out.push(Output::Persist(Persist {
                term: self.term,
                vote: self.vote,
                truncate_from: self.truncate_from.take(),
                append: std::mem::take(&mut self.appended),
            }));
            self.hard_state_changed = false;
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
        self.drop_reads();
        self.set_role(Role::Follower);
    }

    fn voters(&self) -> Vec<ServerId> {
        self.membership.members()
    }

    fn peers(&self) -> Vec<ServerId> {
        self.voters()
            .into_iter()
            .filter(|&s| s != self.id)
            .collect()
    }

    fn on_tick(&mut self) {
        self.election_elapsed += 1;
        self.heartbeat_elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    for peer in self.peers() {
                        self.replicate(peer, true);
                    }
                }
                // Check quorum (RAFT.md §1): a leader that heard from no majority
                // within the minimum election timeout has lost its followers to
                // another leader or a partition, and stops serving.
                self.quorum_elapsed += 1;
                if self.quorum_elapsed >= self.config.election_ticks.0 {
                    self.quorum_elapsed = 0;
                    let mut heard: Vec<ServerId> = self
                        .progress
                        .iter()
                        .filter(|(_, p)| p.active)
                        .map(|(&s, _)| s)
                        .collect();
                    heard.push(self.id);
                    for progress in self.progress.values_mut() {
                        progress.active = false;
                    }
                    if !self.membership.has_majority(&heard) {
                        self.trace(TraceEvent::RaftQuorumLost {
                            server: self.id.0,
                            term: self.term,
                        });
                        self.become_follower(self.term, None);
                    }
                }
            }
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                if self.election_elapsed >= self.election_timeout {
                    if self.config.variant == Variant::NoPreVote {
                        self.become_candidate();
                    } else {
                        self.become_pre_candidate();
                    }
                }
            }
        }
    }

    /// Starts a pre-vote round (thesis §9.6): no term change until a majority says
    /// an election would succeed.
    fn become_pre_candidate(&mut self) {
        self.reset_election_timer();
        self.leader = None;
        self.granted = vec![self.id];
        self.set_role(Role::PreCandidate);
        if self.membership.has_majority(&self.granted) {
            self.become_candidate();
            return;
        }
        let message = Message::PreVote {
            term: self.term + 1,
            last_index: self.last_index(),
            last_term: self.last_term(),
        };
        for peer in self.peers() {
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
        if self.membership.has_majority(&self.granted) {
            self.become_leader();
            return;
        }
        let message = Message::RequestVote {
            term: self.term,
            last_index: self.last_index(),
            last_term: self.last_term(),
            transfer: self.transfer,
        };
        for peer in self.peers() {
            self.send(peer, message.clone());
        }
    }

    /// Takes the lead: a no-op for the term (thesis §6.4), progress for every peer,
    /// and the first round of AppendEntries.
    fn become_leader(&mut self) {
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.quorum_elapsed = 0;
        self.granted.clear();
        self.transfer = false;
        self.transferee = None;
        self.set_role(Role::Leader);
        let last = self.last_index();
        self.first_of_term = last + 1;
        self.progress = self
            .peers()
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
                        guard: Guard::default(),
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
        for peer in self.peers() {
            self.replicate(peer, true);
        }
    }

    /// Appends entries to the local log and to the step's persist.
    fn append_local(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            debug_assert_eq!(entry.index, self.last_index() + 1);
            self.trace(TraceEvent::RaftAppend {
                server: self.id.0,
                index: entry.index,
                entry_term: entry.term,
                hash: entry.payload.hash(),
            });
            self.log.push(entry.clone());
            self.appended.push(entry);
        }
    }

    /// Removes entries from `from` on, in the log and in the step's persist.
    fn truncate(&mut self, from: Index) {
        if from > self.last_index() {
            return;
        }
        self.log.truncate(from as usize - 1);
        self.appended.retain(|e| e.index < from);
        self.truncate_from = Some(self.truncate_from.map_or(from, |f| f.min(from)));
        self.trace(TraceEvent::RaftTruncate {
            server: self.id.0,
            from_index: from,
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
        for peer in self.peers() {
            self.replicate(peer, false);
        }
        // A leader of one commits alone.
        self.maybe_commit();
    }

    /// Sends `to` what it has not got, up to the batch and pipeline limits; with
    /// `heartbeat` an empty AppendEntries goes when there is nothing to send, which
    /// resets the follower's timer and carries the commit index.
    fn replicate(&mut self, to: ServerId, heartbeat: bool) {
        let last = self.last_index();
        let commit = self.commit;
        let term = self.term;
        let (max_batch, max_inflight) = (self.config.max_batch, self.config.max_inflight);
        let Some(progress) = self.progress.get(&to) else {
            return;
        };
        let mut next = progress.next;
        let mut inflight = progress.inflight.clone();
        let probing = progress.probe.is_some();
        let limit = if probing { 1 } else { max_inflight };
        let mut sends = Vec::new();
        while inflight.len() < limit && next <= last {
            let end = last.min(next + max_batch as Index - 1);
            let entries: Vec<Entry> = self.log[next as usize - 1..end as usize].to_vec();
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
        if self.config.variant == Variant::IndexFirstElectionRestriction {
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
        if self.config.variant == Variant::ResetTimerOnAnyRpc {
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
                            self.send(
                                from,
                                Message::AppendEntriesResponse {
                                    term: self.term,
                                    success: false,
                                    prev_index,
                                    match_index: 0,
                                    hint: 0,
                                    echo: sent,
                                    local: 0,
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
                    now,
                },
            ),
        }
    }

    /// A pre-vote is granted only by a server that has not heard from a leader within
    /// its minimum election timeout and whose log the candidate's is at least as up
    /// to date as; it changes nothing here.
    fn on_pre_vote(&mut self, from: ServerId, term: Term, last_index: Index, last_term: Term) {
        let granted = term > self.term
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
        if self.membership.has_majority(&self.granted) {
            self.become_candidate();
        }
    }

    /// Figure 2's RequestVote handler: one vote per term, only for a log at least as
    /// up to date; the vote is persisted before the response leaves, and granting it
    /// resets the election timer (moirae rule 5).
    fn on_request_vote(&mut self, from: ServerId, last_index: Index, last_term: Term) {
        let granted =
            self.vote.is_none_or(|v| v == from) && self.log_up_to_date(last_index, last_term);
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
        if self.membership.has_majority(&self.granted) {
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
        let consistent = match self.term_at(prev_index) {
            Some(t) => t == prev_term,
            None => false,
        };
        if !consistent {
            let hint = match self.term_at(prev_index) {
                None => self.last_index() + 1,
                Some(conflicting) => {
                    // The first index of the conflicting term, so the leader skips it.
                    let mut first = prev_index;
                    while first > 1 && self.term_at(first - 1) == Some(conflicting) {
                        first -= 1;
                    }
                    first
                }
            };
            self.send(
                from,
                Message::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    prev_index,
                    match_index: 0,
                    hint,
                    echo: sent,
                    local: 0,
                },
            );
            return;
        }
        if self.config.variant == Variant::TruncateOnEveryAppend && !entries.is_empty() {
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
                prev_index,
                match_index: matched,
                hint: 0,
                echo: sent,
                local: 0,
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
            now,
        } = ack;
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        // Any answer in this term is a sign of life for check quorum and a promise
        // for the lease, whether the entries fit or not: the follower reset its
        // timer on the request either way (moirae rule 5).
        progress.active = true;
        progress.promise = progress.promise.max(Some(echo));
        if let Some(moved) = progress.guard.observe(&self.config, now, echo, local) {
            self.trace(TraceEvent::RaftLeaseRevoked {
                server: self.id.0,
                follower: from.0,
                offset_moved: moved,
            });
        }
        // A read-index round: an acknowledgement of a request sent after the read
        // arrived says this server was still leader then.
        for read in &mut self.reads {
            if !read.confirmed && echo >= read.at && !read.acks.contains(&from) {
                read.acks.push(from);
            }
        }
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        if success {
            // Monotone: a stale or duplicated response proposes only what was passed.
            progress.matched = progress.matched.max(match_index);
            while progress
                .inflight
                .front()
                .is_some_and(|&end| end <= progress.matched)
            {
                progress.inflight.pop_front();
            }
            progress.next = progress.next.max(progress.matched + 1);
            progress.probe = None;
            let caught_up = progress.matched == self.last_index();
            self.maybe_commit();
            self.serve_reads();
            if self.transferee == Some(from) && caught_up {
                self.send_timeout_now(from);
            }
        } else {
            if progress.probe.is_some_and(|probe| probe != prev_index) {
                return;
            }
            progress.next = hint.max(1).max(progress.matched + 1);
            progress.probe = Some(progress.next - 1);
            progress.inflight.clear();
        }
        self.replicate(from, false);
    }

    /// Advances the commit index to the highest entry of the current term on a
    /// majority (§5.4.2); the buggy variant counts entries of any term.
    fn maybe_commit(&mut self) {
        let last = self.last_index();
        let mut index = last;
        while index > self.commit {
            let term_ok = self.config.variant == Variant::CountOlderTermForCommit
                || self.term_at(index) == Some(self.term);
            if term_ok {
                let mut on: Vec<ServerId> = self
                    .progress
                    .iter()
                    .filter(|(_, p)| p.matched >= index)
                    .map(|(&s, _)| s)
                    .collect();
                on.push(self.id);
                if self.membership.has_majority(&on) {
                    self.commit = index;
                    self.trace(TraceEvent::RaftCommit {
                        server: self.id.0,
                        term: self.term,
                        index,
                    });
                    self.outputs.push(Output::Apply { through: index });
                    return;
                }
            }
            index -= 1;
        }
    }
}

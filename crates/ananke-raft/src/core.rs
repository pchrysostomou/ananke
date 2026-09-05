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

/// The known-buggy cores beside the correct one (RAFT.md §5). Each breaks one rule;
/// the sweep must catch each and pass the correct one. The server-level variants,
/// sending before persisting and applying before committing, live in the server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Variant {
    /// Figure 2 with pre-vote, as RAFT.md §1 says.
    #[default]
    Correct,
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
            variant: Variant::Correct,
        }
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
    },
    /// One tick of the server's timer passed.
    Tick,
    /// A client proposes a command.
    Propose(Bytes),
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
    /// A proposal was refused because this server is not the leader.
    Rejected {
        /// The leader, if known.
        leader: Option<ServerId>,
    },
    /// A state transition that matters, for the trace.
    Trace(TraceEvent),
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
            Input::Applied(index) => self.applied = self.applied.max(index),
            Input::Message { from, message } => self.on_message(from, message),
        }
        self.finish()
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
        self.granted.clear();
        self.set_role(Role::Leader);
        let last = self.last_index();
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
        let mut sends = Vec::new();
        while inflight.len() < max_inflight && next <= last {
            let end = last.min(next + max_batch as Index - 1);
            let entries: Vec<Entry> = self.log[next as usize - 1..end as usize].to_vec();
            sends.push(Message::AppendEntries {
                term,
                prev_index: next - 1,
                prev_term: self.term_at(next - 1).unwrap_or(0),
                entries,
                commit,
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
            });
        }
        if let Some(progress) = self.progress.get_mut(&to) {
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

    fn on_message(&mut self, from: ServerId, message: Message) {
        if self.config.variant == Variant::ResetTimerOnAnyRpc {
            self.election_elapsed = 0;
        }
        let term = message.term();
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
                        Message::AppendEntries { .. } => {
                            self.send(
                                from,
                                Message::AppendEntriesResponse {
                                    term: self.term,
                                    success: false,
                                    match_index: 0,
                                    hint: 0,
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
            Message::RequestVoteResponse { granted, .. } => {
                self.on_request_vote_response(from, granted)
            }
            Message::AppendEntries {
                prev_index,
                prev_term,
                entries,
                commit,
                ..
            } => self.on_append_entries(from, prev_index, prev_term, entries, commit),
            Message::AppendEntriesResponse {
                success,
                match_index,
                hint,
                ..
            } => self.on_append_entries_response(from, success, match_index, hint),
        }
    }

    /// A pre-vote is granted only by a server that has not heard from a leader within
    /// its minimum election timeout and whose log the candidate's is at least as up
    /// to date as; it changes nothing here.
    fn on_pre_vote(&mut self, from: ServerId, term: Term, last_index: Index, last_term: Term) {
        let heard_from_leader = self.role == Role::Leader
            || (self.leader.is_some() && self.election_elapsed < self.config.election_ticks.0);
        let granted =
            term > self.term && !heard_from_leader && self.log_up_to_date(last_index, last_term);
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
                    match_index: 0,
                    hint,
                },
            );
            return;
        }
        if self.config.variant == Variant::TruncateOnEveryAppend && !entries.is_empty() {
            self.truncate(prev_index + 1);
        }
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
        let matched = if to_append.is_empty() {
            prev_index.max(self.last_index().min(prev_index))
        } else {
            to_append.last().map_or(prev_index, |e| e.index)
        };
        let matched = matched.max(prev_index);
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
                match_index: matched,
                hint: 0,
            },
        );
    }

    fn on_append_entries_response(
        &mut self,
        from: ServerId,
        success: bool,
        match_index: Index,
        hint: Index,
    ) {
        if self.role != Role::Leader {
            return;
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
            self.maybe_commit();
        } else {
            progress.next = hint.max(1).max(progress.matched + 1);
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

//! The paper's scenarios against the pure core, no simulator: a handful of cores
//! stepped by hand with messages delivered in the order each scenario needs. Figure 7
//! for the election restriction and log repair, Figure 8 for committing entries of
//! older terms, and moirae's rules for the mistakes everyone makes. Each buggy
//! variant is shown breaking its rule and the correct core holding it.

use std::collections::BTreeMap;

use ananke_env::TraceEvent;
use ananke_raft::core::{Input, Output, Persist, Raft, RaftConfig, Role, Variant};
use ananke_raft::invariants;
use ananke_raft::message::Message;
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId, Term};
use bytes::Bytes;

fn s(n: u64) -> ServerId {
    ServerId(n)
}

/// A log whose entries have these terms, indexed from 1, each a distinct command.
fn log_of(terms: &[Term]) -> Vec<Entry> {
    terms
        .iter()
        .enumerate()
        .map(|(i, &term)| Entry {
            term,
            index: i as Index + 1,
            payload: Payload::Command(Bytes::from(format!("t{term}i{}", i + 1))),
        })
        .collect()
}

fn terms_of(log: &[Entry]) -> Vec<Term> {
    log.iter().map(|e| e.term).collect()
}

/// The cores of a group, their persisted state, the messages in flight, and the
/// trace they produced: enough to drive the paper's figures step by step.
struct Cluster {
    cores: BTreeMap<ServerId, Raft>,
    persisted: BTreeMap<ServerId, (Term, Option<ServerId>, Vec<Entry>)>,
    inbox: Vec<(ServerId, ServerId, Message)>,
    events: Vec<TraceEvent>,
    /// Links cut, as (from, to).
    cut: Vec<(ServerId, ServerId)>,
    /// Links on which AppendEntries are dropped while votes pass, as (from, to).
    no_appends: Vec<(ServerId, ServerId)>,
    /// Servers taken down: their messages are dropped and they are not stepped.
    down: Vec<ServerId>,
    /// The harness clock, in nanoseconds: advanced by `tick`, stamped on messages
    /// where a server would stamp its own clock.
    now: u64,
    /// Each server's clock rate error in parts per million, for the guard tests.
    drift_ppm: BTreeMap<ServerId, i64>,
    /// Reads the cores declared ready: (server, read id, read index).
    reads: Vec<(ServerId, u64, Index)>,
    /// Reads the cores dropped: (server, read id).
    dropped: Vec<(ServerId, u64)>,
}

impl Cluster {
    fn new(
        members: &[ServerId],
        config: &RaftConfig,
        logs: &[(ServerId, Term, Vec<Entry>)],
    ) -> Self {
        let membership = Configuration::of(members);
        let mut cores = BTreeMap::new();
        let mut persisted = BTreeMap::new();
        for &id in members {
            let (term, log) = logs
                .iter()
                .find(|(s, _, _)| *s == id)
                .map_or((0, Vec::new()), |(_, t, l)| (*t, l.clone()));
            cores.insert(
                id,
                Raft::restore(
                    id,
                    membership.clone(),
                    config.clone(),
                    id.0 * 7919,
                    term,
                    None,
                    log.clone(),
                ),
            );
            persisted.insert(id, (term, None, log));
        }
        // The initial logs as the trace would have shown them being written.
        let mut events = Vec::new();
        for (id, (_, _, log)) in &persisted {
            for entry in log {
                events.push(TraceEvent::RaftAppend {
                    server: id.0,
                    index: entry.index,
                    entry_term: entry.term,
                    hash: entry.payload.hash(),
                });
            }
        }
        Self {
            cores,
            persisted,
            inbox: Vec::new(),
            events,
            cut: Vec::new(),
            no_appends: Vec::new(),
            down: Vec::new(),
            now: 0,
            drift_ppm: BTreeMap::new(),
            reads: Vec::new(),
            dropped: Vec::new(),
        }
    }

    /// A server's own clock at the harness time `now`.
    fn local_clock(&self, id: ServerId) -> u64 {
        let ppm = self.drift_ppm.get(&id).copied().unwrap_or(0);
        let now = i128::from(self.now);
        u64::try_from(now + now * i128::from(ppm) / 1_000_000).expect("positive")
    }

    fn step(&mut self, id: ServerId, input: Input) -> Vec<Output> {
        if self.down.contains(&id) {
            return Vec::new();
        }
        let outputs = self.cores.get_mut(&id).expect("a member").step(input);
        let mut sends_before_persist = false;
        let mut persisted = false;
        for output in &outputs {
            match output {
                Output::Persist(p) => {
                    persisted = true;
                    self.apply_persist(id, p);
                }
                Output::Send { to, message } => {
                    if !persisted && outputs.iter().any(|o| matches!(o, Output::Persist(_))) {
                        sends_before_persist = true;
                    }
                    let mut message = message.clone();
                    match &mut message {
                        Message::AppendEntries { sent, .. } => *sent = self.local_clock(id),
                        Message::AppendEntriesResponse { local, .. } => {
                            *local = self.local_clock(id);
                        }
                        _ => {}
                    }
                    self.inbox.push((id, *to, message));
                }
                Output::Trace(event) => self.events.push(event.clone()),
                Output::ReadReady { id: read, index } => self.reads.push((id, *read, *index)),
                Output::ReadDropped { id: read } => self.dropped.push((id, *read)),
                Output::Apply { .. } | Output::Rejected { .. } => {}
            }
        }
        assert!(
            !sends_before_persist,
            "a send came before the step's persist"
        );
        outputs
    }

    fn apply_persist(&mut self, id: ServerId, persist: &Persist) {
        let state = self.persisted.get_mut(&id).expect("a member");
        state.0 = persist.term;
        state.1 = persist.vote;
        if let Some(from) = persist.truncate_from {
            state.2.truncate(from as usize - 1);
        }
        for entry in &persist.append {
            assert_eq!(
                entry.index,
                state.2.len() as Index + 1,
                "appends are consecutive"
            );
            state.2.push(entry.clone());
        }
    }

    /// Delivers every message in flight, in order, until nothing is in flight.
    fn settle(&mut self) {
        while !self.inbox.is_empty() {
            let batch = std::mem::take(&mut self.inbox);
            for (from, to, message) in batch {
                self.deliver(from, to, message);
            }
        }
    }

    fn deliver(&mut self, from: ServerId, to: ServerId, message: Message) {
        if self.cut.contains(&(from, to)) || self.down.contains(&from) || self.down.contains(&to) {
            return;
        }
        if matches!(message, Message::AppendEntries { .. }) && self.no_appends.contains(&(from, to))
        {
            return;
        }
        let now = self.local_clock(to);
        self.step(to, Input::Message { from, message, now });
    }

    fn tick(&mut self, id: ServerId, ticks: u64) {
        for _ in 0..ticks {
            self.now += self.cores[&id].config().tick_nanos;
            self.step(id, Input::Tick);
        }
    }

    /// Makes `id` the leader: first the other servers' timers age past the minimum
    /// timeout, as they would with the old leader gone, with whatever campaigns
    /// they start going nowhere; then `id` is ticked until its own election fires
    /// and settles. Asserts it leads.
    fn elect(&mut self, id: ServerId) {
        let min = self.cores[&id].config().election_ticks.0;
        let others: Vec<ServerId> = self.cores.keys().copied().filter(|&o| o != id).collect();
        for other in others {
            self.tick(other, min);
        }
        self.inbox.clear();
        for _ in 0..200 {
            self.tick(id, 1);
            self.settle();
            if self.role(id) == Role::Leader {
                return;
            }
        }
        panic!("{id} was not elected");
    }

    fn role(&self, id: ServerId) -> Role {
        self.cores[&id].role()
    }

    fn term(&self, id: ServerId) -> Term {
        self.cores[&id].term()
    }

    fn log(&self, id: ServerId) -> Vec<Entry> {
        self.cores[&id].log().to_vec()
    }

    fn commit(&self, id: ServerId) -> Index {
        self.cores[&id].commit()
    }

    fn isolate(&mut self, id: ServerId, members: &[ServerId]) {
        for &other in members {
            if other != id {
                self.cut.push((id, other));
                self.cut.push((other, id));
            }
        }
    }

    fn heal(&mut self) {
        self.cut.clear();
        self.no_appends.clear();
        self.down.clear();
    }

    /// Brings `id` back from its persisted state, as a follower with no memory of
    /// its role, the way a restart does.
    fn restart(&mut self, id: ServerId, config: &RaftConfig) {
        let (term, vote, log) = self.persisted[&id].clone();
        let membership = self.cores[&id].membership().clone();
        self.cores.insert(
            id,
            Raft::restore(
                id,
                membership,
                config.clone(),
                id.0 * 7919 + term,
                term,
                vote,
                log,
            ),
        );
        self.down.retain(|&d| d != id);
    }

    /// Lets votes through from `from` to `to` but no AppendEntries.
    fn block_appends(&mut self, from: ServerId, to: &[ServerId]) {
        for &t in to {
            self.no_appends.push((from, t));
        }
    }

    fn propose(&mut self, id: ServerId, command: &str) {
        self.step(id, Input::Propose(Bytes::from(command.to_owned())));
    }
}

fn five() -> Vec<ServerId> {
    (1..=5).map(s).collect()
}

fn config(variant: Variant) -> RaftConfig {
    RaftConfig {
        election_ticks: (10, 20),
        heartbeat_ticks: 2,
        max_batch: 64,
        max_inflight: 8,
        variant,
        ..RaftConfig::default()
    }
}

/// Figure 7's logs: the leader's, and followers a to f.
fn figure_7() -> Vec<(char, Vec<Term>)> {
    vec![
        ('L', vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6]),
        ('a', vec![1, 1, 1, 4, 4, 5, 5, 6, 6]),
        ('b', vec![1, 1, 1, 4]),
        ('c', vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]),
        ('d', vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        ('e', vec![1, 1, 1, 4, 4, 4, 4]),
        ('f', vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]),
    ]
}

/// The election restriction (§5.4.1) over Figure 7: a candidate with the leader's log
/// gets votes from a, b, e and f, whose logs are no more up to date, and not from c
/// and d, whose logs are. The buggy core that compares lengths first refuses f, and,
/// worse, grants a candidate with f's stale but longer log a vote over the leader's.
#[test]
fn figure_7_votes_follow_last_term_then_length() {
    let logs = figure_7();
    let leader_terms = logs[0].1.clone();
    for variant in [Variant::Correct, Variant::IndexFirstElectionRestriction] {
        let mut granted = Vec::new();
        for (name, terms) in logs.iter().skip(1) {
            let mut voter = Raft::restore(
                s(2),
                Configuration::of(&[s(1), s(2)]),
                config(variant),
                1,
                7,
                None,
                log_of(terms),
            );
            let out = voter.step(Input::Message {
                now: 0,
                from: s(1),
                message: Message::RequestVote {
                    term: 8,
                    last_index: leader_terms.len() as Index,
                    last_term: *leader_terms.last().unwrap(),
                    transfer: false,
                },
            });
            let grant = out.iter().any(|o| {
                matches!(
                    o,
                    Output::Send {
                        message: Message::RequestVoteResponse { granted: true, .. },
                        ..
                    }
                )
            });
            if grant {
                granted.push(*name);
            }
        }
        match variant {
            Variant::Correct => assert_eq!(granted, ['a', 'b', 'e', 'f']),
            _ => assert_eq!(
                granted,
                ['a', 'b', 'e'],
                "length first refuses f's shorter log"
            ),
        }
        // A candidate with f's log asks a server holding the leader's log.
        let mut voter = Raft::restore(
            s(2),
            Configuration::of(&[s(1), s(2)]),
            config(variant),
            1,
            7,
            None,
            log_of(&leader_terms),
        );
        let f = &logs[6].1;
        let out = voter.step(Input::Message {
            now: 0,
            from: s(1),
            message: Message::RequestVote {
                term: 8,
                last_index: f.len() as Index,
                last_term: *f.last().unwrap(),
                transfer: false,
            },
        });
        let grant = out.iter().any(|o| {
            matches!(
                o,
                Output::Send {
                    message: Message::RequestVoteResponse { granted: true, .. },
                    ..
                }
            )
        });
        match variant {
            Variant::Correct => assert!(!grant, "a stale log does not win by being longer"),
            _ => assert!(grant, "the buggy core lets the stale longer log win"),
        }
    }
}

/// Log repair over Figure 7: the leader of term 8 brings every follower to its own
/// log plus its no-op, walking back with the conflict hints, and log matching holds
/// throughout.
#[test]
fn figure_7_append_entries_repairs_every_follower() {
    let logs = figure_7();
    let members: Vec<ServerId> = (1..=7).map(s).collect();
    let initial: Vec<(ServerId, Term, Vec<Entry>)> = logs
        .iter()
        .enumerate()
        .map(|(i, (_, terms))| (s(i as u64 + 1), 7, log_of(terms)))
        .collect();
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &initial);
    cluster.elect(s(1));
    assert_eq!(cluster.term(s(1)), 8);
    // Heartbeats carry the repairs; a few rounds settle every follower.
    for _ in 0..40 {
        cluster.tick(s(1), 2);
        cluster.settle();
    }
    let leader_log = cluster.log(s(1));
    assert_eq!(terms_of(&leader_log), [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 8]);
    for id in members.iter().skip(1) {
        assert_eq!(cluster.log(*id), leader_log, "{id}");
        assert_eq!(cluster.persisted[id].2, leader_log, "{id} on disk");
    }
    assert_eq!(cluster.commit(s(1)), 11, "the no-op commits the whole log");
    invariants::all(&cluster.events).unwrap();
}

/// Figure 8: an entry replicated on a majority under an older term is not committed
/// by counting replicas; the buggy core commits it, a later leader overwrites it,
/// and leader completeness is broken.
#[test]
fn figure_8_an_older_terms_entry_is_not_committed_by_count() {
    for variant in [Variant::Correct, Variant::CountOlderTermForCommit] {
        let members = five();
        let c = RaftConfig {
            max_batch: 1,
            max_inflight: 1,
            ..config(variant)
        };
        // Every server starts with an entry of term 1.
        let initial: Vec<(ServerId, Term, Vec<Entry>)> =
            members.iter().map(|&id| (id, 1, log_of(&[1]))).collect();
        let mut cluster = Cluster::new(&members, &c, &initial);

        // (a) S1 leads term 2; its no-op at index 2 reaches S2 only.
        cluster.block_appends(s(1), &[s(3), s(4), s(5)]);
        cluster.elect(s(1));
        assert_eq!(cluster.term(s(1)), 2);
        assert_eq!(terms_of(&cluster.log(s(2))), [1, 2]);
        for id in [s(3), s(4), s(5)] {
            assert_eq!(terms_of(&cluster.log(id)), [1], "{id} did not get index 2");
        }

        // (b) S1 goes down; S5 leads term 3 with votes from S3 and S4 and appends its
        // no-op at index 2, which reaches nobody: S5 goes down at once.
        cluster.down.push(s(1));
        cluster.block_appends(s(5), &[s(2), s(3), s(4)]);
        cluster.elect(s(5));
        assert_eq!(cluster.term(s(5)), 3);
        assert_eq!(terms_of(&cluster.log(s(5))), [1, 3]);
        cluster.down.push(s(5));
        cluster.inbox.clear();

        // (c) S1 comes back and leads term 4 with votes from S2, S3 and S4. It
        // replicates index 2 (term 2) to S3, a majority with S2, and S4 gets nothing.
        // One deviation from the figure: a restarted leader learns what S2 holds only
        // from a successful AppendEntries (deviation D1), and the first one that can
        // succeed carries S1's term-4 no-op, so S2 ends with [1, 2, 4] rather than
        // [1, 2]. Index 2 is on a majority either way, with a term that is not the
        // leader's, which is the figure's point; S2's extra entry only makes it refuse
        // S5 its vote in (d), where S3 and S4 still make a majority.
        cluster.restart(s(1), &c);
        cluster.no_appends.clear();
        cluster.block_appends(s(1), &[s(3), s(4)]);
        cluster.elect(s(1));
        assert_eq!(cluster.term(s(1)), 4);
        assert_eq!(terms_of(&cluster.log(s(1))), [1, 2, 4]);
        // Now S3 may receive index 2 and nothing past it, while S2 and S1 talk freely.
        cluster.no_appends.retain(|&link| link != (s(1), s(3)));
        for _ in 0..8 {
            cluster.tick(s(1), 2);
            cluster.deliver_where(|from, to, message| match message {
                Message::AppendEntries { entries, .. } if from == s(1) && to == s(3) => {
                    entries.iter().all(|e| e.index <= 2)
                }
                _ => {
                    from == s(1) && to == s(2)
                        || from == s(2) && to == s(1)
                        || from == s(3) && to == s(1)
                }
            });
        }
        assert_eq!(terms_of(&cluster.log(s(3))), [1, 2]);
        assert_eq!(terms_of(&cluster.log(s(2))), [1, 2, 4]);
        assert_eq!(terms_of(&cluster.log(s(4))), [1]);
        let committed_index_2 = cluster.commit(s(1)) >= 2;
        match variant {
            Variant::Correct => assert!(!committed_index_2, "index 2 is of term 2, not 4"),
            _ => assert!(
                committed_index_2,
                "the buggy core counts replicas of any term"
            ),
        }
        cluster.down.push(s(1));
        cluster.inbox.clear();

        // (d) S5 comes back and leads term 5 with votes from S2, S3 and S4, whose last
        // terms are at most 3; its log overwrites index 2 everywhere reachable.
        cluster.no_appends.clear();
        cluster.restart(s(5), &c);
        cluster.elect(s(5));
        assert_eq!(cluster.term(s(5)), 5);
        for _ in 0..10 {
            cluster.tick(s(5), 2);
            cluster.settle();
        }
        for id in [s(2), s(3), s(4)] {
            assert_eq!(terms_of(&cluster.log(id)), [1, 3, 5], "{id} follows S5");
        }
        let verdict = invariants::leader_completeness(&cluster.events);
        match variant {
            Variant::Correct => verdict.unwrap(),
            _ => {
                let violation = verdict.unwrap_err();
                assert!(violation.contains("index 2"), "{violation}");
            }
        }
        invariants::election_safety(&cluster.events).unwrap();
        invariants::log_matching(&cluster.events).unwrap();
    }
}

impl Cluster {
    /// Delivers what is in flight and `allow` lets through; the rest is dropped.
    fn deliver_where(&mut self, allow: impl Fn(ServerId, ServerId, &Message) -> bool) {
        let batch = std::mem::take(&mut self.inbox);
        for (f, t, message) in batch {
            if allow(f, t, &message) {
                self.deliver(f, t, message);
            }
        }
    }
}

/// moirae rule 3: a duplicated older AppendEntries must not delete entries; the buggy
/// core truncates on every request and loses a committed entry to the duplicate.
#[test]
fn a_duplicated_older_append_does_not_truncate() {
    for variant in [Variant::Correct, Variant::TruncateOnEveryAppend] {
        let members = vec![s(1), s(2), s(3)];
        let c = RaftConfig {
            max_batch: 1,
            ..config(variant)
        };
        let mut cluster = Cluster::new(&members, &c, &[]);
        cluster.elect(s(1));
        cluster.propose(s(1), "a");
        cluster.propose(s(1), "b");
        // The first AppendEntries to S2 carries index 2; keep a copy of it.
        let first = cluster
            .inbox
            .iter()
            .find(|(f, t, m)| *f == s(1) && *t == s(2) && matches!(m, Message::AppendEntries { entries, .. } if entries.first().is_some_and(|e| e.index == 2)))
            .cloned()
            .expect("index 2 in flight to S2");
        for _ in 0..6 {
            cluster.tick(s(1), 2);
            cluster.settle();
        }
        assert_eq!(terms_of(&cluster.log(s(2))), [1, 1, 1]);
        assert_eq!(cluster.commit(s(1)), 3);
        // The duplicate arrives late.
        cluster.deliver(first.0, first.1, first.2);
        match variant {
            Variant::Correct => assert_eq!(cluster.log(s(2)).len(), 3, "nothing lost"),
            _ => assert_eq!(cluster.log(s(2)).len(), 2, "the buggy core dropped index 3"),
        }
    }
}

/// moirae rule 5: only the leader of the current term, a granted vote and starting an
/// election reset the timer. A follower fed stale requests still times out; the buggy
/// core never does.
#[test]
fn stale_requests_do_not_reset_the_election_timer() {
    for variant in [Variant::Correct, Variant::ResetTimerOnAnyRpc] {
        let mut follower = Raft::restore(
            s(2),
            Configuration::of(&[s(1), s(2), s(3)]),
            config(variant),
            3,
            5,
            None,
            Vec::new(),
        );
        let mut started = false;
        for _ in 0..100 {
            let out = follower.step(Input::Message {
                now: 0,
                from: s(3),
                message: Message::RequestVote {
                    term: 2,
                    last_index: 0,
                    last_term: 0,
                    transfer: false,
                },
            });
            assert!(
                out.iter().any(|o| matches!(
                    o,
                    Output::Send {
                        message: Message::RequestVoteResponse {
                            term: 5,
                            granted: false
                        },
                        ..
                    }
                )),
                "a stale request gets our term back"
            );
            let out = follower.step(Input::Tick);
            if out.iter().any(|o| {
                matches!(
                    o,
                    Output::Send {
                        message: Message::PreVote { .. },
                        ..
                    }
                )
            }) {
                started = true;
                break;
            }
        }
        match variant {
            Variant::Correct => assert!(started, "the timer fired within a hundred ticks"),
            _ => assert!(!started, "the buggy core reset it on every stale request"),
        }
    }
}

/// moirae rule 1 and rule 6: a higher term makes a follower of a leader, and a stale
/// response changes nothing.
#[test]
fn a_higher_term_steps_a_leader_down_and_a_stale_response_is_ignored() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.elect(s(1));
    let term = cluster.term(s(1));
    // A stale response from an old term.
    cluster.step(
        s(1),
        Input::Message {
            now: 0,
            from: s(2),
            message: Message::AppendEntriesResponse {
                term: term - 1,
                success: true,
                prev_index: 0,
                match_index: 99,
                hint: 0,
                echo: 0,
                local: 0,
            },
        },
    );
    assert_eq!(cluster.role(s(1)), Role::Leader);
    assert_eq!(cluster.commit(s(1)), 1, "a stale success moved nothing");
    // A vote request from a higher term: ignored while this server leads (RAFT.md
    // §1), term and all, since its followers promised it their votes.
    cluster.step(
        s(1),
        Input::Message {
            now: 0,
            from: s(3),
            message: Message::RequestVote {
                term: term + 5,
                last_index: 1,
                last_term: term,
                transfer: false,
            },
        },
    );
    assert_eq!(cluster.role(s(1)), Role::Leader);
    assert_eq!(cluster.term(s(1)), term);
    // An AppendEntries from a higher term: a leader of that term exists, so this one
    // steps down (moirae rule 1).
    cluster.step(
        s(1),
        Input::Message {
            now: 0,
            from: s(3),
            message: Message::AppendEntries {
                term: term + 5,
                prev_index: 0,
                prev_term: 0,
                entries: Vec::new(),
                commit: 0,
                sent: 0,
            },
        },
    );
    assert_eq!(cluster.role(s(1)), Role::Follower);
    assert_eq!(cluster.term(s(1)), term + 5);
    assert_eq!(
        cluster.persisted[&s(1)].0,
        term + 5,
        "the new term was persisted"
    );
}

/// Deviation D1: the leader's match index comes from the response and never moves
/// backwards, so a replayed old success response is harmless.
#[test]
fn a_replayed_success_response_does_not_move_match_index_back() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.elect(s(1));
    for i in 0..5 {
        cluster.propose(s(1), &format!("c{i}"));
    }
    for _ in 0..6 {
        cluster.tick(s(1), 2);
        cluster.settle();
    }
    assert_eq!(cluster.commit(s(1)), 6);
    let term = cluster.term(s(1));
    // An old success for index 1 arrives again: the commit index stays.
    cluster.step(
        s(1),
        Input::Message {
            now: 0,
            from: s(2),
            message: Message::AppendEntriesResponse {
                term,
                success: true,
                prev_index: 0,
                match_index: 1,
                hint: 0,
                echo: 0,
                local: 0,
            },
        },
    );
    assert_eq!(cluster.commit(s(1)), 6);
    cluster.propose(s(1), "after");
    for _ in 0..4 {
        cluster.tick(s(1), 2);
        cluster.settle();
    }
    assert_eq!(
        cluster.commit(s(1)),
        7,
        "replication goes on from where it was"
    );
    invariants::all(&cluster.events).unwrap();
}

/// Pre-vote (thesis §9.6): a server cut off from a working leader does not raise its
/// term, and on rejoining does not depose the leader; the core without pre-vote does
/// both.
#[test]
fn a_partitioned_server_does_not_disrupt_a_leader_when_it_rejoins() {
    for variant in [Variant::Correct, Variant::NoPreVote] {
        let members = vec![s(1), s(2), s(3)];
        let mut cluster = Cluster::new(&members, &config(variant), &[]);
        cluster.elect(s(1));
        let term = cluster.term(s(1));
        cluster.isolate(s(3), &members);
        // The leader keeps heartbeating S2; S3 times out again and again.
        for _ in 0..100 {
            cluster.tick(s(1), 1);
            cluster.tick(s(2), 1);
            cluster.tick(s(3), 1);
            cluster.settle();
        }
        match variant {
            Variant::Correct => assert_eq!(cluster.term(s(3)), term, "no term rose in isolation"),
            _ => assert!(cluster.term(s(3)) > term, "the buggy core raised its term"),
        }
        cluster.heal();
        for _ in 0..30 {
            cluster.tick(s(1), 1);
            cluster.tick(s(2), 1);
            cluster.tick(s(3), 1);
            cluster.settle();
        }
        let leaders = cluster
            .events
            .iter()
            .filter(|e| matches!(e, TraceEvent::RaftLeader { .. }))
            .count();
        match variant {
            Variant::Correct => {
                assert_eq!(cluster.role(s(1)), Role::Leader);
                assert_eq!(cluster.term(s(1)), term);
                assert_eq!(leaders, 1, "one election in the whole run");
            }
            _ => assert!(cluster.term(s(1)) > term, "the leader was deposed"),
        }
        invariants::all(&cluster.events).unwrap();
    }
}

/// Ticks the leader through `rounds` heartbeat rounds, settling each, so the guard
/// sees a window of steady responses per `guard_window_nanos` of harness time.
fn heartbeat_rounds(cluster: &mut Cluster, leader: ServerId, rounds: u64) {
    let beat = cluster.cores[&leader].config().heartbeat_ticks;
    for _ in 0..rounds {
        cluster.tick(leader, beat);
        cluster.settle();
    }
}

/// A leader with a committed, applied entry of its term and every follower's
/// promise observed steady through two guard windows: the state a lease starts from.
fn leader_with_lease(cluster: &mut Cluster, leader: ServerId) {
    cluster.elect(leader);
    cluster.propose(leader, "x");
    cluster.settle();
    let commit = cluster.commit(leader);
    cluster.step(leader, Input::Applied(commit));
    // Two windows of 400ms at 20ms a round, and a round to close the second.
    heartbeat_rounds(cluster, leader, 45);
}

/// Read-index (thesis §6.4): a read is confirmed only by acknowledgements of
/// requests sent after it arrived, and served once its index is applied.
#[test]
fn a_read_index_read_needs_acknowledgements_sent_after_it() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.elect(s(1));
    cluster.propose(s(1), "x");
    cluster.settle();
    let commit = cluster.commit(s(1));
    cluster.step(s(1), Input::Applied(commit));
    // A heartbeat round whose requests are sent now, then time passes before the
    // read arrives: their acknowledgements are older than the read.
    let beat = cluster.cores[&s(1)].config().heartbeat_ticks;
    cluster.tick(s(1), beat);
    let old: Vec<(ServerId, ServerId, Message)> = std::mem::take(&mut cluster.inbox);
    cluster.now += 5_000_000;
    let out = cluster.step(
        s(1),
        Input::Read {
            id: 7,
            now: cluster.now,
        },
    );
    assert!(
        !out.iter().any(|o| matches!(o, Output::ReadReady { .. })),
        "no lease yet: the read waits for a round"
    );
    let fresh: Vec<(ServerId, ServerId, Message)> = std::mem::take(&mut cluster.inbox);
    assert!(
        fresh
            .iter()
            .any(|(_, _, m)| matches!(m, Message::AppendEntries { .. })),
        "the read sent a heartbeat round"
    );
    // The old round's acknowledgements arrive first: not enough.
    cluster.inbox = old;
    cluster.settle();
    assert!(
        cluster.reads.is_empty(),
        "acknowledgements from before the read confirmed it"
    );
    // The fresh round's do.
    cluster.inbox = fresh;
    cluster.settle();
    assert_eq!(cluster.reads, vec![(s(1), 7, commit)]);
    let lease_reads = cluster
        .events
        .iter()
        .filter(|e| matches!(e, TraceEvent::RaftRead { lease: true, .. }))
        .count();
    assert_eq!(lease_reads, 0, "served by a round, not a lease");
}

/// A lease read (RAFT.md §1): served at once within the promise of a majority,
/// and not after it, when only a round can confirm the leader.
#[test]
fn a_lease_read_is_served_within_the_promise_and_not_after() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    leader_with_lease(&mut cluster, s(1));
    let commit = cluster.commit(s(1));
    let lease_end = cluster.cores[&s(1)].lease_end();
    assert!(lease_end > cluster.now, "a lease holds after steady rounds");
    let out = cluster.step(
        s(1),
        Input::Read {
            id: 1,
            now: cluster.now,
        },
    );
    assert!(
        out.iter()
            .any(|o| matches!(o, Output::ReadReady { id: 1, index } if *index == commit)),
        "served at once by the lease: {out:?}"
    );
    assert!(cluster.events.iter().any(|e| matches!(
        e,
        TraceEvent::RaftRead {
            server: 1,
            lease: true,
            ..
        }
    )));
    // Time passes past the lease with nothing heard: the next read needs a round.
    cluster.cut = vec![(s(2), s(1)), (s(3), s(1))];
    while cluster.now < lease_end {
        cluster.tick(s(2), 1);
    }
    cluster.inbox.clear();
    let out = cluster.step(
        s(1),
        Input::Read {
            id: 2,
            now: cluster.now,
        },
    );
    assert!(
        !out.iter().any(|o| matches!(o, Output::ReadReady { .. })),
        "past the promise the lease is gone: {out:?}"
    );
    assert!(
        out.iter().any(|o| matches!(
            o,
            Output::Send {
                message: Message::AppendEntries { .. },
                ..
            }
        )),
        "a round was started instead"
    );
}

/// The drift guard (RAFT.md §1): a follower whose clock runs fast against the
/// leader's is revoked once a window's fastest response has moved beyond the bound,
/// and the lease then rests on the others; with every follower moving there is no
/// lease at all.
#[test]
fn the_guard_revokes_a_follower_whose_clock_moves() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.drift_ppm.insert(s(2), 300_000);
    leader_with_lease(&mut cluster, s(1));
    let revoked: Vec<u64> = cluster
        .events
        .iter()
        .filter_map(|e| match e {
            TraceEvent::RaftLeaseRevoked {
                server: 1,
                follower,
                ..
            } => Some(*follower),
            _ => None,
        })
        .collect();
    assert!(
        revoked.contains(&2),
        "the fast follower was revoked: {revoked:?}"
    );
    assert!(
        !revoked.contains(&3),
        "the steady follower was not: {revoked:?}"
    );
    assert!(
        cluster.cores[&s(1)].lease_holds(cluster.now),
        "the lease rests on server 3"
    );
    // Every follower moving: no majority of promises to trust.
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.drift_ppm.insert(s(2), 300_000);
    cluster.drift_ppm.insert(s(3), -200_000);
    leader_with_lease(&mut cluster, s(1));
    assert!(
        !cluster.cores[&s(1)].lease_holds(cluster.now),
        "no lease with every promise in doubt"
    );
    // The buggy leader trusts them all.
    let mut cluster = Cluster::new(&members, &config(Variant::LeaseTrustsTheClock), &[]);
    cluster.drift_ppm.insert(s(2), 300_000);
    cluster.drift_ppm.insert(s(3), -200_000);
    leader_with_lease(&mut cluster, s(1));
    assert!(
        cluster.cores[&s(1)].lease_holds(cluster.now),
        "the buggy leader holds a lease anyway"
    );
}

/// Check quorum (RAFT.md §1): a leader that hears from no majority for the minimum
/// election timeout steps down, and drops the reads it was holding.
#[test]
fn check_quorum_steps_a_leader_down_that_hears_from_no_majority() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.elect(s(1));
    cluster.settle();
    cluster.cut = vec![(s(2), s(1)), (s(3), s(1))];
    cluster.step(
        s(1),
        Input::Read {
            id: 9,
            now: cluster.now,
        },
    );
    let min = cluster.cores[&s(1)].config().election_ticks.0;
    for _ in 0..2 * min {
        cluster.tick(s(1), 1);
        cluster.settle();
        cluster.inbox.clear();
    }
    assert_eq!(cluster.role(s(1)), Role::Follower);
    assert!(
        cluster
            .events
            .iter()
            .any(|e| matches!(e, TraceEvent::RaftQuorumLost { server: 1, .. }))
    );
    assert_eq!(
        cluster.dropped,
        vec![(s(1), 9)],
        "the read was dropped, not served"
    );
}

/// The vote rule behind the lease (RAFT.md §1, thesis §6.4.1): a follower that has
/// heard from its leader within the minimum election timeout ignores a vote
/// request, term and all; once it has not, it grants one.
#[test]
fn a_follower_that_heard_from_its_leader_ignores_a_vote_request() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.elect(s(1));
    cluster.settle();
    let term = cluster.term(s(2));
    let request = Message::RequestVote {
        term: term + 1,
        last_index: cluster.log(s(2)).len() as Index,
        last_term: term,
        transfer: false,
    };
    cluster.step(
        s(2),
        Input::Message {
            now: 0,
            from: s(3),
            message: request.clone(),
        },
    );
    assert_eq!(cluster.term(s(2)), term, "the term did not move");
    assert!(cluster.inbox.is_empty(), "nothing was answered");
    assert!(cluster.events.iter().any(|e| matches!(
        e,
        TraceEvent::RaftVote {
            server: 2,
            granted: false,
            pre: false,
            ..
        }
    )));
    // The leader goes quiet for the minimum timeout.
    cluster.cut = vec![(s(1), s(2))];
    let min = cluster.cores[&s(2)].config().election_ticks.0;
    cluster.tick(s(2), min);
    cluster.inbox.clear();
    cluster.step(
        s(2),
        Input::Message {
            now: 0,
            from: s(3),
            message: request,
        },
    );
    assert_eq!(cluster.term(s(2)), term + 1);
    assert!(cluster.events.iter().any(|e| matches!(
        e,
        TraceEvent::RaftVote {
            server: 2,
            granted: true,
            pre: false,
            ..
        }
    )));
}

/// Leadership transfer (thesis §3.10): the leader sends TimeoutNow to a follower
/// whose log matches its own, the follower starts an election without a pre-vote,
/// its vote requests carry the leader's wish, and followers that heard from the
/// old leader vote anyway; the old leader steps down on the higher term.
#[test]
fn leadership_transfer_makes_the_target_leader_without_a_pre_vote() {
    let members = vec![s(1), s(2), s(3)];
    let mut cluster = Cluster::new(&members, &config(Variant::Correct), &[]);
    cluster.elect(s(1));
    cluster.propose(s(1), "x");
    cluster.settle();
    let term = cluster.term(s(1));
    let out = cluster.step(s(1), Input::Transfer(s(2)));
    assert!(
        out.iter().any(
            |o| matches!(o, Output::Send { to, message: Message::TimeoutNow { .. } } if *to == s(2))
        ),
        "TimeoutNow went to the caught-up follower: {out:?}"
    );
    cluster.settle();
    assert_eq!(cluster.role(s(2)), Role::Leader);
    assert_eq!(cluster.term(s(2)), term + 1);
    assert_eq!(cluster.role(s(1)), Role::Follower);
    let pre_votes = cluster
        .events
        .iter()
        .filter(|e| matches!(e, TraceEvent::RaftVote { pre: true, term: t, .. } if *t == term + 1))
        .count();
    assert_eq!(pre_votes, 0, "no pre-vote round for a transfer");
    assert!(
        cluster
            .events
            .iter()
            .any(|e| matches!(e, TraceEvent::RaftTransfer { server: 1, to: 2 }))
    );
    assert!(
        cluster.events.iter().any(|e| matches!(e, TraceEvent::RaftVote { server: 3, granted: true, pre: false, term: t, .. } if *t == term + 1)),
        "server 3 voted although it had heard from server 1"
    );
}

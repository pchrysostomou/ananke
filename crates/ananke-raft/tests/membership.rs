//! Joint-consensus membership changes against the pure core (RAFT.md §1, thesis
//! §4.3), no simulator: a handful of cores stepped by hand with messages delivered
//! in the order each scenario needs. The joint entry commits only with majorities
//! of both voter sets and an election while joint needs both; a leader outside
//! `C_new` drives the change but does not count itself and steps down once `C_new`
//! commits; one change is in flight at a time; a truncation reverts the
//! configuration to the latest surviving entry; and the buggy variant that counts
//! one merged majority is shown breaking its rule at the core level, before the
//! simulator sees it (CLAUDE.md, the pair rule).

use std::collections::BTreeMap;

use ananke_env::TraceEvent;
use ananke_raft::core::{Input, Output, Raft, RaftConfig, Role, Variant};
use ananke_raft::invariants;
use ananke_raft::message::Message;
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId};
use bytes::Bytes;

fn s(n: u64) -> ServerId {
    ServerId(n)
}

fn ids(ns: &[u64]) -> Vec<ServerId> {
    ns.iter().copied().map(ServerId).collect()
}

fn config(variant: Variant) -> RaftConfig {
    RaftConfig {
        election_ticks: (10, 20),
        heartbeat_ticks: 2,
        max_batch: 1,
        max_inflight: 8,
        variant,
        ..RaftConfig::default()
    }
}

/// Whether a message is an AppendEntries carrying a configuration entry.
fn carries_config(message: &Message) -> bool {
    matches!(message, Message::AppendEntries { entries, .. }
        if entries.iter().any(|e| matches!(e.payload, Payload::Config(_))))
}

/// Whether the link runs between the leader (server 1) and a learner (4 or 5).
fn learner_link(from: ServerId, to: ServerId) -> bool {
    (from == s(1) && (to == s(4) || to == s(5))) || ((from == s(4) || from == s(5)) && to == s(1))
}

/// A few cores, the messages in flight between them, and the trace they produced:
/// enough to drive a membership change step by step. Servers outside the initial
/// configuration start with [`Configuration::default`], no voters at all, the way
/// a fresh server waiting to be added does.
struct Group {
    cores: BTreeMap<ServerId, Raft>,
    inbox: Vec<(ServerId, ServerId, Message)>,
    events: Vec<TraceEvent>,
}

impl Group {
    fn new(members: &[(u64, &[u64])], variant: Variant) -> Self {
        let cores = members
            .iter()
            .map(|&(id, voters)| {
                let membership = if voters.is_empty() {
                    Configuration::default()
                } else {
                    Configuration::of(&ids(voters))
                };
                (
                    s(id),
                    Raft::restore(
                        s(id),
                        membership,
                        config(variant),
                        id * 7919,
                        0,
                        None,
                        vec![],
                    ),
                )
            })
            .collect();
        Self {
            cores,
            inbox: Vec::new(),
            events: Vec::new(),
        }
    }

    fn step(&mut self, id: ServerId, input: Input) -> Vec<Output> {
        let outputs = self.cores.get_mut(&id).expect("a member").step(input);
        for output in &outputs {
            match output {
                Output::Send { to, message } => self.inbox.push((id, *to, message.clone())),
                Output::Trace(event) => self.events.push(event.clone()),
                _ => {}
            }
        }
        outputs
    }

    /// Drains the messages in flight once: those `allow` lets through are
    /// delivered, the rest stay in flight, held back for a later drain; the
    /// deliveries' own sends join the inbox behind them. Returns whether anything
    /// was delivered.
    fn deliver_where(&mut self, allow: &impl Fn(ServerId, ServerId, &Message) -> bool) -> bool {
        let batch = std::mem::take(&mut self.inbox);
        let mut delivered = false;
        let mut kept = Vec::new();
        for (from, to, message) in batch {
            if allow(from, to, &message) {
                delivered = true;
                self.step(
                    to,
                    Input::Message {
                        from,
                        message,
                        now: 0,
                    },
                );
            } else {
                kept.push((from, to, message));
            }
        }
        kept.append(&mut self.inbox);
        self.inbox = kept;
        delivered
    }

    /// Delivers until nothing `allow` lets through is in flight; what it holds
    /// back stays in flight.
    fn settle_where(&mut self, allow: impl Fn(ServerId, ServerId, &Message) -> bool) {
        while self.deliver_where(&allow) {}
    }

    /// Delivers every message in flight until nothing is.
    fn settle(&mut self) {
        self.settle_where(|_, _, _| true);
    }

    fn tick(&mut self, id: ServerId, ticks: u64) {
        for _ in 0..ticks {
            self.step(id, Input::Tick);
        }
    }

    /// Makes `id` the leader: the other voters' timers age past the minimum, then
    /// `id` is ticked until its election fires and settles.
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
            if self.cores[&id].role() == Role::Leader {
                return;
            }
        }
        panic!("{id} was not elected");
    }

    fn propose(&mut self, id: ServerId, command: &str) {
        self.step(id, Input::Propose(Bytes::from(command.to_owned())));
    }

    fn commit(&self, id: ServerId) -> Index {
        self.cores[&id].commit()
    }

    fn membership(&self, id: ServerId) -> &Configuration {
        self.cores[&id].membership()
    }
}

/// Whether the outputs refuse the request.
fn rejected(outputs: &[Output]) -> bool {
    outputs.iter().any(|o| matches!(o, Output::Rejected { .. }))
}

/// A group of {1, 2, 3} with servers 4 and 5 waiting outside, its leader elected
/// and one command committed, the change to {1, 2, 3, 4, 5} accepted, and the
/// learners caught up over the leader-and-learners links alone — with the joint
/// entry itself held back in flight, so each test decides who receives it. On
/// return the joint entry is in force on the leader only, uncommitted.
fn grown_to_joint(variant: Variant) -> (Group, Index) {
    let mut group = Group::new(
        &[
            (1, &[1, 2, 3]),
            (2, &[1, 2, 3]),
            (3, &[1, 2, 3]),
            (4, &[]),
            (5, &[]),
        ],
        variant,
    );
    group.elect(s(1));
    group.propose(s(1), "a");
    group.settle();
    assert_eq!(group.commit(s(1)), 2, "the no-op and the command committed");
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3, 4, 5])));
    assert!(!rejected(&outputs), "the change was accepted");
    // Catch-up: only the leader and the learners talk, and the joint entry the
    // catch-up ends in is held back in flight.
    group.settle_where(|from, to, m| learner_link(from, to) && !carries_config(m));
    let joint = group.cores[&s(1)].membership_index();
    assert!(
        group.membership(s(1)).new_voters.is_some(),
        "the joint entry is in force on the leader"
    );
    assert_eq!(
        joint, 3,
        "the joint entry follows the no-op and the command"
    );
    assert_eq!(group.commit(s(1)), 2, "the joint entry is uncommitted");
    (group, joint)
}

/// The joint entry needs majorities of BOTH voter sets to commit (thesis §4.3):
/// the leader with both learners is not a majority of `C_old`, the leader with one
/// old voter is not a majority of `C_new`, and one of each completes it, after
/// which the change drives itself to `C_new`.
#[test]
fn a_joint_entry_commits_only_with_majorities_of_both_voter_sets() {
    // Missing the old majority: the learners' acknowledgements alone.
    let (mut group, joint) = grown_to_joint(Variant::Correct);
    group.settle_where(|from, to, _| learner_link(from, to));
    assert_eq!(
        group.commit(s(1)),
        joint - 1,
        "leader, 4 and 5 are no majority of the old voters"
    );
    // Server 2 completes both majorities: 1 and 2 of the old set, 1, 2, 4 and 5
    // of the new.
    group.settle_where(|from, to, _| to == s(2) || (from == s(2) && to == s(1)));
    assert!(
        group.commit(s(1)) >= joint,
        "both majorities commit the joint entry"
    );
    group.settle();
    let leader = group.membership(s(1));
    assert_eq!(leader.new_voters, None, "the change completed");
    assert_eq!(leader.voters, ids(&[1, 2, 3, 4, 5]));
    invariants::all(&group.events).unwrap();
    invariants::commit_majority(&group.events, 3).unwrap();

    // Missing the new majority: an old-majority acknowledgement alone. The
    // learners acknowledged only the log below the joint entry, so the count for
    // it is servers 1 and 2: a majority of the old three, two of the new five.
    let (mut group, joint) = grown_to_joint(Variant::Correct);
    group.settle_where(|from, to, _| to == s(2) || (from == s(2) && to == s(1)));
    assert_eq!(
        group.commit(s(1)),
        joint - 1,
        "an old majority alone does not commit the joint entry"
    );
    group.settle();
    assert!(group.commit(s(1)) >= joint);
    invariants::all(&group.events).unwrap();
    invariants::commit_majority(&group.events, 3).unwrap();
}

/// An election while joint needs majorities of both voter sets (thesis §4.3): a
/// candidate with both learners' grants waits, one old voter's grant completes
/// it; the buggy variant elects on the merged majority alone.
#[test]
fn an_election_while_joint_needs_majorities_of_both_voter_sets() {
    for variant in [Variant::Correct, Variant::SingleMajorityInJointConsensus] {
        let (mut group, joint) = grown_to_joint(variant);
        // The leader's sends reach everyone, so every server holds the joint
        // entry, uncommitted; the acknowledgements stay held back, and the old
        // leader then goes quiet.
        group.settle_where(|from, _, _| from == s(1));
        group.inbox.clear();
        for id in [2, 3, 4, 5] {
            assert!(
                group.membership(s(id)).new_voters.is_some(),
                "server {id} holds the joint entry"
            );
        }
        assert_eq!(group.commit(s(3)), joint - 1, "uncommitted everywhere");
        // Server 3 campaigns; grants come from the learners first.
        let min = group.cores[&s(3)].config().election_ticks.0;
        group.tick(s(3), 2 * min);
        group.inbox.clear();
        let term = group.cores[&s(3)].term();
        let pre_vote = |group: &mut Group, from: u64| {
            group.step(
                s(3),
                Input::Message {
                    from: s(from),
                    message: Message::PreVoteResponse {
                        term: term + 1,
                        granted: true,
                    },
                    now: 0,
                },
            );
        };
        let vote = |group: &mut Group, from: u64| {
            group.step(
                s(3),
                Input::Message {
                    from: s(from),
                    message: Message::RequestVoteResponse {
                        term: term + 1,
                        granted: true,
                    },
                    now: 0,
                },
            );
        };
        pre_vote(&mut group, 4);
        pre_vote(&mut group, 5);
        match variant {
            Variant::Correct => {
                assert_eq!(
                    group.cores[&s(3)].role(),
                    Role::PreCandidate,
                    "3, 4 and 5 are no majority of the old voters"
                );
                pre_vote(&mut group, 2);
                assert_eq!(group.cores[&s(3)].role(), Role::Candidate);
                group.inbox.clear();
                vote(&mut group, 4);
                vote(&mut group, 5);
                assert_eq!(
                    group.cores[&s(3)].role(),
                    Role::Candidate,
                    "votes from 3, 4 and 5 alone elect nobody while joint"
                );
                vote(&mut group, 2);
                assert_eq!(group.cores[&s(3)].role(), Role::Leader);
            }
            _ => {
                assert_eq!(
                    group.cores[&s(3)].role(),
                    Role::Candidate,
                    "the buggy variant counts 3, 4 and 5 as one merged majority"
                );
                group.inbox.clear();
                vote(&mut group, 4);
                vote(&mut group, 5);
                assert_eq!(
                    group.cores[&s(3)].role(),
                    Role::Leader,
                    "the buggy variant elects without an old-set majority"
                );
            }
        }
    }
}

/// The buggy variant commits the joint entry on one merged majority, without a
/// majority of the old voters, and the commit-majority fold over the trace
/// reports it; the correct core waits (the pair rule, at the core level).
#[test]
fn the_single_majority_variant_commits_without_an_old_majority_and_is_seen() {
    for variant in [Variant::Correct, Variant::SingleMajorityInJointConsensus] {
        let (mut group, joint) = grown_to_joint(variant);
        // Only the learners acknowledge the joint entry: 1, 4 and 5 are three of
        // the five merged voters but one of the three old ones.
        group.settle_where(|from, to, _| learner_link(from, to));
        let verdict = invariants::commit_majority(&group.events, 3);
        match variant {
            Variant::Correct => {
                assert_eq!(group.commit(s(1)), joint - 1, "no old majority, no commit");
                verdict.unwrap();
            }
            _ => {
                assert!(
                    group.commit(s(1)) >= joint,
                    "the buggy variant committed on the merged majority"
                );
                let violation = verdict.unwrap_err();
                assert!(violation.contains("commit majority"), "{violation}");
            }
        }
    }
}

/// A leader that is not in `C_new` drives the change, does not count itself for
/// the majorities that commit `C_new`, and steps down once it is committed
/// (thesis §4.3).
#[test]
fn a_leader_outside_c_new_steps_down_once_c_new_commits_and_does_not_count_itself() {
    let mut group = Group::new(
        &[(1, &[1, 2, 3]), (2, &[1, 2, 3]), (3, &[1, 2, 3])],
        Variant::Correct,
    );
    group.elect(s(1));
    group.settle();
    let outputs = group.step(s(1), Input::Change(ids(&[2, 3])));
    assert!(
        !rejected(&outputs),
        "a change that only removes is accepted"
    );
    let joint = group.cores[&s(1)].membership_index();
    assert!(group.membership(s(1)).new_voters.is_some());
    // Everything flows except `C_new` towards server 3: the joint entry commits
    // on both majorities, `C_new` follows and server 2 acknowledges it — but 2
    // alone is one voter of {2, 3}, and the leader does not count itself.
    let c_new = joint + 1;
    group.settle_where(|_, to, m| {
        !(to == s(3)
            && matches!(m, Message::AppendEntries { entries, .. }
                if entries.iter().any(|e| e.index >= c_new)))
    });
    assert!(group.commit(s(1)) >= joint, "the joint entry committed");
    assert_eq!(
        group.cores[&s(1)].membership_index(),
        c_new,
        "C_new was proposed once the joint entry committed"
    );
    assert_eq!(group.membership(s(1)).new_voters, None);
    assert_eq!(group.membership(s(1)).voters, ids(&[2, 3]));
    assert_eq!(
        group.commit(s(1)),
        c_new - 1,
        "2 alone is no majority of C_new, and the leader does not count itself"
    );
    assert_eq!(
        group.cores[&s(1)].role(),
        Role::Leader,
        "still driving the change"
    );
    // Server 3 completes C_new's majority; the leader steps down.
    group.settle();
    assert!(group.commit(s(1)) >= c_new, "C_new committed");
    assert_eq!(
        group.cores[&s(1)].role(),
        Role::Follower,
        "a leader outside C_new steps down once C_new is committed"
    );
    invariants::all(&group.events).unwrap();
    invariants::commit_majority(&group.events, 3).unwrap();
}

/// One change is in flight at a time, the learner catch-up phase included: a
/// request for different voters is refused, one for the voters of the change
/// under way asks for what is already happening, and the next change is accepted
/// only once the last completed.
#[test]
fn one_change_is_in_flight_at_a_time() {
    let mut group = Group::new(
        &[
            (1, &[1, 2, 3]),
            (2, &[1, 2, 3]),
            (3, &[1, 2, 3]),
            (4, &[]),
            (5, &[]),
        ],
        Variant::Correct,
    );
    group.elect(s(1));
    group.settle();
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3, 4, 5])));
    assert!(!rejected(&outputs), "the change was accepted");
    // Catch-up is under way: a different change is refused, the same one is not.
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3, 4])));
    assert!(
        rejected(&outputs),
        "a different change while one is in flight"
    );
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3, 4, 5])));
    assert!(!rejected(&outputs), "the change already under way");
    // The joint phase refuses a different change too.
    group.settle_where(|from, to, _| learner_link(from, to));
    assert!(
        group.membership(s(1)).new_voters.is_some(),
        "joint in force"
    );
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2])));
    assert!(rejected(&outputs), "a different change while joint");
    // The change completes; the next one is accepted, with nobody to catch up.
    group.settle();
    assert_eq!(group.membership(s(1)).voters, ids(&[1, 2, 3, 4, 5]));
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3])));
    assert!(
        !rejected(&outputs),
        "the next change, once the last completed"
    );
    assert!(
        group.membership(s(1)).new_voters.is_some(),
        "joint at once: nobody to catch up"
    );
    // A change to the voters already in force asks for nothing and is not refused.
    group.settle();
    assert_eq!(group.membership(s(1)).voters, ids(&[1, 2, 3]));
    let before = group.cores[&s(1)].membership_index();
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3])));
    assert!(!rejected(&outputs));
    assert_eq!(
        group.cores[&s(1)].membership_index(),
        before,
        "nothing was proposed for a change to the voters in force"
    );
    invariants::all(&group.events).unwrap();
    invariants::commit_majority(&group.events, 3).unwrap();
}

/// A learner's catch-up is measured in rounds against the election timeout
/// (thesis §4.2.1): a round longer than the minimum timeout starts another at
/// the leader's then-last index, and only a round within it promotes.
#[test]
fn a_slow_catch_up_round_starts_another_and_a_fast_one_promotes() {
    let mut group = Group::new(
        &[(1, &[1, 2, 3]), (2, &[1, 2, 3]), (3, &[1, 2, 3]), (4, &[])],
        Variant::Correct,
    );
    group.elect(s(1));
    for i in 0..5 {
        group.propose(s(1), &format!("c{i}"));
    }
    group.settle();
    let outputs = group.step(s(1), Input::Change(ids(&[1, 2, 3, 4])));
    assert!(!rejected(&outputs));
    // The first round takes longer than the minimum election timeout: the
    // leader's clock ticks past it before the learner's acknowledgements arrive.
    let min = group.cores[&s(1)].config().election_ticks.0;
    group.inbox.clear();
    group.tick(s(1), min + 2);
    group.settle_where(|from, to, _| (from == s(1) && to == s(4)) || (from == s(4) && to == s(1)));
    assert!(
        group.membership(s(1)).new_voters.is_none(),
        "a round past the timeout does not promote"
    );
    // The next round is quick: two ticks bring a heartbeat, the acknowledgement
    // covers the last index at once, and the joint entry follows.
    group.tick(s(1), 2);
    group.settle_where(|from, to, _| (from == s(1) && to == s(4)) || (from == s(4) && to == s(1)));
    assert!(
        group.membership(s(1)).new_voters.is_some(),
        "a round within the timeout promotes the learner"
    );
}

/// A truncation that removes the configuration entry in force reverts to the
/// latest surviving one, or to the initial configuration, and the step's persist
/// carries the reverted configuration in the same batch (RAFT.md §1, §3).
#[test]
fn a_truncation_reverts_the_configuration_and_the_persist_carries_it() {
    let initial = Configuration::of(&ids(&[1, 2, 3]));
    let mut follower = Raft::restore(
        s(2),
        initial.clone(),
        config(Variant::Correct),
        1,
        1,
        None,
        vec![Entry {
            term: 1,
            index: 1,
            payload: Payload::Noop,
        }],
    );
    // The leader of term 1 appends a joint entry at index 2.
    let joint = Configuration {
        voters: ids(&[1, 2, 3]),
        new_voters: Some(ids(&[1, 2, 3, 4, 5])),
        learners: Vec::new(),
    };
    let outputs = follower.step(Input::Message {
        from: s(1),
        now: 0,
        message: Message::AppendEntries {
            term: 1,
            prev_index: 1,
            prev_term: 1,
            entries: vec![Entry {
                term: 1,
                index: 2,
                payload: Payload::Config(joint.clone()),
            }],
            commit: 1,
            sent: 0,
        },
    });
    assert_eq!(follower.membership(), &joint, "adopted on append");
    assert_eq!(follower.membership_index(), 2);
    let persisted = outputs.iter().find_map(|o| match o {
        Output::Persist(p) => p.config.clone(),
        _ => None,
    });
    assert_eq!(
        persisted,
        Some((2, joint)),
        "the persist carries the configuration in force"
    );
    // A new leader of term 2 never had the joint entry: its conflicting entry
    // truncates it, and the configuration reverts to the initial one.
    let outputs = follower.step(Input::Message {
        from: s(3),
        now: 0,
        message: Message::AppendEntries {
            term: 2,
            prev_index: 1,
            prev_term: 1,
            entries: vec![Entry {
                term: 2,
                index: 2,
                payload: Payload::Command(Bytes::from_static(b"x")),
            }],
            commit: 1,
            sent: 0,
        },
    });
    assert_eq!(
        follower.membership(),
        &initial,
        "reverted to the initial configuration"
    );
    assert_eq!(follower.membership_index(), 0);
    let persisted = outputs.iter().find_map(|o| match o {
        Output::Persist(p) => p.config.clone(),
        _ => None,
    });
    assert_eq!(persisted, Some((0, initial)));
}

/// A restart restores the configuration in force from the log the store hands
/// back: the latest configuration entry wins over the initial configuration.
#[test]
fn a_restart_restores_the_latest_configuration_entry_from_the_log() {
    let joint = Configuration {
        voters: ids(&[1, 2, 3]),
        new_voters: Some(ids(&[1, 2, 3, 4, 5])),
        learners: Vec::new(),
    };
    let restored = Raft::restore(
        s(2),
        Configuration::of(&ids(&[1, 2, 3])),
        config(Variant::Correct),
        1,
        1,
        None,
        vec![
            Entry {
                term: 1,
                index: 1,
                payload: Payload::Noop,
            },
            Entry {
                term: 1,
                index: 2,
                payload: Payload::Config(joint.clone()),
            },
        ],
    );
    assert_eq!(restored.membership(), &joint);
    assert_eq!(restored.membership_index(), 2);
}

/// A server with no configuration and an empty log sits quiet: its election
/// timer fires and fires and it campaigns for nothing, sends nothing, and stays
/// a follower, so a fresh server waiting to be added cannot disturb anyone.
#[test]
fn a_server_with_no_configuration_sits_quiet() {
    let mut fresh = Raft::restore(
        s(4),
        Configuration::default(),
        config(Variant::Correct),
        4,
        0,
        None,
        vec![],
    );
    let mut outputs = Vec::new();
    for _ in 0..200 {
        outputs.extend(fresh.step(Input::Tick));
    }
    assert_eq!(fresh.role(), Role::Follower);
    assert_eq!(fresh.term(), 0);
    assert!(
        outputs.is_empty(),
        "no sends, no persists, no role changes: {outputs:?}"
    );
}

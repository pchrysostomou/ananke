//! `Input::Campaign` (SHARD.md §5, Q21; PROPOSED D-100): a follower with no leader
//! pre-votes at once and again every heartbeat interval until it hears from a leader
//! or its own timer fires, and a pre-vote changes no term.

use ananke_raft::core::{RaftConfig, Role};
use ananke_raft::types::{Configuration, ServerId};
use ananke_raft::{Input, Message, Output, Raft};

fn config() -> RaftConfig {
    RaftConfig {
        election_ticks: (10, 20),
        heartbeat_ticks: 2,
        ..RaftConfig::default()
    }
}

fn pre_votes(outputs: &[Output]) -> usize {
    outputs
        .iter()
        .filter(|output| {
            matches!(
                output,
                Output::Send {
                    message: Message::PreVote { .. },
                    ..
                }
            )
        })
        .count()
}

#[test]
fn a_campaign_pre_votes_now_and_every_heartbeat_interval_until_a_leader_is_heard() {
    let members = Configuration::of(&[ServerId(1), ServerId(2), ServerId(3)]);
    let mut core = Raft::new(ServerId(1), members, config(), 7);
    assert_eq!(core.role(), Role::Follower);
    let outputs = core.step(Input::Campaign);
    assert_eq!(pre_votes(&outputs), 2, "a pre-vote to each peer at once");
    assert_eq!(core.term(), 0, "a pre-vote changes no term");
    // Nothing on the next tick, the pre-vote again at the heartbeat interval.
    assert_eq!(pre_votes(&core.step(Input::Tick)), 0);
    assert_eq!(pre_votes(&core.step(Input::Tick)), 2);
    assert_eq!(pre_votes(&core.step(Input::Tick)), 0);
    assert_eq!(pre_votes(&core.step(Input::Tick)), 2);
    assert_eq!(core.term(), 0);
    // A leader heard ends the hurry: no pre-vote on the intervals after it.
    let append = Message::AppendEntries {
        term: 1,
        prev_index: 0,
        prev_term: 0,
        entries: Vec::new(),
        commit: 0,
        sent: 0,
    };
    core.step(Input::Message {
        from: ServerId(2),
        message: append,
        now: 0,
    });
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term(), 1);
    for _ in 0..8 {
        assert_eq!(pre_votes(&core.step(Input::Tick)), 0);
    }
}

#[test]
fn a_campaign_does_nothing_for_a_server_that_knows_a_leader_or_cannot_win() {
    let members = Configuration::of(&[ServerId(1), ServerId(2), ServerId(3)]);
    let mut core = Raft::new(ServerId(1), members.clone(), config(), 7);
    core.step(Input::Message {
        from: ServerId(2),
        message: Message::AppendEntries {
            term: 1,
            prev_index: 0,
            prev_term: 0,
            entries: Vec::new(),
            commit: 0,
            sent: 0,
        },
        now: 0,
    });
    assert_eq!(
        pre_votes(&core.step(Input::Campaign)),
        0,
        "a leader is known"
    );
    // A learner is not a voter and campaigns for nothing.
    let mut learner = Raft::new(ServerId(9), members, config(), 7);
    assert_eq!(pre_votes(&learner.step(Input::Campaign)), 0);
    assert_eq!(learner.role(), Role::Follower);
}

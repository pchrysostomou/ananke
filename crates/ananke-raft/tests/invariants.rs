//! What the group key decides in checks 1 to 4 (SHARD.md §8), on records written
//! by hand.
//!
//! Every event of every sweep in this tree carries one group, so a check keyed by
//! term, server or index alone says exactly what one keyed by (group, term),
//! (group, server) or (group, index) says, on every seed at every tier: the sweeps
//! cannot tell the two apart. These are the traces that can. Each case is a pair:
//! a trace of two groups that the keyed check accepts and a check keyed without
//! the group rejects, and a trace of one group that is a real violation and the
//! keyed check rejects, so that keying a check has not widened it into a check of
//! nothing.
//!
//! The two groups are [`A`] and [`B`]: [`SINGLE_GROUP`], the group today's server
//! keeps its state under, and the next range id.

use ananke_env::{ApplyEffect, RangeCause, RangeRemovedCause, TraceEvent};
use ananke_raft::invariants::{self, Traced};
use ananke_raft::node::SINGLE_GROUP;
use bytes::Bytes;

/// One group: the one a server runs today.
const A: u64 = SINGLE_GROUP;
/// Another group on the same servers.
const B: u64 = SINGLE_GROUP + 1;

/// How many servers the checker takes as the initial configuration of a group
/// whose `RangeCreated` a trace does not hold.
const SERVERS: usize = 3;

fn leader(server: u64, range: u64, term: u64) -> TraceEvent {
    TraceEvent::RaftLeader {
        server,
        range,
        term,
        last_index: 0,
    }
}

fn role(server: u64, range: u64, term: u64, role: &'static str) -> TraceEvent {
    TraceEvent::RaftTerm {
        server,
        range,
        term,
        role,
        received: None,
    }
}

fn append(server: u64, range: u64, index: u64, entry_term: u64, hash: u64) -> TraceEvent {
    TraceEvent::RaftAppend {
        server,
        range,
        index,
        entry_term,
        hash,
    }
}

fn truncate(server: u64, range: u64, from_index: u64) -> TraceEvent {
    TraceEvent::RaftTruncate {
        server,
        range,
        from_index,
    }
}

fn commit(server: u64, range: u64, term: u64, index: u64) -> TraceEvent {
    TraceEvent::RaftCommit {
        server,
        range,
        term,
        index,
    }
}

fn apply(server: u64, range: u64, index: u64, entry_term: u64, hash: u64) -> TraceEvent {
    apply_to(server, range, index, entry_term, hash, ApplyEffect::Applied)
}

fn apply_to(
    server: u64,
    range: u64,
    index: u64,
    entry_term: u64,
    hash: u64,
    effect: ApplyEffect,
) -> TraceEvent {
    TraceEvent::RaftApply {
        server,
        range,
        index,
        entry_term,
        hash,
        key: Some(Bytes::from_static(b"k")),
        effect,
    }
}

/// A replica of `range` created with `voters`, its log starting above
/// `(floor_index, floor_term)`.
fn created(range: u64, voters: &[u64], floor_index: u64, floor_term: u64) -> TraceEvent {
    TraceEvent::RangeCreated {
        range,
        cause: RangeCause::Bootstrap,
        parent: None,
        start: Bytes::from_static(b""),
        end: Bytes::from_static(b"\xff"),
        generation: 1,
        voters: voters.to_vec(),
        floor_index,
        floor_term,
        incarnation: 2,
    }
}

fn removed(range: u64) -> TraceEvent {
    TraceEvent::RangeRemoved {
        range,
        generation: 1,
        incarnation: 2,
        cause: RangeRemovedCause::Collected,
    }
}

fn refused(server: u64) -> TraceEvent {
    TraceEvent::RaftRefused {
        server,
        reason: "a table the manifest names is gone".to_owned(),
    }
}

/// The events as a trace with no node: every event here names its server, so this
/// is every case but the two that fold `RangeCreated` and `RangeRemoved`.
fn check(events: &[TraceEvent]) -> Result<(), String> {
    invariants::all(events)
}

/// The events as a trace in which node `on` traced every one of them: what a
/// one-node stretch of a trace looks like to the checks that read the record's
/// node (`RangeCreated`, `RangeRemoved`).
fn check_on(on: u64, events: &[TraceEvent]) -> Result<(), String> {
    invariants::all(events.iter().map(|event| Traced::new(Some(on), event)))
}

/// The events with the node each was traced on, in order.
fn check_nodes(events: &[(u64, TraceEvent)]) -> Result<(), String> {
    invariants::all(
        events
            .iter()
            .map(|(node, event)| Traced::new(Some(*node), event)),
    )
}

// --- Check 1: election safety, keyed by (group, term) ---

/// Two groups electing in one term are two elections, not two leaders of one. A
/// checker keyed by the term alone — the key before SHARD.md §8 — reads this trace
/// as servers 1 and 2 both leading term 5.
#[test]
fn two_groups_may_elect_different_leaders_in_one_term() {
    check(&[leader(1, A, 5), leader(2, B, 5)]).unwrap();
}

/// And within one group it is the violation it always was.
#[test]
fn one_group_may_not_elect_two_leaders_in_one_term() {
    assert_eq!(
        check(&[leader(1, A, 5), leader(2, A, 5)]).unwrap_err(),
        "election safety: servers 1 and 2 both led term 5 of group 2"
    );
}

// --- Check 2: log matching, over logs and floors per (group, server) ---

/// One server's two groups hold two logs. Each group's index 1 is term 1 on both
/// servers, and the two groups' payloads there differ, as unrelated entries do; a
/// checker whose logs are keyed by the server alone reads server 1's replica of B
/// as server 1's log disagreeing with server 2's replica of A.
#[test]
fn two_groups_on_one_server_may_differ_at_one_index() {
    check(&[
        append(1, A, 1, 1, 0xaa),
        append(2, A, 1, 1, 0xaa),
        append(1, B, 1, 1, 0xbb),
        append(2, B, 1, 1, 0xbb),
    ])
    .unwrap();
}

/// And within one group two payloads at one index and term is the violation it
/// always was.
#[test]
fn one_group_may_not_hold_two_payloads_at_one_index_and_term() {
    assert_eq!(
        check(&[append(1, A, 1, 1, 0xaa), append(2, A, 1, 1, 0xbb)]).unwrap_err(),
        "log matching: servers 2 and 1 hold index 1 of group 2 in term 1 with different payloads"
    );
}

/// Two groups' snapshots at one index carry the terms of their own groups' entries
/// there; only two snapshots of one group must agree.
#[test]
fn two_groups_snapshots_at_one_index_may_carry_different_terms() {
    let snapshot = |server, range, last_index, last_term| TraceEvent::RaftSnapshot {
        server,
        range,
        last_index,
        last_term,
        taken: true,
    };
    check(&[snapshot(1, A, 7, 2), snapshot(2, B, 7, 4)]).unwrap();
    assert_eq!(
        check(&[snapshot(1, A, 7, 2), snapshot(2, A, 7, 4)]).unwrap_err(),
        "log matching: server 2's snapshot at index 7 of group 2 has term 4 but server 1's snapshot there has term 2"
    );
}

/// A `RangeCreated` sets its replica's floor as an installed snapshot does
/// (SHARD.md §8, check 2), so a group born above index 0 — a split's right half —
/// has a log and an applied index that start at its floor and nothing below it.
/// Without that floor the replica's first apply is an apply out of order, and the
/// leader's first commit covers five indices it is not holding.
#[test]
fn a_created_replica_starts_at_the_floor_its_creation_names() {
    check_on(
        1,
        &[
            created(B, &[1, 2, 3], 5, 1),
            append(1, B, 6, 1, 0xbb),
            apply(1, B, 6, 1, 0xbb),
        ],
    )
    .unwrap();
    // The same trace without the creation: the replica applies index 6 from
    // nothing.
    assert_eq!(
        check_on(1, &[append(1, B, 6, 1, 0xbb), apply(1, B, 6, 1, 0xbb)]).unwrap_err(),
        "state machine safety: server 1 applied index 6 of group 3 after 0"
    );
    // The floor is the log's too: the leader commits its first entry, index 6, and
    // the entries below it are inside the floor both replicas were created with. A
    // creation that set no floor leaves the check asking for index 1.
    let born = [
        (1, created(B, &[1, 2, 3], 5, 1)),
        (2, created(B, &[1, 2, 3], 5, 1)),
        (1, append(1, B, 6, 1, 0xbb)),
        (2, append(2, B, 6, 1, 0xbb)),
        (1, leader(1, B, 1)),
        (1, commit(1, B, 1, 6)),
    ];
    invariants::commit_majority(
        born.iter()
            .map(|(node, event)| Traced::new(Some(*node), event)),
        SERVERS,
    )
    .unwrap();
}

// --- Check 3: leader completeness, and commit on a majority ---

/// Each group's first configuration is its `RangeCreated`'s voters, not servers 1
/// through the cluster's count (SHARD.md §8, check 3): a group created on servers
/// 4, 5 and 6 commits on a majority of those three. A checker that takes `1..=3`
/// for every group reads this commit as a commit on none of its voters.
#[test]
fn a_group_commits_on_a_majority_of_the_voters_it_was_created_with() {
    let events = [
        created(B, &[4, 5, 6], 0, 0),
        append(4, B, 1, 1, 0xbb),
        append(5, B, 1, 1, 0xbb),
        leader(4, B, 1),
        commit(4, B, 1, 1),
    ];
    invariants::commit_majority(&events, SERVERS).unwrap();
}

/// And a commit short of a majority of *those* voters is the violation it always
/// was.
#[test]
fn a_group_may_not_commit_on_a_minority_of_the_voters_it_was_created_with() {
    let events = [
        created(B, &[4, 5, 6], 0, 0),
        append(4, B, 1, 1, 0xbb),
        leader(4, B, 1),
        commit(4, B, 1, 1),
    ];
    assert_eq!(
        invariants::commit_majority(&events, SERVERS).unwrap_err(),
        "commit majority: leader 4 committed index 1 of group 3 (term 1) in term 1 with it durable on [4] of voters [4, 5, 6]"
    );
}

/// The rescan at every `RaftLeader` is of that group's committed set: a new leader
/// of B need hold nothing of what A committed. A checker with one committed set
/// reads this as a leader missing a committed entry.
#[test]
fn a_new_leader_of_one_group_need_not_hold_another_groups_committed_entries() {
    check(&[
        append(1, A, 1, 1, 0xaa),
        leader(1, A, 1),
        commit(1, A, 1, 1),
        leader(2, B, 2),
    ])
    .unwrap();
}

/// And a new leader of the group that committed it must hold it, as ever.
#[test]
fn a_new_leader_of_the_group_that_committed_an_entry_must_hold_it() {
    assert_eq!(
        check(&[
            append(1, A, 1, 1, 0xaa),
            leader(1, A, 1),
            commit(1, A, 1, 1),
            leader(2, A, 2),
        ])
        .unwrap_err(),
        "leader completeness: index 1 of group 2 (term 1) was committed in term 1 but server 2, leader of term 2, does not hold it"
    );
}

/// Commit by the current term asks it of a leader's commits in the group it leads.
/// Server 1 leads A in term 2 and follows B in term 2, where the entry at index 1
/// is of term 1: a checker that remembers who leads by server alone reads the
/// follower's commit in B as the leader's and flags an older term's entry.
#[test]
fn a_leader_of_one_group_is_not_a_leader_of_another() {
    check(&[
        append(1, B, 1, 1, 0xbb),
        role(1, B, 2, "follower"),
        append(1, A, 1, 2, 0xaa),
        role(1, A, 2, "leader"),
        commit(1, B, 2, 1),
    ])
    .unwrap();
}

/// And in the group it does lead, a commit at an older term's entry is the
/// violation it always was.
#[test]
fn a_leader_may_not_commit_an_older_terms_entry_of_its_own_group() {
    assert_eq!(
        check(&[
            append(1, A, 1, 1, 0xaa),
            role(1, A, 2, "leader"),
            commit(1, A, 2, 1),
        ])
        .unwrap_err(),
        "commit by current term: leader 1 of term 2 committed index 1 of group 2, whose entry has term Some(1)"
    );
}

/// Committed entries stay, per (group, server): one server's commit index in A says
/// nothing about where it may truncate B. A checker keyed by the server alone reads
/// B's truncation as a truncation below A's commit index.
#[test]
fn a_commit_index_in_one_group_does_not_bind_another_groups_truncation() {
    check(&[commit(1, A, 1, 5), truncate(1, B, 1)]).unwrap();
}

/// And a truncation below the commit index of its own group is the violation it
/// always was.
#[test]
fn a_replica_may_not_truncate_below_its_own_groups_commit_index() {
    assert_eq!(
        check(&[commit(1, A, 1, 5), truncate(1, A, 3)]).unwrap_err(),
        "committed entries stay: server 1 truncated group 2 from index 3 with commit index 5"
    );
}

// --- Check 4: state machine safety, a map per group from index to the entry ---

/// Two groups' entries at index 1 are two entries. A checker whose applied map is
/// keyed by the index alone reads the second as the first applied differently.
#[test]
fn two_groups_may_apply_different_entries_at_one_index() {
    check(&[apply(1, A, 1, 1, 0xaa), apply(2, B, 1, 3, 0xbb)]).unwrap();
}

/// And within one group it is the violation it always was.
#[test]
fn one_group_may_not_apply_two_entries_at_one_index() {
    assert_eq!(
        check(&[apply(1, A, 1, 1, 0xaa), apply(2, A, 1, 3, 0xbb)]).unwrap_err(),
        "state machine safety: index 1 of group 2 was applied as term 1 on one server and term 3 on server 2"
    );
}

/// The value folded per index is (term, hash, effect), so two replicas that apply
/// one entry to different effects — one executing a client's command, another
/// refusing it — are seen at the second (SHARD.md §8, check 4). Two groups' effects
/// at one index are unrelated.
#[test]
fn one_group_may_not_apply_one_entry_to_two_effects() {
    check(&[
        apply_to(1, A, 1, 1, 0xaa, ApplyEffect::Applied),
        apply_to(2, B, 1, 1, 0xaa, ApplyEffect::Refused),
    ])
    .unwrap();
    assert_eq!(
        check(&[
            apply_to(1, A, 1, 1, 0xaa, ApplyEffect::Applied),
            apply_to(2, A, 1, 1, 0xaa, ApplyEffect::Refused),
        ])
        .unwrap_err(),
        "state machine safety: index 1 of group 2 was applied to effect applied on one server and refused on server 2"
    );
}

/// Applies are consecutive per (group, server): one server's first apply in B
/// follows nothing of its applies in A. A checker that counts one server's applies
/// in one sequence reads B's index 1 as index 1 applied twice.
#[test]
fn a_servers_applies_are_consecutive_within_each_group() {
    check(&[apply(1, A, 1, 1, 0xaa), apply(1, B, 1, 1, 0xbb)]).unwrap();
}

/// And within one group an index applied twice is the violation it always was.
#[test]
fn a_replica_may_not_apply_one_index_twice() {
    assert_eq!(
        check(&[apply(1, A, 1, 1, 0xaa), apply(1, A, 1, 1, 0xaa)]).unwrap_err(),
        "state machine safety: server 1 applied index 1 of group 2 after 1"
    );
}

/// A `RangeRemoved` ends that (group, server)'s memory the way a node's
/// `RaftRefused` ends its store's, so a group removed from a node and later created
/// there again applies from its new floor (SHARD.md §8, check 4, Q26).
#[test]
fn a_removed_replica_created_again_applies_from_its_new_floor() {
    check_on(
        1,
        &[
            created(A, &[1, 2, 3], 0, 0),
            apply(1, A, 1, 1, 0xaa),
            apply(1, A, 2, 1, 0xab),
            removed(A),
            created(A, &[1, 2, 3], 0, 0),
            apply(1, A, 1, 1, 0xaa),
        ],
    )
    .unwrap();
}

/// It ends that replica's memory and no other's: the removal of A from node 1
/// leaves node 1's replica of B where it was, and leaves node 2's replica of A
/// where it was. A checker that read the removal as the node's, or as the group's
/// everywhere, would let either re-apply an index it had already applied.
#[test]
fn a_removal_ends_one_replicas_memory_and_no_other() {
    assert_eq!(
        check_on(
            1,
            &[apply(1, B, 1, 1, 0xbb), removed(A), apply(1, B, 1, 1, 0xbb)],
        )
        .unwrap_err(),
        "state machine safety: server 1 applied index 1 of group 3 after 1"
    );
    assert_eq!(
        check_nodes(&[
            (2, apply(2, A, 1, 1, 0xaa)),
            (1, removed(A)),
            (2, apply(2, A, 1, 1, 0xaa)),
        ])
        .unwrap_err(),
        "state machine safety: server 2 applied index 1 of group 2 after 1"
    );
}

/// A node's refusal takes its store with it, and with it every group on the node
/// (SHARD.md §8): both replicas re-apply from their re-seeded floors.
#[test]
fn a_nodes_refusal_ends_every_groups_memory_on_it() {
    check(&[
        apply(1, A, 1, 1, 0xaa),
        apply(1, B, 1, 1, 0xbb),
        refused(1),
        apply(1, A, 1, 1, 0xaa),
        apply(1, B, 1, 1, 0xbb),
    ])
    .unwrap();
}

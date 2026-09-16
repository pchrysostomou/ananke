//! Stage E against the core and the store, no sweep (RAFT.md §1, §3): the
//! compacted log answers at its boundary, a leader feeds the snapshot when a
//! follower falls below the prefix and compacts only when every follower is past
//! the checkpoint or designated, a quarantined server grants nothing, and a whole
//! stream assembles, verifies, installs with the receiver's identity repaired
//! before `CURRENT`, and is adopted at the next open. The
//! `SnapshotWithoutCurrentLast` variant is shown leaving a complete-looking store
//! that carries the leader's identity when the install never finished — the state
//! the sweep's crash-mid-install seeds must catch.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Environment, File, FileSystem, OpenOptions, TraceEvent};
use ananke_raft::apply::{Command, apply_command, user_key};
use ananke_raft::core::{
    Input, Output, Persist, Raft, RaftConfig, Role, SnapshotAction, Variant, Variants,
};
use ananke_raft::format::{FORMAT_FILE, Verdict, check_format, record_format};
use ananke_raft::message::Message;
use ananke_raft::node::{SINGLE_GROUP, Start, StartOrder, start_store};
use ananke_raft::snapshot::{Assembler, Feed, Repair, Sender, adopt_staged, staging_dir, take};
use ananke_raft::store::{
    Damage, KeyPrefix, LostState, PURPOSE_CONFIG, RaftStore, STORE_MARKER, mark_store,
    mark_store_lost, marker_path, refuse_lost_store,
};
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId, Term};
use ananke_storage::manifest;
use ananke_storage::{Engine, EngineConfig};
use bytes::Bytes;

fn s(n: u64) -> ServerId {
    ServerId(n)
}

fn members(ids: &[u64]) -> Configuration {
    let ids: Vec<ServerId> = ids.iter().copied().map(ServerId).collect();
    Configuration::of(&ids)
}

fn entry(term: Term, index: Index, command: &str) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Command(Bytes::from(command.to_owned())),
    }
}

/// Steps `core`, collecting sends and keeping everything else.
fn sends(outputs: &[Output]) -> Vec<(ServerId, Message)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send { to, message } => Some((*to, message.clone())),
            _ => None,
        })
        .collect()
}

/// Delivers every message between two cores until nothing is in flight,
/// collecting the snapshot actions the cores emit along the way.
fn settle(
    a: &mut Raft,
    b: &mut Raft,
    mut inflight: Vec<(ServerId, ServerId, Message)>,
    actions: &mut Vec<SnapshotAction>,
) {
    while let Some((from, to, message)) = inflight.pop() {
        if matches!(
            message,
            Message::InstallSnapshot { .. } | Message::InstallSnapshotResponse { .. }
        ) {
            continue;
        }
        let core = if to == a.id() {
            &mut *a
        } else if to == b.id() {
            &mut *b
        } else {
            // A member with no core here: its messages fall on the floor.
            continue;
        };
        let outputs = core.step(Input::Message {
            from,
            message,
            now: 0,
        });
        for output in &outputs {
            if let Output::Snapshot(action) = output {
                actions.push(action.clone());
            }
        }
        for (peer, message) in sends(&outputs) {
            inflight.push((to, peer, message));
        }
    }
}

/// Elects `leader` against `follower`: the follower's timer ages first, its own
/// campaigning discarded, then the leader is ticked until it leads. Returns the
/// snapshot actions the election's replication asked for.
fn elect(leader: &mut Raft, follower: &mut Raft) -> Vec<SnapshotAction> {
    let min = leader.config().election_ticks.0;
    for _ in 0..min {
        let _ = follower.step(Input::Tick);
    }
    let mut actions = Vec::new();
    for _ in 0..200 {
        let outputs = leader.step(Input::Tick);
        let from = leader.id();
        let outbound: Vec<(ServerId, ServerId, Message)> = sends(&outputs)
            .into_iter()
            .map(|(to, message)| (from, to, message))
            .collect();
        settle(leader, follower, outbound, &mut actions);
        if leader.role() == Role::Leader {
            return actions;
        }
    }
    panic!("no election");
}

#[test]
fn the_compacted_log_answers_at_its_boundary() {
    let core = Raft::restore_compacted(
        s(1),
        members(&[1, 2, 3]),
        RaftConfig::default(),
        7,
        3,
        None,
        10,
        2,
        None,
        vec![entry(3, 11, "a"), entry(3, 12, "b")],
        false,
    );
    assert_eq!(core.snapshot(), (10, 2));
    assert_eq!(core.first_index(), 11);
    assert_eq!(core.last_index(), 12);
    assert_eq!(core.last_term(), 3);
    assert_eq!(core.term_at(9), None, "below the prefix");
    assert_eq!(core.term_at(10), Some(2), "the boundary is the snapshot's");
    assert_eq!(core.term_at(11), Some(3));
    assert!(core.entry(10).is_none());
    assert!(core.entry(11).is_some());
    // Empty tail: the election restriction reads the snapshot itself.
    let bare = Raft::restore_compacted(
        s(1),
        members(&[1, 2, 3]),
        RaftConfig::default(),
        7,
        3,
        None,
        10,
        2,
        None,
        Vec::new(),
        false,
    );
    assert_eq!((bare.last_index(), bare.last_term()), (10, 2));
}

#[test]
fn a_leader_feeds_the_snapshot_when_a_follower_falls_below_the_prefix() {
    let config = RaftConfig::default();
    let mut leader = Raft::restore_compacted(
        s(1),
        members(&[1, 2]),
        config.clone(),
        11,
        1,
        None,
        10,
        1,
        None,
        Vec::new(),
        false,
    );
    let mut follower = Raft::new(s(2), members(&[1, 2]), config, 13);
    let actions = elect(&mut leader, &mut follower);
    // The election's replication probed below the follower's empty log, whose
    // hint asked from index 1: at or below the compacted prefix, so the core
    // asked the snapshot task to install.
    assert!(
        actions.iter().any(|action| matches!(
            action,
            SnapshotAction::Install {
                to: ServerId(2),
                index: 10,
                term: 1
            }
        )),
        "the install ask: {actions:?}"
    );
    // Complete the install: replication resumes past the prefix.
    let outputs = leader.step(Input::SnapshotInstalled {
        to: s(2),
        index: 10,
        incarnation: 0,
    });
    let resumed = sends(&outputs);
    assert!(
        resumed.iter().any(|(to, message)| *to == s(2)
            && matches!(message, Message::AppendEntries { prev_index: 10, .. })),
        "replication resumes after the prefix: {resumed:?}"
    );
}

#[test]
fn a_leader_compacts_only_when_every_follower_is_past_the_checkpoint() {
    let config = RaftConfig {
        snapshot_threshold: 4,
        ..RaftConfig::default()
    };
    let mut leader = Raft::new(s(1), members(&[1, 2]), config.clone(), 17);
    let mut follower = Raft::new(s(2), members(&[1, 2]), config.clone(), 19);
    elect(&mut leader, &mut follower);
    for n in 0..5 {
        let outputs = leader.step(Input::Propose(Bytes::from(format!("c{n}"))));
        let from = leader.id();
        let outbound: Vec<_> = sends(&outputs)
            .into_iter()
            .map(|(to, message)| (from, to, message))
            .collect();
        settle(&mut leader, &mut follower, outbound, &mut Vec::new());
    }
    assert_eq!(leader.last_index(), 6, "a no-op and five commands");
    assert_eq!(leader.commit(), 6);
    // The log has outgrown the threshold, but nothing is applied yet: however
    // long the leader leads, no take. The follower keeps answering, so neither
    // check quorum nor the designation fires meanwhile.
    for _ in 0..3 * config.election_ticks.0 {
        let outputs = leader.step(Input::Tick);
        assert!(
            !outputs
                .iter()
                .any(|o| matches!(o, Output::Snapshot(SnapshotAction::Take))),
            "a take before anything applied"
        );
        let from = leader.id();
        let outbound: Vec<_> = sends(&outputs)
            .into_iter()
            .map(|(to, message)| (from, to, message))
            .collect();
        settle(&mut leader, &mut follower, outbound, &mut Vec::new());
    }
    // Applied past the threshold, and the leader has led long enough (a fresh
    // leader defers its first take): the next tick asks for exactly one take.
    let _ = leader.step(Input::Applied(5));
    let outputs = leader.step(Input::Tick);
    assert_eq!(
        outputs
            .iter()
            .filter(|o| matches!(o, Output::Snapshot(SnapshotAction::Take)))
            .count(),
        1,
        "the tick past the threshold asks for a take: {outputs:?}"
    );
    let outputs = leader.step(Input::Tick);
    assert!(
        !outputs
            .iter()
            .any(|o| matches!(o, Output::Snapshot(SnapshotAction::Take))),
        "one take at a time"
    );
    // The checkpoint lands; the follower has matched everything, so the log
    // compacts to it in the same step's persist.
    let outputs = leader.step(Input::SnapshotTaken { index: 5, term: 2 });
    let compacted = outputs.iter().find_map(|o| match o {
        Output::Persist(Persist { compact_to, .. }) => *compact_to,
        _ => None,
    });
    assert_eq!(compacted, Some(5), "{outputs:?}");
    assert!(outputs.iter().any(|o| matches!(
        o,
        Output::Trace(TraceEvent::RaftCompacted {
            server: 1,
            through: 5
        })
    )));
    assert_eq!(leader.snapshot(), (5, 2));
    assert_eq!(leader.first_index(), 6);
    assert_eq!(leader.term_at(5), Some(2));
    assert_eq!(leader.last_index(), 6);
}

#[test]
fn a_lagging_follower_blocks_compaction_until_it_is_designated() {
    let config = RaftConfig {
        snapshot_threshold: 2,
        ..RaftConfig::default()
    };
    // Three members: server 2 runs and acknowledges everything, server 3 is down
    // and silent, so commits go through while its match stays at zero.
    let mut leader = Raft::new(s(1), members(&[1, 2, 3]), config.clone(), 23);
    let mut follower = Raft::new(s(2), members(&[1, 2, 3]), config.clone(), 29);
    elect(&mut leader, &mut follower);
    for n in 0..4 {
        let outputs = leader.step(Input::Propose(Bytes::from(format!("c{n}"))));
        let from = leader.id();
        let outbound: Vec<_> = sends(&outputs)
            .into_iter()
            .map(|(to, message)| (from, to, message))
            .collect();
        settle(&mut leader, &mut follower, outbound, &mut Vec::new());
    }
    assert_eq!(leader.commit(), 5, "a no-op and four commands, on {{1, 2}}");
    let _ = leader.step(Input::Applied(5));
    // A checkpoint at the applied index: server 3's match of zero blocks the
    // compaction, undesignated as it still is.
    let outputs = leader.step(Input::SnapshotTaken { index: 5, term: 2 });
    assert!(
        !outputs.iter().any(|o| matches!(
            o,
            Output::Persist(Persist {
                compact_to: Some(_),
                ..
            })
        )),
        "a follower behind and undesignated blocks compaction: {outputs:?}"
    );
    // Two minimum election timeouts of silence while it lags by more than the
    // threshold designate it snapshot-fed, and compaction goes ahead.
    let quiet = 2 * config.election_ticks.0 + 2;
    let mut compacted = None;
    for _ in 0..quiet {
        let outputs = leader.step(Input::Tick);
        if let Some(to) = outputs.iter().find_map(|o| match o {
            Output::Persist(Persist { compact_to, .. }) => *compact_to,
            _ => None,
        }) {
            compacted = Some(to);
            break;
        }
    }
    assert_eq!(compacted, Some(5), "designation unblocks");
    assert_eq!(leader.snapshot(), (5, 2));
}

#[test]
fn a_quarantined_server_grants_nothing_campaigns_never_and_promises_nothing() {
    let config = RaftConfig::default();
    let mut core = Raft::restore_compacted(
        s(2),
        members(&[1, 2, 3]),
        config.clone(),
        31,
        1,
        None,
        5,
        1,
        None,
        Vec::new(),
        true,
    );
    assert!(core.quarantined());
    // No campaign, however long the timer runs.
    for _ in 0..10 * config.election_ticks.1 {
        let outputs = core.step(Input::Tick);
        assert!(
            sends(&outputs).is_empty(),
            "a quarantined server campaigned"
        );
    }
    assert_eq!(core.role(), Role::Follower);
    // No pre-vote and no vote, however worthy the candidate.
    let outputs = core.step(Input::Message {
        from: s(3),
        message: Message::PreVote {
            term: 2,
            last_index: 100,
            last_term: 1,
        },
        now: 0,
    });
    for (_, message) in sends(&outputs) {
        assert!(
            matches!(message, Message::PreVoteResponse { granted: false, .. }),
            "granted a pre-vote: {message:?}"
        );
    }
    let outputs = core.step(Input::Message {
        from: s(3),
        message: Message::RequestVote {
            term: 2,
            last_index: 100,
            last_term: 1,
            transfer: false,
        },
        now: 0,
    });
    for (_, message) in sends(&outputs) {
        assert!(
            matches!(message, Message::RequestVoteResponse { granted: false, .. }),
            "granted a vote: {message:?}"
        );
    }
    // An append is answered — it still replicates — but with an echo of zero:
    // no lease is ever measured from it.
    let outputs = core.step(Input::Message {
        from: s(1),
        message: Message::AppendEntries {
            term: 2,
            prev_index: 5,
            prev_term: 1,
            entries: vec![entry(2, 6, "x")],
            commit: 5,
            sent: 987_654,
        },
        now: 0,
    });
    let answered = sends(&outputs);
    let response = answered
        .iter()
        .find_map(|(_, m)| match m {
            Message::AppendEntriesResponse { success, echo, .. } => Some((*success, *echo)),
            _ => None,
        })
        .expect("an answer");
    assert_eq!(response, (true, 0), "replicates, promises nothing");
}

#[test]
fn an_append_reaching_below_the_prefix_is_answered_for_its_suffix() {
    let mut core = Raft::restore_compacted(
        s(2),
        members(&[1, 2, 3]),
        RaftConfig::default(),
        37,
        1,
        None,
        10,
        1,
        None,
        Vec::new(),
        false,
    );
    // Entirely covered: already held, by definition of the snapshot.
    let outputs = core.step(Input::Message {
        from: s(1),
        message: Message::AppendEntries {
            term: 1,
            prev_index: 4,
            prev_term: 1,
            entries: vec![entry(1, 5, "e5"), entry(1, 6, "e6")],
            commit: 6,
            sent: 0,
        },
        now: 0,
    });
    let response = sends(&outputs)
        .into_iter()
        .find_map(|(_, m)| match m {
            Message::AppendEntriesResponse {
                success,
                match_index,
                prev_index,
                ..
            } => Some((success, match_index, prev_index)),
            _ => None,
        })
        .expect("an answer");
    assert_eq!(
        response,
        (true, 6, 4),
        "covered entries match by definition"
    );
    assert_eq!(core.last_index(), 10, "nothing appended");
    // Straddling: the suffix past the prefix is appended.
    let outputs = core.step(Input::Message {
        from: s(1),
        message: Message::AppendEntries {
            term: 1,
            prev_index: 8,
            prev_term: 1,
            entries: vec![entry(1, 9, "e9"), entry(1, 10, "e10"), entry(1, 11, "e11")],
            commit: 11,
            sent: 0,
        },
        now: 0,
    });
    let response = sends(&outputs)
        .into_iter()
        .find_map(|(_, m)| match m {
            Message::AppendEntriesResponse {
                success,
                match_index,
                ..
            } => Some((success, match_index)),
            _ => None,
        })
        .expect("an answer");
    assert_eq!(response, (true, 11));
    assert_eq!(core.last_index(), 11);
    assert_eq!(core.entry(11).map(|e| e.term), Some(1));
    assert!(
        core.entry(10).is_none(),
        "the boundary stays the snapshot's"
    );
}

type Out<T> = Arc<Mutex<Option<T>>>;

/// Runs `f` on `node` until it finishes and returns what it produced.
fn on_node<T: Send + 'static>(
    sim: &mut Sim,
    node: ananke_env::NodeId,
    f: impl FnOnce(SimEnv) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>,
) -> T {
    let out: Out<T> = Arc::default();
    let o = out.clone();
    let env = sim.env(node);
    let fut = f(env.clone());
    env.spawn("test", async move {
        *o.lock().unwrap() = Some(fut.await);
    });
    while out.lock().unwrap().is_none() {
        sim.run_for(Duration::from_millis(1));
    }
    out.lock().unwrap().take().expect("the task finished")
}

fn engine_config(dir: &str) -> EngineConfig {
    let mut config = EngineConfig::new(PathBuf::from(dir));
    config.memtable_bytes = 4096;
    config.segment_bytes = 4096;
    config.allow_manifest_fallback = false;
    config.allow_head_gap = false;
    config.refuse_log_damage = true;
    config.background_compaction = true;
    config
}

/// Today's one group, whose Raft state a store keeps (PROPOSED D-060).
fn prefix() -> KeyPrefix {
    KeyPrefix::group(SINGLE_GROUP)
}

/// Records `dir`'s format as a fresh directory's first start does, for a test
/// that stages an install into a directory before any store of it exists: the
/// server records the format before anything else of the directory is written
/// (PROPOSED D-060), and the adoption of a directory with no record would
/// otherwise be refused as 0.3.0's.
async fn record_fresh_format(env: &SimEnv, dir: &str) {
    match check_format(env, Path::new(dir)).await.unwrap() {
        Verdict::Fresh(fresh) => {
            record_format(env, fresh).await.unwrap();
        }
        Verdict::Recorded { .. } => {}
        Verdict::Damaged => panic!("the record cannot be read"),
    }
}

/// Builds a leader-like store at `/leader` with five applied puts and a
/// configuration entry at index 6, past the checkpoint, whose `0 / 2 / config`
/// key rides in the checkpoint the way a real leader's does (D-029) — so an
/// install that failed to rewrite the key would open a store whose key names an
/// entry its log does not hold, and be refused. Takes a checkpoint at index 5
/// and returns the checkpoint directory.
async fn build_leader(env: &SimEnv) -> (Arc<RaftStore<SimEnv>>, PathBuf) {
    let (store, _) = RaftStore::open_dir(env.clone(), engine_config("/leader"), prefix())
        .await
        .unwrap();
    let store = Arc::new(store);
    let mut entries = Vec::new();
    for index in 1..=5u64 {
        entries.push(entry(1, index, &format!("ignored{index}")));
    }
    let grown = members(&[1, 2, 3, 4]);
    entries.push(Entry {
        term: 4,
        index: 6,
        payload: Payload::Config(grown.clone()),
    });
    store
        .persist(&Persist {
            term: 4,
            vote: Some(ServerId(1)),
            truncate_from: None,
            append: entries,
            config: Some((6, grown)),
            compact_to: None,
        })
        .await
        .unwrap();
    for index in 1..=5u64 {
        let command = Command::Put {
            key: Bytes::from(format!("k{index}")),
            value: Bytes::from(format!("v{index}")),
        };
        apply_command(&store, index, Some(&command)).await.unwrap();
    }
    let dir = PathBuf::from("/leader/snap-5");
    take(env, &store, &dir, 5, 1, &members(&[1, 2, 3]))
        .await
        .unwrap();
    (store, dir)
}

/// Streams the checkpoint into an assembler chunk by chunk, duplicating one chunk
/// to show the acknowledgement is idempotent, and returns the verified stage.
async fn stream(
    env: &SimEnv,
    dir: &Path,
    assembler: &mut Assembler<SimEnv>,
) -> ananke_raft::snapshot::Staged {
    let mut sender = Sender::open(env, dir, ServerId(2), 5, 1, 4).await.unwrap();
    let mut duplicated = false;
    loop {
        let chunk = sender.chunk(env, 64).await.unwrap();
        let Message::InstallSnapshot {
            term,
            last_index,
            last_term,
            file,
            offset,
            total,
            done,
            data,
        } = chunk
        else {
            panic!("a chunk")
        };
        let feed = assembler
            .on_chunk(
                ServerId(1),
                term,
                last_index,
                last_term,
                file.clone(),
                offset,
                total,
                done,
                data.clone(),
            )
            .await
            .unwrap();
        match feed {
            Feed::Ack {
                file: ack_file,
                offset: ack_offset,
            } => {
                if !duplicated {
                    // The network duplicated the chunk: the second copy moves
                    // nothing and the acknowledgement names the same next byte.
                    duplicated = true;
                    let again = assembler
                        .on_chunk(
                            ServerId(1),
                            term,
                            last_index,
                            last_term,
                            file,
                            offset,
                            total,
                            done,
                            data,
                        )
                        .await
                        .unwrap();
                    let Feed::Ack {
                        file: dup_file,
                        offset: dup_offset,
                    } = again
                    else {
                        panic!("an ack")
                    };
                    assert_eq!((&dup_file, dup_offset), (&ack_file, ack_offset));
                }
                let rewound = sender.on_more(&ack_file, ack_offset);
                assert!(!rewound, "nothing was lost in this stream");
            }
            Feed::Restart => panic!("the stream restarted"),
            Feed::Staged(staged) => return staged,
        }
    }
}

/// The whole receiver's path: assemble, verify, repair with the receiver's own
/// identity, `CURRENT` last, adopt at the next open — and no adoption before the
/// repair exists.
#[test]
fn an_install_carries_the_receivers_identity_and_is_adopted_at_open() {
    let mut sim = Sim::new(SimConfig::new(77));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            // The follower's directory as its first start leaves it: its format
            // recorded, before anything else of it (PROPOSED D-060).
            record_fresh_format(&env, "/follower").await;
            // First stream: assembled and verified, but the server "crashes"
            // before the repair — nothing to adopt, the debris is swept.
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let _staged = stream(&env, &dir, &mut assembler).await;
            assert!(
                !adopt_staged(&env, Path::new("/follower")).await.unwrap(),
                "an unrepaired staging directory must not win"
            );
            // Second stream: repaired with the receiver's identity, CURRENT last.
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let staged = stream(&env, &dir, &mut assembler).await;
            let repair = Repair {
                term: 7,
                vote: Some(ServerId(3)),
                tail: vec![entry(1, 6, "tail6"), entry(1, 7, "tail7")],
                quarantined: true,
                incarnation: 9,
            };
            assembler.finish(&staged, &repair).await.unwrap();
            assert!(adopt_staged(&env, Path::new("/follower")).await.unwrap());
            // Idempotent: a second look finds nothing left to adopt.
            assert!(!adopt_staged(&env, Path::new("/follower")).await.unwrap());
            // The adopted store: the leader's data at the snapshot, under the
            // receiver's own hard state, applied index, record and quarantine.
            let (store, recovered) =
                RaftStore::open_dir(env.clone(), engine_config("/follower"), prefix())
                    .await
                    .unwrap();
            let engine = store.engine().clone();
            assert_eq!(store.term(), 7);
            assert_eq!(store.vote(), Some(ServerId(3)));
            assert_eq!(
                store.incarnation(),
                9,
                "the repair's incarnation, not the leader's (D-042)"
            );
            assert_eq!(store.applied(), 5);
            assert_eq!((store.first_index(), store.last_index()), (6, 7));
            assert_eq!(
                recovered.log,
                vec![entry(1, 6, "tail6"), entry(1, 7, "tail7")]
            );
            let record = recovered.snapshot.expect("a record");
            assert_eq!(
                (record.last_index, record.last_term, record.taken),
                (5, 1, false)
            );
            assert!(record.dir.is_empty(), "an installed snapshot has no dir");
            assert!(recovered.quarantined, "the quarantine survives the switch");
            // The open above is itself the config-key check (RAFT.md §3, D-029):
            // the leader's checkpoint carried a `0 / 2 / config` naming its
            // entry 6, which this store's log does not hold — had the repair not
            // rewritten the key consistent with the snapshot's configuration,
            // the open would have refused the store as out of step.
            assert!(
                engine
                    .get(&prefix().key(PURPOSE_CONFIG, b"config"))
                    .await
                    .unwrap()
                    .is_some(),
                "the repair writes the configuration key"
            );
            for index in 1..=5u64 {
                let value = engine
                    .get(&user_key(format!("k{index}").as_bytes()))
                    .await
                    .unwrap();
                assert_eq!(
                    value,
                    Some(Bytes::from(format!("v{index}"))),
                    "the leader's state at the snapshot"
                );
            }
        })
    });
}

/// The buggy install (RAFT.md §5): the streamed `CURRENT` written the moment it
/// arrives. A crash mid-install then leaves a staging directory that looks like a
/// complete store and wins the adoption — carrying the *leader's* tenant 0, a
/// state this server never held. The sweep's crash-mid-install seeds turn this
/// into a state machine safety violation; here the wrong identity is asserted
/// directly.
#[test]
fn the_variant_that_writes_current_early_adopts_the_leaders_state_on_a_crash() {
    let mut sim = Sim::new(SimConfig::new(78));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            record_fresh_format(&env, "/follower").await;
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::SnapshotWithoutCurrentLast,
                prefix(),
            );
            let _staged = stream(&env, &dir, &mut assembler).await;
            // The server crashes here: the repair never runs. The staging
            // directory should not be a store yet — under the variant it is.
            assert!(
                adopt_staged(&env, Path::new("/follower")).await.unwrap(),
                "the variant's CURRENT made an unrepaired install win"
            );
            let (store, recovered) =
                RaftStore::open_dir(env.clone(), engine_config("/follower"), prefix())
                    .await
                    .unwrap();
            // The leader's identity, not this server's: the state that never
            // existed here.
            assert_eq!(store.term(), 4, "the leader's term");
            assert_eq!(store.vote(), Some(ServerId(1)), "the leader's vote");
            let record = recovered.snapshot.expect("the leader's record");
            assert!(record.taken, "the leader's own record, not an install's");
            assert!(!recovered.quarantined, "no repair, no quarantine");
        })
    });
}

/// A follower store at `/follower` with an identity of its own — term 3, a vote
/// for server 2, three entries of term 1 with two applied — marked as a store,
/// so an adoption's effect on both is visible.
async fn build_follower(env: &SimEnv) {
    let (store, _) = RaftStore::open_dir(env.clone(), engine_config("/follower"), prefix())
        .await
        .unwrap();
    store
        .persist(&Persist {
            term: 3,
            vote: Some(ServerId(2)),
            truncate_from: None,
            append: (1..=3u64)
                .map(|index| entry(1, index, &format!("own{index}")))
                .collect(),
            config: None,
            compact_to: None,
        })
        .await
        .unwrap();
    for index in 1..=2u64 {
        let command = Command::Put {
            key: Bytes::from(format!("own{index}")),
            value: Bytes::from(format!("mine{index}")),
        };
        apply_command(&store, index, Some(&command)).await.unwrap();
    }
    mark_store(env, Path::new("/follower")).await.unwrap();
}

/// Opens `/follower` the way the server does — the adoption first, then the
/// marker check, then the engine — and returns whether a store was adopted with
/// the store's term and vote, or the refusal's reason.
async fn open_follower(env: &SimEnv) -> Result<(bool, Term, Option<ServerId>), String> {
    match start_store(
        env,
        1,
        &engine_config("/follower"),
        Variants::correct(),
        &prefix(),
        StartOrder::Correct,
    )
    .await
    {
        Start::Opened { store, adopted, .. } => Ok((adopted, store.term(), store.vote())),
        Start::Refused(error) | Start::Failed(error) => Err(error.to_string()),
    }
}

/// The crash-safe adoption (D-041): a completed install at `/follower`
/// is adopted while the disk takes its time, and the node crashes partway — at a
/// different point on every seed. Whatever the point, what is durable afterwards
/// is a store: either the old store's `CURRENT` still naming the old manifest
/// with that manifest on disk, or the adopted store's, never neither; the start
/// that follows adopts (again, on the same staged bytes) or finds the adoption
/// already retired, and opens the receiver's identity — never a fresh store, and
/// never the old one, since the staged install outlives every crash before its
/// `CURRENT` is retired. The marker survives it all.
#[test]
fn a_crash_inside_the_adoption_leaves_a_store_and_the_next_start_adopts() {
    let mut outcomes = BTreeSet::new();
    for seed in 0..24u64 {
        let mut sim = Sim::new({
            let mut c = SimConfig::new(300 + seed);
            c.fs.latency_min = Duration::from_micros(100);
            c.fs.latency_max = Duration::from_millis(2);
            c
        });
        let node = sim.add_node();
        on_node(&mut sim, node, |env| {
            Box::pin(async move {
                let (_leader, dir) = build_leader(&env).await;
                build_follower(&env).await;
                let mut assembler = Assembler::new(
                    env.clone(),
                    Path::new("/follower"),
                    Variant::Correct,
                    prefix(),
                );
                let staged = stream(&env, &dir, &mut assembler).await;
                let repair = Repair {
                    term: 7,
                    vote: Some(ServerId(3)),
                    tail: Vec::new(),
                    quarantined: false,
                    // The receiver's own, as an install into a live store carries
                    // (D-042).
                    incarnation: 1,
                };
                assembler.finish(&staged, &repair).await.unwrap();
            })
        });
        // The adoption runs as the server would run it, and the crash lands
        // after a seed-dependent slice of it.
        let env = sim.env(node);
        let inner = env.clone();
        env.spawn("adopt", async move {
            let _ = adopt_staged(&inner, Path::new("/follower")).await;
        });
        sim.run_for(Duration::from_millis(1 + seed * 3));
        sim.crash(node);
        sim.restart(node);
        // What is durable is a store, whole: a CURRENT that parses and whose
        // manifest is on disk, and the marker.
        let current = sim
            .durable_contents(node, Path::new("/follower/CURRENT"))
            .expect("seed {seed}: a CURRENT survives every crash");
        let named = manifest::parse_current(&current).expect("a CURRENT that parses");
        assert!(
            sim.durable_contents(
                node,
                &manifest::manifest_path(Path::new("/follower"), named)
            )
            .is_some(),
            "seed {seed}: the manifest CURRENT names is on disk"
        );
        assert!(
            sim.durable_contents(node, &marker_path(Path::new("/follower")))
                .is_some(),
            "seed {seed}: the marker survives the adoption"
        );
        let staging_current = sim
            .durable_contents(
                node,
                &manifest::current_path(&staging_dir(Path::new("/follower"))),
            )
            .is_some();
        outcomes.insert((named == 1, staging_current));
        // The next start: the receiver's identity, adopted now or already.
        let (adopted, term, vote) = on_node(&mut sim, node, |env| {
            Box::pin(async move { open_follower(&env).await })
        })
        .unwrap_or_else(|why| panic!("seed {seed}: refused: {why}"));
        assert_eq!(
            (term, vote),
            (7, Some(ServerId(3))),
            "seed {seed}: the receiver's identity, never fresh and never the old store"
        );
        assert_eq!(
            adopted, staging_current,
            "seed {seed}: the start adopts exactly when the staging CURRENT survived"
        );
        // The marker is still there after the adoption's file deletions.
        let names = on_node(&mut sim, node, |env| {
            Box::pin(async move {
                FileSystem::read_dir(env.fs(), Path::new("/follower"))
                    .await
                    .unwrap()
            })
        });
        assert!(
            names.iter().any(|n| n.to_str() == Some(STORE_MARKER)),
            "seed {seed}: the marker is not a store file"
        );
    }
    // The crashes landed on both sides of the switch: some seeds kept the old
    // store's CURRENT (manifest 1) and re-ran the adoption, some crashed after
    // the switch with the adoption still to be retired, and some after all of it.
    assert!(
        outcomes.contains(&(true, true)),
        "no crash before the switch: {outcomes:?}"
    );
    assert!(
        outcomes.contains(&(false, true)) || outcomes.contains(&(false, false)),
        "no crash after the switch: {outcomes:?}"
    );
}

/// A staging directory whose `CURRENT` exists but does not parse is damage, not
/// debris (D-041): the adoption refuses it with `LostState`, sweeps
/// nothing, and the old store is untouched; the same for a `CURRENT` naming a
/// manifest that is not there. Only a staging directory with no `CURRENT` at
/// all is swept.
#[test]
fn a_damaged_staging_current_is_refused_and_not_swept() {
    let mut sim = Sim::new(SimConfig::new(80));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            build_follower(&env).await;
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let staged = stream(&env, &dir, &mut assembler).await;
            let repair = Repair {
                term: 7,
                vote: Some(ServerId(3)),
                tail: Vec::new(),
                quarantined: false,
                // The receiver's own, as an install into a live store carries
                // (D-042).
                incarnation: 1,
            };
            assembler.finish(&staged, &repair).await.unwrap();
            let staging = staging_dir(Path::new("/follower"));
            let fs = env.fs();
            // Bit rot on the staging CURRENT: it exists and does not parse.
            let current = fs
                .open(
                    &manifest::current_path(&staging),
                    OpenOptions::new().write(true),
                )
                .await
                .unwrap();
            current
                .write_at(0, Bytes::from_static(b"MANIFEST-0000x1 00000000\n"))
                .await
                .unwrap();
            current.sync().await.unwrap();
            let before = fs.read_dir(&staging).await.unwrap();
            let refused = adopt_staged(&env, Path::new("/follower"))
                .await
                .expect_err("a damaged staging CURRENT is refused");
            let lost = LostState::from_io(&refused).expect("carries LostState");
            assert_eq!(lost.damaged, Some(Damage::StagingCurrentUnreadable));
            assert_eq!(
                fs.read_dir(&staging).await.unwrap(),
                before,
                "nothing was swept"
            );
            // The old store is whole: it opens with its own identity.
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config("/follower"), prefix())
                .await
                .unwrap();
            assert_eq!((store.term(), store.vote()), (3, Some(ServerId(2))));
            drop(store);
            // A CURRENT that parses but names a manifest that is not there: the
            // repaired manifest, the newest in the staging directory, removed.
            let named = fs
                .read_dir(&staging)
                .await
                .unwrap()
                .iter()
                .filter_map(|n| manifest::manifest_of(n))
                .max()
                .expect("the repaired manifest");
            fs.remove_file(&manifest::manifest_path(&staging, named))
                .await
                .unwrap();
            let current = fs
                .open(
                    &manifest::current_path(&staging),
                    OpenOptions::new().write(true).truncate(true),
                )
                .await
                .unwrap();
            current
                .write_at(0, manifest::encode_current(named))
                .await
                .unwrap();
            current.sync().await.unwrap();
            let refused = adopt_staged(&env, Path::new("/follower"))
                .await
                .expect_err("a missing staged manifest is refused");
            assert_eq!(
                LostState::from_io(&refused).map(|l| l.damaged),
                Some(Some(Damage::StagingManifestUnreadable))
            );
            // No CURRENT at all: an install that never finished, swept.
            fs.remove_file(&manifest::current_path(&staging))
                .await
                .unwrap();
            assert!(!adopt_staged(&env, Path::new("/follower")).await.unwrap());
            assert!(
                fs.read_dir(&staging).await.unwrap().is_empty(),
                "the unfinished install is swept"
            );
        })
    });
}

/// The assembler's own sweep never removes a `CURRENT` (D-041): a
/// completed install abandoned before its adoption — a chunk of another stream
/// arriving first, or the re-seed stream that follows a refusal of a damaged
/// one — loses its files but keeps its commit point, so the next start refuses
/// rather than finding an unfinished install, sweeping it, and opening the old
/// store the acknowledged install superseded; the next completed install
/// replaces it and is adopted.
#[test]
fn an_abandoned_staging_keeps_its_current_so_no_start_opens_the_old_store() {
    let mut sim = Sim::new(SimConfig::new(82));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            build_follower(&env).await;
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let staged = stream(&env, &dir, &mut assembler).await;
            let repair = Repair {
                term: 7,
                vote: Some(ServerId(3)),
                tail: Vec::new(),
                quarantined: false,
                // The receiver's own, as an install into a live store carries
                // (D-042).
                incarnation: 1,
            };
            assembler.finish(&staged, &repair).await.unwrap();
            let staging = staging_dir(Path::new("/follower"));
            let fs = env.fs();
            // Abandoned complete: the files go, CURRENT stays, and the next
            // start refuses for the manifest it names rather than falling back.
            assembler.abandon().await;
            let names = fs.read_dir(&staging).await.unwrap();
            assert_eq!(
                names,
                vec![PathBuf::from("CURRENT")],
                "only CURRENT survives"
            );
            let refused = open_follower(&env)
                .await
                .expect_err("an abandoned install's CURRENT still refuses the old store");
            assert!(
                refused.contains("the staging directory's CURRENT names"),
                "{refused}"
            );
            // Abandoned damaged: the same.
            let current = fs
                .open(
                    &manifest::current_path(&staging),
                    OpenOptions::new().write(true).truncate(true),
                )
                .await
                .unwrap();
            current
                .write_at(0, Bytes::from_static(b"not a current\n"))
                .await
                .unwrap();
            current.sync().await.unwrap();
            assembler.abandon().await;
            assert_eq!(
                fs.read_dir(&staging).await.unwrap(),
                vec![PathBuf::from("CURRENT")],
                "a damaged CURRENT survives the sweep too"
            );
            let refused = open_follower(&env)
                .await
                .expect_err("a damaged CURRENT still refuses the old store");
            assert!(refused.contains("exists but cannot be read"), "{refused}");
            // The next completed install replaces it, and is adopted with the
            // receiver's identity.
            let staged = stream(&env, &dir, &mut assembler).await;
            assembler.finish(&staged, &repair).await.unwrap();
            let (adopted, term, vote) = open_follower(&env).await.unwrap();
            assert!(adopted, "the completed install is adopted");
            assert_eq!((term, vote), (7, Some(ServerId(3))));
        })
    });
}

/// The store marker (D-041): a fresh directory opens as a fresh store
/// and is marked at its first open; a marked directory that has lost its
/// `CURRENT` — even one the engine alone would open fresh again, nothing else
/// remaining — is refused as a lost store, and so is one whose `CURRENT` does
/// not parse.
#[test]
fn a_marked_directory_without_a_valid_current_is_a_lost_store_never_a_fresh_one() {
    let mut sim = Sim::new(SimConfig::new(81));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let fs = env.fs();
            let dir = Path::new("/fresh");
            // Fresh: no marker, so the check passes and the engine opens fresh.
            refuse_lost_store(&env, dir).await.unwrap();
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config("/fresh"), prefix())
                .await
                .unwrap();
            assert_eq!((store.term(), store.applied()), (0, 0));
            assert!(
                !fs.read_dir(dir)
                    .await
                    .unwrap()
                    .iter()
                    .any(|n| n.to_str() == Some(STORE_MARKER)),
                "not marked before the first open completes"
            );
            mark_store(&env, dir).await.unwrap();
            mark_store(&env, dir).await.unwrap();
            assert!(
                fs.read_dir(dir)
                    .await
                    .unwrap()
                    .iter()
                    .any(|n| n.to_str() == Some(STORE_MARKER)),
                "marked after the first open"
            );
            refuse_lost_store(&env, dir).await.unwrap();
            drop(store);
            // The directory loses everything but the marker: the engine's own
            // rule would open it fresh, since no manifest or table remains
            // (D-024); the marker says it was a store.
            // The record stays with the marker: what is left is a directory of
            // this build's format that lost its store, not a directory of
            // another format (PROPOSED D-060).
            for name in fs.read_dir(dir).await.unwrap() {
                if name.to_str() != Some(STORE_MARKER) && name.to_str() != Some(FORMAT_FILE) {
                    fs.remove_file(&dir.join(name)).await.unwrap();
                }
            }
            fs.sync_dir(dir).await.unwrap();
            let refused = refuse_lost_store(&env, dir)
                .await
                .expect_err("a marked directory without CURRENT is refused");
            assert_eq!(
                LostState::from_io(&refused).map(|l| l.damaged),
                Some(Some(Damage::MarkedCurrentMissing))
            );
            assert!(
                Engine::open(env.clone(), engine_config("/fresh"))
                    .await
                    .is_ok(),
                "the engine alone would have opened the emptied directory fresh"
            );
            // A CURRENT that does not parse, with the marker.
            let current = fs
                .open(
                    &manifest::current_path(dir),
                    OpenOptions::new().write(true).create(true).truncate(true),
                )
                .await
                .unwrap();
            current
                .write_at(0, Bytes::from_static(b"not a current\n"))
                .await
                .unwrap();
            current.sync().await.unwrap();
            let refused = refuse_lost_store(&env, dir)
                .await
                .expect_err("a marked directory with an unreadable CURRENT is refused");
            assert_eq!(
                LostState::from_io(&refused).map(|l| l.damaged),
                Some(Some(Damage::MarkedCurrentUnreadable))
            );
        })
    });
}

/// The staging directory never holds a valid `CURRENT` while a correct stream
/// runs, and an identity change starts it over.
#[test]
fn an_identity_change_restarts_the_staging() {
    let mut sim = Sim::new(SimConfig::new(79));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            record_fresh_format(&env, "/follower").await;
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let sender = Sender::open(&env, &dir, ServerId(2), 5, 1, 4)
                .await
                .unwrap();
            let chunk = sender.chunk(&env, 64).await.unwrap();
            let Message::InstallSnapshot {
                file,
                offset,
                total,
                data,
                ..
            } = chunk
            else {
                panic!("a chunk")
            };
            let fed = assembler
                .on_chunk(
                    ServerId(1),
                    4,
                    5,
                    1,
                    file.clone(),
                    offset,
                    total,
                    false,
                    data.clone(),
                )
                .await
                .unwrap();
            assert!(matches!(fed, Feed::Ack { .. }));
            // A mid-stream chunk of a different identity (a new leader's term):
            // not a start, so the receiver asks for a restart.
            let fed = assembler
                .on_chunk(
                    ServerId(1),
                    9,
                    5,
                    1,
                    file.clone(),
                    64,
                    total,
                    false,
                    data.clone(),
                )
                .await
                .unwrap();
            assert!(matches!(fed, Feed::Restart), "resuming across identities");
            // The same new identity starting at offset 0 begins a fresh stream.
            let fed = assembler
                .on_chunk(ServerId(1), 9, 5, 1, file, 0, total, false, data)
                .await
                .unwrap();
            assert!(matches!(fed, Feed::Ack { .. }));
            assert!(
                !adopt_staged(&env, Path::new("/follower")).await.unwrap(),
                "no install completed"
            );
            // Sweeping the debris left nothing behind at the staging path.
            let staging = staging_dir(Path::new("/follower"));
            let names = ananke_env::FileSystem::read_dir(env.fs(), &staging)
                .await
                .unwrap_or_default();
            assert!(
                !names.iter().any(|n| n.to_str() == Some("CURRENT")),
                "no CURRENT while nothing is installed"
            );
        })
    });
}

// --- D-043: versioned takes, pinned streams, the sweep, and a stream per follower ---

use std::collections::BTreeMap;
use std::net::SocketAddr;

use ananke_env::{Clock, Network, NodeId, Socket};
use ananke_raft::apply::Outcome;
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::snapshot::{
    checkpoint_complete, find_version, parse_version, sweep_versions, take_version, version_dir,
};
use ananke_raft::{NodeConfig, invariants, run};

/// Two takes at one index are two directories, numbered by the record's counter,
/// each a complete checkpoint; the record names the newest, and so does the
/// version lookup a stream opens.
#[test]
fn two_takes_at_one_index_are_two_directories() {
    let mut sim = Sim::new(SimConfig::new(81));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            // The first take went to an explicit directory, the way the server
            // as built takes: the counter advanced all the same.
            let (store, shared) = build_leader(&env).await;
            let leader = Path::new("/leader");
            let config = members(&[1, 2, 3]);
            let second = take_version(&env, &store, leader, 5, 1, &config)
                .await
                .unwrap();
            let third = take_version(&env, &store, leader, 5, 1, &config)
                .await
                .unwrap();
            assert_eq!(second, version_dir(leader, 5, 2));
            assert_eq!(third, version_dir(leader, 5, 3));
            assert_ne!(second, third, "two takes, two directories");
            for dir in [&shared, &second, &third] {
                assert!(
                    checkpoint_complete(&env, dir).await.unwrap(),
                    "{} is a complete checkpoint",
                    dir.display()
                );
            }
            let record = store.snapshot_record().await.unwrap().expect("a record");
            assert_eq!(
                (record.last_index, record.take, record.dir.as_str()),
                (5, 3, "/leader/snap-5-3"),
                "the record names the newest version and its count"
            );
            assert_eq!(
                find_version(&env, leader, 5).await.unwrap(),
                Some((third, 3)),
                "a stream opens the newest complete version"
            );
            assert_eq!(find_version(&env, leader, 7).await.unwrap(), None);
            assert_eq!(parse_version("snap-5-3"), Some((5, 3)));
            assert_eq!(parse_version("snap-5"), Some((5, 0)));
            assert_eq!(parse_version("000001.sst"), None);
            assert_eq!(parse_version("snap-x-1"), None);
        })
    });
}

/// Feeds `sender`'s chunks into `assembler` until the stream completes or breaks:
/// the stage on completion, `None` when a chunk could not be read, the receiver
/// refused what it got, or the stream asked to start over.
async fn stream_to_end(
    env: &SimEnv,
    sender: &mut Sender,
    assembler: &mut Assembler<SimEnv>,
) -> Option<ananke_raft::snapshot::Staged> {
    for _ in 0..10_000 {
        let Ok(Message::InstallSnapshot {
            term,
            last_index,
            last_term,
            file,
            offset,
            total,
            done,
            data,
        }) = sender.chunk(env, 64).await
        else {
            return None;
        };
        let fed = assembler
            .on_chunk(
                ServerId(1),
                term,
                last_index,
                last_term,
                file,
                offset,
                total,
                done,
                data,
            )
            .await;
        match fed {
            Ok(Feed::Ack { file, offset }) => {
                sender.on_more(&file, offset);
            }
            Ok(Feed::Staged(staged)) => return Some(staged),
            Ok(Feed::Restart) | Err(_) => return None,
        }
    }
    panic!("the stream never ended");
}

/// A stream pinned to a version reads it untouched while newer takes land, at
/// the same index and a later one, and completes; the shared directory the
/// server as built streams from is rewritten under its stream by a take at the
/// same index, and that stream does not complete.
#[test]
fn a_stream_survives_a_newer_take_and_the_shared_directory_does_not() {
    let mut sim = Sim::new(SimConfig::new(82));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, shared) = build_leader(&env).await;
            let leader = Path::new("/leader");
            let config = members(&[1, 2, 3]);
            let pinned = take_version(&env, &store, leader, 5, 1, &config)
                .await
                .unwrap();
            let mut sender = Sender::open(&env, &pinned, ServerId(2), 5, 1, 4)
                .await
                .unwrap();
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            // The stream is under way when two newer takes land: a fresh version
            // at the same index, then one at the next index, once it applies.
            let first = sender.chunk(&env, 64).await.unwrap();
            let Message::InstallSnapshot {
                file,
                offset,
                total,
                data,
                ..
            } = first
            else {
                panic!("a chunk")
            };
            let fed = assembler
                .on_chunk(ServerId(1), 4, 5, 1, file, offset, total, false, data)
                .await
                .unwrap();
            let Feed::Ack { file, offset } = fed else {
                panic!("an ack")
            };
            sender.on_more(&file, offset);
            let newer = take_version(&env, &store, leader, 5, 1, &config)
                .await
                .unwrap();
            apply_command(&store, 6, None).await.unwrap();
            let later = take_version(&env, &store, leader, 6, 4, &members(&[1, 2, 3, 4]))
                .await
                .unwrap();
            assert_ne!(newer, pinned);
            assert_ne!(later, pinned);
            let staged = stream_to_end(&env, &mut sender, &mut assembler)
                .await
                .expect("the pinned version streams to completion");
            assert_eq!((staged.last_index, staged.last_term), (5, 1));
            assembler.abandon().await;

            // As built: the stream reads the one directory of its index, and a
            // take at that index sweeps and rewrites it under the stream.
            let mut sender = Sender::open(&env, &shared, ServerId(3), 5, 1, 4)
                .await
                .unwrap();
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let first = sender.chunk(&env, 64).await.unwrap();
            let Message::InstallSnapshot {
                file,
                offset,
                total,
                data,
                ..
            } = first
            else {
                panic!("a chunk")
            };
            let fed = assembler
                .on_chunk(ServerId(1), 4, 5, 1, file, offset, total, false, data)
                .await
                .unwrap();
            let Feed::Ack { file, offset } = fed else {
                panic!("an ack")
            };
            sender.on_more(&file, offset);
            take(&env, &store, &shared, 5, 1, &config).await.unwrap();
            assert!(
                stream_to_end(&env, &mut sender, &mut assembler)
                    .await
                    .is_none(),
                "a stream of a directory rewritten under it does not complete"
            );
        })
    });
}

/// The sweep deletes the versions that are neither the record's nor pinned by
/// a stream, and nothing else; a version is deleted once its last reader lets
/// go; the record's is never deleted.
#[test]
fn the_sweep_deletes_only_unpinned_versions() {
    let mut sim = Sim::new(SimConfig::new(83));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, shared) = build_leader(&env).await;
            let leader = Path::new("/leader");
            let config = members(&[1, 2, 3]);
            let second = take_version(&env, &store, leader, 5, 1, &config)
                .await
                .unwrap();
            let third = take_version(&env, &store, leader, 5, 1, &config)
                .await
                .unwrap();
            // A stream reads the second version; the first is nobody's.
            let mut pinned: BTreeMap<PathBuf, usize> = BTreeMap::new();
            pinned.insert(second.clone(), 1);
            let deleted = sweep_versions(&env, &store, leader, &pinned).await.unwrap();
            assert_eq!(deleted, vec![(5, 0)], "the shared directory, by its name");
            assert!(!checkpoint_complete(&env, &shared).await.unwrap());
            assert!(checkpoint_complete(&env, &second).await.unwrap());
            assert!(checkpoint_complete(&env, &third).await.unwrap());
            // The stream ends: the second version goes; the record's never does.
            pinned.clear();
            let deleted = sweep_versions(&env, &store, leader, &pinned).await.unwrap();
            assert_eq!(deleted, vec![(5, 2)]);
            assert!(!checkpoint_complete(&env, &second).await.unwrap());
            assert!(checkpoint_complete(&env, &third).await.unwrap());
            assert_eq!(
                find_version(&env, leader, 5).await.unwrap(),
                Some((third.clone(), 3))
            );
            // Nothing left to sweep, and the store's own files were never touched.
            assert!(
                sweep_versions(&env, &store, leader, &pinned)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let (reopened, _) =
                RaftStore::open_dir(env.clone(), engine_config("/leader"), prefix())
                    .await
                    .unwrap();
            assert_eq!(reopened.applied(), 5);
        })
    });
}

fn addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, u8::try_from(n).expect("small")], 7000))
}

/// A server of a five-voter cluster with a small snapshot threshold, so a
/// follower cut off for a moment falls behind it.
fn cluster_config(id: u64, variants: impl Into<Variants>) -> NodeConfig {
    let mut engine = EngineConfig::new(PathBuf::from("/raft"));
    engine.memtable_bytes = 4096;
    engine.segment_bytes = 4096;
    engine.background_compaction = true;
    NodeConfig {
        id: ServerId(id),
        listen: addr(id),
        servers: (1..=5).map(|s| (ServerId(s), addr(s))).collect(),
        initial_voters: (1..=5).map(ServerId).collect(),
        raft: RaftConfig {
            snapshot_threshold: 4,
            snapshot_chunk: 512,
            variants: variants.into(),
            ..RaftConfig::default()
        },
        engine,
        inbox_capacity: 64,
    }
}

/// Puts `count` keys through whichever server leads, following NotLeader hints.
async fn put_many(env: SimEnv, count: u64) {
    let sock = env.net().bind(addr(9)).await.unwrap();
    let mut target = ServerId(1);
    for seq in 0..count {
        loop {
            let request = Request {
                client: 1,
                seq,
                command: Command::Put {
                    key: Bytes::from(format!("k{seq}")),
                    value: Bytes::from(format!("v{seq}")),
                },
            };
            sock.send(addr(target.0), request.encode()).await.unwrap();
            let deadline = env.clock().now() + Duration::from_millis(100);
            let recv = std::pin::pin!(sock.recv());
            let timer = std::pin::pin!(env.clock().sleep_until(deadline));
            let got = match ananke_env::race(&env, recv, timer).await {
                ananke_env::Either::Left(Ok((_, bytes))) => Response::decode(bytes).ok(),
                _ => None,
            };
            match got {
                Some(response) if response.seq == seq => match response.reply {
                    Reply::Outcome(Outcome::Done) => break,
                    Reply::NotLeader { leader: Some(l) } => target = l,
                    _ => {
                        env.clock().sleep(Duration::from_millis(20)).await;
                        target = ServerId(target.0 % 3 + 1);
                    }
                },
                _ => target = ServerId(target.0 % 3 + 1),
            }
        }
    }
}

/// Runs five servers with 4 and 5 cut off until the leader has compacted past
/// them, then heals and returns the trace: both are designated snapshot-fed and
/// both must be streamed to.
fn two_designated_followers(seed: u64, variant: Variant) -> Vec<TraceEvent> {
    let mut sim = Sim::new(SimConfig::new(seed));
    let servers: Vec<NodeId> = (0..5).map(|_| sim.add_node()).collect();
    sim.partition(&servers[..3], &servers[3..]);
    for (i, &node) in servers.iter().enumerate() {
        let env = sim.env(node);
        let inner = env.clone();
        let config = cluster_config(i as u64 + 1, variant);
        env.spawn("raft", async move {
            let _ = run(inner, config).await;
        });
    }
    let client = sim.add_node();
    sim.run_for(Duration::from_millis(500));
    on_node(&mut sim, client, |env| Box::pin(put_many(env, 40)));
    // The leader takes once its log outgrows the threshold, designates the two
    // quiet followers, and compacts; then they come back.
    for _ in 0..40 {
        sim.run_for(Duration::from_millis(50));
        if sim
            .trace()
            .iter()
            .any(|r| matches!(r.event, TraceEvent::RaftCompacted { .. }))
        {
            break;
        }
    }
    sim.heal();
    sim.run_for(Duration::from_millis(3000));
    sim.trace().into_iter().map(|r| r.event).collect()
}

/// Two designated followers are streamed to at once and both install
/// (D-043); the server as built streams to one at a time, so its
/// second follower waits behind the first, and at this scale still completes —
/// the sweep's seed 5909 is where the first stream never ends.
#[test]
fn two_designated_followers_are_streamed_at_once_and_both_install() {
    let events = two_designated_followers(84, Variant::Correct);
    invariants::all(&events).unwrap();
    for server in [4, 5] {
        assert!(
            events.iter().any(|e| matches!(e,
                TraceEvent::RaftSnapshot { server: s, taken: false, .. } if *s == server)),
            "server {server} installed nothing: {events:?}"
        );
    }
    let most_at_once = events
        .iter()
        .filter_map(|e| match e {
            TraceEvent::RaftSnapshotStreams { streams, .. } => Some(*streams),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    assert_eq!(most_at_once, 2, "both followers streamed to at once");

    let events = two_designated_followers(84, Variant::SharedSnapshotDir);
    let most_at_once = events
        .iter()
        .filter_map(|e| match e {
            TraceEvent::RaftSnapshotStreams { streams, .. } => Some(*streams),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    assert_eq!(most_at_once, 1, "as built, one stream at a time");
}

/// A refusal is durable (D-044): the store directory's marker records
/// that this store lost state, with the reason, and every open after it refuses
/// on the mark alone — across the restart, and however whole the store on disk
/// looks by then. The engine is shown opening the same directory happily, which
/// is what the refused server's own engine had made of it by flushing the
/// memtable its recovery replayed (the thousand-seed premerge, seed 687): the
/// mark is the only thing that remembers.
#[test]
fn a_refusal_is_recorded_in_the_store_and_refuses_every_later_open() {
    let mut sim = Sim::new(SimConfig::new(91));
    let node = sim.add_node();
    let reason = "the engine's recovery lost state: dropped tables [1]";
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            build_follower(&env).await;
            // Whole: the marker says so and the open goes ahead.
            open_follower(&env).await.expect("a whole store opens");
            mark_store_lost(&env, Path::new("/follower"), reason)
                .await
                .unwrap();
        })
    });
    // The process that refused is gone; the mark is not.
    sim.crash(node);
    sim.restart(node);
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let dir = Path::new("/follower");
            for attempt in 1..=3 {
                let refused = refuse_lost_store(&env, dir)
                    .await
                    .expect_err("a marked-lost store is refused at every open");
                let lost = LostState::from_io(&refused).expect("a LostState");
                assert_eq!(lost.damaged, Some(Damage::MarkedLost), "attempt {attempt}");
                assert_eq!(
                    lost.lost_mark.as_deref(),
                    Some(reason),
                    "attempt {attempt}: the reason the refusal recorded"
                );
                assert!(
                    refused.to_string().contains(reason),
                    "attempt {attempt}: {refused}"
                );
            }
            // Nothing else refuses it: the store on disk is whole, CURRENT and
            // all, and the engine alone would open it and let the server vote.
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config("/follower"), prefix())
                .await
                .unwrap();
            assert_eq!((store.term(), store.vote()), (3, Some(ServerId(2))));
            // And the server's own open path refuses all the same.
            let refused = open_follower(&env)
                .await
                .expect_err("the server refuses the marked store");
            assert!(refused.contains(STORE_MARKER), "{refused}");
        })
    });
}

/// An install clears the lost mark (D-044): the adoption writes the
/// marker fresh the moment the installed store is the one in force, so the store
/// that lost state is refused until an install replaces it and not one open
/// after.
#[test]
fn an_install_clears_the_lost_mark() {
    let mut sim = Sim::new(SimConfig::new(92));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            build_follower(&env).await;
            mark_store_lost(
                &env,
                Path::new("/follower"),
                "the engine's recovery lost state: dropped tables [1]",
            )
            .await
            .unwrap();
            open_follower(&env)
                .await
                .expect_err("the marked store is refused");
            // The leader's stream, completed: the next open adopts it.
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::Correct,
                prefix(),
            );
            let staged = stream(&env, &dir, &mut assembler).await;
            let repair = Repair {
                term: 7,
                vote: Some(ServerId(3)),
                tail: Vec::new(),
                quarantined: true,
                incarnation: 9,
            };
            assembler.finish(&staged, &repair).await.unwrap();
            let (adopted, term, vote) = open_follower(&env)
                .await
                .expect("the adopted store opens, the mark cleared");
            assert!(adopted, "the install was adopted");
            assert_eq!((term, vote), (7, Some(ServerId(3))));
            // The mark is gone for good, not just for the open that adopted.
            refuse_lost_store(&env, Path::new("/follower"))
                .await
                .expect("the adopted store is whole");
            open_follower(&env).await.expect("and stays whole");
        })
    });
}

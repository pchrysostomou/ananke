//! Stage E against the core and the store, no sweep (RAFT.md §1, §3): the
//! compacted log answers at its boundary, a leader feeds the snapshot when a
//! follower falls below the prefix and compacts only when every follower is past
//! the checkpoint or designated, a quarantined server grants nothing, and a whole
//! stream assembles, verifies, installs with the receiver's identity repaired
//! before `CURRENT`, and is adopted at the next open. The
//! `SnapshotWithoutCurrentLast` variant is shown leaving a complete-looking store
//! that carries the leader's identity when the install never finished — the state
//! the sweep's crash-mid-install seeds must catch.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Environment, TraceEvent};
use ananke_raft::apply::{Command, apply_command, user_key};
use ananke_raft::core::{Input, Output, Persist, Raft, RaftConfig, Role, SnapshotAction, Variant};
use ananke_raft::message::Message;
use ananke_raft::snapshot::{Assembler, Feed, Repair, Sender, adopt_staged, staging_dir, take};
use ananke_raft::store::RaftStore;
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId, Term};
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

/// Builds a leader-like store at `/leader` with five applied puts and a
/// configuration entry at index 6, past the checkpoint, whose `0 / 2 / config`
/// key rides in the checkpoint the way a real leader's does (D-029) — so an
/// install that failed to rewrite the key would open a store whose key names an
/// entry its log does not hold, and be refused. Takes a checkpoint at index 5
/// and returns the checkpoint directory.
async fn build_leader(env: &SimEnv) -> (Arc<RaftStore<SimEnv>>, PathBuf) {
    let (engine, recovery) = Engine::open(env.clone(), engine_config("/leader"))
        .await
        .unwrap();
    let (store, _) = RaftStore::open(Arc::new(engine), &recovery).await.unwrap();
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
            // First stream: assembled and verified, but the server "crashes"
            // before the repair — nothing to adopt, the debris is swept.
            let mut assembler =
                Assembler::new(env.clone(), Path::new("/follower"), Variant::Correct);
            let _staged = stream(&env, &dir, &mut assembler).await;
            assert!(
                !adopt_staged(&env, Path::new("/follower")).await.unwrap(),
                "an unrepaired staging directory must not win"
            );
            // Second stream: repaired with the receiver's identity, CURRENT last.
            let mut assembler =
                Assembler::new(env.clone(), Path::new("/follower"), Variant::Correct);
            let staged = stream(&env, &dir, &mut assembler).await;
            let repair = Repair {
                term: 7,
                vote: Some(ServerId(3)),
                tail: vec![entry(1, 6, "tail6"), entry(1, 7, "tail7")],
                quarantined: true,
            };
            assembler.finish(&staged, &repair).await.unwrap();
            assert!(adopt_staged(&env, Path::new("/follower")).await.unwrap());
            // Idempotent: a second look finds nothing left to adopt.
            assert!(!adopt_staged(&env, Path::new("/follower")).await.unwrap());
            // The adopted store: the leader's data at the snapshot, under the
            // receiver's own hard state, applied index, record and quarantine.
            let (engine, recovery) = Engine::open(env.clone(), engine_config("/follower"))
                .await
                .unwrap();
            let engine = Arc::new(engine);
            let (store, recovered) = RaftStore::open(engine.clone(), &recovery).await.unwrap();
            assert_eq!(store.term(), 7);
            assert_eq!(store.vote(), Some(ServerId(3)));
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
                    .get(&ananke_raft::store::key(0, 2, b"config"))
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
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/follower"),
                Variant::SnapshotWithoutCurrentLast,
            );
            let _staged = stream(&env, &dir, &mut assembler).await;
            // The server crashes here: the repair never runs. The staging
            // directory should not be a store yet — under the variant it is.
            assert!(
                adopt_staged(&env, Path::new("/follower")).await.unwrap(),
                "the variant's CURRENT made an unrepaired install win"
            );
            let (engine, recovery) = Engine::open(env.clone(), engine_config("/follower"))
                .await
                .unwrap();
            let (store, recovered) = RaftStore::open(Arc::new(engine), &recovery).await.unwrap();
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

/// The staging directory never holds a valid `CURRENT` while a correct stream
/// runs, and an identity change starts it over.
#[test]
fn an_identity_change_restarts_the_staging() {
    let mut sim = Sim::new(SimConfig::new(79));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            let mut assembler =
                Assembler::new(env.clone(), Path::new("/follower"), Variant::Correct);
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

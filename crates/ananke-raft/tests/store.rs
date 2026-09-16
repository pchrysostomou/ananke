//! The persistent state in the engine, on the simulated disk: what a persist writes
//! comes back at the next open, truncation removes what it should, and an entry's
//! writes and the applied index are durable together or not at all, across crashes
//! with every fault on. A disk that lied about a sync shows as a refusal, never as
//! a state with a hole.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Clock, Environment, FileSystem, NodeId};
use ananke_raft::apply::{Command, Outcome, apply_command, user_key};
use ananke_raft::core::Persist;
use ananke_raft::node::SINGLE_GROUP;
use ananke_raft::store::{KeyPrefix, LostState, RaftStore};
use ananke_raft::types::{Entry, Payload, ServerId};
use ananke_storage::{Engine, EngineConfig};
use bytes::Bytes;

fn config() -> EngineConfig {
    let mut config = EngineConfig::new(PathBuf::from("/raft"));
    config.memtable_bytes = 4096;
    config.segment_bytes = 4096;
    config.allow_head_gap = false;
    config.allow_manifest_fallback = false;
    config.background_compaction = true;
    config
}

/// Today's one group, whose Raft state this store keeps (PROPOSED D-060).
fn prefix() -> KeyPrefix {
    KeyPrefix::group(SINGLE_GROUP)
}

fn entry(term: u64, index: u64, command: &str) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Command(Bytes::from(command.to_owned())),
    }
}

type Out<T> = Arc<Mutex<Option<T>>>;

/// Runs `f` on `node` until it finishes and returns what it produced.
fn on_node<T: Send + 'static>(
    sim: &mut Sim,
    node: NodeId,
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

#[test]
fn a_persist_comes_back_at_the_next_open_and_a_truncation_removes_the_tail() {
    let mut sim = Sim::new(SimConfig::new(21));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, recovered) = RaftStore::open_dir(env, config(), prefix()).await.unwrap();
            assert!(recovered.log.is_empty());
            assert_eq!((store.term(), store.vote(), store.applied()), (0, None, 0));
            store
                .persist(&Persist {
                    term: 3,
                    vote: Some(ServerId(2)),
                    truncate_from: None,
                    append: vec![entry(1, 1, "a"), entry(3, 2, "b"), entry(3, 3, "c")],
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            store
                .persist(&Persist {
                    term: 4,
                    vote: None,
                    truncate_from: Some(3),
                    append: vec![entry(4, 3, "c'"), entry(4, 4, "d")],
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            assert_eq!(store.last_index(), 4);
        })
    });
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, recovered) = RaftStore::open_dir(env, config(), prefix()).await.unwrap();
            assert_eq!(
                store.engine().ssts(),
                0,
                "the writes came back from the log, not from a table"
            );
            assert_eq!((store.term(), store.vote(), store.applied()), (4, None, 0));
            assert_eq!(
                recovered.log,
                vec![
                    entry(1, 1, "a"),
                    entry(3, 2, "b"),
                    entry(4, 3, "c'"),
                    entry(4, 4, "d")
                ]
            );
            assert!(recovered.snapshot.is_none());
            assert!(!recovered.quarantined);
            assert_eq!(store.last_index(), 4);
        })
    });
}

/// Applying is one batch with the applied index: after a crash the engine holds the
/// command's effect exactly when its index counts as applied, on every seed, with
/// lost syncs, bit rot and torn writes on. A put is followed by a compare-and-set on
/// the same key, so an entry applied twice would show as a swap that should have
/// failed. A seed whose disk lost a flushed table or the log's head is refused at
/// open, by the engine or by the store; it never comes back as a state with a hole.
///
/// The store's directory and its format record are made before the crash phase and
/// forced durable, so what the crashes sweep is the applies: a fresh store's first
/// write, whose own crash test is `tests/format.rs`, is not this test's subject
/// (PROPOSED D-060).
#[test]
fn an_entrys_writes_and_the_applied_index_are_durable_together() {
    use ananke_raft::format::{STORE_FORMAT, encode_record, format_path};

    let mut verified = 0;
    let mut refused = Vec::new();
    for seed in 0..40u64 {
        let mut sim = Sim::new({
            let mut c = SimConfig::new(seed);
            c.fs.p_durable = 0.7;
            c.fs.p_bitrot = 0.02;
            c
        });
        let node = sim.add_node();
        // The store exists before the crash phase, and its record is durable: a
        // disk that lies about a sync must not leave the record itself owed.
        on_node(&mut sim, node, |env| {
            Box::pin(async move {
                RaftStore::open_dir(env, config(), prefix()).await.unwrap();
            })
        });
        let mut durable = false;
        for _ in 0..50 {
            if sim.durable_contents(node, &format_path(Path::new("/raft")))
                == Some(encode_record(STORE_FORMAT).to_vec())
            {
                durable = true;
                break;
            }
            on_node(&mut sim, node, |env| {
                Box::pin(async move {
                    let file = env
                        .fs()
                        .open(
                            &format_path(Path::new("/raft")),
                            ananke_env::OpenOptions::new().write(true),
                        )
                        .await
                        .unwrap();
                    ananke_env::File::write_at(&file, 0, encode_record(STORE_FORMAT))
                        .await
                        .unwrap();
                    ananke_env::File::sync(&file).await.unwrap();
                })
            });
        }
        assert!(
            durable,
            "seed {seed}: the format record never became durable"
        );
        // Apply a run of entries, crashing at a random point inside.
        let crash_after = Duration::from_micros(200 + (seed * 7919) % 3000);
        let env = sim.env(node);
        let applied_by_task: Out<Vec<(u64, Outcome)>> = Arc::default();
        let a = applied_by_task.clone();
        env.clone().spawn("applier", async move {
            let (store, _) = RaftStore::open_dir(env.clone(), config(), prefix())
                .await
                .unwrap();
            let mut done = Vec::new();
            // Entries 3k+1 and 3k+2 put key k; entry 3k+3 swaps it.
            for index in 1..=30u64 {
                let key = Bytes::from(format!("k{}", (index - 1) / 3));
                let command = if index % 3 == 0 {
                    Command::Cas {
                        key,
                        expect: Some(Bytes::from(format!("v{}", index - 1))),
                        value: Bytes::from(format!("swapped{index}")),
                    }
                } else {
                    Command::Put {
                        key,
                        value: Bytes::from(format!("v{index}")),
                    }
                };
                let outcome = apply_command(&store, index, Some(&command)).await.unwrap();
                done.push((index, outcome.clone()));
                *a.lock().unwrap() = Some(done.clone());
                env.clock().sleep(Duration::from_micros(50)).await;
            }
        });
        sim.run_for(crash_after);
        sim.crash(node);
        sim.restart(node);
        let acknowledged = applied_by_task.lock().unwrap().clone().unwrap_or_default();
        // After the crash: the applied index on disk and the keys agree exactly, or
        // the open is refused because the disk lost state.
        let opened = on_node(&mut sim, node, |env| {
            Box::pin(async move {
                let (store, _) = match RaftStore::open_dir(env, config(), prefix()).await {
                    Ok(opened) => opened,
                    Err(e) => {
                        assert!(
                            LostState::from_io(&e).is_some()
                                || ananke_storage::OpenRefused::from_io(&e).is_some(),
                            "seed {seed}: {e}"
                        );
                        return Err(e.to_string());
                    }
                };
                let engine = store.engine().clone();
                let mut state = Vec::new();
                for k in 0..=10u64 {
                    state.push(
                        engine
                            .get(&user_key(format!("k{k}").as_bytes()))
                            .await
                            .unwrap(),
                    );
                }
                Ok((store.applied(), state))
            })
        });
        let (applied, state) = match opened {
            Ok(opened) => opened,
            Err(why) => {
                refused.push((seed, why));
                continue;
            }
        };
        verified += 1;
        // The model: the state after applying entries 1..=applied, exactly once each.
        // Every swap succeeds, since each follows its put.
        let mut model: Vec<Option<Bytes>> = vec![None; 11];
        for index in 1..=applied {
            let k = ((index - 1) / 3) as usize;
            if index % 3 == 0 {
                assert_eq!(
                    model[k],
                    Some(Bytes::from(format!("v{}", index - 1))),
                    "the model's swap at {index}"
                );
                model[k] = Some(Bytes::from(format!("swapped{index}")));
            } else {
                model[k] = Some(Bytes::from(format!("v{index}")));
            }
        }
        assert_eq!(
            state, model,
            "seed {seed}: the state at applied index {applied}"
        );
        // Every acknowledged apply at or below the applied index had the outcome the
        // model gives.
        for (index, outcome) in &acknowledged {
            if *index <= applied {
                let expected = if index % 3 == 0 {
                    Outcome::Swapped(true)
                } else {
                    Outcome::Done
                };
                assert_eq!(*outcome, expected, "seed {seed} index {index}");
            }
        }
        assert!(
            applied <= acknowledged.len() as u64 + 1,
            "seed {seed}: at most the apply in flight can be durable beyond what was acknowledged"
        );
    }
    // Printed so D-061's table of fixed seed sets can be checked at any tier rather
    // than re-measured with a probe: the margin over the floor is what that row is
    // for.
    println!(
        "an entry's writes and the applied index: {verified} of 40 seeds came back as a \
         state, {} refused",
        refused.len()
    );
    assert!(
        verified >= 20,
        "only {verified} of 40 seeds came back as a state; refused: {refused:?}"
    );
}

/// The `<prefix> / 2 / config` key (RAFT.md §3): a persist that carries the
/// configuration in force writes it in the same synced batch, the next open
/// reads it back consistent with the log, and a store whose key disagrees with
/// its log — something else wrote it — is refused.
#[test]
fn the_config_key_comes_back_consistent_and_a_mismatch_is_refused() {
    use ananke_raft::store::PURPOSE_CONFIG;
    use ananke_raft::types::Configuration;
    use ananke_storage::WriteBatch;

    let mut sim = Sim::new(SimConfig::new(23));
    let node = sim.add_node();
    let joint = Configuration {
        voters: vec![ServerId(1), ServerId(2), ServerId(3)],
        new_voters: Some(vec![ServerId(1), ServerId(2), ServerId(3), ServerId(4)]),
        learners: Vec::new(),
    };
    let config_entry = Entry {
        term: 1,
        index: 2,
        payload: Payload::Config(joint.clone()),
    };
    on_node(&mut sim, node, |env| {
        let joint = joint.clone();
        let config_entry = config_entry.clone();
        Box::pin(async move {
            let (store, _) = RaftStore::open_dir(env, config(), prefix()).await.unwrap();
            store
                .persist(&Persist {
                    term: 1,
                    vote: None,
                    truncate_from: None,
                    append: vec![entry(1, 1, "a"), config_entry],
                    config: Some((2, joint)),
                    compact_to: None,
                })
                .await
                .unwrap();
        })
    });
    // The next open finds the key and the log in step.
    on_node(&mut sim, node, |env| {
        let config_entry = config_entry.clone();
        Box::pin(async move {
            let (_, recovered) = RaftStore::open_dir(env, config(), prefix()).await.unwrap();
            assert_eq!(recovered.log, vec![entry(1, 1, "a"), config_entry]);
        })
    });
    // Something else rewrites the key: the store is refused.
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (engine, _) = Engine::open(env, config()).await.unwrap();
            let mut batch = WriteBatch::new();
            batch.put(
                prefix().key(PURPOSE_CONFIG, b"config"),
                Bytes::from_static(b"\x09\x00\x00\x00\x00\x00\x00\x00\x00"),
            );
            engine.write(batch, true).await.unwrap();
        })
    });
    let refused = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            RaftStore::open_dir(env, config(), prefix())
                .await
                .err()
                .map(|e| e.to_string())
        })
    });
    let refused = refused.expect("the mismatched key refuses the store");
    assert!(refused.contains("config"), "{refused}");
}

/// Two groups' stores share one engine and nothing else (Q40, PROPOSED D-060):
/// each key of a group lies inside its own prefix's interval, so a persist, an
/// apply and a snapshot record on one group are invisible to the other's open,
/// and the stale-log cleanup one group's snapshot triggers deletes none of the
/// other's keys.
#[test]
fn two_group_prefixes_share_one_engine_and_nothing_else() {
    use ananke_raft::format::{FormatChecked, Verdict, check_format, record_format};
    use ananke_raft::store::SnapshotRecord;
    use ananke_raft::types::Configuration;
    use ananke_storage::WriteBatch;

    /// The directory's format, gated as the server gates it.
    async fn gate(env: &SimEnv, dir: &std::path::Path) -> FormatChecked {
        match check_format(env, dir).await.unwrap() {
            Verdict::Fresh(fresh) => record_format(env, fresh).await.unwrap(),
            Verdict::Recorded { checked, .. } => checked,
            Verdict::Damaged => panic!("the record cannot be read"),
        }
    }

    let mut sim = Sim::new(SimConfig::new(24));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let dir = config().dir.clone();
            let checked = gate(&env, &dir).await;
            let (engine, recovery) = Engine::open(env.clone(), config()).await.unwrap();
            let engine = Arc::new(engine);
            let (two, _) = RaftStore::open(
                engine.clone(),
                &recovery,
                KeyPrefix::group(2),
                checked.clone(),
            )
            .await
            .unwrap();
            let (three, _) =
                RaftStore::open(engine.clone(), &recovery, KeyPrefix::group(3), checked)
                    .await
                    .unwrap();
            two.persist(&Persist {
                term: 5,
                vote: Some(ServerId(1)),
                truncate_from: None,
                append: vec![entry(5, 1, "two"), entry(5, 2, "two")],
                config: None,
                compact_to: None,
            })
            .await
            .unwrap();
            two.apply(1, WriteBatch::new()).await.unwrap();
            two.record_snapshot(&SnapshotRecord {
                last_index: 1,
                last_term: 5,
                config: Configuration::of(&[ServerId(1)]),
                dir: String::new(),
                taken: false,
                take: 0,
            })
            .await
            .unwrap();
            // Group 3's own state, with a snapshot past its whole log, so its
            // open deletes the log keys the snapshot covers — its own only.
            three
                .persist(&Persist {
                    term: 9,
                    vote: None,
                    truncate_from: None,
                    append: vec![entry(9, 1, "three"), entry(9, 2, "three")],
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            three
                .record_snapshot(&SnapshotRecord {
                    last_index: 2,
                    last_term: 9,
                    config: Configuration::of(&[ServerId(1)]),
                    dir: String::new(),
                    taken: false,
                    take: 0,
                })
                .await
                .unwrap();
            let checked = gate(&env, &dir).await;
            let (three, recovered) = RaftStore::open(
                engine.clone(),
                &recovery,
                KeyPrefix::group(3),
                checked.clone(),
            )
            .await
            .unwrap();
            assert_eq!((three.term(), three.vote()), (9, None));
            assert_eq!(three.applied(), 2, "the snapshot's index counts as applied");
            assert!(
                recovered.log.is_empty(),
                "group 3's snapshot covers group 3's log"
            );
            // Group 2 is untouched by all of it: its own term, vote, applied
            // index and log tail past its own snapshot.
            let (two, recovered) =
                RaftStore::open(engine.clone(), &recovery, KeyPrefix::group(2), checked)
                    .await
                    .unwrap();
            assert_eq!((two.term(), two.vote()), (5, Some(ServerId(1))));
            assert_eq!(two.applied(), 1);
            assert_eq!(
                recovered.log,
                vec![entry(5, 2, "two")],
                "group 2 keeps its own tail past its own snapshot"
            );
        })
    });
}

/// A key of another length under the log purpose is not a log key: the open
/// refuses the store rather than read it as an entry (PROPOSED D-060).
#[test]
fn a_log_key_of_another_length_under_the_log_purpose_refuses_the_open() {
    use ananke_raft::store::PURPOSE_LOG;
    use ananke_storage::WriteBatch;

    let mut sim = Sim::new(SimConfig::new(25));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, _) = RaftStore::open_dir(env.clone(), config(), prefix())
                .await
                .unwrap();
            let mut batch = WriteBatch::new();
            batch.put(
                prefix().key(PURPOSE_LOG, b"xy"),
                Bytes::from_static(b"not an entry"),
            );
            store.engine().write(batch, true).await.unwrap();
            drop(store);
            let refused = RaftStore::open_dir(env, config(), prefix())
                .await
                .err()
                .expect("a key that is not a log key refuses the open")
                .to_string();
            assert!(refused.contains("log key"), "{refused}");
        })
    });
}

/// User data is tenant 2 and the system tenant stays empty (SHARD.md §1,
/// PROPOSED D-060): every key a store this build writes is either under the
/// group's prefix in tenant 0 or under tenant 2, and nothing is in tenant 1.
#[test]
fn user_keys_are_tenant_2_and_tenant_1_stays_empty() {
    use ananke_raft::apply::{SYSTEM_TENANT, USER_TENANT};

    assert_eq!(SYSTEM_TENANT, 1);
    assert_eq!(USER_TENANT, 2);
    let mut key = 2u64.to_be_bytes().to_vec();
    key.extend_from_slice(&0u64.to_be_bytes());
    key.extend_from_slice(b"k");
    assert_eq!(user_key(b"k"), Bytes::from(key));

    let mut sim = Sim::new(SimConfig::new(26));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, _) = RaftStore::open_dir(env, config(), prefix()).await.unwrap();
            store
                .persist(&Persist {
                    term: 1,
                    vote: Some(ServerId(1)),
                    truncate_from: None,
                    append: vec![entry(1, 1, "a")],
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            apply_command(
                &store,
                1,
                Some(&Command::Put {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v"),
                }),
            )
            .await
            .unwrap();
            let engine = store.engine();
            let every = engine
                .scan(&[][..]..&[0xff; 32][..], &engine.snapshot())
                .await
                .unwrap();
            assert!(!every.is_empty());
            let span = prefix().span();
            for (k, _) in &every {
                let tenant = u64::from_be_bytes(k[..8].try_into().unwrap());
                match tenant {
                    0 => assert!(
                        k[..] >= span.start[..] && k[..] < span.end[..],
                        "a tenant-0 key outside the group's interval: {k:?}"
                    ),
                    2 => {}
                    other => panic!("a key under tenant {other}: {k:?}"),
                }
            }
            assert!(
                every
                    .iter()
                    .any(|(k, _)| u64::from_be_bytes(k[..8].try_into().unwrap()) == USER_TENANT),
                "the user's key is under tenant 2"
            );
        })
    });
}

//! The persistent state in the engine, on the simulated disk: what a persist writes
//! comes back at the next open, truncation removes what it should, and an entry's
//! writes and the applied index are durable together or not at all, across crashes
//! with every fault on. A disk that lied about a sync shows as a refusal, never as
//! a state with a hole.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Clock, Environment, NodeId};
use ananke_raft::apply::{Command, Outcome, apply_command, user_key};
use ananke_raft::core::Persist;
use ananke_raft::store::{LostState, RaftStore};
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
            let (engine, recovery) = Engine::open(env, config()).await.unwrap();
            let (store, log) = RaftStore::open(Arc::new(engine), &recovery).await.unwrap();
            assert!(log.is_empty());
            assert_eq!((store.term(), store.vote(), store.applied()), (0, None, 0));
            store
                .persist(&Persist {
                    term: 3,
                    vote: Some(ServerId(2)),
                    truncate_from: None,
                    append: vec![entry(1, 1, "a"), entry(3, 2, "b"), entry(3, 3, "c")],
                })
                .await
                .unwrap();
            store
                .persist(&Persist {
                    term: 4,
                    vote: None,
                    truncate_from: Some(3),
                    append: vec![entry(4, 3, "c'"), entry(4, 4, "d")],
                })
                .await
                .unwrap();
            assert_eq!(store.last_index(), 4);
        })
    });
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (engine, recovery) = Engine::open(env, config()).await.unwrap();
            assert!(recovery.replayed > 0, "the writes were in the log");
            let (store, log) = RaftStore::open(Arc::new(engine), &recovery).await.unwrap();
            assert_eq!((store.term(), store.vote(), store.applied()), (4, None, 0));
            assert_eq!(
                log,
                vec![
                    entry(1, 1, "a"),
                    entry(3, 2, "b"),
                    entry(4, 3, "c'"),
                    entry(4, 4, "d")
                ]
            );
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
#[test]
fn an_entrys_writes_and_the_applied_index_are_durable_together() {
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
        // Apply a run of entries, crashing at a random point inside.
        let crash_after = Duration::from_micros(200 + (seed * 7919) % 3000);
        let env = sim.env(node);
        let applied_by_task: Out<Vec<(u64, Outcome)>> = Arc::default();
        let a = applied_by_task.clone();
        env.clone().spawn("applier", async move {
            let (engine, recovery) = Engine::open(env.clone(), config()).await.unwrap();
            let (store, _) = RaftStore::open(Arc::new(engine), &recovery).await.unwrap();
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
                let (engine, recovery) = match Engine::open(env, config()).await {
                    Ok(opened) => opened,
                    Err(e) => return Err(e.to_string()),
                };
                let engine = Arc::new(engine);
                let (store, _) = match RaftStore::open(engine.clone(), &recovery).await {
                    Ok(opened) => opened,
                    Err(e) => {
                        assert!(LostState::from_io(&e).is_some(), "seed {seed}: {e}");
                        return Err(e.to_string());
                    }
                };
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
    assert!(
        verified >= 20,
        "only {verified} of 40 seeds came back as a state; refused: {refused:?}"
    );
}

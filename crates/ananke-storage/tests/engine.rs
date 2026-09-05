//! The engine under the simulator without faults: visibility, tombstones across
//! memtables, the flush pipeline, and replay on reopen. The crash sweep with every
//! fault on is `sim/engine.rs`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Environment, TraceEvent};
use ananke_storage::engine::{self, Variant};
use ananke_storage::{Engine, EngineConfig, EngineRecovery, Retain, Value, wal};
use bytes::Bytes;

fn config(memtable_bytes: u64) -> EngineConfig {
    EngineConfig {
        dir: "/db".into(),
        memtable_bytes,
        segment_bytes: 4096,
        variant: Variant::Correct,
        wal_variant: wal::Variant::Correct,
    }
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

type Out<T> = Arc<Mutex<Option<T>>>;

fn take<T>(out: &Out<T>) -> T {
    out.lock().unwrap().take().expect("the task finished")
}

/// Runs `f` on a fresh node of `sim` until it finishes and returns what it produced.
fn on_node<T: Send + 'static>(
    sim: &mut Sim,
    node: ananke_env::NodeId,
    f: impl FnOnce(SimEnv) -> std::pin::Pin<Box<dyn Future<Output = T> + Send>>,
) -> T {
    let out: Out<T> = Arc::default();
    let o = out.clone();
    let env = sim.env(node);
    let fut = f(env.clone());
    env.spawn("test", async move {
        *o.lock().unwrap() = Some(fut.await);
    });
    sim.run_for(Duration::from_millis(10));
    take(&out)
}

use std::future::Future;

#[test]
fn puts_are_visible_once_acknowledged_and_deletes_shadow_older_memtables() {
    let mut sim = Sim::new(SimConfig::new(1));
    let node = sim.add_node();
    let events = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            // A tiny memtable: two or three writes fill it, so the same key spans
            // several memtables and the flushed stand-in.
            let (db, recovery) = Engine::open(env, config(100), Retain::default())
                .await
                .unwrap();
            assert_eq!(recovery.replayed, 0);
            assert_eq!(db.get(b"a").await.unwrap(), None);
            let seq = db.put(b("a"), b("1")).await.unwrap();
            assert_eq!(seq, 1);
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("1")));
            for i in 0..20 {
                db.put(
                    Bytes::from(format!("filler-{i}")),
                    Bytes::from(vec![b'x'; 40]),
                )
                .await
                .unwrap();
            }
            // "a" now lives in an old memtable; a newer write shadows it, and a
            // tombstone shadows that.
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("1")));
            db.put(b("a"), b("2")).await.unwrap();
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("2")));
            db.delete(b("a")).await.unwrap();
            assert_eq!(db.get(b"a").await.unwrap(), None);
            for i in 20..30 {
                db.put(
                    Bytes::from(format!("filler-{i}")),
                    Bytes::from(vec![b'x'; 40]),
                )
                .await
                .unwrap();
            }
            assert_eq!(
                db.get(b"a").await.unwrap(),
                None,
                "the tombstone outlives its memtable"
            );
            assert_eq!(
                db.get(b"filler-3").await.unwrap(),
                Some(Bytes::from(vec![b'x'; 40]))
            );
            (db.immutable_memtables(), db.sink().len())
        })
    });
    let (immutable, flushed) = events;
    assert!(
        flushed > 3,
        "several memtables were flushed to the stand-in: {flushed}"
    );
    assert_eq!(immutable, 0, "the flusher caught up");
    let trace = sim.trace();
    let rotated = trace
        .iter()
        .filter(|r| matches!(r.event, TraceEvent::MemtableRotated { .. }))
        .count();
    let flushed_events = trace
        .iter()
        .filter(|r| matches!(r.event, TraceEvent::MemtableFlushed { .. }))
        .count();
    assert_eq!((rotated, flushed_events), (flushed, flushed));
}

#[test]
fn reopening_replays_the_log_into_the_same_state() {
    let mut sim = Sim::new(SimConfig::new(2));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env, config(200), Retain::default())
                .await
                .unwrap();
            for i in 0..50u32 {
                db.put(
                    Bytes::from(format!("k{}", i % 10)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
            }
            db.delete(b("k3")).await.unwrap();
            db.put(b("k4"), b("final")).await.unwrap();
        })
    });
    let recovery: EngineRecovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, recovery) = Engine::open(env, config(200), Retain::default())
                .await
                .unwrap();
            assert_eq!(db.get(b"k0").await.unwrap(), Some(b("v40")));
            assert_eq!(db.get(b"k9").await.unwrap(), Some(b("v49")));
            assert_eq!(db.get(b"k3").await.unwrap(), None);
            assert_eq!(db.get(b"k4").await.unwrap(), Some(b("final")));
            assert_eq!(db.get(b"k10").await.unwrap(), None);
            recovery
        })
    });
    assert_eq!((recovery.replayed, recovery.wal.next_seq), (52, 53));
}

#[test]
fn records_round_trip_through_the_op_encoding() {
    for (key, value) in [
        (b(""), Value::Live(b(""))),
        (b("k"), Value::Live(b("v"))),
        (b("key"), Value::Tombstone),
        (
            Bytes::from(vec![0u8; 300]),
            Value::Live(Bytes::from(vec![1u8; 5000])),
        ),
    ] {
        let record = engine::encode_op(&key, &value);
        assert_eq!(engine::decode_op(record).unwrap(), (key, value));
    }
    assert!(engine::decode_op(Bytes::from_static(b"\x02\x00\x00\x00\x00")).is_err());
    assert!(engine::decode_op(Bytes::from_static(b"\x00\x05\x00\x00\x00ab")).is_err());
    assert!(engine::decode_op(Bytes::from_static(b"\x01\x01\x00\x00\x00kx")).is_err());
}

/// The bug the sweep must catch, seen up close: in the buggy variant the write is
/// visible before the log has synced it; in the correct one, only after. The moment
/// of visibility is marked by spawning a task, which the trace records in order.
#[test]
fn the_buggy_variant_acknowledges_before_the_log_syncs() {
    for variant in [Variant::Correct, Variant::NoWalBeforeMemtable] {
        let mut sim = Sim::new(SimConfig::new(3));
        let node = sim.add_node();
        let env = sim.env(node);
        env.clone().spawn("test", async move {
            let mut config = config(1 << 20);
            config.variant = variant;
            let (db, _) = Engine::open(env.clone(), config, Retain::default())
                .await
                .unwrap();
            db.put(b("a"), b("1")).await.unwrap();
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("1")));
            env.spawn("visible", async {});
            // Keep the engine alive.
            std::future::pending::<()>().await;
        });
        sim.run_for(Duration::from_millis(1));
        let trace = sim.trace();
        let visible = trace
            .iter()
            .position(|r| {
                matches!(
                    r.event,
                    TraceEvent::TaskSpawned {
                        name: "visible",
                        ..
                    }
                )
            })
            .expect("the write became visible");
        let synced = trace
            .iter()
            .position(|r| matches!(r.event, TraceEvent::WalSynced { .. }))
            .expect("the log synced");
        match variant {
            Variant::Correct => assert!(synced < visible, "correct: visible only after the sync"),
            Variant::NoWalBeforeMemtable => {
                assert!(visible < synced, "buggy: visible before the sync");
            }
        }
    }
}

//! The engine under the simulator without faults: visibility, tombstones across
//! memtables, the flush pipeline, and replay on reopen. The crash sweep with every
//! fault on is `sim/engine.rs`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use std::path::Path;

use ananke_env::{Clock, Environment, File, FileSystem, OpenOptions, TraceEvent};
use ananke_storage::engine::{self, Variant};
use ananke_storage::{Engine, EngineConfig, EngineRecovery, Value, wal};
use bytes::Bytes;

fn config(memtable_bytes: u64) -> EngineConfig {
    EngineConfig {
        dir: "/db".into(),
        memtable_bytes,
        segment_bytes: 4096,
        variant: Variant::Correct,
        wal_variant: wal::Variant::Correct,
        refuse_log_damage: false,
        allow_head_gap: false,
        allow_manifest_fallback: false,
        l0_trigger: 4,
        level_base_bytes: 8192,
        sst_bytes: 2048,
        background_compaction: false,
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
            let (db, recovery) = Engine::open(env, config(100)).await.unwrap();
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
            (db.immutable_memtables(), db.ssts())
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
            let (db, _) = Engine::open(env, config(200)).await.unwrap();
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
            let (db, recovery) = Engine::open(env, config(200)).await.unwrap();
            assert_eq!(db.get(b"k0").await.unwrap(), Some(b("v40")));
            assert_eq!(db.get(b"k9").await.unwrap(), Some(b("v49")));
            assert_eq!(db.get(b"k3").await.unwrap(), None);
            assert_eq!(db.get(b"k4").await.unwrap(), Some(b("final")));
            assert_eq!(db.get(b"k10").await.unwrap(), None);
            recovery
        })
    });
    // Most of the log was flushed to tables and deleted; only the tail replays.
    assert!(recovery.replayed < 52, "{recovery:?}");
    assert_eq!(recovery.wal.next_seq, 53);
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
            let (db, _) = Engine::open(env.clone(), config).await.unwrap();
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
            _ => {
                assert!(visible < synced, "buggy: visible before the sync");
            }
        }
    }
}

/// A flush writes a table, a manifest and CURRENT, then deletes the log segments the
/// table made redundant; a reopen serves the flushed keys from the table and the
/// rest from the log's tail, and never replays what the table holds.
#[test]
fn a_flush_lands_in_a_table_under_a_manifest_and_frees_the_log() {
    let mut sim = Sim::new(SimConfig::new(9));
    let node = sim.add_node();
    let (manifest, segments, names) = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let mut config = config(600);
            config.segment_bytes = 512;
            let (db, _) = Engine::open(env.clone(), config).await.unwrap();
            for i in 0..80u32 {
                db.put(
                    Bytes::from(format!("k{:03}", i % 30)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
            }
            db.delete(b("k005")).await.unwrap();
            // Let the flusher catch up.
            env.clock().sleep(Duration::from_millis(1)).await;
            let mut names = env.fs().read_dir(Path::new("/db")).await.unwrap();
            names.sort();
            (db.manifest(), db.wal_segments(), names)
        })
    });
    assert!(manifest.number >= 4, "several flushes: {manifest:?}");
    // Manifest 1 is the empty state written at the first open; each flush wrote one.
    assert_eq!(manifest.ssts.len() as u64, manifest.number - 1);
    assert!(manifest.flushed_seq > 40);
    assert!(
        segments.len() < 4,
        "flushed segments were deleted, only the tail remains: {segments:?}"
    );
    let listed: Vec<String> = names.iter().map(|n| n.display().to_string()).collect();
    assert!(listed.iter().any(|n| n == "CURRENT"), "{listed:?}");
    assert!(!listed.iter().any(|n| n == "CURRENT.tmp"), "{listed:?}");
    assert!(
        listed.iter().any(|n| n.starts_with("MANIFEST-")),
        "{listed:?}"
    );
    assert!(listed.iter().filter(|n| n.ends_with(".sst")).count() as u64 == manifest.number - 1);

    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, recovery) = Engine::open(env, config(600)).await.unwrap();
            assert_eq!(db.get(b"k000").await.unwrap(), Some(b("v60")));
            assert_eq!(db.get(b"k029").await.unwrap(), Some(b("v59")));
            assert_eq!(db.get(b"k005").await.unwrap(), None, "the tombstone");
            assert_eq!(db.get(b"k030").await.unwrap(), None);
            recovery
        })
    });
    assert_eq!(recovery.manifest, manifest.number);
    assert_eq!(recovery.flushed_seq, manifest.flushed_seq);
    assert_eq!(
        (recovery.ssts, recovery.dropped.len(), recovery.orphans),
        (manifest.ssts.len(), 0, 0)
    );
    assert!(recovery.fallback_from.is_none());
    assert!(recovery.wal.head_gap.is_none());
    assert_eq!(recovery.replayed as u64, 81 - manifest.flushed_seq);
}

/// A log whose first record is past the manifest's `flushed_seq + 1` is missing its
/// head. The open fails and touches nothing unless `allow_head_gap` is set; then the
/// log is discarded and the tables are the state, a clean prefix (D-022).
#[test]
fn a_missing_log_head_is_refused_unless_allowed_and_then_the_tables_are_the_state() {
    let mut sim = Sim::new(SimConfig::new(11));
    let node = sim.add_node();
    // Segments of two records, so the tail of the log past the last flush spans
    // several whatever the flushes did.
    let config = || {
        let mut c = config(600);
        c.segment_bytes = 64;
        c
    };
    let (flushed_seq, names) = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config()).await.unwrap();
            for i in 0..80u32 {
                db.put(
                    Bytes::from(format!("k{:03}", i % 30)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
            }
            env.clock().sleep(Duration::from_millis(1)).await;
            // More writes until the tail past the last flush spans several segments
            // and the flushes have settled, wherever a flush falls.
            let mut i = 80u32;
            loop {
                db.put(
                    Bytes::from(format!("k{:03}", i % 30)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
                i += 1;
                if db.wal_segments().len() >= 5 {
                    env.clock().sleep(Duration::from_millis(1)).await;
                    if db.wal_segments().len() >= 5 && db.immutable_memtables() == 0 {
                        break;
                    }
                }
            }
            let flushed_seq = db.manifest().flushed_seq;
            assert!(flushed_seq > 30, "{flushed_seq}");
            let segments = db.wal_segments();
            drop(db);
            // The head is in one of the two oldest segments left (the older may hold
            // only flushed records); without both, the log starts past the head.
            let fs = env.fs();
            for &segment in &segments[..2] {
                fs.remove_file(&ananke_storage::wal::segment_path(
                    Path::new("/db"),
                    segment,
                ))
                .await
                .unwrap();
            }
            fs.sync_dir(Path::new("/db")).await.unwrap();
            let mut names = fs.read_dir(Path::new("/db")).await.unwrap();
            names.sort();
            (flushed_seq, names)
        })
    });
    let (gap, names_after) = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let err = match Engine::open(env.clone(), config()).await {
                Err(e) => e,
                Ok((db, recovery)) => {
                    panic!("opened with {recovery:?}, segments {:?}", db.wal_segments())
                }
            };
            let gap = ananke_storage::HeadGap::from_io(&err).unwrap();
            let mut names = env.fs().read_dir(Path::new("/db")).await.unwrap();
            names.sort();
            (gap, names)
        })
    });
    assert_eq!(gap.expected, flushed_seq + 1);
    assert!(gap.found > gap.expected);
    assert_eq!(names_after, names, "a refused open touches nothing");
    let value = |i: u32| Some(Bytes::from(format!("v{i}")));
    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let mut allowed = config();
            allowed.allow_head_gap = true;
            let (db, recovery) = Engine::open(env, allowed).await.unwrap();
            // k000 was written at every thirtieth op; the state is the manifest's
            // prefix, so the newest write at or below flushed_seq is what shows.
            let newest = (flushed_seq - 1) / 30 * 30;
            assert_eq!(db.get(b"k000").await.unwrap(), value(newest as u32));
            // The log starts fresh at the head.
            let seq = db.put(b("k000"), b("fresh")).await.unwrap();
            assert_eq!(seq, flushed_seq + 1);
            recovery
        })
    });
    assert_eq!(recovery.wal.head_gap, Some((gap.expected, gap.found)));
    assert_eq!((recovery.replayed, recovery.wal.records.len()), (0, 0));
    assert!(recovery.wal.discarded >= 1);
    assert!(sim.trace().iter().any(|r| r.event
        == TraceEvent::HeadGap {
            expected: gap.expected,
            found: gap.found,
            discarded: true
        }));
}

/// A snapshot sees what was written at or before it and nothing after, across the
/// active memtable, older memtables and tables; a scan at it is the newest write
/// per key in key order, tombstones hiding older values.
#[test]
fn a_snapshot_pins_what_a_read_or_scan_sees() {
    let mut sim = Sim::new(SimConfig::new(12));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config(300)).await.unwrap();
            for i in 0..40u32 {
                db.put(
                    Bytes::from(format!("k{:02}", i % 10)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
            }
            db.delete(b("k03")).await.unwrap();
            env.clock().sleep(Duration::from_millis(1)).await;
            assert!(db.ssts() >= 2, "several memtables were flushed");
            let before = db.snapshot();
            assert_eq!(before.version(), 41);
            // Newer writes, one of them a tombstone, and a flush of some of them.
            for i in 40..60u32 {
                db.put(
                    Bytes::from(format!("k{:02}", i % 10)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
            }
            db.delete(b("k05")).await.unwrap();
            env.clock().sleep(Duration::from_millis(1)).await;
            let after = db.snapshot();
            assert_eq!(after.version(), 62);
            // Reads at the older snapshot see the older writes.
            assert_eq!(db.get_at(b"k00", &before).await.unwrap(), Some(b("v30")));
            assert_eq!(db.get_at(b"k03", &before).await.unwrap(), None);
            assert_eq!(db.get_at(b"k05", &before).await.unwrap(), Some(b("v35")));
            assert_eq!(db.get_at(b"k00", &after).await.unwrap(), Some(b("v50")));
            assert_eq!(db.get_at(b"k03", &after).await.unwrap(), Some(b("v53")));
            assert_eq!(db.get_at(b"k05", &after).await.unwrap(), None);
            assert_eq!(db.get(b"k05").await.unwrap(), None);
            // Scans likewise, in key order, without the deleted key.
            let keys = |scan: Vec<(Bytes, Bytes)>| -> Vec<String> {
                scan.into_iter()
                    .map(|(k, v)| {
                        format!(
                            "{}={}",
                            String::from_utf8_lossy(&k),
                            String::from_utf8_lossy(&v)
                        )
                    })
                    .collect()
            };
            let all = db.scan(&b"k"[..]..&b"l"[..], &before).await.unwrap();
            assert_eq!(
                keys(all),
                [
                    "k00=v30", "k01=v31", "k02=v32", "k04=v34", "k05=v35", "k06=v36", "k07=v37",
                    "k08=v38", "k09=v39"
                ]
            );
            let some = db.scan(&b"k02"[..]..&b"k06"[..], &after).await.unwrap();
            assert_eq!(keys(some), ["k02=v52", "k03=v53", "k04=v54"]);
            assert!(
                db.scan(&b"x"[..]..&b"y"[..], &after)
                    .await
                    .unwrap()
                    .is_empty()
            );
        })
    });
}

/// Writes keys with overwrites and deletes until several memtables have flushed.
async fn fill(db: &Engine<SimEnv>, ops: std::ops::Range<u32>, keys: u32) {
    for i in ops {
        let key = Bytes::from(format!("k{:03}", i % keys));
        if i % 11 == 7 {
            db.delete(key).await.unwrap();
        } else {
            db.put(key, Bytes::from(format!("v{i}"))).await.unwrap();
        }
    }
}

/// The value key `k` should hold after `fill(0..n)`: its newest write, or none if
/// that was a delete.
fn expected(k: u32, n: u32, keys: u32) -> Option<Bytes> {
    let last = (0..n).rev().find(|i| i % keys == k)?;
    (last % 11 != 7).then(|| Bytes::from(format!("v{last}")))
}

/// Level 0 fills with flushed memtables; a round of compaction merges them into
/// level 1, keeping the newest write per key and dropping what a newer write hides,
/// and the inputs are deleted once the manifest no longer names them. Reads and
/// scans see the same state before and after, and after a reopen.
#[test]
fn compaction_merges_level_0_into_level_1_and_deletes_its_inputs() {
    let mut sim = Sim::new(SimConfig::new(13));
    let node = sim.add_node();
    let (rounds, levels, names) = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config(400)).await.unwrap();
            fill(&db, 0..120, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            let before = db.levels();
            assert!(before[0].len() >= 4, "level 0 filled: {before:?}");
            assert!(before[1..].iter().all(Vec::is_empty));
            let mut rounds = Vec::new();
            while let Some(round) = db.compact_once().await.unwrap() {
                rounds.push(round);
            }
            for k in 0..20 {
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    expected(k, 120, 20),
                    "k{k:03}"
                );
            }
            let snapshot = db.snapshot();
            let scanned: Vec<(Bytes, Bytes)> =
                db.scan(&b"k"[..]..&b"l"[..], &snapshot).await.unwrap();
            let want: Vec<(Bytes, Bytes)> = (0..20)
                .filter_map(|k| expected(k, 120, 20).map(|v| (Bytes::from(format!("k{k:03}")), v)))
                .collect();
            assert_eq!(scanned, want);
            let levels = db.levels();
            drop(db);
            let mut names = env.fs().read_dir(Path::new("/db")).await.unwrap();
            names.sort();
            (rounds, levels, names)
        })
    });
    assert!(!rounds.is_empty());
    assert_eq!(rounds[0].level, 0);
    assert!(rounds[0].dropped_versions > 0, "{:?}", rounds[0]);
    assert!(levels[0].is_empty(), "{levels:?}");
    assert!(!levels[1].is_empty(), "{levels:?}");
    // Level 1 is a run: tables in key order that do not overlap.
    for pair in levels[1].windows(2) {
        assert!(pair[0].last_key < pair[1].first_key, "{levels:?}");
    }
    let on_disk: Vec<String> = names.iter().map(|n| n.display().to_string()).collect();
    for round in &rounds {
        for input in &round.inputs {
            assert!(
                !on_disk.contains(&format!("{input:06}.sst")),
                "input {input} deleted: {on_disk:?}"
            );
        }
        for output in &round.outputs {
            assert!(on_disk.contains(&format!("{output:06}.sst")), "{on_disk:?}");
        }
    }
    // After a reopen the manifest's levels are what was installed.
    let reopened = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, recovery) = Engine::open(env, config(400)).await.unwrap();
            assert_eq!((recovery.dropped.len(), recovery.orphans), (0, 0));
            for k in 0..20 {
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    expected(k, 120, 20)
                );
            }
            db.levels()
        })
    });
    assert_eq!(reopened, levels);
}

/// A live snapshot keeps the versions it can see through compaction; once it is
/// dropped, the next round drops them.
#[test]
fn a_snapshot_keeps_its_versions_through_compaction() {
    let mut sim = Sim::new(SimConfig::new(14));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config(400)).await.unwrap();
            fill(&db, 0..40, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            let old = db.snapshot();
            fill(&db, 40..120, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            let mut kept = 0;
            while let Some(round) = db.compact_once().await.unwrap() {
                kept += round.dropped_versions;
            }
            let _ = kept;
            for k in 0..20 {
                assert_eq!(
                    db.get_at(format!("k{k:03}").as_bytes(), &old)
                        .await
                        .unwrap(),
                    expected(k, 40, 20),
                    "k{k:03} at the old snapshot"
                );
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    expected(k, 120, 20),
                    "k{k:03} now"
                );
            }
            // Once the snapshot is gone, more writes and a round drop what it saw.
            drop(old);
            fill(&db, 120..200, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            let mut dropped = 0;
            while let Some(round) = db.compact_once().await.unwrap() {
                dropped += round.dropped_versions;
            }
            assert!(dropped > 0);
            let entries: u64 = db.levels().iter().flatten().map(|m| m.entries).sum();
            assert!(
                entries <= 20 + 20,
                "one or two versions per key remain: {entries}"
            );
        })
    });
}

/// A tombstone survives compaction while an older write of its key lies deeper,
/// and goes once nothing does; the older write goes with it, never resurfacing.
#[test]
fn tombstones_survive_until_no_older_write_lies_below() {
    let mut sim = Sim::new(SimConfig::new(15));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            // A tiny level 1, so every round pushes tables to the level below and
            // older writes end up deep.
            let mut c = config(400);
            c.level_base_bytes = 1;
            let (db, _) = Engine::open(env.clone(), c).await.unwrap();
            for i in 0..60u32 {
                db.put(
                    Bytes::from(format!("k{:03}", i % 20)),
                    Bytes::from(format!("v{i}")),
                )
                .await
                .unwrap();
            }
            env.clock().sleep(Duration::from_millis(1)).await;
            while db.compact_once().await.unwrap().is_some() {}
            let levels = db.levels();
            let deepest = levels.iter().rposition(|l| !l.is_empty()).unwrap();
            assert!(deepest >= 2, "writes went deep: {levels:?}");
            // Delete every key, flush, and compact the tombstones down one level at
            // a time: they survive as long as the values lie deeper.
            for k in 0..20u32 {
                db.delete(Bytes::from(format!("k{k:03}"))).await.unwrap();
            }
            for i in 60..80u32 {
                db.put(Bytes::from(format!("x{i}")), Bytes::from("filler"))
                    .await
                    .unwrap();
            }
            env.clock().sleep(Duration::from_millis(1)).await;
            let mut tombstones_dropped = 0;
            let mut rounds = 0;
            while let Some(round) = db.compact_once().await.unwrap() {
                rounds += 1;
                tombstones_dropped += round.dropped_tombstones;
                for k in 0..20u32 {
                    assert_eq!(
                        db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                        None,
                        "k{k:03} stays deleted after round {rounds}"
                    );
                }
            }
            assert_eq!(tombstones_dropped, 20, "every tombstone reached its values");
            // The tables hold fillers and nothing else; some fillers may still sit
            // in the active memtable.
            let entries: u64 = db.levels().iter().flatten().map(|m| m.entries).sum();
            assert!(entries <= 20, "only fillers remain: {entries}");
        })
    });
}

/// A CURRENT that cannot be read refuses the store; with fallback allowed, the newest
/// older manifest whose every table is intact is used and CURRENT rewritten. With
/// every table gone, the only intact manifest is the first, the empty one, and the
/// log's head is then missing, which is refused in its turn (D-022).
#[test]
fn an_unreadable_current_refuses_the_store_or_falls_back_only_to_an_intact_manifest() {
    let mut sim = Sim::new(SimConfig::new(16));
    let node = sim.add_node();
    let current = Path::new("/db/CURRENT");
    let manifest = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config(400)).await.unwrap();
            fill(&db, 0..60, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            let manifest = db.manifest();
            assert!(manifest.number >= 2);
            drop(db);
            // Flip a byte of CURRENT.
            let fs = env.fs();
            let file = fs
                .open(current, OpenOptions::new().read(true).write(true))
                .await
                .unwrap();
            let mut bytes = file.read_at(0, 64).await.unwrap().to_vec();
            bytes[3] ^= 0x10;
            file.write_at(0, Bytes::from(bytes)).await.unwrap();
            file.sync().await.unwrap();
            manifest
        })
    });
    let refused = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let err = Engine::open(env, config(400)).await.err().unwrap();
            ananke_storage::OpenRefused::from_io(&err)
        })
    });
    assert_eq!(
        refused,
        Some(ananke_storage::OpenRefused::CurrentUnreadable)
    );
    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let mut allowed = config(400);
            allowed.allow_manifest_fallback = true;
            let (db, recovery) = Engine::open(env.clone(), allowed).await.unwrap();
            for k in 0..20 {
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    expected(k, 60, 20)
                );
            }
            drop(db);
            // CURRENT was rewritten to name the manifest used.
            let bytes = env
                .fs()
                .open(current, OpenOptions::new().read(true))
                .await
                .unwrap()
                .read_at(0, 64)
                .await
                .unwrap();
            assert_eq!(
                ananke_storage::manifest::parse_current(&bytes),
                Some(recovery.manifest)
            );
            recovery
        })
    });
    assert_eq!(recovery.fallback_from, Some(0));
    assert_eq!(
        recovery.manifest, manifest.number,
        "the newest manifest was intact"
    );
    assert!(recovery.dropped.is_empty());
    // Now every table is gone. Fallback passes over every manifest that lists one
    // and lands on the first, which lists none and is intact. The log still holds
    // every write, since its one segment was never deleted, so the store opens on
    // the true state: everything replayed, nothing missing.
    let recovered = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let fs = env.fs();
            for name in fs.read_dir(Path::new("/db")).await.unwrap() {
                if name.extension().is_some_and(|e| e == "sst") {
                    fs.remove_file(&Path::new("/db").join(name)).await.unwrap();
                }
            }
            let file = fs
                .open(current, OpenOptions::new().read(true).write(true))
                .await
                .unwrap();
            let mut bytes = file.read_at(0, 64).await.unwrap().to_vec();
            bytes[3] ^= 0x10;
            file.write_at(0, Bytes::from(bytes)).await.unwrap();
            file.sync().await.unwrap();
            fs.sync_dir(Path::new("/db")).await.unwrap();
            let mut allowed = config(400);
            allowed.allow_manifest_fallback = true;
            let (db, recovery) = Engine::open(env, allowed).await.unwrap();
            for k in 0..20 {
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    expected(k, 60, 20),
                    "k{k:03}"
                );
            }
            recovery
        })
    });
    assert_eq!((recovered.manifest, recovered.fallback_from), (1, Some(0)));
    assert_eq!(
        (recovered.ssts, recovered.flushed_seq, recovered.replayed),
        (0, 0, 60)
    );
    assert!(
        recovered
            .rejected
            .iter()
            .all(|(_, why)| matches!(why, ananke_storage::Rejected::TableMissing(_))),
        "{:?}",
        recovered.rejected
    );
    assert!(recovered.wal.head_gap.is_none());
}

/// The first manifest lists no table and is a valid fallback: with the writes still
/// in the log and CURRENT damaged, the store opens on it and replays every write.
#[test]
fn a_fallback_onto_the_first_manifest_replays_the_log() {
    let mut sim = Sim::new(SimConfig::new(20));
    let node = sim.add_node();
    let current = Path::new("/db/CURRENT");
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            // A memtable nothing fills, so no flush and every write stays in the log.
            let (db, _) = Engine::open(env.clone(), config(1 << 20)).await.unwrap();
            fill(&db, 0..60, 20).await;
            assert_eq!(db.manifest().number, 1);
            drop(db);
            let fs = env.fs();
            let file = fs
                .open(current, OpenOptions::new().read(true).write(true))
                .await
                .unwrap();
            let mut bytes = file.read_at(0, 64).await.unwrap().to_vec();
            bytes[3] ^= 0x10;
            file.write_at(0, Bytes::from(bytes)).await.unwrap();
            file.sync().await.unwrap();
        })
    });
    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let err = Engine::open(env.clone(), config(1 << 20))
                .await
                .err()
                .unwrap();
            assert_eq!(
                ananke_storage::OpenRefused::from_io(&err),
                Some(ananke_storage::OpenRefused::CurrentUnreadable)
            );
            let mut allowed = config(1 << 20);
            allowed.allow_manifest_fallback = true;
            let (db, recovery) = Engine::open(env, allowed).await.unwrap();
            for k in 0..20 {
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    expected(k, 60, 20),
                    "k{k:03}"
                );
            }
            recovery
        })
    });
    assert_eq!((recovery.manifest, recovery.fallback_from), (1, Some(0)));
    assert_eq!(
        (recovery.ssts, recovery.flushed_seq, recovery.replayed),
        (0, 0, 60)
    );
    assert!(recovery.wal.head_gap.is_none());
}

/// Files a crash left behind are removed at open: a table no manifest lists, a
/// manifest never switched to, a CURRENT.tmp.
#[test]
fn orphans_are_removed_at_open() {
    let mut sim = Sim::new(SimConfig::new(10));
    let node = sim.add_node();
    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            // A store exists: its first open wrote the empty manifest and CURRENT.
            let (db, _) = Engine::open(env.clone(), config(1 << 20)).await.unwrap();
            drop(db);
            let fs = env.fs();
            for name in ["000007.sst", "MANIFEST-000009", "CURRENT.tmp"] {
                let file = fs
                    .open(
                        &Path::new("/db").join(name),
                        OpenOptions::new().write(true).create_new(true),
                    )
                    .await
                    .unwrap();
                file.write_at(0, b("junk")).await.unwrap();
                file.sync().await.unwrap();
            }
            fs.sync_dir(Path::new("/db")).await.unwrap();
            let (db, recovery) = Engine::open(env.clone(), config(1 << 20)).await.unwrap();
            db.put(b("a"), b("1")).await.unwrap();
            let mut names = fs.read_dir(Path::new("/db")).await.unwrap();
            names.sort();
            (recovery, names)
        })
    });
    let (recovery, names) = recovery;
    assert_eq!(
        (recovery.manifest, recovery.orphans, recovery.ssts),
        (1, 3, 0)
    );
    let listed: Vec<String> = names.iter().map(|n| n.display().to_string()).collect();
    assert_eq!(
        listed,
        vec!["000001.wal", "000002.wal", "CURRENT", "MANIFEST-000001"]
    );
}

/// A batch is one log record: its writes become visible together under one number,
/// a later write to a key in it replaces an earlier one, and a reopen replays it
/// whole. Files without a CURRENT are a store past recognition, refused.
#[test]
fn a_batch_is_applied_and_replayed_as_one() {
    let mut sim = Sim::new(SimConfig::new(17));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config(1 << 20)).await.unwrap();
            db.put(b("a"), b("old")).await.unwrap();
            let before = db.snapshot();
            let mut batch = ananke_storage::WriteBatch::new();
            batch
                .put(b("a"), b("first"))
                .put(b("b"), b("2"))
                .delete(b("c"))
                .put(b("a"), b("last"));
            assert_eq!(batch.len(), 4);
            let seq = db.write(batch, true).await.unwrap();
            assert_eq!(seq, 2);
            assert_eq!(db.snapshot().version(), 2);
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("last")));
            assert_eq!(db.get(b"b").await.unwrap(), Some(b("2")));
            assert_eq!(db.get_at(b"a", &before).await.unwrap(), Some(b("old")));
            assert_eq!(db.get_at(b"b", &before).await.unwrap(), None);
            // An empty batch takes a number and changes nothing.
            assert_eq!(
                db.write(ananke_storage::WriteBatch::new(), true)
                    .await
                    .unwrap(),
                3
            );
        })
    });
    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, recovery) = Engine::open(env, config(1 << 20)).await.unwrap();
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("last")));
            assert_eq!(db.get(b"b").await.unwrap(), Some(b("2")));
            assert_eq!(db.get(b"c").await.unwrap(), None);
            recovery
        })
    });
    assert_eq!(recovery.replayed, 3);
    // Round trips of the record encoding, batches of one included.
    let ops = vec![
        (b("k"), Value::Live(b("v"))),
        (b(""), Value::Tombstone),
        (b("k"), Value::Live(Bytes::from(vec![7u8; 5000]))),
    ];
    assert_eq!(
        engine::decode_record(engine::encode_batch(&ops)).unwrap(),
        ops
    );
    assert_eq!(
        engine::decode_record(engine::encode_batch(&ops[..1])).unwrap(),
        ops[..1]
    );
    assert_eq!(
        engine::decode_record(engine::encode_batch(&[])).unwrap(),
        vec![]
    );
    let mut torn = engine::encode_batch(&ops).to_vec();
    torn.truncate(torn.len() - 1);
    assert!(engine::decode_record(Bytes::from(torn)).is_err());
}

/// A write without sync is acknowledged once written and visible at once; the next
/// synced write makes it durable, and so does closing the engine.
#[test]
fn an_unsynced_write_is_durable_once_a_later_sync_or_the_close_covers_it() {
    let mut sim = Sim::new(SimConfig::new(18));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, _) = Engine::open(env.clone(), config(1 << 20)).await.unwrap();
            let mut batch = ananke_storage::WriteBatch::new();
            batch.put(b("a"), b("1"));
            let seq = db.write(batch, false).await.unwrap();
            assert_eq!(seq, 1);
            assert_eq!(db.get(b"a").await.unwrap(), Some(b("1")), "visible at once");
            db.put(b("b"), b("2")).await.unwrap();
            let mut batch = ananke_storage::WriteBatch::new();
            batch.put(b("c"), b("3"));
            db.write(batch, false).await.unwrap();
            drop(db);
            env.clock().sleep(Duration::from_millis(1)).await;
        })
    });
    // No sync was forced for record 1; the put's sync covered it, and the close's
    // covered record 3.
    let synced: Vec<u64> = sim
        .trace()
        .iter()
        .filter_map(|r| match r.event {
            TraceEvent::WalSynced { up_to, .. } => Some(up_to),
            _ => None,
        })
        .collect();
    assert_eq!(synced, vec![2, 3], "one sync for the put, one at the close");
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (db, recovery) = Engine::open(env, config(1 << 20)).await.unwrap();
            assert_eq!(recovery.replayed, 3);
            assert_eq!(db.get(b"c").await.unwrap(), Some(b("3")));
        })
    });
}

/// A checkpoint is a store of its own at the version it was taken: a fresh open on
/// its directory sees exactly what a snapshot at that version sees, whatever was
/// written since, and one without its CURRENT is refused.
#[test]
fn a_checkpoint_opens_fresh_at_its_version() {
    let mut sim = Sim::new(SimConfig::new(19));
    let node = sim.add_node();
    let (version, expected) = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let mut c = config(400);
            c.background_compaction = true;
            let (db, _) = Engine::open(env.clone(), c).await.unwrap();
            fill(&db, 0..90, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            let info = db.checkpoint(Path::new("/ckpt/one")).await.unwrap();
            assert_eq!(info.version, 90);
            assert!(info.tables >= 2, "{info:?}");
            let expected: Vec<Option<Bytes>> = (0..20).map(|k| expected(k, 90, 20)).collect();
            // Writes after the checkpoint do not reach it.
            fill(&db, 90..130, 20).await;
            env.clock().sleep(Duration::from_millis(1)).await;
            // The directory must be empty.
            assert_eq!(
                db.checkpoint(Path::new("/ckpt/one"))
                    .await
                    .err()
                    .map(|e| e.kind()),
                Some(std::io::ErrorKind::AlreadyExists)
            );
            (info.version, expected)
        })
    });
    let recovery = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let mut c = config(400);
            c.dir = "/ckpt/one".into();
            let (db, recovery) = Engine::open(env.clone(), c).await.unwrap();
            for (k, want) in expected.iter().enumerate() {
                assert_eq!(
                    db.get(format!("k{k:03}").as_bytes()).await.unwrap(),
                    *want,
                    "k{k:03} in the checkpoint"
                );
            }
            assert_eq!(db.snapshot().version(), version);
            // A write to the checkpoint continues its numbering.
            assert_eq!(db.put(b("k000"), b("x")).await.unwrap(), version + 1);
            drop(db);
            // Without its CURRENT the checkpoint is refused, never opened empty.
            let fs = env.fs();
            fs.remove_file(Path::new("/ckpt/one/CURRENT"))
                .await
                .unwrap();
            fs.sync_dir(Path::new("/ckpt/one")).await.unwrap();
            let mut c = config(400);
            c.dir = "/ckpt/one".into();
            let err = Engine::open(env, c).await.err().unwrap();
            assert_eq!(
                ananke_storage::OpenRefused::from_io(&err),
                Some(ananke_storage::OpenRefused::CurrentMissing)
            );
            recovery
        })
    });
    assert_eq!((recovery.manifest, recovery.flushed_seq), (1, version));
    assert_eq!(
        (recovery.dropped.len(), recovery.orphans, recovery.replayed),
        (0, 0, 0)
    );
}

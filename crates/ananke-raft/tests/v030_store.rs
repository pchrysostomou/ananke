//! D-059's fixture: a store written by ananke-raft 0.3.0's own code, the `v0.3.0` tag's,
//! kept in `tests/fixtures/v0.3.0-store/store` (the README beside it says how, with the
//! commands). A later build must refuse it at open with an error naming its format
//! version and the one the build expects, and read no key of it as the build's own.
//!
//! This file holds the fixture to what its README says, at the engine and under 0.3.0's
//! keys spelled out byte by byte, so that the test of the refusal, which lands with the
//! store's format version (D-059), runs against a store known to be 0.3.0's and whole.
//! Nothing here opens the fixture as a Raft store: this build, whose layout is still
//! 0.3.0's, would open it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Environment, File, FileSystem, OpenOptions};
use ananke_storage::{Engine, EngineConfig};
use bytes::Bytes;

const DIR: &str = "/raft";

/// The fixture's files, as the v0.3.0 tag's code wrote them (README.md beside them).
const FIXTURE: [(&str, &[u8]); 15] = [
    (
        "000001.sst",
        include_bytes!("fixtures/v0.3.0-store/store/000001.sst"),
    ),
    (
        "000001.wal",
        include_bytes!("fixtures/v0.3.0-store/store/000001.wal"),
    ),
    (
        "000002.sst",
        include_bytes!("fixtures/v0.3.0-store/store/000002.sst"),
    ),
    (
        "000003.sst",
        include_bytes!("fixtures/v0.3.0-store/store/000003.sst"),
    ),
    (
        "CURRENT",
        include_bytes!("fixtures/v0.3.0-store/store/CURRENT"),
    ),
    (
        "MANIFEST-000001",
        include_bytes!("fixtures/v0.3.0-store/store/MANIFEST-000001"),
    ),
    (
        "MANIFEST-000002",
        include_bytes!("fixtures/v0.3.0-store/store/MANIFEST-000002"),
    ),
    (
        "MANIFEST-000003",
        include_bytes!("fixtures/v0.3.0-store/store/MANIFEST-000003"),
    ),
    (
        "MANIFEST-000004",
        include_bytes!("fixtures/v0.3.0-store/store/MANIFEST-000004"),
    ),
    (
        "RAFT-STORE",
        include_bytes!("fixtures/v0.3.0-store/store/RAFT-STORE"),
    ),
    (
        "snap-4-1/000001.sst",
        include_bytes!("fixtures/v0.3.0-store/store/snap-4-1/000001.sst"),
    ),
    (
        "snap-4-1/000002.sst",
        include_bytes!("fixtures/v0.3.0-store/store/snap-4-1/000002.sst"),
    ),
    (
        "snap-4-1/000003.sst",
        include_bytes!("fixtures/v0.3.0-store/store/snap-4-1/000003.sst"),
    ),
    (
        "snap-4-1/CURRENT",
        include_bytes!("fixtures/v0.3.0-store/store/snap-4-1/CURRENT"),
    ),
    (
        "snap-4-1/MANIFEST-000001",
        include_bytes!("fixtures/v0.3.0-store/store/snap-4-1/MANIFEST-000001"),
    ),
];

/// The engine as the server opens it (node.rs), with the small memtable and log
/// segment the fixture was written with.
fn engine_config() -> EngineConfig {
    let mut config = EngineConfig::new(PathBuf::from(DIR));
    config.memtable_bytes = 512;
    config.segment_bytes = 4096;
    config.allow_manifest_fallback = false;
    config.allow_head_gap = false;
    config.refuse_log_damage = true;
    config.quiesce_on_loss = true;
    config.background_compaction = false;
    config
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

/// Puts the fixture's files under [`DIR`] on `env`'s disk, synced.
async fn load_fixture(env: &SimEnv) {
    let fs = env.fs();
    let dir = Path::new(DIR);
    fs.create_dir_all(&dir.join("snap-4-1")).await.unwrap();
    for (name, bytes) in FIXTURE {
        let file = fs
            .open(
                &dir.join(name),
                OpenOptions::new().write(true).create_new(true),
            )
            .await
            .unwrap();
        file.write_at(0, Bytes::from_static(bytes)).await.unwrap();
        file.sync().await.unwrap();
    }
    fs.sync_dir(&dir.join("snap-4-1")).await.unwrap();
    fs.sync_dir(dir).await.unwrap();
}

/// A key as ananke-raft 0.3.0 wrote it, spelled out here rather than through this
/// build's helpers, whose layout item 6 changes: tenant and table as big-endian
/// `u64`s, then the name.
fn v030_key(tenant: u64, table: u64, name: &[u8]) -> Bytes {
    let mut out = tenant.to_be_bytes().to_vec();
    out.extend_from_slice(&table.to_be_bytes());
    out.extend_from_slice(name);
    Bytes::from(out)
}

fn le(n: u64) -> Bytes {
    Bytes::copy_from_slice(&n.to_le_bytes())
}

/// The fixture is what its README says: its files are the fifteen the README lists,
/// with the store marker of a whole store; it opens as an engine whose recovery lost
/// nothing, with part of its state in tables; and it holds 0.3.0's Raft state under
/// 0.3.0's keys (RAFT.md §3 as the tag had it), the user's data under tenant 1, and no
/// key shorter than a tenant and a table, where D-059 proposes the format version.
#[test]
fn the_v0_3_0_store_fixture_holds_0_3_0_state_under_0_3_0_keys() {
    let mut sim = Sim::new(SimConfig::new(59));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            load_fixture(&env).await;
            let marker = FIXTURE.iter().find(|(name, _)| *name == "RAFT-STORE");
            assert_eq!(
                marker.map(|(_, bytes)| *bytes),
                Some(&b"ananke raft store\n"[..])
            );
            let (engine, recovery) = Engine::open(env, engine_config()).await.unwrap();
            assert!(!recovery.lost_writes(), "{recovery:?}");
            assert_eq!(recovery.tables.len(), 3, "{recovery:?}");
            assert!(recovery.replayed > 0, "{recovery:?}");
            let get = async |key: &Bytes| engine.get(key).await.unwrap();

            // The hard state: term 3, vote for server 2.
            let mut hard = le(3).to_vec();
            hard.extend_from_slice(&le(2));
            assert_eq!(get(&v030_key(0, 0, b"hard")).await, Some(Bytes::from(hard)));
            assert_eq!(get(&v030_key(0, 0, b"applied")).await, Some(le(5)));
            assert_eq!(get(&v030_key(0, 0, b"incarnation")).await, Some(le(1)));
            assert_eq!(get(&v030_key(0, 0, b"reseeded")).await, None);
            // The configuration key names the configuration entry at index 1.
            let config = get(&v030_key(0, 2, b"config")).await.expect("config key");
            assert_eq!(config.slice(..8), le(1));
            // The snapshot record: index 4, term 2, taken here, take 1, into
            // `/raft/snap-4-1`.
            let record = get(&v030_key(0, 3, b"snapshot")).await.expect("record");
            assert_eq!(record.slice(..8), le(4));
            assert_eq!(record.slice(8..16), le(2));
            assert_eq!(record[16], 1, "taken here");
            assert_eq!(record.slice(17..25), le(1), "take 1");
            let dir = b"/raft/snap-4-1";
            assert_eq!(&record[29..29 + dir.len()], dir);

            let every = engine
                .scan(&[][..]..&[0xff; 32][..], &engine.snapshot())
                .await
                .unwrap();
            // The log past the snapshot, entries 5 to 8, the compaction having
            // deleted 1 to 4; each entry's term, as the program wrote it.
            let log: Vec<(u64, Bytes)> = every
                .iter()
                .filter(|(k, _)| k.len() == 24 && k[..16] == v030_key(0, 1, &[])[..])
                .map(|(k, v)| {
                    (
                        u64::from_be_bytes(k[16..24].try_into().unwrap()),
                        v.slice(..8),
                    )
                })
                .collect();
            assert_eq!(
                log,
                [(5, le(2)), (6, le(2)), (7, le(2)), (8, le(3))],
                "the log"
            );
            // The user's data under tenant 1, applied through entry 5: `a` put as 1
            // and swapped to 3, `b` put and deleted; `c`, `d` and `e` not applied.
            let user: Vec<&Bytes> = every
                .iter()
                .filter(|(k, _)| k[..8] == 1u64.to_be_bytes())
                .map(|(k, _)| k)
                .collect();
            assert_eq!(user, [&v030_key(1, 0, b"a")], "the user's keys");
            assert_eq!(
                get(&v030_key(1, 0, b"a")).await,
                Some(Bytes::from_static(b"3"))
            );
            // Every key is 0.3.0's: a tenant, a table and a name, sixteen bytes at
            // least, in tenant 0 or tenant 1. No key is the format version D-059
            // proposes, the eight bytes of tenant 0 alone.
            assert!(
                every
                    .iter()
                    .all(|(k, _)| k.len() > 16 && k[..7] == [0; 7] && k[7] <= 1),
                "a key that is not 0.3.0's: {every:?}"
            );
            assert_eq!(get(&Bytes::from_static(&[0; 8])).await, None);
            assert_eq!(
                every.len(),
                10,
                "hard, applied, incarnation, four entries, config, snapshot and a: {every:?}"
            );
        })
    });
}

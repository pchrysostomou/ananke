//! D-059's fixture: a store written by ananke-raft 0.3.0's own code, the `v0.3.0` tag's,
//! kept in `tests/fixtures/v0.3.0-store/store` (the README beside it says how, with the
//! commands). A later build must refuse it at open with an error naming its format
//! version and the one the build expects, and read no key of it as the build's own.
//!
//! This file holds the fixture to what its README says, at the engine and under 0.3.0's
//! keys spelled out byte by byte, so that the test of the refusal runs against a store
//! known to be 0.3.0's and whole; and then holds this build to D-059: the fixture is
//! refused naming format 1, format 2 and 0.3.0; the refusal is not a loss; nothing on
//! its disk changes and no file is added; a server started on it stops with one
//! `RaftServerFailed` and touches nothing; the same holds for a 0.3.0 store that has
//! also lost state, which the start that checked lost state first re-seeded instead;
//! and the same holds on a real filesystem.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Environment, File, FileSystem, OpenOptions, RealEnv, TraceEvent};
use ananke_raft::core::Variants;
use ananke_raft::format::{
    FORMAT_FILE, FormatRefused, Found, STORE_FORMAT, Subject, UNRECORDED_FORMAT, check_format,
};
use ananke_raft::node::{SINGLE_GROUP, Start, StartOrder, start_store};
use ananke_raft::store::{KeyPrefix, LostState, RaftStore};
use ananke_raft::types::ServerId;
use ananke_raft::{NodeConfig, RaftConfig, run};
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
            assert!(
                FIXTURE.iter().all(|(name, _)| *name != FORMAT_FILE),
                "0.3.0 recorded no format version: the fixture holds no {FORMAT_FILE}"
            );
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

/// Today's one group (PROPOSED D-060).
fn prefix() -> KeyPrefix {
    KeyPrefix::group(SINGLE_GROUP)
}

/// The engine as the server opens it, for a start on the fixture.
fn server_engine_config() -> EngineConfig {
    let mut config = engine_config();
    config.quiesce_on_loss = true;
    config
}

/// Every file under [`DIR`] on `env`'s disk with its bytes, by path, in name order.
async fn read_tree(env: &SimEnv) -> Vec<(String, Bytes)> {
    let fs = env.fs();
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(relative) = dirs.pop() {
        let Ok(names) = fs.read_dir(&Path::new(DIR).join(&relative)).await else {
            continue;
        };
        for name in names {
            let path = relative.join(&name);
            match fs
                .open(&Path::new(DIR).join(&path), OpenOptions::new().read(true))
                .await
            {
                Ok(file) => {
                    let size = usize::try_from(file.size().await.unwrap()).unwrap();
                    files.push((
                        path.display().to_string(),
                        file.read_at(0, size).await.unwrap(),
                    ));
                }
                Err(_) => dirs.push(path),
            }
        }
    }
    files.sort();
    files
}

/// The refusal a store with no format record carries: 0.3.0's.
fn unrecorded(dir: &str) -> FormatRefused {
    FormatRefused {
        dir: PathBuf::from(dir),
        subject: Subject::Store,
        found: Found::Unrecorded,
        expected: STORE_FORMAT,
    }
}

fn assert_names_both_formats(message: &str) {
    assert!(
        message.contains(&format!("format {UNRECORDED_FORMAT}"))
            && message.contains(&format!("format {STORE_FORMAT}"))
            && message.contains("0.3.0"),
        "the refusal does not name both formats: {message}"
    );
}

/// The owner's test of D-059: the v0.3.0 tag's own store, opened by this build,
/// is refused with an error naming format 1, 0.3.0's, and format 2, this
/// build's. The refusal is not a loss, and nothing of the store is read or
/// written: the gate opens one file that is not there and lists the directory,
/// and every file, every byte and the file list itself are as the tag left them
/// — no new log segment, no marker, no lost mark, no record of this build's.
#[test]
fn the_v0_3_0_tags_store_is_refused_before_anything_writes() {
    let mut sim = Sim::new(SimConfig::new(59));
    let node = sim.add_node();
    let words = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            load_fixture(&env).await;
            let before = read_tree(&env).await;
            let refused = check_format(&env, Path::new(DIR))
                .await
                .expect_err("the v0.3.0 store is refused");
            assert_eq!(FormatRefused::from_io(&refused), Some(unrecorded(DIR)));
            assert!(LostState::from_io(&refused).is_none(), "{refused}");
            // The store's own open refuses it the same way, and reads no key.
            let opened = RaftStore::open_dir(env.clone(), server_engine_config(), prefix())
                .await
                .err()
                .expect("the v0.3.0 store is refused at open");
            assert_eq!(FormatRefused::from_io(&opened), Some(unrecorded(DIR)));
            assert_eq!(read_tree(&env).await, before, "the refusal wrote");
            (refused.to_string(), before.len())
        })
    });
    assert_eq!(words.1, 15, "the fixture's fifteen files");
    assert_names_both_formats(&words.0);
    println!("the v0.3.0 store's refusal: {}", words.0);
    // And on the durable disk: the same names and the same bytes, with no
    // RAFT-FORMAT, no temporary name and no new log segment.
    let names: Vec<String> = sim
        .durable_names(node, Path::new(DIR))
        .iter()
        .map(|n| n.display().to_string())
        .collect();
    for (name, bytes) in FIXTURE {
        if !name.contains('/') {
            assert!(names.contains(&name.to_owned()), "{name} went missing");
        }
        assert_eq!(
            sim.durable_contents(node, &Path::new(DIR).join(name))
                .as_deref(),
            Some(bytes),
            "{name} changed"
        );
    }
    assert_eq!(
        names.len(),
        10,
        "ten files and the snapshot directory: {names:?}"
    );
    assert!(
        !names
            .iter()
            .any(|n| n.starts_with("RAFT-FORMAT") || n == "000002.wal"),
        "the refusal added a file: {names:?}"
    );
}

fn addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, u8::try_from(n).expect("small")], 7000))
}

fn node_config() -> NodeConfig {
    NodeConfig {
        id: ServerId(1),
        listen: addr(1),
        servers: (1..=3).map(|s| (ServerId(s), addr(s))).collect(),
        initial_voters: (1..=3).map(ServerId).collect(),
        raft: RaftConfig::default(),
        engine: engine_config(),
        inbox_capacity: 64,
    }
}

/// Runs a server on whatever is at [`DIR`] and returns its error and the trace
/// it left, giving it a simulated second to stop.
fn run_server(sim: &mut Sim, node: ananke_env::NodeId) -> io::Result<()> {
    let ran: Arc<Mutex<Option<io::Result<()>>>> = Arc::default();
    let r = ran.clone();
    let env = sim.env(node);
    env.clone().spawn("server", async move {
        *r.lock().unwrap() = Some(run(env, node_config()).await);
    });
    for _ in 0..1_000 {
        if ran.lock().unwrap().is_some() {
            break;
        }
        sim.run_for(Duration::from_millis(1));
    }
    match ran.lock().unwrap().take() {
        Some(result) => result,
        None => panic!("the server on the v0.3.0 store is still running a second in"),
    }
}

/// A server started on the v0.3.0 store stops with the refusal, traced once with
/// both formats, and changes not one byte: it neither marks the store lost nor
/// waits in re-seed mode for a snapshot to take its place, and the engine never
/// opens — so not even the empty log segment every engine open adds is there.
#[test]
fn a_server_on_the_v0_3_0_store_stops_and_changes_no_byte() {
    let mut sim = Sim::new(SimConfig::new(59));
    let node = sim.add_node();
    let fixture = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            load_fixture(&env).await;
            read_tree(&env).await
        })
    });
    let from = sim.trace().len();
    let error = run_server(&mut sim, node).expect_err("the server stops on the v0.3.0 store");
    assert_eq!(
        FormatRefused::from_io(&error),
        Some(unrecorded(DIR)),
        "{error}"
    );
    let trace = sim.trace();
    let failed: Vec<&str> = trace[from..]
        .iter()
        .filter_map(|r| match &r.event {
            TraceEvent::RaftServerFailed { server: 1, reason } => Some(reason.as_str()),
            _ => None,
        })
        .collect();
    let [reason] = failed.as_slice() else {
        panic!("the server did not fail exactly once: {failed:?}");
    };
    assert_names_both_formats(reason);
    assert!(
        !trace[from..].iter().any(|r| matches!(
            r.event,
            TraceEvent::RaftRefused { .. }
                | TraceEvent::RaftAdopted { .. }
                | TraceEvent::RaftRecovered { .. }
        )),
        "the server went on past the refusal"
    );
    // The engine never opened: none of its records is in the trace.
    assert!(
        !trace[from..].iter().any(|r| matches!(
            r.event,
            TraceEvent::WalRecovered { .. }
                | TraceEvent::WalSegmentOpened { .. }
                | TraceEvent::WalTruncated { .. }
                | TraceEvent::OrphanRemoved { .. }
                | TraceEvent::ManifestWritten { .. }
                | TraceEvent::CurrentSwitched { .. }
                | TraceEvent::OpenRefused { .. }
                | TraceEvent::SstDropped { .. }
                | TraceEvent::EngineQuiesced { .. }
        )),
        "the engine opened on a store this build refuses"
    );
    let after = on_node(&mut sim, node, |env| {
        Box::pin(async move { read_tree(&env).await })
    });
    let (kept, added): (Vec<_>, Vec<_>) = after
        .into_iter()
        .partition(|(name, _)| fixture.iter().any(|(f, _)| f == name));
    assert_eq!(kept, fixture, "the server changed the store");
    assert_eq!(
        added,
        Vec::<(String, Bytes)>::new(),
        "the server added a file to a store it refuses"
    );
}

/// The owner's fifth condition: the format is checked before lost state, so a
/// v0.3.0 store that has *also* lost state is refused for its format and never
/// re-seeded into its own directory. Five shapes, each a way a 0.3.0 store can
/// have lost something, and each refused with nothing written.
///
/// The pair is `LostStateBeforeFormat`, D-059's first draft and the held check
/// patch: the same five shapes are read as lost state — marked lost and
/// re-seeded, or adopted over — before the format is ever read.
#[test]
fn a_v0_3_0_store_that_lost_state_is_refused_for_its_format_never_reseeded() {
    let shapes = [
        "a lost mark",
        "a rotted table",
        "a rotted log record",
        "a completed staged install",
        "nothing but the marker",
    ];
    let mut caught = 0;
    for (shape, what) in shapes.iter().enumerate() {
        let mut sim = Sim::new(SimConfig::new(590 + shape as u64));
        let node = sim.add_node();
        let before = on_node(&mut sim, node, |env| {
            Box::pin(async move {
                load_fixture(&env).await;
                make_shape(&env, shape).await;
                read_tree(&env).await
            })
        });
        // The correct start: refused for its format, with nothing written.
        let (word, message, after) = on_node(&mut sim, node, |env| {
            Box::pin(async move {
                let started = start_store(
                    &env,
                    1,
                    &server_engine_config(),
                    Variants::correct(),
                    &prefix(),
                    StartOrder::Correct,
                )
                .await;
                let (word, message) = match started {
                    Start::Opened { .. } => ("opened", String::new()),
                    Start::Refused(error) => ("refused", error.to_string()),
                    Start::Failed(error) => {
                        assert_eq!(
                            FormatRefused::from_io(&error),
                            Some(unrecorded(DIR)),
                            "{error}"
                        );
                        ("failed", error.to_string())
                    }
                };
                (word, message, read_tree(&env).await)
            })
        });
        assert_eq!(word, "failed", "{what}: {message}");
        assert_names_both_formats(&message);
        assert_eq!(after, before, "{what}: the refused store was written to");

        // The pair, on the same shape: lost state read first.
        let mut sim = Sim::new(SimConfig::new(590 + shape as u64));
        let node = sim.add_node();
        on_node(&mut sim, node, |env| {
            Box::pin(async move {
                load_fixture(&env).await;
                make_shape(&env, shape).await;
            })
        });
        let (word, message, changed) = on_node(&mut sim, node, |env| {
            let before = before.clone();
            Box::pin(async move {
                let started = start_store(
                    &env,
                    1,
                    &server_engine_config(),
                    Variants::correct(),
                    &prefix(),
                    StartOrder::LostStateBeforeFormat,
                )
                .await;
                let (word, message) = match started {
                    Start::Opened { .. } => ("opened", String::new()),
                    Start::Refused(error) => ("refused", error.to_string()),
                    Start::Failed(error) => ("failed", error.to_string()),
                };
                (word, message, read_tree(&env).await != before)
            })
        });
        println!(
            "{what}: the order that reads lost state first {word} (the store changed: \
             {changed}): {message}"
        );
        // The catch: the store is either refused as lost — marked and re-seeded
        // into its own directory — or written to before any format is read. The
        // correct start above did neither.
        assert!(
            word == "refused" || changed,
            "{what}: the pair neither refused the store as lost nor touched it"
        );
        caught += 1;
    }
    assert_eq!(caught, shapes.len(), "the pair was caught on every shape");
}

/// Turns the loaded fixture into one of the five shapes of a 0.3.0 store that
/// has also lost something.
async fn make_shape(env: &SimEnv, shape: usize) {
    let fs = env.fs();
    let dir = Path::new(DIR);
    match shape {
        // (a) 0.3.0's own lost mark, as its refusal would have written it.
        0 => {
            let file = fs
                .open(
                    &dir.join("RAFT-STORE"),
                    OpenOptions::new().write(true).create(true).truncate(true),
                )
                .await
                .unwrap();
            file.write_at(
                0,
                Bytes::from_static(b"ananke raft store lost\nthe engine's recovery lost state\n"),
            )
            .await
            .unwrap();
            file.sync().await.unwrap();
            fs.sync_dir(dir).await.unwrap();
        }
        // (b) One bit of a table the newest manifest lists.
        1 => rot(env, &dir.join("000002.sst"), 40).await,
        // (c) One bit of the first record of the log.
        2 => rot(env, &dir.join("000001.wal"), 24).await,
        // (d) A completed install: a byte copy of the fixture's own checkpoint.
        3 => {
            let staging = dir.join("install");
            fs.create_dir_all(&staging).await.unwrap();
            for (name, bytes) in FIXTURE {
                let Some(inner) = name.strip_prefix("snap-4-1/") else {
                    continue;
                };
                let file = fs
                    .open(
                        &staging.join(inner),
                        OpenOptions::new().write(true).create(true),
                    )
                    .await
                    .unwrap();
                file.write_at(0, Bytes::from_static(bytes)).await.unwrap();
                file.sync().await.unwrap();
            }
            fs.sync_dir(&staging).await.unwrap();
        }
        // (e) A directory 0.3.0 left holding nothing but its marker.
        _ => {
            for name in fs.read_dir(dir).await.unwrap() {
                if name.to_str() != Some("RAFT-STORE") {
                    let _ = fs.remove_file(&dir.join(name)).await;
                }
            }
            fs.sync_dir(dir).await.unwrap();
        }
    }
}

/// Flips one bit of `path` at `offset`, synced.
async fn rot(env: &SimEnv, path: &Path, offset: u64) {
    let file = env
        .fs()
        .open(path, OpenOptions::new().read(true).write(true))
        .await
        .unwrap();
    let byte = file.read_at(offset, 1).await.unwrap();
    file.write_at(offset, Bytes::from(vec![byte[0] ^ 0x20]))
        .await
        .unwrap();
    file.sync().await.unwrap();
}

/// The same refusal on a real filesystem (D-059): the v0.3.0 store copied into a
/// temporary directory is refused with the same words, and every name, every
/// size and every byte is as it was, with no entry added. A directory that is
/// not there is fresh, and is still not there afterwards: the gate creates
/// nothing.
#[test]
fn a_v0_3_0_store_on_a_real_filesystem_is_refused_and_left_untouched() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let root = temp.path().to_path_buf();
    let store = root.join("store");
    let refusal = RealEnv::run(|env| {
        let store = store.clone();
        let root = root.clone();
        async move {
            let fs = env.fs();
            fs.create_dir_all(&store.join("snap-4-1")).await.unwrap();
            for (name, bytes) in FIXTURE {
                let file = fs
                    .open(
                        &store.join(name),
                        OpenOptions::new().write(true).create(true),
                    )
                    .await
                    .unwrap();
                file.write_at(0, Bytes::from_static(bytes)).await.unwrap();
                file.sync().await.unwrap();
            }
            let before = real_tree(&env, &store).await;
            let refused = check_format(&env, &store)
                .await
                .expect_err("the v0.3.0 store is refused on a real disk");
            assert_eq!(
                FormatRefused::from_io(&refused),
                Some(FormatRefused {
                    dir: store.clone(),
                    subject: Subject::Store,
                    found: Found::Unrecorded,
                    expected: STORE_FORMAT,
                })
            );
            let mut config = engine_config();
            config.dir = store.clone();
            let opened = RaftStore::open_dir(env.clone(), config, prefix())
                .await
                .err()
                .expect("the store's open refuses it too");
            assert!(FormatRefused::from_io(&opened).is_some(), "{opened}");
            assert_eq!(
                real_tree(&env, &store).await,
                before,
                "the refusal changed the store on a real disk"
            );
            // A directory that is not there is fresh, and stays not there.
            let absent = root.join("absent");
            check_format(&env, &absent).await.expect("fresh");
            assert!(
                fs.read_dir(&absent).await.is_err(),
                "the gate created the directory"
            );
            refused.to_string()
        }
    });
    assert_names_both_formats(&refusal);
}

/// Every file under `dir` on a real disk with its size and bytes, by path.
async fn real_tree(env: &RealEnv, dir: &Path) -> Vec<(String, u64, Bytes)> {
    let fs = env.fs();
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(relative) = dirs.pop() {
        let Ok(names) = fs.read_dir(&dir.join(&relative)).await else {
            continue;
        };
        for name in names {
            let path = relative.join(&name);
            // A real `open` of a directory succeeds on macOS, so the listing
            // decides which it is.
            if fs.read_dir(&dir.join(&path)).await.is_ok() {
                dirs.push(path);
                continue;
            }
            let file = fs
                .open(&dir.join(&path), OpenOptions::new().read(true))
                .await
                .unwrap();
            let size = file.size().await.unwrap();
            let bytes = file
                .read_at(0, usize::try_from(size).unwrap())
                .await
                .unwrap();
            files.push((path.display().to_string(), size, bytes));
        }
    }
    files.sort();
    files
}

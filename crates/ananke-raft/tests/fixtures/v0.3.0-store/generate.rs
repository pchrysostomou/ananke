//! Writes the v0.3.0 store fixture (D-059): a Raft store made by ananke-raft 0.3.0's own
//! code, so that a later build's refusal of that format is tested against the real thing
//! and not against a store the later build made up.
//!
//! This file is not compiled in the tree it is kept in. It is run from the `v0.3.0` tag,
//! copied into that tree as an example of `ananke-raft`; README.md beside it gives the
//! exact commands. It uses only what 0.3.0 has.
//!
//! The store is written under the simulator, which makes it byte for byte the same on every
//! run and names the store `/raft`, the path the snapshot record carries, rather than a
//! directory of the machine that ran it. The engine and the store are 0.3.0's; the
//! simulator is only the disk they write to. The files are then copied out to a real
//! directory through `RealEnv`.
//!
//! What it writes, in the order a server does: the engine opened as the server opens it,
//! the store opened (which writes the incarnation), the store marker, one persist of the
//! hard state, a configuration entry and six commands with the configuration key, four
//! applies, a snapshot taken at index 4 into its version directory, the log compacted to
//! it, a fifth apply, and a last persist of a new term with one more entry. A memtable of
//! 512 bytes puts some of it in tables and the rest in the log.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Clock, Environment, File, FileSystem, OpenOptions, RealEnv};
use ananke_raft::apply::{Command, apply_command};
use ananke_raft::core::Persist;
use ananke_raft::snapshot::take_version;
use ananke_raft::store::{RaftStore, mark_store};
use ananke_raft::types::{Configuration, Entry, Payload, ServerId};
use ananke_storage::{Engine, EngineConfig};
use bytes::Bytes;

/// The store's directory on the simulated disk.
const DIR: &str = "/raft";

/// The simulator's seed. Nothing the store does draws from it; it is fixed so the run is.
const SEED: u64 = 59;

/// The engine as the server configures it (node.rs), with a small memtable and log
/// segment so the store is a few kilobytes and has both tables and a log.
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

fn put(key: &str, value: &str) -> Command {
    Command::Put {
        key: Bytes::copy_from_slice(key.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
    }
}

/// Writes the store on `env`'s disk.
async fn write_store(env: SimEnv) {
    let dir = Path::new(DIR);
    let (engine, recovery) = Engine::open(env.clone(), engine_config())
        .await
        .expect("the engine opens");
    let (store, _) = RaftStore::open(Arc::new(engine), &recovery)
        .await
        .expect("the store opens");
    mark_store(&env, dir).await.expect("the marker is written");

    let config = Configuration::of(&[ServerId(1), ServerId(2), ServerId(3)]);
    let commands = [
        put("a", "1"),
        put("b", "2"),
        Command::Cas {
            key: Bytes::from_static(b"a"),
            expect: Some(Bytes::from_static(b"1")),
            value: Bytes::from_static(b"3"),
        },
        Command::Delete {
            key: Bytes::from_static(b"b"),
        },
        put("c", "4"),
        put("d", "5"),
    ];
    let mut entries = vec![Entry {
        term: 1,
        index: 1,
        payload: Payload::Config(config.clone()),
    }];
    for (i, command) in commands.iter().enumerate() {
        let index = i as u64 + 2;
        entries.push(Entry {
            term: if index <= 3 { 1 } else { 2 },
            index,
            payload: Payload::Command(command.encode()),
        });
    }
    store
        .persist(&Persist {
            term: 2,
            vote: Some(ServerId(1)),
            truncate_from: None,
            append: entries.clone(),
            config: Some((1, config.clone())),
            compact_to: None,
        })
        .await
        .expect("the entries persist");

    let apply = async |entry: &Entry| {
        let command = match &entry.payload {
            Payload::Command(bytes) => Some(Command::decode(bytes.clone()).expect("a command")),
            _ => None,
        };
        apply_command(&store, entry.index, command.as_ref())
            .await
            .expect("the entry applies");
    };
    for entry in &entries[..4] {
        apply(entry).await;
    }
    take_version(&env, &store, dir, 4, 2, &config)
        .await
        .expect("the snapshot is taken");
    store
        .persist(&Persist {
            term: 2,
            vote: Some(ServerId(1)),
            truncate_from: None,
            append: Vec::new(),
            config: None,
            compact_to: Some(4),
        })
        .await
        .expect("the log compacts");
    apply(&entries[4]).await;
    store
        .persist(&Persist {
            term: 3,
            vote: Some(ServerId(2)),
            truncate_from: None,
            append: vec![Entry {
                term: 3,
                index: 8,
                payload: Payload::Command(put("e", "6").encode()),
            }],
            config: None,
            compact_to: None,
        })
        .await
        .expect("the last entry persists");
    // Every rotated memtable flushed, so what is on disk does not depend on when the
    // copy below reads it.
    while store.engine().immutable_memtables() > 0 {
        env.clock().sleep(Duration::from_millis(1)).await;
    }
}

/// Every file under `dir` on `env`'s disk, with its path relative to `dir`, in name order.
async fn read_tree(env: SimEnv, dir: PathBuf) -> Vec<(PathBuf, Bytes)> {
    let fs = env.fs();
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(relative) = dirs.pop() {
        for name in fs
            .read_dir(&dir.join(&relative))
            .await
            .expect("a directory")
        {
            let path = relative.join(&name);
            match fs
                .open(&dir.join(&path), OpenOptions::new().read(true))
                .await
            {
                Ok(file) => {
                    let size = usize::try_from(file.size().await.expect("a size")).expect("fits");
                    files.push((path, file.read_at(0, size).await.expect("the bytes")));
                }
                Err(_) => dirs.push(path),
            }
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

/// Runs `f` on a new node of `sim` until it finishes and returns what it produced.
fn on_node<T: Send + 'static, F: Future<Output = T> + Send + 'static>(
    sim: &mut Sim,
    f: impl FnOnce(SimEnv) -> F,
) -> T {
    let node = sim.add_node();
    let out: Arc<Mutex<Option<T>>> = Arc::default();
    let o = out.clone();
    let env = sim.env(node);
    let fut = f(env.clone());
    env.spawn("fixture", async move {
        *o.lock().unwrap() = Some(fut.await);
    });
    while out.lock().unwrap().is_none() {
        sim.run_for(Duration::from_millis(1));
    }
    out.lock().unwrap().take().expect("the task finished")
}

fn main() {
    let out = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: v030_store_fixture <output directory, which must not exist>"),
    );
    let mut sim = Sim::new(SimConfig::new(SEED));
    let files = on_node(&mut sim, |env| async move {
        write_store(env.clone()).await;
        read_tree(env, PathBuf::from(DIR)).await
    });
    RealEnv::run(|env| async move {
        let fs = env.fs();
        assert!(
            fs.read_dir(&out).await.is_err(),
            "{} already exists",
            out.display()
        );
        for (path, bytes) in &files {
            let target = out.join(path);
            fs.create_dir_all(target.parent().expect("a parent"))
                .await
                .expect("the directory is made");
            let file = fs
                .open(&target, OpenOptions::new().write(true).create_new(true))
                .await
                .expect("the file is created");
            file.write_at(0, bytes.clone())
                .await
                .expect("the bytes are written");
            file.sync().await.expect("the file is synced");
            println!("{:>6} {}", bytes.len(), path.display());
        }
    });
}

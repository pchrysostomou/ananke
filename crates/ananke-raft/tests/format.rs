//! The store's format record (D-059, PROPOSED D-060): what it survives, what it
//! refuses, and what a crash can leave.
//!
//! The record is the only thing that says which layout a store directory holds,
//! so these tests hold it to four things: one bit of rot never reads as another
//! version and never loses the record; a store this build writes records its
//! format before anything else of the directory exists, at every crash point; a
//! store recording another version is refused with nothing written; and a record
//! that cannot be read at all is lost state, which an install replaces — never a
//! format refusal, and never a store read under the wrong layout.
//!
//! Each known-buggy order beside the correct one is a [`StartOrder`], caught here
//! by the same crashes or the same shapes (CLAUDE.md's pair rule).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Environment, File, FileSystem, NodeId, OpenOptions};
use ananke_raft::apply::{Command, apply_command};
use ananke_raft::core::{Persist, Variant, Variants};
use ananke_raft::format::{
    COPY_LEN, Decoded, FORMAT_FILE, FORMAT_TMP, FormatRefused, Found, RECORD_LEN, STORE_FORMAT,
    Subject, UNRECORDED_FORMAT, Verdict, check_format, decode_record, encode_record, format_path,
    record_format,
};
use ananke_raft::node::{SINGLE_GROUP, Start, StartOrder, start_store};
use ananke_raft::snapshot::{
    Assembler, Feed, Repair, Sender, adopt_staged, staging_dir, take_version,
};
use ananke_raft::store::{Damage, KeyPrefix, LostState, RaftStore, STORE_MARKER, mark_store};
use ananke_raft::types::{Configuration, Entry, Index, Payload, ServerId, Term};
use ananke_storage::{EngineConfig, manifest};
use bytes::Bytes;

const DIR: &str = "/raft";

/// The magic every copy of the record begins with: twenty-five bytes, permanent
/// (PROPOSED D-060), spelled out here rather than read from the crate so a
/// change to it fails this test.
const MAGIC: &[u8] = b"ananke raft store format\n";

fn engine_config(dir: &str) -> EngineConfig {
    let mut config = EngineConfig::new(PathBuf::from(dir));
    config.memtable_bytes = 4096;
    config.segment_bytes = 4096;
    config.allow_manifest_fallback = false;
    config.allow_head_gap = false;
    config.refuse_log_damage = true;
    config.quiesce_on_loss = true;
    config.background_compaction = false;
    config
}

/// Today's one group (PROPOSED D-060).
fn prefix() -> KeyPrefix {
    KeyPrefix::group(SINGLE_GROUP)
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
    for _ in 0..100_000 {
        if out.lock().unwrap().is_some() {
            break;
        }
        sim.run_for(Duration::from_millis(1));
    }
    out.lock().unwrap().take().expect("the task finished")
}

/// Every entry under `dir` on `env`'s disk, by path, in name order: a file with
/// its bytes, and a directory as its path with a trailing `/` and no bytes.
/// Directories are in the picture because a tree this says is unchanged must be
/// unchanged in its directories too — a staging created and left behind is a
/// change to the store.
async fn read_tree(env: &SimEnv, dir: &str) -> Vec<(String, Bytes)> {
    let fs = env.fs();
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(relative) = dirs.pop() {
        let Ok(names) = fs.read_dir(&Path::new(dir).join(&relative)).await else {
            continue;
        };
        for name in names {
            let path = relative.join(&name);
            match fs
                .open(&Path::new(dir).join(&path), OpenOptions::new().read(true))
                .await
            {
                Ok(file) => {
                    let size = usize::try_from(file.size().await.unwrap()).unwrap();
                    files.push((
                        path.display().to_string(),
                        file.read_at(0, size).await.unwrap(),
                    ));
                }
                Err(_) => {
                    files.push((format!("{}/", path.display()), Bytes::new()));
                    dirs.push(path);
                }
            }
        }
    }
    files.sort();
    files
}

/// Writes `bytes` over the record of `dir`, synced: rot, or another build's
/// version.
async fn put_record(env: &SimEnv, dir: &str, bytes: Bytes) {
    let file = env
        .fs()
        .open(
            &format_path(Path::new(dir)),
            OpenOptions::new().write(true).create(true).truncate(true),
        )
        .await
        .unwrap();
    file.write_at(0, bytes).await.unwrap();
    file.sync().await.unwrap();
    env.fs().sync_dir(Path::new(dir)).await.unwrap();
}

/// The record on `dir`'s disk now.
async fn get_record(env: &SimEnv, dir: &str) -> Option<Bytes> {
    let file = env
        .fs()
        .open(&format_path(Path::new(dir)), OpenOptions::new().read(true))
        .await
        .ok()?;
    let size = usize::try_from(file.size().await.unwrap()).unwrap();
    Some(file.read_at(0, size).await.unwrap())
}

fn entry(term: Term, index: Index, command: &str) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Command(Bytes::from(command.to_owned())),
    }
}

/// A store at `dir` with a little state: four entries persisted, two applied,
/// the marker written — a store this build wrote, whole.
async fn build_store(env: &SimEnv, dir: &str) -> Arc<RaftStore<SimEnv>> {
    let (store, _) = RaftStore::open_dir(env.clone(), engine_config(dir), prefix())
        .await
        .unwrap();
    store
        .persist(&Persist {
            term: 3,
            vote: Some(ServerId(1)),
            truncate_from: None,
            append: (1..=4u64).map(|i| entry(3, i, &format!("e{i}"))).collect(),
            config: None,
            compact_to: None,
        })
        .await
        .unwrap();
    for index in 1..=2u64 {
        apply_command(
            &store,
            index,
            Some(&Command::Put {
                key: Bytes::from(format!("k{index}")),
                value: Bytes::from(format!("v{index}")),
            }),
        )
        .await
        .unwrap();
    }
    mark_store(env, Path::new(dir)).await.unwrap();
    Arc::new(store)
}

/// The start the server runs, for a test that wants its verdict.
async fn start(env: &SimEnv, dir: &str, order: StartOrder) -> Start<SimEnv> {
    start_with(env, dir, Variants::correct(), order).await
}

/// The same, for a test that drives a known-buggy variant's own start rather
/// than a known-buggy order.
async fn start_with(
    env: &SimEnv,
    dir: &str,
    variants: Variants,
    order: StartOrder,
) -> Start<SimEnv> {
    start_store(env, 1, &engine_config(dir), variants, &prefix(), order).await
}

/// What a start came to, as a word and a message: the shapes these tests count.
fn outcome(start: &Start<SimEnv>) -> (&'static str, String) {
    match start {
        Start::Opened { .. } => ("opened", String::new()),
        Start::Refused(error) => ("refused", error.to_string()),
        Start::Failed(error) => ("failed", error.to_string()),
    }
}

// --- E6: the codec ---

/// The version copy 1 names, read without its checksum: what a build that kept
/// the magic and the version but no checksum would decode (the pair).
fn version_without_checksum(bytes: &[u8]) -> Option<u64> {
    let copy = bytes.get(..COPY_LEN)?;
    (copy[..MAGIC.len()] == *MAGIC)
        .then(|| u64::from_le_bytes(copy[MAGIC.len()..MAGIC.len() + 8].try_into().unwrap()))
}

/// One bit of rot can never turn the record into another version, and can never
/// make it unreadable: every single flip of the 74 bytes still decodes format 2,
/// with `whole` false so the start heals it. Two flips, one in each copy — two
/// crashes with no completed start between them — are what it takes to lose it,
/// and that is damage, never a version. A truncation is the same.
///
/// The pair is the decoder without the checksum: on some flip it reads a version
/// that was never written, which is the whole reason for the CRC (PROPOSED
/// D-060, alternative "one copy, plain text, or no CRC").
#[test]
fn the_format_record_survives_one_flip_and_never_reads_as_another_version() {
    let record = encode_record(STORE_FORMAT);
    assert_eq!(record.len(), RECORD_LEN);
    assert_eq!(&record[..MAGIC.len()], MAGIC, "the magic is permanent");
    assert_eq!(
        &record[..COPY_LEN],
        &record[COPY_LEN..],
        "two copies of the same bytes"
    );
    assert_eq!(
        decode_record(&record),
        Decoded::Valid {
            version: STORE_FORMAT,
            whole: true
        }
    );
    // Every single-bit flip: still format 2, never whole.
    let mut flips = 0;
    for byte in 0..RECORD_LEN {
        for bit in 0..8u8 {
            let mut rotted = record.to_vec();
            rotted[byte] ^= 1 << bit;
            assert_eq!(
                decode_record(&rotted),
                Decoded::Valid {
                    version: STORE_FORMAT,
                    whole: false
                },
                "byte {byte} bit {bit}"
            );
            flips += 1;
        }
    }
    assert_eq!(flips, RECORD_LEN * 8);
    // Every pair of flips with one in each copy: unreadable, never a version.
    let mut pairs = 0;
    for first in 0..COPY_LEN * 8 {
        for second in COPY_LEN * 8..RECORD_LEN * 8 {
            let mut rotted = record.to_vec();
            rotted[first / 8] ^= 1 << (first % 8);
            rotted[second / 8] ^= 1 << (second % 8);
            assert_eq!(
                decode_record(&rotted),
                Decoded::Unreadable,
                "bits {first} and {second}"
            );
            pairs += 1;
        }
    }
    assert_eq!(pairs, (COPY_LEN * 8) * (COPY_LEN * 8));
    // Every truncation: unreadable, or the surviving first copy.
    for len in 0..RECORD_LEN {
        let decoded = decode_record(&record[..len]);
        assert!(
            decoded == Decoded::Unreadable
                || decoded
                    == Decoded::Valid {
                        version: STORE_FORMAT,
                        whole: false
                    },
            "truncated to {len}: {decoded:?}"
        );
    }
    // Two valid copies naming different versions: refused, naming the one that
    // is not this build's.
    let mut mixed = encode_record(STORE_FORMAT).to_vec();
    mixed[COPY_LEN..].copy_from_slice(&encode_record(STORE_FORMAT + 1)[..COPY_LEN]);
    assert_eq!(
        decode_record(&mixed),
        Decoded::Conflicting {
            versions: [STORE_FORMAT, STORE_FORMAT + 1]
        }
    );

    // The pair: a decoder with no checksum reads some flip as another version.
    let misread: Vec<u64> = (0..RECORD_LEN * 8)
        .filter_map(|bit| {
            let mut rotted = record.to_vec();
            rotted[bit / 8] ^= 1 << (bit % 8);
            version_without_checksum(&rotted).filter(|v| *v != STORE_FORMAT)
        })
        .collect();
    println!(
        "{} flips of {} read as another version without the checksum: {:?}",
        misread.len(),
        RECORD_LEN * 8,
        &misread[..misread.len().min(8)]
    );
    assert!(
        !misread.is_empty(),
        "a decoder without the checksum was not caught"
    );
}

// --- E7, E8, E9: what is recorded, what is refused ---

/// A store this build writes records format 2 before anything else of its
/// directory, keeps the record through its life, and opens again on it. The
/// bytes are pinned: the record's encoding is permanent, so a change to it must
/// be a deliberate one (PROPOSED D-060).
#[test]
fn a_store_this_build_writes_records_format_2_and_opens_again() {
    let mut sim = Sim::new(SimConfig::new(60));
    let node = sim.add_node();
    let (record, names) = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let store = build_store(&env, DIR).await;
            drop(store);
            let record = get_record(&env, DIR).await.expect("the record");
            let names: Vec<String> = env
                .fs()
                .read_dir(Path::new(DIR))
                .await
                .unwrap()
                .iter()
                .map(|n| n.display().to_string())
                .collect();
            (record, names)
        })
    });
    assert_eq!(record, encode_record(STORE_FORMAT));
    assert_eq!(record.len(), RECORD_LEN);
    assert_eq!(
        u64::from_le_bytes(record[MAGIC.len()..MAGIC.len() + 8].try_into().unwrap()),
        STORE_FORMAT,
        "the version, little-endian after the magic"
    );
    println!("the record: {}", hex(&record));
    assert_eq!(
        hex(&record),
        "616e616e6b6520726166742073746f726520666f726d61740a0200000000000000425ea8f2\
         616e616e6b6520726166742073746f726520666f726d61740a0200000000000000425ea8f2",
        "the record's bytes are permanent"
    );
    assert!(
        names.contains(&FORMAT_FILE.to_owned()) && names.contains(&STORE_MARKER.to_owned()),
        "{names:?}"
    );
    // It opens again, on the record it wrote.
    let reopened = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, recovered) = RaftStore::open_dir(env.clone(), engine_config(DIR), prefix())
                .await
                .unwrap();
            let record = get_record(&env, DIR).await.expect("the record");
            (
                store.applied(),
                store.last_index(),
                recovered.log.len(),
                record,
            )
        })
    });
    assert_eq!(
        (reopened.0, reopened.1, reopened.2),
        (2, 4, 4),
        "the state came back"
    );
    assert_eq!(reopened.3, record, "the open rewrote the record");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A store recording a format this build does not read — older or newer — is
/// refused with an error naming both, is not a loss, and is left byte for byte
/// as it was: no log segment, no marker, no record of its own (D-059).
#[test]
fn a_store_recording_format_1_or_3_is_refused_naming_both_and_left_untouched() {
    for found in [UNRECORDED_FORMAT, STORE_FORMAT + 1] {
        let mut sim = Sim::new(SimConfig::new(61 + found));
        let node = sim.add_node();
        let (words, before, after) = on_node(&mut sim, node, |env| {
            Box::pin(async move {
                let store = build_store(&env, DIR).await;
                drop(store);
                put_record(&env, DIR, encode_record(found)).await;
                let before = read_tree(&env, DIR).await;
                let started = start(&env, DIR, StartOrder::Correct).await;
                let (word, message) = outcome(&started);
                assert_eq!(word, "failed", "{message}");
                let refusal = match started {
                    Start::Failed(error) => {
                        assert_eq!(
                            FormatRefused::from_io(&error),
                            Some(FormatRefused {
                                dir: PathBuf::from(DIR),
                                subject: Subject::Store,
                                found: Found::Recorded(found),
                                expected: STORE_FORMAT,
                            }),
                            "{error}"
                        );
                        assert!(LostState::from_io(&error).is_none(), "{error}");
                        error.to_string()
                    }
                    _ => unreachable!(),
                };
                let after = read_tree(&env, DIR).await;
                (refusal, before, after)
            })
        });
        println!("format {found}: {words}");
        assert!(
            words.contains(&format!("format {found}"))
                && words.contains(&format!("format {STORE_FORMAT}")),
            "{words}"
        );
        if found > STORE_FORMAT {
            assert!(words.contains("newer than format"), "{words}");
        }
        assert_eq!(after, before, "the refused store was written to");
    }

    // The record alone in an otherwise empty directory — a build of another
    // format whose first start crashed after its record — is refused too, and
    // never overwritten.
    let mut sim = Sim::new(SimConfig::new(63));
    let node = sim.add_node();
    let after = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            env.fs().create_dir_all(Path::new(DIR)).await.unwrap();
            put_record(&env, DIR, encode_record(STORE_FORMAT + 1)).await;
            let started = start(&env, DIR, StartOrder::Correct).await;
            assert_eq!(outcome(&started).0, "failed", "{}", outcome(&started).1);
            read_tree(&env, DIR).await
        })
    });
    assert_eq!(
        after,
        vec![(FORMAT_FILE.to_owned(), encode_record(STORE_FORMAT + 1))],
        "the newer record was overwritten or something was added"
    );
}

/// A directory with no record is told apart by what else it holds (PROPOSED
/// D-060): nothing at all, or nothing but a torn record or a leftover temporary
/// name, is fresh and gets its record; a store's files without a record are
/// 0.3.0's, format 1; anything else is refused as foreign rather than started as
/// a new store. None of it writes anything.
#[test]
fn a_directory_without_a_record_is_refused_or_fresh_by_what_it_holds() {
    let mut sim = Sim::new(SimConfig::new(64));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let fs = env.fs();
            // A store of this build's with its record removed: format 1's shape.
            let store = build_store(&env, DIR).await;
            drop(store);
            fs.remove_file(&format_path(Path::new(DIR))).await.unwrap();
            fs.sync_dir(Path::new(DIR)).await.unwrap();
            let before = read_tree(&env, DIR).await;
            let refused = check_format(&env, Path::new(DIR))
                .await
                .expect_err("a store with no record is 0.3.0's");
            assert_eq!(
                FormatRefused::from_io(&refused).map(|r| r.found),
                Some(Found::Unrecorded),
                "{refused}"
            );
            assert_eq!(read_tree(&env, DIR).await, before, "the gate wrote");

            // Fresh: no directory, an empty one, one with only the temporary
            // name, one with only a record that cannot be read.
            for (dir, make) in [
                ("/absent", 0),
                ("/empty", 1),
                ("/tmp-only", 2),
                ("/torn-record", 3),
            ] {
                if make > 0 {
                    fs.create_dir_all(Path::new(dir)).await.unwrap();
                }
                if make == 2 {
                    let file = fs
                        .open(
                            &Path::new(dir).join(FORMAT_TMP),
                            OpenOptions::new().write(true).create(true),
                        )
                        .await
                        .unwrap();
                    file.write_at(0, Bytes::from_static(b"half a record"))
                        .await
                        .unwrap();
                    file.sync().await.unwrap();
                    fs.sync_dir(Path::new(dir)).await.unwrap();
                }
                if make == 3 {
                    put_record(&env, dir, Bytes::from_static(b"not a record at all")).await;
                }
                let before = read_tree(&env, dir).await;
                let verdict = check_format(&env, Path::new(dir)).await.unwrap();
                assert!(
                    matches!(verdict, Verdict::Fresh(_)),
                    "{dir}: {verdict:?} rather than fresh"
                );
                assert_eq!(read_tree(&env, dir).await, before, "{dir}: the gate wrote");
            }

            // Foreign: entries nobody here wrote, and no store file among them.
            for (dir, name) in [("/found", "lost+found"), ("/other", "x")] {
                fs.create_dir_all(Path::new(dir)).await.unwrap();
                let file = fs
                    .open(
                        &Path::new(dir).join(name),
                        OpenOptions::new().write(true).create(true),
                    )
                    .await
                    .unwrap();
                file.sync().await.unwrap();
                let refused = check_format(&env, Path::new(dir))
                    .await
                    .expect_err("a directory that was never a store is refused");
                assert_eq!(
                    FormatRefused::from_io(&refused).map(|r| r.found),
                    Some(Found::Foreign(vec![name.to_owned()])),
                    "{refused}"
                );
                assert!(
                    refused.to_string().contains(name)
                        && refused.to_string().contains("empty directory"),
                    "{refused}"
                );
            }

            // Unrecorded: any one name of the store family, with no record.
            // Every arm of `is_store_family`, so dropping one — which would
            // read an operator's only copy of a 0.3.0 store as a foreign
            // directory and tell them to point the store somewhere empty — is
            // caught here.
            for (dir, name, is_dir) in [
                ("/a", STORE_MARKER, false),
                ("/b", "CURRENT.tmp", false),
                ("/c", "MANIFEST-000001", false),
                ("/d", "install", true),
                ("/e", "snap-3-1", true),
                ("/h", "CURRENT", false),
                ("/i", "000002.sst", false),
                ("/j", "000001.wal", false),
            ] {
                fs.create_dir_all(Path::new(dir)).await.unwrap();
                if is_dir {
                    fs.create_dir_all(&Path::new(dir).join(name)).await.unwrap();
                } else {
                    let file = fs
                        .open(
                            &Path::new(dir).join(name),
                            OpenOptions::new().write(true).create(true),
                        )
                        .await
                        .unwrap();
                    file.sync().await.unwrap();
                }
                let before = read_tree(&env, dir).await;
                let refused = check_format(&env, Path::new(dir))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    refused.contains("0.3.0") && refused.contains("format 1"),
                    "{dir} ({name}): {refused}"
                );
                assert_eq!(read_tree(&env, dir).await, before, "{dir}: the gate wrote");
            }
        })
    });
}

// --- E10: the fresh store's first write ---

/// How one crash inside a fresh store's first start came out.
#[derive(Debug, Default)]
struct Outcomes {
    /// The next start opened a store with nothing in it.
    fresh: usize,
    /// The next start opened the store the first one had written.
    with_state: usize,
    /// The next start refused it as lost state the engine's own rule accounts
    /// for: a crash between the engine's first manifest and its `CURRENT`
    /// (D-024), which costs a re-seed and is unchanged by the record.
    lost_engine: usize,
    /// The next start refused it as lost state the *record* caused: the record
    /// there and unreadable beside a store (`Damage::FormatUnreadable`). This is
    /// the re-seed the record's own cost would be, counted apart from the
    /// engine's so D-060 can quote it rather than assert a sum (D-060).
    lost_format: usize,
    /// The next start refused it for its format: a store this build wrote that
    /// it will not read.
    format: Vec<(u64, String)>,
    /// Seeds whose durable directory broke the invariant: an entry other than
    /// the record or its temporary name, with no record.
    broke_i1: Vec<u64>,
    /// How many seeds landed in each crash window (W0 to W3).
    windows: [usize; 4],
}

/// Where seed `seed`'s crash lands: spread across the whole start's span, one
/// step per seed with a seed-drawn offset inside it, so the crashes sweep every
/// step of the start rather than crowding its beginning.
fn crash_at(seed: u64, seeds: u64, span: Duration) -> Duration {
    let step = (u64::try_from(span.as_nanos()).expect("a short span") / seeds).max(1);
    Duration::from_nanos(seed * step + seed * 7_919 % step)
}

/// The simulated time a fresh store's whole first start takes, from nothing to
/// the marker, on a disk with the same latencies and no crash: what the crash
/// instants below must cover for the sweep to reach every step, S1 to S7.
fn first_start_span() -> Duration {
    let mut sim = Sim::new({
        let mut c = SimConfig::new(999);
        c.fs.latency_min = Duration::from_micros(10);
        c.fs.latency_max = Duration::from_micros(150);
        c
    });
    let node = sim.add_node();
    let marked = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let started = start(&env, DIR, StartOrder::Correct).await;
            assert!(matches!(started, Start::Opened { .. }), "the start opened");
            mark_store(&env, Path::new(DIR)).await.unwrap();
            env.fs()
                .read_dir(Path::new(DIR))
                .await
                .unwrap()
                .iter()
                .any(|n| n.to_str() == Some(STORE_MARKER))
        })
    });
    assert!(marked, "the measured start reached the marker (S7)");
    Duration::from_nanos(sim.now().as_nanos())
}

/// Runs a fresh store's first start on seed `seed`'s disk with slow I/O, crashes
/// at a seed-drawn instant inside it, and classifies what the next start makes of
/// what is durable. `order` is the correct start or the known-buggy one that
/// records the format after the engine's open and the store's first batch.
fn crash_in_first_start(
    seed: u64,
    p_durable: f64,
    order: StartOrder,
    at: Duration,
    outcomes: &mut Outcomes,
) {
    let mut sim = Sim::new({
        let mut c = SimConfig::new(seed);
        c.fs.p_durable = p_durable;
        c.fs.p_bitrot = 0.0;
        c.fs.latency_min = Duration::from_micros(10);
        c.fs.latency_max = Duration::from_micros(150);
        c
    });
    let node = sim.add_node();
    let env = sim.env(node);
    env.clone().spawn("first start", async move {
        if let Start::Opened { .. } = start(&env, DIR, order).await {
            mark_store(&env, Path::new(DIR)).await.unwrap();
        }
    });
    sim.run_for(at);
    sim.crash(node);
    sim.restart(node);
    // What the crash left, before anything of the next start runs: the window,
    // and invariant I1 — nothing but the record and its temporary name can be
    // durable without the record.
    let names: Vec<String> = sim
        .durable_names(node, Path::new(DIR))
        .iter()
        .map(|n| n.display().to_string())
        .collect();
    let has_record = names.iter().any(|n| n == FORMAT_FILE);
    let others: Vec<&String> = names
        .iter()
        .filter(|n| *n != FORMAT_FILE && *n != FORMAT_TMP)
        .collect();
    if !others.is_empty() && !has_record {
        outcomes.broke_i1.push(seed);
    }
    let window = match (has_record, others.is_empty()) {
        (false, true) if names.is_empty() => 0,
        (false, true) => 1,
        (true, true) => 2,
        _ => 3,
    };
    outcomes.windows[window] += 1;
    // A refusal carries which damage refused it, so the record's own re-seeds
    // are counted apart from the engine's (D-060).
    let started = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            match start(&env, DIR, StartOrder::Correct).await {
                Start::Opened { .. } => Ok(()),
                Start::Refused(error) => Err((
                    true,
                    LostState::from_io(&error).and_then(|lost| lost.damaged)
                        == Some(Damage::FormatUnreadable),
                    error.to_string(),
                )),
                Start::Failed(error) => Err((
                    FormatRefused::from_io(&error).is_none(),
                    false,
                    error.to_string(),
                )),
            }
        })
    });
    match started {
        Ok(()) if window >= 3 => outcomes.with_state += 1,
        Ok(()) => outcomes.fresh += 1,
        Err((true, true, _)) => outcomes.lost_format += 1,
        Err((true, false, _)) => outcomes.lost_engine += 1,
        Err((false, _, message)) => outcomes.format.push((seed, message)),
    }
}

/// A crash anywhere inside a fresh store's first start never leaves a store this
/// build wrote that it then refuses for its format: the record is written,
/// renamed and its directory synced before the engine creates a single entry, so
/// every durable directory holding anything else holds the record too
/// (invariant I1). What a crash can leave is what it could leave before this
/// commit: nothing, or a store that lost state.
///
/// The pair is `FormatAfterFirstBatch`, the order that records the format after
/// the engine's open and the store's first batch: the same crashes leave a store
/// with engine files and no record, which the next correct start refuses as
/// 0.3.0's, and the invariant is broken at the crash itself.
///
/// Why 200 seeds and not 40. The crash instants are a stratified sweep of the
/// start's whole span (`crash_at`), so how many land in a window is that
/// window's share of the span. W1 is the width of one rename — the temporary
/// name durable and the record not yet — and at 40 seeds it held exactly one
/// seed on both disks, one instant from empty: any later change that rescales
/// `first_start_span` (an operation added to or taken out of the start, which is
/// what D-060 did) could empty it and fail this test on a tree with nothing
/// wrong. At 200 the same window holds several. The figures are printed at every
/// tier and listed in D-061's table of fixed seed sets.
#[test]
fn a_crash_in_a_fresh_stores_first_open_never_leaves_it_refused_for_its_format() {
    let seeds = 200;
    let span = first_start_span();
    println!("a fresh store's whole first start takes {span:?}");
    for p_durable in [1.0, 0.7] {
        let mut correct = Outcomes::default();
        for seed in 0..seeds {
            crash_in_first_start(
                seed,
                p_durable,
                StartOrder::Correct,
                crash_at(seed, seeds, span),
                &mut correct,
            );
        }
        println!("the correct start at p_durable {p_durable} over {seeds} seeds: {correct:?}");
        assert_eq!(
            correct.format,
            [],
            "a store this build wrote was refused for its format"
        );
        assert_eq!(
            correct.broke_i1,
            Vec::<u64>::new(),
            "a durable directory held a store file and no record"
        );
        if p_durable == 1.0 {
            // Every crash window the start passes through, W0 to W3: nothing
            // durable, the temporary name alone, the record alone, and the
            // record with the engine's files.
            for window in 0..4 {
                assert!(
                    correct.windows[window] > 0,
                    "no seed crashed in window W{window}: {correct:?}"
                );
            }
            // The lost-state outcomes at a durable disk are the engine's own
            // rule, unchanged by this commit: a crash between the engine's first
            // manifest and its CURRENT leaves a directory the engine refuses
            // (D-024), which is a re-seed, never a format refusal. The record
            // adds none of its own on a disk that keeps its syncs, which is what
            // this asserts — the sum of the three was a tautology, since every
            // seed lands in exactly one bucket and `format` is asserted empty
            // above (D-060).
            assert_eq!(
                correct.lost_format, 0,
                "a crash in a fresh store's first start left its record unreadable beside a \
                 store on a disk that loses no sync, so the record cost a re-seed the engine's \
                 own rule does not account for: {correct:?}"
            );
        }
    }
    // The pair, on the same seeds and the same instants: the record written
    // after the engine's open and the store's first batch.
    let mut buggy = Outcomes::default();
    for seed in 0..seeds {
        crash_in_first_start(
            seed,
            1.0,
            StartOrder::FormatAfterFirstBatch,
            crash_at(seed, seeds, span),
            &mut buggy,
        );
    }
    println!(
        "the record written after the first batch over {seeds} seeds: {} caught of {seeds}, \
         windows {:?}, first: {}",
        buggy.format.len(),
        buggy.windows,
        buggy.format.first().map_or("", |(_, message)| message)
    );
    assert!(
        !buggy.format.is_empty(),
        "the order that records the format last was not caught: {buggy:?}"
    );
    assert!(
        !buggy.broke_i1.is_empty(),
        "the order that records the format last kept the invariant: {buggy:?}"
    );
}

/// The record's rename is durable before anything else of the store is written.
///
/// The crash sweep above cannot see this. The simulated disk keeps a prefix of a
/// directory's pending operations at a crash, and the record's create and rename
/// are the first two of that directory's, so invariant I1 follows from the order
/// alone and holds with `record_format`'s directory sync removed. On a disk that
/// reorders, it does not: the rename could be the operation that is lost, the
/// engine's files could be the ones that survive, and the next start would refuse
/// this build's own store as 0.3.0's with no re-seed path (D-059). The durable
/// namespace is the oracle for that sync, and this is the test that reads it.
// PROPOSED(D-060)
#[test]
fn a_fresh_stores_record_is_made_durable_before_anything_else_is_written() {
    let mut sim = Sim::new(SimConfig::new(11));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let fresh = match check_format(&env, Path::new(DIR)).await.unwrap() {
                Verdict::Fresh(fresh) => fresh,
                other => panic!("a fresh directory, not {other:?}"),
            };
            record_format(&env, fresh).await.unwrap();
        })
    });
    let durable: Vec<String> = sim
        .durable_names(node, Path::new(DIR))
        .iter()
        .map(|name| name.display().to_string())
        .collect();
    assert!(
        durable.iter().any(|name| name == FORMAT_FILE),
        "the record's rename was never made durable: {durable:?}"
    );
}

// --- E11: the heal ---

/// A record with one damaged copy is healed at the next start, in place and with
/// byte-identical content, so the copy that is still valid cannot be damaged by
/// the heal's own write: every one of the 592 single-bit flips opens the store
/// and leaves the record whole, and a crash swept across the heal at a disk that
/// loses syncs never leaves the record unreadable.
///
/// The pair is `HealByRename`: the same crashes, with the heal done by writing a
/// temporary file and renaming it over the record, do leave the record
/// unreadable — the renamed inode is made durable with its content lost — and
/// the store that follows is refused as lost. That the pair is caught at all is
/// what is asserted; its rate is printed beside it and not asserted, because a
/// fixed seed set runs the same seeds at every tier and so has no tier to move
/// an assertion to (D-061). The set is 160 seeds rather than 40 so the catch has
/// margin against the next tree that redraws these schedules.
#[test]
fn a_record_with_one_bad_copy_is_healed_in_place_and_a_crash_never_loses_the_other_copy() {
    // (a) Every single flip: the start opens and the record comes back whole.
    let mut sim = Sim::new(SimConfig::new(70));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let store = build_store(&env, DIR).await;
            drop(store);
            let whole = encode_record(STORE_FORMAT);
            for bit in 0..RECORD_LEN * 8 {
                let mut rotted = whole.to_vec();
                rotted[bit / 8] ^= 1 << (bit % 8);
                put_record(&env, DIR, Bytes::from(rotted)).await;
                let started = start(&env, DIR, StartOrder::Correct).await;
                let (word, message) = outcome(&started);
                assert_eq!(word, "opened", "bit {bit}: {message}");
                assert_eq!(
                    get_record(&env, DIR).await.as_deref(),
                    Some(&whole[..]),
                    "bit {bit}: the heal did not make the record whole"
                );
            }
            // And a record longer than the encoding — bytes appended past the
            // two copies, the length `Verdict::Recorded` carries — is cut back
            // to the encoding by the heal, so a record can never grow without
            // bound over repeated heals.
            let mut long = whole.to_vec();
            long[COPY_LEN + 3] ^= 0x10;
            long.extend_from_slice(b"appended past the second copy");
            put_record(&env, DIR, Bytes::from(long)).await;
            let started = start(&env, DIR, StartOrder::Correct).await;
            let (word, message) = outcome(&started);
            assert_eq!(word, "opened", "{message}");
            assert_eq!(
                get_record(&env, DIR).await.as_deref(),
                Some(&whole[..]),
                "the heal left the record longer than the encoding"
            );
        })
    });

    // (b) Crashes across the heal, on a disk that loses syncs: once spread over
    // the whole start, so they land on both sides of the heal, and once in the
    // heal's own window, where the pair's rename can lose the surviving copy.
    let seeds = 160;
    for targeted in [false, true] {
        for order in [StartOrder::Correct, StartOrder::HealByRename] {
            let (healed, half, unreadable) = heal_under_crash(order, targeted, seeds);
            println!(
                "{order:?}, {} crashes: {healed} healed, {half} still half, {} unreadable \
                 over {seeds} seeds",
                if targeted { "targeted" } else { "spread" },
                unreadable.len()
            );
            match (order, targeted) {
                (StartOrder::Correct, _) => {
                    assert!(
                        unreadable.is_empty(),
                        "the in-place heal lost the surviving copy: {unreadable:?}"
                    );
                    if !targeted {
                        // Spread over the start, the crashes land on both sides
                        // of the heal, which is what "never unreadable" counts.
                        assert!(
                            healed > 0 && half > 0,
                            "the crashes did not land on both sides of the heal"
                        );
                    }
                }
                (_, false) => {
                    assert!(
                        healed > 0 && half > 0,
                        "the crashes did not land on both sides of the heal"
                    );
                }
                (_, true) => {
                    // The pair: the renamed inode is made durable with its
                    // content still owed, and the copy that was valid goes with
                    // it. What is asserted is that it was caught; the rate is
                    // printed above. A floor on the rate of a fixed seed set
                    // would fail on a tree with nothing wrong the next time
                    // these schedules are redrawn, and the owner's rule of
                    // 2026-09-15 moves an assertion to the tier its rate
                    // supports, which a fixed set has not got (D-061).
                    assert!(
                        !unreadable.is_empty(),
                        "the heal by rename never lost the surviving copy over {seeds} seeds: \
                         the pair's catch is gone, so the in-place heal above is asserted \
                         against nothing"
                    );
                }
            }
        }
    }
}

/// Runs a start on a store whose record has one damaged copy, crashes it, and
/// says what the record decoded as afterwards: (healed whole, still half, the
/// seeds that left it unreadable). With `targeted` the crash lands the moment
/// the record's durable bytes change — the heal's own window — and otherwise at
/// a seed-drawn instant across the whole start.
fn heal_under_crash(order: StartOrder, targeted: bool, seeds: u64) -> (usize, usize, Vec<u64>) {
    let (mut healed, mut half, mut unreadable) = (0, 0, Vec::new());
    for seed in 0..seeds {
        let mut sim = Sim::new({
            let mut c = SimConfig::new(700 + seed);
            c.fs.p_durable = 0.7;
            c.fs.p_bitrot = 0.0;
            c.fs.latency_min = Duration::from_micros(10);
            c.fs.latency_max = Duration::from_micros(150);
            c
        });
        let node = sim.add_node();
        // Copy 2 rotted: one bad copy, which the next start heals. The disk
        // loses syncs, so the setup is repeated until what it wrote is really
        // durable — the crash below must sweep the heal, not the setup.
        let mut rotted = encode_record(STORE_FORMAT).to_vec();
        rotted[COPY_LEN + 3] ^= 0x10;
        let mut ready = false;
        for _ in 0..50 {
            let bytes = Bytes::from(rotted.clone());
            on_node(&mut sim, node, |env| {
                Box::pin(async move {
                    if RaftStore::open_dir(env.clone(), engine_config(DIR), prefix())
                        .await
                        .is_ok()
                    {
                        mark_store(&env, Path::new(DIR)).await.unwrap();
                    }
                    put_record(&env, DIR, bytes).await;
                })
            });
            if sim.durable_contents(node, &format_path(Path::new(DIR))) == Some(rotted.clone())
                && sim
                    .durable_contents(node, &Path::new(DIR).join(STORE_MARKER))
                    .is_some()
            {
                ready = true;
                break;
            }
        }
        assert!(ready, "seed {seed}: the rotted record never became durable");
        let env = sim.env(node);
        env.clone().spawn("heal", async move {
            let _ = start(&env, DIR, order).await;
        });
        if targeted {
            let mut steps = 0;
            while steps < 400
                && sim.durable_contents(node, &format_path(Path::new(DIR))) == Some(rotted.clone())
            {
                sim.run_for(Duration::from_micros(10));
                steps += 1;
            }
        } else {
            sim.run_for(crash_at(seed, seeds, Duration::from_micros(1_500)));
        }
        sim.crash(node);
        sim.restart(node);
        let durable = sim
            .durable_contents(node, &format_path(Path::new(DIR)))
            .unwrap_or_default();
        match decode_record(&durable) {
            Decoded::Valid { whole: true, .. } => healed += 1,
            Decoded::Valid { whole: false, .. } => half += 1,
            Decoded::Conflicting { .. } | Decoded::Unreadable => unreadable.push(seed),
        }
    }
    (healed, half, unreadable)
}

// --- E12, E14, E15: the record under the install, the engine and the sweeps ---

/// Builds a leader-like store at `/leader` with a checkpoint at index 3, and
/// returns the store and the checkpoint's directory.
async fn build_leader(env: &SimEnv) -> (Arc<RaftStore<SimEnv>>, PathBuf) {
    let (store, _) = RaftStore::open_dir(env.clone(), engine_config("/leader"), prefix())
        .await
        .unwrap();
    let store = Arc::new(store);
    store
        .persist(&Persist {
            term: 2,
            vote: Some(ServerId(1)),
            truncate_from: None,
            append: (1..=3u64).map(|i| entry(2, i, &format!("l{i}"))).collect(),
            config: None,
            compact_to: None,
        })
        .await
        .unwrap();
    for index in 1..=3u64 {
        apply_command(
            &store,
            index,
            Some(&Command::Put {
                key: Bytes::from(format!("k{index}")),
                value: Bytes::from(format!("v{index}")),
            }),
        )
        .await
        .unwrap();
    }
    let dir = take_version(
        env,
        &store,
        Path::new("/leader"),
        3,
        2,
        &Configuration::of(&[ServerId(1), ServerId(2), ServerId(3)]),
    )
    .await
    .unwrap();
    (store, dir)
}

/// Streams the checkpoint in `dir` into `assembler` and returns the stage.
async fn stream(env: &SimEnv, dir: &Path, assembler: &mut Assembler<SimEnv>) -> io_result::Staged {
    let mut sender = Sender::open(env, dir, ServerId(2), 3, 2, 4).await.unwrap();
    loop {
        let chunk = sender.chunk(env, 64).await.unwrap();
        let ananke_raft::message::Message::InstallSnapshot {
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
        match assembler
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
            .unwrap()
        {
            Feed::Ack { file, offset } => {
                sender.on_more(&file, offset);
            }
            Feed::Restart => panic!("the stream restarted"),
            Feed::Staged(staged) => return staged,
        }
    }
}

/// The type the stream returns, named so the helper's signature reads.
mod io_result {
    pub use ananke_raft::snapshot::Staged;
}

/// A record neither copy of which can be read, beside a store, is lost state and
/// never a format refusal (PROPOSED D-060, question 1): the server marks the
/// store lost, asks to be re-seeded, and the adoption of the install that
/// follows rewrites the record whole before the store opens on it.
///
/// The pair is `UnreadableIsUnrecorded`, which reads a record that is there and
/// cannot be read as no record at all: it stops the server on a store that only
/// lost state, and nothing ever re-seeds it.
#[test]
fn an_unreadable_record_beside_a_store_is_lost_state_and_the_adoption_rewrites_it() {
    let mut sim = Sim::new(SimConfig::new(80));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            let store = build_store(&env, DIR).await;
            drop(store);
            // Two rots, one in each copy: the record is there and says nothing.
            let mut rotted = encode_record(STORE_FORMAT).to_vec();
            rotted[3] ^= 0x08;
            rotted[COPY_LEN + 3] ^= 0x08;
            put_record(&env, DIR, Bytes::from(rotted.clone())).await;
            assert_eq!(decode_record(&rotted), Decoded::Unreadable);

            // The correct start: lost state, naming the record.
            let started = start(&env, DIR, StartOrder::Correct).await;
            let (word, message) = outcome(&started);
            assert_eq!(word, "refused", "{message}");
            let Start::Refused(error) = started else {
                unreachable!()
            };
            assert_eq!(
                LostState::from_io(&error).and_then(|l| l.damaged),
                Some(Damage::FormatUnreadable),
                "{error}"
            );
            assert!(
                error.to_string().contains(FORMAT_FILE) && FormatRefused::from_io(&error).is_none(),
                "{error}"
            );
            // The pair: the same store, read as a store with no record at all.
            let started = start(&env, DIR, StartOrder::UnreadableIsUnrecorded).await;
            let (word, message) = outcome(&started);
            assert_eq!(
                word, "failed",
                "the order that reads an unreadable record as none was not caught"
            );
            assert!(message.contains("0.3.0"), "{message}");

            // The re-seed: the leader's snapshot, installed and adopted. The
            // adoption rewrites the record for the store that is in force now.
            let mut assembler =
                Assembler::new(env.clone(), Path::new(DIR), Variant::Correct, prefix());
            let staged = stream(&env, &dir, &mut assembler).await;
            assembler
                .finish(
                    &staged,
                    &Repair {
                        term: staged.term,
                        vote: None,
                        tail: Vec::new(),
                        quarantined: true,
                        incarnation: 77,
                    },
                )
                .await
                .unwrap();
            let started = start(&env, DIR, StartOrder::Correct).await;
            let (word, message) = outcome(&started);
            assert_eq!(word, "opened", "{message}");
            let Start::Opened {
                store,
                recovered,
                adopted,
            } = started
            else {
                unreachable!()
            };
            assert!(adopted, "the install was adopted");
            assert!(recovered.quarantined, "the re-seeded store is quarantined");
            assert_eq!(store.incarnation(), 77);
            assert_eq!(
                get_record(&env, DIR).await,
                Some(encode_record(STORE_FORMAT)),
                "the adoption rewrote the record whole"
            );
        })
    });
    // And the same adoption swept by crashes: whatever a crash leaves, the next
    // start is never a format refusal — it re-runs the adoption or opens the
    // store — and the record is whole once the store opens.
    let seeds = 24;
    let span = Duration::from_millis(20);
    let mut ends = std::collections::BTreeSet::new();
    for seed in 0..seeds {
        let mut sim = Sim::new({
            let mut c = SimConfig::new(800 + seed);
            c.fs.latency_min = Duration::from_micros(50);
            c.fs.latency_max = Duration::from_micros(400);
            c
        });
        let node = sim.add_node();
        on_node(&mut sim, node, |env| {
            Box::pin(async move {
                let (_leader, dir) = build_leader(&env).await;
                let store = build_store(&env, DIR).await;
                drop(store);
                let mut rotted = encode_record(STORE_FORMAT).to_vec();
                rotted[3] ^= 0x08;
                rotted[COPY_LEN + 3] ^= 0x08;
                put_record(&env, DIR, Bytes::from(rotted)).await;
                let mut assembler =
                    Assembler::new(env.clone(), Path::new(DIR), Variant::Correct, prefix());
                let staged = stream(&env, &dir, &mut assembler).await;
                assembler
                    .finish(
                        &staged,
                        &Repair {
                            term: staged.term,
                            vote: None,
                            tail: Vec::new(),
                            quarantined: true,
                            incarnation: 77,
                        },
                    )
                    .await
                    .unwrap();
            })
        });
        let env = sim.env(node);
        env.clone().spawn("adopt", async move {
            let _ = start(&env, DIR, StartOrder::Correct).await;
        });
        sim.run_for(crash_at(seed, seeds, span));
        sim.crash(node);
        sim.restart(node);
        // Which manifest is in force: the old store's, or the adoption's, which
        // is numbered past it.
        let current = sim
            .durable_contents(node, &manifest::current_path(Path::new(DIR)))
            .and_then(|bytes| manifest::parse_current(&bytes));
        let record = sim
            .durable_contents(node, &format_path(Path::new(DIR)))
            .map(|bytes| decode_record(&bytes));
        ends.insert((
            current,
            matches!(
                record,
                Some(Decoded::Valid {
                    version: STORE_FORMAT,
                    whole: true
                })
            ),
        ));
        // Starts until the store opens: each one adopts again or opens, and
        // none of them is ever a format refusal.
        let mut opened = false;
        for attempt in 0..4 {
            let verdict = on_node(&mut sim, node, |env| {
                Box::pin(async move {
                    let started = start(&env, DIR, StartOrder::Correct).await;
                    let record = get_record(&env, DIR).await;
                    (outcome(&started).0, outcome(&started).1, record)
                })
            });
            assert_ne!(
                verdict.0, "failed",
                "seed {seed}, attempt {attempt}: the adoption's crash left a refusal: {}",
                verdict.1
            );
            if verdict.0 == "opened" {
                assert_eq!(
                    verdict.2,
                    Some(encode_record(STORE_FORMAT)),
                    "seed {seed}: the store opened on a record that is not whole"
                );
                opened = true;
                break;
            }
        }
        assert!(opened, "seed {seed}: the store never opened again");
    }
    println!(
        "crashes across the adoption of a damaged record over {seeds} seeds: \
         (adopted CURRENT, record whole) {ends:?}"
    );
    assert!(
        ends.len() > 1,
        "the crashes all landed at the same point: {ends:?}"
    );
}

/// The record survives everything else that touches the store directory
/// (PROPOSED D-060): the engine's open and its orphan sweep, both adoptions, the
/// version sweep, the assembler's own sweep, a take, and the marker's writes.
/// This is what "nothing removes `RAFT-FORMAT`" is held to.
#[test]
fn the_format_record_survives_the_engine_the_adoptions_and_the_sweeps() {
    use ananke_raft::snapshot::{staging_dir as staging, sweep_versions};

    let mut sim = Sim::new(SimConfig::new(81));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (leader, _first) = build_leader(&env).await;
            let record = encode_record(STORE_FORMAT);
            let fs = env.fs();
            // The engine's open, with an orphan and a leftover CURRENT.tmp.
            let store = build_store(&env, DIR).await;
            drop(store);
            let orphan = manifest::sst_path(Path::new(DIR), 900);
            let file = fs
                .open(&orphan, OpenOptions::new().write(true).create(true))
                .await
                .unwrap();
            file.write_at(0, Bytes::from_static(b"orphan"))
                .await
                .unwrap();
            file.sync().await.unwrap();
            let tmp = fs
                .open(
                    &manifest::current_tmp_path(Path::new(DIR)),
                    OpenOptions::new().write(true).create(true),
                )
                .await
                .unwrap();
            tmp.sync().await.unwrap();
            fs.sync_dir(Path::new(DIR)).await.unwrap();
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config(DIR), prefix())
                .await
                .unwrap();
            assert_eq!(get_record(&env, DIR).await, Some(record.clone()), "engine");
            drop(store);

            // A take and the version sweep, on the leader.
            let second = take_version(
                &env,
                &leader,
                Path::new("/leader"),
                3,
                2,
                &Configuration::of(&[ServerId(1)]),
            )
            .await
            .unwrap();
            assert_eq!(
                get_record(&env, second.to_str().unwrap()).await,
                Some(record.clone()),
                "the second take carries its own record"
            );
            let swept = sweep_versions(
                &env,
                &leader,
                Path::new("/leader"),
                &std::collections::BTreeMap::new(),
            )
            .await
            .unwrap();
            assert_eq!(swept, vec![(3, 1)], "the older version went");
            assert_eq!(
                get_record(&env, "/leader").await,
                Some(record.clone()),
                "the version sweep"
            );
            // The stream from here on is of the version the record names, the
            // older one having been swept.
            let dir = second;

            // Both adoptions, and the assembler's own sweep between them.
            for variant in [Variant::Correct, Variant::AdoptionAsBuilt] {
                let mut assembler = Assembler::new(env.clone(), Path::new(DIR), variant, prefix());
                let _staged = stream(&env, &dir, &mut assembler).await;
                assembler.abandon().await;
                assert_eq!(
                    get_record(&env, DIR).await,
                    Some(record.clone()),
                    "{variant:?}: the assembler's sweep"
                );
                assert!(
                    fs.read_dir(&staging(Path::new(DIR)))
                        .await
                        .unwrap()
                        .iter()
                        .all(|n| n.to_str() != Some(FORMAT_FILE)),
                    "the staged record is swept with the rest of the staging"
                );
                let mut assembler = Assembler::new(env.clone(), Path::new(DIR), variant, prefix());
                let staged = stream(&env, &dir, &mut assembler).await;
                assembler
                    .finish(
                        &staged,
                        &Repair {
                            term: 9,
                            vote: None,
                            tail: Vec::new(),
                            quarantined: false,
                            incarnation: 5,
                        },
                    )
                    .await
                    .unwrap();
                assert!(
                    ananke_raft::snapshot::adopt_staged_under(&env, Path::new(DIR), variant)
                        .await
                        .unwrap()
                );
                assert_eq!(
                    get_record(&env, DIR).await,
                    Some(record.clone()),
                    "{variant:?}: the adoption kept the store's own record"
                );
            }
            // The marker's writes.
            mark_store(&env, Path::new(DIR)).await.unwrap();
            ananke_raft::store::mark_store_lost(&env, Path::new(DIR), "a reason")
                .await
                .unwrap();
            assert_eq!(get_record(&env, DIR).await, Some(record), "the marker");
        })
    });
}

/// A checkpoint carries its own record and a stream of another format is refused
/// unread (D-059, PROPOSED D-060): the take writes the record after the
/// checkpoint's `CURRENT`, a checkpoint without it is incomplete, the sender
/// streams it first so a staged install always has its version before its
/// `CURRENT`, and the assembler reads it before it opens a single table.
///
/// The pair is `StagedFormatUnchecked`, the adoption without its staged check: a
/// staging recording another format is adopted over the store.
#[test]
fn a_checkpoint_carries_its_record_and_a_stream_of_another_format_is_refused_unread() {
    let mut sim = Sim::new(SimConfig::new(82));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (_leader, dir) = build_leader(&env).await;
            let fs = env.fs();
            // The checkpoint carries its record, and is complete only with it.
            assert_eq!(
                get_record(&env, dir.to_str().unwrap()).await,
                Some(encode_record(STORE_FORMAT))
            );
            assert!(
                ananke_raft::snapshot::checkpoint_complete(&env, &dir)
                    .await
                    .unwrap()
            );
            let saved = get_record(&env, dir.to_str().unwrap()).await.unwrap();
            fs.remove_file(&format_path(&dir)).await.unwrap();
            fs.sync_dir(&dir).await.unwrap();
            assert!(
                !ananke_raft::snapshot::checkpoint_complete(&env, &dir)
                    .await
                    .unwrap(),
                "a checkpoint without its record is incomplete"
            );
            let mut rotted = saved.to_vec();
            rotted[3] ^= 0x08;
            rotted[COPY_LEN + 3] ^= 0x08;
            put_record(&env, dir.to_str().unwrap(), Bytes::from(rotted)).await;
            assert!(
                !ananke_raft::snapshot::checkpoint_complete(&env, &dir)
                    .await
                    .unwrap(),
                "a checkpoint whose record cannot be read is incomplete"
            );
            // And the third clause: a readable record naming another version is
            // not this build's checkpoint either, so it is never streamed.
            put_record(&env, dir.to_str().unwrap(), encode_record(STORE_FORMAT + 1)).await;
            assert!(
                !ananke_raft::snapshot::checkpoint_complete(&env, &dir)
                    .await
                    .unwrap(),
                "a checkpoint recording another format is complete"
            );
            put_record(&env, dir.to_str().unwrap(), saved).await;

            // The sender streams the record first and the manifest last.
            let sender = Sender::open(&env, &dir, ServerId(2), 3, 2, 4)
                .await
                .unwrap();
            let first = sender.chunk(&env, 4096).await.unwrap();
            let ananke_raft::message::Message::InstallSnapshot { file, .. } = first else {
                panic!("a chunk")
            };
            assert_eq!(&file[..], FORMAT_FILE.as_bytes(), "the record goes first");

            // The receiver: the staged record is there before the staged CURRENT
            // under the variant that writes CURRENT on arrival (A.6's reason for
            // the reorder).
            let mut assembler = Assembler::new(
                env.clone(),
                Path::new("/early"),
                Variant::SnapshotWithoutCurrentLast,
                prefix(),
            );
            let mut sender = Sender::open(&env, &dir, ServerId(2), 3, 2, 4)
                .await
                .unwrap();
            let staging = staging_dir(Path::new("/early"));
            loop {
                let chunk = sender.chunk(&env, 64).await.unwrap();
                let ananke_raft::message::Message::InstallSnapshot {
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
                    .await
                    .unwrap();
                let names = fs.read_dir(&staging).await.unwrap_or_default();
                if names.iter().any(|n| n.to_str() == Some("CURRENT")) {
                    assert!(
                        names.iter().any(|n| n.to_str() == Some(FORMAT_FILE)),
                        "the staged CURRENT arrived before the staged record"
                    );
                }
                match fed {
                    Feed::Ack { file, offset } => {
                        sender.on_more(&file, offset);
                    }
                    Feed::Restart => panic!("the stream restarted"),
                    Feed::Staged(_) => break,
                }
            }

            // A stream with no record, or one of another format, is refused
            // before any table is read: the same stream with a garbage table
            // fails on the record, and only with a good record on the table.
            for (name, record, expect_format) in [
                ("none", None, true),
                ("three", Some(encode_record(STORE_FORMAT + 1)), true),
                ("two", Some(encode_record(STORE_FORMAT)), false),
            ] {
                let follower = format!("/f-{name}");
                let mut assembler = Assembler::new(
                    env.clone(),
                    Path::new(&follower),
                    Variant::Correct,
                    prefix(),
                );
                let staging = staging_dir(Path::new(&follower));
                let mut sender = Sender::open(&env, &dir, ServerId(2), 3, 2, 4)
                    .await
                    .unwrap();
                let error = loop {
                    let chunk = sender.chunk(&env, 64).await.unwrap();
                    let ananke_raft::message::Message::InstallSnapshot {
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
                    if done {
                        // The staged tables are made garbage and the record is
                        // put under test, just before the chunk that verifies.
                        for staged in fs.read_dir(&staging).await.unwrap() {
                            let text = staged.to_str().unwrap().to_owned();
                            if !text.ends_with(".sst") {
                                continue;
                            }
                            let out = fs
                                .open(
                                    &staging.join(&text),
                                    OpenOptions::new().write(true).create(true).truncate(true),
                                )
                                .await
                                .unwrap();
                            out.write_at(0, Bytes::from_static(b"not a table"))
                                .await
                                .unwrap();
                            out.sync().await.unwrap();
                        }
                        match &record {
                            None => {
                                fs.remove_file(&format_path(&staging)).await.unwrap();
                                fs.sync_dir(&staging).await.unwrap();
                            }
                            Some(bytes) => {
                                put_record(&env, staging.to_str().unwrap(), bytes.clone()).await;
                            }
                        }
                    }
                    match assembler
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
                    {
                        Ok(Feed::Ack { file, offset }) => {
                            sender.on_more(&file, offset);
                        }
                        Ok(Feed::Restart) => panic!("{name}: the stream restarted"),
                        Ok(Feed::Staged(_)) => panic!("{name}: a garbage table verified"),
                        Err(error) => break error,
                    }
                };
                if expect_format {
                    assert!(
                        FormatRefused::from_io(&error)
                            .is_some_and(|r| r.subject == Subject::StagedInstall),
                        "{name}: {error}"
                    );
                } else {
                    assert!(
                        FormatRefused::from_io(&error).is_none(),
                        "{name}: the table was read before the record: {error}"
                    );
                }
            }

            // A staging that records another format, adopted: refused, with the
            // store's tree unchanged — and the pair, the adoption with no staged
            // check, which adopts it. The third case is `AdoptionAsBuilt`, whose
            // own adoption reads the staged record too: the format rule is not
            // what that variant models, and nothing but this drives that read.
            //
            // The store's own record is left with one copy damaged in every
            // case, so the start has a heal to do. The heal runs *after* the
            // adoption (node.rs), so a refused staging leaves the record half
            // damaged, exactly as it was found — the owner's answer of
            // 2026-09-15, that a store refused for a format is written to
            // nowhere. The order of those two steps is asserted here and
            // nowhere else: swap them and the refusal below heals the record
            // first, `after` differs from `before`, and this fails.
            for (case, variants, order, refuses) in [
                ("correct", Variants::correct(), StartOrder::Correct, true),
                (
                    "unchecked",
                    Variants::correct(),
                    StartOrder::StagedFormatUnchecked,
                    false,
                ),
                (
                    "as-built",
                    Variants::from(Variant::AdoptionAsBuilt),
                    StartOrder::Correct,
                    true,
                ),
            ] {
                let follower = format!("/adopt-{case}");
                let store = build_store(&env, &follower).await;
                drop(store);
                let mut assembler = Assembler::new(
                    env.clone(),
                    Path::new(&follower),
                    Variant::Correct,
                    prefix(),
                );
                let staged = stream(&env, &dir, &mut assembler).await;
                assembler
                    .finish(
                        &staged,
                        &Repair {
                            term: 9,
                            vote: None,
                            tail: Vec::new(),
                            quarantined: false,
                            incarnation: 5,
                        },
                    )
                    .await
                    .unwrap();
                // The staged install now records format 3.
                put_record(
                    &env,
                    staging_dir(Path::new(&follower)).to_str().unwrap(),
                    encode_record(STORE_FORMAT + 1),
                )
                .await;
                // And the store's own record has one damaged copy, so the start
                // has a heal to do after the adoption.
                let mut rotted = encode_record(STORE_FORMAT).to_vec();
                rotted[COPY_LEN + 3] ^= 0x10;
                put_record(&env, &follower, Bytes::from(rotted)).await;
                assert!(
                    matches!(
                        decode_record(&get_record(&env, &follower).await.unwrap()),
                        Decoded::Valid {
                            version: STORE_FORMAT,
                            whole: false
                        }
                    ),
                    "{case}: the store's record is not half damaged before the start"
                );
                let before = read_tree(&env, &follower).await;
                let started = start_with(&env, &follower, variants, order).await;
                let (word, message) = outcome(&started);
                let after = read_tree(&env, &follower).await;
                if refuses {
                    assert_eq!(word, "failed", "{case}: {message}");
                    assert!(message.contains("staged install"), "{case}: {message}");
                    assert_eq!(
                        after, before,
                        "{case}: the refused staging changed the store"
                    );
                    assert!(
                        matches!(
                            decode_record(&get_record(&env, &follower).await.unwrap()),
                            Decoded::Valid {
                                version: STORE_FORMAT,
                                whole: false
                            }
                        ),
                        "{case}: the store's record was healed before the staging was refused, \
                         so a store refused for a format was written to"
                    );
                } else {
                    assert_eq!(word, "opened", "{case}: {message}");
                    assert_ne!(
                        after, before,
                        "{case}: the adoption without its staged check was not caught"
                    );
                }
            }

            // An unreadable staged record is staging damage: refused, not swept.
            let follower = "/staging-damage";
            let store = build_store(&env, follower).await;
            drop(store);
            let mut assembler =
                Assembler::new(env.clone(), Path::new(follower), Variant::Correct, prefix());
            let staged = stream(&env, &dir, &mut assembler).await;
            assembler
                .finish(
                    &staged,
                    &Repair {
                        term: 9,
                        vote: None,
                        tail: Vec::new(),
                        quarantined: false,
                        incarnation: 5,
                    },
                )
                .await
                .unwrap();
            let staging = staging_dir(Path::new(follower));
            let mut rotted = encode_record(STORE_FORMAT).to_vec();
            rotted[3] ^= 0x08;
            rotted[COPY_LEN + 3] ^= 0x08;
            put_record(&env, staging.to_str().unwrap(), Bytes::from(rotted)).await;
            let names = fs.read_dir(&staging).await.unwrap();
            let refused = adopt_staged(&env, Path::new(follower))
                .await
                .expect_err("a staging whose record cannot be read is refused");
            assert_eq!(
                LostState::from_io(&refused).and_then(|l| l.damaged),
                Some(Damage::StagingFormatUnreadable),
                "{refused}"
            );
            assert_eq!(
                fs.read_dir(&staging).await.unwrap(),
                names,
                "the damaged staging was swept"
            );
        })
    });
}

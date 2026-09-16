//! One server under the simulator, without faults: three servers elect a leader and
//! apply a client's write on every server; and a server whose store lost state runs
//! in re-seed mode (RAFT.md §3): it has no core to step, grants nothing, and asks
//! every AppendEntries to feed it from index 1, the snapshot ask.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Clock, Environment, FileSystem, Network, NodeId, Socket, TraceEvent};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::Persist;
use ananke_raft::format::{FORMAT_FILE, STORE_FORMAT, Verdict, check_format, encode_record};
use ananke_raft::message::{Frame, Message};
use ananke_raft::node::SINGLE_GROUP;
use ananke_raft::store::{KeyPrefix, RaftStore, marker_path};
use ananke_raft::types::{Entry, Payload};
use ananke_raft::{NodeConfig, RaftConfig, ServerId, invariants, run};
use ananke_storage::EngineConfig;
use ananke_storage::manifest::sst_path;
use bytes::Bytes;

const DIR: &str = "/raft";

fn addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, u8::try_from(n).expect("small")], 7000))
}

fn engine_config() -> EngineConfig {
    let mut config = EngineConfig::new(PathBuf::from(DIR));
    config.memtable_bytes = 4096;
    config.segment_bytes = 4096;
    config.background_compaction = true;
    config
}

/// Today's one group (PROPOSED D-060).
fn prefix() -> KeyPrefix {
    KeyPrefix::group(SINGLE_GROUP)
}

fn node_config(id: u64, servers: &[u64]) -> NodeConfig {
    NodeConfig {
        id: ServerId(id),
        listen: addr(id),
        servers: servers.iter().map(|&s| (ServerId(s), addr(s))).collect(),
        initial_voters: servers.iter().map(|&s| ServerId(s)).collect(),
        raft: RaftConfig::default(),
        engine: engine_config(),
        inbox_capacity: 64,
    }
}

fn spawn_server(sim: &Sim, node: NodeId, config: NodeConfig) {
    let env = sim.env(node);
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run(inner, config).await;
    });
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
fn three_servers_elect_a_leader_and_a_clients_write_is_applied_on_every_server() {
    let mut sim = Sim::new(SimConfig::new(3));
    let servers: Vec<NodeId> = (0..3).map(|_| sim.add_node()).collect();
    for (i, &node) in servers.iter().enumerate() {
        spawn_server(&sim, node, node_config(i as u64 + 1, &[1, 2, 3]));
    }
    let client = sim.add_node();
    sim.run_for(Duration::from_millis(500));
    let leaders = sim
        .trace()
        .iter()
        .filter(|r| matches!(r.event, TraceEvent::RaftLeader { .. }))
        .count();
    assert!(leaders > 0, "no leader after 500ms");

    // A client puts then gets, following NotLeader hints.
    let replies = on_node(&mut sim, client, |env| {
        Box::pin(async move {
            let sock = env.net().bind(addr(9)).await.unwrap();
            let commands = [
                Command::Put {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v"),
                },
                Command::Get {
                    key: Bytes::from_static(b"k"),
                },
            ];
            let mut target = ServerId(1);
            let mut replies = Vec::new();
            for (seq, command) in commands.into_iter().enumerate() {
                loop {
                    let request = Request {
                        client: 1,
                        seq: seq as u64,
                        command: command.clone(),
                    };
                    sock.send(addr(target.0), request.encode()).await.unwrap();
                    let (_, bytes) = sock.recv().await.unwrap();
                    let response = Response::decode(bytes).unwrap();
                    if response.seq != seq as u64 {
                        continue;
                    }
                    match response.reply {
                        Reply::NotLeader { leader: Some(l) } => target = l,
                        Reply::NotLeader { leader: None } => {
                            env.clock().sleep(Duration::from_millis(20)).await;
                            target = ServerId(target.0 % 3 + 1);
                        }
                        Reply::Outcome(outcome) => {
                            replies.push(outcome);
                            break;
                        }
                    }
                }
            }
            replies
        })
    });
    assert_eq!(
        replies,
        vec![
            Outcome::Done,
            Outcome::Value(Some(Bytes::from_static(b"v")))
        ]
    );
    sim.run_for(Duration::from_millis(200));
    let events: Vec<TraceEvent> = sim.trace().into_iter().map(|r| r.event).collect();
    invariants::all(&events).unwrap();
    invariants::commit_majority(&events, 3).unwrap();
    // Every server applied the no-op and the put, the same index with the same hash
    // on all three; the get was served by a lease or a heartbeat round, not the log.
    let applied_by = |server: u64| -> Vec<(u64, u64)> {
        events
            .iter()
            .filter_map(|e| match e {
                TraceEvent::RaftApply {
                    server: s,
                    index,
                    hash,
                    ..
                } if *s == server => Some((*index, *hash)),
                _ => None,
            })
            .collect()
    };
    let first = applied_by(1);
    assert!(first.len() >= 2, "server 1 applied {first:?}");
    for server in [2, 3] {
        let other = applied_by(server);
        assert!(
            other.len() >= first.len().min(2),
            "server {server} applied {other:?}"
        );
        for (a, b) in first.iter().zip(other.iter()) {
            assert_eq!(a, b, "servers 1 and {server} applied differently");
        }
    }
}

/// A store with a table flushed, then that table removed from the disk: the next
/// open drops it and the store is refused.
///
/// A server whose store lost state runs in re-seed mode (RAFT.md §3): it traces
/// the refusal and steps no core — no term, no vote, no append. It stays
/// reachable, ignores vote and pre-vote requests altogether, and answers an
/// AppendEntries only with the re-seed ask: a rejection whose hint is 1 and whose
/// echo is 0, so no lease promise is ever measured from it.
///
/// The store's format record rides through all of it untouched (PROPOSED D-060):
/// the loss is in the engine, the record is a file of its own, and a store whose
/// engine lost writes re-seeds exactly as it did before the record existed — it
/// is never refused for its format, and the server never stops.
#[test]
fn a_server_whose_store_lost_state_asks_to_be_reseeded_and_grants_nothing() {
    let mut sim = Sim::new(SimConfig::new(5));
    let node = sim.add_node();
    let peer = sim.add_node();
    // A log big enough to flush a table.
    let table = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config(), prefix())
                .await
                .unwrap();
            let engine = store.engine().clone();
            let mut entries = Vec::new();
            for index in 1..=200u64 {
                entries.push(Entry {
                    term: 1,
                    index,
                    payload: Payload::Command(Bytes::from(vec![b'x'; 100])),
                });
            }
            store
                .persist(&Persist {
                    term: 1,
                    vote: Some(ServerId(1)),
                    truncate_from: None,
                    append: entries,
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            while engine.ssts() == 0 {
                env.clock().sleep(Duration::from_millis(1)).await;
            }
            engine.levels().concat()[0].number
        })
    });
    // The disk loses the table.
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let fs = env.fs();
            fs.remove_file(&sst_path(Path::new(DIR), table))
                .await
                .unwrap();
            fs.sync_dir(Path::new(DIR)).await.unwrap();
        })
    });
    // Before the server starts, the store's format is this build's and whole.
    let gated = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            matches!(
                check_format(&env, Path::new(DIR)).await.unwrap(),
                Verdict::Recorded { whole: true, .. }
            )
        })
    });
    assert!(gated, "the store records this build's format, whole");
    let record_before = sim
        .durable_contents(node, &Path::new(DIR).join(FORMAT_FILE))
        .expect("the store carries its format record");
    assert_eq!(record_before, encode_record(STORE_FORMAT));
    // The server refuses the store and waits in re-seed mode; run never returns.
    let env = sim.env(node);
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run(inner, node_config(1, &[1, 2, 3])).await;
    });
    sim.run_for(Duration::from_millis(50));
    assert!(sim.trace().iter().any(|r| matches!(&r.event,
        TraceEvent::RaftRefused { server: 1, reason } if reason.contains("dropped tables"))));
    // The refusal is the engine's loss, not the format's: the record is the
    // bytes it was, the server never stopped, and the re-seed goes ahead.
    assert_eq!(
        sim.durable_contents(node, &Path::new(DIR).join(FORMAT_FILE)),
        Some(record_before.clone()),
        "the refusal rewrote the format record"
    );
    assert!(
        !sim.trace()
            .iter()
            .any(|r| matches!(r.event, TraceEvent::RaftServerFailed { .. })),
        "a lost-state refusal stopped the server"
    );
    // A peer asks for a pre-vote, a vote, and sends an append.
    let answers = on_node(&mut sim, peer, |env| {
        Box::pin(async move {
            let sock = env.net().bind(addr(2)).await.unwrap();
            for message in [
                Message::PreVote {
                    term: 5,
                    last_index: 0,
                    last_term: 0,
                },
                Message::RequestVote {
                    term: 5,
                    last_index: 0,
                    last_term: 0,
                    transfer: false,
                },
                Message::AppendEntries {
                    term: 5,
                    prev_index: 7,
                    prev_term: 1,
                    entries: Vec::new(),
                    commit: 0,
                    sent: 123,
                },
            ] {
                let frame = Frame {
                    from: ServerId(2),
                    message,
                };
                sock.send(addr(1), frame.encode()).await.unwrap();
            }
            let deadline = env.clock().now() + Duration::from_millis(200);
            let mut answers = Vec::new();
            loop {
                let recv = std::pin::pin!(sock.recv());
                let timer = std::pin::pin!(env.clock().sleep_until(deadline));
                match ananke_env::race(&env, recv, timer).await {
                    ananke_env::Either::Left(Ok((_, bytes))) => {
                        if let Ok(frame) = Frame::decode(bytes) {
                            answers.push(frame.message);
                        }
                    }
                    _ => break,
                }
            }
            answers
        })
    });
    // Exactly one answer: the re-seed ask for the append; the votes got nothing.
    assert_eq!(answers.len(), 1, "answers: {answers:?}");
    match &answers[0] {
        Message::AppendEntriesResponse {
            term,
            success,
            prev_index,
            hint,
            echo,
            ..
        } => {
            assert_eq!(
                (*term, *success, *prev_index, *hint, *echo),
                (5, false, 7, 1, 0),
                "the re-seed ask"
            );
        }
        other => panic!("expected the re-seed rejection, got {other:?}"),
    }
    let raft_events_of_one = sim
        .trace()
        .iter()
        .filter(|r| {
            matches!(
                &r.event,
                TraceEvent::RaftTerm { server: 1, .. }
                    | TraceEvent::RaftVote { server: 1, .. }
                    | TraceEvent::RaftAppend { server: 1, .. }
            )
        })
        .count();
    assert_eq!(raft_events_of_one, 0, "the refused server stepped its core");
}

/// A refusal is durable and the refused engine does no work (D-044).
/// The same store as above — two hundred entries, a table flushed, the table
/// then lost — and the server that refuses it records the loss in the store's
/// marker before it traces the refusal, writes nothing over it afterwards, and
/// is refused again at the next start on the mark alone. Without the mark the
/// engine's own flush of the memtable the recovery replayed would rewrite the
/// manifest without the dropped table and delete the log segments that held its
/// records, and the next start would find a store that looks whole (the
/// thousand-seed premerge, seed 687).
#[test]
fn a_refusal_is_durable_and_the_refused_engine_does_no_work() {
    let mut sim = Sim::new(SimConfig::new(7));
    let node = sim.add_node();
    // A log big enough to flush a table, and more records past it in the log.
    let table = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config(), prefix())
                .await
                .unwrap();
            let engine = store.engine().clone();
            let append = |from: u64, to: u64| {
                (from..=to)
                    .map(|index| Entry {
                        term: 1,
                        index,
                        payload: Payload::Command(Bytes::from(vec![b'x'; 100])),
                    })
                    .collect::<Vec<_>>()
            };
            store
                .persist(&Persist {
                    term: 1,
                    vote: Some(ServerId(1)),
                    truncate_from: None,
                    append: append(1, 200),
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            while engine.ssts() == 0 {
                env.clock().sleep(Duration::from_millis(1)).await;
            }
            let table = engine.levels().concat()[0].number;
            // Past the flush: these are in the log alone, and are what the
            // refused engine would flush over the loss with.
            store
                .persist(&Persist {
                    term: 1,
                    vote: Some(ServerId(1)),
                    truncate_from: None,
                    append: append(201, 260),
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
            table
        })
    });
    // The disk loses the table.
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let fs = env.fs();
            fs.remove_file(&sst_path(Path::new(DIR), table))
                .await
                .unwrap();
            fs.sync_dir(Path::new(DIR)).await.unwrap();
        })
    });
    let from = sim.trace().len();
    let env = sim.env(node);
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run(inner, node_config(1, &[1, 2, 3])).await;
    });
    sim.run_for(Duration::from_millis(200));
    let records = sim.trace();
    assert!(
        records[from..]
            .iter()
            .any(|r| matches!(&r.event, TraceEvent::RaftRefused { server: 1, .. })),
        "the store is refused for the dropped table"
    );
    // The mark is durable, and it was written before the refusal was traced.
    let marker = sim
        .durable_contents(node, &marker_path(Path::new(DIR)))
        .expect("the store carries a marker");
    let marker = String::from_utf8(marker).expect("the marker is text");
    assert!(
        marker.starts_with("ananke raft store lost\n"),
        "the marker says the store lost state: {marker:?}"
    );
    assert!(
        marker.contains("dropped tables"),
        "the marker carries the refusal's reason: {marker:?}"
    );
    // The refused engine wrote nothing over the loss, before the refusal or
    // after it: no table, no manifest, no switch, no segment deleted.
    let wrote = records[from..]
        .iter()
        .filter(|r| {
            matches!(
                r.event,
                TraceEvent::SstWritten { .. }
                    | TraceEvent::ManifestWritten { .. }
                    | TraceEvent::CurrentSwitched { .. }
                    | TraceEvent::WalSegmentDeleted { .. }
            )
        })
        .count();
    assert_eq!(wrote, 0, "the refused engine wrote over the loss");
    assert!(
        records[from..]
            .iter()
            .any(|r| matches!(r.event, TraceEvent::EngineQuiesced { .. })),
        "the engine was quiesced"
    );
    // The next start is refused on the mark alone, whatever is on disk: the
    // store's own files are all there and the engine would open them.
    sim.crash(node);
    sim.restart(node);
    let from = sim.trace().len();
    let env = sim.env(node);
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run(inner, node_config(1, &[1, 2, 3])).await;
    });
    sim.run_for(Duration::from_millis(200));
    let records = sim.trace();
    let reason = records[from..]
        .iter()
        .find_map(|r| match &r.event {
            TraceEvent::RaftRefused { server: 1, reason } => Some(reason.clone()),
            _ => None,
        })
        .expect("the marked store is refused at the next start too");
    assert!(
        reason.contains("marker says this store lost state"),
        "refused on the mark: {reason}"
    );
    assert!(
        reason.contains("dropped tables"),
        "carrying the reason the first refusal recorded: {reason}"
    );
}

/// The other shape of a format-2 store whose engine lost writes (PROPOSED
/// D-060): a rotted record in the middle of the log. The engine refuses the
/// damage, the server marks the store lost and re-seeds, and the format record
/// beside it is the bytes it was — the loss is never read as another format, and
/// the server never stops for one.
#[test]
fn a_store_whose_log_record_rotted_reseeds_and_keeps_its_record() {
    let mut sim = Sim::new(SimConfig::new(11));
    let node = sim.add_node();
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (store, _) = RaftStore::open_dir(env.clone(), engine_config(), prefix())
                .await
                .unwrap();
            store
                .persist(&Persist {
                    term: 1,
                    vote: Some(ServerId(1)),
                    truncate_from: None,
                    append: (1..=8u64)
                        .map(|index| Entry {
                            term: 1,
                            index,
                            payload: Payload::Command(Bytes::from(vec![b'x'; 40])),
                        })
                        .collect(),
                    config: None,
                    compact_to: None,
                })
                .await
                .unwrap();
        })
    });
    // One bit of the first log record's payload, with records after it: a hole
    // in the middle of the state, not a torn tail.
    on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let path = Path::new(DIR).join("000001.wal");
            let file = env
                .fs()
                .open(&path, ananke_env::OpenOptions::new().read(true).write(true))
                .await
                .unwrap();
            let byte = ananke_env::File::read_at(&file, 24, 1).await.unwrap();
            ananke_env::File::write_at(&file, 24, Bytes::from(vec![byte[0] ^ 0x20]))
                .await
                .unwrap();
            ananke_env::File::sync(&file).await.unwrap();
        })
    });
    let record_before = sim
        .durable_contents(node, &Path::new(DIR).join(FORMAT_FILE))
        .expect("the store carries its format record");
    let from = sim.trace().len();
    let env = sim.env(node);
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run(inner, node_config(1, &[1, 2, 3])).await;
    });
    sim.run_for(Duration::from_millis(100));
    let records = sim.trace();
    assert!(
        records[from..]
            .iter()
            .any(|r| matches!(&r.event, TraceEvent::RaftRefused { server: 1, .. })),
        "the rotted log refuses the store"
    );
    assert!(
        !records[from..]
            .iter()
            .any(|r| matches!(r.event, TraceEvent::RaftServerFailed { .. })),
        "a lost-state refusal stopped the server"
    );
    assert_eq!(
        sim.durable_contents(node, &Path::new(DIR).join(FORMAT_FILE)),
        Some(record_before),
        "the refusal rewrote the format record"
    );
}

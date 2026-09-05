//! One server under the simulator, without faults: three servers elect a leader and
//! apply a client's write on every server; and a server whose store lost state takes
//! part in nothing (RAFT.md §3): it has no core to step, binds no socket, and a
//! peer's votes and appends reach nobody.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, SimEnv};
use ananke_env::{Clock, DropReason, Environment, FileSystem, Network, NodeId, Socket, TraceEvent};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::Persist;
use ananke_raft::message::{Frame, Message};
use ananke_raft::store::{LostState, RaftStore};
use ananke_raft::types::{Entry, Payload};
use ananke_raft::{NodeConfig, RaftConfig, ServerId, invariants, run};
use ananke_storage::manifest::sst_path;
use ananke_storage::{Engine, EngineConfig};
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

fn node_config(id: u64, servers: &[u64]) -> NodeConfig {
    NodeConfig {
        id: ServerId(id),
        listen: addr(id),
        servers: servers.iter().map(|&s| (ServerId(s), addr(s))).collect(),
        raft: RaftConfig::default(),
        engine: engine_config(),
        tick: Duration::from_millis(10),
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
    // Every server applied the put: the same index with the same hash on all three.
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
    assert!(first.len() >= 3, "server 1 applied {first:?}");
    for server in [2, 3] {
        let other = applied_by(server);
        assert!(
            other.len() >= first.len().min(3),
            "server {server} applied {other:?}"
        );
        for (a, b) in first.iter().zip(other.iter()) {
            assert_eq!(a, b, "servers 1 and {server} applied differently");
        }
    }
}

/// A store with a table flushed, then that table removed from the disk: the next
/// open drops it, the store is refused, and the server never starts. The peer that
/// asks it for a vote and sends it entries gets nothing back: its messages reach no
/// socket.
#[test]
fn a_server_whose_store_lost_state_takes_part_in_nothing() {
    let mut sim = Sim::new(SimConfig::new(5));
    let node = sim.add_node();
    let peer = sim.add_node();
    // A log big enough to flush a table.
    let table = on_node(&mut sim, node, |env| {
        Box::pin(async move {
            let (engine, recovery) = Engine::open(env.clone(), engine_config()).await.unwrap();
            let engine = Arc::new(engine);
            let (store, _) = RaftStore::open(engine.clone(), &recovery).await.unwrap();
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
    // The server refuses to start.
    let result = on_node(&mut sim, node, |env| {
        Box::pin(async move { run(env, node_config(1, &[1, 2, 3])).await })
    });
    let error = result.expect_err("the server is refused");
    let lost = LostState::from_io(&error).expect("a LostState refusal");
    assert_eq!(lost.dropped, vec![table], "{lost}");
    assert!(sim.trace().iter().any(|r| matches!(&r.event,
        TraceEvent::RaftRefused { server: 1, reason } if reason.contains("dropped tables"))));
    // A peer asks for a vote and sends entries: nothing arrives, nothing comes back.
    on_node(&mut sim, peer, |env| {
        Box::pin(async move {
            let sock = env.net().bind(addr(2)).await.unwrap();
            for message in [
                Message::RequestVote {
                    term: 5,
                    last_index: 0,
                    last_term: 0,
                },
                Message::AppendEntries {
                    term: 5,
                    prev_index: 0,
                    prev_term: 0,
                    entries: Vec::new(),
                    commit: 0,
                },
            ] {
                let frame = Frame {
                    from: ServerId(2),
                    message,
                };
                sock.send(addr(1), frame.encode()).await.unwrap();
            }
            env.clock().sleep(Duration::from_millis(100)).await;
        })
    });
    let records = sim.trace();
    let delivered_to_one = records
        .iter()
        .filter(|r| matches!(&r.event, TraceEvent::MessageDelivered { to, .. } if *to == addr(1)))
        .count();
    let unreachable = records
        .iter()
        .filter(|r| {
            matches!(&r.event, TraceEvent::MessageDropped { to, reason: DropReason::Unreachable, .. } if *to == addr(1))
        })
        .count();
    let sent_by_one = records
        .iter()
        .filter(|r| matches!(&r.event, TraceEvent::MessageSent { from, .. } if *from == addr(1)))
        .count();
    assert_eq!(
        (delivered_to_one, unreachable, sent_by_one),
        (0, 2, 0),
        "delivered {delivered_to_one}, unreachable {unreachable}, sent {sent_by_one}"
    );
    let raft_events_of_one = records
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

//! Determinism through the public API: a two-node ping-pong under drops, delays, a
//! partition and a crash produces byte-identical traces for equal seeds.

use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig};
use ananke_env::{
    Clock, Either, Environment, File, FileSystem, Network, OpenOptions, Rng, Socket, det_hash_map,
    race,
};
use bytes::Bytes;

fn addr(n: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, 1], 7000 + n))
}

/// Pings the peer every 5ms, answers pings, and journals every message it sees.
async fn node<E: Environment>(env: E, me: u16, peer: u16) {
    let sock = env.net().bind(addr(me)).await.unwrap();
    let fs = env.fs();
    fs.create_dir_all(Path::new("/journal")).await.unwrap();
    let journal = fs
        .open(
            Path::new("/journal/log"),
            OpenOptions::new().read(true).write(true).create(true),
        )
        .await
        .unwrap();
    let mut offset = journal.size().await.unwrap();
    let mut counts = det_hash_map::<u8, u32>(env.rng());
    let mut next_ping = env.clock().now();
    loop {
        // Either a message arrives or it is time to ping.
        let recv = pin!(sock.recv());
        let timer = pin!(env.clock().sleep_until(next_ping));
        let message = match race(&env, recv, timer).await {
            Either::Left(m) => Some(m.unwrap()),
            Either::Right(()) => None,
        };
        match message {
            Some((from, msg)) => {
                *counts.entry(msg[0]).or_default() += 1;
                let line = format!("{} {}\n", from, String::from_utf8_lossy(&msg));
                journal.write_at(offset, Bytes::from(line)).await.unwrap();
                offset += 1;
                if env.rng().below(4) == 0 {
                    journal.sync().await.unwrap();
                }
                if msg[0] == b'p' {
                    sock.send(from, Bytes::from_static(b"q")).await.unwrap();
                }
            }
            None => {
                next_ping = env.clock().now() + Duration::from_millis(5);
                sock.send(addr(peer), Bytes::from_static(b"p"))
                    .await
                    .unwrap();
            }
        }
    }
}

fn run(seed: u64) -> String {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.1;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(8);
    config.fs.p_durable = 0.5;
    config.clock.max_skew = Duration::from_millis(20);
    config.clock.max_drift_ppm = 1_000;
    let mut sim = Sim::new(config);
    let (a, b) = (sim.add_node(), sim.add_node());
    sim.env(a).spawn("node a", node(sim.env(a), 1, 2));
    sim.env(b).spawn("node b", node(sim.env(b), 2, 1));
    sim.run_for(Duration::from_millis(100));
    sim.partition(&[a], &[b]);
    sim.run_for(Duration::from_millis(50));
    sim.heal();
    sim.run_for(Duration::from_millis(50));
    sim.crash(b);
    sim.env(b).spawn("node b again", node(sim.env(b), 2, 1));
    sim.run_for(Duration::from_millis(100));
    sim.trace_text()
}

#[test]
fn same_seed_same_trace() {
    let first = run(42);
    assert_eq!(first, run(42));
    assert!(
        first.lines().count() > 200,
        "the scenario should produce a substantial trace"
    );
    for needle in [
        "MessageDelivered",
        "MessageDropped",
        "Partitioned",
        "Injected",
        "TimeAdvanced",
        "NodeCrashed",
        "FsyncLost",
    ] {
        assert!(first.contains(needle), "trace lacks {needle}");
    }
}

#[test]
fn different_seeds_different_traces() {
    assert_ne!(run(1), run(2));
}

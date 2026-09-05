//! Simulator unit tests. Scenario-level determinism is covered in `tests/sim_env.rs`.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;

use moirae_sched::Policy;

use super::*;
use crate::{
    Clock, DirEntryOp, DropReason, File, FileSystem, MessageId, Network, OpenOptions, Rng, Socket,
};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// Yields to the scheduler once.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

type Log<T> = Arc<Mutex<Vec<T>>>;

fn log<T>() -> Log<T> {
    Arc::new(Mutex::new(Vec::new()))
}

fn addr(port: u16) -> std::net::SocketAddr {
    std::net::SocketAddr::from(([10, 0, 0, 1], port))
}

fn events(sim: &Sim) -> Vec<TraceEvent> {
    sim.trace().into_iter().map(|r| r.event).collect()
}

#[test]
fn timers_advance_virtual_time_in_order() {
    let mut sim = Sim::new(SimConfig::new(1));
    let n = sim.add_node();
    let env = sim.env(n);
    let fired: Log<(u64, Instant)> = log();
    for (i, delay) in [30u64, 10, 20].into_iter().enumerate() {
        let (env, fired) = (env.clone(), fired.clone());
        env.clone().spawn("timer", async move {
            env.clock().sleep(ms(delay)).await;
            fired.lock().unwrap().push((i as u64, env.clock().now()));
        });
    }
    sim.run_until(Instant::from_nanos(1_000_000_000));
    let fired = fired.lock().unwrap().clone();
    assert_eq!(
        fired,
        vec![
            (1, Instant::from_nanos(10_000_000)),
            (2, Instant::from_nanos(20_000_000)),
            (0, Instant::from_nanos(30_000_000))
        ]
    );
    assert_eq!(
        sim.now(),
        Instant::from_nanos(1_000_000_000),
        "run_until lands exactly on the deadline"
    );
    assert!(events(&sim).contains(&TraceEvent::TimeAdvanced {
        to: Instant::from_nanos(10_000_000)
    }));
}

#[test]
fn sleep_honours_the_clock_contract_under_skew_and_drift() {
    for (skew, drift) in [
        (0, 0),
        (-5_000_000, 0),
        (7_000_000, 0),
        (0, 500),
        (0, -500),
        (-3_000_000, 250_000),
        (3_000_000, -250_000),
    ] {
        let mut sim = Sim::new(SimConfig::new(7));
        let n = sim.add_node_with_clock(skew, drift);
        let env = sim.env(n);
        let checked: Log<bool> = log();
        let c = checked.clone();
        env.clone().spawn("sleeper", async move {
            for delay in [1u64, 7, 33, 1000] {
                let before = env.clock().now();
                env.clock().sleep(Duration::from_micros(delay)).await;
                let after = env.clock().now();
                assert!(
                    after >= before + Duration::from_micros(delay),
                    "skew {skew} drift {drift}: {after:?} < {before:?} + {delay}us"
                );
                // Sleeping until the past resolves at once.
                env.clock().sleep_until(before).await;
                assert_eq!(env.clock().now(), after);
            }
            c.lock().unwrap().push(true);
        });
        sim.run_until(Instant::from_nanos(1_000_000_000));
        assert_eq!(
            checked.lock().unwrap().len(),
            1,
            "skew {skew} drift {drift}: task did not finish"
        );
    }
}

#[test]
fn skewed_clocks_disagree_with_global_time() {
    let mut sim = Sim::new(SimConfig::new(3));
    let ahead = sim.add_node_with_clock(5_000_000, 0);
    let fast = sim.add_node_with_clock(0, 100_000);
    sim.run_until(Instant::from_nanos(100_000_000));
    assert_eq!(
        sim.env(ahead).clock().now(),
        Instant::from_nanos(105_000_000)
    );
    assert_eq!(
        sim.env(fast).clock().now(),
        Instant::from_nanos(110_000_000)
    );
    assert_eq!(
        sim.env(ahead).clock().wall() - sim.env(fast).clock().wall(),
        Duration::ZERO
    );
    assert_eq!(
        sim.env(fast).clock().wall() - sim.env(ahead).clock().wall(),
        ms(5)
    );
}

#[test]
fn scheduler_records_every_poll_and_interleaves_deterministically() {
    let order_for = |seed: u64| {
        let mut sim = Sim::new(SimConfig::new(seed));
        let n = sim.add_node();
        let env = sim.env(n);
        let order: Log<&'static str> = log();
        for name in ["a", "b"] {
            let order = order.clone();
            env.spawn(name, async move {
                for _ in 0..5 {
                    order.lock().unwrap().push(name);
                    YieldOnce(false).await;
                }
            });
        }
        sim.run_until(Instant::ZERO);
        let polls = events(&sim)
            .iter()
            .filter(|e| matches!(e, TraceEvent::TaskPolled { .. }))
            .count();
        assert_eq!(
            polls, 12,
            "each task is polled six times: five yields plus completion"
        );
        assert_eq!(
            events(&sim)
                .iter()
                .filter(|e| matches!(e, TraceEvent::TaskCompleted { .. }))
                .count(),
            2
        );
        let order = order.lock().unwrap().clone();
        assert_eq!(order.len(), 10);
        order
    };
    assert_eq!(order_for(11), order_for(11));
    let distinct = (0..20)
        .map(order_for)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        distinct.len() > 1,
        "random scheduling should produce more than one interleaving over 20 seeds"
    );
}

#[test]
fn abort_drops_a_task_without_a_completion_event() {
    let mut sim = Sim::new(SimConfig::new(1));
    let n = sim.add_node();
    let env = sim.env(n);
    let ran: Log<()> = log();
    let r = ran.clone();
    let e = env.clone();
    let handle = env.spawn("victim", async move {
        e.clock().sleep(ms(10)).await;
        r.lock().unwrap().push(());
    });
    sim.run_until(Instant::from_nanos(1_000_000));
    handle.abort();
    sim.run_until(Instant::from_nanos(100_000_000));
    assert!(ran.lock().unwrap().is_empty());
    assert!(
        !events(&sim)
            .iter()
            .any(|e| matches!(e, TraceEvent::TaskCompleted { .. }))
    );
}

#[test]
fn messages_are_delivered_after_the_configured_delay() {
    let mut config = SimConfig::new(5);
    config.net.delay_min = ms(5);
    config.net.delay_max = ms(5);
    let mut sim = Sim::new(config);
    let (a, b) = (sim.add_node(), sim.add_node());
    let got: Log<(std::net::SocketAddr, Bytes, Instant)> = log();
    let g = got.clone();
    let env_b = sim.env(b);
    env_b.clone().spawn("receiver", async move {
        let sock = env_b.net().bind(addr(2)).await.unwrap();
        let (from, msg) = sock.recv().await.unwrap();
        g.lock().unwrap().push((from, msg, env_b.clock().now()));
    });
    let env_a = sim.env(a);
    env_a.clone().spawn("sender", async move {
        let sock = env_a.net().bind(addr(1)).await.unwrap();
        sock.send(addr(2), Bytes::from_static(b"hi")).await.unwrap();
    });
    sim.run_until(Instant::from_nanos(1_000_000_000));
    let got = got.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![(
            addr(1),
            Bytes::from_static(b"hi"),
            Instant::from_nanos(5_000_000)
        )]
    );
    let ev = events(&sim);
    let sent = ev.iter().find_map(|e| match e {
        TraceEvent::MessageSent {
            id,
            from,
            to,
            payload,
        } if *from == addr(1) && *to == addr(2) && payload[..] == b"hi"[..] => Some(*id),
        _ => None,
    });
    let sent = sent.expect("a MessageSent for the ping");
    assert!(ev.iter().any(|e| matches!(e, TraceEvent::MessageDelivered { id, from, to, len: 2 } if *id == sent && *from == addr(1) && *to == addr(2))));
}

fn ping_and_count(config: SimConfig, setup: impl FnOnce(&mut Sim, NodeId, NodeId)) -> (Sim, usize) {
    let mut sim = Sim::new(config);
    let (a, b) = (sim.add_node(), sim.add_node());
    let received: Log<()> = log();
    let r = received.clone();
    let env_b = sim.env(b);
    env_b.clone().spawn("receiver", async move {
        let sock = env_b.net().bind(addr(2)).await.unwrap();
        loop {
            sock.recv().await.unwrap();
            r.lock().unwrap().push(());
        }
    });
    let env_a = sim.env(a);
    env_a.clone().spawn("sender", async move {
        let sock = env_a.net().bind(addr(1)).await.unwrap();
        for _ in 0..20 {
            sock.send(addr(2), Bytes::from_static(b"ping"))
                .await
                .unwrap();
            env_a.clock().sleep(ms(1)).await;
        }
    });
    sim.run_until(Instant::from_nanos(1_000_000));
    setup(&mut sim, a, b);
    sim.run_until(Instant::from_nanos(1_000_000_000));
    let count = received.lock().unwrap().len();
    (sim, count)
}

#[test]
fn partitions_drop_with_reason_and_heal() {
    let (sim, count) = ping_and_count(SimConfig::new(2), |sim, a, b| sim.partition(&[a], &[b]));
    assert!(
        count <= 1,
        "only the first ping, sent before the partition, may get through: {count}"
    );
    assert!(events(&sim).iter().any(|e| matches!(
        e,
        TraceEvent::MessageDropped {
            reason: DropReason::Partitioned,
            ..
        }
    )));

    let (_, count) = ping_and_count(SimConfig::new(2), |sim, a, b| {
        sim.partition(&[a], &[b]);
        sim.heal();
    });
    assert_eq!(count, 20);

    // Asymmetric: a -> b blocked, b -> a not.
    let (sim, count) = ping_and_count(SimConfig::new(2), |sim, a, b| sim.block(a, b));
    assert!(count <= 1);
    assert!(
        !sim.shared
            .lock()
            .fabric
            .is_blocked(NodeId::new(1), NodeId::new(1))
    );
}

#[test]
fn injected_drops_lose_everything_at_probability_one() {
    let mut config = SimConfig::new(9);
    config.net.p_drop = 1.0;
    let (sim, count) = ping_and_count(config, |_, _, _| {});
    assert_eq!(count, 0);
    assert_eq!(
        events(&sim)
            .iter()
            .filter(|e| matches!(
                e,
                TraceEvent::MessageDropped {
                    reason: DropReason::Injected,
                    ..
                }
            ))
            .count(),
        20
    );
}

#[test]
fn unbound_destinations_are_unreachable() {
    let mut sim = Sim::new(SimConfig::new(1));
    let a = sim.add_node();
    let env = sim.env(a);
    env.clone().spawn("sender", async move {
        let sock = env.net().bind(addr(1)).await.unwrap();
        sock.send(addr(9), Bytes::from_static(b"void"))
            .await
            .unwrap();
    });
    sim.run_until(Instant::from_nanos(1_000_000_000));
    assert!(events(&sim).iter().any(|e| matches!(e, TraceEvent::MessageDropped { from, to, reason: DropReason::Unreachable, .. } if *from == addr(1) && *to == addr(9))));
}

#[test]
fn random_delays_reorder_messages() {
    let mut config = SimConfig::new(4);
    config.net.delay_min = ms(0);
    config.net.delay_max = ms(10);
    let mut sim = Sim::new(config);
    let (a, b) = (sim.add_node(), sim.add_node());
    let got: Log<u32> = log();
    let g = got.clone();
    let env_b = sim.env(b);
    env_b.clone().spawn("receiver", async move {
        let sock = env_b.net().bind(addr(2)).await.unwrap();
        loop {
            let (_, msg) = sock.recv().await.unwrap();
            g.lock()
                .unwrap()
                .push(u32::from_be_bytes(msg[..].try_into().unwrap()));
        }
    });
    let env_a = sim.env(a);
    env_a.clone().spawn("sender", async move {
        let sock = env_a.net().bind(addr(1)).await.unwrap();
        for n in 0..30u32 {
            sock.send(addr(2), Bytes::copy_from_slice(&n.to_be_bytes()))
                .await
                .unwrap();
        }
    });
    sim.run_until(Instant::from_nanos(1_000_000_000));
    let mut got = got.lock().unwrap().clone();
    assert_eq!(got.len(), 30);
    assert!(
        !got.is_sorted(),
        "thirty messages with 0..10ms delays did not reorder: {got:?}"
    );
    got.sort_unstable();
    assert_eq!(got, (0..30).collect::<Vec<_>>());
}

#[test]
fn bind_rejects_duplicate_addresses_and_assigns_ports() {
    let mut sim = Sim::new(SimConfig::new(1));
    let a = sim.add_node();
    let env = sim.env(a);
    let outcome: Log<String> = log();
    let o = outcome.clone();
    env.clone().spawn("binder", async move {
        let first = env.net().bind(addr(1)).await.unwrap();
        let err = env.net().bind(addr(1)).await.unwrap_err();
        o.lock().unwrap().push(format!("{:?}", err.kind()));
        drop(first);
        env.net().bind(addr(1)).await.unwrap();
        let any = env.net().bind(addr(0)).await.unwrap();
        o.lock().unwrap().push(any.local_addr().port().to_string());
    });
    sim.run_until(Instant::ZERO);
    assert_eq!(*outcome.lock().unwrap(), ["AddrInUse", "10000"]);
}

fn open_rw() -> OpenOptions {
    OpenOptions::new().read(true).write(true).create(true)
}

#[test]
fn filesystem_semantics() {
    let mut sim = Sim::new(SimConfig::new(1));
    let n = sim.add_node();
    let env = sim.env(n);
    let done: Log<()> = log();
    let d = done.clone();
    env.clone().spawn("fs", async move {
        let fs = env.fs();
        let dir = Path::new("/data/wal");
        fs.create_dir_all(dir).await.unwrap();
        let path = dir.join("000001.wal");
        assert_eq!(
            fs.open(&path, OpenOptions::new().read(true))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(
            fs.open(&path, OpenOptions::new().read(true).create(true))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        let f = fs.open(&path, open_rw()).await.unwrap();
        f.write_at(0, Bytes::from_static(b"hello ")).await.unwrap();
        f.write_at(6, Bytes::from_static(b"world")).await.unwrap();
        assert_eq!(f.size().await.unwrap(), 11);
        assert_eq!(
            f.read_at(0, 100).await.unwrap(),
            Bytes::from_static(b"hello world")
        );
        assert_eq!(f.read_at(6, 3).await.unwrap(), Bytes::from_static(b"wor"));
        assert!(f.read_at(50, 3).await.unwrap().is_empty());
        f.set_size(3).await.unwrap();
        f.set_size(5).await.unwrap();
        assert_eq!(
            f.read_at(0, 100).await.unwrap(),
            Bytes::from_static(b"hel\0\0")
        );
        assert_eq!(
            fs.open(&path, OpenOptions::new().write(true).create_new(true))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::AlreadyExists
        );
        let ro = fs.open(&path, OpenOptions::new().read(true)).await.unwrap();
        assert_eq!(
            ro.write_at(0, Bytes::from_static(b"x"))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            fs.open(dir, open_rw()).await.unwrap_err().kind(),
            std::io::ErrorKind::IsADirectory
        );

        fs.open(&dir.join("b"), open_rw()).await.unwrap();
        fs.create_dir_all(&dir.join("a")).await.unwrap();
        assert_eq!(
            fs.read_dir(dir).await.unwrap(),
            ["000001.wal", "a", "b"].map(std::path::PathBuf::from)
        );
        assert_eq!(
            fs.read_dir(Path::new("/data")).await.unwrap(),
            [std::path::PathBuf::from("wal")]
        );
        assert_eq!(
            fs.read_dir(Path::new("/nope")).await.unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );

        let renamed = dir.join("000001.wal.old");
        fs.rename(&path, &renamed).await.unwrap();
        fs.sync_dir(dir).await.unwrap();
        assert_eq!(
            fs.open(&path, OpenOptions::new().read(true))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(
            f.read_at(0, 3).await.unwrap(),
            Bytes::from_static(b"hel"),
            "open handles survive a rename"
        );
        fs.remove_file(&renamed).await.unwrap();
        assert_eq!(
            fs.remove_file(&renamed).await.unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(
            fs.read_dir(dir).await.unwrap(),
            ["a", "b"].map(std::path::PathBuf::from)
        );
        d.lock().unwrap().push(());
    });
    sim.run_until(Instant::ZERO);
    assert_eq!(done.lock().unwrap().len(), 1);
}

fn write_three_then(config: SimConfig, sync: bool) -> Sim {
    let mut sim = Sim::new(config);
    let n = sim.add_node();
    let env = sim.env(n);
    env.clone().spawn("writer", async move {
        let f = env.fs().open(Path::new("/f"), open_rw()).await.unwrap();
        env.fs().sync_dir(Path::new("/")).await.unwrap(); // the entry is durable; only the writes are at risk
        f.write_at(0, Bytes::from_static(b"aaaa")).await.unwrap();
        f.write_at(4, Bytes::from_static(b"bbbb")).await.unwrap();
        f.write_at(8, Bytes::from_static(b"cccc")).await.unwrap();
        if sync {
            f.sync().await.unwrap();
        }
    });
    sim.run_until(Instant::ZERO);
    sim
}

#[test]
fn synced_data_survives_a_crash_in_strict_mode() {
    let mut sim = write_three_then(SimConfig::new(1), true);
    assert_eq!(
        sim.durable_contents(NodeId::new(1), Path::new("/f"))
            .unwrap(),
        b"aaaabbbbcccc"
    );
    sim.crash(NodeId::new(1));
    assert_eq!(
        sim.durable_contents(NodeId::new(1), Path::new("/f"))
            .unwrap(),
        b"aaaabbbbcccc"
    );
    assert!(events(&sim).contains(&TraceEvent::NodeCrashed {
        node: NodeId::new(1)
    }));
}

#[test]
fn unsynced_writes_survive_a_crash_only_as_a_prefix() {
    let mut torn_seen = false;
    let mut lengths = std::collections::BTreeSet::new();
    for seed in 0..64 {
        let mut sim = write_three_then(SimConfig::new(seed), false);
        assert!(
            sim.durable_contents(NodeId::new(1), Path::new("/f"))
                .unwrap()
                .is_empty(),
            "nothing is durable before the crash"
        );
        sim.crash(NodeId::new(1));
        let on_disk = sim
            .durable_contents(NodeId::new(1), Path::new("/f"))
            .unwrap();
        assert!(
            b"aaaabbbbcccc".starts_with(&on_disk),
            "seed {seed}: {on_disk:?} is not a prefix"
        );
        lengths.insert(on_disk.len());
        torn_seen |= events(&sim)
            .iter()
            .any(|e| matches!(e, TraceEvent::WriteTorn { .. }));
        // What the restarted node reads is exactly what is on disk.
        let env = sim.env(NodeId::new(1));
        let seen: Log<Bytes> = log();
        let s = seen.clone();
        env.clone().spawn("reader", async move {
            let f = env
                .fs()
                .open(Path::new("/f"), OpenOptions::new().read(true))
                .await
                .unwrap();
            let contents = f.read_at(0, 100).await.unwrap();
            s.lock().unwrap().push(contents);
        });
        sim.run_until(Instant::ZERO);
        assert_eq!(seen.lock().unwrap()[0], on_disk);
    }
    assert!(torn_seen, "64 seeds never tore a write");
    assert!(lengths.len() > 3, "64 seeds produced only {lengths:?}");
}

#[test]
fn lost_fsync_returns_ok_but_persists_nothing() {
    let mut config = SimConfig::new(1);
    config.fs.p_durable = 0.0;
    let mut sim = write_three_then(config, true);
    assert!(
        events(&sim)
            .iter()
            .any(|e| matches!(e, TraceEvent::FsyncLost { .. }))
    );
    assert!(
        sim.durable_contents(NodeId::new(1), Path::new("/f"))
            .unwrap()
            .is_empty()
    );
    sim.crash(NodeId::new(1));
    assert!(
        b"aaaabbbbcccc".starts_with(
            &sim.durable_contents(NodeId::new(1), Path::new("/f"))
                .unwrap()
        )
    );
}

#[test]
fn crash_kills_tasks_and_unbinds_sockets() {
    let mut sim = Sim::new(SimConfig::new(1));
    let (a, b) = (sim.add_node(), sim.add_node());
    let ticks: Log<()> = log();
    let t = ticks.clone();
    let env_a = sim.env(a);
    env_a.clone().spawn("ticker", async move {
        let _sock = env_a.net().bind(addr(1)).await.unwrap();
        loop {
            env_a.clock().sleep(ms(1)).await;
            t.lock().unwrap().push(());
        }
    });
    sim.run_until(Instant::from_nanos(5_500_000));
    assert_eq!(ticks.lock().unwrap().len(), 5);
    sim.crash(a);
    sim.run_until(Instant::from_nanos(50_000_000));
    assert_eq!(ticks.lock().unwrap().len(), 5, "no ticks after the crash");
    let env_b = sim.env(b);
    env_b.clone().spawn("prober", async move {
        let sock = env_b.net().bind(addr(2)).await.unwrap();
        sock.send(addr(1), Bytes::from_static(b"anyone?"))
            .await
            .unwrap();
    });
    sim.run_until(Instant::from_nanos(100_000_000));
    assert!(events(&sim).iter().any(|e| matches!(e, TraceEvent::MessageDropped { from, to, reason: DropReason::Unreachable, .. } if *from == addr(2) && *to == addr(1))));
    // Restart: the address is free again.
    let env_a = sim.env(a);
    let bound: Log<bool> = log();
    let bd = bound.clone();
    env_a.clone().spawn("restarted", async move {
        let ok = env_a.net().bind(addr(1)).await.is_ok();
        bd.lock().unwrap().push(ok);
    });
    sim.run_until(Instant::from_nanos(100_000_001));
    assert_eq!(*bound.lock().unwrap(), [true]);
}

#[test]
fn node_rng_is_seeded_and_independent_of_scheduling() {
    let draw = |seed| {
        let mut sim = Sim::new(SimConfig::new(seed));
        let n = sim.add_node();
        let rng = sim.env(n);
        (rng.rng().next_u64(), rng.rng().next_u64())
    };
    assert_eq!(draw(1), draw(1));
    assert_ne!(draw(1), draw(2));
    let mut sim = Sim::new(SimConfig::new(1));
    let (a, b) = (sim.add_node(), sim.add_node());
    assert_ne!(sim.env(a).rng().next_u64(), sim.env(b).rng().next_u64());
}

#[test]
fn equal_timestamps_fire_in_registration_order() {
    struct Recorder(Log<u8>, u8);
    impl std::task::Wake for Recorder {
        fn wake(self: Arc<Self>) {
            self.0.lock().unwrap().push(self.1);
        }
    }
    let mut sim = Sim::new(SimConfig::new(1));
    sim.add_node();
    let woken: Log<u8> = log();
    let at = Instant::from_nanos(50);
    {
        let mut st = sim.shared.lock();
        for id in [1u8, 2, 3] {
            st.register_timer(
                at,
                std::task::Waker::from(Arc::new(Recorder(woken.clone(), id))),
            );
        }
    }
    let mut wakers = Vec::new();
    sim.shared.lock().advance_to(at, &mut wakers);
    for waker in wakers {
        waker.wake();
    }
    assert_eq!(*woken.lock().unwrap(), [1, 2, 3]);
}

#[test]
fn equal_delivery_times_keep_send_order() {
    let mut config = SimConfig::new(6);
    config.net.delay_min = ms(5);
    config.net.delay_max = ms(5);
    let mut sim = Sim::new(config);
    let (a, b) = (sim.add_node(), sim.add_node());
    let got: Log<u32> = log();
    let g = got.clone();
    let env_b = sim.env(b);
    env_b.clone().spawn("receiver", async move {
        let sock = env_b.net().bind(addr(2)).await.unwrap();
        loop {
            let (_, msg) = sock.recv().await.unwrap();
            g.lock()
                .unwrap()
                .push(u32::from_be_bytes(msg[..].try_into().unwrap()));
        }
    });
    let env_a = sim.env(a);
    env_a.clone().spawn("sender", async move {
        let sock = env_a.net().bind(addr(1)).await.unwrap();
        for n in 0..10u32 {
            sock.send(addr(2), Bytes::copy_from_slice(&n.to_be_bytes()))
                .await
                .unwrap();
        }
    });
    sim.run_until(Instant::from_nanos(1_000_000_000));
    assert_eq!(*got.lock().unwrap(), (0..10).collect::<Vec<_>>());
}

/// `race` draws its poll order from the node's seeded stream, so a task with an
/// always-ready message source still fires its timer soon after it is due.
#[test]
fn race_lets_a_timer_fire_under_a_flood_of_ready_messages() {
    use std::pin::pin;

    use crate::{Either, race};

    const DEADLINE: Duration = Duration::from_millis(10);
    const PROCESSING: Duration = Duration::from_millis(1);
    // Ten polls reach the deadline; after that each poll fires the timer with
    // probability one half, so 64 more is a 2^-64 chance of failing per seed.
    const BOUND: u32 = 10 + 64;

    for seed in 0..100 {
        let mut sim = Sim::new(SimConfig::new(seed));
        let n = sim.add_node();
        let env = sim.env(n);
        let polls: Log<u32> = log();
        let p = polls.clone();
        env.clone().spawn("flooded", async move {
            let deadline = env.clock().now() + DEADLINE;
            let mut count = 0u32;
            loop {
                count += 1;
                let message = pin!(std::future::ready(()));
                let timer = pin!(env.clock().sleep_until(deadline));
                match race(&env, message, timer).await {
                    // "Handle" the message; this is what lets virtual time move.
                    Either::Left(()) => env.clock().sleep(PROCESSING).await,
                    Either::Right(()) => break,
                }
            }
            p.lock().unwrap().push(count);
        });
        sim.run_until(Instant::from_nanos(10_000_000_000));
        let count = polls
            .lock()
            .unwrap()
            .first()
            .copied()
            .unwrap_or_else(|| panic!("seed {seed}: the timer never fired"));
        assert!(count <= BOUND, "seed {seed}: timer took {count} polls");
        assert!(
            count >= 10,
            "seed {seed}: timer fired before it was due after {count} polls"
        );
    }
}

#[test]
fn message_ids_correlate_send_delivery_and_drop() {
    let mut config = SimConfig::new(6);
    config.net.delay_min = ms(1);
    config.net.delay_max = ms(1);
    let mut sim = Sim::new(config);
    let (a, b) = (sim.add_node(), sim.add_node());
    let env_b = sim.env(b);
    env_b.clone().spawn("receiver", async move {
        let sock = env_b.net().bind(addr(2)).await.unwrap();
        loop {
            sock.recv().await.unwrap();
        }
    });
    let env_a = sim.env(a);
    env_a.clone().spawn("sender", async move {
        let sock = env_a.net().bind(addr(1)).await.unwrap();
        sock.send(addr(2), Bytes::from_static(b"one"))
            .await
            .unwrap();
        sock.send(addr(9), Bytes::from_static(b"nobody"))
            .await
            .unwrap();
    });
    sim.run_until(Instant::from_nanos(1_000_000_000));
    let ev = events(&sim);
    let ids: Vec<MessageId> = ev
        .iter()
        .filter_map(|e| match e {
            TraceEvent::MessageSent { id, .. } => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        [MessageId::new(0), MessageId::new(1)],
        "ids are sequential per simulation"
    );
    assert!(
        ev.iter()
            .any(|e| matches!(e, TraceEvent::MessageDelivered { id, .. } if *id == ids[0]))
    );
    assert!(ev.iter().any(|e| matches!(e, TraceEvent::MessageDropped { id, reason: DropReason::Unreachable, .. } if *id == ids[1])));
}

#[test]
fn partition_heal_block_and_restart_are_traced() {
    let mut sim = Sim::new(SimConfig::new(1));
    let (a, b, c) = (sim.add_node(), sim.add_node(), sim.add_node());
    sim.partition(&[a], &[c, b]);
    sim.heal();
    sim.partition(&[a], &[b]); // does not cover c: links, not a partition
    sim.block(b, c);
    sim.block(b, c); // already blocked: no second event
    sim.heal();
    sim.crash(b);
    sim.restart(b);
    let ev = events(&sim);
    let expected = vec![
        TraceEvent::PartitionStarted {
            groups: vec![vec![a], vec![b, c]],
        },
        TraceEvent::PartitionHealed {
            groups: vec![vec![a], vec![b, c]],
        },
        TraceEvent::LinkBlocked { from: a, to: b },
        TraceEvent::LinkBlocked { from: b, to: a },
        TraceEvent::LinkBlocked { from: b, to: c },
        TraceEvent::LinkUnblocked { from: a, to: b },
        TraceEvent::LinkUnblocked { from: b, to: a },
        TraceEvent::LinkUnblocked { from: b, to: c },
        TraceEvent::NodeCrashed { node: b },
        TraceEvent::NodeRestarted { node: b },
    ];
    assert_eq!(ev, expected);
    let started = sim
        .trace()
        .into_iter()
        .find(|r| matches!(r.event, TraceEvent::PartitionStarted { .. }))
        .unwrap();
    assert_eq!(
        started.node, None,
        "a symmetric partition belongs to the simulator, not a node"
    );
}

#[test]
fn every_env_handle_shares_the_node_stream() {
    let mut sim = Sim::new(SimConfig::new(1));
    let n = sim.add_node();
    let first = sim.env(n).rng().next_u64();
    let second = sim.env(n).rng().next_u64();
    let mut fresh = Sim::new(SimConfig::new(1));
    let m = fresh.add_node();
    let env = fresh.env(m);
    assert_eq!(
        (env.rng().next_u64(), env.rng().next_u64()),
        (first, second),
        "two handles continue one stream"
    );
}

#[test]
fn fault_draws_do_not_move_when_the_policy_changes() {
    // D-017: the same seed under both policies produces the same drops and delays.
    let run = |policy: Policy| {
        let mut config = SimConfig::new(3);
        config.net.p_drop = 0.3;
        config.net.delay_min = ms(0);
        config.net.delay_max = ms(10);
        config.policy = Some(policy);
        let (sim, _) = ping_and_count(config, |_, _, _| {});
        sim.trace()
            .into_iter()
            .filter_map(|r| match r.event {
                TraceEvent::MessageDropped { id, reason, .. } => Some((id, reason)),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let uniform = run(Policy::Uniform);
    let pct = run(Policy::Pct { depth: 3 });
    assert!(!uniform.is_empty());
    assert_eq!(uniform, pct);
}

#[test]
fn the_policy_comes_from_the_seed_unless_fixed() {
    assert_eq!(Sim::new(SimConfig::new(0)).policy(), Policy::Uniform);
    assert_eq!(
        Sim::new(SimConfig::new(2)).policy(),
        Policy::Pct { depth: 2 }
    );
    let mut config = SimConfig::new(2);
    config.policy = Some(Policy::Uniform);
    assert_eq!(Sim::new(config).policy(), Policy::Uniform);
    assert_eq!(
        SimConfig::run_length_hint_for(3, Duration::from_millis(1_400)),
        4_200
    );
}

#[test]
fn a_race_never_moves_a_protocol_draw() {
    // D-017: the protocol stream is untouched by however many times race polls.
    let draw_after = |races: u32| {
        let mut sim = Sim::new(SimConfig::new(5));
        let n = sim.add_node();
        let env = sim.env(n);
        let seen: Log<u64> = log();
        let s = seen.clone();
        env.clone().spawn("racer", async move {
            for _ in 0..races {
                let ready = std::pin::pin!(std::future::ready(()));
                let never = std::pin::pin!(std::future::pending::<()>());
                let _ = crate::race(&env, ready, never).await;
            }
            s.lock().unwrap().push(env.rng().next_u64());
        });
        sim.run_until(Instant::ZERO);
        seen.lock().unwrap()[0]
    };
    assert_eq!(draw_after(0), draw_after(50));
    let mut sim = Sim::new(SimConfig::new(5));
    let n = sim.add_node();
    let env = sim.env(n);
    assert_ne!(
        env.rng().next_u64(),
        env.sched_rng().next_u64(),
        "protocol and scheduling streams differ"
    );
}

/// A task that keeps itself runnable without letting virtual time move must fail the
/// run, not hang it.
#[test]
#[should_panic(expected = "busy loop")]
fn a_busy_loop_fails_the_run_instead_of_hanging() {
    let mut config = SimConfig::new(1);
    config.poll_budget = 500;
    let mut sim = Sim::new(config);
    let n = sim.add_node();
    sim.env(n).spawn("spinner", async {
        loop {
            YieldOnce(false).await;
        }
    });
    sim.run_until(Instant::from_nanos(1));
}

/// A step is one poll, or one advance of time when nothing is runnable; a crash after
/// `run_steps` can therefore land between two tasks' polls at one instant.
#[test]
fn run_steps_takes_one_poll_at_a_time_and_advances_time_only_when_idle() {
    let mut sim = Sim::new(SimConfig::new(1));
    let n = sim.add_node();
    let env = sim.env(n);
    let done: Log<&'static str> = log();
    for name in ["a", "b", "c"] {
        let (env, d) = (env.clone(), done.clone());
        env.clone().spawn(name, async move {
            env.clock().sleep(ms(1)).await;
            d.lock().unwrap().push(name);
        });
    }
    // Three spawns are three runnable tasks: three polls register three timers.
    assert_eq!(sim.run_steps(3), 3);
    assert!(done.lock().unwrap().is_empty());
    assert_eq!(sim.now(), Instant::ZERO);
    // Nothing is runnable: the next step advances time to the timers, then one poll
    // completes one task, leaving the other two between polls at the same instant.
    assert_eq!(sim.run_steps(2), 2);
    assert_eq!(sim.now(), Instant::ZERO + ms(1));
    assert_eq!(done.lock().unwrap().len(), 1);
    assert_eq!(sim.run_steps(100), 2, "two polls remained, then nothing");
    assert_eq!(done.lock().unwrap().len(), 3);
}

#[test]
fn the_poll_budget_restarts_when_time_moves() {
    // 400 yields per millisecond for 10 ms is 4000 polls in total, but never more than
    // ~401 at one instant, so a budget of 500 is not exceeded.
    let mut config = SimConfig::new(1);
    config.poll_budget = 500;
    let mut sim = Sim::new(config);
    let n = sim.add_node();
    let env = sim.env(n);
    let done: Log<u32> = log();
    let d = done.clone();
    env.clone().spawn("bursty", async move {
        for _ in 0..10 {
            for _ in 0..400 {
                YieldOnce(false).await;
            }
            env.clock().sleep(ms(1)).await;
        }
        d.lock().unwrap().push(1);
    });
    sim.run_until(Instant::from_nanos(100_000_000));
    assert_eq!(*done.lock().unwrap(), [1]);
}

#[test]
fn bit_rot_flips_one_bit_per_block_at_crash_and_is_traced() {
    let mut config = SimConfig::new(3);
    config.fs.p_bitrot = 1.0;
    config.fs.block_size = 4;
    let mut sim = Sim::new(config);
    let n = sim.add_node();
    let env = sim.env(n);
    env.clone().spawn("writer", async move {
        let f = env.fs().open(Path::new("/f"), open_rw()).await.unwrap();
        f.write_at(0, Bytes::from_static(b"aaaabbbbcc"))
            .await
            .unwrap();
        f.sync().await.unwrap();
        env.fs().sync_dir(Path::new("/")).await.unwrap();
    });
    sim.run_until(Instant::ZERO);
    assert_eq!(
        sim.durable_contents(n, Path::new("/f")).unwrap(),
        b"aaaabbbbcc"
    );
    sim.crash(n);
    let rotted = sim.durable_contents(n, Path::new("/f")).unwrap();
    let original = b"aaaabbbbcc";
    let flipped: Vec<usize> = (0..original.len())
        .filter(|&i| rotted[i] != original[i])
        .collect();
    assert_eq!(
        flipped.len(),
        3,
        "one flipped byte per block, three blocks: {flipped:?}"
    );
    for i in flipped {
        assert_eq!(
            (rotted[i] ^ original[i]).count_ones(),
            1,
            "exactly one bit differs"
        );
    }
    let events: Vec<_> = events(&sim)
        .into_iter()
        .filter(|e| matches!(e, TraceEvent::BlockRotted { .. }))
        .collect();
    assert_eq!(events.len(), 3);
    assert!(
        matches!(&events[0], TraceEvent::BlockRotted { path, block: 0, offset, .. } if path == Path::new("/f") && *offset < 4)
    );
    // What the restarted node reads is the rotted content.
    let env = sim.env(n);
    let seen: Log<Bytes> = log();
    let s = seen.clone();
    env.clone().spawn("reader", async move {
        let f = env
            .fs()
            .open(Path::new("/f"), OpenOptions::new().read(true))
            .await
            .unwrap();
        let contents = f.read_at(0, 100).await.unwrap();
        s.lock().unwrap().push(contents);
    });
    sim.run_until(Instant::ZERO);
    assert_eq!(seen.lock().unwrap()[0][..], rotted[..]);
}

#[test]
fn no_bit_rot_by_default_and_none_on_empty_files() {
    let mut config = SimConfig::new(3);
    config.fs.p_bitrot = 1.0;
    let mut sim = Sim::new(config);
    let n = sim.add_node();
    let env = sim.env(n);
    env.clone().spawn("creator", async move {
        env.fs().open(Path::new("/empty"), open_rw()).await.unwrap();
        env.fs().sync_dir(Path::new("/")).await.unwrap();
    });
    sim.run_until(Instant::ZERO);
    sim.crash(n);
    assert!(
        sim.durable_contents(n, Path::new("/empty"))
            .unwrap()
            .is_empty()
    );
    assert!(
        !events(&sim)
            .iter()
            .any(|e| matches!(e, TraceEvent::BlockRotted { .. }))
    );
    let default = SimConfig::new(1);
    assert_eq!(default.fs.p_bitrot, 0.0);
    assert_eq!(default.fs.block_size, 4096);
}

#[test]
fn a_rename_without_sync_dir_may_be_lost_at_crash_but_never_both_or_neither() {
    let mut old_seen = 0;
    let mut new_seen = 0;
    for seed in 0..64 {
        let mut sim = Sim::new(SimConfig::new(seed));
        let n = sim.add_node();
        let env = sim.env(n);
        env.clone().spawn("renamer", async move {
            let fs = env.fs();
            let f = fs.open(Path::new("/a"), open_rw()).await.unwrap();
            f.write_at(0, Bytes::from_static(b"data")).await.unwrap();
            f.sync().await.unwrap();
            fs.sync_dir(Path::new("/")).await.unwrap();
            fs.rename(Path::new("/a"), Path::new("/b")).await.unwrap();
            // no sync_dir: the rename may be lost
        });
        sim.run_until(Instant::ZERO);
        sim.crash(n);
        let a = sim.durable_contents(n, Path::new("/a"));
        let b = sim.durable_contents(n, Path::new("/b"));
        let old_name_kept = a.is_some();
        match (a, b) {
            (Some(bytes), None) => {
                old_seen += 1;
                assert_eq!(bytes, b"data");
                assert!(events(&sim).iter().any(|e| matches!(e, TraceEvent::DirectoryEntryLost { entry, op: DirEntryOp::Rename, .. } if entry == Path::new("/b"))));
            }
            (None, Some(bytes)) => {
                new_seen += 1;
                assert_eq!(bytes, b"data");
                assert!(
                    !events(&sim)
                        .iter()
                        .any(|e| matches!(e, TraceEvent::DirectoryEntryLost { .. }))
                );
            }
            other => {
                panic!("seed {seed}: the file must exist under exactly one name, got {other:?}")
            }
        }
        // The restarted node sees the same namespace.
        let env = sim.env(n);
        let listed: Log<Vec<std::path::PathBuf>> = log();
        let l = listed.clone();
        env.clone().spawn("lister", async move {
            let names = env.fs().read_dir(Path::new("/")).await.unwrap();
            l.lock().unwrap().push(names);
        });
        sim.run_until(Instant::ZERO);
        let names = listed.lock().unwrap()[0].clone();
        assert_eq!(names.len(), 1);
        assert_eq!(
            names[0].to_str().unwrap(),
            if old_name_kept { "a" } else { "b" }
        );
    }
    assert!(
        old_seen > 0 && new_seen > 0,
        "64 seeds: old {old_seen}, new {new_seen}"
    );
}

#[test]
fn sync_dir_makes_entries_durable_and_an_unsynced_create_may_vanish() {
    let mut vanished = 0;
    for seed in 0..32 {
        let mut sim = Sim::new(SimConfig::new(seed));
        let n = sim.add_node();
        let env = sim.env(n);
        env.clone().spawn("creator", async move {
            let fs = env.fs();
            fs.open(Path::new("/synced"), open_rw()).await.unwrap();
            fs.sync_dir(Path::new("/")).await.unwrap();
            fs.open(Path::new("/unsynced"), open_rw()).await.unwrap();
        });
        sim.run_until(Instant::ZERO);
        sim.crash(n);
        assert!(
            sim.durable_contents(n, Path::new("/synced")).is_some(),
            "seed {seed}: a synced entry survives"
        );
        if sim.durable_contents(n, Path::new("/unsynced")).is_none() {
            vanished += 1;
            assert!(events(&sim).iter().any(|e| matches!(
                e,
                TraceEvent::DirectoryEntryLost {
                    op: DirEntryOp::Link,
                    ..
                }
            )));
        }
    }
    assert!(
        vanished > 0,
        "an unsynced create never vanished in 32 seeds"
    );
}

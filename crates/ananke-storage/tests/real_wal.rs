//! The write-ahead log on a real disk (SPEC.md §9.3): the same code the simulator
//! crashes, appending and recovering through `RealEnv`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::{Clock, Environment, RealEnv};
use ananke_storage::{Variant, Wal, WalConfig};
use bytes::Bytes;

#[test]
fn appends_and_recovers_on_a_real_disk() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        dir: dir.path().join("wal"),
        segment_bytes: 4096,
        variant: Variant::Correct,
        expected_head: 1,
    };
    // Five appenders at once, so the writer groups their records under shared fsyncs;
    // every record still comes back, in the order the log assigned.
    let payloads: Vec<Bytes> = (0..200u32)
        .map(|i| Bytes::from(format!("record {i}: {}", "x".repeat((i % 50) as usize))))
        .collect();
    let c = config.clone();
    let order = RealEnv::run(|env| async move {
        let (wal, recovery) = Wal::open(env.clone(), c).await.unwrap();
        assert!(recovery.records.is_empty());
        let wal = Arc::new(wal);
        let order: Arc<Mutex<Vec<(u64, Bytes)>>> = Arc::default();
        let mut tasks = Vec::new();
        for chunk in payloads.chunks(40) {
            let (wal, order, chunk) = (wal.clone(), order.clone(), chunk.to_vec());
            let done: Arc<Mutex<bool>> = Arc::default();
            let d = done.clone();
            env.spawn("appender", async move {
                for payload in chunk {
                    let seq = wal.append(payload.clone()).await.unwrap();
                    order.lock().unwrap().push((seq, payload));
                }
                *d.lock().unwrap() = true;
            });
            tasks.push(done);
        }
        while !tasks.iter().all(|d| *d.lock().unwrap()) {
            env.clock().sleep(Duration::from_millis(5)).await;
        }
        let mut order = order.lock().unwrap().clone();
        order.sort_unstable_by_key(|(seq, _)| *seq);
        order
    });
    assert_eq!(
        order.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
        (1..=200).collect::<Vec<_>>()
    );
    let expected: Vec<Bytes> = order.into_iter().map(|(_, payload)| payload).collect();
    RealEnv::run(|env| async move {
        let (_wal, recovery) = Wal::open(env, config).await.unwrap();
        assert_eq!(recovery.records, expected);
        assert_eq!(
            (recovery.stop, recovery.discarded, recovery.next_seq),
            (None, 0, 201)
        );
    });
}

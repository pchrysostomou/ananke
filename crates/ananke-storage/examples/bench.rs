//! The Phase 1 sanity bench (SPEC.md §2.8): single-threaded writes on the real
//! environment, reported as writes per second. Not a target, a number to know.
//!
//! ```sh
//! cargo run --release -p ananke-storage --example bench -- [writes]
//! ```
//!
//! Three shapes: one put per write with a sync each, the same without a sync, and
//! batches of a hundred puts without a sync, each followed by one synced write so
//! everything is durable before the clock stops.

use ananke_env::{Clock, Environment};
use ananke_storage::{Engine, EngineConfig, WriteBatch};
use bytes::Bytes;

fn main() {
    let writes: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let dir = tempfile::tempdir().expect("a temporary directory");
    let value = Bytes::from(vec![b'v'; 100]);
    let config = EngineConfig::new(dir.path().to_path_buf());
    ananke_env::RealEnv::run(|env| async move {
        let clock = env.clock();
        let (db, _) = Engine::open(env.clone(), config)
            .await
            .expect("the store opens");
        let key = |i: u64| Bytes::from(format!("key-{i:016}"));

        let synced = writes / 20;
        let start = clock.now();
        for i in 0..synced {
            db.put(key(i), value.clone()).await.expect("put");
        }
        report("one put per write, synced", synced, clock.now() - start);

        let start = clock.now();
        for i in 0..writes {
            let mut batch = WriteBatch::new();
            batch.put(key(i), value.clone());
            db.write(batch, false).await.expect("write");
        }
        db.put(key(writes), value.clone()).await.expect("put");
        report("one put per write, no sync", writes, clock.now() - start);

        let start = clock.now();
        for chunk in 0..writes / 100 {
            let mut batch = WriteBatch::new();
            for i in 0..100 {
                batch.put(key(chunk * 100 + i), value.clone());
            }
            db.write(batch, false).await.expect("write");
        }
        db.put(key(writes), value.clone()).await.expect("put");
        report(
            "batches of a hundred puts, no sync",
            writes,
            clock.now() - start,
        );
    });
}

fn report(what: &str, writes: u64, took: std::time::Duration) {
    let rate = writes as f64 / took.as_secs_f64();
    println!("{what}: {writes} writes in {took:.2?}, {rate:.0} writes/s");
}

//! Real-environment integration tests: few and slow by design (SPEC.md §9.3). They reach
//! the real clock and disk only through `RealEnv`.

use std::path::PathBuf;
use std::time::Duration;

use ananke_env::{Clock, Environment, File, FileSystem, OpenOptions, RealEnv, Rng, det_hash_map};
use bytes::Bytes;

#[test]
fn clock_is_monotonic_and_timers_wait() {
    RealEnv::run(|env| async move {
        let clock = env.clock();
        let a = clock.now();
        let b = clock.now();
        assert!(b >= a);

        let start = clock.now();
        clock.sleep(Duration::from_millis(20)).await;
        assert!(clock.now() - start >= Duration::from_millis(20));

        // sleep_until in the past resolves immediately.
        clock.sleep_until(start).await;

        // The wall clock is after 2020-01-01T00:00:00Z.
        assert!(clock.wall().as_unix_nanos() > 1_577_836_800 * 1_000_000_000);
    });
}

#[test]
fn filesystem_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    RealEnv::run(|env| async move {
        let fs = env.fs();
        let sub = dir.path().join("a").join("b");
        fs.create_dir_all(&sub).await.unwrap();
        fs.create_dir_all(&sub).await.unwrap(); // idempotent

        let path = sub.join("log");
        let rw = OpenOptions::new().read(true).write(true).create_new(true);
        let file = fs.open(&path, rw).await.unwrap();
        file.write_at(0, Bytes::from_static(b"hello "))
            .await
            .unwrap();
        file.write_at(6, Bytes::from_static(b"world"))
            .await
            .unwrap();
        file.sync().await.unwrap();

        assert_eq!(file.size().await.unwrap(), 11);
        assert_eq!(
            file.read_at(0, 11).await.unwrap(),
            Bytes::from_static(b"hello world")
        );
        assert_eq!(
            file.read_at(6, 100).await.unwrap(),
            Bytes::from_static(b"world"),
            "short read at EOF"
        );
        assert!(file.read_at(11, 4).await.unwrap().is_empty());

        file.set_size(5).await.unwrap();
        assert_eq!(
            file.read_at(0, 11).await.unwrap(),
            Bytes::from_static(b"hello")
        );
        file.set_size(7).await.unwrap();
        assert_eq!(
            file.read_at(0, 11).await.unwrap(),
            Bytes::from_static(b"hello\0\0")
        );

        // create_new refuses to clobber.
        let err = fs.open(&path, rw).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

        // Directory listing is sorted regardless of creation order.
        for name in ["c", "a", "b"] {
            fs.open(&sub.join(name), OpenOptions::new().write(true).create(true))
                .await
                .unwrap();
        }
        let listed = fs.read_dir(&sub).await.unwrap();
        assert_eq!(listed, ["a", "b", "c", "log"].map(PathBuf::from));

        let renamed = sub.join("log.1");
        fs.rename(&path, &renamed).await.unwrap();
        fs.sync_dir(&sub).await.unwrap();
        let reopened = fs
            .open(&renamed, OpenOptions::new().read(true))
            .await
            .unwrap();
        assert_eq!(
            reopened.read_at(0, 5).await.unwrap(),
            Bytes::from_static(b"hello")
        );

        for name in ["a", "b", "c", "log.1"] {
            fs.remove_file(&sub.join(name)).await.unwrap();
        }
        assert!(fs.read_dir(&sub).await.unwrap().is_empty());
        let err = fs
            .open(&renamed, OpenOptions::new().read(true))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    });
}

#[test]
fn rng_draws_fresh_entropy_and_seeds_maps() {
    RealEnv::run(|env| async move {
        let rng = env.rng();
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        rng.fill_bytes(&mut a);
        rng.fill_bytes(&mut b);
        assert_ne!(a, b);
        assert!(rng.below(10) < 10);

        let mut map = det_hash_map(rng);
        map.insert("k", 1);
        assert_eq!(map.get("k"), Some(&1));
    });
}

#[test]
fn spawn_runs_tasks_and_abort_cancels_them() {
    RealEnv::run(|env| async move {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = env.spawn("answer", async move {
            tx.send(42).ok();
        });
        assert_eq!(handle.name(), "answer");
        assert_eq!(rx.await.unwrap(), 42);

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let inner = env.clone();
        let sleeper = env.spawn("sleeper", async move {
            inner.clock().sleep(Duration::from_secs(60)).await;
            tx.send(()).ok();
        });
        assert!(sleeper.id() > handle.id());
        sleeper.abort();
        assert!(rx.await.is_err(), "aborted task must not reach its send");
    });
}

//! SSTables on the simulated disk: round trips, prefix compression across blocks,
//! tombstones, and what a flipped bit or a torn tail does.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig};
use ananke_env::{Environment, File, FileSystem, OpenOptions};
use ananke_storage::Value;
use ananke_storage::sst::{SstReader, SstWriter};
use bytes::Bytes;

fn value(i: u32) -> Value {
    if i % 7 == 3 {
        Value::Tombstone
    } else {
        Value::Live(Bytes::from(format!(
            "value-{i}-{}",
            "x".repeat((i % 90) as usize)
        )))
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("user/table/{:08}/column", i * 3).into_bytes()
}

/// Writes `bytes` to `/t.sst`, corrupts it with `mangle`, and opens it.
fn with_table<T: Send + 'static>(
    seed: u64,
    bytes: Bytes,
    mangle: impl FnOnce(&mut Vec<u8>) + Send + 'static,
    check: impl FnOnce(
        std::io::Result<SstReader<ananke_env::sim::SimFile>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = T> + Send>>
    + Send
    + 'static,
) -> T {
    let mut sim = Sim::new(SimConfig::new(seed));
    let node = sim.add_node();
    let env = sim.env(node);
    let out: Arc<Mutex<Option<T>>> = Arc::default();
    let o = out.clone();
    env.clone().spawn("test", async move {
        let fs = env.fs();
        let mut data = bytes.to_vec();
        mangle(&mut data);
        let file = fs
            .open(
                Path::new("/t.sst"),
                OpenOptions::new().read(true).write(true).create_new(true),
            )
            .await
            .unwrap();
        file.write_at(0, Bytes::from(data)).await.unwrap();
        file.sync().await.unwrap();
        let reader = SstReader::open(file).await;
        *o.lock().unwrap() = Some(check(reader).await);
    });
    sim.run_for(Duration::from_millis(1));
    out.lock().unwrap().take().expect("the task finished")
}

use std::future::Future;

fn table(n: u32) -> Bytes {
    let mut writer = SstWriter::new();
    for i in 0..n {
        writer.add(&key(i), &value(i));
    }
    writer.finish()
}

#[test]
fn every_key_comes_back_and_absent_keys_do_not() {
    let (blocks, entries) = with_table(
        1,
        table(3000),
        |_| {},
        |reader| {
            Box::pin(async move {
                let reader = reader.unwrap();
                reader.verify().await.unwrap();
                for i in 0..3000 {
                    assert_eq!(
                        reader.get(&key(i)).await.unwrap(),
                        Some(value(i)),
                        "key {i}"
                    );
                }
                for i in 0..3000u32 {
                    let absent = format!("user/table/{:08}/column", i * 3 + 1);
                    assert_eq!(reader.get(absent.as_bytes()).await.unwrap(), None);
                }
                assert_eq!(reader.get(b"").await.unwrap(), None);
                assert_eq!(reader.get(b"zzz").await.unwrap(), None);
                (reader.blocks(), reader.entries())
            })
        },
    );
    assert!(blocks > 20, "3000 entries span many blocks: {blocks}");
    assert_eq!(entries, 3000);
}

#[test]
fn an_empty_table_opens_and_holds_nothing() {
    with_table(
        2,
        table(0),
        |_| {},
        |reader| {
            Box::pin(async move {
                let reader = reader.unwrap();
                reader.verify().await.unwrap();
                assert_eq!(reader.get(b"a").await.unwrap(), None);
                assert_eq!((reader.blocks(), reader.entries()), (0, 0));
            })
        },
    );
}

#[test]
fn a_flipped_bit_in_a_data_block_fails_verify_and_the_read_that_hits_it() {
    with_table(
        3,
        table(500),
        |data| data[100] ^= 0x04,
        |reader| {
            Box::pin(async move {
                let reader = reader.unwrap();
                assert!(reader.verify().await.is_err());
                // The first block is the one with the flip; a key in it fails, a later one reads.
                assert!(reader.get(&key(0)).await.is_err());
                assert_eq!(reader.get(&key(499)).await.unwrap(), Some(value(499)));
            })
        },
    );
}

#[test]
fn a_torn_tail_or_a_flipped_footer_bit_does_not_open() {
    with_table(
        4,
        table(100),
        |data| data.truncate(data.len() - 20),
        |reader| {
            Box::pin(async move {
                assert!(reader.is_err());
            })
        },
    );
    with_table(
        5,
        table(100),
        |data| {
            let n = data.len();
            data[n - 10] ^= 1;
        },
        |reader| {
            Box::pin(async move {
                assert!(reader.is_err());
            })
        },
    );
    with_table(
        6,
        table(100),
        |data| {
            let n = data.len();
            data[n - 60] ^= 1;
        },
        |reader| {
            Box::pin(async move {
                // A flip in the index block.
                assert!(reader.is_err());
            })
        },
    );
}

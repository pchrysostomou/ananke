//! The write-ahead log under the simulator without faults: framing, recovery's stop
//! rule, group commit. The crash sweep with every fault on is `sim/wal.rs`.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig};
use ananke_env::{Environment, File, FileSystem, OpenOptions, TraceEvent, WalStop, WalStopReason};
use ananke_storage::wal::{HEADER_LEN, encode_record, segment_path};
use ananke_storage::{HeadGap, HeadGapPolicy, Recovery, Variant, Wal, WalConfig};
use bytes::Bytes;

const DIR: &str = "/wal";

fn config(variant: Variant) -> WalConfig {
    WalConfig {
        dir: DIR.into(),
        segment_bytes: 256,
        variant,
        expected_head: 1,
        head_gap: HeadGapPolicy::Discard,
    }
}

type Out<T> = Arc<Mutex<Option<T>>>;

fn out<T>() -> Out<T> {
    Arc::new(Mutex::new(None))
}

fn take<T>(out: &Out<T>) -> T {
    out.lock().unwrap().take().expect("the task finished")
}

/// Writes `bytes` as segment `segment`, durably.
async fn write_segment<E: Environment>(env: &E, segment: u64, bytes: Vec<u8>) {
    let fs = env.fs();
    fs.create_dir_all(Path::new(DIR)).await.unwrap();
    let file = fs
        .open(
            &segment_path(Path::new(DIR), segment),
            OpenOptions::new().write(true).create_new(true),
        )
        .await
        .unwrap();
    file.write_at(0, Bytes::from(bytes)).await.unwrap();
    file.sync().await.unwrap();
    fs.sync_dir(Path::new(DIR)).await.unwrap();
}

/// Records numbered from `first`.
fn records(first: u64, payloads: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (i, payload) in payloads.iter().enumerate() {
        encode_record(&mut buf, first + i as u64, payload);
    }
    buf
}

fn payload(i: u64) -> Bytes {
    Bytes::from(vec![
        u8::try_from(i % 251).unwrap();
        usize::try_from(i % 40).unwrap()
    ])
}

#[test]
fn an_empty_directory_recovers_nothing_and_starts_segment_one() {
    let mut sim = Sim::new(SimConfig::new(1));
    let node = sim.add_node();
    let env = sim.env(node);
    let result: Out<(Recovery, Vec<std::path::PathBuf>)> = out();
    let r = result.clone();
    env.clone().spawn("open", async move {
        let (_wal, recovery) = Wal::open(env.clone(), config(Variant::Correct))
            .await
            .unwrap();
        let names = env.fs().read_dir(Path::new(DIR)).await.unwrap();
        *r.lock().unwrap() = Some((recovery, names));
    });
    sim.run_for(Duration::from_millis(1));
    let (recovery, names) = take(&result);
    assert_eq!(
        recovery,
        Recovery {
            first_seq: 1,
            head_gap: None,
            covered_stops: vec![],
            records: vec![],
            stop: None,
            discarded: 0,
            next_seq: 1
        }
    );
    assert_eq!(names, vec![std::path::PathBuf::from("000001.wal")]);
}

#[test]
fn records_come_back_in_order_after_reopen_across_segments() {
    let mut sim = Sim::new(SimConfig::new(2));
    let node = sim.add_node();
    let env = sim.env(node);
    let seqs: Out<Vec<u64>> = out();
    let s = seqs.clone();
    env.clone().spawn("writer", async move {
        let (wal, _) = Wal::open(env, config(Variant::Correct)).await.unwrap();
        let mut got = Vec::new();
        for i in 0..60 {
            let append = wal.append(payload(i));
            assert_eq!(append.seq(), i + 1, "the number is known at enqueue");
            got.push(append.await.unwrap());
        }
        *s.lock().unwrap() = Some(got);
    });
    sim.run_for(Duration::from_millis(1));
    assert_eq!(take(&seqs), (1..=60).collect::<Vec<_>>());
    let opened = sim
        .trace()
        .iter()
        .filter(|r| matches!(r.event, TraceEvent::WalSegmentOpened { .. }))
        .count();
    assert!(
        opened > 2,
        "60 records of up to 40 bytes span several 256-byte segments, got {opened}"
    );

    let env = sim.env(node);
    let result: Out<Recovery> = out();
    let r = result.clone();
    env.clone().spawn("reader", async move {
        let (_wal, recovery) = Wal::open(env, config(Variant::Correct)).await.unwrap();
        *r.lock().unwrap() = Some(recovery);
    });
    sim.run_for(Duration::from_millis(1));
    let recovery = take(&result);
    assert_eq!(recovery.records, (0..60).map(payload).collect::<Vec<_>>());
    assert_eq!(
        (recovery.stop, recovery.discarded, recovery.next_seq),
        (None, 0, 61)
    );
}

#[test]
fn concurrent_appenders_share_syncs() {
    let mut fewer_syncs_than_records = 0;
    for seed in 0..20 {
        let mut sim = Sim::new(SimConfig::new(seed));
        let node = sim.add_node();
        let env = sim.env(node);
        let seqs: Out<Vec<u64>> = Arc::new(Mutex::new(Some(Vec::new())));
        let opened: Out<Arc<Wal<_>>> = out();
        let o = opened.clone();
        env.clone().spawn("open", async move {
            let (wal, _) = Wal::open(env, config(Variant::Correct)).await.unwrap();
            *o.lock().unwrap() = Some(Arc::new(wal));
        });
        sim.run_for(Duration::from_millis(1));
        let wal = take(&opened);
        for _ in 0..4 {
            let (wal, seqs) = (wal.clone(), seqs.clone());
            sim.env(node).spawn("appender", async move {
                for i in 0..8 {
                    let seq = wal.append(payload(i)).await.unwrap();
                    seqs.lock().unwrap().as_mut().unwrap().push(seq);
                }
            });
        }
        drop(wal);
        sim.run_for(Duration::from_millis(1));
        let mut got = take(&seqs);
        got.sort_unstable();
        assert_eq!(got, (1..=32).collect::<Vec<_>>(), "seed {seed}");
        let syncs = sim
            .trace()
            .iter()
            .filter(|r| matches!(r.event, TraceEvent::WalSynced { .. }))
            .count();
        assert!(syncs <= 32, "seed {seed}: {syncs} syncs for 32 records");
        fewer_syncs_than_records += usize::from(syncs < 32);
    }
    assert!(
        fewer_syncs_than_records > 0,
        "no seed ever grouped two records under one sync"
    );
}

/// Recovers with `variant` after the segments were written by hand; the log is
/// dropped at once so the directory shows recovery's cleanup.
fn recover(sim: &mut Sim, node: ananke_env::NodeId, variant: Variant) -> (Recovery, Vec<String>) {
    let env = sim.env(node);
    let result: Out<(Recovery, Vec<String>)> = out();
    let r = result.clone();
    env.clone().spawn("recover", async move {
        let (wal, recovery) = Wal::open(env.clone(), config(variant)).await.unwrap();
        drop(wal);
        let names = env.fs().read_dir(Path::new(DIR)).await.unwrap();
        let names = names.iter().map(|n| n.display().to_string()).collect();
        *r.lock().unwrap() = Some((recovery, names));
    });
    sim.run_for(Duration::from_millis(1));
    take(&result)
}

#[test]
fn recovery_stops_at_a_torn_record_cuts_it_and_discards_later_segments() {
    let mut sim = Sim::new(SimConfig::new(3));
    let node = sim.add_node();
    let env = sim.env(node);
    env.clone().spawn("setup", async move {
        let mut one = records(1, &[b"alpha", b"beta"]);
        let good = one.len() as u64;
        one.extend_from_slice(&records(3, &[b"gamma"])[..HEADER_LEN + 2]);
        write_segment(&env, 1, one).await;
        write_segment(&env, 2, records(3, &[b"delta"])).await;
        assert_eq!(good, 2 * HEADER_LEN as u64 + 9);
    });
    sim.run_for(Duration::from_millis(1));
    let (recovery, names) = recover(&mut sim, node, Variant::Correct);
    assert_eq!(
        recovery.records,
        vec![Bytes::from("alpha"), Bytes::from("beta")]
    );
    assert_eq!(
        recovery.stop,
        Some(WalStop {
            segment: 1,
            offset: 2 * HEADER_LEN as u64 + 9,
            reason: WalStopReason::TornRecord
        })
    );
    assert_eq!((recovery.discarded, recovery.next_seq), (1, 3));
    // Segment 1 was cut; segment 2 was discarded and the fresh segment numbered past
    // it, since a segment number is never reused.
    assert_eq!(names, ["000001.wal", "000003.wal"]);
    assert_eq!(
        sim.durable_contents(node, &segment_path(Path::new(DIR), 1))
            .map(|b| b.len()),
        Some(2 * HEADER_LEN + 9)
    );
    assert_eq!(
        sim.durable_contents(node, &segment_path(Path::new(DIR), 3))
            .map(|b| b.len()),
        Some(0)
    );
    assert!(sim.trace().iter().any(|r| r.event
        == TraceEvent::WalTruncated {
            segment: 1,
            len: 2 * HEADER_LEN as u64 + 9
        }));
    // A second recovery is clean.
    let (again, _) = recover(&mut sim, node, Variant::Correct);
    assert_eq!(again.records.len(), 2);
    assert_eq!((again.stop, again.discarded, again.next_seq), (None, 0, 3));
}

#[test]
fn recovery_stops_at_a_bad_checksum_unless_the_variant_skips_it() {
    for variant in [Variant::Correct, Variant::NoChecksum] {
        let mut sim = Sim::new(SimConfig::new(4));
        let node = sim.add_node();
        let env = sim.env(node);
        env.clone().spawn("setup", async move {
            let mut one = records(1, &[b"alpha", b"beta", b"gamma"]);
            // Flip one bit of "beta"'s payload.
            let beta = HEADER_LEN + 5 + HEADER_LEN;
            one[beta] ^= 0x10;
            write_segment(&env, 1, one).await;
        });
        sim.run_for(Duration::from_millis(1));
        let (recovery, _) = recover(&mut sim, node, variant);
        match variant {
            Variant::Correct => {
                assert_eq!(recovery.records, vec![Bytes::from("alpha")]);
                assert_eq!(
                    recovery.stop,
                    Some(WalStop {
                        segment: 1,
                        offset: HEADER_LEN as u64 + 5,
                        reason: WalStopReason::BadChecksum
                    })
                );
            }
            _ => {
                // The bug: the flipped byte comes back as data.
                assert_eq!(
                    recovery.records,
                    vec![
                        Bytes::from("alpha"),
                        Bytes::from("reta"),
                        Bytes::from("gamma")
                    ]
                );
                assert_eq!(recovery.stop, None);
            }
        }
    }
}

/// A hole in the segment numbers is not a stop while the records stay contiguous:
/// a discarded segment's number is never reused, so holes are normal. A segment that
/// went missing with records in it shows as a gap at the next segment's first record
/// (`recovery_stops_at_a_gap_in_the_numbering`).
#[test]
fn a_hole_in_the_segment_numbers_is_not_a_stop_while_the_records_are_contiguous() {
    let mut sim = Sim::new(SimConfig::new(5));
    let node = sim.add_node();
    let env = sim.env(node);
    env.clone().spawn("setup", async move {
        write_segment(&env, 1, records(1, &[b"alpha"])).await;
        write_segment(&env, 3, records(2, &[b"gamma"])).await;
    });
    sim.run_for(Duration::from_millis(1));
    let (recovery, names) = recover(&mut sim, node, Variant::Correct);
    assert_eq!(
        recovery.records,
        vec![Bytes::from("alpha"), Bytes::from("gamma")]
    );
    assert_eq!(recovery.stop, None);
    assert_eq!((recovery.discarded, recovery.next_seq), (0, 3));
    // The fresh segment is numbered past the highest one held.
    assert_eq!(names, ["000001.wal", "000003.wal", "000004.wal"]);
}

/// A first record past the expected head is a missing head: refused, the open fails
/// and touches nothing; discarded, the whole log goes and a fresh segment starts at
/// the head (D-022).
#[test]
fn a_missing_head_is_refused_or_discards_the_log() {
    let mut sim = Sim::new(SimConfig::new(7));
    let node = sim.add_node();
    let env = sim.env(node);
    env.clone().spawn("setup", async move {
        write_segment(&env, 1, records(3, &[b"gamma", b"delta"])).await;
        write_segment(&env, 2, records(5, &[b"epsilon"])).await;
    });
    sim.run_for(Duration::from_millis(1));
    /// What an open produced: the recovery, or the head gap the error carried, and
    /// the directory listing after it.
    type Outcome = (Result<Recovery, Option<HeadGap>>, Vec<String>);
    let open = |sim: &mut Sim, policy: HeadGapPolicy| {
        let env = sim.env(node);
        let result: Out<Outcome> = out();
        let r = result.clone();
        env.clone().spawn("open", async move {
            let mut config = config(Variant::Correct);
            config.head_gap = policy;
            let outcome = match Wal::open(env.clone(), config).await {
                Ok((wal, recovery)) => {
                    drop(wal);
                    Ok(recovery)
                }
                Err(e) => Err(HeadGap::from_io(&e)),
            };
            let names = env.fs().read_dir(Path::new(DIR)).await.unwrap();
            let names = names.iter().map(|n| n.display().to_string()).collect();
            *r.lock().unwrap() = Some((outcome, names));
        });
        sim.run_for(Duration::from_millis(1));
        take(&result)
    };
    let (refused, names) = open(&mut sim, HeadGapPolicy::Refuse);
    assert_eq!(
        refused.unwrap_err(),
        Some(HeadGap {
            expected: 1,
            found: 3
        })
    );
    assert_eq!(names, ["000001.wal", "000002.wal"]);
    assert_eq!(
        sim.durable_contents(node, &segment_path(Path::new(DIR), 1))
            .map(|b| b.len()),
        Some(2 * HEADER_LEN + 10),
        "nothing was cut"
    );
    let (discarded, names) = open(&mut sim, HeadGapPolicy::Discard);
    let recovery = discarded.unwrap();
    assert_eq!(recovery.head_gap, Some((1, 3)));
    assert!(recovery.records.is_empty());
    assert_eq!(
        recovery.stop,
        Some(WalStop {
            segment: 1,
            offset: 0,
            reason: WalStopReason::Gap {
                expected: 1,
                found: 3
            }
        })
    );
    // Every segment is removed rather than cut, so a lost sync cannot bring the
    // old records back; the fresh segment is numbered past them all.
    assert_eq!((recovery.discarded, recovery.next_seq), (2, 1));
    assert_eq!(names, ["000003.wal"]);
    let gaps: Vec<(u64, u64, bool)> = sim
        .trace()
        .iter()
        .filter_map(|r| match r.event {
            TraceEvent::HeadGap {
                expected,
                found,
                discarded,
            } => Some((expected, found, discarded)),
            _ => None,
        })
        .collect();
    assert_eq!(gaps, [(1, 3, false), (1, 3, true)]);
}

/// A segment that ends early because the sync of its last group was lost, followed
/// by an intact next segment, reads as a hole; the sequence numbers catch it.
#[test]
fn recovery_stops_at_a_gap_in_the_numbering() {
    let mut sim = Sim::new(SimConfig::new(6));
    let node = sim.add_node();
    let env = sim.env(node);
    env.clone().spawn("setup", async move {
        write_segment(&env, 1, records(1, &[b"alpha", b"beta"])).await;
        // Record 3 never reached the disk; segment 2 starts at 4.
        write_segment(&env, 2, records(4, &[b"delta", b"epsilon"])).await;
    });
    sim.run_for(Duration::from_millis(1));
    let (recovery, names) = recover(&mut sim, node, Variant::Correct);
    assert_eq!(
        recovery.records,
        vec![Bytes::from("alpha"), Bytes::from("beta")]
    );
    assert_eq!(
        recovery.stop,
        Some(WalStop {
            segment: 2,
            offset: 0,
            reason: WalStopReason::Gap {
                expected: 3,
                found: 4
            }
        })
    );
    assert_eq!((recovery.discarded, recovery.next_seq), (0, 3));
    // Segment 2 was cut to nothing and reopened fresh as the next segment.
    assert_eq!(names, ["000001.wal", "000002.wal", "000003.wal"]);
}

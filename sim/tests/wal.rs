//! The Phase 1 crash-injection property (SPEC.md §2.8) for the write-ahead log, as
//! the sweep CLAUDE.md asks of every fault-model test: the correct log passes every
//! seed with every §1.3 fault on, and each known-buggy variant is caught.

use std::path::Path;
use std::time::Duration;

use ananke_env::{Environment, File, FileSystem, OpenOptions, RealEnv, TraceEvent, WalStopReason};
use ananke_sim::wal::{self, Excuse};
use ananke_storage::Variant;
use bytes::Bytes;

const SEEDS: u64 = 100;

/// Two runs with the same seed produce byte-identical traces.
#[test]
fn same_seed_gives_byte_identical_trace() {
    let first = wal::run(42, Variant::Correct);
    let second = wal::run(42, Variant::Correct);
    assert_eq!(first.jsonl.as_bytes(), second.jsonl.as_bytes());
    assert_ne!(first.jsonl, wal::run(43, Variant::Correct).jsonl);
}

/// The seed-42 trace is written for the studio.
#[test]
fn the_seed_42_trace_is_written_for_the_studio() {
    let report = wal::run(42, Variant::Correct);
    report.check().unwrap();
    let jsonl = report.jsonl.clone();
    RealEnv::run(|env| async move {
        let fs = env.fs();
        fs.create_dir_all(Path::new("out")).await.unwrap();
        let file = fs
            .open(
                Path::new("out/wal-42.jsonl"),
                OpenOptions::new().write(true).create(true).truncate(true),
            )
            .await
            .unwrap();
        file.write_at(0, Bytes::from(jsonl)).await.unwrap();
        file.sync().await.unwrap();
    });
}

/// The positive control: the correct log satisfies the property on every seed, and
/// the sweep actually reached every fault and used every excuse. The negative
/// controls: each known bug is caught on some seed.
#[test]
fn the_correct_log_passes_every_seed_and_every_bug_is_caught() {
    let mut coverage = Coverage::default();
    let mut violations = Vec::new();
    for seed in 0..SEEDS {
        let report = wal::run(seed, Variant::Correct);
        coverage.add(&report);
        if let Err(violation) = report.check() {
            violations.push(violation);
        }
    }
    eprintln!("Correct: {coverage:?}");
    assert!(violations.is_empty(), "{violations:#?}");
    coverage.assert_complete();

    for variant in [
        Variant::NoSyncDir,
        Variant::NoChecksum,
        Variant::AckBeforeSync {
            interval: Duration::from_millis(2),
        },
    ] {
        let mut caught = Vec::new();
        for seed in 0..SEEDS {
            if let Err(violation) = wal::run(seed, variant).check() {
                caught.push(violation);
            }
        }
        eprintln!(
            "{variant:?}: caught on {} of {SEEDS} seeds, first: {}",
            caught.len(),
            caught.first().map_or("", String::as_str)
        );
        assert!(!caught.is_empty(), "{variant:?} was never caught");
    }
}

/// What the correct log's sweep saw.
#[derive(Debug, Default)]
struct Coverage {
    seeds: u32,
    epochs: u32,
    records: usize,
    torn_writes: u32,
    lost_fsyncs: u32,
    bit_rot: u32,
    lost_entries: u32,
    stops_torn: u32,
    stops_bad_checksum: u32,
    stops_missing: u32,
    discarded: u32,
    excused_lost_fsync: u32,
    excused_bit_rot: u32,
    excused_betrayed_cut: u32,
}

impl Coverage {
    fn add(&mut self, report: &wal::Report) {
        self.seeds += 1;
        self.epochs += report.epochs.len() as u32;
        self.records += report.appended();
        self.torn_writes += u32::from(report.has(|e| matches!(e, TraceEvent::WriteTorn { .. })));
        self.lost_fsyncs += u32::from(report.has(|e| matches!(e, TraceEvent::FsyncLost { .. })));
        self.bit_rot += u32::from(report.has(|e| matches!(e, TraceEvent::BlockRotted { .. })));
        self.lost_entries +=
            u32::from(report.has(|e| matches!(e, TraceEvent::DirectoryEntryLost { .. })));
        for epoch in &report.epochs {
            match epoch.recovery.stop.map(|s| s.reason) {
                Some(WalStopReason::TornRecord) => self.stops_torn += 1,
                Some(WalStopReason::BadChecksum) => self.stops_bad_checksum += 1,
                Some(WalStopReason::MissingSegment) => self.stops_missing += 1,
                _ => {}
            }
            self.discarded += u32::from(epoch.recovery.discarded > 0);
            match epoch.excuse {
                Some(Excuse::LostFsync) => self.excused_lost_fsync += 1,
                Some(Excuse::BitRot) => self.excused_bit_rot += 1,
                Some(Excuse::BetrayedCut) => self.excused_betrayed_cut += 1,
                None => {}
            }
        }
    }

    fn assert_complete(&self) {
        for (what, seen) in [
            ("torn writes", self.torn_writes),
            ("lost fsyncs", self.lost_fsyncs),
            ("bit rot", self.bit_rot),
            ("stops at a torn record", self.stops_torn),
            ("stops at a bad checksum", self.stops_bad_checksum),
            ("discarded segments", self.discarded),
            ("the lost-fsync excuse", self.excused_lost_fsync),
            ("the bit-rot excuse", self.excused_bit_rot),
            ("the betrayed-cut excuse", self.excused_betrayed_cut),
        ] {
            assert!(seen > 0, "the sweep never saw {what}: {self:?}");
        }
        // A correctly synced log never has a directory operation pending at a crash,
        // and so never loses a segment.
        assert_eq!(
            (self.lost_entries, self.stops_missing),
            (0, 0),
            "the correct log lost a directory entry: {self:?}"
        );
        assert!(
            self.records > 10_000,
            "too few records to mean much: {self:?}"
        );
    }
}

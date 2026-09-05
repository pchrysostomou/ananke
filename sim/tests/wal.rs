//! The Phase 1 crash-injection property (SPEC.md §2.8) for the write-ahead log, as
//! the sweep CLAUDE.md asks of every fault-model test: the correct log passes every
//! seed with every §1.3 fault on, and each known-buggy variant is caught. One test per
//! variant, so they run side by side and a nightly of 10 000 seeds stays tractable.

use std::time::Duration;

use ananke_env::{TraceEvent, WalStopReason};
use ananke_sim::wal::{self, Excuse};
use ananke_sim::{seeds, write_trace};
use ananke_storage::Variant;

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
    write_trace("wal-42", &report.jsonl);
}

/// The positive control: the correct log satisfies the property on every seed, and
/// the sweep actually reached every fault and used every excuse. A failing seed
/// leaves its trace in `out/`.
#[test]
fn the_correct_log_passes_every_seed() {
    let mut coverage = Coverage::default();
    for seed in 0..seeds() {
        let report = wal::run(seed, Variant::Correct);
        coverage.add(&report);
        if let Err(violation) = report.check() {
            write_trace(&format!("wal-{seed}"), &report.jsonl);
            panic!("{violation}");
        }
    }
    eprintln!("Correct: {coverage:?}");
    coverage.assert_complete();
}

/// The negative controls: each known bug is caught on some seed.
fn is_caught(variant: Variant) {
    let mut caught = Vec::new();
    for seed in 0..seeds() {
        if let Err(violation) = wal::run(seed, variant).check() {
            caught.push(violation);
        }
    }
    eprintln!(
        "{variant:?}: caught on {} of {} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(!caught.is_empty(), "{variant:?} was never caught");
}

#[test]
fn a_log_that_skips_sync_dir_on_rotation_is_caught() {
    is_caught(Variant::NoSyncDir);
}

#[test]
fn a_log_that_skips_the_checksum_is_caught() {
    is_caught(Variant::NoChecksum);
}

#[test]
fn a_log_that_acknowledges_before_syncing_is_caught() {
    is_caught(Variant::AckBeforeSync {
        interval: Duration::from_millis(2),
    });
}

/// What the correct log's sweep saw.
#[derive(Debug, Default)]
struct Coverage {
    seeds: u64,
    epochs: u32,
    records: usize,
    torn_writes: u32,
    lost_fsyncs: u32,
    bit_rot: u32,
    lost_entries: u32,
    stops_torn: u32,
    stops_bad_checksum: u32,
    stops_gap: u32,
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
                Some(WalStopReason::Gap { .. }) => self.stops_gap += 1,
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
        // A gap needs a lost sync on a segment's last group and then a crash that
        // drops that write whole rather than tearing it: one to two epochs in a
        // hundred. Twenty seeds cannot promise one; a hundred can.
        if self.seeds >= 100 {
            assert!(self.stops_gap > 0, "the sweep never saw a gap: {self:?}");
        }
        // A correctly synced log never has a directory operation pending at a crash,
        // and so never loses a segment.
        assert_eq!(
            self.lost_entries, 0,
            "the correct log lost a directory entry: {self:?}"
        );
        assert!(
            self.records as u64 > 100 * self.seeds,
            "too few records to mean much: {self:?}"
        );
    }
}

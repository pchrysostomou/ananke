//! The Phase 1 crash-injection property (SPEC.md §2.8) for the write-ahead log, as
//! the sweep CLAUDE.md asks of every fault-model test: the correct log passes every
//! seed with every §1.3 fault on, and each known-buggy variant is caught. One test per
//! variant, so they run side by side and a nightly of 10 000 seeds stays tractable.

use std::sync::Mutex;
use std::time::Duration;

use ananke_env::{TraceEvent, WalStopReason};
use ananke_sim::wal::{self, Excuse};
use ananke_sim::{released_seeds, sweep, verdict, write_trace};
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
    let coverage = Mutex::new(Coverage::default());
    let verdicts = sweep(released_seeds(), |seed| {
        let report = wal::run(seed, Variant::Correct);
        coverage.lock().unwrap().add(&report);
        report.check().map_err(|violation| {
            write_trace(&format!("wal-{seed}"), &report.jsonl);
            format!("seed {seed}: {violation}")
        })
    });
    let coverage = coverage.into_inner().unwrap();
    eprintln!("Correct: {coverage:?}");
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete();
}

/// The negative controls: each known bug is caught on some seed.
fn is_caught(variant: Variant) {
    let caught: Vec<String> = sweep(released_seeds(), |seed| {
        wal::run(seed, variant).check().err().map(|v| v.to_string())
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "{variant:?}: caught on {} of {} seeds, first: {}",
        caught.len(),
        released_seeds(),
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
    /// Seeds where a cut of recovery's own was made on a sync the disk lied about:
    /// the cut may not hold, so the records it discarded can come back at the next
    /// crash under numbers the log has since re-issued (D-062).
    // PROPOSED(D-062): the WAL's supersede rule.
    betrayed_cuts: u32,
    /// Of those, seeds where such a cut was to nothing, which brings a whole segment
    /// back in front of the live one — the nightly's seed 3123.
    // PROPOSED(D-062): the WAL's supersede rule.
    betrayed_cuts_to_nothing: u32,
    /// Seeds where recovery met a resurrected segment and superseded it.
    // PROPOSED(D-062): the WAL's supersede rule.
    superseded: u32,
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
        // PROPOSED(D-062): the WAL's supersede rule.
        let betrayed = report.betrayed_cuts();
        self.betrayed_cuts += u32::from(!betrayed.is_empty());
        self.betrayed_cuts_to_nothing += u32::from(betrayed.iter().any(|&(_, len)| len == 0));
        self.superseded += u32::from(report.has(|e| matches!(e, TraceEvent::WalSuperseded { .. })));
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
                Some(Excuse::Compacted) | None => {}
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
        ] {
            assert!(seen > 0, "the sweep never saw {what}: {self:?}");
        }
        // A gap needs a lost sync on a segment's last group and then a crash that
        // drops that write whole rather than tearing it: one to two epochs in a
        // hundred. Twenty seeds cannot promise one; a hundred can. It is on 51 of the
        // first thousand seeds, 5.1 %, so D-061 leaves it at the hundred-seed tier,
        // where a hundred see none with probability 0.949^100 = 0.005.
        if self.seeds >= 100 {
            assert!(self.stops_gap > 0, "the sweep never saw a gap: {self:?}");
        }
        // The betrayed-cut excuse needs a recovery that cut a segment, a lost sync of
        // that cut, and the next recovery stopping exactly there: on 34 of the first
        // thousand seeds, 3.4 % (36 epochs; 401 epochs at the nightly's ten thousand,
        // so at most 4.0 % of its seeds). D-061, the owner's rule of 2026-09-15: a state
        // reached on under 5 % of seeds is asserted from the thousand-seed tier, the
        // premerge and the nightly, and printed with the coverage at every tier. At
        // 3.4 % the gate's twenty see none with probability 0.966^20 = 0.50 and a
        // hundred with 0.966^100 = 0.031, so the assertion there would fail a log with
        // nothing wrong the day a change redraws the schedules; a thousand see none
        // with probability 0.966^1000 = 9.5e-16.
        if self.seeds >= 1000 {
            assert!(
                self.excused_betrayed_cut > 0,
                "the sweep never saw the betrayed-cut excuse: {self:?}"
            );
        }
        // The shape the supersede rule exists for (D-062): a cut of recovery's own
        // made on a sync the disk lied about. The cut may not hold, and when it does
        // not the discarded records come back under numbers the log has re-issued.
        // On 765 of the first thousand seeds, 76.5 %, so it is asserted at every
        // tier, where the gate's twenty see none with probability 0.235^20 = 3e-13.
        // PROPOSED(D-062): the WAL's supersede rule.
        assert!(
            self.betrayed_cuts > 0,
            "the sweep never saw a cut whose sync the disk lied about: {self:?}"
        );
        // The worse half of that shape: the betrayed cut was to nothing, so a whole
        // segment comes back in front of the live one, which is what the nightly met
        // at seed 3123. On 60 of the first thousand, 6.0 %, so by D-061 it stays at
        // the hundred-seed tier, where a hundred see none with probability
        // 0.94^100 = 0.002 and the gate's twenty with 0.29 — far too often to assert
        // there. The rule firing is rarer still and is not asserted anywhere: no
        // seed of the first thousand superseded here, and the engine's seek sweep
        // superseded on 1 of its first ten thousand. Seed 3123 pins it instead
        // (sim/tests/engine.rs), and `superseded` above prints it at every tier.
        // PROPOSED(D-062): the WAL's supersede rule.
        if self.seeds >= 100 {
            assert!(
                self.betrayed_cuts_to_nothing > 0,
                "the sweep never saw a betrayed cut to nothing: {self:?}"
            );
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

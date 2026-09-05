//! The Phase 1 crash-injection property (SPEC.md §2.8) for the engine: the correct
//! engine passes every seed with every §1.3 fault on, filesystem latency and crashes
//! mid-flush; the engine that acknowledges before the log is caught, and so is the
//! one that releases a memtable and its log segments before the manifest names its
//! table.

use ananke_env::TraceEvent;
use ananke_sim::engine::{self, Variant};
use ananke_sim::{seeds, write_trace};

/// Two runs with the same seed produce byte-identical traces.
#[test]
fn same_seed_gives_byte_identical_trace() {
    let first = engine::run(42, Variant::Correct);
    let second = engine::run(42, Variant::Correct);
    assert_eq!(first.jsonl.as_bytes(), second.jsonl.as_bytes());
}

/// The seed-42 trace is written for the studio.
#[test]
fn the_seed_42_trace_is_written_for_the_studio() {
    let report = engine::run(42, Variant::Correct);
    report.check().unwrap();
    write_trace("engine-42", &report.jsonl);
}

/// The positive control: the correct engine satisfies every property on every seed,
/// and the sweep reached the states that matter.
#[test]
fn the_correct_engine_passes_every_seed() {
    let mut coverage = Coverage::default();
    for seed in 0..seeds() {
        let report = engine::run(seed, Variant::Correct);
        coverage.add(&report);
        if let Err(violation) = report.check() {
            write_trace(&format!("engine-{seed}"), &report.jsonl);
            panic!("{violation}");
        }
    }
    eprintln!("Correct: {coverage:?}");
    coverage.assert_complete();
}

/// The first nightly sweep failed here: a live read of k46 returned a value two
/// writes old. Two writes to one key acknowledged in the same group were applied by
/// their callers newer-first, the memtable rotated in between, and the older write
/// landed in the newer memtable and shadowed the newer one. Writes now apply in
/// sequence order (D-021); this seed stays in the gate so they keep doing so.
#[test]
fn seed_420_which_the_first_nightly_found_stays_green() {
    engine::run(420, Variant::Correct).check().unwrap();
}

/// The negative controls: each known bug is caught on some seed.
fn is_caught(variant: Variant) {
    let mut caught = Vec::new();
    for seed in 0..seeds() {
        if let Err(violation) = engine::run(seed, variant).check() {
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
fn an_engine_that_acknowledges_before_the_log_is_caught() {
    is_caught(Variant::NoWalBeforeMemtable);
}

#[test]
fn an_engine_that_releases_a_memtable_before_the_manifest_is_caught() {
    is_caught(Variant::ReleaseBeforeManifest);
}

/// What the correct engine's sweep saw.
#[derive(Debug, Default)]
struct Coverage {
    seeds: u64,
    epochs: u32,
    ops: usize,
    reads: u64,
    rotations: u32,
    flushes: u32,
    crashes_mid_flush: u32,
    recoveries_with_records: u32,
    excused: u32,
    lost_fsyncs: u32,
    bit_rot: u32,
    torn_writes: u32,
    tables_written: u32,
    segments_deleted: u32,
    orphans_removed: u32,
    tables_dropped: u32,
    manifest_fallbacks: u32,
    head_gaps: u32,
    flusher_failures: u32,
}

impl Coverage {
    fn add(&mut self, report: &engine::Report) {
        self.seeds += 1;
        self.epochs += report.epochs.len() as u32;
        self.ops += report
            .epochs
            .iter()
            .map(|e| e.appended - e.base)
            .sum::<usize>();
        self.reads += report.reads;
        self.rotations += report
            .records
            .iter()
            .filter(|r| matches!(r.event, TraceEvent::MemtableRotated { .. }))
            .count() as u32;
        self.flushes += report
            .records
            .iter()
            .filter(|r| matches!(r.event, TraceEvent::MemtableFlushed { .. }))
            .count() as u32;
        self.crashes_mid_flush += report.epochs.iter().filter(|e| e.mid_flush > 0).count() as u32;
        self.recoveries_with_records += report
            .epochs
            .iter()
            .filter(|e| e.recovery.replayed > 0)
            .count() as u32;
        self.excused += report.epochs.iter().filter(|e| e.excuse.is_some()).count() as u32;
        self.lost_fsyncs += u32::from(report.has(|e| matches!(e, TraceEvent::FsyncLost { .. })));
        self.bit_rot += u32::from(report.has(|e| matches!(e, TraceEvent::BlockRotted { .. })));
        self.torn_writes += u32::from(report.has(|e| matches!(e, TraceEvent::WriteTorn { .. })));
        self.tables_written += report.count(|e| matches!(e, TraceEvent::SstWritten { .. })) as u32;
        self.segments_deleted +=
            report.count(|e| matches!(e, TraceEvent::WalSegmentDeleted { .. })) as u32;
        self.orphans_removed +=
            report.count(|e| matches!(e, TraceEvent::OrphanRemoved { .. })) as u32;
        self.tables_dropped += report
            .epochs
            .iter()
            .map(|e| e.recovery.dropped.len() as u32)
            .sum::<u32>();
        self.manifest_fallbacks += report
            .epochs
            .iter()
            .filter(|e| e.recovery.fallback_from.is_some())
            .count() as u32;
        self.flusher_failures +=
            report.count(|e| matches!(e, TraceEvent::FlusherFailed { .. })) as u32;
        self.head_gaps += report
            .epochs
            .iter()
            .filter(|e| e.recovery.wal.head_gap.is_some())
            .count() as u32;
    }

    fn assert_complete(&self) {
        for (what, seen) in [
            ("live reads", u32::try_from(self.reads).unwrap_or(u32::MAX)),
            ("memtable rotations", self.rotations),
            ("memtable flushes", self.flushes),
            ("crashes with a memtable mid-flush", self.crashes_mid_flush),
            (
                "recoveries that replayed records",
                self.recoveries_with_records,
            ),
            ("excused losses", self.excused),
            ("lost fsyncs", self.lost_fsyncs),
            ("bit rot", self.bit_rot),
            ("torn writes", self.torn_writes),
            ("tables written", self.tables_written),
            ("log segments deleted behind a flush", self.segments_deleted),
            (
                "orphans removed after a crash mid-flush",
                self.orphans_removed,
            ),
            (
                "tables dropped for a fault the trace explains",
                self.tables_dropped,
            ),
            ("manifest fallbacks", self.manifest_fallbacks),
            ("missing log heads, discarded", self.head_gaps),
        ] {
            assert!(seen > 0, "the sweep never saw {what}: {self:?}");
        }
        // The simulator raises no I/O error of its own, so a flusher that stopped hit
        // one the engine made (a stale segment it tried to delete twice, once).
        assert_eq!(
            self.flusher_failures, 0,
            "the flusher stopped on an error: {self:?}"
        );
        assert!(
            self.ops as u64 > 100 * self.seeds,
            "too few ops to mean much: {self:?}"
        );
    }
}

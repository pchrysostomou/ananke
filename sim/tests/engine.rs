//! The Phase 1 crash-injection property (SPEC.md §2.8) for the engine: the correct
//! engine passes every seed with every §1.3 fault on, filesystem latency and crashes
//! mid-flush and mid-compaction; the engine that acknowledges before the log is
//! caught, and so are the one that releases a memtable and its log segments before
//! the manifest names its table and the one whose compaction deletes its inputs
//! before the manifest stops naming them.

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

/// The first 3000-seed sweep with compaction found this: CURRENT and the two newest
/// manifests were damaged at one crash, recovery fell back to the newest readable
/// manifest, whose tables a later compaction had deleted, and the store came back
/// empty. Recovery now refuses a store whose CURRENT or manifest cannot be read, and
/// with fallback allowed uses only an older manifest whose every table is intact
/// (D-022). The seed's earlier fallbacks take a different path under that rule, so
/// the run no longer reaches the same crash; what is pinned is the rule on the seed
/// that motivated it. With fallback allowed, every fallback in the run lands on a
/// manifest with no table missing, and what it opens is what that manifest and the
/// log hold; without it, the first unreadable CURRENT refuses the store for a fault
/// and the run ends.
#[test]
fn seed_44_never_opens_empty_in_either_mode() {
    // The schedule the sweep ran with when it found the seed: level 1 eight times
    // larger than the gate's.
    let schedule = engine::Schedule {
        level_base_bytes: 8192,
        ..engine::Schedule::default()
    };
    let allowed = engine::run_with(44, schedule, Variant::Correct);
    allowed.check().unwrap();
    let fallbacks: Vec<&engine::Epoch> = allowed
        .epochs
        .iter()
        .filter(|e| e.recovery.fallback_from.is_some())
        .collect();
    assert!(
        !fallbacks.is_empty(),
        "the seed still exercises the fallback"
    );
    for epoch in fallbacks {
        assert!(epoch.recovery.dropped.is_empty(), "{:?}", epoch.recovery);
        assert!(
            epoch.recovery.ssts > 0
                || epoch.recovery.replayed > 0
                || epoch.recovery.flushed_seq == 0,
            "{:?}",
            epoch.recovery
        );
    }
    if let Some(refusal) = &allowed.refused {
        assert!(
            refusal.reason.contains("no manifest older than"),
            "{refusal:?}"
        );
    }
    let refusing = engine::run_with(
        44,
        engine::Schedule {
            allow_manifest_fallback: false,
            ..schedule
        },
        Variant::Correct,
    );
    refusing.check().unwrap();
    let refusal = refusing.refused.as_ref().expect("the store is refused");
    assert!(refusal.reason.contains("cannot be read"), "{refusal:?}");
    assert!(
        refusing.epochs.len() < allowed.epochs.len(),
        "refusing ends the run earlier"
    );
}

/// The nightly's deep-levels run: per-level limits so small that compaction reaches
/// level 3 and below, which the gate's and CI's schedule never does with its key
/// space. Runs only when `ANANKE_DEEP_SEEDS` is set, as the nightly sets it.
#[test]
fn the_correct_engine_passes_every_seed_with_deep_levels() {
    let seeds = ananke_sim::deep_seeds();
    if seeds == 0 {
        eprintln!("ANANKE_DEEP_SEEDS is not set: skipped");
        return;
    }
    let mut rounds_at_or_below_level_2 = 0u32;
    let mut deepest = 0u8;
    for seed in 0..seeds {
        let report = engine::run_with(seed, engine::Schedule::deep(), Variant::Correct);
        if let Err(violation) = report.check() {
            write_trace(&format!("engine-deep-{seed}"), &report.jsonl);
            panic!("{violation}");
        }
        for record in &report.records {
            if let TraceEvent::CompactionWritten { level, .. } = record.event {
                rounds_at_or_below_level_2 += u32::from(level >= 2);
                deepest = deepest.max(level + 1);
            }
        }
    }
    eprintln!(
        "deep levels: {rounds_at_or_below_level_2} rounds from level 2 or deeper, deepest level {deepest}"
    );
    assert!(
        rounds_at_or_below_level_2 > 0,
        "no round compacted level 2 or deeper into the level below"
    );
    assert!(
        deepest >= 3,
        "compaction never reached level 3: deepest {deepest}"
    );
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

#[test]
fn a_compaction_that_deletes_its_inputs_before_the_manifest_is_caught() {
    is_caught(Variant::DeleteBeforeManifest);
}

/// What the correct engine's sweep saw.
#[derive(Debug, Default)]
struct Coverage {
    seeds: u64,
    epochs: u32,
    ops: usize,
    reads: u64,
    scans: u64,
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
    refusals: u32,
    batches: u64,
    unsynced_writes: u64,
    checkpoints_verified: u64,
    checkpoints_damaged: u64,
    compactions: u32,
    compactions_below_level_0: u32,
    tables_deleted: u32,
    versions_dropped: u64,
    tombstones_dropped: u64,
    crashes_mid_compaction: u32,
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
        self.scans += report.scans;
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
        self.refusals += u32::from(report.refused.is_some());
        self.batches += report.batches;
        self.unsynced_writes += report.unsynced;
        self.checkpoints_verified += report.checkpoints_verified;
        self.checkpoints_damaged += report.checkpoints_damaged;
        self.compactions +=
            report.count(|e| matches!(e, TraceEvent::CompactionWritten { .. })) as u32;
        self.compactions_below_level_0 += report
            .count(|e| matches!(e, TraceEvent::CompactionWritten { level, .. } if *level > 0))
            as u32;
        self.tables_deleted += report.count(|e| matches!(e, TraceEvent::SstDeleted { .. })) as u32;
        for record in &report.records {
            if let TraceEvent::CompactionWritten {
                dropped_versions,
                dropped_tombstones,
                ..
            } = record.event
            {
                self.versions_dropped += dropped_versions;
                self.tombstones_dropped += dropped_tombstones;
            }
        }
        // A crash inside a compaction: an output written for a deeper level, and the
        // next crash before any manifest was switched to.
        let mut pending = false;
        for record in &report.records {
            match record.event {
                TraceEvent::SstWritten { level, .. } if level > 0 => pending = true,
                TraceEvent::CurrentSwitched { .. } => pending = false,
                TraceEvent::NodeCrashed { .. } => {
                    self.crashes_mid_compaction += u32::from(pending);
                    pending = false;
                }
                _ => {}
            }
        }
        self.head_gaps += report
            .epochs
            .iter()
            .filter(|e| e.recovery.wal.head_gap.is_some())
            .count() as u32;
    }

    fn assert_complete(&self) {
        for (what, seen) in [
            ("live reads", u32::try_from(self.reads).unwrap_or(u32::MAX)),
            (
                "scans at a snapshot",
                u32::try_from(self.scans).unwrap_or(u32::MAX),
            ),
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
            ("batches", u32::try_from(self.batches).unwrap_or(u32::MAX)),
            (
                "writes without a sync",
                u32::try_from(self.unsynced_writes).unwrap_or(u32::MAX),
            ),
            (
                "checkpoints opened fresh after a crash",
                u32::try_from(self.checkpoints_verified).unwrap_or(u32::MAX),
            ),
            ("compactions", self.compactions),
            ("compactions below level 0", self.compactions_below_level_0),
            ("input tables deleted", self.tables_deleted),
            (
                "writes dropped by compaction",
                u32::try_from(self.versions_dropped).unwrap_or(u32::MAX),
            ),
            (
                "tombstones dropped by compaction",
                u32::try_from(self.tombstones_dropped).unwrap_or(u32::MAX),
            ),
        ] {
            assert!(seen > 0, "the sweep never saw {what}: {self:?}");
        }
        // A crash inside a compaction needs one to land in the few operations between
        // an output's write and the manifest's: about one seed in six. Twenty seeds
        // cannot promise one; a hundred can.
        if self.seeds >= 100 {
            assert!(
                self.crashes_mid_compaction > 0,
                "the sweep never saw a crash inside a compaction: {self:?}"
            );
            assert!(
                self.refusals > 0,
                "the sweep never saw a store refused for a fault: {self:?}"
            );
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

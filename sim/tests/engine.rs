//! The Phase 1 crash-injection property (SPEC.md §2.8) for the engine: the correct
//! engine passes every seed with every §1.3 fault on, filesystem latency and crashes
//! mid-flush and mid-compaction; the engine that acknowledges before the log is
//! caught, and so are the one that releases a memtable and its log segments before
//! the manifest names its table and the one whose compaction deletes its inputs
//! before the manifest stops naming them. The live install's crash test (Q2, D-054)
//! runs the same scenario with installs, beside the install that switches twice and
//! the span checkpoint that does not sync its tables; the range delete's and the
//! bounded seek's (D-055) beside the delete that forgets the memtables and the seek
//! that counts tombstones. The default schedule runs all four primitives.

use std::sync::Mutex;

use ananke_env::TraceEvent;
use ananke_sim::engine::{self, Variant};
use ananke_sim::{seeds, sweep, verdict, write_trace};

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
    let coverage = Mutex::new(Coverage::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = engine::run(seed, Variant::Correct);
        coverage.lock().unwrap().add(&report);
        report.check().map_err(|violation| {
            write_trace(&format!("engine-{seed}"), &report.jsonl);
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

/// The first nightly sweep failed here: a live read of k46 returned a value two
/// writes old. Two writes to one key acknowledged in the same group were applied by
/// their callers newer-first, the memtable rotated in between, and the older write
/// landed in the newer memtable and shadowed the newer one. Writes now apply in
/// sequence order (D-021); this seed stays in the gate so they keep doing so.
///
/// It runs the Phase 1 workload it was found in: the default schedule runs Stage A's
/// primitives beside it since D-055, which moves every seed's schedule, and this pin
/// is on the schedule it was found on, which that commit left as it was.
// PROPOSED(D-055): the pins keep the schedule they were found on.
#[test]
fn seed_420_which_the_first_nightly_found_stays_green() {
    engine::run_with(420, engine::Schedule::phase_1(), Variant::Correct)
        .check()
        .unwrap();
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
    // larger than the gate's, and the Phase 1 workload, without the primitives the
    // default schedule runs since D-055.
    // PROPOSED(D-055): the pins keep the schedule they were found on.
    let schedule = engine::Schedule {
        level_base_bytes: 8192,
        ..engine::Schedule::phase_1()
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
    let per_seed = sweep(seeds, |seed| {
        let report = engine::run_with(seed, engine::Schedule::deep(), Variant::Correct);
        let verdict = report.check().map_err(|violation| {
            write_trace(&format!("engine-deep-{seed}"), &report.jsonl);
            format!("seed {seed}: {violation}")
        });
        let mut rounds = 0u32;
        let mut deepest = 0u8;
        for record in &report.records {
            if let TraceEvent::CompactionWritten { level, .. } = record.event {
                rounds += u32::from(level >= 2);
                deepest = deepest.max(level + 1);
            }
        }
        (verdict, rounds, deepest)
    });
    let mut rounds_at_or_below_level_2 = 0u32;
    let mut deepest = 0u8;
    let mut verdicts = Vec::with_capacity(per_seed.len());
    for (verdict, rounds, deep) in per_seed {
        rounds_at_or_below_level_2 += rounds;
        deepest = deepest.max(deep);
        verdicts.push(verdict);
    }
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
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

/// Q2's criterion, the stage's gate (SHARD.md §11 storage 5, §12 Stage A; D-054):
/// the live install's crash test. A task checkpoints random spans and installs each
/// over its span a little later while the writers write every key, and every crash
/// is aimed at an install, anywhere from the moment it is asked for to past its end,
/// under the full disk fault model. After each crash the span comes back as it was
/// or as installed, never a mixture; every key outside it as it was, bar what a
/// fault explains; and every write to the span after the install is read over the
/// installed version. The correct engine passes every seed, and the sweep is seen
/// to crash before an install's switch, after it, and after writes over it.
// PROPOSED(D-054): the live install's crash test.
#[test]
fn the_live_install_crash_test_passes_every_seed() {
    let totals = Mutex::new((
        engine::InstallOutcomes::default(),
        0u64,
        0u64,
        0u64,
        0u64,
        0u64,
    ));
    let verdicts = sweep(seeds(), |seed| {
        let report = engine::run_with(seed, engine::Schedule::install(), Variant::Correct);
        {
            let mut t = totals.lock().unwrap();
            add_outcomes(&mut t.0, report.install_outcomes);
            t.1 += report.installs_started;
            t.2 += report.installs_completed;
            t.3 += report.reads_over_installs;
            t.4 += report.span_checkpoints_verified;
            t.5 += report.reads_unjudged_for_installs;
        }
        report.check().map_err(|violation| {
            write_trace(&format!("engine-install-{seed}"), &report.jsonl);
            format!("seed {seed}: {violation}")
        })
    });
    let (outcomes, started, completed, reads_over, verified, unjudged) =
        totals.into_inner().unwrap();
    eprintln!(
        "live install, Correct: {started} installs asked for, {completed} resolved, {reads_over} live reads over an install, {verified} span checkpoints verified, {unjudged} reads left unjudged while an install ran; after the crashes: {outcomes:?}"
    );
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    assert_install_windows(&outcomes, "an install");
    assert!(verified > 0, "no span checkpoint was opened after a crash");
    assert!(
        reads_over > 0,
        "no live read of a key written after an install: {outcomes:?}"
    );
}

/// Adds one run's install outcomes to a sweep's.
fn add_outcomes(total: &mut engine::InstallOutcomes, o: engine::InstallOutcomes) {
    total.aimed += o.aimed;
    total.kept += o.kept;
    total.crashed_after_switch += o.crashed_after_switch;
    total.crashed_before_record_durable += o.crashed_before_record_durable;
    total.crashed_between_replacement_and_switch += o.crashed_between_replacement_and_switch;
    total.crashed_otherwise_before_switch += o.crashed_otherwise_before_switch;
    total.lost_to_a_fault += o.lost_to_a_fault;
    total.keys_written_after += o.keys_written_after;
}

/// The windows an install's crash test must have crashed in (D-054): aimed at all,
/// between its replacement and its switch, which is the one-switch rule's window,
/// after the switch before the install resolved, and with writes over it checked.
// PROPOSED(D-054): the live install's crash test.
fn assert_install_windows(outcomes: &engine::InstallOutcomes, what: &str) {
    assert!(outcomes.aimed > 0, "no crash was aimed at {what}");
    assert!(
        outcomes.crashed_between_replacement_and_switch > 0,
        "no crash landed between {what}'s replacement and its switch: {outcomes:?}"
    );
    assert!(
        outcomes.crashed_after_switch > 0,
        "no crash landed after {what}'s switch before it resolved: {outcomes:?}"
    );
    assert!(
        outcomes.keys_written_after > 0,
        "no key written after {what} was checked after a crash: {outcomes:?}"
    );
}

/// The install's known-buggy engine beside it (CLAUDE.md's pair rule; D-054): the
/// install that takes the span's keys out with one manifest switch and puts the
/// installed tables in with a second is caught by the same crash test, on some seed
/// at every tier, and the rate is printed.
// PROPOSED(D-054): the live install's crash test.
#[test]
fn an_install_in_two_switches_is_caught() {
    let caught: Vec<String> = sweep(seeds(), |seed| {
        engine::run_with(
            seed,
            engine::Schedule::install(),
            Variant::InstallInTwoSwitches,
        )
        .check()
        .err()
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "InstallInTwoSwitches: caught on {} of {} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(!caught.is_empty(), "InstallInTwoSwitches was never caught");
}

/// The range delete's crash test (D-055): a task deletes a random span now and then
/// while the writers write every key, and every crash is aimed at a delete, under
/// the full disk fault model. After each crash the span comes back as it was or
/// empty, never a mixture, with every other key as the oracle says and every write
/// after the delete read over it. The correct engine passes every seed, and the
/// sweep is seen to crash before a delete's switch and after it.
// PROPOSED(D-055): the range delete's crash test.
#[test]
fn the_range_delete_crash_test_passes_every_seed() {
    let totals = Mutex::new((engine::InstallOutcomes::default(), 0u64, 0u64, 0u64));
    let verdicts = sweep(seeds(), |seed| {
        let report = engine::run_with(seed, engine::Schedule::range_delete(), Variant::Correct);
        {
            let mut t = totals.lock().unwrap();
            add_outcomes(&mut t.0, report.install_outcomes);
            t.1 += report.deletes_started;
            t.2 += report.installs_completed;
            t.3 += report.reads_unjudged_for_installs;
        }
        report.check().map_err(|violation| {
            write_trace(&format!("engine-range-delete-{seed}"), &report.jsonl);
            format!("seed {seed}: {violation}")
        })
    });
    let (outcomes, started, completed, unjudged) = totals.into_inner().unwrap();
    eprintln!(
        "range delete, Correct: {started} deletes asked for, {completed} resolved, {unjudged} reads left unjudged while a delete ran; after the crashes: {outcomes:?}"
    );
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    assert_install_windows(&outcomes, "a delete");
}

/// The range delete's known-buggy engine beside it (D-055): a delete that takes the
/// span's writes out of the tables but leaves the memtables unflushed and the
/// manifest's `flushed_seq` where it was, so the span's writes in a memtable stay
/// readable and come back, is caught by the same crash test on some seed at every
/// tier, and the rate is printed.
// PROPOSED(D-055): the range delete's crash test.
#[test]
fn a_range_delete_that_skips_the_memtables_is_caught() {
    let caught: Vec<String> = sweep(high_rate_share(), |seed| {
        engine::run_with(
            seed,
            engine::Schedule::range_delete(),
            Variant::RangeDeleteSkipsMemtables,
        )
        .check()
        .err()
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "RangeDeleteSkipsMemtables: caught on {} of {} seeds, first: {}",
        caught.len(),
        high_rate_share(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        !caught.is_empty(),
        "RangeDeleteSkipsMemtables was never caught"
    );
}

/// The bounded seek's crash test (D-055): half the readers' scans are seeks of one
/// to six keys at a snapshot, which must be the first keys the model holds in the
/// range, and every recovery is walked by seeks of three keys at a time, which must
/// be the model's state. The correct engine passes every seed, and the sweep is seen
/// to make seeks that stop at their limit and walks after a crash.
// PROPOSED(D-055): the bounded seek's crash test.
#[test]
fn the_seek_crash_test_passes_every_seed() {
    let totals = Mutex::new((0u64, 0u64, 0u64));
    let verdicts = sweep(seeds(), |seed| {
        let report = engine::run_with(seed, engine::Schedule::seek(), Variant::Correct);
        {
            let mut t = totals.lock().unwrap();
            t.0 += report.seeks.0;
            t.1 += report.seeks.1;
            t.2 += report.recovery_seeks;
        }
        report.check().map_err(|violation| {
            write_trace(&format!("engine-seek-{seed}"), &report.jsonl);
            format!("seed {seed}: {violation}")
        })
    });
    let (seeks, limited, walks) = totals.into_inner().unwrap();
    eprintln!(
        "seek, Correct: {seeks} live seeks, {limited} of them stopped at their limit, {walks} seeks walking a recovered engine"
    );
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    assert!(limited > 0, "no seek stopped at its limit");
    assert!(walks > 0, "no recovered engine was walked by seeks");
}

/// The bounded seek's known-buggy engine beside it (D-055): a seek that counts a
/// deleted key against its limit returns fewer keys than the range holds, and is
/// caught by the same crash test on some seed at every tier.
// PROPOSED(D-055): the bounded seek's crash test.
#[test]
fn a_seek_that_counts_tombstones_is_caught() {
    let caught: Vec<String> = sweep(high_rate_share(), |seed| {
        engine::run_with(
            seed,
            engine::Schedule::seek(),
            Variant::SeekCountsTombstones,
        )
        .check()
        .err()
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "SeekCountsTombstones: caught on {} of {} seeds, first: {}",
        caught.len(),
        high_rate_share(),
        caught.first().map_or("", String::as_str)
    );
    assert!(!caught.is_empty(), "SeekCountsTombstones was never caught");
}

/// The install that keeps its source's sequence numbers beside the same crash test
/// (D-054): half the installs take their source from a store further along than
/// the live engine, so a kept number hides the writes that follow the install, and
/// the oracle checks that every installed table the manifest in force lists carries
/// the install's own number. Caught on some seed at every tier, and the rate is
/// printed.
///
/// A kept number can also equal the number of a later local write of the same key,
/// and two writes under one internal key stop the variant's next compaction at the
/// table writer's order assertion: the engine itself refuses the state the bug
/// made. Such a seed counts as caught, and the test prints how many there were.
// PROPOSED(D-054): the installed sequence numbers are the install's.
#[test]
fn an_install_that_keeps_its_sources_numbers_is_caught() {
    let outcomes: Vec<Option<(String, bool)>> = sweep(high_rate_share(), |seed| {
        std::panic::catch_unwind(|| {
            engine::run_with(
                seed,
                engine::Schedule::install(),
                Variant::InstallKeepsSourceNumbers,
            )
            .check()
            .err()
            .map(|violation| (violation, false))
        })
        .unwrap_or_else(|_| Some((format!("seed {seed}: the engine panicked"), true)))
    });
    let caught: Vec<&(String, bool)> = outcomes.iter().flatten().collect();
    let panicked = caught.iter().filter(|(_, p)| *p).count();
    eprintln!(
        "InstallKeepsSourceNumbers: caught on {} of {} seeds, {panicked} of them by the engine's own assertion, first: {}",
        caught.len(),
        high_rate_share(),
        caught
            .iter()
            .find(|(_, p)| !*p)
            .map_or("", |(v, _)| v.as_str())
    );
    assert!(
        !caught.is_empty(),
        "InstallKeepsSourceNumbers was never caught"
    );
}

/// The seeds a variant caught on at least four seeds in five runs: a tenth of the
/// tier, and never fewer than twenty or the tier itself. At those rates a share of
/// twenty still expects sixteen catches or more, and a premerge share of a hundred
/// eighty or more, while the engine binary's cost stays near what the sweep's other
/// tests make it.
// PROPOSED(D-055): the high-rate variants run a share of the seeds.
fn high_rate_share() -> u64 {
    (seeds() / 10).max(seeds().min(20))
}

/// The span checkpoint's known-buggy engine beside the same crash test (D-054): a
/// checkpoint of a span whose tables are not synced before the manifest and
/// `CURRENT` that name them is caught when a crash leaves one of them short and
/// the checkpoint is opened fresh after it, on some seed at every tier.
// PROPOSED(D-054): the checkpoint of a span, which the live install installs from.
#[test]
fn a_span_checkpoint_without_syncs_is_caught() {
    let caught: Vec<String> = sweep(high_rate_share(), |seed| {
        engine::run_with(
            seed,
            engine::Schedule::install(),
            Variant::SpanCheckpointUnsynced,
        )
        .check()
        .err()
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "SpanCheckpointUnsynced: caught on {} of {} seeds, first: {}",
        caught.len(),
        high_rate_share(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        !caught.is_empty(),
        "SpanCheckpointUnsynced was never caught"
    );
}

/// The negative controls: each known bug is caught on some seed.
fn is_caught(variant: Variant) {
    let caught: Vec<String> = sweep(seeds(), |seed| {
        engine::run(seed, variant)
            .check()
            .err()
            .map(|v| v.to_string())
    })
    .into_iter()
    .flatten()
    .collect();
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
    installs: u64,
    range_deletes: u64,
    span_checkpoints: u64,
    seeks: u64,
    recovery_seeks: u64,
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
        self.installs += report.installs_started - report.deletes_started;
        self.range_deletes += report.deletes_started;
        self.span_checkpoints += report.span_checkpoints;
        self.seeks += report.seeks.0;
        self.recovery_seeks += report.recovery_seeks;
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
            // PROPOSED(D-055): the sweep runs all four primitives.
            ("installs", u32::try_from(self.installs).unwrap_or(u32::MAX)),
            (
                "range deletes",
                u32::try_from(self.range_deletes).unwrap_or(u32::MAX),
            ),
            (
                "span checkpoints",
                u32::try_from(self.span_checkpoints).unwrap_or(u32::MAX),
            ),
            ("seeks", u32::try_from(self.seeks).unwrap_or(u32::MAX)),
            (
                "seeks walking a recovered engine",
                u32::try_from(self.recovery_seeks).unwrap_or(u32::MAX),
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

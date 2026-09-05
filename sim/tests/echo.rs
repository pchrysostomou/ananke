//! Phase 0 exit criteria (SPEC.md §1.6) and the Phase 1 gate: the echo scenario is
//! deterministic, its moirae trace hashes to a pinned value, it doubles as a smoke test
//! for the simulator across many seeds, and that sweep exercises bit rot,
//! directory-entry loss and torn writes on the nodes' journals under a poll budget.
//! The sweep runs the journal in both [`Variant`]s: the buggy one must be seen to lose
//! its journal, the correct one must never lose it. Together they show the fault
//! model distinguishes a bug from correct code, which either alone would not.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use ananke_env::sim::Sim;
use ananke_env::{Environment, File, FileSystem, OpenOptions, RealEnv, TraceEvent};
use ananke_sim::echo::{self, Variant};
use bytes::Bytes;
use moirae_trace::trace_hash;

/// The pinned hash of the seed-42 trace (`out/echo-42.jsonl`) of the `NoSyncDir`
/// variant, the run that shows every fault, as moirae pins its example traces. It covers the **body**: every line after the header, hashed with
/// FNV-1a over the exact bytes. The header is excluded because it carries the crate
/// version, and a release must not invalidate every fixture; everything the header
/// describes still shapes the body. The value changes only when the simulator, the
/// protocol, the export or the scheduling policy changes on purpose: update it in the
/// same commit and say why, and update the copy of the trace committed in the moirae
/// repo as the studio's `echo-42.jsonl` fixture, whose test pins the same rule and value.
const GOLDEN: &str = "19f19201df99a799";

/// Two runs with the same seed produce byte-identical traces.
#[test]
fn same_seed_gives_byte_identical_trace() {
    for variant in [Variant::Correct, Variant::NoSyncDir] {
        let first = echo::run(42, variant);
        let second = echo::run(42, variant);
        assert_eq!(first.jsonl.as_bytes(), second.jsonl.as_bytes());
        first.check().unwrap();
        assert!(
            first.jsonl.lines().count() > 1_000,
            "expected a substantial trace, got {} lines",
            first.jsonl.lines().count()
        );
    }
}

/// The seed-42 trace hashes to the pinned value; the trace is written first so a
/// mismatch leaves the new bytes on disk to diff against the moirae fixture.
#[test]
fn trace_hash_matches_the_pinned_golden() {
    let report = echo::run(42, Variant::NoSyncDir);
    let jsonl = report.jsonl.clone();
    RealEnv::run(|env| async move {
        let fs = env.fs();
        fs.create_dir_all(Path::new("out")).await.unwrap();
        let file = fs
            .open(
                Path::new("out/echo-42.jsonl"),
                OpenOptions::new().write(true).create(true).truncate(true),
            )
            .await
            .unwrap();
        file.write_at(0, Bytes::from(jsonl)).await.unwrap();
        file.sync().await.unwrap();
    });
    assert_eq!(body_hash(&report.jsonl), GOLDEN);
}

/// Different seeds explore different runs.
#[test]
fn different_seeds_give_different_traces() {
    assert_ne!(
        echo::run(1, Variant::NoSyncDir).jsonl,
        echo::run(2, Variant::NoSyncDir).jsonl
    );
}

/// One hundred consecutive seeds, both variants: every run must satisfy the
/// scenario's invariants, and between them the runs must have exercised every §1.3
/// fault on the journal. With the buggy journal the sweep must see a lost directory
/// entry make the journal vanish; with the correct one it never may, while bit rot and
/// torn writes still hit both. A failure here names the seed, which reproduces it.
#[test]
fn one_hundred_seeds_satisfy_the_invariants_and_exercise_the_disk_faults() {
    let mut pongs = 0;
    let mut sloppy = Coverage::default();
    let mut correct = Coverage::default();
    for seed in 0..100 {
        for (variant, coverage) in [
            (Variant::NoSyncDir, &mut sloppy),
            (Variant::Correct, &mut correct),
        ] {
            let report = echo::run(seed, variant);
            report
                .check()
                .unwrap_or_else(|violation| panic!("{variant:?}: {violation}"));
            pongs += report.pongs_received();
            coverage.add(&report);
        }
    }
    assert!(pongs > 0);
    eprintln!("NoSyncDir: {sloppy:?}");
    eprintln!("Correct: {correct:?}");
    // The negative control: the bug is visible.
    sloppy.assert_every_fault_seen();
    // The positive control: the same faults, and the journal never vanishes.
    correct.assert_disk_faults_seen();
    assert_eq!(
        (
            correct.seeds_with_lost_entries,
            correct.seeds_with_missing_journal,
            correct.seeds_with_missing_previous,
        ),
        (0, 0, 0),
        "the correct journal lost something: {correct:?}"
    );
}

/// The scenario's poll budget is in force: a task that keeps waking itself without
/// letting time move fails the run, naming the budget, instead of hanging a sweep.
#[test]
#[should_panic(expected = "busy loop")]
fn a_busy_looping_task_fails_the_scenario_instead_of_hanging() {
    let mut sim = Sim::new(echo::config(42));
    let node = sim.add_node();
    sim.env(node).spawn("spinner", async {
        loop {
            YieldOnce(false).await;
        }
    });
    sim.run_for(echo::PING_INTERVAL);
}

/// Which faults the sweep has seen, in the trace and in what the restarted node found.
#[derive(Debug, Default)]
struct Coverage {
    seeds_with_bit_rot: u32,
    seeds_with_corrupt_records: u32,
    seeds_with_torn_writes: u32,
    seeds_with_torn_files: u32,
    seeds_with_lost_entries: u32,
    seeds_with_missing_journal: u32,
    seeds_with_missing_previous: u32,
}

impl Coverage {
    fn add(&mut self, report: &echo::Report) {
        let has = |f: &dyn Fn(&TraceEvent) -> bool| report.records.iter().any(|r| f(&r.event));
        let journal = report.stats[2]
            .journal
            .as_ref()
            .expect("node 2 keeps a journal");
        self.seeds_with_bit_rot += u32::from(has(&|e| matches!(e, TraceEvent::BlockRotted { .. })));
        self.seeds_with_corrupt_records += u32::from(journal.corrupt > 0);
        self.seeds_with_torn_writes +=
            u32::from(has(&|e| matches!(e, TraceEvent::WriteTorn { .. })));
        self.seeds_with_torn_files += u32::from(journal.torn > 0);
        self.seeds_with_lost_entries +=
            u32::from(has(&|e| matches!(e, TraceEvent::DirectoryEntryLost { .. })));
        self.seeds_with_missing_journal += u32::from(!journal.found);
        self.seeds_with_missing_previous += u32::from(!journal.found_previous);
    }

    /// The faults that hit the file contents, which no `sync_dir` discipline prevents.
    fn assert_disk_faults_seen(&self) {
        let counts = [
            ("bit rot", self.seeds_with_bit_rot),
            (
                "corrupt records caught by the checksum",
                self.seeds_with_corrupt_records,
            ),
            ("torn writes", self.seeds_with_torn_writes),
            ("torn files seen at replay", self.seeds_with_torn_files),
        ];
        for (what, seeds) in counts {
            assert!(seeds > 0, "no seed produced {what}: {self:?}");
        }
    }

    /// The disk faults plus the directory ones only the buggy journal exposes.
    fn assert_every_fault_seen(&self) {
        self.assert_disk_faults_seen();
        let counts = [
            ("lost directory entries", self.seeds_with_lost_entries),
            ("a vanished journal", self.seeds_with_missing_journal),
            ("a vanished journal.prev", self.seeds_with_missing_previous),
        ];
        for (what, seeds) in counts {
            assert!(seeds > 0, "no seed produced {what}: {self:?}");
        }
    }
}

/// Pending once, waking itself, then ready: awaited in a loop it is a busy loop.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// The pin rule: the body is everything after the header line's LF.
fn body_hash(jsonl: &str) -> String {
    let body = jsonl.split_once('\n').map_or("", |(_, rest)| rest);
    trace_hash(body)
}

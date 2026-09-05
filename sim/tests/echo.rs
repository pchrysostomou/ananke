//! Phase 0 exit criteria (SPEC.md §1.6) and the Phase 1 gate: the echo scenario is
//! deterministic, its moirae trace hashes to a pinned value, it doubles as a smoke test
//! for the simulator across many seeds, and that sweep exercises bit rot,
//! directory-entry loss and torn writes on the nodes' journals under a poll budget.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use ananke_env::sim::Sim;
use ananke_env::{Environment, File, FileSystem, OpenOptions, RealEnv, TraceEvent};
use ananke_sim::echo;
use bytes::Bytes;
use moirae_trace::trace_hash;

/// The pinned hash of the seed-42 trace (`out/echo-42.jsonl`), as moirae pins its
/// example traces. It covers the **body**: every line after the header, hashed with
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
    let first = echo::run(42);
    let second = echo::run(42);
    assert_eq!(first.jsonl.as_bytes(), second.jsonl.as_bytes());
    first.check().unwrap();
    assert!(
        first.jsonl.lines().count() > 1_000,
        "expected a substantial trace, got {} lines",
        first.jsonl.lines().count()
    );
}

/// The seed-42 trace hashes to the pinned value; the trace is written first so a
/// mismatch leaves the new bytes on disk to diff against the moirae fixture.
#[test]
fn trace_hash_matches_the_pinned_golden() {
    let report = echo::run(42);
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
    assert_ne!(echo::run(1).jsonl, echo::run(2).jsonl);
}

/// One hundred consecutive seeds: every run must satisfy the scenario's invariants,
/// and between them the runs must have exercised every §1.3 fault on the journal:
/// bit rot that the checksums caught, a torn write, and a lost directory entry that
/// made the journal vanish. A failure here names the seed, which reproduces it exactly.
#[test]
fn one_hundred_seeds_satisfy_the_invariants_and_exercise_the_disk_faults() {
    let mut pongs = 0;
    let mut coverage = Coverage::default();
    for seed in 0..100 {
        let report = echo::run(seed);
        report
            .check()
            .unwrap_or_else(|violation| panic!("{violation}"));
        pongs += report.pongs_received();
        coverage.add(&report);
    }
    assert!(pongs > 0);
    coverage.assert_complete();
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

    fn assert_complete(&self) {
        eprintln!("journal fault coverage over the sweep: {self:?}");
        let counts = [
            ("bit rot", self.seeds_with_bit_rot),
            (
                "corrupt records caught by the checksum",
                self.seeds_with_corrupt_records,
            ),
            ("torn writes", self.seeds_with_torn_writes),
            ("torn files seen at replay", self.seeds_with_torn_files),
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

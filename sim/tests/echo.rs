//! Phase 0 exit criteria (SPEC.md §1.6): the echo scenario is deterministic, its moirae
//! trace hashes to a pinned value, and it doubles as a smoke test for the simulator
//! across many seeds.

use std::path::Path;

use ananke_env::{Environment, File, FileSystem, OpenOptions, RealEnv};
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
const GOLDEN: &str = "d3daa9b2184ebd18";

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

/// One hundred consecutive seeds: every run must satisfy the scenario's invariants.
/// A failure here names the seed, which reproduces it exactly.
#[test]
fn one_hundred_seeds_satisfy_the_invariants() {
    let mut pongs = 0;
    for seed in 0..100 {
        let report = echo::run(seed);
        report
            .check()
            .unwrap_or_else(|violation| panic!("{violation}"));
        pongs += report.pongs_received();
    }
    assert!(pongs > 0);
}

/// The pin rule: the body is everything after the header line's LF.
fn body_hash(jsonl: &str) -> String {
    let body = jsonl.split_once('\n').map_or("", |(_, rest)| rest);
    trace_hash(body)
}

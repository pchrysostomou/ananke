//! Phase 0 exit criteria (SPEC.md §1.6): the echo scenario is deterministic, and it
//! doubles as a smoke test for the simulator across many seeds.

use ananke_sim::echo;

/// Two runs with the same seed produce byte-identical trace text.
#[test]
fn same_seed_gives_byte_identical_trace() {
    let first = echo::run(42);
    let second = echo::run(42);
    assert_eq!(first.trace_text, second.trace_text);
    assert_eq!(first.trace_text.as_bytes(), second.trace_text.as_bytes());
    first.check().unwrap();
    assert!(
        first.trace_text.lines().count() > 1_000,
        "expected a substantial trace, got {} lines",
        first.trace_text.lines().count()
    );
}

/// Different seeds explore different runs.
#[test]
fn different_seeds_give_different_traces() {
    assert_ne!(echo::run(1).trace_text, echo::run(2).trace_text);
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

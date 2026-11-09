//! `sim/shard.rs`, the sharded sweep (SHARD.md §10; §12, Stage C): five nodes, three of
//! them the bootstrap nodes, ranges made by the splits the driver draws on stream
//! `shard`, leadership transfers, four clients over sixteen keys, and Phase 2's network
//! and disk faults.
//!
//! What it asserts, at every tier: the correct system passes every seed under every
//! check `sim/shard.rs`'s `Report::check` makes — checks 1 to 4 keyed by range, the range
//! layer's folds (7, 8, 9, 10, 16, 17, 18, 20), linearizability of every key across every
//! split, pre-vote's property, and on uniform seeds the timer check and the write bound —
//! and that nodes 4 and 5 hold nothing. It prints the driver's work and the refusals by
//! the loss that made them (#45).
// PROPOSED(D-113): `sim/shard.rs`, Stage C's sharded sweep.

use std::collections::BTreeMap;

use ananke_sim::shard::{self, DriverStats, Loss};
use ananke_sim::{seeds, sweep, verdict, write_trace};

/// What one seed of the sweep hands back: its verdict and its coverage.
struct Seen {
    verdict: Result<(), String>,
    splits: usize,
    splits_of_halves: usize,
    ranges_at_end: usize,
    driver: DriverStats,
    refusals: BTreeMap<Loss, usize>,
    refused: usize,
}

#[test]
fn every_seed_passes_on_the_correct_sharded_sweep() {
    let seen = sweep(seeds(), |seed| {
        let report = shard::run(seed);
        let verdict = report.check();
        if verdict.is_err() {
            write_trace(&format!("shard-{seed}"), &report.jsonl());
        }
        Seen {
            verdict,
            splits: report.splits(),
            splits_of_halves: report.splits_of_halves(),
            ranges_at_end: report.spans().user().count(),
            driver: report.driver.clone(),
            refusals: report.refusals_by_loss(),
            refused: report.refusals().len(),
        }
    });
    let count = seen.len();
    let splits: usize = seen.iter().map(|s| s.splits).sum();
    let halves: usize = seen.iter().map(|s| s.splits_of_halves).sum();
    let unsplit = seen.iter().filter(|s| s.splits == 0).count();
    let most_ranges = seen.iter().map(|s| s.ranges_at_end).max().unwrap_or(0);
    let asked: u64 = seen.iter().map(|s| s.driver.splits_asked).sum();
    let done: u64 = seen.iter().map(|s| s.driver.splits_done).sum();
    let skipped: u64 = seen.iter().map(|s| s.driver.splits_skipped).sum();
    let unanswered: u64 = seen.iter().map(|s| s.driver.splits_unanswered).sum();
    let transfers: u64 = seen.iter().map(|s| s.driver.transfers_asked).sum();
    let transferred: u64 = seen.iter().map(|s| s.driver.transfers_done).sum();
    let mut refused: BTreeMap<&str, u64> = BTreeMap::new();
    for s in &seen {
        for (reason, n) in &s.driver.refused {
            *refused.entry(reason).or_default() += n;
        }
    }
    let no_range_id_seeds = seen
        .iter()
        .filter(|s| !s.driver.no_range_id.is_empty())
        .count();
    let mut losses: BTreeMap<Loss, usize> = BTreeMap::new();
    for s in &seen {
        for (loss, n) in &s.refusals {
            *losses.entry(*loss).or_default() += n;
        }
    }
    let refusal_seeds = seen.iter().filter(|s| s.refused > 0).count();
    let refusals: usize = seen.iter().map(|s| s.refused).sum();
    println!(
        "shard: {count} seeds; {splits} splits took effect, {halves} of them of a range a \
         split made, on all but {unsplit} seeds; at most {most_ranges} user ranges at a run's \
         end; the driver asked {asked} splits, {done} done, refused {refused:?}, {unanswered} \
         unanswered, {skipped} with no span to split; {transferred}/{transfers} transfers \
         done; `NoRangeId` on {no_range_id_seeds}/{count} seeds (PROPOSED D-092)"
    );
    println!(
        "shard: {refusals} store refusals on {refusal_seeds}/{count} seeds, by the loss that \
         made them (#45): {losses:?}"
    );
    let verdicts: Vec<Result<(), String>> = seen.into_iter().map(|s| s.verdict).collect();
    verdict(&verdicts).unwrap();
}

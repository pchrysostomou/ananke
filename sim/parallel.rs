//! Running a scenario over many seeds at once (D-040).
//!
//! Every seed's `Sim` is independent: nothing in the workspace holds process-wide
//! mutable state, and a `Sim` draws every stream from its seed by name (D-017), so
//! the trace a seed produces does not depend on which thread ran it or on what ran
//! beside it. [`sweep`] runs seeds `0..count` across rayon's global thread pool and
//! hands back each seed's result in seed order. The pool is one per process, so
//! several sweeps in one test binary share it and no more simulations run at once
//! than the machine has cores, whatever the test harness's own parallelism.
//! `sim/tests/parallel.rs` proves the independence: a seed run alone and the same
//! seed run inside the driver, beside its neighbours, hash to the same trace.
//!
//! A sweep's closure returns something small — a verdict, a count, a summary — and
//! drops the report, since a thousand traces do not fit in memory together; the
//! failing seed's trace is written inside the closure, where the report still is.
//! [`verdict`] then names the first failing seed in seed order and every other one.
//!
//! This is the one place outside `crates/ananke-env` that puts work on host
//! threads. The scheduler's ban on spawning (clippy.toml) is about the code under
//! test, which must not escape the simulator; the driver only decides which
//! simulator runs next, and `scripts/check-direct-io.sh` allows `rayon` in this
//! file alone.

use rayon::prelude::*;

/// Runs `run` for every seed in `0..count`, seeds in parallel, and returns the
/// results in seed order.
pub fn sweep<T: Send>(count: u64, run: impl Fn(u64) -> T + Sync + Send) -> Vec<T> {
    (0..count).into_par_iter().map(run).collect()
}

/// The verdict of a positive control over per-seed results in seed order: `Ok`
/// when every seed passed, else the first violation, which must name its seed,
/// followed by the other failing seeds so a run that fails on several says so at
/// once.
///
/// # Errors
///
/// The first failing seed's violation, with the other failing seeds appended.
pub fn verdict(results: &[Result<(), String>]) -> Result<(), String> {
    let mut failing = results
        .iter()
        .enumerate()
        .filter_map(|(seed, result)| result.as_ref().err().map(|v| (seed, v)));
    let Some((_, first)) = failing.next() else {
        return Ok(());
    };
    let others: Vec<String> = failing.map(|(seed, _)| seed.to_string()).collect();
    if others.is_empty() {
        Err(first.clone())
    } else {
        Err(format!("{first} (seeds {} failed too)", others.join(", ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_come_back_in_seed_order() {
        assert_eq!(
            sweep(64, |seed| seed * 3),
            (0..64).map(|s| s * 3).collect::<Vec<_>>()
        );
        assert!(sweep(0, |seed| seed).is_empty());
    }

    #[test]
    fn the_verdict_names_the_first_failing_seed_and_the_rest() {
        assert_eq!(verdict(&[Ok(()), Ok(())]), Ok(()));
        let results = [
            Ok(()),
            Err("seed 1: first".to_owned()),
            Ok(()),
            Err("seed 3: later".to_owned()),
            Err("seed 4: later".to_owned()),
        ];
        assert_eq!(
            verdict(&results),
            Err("seed 1: first (seeds 3, 4 failed too)".to_owned())
        );
        assert_eq!(
            verdict(&[Err("seed 0: alone".to_owned())]),
            Err("seed 0: alone".to_owned())
        );
    }
}

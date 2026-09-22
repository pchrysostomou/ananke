//! `sim/raft.rs`'s arms on the node (SHARD.md §12, Stage B's first exit criterion):
//! three nodes, **four ranges on every node**, under the Phase 2 sweep's own fault
//! schedule.
//!
//! `sim/tests/ranges.rs` is the node's *structure* — four replicas a node, frames
//! carrying several ranges, creations that agree — under three arms of its own.
//! This binary is the node under the *raft sweep's* arms: the two lease trials, the
//! isolations, the leader isolation cut per range, the one-way blocks, the crashes,
//! the leader crash, `StaleSender`'s rule-5 shape and the Figure 8 driver, drawn
//! from the same seed and the same streams as the one-group sweep draws them
//! (`raft::Schedule::draw_on_the_node`).
//!
//! What it asserts, at every tier:
//!
//! - the correct node passes every seed, under every check the one-group sweep
//!   makes: the four log invariants keyed by range, linearizability of the
//!   clients' history, pre-vote's property per (range, server), the timer check,
//!   the liveness bound and the write bound per key of a range whose unimpaired
//!   replicas form a majority;
//! - Phase 2's variants on these arms are caught on the node too, each at the tier
//!   its **measured** rate supports and no stronger than its Phase 2 test asserts
//!   (§10, D-061). Every rate is printed;
//! - the run reaches the shape the keyed checks need: four replicas a node, every
//!   range electing and applying, and frames between two nodes carrying messages
//!   of several ranges;
//! - the paths this node has not got are absent, asserted with their reason: no
//!   snapshot action asked for and no store refused (`raft::Report::check`).
//!
//! It also prints the measurements SHARD.md §12 asks for under the sweeps: the
//! inbox's drops under its byte bound, the apply lag per range against §4's
//! twenty-millisecond threshold, how long one range's applies hold the node's
//! others, and the trace records a run holds per range per virtual second against
//! `TRACE_CAP`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ananke_raft::core::{Variant, Variants};
use ananke_shard::variant::NodeVariants;
use ananke_sim::raft::{self, Cluster};
use ananke_sim::{seeds, sweep, verdict, write_trace};

/// The correct node, on the seeds this tier runs.
fn correct(seed: u64) -> raft::Report {
    raft::run_on_the_node(seed, Variants::default(), NodeVariants::correct())
}

/// A node whose cores carry `variants`, with the node itself correct.
fn buggy(seed: u64, variants: impl Into<Variants>) -> raft::Report {
    raft::run_on_the_node(seed, variants, NodeVariants::correct())
}

/// The ranges the node hosts.
fn ranges() -> Vec<u64> {
    Cluster::Node.ranges()
}

/// The run's verdict: every check the one-group sweep makes, then the snapshot path
/// asserted **reached on every seed**, and then the one path this node still has not
/// got, asserted **absent with its reason**.
///
/// Both halves are the same rule, which is why they sit together. A variant whose
/// situation the run cannot reach is a test that passes because nothing was injected,
/// the one failure mode a sweep cannot report on its own (CLAUDE.md). Until PROPOSED
/// D-083 the node had no `snapshot` task, so this function asserted the *absence* of a
/// snapshot action and of the condition behind one, naming the slice that owed the
/// path. The path is wired now and this slice puts the four stream variants of §10
/// under these arms, so the absence becomes its opposite: a seed that takes no
/// snapshot fails here, because `SnapshotWithoutCurrentLast` and `SharedSnapshotDir`
/// assert nothing on a run with no take and no stream in it.
///
/// What stays an absence is the refusal, which is Q15's and PR #86's. It is asserted
/// exactly as it was, so the day that path arrives the sweep says so rather than
/// passing over it.
// PROPOSED(D-086): the snapshot path is reached, and the absence becomes a reach.
fn checked(report: &raft::Report) -> Result<(), String> {
    let seed = report.seed;
    report.check()?;
    // The reach, per seed. `snapshot_actions` counts what a core *asked* for, which
    // the node's host counts whether or not the task served it; the takes and the
    // installs below are what the task actually did, read off the trace. The three
    // are asserted separately on purpose: an action asked for and never served is
    // exactly the shape D-082 found `SendBeforePersist` in — a bit set, carried, and
    // read by nothing — and a count of asks would not have shown it.
    if report.snapshot_actions() == 0 {
        return Err(format!(
            "seed {seed}: no core asked for a snapshot action at all, though \
             `snapshot_threshold` is {}: the stream variants of §10 assert nothing on a \
             run with no take in it, so this seed's evidence is vacuous rather than \
             green",
            raft::NODE_SNAPSHOT_THRESHOLD
        ));
    }
    let takes = report.snapshot_takes();
    if takes.is_empty() {
        return Err(format!(
            "seed {seed}: a core asked for a snapshot action and no take completed, so \
             nothing was checkpointed for a stream to read: `SharedSnapshotDir`'s \
             re-take and `SnapshotWithoutCurrentLast`'s install both assert nothing here"
        ));
    }
    if let Some((server, reason)) = report.refused.first() {
        return Err(format!(
            "seed {seed}: server {server} refused its store ({reason}), a path this node does \
             not have: Q15's whole-node refusal and re-seed are PR #86's. \
             `RefusalNotDurable` is not re-asserted here, and a run that reaches this must \
             say so rather than pass"
        ));
    }
    Ok(())
}

#[test]
fn a_node_under_the_raft_sweeps_arms_runs_every_range() {
    let report = correct(1);
    checked(&report).expect("the correct node passes seed 1");
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    let mut applies: BTreeMap<u64, usize> = BTreeMap::new();
    for record in &report.records {
        match &record.event {
            ananke_env::TraceEvent::RaftLeader { range, .. } => {
                *leaders.entry(*range).or_default() += 1;
            }
            ananke_env::TraceEvent::RaftApply { range, .. } => {
                *applies.entry(*range).or_default() += 1;
            }
            _ => {}
        }
    }
    println!("node: seed 1 leaders by range {leaders:?}, applies by range {applies:?}");
    for range in ranges() {
        assert!(
            leaders.get(&range).copied().unwrap_or(0) > 0,
            "range {range} never elected a leader: {leaders:?}"
        );
        assert!(
            applies.get(&range).copied().unwrap_or(0) > 0,
            "range {range} applied nothing: {applies:?}"
        );
    }
}

#[test]
fn every_seed_passes_on_the_correct_node_under_the_raft_sweeps_arms() {
    let seeds = seeds();
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    let mut applies: BTreeMap<u64, usize> = BTreeMap::new();
    let mut drops: BTreeMap<(String, u64), usize> = BTreeMap::new();
    let mut lags_by_range: BTreeMap<u64, Vec<Duration>> = BTreeMap::new();
    let mut holds: Vec<Duration> = Vec::new();
    let mut per_range_per_second = 0.0f64;
    let mut busiest_range_per_second = 0.0f64;
    let mut multi_range_frames = 0usize;
    let mut records = 0usize;
    let results = sweep(seeds, |seed| {
        let report = correct(seed);
        let outcome = checked(&report);
        if outcome.is_err() {
            write_trace(&format!("node-{seed}"), &report.jsonl());
        }
        let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
        let mut applies: BTreeMap<u64, usize> = BTreeMap::new();
        for record in &report.records {
            match &record.event {
                ananke_env::TraceEvent::RaftLeader { range, .. } => {
                    *leaders.entry(*range).or_default() += 1;
                }
                ananke_env::TraceEvent::RaftApply { range, .. } => {
                    *applies.entry(*range).or_default() += 1;
                }
                _ => {}
            }
        }
        let drops: BTreeMap<(String, u64), usize> = report
            .inbox_drops()
            .into_iter()
            .map(|((kind, range), count)| ((kind.to_owned(), range), count))
            .collect();
        (
            outcome,
            (
                leaders,
                applies,
                drops,
                report.apply_lags(),
                report.cross_range_apply_holds(),
                (
                    report.records_per_second_per_range(),
                    report.busiest_range_records_per_second(),
                    report.frames_of_several_ranges(),
                    report.records.len(),
                ),
                (
                    report.lease_revokes(),
                    report.lease_reads(),
                    report.read_index_reads(),
                    report.drift_exceeded(),
                    report.snapshot_actions(),
                ),
                report.arms_hit_their_ranges(),
                (
                    retook_into_one_directory(&report),
                    took_an_index_twice_on_the_node(&report),
                ),
            ),
        )
    });
    let mut revoked = 0usize;
    let mut lease_reads = 0usize;
    let mut read_index_reads = 0usize;
    let mut exceeded = 0usize;
    let mut snapshot_actions = 0usize;
    let mut arms_hit = 0usize;
    let mut arms_fired = 0usize;
    let mut retook = 0usize;
    let mut at_one_index = 0usize;
    let mut least_actions = usize::MAX;
    for (_, (by_leader, by_apply, dropped, lags, held, rates, lease, arms, takes)) in &results {
        retook += usize::from(takes.0);
        at_one_index += usize::from(takes.1);
        let (rate, busiest, several, len) = rates;
        let (revokes, leased, round_trip, drifted, actions) = lease;
        arms_hit += arms.0;
        arms_fired += arms.1;
        revoked += usize::from(*revokes > 0);
        lease_reads += leased;
        read_index_reads += round_trip;
        exceeded += usize::from(*drifted);
        snapshot_actions += actions;
        least_actions = least_actions.min(*actions);
        for (range, count) in by_leader {
            *leaders.entry(*range).or_default() += count;
        }
        for (range, count) in by_apply {
            *applies.entry(*range).or_default() += count;
        }
        for (key, count) in dropped {
            *drops.entry(key.clone()).or_default() += count;
        }
        for (range, of_range) in lags {
            lags_by_range
                .entry(*range)
                .or_default()
                .extend(of_range.iter().copied());
        }
        holds.extend(held.iter().copied());
        per_range_per_second = per_range_per_second.max(*rate);
        busiest_range_per_second = busiest_range_per_second.max(*busiest);
        multi_range_frames += several;
        records += len;
    }
    let (median_lag, per_range_median) = medians(&lags_by_range);
    holds.sort_unstable();
    println!(
        "node: {seeds} seeds, leaders by range {leaders:?}, applies by range {applies:?}, \
         {records} records in all; at most {per_range_per_second:.0} trace records per virtual \
         second per range and {busiest_range_per_second:.0} of the busiest range's own, against \
         TRACE_CAP of {}; {multi_range_frames} peer frames carried messages of more than one \
         range",
        raft::TRACE_CAP
    );
    println!("node: the inbox dropped {drops:?} under its byte bound");
    println!(
        "node: apply lag, median over every range {median_lag:?}, per range {per_range_median:?}, \
         against SHARD.md §4's threshold of {HEARTBEAT:?}; {} applies measured",
        lags_by_range.values().map(Vec::len).sum::<usize>()
    );
    println!(
        "node: one range's applies held another's for a median of {:?} and at most {:?}, over {} \
         waits that crossed a range",
        median_of(&holds),
        holds.last(),
        holds.len()
    );
    println!(
        "node: the drift bound was exceeded on {exceeded}/{seeds} seeds and the correct node's          guard revoked on {revoked}/{seeds}; {lease_reads} reads were served by a lease and          {read_index_reads} after a heartbeat round; {snapshot_actions} snapshot actions were          asked for"
    );
    // The lease trial's firing, at every tier (§10, D-061): a tier whose clocks never
    // exceed the bound, or whose correct node never revokes, says nothing about the
    // guard, and `LeaseTrustsTheClock`'s catch below would be a green that means
    // nothing. This is where the correct node is run, so the variant's own test runs
    // the buggy node alone.
    assert!(
        exceeded > 0,
        "no seed of this tier exceeded the drift bound, so the guard was never asked"
    );
    assert!(
        revoked > 0,
        "the correct node never revoked a promise, so the guard's path was not reached"
    );
    assert!(
        lease_reads > 0,
        "no read was served by a lease, so there is no stale read for the variant to serve"
    );
    let aimed_rate = if arms_fired == 0 {
        0.0
    } else {
        arms_hit as f64 * 100.0 / arms_fired as f64
    };
    println!(
        "node: {arms_hit}/{arms_fired} leader-relative arms ({aimed_rate:.1}%) hit the leader of \
         the range they drew"
    );
    // §11, env 8's teeth. An arm that resolved "the leader" without its range would
    // cut off whichever range elected last — a perfectly good fault that no check of
    // the run would report, which is why this is here rather than left to the run's
    // other checks.
    //
    // Both figures were measured before the floor was written (D-061), by planting the
    // mutation: the correct node hits **115 of 115** over a hundred seeds, and a
    // `leader_of_range` that ignores its range argument hits **82 of 115, 71.3 %** —
    // not one in four, because three nodes hold four ranges and the leader of the
    // range that elected last is often the leader of the range the arm drew as well.
    // The floor sits between them with room on both sides. It is a bound the correct
    // system must not trip, so a tier that comes in under it is a model error to take
    // to the owner and not a number to widen (D-030, D-039).
    assert!(arms_fired > 0, "no leader-relative arm fired over the tier");
    assert!(
        aimed_rate >= 90.0,
        "{arms_hit} of {arms_fired} leader-relative arms hit the leader of the range they drew \
         ({aimed_rate:.1}%), under the 90% floor: a harness that resolved a leader without its \
         range sits at about 71%"
    );
    // The shape the keyed checks need, on every seed: four ranges on every node,
    // each electing and applying. It is what says the sweep can tell a keyed check
    // from a wrongly keyed one at all, so it is asserted at every tier.
    for range in ranges() {
        assert!(leaders.contains_key(&range), "range {range} never led");
        assert!(applies.contains_key(&range), "range {range} never applied");
    }
    // And the work is spread over the four, not piled on one. "Every range applied
    // something" is satisfied by a range that applied only its leader's no-ops, which
    // is what the node looks like when the scenario's key map sends every client key
    // to one range: the sweep would then be a one-range sweep wearing four names, and
    // every check keyed by range would have nothing to be wrong about.
    //
    // Measured before it was asserted (D-061), by planting that mutation. Over a
    // hundred seeds the correct node applies {2: 31 264, 3: 36 021, 4: 32 403,
    // 5: 33 776} — least over busiest, **0.87** — and a key map that answers one range
    // gives {2: 95 986, 3: 16 459, 4: 12 886, 5: 14 353}, **0.13**. The floor is half,
    // between them and far from both.
    let least = applies.values().copied().min().unwrap_or(0);
    let busiest = applies.values().copied().max().unwrap_or(0);
    let spread = least as f64 / busiest.max(1) as f64;
    println!(
        "node: the least-applied range took {least} entries against the busiest range's \
         {busiest}, a spread of {spread:.2}"
    );
    assert!(
        spread >= 0.5,
        "the least-applied range took {least} entries against the busiest's {busiest} \
         ({spread:.2}), which is what a sweep whose client work all lands on one range looks \
         like"
    );
    // Read off the frames themselves, not off the scenario's parameters: a frame
    // between two nodes carries messages of several ranges, which is what four
    // ranges to a node is the parameter for (SHARD.md §12). The floor was measured
    // before it was asserted (D-061) and is recorded in the entry.
    assert!(
        multi_range_frames >= 100 * seeds as usize,
        "{multi_range_frames} frames of several ranges over {seeds} seeds is too few to say a \
         frame between two nodes carries several ranges"
    );
    // **The correct node never writes two takes of one range at one index into one
    // version directory**, over the tier. That is what the directory's take counter is
    // for (D-043, D-075): every take goes to a name of its own, so a stream reading one
    // version is never read out from under. It is the property `SharedSnapshotDir`
    // turns off.
    //
    // The *index* alone is not the property and the figure beside it says so: the
    // correct node re-takes at an index it has already taken at on 44 of 100 seeds,
    // after
    // an install leaves the core's `taken` naming a snapshot this replica never took
    // (D-078) while the applied index stands still. Asserting the index would have been
    // a bound the correct system trips.
    // PROPOSED(D-086): the take counter's property, keyed by range.
    println!(
        "node: {snapshot_actions} snapshot actions over {seeds} seeds, {:.1} a seed, fewest on \
         any one seed {least_actions}",
        snapshot_actions as f64 / seeds as f64
    );
    // **A floor on the snapshot actions a seed asks for, on average over the tier.**
    // Nothing else in the tree checks that a replica which asks for a snapshot gets an
    // answer, and D-078's follower compaction has no direct check at all: a `Record` is
    // written between two applies and traces nothing, by design, so the ask and the
    // answer are both invisible. What *is* visible is the consequence — the core sets
    // `take_pending` when it asks and only an answer clears it, so a node that drops
    // the ask stops taking snapshots altogether.
    //
    // Measured, all on the same tiers: the correct node asks **60.4 a seed** over 100
    // seeds (6 041, fewest 26 on any one) and **58.8** over 20 (1 176, fewest 32). A
    // node whose `Record` goes to the `snapshot` task, which drops it, asks **11.6**
    // over 20; one that routes `Record` to the node's *first* range rather than the
    // range that asked — textually a no-op on one group, and a mutation no
    // single-range world could catch — asks **23.3**.
    //
    // The floor is **30 a seed**: a little under half the correct node's mean, and
    // over both mutants. It is taken over the tier and not per seed because a single
    // heavily-partitioned seed writes little, and a per-seed minimum would be a bound
    // the correct system could trip (D-030, D-039); a mean over the tier moves only
    // when the mechanism does.
    //
    // The range-routing mutant is why the floor is not 20: at 20 it passed, and the
    // node sweep sees it otherwise only from seed 25, so the gate's twenty were green.
    // PROPOSED(D-086): a floor on the snapshot actions a seed asks for.
    const ACTIONS_A_SEED: usize = 30;
    assert!(
        snapshot_actions >= ACTIONS_A_SEED * seeds as usize,
        "the node asked for {snapshot_actions} snapshot actions over {seeds} seeds, under the \
         floor of {ACTIONS_A_SEED} a seed: a replica that asks for a take or a compaction \
         record and is not answered keeps `take_pending` set and never asks again, which is \
         how a range wedges (PROPOSED D-086)"
    );
    println!(
        "node: the correct node re-took a range at an index it had already taken it at on \
         {at_one_index}/{seeds} seeds — which is legitimate after an install — and into the \
         **same directory** on {retook}/{seeds}"
    );
    assert_eq!(
        retook, 0,
        "the correct node took a snapshot of a range at an index it had already taken that \
         range at *and wrote it into the same version directory*, which a stream may have \
         open: that is the behaviour `SharedSnapshotDir` exists to be the opposite of, and \
         the take counter exists to prevent"
    );
    let verdicts: Vec<Result<(), String>> =
        results.into_iter().map(|(verdict, _)| verdict).collect();
    verdict(&verdicts).expect("the correct node passes every seed");
}

/// One heartbeat interval: two ticks (SHARD.md §4), and the threshold §4 sets for
/// the median apply lag.
const HEARTBEAT: Duration = Duration::from_millis(20);

fn median_of(sorted: &[Duration]) -> Option<Duration> {
    (!sorted.is_empty()).then(|| sorted[(sorted.len() - 1) / 2])
}

fn medians(by_range: &BTreeMap<u64, Vec<Duration>>) -> (Option<Duration>, BTreeMap<u64, Duration>) {
    let mut all: Vec<Duration> = Vec::new();
    let mut per_range = BTreeMap::new();
    for (range, lags) in by_range {
        let mut lags = lags.clone();
        all.extend(lags.iter().copied());
        lags.sort_unstable();
        if let Some(median) = median_of(&lags) {
            per_range.insert(*range, median);
        }
    }
    all.sort_unstable();
    (median_of(&all), per_range)
}

/// The seeds a variant caught at a high rate runs: a tenth of the tier and never
/// fewer than twenty, `sim/tests/engine.rs`'s `high_rate_share` (D-055, D-061).
///
/// The rate the assertion rests on is over the share, as D-061 requires, and the
/// share is what keeps this binary's cost near the sweeps beside it. Every variant
/// run on the share was measured over the share before the assertion was written,
/// and the lowest of them is `ResetTimerOnAnyRpc`: at its rate a share of twenty
/// catches none with probability about 1e-6 and a share of a hundred about 1e-30.
// PROPOSED(D-082): the high-rate variants on the node run a share of the tier.
fn high_rate_share() -> u64 {
    (seeds() / 10).max(seeds().min(20))
}

/// How often `variants` is caught over the share, printed.
fn caught_on(name: &str, variants: impl Into<Variants> + Copy + Send + Sync) -> (usize, u64) {
    let seeds = high_rate_share();
    let results = sweep(seeds, |seed| (checked(&buggy(seed, variants)).is_err(), ()));
    let caught = results.iter().filter(|(failed, ())| *failed).count();
    let rate = caught as f64 * 100.0 / seeds as f64;
    println!("node: {name} caught on {caught}/{seeds} seeds ({rate:.1}%)");
    (caught, seeds)
}

// Phase 2's variants on `sim/raft.rs`'s arms, re-asserted on the node (§10, §12).
// Each is caught on some seed at every tier on the one-group server; the rate on
// the node was measured before the assertion was written (D-061) and is in the
// entry. Where a rate falls under 5 % the assertion moves to the thousand-seed
// tier, as D-061 requires, and says so.

#[test]
fn a_server_that_sends_before_it_persists_is_caught_on_the_node() {
    // Against Q41's round the one that matters most: a send that follows a core's
    // persist leaves when that persist resolves, and the variant sends it first
    // (§10). On the node the round submits four ranges' persists together, so the
    // send the variant lets out early races a sync that carries other ranges' work.
    let (caught, _) = caught_on("SendBeforePersist", Variant::SendBeforePersist);
    assert!(caught > 0, "SendBeforePersist was never caught on the node");
}

#[test]
fn a_server_that_applies_before_commit_is_caught_on_the_node() {
    let (caught, _) = caught_on("ApplyBeforeCommit", Variant::ApplyBeforeCommit);
    assert!(caught > 0, "ApplyBeforeCommit was never caught on the node");
}

#[test]
fn a_server_without_pre_vote_is_caught_on_the_node() {
    let (caught, _) = caught_on("NoPreVote", Variant::NoPreVote);
    assert!(caught > 0, "NoPreVote was never caught on the node");
}

#[test]
fn a_leader_that_commits_an_older_terms_entry_by_count_is_caught_on_the_node() {
    // The Figure 8 driver's window (D-031), on the node: the burst writes a key of
    // the range the arm drew, so the backlog the restarted leader re-sends is that
    // range's.
    let (caught, _) = caught_on("CountOlderTermForCommit", Variant::CountOlderTermForCommit);
    assert!(
        caught > 0,
        "CountOlderTermForCommit was never caught on the node"
    );
}

#[test]
fn a_follower_that_truncates_on_every_append_is_caught_on_the_node() {
    let (caught, _) = caught_on("TruncateOnEveryAppend", Variant::TruncateOnEveryAppend);
    assert!(
        caught > 0,
        "TruncateOnEveryAppend was never caught on the node"
    );
}

#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_node() {
    let (caught, _) = caught_on("ResetTimerOnAnyRpc", Variant::ResetTimerOnAnyRpc);
    assert!(
        caught > 0,
        "ResetTimerOnAnyRpc was never caught on the node"
    );
}

#[test]
fn a_leader_that_trusts_the_clock_is_caught_on_the_node() {
    // The lease trial on the node hands **every** range to the slowest clock, so a
    // node cut off with the reading client holds four leases and not one. What the
    // correct node does — revoke — and that some seed exceeds the drift bound at all
    // are asserted where the correct node is already run, in the sweep above; here
    // the buggy node runs alone, which is what keeps this test to one run a seed.
    //
    // The catch is a stale read, found by linearizability, and it is asserted from
    // the thousand-seed tier: exactly where the one-group test asserts it and no
    // stronger (§10, §12). On one group the stale read is caught on 4.0 % of the
    // first thousand seeds, and D-061 puts a catch under 5 % at `seeds() >= 1000` —
    // at 4 % the gate's twenty catch none with probability 0.44 and a hundred with
    // 0.017, so an assertion there would fail a tree with nothing wrong the day a
    // change redraws the schedules. The node's own rate is in the entry.
    let seeds = seeds();
    let caught: Vec<String> = sweep(seeds, |seed| {
        checked(&buggy(seed, Variant::LeaseTrustsTheClock)).err()
    })
    .into_iter()
    .flatten()
    .collect();
    let rate = caught.len() as f64 * 100.0 / seeds as f64;
    println!(
        "node: LeaseTrustsTheClock caught on {}/{seeds} seeds ({rate:.1}%), first: {}",
        caught.len(),
        caught.first().map_or("", String::as_str)
    );
    if seeds >= 1000 {
        assert!(
            !caught.is_empty(),
            "LeaseTrustsTheClock was never caught on the node"
        );
    }
}

#[test]
fn the_nodes_arms_aim_at_every_range_and_not_at_one() {
    // §11, env 8: a leader-relative arm on a node of many ranges chooses its range
    // from its own stream. What says the draw is spent is the set of ranges the
    // schedules of a run of seeds aim at — one range would mean three of the four
    // never saw a leader isolation, a leader crash or a Figure 8 burst at all.
    let mut aimed: BTreeSet<u64> = BTreeSet::new();
    for seed in 0..64 {
        let schedule = raft::Schedule::draw_on_the_node(seed);
        assert_eq!(schedule.range_picks.len(), schedule.faults.len());
        for i in 0..schedule.faults.len() {
            aimed.insert(schedule.range_of(Cluster::Node, i));
        }
    }
    println!("node: over 64 seeds the arms aimed at ranges {aimed:?}");
    assert_eq!(
        aimed,
        ranges().into_iter().collect::<BTreeSet<u64>>(),
        "the arms do not reach every range of the node"
    );
}

#[test]
fn a_seed_replays_to_the_same_trace_on_the_node() {
    // The node's schedule is the simulator's: one engine, one socket and four cores
    // on one ticker, with the persists of a round resolved in the order the
    // scheduling stream draws (D-073). Two runs of one seed must still be the same
    // run, records and all (SPEC.md §1.6).
    let first = correct(2);
    let second = correct(2);
    assert_eq!(first.records.len(), second.records.len());
    for (a, b) in first.records.iter().zip(&second.records) {
        assert_eq!(a.at, b.at);
        assert_eq!(a.event, b.event);
    }
}

// Phase 2's four **stream** variants, re-asserted on the node (§10, §12's Stage B).
//
// These four were the part of Stage B's variant criterion that PROPOSED D-082 recorded
// as blocked: "until [the install path] exists a variant on that path cannot be
// re-asserted on the node at all". PROPOSED D-083 built the wiring and left them owed,
// by name. This slice puts them under `sim/raft.rs`'s arms, with the node's
// `snapshot_threshold` at the one-group sweep's 12 and `Fault::CrashInstalling` and
// `Fault::RetakeUnderStream` back in `Schedule::draw_on_the_node`.
//
// Each is asserted to the standard its Phase 2 test asserts and no stronger, at the
// tier that test uses today (Q39). Every rate below was measured on the node before its
// assertion was written (D-061) and is printed at every tier.

/// The install that makes its switch without the range's repair (RAFT.md:694, D-066),
/// on the node — **injected, and not yet caught at its Phase 2 tier**.
///
/// Phase 2's standard is `is_caught`: caught on some seed at every tier
/// (`a_server_that_installs_without_current_last_is_caught`, sim/tests/raft.rs). On the
/// node the measured rate does not support it, and D-061's rule is that the tier a
/// Phase 2 variant keeps is the owner's, so the catch is **not** asserted here and the
/// numbers go to the owner instead of a quieter assertion (PROPOSED D-086):
///
/// - the variant is **injected**, which the assertion below tests from the
///   thousand-seed tier: the arm has to have crashed its victim at the final chunk of
///   the range it drew for the switch-without-repair to have been reached at all, and
///   that count is 0 when the variant's bit is disconnected;
/// - `Fault::CrashInstalling` reached the final chunk of the range it drew and crashed
///   its victim there on **1 of 100 seeds**, against one group, where the arm's whole
///   scenario is the one range it has. On a node the victim is drawn without regard to
///   which of its four ranges it is behind on, so the arm must find a victim that is
///   behind *that* range's compacted prefix, designated for it, and streamed within
///   `INSTALL_WAIT_BUDGET`;
/// - the catch is **0 of 100 seeds**, which at an arm firing on 1 % is what a variant
///   that is never aimed at looks like, not a variant that is aimed at and survives.
///
/// So the catch is asserted nowhere and the arm's firing from the thousand-seed tier,
/// which is what D-061 allows at a measured 1 %. The rate is printed at every tier so
/// the day the arm is aimed better the number is visible, and the variant keeps its
/// Phase 2 assertion on `Cluster::OneGroup`, which this sweep leaves running exactly as
/// it is.
// PROPOSED(D-086): Phase 2's stream variants re-asserted on the node.
#[test]
fn a_server_that_installs_without_current_last_is_injected_on_the_node() {
    let seeds = seeds();
    let outcomes: Vec<(Option<String>, usize, usize)> = sweep(seeds, |seed| {
        let report = buggy(seed, Variant::SnapshotWithoutCurrentLast);
        (
            checked(&report).err(),
            report.aimed_installs,
            report.snapshot_actions(),
        )
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _, _)| v.as_ref()).collect();
    let fired = outcomes.iter().filter(|(_, aimed, _)| *aimed > 0).count();
    let actions: usize = outcomes.iter().map(|(_, _, a)| a).sum();
    let rate = caught.len() as f64 * 100.0 / seeds as f64;
    println!(
        "node: SnapshotWithoutCurrentLast caught on {}/{seeds} seeds ({rate:.1}%), the install \
         crash reached the final chunk of the range it drew and crashed there on {fired}/{seeds} \
         seeds, {actions} snapshot actions asked for, first: {}",
        caught.len(),
        caught.first().map_or("", |v| v.as_str())
    );
    // **The arm's own firing, from the thousand-seed tier.** This is the only thing
    // here that can fail, and until PROPOSED D-086's review the test asserted
    // `actions > 0` instead — which `checked()` already asserts on every seed of every
    // sweep, so the test could not fail at all. Removing the variant's bit from
    // `with_repair` left the node binary green at a hundred seeds and
    // `sim/tests/install.rs` green at twenty: a bit set, carried, and read by nothing,
    // which is the exact shape D-082 found `SendBeforePersist` in and this entry
    // claimed to have fixed.
    //
    // `aimed_installs` is the discriminator and it moves: **1 of 100** on the correct
    // wiring against **0 of 100** with the bit disconnected. D-061 puts a figure under
    // 5 % at `seeds() >= 1000`, so that is where it is asserted; at the gate's twenty
    // and CI's hundred the rate is printed and nothing is claimed. Aiming the arm at a
    // range its victim is behind on should put it at 10 to 17 %, which would carry it
    // at a hundred — that is a change to how the arms are drawn and is the owner's
    // (PROPOSED D-086).
    if seeds >= 1000 {
        assert!(
            fired > 0,
            "`Fault::CrashInstalling` never crashed its victim at the final chunk of the \
             range it drew over {seeds} seeds, so `SnapshotWithoutCurrentLast` was not \
             injected at the moment it is about and its 0 catches say nothing"
        );
    }
    let _ = actions;
}

/// The leader that shares one snapshot directory per index and streams one follower at
/// a time (D-043), on the node.
///
/// Phase 2 asserts four things
/// (`a_leader_that_shares_one_snapshot_directory_and_streams_one_follower_at_a_time_is_caught`).
/// All four are asserted, each at the tier **this cluster's** measured rate supports
/// rather than one group's — three at Phase 2's own tiers, one moved down:
///
/// - **the fault fired**, at every tier as Phase 2 has it: a take at an index that
///   server had already taken **of that range**, the re-take the variant rewrites one
///   directory for. **100 of 100** seeds — every one;
/// - **the aimed arm reached its stream**, moved to the thousand-seed tier where Phase
///   2 has it at every tier. **3 of 100**, under D-061's 5 %: the variant wedges the
///   run so readily that `RetakeUnderStream`'s own setup often never completes;
/// - **the wedge's stream half** — that re-take landing under a live stream the
///   follower never installs at afterwards — from the **thousand-seed** tier, where
///   Phase 2 has it from a hundred, which its rate here supports: **12 of 100** against
///   13.5 % of a thousand there;
/// - **the liveness catch at ten thousand**, Phase 2's own tier and no stronger. On the
///   node it is **36 of 100** against one group's **4 of 10 000**: four ranges share one
///   directory name per index and one `snapshot` task, so a re-take under a live stream
///   is not the coincidence it is on a server. That rate would carry an assertion at a
///   hundred seeds and it is deliberately not written there — §12 re-asserts a Phase 2
///   variant to its own standard **and no stronger**.
///
/// Every fold behind these is keyed by `(server, range, index)` since PROPOSED D-086.
/// On one group they were keyed by `(server, index)`, which named a take uniquely
/// there; on a node four ranges cross `snapshot_threshold` within a few indices of one
/// another, so `(server, index)` names two different snapshots of two different ranges
/// over and over.
///
/// **None of these numbers existed until the variant could express its bug.** Until
/// PROPOSED D-086's review the node's take did not empty its version directory before
/// checkpointing, `Engine::checkpoint_spans` refuses a directory that is not empty, and
/// the variant's re-take therefore *failed* rather than rewriting — so all three folds
/// read zero and the first measurement of this variant on the node was a measurement of
/// that defect. `ServerApplier::take` empties it now, as `snapshot::take_numbered`
/// always has for the one-group server.
// PROPOSED(D-086): Phase 2's stream variants re-asserted on the node.
#[test]
fn a_leader_that_shares_one_snapshot_directory_is_caught_on_the_node() {
    // The share, as the high-rate variants use (D-055, D-061), and here it is a cost
    // decision as much as a statistical one. This variant **wedges the node on 36 % of
    // seeds**, against one group's 4 in 10 000, and a wedged run plays out its whole
    // length with a scrambled stream restarting under it — so the full tier costs many
    // times what the correct node's does. At rates of 100 % (the fault), 12 % (the
    // scramble) and 36 % (the catch) a share measures each as well as the tier: over a
    // share of 20 the catch is missed with probability 0.64^20, about one run in
    // 10 000. The rate is over the share, as D-061 requires (PROPOSED D-086).
    let seeds = high_rate_share();
    let outcomes: Vec<(Option<String>, bool, usize, bool, usize)> = sweep(seeds, |seed| {
        let report = buggy(seed, Variant::SharedSnapshotDir);
        let scrambled: Vec<_> = report
            .retakes_under_streams()
            .into_iter()
            .filter(|retake| !retake.installed_after)
            .collect();
        // D-043's own symptom under a scrambled stream: the follower answering `More`
        // for a file it has already been sent, over and over — of that range's stream.
        let looped = scrambled
            .iter()
            .map(|retake| {
                report.duplicate_chunk_loop(
                    retake.leader,
                    retake.range,
                    retake.follower,
                    retake.retook,
                )
            })
            .sum();
        (
            checked(&report).err(),
            took_an_index_twice_on_the_node(&report),
            report.aimed_streams,
            !scrambled.is_empty(),
            looped,
        )
    });
    let caught: Vec<&String> = outcomes
        .iter()
        .filter_map(|(v, _, _, _, _)| v.as_ref())
        .collect();
    let fired = outcomes.iter().filter(|(_, fired, _, _, _)| *fired).count();
    let aimed = outcomes
        .iter()
        .filter(|(_, _, aimed, _, _)| *aimed > 0)
        .count();
    let scrambled = outcomes.iter().filter(|(_, _, _, s, _)| *s).count();
    let looped: usize = outcomes.iter().map(|(_, _, _, _, l)| l).sum();
    let liveness = caught.iter().filter(|v| v.contains(": liveness: ")).count();
    println!(
        "node: SharedSnapshotDir caught on {}/{seeds} seeds, {liveness} by the liveness check, \
         re-took at an index already taken of one range on {fired} seeds, scrambled a live \
         stream the follower never installed after on {scrambled} seeds ({looped} \
         duplicate-chunk loops after those), the aimed re-take arm reached its stream on \
         {aimed} seeds, first: {}",
        caught.len(),
        caught.first().map_or("", |v| v.as_str())
    );
    // **The fault itself, at every tier**, as Phase 2 asserts it: a take at an index
    // this server had already taken **that range** at, which is the re-take the variant
    // rewrites one directory for. **100 of 100** seeds — every seed — so the gate's
    // twenty carry it with nothing to spare.
    assert!(
        fired > 0,
        "SharedSnapshotDir never re-took at an index it had already taken that range at: the \
         fault was not injected on any of the {seeds} seeds"
    );
    // **The wedge's stream half, from the hundred-seed tier**, which is Phase 2's own
    // tier for it: that re-take landing under a live stream the follower never installs
    // at afterwards. **12 of 100** here against 13.5 % of a thousand on one group — the
    // same order, and above D-061's 5 %, so the tier is Phase 2's unchanged. At the
    // gate's twenty a 12 % rate sees none about one run in thirteen, which is why it is
    // not asserted there.
    if seeds >= 100 {
        assert!(
            scrambled > 0,
            "SharedSnapshotDir never re-took into a directory a live stream had open and left \
             unfinished: the wedge's stream half was not built on any of the {seeds} seeds"
        );
    }
    // **The aimed arm, from the thousand-seed tier**, where Phase 2 asserts it at every
    // tier. It reaches a stream of the range it drew on **3 of 100** seeds here, under
    // D-061's 5 %: the variant wedges the run so readily that `RetakeUnderStream`'s own
    // setup often never completes. This is the one assertion of the four that is weaker
    // than Phase 2's, it is weaker because the measurement says so, and the number goes
    // to the owner with it (PROPOSED D-086).
    if seeds >= 1000 {
        assert!(
            aimed > 0,
            "the aimed re-take arm never reached a stream of the range it drew on the node \
             over {seeds} seeds"
        );
    }
    // **The liveness catch at the nightly's ten thousand, which is Phase 2's tier for
    // it and no stronger.** On the node it is **36 of 100** against one group's 4 of
    // 10 000: the variant wedges a node far more readily than a server, because four
    // ranges share one directory name per index and one `snapshot` task. The rate would
    // carry an assertion at a hundred seeds, and it is **not** written there — Phase 2
    // asserts this catch at ten thousand and this re-assertion is held to that standard
    // and no stronger (§10, §12's Stage B).
    if seeds >= 10_000 {
        assert!(
            liveness > 0,
            "SharedSnapshotDir's wedge was never caught by the liveness check on the node"
        );
    }
    println!(
        "node: SharedSnapshotDir's liveness catch is {liveness}/{seeds}, asserted at ten \
         thousand as Phase 2 asserts it and no stronger"
    );
}

/// Whether some server took a snapshot **of one range** at an index it had already
/// taken that range at: the re-take D-043's variant rewrites one directory for.
///
/// Keyed by `(server, range, index)`. `sim/tests/raft.rs`'s own `took_an_index_twice`
/// keys by `(server, index)`, which is the same key on a server of one group and is
/// **not** on a node: four ranges taking at one index would answer true on the correct
/// node (PROPOSED D-086).
fn took_an_index_twice_on_the_node(report: &raft::Report) -> bool {
    let takes = report.snapshot_takes();
    takes.iter().enumerate().any(|(n, take)| {
        takes[..n].iter().any(|earlier| {
            earlier.server == take.server
                && earlier.range == take.range
                && earlier.index == take.index
        })
    })
}

/// Whether some server took a snapshot of one range at an index it had already taken
/// that range at **into the directory it had already written**: `SharedSnapshotDir`'s
/// symptom exactly, and the thing the correct node's take counter exists to prevent.
///
/// The index alone is *not* the property, and finding that out is what the campaign
/// behind PROPOSED D-086 was for. **The correct node** re-takes at an index it has
/// already taken at on **44 of 100 seeds** — its own figure, not one group's — and does
/// so legitimately: after a live install the
/// core's `taken` names a snapshot this replica never took (D-078), the record behind
/// it names an older one, the stream asks for a take, and the applied index has not
/// moved — so the take is at the same index, into `snap-r<range>-<index>-<take + 1>`,
/// a directory of its own that no stream can be reading. Asserting the index alone
/// would have been a bound the correct system trips, which is a model error and not a
/// bound to keep (D-030, D-039).
// PROPOSED(D-086): the take counter's property is about the directory, not the index.
fn retook_into_one_directory(report: &raft::Report) -> bool {
    let takes = report.snapshot_takes();
    takes.iter().enumerate().any(|(n, take)| {
        takes[..n].iter().any(|earlier| {
            earlier.server == take.server
                && earlier.range == take.range
                && earlier.index == take.index
                && earlier.dir == take.dir
        })
    })
}

/// `IgnoreIncarnation` on the node (D-042), and **why it is still blocked** — not by
/// the snapshot wiring, which this slice puts under these arms, but by Q15's whole-node
/// refusal and re-seed, which is PR #86's.
///
/// Phase 2 asserts two things
/// (`a_leader_that_ignores_incarnations_never_forgets`, sim/tests/raft.rs):
///
/// - **the injection**, at every tier: the leader as built never forgets a follower's
///   progress, so it traces no `RaftProgressReset` on any seed, "while the correct
///   server's own sweep above requires one wherever it saw a refusal";
/// - **the reach**, from a hundred seeds: a refused follower re-seeded and applying
///   again, the state the wedge is built on.
///
/// Neither is assertable here, and the reason is one fact with a citation. A leader
/// resets a follower's progress when the store incarnation that follower answers with
/// **changes** (`Raft::note_incarnation`, core.rs:1834-1862), and a store's incarnation
/// is "1 for a store started fresh, a fresh value on every store a re-seed rebuilt"
/// (store.rs:28). The node's live install deliberately **keeps** the incarnation — "an
/// install into a live store keeps its incarnation: the kept tail is everything
/// acknowledged past the snapshot, so nothing a leader matched is lost"
/// (server.rs, `ServerHost::repair`, D-042) — so on this node no replica's incarnation
/// ever changes, the correct leader never resets a progress either, and asserting that
/// the variant's leader traces no reset would pass on a node where nothing was
/// injected. That is the one failure mode a sweep cannot report on its own.
///
/// What this test does instead is assert the **absence with its reason and its
/// non-vacuity** (CLAUDE.md): the correct node traces no `RaftProgressReset` and no
/// refusal, over a run that is asserted to reach the install path. The day Q15's
/// re-seed lands, a reset appears here and this test fails, which is when the variant
/// can be re-asserted rather than a day later.
// PROPOSED(D-086): `IgnoreIncarnation` is blocked on Q15's re-seed, not on the wiring.
#[test]
fn a_leader_that_ignores_incarnations_has_no_incarnation_to_ignore_on_the_node_yet() {
    // The share, as the high-rate variants use (D-055, D-061): this runs the correct
    // node *and* the variant on every seed it takes, so the full tier would cost twice
    // the correct sweep beside it, and what it asserts is an absence of a mechanism
    // that is absent by construction — a store incarnation that never changes — rather
    // than a rate that more seeds would sharpen.
    let seeds = high_rate_share();
    let outcomes: Vec<(usize, usize, usize, usize)> = sweep(seeds, |seed| {
        let correct = correct(seed);
        let buggy = buggy(seed, Variant::IgnoreIncarnation);
        let resets = |report: &raft::Report| {
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftProgressReset { .. }))
        };
        (
            resets(&correct),
            resets(&buggy),
            correct.refused.len(),
            correct.snapshot_actions(),
        )
    });
    let correct_resets: usize = outcomes.iter().map(|(c, _, _, _)| c).sum();
    let buggy_resets: usize = outcomes.iter().map(|(_, b, _, _)| b).sum();
    let refusals: usize = outcomes.iter().map(|(_, _, r, _)| r).sum();
    let actions: usize = outcomes.iter().map(|(_, _, _, a)| a).sum();
    println!(
        "node: IgnoreIncarnation over {seeds} seeds — the correct node reset a follower's \
         progress {correct_resets} times and the variant {buggy_resets}, over {refusals} store \
         refusals and {actions} snapshot actions. A leader resets on a *change* of store \
         incarnation, and only a re-seed rebuilds a store with a fresh one (Q15, PR #86); a \
         live install keeps it (D-042). So neither leader has anything to forget yet"
    );
    // Non-vacuity: the install path is reached on these seeds, so this is an absence of
    // incarnation changes and not an absence of runs.
    assert!(
        actions > 0,
        "no core asked for a snapshot action over {seeds} seeds, so this says nothing"
    );
    // The absence, with its reason. Both halves are asserted so a move either way is
    // seen: the day a store's incarnation changes on this node, the correct leader
    // resets and this fails, and the variant can be re-asserted at its Phase 2 tier.
    assert_eq!(
        correct_resets, 0,
        "the correct node reset a follower's progress, so a store incarnation changed on it: \
         Q15's re-seed has landed and `IgnoreIncarnation` can be re-asserted here now — \
         re-audit this test rather than this absence"
    );
    assert_eq!(
        buggy_resets, 0,
        "the leader that ignores incarnations reset a follower's progress, which is the fix \
         the variant turns off"
    );
    assert_eq!(
        refusals, 0,
        "a store was refused on the node, which is Q15's path (PR #86): re-audit this test"
    );
}

/// The pair `{IgnoreIncarnation, SharedSnapshotDir}` on the node, and what became of
/// **seed 680**, which D-045 pinned it on (RAFT.md:658-661; sim/tests/raft.rs:405).
///
/// SHARD.md's Stage B says the node's schedule move retires the pin, that seed 680's
/// test asserts the wedge where the moved schedule still reaches it or, with the reason,
/// the situation's absence, and that the first thousand seeds are searched again for a
/// seed the pair is caught on.
///
/// **Seed 680's own pin did not move, and the evidence is not an argument.** This slice
/// changes nothing about `Cluster::OneGroup`: seed 42's moirae JSONL is 12 898 025
/// bytes and hashes to
/// `445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on this branch,
/// which is the hash D-082 recorded for `origin/main` and for its own branch. So
/// `seed_680_which_pinned_the_combined_variant_before_d056_no_longer_wedges` runs the
/// run it was pinned on and keeps saying exactly what it says, and this slice does not
/// touch it.
///
/// **On the node the pair is the stream half alone**, because the incarnation half has
/// nothing to ignore until Q15's re-seed lands (see the test above). So the search
/// SHARD.md asks for cannot be run against the node yet: a seed the pair is "caught on"
/// here would be a seed `SharedSnapshotDir` alone is caught on, which is not a wedge
/// that needs both bugs and so is not what D-045 pinned. That goes to the owner as the
/// entry records, and this test pins what the node *does* do, so that the day the other
/// half arrives the difference is visible.
// PROPOSED(D-086): the pair on the node is the stream half alone until Q15's re-seed.
#[test]
fn the_pair_on_the_node_is_the_stream_half_alone_until_the_reseed_lands() {
    // The share (D-055, D-061): this runs three variants on every seed it takes, and
    // what it asserts is the *equality* of two of them — a structural claim about one
    // half being a no-op, which a share settles as well as a tier and at a tenth of
    // the cost.
    let seeds = high_rate_share();
    let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
    let outcomes: Vec<(u64, bool, bool, bool)> = sweep(seeds, |seed| {
        (
            seed,
            checked(&buggy(seed, both)).is_err(),
            checked(&buggy(seed, Variant::SharedSnapshotDir)).is_err(),
            checked(&buggy(seed, Variant::IgnoreIncarnation)).is_err(),
        )
    });
    // **Per seed, not per count.** Counts were what this compared until PROPOSED
    // D-086's review, and a count equality cannot see the pin disappear: a genuine
    // D-045 wedge on one seed and a stream-only catch on another leave `pair` and
    // `stream` equal, and the wedge — the whole reason the pair exists — goes
    // unreported. The sets are compared, and the seeds where the pair is caught and
    // neither half alone is are asserted empty, which is the wedge itself.
    let seeds_where = |f: fn(&(u64, bool, bool, bool)) -> bool| -> BTreeSet<u64> {
        outcomes.iter().filter(|o| f(o)).map(|o| o.0).collect()
    };
    let pair = seeds_where(|o| o.1);
    let stream = seeds_where(|o| o.2);
    let incarnation = seeds_where(|o| o.3);
    let only_the_pair = seeds_where(|o| o.1 && !o.2 && !o.3);
    println!(
        "node: the pair caught on {}/{seeds} seeds {pair:?}, `SharedSnapshotDir` alone on {} \
         {stream:?}, `IgnoreIncarnation` alone on {} {incarnation:?}, and on {} seeds \
         {only_the_pair:?} the pair is caught where neither half alone is — which is the \
         wedge D-045 pinned",
        pair.len(),
        stream.len(),
        incarnation.len(),
        only_the_pair.len()
    );
    // The wedge itself, asserted rather than printed: a seed the pair is caught on and
    // neither half alone is, is a wedge that needs both bugs, which is what D-045
    // pinned seed 680 for and what this node cannot produce while `IgnoreIncarnation`
    // is a no-op on it. The day one appears, pin it here as D-045 pinned 680.
    assert!(
        only_the_pair.is_empty(),
        "the pair is caught on seeds {only_the_pair:?} where neither half alone is: that is a \
         wedge needing both bugs, which is what D-045 pinned seed 680 for — pin it here \
         rather than this absence, and take it to the owner"
    );
    // And the equality that says why: `IgnoreIncarnation` is a no-op on this node, so
    // the pair is exactly the stream half, seed for seed.
    assert_eq!(
        pair, stream,
        "the pair no longer catches exactly the seeds `SharedSnapshotDir` alone catches on \
         the node, so `IgnoreIncarnation` is doing something here now: search the first \
         thousand seeds for a wedge that needs both bugs and pin it, as D-045 pinned 680"
    );
    // `IgnoreIncarnation` alone is caught on 0 of 10 000 on one group too
    // (RAFT.md:697, SHARD.md:1579), so this is **not** the guard that sees Q15's
    // re-seed land — the two assertions above are. It is kept because a catch here
    // would mean something new either way.
    assert!(
        incarnation.is_empty(),
        "`IgnoreIncarnation` alone is caught on the node on seeds {incarnation:?}, which it \
         cannot be while a store's incarnation never changes: re-audit this test"
    );
}

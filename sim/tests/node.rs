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

/// The run's verdict: every check the one-group sweep makes, and then the two paths
/// this node has not got, asserted **absent on every seed with their reason**.
///
/// The absences are the point, not a formality. A variant whose situation the run
/// cannot reach is a test that passes because nothing was injected, which is the one
/// failure mode a sweep cannot report on its own (CLAUDE.md). This node takes no
/// snapshot and cannot be refused, so `snapshot_threshold` is held far above what a
/// run writes and the disk does not rot; if either changed and the sweep said
/// nothing, the install-path variants would look re-asserted on the node when
/// nothing had been injected at all. So the seed that first reaches one of these
/// fails here, naming the path and the slice that owns it.
// PROPOSED(D-082): what the node cluster does not reach yet, asserted absent.
fn checked(report: &raft::Report) -> Result<(), String> {
    let seed = report.seed;
    report.check()?;
    let actions = report.snapshot_actions();
    if actions > 0 {
        return Err(format!(
            "seed {seed}: {actions} snapshot actions were traced, a path this node does not \
             have: `ananke_shard::snapshot` is not wired to \
             `ananke_shard::server::ServerHost`. The install-path variants are not \
             re-asserted here, and a run that reaches this must say so rather than pass"
        ));
    }
    // The absence proper. A `RaftSnapshot` record is not evidence on its own: the
    // node's `Host::snapshot` bumps `Gaps::snapshot_actions` and traces nothing, so a
    // take the node dropped on the floor would leave the line above green against a
    // silence. What is asserted here is the condition behind every action a core can
    // ask for — a log longer than `snapshot_threshold` — which the trace does carry.
    let highest = report.highest_index();
    if highest >= raft::NODE_SNAPSHOT_THRESHOLD {
        return Err(format!(
            "seed {seed}: a replica reached index {highest}, at or past the {} this cluster \
             sets `snapshot_threshold` to, so a core could ask for a take, a record or an \
             install and the node would drop it in silence: the snapshot path is not wired \
             here",
            raft::NODE_SNAPSHOT_THRESHOLD
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
    for (_, (by_leader, by_apply, dropped, lags, held, rates, lease, arms)) in &results {
        let (rate, busiest, several, len) = rates;
        let (revokes, leased, round_trip, drifted, actions) = lease;
        arms_hit += arms.0;
        arms_fired += arms.1;
        revoked += usize::from(*revokes > 0);
        lease_reads += leased;
        read_index_reads += round_trip;
        exceeded += usize::from(*drifted);
        snapshot_actions += actions;
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

/// How many seeds the probe below runs, whatever the tier. There is no rate here to
/// price (D-061): whether the node has a re-seed path or a snapshot wiring is a
/// property of the tree and not of a seed, and what a seed varies is only the
/// schedule that carries the run to the refusal. Twenty is enough to show the
/// refusal lands on every one of them, which is the clause that makes the absences
/// evidence, and a directed check whose cost does not grow with the tier is what
/// D-077's own scenario is for the same reason.
// PROPOSED(D-085): a directed absence, not a sweep.
const NODE_PATH_SEEDS: u64 = 20;

/// The two paths `sim/quorum.rs` is made of, asserted **absent on the node on every
/// seed, each with the slice that owns it** (CLAUDE.md:58-67; PROPOSED D-085).
///
/// Stage B's first exit criterion asks `sim/quorum.rs` to run on the node beside
/// `sim/raft.rs`'s arms and `sim/membership.rs` (SHARD.md:2280-2290), and D-082
/// recorded why that one cannot follow the other two yet: it "is a re-seed scenario
/// from end to end", so it stacks on PR #86 and on the node's snapshot wiring,
/// neither of which is in this tree. This is that claim as a run rather than as a
/// paragraph, and as a tripwire rather than a note: it drives the refusal at the
/// node exactly as the scenario drives it at a server — crash, restart on a store
/// marked lost — and asserts that the refused node answers nothing, is never
/// re-seeded, and has no stream opened toward it.
///
/// The first clause is the one to read twice. It asserts the run **did** refuse the
/// node, because an absence read off a run that never refused anything is no
/// evidence at all — the same trap D-082's `checked` avoids by asserting the
/// condition behind a snapshot action rather than the silence of one.
///
/// The day either path lands this test fails, and its message says which half of
/// the sharded scenario can then be built. That is the intended failure: a
/// re-assertion has to be run and not argued (D-082), and a sharded `sim/quorum.rs`
/// written against a node that answers nothing would assert nothing.
// PROPOSED(D-085): the sharded scenario's two dependencies, asserted absent per seed.
#[test]
fn the_quorum_scenarios_two_paths_are_absent_on_the_node_and_this_says_when_they_arrive() {
    let paths: Vec<ananke_sim::quorum::NodePaths> =
        sweep(NODE_PATH_SEEDS, ananke_sim::quorum::node_paths);
    for path in &paths {
        path.the_two_paths_are_absent()
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let refused: usize = paths.iter().map(|p| p.refused).sum();
    println!(
        "node: over {} seeds, {refused} whole-node refusals, and of the paths a sharded \
         sim/quorum.rs needs: {} answers from a refused node (PR #86), {} re-seeds (PR #86), \
         {} streams opened toward it and {} installs (the snapshot wiring, D-082)",
        paths.len(),
        paths.iter().map(|p| p.answers).sum::<usize>(),
        paths.iter().map(|p| p.reseeded).sum::<usize>(),
        paths.iter().map(|p| p.streams).sum::<usize>(),
        paths.iter().map(|p| p.installs).sum::<usize>(),
    );
}

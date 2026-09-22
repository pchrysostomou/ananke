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
//!
//! # The membership scenario on the node
//!
//! The second half of this binary is `sim/membership.rs` on the same node (PROPOSED
//! D-084, following D-082): five nodes, **four ranges on every one of them**, 3 → 5 → 3
//! on every range under the partitions the seed draws, each range placed as today's one
//! group is. It is here rather than in a binary of its own for the reason the one-group
//! membership tests sit in `sim/tests/raft.rs`: the scenario shares the sweep's client,
//! its addresses and its schedule's clocks, and a second binary would be a second build
//! of all of it.
//!
//! What it adds to the list above:
//!
//! - the other half of issue #46's extension, the half four ranges make possible and
//!   one group could not have: **a change of a range while another range on the same
//!   node is changing**, asserted on every seed and named on one;
//! - liveness and availability asked of **each range** and not of the cluster, on
//!   bounds measured on the correct node before they were written;
//! - `SingleMajorityInJointConsensus` re-asserted on the node to the standard its
//!   Phase 2 test asserts and no stronger, at the tier it uses today;
//! - the half of issue #46 the node **cannot** reach — a joining server fed by a
//!   snapshot in its learner phase — asserted absent on every seed with its reason and
//!   the slice that owns the wiring, and left asserted where the path is, on
//!   `Cluster::OneGroup`.

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

// --- The membership scenario on the node (SHARD.md §12's Stage B, issue #46) ---

use ananke_sim::membership;

/// The correct node's membership run for `seed`: five nodes, four ranges on each,
/// 3 → 5 → 3 on **every** range under the partitions the seed draws.
fn membership_node(seed: u64, variants: impl Into<Variants>) -> membership::Report {
    membership::run_on(Cluster::Node, seed, variants)
}

/// The run's verdict on the node: the scenario's own checks — which are now each
/// range's, not the cluster's — and then the shape four ranges are *for*, asserted on
/// every seed rather than printed.
///
/// The two clauses here are the ones a single-range world could not have made, and
/// each is stated per seed because a floor over a tier cannot see a seed that reached
/// nothing (CLAUDE.md): a run where the four changes never overlapped, or where the
/// joiners were admitted to one range and not four, has not run the scenario this
/// binary claims to run, and must say so rather than pass.
// PROPOSED(D-084): the membership scenario on the node, four ranges on every node.
fn membership_checked(report: &membership::Report) -> Result<(), String> {
    let seed = report.seed;
    report.check()?;
    // Issue #46's extension the node makes possible: a change of a range while
    // another range **on the same node** is changing. One group has one range and no
    // overlap to have, which is why this could not be asserted before.
    //
    // Asked as `witnessed_joint_overlap` and not as `joint_overlap`, because the
    // forward fold that produces the answer can be widened into always answering —
    // drop the line that clears a range when its joint configuration ends — and no
    // floor, count or bound could see it, since a fold that always answers passes
    // everything. The witness reads the same trace backwards at the record the fold
    // stopped on and asks whether both ranges really were joint there. It cannot be
    // satisfied by construction and it is not a bound, so the correct node cannot trip
    // it: 100 of 100 witnessed, against 62 of 100 for the never-clearing fold.
    // PROPOSED(D-084): the answer is witnessed, which is what guards the fold.
    if let Err(why) = report.witnessed_joint_overlap() {
        return Err(format!("seed {seed}: {why}"));
    }
    // The grow admits servers 4 and 5 to **every** range. A run that admitted them to
    // one range and left three unchanged completes "a change" and is not this
    // scenario; the whole-cluster reading cannot tell the two apart. Asked only where
    // the grow completed, which `Report::check` asserts of every uniformly scheduled
    // seed and liveness cannot be asked of the others (D-016).
    if report.grow_completed {
        let held: BTreeSet<u64> = report.ranges().into_iter().collect();
        for joiner in membership::INITIAL_VOTERS + 1..=membership::SERVERS {
            let admitted = report.ranges_admitting(joiner);
            if admitted != held {
                return Err(format!(
                    "seed {seed}: the grow completed, but server {joiner} became a voter of \
                     {admitted:?} and not of every range this node holds, {held:?}"
                ));
            }
        }
    }
    Ok(())
}

/// What the correct node's membership runs saw, over a tier.
#[derive(Debug, Default)]
struct NodeMembershipCoverage {
    seeds: u64,
    uniform_seeds: u64,
    grows_completed: u64,
    shrinks_completed: u64,
    /// Seeds on which a node held two ranges' joint configurations at once, **and the
    /// trace read backwards witnessed it** ([`membership::Report::witnessed_joint_overlap`]).
    overlapped: u64,
    /// Seeds on which both joiners became voters of every range.
    joiners_on_every_range: u64,
    /// Partitions that cut off the leader of the range they drew, and partitions made.
    partitions_hit: usize,
    partitions_made: usize,
    /// Snapshot actions traced, and the highest index any replica reached: the two
    /// halves of the absence this cluster asserts (`Report::check`).
    snapshot_actions: usize,
    highest_index: u64,
    /// Ranges that elected a leader and ranges that applied something, over the tier.
    leaders: BTreeMap<u64, usize>,
    applies: BTreeMap<u64, usize>,
    /// The worst gap any one range went without a completed operation, and the worst
    /// first write after a heal, against their bounds.
    worst_range_gap: Duration,
    worst_write_after_heal: Duration,
    /// The same two folded over the **cluster** instead, which is what the one-group
    /// scenario has always asked. They are here to be compared with the two above:
    /// a per-range fold that saw no more than the cluster's fold is not a per-range
    /// fold, and the comparison is the only thing that says so (see
    /// `assert_complete`).
    worst_cluster_gap: Duration,
    worst_cluster_write_after_heal: Duration,
    /// The ranges the run's partitions aimed at, over the tier.
    aimed_ranges: BTreeSet<u64>,
    /// The ranges a leadership transfer was asked for, over the tier, and how many of
    /// those transfers were followed by the asked-for server leading that range.
    // PROPOSED(D-084): the transfer hands over every range the node holds.
    transfer_ranges: BTreeSet<u64>,
    transfers_landed: usize,
    transfers_made: usize,
    /// The states the one-group scenario's own coverage asserts, folded here over the
    /// node's trace so that this positive control demands what that one does. Each is
    /// reachable on the node and none needs a snapshot path; the ones the node cannot
    /// reach are named in `assert_complete` with the reason, not left out.
    // PROPOSED(D-084): the node's positive control asks what the one-group one asks.
    joint_configs_taken: usize,
    new_configs_taken: usize,
    learners_promoted: usize,
    learner_rounds_caught_up: usize,
    config_reverts: usize,
    elections_while_joint: usize,
    step_downs_outside_new: usize,
    changes_accepted: usize,
    match_starts: usize,
    learner_rounds: usize,
    seeds_with_a_match_start: u64,
    seeds_with_a_learner_round: u64,
    seeds_with_a_change_accepted: u64,
    partitions: usize,
    completed: u64,
    /// Trace records, and the longest run in virtual seconds, for the density figure.
    records: usize,
    longest_run: f64,
}

impl NodeMembershipCoverage {
    fn add(&mut self, report: &membership::Report) {
        self.seeds += 1;
        self.uniform_seeds += u64::from(report.uniform());
        self.grows_completed += u64::from(report.grow_completed);
        self.shrinks_completed += u64::from(report.shrink_completed);
        self.overlapped += u64::from(report.witnessed_joint_overlap().is_ok());
        let held: BTreeSet<u64> = report.ranges().into_iter().collect();
        let every = (membership::INITIAL_VOTERS + 1..=membership::SERVERS)
            .all(|joiner| report.ranges_admitting(joiner) == held);
        self.joiners_on_every_range += u64::from(every);
        let (hit, made) = report.partitions_hit_their_ranges();
        self.partitions_hit += hit;
        self.partitions_made += made;
        self.snapshot_actions += report.snapshot_actions();
        self.highest_index = self.highest_index.max(report.highest_index());
        for range in report.ranges() {
            if let Some(gap) = report.longest_completion_gap_of(range) {
                self.worst_range_gap = self.worst_range_gap.max(gap);
            }
            if let Some(took) = report.time_to_write_after_heal_of(range) {
                self.worst_write_after_heal = self.worst_write_after_heal.max(took);
            }
        }
        if let Some(gap) = report.longest_completion_gap() {
            self.worst_cluster_gap = self.worst_cluster_gap.max(gap);
        }
        if let Some(took) = report.time_to_write_after_heal() {
            self.worst_cluster_write_after_heal = self.worst_cluster_write_after_heal.max(took);
        }
        self.aimed_ranges
            .extend(report.aimed.iter().map(|aimed| aimed.range));
        self.transfer_ranges.extend(report.transfer_ranges());
        let (landed, made) = report.transfers_landed();
        self.transfers_landed += landed;
        self.transfers_made += made;
        self.partitions += report.partitions.len();
        self.completed += report.clients.completed;
        self.fold_the_one_group_states(report);
        for record in &report.records {
            match &record.event {
                ananke_env::TraceEvent::RaftLeader { range, .. } => {
                    *self.leaders.entry(*range).or_default() += 1;
                }
                ananke_env::TraceEvent::RaftApply { range, .. } => {
                    *self.applies.entry(*range).or_default() += 1;
                }
                _ => {}
            }
        }
        self.records += report.records.len();
        let end = report.records.last().map_or(0, |r| r.at.as_nanos());
        self.longest_run = self.longest_run.max(end as f64 / 1e9);
    }

    fn merge(&mut self, other: Self) {
        self.seeds += other.seeds;
        self.uniform_seeds += other.uniform_seeds;
        self.grows_completed += other.grows_completed;
        self.shrinks_completed += other.shrinks_completed;
        self.overlapped += other.overlapped;
        self.joiners_on_every_range += other.joiners_on_every_range;
        self.partitions_hit += other.partitions_hit;
        self.partitions_made += other.partitions_made;
        self.snapshot_actions += other.snapshot_actions;
        self.highest_index = self.highest_index.max(other.highest_index);
        self.worst_range_gap = self.worst_range_gap.max(other.worst_range_gap);
        self.worst_write_after_heal = self
            .worst_write_after_heal
            .max(other.worst_write_after_heal);
        self.worst_cluster_gap = self.worst_cluster_gap.max(other.worst_cluster_gap);
        self.worst_cluster_write_after_heal = self
            .worst_cluster_write_after_heal
            .max(other.worst_cluster_write_after_heal);
        for (range, count) in other.leaders {
            *self.leaders.entry(range).or_default() += count;
        }
        for (range, count) in other.applies {
            *self.applies.entry(range).or_default() += count;
        }
        self.records += other.records;
        self.longest_run = self.longest_run.max(other.longest_run);
        self.aimed_ranges.extend(other.aimed_ranges);
        self.transfer_ranges.extend(other.transfer_ranges);
        self.transfers_landed += other.transfers_landed;
        self.transfers_made += other.transfers_made;
        self.joint_configs_taken += other.joint_configs_taken;
        self.new_configs_taken += other.new_configs_taken;
        self.learners_promoted += other.learners_promoted;
        self.learner_rounds_caught_up += other.learner_rounds_caught_up;
        self.config_reverts += other.config_reverts;
        self.elections_while_joint += other.elections_while_joint;
        self.step_downs_outside_new += other.step_downs_outside_new;
        self.changes_accepted += other.changes_accepted;
        self.match_starts += other.match_starts;
        self.learner_rounds += other.learner_rounds;
        self.seeds_with_a_match_start += other.seeds_with_a_match_start;
        self.seeds_with_a_learner_round += other.seeds_with_a_learner_round;
        self.seeds_with_a_change_accepted += other.seeds_with_a_change_accepted;
        self.partitions += other.partitions;
        self.completed += other.completed;
    }

    /// The states `MembershipCoverage` counts on the one-group server, folded over the
    /// node's trace with the same rules — a configuration's own `RaftConfig` per
    /// (server, range), a promotion per (index, joiner), a step-down of a leader the
    /// new configuration leaves out, a revert to a lower index, an election while
    /// joint — so that what this control demands and what that one demands can be
    /// compared line for line (PROPOSED D-084).
    // PROPOSED(D-084): the node's positive control asks what the one-group one asks.
    fn fold_the_one_group_states(&mut self, report: &membership::Report) {
        let match_starts =
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftMatchStarted { .. }));
        let learner_rounds =
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftLearnerRound { .. }));
        let changes =
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftChangeAccepted { .. }));
        self.match_starts += match_starts;
        self.learner_rounds += learner_rounds;
        self.changes_accepted += changes;
        self.seeds_with_a_match_start += u64::from(match_starts > 0);
        self.seeds_with_a_learner_round += u64::from(learner_rounds > 0);
        self.seeds_with_a_change_accepted += u64::from(changes > 0);
        self.learner_rounds_caught_up += report.count(|e| {
            matches!(
                e,
                ananke_env::TraceEvent::RaftLearnerRound {
                    caught_up: true,
                    ..
                }
            )
        });
        // Keyed by (server, range) and not by server: a node holds four replicas and
        // one entry would read one range's configuration as another's, which is the
        // mistake this whole slice exists to make visible.
        let mut in_force: BTreeMap<(u64, u64), (u64, bool)> = BTreeMap::new();
        let mut leading: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut promoted: BTreeSet<(u64, u64, u64)> = BTreeSet::new();
        for record in &report.records {
            match &record.event {
                ananke_env::TraceEvent::RaftConfig {
                    server,
                    range,
                    index,
                    old,
                    new,
                    joint,
                    ..
                } => {
                    if *joint {
                        self.joint_configs_taken += 1;
                        for id in new {
                            if !old.contains(id) {
                                promoted.insert((*range, *index, *id));
                            }
                        }
                    } else if *index > 0 {
                        self.new_configs_taken += 1;
                        if leading.contains(&(*range, *server)) && !old.contains(server) {
                            self.step_downs_outside_new += 1;
                        }
                    }
                    if let Some(&(previous, _)) = in_force.get(&(*range, *server))
                        && *index < previous
                    {
                        self.config_reverts += 1;
                    }
                    in_force.insert((*range, *server), (*index, *joint));
                }
                ananke_env::TraceEvent::RaftTerm {
                    server,
                    range,
                    role,
                    ..
                } => {
                    if &**role == "leader" {
                        leading.insert((*range, *server));
                    } else {
                        leading.remove(&(*range, *server));
                    }
                }
                ananke_env::TraceEvent::RaftLeader { server, range, .. } => {
                    leading.insert((*range, *server));
                    if in_force
                        .get(&(*range, *server))
                        .is_some_and(|&(_, joint)| joint)
                    {
                        self.elections_while_joint += 1;
                    }
                }
                _ => {}
            }
        }
        self.learners_promoted += promoted.len();
    }

    /// The floors, each measured on the correct node before it was written (D-061).
    ///
    /// The per-range counts are the ones that mean anything here: a scenario that
    /// elected and applied on one range while three sat idle would satisfy every
    /// whole-cluster count there is, which is the mistake `applies.contains_key` made
    /// in D-082's own campaign.
    fn assert_complete(&self) {
        assert_eq!(self.seeds, seeds(), "every seed of the tier is counted");
        assert_eq!(
            self.overlapped, self.seeds,
            "some seed's four changes never overlapped on one node: {self:?}"
        );
        assert_eq!(
            self.joiners_on_every_range, self.seeds,
            "some seed admitted the joiners to fewer than every range: {self:?}"
        );
        assert_eq!(
            self.grows_completed, self.seeds,
            "some seed's grow did not complete on every range: {self:?}"
        );
        assert_eq!(
            self.shrinks_completed, self.seeds,
            "some seed's shrink did not complete on every range: {self:?}"
        );
        // The absence this cluster asserts, over the tier as well as per seed.
        assert_eq!(
            self.snapshot_actions, 0,
            "the node traced a snapshot action, a path it does not have: {self:?}"
        );
        assert!(
            self.highest_index < raft::NODE_SNAPSHOT_THRESHOLD,
            "a replica reached the snapshot threshold: {self:?}"
        );
        for range in Cluster::Node.ranges() {
            assert!(
                self.leaders.get(&range).copied().unwrap_or(0) > 0,
                "range {range} never elected a leader over the tier: {self:?}"
            );
            assert!(
                self.applies.get(&range).copied().unwrap_or(0) > 0,
                "range {range} applied nothing over the tier: {self:?}"
            );
        }
        // Every partition aimed at a range cut off *that range's* leader. Measured at
        // 200/200 over a hundred seeds on the correct node; the floor is 90 %, as
        // D-082 set the raft sweep's, because a leadership change between the
        // driver's read and the partition is ordinary and is not a fault.
        assert!(
            self.partitions_made > 0,
            "no partition was made at all: {self:?}"
        );
        let hit = self.partitions_hit as f64 / self.partitions_made as f64;
        assert!(
            hit >= 0.9,
            "only {hit:.3} of the partitions cut off the leader of the range they drew, under \
             the floor of 0.900: {self:?}"
        );
        // The applies are spread over the ranges rather than piled on one. The floor
        // is the least range's share of the busiest, measured **on this scenario** at
        // **0.967** at a hundred seeds ({2: 11 394, 3: 11 436, 4: 11 324, 5: 11 054})
        // and 0.912 at the gate's twenty; D-082 set the same floor at 0.5 against its
        // own sweep's 0.87, for the same reason, that `contains_key` passes a range
        // which applied only its leader's no-ops.
        // Every range was aimed at by some partition over the tier. Without this a
        // schedule whose draw answered the first range every time would leave three
        // ranges' leaders never cut off, and every other floor here would be met:
        // `partitions_hit` counts hits against what was *aimed at*, so a draw that
        // always aims at one range hits it every time.
        assert_eq!(
            self.aimed_ranges,
            Cluster::Node.ranges().into_iter().collect::<BTreeSet<_>>(),
            "the partitions aimed at {:?} and not at every range this node holds: {self:?}",
            self.aimed_ranges
        );
        // A per-range fold that saw no more than the cluster's fold is not a per-range
        // fold. Both quantities below are per-range maxima over folds whose inputs are
        // *subsets* of the cluster fold's, so each is at least the cluster's by
        // construction and the only question is whether it is ever strictly more. Over
        // a tier it is, comfortably — at a hundred seeds the worst range went 996 ms
        // without a completed operation against the cluster's 329 ms, and a range's
        // first write after a heal took 1.130 s against the cluster's first — and this
        // is what fails if either fold is quietly widened back to the whole history,
        // which no bound below could see, since a widened fold only ever passes.
        assert!(
            self.worst_range_gap > self.worst_cluster_gap,
            "the worst gap of any one range, {:?}, is no worse than the cluster's {:?}: the \
             per-range availability fold is reading the whole history: {self:?}",
            self.worst_range_gap,
            self.worst_cluster_gap
        );
        assert!(
            self.worst_write_after_heal > self.worst_cluster_write_after_heal,
            "the worst first write after a heal of any one range, {:?}, is no worse than the \
             cluster's {:?}: the per-range liveness fold is reading the whole history: \
             {self:?}",
            self.worst_write_after_heal,
            self.worst_cluster_write_after_heal
        );
        // Every range a leadership transfer was asked for. D-084's item 6 says the
        // transfer hands over **every** range the node holds, so that the shrink's
        // leader is outside `C_new` on each of them; nothing else records that claim.
        // A transfer that reached one range of four leaves every other floor here met
        // — the elections it costs the other three are lost among the ones the
        // partitions cause anyway — so this is what says the claim was kept.
        // PROPOSED(D-084): the transfer hands over every range the node holds.
        assert!(
            self.transfers_made > 0,
            "no leadership transfer was asked for at all: {self:?}"
        );
        assert_eq!(
            self.transfer_ranges,
            Cluster::Node.ranges().into_iter().collect::<BTreeSet<_>>(),
            "leadership was handed over for {:?} and not for every range this node holds: \
             {self:?}",
            self.transfer_ranges
        );
        // And the transfers were followed by the server they named leading that range,
        // which is what sets the step-down up. Not at one — a transfer is one shot and
        // best effort, as the sweep's lease trial is — but at a floor measured on the
        // correct node: 140 of 192 (72.9 %) at a hundred seeds and 25 of 32 (78.1 %) at
        // the gate's twenty.
        let landed = self.transfers_landed as f64 / self.transfers_made as f64;
        assert!(
            landed >= 0.5,
            "only {landed:.3} of the leadership transfers were followed by the server they \
             named leading that range, under the floor of 0.500: {self:?}"
        );
        // What the one-group scenario's own coverage asserts of this scenario
        // (`MembershipCoverage::assert_complete`), asked here with the same tiering, so
        // that the node's positive control demands what the one-group one demands.
        // Every one of these is reachable on the node and none needs a snapshot path;
        // the ones the node cannot reach are named below with their reason rather than
        // left out.
        // PROPOSED(D-084): the node's positive control asks what the one-group one asks.
        for (what, seen) in [
            (
                "joint configurations taken",
                self.joint_configs_taken as u64,
            ),
            ("new configurations taken", self.new_configs_taken as u64),
            ("learners promoted", self.learners_promoted as u64),
            (
                "learner rounds that caught up",
                self.learner_rounds_caught_up as u64,
            ),
            ("partitions", self.partitions as u64),
            ("completed operations", self.completed),
            ("uniformly scheduled seeds", self.uniform_seeds),
        ] {
            assert!(
                seen > 0,
                "the node's membership runs never saw {what}: {self:?}"
            );
        }
        // SHARD.md §8's three events, each on *every* seed, as the one-group control
        // asserts them: the driver grows the configuration of every range on every
        // seed, so a change is accepted, a learner is tracked and caught up, and every
        // leader's first answer from a follower raises `matched` under the incarnation
        // it carried. 100 of 100 and 20 of 20 for all three here. A total above zero is
        // what an emission rule gone wrong passes; these say the rule fires where it
        // must.
        for (what, seen) in [
            (
                "a leader's match rise under a follower's incarnation",
                self.seeds_with_a_match_start,
            ),
            (
                "a learner's catch-up round",
                self.seeds_with_a_learner_round,
            ),
            ("an accepted change", self.seeds_with_a_change_accepted),
        ] {
            assert_eq!(
                seen, self.seeds,
                "a node membership run saw no {what}: {self:?}"
            );
        }
        // The one-group control's tiering for the two states that need the partition to
        // land inside a narrow phase of a change. On the node they are commoner —
        // 219 step-downs and 33 reverts at a hundred seeds, 44 and 3 at the gate's
        // twenty — because four ranges give four changes a seed; the tier is kept the
        // one-group one rather than tightened, since nothing here measured the rate a
        // tighter tier would rest on.
        if self.seeds >= 100 {
            for (what, seen) in [
                (
                    "step-downs of a leader outside C_new",
                    self.step_downs_outside_new as u64,
                ),
                ("configuration reverts", self.config_reverts as u64),
            ] {
                assert!(
                    seen > 0,
                    "the node's membership runs never saw {what}: {self:?}"
                );
            }
        }
        // An election while joint needs the partition to cut a leader off inside the
        // joint phase itself, and the one-group control asserts it from the
        // thousand-seed tier under D-061 (3.4 % of its seeds). On the node it is 6
        // elections over a hundred seeds and none over the gate's twenty, so the same
        // tier is kept.
        if self.seeds >= 1000 {
            assert!(
                self.elections_while_joint > 0,
                "the node's membership runs never saw elections while joint: {self:?}"
            );
        }
        let least = self.applies.values().copied().min().unwrap_or(0) as f64;
        let busiest = self.applies.values().copied().max().unwrap_or(1).max(1) as f64;
        assert!(
            least / busiest >= 0.5,
            "the least busy range applied {:.2} of the busiest range's entries, under the floor \
             of 0.50: {self:?}",
            least / busiest
        );
    }
}

/// The positive control: the correct node passes 3 → 5 → 3 on every one of its four
/// ranges, on every seed, with the changes overlapping on a node and both joiners
/// admitted to every range.
///
/// Every figure SHARD.md §12 asks of a sweep on the node is printed beside the
/// verdict: the trace records a run holds per range per virtual second against
/// `TRACE_CAP`, the worst gap and first write after a heal any range showed against
/// their bounds, and the two halves of the snapshot path's absence.
// PROPOSED(D-084): the membership scenario on the node, four ranges on every node.
#[test]
fn every_seed_passes_the_membership_scenario_on_the_correct_node() {
    let coverage = std::sync::Mutex::new(NodeMembershipCoverage::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = membership_node(seed, Variants::default());
        let mut mine = NodeMembershipCoverage::default();
        mine.add(&report);
        coverage.lock().unwrap().merge(mine);
        membership_checked(&report)
            .inspect_err(|_| write_trace(&format!("node-membership-{seed}"), &report.jsonl()))
    });
    let coverage = coverage.into_inner().unwrap();
    let per_range_per_second =
        coverage.records as f64 / 4.0 / coverage.longest_run.max(1e-9) / coverage.seeds as f64;
    eprintln!(
        "node membership: {coverage:?}; about {per_range_per_second:.0} records per range per \
         virtual second against a TRACE_CAP of {}",
        membership::TRACE_CAP
    );
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete();
}

/// The negative control on the node: a server that counts one merged majority while
/// joint (thesis §4.3) is caught by the scenario's checks on some seed.
///
/// Its Phase 2 test asserts exactly this and no more — caught on some seed, at every
/// tier — and that is what is asserted here (§10, Q39). Its **measured** rate on the
/// node is 14 of 100 seeds against the one-group server's 19 of 100, above D-061's
/// five per cent, so the tier it keeps is the tier it has. At the gate's twenty seeds
/// the catch is deterministic and not a coin: seeds 3 and 15 of the first twenty catch
/// it, so a green gate here is a gate that injected the fault.
// PROPOSED(D-084): Phase 2's variant re-asserted on the node, at the tier it uses today.
#[test]
fn a_server_that_counts_one_majority_in_joint_consensus_is_caught_on_the_node() {
    let outcomes: Vec<(Option<String>, bool)> = sweep(seeds(), |seed| {
        let report = membership_node(seed, Variant::SingleMajorityInJointConsensus);
        (report.check().err(), report.joint_overlap().is_some())
    });
    let overlapped = outcomes.iter().filter(|(_, o)| *o).count();
    let caught: Vec<String> = outcomes.into_iter().filter_map(|(v, _)| v).collect();
    eprintln!(
        "SingleMajorityInJointConsensus on the node: caught on {} of {} seeds, the changes \
         overlapped on {overlapped}, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        !caught.is_empty(),
        "SingleMajorityInJointConsensus was never caught on the node"
    );
}

/// The node's membership scenario replays: two runs of one seed give byte-identical
/// traces, so the driver's decisions — which range it asks first, which range's leader
/// the partition cuts off — are functions of the trace and the seed alone.
// PROPOSED(D-084): the membership scenario on the node, four ranges on every node.
#[test]
fn a_membership_seed_replays_to_the_same_trace_on_the_node() {
    let first = membership_node(7, Variants::default());
    let second = membership_node(7, Variants::default());
    assert_eq!(first.jsonl().as_bytes(), second.jsonl().as_bytes());
}

/// One seed's run, read record by record: the two ranges a node changed at once, and
/// the node that held them.
///
/// The sweep asserts the overlap on every seed and this names it, so a reader can see
/// what issue #46's extension looks like on the node without running a tier.
// PROPOSED(D-084): #46's extension the node makes possible — one node, two ranges
// changing at once.
#[test]
fn a_node_changes_two_of_its_ranges_at_once() {
    let report = membership_node(1, Variants::default());
    membership_checked(&report).expect("the correct node passes membership seed 1");
    let (node, first, second, at) = report
        .joint_overlap()
        .expect("a node held two ranges' joint configurations at once");
    println!(
        "node membership: node {node} was jointly configured on ranges {first} and {second} at \
         {at:?}"
    );
    assert_ne!(first, second, "two ranges, not one");
    assert!(
        report.ranges().contains(&first) && report.ranges().contains(&second),
        "both are ranges this node holds"
    );
}

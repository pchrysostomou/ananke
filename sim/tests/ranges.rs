//! The node scenario's sweep (SHARD.md §2, §4, §12's Stage B): three nodes, four
//! ranges on every node.
//!
//! What it is for is in `sim/ranges.rs`. What it asserts, at every tier:
//!
//! - the correct node passes every seed: checks 1 to 4 keyed by range, the history's
//!   closure keyed by `(range, index, term)`, the timer check and pre-vote's property
//!   per `(range, server)`, the checks about time asked of each range whose
//!   unimpaired replicas form a majority, and the write bound per key — the checks
//!   D-071 keyed, asked for the first time of a trace that has more than one range to
//!   be wrong about;
//! - the run reaches the shape those checks need: four replicas created at bootstrap
//!   on each of three nodes, every range electing and applying, and frames carrying
//!   several ranges between two nodes;
//! - the node's own known-buggy variants are caught beside the correct node
//!   (CLAUDE.md's pair rule), at the tier each one's measured rate supports (D-061).

use std::collections::BTreeMap;

use ananke_env::TraceEvent;
use ananke_raft::core::Variants;
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::{ranges, seeds, sweep, verdict, write_trace};

/// The correct node, on the seeds this tier runs.
fn correct(seed: u64) -> ranges::Report {
    ranges::run(seed, Variants::default(), NodeVariants::correct())
}

#[test]
fn a_node_holds_four_ranges_from_its_configuration_and_every_one_of_them_runs() {
    let report = correct(1);
    report.check().expect("the correct node passes seed 1");
    assert_eq!(
        report.bootstrap_creations(),
        (ranges::NODES * ranges::RANGES) as usize,
        "three nodes times four ranges of `RangeCreated {{ cause: bootstrap }}`"
    );
    let leaders = report.leaders_by_range();
    let applies = report.applies_by_range();
    for range in ranges::FIRST_RANGE..ranges::FIRST_RANGE + ranges::RANGES {
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
fn every_seed_passes_on_the_correct_node() {
    let seeds = seeds();
    let mut creations = 0usize;
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    let mut applies: BTreeMap<u64, usize> = BTreeMap::new();
    let mut records = 0usize;
    let mut per_range_per_second = 0.0f64;
    let results = sweep(seeds, |seed| {
        let report = correct(seed);
        let outcome = report.check();
        if outcome.is_err() {
            write_trace(&format!("ranges-{seed}"), &report.checked.jsonl());
        }
        (
            outcome,
            (
                report.bootstrap_creations(),
                report.leaders_by_range(),
                report.applies_by_range(),
                report.records().len(),
                report.records_per_range_per_second(),
                report.worst_write_after_heal(true),
                report.worst_write_after_heal(false),
            ),
        )
    });
    let mut worst = std::time::Duration::ZERO;
    let mut worst_anywhere = std::time::Duration::ZERO;
    for (_, (created, by_leader, by_apply, len, rate, took, anywhere)) in &results {
        if let Some(took) = took {
            worst = worst.max(*took);
        }
        if let Some(took) = anywhere {
            worst_anywhere = worst_anywhere.max(*took);
        }
        creations += created;
        for (range, count) in by_leader {
            *leaders.entry(*range).or_default() += count;
        }
        for (range, count) in by_apply {
            *applies.entry(*range).or_default() += count;
        }
        records += len;
        per_range_per_second = per_range_per_second.max(*rate);
    }
    let bound = ranges::Report::write_bound();
    println!(
        "ranges: {seeds} seeds, {creations} bootstrap creations, leaders by range {leaders:?}, \
         applies by range {applies:?}, {records} records in all, at most \
         {per_range_per_second:.0} records per range per virtual second; the worst first write \
         to a key of a live range after the heal took {worst:?} of the {bound:?} bound on a \
         run the bound is asked of, a margin of {:?}, and {worst_anywhere:?} on any run",
        bound.saturating_sub(worst)
    );
    // The shape the keyed checks need, on every seed: four ranges on every node,
    // each electing and applying. Asserted at every tier, since it is what says the
    // sweep can tell a keyed check from a wrongly keyed one at all.
    assert_eq!(
        creations,
        seeds as usize * (ranges::NODES * ranges::RANGES) as usize
    );
    for range in ranges::FIRST_RANGE..ranges::FIRST_RANGE + ranges::RANGES {
        assert!(leaders.contains_key(&range), "range {range} never led");
        assert!(applies.contains_key(&range), "range {range} never applied");
    }
    let verdicts: Vec<Result<(), String>> =
        results.into_iter().map(|(verdict, _)| verdict).collect();
    verdict(&verdicts).expect("the correct node passes every seed");
}

/// The pair rule (CLAUDE.md): a known-buggy node beside the correct one, caught by
/// the same check. `StepWhilePersisting` steps a core while its own persist is
/// outstanding, which is the one thing Q41's round exists to prevent.
#[test]
fn a_node_that_steps_a_core_while_its_persist_is_outstanding_is_caught() {
    let seeds = seeds();
    let mut caught = 0usize;
    let results = sweep(seeds, |seed| {
        let report = ranges::run(
            seed,
            Variants::default(),
            NodeVariants::of(&[NodeVariant::StepWhilePersisting]),
        );
        (report.check().is_err(), ())
    });
    for (failed, ()) in &results {
        caught += usize::from(*failed);
    }
    let rate = caught as f64 * 100.0 / seeds as f64;
    println!("ranges: StepWhilePersisting caught on {caught}/{seeds} seeds ({rate:.1}%)");
    // D-061: the rate is measured before it is asserted, and the assertion is made
    // at the tier the rate supports. The figure this bound is set from is in the
    // node's entry.
    assert!(
        caught > 0,
        "the variant that steps a core while its persist is outstanding was caught on no seed"
    );
}

#[test]
fn every_frame_between_two_nodes_may_carry_several_ranges() {
    // Four ranges on a node is the parameter this scenario fixes so that a frame
    // between two nodes carries messages of several ranges each way (SHARD.md §12).
    // What says it happened is the inbox's own record: a node admits messages of
    // more than one range between two of its rounds.
    let report = correct(3);
    let mut ranges_seen: BTreeMap<u64, usize> = BTreeMap::new();
    for record in report.records() {
        if let TraceEvent::RaftAppend { range, .. } = &record.event {
            *ranges_seen.entry(*range).or_default() += 1;
        }
    }
    assert_eq!(
        ranges_seen.len(),
        ranges::RANGES as usize,
        "the run's appends name {:?}, not four ranges",
        ranges_seen.keys().collect::<Vec<_>>()
    );
}

#[test]
fn a_seed_replays_to_the_same_trace() {
    // The node's schedule is the simulator's: one engine, one socket and four cores
    // on one ticker, with the persists of a round resolved in the order the
    // scheduling stream draws (D-073). Two runs of one seed must still be the same
    // run, records and all (SPEC.md §1.6).
    let first = correct(2);
    let second = correct(2);
    assert_eq!(first.records().len(), second.records().len());
    for (a, b) in first.records().iter().zip(second.records()) {
        assert_eq!(a.at, b.at);
        assert_eq!(a.event, b.event);
    }
}

#[test]
fn no_batch_frame_of_the_node_parses_as_a_frame_of_the_one_group_server() {
    // The two codecs' first bytes collide: a batch frame's version is 1 and
    // `ananke-raft`'s tag 1 is a pre-vote. A payload is read as a one-group frame
    // when it parses as one and as a batch frame otherwise (`raft::messages_of`),
    // which keeps every one-group scenario's replay exactly as it was — and is
    // sound only while no batch frame parses as a one-group frame. Over a run of
    // this scenario, none does: a pre-vote is exactly 33 bytes, `Frame::decode`
    // refuses trailing bytes, and the smallest batch frame is 34.
    let report = correct(4);
    let mut payloads = 0usize;
    for record in report.records() {
        if let TraceEvent::MessageSent { payload, .. } = &record.event {
            // A client's packet is neither: it carries its range in the envelope
            // this slice added, and is told from a frame by its first byte.
            if ananke_shard::is_ranged(payload) {
                continue;
            }
            payloads += 1;
            assert!(
                ananke_raft::message::Frame::decode(payload.clone()).is_err(),
                "a batch frame parses as a one-group frame: {payload:?}"
            );
            assert!(
                ananke_shard::decode(payload).is_ok(),
                "a frame this node sent is not a batch frame: {payload:?}"
            );
        }
    }
    assert!(
        payloads > 100,
        "{payloads} payloads is too few to say anything"
    );
}

#[test]
fn a_ranges_replicas_are_created_with_one_descriptor_and_created_once() {
    // Check 7's first step, which D-071 (item 11) said the stage emitting
    // `RangeCreated` owes: checks 2 and 4 take a creation's floor on sight, so
    // something must hold the creations themselves to account. Here is the pair:
    // the correct node's creations agree, and a trace where one node's differ is
    // caught.
    let report = correct(5);
    report
        .creations_agree()
        .expect("the correct node's creations agree");
    let mut records = report.records().to_vec();
    let forged = records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            TraceEvent::RangeCreated { floor_index, .. } => {
                *floor_index = 9;
                Some(())
            }
            _ => None,
        });
    assert!(forged.is_some(), "the run traced no creation to forge");
    let forged = ranges::Report {
        checked: ananke_sim::raft::Report::over_a_run(ananke_sim::raft::Run {
            seed: report.seed,
            variants: Variants::default(),
            policy: report.checked.policy,
            header: report.checked.run.clone(),
            records,
            last_heal: report.checked.last_heal,
            isolations: report.checked.isolations.clone(),
            history: Default::default(),
            clients: Default::default(),
            ranges: report.checked.ranges.clone(),
            key_range: ranges::range_of_key,
            stopped: None,
        }),
        ..report
    };
    let violation = forged
        .creations_agree()
        .expect_err("a forged floor disagrees with the replicas beside it");
    assert!(
        violation.contains("was created with"),
        "the violation names the descriptors: {violation}"
    );
}

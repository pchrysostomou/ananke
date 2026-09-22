//! The node scenario's sweep (SHARD.md §2, §4, §12's Stage B): three nodes, four
//! ranges on every node.
//!
//! What it is for is in `sim/ranges.rs`. What it asserts, at every tier:
//!
//! - the correct node passes every seed: checks 1 to 4 keyed by range, the history's
//!   closure keyed by `(range, index, term)`, the timer check and pre-vote's property
//!   per `(range, server)`, the checks about time asked of each range whose
//!   unimpaired replicas form a majority, the write bound per key — each write from
//!   its own call — and the recovery time per range from the heal (D-076), and that
//!   every payload a node sent a node is this node's batch frame and no frame of the
//!   one-group server's: the checks D-071 keyed, asked for the first time of a trace
//!   that has more than one range to be wrong about;
//! - the run reaches the shape those checks need: four replicas created at bootstrap
//!   on each of three nodes, every range electing and applying, and — read off the
//!   frames themselves, not off the scenario's own parameters — frames carrying
//!   messages of several ranges between two nodes;
//! - the node's own known-buggy variants are caught beside the correct node
//!   (CLAUDE.md's pair rule), at the tier each one's measured rate supports (D-061).
//!
//! What the sweep cannot say is said by a directed scenario beside it
//! (`the_ranges_of_one_node_draw_their_own_election_timeouts`): one node alone, whose
//! four cores each campaign on an election timer of their own, which is what D-057's
//! per-range protocol stream is for.

use std::collections::{BTreeMap, BTreeSet};

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
    let mut busiest_range_per_second = 0.0f64;
    let mut multi_range_frames = 0usize;
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
                (
                    report.records_per_second_per_range(),
                    report.busiest_range_records_per_second(),
                ),
                (
                    report.worst_write_after_heal(true),
                    report.worst_write_after_heal(false),
                ),
                (
                    report.worst_range_recovery(true),
                    report.frames_of_several_ranges(),
                ),
            ),
        )
    });
    let mut worst = std::time::Duration::ZERO;
    let mut worst_anywhere = std::time::Duration::ZERO;
    let mut worst_recovery = std::time::Duration::ZERO;
    for (_, (created, by_leader, by_apply, len, rate, write, range_wide)) in &results {
        let (rate, busiest) = rate;
        let (took, anywhere) = write;
        let (recovery, several) = range_wide;
        if let Some(took) = took {
            worst = worst.max(*took);
        }
        if let Some(took) = anywhere {
            worst_anywhere = worst_anywhere.max(*took);
        }
        if let Some(took) = recovery {
            worst_recovery = worst_recovery.max(*took);
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
        busiest_range_per_second = busiest_range_per_second.max(*busiest);
        multi_range_frames += several;
    }
    let bound = ranges::Report::write_bound();
    println!(
        "ranges: {seeds} seeds, {creations} bootstrap creations, leaders by range {leaders:?}, \
         applies by range {applies:?}, {records} records in all, at most \
         {per_range_per_second:.0} trace records per virtual second per range and \
         {busiest_range_per_second:.0} of the busiest range's own; {multi_range_frames} peer \
         frames carried messages of more than one range; the worst first write to a key of a \
         live range after the heal took {worst:?} from its own call of the {bound:?} bound on a \
         run the bound is asked of, a margin of {:?}, and {worst_anywhere:?} on any run; the \
         worst live range took {worst_recovery:?} after the heal to complete a write, a margin \
         of {:?}",
        bound.saturating_sub(worst),
        bound.saturating_sub(worst_recovery)
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
    // The other half of that shape, read off the frames themselves: a frame between
    // two nodes carries messages of several ranges, which is what four ranges to a
    // node is the parameter for (SHARD.md §12). The figure was measured before it
    // was asserted (D-061): 3 898 + 1 194 + 31 = 5 123 such frames on seeds 1 to 5,
    // about a thousand a seed, against a floor of a hundred a seed here.
    assert!(
        multi_range_frames >= 100 * seeds as usize,
        "{multi_range_frames} frames of several ranges over {seeds} seeds is too few to say a \
         frame between two nodes carries several ranges"
    );
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
    // What says it happened is the frames: each one decoded, and counted by how many
    // messages and how many distinct ranges it carried. Counting the ranges the
    // *run* names would restate the scenario's own shape — `RANGES` is four, and
    // `bootstrap_creations` and `applies_by_range` assert it twice over — and would
    // be green on a node whose every frame carried exactly one message.
    let report = correct(3);
    let (messages, ranges_per) = report.frames_carried();
    let frames: usize = messages.values().sum();
    println!(
        "ranges: seed 3 sent {frames} peer frames, messages per frame {messages:?}, ranges per \
         frame {ranges_per:?}"
    );
    let several = report.frames_of_several_ranges();
    assert!(
        several >= 100,
        "{several} of {frames} frames carried more than one range: {ranges_per:?}"
    );
    assert!(
        ranges_per.keys().any(|carried| *carried >= 3),
        "no frame carried three ranges at once: {ranges_per:?}"
    );
    assert!(
        messages.keys().any(|carried| *carried > 1),
        "no frame carried more than one message: {messages:?}"
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
    //
    // The direction itself is pinned on **every seed of every tier**, because
    // `ranges::Report::frames_are_this_nodes` is one of `check`'s own steps (it was
    // this one seed's until the review of D-076 found the claim wider than the
    // check). What is left here is the argument written down beside a run of it,
    // and the count that says the run had frames to say it of.
    let report = correct(4);
    report
        .frames_are_this_nodes()
        .expect("every payload is this node's batch frame and no one-group frame");
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
    // The pair (CLAUDE.md:52-57): a payload that is a one-group frame is caught by
    // the check on the run's own frames, whichever seed it runs.
    let prevote = ananke_raft::message::Frame {
        from: ananke_raft::ServerId(1),
        message: ananke_raft::message::Message::PreVote {
            term: 1,
            last_index: 0,
            last_term: 0,
        },
    }
    .encode();
    assert!(
        ananke_raft::message::Frame::decode(prevote.clone()).is_ok(),
        "the forged payload is a one-group frame"
    );
    let mut records = report.records().to_vec();
    let put = records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            TraceEvent::MessageSent { payload, .. } if !ananke_shard::is_ranged(payload) => {
                *payload = prevote.clone();
                Some(())
            }
            _ => None,
        });
    assert!(put.is_some(), "the run sent a peer frame to forge");
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
        .frames_are_this_nodes()
        .expect_err("a one-group frame among the node's own is not caught");
    assert!(
        violation.contains("parses as a frame of the one-group server"),
        "the violation names what it found: {violation}"
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

#[test]
fn the_ranges_of_one_node_draw_their_own_election_timeouts() {
    // D-057 keyed the protocol stream by range and this node is its first caller
    // ever (`server.rs`, `Environment::range_rng`): each core is seeded from
    // `n{id}/r{range}/protocol`, so two ranges of one node draw different election
    // timeouts and four ranges do not campaign in lockstep.
    //
    // The sweep cannot say so — three live nodes reset each other's timers, and a
    // range whose leader is elsewhere never campaigns at all — so this is the
    // directed scenario D-061 asks for in place of a lower bar: one node alone, its
    // two fellow voters configured and never started, where the only thing that
    // moves an election timer is the timer.
    //
    // The pair is the plausible bug it is built against: a node seeded once, whose
    // four cores share a seed. Their timeouts are then equal, they campaign on the
    // same tick of the same ticker, and all four first pre-votes fall at one
    // instant — which is what this check reads.
    //
    // The figures were measured before they were asserted (D-061): over seeds 1 to
    // 4 the node's four ranges first campaigned at 4, 3, 3 and 4 distinct instants
    // — two ranges of one node draw the same election timeout now and then, since a
    // timeout is a whole number of ticks — and a node seeded once gives 1 on every
    // seed. So each seed is asked for more than one instant, and the set of seeds
    // for at least one seed where all four differ.
    let mut all_four = 0usize;
    for seed in 1..=4 {
        let records = ranges::alone(seed, std::time::Duration::from_millis(600));
        let first = ranges::first_campaigns(&records);
        assert_eq!(
            first.len(),
            ranges::RANGES as usize,
            "seed {seed}: only {:?} of the node's four ranges campaigned",
            first.keys().collect::<Vec<_>>()
        );
        let at: BTreeSet<_> = first.values().copied().collect();
        println!(
            "ranges: seed {seed} first campaigned per range at {first:?} — {} distinct instants",
            at.len()
        );
        assert!(
            at.len() > 1,
            "seed {seed}: the node's four ranges all campaigned at {at:?}, which is what a node \
             whose cores share one seed looks like"
        );
        all_four += usize::from(at.len() == ranges::RANGES as usize);
    }
    assert!(
        all_four > 0,
        "no seed gave the node's four ranges four election timeouts of their own"
    );
}

/// Q15's whole-node refusal and its re-seed, on the node that has real range
/// membership (SHARD.md §11, storage 8; §12's "A loss in the shared engine").
///
/// A loss in the shared engine is not one range's: the node owns one engine (Q2), so
/// the refusal is the node's and every replica on it goes down with it. The node then
/// rebuilds into a *fresh* engine in a new directory beside the refused one, which
/// stays marked lost and quiesced (D-041), and each replica's durable refused mark is
/// written into the new engine before that replica serves anything (D-035, D-042).
///
/// Directed and not a sweep because the thing under test happens once, at a start; the
/// rate rule (D-061) asks a tier of a *sweep's* assertion, and there is no sweep here
/// to measure one on. What the sweep beside it cannot reach at all is the refusal
/// itself: the node scenario raises no disk fault that loses a store.
///
/// The three variants this catches are each a mutation a single-range world could not
/// see. With one range on a node, refusing only that range *is* refusing the node, and
/// a per-range incarnation stream is the node's generator drawn once.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[test]
fn a_loss_in_the_shared_engine_refuses_the_whole_node_and_reseeds_beside_it() {
    let for_ = std::time::Duration::from_millis(600);
    let every_range: BTreeSet<u64> = (0..ranges::RANGES)
        .map(|i| ranges::FIRST_RANGE + i)
        .collect();

    let (records, dirs) = ranges::refused_whole_dirs(1, for_, NodeVariants::correct());
    let read = ranges::refusal(&records);

    // The node is refused once, as a node, and every replica it holds is refused with
    // it — all four, not the one whose store open failed.
    assert_eq!(read.refusals, 1, "one refusal, the node's");
    assert_eq!(
        read.replicas_refused, every_range,
        "a loss in the shared engine refuses every replica on the node"
    );

    // Each replica's durable refused mark is in the new engine, and none of them
    // traced anything of its own before its mark was durable.
    assert_eq!(
        read.marked, every_range,
        "every re-seeded replica's refused mark is durable"
    );
    assert_eq!(
        read.served_before_the_mark,
        BTreeSet::new(),
        "no replica speaks before its refused mark is durable"
    );

    // A re-seeded replica is not a bootstrap: its `RangeCreated` is its install's,
    // with `cause: snapshot`, and the install is the node's snapshot wiring, which no
    // slice has built yet.
    assert_eq!(
        read.bootstrapped,
        BTreeSet::new(),
        "a re-seeded replica is created by its install, not at bootstrap"
    );

    // The incarnation is per replica, drawn from the node's generator, never the
    // first incarnation a fresh store opens at (Q26, D-042).
    assert_eq!(
        read.incarnations.keys().copied().collect::<BTreeSet<u64>>(),
        every_range,
        "every replica restates an incarnation of its own"
    );
    assert!(
        read.incarnations.values().all(|&i| i > 1),
        "no re-seeded replica keeps a fresh store's incarnation 1: {:?}",
        read.incarnations
    );
    let distinct: BTreeSet<u64> = read.incarnations.values().copied().collect();
    assert_eq!(
        distinct.len(),
        read.incarnations.len(),
        "the incarnations are drawn per replica, not once for the node: {:?}",
        read.incarnations
    );

    // D-041: the refused directory is still there, still marked lost, and the node
    // rebuilt beside it rather than in it.
    assert_eq!(
        dirs.get("node"),
        Some(&true),
        "the refused directory stays marked lost: {dirs:?}"
    );
    assert_eq!(
        dirs.get("node-g1"),
        Some(&false),
        "the re-seed built the generation beside it: {dirs:?}"
    );

    // The pair rule, each variant beside the correct node above.
    let one_range = ranges::refusal(&ranges::refused_whole(
        1,
        for_,
        NodeVariants::of(&[NodeVariant::RefuseOneRangeOnly]),
    ));
    assert_ne!(
        one_range.replicas_refused, every_range,
        "RefuseOneRangeOnly leaves the node's other replicas serving over a lost engine"
    );

    let (_, in_place) = ranges::refused_whole_dirs(
        1,
        for_,
        NodeVariants::of(&[NodeVariant::ReseedIntoRefusedDir]),
    );
    assert_eq!(
        in_place.get("node-g1"),
        None,
        "ReseedIntoRefusedDir builds no directory beside the refused one: {in_place:?}"
    );

    let unmarked = ranges::refusal(&ranges::refused_whole(
        1,
        for_,
        NodeVariants::of(&[NodeVariant::ServeBeforeRefusedMark]),
    ));
    assert_eq!(
        unmarked.marked,
        BTreeSet::new(),
        "ServeBeforeRefusedMark writes no refused mark at all"
    );
    assert_ne!(
        unmarked.served_before_the_mark,
        BTreeSet::new(),
        "and its replicas answer without one: {unmarked:?}"
    );
    assert!(
        unmarked.incarnations.values().any(|&i| i == 1),
        "a replica that never marked keeps a fresh store's incarnation: {:?}",
        unmarked.incarnations
    );

    let per_range = ranges::refusal(&ranges::refused_whole(
        1,
        for_,
        NodeVariants::of(&[NodeVariant::IncarnationPerRangeStream]),
    ));
    assert_ne!(
        per_range.incarnations, read.incarnations,
        "IncarnationPerRangeStream draws a replica's number from its range's stream"
    );
}

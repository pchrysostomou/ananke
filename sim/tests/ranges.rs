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
         frames carried messages of more than one range; the slowest post-heal write to a key \
         of a live range took {worst:?} from its own call of the {bound:?} bound on a \
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
    // was asserted (D-061): 1 103 898 such frames over a thousand seeds, about
    // 1 104 a seed, and 107 934 over a hundred — against a floor of a hundred a
    // seed here.
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
    // And the two histograms are of two different things, which is the whole claim:
    // every assertion above is satisfied by a node that batched four messages of
    // *one* range, if "ranges per frame" is really the message count under another
    // name. D-076's review folded the ranges per frame as `decoded.messages.len()`
    // and watched nothing fail. On this seed the two differ in every bucket —
    // messages {1: 2604, 2: 928, 3: 23} against ranges {1: 2681, 2: 852, 3: 22} —
    // because a frame carrying two messages of one range counts once in each
    // histogram and in different places.
    assert_ne!(
        messages, ranges_per,
        "the ranges a frame carried are counted as the messages it carried: {messages:?}"
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
    // The pair (CLAUDE.md:52-57), both ways round, because the check has two
    // clauses and D-076's review deleted the second one and watched every test in
    // the tree stay green: a payload that *is* a one-group frame is caught, and a
    // payload that is neither codec's is caught too.
    let seed = report.seed;
    let variants = report.variants;
    let node_variants = report.node_variants;
    let schedule = report.schedule.clone();
    let policy = report.checked.policy;
    let header = report.checked.run.clone();
    let last_heal = report.checked.last_heal;
    let isolations = report.checked.isolations.clone();
    let of_run = report.checked.ranges.clone();
    let with_payload = |payload: bytes::Bytes| {
        let mut records = report.records().to_vec();
        let put = records
            .iter_mut()
            .find_map(|record| match &mut record.event {
                TraceEvent::MessageSent { payload: sent, .. } if !ananke_shard::is_ranged(sent) => {
                    *sent = payload.clone();
                    Some(())
                }
                _ => None,
            });
        assert!(put.is_some(), "the run sent a peer frame to forge");
        ranges::Report {
            seed,
            variants,
            node_variants,
            schedule: schedule.clone(),
            checked: ananke_sim::raft::Report::over_a_run(ananke_sim::raft::Run {
                seed,
                variants,
                policy,
                header: header.clone(),
                records,
                last_heal,
                isolations: isolations.clone(),
                history: Default::default(),
                clients: Default::default(),
                ranges: of_run.clone(),
                key_range: ranges::range_of_key,
                stopped: None,
            }),
        }
    };
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
    let violation = with_payload(prevote)
        .frames_are_this_nodes()
        .expect_err("a one-group frame among the node's own is not caught");
    assert!(
        violation.contains("parses as a frame of the one-group server"),
        "the violation names what it found: {violation}"
    );
    // The other clause: a payload of neither codec — a batch frame's version byte
    // with nothing behind it — which is what a node sending something this scenario
    // cannot read would look like.
    let garbled = bytes::Bytes::from_static(b"\x01\x00\x00");
    assert!(
        !ananke_shard::is_ranged(&garbled),
        "the forged payload is no client packet"
    );
    assert!(
        ananke_raft::message::Frame::decode(garbled.clone()).is_err(),
        "the forged payload is no one-group frame either"
    );
    let violation = with_payload(garbled)
        .frames_are_this_nodes()
        .expect_err("a payload of neither codec is not caught");
    assert!(
        violation.contains("is no batch frame"),
        "the violation names what it found: {violation}"
    );
}

#[test]
fn a_ranges_replicas_are_created_with_one_descriptor_and_created_once() {
    // Check 7's first step, which D-071 (item 11) said the stage emitting
    // `RangeCreated` owes: checks 2 and 4 take a creation's floor on sight, so
    // something must hold the creations themselves to account. Here is the pair: the
    // correct node's creations agree, and a trace forged against each of the four
    // things the check claims is caught, naming it.
    //
    // Four, because D-076's review deleted three of them one at a time — the
    // `cause` clause, the "created twice" clause, and the generation and voters out
    // of the descriptor compared — and the sweep and this test stayed green on all
    // three: one forged floor pinned the floor comparison and nothing else.
    let report = correct(5);
    report
        .creations_agree()
        .expect("the correct node's creations agree");
    let seed = report.seed;
    let variants = report.variants;
    let node_variants = report.node_variants;
    let schedule = report.schedule.clone();
    let policy = report.checked.policy;
    let header = report.checked.run.clone();
    let last_heal = report.checked.last_heal;
    let isolations = report.checked.isolations.clone();
    let of_run = report.checked.ranges.clone();
    let over = |records: Vec<ananke_env::sim::TraceRecord>| ranges::Report {
        seed,
        variants,
        node_variants,
        schedule: schedule.clone(),
        checked: ananke_sim::raft::Report::over_a_run(ananke_sim::raft::Run {
            seed,
            variants,
            policy,
            header: header.clone(),
            records,
            last_heal,
            isolations: isolations.clone(),
            history: Default::default(),
            clients: Default::default(),
            ranges: of_run.clone(),
            key_range: ranges::range_of_key,
            stopped: None,
        }),
    };
    // A creation whose floor disagrees with the replicas beside it.
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
    let violation = over(records)
        .creations_agree()
        .expect_err("a forged floor disagrees with the replicas beside it");
    assert!(
        violation.contains("was created with"),
        "the violation names the descriptors: {violation}"
    );
    // A creation whose generation and voters disagree: the rest of the descriptor
    // check 7 compares, which a check that compared the span and the floor alone
    // would pass.
    let mut records = report.records().to_vec();
    let forged = records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            TraceEvent::RangeCreated {
                generation, voters, ..
            } => {
                *generation += 1;
                voters.push(99);
                Some(())
            }
            _ => None,
        });
    assert!(forged.is_some(), "the run traced no creation to forge");
    let violation = over(records)
        .creations_agree()
        .expect_err("a forged generation and voters disagree with the replicas beside it");
    assert!(
        violation.contains("was created with"),
        "the violation names the descriptors: {violation}"
    );
    // A creation by something other than the bootstrap this scenario runs: a split's
    // or an install's, which this node has no path to and which check 7's later
    // steps — the ones the stage producing them owes — are what would hold to
    // account.
    let mut records = report.records().to_vec();
    let forged = records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            TraceEvent::RangeCreated { cause, .. } => {
                *cause = ananke_env::RangeCause::Split;
                Some(())
            }
            _ => None,
        });
    assert!(forged.is_some(), "the run traced no creation to forge");
    let violation = over(records)
        .creations_agree()
        .expect_err("a creation this scenario has no path to is caught");
    assert!(
        violation.contains("was created by Split"),
        "the violation names the cause: {violation}"
    );
    // One replica created twice: a node that re-bootstrapped a range it already
    // held, instead of restating it from its store.
    let mut records = report.records().to_vec();
    let again = records
        .iter()
        .find(|record| matches!(record.event, TraceEvent::RangeCreated { .. }))
        .cloned()
        .expect("the run traced a creation to repeat");
    records.push(again);
    let violation = over(records)
        .creations_agree()
        .expect_err("a replica created twice is caught");
    assert!(
        violation.contains("twice"),
        "the violation says which replica was created twice: {violation}"
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
    // What the check above cannot yet say, asserted as an absence with its reason
    // rather than left as a silent green (CLAUDE.md). §12's exit criterion (c) orders
    // two things — the mark, then the replica's first answer — and on the correct node
    // this scenario has only the first: a re-seeded replica is quarantined and takes
    // part in nothing until its install (RAFT.md §3), and there is no install here to
    // give it, so it never sends a frame at all. `served_before_the_mark` is therefore
    // empty on the correct node because there is no answer to be early, not because an
    // answer was late enough; the variant beside it is what shows the set can fill.
    // The day the node's snapshot wiring gives a re-seeded replica something to answer,
    // this assertion fails, and the pair can be upgraded to the ordering (c) names.
    assert_eq!(
        read.answered_after_the_mark,
        BTreeSet::new(),
        "a re-seeded replica answers nothing at all until its install: {read:?}"
    );
    assert!(
        read.failures.is_empty(),
        "the re-seeded node carries on rather than stopping: {:?}",
        read.failures
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

    // The pair rule, each variant beside the correct node above. Each says what the
    // variant's run *did*, not merely that it differed from the correct one: a run
    // that fell over before it reached the re-seed differs from the correct one too,
    // and a check that only asks for a difference cannot tell the two apart.
    let one_range = ranges::refusal(&ranges::refused_whole(
        1,
        for_,
        NodeVariants::of(&[NodeVariant::RefuseOneRangeOnly]),
    ));
    assert_eq!(
        one_range.replicas_refused,
        BTreeSet::from([ranges::FIRST_RANGE]),
        "RefuseOneRangeOnly refuses the one range whose store open failed and no other, \
         leaving the node's three other replicas unrefused over an engine that lost state"
    );

    let (refused_in_place, in_place) = ranges::refused_whole_dirs(
        1,
        for_,
        NodeVariants::of(&[NodeVariant::ReseedIntoRefusedDir]),
    );
    let in_place_read = ranges::refusal(&refused_in_place);
    assert_eq!(
        in_place.get("node-g1"),
        None,
        "ReseedIntoRefusedDir builds no directory beside the refused one: {in_place:?}"
    );
    // ...because it built *in* the refused one, whose marker says lost, so the fresh
    // engine's own open is refused and the node stops there. Without this the check
    // above would pass on a run that never reached the re-seed at all.
    assert_eq!(
        in_place.get("node"),
        Some(&true),
        "and the refused directory is the one it tried: {in_place:?}"
    );
    assert!(
        in_place_read
            .failures
            .iter()
            .any(|reason| reason.contains("the re-seed's fresh engine")),
        "ReseedIntoRefusedDir reaches the re-seed and is refused by its own marker: {:?}",
        in_place_read.failures
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
    // Every replica still restates an incarnation — the variant is a different draw,
    // not a missing one — and every one of them is a different number from the node
    // generator's. Asserting only that the two maps differ would pass on a run that
    // traced no incarnation at all.
    assert_eq!(
        per_range
            .incarnations
            .keys()
            .copied()
            .collect::<BTreeSet<u64>>(),
        every_range,
        "IncarnationPerRangeStream still gives every replica an incarnation: {:?}",
        per_range.incarnations
    );
    for range in &every_range {
        assert_ne!(
            per_range.incarnations.get(range),
            read.incarnations.get(range),
            "IncarnationPerRangeStream draws range {range}'s number from its range's \
             stream and not from the node's: {:?} against {:?}",
            per_range.incarnations,
            read.incarnations
        );
    }
}

/// The survivors' campaign ticks as offsets from the lowest one's, in range order.
///
/// Dropping a range takes a store open and a bootstrap creation out of the node's
/// start, which moves the whole node a fraction of a tick and can carry every one of
/// its campaigns over a tick boundary together (seed 4 below: all three survivors one
/// tick earlier, every gap between them the same). The offsets are what the seeds
/// themselves decide, and the shift is what the start decides.
fn offsets(ticks: &BTreeMap<u64, u64>) -> Vec<i64> {
    let base = ticks.values().next().copied().unwrap_or(0);
    ticks
        .values()
        .map(|tick| i64::try_from(*tick).expect("small") - i64::try_from(base).expect("small"))
        .collect()
}

#[test]
fn each_range_draws_its_election_timer_from_its_own_stream() {
    // What `Environment::range_rng` buys is not that the four timeouts differ — four
    // draws off one stream differ too, which is why
    // `the_ranges_of_one_node_draw_their_own_election_timeouts` cannot see the
    // difference and D-076's review planted exactly that mutation and watched it
    // live. What it buys is that range r's stream is r's alone: the seed a core is
    // given is a function of (node, range) and of nothing else, so a range added to
    // or taken out of the configuration moves no other range's timer relative to its
    // fellows (D-057, Q13).
    //
    // So this asks the node for three of its four ranges and compares the survivors'
    // election timers with the ones they drew beside the fourth. The range dropped is
    // the *first*, because four draws off one stream would give the survivors the
    // node's first three draws where they had its last three; dropping the last would
    // leave a shared stream looking right.
    //
    // The pair (CLAUDE.md:52-57) is `OneSeedForEveryCore` beside it, the node with
    // D-057's keying abandoned: the same two runs, and its survivors' timers move
    // against each other. Measured before asserted (D-061): on each of seeds 1 to 4
    // the correct node's survivors keep every gap exactly, and the variant's move on
    // every one of the four.
    let for_ = std::time::Duration::from_millis(600);
    let four = ranges::ranges();
    let three: Vec<_> = four.iter().skip(1).cloned().collect();
    let one_seed = NodeVariants::of(&[NodeVariant::OneSeedForEveryCore]);
    let survivors: Vec<u64> = three.iter().map(|range| range.id.get()).collect();
    let of = |ticks: &BTreeMap<u64, u64>| {
        let kept: BTreeMap<u64, u64> = ticks
            .iter()
            .filter(|(range, _)| survivors.contains(range))
            .map(|(range, tick)| (*range, *tick))
            .collect();
        offsets(&kept)
    };
    for seed in 1..=4 {
        let with_four = ranges::first_campaign_ticks(&ranges::alone(seed, for_));
        let with_three = ranges::first_campaign_ticks(&ranges::alone_of(
            seed,
            for_,
            three.clone(),
            NodeVariants::correct(),
        ));
        println!(
            "ranges: seed {seed} campaign ticks with four ranges {with_four:?}, with three \
             {with_three:?}"
        );
        assert_eq!(
            with_three.len(),
            three.len(),
            "seed {seed}: the node of three ranges campaigned with {with_three:?}"
        );
        assert_eq!(
            of(&with_three),
            of(&with_four),
            "seed {seed}: the node's three ranges drew {with_three:?} without range \
             {} and {with_four:?} with it, so a range's timer is not its own",
            four[0].id.get()
        );
        // The pair: the same two runs on the node that seeds every core off one
        // stream, where taking a range out shifts every other range's draw.
        let buggy_four =
            ranges::first_campaign_ticks(&ranges::alone_of(seed, for_, four.clone(), one_seed));
        let buggy_three =
            ranges::first_campaign_ticks(&ranges::alone_of(seed, for_, three.clone(), one_seed));
        println!(
            "ranges: seed {seed} under OneSeedForEveryCore {buggy_four:?} then {buggy_three:?}"
        );
        assert_ne!(
            of(&buggy_three),
            of(&buggy_four),
            "seed {seed}: a node that seeds every core off one stream kept its survivors' \
             timers ({buggy_four:?} then {buggy_three:?}), so this check does not say what \
             it is for"
        );
    }
}

/// The second question D-066 answers, which the scenario above cannot ask: once a node
/// has re-seeded, which directory does its *next start* open?
///
/// The re-seed's own choice is made in the run above. This one is made in
/// `server::run`, before any store opens, over the listing beside the node, and until
/// this test nothing in the simulator asked it — `newest_not_lost` was driven only by
/// `reseed::tests` calling it directly, so a `run` that ignored it and opened the
/// directory configuration names every time passed the whole tree. That is the defect
/// a check that drives the helper instead of the thing under test leaves behind.
///
/// The evidence is what a wrong answer would cost: the configured directory is marked
/// lost, so a node that reopens it is refused a *second* time and re-seeds into a third
/// generation. One refusal in the whole run, and `node-g2` never built, is the start
/// having found the directory its re-seed left.
// PROPOSED(D-077): Q15's whole-node refusal, and the re-seed per replica.
#[test]
fn a_restarted_node_opens_the_directory_its_reseed_built_and_is_not_refused_again() {
    let for_ = std::time::Duration::from_millis(600);
    let every_range: BTreeSet<u64> = (0..ranges::RANGES)
        .map(|i| ranges::FIRST_RANGE + i)
        .collect();

    let (records, dirs) = ranges::refused_whole_restarted(1, for_, NodeVariants::correct());
    let read = ranges::refusal(&records);

    assert_eq!(
        read.refusals, 1,
        "one refusal in the whole run: the restart opened the re-seeded directory and \
         was not refused on the old one's marker"
    );
    assert_eq!(
        read.replicas_refused, every_range,
        "and the one refusal is still the whole node's"
    );
    assert_eq!(
        dirs,
        BTreeMap::from([("node".to_owned(), true), ("node-g1".to_owned(), false)]),
        "the refused directory stays lost, the re-seed's generation is the live one, \
         and the restart built no third: {dirs:?}"
    );
    // The marks the re-seed made durable are what the restart read back: each replica
    // is restated quarantined, on the disk's word rather than on the re-seed's.
    assert_eq!(
        read.marked, every_range,
        "every replica's refused mark survives the crash and is restated"
    );
    assert!(
        read.failures.is_empty(),
        "the restarted node runs: {:?}",
        read.failures
    );
}

/// The pair rule again, for the path this scenario asserts **absent**: a core that
/// reaches the snapshot threshold fails the run, and the check says why.
///
/// `Report::check` said this absence was asserted and it was not: on the unwired node
/// of D-076's day the dropped action left no `RaftSnapshot` record, so the review set
/// the scenario's threshold to twelve, every core asked for takes the host dropped,
/// and all three test targets stayed green. The node serves the action since D-083,
/// so at twelve every core takes a snapshot and the record is in the trace; the
/// scenario's own check, which keys on that record, is what fails the run now.
#[test]
fn a_core_that_asks_for_a_snapshot_action_fails_the_run() {
    // The correct scenario keeps its cores below the threshold, and passes.
    correct(1)
        .check()
        .expect("the scenario's own threshold is never reached");
    // Twelve entries is under any range's share of one run's writes, so every core
    // asks — and the node has nowhere to send the request.
    let report = ranges::asking_for_snapshots(1, 12);
    let violation = report
        .check()
        .expect_err("a run past this scenario's snapshot threshold is not caught");
    println!("ranges: a core reaching the snapshot threshold gives: {violation}");
    assert!(
        violation.contains("took or restated a snapshot"),
        "the violation names the snapshot the trace carries: {violation}"
    );
    assert!(
        violation.contains("asserts that path absent"),
        "the violation names the absence this scenario asserts: {violation}"
    );
}

/// The pair rule for the registration a refused read leaves behind
/// (`NodeVariant::RefusedReadLeft`): the node exactly as it was before D-076, whose
/// `reads` map grows for the life of the run.
///
/// D-076 fixed that leak and pinned `Replica::refuse` in a unit case, which is not
/// the node: D-076's review planted the leak back in `ServerHost::rejected`, left
/// `refuse` untouched, and every test in the tree stayed green. What sees it is
/// `READS_OUTSTANDING` — a replica's registrations bounded on the node's own path —
/// and at a bound of 8 this run caught it on 535 of 1 000 seeds.
///
/// **The bound is 38 since PR #109's merge with `main`** (D-076 point 12, re-measured
/// by D-076's own rule with the stream arms on the node), and this scenario's runs are
/// too short for the leak to reach it: **0 of 1 000 seeds** in release. So the catch is
/// asserted where the bound sees it — `sim/tests/node.rs`'s
/// `a_node_that_leaves_a_refused_reads_registration_behind_is_caught_on_the_node`,
/// under the raft arms — and what this run asserts, at every tier, is the **fault's
/// firing** (D-061): the leaking node holds more reads registered at once than the
/// correct node does on the same seeds, read from the high-water mark both trace
/// (`RaftReadsOutstanding`), and strictly more on some seed. Measured before it was
/// asserted (D-061), on the merged tree in release: the leaking node held at most 19
/// reads at once against the correct node's 4, and more than it on **1 000 of 1 000**
/// seeds, so the firing is asserted at every tier. The catch rate is still printed
/// here, and a catch that does happen is still required to be the bound's.
// PROPOSED(D-086): the read leak's catch moves to the arms; its firing stays here.
#[test]
fn a_node_that_leaves_a_refused_reads_registration_behind_is_caught() {
    let seeds = seeds();
    let results: Vec<(bool, bool, u64, u64)> = sweep(seeds, |seed| {
        let leaking = ranges::run(
            seed,
            Variants::default(),
            NodeVariants::of(&[NodeVariant::RefusedReadLeft]),
        );
        let correct = ranges::run(seed, Variants::default(), NodeVariants::correct());
        let verdict = leaking.check();
        let named = verdict
            .as_ref()
            .err()
            .is_some_and(|violation| violation.contains("registered reads"));
        let worst = |report: &ranges::Report| {
            report
                .checked
                .reads_outstanding_worst()
                .map_or(0, |(_, _, outstanding)| outstanding)
        };
        (verdict.is_err(), named, worst(&leaking), worst(&correct))
    });
    let caught = results.iter().filter(|(failed, _, _, _)| *failed).count();
    let named = results.iter().filter(|(_, named, _, _)| *named).count();
    let leaking_worst = results.iter().map(|(_, _, l, _)| *l).max().unwrap_or(0);
    let correct_worst = results.iter().map(|(_, _, _, c)| *c).max().unwrap_or(0);
    let seeds_the_leak_shows_on = results.iter().filter(|(_, _, l, c)| l > c).count();
    let seeds_the_leak_hides_on = results.iter().filter(|(_, _, l, c)| l < c).count();
    let rate = caught as f64 * 100.0 / seeds as f64;
    println!(
        "ranges: RefusedReadLeft caught on {caught}/{seeds} seeds ({rate:.1}%), {named} of them \
         by the outstanding-reads bound; the leaking node held at most {leaking_worst} reads \
         at once against the correct node's {correct_worst}, and held more than it on \
         {seeds_the_leak_shows_on} seeds"
    );
    // The firing, at every tier: a leak that the bound of 38 does not reach here is
    // still a map that grows where the correct node's does not.
    assert!(
        seeds_the_leak_shows_on > 0,
        "the node that keeps every read it refuses never held more reads than the correct \
         node over {seeds} seeds, so the leak was not injected"
    );
    assert_eq!(
        seeds_the_leak_hides_on, 0,
        "on {seeds_the_leak_hides_on} seeds the leaking node held fewer reads at once than the \
         correct node, which a map that only grows cannot do: the variant is not the leak"
    );
    // A catch here is the bound's or it is not this pair's; the catch itself is
    // asserted under the raft arms, where the bound sees it (D-076 point 12).
    assert_eq!(
        named,
        caught,
        "{} of the {caught} seeds caught were caught by something other than the \
         outstanding-reads bound, which is not what this pair is for",
        caught - named
    );
}

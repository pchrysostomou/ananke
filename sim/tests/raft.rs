//! The Phase 2 sweep (SPEC.md §3): the correct server holds every invariant of
//! RAFT.md §2 on every seed under the full network fault model, partitions, one-way
//! blocks and crashes with the disk model, and each known-buggy variant this stage
//! ships (RAFT.md §5) is caught on some seed. The catch rate of each is printed, so a
//! hundred-seed run reports it.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use ananke_env::{ClientOp, DropReason, TraceEvent};
use ananke_raft::core::{Variant, Variants};
use ananke_raft::invariants::{self, Checker};
use ananke_raft::store::{LOST_STATE, STORE_MARKER};
use ananke_sim::raft::DRIFT_BOUND_PPM;
use ananke_sim::raft::{self, Fault, Moved, RecordTime, TimerResets};
use ananke_sim::{seeds, sweep, verdict, write_trace};

/// Two runs with the same seed produce byte-identical traces.
#[test]
fn same_seed_gives_byte_identical_trace() {
    let first = raft::run(42, Variant::Correct);
    let second = raft::run(42, Variant::Correct);
    assert_eq!(first.jsonl().as_bytes(), second.jsonl().as_bytes());
}

/// The seed-42 trace is written for the studio.
#[test]
fn the_seed_42_trace_is_written_for_the_studio() {
    let report = raft::run(42, Variant::Correct);
    write_trace("raft-42", &report.jsonl());
    report.check().unwrap();
}

/// Seed 164, found by a local ten-thousand-seed run on 1373601: the first
/// correct-server failure a ten-thousand-seed run produced, and a gap in the timer
/// check rather than in the server. Server 2 was about sixty-eight entries behind
/// when server 3, leading term 12 with its log through index 276 and its snapshot
/// at 210, began feeding it `InstallSnapshot` chunks at 11.854 s: it had appended
/// through 208. At 13.340 s the check flagged it for 399.26 ms without an
/// AppendEntries or a granted vote, against its 399.19 ms bound. Nobody led in
/// that stretch: server 3 had lost its quorum at 12.936 s and was pre-voting, and
/// the 21 chunks server 2 received in it were that deposed leader's leftover
/// stream. The check read only AppendEntries as contact; it now counts an
/// InstallSnapshot of the receiver's term or later as a reset too, by its term
/// and not by who sent it (D-030's stanza, f54b468). The same stretch also holds
/// an install's restatement, at 13.143 s, so D-039's later arm alone
/// would have silenced it as well: no predicate on this seed isolates the
/// InstallSnapshot arm.
///
/// Today the seed does not reach that situation, and the test asserts so. Re-audited
/// on the tree with D-056's send queue, which moved every schedule: every frame now
/// arrives its write time later, from the run's first frame on, so the run after the
/// first delivery is another run. On it the timer replay that reads AppendEntries
/// alone as a leader's contact finds no gap at all: no follower goes past its bound,
/// fed by snapshot chunks or not, over three refusals of server 3. The day [`raft::Report::snapshot_fed_timer_gaps`] is not empty, the seed
/// reaches the situation again and the pin should assert it: those gaps present,
/// and the check green.
///
/// On the tree before D-056 the fault list was the same through the 9.691 s
/// partition, but the run elected a different leader after it: server 2 finished its
/// install before the 12.541 s partition, and the longest AppendEntries-less stretch
/// that held an InstallSnapshot was 161.6 ms, server 3's from 11.822 s, against its
/// 394 ms bound.
///
/// D-047 does not bear on this seed: its failure was a gap in the timer
/// check's rules — no reset for an InstallSnapshot — which neither of a record's two
/// times supplies. The replay now reads records by decision time, which moves only
/// the resets a server makes at a step — a campaign, a granted vote, a step-down —
/// earlier, by the persist each waited on. The numbers above are the same under it.
/// On the trace the seed failed with, which carries no decision times, the
/// predicate still finds its one gap, and the reset that ended it, a granted vote,
/// was traced 30.4 ms after the gap was flagged: no persist could have put its
/// decision before the flag, a vote's lag being at most 6.89 ms over the correct
/// server's first 3 000 seeds on this tree.
#[test]
fn seed_164_which_a_local_ten_thousand_seed_run_found_stays_green() {
    let report = raft::run(164, Variant::Correct);
    report.check().unwrap();
    let gaps = report.snapshot_fed_timer_gaps();
    assert!(
        gaps.is_empty(),
        "seed 164 reaches a snapshot-fed timer gap again — a follower past its bound on \
         AppendEntries alone while InstallSnapshot chunks of its term arrive: {gaps:?}; \
         pin the mechanism: those gaps present and the check green"
    );
}

/// Seed 385, found by a local ten-thousand-seed run on f54b468: server 1, cut off
/// alone by the partition at 14.035 s while it finished installing snapshot 329 —
/// its last chunk arrived at 14.020 s and the leader's last AppendEntries at
/// 14.031 s — completed the install locally, and the install's restatement at
/// 14.260 s rebuilt its core with a fresh election timer, so it campaigned at
/// 14.358 s: 98 ms after the restatement, 327 ms after the leader's last contact
/// and 25 ms past its 302 ms bound. The check did not know that the restatement
/// resets the timer; it now counts one on a server that never went down as the
/// leader's contact (D-039).
///
/// Today the seed does not reach that situation, and the test asserts so. Re-audited
/// on the tree with D-056's send queue, which moved every schedule (see seed 164's
/// pin): the timer replay without D-039's arm finds no gap anywhere on the run, so
/// there is no stretch for a restatement to rescue, over five refusals: servers 2
/// and 3 for lost state, and server 1 once for a damaged log and twice more on the
/// store's lost mark.
/// With the check green, [`raft::Report::timer_gaps_rescued_by_restatement`] is every
/// gap of that replay; the day it is not empty the seed reaches the situation again,
/// and the pin should assert it: those gaps present, and the check green.
///
/// On the tree before D-056 the partition isolated server 1 alone as the failing
/// run's did, but leader 3 had compacted only through 324, so server 1 was kept
/// current by AppendEntries and had no install in flight; it pre-voted 131.6 ms after
/// the leader's last AppendEntries, 43.5 % of its bound, with no restatement between.
///
/// D-047 does not bear on this seed: its failure was the timer check not
/// knowing that an install's switch starts a fresh timer — a missing rule — which
/// neither of a record's two times supplies. The restatement that rule reads is
/// traced as the new incarnation starts and has one time. The replay now reads
/// records by decision time; the numbers above are the same under it. On the trace
/// the seed failed with, which carries no decision times, the predicate still finds
/// its one gap, and the reset that ended it, the pre-candidate campaign, was traced
/// 22.9 ms after the gap was flagged; a pre-vote campaign persists nothing and is
/// traced as its step is taken.
#[test]
fn seed_385_which_a_local_ten_thousand_seed_run_found_stays_green() {
    let report = raft::run(385, Variant::Correct);
    report.check().unwrap();
    let gaps = report.timer_gaps_rescued_by_restatement();
    assert!(
        gaps.is_empty(),
        "seed 385 reaches an install's restatement inside a follower's timer window again, \
         with the campaign after the bound: {gaps:?}; pin the mechanism: those gaps present \
         and the check green"
    );
}

/// The ten-thousand-seed nightly's seed 7381, on ea6fe7d: server 2 held snapshots
/// through index 128 when it was crashed at 6.304 s, leading term 7, by a Figure 8
/// driver. Its restart was refused at 6.641 s, `MANIFEST-000005` unreadable, and
/// the leader re-seeded it from snapshot 58, the newest that leader had and below
/// the floor the lost store had reached. It appended past 58 and applied through
/// 64, was crashed again at 8.068 s, and on restart its restatement recovered an
/// applied index of 65. The checker's snapshot floor only ever rose, so it still
/// stood at the lost store's 128 and read index 65 as covered by it rather than
/// held by the log, and state machine safety reported that the log did not hold
/// it. An installed snapshot now sets the floor exactly (D-030, cb15eb1).
///
/// Today the seed does not reach that situation, and the test asserts so: no
/// restatement replays an index between the exact floor and the risen one, and no
/// install lowers a floor at all, which is the only way the two rules ever
/// disagree. Re-audited on the tree with D-056's send queue, which moved every
/// schedule (see seed 164's pin): the run's only refusal is server 2's at 12.575 s,
/// for lost state (table 5 dropped), with its floor at 295, and the leader re-seeds
/// it from snapshot 334, above that floor; server 3's installs, of 50, 102 and 236,
/// are each above the floor it had reached too. So the two floor rules agree at every
/// event, the seed would pass the old checker too, and this pin holds the seed green
/// without exercising the fix. (Before D-056 the one refusal was also server 2's, at
/// 12.585 s for a missing log head, floor 251, re-seeded from 312.) The day
/// [`raft::Report::floor_lowering_installs`] is not empty, the seed is near the
/// situation again and the pin should be re-audited; the day
/// [`raft::Report::recoveries_under_a_lost_floor`] is not empty, it should assert
/// the mechanism: that replay present, and the check green.
///
/// D-047 does not bear on this seed: its failure was the checker's floor
/// never coming back down on a re-seed, a gap in a fold that reads records in
/// order and no time at all, so neither of a record's two times could have
/// prevented it; both predicates are the same fold and unchanged.
#[test]
fn seed_7381_which_the_first_nightly_found_stays_green() {
    let report = raft::run(7381, Variant::Correct);
    report.check().unwrap();
    let replays = report.recoveries_under_a_lost_floor();
    assert!(
        replays.is_empty(),
        "seed 7381 replays a recovered applied index under a lost floor again: {replays:?}; \
         pin the mechanism: that replay present and the check green"
    );
    let lowered = report.floor_lowering_installs();
    assert!(
        lowered.is_empty(),
        "seed 7381 installs a snapshot below a floor its server had reached, where the old \
         floor rule and the exact one part: {lowered:?}; re-audit the pin"
    );
}

/// The ten-thousand-seed nightly's seed 6325 (run 34496762339): server 1 finished
/// installing a snapshot at index 129 and the schedule crashed it forty-one
/// milliseconds later, inside the adoption's copy. The adoption as built had
/// removed the old store's `CURRENT` and files before the copies' directory
/// entries were synced, so the crash lost the copied tables; its bit rot landed
/// on the staging directory's `CURRENT`, which the next start swept as debris —
/// the only copy left; and the engine, finding nothing on disk, opened a fresh
/// store. A voter holding term 5 and a hundred and nineteen committed entries
/// restated term 0, applied 0, and committed-entries-stay reported the
/// truncation from index 1. The adoption now copies and syncs first and switches
/// `CURRENT` last, a damaged staging `CURRENT` is refused rather than swept, and
/// a directory carrying the store marker never opens fresh (D-041).
///
/// Today the seed does not reach that situation, under the correct server or the
/// adoption as built, and the test asserts both. The crash that hit the nightly's
/// adoption was no aimed fault but the schedule's first, `Crash { server: 1 }` for
/// 312 ms, half a second after the second lease trial heals, and it still fires;
/// the seed draws neither the adoption crash storm nor the crash aimed at an
/// install. What moved is the install it hit. Re-audited on the tree with D-056's
/// send queue, which moved every schedule (see seed 164's pin): that crash lands at
/// 5.512 s under the correct server, 247 ms after server 1's adoption of that stretch
/// closed (at 5.265 s), and at 5.812 s as built, 126 ms after its closed (at 5.686 s). No crash lands inside any of the correct run's 8 adoption windows or the
/// variant's 9: the nearest a crash comes to a window of its own server is 159 ms
/// before one under the correct server (server 2, crashed at 18.466 s, installing at
/// 18.625 s) and those 126 ms as built, and the variant passes the seed. (Before
/// D-056 the install finished 182 ms and 208 ms after the trial's heal, and the crash
/// landed 262 ms and 230 ms after the adoption closed, over 12 and 9 windows.) The day
/// [`raft::Report::adoption_windows`] shows a crash inside one, the pin should
/// assert the mechanism: under `AdoptionAsBuilt` the truncation from index 1
/// reported, and under the correct server the adoption re-run or the staging
/// store refused, and the check green.
#[test]
fn seed_6325_which_the_nightly_found_stays_green() {
    for variant in [Variant::Correct, Variant::AdoptionAsBuilt] {
        let report = raft::run(6325, variant);
        let windows = report.adoption_windows();
        assert!(
            !windows.is_empty(),
            "seed 6325 under {variant:?} adopts no install at all: the absence below means nothing"
        );
        let crashed: Vec<_> = windows.iter().filter(|w| w.crashed.is_some()).collect();
        assert!(
            crashed.is_empty(),
            "seed 6325 under {variant:?} crashes a server inside an adoption again: {crashed:?}; \
             pin the mechanism rather than the green"
        );
        assert_eq!(
            report.check().err(),
            None,
            "seed 6325 under {variant:?} no longer passes: re-audit the pin"
        );
    }
}

/// Seed 5909's wedge, the nightly's (run 34496762339, on ea6fe7d), and what the
/// correct server does with the seed today.
///
/// On the nightly's trace the leader's last commit was 329 at 13.43 s, and
/// nothing committed for the remaining 5.4 s of the run. Leader 1 had been
/// streaming snapshot 329 to server 2 since 13.872 s; between 15.261 s and
/// 15.370 s it re-took the same snapshot five times into `/raft/snap-329`, the
/// one directory that stream was reading, and the stream never completed — from
/// 15.361 s the leader sent `000010.sst` at offset 0 seven hundred times while
/// server 2 kept asking for `000005.sst` — so server 2 was never counted. Server
/// 3, refused and re-seeded, with a log ending at the 333 it had acknowledged, was
/// designated snapshot-fed and queued behind that never-completing stream: it
/// received no chunk, and every one of its 302 answers to the term-11 leader was
/// a rejection with hint 334. It was not a stale match index: that leader was
/// elected at 14.757 s, after server 3's last acknowledgement, and a new leader's
/// progress starts at `matched: 0`. So D-043's two bugs — the shared
/// directory scrambling the stream and one stream per leader queuing the other
/// follower behind it — explain the wedge on their own, and D-042's stale
/// `matched` did not occur in it. Takes are now versioned directories, a stream
/// pins the one it opened, and every designated follower is streamed to at once
/// (D-043). D-042 is a sound fix of its own, and today's correct run exercises it,
/// but it was not the fix for 5909.
///
/// Today's run does not replay the wedge. The correct server re-takes no index at
/// all here. What the test asserts is what the run does reach, and what it does
/// not. Re-audited on the tree with D-056's send queue, which moved every schedule
/// (see seed 164's pin), it reaches D-042's reset on another server than before:
/// server 1 is refused at 7.025 s for a damaged log, the leader, server 3, resets
/// its progress at 8.018 s, and it is re-seeded at 8.145 s. (Before D-056 it was
/// server 3, refused at 18.696 s, reset at 18.698 s and re-seeded at 18.970 s.) It
/// does not reach the wedge's shape: no re-take lands under a live stream and
/// scrambles it, and no follower goes uncounted after the last heal.
#[test]
fn seed_5909_which_the_nightly_found_stays_green() {
    let report = raft::run(5909, Variant::Correct);
    report.check().unwrap();
    assert!(
        report.refusal_reset_reseed(1).is_some(),
        "seed 5909 no longer refuses server 1, resets the leader's progress for it and \
         re-seeds it: the D-042 path this pin asserts is gone; re-audit the pin"
    );
    assert_no_stream_wedge(&report);
}

/// Seed 5909 under each of the two variants D-045's Context names, and
/// under both of them at once, pinned to what it does on this tree, which is
/// pass, and to why.
///
/// The Context's premise was that the nightly's wedge needed both bugs, a stale
/// `matched` (D-042) beside a never-completing stream (D-043). Measured on the
/// nightly's trace it did not: the wedge was D-043's alone, as the test above
/// says, and seed 132 below agrees, where `SharedSnapshotDir` alone fails exactly
/// as the pair does. The variant set still makes the pair a run the sweep can ask
/// about, and this seed is asked.
///
/// No run of the seed has two uncounted followers. Re-audited on the tree with
/// D-056's send queue, which moved every schedule (see seed 164's pin), only
/// `IgnoreIncarnation` alone still reaches its half, harmlessly, and the test
/// asserts each half where it is reached and, with the reason, absent where not:
///
/// - under `IgnoreIncarnation` the leader's progress for server 3 goes stale —
///   leader 2 of term 8 had 204 acknowledged, server 3 is refused at 9.503 s, and
///   the leader's 88 AppendEntries after that never probe below 204 while server 3
///   rejects all 88 — but no index is taken twice;
/// - under `SharedSnapshotDir` no index is taken twice at all, so no re-take lies
///   under a stream; its refusals of servers 2 (11.573 s) and 3 (17.854 s) are each
///   reset by the leader and re-seeded;
/// - under the pair neither half: no index is taken twice, and neither refused
///   follower is left with stale progress. Server 2, refused at 11.573 s, is
///   re-seeded at 13.119 s by leader 3 of term 11, elected at 12.592 s, after the
///   refusal, with nothing recorded of server 2's log to go stale, and a re-seed ends
///   the hazard; and server 3, refused at 15.867 s, had last answered with
///   success a leader whose term was over by then (server 3 itself led terms 11 and
///   13 since), so nothing of that term probes it after the refusal.
///
/// (Before D-056 each variant reached its own half and the pair both, one after the
/// other: a harmless re-take under a live stream at 15.071 s, then stale progress for
/// server 3 from 16.128 s.)
#[test]
fn seed_5909_passes_under_both_bugs_together_which_is_the_finding() {
    let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
    for variants in [
        Variants::from(Variant::IgnoreIncarnation),
        Variants::from(Variant::SharedSnapshotDir),
        both,
    ] {
        let report = raft::run(5909, variants);
        assert_eq!(
            report.check().err(),
            None,
            "seed 5909 under {variants:?} no longer passes: the pin's story is out of date"
        );
        assert_no_stream_wedge(&report);
        let takes = report.snapshot_takes();
        let retaken = takes.iter().enumerate().any(|(n, take)| {
            takes[..n]
                .iter()
                .any(|t| t.server == take.server && t.index == take.index)
        });
        assert!(
            !retaken && report.retakes_under_streams().is_empty(),
            "seed 5909 under {variants:?} takes an index twice again, so a re-take may lie \
             under a stream: re-audit the pin"
        );
        let stale: Vec<u64> = (1..=raft::SERVERS)
            .filter(|&s| report.stale_progress(s).is_some())
            .collect();
        if variants == Variants::from(Variant::IgnoreIncarnation) {
            assert_eq!(
                stale,
                [3],
                "seed 5909 under {variants:?} no longer leaves the leader's progress for the \
                 refused server 3, and only it, stale: re-audit the pin"
            );
        } else {
            assert_eq!(
                stale,
                Vec::<u64>::new(),
                "seed 5909 under {variants:?} leaves a refused follower's progress stale again: \
                 re-audit the pin"
            );
        }
    }
}

/// The shape of seed 5909's wedge, asserted absent: no re-take lands under a live
/// stream that the follower then never installs, and the leader in force at the
/// last heal has at most one follower it could never count after it.
fn assert_no_stream_wedge(report: &raft::Report) {
    let seed = report.seed;
    let variants = report.variants;
    let scrambling: Vec<_> = report
        .retakes_under_streams()
        .into_iter()
        .filter(|retake| !retake.installed_after)
        .collect();
    assert!(
        scrambling.is_empty(),
        "seed {seed} under {variants:?} re-takes under a live stream that never completes after \
         it: {scrambling:?}; the wedge's stream half is back, so pin it"
    );
    let uncounted = report.uncounted_after_heal();
    assert!(
        uncounted.len() < 2,
        "seed {seed} under {variants:?} leaves followers {uncounted:?} uncounted after the last \
         heal: the wedge is back, so pin it"
    );
}

/// The combined variant pinned on the seed the sweep does catch it on — seed 132
/// — and, said plainly, what that seed does and does not show.
///
/// It is the first seed of the first thousand on which a server carrying
/// `{IgnoreIncarnation, SharedSnapshotDir}` is caught, by the liveness check: no
/// client write completed after the last heal at 22.32 s. The correct server passes
/// it. The seed does not need both bugs: `SharedSnapshotDir` alone is caught with the
/// byte-identical message, and `IgnoreIncarnation` alone passes. Swept over seeds
/// 0..1000 in release on the tree with D-056's send queue, the pair is caught on 2 of
/// 1000 (this seed and 848, both by the liveness check), `SharedSnapshotDir` alone on
/// the same 2 with the same messages, `IgnoreIncarnation` alone on 0 of 1000, and no
/// seed catches the pair without a single. Nor was a wedge that needs both ever seen:
/// seed 5909's was D-043's alone (above).
///
/// Seed 680 held this pin until D-056's queue moved every schedule (see seed 164's
/// pin); the search SHARD.md §12 asks for at that move found this seed, and seed 680's
/// own test below asserts what it does now.
///
/// What wedges it is D-043's bugs, and the test asserts the mechanism on the
/// `SharedSnapshotDir` run. Leader 3 wins term 14 at 11.262 s and never commits
/// again; the last commit on any server is index 220 at 10.341 s. The seed draws no
/// re-take arm; the server re-takes on its own, eleven times into `/raft/snap-220`
/// between 11.958 s and 13.196 s, five of them under a live stream (12.255, 12.313,
/// 12.373, 12.438 and 12.718 s), none of whose followers ever installs 220 after
/// it. The last stream, to server 1 at 13.635 s, never completes either: a file's
/// first chunk is answered `More` naming another file, 2444 times until the run
/// ends, and every `More` keeps the stream from timing out — the duplicate-file loop
/// D-043 names. Both followers go uncounted after the heal. (Seed
/// 848's wedge is the same re-takes under live streams without the duplicate loop,
/// which is why the first seed and not the second is pinned with this assertion.)
///
/// Why the pair adds nothing here: no server on this seed is ever refused or
/// re-seeded, and every store keeps incarnation 1, so `IgnoreIncarnation` has
/// nothing to ignore and the pair's trace is byte for byte the single's.
///
/// And what the correct server's pass is not: it never re-takes an index on this
/// seed — its run diverges from the variant's within the first 25 ms — so it is
/// the pair rule holding, not evidence that versioned directories and pinned
/// streams rescued a re-take here. The test asserts that too, so the day the
/// correct server re-takes on this seed the pin can say whether the stream
/// survived it.
#[test]
fn seed_132_pins_the_combined_variant_and_the_stream_half_alone_catches_it_too() {
    let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
    let paired = raft::run(132, both);
    let paired_violation = paired
        .check()
        .expect_err("seed 132 under {both:?} no longer reproduces: the pin's story is out of date");
    assert!(
        paired_violation.contains("liveness"),
        "seed 132 under {both:?} is caught, but not by the liveness check: {paired_violation}"
    );

    // The honest half of the pin: the stream bug alone reaches the same wedge on
    // this seed, so 132 is not evidence that the pair is needed.
    let stream = raft::run(132, Variant::SharedSnapshotDir);
    let stream_only = stream.check().expect_err(
        "SharedSnapshotDir alone no longer catches seed 132: the pin's story is out of date",
    );
    assert_eq!(
        paired_violation, stream_only,
        "the pair and the stream half alone no longer fail seed 132 the same way: \
         the pair may now be buying a catch of its own, which is worth recording"
    );
    assert_eq!(
        paired.records, stream.records,
        "the pair's trace on seed 132 is no longer the stream half's: a refusal or a re-seed \
         now gives IgnoreIncarnation something to ignore; re-audit the pin"
    );
    assert_stream_wedge(&stream);

    // And the other half alone reaches nothing, here as everywhere.
    assert_eq!(
        raft::run(132, Variant::IgnoreIncarnation).check().err(),
        None,
        "IgnoreIncarnation alone now catches seed 132: the pin's story is out of date"
    );

    // The pair rule (CLAUDE.md): the correct server passes the seed its buggy
    // siblings fail — without ever meeting a re-take on it.
    let correct = raft::run(132, Variant::Correct);
    correct.check().unwrap();
    let takes = correct.snapshot_takes();
    let retaken: Vec<_> = takes
        .iter()
        .enumerate()
        .filter(|(n, take)| {
            takes[..*n]
                .iter()
                .any(|t| t.server == take.server && t.index == take.index)
        })
        .map(|(_, take)| take)
        .collect();
    assert!(
        retaken.is_empty(),
        "the correct server now re-takes an index on seed 132: {retaken:?}; the pin can now \
         assert whether its stream survived the re-take"
    );
}

/// Seed 680, which pinned the combined variant before D-056's send queue moved every
/// schedule (see seed 164's pin), and what it does now: the pair, each half alone and
/// the correct server all pass it, and no run leaves both followers uncounted after the
/// last heal, which the test asserts, so the day the seed wedges again it says so.
///
/// Each half alone reaches its own situation without the other. Under
/// `IgnoreIncarnation` the leader's progress for server 1 goes stale — leader 3 of term
/// 8 had 441 acknowledged, server 1 is refused at 17.042 s, and 681 of the leader's 690
/// AppendEntries after it are rejected with none accepted — and server 1 goes uncounted
/// after the heal, but server 2 is countable and nothing is re-taken.
///
/// The stream half still re-takes under a live stream, harmlessly: under the pair and
/// under `SharedSnapshotDir` alone, leader 1 re-takes index 54 into `/raft/snap-54`
/// at 2.707 s under its stream to server 3, which never installs 54 after it, but the
/// leader commits index 56 44 ms later, and no follower goes uncounted after the last
/// heal. And the pair's run now differs from the stream half's: server 3 is refused
/// for lost state at 13.599 s and re-seeded, and the stream half alone, whose leader
/// keeps D-042's fix, resets its progress for server 3 at 13.603 s, the first record
/// at which the two traces part; the pair's leader, carrying `IgnoreIncarnation`,
/// does not.
#[test]
fn seed_680_which_pinned_the_combined_variant_before_d056_no_longer_wedges() {
    for variants in [
        Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]),
        Variants::from(Variant::SharedSnapshotDir),
        Variants::from(Variant::IgnoreIncarnation),
        Variants::from(Variant::Correct),
    ] {
        let report = raft::run(680, variants);
        assert_eq!(
            report.check().err(),
            None,
            "seed 680 under {variants:?} is caught again: re-audit the pin, and whether it \
             should pin the combined variant once more"
        );
        let uncounted = report.uncounted_after_heal();
        if variants == Variants::from(Variant::IgnoreIncarnation) {
            // D-042's half alone: stale progress for one refused follower, the other
            // countable.
            assert!(
                report.stale_progress(1).is_some() && uncounted == BTreeSet::from([1]),
                "seed 680 under {variants:?} no longer leaves the leader's progress for the \
                 refused server 1 stale and only it uncounted after the heal ({uncounted:?}): \
                 re-audit the pin"
            );
        } else {
            assert!(
                uncounted.is_empty(),
                "seed 680 under {variants:?} leaves a follower uncounted after the last heal \
                 again ({uncounted:?}): re-audit the pin"
            );
        }
        if variants.contains(Variant::SharedSnapshotDir) {
            // The scrambled stream is there; the wedge it would need is not.
            let scrambling: Vec<_> = report
                .retakes_under_streams()
                .into_iter()
                .filter(|r| !r.installed_after)
                .collect();
            let [retake] = scrambling.as_slice() else {
                panic!(
                    "seed 680 under {variants:?} no longer re-takes under exactly one live stream \
                     that never completes after it: {scrambling:?}; re-audit the pin"
                );
            };
            assert_eq!(
                (
                    retake.leader,
                    retake.follower,
                    retake.index,
                    retake.same_dir
                ),
                (1, 3, 54, true),
                "seed 680 under {variants:?} re-takes under another stream: re-audit the pin"
            );
            assert!(
                report.records.iter().any(|r| r.at > retake.retook
                    && matches!(r.event, TraceEvent::RaftCommit { server: 1, .. })),
                "seed 680 under {variants:?}: leader 1 never commits after its re-take under the \
                 stream to server 3, so the stream half may wedge it now: re-audit the pin"
            );
        } else {
            assert_no_stream_wedge(&report);
        }
    }
}

/// The stream half of the wedge on seed 132 under `SharedSnapshotDir`, from the
/// trace: the last leader re-takes into its own snapshot directory under a live
/// stream; its last stream, opened before the heal, never installs; nothing
/// commits after the first such re-take; the last stream is stuck in the
/// duplicate-file loop; and both followers go uncounted after the heal.
fn assert_stream_wedge(report: &raft::Report) {
    let leader = report
        .records
        .iter()
        .rev()
        .find_map(|r| match &r.event {
            TraceEvent::RaftLeader { server, .. } => Some(*server),
            _ => None,
        })
        .expect("seed 132 elects a leader");
    let scrambling: Vec<_> = report
        .retakes_under_streams()
        .into_iter()
        .filter(|r| r.leader == leader && r.same_dir && !r.installed_after)
        .collect();
    let first = scrambling.first().unwrap_or_else(|| {
        panic!(
            "seed 680's last leader {leader} no longer re-takes into its own snapshot directory \
             under a live stream: the wedge's cause has moved; re-audit the pin"
        )
    });
    let (opened, follower) = report
        .records
        .iter()
        .rev()
        .find_map(|r| match &r.event {
            TraceEvent::RaftSnapshotStreams { server, to, .. } if *server == leader => {
                Some((r.at, *to))
            }
            _ => None,
        })
        .expect("seed 680's leader opens a stream");
    assert!(
        opened < report.last_heal,
        "seed 680's last stream opens after the last heal, so its never installing says nothing"
    );
    assert!(
        !report.records.iter().any(|r| r.at > opened
            && matches!(
                r.event,
                TraceEvent::RaftSnapshot { server, taken: false, .. } if server == follower
            )),
        "seed 680's last stream, to server {follower} at {opened:?}, now installs: re-audit the pin"
    );
    assert!(
        !report
            .records
            .iter()
            .any(|r| r.at >= first.retook && matches!(r.event, TraceEvent::RaftCommit { .. })),
        "seed 680 commits after the first re-take under a live stream at {:?}: re-audit the pin",
        first.retook
    );
    let looped = report.duplicate_chunk_loop(leader, follower, opened);
    assert!(
        looped >= 1000,
        "seed 680's last stream is answered with More naming another file only {looped} times: \
         the duplicate-file loop that keeps it alive is gone; re-audit the pin"
    );
    let uncounted = report.uncounted_after_heal();
    assert_eq!(
        uncounted.len(),
        2,
        "seed 680 no longer leaves both followers uncounted after the heal: {uncounted:?}"
    );
}

/// The thousand-seed premerge's seed 687, on b0e13e8: server 3's engine open
/// dropped SST 1 — it held sequence numbers 1..98 of the state machine — and the
/// store was refused with `LostState { dropped: [1] }`, traced at 7.9209 s as
/// RAFT.md §3 and D-025 require. Seven milliseconds later the refused server's own
/// engine flushed the memtable the recovery had replayed: table 4, then manifest 5
/// listing tables 2, 3 and 4 with table 1 forgotten, `CURRENT` switched to it, and
/// log segment 2 deleted. The evidence of the loss was laundered away. No leader
/// existed for the next 5.4 s, so no re-seed came; the schedule crashed server 3
/// at 13.32 s and restarted it at 13.53 s, the open found a self-consistent store,
/// removed `000001.sst` as an orphan and opened clean — `RaftRecovered { applied:
/// 185, last_index: 189 }`, no refusal and no install — and a voter with a hole in
/// its state machine rejoined and began pre-voting. State machine safety reported
/// the restatement whose log does not hold the index 1 it claimed to have applied.
/// A refusal is now recorded in the store's marker before anything else and
/// refuses every later open until an install replaces the store, and the refused
/// engine is quiesced (D-044).
///
/// Re-audited on the tree with D-056's send queue, which moved every schedule (see
/// seed 164's pin), the seed comes nearer that situation than it did, and the test
/// asserts what it reaches under each server and what it does not, with the reason.
///
/// Under the refusal as built the first half is there: server 1, crashed at 16.456 s
/// and restarted, drops table 28 (sequence numbers 575 to 639) at its open and is
/// refused for lost state at 16.504 s; `Fault::CrashRefused` crashes it 0.7 ms later
/// and restarts it at 16.524, 16.641, 16.758 and 16.875 s, before a leader's install
/// replaces its store (adopted at 17.285 s) — the restarts
/// [`raft::Report::restarts_after_lost_state_refusal`] names. The second half is not:
/// the refused engine flushed nothing in those 0.7 ms, and nothing on server 1's node
/// writes a manifest before the adoption, so the loss is never laundered, and every one
/// of those opens drops table 28 again and is refused, for the log's missing head
/// (record 720 where 640 was expected). No store opens clean over the hole, so state
/// machine safety has nothing to report, and the run passes.
///
/// Under the correct server the refused engine's quiesce is reached and holds: after
/// its crash at 19.024 s server 1 drops table 56 at its open, its engine is quiesced
/// at 19.096 s and the store refused for lost state at 19.101 s, and nothing on its
/// node flushes, writes a manifest, switches `CURRENT` or deletes a log segment, and
/// it does not restart, until the leader's install is adopted at 19.517 s; the run
/// passes. (Its crash at 16.505 s, 0.7 ms after the same quiesce over table 28, came
/// before the refusal was traced, and the open after it refused the store for the
/// missing head.) No restart follows a refusal for lost state before an install,
/// which is D-044's rule, and the test asserts that too.
///
/// Before D-056 the seed reached neither half: its one refusal was of a store damaged
/// before its engine opened, re-seeded before its next crash.
#[test]
fn seed_687_which_the_premerge_found_stays_green() {
    use ananke_env::NodeId;
    let lost_state = |report: &raft::Report| {
        report
            .records
            .iter()
            .find(|r| {
                matches!(&r.event, TraceEvent::RaftRefused { server: 1, reason }
                    if reason.starts_with(LOST_STATE))
            })
            .map(|r| r.at)
    };
    let adopted_after = |report: &raft::Report, at: ananke_env::Instant| {
        report
            .records
            .iter()
            .find(|r| r.at > at && matches!(r.event, TraceEvent::RaftAdopted { server: 1 }))
            .map(|r| r.at)
    };
    // Whether server 1's node flushed, wrote a manifest, switched `CURRENT`, deleted a
    // log segment or restarted in `(from, until)`.
    let worked = |report: &raft::Report, from, until| {
        report.records.iter().any(|r| {
            r.at > from
                && r.at < until
                && r.node == Some(NodeId::new(1))
                && matches!(
                    r.event,
                    TraceEvent::MemtableFlushed { .. }
                        | TraceEvent::ManifestWritten { .. }
                        | TraceEvent::CurrentSwitched { .. }
                        | TraceEvent::WalSegmentDeleted { .. }
                        | TraceEvent::NodeRestarted { .. }
                )
        })
    };

    let built = raft::run(687, Variant::RefusalNotDurable);
    let refused = lost_state(&built)
        .expect("seed 687 as built no longer refuses server 1 for lost state: re-audit the pin");
    let adopted = adopted_after(&built, refused)
        .expect("seed 687 as built never re-seeds server 1 after its refusal: re-audit the pin");
    let restarts = built.restarts_after_lost_state_refusal();
    assert!(
        !restarts.is_empty()
            && restarts
                .iter()
                .all(|&(server, at, _)| server == 1 && at == refused),
        "seed 687 as built no longer restarts server 1 after its refusal for lost state at \
         {refused:?} before an install: {restarts:?}; re-audit the pin"
    );
    let laundered = built.records.iter().any(|r| {
        r.at > refused
            && r.at < adopted
            && r.node == Some(NodeId::new(1))
            && matches!(r.event, TraceEvent::ManifestWritten { .. })
    });
    let opened_clean = built.records.iter().any(|r| {
        r.at > refused
            && r.at < adopted
            && matches!(r.event, TraceEvent::RaftRecovered { server: 1, .. })
    });
    assert!(
        !laundered && !opened_clean,
        "seed 687 as built now writes a manifest over the loss or opens server 1's store between \
         its refusal at {refused:?} and its re-seed at {adopted:?}: the laundered store is \
         reachable, so pin the mechanism — state machine safety reporting the restatement"
    );
    assert_eq!(
        built.check().err(),
        None,
        "seed 687 as built no longer passes: re-audit the pin"
    );

    let correct = raft::run(687, Variant::Correct);
    assert!(
        correct.restarts_after_lost_state_refusal().is_empty(),
        "seed 687 under the correct server restarts a server refused for lost state before an \
         install replaced its store: {:?}",
        correct.restarts_after_lost_state_refusal()
    );
    let refused = lost_state(&correct).expect(
        "seed 687 under the correct server no longer refuses server 1 for lost state: the \
         refused engine's quiesce is not reached here any more; re-audit the pin",
    );
    let quiesced = correct.records.iter().rev().find(|r| {
        r.at <= refused
            && r.node == Some(NodeId::new(1))
            && matches!(r.event, TraceEvent::EngineQuiesced { .. })
    });
    let quiesced = quiesced
        .map(|r| r.at)
        .expect("seed 687's correct refusal of server 1 is not preceded by its engine's quiesce");
    let adopted = adopted_after(&correct, refused)
        .expect("seed 687 under the correct server never re-seeds server 1: re-audit the pin");
    assert!(
        !worked(&correct, quiesced, adopted),
        "seed 687: server 1's quiesced engine did work, or the node restarted, between the \
         quiesce at {quiesced:?} and the re-seed's adoption at {adopted:?}"
    );
    assert_eq!(
        correct.check().err(),
        None,
        "seed 687 under the correct server no longer passes: re-audit the pin"
    );
}

/// Seed 1885 of the ten-thousand-seed nightly (run 34711427220, on 14c3e17): the
/// correct server failed with *pre-vote: server 1 raised its term from 8 to 10
/// while isolated from 15.203 s to 17.112 s*. The protocol held. Server 2's
/// RequestVote of term 10 was delivered to server 1 at 15.200469 s, and server 1's
/// step took it there — adopting term 10 and granting the vote — 2.53 ms before a
/// `Fault::RetakeUnderStream` partition cut it off at 15.203 s. The step's persist
/// became durable 48 µs after the partition, and because a node traces a step's
/// events only once they are durable (D-026), the `RaftTerm` record landed inside
/// the window. No message from a server reached server 1 until the heal. The check
/// read the record's time as the moment of the rise.
///
/// Every record now carries both times (D-047): `at`, when it was traced
/// and so when what it reports was durable, and `decided`, when the step behind it
/// was taken. The pre-vote check is about why a term moved, so it reads the
/// decision time. The test asserts the mechanism, not just green: the rise straddles
/// the isolation's start — decided before it, traced inside it — with no delivery to
/// the server in the window; the check by durability time still fails with the
/// nightly's message word for word; the same check by decision time passes; and so
/// does the whole check. The schedule was the nightly's until D-056.
///
/// D-056's send queue moved the schedule (see seed 164's pin), and the seed no longer
/// reaches the straddle, which the test asserts ([`assert_straddle_gone`]): the
/// partition aimed by `Fault::RetakeUnderStream` no longer isolates server 1 at
/// 15.203 s, none of the run's seven isolations begins then, server 1 is in term 13
/// by that time, and no term change of any server straddles any isolation's start.
/// The run passes. The directed term-raise schedule still reaches D-047's straddle,
/// asserted on its seed 4 below, and every sweep asserts every catch decision time
/// removes (D-051).
#[test]
fn seed_1885_which_the_nightly_failed_on_a_trace_timestamp_no_longer_straddles_an_isolation() {
    let report = raft::run(1885, Variant::Correct);
    assert_straddle_gone(
        &report,
        "pre-vote: server 1 raised its term from 8 to 10 while isolated from Instant(15.203s) to Instant(17.112s)",
        false,
    );
    report.check().unwrap();
}

/// Seed 2023 of the same nightly, the same gap: the correct server failed with
/// *pre-vote: server 1 raised its term from 13 to 14 while isolated from 19.22 s to
/// 20.822 s*. Server 3's AppendEntries of term 14 was delivered to server 1 and
/// stepped at 19.217879 s, 2.12 ms before the partition; the adopted term was
/// durable and traced 671 µs inside the window, and nothing from a server reached
/// server 1 until the heal. The test asserted what seed 1885's did (D-047).
///
/// D-056's send queue moved the schedule (see seed 164's pin): none of the run's seven
/// isolations begins at 19.22 s, server 1's rise from 13 to 14 comes at 19.776 s with
/// no isolation around it, and no term change straddles any isolation's start. The
/// test asserts the absence, as seed 1885's does, and the run passes.
#[test]
fn seed_2023_which_the_nightly_failed_on_a_trace_timestamp_no_longer_straddles_an_isolation() {
    let report = raft::run(2023, Variant::Correct);
    assert_straddle_gone(
        &report,
        "pre-vote: server 1 raised its term from 13 to 14 while isolated from Instant(19.22s) to Instant(20.822s)",
        false,
    );
    report.check().unwrap();
}

/// The nightly's eleven variant catches of the same gap (run 34711427220): four of
/// `IgnoreIncarnation`'s and seven of `SharedSnapshotDir`'s, every one the pre-vote
/// check reading a trace timestamp and none either bug. Ten are a term adopted from
/// a RequestVote or an AppendEntries whose step came before the isolation and whose
/// record came after it; seed 5203's is a candidacy, decided on a granting
/// PreVoteResponse before the isolation and traced, with its persisted term and
/// vote, inside it. No message from a server reached the isolated server inside the
/// window on any of them. Each is asserted for that reason (D-047): the
/// straddle, no delivery in the window, the durability-time check failing with the
/// nightly's message, and the decision-time check passing. None of the eleven now
/// reports a pre-vote violation; each run's verdict is printed.
///
/// None of the eleven reaches the straddle on the tree with D-056's send queue, which
/// moved every schedule (see seed 164's pin), and the pin asserts its absence with the
/// reason ([`assert_straddle_gone`]): on no run does a term change straddle an
/// isolation's start, and every run passes. The isolation the nightly named still
/// comes on two of them, on the same server at the same instants — seed 5203's server
/// 2 from 12.369 s and seed 6691's server 1 from 17.298 s — and the server keeps its
/// term through it, only pre-voting; on the other nine the leader-relative fault that
/// made it lands elsewhere. The day a straddle returns on any, the absence assertion
/// fails and the pin can be upgraded back to [`assert_rise_straddles_the_isolation`].
///
/// Two of the eleven, `IgnoreIncarnation`'s seeds 2509 and 5990, had already moved
/// away under D-049: the leader under D-042's bug stepped down on check quorum —
/// seed 2509's leader 2 of term 12 at 14.547925798 s leaving server 3 uncounted, seed
/// 5990's leader 3 of term 11 at 13.944738313 s leaving server 2 — before the
/// isolation the straddle was at. On this tree neither run has a step-down that
/// leaves a follower uncounted at all.
#[test]
fn the_nightlys_eleven_variant_catches_of_the_trace_timestamp_gap_are_not_catches() {
    type Pair = (u64, Variant, (u64, u64, u64, &'static str), &'static str);
    let pairs: [Pair; 11] = [
        (
            1252,
            Variant::IgnoreIncarnation,
            (3, 10, 11, "follower"),
            "pre-vote: server 3 raised its term from 10 to 11 while isolated from Instant(20.16625s) to Instant(22.33725s)",
        ),
        (
            2509,
            Variant::IgnoreIncarnation,
            (1, 12, 13, "follower"),
            "pre-vote: server 1 raised its term from 12 to 13 while isolated from Instant(14.859s) to Instant(16.412s)",
        ),
        (
            3087,
            Variant::IgnoreIncarnation,
            (3, 10, 11, "follower"),
            "pre-vote: server 3 raised its term from 10 to 11 while isolated from Instant(17.701s) to Instant(20.034s)",
        ),
        (
            5990,
            Variant::IgnoreIncarnation,
            (1, 11, 12, "follower"),
            "pre-vote: server 1 raised its term from 11 to 12 while isolated from Instant(14.448s) to Instant(16.741s)",
        ),
        (
            1176,
            Variant::SharedSnapshotDir,
            (3, 14, 15, "follower"),
            "pre-vote: server 3 raised its term from 14 to 15 while isolated from Instant(18.29s) to Instant(20.536s)",
        ),
        (
            2407,
            Variant::SharedSnapshotDir,
            (3, 10, 11, "follower"),
            "pre-vote: server 3 raised its term from 10 to 11 while isolated from Instant(15.314s) to Instant(17.394s)",
        ),
        (
            3863,
            Variant::SharedSnapshotDir,
            (2, 10, 11, "follower"),
            "pre-vote: server 2 raised its term from 10 to 11 while isolated from Instant(18.222s) to Instant(20.706s)",
        ),
        (
            4713,
            Variant::SharedSnapshotDir,
            (3, 13, 14, "follower"),
            "pre-vote: server 3 raised its term from 13 to 14 while isolated from Instant(18.797s) to Instant(20.433s)",
        ),
        (
            5203,
            Variant::SharedSnapshotDir,
            (2, 11, 12, "candidate"),
            "pre-vote: server 2 raised its term from 11 to 12 while isolated from Instant(12.369s) to Instant(13.319s)",
        ),
        (
            6691,
            Variant::SharedSnapshotDir,
            (1, 13, 14, "follower"),
            "pre-vote: server 1 raised its term from 13 to 14 while isolated from Instant(17.298s) to Instant(18.322s)",
        ),
        (
            9670,
            Variant::SharedSnapshotDir,
            (3, 12, 13, "follower"),
            "pre-vote: server 3 raised its term from 12 to 13 while isolated from Instant(26.809s) to Instant(29.254s)",
        ),
    ];
    // D-056: the two whose named isolation still comes, on the same server at the
    // same instants.
    const KEPT: [u64; 2] = [5203, 6691];
    let verdicts = sweep(pairs.len() as u64, |i| {
        let (seed, variant, _, original) = pairs[usize::try_from(i).expect("small")];
        let report = raft::run(seed, variant);
        assert_straddle_gone(&report, original, KEPT.contains(&seed));
        assert!(
            !report.has(
                |e| matches!(e, TraceEvent::RaftQuorumLost { uncounted, .. } if !uncounted.is_empty())
            ),
            "seed {seed} under {variant:?} steps a leader down leaving a follower uncounted again, \
             as D-049 recorded on 2509 and 5990: re-audit the pin"
        );
        (seed, variant, report.check().err())
    });
    for (seed, variant, verdict) in &verdicts {
        eprintln!("seed {seed} under {variant:?}: {verdict:?}");
        assert_eq!(
            verdict, &None,
            "seed {seed} under {variant:?}: the run no longer passes the check outright"
        );
    }
}

/// Issue #33: the sweeps' assertions on a removed catch, run on every catch the
/// ten-thousand-seed nightlies removed. Runs 34749071877 (on 9b5995d) and
/// 34852980174 (on a8656e8) printed 28 removed catches between them — 27 pre-vote
/// catches and one timer catch, `ResetTimerOnAnyRpc` on seed 5153 — each with the
/// words the check by durability time used, which are the words here. Each pair is
/// run on this tree through [`checked`], whose assertions are the ones every sweep
/// makes: a removed pre-vote catch matched to a term change straddling the start
/// of the isolation the catch names (D-047's, or D-050's received before it), and
/// a removed timer catch matched to a reset of the flagged server that the replay
/// counts, decided by the flag and traced after it.
///
/// On the tree with D-056's send queue, which moved every schedule (see seed 164's
/// pin), none of the 28 runs has its catch to remove, which is asserted, so the day
/// one reaches it again this test runs the assertion on it; until then each run still
/// goes through [`checked`], so a catch decision time removes on any of them meets the
/// sweeps' assertions, and no catch may be added. On six of them the isolation the
/// catch named still comes, on the same server at the same instants (seeds 5203, 6691,
/// 5051, 5879, 6717 and 2578), and the server keeps its term through it; on the other
/// 21 pre-vote catches the leader-relative fault lands elsewhere (on three, 4814, 5918
/// and 6366, at the same instant on another server). Seed 5153's timer gap is gone with
/// its schedule: the timer replay by durability time finds no gap on the run, which is
/// asserted. Every run passes the check but one: seed 6717 under `ResetTimerOnAnyRpc`
/// is caught by the timer check, server 3 having heard from no leader of its term since
/// 4.465 s and not campaigned by 4.866 s — the variant's own bug, a timer reset by a
/// message that is no leader's contact, caught on a schedule D-056 moved, and not a
/// catch decision time removes, as `checked` asserts. That catch is asserted too.
/// (Before D-056, 26 of the 28 were removed here in the nightlies' words and passed;
/// `IgnoreIncarnation` on seeds 2509 and 5990 had moved under D-049.)
#[test]
// PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
// names.
fn the_nightlies_removed_catches_meet_the_sweeps_assertions() {
    type Removed = (u64, Variant, &'static str);
    const REMOVED: [Removed; 28] = [
        (
            1885,
            Variant::Correct,
            "pre-vote: server 1 raised its term from 8 to 10 while isolated from Instant(15.203s) to Instant(17.112s)",
        ),
        (
            2023,
            Variant::Correct,
            "pre-vote: server 1 raised its term from 13 to 14 while isolated from Instant(19.22s) to Instant(20.822s)",
        ),
        (
            1252,
            Variant::IgnoreIncarnation,
            "pre-vote: server 3 raised its term from 10 to 11 while isolated from Instant(20.16625s) to Instant(22.33725s)",
        ),
        (
            2509,
            Variant::IgnoreIncarnation,
            "pre-vote: server 1 raised its term from 12 to 13 while isolated from Instant(14.859s) to Instant(16.412s)",
        ),
        (
            3087,
            Variant::IgnoreIncarnation,
            "pre-vote: server 3 raised its term from 10 to 11 while isolated from Instant(17.701s) to Instant(20.034s)",
        ),
        (
            5990,
            Variant::IgnoreIncarnation,
            "pre-vote: server 1 raised its term from 11 to 12 while isolated from Instant(14.448s) to Instant(16.741s)",
        ),
        (
            1176,
            Variant::SharedSnapshotDir,
            "pre-vote: server 3 raised its term from 14 to 15 while isolated from Instant(18.29s) to Instant(20.536s)",
        ),
        (
            2407,
            Variant::SharedSnapshotDir,
            "pre-vote: server 3 raised its term from 10 to 11 while isolated from Instant(15.314s) to Instant(17.394s)",
        ),
        (
            3863,
            Variant::SharedSnapshotDir,
            "pre-vote: server 2 raised its term from 10 to 11 while isolated from Instant(18.222s) to Instant(20.706s)",
        ),
        (
            4713,
            Variant::SharedSnapshotDir,
            "pre-vote: server 3 raised its term from 13 to 14 while isolated from Instant(18.797s) to Instant(20.433s)",
        ),
        (
            5203,
            Variant::SharedSnapshotDir,
            "pre-vote: server 2 raised its term from 11 to 12 while isolated from Instant(12.369s) to Instant(13.319s)",
        ),
        (
            6691,
            Variant::SharedSnapshotDir,
            "pre-vote: server 1 raised its term from 13 to 14 while isolated from Instant(17.298s) to Instant(18.322s)",
        ),
        (
            9670,
            Variant::SharedSnapshotDir,
            "pre-vote: server 3 raised its term from 12 to 13 while isolated from Instant(26.809s) to Instant(29.254s)",
        ),
        (
            2627,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 3 raised its term from 7 to 8 while isolated from Instant(14.425s) to Instant(16.654s)",
        ),
        (
            4426,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 1 raised its term from 11 to 12 while isolated from Instant(23.3235s) to Instant(25.1915s)",
        ),
        (
            4814,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 3 raised its term from 11 to 15 while isolated from Instant(12.111s) to Instant(13.177s)",
        ),
        (
            5051,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 3 raised its term from 5 to 6 while isolated from Instant(9.058s) to Instant(10.103s)",
        ),
        (
            5153,
            Variant::ResetTimerOnAnyRpc,
            "timers: server 2 heard from no leader of its term and granted no vote since Instant(7.864918384s) and had not campaigned by Instant(8.265321611s)",
        ),
        (
            5879,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 2 raised its term from 11 to 12 while isolated from Instant(13.358s) to Instant(14.312s)",
        ),
        (
            5918,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 3 raised its term from 7 to 8 while isolated from Instant(9.532s) to Instant(10.475s)",
        ),
        (
            6717,
            Variant::ResetTimerOnAnyRpc,
            "pre-vote: server 3 raised its term from 7 to 9 while isolated from Instant(9.057s) to Instant(10.127s)",
        ),
        (
            1929,
            Variant::AdoptionAsBuilt,
            "pre-vote: server 1 raised its term from 12 to 13 while isolated from Instant(17.879s) to Instant(20.093s)",
        ),
        (
            2578,
            Variant::AdoptionAsBuilt,
            "pre-vote: server 2 raised its term from 10 to 11 while isolated from Instant(8.024s) to Instant(9.106s)",
        ),
        (
            2698,
            Variant::AdoptionAsBuilt,
            "pre-vote: server 2 raised its term from 8 to 9 while isolated from Instant(13.667s) to Instant(15.228s)",
        ),
        (
            5859,
            Variant::AdoptionAsBuilt,
            "pre-vote: server 2 raised its term from 10 to 11 while isolated from Instant(16.983s) to Instant(19.363s)",
        ),
        (
            9557,
            Variant::AdoptionAsBuilt,
            "pre-vote: server 3 raised its term from 9 to 10 while isolated from Instant(15.0055s) to Instant(17.3035s)",
        ),
        (
            2305,
            Variant::SnapshotWithoutCurrentLast,
            "pre-vote: server 2 raised its term from 7 to 8 while isolated from Instant(19.377s) to Instant(21.605s)",
        ),
        (
            6366,
            Variant::ApplyBeforeCommit,
            "pre-vote: server 2 raised its term from 11 to 13 while isolated from Instant(9.677s) to Instant(10.784s)",
        ),
    ];
    // D-056 moved every schedule away from its catch; these six keep the isolation
    // the catch named.
    const KEPT: [u64; 6] = [5203, 6691, 5051, 5879, 6717, 2578];
    let runs = sweep(REMOVED.len() as u64, |i| {
        let (seed, variant, was) = REMOVED[usize::try_from(i).expect("small")];
        let report = raft::run(seed, variant);
        if was.starts_with("pre-vote: ") {
            assert_straddle_gone(&report, was, KEPT.contains(&seed));
        }
        let moved = Mutex::new(MovedSeeds::default());
        let verdict = checked(&report, &moved);
        let moved = moved.into_inner().unwrap();
        // D-056: seed 5153's timer gap, asserted gone by the replay by durability time.
        let durable_gaps = report
            .timer_gaps_by(TimerResets::ALL, RecordTime::Durable)
            .len();
        (
            seed,
            variant,
            was,
            verdict,
            moved.removed,
            moved.added,
            durable_gaps,
        )
    });
    for (seed, variant, was, verdict, removed, added, durable_gaps) in &runs {
        for (_, line) in removed {
            eprintln!("seed {seed} under {variant:?}: removed: {line}");
        }
        assert_eq!(
            added,
            &Vec::new(),
            "seed {seed} under {variant:?}: a catch was added"
        );
        let removes_it = removed.iter().any(|(_, line)| line.starts_with(was));
        assert!(
            !removes_it,
            "seed {seed} under {variant:?} removes the nightly's catch again: upgrade the test to \
             assert it removed, and the run passing"
        );
        eprintln!("seed {seed} under {variant:?}: {verdict:?}");
        if *seed == 5153 {
            assert_eq!(
                *durable_gaps, 0,
                "seed 5153 under {variant:?}: the timer replay by durability time finds a gap \
                 again: re-audit the pin"
            );
        }
        if (*seed, *variant) == (6717, Variant::ResetTimerOnAnyRpc) {
            assert!(
                verdict.as_ref().is_some_and(|v| v.starts_with(
                    "seed 6717: timers: server 3 heard from no leader of its term and granted no vote"
                )),
                "seed 6717 under {variant:?} is no longer caught by the timer check on server 3: \
                 {verdict:?}; re-audit the pin"
            );
        } else {
            assert_eq!(
                verdict, &None,
                "seed {seed} under {variant:?}: the run no longer passes the check"
            );
        }
    }
}

/// A pin of the trace-timestamp gap (D-047) whose seed D-056's send queue moved away
/// from it, asserted absent with its reason: no term change on the run straddles the
/// start of any isolation, whether decided before it and traced inside it or received
/// before it and decided inside it, so the pre-vote check by durability time finds
/// nothing either and decision time has nothing to remove. `original` is the catch the
/// pin was made for; when `kept`, the isolation it names still comes, on the same
/// server at the same instants, and the server keeps its term through it, and
/// otherwise that server is not isolated from that instant. The day a
/// straddle returns this fails, and the pin can be upgraded back to
/// [`assert_rise_straddles_the_isolation`].
// PROPOSED(D-056): the pinned seeds re-audited on the moved schedules.
fn assert_straddle_gone(report: &raft::Report, original: &str, kept: bool) {
    let (seed, variants) = (report.seed, report.variants);
    let instant = |after: &str| {
        let text = original
            .split(after)
            .nth(1)
            .and_then(|rest| rest.split("s)").next())
            .expect("the catch names its isolation");
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        let nanos = format!("{fraction:0<9}");
        ananke_env::Instant::from_nanos(
            whole.parse::<u64>().expect("whole seconds") * 1_000_000_000
                + nanos.parse::<u64>().expect("nanoseconds"),
        )
    };
    let server: u64 = original
        .split("server ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|id| id.parse().ok())
        .expect("the catch names its server");
    let (from, until) = (instant("from Instant("), instant("to Instant("));
    assert_eq!(
        (
            report.isolation_term_straddles(),
            report.isolation_received_straddles()
        ),
        (Vec::new(), Vec::new()),
        "seed {seed} under {variants:?} straddles an isolation again: upgrade the pin for \
         `{original}`"
    );
    assert_eq!(
        report.isolation_keeps_the_term_by(RecordTime::Durable),
        Ok(()),
        "seed {seed} under {variants:?}: the pre-vote check by durability time flags a window \
         again: re-audit the pin for `{original}`"
    );
    if kept {
        assert!(
            report.isolations.contains(&(server, from, until)),
            "seed {seed} under {variants:?} no longer isolates server {server} from {from:?} to \
             {until:?}: re-audit the pin"
        );
    } else {
        assert!(
            !report
                .isolations
                .iter()
                .any(|&(s, f, _)| s == server && f == from),
            "seed {seed} under {variants:?} isolates server {server} from {from:?} again: \
             re-audit the pin for `{original}`"
        );
    }
}

/// The trace-timestamp gap on one run (D-047): the run holds `count` term rises
/// that straddle the start of their server's isolation, each with no message from a
/// server delivered to that server in the window, and the first — `rise` names its
/// (server, term before, term after, role) — decided before the isolation began and
/// traced inside it, with no message from a server delivered to that server in the
/// window; the pre-vote check by durability time fails with `original`, the
/// nightly's message, and the same check by decision time passes. Returns the
/// straddle, so a pin can tie its decision time to the message the step took.
fn assert_rise_straddles_the_isolation(
    report: &raft::Report,
    count: usize,
    rise: (u64, u64, u64, &str),
    original: &str,
) -> raft::TermStraddle {
    let (seed, variants) = (report.seed, report.variants);
    let straddles = report.isolation_term_straddles();
    let [straddle, ..] = straddles.as_slice() else {
        panic!(
            "seed {seed} under {variants:?} no longer has a term rise straddling an isolation's \
             start: re-audit the pin"
        );
    };
    assert_eq!(
        straddles.len(),
        count,
        "seed {seed} under {variants:?} no longer has {count} term rises straddling an \
         isolation's start: {straddles:?}; re-audit the pin"
    );
    for other in &straddles {
        assert!(
            other.decided < other.from
                && other.from <= other.at
                && other.at <= other.until
                && other.deliveries == 0,
            "seed {seed} under {variants:?}: a straddle is not decided before its isolation and \
             traced inside it with nothing delivered in the window: {other:?}"
        );
    }
    assert_eq!(
        (
            straddle.server,
            straddle.before,
            straddle.term,
            straddle.role
        ),
        rise,
        "seed {seed} under {variants:?} straddles with another rise: {straddle:?}"
    );
    assert!(
        straddle.decided < straddle.from
            && straddle.from <= straddle.at
            && straddle.at <= straddle.until,
        "seed {seed} under {variants:?}: the rise does not straddle the isolation: {straddle:?}"
    );
    assert_eq!(
        straddle.deliveries, 0,
        "seed {seed} under {variants:?}: a server's message reached the isolated server in the \
         window, so the rise may be the protocol's: {straddle:?}"
    );
    eprintln!(
        "seed {seed} under {variants:?}: server {} term {} -> {} ({}) decided {:?} before the \
         isolation at {:?}, traced {:?} after it",
        straddle.server,
        straddle.before,
        straddle.term,
        straddle.role,
        straddle.from.duration_since(straddle.decided),
        straddle.from,
        straddle.at.duration_since(straddle.from)
    );
    assert_eq!(
        report.isolation_keeps_the_term_by(RecordTime::Durable),
        Err(original.to_owned()),
        "seed {seed} under {variants:?}: the pre-vote check by durability time no longer fails \
         the way the nightly did"
    );
    assert_eq!(
        report.isolation_keeps_the_term_by(RecordTime::Decided),
        Ok(()),
        "seed {seed} under {variants:?}: the pre-vote check by decision time fails"
    );
    assert_eq!(
        report.moved_by_decision_time(&report.check()),
        Some(Moved::Lost(original.to_owned())),
        "seed {seed} under {variants:?}: the sweeps' report of what decision time moved does not \
         name this run as the catch it removed"
    );
    straddle.clone()
}

/// How many rounds the directed scenario of issue #32 runs per seed (D-050).
const TERM_RAISE_TRIES: u64 = 8;

/// Issue #32, the open case of D-047, aimed at (D-050): a message raising a
/// server's term delivered before an isolation begins and taken by a step inside
/// it, because the server was still awaiting a persist when the message arrived.
/// No correct-server seed of the ten-thousand-seed nightlies reached it, so the
/// directed scenario builds it: each round a transfer's campaign sends the third
/// server a RequestVote of a higher term, and that server is cut off the moment the
/// message is delivered ([`Fault::IsolateOnTermRaise`]).
///
/// The check by decision time flags such a change, since its step is inside the
/// window; the check [`raft::Report::check`] makes reads the receipt the server
/// records on it and excuses it. On every seed the correct server passes the whole
/// check, and every such change is asserted for its reason: received at or before
/// the isolation's start and decided inside the window, no message from a server
/// delivered to the server in the window, a message of the new term delivered to it
/// at the receipt, and the check by decision time flagging an isolation the check by
/// cause does not. The shape must be reached on some seed at every tier, and the
/// count is printed.
#[test]
fn a_term_change_stepped_inside_an_isolation_from_a_message_received_before_it_is_excused() {
    // PROPOSED(D-051): on this schedule most runs hold a catch reading decision
    // time removes, so each run goes through the sweeps' `checked` and its
    // assertions meet hundreds of real removals at every tier.
    let moved = Mutex::new(MovedSeeds::default());
    let per_seed = sweep(seeds(), |seed| {
        let report = raft::run_with(
            seed,
            raft::Schedule::term_raise_behind_a_step(TERM_RAISE_TRIES),
            Variant::Correct,
        );
        let received = report.isolation_received_straddles();
        for s in &received {
            assert_received_straddle(&report, s);
        }
        let verdict = checked(&report, &moved).map_or(Ok(()), Err);
        if verdict.is_err() {
            write_trace(&format!("raft-term-raise-{seed}"), &report.jsonl());
        }
        let first = received.first().map(|s| {
            format!(
                "seed {seed}: server {} {}->{} ({}) received {:?}, isolated from {:?} to {:?}, \
                 decided {:?} into it, traced at {:?}, at the receipt {:?}",
                s.server,
                s.before,
                s.term,
                s.role,
                s.received,
                s.from,
                s.until,
                s.decided.duration_since(s.from),
                s.at,
                s.causes
            )
        });
        (
            first,
            received.len(),
            report.isolations.len(),
            report.isolation_term_straddles().len(),
            verdict,
        )
    });
    let reached = per_seed.iter().filter(|(_, n, ..)| *n > 0).count();
    let changes: usize = per_seed.iter().map(|(_, n, ..)| n).sum();
    let isolations: usize = per_seed.iter().map(|(_, _, i, ..)| i).sum();
    let decided: usize = per_seed.iter().map(|(_, _, _, d, _)| d).sum();
    eprintln!(
        "term raised behind a step (D-050): the shape on {reached} of {} seeds, {changes} \
         changes in {isolations} isolations; {decided} rises decided before an isolation and \
         traced inside it (D-047)",
        seeds()
    );
    for line in per_seed
        .iter()
        .filter_map(|(first, ..)| first.as_ref())
        .take(10)
    {
        eprintln!("  {line}");
    }
    print_moved("Correct on the term-raise schedule", moved);
    let failures: Vec<&String> = per_seed
        .iter()
        .filter_map(|(.., verdict)| verdict.as_ref().err())
        .collect();
    assert!(
        failures.is_empty(),
        "the correct server fails: {failures:?}"
    );
    assert!(
        reached > 0,
        "no seed reached a term change stepped inside an isolation from a message received \
         before it"
    );
}

/// Seed 1 of the directed schedule pins the shape with its numbers (D-050).
/// Server 1's campaign for term 4 sent server 2 a RequestVote of term 4, delivered
/// and received at 2.041294077 s. The isolation began 5.923 µs later, at the end of
/// the watch's slice, 2.0413 s, and server 2's step took the message 144.397 µs into
/// it: a change from term 3 to 4 decided inside the window, with no message from a
/// server reaching server 2 until the heal at 2.3413 s. The check by decision time
/// flags the isolation in the words below; the check by cause excuses it, since the
/// record says the message was received before the window; and the run passes the
/// whole check. When a change to the simulator or the server moves the seed away from
/// the shape, this fails and names what it found instead.
///
/// Seed 4 pinned the shape until D-056's send queue moved every schedule (see seed
/// 164's pin); seed 1 is the lowest seed of the schedule that reaches it on the tree
/// with the queue, and seed 4's own test below asserts what it reaches instead.
#[test]
fn seed_1_of_the_term_raise_schedule_steps_a_message_received_before_its_isolation() {
    let report = raft::run_with(
        1,
        raft::Schedule::term_raise_behind_a_step(TERM_RAISE_TRIES),
        Variant::Correct,
    );
    let received = report.isolation_received_straddles();
    let [s] = received.as_slice() else {
        panic!(
            "seed 1 no longer has exactly one term change received before an isolation and \
             stepped inside it: {received:?}"
        );
    };
    assert_eq!(
        (s.server, s.before, s.term, s.role, s.causes.as_slice()),
        (2, 3, 4, "follower", &[(1, "request-vote", 4)][..]),
        "seed 1 steps another change: {s:?}"
    );
    assert_eq!(
        (s.received, s.from, s.decided, s.until),
        (
            ananke_env::Instant::from_nanos(2_041_294_077),
            ananke_env::Instant::from_nanos(2_041_300_000),
            ananke_env::Instant::from_nanos(2_041_444_397),
            ananke_env::Instant::from_nanos(2_341_300_000),
        ),
        "seed 1's receipt, isolation or step moved: {s:?}"
    );
    assert_received_straddle(&report, s);
    assert_eq!(
        report.isolation_keeps_its_term_by(RecordTime::Decided, s.server, s.from, s.until),
        Err(
            "pre-vote: server 2 raised its term from 3 to 4 while isolated from Instant(2.0413s) \
             to Instant(2.3413s)"
                .to_owned()
        )
    );
    report.check().unwrap();
}

/// Seed 4 of the directed schedule, which pinned D-050's shape above until D-056's
/// send queue moved every schedule (see seed 164's pin). On the tree with the queue
/// it does not reach that shape, which the test asserts: no term change on the run
/// was received before an isolation and stepped inside it. The reason is the other
/// side of the same boundary: in seven of its eight isolations, cut as a RequestVote of
/// a higher term reaches a server, the server's step takes the message before the
/// watch's slice ends and the isolation begins, and only the step's persist, and so
/// its record, falls inside the window — D-047's straddle, which the nightly's seeds
/// 1885 and 2023 were pinned for before D-056 moved them away. So the seed now pins
/// D-047's mechanism on the directed schedule, asserted as those seeds' pins asserted
/// it: seven rises decided before their isolations' starts and traced inside them,
/// none with a server's message delivered in the window; the first, server 2's from
/// term 1 to 2, decided 4.756 µs before its isolation at 1.22268 s at the delivery of
/// server 1's RequestVote of term 2, and traced 1.761 ms into it; the check by
/// durability time failing with that window's words; the check by decision time
/// passing; and the whole check green.
#[test]
fn seed_4_of_the_term_raise_schedule_now_straddles_its_isolations_by_decision_time() {
    let report = raft::run_with(
        4,
        raft::Schedule::term_raise_behind_a_step(TERM_RAISE_TRIES),
        Variant::Correct,
    );
    assert_eq!(
        report.isolation_received_straddles(),
        Vec::new(),
        "seed 4 steps a message received before its isolation inside it again: it reaches \
         D-050's shape once more; re-audit the pins of seeds 1 and 4"
    );
    let straddle = assert_rise_straddles_the_isolation(
        &report,
        7,
        (2, 1, 2, "follower"),
        "pre-vote: server 2 raised its term from 1 to 2 while isolated from Instant(1.22268s) to Instant(1.52268s)",
    );
    assert_eq!(
        straddle.causes,
        [(1, "request-vote", 2)],
        "seed 4: the rise was not decided at the delivery of server 1's RequestVote of term 2, \
         so its decision time is not that message's step: {straddle:?}"
    );
    report.check().unwrap();
}

/// The pair of the test above (D-050): the server without pre-vote, on the same
/// directed schedule, campaigns on its own timer while cut off, and a campaign on a
/// timer takes no peer's message, so its term change carries no receipt and the
/// check by cause must still flag it on some seed; a check that excused everything
/// would pass here. (A candidacy stepped from a granting PreVoteResponse or a
/// TimeoutNow does carry one, and is excused when that message was received by the
/// isolation's start, like any change a message caused.)
///
/// Every window the check by decision time flags whose term changes include one
/// received by the isolation's start and one that is not must be flagged by the
/// check by cause too, which is the "every" of D-050's excuse; the
/// count of such windows is printed. Each run also goes through the sweeps'
/// `checked`, so its removed catches meet D-051's assertions.
#[test]
fn a_server_without_pre_vote_is_caught_on_the_term_raise_schedule() {
    let moved = Mutex::new(MovedSeeds::default());
    let per_seed = sweep(seeds(), |seed| {
        let report = raft::run_with(
            seed,
            raft::Schedule::term_raise_behind_a_step(TERM_RAISE_TRIES),
            Variant::NoPreVote,
        );
        let _ = checked(&report, &moved);
        // PROPOSED(D-050): the excuse needs every change received by the start.
        let mut mixed = 0;
        for &(server, from, until) in &report.isolations {
            let changes = report.isolation_term_changes(server, from, until);
            let before = changes
                .iter()
                .filter(|r| r.received().is_some_and(|received| received <= from))
                .count();
            // Only a window the check by decision time flags consults the excuse;
            // one the pre-vote check skips, for an install inside it, does not.
            if before > 0
                && before < changes.len()
                && report
                    .isolation_keeps_its_term_by(RecordTime::Decided, server, from, until)
                    .is_err()
            {
                mixed += 1;
                assert!(
                    report
                        .isolation_keeps_its_term_by_cause(server, from, until)
                        .is_err(),
                    "seed {seed}: server {server}'s window from {from:?} to {until:?} has a change \
                     received before it beside one that was not, and is excused: {changes:?}"
                );
            }
        }
        (report.isolation_keeps_the_term_by_cause().is_err(), mixed)
    });
    let caught = per_seed.iter().filter(|(caught, _)| *caught).count();
    let mixed: usize = per_seed.iter().map(|(_, mixed)| mixed).sum();
    eprintln!(
        "NoPreVote on the term-raise schedule: caught by the pre-vote check on {caught} of {} \
         seeds; {mixed} windows mixed a change received before the isolation with one that was \
         not, each flagged",
        seeds()
    );
    print_moved("NoPreVote on the term-raise schedule", moved);
    assert!(
        caught > 0,
        "NoPreVote was never caught on the term-raise schedule"
    );
}

/// Asserts that `s`, a change of an isolated server's term decided inside the
/// isolation from a message received before it, is that for the reason D-050
/// gives: the receipt at or before the isolation's start and the step inside it;
/// no message from a server delivered to the server in the window, so nothing it
/// received while cut off raised the term; a message of the new term from a server
/// delivered to it at the instant it received one, so the receipt is that
/// message's; and the check by decision time flagging the isolation while the check
/// by cause does not.
fn assert_received_straddle(report: &raft::Report, s: &raft::ReceivedStraddle) {
    let (seed, variants) = (report.seed, report.variants);
    assert!(
        s.received <= s.from && s.from < s.decided && s.decided <= s.until,
        "seed {seed} under {variants:?}: not received before and decided inside: {s:?}"
    );
    assert_eq!(
        s.deliveries, 0,
        "seed {seed} under {variants:?}: a server's message reached the isolated server in the \
         window: {s:?}"
    );
    assert!(
        s.causes
            .iter()
            .any(|&(from, _, term)| from != s.server && term == s.term),
        "seed {seed} under {variants:?}: no message of term {} was delivered at the receipt: {s:?}",
        s.term
    );
    assert!(
        report.isolations.contains(&(s.server, s.from, s.until)),
        "seed {seed} under {variants:?}: not one of the run's isolations: {s:?}"
    );
    assert!(
        report
            .isolation_keeps_its_term_by(RecordTime::Decided, s.server, s.from, s.until)
            .is_err(),
        "seed {seed} under {variants:?}: the check by decision time does not flag the \
         isolation: {s:?}"
    );
    assert_eq!(
        report.isolation_keeps_its_term_by_cause(s.server, s.from, s.until),
        Ok(()),
        "seed {seed} under {variants:?}: the check by cause flags the isolation: {s:?}"
    );
}

/// The positive control: the correct server satisfies every property on every
/// seed, and the sweep reached the states that matter.
#[test]
fn the_correct_server_passes_every_seed() {
    let coverage = Mutex::new(Coverage::default());
    let episodes = Mutex::new((ReseedEpisodes::default(), EpisodeLengths::default()));
    let moved = Mutex::new(MovedSeeds::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::Correct);
        coverage.lock().unwrap().add(&report);
        {
            let mut episodes = episodes.lock().unwrap();
            let (counts, lengths) = &mut *episodes;
            counts.add(&report, lengths);
        }
        checked(&report, &moved).map_or(Ok(()), |violation| {
            write_trace(&format!("raft-{seed}"), &report.jsonl());
            Err(violation)
        })
    });
    let coverage = coverage.into_inner().unwrap();
    eprintln!("Correct: {coverage:?}");
    let (mut episodes, lengths) = episodes.into_inner().unwrap();
    episodes.finish(lengths);
    eprintln!("Correct: re-seed episodes (D-049): {episodes:?}");
    print_moved("Correct", moved);
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete();
}

/// The negative controls: each known bug is caught on some seed, and the rate is
/// reported. Takes a set, so a pair of bugs is asked the same question as one
/// (D-045); the sweep's own list is all single variants.
fn is_caught(variants: impl Into<Variants>) {
    let variants = variants.into();
    let moved = Mutex::new(MovedSeeds::default());
    let caught: Vec<String> = sweep(seeds(), |seed| checked(&raft::run(seed, variants), &moved))
        .into_iter()
        .flatten()
        .collect();
    eprintln!(
        "{variants:?}: caught on {} of {} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    print_moved(&format!("{variants:?}"), moved);
    assert!(!caught.is_empty(), "{variants:?} was never caught");
}

/// What reading records by decision time (D-047) moved over one sweep:
/// a line per seed whose catch it removed and per seed whose catch it added.
#[derive(Default)]
struct MovedSeeds {
    removed: Vec<(u64, String)>,
    added: Vec<(u64, String)>,
}

/// `report`'s violation, if any, with what D-047 moved on the run noted in
/// `moved`, and each removed catch asserted to have been removed for a reason the
/// entries give (issue #33), at every tier.
///
/// A removed pre-vote catch names an isolation — its server, `from` and `until` —
/// and that isolation must hold a term change of its server that straddles its
/// start: decided at or before it and traced inside it (D-047), or received at or
/// before it and decided inside it (D-050). A straddle on some other isolation of the
/// run is not the reason. A removed timer catch is the replay's first gap by
/// durability time, and [`raft::Report::timer_removal`] must give its reason, read
/// off the two replays at the gap's flag record: a reset of the server moved back
/// past it, the flag record itself moved back within the bound, or the server's
/// status there moved. Every removal has one of these, so anything else is a fault in
/// the reasoning and fails the sweep.
// PROPOSED(D-051): a removed catch is asserted against the isolation or the flag it
// names.
fn checked(report: &raft::Report, moved: &Mutex<MovedSeeds>) -> Option<String> {
    let (seed, variants) = (report.seed, report.variants);
    let verdict = report.check();
    match report.moved_by_decision_time(&verdict) {
        Some(Moved::Lost(was)) if was.starts_with("pre-vote: ") => {
            let Some((server, from, until)) = report.isolation_named_by(RecordTime::Durable, &was)
            else {
                panic!(
                    "seed {seed} under {variants:?}: decision time removed the catch `{was}`, but no \
                     isolation of the run is flagged in those words by durability time"
                );
            };
            let named = |s: u64, f: ananke_env::Instant, u: ananke_env::Instant| {
                (s, f, u) == (server, from, until)
            };
            let straddles: Vec<String> = report
                .isolation_term_straddles()
                .iter()
                .filter(|s| named(s.server, s.from, s.until))
                .map(|s| {
                    format!(
                        "server {} {}->{} ({}) decided {:?} before, traced {:?} after, {} in-window deliveries, at the decision {:?}",
                        s.server,
                        s.before,
                        s.term,
                        s.role,
                        s.from.duration_since(s.decided),
                        s.at.duration_since(s.from),
                        s.deliveries,
                        s.causes
                    )
                })
                // D-050: a term change received before the isolation and
                // stepped inside it is the other way the catch goes.
                .chain(
                    report
                        .isolation_received_straddles()
                        .iter()
                        .filter(|s| named(s.server, s.from, s.until))
                        .map(|s| {
                            format!(
                                "server {} {}->{} ({}) received {:?} before, decided {:?} after, {} in-window deliveries, at the receipt {:?}",
                                s.server,
                                s.before,
                                s.term,
                                s.role,
                                s.from.duration_since(s.received),
                                s.decided.duration_since(s.from),
                                s.deliveries,
                                s.causes
                            )
                        }),
                )
                .collect();
            assert!(
                !straddles.is_empty(),
                "seed {seed} under {variants:?}: decision time removed the catch `{was}`, but no \
                 term change of server {server} straddles the start of that isolation, from \
                 {from:?} to {until:?}"
            );
            moved
                .lock()
                .unwrap()
                .removed
                .push((seed, format!("{was} [{}]", straddles.join("; "))));
        }
        Some(Moved::Lost(was)) => {
            let gap = report
                .timer_gaps_by(TimerResets::ALL, RecordTime::Durable)
                .into_iter()
                .next();
            let Some(gap) = gap.filter(|gap| gap.violation() == was) else {
                panic!(
                    "seed {seed} under {variants:?}: decision time removed the catch `{was}`, which \
                     is not the timer replay's first gap by durability time"
                );
            };
            // PROPOSED(D-051): the reason is read off the two replays at the flag
            // record, and every removal has one.
            let reasons = report.timer_removal(&gap).unwrap_or_else(|why| {
                panic!(
                    "seed {seed} under {variants:?}: decision time removed the timer catch `{was}` \
                     for no reason the replays show: {why}"
                )
            });
            let reasons: Vec<String> = reasons
                .iter()
                .map(|reason| {
                    let (what, index) = match *reason {
                        raft::TimerRemoval::ResetMovedBack { reset } => ("reset moved back", reset),
                        raft::TimerRemoval::FlagMovedBack { flag } => ("flag moved back", flag),
                        raft::TimerRemoval::StatusMoved { record } => ("status moved", record),
                    };
                    let r = &report.records[index];
                    let event = format!("{:?}", r.event);
                    let name = event.split([' ', '{', '(']).next().unwrap_or("");
                    format!(
                        "{what}: {name} decided {:?} before the flag, traced {:?} after",
                        gap.at.duration_since(r.decided),
                        r.at.duration_since(gap.at)
                    )
                })
                .collect();
            moved
                .lock()
                .unwrap()
                .removed
                .push((seed, format!("{was} [{}]", reasons.join("; "))));
        }
        Some(Moved::Gained(now)) => moved.lock().unwrap().added.push((seed, now)),
        None => {}
    }
    verdict.err()
}

/// Prints what D-047 moved over a sweep, fifty lines of each at most.
fn print_moved(name: &str, moved: Mutex<MovedSeeds>) {
    let mut moved = moved.into_inner().unwrap();
    moved.removed.sort();
    moved.added.sort();
    eprintln!(
        "{name}: decision time (D-047) removed {} catches and added {}",
        moved.removed.len(),
        moved.added.len()
    );
    for (seed, line) in moved.removed.iter().take(50) {
        eprintln!("  removed: seed {seed}: {line}");
    }
    for (seed, line) in moved.added.iter().take(50) {
        eprintln!("  added: seed {seed}: {line}");
    }
}

/// The server without pre-vote campaigns on its own timer while it is cut off, so
/// its term rises are decided inside the isolation, and the pre-vote check, which
/// reads a rise by its decision time (D-047), must still see them. The
/// catches are counted by that check, not by whichever check a run failed first.
#[test]
fn a_server_without_pre_vote_is_caught() {
    let moved = Mutex::new(MovedSeeds::default());
    let caught: Vec<String> = sweep(seeds(), |seed| {
        checked(&raft::run(seed, Variant::NoPreVote), &moved)
    })
    .into_iter()
    .flatten()
    .collect();
    let by_pre_vote = caught.iter().filter(|v| v.contains(": pre-vote: ")).count();
    eprintln!(
        "{:?}: caught on {} of {} seeds, {by_pre_vote} by the pre-vote check, first: {}",
        Variants::from(Variant::NoPreVote),
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    print_moved("NoPreVote", moved);
    assert!(
        by_pre_vote > 0,
        "NoPreVote was never caught by the pre-vote check reading decision time"
    );
}

#[test]
fn a_server_that_sends_before_it_persists_is_caught() {
    is_caught(Variant::SendBeforePersist);
}

#[test]
fn a_server_that_applies_before_commit_is_caught() {
    is_caught(Variant::ApplyBeforeCommit);
}

#[test]
fn a_leader_that_commits_an_older_terms_entry_by_count_is_caught() {
    is_caught(Variant::CountOlderTermForCommit);
}

#[test]
fn a_follower_that_truncates_on_every_append_is_caught() {
    is_caught(Variant::TruncateOnEveryAppend);
}

#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught() {
    is_caught(Variant::ResetTimerOnAnyRpc);
}

/// The install that writes the staged store's `CURRENT` before the repair is
/// durable (RAFT.md §5): a crash mid-install then adopts the leader's identity, a
/// state this server never held, and state machine safety reports the restart
/// whose restated log cannot account for its recovered applied index. The
/// crash-mid-install fault aims the crash; the pair rule holds because the
/// correct server passes the same seeds above.
#[test]
fn a_server_that_installs_without_current_last_is_caught() {
    is_caught(Variant::SnapshotWithoutCurrentLast);
}

/// The adoption as built under D-038 (D-041): the old store's `CURRENT`
/// and files removed before the staged copies' directory entries are synced, a
/// staging `CURRENT` that does not parse swept as debris, and no store marker to
/// refuse the emptied directory. Its window is a crash inside the copy whose bit
/// rot lands on the staging `CURRENT` — one block, two per cent per crash — with
/// none of the copies' entries surviving, after which the server restarts on a
/// fresh store and committed-entries-stay reports the truncation from index 1.
/// The crash-mid-adoption fault aims every crash at that window and rolls the
/// rot's dice sixteen to thirty-two times on a seed that draws it.
///
/// The storm rides one seed in four (`raft::ADOPTION_STORM_IN`), which is what
/// keeps `scripts/premerge.sh` inside its quarter of an hour, and the catch rate
/// went with the share: 23 of 100 release seeds when the storm rode every
/// schedule, 8 of 100 now, and 0 of the gate's 20 — the dice are simply not
/// rolled on three seeds in four. So the catch is asserted at the hundred-seed
/// tier and the fault's firing at every tier, the shape D-044 gave
/// `RefusalNotDurable` (whose catch D-056 moved to the thousand-seed tier, its
/// rate being thinner): what a gate run must still see is that the storm was drawn
/// and that it had adoptions to crash into, so a sweep that passes is known to
/// have injected the fault. The rate is printed at every tier, and the pair rule
/// holds because the correct server passes the same seeds above.
#[test]
fn a_server_whose_adoption_is_as_built_is_caught() {
    let moved = Mutex::new(MovedSeeds::default());
    let outcomes: Vec<(Option<String>, bool, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::AdoptionAsBuilt);
        let stormed = report
            .schedule
            .faults
            .iter()
            .any(|f| matches!(f, Fault::CrashAdopting { .. }));
        let adoptions = report.count(|e| matches!(e, TraceEvent::RaftAdopted { .. }));
        (checked(&report, &moved), stormed, adoptions)
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _, _)| v.as_ref()).collect();
    let stormed = outcomes.iter().filter(|(_, s, _)| *s).count();
    let adoptions: usize = outcomes.iter().map(|(_, _, a)| *a).sum();
    eprintln!(
        "AdoptionAsBuilt: caught on {} of {} seeds, the adoption storm drawn on {stormed} seeds with {adoptions} adoptions under it, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", |v| v.as_str())
    );
    print_moved("AdoptionAsBuilt", moved);
    assert!(
        stormed > 0,
        "the adoption crash storm was drawn on no seed: the fault was not injected"
    );
    assert!(
        adoptions > 0,
        "no staged install was ever adopted: the storm had nothing to crash into"
    );
    if seeds() >= 100 {
        assert!(!caught.is_empty(), "AdoptionAsBuilt was never caught");
    }
}

/// The refusal that lives only in the running process, and the refused engine
/// that keeps working: the server as built before D-044. Its window is
/// a server refused for lost state whose engine then flushes the memtable the
/// recovery replayed — a manifest without the dropped table, `CURRENT` switched
/// to it and the log segments that held the lost records deleted — and a crash
/// and restart before a leader re-seeds it, after which the store is
/// self-consistent and opens clean. State machine safety reports the
/// restatement whose recovered applied index its log cannot account for, which
/// is how the thousand-seed premerge's seed 687 read.
///
/// `Fault::CrashRefused` aims at that window: the replayed memtable is over the
/// threshold only when the crash before it landed inside a flush, which is a
/// hundredth of the time unaimed, and a crash on a server already sitting
/// refused is the restart that exposes the laundered store. What every tier must
/// see is the fault firing — a crash landing on a refused server — so that a
/// sweep which passes is known to have injected it. The pair rule holds because
/// the correct server passes the same seeds above.
#[test]
fn a_server_whose_refusal_is_not_durable_is_caught() {
    let moved = Mutex::new(MovedSeeds::default());
    let outcomes: Vec<(Option<String>, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::RefusalNotDurable);
        (checked(&report, &moved), crashes_while_refused(&report))
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _)| v.as_ref()).collect();
    let fired = outcomes.iter().filter(|(_, hits)| *hits > 0).count();
    eprintln!(
        "RefusalNotDurable: caught on {} of {} seeds, crashed a refused server on {fired} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", |v| v.as_str())
    );
    print_moved("RefusalNotDurable", moved);
    assert!(
        fired > 0,
        "no crash ever landed on a refused server: the fault was not injected"
    );
    // The catch needs three things of one crash: it must land inside a flush, so
    // that the open after it replays two memtables; its bit rot must land in a
    // table the manifest lists, so that open is refused; and the crash after it
    // must come before a leader re-seeds the server. That is a thin conjunction —
    // on 268cf58, before D-056, 3 of the first hundred release seeds and none of the
    // gate's twenty (the first at seed 80), 14 of a thousand; at the nightly's ten
    // thousand 132 (D-047, DECISIONS.md:3141).
    //
    // D-056, the owner's decision of 2026-09-15: the catch is asserted from the
    // thousand-seed tier (the premerge and the nightly), the fault's firing at every
    // tier above, and the rate printed at every tier. On the tree with the send
    // queue, which moved every schedule, the catch is 16 of the first thousand seeds,
    // none of them below seed 100 (the first is 119). At the nightly's rate, 1.32 %,
    // a hundred seeds catch none about one time in four (0.9868^100 = 0.26) and the
    // gate's twenty three times in four, so the assertion there would fail a tree
    // with nothing wrong on the draw alone; a thousand miss about once in six hundred
    // thousand (0.9868^1000 = 1.7e-6). Seed 119, the first catch of the thousand, is
    // pinned with its mechanism at every tier:
    // `seed_119_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss`.
    if seeds() >= 1000 {
        assert!(!caught.is_empty(), "RefusalNotDurable was never caught");
    }
}

/// Seed 119, pinned by the owner's decision of 2026-09-15 (D-056): the first seed of
/// the first thousand on which `RefusalNotDurable` is caught, on the tree with the send
/// queue. The catch is 16 of those thousand, none below seed 100, and the nightly's
/// last measure, before D-056, was 132 of ten thousand, at which a hundred seeds catch
/// none about one time in four; so the sweep asserts the catch only from the
/// thousand-seed tier, and this pin keeps the variant's catch, with its mechanism, at
/// every tier, the gate's and CI's included.
///
/// As built, read off the trace: `Fault::CrashRefused` crashes server 3 (server 2
/// leads, so its neighbour is the victim) four times. The first, at 11.147 s, lands
/// inside a flush, and its restart is refused for the log's missing head and re-seeded.
/// The second, at 12.769 s, lands inside a flush with its bit rot on tables 29 and 31,
/// which the manifest lists: the open drops both and server 3 is refused for lost state
/// at 12.835 s. The third, at 12.836 s, lands on the refused server, whose next open is
/// refused again at 12.891 s; this time the refused engine, not quiesced, flushes what
/// the recovery replayed — manifest 11 listing tables 30 and 32 only, `CURRENT`
/// switched to it, log segments 1 and 2 deleted — and the loss is laundered. The
/// fourth, at 12.989 s, comes the fault's grace, 136 ms, after that refused restart;
/// its open finds a self-consistent store, removes tables 29 and 31 as orphans and
/// recovers clean at 13.070 s with an applied index of 330, before a leader's install
/// is adopted at 13.432 s. State machine safety reports that restatement, whose log
/// does not hold the index 1 it claims to have applied. That is seed 687's premerge
/// failure, the bug D-044 fixed, and decision time (D-047) does not move it.
///
/// The correct server's run is the variant's up to server 3's first refusal, which it
/// records in the store's marker before tracing it (D-044): the mark's write and sync
/// put its `RaftRefused` at 11.208 s against the variant's 11.203 s, and the run's
/// schedule differs from there on. On that schedule the fault still fires as aimed —
/// its three later rounds crash server 3 inside a flush, at 13.044, 13.106 and 13.168 s
/// — but none of their bit rot lands on a table an open reads, so no open drops a
/// table, server 3 is never refused for lost state and no crash lands on a refused
/// server; the run passes. The situation the fix handles is absent here, asserted, with
/// that reason; the correct server's quiesce and durable refusal are pinned on seed
/// 687, where its schedule reaches them, and this pin runs its seed 119 at every tier,
/// where the correct server's sweep reaches that seed only from the thousand-seed tier.
#[test]
fn seed_119_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss() {
    use ananke_env::NodeId;
    let node = Some(NodeId::new(3));
    let lost_state = |report: &raft::Report| -> Vec<ananke_env::Instant> {
        report
            .records
            .iter()
            .filter(|r| {
                matches!(&r.event, TraceEvent::RaftRefused { server: 3, reason }
                    if reason.starts_with(LOST_STATE))
            })
            .map(|r| r.at)
            .collect()
    };
    // Server 3's crashes that land with a memtable rotated and not yet flushed: the
    // moment `Fault::CrashRefused` waits for. Memtables are numbered per open.
    let crashes_inside_a_flush = |report: &raft::Report, after: ananke_env::Instant| {
        let mut pending: BTreeSet<u64> = BTreeSet::new();
        let mut crashes = Vec::new();
        for r in report.records.iter().filter(|r| r.node == node) {
            match r.event {
                TraceEvent::MemtableRotated { memtable, .. } => {
                    pending.insert(memtable);
                }
                TraceEvent::MemtableFlushed { memtable, .. } => {
                    pending.remove(&memtable);
                }
                TraceEvent::NodeCrashed { .. } => {
                    if !pending.is_empty() && r.at > after {
                        crashes.push(r.at);
                    }
                    pending.clear();
                }
                _ => {}
            }
        }
        crashes
    };

    let built = raft::run(119, Variant::RefusalNotDurable);
    let verdict = built.check();
    assert_eq!(
        built.moved_by_decision_time(&verdict),
        None,
        "seed 119 as built: decision time moves its verdict; re-audit the pin"
    );
    let violation = verdict.err().unwrap_or_default();
    assert!(
        violation.starts_with(
            "seed 119: state machine safety: server 3 recovered an applied index of 330 but its \
             log does not hold index 1"
        ),
        "seed 119 as built is no longer caught with the laundered store's restatement: \
         {violation:?}; re-audit the pin"
    );
    let refusals = lost_state(&built);
    let (Some(&refused), Some(&last_refused)) = (refusals.first(), refusals.last()) else {
        panic!("seed 119 as built never refuses server 3 for lost state: re-audit the pin");
    };
    assert!(
        crashes_while_refused(&built) > 0,
        "seed 119 as built: no crash lands on a refused server; re-audit the pin"
    );
    let restarts = built.restarts_after_lost_state_refusal();
    assert!(
        !restarts.is_empty()
            && restarts
                .iter()
                .all(|&(server, at, _)| server == 3 && at == refused),
        "seed 119 as built no longer restarts server 3 after its refusal for lost state at \
         {refused:?} before an install: {restarts:?}; re-audit the pin"
    );
    let dropped: BTreeSet<u64> = built
        .records
        .iter()
        .filter(|r| r.node == node && r.at <= refused)
        .filter_map(|r| match r.event {
            TraceEvent::SstDropped { number, .. } => Some(number),
            _ => None,
        })
        .collect();
    assert_eq!(
        dropped,
        BTreeSet::from([29, 31]),
        "seed 119 as built: server 3's refused open no longer drops tables 29 and 31; re-audit \
         the pin"
    );
    let recovered_clean = built
        .records
        .iter()
        .find(|r| {
            r.at > last_refused
                && matches!(
                    r.event,
                    TraceEvent::RaftRecovered {
                        server: 3,
                        applied: 330,
                        ..
                    }
                )
        })
        .map(|r| r.at)
        .expect(
            "seed 119 as built: server 3 never opens clean after its refusal; re-audit the pin",
        );
    let adopted = built
        .records
        .iter()
        .find(|r| r.at > refused && matches!(r.event, TraceEvent::RaftAdopted { server: 3 }))
        .map(|r| r.at)
        .expect("seed 119 as built never re-seeds server 3 after its refusal: re-audit the pin");
    assert!(
        recovered_clean < adopted,
        "seed 119 as built: server 3's clean open at {recovered_clean:?} no longer comes before \
         the install adopted at {adopted:?}; re-audit the pin"
    );
    let between = |r: &&ananke_env::sim::TraceRecord| {
        r.node == node && r.at > last_refused && r.at < recovered_clean
    };
    let laundering_manifest = built.records.iter().filter(between).any(|r| {
        matches!(&r.event, TraceEvent::ManifestWritten { tables, .. }
            if !tables.is_empty() && tables.iter().all(|t| !dropped.contains(t)))
    });
    let segments_deleted = built
        .records
        .iter()
        .filter(between)
        .filter(|r| matches!(r.event, TraceEvent::WalSegmentDeleted { .. }))
        .count();
    assert!(
        laundering_manifest && segments_deleted > 0,
        "seed 119 as built: the refused engine no longer writes a manifest without tables \
         {dropped:?} and deletes log segments ({segments_deleted}) before the clean open at \
         {recovered_clean:?}; re-audit the pin"
    );

    let correct = raft::run(119, Variant::Correct);
    assert_eq!(
        correct.check().err(),
        None,
        "seed 119 under the correct server no longer passes"
    );
    let head_refused = |report: &raft::Report| {
        report
            .records
            .iter()
            .find(|r| {
                matches!(&r.event, TraceEvent::RaftRefused { server: 3, reason }
                if reason.starts_with("the log's head is missing"))
            })
            .map(|r| r.at)
    };
    let (Some(built_head), Some(correct_head)) = (head_refused(&built), head_refused(&correct))
    else {
        panic!("seed 119: server 3's first refusal, for the log's missing head, is gone; re-audit");
    };
    assert!(
        correct_head > built_head,
        "seed 119: the correct server's first refusal ({correct_head:?}) no longer comes after \
         the variant's ({built_head:?}), where the durable mark moves the schedule; re-audit"
    );
    let aimed = crashes_inside_a_flush(&correct, correct_head);
    assert_eq!(
        aimed.len(),
        3,
        "seed 119 under the correct server: the fault's later rounds no longer crash server 3 \
         inside a flush three times ({aimed:?}); re-audit the pin"
    );
    let dropped_any = correct
        .records
        .iter()
        .any(|r| r.node == node && matches!(r.event, TraceEvent::SstDropped { .. }));
    assert!(
        lost_state(&correct).is_empty() && !dropped_any && crashes_while_refused(&correct) == 0,
        "seed 119 under the correct server now drops a table or refuses server 3 for lost state, \
         or crashes a refused server: the situation the fix handles is reached, so pin the \
         engine's quiesce and the durable refusal on it"
    );
}

/// The leader that ignores the store incarnation its followers answer with
/// (RAFT.md §3, D-042): a follower refused for lost state and re-seeded
/// from a snapshot comes back below the match index the leader recorded for it,
/// the match is monotone and the probe never reaches below it, so every answer
/// is discarded and the follower is never counted again while that leader leads.
///
/// Over 100 release seeds the sweep catches it on none, and the owner's ban on
/// `#[ignore]`d variant tests keeps the test running rather than skipped. Only
/// with the third server unavailable at the same time does the wedge stall a
/// commit, and after the last heal every fault has healed or restarted, so a
/// server is unavailable then only by refusal — and a refused server beside a
/// re-seeded one is exactly the configuration `Report::majority_up` withholds the
/// liveness bound from (D-035's carve-out). Were the bound asked there, the
/// leader as built re-seeds the refused server too, when it was designated while
/// it was down, and commits with it inside the bound. Seeing the wedge needs a
/// liveness ask when a leader in force at the last heal has a commit majority
/// among the servers that are up, quarantined ones included, and a schedule that
/// refuses a second follower under that leader, which the disk model's rot draws
/// on its own and no driver can aim. Seed 5909 was not that shape: its wedge was
/// D-043's alone (`seed_5909_which_the_nightly_found_stays_green`).
///
/// What the test asserts is what is true and what the pair rule can still be
/// held to here: the variant is really the leader as built, which the trace says
/// exactly — this leader never forgets a follower's progress, so it traces no
/// `RaftProgressReset` on any seed, while the correct server's own sweep above
/// requires one wherever it saw a refusal — and the sweep reaches the state the
/// wedge is built on, a refused follower re-seeded and applying again. A sweep
/// that could not distinguish the two leaders at all would fail here. The catch
/// rate is printed at every tier.
///
/// At the nightly's ten thousand (run 34711427220, on 14c3e17) it printed 4 of
/// 10 000, and every one was the pre-vote isolation check's trace-timing gap — a
/// term rise adopted from a message delivered before the isolation began and
/// traced after it, the gap that failed the correct server on seeds 1885 and
/// 2023 — not this bug. D-047 closes that gap: at ten thousand seeds after it
/// (runs 34731272921 and 34749071877) it printed 0 of 10 000, and the four are
/// pinned as not catches in
/// `the_nightlys_eleven_variant_catches_of_the_trace_timestamp_gap_are_not_catches`.
#[test]
fn a_leader_that_ignores_incarnations_never_forgets() {
    let moved = Mutex::new(MovedSeeds::default());
    let outcomes: Vec<(Option<String>, usize, bool)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::IgnoreIncarnation);
        let resets = report.count(|e| matches!(e, TraceEvent::RaftProgressReset { .. }));
        let reseeded = reseed_completed(&report);
        (checked(&report, &moved), resets, reseeded)
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _, _)| v.as_ref()).collect();
    let resets: usize = outcomes.iter().map(|(_, r, _)| *r).sum();
    let reseeds = outcomes.iter().filter(|(_, _, r)| *r).count();
    eprintln!(
        "IgnoreIncarnation: caught on {} of {} seeds, {resets} progress resets, a refused follower re-seeded and applying again on {reseeds} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", |v| v.as_str())
    );
    print_moved("IgnoreIncarnation", moved);
    assert_eq!(
        resets, 0,
        "the leader that ignores incarnations reset a follower's progress: the variant was not injected"
    );
    // A refusal needs the disk's rot to land in a table still in use, and the
    // re-seed follows the refusal: twenty seeds cannot promise one, a hundred can.
    if seeds() >= 100 {
        assert!(
            reseeds > 0,
            "no refused follower was ever re-seeded and applying again: the sweep never reached the state the wedge is built on"
        );
    }
}

/// The leader as built before D-043: one mutable checkpoint directory per
/// index, rewritten by every take at that index under whatever stream reads it,
/// and one snapshot stream at a time, every other designated follower queued
/// behind it. A retake at the index a stream is reading scrambles that stream,
/// which never completes, and the follower queued behind it gets neither the
/// stream nor entries; with both followers uncountable the leader loses its
/// quorum and nothing commits, which the liveness check reports (nightly run
/// 34496762339, seed 5909).
///
/// `Fault::RetakeUnderStream` aims at that shape and reaches it: it fills the
/// state machine so a checkpoint is worth streaming, isolates a follower until
/// it is designated snapshot-fed, waits for the leader to open the stream, and
/// then cuts the leader's other follower off — so the leader keeps its quorum
/// through the follower it is feeding, which answers every heartbeat, and has
/// nobody to count, and its applied index stands still at the index it last
/// took, which is the index the running stream is reading. The arm rides one
/// seed in four and got as far as the stream on 16 of 100 release seeds.
///
/// It has not been shown to make the variant catchable at any tier: 0 of 100
/// release seeds, 2 of 1000 on the tree with D-056's send queue — seeds 132 and 848,
/// by the liveness check (seed 680 alone before it) — and neither catch is this arm's:
/// neither seed draws it, and their re-takes are the server's own
/// (`seed_132_pins_the_combined_variant_and_the_stream_half_alone_catches_it_too`).
/// The arm reached a live stream on 143 of 1000 seeds (152 before D-056) and caught
/// none of them.
///
/// Why the arm reaches the shape without catching it is worth writing down,
/// because it is not a matter of
/// running more seeds at the hundred tier. A leader needs *one* countable
/// follower for a majority, and on this sweep an install is over in about a
/// hundred and fifty milliseconds — the state machine is small and a checkpoint
/// of it is a couple of dozen chunks. The moment the fed follower's install
/// completes it is countable again, the leader commits, its applied index moves
/// off the index the stream was reading, and the freeze is over: the queue half
/// of the bug costs a second designated follower a few hundred milliseconds
/// against a two-second bound, never the bound itself. Only a stream that never
/// completes stalls a commit that long, and that needs a take to land on the
/// very directory a live stream has open — the applied index standing exactly
/// where the record already points *and* a `retake` having cleared the
/// checkpoint, a coincidence inside the snapshot task's own failure paths that
/// the arm can make likely but cannot force. The re-take at an index already
/// taken happens often on its own — 46 of 100 seeds, 525 of 1000 — and is
/// harmless every time, because no stream had that directory open.
///
/// So the test asserts what is true: that the fault fired, on both counts — a
/// take at an index already taken, into the directory a stream may be reading,
/// and the aimed arm's own stream under a leader that cannot commit — and that
/// the catch holds at the tier that ever produced it, the nightly's ten
/// thousand, where a rate of about one in a thousand gives some ten catches.
/// Asserting it at the pre-merge tier on one observation would make that tier
/// flaky; the rates are printed at every tier so the day the rate is worth an
/// assertion is visible. The pair rule holds because the correct server passes
/// the same seeds.
///
/// The nightly's ten thousand (run 34711427220) printed 13: 4 by the liveness
/// check, the wedge itself; 7 by the pre-vote isolation check's trace-timing
/// gap, the one that failed the correct server on seeds 1885 and 2023; and 2 by
/// the linearizability checker exhausting its search budget, which proves
/// nothing. The assertion used to count all thirteen. D-047 closes the
/// seven — the pre-vote check reads a rise by its decision time, and
/// `the_nightlys_eleven_variant_catches_of_the_trace_timestamp_gap_are_not_catches`
/// pins them — and a budget exhaustion is no evidence of the bug, so the
/// ten-thousand-seed assertion now counts only the liveness check's catches, which
/// are the wedge, and the catches are printed by check.
#[test]
fn a_leader_that_shares_one_snapshot_directory_and_streams_one_follower_at_a_time_is_caught() {
    let moved = Mutex::new(MovedSeeds::default());
    let outcomes: Vec<(Option<String>, bool, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::SharedSnapshotDir);
        (
            checked(&report, &moved),
            retook_at_one_index(&report),
            report.aimed_streams,
        )
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _, _)| v.as_ref()).collect();
    let fired = outcomes.iter().filter(|(_, fired, _)| *fired).count();
    let aimed = outcomes.iter().filter(|(_, _, aimed)| *aimed > 0).count();
    let liveness = caught.iter().filter(|v| v.contains(": liveness: ")).count();
    // D-047: the catches by check, the name a violation starts with.
    let mut by_check: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &caught {
        let check = violation
            .split_once(": ")
            .and_then(|(_, rest)| rest.split_once(':'))
            .map_or(violation.as_str(), |(check, _)| check);
        *by_check.entry(check).or_default() += 1;
    }
    eprintln!(
        "SharedSnapshotDir: caught on {} of {} seeds, {liveness} by the liveness check, by check {by_check:?}, re-took at an index already taken on {fired} seeds, the aimed re-take arm reached its stream on {aimed} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", |v| v.as_str())
    );
    print_moved("SharedSnapshotDir", moved);
    assert!(
        fired > 0,
        "SharedSnapshotDir never re-took at an index already taken: the fault was not injected"
    );
    assert!(
        aimed > 0,
        "the aimed re-take arm never reached a stream: the shape it exists to build was never built"
    );
    if seeds() >= 10_000 {
        assert!(
            liveness > 0,
            "SharedSnapshotDir's wedge was never caught by the liveness check; by check: {by_check:?}"
        );
    }
}

/// How many crashes landed on a server that was sitting refused for lost state:
/// what [`Fault::CrashRefused`] aims at, counted from the trace so a sweep that
/// passes is known to have injected the fault (D-044). A server is
/// refused from its `RaftRefused` until its next restatement.
fn crashes_while_refused(report: &raft::Report) -> usize {
    let mut refused: BTreeSet<u64> = BTreeSet::new();
    let mut crashes = 0;
    for event in report.events() {
        match event {
            TraceEvent::RaftRefused { server, .. } => {
                refused.insert(server);
            }
            TraceEvent::RaftRecovered { server, .. } => {
                refused.remove(&server);
            }
            TraceEvent::NodeCrashed { node } if refused.contains(&u64::from(node.get())) => {
                crashes += 1;
            }
            _ => {}
        }
    }
    crashes
}

/// Whether some server took a snapshot at the index it had already taken: the
/// re-take that, as built, sweeps and rewrites the shared directory under any
/// stream reading it (D-043). A restart re-states the record's snapshot
/// right after its `RaftTruncate`; that is the disk's picture, not a take.
fn retook_at_one_index(report: &raft::Report) -> bool {
    let mut last_take: BTreeMap<u64, u64> = BTreeMap::new();
    let mut restating: BTreeSet<u64> = BTreeSet::new();
    for event in report.events() {
        match event {
            TraceEvent::RaftTruncate { server, .. } => {
                restating.insert(server);
            }
            TraceEvent::RaftSnapshot {
                server,
                last_index,
                taken: true,
                ..
            } => {
                if !restating.remove(&server) && last_take.get(&server) == Some(&last_index) {
                    return true;
                }
                last_take.insert(server, last_index);
            }
            _ => {}
        }
    }
    false
}

/// Lease safety under drift (RAFT.md §2, invariant 6): on every seed where the
/// simulated drift exceeds the bound, either the guard revoked the drifting
/// follower's trust or the checker reports the stale read and the run fails. The
/// correct server passes every seed (above); here each exceeded seed is run with
/// the guard and without it, and the report says how many revoked, how many read
/// stale without the guard, and how many did neither.
#[test]
fn a_leader_that_trusts_the_clock_is_caught_and_the_guard_revokes() {
    /// What one seed contributes: the guardless server runs only where the drift
    /// exceeded the bound, and its stale read's violation is kept for the report.
    struct Seed {
        led: usize,
        exceeded: bool,
        revoked: bool,
        stale: Option<String>,
        lease_reads_within: usize,
    }
    let per_seed = sweep(seeds(), |seed| {
        let correct = raft::run(seed, Variant::Correct);
        let led = correct.trials_led_by_slowest;
        if !correct.drift_exceeded() {
            return Seed {
                led,
                exceeded: false,
                revoked: false,
                stale: None,
                lease_reads_within: correct.lease_reads(),
            };
        }
        let revoked = correct.lease_revokes() > 0;
        let stale = match raft::run(seed, Variant::LeaseTrustsTheClock).check() {
            Err(violation) if violation.contains("linearizability") => Some(violation),
            _ => None,
        };
        Seed {
            led,
            exceeded: true,
            revoked,
            stale,
            lease_reads_within: 0,
        }
    });
    let mut exceeded = 0;
    let mut revoked = 0;
    let mut stale = 0;
    let mut neither = 0;
    let mut lease_reads_within = 0;
    let mut slowest_led = 0;
    let mut first_stale = String::new();
    for seed in per_seed {
        slowest_led += seed.led;
        lease_reads_within += seed.lease_reads_within;
        if !seed.exceeded {
            continue;
        }
        exceeded += 1;
        revoked += usize::from(seed.revoked);
        let read_stale = seed.stale.is_some();
        if let Some(violation) = seed.stale
            && first_stale.is_empty()
        {
            first_stale = violation;
        }
        stale += usize::from(read_stale);
        neither += usize::from(!seed.revoked && !read_stale);
    }
    eprintln!(
        "lease safety: drift beyond {DRIFT_BOUND_PPM} ppm on {exceeded} of {} seeds; of those, the guard revoked on {revoked}, a stale read was caught without the guard on {stale}, neither on {neither}; the slowest clock led {slowest_led} of the trials; {lease_reads_within} lease reads on the seeds within the bound; first stale: {first_stale}",
        seeds()
    );
    assert!(exceeded > 0, "no seed exceeded the drift bound");
    assert!(revoked > 0, "the guard never revoked");
    assert!(stale > 0, "LeaseTrustsTheClock was never caught");
}

/// What the correct server's sweep saw.
#[derive(Debug, Default)]
struct Coverage {
    seeds: u64,
    uniform_seeds: u64,
    partitions: usize,
    one_way_blocks: usize,
    crashes: usize,
    leader_crashes: usize,
    stale_sender_faults: usize,
    figure_eight_faults: usize,
    burst_puts: usize,
    drift_exceeded_seeds: u64,
    lease_reads: usize,
    read_index_reads: usize,
    lease_revokes: usize,
    quorum_losses: usize,
    refusals: usize,
    duplicates: usize,
    drops: usize,
    // PROPOSED(D-056): frames a sending socket's full queue dropped, printed and not
    // asserted: a queue of 1 024 fills only when one socket sends one destination
    // faster than a gigabit drains it, which no Phase 2 scenario does (0 over the
    // correct server's first 1 000 seeds).
    queue_drops: usize,
    leaders: usize,
    terms_above_one: u64,
    truncations: usize,
    commits: usize,
    applies: usize,
    inbox_drops: usize,
    snapshots_taken: usize,
    snapshots_installed: usize,
    snapshot_resumes: usize,
    snapshot_versions_deleted: usize,
    snapshot_takes_reused: usize,
    snapshot_streams_at_once: usize,
    compactions: usize,
    reseeded: usize,
    reseed_completions: u64,
    progress_resets: usize,
    install_crash_faults: usize,
    adoption_crash_faults: usize,
    // D-043: the re-take-under-a-stream arm, and how many of its arms
    // got as far as the stream they aim under.
    retake_stream_faults: usize,
    aimed_streams: usize,
    adoptions: usize,
    marker_refusals: usize,
    // D-044: the crash-after-refusal fault, the crashes it landed on a
    // refused server, the engines quiesced, and the refusals the store's own
    // lost mark made.
    refusal_crash_faults: usize,
    refused_crashes: usize,
    quiesced_engines: usize,
    lost_mark_refusals: usize,
    bit_rot: usize,
    torn_writes: usize,
    // D-049: check-quorum step-downs that left a refused follower's
    // rejections uncounted for want of re-seed progress.
    step_downs_uncounting_refused: usize,
    puts: u64,
    gets: u64,
    deletes: u64,
    cas: u64,
    completed: u64,
    abandoned: u64,
    redirected: u64,
    slowest_write_after_heal: Duration,
}

/// The re-seed episodes the correct server's sweep ran (D-049,
/// `raft::Report::reseed_episodes`): the evidence D-049 records against counting
/// nothing from a refused follower. Every figure is over completed episodes, as if
/// the leader's other follower had been away for each, measured twice: up to the
/// follower's `Installed` answer, and through the adoption to its first answer
/// from the new store. An expected count sums each episode's share of window
/// placements that find a window with nothing to count; a certain count is of the
/// episodes where every placement does.
#[derive(Debug, Default)]
struct ReseedEpisodes {
    episodes: usize,
    completed: usize,
    to_installed: RuleCounts,
    /// Of the completed episodes, those whose follower answered the leader from
    /// its new store within the tenure; the rest are measured to the tenure's end.
    answered_from_store: usize,
    through_adoption: RuleCounts,
    /// No other server answering from a store for over two windows before the
    /// install: where this sweep itself left a leader's majority needing the
    /// refused follower.
    only_contact_over_two_windows: usize,
    median_length_windows: f64,
    longest_length_windows: f64,
    median_adoption_windows: f64,
    longest_adoption_windows: f64,
}

/// Expected and certain step-downs under the three ways of counting.
#[derive(Debug, Default)]
struct RuleCounts {
    expected_counting_nothing: f64,
    certain_counting_nothing: usize,
    expected_correct: f64,
    certain_correct: usize,
    expected_as_built: f64,
    certain_as_built: usize,
}

impl RuleCounts {
    fn add(&mut self, nothing: f64, correct: f64, as_built: f64) {
        self.expected_counting_nothing += nothing;
        self.certain_counting_nothing += usize::from(nothing >= 1.0);
        self.expected_correct += correct;
        self.certain_correct += usize::from(correct >= 1.0);
        self.expected_as_built += as_built;
        self.certain_as_built += usize::from(as_built >= 1.0);
    }
}

/// The completed episodes' lengths, up to the install and through the adoption.
#[derive(Default)]
struct EpisodeLengths {
    stream: Vec<f64>,
    adoption: Vec<f64>,
}

impl ReseedEpisodes {
    /// Adds `report`'s episodes, and each completed one's lengths to `lengths`.
    fn add(&mut self, report: &raft::Report, lengths: &mut EpisodeLengths) {
        for episode in report.reseed_episodes() {
            self.episodes += 1;
            if !episode.completed {
                continue;
            }
            self.completed += 1;
            lengths.stream.push(episode.length_windows);
            lengths.adoption.push(episode.adoption_windows);
            self.to_installed.add(
                episode.deposed_counting_nothing,
                episode.deposed_correct,
                episode.deposed_as_built,
            );
            self.answered_from_store += usize::from(episode.answered_from_store);
            self.through_adoption.add(
                episode.deposed_counting_nothing_through_adoption,
                episode.deposed_correct_through_adoption,
                episode.deposed_as_built_through_adoption,
            );
            self.only_contact_over_two_windows += usize::from(episode.only_contact_windows > 2.0);
        }
    }

    /// Sets the medians and the longest of the completed episodes' `lengths`.
    fn finish(&mut self, lengths: EpisodeLengths) {
        let median_and_longest = |mut all: Vec<f64>| {
            all.sort_by(f64::total_cmp);
            all.last()
                .map_or((0.0, 0.0), |&longest| (all[all.len() / 2], longest))
        };
        (self.median_length_windows, self.longest_length_windows) =
            median_and_longest(lengths.stream);
        (self.median_adoption_windows, self.longest_adoption_windows) =
            median_and_longest(lengths.adoption);
    }
}

/// Whether some refused server came back (RAFT.md §3): a `RaftRefused`, then a
/// re-seeded restatement on that server, then an apply on it.
fn reseed_completed(report: &raft::Report) -> bool {
    let mut refused: BTreeSet<u64> = BTreeSet::new();
    let mut reseeded: BTreeSet<u64> = BTreeSet::new();
    for record in &report.records {
        match &record.event {
            TraceEvent::RaftRefused { server, .. } => {
                refused.insert(*server);
            }
            TraceEvent::RaftReseeded { server } if refused.contains(server) => {
                reseeded.insert(*server);
            }
            TraceEvent::RaftApply { server, .. } if reseeded.contains(server) => return true,
            _ => {}
        }
    }
    false
}

impl Coverage {
    fn add(&mut self, report: &raft::Report) {
        self.seeds += 1;
        self.uniform_seeds += u64::from(report.uniform());
        self.partitions += report.count(|e| matches!(e, TraceEvent::PartitionStarted { .. }));
        self.one_way_blocks += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::OneWay { .. }))
            .count();
        self.crashes += report.count(|e| matches!(e, TraceEvent::NodeCrashed { .. }));
        self.leader_crashes += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::CrashLeader { .. }))
            .count();
        self.stale_sender_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::StaleSender { .. }))
            .count();
        self.figure_eight_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::FigureEight { .. }))
            .count();
        self.burst_puts += report.burst_puts();
        self.drift_exceeded_seeds += u64::from(report.drift_exceeded());
        self.lease_reads += report.lease_reads();
        self.read_index_reads += report.read_index_reads();
        self.lease_revokes += report.lease_revokes();
        self.quorum_losses += report.quorum_losses();
        self.refusals += report.refused.len();
        self.duplicates +=
            report.count(|e| matches!(e, TraceEvent::MessageDelivered { dup: true, .. }));
        self.drops += report.count(|e| {
            matches!(
                e,
                TraceEvent::MessageDropped {
                    reason: DropReason::Injected,
                    ..
                }
            )
        });
        self.queue_drops += report.count(|e| {
            matches!(
                e,
                TraceEvent::MessageDropped {
                    reason: DropReason::QueueFull,
                    ..
                }
            )
        });
        self.leaders += report.count(|e| matches!(e, TraceEvent::RaftLeader { .. }));
        self.terms_above_one += u64::from(
            report.has(|e| matches!(e, TraceEvent::RaftLeader { term, .. } if *term > 1)),
        );
        self.truncations += report.count(|e| matches!(e, TraceEvent::RaftTruncate { .. }));
        self.commits += report.count(|e| matches!(e, TraceEvent::RaftCommit { .. }));
        self.applies += report.count(|e| matches!(e, TraceEvent::RaftApply { .. }));
        self.inbox_drops += report.count(|e| matches!(e, TraceEvent::RaftInboxDropped { .. }));
        self.snapshots_taken +=
            report.count(|e| matches!(e, TraceEvent::RaftSnapshot { taken: true, .. }));
        self.snapshots_installed +=
            report.count(|e| matches!(e, TraceEvent::RaftSnapshot { taken: false, .. }));
        self.snapshot_resumes +=
            report.count(|e| matches!(e, TraceEvent::RaftSnapshotResumed { .. }));
        // D-043: versions swept, takes answered by the recorded
        // version, and leaders streaming to more than one follower at once.
        self.snapshot_versions_deleted +=
            report.count(|e| matches!(e, TraceEvent::RaftSnapshotDeleted { .. }));
        self.snapshot_takes_reused +=
            report.count(|e| matches!(e, TraceEvent::RaftSnapshotReused { .. }));
        self.snapshot_streams_at_once += report.count(
            |e| matches!(e, TraceEvent::RaftSnapshotStreams { streams, .. } if *streams > 1),
        );
        self.compactions += report.count(|e| matches!(e, TraceEvent::RaftCompacted { .. }));
        self.reseeded += report.count(|e| matches!(e, TraceEvent::RaftReseeded { .. }));
        self.reseed_completions += u64::from(reseed_completed(report));
        self.progress_resets += report.count(|e| matches!(e, TraceEvent::RaftProgressReset { .. }));
        self.install_crash_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::CrashInstalling { .. }))
            .count();
        // D-041: the crash-mid-adoption fault, the adoptions it and the
        // installs produce, and the refusals the store marker made where the
        // engine alone would have opened a fresh store.
        self.adoption_crash_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::CrashAdopting { .. }))
            .count();
        self.adoptions += report.count(|e| matches!(e, TraceEvent::RaftAdopted { .. }));
        // D-043: the aimed arm, and the stream it got as far as.
        self.retake_stream_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::RetakeUnderStream { .. }))
            .count();
        self.aimed_streams += report.aimed_streams;
        self.marker_refusals += report
            .refused
            .iter()
            .filter(|(_, reason)| reason.contains(STORE_MARKER))
            .count();
        // D-044: the crash-after-refusal arm, the crashes it landed,
        // the engines quiesced and the refusals the lost mark itself made.
        self.refusal_crash_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::CrashRefused { .. }))
            .count();
        self.refused_crashes += crashes_while_refused(report);
        self.quiesced_engines += report.count(|e| matches!(e, TraceEvent::EngineQuiesced { .. }));
        self.lost_mark_refusals += report
            .refused
            .iter()
            .filter(|(_, reason)| reason.contains("marker says this store lost state"))
            .count();
        self.bit_rot += report.count(|e| matches!(e, TraceEvent::BlockRotted { .. }));
        self.torn_writes += report.count(|e| matches!(e, TraceEvent::WriteTorn { .. }));
        self.step_downs_uncounting_refused += report.count(
            |e| matches!(e, TraceEvent::RaftQuorumLost { uncounted, .. } if !uncounted.is_empty()),
        );
        for op in &report.history.ops {
            match op.op {
                ClientOp::Put { .. } => self.puts += 1,
                ClientOp::Get { .. } => self.gets += 1,
                ClientOp::Delete { .. } => self.deletes += 1,
                ClientOp::Cas { .. } => self.cas += 1,
            }
        }
        self.completed += report.clients.completed;
        self.abandoned += report.clients.abandoned;
        self.redirected += report.clients.redirected;
        if let Some(took) = report.time_to_write_after_heal() {
            self.slowest_write_after_heal = self.slowest_write_after_heal.max(took);
        }
    }

    fn assert_complete(&self) {
        for (what, seen) in [
            ("symmetric partitions", self.partitions as u64),
            ("one-way blocks", self.one_way_blocks as u64),
            ("crashes", self.crashes as u64),
            ("leader crashes", self.leader_crashes as u64),
            ("stale-sender faults", self.stale_sender_faults as u64),
            ("figure-8 drivers", self.figure_eight_faults as u64),
            ("burst puts", self.burst_puts as u64),
            (
                "seeds with drift beyond the bound",
                self.drift_exceeded_seeds,
            ),
            ("lease reads", self.lease_reads as u64),
            ("read-index reads", self.read_index_reads as u64),
            ("lease revocations", self.lease_revokes as u64),
            ("check-quorum step-downs", self.quorum_losses as u64),
            ("duplicated deliveries", self.duplicates as u64),
            ("injected drops", self.drops as u64),
            ("elections", self.leaders as u64),
            ("seeds with a term above one", self.terms_above_one),
            ("log truncations", self.truncations as u64),
            ("snapshots taken", self.snapshots_taken as u64),
            ("log compactions", self.compactions as u64),
            ("crash-mid-install faults", self.install_crash_faults as u64),
            (
                "crash-mid-adoption faults",
                self.adoption_crash_faults as u64,
            ),
            (
                "re-take-under-a-stream faults",
                self.retake_stream_faults as u64,
            ),
            ("commits", self.commits as u64),
            ("applies", self.applies as u64),
            ("bit rot", self.bit_rot as u64),
            ("puts", self.puts),
            ("gets", self.gets),
            ("deletes", self.deletes),
            ("compare-and-sets", self.cas),
            ("completed operations", self.completed),
            ("abandoned operations", self.abandoned),
            ("redirected tries", self.redirected),
            ("uniformly scheduled seeds", self.uniform_seeds),
        ] {
            assert!(seen > 0, "the sweep never saw {what}: {self:?}");
        }
        // A refusal needs bit rot to land in a table or a log block still in use:
        // twenty seeds cannot promise one; a hundred can. The same goes for what
        // follows from a refusal — the re-seed — and for a resumed stream, which
        // needs a drop to land on a chunk or its acknowledgement.
        if self.seeds >= 100 {
            assert!(
                self.refusals > 0,
                "the sweep never saw a server refused for lost state: {self:?}"
            );
            assert!(
                self.torn_writes > 0,
                "the sweep never saw a torn write: {self:?}"
            );
            assert!(
                self.snapshots_installed > 0,
                "the sweep never saw a snapshot installed: {self:?}"
            );
            assert!(
                self.snapshot_resumes > 0,
                "the sweep never saw a snapshot stream resumed after loss: {self:?}"
            );
            assert!(
                self.reseeded > 0,
                "the sweep never saw a re-seeded server: {self:?}"
            );
            assert!(
                self.reseed_completions > 0,
                "no refused server was ever re-seeded and applying again: {self:?}"
            );
            // D-041: every install is adopted at the next start, so a
            // hundred seeds that install also adopt.
            assert!(
                self.adoptions > 0,
                "the sweep never saw a staged install adopted: {self:?}"
            );
            // A refused server answers from no store, and a leader that had
            // matched entries on the lost one forgets them (D-042):
            // with refusals seen, so is the reset.
            assert!(
                self.progress_resets > 0,
                "no leader ever forgot a re-seeded follower's progress: {self:?}"
            );
        }
        assert!(
            self.completed > 100 * self.seeds,
            "too few operations to mean much: {self:?}"
        );
    }
}

// --- The membership scenario (SPEC §3, RAFT.md §1, stage D) ---

use std::collections::BTreeMap;

use ananke_sim::membership;

/// Two membership runs with the same seed produce byte-identical traces: the
/// driver's decisions are functions of the trace and the seed alone.
#[test]
fn the_membership_scenario_has_byte_identical_traces_for_one_seed() {
    let first = membership::run(7, Variant::Correct);
    let second = membership::run(7, Variant::Correct);
    assert_eq!(first.jsonl().as_bytes(), second.jsonl().as_bytes());
}

/// The positive control: the correct server passes 3 → 5 → 3 under partition on
/// every seed, and the runs reached the states that matter. On every seed a server
/// joining the configuration is fed a snapshot while it is a learner (issue #46), which
/// the sweep asserts seed by seed; a store refused for anything but lost state fails
/// the run's own check.
#[test]
fn the_correct_server_passes_the_membership_scenario_on_every_seed() {
    let coverage = Mutex::new(MembershipCoverage::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = membership::run(seed, Variant::Correct);
        coverage.lock().unwrap().add(&report);
        report
            .check()
            // PROPOSED(D-058): a joining server fed by snapshot, on every seed.
            .and_then(|()| {
                if report.snapshot_fed_joiners().is_empty() {
                    Err(format!(
                        "seed {seed}: no server joining the configuration installed a snapshot \
                         in its learner phase"
                    ))
                } else {
                    Ok(())
                }
            })
            .inspect_err(|_| write_trace(&format!("membership-{seed}"), &report.jsonl()))
    });
    let coverage = coverage.into_inner().unwrap();
    eprintln!("Membership: {coverage:?}");
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete(seeds());
}

/// The negative control: a server that counts one merged majority while joint
/// (thesis §4.3) is caught by the membership scenario's checks on some seed. How many
/// of its runs fed a joining server a snapshot is printed beside the rate.
#[test]
fn a_server_that_counts_one_majority_in_joint_consensus_is_caught() {
    let outcomes: Vec<(Option<String>, bool)> = sweep(seeds(), |seed| {
        let report = membership::run(seed, Variant::SingleMajorityInJointConsensus);
        (
            report.check().err(),
            !report.snapshot_fed_joiners().is_empty(),
        )
    });
    let fed = outcomes.iter().filter(|(_, fed)| *fed).count();
    let caught: Vec<String> = outcomes.into_iter().filter_map(|(v, _)| v).collect();
    eprintln!(
        "SingleMajorityInJointConsensus: caught on {} of {} seeds, a joining server fed a \
         snapshot on {fed}, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        !caught.is_empty(),
        "SingleMajorityInJointConsensus was never caught"
    );
}

/// What the correct server's membership runs saw.
#[derive(Debug, Default)]
struct MembershipCoverage {
    seeds: u64,
    uniform_seeds: u64,
    grows_completed: u64,
    shrinks_completed: u64,
    joint_configs_taken: usize,
    new_configs_taken: usize,
    learners_promoted: usize,
    config_reverts: usize,
    elections_while_joint: usize,
    step_downs_outside_new: usize,
    partitions: usize,
    completed: u64,
    abandoned: u64,
    redirected: u64,
    worst_completion_gap: Duration,
    slowest_write_after_heal: Duration,
    // PROPOSED(D-058): snapshots installed by joining servers in their learner phase,
    // the seeds that had one, the leaders' compactions, and the seeds whose operator
    // found no compacted leader in time and the longest wait for one.
    snapshot_fed_joiners: usize,
    seeds_with_a_snapshot_fed_joiner: u64,
    compactions: usize,
    seeds_with_a_compaction_fallback: u64,
    longest_compaction_wait: Duration,
    // PROPOSED(D-058): adoptions, and refusals by the reason's first clause, of which
    // only lost state may appear: a configuration key an install's repair wrote out of
    // step with its log refuses the store at the adoption's open.
    adoptions: usize,
    refusals: BTreeMap<String, usize>,
    // PROPOSED(D-058): reverts that restore a server's compacted or installed prefix's
    // configuration, and of those the ones a truncation in the running core made (the
    // core's revert floor); installs that kept a tail of the receiver's log, and those
    // whose tail carried a configuration entry (the key repair's non-trivial branch).
    reverts_to_a_prefix: usize,
    truncation_reverts_to_a_prefix: usize,
    installs_keeping_a_tail: usize,
    installs_whose_tail_carries_a_configuration: usize,
}

impl MembershipCoverage {
    fn add(&mut self, report: &membership::Report) {
        self.seeds += 1;
        let joiners = report.snapshot_fed_joiners();
        self.snapshot_fed_joiners += joiners.len();
        self.seeds_with_a_snapshot_fed_joiner += u64::from(!joiners.is_empty());
        self.compactions += report.count(|e| matches!(e, TraceEvent::RaftCompacted { .. }));
        self.seeds_with_a_compaction_fallback += u64::from(report.compaction_fallbacks > 0);
        self.longest_compaction_wait = self.longest_compaction_wait.max(report.compaction_waited);
        self.uniform_seeds += u64::from(report.uniform());
        self.grows_completed += u64::from(report.grow_completed);
        self.shrinks_completed += u64::from(report.shrink_completed);
        self.partitions += report.partitions.len();
        self.completed += report.clients.completed;
        self.abandoned += report.clients.abandoned;
        self.redirected += report.clients.redirected;
        if report.uniform() {
            if let Some(gap) = report.longest_completion_gap() {
                self.worst_completion_gap = self.worst_completion_gap.max(gap);
            }
            if let Some(took) = report.time_to_write_after_heal() {
                self.slowest_write_after_heal = self.slowest_write_after_heal.max(took);
            }
        }
        // The configurations each server had in force, and who led, over the run.
        let mut in_force: BTreeMap<u64, (u64, bool)> = BTreeMap::new();
        let mut leading: BTreeSet<u64> = BTreeSet::new();
        let mut promoted: BTreeSet<(u64, u64)> = BTreeSet::new();
        // PROPOSED(D-058): per server, its compacted or installed prefix, the kind of its
        // last Raft record, and, from an adoption to its recovery, the restated
        // snapshot's index.
        let mut prefix: BTreeMap<u64, u64> = BTreeMap::new();
        let mut after_truncate: BTreeSet<u64> = BTreeSet::new();
        let mut restating: BTreeMap<u64, Option<u64>> = BTreeMap::new();
        for event in report.events() {
            let truncated = match &event {
                TraceEvent::RaftTruncate { server, .. } => Some((*server, true)),
                TraceEvent::RaftSnapshot { server, .. }
                | TraceEvent::RaftAppend { server, .. }
                | TraceEvent::RaftConfig { server, .. }
                | TraceEvent::RaftCompacted { server, .. }
                | TraceEvent::RaftAdopted { server }
                | TraceEvent::RaftRecovered { server, .. } => Some((*server, false)),
                _ => None,
            };
            let followed_truncate = truncated.is_some_and(|(s, _)| after_truncate.contains(&s));
            match truncated {
                Some((s, true)) => {
                    after_truncate.insert(s);
                }
                Some((s, false)) => {
                    after_truncate.remove(&s);
                }
                None => {}
            }
            match event {
                TraceEvent::RaftCompacted { server, through } => {
                    prefix.insert(server, through);
                }
                TraceEvent::RaftSnapshot {
                    server,
                    last_index,
                    taken: false,
                    ..
                } => {
                    if followed_truncate && let Some(slot) = restating.get_mut(&server) {
                        *slot = Some(last_index);
                    }
                    prefix.insert(server, last_index);
                }
                TraceEvent::RaftAdopted { server } => {
                    self.adoptions += 1;
                    restating.insert(server, None);
                }
                TraceEvent::RaftRecovered {
                    server,
                    applied,
                    last_index,
                    ..
                } => {
                    if restating.remove(&server).is_some() && last_index > applied {
                        self.installs_keeping_a_tail += 1;
                    }
                }
                TraceEvent::RaftRefused { reason, .. } => {
                    let clause = reason.split(':').next().unwrap_or_default().to_owned();
                    *self.refusals.entry(clause).or_default() += 1;
                }
                TraceEvent::RaftConfig {
                    server,
                    index,
                    old,
                    new,
                    joint,
                    ..
                } => {
                    if joint {
                        self.joint_configs_taken += 1;
                        for id in &new {
                            if !old.contains(id) {
                                promoted.insert((index, *id));
                            }
                        }
                    } else if index > 0 {
                        self.new_configs_taken += 1;
                        if leading.contains(&server) && !old.contains(&server) {
                            self.step_downs_outside_new += 1;
                        }
                    }
                    if let Some(&(previous, _)) = in_force.get(&server)
                        && index < previous
                    {
                        self.config_reverts += 1;
                        if index > 0 && prefix.get(&server) == Some(&index) {
                            self.reverts_to_a_prefix += 1;
                            self.truncation_reverts_to_a_prefix += usize::from(followed_truncate);
                        }
                    }
                    if let Some(Some(snapshot)) = restating.get(&server)
                        && index > *snapshot
                    {
                        self.installs_whose_tail_carries_a_configuration += 1;
                    }
                    in_force.insert(server, (index, joint));
                }
                TraceEvent::RaftTerm { server, role, .. } => {
                    if role == "leader" {
                        leading.insert(server);
                    } else {
                        leading.remove(&server);
                    }
                }
                TraceEvent::RaftLeader { server, .. } => {
                    leading.insert(server);
                    if in_force.get(&server).is_some_and(|&(_, joint)| joint) {
                        self.elections_while_joint += 1;
                    }
                }
                _ => {}
            }
        }
        self.learners_promoted += promoted.len();
    }

    fn assert_complete(&self, seeds: u64) {
        for (what, seen) in [
            ("grow completions", self.grows_completed),
            ("shrink completions", self.shrinks_completed),
            (
                "joint configurations taken",
                self.joint_configs_taken as u64,
            ),
            ("new configurations taken", self.new_configs_taken as u64),
            ("learners promoted", self.learners_promoted as u64),
            ("partitions", self.partitions as u64),
            ("completed operations", self.completed),
            ("uniformly scheduled seeds", self.uniform_seeds),
            ("log compactions", self.compactions as u64),
        ] {
            assert!(seen > 0, "the membership runs never saw {what}: {self:?}");
        }
        assert_eq!(
            self.seeds_with_a_snapshot_fed_joiner, seeds,
            "a membership run fed no joining server a snapshot in its learner phase: {self:?}"
        );
        // PROPOSED(D-058): every seed adopts installs, and none is refused for anything
        // but lost state, which each run's check also fails.
        assert!(self.adoptions > 0, "no install was adopted: {self:?}");
        assert!(
            self.refusals
                .keys()
                .all(|clause| clause.starts_with(LOST_STATE)),
            "a membership run refused a store for something other than lost state: {self:?}"
        );
        // Rarer states need the partition to land inside a narrow phase of the
        // change: twenty seeds cannot promise them; a hundred can.
        if seeds >= 100 {
            for (what, seen) in [
                ("elections while joint", self.elections_while_joint as u64),
                (
                    "step-downs of a leader outside C_new",
                    self.step_downs_outside_new as u64,
                ),
                ("configuration reverts", self.config_reverts as u64),
                // PROPOSED(D-058): an install whose snapshot's configuration is older
                // than the receiver's, taking the receiver back to the installed prefix.
                (
                    "reverts to a compacted or installed prefix",
                    self.reverts_to_a_prefix as u64,
                ),
            ] {
                assert!(seen > 0, "the membership runs never saw {what}: {self:?}");
            }
        }
    }
}

/// How many seeds the incremental checker is compared over: the hundred the owner
/// asked for at CI's tier and above, and the gate's twenty at the gate, which is a
/// twentieth more runs than the gate's raft sweeps already do (D-046).
fn compared_seeds() -> u64 {
    seeds().min(100)
}

/// The servers the comparison runs, one per seed in turn: the correct one, and
/// three known-buggy ones whose violations three different checks report, so that
/// the comparison sees `Err` verdicts and the words of their messages and not only
/// `Ok`.
const COMPARED: [Variant; 4] = [
    Variant::Correct,
    Variant::TruncateOnEveryAppend,
    Variant::SendBeforePersist,
    Variant::CountOlderTermForCommit,
];

/// How many prefixes of a run's trace the two are compared over, the whole trace
/// being the last of them.
const PREFIXES: usize = 8;

/// How many events are pushed into the incremental checker at a time: a prime, so
/// that no prefix the comparison looks at is a boundary the checker was fed on.
const CHUNK: usize = 37;

/// One run's comparison: at every prefix, the verdict of a checker fed the trace in
/// chunks against the verdict of the folds over that whole prefix from the first
/// record — the same `Ok` or `Err` and, when `Err`, the same words. `Ok(true)` if
/// some prefix was in violation, so the sweep can say the comparison saw one.
fn compare(seed: u64, variant: Variant, events: &[TraceEvent]) -> Result<bool, String> {
    let servers = raft::SERVERS as usize;
    let mut checker = Checker::new(servers);
    let mut fed = 0;
    let mut violated = false;
    for step in 1..=PREFIXES {
        let stop = events.len() * step / PREFIXES;
        while fed < stop {
            let next = (fed + CHUNK).min(stop);
            checker.extend(&events[fed..next]);
            fed = next;
        }
        let incremental = checker.verdict();
        let whole = invariants::all(&events[..stop])
            .and_then(|()| invariants::commit_majority(&events[..stop], servers));
        violated |= whole.is_err();
        if incremental != whole {
            return Err(format!(
                "seed {seed}: under {variant:?}, over the first {stop} of {} records, the incremental checker said {incremental:?} and the fold over the whole prefix said {whole:?}",
                events.len()
            ));
        }
    }
    Ok(violated)
}

/// The equivalence the sweep's incremental checking rests on (issue #25,
/// D-046): a checker fed a run's records in chunks as they arrive says exactly what
/// the folds say over the whole trace from the first record — the same verdict, and
/// when it is a violation, the same message, at every prefix and on every seed. The
/// sweep stops a run at the first violation and the pinned seeds assert fragments of
/// these messages, so a checker that agreed only on `Ok` would be no checker at all;
/// a quarter of the seeds run each known-buggy server that the checks catch
/// directly, and the count of prefixes found in violation is printed so a run that
/// compared nothing but `Ok` is visible.
#[test]
fn the_incremental_checker_agrees_with_the_fold_over_the_whole_trace() {
    let compared = compared_seeds();
    let outcomes = sweep(compared, |seed| {
        let variant = COMPARED[seed as usize % COMPARED.len()];
        compare(seed, variant, &raft::run(seed, variant).events())
    });
    let violated = outcomes.iter().filter(|o| matches!(o, Ok(true))).count();
    eprintln!(
        "Incremental checker: {compared} seeds compared at {PREFIXES} prefixes each, {violated} of them with a violation to agree on"
    );
    let verdicts: Vec<Result<(), String>> = outcomes.into_iter().map(|o| o.map(|_| ())).collect();
    if let Err(mismatch) = verdict(&verdicts) {
        panic!("{mismatch}");
    }
    assert!(
        violated > 0,
        "no compared seed reached a violation: the comparison saw only Ok verdicts"
    );
}

// --- The check-quorum re-seed scenario (RAFT.md §1 and §3, D-049) ---

use ananke_sim::quorum::{self, Disk, Half};

/// One half of the re-seed scenario over the sweep's seeds under `variant`: every
/// seed's violation, if it has one, and the figures the tests print.
fn quorum_sweep(variant: Variant, half: Half) -> (Vec<String>, QuorumFigures) {
    let figures = Mutex::new(QuorumFigures::default());
    let violations: Vec<String> = sweep(seeds(), |seed| {
        let report = quorum::run(seed, variant, half);
        figures.lock().unwrap().add(&report);
        report.check().err().inspect(|_| {
            if variant == Variant::Correct {
                write_trace(&format!("quorum-{half:?}-{seed}"), &report.jsonl());
            }
        })
    })
    .into_iter()
    .flatten()
    .collect();
    (violations, figures.into_inner().unwrap())
}

/// What one half of the re-seed scenario saw over a sweep, in check-quorum windows
/// of the leader's clock where it is a time.
#[derive(Debug, Default)]
struct QuorumFigures {
    seeds: u64,
    step_downs: u64,
    step_downs_naming_the_refused: u64,
    slowest_step_down_windows: f64,
    reseeded: u64,
    slowest_reseed_windows: f64,
    commits_after_install: u64,
    slowest_commit_windows: f64,
    chunk_acks: usize,
    refused_rejections: usize,
    oversized: usize,
}

impl QuorumFigures {
    fn add(&mut self, report: &quorum::Report) {
        self.seeds += 1;
        let (Some(cast), Some(hold), Some((cut, _))) = (report.cast, report.hold(), report.cut)
        else {
            return;
        };
        let window = report.global(cast.leader, raft::ELECTION_MIN).as_secs_f64();
        let windows = |at: ananke_env::Instant| at.duration_since(cut).as_secs_f64() / window;
        if let Some((at, uncounted)) = &hold.quorum_lost {
            self.step_downs += 1;
            self.step_downs_naming_the_refused += u64::from(uncounted.contains(&cast.refused));
            self.slowest_step_down_windows = self.slowest_step_down_windows.max(windows(*at));
        }
        if let Some(at) = hold.reseeded {
            self.reseeded += 1;
            self.slowest_reseed_windows = self.slowest_reseed_windows.max(windows(at));
        }
        if let Some(at) = hold.commit_after_install {
            self.commits_after_install += 1;
            self.slowest_commit_windows = self.slowest_commit_windows.max(windows(at));
        }
        self.chunk_acks += hold.chunk_acks;
        self.refused_rejections += hold.refused_rejections;
        self.oversized += hold.oversized;
    }
}

/// The directed scenario's positive control (D-049): on every seed, with a
/// follower refused and being re-seeded and the leader's other follower cut off,
/// the correct leader steps down within two check-quorum windows of its re-seed
/// stream being blocked, naming the refused follower as the answer it did not
/// count; and with the stream open it keeps its office through the install and
/// commits through the re-seeded follower after it.
#[test]
fn the_correct_leader_steps_down_on_a_blocked_reseed_and_commits_through_an_open_one() {
    for half in [Half::Blocked, Half::Open] {
        let (violations, figures) = quorum_sweep(Variant::Correct, half);
        eprintln!(
            "Correct {half:?}: {} of {} seeds failed, {figures:?}, first: {}",
            violations.len(),
            seeds(),
            violations.first().map_or("", String::as_str)
        );
        assert!(violations.is_empty(), "{}", violations[0]);
        match half {
            Half::Blocked => assert_eq!(figures.step_downs_naming_the_refused, figures.seeds),
            Half::Open => assert_eq!(figures.commits_after_install, figures.seeds),
        }
    }
}

/// The leader as built (`RefusedCountsForQuorum`, D-049): a refused
/// follower's rejections count for check quorum whatever becomes of the re-seed
/// stream to it, so with the stream blocked and the other follower cut off the
/// leader keeps its office for the whole hold, where the correct leader above steps
/// down within two windows. The open half asks nothing of it that the correct
/// leader's does not already show, so only the blocked half runs.
#[test]
fn a_leader_that_counts_a_refused_followers_rejections_whatever_its_stream_does_is_caught() {
    let (caught, figures) = quorum_sweep(Variant::RefusedCountsForQuorum, Half::Blocked);
    eprintln!(
        "{:?} Blocked: caught on {} of {} seeds, {figures:?}, first: {}",
        Variants::from(Variant::RefusedCountsForQuorum),
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        figures.oversized > 0,
        "no chunk was lost to the limit: the block was not injected"
    );
    assert_eq!(
        caught.len() as u64,
        seeds(),
        "RefusedCountsForQuorum was not caught on every seed: {caught:?}"
    );
    assert!(
        caught
            .iter()
            .all(|v| v.contains(": check quorum: ") && v.contains("kept its office")),
        "caught by something other than the kept office: {caught:?}"
    );
}

/// The rejected alternative (`RefusedNeverCounts`, D-049): nothing from a
/// refused follower counts for check quorum, so with the stream open and the other
/// follower cut off the leader steps down mid-re-seed, and with the re-seeded server
/// never voting (D-035) nothing commits after the install, where the correct leader
/// above keeps its office and commits. Only the open half runs: blocked, this leader
/// steps down as the correct one does.
#[test]
fn a_leader_that_counts_nothing_from_a_refused_follower_is_caught() {
    let (caught, figures) = quorum_sweep(Variant::RefusedNeverCounts, Half::Open);
    eprintln!(
        "{:?} Open: caught on {} of {} seeds, {figures:?}, first: {}",
        Variants::from(Variant::RefusedNeverCounts),
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert_eq!(
        caught.len() as u64,
        seeds(),
        "RefusedNeverCounts was not caught on every seed: {caught:?}"
    );
}

/// What the scenario's instant disk leaves out (D-049), measured rather than
/// assumed: on the sweep's disk a refused server's verification, repair and
/// adoption take over a check-quorum window, it answers nothing at all meanwhile,
/// and a leader whose majority needs it steps down in that silence whichever way
/// it counts a refused follower's rejections. So the open half is asked on the
/// instant disk, and here, on the sweep's, the leader as built — which never leaves
/// a rejection uncounted, as asserted — is asserted from a hundred seeds to lose
/// office in the silence on some seed, so the day the silence is gone this test
/// says so. The correct leader's figures are printed beside it: it loses
/// office on the same kind of seed, and a step-down naming the refused follower is
/// one where the stream itself stalled for a window on the disk. A failure with no
/// step-down is a seed whose re-seed did not finish within the hold on that disk,
/// printed with its seed.
#[test]
fn on_the_sweeps_disk_the_install_silence_deposes_the_leader_under_either_count() {
    for variant in [Variant::RefusedCountsForQuorum, Variant::Correct] {
        let outcomes: Vec<(u64, bool, Option<Vec<u64>>)> = sweep(seeds(), |seed| {
            let report = quorum::run_on(seed, variant, Half::Open, Disk::Sweep);
            let lost = report
                .hold()
                .and_then(|hold| hold.quorum_lost)
                .map(|(_, uncounted)| uncounted);
            (seed, report.check().is_err(), lost)
        });
        let failed: Vec<&(u64, bool, Option<Vec<u64>>)> =
            outcomes.iter().filter(|(_, f, _)| *f).collect();
        let silent = failed
            .iter()
            .filter(|(_, _, l)| l.as_ref().is_some_and(Vec::is_empty))
            .count();
        let named = failed
            .iter()
            .filter(|(_, _, l)| l.as_ref().is_some_and(|u| !u.is_empty()))
            .count();
        // A failure with no step-down: the leader kept its office and the re-seed
        // did not finish within the hold.
        let unfinished: Vec<u64> = failed
            .iter()
            .filter(|(_, _, l)| l.is_none())
            .map(|(seed, _, _)| *seed)
            .collect();
        eprintln!(
            "{:?} Open on the sweep's disk: failed on {} of {} seeds, {silent} by a step-down with nothing uncounted, {named} by one naming the refused follower, {} with no step-down {unfinished:?}",
            Variants::from(variant),
            failed.len(),
            seeds(),
            unfinished.len()
        );
        if variant == Variant::RefusedCountsForQuorum {
            assert_eq!(
                named, 0,
                "the leader as built named an uncounted follower: the variant was not injected"
            );
            if seeds() >= 100 {
                assert!(
                    silent > 0,
                    "the install silence deposed no leader: the instant disk may no longer be needed"
                );
            }
        }
    }
}

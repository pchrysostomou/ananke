//! The Phase 2 sweep (SPEC.md §3): the correct server holds every invariant of
//! RAFT.md §2 on every seed under the full network fault model, partitions, one-way
//! blocks and crashes with the disk model, and each known-buggy variant this stage
//! ships (RAFT.md §5) is caught on some seed. The catch rate of each is printed, so a
//! hundred-seed run reports it.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use ananke_env::sim::TraceRecord;
use ananke_env::{ClientOp, DropReason, Instant, TraceEvent};
use ananke_raft::core::{Variant, Variants};
use ananke_raft::invariants::{self, Checker};
use ananke_raft::store::{LOST_STATE, STORE_MARKER};
use ananke_sim::raft::DRIFT_BOUND_PPM;
use ananke_sim::raft::{self, Fault, Moved, RecordTime, TimerResets};
use ananke_sim::{seeds, sweep, traced, verdict, write_trace};

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
/// Today the seed does not reach that situation, and the test asserts so. Re-audited on
/// the tree with the key layout and the store's format record (D-059, D-060), which
/// moved every raft schedule again after D-056's send queue had: every start now reads
/// a record before the engine opens and every Raft key carries eight more bytes, so the
/// simulated disk's latency, torn-write and bit-rot draws move from the first start on,
/// and the run after it is another run. On it the timer replay that reads AppendEntries
/// alone as a leader's contact finds no gap at all: no follower goes past its bound, fed
/// by snapshot chunks or not, over four refusals — server 2's for lost state at
/// 6.405597155 s (tables 1 and 3 dropped) and server 3's three, from 8.66689086 s, for a
/// manifest its `CURRENT` names and that cannot be read and then twice on the store's
/// durable lost mark. The day [`raft::Report::snapshot_fed_timer_gaps`] is not empty,
/// the seed reaches the situation again and the pin should assert it: those gaps
/// present, and the check green.
///
/// With D-056's queue alone the replay also found no gap, over three refusals of server
/// 3. On the tree before the queue the fault list was the same through the 9.691 s
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
/// on the tree with the key layout and the store's format record (D-059, D-060), which
/// moved every raft schedule again (see seed 164's pin): the timer replay without
/// D-039's arm finds no gap anywhere on the run, so there is no stretch for a
/// restatement to rescue, and the run holds no refusal at all — no store on it is
/// damaged past its own recovery, over seven crashes and six isolations.
/// With the check green, [`raft::Report::timer_gaps_rescued_by_restatement`] is every
/// gap of that replay; the day it is not empty the seed reaches the situation again,
/// and the pin should assert it: those gaps present, and the check green.
///
/// With D-056's queue alone the replay also found no gap, over five refusals. On the
/// tree before the queue the partition isolated server 1 alone as the failing
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

/// Seed 2605, the ten-thousand-seed nightly's (GitHub run 35111624618,
/// `phase-3-stage-a` at 1a1cad2, PR #60): the window in which a server adopts a
/// completed install, which it spends with no core and no election timer, and
/// which the check was charging to the leader's last contact before it.
///
/// Server 3 heard the leader's last AppendEntries at 19.679704065 s. Its install of
/// snapshot 374 completed 24.655 ms later — decided at 19.704359292 s, durable at
/// 19.763649610 s — and that completion retired its incarnation (D-030: "the
/// completed install retires the incarnation and the next open adopts the staged
/// store"). The adoption that followed took 297.707 ms: `RaftAdopted` at
/// 19.880743071 s, the store's format record, the engine, the WAL. Only the
/// restatement at 20.002065925 s armed the next core's first tick. The check had
/// the server running throughout, so at 19.994418991 s — the first record past the
/// bound, 8.4 ms before the restatement — it flagged 314.715 ms against server 3's
/// 313.983572 ms bound and the nightly panicked. The fix is PROPOSED D-063, in the
/// check alone: a completed install takes the server out of the replay's running set
/// until its restatement puts it back, the same treatment a crash gets.
///
/// Re-audited under D-069, which moved every raft schedule that serves a read (the read
/// is served at one engine version now, an extra engine read on the path). **The seed no
/// longer reaches the window, and no seed of the first thousand does**, so there is
/// nowhere to move the pin and the test asserts the absence with its reason, as
/// D-056's re-audit did for the pair on seed 132. The search SHARD.md §12 asks for at
/// such a move was run over seeds 0..1000 in release: the replay without D-063's arm —
/// the check as it stood on 1a1cad2 — finds a gap holding a completed install on 0 of
/// them. On this seed it finds no gap at all, although the run holds 14 adoption
/// windows, so it is not that the seed stopped adopting: it is that no adoption's
/// window now overlaps a stretch the timer replay flags. The test asserts both, the
/// windows being there and the replay being empty, so neither half can go vacuous.
///
/// Re-audited again under D-078, which moves every schedule once more: a follower now
/// writes a snapshot record and deletes log keys where it wrote and deleted nothing, so
/// every simulated disk draw from the first compaction on is another draw and the run
/// after it is another run. The correct half is where it was — the run holds **20**
/// adoption windows now, the replay without D-063's arm still finds no gap at all, and
/// no adoption window overlaps a flagged stretch — so this pin keeps asserting that
/// absence with its reason, and both halves are still asserted so neither goes vacuous.
///
/// **The pair has come back.** `ResetTimerOnAnyRpc`, the variant the timer rule is
/// written for (RAFT.md §5, moirae rule 5), *is* caught here again: the run's majority
/// is up at its end now, so RAFT.md §2's carve-out (D-035) no longer withholds the
/// bound, and the replay's five gaps — none of them holding a completed install, so
/// D-063's arm exempts none — make the first of them the run's violation. The pin
/// asserts the catch, in the words the check reports it in, as CLAUDE.md:58-67 asks of
/// a pin whose mechanism is reachable again: the absence it asserted before D-078 was
/// the weaker statement and is gone. The variant's catch is asserted at every tier by
/// its own sweep, `a_server_that_resets_its_timer_on_any_message_is_caught`.
#[test]
// PROPOSED(D-063): a server adopting a completed install has no election timer.
fn seed_2605_which_the_nightly_found_is_an_adoption_window_and_still_catches_the_variant() {
    let report = raft::run(2605, Variant::Correct);
    report.check().unwrap();
    assert!(
        !report.adoption_windows().is_empty(),
        "seed 2605 adopts no install at all, so the absence below says nothing: re-audit the pin"
    );
    let gaps = report.timer_gaps_rescued_by_adoption();
    assert!(
        gaps.is_empty(),
        "seed 2605 reaches an adoption window inside a timer gap again: {gaps:?}; pin the \
         mechanism — that gap present, holding one completed install, and the check green"
    );
    assert_eq!(
        report.timer_gaps(TimerResets::WITHOUT_ADOPTION),
        Vec::new(),
        "the check as it stood on 1a1cad2 flags a stretch on seed 2605 again, which D-063's \
         arm may or may not exempt: re-audit the pin"
    );

    // The pair, a catch again since D-078: the majority is up at the run's end, so the
    // timer bound is asked of it, and the replay's first gap is the violation.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    let buggy = raft::run(2605, Variant::ResetTimerOnAnyRpc);
    assert_eq!(
        buggy.check(),
        Err(
            "seed 2605: timers: server 2's replica of range 2 heard from no leader of its term \
             and granted no vote since Instant(2.285324846s) and had not campaigned by \
             Instant(2.600034004s)"
                .to_owned()
        ),
        "seed 2605's catch of ResetTimerOnAnyRpc has moved: re-audit the pin"
    );
    assert!(
        buggy.majority_up(),
        "seed 2605 under ResetTimerOnAnyRpc no longer ends with its majority up, so the timer \
         bound is withheld from the run (D-035) and the catch above cannot be the reason: \
         re-audit the pin"
    );
    let gaps = buggy.timer_gaps(TimerResets::ALL);
    assert_eq!(
        gaps.len(),
        5,
        "seed 2605's replay no longer finds the variant out five times over: {gaps:?}; \
         re-audit the pin"
    );
    assert!(
        gaps.iter().all(|gap| gap.adoptions == 0),
        "a gap of ResetTimerOnAnyRpc on seed 2605 now rests on a stretch holding a completed \
         install, which D-063 exempts: {gaps:?}"
    );
    assert_eq!(
        buggy.timer_gaps(TimerResets::WITHOUT_ADOPTION),
        gaps,
        "D-063's arm moved what the timer check finds under ResetTimerOnAnyRpc on seed 2605"
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
/// disagree. Re-audited on the tree with the key layout and the store's format record
/// (D-059, D-060), which moved every raft schedule again (see seed 164's pin): the
/// run's only refusal is server 1's at 6.396598792 s, for lost state (table 1
/// dropped), and every install on the run lands at or above the floor its receiver had
/// reached, which [`raft::Report::floor_lowering_installs`] says directly by being
/// empty. So the two floor rules agree at every event, the seed would pass the old
/// checker too, and this pin holds the seed green without exercising the fix. (With
/// D-056's queue alone the one refusal was server 2's at 12.575 s, floor 295, re-seeded
/// from 334; before the queue it was server 2's at 12.585 s for a missing log head,
/// floor 251, re-seeded from 312.) The day
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
/// install. What moved is the install it hit. Re-audited on the tree with the key
/// layout and the store's format record (D-059, D-060), which moved every raft schedule
/// again (see seed 164's pin): that crash lands at 5.812 s under both servers now, and
/// under each it comes *before* server 1's next adoption rather than after one — 691 ms
/// before the window that opens at 6.503404315 s under the correct server, and 712 ms
/// before the one at 6.523678468 s as built. No crash lands inside any of the correct
/// run's 6 adoption windows or the variant's 6: the nearest a crash comes to a window of
/// its own server is those 691 ms under the correct server and 686 ms as built (server
/// 2, crashed at 12.033 s, installing at 12.718711936 s), and the variant passes the
/// seed. (With D-056's queue alone the crash landed 247 ms and 126 ms after an adoption
/// closed, over 8 and 9 windows; before the queue, 262 ms and 230 ms after, over 12 and
/// 9.) The day
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
/// not. Re-audited on the tree with the key layout and the store's format record
/// (D-059, D-060), which moved every raft schedule again (see seed 164's pin): it
/// reaches D-042's reset on server 3, refused at 18.200834224 s on the marker its
/// lost-state refusal wrote, its progress reset by the leader at 18.206060773 s and
/// re-seeded at 18.492068093 s. (Before D-069's one-version read it was the same server
/// at 9.793751132 s; with D-056's queue alone it was server 1, refused at 7.025 s;
/// before the queue, server 3 at 18.696 s.) It does not
/// reach the wedge's shape: the run takes 29 snapshots and no two at one index, so no
/// re-take lands under a live stream to scramble it, and no follower goes uncounted
/// after the last heal.
#[test]
fn seed_5909_which_the_nightly_found_stays_green() {
    let report = raft::run(5909, Variant::Correct);
    report.check().unwrap();
    assert!(
        report.refusal_reset_reseed(3).is_some(),
        "seed 5909 no longer refuses server 3, resets the leader's progress for it and \
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
/// says. (The sentence here used to add "and seed 132 below agrees, where
/// `SharedSnapshotDir` alone fails exactly as the pair does". That was true of
/// the tree with D-056's send queue, where both were caught on seed 132; on this
/// tree neither is caught on it, which is what seed 132's own pin says, so the
/// clause is dropped rather than left contradicting the pin it cites.) The
/// variant set still makes the pair a run the sweep can ask about, and this seed
/// is asked.
///
/// No run of the seed has two uncounted followers. Re-audited under D-069, which moved
/// every raft schedule that serves a read (the read is served at one engine version now,
/// an extra engine read on the path), on top of the key layout and the store's format
/// record (D-059, D-060) before it (see seed 164's pin): D-042's half is reached under
/// `IgnoreIncarnation` alone and under the pair, on different servers, always
/// harmlessly, and the stream half is reached under neither. The test asserts each half
/// where it is reached and, with the reason, absent where not. No run of the seed takes
/// one index twice — the runs take 29, 29, 39 and 37 snapshots, each at a new index — so
/// no re-take can lie under a stream on any of them:
///
/// - under `IgnoreIncarnation` the leader's progress for server 3 goes stale — leader 2
///   of term 14 had 528 acknowledged, server 3 is refused at 18.193166069 s on the
///   marker its earlier lost-state refusal wrote, and of the leader's 373 probes after
///   that none goes below 528 while server 3 rejects 368 — and server 3 alone goes
///   uncounted after the last heal, which is one follower, not the wedge's two. (Before
///   D-069 the same half was reached on the same server and left nothing uncounted.)
/// - under `SharedSnapshotDir` nothing goes stale: its one refusal, of server 1 at
///   17.021205177 s, is reset by the leader and re-seeded at 17.490780068 s;
/// - under the pair the leader's progress for server 1 goes stale instead — leader 3 of
///   term 15 had 435 acknowledged and server 1 is refused at the same 17.021205177 s,
///   with 805 probes and 788 rejections after it — and server 1 alone goes uncounted
///   after the last heal.
///
/// (With D-056's queue alone only `IgnoreIncarnation` reached its half, on server 3 from
/// 9.503 s, and the pair reached neither. Before the queue each variant reached its own
/// half and the pair both, one after the other.)
///
/// Re-audited under D-078, which moves every schedule again — a follower writes a
/// snapshot record and deletes log keys where it wrote and deleted nothing, so every
/// disk draw from the first compaction on is another draw. The stream half is still out
/// of reach under every variant, and asserted so with its non-vacuity: 18 to 27 takes a
/// run, no index taken twice, nothing re-taken under a stream. D-042's half has moved
/// again: `IgnoreIncarnation` still leaves the leader's progress for server 3 stale, and
/// the pair now leaves nothing stale where it left server 1 stale before. **No variant
/// leaves anything uncounted after the last heal now** — the refusals land where the
/// leader's check-quorum window has already closed on them — so the uncounted set is
/// asserted empty everywhere, and the day one comes back the test says so and the pin
/// can be upgraded. The stale set is what still carries D-042's half here, and it is
/// asserted per variant, so a move either way is seen (CLAUDE.md:58-67).
// PROPOSED(D-078): a follower compacts its log to its own applied index.
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
        assert!(
            !takes.is_empty(),
            "seed 5909 under {variants:?} takes no snapshot at all: the claim below is \
             vacuous, so re-audit the pin and `snapshot_takes`"
        );
        assert!(
            !took_an_index_twice(&takes) && report.retakes_under_streams().is_empty(),
            "seed 5909 under {variants:?} takes an index twice again, so a re-take may lie \
             under a stream: re-audit the pin"
        );
        let stale: Vec<u64> = (1..=raft::SERVERS)
            .filter(|&s| report.stale_progress(s).is_some())
            .collect();
        let uncounted = report.uncounted_after_heal();
        // D-042's half, where each run reaches it: one refused follower's progress
        // left stale, and at most that one follower uncounted after the last heal.
        let (expected_stale, expected_uncounted): (&[u64], &[u64]) =
            if variants == Variants::from(Variant::IgnoreIncarnation) {
                (&[3], &[])
            } else {
                (&[], &[])
            };
        assert_eq!(
            stale, expected_stale,
            "seed 5909 under {variants:?} no longer leaves exactly {expected_stale:?} stale: \
             re-audit the pin"
        );
        assert_eq!(
            uncounted,
            expected_uncounted
                .iter()
                .copied()
                .collect::<BTreeSet<u64>>(),
            "seed 5909 under {variants:?} no longer leaves exactly {expected_uncounted:?} \
             uncounted after the last heal: re-audit the pin"
        );
    }
}

/// The widest set of operation windows open at one instant, on the operations of
/// one key: how much concurrency the linearizability search must order there. A
/// pending operation is open to the end of the history.
// PROPOSED(D-080): the read-only candidates go first, together.
fn most_open_at_once(ops: &[&ananke_sim::lin::Op]) -> usize {
    let last = ops.iter().filter_map(|op| op.ret).max();
    let mut edges: Vec<(Instant, i64)> = Vec::new();
    for op in ops {
        edges.push((op.call, 1));
        edges.push((op.ret.or(last).unwrap_or(op.call), -1));
    }
    edges.sort();
    let (mut open, mut most) = (0i64, 0i64);
    for (_, step) in edges {
        open += step;
        most = most.max(open);
    }
    usize::try_from(most).expect("a count of operations")
}

/// The ten-thousand-seed nightly's seeds 3085 and 4065 (GitHub run 35705563274, on
/// 7127745): the first correct-server failures that were not violations at all. The
/// checker reported its own search budget — "332 of 397 operations placed before
/// the search budget ran out" on 3085's key `"k1"`, 376 of 438 on 4065's `"k0"` —
/// and both histories are in fact linearizable (issue #82).
///
/// The search branched over every candidate. A window of *w* concurrent reads of
/// one value is then 2^w different sets of linearized operations, all at the same
/// register value, and the memo cannot collapse them because they are genuinely
/// different sets. 3085's `"k1"` holds 397 operations, 199 of them gets, 20 windows
/// open at once at the widest; 4065's `"k0"` holds 438, 244 gets, 22 open. Measured
/// on 7127745, the search expanded 296 345 and 287 535 states before the 2 000 000
/// budget ran out; a throwaway copy with the budget raised decided 3085 at
/// 4 000 000 states and 4065 at 400 000 000. D-080 commits every read-only
/// candidate outright, keeping no branch point, and the two decide at 383 and 408
/// states — beside a worst key of 778 over seeds 0..1000.
///
/// The pin asserts the mechanism and not the green: each key must still hold the
/// window of concurrent reads that the reduction is what carries. The day a
/// schedule moves that window away the assertion says so, and the pin should be
/// re-audited against a seed that still reaches it rather than quietly kept.
#[test]
fn seeds_3085_and_4065_which_the_nightly_found_linearize_inside_the_budget() {
    for (seed, key, least_ops, least_gets, least_open) in [
        (3085u64, "k1", 300usize, 150usize, 12usize),
        (4065, "k0", 300, 150, 12),
    ] {
        let report = raft::run(seed, Variant::Correct);
        let ops: Vec<&ananke_sim::lin::Op> = report
            .history
            .ops
            .iter()
            .filter(|op| op.op.key().as_ref() == key.as_bytes())
            .collect();
        let gets = ops
            .iter()
            .filter(|op| matches!(op.op, ClientOp::Get { .. }))
            .count();
        let open = most_open_at_once(&ops);
        assert!(
            ops.len() >= least_ops && gets >= least_gets && open >= least_open,
            "seed {seed} no longer reaches the shape D-080's reduction decides: key {key:?} \
             holds {} operations, {gets} of them gets, {open} open at once, against the \
             {least_ops}/{least_gets}/{least_open} the pin was taken at; re-audit it against \
             a seed that does",
            ops.len()
        );
        report.check().unwrap();
    }
}

/// Whether some server took a snapshot at an index it had already taken: the
/// re-take D-043's variant rewrites one directory for, read from the takes the
/// fold paired rather than from the raw records (`retook_at_one_index` below
/// reads those, and the two agree).
fn took_an_index_twice(takes: &[raft::SnapshotTake]) -> bool {
    takes.iter().enumerate().any(|(n, take)| {
        takes[..n]
            .iter()
            .any(|earlier| earlier.server == take.server && earlier.index == take.index)
    })
}

/// Every store refusal a run traced. The pins that assert an *absence* of refusals
/// share this matcher, so that a slip narrowing it — a `server: 0` in the pattern, say,
/// where server ids run from 1 — cannot make one pin's absence assertion vacuous
/// without failing the non-vacuity assertion the other makes on the same matcher
/// (seed 132 below, where `IgnoreIncarnation` and the correct server do refuse).
fn refusals(report: &raft::Report) -> Vec<&TraceEvent> {
    report
        .records
        .iter()
        .map(|r| &r.event)
        .filter(|e| matches!(e, TraceEvent::RaftRefused { .. }))
        .collect()
}

/// The shape of seed 5909's wedge, asserted absent: no re-take lands under a live
/// stream that the follower then never installs, and the leader in force at the
/// last heal has at most one follower it could never count after it. Since
/// D-060's re-audit of `snapshot_takes` this is asserting something on every
/// seed: on the thousand, `SharedSnapshotDir` re-takes under a live stream on 180
/// and never installs after on 135 of them, against the correct server's 0.
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

/// Seed 132, which pinned the combined variant on the tree with D-056's send queue
/// alone, and what it does now that the key layout and the store's format record
/// (D-059, D-060) have moved every raft schedule again (see seed 164's pin): nothing.
/// The pair passes it, so does each half alone, and so does the correct server.
///
/// **No seed of the first thousand catches the pair on this tree, or either half of
/// it.** The search SHARD.md §12 asks for at such a move was run again over seeds
/// 0..1000 in release: `{IgnoreIncarnation, SharedSnapshotDir}` is caught on 0 of 1000,
/// `SharedSnapshotDir` alone on 0, `IgnoreIncarnation` alone on 0, and the correct
/// server on 0. So there is no seed to move this pin to, and the test asserts the
/// absence on the seed that held it, with the reason. `SharedSnapshotDir` asserts its
/// liveness catch only at the nightly's ten thousand (D-043, D-047, RAFT.md §5), which
/// is where the catch is asked for; its firing is asserted at every tier and is
/// undisturbed — a re-take at an index already taken on 525 of the thousand, and the
/// aimed arm reaching its stream on 153.
///
/// Why this seed reaches nothing: **no run of it takes any index twice**, so no
/// re-take can land under a live stream and the stream half's whole mechanism is out of
/// reach. It is not that nothing is taken — the runs take 32, 30, 35 and 36 snapshots —
/// but that each take's applied index is a
/// new one, so the variant's one-directory-per-index rewrite never lands on a
/// directory a stream is reading. (The pin said "takes no snapshot at all" until the
/// re-audit of `snapshot_takes` below: the fold it asked paired a take's record with a
/// checkpoint written at the same instant, and D-060 put an awaited write between the
/// two, so it answered empty everywhere. The assertion is now on the reason that is
/// true, and asserts it is not vacuous.)
///
/// Re-audited under D-069, which moved every raft schedule that serves a read (the read
/// is served at one engine version now, an extra engine read on the path). Every run of
/// the seed refuses server 2 at least once, where before D-069 the pair and the stream
/// half refused nothing, so `IgnoreIncarnation` has something to ignore here and the
/// pair's trace is no longer the stream half's record for record. D-042's half is
/// reached under the pair and only there, and harmlessly: leader 3 of term 13 had 335
/// acknowledged when server 2 was refused at 13.750549431 s, and of its 806 probes after
/// that 778 are rejected and none accepted, so server 2 is the one follower left
/// uncounted after the last heal. One uncounted follower is not the wedge, which needs
/// both. Under `SharedSnapshotDir` alone the same refusal at the same instant is reset
/// by the leader and re-seeded at 14.064495061 s; under `IgnoreIncarnation` alone (one
/// refusal, table 11 dropped) and under the correct server (two, tables 11 and 43)
/// nothing goes stale and nothing is uncounted. The test asserts each run's stale set
/// and uncounted set, and the fix — refusal, reset, re-seed — where the leader keeps
/// D-042's rule.
///
/// (With D-056's queue alone the pair was caught on 2 of 1000, seeds 132 and 848, both
/// by the liveness check, with `SharedSnapshotDir` alone caught on the same two with the
/// same messages; seed 132's wedge was eleven re-takes into `/raft/snap-220`, five of
/// them under live streams, and the duplicate-file loop D-043 names. Seed 680 held the
/// pin before the queue. Nor was a wedge that needs both ever seen: seed 5909's was
/// D-043's alone, above.)
#[test]
fn seed_132_which_pinned_the_combined_variant_before_the_layout_reaches_no_wedge() {
    let paired = raft::run(
        132,
        Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]),
    );
    let stream = raft::run(132, Variant::SharedSnapshotDir);
    let incarnation = raft::run(132, Variant::IgnoreIncarnation);
    let correct = raft::run(132, Variant::Correct);
    for report in [&paired, &stream, &incarnation, &correct] {
        assert_eq!(
            report.check().err(),
            None,
            "seed 132 under {:?} is caught again: re-audit the pin, and whether the combined \
             variant should be pinned here once more",
            report.variants
        );
        // PROPOSED(D-078): the reason the stream half is out of reach has moved down
        // one step. An index *is* taken twice now, under the runs that carry
        // `SharedSnapshotDir` and only those — that is the variant doing its work, one
        // mutable directory per index — but no such re-take lands under a live stream,
        // which is the second half the wedge needs. `retakes_under_streams` says that
        // directly, so this is a stronger statement than the one it replaces, and it is
        // asserted with its non-vacuity: a fold that found no take at all would pass it
        // silently (D-060's re-audit).
        let takes = report.snapshot_takes();
        assert!(
            !takes.is_empty(),
            "seed 132 under {:?} takes no snapshot at all, so the claims below say nothing: \
             re-audit the pin and `snapshot_takes`",
            report.variants
        );
        assert_eq!(
            took_an_index_twice(&takes),
            report.variants.contains(Variant::SharedSnapshotDir),
            "seed 132 under {:?} no longer takes an index twice exactly when it shares one \
             directory per index: re-audit the pin",
            report.variants
        );
        assert!(
            report.retakes_under_streams().is_empty(),
            "seed 132 under {:?} re-takes under a live stream again ({:?}), so the wedge's \
             second half is reachable here: re-audit the pin",
            report.variants,
            report.retakes_under_streams()
        );
        assert_no_stream_wedge(report);
    }
    // PROPOSED(D-078): the refusals have moved with the schedules. The correct server
    // and `IgnoreIncarnation` still refuse a store here; the two runs that share one
    // snapshot directory no longer refuse any, so `IgnoreIncarnation` has nothing to
    // ignore under the pair and D-042's half is reached under `IgnoreIncarnation`
    // alone, on server 2, where it was reached under the pair before. Nothing is left
    // uncounted after the last heal on any run now: the refusal here outlives no
    // check-quorum window. Each run's stale set, uncounted set and refusal count is
    // asserted, so a move either way is seen (CLAUDE.md:58-67), and the stale sets of
    // the runs that refuse nothing are asserted empty *for that reason*, not silently.
    for report in [&paired, &stream, &incarnation, &correct] {
        let refuses = !refusals(report).is_empty();
        assert_eq!(
            refuses,
            !report.variants.contains(Variant::SharedSnapshotDir),
            "seed 132 under {:?} no longer refuses a store exactly when it does not share one \
             snapshot directory ({:?}): re-audit the pin and the refusal matcher",
            report.variants,
            refusals(report)
        );
        let stale: Vec<u64> = (1..=raft::SERVERS)
            .filter(|&s| report.stale_progress(s).is_some())
            .collect();
        let expected: &[u64] = if report.variants == Variants::from(Variant::IgnoreIncarnation) {
            &[2]
        } else {
            &[]
        };
        assert_eq!(
            stale, expected,
            "seed 132 under {:?} no longer leaves exactly {expected:?} stale: re-audit the pin",
            report.variants
        );
        assert_eq!(
            report.uncounted_after_heal(),
            expected.iter().copied().collect::<BTreeSet<u64>>(),
            "seed 132 under {:?} no longer leaves exactly {expected:?} uncounted after the \
             last heal: re-audit the pin",
            report.variants
        );
    }
    // And the fix, where the leader is the correct one: the refusal, the leader's reset
    // and the re-seed, on the run whose leader keeps D-042's rule and whose refusal it
    // outlives. `SharedSnapshotDir` refuses nothing here any more, so it is not asked.
    assert!(
        correct.refusal_reset_reseed(2).is_some(),
        "seed 132 under the correct server no longer refuses server 2, resets the leader's \
         progress for it and re-seeds it: re-audit the pin"
    );
}

/// Seed 680, which pinned the combined variant before D-056's send queue moved every
/// schedule, and what it does now, re-audited again on the tree with the key layout and
/// the store's format record (D-059, D-060), which moved every raft schedule once more
/// (see seed 164's pin): the pair, each half alone and the correct server all pass it,
/// and no run leaves both followers uncounted after the last heal, which the test
/// asserts, so the day the seed wedges again it says so.
///
/// The stream half's *shape* is reached here, and the test asserts it rather than
/// assumes its absence. Under `SharedSnapshotDir` and under the pair the seed takes 73
/// snapshots into 36 directories: 37 of them are at an index that server had already
/// taken — the variant's one directory per index, rewritten in place — and 7 of those
/// land under a live stream of that index to a follower, the first at 9.105796790 s
/// into `/raft/snap-152` under the stream to server 2 opened at 9.100938503 s. Every
/// one of the 7 is a stream the follower still installs afterwards, so none is the
/// wedge, which needs the stream never to complete. That is what
/// `assert_no_stream_wedge` asserts, and on this seed it is asserting something rather
/// than nothing. Under `IgnoreIncarnation` alone and under the correct server no index
/// is taken twice at all (37 and 42 takes, each into its own version directory), so
/// `SharedSnapshotDir` alone otherwise behaves as the correct server does on the only
/// refusal the seed has: server 3, refused at 17.852606007 s for lost state (table 11
/// dropped), has the leader's progress for it reset at 17.857397623 s and is re-seeded
/// at 18.117876106 s.
///
/// (The pin said "no run of this seed takes a snapshot at all" until D-060's re-audit
/// of `snapshot_takes`: the fold paired a take's record with a checkpoint written at
/// the same recorded instant, and D-060's checkpoint format record put an awaited write
/// between the two, so the fold answered empty on every seed and the assertion could
/// not fail. The mechanism was there all along.)
///
/// D-042's half is reached under the pair, and only there. Its leader, carrying
/// `IgnoreIncarnation`, does not reset: leader 2 of term 10 had 370 acknowledged when
/// server 3 was refused at that same 17.852606007 s, and of its 962 probes after it 940
/// are rejected and none accepted, so server 3 is the one follower left uncounted after
/// the last heal at 20.709 s. One uncounted follower is not the wedge, which needs both.
/// `IgnoreIncarnation` alone does not reach it: on its schedule server 3 is refused
/// twenty times over for a staging `CURRENT` that cannot be read, a refusal no leader's
/// progress outlives, and nothing goes stale. The correct server resets and re-seeds
/// server 3 after its own refusal at 12.162591955 s.
///
/// **Re-audited under D-078, which moves every schedule again** — a follower writes a
/// snapshot record and deletes log keys where it wrote and deleted nothing, so every
/// disk draw from the first compaction on is another draw. Both halves have left this
/// seed. No run of it takes an index twice now (29 to 41 takes a run, each into its own
/// version directory even under `SharedSnapshotDir`, whose one mutable directory per
/// index is never revisited), so nothing is re-taken under a live stream; and no run
/// leaves a follower uncounted after the last heal or a leader's progress stale, under
/// the pair or under either half. The test asserts both absences with their
/// non-vacuity — the takes are asserted non-empty, and server 3's refusals asserted
/// present under the runs whose leader's reset is asked about — so the day either half
/// comes back it says so and the pin can be upgraded (CLAUDE.md:58-67).
///
/// **The search SHARD.md §12 asks for at such a move was run, and its answer goes to
/// the owner.** Over seeds 0..1000 in release on this tree the pair is caught on four —
/// 332, 796 and 848 by liveness and 847 by linearizability — and on *every one of them
/// `SharedSnapshotDir` alone is caught too*, so none is a wedge that needs both bugs.
/// The same search on the tree this branch is cut from (`ae75bdf`) finds one, seed 954,
/// and `SharedSnapshotDir` alone is caught there as well. So the pair has no wedge seed
/// in the first thousand either before or after D-078: the absence is not this change's
/// doing, and it is reported rather than papered over, since the pair is Phase 2's
/// control for a wedge that needs both bugs (D-045).
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
        // What the stream half reaches on this seed, asserted per variant: the
        // re-take at an index already taken and the streams it lands under, where
        // the variant is carried, and neither where it is not.
        let takes = report.snapshot_takes();
        assert!(
            !takes.is_empty(),
            "seed 680 under {variants:?} takes no snapshot at all: every claim below is \
             vacuous, so re-audit the pin and `snapshot_takes`"
        );
        // PROPOSED(D-078): the stream half's shape has left this seed. No run takes
        // an index twice, so nothing can be re-taken under a live stream; both are
        // asserted, over takes asserted non-empty just above, so neither goes vacuous.
        let under = report.retakes_under_streams();
        assert!(
            !took_an_index_twice(&takes) && under.is_empty(),
            "seed 680 under {variants:?} takes an index twice ({takes:?}) or re-takes under a \
             live stream ({under:?}) again: the shape this pin asserts the absence of is back, \
             so pin it rather than the absence"
        );
        // PROPOSED(D-078): and so has D-042's half, under the pair as under either
        // half alone. The day a follower is left uncounted here again, pin it.
        let uncounted = report.uncounted_after_heal();
        assert!(
            uncounted.is_empty(),
            "seed 680 under {variants:?} leaves a follower uncounted after the last heal again \
             ({uncounted:?}): pin it rather than this absence"
        );
        let stale: Vec<u64> = (1..=raft::SERVERS)
            .filter(|&s| report.stale_progress(s).is_some())
            .collect();
        assert!(
            stale.is_empty(),
            "seed 680 under {variants:?} leaves a leader's progress stale again ({stale:?}): \
             pin it rather than this absence"
        );
        if variants.contains(Variant::IgnoreIncarnation) {
            // The absence means nothing unless server 3 is refused at all, and
            // `refusal_reset_reseed` answers `None` to both: under the pair server 3
            // is refused once at 17.852606007 s and under `IgnoreIncarnation` alone
            // twenty times from 12.162591955 s, for a staging `CURRENT` that cannot
            // be read.
            let refused: Vec<&TraceEvent> = report
                .records
                .iter()
                .map(|record| &record.event)
                .filter(|event| matches!(event, TraceEvent::RaftRefused { server: 3, .. }))
                .collect();
            assert!(
                !refused.is_empty(),
                "seed 680 under {variants:?} no longer refuses server 3 at all, so the absence \
                 below says nothing about the leader's progress: re-audit the pin"
            );
            assert!(
                report.refusal_reset_reseed(3).is_none(),
                "seed 680 under {variants:?}: the leader as built forgot a refused follower's \
                 progress, which is the fix the variant turns off: re-audit the pin"
            );
        } else {
            // D-042's fix, under the correct server and under the stream half alone:
            // the refusal, the leader's reset and the re-seed.
            assert!(
                report.refusal_reset_reseed(3).is_some(),
                "seed 680 under {variants:?} no longer refuses server 3, resets the leader's \
                 progress for it and re-seeds it: re-audit the pin"
            );
        }
        assert_no_stream_wedge(&report);
    }
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
/// Re-audited under D-069, which moved every raft schedule that serves a read (the read
/// is served at one engine version now, an extra engine read on the path), on top of the
/// key layout and the store's format record (D-059, D-060) before it (see seed 164's
/// pin). The two halves have swapped since that audit, and the test asserts each where
/// it is and, with the reason, absent where it is not.
///
/// **The fix's half is here now, on server 3**, and the test asserts it record by
/// record: the open at 19.024651157 s drops table 34, which the manifest lists; the
/// engine is quiesced at 19.061652101 s, "the recovery lost writes in the middle of the
/// state", with the refusal written into the store's marker before anything else; the
/// schedule restarts the node at 19.083 s; and that open is refused at 19.093731093 s on
/// the durable mark, naming the loss it recorded. Between the quiesce and the leader's
/// install, adopted at 19.318300999 s, nothing on that node writes a manifest, switches
/// `CURRENT`, deletes a log segment or opens the store clean — the quiesced engine does
/// no work (D-044) — so there is nothing for state machine safety to report and the run
/// passes. (An earlier refusal of the same server, at 17.211872454 s, is for a log that
/// stops at a bad checksum, damage the start finds rather than a recovery losing state.)
///
/// **The bug's half is not here**, and the test asserts the absence with its reason: no
/// engine on the run as built reports lost state at all, so there is no refusal for a
/// flush to launder, and `Fault::CrashRefused` lands on no refused server. The variant's
/// catch is pinned with its mechanism on seed 102, the first seed of the first thousand
/// on which it is caught on this tree.
///
/// (Before D-069 the halves were the other way about on this seed: as built, server 1
/// was refused for lost state over table 44 at 18.109367393 s and crashed twice while
/// refused without the loss ever being laundered, and under the correct server nothing
/// was dropped at all. With D-056's queue alone the seed reached both halves, as built
/// over table 28 from 16.504 s and under the correct server over table 56 from
/// 19.101 s. Before the queue it reached neither.)
///
/// **Re-audited under D-078**, which moves every schedule again — a follower writes a
/// snapshot record and deletes log keys where it wrote and deleted nothing. The fix's
/// half is still here and is still asserted record by record, but it has moved to
/// **server 1** and its last step has changed. The open at 18.853009241 s drops table
/// 74, the engine is quiesced at 18.876708665 s, the schedule restarts the node at
/// 18.896 s, and that open drops table 74 again at 18.936568683 s, quiesces again at
/// 18.954351937 s and refuses the store at 18.958974816 s — **for the loss it found
/// itself**, `the engine's recovery lost state: dropped tables [74]`, not for the mark
/// the first refusal wrote. The mark is not what this run now shows surviving a crash;
/// what it shows is the quiesce, the restart and a second refusal of the same store,
/// which is the part of D-044 this seed still reaches. That the marker survives a crash
/// is shown on the same run by the earlier refusal of the same server at
/// 16.730362789 s, `the directory carries the RAFT-STORE marker and a CURRENT that
/// cannot be read`: the marker is there at a later start. Both are asserted, and the
/// refusal-on-the-mark shape is asserted *absent after the quiesce*, with this reason,
/// so the day the second open stops finding the loss itself and falls back on the mark
/// the pin says so and can be upgraded.
///
/// The bug's half is still not here, asserted as before. Its own pin, seed 102, has lost
/// the mechanism too — see D-078, which reports that to the owner.
#[test]
// PROPOSED(D-078): a follower compacts its log to its own applied index.
fn seed_687_which_the_premerge_found_stays_green() {
    use ananke_env::NodeId;
    let node = Some(NodeId::new(1));
    let lost_state = |report: &raft::Report| -> Vec<(u64, ananke_env::Instant)> {
        report
            .records
            .iter()
            .filter_map(|r| match &r.event {
                TraceEvent::RaftRefused { server, reason } if reason.starts_with(LOST_STATE) => {
                    Some((*server, r.at))
                }
                _ => None,
            })
            .collect()
    };

    // The bug's half, absent with its reason: nothing is dropped, so no engine reports
    // lost state and there is no refusal for a flush to launder. Seed 102 pins the
    // catch.
    let built = raft::run(687, Variant::RefusalNotDurable);
    assert_eq!(
        built.check().err(),
        None,
        "seed 687 as built no longer passes: re-audit the pin"
    );
    let refused_as_built = lost_state(&built);
    assert!(
        refused_as_built.is_empty(),
        "seed 687 as built refuses a server for lost state again ({refused_as_built:?}): the \
         laundered store may be reachable here, so pin the mechanism — state machine safety \
         reporting the restatement"
    );
    assert_eq!(
        crashes_while_refused(&built),
        0,
        "seed 687 as built crashes a refused server again, which is the fault the bug's half \
         needs: re-audit the pin"
    );
    assert!(
        built.restarts_after_lost_state_refusal().is_empty(),
        "seed 687 as built restarts a server after a refusal for lost state again: re-audit \
         the pin"
    );

    // The fix's half, which this seed does reach: the loss, the quiesce, the durable
    // mark honoured by the open after the crash, and no work by the quiesced engine
    // until the leader's install replaces the store.
    let correct = raft::run(687, Variant::Correct);
    assert_eq!(
        correct.check().err(),
        None,
        "seed 687 under the correct server no longer passes: re-audit the pin"
    );
    let dropped: Vec<u64> = correct
        .records
        .iter()
        .filter_map(|r| match r.event {
            TraceEvent::SstDropped { number, .. } => Some(number),
            _ => None,
        })
        .collect();
    // PROPOSED(D-078): a follower's compaction moved the disk draws, so the recovery
    // that loses writes here now drops table 74 twice — once at each of the two starts
    // the seed makes on that store — where it dropped tables 21 and 34 before. What the
    // pin needs of this is that the recovery really lost tables, which is what quiesces
    // the engine below; the numbers are asserted so a run that stops losing any says so.
    assert_eq!(
        dropped,
        vec![74, 74],
        "seed 687 under the correct server no longer drops table 74 twice: {dropped:?}; \
         re-audit the pin"
    );
    let quiesced = correct
        .records
        .iter()
        .find(|r| r.node == node && matches!(r.event, TraceEvent::EngineQuiesced { .. }))
        .map(|r| r.at)
        .expect(
            "seed 687 under the correct server no longer quiesces server 3's engine over a \
             recovery that lost writes: re-audit the pin",
        );
    // PROPOSED(D-078): the second open finds the loss itself, so it refuses for lost
    // state rather than on the mark the first refusal wrote. The mark's own survival
    // across a crash is shown by the earlier refusal of the same store, asserted below.
    let refused_again = correct
        .records
        .iter()
        .find(|r| {
            r.at > quiesced
                && matches!(&r.event, TraceEvent::RaftRefused { server: 1, reason }
                    if reason.starts_with(LOST_STATE))
        })
        .map(|r| r.at)
        .expect(
            "seed 687 under the correct server no longer refuses server 1's store again after \
             the quiesce: re-audit the pin",
        );
    assert!(
        !correct.records.iter().any(|r| {
            r.at > quiesced
                && matches!(&r.event, TraceEvent::RaftRefused { server: 1, reason }
                    if reason.contains("the RAFT-STORE marker says this store lost state"))
        }),
        "seed 687 under the correct server refuses server 1's store on the durable mark after \
         the quiesce again: pin that, which is the stronger statement, rather than the second \
         open finding the loss itself"
    );
    // The marker does survive a crash on this run, shown by the earlier refusal of the
    // same store at a later start: the absence just above is of the mark's *lost state*
    // clause, not of the marker being read at all.
    assert!(
        correct.records.iter().any(|r| {
            r.at < quiesced
                && matches!(&r.event, TraceEvent::RaftRefused { server: 1, reason }
                    if reason.contains(STORE_MARKER))
        }),
        "seed 687 under the correct server no longer refuses server 1's store on a marker it \
         finds at a start, so the absence above says nothing about the marker: re-audit the pin"
    );
    let restarted = correct.records.iter().any(|r| {
        r.at > quiesced
            && r.at < refused_again
            && matches!(r.event, TraceEvent::NodeRestarted { .. })
    });
    assert!(
        restarted,
        "seed 687 under the correct server no longer restarts the node between the quiesce at \
         {quiesced:?} and the second refusal at {refused_again:?}, so the refusal is the same \
         open's: re-audit the pin"
    );
    let worked = correct.records.iter().any(|r| {
        r.at > quiesced
            && r.at < refused_again
            && r.node == node
            && matches!(
                r.event,
                TraceEvent::ManifestWritten { .. }
                    | TraceEvent::CurrentSwitched { .. }
                    | TraceEvent::WalSegmentDeleted { .. }
                    | TraceEvent::RaftRecovered { server: 1, .. }
            )
    });
    assert!(
        !worked,
        "seed 687 under the correct server writes a manifest, switches CURRENT, deletes a log \
         segment or opens server 1's store clean between the quiesce at {quiesced:?} and the \
         refusal at {refused_again:?}: the quiesced engine is doing work (D-044); re-audit \
         the pin"
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
/// D-056's send queue moved the schedule, and the key layout and the store's format
/// record (D-059, D-060) moved it again (see seed 164's pin); the seed still does not
/// reach the straddle, which the test asserts ([`assert_straddle_gone`]): the partition
/// aimed by `Fault::RetakeUnderStream` does not isolate server 1 at 15.203 s, none of
/// the run's seven isolations begins then — server 1's are at 2.334, 4.663, 6.623 and
/// 17.664 s — server 1 is in term 9 across that stretch, having raised it at
/// 10.89669058 s and not again until 17.425724973 s, and no term change of any server
/// straddles any isolation's start. The run passes. The directed term-raise schedule
/// still reaches D-047's straddle, asserted on its seed 1 above, and every sweep
/// asserts every catch decision time removes (D-051).
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
/// D-056's send queue moved the schedule and the key layout and the store's format
/// record (D-059, D-060) moved it again (see seed 164's pin): none of the run's seven
/// isolations begins at 19.22 s. Server 1 is cut off from 19.37 s to 20.972 s instead,
/// and keeps term 13 through the whole of it — set at 18.341754321 s and not raised
/// until 20.988476092 s, 16.5 ms after the heal — so no term change straddles that
/// isolation's start, or any other's. The test asserts the absence, as seed 1885's
/// does, and the run passes.
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
/// None of the eleven reaches the straddle on the tree with the key layout and the
/// store's format record (D-059, D-060), which moved every raft schedule again after
/// D-056's send queue had (see seed 164's pin), and the pin asserts its absence with the
/// reason ([`assert_straddle_gone`]): on no run does a term change straddle an
/// isolation's start, and every run passes. The isolation the nightly named still
/// comes on the same two of them, on the same server at the same instants — seed 5203's
/// server 2 from 12.369 s and seed 6691's server 1 from 17.298 s — and the server keeps
/// its term through it, only pre-voting; on the other nine the leader-relative fault that
/// made it lands elsewhere. The day a straddle returns on any, the absence assertion
/// fails and the pin can be upgraded back to [`assert_rise_straddles_the_isolation`].
///
/// Two of the eleven, `IgnoreIncarnation`'s seeds 2509 and 5990, had already moved
/// away under D-049: the leader under D-042's bug stepped down on check quorum —
/// seed 2509's leader 2 of term 12 at 14.547925798 s leaving server 3 uncounted, seed
/// 5990's leader 3 of term 11 at 13.944738313 s leaving server 2 — before the
/// isolation the straddle was at. Neither reaches that state on this tree, and under
/// D-069, which moved every raft schedule that serves a read, neither does seed 1252,
/// which held it on the tree before: seed 3087, the third of `IgnoreIncarnation`'s
/// four, is the one that does now. The test asserts which runs have such a step-down
/// rather than that none has, so that a move either way is seen. The
/// run still passes: a step-down leaving a follower uncounted is the leader as built
/// doing what D-042 describes, not a violation of a check.
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
    // D-060's layout moved the schedules again: one of the eleven now steps a leader
    // down leaving a follower uncounted, the state D-049 recorded on 2509 and 5990.
    // PROPOSED(D-069) moved them once more, and it was a different one: seed 3087's
    // run had the step-down and seed 1252's no longer did.
    // PROPOSED(D-078) moved them once more and **none of the eleven has it now**: the
    // list is empty and asserted empty, so the day a run of these eleven steps a leader
    // down leaving a follower uncounted this test says which.
    const UNCOUNTED_STEP_DOWN: [u64; 0] = [];
    let verdicts = sweep(pairs.len() as u64, |i| {
        let (seed, variant, _, original) = pairs[usize::try_from(i).expect("small")];
        let report = raft::run(seed, variant);
        assert_straddle_gone(&report, original, KEPT.contains(&seed));
        let uncounted = report.has(
            |e| matches!(e, TraceEvent::RaftQuorumLost { uncounted, .. } if !uncounted.is_empty()),
        );
        assert_eq!(
            uncounted,
            UNCOUNTED_STEP_DOWN.contains(&seed),
            "seed {seed} under {variant:?}: whether a leader steps down leaving a follower \
             uncounted has moved (found {uncounted}); re-audit the pin"
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
/// On the tree with the key layout and the store's format record (D-059, D-060), which
/// moved every raft schedule again after D-056's send queue had (see seed 164's pin),
/// none of the 28 runs has its catch to remove, which is asserted, so the day one
/// reaches it again this test runs the assertion on it; until then each run still goes
/// through [`checked`], so a catch decision time removes on any of them meets the
/// sweeps' assertions, and no catch may be added.
///
/// Re-audited under D-069, which moved every raft schedule that serves a read (the read
/// is served at one engine version now, an extra engine read on the path). On six of
/// them the isolation the catch named still comes, on the same server at the same
/// instants (seeds 5203, 6691, 5051, 5879, 5918 and 2578), and the server keeps its term
/// through it; on the other 21 pre-vote catches the leader-relative fault lands
/// elsewhere. Seed 5153's own timer gap is gone with its schedule; what the replay by
/// durability time finds there now is three other gaps on a run the timer bound is not
/// asked of at all, which is asserted. Every run passes the check but three, each caught
/// over its own variant's bug and none over a record's time, which `checked` asserts and
/// which this test asserts one by one: seed 2305 under `SnapshotWithoutCurrentLast` by
/// state machine safety, server 3 recovering an applied index of 282 whose log does not
/// hold index 281 — the streamed `CURRENT` written before the repair; seed 6717 under
/// `ResetTimerOnAnyRpc` by the timer check, the rule that variant is written for; and
/// seed 9557 under `AdoptionAsBuilt` by committed-entries-stay, server 2 truncating from
/// index 1, which is the shape D-041's seed 6325 describes. (Before D-069 only seed
/// 2305 was caught, at applied index 288 over index 279.)
/// (With D-056's queue alone, six kept their isolation and seed 6717 under
/// `ResetTimerOnAnyRpc` was the one catch, by the timer check; before the queue, 26 of
/// the 28 were removed here in the nightlies' words and passed, `IgnoreIncarnation` on
/// seeds 2509 and 5990 having moved under D-049.)
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
    // D-056's queue and then the key layout and the format record (D-060) moved every
    // schedule away from its catch; these keep the isolation the catch named.
    // PROPOSED(D-069) moved them once more: seed 5918 keeps its isolation now, where
    // it did not before, and the other five are unchanged.
    // PROPOSED(D-078) moved the schedules again: seed 5918's isolation of server 3 from
    // 9.532 s is gone and seed 4814's of server 3 from 12.111 s has come back, so six of
    // the 28 still hold the isolation their catch named, a different six.
    const KEPT: [u64; 6] = [5203, 6691, 5051, 5879, 4814, 2578];
    let runs = sweep(REMOVED.len() as u64, |i| {
        let (seed, variant, was) = REMOVED[usize::try_from(i).expect("small")];
        let report = raft::run(seed, variant);
        if was.starts_with("pre-vote: ") {
            assert_straddle_gone(&report, was, KEPT.contains(&seed));
        }
        let moved = Mutex::new(MovedSeeds::default());
        let verdict = checked(&report, &moved);
        let moved = moved.into_inner().unwrap();
        // D-056, then D-060: seed 5153's own timer gap, asserted gone from the replay
        // by durability time, with what that replay does find beside it.
        let gaps = report.timer_gaps_by(TimerResets::ALL, RecordTime::Durable);
        let names_the_nightlys = gaps.iter().any(|gap| gap.violation() == was);
        let asked = report.uniform() && report.majority_up();
        (
            seed,
            variant,
            was,
            verdict,
            moved.removed,
            moved.added,
            (gaps.len(), names_the_nightlys, asked),
        )
    });
    for (seed, variant, was, verdict, removed, added, gaps) in &runs {
        let (durable_gaps, names_the_nightlys, asked) = *gaps;
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
            // The key layout and the format record (D-060) moved this schedule, and the
            // read's move to the server that serves it (D-069) moved it again: the count
            // below was five stretches before that and three after it.
            // PROPOSED(D-078) moves it once more — a follower writes a snapshot record
            // and deletes log keys where it wrote and deleted nothing — and the run is
            // **four** stretches now and the timer bound *is* asked of it, its schedule
            // being uniform and its majority up, so the first of the four is a catch:
            // the variant's own rule, caught where the rule is written for it. The
            // nightly's own gap is still gone from the replay by durability time and
            // decision time still removes nothing, which is why nothing is removed here;
            // the catch itself is asserted below, with the other runs caught over their
            // own bug.
            assert!(
                durable_gaps > 0 && !names_the_nightlys && asked && removed.is_empty(),
                "seed 5153 under {variant:?}: the replay by durability time finds \
                 {durable_gaps} gaps, the nightly's own among them: {names_the_nightlys}; the \
                 timer bound is asked of the run: {asked}; decision time removed {} catches; \
                 re-audit the pin",
                removed.len()
            );
        }
        // PROPOSED(D-069): three of the 28 were caught, each by its own variant's
        // bug on a schedule the read's move redrew, and none by a check reading a
        // record's time — `checked` above asserts that decision time removes nothing
        // on any of them. Each catch is asserted, so a move either way is seen.
        // PROPOSED(D-078) moved them once more, and it is four now and a different
        // set: seed 9557 under `AdoptionAsBuilt` passes outright, seed 5153 under
        // `ResetTimerOnAnyRpc` and seed 6366 under `ApplyBeforeCommit` are caught over
        // their own variants' bugs where they were not, and seeds 2305 and 6717 keep
        // their catch with the numbers the moved schedules give.
        let own_bug: Option<&str> = match (*seed, *variant) {
            // The streamed `CURRENT` written the moment it arrives, and a restart that
            // adopts the store it leaves (RAFT.md §5).
            (2305, Variant::SnapshotWithoutCurrentLast) => Some(
                "seed 2305: state machine safety: server 3 recovered an applied index of 327 \
                 in group 2 but its log does not hold index 322",
            ),
            // PROPOSED(D-078): seed 6366's run under `ApplyBeforeCommit` is caught by
            // state machine safety now, over the variant's own bug: entries handed to
            // the apply task before any commit index reaches them, applied differently
            // on two servers.
            (6366, Variant::ApplyBeforeCommit) => Some(
                "seed 6366: state machine safety: index 60 of group 2 was applied as term 2 \
                 payload 0x7eed89c74e606ebf on one server and term 3 payload \
                 0xaf63bd4c8601b7df on server 1",
            ),
            // PROPOSED(D-078): seed 5153's run is caught by the timer check now too,
            // over a gap of its own rather than the nightly's, which is gone.
            (5153, Variant::ResetTimerOnAnyRpc) => Some(
                "seed 5153: timers: server 3's replica of range 2 heard from no leader of its \
                 term and granted no vote since Instant(2.184906554s) and had not campaigned \
                 by Instant(2.585015833s)",
            ),
            // The timer variant's own catch, the rule it is written for.
            (6717, Variant::ResetTimerOnAnyRpc) => Some(
                "seed 6717: timers: server 1's replica of range 2 heard from no leader of its \
                 term and granted no vote since Instant(1.992451251s) and had not campaigned \
                 by Instant(2.392781988s)",
            ),
            // PROPOSED(D-078): seed 9557's run under `AdoptionAsBuilt` is no longer
            // caught at all — the adoption as built loses no copied store on the moved
            // schedule — so it falls to the `None` arm with the other 24 and is
            // asserted to pass outright. The variant's own catch is asserted at its
            // Phase 2 tier by `a_server_whose_adoption_is_as_built_is_caught`.
            _ => None,
        };
        if let Some(expected) = own_bug {
            assert!(
                verdict.as_ref().is_some_and(|v| v.starts_with(expected)),
                "seed {seed} under {variant:?} is no longer caught over its own bug with \
                 `{expected}`: {verdict:?}; re-audit the pin"
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

/// Seed 1 of the directed schedule, which pinned D-050's shape below from D-056's send
/// queue until the key layout and the store's format record (D-059, D-060) moved every
/// raft schedule again (see seed 164's pin). On this tree it does not reach that shape,
/// which the test asserts: no term change on the run was received before an isolation
/// and stepped inside it. What it reaches instead is the other side of the same
/// boundary: in every one of its five isolations, cut as a RequestVote of a higher term
/// reaches a server, the server's step takes the message before the watch's slice ends
/// and the isolation begins, and only the step's persist, and so its record, falls
/// inside the window — D-047's straddle, which the nightly's seeds 1885 and 2023 were
/// pinned for before D-056 moved them away. So the seed now pins D-047's mechanism on
/// the directed schedule, asserted as those seeds' pins asserted it: five rises decided
/// before their isolations' starts and traced inside them, none with a server's message
/// delivered in the window; the first, server 2's from term 1 to 2, decided 3.153 µs
/// before its isolation at 1.21785 s at the delivery of server 1's RequestVote of term
/// 2, and traced 2.758 ms into it; the check by durability time failing with that
/// window's words; the check by decision time passing; and the whole check green.
/// Seed 4 below reaches D-050's shape again and asserts it.
///
/// Re-audited under D-069, which moved every raft schedule that serves a read (the read
/// is served at one engine version now, an extra engine read on the path): the fifth
/// isolation, server 1's from 6.90976 s, straddles too, where it did not before. The
/// first four are unchanged, instant for instant, and so is the first, which this test
/// names.
///
/// Re-audited again under D-078, which moves every schedule once more — a follower
/// writes a snapshot record and deletes log keys where it wrote and deleted nothing.
/// The seed still reaches D-047's shape and not D-050's, so this pin keeps asserting
/// the same mechanism; what has moved is how many windows reach it. **Seven** of the
/// run's isolations straddle now, not five, and the first is server 2's from term 1 to
/// 2, decided 9.447 µs before its isolation at **1.24716 s** and traced 1.297 ms into
/// it, at the delivery of server 1's **AppendEntries** of term 2 rather than its
/// RequestVote. The count and the first are both asserted, as before.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
#[test]
fn seed_1_of_the_term_raise_schedule_now_straddles_its_isolations_by_decision_time() {
    let report = raft::run_with(
        1,
        raft::Schedule::term_raise_behind_a_step(TERM_RAISE_TRIES),
        Variant::Correct,
    );
    assert_eq!(
        report.isolation_received_straddles(),
        Vec::new(),
        "seed 1 steps a message received before its isolation inside it again: it reaches \
         D-050's shape once more; re-audit the pins of seeds 1 and 4"
    );
    let straddle = assert_rise_straddles_the_isolation(
        &report,
        7,
        (2, 1, 2, "follower"),
        "pre-vote: server 2 raised its term from 1 to 2 while isolated from Instant(1.24716s) to Instant(1.54716s)",
    );
    assert_eq!(
        straddle.causes,
        [(1, "append-entries", 2)],
        "seed 1: the rise was not decided at the delivery of server 1's AppendEntries of term \
         2, so its decision time is not that message's step: {straddle:?}"
    );
    report.check().unwrap();
}

/// Seed 4 of the directed schedule pins the shape with its numbers (D-050).
/// Server 2's campaign for term 5 sent server 3 a RequestVote of term 5, delivered
/// and received at 2.853812638 s. The isolation began 7.362 µs later, at the end of
/// the watch's slice, 2.85382 s, and server 3's step took the message 12.88 µs into
/// it: a change from term 4 to 5 decided inside the window, with no message from a
/// server reaching server 3 until the heal at 3.15382 s. The check by decision time
/// flags the isolation in the words below; the check by cause excuses it, since the
/// record says the message was received before the window; and the run passes the
/// whole check. When a change to the simulator or the server moves the seed away from
/// the shape, this fails and names what it found instead.
///
/// Seed 4 held this pin until D-056's send queue moved every schedule; seed 1 held it
/// on the tree with the queue alone, and the key layout and the store's format record
/// (D-059, D-060) moved the schedules again and gave it back to seed 4: the lowest seed
/// of the schedule whose run holds exactly one such change on this tree, seed 2's
/// holding two (server 1's, from terms 3 and 5, at 2.85601 s and 3.67795 s). The shape
/// is reached on 276 of the first thousand seeds of the schedule. Seed 1's own test
/// above asserts what it reaches instead.
///
/// **Re-audited under D-078**, which moves every schedule once more. Seed 4 still holds
/// exactly one such change and keeps the pin, but it is a different one: server **2**'s
/// from term 1 to 2, caused by server 1's RequestVote of term 2, received at
/// 1.22460251 s, with the isolation beginning 7.49 µs later at 1.22461 s and the step
/// taking the message 1.017 µs into it. Seed 1 still reaches none; seed 2 now holds
/// exactly one too, so seed 4 is no longer the *lowest* such seed — the pin stays on 4
/// because it is the seed the shape has been pinned on, and seed 2's own run is not
/// pinned anywhere. The numbers below are the new ones and are asserted to the
/// nanosecond, so the next move says so.
// PROPOSED(D-078): a follower compacts its log to its own applied index.
#[test]
fn seed_4_of_the_term_raise_schedule_steps_a_message_received_before_its_isolation() {
    let report = raft::run_with(
        4,
        raft::Schedule::term_raise_behind_a_step(TERM_RAISE_TRIES),
        Variant::Correct,
    );
    let received = report.isolation_received_straddles();
    let [s] = received.as_slice() else {
        panic!(
            "seed 4 no longer has exactly one term change received before an isolation and \
             stepped inside it: {received:?}"
        );
    };
    assert_eq!(
        (s.server, s.before, s.term, s.role, s.causes.as_slice()),
        (2, 1, 2, "follower", &[(1, "request-vote", 2)][..]),
        "seed 4 steps another change: {s:?}"
    );
    assert_eq!(
        (s.received, s.from, s.decided, s.until),
        (
            ananke_env::Instant::from_nanos(1_224_602_510),
            ananke_env::Instant::from_nanos(1_224_610_000),
            ananke_env::Instant::from_nanos(1_225_627_305),
            ananke_env::Instant::from_nanos(1_524_610_000),
        ),
        "seed 4's receipt, isolation or step moved: {s:?}"
    );
    assert_received_straddle(&report, s);
    assert_eq!(
        report.isolation_keeps_its_term_by(RecordTime::Decided, s.server, s.from, s.until),
        Err(
            "pre-vote: server 2 raised its term from 1 to 2 while isolated from \
             Instant(1.22461s) to Instant(1.52461s)"
                .to_owned()
        )
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

/// D-065's own assertion, with the known-buggy variant beside it that the same
/// fold catches (CLAUDE.md's pair rule).
///
/// A follower compacts to its own applied index. On the correct system that
/// index never passes its commit index, so the prefix it drops holds nothing
/// uncommitted — and `raft::compaction_stays_committed` says so over the whole
/// trace, on every seed at every tier, as part of
/// [`the_correct_server_passes_every_seed`]'s `check()`.
///
/// `ApplyBeforeCommit` is the bug that breaks exactly that step: it hands the
/// `apply` task the entries of a persist as they become durable, before any
/// commit index reaches them (`node.rs`), so a follower's applied index runs past
/// its commit index and the record it writes names an index nothing has
/// committed. The fold is asked of that variant here on its own, rather than
/// through `check()`, because `check()` returns the first violation and state
/// machine safety usually sees this variant first: what this test measures is
/// whether the new fold *by itself* distinguishes the bug from correct code.
#[test]
fn a_server_that_applies_before_commit_compacts_past_its_commit_index() {
    let caught: Vec<String> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::ApplyBeforeCommit);
        raft::compaction_stays_committed(&report.records)
            .err()
            .map(|violation| format!("seed {seed}: {violation}"))
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "ApplyBeforeCommit: the compaction fold caught it on {} of {} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        !caught.is_empty(),
        "the compaction fold never saw ApplyBeforeCommit compact past a commit index"
    );
}

/// The seed the follower-log bound's pair is pinned at. Five seeds of the first
/// thousand carry a replica's log past [`raft::FOLLOWER_LOG_BOUND`] under
/// [`Variant::FollowerNeverCompacts`] — 116, 429, 512, 577 and 757 — and this is
/// the one with the most room: 878 entries against the bound's 768, where seed
/// 116, the lowest, holds 788. A pin two per cent over a bound would go quiet at
/// the next schedule move and say nothing about it.
// PROPOSED(D-078): Stage B's exit measures the largest in-memory log of any
// follower replica.
const FOLLOWER_LOG_SEED: u64 = 512;

/// Stage B's exit bound on a follower's log, with the known-buggy variant beside
/// it that the same check catches (CLAUDE.md's pair rule).
///
/// `FOLLOWER_NEVER_COMPACTS` is the server as it was built before D-065: a replica
/// that is not leading never compacts, so its in-memory log keeps every entry
/// since the last snapshot a leader gave it or it took itself, and grows with the
/// run. That is the only thing in the tree that can make
/// `Report::follower_log_is_bounded` fire, and before this variant existed the
/// bound was unfalsifiable — widening it from 768 entries to 49 152 changed no
/// test at any tier, which is how the review of this slice found it.
///
/// **The rate, measured before it is asserted (D-061).** Over the first thousand
/// seeds in release, the variant is caught on **5** — 116, 429, 512, 577 and 757 —
/// and on every one of the five it is this bound that catches it; its largest
/// follower log is 878 entries, 73 × the threshold, on seed 512. Half a per cent
/// is far too thin for a sweep at the gate's twenty seeds, so this is pinned at a
/// seed instead of swept: [`FOLLOWER_LOG_SEED`] asserts the mechanism by name, at
/// every tier, and the sweep's own `follower_log_multiples` keeps the distribution
/// in view.
///
/// The correct half of the pair is the same seed run correct: it passes, and its
/// largest follower log is inside the bound with room.
// PROPOSED(D-078): Stage B's exit measures the largest in-memory log of any
// follower replica.
#[test]
fn a_replica_that_never_compacts_outgrows_the_follower_log_bound() {
    let bound = raft::FOLLOWER_LOG_BOUND;

    let broken = raft::run(FOLLOWER_LOG_SEED, Variant::FollowerNeverCompacts);
    let (longest, server) = broken.largest_follower_log();
    let violation = broken.check().expect_err(&format!(
        "seed {FOLLOWER_LOG_SEED} under FollowerNeverCompacts passed every check; its \
         largest follower log was {longest} entries against the {bound} of the bound"
    ));
    assert!(
        violation.contains("follower log:"),
        "seed {FOLLOWER_LOG_SEED} under FollowerNeverCompacts is caught, but by something \
         else first — the pin asserts this bound's own mechanism: {violation}"
    );
    assert!(
        longest > bound,
        "the violation names the bound, so the log must be past it: {longest} of {bound} \
         on server {server}"
    );
    eprintln!(
        "FollowerNeverCompacts: seed {FOLLOWER_LOG_SEED} held {longest} entries on server \
         {server}, past the bound's {bound}"
    );

    let correct = raft::run(FOLLOWER_LOG_SEED, Variants::default());
    let (longest, server) = correct.largest_follower_log();
    assert!(
        correct.check().is_ok(),
        "the correct half of the pair must pass: {:?}",
        correct.check().err()
    );
    assert!(
        longest <= bound,
        "the correct server's own log is what the bound is about: {longest} of {bound} \
         on server {server}"
    );
    eprintln!(
        "Correct: seed {FOLLOWER_LOG_SEED} held {longest} entries on server {server}, \
         inside the {bound} of the bound"
    );
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
    // queue alone the catch was 16 of the first thousand seeds, none of them below seed
    // 100 (the first at 119); with the key layout and the store's format record
    // (D-059, D-060) it was 9, the first at seed 158. Since D-078 moved every
    // schedule again it is **58 of the first thousand, 5.8 %**, the first at seed 20,
    // and every one of the 58 is the `match starts` oracle's rather than state
    // machine safety's — D-078's own entry says so and D-079 re-measured it, on that
    // branch and on `main` alike. The 0.7 % this comment argued from, and the seven
    // seeds it named, are the figures of the tree before D-078.
    //
    // At 5.8 % a hundred seeds catch none about once in four hundred
    // (0.942^100 = 2.5e-3) and the gate's twenty about three times in ten
    // (0.942^20 = 0.30). So the hundred-seed tier is now comfortably above D-061's
    // 5 % rule rather than the coin-flip it was, and only the gate's twenty still
    // needs the pin. **Where the assertion sits is the owner's** (D-056): it stays at
    // the thousand-seed tier until the owner moves it, and this note records that the
    // reason for putting it there has gone rather than moving it. Seed 102 is pinned
    // with its mechanism at every tier:
    // `seed_102_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss`.
    // Seeds 119 and 158, which held that pin before and which the moved schedules took
    // the situation off, are kept beside it as asserted absences.
    if seeds() >= 1000 {
        assert!(!caught.is_empty(), "RefusalNotDurable was never caught");
    }
}

/// Seed 102, which pinned `RefusalNotDurable`'s own mechanism at every tier from
/// D-069 until D-078 moved every schedule again — a follower writes a snapshot record
/// and deletes log keys where it wrote and deleted nothing, so every simulated disk draw
/// from the first compaction on is another draw and the run after it is another run.
/// The pin was put here under the owner's decision of 2026-09-15 (D-056), in place of
/// seed 158, because the sweep asserts the catch only from the thousand-seed tier and a
/// pinned seed keeps it at the gate's twenty and CI's hundred.
///
/// **The mechanism has left this seed, and it has left the first thousand.** The search
/// CLAUDE.md:58-67 asks for at such a move was run over seeds 0..1000 in release on this
/// tree, for a run of `RefusalNotDurable` whose verdict is a state machine safety
/// violation *and* that crashes a refused server *and* restarts it after a lost-state
/// refusal before any install — the three facts this pin asserted: **0 of 1 000**. It
/// was 7 of 1 000 on the tree this branch is cut from (`ae75bdf`): seeds 102, 293, 378,
/// 465, 744, 893 and 926.
///
/// The variant is still caught, and more often — 58 of 1 000 against 26 — but every one
/// of the 58 is now the `match starts` oracle rather than state machine safety, where
/// before the 26 were 19 by the oracle and 7 by state machine safety. The reason is
/// D-065 itself: a follower's compaction leaves a snapshot record in the store, so the
/// laundered store that opens fresh has a prefix to account for its applied index with,
/// and the restatement no longer contradicts its own log. The oracle catches the variant
/// by the other consequence of the same bug — a store that lost its refusal opens fresh,
/// takes a new incarnation, and the leader traces a second first rise of its match.
/// **This goes to the owner**: the variant's Phase 2 standard, the catch from the
/// hundred-seed tier (§10), still holds at 5.8 %, but the mechanism D-044 names is no
/// longer reached at this tier by any seed, so no pin can keep it at the gate's twenty.
///
/// Until the owner says otherwise this test asserts the absence with its reason, in the
/// shape seed 158's pin below uses, so the day an open drops a table here again it says
/// so and the pin can assert what each server does with it. The fix's own mechanism —
/// the quiesce, the durable mark and the opens refused on it — is on seed 687, whose
/// schedule still reaches it.
///
/// Its companion for non-vacuity is the refusal matcher, asserted non-empty on both
/// runs: what is absent is the lost-state refusal, not every refusal.
#[test]
// PROPOSED(D-078): a follower compacts its log to its own applied index.
fn seed_102_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss() {
    let lost_state = |report: &raft::Report| -> Vec<(u64, ananke_env::Instant)> {
        report
            .records
            .iter()
            .filter_map(|r| match &r.event {
                TraceEvent::RaftRefused { server, reason } if reason.starts_with(LOST_STATE) => {
                    Some((*server, r.at))
                }
                _ => None,
            })
            .collect()
    };
    let built = raft::run(102, Variant::RefusalNotDurable);
    let correct = raft::run(102, Variant::Correct);
    assert_eq!(
        built.check().err(),
        None,
        "seed 102 as built is caught again: pin the mechanism rather than this absence"
    );
    assert_eq!(
        correct.check().err(),
        None,
        "seed 102 under the correct server no longer passes: re-audit the pin"
    );
    for report in [&built, &correct] {
        assert!(
            !refusals(report).is_empty(),
            "seed 102 under {:?} refuses no store at all, so the absence below says nothing: \
             re-audit the pin and the refusal matcher",
            report.variants
        );
        let lost = lost_state(report);
        assert!(
            lost.is_empty(),
            "seed 102 under {:?} refuses a server for lost state again ({lost:?}): D-044's \
             mechanism is reachable here once more, so pin it",
            report.variants
        );
        assert_eq!(
            crashes_while_refused(report),
            0,
            "seed 102 under {:?}: a crash lands on a refused server again, so the laundered \
             store may be reachable: re-audit the pin",
            report.variants
        );
        assert!(
            report.restarts_after_lost_state_refusal().is_empty(),
            "seed 102 under {:?} restarts a server after a lost-state refusal again: pin the \
             mechanism rather than this absence",
            report.variants
        );
    }
}

/// Seed 158, which held the pin above from the key layout and the store's format record
/// (D-059, D-060) until D-069 moved every raft schedule that serves a read (the read is
/// served at one engine version now, an extra engine read on the path), and what it does
/// now: no engine on the run reports lost state, under either server. Nothing is dropped
/// at any open, so `RefusalNotDurable` has no refusal for a flush to launder and the
/// correct server has nothing to quiesce. The seed still refuses stores — a log that
/// stops at a bad checksum, a manifest `CURRENT` names that cannot be read, a marker
/// that cannot itself be read — but those are damage the start finds, not a recovery
/// losing writes in the middle of the state, which is the only thing either half of this
/// pair is about. A crash does still land on a refused server, so the fault fires; it
/// simply has no laundered store to expose.
///
/// The test asserts that absence with its reason, so the day an open drops a table again
/// it says so and the pin can assert what each server does with it. The mechanism itself
/// is pinned on seed 102 above, the bug's half, and on seed 687, the fix's.
///
/// Its companion for non-vacuity is the refusal matcher, which is asserted non-empty on
/// both runs here: what is absent is the lost-state refusal, not every refusal, so a
/// matcher narrowed until it finds nothing fails on this seed itself.
#[test]
fn seed_158_which_pinned_the_refusal_that_is_not_durable_before_the_read_moved_loses_nothing() {
    let lost_state = |report: &raft::Report| -> Vec<(u64, ananke_env::Instant)> {
        report
            .records
            .iter()
            .filter_map(|r| match &r.event {
                TraceEvent::RaftRefused { server, reason } if reason.starts_with(LOST_STATE) => {
                    Some((*server, r.at))
                }
                _ => None,
            })
            .collect()
    };
    let built = raft::run(158, Variant::RefusalNotDurable);
    let correct = raft::run(158, Variant::Correct);
    assert_eq!(
        built.check().err(),
        None,
        "seed 158 as built is caught again: pin the mechanism rather than this absence"
    );
    assert_eq!(
        correct.check().err(),
        None,
        "seed 158 under the correct server no longer passes: re-audit the pin"
    );
    for report in [&built, &correct] {
        // The companion: the seed does refuse, so the absence below is of lost state
        // and not of the matcher finding anything at all.
        assert!(
            !refusals(report).is_empty(),
            "seed 158 under {:?} refuses no store at all, so the absence below says nothing: \
             re-audit the pin and the refusal matcher",
            report.variants
        );
        let lost = lost_state(report);
        assert!(
            lost.is_empty(),
            "seed 158 under {:?} refuses a server for lost state again ({lost:?}): the two \
             halves of D-044's pair are reachable here, so pin them",
            report.variants
        );
        assert!(
            !report.records.iter().any(|r| matches!(
                r.event,
                TraceEvent::SstDropped { .. } | TraceEvent::EngineQuiesced { .. }
            )),
            "seed 158 under {:?} drops a table or quiesces an engine again: re-audit the pin",
            report.variants
        );
    }
}

/// Seed 119, which held the pin above from D-056's send queue until the key layout and
/// the store's format record (D-059, D-060) moved every raft schedule again, and what it
/// does now, re-audited under D-078, which moves every schedule once more — a follower
/// writes a snapshot record and deletes log keys where it wrote and deleted nothing.
///
/// The seed refuses a store again, where it refused none: server 3's start finds a
/// `RAFT-STORE` marker it cannot read and refuses on it, under the correct server and
/// under the variant alike. That is damage the *start* finds in the marker's own bytes,
/// not the lost-state refusal `RefusalNotDurable` is about: the variant's bug is what a
/// server does *after* its engine's recovery reports lost state — keep the refusal in
/// the process instead of writing it into the store — and no recovery on this run
/// reports lost state, so the variant still has nothing to do here and neither run is
/// caught. Both facts are asserted: the refusal present, no refusal for lost state, and
/// no crash landing on a refused server.
///
/// The two runs are no longer record for record the same, as they were before D-078, and
/// that is asserted as a difference rather than left unstated: a run under a variant is
/// another run once the variant changes any draw, and `Variants` is part of each core's
/// seed material. What the pin turns on is the absence of the lost-state refusal, which
/// is asserted directly on both runs, so the identity is no longer needed to carry it.
///
/// Its companion for non-vacuity is the refusal matcher, asserted non-empty on both runs
/// here, which it was not before: what is absent is the lost-state refusal, not every
/// refusal, so a matcher narrowed until it finds nothing fails on this seed itself.
#[test]
// PROPOSED(D-078): a follower compacts its log to its own applied index.
fn seed_119_which_pinned_the_refusal_that_is_not_durable_before_the_layout_refuses_nothing() {
    let built = raft::run(119, Variant::RefusalNotDurable);
    let correct = raft::run(119, Variant::Correct);
    assert_eq!(
        built.check().err(),
        None,
        "seed 119 as built is caught again: re-audit the pin"
    );
    assert_eq!(
        correct.check().err(),
        None,
        "seed 119 under the correct server no longer passes: re-audit the pin"
    );
    for report in [&built, &correct] {
        // The companion: the seed does refuse, so the absence below is of lost state
        // and not of the matcher finding anything at all.
        let found = refusals(report);
        assert!(
            !found.is_empty(),
            "seed 119 under {:?} refuses no store at all, so the absence below says nothing: \
             re-audit the pin and the refusal matcher",
            report.variants
        );
        assert!(
            found.iter().all(|event| matches!(
                event,
                TraceEvent::RaftRefused { reason, .. } if !reason.starts_with(LOST_STATE)
            )),
            "seed 119 under {:?} refuses a server for lost state again ({found:?}): the variant \
             now has something to do here, so pin what it does",
            report.variants
        );
        assert!(
            !report.records.iter().any(|r| matches!(
                r.event,
                TraceEvent::SstDropped { .. } | TraceEvent::EngineQuiesced { .. }
            )),
            "seed 119 under {:?} drops a table or quiesces an engine again: a recovery here \
             lost writes, which is what the variant is about, so pin it",
            report.variants
        );
    }
    assert_eq!(
        crashes_while_refused(&built),
        0,
        "seed 119 as built: a crash lands on a refused server again; re-audit the pin"
    );
    assert_ne!(
        built.records, correct.records,
        "seed 119's two runs are record for record the same again: say so in the pin, since it \
         is a stronger statement than the one asserted here"
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
/// (`seed_132_which_pinned_the_combined_variant_before_the_layout_reaches_no_wedge`).
/// On the tree with the key layout (D-060) the variant is caught on **0 of the first
/// thousand**, which is D-061's open question for the owner; the arm reaches a live
/// stream on **153 of 1000** (143 before the layout, 152 before D-056) and catches
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
/// taken happens often on its own — 46 of 100 seeds, 525 of 1000 — and lands under a
/// live stream of that index on 180 of the thousand, on 135 of which the follower
/// never installs at that index afterwards: D-043's scrambled stream, the wedge's
/// stream half, built and stalling nothing because the leader still has a countable
/// follower. (Until D-060's re-audit of `snapshot_takes` this read "harmless every
/// time, because no stream had that directory open". The fold it read that from was
/// answering empty on every seed. What is true is that the stream half fires often and
/// wedges nothing, which the test below now asserts; the correct server is at 0 of the
/// thousand for the same measure, which is the pair.)
///
/// So the test asserts what is true: that the fault fired, on all three counts — a
/// take at an index already taken, into the directory a stream may be reading; that
/// re-take landing under a live stream the follower never installs at afterwards,
/// which is the wedge's stream half itself, from the hundred-seed tier where its
/// 13.5 % rate carries an assertion; and the aimed arm's own stream under a leader
/// that cannot commit — and that the catch holds at the tier that ever produced it,
/// the nightly's ten thousand, where the 4 liveness catches of 10 000 that the
/// assertion counts are missed about one run in fifty.
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
    let outcomes: Vec<(Option<String>, bool, usize, bool, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::SharedSnapshotDir);
        let scrambled: Vec<_> = report
            .retakes_under_streams()
            .into_iter()
            .filter(|retake| !retake.installed_after)
            .collect();
        // D-043's own symptom under a scrambled stream: the follower answering
        // `More` for a file it has already been sent, over and over.
        let looped = scrambled
            .iter()
            .map(|retake| {
                report.duplicate_chunk_loop(retake.leader, retake.follower, retake.retook)
            })
            .sum();
        (
            checked(&report, &moved),
            retook_at_one_index(&report),
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
        "SharedSnapshotDir: caught on {} of {} seeds, {liveness} by the liveness check, by check {by_check:?}, re-took at an index already taken on {fired} seeds, scrambled a live stream the follower never installed after on {scrambled} seeds ({looped} duplicate-chunk loops after those), the aimed re-take arm reached its stream on {aimed} seeds, first: {}",
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
    // The wedge's stream half itself: a re-take into the directory a live stream
    // has open, which that follower then never installs at. On the thousand it is
    // 135 seeds (13.5 %) against the correct server's 0, so the hundred-seed tier
    // carries it and the gate's twenty do not (the owner's rule of 2026-09-15,
    // D-061). Until D-060 this was asserted nowhere, and the fold that reads it
    // was answering empty on every seed.
    if seeds() >= 100 {
        assert!(
            scrambled > 0,
            "SharedSnapshotDir never re-took into a directory a live stream had open and left \
             unfinished: the wedge's stream half was not built on any of the {} seeds",
            seeds()
        );
    }
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
/// stale without the guard, and how many did neither. The fault's firing, a seed
/// whose drift exceeds the bound and a guard that revokes, is asserted at every
/// tier; the guardless server's stale read, `LeaseTrustsTheClock`'s catch, from the
/// thousand-seed tier (D-061).
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
    // D-061, the owner's rule of 2026-09-15: a variant caught on under 5 % of seeds
    // asserts its catch from the thousand-seed tier (the premerge and the nightly), its
    // firing at every tier above, and its rate printed at every tier. The stale read is
    // caught on 41 of the first thousand seeds on this tree, 4.1 % (40, 4.0 %, on the
    // tree D-069 left; 37, 3.7 %, before the read moved to the server it is served on;
    // 41, 4.1 %, on the tree with D-056's send queue alone, before the key layout redrew
    // them), and was on 472 of the ten thousand of the nightlies before the queue,
    // 4.72 %; the drift exceeds the bound on 503 of the thousand seeds and the guard
    // revokes on every one of them.
    //
    // The count on this tree is one lower than the same thousand seeds give with the
    // search D-080 replaced, which reported 42. The one it drops is seed 18, where the
    // old search ran out of its budget rather than proving anything: an undecided search
    // returns an error that says "linearizability", which this counter and `is_caught`
    // both read as a catch. D-080 decides that history, and it is linearizable, so the
    // catch was never the variant's. The rate is the honest one, and the margin below is
    // computed at 4 %, which both figures round to.
    // PROPOSED(D-080): the read-only candidates go first, together.
    // The rate that carries the assertion is over the tier's seeds, as every row of
    // D-061's table is: at 4.0 % the gate's twenty catch none with probability
    // 0.96^20 = 0.44 and a hundred with 0.96^100 = 0.017, so the assertion there would
    // fail a tree with nothing wrong the day a change redraws the schedules; a thousand
    // catch none with probability 0.96^1000 = 1.9e-18.
    if seeds() >= 1000 {
        assert!(stale > 0, "LeaseTrustsTheClock was never caught");
    }
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
    // PROPOSED(D-056): frames a sending socket's full queue dropped. A queue of 1 024
    // fills only when one socket sends one destination faster than a gigabit drains it,
    // which no Phase 2 scenario does, so this is 0 and `assert_complete` asserts the
    // absence with that reason (D-056; CLAUDE.md's rule for a state a scenario does not
    // reach). It was printed and not asserted until the mutation pass: a send queue
    // whose bound counted frames *ever sent* rather than outstanding ones turned it
    // into 3 691 drops at twenty seeds and 42 393 at a hundred with every tier green.
    queue_drops: usize,
    leaders: usize,
    terms_above_one: u64,
    truncations: usize,
    commits: usize,
    applies: usize,
    /// PROPOSED(D-069): the first rise of a leader's `matched` under a follower's
    /// store incarnation, the event SHARD.md §8's re-add window assertion reads, with
    /// the seeds that saw one: D-061 measures a counter's rate over those.
    match_starts: usize,
    seeds_with_a_match_start: u64,
    inbox_drops: usize,
    snapshots_taken: usize,
    /// Takes the fold paired with their own checkpoints
    /// ([`raft::Report::snapshot_takes`]), against `snapshots_taken`, which
    /// counts the raw records. The two are here together because the fold went
    /// silently empty once before, at D-060, and every assertion built on it
    /// passed while it did.
    takes_paired: usize,
    /// Snapshots a server really installed, and prefixes a restart re-stated:
    /// one event kind, split because D-065 made the second population large and
    /// counting them together reported installs that never happened
    /// ([`raft::Report::snapshots_installed_and_restated`]).
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    snapshots_installed: usize,
    snapshot_prefixes_restated: usize,
    snapshot_resumes: usize,
    snapshot_versions_deleted: usize,
    snapshot_takes_reused: usize,
    snapshot_streams_at_once: usize,
    compactions: usize,
    /// D-065's own path: the compactions a replica made while it was not leading,
    /// with the seeds that saw one, and the largest in-memory log any follower
    /// replica held over the sweep — Stage B's exit measurement (Q39), printed at
    /// every tier beside the bound `raft::FOLLOWER_LOG_BOUND` asserts per seed.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    follower_compactions: usize,
    seeds_with_a_follower_compaction: u64,
    /// Of those, the ones whose prefix swallowed the configuration entry in
    /// force, so D-029's revert floor is what the replica reverts to from there.
    follower_compactions_swallowing_the_config: usize,
    largest_follower_log: u64,
    largest_follower_log_seed: u64,
    /// The distribution of the per-seed largest follower log, in multiples of
    /// `raft::SNAPSHOT_THRESHOLD`: the bound is set from the whole shape, not
    /// from the maximum alone.
    follower_log_multiples: BTreeMap<u64, u64>,
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
            TraceEvent::RaftReseeded { server, .. } if refused.contains(server) => {
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
        // PROPOSED(D-069): the core already does what `RaftMatchStarted` reports;
        // this sweep is where it is seen, on every seed.
        let match_starts = report.count(|e| matches!(e, TraceEvent::RaftMatchStarted { .. }));
        self.match_starts += match_starts;
        self.seeds_with_a_match_start += u64::from(match_starts > 0);
        self.inbox_drops += report.count(|e| matches!(e, TraceEvent::RaftInboxDropped { .. }));
        self.snapshots_taken +=
            report.count(|e| matches!(e, TraceEvent::RaftSnapshot { taken: true, .. }));
        self.takes_paired += report.snapshot_takes().len();
        // PROPOSED(D-078): a follower compacts its log to its own applied index.
        let (installed, restated) = report.snapshots_installed_and_restated();
        self.snapshots_installed += installed;
        self.snapshot_prefixes_restated += restated;
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
        // PROPOSED(D-078): a follower compacts its log to its own applied index.
        let (follower_compactions, swallowed) = report.follower_compactions();
        self.follower_compactions += follower_compactions;
        self.follower_compactions_swallowing_the_config += swallowed;
        self.seeds_with_a_follower_compaction += u64::from(follower_compactions > 0);
        let (longest, _) = report.largest_follower_log();
        if longest > self.largest_follower_log {
            self.largest_follower_log = longest;
            self.largest_follower_log_seed = report.seed;
        }
        *self
            .follower_log_multiples
            .entry(longest / raft::SNAPSHOT_THRESHOLD)
            .or_default() += 1;
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
        // The write bound is asked of every key some client wrote to after the heal
        // (SHARD.md §8), so the figure is the worst of those keys' first
        // completions and not the best of them.
        for took in report.writes_after_heal_by_key().into_values().flatten() {
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
            (
                "takes paired with their checkpoints",
                self.takes_paired as u64,
            ),
            ("log compactions", self.compactions as u64),
            (
                "compactions by a replica that was not leading",
                self.follower_compactions as u64,
            ),
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
        // PROPOSED(D-078): an absence with its reason. This scenario proposes one
        // configuration — the initial one, which is no log entry — and never changes
        // it, so no prefix a compaction drops can contain a configuration entry and
        // D-029's revert floor is unreachable here by construction. The measure is
        // kept in this sweep all the same, because a count above zero would mean it
        // is reading something other than what it names; where the floor *is*
        // reachable is the membership scenario, and that is where it is asserted
        // positive (`MembershipCoverage::assert_complete`).
        assert_eq!(
            self.follower_compactions_swallowing_the_config, 0,
            "the raft scenario changes no configuration, so nothing here can swallow \
             one: {self:?}"
        );
        // PROPOSED(D-069): `RaftMatchStarted` is on every seed of this sweep, not
        // merely somewhere in it: every leader's first answer from a follower raises
        // `matched` under the incarnation it carried, so the count is 1 000 of 1 000
        // seeds at the premerge and every tier below. A whole-sweep total above zero
        // is what a wrong emission rule passes; the seeds are what a rule that stops
        // emitting on a tier's worth of seeds fails. The rate is over the seeds the
        // assertion sees, as D-061 measures it: 100 %.
        assert_eq!(
            self.seeds_with_a_match_start, self.seeds,
            "a run of this sweep saw no leader's match rise under a follower's incarnation: \
             {self:?}"
        );
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
            // PROPOSED(D-078): both halves of the split, not the sum. The split is
            // read off a record's position in the trace, and a rule that put the
            // whole population on one side would satisfy an assertion on that side
            // alone — which is how this counter, called "installed" when it held
            // both, reported installs that never happened. Asserting each half
            // says the rule divides something. (`Restating`'s own unit tests are
            // where the rule itself is held; this is the sweep's non-vacuity.)
            assert!(
                self.snapshot_prefixes_restated > 0,
                "the sweep never saw a restart re-state a compacted or installed \
                 prefix, so the install/re-statement split is putting everything \
                 on one side: {self:?}"
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
        // PROPOSED(D-056): the send queue's drop is a state this scenario does not
        // reach, asserted as an absence with its reason rather than left printed. The
        // default queue holds 1 024 frames and drains at a gigabit, so a drop needs
        // 1 025 frames outstanding to one destination — about 1 025 frames inside the
        // 8 µs a kilobyte frame takes to write — which the raft workload cannot
        // produce: 0 at 20, 100 and 1 000 seeds here and in D-056's own measurement.
        // The deepest same-instant burst on one link measured on seed 42 is 348 frames,
        // so the margin is a factor of three, not a large one; when Stage B's batch
        // frames put many ranges on one socket this is the assertion that will say so,
        // and it becomes a bound rather than a zero.
        assert_eq!(
            self.queue_drops, 0,
            "a sending socket's queue filled and dropped a frame, which no Phase 2 \
             scenario reaches: {self:?}"
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
/// the sweep asserts seed by seed, and every server it counts is one of the joiners;
/// a store refused for anything but lost state fails the run's own check. The fold's
/// other bound, the learner phase itself, is held by `membership`'s own unit test,
/// where a window can be written by hand.
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
                let fed = report.snapshot_fed_joiners();
                if fed.is_empty() {
                    return Err(format!(
                        "seed {seed}: no server joining the configuration installed a snapshot \
                         in its learner phase"
                    ));
                }
                // And every server counted is a *joining* one. Servers 1 to 3 are the
                // initial voters and join nothing, so an install of theirs — an
                // ordinary follower catching up behind a compacted leader, which this
                // scenario produced before D-058 — is not what issue #46 asks for. The
                // emptiness check above cannot see the difference, and a fold that
                // counted every server passed every tier.
                let voters: Vec<_> = fed
                    .iter()
                    .copied()
                    .filter(|&(server, _, _)| server <= membership::INITIAL_VOTERS)
                    .collect();
                if voters.is_empty() {
                    Ok(())
                } else {
                    Err(format!(
                        "seed {seed}: {voters:?} are counted as snapshot-fed joiners, but \
                         servers 1 to {} are the initial voters and join nothing",
                        membership::INITIAL_VOTERS
                    ))
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
    // PROPOSED(D-069): the three events of SHARD.md §8 the core already does the
    // work for. This scenario is the one that drives changes, so it is where a
    // learner's round and an accepted change are seen.
    match_starts: usize,
    learner_rounds: usize,
    learner_rounds_caught_up: usize,
    changes_accepted: usize,
    seeds_with_a_match_start: u64,
    seeds_with_a_learner_round: u64,
    seeds_with_a_change_accepted: u64,
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
    /// D-065's own path in the scenario that has configuration entries to
    /// swallow: the compactions a replica made while not leading, and of those
    /// the ones whose prefix swallowed the configuration entry in force, so that
    /// D-029's revert floor is what the replica would revert to from there.
    ///
    /// This lives here and not only in the raft sweep because the raft scenario
    /// appends no configuration entry after the first, so its `swallowed` is
    /// **zero by construction** — a measure that can only read zero evidences
    /// nothing. The review of this slice found the corrected measure calculated
    /// nowhere that ships and asserted nowhere at all; it is asserted below, at
    /// every tier, on the rate this scenario really has.
    // PROPOSED(D-078): a follower compacts its log to its own applied index.
    follower_compactions: usize,
    follower_compactions_swallowing_the_config: usize,
    seeds_with_a_swallowed_config: u64,
}

impl MembershipCoverage {
    fn add(&mut self, report: &membership::Report) {
        self.seeds += 1;
        let joiners = report.snapshot_fed_joiners();
        self.snapshot_fed_joiners += joiners.len();
        self.seeds_with_a_snapshot_fed_joiner += u64::from(!joiners.is_empty());
        self.compactions += report.count(|e| matches!(e, TraceEvent::RaftCompacted { .. }));
        // PROPOSED(D-078): a follower compacts its log to its own applied index.
        let (follower_compactions, swallowed) = raft::follower_compactions(&report.records);
        self.follower_compactions += follower_compactions;
        self.follower_compactions_swallowing_the_config += swallowed;
        self.seeds_with_a_swallowed_config += u64::from(swallowed > 0);
        // PROPOSED(D-069): the three events emitted from this stage, each with the
        // seeds that saw one: D-061 measures a counter's rate over those.
        let match_starts = report.count(|e| matches!(e, TraceEvent::RaftMatchStarted { .. }));
        let learner_rounds = report.count(|e| matches!(e, TraceEvent::RaftLearnerRound { .. }));
        let changes = report.count(|e| matches!(e, TraceEvent::RaftChangeAccepted { .. }));
        self.match_starts += match_starts;
        self.learner_rounds += learner_rounds;
        self.changes_accepted += changes;
        self.seeds_with_a_match_start += u64::from(match_starts > 0);
        self.seeds_with_a_learner_round += u64::from(learner_rounds > 0);
        self.seeds_with_a_change_accepted += u64::from(changes > 0);
        self.learner_rounds_caught_up += report.count(|e| {
            matches!(
                e,
                TraceEvent::RaftLearnerRound {
                    caught_up: true,
                    ..
                }
            )
        });
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
                TraceEvent::RaftCompacted {
                    server, through, ..
                } => {
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
            // PROPOSED(D-069): a round that caught up inside the minimum election
            // timeout is the one of the four that is not on every seed — 2 494 rounds
            // over 1 000 seeds, against 5 171 rounds — so it is the one asserted as a
            // whole-sweep total. The other three are asserted per seed below.
            (
                "learner rounds that caught up",
                self.learner_rounds_caught_up as u64,
            ),
        ] {
            assert!(seen > 0, "the membership runs never saw {what}: {self:?}");
        }
        assert_eq!(
            self.seeds_with_a_snapshot_fed_joiner, seeds,
            "a membership run fed no joining server a snapshot in its learner phase: {self:?}"
        );
        // PROPOSED(D-069): SHARD.md §8's three events the core already does the work
        // for, each on *every* seed of this scenario and asserted seed by seed: the
        // driver grows the configuration on every seed, so a change is accepted and a
        // learner is tracked and caught up, and every leader's first answer from a
        // follower raises `matched` under the incarnation it carried. Measured at a
        // thousand seeds in release: 1 000 of 1 000 for each. A total above zero is
        // what an emission rule gone wrong passes — one event on one seed satisfies
        // it — and these say the rule fires where it must. The rate is over the seeds
        // the assertion sees, as D-061 measures it: 100 % for all three.
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
            assert_eq!(seen, seeds, "a membership run saw no {what}: {self:?}");
        }
        // PROPOSED(D-078): D-029's revert floor on a follower, which is what D-065
        // said this change would make routine, observed on this scenario rather than
        // argued. A follower compacts to its own applied index; when the
        // configuration entry in force sits inside the prefix that compaction drops,
        // the floor — the configuration held at the new prefix's end — is what that
        // replica reverts to. Before this change the floor was reached on 3 of 10 000
        // seeds (issue #56); here it is reached on every seed of the scenario.
        //
        // The rate, measured before it is asserted (D-061): D-078 records the sweep.
        // At 100 % of seeds the gate's twenty support it, so it is asked at every
        // tier and not merely as a sweep total — a total above zero is what a measure
        // gone structural passes, and a measure that reads the whole population is
        // exactly the failure the review of this slice found in the first build of
        // this counter.
        assert_eq!(
            self.seeds_with_a_swallowed_config, seeds,
            "a membership run saw no follower compaction swallow the configuration in \
             force, which is D-029's revert floor on a follower: {self:?}"
        );
        assert!(
            self.follower_compactions_swallowing_the_config < self.follower_compactions,
            "every follower compaction swallowed a configuration entry, which is the \
             shape of a measure that is structural rather than observed (D-039): {self:?}"
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
                (
                    "step-downs of a leader outside C_new",
                    self.step_downs_outside_new as u64,
                ),
                ("configuration reverts", self.config_reverts as u64),
            ] {
                assert!(seen > 0, "the membership runs never saw {what}: {self:?}");
            }
        }
        // An election while joint needs the partition to cut a leader off inside the
        // joint phase itself. D-061, the owner's rule of 2026-09-15: a state reached on
        // under 5 % of seeds is asserted from the thousand-seed tier (the premerge and
        // the nightly) and printed with the coverage at every tier. On this tree it is
        // on 34 of the first thousand seeds, 3.4 % (34 elections, one
        // per seed; 31 seeds and the same 34 elections on the tree with D-056's send
        // queue alone, and 463 elections at the nightlies' ten thousand before the queue
        // and D-058, so at most 4.6 % of their seeds). At 3.4 % a hundred seeds see none
        // with probability 0.966^100 = 0.031 and the gate's twenty with 0.50; a thousand
        // with 0.966^1000 = 9.5e-16.
        if seeds >= 1000 {
            assert!(
                self.elections_while_joint > 0,
                "the membership runs never saw elections while joint: {self:?}"
            );
        }
        // PROPOSED(D-058): an install whose snapshot's configuration is older than the
        // receiver's, taking the receiver back to the installed prefix. The owner's
        // decision of 2026-09-15, recorded in D-058: asserted from the thousand-seed tier
        // (the premerge and the nightly) and at no lower tier, and printed with the
        // coverage at every tier. On this tree it happens on 25 of the first thousand
        // seeds, once on each, and on 3 of the first hundred — seeds 40, 94 and 95 (on
        // the tree with D-056's send queue alone it was 28 of the thousand and one of
        // the hundred, seed 97). At 2.5 % a hundred seeds see none with probability
        // 0.975^100 = 0.080 and the gate's twenty with 0.60, so the assertion there
        // would fail a tree with nothing wrong on the draw alone; a thousand see none
        // with 0.975^1000 = 1.3e-11.
        if seeds >= 1000 {
            assert!(
                self.reverts_to_a_prefix > 0,
                "the membership runs never saw reverts to a compacted or installed prefix: \
                 {self:?}"
            );
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
///
/// Both are fed the records as the sweep feeds them, each event with the node that
/// traced it ([`traced`]), so the comparison covers the checks as they are keyed by
/// group — including the two range events, which name no server of their own
/// (SHARD.md §8).
fn compare(seed: u64, variant: Variant, records: &[TraceRecord]) -> Result<bool, String> {
    let servers = raft::SERVERS as usize;
    let mut checker = Checker::new(servers);
    let mut fed = 0;
    let mut violated = false;
    for step in 1..=PREFIXES {
        let stop = records.len() * step / PREFIXES;
        while fed < stop {
            let next = (fed + CHUNK).min(stop);
            checker.extend(traced(&records[fed..next]));
            fed = next;
        }
        let incremental = checker.verdict();
        let whole = invariants::all(traced(&records[..stop]))
            .and_then(|()| invariants::commit_majority(traced(&records[..stop]), servers));
        violated |= whole.is_err();
        if incremental != whole {
            return Err(format!(
                "seed {seed}: under {variant:?}, over the first {stop} of {} records, the incremental checker said {incremental:?} and the fold over the whole prefix said {whole:?}",
                records.len()
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
        compare(seed, variant, &raft::run(seed, variant).records)
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

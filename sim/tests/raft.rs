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
    assert_eq!(first.jsonl.as_bytes(), second.jsonl.as_bytes());
}

/// The seed-42 trace is written for the studio.
#[test]
fn the_seed_42_trace_is_written_for_the_studio() {
    let report = raft::run(42, Variant::Correct);
    write_trace("raft-42", &report.jsonl);
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
/// Today the seed does not reach that situation, and the test asserts so. The
/// fault list is the same through the 9.691 s partition, but the run elects a
/// different leader after it, so the leader-relative faults that follow hit other
/// servers: server 2 finishes its install before the 12.541 s partition (adopted
/// at 11.760 s), and that partition isolates server 2 itself, where no chunk
/// reaches it. The longest AppendEntries-less stretch that holds an
/// InstallSnapshot is 161.6 ms, server 3's from 11.822 s, against its 394 ms
/// bound. The day [`raft::Report::snapshot_fed_timer_gaps`] is not empty, the seed
/// reaches the situation again and the pin should assert it: those gaps present,
/// and the check green.
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
/// Today the seed does not reach that situation, and the test asserts so. The
/// partition comes at the same instant and isolates server 1 alone as before, but
/// the log history before it has moved: leader 3 compacted only through 324, at
/// 13.677 s, so server 1 was kept current by AppendEntries, through index 336 by
/// 14.029 s, and had no install in flight — no chunk was sent to it between
/// 13.152 s and 14.667 s. It pre-voted at 14.159 s, 131.6 ms after the leader's
/// last AppendEntries at 14.027 s, 43.5 % of its bound, with no restatement
/// between. The check without D-039's arm flags nothing on the whole run, and its
/// closest stretch across a restatement is 167.1 ms, 55.3 % of the bound. With the check
/// green, [`raft::Report::timer_gaps_rescued_by_restatement`] is every gap of that
/// replay; the day it is not empty the seed reaches the situation again, and the
/// pin should assert it: those gaps present, and the check green.
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
/// disagree. The Figure 8 driver's crash lands at the same instant on the leader
/// in force, which is server 3 this time and restarts clean. The run's only
/// refusal is server 2's at 12.585 s, a missing log head, with its floor at 251,
/// and the leader re-seeds it from snapshot 312, above that floor. So the two
/// floor rules agree at every event, the seed would pass the old checker too, and
/// this pin holds the seed green without exercising the fix. The day
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
/// install. What moved is the install it hit. On the nightly's trace the leader
/// took a newer snapshot, 129 over 120, while the stream ran, and the stream
/// started over on it, so the install finished 459 ms after that heal; today one
/// take feeds the stream in seven chunks, with no take during it, and the install
/// finishes 182 ms after the heal under the correct server and 208 ms as built. So the crash lands 318 ms
/// and 292 ms after the install, and 262 ms and 230 ms after the adoption had
/// already closed. No crash lands inside any of the correct run's 12 adoption
/// windows or the variant's 9, and the variant passes the seed. The day
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
/// Today's run does not replay the wedge. The fault times are the nightly's
/// through the restart at 16.114 s, but leadership resolves differently, so the
/// crashes and isolations aimed at a leader or its neighbour hit other servers,
/// and D-044's refusal storm adds crashes of server 3 after that. The correct
/// server re-takes no index at all here. What the test asserts is what the run
/// does reach, and what it does not. It reaches D-042's reset: server 3 is
/// refused at 18.696 s, the leader resets its progress at 18.698 s, and it is
/// re-seeded at 18.970 s. It does not reach the wedge's shape: no re-take lands
/// under a live stream and scrambles it, and no two followers go uncounted after
/// the last heal.
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
/// says, and seed 680 below agrees, where `SharedSnapshotDir` alone fails exactly
/// as the pair does. The variant set still makes the pair a run the sweep can ask
/// about, and this seed is asked.
///
/// On this tree no run of the seed has two uncounted followers, and each variant
/// reaches only its own half, harmlessly:
///
/// - under `IgnoreIncarnation` the leader's progress for server 3 goes stale — its
///   last success acknowledged 572 at 18.626 s, it is refused at 18.696 s, and the
///   leader's 826 AppendEntries after that never probe below 572 while server 3
///   rejects 814 of them and accepts none — but server 1 is countable and no
///   re-take happens at all;
/// - under `SharedSnapshotDir` leader 2 opens a stream of snapshot 395 to server 3
///   at 15.051 s and re-takes 395 into `/raft/snap-395` at 15.071 s, under that
///   live stream, and it is harmless: server 3 already held 395 and answered
///   `Installed` at 15.109 s, three times, all dropped because its sends were
///   blocked until 15.406 s, and no follower goes uncounted after the heal;
/// - under the pair both happen, one after the other and to the same follower:
///   the harmless re-take at 15.071 s, then stale progress for server 3 — its
///   last success acknowledged 416 at 15.971 s, it is refused at 16.128 s, and
///   558 AppendEntries at or above 416 draw 543 rejections — while server 1 stays
///   countable.
///
/// The leader in force at the last heal commits within 21 to 45 ms of it under
/// each. The test asserts each half where its variant carries it, and the wedge's
/// shape absent in every run.
#[test]
fn seed_5909_passes_under_both_bugs_together_which_is_the_finding() {
    for variants in [
        Variants::from(Variant::IgnoreIncarnation),
        Variants::from(Variant::SharedSnapshotDir),
        Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]),
    ] {
        let report = raft::run(5909, variants);
        assert_eq!(
            report.check().err(),
            None,
            "seed 5909 under {variants:?} no longer passes: the pin's story is out of date"
        );
        assert_no_stream_wedge(&report);
        if variants.contains(Variant::IgnoreIncarnation) {
            assert!(
                report.stale_progress(3).is_some(),
                "seed 5909 under {variants:?} no longer leaves the leader's progress for the \
                 refused server 3 stale: re-audit the pin"
            );
        }
        if variants.contains(Variant::SharedSnapshotDir) {
            assert!(
                !report.retakes_under_streams().is_empty(),
                "seed 5909 under {variants:?} no longer re-takes under a live stream: \
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

/// The combined variant pinned on the seed the sweep does catch it on — seed 680
/// — and, said plainly, what that seed does and does not show.
///
/// It is the seed of the first thousand on which a server carrying
/// `{IgnoreIncarnation, SharedSnapshotDir}` is caught, by the liveness check: no
/// client write completed after the last heal at 28.683 s, which is the refusal
/// storm's fifth restart of server 3. The correct server passes it. The seed does
/// not need both bugs: `SharedSnapshotDir` alone is caught with the byte-identical
/// message, and `IgnoreIncarnation` alone passes. Swept over seeds 0..1000 in
/// release, the pair is caught on 1 of 1000 (this seed), `SharedSnapshotDir` alone
/// on 1 of 1000 (this seed), `IgnoreIncarnation` alone on 0 of 1000, and no seed
/// catches the pair without a single. Nor was a wedge that needs both ever seen:
/// seed 5909's was D-043's alone (above).
///
/// What wedges it is D-043's bugs, and the test asserts the mechanism on the
/// `SharedSnapshotDir` run. Leader 1 wins term 10 at 8.407 s and never commits
/// again; the last commit on any server is at 7.653 s. Server 3's
/// acknowledgements of the leader's first entry were all dropped by the Figure 8
/// driver's blocks and server 2 is behind the leader's snapshot, so the leader
/// designates both to be fed one. The seed draws no re-take arm; the server re-takes on
/// its own, twenty times into `/raft/snap-187` between 8.595 s and 10.454 s, five
/// of them under a live stream (9.563, 10.091, 10.359, 10.407 and 10.454 s). The
/// three streams those land under open half-written directories and fail. The
/// fourth, to server 2 at 10.699 s, has no take under it and still never
/// completes: server 2 kept a partial `000005.sst` from an earlier stream under
/// the same identity, so the whole file's first chunk reads as a duplicate and is
/// answered `More` for `000004.sst`, 8739 times until the run ends, and every
/// `More` keeps the stream from timing out — the duplicate hazard D-043 names.
/// Server 3 waits behind it in the one-stream backlog, and both followers go
/// uncounted after the heal.
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
fn seed_680_pins_the_combined_variant_and_the_stream_half_alone_catches_it_too() {
    let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
    let paired = raft::run(680, both)
        .check()
        .expect_err("seed 680 under {both:?} no longer reproduces: the pin's story is out of date");
    assert!(
        paired.contains("liveness"),
        "seed 680 under {both:?} is caught, but not by the liveness check: {paired}"
    );

    // The honest half of the pin: the stream bug alone reaches the same wedge on
    // this seed, so 680 is not evidence that the pair is needed.
    let stream = raft::run(680, Variant::SharedSnapshotDir);
    let stream_only = stream.check().expect_err(
        "SharedSnapshotDir alone no longer catches seed 680: the pin's story is out of date",
    );
    assert_eq!(
        paired, stream_only,
        "the pair and the stream half alone no longer fail seed 680 the same way: \
         the pair may now be buying a catch of its own, which is worth recording"
    );
    assert_stream_wedge(&stream);

    // And the other half alone reaches nothing, here as everywhere.
    assert_eq!(
        raft::run(680, Variant::IgnoreIncarnation).check().err(),
        None,
        "IgnoreIncarnation alone now catches seed 680: the pin's story is out of date"
    );

    // The pair rule (CLAUDE.md): the correct server passes the seed its buggy
    // siblings fail — without ever meeting a re-take on it.
    let correct = raft::run(680, Variant::Correct);
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
        "the correct server now re-takes an index on seed 680: {retaken:?}; the pin can now \
         assert whether its stream survived the re-take"
    );
}

/// The stream half of the wedge on seed 680 under `SharedSnapshotDir`, from the
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
        .expect("seed 680 elects a leader");
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
/// Today the seed does not reach that situation, under the correct server or the
/// refusal as built, and the test asserts both. Not because D-044's aimed crash
/// re-drew the schedule: `Fault::CrashRefused` is drawn from its own stream and
/// appended last, it first fires at 15.141 s, after the premerge's violation at
/// 13.932 s, and every fault through 13.532 s is at the instant it was. What moved
/// is the disk. The traces first differ at 5.416 s, where server 2's adoption
/// finishes 3.7 ms later — consistent with D-044 rewriting the store marker there
/// — and the bit rot a crash draws block by block over a node's files then lands
/// elsewhere: at 7.566 s it rots a checkpoint's copy, `snap-125-6/000001.sst`,
/// instead of the live `000001.sst`, and server 3 recovers clean. No engine that
/// opened is refused for lost state anywhere in the run. Its one refusal is
/// server 3's at 15.424 s, for an unreadable `MANIFEST-000009`: a store damaged
/// before its engine opened, with no engine to flush anything, and it is re-seeded
/// (adopted at 15.624 s) before its next crash at 17.902 s. `RefusalNotDurable`
/// passes the seed the same way (adopted at 15.600 s, crashed at 17.627 s); that
/// variant's own sweep test catches the shape on other seeds. The day
/// [`raft::Report::restarts_after_lost_state_refusal`] is not empty, the pin
/// should assert the mechanism: under `RefusalNotDurable` state machine safety
/// reporting the recovered applied index, and under the correct server the
/// restart refused on the store's lost mark and the check green.
#[test]
fn seed_687_which_the_premerge_found_stays_green() {
    for variant in [Variant::Correct, Variant::RefusalNotDurable] {
        let report = raft::run(687, variant);
        let restarts = report.restarts_after_lost_state_refusal();
        assert!(
            restarts.is_empty(),
            "seed 687 under {variant:?} restarts a server refused for lost state before an \
             install replaced its store, as (server, refused, restarted): {restarts:?}; pin the \
             mechanism rather than the green"
        );
        assert!(
            !report.has(|e| matches!(
                e,
                TraceEvent::RaftRefused { reason, .. } if reason.starts_with(LOST_STATE)
            )),
            "seed 687 under {variant:?} refuses a store whose opened engine lost state again: \
             the refused engine's quiesce is reachable here; re-audit the pin"
        );
        assert_eq!(
            report.check().err(),
            None,
            "seed 687 under {variant:?} no longer passes: re-audit the pin"
        );
    }
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
/// does the whole check. The schedule is the nightly's: this tree's trace of the
/// seed is the nightly's byte for byte once the new `decidedNs` fields are removed.
#[test]
fn seed_1885_which_the_nightly_failed_on_a_trace_timestamp_passes_by_decision_time() {
    let report = raft::run(1885, Variant::Correct);
    let straddle = assert_rise_straddles_the_isolation(
        &report,
        (1, 8, 10, "follower"),
        "pre-vote: server 1 raised its term from 8 to 10 while isolated from Instant(15.203s) to Instant(17.112s)",
    );
    assert_eq!(
        straddle.causes,
        [(2, "request-vote", 10)],
        "seed 1885: the rise was not decided at the delivery of server 2's RequestVote of term \
         10, so its decision time is not that message's step: {straddle:?}"
    );
    report.check().unwrap();
}

/// Seed 2023 of the same nightly, the same gap: the correct server failed with
/// *pre-vote: server 1 raised its term from 13 to 14 while isolated from 19.22 s to
/// 20.822 s*. Server 3's AppendEntries of term 14 was delivered to server 1 and
/// stepped at 19.217879 s, 2.12 ms before the partition; the adopted term was
/// durable and traced 671 µs inside the window, and nothing from a server reached
/// server 1 until the heal. The test asserts what seed 1885's does (D-047).
#[test]
fn seed_2023_which_the_nightly_failed_on_a_trace_timestamp_passes_by_decision_time() {
    let report = raft::run(2023, Variant::Correct);
    let straddle = assert_rise_straddles_the_isolation(
        &report,
        (1, 13, 14, "follower"),
        "pre-vote: server 1 raised its term from 13 to 14 while isolated from Instant(19.22s) to Instant(20.822s)",
    );
    assert_eq!(
        straddle.causes,
        [(3, "append-entries", 14)],
        "seed 2023: the rise was not decided at the delivery of server 3's AppendEntries of term \
         14, so its decision time is not that message's step: {straddle:?}"
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
/// Two of the eleven, `IgnoreIncarnation`'s seeds 2509 and 5990, no longer reach
/// the straddle, and the pin asserts its absence with the reason (D-049). On
/// both, the leader under D-042's bug keeps a stale match for a follower refused
/// for lost state, so it never re-seeds it, and with the third server away its
/// check quorum, which counts a refused follower's rejections only beside re-seed
/// progress, steps it down — seed 2509's leader 2 of term 12 at 14.547925798 s
/// leaving server 3 uncounted, seed 5990's leader 3 of term 11 at 13.944738313 s
/// leaving server 2 — where the leader before D-049 kept its office on those
/// rejections. That step-down is the first record in which either trace differs
/// from the tree before D-049, and it comes before the isolation the straddle was
/// at (14.859 s and 14.448 s), so the run after it is another run. The day a
/// straddle returns on either, the absence assertion fails and the pin can be
/// upgraded back.
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
    // D-049: (seed, the step-down that moved the schedule: server, term,
    // the follower left uncounted, when, in nanoseconds of global time).
    const MOVED: [(u64, (u64, u64, u64, u64)); 2] = [
        (2509, (2, 12, 3, 14_547_925_798)),
        (5990, (3, 11, 2, 13_944_738_313)),
    ];
    let verdicts = sweep(pairs.len() as u64, |i| {
        let (seed, variant, rise, original) = pairs[usize::try_from(i).expect("small")];
        let report = raft::run(seed, variant);
        match MOVED.iter().find(|(moved, _)| *moved == seed) {
            Some(&(_, (server, term, follower, nanos))) => {
                assert_eq!(
                    report.isolation_term_straddles(),
                    Vec::new(),
                    "seed {seed} under {variant:?} straddles an isolation again: upgrade the pin"
                );
                let first = report.records.iter().find_map(|r| match &r.event {
                    TraceEvent::RaftQuorumLost {
                        server,
                        term,
                        uncounted,
                    } if !uncounted.is_empty() => Some((*server, *term, uncounted.clone(), r.at)),
                    _ => None,
                });
                assert_eq!(
                    first,
                    Some((
                        server,
                        term,
                        vec![follower],
                        ananke_env::Instant::from_nanos(nanos)
                    )),
                    "seed {seed} under {variant:?}: the step-down that moved the schedule is not \
                     the one recorded"
                );
            }
            None => {
                assert_rise_straddles_the_isolation(&report, rise, original);
            }
        }
        (seed, variant, report.check().err())
    });
    for (seed, variant, verdict) in &verdicts {
        eprintln!("seed {seed} under {variant:?}: {verdict:?}");
        assert!(
            !verdict.as_ref().is_some_and(|v| v.contains("pre-vote")),
            "seed {seed} under {variant:?} still reports a pre-vote violation: {verdict:?}"
        );
        assert_eq!(
            verdict, &None,
            "seed {seed} under {variant:?}: the run no longer passes the check outright"
        );
    }
}

/// The trace-timestamp gap on one run (D-047): the run holds exactly one
/// term rise that straddles the start of its server's isolation — `rise` names its
/// (server, term before, term after, role) — decided before the isolation began and
/// traced inside it, with no message from a server delivered to that server in the
/// window; the pre-vote check by durability time fails with `original`, the
/// nightly's message, and the same check by decision time passes. Returns the
/// straddle, so a pin can tie its decision time to the message the step took.
fn assert_rise_straddles_the_isolation(
    report: &raft::Report,
    rise: (u64, u64, u64, &str),
    original: &str,
) -> raft::TermStraddle {
    let (seed, variants) = (report.seed, report.variants);
    let straddles = report.isolation_term_straddles();
    let [straddle] = straddles.as_slice() else {
        panic!(
            "seed {seed} under {variants:?} no longer has exactly one term rise straddling an \
             isolation's start: {straddles:?}; re-audit the pin"
        );
    };
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
            write_trace(&format!("raft-{seed}"), &report.jsonl);
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
/// `moved`. Reading a rise earlier can remove a pre-vote catch only where the rise
/// was decided before an isolation's start and traced after it, so a removed
/// pre-vote catch without such a straddle is a fault in the reasoning and fails the
/// sweep. A removed timer catch is noted with the decisions of the flagged server
/// that straddle the flag, the way a timer catch can be removed.
fn checked(report: &raft::Report, moved: &Mutex<MovedSeeds>) -> Option<String> {
    let (seed, variants) = (report.seed, report.variants);
    let verdict = report.check();
    match report.moved_by_decision_time(&verdict) {
        Some(Moved::Lost(was)) if was.starts_with("pre-vote: ") => {
            let straddles = report.isolation_term_straddles();
            assert!(
                !straddles.is_empty(),
                "seed {seed} under {variants:?}: decision time removed the catch `{was}`, but no \
                 term rise straddles an isolation's start"
            );
            let straddles: Vec<String> = straddles
                .iter()
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
                .collect();
            moved
                .lock()
                .unwrap()
                .removed
                .push((seed, format!("{was} [{}]", straddles.join("; "))));
        }
        Some(Moved::Lost(was)) => {
            let straddling: Vec<String> = report
                .timer_gaps_by(TimerResets::ALL, RecordTime::Durable)
                .first()
                .map(|gap| {
                    report
                        .decisions_straddling(gap.server, gap.at)
                        .iter()
                        .map(|r| {
                            let event = format!("{:?}", r.event);
                            let name = event.split([' ', '{', '(']).next().unwrap_or("");
                            format!(
                                "{name} decided {:?} before the flag, traced {:?} after",
                                gap.at.duration_since(r.decided),
                                r.at.duration_since(gap.at)
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            moved
                .lock()
                .unwrap()
                .removed
                .push((seed, format!("{was} [straddling the flag: {straddling:?}]")));
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
/// tier and the fault's firing at every tier, as `RefusalNotDurable` is
/// (D-044): what a gate run must still see is that the storm was drawn
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
    // must come before a leader re-seeds the server. That is a thin conjunction
    // — two of a hundred release seeds, one of the gate's twenty — so it is
    // asserted at the hundred-seed tier and reported at every tier, as
    // `SharedSnapshotDir` is at the nightly's (D-043).
    if seeds() >= 100 {
        assert!(!caught.is_empty(), "RefusalNotDurable was never caught");
    }
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
/// seed in four and got as far as the stream on 14 of 100 release seeds.
///
/// It has not been shown to make the variant catchable at any tier: 0 of 100
/// release seeds, 1 of 1000 — seed 680, by the liveness check, `no client write
/// completed after the last heal at 28.683 s` — and that one catch is not this
/// arm's: seed 680 does not draw it, and its re-takes are the server's own
/// (`seed_680_pins_the_combined_variant_and_the_stream_half_alone_catches_it_too`).
/// The arm reached a live stream on 151 of 1000 seeds and caught none of them.
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
/// taken happens often on its own — 48 of 100 seeds, 532 of 1000 — and is
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
    assert_eq!(first.jsonl.as_bytes(), second.jsonl.as_bytes());
}

/// The positive control: the correct server passes 3 → 5 → 3 under partition on
/// every seed, and the runs reached the states that matter.
#[test]
fn the_correct_server_passes_the_membership_scenario_on_every_seed() {
    let coverage = Mutex::new(MembershipCoverage::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = membership::run(seed, Variant::Correct);
        coverage.lock().unwrap().add(&report);
        report
            .check()
            .inspect_err(|_| write_trace(&format!("membership-{seed}"), &report.jsonl))
    });
    let coverage = coverage.into_inner().unwrap();
    eprintln!("Membership: {coverage:?}");
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete(seeds());
}

/// The negative control: a server that counts one merged majority while joint
/// (thesis §4.3) is caught by the membership scenario's checks on some seed.
#[test]
fn a_server_that_counts_one_majority_in_joint_consensus_is_caught() {
    let caught: Vec<String> = sweep(seeds(), |seed| {
        membership::run(seed, Variant::SingleMajorityInJointConsensus)
            .check()
            .err()
    })
    .into_iter()
    .flatten()
    .collect();
    eprintln!(
        "SingleMajorityInJointConsensus: caught on {} of {} seeds, first: {}",
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
}

impl MembershipCoverage {
    fn add(&mut self, report: &membership::Report) {
        self.seeds += 1;
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
        for event in report.events() {
            match event {
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
        ] {
            assert!(seen > 0, "the membership runs never saw {what}: {self:?}");
        }
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
                write_trace(&format!("quorum-{half:?}-{seed}"), &report.jsonl);
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

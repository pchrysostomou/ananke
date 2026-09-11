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
use ananke_raft::store::STORE_MARKER;
use ananke_sim::raft::DRIFT_BOUND_PPM;
use ananke_sim::raft::{self, Fault};
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

/// The first correct-server failures the ten-thousand-seed nightly ever produced,
/// both the timer check misreading a follower being fed a snapshot: seed 164, a
/// follower two hundred entries behind a compacting leader, fed by a train of
/// InstallSnapshot chunks the check did not count as the leader's contact; seed
/// 385, a follower cut off alone mid-install, campaigning a hundred milliseconds
/// after the install's restatement rebuilt its core with a fresh timer and
/// twenty-five past the bound (D-030, PROPOSED D-039). Both stay in the gate.
#[test]
fn seeds_164_and_385_which_the_first_nightly_found_stay_green() {
    for seed in [164, 385] {
        raft::run(seed, Variant::Correct).check().unwrap();
    }
}

/// The ten-thousand-seed nightly's seed 7381: a refused server re-seeded from a
/// snapshot older than the one its lost store had, then applying entries past it.
/// The checker's snapshot floor only ever rose, so the lost store's floor outlived
/// it and the applied entries read as covered rather than held. An installed
/// snapshot now sets the floor exactly (D-030). Stays in the gate.
#[test]
fn seed_7381_which_the_first_nightly_found_stays_green() {
    raft::run(7381, Variant::Correct).check().unwrap();
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
/// a directory carrying the store marker never opens fresh (PROPOSED D-041).
/// Stays in the gate.
#[test]
fn seed_6325_which_the_nightly_found_stays_green() {
    raft::run(6325, Variant::Correct).check().unwrap();
}

/// The ten-thousand-seed nightly's seed 5909 (run 34496762339): the leader's last
/// commit was 329 at 13.43 s and nothing committed for the remaining 5.4 s of the
/// run. Server 2 had been snapshot-fed since 7.46 s — 744 chunks, the stream
/// restarted from offset 0 six times — because the leader re-took the same
/// snapshot 329 five times into the one directory that stream was reading, so
/// sender and receiver never stood on the same file again; and server 3, refused
/// and re-seeded, was designated snapshot-fed and received nothing, its stream
/// queued behind server 2's never-ending one, while the leader's match index for
/// it stood above its rebuilt log, so every heartbeat it answered was rejected
/// and it was never counted. With neither follower countable the commit froze,
/// and the liveness check reported it. Takes are now versioned directories, a
/// stream pins the one it opened, and every designated follower is streamed to
/// at once (PROPOSED D-043), and a follower's store incarnation resets what the
/// leader knew of its log (PROPOSED D-042). Said plainly: the fixed tree's
/// schedule for this seed diverges from the nightly's trace — the snapshot record
/// grew by eight bytes, which moves the engine's flushes and with them every
/// checkpoint, and D-041 appends a crash storm to every schedule — so this pin
/// holds the seed green rather than replaying the failure; the shape itself is
/// carried by `SharedSnapshotDir` and `IgnoreIncarnation` below. Stays in the
/// gate.
#[test]
fn seed_5909_which_the_nightly_found_stays_green() {
    raft::run(5909, Variant::Correct).check().unwrap();
}

/// Seed 5909 under each of the two variants whose bugs wedged it, and under both
/// of them at once, pinned to what it actually does on this tree, which is pass.
/// That is the finding. Round two could only report it of the two variants
/// separately and had to record the pair as a run the mechanism could not make;
/// PROPOSED D-045 makes it, and the answer is the same.
///
/// The nightly's wedge needed both bugs at once: the leader re-took the same
/// snapshot 329 into the one directory server 2's stream was reading, so that
/// stream never completed, *and* the leader's `matched` for the re-seeded server
/// 3 stood above its rebuilt log, so server 3 was never counted either. Neither
/// half stalls a commit on its own, because a leader needs only one countable
/// follower for a majority: with D-043's fix in place the pinned stream
/// completes and the cluster commits through server 2 though the leader's
/// `matched` for server 3 is stale, and with D-042's fix in place server 3's
/// incarnation resets that `matched` and the cluster commits through server 3
/// though server 2's stream is scrambled. So a server carrying one of the two
/// bugs is not the server the nightly caught, and the run that is — one
/// carrying both — is `Variants::of(&[IgnoreIncarnation, SharedSnapshotDir])`,
/// which `Variant` as a single enum on `RaftConfig` could not express (PROPOSED
/// D-045's Context).
///
/// It can be expressed now, and seed 5909 still passes it. The reason is the
/// schedule, which is the second and independent reason this seed was never
/// going to replay. The nightly's trace for 5909 has been unreachable on this
/// branch since D-043 grew the snapshot record by eight bytes, which moves every
/// flush and every checkpoint's file set, and D-041, D-044 and the re-take arm
/// have each re-drawn the fault list since. On this tree seed 5909 draws three
/// Figure 8 drivers, two crashes, an isolation and the refusal storm, and
/// neither a snapshot-crash nor an adoption storm nor a re-take arm at all — so
/// the leader never re-takes under a running stream here whatever bugs it
/// carries, and there is no wedge for the pair to reach.
///
/// So this stays a pin that holds a seed green, worth keeping as the nightly's
/// seed and worth nothing as a replay. The replay is the pinned seed below,
/// which the pair does wedge and neither single does.
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
    }
}

/// The combined variant pinned on the seed the sweep does catch it on — seed 680
/// — and, said plainly, what that seed does not show.
///
/// Seed 5909 is the wedge this pair exists for and no longer reaches it (above),
/// so the pin stands in for it: this is the seed of the first thousand on which
/// a server carrying `{IgnoreIncarnation, SharedSnapshotDir}` is caught, by the
/// liveness check, `no client write completed after the last heal at 28.683 s`.
/// That is a real catch of a real wedge and the pair rule holds on it: the
/// correct server passes the same seed.
///
/// **It is not a seed that needs both bugs.** `SharedSnapshotDir` alone is
/// caught on seed 680 too, with the byte-identical message, which this test
/// asserts rather than glosses: the wedge here is the stream half's on its own.
/// Swept over seeds 0..1000 in release, the pair is caught on 1 of 1000 (this
/// seed), `SharedSnapshotDir` alone on 1 of 1000 (this seed), `IgnoreIncarnation`
/// alone on 0 of 1000, and the seeds where the pair is caught and *neither*
/// single is number 0 of 1000. So what PROPOSED D-045 has
/// demonstrated is that the run can now be made and what it does can now be
/// asked; it has not yet demonstrated a wedge that needs both bugs, and this
/// comment does not claim one.
///
/// Why that is hard is PROPOSED D-043's own account: the wedge needs a stream
/// that *never completes*, which needs a take to land on the very directory a
/// live stream has open — and once that coincidence happens the stream half
/// alone already stalls the commit past the bound, leaving D-042's stale
/// `matched` nothing to add. A seed that needs both would be one where the
/// stream recovers but the re-seeded follower is uncountable at the same moment.
/// The day one is found it belongs here beside this seed.
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
    let stream_only = raft::run(680, Variant::SharedSnapshotDir)
        .check()
        .expect_err(
            "SharedSnapshotDir alone no longer catches seed 680: the pin's story is out of date",
        );
    assert_eq!(
        paired, stream_only,
        "the pair and the stream half alone no longer fail seed 680 the same way: \
         the pair may now be buying a catch of its own, which is worth recording"
    );

    // And the other half alone reaches nothing, here as everywhere.
    assert_eq!(
        raft::run(680, Variant::IgnoreIncarnation).check().err(),
        None,
        "IgnoreIncarnation alone now catches seed 680: the pin's story is out of date"
    );

    // The pair rule (CLAUDE.md): the correct server passes the seed its buggy
    // siblings fail.
    raft::run(680, Variant::Correct).check().unwrap();
}

/// The thousand-seed premerge's seed 687: server 3's engine open dropped SST 1 —
/// it held sequence numbers 1..98 of the state machine — and the store was
/// refused with `LostState { dropped: [1] }`, traced at 7.9209 s as RAFT.md §3
/// and D-025 require. Seven milliseconds later the refused server's own engine
/// flushed the memtable the recovery had replayed: table 4, then manifest 5
/// listing tables 2, 3 and 4 with table 1 forgotten, `CURRENT` switched to it,
/// and log segment 2 deleted. The evidence of the loss was laundered away. No
/// leader existed for the next 5.4 s, so no re-seed came; the schedule crashed
/// server 3 at 13.32 s and restarted it at 13.53 s, the open found a
/// self-consistent store, removed `000001.sst` as an orphan and opened clean —
/// `RaftRecovered { applied: 185, last_index: 189 }`, no refusal and no install
/// — and a voter with a hole in its state machine rejoined and began pre-voting.
/// State machine safety reported the restatement whose log does not hold the
/// index 1 it claimed to have applied. A refusal is now recorded in the store's
/// marker before anything else and refuses every later open until an install
/// replaces the store, and the refused engine is quiesced (PROPOSED D-044). Said plainly: the fixed tree's schedule
/// for this seed diverges from the premerge's — D-044 appends a crash aimed at
/// refused servers to every schedule, which moves every seed's interleaving — so
/// this pin holds the seed green rather than replaying the failure; the shape
/// itself is carried by `RefusalNotDurable` below. Stays in the gate.
#[test]
fn seed_687_which_the_premerge_found_stays_green() {
    raft::run(687, Variant::Correct).check().unwrap();
}

/// The positive control: the correct server satisfies every property on every
/// seed, and the sweep reached the states that matter.
#[test]
fn the_correct_server_passes_every_seed() {
    let coverage = Mutex::new(Coverage::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::Correct);
        coverage.lock().unwrap().add(&report);
        report
            .check()
            .inspect_err(|_| write_trace(&format!("raft-{seed}"), &report.jsonl))
    });
    let coverage = coverage.into_inner().unwrap();
    eprintln!("Correct: {coverage:?}");
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete();
}

/// The negative controls: each known bug is caught on some seed, and the rate is
/// reported. Takes a set, so a pair of bugs is asked the same question as one
/// (PROPOSED(D-045)); the sweep's own list is all single variants.
fn is_caught(variants: impl Into<Variants>) {
    let variants = variants.into();
    let caught: Vec<String> = sweep(seeds(), |seed| raft::run(seed, variants).check().err())
        .into_iter()
        .flatten()
        .collect();
    eprintln!(
        "{variants:?}: caught on {} of {} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(!caught.is_empty(), "{variants:?} was never caught");
}

#[test]
fn a_server_without_pre_vote_is_caught() {
    is_caught(Variant::NoPreVote);
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

/// The adoption as built under D-038 (PROPOSED D-041): the old store's `CURRENT`
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
/// (PROPOSED D-044): what a gate run must still see is that the storm was drawn
/// and that it had adoptions to crash into, so a sweep that passes is known to
/// have injected the fault. The rate is printed at every tier, and the pair rule
/// holds because the correct server passes the same seeds above.
#[test]
fn a_server_whose_adoption_is_as_built_is_caught() {
    let outcomes: Vec<(Option<String>, bool, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::AdoptionAsBuilt);
        let stormed = report
            .schedule
            .faults
            .iter()
            .any(|f| matches!(f, Fault::CrashAdopting { .. }));
        let adoptions = report.count(|e| matches!(e, TraceEvent::RaftAdopted { .. }));
        (report.check().err(), stormed, adoptions)
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
/// that keeps working: the server as built before PROPOSED D-044. Its window is
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
    let outcomes: Vec<(Option<String>, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::RefusalNotDurable);
        (report.check().err(), crashes_while_refused(&report))
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _)| v.as_ref()).collect();
    let fired = outcomes.iter().filter(|(_, hits)| *hits > 0).count();
    eprintln!(
        "RefusalNotDurable: caught on {} of {} seeds, crashed a refused server on {fired} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", |v| v.as_str())
    );
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
    // `SharedSnapshotDir` is at the nightly's (PROPOSED D-043).
    if seeds() >= 100 {
        assert!(!caught.is_empty(), "RefusalNotDurable was never caught");
    }
}

/// The leader that ignores the store incarnation its followers answer with
/// (RAFT.md §3, PROPOSED(D-042)): a follower refused for lost state and re-seeded
/// from a snapshot comes back below the match index the leader recorded for it,
/// the match is monotone and the probe never reaches below it, so every answer
/// is discarded and the follower is never counted again while that leader leads.
///
/// The sweep does not catch it: 0 of 100 release seeds, by construction rather
/// than by chance, and the owner's ban on `#[ignore]`d variant tests means the
/// test says so out loud instead of being skipped. Only with the third server
/// unavailable at the same time does the wedge stall a commit, and after the
/// last heal every fault has healed or restarted, so a server is unavailable
/// then only by refusal — and a refused server beside a re-seeded one is exactly
/// the configuration `Report::majority_up` withholds the liveness bound from
/// (PROPOSED D-035's carve-out). Were the bound asked there, the leader as built
/// re-seeds the refused server too, when it was designated while it was down,
/// and commits with it inside the bound. Seeing the wedge needs a liveness ask
/// when a leader in force at the last heal has a commit majority among the
/// servers that are up, quarantined ones included, and a schedule that refuses a
/// second follower under that leader — seed 5909's shape — which the disk
/// model's rot draws on its own and no driver can aim.
///
/// What the test asserts is what is true and what the pair rule can still be
/// held to here: the variant is really the leader as built, which the trace says
/// exactly — this leader never forgets a follower's progress, so it traces no
/// `RaftProgressReset` on any seed, while the correct server's own sweep above
/// requires one wherever it saw a refusal — and the sweep reaches the state the
/// wedge is built on, a refused follower re-seeded and applying again. A sweep
/// that could not distinguish the two leaders at all would fail here. The catch
/// rate is printed at every tier so the day it stops being zero is visible.
#[test]
fn a_leader_that_ignores_incarnations_never_forgets_and_the_sweep_cannot_see_it() {
    let outcomes: Vec<(Option<String>, usize, bool)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::IgnoreIncarnation);
        let resets = report.count(|e| matches!(e, TraceEvent::RaftProgressReset { .. }));
        let reseeded = reseed_completed(&report);
        (report.check().err(), resets, reseeded)
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

/// The leader as built before PROPOSED D-043: one mutable checkpoint directory per
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
/// It moves the variant from uncatchable at the pre-merge tier to catchable
/// there, and no further: 0 of 100 release seeds, 1 of 1000 — seed 680, by the
/// liveness check, `no client write completed after the last heal at 28.683 s`,
/// which is seed 5909's shape reproduced by construction. D-043 recorded 0 of
/// 1000 before this arm existed, so a tier that never caught it now does, about
/// once.
///
/// Why only about once is worth writing down, because it is not a matter of
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
#[test]
fn a_leader_that_shares_one_snapshot_directory_and_streams_one_follower_at_a_time_is_caught() {
    let outcomes: Vec<(Option<String>, bool, usize)> = sweep(seeds(), |seed| {
        let report = raft::run(seed, Variant::SharedSnapshotDir);
        (
            report.check().err(),
            retook_at_one_index(&report),
            report.aimed_streams,
        )
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _, _)| v.as_ref()).collect();
    let fired = outcomes.iter().filter(|(_, fired, _)| *fired).count();
    let aimed = outcomes.iter().filter(|(_, _, aimed)| *aimed > 0).count();
    let liveness = caught.iter().filter(|v| v.contains("liveness")).count();
    eprintln!(
        "SharedSnapshotDir: caught on {} of {} seeds, {liveness} by the liveness check, re-took at an index already taken on {fired} seeds, the aimed re-take arm reached its stream on {aimed} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", |v| v.as_str())
    );
    assert!(
        fired > 0,
        "SharedSnapshotDir never re-took at an index already taken: the fault was not injected"
    );
    assert!(
        aimed > 0,
        "the aimed re-take arm never reached a stream: the shape it exists to build was never built"
    );
    if seeds() >= 10_000 {
        assert!(!caught.is_empty(), "SharedSnapshotDir was never caught");
    }
}

/// How many crashes landed on a server that was sitting refused for lost state:
/// what [`Fault::CrashRefused`] aims at, counted from the trace so a sweep that
/// passes is known to have injected the fault (PROPOSED D-044). A server is
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
/// stream reading it (PROPOSED D-043). A restart re-states the record's snapshot
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
    // PROPOSED(D-043): the re-take-under-a-stream arm, and how many of its arms
    // got as far as the stream they aim under.
    retake_stream_faults: usize,
    aimed_streams: usize,
    adoptions: usize,
    marker_refusals: usize,
    // PROPOSED(D-044): the crash-after-refusal fault, the crashes it landed on a
    // refused server, the engines quiesced, and the refusals the store's own
    // lost mark made.
    refusal_crash_faults: usize,
    refused_crashes: usize,
    quiesced_engines: usize,
    lost_mark_refusals: usize,
    bit_rot: usize,
    torn_writes: usize,
    puts: u64,
    gets: u64,
    deletes: u64,
    cas: u64,
    completed: u64,
    abandoned: u64,
    redirected: u64,
    slowest_write_after_heal: Duration,
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
        // PROPOSED(D-043): versions swept, takes answered by the recorded
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
        // PROPOSED(D-041): the crash-mid-adoption fault, the adoptions it and the
        // installs produce, and the refusals the store marker made where the
        // engine alone would have opened a fresh store.
        self.adoption_crash_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::CrashAdopting { .. }))
            .count();
        self.adoptions += report.count(|e| matches!(e, TraceEvent::RaftAdopted { .. }));
        // PROPOSED(D-043): the aimed arm, and the stream it got as far as.
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
        // PROPOSED(D-044): the crash-after-refusal arm, the crashes it landed,
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
            // PROPOSED(D-041): every install is adopted at the next start, so a
            // hundred seeds that install also adopt.
            assert!(
                self.adoptions > 0,
                "the sweep never saw a staged install adopted: {self:?}"
            );
            // A refused server answers from no store, and a leader that had
            // matched entries on the lost one forgets them (PROPOSED(D-042)):
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

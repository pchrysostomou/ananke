//! The Phase 2 sweep (SPEC.md §3): the correct server holds every invariant of
//! RAFT.md §2 on every seed under the full network fault model, partitions, one-way
//! blocks and crashes with the disk model, and each known-buggy variant this stage
//! ships (RAFT.md §5) is caught on some seed. The catch rate of each is printed, so a
//! hundred-seed run reports it.

use std::collections::BTreeSet;
use std::time::Duration;

use ananke_env::{ClientOp, DropReason, TraceEvent};
use ananke_raft::core::Variant;
use ananke_sim::raft::DRIFT_BOUND_PPM;
use ananke_sim::raft::{self, Fault};
use ananke_sim::{seeds, write_trace};

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

/// The positive control: the correct server satisfies every property on every
/// seed, and the sweep reached the states that matter.
#[test]
fn the_correct_server_passes_every_seed() {
    let mut coverage = Coverage::default();
    for seed in 0..seeds() {
        let report = raft::run(seed, Variant::Correct);
        coverage.add(&report);
        if let Err(violation) = report.check() {
            write_trace(&format!("raft-{seed}"), &report.jsonl);
            panic!("{violation}");
        }
    }
    eprintln!("Correct: {coverage:?}");
    coverage.assert_complete();
}

/// The negative controls: each known bug is caught on some seed, and the rate is
/// reported.
fn is_caught(variant: Variant) {
    let mut caught = Vec::new();
    for seed in 0..seeds() {
        if let Err(violation) = raft::run(seed, variant).check() {
            caught.push(violation);
        }
    }
    eprintln!(
        "{variant:?}: caught on {} of {} seeds, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(!caught.is_empty(), "{variant:?} was never caught");
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

/// Lease safety under drift (RAFT.md §2, invariant 6): on every seed where the
/// simulated drift exceeds the bound, either the guard revoked the drifting
/// follower's trust or the checker reports the stale read and the run fails. The
/// correct server passes every seed (above); here each exceeded seed is run with
/// the guard and without it, and the report says how many revoked, how many read
/// stale without the guard, and how many did neither.
#[test]
fn a_leader_that_trusts_the_clock_is_caught_and_the_guard_revokes() {
    let mut exceeded = 0;
    let mut revoked = 0;
    let mut stale = 0;
    let mut neither = 0;
    let mut lease_reads_within = 0;
    let mut slowest_led = 0;
    let mut first_stale = String::new();
    for seed in 0..seeds() {
        let correct = raft::run(seed, Variant::Correct);
        slowest_led += correct.trials_led_by_slowest;
        if !correct.drift_exceeded() {
            lease_reads_within += correct.lease_reads();
            continue;
        }
        exceeded += 1;
        let guard_revoked = correct.lease_revokes() > 0;
        revoked += usize::from(guard_revoked);
        let buggy = raft::run(seed, Variant::LeaseTrustsTheClock);
        let read_stale = match buggy.check() {
            Err(violation) if violation.contains("linearizability") => {
                if first_stale.is_empty() {
                    first_stale = violation;
                }
                true
            }
            _ => false,
        };
        stale += usize::from(read_stale);
        neither += usize::from(!guard_revoked && !read_stale);
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
    compactions: usize,
    reseeded: usize,
    reseed_completions: u64,
    install_crash_faults: usize,
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
        self.compactions += report.count(|e| matches!(e, TraceEvent::RaftCompacted { .. }));
        self.reseeded += report.count(|e| matches!(e, TraceEvent::RaftReseeded { .. }));
        self.reseed_completions += u64::from(reseed_completed(report));
        self.install_crash_faults += report
            .schedule
            .faults
            .iter()
            .filter(|f| matches!(f, Fault::CrashInstalling { .. }))
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
    let mut coverage = MembershipCoverage::default();
    for seed in 0..seeds() {
        let report = membership::run(seed, Variant::Correct);
        coverage.add(&report);
        if let Err(violation) = report.check() {
            write_trace(&format!("membership-{seed}"), &report.jsonl);
            panic!("{violation}");
        }
    }
    eprintln!("Membership: {coverage:?}");
    coverage.assert_complete(seeds());
}

/// The negative control: a server that counts one merged majority while joint
/// (thesis §4.3) is caught by the membership scenario's checks on some seed.
#[test]
fn a_server_that_counts_one_majority_in_joint_consensus_is_caught() {
    let mut caught = Vec::new();
    for seed in 0..seeds() {
        if let Err(violation) =
            membership::run(seed, Variant::SingleMajorityInJointConsensus).check()
        {
            caught.push(violation);
        }
    }
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

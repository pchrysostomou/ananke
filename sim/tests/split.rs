//! `sim/split.rs` under the sweep (SHARD.md §10, §12): directed shapes about a split, on
//! four nodes, nodes 1 to 3 the bootstrap nodes and node 4 a node that joins and holds
//! nothing. Shapes (i), *overlap*, and (ii), *vote*, are built here; (iii) is Stage D's.
//!
//! Shape (i), at every tier (PROPOSED D-110): the correct system passes every seed, and on
//! every seed the shape is built, its premise holds, and while the limit held a chunk of
//! R's stream reached `x` while a chunk of P's was dropped as oversized; `x` refused R's
//! install for overlap, R's leader waited a minimum election timeout each time before it
//! streamed again, from the same checkpoint, and `x` made R once its P had moved off R's
//! span. Under `SnapshotOverlapsReplica` the shape is built too, and check 8's overlap
//! clause trips on every seed at `x`'s `RangeCreated { cause: snapshot }` of R.
//!
//! What it asserts, at every tier (SHARD.md §12, Stage C's exit): the correct system
//! passes every seed — checks 1 to 4, the range layer's folds and no node stopped — and
//! on every seed the shape is built, its premise holds — `x`'s P got past the split by
//! P's snapshot alone, and `x` made no right half but by a snapshot — and a pre-vote or
//! vote request of the right half reached the cut-off follower `x` before `x`'s creation
//! of it. Under
//! `UninitialisedReplicaVotes` the shape is built too, and `x`'s placeholder grants the
//! right half's remaining replica its vote on every seed, and check 20 catches it on every
//! seed.
// PROPOSED(D-109): `sim/split.rs` (ii).
// PROPOSED(D-110): `sim/split.rs` (i).

use ananke_env::sim::{Sim, SimConfig};
use ananke_env::{Environment, TraceEvent};
use ananke_raft::ServerId;
use ananke_shard::invariants::{self, Fold};
use ananke_shard::range_variant::{RangeVariant, RangeVariants};
use ananke_sim::{seeds, split, sweep, verdict, write_trace};

/// The right half's votes and pre-votes `x` granted before `x` created the right half:
/// none on the correct system, which keeps a placeholder there and grants nothing.
fn grants_by_x_before_its_creation(report: &split::Report) -> usize {
    let Some((_, right)) = report.split else {
        return 0;
    };
    let created = report.x_created_right_at();
    report
        .records
        .iter()
        .enumerate()
        .filter(|(at, record)| {
            created.is_none_or(|created| *at < created)
                && record.node.map(|node| u64::from(node.get())) == Some(report.x)
                && matches!(
                    &record.event,
                    TraceEvent::RaftVote { range, granted: true, .. } if *range == right
                )
        })
        .count()
}

/// How long after the heal `x` created the right half, in milliseconds, if it did.
fn created_after_heal(report: &split::Report) -> Option<u64> {
    let created = report.x_created_right_at()?;
    let healed = report.healed?;
    Some(
        report.records[created]
            .at
            .duration_since(healed)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

/// Shape (ii) on the correct system, every seed of the tier: the run passes its checks,
/// the shape is built and a vote request of the right half reached `x` before `x`
/// created it, and `x` granted none of them. `x`'s right half comes once the crashed
/// node is back and the right half elects without `x`, from that leader's stream to `x`'s
/// placeholder: measured before it was asserted, on the first seeds, 2.1 to 2.6 s after
/// the heal, past the ten maximum election timeouts the crashed node is down for.
#[test]
fn every_seed_of_the_vote_shape_passes_on_the_correct_node() {
    let seeds = seeds();
    let outcomes = sweep(seeds, |seed| {
        let report = split::run(seed, RangeVariants::correct());
        let outcome = report.check().and_then(|()| report.shape()).and_then(|()| {
            match grants_by_x_before_its_creation(&report) {
                0 => Ok(()),
                granted => Err(format!(
                    "seed {seed}: node {} granted the right half {granted} votes or pre-votes \
                     before it created the right half, where a placeholder grants nothing \
                     (SHARD.md §5, Q22)",
                    report.x
                )),
            }
        });
        if outcome.is_err() {
            write_trace(&format!("split-vote-{seed}"), &report.jsonl());
        }
        (outcome, created_after_heal(&report))
    });
    let created: Vec<u64> = outcomes.iter().filter_map(|(_, at)| *at).collect();
    println!(
        "split (ii): {seeds} seeds, x created the right half on {} of them, {} to {} ms after \
         the heal",
        created.len(),
        created.iter().min().copied().unwrap_or(0),
        created.iter().max().copied().unwrap_or(0)
    );
    let verdicts: Vec<Result<(), String>> =
        outcomes.into_iter().map(|(outcome, _)| outcome).collect();
    verdict(&verdicts).expect("the correct node passes every seed of shape (ii)");
}

/// Shape (ii) under `UninitialisedReplicaVotes`, every seed of the tier: the shape is
/// built, and `x`'s placeholder grants the right half's remaining replica a pre-vote or a
/// vote before `x` holds the right half — the injection, on every seed, which a sweep that
/// passed could otherwise not show it had made. With the placeholder's vote the right
/// half elects without the crashed node, so `x` gets its right half well inside the ten
/// maximum election timeouts the correct system waits. Check 20 trips on every seed, a
/// placeholder's grant (PROPOSED D-109 §3), and the correct system's test beside it holds
/// check 20 green on every seed through the run's verdict.
#[test]
fn a_placeholder_that_grants_votes_grants_one_on_every_seed_of_the_vote_shape() {
    let seeds = seeds();
    let outcomes = sweep(seeds, |seed| {
        let report = split::run(
            seed,
            RangeVariants::of(&[RangeVariant::UninitialisedReplicaVotes]),
        );
        let granted = grants_by_x_before_its_creation(&report);
        let outcome = report.shape().and_then(|()| {
            if granted > 0 {
                Ok(())
            } else {
                Err(format!(
                    "seed {seed}: under UninitialisedReplicaVotes node {}'s placeholder granted \
                     the right half nothing before it created it, so the variant was not \
                     injected on this seed",
                    report.x
                ))
            }
        });
        // And check 20 catches it, on every seed: the fold's own verdict, a placeholder's
        // grant (PROPOSED D-109 §3).
        let outcome = outcome.and_then(|()| {
            match invariants::folded(&report.records).verdict_of(Fold::PlaceholderGrants) {
                Err(words) if words.contains("from its placeholder") => Ok(()),
                Err(words) => Err(format!(
                    "seed {seed}: check 20 tripped on a grant that was not a placeholder's: \
                     {words}"
                )),
                Ok(()) => Err(format!(
                    "seed {seed}: under UninitialisedReplicaVotes node {}'s placeholder granted \
                     {granted} votes or pre-votes and check 20 did not trip",
                    report.x
                )),
            }
        });
        (outcome, granted)
    });
    let granted: usize = outcomes.iter().map(|(_, granted)| granted).sum();
    println!(
        "split (ii) under UninitialisedReplicaVotes: {seeds} seeds, {granted} grants by x's \
         placeholder in all"
    );
    let verdicts: Vec<Result<(), String>> =
        outcomes.into_iter().map(|(outcome, _)| outcome).collect();
    verdict(&verdicts).expect("the variant is injected on every seed of shape (ii)");
}

/// Exactly three bootstrap nodes (SHARD.md §2; Q7, Q32): a node configured with any
/// other number stops at the top of its start, before it opens its store or binds its
/// socket, and says why. Five, as `sim/install.rs` configured before the placeholders,
/// and two, and three with one named twice. A bind leaves no trace of its own, so what
/// is asserted is what one would lead to: the node sends nothing, opens no WAL and
/// spawns no task of its own, and creates or restates no replica. The same harness with
/// `[1, 2, 3]` is the control: that node starts, creates its replicas and sends, so the
/// harness tells a node that stopped from one that did not.
// PROPOSED(D-109): exactly three bootstrap nodes, enforced.
#[test]
fn a_node_configured_with_other_than_three_bootstrap_nodes_stops_and_says_why() {
    // What the harness saw of a node configured with `bootstrap`: its failures, and
    // whether it sent a message, opened a WAL, spawned a task beside the harness's own
    // and created or restated a replica.
    let started = |bootstrap: &[u64]| {
        let mut sim = Sim::new(SimConfig::new(1));
        let node = sim.add_node();
        let mut config = split::server_config(1, RangeVariants::correct());
        config.bootstrap = bootstrap.iter().copied().map(ServerId).collect();
        let env = sim.env(node);
        let inner = env.clone();
        env.spawn("node", async move {
            let _ = ananke_shard::server::run(inner, config).await;
        });
        sim.run_for(std::time::Duration::from_millis(500));
        let trace = sim.trace();
        let failed: Vec<String> = trace
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftServerFailed { reason, .. } => Some(reason.clone()),
                _ => None,
            })
            .collect();
        let any = |what: &dyn Fn(&TraceEvent) -> bool| trace.iter().any(|r| what(&r.event));
        let sent = any(&|event| matches!(event, TraceEvent::MessageSent { .. }));
        let opened = any(&|event| matches!(event, TraceEvent::WalSegmentOpened { .. }));
        let spawned =
            any(&|event| matches!(event, TraceEvent::TaskSpawned { name, .. } if *name != "node"));
        let replicas = any(&|event| {
            matches!(
                event,
                TraceEvent::RangeCreated { .. } | TraceEvent::RangesRestated { .. }
            )
        });
        (failed, sent, opened, spawned, replicas)
    };
    for bootstrap in [vec![1, 2, 3, 4, 5], vec![1, 2], vec![1, 2, 2]] {
        let (failed, sent, opened, spawned, replicas) = started(&bootstrap);
        assert_eq!(
            failed.len(),
            1,
            "a node configured with the bootstrap nodes {bootstrap:?} did not stop once: \
             {failed:?}"
        );
        assert!(
            failed[0].contains("exactly 3"),
            "the node's failure does not say why: {}",
            failed[0]
        );
        assert!(
            !sent && !opened && !spawned && !replicas,
            "a node configured with the bootstrap nodes {bootstrap:?} got past the top of its \
             start before it stopped: sent {sent}, opened a WAL {opened}, spawned a task \
             {spawned}, created or restated a replica {replicas}"
        );
    }
    // The control: three bootstrap nodes, and the same harness sees the node start.
    let (failed, sent, opened, spawned, replicas) = started(&[1, 2, 3]);
    assert!(
        failed.is_empty() && sent && opened && spawned && replicas,
        "a node configured with the bootstrap nodes [1, 2, 3] did not start as the harness \
         expects: failures {failed:?}, sent {sent}, opened a WAL {opened}, spawned a task \
         {spawned}, created or restated a replica {replicas}"
    );
}

/// The span of `values`, as "min to max".
fn span_of(values: &[usize]) -> String {
    format!(
        "{} to {}",
        values.iter().min().copied().unwrap_or(0),
        values.iter().max().copied().unwrap_or(0)
    )
}

/// What one correct run of shape (i) measured: R's and P's largest chunk frames toward
/// `x`, R's and P's chunks across the limit, `x`'s refusals for overlap, R's leader's
/// waits, and how long after the lift `x` made R.
#[derive(Default)]
struct OverlapMeasured {
    right_frame: usize,
    parent_frame: usize,
    delivered: usize,
    dropped: usize,
    refusals: usize,
    waits: usize,
    made_after_lift: u64,
}

/// Shape (i) on the correct system, every seed of the tier (SHARD.md §12, Stage C's exit;
/// PROPOSED D-110): the run passes its checks and the shape is built, its premise held and
/// across the limit a chunk of R's stream reached `x` while a chunk of P's was dropped as
/// oversized. And the overlap rule's mechanism, on every seed: `x` refused R's install for
/// overlap while its P still covered R's span; each refusal ended R's leader's stream, and
/// the leader streamed again only after a minimum election timeout, from the same
/// checkpoint, R having split nothing since it took it (`RaftSnapshotRestream { retake:
/// false }`); and once `x`'s P was installed past the split, `x` made R by a snapshot.
///
/// The frame-length limit is measured here, every tier printing R's largest chunk frame
/// toward `x` and P's, and asserted between them on every seed: a limit at or below R's
/// largest frame would drop R's stream too, and one at or above P's would let P's through.
#[test]
fn every_seed_of_the_overlap_shape_passes_on_the_correct_node() {
    let seeds = seeds();
    let outcomes = sweep(seeds, |seed| {
        let report = split::run_overlap(seed, RangeVariants::correct());
        let mut measured = OverlapMeasured::default();
        let outcome = report
            .check()
            .and_then(|()| report.shape())
            .and_then(|()| overlap_mechanism(&report, &mut measured));
        if outcome.is_err() {
            write_trace(&format!("split-overlap-{seed}"), &report.jsonl());
        }
        (outcome, measured)
    });
    let pick = |what: fn(&OverlapMeasured) -> usize| -> Vec<usize> {
        outcomes
            .iter()
            .map(|(_, measured)| what(measured))
            .collect()
    };
    println!(
        "split (i): {seeds} seeds under a limit of {} bytes toward x: R's largest chunk frame \
         {} bytes, P's {} bytes; across the limit R's chunks delivered {} and P's dropped as \
         oversized {}; x's refusals for overlap {}; R's leader's waits {}; x made R {} ms \
         after the lift",
        split::FRAME_LIMIT,
        span_of(&pick(|m| m.right_frame)),
        span_of(&pick(|m| m.parent_frame)),
        span_of(&pick(|m| m.delivered)),
        span_of(&pick(|m| m.dropped)),
        span_of(&pick(|m| m.refusals)),
        span_of(&pick(|m| m.waits)),
        span_of(&pick(
            |m| usize::try_from(m.made_after_lift).unwrap_or(usize::MAX)
        )),
    );
    let verdicts: Vec<Result<(), String>> =
        outcomes.into_iter().map(|(outcome, _)| outcome).collect();
    verdict(&verdicts).expect("the correct node passes every seed of shape (i)");
}

/// The overlap rule's mechanism on one correct run of shape (i), measured into `measured`.
fn overlap_mechanism(report: &split::Report, measured: &mut OverlapMeasured) -> Result<(), String> {
    let (seed, x) = (report.seed, report.x);
    let (_, right) = report.split.ok_or("no split")?;
    measured.right_frame = report.largest_chunk_frame_to_x(right).unwrap_or(0);
    measured.parent_frame = report.largest_chunk_frame_to_x(split::PARENT).unwrap_or(0);
    if !(measured.right_frame < split::FRAME_LIMIT && split::FRAME_LIMIT < measured.parent_frame) {
        return Err(format!(
            "seed {seed}: the limit of {} bytes does not lie between R's largest chunk frame \
             toward node {x}, {} bytes, and P's, {} bytes",
            split::FRAME_LIMIT,
            measured.right_frame,
            measured.parent_frame
        ));
    }
    (measured.delivered, measured.dropped) = report.limited_chunks();
    let refusals = report.overlap_refusals();
    measured.refusals = refusals.len();
    let installed = report
        .x_parent_installed_at()
        .ok_or_else(|| format!("seed {seed}: node {x}'s P was never installed past the split"))?;
    let Some(&first) = refusals.first() else {
        return Err(format!(
            "seed {seed}: node {x} never refused R's install for overlap, so its P never \
             covered R's span when R's stream completed"
        ));
    };
    if first > installed {
        return Err(format!(
            "seed {seed}: node {x}'s first refusal of R for overlap came after its P was \
             installed past the split, when its P no longer covered R's span"
        ));
    }
    let waits = report.waits()?;
    measured.waits = waits.len();
    if waits.is_empty() {
        return Err(format!(
            "seed {seed}: R's leader never ended a wait after an overlap refusal by node {x}"
        ));
    }
    if waits.iter().any(|(_, _, retake)| *retake) {
        return Err(format!(
            "seed {seed}: R's leader retook its checkpoint after an overlap refusal, though R \
             split nothing after it took it"
        ));
    }
    let made = report.x_created_right_at().ok_or_else(|| {
        format!("seed {seed}: node {x} never made R, though its P moved off R's span")
    })?;
    if made < installed {
        return Err(format!(
            "seed {seed}: node {x} made R before its P was installed past the split"
        ));
    }
    let lifted = report.lifted.ok_or("never lifted")?;
    measured.made_after_lift = report.records[made]
        .at
        .duration_since(lifted)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    Ok(())
}

/// Shape (i) under `SnapshotOverlapsReplica`, every seed of the tier (SHARD.md §10, §12;
/// PROPOSED D-110): the shape is built, `x` installs R's snapshot while its P still covers
/// R's span, and check 8's overlap clause trips on every seed at that install's
/// `RangeCreated { cause: snapshot }` — the record at which the fold, fed one record at a
/// time, first reads a violation is that creation, on `x`, of R. The correct system's test
/// beside it holds check 8 green on every seed through the run's verdict.
#[test]
fn a_node_that_installs_every_snapshot_is_caught_by_check_8_on_every_seed_of_the_overlap_shape() {
    let seeds = seeds();
    let outcomes = sweep(seeds, |seed| {
        let report = split::run_overlap(
            seed,
            RangeVariants::of(&[RangeVariant::SnapshotOverlapsReplica]),
        );
        let outcome = report.shape().and_then(|()| {
            let (_, right) = report.split.ok_or("no split")?;
            let at = report.overlap_tripped_at().ok_or_else(|| {
                format!(
                    "seed {seed}: under SnapshotOverlapsReplica check 8's overlap clause did \
                     not trip"
                )
            })?;
            let record = &report.records[at];
            let on_x = record.node.map(|node| u64::from(node.get())) == Some(report.x);
            if !(on_x
                && matches!(&record.event, TraceEvent::RangeCreated {
                    range, cause: ananke_env::RangeCause::Snapshot, ..
                } if *range == right))
            {
                return Err(format!(
                    "seed {seed}: check 8's overlap clause tripped at {:?}, not at node {}'s \
                     install of R",
                    record.event, report.x
                ));
            }
            match invariants::folded(&report.records).verdict_of(Fold::Overlap) {
                Err(words) if words.starts_with(Fold::Overlap.words()) => Ok(()),
                other => Err(format!(
                    "seed {seed}: check 8's overlap clause over the run said {other:?}"
                )),
            }
        });
        if outcome.is_err() {
            write_trace(&format!("split-overlap-variant-{seed}"), &report.jsonl());
        }
        outcome
    });
    println!(
        "split (i) under SnapshotOverlapsReplica: {seeds} seeds, check 8 caught {} of them at \
         the install's RangeCreated",
        outcomes.iter().filter(|outcome| outcome.is_ok()).count()
    );
    verdict(&outcomes).expect("check 8 catches the variant on every seed of shape (i)");
}

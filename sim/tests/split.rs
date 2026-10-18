//! `sim/split.rs` under the sweep (SHARD.md §10, §12): directed shapes about a split, on
//! four nodes, nodes 1 to 3 the bootstrap nodes and node 4 a node that joins and holds
//! nothing. Shape (ii), *vote*, is built here; (i) is the overlap rule's slice's and (iii)
//! Stage D's.
//!
//! What it asserts, at every tier (SHARD.md §12, Stage C's exit): the correct system
//! passes every seed — checks 1 to 4, the range layer's folds and no node stopped — and
//! on every seed the shape is built and a pre-vote or vote request of the right half
//! reached the cut-off follower `x` before `x`'s creation of it. Under
//! `UninitialisedReplicaVotes` the shape is built too, and `x`'s placeholder grants the
//! right half's remaining replica its vote on every seed, which is what check 20 sees.
// PROPOSED(D-109): `sim/split.rs` (ii).

use ananke_env::sim::{Sim, SimConfig};
use ananke_env::{Environment, TraceEvent};
use ananke_raft::ServerId;
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
/// maximum election timeouts the correct system waits.
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
/// other number stops before it binds anything, and says why. Five, as `sim/install.rs`
/// configured before the placeholders, and two, and three with one named twice.
// PROPOSED(D-109): exactly three bootstrap nodes, enforced.
#[test]
fn a_node_configured_with_other_than_three_bootstrap_nodes_stops_and_says_why() {
    for bootstrap in [vec![1, 2, 3, 4, 5], vec![1, 2], vec![1, 2, 2]] {
        let mut sim = Sim::new(SimConfig::new(1));
        let node = sim.add_node();
        let mut config = split::server_config(1, RangeVariants::correct());
        config.bootstrap = bootstrap.iter().copied().map(ServerId).collect();
        let env = sim.env(node);
        let inner = env.clone();
        env.spawn("node", async move {
            let _ = ananke_shard::server::run(inner, config).await;
        });
        sim.run_for(std::time::Duration::from_millis(50));
        let failed: Vec<String> = sim
            .trace()
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftServerFailed { reason, .. } => Some(reason.clone()),
                _ => None,
            })
            .collect();
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
            !sim.trace().iter().any(|record| matches!(
                record.event,
                TraceEvent::RangeCreated { .. } | TraceEvent::RangesRestated { .. }
            )),
            "a node configured with the bootstrap nodes {bootstrap:?} created or restated a \
             replica before it stopped"
        );
    }
}

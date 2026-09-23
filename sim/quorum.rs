//! The check-quorum re-seed scenario (RAFT.md §1 and §3, D-049): three servers, one
//! of them refused for lost state and being re-seeded by the leader, the leader's
//! other follower cut off, and the question check quorum must answer — do the
//! refused follower's rejections keep the leader in office? — asked twice on every
//! seed:
//!
//! - [`Half::Blocked`]: the leader's re-seed stream to the refused follower is
//!   blocked, by a path-MTU black hole that loses every chunk and lets the
//!   heartbeats and their rejections through (`Sim::limit_frames`). No chunk is
//!   acknowledged, so the rejections count for nothing and the leader must step down
//!   within two check-quorum windows of the cut, naming the refused follower as the
//!   answer it did not count. `Variant::RefusedCountsForQuorum`, the leader as
//!   built, keeps its office on the rejections for as long as the stream stays
//!   blocked.
//! - [`Half::Open`]: the stream runs. Its acknowledgements keep the rejections
//!   counting, so the leader keeps its office until the install completes, and then
//!   commits through the re-seeded follower. `Variant::RefusedNeverCounts`, the
//!   alternative D-049 rejected, steps down mid-re-seed, and with a re-seeded server
//!   that never votes (D-035) and the third server away nothing commits.
//!
//! The refusal is aimed rather than waited for: the victim is crashed and its
//! restart first records in its store directory that the store lost state, which is
//! the mark every refusal leaves (D-044), so its open is refused on the mark and the
//! refusal lands on every seed instead of on the few where the disk's rot hits a
//! table in use. What the scenario leaves out is said with the reason: no message is
//! dropped at random and no bit rots, since a stream that stalls on loss for a window
//! is a step-down the rule asks for and the open half is about a stream that runs;
//! every seed is scheduled uniformly, since the halves are claims about time and
//! D-016 asks time only of uniform schedules; and the disk takes no time
//! ([`Disk::Instant`]). A refused server answers nothing at all while it verifies and
//! repairs its staged stream and while it adopts the install, and on the sweep's
//! disk that silence outlasts a check-quorum window: there the leader steps down in
//! it under every counting rule, the leader as built's included, which is the
//! re-seed's own cost and not the rule's ([`Disk::Sweep`] measures it).
//! Duplicates, delays and reordering stay.
//!
//! [`node_run`] is the same scenario **sharded**, on the node of SHARD.md §4 (PROPOSED
//! D-085): one `mark_store_lost` refuses a node and every one of the four replicas it
//! holds (D-077), and check quorum is then asked separately of each range's own leader
//! about that one node. It is not D-049's two halves. Those are about a *store-less*
//! refused server, whose rejections are stamped incarnation 0, and about the re-seed
//! stream whose acknowledgements make them count; the node produces neither, and
//! [`NodeReport::check`] asserts both absences per seed, with the reason and with what
//! would upgrade them.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{RunHeader, Sim, SimConfig, TraceRecord};
use ananke_env::{DropReason, Environment, Instant, NodeId, RangeCause, TraceEvent};
use ananke_raft::core::Variants;
use ananke_raft::message::{self, Frame, Message, SnapshotStatus};
use ananke_raft::store::mark_store_lost;
use ananke_raft::{invariants, run as run_server};
use ananke_shard::variant::NodeVariants;
use moirae_sched::Policy;

use crate::lin::{self, History};
use crate::raft::{
    self, CLIENTS, ClientStats, Cluster, DIR, ELECTION_MIN, SERVERS, TICK, node_config,
    node_server_config, server_of,
};

/// The frames the blocked link direction still carries, in bytes: a heartbeat and a
/// rejection are a few dozen, a chunk of the sweep's snapshot four kilobytes and
/// more.
pub const MTU: usize = 1024;

/// How long the leader's other follower stays cut off, and the stream blocked in the
/// blocked half: fifteen check-quorum windows, several times what a re-seed of the
/// scenario's state machine takes.
pub const HOLD: Duration = Duration::from_millis(1500);

/// How long the scenario waits for the leader to open its stream to the refused
/// follower before it gives the seed up as a setup failure.
pub const STREAM_WAIT: Duration = Duration::from_millis(3000);

/// How many filling puts the leader takes before the refusal, each of
/// `raft::SPREAD_VALUE_BYTES`: enough that the checkpoint streams in some thirty
/// chunks rather than one, so the stream has acknowledgements to count.
pub const FILL: u64 = 300;

/// The filling driver's number, which picks its address.
const FILL_DRIVER: u64 = 1;

/// The disk the scenario's servers run on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disk {
    /// Every filesystem operation completes at the instant it is issued: what the
    /// halves are asked on. A refused server answers nothing while it verifies and
    /// repairs the stream it has staged, nor while it adopts the install and opens
    /// the store it built, and no check-quorum rule counts a follower that answers
    /// nothing; on this disk those take no time.
    Instant,
    /// The sweep's disk, every operation taking a tenth of a millisecond to two:
    /// the verification, the repair and the adoption then take over a check-quorum
    /// window, and a leader whose majority needs the re-seeded follower steps down
    /// in that silence under every counting rule.
    Sweep,
}

/// Which half of the scenario a run is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Half {
    /// The re-seed stream to the refused follower is blocked for the hold.
    Blocked,
    /// The re-seed stream runs.
    Open,
}

/// What one run of the scenario produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// Which server ran.
    pub variants: Variants,
    /// Which half.
    pub half: Half,
    /// Which disk.
    pub disk: Disk,
    /// The leader that opened the stream to the refused follower, the refused
    /// follower and the leader's other follower, once the setup got that far.
    pub cast: Option<Cast>,
    /// When the other follower was cut off, and in the blocked half the stream
    /// blocked; and when the hold ended.
    pub cut: Option<(Instant, Instant)>,
    /// Each server's clock rate error, in parts per million.
    pub drifts: Vec<i64>,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// What the moirae export needs besides [`Report::records`]:
    /// [`Report::jsonl`] writes the trace from the two when it is asked for.
    // PROPOSED(D-052): a scenario's moirae JSONL is written when it is asked for.
    pub run: RunHeader,
    /// The clients' history.
    pub history: History,
}

/// Who plays which part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cast {
    /// The leader that streams to the refused follower.
    pub leader: u64,
    /// The follower refused for lost state.
    pub refused: u64,
    /// The leader's other follower, cut off for the hold.
    pub other: u64,
}

/// What happened in the hold, read from the trace.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Hold {
    /// The leader's term at the cut.
    pub term: u64,
    /// The leader's commit index at the cut.
    pub commit_at_cut: u64,
    /// The leader's check-quorum step-down in the hold, by decision time, with the
    /// followers it did not count.
    pub quorum_lost: Option<(Instant, Vec<u64>)>,
    /// The refused follower's start on the store its re-seed built, in the hold.
    pub reseeded: Option<Instant>,
    /// The leader's first commit of its term past its commit index at the cut,
    /// decided after the re-seeded start, in the hold.
    pub commit_after_install: Option<Instant>,
    /// Chunk acknowledgements from the refused follower delivered to the leader in
    /// the hold, the install's answer included.
    pub chunk_acks: usize,
    /// Rejections stamped incarnation 0 from the refused follower delivered to the
    /// leader in the hold.
    pub refused_rejections: usize,
    /// Frames from the leader to the refused follower lost to the frame-length limit.
    pub oversized: usize,
}

fn node(id: u64) -> NodeId {
    NodeId::new(u32::try_from(id).expect("small"))
}

/// A server's start on `cluster`: with `lost`, it first records in its store
/// directory that the store lost state, the mark a refusal leaves (D-044), so the
/// open that follows is refused.
///
/// The cluster is [`Cluster::OneGroup`] for every run of this scenario
/// ([`run_on`]), which is what keeps its draws and its pinned figures exactly
/// Phase 2's; [`Cluster::Node`] is reached only by [`node_run`], the sharded
/// scenario, which drives the same refusal at the node.
// PROPOSED(D-085): the refusal is driven at either cluster; only the probe uses the node.
fn spawn(
    sim: &Sim,
    cluster: Cluster,
    id: u64,
    variants: Variants,
    node_variants: NodeVariants,
    lost: bool,
) {
    let env = sim.env(node(id));
    let inner = env.clone();
    // The one-group task keeps the name it always had, so its runs are unmoved.
    let task = match cluster {
        Cluster::OneGroup => "raft",
        Cluster::Node => "node",
    };
    env.spawn(task, async move {
        if lost
            && mark_store_lost(&inner, Path::new(DIR), "the scenario lost this store")
                .await
                .is_err()
        {
            return;
        }
        match cluster {
            Cluster::OneGroup => {
                let _ = run_server(inner, node_config(id, variants)).await;
            }
            Cluster::Node => {
                let _ = ananke_shard::server::run(
                    inner,
                    node_server_config(id, variants, node_variants),
                )
                .await;
            }
        }
    });
}

/// Runs the simulation in slices of five milliseconds until a new record satisfies
/// `found` or `budget` passes; returns that record.
fn run_until_record(
    sim: &mut Sim,
    budget: Duration,
    found: impl Fn(&TraceRecord) -> bool,
) -> Option<TraceRecord> {
    let step = Duration::from_millis(5);
    let mut scanned = sim.trace_len();
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        let records = sim.trace_from(scanned);
        scanned += records.len();
        if let Some(record) = records.into_iter().find(|r| found(r)) {
            return Some(record);
        }
    }
    None
}

/// How long the node's four ranges elect before the victim is refused.
const NODE_WARMUP: Duration = Duration::from_millis(1200);

/// How many puts of [`raft::SPREAD_VALUE_BYTES`] each the scenario drives into every
/// range the keeper still leads once the cut is made, so that each of them has an
/// entry of the leader's term to commit past its commit index at the cut. Four is
/// enough for the claim and small enough that the hold is not a load test.
const HOLD_PUTS: u64 = 4;

/// How long the scenario gives the whole-node re-seed to land and the ranges the
/// victim led to find leaders again, before it takes the cast and cuts the third node
/// off. Every range must be led by a node that is not the victim when the cast is
/// taken, or the seed is a setup failure and says so.
const SETTLE: Duration = Duration::from_millis(1500);

/// The parts of the sharded scenario, which are per node and then per range.
///
/// On one group "the refused server", "the refused replica" and "the leader" are one
/// sentence each. On the node they are three: one `mark_store_lost` refuses the node
/// and every replica it holds (D-077), and check quorum is then asked separately of
/// each range's own leader about that same node.
// PROPOSED(D-085): sharded is one fault and four answers, not the scenario four times.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeCast {
    /// The node refused for lost state, whose four replicas are re-seeded with it.
    pub victim: u64,
    /// The node that keeps its side of the cut: it leads at least two of the four
    /// ranges, and for each of them its majority is itself and the refused node.
    pub keeper: u64,
    /// The node cut off for the hold. It leads the ranges the keeper does not.
    pub other: u64,
    /// Each range's leader when the cut is made.
    pub leaders: BTreeMap<u64, u64>,
}

/// What one range of the node saw in the hold, read from the trace.
///
/// [`Hold`] is the one-group scenario's and is about one leader and one refused
/// follower; this is the same reading asked per range of four leaders and four
/// refused replicas of one node.
// PROPOSED(D-085): sharded is one fault and four answers, not the scenario four times.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeHold {
    /// The range.
    pub range: u64,
    /// Its leader at the cut.
    pub leader: u64,
    /// `RaftReplicaRefused` of the victim for this range: the fan-out of the one
    /// `mark_store_lost` (D-077), which a single-range world cannot tell from the
    /// refusal of the node itself.
    pub replica_refused: bool,
    /// `RaftReseeded` of the victim for this range.
    pub reseeded: bool,
    /// The store incarnation the victim's re-seeded replica of this range restated
    /// at its first start (`RaftRecovered`). A re-seeded replica draws one of its own
    /// from the **node's** generator (D-077, Q26), so the four ranges' are four
    /// different numbers — which is a question only a node of several ranges has.
    pub incarnation: Option<u64>,
    /// The store incarnations that replica's answers of this range actually carried
    /// to this range's leader in the hold: the leader's own evidence of it, which
    /// only a range whose leader the cut left reachable has.
    pub answered_incarnations: BTreeSet<u64>,
    /// The leader's term at the cut.
    pub term: u64,
    /// The leader's commit index at the cut.
    pub commit_at_cut: u64,
    /// The leader's check-quorum step-down in the hold, by decision time, with the
    /// followers it did not count.
    pub quorum_lost: Option<(Instant, Vec<u64>)>,
    /// Answers of this range the victim delivered to this range's leader in the hold
    /// that fitted: the replica its re-seed built being caught up.
    pub fitted: usize,
    /// Rejections of this range the victim delivered to this range's leader in the
    /// hold stamped a store incarnation of its own: a re-seeded replica's, which
    /// check quorum counts as any follower's (D-049).
    pub store_rejections: usize,
    /// Rejections of this range the victim delivered to this range's leader in the
    /// hold stamped **incarnation 0**: a store-less refused server's, which is the
    /// only answer D-049's rule is about. The node produces none, and
    /// [`NodeReport::check`] says so per seed.
    pub refused_rejections: usize,
    /// The leader's first commit of its term past its commit index at the cut, in
    /// the hold.
    pub commit_after_cut: Option<Instant>,
    /// Servers that became leader of this range in the hold, keeper's side and
    /// other's alike.
    pub elected_in_hold: BTreeSet<u64>,
    /// Snapshot streams opened toward the victim for this range: the wiring's path.
    pub streams: usize,
}

/// What one run of the sharded scenario produced.
// PROPOSED(D-085): sharded is one fault and four answers, not the scenario four times.
#[derive(Debug)]
pub struct NodeReport {
    /// The seed.
    pub seed: u64,
    /// Which core ran.
    pub variants: Variants,
    /// Which node ran.
    pub node: NodeVariants,
    /// Who played which part, once the setup got that far.
    pub cast: Option<NodeCast>,
    /// When the third node was cut off, and when the hold ended.
    pub cut: Option<(Instant, Instant)>,
    /// `RaftRefused` of the victim: the setup's own non-vacuity.
    pub refusals: usize,
    /// Whether the cut left ranges on both sides — the keeper's majority of itself
    /// and the refused node, and the cut-off node's leadership of the rest — so that
    /// this seed carries **both** of the scenario's outcomes at once.
    ///
    /// D-085 designed this as a per-seed guarantee, and on the node it is not one
    /// without the receive cap: which of the two survivors leads which range is
    /// leadership's business and not the scenario's, and a seed on which one of them
    /// leads all four carries one outcome. The victim is chosen to preserve the split
    /// its two neighbours already have (`node_run`), which leaves the degenerate seeds
    /// few; the share is measured and printed rather than assumed, and every range is
    /// asked its own question on every seed either way.
    pub both_outcomes: bool,
    /// Each node's clock rate error, in parts per million.
    pub drifts: Vec<i64>,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// What the moirae export needs besides [`NodeReport::records`].
    pub run: RunHeader,
    /// The clients' history.
    pub history: History,
}

/// The sharded check-quorum scenario on the node of SHARD.md §4 (PROPOSED D-085):
/// three nodes, four ranges on every node, **one** `mark_store_lost` at the victim's
/// restart, and check quorum asked of four leaders about the one refused node.
///
/// What "sharded" means here is D-085's reading: not the scenario run four times, but
/// one fault and four answers. Four things change in kind, and none of them can be
/// said in a single-range world:
///
/// 1. **The refusal is the node's.** The one mark refuses four replicas and re-seeds
///    four, each with a store incarnation of its own drawn from the node's generator
///    (D-077). `NodeVariant::RefuseOneRangeOnly` refuses one, and this scenario
///    catches it on every seed.
/// 2. **The leader is per range.** The four ranges need not agree on a leader, so the
///    cast is a leader per range, each with its own term, its own commit index at the
///    cut and its own check-quorum window on its own clock.
/// 3. **The cut is one partition and the answers are four.** Cutting the third node
///    off is one fault on one link — on the node, one socket carrying every range's
///    frames (Q10, D-072) — and it leaves the ranges the keeper leads with a majority
///    of the keeper and the refused node, and the ranges the other leads with no
///    majority on either side.
/// 4. **Both outcomes at once, on one node, on every seed.** The keeper's ranges keep
///    their leader through the hold and commit past their commit index at the cut,
///    through the very replicas the refusal re-seeded; the other's ranges lose theirs
///    within two windows of the cut and **stay leaderless**, because the re-seeded
///    replicas are quarantined and neither vote nor campaign (D-035) and the keeper
///    alone is no majority. A single range asserts one or the other and never both.
///
/// **What this is not.** It is not D-049's two halves. Those are about a *store-less*
/// refused server, whose rejections are stamped incarnation 0, and about the re-seed
/// stream whose acknowledgements make them count; the node produces neither, and
/// [`NodeReport::check`] asserts both absences per seed with the reason and the
/// trigger that upgrades them.
// PROPOSED(D-085): sharded is one fault and four answers, not the scenario four times.
#[must_use]
pub fn node_run(
    seed: u64,
    variants: impl Into<Variants>,
    node_variants: NodeVariants,
) -> NodeReport {
    let cluster = Cluster::Node;
    let variants = variants.into();
    let mut rng = moirae_sched::stream(seed, "quorum-node");
    let mut config = SimConfig::new(seed);
    // D-049's network and disk, kept: the halves are claims about time, so the
    // schedule is uniform (D-016), nothing is dropped at random and nothing rots, and
    // the disk takes no time — which on the node is the stronger reading of D-049's
    // own reason, since the silence while a refused node rebuilds is now four stores'
    // worth and is shared by all four ranges at once.
    config.net.p_drop = 0.0;
    config.net.p_duplicate = 0.05;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(10);
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.0;
    config.policy = Some(Policy::Uniform);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(SERVERS + CLIENTS).expect("small"),
        Duration::from_secs(8),
    );
    let mut sim = Sim::new(config);
    let mut drifts = Vec::new();
    for _ in 0..SERVERS {
        let magnitude = i64::try_from(rng.below(334)).expect("small");
        let drift = if rng.below(2) == 0 {
            magnitude
        } else {
            -magnitude
        };
        let skew = i64::try_from(rng.below(50_000_000)).expect("small");
        drifts.push(drift);
        sim.add_node_with_clock(skew, drift);
    }
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    for id in 1..=SERVERS {
        spawn(&sim, cluster, id, variants, node_variants, false);
    }
    for (i, &client) in clients.iter().enumerate() {
        let env = sim.env(client);
        let inner = env.clone();
        let stats = Arc::new(Mutex::new(ClientStats::default()));
        env.spawn(
            "client",
            raft::client_on(cluster, inner, i as u64 + 1, SERVERS, stats),
        );
    }
    let mut report = NodeReport {
        seed,
        variants,
        node: node_variants,
        cast: None,
        cut: None,
        refusals: 0,
        both_outcomes: false,
        drifts,
        records: Vec::new(),
        run: sim.run_header(),
        history: History::default(),
    };
    sim.run_for(NODE_WARMUP);
    // The victim is chosen so that the cut can carry **both** outcomes: a node whose
    // two neighbours already lead a range each, since a leader keeps the ranges it
    // leads through another node's crash and a re-seeded replica never campaigns
    // (D-035), so the split the refusal leaves is the split those two had. Which of
    // the candidates is a draw, as the one-group scenario draws its victim from the
    // leader's two followers; when no node leaves such a split — one node leading
    // all four — the draw falls back to any follower of the first range and the seed
    // carries one outcome, which `check` names rather than passes over.
    let leading_now = |sim: &Sim| -> BTreeMap<u64, usize> {
        let mut leading: BTreeMap<u64, usize> = BTreeMap::new();
        for range in cluster.ranges() {
            *leading
                .entry(raft::leader_of_range(sim, range))
                .or_default() += 1;
        }
        leading
    };
    let leading = leading_now(&sim);
    let splits: Vec<u64> = (1..=SERVERS)
        .filter(|&s| {
            (1..=SERVERS)
                .filter(|&o| o != s)
                .all(|o| leading.get(&o).copied().unwrap_or(0) > 0)
        })
        .collect();
    let candidates: Vec<u64> = if splits.is_empty() {
        let first = raft::leader_of_range(&sim, cluster.ranges()[0]);
        (1..=SERVERS).filter(|&s| s != first).collect()
    } else {
        splits
    };
    let victim =
        candidates[usize::try_from(rng.below(u64::try_from(candidates.len()).expect("small")))
            .expect("small")];
    sim.crash(node(victim));
    sim.run_for(Duration::from_millis(20));
    sim.restart(node(victim));
    spawn(&sim, cluster, victim, variants, node_variants, true);
    // The re-seed lands and the ranges the victim led elect again. A re-seeded replica
    // never campaigns (D-035), so every range is led by one of the other two or by
    // nobody; the cast is taken only once none of them names the victim.
    let mut settled = Duration::ZERO;
    let step = Duration::from_millis(50);
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    while settled < SETTLE {
        sim.run_for(step);
        settled += step;
        leaders = cluster
            .ranges()
            .into_iter()
            .map(|range| (range, raft::leader_of_range(&sim, range)))
            .collect();
        if leaders.values().all(|&leader| leader != victim)
            && sim.trace().iter().any(
                |r| matches!(r.event, TraceEvent::RaftReseeded { server, .. } if server == victim),
            )
        {
            break;
        }
    }
    if leaders.values().all(|&leader| leader != victim) {
        // The keeper is the node that leads most of what is left, so the ranges it
        // leads — at least two of four on two eligible nodes — are the ones whose
        // majority the cut leaves as the keeper and the refused node.
        let mut leading: BTreeMap<u64, usize> = BTreeMap::new();
        for &leader in leaders.values() {
            *leading.entry(leader).or_default() += 1;
        }
        let keeper = (1..=SERVERS)
            .filter(|&s| s != victim)
            .max_by_key(|s| (leading.get(s).copied().unwrap_or(0), std::cmp::Reverse(*s)))
            .expect("two nodes are not the victim");
        let other = (1..=SERVERS)
            .find(|&s| s != victim && s != keeper)
            .expect("three nodes");
        report.cast = Some(NodeCast {
            victim,
            keeper,
            other,
            leaders,
        });
        let rest: Vec<NodeId> = (1..=SERVERS)
            .filter(|&s| s != other)
            .map(node)
            .chain(clients.iter().copied())
            .chain(std::iter::once(admin))
            .collect();
        let cut = sim.now();
        sim.partition(&[node(other)], &rest);
        // Each range the keeper still leads is given something of its own to commit
        // in the hold, on a key of that range, so that "the leader committed past its
        // commit index at the cut" is a claim the scenario drives rather than one it
        // waits for the clients to happen to make. The ranges the cut-off node leads
        // are given nothing: their leader is on the far side of the cut.
        let keepers_ranges: Vec<u64> = report
            .cast
            .as_ref()
            .expect("the cast is set")
            .leaders
            .iter()
            .filter(|&(_, &leader)| leader == keeper)
            .map(|(&range, _)| range)
            .collect();
        for (i, range) in keepers_ranges.into_iter().enumerate() {
            let env = sim.env(admin);
            let inner = env.clone();
            let driver = u64::try_from(i).expect("small") + 1;
            env.spawn("hold", async move {
                raft::spread_on(cluster, inner, driver, keeper, range, HOLD_PUTS).await;
            });
        }
        sim.run_for(HOLD);
        report.cut = Some((cut, sim.now()));
        sim.heal();
        sim.run_for(Duration::from_millis(500));
    }
    report.records = sim.trace();
    report.refusals = report
        .records
        .iter()
        .filter(|r| matches!(r.event, TraceEvent::RaftRefused { server, .. } if server == victim))
        .count();
    report.both_outcomes = report.cast.as_ref().is_some_and(|cast| {
        let sides: BTreeSet<u64> = cast.leaders.values().copied().collect();
        sides.contains(&cast.keeper) && sides.contains(&cast.other)
    });
    report.history = History::from_trace(&report.records);
    report.run = sim.run_header();
    report
}

impl NodeReport {
    /// The trace as moirae JSONL, written when it is asked for (D-052).
    ///
    /// # Panics
    ///
    /// If the trace does not export to moirae v2.
    // PROPOSED(D-052): a scenario's moirae JSONL is written when it is asked for.
    #[must_use]
    pub fn jsonl(&self) -> String {
        self.run
            .to_moirae(&self.records, &Export::new(&ananke_shard::frame::studio))
            .expect("the scenario's trace exports to moirae v2")
    }

    /// How long `local` of `server`'s clock takes in global time, at its rate.
    #[must_use]
    pub fn global(&self, server: u64, local: Duration) -> Duration {
        let ppm = self.drifts[usize::try_from(server - 1).expect("a node")];
        let nanos = i128::try_from(local.as_nanos()).expect("small") * 1_000_000
            / (1_000_000 + i128::from(ppm));
        Duration::from_nanos(u64::try_from(nanos).expect("positive"))
    }

    /// What each range saw in the hold.
    #[must_use]
    pub fn holds(&self) -> Option<BTreeMap<u64, RangeHold>> {
        let (cast, (cut, heal)) = (self.cast.as_ref()?, self.cut?);
        let victim = cast.victim;
        let in_hold = |t: Instant| cut < t && t <= heal;
        // The refusal's place in the trace, not its instant: the disk takes no time
        // here, so the re-seed's restatements share the refusal's instant exactly.
        let refusal = self
            .records
            .iter()
            .position(
                |r| matches!(r.event, TraceEvent::RaftRefused { server, .. } if server == victim),
            )
            .unwrap_or(usize::MAX);
        let mut holds: BTreeMap<u64, RangeHold> = cast
            .leaders
            .iter()
            .map(|(&range, &leader)| {
                (
                    range,
                    RangeHold {
                        range,
                        leader,
                        ..RangeHold::default()
                    },
                )
            })
            .collect();
        for r in self.records.iter().filter(|r| r.decided <= cut) {
            match &r.event {
                TraceEvent::RaftLeader {
                    server,
                    range,
                    term,
                    ..
                } => {
                    if let Some(hold) = holds.get_mut(range)
                        && *server == hold.leader
                    {
                        hold.term = *term;
                    }
                }
                TraceEvent::RaftCommit {
                    server,
                    range,
                    index,
                    ..
                } => {
                    if let Some(hold) = holds.get_mut(range)
                        && *server == hold.leader
                    {
                        hold.commit_at_cut = hold.commit_at_cut.max(*index);
                    }
                }
                _ => {}
            }
        }
        let mut payloads: BTreeMap<ananke_env::MessageId, bytes::Bytes> = BTreeMap::new();
        for (i, r) in self.records.iter().enumerate() {
            match &r.event {
                TraceEvent::RaftReplicaRefused { server, range } if *server == victim => {
                    if let Some(hold) = holds.get_mut(range) {
                        hold.replica_refused = true;
                    }
                }
                TraceEvent::RaftReseeded { server, range } if *server == victim => {
                    if let Some(hold) = holds.get_mut(range) {
                        hold.reseeded = true;
                    }
                }
                // The re-seeded replica's own restatement of the store it built, which
                // every range has whether or not its leader is reachable across the cut.
                TraceEvent::RaftRecovered {
                    server,
                    range,
                    incarnation,
                    ..
                } if *server == victim && i > refusal => {
                    if let Some(hold) = holds.get_mut(range) {
                        hold.incarnation = Some(*incarnation);
                    }
                }
                TraceEvent::RaftSnapshotStreams { range, to, .. } if *to == victim => {
                    if let Some(hold) = holds.get_mut(range) {
                        hold.streams += 1;
                    }
                }
                TraceEvent::RaftLeader {
                    server,
                    range,
                    term,
                    ..
                } if in_hold(r.decided) => {
                    if let Some(hold) = holds.get_mut(range) {
                        hold.elected_in_hold.insert(*server);
                        let _ = term;
                    }
                }
                TraceEvent::RaftQuorumLost {
                    server,
                    range,
                    term,
                    uncounted,
                } if in_hold(r.decided) => {
                    if let Some(hold) = holds.get_mut(range)
                        && *server == hold.leader
                        && *term == hold.term
                        && hold.quorum_lost.is_none()
                    {
                        hold.quorum_lost = Some((r.decided, uncounted.clone()));
                    }
                }
                TraceEvent::RaftCommit {
                    server,
                    range,
                    term,
                    index,
                    ..
                } if in_hold(r.decided) => {
                    if let Some(hold) = holds.get_mut(range)
                        && *server == hold.leader
                        && *term == hold.term
                        && *index > hold.commit_at_cut
                        && hold.commit_after_cut.is_none()
                    {
                        hold.commit_after_cut = Some(r.decided);
                    }
                }
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDelivered { id, to, .. } if in_hold(r.at) => {
                    let Some(bytes) = payloads.get(id) else {
                        continue;
                    };
                    let Ok(decoded) = ananke_shard::frame::decode(bytes) else {
                        continue;
                    };
                    for tagged in decoded.messages {
                        let Some(hold) = holds.get_mut(&tagged.range.get()) else {
                            continue;
                        };
                        if tagged.frame.from.0 != victim || server_of(*to) != Some(hold.leader) {
                            continue;
                        }
                        match tagged.frame.message {
                            Message::AppendEntriesResponse {
                                success: true,
                                incarnation,
                                ..
                            } => {
                                hold.fitted += 1;
                                hold.answered_incarnations.insert(incarnation);
                            }
                            Message::AppendEntriesResponse {
                                success: false,
                                incarnation: 0,
                                ..
                            } => hold.refused_rejections += 1,
                            Message::AppendEntriesResponse {
                                success: false,
                                incarnation,
                                ..
                            } => {
                                hold.store_rejections += 1;
                                hold.answered_incarnations.insert(incarnation);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Some(holds)
    }

    /// What the sharded scenario must satisfy, or the first violation.
    ///
    /// Asked per seed, and inside it per range, in this order:
    ///
    /// 1. **The setup.** The one `mark_store_lost` refused the node — an absence read
    ///    off a run that refused nothing is no evidence at all — and every range found
    ///    a leader that is not the victim before the cut.
    /// 2. **The safety checks** of §8 keyed by range, commit by majority and
    ///    linearizability over the trace.
    /// 3. **The fan-out (D-077).** Every one of the node's four replicas was refused
    ///    with it and re-seeded, each stamping its answers with a store incarnation of
    ///    its own, the four of them four different numbers.
    /// 4. **Check quorum, per range.** A range the keeper leads keeps its leader
    ///    through the hold and commits an entry of its term past its commit index at
    ///    the cut — on a majority of the keeper and the node the refusal re-seeded. A
    ///    range the cut-off node leads loses its leader within two windows and three
    ///    ticks of the cut, and **stays leaderless for the hold**, because a re-seeded
    ///    replica neither votes nor campaigns (D-035) and the keeper alone is no
    ///    majority. A range that reaches neither fails, naming the range: the scenario
    ///    must not pass by not asking.
    /// 5. **The two absences D-049's halves need, per range, with the slice that owns
    ///    each.** No answer of the victim's was a store-less refused server's — the
    ///    rejection stamped incarnation 0 that is the only answer D-049's rule is
    ///    about — so no step-down left anything `uncounted`; and no snapshot stream was
    ///    opened toward the victim and no replica created by an install. The day
    ///    either arrives this fails and says which half can then be built.
    ///
    /// # Errors
    ///
    /// A message naming the seed, the range and the violation.
    #[expect(
        clippy::too_many_lines,
        reason = "the verdict is four ranges' worth of one scenario, and splitting it \
                  would hide the order the clauses are asked in"
    )]
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        let fail = |what: String| Err(format!("seed {seed}: {what}"));
        if self.refusals != 1 {
            return fail(format!(
                "setup: the victim's restart on a store marked lost traced {} whole-node \
                 refusals, not the one this scenario is about, so nothing below is evidence",
                self.refusals
            ));
        }
        let Some(cast) = self.cast.as_ref() else {
            return fail(format!(
                "setup: some range was still led by the refused node {SETTLE:?} after its \
                 restart, so the cut had no keeper to leave a majority with"
            ));
        };
        crate::ranges::invariants_of(&self.records).map_err(|v| format!("seed {seed}: {v}"))?;
        lin::check(&self.history).map_err(|v| format!("seed {seed}: {v}"))?;
        let holds = self.holds().expect("a cast has holds");
        let (cut, _) = self.cut.expect("a cast has a cut");
        let NodeCast {
            victim,
            keeper,
            other,
            ..
        } = *cast;
        if let Some(record) = self.records.iter().find(|r| {
            matches!(&r.event, TraceEvent::RangeCreated { cause, .. } if *cause == RangeCause::Snapshot)
        }) {
            return fail(format!(
                "a replica was created by a snapshot install ({:?}), where this tree's \
                 `ananke_shard::server::ServerHost` counts a snapshot action and drops it. The \
                 node's snapshot wiring (PR #107) has landed, and the open half of D-049 — the \
                 stream whose acknowledgements make a refused follower count — can be asked of \
                 the node; ask also whether a re-seeded replica the leader can no longer feed \
                 from its log is still counted, which is the finding in PROPOSED D-085",
                record.event
            ));
        }
        // The fan-out, and the four incarnations that are the sharded reading of it.
        let mut incarnations: BTreeMap<u64, u64> = BTreeMap::new();
        for hold in holds.values() {
            let range = hold.range;
            if !hold.replica_refused {
                return fail(format!(
                    "range {range}: the one `mark_store_lost` at node {victim}'s restart refused \
                     the node but not this range's replica, where D-077 refuses every replica the \
                     node held"
                ));
            }
            if !hold.reseeded {
                return fail(format!(
                    "range {range}: node {victim}'s replica of it was refused and never re-seeded"
                ));
            }
            if hold.streams > 0 {
                return fail(format!(
                    "range {range}: {} snapshot streams were opened toward refused node {victim}, \
                     where this tree's node counts a snapshot action and drops it. The node's \
                     snapshot wiring (PR #107) has landed and D-049's open half can be built",
                    hold.streams
                ));
            }
            if hold.refused_rejections > 0 {
                return fail(format!(
                    "range {range}: node {victim} answered leader {} with {} rejections stamped \
                     incarnation 0, the store-less refused server's answer D-049's rule is the \
                     whole of. This tree's node re-seeds into a fresh engine before it serves, so \
                     every answer carries a store incarnation of its own and `refused_answered` is \
                     never set. Something now gives the node a store-less state: D-049's pair can \
                     be re-asserted on it, and PROPOSED D-085's finding must be re-read",
                    hold.leader, hold.refused_rejections
                ));
            }
            if let Some((_, uncounted)) = &hold.quorum_lost
                && !uncounted.is_empty()
            {
                return fail(format!(
                    "range {range}: leader {} stepped down leaving {uncounted:?} uncounted, where \
                     no answer on the node is a refused server's. D-049's rule has a site on the \
                     node now and its pair can be re-asserted there",
                    hold.leader
                ));
            }
            let Some(incarnation) = hold.incarnation else {
                return fail(format!(
                    "range {range}: node {victim}'s re-seeded replica of it restated no store \
                     incarnation of its own, where every re-seeded replica draws one from the \
                     node's generator (D-077)"
                ));
            };
            if let Some(twin) = incarnations.insert(incarnation, range) {
                return fail(format!(
                    "ranges {twin} and {range}: node {victim}'s re-seeded replicas of them carry \
                     the same store incarnation {incarnation}, where each draws its own from the \
                     node's generator (D-077, Q26). One number for four replicas is a mark read \
                     per node where it is per (range, node)"
                ));
            }
        }
        // Check quorum, per range, of four leaders about the one refused node.
        let mut kept = 0usize;
        let mut lost = 0usize;
        for hold in holds.values() {
            let range = hold.range;
            if hold.leader == keeper {
                kept += 1;
                if let Some((at, _)) = hold.quorum_lost {
                    return fail(format!(
                        "range {range}: keeper {keeper} stepped down {:?} after the cut at \
                         {cut:?}, with its majority of itself and the node its refusal re-seeded \
                         ({} answers of this range fitted, {} rejections carried its store)",
                        at.duration_since(cut),
                        hold.fitted,
                        hold.store_rejections
                    ));
                }
                if hold.fitted + hold.store_rejections == 0 {
                    return fail(format!(
                        "range {range}: keeper {keeper} kept its office through the hold with no \
                         answer of this range from the node its refusal re-seeded, so the office \
                         it kept was not kept on the answers this scenario is about"
                    ));
                }
                let mine = hold.incarnation.expect("every range restated one");
                if hold.answered_incarnations != BTreeSet::from([mine]) {
                    return fail(format!(
                        "range {range}: the answers node {victim} delivered to keeper {keeper} \
                         for it carry {:?}, where its re-seeded replica of this range carries \
                         store incarnation {mine} and no other. An answer counted per \
                         node rather than per (range, follower) would carry every range's",
                        hold.answered_incarnations
                    ));
                }
                if hold.commit_after_cut.is_none() {
                    return fail(format!(
                        "range {range}: keeper {keeper} of term {} committed nothing past index {} \
                         in the hold, through the replica its refusal re-seeded ({} answers \
                         fitted)",
                        hold.term, hold.commit_at_cut, hold.fitted
                    ));
                }
            } else if hold.leader == other {
                lost += 1;
                let bound = self.global(other, ELECTION_MIN * 2 + TICK * 3);
                match hold.quorum_lost {
                    None => {
                        return fail(format!(
                            "range {range}: cut-off leader {other} of term {} kept its office \
                             through the {HOLD:?} after the cut at {cut:?}",
                            hold.term
                        ));
                    }
                    Some((at, _)) if at.duration_since(cut) > bound => {
                        return fail(format!(
                            "range {range}: cut-off leader {other} stepped down {:?} after the cut \
                             at {cut:?}, past two windows and three ticks, {bound:?}",
                            at.duration_since(cut)
                        ));
                    }
                    Some(_) => {}
                }
                if hold.fitted + hold.store_rejections + hold.refused_rejections > 0 {
                    return fail(format!(
                        "range {range}: node {victim}'s replica of it delivered {} answers of \
                         this range to leader {other} in the hold, where the cut put that leader \
                         on the far side of it. An answer counted per node rather than per range \
                         would show exactly this",
                        hold.fitted + hold.store_rejections + hold.refused_rejections
                    ));
                }
                if !hold.elected_in_hold.is_empty() {
                    return fail(format!(
                        "range {range}: {:?} became leader of it in the hold, where the keeper \
                         {keeper} alone is no majority and node {victim}'s re-seeded replica \
                         neither votes nor campaigns (D-035)",
                        hold.elected_in_hold
                    ));
                }
            } else {
                return fail(format!(
                    "range {range}: it was led by {} at the cut, which is neither the keeper \
                     {keeper} nor the cut-off node {other}, so the scenario asks it nothing",
                    hold.leader
                ));
            }
        }
        if kept + lost != holds.len() {
            return fail(format!(
                "{kept} ranges on the keeper {keeper}'s majority and {lost} on the cut-off node \
                 {other}, of {} the node holds: some range was asked nothing",
                holds.len()
            ));
        }
        Ok(())
    }
}

/// Runs one half of the scenario on `seed` under `variants`, on the instant disk.
#[must_use]
pub fn run(seed: u64, variants: impl Into<Variants>, half: Half) -> Report {
    run_on(seed, variants, half, Disk::Instant)
}

/// Runs one half of the scenario on `seed` under `variants`, on `disk`.
#[must_use]
pub fn run_on(seed: u64, variants: impl Into<Variants>, half: Half, disk: Disk) -> Report {
    let variants = variants.into();
    let mut rng = moirae_sched::stream(seed, "quorum");
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.0;
    config.net.p_duplicate = 0.05;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(10);
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.0;
    if disk == Disk::Sweep {
        config.fs.latency_min = Duration::from_micros(100);
        config.fs.latency_max = Duration::from_millis(2);
    }
    config.policy = Some(Policy::Uniform);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(SERVERS + CLIENTS).expect("small"),
        Duration::from_secs(8),
    );
    let mut sim = Sim::new(config);
    // Clocks within a third of the drift bound, as the sweep's lenient half draws.
    let mut drifts = Vec::new();
    for _ in 0..SERVERS {
        let magnitude = i64::try_from(rng.below(334)).expect("small");
        let drift = if rng.below(2) == 0 {
            magnitude
        } else {
            -magnitude
        };
        let skew = i64::try_from(rng.below(50_000_000)).expect("small");
        drifts.push(drift);
        sim.add_node_with_clock(skew, drift);
    }
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    for id in 1..=SERVERS {
        spawn(
            &sim,
            Cluster::OneGroup,
            id,
            variants,
            NodeVariants::correct(),
            false,
        );
    }
    for (i, &client) in clients.iter().enumerate() {
        let env = sim.env(client);
        let inner = env.clone();
        let stats = Arc::new(Mutex::new(ClientStats::default()));
        env.spawn("client", raft::client(inner, i as u64 + 1, SERVERS, stats));
    }
    let mut report = Report {
        seed,
        variants,
        half,
        disk,
        cast: None,
        cut: None,
        drifts,
        records: Vec::new(),
        run: sim.run_header(),
        history: History::default(),
    };
    sim.run_for(Duration::from_millis(1200));
    let first = raft::leader_now(&sim);
    {
        let env = sim.env(admin);
        let inner = env.clone();
        env.spawn("fill", async move {
            raft::spread(inner, FILL_DRIVER, first, FILL).await;
        });
    }
    sim.run_for(Duration::from_millis(1000));
    let leader = raft::leader_now(&sim);
    let followers: Vec<u64> = (1..=SERVERS).filter(|&s| s != leader).collect();
    let victim = followers[usize::try_from(rng.below(2)).expect("small")];
    sim.crash(node(victim));
    sim.run_for(Duration::from_millis(20));
    sim.restart(node(victim));
    spawn(
        &sim,
        Cluster::OneGroup,
        victim,
        variants,
        NodeVariants::correct(),
        true,
    );
    let opened = run_until_record(
        &mut sim,
        STREAM_WAIT,
        |r| matches!(r.event, TraceEvent::RaftSnapshotStreams { to, .. } if to == victim),
    );
    if let Some(TraceEvent::RaftSnapshotStreams { server: leader, .. }) = opened.map(|r| r.event) {
        let other = (1..=SERVERS)
            .find(|&s| s != leader && s != victim)
            .expect("three servers");
        report.cast = Some(Cast {
            leader,
            refused: victim,
            other,
        });
        let rest: Vec<NodeId> = (1..=SERVERS)
            .filter(|&s| s != other)
            .map(node)
            .chain(clients.iter().copied())
            .chain(std::iter::once(admin))
            .collect();
        let cut = sim.now();
        sim.partition(&[node(other)], &rest);
        if half == Half::Blocked {
            sim.limit_frames(node(leader), node(victim), MTU);
        }
        sim.run_for(HOLD);
        report.cut = Some((cut, sim.now()));
        sim.heal();
        sim.run_for(Duration::from_millis(1000));
    }
    report.records = sim.trace();
    report.history = History::from_trace(&report.records);
    report.run = sim.run_header();
    report
}

impl Report {
    /// The trace as moirae JSONL, written from [`Report::records`] under the run's
    /// header now, when it is asked for, rather than at the end of every run; the
    /// bytes are the ones the simulator's own export writes (D-052).
    ///
    /// # Panics
    ///
    /// If the trace does not export to moirae v2.
    // PROPOSED(D-052): a scenario's moirae JSONL is written when it is asked for.
    #[must_use]
    pub fn jsonl(&self) -> String {
        self.run
            .to_moirae(&self.records, &Export::new(&message::studio))
            .expect("the scenario's trace exports to moirae v2")
    }

    /// How long `local` of `server`'s clock takes in global time, at its rate.
    #[must_use]
    pub fn global(&self, server: u64, local: Duration) -> Duration {
        let ppm = self.drifts[usize::try_from(server - 1).expect("a server")];
        let nanos = i128::try_from(local.as_nanos()).expect("small") * 1_000_000
            / (1_000_000 + i128::from(ppm));
        Duration::from_nanos(u64::try_from(nanos).expect("positive"))
    }

    /// What happened in the hold.
    #[must_use]
    pub fn hold(&self) -> Option<Hold> {
        let (cast, (cut, heal)) = (self.cast?, self.cut?);
        let Cast {
            leader, refused, ..
        } = cast;
        let in_hold = |t: Instant| cut < t && t <= heal;
        let mut hold = Hold::default();
        for r in self.records.iter().filter(|r| r.decided <= cut) {
            match &r.event {
                TraceEvent::RaftLeader { server, term, .. } if *server == leader => {
                    hold.term = *term;
                }
                TraceEvent::RaftCommit { server, index, .. } if *server == leader => {
                    hold.commit_at_cut = hold.commit_at_cut.max(*index);
                }
                _ => {}
            }
        }
        let mut payloads: BTreeMap<ananke_env::MessageId, bytes::Bytes> = BTreeMap::new();
        for r in &self.records {
            match &r.event {
                TraceEvent::RaftQuorumLost {
                    server,
                    term,
                    uncounted,
                    ..
                } if *server == leader
                    && *term == hold.term
                    && in_hold(r.decided)
                    && hold.quorum_lost.is_none() =>
                {
                    hold.quorum_lost = Some((r.decided, uncounted.clone()));
                }
                TraceEvent::RaftReseeded { server, .. }
                    if *server == refused && in_hold(r.at) && hold.reseeded.is_none() =>
                {
                    hold.reseeded = Some(r.at);
                }
                TraceEvent::RaftCommit {
                    server,
                    term,
                    index,
                    ..
                } if *server == leader
                    && *term == hold.term
                    && *index > hold.commit_at_cut
                    && in_hold(r.decided)
                    && hold.reseeded.is_some_and(|at| r.decided > at)
                    && hold.commit_after_install.is_none() =>
                {
                    hold.commit_after_install = Some(r.decided);
                }
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDropped {
                    to,
                    reason: DropReason::Oversized,
                    ..
                } if server_of(*to) == Some(refused) => hold.oversized += 1,
                TraceEvent::MessageDelivered { id, to, .. }
                    if in_hold(r.at) && server_of(*to) == Some(leader) =>
                {
                    let Some(frame) = payloads.get(id).and_then(|p| Frame::decode(p.clone()).ok())
                    else {
                        continue;
                    };
                    if frame.from.0 != refused {
                        continue;
                    }
                    match frame.message {
                        Message::InstallSnapshotResponse {
                            status: SnapshotStatus::More | SnapshotStatus::Installed,
                            ..
                        } => hold.chunk_acks += 1,
                        Message::AppendEntriesResponse {
                            success: false,
                            incarnation: 0,
                            ..
                        } => hold.refused_rejections += 1,
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        Some(hold)
    }

    /// What the half must satisfy, or the first violation: the setup reached the
    /// stream, the safety checks hold over the trace, and
    ///
    /// - blocked: the leader stepped down within two check-quorum windows and three
    ///   ticks of the cut, by its own clock, naming the refused follower as the
    ///   answer it did not count, with the stream's chunks lost to the limit;
    /// - open: the leader kept its office until the refused follower started on
    ///   the store its re-seed built, and then committed an entry of its term past
    ///   its commit index at the cut, with no check-quorum step-down before that
    ///   commit.
    ///
    /// # Errors
    ///
    /// A message naming the seed, the half and the violation.
    pub fn check(&self) -> Result<(), String> {
        let (seed, half) = (self.seed, self.half);
        let fail = |what: String| Err(format!("seed {seed} {half:?}: {what}"));
        let Some(cast) = self.cast else {
            return fail(format!(
                "setup: no leader opened a snapshot stream to the refused follower within {STREAM_WAIT:?}"
            ));
        };
        if let Err(violation) = invariants::all(crate::traced(&self.records)) {
            return fail(violation);
        }
        if let Err(violation) = invariants::commit_majority(
            crate::traced(&self.records),
            usize::try_from(SERVERS).expect("small"),
        ) {
            return fail(violation);
        }
        if let Err(violation) = lin::check(&self.history) {
            return fail(violation.to_string());
        }
        let hold = self.hold().expect("a cast has a hold");
        let (cut, _) = self.cut.expect("a cast has a cut");
        let Cast {
            leader,
            refused,
            other,
        } = cast;
        match half {
            Half::Blocked => {
                if hold.oversized == 0 {
                    return fail(format!(
                        "setup: no frame from leader {leader} to refused follower {refused} was lost to the limit"
                    ));
                }
                // An acknowledgement in flight at the cut lands within the
                // network's ten milliseconds, a tick, and reaches the core at the
                // tick after, a second: it can make the window after the cut's
                // count the follower too. The window after that has none, and its
                // check, at most two windows and those two ticks past the cut,
                // steps the leader down; one tick more for the check's own.
                let bound = self.global(leader, ELECTION_MIN * 2 + TICK * 3);
                match &hold.quorum_lost {
                    None => fail(format!(
                        "check quorum: leader {leader} of term {} kept its office through the {HOLD:?} after the cut at {cut:?}, with server {other} cut off and the re-seed stream to refused follower {refused} blocked ({} of its rejections and {} chunk acknowledgements delivered)",
                        hold.term, hold.refused_rejections, hold.chunk_acks
                    )),
                    Some((at, _)) if at.duration_since(cut) > bound => fail(format!(
                        "check quorum: leader {leader} of term {} stepped down {:?} after the cut at {cut:?}, past two windows and three ticks, {bound:?}",
                        hold.term,
                        at.duration_since(cut)
                    )),
                    Some((_, uncounted)) if !uncounted.contains(&refused) => fail(format!(
                        "check quorum: leader {leader} stepped down without naming refused follower {refused} uncounted: {uncounted:?}"
                    )),
                    Some(_) => Ok(()),
                }
            }
            Half::Open => {
                let Some(reseeded) = hold.reseeded else {
                    return fail(format!(
                        "re-seed: refused follower {refused} did not start on a re-seeded store in the hold ({} chunk acknowledgements delivered to leader {leader}; step-down: {:?})",
                        hold.chunk_acks, hold.quorum_lost
                    ));
                };
                if let Some((at, uncounted)) = &hold.quorum_lost
                    && hold.commit_after_install.is_none_or(|commit| *at < commit)
                {
                    return fail(format!(
                        "check quorum: leader {leader} of term {} stepped down at {at:?}, {:?} after the cut, with the re-seed stream to refused follower {refused} open and the follower re-seeded at {reseeded:?} (uncounted: {uncounted:?}; {} chunk acknowledgements delivered)",
                        hold.term,
                        at.duration_since(cut),
                        hold.chunk_acks
                    ));
                }
                if hold.commit_after_install.is_none() {
                    return fail(format!(
                        "commit: leader {leader} of term {} committed nothing past index {} after refused follower {refused} was re-seeded at {reseeded:?}",
                        hold.term, hold.commit_at_cut
                    ));
                }
                Ok(())
            }
        }
    }
}

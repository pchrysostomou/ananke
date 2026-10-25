//! `sim/split.rs` (SHARD.md §10): directed shapes about a split, on four nodes, nodes 1
//! to 3 the bootstrap nodes and node 4 a node that joins and holds nothing (SHARD.md §2).
//! This file builds shape (ii), *vote*; (i), *overlap*, is the overlap rule's slice's,
//! and (iii), *crash*, Stage D's.
//!
//! **Shape (ii)** (SHARD.md:1624-1630): P, range 2 over `[k0, k2)`, on nodes 1 to 3.
//! A follower `x` of P is cut off from before a split of P at `k1` until P's leader and
//! R have compacted past the split's index `s`, so `x`'s P can reach past `s` only by P's
//! post-split snapshot, which creates no R, and `x` holds no R of its own. At the heal
//! the node leading R is crashed and `x`'s links to the third node healed, its links to
//! the crashed node staying cut, and the crashed node restarts after ten maximum
//! election timeouts. Until then R's remaining replica can win only with `x`'s vote, and
//! `x`'s R is a placeholder throughout: R has no leader to stream R's snapshot. The
//! correct placeholder grants nothing (Q22), so R waits for the crashed node; under
//! `UninitialisedReplicaVotes` it grants, R elects with it, and check 20 sees the grant.
//!
//! **What is asserted per seed**: the shape was built — the cut began before the split
//! and ended after P's leader and some replica of R compacted past `s`, R had a leader
//! at the heal and it was crashed — and its premise held: `x` applied no split of P,
//! `x`'s P was installed at or past `s`, and where `x` created R it was by a snapshot.
//! And a pre-vote or vote request of R reached `x` before `x`'s `RangeCreated` of R
//! (SHARD.md §12, Stage C's exit). The verdict is checks 1 to 4 and the range layer's
//! folds, checks 7 to 20, and that no node stopped.
// PROPOSED(D-109): `sim/split.rs` (ii).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{RunHeader, Sim, SimConfig, TraceRecord};
use ananke_env::{
    Clock, Either, Environment, Instant, Network, NodeId, Rng, Socket, TraceEvent, race,
};
use ananke_raft::ServerId;
use ananke_raft::apply::{Command, Outcome, user_key, user_key_of};
use ananke_raft::client::{Reply, Request};
use ananke_raft::core::RaftConfig;
use ananke_raft::invariants;
use ananke_raft::message::{self, Message};
use ananke_shard::client::{RangedRequest, RangedResponse};
use ananke_shard::descriptor::{FIRST_GENERATION, RangeDescriptor};
use ananke_shard::range::RangeId;
use ananke_shard::range_variant::RangeVariants;
use ananke_shard::server::ServerConfig;
use ananke_shard::variant::NodeVariants;
use ananke_storage::EngineConfig;
use bytes::Bytes;

use crate::raft::{TICK, admin_addr, client_addr, election_max, server_addr};
use crate::ranges::{DIR, ID_BLOCK, INBOX_BYTES, REFILL_AT, SNAPSHOT_CAP};

/// The nodes: three bootstrap nodes and one that joins (SHARD.md §2, §10).
pub const NODES: u64 = 4;
/// The bootstrap nodes, `1..=BOOTSTRAP`.
pub const BOOTSTRAP: u64 = 3;
/// P, the range that splits: the first user range, `[k0, k2)`.
pub const PARENT: u64 = crate::ranges::FIRST_RANGE;
/// The key P splits at: R is `[k1, k2)`.
pub const SPLIT_KEY: &str = "k1";
/// The left half's key and the right half's, which the writer writes in turn.
const LEFT_KEY: &str = "k0";
const RIGHT_KEY: &str = "k1";
/// The snapshot threshold: low, so both halves compact past the split within the cut.
pub const SNAPSHOT_THRESHOLD: u64 = 12;
/// How long the cluster runs before the cut: every range elects.
const SETTLE: Duration = Duration::from_secs(2);
/// How long the cut runs before the split is asked.
const BEFORE_SPLIT: Duration = Duration::from_millis(300);
/// How long the split, and then both halves' compactions past it, may take.
const SPLIT_BUDGET: Duration = Duration::from_secs(3);
const COMPACT_BUDGET: Duration = Duration::from_secs(8);
/// The slice the driver advances in while it waits for something.
const SLICE: Duration = Duration::from_millis(50);
/// How long the run goes on after the crashed node restarts.
const AFTER: Duration = Duration::from_secs(4);
/// How long the writer waits for an answer.
const WRITE_WAIT: Duration = Duration::from_millis(80);

/// One node's configuration: the bootstrap nodes named, the four user ranges
/// configuration fixes, and a threshold low enough that both halves compact.
#[must_use]
pub fn server_config(id: u64, range_variants: RangeVariants) -> ServerConfig {
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    ServerConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=NODES).map(|s| (ServerId(s), server_addr(s))).collect(),
        ranges: crate::ranges::ranges(),
        bootstrap: (1..=BOOTSTRAP).map(ServerId).collect(),
        raft: RaftConfig {
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: INBOX_BYTES,
        snapshot_cap: SNAPSHOT_CAP,
        id_block: ID_BLOCK,
        refill_at: REFILL_AT,
        node: NodeVariants::correct(),
        range_variants,
    }
}

/// The simulator: a network that delays and a disk that takes time, and nothing
/// dropped or rotted — the shape's faults are the driver's alone.
#[must_use]
pub fn config(seed: u64) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.0;
    config.net.p_duplicate = 0.0;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(4);
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.0;
    config.fs.latency_min = Duration::from_micros(100);
    config.fs.latency_max = Duration::from_millis(1);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(NODES + 1).expect("small"),
        SETTLE + BEFORE_SPLIT + SPLIT_BUDGET + COMPACT_BUDGET + election_max() * 10 + AFTER,
    );
    config
}

/// What one run of shape (ii) built and traced.
#[derive(Clone, Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// The range layer's variants the nodes ran.
    pub range_variants: RangeVariants,
    /// The run's trace.
    pub records: Vec<TraceRecord>,
    /// What the moirae export needs besides [`Report::records`] (D-052).
    pub run: RunHeader,
    /// The follower of P cut off across the split.
    pub x: u64,
    /// R's leader at the heal, crashed there.
    pub crashed: Option<u64>,
    /// The split: its index and the right half's id.
    pub split: Option<(u64, u64)>,
    /// When the cut began and when it healed.
    pub cut: Instant,
    /// When the cut healed, if the shape got that far.
    pub healed: Option<Instant>,
    /// What the driver could not build, if anything.
    pub unbuilt: Option<String>,
}

impl Report {
    /// The trace as moirae JSONL, written when it is asked for (D-052).
    ///
    /// # Panics
    ///
    /// If the trace does not export to moirae v2.
    #[must_use]
    pub fn jsonl(&self) -> String {
        self.run
            .to_moirae(&self.records, &Export::new(&message::studio))
            .expect("the split trace exports to moirae v2")
    }

    /// The node of a record, as a server id: a scenario's node ids are its server ids.
    fn node_of(record: &TraceRecord) -> Option<u64> {
        record.node.map(|node| u64::from(node.get()))
    }

    /// The record index at which `x`'s `RangeCreated` of R was traced, if it was.
    #[must_use]
    pub fn x_created_right_at(&self) -> Option<usize> {
        let (_, right) = self.split?;
        self.records.iter().position(|record| {
            Self::node_of(record) == Some(self.x)
                && matches!(&record.event, TraceEvent::RangeCreated { range, .. } if *range == right)
        })
    }

    /// The first pre-vote or vote request of R delivered to `x`, as a record index.
    #[must_use]
    pub fn vote_asked_of_x_at(&self) -> Option<usize> {
        let (_, right) = self.split?;
        let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
        for (at, record) in self.records.iter().enumerate() {
            match &record.event {
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDelivered { id, to, .. }
                    if crate::raft::server_of(*to) == Some(self.x) =>
                {
                    let Some(payload) = payloads.get(id) else {
                        continue;
                    };
                    if crate::raft::messages_of(payload)
                        .into_iter()
                        .any(|(range, message)| {
                            range == right
                                && matches!(
                                    message,
                                    Message::PreVote { .. } | Message::RequestVote { .. }
                                )
                        })
                    {
                        return Some(at);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Shape (ii)'s premise, per seed: `x` applied no split of P, `x`'s P was installed at
    /// or past the split's index, and `x` created R, where it did, by a snapshot alone.
    ///
    /// # Errors
    ///
    /// Which of the three did not hold, with the seed.
    pub fn premise(&self) -> Result<(), String> {
        let (seed, x) = (self.seed, self.x);
        let Some((index, right)) = self.split else {
            return Err(format!(
                "seed {seed}: P never split, so there is no premise"
            ));
        };
        let on_x = |record: &&TraceRecord| Self::node_of(record) == Some(x);
        if self.records.iter().filter(on_x).any(|record| {
            matches!(&record.event, TraceEvent::RangeSplit { range, .. } if *range == PARENT)
        }) {
            return Err(format!(
                "seed {seed}: node {x} applied P's split at {index} by its log, where shape                  (ii) has its P reach past the split only by P's post-split snapshot"
            ));
        }
        let installed = self.records.iter().any(|record| {
            matches!(&record.event, TraceEvent::RaftSnapshot {
                server, range, last_index, taken: false, ..
            } if *server == x && *range == PARENT && *last_index >= index)
        });
        if !installed {
            return Err(format!(
                "seed {seed}: node {x}'s P was never installed at or past the split's index                  {index}, so nothing says how it got past the split, if it did"
            ));
        }
        let causes: Vec<ananke_env::RangeCause> = self
            .records
            .iter()
            .filter(on_x)
            .filter_map(|record| match &record.event {
                TraceEvent::RangeCreated { range, cause, .. } if *range == right => Some(*cause),
                _ => None,
            })
            .collect();
        if causes
            .iter()
            .any(|cause| *cause != ananke_env::RangeCause::Snapshot)
        {
            return Err(format!(
                "seed {seed}: node {x} created the right half {right} by {causes:?}, where                  shape (ii) has it hold no R but one a snapshot gave it"
            ));
        }
        Ok(())
    }

    /// Every `RaftServerFailed` of the run.
    #[must_use]
    pub fn failures(&self) -> Vec<String> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftServerFailed { server, reason } => {
                    Some(format!("server {server}: {reason}"))
                }
                _ => None,
            })
            .collect()
    }

    /// The shape, per seed (SHARD.md §12, Stage C's exit): built, its premise held, and
    /// a pre-vote or vote request of R reached `x` before `x`'s `RangeCreated` of R.
    ///
    /// The premise is that `x`'s P reaches past `s` only by P's post-split snapshot,
    /// which creates no R, so that `x` holds no R of its own. The driver's wait for P's
    /// leader to compact past `s` aims at it, and these three say it was reached: no
    /// `RangeSplit` of P traced on `x`, which a log that carried `s` to `x` would have
    /// traced with R's `RangeCreated { cause: split }`; an install of P on `x` at or
    /// past `s`; and every creation of R on `x` a snapshot's. A schedule that let `x`
    /// take `s` by its log after all — P's leadership moving at the heal to a replica
    /// that had not compacted — fails here rather than passing whenever the vote request
    /// happened to arrive first (#174's review, N2).
    ///
    /// # Errors
    ///
    /// What was not built or not reached, with the seed.
    pub fn shape(&self) -> Result<(), String> {
        let seed = self.seed;
        if let Some(why) = &self.unbuilt {
            return Err(format!("seed {seed}: shape (ii) was not built: {why}"));
        }
        self.premise()?;
        let asked = self.vote_asked_of_x_at().ok_or_else(|| {
            format!(
                "seed {seed}: no pre-vote or vote request of the right half reached node {}",
                self.x
            )
        })?;
        if let Some(created) = self.x_created_right_at()
            && created < asked
        {
            return Err(format!(
                "seed {seed}: node {} created the right half before any pre-vote or vote \
                 request of it reached it",
                self.x
            ));
        }
        Ok(())
    }

    /// The run's verdict: checks 1 to 4, the range layer's folds (checks 7 to 20), and
    /// no node stopped.
    ///
    /// # Errors
    ///
    /// The first violation, with the seed.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        if let Some(failed) = self.failures().first() {
            return Err(format!("seed {seed}: a node stopped: {failed}"));
        }
        if let Err(violation) = invariants::all(crate::traced(&self.records)) {
            return Err(format!("seed {seed}: {violation}"));
        }
        if let Err(violation) =
            invariants::commit_majority(crate::traced(&self.records), BOOTSTRAP as usize)
        {
            return Err(format!("seed {seed}: {violation}"));
        }
        if let Err(violation) = ananke_shard::invariants::all(&self.records) {
            return Err(format!("seed {seed}: {violation}"));
        }
        Ok(())
    }
}

/// The range a key is in, and its generation, as the writer last learnt them.
type Routes = Arc<Mutex<BTreeMap<Bytes, (u64, u64)>>>;

/// The writer: puts to the left half's key and the right half's in turn, so both halves
/// of P go on taking entries and compact past the split. It routes by what a
/// `RangeMismatch` tells it and follows `NotLeader`, and keeps nothing else.
async fn writer<E: Environment>(env: E, routes: Routes) {
    let Ok(sock) = env.net().bind(client_addr(1)).await else {
        return;
    };
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    let mut seq = 0u64;
    loop {
        for key in [LEFT_KEY, RIGHT_KEY] {
            let key = Bytes::from(key);
            let (range, generation) = lock(&routes)
                .get(&key)
                .copied()
                .unwrap_or((PARENT, FIRST_GENERATION));
            seq += 1;
            let target = leaders
                .get(&range)
                .copied()
                .unwrap_or_else(|| 1 + env.rng().below(BOOTSTRAP));
            let request = RangedRequest {
                generation,
                range: RangeId(range),
                request: Request {
                    client: 1,
                    seq,
                    command: Command::Put {
                        key: key.clone(),
                        value: Bytes::from(format!("v{seq}")),
                    },
                },
            };
            let _ = sock.send(server_addr(target), request.encode()).await;
            let deadline = env.clock().now() + WRITE_WAIT;
            loop {
                let recv = pin!(sock.recv());
                let timer = pin!(env.clock().sleep_until(deadline));
                let bytes = match race(&env, recv, timer).await {
                    Either::Left(Ok((_, bytes))) => bytes,
                    Either::Left(Err(_)) | Either::Right(()) => {
                        leaders.remove(&range);
                        break;
                    }
                };
                let Ok(answer) = RangedResponse::decode(bytes) else {
                    continue;
                };
                if answer.response.seq != seq {
                    continue;
                }
                match answer.response.reply {
                    Reply::NotLeader { leader: Some(who) } => {
                        leaders.insert(range, who.0);
                    }
                    Reply::NotLeader { leader: None } => {
                        leaders.remove(&range);
                    }
                    Reply::RangeMismatch { descriptors } => {
                        let encoded = user_key(&key);
                        if let Some(owner) = descriptors
                            .iter()
                            .filter_map(|bytes| RangeDescriptor::decode(bytes.clone()).ok())
                            .filter(|d| d.start <= encoded && encoded < d.end)
                            .max_by_key(|d| d.generation)
                        {
                            lock(&routes)
                                .insert(key.clone(), (owner.range.get(), owner.generation));
                        }
                    }
                    Reply::Outcome(_) => {
                        leaders.insert(range, target);
                    }
                }
                break;
            }
            env.clock().sleep(Duration::from_millis(5)).await;
        }
    }
}

fn lock<T>(what: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    what.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The operator's split of P at [`SPLIT_KEY`], asked of `target` and following
/// `NotLeader` round the bootstrap nodes; it records nothing, since the trace says
/// whether the split took effect.
async fn split<E: Environment>(env: E, mut target: u64) {
    let Ok(sock) = env.net().bind(admin_addr(1)).await else {
        return;
    };
    for _ in 0..BOOTSTRAP * 4 {
        let request = RangedRequest {
            generation: FIRST_GENERATION,
            range: RangeId(PARENT),
            request: Request {
                client: u64::MAX >> 2,
                seq: 1,
                command: Command::Split {
                    key: Bytes::from(SPLIT_KEY),
                    right: 0,
                },
            },
        };
        if sock
            .send(server_addr(target), request.encode())
            .await
            .is_err()
        {
            return;
        }
        let recv = pin!(sock.recv());
        let timer = pin!(env.clock().sleep(Duration::from_millis(300)));
        let Either::Left(Ok((_, bytes))) = race(&env, recv, timer).await else {
            target = target % BOOTSTRAP + 1;
            continue;
        };
        let Ok(answer) = RangedResponse::decode(bytes) else {
            continue;
        };
        match answer.response.reply {
            Reply::NotLeader { leader: Some(who) } => target = who.0,
            Reply::NotLeader { leader: None } => {
                env.clock().sleep(Duration::from_millis(50)).await;
                target = target % BOOTSTRAP + 1;
            }
            Reply::Outcome(Outcome::Done) | Reply::Outcome(_) | Reply::RangeMismatch { .. } => {
                return;
            }
        }
    }
}

/// The latest leader `records` name for `range`, by term.
fn leader_of(records: &[TraceRecord], range: u64) -> Option<u64> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            TraceEvent::RaftLeader {
                server,
                range: led,
                term,
                ..
            } if *led == range => Some((*term, *server)),
            _ => None,
        })
        .max()
        .map(|(_, server)| server)
}

/// The split of P the trace names, by its first `RangeSplit`: (index, right half).
fn split_of(records: &[TraceRecord]) -> Option<(u64, u64)> {
    records.iter().find_map(|record| match &record.event {
        TraceEvent::RangeSplit {
            range,
            right,
            index,
            key,
        } if *range == PARENT && user_key_of(key) == Some(SPLIT_KEY.as_bytes()) => {
            Some((*index, *right))
        }
        _ => None,
    })
}

/// Whether some replica of `range` compacted its log past `index`, or, with `by`, that
/// replica on server `by`.
fn compacted_past(records: &[TraceRecord], range: u64, index: u64, by: Option<u64>) -> bool {
    records.iter().any(|record| {
        matches!(&record.event, TraceEvent::RaftCompacted { server, range: of, through }
            if *of == range && *through > index && by.is_none_or(|by| by == *server))
    })
}

/// Runs shape (ii) for `seed` under `range_variants`.
#[must_use]
pub fn run(seed: u64, range_variants: RangeVariants) -> Report {
    let mut sim = Sim::new(config(seed));
    let nodes: Vec<NodeId> = (0..NODES).map(|_| sim.add_node()).collect();
    let client = sim.add_node();
    for id in 1..=NODES {
        spawn(&sim, nodes[id as usize - 1], id, range_variants);
    }
    let routes: Routes = Arc::default();
    {
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn("writer", writer(inner, routes.clone()));
    }
    sim.run_for(SETTLE);
    let mut draw = moirae_sched::stream(seed, "split-vote");
    let leader = leader_of(&sim.trace(), PARENT).unwrap_or(1);
    // x: a follower of P, drawn from the two bootstrap nodes that do not lead it.
    let followers: Vec<u64> = (1..=BOOTSTRAP).filter(|id| *id != leader).collect();
    let x = followers[usize::try_from(draw.below(followers.len() as u64)).expect("small")];
    let others: Vec<NodeId> = (1..=NODES)
        .filter(|id| *id != x)
        .map(|id| nodes[id as usize - 1])
        .chain([client])
        .collect();
    sim.partition(&[nodes[x as usize - 1]], &others);
    let cut = sim.now();
    let mut report = Report {
        seed,
        range_variants,
        records: Vec::new(),
        run: sim.run_header(),
        x,
        crashed: None,
        split: None,
        cut,
        healed: None,
        unbuilt: None,
    };
    sim.run_for(BEFORE_SPLIT);
    {
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn("split", split(inner, leader));
    }
    let deadline = sim.now() + SPLIT_BUDGET;
    while split_of(&sim.trace()).is_none() && sim.now() < deadline {
        sim.run_for(SLICE);
    }
    let Some((index, right)) = split_of(&sim.trace()) else {
        report.unbuilt = Some(format!("P never split within {SPLIT_BUDGET:?}"));
        report.records = sim.trace();
        return report;
    };
    report.split = Some((index, right));
    let deadline = sim.now() + COMPACT_BUDGET;
    loop {
        let records = sim.trace();
        // P's compaction is its leader's, which is the replica that feeds `x` at the
        // heal: a follower's would leave the leader able to send `x` the split's entry
        // by its log (#174's review, N2). R's is any replica's, since `x` holds no R.
        let leader = leader_of(&records, PARENT);
        if leader.is_some_and(|leader| compacted_past(&records, PARENT, index, Some(leader)))
            && compacted_past(&records, right, index, None)
        {
            break;
        }
        if sim.now() >= deadline {
            report.unbuilt = Some(format!(
                "P's leader and R did not both compact past {index} within {COMPACT_BUDGET:?}"
            ));
            report.records = records;
            return report;
        }
        sim.run_for(SLICE);
    }
    let Some(crashed) = leader_of(&sim.trace(), right) else {
        report.unbuilt = Some(format!("the right half {right} had no leader at the heal"));
        report.records = sim.trace();
        return report;
    };
    report.crashed = Some(crashed);
    // The heal: R's leader crashed, x's links to the third node healed, its links to the
    // crashed node staying cut until the run ends.
    let (at_x, at_crashed) = (nodes[x as usize - 1], nodes[crashed as usize - 1]);
    sim.crash(at_crashed);
    sim.heal();
    sim.block(at_x, at_crashed);
    sim.block(at_crashed, at_x);
    report.healed = Some(sim.now());
    sim.run_for(election_max() * 10);
    sim.restart(at_crashed);
    spawn(&sim, at_crashed, crashed, range_variants);
    sim.run_for(AFTER);
    report.records = sim.trace();
    report
}

fn spawn(sim: &Sim, at: NodeId, id: u64, range_variants: RangeVariants) {
    let env = sim.env(at);
    let inner = env.clone();
    env.spawn("node", async move {
        let _ = ananke_shard::server::run(inner, server_config(id, range_variants)).await;
    });
}

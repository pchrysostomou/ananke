//! The membership scenario (SPEC §3, RAFT.md §1): 3 → 5 → 3 under partition.
//!
//! Five server nodes run from the start so trace node ids line up, but servers 4
//! and 5 begin with empty stores outside the initial configuration {1, 2, 3}: no
//! voters, no log, sitting quiet until a leader's entries reach them. An operator
//! asks for the change to {1, 2, 3, 4, 5}; a partition drawn from the seed lands
//! during the change and puts the leader in force on the minority side of the old
//! voters — either alone with client 1, or keeping servers 4 and 5, the shape
//! where a broken joint-majority rule commits against a disjoint majority. The
//! partition heals, the change completes (the operator asks again if the
//! partition killed it: the request is idempotent for the same voters, D-029),
//! leadership is handed to server 4 or 5 on some seeds, and the operator shrinks
//! back to {1, 2, 3} under another partition; a leader outside `C_new` completing
//! that change steps down.
//!
//! Membership changes and snapshots run on one schedule (issue #46, PROPOSED D-058).
//! The servers run the sweep's snapshot threshold of 12, so leaders take checkpoints
//! and compact routinely, and the operator asks for the grow only of a leader that has
//! compacted since it took office, and of that leader alone, following no hint: the
//! empty servers 4 and 5 are then behind its compacted prefix from the start, and the
//! first leader to catch them up as learners feeds them a snapshot. Every seed is asked
//! to show one in a joiner's learner phase ([`Report::snapshot_fed_joiners`]), and a
//! refusal for anything but lost state fails the run, so a configuration key an
//! install's repair wrote out of step with its log cannot pass as a re-seed.
//!
//! The checks are the sweep's (RAFT.md §2): the log invariants and rule folds,
//! commit majority against the configuration in force, linearizability, and, on
//! uniformly scheduled seeds, liveness after the last heal and the availability
//! criterion of SPEC §3: the longest gap in completed client operations, with the
//! time inside partition windows taken out — the partition itself may block
//! writes while the leader is on the minority side, so the clock for the bound
//! effectively starts at the heal. The bound is chosen so the correct server
//! never trips it (RAFT.md §5); at ten thousand seeds, on the schedule before D-058's
//! snapshots, the worst gap was 549 ms against its 2 s, and SPEC §3 states the
//! criterion as this bound.
//!
//! The pair rule (CLAUDE.md):
//! [`Variant::SingleMajorityInJointConsensus`](ananke_raft::core::Variant::SingleMajorityInJointConsensus)
//! must be
//! caught here on some seeds and the correct server must pass every one.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{RunHeader, Sim, SimConfig, TraceRecord};
use ananke_env::{Clock, Either, Environment, Instant, Network, NodeId, Socket, TraceEvent, race};
use ananke_raft::apply::Command;
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::message;
use ananke_raft::store::LOST_STATE;
use ananke_raft::{NodeConfig, ServerId, invariants, run as run_server};
use ananke_storage::EngineConfig;
use moirae_sched::Policy;

use crate::lin::{self, History};
use crate::raft::{
    self, CLIENTS, ClientStats, DIR, DRIFT_BOUND_PPM, LIVENESS_TIMEOUTS, SLICE, TICK, admin_addr,
    client, election_max, leader_now, server_addr,
};

/// The raft sweep's snapshot threshold, which the membership servers run too, so that
/// leaders compact routinely and a server joining empty is fed a snapshot (D-030).
// PROPOSED(D-058): the membership scenario past the snapshot threshold.
pub const SNAPSHOT_THRESHOLD: u64 = 12;
/// The raft sweep's chunk size: an install takes several chunks.
// PROPOSED(D-058): the membership scenario past the snapshot threshold.
pub const SNAPSHOT_CHUNK: usize = 4096;
/// How long the operator waits, at most, for a leader that has compacted since it took
/// office before it asks for the grow: a fresh leader's first take waits two minimum
/// election timeouts (D-030), and the client writes fill a threshold of 12 in well under
/// a second. [`Schedule::total`] leaves the wait out (D-058).
// PROPOSED(D-058): the membership scenario past the snapshot threshold.
const COMPACTION_WAIT_BUDGET: Duration = Duration::from_millis(2000);

/// How many server nodes run, servers 4 and 5 outside the initial configuration.
pub const SERVERS: u64 = 5;
/// How many servers the initial configuration holds: servers 1 through 3.
pub const INITIAL_VOTERS: u64 = 3;
/// The longest gap in completed client operations, outside the partition
/// windows, that a uniformly scheduled seed may show, in maximum election
/// timeouts (SPEC §3, D-029): chosen so the correct server never trips it.
pub const AVAILABILITY_TIMEOUTS: u32 = 10;
/// The most trace records a membership run may produce before it is stopped as
/// a runaway: five servers over about ten virtual seconds stay well under it.
pub const TRACE_CAP: usize = 600_000;

/// How long the operator's transfer gets before the shrink is asked for.
const TRANSFER_WAIT: Duration = Duration::from_millis(300);
/// The operator's client process in the trace.
const ADMIN: u64 = 98 << 32;
/// How often a change is asked for before the run gives up on it.
const ATTEMPTS: u32 = 4;
/// How many completion polls, [`POLL`] apart, each attempt gets.
const POLL_BUDGET: u32 = 10;
/// How long the driver advances between completion polls.
const POLL: Duration = Duration::from_millis(200);

/// The partition drawn for one change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Phase {
    /// How long after the change request the partition starts.
    pub after: Duration,
    /// How long it lasts.
    pub for_: Duration,
    /// Whether the leader keeps servers 4 and 5 on its side — the shape where a
    /// merged majority is disjoint from the old voters' — or is cut off alone
    /// with client 1.
    pub with_movers: bool,
}

/// The plan of one run, in global virtual time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// All links up, {1, 2, 3} electing and the clients starting.
    pub warmup: Duration,
    /// The partition during the change to five voters.
    pub grow: Phase,
    /// The partition during the change back to three.
    pub shrink: Phase,
    /// The server leadership is handed to between the changes, when drawn: 4 or
    /// 5, so the shrink's leader is outside `C_new` on those seeds and the
    /// step-down is exercised.
    pub transfer_to: Option<u64>,
    /// The quiet after the last change: the liveness window.
    pub settle: Duration,
    /// Each server's clock rate error in parts per million, within the lease's
    /// bound: this scenario is about membership, not the lease.
    pub drifts: Vec<i64>,
    /// Each server's clock offset in nanoseconds.
    pub skews: Vec<i64>,
}

impl Schedule {
    /// A schedule drawn from `seed`.
    #[must_use]
    pub fn draw(seed: u64) -> Self {
        let mut rng = moirae_sched::stream(seed, "membership");
        let ms = |rng: &mut moirae_sched::Pcg32, lo: u64, hi: u64| {
            Duration::from_millis(lo + rng.below(hi - lo + 1))
        };
        let phase = |rng: &mut moirae_sched::Pcg32| Phase {
            after: ms(rng, 20, 400),
            for_: ms(rng, 300, 800),
            with_movers: rng.below(3) < 2,
        };
        let grow = phase(&mut rng);
        let shrink = phase(&mut rng);
        let transfer_to = (rng.below(2) == 0).then(|| 4 + rng.below(2));
        let mut drifts = Vec::new();
        let mut skews = Vec::new();
        for _ in 0..SERVERS {
            let magnitude = i64::try_from(rng.below(DRIFT_BOUND_PPM / 3 + 1)).expect("small");
            drifts.push(if rng.below(2) == 0 {
                magnitude
            } else {
                -magnitude
            });
            let skew = i64::try_from(rng.below(50_000_000)).expect("small");
            skews.push(if rng.below(2) == 0 { skew } else { -skew });
        }
        Self {
            warmup: Duration::from_millis(1000),
            grow,
            shrink,
            transfer_to,
            settle: election_max() * LIVENESS_TIMEOUTS + Duration::from_millis(200),
            drifts,
            skews,
        }
    }

    /// A generous bound on the run's virtual duration, for the run-length hint:
    /// the changes' polls end early when a change completes. The operator's wait for a
    /// compacted leader is not counted: measured, it is a few milliseconds of a run, and
    /// counting its budget would lower PCT's change-point rate on every run for time no
    /// run spends (D-058).
    #[must_use]
    pub fn total(&self) -> Duration {
        let budget = POLL * POLL_BUDGET * ATTEMPTS;
        self.warmup
            + self.grow.after
            + self.grow.for_
            + self.shrink.after
            + self.shrink.for_
            + TRANSFER_WAIT
            + budget * 2
            + self.settle
    }
}

/// The bound the availability criterion uses.
#[must_use]
pub fn availability_bound() -> Duration {
    election_max() * AVAILABILITY_TIMEOUTS
}

/// The simulator configuration for `seed` and `schedule`: the sweep's network
/// and disk knobs (no crashes are scheduled, so the disk model only matters for
/// the running engines).
#[must_use]
pub fn config(seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.05;
    config.net.p_duplicate = 0.05;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(10);
    config.clock.max_skew = Duration::from_millis(50);
    config.clock.max_drift_ppm = 500;
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.02;
    config.fs.latency_min = Duration::from_micros(100);
    config.fs.latency_max = Duration::from_millis(2);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(SERVERS + CLIENTS).expect("small"),
        schedule.total(),
    );
    config
}

/// The server configuration for `id` under `variants`: the address book holds
/// all five servers, and only servers 1 through 3 start with voters.
// D-045: a variant is a set.
#[must_use]
pub fn node_config(id: u64, variants: impl Into<Variants>) -> NodeConfig {
    let variants = variants.into();
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    NodeConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=SERVERS)
            .map(|s| (ServerId(s), server_addr(s)))
            .collect(),
        initial_voters: if id <= INITIAL_VOTERS {
            (1..=INITIAL_VOTERS).map(ServerId).collect()
        } else {
            Vec::new()
        },
        // One entry per message, as the sweep runs (D-026, issue #22).
        // PROPOSED(D-058): the sweep's snapshot threshold and chunk, so membership
        // changes and snapshots run on one schedule (issue #46).
        raft: RaftConfig {
            variants,
            max_batch: 1,
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            drift_bound_ppm: DRIFT_BOUND_PPM,
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            snapshot_chunk: SNAPSHOT_CHUNK,
            ..RaftConfig::default()
        },
        engine,
        inbox_capacity: 128,
    }
}

fn spawn_server(sim: &Sim, id: u64, variants: Variants) {
    let env = sim.env(NodeId::new(u32::try_from(id).expect("small")));
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run_server(inner, node_config(id, variants)).await;
    });
}

type SharedStats = Arc<Mutex<ClientStats>>;

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// Which server ran: the set of known bugs it carried (D-045).
    // D-045: a variant is a set.
    pub variants: Variants,
    /// How the run was scheduled (D-016).
    pub policy: Policy,
    /// The plan it ran.
    pub schedule: Schedule,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// What the moirae export needs besides [`Report::records`]:
    /// [`Report::jsonl`] writes the trace from the two when it is asked for.
    // PROPOSED(D-052): a scenario's moirae JSONL is written when it is asked for.
    pub run: RunHeader,
    /// The partitions made, as (from, until).
    pub partitions: Vec<(Instant, Instant)>,
    /// When the last partition healed.
    pub last_heal: Instant,
    /// Whether {1, 2, 3, 4, 5} took effect, non-joint, on a majority of it.
    pub grow_completed: bool,
    /// Whether {1, 2, 3} took effect again the same way, after the grow.
    pub shrink_completed: bool,
    /// When the operator first asked for the grow, and when the driver stopped driving
    /// it, completed or not.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    pub grow: Option<(Instant, Instant)>,
    /// How many times the operator found no leader that had compacted since it took
    /// office within the two-second budget and asked the leader in force instead.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    pub compaction_fallbacks: u32,
    /// How long the operator waited, in all, for a compacted leader.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    pub compaction_waited: Duration,
    /// Why the run stopped early, if it did.
    pub stopped: Option<String>,
    /// The clients' history.
    pub history: History,
    /// The clients' counts.
    pub clients: ClientStats,
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
            .expect("the membership trace exports to moirae v2")
    }

    /// The events, without their times.
    #[must_use]
    pub fn events(&self) -> Vec<TraceEvent> {
        self.records.iter().map(|r| r.event.clone()).collect()
    }

    /// How many records satisfy `f`.
    pub fn count(&self, f: impl Fn(&TraceEvent) -> bool) -> usize {
        self.records.iter().filter(|r| f(&r.event)).count()
    }

    /// Whether the run was scheduled uniformly, so liveness can be asked of it.
    #[must_use]
    pub fn uniform(&self) -> bool {
        self.policy == Policy::Uniform
    }

    /// Every snapshot a server joining the configuration installed while it was a
    /// learner, as (server, when the install completed, the snapshot's last index): what
    /// issue #46 asks every seed to show, a learner fed by a snapshot during a change.
    /// A joiner's learner phase runs from the operator's first request for the grow
    /// ([`Report::grow`]) until the first joint configuration naming it in `new` takes
    /// effect on any server — the entry that ends the catch-up (D-029, D-032) — or, when
    /// none ever does, until the driver stopped driving the grow. Installs after that,
    /// by a voter of the joint or new configuration, in the transfer's wait, the shrink or
    /// the settle, are not counted; nor is an install's restatement at the adoption that
    /// follows it, which is the same snapshot.
    ///
    /// Both bounds stated above — *which* servers may be counted and *when* — are held
    /// by the unit tests of `snapshot_fed_joiners_of`, the fold this delegates to, since
    /// every reader of this list only asks whether it is empty and so cannot see either
    /// bound widen.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    #[must_use]
    pub fn snapshot_fed_joiners(&self) -> Vec<(u64, Instant, u64)> {
        snapshot_fed_joiners_of(&self.records, self.grow)
    }

    /// How long after the last heal the first client write completed, if one did.
    #[must_use]
    pub fn time_to_write_after_heal(&self) -> Option<Duration> {
        self.history
            .ops
            .iter()
            .filter(|op| op.op.is_write() && op.call >= self.last_heal)
            .filter_map(|op| op.ret)
            .map(|ret| ret.duration_since(self.last_heal))
            .min()
    }

    /// The longest gap between consecutive completed client operations with the
    /// time spent inside partition windows taken out: SPEC §3's availability
    /// criterion, with the clock starting at the heal when a partition blocked
    /// the cluster (D-029). None with fewer than two completions.
    #[must_use]
    pub fn longest_completion_gap(&self) -> Option<Duration> {
        let mut returns: Vec<Instant> = self.history.ops.iter().filter_map(|op| op.ret).collect();
        returns.sort_unstable();
        if returns.len() < 2 {
            return None;
        }
        let mut worst = Duration::ZERO;
        for pair in returns.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let mut gap = b.duration_since(a);
            for &(from, until) in &self.partitions {
                let lo = a.max(from);
                let hi = b.min(until);
                if hi > lo {
                    gap = gap.saturating_sub(hi.duration_since(lo));
                }
            }
            worst = worst.max(gap);
        }
        Some(worst)
    }

    /// Every property the run must satisfy, or the first violation.
    ///
    /// # Errors
    ///
    /// A message naming the seed and the violation.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        let fail = |what: String| Err(format!("seed {seed}: {what}"));
        if let Some(why) = &self.stopped {
            return fail(why.clone());
        }
        if let Err(violation) = invariants::all(crate::traced(&self.records)) {
            return fail(violation);
        }
        if let Err(violation) =
            invariants::commit_majority(crate::traced(&self.records), INITIAL_VOTERS as usize)
        {
            return fail(violation);
        }
        if let Err(violation) = lin::check(&self.history) {
            return fail(violation.to_string());
        }
        if let Some(failed) = self.records.iter().find_map(|r| match &r.event {
            TraceEvent::RaftServerFailed { server, reason } => Some((server, reason)),
            _ => None,
        }) {
            return fail(format!("server {} failed: {}", failed.0, failed.1));
        }
        // PROPOSED(D-069): the payload of SHARD.md §8's trace, and the rule
        // `RaftMatchStarted` states, folded here as in the raft sweep — this is the
        // scenario that drives changes, so it is where learners and re-tracked
        // followers raise a match.
        if let Err(violation) = raft::payload_is_well_formed(&self.records) {
            return fail(violation);
        }
        if let Err(violation) = raft::match_starts_are_first_rises(&self.records) {
            return fail(violation);
        }
        // PROPOSED(D-058): no crash is scheduled here, so a store refused at an open is
        // an install's adoption gone wrong — a configuration key its repair wrote out
        // of step with the log refuses the store and puts the server in re-seed mode,
        // which is not a failure — unless the refusal is for state lost below the
        // store, which fails nothing of the protocol's.
        if let Some((server, reason)) = self.records.iter().find_map(|r| match &r.event {
            TraceEvent::RaftRefused { server, reason } if !reason.starts_with(LOST_STATE) => {
                Some((server, reason))
            }
            _ => None,
        }) {
            return fail(format!("server {server} refused its store: {reason}"));
        }
        // Completion and availability are liveness: asked only of seeds the
        // scheduler cannot starve (D-016).
        if self.uniform() {
            if !self.grow_completed {
                return fail("the change to {1, 2, 3, 4, 5} never completed".to_owned());
            }
            if !self.shrink_completed {
                return fail("the change back to {1, 2, 3} never completed".to_owned());
            }
            let bound = election_max() * LIVENESS_TIMEOUTS;
            match self.time_to_write_after_heal() {
                Some(took) if took <= bound => {}
                Some(took) => {
                    return fail(format!(
                        "liveness: the first client write after the last heal took {took:?}, over {bound:?}"
                    ));
                }
                None => {
                    return fail(format!(
                        "liveness: no client write completed after the last heal at {:?}",
                        self.last_heal
                    ));
                }
            }
            if let Some(gap) = self.longest_completion_gap()
                && gap > availability_bound()
            {
                return fail(format!(
                    "availability: {gap:?} without a completed operation outside the partitions, over {:?}",
                    availability_bound()
                ));
            }
        }
        Ok(())
    }
}

/// [`Report::snapshot_fed_joiners`] over a trace's records and the window the grow was
/// driven in, so the predicate can be asked of records built by hand: every snapshot a
/// server *joining* the configuration — one above [`INITIAL_VOTERS`], never an original
/// voter — installed *in its learner phase*, which runs from `from` until the first
/// joint configuration naming that server in `new` takes effect on any server, or, when
/// none ever does, until `grow_end`.
///
/// The sweep and the coverage read the list only for emptiness, so neither bound can be
/// seen to widen there; the mutation pass showed both widening unnoticed at every tier
/// (the count 80 → 93 with the learner phase dropped, 80 → 92 with the initial voters
/// counted as joiners). The tests below hold them.
// PROPOSED(D-058): the membership scenario past the snapshot threshold.
fn snapshot_fed_joiners_of(
    records: &[TraceRecord],
    grow: Option<(Instant, Instant)>,
) -> Vec<(u64, Instant, u64)> {
    let Some((from, grow_end)) = grow else {
        return Vec::new();
    };
    let learner_until = |joiner: u64| {
        records
            .iter()
            .find_map(|r| match &r.event {
                TraceEvent::RaftConfig {
                    joint: true, new, ..
                } if r.at >= from && new.contains(&joiner) => Some(r.at),
                _ => None,
            })
            .unwrap_or(grow_end)
    };
    let until: Vec<(u64, Instant)> = (INITIAL_VOTERS + 1..=SERVERS)
        .map(|joiner| (joiner, learner_until(joiner)))
        .collect();
    let mut joiners: Vec<(u64, Instant, u64)> = Vec::new();
    for record in records {
        if let TraceEvent::RaftSnapshot {
            server,
            last_index,
            taken: false,
            ..
        } = record.event
            && until
                .iter()
                .any(|&(joiner, end)| joiner == server && record.at >= from && record.at < end)
            && !joiners
                .iter()
                .any(|&(s, _, index)| s == server && index == last_index)
        {
            joiners.push((server, record.at, last_index));
        }
    }
    joiners
}

/// What the sliced advance watches for, as the sweep's does.
struct Watch {
    slices: u32,
    stopped: Option<String>,
    /// The safety checks, one checker for the whole run with the state of each
    /// check kept across looks, as the sweep's advance does (D-046).
    checker: invariants::Checker,
    /// How many trace records the checker has been fed.
    checked: usize,
}

/// What the driver has read of leadership from the trace: the latest leader, and
/// whether it has compacted its log since it took office.
// PROPOSED(D-058): the membership scenario past the snapshot threshold.
#[derive(Default)]
struct Leadership {
    scanned: usize,
    leader: Option<u64>,
    compacted: bool,
    /// Requests that found no compacted leader in time.
    fallbacks: u32,
    /// Time spent waiting for one.
    waited: Duration,
}

impl Default for Watch {
    fn default() -> Self {
        Self {
            slices: 0,
            stopped: None,
            checker: invariants::Checker::new(INITIAL_VOTERS as usize),
            checked: 0,
        }
    }
}

/// The run under way: the simulator and what the driver tracks about it.
struct Driver {
    sim: Sim,
    watch: Watch,
    servers: Vec<NodeId>,
    clients: Vec<NodeId>,
    admin: NodeId,
    partitions: Vec<(Instant, Instant)>,
    last_heal: Instant,
    admin_seq: u64,
    leadership: Leadership,
}

impl Driver {
    /// The leader in force, if it has compacted its log since it took office: the
    /// leader the operator asks for the grow, whose learners start behind its
    /// compacted prefix.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    fn compacted_leader(&mut self) -> Option<u64> {
        let records = self.sim.trace_from(self.leadership.scanned);
        self.leadership.scanned += records.len();
        for record in &records {
            match record.event {
                TraceEvent::RaftLeader { server, .. } => {
                    self.leadership.leader = Some(server);
                    self.leadership.compacted = false;
                }
                TraceEvent::RaftCompacted { server, .. }
                    if self.leadership.leader == Some(server) =>
                {
                    self.leadership.compacted = true;
                }
                _ => {}
            }
        }
        self.leadership.leader.filter(|_| self.leadership.compacted)
    }

    /// Waits, in slices and at most [`COMPACTION_WAIT_BUDGET`], for a leader that has
    /// compacted since it took office, and returns it; when none appears in time, the
    /// leader in force, so that the seed's missing snapshot is reported rather than
    /// hidden. Every fallback and every slice waited is counted in the report.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    fn await_compacted_leader(&mut self) -> u64 {
        let mut waited = Duration::ZERO;
        loop {
            if let Some(leader) = self.compacted_leader() {
                return leader;
            }
            if waited >= COMPACTION_WAIT_BUDGET || self.watch.stopped.is_some() {
                self.leadership.fallbacks += 1;
                return leader_now(&self.sim);
            }
            self.advance(SLICE);
            waited += SLICE;
            self.leadership.waited += SLICE;
        }
    }

    /// Advances in slices with the safety folds run over the trace so far, the
    /// way the sweep does, so a violating variant stops with a verdict.
    fn advance(&mut self, duration: Duration) {
        if self.watch.stopped.is_some() {
            return;
        }
        let mut left = duration;
        while left > Duration::ZERO {
            let step = left.min(SLICE);
            self.sim.run_for(step);
            left -= step;
            self.watch.slices += 1;
            let len = self.sim.trace_len();
            if len > TRACE_CAP {
                self.watch.stopped = Some(format!(
                    "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                    self.sim.now()
                ));
                return;
            }
            if self.watch.slices.is_multiple_of(crate::raft::CHECK_EVERY) {
                let records = self.sim.trace_from(self.watch.checked);
                self.watch.checked += records.len();
                self.watch.checker.extend(crate::traced(&records));
                if let Err(violation) = self.watch.checker.verdict() {
                    self.watch.stopped = Some(format!("{violation} (at {:?})", self.sim.now()));
                    return;
                }
            }
        }
    }

    /// Asks the leader in force for a change to `voters`, from a fresh operator
    /// socket, following NotLeader hints and retrying until a server accepts —
    /// asking again for the same voters is idempotent (D-029). Given `only`, asks that
    /// server alone, retrying it and following no hint, so that no other leader starts
    /// the change on this request.
    fn request_change(&mut self, voters: &[u64], only: Option<u64>) {
        self.admin_seq += 1;
        let seq = self.admin_seq;
        let first = only.unwrap_or_else(|| leader_now(&self.sim));
        let env = self.sim.env(self.admin);
        let inner = env.clone();
        let voters = voters.to_vec();
        env.spawn("admin", async move {
            let Ok(sock) = inner.net().bind(admin_addr(seq)).await else {
                return;
            };
            let mut target = first;
            for _ in 0..12 {
                let request = Request {
                    client: ADMIN,
                    seq,
                    command: Command::Change {
                        voters: voters.clone(),
                    },
                };
                if sock
                    .send(server_addr(target), request.encode())
                    .await
                    .is_err()
                {
                    return;
                }
                let deadline = inner.clock().now() + Duration::from_millis(150);
                let mut reply = None;
                loop {
                    let recv = pin!(sock.recv());
                    let timer = pin!(inner.clock().sleep_until(deadline));
                    match race(&inner, recv, timer).await {
                        Either::Left(Ok((_, bytes))) => {
                            if let Ok(response) = Response::decode(bytes)
                                && response.client == ADMIN
                                && response.seq == seq
                            {
                                reply = Some(response.reply);
                                break;
                            }
                        }
                        Either::Left(Err(_)) => return,
                        Either::Right(()) => break,
                    }
                }
                match reply {
                    Some(Reply::Outcome(_)) => return,
                    // PROPOSED(D-058): a request for one server alone ends where that
                    // server is not the leader; the driver asks again.
                    Some(Reply::NotLeader { .. }) if only.is_some() => return,
                    Some(Reply::NotLeader { leader: Some(l) }) => target = l.0,
                    None if only.is_some() => {
                        inner.clock().sleep(Duration::from_millis(25)).await;
                    }
                    Some(Reply::NotLeader { leader: None }) | None => {
                        inner.clock().sleep(Duration::from_millis(25)).await;
                        target = target % SERVERS + 1;
                    }
                }
            }
        });
    }

    /// Hands leadership to `to`, one shot, the way the sweep's lease trial does.
    fn transfer(&mut self, to: u64) {
        self.admin_seq += 1;
        let seq = self.admin_seq;
        let leader = leader_now(&self.sim);
        let env = self.sim.env(self.admin);
        let inner = env.clone();
        env.spawn("admin", async move {
            let Ok(sock) = inner.net().bind(admin_addr(seq)).await else {
                return;
            };
            let request = Request {
                client: ADMIN,
                seq,
                command: Command::Transfer { to },
            };
            let _ = sock.send(server_addr(leader), request.encode()).await;
        });
    }

    /// Whether `voters` has taken effect, non-joint, on a majority of itself.
    fn change_complete(&self, voters: &[u64]) -> bool {
        let mut in_force: BTreeSet<u64> = BTreeSet::new();
        for record in self.sim.trace() {
            if let TraceEvent::RaftConfig {
                server,
                index,
                old,
                joint: false,
                ..
            } = &record.event
                && *index > 0
                && old.as_slice() == voters
            {
                in_force.insert(*server);
            }
        }
        in_force.len() * 2 > voters.len()
    }

    /// Drives one change to completion: the request, the partition drawn for it
    /// with the leader on the minority side, the heal, and up to [`ATTEMPTS`]
    /// fresh requests should the partition have killed the change (a leadership
    /// change abandons the catch-up phase, D-032). When `snapshot_fed`, every request
    /// goes to a leader that has compacted since it took office, and to it alone.
    fn drive_change(&mut self, voters: &[u64], phase: &Phase, snapshot_fed: bool) -> bool {
        for attempt in 0..ATTEMPTS {
            if self.watch.stopped.is_some() {
                return false;
            }
            // PROPOSED(D-058): the grow is asked of a compacted leader alone.
            let only = snapshot_fed.then(|| self.await_compacted_leader());
            if self.watch.stopped.is_some() {
                return false;
            }
            self.request_change(voters, only);
            if attempt == 0 {
                self.advance(phase.after);
                if self.watch.stopped.is_some() {
                    return false;
                }
                let leader = leader_now(&self.sim);
                let mut side_servers: BTreeSet<u64> = BTreeSet::new();
                side_servers.insert(leader);
                if phase.with_movers {
                    side_servers.insert(4);
                    side_servers.insert(5);
                }
                let side: Vec<NodeId> = side_servers
                    .iter()
                    .map(|&s| self.servers[s as usize - 1])
                    .chain(std::iter::once(self.clients[0]))
                    .collect();
                let rest: Vec<NodeId> = self
                    .servers
                    .iter()
                    .chain(self.clients.iter())
                    .chain(std::iter::once(&self.admin))
                    .copied()
                    .filter(|n| !side.contains(n))
                    .collect();
                let from = self.sim.now();
                self.sim.partition(&side, &rest);
                self.advance(phase.for_);
                self.sim.heal();
                self.partitions.push((from, self.sim.now()));
                self.last_heal = self.sim.now();
            }
            for _ in 0..POLL_BUDGET {
                if self.watch.stopped.is_some() {
                    return false;
                }
                self.advance(POLL);
                if self.change_complete(voters) {
                    return true;
                }
            }
        }
        self.change_complete(voters)
    }
}

/// Runs the scenario for `seed` with the schedule drawn from it, under the set
/// of bugs `variants` (D-045).
// D-045: a variant is a set.
#[must_use]
pub fn run(seed: u64, variants: impl Into<Variants>) -> Report {
    run_with(seed, Schedule::draw(seed), variants)
}

/// Runs the scenario for `seed` with an explicit schedule.
// D-045: a variant is a set.
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule, variants: impl Into<Variants>) -> Report {
    let variants = variants.into();
    let mut sim = Sim::new(config(seed, &schedule));
    let servers: Vec<NodeId> = (0..SERVERS as usize)
        .map(|i| sim.add_node_with_clock(schedule.skews[i], schedule.drifts[i]))
        .collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    let stats: Vec<SharedStats> = (0..CLIENTS).map(|_| SharedStats::default()).collect();
    for id in 1..=SERVERS {
        spawn_server(&sim, id, variants);
    }
    for (i, &node) in clients.iter().enumerate() {
        let env = sim.env(node);
        let inner = env.clone();
        let stats = stats[i].clone();
        env.spawn("client", client(inner, i as u64 + 1, SERVERS, stats));
    }
    let last_heal = sim.now();
    let mut driver = Driver {
        sim,
        watch: Watch::default(),
        servers,
        clients,
        admin,
        partitions: Vec::new(),
        last_heal,
        admin_seq: 0,
        leadership: Leadership::default(),
    };
    driver.advance(schedule.warmup);
    let grow_requested = driver.sim.now();
    let grow_completed = driver.drive_change(&[1, 2, 3, 4, 5], &schedule.grow, true);
    let grow = Some((grow_requested, driver.sim.now()));
    if let Some(to) = schedule.transfer_to
        && grow_completed
        && driver.watch.stopped.is_none()
    {
        driver.transfer(to);
        driver.advance(TRANSFER_WAIT);
    }
    let shrink_completed = driver.drive_change(&[1, 2, 3], &schedule.shrink, false);
    if driver.watch.stopped.is_none() {
        driver.advance(schedule.settle);
    }
    let records = driver.sim.trace();
    let history = History::from_trace(&records);
    let mut clients_total = ClientStats::default();
    for s in &stats {
        let s = s.lock().unwrap();
        clients_total.completed += s.completed;
        clients_total.abandoned += s.abandoned;
        clients_total.redirected += s.redirected;
    }
    Report {
        seed,
        variants,
        policy: driver.sim.policy(),
        schedule,
        run: driver.sim.run_header(),
        records,
        partitions: driver.partitions,
        last_heal: driver.last_heal,
        grow_completed,
        shrink_completed,
        grow,
        compaction_fallbacks: driver.leadership.fallbacks,
        compaction_waited: driver.leadership.waited,
        stopped: driver.watch.stopped,
        history,
        clients: clients_total,
    }
}

#[cfg(test)]
mod tests {
    //! The fold and the check that hold what the scenario claims, on records written
    //! by hand and on one seed's own run. The sweep and the coverage read
    //! [`Report::snapshot_fed_joiners`] only for emptiness and have never seen a
    //! refusal at any tier, so neither the fold's bounds nor the refusal clause is
    //! held by a sweep; both are held here.

    use ananke_raft::core::Variant;
    use ananke_raft::node::SINGLE_GROUP;

    use super::*;

    fn at(millis: u64) -> Instant {
        Instant::from_nanos(millis * 1_000_000)
    }

    fn record(millis: u64, event: TraceEvent) -> TraceRecord {
        TraceRecord {
            at: at(millis),
            decided: at(millis),
            node: None,
            event,
        }
    }

    /// An install of `last_index` by `server`, as a joining server's feed traces.
    fn installed(millis: u64, server: u64, last_index: u64) -> TraceRecord {
        record(
            millis,
            TraceEvent::RaftSnapshot {
                server,
                range: SINGLE_GROUP,
                last_index,
                last_term: 1,
                taken: false,
            },
        )
    }

    /// The joint configuration that admits `joiner`, which ends its learner phase.
    fn joint_admitting(millis: u64, joiner: u64) -> TraceRecord {
        record(
            millis,
            TraceEvent::RaftConfig {
                server: 1,
                range: SINGLE_GROUP,
                index: 10,
                old: (1..=INITIAL_VOTERS).collect(),
                new: (1..=INITIAL_VOTERS).chain([joiner]).collect(),
                joint: true,
                learners: Vec::new(),
            },
        )
    }

    /// Issue #46 asks for a *learner* fed by a snapshot during the change, and both
    /// bounds of that are the fold's alone: the window (until the joint configuration
    /// naming the joiner takes effect) and the set (a server above `INITIAL_VOTERS`,
    /// never one of the original voters catching up behind a compacted leader). The
    /// mutation pass widened each in turn — the counted feeds went 80 → 93 over the
    /// gate's twenty seeds with the window dropped and 80 → 92 with the set widened —
    /// and every tier stayed green, because the sweep and the coverage ask only whether
    /// the list is empty. Here a trace holds one install of each kind.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    #[test]
    fn only_a_joiners_install_in_its_learner_phase_is_counted() {
        let grow = Some((at(100), at(900)));
        let records = vec![
            // Before the grow was asked for: not a feed of the change.
            installed(50, 4, 1),
            // An original voter catching up behind a compacted leader, inside the
            // window: never a joiner, whatever else it is.
            installed(200, 2, 2),
            // The joiner's own feed, in its learner phase: the one #46 asks for.
            installed(300, 4, 3),
            // The joiner is admitted to the configuration; its learner phase ends.
            joint_admitting(400, 4),
            // A voter of the joint configuration installing after that: not a
            // learner-phase feed.
            installed(500, 4, 4),
            // Another server's feed, whose own phase has not ended, is counted.
            installed(600, 5, 5),
            // A take is not a feed.
            record(
                650,
                TraceEvent::RaftSnapshot {
                    server: 5,
                    range: SINGLE_GROUP,
                    last_index: 6,
                    last_term: 1,
                    taken: true,
                },
            ),
            // The same install restated at the adoption that follows it.
            installed(700, 5, 5),
            // After the driver stopped driving the grow.
            installed(950, 5, 7),
        ];
        assert_eq!(
            snapshot_fed_joiners_of(&records, grow),
            vec![(4, at(300), 3), (5, at(600), 5)]
        );
        assert!(
            snapshot_fed_joiners_of(&records, None).is_empty(),
            "a run whose grow was never asked for feeds no joiner"
        );
    }

    /// A store refused for anything but lost state fails the run (D-058): no crash is
    /// scheduled in this scenario, so such a refusal is an install's adoption gone
    /// wrong — a configuration key its repair wrote out of step with the log — and must
    /// not pass as a re-seed. No run has ever produced one (the coverage's `refusals` is
    /// empty at 20, 100 and 1 000 seeds), so the clause discriminates nowhere in the
    /// sweep: flipping its negation left every tier green. It is given a case here, on a
    /// seed's own passing run with one refusal record appended, both ways round.
    // PROPOSED(D-058): the membership scenario past the snapshot threshold.
    #[test]
    fn a_refusal_that_is_not_for_lost_state_fails_the_run() {
        let mut report = run(0, Variant::Correct);
        assert_eq!(
            report.check().err(),
            None,
            "seed 0 under the correct server passes as it runs"
        );
        let ran = report.records.clone();
        let end = ran.last().expect("a trace").at;
        let mut refused = |reason: &str| {
            report.records.clone_from(&ran);
            report.records.push(TraceRecord {
                at: end,
                decided: end,
                node: None,
                event: TraceEvent::RaftRefused {
                    server: 2,
                    reason: reason.to_owned(),
                },
            });
            report.check().err()
        };
        assert_eq!(
            refused(&format!("{LOST_STATE}: tables 29 and 31")),
            None,
            "a refusal for state lost below the store is a re-seed, not a failure"
        );
        assert_eq!(
            refused("the configuration key is out of step with the log"),
            Some(
                "seed 0: server 2 refused its store: the configuration key is out of step with \
                 the log"
                    .to_owned()
            ),
        );
    }
}

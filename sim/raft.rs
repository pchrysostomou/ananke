//! The Phase 2 sweep (SPEC.md §3, RAFT.md §2, §5): three servers running
//! `ananke_raft::node` and two clients, under drops, duplicates, reordering, clock
//! skew and drift, partitions, one-way blocks and crashes with the §1.3 disk model.
//! [`run`] draws a fault schedule from the seed, drives it, and returns a [`Report`]
//! whose [`check`](Report::check) states what the run must satisfy:
//!
//! - the four log invariants of Figure 3 and the three rule folds, from
//!   `ananke_raft::invariants`, over the trace;
//! - linearizability of the clients' history, by [`crate::lin`], which is what
//!   decides whether a read served by a lease was stale (RAFT.md §2, invariant 6);
//! - pre-vote's property: a server cut off from the others does not raise its term;
//! - on seeds scheduled uniformly, where a task cannot be starved: after the last
//!   fault heals a client write completes within [`LIVENESS_TIMEOUTS`] election
//!   timeouts; and a follower that hears from no current leader and grants no vote
//!   for [`TIMER_TIMEOUTS`] timeouts campaigns, the rule a server that resets its
//!   timer on any message breaks.
//!
//! Every server's clock is drawn per seed (RAFT.md §3): on half the seeds every
//! rate error is within a third of [`DRIFT_BOUND_PPM`], so the lease's assumption
//! holds. On the other half one server runs slow and two run fast, so the
//! assumption fails between every pair, and the guard must revoke or the checker
//! must see a stale read: on half of those the magnitudes are moderate, two
//! thousand to a hundred thousand parts per million, where the guard has to notice
//! movement the lease's margin still absorbs; on the rest they are severe, the slow
//! server at a quarter to two fifths of true rate and the fast ones fifteen to
//! thirty-five percent fast, where a lease the slow server measures by its own
//! clock outlives the fast followers' timers and a read it serves is stale. No real
//! clock does that; with a hundred-millisecond election timeout and a tick of
//! margin it is what the arithmetic needs, and the arithmetic is the same at any
//! scale. [`Report::drift_exceeded`] says which a seed was.
//!
//! A slow clock never leads on its own: its timer fires late in real time and the
//! fast servers win every election. So every schedule opens with a lease trial
//! ([`Trial`]): an operator asks the leader to hand over to the server with the
//! slowest clock (leadership transfer, thesis §3.10), its lease forms, and it is
//! then cut off with a reading client while the others elect and write. The
//! adversary chose the clocks and chooses the operator's request; correlating them
//! is its privilege. Under the guard the slow leader trusts no promise whose offset
//! moved and serves by heartbeat round; without it
//! (`Variant::LeaseTrustsTheClock`) it serves a stale read.
//!
//! Every fault-model test runs a known-buggy variant beside the correct one
//! (CLAUDE.md): each [`Variant`] of RAFT.md §5 that this stage ships must be caught
//! by one of these checks on some seed, and the correct server must pass every seed.
//!
//! The disk honours `fsync` here (`p_durable = 1`): a disk that acknowledges a sync
//! it did not do loses persistent state, and Raft's safety argument assumes it is
//! persistent (D-026). Bit rot and torn writes stay on, and the engine's checksums
//! turn them into a refusal (`RaftRefused`) rather than a hole.
//!
//! Clients keep the history honest (RAFT.md §4, `ananke_raft::client`): a write
//! whose answer never comes is not resent, since the entry may yet commit; it is
//! abandoned as pending and the client continues as a new process. A get may be
//! retried, since a second read changes nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{
    ClientOp, ClientResult, Clock, Either, Environment, Instant, Network, NodeId, Rng, Socket,
    TraceEvent, race,
};
use ananke_raft::apply::{Command, Outcome};
use ananke_raft::client::{Reply, Request, Response};
use ananke_raft::core::{RaftConfig, Variant};
use ananke_raft::message::{self, Frame, Message};
use ananke_raft::{NodeConfig, ServerId, invariants, run as run_server};
use ananke_storage::EngineConfig;
use bytes::Bytes;
use moirae_sched::Policy;

use crate::lin::{self, History};

/// How many servers.
pub const SERVERS: u64 = 3;
/// How many clients, each on its own node.
pub const CLIENTS: u64 = 2;
/// How many keys the clients touch: few, so that reads and writes meet.
pub const KEYS: u64 = 2;
/// The minimum election timeout; the maximum is twice it (RAFT.md §1).
pub const ELECTION_MIN: Duration = Duration::from_millis(100);
/// One tick of the core: `ELECTION_MIN` over the core's minimum election ticks.
pub const TICK: Duration = Duration::from_millis(10);
/// How long a client waits for a get before abandoning it, across its tries.
pub const OP_TIMEOUT: Duration = Duration::from_millis(250);
/// How long a client waits on one server before trying another, for a get.
pub const TRY_TIMEOUT: Duration = Duration::from_millis(40);
/// How long a client waits for a write, which gets one try: a second copy would
/// be a second write, and a client that waits longer on a leader that has been
/// cut off writes nothing while the lease it should be testing is live.
pub const WRITE_TIMEOUT: Duration = Duration::from_millis(60);
/// The pause between a client's operations.
pub const OP_GAP: Duration = Duration::from_millis(5);
/// After the last heal, a client write must complete within this many maximum
/// election timeouts.
pub const LIVENESS_TIMEOUTS: u32 = 10;
/// A follower that hears from no current leader and grants no vote for this many
/// maximum election timeouts must have started an election.
pub const TIMER_TIMEOUTS: u32 = 2;
/// The bound on the rate at which two servers' clocks may drift apart, in parts per
/// million, that the lease assumes (RAFT.md §1).
pub const DRIFT_BOUND_PPM: u64 = 1_000;
/// Where each server keeps its store.
pub const DIR: &str = "/raft";
/// The slice of virtual time the run advances between looks at the trace.
pub const SLICE: Duration = Duration::from_millis(50);
/// How often, in slices, the safety folds run over the trace so far.
pub const CHECK_EVERY: u32 = 10;
/// The most trace records a run may produce before it is stopped as a runaway:
/// the correct server produces a few tens of thousands.
pub const TRACE_CAP: usize = 400_000;

/// The maximum election timeout.
#[must_use]
pub fn election_max() -> Duration {
    ELECTION_MIN * 2
}

/// The address of server `id` (1-based).
#[must_use]
pub fn server_addr(id: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, u8::try_from(id).expect("small")], 7000))
}

/// The address of client `n` (1-based).
#[must_use]
pub fn client_addr(n: u64) -> SocketAddr {
    SocketAddr::from(([10, 0, 1, u8::try_from(n).expect("small")], 7000))
}

/// The operator's address.
#[must_use]
pub fn admin_addr() -> SocketAddr {
    SocketAddr::from(([10, 0, 2, 1], 7000))
}

/// The server bound to `addr`, if it is a server's.
#[must_use]
pub fn server_of(addr: SocketAddr) -> Option<u64> {
    (1..=SERVERS).find(|&id| server_addr(id) == addr)
}

/// A node id (1-based, like the trace) for server `id`: servers are added first.
fn node_of_server(id: u64) -> NodeId {
    NodeId::new(u32::try_from(id).expect("small"))
}

/// One fault of a schedule. Every fault heals before the next starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// A symmetric partition: `server` and client `client` on one side, everyone
    /// else on the other.
    Isolate {
        /// The server cut off.
        server: u64,
        /// The client on its side (1-based).
        client: u64,
        /// How long.
        for_: Duration,
    },
    /// The leader in force isolated with client 1, the others and client 2 on the
    /// other side.
    IsolateLeader {
        /// How long.
        for_: Duration,
    },
    /// One direction of one link between servers blocked.
    OneWay {
        /// Messages from this server...
        from: u64,
        /// ...to this one are dropped.
        to: u64,
        /// How long.
        for_: Duration,
    },
    /// A server crashes and restarts after `down`.
    Crash {
        /// Which.
        server: u64,
        /// How long it stays down.
        down: Duration,
    },
    /// The leader in force crashes and restarts after `down`.
    CrashLeader {
        /// How long it stays down.
        down: Duration,
    },
    /// The rule-5 scenario (RAFT.md §5): the links towards `server`, a follower,
    /// are blocked for `one_way` while its own messages still arrive, so it hears
    /// nothing, times out, and pre-votes into the majority every timeout, each
    /// request declined; `crash_after` into that, the leader crashes for `down`. A
    /// follower that resets its timer on the declined requests does not campaign
    /// while the leader is down. Under check quorum a deposed leader stops its
    /// stale heartbeats within an election timeout, so this is the stale sender
    /// that lasts (D-028).
    StaleSender {
        /// The follower cut off from receiving; the leader's neighbour if it leads.
        server: u64,
        /// How long the links towards it stay blocked.
        one_way: Duration,
        /// When the leader crashes, into the block.
        crash_after: Duration,
        /// How long it stays down.
        down: Duration,
    },
}

/// The lease trial that opens every schedule; see the module documentation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trial {
    /// How long the slowest leads before it is cut off: time for its lease.
    pub settle: Duration,
    /// How long it is cut off with client 1.
    pub isolate: Duration,
}

/// How long the trial gives the transfer before it looks for the new leader.
const TRANSFER_WAIT: Duration = Duration::from_millis(300);
/// The operator's client process in the trace.
const ADMIN: u64 = 99 << 32;

/// The fault schedule of one run, in global virtual time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// All links up, servers electing and clients starting.
    pub warmup: Duration,
    /// The lease trial, right after the warmup.
    pub trial: Trial,
    /// The faults, each healed before the next, with `gaps[i]` of quiet after it.
    pub faults: Vec<Fault>,
    /// The quiet after each fault.
    pub gaps: Vec<Duration>,
    /// The quiet after the last fault: the liveness window.
    pub settle: Duration,
    /// Each server's clock rate error in parts per million, by server index.
    pub drifts: Vec<i64>,
    /// Each server's clock offset in nanoseconds, by server index.
    pub skews: Vec<i64>,
}

impl Schedule {
    /// A schedule drawn from `seed`: three to six faults with random kinds,
    /// targets and durations, each followed by a quiet of at least two maximum
    /// election timeouts, so a check after a heal sees the heal's effect alone.
    #[must_use]
    pub fn draw(seed: u64) -> Self {
        let mut rng = moirae_sched::stream(seed, "schedule");
        let ms = |rng: &mut moirae_sched::Pcg32, lo: u64, hi: u64| {
            Duration::from_millis(lo + rng.below(hi - lo + 1))
        };
        let count = 3 + rng.below(4);
        let mut faults = Vec::new();
        let mut gaps = Vec::new();
        for _ in 0..count {
            let fault = match rng.below(6) {
                0 => Fault::Isolate {
                    server: 1 + rng.below(SERVERS),
                    client: 1 + rng.below(CLIENTS),
                    for_: ms(&mut rng, 300, 900),
                },
                1 => Fault::IsolateLeader {
                    for_: ms(&mut rng, 300, 900),
                },
                2 => {
                    let from = 1 + rng.below(SERVERS);
                    let to = 1 + (from - 1 + 1 + rng.below(SERVERS - 1)) % SERVERS;
                    Fault::OneWay {
                        from,
                        to,
                        for_: ms(&mut rng, 300, 900),
                    }
                }
                3 => Fault::Crash {
                    server: 1 + rng.below(SERVERS),
                    down: ms(&mut rng, 50, 400),
                },
                4 => Fault::CrashLeader {
                    down: ms(&mut rng, 50, 400),
                },
                _ => Fault::StaleSender {
                    server: 1 + rng.below(SERVERS),
                    one_way: ms(&mut rng, 900, 1300),
                    crash_after: ms(&mut rng, 200, 400),
                    down: ms(&mut rng, 300, 500),
                },
            };
            faults.push(fault);
            gaps.push(ms(&mut rng, 450, 700));
        }
        // Clocks, as the module documentation says: within the bound, or one
        // server slow and two fast, moderately or severely.
        let within = rng.below(2) == 0;
        let severe = rng.below(2) == 0;
        let slow = rng.below(SERVERS);
        let mut drifts = Vec::new();
        let mut skews = Vec::new();
        for i in 0..SERVERS {
            let (magnitude, sign) = if within {
                let sign = if rng.below(2) == 0 { 1 } else { -1 };
                (rng.below(DRIFT_BOUND_PPM / 3 + 1), sign)
            } else if severe {
                // The slow server's heartbeats, at a fifth of its clock's timeout,
                // must still arrive within the fast followers' minimum timeout, or
                // it cannot lead at all: the rates stay within a factor of five.
                let (lo, hi) = if i == slow {
                    (650_000, 750_000)
                } else {
                    (250_000, 350_000)
                };
                (lo + rng.below(hi - lo + 1), if i == slow { -1 } else { 1 })
            } else {
                let lo = 2_000f64.ln();
                let hi = 100_000f64.ln();
                let u = rng.below(1_000_000) as f64 / 1_000_000.0;
                (
                    (lo + (hi - lo) * u).exp() as u64,
                    if i == slow { -1 } else { 1 },
                )
            };
            drifts.push(sign * i64::try_from(magnitude).expect("small"));
            let skew = i64::try_from(rng.below(50_000_000)).expect("small");
            skews.push(if rng.below(2) == 0 { skew } else { -skew });
        }
        Self {
            warmup: Duration::from_millis(1200),
            trial: Trial {
                settle: ms(&mut rng, 600, 900),
                isolate: ms(&mut rng, 500, 800),
            },
            faults,
            gaps,
            settle: election_max() * LIVENESS_TIMEOUTS + Duration::from_millis(200),
            drifts,
            skews,
        }
    }

    /// The server with the slowest clock, 1-based.
    #[must_use]
    pub fn slowest(&self) -> u64 {
        let (i, _) = self
            .drifts
            .iter()
            .enumerate()
            .min_by_key(|(_, d)| **d)
            .expect("servers");
        i as u64 + 1
    }

    /// The largest rate at which two servers' clocks drift apart, in parts per
    /// million.
    #[must_use]
    pub fn max_relative_drift_ppm(&self) -> u64 {
        let mut max = 0;
        for a in &self.drifts {
            for b in &self.drifts {
                max = max.max((a - b).unsigned_abs());
            }
        }
        max
    }

    /// Whether some pair of servers drifts apart faster than the lease assumes.
    #[must_use]
    pub fn drift_exceeded(&self) -> bool {
        self.max_relative_drift_ppm() > DRIFT_BOUND_PPM
    }

    /// The whole run's virtual duration.
    #[must_use]
    pub fn total(&self) -> Duration {
        let faults: Duration = self
            .faults
            .iter()
            .map(|fault| match fault {
                Fault::Isolate { for_, .. }
                | Fault::IsolateLeader { for_ }
                | Fault::OneWay { for_, .. } => *for_,
                Fault::Crash { down, .. } | Fault::CrashLeader { down } => *down,
                Fault::StaleSender { one_way, .. } => *one_way,
            })
            .sum();
        let trial = TRANSFER_WAIT + self.trial.settle + self.trial.isolate;
        self.warmup + trial + faults + self.gaps.iter().sum::<Duration>() + self.settle
    }
}

/// What the clients counted.
#[derive(Clone, Debug, Default)]
pub struct ClientStats {
    /// Operations that returned.
    pub completed: u64,
    /// Operations abandoned as pending.
    pub abandoned: u64,
    /// Tries answered with NotLeader.
    pub redirected: u64,
}

type SharedStats = Arc<Mutex<ClientStats>>;

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// Which server ran.
    pub variant: Variant,
    /// How the run was scheduled (D-016).
    pub policy: Policy,
    /// The faults it ran.
    pub schedule: Schedule,
    /// The trace as records.
    pub records: Vec<TraceRecord>,
    /// The trace as moirae JSONL.
    pub jsonl: String,
    /// When the last fault healed or the last crashed server restarted.
    pub last_heal: Instant,
    /// Every isolation of one server: (server, from, until).
    pub isolations: Vec<(u64, Instant, Instant)>,
    /// Whether the slowest clock led when the trial cut the leader off.
    pub trial_led_by_slowest: bool,
    /// Servers refused at a restart, with the reason.
    pub refused: Vec<(u64, String)>,
    /// Why the run stopped early, if it did: a safety violation the folds saw at a
    /// slice boundary, or a runaway past [`TRACE_CAP`].
    pub stopped: Option<String>,
    /// The clients' history.
    pub history: History,
    /// The clients' counts.
    pub clients: ClientStats,
}

impl Report {
    /// Whether some pair of servers' clocks drifted apart faster than the lease
    /// assumes on this seed.
    #[must_use]
    pub fn drift_exceeded(&self) -> bool {
        self.schedule.drift_exceeded()
    }

    /// Reads served by a lease.
    #[must_use]
    pub fn lease_reads(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftRead { lease: true, .. }))
    }

    /// Reads served after a heartbeat round.
    #[must_use]
    pub fn read_index_reads(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftRead { lease: false, .. }))
    }

    /// Times the guard stopped trusting a follower.
    #[must_use]
    pub fn lease_revokes(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftLeaseRevoked { .. }))
    }

    /// Times a leader stepped down for want of a majority.
    #[must_use]
    pub fn quorum_losses(&self) -> usize {
        self.count(|e| matches!(e, TraceEvent::RaftQuorumLost { .. }))
    }
}

impl Report {
    /// The events, without their times.
    #[must_use]
    pub fn events(&self) -> Vec<TraceEvent> {
        self.records.iter().map(|r| r.event.clone()).collect()
    }

    /// How many records satisfy `f`.
    pub fn count(&self, f: impl Fn(&TraceEvent) -> bool) -> usize {
        self.records.iter().filter(|r| f(&r.event)).count()
    }

    /// Whether any record satisfies `f`.
    pub fn has(&self, f: impl Fn(&TraceEvent) -> bool) -> bool {
        self.records.iter().any(|r| f(&r.event))
    }

    /// Whether the run was scheduled uniformly, so that liveness can be asked of it.
    #[must_use]
    pub fn uniform(&self) -> bool {
        self.policy == Policy::Uniform
    }

    /// Whether a majority of servers was running at the end: liveness needs one.
    #[must_use]
    pub fn majority_up(&self) -> bool {
        (self.refused.len() as u64) * 2 < SERVERS
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

    /// Every invariant the run must satisfy, or the first violation.
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
        let events = self.events();
        if let Err(violation) = invariants::all(&events) {
            return fail(violation);
        }
        if let Err(violation) = invariants::commit_majority(&events, SERVERS as usize) {
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
        if let Err(violation) = self.isolation_keeps_the_term() {
            return fail(violation);
        }
        if self.uniform() && self.majority_up() {
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
            if let Err(violation) = self.timers_fire() {
                return fail(violation);
            }
        }
        Ok(())
    }

    /// Pre-vote (thesis §9.6): a server that receives nothing does not raise its
    /// term. Checked over every isolation the schedule made: the server's term at
    /// the heal equals its term when the isolation began.
    fn isolation_keeps_the_term(&self) -> Result<(), String> {
        for &(server, from, until) in &self.isolations {
            let term_at = |at: Instant| {
                self.records
                    .iter()
                    .filter(|r| r.at <= at)
                    .filter_map(|r| match &r.event {
                        TraceEvent::RaftTerm {
                            server: s, term, ..
                        } if *s == server => Some(*term),
                        _ => None,
                    })
                    .last()
                    .unwrap_or(0)
            };
            let (before, after) = (term_at(from), term_at(until));
            if after != before {
                return Err(format!(
                    "pre-vote: server {server} raised its term from {before} to {after} while isolated from {from:?} to {until:?}"
                ));
            }
        }
        Ok(())
    }

    /// Election timers fire (moirae rule 5): a running server that is not the
    /// leader campaigns within [`TIMER_TIMEOUTS`] maximum election timeouts of the
    /// last AppendEntries it received from a leader of its term or later, the last
    /// vote it granted, or its start.
    fn timers_fire(&self) -> Result<(), String> {
        // A server measures its timeout by its own clock: a slow one takes longer
        // in global time, and the bound scales with its rate.
        let bound_for = |server: u64| -> Duration {
            let ppm = self.schedule.drifts[server as usize - 1];
            let rate = (1_000_000 + ppm) as f64 / 1_000_000.0;
            (election_max() * TIMER_TIMEOUTS).div_f64(rate)
        };
        let mut payloads: BTreeMap<ananke_env::MessageId, Bytes> = BTreeMap::new();
        let mut up: BTreeSet<u64> = BTreeSet::new();
        let mut leaders: BTreeSet<u64> = BTreeSet::new();
        let mut terms: BTreeMap<u64, u64> = BTreeMap::new();
        let mut last_reset: BTreeMap<u64, Instant> = BTreeMap::new();
        for record in &self.records {
            let at = record.at;
            match &record.event {
                TraceEvent::MessageSent { id, payload, .. } => {
                    payloads.insert(*id, payload.clone());
                }
                TraceEvent::MessageDelivered { id, to, .. } => {
                    if let Some(server) = server_of(*to)
                        && let Some(payload) = payloads.get(id)
                        && let Ok(frame) = Frame::decode(payload.clone())
                        && let Message::AppendEntries { term, .. } = frame.message
                        && term >= terms.get(&server).copied().unwrap_or(0)
                    {
                        last_reset.insert(server, at);
                    }
                }
                TraceEvent::RaftTerm { server, term, role } => {
                    terms.insert(*server, *term);
                    if !up.contains(server) {
                        up.insert(*server);
                        last_reset.insert(*server, at);
                    }
                    match *role {
                        "leader" => {
                            leaders.insert(*server);
                        }
                        "pre-candidate" | "candidate" => {
                            leaders.remove(server);
                            last_reset.insert(*server, at);
                        }
                        _ => {
                            // A leader that steps down starts counting from here:
                            // its timer meant nothing while it led.
                            if leaders.remove(server) {
                                last_reset.insert(*server, at);
                            }
                        }
                    }
                }
                TraceEvent::RaftLeader { server, .. } => {
                    leaders.insert(*server);
                }
                TraceEvent::RaftVote {
                    server,
                    granted: true,
                    pre: false,
                    ..
                } => {
                    last_reset.insert(*server, at);
                }
                TraceEvent::NodeCrashed { node } => {
                    let server = u64::from(node.get());
                    up.remove(&server);
                    leaders.remove(&server);
                }
                _ => {}
            }
            for server in &up {
                if leaders.contains(server) {
                    continue;
                }
                let since = last_reset.get(server).copied().unwrap_or(at);
                if at.duration_since(since) > bound_for(*server) {
                    return Err(format!(
                        "timers: server {server} heard from no leader of its term and granted no vote since {since:?} and had not campaigned by {at:?}"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// The simulator configuration for `seed` and `schedule`.
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

/// The server configuration for `id` under `variant`.
#[must_use]
pub fn node_config(id: u64, variant: Variant) -> NodeConfig {
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
        // One entry per message: a follower behind by any number of entries is
        // caught up one message at a time, so the pipeline is exercised and a new
        // leader's own no-op reaches a follower in a message of its own, after the
        // older entries it re-sent. That is the Figure 8 window, a few milliseconds
        // wide, and with batching it closes whenever the older entries fit in the
        // no-op's batch (D-026, issue #22 for a batched sweep).
        raft: RaftConfig {
            variant,
            max_batch: 1,
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            drift_bound_ppm: DRIFT_BOUND_PPM,
            ..RaftConfig::default()
        },
        engine,
        inbox_capacity: 128,
    }
}

fn spawn_server(sim: &Sim, id: u64, variant: Variant) {
    let env = sim.env(node_of_server(id));
    let inner = env.clone();
    env.spawn("raft", async move {
        let _ = run_server(inner, node_config(id, variant)).await;
    });
}

/// The leader in force: the server of the latest `RaftLeader` event, or server 1.
fn leader_now(sim: &Sim) -> u64 {
    sim.trace()
        .iter()
        .rev()
        .find_map(|r| match r.event {
            TraceEvent::RaftLeader { server, .. } => Some(server),
            _ => None,
        })
        .unwrap_or(1)
}

fn to_command(op: &ClientOp) -> Command {
    match op {
        ClientOp::Put { key, value } => Command::Put {
            key: key.clone(),
            value: value.clone(),
        },
        ClientOp::Get { key } => Command::Get { key: key.clone() },
        ClientOp::Delete { key } => Command::Delete { key: key.clone() },
        ClientOp::Cas { key, expect, value } => Command::Cas {
            key: key.clone(),
            expect: expect.clone(),
            value: value.clone(),
        },
    }
}

fn to_result(outcome: Outcome) -> ClientResult {
    match outcome {
        Outcome::Done => ClientResult::Done,
        Outcome::Swapped(swapped) => ClientResult::Swapped(swapped),
        Outcome::Value(value) => ClientResult::Value(value),
    }
}

/// One client: operations on random keys against the leader it last heard of,
/// following NotLeader hints, abandoning a write it hears nothing about. Client 1
/// reads more than it writes: it is the client the trial and the leader isolation
/// keep on the cut-off leader's side, where a lease read is what matters.
async fn client<E: Environment>(env: E, n: u64, stats: SharedStats) {
    let Ok(sock) = env.net().bind(client_addr(n)).await else {
        return;
    };
    let mut incarnation = 0u64;
    let mut process = n << 32 | incarnation;
    let mut seq = 0u64;
    let mut leader: Option<u64> = None;
    // The server the last abandoned operation went to: not the first to try next.
    let mut avoid: Option<u64> = None;
    let mut known: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
    loop {
        let key = Bytes::from(format!("k{}", env.rng().below(KEYS)));
        let value = Bytes::from(format!("{n}.{incarnation}.{seq}"));
        let draw = env.rng().below(10);
        let op = match (n, draw) {
            (1, 0..=6) | (_, 4..=6) => ClientOp::Get { key: key.clone() },
            (1, 7) | (_, 0..=3) => ClientOp::Put {
                key: key.clone(),
                value,
            },
            (1, 8) | (_, 7) => ClientOp::Delete { key: key.clone() },
            _ => ClientOp::Cas {
                key: key.clone(),
                expect: known.get(&key).cloned().unwrap_or(None),
                value,
            },
        };
        env.trace(TraceEvent::ClientInvoke {
            client: process,
            seq,
            op: op.clone(),
        });
        let command = to_command(&op);
        let deadline = env.clock().now()
            + if op.is_write() {
                WRITE_TIMEOUT
            } else {
                OP_TIMEOUT
            };
        let mut target = leader.unwrap_or_else(|| {
            let pick = 1 + env.rng().below(SERVERS);
            if avoid == Some(pick) {
                pick % SERVERS + 1
            } else {
                pick
            }
        });
        let mut outcome = None;
        loop {
            let request = Request {
                client: process,
                seq,
                command: command.clone(),
            };
            if sock
                .send(server_addr(target), request.encode())
                .await
                .is_err()
            {
                return;
            }
            let try_deadline = if op.is_write() {
                deadline
            } else {
                deadline.min(env.clock().now() + TRY_TIMEOUT)
            };
            let mut got = None;
            loop {
                let recv = pin!(sock.recv());
                let timer = pin!(env.clock().sleep_until(try_deadline));
                match race(&env, recv, timer).await {
                    Either::Left(Ok((_, bytes))) => {
                        if let Ok(response) = Response::decode(bytes)
                            && response.client == process
                            && response.seq == seq
                        {
                            got = Some(response.reply);
                            break;
                        }
                    }
                    Either::Left(Err(_)) => return,
                    Either::Right(()) => break,
                }
            }
            let now = env.clock().now();
            match got {
                Some(Reply::Outcome(result)) => {
                    outcome = Some(result);
                    break;
                }
                Some(Reply::NotLeader { leader: hint }) => {
                    stats.lock().unwrap().redirected += 1;
                    if now >= deadline {
                        break;
                    }
                    match hint {
                        Some(l) => target = l.0,
                        None => {
                            env.clock().sleep(Duration::from_millis(20)).await;
                            target = target % SERVERS + 1;
                        }
                    }
                }
                None => {
                    // A get can be asked again elsewhere; a write cannot.
                    if !op.is_write() && now < deadline {
                        target = target % SERVERS + 1;
                    } else {
                        break;
                    }
                }
            }
        }
        match outcome {
            Some(result) => {
                leader = Some(target);
                match (&op, &result) {
                    (ClientOp::Put { value, .. }, Outcome::Done) => {
                        known.insert(key, Some(value.clone()));
                    }
                    (ClientOp::Delete { .. }, Outcome::Done) => {
                        known.insert(key, None);
                    }
                    (ClientOp::Get { .. }, Outcome::Value(value)) => {
                        known.insert(key, value.clone());
                    }
                    (ClientOp::Cas { value, .. }, Outcome::Swapped(true)) => {
                        known.insert(key, Some(value.clone()));
                    }
                    _ => {}
                }
                env.trace(TraceEvent::ClientReturn {
                    client: process,
                    seq,
                    result: to_result(result),
                });
                stats.lock().unwrap().completed += 1;
            }
            None => {
                stats.lock().unwrap().abandoned += 1;
                leader = None;
                avoid = Some(target);
                incarnation += 1;
                process = n << 32 | incarnation;
                known.clear();
            }
        }
        seq += 1;
        env.clock().sleep(OP_GAP).await;
    }
}

/// Runs the scenario for `seed` with the schedule drawn from it.
#[must_use]
pub fn run(seed: u64, variant: Variant) -> Report {
    run_with(seed, Schedule::draw(seed), variant)
}

/// Runs the scenario for `seed` with an explicit schedule.
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule, variant: Variant) -> Report {
    let mut sim = Sim::new(config(seed, &schedule));
    let servers: Vec<NodeId> = (0..SERVERS as usize)
        .map(|i| sim.add_node_with_clock(schedule.skews[i], schedule.drifts[i]))
        .collect();
    let clients: Vec<NodeId> = (0..CLIENTS).map(|_| sim.add_node()).collect();
    let admin = sim.add_node();
    let stats: Vec<SharedStats> = (0..CLIENTS).map(|_| SharedStats::default()).collect();
    for id in 1..=SERVERS {
        spawn_server(&sim, id, variant);
    }
    for (i, &node) in clients.iter().enumerate() {
        let env = sim.env(node);
        let inner = env.clone();
        let stats = stats[i].clone();
        env.spawn("client", client(inner, i as u64 + 1, stats));
    }
    let all_but = |server: u64, client: u64| -> (Vec<NodeId>, Vec<NodeId>) {
        let side: Vec<NodeId> = vec![servers[server as usize - 1], clients[client as usize - 1]];
        let rest: Vec<NodeId> = servers
            .iter()
            .chain(clients.iter())
            .chain(std::iter::once(&admin))
            .copied()
            .filter(|n| !side.contains(n))
            .collect();
        (side, rest)
    };
    let mut isolations = Vec::new();
    let mut watch = Watch::default();
    advance(&mut sim, schedule.warmup, &mut watch);
    let restart = |sim: &mut Sim, server: u64| {
        sim.restart(node_of_server(server));
        spawn_server(sim, server, variant);
    };
    // The lease trial: the operator hands leadership to the slowest clock, which
    // leads for a while and is then cut off with client 1.
    let slowest = schedule.slowest();
    let leader = leader_now(&sim);
    if leader != slowest {
        let env = sim.env(admin);
        let inner = env.clone();
        env.spawn("admin", async move {
            let Ok(sock) = inner.net().bind(admin_addr()).await else {
                return;
            };
            let request = Request {
                client: ADMIN,
                seq: 0,
                command: Command::Transfer { to: slowest },
            };
            let _ = sock.send(server_addr(leader), request.encode()).await;
        });
        advance(&mut sim, TRANSFER_WAIT, &mut watch);
    }
    advance(&mut sim, schedule.trial.settle, &mut watch);
    let leader = leader_now(&sim);
    let trial_led_by_slowest = leader == slowest;
    {
        let (side, rest) = all_but(leader, 1);
        let from = sim.now();
        sim.partition(&side, &rest);
        advance(&mut sim, schedule.trial.isolate, &mut watch);
        sim.heal();
        isolations.push((leader, from, sim.now()));
    }
    let mut last_heal = sim.now();
    advance(&mut sim, Duration::from_millis(500), &mut watch);
    for (fault, gap) in schedule.faults.iter().zip(schedule.gaps.iter()) {
        if watch.stopped.is_some() {
            break;
        }
        match fault {
            Fault::Isolate {
                server,
                client,
                for_,
            } => {
                let (side, rest) = all_but(*server, *client);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *for_, &mut watch);
                sim.heal();
                isolations.push((*server, from, sim.now()));
            }
            Fault::IsolateLeader { for_ } => {
                let leader = leader_now(&sim);
                let (side, rest) = all_but(leader, 1);
                let from = sim.now();
                sim.partition(&side, &rest);
                advance(&mut sim, *for_, &mut watch);
                sim.heal();
                isolations.push((leader, from, sim.now()));
            }
            Fault::OneWay { from, to, for_ } => {
                sim.block(node_of_server(*from), node_of_server(*to));
                advance(&mut sim, *for_, &mut watch);
                sim.heal();
            }
            Fault::Crash { server, down } => {
                sim.crash(node_of_server(*server));
                advance(&mut sim, *down, &mut watch);
                restart(&mut sim, *server);
            }
            Fault::CrashLeader { down } => {
                let leader = leader_now(&sim);
                sim.crash(node_of_server(leader));
                advance(&mut sim, *down, &mut watch);
                restart(&mut sim, leader);
            }
            Fault::StaleSender {
                server,
                one_way,
                crash_after,
                down,
            } => {
                let leader = leader_now(&sim);
                let stale = if *server == leader {
                    server % SERVERS + 1
                } else {
                    *server
                };
                for other in (1..=SERVERS).filter(|&s| s != stale) {
                    sim.block(node_of_server(other), node_of_server(stale));
                }
                advance(&mut sim, *crash_after, &mut watch);
                let leader = leader_now(&sim);
                let crashed = if leader == stale { None } else { Some(leader) };
                if let Some(leader) = crashed {
                    sim.crash(node_of_server(leader));
                }
                advance(&mut sim, *down, &mut watch);
                if let Some(leader) = crashed {
                    restart(&mut sim, leader);
                }
                let spent = *crash_after + *down;
                if *one_way > spent {
                    advance(&mut sim, *one_way - spent, &mut watch);
                }
                sim.heal();
            }
        }
        last_heal = sim.now();
        advance(&mut sim, *gap, &mut watch);
    }
    if watch.stopped.is_none() {
        advance(&mut sim, schedule.settle, &mut watch);
    }
    let records = sim.trace();
    let refused: Vec<(u64, String)> = records
        .iter()
        .filter_map(|r| match &r.event {
            TraceEvent::RaftRefused { server, reason } => Some((*server, reason.clone())),
            _ => None,
        })
        .collect();
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
        variant,
        policy: sim.policy(),
        schedule,
        jsonl: sim
            .to_moirae(&Export::new(&message::studio))
            .expect("the raft trace exports to moirae v2"),
        records,
        last_heal,
        isolations,
        trial_led_by_slowest,
        refused,
        stopped: watch.stopped,
        history,
        clients: clients_total,
    }
}

/// What the sliced advance watches for.
#[derive(Default)]
struct Watch {
    slices: u32,
    stopped: Option<String>,
}

/// Runs the simulation for `duration` in slices of [`SLICE`], running the safety
/// folds over the trace so far every [`CHECK_EVERY`] slices and stopping at the first
/// violation, or at [`TRACE_CAP`] records. A buggy server can make the cluster do
/// unbounded work, a follower that truncates on every append re-fetching its tail
/// forever, and a run must still end with a verdict.
fn advance(sim: &mut Sim, duration: Duration, watch: &mut Watch) {
    if watch.stopped.is_some() {
        return;
    }
    let mut left = duration;
    while left > Duration::ZERO {
        let step = left.min(SLICE);
        sim.run_for(step);
        left -= step;
        watch.slices += 1;
        let len = sim.trace_len();
        if len > TRACE_CAP {
            watch.stopped = Some(format!(
                "runaway: {len} trace records by {:?}, over the cap of {TRACE_CAP}",
                sim.now()
            ));
            return;
        }
        if watch.slices.is_multiple_of(CHECK_EVERY) {
            let events: Vec<TraceEvent> = sim.trace().into_iter().map(|r| r.event).collect();
            let verdict = invariants::all(&events)
                .and_then(|()| invariants::commit_majority(&events, SERVERS as usize));
            if let Err(violation) = verdict {
                watch.stopped = Some(format!("{violation} (at {:?})", sim.now()));
                return;
            }
        }
    }
}

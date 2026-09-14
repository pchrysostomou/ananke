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

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::moirae::Export;
use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{DropReason, Environment, Instant, NodeId, TraceEvent};
use ananke_raft::core::Variants;
use ananke_raft::message::{self, Frame, Message, SnapshotStatus};
use ananke_raft::store::mark_store_lost;
use ananke_raft::{invariants, run as run_server};
use moirae_sched::Policy;

use crate::lin::{self, History};
use crate::raft::{
    self, CLIENTS, ClientStats, DIR, ELECTION_MIN, SERVERS, TICK, node_config, server_of,
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
    /// The trace as moirae JSONL.
    pub jsonl: String,
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

/// A server's start: with `lost`, it first records in its store directory that
/// the store lost state, the mark a refusal leaves (D-044), so the open that
/// follows is refused.
fn spawn(sim: &Sim, id: u64, variants: Variants, lost: bool) {
    let env = sim.env(node(id));
    let inner = env.clone();
    env.spawn("raft", async move {
        if lost
            && mark_store_lost(&inner, Path::new(DIR), "the scenario lost this store")
                .await
                .is_err()
        {
            return;
        }
        let _ = run_server(inner, node_config(id, variants)).await;
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
        spawn(&sim, id, variants, false);
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
        jsonl: String::new(),
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
    spawn(&sim, victim, variants, true);
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
    report.jsonl = sim
        .to_moirae(&Export::new(&message::studio))
        .expect("the scenario's trace exports to moirae v2");
    report
}

impl Report {
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
                } if *server == leader
                    && *term == hold.term
                    && in_hold(r.decided)
                    && hold.quorum_lost.is_none() =>
                {
                    hold.quorum_lost = Some((r.decided, uncounted.clone()));
                }
                TraceEvent::RaftReseeded { server }
                    if *server == refused && in_hold(r.at) && hold.reseeded.is_none() =>
                {
                    hold.reseeded = Some(r.at);
                }
                TraceEvent::RaftCommit {
                    server,
                    term,
                    index,
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
        let events: Vec<TraceEvent> = self.records.iter().map(|r| r.event.clone()).collect();
        if let Err(violation) = invariants::all(&events) {
            return fail(violation);
        }
        if let Err(violation) =
            invariants::commit_majority(&events, usize::try_from(SERVERS).expect("small"))
        {
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

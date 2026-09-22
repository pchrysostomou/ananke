//! The directed re-seed shape: Q15's path from the loss to the four live installs
//! (SHARD.md §12, Stage B's exit criterion for the re-seed shape; §11, storage 8).
//!
//! Two slices meet here and neither reaches this on its own. D-077 refuses a node
//! whose shared engine lost state and rebuilds its four replicas in a fresh engine
//! beside the refused directory, where each waits, empty and marked, for a stream
//! that nothing in that tree could send. D-083 makes streams flow and installs
//! complete on the node, on a node that was never refused. The shape is the two
//! together: **a node refused, and re-seeded by four installs at once**.
//!
//! **The shape** (SHARD.md §12, Stage B's builds):
//!
//! - three servers, each hosting all four ranges;
//! - the node refused by its store's lost mark at a restart, as `sim/quorum.rs`
//!   refuses a server (D-049): it is crashed, its directory's marker is written, and
//!   it is started again, so the directory that is refused is one that really held a
//!   store and really lost it;
//! - its cap on streams **received** set to [`RECEIVE_CAP`], two, below its four
//!   ranges, so the re-seeds toward it wait for one another (Q14, D-075);
//! - a crash arm on a stream of its own, `reseed-crash` (D-031), which crashes the
//!   refused node on the trace event of one replica's refused mark — after that mark
//!   is written into the new engine and before that replica answers anything other
//!   than its re-seed stream — and restarts it.
//!
//! **What it asserts, per seed, on the correct system** ([`Report::check`]), in this
//! order:
//!
//! - **the node is still running**, because a node that stopped still has every stream,
//!   install and creation it managed in its trace;
//! - **(c), first half**: no replica answered anything other than its own re-seed stream
//!   before its refused mark was durable. First, because it is an ordering, and because
//!   `ServeBeforeRefusedMark` is its plant and was caught by (a) while (a) came first;
//! - **(a)** each of the four ranges traces its `RangeCreated { cause: snapshot }`, with
//!   exactly one start of the node between the refusal and the last install — the arm's —
//!   and no `RaftAdopted` at all: every range installed **live**, into the directory the
//!   re-seed built;
//! - **(b)** every re-seed stream completes into an install; the four were owed at once;
//!   and the cap held at least one stream back, read off `RaftSnapshotStartOver` with
//!   reason `Cap`. Raise the cap to the range count and that last clause is the one that
//!   fails, which is what makes it an assertion about the cap;
//! - **(c), second half**: every replica answered something after **its own install**.
//!   This is the half PR #86 could assert only as an absence, and it is keyed on the
//!   install rather than on the mark because a replica that was never re-seeded answers
//!   too — with the rejections of an empty log — so the mark-keyed set is a constant;
//! - **(d)** after the arm's crash the replica whose mark was written as refused restarts
//!   **as refused** — `RaftRecovered`'s state, which D-067 asked this trace for — and is
//!   re-seeded by an install that lands after the restart;
//! - **(e)** the run ends with the refused directory still marked lost and not one byte
//!   of it changed since the arm's crash (D-041). The baseline is read there and not at
//!   the refusal: an audit costs simulated disk time, and one taken at the refusal would
//!   push the arm's crash past the syncs D-067 requires it to precede.
//!
//! The arm's firing is asserted on every seed. `NodeVariant::ReseedMarkNotSynced` (D-067)
//! is caught at 48 % of a hundred seeds, so at every tier, and the rate is printed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ananke_env::sim::{Sim, SimConfig, TraceRecord};
use ananke_env::{
    Clock, Either, Environment, File, FileSystem, Instant, Network, NodeId, OpenOptions,
    RangeCause, RecoveredAs, Rng, Socket, StartOver, TraceEvent, race,
};
use ananke_raft::ServerId;
use ananke_raft::apply::Command;
use ananke_raft::client::{Reply, Request};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::message::Message;
use ananke_raft::store::{is_marked_lost, mark_store_lost};
use ananke_shard::client::{RangedRequest, RangedResponse};
use ananke_shard::range::RangeId;
use ananke_shard::server::ServerConfig;
use ananke_shard::variant::NodeVariants;
use ananke_storage::EngineConfig;
use bytes::Bytes;

use crate::raft::{TICK, client_addr, server_addr};
use crate::ranges::{DIR, FIRST_RANGE, INBOX_BYTES, RANGES, range_of_key};

/// The servers: three, each hosting all four ranges.
///
/// Three and not five: the refused node must be a voter the cluster can lose and
/// still write, so its ranges' leaders keep committing and keep compacting past it
/// while it is down, which is what leaves it behind the prefix and makes its re-seed
/// a stream rather than a catch-up by entries.
pub const SERVERS: u64 = 3;

/// The node whose engine loses state: the one this shape is about.
pub const VICTIM: u64 = 3;

/// The refused node's cap on snapshot streams **received** (Q14, D-075).
///
/// Two, below its four ranges, so two of the four re-seeds wait for a slot. This is
/// the scenario SHARD.md §12 names, and the reason the shape is four ranges and not
/// one: with a cap at or above the range count nothing ever waits, and a node that
/// abandoned an assembly for a chunk of another identity — the node as SHARD.md
/// :573-576 describes it — would look exactly like a node that did not.
pub const RECEIVE_CAP: usize = 2;

/// The log length a range passes before its leader takes a snapshot and compacts.
///
/// Low on purpose, as `sim/install.rs`'s is: the re-seed's installs are the thing
/// under test, and a leader that never compacted would feed the re-seeded replica by
/// AppendEntries, which is correct behaviour and no re-seed at all.
pub const SNAPSHOT_THRESHOLD: u64 = 64;

/// The clients: one per range, each writing the two keys of its own range.
///
/// One client round-robining over the four ranges writes each of them at a quarter of
/// its rate, and the rate is what this shape needs: a leader designates a follower
/// only once the log has outgrown that follower's match by `snapshot_threshold`
/// (D-037), so a range written too slowly while the node is gone is a range whose
/// leader never compacts past it and whose re-seed is a catch-up by entries — correct
/// behaviour, reported as a missing install.
const CLIENTS: u64 = RANGES;

/// How long the three run before the loss: long enough to elect on every range,
/// write past the threshold on every range, and compact.
const BEFORE: Duration = Duration::from_secs(5);
/// How long the node is gone before its store's marker is written and it is started
/// again.
///
/// Long enough for every range's leader to designate it — a follower quiet for two
/// minimum election timeouts and more than `snapshot_threshold` behind no longer
/// blocks compaction (D-037) — so that every one of the four leaders has compacted
/// past it and every one of the four re-seeds is a *stream* rather than a catch-up by
/// entries. Without it the shape asserts four installs and gets the two or three the
/// seed happened to compact past, which is correct behaviour reported as a failure.
const GONE: Duration = Duration::from_secs(4);
/// How long the arm keeps the node down after its crash.
const DOWN: Duration = Duration::from_millis(300);
/// How long the run goes on after the arm's restart: long enough for four streams
/// through two slots, and the installs they complete.
const AFTER: Duration = Duration::from_secs(7);
/// The longest the arm waits for the refusal, and then for the mark it crashes on.
/// A wait that runs out is an arm that did not fire, which [`Report::check`] fails on
/// rather than passing quietly.
pub const ARM_WAIT_BUDGET: Duration = Duration::from_millis(4000);
/// The slice the arm advances in while it waits.
///
/// A millisecond, an order below the disk's slowest write, so that the crash lands on
/// the mark's own trace event and not several marks later: the window D-067 requires,
/// before anything else syncs the new engine's log.
const ARM_STEP: Duration = Duration::from_millis(1);
/// The slice an audit of a directory advances in, and the longest it is given.
///
/// Every file it reads pays the simulated disk's latency, so an audit of a directory
/// of thirty files takes tens of milliseconds of virtual time; the budget is far above
/// that and exists so a wedged audit ends the run rather than hanging it.
const AUDIT_STEP: Duration = Duration::from_millis(20);
const AUDIT_BUDGET: Duration = Duration::from_secs(5);

/// A directory as an audit found it: each entry's name against its length and a
/// checksum of its bytes.
pub type DirAudit = BTreeMap<String, (u64, u64)>;

/// What one run of the shape found.
#[derive(Clone, Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// The node's variants.
    pub node_variants: NodeVariants,
    /// The run's trace.
    pub records: Vec<TraceRecord>,
    /// What the arm did.
    pub arm: Arm,
    /// The refused directory as the node's second life found it, or `None` if the
    /// audit did not finish.
    pub refused_at_refusal: Option<DirAudit>,
    /// The refused directory as the run left it, or `None` if the audit did not
    /// finish.
    pub refused_at_end: Option<DirAudit>,
    /// Whether the refused directory's marker still says lost at the end.
    pub still_lost: bool,
    /// Which server each simulated node is: `RangeCreated` names no server (§8), so
    /// the check reads it through the record's node.
    node_of: BTreeMap<NodeId, u64>,
}

/// What the `reseed-crash` arm did, recorded as it ran rather than read back.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Arm {
    /// Whether the arm fired: the refusal was reached, a replica's mark was traced,
    /// and the node was crashed on it.
    pub fired: bool,
    /// The range whose refused mark drew the crash.
    pub marked: Option<u64>,
    /// When the crash landed.
    pub at: Option<Instant>,
    /// When the node was started again.
    pub restarted: Option<Instant>,
    /// Trace records at the moment of the crash, so the checks can say what came
    /// before it and what came after.
    pub crashed_after: usize,
}

impl Report {
    /// The victim's server id.
    #[must_use]
    pub fn victim(&self) -> u64 {
        VICTIM
    }

    /// Every range of the scenario.
    #[must_use]
    pub fn every_range() -> BTreeSet<u64> {
        (0..RANGES).map(|i| FIRST_RANGE + i).collect()
    }

    /// The trace read for the refusal, the marks, the installs and the answers.
    #[must_use]
    pub fn read(&self) -> Read {
        read(&self.records, VICTIM, self.node_of.clone())
    }

    /// Whether the run reached the situation at all, which is the first thing to
    /// assert of a scenario built to reach one.
    ///
    /// A run whose node was never refused re-seeded nothing, and every assertion
    /// after that would pass against a silence.
    ///
    /// # Errors
    ///
    /// Naming what was missing, with the seed.
    pub fn reached(&self) -> Result<(), String> {
        let seed = self.seed;
        let read = self.read();
        if read.refusals == 0 {
            return Err(format!(
                "seed {seed}: node {VICTIM}'s store was never refused, so nothing \
                 was re-seeded and the shape did not reach the path it exists to \
                 exercise"
            ));
        }
        let every = Self::every_range();
        if read.replicas_refused != every {
            return Err(format!(
                "seed {seed}: the refusal took down {:?}, not the {every:?} the node \
                 holds, so the ranges it left standing are asserted about nothing",
                read.replicas_refused
            ));
        }
        // The arm is a fault, and a fault that did not fire is a run that tested the
        // schedule and not the crash (D-031). It is asserted here, per seed, as
        // SHARD.md §12 asks.
        if !self.arm.fired {
            return Err(format!(
                "seed {seed}: the `reseed-crash` arm never fired — no replica's \
                 refused mark was traced within {ARM_WAIT_BUDGET:?} of the refusal, \
                 so the node was never crashed on one and (d) is asserted about \
                 nothing"
            ));
        }
        Ok(())
    }

    /// The run's verdict: SHARD.md §12's (a) to (e), in that order.
    ///
    /// # Errors
    ///
    /// Naming the first criterion that fails, with the seed and what it saw.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        if self.records.len() > crate::ranges::TRACE_CAP {
            return Err(format!(
                "seed {seed}: runaway: {} trace records, over the cap of {}",
                self.records.len(),
                crate::ranges::TRACE_CAP
            ));
        }
        self.reached()?;
        let read = self.read();
        let every = Self::every_range();

        // Before any of (a) to (e): the re-seeded node is still running. A node that
        // stopped answers nothing further, and every criterion below would then be
        // asserted about a run that ended early rather than about a re-seed.
        if let Some(reason) = read.failures.first() {
            return Err(format!(
                "seed {seed}: node {VICTIM} stopped after its re-seed: {reason}. The \
                 point of Q15's re-seed is that the node does *not* stop"
            ));
        }

        // (c), first half, and it is first because it is an *ordering*: a replica that
        //     answered before its mark is one that answered whether or not anything
        //     else about the run went right, and the clause must be what catches it.
        //     `ServeBeforeRefusedMark` — a node that writes no mark at all — is this
        //     clause's plant, and while (a) came first the variant was caught by (a)
        //     instead, which tests the wrong sentence.
        if !read.served_before_the_mark.is_empty() {
            return Err(format!(
                "seed {seed}: (c) {:?} answered before their refused mark was durable: \
                 a replica in a fresh engine that answers before its mark is one that \
                 may vote again on state its node lost (D-035, D-042)",
                read.served_before_the_mark
            ));
        }

        // (a) Every range installed live into the new directory.
        let missing: Vec<u64> = every
            .difference(&read.created_by_install)
            .copied()
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: (a) {} of {} ranges never traced \
                 `RangeCreated {{ cause: snapshot }}` on node {VICTIM}: {missing:?}. \
                 A replica the re-seed left empty and marked is a replica that waits \
                 for its stream, and these are still waiting",
                missing.len(),
                every.len()
            ));
        }
        // A restart of the node between the refusal and its installs would mean the
        // ranges came back at a start rather than being installed into a running
        // engine. The arm's is the one restart allowed in that window, and it is the
        // one counted: the run's own second restart, which puts the node back over
        // replicas the installs had already filled, comes after them and is not in it.
        let last_install = read.installed_at.values().max().copied().unwrap_or(0);
        let restarts = read
            .starts_after_refusal
            .iter()
            .filter(|at| **at < last_install)
            .count();
        if restarts != 1 {
            return Err(format!(
                "seed {seed}: (a) node {VICTIM} started {restarts} times between its \
                 refusal and its last install, not once for the arm alone: an install \
                 that follows a start of the node is not an install into a *live* \
                 engine (§11, storage 5)"
            ));
        }
        if read.adoptions != 0 {
            return Err(format!(
                "seed {seed}: (a) {} `RaftAdopted` on node {VICTIM}: an install on the \
                 node is a live install and adopts nothing (D-066)",
                read.adoptions
            ));
        }

        // (b) Every re-seed stream completed, none abandoned for another identity.
        let streamed: BTreeSet<u64> = read.streams_to_victim.keys().copied().collect();
        let missing: Vec<u64> = every.difference(&streamed).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: (b) no stream was ever opened toward node {VICTIM} for \
                 {missing:?}"
            ));
        }
        let missing: Vec<u64> = every.difference(&read.installs).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: (b) {} of {} re-seed streams never completed into an \
                 install: {missing:?}. Four ranges share {RECEIVE_CAP} slots here, so \
                 a node that abandoned an assembly for a chunk of another identity \
                 would leave exactly this (SHARD.md:573-576)",
                missing.len(),
                every.len()
            ));
        }
        // And they shared the node's slots. Two things are asserted, because the first
        // is structural and the second is not: the four re-seeds were *owed* at once —
        // the node is refused whole, so every mark precedes the first install — and
        // more than [`RECEIVE_CAP`] of them were in flight at once, which is the cap
        // actually holding a stream back rather than four streams arriving to an idle
        // receiver. Raise the cap to the range count and the second fails, which is
        // what makes it an assertion about the cap rather than prose beside one.
        let last_mark = read.marked_at.values().max().copied();
        let first_install = read.installed_at.values().min().copied();
        match (last_mark, first_install) {
            (Some(mark), Some(install)) if install > mark => {}
            (mark, install) => {
                return Err(format!(
                    "seed {seed}: (b) the four re-seeds were not owed at once: the last \
                     refused mark is at {mark:?} and the first install at {install:?}, \
                     so the node's cap of {RECEIVE_CAP} was never asked to hold {} \
                     streams back",
                    every.len()
                ));
            }
        }
        // And the cap held a stream back: the node answered at least one chunk with
        // `StartOver { reason: Cap }` because its slots were full. That is the receive
        // cap doing the thing this shape sets it to two for, read off the node's own
        // trace rather than argued from the range count — raise the cap to the range
        // count and this is the clause that fails.
        if read.waited_for_a_slot.is_empty() {
            return Err(format!(
                "seed {seed}: (b) no stream toward node {VICTIM} was ever held back by \
                 its cap of {RECEIVE_CAP}: four re-seeds went through {} slots without \
                 one of them waiting, so the run says nothing about them sharing \
                 ({} were in flight at once)",
                RECEIVE_CAP,
                read.in_flight_at_once()
            ));
        }

        // (c) The mark is durable before the replica's first answer that is not its own
        //     re-seed stream — and every replica answered *after its own install*, so
        //     the fold has both sides. PR #86 could assert only the empty one, with its
        //     reason.
        //
        //     The ordering clause runs before the marks are counted, so a node that
        //     wrote no mark at all fails *here*, on the order, rather than on the
        //     situation: `ServeBeforeRefusedMark` is this clause's plant, and it was
        //     caught by `reached` and never by (c) while the count came first.
        if read.marked != every {
            return Err(format!(
                "seed {seed}: (c) {:?} of {every:?} replicas were marked refused in \
                 the new engine",
                read.marked
            ));
        }
        let silent: Vec<u64> = every
            .difference(&read.answered_after_the_install)
            .copied()
            .collect();
        if !silent.is_empty() {
            return Err(format!(
                "seed {seed}: (c) {silent:?} answered nothing at all, other than their \
                 re-seed stream, after their own install completed, so the order (c) \
                 asserts is asserted about nothing for them. It is read against the \
                 install and not against the mark on purpose: a replica that was never \
                 re-seeded still answers, with the rejections of an empty log, and a \
                 set keyed on the mark alone is full on exactly the runs this is meant \
                 to fail ({:?} answered after their mark)",
                read.answered_after_the_mark
            ));
        }

        // (d) The replica whose mark drew the crash comes back refused, and is
        //     re-seeded after the restart.
        let marked = self
            .arm
            .marked
            .ok_or_else(|| format!("seed {seed}: (d) the arm fired without recording its range"))?;
        let after: Vec<(u64, RecoveredAs)> = read
            .restatements
            .iter()
            .filter(|(at, _, _)| *at >= self.arm.crashed_after)
            .map(|(_, range, state)| (*range, *state))
            .collect();
        if after.is_empty() {
            return Err(format!(
                "seed {seed}: (d) node {VICTIM} restated no replica after the arm's \
                 crash: it was crashed and never came back"
            ));
        }
        let state = after
            .iter()
            .find(|(range, _)| *range == marked)
            .map(|(_, state)| *state);
        if state != Some(RecoveredAs::Refused) {
            return Err(format!(
                "seed {seed}: (d) range {marked}, whose refused mark drew the crash, \
                 restated as {state:?} and not `Refused`. A replica whose mark did not \
                 survive the crash opens fresh — term 0, no vote, incarnation 1 — and \
                 votes from then on (D-035, D-067)"
            ));
        }
        // Every other replica of a node refused whole restates refused or quarantined
        // too, never neither: D-067's second check, the one that catches a lost mark
        // whether or not a vote follows it.
        if let Some((range, state)) = after
            .iter()
            .find(|(_, state)| *state == RecoveredAs::Neither)
        {
            return Err(format!(
                "seed {seed}: (d) range {range} of a node refused whole restated as \
                 {state:?}: every replica of such a node restates refused or \
                 quarantined until its node is re-seeded (D-067)"
            ));
        }
        let reseeded_after = read
            .installed_at
            .get(&marked)
            .is_some_and(|at| *at >= self.arm.crashed_after);
        if !reseeded_after {
            return Err(format!(
                "seed {seed}: (d) range {marked} was not re-seeded after the arm's \
                 crash: its install is at {:?} and the crash at record {}",
                read.installed_at.get(&marked),
                self.arm.crashed_after
            ));
        }

        // (e) The refused directory never opened fresh (D-041).
        if !self.still_lost {
            return Err(format!(
                "seed {seed}: (e) the refused directory's marker no longer says lost"
            ));
        }
        let (Some(at_refusal), Some(at_end)) = (&self.refused_at_refusal, &self.refused_at_end)
        else {
            return Err(format!(
                "seed {seed}: (e) the refused directory could not be read within \
                 {AUDIT_BUDGET:?}, so the run says nothing about whether it changed"
            ));
        };
        if at_end != at_refusal {
            let changed: Vec<&String> = at_end
                .keys()
                .chain(at_refusal.keys())
                .filter(|name| at_end.get(*name) != at_refusal.get(*name))
                .collect();
            return Err(format!(
                "seed {seed}: (e) the refused directory changed after the refusal: \
                 {changed:?}. A directory that held a store never opens fresh and is \
                 never written to again (D-041, D-044)"
            ));
        }
        Ok(())
    }
}

/// What the shape's trace is read for.
#[derive(Clone, Debug, Default)]
pub struct Read {
    /// `RaftRefused` for the victim. One on the correct node.
    pub refusals: usize,
    /// The ranges the refusal took down, from `RaftReplicaRefused` (D-077).
    pub replicas_refused: BTreeSet<u64>,
    /// The ranges whose durable refused mark was traced by the re-seed.
    pub marked: BTreeSet<u64>,
    /// Where each range's first mark stands in the trace.
    pub marked_at: BTreeMap<u64, usize>,
    /// The ranges whose replica sent something other than its re-seed stream before
    /// its mark was durable: what (c) forbids.
    pub served_before_the_mark: BTreeSet<u64>,
    /// The ranges whose replica sent something other than its re-seed stream after
    /// its mark was durable.
    ///
    /// On its own this set is a constant: an empty replica answers AppendEntries with
    /// rejections whether or not its re-seed ever completed, so a full set here says
    /// nothing about the install. [`Read::answered_after_the_install`] is the one the
    /// check reads.
    pub answered_after_the_mark: BTreeSet<u64>,
    /// The ranges whose replica sent something other than its re-seed stream **after
    /// its own install completed**: what makes (c)'s fold non-vacuous.
    ///
    /// This is the half PR #86 could not assert at all, and it is asserted against the
    /// install rather than against the mark because only the second is evidence that
    /// the re-seed worked. A replica that was never installed still answers — with the
    /// rejections of an empty log — so a set keyed on the mark alone is full on runs
    /// where a range was never re-seeded at all.
    pub answered_after_the_install: BTreeSet<u64>,
    /// The ranges whose replica answered a chunk of its own re-seed stream, which is
    /// the answer (c) excludes by name.
    pub stream_answers: BTreeSet<u64>,
    /// The ranges an install created on the victim: `RangeCreated { cause: snapshot }`.
    pub created_by_install: BTreeSet<u64>,
    /// The ranges whose install completed on the victim after the refusal.
    pub installs: BTreeSet<u64>,
    /// Where each range's first install after the refusal stands in the trace.
    pub installed_at: BTreeMap<u64, usize>,
    /// Streams opened toward the victim, per range.
    pub streams_to_victim: BTreeMap<u64, usize>,
    /// The ranges whose stream the node held back because its slots were full, from
    /// `RaftSnapshotStartOver { reason: Cap }` (D-083's event): the receive cap doing
    /// the thing the shape sets it to two for.
    pub waited_for_a_slot: BTreeMap<u64, usize>,
    /// Where each range's first stream toward the victim after the refusal stands in
    /// the trace.
    pub stream_opened_at: BTreeMap<u64, usize>,
    /// Where each start of the victim's node task after the refusal stands in the
    /// trace.
    pub starts_after_refusal: Vec<usize>,
    /// Each restatement of a victim's replica: where it stands, its range, and the
    /// state the disk said it was in (D-067).
    pub restatements: Vec<(usize, u64, RecoveredAs)>,
    /// `RaftAdopted` on the victim.
    pub adoptions: usize,
    /// Why the victim stopped, from `RaftServerFailed`.
    pub failures: Vec<String>,
}

impl Read {
    /// The most re-seeds that were in flight toward the node at once: a re-seed is in
    /// flight from the first stream opened for its range to the install that
    /// completes it.
    ///
    /// It is the coverage figure for the receive cap. Four ranges against
    /// [`RECEIVE_CAP`] slots is the whole point of the shape, and a run in which the
    /// re-seeds happened to arrive one at a time would assert nothing about sharing
    /// them: three at once means at least one of them waited for a slot and was not
    /// abandoned while it waited (Q14, D-075).
    #[must_use]
    pub fn in_flight_at_once(&self) -> usize {
        let mut marks: Vec<(usize, i64)> = Vec::new();
        for range in self.installed_at.keys() {
            // From the range's own refused mark, not from the stream's opening: a
            // leader that opened a stream while the node was down opened it before the
            // refusal, and a re-seed is owed from the moment its replica is marked.
            let from = self
                .stream_opened_at
                .get(range)
                .copied()
                .min(self.marked_at.get(range).copied())
                .or_else(|| self.marked_at.get(range).copied());
            let Some(from) = from else {
                continue;
            };
            marks.push((from, 1));
            if let Some(installed) = self.installed_at.get(range) {
                marks.push((*installed, -1));
            }
        }
        marks.sort_unstable();
        let mut at_once = 0i64;
        let mut most = 0i64;
        for (_, delta) in marks {
            at_once += delta;
            most = most.max(at_once);
        }
        usize::try_from(most).unwrap_or(0)
    }
}

/// Reads a [`Read`] off the shape's trace, in the order the trace holds it.
#[must_use]
pub fn read(records: &[TraceRecord], victim: u64, node_of: BTreeMap<NodeId, u64>) -> Read {
    let mut out = Read::default();
    let is_victim = |record: &TraceRecord| {
        record
            .node
            .and_then(|node| node_of.get(&node))
            .copied()
            .map(|server| server == victim)
            .unwrap_or(false)
    };
    let mut refused = false;
    for (at, record) in records.iter().enumerate() {
        match &record.event {
            TraceEvent::RaftRefused { server, .. } if *server == victim => {
                out.refusals += 1;
                refused = true;
            }
            TraceEvent::RaftReplicaRefused { server, range } if *server == victim => {
                out.replicas_refused.insert(*range);
            }
            TraceEvent::RaftReseeded { server, range } if *server == victim => {
                out.marked.insert(*range);
                out.marked_at.entry(*range).or_insert(at);
            }
            TraceEvent::RaftRecovered {
                server,
                range,
                state,
                ..
            } if *server == victim => out.restatements.push((at, *range, *state)),
            TraceEvent::RaftAdopted { server } if *server == victim => out.adoptions += 1,
            TraceEvent::RaftServerFailed { server, reason } if *server == victim => {
                out.failures.push(reason.clone());
            }
            TraceEvent::RaftSnapshotStartOver {
                server,
                range,
                reason: StartOver::Cap,
                ..
            } if *server == victim => {
                *out.waited_for_a_slot.entry(*range).or_default() += 1;
            }
            TraceEvent::RaftSnapshotStreams { to, range, .. } if *to == victim && refused => {
                *out.streams_to_victim.entry(*range).or_default() += 1;
                out.stream_opened_at.entry(*range).or_insert(at);
            }
            // An install completing on the victim. A restatement of an installed
            // replica traces the same event, so only the ones after the refusal are
            // read, and `installed_at` keeps the first of them.
            TraceEvent::RaftSnapshot {
                server,
                range,
                taken: false,
                ..
            } if *server == victim && refused => {
                out.installs.insert(*range);
                out.installed_at.entry(*range).or_insert(at);
            }
            TraceEvent::RangeCreated {
                range,
                cause: RangeCause::Snapshot,
                ..
            } if is_victim(record) => {
                out.created_by_install.insert(*range);
            }
            // The node's own task starting again: the harness is the only thing that
            // can start it, and a restart of the node between the refusal and an
            // install is what (a) forbids.
            TraceEvent::TaskSpawned { name, .. } if *name == "node" && is_victim(record) => {
                if refused {
                    out.starts_after_refusal.push(at);
                }
            }
            // A message on the wire is the replica answering. Read off the frames
            // themselves, so what counts is what the node actually sent. A chunk and
            // its acknowledgement are the re-seed stream, which (c) excludes by name;
            // everything else is an answer.
            TraceEvent::MessageSent { payload, .. } if is_victim(record) => {
                if ananke_shard::is_ranged(payload) {
                    continue;
                }
                let Ok(decoded) = ananke_shard::decode(payload) else {
                    continue;
                };
                for tagged in decoded.messages {
                    let range = tagged.range.get();
                    if !out.replicas_refused.contains(&range) {
                        continue;
                    }
                    if matches!(
                        tagged.frame.message,
                        Message::InstallSnapshot { .. } | Message::InstallSnapshotResponse { .. }
                    ) {
                        out.stream_answers.insert(range);
                        continue;
                    }
                    if out.marked.contains(&range) {
                        out.answered_after_the_mark.insert(range);
                        if out.installed_at.contains_key(&range) {
                            out.answered_after_the_install.insert(range);
                        }
                    } else {
                        out.served_before_the_mark.insert(range);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// The simulator's configuration: a network that loses and delays and a disk that
/// takes time, as `sim/ranges.rs`'s is.
///
/// No bit rotting: the loss this shape is about is written by the scenario, at a
/// moment the arm can aim from, rather than drawn. A rotted table would refuse a node
/// the shape did not choose, which is `sim/raft.rs`'s business and not this one's.
#[must_use]
pub fn config(seed: u64) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.net.p_drop = 0.02;
    config.net.p_duplicate = 0.02;
    config.net.delay_min = Duration::from_millis(1);
    config.net.delay_max = Duration::from_millis(8);
    config.fs.p_durable = 1.0;
    config.fs.p_bitrot = 0.0;
    config.fs.latency_min = Duration::from_micros(100);
    config.fs.latency_max = Duration::from_millis(2);
    config.run_length_hint = SimConfig::run_length_hint_for(
        u32::try_from(SERVERS + 1).expect("small"),
        BEFORE + GONE + ARM_WAIT_BUDGET + DOWN + AFTER,
    );
    config
}

/// One node's configuration for this shape.
#[must_use]
pub fn server_config(id: u64, variants: impl Into<Variants>, node: NodeVariants) -> ServerConfig {
    let mut engine = EngineConfig::new(PathBuf::from(DIR));
    engine.memtable_bytes = 16 * 1024;
    engine.segment_bytes = 16 * 1024;
    engine.background_compaction = true;
    ServerConfig {
        id: ServerId(id),
        listen: server_addr(id),
        servers: (1..=SERVERS)
            .map(|s| (ServerId(s), server_addr(s)))
            .collect(),
        ranges: crate::ranges::ranges(),
        initial_voters: (1..=SERVERS).map(ServerId).collect(),
        raft: RaftConfig {
            variants: variants.into(),
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: INBOX_BYTES,
        // The whole point: two slots for four ranges (Q14, D-075).
        snapshot_cap: RECEIVE_CAP,
        node,
    }
}

/// A writer of one range, which keeps that range's log growing so it passes its
/// threshold and its leader compacts.
async fn writer<E: Environment>(env: E, client: u64, range: u64, committed: Arc<Mutex<u64>>) {
    let Ok(sock) = env.net().bind(client_addr(client)).await else {
        return;
    };
    let mut leaders: BTreeMap<u64, u64> = BTreeMap::new();
    let mut seq = 0u64;
    loop {
        // The two keys of this client's own range, so every range is written at the
        // same rate and no range is left short of the threshold.
        let key = Bytes::from(format!("k{}", (range - FIRST_RANGE) * 2 + seq % 2));
        debug_assert_eq!(range_of_key(&key), range, "a client writes its own range");
        let value = Bytes::from(format!("v{seq}"));
        seq += 1;
        let target = leaders
            .get(&range)
            .copied()
            .unwrap_or_else(|| 1 + env.rng().below(SERVERS));
        let request = RangedRequest {
            range: RangeId(range),
            request: Request {
                client: 1,
                seq,
                command: Command::Put { key, value },
            },
        };
        let _ = sock.send(server_addr(target), request.encode()).await;
        let deadline = env.clock().now() + Duration::from_millis(80);
        loop {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep_until(deadline));
            let bytes = match race(&env, recv, timer).await {
                Either::Left(Ok((_, bytes))) => bytes,
                Either::Left(Err(_)) | Either::Right(()) => break,
            };
            let Ok(response) = RangedResponse::decode(bytes) else {
                continue;
            };
            match response.response.reply {
                Reply::NotLeader { leader: Some(who) } => {
                    leaders.insert(range, who.0);
                    break;
                }
                Reply::NotLeader { leader: None } => {
                    leaders.remove(&range);
                    break;
                }
                Reply::Outcome(_) => {
                    leaders.insert(range, target);
                    *committed.lock().expect("the counter") += 1;
                    break;
                }
            }
        }
        env.clock().sleep(Duration::from_millis(4)).await;
    }
}

/// Runs the shape for `seed`.
#[must_use]
pub fn run(seed: u64, variants: impl Into<Variants>, node_variants: NodeVariants) -> Report {
    let variants = variants.into();
    let mut sim = Sim::new(config(seed));
    let nodes: Vec<NodeId> = (0..SERVERS as usize).map(|_| sim.add_node()).collect();
    let client = sim.add_node();
    let committed: Arc<Mutex<u64>> = Arc::default();
    for id in 1..=SERVERS {
        spawn(
            &sim,
            nodes[id as usize - 1],
            id,
            variants,
            node_variants,
            false,
        );
    }
    for i in 0..CLIENTS {
        let env = sim.env(client);
        let inner = env.clone();
        let committed = committed.clone();
        env.spawn("client", writer(inner, i + 1, FIRST_RANGE + i, committed));
    }
    // Every range elects, writes past its threshold and compacts.
    sim.run_for(BEFORE);

    // The loss. The node is crashed, its directory's marker is written while it is
    // down, and it is started again: what is refused is a directory that held a
    // store and lost it, which is D-041's case and `sim/quorum.rs`'s (D-049).
    let at = nodes[VICTIM as usize - 1];
    sim.crash(at);
    // Gone long enough that every range's leader designates it and compacts past it
    // (D-037), so every one of the four re-seeds needs a stream.
    sim.run_for(GONE);
    sim.restart(at);
    spawn(&sim, at, VICTIM, variants, node_variants, true);

    // The `reseed-crash` arm, on its own stream of decisions: it draws nothing, so it
    // lengthens no schedule stream (D-031). It advances in slices until the node is
    // refused and then, in the *same* watch, until the first replica's refused mark,
    // and crashes the node on that event — before anything else syncs the new engine's
    // log, which is the window D-067 requires.
    //
    // One watch and not two. A watch that stopped at the refusal and started another
    // would skip whatever the re-seed traced in between, and the re-seed writes all
    // four marks inside a few milliseconds: the second watch would then crash on a
    // *later* `RaftReseeded` — an install's restatement of a replica's quarantine,
    // which is the same event — and the arm would be aimed at something else entirely.
    let mut arm = Arm::default();
    let mut refused_at_refusal = None;
    {
        let mut scanned = sim.trace_len();
        let mut waited = Duration::ZERO;
        let mut refused = false;
        let mut refused_seen = false;
        while waited < ARM_WAIT_BUDGET && !arm.fired {
            sim.run_for(ARM_STEP);
            waited += ARM_STEP;
            let records = sim.trace_from(scanned);
            scanned += records.len();
            for record in &records {
                match &record.event {
                    TraceEvent::RaftRefused { server, .. } if *server == VICTIM => {
                        refused = true;
                        refused_seen = true;
                    }
                    TraceEvent::RaftReseeded { server, range } if *server == VICTIM && refused => {
                        arm.marked = Some(*range);
                        arm.fired = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
        // A crash at the budget's end when no mark ever arrives, as `Fault::CrashRefused`
        // crashes a victim that never flushes (D-044): the arm is a crash, and a run
        // where the node was never crashed asserts nothing about (d). It is also what
        // gives (c) its plant — `ServeBeforeRefusedMark` writes no mark at all, so
        // without this the arm would never fire on it and the variant would be caught by
        // the situation rather than by the clause it breaks.
        if !arm.fired && refused_seen {
            arm.fired = true;
        }
        if arm.fired {
            arm.at = Some(sim.now());
            arm.crashed_after = sim.trace_len();
            sim.crash(at);
            // The refused directory, read with the node down so nothing on it can be
            // writing. This is the first moment after the refusal at which that is
            // true, and it is *not* the refusal itself: an audit there advances the run
            // while it reads — every read pays the disk — and the re-seed's remaining
            // stores would open, and sync, inside it. D-067 requires this crash to land
            // on the mark's own trace event, before anything else syncs the new
            // engine's log, and that requirement wins: measured both ways,
            // `ReseedMarkNotSynced`'s catch is 39 % with the crash on the mark and 1 %
            // with an audit in front of it. What (e) therefore does not cover is the
            // few milliseconds between `RaftRefused` and this crash, in which the node
            // is opening the *new* directory and the refused engine is already quiesced
            // (D-044 marks and quiesces before the event is traced). The entry says so.
            refused_at_refusal = audit(&mut sim, at, Path::new(DIR));
            sim.run_for(DOWN);
            sim.restart(at);
            spawn(&sim, at, VICTIM, variants, node_variants, false);
            arm.restarted = Some(sim.now());
        }
    }

    // The four streams, through two slots, and the installs they complete.
    sim.run_for(AFTER);

    let refused_at_end = audit(&mut sim, at, Path::new(DIR));
    let still_lost = marked_lost(&mut sim, at, Path::new(DIR));
    Report {
        seed,
        node_variants,
        records: sim.trace(),
        arm,
        refused_at_refusal,
        refused_at_end,
        still_lost,
        node_of: nodes
            .iter()
            .enumerate()
            .map(|(at, node)| (*node, at as u64 + 1))
            .collect(),
    }
}

/// Every entry of `dir` on the node's own disk, with its length and a checksum of
/// its bytes.
///
/// Read by a task on the node itself, through the same `Environment` the node wrote
/// them through, which is what makes the answer the node's own view of its disk
/// rather than the harness's (`sim/ranges.rs`'s directory audit does the same).
///
/// The run is advanced until that task *finishes*, not for a fixed slice. Every read
/// it makes pays the simulated disk's latency, so a fixed slice hands back however
/// much of the directory the task got through before the slice ran out — and two such
/// audits differ from each other wherever they stopped, which reads exactly like the
/// directory having changed. That is [`AUDIT_BUDGET`]'s reason, and an audit that does
/// not finish inside it is handed back as `None` rather than as a short listing.
fn audit(sim: &mut Sim, at: NodeId, dir: &Path) -> Option<DirAudit> {
    let found: Arc<Mutex<DirAudit>> = Arc::new(Mutex::new(DirAudit::new()));
    let done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let out = found.clone();
    let finished = done.clone();
    let env = sim.env(at);
    let inner = env.clone();
    let dir = dir.to_path_buf();
    env.spawn("audit", async move {
        let Ok(listed) = inner.fs().read_dir(&dir).await else {
            return;
        };
        for entry in listed {
            // A listing hands back names, not paths.
            let Some(name) = entry
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
            else {
                continue;
            };
            let path = dir.join(&name);
            let Ok(file) = inner.fs().open(&path, OpenOptions::new().read(true)).await else {
                continue;
            };
            let Ok(size) = file.size().await else {
                continue;
            };
            let bytes = file
                .read_at(0, usize::try_from(size).unwrap_or(usize::MAX))
                .await
                .unwrap_or_default();
            out.lock()
                .expect("the audit")
                .insert(name, (size, checksum(&bytes)));
        }
        *finished.lock().expect("the audit") = true;
    });
    let mut waited = Duration::ZERO;
    while waited < AUDIT_BUDGET && !*done.lock().expect("the audit") {
        sim.run_for(AUDIT_STEP);
        waited += AUDIT_STEP;
    }
    let complete = *done.lock().expect("the audit");
    let taken = found.lock().expect("the audit").clone();
    complete.then_some(taken)
}

/// Whether `dir`'s marker says the store there lost state, asked of the node's own
/// disk after the run.
fn marked_lost(sim: &mut Sim, at: NodeId, dir: &Path) -> bool {
    let found: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    let out = found.clone();
    let env = sim.env(at);
    let inner = env.clone();
    let dir = dir.to_path_buf();
    env.spawn("marker", async move {
        if let Ok(lost) = is_marked_lost(&inner, &dir).await {
            *out.lock().expect("the marker") = Some(lost);
        }
    });
    // One file, one read, and the same reason as [`audit`]'s loop: the answer is
    // waited for rather than sampled after a slice. An answer that never comes reads
    // as `false`, which fails (e) rather than passing it.
    let mut waited = Duration::ZERO;
    while waited < AUDIT_BUDGET && found.lock().expect("the marker").is_none() {
        sim.run_for(AUDIT_STEP);
        waited += AUDIT_STEP;
    }
    let taken = *found.lock().expect("the marker");
    taken.unwrap_or(false)
}

/// FNV-1a over a file's bytes: enough to say that a byte of the refused directory
/// changed, and deterministic, which a hash from the environment would not be.
fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn spawn(
    sim: &Sim,
    at: NodeId,
    id: u64,
    variants: Variants,
    node: NodeVariants,
    lose_the_store: bool,
) {
    let env = sim.env(at);
    let inner = env.clone();
    env.spawn("node", async move {
        if lose_the_store {
            let marked = mark_store_lost(
                &inner,
                Path::new(DIR),
                "the scenario lost this node's store",
            )
            .await;
            if marked.is_err() {
                return;
            }
        }
        let _ = ananke_shard::server::run(inner, server_config(id, variants, node)).await;
    });
}

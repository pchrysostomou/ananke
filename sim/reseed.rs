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
//! - three servers, each hosting all four user ranges and, since PROPOSED D-096, the
//!   root and the meta range every bootstrap node hosts: six replicas, refused and
//!   marked together. The four user ranges are re-seeded by a stream each, as below;
//!   the two system ranges, which nothing writes to here, are re-seeded by the log
//!   from its first entry, and (a) asserts that of them ([`Report::log_ranges`]);
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
//! - **(a)** each of the four user ranges traces its `RangeCreated { cause: snapshot }`,
//!   with exactly one start of the node between the refusal and the last install — the
//!   arm's — and no `RaftAdopted` at all: every range installed **live**, into the
//!   directory the re-seed built; and each of the two system ranges is appended to from
//!   index 1 after its mark, with no stream opened toward it and no install;
//! - **(b)** every re-seed stream completes into an install, and the four were owed at
//!   once — every mark before the first install. The cap *holding a stream back* is a
//!   **rate**, not an every-seed property: measured, it happens on 199 seeds of 200, and
//!   the seed it does not is a legitimate interleaving where each slot frees before the
//!   next chunk arrives. The test prints that rate at every tier, asserts it against a
//!   measured floor, and pins the mechanism on a seed that has it;
//! - **(c), second half**: every streamed replica answered something after **its own
//!   install**, and every replica the log rebuilt answered after its mark.
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
    ApplyEffect, Clock, Either, Environment, File, FileSystem, Instant, Network, NodeId,
    OpenOptions, RangeCause, RangeState, RecoveredAs, Rng, Socket, StartOver, TraceEvent, race,
};
use ananke_raft::ServerId;
use ananke_raft::apply::Command;
use ananke_raft::client::{Reply, Request};
use ananke_raft::core::{RaftConfig, Variants};
use ananke_raft::message::Message;
use ananke_raft::store::{STORE_MARKER, is_marked_lost, mark_store_lost};
use ananke_shard::client::{RangedRequest, RangedResponse};
use ananke_shard::descriptor::{FIRST_GENERATION, RangeDescriptor};
use ananke_shard::generation_dir;
use ananke_shard::invariants::Held;
use ananke_shard::range::RangeId;
use ananke_shard::range_variant::RangeVariants;
use ananke_shard::server::ServerConfig;
use ananke_shard::snapshot::parse_version;
use ananke_shard::variant::NodeVariants;
use ananke_storage::{Engine, EngineConfig};
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

/// The bytes a snapshot chunk carries.
///
/// Four kilobytes, as `sim/raft.rs`'s streams use, and not `RaftConfig`'s 256 KiB
/// default: at the default every range's re-seed fits in **one** chunk, so a slot is
/// taken and given back inside a single round trip and the four re-seeds can interleave
/// so that none of them is ever told to wait. The cap is then a setting the run does not
/// exercise — measured, it held nothing back on 1 seed of 200, which is the seed the
/// nightly failed on — and (b)'s clause about it would be an every-seed assertion of
/// something the schedule does not force. At four kilobytes a stream is a dozen chunks
/// and a slot is held across round trips, which is what makes two slots for four ranges
/// a situation rather than a setting.
const SNAPSHOT_CHUNK: usize = 512;

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

    /// Every replica the node holds: the four user ranges and, since PROPOSED D-096,
    /// the two system ranges every bootstrap node hosts. A refusal takes all six
    /// down, the re-seed marks all six, and every restatement is read for all six.
    #[must_use]
    pub fn every_range() -> BTreeSet<u64> {
        Self::log_ranges()
            .into_iter()
            .chain(Self::streamed_ranges())
            .collect()
    }

    /// The ranges the re-seed rebuilds by a **stream**: the four user ranges, whose
    /// clients' writes had their leaders compact past index 1 before the refusal, so
    /// a leader that forgot the replica's progress (D-042) has nothing to append from
    /// and streams. (a), (b), (d)'s install and the cap are about these four.
    #[must_use]
    pub fn streamed_ranges() -> BTreeSet<u64> {
        (0..RANGES).map(|i| FIRST_RANGE + i).collect()
    }

    /// The ranges the re-seed rebuilds by the **log**: the root and the meta range,
    /// which nothing writes to in this shape, so their leaders hold every entry from
    /// index 1 and compact nothing. A leader that forgot such a replica's progress
    /// appends from index 1 and the replica, empty and marked, takes the log from its
    /// first entry; no stream is opened and no install lands. It is the same re-seed
    /// — the mark, the incarnation, the leader's reset — completed by the other of
    /// Raft's two ways to catch a follower up, and (a) asserts it as such
    /// (PROPOSED D-096). The day a system range carries writes enough to compact,
    /// (a) says so here and the range moves to [`Self::streamed_ranges`].
    #[must_use]
    pub fn log_ranges() -> BTreeSet<u64> {
        BTreeSet::from([crate::ranges::ROOT_RANGE, crate::ranges::META_RANGE])
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
        // SHARD.md §8's checks 7, 9, 10, 16, 17 and 18 — `ananke_shard::invariants`'
        // seven folds — over this node scenario's trace (Q40; PROPOSED D-097, D-098).
        // Every node scenario folds them, the sharded quorum scenario since PROPOSED
        // D-106 (#140).
        if let Err(violation) = ananke_shard::invariants::all(&self.records) {
            return Err(format!("seed {seed}: {violation}"));
        }
        let read = self.read();
        let every = Self::every_range();
        let streamed = Self::streamed_ranges();
        let by_log = Self::log_ranges();

        // (c), first half, and it is first because it is an *ordering*: a replica that
        //     answered before its mark is one that answered whether or not anything
        //     else about the run went right, and the clause must be what catches it.
        //     `ServeBeforeRefusedMark` — a node that writes no mark at all — is this
        //     clause's plant, and while (a) came first the variant was caught by (a)
        //     instead, which tests the wrong sentence.
        //
        //     First before the runaway cap too, for the same reason (PROPOSED D-086, on
        //     its merge with `main`): the answer before the mark is in the trace's
        //     prefix, and a run that later runs past the cap did not un-answer it. On
        //     that merge the variant's seed 7 grew from 350 130 records to 401 151 —
        //     the re-seeded node's installs complete and it serves, so the run does
        //     more: proposals +33 %, applies +27 %, takes and switches +28 % — and the
        //     cap, asked first, reported a runaway where (c) had its evidence all along.
        //     The correct shape stays under the cap on every seed, so the cap still
        //     reports a runaway there; this only stops a runaway from hiding (c).
        if !read.served_before_the_mark.is_empty() {
            return Err(format!(
                "seed {seed}: (c) {:?} answered before their refused mark was durable: \
                 a replica in a fresh engine that answers before its mark is one that \
                 may vote again on state its node lost (D-035, D-042)",
                read.served_before_the_mark
            ));
        }
        if self.records.len() > crate::ranges::TRACE_CAP {
            return Err(format!(
                "seed {seed}: runaway: {} trace records, over the cap of {}",
                self.records.len(),
                crate::ranges::TRACE_CAP
            ));
        }
        self.reached()?;

        // Before any of (a) to (e): the re-seeded node is still running. A node that
        // stopped answers nothing further, and every criterion below would then be
        // asserted about a run that ended early rather than about a re-seed.
        if let Some(reason) = read.failures.first() {
            return Err(format!(
                "seed {seed}: node {VICTIM} stopped after its re-seed: {reason}. The \
                 point of Q15's re-seed is that the node does *not* stop"
            ));
        }

        // (a) Every user range installed live into the new directory.
        let missing: Vec<u64> = streamed
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
                streamed.len()
            ));
        }
        // And the two system ranges rebuilt by the log from its first entry, with
        // nothing streamed and nothing installed: their leaders compacted nothing
        // (PROPOSED D-096; [`Self::log_ranges`]).
        for range in &by_log {
            let first = read.first_appended_after_the_mark.get(range).copied();
            let streams = read.streams_to_victim.get(range).copied().unwrap_or(0);
            let installed = read.installs.contains(range);
            if first != Some(1) || streams != 0 || installed {
                return Err(format!(
                    "seed {seed}: (a) range {range}, a system range nothing writes to, \
                     was not rebuilt on node {VICTIM} by the log from index 1: its \
                     first append after its refused mark is at {first:?}, {streams} \
                     streams were opened toward it and its install is {installed}. \
                     A leader that compacted nothing appends from the first entry to \
                     a follower whose progress it forgot (D-042), and a replica the \
                     re-seed left empty takes that log; a stream here means the \
                     range compacted, and it belongs with the streamed four"
                ));
            }
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
        let opened: BTreeSet<u64> = read.streams_to_victim.keys().copied().collect();
        let missing: Vec<u64> = streamed.difference(&opened).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: (b) no stream was ever opened toward node {VICTIM} for \
                 {missing:?}"
            ));
        }
        let missing: Vec<u64> = streamed.difference(&read.installs).copied().collect();
        if !missing.is_empty() {
            return Err(format!(
                "seed {seed}: (b) {} of {} re-seed streams never completed into an \
                 install: {missing:?}. Four ranges share {RECEIVE_CAP} slots here, so \
                 a node that abandoned an assembly for a chunk of another identity \
                 would leave exactly this (SHARD.md:573-576)",
                missing.len(),
                streamed.len()
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
                    streamed.len()
                ));
            }
        }
        // The cap holding a stream back is **not** asserted here, and that is a
        // measurement rather than a preference: over 200 seeds it holds one back on
        // 199 (median 9 chunks, quartiles 7 and 13), and the seed it does not — 113 —
        // is a legitimate run of the correct system, where the four re-seeds arrive
        // spread out enough that each slot is free again before the next chunk comes.
        // An every-seed assertion of it fails that seed, and a bound the correct system
        // trips is a model error to fix rather than a bound to widen (D-030, D-039) —
        // the model error being here, in asserting per seed a situation the schedule
        // does not force. It is §10's *rate* standard instead:
        // [`Read::waited_for_a_slot`] is what a sweep reads it from, and
        // `sim/tests/reseed.rs` prints the rate at every tier, asserts it against a
        // floor measured on the correct system, and pins the mechanism on a seed that
        // has it.

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
        let silent: Vec<u64> = streamed
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
        // A replica the log rebuilt has no install to key on: its answers *are* the
        // rebuild — the acknowledgements of the appends from index 1 — so for it the
        // mark is the right key, and (a) has already asserted the appends.
        let silent: Vec<u64> = by_log
            .difference(&read.answered_after_the_mark)
            .copied()
            .collect();
        if !silent.is_empty() {
            return Err(format!(
                "seed {seed}: (c) {silent:?}, rebuilt by the log, answered nothing \
                 after their refused mark, so nothing acknowledged the log that \
                 rebuilt them"
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
    /// The index of the victim's first `RaftAppend` per range after that range's
    /// refused mark: `1` for a replica the log rebuilt from its first entry
    /// (PROPOSED D-096; [`Report::log_ranges`]).
    pub first_appended_after_the_mark: BTreeMap<u64, u64>,
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
            TraceEvent::RaftAppend {
                server,
                range,
                index,
                ..
            } if *server == victim && out.marked.contains(range) => {
                out.first_appended_after_the_mark
                    .entry(*range)
                    .or_insert(*index);
            }
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
        bootstrap: (1..=SERVERS).map(ServerId).collect(),
        raft: RaftConfig {
            variants: variants.into(),
            tick_nanos: u64::try_from(TICK.as_nanos()).expect("small"),
            snapshot_threshold: SNAPSHOT_THRESHOLD,
            snapshot_chunk: SNAPSHOT_CHUNK,
            ..RaftConfig::default()
        },
        engine,
        inbox_bytes: INBOX_BYTES,
        // The whole point: two slots for four ranges (Q14, D-075).
        snapshot_cap: RECEIVE_CAP,
        id_block: crate::ranges::ID_BLOCK,
        refill_at: crate::ranges::REFILL_AT,
        node,
        // PROPOSED(D-105): no range-layer variant runs on this scenario.
        range_variants: RangeVariants::correct(),
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
            generation: FIRST_GENERATION,
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
                // This writer routes by the scenario's fixed map, which no split
                // moves here, so a mismatch is not a route it can follow: it drops
                // the hint and tries elsewhere, as it does for a leader it lost.
                // PROPOSED(D-097)
                Reply::NotLeader { leader: None } | Reply::RangeMismatch { .. } => {
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
                    // The first mark of a range the re-seed streams to: (d) is
                    // about the install that lands after the restart, and a system
                    // range the log rebuilds has none (PROPOSED D-096). The six
                    // marks are written inside one arm step, so the crash lands at
                    // the same moment whichever of them it is aimed at.
                    TraceEvent::RaftReseeded { server, range }
                        if *server == VICTIM
                            && refused
                            && Report::streamed_ranges().contains(range) =>
                    {
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

/// Every entry under `dir` on the node's own disk, at any depth: a file by its path
/// below `dir` against its length and checksum, as [`audit`] records one, and a
/// directory by its path and a trailing `/` against `(0, 0)`.
///
/// [`audit`] reads `dir`'s own files and skips what is not one, so a directory created
/// inside `dir` — a staging directory, a version directory — and every byte written into
/// it are invisible to it. This descends. The simulated disk keeps a directory once made
/// (nothing removes one), so a directory created and later emptied is still listed.
fn audit_tree(sim: &mut Sim, at: NodeId, dir: &Path) -> Option<DirAudit> {
    let found: Arc<Mutex<DirAudit>> = Arc::new(Mutex::new(DirAudit::new()));
    let done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let out = found.clone();
    let finished = done.clone();
    let env = sim.env(at);
    let inner = env.clone();
    let root = dir.to_path_buf();
    env.spawn("audit", async move {
        // Paths below `root`, the empty one being `root` itself.
        let mut pending: Vec<PathBuf> = vec![PathBuf::new()];
        while let Some(below) = pending.pop() {
            let here = if below.as_os_str().is_empty() {
                root.clone()
            } else {
                root.join(&below)
            };
            let Ok(listed) = inner.fs().read_dir(&here).await else {
                return;
            };
            for entry in listed {
                let Some(name) = entry.file_name() else {
                    continue;
                };
                let child = below.join(name);
                let path = root.join(&child);
                let shown = child.to_string_lossy().into_owned();
                match inner.fs().open(&path, OpenOptions::new().read(true)).await {
                    Ok(file) => {
                        let Ok(size) = file.size().await else {
                            continue;
                        };
                        let bytes = file
                            .read_at(0, usize::try_from(size).unwrap_or(usize::MAX))
                            .await
                            .unwrap_or_default();
                        out.lock()
                            .expect("the audit")
                            .insert(shown, (size, checksum(&bytes)));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::IsADirectory => {
                        out.lock()
                            .expect("the audit")
                            .insert(format!("{shown}/"), (0, 0));
                        pending.push(child);
                    }
                    Err(_) => {}
                }
            }
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

// The re-seed after a split: a right half back through a placeholder.

/// The range the re-seed after a split splits: the first user range, `[k0, k2)`.
pub const SPLIT_PARENT: u64 = FIRST_RANGE;
/// The key it splits at: the right half is `[k1, k2)`.
pub const SPLIT_KEY: &str = "k1";
/// The key past the right half's span.
const RIGHT_END: &str = "k2";
/// The key the run's writer writes, P's left half's, so that P's log goes on growing past
/// the split and its leader takes and compacts past it.
const LEFT_KEY: &str = "k0";
/// How long the three run before the split is asked: every range elects.
const SPLIT_SETTLE: Duration = Duration::from_secs(2);
/// The longest the run waits for the split to be traced, and then for `x` to apply it.
const SPLIT_BUDGET: Duration = Duration::from_secs(4);
/// The longest the run waits, `x` down, for P's leader to compact past the split, after
/// [`GONE`] has passed.
const SPLIT_COMPACT_BUDGET: Duration = Duration::from_secs(8);
/// The slice the run advances in while it waits for something.
const SPLIT_SLICE: Duration = Duration::from_millis(50);
/// How long the run goes on after `x` starts again on its lost store: its re-seed
/// streams, and its right half's stream to its placeholder.
const SPLIT_AFTER: Duration = Duration::from_secs(8);
/// How many keys the right half's writer writes, one put a key, resent until it is
/// answered: a key a resend applies a second time can hide its own loss (D-128 §5).
// PROPOSED(D-128)
const RIGHT_KEYS: u64 = 100;
/// The gap between two of its writes, so its keys span the split and `x`'s crash and the
/// right half's snapshot stays a few dozen chunks ([`SNAPSHOT_CHUNK`]).
// PROPOSED(D-128)
const RIGHT_EVERY: Duration = Duration::from_millis(40);
/// The key the split after the re-seed splits P's left half at: its right half is
/// `[k0a, k1)`. P's left half and not another range because `x` got it back by a stream,
/// which carries its descriptor; a range the log rebuilt holds none, and its next split
/// fails state machine safety on `x` (#199).
// PROPOSED(D-117)
const LEFT_SPLIT_KEY: &str = "k0a";
/// The key the writer of that right half writes.
// PROPOSED(D-117)
const LEFT_RIGHT_KEY: &str = "k0b";
/// The longest the run waits for `x` to apply the split after its re-seed.
// PROPOSED(D-117)
const SECOND_SPLIT_BUDGET: Duration = Duration::from_secs(4);
/// How long the new right half is given to elect before the run asks who leads it.
// PROPOSED(D-117)
const SECOND_SPLIT_SETTLE: Duration = Duration::from_millis(600);
/// How often the run crashes that right half's leader, if it is not `x`, and how long it
/// gives the next election.
// PROPOSED(D-117)
const LEADER_TRIES: usize = 12;
// PROPOSED(D-117)
const LEADER_TRY: Duration = Duration::from_millis(1200);
/// How long `x` is written to as that right half's leader: past its threshold several
/// times over, so that it takes several checkpoints and sweeps the earlier ones.
// PROPOSED(D-117)
const TAKES_FOR: Duration = Duration::from_secs(5);

/// What one run of the re-seed after a split built and traced
/// ([`run_after_split`]).
#[derive(Clone, Debug)]
pub struct AfterSplit {
    /// The seed.
    pub seed: u64,
    /// The run's trace.
    pub records: Vec<TraceRecord>,
    /// The follower of P that applied the split, and was then refused and re-seeded.
    pub x: u64,
    /// The split: its index and the right half's id.
    pub split: Option<(u64, u64)>,
    /// What the run could not build, if anything.
    pub unbuilt: Option<String>,
    /// The user keys of the right half's span in `x`'s engine once the run is over,
    /// read off its disk with the node stopped (`keys_on_disk`). `None` where the run
    /// was not built or the read did not finish.
    // PROPOSED(D-128)
    pub right_keys_on_x: Option<BTreeSet<Bytes>>,
    /// `x`'s engine directory, the one its restart refuses, as an audit found it with
    /// `x` down just before that restart, or `None` if the audit did not finish.
    pub refused_before: Option<DirAudit>,
    /// The same directory at the run's end, `x` crashed so that nothing on it is
    /// writing, or `None` if the audit did not finish.
    pub refused_at_end: Option<DirAudit>,
    /// Whether that directory's marker still says lost at the end.
    pub still_lost: bool,
    /// The directory `x`'s re-seed built and the rest of its start runs on, beside the
    /// refused one, as an audit found it at the end with `x` crashed, or `None` if the
    /// audit did not finish.
    pub fresh_at_end: Option<DirAudit>,
    /// The right half of the split after the re-seed, where this run asked for one and
    /// `x` applied it ([`run_split_after_reseed`]).
    pub second_right: Option<u64>,
}

impl AfterSplit {
    /// Whether `record` is `x`'s: a scenario's node ids are its server ids.
    fn on_x(&self, record: &TraceRecord) -> bool {
        record.node.map(|node| u64::from(node.get())) == Some(self.x)
    }

    /// Where `x`'s refusal stands in the trace.
    #[must_use]
    pub fn refused_at(&self) -> Option<usize> {
        self.records.iter().position(|record| {
            matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == self.x)
        })
    }

    /// Whether `x` created `range` before its refusal: the right half `x`'s own apply of
    /// the split made, which its refused engine held.
    #[must_use]
    pub fn held_before_refusal(&self, range: u64) -> bool {
        let refused = self.refused_at().unwrap_or(self.records.len());
        self.records[..refused].iter().any(|record| {
            self.on_x(record)
                && matches!(&record.event, TraceEvent::RangeCreated { range: r, .. } if *r == range)
        })
    }

    /// Whether `x` made a placeholder of `range` after its refusal.
    #[must_use]
    pub fn placeholder_after_refusal(&self, range: u64) -> bool {
        let Some(refused) = self.refused_at() else {
            return false;
        };
        self.records[refused..].iter().any(|record| {
            self.on_x(record)
                && matches!(record.event, TraceEvent::RangeReplicaCreated { range: r } if r == range)
        })
    }

    /// `x`'s creation of `range` by an install after its refusal: where it stands, and the
    /// generation and span it names.
    // PROPOSED(D-115): reads the one search below.
    #[must_use]
    pub fn installed_after_refusal(&self, range: u64) -> Option<(usize, u64, Bytes, Bytes)> {
        self.created_by_install_after_refusal(range)
            .map(|(at, _, held)| (at, held.generation, held.start, held.end))
    }

    /// `x`'s first creation of `range` by an install after its refusal, its `RangeCreated
    /// { cause: snapshot }`: where it stands, its floor index, and the descriptor it names,
    /// as check 7 holds a creation's.
    // PROPOSED(D-115)
    #[must_use]
    pub fn created_by_install_after_refusal(&self, range: u64) -> Option<(usize, u64, Held)> {
        let refused = self.refused_at()?;
        self.records
            .iter()
            .enumerate()
            .skip(refused)
            .find_map(|(at, record)| match &record.event {
                TraceEvent::RangeCreated {
                    range: r,
                    cause: RangeCause::Snapshot,
                    start,
                    end,
                    generation,
                    voters,
                    floor_index,
                    ..
                } if *r == range && self.on_x(record) => Some((
                    at,
                    *floor_index,
                    Held {
                        start: start.clone(),
                        end: end.clone(),
                        generation: *generation,
                        voters: voters.clone(),
                        state: RangeState::Live,
                    },
                )),
                _ => None,
            })
    }

    /// Whether `x` stated `range` quarantined after record `at`: its `RaftReseeded` of the
    /// range, and a `RaftRecovered` of it saying `Quarantined` (D-035, D-081).
    #[must_use]
    pub fn quarantined_after(&self, range: u64, at: usize) -> bool {
        let after = &self.records[at..];
        let marked = after.iter().any(|record| {
            matches!(record.event, TraceEvent::RaftReseeded { server, range: r }
                if server == self.x && r == range)
        });
        let stated = after.iter().any(|record| {
            matches!(record.event, TraceEvent::RaftRecovered { server, range: r, state, .. }
                if server == self.x && r == range && state == RecoveredAs::Quarantined)
        });
        marked && stated
    }

    /// Issue #172's clause, per seed, asked after [`check`](Self::check): `x`'s re-seed
    /// creates P by an install at the split's generation over P's left half, `[k0, k1)`,
    /// and the creation equals the value check 7's map holds for P at or below its floor.
    ///
    /// The second half makes check 7's comparison again here; it does not show that check
    /// 7 made it. That check 7 judges an install's creation is held by its own unit test,
    /// `an_install_must_agree_with_the_descriptor_in_force_at_its_index`, in `ananke-shard`'s
    /// `invariants.rs`, and [`check`](Self::check) runs check 7 on this trace first. The
    /// comparison adds the voters, which [`reached`](Self::reached) does not read and only
    /// check 7 reads of this creation, and a fault check 7 would miss. In this run the
    /// split's value of P, at generation 2, lies at or below the floor on every seed
    /// (measured 200 of 200), so neither of check 7's waits arises here.
    ///
    /// An install's creation names the descriptor its snapshot put in place (PROPOSED
    /// D-107 §2). With the first generation put back, whatever the snapshot held, check 7
    /// fails this run on every seed (PROPOSED D-115).
    ///
    /// # Errors
    ///
    /// What does not hold, with the seed.
    // PROPOSED(D-115)
    pub fn parent_created_at_the_splits_generation(&self) -> Result<(), String> {
        let (seed, x) = (self.seed, self.x);
        let Some((_, floor, created)) = self.created_by_install_after_refusal(SPLIT_PARENT) else {
            return Err(format!(
                "seed {seed}: node {x}'s re-seed created no replica of range {SPLIT_PARENT} \
                 by an install"
            ));
        };
        let left = (
            ananke_raft::apply::user_key(LEFT_KEY.as_bytes()),
            ananke_raft::apply::user_key(SPLIT_KEY.as_bytes()),
        );
        if created.generation != FIRST_GENERATION + 1
            || (created.start.clone(), created.end.clone()) != left
        {
            return Err(format!(
                "seed {seed}: node {x}'s install created range {SPLIT_PARENT} at its floor \
                 {floor} with {created:?}, not at the split's generation {} over its left \
                 half [{:?}, {:?})",
                FIRST_GENERATION + 1,
                left.0,
                left.1
            ));
        }
        let checker = ananke_shard::invariants::folded(&self.records);
        match checker.descriptors().at(SPLIT_PARENT, floor) {
            Some(value) if *value == created => Ok(()),
            value => Err(format!(
                "seed {seed}: node {x}'s install creation of range {SPLIT_PARENT} at its floor \
                 {floor} names {created:?}, and check 7's value there is {value:?}"
            )),
        }
    }

    /// Whether `x`'s install of the right half landed before its install of P: the
    /// order in which P's install meets a replica that holds keys of the span
    /// configuration names for P. Both are creations, since `x`'s engine is fresh. It
    /// reads the creations' records in the trace; a run that has not both reads `false`.
    // PROPOSED(D-128)
    #[must_use]
    pub fn right_installed_first(&self) -> bool {
        let Some((_, right)) = self.split else {
            return false;
        };
        match (
            self.installed_after_refusal(right),
            self.installed_after_refusal(SPLIT_PARENT),
        ) {
            (Some((right_at, ..)), Some((parent_at, ..))) => right_at < parent_at,
            _ => false,
        }
    }

    /// The floor of `x`'s creation of `range` by an install: the snapshot's last index.
    // PROPOSED(D-128)
    #[must_use]
    pub fn install_floor(&self, range: u64) -> Option<u64> {
        self.records.iter().find_map(|record| match &record.event {
            TraceEvent::RangeCreated {
                range: r,
                cause: RangeCause::Snapshot,
                floor_index,
                ..
            } if *r == range && self.on_x(record) => Some(*floor_index),
            _ => None,
        })
    }

    /// The keys `x`'s install of the right half owes its span: every key of the span a
    /// replica applied below the split, in P's log, or at or below the install's floor,
    /// in the right half's own. The snapshot carries each of them, so each is in the
    /// switch that created the replica, and the writer writes each key once, so nothing
    /// after the switch can put one back.
    // PROPOSED(D-128)
    #[must_use]
    pub fn right_keys_owed(&self) -> BTreeSet<Bytes> {
        let Some((split, right)) = self.split else {
            return BTreeSet::new();
        };
        let Some(floor) = self.install_floor(right) else {
            return BTreeSet::new();
        };
        let span = SPLIT_KEY.as_bytes()..RIGHT_END.as_bytes();
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftApply {
                    range,
                    index,
                    key: Some(key),
                    effect: ApplyEffect::Applied,
                    ..
                } if ((*range == SPLIT_PARENT && *index < split)
                    || (*range == right && *index <= floor))
                    && span.contains(&&key[..]) =>
                {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// The right half's keys survive on `x`, whichever of its two installs landed first
    /// (SHARD.md §5, §11 storage 5): every key its install owes the span is in `x`'s
    /// engine when the run is over. The traced descriptors say nothing of this: an
    /// install's switch removes keys, and the only record of which is the engine's.
    ///
    /// # Errors
    ///
    /// What is missing, with the seed.
    // PROPOSED(D-128)
    pub fn right_keys_survive(&self) -> Result<(), String> {
        let (seed, x) = (self.seed, self.x);
        let Some(held) = &self.right_keys_on_x else {
            return Err(format!(
                "seed {seed}: node {x}'s engine was not read after the run"
            ));
        };
        let owed = self.right_keys_owed();
        if owed.is_empty() {
            return Err(format!(
                "seed {seed}: node {x}'s install of the right half owed its span no key, so \
                 this run says nothing of them"
            ));
        }
        let missing: Vec<&Bytes> = owed.difference(held).collect();
        if missing.is_empty() {
            return Ok(());
        }
        Err(format!(
            "seed {seed}: node {x} lost {} of the {} keys its install of the right half owes \
             its span (the right half installed {} P), first {:?}: its engine holds {} keys \
             of the span",
            missing.len(),
            owed.len(),
            if self.right_installed_first() {
                "before"
            } else {
                "after"
            },
            missing[0],
            held.len()
        ))
    }

    /// What `x` read back of `range`'s install holds what the take that fed it held
    /// (D-083): the state it traced at its switch, paired with another server's take by
    /// `(range, last_index, last_term)`. P's replica on `x` has no descriptor in the
    /// host's table until the host handles the switch, and until PROPOSED D-128 its
    /// read-back found none and traced nothing, so this creation alone went uncompared.
    /// The read is the snapshot's span and not configuration's: the right half's keys, if
    /// its install landed first, are no part of what P's take held.
    ///
    /// # Errors
    ///
    /// What does not hold, with the seed.
    // PROPOSED(D-128)
    pub fn state_read_back_is_the_takes(&self, range: u64) -> Result<(), String> {
        let (seed, x) = (self.seed, self.x);
        let Some(floor) = self.install_floor(range) else {
            return Err(format!(
                "seed {seed}: node {x} created no replica of range {range} by an install"
            ));
        };
        let state = |record: &TraceRecord| match record.event {
            TraceEvent::RaftSnapshotState {
                server,
                range: r,
                last_index,
                last_term,
                user_keys,
                user_digest,
                ..
            } if r == range => Some((server, last_index, last_term, user_keys, user_digest)),
            _ => None,
        };
        // The install's own state, by its last index: a later take of the range on `x`
        // traces the same event, and would answer for an install that traced none.
        let refused = self.refused_at().unwrap_or(0);
        let Some((_, last_index, last_term, user_keys, user_digest)) = self.records[refused..]
            .iter()
            .filter_map(state)
            .find(|(server, index, ..)| *server == x && *index == floor)
        else {
            return Err(format!(
                "seed {seed}: node {x}'s install of range {range} traced no state read back"
            ));
        };
        let taken = self
            .records
            .iter()
            .filter_map(state)
            .find(|(server, index, term, ..)| {
                *server != x && (*index, *term) == (last_index, last_term)
            });
        match taken {
            Some((_, _, _, keys, digest)) if (keys, digest) == (user_keys, user_digest) => Ok(()),
            Some((server, _, _, keys, digest)) => Err(format!(
                "seed {seed}: node {x}'s install of range {range} at {last_index} read back \
                 {user_keys} keys, digest {user_digest:#x}, and server {server}'s take there \
                 held {keys} keys, digest {digest:#x}"
            )),
            None => Err(format!(
                "seed {seed}: no server's take of range {range} at {last_index} is traced for \
                 node {x}'s install"
            )),
        }
    }

    /// The votes and pre-votes `x` granted `range` after record `at`.
    #[must_use]
    pub fn granted_after(&self, range: u64, at: usize) -> usize {
        self.records[at..]
            .iter()
            .filter(|record| {
                matches!(record.event, TraceEvent::RaftVote { server, range: r, granted: true, .. }
                    if server == self.x && r == range)
            })
            .count()
    }

    /// The run's verdict: checks 1 to 4, the commit majority, the range layer's folds
    /// (checks 7 to 18) and no node stopped.
    ///
    /// # Errors
    ///
    /// The first violation, with the seed.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        if let Some((server, reason)) = self.records.iter().find_map(|record| match &record.event {
            TraceEvent::RaftServerFailed { server, reason } => Some((*server, reason.clone())),
            _ => None,
        }) {
            return Err(format!("seed {seed}: server {server} stopped: {reason}"));
        }
        if let Err(violation) = ananke_raft::invariants::all(crate::traced(&self.records)) {
            return Err(format!("seed {seed}: {violation}"));
        }
        if let Err(violation) = ananke_raft::invariants::commit_majority(
            crate::traced(&self.records),
            usize::try_from(SERVERS).expect("small"),
        ) {
            return Err(format!("seed {seed}: {violation}"));
        }
        if let Err(violation) = ananke_shard::invariants::all(&self.records) {
            return Err(format!("seed {seed}: {violation}"));
        }
        Ok(())
    }

    /// The case the run is for, per seed: `x` held the right half, was refused, and was
    /// created it again by its placeholder's install, after its P was installed past the
    /// split — P's install creation at the split's generation over the left half, which
    /// is what leaves `x` nothing to create the right half from but its placeholder.
    ///
    /// # Errors
    ///
    /// What was not built or not reached, with the seed.
    pub fn reached(&self) -> Result<(), String> {
        let seed = self.seed;
        let x = self.x;
        if let Some(why) = &self.unbuilt {
            return Err(format!(
                "seed {seed}: the re-seed after a split was not built: {why}"
            ));
        }
        let Some((_, right)) = self.split else {
            return Err(format!("seed {seed}: no split was traced"));
        };
        if self.refused_at().is_none() {
            return Err(format!("seed {seed}: node {x} was never refused"));
        }
        if !self.held_before_refusal(right) {
            return Err(format!(
                "seed {seed}: node {x} never created the right half {right} before its \
                 refusal, so it lost nothing of it"
            ));
        }
        let left_end = ananke_raft::apply::user_key(SPLIT_KEY.as_bytes());
        match self.installed_after_refusal(SPLIT_PARENT) {
            Some((_, generation, _, end))
                if generation == FIRST_GENERATION + 1 && end == left_end => {}
            other => {
                return Err(format!(
                    "seed {seed}: node {x}'s re-seed of range {SPLIT_PARENT} was not an install \
                     of its left half at the split's generation: {other:?}"
                ));
            }
        }
        if !self.placeholder_after_refusal(right) {
            return Err(format!(
                "seed {seed}: node {x} made no placeholder of the right half {right} after \
                 its refusal"
            ));
        }
        match self.installed_after_refusal(right) {
            Some((_, generation, start, _))
                if generation == FIRST_GENERATION + 1 && start == left_end => {}
            other => {
                return Err(format!(
                    "seed {seed}: node {x} was not created the right half {right} by its \
                     placeholder's install at the split's generation: {other:?}"
                ));
            }
        }
        Ok(())
    }

    /// D-041 on a node that is re-seeded and runs on: `x`'s refused directory still says
    /// lost at the end, and not one entry of it, at any depth, changed between the audit
    /// before `x`'s restart and the one at the end, both with `x` down, other than the
    /// marker itself, which the refusal writes (D-044).
    ///
    /// The re-seed shape's (e) asks the same of its refused directory, from the arm's
    /// crash on; but the arm restarts the node within milliseconds of its re-seed, and
    /// from that start on the node opens the new directory (D-066), so the start that
    /// re-seeded never gets as far as a stream. Here `x` is not restarted after its
    /// re-seed, and that start receives two streams, its re-seed's of P and its right
    /// half's to its placeholder. Before PROPOSED D-117 both staged, synced and were
    /// cleared in the refused directory, and this failed on every seed (#176).
    ///
    /// # Errors
    ///
    /// Naming every entry that changed, with the seed.
    // PROPOSED(D-117): the start's tasks run on the directory the re-seed built.
    pub fn refused_untouched(&self) -> Result<(), String> {
        let seed = self.seed;
        let x = self.x;
        if !self.still_lost {
            return Err(format!(
                "seed {seed}: node {x}'s refused directory's marker no longer says lost"
            ));
        }
        let Some(changed) = self.refused_changed() else {
            return Err(format!(
                "seed {seed}: node {x}'s refused directory could not be read within \
                 {AUDIT_BUDGET:?}, so the run says nothing about whether it changed"
            ));
        };
        if changed.is_empty() {
            return Ok(());
        }
        Err(format!(
            "seed {seed}: node {x}'s refused directory changed after its refusal: \
             {changed:?}. A directory that held a store is marked lost and never written \
             to again (D-041, D-044)"
        ))
    }

    /// Every entry of `x`'s refused directory, at any depth, that differs between the audit
    /// before its restart and the one at the end, other than the refusal's own marker; or
    /// `None` if either audit did not finish.
    // PROPOSED(D-117)
    #[must_use]
    pub fn refused_changed(&self) -> Option<BTreeSet<String>> {
        let before = self.refused_before.as_ref()?;
        let at_end = self.refused_at_end.as_ref()?;
        Some(
            at_end
                .keys()
                .chain(before.keys())
                .filter(|name| name.as_str() != STORE_MARKER)
                .filter(|name| at_end.get(*name) != before.get(*name))
                .cloned()
                .collect(),
        )
    }

    /// Where the start that re-seeded staged its streams and cleared them: the directory
    /// it runs on. For P's re-seed and for the right half's install, the directory the
    /// re-seed built holds a staging directory of that range, so
    /// [`Self::refused_untouched`] is asserted of runs whose streams staged somewhere,
    /// and the somewhere is named. And that directory is empty: the `snapshot` task's
    /// `clear_staging` lists the directory it was handed and clears the install's staging
    /// there, so a task left on the refused directory finds nothing to clear and leaves
    /// every file here.
    ///
    /// Measured on the 200 seeds this runs at the cap: two staging directories and not
    /// one file in either, on every seed. A stream the run's end cut short, or the
    /// assembly of a sender its range superseded, would leave files without any bug; if a
    /// schedule ever reaches that, this says so here and the clause needs to read which
    /// assembly the install came from.
    ///
    /// # Errors
    ///
    /// Naming the range with no staging directory there, or the files left in one, with
    /// the seed.
    // PROPOSED(D-117): the start's tasks run on the directory the re-seed built.
    pub fn staged_where_it_runs(&self) -> Result<(), String> {
        let seed = self.seed;
        let x = self.x;
        let fresh = fresh_dir();
        let Some(at_end) = &self.fresh_at_end else {
            return Err(format!(
                "seed {seed}: node {x}'s directory {} could not be read within \
                 {AUDIT_BUDGET:?}",
                fresh.display()
            ));
        };
        let Some((_, right)) = self.split else {
            return Err(format!("seed {seed}: no split was traced"));
        };
        for range in [SPLIT_PARENT, right] {
            let prefix = format!("staging-r{range}-s");
            let staged = at_end
                .keys()
                .any(|name| name.starts_with(&prefix) && name.ends_with('/'));
            if !staged {
                return Err(format!(
                    "seed {seed}: node {x} installed range {range} after its re-seed and \
                     {} holds no staging directory of it, so its stream staged \
                     somewhere other than the directory the node runs on: {:?}",
                    fresh.display(),
                    at_end
                        .keys()
                        .filter(|name| name.ends_with('/'))
                        .collect::<Vec<_>>()
                ));
            }
            let left: Vec<&String> = at_end
                .keys()
                .filter(|name| name.starts_with(&prefix) && !name.ends_with('/'))
                .collect();
            if !left.is_empty() {
                return Err(format!(
                    "seed {seed}: node {x} installed range {range} after its re-seed and left \
                     {left:?} in {}: an install clears its own staging there",
                    fresh.display()
                ));
            }
        }
        Ok(())
    }

    /// `x`'s takes and sweeps after a split of P's left half that followed its re-seed
    /// ([`run_split_after_reseed`]), in the directory it runs on: `x` led the new right
    /// half and took at least two checkpoints of it; each take's version directory is in
    /// the directory the re-seed built, and none is in the refused one; and a sweep has
    /// emptied an earlier version there.
    ///
    /// # Errors
    ///
    /// What the run did not reach, or where the versions went, with the seed.
    // PROPOSED(D-117): the start's tasks run on the directory the re-seed built.
    pub fn took_where_it_runs(&self) -> Result<(), String> {
        let (seed, x) = (self.seed, self.x);
        let Some(right) = self.second_right else {
            return Err(format!(
                "seed {seed}: node {x} did not apply a split of P's left half after its re-seed"
            ));
        };
        let led = self.records.iter().any(|record| {
            matches!(record.event, TraceEvent::RaftLeader { server, range, .. }
                if server == x && range == right)
        });
        let takes = self.takes_of(right);
        if !led || takes < 2 {
            return Err(format!(
                "seed {seed}: node {x} led the right half {right} made after its re-seed: {led}, \
                 and took {takes} checkpoints of it, where this asks for two so that a sweep \
                 has an earlier one to empty"
            ));
        }
        let (Some(fresh), Some(refused)) = (&self.fresh_at_end, &self.refused_at_end) else {
            return Err(format!(
                "seed {seed}: node {x}'s directories could not be read within {AUDIT_BUDGET:?}"
            ));
        };
        let versions = versions_of(fresh, right);
        let elsewhere = versions_of(refused, right);
        if versions.len() < takes || !elsewhere.is_empty() {
            return Err(format!(
                "seed {seed}: node {x} took {takes} checkpoints of range {right}, and the \
                 directory it runs on holds {} version directories of it where the one it \
                 refused holds {elsewhere:?}: a take writes where the node runs (D-041)",
                versions.len()
            ));
        }
        // The newest is left out: the crash that ends the run may have cut its take short,
        // which leaves an empty directory no sweep emptied. A task left on the refused
        // directory passes on 3 seeds of 20 without this.
        let newest = versions.iter().filter_map(|version| take_in(version)).max();
        let swept = versions
            .iter()
            .filter(|version| take_in(version) != newest)
            .filter(|version| {
                !fresh
                    .keys()
                    .any(|name| name.len() > version.len() && name.starts_with(version.as_str()))
            })
            .count();
        if swept == 0 {
            return Err(format!(
                "seed {seed}: node {x} took {takes} checkpoints of range {right} and none of \
                 its {} version directories but the newest was swept empty in the directory \
                 it runs on",
                versions.len()
            ));
        }
        Ok(())
    }

    /// The checkpoints `x` took of `range`, by their trace: each a checkpoint written and
    /// its `RaftSnapshot { taken: true }` ([`crate::raft::snapshot_takes_of`]), so a
    /// restart restating the record of a take, which says `taken: true` too, is not one.
    // PROPOSED(D-117)
    // PROPOSED(D-118): a restart restating the record of a take is not a take.
    #[must_use]
    pub fn takes_of(&self, range: u64) -> usize {
        crate::raft::snapshot_takes_of(&self.records)
            .iter()
            .filter(|take| take.server == self.x && take.range == range)
            .count()
    }
}

/// The take a version directory's name carries, as an audit lists it.
// PROPOSED(D-117)
fn take_in(version: &str) -> Option<u64> {
    parse_version(version.trim_end_matches('/')).map(|(_, _, take)| take)
}

/// The version directories of `range` an audit found, by their names.
// PROPOSED(D-117)
#[must_use]
pub fn versions_of(audit: &DirAudit, range: u64) -> BTreeSet<String> {
    let prefix = format!("snap-r{range}-");
    audit
        .keys()
        .filter(|name| name.starts_with(&prefix) && name.ends_with('/'))
        .cloned()
        .collect()
}

/// The run's writer: puts to P's left half's key, routed by the scenario's fixed map,
/// which the split leaves true for that key, and following `NotLeader`.
async fn left_writer<E: Environment>(env: E) {
    let Ok(sock) = env.net().bind(client_addr(1)).await else {
        return;
    };
    let mut leader: Option<u64> = None;
    let mut seq = 0u64;
    loop {
        seq += 1;
        let target = leader.unwrap_or_else(|| 1 + env.rng().below(SERVERS));
        let request = RangedRequest {
            generation: FIRST_GENERATION,
            range: RangeId(SPLIT_PARENT),
            request: Request {
                client: 1,
                seq,
                command: Command::Put {
                    key: Bytes::from(LEFT_KEY),
                    value: Bytes::from(format!("v{seq}")),
                },
            },
        };
        let _ = sock.send(server_addr(target), request.encode()).await;
        let deadline = env.clock().now() + Duration::from_millis(80);
        loop {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep_until(deadline));
            let bytes = match race(&env, recv, timer).await {
                Either::Left(Ok((_, bytes))) => bytes,
                Either::Left(Err(_)) | Either::Right(()) => {
                    leader = None;
                    break;
                }
            };
            let Ok(response) = RangedResponse::decode(bytes) else {
                continue;
            };
            if response.response.seq != seq {
                continue;
            }
            leader = match response.response.reply {
                Reply::NotLeader { leader: Some(who) } => Some(who.0),
                Reply::NotLeader { leader: None } | Reply::RangeMismatch { .. } => None,
                Reply::Outcome(_) => Some(target),
            };
            break;
        }
        env.clock().sleep(Duration::from_millis(4)).await;
    }
}

/// The run's second writer: [`RIGHT_KEYS`] keys of the right half's span, each written
/// once and in order, each resent until a node answers it. It asks P until a node's
/// `RangeMismatch` names the range that holds the key, as a client's cache would learn
/// it (SHARD.md §3), which needs no fixed map for a range the run's split makes.
// PROPOSED(D-128)
async fn right_writer<E: Environment>(env: E) {
    let Ok(sock) = env.net().bind(client_addr(2)).await else {
        return;
    };
    let mut leader: Option<u64> = None;
    let mut range = RangeId(SPLIT_PARENT);
    let mut generation = FIRST_GENERATION;
    for seq in 1..=RIGHT_KEYS {
        let key = Bytes::from(format!("{SPLIT_KEY}-{seq:04}"));
        let encoded = ananke_raft::apply::user_key(&key);
        loop {
            let target = leader.unwrap_or_else(|| 1 + env.rng().below(SERVERS));
            let request = RangedRequest {
                generation,
                range,
                request: Request {
                    client: 2,
                    seq,
                    command: Command::Put {
                        key: key.clone(),
                        value: Bytes::from(format!("v{seq}")),
                    },
                },
            };
            let _ = sock.send(server_addr(target), request.encode()).await;
            let deadline = env.clock().now() + Duration::from_millis(80);
            let answered = loop {
                let recv = pin!(sock.recv());
                let timer = pin!(env.clock().sleep_until(deadline));
                let bytes = match race(&env, recv, timer).await {
                    Either::Left(Ok((_, bytes))) => bytes,
                    Either::Left(Err(_)) | Either::Right(()) => {
                        leader = None;
                        break false;
                    }
                };
                let Ok(response) = RangedResponse::decode(bytes) else {
                    continue;
                };
                if response.response.seq != seq {
                    continue;
                }
                match response.response.reply {
                    Reply::Outcome(_) => {
                        leader = Some(target);
                        break true;
                    }
                    Reply::NotLeader { leader: who } => {
                        leader = who.map(|who| who.0);
                        break false;
                    }
                    Reply::RangeMismatch { descriptors } => {
                        let holder = descriptors.into_iter().find_map(|bytes| {
                            RangeDescriptor::decode(bytes).ok().filter(|descriptor| {
                                descriptor.start <= encoded && encoded < descriptor.end
                            })
                        });
                        if let Some(holder) = holder {
                            range = holder.range;
                            generation = holder.generation;
                        }
                        leader = None;
                        break false;
                    }
                }
            };
            if answered {
                break;
            }
            env.clock().sleep(Duration::from_millis(4)).await;
        }
        env.clock().sleep(RIGHT_EVERY).await;
    }
}

/// The user keys of `[from, to)` in the engine at `dir` on `at`, read off its disk: the
/// node must be down. `None` where the read did not finish inside [`AUDIT_BUDGET`].
// PROPOSED(D-128)
fn keys_on_disk(
    sim: &mut Sim,
    at: NodeId,
    dir: &Path,
    from: Bytes,
    to: Bytes,
) -> Option<BTreeSet<Bytes>> {
    let found: Arc<Mutex<Option<BTreeSet<Bytes>>>> = Arc::new(Mutex::new(None));
    let out = found.clone();
    let env = sim.env(at);
    let inner = env.clone();
    let dir = dir.to_path_buf();
    env.spawn("keys-on-disk", async move {
        let Ok((engine, _)) = Engine::open(inner, EngineConfig::new(dir)).await else {
            return;
        };
        let version = engine.snapshot();
        if let Ok(scanned) = engine.scan(&from[..]..&to[..], &version).await {
            *out.lock().expect("the read") = Some(
                scanned
                    .into_iter()
                    .filter_map(|(key, _)| {
                        ananke_raft::apply::user_key_of(&key).map(Bytes::copy_from_slice)
                    })
                    .collect(),
            );
        }
    });
    let mut waited = Duration::ZERO;
    while waited < AUDIT_BUDGET && found.lock().expect("the read").is_none() {
        sim.run_for(AUDIT_STEP);
        waited += AUDIT_STEP;
    }
    found.lock().expect("the read").take()
}

/// The operator's split of P at `key`, asked of `target` and following `NotLeader`; it
/// records nothing, since the trace says whether the split took effect.
///
/// `operator` says which: each split is asked on an address of its own and by a client of
/// its own, since a replica does not propose a copy of a request it proposed, by client and
/// sequence number (`Replica::proposed`).
async fn split_operator<E: Environment>(env: E, mut target: u64, operator: u64, key: &'static str) {
    let Ok(sock) = env.net().bind(crate::raft::admin_addr(operator)).await else {
        return;
    };
    for _ in 0..SERVERS * 4 {
        let request = RangedRequest {
            generation: FIRST_GENERATION,
            range: RangeId(SPLIT_PARENT),
            request: Request {
                client: (u64::MAX >> 2) - (operator - 1),
                seq: 1,
                command: Command::Split {
                    key: Bytes::from(key),
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
            target = target % SERVERS + 1;
            continue;
        };
        let Ok(answer) = RangedResponse::decode(bytes) else {
            continue;
        };
        match answer.response.reply {
            Reply::NotLeader { leader: Some(who) } => target = who.0,
            Reply::NotLeader { leader: None } => {
                env.clock().sleep(Duration::from_millis(50)).await;
                target = target % SERVERS + 1;
            }
            Reply::Outcome(_) | Reply::RangeMismatch { .. } => return,
        }
    }
}

/// A writer of one key of `range`, sent straight to `to` and never read back: it exists
/// to grow that replica's log while it leads. Client 3, on its own address: client 2 is
/// [`right_writer`], which may still be retrying its last key.
// PROPOSED(D-117)
async fn second_right_writer<E: Environment>(env: E, range: u64, to: u64, key: &'static str) {
    let Ok(sock) = env.net().bind(client_addr(3)).await else {
        return;
    };
    let mut seq = 0u64;
    loop {
        seq += 1;
        let request = RangedRequest {
            generation: FIRST_GENERATION,
            range: RangeId(range),
            request: Request {
                client: 3,
                seq,
                command: Command::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(format!("v{seq}")),
                },
            },
        };
        let _ = sock.send(server_addr(to), request.encode()).await;
        env.clock().sleep(Duration::from_millis(4)).await;
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

/// The split of P the trace names, by its first `RangeSplit`: its index and right half.
fn split_of(records: &[TraceRecord]) -> Option<(u64, u64)> {
    records.iter().find_map(|record| match &record.event {
        TraceEvent::RangeSplit {
            range,
            right,
            index,
            ..
        } if *range == SPLIT_PARENT => Some((*index, *right)),
        _ => None,
    })
}

/// Runs the re-seed after a split for `seed` on the correct system.
///
/// The three servers of the re-seed shape and a writer on P's left half's key. P, the
/// first user range, is split at [`SPLIT_KEY`] by an operator once every range has
/// elected. A follower `x` of P, drawn on the run's own stream, applies the split — it
/// creates the right half R and holds it — and is crashed; it stays down for `GONE`
/// and until P's leader has compacted past the split, and starts again on a store
/// marked lost, so it is refused and re-seeded (Q15). The re-seed rebuilds the replicas
/// configuration names. R is not one of them and the fresh engine holds no key of it,
/// so R comes back to `x` only through its placeholder: R's leader reaches it, and is
/// told to stream (SHARD.md §5, Q22). `x` may have voted in R before its loss, and the
/// replica its placeholder's install creates is quarantined (PROPOSED D-109 §2).
///
/// A second writer puts keys of the right half's span, one put a key, through the split
/// and `x`'s loss. Which of `x`'s two installs lands first is the schedule's draw: when
/// the right half's does, P's install finds a replica holding keys of the span
/// configuration names for P, and when the run is over `x`'s engine is read off its disk
/// to say whether they are still there ([`AfterSplit::right_keys_survive`], PROPOSED
/// D-128).
#[must_use]
pub fn run_after_split(seed: u64) -> AfterSplit {
    run_after_split_under(seed, NodeVariants::correct())
}

/// [`run_after_split`] with the node's `node_variants`, on every server.
// PROPOSED(D-128)
#[must_use]
pub fn run_after_split_under(seed: u64, node_variants: NodeVariants) -> AfterSplit {
    after_split(seed, node_variants, false)
}

/// [`run_after_split`], and then a split of P's left half with `x` still in the start that
/// re-seeded.
///
/// Every replica the re-seed rebuilt and every replica a placeholder's install created is
/// quarantined and never campaigns (D-035, PROPOSED D-109 §2), and since D-130 so is a right
/// half a split's apply makes on one of them, this one included: the correct node does not
/// lead it, and nothing on a re-seeded node takes a checkpoint. Under
/// `NodeVariant::SplitLeavesQuarantineBehind` the split writes it no quarantine, as it did
/// when this run was built, and `x` can lead it. The run tries: whichever other node leads
/// that right half is crashed and the last one restarted, until `x` is elected. Then `x`
/// is written to past the threshold several times over, so under that variant it takes
/// checkpoints and sweeps the earlier ones, which is the `apply` task's and the `snapshot`
/// task's use of the directory no other run reaches (PROPOSED D-117; #176).
///
/// P's left half, which came back to `x` by a stream, and not a range the log rebuilt,
/// whose split fails state machine safety on `x` (#199).
// PROPOSED(D-117)
#[must_use]
pub fn run_split_after_reseed(seed: u64, node_variants: NodeVariants) -> AfterSplit {
    after_split(seed, node_variants, true)
}

/// The runs above; `split_again` is [`run_split_after_reseed`]'s.
fn after_split(seed: u64, node_variants: NodeVariants, split_again: bool) -> AfterSplit {
    let mut sim = Sim::new(config(seed));
    let nodes: Vec<NodeId> = (0..SERVERS as usize).map(|_| sim.add_node()).collect();
    let client = sim.add_node();
    let variants = Variants::default();
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
    {
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn("writer", left_writer(inner));
        let inner = env.clone();
        env.spawn("right-writer", right_writer(inner));
    }
    sim.run_for(SPLIT_SETTLE);
    let leader = leader_of(&sim.trace(), SPLIT_PARENT).unwrap_or(1);
    let mut draw = moirae_sched::stream(seed, "reseed-after-split");
    let followers: Vec<u64> = (1..=SERVERS).filter(|id| *id != leader).collect();
    let x = followers[usize::try_from(draw.below(followers.len() as u64)).expect("small")];
    let mut report = AfterSplit {
        seed,
        records: Vec::new(),
        x,
        split: None,
        unbuilt: None,
        right_keys_on_x: None,
        refused_before: None,
        refused_at_end: None,
        still_lost: false,
        fresh_at_end: None,
        second_right: None,
    };
    {
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn("split", split_operator(inner, leader, 1, SPLIT_KEY));
    }
    let deadline = sim.now() + SPLIT_BUDGET;
    let applied_by_x = |records: &[TraceRecord]| {
        records.iter().any(|record| {
            record.node.map(|node| u64::from(node.get())) == Some(x)
                && matches!(record.event, TraceEvent::RangeSplit { range, .. } if range == SPLIT_PARENT)
        })
    };
    loop {
        let records = sim.trace();
        report.split = split_of(&records);
        if report.split.is_some() && applied_by_x(&records) {
            break;
        }
        if sim.now() >= deadline {
            report.unbuilt = Some(format!(
                "node {x} did not apply a split of range {SPLIT_PARENT} within {SPLIT_BUDGET:?}"
            ));
            report.records = records;
            return report;
        }
        sim.run_for(SPLIT_SLICE);
    }
    let Some((index, _)) = report.split else {
        unreachable!("the loop above leaves only with a split");
    };
    // The loss: `x` crashed, kept down until P's leader has taken and compacted past the
    // split, and started again on a store marked lost, as the re-seed shape loses one.
    let at = nodes[x as usize - 1];
    sim.crash(at);
    sim.run_for(GONE);
    // P's leader, and not any replica: a follower compacts to its own applied index with
    // no checkpoint behind it (D-078), and `x` may have done so before its crash. A leader
    // whose snapshot is past the split streams `x` a version past it, which is what
    // leaves `x` its right half to get through its placeholder rather than by a replay of
    // the split.
    let compacted_by_leader = |records: &[TraceRecord]| {
        leader_of(records, SPLIT_PARENT).is_some_and(|leader| {
            records.iter().any(|record| {
                matches!(record.event, TraceEvent::RaftCompacted { server, range, through, .. }
                    if server == leader && range == SPLIT_PARENT && through > index)
            })
        })
    };
    let deadline = sim.now() + SPLIT_COMPACT_BUDGET;
    while !compacted_by_leader(&sim.trace()) {
        if sim.now() >= deadline {
            report.unbuilt = Some(format!(
                "range {SPLIT_PARENT}'s leader did not compact past the split at {index} \
                 within {SPLIT_COMPACT_BUDGET:?} of node {x}'s crash and {GONE:?}"
            ));
            report.records = sim.trace();
            return report;
        }
        sim.run_for(SPLIT_SLICE);
    }
    // D-041's baseline: the directory the restart refuses, read with `x` down. The
    // refusal's own marker is written after it, and nothing else may be (PROPOSED D-117).
    report.refused_before = audit_tree(&mut sim, at, Path::new(DIR));
    sim.restart(at);
    spawn(&sim, at, x, variants, node_variants, true);
    sim.run_for(SPLIT_AFTER);
    if split_again {
        // PROPOSED(D-117): a right half made after the re-seed, led by `x`, written past
        // its threshold.
        let before = rights_on(&sim.trace(), x).len();
        let target = leader_of(&sim.trace(), SPLIT_PARENT).unwrap_or(1);
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn(
            "split-again",
            split_operator(inner, target, 2, LEFT_SPLIT_KEY),
        );
        let deadline = sim.now() + SECOND_SPLIT_BUDGET;
        while sim.now() < deadline && report.second_right.is_none() {
            sim.run_for(SPLIT_SLICE);
            report.second_right = rights_on(&sim.trace(), x).get(before).copied();
        }
        if let Some(right) = report.second_right {
            sim.run_for(SECOND_SPLIT_SETTLE);
            let mut down = None;
            for _ in 0..LEADER_TRIES {
                let leader = leader_of(&sim.trace(), right);
                if leader == Some(x) {
                    break;
                }
                if let Some(id) = down.take() {
                    bring_back(&mut sim, &nodes, id, variants, node_variants);
                }
                if let Some(id) = leader {
                    sim.crash(nodes[id as usize - 1]);
                    down = Some(id);
                }
                sim.run_for(LEADER_TRY);
            }
            if let Some(id) = down.take() {
                bring_back(&mut sim, &nodes, id, variants, node_variants);
            }
            let env = sim.env(client);
            let inner = env.clone();
            env.spawn(
                "second-right-writer",
                second_right_writer(inner, right, x, LEFT_RIGHT_KEY),
            );
            sim.run_for(TAKES_FOR);
        }
    }
    // And the directories at the end, `x` crashed so that nothing on it is writing. `x`
    // was never started again after its re-seed, so everything its re-seed's start did —
    // both streams' staging, their installs' sweeps, any take — was done by the start
    // that re-seeded, which is the start that held the wrong directory (#176).
    sim.crash(at);
    report.refused_at_end = audit_tree(&mut sim, at, Path::new(DIR));
    report.still_lost = marked_lost(&mut sim, at, Path::new(DIR));
    report.fresh_at_end = audit_tree(&mut sim, at, &fresh_dir());
    report.records = sim.trace();
    // What `x`'s engine holds, read with the run's trace already taken, so the read moves
    // nothing the run's checks see. The engine a re-seed built is the first generation
    // past the refused one (D-041).
    // PROPOSED(D-128)
    sim.crash(at);
    sim.restart(at);
    report.right_keys_on_x = keys_on_disk(
        &mut sim,
        at,
        &generation_dir(Path::new(DIR), 1),
        ananke_raft::apply::user_key(SPLIT_KEY.as_bytes()),
        ananke_raft::apply::user_key(RIGHT_END.as_bytes()),
    );
    report
}

// A leader that can confirm no read (PROPOSED D-129): the pair on a run that holds one
// there. `sim/shard.rs` found the state on seeds 128 and 643; this run builds it on every
// seed from the same two re-seeds, on the scenario main already has.

/// The range whose leader the run holds in the state.
pub const READS_RANGE: u64 = FIRST_RANGE;
/// The key the run's readers read: the range's first.
const READS_KEY: &str = "k0";
/// The run's readers. Each asks for a fresh get whenever its last one is answered or times
/// out, as `sim/shard.rs`'s clients do after their operation timeout: the registrations
/// that seed 128 counted were these, not retries (#125). Two at `READ_WAIT` are about
/// twenty-four a second, about the rate of the clients of a sweep with four and several
/// ranges, so a leader that keeps them passes the bound well inside [`READS_HOLD`].
const READERS: u64 = 2;
/// How long a reader waits for its answer before it goes on to a fresh get.
const READ_WAIT: Duration = Duration::from_millis(80);
/// How long the three run before the first loss: every range elects, and the writer takes
/// the range's log over its threshold.
const READS_SETTLE: Duration = Duration::from_secs(2);
/// How long the first follower is gone at least, so that the leader designates it and
/// compacts past it and its re-seed is a stream (D-037), as seed 128's were.
const READS_GONE: Duration = Duration::from_secs(2);
/// The longest the run waits for a re-seed to count: the leader's compaction, then the
/// first rise of the follower's match under its new store.
const READS_BUDGET: Duration = Duration::from_secs(8);
/// How long the first follower is left after its re-seed before the second is crashed, so
/// that it is caught up and can commit alone with the leader.
const READS_MARGIN: Duration = Duration::from_millis(300);
/// How long the second follower is gone. Short on purpose. This is the transient: a
/// follower silent since it last confirmed counts as able, the reads wait on it, and the
/// bound's measured figure covers them (D-076). Its first answer drops them (D-129 §3).
const READS_DOWN: Duration = Duration::from_millis(300);
/// How long the leader is held in the state, both its followers re-seeded and its clients
/// reading: long enough for a leader that kept every read to pass the bound twice.
pub const READS_HOLD: Duration = Duration::from_secs(4);
/// The longest frame the leader's path to the second follower carries in the stalled run:
/// under a chunk's `SNAPSHOT_CHUNK` alone and over a frame of an empty AppendEntries for
/// each of the node's six ranges, about 400 bytes. A path-MTU black hole, as `sim/quorum.rs`
/// blocks a stream (D-049).
const READS_STALL_FRAME: usize = 500;
/// The slice the run advances in while it waits.
const READS_SLICE: Duration = Duration::from_millis(10);
/// How long after the state shows the last read a round confirmed may still be served:
/// past any lease a follower promised before it was lost, and past any acknowledgement
/// still on its way back then. The leader's own heartbeats are ten times this close.
const READS_SETTLED: Duration = Duration::from_secs(1);

/// What the run's readers heard.
// PROPOSED(D-129)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadersHeard {
    /// A value: served.
    pub served: u64,
    /// `NotLeader`, naming nobody: a read the leader held and dropped. Counted whichever
    /// get it answers, since a read that waited through the second follower's silence has
    /// usually been given up on by the time its answer comes.
    pub not_leader: u64,
    /// Nothing, within `READ_WAIT`: on the node a read refused at the step is not
    /// answered (D-076).
    pub unanswered: u64,
}

/// One run of the leader held with both followers re-seeded and its clients reading
/// ([`run_unconfirmable_reads`]).
// PROPOSED(D-129)
#[derive(Clone, Debug)]
pub struct UnconfirmableReads {
    /// The seed.
    pub seed: u64,
    /// The cores' variants: empty for the correct one, `UnconfirmableReadKept` for the pair.
    pub variants: Variants,
    /// The run's trace.
    pub records: Vec<TraceRecord>,
    /// The leader of [`READS_RANGE`] the run held, once it had one.
    pub leader: u64,
    /// The follower lost and re-seeded first, then the second.
    pub lost: [u64; 2],
    /// Whether the second follower's re-seed was stalled, so it stays refused for good
    /// ([`run_unconfirmable_reads_stalled`]).
    pub stalled: bool,
    /// Where the hold starts in the trace: the second re-seed has counted.
    pub hold_from: usize,
    /// When the hold starts.
    pub hold_at: Option<Instant>,
    /// What the readers heard over the whole run.
    pub readers: ReadersHeard,
    /// Writes the writer saw committed, at the hold's start and at the run's end.
    pub committed: (u64, u64),
    /// What the run could not build, if anything.
    pub unbuilt: Option<String>,
}

impl UnconfirmableReads {
    /// Where the leader's state first shows in the trace.
    #[must_use]
    pub fn state_at(&self) -> Option<usize> {
        self.records.iter().position(|record| {
            matches!(record.event, TraceEvent::RaftReadsConfirmable {
                server, range, confirmable: false, ..
            } if server == self.leader && range == READS_RANGE)
        })
    }

    /// The reads the leader refused at the step, from `at` in the trace on.
    #[must_use]
    pub fn refused_from(&self, at: usize) -> usize {
        self.records[at.min(self.records.len())..]
            .iter()
            .filter(|record| {
                matches!(record.event, TraceEvent::RaftReadRefused { server, range, .. }
                    if server == self.leader && range == READS_RANGE)
            })
            .count()
    }

    /// The pending reads the leader dropped at answers that showed the state.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.records
            .iter()
            .filter_map(|record| match record.event {
                TraceEvent::RaftReadsConfirmable {
                    server,
                    range,
                    dropped,
                    ..
                } if server == self.leader && range == READS_RANGE => Some(dropped),
                _ => None,
            })
            .sum()
    }

    /// The commits the leader traced from `at` in the trace on.
    #[must_use]
    pub fn commits_from(&self, at: usize) -> usize {
        self.records[at.min(self.records.len())..]
            .iter()
            .filter(|record| {
                matches!(record.event, TraceEvent::RaftCommit { server, range, .. }
                    if server == self.leader && range == READS_RANGE)
            })
            .count()
    }

    /// The most reads any replica of the range held registered at once, as traced.
    #[must_use]
    pub fn outstanding(&self) -> u64 {
        self.records
            .iter()
            .filter_map(|record| match record.event {
                TraceEvent::RaftReadsOutstanding {
                    range, outstanding, ..
                } if range == READS_RANGE => Some(outstanding),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }

    /// The reads of the leader's a round confirmed, `READS_SETTLED` after the hold began.
    /// None may be: nothing can confirm them there. A rule that counted a quarantined
    /// acknowledgement for read-index, the one the owner did not take, would fill this.
    #[must_use]
    pub fn confirmed_in_the_hold(&self) -> usize {
        let Some(from) = self.hold_at.map(|at| at + READS_SETTLED) else {
            return 0;
        };
        self.records
            .iter()
            .filter(|record| {
                record.at >= from
                    && matches!(record.event, TraceEvent::RaftRead {
                        server, range, lease: false, ..
                    } if server == self.leader && range == READS_RANGE)
            })
            .count()
    }

    /// The first failure the bound reported of the leader's replica, if it did.
    #[must_use]
    pub fn over_the_bound(&self) -> Option<&str> {
        let named = format!("range {READS_RANGE} holds");
        self.records.iter().find_map(|record| match &record.event {
            TraceEvent::RaftServerFailed { server, reason }
                if *server == self.leader
                    && reason.contains(&named)
                    && reason.contains("registered reads") =>
            {
                Some(reason.as_str())
            }
            _ => None,
        })
    }

    /// The run's verdict: no node stopped, checks 1 to 4, the commit majority and the range
    /// layer's folds.
    ///
    /// # Errors
    ///
    /// The first violation, with the seed.
    pub fn check(&self) -> Result<(), String> {
        let seed = self.seed;
        if let Some((server, reason)) = self.records.iter().find_map(|record| match &record.event {
            TraceEvent::RaftServerFailed { server, reason } => Some((*server, reason.clone())),
            _ => None,
        }) {
            return Err(format!("seed {seed}: server {server} stopped: {reason}"));
        }
        if let Err(violation) = ananke_raft::invariants::all(crate::traced(&self.records)) {
            return Err(format!("seed {seed}: {violation}"));
        }
        if let Err(violation) = ananke_raft::invariants::commit_majority(
            crate::traced(&self.records),
            usize::try_from(SERVERS).expect("small"),
        ) {
            return Err(format!("seed {seed}: {violation}"));
        }
        if let Err(violation) = ananke_shard::invariants::all(&self.records) {
            return Err(format!("seed {seed}: {violation}"));
        }
        Ok(())
    }

    /// The state, per seed. The leader led throughout, both followers were refused and
    /// re-seeded quarantined with the log in them, so what holds the leader in office is
    /// their answers; its trace says it can confirm nothing, first at an answer of the
    /// second follower's, and still says so at the end; and it was the only leader the range
    /// had from there on. A clause a run did not reach is a run that says nothing about the
    /// rule, whichever half it is.
    ///
    /// # Errors
    ///
    /// What was not built or not reached, with the seed.
    pub fn reached(&self) -> Result<(), String> {
        let seed = self.seed;
        if let Some(why) = &self.unbuilt {
            return Err(format!("seed {seed}: the state was not built: {why}"));
        }
        for follower in self.lost {
            let refused = self.records.iter().position(|record| {
                matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == follower)
            });
            let Some(refused) = refused else {
                return Err(format!("seed {seed}: node {follower} was never refused"));
            };
            let stated = self.records[refused..].iter().any(|record| {
                matches!(record.event, TraceEvent::RaftReseeded { server, range }
                    if server == follower && range == READS_RANGE)
            });
            let counted = self.records[refused..].iter().any(|record| {
                matches!(record.event, TraceEvent::RaftMatchStarted { follower: f, range, .. }
                    if f == follower && range == READS_RANGE)
            });
            if self.stalled && follower == self.lost[1] {
                if counted {
                    return Err(format!(
                        "seed {seed}: node {follower}'s stream was meant to be stalled, and its \
                         match rose: it did not stay refused"
                    ));
                }
                continue;
            }
            if !stated || !counted {
                return Err(format!(
                    "seed {seed}: node {follower}'s replica of range {READS_RANGE} was not \
                     re-seeded into counting: stated quarantined {stated}, its match rose \
                     {counted}"
                ));
            }
        }
        let Some(state_at) = self.state_at() else {
            return Err(format!(
                "seed {seed}: leader {} never traced that it can confirm no read of range \
                 {READS_RANGE}",
                self.leader
            ));
        };
        let last_side = self
            .records
            .iter()
            .rev()
            .find_map(|record| match record.event {
                TraceEvent::RaftReadsConfirmable {
                    server,
                    range,
                    confirmable,
                    ..
                } if server == self.leader && range == READS_RANGE => Some(confirmable),
                _ => None,
            });
        if last_side != Some(false) {
            return Err(format!(
                "seed {seed}: the leader's last word on its reads is {last_side:?}, not that \
                 it can confirm none"
            ));
        }
        let left_office = self.records[state_at..]
            .iter()
            .find_map(|record| match &record.event {
                TraceEvent::RaftQuorumLost { server, range, .. }
                    if *server == self.leader && *range == READS_RANGE =>
                {
                    Some(format!("{:?}", record.event))
                }
                TraceEvent::RaftLeader { server, range, .. } if *range == READS_RANGE => {
                    Some(format!("server {server} took office"))
                }
                _ => None,
            });
        if let Some(how) = left_office {
            return Err(format!(
                "seed {seed}: the leader did not stay in the state: {how}"
            ));
        }
        Ok(())
    }

    /// What the owner's ruling asks of the correct leader in the state, per seed: every read
    /// it could not confirm refused or dropped, what it holds bounded, nothing confirmed
    /// there, and writes committing the whole time. Each clause reads what it asserts off
    /// the trace, or off what the clients heard, and none only that the run was green.
    ///
    /// # Errors
    ///
    /// The clause that does not hold, with the seed.
    pub fn refused_and_committing(&self) -> Result<(), String> {
        let seed = self.seed;
        let state_at = self.state_at().unwrap_or(self.records.len());
        let refused = self.refused_from(self.hold_from);
        // A reader waits `READ_WAIT` on a refused read and asks again, so the hold has at
        // least this many in it even if half of it went on one reader.
        let hold_reads = READS_HOLD.as_millis() / READ_WAIT.as_millis() / 2;
        if (refused as u128) < hold_reads {
            return Err(format!(
                "seed {seed}: the leader refused {refused} reads in a hold of {READS_HOLD:?} \
                 at {READERS} readers, under {hold_reads}"
            ));
        }
        if self.dropped() == 0 {
            return Err(format!(
                "seed {seed}: the leader dropped no read at the answer that showed the state, \
                 so the reads that waited on the second follower through {READS_DOWN:?} wait \
                 still"
            ));
        }
        if self.readers.not_leader == 0 {
            return Err(format!(
                "seed {seed}: no reader was told `NotLeader` of a dropped read: {:?}",
                self.readers
            ));
        }
        let outstanding = self.outstanding();
        if outstanding > READS_HELD_AT_MOST {
            return Err(format!(
                "seed {seed}: the replica held {outstanding} reads at once, over the \
                 {READS_HELD_AT_MOST} {READERS} readers through {READS_DOWN:?} can leave on it"
            ));
        }
        let confirmed = self.confirmed_in_the_hold();
        if confirmed != 0 {
            return Err(format!(
                "seed {seed}: {confirmed} reads were confirmed by a round in a state where \
                 nothing can confirm one"
            ));
        }
        let commits = self.commits_from(self.hold_from);
        let written = self.committed.1.saturating_sub(self.committed.0);
        if commits == 0 || written == 0 {
            return Err(format!(
                "seed {seed}: writes stopped in the state: {commits} commits traced, \
                 {written} answered, from the hold on; {state_at} is where the state began"
            ));
        }
        Ok(())
    }
}

/// The most reads the correct node holds on this run, and it is a figure about the run and
/// not the bound: the two readers through the second follower's silence, and each reader's
/// read in flight when an install's answer makes a follower able for a moment. Twice the
/// worst measured over the binary's two hundred seeds in release, 11, by D-076's rule. The
/// bound itself, 38, is asked by [`UnconfirmableReads::check`].
// PROPOSED(D-129)
pub const READS_HELD_AT_MOST: u64 = 22;

/// A reader of [`READS_RANGE`]: a fresh get whenever the last is answered or times out,
/// always at the one leader, recording what it hears.
async fn reader<E: Environment>(env: E, client: u64, target: u64, heard: Arc<Mutex<ReadersHeard>>) {
    let Ok(sock) = env.net().bind(client_addr(client)).await else {
        return;
    };
    let mut seq = 0u64;
    loop {
        seq += 1;
        let request = RangedRequest {
            generation: FIRST_GENERATION,
            range: RangeId(READS_RANGE),
            request: Request {
                client,
                seq,
                command: Command::Get {
                    key: Bytes::from(READS_KEY),
                },
            },
        };
        let _ = sock.send(server_addr(target), request.encode()).await;
        let deadline = env.clock().now() + READ_WAIT;
        let reply = loop {
            let recv = pin!(sock.recv());
            let timer = pin!(env.clock().sleep_until(deadline));
            let bytes = match race(&env, recv, timer).await {
                Either::Left(Ok((_, bytes))) => bytes,
                Either::Left(Err(_)) | Either::Right(()) => break None,
            };
            let Ok(response) = RangedResponse::decode(bytes) else {
                continue;
            };
            if matches!(response.response.reply, Reply::NotLeader { leader: None }) {
                heard.lock().expect("the readers' tally").not_leader += 1;
            }
            if response.response.seq == seq {
                break Some(response.response.reply);
            }
        };
        {
            let mut heard = heard.lock().expect("the readers' tally");
            match reply {
                Some(Reply::Outcome(_)) => heard.served += 1,
                Some(_) => {}
                None => heard.unanswered += 1,
            }
        }
        env.clock().sleep(Duration::from_millis(4)).await;
    }
}

/// Advances `sim` until a record from now on satisfies `seen`, or `budget` is spent.
fn until_traced(
    sim: &mut Sim,
    budget: Duration,
    mut seen: impl FnMut(&TraceRecord) -> bool,
) -> bool {
    let deadline = sim.now() + budget;
    let mut scanned = sim.trace_len();
    loop {
        let records = sim.trace_from(scanned);
        scanned += records.len();
        if records.iter().any(&mut seen) {
            return true;
        }
        if sim.now() >= deadline {
            return false;
        }
        sim.run_for(READS_SLICE);
    }
}

/// [`run_unconfirmable_reads`] with the second follower's re-seed stalled for good: its
/// stream is lost to a path-MTU black hole, and its heartbeats and rejections pass. It never
/// gets past refused, and the leader keeps its office on the first follower alone (D-049).
/// This is the run that decides how a refused follower counts (PROPOSED D-129 §2): under
/// "echo 0 and no refused mark" the follower counts as able to confirm for as long as its
/// stream stalls, the reads wait on it, and the bound trips on the correct system.
// PROPOSED(D-129): a refused follower's echo of 0 counts as a quarantined one's.
#[must_use]
pub fn run_unconfirmable_reads_stalled(seed: u64) -> UnconfirmableReads {
    unconfirmable_reads(seed, Variants::default(), true)
}

/// Holds a leader with both its followers re-seeded and its clients reading, for `seed`.
///
/// The three servers of the re-seed shape, a writer of [`READS_RANGE`] and, once the range
/// has its leader, two readers of it at that leader. No message is dropped at random, as in
/// `sim/quorum.rs` and for its reason: a leader that lost a whole window of answers from the
/// one follower it has left would step down into a range that can elect nobody (D-035), and
/// the state would be a matter of the draw. Duplicates, delays, reordering and the disk's
/// latency stay.
///
/// The one follower is lost as the re-seed shape loses one: crashed, left down until the
/// leader has compacted past it, and started again on a store marked lost. It is refused,
/// the leader streams its re-seed, and it comes back quarantined with the log in it, so it
/// commits and counts for check quorum. Then the other. The leader keeps its office on the
/// first through the second's absence, which is the state's way in: a read waits on the
/// silent follower, as D-076's transient may, until its first answer says it never will.
/// That is a refused server's, and then a quarantined one's after its install, and the
/// leader stays there for [`READS_HOLD`]. It is seeds 128 and 643 of `sim/shard.rs` with
/// nothing else going on.
// PROPOSED(D-129): a leader refuses a read it can never confirm.
#[must_use]
pub fn run_unconfirmable_reads(seed: u64, variants: impl Into<Variants>) -> UnconfirmableReads {
    unconfirmable_reads(seed, variants.into(), false)
}

fn unconfirmable_reads(seed: u64, variants: Variants, stall: bool) -> UnconfirmableReads {
    let node_variants = NodeVariants::correct();
    let mut config = config(seed);
    config.net.p_drop = 0.0;
    let mut sim = Sim::new(config);
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
    {
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn("writer", writer(inner, 1, READS_RANGE, committed.clone()));
    }
    sim.run_for(READS_SETTLE);

    let leader = leader_of(&sim.trace(), READS_RANGE).unwrap_or(1);
    let mut draw = moirae_sched::stream(seed, "reseed-unconfirmable-reads");
    let followers: Vec<u64> = (1..=SERVERS).filter(|id| *id != leader).collect();
    let first = followers[usize::try_from(draw.below(followers.len() as u64)).expect("small")];
    let second = followers
        .iter()
        .copied()
        .find(|id| *id != first)
        .expect("three servers");
    let heard: Arc<Mutex<ReadersHeard>> = Arc::default();
    for n in 0..READERS {
        let env = sim.env(client);
        let inner = env.clone();
        env.spawn("reader", reader(inner, 2 + n, leader, heard.clone()));
    }
    let mut report = UnconfirmableReads {
        seed,
        variants,
        records: Vec::new(),
        leader,
        lost: [first, second],
        stalled: stall,
        hold_from: 0,
        hold_at: None,
        readers: ReadersHeard::default(),
        committed: (0, 0),
        unbuilt: None,
    };
    let unbuilt = |mut report: UnconfirmableReads, sim: &Sim, why: String| {
        report.unbuilt = Some(why);
        report.records = sim.trace();
        report
    };

    // The first follower: gone until the leader has compacted past it.
    let at = nodes[first as usize - 1];
    sim.crash(at);
    sim.run_for(READS_GONE);
    let compacted = |record: &TraceRecord| {
        matches!(record.event, TraceEvent::RaftCompacted { server, range, .. }
            if server == leader && range == READS_RANGE)
    };
    if !sim.trace().iter().any(compacted) && !until_traced(&mut sim, READS_BUDGET, compacted) {
        let why = format!("leader {leader} did not compact with node {first} gone");
        return unbuilt(report, &sim, why);
    }
    sim.restart(at);
    spawn(&sim, at, first, variants, node_variants, true);
    let counted = |follower: u64| {
        move |record: &TraceRecord| {
            matches!(record.event, TraceEvent::RaftMatchStarted { follower: f, range, .. }
                if f == follower && range == READS_RANGE)
        }
    };
    if !until_traced(&mut sim, READS_BUDGET, counted(first)) {
        let why = format!("node {first}'s re-seed never counted");
        return unbuilt(report, &sim, why);
    }
    sim.run_for(READS_MARGIN);

    // The second: the state's way in, through a silence D-076's bound covers.
    let at = nodes[second as usize - 1];
    sim.crash(at);
    sim.run_for(READS_DOWN);
    if stall {
        sim.limit_frames(nodes[leader as usize - 1], at, READS_STALL_FRAME);
    }
    sim.restart(at);
    spawn(&sim, at, second, variants, node_variants, true);
    let second_counted = counted(second);
    let shown = |record: &TraceRecord| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == second);
    let hold_begins = if stall {
        // The stream never ends: the hold starts at the refusal, and the second follower's
        // answers show the state just after.
        until_traced(&mut sim, READS_BUDGET, shown)
    } else {
        until_traced(&mut sim, READS_BUDGET, second_counted)
    };
    if !hold_begins {
        let why = format!("node {second}'s re-seed never counted or was never refused");
        return unbuilt(report, &sim, why);
    }

    // The hold.
    report.hold_from = sim.trace_len();
    report.hold_at = Some(sim.now());
    let before = *committed.lock().expect("the counter");
    sim.run_for(READS_HOLD);
    report.committed = (before, *committed.lock().expect("the counter"));
    report.readers = *heard.lock().expect("the readers' tally");
    report.records = sim.trace();
    report
}

/// The right halves of P's splits node `x` applied, in the order it applied them.
// PROPOSED(D-117)
fn rights_on(records: &[TraceRecord], x: u64) -> Vec<u64> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            TraceEvent::RangeSplit { range, right, .. }
                if *range == SPLIT_PARENT
                    && record.node.map(|node| u64::from(node.get())) == Some(x) =>
            {
                Some(*right)
            }
            _ => None,
        })
        .collect()
}

/// Starts node `id` again on its own store, after a crash the run aimed at it.
// PROPOSED(D-117)
fn bring_back(
    sim: &mut Sim,
    nodes: &[NodeId],
    id: u64,
    variants: Variants,
    node_variants: NodeVariants,
) {
    let node = nodes[id as usize - 1];
    sim.restart(node);
    spawn(sim, node, id, variants, node_variants, false);
}

/// The directory a node of this shape's first re-seed builds: the generation after the
/// configured one, beside it (D-066, D-077).
#[must_use]
pub fn fresh_dir() -> PathBuf {
    ananke_shard::reseed::generation_dir(Path::new(DIR), ananke_shard::reseed::FIRST_GENERATION + 1)
}

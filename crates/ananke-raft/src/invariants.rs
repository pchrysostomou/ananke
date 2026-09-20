//! The safety properties of Figure 3 of the paper, as folds over the trace (RAFT.md
//! §2). Each takes every event of a run so far and returns the first violation, so a
//! sweep can run them after every crash and at the end, and a unit test over a
//! handful of cores can run them over the outputs it collected.
//!
//! Three more folds make the rules behind the properties visible in the trace. A
//! server traces `RaftAppend` only once the entry is durable, so [`commit_majority`]
//! can ask whether every entry a leader committed was durable on a majority when it
//! did, which is what a server that sends before it persists breaks. A server's
//! commit index only ever covers entries every later leader holds, so
//! [`committed_never_truncated`] asks that no server truncates below its own commit
//! index, which is what a follower that truncates on every append breaks. And a
//! leader advances its commit index only to an entry of its own term (§5.4.2), so
//! [`commit_by_current_term`] asks exactly that of every leader's commit, which is
//! what a leader that commits by count alone breaks: the Figure 8 window, between a
//! follower matching older entries and matching the leader's own, is a few
//! milliseconds wide and needs no crash to be seen.
//!
//! # Every check is keyed by its group
//!
//! A node runs a Raft group per range (SHARD.md §4), and one trace holds every
//! group's events: each names its group, as [`TraceEvent::RaftLeader`]'s `range`
//! does. Every property of Figure 3 is a property *of one group*, so every map
//! below is keyed by [`Group`] first (SHARD.md §8, checks 1 to 4): the leader of
//! each (group, term), the log and snapshot floor of each (group, server), the
//! committed set of each group, and what each group's index applied as. A check
//! keyed by term, server or index alone would read two groups electing in one term
//! as two leaders of it, two replicas' unrelated logs as one, and two groups'
//! entries at index 1 as the same entry — and with one group per trace it would
//! never say so, since every event carries the same group. The crate names no
//! range: a group is an opaque id here, [`crate::node::SINGLE_GROUP`] while a
//! server runs one, and what a range's replicas hold when a node runs many
//! (SHARD.md §11, raft 12).
//!
//! Two of the events a group's state is keyed by name their range and no server —
//! `RangeCreated` and `RangeRemoved` are recorded with their node, as every record
//! is (SHARD.md §8) — so the checker is fed [`Traced`], the event with the node
//! that traced it beside it. A `&TraceEvent` converts into one with no node, which
//! is every caller that has only the events: a `RangeCreated` fed that way names no
//! replica and is skipped.
//!
//! # The checker keeps its state
//!
//! A sweep that folds every check over the whole trace every few slices pays for
//! the whole trace at every look, and a run costs time quadratic in its length
//! (issue #25, D-046). [`Checker`] is the same folds with their state kept
//! across calls — the reconstructed logs, the leaders per term, the committed set,
//! the applied map, the snapshot floors, the configuration in force — so a slice
//! costs only its own new events. It is the one implementation: every function in
//! this module drives a [`Checker`] and reads one of its verdicts, so what the
//! sweep sees at a slice boundary and what [`all`] says over the whole trace are
//! the same fold reporting the same violation in the same words.

use std::collections::{BTreeMap, BTreeSet};

use ananke_env::{ApplyEffect, TraceEvent};

use crate::types::{Index, Term};

/// The group a check's state is keyed by (SHARD.md §8): the `range` every
/// `Raft*` event about a replica carries. This crate names no range — the group is
/// an opaque id, [`crate::node::SINGLE_GROUP`] while a server runs one group and a
/// range id when a node runs many (SHARD.md §11, raft 12).
// PROPOSED(D-071): checks 1 to 4 keyed by group.
pub type Group = u64;

/// One trace record as the checks read it: the event, and the node that traced it.
///
/// Every `Raft*` event about a replica names its server. The two range events the
/// checks fold — `RangeCreated`, which sets a replica's floor, and `RangeRemoved`,
/// which ends a replica's memory — name their range and their node alone
/// (SHARD.md §8), so the checker is given the node beside the event. A
/// `&TraceEvent` converts into a `Traced` with no node, which is what every caller
/// holding only the events gets; the two range events are skipped there, since
/// without a node they name no replica.
// PROPOSED(D-071): checks 1 to 4 keyed by group.
#[derive(Clone, Copy, Debug)]
pub struct Traced<'a> {
    /// The node that traced the event, where the trace records one.
    pub node: Option<u64>,
    /// The event.
    pub event: &'a TraceEvent,
}

impl<'a> Traced<'a> {
    /// The event as `node` traced it.
    #[must_use]
    pub fn new(node: Option<u64>, event: &'a TraceEvent) -> Self {
        Self { node, event }
    }
}

impl<'a> From<&'a TraceEvent> for Traced<'a> {
    fn from(event: &'a TraceEvent) -> Self {
        Self { node: None, event }
    }
}

/// One server's log as the trace shows it: index to (term, payload hash).
type Log = BTreeMap<Index, (Term, u64)>;

/// A configuration in force on one replica: the voters, and the new voters while it
/// is joint.
type Config = (Vec<u64>, Option<Vec<u64>>);

/// Every replica's log and snapshot floor as the trace shows them (RAFT.md §2),
/// keyed by (group, server): an installed snapshot replaces the log prefix at or
/// below its last index, and either kind of snapshot means the replica durably
/// holds the committed prefix through it. A `RangeCreated` sets the floor the same
/// way (SHARD.md §8, check 2), so a group born at a split starts at its floor with
/// no log below it.
#[derive(Default)]
struct Logs {
    logs: BTreeMap<(Group, u64), Log>,
    /// Per replica, the last snapshot's (last index, last term).
    floor: BTreeMap<(Group, u64), (Index, Term)>,
}

impl Logs {
    /// Replays one append, truncation, snapshot or creation into the logs. A
    /// snapshot must agree with any entry another replica of the same group holds
    /// at its last index, the log-matching check at the boundary the prefix no
    /// longer shows.
    fn replay(&mut self, traced: Traced<'_>) -> Result<(), String> {
        match traced.event {
            TraceEvent::RaftAppend {
                server,
                range,
                index,
                entry_term,
                hash,
            } => {
                self.logs
                    .entry((*range, *server))
                    .or_default()
                    .insert(*index, (*entry_term, *hash));
            }
            TraceEvent::RaftTruncate {
                server,
                range,
                from_index,
            } => {
                self.logs
                    .entry((*range, *server))
                    .or_default()
                    .retain(|&index, _| index < *from_index);
            }
            TraceEvent::RaftSnapshot {
                server,
                range,
                last_index,
                last_term,
                taken,
            } => {
                self.snapshot(*range, *server, *last_index, *last_term, !*taken)?;
            }
            // SHARD.md §8, check 2: a `RangeCreated` sets its replica's floor as an
            // installed snapshot does, so a range born at a split has a log that
            // starts at the floor its creation names and nothing below it. The node
            // the record carries is the replica's; a creation fed without one names
            // no replica and sets no floor.
            TraceEvent::RangeCreated {
                range,
                floor_index,
                floor_term,
                ..
            } => {
                if let Some(server) = traced.node {
                    self.snapshot(*range, server, *floor_index, *floor_term, true)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// One replica's floor moving to `(last_index, last_term)`. `replaces` is an
    /// install's or a creation's: the store is the checkpoint now, so the floor is
    /// exactly the new one, lower or higher than before — a re-seeded server's
    /// earlier floor belonged to the store it lost (the nightly's seed 7381,
    /// D-030) — and the log prefix it covers is gone with the old store. A
    /// snapshot the replica took itself only ever raises the floor.
    fn snapshot(
        &mut self,
        group: Group,
        server: u64,
        last_index: Index,
        last_term: Term,
        replaces: bool,
    ) -> Result<(), String> {
        // Two snapshots of one group cover committed prefixes, so at one index
        // their terms agree: the log-matching check at a boundary the logs no
        // longer show. An uncommitted entry another log holds at this index may
        // legitimately differ; the applied entries may not, which state machine
        // safety checks. Two *groups* share nothing at an index, so the comparison
        // is within the group.
        for (&(other_group, other), &(index, term)) in &self.floor {
            if other_group == group && index == last_index && term != last_term {
                return Err(format!(
                    "log matching: server {server}'s snapshot at index {last_index} of group {group} has term {last_term} but server {other}'s snapshot there has term {term}"
                ));
            }
        }
        let floor = self.floor.entry((group, server)).or_default();
        if replaces {
            *floor = (last_index, last_term);
            self.logs
                .entry((group, server))
                .or_default()
                .retain(|&index, _| index > last_index);
        } else if last_index >= floor.0 {
            *floor = (last_index, last_term);
        }
        Ok(())
    }

    fn log(&self, group: Group, server: u64) -> Option<&Log> {
        self.logs.get(&(group, server))
    }

    /// The replica's snapshot floor: it durably holds the committed prefix through
    /// this index.
    fn floor_of(&self, group: Group, server: u64) -> Index {
        self.floor
            .get(&(group, server))
            .map_or(0, |&(index, _)| index)
    }

    /// Whether `server` durably holds `entry` at `index` of `group`: in its log, or
    /// inside its snapshot, which carries the committed prefix by construction.
    fn holds(&self, group: Group, server: u64, index: Index, entry: (Term, u64)) -> bool {
        if self.floor_of(group, server) >= index {
            return true;
        }
        self.logs
            .get(&(group, server))
            .is_some_and(|log| log.get(&index) == Some(&entry))
    }

    /// What the trace shows of `server`'s replica of `group` at `index`: the entry,
    /// the compacted prefix standing in for it, or nothing. Index 0 counts as
    /// covered: every log agrees before its first entry.
    fn at(&self, group: Group, server: u64, index: Index) -> Cell {
        if index <= self.floor_of(group, server) || index == 0 {
            return Cell::Covered;
        }
        match self
            .logs
            .get(&(group, server))
            .and_then(|log| log.get(&index))
        {
            Some(&entry) => Cell::Entry(entry),
            None => Cell::Absent,
        }
    }

    /// Every server the trace shows a log or a snapshot of `group` for.
    fn servers(&self, group: Group) -> BTreeSet<u64> {
        self.logs
            .keys()
            .chain(self.floor.keys())
            .filter(|(g, _)| *g == group)
            .map(|&(_, server)| server)
            .collect()
    }
}

/// One position of one replica's log as [`Logs::at`] shows it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cell {
    /// The entry, as (term, hash).
    Entry((Term, u64)),
    /// Inside the replica's compacted prefix: committed there by construction, so
    /// it agrees with anything.
    Covered,
    /// Not held.
    Absent,
}

impl Cell {
    /// Whether two positions can belong to logs that agree: an entry against an
    /// entry must be the same one; a compacted prefix agrees with anything; a
    /// missing entry disagrees with a held one.
    fn agrees(self, other: Cell) -> bool {
        match (self, other) {
            (Cell::Covered, _) | (_, Cell::Covered) => true,
            (Cell::Entry(a), Cell::Entry(b)) => a == b,
            (Cell::Absent, Cell::Absent) => true,
            (Cell::Entry(_), Cell::Absent) | (Cell::Absent, Cell::Entry(_)) => false,
        }
    }
}

/// What one index of one group applied as (SHARD.md §8, check 4): the entry, and
/// what applying it did. `effect` is `None` for an apply the trace shows no effect
/// for — the entries a restart's recovered applied index accounts for are read from
/// the replica's log, which holds the entry and not what applying it did — and a
/// `None` agrees with anything and is filled in by the first apply that names one.
#[derive(Clone, Copy, PartialEq, Eq)]
struct AppliedEntry {
    term: Term,
    hash: u64,
    effect: Option<ApplyEffect>,
}

/// What state machine safety has seen applied: per group, each index once per
/// replica, in order, and the same entry to the same effect on every replica of
/// that group.
#[derive(Default)]
struct Applied {
    by_index: BTreeMap<(Group, Index), AppliedEntry>,
    last: BTreeMap<(Group, u64), Index>,
}

impl Applied {
    fn record(
        &mut self,
        group: Group,
        server: u64,
        index: Index,
        entry: AppliedEntry,
    ) -> Result<(), String> {
        let previous = self.last.get(&(group, server)).copied().unwrap_or(0);
        if index != previous + 1 {
            return Err(format!(
                "state machine safety: server {server} applied index {index} of group {group} after {previous}"
            ));
        }
        self.last.insert((group, server), index);
        let Some(other) = self.by_index.get(&(group, index)).copied() else {
            self.by_index.insert((group, index), entry);
            return Ok(());
        };
        if (other.term, other.hash) != (entry.term, entry.hash) {
            return Err(format!(
                "state machine safety: index {index} of group {group} was applied as term {} on one server and term {} on server {server}",
                other.term, entry.term
            ));
        }
        match (other.effect, entry.effect) {
            (Some(one), Some(another)) if one != another => {
                return Err(format!(
                    "state machine safety: index {index} of group {group} was applied to effect {} on one server and {} on server {server}",
                    one.as_str(),
                    another.as_str()
                ));
            }
            (None, Some(_)) => {
                self.by_index.insert((group, index), entry);
            }
            _ => {}
        }
        Ok(())
    }
}

/// One check's verdict: `None` while it holds, and its first violation once it
/// fails. A fold returns at its first violation, so no later event can change a
/// check's answer: once it has one, the check consumes no more events, and the
/// message it holds is the one it would report over any longer prefix.
type Verdict = Option<String>;

/// Where each check stands, in the order [`all`] runs them, the commit-majority
/// check last.
#[derive(Default)]
struct Verdicts {
    election_safety: Verdict,
    log_matching: Verdict,
    leader_completeness: Verdict,
    commit_by_current_term: Verdict,
    committed_never_truncated: Verdict,
    state_machine_safety: Verdict,
    commit_majority: Verdict,
}

/// Every check of this module, folded over the events it is given, with the state
/// of each kept across calls (D-046): feeding a run's new events costs
/// only those events, where folding from the first record costs the run's whole
/// trace at every look. Feeding it one event at a time, a slice at a time or a
/// whole trace at once gives the same verdict, which the sweep's
/// `the_incremental_checker_agrees_with_the_fold_over_the_whole_trace` asserts
/// over a hundred seeds.
///
/// Every check is keyed by the group each event names, so two groups are two
/// unrelated sets of logs, leaders, committed entries and applies (SHARD.md §8).
///
/// ```
/// use ananke_env::TraceEvent;
/// use ananke_raft::invariants::Checker;
/// use ananke_raft::node::SINGLE_GROUP;
///
/// let leader = |server, range, term| TraceEvent::RaftLeader {
///     server,
///     range,
///     term,
///     last_index: 0,
/// };
/// let mut checker = Checker::new(3);
/// checker.push(&leader(1, SINGLE_GROUP, 7));
/// // Another group's leader of the same term is another election.
/// checker.push(&leader(2, SINGLE_GROUP + 1, 7));
/// assert!(checker.verdict().is_ok());
/// checker.push(&leader(2, SINGLE_GROUP, 7));
/// assert_eq!(
///     checker.verdict().unwrap_err(),
///     "election safety: servers 1 and 2 both led term 7 of group 2"
/// );
/// ```
pub struct Checker {
    /// The voters of the initial configuration of a group whose `RangeCreated` the
    /// trace does not hold: servers 1 through `servers`.
    initial: Vec<u64>,
    /// Each group's first configuration, from its `RangeCreated` (SHARD.md §8,
    /// check 3).
    born_with: BTreeMap<Group, Vec<u64>>,
    /// Whether the commit-majority check runs; [`all`] does not ask for it.
    checks_majority: bool,
    /// The logs as the trace shows them, replayed once for every check that reads
    /// them rather than once per check.
    logs: Logs,
    /// Whether the replay has reported a violation, after which no check that
    /// survives it reads the logs again.
    logs_stopped: bool,
    /// Who leads each group, as `RaftTerm` and `RaftLeader` say: shared by the
    /// three checks that ask whether a commit is a leader's.
    is_leader: BTreeMap<(Group, u64), Term>,
    /// Election safety: the leader of each (group, term).
    leaders: BTreeMap<(Group, Term), u64>,
    /// Leader completeness: each group's committed entries as (term, hash, the term
    /// it was committed in), by the first leader whose commit index reached it.
    committed: BTreeMap<(Group, Index), (Term, u64, Term)>,
    /// Committed entries stay: the highest commit index each replica has reached.
    commit: BTreeMap<(Group, u64), Index>,
    /// State machine safety: what has been applied, and where.
    applied: Applied,
    /// Commit on a majority: how far each leader's commits have been checked, and
    /// the configuration in force on each replica.
    checked: BTreeMap<(Group, u64), Index>,
    configs: BTreeMap<(Group, u64), Config>,
    /// Where each check stands.
    verdicts: Verdicts,
}

impl Checker {
    /// A checker for a cluster of `servers` servers, running every check this
    /// module offers: what [`all`] covers, and [`commit_majority`], which needs
    /// the cluster size for the initial configuration of a group whose
    /// `RangeCreated` the trace does not hold.
    #[must_use]
    pub fn new(servers: usize) -> Self {
        Self {
            initial: (1..=servers as u64).collect(),
            checks_majority: true,
            ..Self::without_majority()
        }
    }

    /// The checker [`all`] drives: every check but the commit-majority one, which
    /// needs a cluster size [`all`] is not given.
    fn without_majority() -> Self {
        Self {
            initial: Vec::new(),
            born_with: BTreeMap::new(),
            checks_majority: false,
            logs: Logs::default(),
            logs_stopped: false,
            is_leader: BTreeMap::new(),
            leaders: BTreeMap::new(),
            committed: BTreeMap::new(),
            commit: BTreeMap::new(),
            applied: Applied::default(),
            checked: BTreeMap::new(),
            configs: BTreeMap::new(),
            verdicts: Verdicts::default(),
        }
    }

    /// Folds one more event into every check. A `&TraceEvent` is the event with no
    /// node; [`Traced::new`] carries the node the record holds, which the two range
    /// events need ([`Traced`]).
    ///
    /// It returns nothing, because a slice of events has no verdict of its own:
    /// [`all`] reports the first violation in *its* order of the checks, not the
    /// earliest violation in the trace — an election safety violation at the last
    /// event outranks a log matching violation at the first — so only
    /// [`verdict`](Self::verdict), which sees every check at once, can answer.
    pub fn push<'a>(&mut self, traced: impl Into<Traced<'a>>) {
        let traced = traced.into();
        self.replay(traced);
        self.track_leader(traced.event);
        self.note_group(traced.event);
        if self.verdicts.election_safety.is_none()
            && let Err(why) = self.election_safety(traced.event)
        {
            self.verdicts.election_safety = Some(why);
        }
        if self.verdicts.log_matching.is_none()
            && let Err(why) = self.log_matching(traced.event)
        {
            self.verdicts.log_matching = Some(why);
        }
        if self.verdicts.leader_completeness.is_none()
            && let Err(why) = self.leader_completeness(traced.event)
        {
            self.verdicts.leader_completeness = Some(why);
        }
        if self.verdicts.commit_by_current_term.is_none()
            && let Err(why) = self.commit_by_current_term(traced.event)
        {
            self.verdicts.commit_by_current_term = Some(why);
        }
        if self.verdicts.committed_never_truncated.is_none()
            && let Err(why) = self.committed_never_truncated(traced)
        {
            self.verdicts.committed_never_truncated = Some(why);
        }
        if self.verdicts.state_machine_safety.is_none()
            && let Err(why) = self.state_machine_safety(traced)
        {
            self.verdicts.state_machine_safety = Some(why);
        }
        if self.checks_majority
            && self.verdicts.commit_majority.is_none()
            && let Err(why) = self.commit_majority(traced.event)
        {
            self.verdicts.commit_majority = Some(why);
        }
    }

    /// Folds a slice of events into every check, in order.
    pub fn extend<'a, I, T>(&mut self, events: I)
    where
        I: IntoIterator<Item = T>,
        T: Into<Traced<'a>>,
    {
        for event in events {
            self.push(event);
        }
    }

    /// The first violation the checker holds, in the order [`all`] runs the checks
    /// and then the commit-majority check: exactly what
    /// `all(events).and_then(|()| commit_majority(events, servers))` says of the
    /// events fed so far.
    ///
    /// # Errors
    ///
    /// See each check.
    pub fn verdict(&self) -> Result<(), String> {
        let first = [
            &self.verdicts.election_safety,
            &self.verdicts.log_matching,
            &self.verdicts.leader_completeness,
            &self.verdicts.commit_by_current_term,
            &self.verdicts.committed_never_truncated,
            &self.verdicts.state_machine_safety,
            &self.verdicts.commit_majority,
        ]
        .into_iter()
        .flatten()
        .next();
        first.map_or(Ok(()), |why| Err(why.clone()))
    }

    /// Replays one event into the shared view of the logs, which is the first
    /// thing every check that reads them does. A replay error is therefore the
    /// first violation of each of those checks that has not already failed, and
    /// the view stops there: no check that survives it will read the logs again.
    fn replay(&mut self, traced: Traced<'_>) {
        if self.logs_stopped {
            return;
        }
        let Err(why) = self.logs.replay(traced) else {
            return;
        };
        self.logs_stopped = true;
        for verdict in [
            &mut self.verdicts.log_matching,
            &mut self.verdicts.leader_completeness,
            &mut self.verdicts.commit_by_current_term,
            &mut self.verdicts.state_machine_safety,
        ] {
            if verdict.is_none() {
                *verdict = Some(why.clone());
            }
        }
        if self.checks_majority && self.verdicts.commit_majority.is_none() {
            self.verdicts.commit_majority = Some(why);
        }
    }

    /// Follows who leads each group, which three of the checks ask of every commit.
    fn track_leader(&mut self, event: &TraceEvent) {
        match event {
            TraceEvent::RaftTerm {
                server,
                range,
                role,
                term,
                ..
            } => {
                if *role == "leader" {
                    self.is_leader.insert((*range, *server), *term);
                } else {
                    self.is_leader.remove(&(*range, *server));
                }
            }
            TraceEvent::RaftLeader {
                server,
                range,
                term,
                ..
            } => {
                self.is_leader.insert((*range, *server), *term);
            }
            _ => {}
        }
    }

    /// A group's first configuration, which the commit-majority check takes from
    /// its `RangeCreated` where today it takes servers 1 through `servers`
    /// (SHARD.md §8, check 3). The first creation traced of a group is its birth:
    /// every replica of it is created with the same voters, and a later creation on
    /// another node — an install's — restates them.
    fn note_group(&mut self, event: &TraceEvent) {
        if let TraceEvent::RangeCreated { range, voters, .. } = event {
            self.born_with
                .entry(*range)
                .or_insert_with(|| voters.clone());
        }
    }

    /// The voters a group started with: its `RangeCreated`'s, or the initial
    /// configuration for a group whose creation the trace does not hold.
    fn initial_voters(&self, group: Group) -> Vec<u64> {
        self.born_with
            .get(&group)
            .cloned()
            .unwrap_or_else(|| self.initial.clone())
    }

    /// Election safety, one event at a time: see [`election_safety`].
    fn election_safety(&mut self, event: &TraceEvent) -> Result<(), String> {
        if let TraceEvent::RaftLeader {
            server,
            range,
            term,
            ..
        } = event
            && let Some(other) = self.leaders.insert((*range, *term), *server)
            && other != *server
        {
            return Err(format!(
                "election safety: servers {other} and {server} both led term {term} of group {range}"
            ));
        }
        Ok(())
    }

    /// Log matching, one event at a time: see [`log_matching`].
    fn log_matching(&self, event: &TraceEvent) -> Result<(), String> {
        let TraceEvent::RaftAppend {
            server,
            range,
            index,
            entry_term,
            hash,
        } = event
        else {
            return Ok(());
        };
        let below = self.logs.at(*range, *server, index - 1);
        for other in self.logs.servers(*range) {
            if other == *server {
                continue;
            }
            let Cell::Entry((term, other_hash)) = self.logs.at(*range, other, *index) else {
                continue;
            };
            if term != *entry_term {
                continue;
            }
            if other_hash != *hash {
                return Err(format!(
                    "log matching: servers {server} and {other} hold index {index} of group {range} in term {entry_term} with different payloads"
                ));
            }
            if !below.agrees(self.logs.at(*range, other, index - 1)) {
                return Err(format!(
                    "log matching: servers {server} and {other} agree at index {index} of group {range} term {entry_term} but differ at index {}",
                    index - 1
                ));
            }
        }
        Ok(())
    }

    /// Leader completeness, one event at a time: see [`leader_completeness`].
    fn leader_completeness(&mut self, event: &TraceEvent) -> Result<(), String> {
        match event {
            TraceEvent::RaftLeader {
                server,
                range,
                term,
                ..
            } => {
                // The rescan is of this group's committed set: what another group
                // committed is no business of this one's leader.
                for (&(_, index), &(entry_term, hash, in_term)) in
                    self.committed.range((*range, 0)..=(*range, Index::MAX))
                {
                    if in_term < *term
                        && !self.logs.holds(*range, *server, index, (entry_term, hash))
                    {
                        return Err(format!(
                            "leader completeness: index {index} of group {range} (term {entry_term}) was committed in term {in_term} but server {server}, leader of term {term}, does not hold it"
                        ));
                    }
                }
            }
            TraceEvent::RaftCommit {
                server,
                range,
                term,
                index,
            } if self.is_leader.get(&(*range, *server)) == Some(term) => {
                if let Some(log) = self.logs.log(*range, *server) {
                    for (i, (entry_term, hash)) in log.range(..=index) {
                        self.committed
                            .entry((*range, *i))
                            .or_insert((*entry_term, *hash, *term));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Commit by the current term, one event at a time: see
    /// [`commit_by_current_term`].
    fn commit_by_current_term(&self, event: &TraceEvent) -> Result<(), String> {
        let TraceEvent::RaftCommit {
            server,
            range,
            term,
            index,
        } = event
        else {
            return Ok(());
        };
        if self.is_leader.get(&(*range, *server)) != Some(term) {
            return Ok(());
        }
        let entry_term = self
            .logs
            .log(*range, *server)
            .and_then(|log| log.get(index))
            .map(|e| e.0);
        if entry_term != Some(*term) {
            return Err(format!(
                "commit by current term: leader {server} of term {term} committed index {index} of group {range}, whose entry has term {entry_term:?}"
            ));
        }
        Ok(())
    }

    /// Committed entries stay, one event at a time: see
    /// [`committed_never_truncated`].
    fn committed_never_truncated(&mut self, traced: Traced<'_>) -> Result<(), String> {
        match traced.event {
            TraceEvent::RaftCommit {
                server,
                range,
                index,
                ..
            } => {
                let known = self.commit.entry((*range, *server)).or_default();
                *known = (*known).max(*index);
            }
            // A node's refusal takes its store with it, and with it every group on
            // it (SHARD.md §8).
            TraceEvent::RaftRefused { server, .. } => {
                self.commit.retain(|&(_, s), _| s != *server);
            }
            // A replica's removal ends that (group, server)'s memory the way a
            // refusal ends a store's, so a group removed and later added to the same
            // node starts clean (SHARD.md §8, check 4).
            TraceEvent::RangeRemoved { range, .. } => {
                if let Some(server) = traced.node {
                    self.commit.remove(&(*range, server));
                }
            }
            TraceEvent::RaftTruncate {
                server,
                range,
                from_index,
            } => {
                let known = self.commit.get(&(*range, *server)).copied().unwrap_or(0);
                if *from_index <= known {
                    return Err(format!(
                        "committed entries stay: server {server} truncated group {range} from index {from_index} with commit index {known}"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// State machine safety, one event at a time: see [`state_machine_safety`].
    fn state_machine_safety(&mut self, traced: Traced<'_>) -> Result<(), String> {
        match traced.event {
            TraceEvent::RaftApply {
                server,
                range,
                index,
                entry_term,
                hash,
                effect,
                ..
            } => self.applied.record(
                *range,
                *server,
                *index,
                AppliedEntry {
                    term: *entry_term,
                    hash: *hash,
                    effect: Some(*effect),
                },
            )?,
            TraceEvent::RaftSnapshot {
                server,
                range,
                last_index,
                taken: false,
                ..
            } => {
                // An installed snapshot is the state at its last index: applies
                // resume from there. The entries it covers were applied on the
                // servers whose applies fed the checkpoint, and stay in
                // `by_index` for the applies that follow to agree with.
                let floor = self.applied.last.entry((*range, *server)).or_default();
                *floor = (*floor).max(*last_index);
            }
            // A creation sets the replica's applied floor as an install does: a
            // group born at a split applies from the index its creation names
            // (SHARD.md §8, check 4).
            TraceEvent::RangeCreated {
                range, floor_index, ..
            } => {
                if let Some(server) = traced.node {
                    let floor = self.applied.last.entry((*range, server)).or_default();
                    *floor = (*floor).max(*floor_index);
                }
            }
            TraceEvent::RaftRefused { server, .. } => {
                self.applied.last.retain(|&(_, s), _| s != *server);
            }
            TraceEvent::RangeRemoved { range, .. } => {
                if let Some(server) = traced.node {
                    self.applied.last.remove(&(*range, server));
                }
            }
            TraceEvent::RaftRecovered {
                server,
                range,
                applied: through,
                ..
            } => {
                let from = self
                    .applied
                    .last
                    .get(&(*range, *server))
                    .copied()
                    .unwrap_or(0)
                    + 1;
                for index in from..=*through {
                    let Cell::Entry((term, hash)) = self.logs.at(*range, *server, index) else {
                        return Err(format!(
                            "state machine safety: server {server} recovered an applied index of {through} in group {range} but its log does not hold index {index}"
                        ));
                    };
                    // The log holds the entry and not what applying it did, so the
                    // effect is unknown here and agrees with whatever the replicas
                    // that traced their applies recorded.
                    self.applied.record(
                        *range,
                        *server,
                        index,
                        AppliedEntry {
                            term,
                            hash,
                            effect: None,
                        },
                    )?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Commit on a majority, one event at a time: see [`commit_majority`].
    fn commit_majority(&mut self, event: &TraceEvent) -> Result<(), String> {
        match event {
            TraceEvent::RaftConfig {
                server,
                range,
                old,
                new,
                joint,
                ..
            } => {
                self.configs
                    .insert((*range, *server), (old.clone(), joint.then(|| new.clone())));
            }
            TraceEvent::RaftCommit {
                server,
                range,
                term,
                index,
            } if self.is_leader.get(&(*range, *server)) == Some(term) => {
                let from = self
                    .checked
                    .get(&(*range, *server))
                    .copied()
                    .unwrap_or(0)
                    .max(self.logs.floor_of(*range, *server))
                    + 1;
                let (old, new) = self
                    .configs
                    .get(&(*range, *server))
                    .cloned()
                    .unwrap_or((self.initial_voters(*range), None));
                for i in from..=*index {
                    let entry = match self.logs.at(*range, *server, i) {
                        Cell::Entry(entry) => entry,
                        Cell::Covered => continue,
                        Cell::Absent => {
                            return Err(format!(
                                "commit majority: leader {server} committed index {i} of group {range} in term {term} without holding it"
                            ));
                        }
                    };
                    let on: Vec<u64> = self
                        .logs
                        .servers(*range)
                        .iter()
                        .filter(|&&other| self.logs.holds(*range, other, i, entry))
                        .copied()
                        .collect();
                    let majority_of =
                        |set: &[u64]| set.iter().filter(|s| on.contains(s)).count() * 2 > set.len();
                    if !(majority_of(&old) && new.as_deref().is_none_or(majority_of)) {
                        return Err(format!(
                            "commit majority: leader {server} committed index {i} of group {range} (term {}) in term {term} with it durable on {on:?} of voters {old:?}{}",
                            entry.0,
                            new.as_ref()
                                .map_or(String::new(), |n| format!(" joint with {n:?}"))
                        ));
                    }
                }
                self.checked.insert((*range, *server), *index);
            }
            _ => {}
        }
        Ok(())
    }
}

/// One check's verdict, as a `Result`.
fn verdict(check: &Verdict) -> Result<(), String> {
    check.clone().map_or(Ok(()), Err)
}

/// Every check but the commit-majority one, folded over `events`: the checker
/// [`all`] and the single-check functions read their answer from.
fn folded<'a, I, T>(events: I) -> Checker
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    let mut checker = Checker::without_majority();
    checker.extend(events);
    checker
}

/// Election safety: at most one leader per term *of one group*. Two groups electing
/// in one term are two elections (SHARD.md §8, check 1).
///
/// # Errors
///
/// The first (group, term) with two leaders.
pub fn election_safety<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    verdict(&folded(events).verdicts.election_safety)
}

/// Log matching: if two logs *of one group* hold an entry with the same index and
/// term, they are identical up to that index. Checked at every append, over the
/// logs as they then were, in its inductive form: the new entry's payload agrees
/// with every other replica of that group holding that index and term, and so does
/// the entry below it. Every earlier append was checked the same way in trace
/// order, so agreement at the entry below carries agreement of the whole prefix. A
/// compacted prefix stands in for the entries it covers (RAFT.md §2): it holds only
/// committed entries, so it agrees with anything at the entry below, and two
/// snapshots of one group that end at the same index must carry the same term,
/// which the replay checks at every snapshot event. A `RangeCreated` sets its
/// replica's floor the same way (SHARD.md §8, check 2).
///
/// Two replicas of *different* groups share nothing: one server holding two groups
/// whose logs differ at index 1 is two logs, not one that disagrees with itself.
///
/// That inductive form is also what lets [`Checker`] carry the logs from one call
/// to the next rather than rebuild them: the check at an append reads the logs as
/// they stand, and no earlier append is ever re-examined.
///
/// # Errors
///
/// The first pair of servers and the index of a group that disagree.
pub fn log_matching<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    verdict(&folded(events).verdicts.log_matching)
}

/// Leader completeness: an entry committed in some term of a group is in the log of
/// every later leader *of that group*, at the moment it becomes leader. An entry
/// counts as committed when a leader's commit index reaches it. A leader whose
/// compacted prefix reaches an entry holds it (RAFT.md §2): a snapshot carries every
/// committed entry at or below its last index by construction. The rescan at every
/// `RaftLeader` is of that group's committed set (SHARD.md §8, check 3).
///
/// # Errors
///
/// The first later leader of a group missing an entry committed in it.
pub fn leader_completeness<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    verdict(&folded(events).verdicts.leader_completeness)
}

/// State machine safety: the same index of one group applies the same entry, to the
/// same effect, on every replica of that group, and each replica applies indices
/// once, in order, from the floor its creation or an install sets (SHARD.md §8,
/// check 4). An apply that was durable at a crash but not traced shows at the
/// restart as a recovered applied index past the last traced apply; the entries
/// between are what the replica's log holds at those indices, and they are checked
/// like any other apply, save that a log shows the entry and not what applying it
/// did.
///
/// Snapshots move the applied floor (RAFT.md §2): an installed snapshot puts the
/// replica at its last index, so applies resume from there; a snapshot the replica
/// took itself moves nothing, since it was taken at an index already applied. A
/// refusal ([`TraceEvent::RaftRefused`]) resets every group's floor on that node:
/// its store is gone, and the re-seed that brings it back re-states the snapshot it
/// stands on and legitimately re-applies the entries after it, which must equal what
/// every other replica applied there. A removal ([`TraceEvent::RangeRemoved`]) ends
/// one (group, server)'s memory the same way, so a group removed and later added to
/// the same node starts clean. A store switched to without its repair, the
/// `SnapshotWithoutCurrentLast` install, re-states a *taken* snapshot, the leader's
/// own record, and a recovered applied index its restated log cannot account for,
/// which is exactly what this check reports.
///
/// # Errors
///
/// The first index of a group applied twice or applied differently.
pub fn state_machine_safety<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    verdict(&folded(events).verdicts.state_machine_safety)
}

/// Commit on a majority of the configuration in force (RAFT.md §2 check 3): every
/// entry a leader's commit index covers was appended, durably as the trace reports
/// it, on a majority of the voters in force on that leader when it committed, with
/// the leader's term and payload at that index — a majority of both voter sets
/// while the configuration is joint (thesis §4.3). The configuration in force is
/// followed through `RaftConfig` events, per (group, server); a replica that has
/// emitted none yet is on its group's first configuration, the voters of its
/// `RangeCreated`, or servers 1 through `servers` for a group whose creation the
/// trace does not hold (SHARD.md §8, check 3). A replica whose compacted prefix
/// reaches the index holds the entry (RAFT.md §2): a snapshot is the durable
/// committed prefix through its last index. A leader's own compacted prefix likewise
/// stands in for entries below its first index, which arises when a restarted leader
/// re-commits from its snapshot's floor.
///
/// # Errors
///
/// The first committed index short of a majority.
pub fn commit_majority<'a, I, T>(events: I, servers: usize) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    let mut checker = Checker::new(servers);
    checker.extend(events);
    verdict(&checker.verdicts.commit_majority)
}

/// Commit by the current term (§5.4.2): a leader's commit index only ever lands on
/// an entry of its own term in its own group; older entries commit by being below
/// one.
///
/// # Errors
///
/// The first leader commit at an index of an older term.
pub fn commit_by_current_term<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    verdict(&folded(events).verdicts.commit_by_current_term)
}

/// Committed entries stay: no replica truncates its group's log at or below its own
/// commit index in that group. Compaction is not truncation: it deletes entries a
/// snapshot stands in for and traces [`TraceEvent::RaftCompacted`], not
/// [`TraceEvent::RaftTruncate`]. A refusal resets the commit memory of every group
/// on the node: its store is gone, and the re-seed restart legitimately re-states a
/// log shorter than what it had once committed, the snapshot standing in for the
/// prefix and the leader re-sending the rest (RAFT.md §3). A `RangeRemoved` resets
/// that one replica's the same way.
///
/// # Errors
///
/// The first truncation that removes a committed entry.
pub fn committed_never_truncated<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    verdict(&folded(events).verdicts.committed_never_truncated)
}

/// Every log invariant at once, the first violation in the order above; the
/// majority check needs the cluster size and is [`commit_majority`].
///
/// # Errors
///
/// See each check.
pub fn all<'a, I, T>(events: I) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    T: Into<Traced<'a>>,
{
    folded(events).verdict()
}

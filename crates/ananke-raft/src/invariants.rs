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

use ananke_env::TraceEvent;

use crate::types::{Index, Term};

/// One server's log as the trace shows it: index to (term, payload hash).
type Log = BTreeMap<Index, (Term, u64)>;

/// Every server's log and snapshot floor as the trace shows them (RAFT.md §2): an
/// installed snapshot replaces the log prefix at or below its last index, and
/// either kind of snapshot means the server durably holds the committed prefix
/// through it.
#[derive(Default)]
struct Logs {
    logs: BTreeMap<u64, Log>,
    /// Per server, the last snapshot's (last index, last term).
    floor: BTreeMap<u64, (Index, Term)>,
}

impl Logs {
    /// Replays one append, truncation or snapshot into the logs. A snapshot must
    /// agree with any entry another server holds at its last index, the
    /// log-matching check at the boundary the prefix no longer shows.
    fn replay(&mut self, event: &TraceEvent) -> Result<(), String> {
        match event {
            TraceEvent::RaftAppend {
                server,
                index,
                entry_term,
                hash,
            } => {
                self.logs
                    .entry(*server)
                    .or_default()
                    .insert(*index, (*entry_term, *hash));
            }
            TraceEvent::RaftTruncate { server, from_index } => {
                self.logs
                    .entry(*server)
                    .or_default()
                    .retain(|&index, _| index < *from_index);
            }
            TraceEvent::RaftSnapshot {
                server,
                last_index,
                last_term,
                taken,
            } => {
                // Two snapshots cover committed prefixes, so at one index their
                // terms agree: the log-matching check at a boundary the logs no
                // longer show. An uncommitted entry another log holds at this
                // index may legitimately differ; the applied entries may not,
                // which state machine safety checks.
                for (other, (index, term)) in &self.floor {
                    if index == last_index && term != last_term {
                        return Err(format!(
                            "log matching: server {server}'s snapshot at index {last_index} has term {last_term} but server {other}'s snapshot there has term {term}"
                        ));
                    }
                }
                let floor = self.floor.entry(*server).or_default();
                if !taken {
                    // Installed: the store is the leader's checkpoint now, and the
                    // log prefix the snapshot covers is gone with the old store.
                    // The floor is exactly the snapshot's, lower or higher than
                    // before: a re-seeded server's earlier floor belonged to the
                    // store it lost (the nightly's seed 7381, D-030).
                    *floor = (*last_index, *last_term);
                    self.logs
                        .entry(*server)
                        .or_default()
                        .retain(|&index, _| index > *last_index);
                } else if *last_index >= floor.0 {
                    *floor = (*last_index, *last_term);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn log(&self, server: u64) -> Option<&Log> {
        self.logs.get(&server)
    }

    /// The server's snapshot floor: it durably holds the committed prefix through
    /// this index.
    fn floor_of(&self, server: u64) -> Index {
        self.floor.get(&server).map_or(0, |&(index, _)| index)
    }

    /// Whether `server` durably holds `entry` at `index`: in its log, or inside
    /// its snapshot, which carries the committed prefix by construction.
    fn holds(&self, server: u64, index: Index, entry: (Term, u64)) -> bool {
        if self.floor_of(server) >= index {
            return true;
        }
        self.logs
            .get(&server)
            .is_some_and(|log| log.get(&index) == Some(&entry))
    }

    /// What the trace shows of `server` at `index`: the entry, the compacted
    /// prefix standing in for it, or nothing. Index 0 counts as covered: every log
    /// agrees before its first entry.
    fn at(&self, server: u64, index: Index) -> Cell {
        if index <= self.floor_of(server) || index == 0 {
            return Cell::Covered;
        }
        match self.logs.get(&server).and_then(|log| log.get(&index)) {
            Some(&entry) => Cell::Entry(entry),
            None => Cell::Absent,
        }
    }

    /// Every server the trace shows a log or a snapshot for.
    fn servers(&self) -> BTreeSet<u64> {
        self.logs.keys().chain(self.floor.keys()).copied().collect()
    }
}

/// One position of one server's log as [`Logs::at`] shows it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cell {
    /// The entry, as (term, hash).
    Entry((Term, u64)),
    /// Inside the server's compacted prefix: committed there by construction, so
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

/// What state machine safety has seen applied: each index once per server, in
/// order, and the same entry on every server.
#[derive(Default)]
struct Applied {
    by_index: BTreeMap<Index, (Term, u64)>,
    last: BTreeMap<u64, Index>,
}

impl Applied {
    fn record(&mut self, server: u64, index: Index, entry: (Term, u64)) -> Result<(), String> {
        let previous = self.last.get(&server).copied().unwrap_or(0);
        if index != previous + 1 {
            return Err(format!(
                "state machine safety: server {server} applied index {index} after {previous}"
            ));
        }
        self.last.insert(server, index);
        if let Some(other) = self.by_index.insert(index, entry)
            && other != entry
        {
            return Err(format!(
                "state machine safety: index {index} was applied as term {} on one server and term {} on server {server}",
                other.0, entry.0
            ));
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
/// ```
/// use ananke_env::TraceEvent;
/// use ananke_raft::invariants::Checker;
///
/// let leader = |server, term| TraceEvent::RaftLeader {
///     server,
///     term,
///     last_index: 0,
/// };
/// let mut checker = Checker::new(3);
/// checker.push(&leader(1, 7));
/// assert!(checker.verdict().is_ok());
/// checker.push(&leader(2, 7));
/// assert_eq!(
///     checker.verdict().unwrap_err(),
///     "election safety: servers 1 and 2 both led term 7"
/// );
/// ```
pub struct Checker {
    /// The voters of the initial configuration: servers 1 through `servers`.
    initial: Vec<u64>,
    /// Whether the commit-majority check runs; [`all`] does not ask for it.
    checks_majority: bool,
    /// The logs as the trace shows them, replayed once for every check that reads
    /// them rather than once per check.
    logs: Logs,
    /// Whether the replay has reported a violation, after which no check that
    /// survives it reads the logs again.
    logs_stopped: bool,
    /// Who leads, as `RaftTerm` and `RaftLeader` say: shared by the three checks
    /// that ask whether a commit is a leader's.
    is_leader: BTreeMap<u64, Term>,
    /// Election safety: the leader of each term.
    leaders: BTreeMap<Term, u64>,
    /// Leader completeness: each committed entry as (term, hash, the term it was
    /// committed in), by the first leader whose commit index reached it.
    committed: BTreeMap<Index, (Term, u64, Term)>,
    /// Committed entries stay: the highest commit index each server has reached.
    commit: BTreeMap<u64, Index>,
    /// State machine safety: what has been applied, and where.
    applied: Applied,
    /// Commit on a majority: how far each leader's commits have been checked, and
    /// the configuration in force on each server.
    checked: BTreeMap<u64, Index>,
    configs: BTreeMap<u64, (Vec<u64>, Option<Vec<u64>>)>,
    /// Where each check stands.
    verdicts: Verdicts,
}

impl Checker {
    /// A checker for a cluster of `servers` servers, running every check this
    /// module offers: what [`all`] covers, and [`commit_majority`], which needs
    /// the cluster size for the initial configuration.
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

    /// Folds one more event into every check.
    ///
    /// It returns nothing, because a slice of events has no verdict of its own:
    /// [`all`] reports the first violation in *its* order of the checks, not the
    /// earliest violation in the trace — an election safety violation at the last
    /// event outranks a log matching violation at the first — so only
    /// [`verdict`](Self::verdict), which sees every check at once, can answer.
    pub fn push(&mut self, event: &TraceEvent) {
        self.replay(event);
        self.track_leader(event);
        if self.verdicts.election_safety.is_none()
            && let Err(why) = self.election_safety(event)
        {
            self.verdicts.election_safety = Some(why);
        }
        if self.verdicts.log_matching.is_none()
            && let Err(why) = self.log_matching(event)
        {
            self.verdicts.log_matching = Some(why);
        }
        if self.verdicts.leader_completeness.is_none()
            && let Err(why) = self.leader_completeness(event)
        {
            self.verdicts.leader_completeness = Some(why);
        }
        if self.verdicts.commit_by_current_term.is_none()
            && let Err(why) = self.commit_by_current_term(event)
        {
            self.verdicts.commit_by_current_term = Some(why);
        }
        if self.verdicts.committed_never_truncated.is_none()
            && let Err(why) = self.committed_never_truncated(event)
        {
            self.verdicts.committed_never_truncated = Some(why);
        }
        if self.verdicts.state_machine_safety.is_none()
            && let Err(why) = self.state_machine_safety(event)
        {
            self.verdicts.state_machine_safety = Some(why);
        }
        if self.checks_majority
            && self.verdicts.commit_majority.is_none()
            && let Err(why) = self.commit_majority(event)
        {
            self.verdicts.commit_majority = Some(why);
        }
    }

    /// Folds a slice of events into every check, in order.
    pub fn extend<'a, I>(&mut self, events: I)
    where
        I: IntoIterator<Item = &'a TraceEvent>,
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
    fn replay(&mut self, event: &TraceEvent) {
        if self.logs_stopped {
            return;
        }
        let Err(why) = self.logs.replay(event) else {
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

    /// Follows who leads, which three of the checks ask of every commit.
    fn track_leader(&mut self, event: &TraceEvent) {
        match event {
            TraceEvent::RaftTerm { server, role, term } => {
                if *role == "leader" {
                    self.is_leader.insert(*server, *term);
                } else {
                    self.is_leader.remove(server);
                }
            }
            TraceEvent::RaftLeader { server, term, .. } => {
                self.is_leader.insert(*server, *term);
            }
            _ => {}
        }
    }

    /// Election safety, one event at a time: see [`election_safety`].
    fn election_safety(&mut self, event: &TraceEvent) -> Result<(), String> {
        if let TraceEvent::RaftLeader { server, term, .. } = event
            && let Some(other) = self.leaders.insert(*term, *server)
            && other != *server
        {
            return Err(format!(
                "election safety: servers {other} and {server} both led term {term}"
            ));
        }
        Ok(())
    }

    /// Log matching, one event at a time: see [`log_matching`].
    fn log_matching(&self, event: &TraceEvent) -> Result<(), String> {
        let TraceEvent::RaftAppend {
            server,
            index,
            entry_term,
            hash,
        } = event
        else {
            return Ok(());
        };
        let below = self.logs.at(*server, index - 1);
        for other in self.logs.servers() {
            if other == *server {
                continue;
            }
            let Cell::Entry((term, other_hash)) = self.logs.at(other, *index) else {
                continue;
            };
            if term != *entry_term {
                continue;
            }
            if other_hash != *hash {
                return Err(format!(
                    "log matching: servers {server} and {other} hold index {index} in term {entry_term} with different payloads"
                ));
            }
            if !below.agrees(self.logs.at(other, index - 1)) {
                return Err(format!(
                    "log matching: servers {server} and {other} agree at index {index} term {entry_term} but differ at index {}",
                    index - 1
                ));
            }
        }
        Ok(())
    }

    /// Leader completeness, one event at a time: see [`leader_completeness`].
    fn leader_completeness(&mut self, event: &TraceEvent) -> Result<(), String> {
        match event {
            TraceEvent::RaftLeader { server, term, .. } => {
                for (index, (entry_term, hash, in_term)) in &self.committed {
                    if in_term < term && !self.logs.holds(*server, *index, (*entry_term, *hash)) {
                        return Err(format!(
                            "leader completeness: index {index} (term {entry_term}) was committed in term {in_term} but server {server}, leader of term {term}, does not hold it"
                        ));
                    }
                }
            }
            TraceEvent::RaftCommit {
                server,
                term,
                index,
            } if self.is_leader.get(server) == Some(term) => {
                if let Some(log) = self.logs.log(*server) {
                    for (i, (entry_term, hash)) in log.range(..=index) {
                        self.committed
                            .entry(*i)
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
            term,
            index,
        } = event
        else {
            return Ok(());
        };
        if self.is_leader.get(server) != Some(term) {
            return Ok(());
        }
        let entry_term = self
            .logs
            .log(*server)
            .and_then(|log| log.get(index))
            .map(|e| e.0);
        if entry_term != Some(*term) {
            return Err(format!(
                "commit by current term: leader {server} of term {term} committed index {index}, whose entry has term {entry_term:?}"
            ));
        }
        Ok(())
    }

    /// Committed entries stay, one event at a time: see
    /// [`committed_never_truncated`].
    fn committed_never_truncated(&mut self, event: &TraceEvent) -> Result<(), String> {
        match event {
            TraceEvent::RaftCommit { server, index, .. } => {
                let known = self.commit.entry(*server).or_default();
                *known = (*known).max(*index);
            }
            TraceEvent::RaftRefused { server, .. } => {
                self.commit.remove(server);
            }
            TraceEvent::RaftTruncate { server, from_index } => {
                let known = self.commit.get(server).copied().unwrap_or(0);
                if *from_index <= known {
                    return Err(format!(
                        "committed entries stay: server {server} truncated from index {from_index} with commit index {known}"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// State machine safety, one event at a time: see [`state_machine_safety`].
    fn state_machine_safety(&mut self, event: &TraceEvent) -> Result<(), String> {
        match event {
            TraceEvent::RaftApply {
                server,
                index,
                entry_term,
                hash,
            } => self.applied.record(*server, *index, (*entry_term, *hash))?,
            TraceEvent::RaftSnapshot {
                server,
                last_index,
                taken: false,
                ..
            } => {
                // An installed snapshot is the state at its last index: applies
                // resume from there. The entries it covers were applied on the
                // servers whose applies fed the checkpoint, and stay in
                // `by_index` for the applies that follow to agree with.
                let floor = self.applied.last.entry(*server).or_default();
                *floor = (*floor).max(*last_index);
            }
            TraceEvent::RaftRefused { server, .. } => {
                self.applied.last.remove(server);
            }
            TraceEvent::RaftRecovered {
                server,
                applied: through,
                ..
            } => {
                let from = self.applied.last.get(server).copied().unwrap_or(0) + 1;
                for index in from..=*through {
                    let Cell::Entry(entry) = self.logs.at(*server, index) else {
                        return Err(format!(
                            "state machine safety: server {server} recovered an applied index of {through} but its log does not hold index {index}"
                        ));
                    };
                    self.applied.record(*server, index, entry)?;
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
                old,
                new,
                joint,
                ..
            } => {
                self.configs
                    .insert(*server, (old.clone(), joint.then(|| new.clone())));
            }
            TraceEvent::RaftCommit {
                server,
                term,
                index,
            } if self.is_leader.get(server) == Some(term) => {
                let from = self
                    .checked
                    .get(server)
                    .copied()
                    .unwrap_or(0)
                    .max(self.logs.floor_of(*server))
                    + 1;
                let (old, new) = self
                    .configs
                    .get(server)
                    .cloned()
                    .unwrap_or((self.initial.clone(), None));
                for i in from..=*index {
                    let entry = match self.logs.at(*server, i) {
                        Cell::Entry(entry) => entry,
                        Cell::Covered => continue,
                        Cell::Absent => {
                            return Err(format!(
                                "commit majority: leader {server} committed index {i} in term {term} without holding it"
                            ));
                        }
                    };
                    let on: Vec<u64> = self
                        .logs
                        .servers()
                        .iter()
                        .filter(|&&other| self.logs.holds(other, i, entry))
                        .copied()
                        .collect();
                    let majority_of =
                        |set: &[u64]| set.iter().filter(|s| on.contains(s)).count() * 2 > set.len();
                    if !(majority_of(&old) && new.as_deref().is_none_or(majority_of)) {
                        return Err(format!(
                            "commit majority: leader {server} committed index {i} (term {}) in term {term} with it durable on {on:?} of voters {old:?}{}",
                            entry.0,
                            new.as_ref()
                                .map_or(String::new(), |n| format!(" joint with {n:?}"))
                        ));
                    }
                }
                self.checked.insert(*server, *index);
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
fn folded(events: &[TraceEvent]) -> Checker {
    let mut checker = Checker::without_majority();
    checker.extend(events);
    checker
}

/// Election safety: at most one leader per term.
///
/// # Errors
///
/// The first term with two leaders.
pub fn election_safety(events: &[TraceEvent]) -> Result<(), String> {
    verdict(&folded(events).verdicts.election_safety)
}

/// Log matching: if two logs hold an entry with the same index and term, they are
/// identical up to that index. Checked at every append, over the logs as they then
/// were, in its inductive form: the new entry's payload agrees with every other
/// server holding that index and term, and so does the entry below it. Every earlier
/// append was checked the same way in trace order, so agreement at the entry below
/// carries agreement of the whole prefix. A compacted prefix stands in for the
/// entries it covers (RAFT.md §2): it holds only committed entries, so it agrees
/// with anything at the entry below, and two snapshots that end at the same index
/// must carry the same term, which the replay checks at every snapshot event.
///
/// That inductive form is also what lets [`Checker`] carry the logs from one call
/// to the next rather than rebuild them: the check at an append reads the logs as
/// they stand, and no earlier append is ever re-examined.
///
/// # Errors
///
/// The first pair of servers and the index that disagree.
pub fn log_matching(events: &[TraceEvent]) -> Result<(), String> {
    verdict(&folded(events).verdicts.log_matching)
}

/// Leader completeness: an entry committed in some term is in the log of every
/// leader of every later term, at the moment it becomes leader. An entry counts as
/// committed when a leader's commit index reaches it. A leader whose compacted
/// prefix reaches an entry holds it (RAFT.md §2): a snapshot carries every
/// committed entry at or below its last index by construction.
///
/// # Errors
///
/// The first later leader missing a committed entry.
pub fn leader_completeness(events: &[TraceEvent]) -> Result<(), String> {
    verdict(&folded(events).verdicts.leader_completeness)
}

/// State machine safety: the same index applies the same entry on every server, and
/// each server applies indices once, in order. An apply that was durable at a crash
/// but not traced shows at the restart as a recovered applied index past the last
/// traced apply; the entries between are what the server's log holds at those
/// indices, and they are checked like any other apply.
///
/// Snapshots move the applied floor (RAFT.md §2): an installed snapshot puts the
/// server at its last index, so applies resume from there; a snapshot the server
/// took itself moves nothing, since it was taken at an index already applied. A
/// refusal ([`TraceEvent::RaftRefused`]) resets the server's floor: its store is
/// gone, and the re-seed that brings it back re-states the snapshot it stands on
/// and legitimately re-applies the entries after it, which must equal what every
/// other server applied there. A store switched to without its repair, the
/// `SnapshotWithoutCurrentLast` install, re-states a *taken* snapshot, the
/// leader's own record, and a recovered applied index its restated log cannot
/// account for, which is exactly what this check reports.
///
/// # Errors
///
/// The first index applied twice or applied differently.
pub fn state_machine_safety(events: &[TraceEvent]) -> Result<(), String> {
    verdict(&folded(events).verdicts.state_machine_safety)
}

/// Commit on a majority of the configuration in force (RAFT.md §2 check 3): every
/// entry a leader's commit index covers was appended, durably as the trace reports
/// it, on a majority of the voters in force on that leader when it committed, with
/// the leader's term and payload at that index — a majority of both voter sets
/// while the configuration is joint (thesis §4.3). The configuration in force is
/// followed through `RaftConfig` events; a server that has emitted none yet is on
/// the initial configuration, servers 1 through `servers`. A server whose
/// compacted prefix reaches the index holds the entry (RAFT.md §2): a snapshot is
/// the durable committed prefix through its last index. A leader's own compacted
/// prefix likewise stands in for entries below its first index, which arises when
/// a restarted leader re-commits from its snapshot's floor.
///
/// # Errors
///
/// The first committed index short of a majority.
pub fn commit_majority(events: &[TraceEvent], servers: usize) -> Result<(), String> {
    let mut checker = Checker::new(servers);
    checker.extend(events);
    verdict(&checker.verdicts.commit_majority)
}

/// Commit by the current term (§5.4.2): a leader's commit index only ever lands on
/// an entry of its own term; older entries commit by being below one.
///
/// # Errors
///
/// The first leader commit at an index of an older term.
pub fn commit_by_current_term(events: &[TraceEvent]) -> Result<(), String> {
    verdict(&folded(events).verdicts.commit_by_current_term)
}

/// Committed entries stay: no server truncates at or below its own commit index.
/// Compaction is not truncation: it deletes entries a snapshot stands in for and
/// traces [`TraceEvent::RaftCompacted`], not [`TraceEvent::RaftTruncate`]. A
/// refusal resets the server's commit memory: its store is gone, and the re-seed
/// restart legitimately re-states a log shorter than what it had once committed,
/// the snapshot standing in for the prefix and the leader re-sending the rest
/// (RAFT.md §3).
///
/// # Errors
///
/// The first truncation that removes a committed entry.
pub fn committed_never_truncated(events: &[TraceEvent]) -> Result<(), String> {
    verdict(&folded(events).verdicts.committed_never_truncated)
}

/// Every log invariant at once, the first violation in the order above; the
/// majority check needs the cluster size and is [`commit_majority`].
///
/// # Errors
///
/// See each check.
pub fn all(events: &[TraceEvent]) -> Result<(), String> {
    folded(events).verdict()
}

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

/// Election safety: at most one leader per term.
///
/// # Errors
///
/// The first term with two leaders.
pub fn election_safety(events: &[TraceEvent]) -> Result<(), String> {
    let mut leaders: BTreeMap<Term, u64> = BTreeMap::new();
    for event in events {
        if let TraceEvent::RaftLeader { server, term, .. } = event
            && let Some(other) = leaders.insert(*term, *server)
            && other != *server
        {
            return Err(format!(
                "election safety: servers {other} and {server} both led term {term}"
            ));
        }
    }
    Ok(())
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
/// # Errors
///
/// The first pair of servers and the index that disagree.
pub fn log_matching(events: &[TraceEvent]) -> Result<(), String> {
    let mut logs = Logs::default();
    for event in events {
        logs.replay(event)?;
        let TraceEvent::RaftAppend {
            server,
            index,
            entry_term,
            hash,
        } = event
        else {
            continue;
        };
        let below = logs.at(*server, index - 1);
        for other in logs.servers() {
            if other == *server {
                continue;
            }
            let Cell::Entry((term, other_hash)) = logs.at(other, *index) else {
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
            if !below.agrees(logs.at(other, index - 1)) {
                return Err(format!(
                    "log matching: servers {server} and {other} agree at index {index} term {entry_term} but differ at index {}",
                    index - 1
                ));
            }
        }
    }
    Ok(())
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
    let mut logs = Logs::default();
    let mut committed: BTreeMap<Index, (Term, u64, Term)> = BTreeMap::new();
    let mut is_leader: BTreeMap<u64, Term> = BTreeMap::new();
    for event in events {
        logs.replay(event)?;
        match event {
            TraceEvent::RaftTerm { server, role, term } => {
                if *role == "leader" {
                    is_leader.insert(*server, *term);
                } else {
                    is_leader.remove(server);
                }
            }
            TraceEvent::RaftLeader { server, term, .. } => {
                is_leader.insert(*server, *term);
                for (index, (entry_term, hash, in_term)) in &committed {
                    if in_term < term && !logs.holds(*server, *index, (*entry_term, *hash)) {
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
            } if is_leader.get(server) == Some(term) => {
                if let Some(log) = logs.log(*server) {
                    for (i, (entry_term, hash)) in log.range(..=index) {
                        committed.entry(*i).or_insert((*entry_term, *hash, *term));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
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
    /// One apply: in order per server, and the same entry as every other server's.
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
    let mut logs = Logs::default();
    let mut applied = Applied {
        by_index: BTreeMap::new(),
        last: BTreeMap::new(),
    };
    for event in events {
        logs.replay(event)?;
        match event {
            TraceEvent::RaftApply {
                server,
                index,
                entry_term,
                hash,
            } => applied.record(*server, *index, (*entry_term, *hash))?,
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
                let floor = applied.last.entry(*server).or_default();
                *floor = (*floor).max(*last_index);
            }
            TraceEvent::RaftRefused { server, .. } => {
                applied.last.remove(server);
            }
            TraceEvent::RaftRecovered {
                server,
                applied: through,
                ..
            } => {
                let from = applied.last.get(server).copied().unwrap_or(0) + 1;
                for index in from..=*through {
                    let Cell::Entry(entry) = logs.at(*server, index) else {
                        return Err(format!(
                            "state machine safety: server {server} recovered an applied index of {through} but its log does not hold index {index}"
                        ));
                    };
                    applied.record(*server, index, entry)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
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
    let initial: Vec<u64> = (1..=servers as u64).collect();
    let mut logs = Logs::default();
    let mut is_leader: BTreeMap<u64, Term> = BTreeMap::new();
    let mut checked: BTreeMap<u64, Index> = BTreeMap::new();
    let mut configs: BTreeMap<u64, (Vec<u64>, Option<Vec<u64>>)> = BTreeMap::new();
    for event in events {
        logs.replay(event)?;
        match event {
            TraceEvent::RaftConfig {
                server,
                old,
                new,
                joint,
                ..
            } => {
                configs.insert(*server, (old.clone(), joint.then(|| new.clone())));
            }
            TraceEvent::RaftTerm { server, role, term } => {
                if *role == "leader" {
                    is_leader.insert(*server, *term);
                } else {
                    is_leader.remove(server);
                }
            }
            TraceEvent::RaftLeader { server, term, .. } => {
                is_leader.insert(*server, *term);
            }
            TraceEvent::RaftCommit {
                server,
                term,
                index,
            } if is_leader.get(server) == Some(term) => {
                let from = checked
                    .get(server)
                    .copied()
                    .unwrap_or(0)
                    .max(logs.floor_of(*server))
                    + 1;
                let (old, new) = configs
                    .get(server)
                    .cloned()
                    .unwrap_or((initial.clone(), None));
                for i in from..=*index {
                    let entry = match logs.at(*server, i) {
                        Cell::Entry(entry) => entry,
                        Cell::Covered => continue,
                        Cell::Absent => {
                            return Err(format!(
                                "commit majority: leader {server} committed index {i} in term {term} without holding it"
                            ));
                        }
                    };
                    let on: Vec<u64> = logs
                        .servers()
                        .iter()
                        .filter(|&&other| logs.holds(other, i, entry))
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
                checked.insert(*server, *index);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Commit by the current term (§5.4.2): a leader's commit index only ever lands on
/// an entry of its own term; older entries commit by being below one.
///
/// # Errors
///
/// The first leader commit at an index of an older term.
pub fn commit_by_current_term(events: &[TraceEvent]) -> Result<(), String> {
    let mut logs = Logs::default();
    let mut is_leader: BTreeMap<u64, Term> = BTreeMap::new();
    for event in events {
        logs.replay(event)?;
        match event {
            TraceEvent::RaftTerm { server, role, term } => {
                if *role == "leader" {
                    is_leader.insert(*server, *term);
                } else {
                    is_leader.remove(server);
                }
            }
            TraceEvent::RaftLeader { server, term, .. } => {
                is_leader.insert(*server, *term);
            }
            TraceEvent::RaftCommit {
                server,
                term,
                index,
            } if is_leader.get(server) == Some(term) => {
                let entry_term = logs
                    .log(*server)
                    .and_then(|log| log.get(index))
                    .map(|e| e.0);
                if entry_term != Some(*term) {
                    return Err(format!(
                        "commit by current term: leader {server} of term {term} committed index {index}, whose entry has term {entry_term:?}"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
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
    let mut commit: BTreeMap<u64, Index> = BTreeMap::new();
    for event in events {
        match event {
            TraceEvent::RaftCommit { server, index, .. } => {
                let known = commit.entry(*server).or_default();
                *known = (*known).max(*index);
            }
            TraceEvent::RaftRefused { server, .. } => {
                commit.remove(server);
            }
            TraceEvent::RaftTruncate { server, from_index } => {
                let known = commit.get(server).copied().unwrap_or(0);
                if *from_index <= known {
                    return Err(format!(
                        "committed entries stay: server {server} truncated from index {from_index} with commit index {known}"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Every log invariant at once, the first violation in the order above; the
/// majority check needs the cluster size and is [`commit_majority`].
///
/// # Errors
///
/// See each check.
pub fn all(events: &[TraceEvent]) -> Result<(), String> {
    election_safety(events)?;
    log_matching(events)?;
    leader_completeness(events)?;
    commit_by_current_term(events)?;
    committed_never_truncated(events)?;
    state_machine_safety(events)
}

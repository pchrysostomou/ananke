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

use std::collections::BTreeMap;

use ananke_env::TraceEvent;

use crate::types::{Index, Term};

/// One server's log as the trace shows it: index to (term, payload hash).
type Log = BTreeMap<Index, (Term, u64)>;

/// Replays one append or truncation into the logs.
fn replay(logs: &mut BTreeMap<u64, Log>, event: &TraceEvent) {
    match event {
        TraceEvent::RaftAppend {
            server,
            index,
            entry_term,
            hash,
        } => {
            logs.entry(*server)
                .or_default()
                .insert(*index, (*entry_term, *hash));
        }
        TraceEvent::RaftTruncate { server, from_index } => {
            logs.entry(*server)
                .or_default()
                .retain(|&index, _| index < *from_index);
        }
        _ => {}
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
/// carries agreement of the whole prefix.
///
/// # Errors
///
/// The first pair of servers and the index that disagree.
pub fn log_matching(events: &[TraceEvent]) -> Result<(), String> {
    let mut logs: BTreeMap<u64, Log> = BTreeMap::new();
    for event in events {
        replay(&mut logs, event);
        let TraceEvent::RaftAppend {
            server,
            index,
            entry_term,
            hash,
        } = event
        else {
            continue;
        };
        let below = if *index > 1 {
            logs[server].get(&(index - 1)).copied()
        } else {
            None
        };
        for (other, log) in &logs {
            if other == server {
                continue;
            }
            let Some((term, other_hash)) = log.get(index) else {
                continue;
            };
            if term != entry_term {
                continue;
            }
            if other_hash != hash {
                return Err(format!(
                    "log matching: servers {server} and {other} hold index {index} in term {entry_term} with different payloads"
                ));
            }
            if *index > 1 && log.get(&(index - 1)).copied() != below {
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
/// committed when a leader's commit index reaches it.
///
/// # Errors
///
/// The first later leader missing a committed entry.
pub fn leader_completeness(events: &[TraceEvent]) -> Result<(), String> {
    let mut logs: BTreeMap<u64, Log> = BTreeMap::new();
    let mut committed: BTreeMap<Index, (Term, u64, Term)> = BTreeMap::new();
    let mut is_leader: BTreeMap<u64, Term> = BTreeMap::new();
    for event in events {
        replay(&mut logs, event);
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
                let log = logs.get(server).cloned().unwrap_or_default();
                for (index, (entry_term, hash, in_term)) in &committed {
                    if in_term < term && log.get(index) != Some(&(*entry_term, *hash)) {
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
                if let Some(log) = logs.get(server) {
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
    let mut logs: BTreeMap<u64, Log> = BTreeMap::new();
    let mut applied = Applied {
        by_index: BTreeMap::new(),
        last: BTreeMap::new(),
    };
    for event in events {
        replay(&mut logs, event);
        match event {
            TraceEvent::RaftApply {
                server,
                index,
                entry_term,
                hash,
            } => applied.record(*server, *index, (*entry_term, *hash))?,
            TraceEvent::RaftRecovered {
                server,
                applied: through,
                ..
            } => {
                let from = applied.last.get(server).copied().unwrap_or(0) + 1;
                for index in from..=*through {
                    let Some(entry) = logs.get(server).and_then(|log| log.get(&index)) else {
                        return Err(format!(
                            "state machine safety: server {server} recovered an applied index of {through} but its log does not hold index {index}"
                        ));
                    };
                    applied.record(*server, index, *entry)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Commit on a majority: every entry a leader's commit index covers was appended,
/// durably as the trace reports it, on a majority of the `servers` when the leader
/// committed it, with the leader's term and payload at that index.
///
/// # Errors
///
/// The first committed index short of a majority.
pub fn commit_majority(events: &[TraceEvent], servers: usize) -> Result<(), String> {
    let mut logs: BTreeMap<u64, Log> = BTreeMap::new();
    let mut is_leader: BTreeMap<u64, Term> = BTreeMap::new();
    let mut checked: BTreeMap<u64, Index> = BTreeMap::new();
    for event in events {
        replay(&mut logs, event);
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
                let from = checked.get(server).copied().unwrap_or(0) + 1;
                let log = logs.get(server).cloned().unwrap_or_default();
                for i in from..=*index {
                    let Some(entry) = log.get(&i) else {
                        return Err(format!(
                            "commit majority: leader {server} committed index {i} in term {term} without holding it"
                        ));
                    };
                    let on = logs
                        .values()
                        .filter(|other| other.get(&i) == Some(entry))
                        .count();
                    if on * 2 <= servers {
                        return Err(format!(
                            "commit majority: leader {server} committed index {i} (term {}) in term {term} with it durable on {on} of {servers} servers",
                            entry.0
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
    let mut logs: BTreeMap<u64, Log> = BTreeMap::new();
    let mut is_leader: BTreeMap<u64, Term> = BTreeMap::new();
    for event in events {
        replay(&mut logs, event);
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
                let entry_term = logs.get(server).and_then(|log| log.get(index)).map(|e| e.0);
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

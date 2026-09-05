//! The safety properties of Figure 3 of the paper, as folds over the trace (RAFT.md
//! §2). Each takes every event of a run so far and returns the first violation, so a
//! sweep can run them after every crash and at the end, and a unit test over a
//! handful of cores can run them over the outputs it collected.

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
/// were.
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
        let mine = &logs[server];
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
            for (i, entry) in mine.range(..index) {
                if log.get(i) != Some(entry) {
                    return Err(format!(
                        "log matching: servers {server} and {other} agree at index {index} term {entry_term} but differ at index {i}"
                    ));
                }
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
/// each server applies indices once, in order.
///
/// # Errors
///
/// The first index applied twice or applied differently.
pub fn state_machine_safety(events: &[TraceEvent]) -> Result<(), String> {
    let mut applied: BTreeMap<Index, (Term, u64)> = BTreeMap::new();
    let mut last: BTreeMap<u64, Index> = BTreeMap::new();
    for event in events {
        if let TraceEvent::RaftApply {
            server,
            index,
            entry_term,
            hash,
        } = event
        {
            let previous = last.get(server).copied().unwrap_or(0);
            if *index != previous + 1 {
                return Err(format!(
                    "state machine safety: server {server} applied index {index} after {previous}"
                ));
            }
            last.insert(*server, *index);
            if let Some(other) = applied.insert(*index, (*entry_term, *hash))
                && other != (*entry_term, *hash)
            {
                return Err(format!(
                    "state machine safety: index {index} was applied as term {} on one server and term {entry_term} on server {server}",
                    other.0
                ));
            }
        }
    }
    Ok(())
}

/// Every log invariant at once, the first violation in the order above.
///
/// # Errors
///
/// See each check.
pub fn all(events: &[TraceEvent]) -> Result<(), String> {
    election_safety(events)?;
    log_matching(events)?;
    leader_completeness(events)?;
    state_machine_safety(events)
}

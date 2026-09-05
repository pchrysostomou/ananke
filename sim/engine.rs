//! The Phase 1 crash-injection property (SPEC.md §2.8) for the engine: a write-ahead
//! log in front of memtables, flushed to SSTables under a manifest (D-020 to D-022).
//! Writers put and delete random keys while readers check what they see, the harness
//! crashes the node at random points with every §1.3 fault on, including filesystem
//! latency so a crash lands inside a flush as often as between two, and after each
//! recovery the engine's state is checked against a `BTreeMap` model.
//!
//! The log-level obligation is the WAL scenario's (`wal::check_epoch`), told what the
//! tables cover and which losses the trace explains: a table the manifest lists that
//! could not be read is excused if its sync was lost or bit rot hit it, and a manifest
//! that could not be read likewise, with everything flushed since. Nothing else is:
//! a table or manifest gone without a fault is a bug. On top of it, the state after
//! recovery must equal the model folded over exactly the records that survived, and
//! every live read during the run must return what the model holds for a key with no
//! write in flight. All three [`Variant`]s run through the same checks; the correct
//! engine must pass every seed and each buggy one must fail some (CLAUDE.md).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ananke_env::moirae::{Export, bytes_decoder};
use ananke_env::sim::{Sim, SimConfig, SimEnv, TraceRecord};
use ananke_env::{Clock, Environment, NodeId, Rng, TraceEvent};
use ananke_storage::engine;
use ananke_storage::manifest;
use ananke_storage::{Engine, EngineConfig, EngineRecovery, Value, WriteBatch, wal};
use bytes::Bytes;

pub use ananke_storage::engine::Variant;

use crate::wal::{Excuse, Model as LogModel, Recovered, check_epoch, rotted, syncs};

/// Where the engine lives on the node's disk.
pub const DIR: &str = "/db";
/// The key space: `k00` to `k47`, small so writes collide and tombstones matter.
pub const KEYS: u64 = 48;

/// How the run is shaped.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    /// Crashes per run; each is followed by a recovery that is checked.
    pub crashes: u32,
    /// Tasks writing at once.
    pub writers: u32,
    /// Tasks reading at once.
    pub readers: u32,
    /// The longest value; lengths are uniform from zero.
    pub value_max: u64,
    /// The longest pause between one task's operations, in microseconds.
    pub gap_max_us: u64,
    /// The shortest and longest time between a start and the next crash.
    pub run_min: Duration,
    /// See `run_min`.
    pub run_max: Duration,
    /// The active memtable rotates past this many bytes.
    pub memtable_bytes: u64,
    /// The log's segment size.
    pub segment_bytes: u64,
    /// The least and most time a filesystem operation takes.
    pub io_latency: (Duration, Duration),
    /// Level 0 is compacted at this many tables.
    pub l0_trigger: usize,
    /// Level 1's size limit; each deeper level's is ten times the one before.
    pub level_base_bytes: u64,
    /// Compaction outputs are sealed at this size.
    pub sst_bytes: u64,
    /// Whether the engine may fall back to an older intact manifest when `CURRENT`
    /// or the manifest it names cannot be read; off, it refuses the store (D-022).
    pub allow_manifest_fallback: bool,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            crashes: 8,
            writers: 3,
            readers: 2,
            value_max: 48,
            gap_max_us: 500,
            run_min: Duration::from_millis(2),
            run_max: Duration::from_millis(40),
            memtable_bytes: 2048,
            segment_bytes: 1024,
            io_latency: (Duration::from_micros(20), Duration::from_micros(200)),
            l0_trigger: 4,
            // Small enough that level 1 overflows into level 2 with a few kilobytes
            // of live data, so rounds below level 0 happen in every run.
            level_base_bytes: 1024,
            sst_bytes: 2048,
            allow_manifest_fallback: true,
        }
    }
}

impl Schedule {
    /// The nightly's deep-levels shape: level limits so small that a few kilobytes of
    /// live data overflow level 1 into 2 and level 2 into 3 and below, so the rounds
    /// that only deep levels take are exercised. Everything else as the default.
    #[must_use]
    pub fn deep() -> Self {
        Self {
            level_base_bytes: 64,
            sst_bytes: 512,
            ..Self::default()
        }
    }
}

/// The key numbered `i`.
#[must_use]
pub fn key(i: u64) -> Bytes {
    Bytes::from(format!("k{i:02}"))
}

/// One log record's writes, in order: one for a put or a delete, several for a
/// batch (D-024).
pub type Record = Vec<(Bytes, Value)>;

/// The writes a record leaves in the memtable: the last one per key, in key order.
#[must_use]
pub fn effective(record: &[(Bytes, Value)]) -> Vec<(Bytes, Value)> {
    let mut last: BTreeMap<Bytes, Value> = BTreeMap::new();
    for (key, value) in record {
        last.insert(key.clone(), value.clone());
    }
    last.into_iter().collect()
}

/// A checkpoint the run took and what it must hold (D-024).
#[derive(Clone, Debug)]
pub struct Checkpoint {
    /// Its directory.
    pub dir: PathBuf,
    /// The version it was taken at.
    pub version: u64,
    /// The model's state at that version when it was taken.
    pub expected: BTreeMap<Bytes, Value>,
}

/// What the writers know, on top of the log model: the ops in log order, which keys
/// have a write in flight, the committed value per key, and which records are gone
/// for good with an explanation.
#[derive(Debug, Default)]
pub struct Model {
    /// The log model: encoded ops and their acknowledgements.
    pub log: LogModel,
    /// The records by position, parallel to `log.appended`.
    pub ops: Vec<Record>,
    /// Writes enqueued and not yet acknowledged, per key.
    pub in_flight: BTreeMap<Bytes, u32>,
    /// The newest acknowledged write per key, by sequence number.
    pub committed: BTreeMap<Bytes, (u64, Value)>,
    /// Records none of whose writes survives: lost with a dropped table or manifest
    /// and not brought back by a log replay, for good.
    pub lost: BTreeSet<u64>,
    /// Writes, as (key, record), that no table in service holds and the log did not
    /// replay: compacted away, or lost. The state folds over the rest (D-024).
    pub lost_writes: BTreeSet<(Bytes, u64)>,
    /// Live reads performed, scans included.
    pub reads: u64,
    /// Of those, scans at a snapshot.
    pub scans: u64,
    /// Live reads that disagreed with the model.
    pub read_violations: Vec<String>,
    /// Checkpoints taken and not yet verified.
    pub checkpoints: Vec<Checkpoint>,
    /// Checkpoints taken over the run.
    pub checkpoints_taken: u64,
    /// Batches of more than one write over the run.
    pub batches: u64,
    /// Writes that asked for no sync over the run.
    pub unsynced: u64,
}

impl Model {
    /// The state after the first `n` records, minus the ones lost for good: the
    /// newest surviving write per key.
    #[must_use]
    pub fn state_after(&self, n: usize) -> BTreeMap<Bytes, Value> {
        let mut state = BTreeMap::new();
        for (i, record) in self.ops[..n].iter().enumerate() {
            let seq = i as u64 + 1;
            for (key, value) in effective(record) {
                if self.lost_writes.contains(&(key.clone(), seq)) {
                    continue;
                }
                state.insert(key, value);
            }
        }
        state
    }
}

type SharedModel = Arc<Mutex<Model>>;

/// What the trace says one table holds.
#[derive(Clone, Debug, Default)]
pub struct TableMirror {
    /// Its level.
    pub level: u8,
    /// Its key range.
    pub first_key: Bytes,
    /// See `first_key`.
    pub last_key: Bytes,
    /// The writes it holds, as (key, record number).
    pub writes: BTreeSet<(Bytes, u64)>,
}

/// The trace's account of what every table holds and every manifest lists: flushes
/// and compactions mirrored from the events (D-023). A flushed table holds every
/// write in its sequence range; a compaction's outputs hold what the merge of its
/// inputs kept, by the engine's rules, split by the key ranges the trace gives.
/// Rebuilt from the whole trace each epoch, against the model's ops as they are
/// numbered now: every table a manifest in force lists holds records below the
/// model's base, which are never renumbered.
#[derive(Debug, Default)]
pub struct Mirror {
    /// Every table ever written, by number.
    pub tables: BTreeMap<u64, TableMirror>,
    /// The tables each manifest lists.
    pub manifests: BTreeMap<u64, Vec<u64>>,
    /// Each compaction's manifest and the writes it dropped, in order.
    pub compactions: Vec<(u64, BTreeSet<(Bytes, u64)>)>,
    /// Tables the engine deleted once no manifest in force listed them: inputs of a
    /// compaction that had finished, or orphans an open removed. A deletion before
    /// the manifest stopped listing the table is the bug, and is not here.
    pub deleted: BTreeSet<u64>,
    /// Inputs of every compaction whose manifest the trace saw written: from then on
    /// their deletion is legitimate.
    finished_inputs: BTreeSet<u64>,
    /// Inputs of compactions written out whose manifest is not yet, by manifest.
    pending_inputs: BTreeMap<u64, Vec<u64>>,
    /// Tables the latest open dropped, out of service until the next one.
    dropped_at_open: BTreeSet<u64>,
}

impl Mirror {
    /// Mirrors `events` against `ops`.
    #[must_use]
    pub fn build(events: &[&TraceEvent], ops: &[Record]) -> Self {
        let mut mirror = Self::default();
        for event in events {
            match event {
                TraceEvent::NodeRestarted { .. } => mirror.dropped_at_open.clear(),
                TraceEvent::SstDropped { number, .. } => {
                    mirror.dropped_at_open.insert(*number);
                }
                TraceEvent::SstWritten {
                    number,
                    level: 0,
                    first_seq,
                    max_seq,
                    ..
                } => {
                    // A flushed table holds what every record in its range left in
                    // the memtable: its last write per key.
                    let writes: BTreeSet<(Bytes, u64)> = (*first_seq..=*max_seq)
                        .filter(|&seq| seq as usize <= ops.len())
                        .flat_map(|seq| {
                            effective(&ops[seq as usize - 1])
                                .into_iter()
                                .map(move |(key, _)| (key, seq))
                        })
                        .collect();
                    let keys = || writes.iter().map(|(k, _)| k);
                    mirror.tables.insert(
                        *number,
                        TableMirror {
                            level: 0,
                            first_key: keys().min().cloned().unwrap_or_default(),
                            last_key: keys().max().cloned().unwrap_or_default(),
                            writes,
                        },
                    );
                }
                TraceEvent::SstWritten { number, level, .. } => {
                    // A compaction's output: filled in when the compaction finishes.
                    mirror.tables.insert(
                        *number,
                        TableMirror {
                            level: *level,
                            ..TableMirror::default()
                        },
                    );
                }
                TraceEvent::ManifestWritten { number, tables, .. } => {
                    // A number written again supersedes what was written under it
                    // and after it: a lineage a fallback abandoned. The compaction
                    // this manifest is for was recorded just before it and stays.
                    let current = mirror
                        .compactions
                        .last()
                        .filter(|(n, _)| n == number)
                        .cloned();
                    mirror.manifests.retain(|&n, _| n < *number);
                    mirror.manifests.insert(*number, tables.clone());
                    mirror.compactions.retain(|(n, _)| *n < *number);
                    mirror.compactions.extend(current);
                    if let Some(inputs) = mirror.pending_inputs.remove(number) {
                        mirror.finished_inputs.extend(inputs);
                    }
                }
                TraceEvent::CompactionWritten {
                    level,
                    manifest,
                    inputs,
                    outputs,
                    snapshot,
                    ..
                } => mirror.compaction(ops, *level, *manifest, inputs, outputs, *snapshot),
                TraceEvent::SstDeleted { number } => {
                    if mirror.finished_inputs.contains(number) {
                        mirror.deleted.insert(*number);
                    }
                }
                TraceEvent::OrphanRemoved { path } => {
                    if let Some(number) = manifest::sst_of(path) {
                        mirror.deleted.insert(number);
                    }
                }
                _ => {}
            }
        }
        mirror
    }

    /// Merges the inputs by the engine's rules and fills in the outputs.
    fn compaction(
        &mut self,
        ops: &[Record],
        level: u8,
        manifest: u64,
        inputs: &[u64],
        outputs: &[(u64, Bytes, Bytes)],
        snapshot: u64,
    ) {
        let output_level = level + 1;
        // The tables in service when the round was picked: the previous manifest's,
        // less those the open dropped. Those deeper than the output level decide
        // whether a tombstone may go. When the trace never saw the previous manifest
        // written (whole on disk without its sync), every table written and not yet
        // deleted stands in.
        let live = self
            .manifests
            .get(&manifest.saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| {
                self.tables
                    .keys()
                    .filter(|t| !self.deleted.contains(t))
                    .copied()
                    .collect()
            });
        let deeper: Vec<(Bytes, Bytes)> = live
            .iter()
            .filter(|t| !self.dropped_at_open.contains(t))
            .filter_map(|t| self.tables.get(t))
            .filter(|t| t.level > output_level)
            .map(|t| (t.first_key.clone(), t.last_key.clone()))
            .collect();
        // Every write the inputs hold, by user key, as (number, is a tombstone).
        let mut by_key: BTreeMap<Bytes, Vec<(u64, bool)>> = BTreeMap::new();
        for table in inputs {
            let Some(table) = self.tables.get(table) else {
                continue;
            };
            for (key, seq) in &table.writes {
                if *seq as usize > ops.len() {
                    continue;
                }
                let tombstone = ops[*seq as usize - 1]
                    .iter()
                    .rev()
                    .find(|(k, _)| k == key)
                    .is_some_and(|(_, v)| *v == Value::Tombstone);
                by_key
                    .entry(key.clone())
                    .or_default()
                    .push((*seq, tombstone));
            }
        }
        let mut dropped = BTreeSet::new();
        let mut kept: Vec<(Bytes, u64)> = Vec::new();
        for (key, mut writes) in by_key {
            writes.sort_unstable_by_key(|(seq, _)| std::cmp::Reverse(*seq));
            let mut prev: Option<u64> = None;
            for (seq, tombstone) in writes {
                let drop = match prev {
                    Some(previous) => previous <= snapshot,
                    None => {
                        tombstone
                            && seq <= snapshot
                            && !deeper.iter().any(|(f, l)| *f <= key && key <= *l)
                    }
                };
                if drop {
                    dropped.insert((key.clone(), seq));
                } else {
                    kept.push((key.clone(), seq));
                }
                prev = Some(seq);
            }
        }
        for (number, first, last) in outputs {
            let writes = kept
                .iter()
                .filter(|(k, _)| first <= k && k <= last)
                .cloned()
                .collect();
            self.tables.insert(
                *number,
                TableMirror {
                    level: output_level,
                    first_key: first.clone(),
                    last_key: last.clone(),
                    writes,
                },
            );
        }
        self.compactions.push((manifest, dropped));
        self.pending_inputs.insert(manifest, inputs.to_vec());
    }
}

fn lock(model: &SharedModel) -> MutexGuard<'_, Model> {
    model.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One crash and the recovery after it, checked.
#[derive(Debug)]
pub struct Epoch {
    /// What recovery found.
    pub recovery: EngineRecovery,
    /// Ops in the model when the node crashed.
    pub appended: usize,
    /// Of those, acknowledged.
    pub acked: usize,
    /// Records the state covered when this epoch started.
    pub base: usize,
    /// Memtables rotated but not flushed when the node crashed.
    pub mid_flush: usize,
    /// What let the engine off, if anything did.
    pub excuse: Option<Excuse>,
    /// The first violation, if any.
    pub verdict: Result<(), String>,
    /// The model's records when the node crashed, for diagnosis.
    pub ops: Vec<Record>,
    /// Records counted as lost after this recovery, for diagnosis.
    pub lost: Vec<u64>,
}

/// An open the engine refused, which ends the run: the store cannot be trusted to
/// be a state that existed (D-022). Excused only by a fault on `CURRENT` or the
/// manifest it named.
#[derive(Debug)]
pub struct Refusal {
    /// The epoch whose open was refused, counting crashes from 1.
    pub after_crash: u32,
    /// What the engine said.
    pub reason: String,
    /// Whether a fault explains it.
    pub verdict: Result<(), String>,
}

/// What one run produced.
#[derive(Debug)]
pub struct Report {
    /// The seed.
    pub seed: u64,
    /// Which engine ran.
    pub variant: Variant,
    /// Each crash and its recovery, in order.
    pub epochs: Vec<Epoch>,
    /// The open that ended the run early, if one did.
    pub refused: Option<Refusal>,
    /// Live reads over the run, scans included.
    pub reads: u64,
    /// Of those, scans at a snapshot.
    pub scans: u64,
    /// Batches of more than one write.
    pub batches: u64,
    /// Writes that asked for no sync.
    pub unsynced: u64,
    /// Checkpoints taken.
    pub checkpoints_taken: u64,
    /// Checkpoints opened fresh and checked after a crash.
    pub checkpoints_verified: u64,
    /// Checkpoints a fault touched, not checked.
    pub checkpoints_damaged: u64,
    /// The whole trace.
    pub records: Vec<TraceRecord>,
    /// The trace as moirae JSONL.
    pub jsonl: String,
}

impl Report {
    /// The first violation across the epochs, naming the seed and the variant.
    ///
    /// # Errors
    ///
    /// A message naming the seed, the variant, the epoch and the violated property.
    pub fn check(&self) -> Result<(), String> {
        for (i, epoch) in self.epochs.iter().enumerate() {
            if let Err(violation) = &epoch.verdict {
                return Err(format!(
                    "seed {} {:?} epoch {i}: {violation}",
                    self.seed, self.variant
                ));
            }
        }
        if let Some(refusal) = &self.refused
            && let Err(violation) = &refusal.verdict
        {
            return Err(format!(
                "seed {} {:?} after crash {}: {violation}",
                self.seed, self.variant, refusal.after_crash
            ));
        }
        Ok(())
    }

    /// Whether the trace has an event matching `f`.
    #[must_use]
    pub fn has(&self, f: impl Fn(&TraceEvent) -> bool) -> bool {
        self.records.iter().any(|r| f(&r.event))
    }

    /// How many trace events match `f`.
    #[must_use]
    pub fn count(&self, f: impl Fn(&TraceEvent) -> bool) -> usize {
        self.records.iter().filter(|r| f(&r.event)).count()
    }
}

/// The simulator configuration: every §1.3 fault on, including latency.
#[must_use]
pub fn config(seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.fs.p_durable = 0.8;
    config.fs.p_bitrot = 0.05;
    config.fs.latency_min = schedule.io_latency.0;
    config.fs.latency_max = schedule.io_latency.1;
    config.poll_budget = 10_000;
    let total = schedule.run_max * (schedule.crashes + 1);
    config.run_length_hint = SimConfig::run_length_hint_for(1, total);
    config
}

/// Runs the scenario for `seed` with the default [`Schedule`].
#[must_use]
pub fn run(seed: u64, variant: Variant) -> Report {
    run_with(seed, Schedule::default(), variant)
}

/// Runs the scenario for `seed` with an explicit [`Schedule`].
#[must_use]
pub fn run_with(seed: u64, schedule: Schedule, variant: Variant) -> Report {
    let mut sim = Sim::new(config(seed, &schedule));
    let node = sim.add_node();
    let mut harness = moirae_sched::stream(seed, "harness");
    let model = SharedModel::default();
    let mut epochs: Vec<Epoch> = Vec::new();
    let mut base = 0;
    let mut epoch_start = 0;
    let mut previous_manifest: Option<(u64, u64)> = None;
    let mut refused = None;
    let (mut checkpoints_verified, mut checkpoints_damaged) = (0u64, 0u64);
    for crash in 0..=schedule.crashes {
        let before_open = sim.trace().len();
        let dir = Path::new(DIR);
        let (db, recovery) = match open(&mut sim, node, &schedule, variant) {
            Ok(opened) => opened,
            Err(error) => {
                // A refused store ends the run. The refusal is excused only if a
                // fault explains why CURRENT or the manifest it named could not be
                // read: its content's sync lost, for the switch or one in flight at
                // the crash, or bit rot.
                let records = sim.trace();
                let events: Vec<&TraceEvent> = records[epoch_start..before_open]
                    .iter()
                    .map(|r| &r.event)
                    .collect();
                let all: Vec<&TraceEvent> =
                    records[..before_open].iter().map(|r| &r.event).collect();
                let synced = syncs(&events, dir);
                let refusal = engine::OpenRefused::from_io(&error);
                let named = match refusal {
                    Some(engine::OpenRefused::ManifestUnreadable(n)) => Some(n),
                    Some(engine::OpenRefused::NoIntactManifest { named }) if named > 0 => {
                        Some(named)
                    }
                    _ => None,
                };
                let explained = match named {
                    Some(n) => {
                        synced.manifest_betrayed.contains(&n)
                            || rotted(&all, &manifest::manifest_path(dir, n))
                    }
                    None => {
                        let from = synced
                            .switched
                            .last()
                            .copied()
                            .or_else(|| previous_manifest.map(|(n, _)| n));
                        from.is_some_and(|from| synced.current_betrayed.contains(&from))
                            || synced.current_tmp_lost_in_flight
                            || rotted(&all, &manifest::current_path(dir))
                    }
                };
                refused = Some(Refusal {
                    after_crash: crash,
                    reason: error.to_string(),
                    verdict: if refusal.is_none() {
                        Err(format!(
                            "open failed with something other than a refusal: {error}"
                        ))
                    } else if explained {
                        Ok(())
                    } else {
                        Err(format!("open was refused without a fault: {error}"))
                    },
                });
                break;
            }
        };
        let records = sim.trace();
        let events: Vec<&TraceEvent> = records[epoch_start..before_open]
            .iter()
            .map(|r| &r.event)
            .collect();
        let all: Vec<&TraceEvent> = records[..before_open].iter().map(|r| &r.event).collect();
        let segment_first: BTreeMap<u64, u64> = records
            .iter()
            .filter_map(|r| match r.event {
                TraceEvent::WalSegmentOpened { segment, first } => Some((segment, first)),
                _ => None,
            })
            .collect();
        // What the trace says every table holds and every manifest lists: the mirror
        // of flushes and compactions, rebuilt from the whole trace each epoch. A
        // record is present if a table in service holds it or the log replayed it;
        // every other record the tables owed needs an explanation, else it is a
        // violation.
        let ops_now: Vec<Record> = lock(&model).ops.clone();
        let mirror = Mirror::build(&all, &ops_now);
        // What the manifest in force lists comes from the recovery, not the trace: a
        // manifest can be whole on disk without the sync that would have reported it
        // written, as CURRENT can name it without the sync that reports the switch.
        let dropped_numbers: BTreeSet<u64> = recovery.dropped.iter().map(|m| m.number).collect();
        let in_service: Vec<u64> = recovery
            .tables
            .iter()
            .map(|m| m.number)
            .filter(|n| !dropped_numbers.contains(n))
            .collect();
        let last_recovered = recovery.first_seq_end();
        let replayed = (recovery.flushed_seq + 1).max(recovery.wal.first_seq)..=last_recovered;
        // A write is present if a table in service holds it or the log replayed its
        // record; a record is present if any of its writes is.
        let present_write = |key: &Bytes, seq: u64| {
            (!recovery.wal.records.is_empty() && replayed.contains(&seq))
                || in_service.iter().any(|t| {
                    mirror
                        .tables
                        .get(t)
                        .is_some_and(|t| t.writes.contains(&(key.clone(), seq)))
                })
        };
        let writes_of = |seq: u64| -> Vec<Bytes> {
            ops_now
                .get(seq as usize - 1)
                .map(|record| effective(record).into_iter().map(|(k, _)| k).collect())
                .unwrap_or_default()
        };
        let present = |seq: u64| writes_of(seq).iter().any(|k| present_write(k, seq));
        let compacted_writes: BTreeSet<(Bytes, u64)> = mirror
            .compactions
            .iter()
            .filter(|(manifest, _)| *manifest <= recovery.manifest)
            .flat_map(|(_, dropped)| dropped.iter().cloned())
            .collect();
        // A record is compacted away when every write of it not present was dropped.
        let compacted = |seq: u64| {
            writes_of(seq)
                .iter()
                .all(|k| present_write(k, seq) || compacted_writes.contains(&(k.clone(), seq)))
        };
        let mut verdict = Ok(());
        let synced = syncs(&events, dir);
        // Damage to a table lasts: a table torn at one crash is dropped at every open
        // after, so its lost sync is looked for in the whole trace.
        let all_synced = syncs(&all, dir);
        // A fallback used an older manifest than CURRENT should have named. Every
        // step of it must be explained by a fault, or by a write that was in flight
        // at the crash. The manifest CURRENT named could not be used: CURRENT itself
        // unreadable (its content's sync lost, for the switch or one in flight, or
        // bit rot), or the manifest unreadable (its sync lost, bit rot, or its write
        // never synced before the crash, which the trace shows as no record of it
        // written). And each manifest passed over on the way to the one used was
        // rejected for a fault on it or on a table it lists, or because it lists no
        // table. A fallback that used the last manifest switched to lost nothing,
        // since CURRENT named a switch still in flight; only one that went older
        // lost what was flushed since.
        let mut fallback_why: Option<Excuse> = None;
        if let Some(named) = recovery.fallback_from {
            let from = synced
                .switched
                .last()
                .copied()
                .or_else(|| previous_manifest.map(|(n, _)| n))
                .unwrap_or(0);
            let written = |m: u64| {
                all.iter().any(
                    |e| matches!(e, TraceEvent::ManifestWritten { number, .. } if *number == m),
                )
            };
            let manifest_fault = |m: u64| -> Option<Excuse> {
                if !written(m) || all_synced.manifest_betrayed.contains(&m) {
                    Some(Excuse::LostFsync)
                } else if rotted(&all, &manifest::manifest_path(dir, m)) {
                    Some(Excuse::BitRot)
                } else {
                    None
                }
            };
            let table_fault = |t: u64| -> Option<Excuse> {
                if all_synced.sst_betrayed.contains(&t) || mirror.deleted.contains(&t) {
                    Some(Excuse::LostFsync)
                } else if rotted(&all, &manifest::sst_path(dir, t)) {
                    Some(Excuse::BitRot)
                } else {
                    None
                }
            };
            let named_why = if named == 0 {
                let current_betrayed =
                    synced.current_betrayed.contains(&from) || synced.current_tmp_lost_in_flight;
                if current_betrayed {
                    Some(Excuse::LostFsync)
                } else if rotted(&all, &manifest::current_path(dir)) {
                    Some(Excuse::BitRot)
                } else {
                    None
                }
            } else {
                manifest_fault(named)
            };
            let mut unexplained: Option<String> = None;
            if named_why.is_none() {
                unexplained = Some(if named == 0 {
                    "CURRENT could not be read and no fault touched it".to_owned()
                } else {
                    format!("manifest {named} could not be read and no fault touched it")
                });
            }
            for (m, why) in &recovery.rejected {
                let explained = match why {
                    engine::Rejected::NoTables => Some(Excuse::LostFsync),
                    engine::Rejected::Unreadable => manifest_fault(*m),
                    engine::Rejected::TableMissing(t) | engine::Rejected::TableDamaged(t) => {
                        table_fault(*t)
                    }
                };
                if explained.is_none() && unexplained.is_none() {
                    unexplained = Some(format!(
                        "manifest {m} was passed over as {why:?} and no fault explains it"
                    ));
                }
            }
            if let Some(what) = unexplained
                && recovery.manifest < from
            {
                verdict = Err(format!(
                    "CURRENT named manifest {named} and recovery used {}, but the last switch was to {from}: {what}",
                    recovery.manifest
                ));
            }
            fallback_why = named_why.or(Some(Excuse::LostFsync));
        }
        // A dropped table: its sync was lost or bit rot hit it; or it is missing
        // because the engine deleted it once no manifest in force listed it, as a
        // compaction's input or as an orphan, and a fallback, this epoch's or an
        // earlier one's, went back to a manifest that did. A table deleted before
        // its manifest stopped listing it is the bug.
        let mut table_why: BTreeMap<u64, Excuse> = BTreeMap::new();
        for meta in &recovery.dropped {
            let path = manifest::sst_path(dir, meta.number);
            let deleted = mirror.deleted.contains(&meta.number);
            let why = if all_synced.sst_betrayed.contains(&meta.number) {
                Some(Excuse::LostFsync)
            } else if rotted(&all, &path) {
                Some(Excuse::BitRot)
            } else if deleted {
                Some(fallback_why.unwrap_or(Excuse::LostFsync))
            } else {
                None
            };
            match why {
                Some(why) => {
                    table_why.insert(meta.number, why);
                }
                None if verdict.is_ok() => {
                    let reason = records
                        .iter()
                        .rev()
                        .find_map(|r| match &r.event {
                            TraceEvent::SstDropped { number, reason, .. }
                                if *number == meta.number =>
                            {
                                Some(*reason)
                            }
                            _ => None,
                        })
                        .unwrap_or("?");
                    verdict = Err(format!(
                        "table {} at level {} covering {}..={} was dropped ({reason}) without a fault",
                        meta.number, meta.level, meta.first_seq, meta.max_seq
                    ));
                }
                None => {}
            }
        }
        // Every record the tables owed that is not there, with why: dropped by a
        // compaction in the manifest's lineage, in a dropped table, in a table a
        // fallback left behind, or lost before and not brought back. What was lost
        // before stays lost unless the log brought it back, wherever it lies.
        let previously_lost: BTreeSet<u64> = lock(&model).lost.clone();
        let mut excused: BTreeMap<u64, Excuse> = previously_lost
            .iter()
            .filter(|&&seq| !present(seq))
            .map(|&seq| (seq, Excuse::LostFsync))
            .collect();
        // Past the manifest's flushed point, a fallback left tables behind whose
        // records are owed by nothing but explained by it: the log's head among them.
        let owed_through = if fallback_why.is_some() {
            mirror
                .tables
                .values()
                .filter_map(|t| t.writes.iter().map(|(_, s)| *s).max())
                .max()
                .unwrap_or(0)
                .max(recovery.flushed_seq)
        } else {
            recovery.flushed_seq
        };
        for seq in 1..=owed_through {
            if present(seq) {
                continue;
            }
            let held_by = |t: &u64| {
                mirror
                    .tables
                    .get(t)
                    .is_some_and(|t| t.writes.iter().any(|(_, s)| *s == seq))
            };
            if seq > recovery.flushed_seq {
                if mirror.tables.keys().any(held_by)
                    && let Some(why) = fallback_why
                {
                    excused.insert(seq, why);
                }
                continue;
            }
            let why = if compacted(seq) {
                Some(Excuse::Compacted)
            } else if let Some(why) = table_why
                .iter()
                .find(|(t, _)| held_by(t))
                .map(|(_, why)| *why)
            {
                Some(why)
            } else if fallback_why.is_some() && mirror.tables.keys().any(held_by) {
                fallback_why
            } else if previously_lost.contains(&seq) {
                Some(Excuse::LostFsync)
            } else {
                None
            };
            match why {
                Some(why) => {
                    excused.insert(seq, why);
                }
                None if verdict.is_ok() => {
                    verdict = Err(format!(
                        "record {seq} is in no table manifest {} lists, no compaction dropped it, and no fault explains it",
                        recovery.manifest
                    ));
                }
                None => {}
            }
        }
        // After a missing head the log is discarded: nothing replays, and the state
        // must be the manifest's prefix and nothing else (D-022).
        if verdict.is_ok()
            && let Some((expected, found)) = recovery.wal.head_gap
            && (recovery.replayed > 0 || !recovery.wal.records.is_empty())
        {
            verdict = Err(format!(
                "the log's head was missing (expected record {expected}, found {found}) but {} records were replayed past it",
                recovery.replayed
            ));
        }
        let end = usize::try_from(recovery.flushed_seq.max(last_recovered)).expect("fits");
        {
            let mut m = lock(&model);
            m.lost = (1..=end as u64).filter(|&seq| !present(seq)).collect();
            m.lost_writes = (1..=end as u64)
                .flat_map(|seq| {
                    writes_of(seq)
                        .into_iter()
                        .filter(move |k| !present_write(k, seq))
                        .map(move |k| (k, seq))
                })
                .collect();
        }

        if crash > 0 {
            let m = lock(&model);
            if verdict.is_ok() {
                let recovered = Recovered {
                    first_seq: recovery.wal.first_seq,
                    records: &recovery.wal.records,
                    stop: recovery.wal.stop,
                    head_gap: recovery.wal.head_gap,
                    covered_stops: &recovery.wal.covered_stops,
                    segment_first: &segment_first,
                    covered_through: recovery.flushed_seq,
                    excused: excused.clone(),
                };
                let (v, e) = check_epoch(&m.log, base, &recovered, &events, &all, dir);
                verdict = v;
                if verdict.is_ok()
                    && let Some(violation) = m.read_violations.first()
                {
                    verdict = Err(violation.clone());
                }
                let mid_flush = unflushed(&events);
                epochs.push(Epoch {
                    recovery: recovery.clone(),
                    appended: m.ops.len(),
                    acked: m.log.acked.iter().filter(|&&a| a).count(),
                    base,
                    mid_flush,
                    excuse: e,
                    verdict: Ok(()),
                    ops: m.ops.clone(),
                    lost: m.lost.iter().copied().collect(),
                });
            } else {
                let mid_flush = unflushed(&events);
                epochs.push(Epoch {
                    recovery: recovery.clone(),
                    appended: m.ops.len(),
                    acked: m.log.acked.iter().filter(|&&a| a).count(),
                    base,
                    mid_flush,
                    excuse: None,
                    verdict: Ok(()),
                    ops: m.ops.clone(),
                    lost: m.lost.iter().copied().collect(),
                });
            }
        }
        // The state after recovery, key by key, against the model folded over what
        // survived. The engine reads tables from disk, so this takes virtual time.
        if let Some(epoch) = epochs.last_mut()
            && verdict.is_ok()
        {
            let expected = {
                let m = lock(&model);
                let n = end.min(m.ops.len());
                m.state_after(n)
            };
            if let Err(violation) = check_state(&mut sim, node, &db, expected, end) {
                verdict = Err(match recovery.wal.head_gap {
                    Some((expected, found)) => format!(
                        "after a missing head (expected record {expected}, found {found}) the state is not the manifest's prefix: {violation}"
                    ),
                    None => violation,
                });
            }
            epoch.verdict = verdict.clone();
        } else if let Some(epoch) = epochs.last_mut() {
            epoch.verdict = verdict.clone();
        }
        // Every checkpoint taken before the crash opens fresh at its version, unless
        // a fault touched its files, which the crash's bit rot or a lost sync can.
        let pending: Vec<Checkpoint> = std::mem::take(&mut lock(&model).checkpoints);
        for checkpoint in pending {
            let touched = all.iter().any(|e| match e {
                TraceEvent::BlockRotted { path, .. }
                | TraceEvent::FsyncLost { path }
                | TraceEvent::WriteTorn { path, .. } => path.starts_with(&checkpoint.dir),
                _ => false,
            });
            if touched {
                checkpoints_damaged += 1;
                continue;
            }
            if let Some(epoch) = epochs.last_mut()
                && epoch.verdict.is_ok()
                && let Err(violation) = check_checkpoint(&mut sim, node, &schedule, &checkpoint)
            {
                epoch.verdict = Err(violation);
            }
            checkpoints_verified += 1;
        }
        previous_manifest = Some((recovery.manifest, recovery.flushed_seq));
        base = end;
        {
            let mut m = lock(&model);
            m.log.appended.truncate(base);
            m.log.acked.truncate(base);
            m.log.sync_requested.truncate(base);
            m.log.acked.iter_mut().for_each(|a| *a = true);
            m.ops.truncate(base);
            m.in_flight.clear();
            m.committed = m
                .state_after(base)
                .into_iter()
                .map(|(k, v)| (k, (0, v)))
                .collect();
            m.read_violations.clear();
        }
        epoch_start = before_open;
        if crash == schedule.crashes {
            break;
        }
        spawn_clients(&sim, node, db, &schedule, &model);
        let span = schedule.run_max.saturating_sub(schedule.run_min);
        let extra = Duration::from_nanos(harness.below(span.as_nanos() as u64 + 1));
        sim.run_for(schedule.run_min + extra);
        // Then a few more scheduling steps, so the crash lands between two polls at
        // one instant and not only where every queue has drained.
        sim.run_steps(harness.below(64));
        sim.crash(node);
        sim.restart(node);
    }
    let (reads, scans, batches, unsynced, checkpoints_taken) = {
        let m = lock(&model);
        (m.reads, m.scans, m.batches, m.unsynced, m.checkpoints_taken)
    };
    Report {
        seed,
        variant,
        epochs,
        refused,
        reads,
        scans,
        batches,
        unsynced,
        checkpoints_taken,
        checkpoints_verified,
        checkpoints_damaged,
        jsonl: sim
            .to_moirae(&Export::new(&bytes_decoder))
            .expect("the engine trace exports to moirae v2"),
        records: sim.trace(),
    }
}

/// The last sequence number the log recovered, or 0.
trait RecoveryEnd {
    fn first_seq_end(&self) -> u64;
}

impl RecoveryEnd for EngineRecovery {
    fn first_seq_end(&self) -> u64 {
        if self.wal.records.is_empty() {
            0
        } else {
            self.wal.first_seq + self.wal.records.len() as u64 - 1
        }
    }
}

/// Memtables rotated in the epoch and not flushed before its crash.
fn unflushed(events: &[&TraceEvent]) -> usize {
    let rotated = events
        .iter()
        .filter(|e| matches!(e, TraceEvent::MemtableRotated { .. }))
        .count();
    let flushed = events
        .iter()
        .filter(|e| matches!(e, TraceEvent::MemtableFlushed { .. }))
        .count();
    rotated.saturating_sub(flushed)
}

type Db = Arc<Engine<SimEnv>>;

/// Where the open task leaves the engine and what it recovered, or the error.
type Opened = Arc<Mutex<Option<std::io::Result<(Db, EngineRecovery)>>>>;

/// Opens the engine on `node` and runs the open to completion.
fn open(
    sim: &mut Sim,
    node: NodeId,
    schedule: &Schedule,
    variant: Variant,
) -> std::io::Result<(Db, EngineRecovery)> {
    let env = sim.env(node);
    let opened: Opened = Arc::default();
    let o = opened.clone();
    let config = EngineConfig {
        dir: PathBuf::from(DIR),
        memtable_bytes: schedule.memtable_bytes,
        segment_bytes: schedule.segment_bytes,
        variant,
        wal_variant: wal::Variant::Correct,
        // A missing head is judged by the oracle, so the run goes on past it.
        allow_head_gap: true,
        allow_manifest_fallback: schedule.allow_manifest_fallback,
        l0_trigger: schedule.l0_trigger,
        level_base_bytes: schedule.level_base_bytes,
        sst_bytes: schedule.sst_bytes,
        background_compaction: true,
    };
    env.clone().spawn("engine-open", async move {
        let opened = Engine::open(env, config)
            .await
            .map(|(db, recovery)| (Arc::new(db), recovery));
        *o.lock().unwrap_or_else(PoisonError::into_inner) = Some(opened);
    });
    // Recovery reads files, which take time; nothing else is running yet.
    while opened
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_none()
    {
        sim.run_for(Duration::from_millis(1));
    }
    opened
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the open completed")
}

/// Reads every key of the space and compares with `expected`.
fn check_state(
    sim: &mut Sim,
    node: NodeId,
    db: &Db,
    expected: BTreeMap<Bytes, Value>,
    end: usize,
) -> Result<(), String> {
    let env = sim.env(node);
    let out: Arc<Mutex<Option<Vec<String>>>> = Arc::default();
    let o = out.clone();
    let db = db.clone();
    env.spawn("state-check", async move {
        let mut violations = Vec::new();
        for i in 0..KEYS {
            let key = key(i);
            let got = db.get(&key).await.expect("tables read");
            let want = expected.get(&key).cloned().and_then(Value::live);
            if got != want {
                violations.push(format!(
                    "after recovering through record {end}, key {} holds {got:?} but the model has {want:?}",
                    String::from_utf8_lossy(&key)
                ));
            }
        }
        // And the same through a scan of the whole space.
        let snapshot = db.snapshot();
        let scanned = db
            .scan(&key(0)[..]..&key(KEYS)[..], &snapshot)
            .await
            .expect("tables read");
        let want: Vec<(Bytes, Bytes)> = expected
            .iter()
            .filter_map(|(k, v)| v.clone().live().map(|v| (k.clone(), v)))
            .collect();
        if scanned != want {
            violations.push(format!(
                "after recovering through record {end}, a scan at version {} saw {} keys but the model has {}",
                snapshot.version(),
                scanned.len(),
                want.len()
            ));
        }
        *o.lock().unwrap_or_else(PoisonError::into_inner) = Some(violations);
    });
    while out.lock().unwrap_or_else(PoisonError::into_inner).is_none() {
        sim.run_for(Duration::from_millis(1));
    }
    let violations = out
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the check completed");
    violations.into_iter().next().map_or(Ok(()), Err)
}

/// Opens `checkpoint`'s directory as a fresh store and compares every key and a
/// scan with what the model says it holds.
fn check_checkpoint(
    sim: &mut Sim,
    node: NodeId,
    schedule: &Schedule,
    checkpoint: &Checkpoint,
) -> Result<(), String> {
    let env = sim.env(node);
    let out: Arc<Mutex<Option<Result<(), String>>>> = Arc::default();
    let o = out.clone();
    let checkpoint = checkpoint.clone();
    let config = EngineConfig {
        dir: checkpoint.dir.clone(),
        memtable_bytes: schedule.memtable_bytes,
        segment_bytes: schedule.segment_bytes,
        variant: Variant::Correct,
        wal_variant: wal::Variant::Correct,
        allow_head_gap: false,
        allow_manifest_fallback: false,
        l0_trigger: schedule.l0_trigger,
        level_base_bytes: schedule.level_base_bytes,
        sst_bytes: schedule.sst_bytes,
        background_compaction: false,
    };
    env.clone().spawn("checkpoint-check", async move {
        let result = async {
            let (db, recovery) = Engine::open(env, config).await.map_err(|e| {
                format!(
                    "checkpoint {} at version {} does not open: {e}",
                    checkpoint.dir.display(),
                    checkpoint.version
                )
            })?;
            if !recovery.dropped.is_empty() || recovery.replayed != 0 {
                return Err(format!(
                    "checkpoint {} at version {} opened with {} tables dropped and {} records replayed",
                    checkpoint.dir.display(),
                    checkpoint.version,
                    recovery.dropped.len(),
                    recovery.replayed
                ));
            }
            for i in 0..KEYS {
                let key = key(i);
                let got = db.get(&key).await.expect("tables read");
                let want = checkpoint.expected.get(&key).cloned().and_then(Value::live);
                if got != want {
                    return Err(format!(
                        "checkpoint {} at version {}: key {} holds {got:?} but the model has {want:?}",
                        checkpoint.dir.display(),
                        checkpoint.version,
                        String::from_utf8_lossy(&key)
                    ));
                }
            }
            let snapshot = db.snapshot();
            let scanned = db
                .scan(&key(0)[..]..&key(KEYS)[..], &snapshot)
                .await
                .expect("tables read");
            let want: Vec<(Bytes, Bytes)> = checkpoint
                .expected
                .iter()
                .filter_map(|(k, v)| v.clone().live().map(|v| (k.clone(), v)))
                .collect();
            if scanned != want {
                return Err(format!(
                    "checkpoint {} at version {}: a scan saw {} keys but the model has {}",
                    checkpoint.dir.display(),
                    checkpoint.version,
                    scanned.len(),
                    want.len()
                ));
            }
            Ok(())
        }
        .await;
        *o.lock().unwrap_or_else(PoisonError::into_inner) = Some(result);
    });
    while out.lock().unwrap_or_else(PoisonError::into_inner).is_none() {
        sim.run_for(Duration::from_millis(1));
    }
    out.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the check completed")
}

/// Starts the writers and readers; they run until the crash.
fn spawn_clients(sim: &Sim, node: NodeId, db: Db, schedule: &Schedule, model: &SharedModel) {
    let (value_max, gap_max_us) = (schedule.value_max, schedule.gap_max_us);
    for _ in 0..schedule.writers {
        let (env, db, model) = (sim.env(node), db.clone(), model.clone());
        env.clone().spawn("writer", async move {
            loop {
                // One write in four is a batch of up to four, keys repeating; one in
                // four asks for no sync (D-024).
                let count = if env.rng().below(4) == 0 {
                    1 + env.rng().below(4)
                } else {
                    1
                };
                let sync = env.rng().below(4) != 0;
                let mut record: Record = Vec::new();
                let mut batch = WriteBatch::new();
                for _ in 0..count {
                    let key = key(env.rng().below(KEYS));
                    let value = if env.rng().below(10) < 3 {
                        Value::Tombstone
                    } else {
                        let len = usize::try_from(env.rng().below(value_max + 1)).expect("fits");
                        let mut bytes = vec![0u8; len];
                        env.rng().fill_bytes(&mut bytes);
                        Value::Live(Bytes::from(bytes))
                    };
                    match &value {
                        Value::Live(bytes) => batch.put(key.clone(), bytes.clone()),
                        Value::Tombstone => batch.delete(key.clone()),
                    };
                    record.push((key, value));
                }
                let write = db.write(batch, sync);
                let keys: BTreeSet<Bytes> = record.iter().map(|(k, _)| k.clone()).collect();
                {
                    let mut m = lock(&model);
                    m.log.appended.push(engine::encode_batch(&record));
                    m.log.acked.push(false);
                    m.log.sync_requested.push(sync);
                    m.ops.push(record.clone());
                    m.batches += u64::from(count > 1);
                    m.unsynced += u64::from(!sync);
                    assert_eq!(
                        m.ops.len() as u64,
                        write.seq(),
                        "the model and the log number writes alike"
                    );
                    for key in &keys {
                        *m.in_flight.entry(key.clone()).or_default() += 1;
                    }
                }
                match write.await {
                    Ok(seq) => {
                        let mut m = lock(&model);
                        if let Some(acked) = m.log.acked.get_mut(seq as usize - 1) {
                            *acked = true;
                        }
                        for key in &keys {
                            if let Some(n) = m.in_flight.get_mut(key) {
                                *n -= 1;
                            }
                        }
                        for (key, value) in effective(&record) {
                            let newer = m.committed.get(&key).is_none_or(|(s, _)| *s < seq);
                            if newer {
                                m.committed.insert(key, (seq, value));
                            }
                        }
                    }
                    Err(_) => return,
                }
                let gap = env.rng().below(gap_max_us + 1);
                env.clock().sleep(Duration::from_micros(gap)).await;
            }
        });
    }
    // One task takes checkpoints now and then, into a directory of its own each,
    // and records what each must hold: the model's state at the version the engine
    // reports, which nothing after can change.
    {
        let (env, db, model) = (sim.env(node), db.clone(), model.clone());
        env.clone().spawn("checkpointer", async move {
            loop {
                let gap = 2000 + env.rng().below(6000);
                env.clock().sleep(Duration::from_micros(gap)).await;
                let n = lock(&model).checkpoints_taken;
                let dir = PathBuf::from(format!("/ckpt/{n:04}"));
                lock(&model).checkpoints_taken += 1;
                let Ok(info) = db.checkpoint(&dir).await else {
                    return;
                };
                let mut m = lock(&model);
                let version = usize::try_from(info.version).expect("fits");
                let expected = m.state_after(version.min(m.ops.len()));
                m.checkpoints.push(Checkpoint {
                    dir,
                    version: info.version,
                    expected,
                });
            }
        });
    }
    for _ in 0..schedule.readers {
        let (env, db, model) = (sim.env(node), db.clone(), model.clone());
        env.clone().spawn("reader", async move {
            loop {
                // Every other read is a scan at a snapshot: the engine's version pins
                // exactly which ops the model folds, so the answer is exact.
                if env.rng().below(2) == 0 {
                    let lo = env.rng().below(KEYS);
                    let hi = lo + env.rng().below(KEYS - lo + 1);
                    let snapshot = db.snapshot();
                    let want: Vec<(Bytes, Bytes)> = {
                        let m = lock(&model);
                        let n = usize::try_from(snapshot.version()).expect("fits");
                        m.state_after(n.min(m.ops.len()))
                            .into_iter()
                            .filter(|(k, _)| *k >= key(lo) && *k < key(hi))
                            .filter_map(|(k, v)| v.live().map(|v| (k, v)))
                            .collect()
                    };
                    let got = db
                        .scan(&key(lo)[..]..&key(hi)[..], &snapshot)
                        .await
                        .expect("tables read");
                    {
                        let mut m = lock(&model);
                        m.reads += 1;
                        m.scans += 1;
                        if got != want {
                            m.read_violations.push(format!(
                                "scan of k{lo:02}..k{hi:02} at version {} saw {} keys but the model has {}: {:?} against {:?}",
                                snapshot.version(),
                                got.len(),
                                want.len(),
                                got.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect::<Vec<_>>(),
                                want.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect::<Vec<_>>()
                            ));
                        }
                    }
                    drop(snapshot);
                    let gap = env.rng().below(gap_max_us + 1);
                    env.clock().sleep(Duration::from_micros(gap)).await;
                    continue;
                }
                let key = key(env.rng().below(KEYS));
                let expected = {
                    let m = lock(&model);
                    if m.in_flight.get(&key).is_some_and(|&n| n > 0) {
                        None
                    } else {
                        Some(m.committed.get(&key).cloned().and_then(|(_, v)| v.live()))
                    }
                };
                if let Some(want) = expected {
                    let got = db.get(&key).await.expect("tables read");
                    let mut m = lock(&model);
                    m.reads += 1;
                    // A write acknowledged while the read was on its way is not a
                    // disagreement: the read sees before or after it.
                    let now = m.committed.get(&key).cloned().and_then(|(_, v)| v.live());
                    let in_flight = m.in_flight.get(&key).is_some_and(|&n| n > 0);
                    if got != want && got != now && !in_flight {
                        m.read_violations.push(format!(
                            "live read of {} saw {got:?} but the model has {want:?}",
                            String::from_utf8_lossy(&key)
                        ));
                    }
                }
                let gap = env.rng().below(gap_max_us + 1);
                env.clock().sleep(Duration::from_micros(gap)).await;
            }
        });
    }
}

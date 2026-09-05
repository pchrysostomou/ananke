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
use ananke_storage::{Engine, EngineConfig, EngineRecovery, Value, wal};
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
        }
    }
}

/// The key numbered `i`.
#[must_use]
pub fn key(i: u64) -> Bytes {
    Bytes::from(format!("k{i:02}"))
}

/// What the writers know, on top of the log model: the ops in log order, which keys
/// have a write in flight, the committed value per key, and which records are gone
/// for good with an explanation.
#[derive(Debug, Default)]
pub struct Model {
    /// The log model: encoded ops and their acknowledgements.
    pub log: LogModel,
    /// The ops by position, parallel to `log.appended`.
    pub ops: Vec<(Bytes, Value)>,
    /// Writes enqueued and not yet acknowledged, per key.
    pub in_flight: BTreeMap<Bytes, u32>,
    /// The newest acknowledged write per key, by sequence number.
    pub committed: BTreeMap<Bytes, (u64, Value)>,
    /// Records lost with a dropped table or manifest and not brought back by a log
    /// replay, for good.
    pub lost: BTreeSet<u64>,
    /// Live reads performed.
    pub reads: u64,
    /// Live reads that disagreed with the model.
    pub read_violations: Vec<String>,
}

impl Model {
    /// The state after the first `n` ops, minus the ones lost for good: the newest
    /// surviving write per key.
    #[must_use]
    pub fn state_after(&self, n: usize) -> BTreeMap<Bytes, Value> {
        let mut state = BTreeMap::new();
        for (i, (key, value)) in self.ops[..n].iter().enumerate() {
            let seq = i as u64 + 1;
            if self.lost.contains(&seq) {
                continue;
            }
            state.insert(key.clone(), value.clone());
        }
        state
    }
}

type SharedModel = Arc<Mutex<Model>>;

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
    /// The model's ops when the node crashed, for diagnosis.
    pub ops: Vec<(Bytes, Value)>,
    /// Records counted as lost after this recovery, for diagnosis.
    pub lost: Vec<u64>,
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
    /// Live reads over the run.
    pub reads: u64,
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
    for crash in 0..=schedule.crashes {
        let before_open = sim.trace().len();
        let (db, recovery) = open(&mut sim, node, &schedule, variant);
        let dir = Path::new(DIR);
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
        // Losses the trace explains, else violations. What was lost before stays
        // lost unless the log brought it back.
        let mut verdict = Ok(());
        let mut excused: BTreeMap<u64, Excuse> = lock(&model)
            .lost
            .iter()
            .map(|&seq| (seq, Excuse::LostFsync))
            .collect();
        let synced = syncs(&events, dir);
        for meta in &recovery.dropped {
            // A table dropped once stays listed and is dropped at every open after.
            if excused.contains_key(&meta.first_seq) {
                continue;
            }
            let path = manifest::sst_path(dir, meta.number);
            let why = if synced.sst_betrayed.contains(&meta.number) {
                Some(Excuse::LostFsync)
            } else if rotted(&all, &path) {
                Some(Excuse::BitRot)
            } else {
                None
            };
            match why {
                Some(why) => {
                    for seq in meta.first_seq..=meta.max_seq {
                        excused.insert(seq, why);
                    }
                }
                None => {
                    verdict = Err(format!(
                        "table {} covering {}..={} was dropped without a fault",
                        meta.number, meta.first_seq, meta.max_seq
                    ));
                }
            }
        }
        if let Some(named) = recovery.fallback_from {
            // What CURRENT should have named: the last manifest it was switched to.
            // A fallback that used that manifest or a newer one, written but never
            // switched to, lost nothing and needs no explanation. One that used an
            // older manifest lost everything flushed since, and must be explained:
            // CURRENT itself was damaged (unreadable, or naming something it never was
            // switched to) because its content's sync was lost, for the switch or one
            // in flight at the crash, or bit rot hit it; or the manifest was damaged
            // because its sync was lost or bit rot hit it. Nothing else explains it.
            let from = synced
                .switched
                .last()
                .copied()
                .or_else(|| previous_manifest.map(|(n, _)| n))
                .unwrap_or(0);
            if recovery.manifest < from {
                let current_damaged = named != from;
                let current_betrayed =
                    synced.current_betrayed.contains(&from) || synced.current_tmp_lost_in_flight;
                let why = if current_damaged && current_betrayed {
                    Some(Excuse::LostFsync)
                } else if current_damaged && rotted(&all, &manifest::current_path(dir)) {
                    Some(Excuse::BitRot)
                } else if synced.manifest_betrayed.contains(&from) {
                    Some(Excuse::LostFsync)
                } else if rotted(&all, &manifest::manifest_path(dir, from)) {
                    Some(Excuse::BitRot)
                } else {
                    None
                };
                let flushed_from = synced.manifest_flushed.get(&from).copied().or_else(|| {
                    previous_manifest
                        .filter(|&(n, _)| n == from)
                        .map(|(_, f)| f)
                });
                // Only records that existed can be lost; the manifest's flushed point
                // can be past what the model had, since a flush and a crash race the
                // writers.
                let existed = lock(&model).ops.len() as u64;
                match (why, flushed_from) {
                    (Some(why), Some(flushed_from)) if flushed_from > recovery.flushed_seq => {
                        for seq in recovery.flushed_seq + 1..=flushed_from.min(existed) {
                            excused.insert(seq, why);
                        }
                    }
                    (Some(_), _) => {}
                    (None, _) => {
                        verdict = Err(format!(
                            "CURRENT named manifest {named} and recovery used {}, but the last switch was to {from} and no fault touched CURRENT or manifest {from}",
                            recovery.manifest
                        ));
                    }
                }
            }
        }
        let last_recovered = recovery.first_seq_end();
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
        // What the log replayed into memtables is back, whatever a table lost: the
        // records past the manifest's flushed point and from the log's first record,
        // up to the last one recovered. What the log skipped past the flushed point,
        // the rest of a segment after a covered stop, is gone like a dropped table.
        let replayed = (recovery.flushed_seq + 1).max(recovery.wal.first_seq)..=last_recovered;
        let holes = (recovery.flushed_seq + 1..=end as u64)
            .filter(|seq| recovery.wal.records.is_empty() || !replayed.contains(seq));
        {
            let mut m = lock(&model);
            m.lost = excused
                .keys()
                .copied()
                .filter(|seq| recovery.wal.records.is_empty() || !replayed.contains(seq))
                .chain(holes)
                .filter(|&seq| seq <= end as u64)
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
        previous_manifest = Some((recovery.manifest, recovery.flushed_seq));
        base = end;
        {
            let mut m = lock(&model);
            m.log.appended.truncate(base);
            m.log.acked.truncate(base);
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
    let reads = lock(&model).reads;
    Report {
        seed,
        variant,
        epochs,
        reads,
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

/// Where the open task leaves the engine and what it recovered.
type Opened = Arc<Mutex<Option<(Db, EngineRecovery)>>>;

/// Opens the engine on `node` and runs the open to completion.
fn open(
    sim: &mut Sim,
    node: NodeId,
    schedule: &Schedule,
    variant: Variant,
) -> (Db, EngineRecovery) {
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
    };
    env.clone().spawn("engine-open", async move {
        let (db, recovery) = Engine::open(env, config).await.expect("the engine opens");
        *o.lock().unwrap_or_else(PoisonError::into_inner) = Some((Arc::new(db), recovery));
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

/// Starts the writers and readers; they run until the crash.
fn spawn_clients(sim: &Sim, node: NodeId, db: Db, schedule: &Schedule, model: &SharedModel) {
    let (value_max, gap_max_us) = (schedule.value_max, schedule.gap_max_us);
    for _ in 0..schedule.writers {
        let (env, db, model) = (sim.env(node), db.clone(), model.clone());
        env.clone().spawn("writer", async move {
            loop {
                let key = key(env.rng().below(KEYS));
                let value = if env.rng().below(10) < 3 {
                    Value::Tombstone
                } else {
                    let len = usize::try_from(env.rng().below(value_max + 1)).expect("fits");
                    let mut bytes = vec![0u8; len];
                    env.rng().fill_bytes(&mut bytes);
                    Value::Live(Bytes::from(bytes))
                };
                let write = match &value {
                    Value::Live(bytes) => db.put(key.clone(), bytes.clone()),
                    Value::Tombstone => db.delete(key.clone()),
                };
                {
                    let mut m = lock(&model);
                    m.log.appended.push(engine::encode_op(&key, &value));
                    m.log.acked.push(false);
                    m.ops.push((key.clone(), value.clone()));
                    assert_eq!(
                        m.ops.len() as u64,
                        write.seq(),
                        "the model and the log number writes alike"
                    );
                    *m.in_flight.entry(key.clone()).or_default() += 1;
                }
                match write.await {
                    Ok(seq) => {
                        let mut m = lock(&model);
                        if let Some(acked) = m.log.acked.get_mut(seq as usize - 1) {
                            *acked = true;
                        }
                        if let Some(n) = m.in_flight.get_mut(&key) {
                            *n -= 1;
                        }
                        let newer = m.committed.get(&key).is_none_or(|(s, _)| *s < seq);
                        if newer {
                            m.committed.insert(key, (seq, value));
                        }
                    }
                    Err(_) => return,
                }
                let gap = env.rng().below(gap_max_us + 1);
                env.clock().sleep(Duration::from_micros(gap)).await;
            }
        });
    }
    for _ in 0..schedule.readers {
        let (env, db, model) = (sim.env(node), db.clone(), model.clone());
        env.clone().spawn("reader", async move {
            loop {
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

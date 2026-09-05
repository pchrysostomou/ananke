//! The Phase 1 crash-injection property (SPEC.md §2.8) for the engine so far, a
//! write-ahead log in front of memtables (D-020): writers put and delete random keys
//! while readers check what they see, the harness crashes the node at random points
//! with every §1.3 fault on and, often, with a memtable mid-flush, and after each
//! recovery the engine's state is checked against a `BTreeMap` model.
//!
//! The log-level obligation is the WAL scenario's, reused as is: the recovered
//! records must be a prefix of what was written, reaching every acknowledged write
//! whose sync the simulator honoured (`wal::check_epoch`). On top of it: the state
//! after recovery must equal the model folded over exactly the recovered prefix, and
//! every live read during the run must return what the model holds for a key with
//! no write in flight. Both [`Variant`]s run through the same checks; the correct
//! engine must pass every seed and the buggy one must fail some (CLAUDE.md).

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ananke_env::moirae::{Export, bytes_decoder};
use ananke_env::sim::{Sim, SimConfig, SimEnv, TraceRecord};
use ananke_env::{Clock, Environment, NodeId, Rng, TraceEvent};
use ananke_storage::engine;
use ananke_storage::{
    Engine, EngineConfig, EngineRecovery, FlushSink, Memtable, Retain, Value, wal,
};
use bytes::Bytes;

use crate::wal::{Excuse, Model as LogModel, check_epoch};

pub use ananke_storage::engine::Variant;

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
    /// How long the stand-in sink takes to flush a memtable: the window in which a
    /// crash finds one mid-flush.
    pub flush_delay: Duration,
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
            flush_delay: Duration::from_millis(1),
        }
    }
}

/// The key numbered `i`.
#[must_use]
pub fn key(i: u64) -> Bytes {
    Bytes::from(format!("k{i:02}"))
}

/// What the writers know, on top of the log model: the ops in log order, which keys
/// have a write in flight, and the committed value per key.
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
    /// Live reads performed.
    pub reads: u64,
    /// Live reads that disagreed with the model.
    pub read_violations: Vec<String>,
    /// Keys whose state after a recovery disagreed with the model.
    pub state_violations: Vec<String>,
}

impl Model {
    /// The state after replaying the first `n` ops: the newest write per key.
    #[must_use]
    pub fn state_after(&self, n: usize) -> BTreeMap<Bytes, Value> {
        let mut state = BTreeMap::new();
        for (key, value) in &self.ops[..n] {
            state.insert(key.clone(), value.clone());
        }
        state
    }
}

type SharedModel = Arc<Mutex<Model>>;

fn lock(model: &SharedModel) -> MutexGuard<'_, Model> {
    model.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The stand-in sink with a cost: a flush takes `delay` of virtual time before the
/// memtable is released, so a crash can land while one is in flight.
pub struct SlowRetain {
    env: SimEnv,
    delay: Duration,
    inner: Retain,
}

impl FlushSink for SlowRetain {
    fn flush(&self, memtable: Arc<Memtable>) -> impl Future<Output = io::Result<()>> + Send {
        let (env, delay) = (self.env.clone(), self.delay);
        async move {
            env.clock().sleep(delay).await;
            self.inner.flush(memtable).await
        }
    }

    fn get(&self, key: &[u8]) -> impl Future<Output = io::Result<Option<Value>>> + Send {
        self.inner.get(key)
    }
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
    /// Ops the previous recovery had returned.
    pub base: usize,
    /// Memtables rotated but not flushed when the node crashed.
    pub mid_flush: usize,
    /// What let the log off, if anything did.
    pub excuse: Option<Excuse>,
    /// The first violation, if any.
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
}

/// The simulator configuration: every §1.3 fault on.
#[must_use]
pub fn config(seed: u64, schedule: &Schedule) -> SimConfig {
    let mut config = SimConfig::new(seed);
    config.fs.p_durable = 0.8;
    config.fs.p_bitrot = 0.05;
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
    let mut epochs = Vec::new();
    let mut base = 0;
    let mut epoch_start = 0;
    for crash in 0..=schedule.crashes {
        let before_open = sim.trace().len();
        let (db, recovery) = open(&mut sim, node, &schedule, variant, &model);
        let recovered = recovery.wal.records.len();
        if crash > 0 {
            let records = sim.trace();
            let events: Vec<&TraceEvent> = records[epoch_start..before_open]
                .iter()
                .map(|r| &r.event)
                .collect();
            let mut m = lock(&model);
            let (mut verdict, excuse) =
                check_epoch(&m.log, base, &recovery.wal, &events, Path::new(DIR));
            if verdict.is_ok()
                && let Some(violation) = m.state_violations.first()
            {
                verdict = Err(violation.clone());
            }
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
                excuse,
                verdict,
            });
            m.state_violations.clear();
            m.read_violations.clear();
        }
        base = recovered;
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
        // one instant: a write may be acknowledged but not yet written, a flush may
        // be half done.
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

type Db = Arc<Engine<SimEnv, SlowRetain>>;

/// Where the open task leaves the engine and what it recovered.
type Opened = Arc<Mutex<Option<(Db, EngineRecovery)>>>;

/// Opens the engine on `node`, checks its state against the model folded over the
/// recovered prefix, and runs the open to completion at the current instant.
fn open(
    sim: &mut Sim,
    node: NodeId,
    schedule: &Schedule,
    variant: Variant,
    model: &SharedModel,
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
    };
    let sink = SlowRetain {
        env: env.clone(),
        delay: schedule.flush_delay,
        inner: Retain::default(),
    };
    let model = model.clone();
    env.clone().spawn("engine-open", async move {
        let (db, recovery) = Engine::open(env, config, sink)
            .await
            .expect("the engine opens");
        let n = recovery.wal.records.len();
        let expected = lock(&model).state_after(n);
        let mut violations = Vec::new();
        for i in 0..KEYS {
            let key = key(i);
            let got = db.get(&key).await.expect("the stand-in sink reads");
            let want = expected.get(&key).cloned().and_then(Value::live);
            if got != want {
                violations.push(format!(
                    "after recovering {n} records, key {} holds {got:?} but the model has {want:?}",
                    String::from_utf8_lossy(&key)
                ));
            }
        }
        lock(&model).state_violations = violations;
        *o.lock().unwrap_or_else(PoisonError::into_inner) = Some((Arc::new(db), recovery));
    });
    sim.run_for(Duration::ZERO);
    opened
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the open completed at this instant")
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
                    let got = db.get(&key).await.expect("the stand-in sink reads");
                    let mut m = lock(&model);
                    m.reads += 1;
                    if got != want {
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

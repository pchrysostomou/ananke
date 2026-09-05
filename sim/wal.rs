//! The Phase 1 crash-injection property (SPEC.md §2.8) reduced to the write-ahead log
//! (D-018): appenders write random records while the harness crashes the node at
//! random points with every §1.3 fault on, and after each recovery what came back is
//! checked against a model of what was committed.
//!
//! The model is built from the appenders' acknowledgements and the trace, never from
//! the disk. Recovered records must be a prefix of what was appended, and the prefix
//! must reach every record that was acknowledged and covered by a sync the simulator
//! honoured, with three excuses and no more: a record covered only by lost syncs, a
//! stop on a record bit rot hit, and a stop exactly where a cut whose sync was lost
//! had been. And, whatever recovery returned, nothing may have been acknowledged
//! before a sync was asked for: that is the bug an excuse must never hide. Every [`Variant`] runs through the same check; the correct log must pass
//! every seed and each buggy one must fail some (CLAUDE.md).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ananke_env::moirae::{Export, bytes_decoder};
use ananke_env::sim::{Sim, SimConfig, SimEnv, TraceRecord};
use ananke_env::{Clock, Environment, NodeId, Rng, TraceEvent};
use ananke_storage::wal::{HEADER_LEN, segment_of, segment_path};
use ananke_storage::{Recovery, Seq, Variant, Wal, WalConfig};
use bytes::Bytes;

/// Where the log lives on the node's disk.
pub const DIR: &str = "/wal";

/// How the run is shaped.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    /// Crashes per run; each is followed by a recovery that is checked.
    pub crashes: u32,
    /// Tasks appending at once, so groups have something to group.
    pub appenders: u32,
    /// The longest payload; lengths are uniform from zero.
    pub payload_max: u64,
    /// The longest pause between one appender's records, in microseconds.
    pub gap_max_us: u64,
    /// The shortest and longest time between a start and the next crash.
    pub run_min: Duration,
    /// See `run_min`.
    pub run_max: Duration,
    /// The log's segment size.
    pub segment_bytes: u64,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            crashes: 8,
            appenders: 3,
            payload_max: 64,
            gap_max_us: 500,
            run_min: Duration::from_millis(2),
            run_max: Duration::from_millis(40),
            segment_bytes: 1024,
        }
    }
}

/// What the appenders know: every record they enqueued, in log order, and which
/// were acknowledged. Truncated to what recovery returned after each crash.
#[derive(Debug, Default)]
pub struct Model {
    /// Payloads by position; `appended[i]` has sequence number `i + 1`.
    pub appended: Vec<Bytes>,
    /// Whether the append at each position resolved with `Ok`.
    pub acked: Vec<bool>,
}

type SharedModel = Arc<Mutex<Model>>;

fn lock(model: &SharedModel) -> MutexGuard<'_, Model> {
    model.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Why a missing record, or everything after it, did not count against the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Excuse {
    /// Every sync that covered the record was lost by the simulator.
    LostFsync,
    /// Recovery stopped on a record that bit rot had flipped a bit of.
    BitRot,
    /// Recovery stopped exactly where the previous recovery had cut a segment, and
    /// the sync of that cut was lost.
    BetrayedCut,
}

/// One crash and the recovery after it, checked.
#[derive(Debug)]
pub struct Epoch {
    /// What recovery returned.
    pub recovery: Recovery,
    /// Records in the model when the node crashed.
    pub appended: usize,
    /// The model's records when the node crashed, for diagnosis.
    pub model: Vec<Bytes>,
    /// Of those, acknowledged.
    pub acked: usize,
    /// Records the previous recovery had returned: on disk when this epoch started.
    pub base: usize,
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
    /// Which log ran.
    pub variant: Variant,
    /// Each crash and its recovery, in order.
    pub epochs: Vec<Epoch>,
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

    /// How many records the run appended, over every epoch.
    #[must_use]
    pub fn appended(&self) -> usize {
        self.epochs.iter().map(|e| e.appended - e.base).sum()
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
    // Where this epoch's events start in the trace: its own open, its run, and the
    // crash's fault events, but not the next open, whose cut cannot explain this stop.
    let mut epoch_start = 0;
    for crash in 0..=schedule.crashes {
        let before_open = sim.trace().len();
        let (wal, recovery) = open(&mut sim, node, &schedule, variant);
        if crash > 0 {
            let records = sim.trace();
            let events: Vec<&TraceEvent> = records[epoch_start..before_open]
                .iter()
                .map(|r| &r.event)
                .collect();
            let (appended, acked, snapshot) = {
                let m = lock(&model);
                (
                    m.appended.len(),
                    m.acked.iter().filter(|&&a| a).count(),
                    m.appended.clone(),
                )
            };
            let (verdict, excuse) =
                check_epoch(&lock(&model), base, &recovery, &events, Path::new(DIR));
            epochs.push(Epoch {
                recovery: recovery.clone(),
                appended,
                model: snapshot,
                acked,
                base,
                excuse,
                verdict,
            });
        }
        base = recovery.records.len();
        {
            let mut m = lock(&model);
            m.appended.truncate(base);
            m.acked.truncate(base);
            m.acked.iter_mut().for_each(|a| *a = false);
        }
        epoch_start = before_open;
        if crash == schedule.crashes {
            break;
        }
        spawn_appenders(&sim, node, wal, &schedule, &model);
        let span = schedule.run_max.saturating_sub(schedule.run_min);
        let extra = Duration::from_nanos(harness.below(span.as_nanos() as u64 + 1));
        sim.run_for(schedule.run_min + extra);
        // Then a few more scheduling steps, so the crash lands between two polls at
        // one instant and not only where every queue has drained.
        sim.run_steps(harness.below(64));
        sim.crash(node);
        sim.restart(node);
    }
    Report {
        seed,
        variant,
        epochs,
        jsonl: sim
            .to_moirae(&Export::new(&bytes_decoder))
            .expect("the wal trace exports to moirae v2"),
        records: sim.trace(),
    }
}

/// Where the open task leaves the log and what it recovered.
type Opened = Arc<Mutex<Option<(Arc<Wal<SimEnv>>, Recovery)>>>;

/// Opens the log on `node` and runs the open to completion at the current instant.
fn open(
    sim: &mut Sim,
    node: NodeId,
    schedule: &Schedule,
    variant: Variant,
) -> (Arc<Wal<SimEnv>>, Recovery) {
    let env = sim.env(node);
    let opened: Opened = Arc::default();
    let o = opened.clone();
    let config = WalConfig {
        dir: PathBuf::from(DIR),
        segment_bytes: schedule.segment_bytes,
        variant,
    };
    env.clone().spawn("wal-open", async move {
        let (wal, recovery) = Wal::open(env, config).await.expect("the log opens");
        *o.lock().unwrap_or_else(PoisonError::into_inner) = Some((Arc::new(wal), recovery));
    });
    // Every step of recovery is a ready future, so the open completes at this instant.
    sim.run_for(Duration::ZERO);
    opened
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the open completed at this instant")
}

/// Starts the appenders: random payloads at random gaps, each recorded in the model
/// as it is enqueued and marked when it is acknowledged. They run until the crash.
fn spawn_appenders(
    sim: &Sim,
    node: NodeId,
    wal: Arc<Wal<SimEnv>>,
    schedule: &Schedule,
    model: &SharedModel,
) {
    let (payload_max, gap_max_us) = (schedule.payload_max, schedule.gap_max_us);
    for _ in 0..schedule.appenders {
        let (env, wal, model) = (sim.env(node), wal.clone(), model.clone());
        env.clone().spawn("appender", async move {
            loop {
                let len = usize::try_from(env.rng().below(payload_max + 1)).expect("fits");
                let mut payload = vec![0u8; len];
                env.rng().fill_bytes(&mut payload);
                let payload = Bytes::from(payload);
                let append = wal.append(payload.clone());
                {
                    let mut m = lock(&model);
                    m.appended.push(payload);
                    m.acked.push(false);
                    assert_eq!(
                        m.appended.len() as u64,
                        append.seq(),
                        "the model and the log number records alike"
                    );
                }
                match append.await {
                    Ok(seq) => {
                        let mut m = lock(&model);
                        if let Some(acked) = m.acked.get_mut(seq as usize - 1) {
                            *acked = true;
                        }
                    }
                    Err(_) => return,
                }
                let gap = env.rng().below(gap_max_us + 1);
                env.clock().sleep(Duration::from_micros(gap)).await;
            }
        });
    }
}

/// Checks one recovery against the model. Returns the verdict and the excuse used.
/// Shared with the engine scenario, whose log records are its ops.
pub fn check_epoch(
    model: &Model,
    base: usize,
    recovery: &Recovery,
    events: &[&TraceEvent],
    dir: &Path,
) -> (Result<(), String>, Option<Excuse>) {
    let recovered = &recovery.records;
    // Property A: what came back is a prefix of what was appended.
    if recovered.len() > model.appended.len() {
        return (
            Err(format!(
                "recovered {} records but only {} were ever appended",
                recovered.len(),
                model.appended.len()
            )),
            None,
        );
    }
    for (i, (got, want)) in recovered.iter().zip(&model.appended).enumerate() {
        if got != want {
            return (
                Err(format!(
                    "record {} came back changed: {} bytes recovered, {} appended",
                    i + 1,
                    got.len(),
                    want.len()
                )),
                None,
            );
        }
    }
    // The syncs the log claimed, and whether the simulator honoured each.
    let mut lost_since: BTreeSet<u64> = BTreeSet::new();
    let mut syncs: Vec<(Seq, Seq, bool)> = Vec::new();
    let mut cuts: Vec<(u64, u64, bool)> = Vec::new();
    for event in events {
        match event {
            TraceEvent::FsyncLost { path } => {
                if path.parent() == Some(dir)
                    && let Some(segment) = segment_of(path)
                {
                    lost_since.insert(segment);
                }
            }
            TraceEvent::WalSynced {
                segment,
                first,
                up_to,
            } => syncs.push((*first, *up_to, lost_since.remove(segment))),
            TraceEvent::WalTruncated { segment, len } => {
                cuts.push((*segment, *len, lost_since.remove(segment)));
            }
            _ => {}
        }
    }
    // Property C: nothing was acknowledged before a sync was asked for. Independent
    // of what recovery returned, so a lost fsync earlier in the log cannot hide it.
    for (i, &acked) in model.acked.iter().enumerate() {
        let seq = i as u64 + 1;
        if seq as usize <= base || !acked {
            continue;
        }
        if !syncs
            .iter()
            .any(|&(first, up_to, _)| first <= seq && seq <= up_to)
        {
            return (
                Err(format!(
                    "record {seq} was acknowledged with no sync attempted before the crash"
                )),
                None,
            );
        }
    }
    // Whether the stop is explained: bit rot inside the record it stopped on, or a
    // betrayed cut exactly there.
    let stop_excuse = recovery.stop.and_then(|stop| {
        let next = model.appended.get(recovered.len()).map_or(0, Bytes::len) as u64;
        let span = stop.offset..stop.offset + HEADER_LEN as u64 + next;
        let path = segment_path(dir, stop.segment);
        let rotted = events.iter().any(|e| {
            matches!(e, TraceEvent::BlockRotted { path: p, offset, .. }
                if *p == path && span.contains(offset))
        });
        let betrayed_cut = cuts
            .iter()
            .any(|&(segment, len, lost)| lost && segment == stop.segment && len == stop.offset);
        if rotted {
            Some(Excuse::BitRot)
        } else if betrayed_cut {
            Some(Excuse::BetrayedCut)
        } else {
            None
        }
    });
    // Property B: every record the log owed is there.
    let mut excuse = None;
    for seq in (recovered.len() as u64 + 1)..=(model.appended.len() as u64) {
        let attempts: Vec<bool> = syncs
            .iter()
            .filter(|&&(first, up_to, _)| first <= seq && seq <= up_to)
            .map(|&(_, _, lost)| lost)
            .collect();
        let honoured = attempts.iter().any(|&lost| !lost);
        let attempted = !attempts.is_empty();
        let acked = model.acked.get(seq as usize - 1).copied().unwrap_or(false);
        let owed = seq as usize <= base || honoured || (acked && !attempted);
        if owed {
            if let Some(why) = stop_excuse {
                excuse = Some(why);
                break;
            }
            let reason = recovery.stop.map_or("the end of the log".to_owned(), |s| {
                format!(
                    "segment {} offset {} ({})",
                    s.segment,
                    s.offset,
                    s.reason.as_str()
                )
            });
            let what = if seq as usize <= base {
                "was on disk at the start of the epoch"
            } else if honoured {
                "was acknowledged after a sync the simulator honoured"
            } else {
                "was acknowledged without any sync"
            };
            return (
                Err(format!(
                    "record {seq} {what} but recovery stopped at {reason} with {} records",
                    recovered.len()
                )),
                None,
            );
        }
        if attempted {
            excuse = Some(Excuse::LostFsync);
            break;
        }
    }
    (Ok(()), excuse.or(stop_excuse))
}

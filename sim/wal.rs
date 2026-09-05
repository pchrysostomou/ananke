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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ananke_env::moirae::{Export, bytes_decoder};
use ananke_env::sim::{Sim, SimConfig, SimEnv, TraceRecord};
use ananke_env::{Clock, DirEntryOp, Environment, NodeId, Rng, TraceEvent, WalStop};
use ananke_storage::manifest;
use ananke_storage::wal::{HEADER_LEN, segment_of, segment_path};
use ananke_storage::{CoveredStop, HeadGapPolicy, Recovery, Seq, Variant, Wal, WalConfig};
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
    /// Records below the log's first after the last recovery: gone with their
    /// segments, already judged, and not owed again.
    pub lost: BTreeSet<Seq>,
    /// Appends the log numbered other than the model expected, which no property
    /// could judge from then on.
    pub numbering: Vec<String>,
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
    /// A compaction dropped the record: a newer write of its key hid it, or it was a
    /// tombstone with nothing older below (D-023). Only the engine scenario uses it.
    Compacted,
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
            let segment_first: BTreeMap<u64, Seq> = records
                .iter()
                .filter_map(|r| match r.event {
                    TraceEvent::WalSegmentOpened { segment, first } => Some((segment, first)),
                    _ => None,
                })
                .collect();
            let recovered = Recovered {
                first_seq: recovery.first_seq,
                records: &recovery.records,
                stop: recovery.stop,
                head_gap: recovery.head_gap,
                covered_stops: &recovery.covered_stops,
                segment_first: &segment_first,
                covered_through: 0,
                excused: lock(&model)
                    .lost
                    .iter()
                    .map(|&seq| (seq, Excuse::LostFsync))
                    .collect(),
            };
            let all: Vec<&TraceEvent> = records[..before_open].iter().map(|r| &r.event).collect();
            let (mut verdict, excuse) = check_epoch(
                &lock(&model),
                base,
                &recovered,
                &events,
                &all,
                Path::new(DIR),
            );
            if verdict.is_ok()
                && let Some(violation) = lock(&model).numbering.first()
            {
                verdict = Err(violation.clone());
            }
            if verdict.is_ok() && recovery.next_seq > appended as u64 + 1 {
                verdict = Err(format!(
                    "the log's next number is {} but only {appended} records were ever appended",
                    recovery.next_seq
                ));
            }
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
        // The model follows the log's numbering: the next append takes `next_seq`,
        // so the model holds `next_seq - 1` records, and those below the log's first
        // are gone with their segments.
        {
            let mut m = lock(&model);
            base = usize::try_from(recovery.next_seq - 1)
                .unwrap_or(usize::MAX)
                .min(m.appended.len());
            m.appended.truncate(base);
            m.acked.truncate(base);
            m.acked.iter_mut().for_each(|a| *a = false);
            // Only records the model holds can be owed; a log numbering past them
            // (a checksum skipped over a rotted number) was judged above.
            m.lost = (1..recovery.first_seq.min(base as u64 + 1)).collect();
            m.numbering.clear();
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
        expected_head: 1,
        // A bare log holds nothing elsewhere: a missing head empties it.
        head_gap: HeadGapPolicy::Discard,
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
                    let expected = m.appended.len() as u64 + 1;
                    if append.seq() != expected {
                        // A log numbering records other than by position is a
                        // violation no other property can judge: record it and stop.
                        m.numbering.push(format!(
                            "the log numbered an append {} where the model expected {expected}",
                            append.seq()
                        ));
                        return;
                    }
                    m.appended.push(payload);
                    m.acked.push(false);
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

/// What a recovery answers for: the log records that came back, numbered from
/// `first_seq`, plus what is held elsewhere. A bare log covers nothing and excuses
/// nothing; the engine passes its manifest's `flushed_seq` and the ranges whose loss
/// the trace explained.
pub struct Recovered<'a> {
    /// The sequence number of `records[0]`.
    pub first_seq: Seq,
    /// The records the log recovered.
    pub records: &'a [Bytes],
    /// Where the log stopped short, if it did.
    pub stop: Option<WalStop>,
    /// Records from the expected head up to the first one found, gone with their
    /// segments, and the whole log discarded with them (D-022).
    pub head_gap: Option<(Seq, Seq)>,
    /// Stops the log skipped because the tables held the record; each cost the rest
    /// of its segment.
    pub covered_stops: &'a [CoveredStop],
    /// The first sequence number of every segment the log ever opened, from the
    /// whole trace, for a skip whose stop was a segment's first record.
    pub segment_first: &'a BTreeMap<u64, Seq>,
    /// Records numbered this or below are held in tables, unless excused.
    pub covered_through: Seq,
    /// Records that are gone for a reason the trace explained.
    pub excused: BTreeMap<Seq, Excuse>,
}

/// Syncs the trace recorded and whether the simulator honoured each, by file.
#[derive(Debug, Default)]
pub struct Syncs {
    /// The log's group syncs: (segment, first, up_to, lost).
    pub wal: Vec<(u64, Seq, Seq, bool)>,
    /// Recovery's cuts: (segment, len, lost).
    pub cuts: Vec<(u64, u64, bool)>,
    /// Tables whose sync was lost.
    pub sst_betrayed: BTreeSet<u64>,
    /// Manifests whose sync was lost.
    pub manifest_betrayed: BTreeSet<u64>,
    /// Every manifest written and what it covered.
    pub manifest_flushed: BTreeMap<u64, Seq>,
    /// Manifests `CURRENT` was switched to, in order.
    pub switched: Vec<u64>,
    /// Switches whose `CURRENT.tmp` sync was lost: `CURRENT` may come back empty.
    pub current_betrayed: BTreeSet<u64>,
    /// A switch was in flight at the end with its `CURRENT.tmp` sync lost: the rename
    /// may have survived the crash with nothing behind it.
    pub current_tmp_lost_in_flight: bool,
    /// Segments the engine deleted once tables held their records: nothing that
    /// happened to their syncs explains a loss.
    pub deleted: BTreeSet<u64>,
}

/// Reads the syncs out of an epoch's events: a sync event for a file that a
/// `FsyncLost` for the same file preceded, with no sync event in between, was lost.
#[must_use]
pub fn syncs(events: &[&TraceEvent], dir: &Path) -> Syncs {
    let mut lost_since: BTreeSet<PathBuf> = BTreeSet::new();
    let mut out = Syncs::default();
    for event in events {
        match event {
            TraceEvent::FsyncLost { path } if path.parent() == Some(dir) => {
                lost_since.insert(path.clone());
            }
            TraceEvent::WalSynced {
                segment,
                first,
                up_to,
            } => {
                let lost = lost_since.remove(&segment_path(dir, *segment));
                out.wal.push((*segment, *first, *up_to, lost));
            }
            TraceEvent::WalTruncated { segment, len } => {
                let lost = lost_since.remove(&segment_path(dir, *segment));
                out.cuts.push((*segment, *len, lost));
            }
            // A table or manifest number can be written again after a fallback
            // abandoned its first life; the latest write is the file on disk.
            TraceEvent::SstWritten { number, .. } => {
                if lost_since.remove(&manifest::sst_path(dir, *number)) {
                    out.sst_betrayed.insert(*number);
                } else {
                    out.sst_betrayed.remove(number);
                }
            }
            TraceEvent::ManifestWritten {
                number,
                flushed_seq,
                ..
            } => {
                if lost_since.remove(&manifest::manifest_path(dir, *number)) {
                    out.manifest_betrayed.insert(*number);
                } else {
                    out.manifest_betrayed.remove(number);
                }
                out.manifest_flushed.insert(*number, *flushed_seq);
            }
            TraceEvent::CurrentSwitched { manifest: number } => {
                if lost_since.remove(&manifest::current_tmp_path(dir)) {
                    out.current_betrayed.insert(*number);
                }
                out.switched.push(*number);
            }
            TraceEvent::WalSegmentDeleted { segment } => {
                out.deleted.insert(*segment);
            }
            // A deletion the crash undid: the segment is back, and what became of
            // its syncs explains its records again.
            TraceEvent::DirectoryEntryLost {
                entry,
                op: DirEntryOp::Unlink,
                ..
            } => {
                if let Some(segment) = segment_of(entry) {
                    out.deleted.remove(&segment);
                }
            }
            _ => {}
        }
    }
    out.current_tmp_lost_in_flight = lost_since.contains(&manifest::current_tmp_path(dir));
    out
}

/// Whether a `BlockRotted` in `events` hit `path`.
#[must_use]
pub fn rotted(events: &[&TraceEvent], path: &Path) -> bool {
    events
        .iter()
        .any(|e| matches!(e, TraceEvent::BlockRotted { path: p, .. } if p == path))
}

/// Checks one recovery against the model. Returns the verdict and the excuse used.
/// Shared with the engine scenario, whose log records are its ops. `events` are the
/// epoch's, for the syncs; `all_events` the whole trace so far, for bit rot, which
/// stays in a file until the file is cut or deleted and can bite epochs later.
pub fn check_epoch(
    model: &Model,
    base: usize,
    recovered: &Recovered<'_>,
    events: &[&TraceEvent],
    all_events: &[&TraceEvent],
    dir: &Path,
) -> (Result<(), String>, Option<Excuse>) {
    let records = recovered.records;
    let first = recovered.first_seq;
    // Property A: what came back is what was appended, at the same numbers.
    for (i, got) in records.iter().enumerate() {
        let seq = first + i as u64;
        let Some(want) = model.appended.get(seq as usize - 1) else {
            return (
                Err(format!(
                    "recovered record {seq} but only {} were ever appended",
                    model.appended.len()
                )),
                None,
            );
        };
        if got != want {
            return (
                Err(format!(
                    "record {seq} came back changed: {} bytes recovered, {} appended",
                    got.len(),
                    want.len()
                )),
                None,
            );
        }
    }
    let all_syncs = syncs(all_events, dir);
    let syncs = syncs(events, dir);
    // Property C: nothing was acknowledged before a sync was asked for. Independent
    // of what recovery returned, so a lost fsync earlier in the log cannot hide it.
    for (i, &acked) in model.acked.iter().enumerate() {
        let seq = i as u64 + 1;
        if seq as usize <= base || !acked {
            continue;
        }
        if !syncs
            .wal
            .iter()
            .any(|&(_, first, up_to, _)| first <= seq && seq <= up_to)
        {
            return (
                Err(format!(
                    "record {seq} was acknowledged with no sync attempted before the crash"
                )),
                None,
            );
        }
    }
    // What a stop costs and what explains it. A stop, or a skip over a covered stop,
    // makes a range of records unrecoverable: everything after a stop, the rest of
    // the segment after a skip. Each is explained by a fault on the record it stopped
    // at: bit rot inside it, a write of it the crash tore, a cut whose sync was lost
    // exactly there, or the record having been covered only by lost syncs. Damage to
    // a file is durable until the file is cut or deleted, and a record the tables
    // covered is skipped rather than cut, so the fault may be epochs old: these look
    // at the whole trace, by segment, since segment numbers are never reused.
    let last_recovered = first + records.len() as u64 - 1;
    // Whether every sync that covered record `from` was lost, in the latest segment
    // numbered at most `bound` that held a record of that number: a gap is found at
    // the next segment's first byte while the missing record lived in the one before,
    // and numbers are reused after a cut, so the latest such segment is the record
    // that matters. A segment the engine deleted explains nothing: tables owed its
    // records, whatever became of its syncs.
    let betrayed = |bound: u64, from: Seq| -> bool {
        let mut by_segment: BTreeMap<u64, Vec<bool>> = BTreeMap::new();
        for &(segment, f, up_to, lost) in &all_syncs.wal {
            if segment <= bound
                && f <= from
                && from <= up_to
                && !all_syncs.deleted.contains(&segment)
            {
                by_segment.entry(segment).or_default().push(lost);
            }
        }
        by_segment
            .iter()
            .next_back()
            .is_some_and(|(_, attempts)| attempts.iter().all(|&lost| lost))
    };
    let explain = |stop: &WalStop, from: Seq| -> Option<Excuse> {
        let len = model.appended.get(from as usize - 1).map_or(0, Bytes::len) as u64;
        let span = stop.offset..stop.offset + HEADER_LEN as u64 + len;
        let path = segment_path(dir, stop.segment);
        let rotted = all_events.iter().any(|e| {
            matches!(e, TraceEvent::BlockRotted { path: p, offset, .. }
                if *p == path && span.contains(offset))
        });
        // A torn group write spans every record of the group; the stop is at the
        // first record the tear cut into, anywhere inside the write.
        let torn = all_events.iter().any(|e| {
            matches!(e, TraceEvent::WriteTorn { path: p, offset, written, .. }
                if *p == path && *offset <= stop.offset && stop.offset < *offset + *written as u64)
        });
        let betrayed_cut = all_syncs
            .cuts
            .iter()
            .any(|&(segment, len, lost)| lost && segment == stop.segment && len == stop.offset);
        let betrayed = betrayed(stop.segment, from);
        if rotted {
            Some(Excuse::BitRot)
        } else if torn || betrayed {
            Some(Excuse::LostFsync)
        } else if betrayed_cut {
            Some(Excuse::BetrayedCut)
        } else {
            None
        }
    };
    let mut unrecoverable: Vec<(Seq, Seq, Option<Excuse>)> = Vec::new();
    for skip in recovered.covered_stops {
        let hi = skip.resumed.map_or(u64::MAX, |r| r - 1);
        let from = skip
            .from
            .or_else(|| recovered.segment_first.get(&skip.stop.segment).copied())
            .unwrap_or(1);
        unrecoverable.push((from, hi, explain(&skip.stop, from)));
    }
    if let Some((expected, _)) = recovered.head_gap {
        // A missing head: the whole log went with it (D-022), so one range from the
        // expected head on. On the log's side two things explain it: the segment that
        // held the head was emptied at the crash because every sync of it was lost,
        // or a skip over a covered stop before the first record found, explained by
        // the fault at that stop, took the head with the rest of its segment; and a
        // previous recovery's cut of the segment the gap is found in, whose sync was
        // lost, brings the old records back in front of the new ones. Off the log's
        // side, the caller may have excused the head itself: a manifest fallback that
        // lost the tables the head's segments were deleted for.
        let skipped = unrecoverable.iter().find_map(|&(_, _, why)| why);
        let why = betrayed(u64::MAX, expected)
            .then_some(Excuse::LostFsync)
            .or(skipped)
            .or_else(|| recovered.stop.and_then(|stop| explain(&stop, expected)))
            .or_else(|| recovered.excused.get(&expected).copied());
        unrecoverable.push((expected, u64::MAX, why));
    } else if let Some(stop) = recovered.stop {
        // The record the log stopped at: the one after the last recovered, or, when
        // nothing was, the first record of the stopping segment, which the log may
        // not know when the segments before it were deleted after a flush.
        let from = if records.is_empty() {
            recovered
                .segment_first
                .get(&stop.segment)
                .copied()
                .unwrap_or(first)
        } else {
            last_recovered + 1
        };
        unrecoverable.push((from, u64::MAX, explain(&stop, from)));
    }
    let excused_by = |seq: Seq| recovered.excused.get(&seq).copied();
    let present = |seq: Seq| {
        (seq <= recovered.covered_through && excused_by(seq).is_none())
            || (!records.is_empty() && seq >= first && seq <= last_recovered)
    };
    // Property B: every record the log owed is there, or its absence is explained.
    let mut excuse = None;
    for seq in 1..=(model.appended.len() as u64) {
        if present(seq) {
            continue;
        }
        if let Some(why) = excused_by(seq) {
            excuse = Some(why);
            continue;
        }
        let attempts: Vec<bool> = syncs
            .wal
            .iter()
            .filter(|&&(_, f, up_to, _)| f <= seq && seq <= up_to)
            .map(|&(_, _, _, lost)| lost)
            .collect();
        let honoured = attempts.iter().any(|&lost| !lost);
        let attempted = !attempts.is_empty();
        let acked = model.acked.get(seq as usize - 1).copied().unwrap_or(false);
        let owed = seq as usize <= base || honoured || (acked && !attempted);
        if !owed {
            if attempted {
                excuse = Some(Excuse::LostFsync);
            }
            continue;
        }
        let range = unrecoverable
            .iter()
            .find(|&&(lo, hi, _)| lo <= seq && seq <= hi);
        match range {
            Some(&(_, _, Some(why))) => excuse = Some(why),
            Some(&(lo, _, None)) => {
                let what = if seq as usize <= base {
                    "was on disk at the start of the epoch"
                } else if honoured {
                    "was acknowledged after a sync the simulator honoured"
                } else {
                    "was acknowledged without any sync"
                };
                return (
                    Err(format!(
                        "record {seq} {what} but it is gone with the log from record {lo} on, and no fault explains the stop there (records {first}..={last_recovered}, tables through {}, stop {:?})",
                        recovered.covered_through, recovered.stop
                    )),
                    None,
                );
            }
            None => {
                return (
                    Err(format!(
                        "record {seq} is gone although the log stopped at nothing near it (records {first}..={last_recovered}, tables through {})",
                        recovered.covered_through
                    )),
                    None,
                );
            }
        }
    }
    (Ok(()), excuse)
}

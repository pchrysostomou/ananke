//! Leveled compaction (SPEC.md §2.5, D-023): levels 0 to 6, each ten times the size
//! of the one before, level 0 the flushed memtables that may overlap one another and
//! every deeper level a run of tables with disjoint key ranges.
//!
//! A round picks the level furthest over its limit: level 0 by table count, a deeper
//! level by bytes. From level 0 every table goes; from a deeper level one table, the
//! first past where the last round on that level left off. The tables of the next
//! level that overlap them join, and one merge over all of them writes the outputs
//! into the next level, sealed at `sst_bytes` and never between two writes of one
//! user key. A write is dropped when a newer write of its key sits at or below the
//! oldest live snapshot, since no reader can reach past that newer one; a tombstone
//! that is the newest write of its key is dropped when it is at or below that
//! snapshot and no table of a deeper level holds the key, so nothing older can
//! resurface. Deletes thus survive until they reach the bottom level or no older
//! write lies below.
//!
//! Crash-safe in the flush's order: outputs written and synced (`CompactionWritten`
//! in the trace), the next manifest written, synced and switched to, the outputs put
//! in service, and only then the inputs deleted. A crash before the switch leaves
//! the old manifest in force and the outputs as orphans; one after it leaves the
//! inputs as orphans; recovery removes either. The trace event comes before the
//! manifest is written because a crash can leave the manifest whole on disk, and
//! `CURRENT` naming it, without the syncs that would report either.
//! `Variant::DeleteBeforeManifest` deletes the inputs first, the bug the sweep must
//! catch: a crash between leaves a manifest naming tables that are gone.

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ananke_env::{Environment, FileSystem, TraceEvent};
use bytes::Bytes;

use crate::engine::{FileOf, Shared, Variant, lock};
use crate::ikey;
use crate::manifest::{BOTTOM_LEVEL, LEVELS, Manifest, SstMeta, sst_path};
use crate::memtable::Value;
use crate::merge::{MergeIter, Source};
use crate::sst::{SstReader, SstWriter};
use crate::wal::Seq;

/// What one round of compaction did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compaction {
    /// The level compacted; the outputs went one deeper.
    pub level: u8,
    /// The tables merged and deleted.
    pub inputs: Vec<u64>,
    /// The tables written.
    pub outputs: Vec<u64>,
    /// Writes dropped because a newer write of the key hid them.
    pub dropped_versions: u64,
    /// Tombstones dropped because no older write of the key lay below.
    pub dropped_tombstones: u64,
}

/// A round chosen: which tables to merge into which level.
struct Plan {
    level: u8,
    inputs: Vec<SstMeta>,
    next_inputs: Vec<SstMeta>,
    /// Tables deeper than the output level, for the tombstone rule.
    deeper: Vec<SstMeta>,
    snapshot: Seq,
}

impl<E: Environment> Shared<E> {
    /// The tables in service, as a manifest, for their levels.
    fn in_service(&self) -> Manifest {
        let tables = lock(&self.tables);
        let mut manifest = tables.manifest.clone();
        manifest.ssts = tables.ssts.iter().map(|(m, _)| m.clone()).collect();
        manifest
    }

    /// The bytes a level may hold before it is compacted: `level_base_bytes` at
    /// level 1, ten times more at each level after.
    fn target_bytes(&self, level: u8) -> u64 {
        self.config
            .level_base_bytes
            .saturating_mul(10u64.saturating_pow(u32::from(level) - 1))
    }

    /// The oldest snapshot version live, or the newest write applied when none is.
    fn smallest_snapshot(&self) -> Seq {
        lock(&self.snapshots)
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| self.visible.load(Ordering::Acquire))
    }

    /// Picks the level furthest over its limit, if any is over it, and the tables
    /// a round on it merges.
    fn pick(&self) -> Option<Plan> {
        let manifest = self.in_service();
        let levels: Vec<Vec<SstMeta>> = (0..LEVELS).map(|l| manifest.level(l as u8)).collect();
        // Each level's fullness as a fraction (over, limit); the bottom never compacts.
        let mut best: Option<(u8, u128, u128)> = None;
        for level in 0..BOTTOM_LEVEL {
            let (over, limit) = if level == 0 {
                (
                    levels[0].len() as u128,
                    u128::from(self.config.l0_trigger.max(1) as u64),
                )
            } else {
                (
                    levels[level as usize]
                        .iter()
                        .map(|t| u128::from(t.bytes))
                        .sum(),
                    u128::from(self.target_bytes(level).max(1)),
                )
            };
            if over < limit {
                continue;
            }
            let fuller = best.is_none_or(|(_, o, l)| over * l > o * limit);
            if fuller {
                best = Some((level, over, limit));
            }
        }
        let (level, _, _) = best?;
        let inputs: Vec<SstMeta> = if level == 0 {
            levels[0].clone()
        } else {
            // The table after where the last round on this level left off, or the
            // first: rounds walk the level and wrap.
            let pointer = lock(&self.compact_pointer)[level as usize].clone();
            let tables = &levels[level as usize];
            let next = pointer
                .and_then(|p| tables.iter().find(|t| t.first_key > p))
                .or_else(|| tables.first())
                .cloned();
            vec![next?]
        };
        let first = inputs.iter().map(|t| &t.first_key).min()?.clone();
        let last = inputs.iter().map(|t| &t.last_key).max()?.clone();
        let next_inputs: Vec<SstMeta> = levels[level as usize + 1]
            .iter()
            .filter(|t| t.overlaps(&first, &last))
            .cloned()
            .collect();
        let deeper: Vec<SstMeta> = levels[level as usize + 2..]
            .iter()
            .flatten()
            .cloned()
            .collect();
        Some(Plan {
            level,
            inputs,
            next_inputs,
            deeper,
            snapshot: self.smallest_snapshot(),
        })
    }

    /// Writes `writer` as the next table at `level`, synced.
    async fn write_output(
        &self,
        writer: SstWriter,
        level: u8,
    ) -> io::Result<(SstMeta, SstReader<FileOf<E>>)> {
        let (first_key, last_key) = writer.key_range().expect("an output has writes");
        let (first_seq, max_seq) = writer.seq_range().expect("an output has writes");
        let entries = writer.entries();
        let bytes = writer.finish();
        let number = self.next_sst.fetch_add(1, Ordering::Relaxed);
        let (reader, len) = self.write_table(number, bytes).await?;
        self.env.trace(TraceEvent::SstWritten {
            number,
            level,
            entries,
            bytes: len,
            first_seq,
            max_seq,
        });
        Ok((
            SstMeta {
                number,
                level,
                first_seq,
                max_seq,
                entries,
                bytes: len,
                first_key,
                last_key,
            },
            reader,
        ))
    }

    /// One round: picks, merges, writes, commits, deletes. `None` when no level is
    /// over its limit. Call it with the turnstile held.
    pub(crate) async fn compact(&self) -> io::Result<Option<Compaction>> {
        let Some(plan) = self.pick() else {
            return Ok(None);
        };
        let output_level = plan.level + 1;
        let removed: Vec<u64> = plan
            .inputs
            .iter()
            .chain(&plan.next_inputs)
            .map(|t| t.number)
            .collect();
        let readers: Vec<Arc<SstReader<FileOf<E>>>> = {
            let tables = lock(&self.tables);
            removed
                .iter()
                .filter_map(|n| {
                    tables
                        .ssts
                        .iter()
                        .find(|(m, _)| m.number == *n)
                        .map(|(_, r)| r.clone())
                })
                .collect()
        };
        let mut merge = MergeIter::new(readers.iter().map(|r| Source::Sst(r.iter())).collect());
        let mut outputs = Vec::new();
        let mut writer = SstWriter::new();
        let mut last_user: Option<Bytes> = None;
        // The number of the previous write of the same user key seen in this merge,
        // dropped or not: what hides the current one.
        let mut prev_seq: Option<Seq> = None;
        let (mut dropped_versions, mut dropped_tombstones) = (0, 0);
        while let Some((key, value)) = merge.next().await? {
            let (user, seq) = ikey::decode(&key)?;
            if last_user.as_ref() != Some(&user) {
                // An output is sealed only between two user keys, so every write of
                // a key is in one table and a read finds the newest where it looks.
                if writer.bytes_so_far() as u64 >= self.config.sst_bytes {
                    outputs.push(self.write_output(writer, output_level).await?);
                    writer = SstWriter::new();
                }
                last_user = Some(user.clone());
                prev_seq = None;
            }
            let drop = match prev_seq {
                // A newer write every live snapshot sees hides this one for good.
                Some(previous) => previous <= plan.snapshot,
                // The newest write of the key: a tombstone goes once every snapshot
                // sees it and nothing older lies deeper than the output level.
                None => {
                    value == Value::Tombstone
                        && seq <= plan.snapshot
                        && !plan.deeper.iter().any(|t| t.contains(&user))
                }
            };
            if drop {
                if prev_seq.is_none() {
                    dropped_tombstones += 1;
                } else {
                    dropped_versions += 1;
                }
            } else {
                writer.add(&user, seq, &value);
            }
            prev_seq = Some(seq);
        }
        if writer.entries() > 0 {
            outputs.push(self.write_output(writer, output_level).await?);
        }
        let added: Vec<SstMeta> = outputs.iter().map(|(m, _)| m.clone()).collect();
        let next = self.manifest_edit(&removed, added.clone());
        if self.config.variant == Variant::DeleteBeforeManifest {
            // The bug: the inputs go before the manifest stops naming them.
            self.delete_tables(&removed).await?;
        }
        self.env.trace(TraceEvent::CompactionWritten {
            level: plan.level,
            manifest: next.number,
            inputs: removed.clone(),
            outputs: added
                .iter()
                .map(|m| (m.number, m.first_key.clone(), m.last_key.clone()))
                .collect(),
            snapshot: plan.snapshot,
            dropped_versions,
            dropped_tombstones,
        });
        self.write_manifest(&next).await?;
        self.install(next, &removed, outputs);
        lock(&self.compact_pointer)[plan.level as usize] = added.last().map(|m| m.last_key.clone());
        if self.config.variant != Variant::DeleteBeforeManifest {
            self.delete_tables(&removed).await?;
        }
        Ok(Some(Compaction {
            level: plan.level,
            inputs: removed,
            outputs: added.iter().map(|m| m.number).collect(),
            dropped_versions,
            dropped_tombstones,
        }))
    }

    /// Deletes tables no manifest names any more and syncs the directory.
    async fn delete_tables(&self, numbers: &[u64]) -> io::Result<()> {
        let fs = self.env.fs();
        for &number in numbers {
            fs.remove_file(&sst_path(&self.config.dir, number)).await?;
            self.env.trace(TraceEvent::SstDeleted { number });
        }
        if !numbers.is_empty() {
            fs.sync_dir(&self.config.dir).await?;
        }
        Ok(())
    }
}

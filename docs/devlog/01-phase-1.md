# Phase 1: a storage engine the simulator kept catching

_September 2026. Tagged v0.2.0; `ananke-storage` 0.2.0 on crates.io._

## What Phase 1 built

An LSM storage engine: a write-ahead log, memtables, SSTables under a manifest, leveled
compaction, snapshots, scans, write batches and checkpoints. Every byte of it is generic
over the `Environment` trait from Phase 0, so the code that runs on a real disk is the
code the simulator crashes. That was the point of Phase 0, and Phase 1 is where it paid.

The deliverable is not the engine. Plenty of LSM engines exist. The deliverable is the
crash sweep: a scenario in which writers put, delete, batch and scan random keys while a
harness crashes the node at random instants with every disk fault the simulator has, and
after each recovery checks the engine's state, key by key and by a scan, against a model
that knows exactly which writes the faults are allowed to have taken. The correct engine
passes ten thousand seeds every night. Three deliberately broken engines fail, each on
its own seeds. That pair is the evidence; either half alone proves nothing.

This post is about what the sweep found on the way, because that is the story. Six
times it found something I had written down as correct and shown to be wrong. Four of
those were in the recovery logic. Two were in the specification.

## How the oracle knows what to forgive

A crash sweep with a lying disk cannot demand that nothing is ever lost. The simulator
drops writes whose `fsync` it lied about, tears the last write, flips a bit in a block
after a crash, and loses a directory entry whose parent was not synced. All of those
lose data, legitimately. The oracle's job is to forgive exactly those losses and no
others.

It does that from the trace. Every state transition in ananke emits a trace event, so
the harness can see which syncs were honoured and which were lost, which blocks rotted,
which writes were torn, and what the engine did in between. A missing record is excused
if the trace shows a fault that explains it: the sync that covered it was lost, or the
record it stopped at was rotted, or the cut of the segment it was in was betrayed. A
missing record with no such fault is a violation. The property the engine must satisfy is
not "loses nothing" but "loses only what the disk took", and the disk's confessions are
in the trace.

That design has a consequence I did not expect: the oracle is as much a piece of
engineering as the engine, and most of the failures below were arguments between the two
about who was right. Sometimes the engine was. Sometimes the oracle was. In two cases
neither was, because the specification had a hole.

## Seed 59: the log had a hole and read clean

The first sweep of the write-ahead log failed at seed 59. The log wrote records framed as
length, checksum and payload, synced each group before acknowledging it, and at recovery
read segments until the first bad checksum or torn record. Seed 59 lost the sync covering
the last two records of a segment, rotated to the next segment whose syncs were honoured,
and crashed. The crash dropped the unsynced tail. Recovery read the short segment to its
clean end and went on to the next: records 1 to 61, then 63 onward, every checksum valid.
A hole, and nothing in the format could see it.

The fix was to the specification. Records now carry their sequence number, the checksum
covers it, and recovery stops at a gap in the numbering. D-019 records it. Every later
bug in this list was found by a check that depends on that number.

## Seed 420: an older write shadowed a newer one

The first nightly run at ten thousand seeds found seed 420. Two writes to one key were
acknowledged in the same group. Each caller applied its own write to the memtable when
it saw the acknowledgement, in whichever order the executor polled them. The newer one
went first, the memtable rotated between the two, and the older one landed in the new
memtable, where a read found it first and returned a value two writes old.

Twenty seeds at the gate and a hundred in CI had not reached that interleaving. Ten
thousand did, on the first night. Writes now apply in sequence order whatever the polling
order (D-021), and seed 420 is pinned in the gate.

## The flipped bit that named an older manifest

`CURRENT` is a one-line file naming the manifest in force. Bit rot turned the `000007` in
it into `000003`, the name of a manifest that still existed. Recovery took that manifest
as the one in force and removed four newer tables as orphans, which is what recovery is
supposed to do with tables no manifest lists. Every step was correct and the result was
the loss of four tables to one flipped bit.

`CURRENT` now carries the crc32c of the name it holds, and a `CURRENT` that fails it
counts as unreadable. The lesson is older than this project: a pointer that can be
corrupted into another valid pointer needs a checksum, however short it is.

## Seed 191: a cut that came back

When the log's head is missing, because the segments holding it were deleted after a
flush and the manifest that recorded the flush was then lost, the engine now refuses to
replay past the gap: a state with a hole in it is one that never existed. With an option
it discards the log instead. My first version discarded by cutting the first segment to
nothing. Seed 191 lost the sync of that cut. At the next crash the old records came back,
in front of the new ones, numbered as if they were current, and a record "came back
changed".

A cut to nothing whose sync the disk lied about is worse than no cut at all. The discard
now removes the segment files, which the directory sync makes durable, and never reuses
their numbers. D-022 records the rule and the seed.

## Seed 44: falling back onto tables that were gone

Compaction landed and the sweep ran three thousand seeds. At seed 44, `CURRENT` and the
two newest manifests were all damaged at one crash. Recovery fell back to the newest
readable manifest, as designed. That manifest listed tables a later compaction had
deleted, legitimately, after the newer manifest was switched to. The store came back
empty.

Every step was excused by a fault, so the oracle passed the epoch, and the state check
was what failed: forty-eight keys that the model said were there and the engine said were
not. The fix was to the rule: a store whose `CURRENT` or manifest cannot be read is
refused, and with fallback allowed recovery uses only an older manifest whose every table
is on disk and intact, never one that lists a missing table. That
is the same principle as the missing log head. A rollback onto a manifest whose tables
are gone is a state that never existed.

## Seed 7218: a deletion the crash undid

A segment was deleted after a flush, and the crash lost the directory entry for the
deletion. The segment was back. Its unsynced tail was gone, so the log read a gap at the
next segment's first record and stopped there, discarding everything after: the right
thing to do. The oracle, though, had been told that a deleted segment explains nothing,
because the tables owed its records; and it kept saying so about a segment the crash had
brought back. The oracle was wrong. It now reads the lost-entry event and lets the
segment's lost syncs explain the loss again.

That one is in the list because it is the shape of most of the disagreements: the trace
had the fact, the oracle had a rule that was true until a fault made it false, and the
sweep found the seed where it was false.

## The oracle is code too

Two of the nightly's last findings were not about the engine at all. At seed 1953 the
manifest `CURRENT` named was still being written when the crash came: whole on disk,
never synced, so the trace had no record of it being written, and recovery rightly
passed it over. The oracle had no notion of a write in flight and called the fallback
unexplained. At seed 6771 a manifest number that a fallback had abandoned was written
again, and the trace event from its first life stood in for the file now on disk. In
both cases the engine was right and the oracle was reading the trace naively.

That is the price of an oracle that forgives by reading the trace: it is a second
implementation of the recovery rules, and it has its own bugs. The rule that made these
cheap to fix is the one from Phase 0: every state transition emits an event, so the
oracle's misreading is always a question of which event it should have looked at, and
the seed replays until the answer is found. Both fixes were a few lines. Neither would
have been found by reasoning; both were found by the tenth thousand seed.

## What the sweep is now

The engine scenario runs eight crashes per seed with lost syncs at twenty percent, bit
rot at five percent per block, torn writes, lost directory entries, filesystem latency so
a crash lands inside a flush or a compaction, and crashes between two polls at one
instant. Writers put, delete and batch, one write in four without a sync. Readers scan at
snapshots. A task takes checkpoints, and after each crash every checkpoint a fault did
not touch is opened fresh and checked. The oracle mirrors every flush and compaction from
the trace, so it knows which table holds which write, and a dropped table is excused only
for the write it held.

The gate runs twenty seeds in about twenty seconds. CI runs a hundred. The nightly runs
ten thousand in release, plus a thousand more with level limits small enough for
compaction to reach level 3.

## Phase 1 exit criteria

SPEC §2.8 names three.

- **Property test: random ops and random crash points, recovered state equals the
  model.** `sim/engine.rs` and `sim/wal.rs`, run by `sim/tests/engine.rs` and
  `sim/tests/wal.rs`; the model is the `BTreeMap` fold in `Model::state_after` over the
  writes the faults left. Every known-buggy variant is caught alongside.
- **10k seeds green in CI nightly.** The nightly workflow runs `ANANKE_SEEDS=10000` in
  release for every sweep, and the deep-levels run at a thousand:
  [run 33986588539](https://github.com/pchrysostomou/ananke/actions/runs/33986588539)
  on commit `85b78df`, green in 33 minutes.
- **Bench: over 200k writes per second single-threaded on the real environment, as a
  sanity number.** `cargo run --release -p ananke-storage --example bench` on one laptop
  disk: 468 writes/s with a sync per write, 33 523 without, 299 169 in batches of a
  hundred without. The number is only reachable without a sync per write, which is what
  the flag in `write(batch, sync)` is for.

## What is next

Raft, on top of this engine, with the network faults the simulator has had since Phase
0 and message duplication, which lands first. Raft's snapshots will be checkpoints. Its
log will not be this log; it will be a table in this engine, which is why the engine has
scans.

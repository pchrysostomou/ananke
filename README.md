# ananke

ananke is a distributed SQL database written in Rust, built so that every failure it
can have is reproducible. Every source of non-determinism the code touches, disk,
network, clock, randomness and task scheduling, goes through one `Environment` trait.
The same code that runs on real machines runs inside a deterministic simulator that
injects torn writes, lost fsyncs, bit rot, lost directory entries, dropped, duplicated
and delayed messages, partitions, one-way blocks, clock skew and drift, and crashes,
under a seed. A failing seed replays byte for byte on the tree that found it, its trace
opens in a visual studio, and it becomes a regression test that asserts the mechanism it
was pinned for, since a seed's schedule moves when the code does. The simulator's traces
and scheduling policies come from [moirae](https://github.com/pchrysostomou/moirae), a
deterministic-simulation-testing framework with a trace replay studio; ananke is
moirae's largest consumer and moirae is ananke's test harness.

Phase 0 (the runtime and the simulator), Phase 1 (the storage engine) and Phase 2 (Raft)
are released. Everything above them, sharding, transactions and SQL, is design only. The
tables below say exactly where things stand.

## Architecture

```mermaid
flowchart LR
  subgraph node["one ananke node (crates/)"]
    direction TB
    sql["ananke-sql (planned)<br/>parser, planner, executor"] --> txn["ananke-txn (planned)<br/>MVCC, transactions"]
    txn --> shard["ananke-shard<br/>ranges, multi-raft"]
    shard --> raft["ananke-raft<br/>consensus"]
    raft --> storage["ananke-storage<br/>WAL, memtable, SSTables"]
    storage --> env["ananke-env<br/>Environment: clock, fs, net, rng, spawn"]
  end
  env --> real["RealEnv<br/>tokio, std::fs, OS clock"]
  env --> sim["SimEnv<br/>virtual clock, in-memory disk and network, fault injection, seeded scheduler"]
  sim --> trace["trace.jsonl"]
  trace --> studio["moirae studio"]
  sched["moirae-sched<br/>PCG32 streams, PCT scheduling"] --> sim
  sim --> wtrace["moirae-trace<br/>format v2 writer"]
  wtrace --> trace
```

The design of each layer is a section of the [SPEC](docs/SPEC.md); Raft's is
[RAFT.md](docs/RAFT.md).

## How simulation works

Every crate is generic over `E: Environment`. Nothing else in the workspace may call
`std::time`, `std::fs`, `std::net`, tokio's I/O or timers, `rand`, or spawn a thread:
clippy's `disallowed-methods` list in `clippy.toml` and a textual second check,
`scripts/check-direct-io.sh`, enforce it. Time is `ananke_env::Instant`; hash maps are
seeded from the environment's random stream, so iteration order is part of the seed.

Under `Sim`, a scenario adds nodes, spawns their tasks on `sim.env(node)`, and drives
virtual time with `run_for`, `run_steps`, `crash`, `restart`, `partition` and `heal`.
The disk model (SPEC §1.3): a write is visible at once and durable only after `sync`;
`sync` returns Ok but persists nothing with probability `1 - p_durable`; at a crash a
random prefix of the pending writes survives, the next may survive as a torn prefix,
one bit per block flips with probability `p_bitrot`, and a random prefix of each
directory's unsynced creates, removes and renames survives. The network model (SPEC
§1.4): drops, duplicates with a delay of their own, delays, symmetric partitions and
one-way blocks. The scheduler picks which runnable task to poll next, uniformly or with
probabilistic concurrency testing, per seed.

Every state transition that matters is a trace event. This is what the echo scenario's
seed 42 records when node 3 crashes at 1.1 seconds, from `sim/out/echo-42.jsonl`, the
trace whose hash `sim/tests/echo.rs` pins:

```jsonl
{"t":1100000000,"seq":969,"kind":"log","node":3,"event":"ananke.fs.bit-rot","data":{"path":"/echo/journal.prev","block":0,"offset":212,"bit":4}}
{"t":1100000000,"seq":970,"kind":"log","node":3,"event":"ananke.fs.write-torn","data":{"path":"/echo/journal","offset":96,"written":16,"kept":14}}
{"t":1100000000,"seq":971,"kind":"log","node":3,"event":"ananke.fs.dir-entry-lost","data":{"dir":"/echo","entry":"/echo/journal.prev","op":"rename"}}
{"t":1100000000,"seq":972,"kind":"log","node":3,"event":"ananke.fs.dir-entry-lost","data":{"dir":"/echo","entry":"/echo/journal","op":"link"}}
{"t":1100000000,"seq":977,"kind":"fault","fault":"crash","node":3,"cause":"schedule"}
{"t":1100000000,"seq":978,"kind":"fault","fault":"restart","node":3}
```

One bit of the node's rotated journal flipped, the last record of its current journal
lost two of its sixteen bytes, and the rename and create of its last rotation were never
synced and did not survive. In the Raft scenario a record can also carry `decidedNs`,
the time the step that produced it was decided, where that came before what it reports
was durable (D-047).

Every fault-model test runs a pair: a known-buggy variant the sweep must catch and the
correct code it must pass, under the same seeds. The write-ahead log carries three
deliberate bugs, the engine three, and the Raft core fourteen, thirteen of them with a
sweep of their own ([RAFT.md §5](docs/RAFT.md)); each sweep prints its catch rates.

## Status

| Component | State today | Version, tag |
|---|---|---|
| `ananke-env` | Released. The `Environment` trait (`Clock`, `FileSystem` with explicit `sync` and `sync_dir`, a message-oriented `Network`, `Rng`, `spawn`); `RealEnv` on tokio; `Sim` / `SimEnv` with the §1.3 disk and §1.4 network fault models, per-node clock skew and drift, and a poll budget; the moirae format v2 export. New in 0.3.0: message duplication, the Raft trace events, and a record's decision time beside its durability time (D-047) | 0.1.0 (`v0.1.0`), 0.2.0 (`v0.2.0`) and 0.3.0 (`v0.3.0`) on crates.io |
| `ananke-storage` | Released. The WAL, memtable, SSTables under a manifest, log truncation, versions, snapshots, scans, leveled compaction, write batches, unsynced writes and checkpoints (D-018 to D-024). New in 0.3.0: the WAL record header carries its own checksum, a format change (D-027), and a refused engine does no work (D-044) | 0.2.0 (`v0.2.0`) and 0.3.0 (`v0.3.0`) on crates.io |
| `ananke-raft` | Released. A pure protocol core with pre-vote, read-index and lease reads with a drift guard, check quorum, leadership transfer, joint-consensus membership changes with learners, and snapshots streamed as resumable chunks of an engine checkpoint; its state in the storage engine; the server as `raft`, `net`, `apply` and `snapshot` tasks; a single-shard key-value store; lost-state refusal and re-seeding (D-025 to D-049). It runs under the simulator; no binary runs it on real sockets yet | 0.3.0 (`v0.3.0`) on crates.io, its first version |
| `ananke-server` | The node binary, Phase 0 protocol only: `ananke-server echo` runs the echo protocol on `RealEnv`, the same code the simulator runs, with a checksummed journal | `publish = false` |
| `ananke-sim` (`sim/`) | The scenarios `echo`, `wal`, `engine`, `raft` and `membership`, the linearizability checker `lin.rs`, and the parallel sweep driver (D-040) | `publish = false` |
| `ananke` | A placeholder reserving the name | 0.1.0, 0.2.0 and 0.3.0 on crates.io |
| `ananke-shard` | Phase 3, begun. The node's wire: the batch frame, which carries several messages each tagged with its 8-byte range id and wraps the Raft codec unchanged; the per-peer outbox, one frame per peer per flush cut under `MAX_FRAME_LEN`; the node's inbox, bounded in bytes with constant-time admission; the studio decoder that shows every message of a frame. The node's tasks, descriptors, split, merge and the rebalancer are not built | None; not released |
| `ananke-txn`, `ananke-sql` | Planned for Phases 4 and 5. No code exists | None |

## Phases

| Phase | Name | Exit criteria, in brief ([SPEC](docs/SPEC.md)) | State |
|---|---|---|---|
| 0 | Deterministic runtime (§1) | The echo protocol under partitions with its trace open in the studio; byte-identical traces for one seed, checked in CI; clippy's `disallowed-methods` for all direct I/O | Released, `v0.1.0`. [Devlog](docs/devlog/00-phase-0.md) |
| 1 | Storage engine (§2) | Random operations and crash points recover to the model's state; 10k seeds green nightly; over 200k writes/s single-threaded as a sanity number | Released, `v0.2.0`. Nightly run 33986588539 on `85b78df`; 299 169 writes/s in unsynced batches of a hundred. [Devlog](docs/devlog/01-phase-1.md) |
| 2 | Raft (§3) | The five invariants across 10k seeds under the network and disk fault model; 3 → 5 → 3 under partition completes both ways with no gap in completed client operations over ten maximum election timeouts; a devlog post showing a real bug and its trace | Released, `v0.3.0`. Green at 10 000 seeds on `cd411b4` (nightly run 34839613587) and, with D-049's check-quorum rule, on `a8656e8` (run 34852980174), the tree `main` had before the release commit; the disk honours `fsync` (D-026; lost syncs are issue #23); worst membership gap 549.359683 ms against the 2 s bound. [Devlog](docs/devlog/02-phase-2.md) |
| 3 | Multi-raft sharding (§4) | Linearizability across split, merge and rebalance under faults; a 1000-range cluster stays balanced within 10% after node add and remove | Planned |
| 4 | Transactions (§5) | elle reports no snapshot-isolation anomalies over simulation traces; an injected bug is caught within 100 seeds | Planned |
| 5 | SQL (§6) | A sqllogictest subset passes; `psql` connects and runs the demo schema | Planned |
| 6 | Security (§7) | A threat model reviewed against STRIDE; no plaintext user data recoverable from a dumped node disk; the audit chain verifies and a tampered entry is detected | Planned |
| 7 | External verification (§8) | No exit criteria stated; the section names a Jepsen suite against a real five-node deployment, fuzzing targets and a 24-hour chaos simulation | Planned |

## Bugs the simulator has found

### Phase 1: the storage engine ([devlog](docs/devlog/01-phase-1.md))

- **A hole in the log after a lost fsync** (seed 59, [D-019](docs/DECISIONS.md)). The
  sync covering the last two records of a segment was lost, the log rotated, and the
  crash dropped the tail. Recovery read records 1 to 61, then 63 onward, every checksum
  valid. Records now carry their sequence number and recovery stops at a gap.
- **An older write shadowed a newer one** (seed 420, [D-021](docs/DECISIONS.md)). Two
  writes to one key acknowledged in one group were applied newer-first, the memtable
  rotated between them, and a read returned a value two writes old. Found by the first
  nightly run; writes now apply in sequence order.
- **A flipped bit that named an older manifest** ([D-022](docs/DECISIONS.md)). Bit rot
  turned `000007` in `CURRENT` into `000003`, a manifest that still existed, and
  recovery removed four newer tables as orphans. `CURRENT` now carries a crc32c.
- **A cut that came back** (seed 191, `29f70f8`, D-022). Discarding a log by cutting its
  first segment to nothing lost that cut's sync, and at the next crash the old records
  came back numbered as current. The discard now removes the segment files.
- **A fallback onto deleted tables** (seed 44, [D-022](docs/DECISIONS.md)). `CURRENT`
  and the two newest manifests were damaged at one crash; recovery fell back to a
  manifest whose tables a later compaction had deleted, and the store came back empty.
  Recovery now refuses such a store, or with fallback allowed uses only a manifest whose
  every table is intact.
- **The oracle's own bugs.** The harness's model of what the disk owed hard-coded one
  scenario's directory (`5112a8b`); at seed 7218 it went on treating a segment whose
  deletion the crash had undone as deleted; at seed 1953 it had no notion of a manifest
  write still in flight; and at seed 6771 it took the first write of a reused manifest
  number for the file on disk (`85b78df`). The engine was right each time.

### Phase 2: Raft ([devlog](docs/devlog/02-phase-2.md))

While the sweep was being built:

- **The network wrote twice** (seed 42, [D-026](docs/DECISIONS.md)). A client request
  the network duplicated was proposed twice by the leader and a compare-and-set applied
  twice; the linearizability checker named the key. Leaders now deduplicate by client
  and sequence number while the entry is in the log. On the same seed, a rotted block
  under a record the tables already covered made recovery skip the rest of a segment;
  that stop is now a refusal.
- **A rotted length read as a torn tail** (seed 16, [D-027](docs/DECISIONS.md)). Bit
  rot in a synced record's length looked like a write torn at a crash, and a server's
  applied index went from 81 back to 68. The record header now carries its own checksum.

Found by the tiers above CI's hundred seeds, four real server bugs, three out of the
nightly's ten thousand (run 34496762339) and the fourth out of the thousand-seed
premerge once those were fixed:

- **An adoption that was not crash-safe** (seed 6325, [D-041](docs/DECISIONS.md)). The
  adoption of a staged snapshot install removed the old store before the copies were
  durable and swept a staging `CURRENT` damaged by bit rot as debris, and the emptied
  directory opened as a fresh store: a crash inside the adoption left a voter that
  remembered nothing.
- **Two snapshot-stream bugs** (seed 5909, [D-043](docs/DECISIONS.md)). Re-takes of a
  snapshot at one index wrote one shared directory under a stream still reading it, so
  the stream never completed; and a leader streamed to one follower at a time, so the
  other designated follower waited behind it. No client write completed after the last
  heal.
- **A refusal that did not survive a restart, and a refused engine that kept working**
  (seed 687, [D-044](docs/DECISIONS.md), the thousand-seed premerge). A server refused
  its store for lost state, but the refusal lived only in the process, and its engine
  flushed, rewrote the manifest without the lost table and deleted the log segment that
  held the evidence. The next restart opened clean, a voter with a hole in its state
  machine.

Every other seed the ten thousand failed the correct server on was the checker:

- **Rules the checker lacked.** Seed 164, from a local ten-thousand-seed run on
  `1373601` (the tree of `48e5276`): the timer check read only AppendEntries as contact, and flagged a follower
  receiving InstallSnapshot chunks of its term (D-030's rule; its account is
  [D-048](docs/DECISIONS.md)). Seed 385, from a local run on `f54b468` (`635aea0`): a follower
  campaigned 25 ms past its bound after an install rebuilt its core with a fresh timer,
  which the check did not model ([D-039](docs/DECISIONS.md)). Seed 7381, from nightly
  run 34496762339: the snapshot floor only ever rose, so a server re-seeded from an
  older snapshot was held to its lost store's floor; an installed snapshot now sets it
  exactly (D-030, provenance in D-048).
- **Durability time read as decision time** (seeds 1885 and 2023,
  [D-047](docs/DECISIONS.md)). Nightly run 34711427220 on `14c3e17` failed the correct
  server on both. A RequestVote of term 10 reached server 1 2.530703 ms before an
  isolation began, and the term rise, traced once its persist was durable, was stamped
  48.446 µs inside the window; the pre-vote check read that as a rise while isolated.
  Every record now carries its decision time beside its durability time, and a check
  about why a server acted reads the former. At ten thousand seeds, in runs 34749071877
  and 34769934684, that removed 28 catches and added none: the two correct-server
  failures, and 26 catches that had inflated known-buggy variants' rates
  (`ResetTimerOnAnyRpc` 8, `SharedSnapshotDir` 7, `AdoptionAsBuilt` 5,
  `IgnoreIncarnation` 4, `SnapshotWithoutCurrentLast` 1, `ApplyBeforeCommit` 1).

## How to run

```sh
git clone https://github.com/pchrysostomou/ananke && cd ananke
rustup toolchain install     # the toolchain pinned in rust-toolchain.toml

scripts/gate.sh              # rustfmt, clippy, the direct-I/O check, docs, every test and doctest
scripts/premerge.sh          # every sweep at 1000 seeds in release, catch rates printed
```

`scripts/gate.sh` precedes every commit. The sweeps run seeds `0..ANANKE_SEEDS` in
parallel ([D-040](docs/DECISIONS.md)), in four tiers: 20 at the gate, 100 in CI on pull
requests and pushes to `main`, 1000 under `scripts/premerge.sh` on your machine before a
merge, and 10 000 in the nightly workflow on GitHub, the only place ten thousand run,
which also sweeps the engine at a thousand seeds with levels small enough for compaction
to reach level 3. Any tier can be run by hand:

```sh
ANANKE_SEEDS=500 cargo test --release -p ananke-sim --test engine
cargo test --release -p ananke-sim --test raft membership -- --nocapture   # prints MembershipCoverage
```

A sweep writes each failing seed's trace to `sim/out/<scenario>-<seed>.jsonl`
(`write_trace` in `sim/lib.rs`), and the nightly uploads those as artifacts. A seed runs
the same whichever thread runs it, so to replay one on its own, call its scenario with
it, as the pinned seeds in `sim/tests/raft.rs` do:

```rust
let report = ananke_sim::raft::run(5909, ananke_raft::core::Variant::Correct);
ananke_sim::write_trace("raft-5909", &report.jsonl); // sim/out/raft-5909.jsonl
report.check().unwrap();
```

```sh
cargo test --release -p ananke-sim --test raft seed_5909   # the pins for one seed
cargo test -p ananke-sim --test raft the_seed_42_trace_is_written_for_the_studio
npx moirae replay sim/out/raft-42.jsonl                    # open a trace in the moirae studio
```

The three-process echo cluster on real sockets:

```sh
cargo run -p ananke-server -- echo --listen 127.0.0.1:7001 --peers 127.0.0.1:7002,127.0.0.1:7003 --duration-secs 3
```

## Further reading

- [docs/SPEC.md](docs/SPEC.md): what each phase builds and how.
- [docs/RAFT.md](docs/RAFT.md): the Raft variant, its invariants and how the trace checks each, and the known-buggy variants.
- [docs/DECISIONS.md](docs/DECISIONS.md): why this over that, one entry per decision, never deleted.
- [docs/devlog/](docs/devlog/): one post per phase.
- [docs/BACKLOG.md](docs/BACKLOG.md): where deferred ideas live, which is the issue tracker, by phase.
- [CONTRIBUTING.md](CONTRIBUTING.md): the working agreements, for anyone sending a change.
- [CLAUDE.md](CLAUDE.md): the same agreements as an agent session reads them.
- [moirae](https://github.com/pchrysostomou/moirae): the simulation framework and studio; `moirae-trace` and `moirae-sched` on crates.io.

Licensed under MIT or Apache-2.0, at your option.

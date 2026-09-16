# DECISIONS.md — ananke

Architecture decision log. One entry per decision, newest at the bottom. Format:
**Context → Decision → Alternatives → Consequences**. Never delete an entry; supersede it.

---

## D-001 — Name: `ananke`

**Context.** Needed a name in the same mythological family as moirae, free on crates.io,
and not likely to collide with anything well-known in the database space.

**Decision.** `ananke` (Ἀνάγκη — necessity, mother of the Moirai). Crate `ananke`, repo
`pchrysostomou/ananke`. npm name is taken; irrelevant for now — a future TS client would
be `ananke-client`.

**Alternatives.** `lachesis` (taken on crates.io), `atropos`, `klotho` (free, but the
"child of" relationship to moirae reads better than "sibling").

**Consequences.** Publish placeholder v0.0.1 to crates.io immediately (lesson from
moira → nemea → moirae).

---

## D-002 — Language: Rust

**Context.** moirae is TypeScript. A database needs control over memory layout, fsync
semantics, and predictable latency.

**Decision.** Rust for all of ananke. The moirae bridge is implemented against the
moirae trace *protocol*, not its TypeScript runtime.

**Alternatives.** TypeScript (would run directly inside moirae, but a storage engine in
JS is a toy). Go (good, but no control over allocation; GC pauses fight with
determinism). Zig (immature ecosystem for TLS/crypto).

**Consequences.** Need a Rust implementation of the moirae scheduler core, or a
sim executor that speaks moirae's trace format. Decided: **port the scheduler core to
Rust as `moirae-rs`** (lives in the moirae repo) — this benefits moirae itself and
keeps the trace format canonical. Learning curve on Rust async internals accepted.

---

## D-003 — All I/O through an `Environment` trait

**Context.** DST only works if the code under test cannot bypass the simulator.

**Decision.** Single generic `Environment` trait; every crate is generic over `E:
Environment`. No global runtime. Enforced by clippy `disallowed-methods` and a CI grep.

**Alternatives.** Dependency injection at the struct level per resource (more flexible,
more boilerplate, easy to forget one). `cfg(test)` swaps (not the same code path —
defeats the purpose).

**Consequences.** Generics everywhere; longer compile times; the pattern is exactly what
FoundationDB (Flow) and TigerBeetle do, so precedent is strong.

---

## D-004 — Storage: LSM, not B-tree

**Context.** Workload is Raft log appends + MVCC version writes + compaction-friendly
deletes.

**Decision.** Leveled LSM.

**Alternatives.** B+tree (better point-read latency, much harder crash-safe
implementation in-place; would need copy-on-write which loses the advantage). Use
RocksDB (would remove the biggest learning payoff and is not simulatable).

**Consequences.** Read amplification; mitigated by bloom filters and block cache.
Compaction scheduling becomes a tunable we own.

---

## D-005 — Consensus: Raft with joint consensus, pre-vote, lease reads

**Context.** Raft is the best-documented option and the one moirae already has notes
for (RAFT.md).

**Decision.** Raft, extended-paper version, joint consensus for membership.

**Alternatives.** Multi-Paxos (fewer papers with implementation detail), EPaxos
(leaderless, much harder, unclear payoff for v1), Viewstamped Replication (TigerBeetle
uses it; attractive but less familiar to reviewers/interviewers).

**Consequences.** Leader-based; cross-region latency not a v1 concern. Joint consensus
is harder than single-server changes — chosen deliberately so the simulator has
something meaty to break.

---

## D-006 — Transactions: Percolator over 2PC-with-coordinator

**Context.** Need cross-shard atomic commit without a single point of failure.

**Decision.** Percolator-style (primary lock, lazy secondary resolution), as TiKV.

**Alternatives.** Classic 2PC with a replicated coordinator (Spanner-like; more moving
parts, coordinator recovery is its own project). Calvin/deterministic scheduling
(elegant, but requires a sequencer layer and changes the whole execution model).

**Consequences.** Locks live in data; readers may need to resolve locks (latency
tail). Lock TTL is a correctness-relevant tunable, which is exactly the kind of thing
the sim should attack.

---

## D-007 — Timestamps: HLC + oracle range in v1

**Context.** SI needs a total order on commit timestamps.

**Decision.** Hybrid logical clocks per node, with a Raft-replicated timestamp oracle
range issuing batched timestamps for the strict-ordering path.

**Alternatives.** Pure HLC with uncertainty windows (Cockroach) — requires
read-restart logic; deferred. TrueTime — no hardware.

**Consequences.** Oracle is a throughput ceiling; acceptable for v1. Revisit in a
future D-entry when moving to uncertainty-based commit.

---

## D-008 — SQL parser: hand-written

**Context.** Could use `sqlparser-rs`.

**Decision.** Hand-written recursive descent for the ananke subset.

**Alternatives.** `sqlparser-rs` (broad grammar, but we'd support 5% of it and the AST
is shaped for Postgres compatibility we don't want yet).

**Consequences.** Own the grammar and errors; fuzz target for Phase 7. More code, all
of it understood.

---

## D-009 — Wire protocol: Postgres

**Context.** Want existing drivers and `psql` to just work.

**Decision.** Implement the Postgres v3 wire protocol (simple + extended query), SCRAM
auth, TLS.

**Alternatives.** Custom gRPC API (simpler, zero ecosystem). MySQL protocol (less
clean).

**Consequences.** Some type-encoding fidelity work; worth it for demo-ability.

---

## D-010 — Security designed in from Phase 0, implemented in Phase 6

**Context.** Retrofitting tenant isolation or encryption into a key layout is a
rewrite.

**Decision.** Tenant id is the first component of every key from day one. Node
identity and KMS are traits from Phase 0 with `Insecure*` dev implementations.
Real implementations in Phase 6.

**Alternatives.** Do security last, properly (rewrite risk). Do it first (delays
everything interesting).

**Consequences.** Slight overhead early; a coherent security design doc becomes a
natural MSc-adjacent artefact.

---

## D-011 — Definition of "phase done"

**Context.** Solo long project; risk of perpetual 80%.

**Decision.** A phase is done only when: exit criteria in SPEC.md pass in CI, the
workspace is tagged and published, and a devlog post is written. No exceptions.

**Consequences.** Some phases will feel "unpolished" at tag time. That is the point.

---

## D-012 — Fault model scope: crash-stop, not Byzantine

**Context.** Byzantine tolerance would change consensus, storage, and networking
fundamentally.

**Decision.** Crash-stop nodes, authenticated links, honest-but-faulty disks.

**Consequences.** Security threat model (Phase 6) explicitly excludes a compromised
node acting maliciously inside the cluster. Documented, not hidden.

---

## D-013 — Clock types are ananke-owned, not `std::time`

**Context.** SPEC §1.2 had `Clock::now() -> Instant` and `Clock::wall() -> SystemTime`.
`std::time::Instant` has no public constructor, so a simulated clock cannot produce one
except by adding a virtual offset to a real anchor, and `Instant::elapsed` /
`SystemTime::elapsed` read the real clock behind the abstraction. `SystemTime` has the
same problems.

**Decision.** `ananke-env` defines `Instant` (monotonic nanoseconds since an arbitrary
per-node epoch) and `WallTime` (nanoseconds since the Unix epoch). Both are plain `Copy`
integers with `Duration` arithmetic; `std::time::Duration` stays the duration type.
Under `RealEnv`, `Instant` is measured from a process-start anchor and `WallTime` from
`SystemTime::now()`. Under `SimEnv`, both come from the virtual clock with per-node skew
and drift. Conversions to and from `std::time` exist only inside `ananke-env`, for the
process edges (certificate validity in Phase 6, log timestamps). `std::time::Instant`
and `std::time::SystemTime` are banned as types outside `ananke-env` via clippy
`disallowed-types`.

**Alternatives.** Reuse `std::time::Instant` and fake it under simulation (anchor plus
virtual offset): `elapsed()` silently bypasses the simulator and any comparison against
a real `Instant::now()` goes undetected. `tokio::time::Instant`: same, plus a tokio
dependency in every crate.

**Consequences.** One conversion at each process edge. The `elapsed()` hole is closed by
construction rather than by lint. Trace timestamps are integers, which serialise
trivially to moirae's format.

---

## D-014 — Hash maps are seeded from `Environment::rng()`

**Context.** `std::collections::HashMap` and `HashSet` seed their hasher from OS entropy
per map, so iteration order differs between runs. Any trace event emitted while
iterating one breaks byte-identical replay (SPEC §1.5). The default states of
`hashbrown`, `ahash` and `foldhash` have the same property.

**Decision.** Those types are banned outside `ananke-env` via clippy `disallowed-types`.
`BTreeMap` / `BTreeSet` are the default. Where hashing performance matters, `ananke-env`
exports `DetHashMap` / `DetHashSet`: the `std` maps with a SipHash-1-3 state (the same
algorithm `std` uses) whose two keys are drawn from `Environment::rng()` at map
construction. The seed is never a compile-time constant. Under `SimEnv` the rng is
seeded, so iteration order is a function of the run seed; under `RealEnv` it is OS
entropy, so HashDoS resistance is identical to `std`'s default and nothing is deferred
to Phase 6.

**Alternatives.** Rely on review (people forget). Ban only `RandomState` (misses
`HashMap::new()` and `collect()`, which pick it implicitly). A fixed compile-time seed
(deterministic, but every deployment shares one key: a HashDoS vector).

**Consequences.** Constructing a hash map needs an `&impl Rng` in hand, which nudges code
towards `BTreeMap` unless it has a reason. Iteration order still varies *across* seeds,
which is the point: a bug that depends on map order shows up as a seed-dependent failure
instead of a heisenbug.

---

## D-015 — Network is message-oriented, and `send` never blocks

**Context.** SPEC §1.4 lists per-link drop, duplicate, reorder and delay, which only
exist for messages, while the BOOTSTRAP_PROMPT.md status line said "simulated TCP pair",
a byte stream. Raft, the first real consumer, is a message protocol. The client wire
protocol (Phase 5) is a stream.

**Decision.** `Network::bind(addr)` yields a `Socket`. A socket carries unreliable,
unordered, at-most-once datagrams between `std::net::SocketAddr`s: `send(to, msg)` and
`recv() -> (from, msg)`, where `from` is the bound address of the sending socket.
Reliability is the protocol's job.

The send semantics are part of this decision because Raft's liveness depends on them:
`send` is enqueue-and-return. It never awaits a connect, a slow peer, or a full socket
buffer. Each destination has a bounded queue; on overflow the oldest queued frame is
dropped and a `MessageDropped` trace event is emitted. Connecting and reconnecting
happen in a background task owned by the socket. A dead or slow peer must never stall
the caller.

Frames are capped at `MAX_FRAME_LEN` (16 MiB), one constant in `ananke-env` that Phase 2
snapshot chunking reads.

`RealEnv`: one TCP connection per destination, connected lazily on first send, a hello
frame carrying the sender's bound address, then length-prefixed frames; reconnect with
bounded backoff. Frames in flight on a broken connection are lost, never retransmitted.
TLS (Phase 6) sits under the framing. `SimEnv`: synthetic addresses and a delivery
queue with the §1.4 faults.

**Alternatives.** Stream-only (FoundationDB's Flow model): the simulator must model
partial delivery, backpressure and bandwidth caps at byte level, and drop, duplicate and
reorder cannot be expressed, so Raft's stale-message paths only get exercised through
reconnects. Both APIs from day one: more surface than Phase 0 needs. A blocking `send`
with backpressure: simpler, but a partitioned follower would stall the leader's
heartbeat loop.

**Consequences.** Every node-to-node protocol is written against loss and reorder from
the start; the Phase 0 echo scenario is "send ping, expect pong or time out".
`SocketAddr` as peer identity is a Phase 0–5 simplification: under mTLS in Phase 6 the
authenticated identity comes from the certificate, and `recv` will need to return a
peer handle rather than a bare address. Not solved now; recorded so it is not forgotten.
The reconnect path is real-only and invisible to the simulator, so it stays as small and
boring as possible, and a `RealEnv` integration test kills the connection mid-traffic
and asserts that the pair recovers and that frames in flight were lost, not duplicated.
Stream-oriented client connections get a separate listener API when Phase 5 needs it.

---

## D-016 — Scheduling policy: a hybrid of uniform random and PCT, chosen per seed

**Context.** In a discrete-event simulator a scheduling choice exists only when several
tasks are runnable at the same instant; most message reordering comes from the network
delay draws. Uniform random selection is fair, so liveness holds, but the chance of
producing one specific ordering of d events among k polls decays exponentially in d.
PCT — Burckhardt, Kothari, Musuvathi and Nagarakatte, "A Randomized Scheduler with
Probabilistic Guarantees of Finding Bugs", ASPLOS 2010 — gives every task a random
priority, always runs the highest runnable one, and lowers the running task at d−1
change points; it finds any bug of depth d with probability at least 1/(n·k^(d−1)) per
run, and is unfair by construction. PCTCP — Ozkan, Majumdar, Niksic, Tabaei Befrouei
and Weissenbacher, "Randomized Testing of Distributed Systems with Probabilistic
Guarantees", OOPSLA 2018 — carries the idea to message passing. Deligiannis et al.,
"Uncovering Bugs in Distributed Storage Systems during Testing (Not in Production!)",
FAST 2016, found bugs with a mix that neither policy found alone.

**Decision.** The policy is chosen per run from the seed by `moirae_sched::Policy::for_seed`:
half the seeds run uniform random, the rest PCT with depth 2 to 4. The choice is
recorded in the trace header. Both live behind `moirae_sched::Scheduler`; the executor
asks `choose(runnable)` and never knows which. PCT draws priorities at spawn, serves the
runnable set by priority, and places change points as a geometric process over polls at
rate (d−1)/hint, where the hint is an estimate of the run's total polls that
`SimConfig::run_length_hint` supplies from the scenario's duration and node count, never
from a poll budget. Unfairness is bounded to one instant because virtual time advances
only when nothing is runnable, and the poll budget (BACKLOG, Phase 1 gate) turns a task
that keeps itself runnable into a failing test. Liveness invariants are asserted only on
uniform seeds. `race` stays a fair coin under both policies, drawn from the node's
`n/sched` substream (D-017), the same stream `moirae_sched::Scheduler::coin` reads; a
biased coin would reintroduce the starvation removed in Phase 0 step 5.

**Alternatives.** Uniform only (misses deep bugs). PCT only (no liveness testing).
Choosing the policy by configuration rather than by seed (a fuzz campaign would need two
configs and two golden traces per scenario).

**Consequences.** Trace size does not depend on the policy, since one `TaskPolled` line
is emitted per poll either way; PCT's replay input is tiny. Interleaving power grows
with task count, so the payoff arrives with Raft's per-peer tasks in Phase 2. A Replay
scheduler that feeds recorded decisions back is v2 of the bridge.

---

## D-017 — Named RNG substreams; `Environment::sched_rng`; `race` takes the environment

**Context.** Task selection and fault draws shared one generator, so a scheduler change
reshuffled which messages dropped; and `race` drew from the protocol stream, so adding
one poll changed which peer a node pinged. This entry must outlive any change to D-016:
whatever the scheduling policy becomes, protocol-visible randomness must not move.

**Decision.** Every stream is derived from the seed by name through
`moirae_sched::stream(seed, label)` — PCG32 seeded by FNV-1a over `"{seed}/{label}"`,
exactly as moirae's engine derives its own streams: `sched` for task selection, `net`
for drops and delays, `fs` for lost fsyncs and torn writes, `clock` for skew and drift,
and per node `n{id}/protocol` and `n{id}/sched`. `Environment::rng()` is the node's
protocol stream and seeds `DetHashMap` (D-014). `Environment::sched_rng()` is the node's
scheduling stream; `race` takes `&impl Environment` and draws its poll order from it, so
a caller cannot pick the wrong stream. Under `RealEnv` both are OS entropy. ananke's
in-crate xoshiro is gone; the generator is `moirae-sched`'s PCG32.

**Alternatives.** One stream (the status quo). Per-node streams only (fault draws would
still depend on scheduling order).

**Consequences.** Adding a poll, a race, or a whole policy changes only `sched` and
`n/sched` draws, never which messages drop or which peer a node pings. Every existing
trace changed once; nothing was pinned yet.

---

## D-018 — WAL: record framing, one writer task, recovery's cut, and what the crash sweep may excuse

**Context.** SPEC §2.2 fixes the record shape, group commit and the stop rule, and
leaves open the checksum's scope and source, segment naming and rotation, how the
writer is structured, what recovery does with a bad tail, and what "recovered equals
the committed prefix" (§2.8) can mean under lost fsync and bit rot, which no
single-disk log survives.

**Decision.** A record is `len: u32 LE | crc32c: u32 LE | payload`, the CRC-32C over
the length bytes and the payload. CRC-32C is implemented in `ananke_storage::crc32c`,
table driven and checked against the standard check value, with no dependency.
Segments are `<n>.wal` with `n` zero-padded to six digits so listings sort in log
order, numbered from 1, created with `create_new` and `sync_dir`, and rotated before
a group that would start at or beyond `segment_bytes`. Each `Wal` has one writer task
spawned through `Environment::spawn`; `append` enqueues, assigns the sequence number,
and returns a future; the writer takes everything queued as one group, writes it with
one `write_at`, syncs once and acknowledges the group, which is group commit. Recovery
reads segments in order and stops at the first torn record, bad checksum or missing
segment; the stopping segment is cut to its last good record and synced, later
segments are removed, the directory is synced, and a fresh segment is opened. Four
trace events say what happened: `WalSegmentOpened`, `WalSynced` (one sync covering
`first..up_to` of a segment, recorded after the call so a preceding `FsyncLost` says it
lied), `WalTruncated` (recovery's cut, likewise) and `WalRecovered`.

The crash sweep's oracle (`sim/wal.rs`) is built from the appenders' acknowledgements
and the trace, never from the disk. A record that was acknowledged and covered by a
sync the simulator honoured must be recovered. A record covered only by syncs the
simulator lost is excused, and so is everything after it. A record acknowledged with
no sync attempt at all is the bug, never excused. A stop is explained, and ends the
obligation, when a `BlockRotted` falls inside the record it stopped on, or when it
lands exactly where a `WalTruncated` whose sync was lost had cut. Nothing else is
excused: a torn write or a lost directory entry never touches a correctly synced
record, so either reaching one is a bug. The log ships in four `Variant`s: `Correct`,
`NoSyncDir` (rotation without `sync_dir`), `NoChecksum` (recovery trusts the length),
`AckBeforeSync` (acknowledge on write, sync on an interval); the sweep must pass the
first and catch each of the others (CLAUDE.md).

**Alternatives.** The `crc32c` crate (SSE4.2): faster, but a dependency for twenty
lines; revisit if a profile shows it. CRC over the payload only as the checksum bug:
a flipped length changes the payload the reader checksums, so the sim cannot tell it
from the correct scope, and a bug the sweep cannot catch is not a control. Reading on
into the next segment after a bad tail: would produce holes. Strict-mode sweeps only:
would never exercise lost fsync.

**Consequences.** Recovery reads whole segments into memory (BACKLOG: streaming). A
lost fsync of the cut can make the next recovery discard a good later segment; the
sweep excuses exactly that and the devlog should explain it. The `Variant` enum is in
the public API with `Correct` as the default. `Environment` gained `Clone` as a
supertrait, which its doc comment had promised all along.

---

## D-019 — WAL records carry their sequence number; recovery stops at a gap

**Context.** The first crash sweep of the D-018 log failed on the correct variant, seed
59: the sync covering the last two records of a segment was lost, the segment was
rotated, the next segment's syncs were honoured, and the crash dropped the pending
tail. Recovery read the shortened segment to its clean end, went on to the intact
next segment, and returned a log with a hole in it: records 1 to 61, then 63 onward,
every checksum valid. "First CRC failure or torn record" (SPEC §2.2) cannot see a hole
whose edges are both well-formed, and a hole is worse than a short log: the memtable
would replay 63 without 62.

**Decision.** The record header is `len: u32 LE | crc32c: u32 LE | seq: u64 LE`, the
checksum covering all three fields and the payload, and recovery expects each record
to carry the number after the previous one, starting at 1. A record that does not
stops recovery with `WalStopReason::Gap { expected, found }`, treated like any other
stop: cut there, discard what follows. The sweep's oracle needs no change: the record
before the gap was covered only by lost syncs, so it and everything after it are
excused. SPEC §2.2 is amended to say so. The bridge writes any integer above
JavaScript's safe range as a decimal string, because a rotted sequence number read
by the `NoChecksum` variant is still data and the studio must still open the trace.

**Alternatives.** A checksum chained from the previous record's: no extra bytes and
the same detection, but no record can then be verified on its own, and a stop reads
as "bad checksum" rather than "gap". A sealing footer written and synced before
rotation: an extra write per segment, and the footer itself can be lost. Assuming a
sync that returned is durable: exactly the assumption SPEC §1.3 exists to break.

**Consequences.** Sixteen-byte headers. When the engine starts deleting old segments
after a flush, the first surviving record will not be number 1; the manifest must
then carry the first sequence number recovery should expect (BACKLOG). Found in the
sweep's first run, which is the point of the sweep.

---

## D-020 — Memtable and engine: a sequence-guarded skiplist, a flush sink that stands in for SSTables, and what the sweep may not excuse

**Context.** SPEC §2.3 names the skiplist and the rule that an immutable memtable stays
readable until its flush completes, and leaves open how writes acknowledged together
apply in order, where flushed memtables go before SSTables exist, how recovery rebuilds
memtables, and how a sweep can catch an engine that acknowledges before the log when a
lost fsync somewhere earlier would excuse the loss anyway.

**Decision.** The memtable is a `crossbeam-skiplist` map from key bytes to the newest
write, each entry carrying the log sequence number of the write that made it and a
value or a tombstone; `apply` is a `compare_insert` guarded by that number, so two
writes to one key acknowledged in the same group may be applied by their callers in
either order and the number decides. Its level generator is a constant-seeded
xorshift, so a memtable's shape is a function of its inserts. The engine writes one
log record per `put` or `delete`, `tag | key_len | key | value`, applies it to the
active memtable once the log has acknowledged it, and rotates the active memtable
into an immutable queue when it accounts for more than `memtable_bytes`; a flusher
task hands the queue's head to a `FlushSink` and releases it once the sink has it.
Reads consult the active memtable, the immutable ones newest first, then the sink.
`Retain`, the sink until §2.4, keeps flushed memtables in memory; the log is not
truncated until SSTables exist, so recovery replays every record into fresh memtables.
`Variant::NoWalBeforeMemtable` applies and acknowledges before the log has the
record, the bug the sweep must catch.

Two things the sweep needed. `Sim::run_steps` takes one scheduling step at a time,
because `run_until` stops only with nothing runnable, so a crash after it always
found every queue drained; the crash scenarios now run a random handful of steps past
their deadline and crash between two polls. And the oracle gained a property that no
excuse touches: a record acknowledged with no sync attempted before the crash is a
violation whatever recovery returned. Without it the buggy engine was caught on four
seeds in twenty, because a lost fsync earlier in the log, rightly excusing every later
record's absence, also hid the one record nobody had asked the disk about. After each
recovery the engine's state must equal the model folded over exactly the recovered
prefix, key by key, and during the run every read of a key with no write in flight
must return the newest acknowledged write.

**Alternatives.** A `Mutex<BTreeMap>`: simpler, serialises every writer, and the SPEC
names the skiplist. Applying writes in the log writer's acknowledgement path: keeps
order by construction but puts memtable work on the one task every appender waits
for; the sequence guard makes arrival order irrelevant instead. A sorted file as the
sink: the SSTable step by another name. Per-operation I/O latency to widen the window
between a write and its sync, which is where an acknowledge-before-sync bug lives on a
real disk: the principled fix, in BACKLOG; the acknowledged-without-sync property
catches the bug without it.

**Consequences.** One dependency with `unsafe` inside it, none in this crate. Flushed
data lives in memory until SSTables land, so a long run grows without bound: fine for
sweeps, not for anything else. `run_steps` changes no existing trace. The
acknowledged-without-sync property is part of every crash sweep from here on.

---

## D-021 — Engine writes apply in sequence order, not in acknowledgement-poll order

**Context.** D-020 had each caller apply its own write to the active memtable when
it saw the log's acknowledgement, and relied on the memtable's sequence guard to make
the callers' polling order irrelevant. The first nightly sweep (seed 420, epoch 1)
showed the guard is not enough: two writes to one key acknowledged in the same group
were applied newer-first, the active memtable rotated between the two applications,
and the older write went into the new active memtable, where a read found it first
and returned a value two writes old. The guard only orders writes within one
memtable; rotation is what it cannot see.

**Decision.** The engine keeps every appended write in a map by sequence number until
it is applied. When any caller sees its write acknowledged, it applies every pending
write up to its own, oldest first: the log acknowledges in sequence order, so all of
them are durable by then. Applying is thus in sequence order whatever the executor
does, and a rotation can only ever fall between an older and a newer write in that
order. The memtable's guard stays, as a second line. This supersedes D-020's
"applied by whichever task is polled first". Seed 420 is pinned in the gate.

**Alternatives.** Applying in the log writer's acknowledgement path: the same order
by construction, but memtable work on the one task every appender waits for. Keys
versioned by sequence number in the memtable, with reads taking the newest across
all memtables (SPEC §2.6): the LSM answer, and what the SSTable step will bring;
until then the order rule is smaller and does not change the memtable's shape.

**Consequences.** One more lock per write and a map that is empty between groups.
Found by the nightly on its first run, at seed 420: twenty seeds at the gate and a
hundred in CI had not reached the interleaving, which is what ten thousand are for.

**Completed by PROPOSED D-054.** The rule above assumes every write numbered below an
acknowledged one is already in the map. On a runtime with more than one thread it was
not: the log numbered a record before the engine put it in the map, and applying popped
and applied under different locks. D-054's *Applies in order* names both windows; a
record is now numbered and put in the map under the map's lock, and applies are
serialised.

---

## D-022 — SSTables, the manifest, the flush order, log truncation, and what the sweep excuses

**Context.** SPEC §2.1 and §2.4 fix the file layout and the table's parts and leave
open how a flush is made crash-safe, when log segments may go, what recovery does with
files a crash left behind or damaged, and how the crash sweep tells a fault's damage
from a bug's. Before this step the engine kept flushed memtables in memory and never
truncated the log, so a flush had no durability consequence to test.

**Decision.** A table is data blocks sealed near 4 KiB with a crc32c each, keys
prefix-compressed against the previous key and stored whole at a block's start,
tombstones as a value length of `u32::MAX`; a bloom block at ten bits per key with
seven probes; an index block of first keys and block locations; a 48-byte footer with
the offsets, the entry count, the format version, a magic and its own crc. The reader
keeps the index and bloom in memory, reads one block per lookup, and verifies every
block at open. The manifest is one file `MANIFEST-<n>` written whole with a crc,
listing the tables with their sequence ranges, the next table number and
`flushed_seq`; `CURRENT` names it, written as `CURRENT.tmp` and renamed, one line of
the name and the crc32c of the name, and must parse exactly with `n` at least 1 or it
names nothing. Manifests are never modified and older ones are kept.

A flush, in order: write and sync the table; write and sync the next manifest; write
and sync `CURRENT.tmp`, rename it over `CURRENT`, sync the directory; put the table in
service and release the memtable; delete every log segment whose records are all at
or below `flushed_seq`, and sync the directory. A crash before the switch leaves the
old manifest in force and the new files as orphans, which recovery removes; the log
still holds the records.

Recovery reads `CURRENT` and the manifest it names. No `CURRENT` at all is the
empty state, since a switch is what creates it. If `CURRENT` cannot be read, or
names a manifest that cannot be, `Engine::open` fails with an error naming the file,
unless `allow_manifest_fallback` is set: then recovery uses the newest older manifest
whose every table is on disk and passes its checks, never one that lists a missing
or damaged table, reports the fallback, and rewrites `CURRENT` to name the
manifest it chose; with no such manifest it fails as well. The first version fell
back to the newest readable manifest and, at seed 44 of the compaction sweep,
landed on one whose tables a later compaction had deleted, and the store came back
empty: a rollback onto a manifest whose tables are gone is a state that never
existed, refused for the reason a missing log head is. Seed 44 is pinned in both
modes. Every table the manifest in force lists is opened and verified whole; one
that cannot be read is dropped from service and reported with its range. Orphans are
removed. The log is opened expecting its head at `flushed_seq + 1`: a first record
past that is a missing head, reported as a `HeadGap` trace event, and the log is not
replayed past it. `Engine::open` then fails, with an error carrying the gap and
nothing on disk touched, unless `allow_head_gap` is set, which discards the whole log
and keeps the manifest's tables as the state: a clean prefix. A state with a hole in
it is one that never existed, and a store that serves it has lied about every write
in the hole; the records are gone either way, and Raft (Phase 2) is the channel for
re-supplying lost writes from a peer, not a replay that skips over them. A jump in
the numbering that lands at or below the head skips only records the tables hold and
is not a stop; replay applies records past `flushed_seq` only; the next number is
never one below the head. After a recovery that discarded segments, the fresh segment is
numbered past every segment the directory held, discarded ones included: a segment
number is never reused, so the trace names one file per number. This supersedes
D-019's note that the manifest must carry the
first sequence number: the log's records number themselves, the manifest carries
`flushed_seq`, and the engine tells the log what head to expect.

The simulator's filesystem operations now take time (`FsFaults::latency`), so a crash
lands inside a flush as often as between two, and `Variant::ReleaseBeforeManifest`
releases a memtable and deletes its log segments once its table is written and in
service, before the manifest names it: a crash in that window leaves the table an
orphan and its records nowhere. The
sweep excuses exactly these losses: a dropped table whose sync the simulator lost or
which bit rot hit; a fallback whose manifest's sync, or whose `CURRENT.tmp` sync, was
lost or which rot hit, with everything flushed after the manifest used; a head of the
log gone, and the log discarded with it, because every sync of the segment that held
it was lost, or a fault at a covered stop before it took it, or a previous recovery's
cut of the segment it is found in was betrayed by a lost sync, or the fallback that
left the head behind is itself excused; never because the segment was deleted, since
tables owed a deleted segment's records. A record lost that way stays lost in later
epochs unless a log replay brought it back. After a missing head the sweep also
requires that nothing was replayed and that the state equals the manifest's prefix
exactly. Nothing else is excused. The WAL scenario follows the log's numbering rather than assuming it, and
a log that numbers an append other than by position is a violation of its own.

**Alternatives.** A manifest log appended to, as RocksDB keeps one: fewer bytes per
flush and more code; whole rewrites are small while tables are few. Repairing the
manifest at open when a table is dropped: BACKLOG, since a repair that rewrites state
under a fault deserves its own sweep. Lazy verification of tables: BACKLOG, once tables
are large enough for a full read at open to cost something.

**Consequences.** Manifests accumulate until a garbage collector keeps the last few
(BACKLOG). Every open reads every table whole. Found by the sweep during this step: a
torn `CURRENT` parsed as "manifest 0" and recovery, taking the store for fresh,
deleted a durable manifest and its table as orphans; a hole in the log inside the
flushed range stopped recovery and discarded the tail the tables did not cover; an
unreadable `CURRENT` had no fallback at all; and one flipped bit turned `CURRENT`'s
`000007` into `000003`, a manifest that existed, so recovery reverted to it and
deleted four newer tables as orphans, which is why `CURRENT` now carries a checksum.
Found by the 3000-seed sweep of the correct engine: after a recovery discarded
segments, the fresh segment reused the first discarded number, so two unrelated files
had lived under one name and the sweep's oracle, which reads a segment's sync history
by number, could not tell them apart; segment numbers are now monotone, and with holes
in the numbering allowed the "missing segment" stop is gone, since a segment lost with
records in it shows as a gap at the next segment's first record and the numbering of
the records is the check that matters.

---

## D-023 — Versions in the memtable and the tables, snapshots, one merge for reads and compaction, and leveled compaction

**Context.** SPEC §2.5 asks for leveled compaction, crash-safe through the manifest,
and §2.7 for `get` and `scan` at a snapshot version. D-020's memtable kept the newest
write per key and D-022's tables the same, so a snapshot had nothing to read from:
a key overwritten after the snapshot was gone, and a scan could see a key before a
write and its neighbour after it. And the sweep's oracle judged a dropped table by
its sequence range, which is exact only while every table is a flushed memtable.

**Decision.** Keys are internal keys: the user key, escaped so a shorter key sorts
before every longer key it is a prefix of, then `!seq` big-endian, so byte order is
user key ascending and newest write first (`ikey`). The memtable keeps every write
since the last flush under its internal key, and a table holds whatever writes its
flush or compaction gave it, several of one user key allowed; the bloom filter is
over user keys, and the table format is version 2. A read at sequence number `s`
seeks to `(key, s)` and answers with the first entry if it is a write of `key`: the
newest at or below `s`. The engine's `visible` is the highest number applied; a
`Snapshot` pins that number, counted in a map compaction consults for the oldest
version it must keep, and is released on drop. `get` reads at `visible`, `get_at` at
a snapshot, and `scan` merges every memtable and table into one walk in internal-key
order, seeks to the range's start and reports the newest write per user key at or
below the snapshot, tombstones hiding older ones. The merge iterator (`merge`) is
the same one compaction reads its inputs through. This supersedes D-020's newest
write per key and D-021's guard, which the numbering makes unnecessary; writes still
apply in sequence order, which is what makes `visible` mean "everything at or below".

Compaction is leveled, levels 0 to 6, level 1 allowed `level_base_bytes` and each
deeper level ten times more; level 0 is compacted at `l0_trigger` tables. The
manifest carries each table's level, key range and size (format version 2). A round
picks the level furthest over its limit: every table from level 0, one from a deeper
level, the first past where the last round on that level stopped; the next level's
tables that overlap join; one merge writes the outputs into the next level, sealed at
`sst_bytes` and never between two writes of one user key, so a lookup finds every
write of a key in the one table its range names. A write is dropped when a newer
write of its key is at or below the oldest live snapshot; a tombstone that is the
newest write of its key is dropped when it is at or below that snapshot and no table
deeper than the output level holds the key. Deletes thus survive until they reach the
bottom or no older write lies below. The order is the flush's: outputs written and
synced, the manifest written, synced and switched to, the outputs installed, then the
inputs deleted and the directory synced; `Variant::DeleteBeforeManifest` deletes
first, the bug the sweep must catch. The next manifest is built from the tables in
service, so a table dropped at open is not listed again. One flush or compaction
runs at a time, through a turnstile, since both compute their manifest from the one
in force. The flusher runs rounds after each flush until no level is over its limit;
`Engine::compact_once` is the manual trigger. A read consults level 0 newest first
and then the one table per deeper level whose range holds the key.

The sweep's oracle mirrors the trace: a flushed table holds every write in its
sequence range, a compaction's outputs hold what the merge of its inputs kept by the
same rules, split by the key ranges the trace records, and each manifest lists the
tables the trace says. A record is present if a table the manifest in force lists
and the open did not drop holds it, or the log replayed it. Every other record the
tables owed is excused only if a compaction in the manifest's lineage dropped it, a
dropped table held it (its sync lost, bit rot, or deleted by a compaction after a
manifest that an explained fallback then abandoned), a fallback left it in a table
no manifest in force lists, or it was lost before; else it is a violation. The
state check folds the model over present records only, so a version compaction
dropped is skipped and its newer shadow is what counts, as in the engine.

**Alternatives.** Snapshots by version in the user key alone (§2.6): that is the
transaction layer's versioning, above the engine, and it cannot give a consistent
scan across an engine write that lands mid-walk. A `Version` parameter without a
guard, as §2.7 sketches: compaction would have no way to know which versions are
still wanted; the guard carries the version and its lifetime. Materialising a
memtable for a scan: the cursor asks the skiplist for the next entry past the last
instead, so a scan holds no borrow and no copy. Judging dropped tables by sequence
range: exact for flushed tables, wrong once a compaction's siblings share a range,
which is why the oracle mirrors the trace instead. Reading the tables back from the
simulated disk for the oracle: would check the engine against its own files and
miss a merge that keeps the wrong version. Tiered or size-tiered compaction: the
SPEC names leveled, and Raft's snapshots (Phase 2) want one run per level to copy.

**Consequences.** A memtable fills with every write, not every key, and flushes
about twice as often in the sweep. The sweep's readers now scan at snapshots and
compare with the model folded over exactly the ops the version covers, which is
exact rather than tolerant of in-flight writes; and the state check after a recovery
scans the whole space besides reading every key. A compaction rewrites a table it
could have moved down whole (LevelDB's trivial move): BACKLOG. Only the oldest live
snapshot bounds what compaction keeps, so one long-lived snapshot keeps every
version newer than it: BACKLOG. A table dropped at open is forgotten by the next
manifest and its file removed as an orphan at the open after, which is the manifest
repair D-022 left in BACKLOG, done by the same rule that writes every manifest.

---

## D-024 — Write batches, writes without a sync, checkpoints, and the first manifest

**Context.** SPEC §2.7 gives `write(batch, sync)` and `checkpoint(dir)`, and §2.8 a
write rate no log that syncs every write can reach. The engine wrote one record per
put or delete, synced before acknowledging, and had no way to hand a state to Raft.

**Decision.** A `WriteBatch` is one log record under one sequence number: `2 |
count | writes`, a batch of one encoded as that write. Its writes apply together and
become visible together, a later write to a key in it replacing an earlier one, and
a crash keeps all or none. `write(batch, sync)`: with `sync`, the future resolves
once the record is durable, as every write did; without it, once the record is
written, and the next group that asks for a sync, the next rotation or the close
makes it durable. A write without a sync is visible at once and a crash before its
sync loses it, acknowledged and read or not: that is what the caller asked for, and
the sweep's oracle owes it nothing until a later sync covers it, exempting it from
the acknowledged-without-sync property. The model records what a batch leaves in the
memtable, its last write per key, and a record is compacted away only when every
write of it is.

`checkpoint(dir)` writes the state as of the newest write applied into an empty
directory as a store of its own: a copy of every table in service, one table of what
the memtables hold at that version, a manifest listing them and `CURRENT`, each
synced in that order, under the turnstile so no flush or compaction runs meanwhile.
A crash leaves either a complete checkpoint or one without `CURRENT`. So that the
latter is recognisable, every store now writes an empty first manifest and `CURRENT`
at its first open: from then on a missing `CURRENT` with manifests or tables on disk
is refused. The first manifest, which lists no table, is a valid fallback: with the
log replayed after it, what opens is the true state, not an empty one. The sweep
takes checkpoints while it runs, records the model's state at each version, and
after the crash opens each checkpoint fresh, requiring no table dropped, nothing
replayed and every key and a scan equal to that state, unless a fault touched a file
under it.

**Alternatives.** A number per write inside a batch: the log numbers records, and
the memtable's replace-on-insert gives the same last-write-wins. A checkpoint by
hard links, as RocksDB makes them: the simulator's filesystem has no links, and a
copy is what the fault model can tear. A temporary directory renamed into place:
directory renames are not modelled either; `CURRENT` last is the same atomicity.
Treating `sync: false` as a hint and syncing anyway: honest but pointless, since the
flag exists for the rate it buys.

**Consequences.** The bench (SPEC §2.8) measures single writes with and without a
sync and batches without one. A reader can see a write that a crash then takes
back, only ever one that asked for no sync. Every store directory has a `CURRENT`.

---

## D-025 — The Raft core as a pure step function, its state in the engine, and the invariants as folds

**Context.** RAFT.md fixes the variant and the invariants. Stage A of its
implementation order is the protocol without a server: the core, the wire form, the
persistent state, and the paper's scenarios as tests.

**Decision.** `ananke_raft::core::Raft` is Figure 2 with pre-vote (thesis §9.6), a
no-op on election (thesis §6.4), batching and pipelining with moirae's deviation D1,
as a state machine with no I/O: `step(Input) -> Vec<Output>`, inputs being a message,
a tick, a proposal or a completed apply, outputs a persist, sends, an apply, a
rejection or a trace event. Outputs come in an order the server keeps: the persist
first, then the messages that depend on it; that order is the persistence discipline
of Figure 2, and the server variant that sends first is what stage B's sweep must
catch. The core holds the log as a vector and draws election timeouts from a
generator seeded by the server, so it is a function of its inputs and its seed. The
known-buggy cores are variants on it: no pre-vote, counting an older term's entry
for commit, truncating on every append, resetting the timer on any message, and the
election restriction by length first.

The wire form is one frame, `kind | from | term | fields`, little-endian, entries as
`term | index | payload`; a frame that does not decode is a dropped message. The
studio decoder turns a frame into `{"type": "raft.<kind>", "from", "term", ...}` with
the entry range rather than the entries, the field set issue #3 filters by.

The persistent state lives in the engine under tenant 0 with SPEC §2.6's key shape:
the hard state and the applied index under one key in table 0, the log one key per
index in table 1. A persist is one synced batch of the hard state when it changed, the
truncation's deletes and the appends; applying an entry is one synced batch of the
entry's writes under the user tenant and the applied index, so both are durable or
neither is and an entry applies exactly once whatever the crash schedule. Commands
are put, delete and compare-and-set, the last so that a double apply shows as a wrong
boolean. The store refuses an engine whose recovery dropped a table, fell back to an
older manifest or discarded a log head (`LostState`): the engine keeps what it can
and reports the rest, which is right for a store on its own, but under Raft a hole
below the applied index is a state that never existed (D-022), and the channel for
re-supplying it is a snapshot, not a start. The engine is opened with fallback and
head-gap discard off for the same reason.

The four log invariants of Figure 3 are folds over the trace events the core emits
(`RaftTerm`, `RaftVote`, `RaftLeader`, `RaftAppend`, `RaftTruncate`, `RaftCommit`,
`RaftApply`), in `ananke_raft::invariants`, so the paper's scenarios check them over a
few cores stepped by hand and stage B's sweep checks them over a run.

**Alternatives.** raft-rs's `Ready` with an explicit `advance`: the same idea with more
ceremony; ordered outputs say the same thing in one list. A log in its own file: the
engine already gives synced batches, scans and checkpoints, and SPEC §3 puts the log
in it. A core that owns its timers with real time: nothing in the core may touch a
clock, so ticks are inputs.

**Consequences.** The whole log sits in memory in the core until snapshots (stage E)
let it drop a prefix. Every buggy core is shown breaking its rule in
`crates/ananke-raft/tests/paper.rs` and the correct core holding it, before any
simulator run exists. A node whose disk lied about a sync under a flushed table
refuses to start until stage E gives it a snapshot to start from; the store's crash
test (`crates/ananke-raft/tests/store.rs`) shows the refusal on the seeds where the
simulated disk does that, and the exact state on the rest.

---

## D-026 — The server's tasks, the sweep's checks, and what the sweep found

**Context.** Stage B of RAFT.md's order: the server on `Environment`, election and
replication only, a single-shard key-value store, the invariants folded from the
trace, the linearizability checker, and the sweep under the network fault model,
partitions, one-way blocks and crashes with the disk model, with six known-buggy
variants that must each be caught.

**Decision.** One server is three tasks for now, `raft`, `net` and `apply`, under
`Environment::spawn`, joined by queues with no runtime behind them
(`ananke_raft::queue`); the `snapshot` task of RAFT.md §3 arrives with snapshots. The
`raft` task executes a step's outputs in order and awaits the persist before the sends
that follow it; `Variant::SendBeforePersist` sends first, and its trace events still
follow the persist, so the trace says what is durable. `Variant::ApplyBeforeCommit`
hands the `apply` task entries as they are appended. The inbox is bounded and drops
the oldest heartbeat first, never an `AppendEntries` with entries, recording
`RaftInboxDropped`. The hard state and the applied index are separate keys, since
separate tasks write them, and the store is shared behind an `Arc` with atomics for
what each task caches.

The sweep's disk honours `fsync`. A disk that acknowledges a sync it did not do loses
persistent state silently, and Raft's safety argument assumes persistent state is
persistent: no consensus protocol survives a lying disk, and a sweep that excused the
resulting violations would check nothing. Bit rot and torn writes stay on, and every
way the engine's recovery can lose state in the middle, a dropped table, a fallback, a
discarded head, a log stopped at a bad checksum or a gap, a corrupt record skipped in
a segment the tables cover, is a refusal (`LostState`): the server traces
`RaftRefused`, never binds its socket, and takes part in nothing until stage E
re-seeds it.

Gets go through the log until stage C, so every client operation is an entry and the
history is linearizable by construction if the log is right. A client that hears
nothing back about a write does not resend it, since the entry may yet commit; it
abandons the operation and continues as a new process. A get may be retried. Client
sessions (thesis §6.3), which would make a retry safe across leaders, are issue #21.
A leader deduplicates a request the network delivered twice, by client and sequence
number, while the entry is in its log.

The checks are functions of the trace (RAFT.md §2): the four log invariants; three
folds of the rules behind them, commit by majority, commit by current term, committed
entries stay; linearizability by a Wing-Gong search with memoisation and a budget of
states per key, over a history in which the trace closes abandoned operations through
`RaftProposed`; pre-vote's property, that an isolated server's term does not move;
and, on uniformly scheduled seeds only, two liveness bounds, a client write within ten
maximum election timeouts of the last heal and an election within three of a
follower's last legitimate timer reset. The sweep runs small batches so that a
follower a few entries behind is caught up in several messages, which is where the
Figure 8 window lies. Two events exist for the checks alone: `RaftRecovered`, so an
apply durable at a crash but not yet traced is accounted for at the restart, and
`RaftProposed`.

**What the sweep found before it passed.** Seed 42, the first seed run: a client
request the network duplicated was proposed twice by the leader, and a
compare-and-set applied twice; the client was told the second, failing swap while the
first had changed the value, and the checker reported the key. The fix is the
deduplication above. Seed 42 again: a rotted block under a log record the tables
already covered made recovery skip the rest of that segment, acknowledged records
included, and the server came back on a state that went backwards; state machine
safety reported the double apply, and the fix is the covered-stop refusal above. Seed
2: an apply that was durable when the crash landed but never traced, since the crash
fell between the sync completing and the task's next poll; the trace had a gap, the
system was right, and `RaftRecovered` closes it. Seed 38, the same for an append: a
restarting server re-states its durable log before `RaftRecovered`, so the trace's
picture of a log is the disk's. And `CountOlderTermForCommit` was
never caught at the default batch size: a new leader's own no-op is sent in the same
message as the older entries it re-sends, so a follower matches both at once and the
count rule never gets its window. That is a hole in the sweep, not a reason to keep
the variant unexercised: the sweep runs one entry per message and checks the rule
directly; a batched sweep needs a driver for the window (issue #22). Then, with one
entry per message, the correct leader flooded a follower with forty thousand
appends: a rejection reset the follower's pipeline and re-sent up to eight messages,
every other message of the pipeline was rejected in turn, and each of those
rejections re-sent eight more. The leader now probes with one message after a
rejection and ignores a rejection of anything but the probe, which is why
`AppendEntriesResponse` carries the request's `prev_index`. The same seed's follower
matched an entry it already held and reported the index below it, against
deviation D1; the match is now the request's last index. And seed 16 found bit rot in
a record's length reading as a torn tail, and a log cut at one open reading as whole
at the next: D-027.

**Alternatives.** Client sessions now: correct and small, but a state-machine change
with a clock for expiry, better done as its own step (#21). A lying disk with
violations excused by the trace, as Phase 1's sweep did for lost writes: an excused
safety violation is no check. A fourth task now: nothing for it to do until snapshots.
Retrying writes after a timeout: at-least-once without sessions is a second write.

**Consequences.** Every operation costs a log entry, gets included, until stage C. A
refused server stays down for the rest of a run until stage E, and liveness is asked
only while a majority is up. The checker's budget is a knob the correct server must
never hit; a seed that does is a sweep problem to fix, not a pass.

**Superseded in part by D-047.** A trace record's time is no longer read as when
the thing it reports happened. A record carries its decision time, when the step
behind it was taken, beside its durability time, when what it reports was durable:
a check about why a server acted — the pre-vote isolation check, the timer check,
the leader in force at the last heal — reads the decision time, and a check about
what was durable reads the durability time or the records' order. The order of
execution above stands: a step's trace events follow its persist, so the trace
says what is durable.

**Superseded in part by D-030.** The refused server above, which "never binds its
socket, and takes part in nothing" until stage E re-seeds it, is not the server
stage E built. Under D-030 a refused server runs in re-seed mode (RAFT.md §3): it
binds its socket and answers every AppendEntries with a rejection asking from
index 1, the ask on which a leader designates it snapshot-fed (D-037's trigger);
it grants nothing, serves nothing, and takes only its own re-seed, the
`InstallSnapshot` stream it assembles and installs. The refusal stands: every way
of losing state listed above is still a `LostState` refusal traced `RaftRefused`,
and D-044 makes it durable.

---

## D-027 — The WAL record header carries its own checksum

**Context.** The Raft sweep (D-026), seed 16: bit rot flipped a bit in the length
field of a log record that had been synced and acknowledged. Recovery read the bogus
length as running past the end of the file, reported a torn record, kept what came
before, and the server started on a log shorter than what it had promised: its
applied index went from 81 back to 68, which state machine safety reported. A torn
record was the one stop the store did not refuse (D-026), because a write in flight at
a crash leaves one and was never acknowledged; but a flipped bit in a length looks
the same, and the two cannot be told apart from the payload checksum, which cannot be
checked without the payload.

**Decision.** The record is `len | header crc32c | crc32c | seq | payload`: the header
checksum covers the length and the sequence number and is checked before the length
is trusted. A flipped bit anywhere in the header is a bad checksum; a torn record is
one whose bytes are missing, which only a write in flight at a crash leaves. SPEC §2.2
is amended; the WAL has no versioning yet and no released store to migrate, so the
change is the format.

The same seed showed a second thing: recovery cuts the log at a stop before the
caller sees the recovery, so a store refused for a bad checksum at one open found a
whole, shorter log at the next and started. `WalConfig::refuse_damage`, reached
through `EngineConfig::refuse_log_damage`, fails the open with `wal::LogDamaged`
before anything is cut, at a bad checksum, a gap, or a corrupt record skipped in a
covered segment; a torn tail is still cut. A Raft server always opens its engine with
it, fallback off and head-gap discard off, whatever configuration it is handed.

**Alternatives.** Refusing a Raft store on any torn record: a write is in flight at
most crashes, so most restarts would refuse and the cluster would lose its servers
one crash at a time. Trusting a length that runs past the file only when nothing
follows it: with a torn write the file ends at the partial record and with a rotted
length it also appears to, so there is nothing to see.

**Consequences.** Four more bytes per record. The engine's crash sweeps see rot in a
header as a bad checksum from now on, which they excuse like any stop a fault
explains; the Raft store refuses it.

---

## D-028 — Reads, leases, the guard as built, check quorum, the vote rule, and leadership transfer

**Context.** Stage C of RAFT.md's order: read-index reads, lease reads with the
conservative guard, check quorum, `Variant::LeaseTrustsTheClock`, and invariant 6
under the simulator's drift; and, from the stage B review, a stale-sender scenario so
that `ResetTimerOnAnyRpc` is caught at a useful rate.

**Decision.** The core takes the server's clock as a number on the inputs that need
it, a message's arrival and a read's, and stays a function of its inputs. An
AppendEntries carries `sent`, the leader's clock at sending, stamped by the server on
the way out; the response echoes it and adds `local`, the follower's clock, stamped
the same way. `sent` is what a promise runs from and what tells a read-index round
which acknowledgements came after the read; `local − sent` is what the guard watches.
The guard observes each follower through the fastest response of a window, compares
windows with the first, revokes on movement beyond the bound over the time between
them, and trusts a follower only from its first steady comparison; a fresh leader
therefore serves by read-index for two windows, and a spurious revoke at the first
comparison, where jitter and the allowance are of a size, costs a round trip. The
lease is the latest-expiring promise among trusted followers that makes a majority
with the leader; a read the lease covers is served at the commit index once applied,
any other after a heartbeat round a majority acknowledged with requests sent after
it, and neither before the term's no-op commits. A leader that stops leading drops
its pending reads and the client asks elsewhere.

Check quorum runs every minimum election timeout on the leader's ticks over the
responses since the last check. The vote rule behind the lease is etcd's: a follower
that has heard from its leader within its minimum election timeout ignores a vote
request, term included; with pre-vote, a candidate that reached a real election
already has a majority that has not heard, so liveness is kept. Leadership transfer
(thesis §3.10) is the exception, marked on the vote request: `TimeoutNow` from the
leader makes the target campaign without a pre-vote once its log matches, and
followers vote for it although they heard from the old leader, who steps down on the
higher term. An operator asks for a transfer with a client command the server acts on
directly.

The sweep draws every server's clock per seed: within a third of the bound on half
the seeds; on the other half one server slow and two fast, moderately, two thousand
to a hundred thousand parts per million, where the guard must notice movement the
lease's margin still absorbs, or severely, the slow one at a quarter to a third of
true rate and the fast ones a quarter to a third fast, where a lease the slow server
measures by its own clock outlives the fast followers' timers. A slow clock never
leads on its own, its timer fires late in real time and the fast servers win every
election, so every schedule opens with a lease trial: an operator transfers
leadership to the slowest clock, its lease forms, and it is cut off with a
read-heavy client while the others elect and write. The adversary chose the clocks
and chooses the operator's request. The stale sender for rule 5 is a follower cut
off from receiving for a second, pre-voting into the majority every timeout, with the
leader crashed part way; it replaces the stale-leader schedule of D-026, whose
deposed leader now stops within an election timeout under check quorum. The timer
bound is two maximum election timeouts scaled by the server's own clock rate.

**What the sweep found before it passed.** With independent random rate errors the
stale window never opened, since the leader was never the slow server; crashing the
faster servers to hand it the lead lost a server to bit rot on many seeds and still
lost the post-restart election to the faster timers; transfer is the tool. With four
keys and a mixed client on the cut-off side, the window was hit but no write to the
key read had landed in it; two keys, a read-heavy client there, a write that gets one
try of a hundred and twenty milliseconds, a get that tries elsewhere after forty, and
a retry that avoids the server that went quiet are what make the majority side write
into the window. With the lease's arithmetic, a hundred-millisecond election timeout,
a tick of margin and heartbeats that must still arrive within the fast followers'
minimum timeout, the rates that open a window are tens of percent, which no real
clock does; the arithmetic is the same at any scale, and this is where the sweep can
reach it.

**Alternatives.** A clock rate that changes mid-run to make an elected leader slow:
the guard cannot see a change within a window by construction, so the correct server
would serve a stale read for one window and the test would say nothing about the
guard. Sudden drift is a different fault from the one the lease assumes bounded.
Refusing every vote request while leading, without the transfer exception: no
transfer, and no way to test the lease on a slow leader. Reads through the log, as
stage B did: linearizable, and a log entry per read.

**Consequences.** Every AppendEntries and its response carry two more words. A lease
read is served by the `raft` task from the engine directly. The correct server's
lease reads, revocations and check-quorum step-downs are coverage the sweep asserts;
the lease-safety test reports, at every seed count, how many seeds exceeded the
bound and, of those, how many the guard revoked on and how many read stale without
it.

**Superseded in part by D-049.** Check quorum no longer counts every response since
the last check. A refused server's rejection, stamped store incarnation 0, counts
only in a window in which a chunk of the leader's re-seed stream to that server was
acknowledged; every other response counts as above.

---

## D-029 — Joint-consensus membership changes: the configuration in force, learners first, and one change in flight

**Context.** Stage D of RAFT.md's order: membership changes by joint consensus
(thesis §4.3), servers being added catching up as non-voting learners first
(thesis §4.2.1), one change in flight at a time, the `0 / 2 / config` key, the
3 → 5 → 3 scenario under partition with its availability criterion (SPEC §3),
and `Variant::SingleMajorityInJointConsensus`.

**Decision.** A change from C_old to C_new is two entries, `C_old,new` and, once
that is committed, `C_new`, and a server uses the latest configuration entry in
its log, committed or not (RAFT.md §1): the core adopts it on append, leader
and follower alike, reverts on truncation to the latest surviving entry or the
initial configuration, and restores at restart by scanning the log the store
hands back. Every take-effect emits `RaftConfig`; at a restart the node
re-states the configuration in force after the log re-statement and before
`RaftRecovered`, an order that is kept stable. The step's persist carries the
configuration when it changed, and the store writes it under `0 / 2 / config`
in the same synced batch as the append or truncation, checks it against the
log at open, and refuses a store whose key and log disagree.

Counting and addressing were pulled apart, which is where the stage's latent
bug lived: `voters()` used to return every member. Now votes, pre-votes,
commits, check quorum and read-index acknowledgements are counted through
`Configuration::has_majority`, which takes a majority of each voter set in
force and counts only members of each set — so a leader outside C_new
contributes nothing to the majorities that commit it (thesis §4.3) with no
special case, and learner acknowledgements count for nothing anywhere. The
lease generalises the same way: per voter set, the promise that makes a
majority of the set expire latest, the leader counted where it is a member,
and while joint the earlier of the two sets' ends. Replication and heartbeats
go to members plus the learners of the change under way.

A learner's catch-up is measured in rounds by the leader's ticks: a round runs
from its start to the acknowledgement covering the leader's then-last index,
a round shorter than the minimum election timeout promotes the learner, a
longer one starts the next round at the current last index, and the joint
entry is proposed once every learner is caught up. The catch-up state is
leader-local and volatile (PROPOSED D-032). From the joint entry on the change
drives itself on whichever leader holds it: commit of the joint entry proposes
C_new, commit of C_new steps the leader down if it is not in it, so the change
survives the leader that started it. One change is in flight at a time,
catch-up included: a request for different voters while one is under way, or
while the latest configuration entry is uncommitted, is refused the way a
proposal to a non-leader is; a request for the voters of the change under way
or already in force asks for what is already true, is answered `Done`, and
proposes nothing, so an operator's retry over a lossy network is harmless. The
operator asks with `Command::Change { voters }`, a trigger and never an entry,
answered `Done` on accept like a transfer (D-028), completion being
configuration entries the trace shows. `NodeConfig` now separates the address
book, every server that may exist, from `initial_voters`, what a fresh store
starts with: the scenario's servers 4 and 5 start with no configuration at
all, and a server that is not a voter of its configuration in force does not
campaign (PROPOSED D-033). `commit_majority` judges every commit against the
configuration in force on the committing leader at that moment, followed
through `RaftConfig` events, both majorities while joint, with the `servers`
parameter as the initial-configuration fallback.

The scenario (`sim/membership.rs`) runs five server nodes, servers 4 and 5
outside the initial {1, 2, 3}; the operator grows to five voters and shrinks
back, each change with a seed-drawn partition that puts the leader in force on
the minority side of the old voters, alone with a client or keeping 4 and 5;
on half the seeds leadership is handed to 4 or 5 between the changes, so the
shrink exercises the step-down. A change the partition killed is asked for
again, up to four requests. Availability is SPEC §3's criterion as the
existing liveness checks are shaped: on uniformly scheduled seeds, the longest
gap between consecutive completed client operations, with time inside the
partition windows taken out, must stay under ten maximum election timeouts —
the partition itself may block writes while the leader is on the minority
side, so the clock effectively starts at the heal. The variant counts one
merged majority of the two sets while joint, in commits and elections both:
the single-majority rule §4.3 exists to forbid, since a majority of the union
need not contain a majority of either set.

**What the sweep found.** The stage landed green: the core-level tests
(`crates/ananke-raft/tests/membership.rs`) pinned the majority arithmetic
before the simulator ran, and the first twenty-seed sweep passed with every
coverage counter lit — six configuration reverts, three elections while
joint, seven step-downs of a leader outside C_new. The variant is caught on
twenty-eight of a hundred seeds, first as commit majority: leader 2 committed
an entry durable on servers 2, 4 and 5 alone, three of the five merged voters
but one of the three old ones — the disjoint-majority window the partition
with the leader keeping 4 and 5 opens while servers 1 and 3 still stand on
C_old. The measured worst completion gap on the correct server at a hundred
seeds was just under three hundred milliseconds, under a sixth of the
two-second bound; the ten-thousand-seed nightly will say whether it can
tighten.

**Alternatives.** Single-server changes (thesis §4.1): SPEC §3 asks for the
hard version deliberately. Replicating the learner phase as its own
configuration entry, etcd's shape: survives a leader crash mid-catch-up, but
leaves a new leader holding learners with no recorded target C_new and needs
a second entry form; a leader-local phase whose loss the operator observes
and retries is smaller. Refusing a same-voters retry outright: an operator
behind a lossy network could not tell "accepted, answer lost" from "refused"
and would be wedged. Answering the operator only when C_new commits: couples
one response to a multi-round future the trace already shows.

**Consequences.** A leadership change during catch-up abandons the change and
the operator retries. Removed servers keep the last configuration they saw,
sit quiet under the D-033 rule, and are never told to shut down — an
operator's business, left to the backlog. Configuration entries carry empty
learner lists in this stage; the field waits for snapshots (stage E), which
also inherit the `0 / 2 / config` key so a compacted store still knows its
configuration.

---


## D-030 — Snapshots: the compacted log, the streamed checkpoint, the staged install, and the re-seeded server

**Context.** Stage E of RAFT.md's order: snapshots per §1 and §3 — a checkpoint as
the snapshot, `InstallSnapshot` streaming with resumption, the staged install, log
compaction, the `snapshot` task, the refused server re-seeded, the
`SnapshotWithoutCurrentLast` variant, and the invariants learning `RaftSnapshot`.
The genuinely new choices the documents leave open are PROPOSED D-035..D-038;
what follows is the approved design as built.

**Decision.** The core's log carries a compacted prefix: a (last index, last
term) pair the tail starts after, `term_at` answering at the boundary, the
election restriction and the consistency check reading the snapshot when the log
is empty, and a follower whose `next` falls at or below the prefix — or one
designated snapshot-fed (D-037) — fed through `Output::Snapshot(Install)` rather
than entries. Compaction is part of a persist (`compact_to`), taken only when
every follower is past the checkpoint or designated, and traced `RaftCompacted`.
A take is triggered on the leader's tick once the log outgrows
`snapshot_threshold` past the last take, with one deferral: a fresh leader waits
two minimum election timeouts before its first threshold take, because a
checkpoint stalls applies (D-036) and a fresh leader's first duty is its no-op
and its followers; a follower that needs a snapshot sooner gets one on demand.

The take runs in the apply task (D-036): the record under `0 / 3 / snapshot`
first, synced, then `Engine::checkpoint` to `snap-<index>`, so the copy carries
its own identity before its `CURRENT` (D-024). Streaming keeps one chunk
outstanding; the acknowledgement names the next byte wanted, a timeout resends
from there — the resumption RAFT.md asks for, traced `RaftSnapshotResumed` —
eight resends give the stream up, a receiver's restart starts it over, and two
restarts retake the checkpoint. The stream's identity is (sender, leader term,
last index, last term); a change starts the staging over. The receiver assembles
under `install/`, holds the streamed `CURRENT` aside in memory, syncs each
completed file and its directory entry, verifies every staged table with the
engine's own checks, and installs by one repair table and a successor manifest —
the receiver's hard state, the applied index at the snapshot, the record, the
kept tail (kept only when its entry at the snapshot's last index carries the
snapshot's last term), tombstones for the leader's log keys, the quarantine on a
re-seed — with the staged `CURRENT` written last (D-038). The `raft` loop
quiesces the apply task before handing the repair over, so the applied index is
final and the kept tail exact; the completed install retires the incarnation and
the next open adopts the staged store. A refused server binds in re-seed mode,
answers every AppendEntries with a rejection asking from index 1, and is
quarantined for good on the store the install builds (D-035).

The invariants fold `RaftSnapshot` as a per-server floor standing in for the
committed prefix: log matching compares at the boundary only what both sides
still show and requires two snapshots ending at one index to agree in term;
leader completeness and commit majority count a floor as holding the entry;
state machine safety resumes applies at an installed snapshot's index, resets a
refused server's floor for the re-seed restatement, and is exactly what reports
the variant — an unrepaired adoption re-states a *taken* snapshot and a
recovered applied index its restated log cannot account for. The sweep runs
`snapshot_threshold` 12 and `snapshot_chunk` 4096, counts takes, installs,
resumes, compactions, re-seeds and completed re-seeds, treats a re-seeded server
as up again and exempts it from the timer bound, and aims `CrashInstalling` —
drawn from its own `moirae_sched` stream, never lengthening the shared schedule
stream (D-031) — by isolating a follower past the threshold and crashing it a
few milliseconds after the stream's final chunk lands.

**What the sweep found before it passed.** Seed 9 of the twenty-seed gate, the
first run with re-seeding live: the re-seeded server was flagged by the timer
check for never campaigning — which is its design (D-035) — so the check
exempts it. With the threshold at twelve, `LeaseTrustsTheClock` stopped being
caught at all: every trial's fresh majority-side leader took its first
checkpoint the moment it was elected — its whole history was past the threshold
— and the apply stall delayed the write that must land inside the stale-read
window past the client's sixty-millisecond budget; the fresh-leader deferral
above restored the catch. Seed 8: a server that finished installing during an
isolation re-stated the stream's term inside the window and the pre-vote check
called it a term raise; isolations during a refusal, a re-seed or an install
are now skipped. And the variant was uncatchable until the assembler synced the
staging directory's *entries*: the simulated crash keeps only directory
operations `sync_dir` covered, so the staged files all vanished at the crash
and the bogus `CURRENT` never had a store to win — the honest assembler now
syncs the directory as each file completes, which is also what makes an
acknowledged offset durable, and the variant's window became real: caught on 6
of 20 seeds at the gate. And the hundred-seed release run, seed 60: rot refused
one server while another was already quarantined from an earlier re-seed, and
the one remaining voter pre-voted forever — a quarantined server grants nothing
(D-035) and a refused one can only be re-seeded *by* a leader, so no leader can
ever form. That deadlock is D-035's priced-in availability cost, not a liveness
bug: the sweep's liveness bound is now asked only of clusters whose unimpaired
servers — neither refused nor quarantined — still form a majority. The same
seed showed the repair had to own the quarantine key outright: carried forward
when the receiver's history was ever re-seeded, whatever kind of install
refreshes the store, and tombstoned otherwise so a flag riding in the leader's
checkpointed tenant 0 can never quarantine a healthy receiver. And the
ten-thousand-seed nightly, seed 164, the first correct-server failure the sweep
ever produced: a follower two hundred entries behind, fed by a long train of
`InstallSnapshot` chunks because the leader's log past it was compacted, was
flagged by the timer check for not campaigning across four hundred milliseconds
— while the leader was reaching it every few milliseconds. An InstallSnapshot
is a leader's contact as much as an AppendEntries (moirae rule 5), and the real
follower's incarnation keeps its election timer fresh across the install; the
core routes the snapshot to its own task, so the timer check, which read only
AppendEntries as a reset, saw a gap that the server never had. It now counts an
InstallSnapshot from a leader of the server's term or later as a reset too. The
hundred-seed runs never reached a follower that far behind under a compacting
leader; ten thousand did, which is what ten thousand are for. Seed 7381 of the same run was the checker's snapshot floor: it only ever rose, so a refused server re-seeded from a snapshot older than its lost store's kept the lost store's floor and its applied entries read as covered rather than held; an installed snapshot now sets the floor exactly.

**Alternatives.** Multiple chunks in flight: resumption bookkeeping for a
pipeline, for a path whose cost is the checkpoint, not the round trips.
Follower-triggered snapshots: RAFT.md gives taking to the leader, and nothing
here needs more. Deleting checkpoint directories eagerly: a stream may still be
reading one; GC is a backlog line. Streaming the live store instead of a
checkpoint: the checkpoint is the engine's own consistent copy and D-024 already
paid for it.

**Consequences.** A follower fed a snapshot restarts its incarnation on the
adopted store; its socket and inbox survive, so peers see a pause, not a loss.
Applies stall for the duration of a take (D-036). Checkpoint directories
accumulate until a GC exists. The re-seed path makes a refusal recoverable, at
the price of a quarantined voter (D-035), and the sweep asserts a refused server
comes back and applies again across a hundred seeds.

**Superseded in part by D-048.** The account above of seed 164, and of where seed
7381 came from, is D-048's. Seed 164 came from a local ten-thousand-seed run on
1373601, not the GitHub nightly, and was the raft sweep's first correct-server
failure at ten thousand seeds, not the first the sweep ever produced; server 2 was
about sixty-eight entries behind, not two hundred; and no leader was reaching it in
the stretch the check flagged: server 3 had lost its quorum at 12.936 s, and the 21
chunks server 2 received in that stretch were the deposed leader's leftover stream.
Seed 7381 was not of the same run: it came from the nightly, run 34496762339 on
ea6fe7d. The rules this stanza records stand as built: the timer check's
InstallSnapshot arm and the exact snapshot floor.

---


## D-031 — The Figure 8 driver and the batched sweep

**Context.** Issue #22. The sweep ran one entry per AppendEntries (D-026) because
`Variant::CountOlderTermForCommit` (§5.4.2, Figure 8) was never caught at the
default batch size: `become_leader` appends the term's no-op before its first
replicate, so whenever a follower is behind by no more than a batch the no-op
rides in the same message as the older entries the new leader re-sends, the
follower matches both at once, and the count rule never sees an older-term entry
acknowledged without one of the leader's own above it. The window needs a new
leader holding a backlog of uncommitted older-term entries longer than
`max_batch`, and a follower that lacks them.

**Decision.** The sweep runs the default batch size, and one fault of the
schedule is a driver rather than an outage. `Fault::FigureEight` isolates a
follower with a client, fires a burst of puts at the leader without awaiting
replies — each invoked in the trace as its own client process and abandoned, on
a key of its own, so the checker closes each at its entry's apply and the
per-key search sees only puts that always take effect — crashes the leader with
the burst appended, and steers it back into the lead: the third server's sends
are blocked, so it can answer nothing and the isolated follower, whose log is
short, can be voted in by nobody, and the restarted leader, holding the longest
log, campaigns and wins with the isolated follower's vote. It then re-sends its
backlog in batches, and the buggy leader advances its commit index onto an
older term's entry at the first acknowledgement below its no-op, which the
commit-by-current-term fold reports on the spot; the correct leader commits
nothing until the batch carrying the no-op is acknowledged. The driver's
parameters are drawn from a stream of their own, so tuning them re-draws no
other schedule; it takes two slots of eight in the fault draw, since its window
needs the burst, the crash, the disk and the steering to line up and a rarer
draw reports no rate; and `paper.rs` steps the mechanism by hand at the core
level, batches of four against a log of ten.

**What the sweep found.** The issue's sketch — crash the leader before its
commit advance reaches the surviving follower — cannot open the window at this
sweep's speeds, and no crash timing fixes it. A leader appends one entry per
synced batch, a millisecond or two each, and a follower's commit knowledge lags
its appends by one message round trip, so the gap between what the survivor
holds and what it knows committed is the append rate times the round trip: ten
or twenty entries, never sixty-four. What opens the window is the crash itself:
the commit index is not persisted (Figure 2 keeps it volatile), so the doomed
leader restarts with commit zero and its whole log above it uncommitted-as-far-
as-it-knows. Steered back into the lead, it rebuilds its commit index by
counting acknowledgements from the one follower allowed to answer, batch end by
batch end from far below — every end under the no-op is the violation. The rule
follows: the driver needs the old leader re-elected, not the survivor, and the
steering earns its keep — a first cut that let the survivor win caught nothing,
since the survivor's commit knowledge was a round trip behind its log, ten
entries, and one batch covered its whole catch-up. Two of its windows die with
the fault model and are left to it: the restart is refused outright where bit
rot under the crash landed where recovery refuses to guess (D-026, D-027), and
a severely slow clock (D-028's draw) can put the restarted leader's election
past the steering window, after which the heal makes the run an ordinary
leader crash. The crash window has to fit the disk,
not the network: a first cut crashed the leader a few hundred milliseconds
after the burst and on the slower seeds it died holding thirty to sixty
entries, under a batch, since every proposal costs a synced batch to append
and another to apply and the adversarial scheduler (D-016) stretches both —
the crash now waits four hundred and fifty to seven hundred and fifty
milliseconds, drawn from the seed. And adding a fault arm re-draws every
schedule after it from the shared stream: the lease trials' geometry reshuffled
and `LeaseTrustsTheClock` went uncaught on the gate's twenty seeds until the
driver's draws moved to their own stream — the catch rates of every variant are
a function of the whole schedule, and a driver must not move the others' dice.
At a hundred seeds, release: batch one without the driver caught
`CountOlderTermForCommit` on 58; the default batch without the driver on none,
D-026's hole verified; the default batch with the driver on 58, the driver
drawn on 78 of the hundred schedules. What the driver loses it loses to the
fault model and the adversary: the restart is refused on about a sixth of its
windows, the starved seeds append less than a batch before the crash, and on
two windows the burst arrived just as leadership moved and no entry of it was
ever proposed. Every other variant's rate held, and the correct server passes
all hundred: normal client-paced traffic still replicates one entry per
message, so batching bites only on the backlogs the driver builds.

**Alternatives.** A one-way block of the survivor's acknowledgements during the
burst, to freeze the doomed leader's commit index: works, but check quorum
steps the leader down within two hundred milliseconds of the block and caps the
backlog, and the restart's commit reset makes the freeze redundant. Crashing
the survivor instead of muting it: the majority is then the restarted leader
and the isolated follower, the same window, but a second crash doubles the
refusals. `max_batch: 1` forever: the batched send and receive paths, which
every deployment would run, stay unexercised — the hole D-026 recorded. A
guaranteed driver per schedule, like the lease trials: the plain outages would
lose half their draws, and the variant does not need every seed, only a rate.

**Consequences.** The sweep exercises batching and pipelining on every seed and
the Figure 8 window on most. A schedule's faults now draw from two streams, and
any future fault arm must bring its own stream rather than lengthen the shared
one. The burst leaves a hundred-odd pending puts in the history on its own key,
which the checker disposes of linearly. Coverage counts the drivers drawn and
the burst puts invoked, and the sweep asserts both were seen.

---

## D-040 — Seeds run in parallel, and the sweeps' four tiers

**Context.** A ten-thousand-seed run of the Phase 2 sweeps takes about three hours
awake on a laptop, and a laptop is not awake for three hours: the second such run
of the overnight session spent most of its wall-clock asleep behind a closed lid
and did twenty-four minutes of work in three hours forty. Every sweep also ran
its seeds one after another inside its test, so a single test's tail — the lease
test, which runs the guardless server on top of the correct one — set the
binary's wall-clock whatever the machine had to offer. And the nightly's catch
rates were never seen: cargo captures a passing test's output.

**Decision.** Every sweep runs its seeds in parallel through `ananke_sim::sweep`
(`sim/parallel.rs`): `0..count` over rayon's global thread pool, results in seed
order. Every seed's `Sim` is independent — nothing in the workspace holds
process-wide mutable state, and a `Sim` draws every stream from its seed by name
(D-017) — so a trace does not depend on which thread ran it or on what ran
beside it; `sim/tests/parallel.rs` proves it, hashing a seed run alone against
the same seed run inside the driver among its neighbours, for every scenario.
The pool is one per process, so the sweeps of one test binary share it and no
more simulations run at once than the machine has cores, whatever the test
harness's own parallelism. A sweep's closure returns something small and drops
the report, writing a failing seed's trace where the report still is; the
verdict names the first failing seed in seed order and every other one. The
driver is the one place outside `crates/ananke-env` that puts work on host
threads: the scheduler's ban on spawning is about the code under test, which
must not escape the simulator, and `scripts/check-direct-io.sh` confines `rayon`
to that file.

The seeds come in four tiers: 20 at the gate, 100 in CI, 1000 under
`scripts/premerge.sh` — release, seeds in parallel, catch rates printed, run on a
branch before asking for its merge, about a quarter of an hour — and 10 000 in
the nightly workflow on GitHub, which is the only place ten thousand run. The
nightly prints its catch rates too (`--nocapture`).

**Alternatives.** `std::thread::scope` in the sim crate: banned by clippy.toml
and the textual check, and sanctioning a fourth file for the allow would put a
hand-rolled pool and its concurrency cap beside the driver for no gain over
rayon's. One process per seed: the traces and reports are in memory for the
checks, and a process per seed would serialise them for nothing. Running the ten
thousand on the laptop anyway, awake: three hours per verdict, and a closed lid
away from none.

**Consequences.** The coverage folds add reports in whatever order seeds finish;
every field is a sum or a maximum, so the totals are the same. A run that fails
on several seeds reports all of them at once instead of stopping at the first.
Memory is the cores' worth of simulations at a time, which the test harness's
parallelism already was. One dependency, `rayon`, used in one file.

---

## D-032 — Learner catch-up state is leader-local and volatile

**Context.** RAFT.md §1 has servers being added catch up as learners before
the joint entry exists, but does not say where the catch-up state lives. A
leader can crash or be deposed mid-catch-up.

**Decision.** The catch-up phase is leader-local and not replicated: the set
of learners, each one's round and the target voters live in the core of the
leader that accepted the change, and `become_follower` or `become_leader`
clears them. A leadership change during catch-up abandons the change; the
operator sees no joint entry in the trace and asks again, which is harmless
because a request for the same voters is idempotent (D-029).

**Alternatives.** A configuration entry that adds learners, the way etcd's
AddLearnerNode does: the change would survive leader crashes, but the new
leader would hold learners with no recorded target C_new to promote them
into, and the log would need a second configuration-entry form. Blocking the
operator until the joint entry exists: the answer would ride on a replication
future the accept does not control.

**Consequences.** A change can vanish without a trace entry beyond the
request's `Done`; the membership scenario's driver retries, and an operator
must too. Configuration entries always carry empty learner lists in stage D.

---

## D-033 — A server that is not a voter of its configuration in force does not campaign

**Context.** The thesis makes learners non-voting (§4.2.1) and discusses
disruptive removed servers (§4.2.3); the stage D brief requires a server with
no configuration and an empty log to sit quiet. Nothing in RAFT.md says
whether such a server may start elections; pre-vote alone would keep it from
winning but not from knocking every timeout, and `has_majority` of an empty
voter set being false only makes the loss certain, not the knocking quiet.

**Decision.** On its election timeout, a server that is not a voter of the
configuration in force resets its timer and stays a follower: a learner, a
server with no configuration yet, and a removed server cannot win an election
and would only knock. It still grants votes and pre-votes by the usual rules,
still follows any leader that appends to it, and ignores `TimeoutNow` for the
same reason.

**Alternatives.** Campaigning and losing: floods the trace with a role change
and a message fan-out every timeout, forever, on every removed or waiting
server. Special-casing only the empty configuration: leaves a removed server
knocking, the §4.2.3 disruption pre-vote only dampens.

**Consequences.** A server whose latest configuration entry excludes it never
campaigns even while that entry is uncommitted; if a truncation reverts the
entry, the revert restores its right to campaign with its configuration, so
no liveness is lost that the thesis' rules would have kept.

---

## D-035 — A re-seeded server never votes again on that store

**Context.** RAFT.md §3: a server whose store lost state is refused and re-seeded
with a snapshot from the leader (stage E). The lost state may have included the
current term and a vote cast in it, and neither survives the loss by definition: a
re-seeded server that voted normally could vote twice in a term it already voted
in, which breaks election safety, the one property everything else stands on. The
documents fix the re-seed but not what the re-seeded server may afterwards do.

**Decision.** A store rebuilt by a re-seed carries a durable quarantine flag
(`0 / 0 / reseeded`), written in the same repair as the rest of the staged store's
tenant 0, and a server on such a store, for the rest of its life on it and across
any number of clean restarts: grants no vote and no pre-vote, never campaigns, and
makes no lease promise — its AppendEntries responses carry an echo of zero, which
the leader's lease and read-index arithmetic ignore. It still replicates, applies,
answers reads it would forward anyway, and counts for commit majorities.

That is safe by the quorum arithmetic. Election safety needs no help: this server
casts no vote at all, so the vote it may have lost cannot be doubled. Leader
completeness holds because a candidate still needs a true majority of the full
membership, so with one server of three quarantined it needs both of the other
two; any commit majority is two of three and therefore intersects the vote quorum
in at least one *voting* server, which holds the committed entry and bounds the
winner's log by the election restriction. With two of three quarantined no
election can succeed at all: availability is lost, safety is not, which is the
conservative side. The lease is the same story one level down: a promise majority
must intersect a vote quorum in a voter, and a quarantined server's answers form
no promise.

**Alternatives.** Wiping the server and re-adding it through joint consensus as a
new member: the clean answer, but it needs stage D's machinery in the recovery
path and an operator's membership change for every refusal. Protocol-aware
recovery, repairing the local log from the other replicas and reconstructing what
was promised — Alagappan et al., *Protocol-Aware Recovery for Consensus-Based
Storage* (FAST 2018) — is the full answer and is issue #23; this quarantine is
the conservative floor to stand on until then. Suppressing the vote only until
the term visibly advances past anything the server could have voted in:
under-specified exactly where it matters, since the lost vote's term is unknown
by construction.

**Consequences.** A cluster that re-seeds a server keeps one fewer potential
candidate and lease promiser until the operator replaces the store; repeated
refusals can leave no electable majority and cost availability with safety
intact — the release run's seed 60 reached exactly that, one quarantined voter
plus one refused server, with the last server pre-voting into silence. The
sweep prices this in: its liveness bound is asked only where the unimpaired
servers still form a majority, and a re-seeded server otherwise counts as up,
since it commits and applies. The quarantine sticks to the store's history:
any later install carries it forward, and the repair tombstones the key
otherwise so the leader's own tenant 0 cannot quarantine a receiver. Every
affected site is marked `D-035`.

---

## D-036 — The snapshot's metadata is made exact by taking it in the apply task

**Context.** RAFT.md §1: a snapshot's identity — index, term, configuration — is
written into the checkpoint's reserved tenant *before* the checkpoint's `CURRENT`.
The apply task advances the applied index concurrently, so metadata written by any
other task can be stale by the time `Engine::checkpoint` captures the store, and
an installed snapshot with a wrong index is a state machine safety violation
waiting to be installed. The documents ask for exactness and give the seam (the
apply queue) but do not fix the mechanism.

**Decision.** A take is a job on the same queue as the entries the apply task
applies. The apply task, between applies, writes the snapshot record under
`0 / 3 / snapshot` into the live store, synced, with its own applied index and the
term of the last entry it applied, and then calls `Engine::checkpoint`. The apply
task is the only writer of user state and of the applied index, and it is busy
checkpointing, so no apply lands between the record and the copy: the record is
exact by construction, and the checkpoint's copy carries it before the
checkpoint's own `CURRENT` (D-024). Applies queue behind the take and resume
after it.

**Alternatives.** Quiescing the apply task from the snapshot task with a
handshake: the same guarantee with more machinery and two tasks touching the
checkpoint. Reconciling after the fact — checkpoint first, then read back what it
captured: the engine's checkpoint does not expose "the applied key as of the
copy" without opening the copy, which is a second store open per take.

**Consequences.** Applies stall for the duration of a checkpoint; the sweep sees
that as apply latency, not a fault. A crash between the record and the checkpoint
leaves a record naming a directory that is incomplete: it never touches the
store's own correctness, and a stream that trips on it fails with a retake, which
takes a fresh checkpoint. Every affected site is marked `D-036`.

**Superseded in part by D-043.** The Consequences' account of a stream that trips
on an incomplete directory — it fails with a retake, which takes a fresh
checkpoint — did not hold while every take at an index wrote into one directory.
On the nightly's seed 5909 (run 34496762339) the retake was the fault: the leader
re-took snapshot 329 five times in a hundred and ten milliseconds, checkpoint
versions 779 to 783 from 15.261 to 15.370 s, every one into `/raft/snap-329`
under a stream that never completed. D-043 gives every take its own
`snap-<index>-<take>` directory, pins a stream to the newest complete version for
its whole life, and keeps a stream that finds no complete version while a take is
in flight from asking for a retake. The take in the apply task, with the record
before the checkpoint, stands, and D-043's versions rely on it.

---

## D-037 — When an unresponsive follower stops blocking compaction

**Context.** RAFT.md §1: the log compacts to a snapshot only once every
follower's match is past it, *or the follower is a learner being replaced by the
snapshot*. An unresponsive follower must not block compaction forever, or a
refused server's re-seed path never runs: the leader must at some point decide a
follower will be fed the snapshot rather than the log. The documents license the
designation and do not fix its trigger.

**Decision.** A leader designates a follower snapshot-fed when the follower is
behind by more than `snapshot_threshold` entries and has answered nothing for two
minimum election timeouts — long enough that a live follower would have
heartbeat-acknowledged several times — or immediately when the follower rejects
an append at index 1, which no follower with a log does (index 0 is consistent
with every log): that rejection is the re-seed ask of a server whose store is
gone (RAFT.md §3). A designated follower no longer blocks compaction, and
replication to it goes through the snapshot path; a successful append
acknowledgement clears the designation.

**Alternatives.** Blocking until the follower answers: the re-seed path
deadlocks, since a refused server can never answer from a log it does not have.
Compacting on threshold alone, unconditionally: a follower briefly partitioned
away loses its log tail on the leader and pays a whole snapshot for a
half-second's absence. An operator command: right for real deployments,
untestable in a sweep that must exercise the path on random schedules.

**Consequences.** A follower partitioned for more than two election timeouts
while the leader outruns it by more than the threshold is fed a snapshot on heal
rather than the log, which costs a stream where a shorter partition would have
cost a scan. The sweep's `snapshot_threshold` is set low so this happens on real
schedules. The site is marked `D-037`.

**Amended under D-042.** The immediate trigger above cannot fire while a leader's
`matched` for a refused follower stands above 0: the probe never walks below
`matched`, so the follower never rejects an append at index 1, and that leader
re-seeds it only through a designation earned by silence while it was down.
D-042's store incarnations make the trigger reachable. A refused server answers
with incarnation 0, the change resets the follower's progress to `matched = 0` and
clears any designation, and the probe walks to index 0, where the leader either
feeds the snapshot outright or hears the rejection of an append at index 1
(`prev_index` 0) that this entry designates on. A designation is cleared by a
successful append acknowledgement, as above, by a completed install, and by an
incarnation change.

---

## D-038 — The staged install commits by CURRENT-last and is adopted by copy at open

**Context.** RAFT.md §1 fixes what an install must do; D-024 fixes the atomicity
anchor (`CURRENT` last is the store's commit point) and rules out directory
renames, which the fault model does not have. What shape the staging directory
takes, how the receiver's identity gets into the staged store before its
`CURRENT`, and how a complete install replaces the old store across crashes, the
documents do not fully answer.

**Decision.** Three parts, all in `snapshot.rs`.

*Assembly.* Chunks land in `install/` under the server's data directory. The
streamed `CURRENT` is held aside in memory, never written, so the staging
directory is not a store while the stream runs; every other file is written at
its offsets and synced when complete.

*Repair, then CURRENT.* On the final chunk the staged tables are verified with
the engine's own checks and the staged record must match the stream's identity.
The repair is one new level-0 table and a successor manifest, built with the
engine's own writers at `flushed_seq + 1`: the receiver's hard state, the applied
index at the snapshot, the snapshot record (installed, no directory), the kept
log tail, tombstones for the leader's log keys, and the quarantine flag on a
re-seed. Only after the repair is durable is the staged `CURRENT` written,
tmp-and-rename, the same commit point as every store switch (D-024). A crash
before it leaves debris the next open sweeps; a crash after it leaves a complete
install. `Variant::SnapshotWithoutCurrentLast` writes the streamed `CURRENT` the
moment it arrives instead, so a crash mid-install adopts the leader's identity —
the state that never existed, which state machine safety reports.

*Adoption.* At every server start, before the engine opens: a staging directory
with a valid `CURRENT` wins. The old store's `CURRENT` is deleted first, then its
files, then the staged files are copied over with `CURRENT` last again, and only
then is the staging directory's own `CURRENT` deleted — the point of no return,
after which the staging directory can never win again and the adopted store's
own writes are safe. Every step is idempotent, so a crash anywhere re-runs the
adoption on the same bytes.

**Alternatives.** Renaming the staging directory into place: not modelled
(D-024). Running the engine at the staging path forever after: two live
locations for one store, and every later open must decide which is real. An
install-complete marker key checked by opening the staged store: makes winning
depend on a read through the engine rather than on the filesystem's one atomic
switch, and moves the variant off the `CURRENT`-last rule RAFT.md names.

**Consequences.** An install costs one extra copy of the store at the next open.
Old checkpoint directories (`snap-<index>`) are never deleted, since a stream may
still be reading one: garbage until a GC exists, recorded as a backlog line. The
sites are marked `D-038`.

**Superseded in part by D-041 and D-043.** The adoption order above — the old
store's `CURRENT` deleted first, then its files, then the copy — lost a store on
the nightly's seed 6325 (run 34496762339): the schedule crashed server 1
forty-one milliseconds after it finished installing snapshot 129, inside the
adoption, and it restarted on a fresh store, restating term 0, applied 0 and last
index 0. D-041 replaces the order with copy and sync first, the `CURRENT` switch
as the commit point, then the old files' removal, and the removal of the staging
directory's own `CURRENT` last, as before; refuses a damaged staging `CURRENT`
rather than sweeping it; and adds the `RAFT-STORE` marker. The
Consequences' old checkpoint directories that are never deleted are replaced by
D-043's sweep, which deletes every version the record does not name and no stream
reads. And the repair list in *Repair, then CURRENT* is short of what the repair
writes: it also writes the receiver's `0 / 2 / config` key (D-029) and D-042's
`0 / 0 / incarnation`, and the quarantine flag is not written only on a re-seed
but carried forward whenever the receiver's history was ever re-seeded and
tombstoned otherwise (D-030, D-035).

---

## D-039 — A completed snapshot install counts as the leader's contact for the timer check

**Context.** Local ten-thousand-seed runs, on 1373601 and then on f54b468, not
the GitHub nightly, produced the raft sweep's first two correct-server failures at
ten thousand seeds, both
from the timers-fire check (RAFT.md §2, moirae rule 5: a follower that hears from
no leader of its term and grants no vote for two maximum election timeouts must
have campaigned), and both against a follower being fed a snapshot. Seed 164: a
follower about sixty-eight entries behind a leader whose log was compacted past
it, fed a train of `InstallSnapshot` chunks that went on arriving after that
leader lost its quorum, with no leader in the cluster, and that the check did not
read as contact, since the core routes them to the snapshot task; that half is a
plain checker gap, fixed in D-030's stanza. Seed
385: a follower cut off alone by a partition mid-install spent two hundred and
twenty-five milliseconds finishing the install locally — verifying the staged
tables, repairing tenant 0, switching stores — after which the incarnation
switch of D-030 rebuilt its core with a fresh election timer, and it campaigned a
hundred milliseconds later: three hundred and twenty-seven milliseconds from the
leader's last contact, twenty-five past its bound. The code resets the timer at
the switch; the check did not know that.

**Decision.** For the timer check, a completed snapshot install is the leader's
contact: the install was leader-initiated and the server was legitimately busy
finishing it, so the install's restatement (`RaftRecovered` on a server that
never went down) resets the check's clock the way a crash restart's restatement
does. The protocol is unchanged: a new incarnation draws a fresh timeout as it
always has, and the check now models that. Seeds 164 and 385 are pinned in the
gate.

**Alternatives.** Widening `TIMER_TIMEOUTS` from two to three, the knob RAFT.md
§5 names for bounds the ten-thousand-seed sweep trips: blunt, and it dulls the
check that catches `ResetTimerOnAnyRpc`. Changing the code so a new incarnation
inherits the old timer's elapsed count: arguably closer to the paper, where an
install is one RPC and not a fresh start, but a behaviour change to the
protocol's timing under review, and the fresh timer is defensible — a server that
just installed the leader's snapshot has just heard from it.

**Consequences.** An install that takes longer than the bound with no
completion in the window would still trip the check, which is the right
sensitivity to keep: the sweep should see an install that slow. The check stays
a function of the trace alone. The site is marked `D-039`.

**See D-048.** The account of seed 164 this Context builds on is D-048's. D-030's
stanza, which the Context names as seed 164's fix, still gives the account written
before the audit — the nightly's seed, two hundred entries behind, the leader
reaching it, the sweep's first correct-server failure — and D-048 supersedes that
account; the fix stands.

---

## D-041 — The staged install is adopted crash-safely, a damaged install is refused, and a marked store never opens fresh

**Context.** The ten-thousand-seed nightly (run 34496762339), seed 6325: server 1
finished installing a snapshot at index 129 at 5.7711 s and the schedule crashed
it forty-one milliseconds later, inside the adoption. The simulator's confession
at the crash: bit rot on `/raft/install/CURRENT` and lost directory entries for
`/raft/000001.sst` to `000004.sst` — the tables the adoption had just copied
into the store directory, not yet `sync_dir`'d. On restart the engine wrote an
empty first manifest, recovered no log, and the server restated `RaftRecovered
{term 0, applied 0, last_index 0}`: a fresh store, not a refusal. It joined term
5 and was re-installed at 6.3584 s, and committed-entries-stay reported the
truncation from index 1 against a commit index of 119. A voter that held term 5
and a hundred and nineteen committed entries forgot everything, which is the
state D-022 and D-025 exist to refuse. Three holes lined up in
`snapshot.rs::adopt_staged` as built under D-038: the old store's
`CURRENT` and files were removed *before* the staged copies and their directory
entries were durable, so the point of no return preceded the durability of the
new files; a staging `CURRENT` that exists but does not parse was treated as
debris of an install that never finished and swept, which after the old store
was gone deleted the only copy; and the engine's rule (D-024) refuses a missing
`CURRENT` only while manifests or tables remain, so a directory emptied past
that opened fresh.

**Decision.** Four parts, each site marked `D-041`.

*The order.* The adoption copies first and switches last, the way every store
switch is made (D-024). The staged manifest is decoded and every table it lists
must be in the staging directory; then each staged table is copied into the
store directory and synced, and the manifest is written with the copies'
numbers, each file synced and the directory synced; then `CURRENT` is switched
to the copied manifest, tmp-and-rename and `sync_dir`, the commit point; only
then are the old store's files — everything of the store proper listed before
the copies began, log segments included — removed and the directory synced; and
last the staging directory's own `CURRENT` is removed, synced, and its files
swept, as before. Until the switch is durable the old `CURRENT` names the old
store, whole; a crash anywhere before the staging `CURRENT` is gone re-runs the
adoption on the same staged bytes; no directory rename is needed. Names cannot
be reused: the staged store's numbers are the leader's checkpoint's, tables
from 1 and a manifest of 1 or 2, and collide with the receiver's almost always,
so the copies go under numbers past everything on disk — each table at the
highest table number on disk plus its staged number, the manifest one past the
highest manifest, `next_sst` shifted alike, the manifest rewritten to say so.
A re-run after a crash lists the earlier copies with the old store and numbers
past them; they go with the old files at the end, or as orphans at the engine's
next open. A completed adoption traces `RaftAdopted`.

*Damage.* A staging directory with no `CURRENT` at all is an install that never
finished and is swept. One whose `CURRENT` exists but does not parse, names a
manifest that is missing or does not decode, or lists a table that is not
there, is damage: the adoption refuses with `LostState` carrying a `Damage`
reason and touches nothing, the server traces `RaftRefused` and waits in
re-seed mode for a leader's stream, the way a store whose recovery lost state
does. The assembler's own sweep (`Assembler::abandon`, on an identity change or
an install the server decided against) now leaves a `CURRENT` where it is,
damaged or not: only the next completed install's own `CURRENT` replaces it
(tmp-and-rename), so no start in between finds an unfinished install, sweeps
it, and opens the old store the acknowledged install superseded.

*The marker.* A Raft store directory carries `RAFT-STORE`, a small file written
once the engine and the store have opened successfully — a fresh directory at
its first open, a store from before the marker at its next — synced, with the
directory synced after, and never removed: it is not a store file to the
adoption, and the engine's orphan sweep knows only tables, manifests and
`CURRENT.tmp`. Before the engine opens, a directory that carries the marker and
no `CURRENT` that parses is refused with `LostState` (`MarkedCurrentMissing`,
`MarkedCurrentUnreadable`): it was a store, so it is a lost one, never a fresh
one. The as-built variant neither checks nor writes it, so its disk sees exactly
the operations the nightly's did.

*The variant and its fault.* `Variant::AdoptionAsBuilt` is the adoption as
built: the old store removed first, the copies synced after, a damaged staging
`CURRENT` swept, no marker. `Fault::CrashAdopting` aims at it, on every seed as
first written here and on one seed in four since the amendment in the
Consequences below, drawn from its own stream (D-031): the install crash's setup,
then, once the
receiver traces the install complete, a crash the moment the adoption's first
change to the store directory is durable — read from the simulated disk's
durable namespace (`Sim::durable_names`), the old store's files all gone or a
copied file's entry synced in, whichever the adoption does first — repeated
sixteen to thirty-two times, each restart re-running the adoption the crash
interrupted. For the crash-safe order the first durable change is a copy synced
in with the old store whole; for the as-built order it is the old store gone
with the copies' entries pending, and a crash there whose bit rot lands on the
staging `CURRENT` — one block, two per cent per crash — restarts the server on
a fresh store. Thirty-two crashes are thirty-two rolls of the disk's dice. Seed 6325
is pinned in the gate (`seed_6325_which_the_nightly_found_stays_green`).

**What the sweep found.** At a hundred seeds, release, `AdoptionAsBuilt` is
caught on 23 of 100, every catch the nightly's signature,
committed-entries-stay reporting a truncation from index 1; at the gate's
twenty, on 2 of 20. The correct server passes all hundred. The rate is
the rot's: a crash aimed at the window converts a live rot on the staging
`CURRENT` into a fresh store about five times in six, so each aimed crash is
worth a little under two per cent, and the storm's size is what sets the rate.
A first cut drawn on half the seeds, crashing six to twelve times, caught 1 of
20 and 6 of 100; on every seed with eight to sixteen crashes, 0 of 20 and 10 of
100, the gate's twenty seeds being the twenty they are; sixteen to thirty-two
put it at 2 of 20 and 23 of 100. A crash at a fixed delay after the install's
completion, the obvious first aim, lands in the copy phase far less often, since
the adoption's opening reads and the old store's removal take a variable dozen
disk operations first. The simulator's rot rolls over unlinked inodes too, so a
`BlockRotted` on the staging `CURRENT` in a trace is not always a live one:
every retired install leaves a ghost with that path, and most of the rots the
first measurements counted were ghosts. And a correct-server failure during
development, seed 96: a crash rotted the staged manifest, the next start
refused it as damage, correctly, and the leader's re-seed stream began; the
assembler's sweep at the stream's start removed the damaged `CURRENT` with the
files, the fault's next crash landed mid-stream, and the start after it found a
staging directory with no `CURRENT`, swept it as an install that never
finished, passed the marker check on the old store's valid `CURRENT`, and
opened the old store — a rollback to applied 252 on a log compacted at 228,
past an install the server had acknowledged, which state machine safety
reported since the refusal had reset the server's floor. That is the
`abandon` rule above: after a refusal a server comes back only through an
install, which is what the checker models.

**Alternatives.** Renaming the staging directory into place: not modelled
(D-024). Copying under the staged names after checking for collisions: they
collide almost always. Verifying every staged table's checksum before adopting:
the assembler verified them at the finish and the engine verifies them at the
open, and a table rotted since is a refusal either way. Removing the old
store's `CURRENT` when the adoption refuses damaged staging, so the marker
refuses every start until an install is adopted: it destroys a store to express
what the kept staging `CURRENT` already expresses, and loses the adopted store
in the window after the switch and before the staging is retired. Teaching the
checker to accept a restatement from an older store after a refusal: it would
accept exactly the rollback D-022 refuses. A marker key inside the store rather
than a file: the failure is a directory emptied past the store. Aiming the
adoption crash by a trace event at the adoption's start rather than by the
disk: the copy phase begins a variable dozen disk operations later, and the
window is the copy phase.

**Consequences.** An adoption costs the same one copy, plus the renumbering;
the copies of an interrupted adoption are garbage until the next open. A refusal
for staging damage costs a re-seed even when the store directory already holds
the adopted store — a crash after the switch and before the staging is retired,
with rot on the staging `CURRENT` — priced in, a two-per-cent roll inside a
window of a few milliseconds. A store from before the marker is marked at its
next open. One sweep schedule in four now ends with an isolation, an install and
a crash storm on the receiver; the other three are as cheap as they were.

That share is an amendment to what this entry first said, and the sweep's cost
is why. The storm rode every schedule as written, and waiting for an install and
then crashing the receiver some two dozen times with a restart each is by a wide
margin the most expensive fault a schedule carries: it took the raft test
binary's thousand seeds from 667 s to 2218 s, and `scripts/premerge.sh` from
about thirteen minutes to forty, well past the quarter of an hour that tier
exists for (D-040). The share is drawn from the fault's own `"adoption-crash"`
stream (`raft::ADOPTION_STORM_IN`), so no other arm's dice moved with it, and
the crash count on a seed that draws the storm is unchanged; the price is paid
in catch rate, and it is paid about in proportion. `AdoptionAsBuilt` was caught
on 23 of 100 release seeds and 270 of 1000 with the storm everywhere; on one
seed in four it is caught on 8 of 100, the fault is drawn on 26 of those 100
seeds, and the sweep still counts 813 adoptions (re-measured on ec2ecc4 at a
hundred release seeds, two independent re-runs agreeing), so
`Coverage::assert_complete`'s `adoptions` and `adoption_crash_faults` both still
hold at the hundred-seed tier.

One trade was needed, and it is a tier rather than a dice roll. At 8 of 100 the
catch is 0 of the gate's 20 seeds — three seeds in four never roll the rot's
dice at all — so `a_server_whose_adoption_is_as_built_is_caught` asserts the
catch at the hundred-seed tier and asserts at every tier that the fault fired:
the storm drawn on some seed, and adoptions under it for the storm to crash
into. That is the shape D-044 already gives `RefusalNotDurable`, which is caught
on 3 of 100. The alternative — raising the crash count on the seeds that draw
the storm until the gate catches it again — buys a gate signal with the very
cost this amendment exists to remove, and the gate's twenty seeds were never the
tier that owned this catch.

What the share did not buy is the fifteen-minute target itself, and the
measurement says where the rest of the time is. On this machine the raft test
binary's thousand seeds now take 1648 s — down from 2218 s with the storm
everywhere, a quarter off — but `scripts/premerge.sh` runs that binary and the
others, so the tier is still over its quarter of an hour. Attributed at a
hundred seeds, where the same binary takes 146.0 s: without D-043's
re-take arm 138.6 s, without the adoption storm as well 99.1 s, and without
D-044's refusal storm on top of that 90.5 s. So the storm at one seed in four is
27 per cent of the binary, the refusal storm 6 per cent and the re-take arm 5 per
cent, and the remaining 62 per cent — some seventeen minutes at a thousand seeds
— is the sweep without any crash storm at all. The 667 s this entry compares
against was measured before the sweep carried the snapshot work of D-042 and
D-043 and the refusal work of D-044, and no share of the storms recovers it:
getting the tier back under fifteen minutes is a separate piece of work on what
the base run itself costs, and it is not this entry's to do.

`Sim::durable_names` joins `durable_contents` as a harness accessor. Every site
is marked `D-041`.

---

## D-042 — Store incarnations: a leader forgets what a re-seeded follower forgot

**Context.** The ten-thousand-seed nightly (run 34496762339), seed 5909: server 3
was refused at 9.80 s and re-seeded five times, at 10.10, 10.47, 10.85, 13.27 and
16.18 s. It had acknowledged index 333 at 13.86 s and the leader had pipelined
334..408 to it when it crashed; after the last re-seed its log ended at 333, since
the checkpoint a leader feeds is the last one it took, which can sit well below
what the follower acknowledged after it. A leader's `matched` for a follower is
monotone by design (D-026: a stale or duplicated response can only propose a
value already passed), an install's completion raises it and never lowers it, and
the probe rule is `next = hint.max(1).max(matched + 1)`: a leader whose `matched`
stands above a follower's log end can never probe below it. Every rejection walks
the probe back to `matched`, the follower rejects that too, and it is never
counted for a commit again nor re-designated snapshot-fed, since every answer
keeps it from going quiet — until the leader changes. The same `matched` keeps a
refused follower from ever being re-seeded by the leader that matched it: its
rejections ask from index 1 but reject a probe at `matched`, never an append at
index 1 (`prev_index` 0), so the re-seed ask of D-037 is never heard, and only a
designation earned by silence while it was down ever streams to it. Stage E's
re-seed (D-030, D-035)
broke the assumption behind monotone `matched`: a follower can now legitimately
lose entries it acknowledged. Measured on the nightly's trace since, seed 5909's
wedge was not this hazard but the snapshot streams' (D-043) alone: its leader in
force was elected after server 3's last acknowledgement and rebuilt its progress
at `matched: 0`, and server 3 was uncounted because it was queued behind the
other follower's stream (the amendment below). The hazard this entry closes is
real all the same; seed 5909 was not an instance of it.

**Decision.** Three parts.

*The store.* Every Raft store carries an incarnation number under
`0 / 0 / incarnation`, beside `hard` and `applied`: written as 1 at a fresh
store's first open, synced, so every store carries the key explicitly; and by an
install's repair, with the rest of the staged store's tenant 0, before the staged
`CURRENT` (D-038) — carried forward unchanged on an install into a live store,
whose kept tail is everything acknowledged past the snapshot, and drawn afresh on
a re-seed. It is not the lost store's number plus one: at a refusal the engine is
dropped and only its directory reaches the re-seed path, and a number read from a
store the engine refused would not be trusted anyway — a recovery that fell back
to an older manifest could read the leader's checkpointed value, and its successor
could then collide with what a leader had already recorded. The re-seed draws the
number from the environment's rng, never 1, and the leader compares for
inequality only: no order is assumed of it. The store's `RaftRecovered`
restatement carries it.

*The wire.* `AppendEntriesResponse` and `InstallSnapshotResponse` carry the
responder's incarnation, stamped by the server on the way out like the clock, an
additive field on the frame that the studio decoder shows. A refused server,
which has no store, stamps 0 on the rejections it answers with; the re-seed's
`Installed` answer carries the incarnation of the store the install built.

*The leader.* `Progress` records the incarnation a follower last answered with;
the first answer seen only records. An answer whose incarnation differs from the
record resets the follower's progress before the answer is otherwise processed —
`matched = 0`, `next` at the leader's last index plus one, the pipeline, the
probe and any snapshot-feed designation cleared, traced `RaftProgressReset` — so
a rejection's hint is where the rebuilt log ends, and the normal probe walks back
from it rather than from the stale match. After a reset an empty probe goes at
once, as a heartbeat would, so a successful answer that leaves nothing to send
still finds the rebuilt log within a round trip. An install's completion resets
the same way before the install's match is recorded. A stream in flight is left
to the snapshot task, which ends it either way; the quiet count and the lease
state are untouched, since the follower just answered. A refused server's 0 is
a change like any other, so its first rejection walks the probe to index 0,
where the leader either feeds the snapshot outright, the probe being below its
compacted prefix, or hears the rejection of an append at index 1 (`prev_index` 0)
that D-037 designates on: a
refused follower is re-seeded by the leader that matched it, whatever it was
matched at. `Variant::IgnoreIncarnation` records and never resets: the leader
as built. At a hundred release seeds the sweep catches it on 0 of 100; what ten
thousand seeds caught, and why, is at the end of this entry. The wedge stalls a
commit only while the
third server is unavailable; after the last heal every fault has healed or
restarted, so a server is unavailable then only by refusal, and a refused
server beside a re-seeded one is the configuration D-035's carve-out withholds
the liveness bound from (`majority_up`: two impaired servers of three); and
were the bound asked there, the leader as built re-seeds the refused server
too, when it was designated while down, and commits with it inside the bound.
Seeing the wedge would need a liveness ask when a leader in force at the last
heal has a commit majority among the servers that are up, quarantined ones
included, and a schedule that refuses a second follower under that leader, which
only the disk model's rot produces and no driver can aim.

The variant and its sweep test ship as the pair rule asks, and the test is not
ignored: a variant the sweep does not catch is a hole in the sweep to be named,
not a test to be skipped (CLAUDE.md). It asserts instead what is true of this
leader and what a sweep that could not tell the two leaders apart would fail —
that the leader as built never forgets a follower's progress, tracing no
`RaftProgressReset` on any seed, where the correct server's own sweep requires
one wherever it saw a refusal — and that the sweep reaches the state the wedge
is built on: a refused follower re-seeded and applying again, on 67 of 100
release seeds. The catch rate is printed at every tier.

**Alternatives.** Letting a rejection lower `matched`: any stale or duplicated
rejection could then walk a live follower's match back and re-send what it
holds, the flood D-026's probe rule exists to prevent, and a rejection from the
old store would still walk it to the wrong place. Designating a follower
snapshot-fed whenever a probe repeats: hides the wedge behind a second stream of
the same snapshot, which the same `matched` wedges on again. Incrementing the lost
store's number: unreadable at the refusal, and not to be trusted if read. A
number the leader stamps on the stream: a field on a request the snapshot streams
carry (D-043's ground), and a leader that took over mid-stream would stamp from
no record. The shape itself is the usual one — etcd's raft and raft-rs reset a
follower's `Progress` when a leader takes office; a store that can lose
acknowledged entries needs the same reset when the store changes, which is what
the number makes visible.

**Consequences.** A leader forgets a follower's progress once per refusal and
once per re-seed, at the price of a probe walk from its own end. A rejection
stamped 0 that is delayed past the install is a change too: the leader resets,
the rejection asks from index 1, the follower is designated and offered the same
snapshot, and its answer that everything is already there resets it back — a
round trip, not a stall. The rule is inequality, so a success from the dead
store delayed past the rebuilt store's first answer would set `matched` from the
dead store until the next answer resets it again: a window one message delay
long against a whole re-seed. A total order on incarnations, or a per-follower
set of retired ones, would drop such an answer instead, and is the step to take
if a sweep ever finds that window. A store started fresh on a wiped directory
carries 1 again, the number the leader may have recorded for the store that was
wiped: a wipe is outside the fault model (D-012), and a wiped server is a new
member for the membership path, not a re-seed. `RaftRecovered` gains a field,
`RaftProgressReset` is new, and the sweep counts the resets and requires one
wherever it saw a refusal. The core-level scenario is in
`crates/ananke-raft/tests/paper.rs`.

**Amended under D-045.** `Variant` is a set now, so the run this entry
said no sweep could make — one server carrying this bug *and* D-043's at once —
is a run the sweep makes. Four things it settles.

*D-043's fix was the fix for 5909; this entry's was not.* Round two read the
nightly's wedge as needing both conditions at once — this entry's stale
`matched` for the re-seeded follower and D-043's never-completing stream to the
other — each fix removing one. Measured on the nightly's trace since, no stale
`matched` occurred in it: the term-11 leader was elected at 14.757 s, after
server 3's last acknowledgement, and a new leader rebuilds every follower's
progress at `matched: 0` (ea6fe7d, `become_leader`); server 3 then sent that
leader 302 rejections, every one with hint 334, and not one success, and
received no `InstallSnapshot` chunk. It went uncounted because it was designated
snapshot-fed and queued behind server 2's never-completing stream — the shared
directory scrambling that stream, and one stream per leader — which are both
D-043's bugs. So D-043 alone explains 5909, as seed 680 agrees, where
`SharedSnapshotDir` alone fails exactly as the pair does. This entry's fix
remains sound, and today's correct run of seed 5909 exercises its reset —
server 3 refused, then its progress reset at 18.698 s, then re-seeded — but it
was not the fix for 5909. **The combined variant
`{IgnoreIncarnation, SharedSnapshotDir}` is the negative control for a wedge
that needs both bugs**, which a leader needing only *one* countable follower
makes possible and no trace has yet shown; each single variant is the control
for its own half.

*Seed 5909 itself still does not reach the wedge, under the pair as under each
single.* Measured on this tree in release: `raft::run(5909, Variants::of(&[
IgnoreIncarnation, SharedSnapshotDir]))` checks clean, exactly as both singles
and the correct server do. That is a seed whose schedule has moved, not a bug
that has gone: this entry's own eight-byte record change moved every checkpoint,
and D-041, D-044 and the re-take arm have each appended a fault arm drawn from a
stream of its own since, which leaves the shared draws in place — 5909's fault
times match the nightly's through 16.114 s — but lengthens every run and so moves
every seed's interleaving and who leads; and 5909 on this tree draws neither a
snapshot crash nor an adoption storm nor a re-take arm at all, and no re-take on it scrambles a running stream
whatever bugs it carries: under `SharedSnapshotDir`, alone or in the pair, the
leader re-takes once under a live stream, to a follower that had already
installed that snapshot. The pin stays a seed held green and is still worth
nothing as a replay.

*The set bought no new catch at the thousand-seed tier, and that is reported
rather than dressed up.* Swept over seeds 0..1000 in release, all three servers
on every seed: the pair is caught on **1 of 1000** — seed 680, by the liveness
check — `IgnoreIncarnation` alone on **0 of 1000**, `SharedSnapshotDir` alone on
**1 of 1000**, and that is the same seed 680 with the byte-identical message
(`no client write completed after the last heal at 28.683 s`). Seeds where the
pair is caught and *neither* single is: **0 of 1000**. So the
combined variant is not yet shown to catch anything the stream half does not
catch alone, and the negative control it provides for a wedge of both bugs is a
mechanism that now exists and a claim the sweep has not yet been able to
discharge at the tiers run here — which is the honest reading and the one
recorded. The reason it is hard is D-043's own: the wedge needs a stream that
*never completes*, which needs a take to land on the very directory a live
stream has open, and that coincidence is what is rare; once it happens the
stream half alone already stalls the commit, and this entry's stale `matched`
has nothing left to add.

*`IgnoreIncarnation` stays in the sweep — defence in depth, and the owner's
decision.* Measured over a thousand release seeds on this tree: caught
on **0 of 1000**, with **0** progress resets, and the precondition the wedge is
built on — a refused follower re-seeded and applying again — reached on **637 of
1000**, where the correct server needs a reset per refusal and this leader
traces none. Once D-043 holds, this bug on its own is **a delay, not a wedge**:
the stream to the other follower completes, that follower is countable within an
install, and the leader commits through it while the stale `matched` for the
re-seeded one costs that follower a probe walk rather than the cluster a commit.
It is kept because the day something else makes the second follower uncountable
— another bug, another fault arm, a schedule nobody has drawn — the stale
`matched` is a wedge again, and because the fix is cheap while its absence is
invisible until then. The variant ships and its test is not ignored (CLAUDE.md);
what it asserts is what is true of it, and the rate is printed at every tier.

At the nightly's ten thousand seeds (run 34711427220, on 14c3e17) it was caught
on 4 of 10 000 — seeds 1252, 2509, 3087 and 5990 — each a pre-vote isolation
straddle: the isolated server's term rise was adopted from a message delivered
before the isolation began, between 0.31 and 16.57 ms before it, and traced
after, once its persist was durable, with no message reaching the server inside
the window. That is the gap that failed the correct server itself on seeds 1885
and 2023 of the same run. At ten thousand seeds after D-047 (runs 34731272921 on
bd93ed3 and 34749071877 on 9b5995d) it was caught on 0 of 10 000, and the second
run's per-seed report named exactly those four as the catches reading decision
time removed. D-047 moved no schedule — every trace it compared with a tree
before it is byte-identical once `decidedNs` is removed — so the drop from 4 to 0
is itself evidence that the four were timing artefacts, as D-047 had measured each
directly
(`the_nightlys_eleven_variant_catches_of_the_trace_timestamp_gap_are_not_catches`).

Every site is marked `D-042`.

---

## D-043 — Snapshot takes are versioned directories, a stream pins one, and a leader streams to every designated follower at once

**Context.** The ten-thousand-seed nightly (run 34496762339), seed 5909: the
leader's last commit was 329 at 13.43 s and nothing committed for the remaining
5.4 s of the run. Server 2 was fed snapshot 329 from 13.872 s — 735
`InstallSnapshot` chunks, the stream resumed from offset 0 fifteen times — and at
the end the receiver was still acknowledging `000005.sst` while the sender was on
`000010.sst`. The first eight resumes, from 13.92 s, came while a partition had
server 2 cut off (13.386 to 14.322 s). The leader, which had first taken 329 at
13.893 s, then re-took the *same* snapshot five times in a hundred and ten
milliseconds (checkpoint versions 779 to 783, 15.261 to 15.370 s, every one into
`/raft/snap-329`), rewriting the directory under the stream; the resume at
15.409 s follows them, and the stream never completed.
Server 3, refused and re-seeded, was designated snapshot-fed and received
nothing: the `snapshot` task streams to one follower at a time, its stream waited
behind server 2's never-ending one, and a designated follower gets no entries —
every heartbeat rejected with hint 334, three hundred and two times from its
leader's election at 14.757 s. Neither
follower could be counted; the leader lost its quorum at 13.99 s, won term 11 at
14.76 s and was no better off. The liveness check reported it.

As built (D-030, D-036, D-038): every take at an index writes
`snap-<index>`, sweeping whatever was there; the record under `0 / 3 / snapshot`
is written before the checkpoint, so it can name a directory still being
written; the task keeps one outbound stream and a queue of followers behind it;
and no checkpoint directory is ever deleted, the backlog line D-038 left. The
mechanism behind the five re-takes is the record's head start: a threshold take
wrote the record naming the directory it was about to write; an `Install` for
another follower read the record, opened an empty or partial directory, and
reported the checkpoint unusable; the core cleared both its checkpoint and its
pending take, and the next heartbeat asked again — a second take at the same
index queued behind the first, into the same directory, and so on. RAFT.md §1
gives the stream its resumption and the leader its threshold rule; it does not
say where a take goes, what a stream reads while the next take lands, when a
directory may go, or how many followers a leader feeds at once.

**Decision.** Six parts, every site marked `D-043`.

*Versioned takes.* Every take goes to its own directory,
`snapshot::version_dir`, `snap-<index>-<take>`, numbered by a per-store take
counter the record carries (`SnapshotRecord::take`, eight more bytes in the
value): the counter is read from the record and advanced by every take, so a
restart continues the numbering, and two takes at one index are two directories.
An install's repair writes the counter as zero, so a name can recur on a
re-seeded server whose lost store had taken at the very index it takes at
again; that is harmless because the sweep below empties every directory the
record does not name before the incarnation's tasks run, and a take clears its
directory before writing anyway. `snapshot::take` keeps its signature and its
sweep-and-rewrite of an explicit directory, which the variant uses; the correct
server takes through `snapshot::take_version`.

*A stream pins a version.* A stream reads the directory it opened for its whole
life: a resend after loss resumes on it, and a newer take, at the same index or
a later one, never touches it. What it opens is `snapshot::find_version`, the
newest *complete* version of the index the core asked for — complete meaning
the checkpoint's own `CURRENT` is there, since the engine writes it last and
synced (D-024) and the record precedes the checkpoint (D-036), so the record may
name a take still in flight or one a crash cut short. The conservative option,
taken: a leader that has taken a newer snapshot keeps streaming the pinned one
to completion. The stream's identity on the wire is (sender, leader term, last
index, last term), which cannot tell two takes at one index apart; switching
versions mid-stream would let the receiver resume across them, and a deliberate
restart at offset 0 under the same identity is read by the assembler as a
duplicate of a file already done. Both need the codec, which D-042 owns. The
cost is one more install where the leader compacted past the pinned index while
the stream ran: the follower installs the older snapshot, is found below the
prefix, and is fed the newer one.

*Deletion.* `snapshot::sweep_versions` deletes every version that is neither the
record's nor read by a stream; the `snapshot` task keeps a reader count per
directory (`Streams::readers`), incremented when a stream opens and released
when it ends. The directories are listed *before* the record is read: a take
writes its record before it creates its directory, so a directory the listing
saw and the record does not name is an old version, never one in flight. The
filesystem has no directory removal (D-024), so a version is deleted by
removing its files and an empty directory is not a version. The sweep runs at
every incarnation's start, in `incarnation` before its tasks are spawned — a
fresh incarnation reads none of its predecessor's versions, an installed
store's record names none, and running it before any task can take is what
keeps a recurring name from ever naming a directory with files in it — then in
the `snapshot` task after every completed take and after every stream ends;
each deletion is `RaftSnapshotDeleted`. This closes the checkpoint-directory GC
that D-030 and D-038 left to the backlog.

*One stream per designated follower.* The task keeps `Streams::outbound`, a
stream per follower, and services them all: the chunk timer is the earliest
deadline among them, every stream past its deadline is resent or given up in
the same pass, and an acknowledgement finds its own stream by sender. A
designated follower is never queued behind another's stream. Each opening is
traced `RaftSnapshotStreams` with the count in flight.

*The guard, and the retake gate.* A take that would not advance the index is a
second version of the same state, so the `apply` task answers a plain
`Job::Take` at the record's own index with the recorded version when it is
complete, traced `RaftSnapshotReused`, and takes a fresh version otherwise. A
take the core asks for after a checkpoint was found unusable — a stream that
could not open one, a receiver that refused one twice, a take that failed — is
a `Job::Retake`, a fresh version even at the record's index, since the recorded
one is the one found wanting. And a stream that finds no complete version while
a take is already in flight reports its failure without `retake`: the core then
asks the stream again on the next heartbeat and finds the take landed, where a
retake would have cleared the pending take and queued a second one at the same
index behind it — the cascade of seed 5909. Recorded honestly: by construction
the correct server's plain takes always advance the index — the threshold take
requires the applied index past the last take, and the on-demand take only runs
with no checkpoint at all — so the guard is a belt whose count the sweep
reports; the two gates are what stop the waste.

*The variant.* `Variant::SharedSnapshotDir` is the server as built: one
directory per index through `checkpoint_dir` and `take`, one stream at a time
with the queue behind it, the record's directory opened whatever its state, and
a retake asked whenever a stream fails for want of a checkpoint. The sweep runs
it beside the correct server and reports its catch rate, how many of its
catches were the liveness check's, and on how many seeds the fault fired — a
take at the index already taken, into the directory a stream may be reading,
which the test folds from the trace and asserts at every tier, so a sweep that
passes is known to have injected the fault. The catch itself is asserted at the
nightly's tier, ten thousand seeds, the only tier that ever produced it: this
is the server whose hundred seeds CI passed when it merged.

**What the sweep found.** The hundred-seed release run, as recorded in 1bafacb:
the correct server
passes all hundred, with 2905 snapshots taken, 1279 installed, 4696 streams
resumed, 2540 versions deleted by the sweep, 70 stream openings that made two
streams run at once, and no take answered by the recorded version — the guard
never fired, as the construction above predicts; the slowest write after a heal
took 375 ms against the 2 s bound. `SharedSnapshotDir` is caught on 0 of 100
seeds, and on 0 of 1000 at the pre-merge tier, while the fault fired — a take at
the index already taken, into the shared directory — on 8 of the gate's 20 seeds
and 51 of the 100. That is the expected shape, not a surprise: the
variant is the server that passed CI's hundred seeds when it merged, and the
catch took the nightly's ten thousand, once. As built, cb15eb1 still fails seed
5909 on this machine with `liveness: no client write completed after the last
heal at 16.114 s`; on this branch the same seed passes under both the correct
server and the variant, because the record's value is eight bytes longer on
every take, which moves the engine's flushes and with them the checkpoints'
file sets, so no schedule on this branch replays the nightly's. The variant is
therefore asserted caught only at the nightly's tier, `ANANKE_SEEDS` of ten
thousand or more, and asserted to have fired at every tier; a nightly that
does not catch it is a hole in the sweep to be reported, not a variant to
delete (RAFT.md §5).

**Alternatives.** Carrying the take number on the wire, so a stream could switch
to a newer version by restarting under a new identity: a codec field, D-042's
territory, for a switch nothing needs. Deriving the take number from the
directory listing instead of the record: a scan at every take, and a crash
between a listing and a checkpoint leaves the numbering to the next scan; the
record is already synced before every checkpoint. Deleting versions eagerly at
the next take: D-030 rejected it for the stream still reading. Keeping the
core's pending take across a retake, in `core.rs`: the same effect, kept out to
leave the core to D-042's Progress reset; the server-side gate sees the same
events. Round-robin over one stream at a time: a stream that never ends still
starves the rest, and RAFT.md §3 gives the task no reason to hold one back.
Directory removal in the filesystem model: D-024's decision, out of scope.

**Consequences.** Three trace events (`RaftSnapshotDeleted`,
`RaftSnapshotReused`, `RaftSnapshotStreams`) and their moirae lines; the snapshot
record's value grows by eight bytes, with no released store to migrate. A
leader's data directory holds the record's version plus whatever streams still
read, and nothing else after the next sweep; a follower sweeps its old versions
at its next start. Every stream costs one chunk in flight, so a leader feeding
two followers has two. Seed 5909 is pinned under this entry's variant and
D-042's alike, and passes under each: see D-045 for why one enum could not
carry both bugs. `sim/tests/raft.rs` counts versions deleted, takes
reused and streams at once, and `crates/ananke-raft/tests/snapshot.rs` shows
two takes at one index as two directories, a stream completing under a newer
take where the shared directory's does not, the sweep sparing the pinned and
the recorded versions, and two designated followers streamed to at once.

*The aimed fault, and what it did not buy.* This entry left the shape to the
sweep's owner — "a re-take under a running stream and a second designated
follower at once, which no driver-side fault forces directly" — and
`Fault::RetakeUnderStream` is that fault, drawn on one seed in four from its
own `"retake-stream"` stream. It fills the state machine with a couple of
hundred keys so a checkpoint is worth streaming at all, isolates a follower
until it is designated snapshot-fed, waits in small slices for the leader to
open the stream to it, and then cuts the leader's *other* follower off. The
leader keeps its quorum — the follower it is feeding answers every heartbeat,
so check quorum is satisfied — and has nobody to count, so nothing commits and
its applied index stands still at the index it last took, which is the index
the running stream is reading. It reaches that state on 14 of 100 release
seeds, and the sweep asserts at every tier that it did.

It has not been shown to make the variant catchable at any tier. Measured on this
branch: 0 of 100 release seeds, and 1 of 1000 — seed 680, by the liveness check,
`no client write completed after the last heal at 28.683 s`. That catch is not
the arm's: seed 680 draws no re-take arm (its faults are an isolation, a Figure 8
driver, a one-way block, the adoption storm and the refusal storm), and its
re-takes are the server's own. The arm reached a live stream on 151 of 1000
seeds and caught none of them. This entry recorded 0 of 1000 before the arm
existed; the one catch since came from how that round moved seed 680's
interleaving, not from the arm.

Why the arm reaches the shape without catching it is structural rather than
statistical. A leader needs *one*
countable follower for a majority, and on this sweep an install is over in about
a hundred and fifty milliseconds, since the state machine is small and a
checkpoint of it is a couple of dozen chunks. The moment the fed follower's
install completes it is countable again, the leader commits, its applied index
moves off the index the stream was reading, and the freeze is over. So the queue
half of the bug costs a second designated follower a few hundred milliseconds
against a two-second liveness bound, never the bound itself; only a stream that
never completes stalls a commit for as long as the check asks. Making one needs a
take to land on the very directory a live stream has open, which as built needs
the leader's applied index to be standing exactly where its record already points
*and* a `retake` to have cleared its checkpoint — a coincidence inside the
snapshot task's own failure paths, which the arm makes likely (a frozen applied
index, a stream in flight, and every take asked for landing at that index) but
cannot force, since those paths are the receiver's refusals and the network's
duplicates rather than anything a partition, block or crash schedules. The
re-take at an index already taken happens often on its own, on 48 of 100 seeds
(173c84f) and 532 of 1000 (78a3711's pre-merge run, on 173c84f's code), and is
harmless every time, because no stream had that directory
open.

Two ways past that were weighed and not taken. Filling the state machine until an
install runs longer than the liveness bound: measured, and at four-kilobyte
values the sixteen-kilobyte memtable rotates so often that the cluster's commit
rate collapses, no follower falls behind enough to be designated, and the arm
stops reaching its own shape at all. Reaching inside the server for a fault hook
on the take: that is a fault model of the implementation rather than of the world
(D-012). So the catch stays asserted at the nightly's ten thousand, where a rate
of about one in a thousand gives some ten catches; asserting it at the pre-merge
tier on a single observation would make that tier flaky. The firing is asserted
at every tier on both counts — a re-take at an index already taken, and the arm
reaching its stream — and a nightly whose ten thousand seeds never catch it is
still a hole in the sweep to report rather than a variant to delete (RAFT.md §5),
the more so because this branch has moved every seed's interleaving again.

*At the nightly's ten thousand.* Run 34711427220, on 14c3e17, printed this
variant caught on 13 of 10 000 seeds, which is not thirteen catches of this bug.
Four are the liveness check, the wedge this entry is about: seeds 680, 2013,
9445 and 9993. Seven are the pre-vote isolation check reading a trace timestamp
— a term rise adopted from a message delivered before the isolation began, or on
seed 5203 a candidacy decided on a pre-vote answer delivered before it, traced
after its persist with no message reaching the server in the window — the gap
that failed the correct server on seeds 1885 and 2023 of the same run: seeds
1176, 2407, 3863, 4713, 5203, 6691 and 9670. Two are the linearizability checker
running out of its search budget, on seeds 1262 and 7222, which is not a proven
violation. The test's ten-thousand-seed assertion counted all thirteen; D-047
changed it to assert, at that tier, that the liveness check caught the variant.

**Amended under D-045.** `Variant` is a set now, so the sweep can run
one server carrying this entry's bug and D-042's at once, which is what the
nightly's seed 5909 was. This entry's fix alone was the fix for 5909. Round two
read the wedge as needing a stale `matched` for the re-seeded follower (D-042)
beside the stream that never completed; measured on the nightly's trace since,
there was no stale `matched` — the term-11 leader had rebuilt its progress at
`matched: 0` — and both followers were uncounted by this entry's two bugs:
server 2 behind the stream the shared directory scrambled, and server 3
designated snapshot-fed and queued behind that stream in the one-stream backlog
(D-042's amendment has the numbers). Seed 680 agrees: this entry's variant alone
fails it exactly as the pair does. So this entry's variant is the control for
the wedge 5909 was, and **the combined variant `{IgnoreIncarnation,
SharedSnapshotDir}` is the negative control for a wedge that needs both bugs**,
which no trace has yet shown.

What the pair measures on this tree is reported as measured. Over seeds 0..1000
in release the pair is caught on 1 of 1000 and this entry's variant alone on 1
of 1000, and it is the *same* seed 680 with the byte-identical liveness message;
`IgnoreIncarnation` alone is caught on 0 of 1000; and the seeds where the pair
is caught and neither single is number 0 of 1000. That is
consistent with this entry's own account of why the catch is rare: the wedge
needs a stream that never completes, which needs a take to land on the very
directory a live stream has open, and once that coincidence happens the stream
half alone already stalls the commit for longer than the bound, leaving D-042's
stale `matched` nothing to add. The pair costs the sweep nothing it was not
already paying and is the only run that can ever be the nightly's server; it
stays, and its rate is reported beside this entry's own.

---

## D-044 — A refusal is durable, and a refused engine does no work

**Context.** The thousand-seed premerge, seed 687: server 3's engine open dropped
SST 1 — it held sequence numbers 1..98 of the state machine — and
`RaftStore::open` refused with `LostState { dropped: [1] }`; the node traced
`RaftRefused` at 7.9209 s, exactly as RAFT.md §3 and D-025 require. Seven
milliseconds later the refused server's own engine carried on working:

```
7.9274 ananke.sst.written        {"number": 4, "level": 0, "entries": 167, "firstSeq": 275, "maxSeq": 388}
7.9303 ananke.manifest.written   {"number": 5, "flushedSeq": 388, "tables": [2, 3, 4]}   <- table 1 forgotten
7.9341 ananke.manifest.switched  {"manifest": 5}
7.9341 ananke.memtable.flushed   {"memtable": 1, "upTo": 388}
7.9348 ananke.wal.segment-deleted {"segment": 2}
```

The flush of the memtable the recovery had just replayed rewrote the manifest
without the lost table and deleted the log segment that held its records: the
evidence of the loss, laundered away. No leader existed for the next 5.4 s —
server 1 had been refused at 5.92 s — so no re-seed came. The schedule crashed
server 3 at 13.32 s and restarted it at 13.53 s; this time the open found a
self-consistent store, removed `000001.sst` as an orphan and opened clean:
`RaftRecovered { applied: 185, last_index: 189 }`, no refusal and no install. A
voter with a hole in its state machine rejoined and began pre-voting, and the
sweep reported *state machine safety: server 3 recovered an applied index of 185
but its log does not hold index 1* — the rule that a `RaftRecovered` may not
follow a `RaftRefused` without an install between them, which is the right rule.

Two flaws behind it. **A refusal is not durable**: it lives only in the running
process, and D-041's marker says a directory *was* a store, not that the
store *lost state*, so the next start decides afresh on whatever it finds. And
**a refused engine keeps running**: its flusher, its compaction and the
log-segment deletion that follows a flush are all still on, and the first of them
writes over the very hole the recovery reported. RAFT.md §3 says a refused server
"participates in nothing"; it says nothing about the engine underneath it, and
nothing about a refusal outliving the process that made it.

**Decision.** Four parts, every site marked `D-044`.

*The mark.* D-041's `RAFT-STORE` marker gains a second form. A whole store's
marker holds one line, `ananke raft store`; a lost store's holds `ananke raft
store lost` and the refusal's reason on the line after it, word for word
(`store.rs`: `mark_store_lost`, `Marker`, `write_marker`). It is written in place
and synced, with the directory synced after, through `env.fs()` and never through
the engine, which is the thing that is damaged. `refuse_lost_store` reads the
marker first: a lost one refuses at once with `LostState::from_mark`
(`Damage::MarkedLost`, the recorded reason carried in the new `lost_mark` field),
before it looks at `CURRENT` at all. **Any content that is not exactly the whole
store's line reads as lost**: the marker is written in place, so a write a crash
cut short leaves a file that is neither, and the store it stands for is the one
the refusal was writing about. The node writes the mark at *every* refusal,
before the `RaftRefused` trace and before re-seed mode — the staging damage of
D-041 included, where the store directory may still hold a whole store: a server
refused there has acknowledged an install it no longer has, so the store under it
is a rollback waiting to happen, which is the failure D-041's seed 96 found. A
marker that cannot be written fails the server (`RaftServerFailed`) rather than
leaving it refused on a disk that will open clean.

*The quiesce.* `EngineRecovery::lost_writes` is the engine's own name for a hole
in the middle of the state — a dropped table, a manifest fallback, a discarded
log head, a log stopped at a bad checksum or a gap, a corrupt record skipped in a
segment the tables cover — and `ananke-raft`'s `LostState::of` now asks it rather
than repeating the rule, so the two can never disagree. With the new
`EngineConfig::quiesce_on_loss`, an open whose recovery lost writes **never
spawns the flusher**: no table, no manifest, no compaction, no log segment
deleted, and `TraceEvent::EngineQuiesced` says so. `Engine::quiesce` does the same
to a running engine, and the node calls it the moment `RaftStore::open` refuses a
store the engine itself opened happily. Both are needed: the flag stops the flush
that would otherwise land while the store is still being read, which is the seven
milliseconds seed 687 lost; the call covers a refusal only the store can see. The
flag is off by default, so an engine whose caller allows fallbacks and head gaps
keeps the behaviour it had; the Raft node sets it.

*The install clears it.* The adoption writes the marker fresh
(`snapshot.rs::adopt_staged_under`) immediately after the switch of `CURRENT` is
durable — the point at which the installed store is the one in force. Before the
switch a crash must leave the old store refused, which is what the mark is for;
after it the store in the directory is a new one and the old one's mark goes with
it. `Assembler::finish` needs nothing: the marker lives in the store directory,
and only the adoption puts a store there, so the re-seed in `node.rs` clears the
mark at its next start, through the adoption, like every other install.

*The variant and its fault.* `Variant::RefusalNotDurable` is the server as built:
no mark, and an engine that keeps working. `Fault::CrashRefused` aims at it, on
every seed, drawn from its own stream (D-031): three to five rounds, each waiting
for the victim to rotate a memtable it has not finished flushing and crashing it
there, then restarting it — and when the victim is already sitting refused,
crashing it after a grace of sixty to a hundred and sixty milliseconds instead.
Both halves are aimed, and the measurements said why. The engine as built
launders a refusal away only if the memtable its recovery replayed is over the
threshold and gets flushed, and a crash at an ordinary moment leaves a tail of
*one* memtable, which replays into a memtable that never rotates: over a hundred
release seeds, the sweep's seventy-odd refusals for a dropped table laundered
nothing at all. A crash inside a flush leaves a tail of two, because the manifest
in force is still the older one until the flush switches `CURRENT`. At the
sweep's write rate a server fills a sixteen-kilobyte memtable about every two
seconds and takes some fifteen milliseconds to flush it, so that window is a
hundredth of the time and no crash of its own choosing finds it; the fault waits
for it. The grace is the second measurement: the laundering flush itself takes
those fifteen milliseconds, and a crash two milliseconds after the refusal kills
it half-done, which leaves the store visibly damaged and the bug invisible.

**What the sweep found.** At a hundred seeds, release, on cf11ddc, when D-041's
adoption storm still rode every seed, `RefusalNotDurable` is caught on 2 of 100,
both the premerge's signature — *state machine safety: server 1 recovered an
applied index of 551 but its log does not hold index 1* on the first — and the
fault is seen firing, a crash landing on a refused server, on 67 of those hundred
seeds; at the gate's twenty it is caught on 1 of 20. The correct
server passes all hundred of both sweeps, and its coverage over them counts 851
refusals, 77 engines quiesced, 41 refusals the store's own lost mark made, and
687 crashes landing on a refused server. The rate is the conjunction's: a crash
must land inside a flush, about a hundredth of the time and the reason the fault
waits for one; its bit rot must land in a table the manifest lists, two per cent
per block; and the crash after it must come before a leader re-seeds the server.
The first cut of the fault, which crashed a refused server at a moment of its own
choosing, was caught on 0 of 100 with seventy-five dropped-table refusals to work
with and not one of them laundered: the memtable a crash at an ordinary moment
leaves is under the threshold and is never flushed at all, which is what sent the
aim at the flush window. A second cut, which crashed two milliseconds after the
refusal, was caught on 0 of 100 for the opposite reason — it killed the
laundering flush half-done, and a store the crash interrupts stays visibly
damaged — which is what set the grace at sixty milliseconds and up. Because the
conjunction is thin, the variant test asserts the catch at the hundred-seed tier
and reports the rate at every tier, the way `SharedSnapshotDir` is asserted at the
nightly's (D-043); what every tier asserts is that the fault fired.

**Alternatives.** A mark inside the store, under tenant 0: the engine is the
damaged thing, and writing the refusal through it is writing through the hole.
Removing `CURRENT` at a refusal, so D-041's marker rule alone refuses every
start: it destroys a store to express what a line of text expresses, and it is
irreversible if the refusal was the disk's fault and not the store's. A separate
`RAFT-LOST` file beside the marker: two files where one has two forms, and a
sweep that removes one of them is a bug waiting to be written. Writing the mark
by tmp-and-rename, so a torn write cannot leave a file that is neither form: a
crash then leaves a `RAFT-STORE.tmp` that nothing sweeps, and reading an
unrecognised marker as lost costs nothing, since the conservative direction is
the refusing one. Clearing the mark in `Assembler::finish`, when the staged store
is complete: the store in the directory is still the lost one until the adoption
switches to it. Quiescing every refused engine by dropping it: a dropped engine's
flusher still finishes the memtables it holds (`NextImmutable` stops only when the
queue is empty), which is exactly what seed 687 shows. Refusing writes on a
quiesced engine as well: nothing writes to a refused store, and the log taking a
write it never flushes harms nothing. Teaching the checker that a restatement
after a refusal is allowed when the store looks whole: it would accept the state
that never existed, which is the thing D-022 refuses. Aiming the fault by a trace
event for the flush rather than by waiting for one: the same watch, one indirection
further away.

**Consequences.** A refusal costs one small file write and two syncs, on a path
that already runs at most once per start. A store refused once needs an install
to come back, whatever the disk looks like afterwards — including the case where
the refusal was for damage in the staging directory and the store proper was
whole, which now costs a re-seed it did not cost before; that is the price of not
rolling back past an install the server acknowledged. A crash between the
adoption's switch and the marker it writes next leaves the adopted store behind a
lost mark and costs another install, a window of one file write. A quiesced
engine keeps a store that is bigger than it needs to be: the memtables it
replayed are never written down and its log segments are never deleted, until an
install replaces the directory. `Sim` gains one accessor, `trace_from`, a copy
of the records from an index on, through which the crash aimed at a flush watches
the trace; `TraceEvent` gains
`EngineQuiesced`, and the moirae bridge a line for it. Every schedule now ends
with a crash storm aimed at a flush, three to five crashes and up to two and a
half seconds of waiting each, which lengthens a seed's run; the correct server
passes every seed of it. The `AdoptionAsBuilt` variant of D-041 writes no lost
mark, since it is the server before the marker existed at all, but its engine is
still quiesced: a variant turns off its own fix and no other.

---

## D-045 — A variant is a set

**Context.** Round two's reading, recorded at the end of D-042 and in
D-043, was that the nightly's seed 5909 (run 34496762339) wedged the
cluster because *two* bugs held at once — the leader's `matched` for a
re-seeded follower standing above its rebuilt log (D-042) and the snapshot
stream to the other follower never completing (D-043) — so that either fix
alone would remove one of two necessary conditions, a leader needing only *one*
countable follower for a majority. Measured on the nightly's trace since, that
premise is wrong: the leader in force held no stale `matched`, having been
elected after the re-seeded follower's last acknowledgement and rebuilt its
progress at `matched: 0`, and that follower was uncounted because it was
designated snapshot-fed and queued behind the scrambled stream, so D-043's two
bugs explain 5909 on their own (the amendments to D-042 and D-043). What the
premise exposed stands regardless: `Variant` was a single enum on `RaftConfig`,
so no run of the sweep could put two bugs in one server, and a wedge that does
need two — which the one-countable-follower arithmetic makes possible — had no
negative control and could not even be asked about. Both round-two entries
recorded that as a structural limit of the mechanism rather than close it. The
owner's decision after round two, which this entry records: `Variant` becomes a
set.

**Decision.** Five parts, every site marked `D-045`.

*The vocabulary stays.* `Variant` remains what it was: the enum of single bugs,
one arm per rule broken, each with its reference and its `D-0xx`
marker. No variant's meaning changes here, and no variant is added or removed.
`Variant::BUGS` lists the buggy arms in declaration order — `Variant::Correct`
is not among them, because the correct server is the *absence* of every bug
rather than a bug of its own — and `Variant::bit` maps each arm to a distinct
bit by an exhaustive match, so a variant added later does not compile until it
has been given one.

*The set.* `Variants(u32)`: a `Copy` bitmask struct with `Variants::correct()`,
`of(&[Variant])`, `contains(Variant)`, `with(Variant)`, `is_correct()`, `len`,
`is_empty` and `iter`, all but `iter` `const`. The empty set is the correct
server, so `Variants::default()` is correct and a `RaftConfig::default()`
carries no bug. Asking a set whether it `contains(Variant::Correct)` asks
whether it is empty: `Variant::Correct` contributes no bit, so it is the only
answer consistent with `Variants::of(&[Variant::Correct])` being the correct
server, and it is the conservative one — it can never report a bug that is not
there. A bitmask rather than a collection because this is read on a path the
core walks every step: no allocation, no hashing, `Copy` so the server's tasks
each hold their own, and deterministic by construction. `HashSet` is banned
outside `ananke-env` anyway (D-014), and a `BTreeSet<Variant>` would cost an
allocation and a `Clone` for a set that never exceeds a handful of members.

*The composition.* A set turns off exactly the fixes of its members and no
others: every `contains` site reads one variant's bit and nothing else, so a
server carrying two bugs is the server carrying each of them, with no third
behaviour introduced between them. That is the conservative reading and the
only one the existing sites support — each was already written as "this variant
turns off its own fix and no other" (D-044) — and it is what makes the pair a
negative control for the wedge rather than a new server to be argued for.

*The config and its sites.* `RaftConfig::variant: Variant` becomes
`RaftConfig::variants: Variants`, and every comparison — `core.rs`, `node.rs`,
`snapshot.rs` — becomes `variants.contains(Variant::X)`, `!= ` becoming
`!...contains(...)`. The `Server` and `Assembler` fields and the `apply` task's
parameter follow the config's type. Nothing is serialized: the bits appear on
no wire frame and in no trace event, so the numbering is an implementation
detail free to change with the enum.

*The call sites.* `sim::raft::run`, `run_with`, `node_config`, their
`sim::membership` twins, `snapshot::Assembler::new`, `adopt_staged_under` and
the core tests' `config` helpers take `impl Into<Variants>`, and
`From<Variant> for Variants` converts, so the roughly thirty existing
`raft::run(seed, Variant::Correct)` call sites compile unchanged and a single
variant stays as short to write as it was. `Report::variant` becomes
`Report::variants`, and `Variants`' `Debug` is written by hand rather than
derived, because the sweep's rate lines print it: `Correct` for the empty set,
`{IgnoreIncarnation, SharedSnapshotDir}` for a pair.

**Alternatives.** *A combined variant enum arm* — one `Variant` arm meaning
both bugs. It is the smallest change and it was rejected: every pair worth
testing needs its own arm, which is quadratic in a list already fourteen long,
each arm must be threaded through every `==` site it participates in, and each
would be a new known-buggy server to document rather than a composition of two
documented ones. *A second config flag* beside `variant`, holding an optional
extra variant: it makes two the maximum by construction, leaves two fields that
can disagree about the same bug, and asks every site which of them to read.
*A `Vec<Variant>` or `BTreeSet<Variant>` on the config*: the same expressiveness
as the bitmask, at an allocation and a `Clone` per server and a linear scan on a
path walked every step, with `HashSet` banned outside `ananke-env` (D-014).
*Leaving it alone and recording the gap*, which is what round two did: it is
honest, and it leaves the sweep without a negative control for a wedge of two
bugs, which round two believed had already happened once in a nightly.

**Consequences.** `RaftConfig` gains a renamed field: `variant: Variant` becomes
`variants: Variants`, which every construction of a config outside a
`..RaftConfig::default()` must follow, and `Report::variant` becomes
`Report::variants` for both scenarios. Nothing else about any server changes.
The bits reach no wire frame and no trace event, so no trace hash moves and no
schedule is re-drawn by this entry — the check that says so is that the numbers
this branch measures for the existing variants are the numbers round two
measured: `IgnoreIncarnation` reaches its precondition on 637 of 1000 release
seeds and traces 0 progress resets, and `SharedSnapshotDir` is caught on seed
680 alone of the first thousand, both unchanged. A variant added later does not
compile until `Variant::bit` and `Variant::BUGS` have been given it, which is
the point of the exhaustive match; the set is a `u32`, so the vocabulary may
reach thirty-two arms before the width is a decision to revisit.

What the mechanism bought, measured rather than assumed, is one run that could
not be made before and can be made now — a server carrying `IgnoreIncarnation`
and `SharedSnapshotDir` at once, the server the nightly's seed 5909 was — and
the honest report of what that run does at the tiers run here. Seed 5909 itself
passes under the pair, as it does under each single and under the correct
server, because its schedule has moved several times since the nightly (D-042's
amendment). Over seeds 0..1000 in release the pair is caught on 1 of 1000, seed
680 by the liveness check; `SharedSnapshotDir` alone is caught on the same seed
with the byte-identical message; `IgnoreIncarnation` alone on none; and the
seeds where the pair is caught and neither single is number 0 of 1000. So
this entry ships a mechanism and a negative control that the
sweep has not yet been able to distinguish from its stream half, and says so:
the set is the thing that makes the distinction *askable*, and the question is
now asked at every tier and printed, where before it could not be posed at all.
The combined variant is pinned on the seed the sweep does catch it on rather
than asserted over a sweep tier, so no tier is made flaky by it, and the pair
rule holds on that seed as everywhere: the correct server passes it.

Three questions the documents did not answer, taken conservatively and recorded
here. A set containing `Variant::Correct` is the correct server rather than a
server with an extra bug, and `contains(Variant::Correct)` answers whether the
set is empty: the only reading under which `Correct` keeps meaning "no bug", and
the one that can never report a bug that is not there. A set's behaviour is the
conjunction of its members' and nothing more — each `contains` site reads one
bit — so composing two variants introduces no third server to document. And the
bit numbering is an implementation detail, because it is persisted nowhere; if
it ever reaches a trace or a frame, that is a new decision.

**Correction.** The Consequences' "the question is now asked at every tier and
printed" does not hold, and did not at 42ab4b5, the commit that wrote it. The pair
`{IgnoreIncarnation, SharedSnapshotDir}` has no sweep: in `sim/tests/raft.rs`
`Variants::of` appears only in two pinned seeds,
`seed_680_pins_the_combined_variant_and_the_stream_half_alone_catches_it_too`,
which asserts the pair's liveness catch on seed 680 and that `SharedSnapshotDir`
alone fails the seed with the byte-identical message, and
`seed_5909_passes_under_both_bugs_together_which_is_the_finding`, which asserts
the pair passes seed 5909. No tier sweeps the pair or prints a rate for it: every
tier, the nightly's ten thousand included, runs it on those two seeds alone. The
next sentence above, that the combined variant is pinned rather than asserted
over a sweep tier, is the accurate one, and RAFT.md §5 states it that way. The
mechanism and the pins stand.

---

## D-046 — The sweep's safety re-check keeps its state

**Context.** `sim/raft.rs`'s `advance` runs a seed in fifty-millisecond slices and,
every tenth slice, ran every safety fold over the trace from its first record:

```rust
let events: Vec<TraceEvent> = sim.trace().into_iter().map(|r| r.event).collect();
let verdict = invariants::all(&events)
    .and_then(|()| invariants::commit_majority(&events, SERVERS as usize));
```

Each look copied the whole trace — tens of thousands of `TraceRecord`s with their
message payloads — and rebuilt every check's state from nothing: the log of every
server, the leaders per term, the committed set, the applied map, the snapshot
floors, the configuration in force. A run that looks L times at a trace that grows
to N records pays O(L·N), and L grows with the run, so a seed's checking cost is
quadratic in its length. `invariants::all` multiplied the constant: six checks, four
of which replay the logs, replayed them four times per look, and `commit_majority`
a fifth.

The measurement on `main` at 37e3bad: the raft test binary took **1647.92 s at a
thousand seeds** and `scripts/premerge.sh` **29 minutes**, against a 667 s binary
before the stage-E snapshot work, which lengthened runs and so lengthened every
look; attribution at a hundred seeds put about 62% of the cost in the storm-free
sweep, whose dominant frames were `Vec<TraceRecord>::clone`, the drops of those
clones, and `Logs::replay` (issue #25).

Nothing in the checks needs the rebuild. RAFT.md §2 states log matching
inductively — the check at an append reads the logs as they stand, and no earlier
append is re-examined — and every other check is a left fold over the events with
no lookahead. The state of a check after k events is all it needs to consume event
k+1.

**Decision.** `invariants::Checker` is every check of the module with the state of
each kept across calls: `Checker::new(servers)`, `push(&TraceEvent)`,
`extend(events)` and `verdict()`. `advance` keeps one checker per run and feeds it
`Sim::trace_from(checked)` — the records since its last look (D-044) — so a look
costs its own new events and a run costs its trace once. The membership scenario's
`advance` does the same.

*One implementation.* `all` and `commit_majority` keep their signatures and their
meaning and are now the checker driven over the events and asked for one verdict, so
there is one fold per check in the workspace and no second copy to drift. The
checker also replays the logs once for the four checks that read them instead of
once each, and follows who leads once for the three checks that ask.

*A verdict per check, latched.* Each check holds `None` until its first violation
and its message afterwards, and consumes no further events once it has one: a fold
returns at its first violation, so no later event can change its answer.
`verdict()` reports the first violation in the order `all` ran the checks, the
commit-majority check last, which is what `all(events).and_then(|()|
commit_majority(events, servers))` reported. This is why `push` returns nothing: a
slice of events has no verdict of its own, since `all` reports the first violation
in *its* order of the checks and not the earliest violation in the trace — an
election safety violation at the last event outranks a log matching violation at the
first — and only a look at every check at once can answer. A replay error, which
`Logs::replay` raises for two snapshots that disagree at one index, is the first
violation of every check that reads the logs and is recorded as such in each.

*The equivalence test.* `the_incremental_checker_agrees_with_the_fold_over_the_whole
_trace` runs a hundred seeds — the gate's twenty at the gate's tier — and, at eight
prefixes of each run's trace, compares the verdict of a checker fed that trace in
chunks of 37 events against `all` and `commit_majority` folded over the whole prefix
from the first record: the same `Ok` or `Err`, and when `Err`, the same words. A
quarter of the seeds run each of `TruncateOnEveryAppend`, `SendBeforePersist` and
`CountOlderTermForCommit`, whose violations three different checks report, and the
test asserts that some compared prefix was in violation, so a comparison that agreed
only on `Ok` fails rather than passes.

*Three other whole-trace scans per slice.* `install_landing` and `install_completed`
copied the whole trace every five milliseconds of their watch and looked at its
tail; they now read `trace_from` like `flush_in_flight` (D-044) and `stream_opened`
(D-043's re-take arm) already did. `leader_now`, which every fault round asks for
the latest `RaftLeader`, copied the whole trace to read backwards over it; it now
reads back over the tail in windows that double until one holds a leader, which is
the same answer.

**Alternatives.** Checking only at the end of a run: a violation would be reported
at the end of a trace rather than near the event that caused it, the run would keep
going after it and the runaway a buggy variant produces would be bounded only by the
trace cap, which is the reason the periodic check exists. Checking a sample of the
slices, or raising `CHECK_EVERY`: it buys a constant factor and keeps the quadratic,
and it moves a violation's report further from its cause. Keeping the folds and
copying the trace once per run instead of once per look: the copy is only part of
the cost, the replays are the rest. Keeping the old folds as a second implementation
for the equivalence test to compare against: two implementations of a safety check
is how one of them comes to be wrong, and the comparison against the folds as they
stood at 37e3bad was run once, over a hundred seeds and twelve variants at eight
prefixes each, before this branch's first commit rather than for ever after. An
incremental `leader_completeness` that stops re-scanning the committed set at every
election, and an incremental `change_complete` in the membership driver: both are
linear in the run rather than in the slice, neither showed in the profile, and this
entry is about the quadratic.

**Consequences.** The raft test binary at a thousand seeds falls from **1647.92 s
to 327.57 s** on the same machine, five times faster, and `scripts/premerge.sh`
from **29 minutes to 7 minutes 26 seconds**, under the fifteen the owner asked for;
every sweep is green at a thousand seeds and every variant is caught at its
established rate, the rates identical to the run before. (The binary's figure is
the one the premerge's own run reports too, 333.62 s; a third measurement said
946.98 s and was taken while another agent's sweep had the eight-core machine at a
load average of fifty, which is what a sweep measured on a busy laptop looks like.)
A `sample` profile of the binary at three hundred seeds, 184 938 busy samples of
396 029, says what the remaining time is:

| what | share of busy samples |
| --- | --- |
| the allocator | 24.4% |
| the simulator and the server under it, everything not named below | 33.9% |
| `std::path` comparison, the simulated filesystem's `BTreeMap<PathBuf, _>` | 11.5% |
| `moirae_trace` JSON and `core::fmt`, the run's JSONL export | 6.9% |
| `memmove`/`memcpy`/`memset`/`memcmp` | 7.4% |
| `Sim::trace_from` | 4.6% |
| `Report::check`'s own scans and the linearizability search | 4.6% |
| **`invariants::Checker`** | **3.9%** |
| `Vec<TraceRecord>::clone`, the copies that remain | 2.7% |

What is left is the simulation, not the checking. Two costs this entry does not
touch and that the next measurement should look at, both outside issue #25: every
run builds its moirae JSONL export whether or not it is written, which is the 6.9%
of `moirae_trace` and most of the `core::fmt` beside it; and
`Report::isolation_keeps_the_term` scans the whole trace once per isolation at the
end of every run, a linear scan of a time-ordered trace that a binary search on
`TraceRecord::at` would bound (5442 samples on its own). Both want a backlog issue,
not a widening of this one.

The checker is public API: `invariants::Checker`, with `new`, `push`, `extend` and
`verdict`. Every check stays a function of the trace alone, so a failing seed still
replays in the studio and the pinned seeds' message fragments still hold. A run now
holds one checker's state for its whole length — the logs, the applied map and the
committed set, which the old folds built and dropped at every look — so a seed's
peak memory is a little higher and its allocation rate much lower. `Report::check`
still folds `all` and `commit_majority` from the first record at the end of every
run, over the whole trace, which is a second opinion on the incremental verdict on
every seed of every sweep: an incremental checker that missed a violation would be
caught there, on every seed, as a run that passed the slices and failed at the end.

---

## D-047 — Every trace record carries its decision time and its durability time

**Context.** The ten-thousand-seed nightly (run 34711427220, on `main` at 14c3e17)
failed the correct server on seeds 1885 and 2023 with *pre-vote: server 1 raised
its term ... while isolated*. Both windows came from `Fault::RetakeUnderStream`, but
the race is general: every isolating fault takes `from = sim.now()` and partitions
at that instant. In both, a term-raising message reached server 1 just before the
partition; the server adopted the term and persisted it; and because the node
traces a step's events only after its persist is durable (D-026: "the trace events
still follow the persist, so the trace says what is durable"), the `RaftTerm`
record is stamped just inside the window. Zero messages reached the server inside
the window. The protocol held; the check read the trace timestamp as the moment of
the rise.

The same gap produced 11 of the 17 catches the nightly printed for two variants:

| Seed | Variant | Rise traced after `from` | Cause, delivered before `from` |
|---|---|---|---|
| 1885 | Correct | +48 µs, follower | RequestVote t10 from 2, 2.53 ms before |
| 2023 | Correct | +671 µs, follower | AppendEntries t14 from 3, 2.12 ms before |
| 1252, 2509, 3087, 5990 | IgnoreIncarnation | +434 to +1600 µs, follower | RequestVote or AppendEntries, 0.31 to 16.57 ms before |
| 1176, 2407, 3863, 4713, 6691, 9670 | SharedSnapshotDir | +662 to +2753 µs, follower | RequestVote or AppendEntries, 0.60 to 2.75 ms before |
| 5203 | SharedSnapshotDir | +656 µs, **candidate** | a granting PreVoteResponse t12 from 1, 1.37 ms before — a candidacy decided before the window |

Every row: zero server-to-server deliveries to the isolated server inside the
window, and every trace regenerates byte-identically from 14c3e17.

What this entry does **not** close, said plainly: seeds 164, 385 and 7381. Those
were gaps in the checker's *rules* — the timer check not counting an
InstallSnapshot as a leader's contact (164, D-030's stanza), not knowing that an
install's switch starts a fresh timer (385, D-039), and the snapshot floor
never coming back down on a re-seed (7381, D-030's stanza) — each already closed by
its own rule and asserted by the pinned-seed audit's predicates. A record carrying
two times would not have prevented any of them: none was a record read at the
wrong one of its times.

**Decision.** A record's time is two times. `TraceRecord::at` stays what it was,
global virtual time when the event was recorded, which is when what it reports was
durable: its **durability time**. `TraceRecord::decided` is new, global virtual
time when the step that produced the event was taken: its **decision time**, at or
before `at`, and equal to it for every record traced as it happens. A check about
why a server did something, and so about what it could have known by then, reads
the decision time; a check about what was durable when reads the durability time or
the records' order. Every site is marked `D-047`.

*The environment.* A node's own clock is skewed and drifting, so the stamp comes
from the environment: `Environment::decision(&self) -> Decision`, an opaque `Copy`
stamp — global virtual time under `SimEnv`, the real monotonic clock under
`RealEnv` — and `Environment::trace_decided(&self, decided: Decision, event)`.
`trace(event)` stays and means decided now. Taking a stamp reads the time under the
simulator's lock and nothing else: no await, no poll, no draw. That it moves no
schedule is measured, not assumed. The echo scenario's pinned body hash
(`sim/tests/echo.rs`, `GOLDEN`, `19f19201df99a799`) is unchanged. The raft trace of
seed 42 from this tree is the one from 5624f24, the tree before this entry, byte
for byte once the new field is removed: 100 992 lines each, 4 744 of them carrying
the field and no other line differing. The traces of seeds 1885 and 2023 from this
tree are the nightly's from 14c3e17 in the same sense: 82 827 lines with 3 322
carrying the field, and 96 023 lines with 3 904. Under `RealEnv` the log line
carries `decided_ns` beside the event.

*The export.* A `log` line's `data` is an open object (moirae SPEC §5), so the
decision time is written there, as `decidedNs` after the event's own fields, and
only when it differs from `t`. Nothing else in the export moves: `t` is the
durability time, a trace whose records are all decided as recorded exports byte for
byte as before, and no moirae format version changes. Sends, deliveries, drops and
faults are recorded as they happen and never carry it.

*The sites in `ananke-raft`'s `node.rs`.* Stamped:

- the `raft` loop in `incarnation`: a stamp immediately before every `core.step`;
  that step's `Output::Trace` events are traced with it by `Server::execute`, after
  the persist, the sends and the reads that follow, and so is the step's
  `RaftProposed`;
- `install_decision`: the `Input::Applied` steps it takes while quiescing the
  `apply` task, the same way;
- the `apply` task: `RaftApply` stamped as the task takes each entry, before its
  synced batch; `RaftSnapshot { taken: true }` and `RaftSnapshotReused` stamped as
  it takes the take job, before the record read, the record and the checkpoint;
- the `snapshot` task: `RaftSnapshotStreams` stamped at the start of
  `Streamer::open`, before the version lookup, the sender's open and the first
  chunk; every `RaftSnapshotResumed` of a timeout pass stamped at the pass's start,
  each after the first traced behind the resends before it; the install's
  `RaftSnapshot { taken: false }` and `RaftConfig` stamped when the task takes the
  `raft` loop's `Snap::Finish`, before the repair and the staged `CURRENT`;
- re-seed mode: the install's `RaftSnapshot { taken: false }` stamped when the whole
  stream is staged, before the repair;
- `run`: both `RaftRefused` sites stamped when the open or the adoption returns the
  loss, before `record_loss` writes and syncs the lost mark.

Examined and decided as they are traced, each with its reason:

- *the restart and install restatements* in `incarnation` — `RaftTruncate`, the
  restated `RaftSnapshot`, `RaftReseeded`, the `RaftAppend`s, `RaftConfig`,
  `RaftRecovered`, `RaftTerm`. This refines the integrator's design, which named
  them as sites. They report state that was durable before the incarnation began,
  nothing a step of it decided, and the incarnation starts where they are traced:
  nothing awaits between them and the loop arming its first tick, which is where the
  new core's election timer really starts counting — the moment D-039's
  arm of the timer check reads them as. The version sweep's await before them decides
  nothing they report. A stamp taken when the core was restored, before that await,
  would put the fresh timer earlier than the server has it and make the timer check
  stricter than the protocol by the sweep's disk time;
- `RaftSnapshotResumed` on an acknowledgement and `RaftInboxDropped` in the `net`
  task: no await between the decision and the record;
- `RaftServerFailed`: the failure is known when the await that fails returns;
- the `core.step(Input::Applied(..))` at an incarnation's start, whose outputs are
  discarded and trace nothing.

Two open points, taken conservatively: `RaftAdopted` and `RaftSnapshotDeleted` are
decided inside `adopt_staged_under` and `snapshot::sweep_versions`, after reads those
helpers make and in the same calls that do the work, so no stamp taken outside them
could be the decision's own. They are recorded as decided when traced, since no
earlier time is provably theirs; a stamp returned from inside the helpers is a
signature change of public functions for records no check reads by time.
`ananke-storage` traces as its events happen and is untouched.

*The classification.* Every check and predicate that reads a record's time:

| Check | Where | Reads | Why |
|---|---|---|---|
| Election safety, log matching, leader completeness, state machine safety with its `RaftRecovered` accounting and snapshot floor, committed entries stay, commit by current term | `invariants::all`, `invariants::Checker` | record order | folds over what became durable, in the order it did; a restatement accounts for what was durable at a crash |
| Commit by majority | `invariants::commit_majority` | record order | an entry counts as held by a server once its `RaftAppend` is traced, which is after the persist: durable on a majority when committed |
| The sliced re-check (D-046) | `advance` in `sim/raft.rs` and `sim/membership.rs` | record order | the same folds, and its equivalence test compares events |
| Linearizability: invocations and returns | `lin::History::from_trace` | one time | the clients trace as it happens |
| Linearizability: an abandoned operation returns at its entry's apply | `lin::History::from_trace` | durability | when the effect was durable, the latest it can have become visible: the conservative upper bound; its decision time could end the operation before a read that could still miss it |
| Pre-vote: the term at an isolation's start and at its heal | `Report::isolation_keeps_the_term` | **decision** | why the term moved: a rise decided before the isolation is not the isolated server's election |
| Pre-vote: the skip for a refusal, re-seed or install in the window | same | durability | stands in for a restatement landing in the window, which is traced after the event it looks for is durable |
| Timers fire | `Report::timers_fire`, `Report::replay_timers` | **decision** | whether a server campaigned in time is when it decided to; the records are replayed in decision order, so no bound is measured past a reset the server had already made |
| Seeds 164's and 385's predicates | `snapshot_fed_timer_gaps`, `timer_gaps_rescued_by_restatement` | **decision** | the same replay as the check, so the two cannot drift apart |
| The leader in force at the last heal | `Report::leader_at_last_heal` | **decision** | who led by the heal is what the servers had decided by it; folded in decision order, since two servers' elections can be traced in the other order (on `SharedSnapshotDir` seed 367, term 2's leader was decided 0.6 ms before term 3's and traced 1.7 ms after it) |
| Liveness: the first write after the last heal | `time_to_write_after_heal`, both scenarios | one time, or durability | a client's return, or an abandoned write closed at its durable apply |
| Availability gap | `membership::Report::longest_completion_gap` | one time, or durability | the same history |
| A majority up, a change completed | `majority_up`, `change_complete` | record order | no time read |
| Seed 7381's predicates | `floor_lowering_installs`, `recoveries_under_a_lost_floor` | record order | the floor fold reads no time; the time reported is the record's |
| Seed 6325's adoption windows | `adoption_windows` | durability | a crash inside the disk work between an install being durable and its adoption being durable |
| Seed 687's restarts | `restarts_after_lost_state_refusal` | record order | a restart after the refusal as recorded |
| Snapshot takes | `snapshot_takes` | durability | pairs a take's record with the last checkpoint written on its node and not yet claimed; a crash or a `RaftTruncate` discards an unclaimed one. It asked for the two records to carry the *same* instant until D-060 put an awaited write between them, after which it paired nothing on any seed |
| Re-takes under streams | `retakes_under_streams` | durability, record order | a take as recorded against chunks sent and openings as recorded, the order the audit measured in |
| Uncounted followers, the duplicate-chunk loop | `uncounted_after_heal`, `duplicate_chunk_loop` | one time | sends and deliveries |
| Stale progress; refusal, reset, re-seed | `stale_progress`, `refusal_reset_reseed` | record order, durability | a refusal as recorded against the messages after it; the reset answers a rejection the refused server sends only after its refusal is recorded |
| The pin helpers | `assert_no_stream_wedge`, `took_an_index_twice`, `crashes_while_refused`, `retook_at_one_index`, `reseed_completed`, `Coverage` in `sim/tests/raft.rs` | durability, record order | what the audit measured, as recorded (`assert_stream_wedge` was replaced by `assert_no_stream_wedge` when the wedge stopped being reachable on a pinned seed) |

The fault drivers — `leader_now`, `install_landing`, `install_completed`,
`stream_opened`, `refreshed_refused`, `flush_in_flight`, `adoption_change` — read
records' presence and order, never a time, and steer the schedule; they are
unchanged. Two refinements of the integrator's reading, with their reasons: the
pre-vote skip reads the durability time, above, so the check by durability time is
the check as it stood word for word; and `leader_at_last_heal`, a pinned-seed
predicate, is causality and reads decision time.

The pre-vote check and the timer replay each take the time they read as a
parameter, `sim::raft::RecordTime`: `Report::isolation_keeps_the_term_by` and
`Report::timer_gaps_by`. The check `Report::check` makes is the decision-time
instance; the durability-time instance is the check as it stood. A pinned seed
shows both.

*The supersession.* This entry supersedes the part of D-026 on which a check may
read a trace record's timestamp as when the thing it reports happened. The trace
still says what is durable, through `at`; D-026's order of execution is unchanged.
D-026 itself is not edited; its forward pointer is added when this entry is
approved.

*The pins.* Seeds 1885 and 2023 are new pinned tests asserting the straddle itself
— the isolated server's one term rise has `decided < from <= at <= until`, with no
server-to-server delivery to it in `(from, until]` — the check by durability time
failing with the nightly's message word for word, the same check by decision time
passing, and `check()` passing. Each also ties the decision time to its cause: the
one message from a server delivered to server 1 at the rise's decision instant is
the term-raising message — server 2's RequestVote of term 10 on 1885, server 3's
AppendEntries of term 14 on 2023 — so a stamp taken anywhere else before the window
fails the pin. Measured on this tree: seed 1885's rise to term 10
decided 2.531 ms before the partition and traced 48 µs after it; seed 2023's to
term 14, 2.121 ms before and 671 µs after. One test runs the eleven variant pairs and
asserts the same straddle of each, that none reports a pre-vote violation, and that
every one of the eleven now passes `check()` outright. Their steps were decided 0.262 (1252),
1.718 (2509), 1.993 (3087), 0.312 (5990), 2.198 (1176), 0.666 (2407), 0.275 (3863),
0.653 (4713), 1.533 (6691), 0.596 (9670) and 1.372 ms (5203, the candidacy) before
their isolations. Seeds 164, 385 and 7381 are re-verified: each pin passes on this
tree, and each comment says in a sentence why this entry does not bear on it. A
port of the replay over the JSONL, reading `decidedNs`, gives on this tree's traces
what the Rust gives — no gap under either reading — and the audit's figures in the
164 and 385 comments (a 161.6 ms longest AppendEntries-less stretch holding a chunk;
a 167.1 ms, 55.3 % stretch across a restatement) are the same under decision time.
On the traces the seeds originally failed with, which carry no decision times, both
predicates still fire — seed 164's one gap from 12.9405 s flagged at 13.3397 s with
21 chunks, seed 385's from 14.0308 s flagged at 14.3350 s — and neither could have
been moved by stamps: the reset that ended 164's gap, a granted vote, was traced
30.4 ms after the flag, against a largest lag between a vote's decision and its
record of 6.89 ms over the correct server's first 3 000 seeds, and 385's, a pre-vote campaign, 22.9
ms after the flag, and a pre-vote campaign persists nothing and carries no separate
decision time. Seed 7381's predicates are a fold over record order and still find
the index-65 replay under a floor of 128 on its original trace.

**Alternatives.** *Checker-side causal matching of each rise to the delivery that
caused it*: the checker would re-implement the core's term rule and the inbox —
which message a step took, behind which persist, past which drops and duplicates —
as a second implementation to drift from the first, and would still not know when
the step was taken; the server knows that, so the server says it. *Recording an
isolation's `from` only once the victim has no step in flight*: a fault driver
waiting on the server's internals is a fault model of the implementation rather
than of the world (D-012), it moves every seed's isolation instants and with them
every schedule, and it hides the race from the checker instead of letting the check
see both times. *A top-level moirae field*: a format-version bump in moirae and a
new `moirae-trace` release for ananke to consume, and publishing is not open to this
branch; the checks read `TraceRecord`, not the export, so nothing is bought by it.
*Stamping the restatements when the core is restored*: see the sites above — it
would place the fresh timer earlier than the server has it. *Tracing before the
persist*: it would make the trace say what is intended rather than what is durable,
which commit by majority and `SendBeforePersist`'s catch depend on.

**Consequences.** `Environment` gains two required methods, and both
implementations carry them; `TraceRecord` gains a field; the raft scenario's JSONL
gains `decidedNs` on the records of every persisted step, while the echo, WAL and
engine traces are unchanged. The pre-vote check and the timer check read decision
time, so the class of 1885 and 2023 — a rise decided before an isolation and traced
inside it — and the eleven variant catches of it are no longer catches.

At a hundred release seeds every rate is the rate of the tree before this entry
(5624f24), and no rate changed: `NoPreVote` 100 of 100, now also counted by the
pre-vote check, 100 of 100 — its rises are its own campaigns, decided inside the
isolation — `SendBeforePersist` 100, `TruncateOnEveryAppend` 100,
`ApplyBeforeCommit` 87, `CountOlderTermForCommit` 52, `SnapshotWithoutCurrentLast`
36, `ResetTimerOnAnyRpc` 33, `SingleMajorityInJointConsensus` 29, `AdoptionAsBuilt`
8, `RefusalNotDurable` 3, `IgnoreIncarnation` 0 and `SharedSnapshotDir` 0, the lease
trials' 52 exceeded seeds with 8 stale reads caught without the guard, and the
correct server's coverage and the membership scenario's field for field. The
incremental checker's equivalence test and the parallel driver's trace-identity test
pass. None of the thirteen seeds lies in the first hundred, which is why no rate
moved there.

`SharedSnapshotDir`'s ten-thousand-seed assertion counted every catch, the
seven timing artefacts and two linearizability budget exhaustions (seeds 1262 and
7222, which prove nothing) among them. It now asserts at that tier that the
liveness check — the wedge itself — caught it, and prints the catches by check at
every tier.

*At ten thousand seeds* (nightly run 34731272921, on `bd93ed3`, this entry's second
commit) every test passed. Against the nightly on `main` at 14c3e17 (run
34711427220): the correct server passed every seed, where that run failed it on
1885 and 2023; `IgnoreIncarnation` 0 catches, from 4; `SharedSnapshotDir` 6, from
13 — 4 by liveness and 2 by linearizability, the counts the earlier run's
breakdown gave for those checks — so the seven pre-vote artefacts are gone; `NoPreVote` 9 999, unchanged, all 9 999 by the pre-vote check.
Unchanged too: `SendBeforePersist` 10 000, `TruncateOnEveryAppend` 9 995,
`CountOlderTermForCommit` 4 413, `SingleMajorityInJointConsensus` 2 720,
`RefusalNotDurable` 132, the lease trials' 472 stale reads in 5 023 exceeded seeds,
all 5 023 revoked, and the three engine variants' rates. **Four rates fell that the
thirteen seeds do not account for**: `ResetTimerOnAnyRpc` 3 462, from 3 470;
`AdoptionAsBuilt` 646, from 651; `ApplyBeforeCommit` 8 902, from 8 903; and
`SnapshotWithoutCurrentLast` 3 303, from 3 304 — fifteen seeds. That run printed only
each variant's first catch. Every sweep of the raft scenario now also reports, per
seed, the catches reading decision time removed and the catches it added
(`Report::moved_by_decision_time`, which derives the verdict by durability time from
the check's own without re-running the linearizability search), and asserts that every
pre-vote catch it removed is on a run with a term rise straddling an isolation's
start — the only way reading a rise earlier can remove one. A timer catch removed is
printed with the flagged server's decisions straddling the flag. The pinned straddles
assert that report names each of them.

*The second ten thousand* (nightly run 34749071877, on `9b5995d`) passed with every
rate as above, and the report named what moved: **28 catches removed, 0 added**, the
same on every sweep. The correct server's 2 (1885, 2023), `IgnoreIncarnation`'s 4 and
`SharedSnapshotDir`'s 7 are the thirteen. The fifteen are `ResetTimerOnAnyRpc` 2627,
4426, 4814, 5051, 5153, 5879, 5918 and 6717; `AdoptionAsBuilt` 1929, 2578, 2698, 5859
and 9557; `SnapshotWithoutCurrentLast` 2305; and `ApplyBeforeCommit` 6366. Of the 28:

- **27 are pre-vote catches, every one this entry's straddle**: the isolated server's
  term rise decided 11.7 µs to 2.53 ms before the isolation began, traced 48 µs to
  3.36 ms after it, and no message from a server delivered to it in the window. In 25
  the decision instant is the delivery of the message that carries the new term — a
  RequestVote or an AppendEntries of that term for a follower, a PreVoteResponse for
  the two candidacies (5203, 2578) and for one step-down (6366, from 11 to 13). In the
  other 2 (`SharedSnapshotDir` 3863, `IgnoreIncarnation` 1252, measured on their
  traces) the decision instant is a re-seed install's completion: the new incarnation's
  first step took an AppendEntries of the new term that had been delivered 2.48 ms and
  16.31 ms earlier and waited in the inbox behind the install, and its restatement is
  traced at that same instant, before the window, so the pre-vote check's skip does
  not apply.
- **1 is a timer catch**, `ResetTimerOnAnyRpc` 5153, measured on its trace: server 2,
  pre-candidate at 7.8649 s, granted server 3's RequestVote of term 7 at its delivery at
  8.262757 s, inside its bound; the vote and its term were durable and traced at
  8.265322 s, the first record past the bound, which is where the check by durability
  time flagged it. The check's own rule counts a granted vote as a reset.

So on the tier the gap was found on, reading decision time removed only catches of the
gap and added none, and the correct server passes every seed. Two limits of that
evidence: the sweep asserts a removed pre-vote catch shares its run with a straddle
rather than matching the two, which the printed report does for all 27; and a removed
timer catch is printed, not asserted. Asserting both is issue #33.

Three open points besides the two sites above. A term-raising message delivered
before an isolation but *stepped* inside it — queued behind a persist — is decided
inside the window and would still be flagged by the pre-vote check with no delivery
in the window; closing it would take the causal matching rejected above, and it is
open as issue #32. The nearest measured case is the two above, queued behind an
install and stepped just before the window; no run of the correct server at ten
thousand seeds was flagged by the check by decision time, so none reached it. The
timer check's resets for a campaign, a granted vote or a step-down now land at the
step rather than after its persist, which makes the check stricter by that
persist, at most 6.91 ms (a term record; 6.89 ms for a
vote) over the correct server's first 3 000 seeds, and more lenient by never
measuring a bound past a reset already decided. No seed of the hundred moved either
way; over those 3 000 seeds the gap lists are identical under both times, and under
`ResetTimerOnAnyRpc` 27 of 400 seeds show a gap whose start or flag moved by at most
about 3 ms, none gaining or losing a gap; at ten thousand seeds the report above
removed one timer catch and added none. And RealEnv's stamps are the process's
monotonic clock, comparable only within one process, which is all a log line needs.

**Superseded in part by D-049.** Two of the eleven variant pairs the pin runs,
`IgnoreIncarnation` on seeds 2509 and 5990, no longer reach the straddle. On each,
D-049's check quorum steps the leader down before the isolation the straddle was at,
leaving uncounted a refused follower that D-042's bug keeps it from re-seeding, and the
run after that step-down is another run; the pin asserts the straddle absent and names
the step-down. The other nine are asserted as above.

---

## D-048 — D-030's account of seeds 164 and 7381 is superseded by the record

**Context.** D-030's *What the sweep found before it passed* gives seed 164 as "the
ten-thousand-seed nightly, seed 164, the first correct-server failure the sweep
ever produced": a follower "two hundred entries behind", flagged by the timer check
"while the leader was reaching it every few milliseconds". It gives seed 7381 as
"of the same run". The pinned-seed audit (7e5fbd4, 60a0f33) measured seed 164 on
the trace it failed with, and D-039's Context and the seed's pin carry what it
found. D-030, the entry that recorded the seed, does not, and an accepted entry is
superseded, not edited.

*Where the seeds came from.* Seed 164 came from a local ten-thousand-seed run on
1373601 (the same tree as 48e5276), Run 1 of the overnight session in
`docs/OVERNIGHT.md`, not from the GitHub nightly. Seed 7381 was not of that run: it
came from the nightly, run 34496762339 on ea6fe7d, one of that run's three failing
seeds with 5909 and 6325.

*Which failure it was.* Not the first correct-server failure the sweep ever
produced: D-026 records seeds 42, 2, 38 and 16 from stage B, and D-030's own
stanza seeds 9, 8 and 60. It was the raft sweep's first correct-server failure at
ten thousand seeds; the nightly's runs on `main` through stage C, 34020438211 on
10809b3 and 34453669517 on 9084c13 among them, passed the correct server at that
tier.

*How far behind.* Server 2 had appended through index 208 when server 3, leading
term 12 with its log through 276 and its snapshot at 210, began feeding it
`InstallSnapshot` chunks at 11.854 s: about sixty-eight entries behind, not two
hundred.

*What reached it.* The gap the timer check flagged on server 2 ran from 12.9405 s
and was flagged at 13.3397 s, against a 399.19 ms bound. Nobody led in it: server 3
had lost its quorum at 12.936 s and was pre-voting, with no leader in the cluster,
and the 21 chunks server 2 received in the gap came from server 3 after it lost
its quorum, the deposed leader's leftover stream. No leader was reaching server 2
in the stretch the check flagged.

**Decision.** The account above supersedes D-030's account of seed 164 — where it
came from, which failure it was, how far behind the follower was and what reached
it — and D-030's "of the same run" for seed 7381. D-030's text is not edited and
carries no pointer: the supersession is recorded here, in the later entry, as
D-021, D-022 and D-023 record theirs. No rule changes: the timer check's
InstallSnapshot arm, D-039's restatement arm and the exact snapshot floor D-030
records for seed 7381 stand as built.

**Alternatives.** Editing D-030's stanza in place: accepted text is superseded, not
rewritten, and the owner ruled so for this stanza. A forward pointer on D-030: an
edit to accepted text as well; the one pointer added to an entry that was already
accepted, D-026's, is the one D-047's own text promised on its approval, and the
pointers on D-036 to D-039 were added as those entries were accepted. Leaving the
correction where the audit put it, in D-039's Context and the pin: the log would
hold two accounts of one seed with nothing in the entry that recorded it saying
which stands.

**Consequences.** D-039 points here. The code agrees with this entry on where each
seed came from, how far behind server 2 was and what reached it: the comments on
`seed_164_which_a_local_ten_thousand_seed_run_found_stays_green` and
`seed_7381_which_the_first_nightly_found_stays_green` in `sim/tests/raft.rs`, and
`Report::snapshot_fed_timer_gaps` and the replay's InstallSnapshot arm in
`sim/raft.rs`. Two phrases there are looser than this entry. Seed 164's comment
calls it the first correct-server failure a ten-thousand-seed run produced, without
"of the raft sweep": the engine sweep's `the_correct_engine_passes_every_seed` had
failed at ten thousand seeds on 2026-09-05, in run 33967250798 on 5112a8b. And seed
7381's test name calls run 34496762339 the first nightly, though the nightlies on
`main` named above ran before it. `docs/OVERNIGHT.md`'s copy of the account is
corrected in place, with a note pointing here. Seed 164's schedule has since moved
away from the situation — a different leader is elected after its 9.691 s
partition — and its pin asserts the situation absent.

**Superseded in part by the owner's ruling of 2026-09-14.** The Decision's "carries
no pointer" and the Alternatives' rejection of a forward pointer on D-030 no longer
hold. The owner ruled that an accepted entry superseded in part gets a forward
pointer, never an edit, and D-030 now carries one pointing here, as D-026 carries
one to D-030 and D-045 a correction of its own Consequences. The account of seeds
164 and 7381 above stands, and so does the rule that accepted text is not
rewritten.

---

## D-049 — Check quorum counts a refused follower only while its re-seed progresses

**Context.** Check quorum (D-028, RAFT.md §1) steps a leader down when a minimum
election timeout passes without answers from a majority, so that a leader cut off from
its followers stops serving. As built it counted any AppendEntries response of the
leader's term, whether the entries fitted or not (moirae rule 5). Stage E made one such
answer different in kind. A refused server (D-030, RAFT.md §3) has no store: it answers
every AppendEntries, whatever its term and entries, with a rejection carrying echo 0 and
store incarnation 0 (D-042), and it goes on answering whatever becomes of its re-seed.
So a leader whose other follower was away kept its office on a refused follower's
rejections for as long as its re-seed stream to that follower stayed stalled — chunks
lost, a checkpoint the receiver cannot use, a stream not yet opened — though that
follower counts for no commit until its install completes. The answers check quorum
counted no longer said that the leader could reach a majority it could commit with.

The owner decided the rule below. The alternative weighed beside it was to count nothing
from a refused follower.

**Decision.** A refused follower's rejection counts for check quorum only while the
leader's re-seed stream to that follower has had a chunk acknowledged within the
check-quorum window. With no re-seed progress in the window the follower does not count,
and a leader whose majority needed it steps down. Every site is marked `D-049`.

*The rule in the core.* `Progress` keeps three marks for the window since the last
check: whether the follower answered from a store (`active`: any AppendEntries response
but a rejection stamped incarnation 0), whether it answered as a refused server does
(`refused_answered`: such a rejection), and whether the stream to it had a chunk
acknowledged (`stream_acked`). The check, `heard_this_window`, counts a follower with the
first, or with the second beside the third, and clears all three. The window is D-028's,
`election_ticks.0` ticks of the leader's clock. `RaftQuorumLost` gains `uncounted`, the
followers that answered in the window only as refused servers and were not counted,
which the moirae line carries only when it is not empty. A quarantined follower (D-035)
answers from its store, with echo 0 and its own incarnation, and counts as any follower
does: the rule is about the refused server's rejection alone.

*The signal.* Chunk acknowledgements go to the `snapshot` task, not the core (RAFT.md
§1), so the core had no evidence of re-seed progress. The task now marks a follower when
an acknowledgement takes that follower's stream past the furthest point any
acknowledgement had taken it, in a set it shares with the `raft` loop. The loop takes the
set before every tick and steps `Input::SnapshotAcked { to }` into the core for each
follower in it, each step with its own decision stamp (D-047). The install's answer
already reaches the core as `Input::SnapshotInstalled`, and marks the stream acknowledged
too. A mark is a lock and an insert: no event on the inbox, no task woken that would not
have woken anyway.

*The variants.* `Variant::RefusedCountsForQuorum` counts a refused follower's rejection
whatever the stream does: the leader as built. `Variant::RefusedNeverCounts` counts
nothing from a refused follower: the rejected alternative. In one set, the first's
counting wins.

*The scenario.* `sim/quorum.rs` asks two halves on every seed at every tier, of three
servers and two clients. A leader takes three hundred puts of 400 bytes, so its
checkpoint streams in many chunks; a follower is crashed and restarted with its store
directory's marker saying the store lost state (D-044), so its open is refused on the
mark on every seed; and the moment the leader opens its re-seed stream to it, the
leader's other follower is cut off for 1.5 s, fifteen windows. In the *blocked* half the
same instant puts a path-MTU black hole of 1 024 bytes on the leader-to-refused
direction, which loses every chunk and passes the heartbeats and their rejections; the
leader must step down within two windows and three ticks of the cut, by its own clock —
an acknowledgement in flight at the cut can make the next window count the follower, a
tick for its delivery and one for the tick that hands it to the core, and one more for
the check's own — naming the refused follower in `uncounted`. In the *open* half the
stream runs; the leader must keep its office until the refused follower starts on the
store its re-seed built, and then commit an entry of its term past its commit index at
the cut, with no step-down before that commit. Both halves check the invariants, commit
by majority and linearizability over the trace. The scenario drops no message at random
and rots no block, since a stream that stalls on loss for a window is a step-down the
rule asks for; schedules every seed uniformly, since both halves are claims about time
(D-016); and runs on a disk that takes no time, for the reason under *What the scenario
found*. `RefusedCountsForQuorum` keeps its office through the blocked half's whole hold,
and `RefusedNeverCounts` steps down mid-re-seed in the open half and, the re-seeded
server never voting and the other follower away, commits nothing after the install.
Measured in release: the correct leader passes both halves on 100 of 100 seeds and on
1 000 of 1 000, stepping down in the blocked half at most 2.000 windows after the cut at
a hundred seeds and 2.057 at a thousand, and in the open half seeing the refused
follower re-seeded at most 7.09 and 7.35 windows after the cut and committing through it
at most 12.51 and 14.26; `RefusedCountsForQuorum` is caught on 100 of 100 and 1 000 of
1 000, and `RefusedNeverCounts` on 100 of 100 and 1 000 of 1 000, stepping down at most
1.995 windows after the cut. Each test asserts the catch on every seed at every tier.
`crates/ananke-raft/tests/paper.rs` states the rule against the bare core: progress every
window keeps the office, none steps down within two windows naming the follower,
progress in one window does not carry into the next, acknowledgements with no rejection
count nothing, a quarantined follower counts, and each variant does what it names.

*The simulator.* `Sim::limit_frames(from, to, max_len)` loses every frame longer than
`max_len` bytes on that direction of a link, at its send and at its delivery, until the
next heal: `LinkLimited` and `LinkUnlimited`, each loss `MessageDropped` with
`DropReason::Oversized`, and in the moirae export `ananke.link.limited`, `.unlimited` and
the drop reason `oversized`. A blocked direction could not have blocked the stream alone:
blocking leader to follower loses the heartbeats and with them the rejections, and
blocking follower to leader loses the rejections. SPEC §1.4 gains the fault.

*Choices made in implementation.* The owner fixed the rule and left these open; each took
the conservative reading, for these reasons. **A chunk acknowledged** is an
acknowledgement that takes the stream past the furthest point any acknowledgement had
taken that stream: a duplicate, the answer a resend gets and ground covered again after a
restart are not progress, since none brings the install nearer than it was. **A restart
from offset 0** does not lower that point, so a stream the receiver started over is
progress again only once it passes where it had been. A stream opened afresh starts its
own count: the leader gives a stream up after eight resends fifty milliseconds apart with
nothing acknowledged, over four windows, by which time a leader that needed the follower
has already stepped down. **The `Installed` answer counts** as a chunk's acknowledgement:
it is the receiver's answer to the final chunk, and a checkpoint that fits in one chunk
has no other, so leaving it out would make every one-chunk re-seed the rejected
alternative. **The window** is check quorum's own and discrete: the rejection and the
progress must fall in the same window, and progress does not carry into the next.
**Progress alone is not an answer**: the rule counts the rejection, so a window with
acknowledgements and no rejection does not count the follower. **The rejected
alternative became a variant**, `RefusedNeverCounts`, which the open half catches:
without a server that fails the open half, nothing shows that half can fail (the pair
rule, CLAUDE.md). **The random sweep gets no trace check.** A check reading deliveries
could flag only a leader in office more than three windows after its majority last
counted — the check's own window, one more for where the leader's windows fall, and one
for a delivery's way to the core — and at a thousand seeds the correct server's sweep
left a leader's majority needing a refused follower for more than two windows before the
install in 2 of 1 372 completed re-seeds, and stepped a leader down for such a follower
once (seed 350, below): too rare for `RefusedCountsForQuorum` to be caught there, and a
check would assert only what the directed scenario asserts on every seed. **The signal is a shared
mark, not an inbox event**: an event for each acknowledgement would wake the `raft` task
on every stream of every seed and move every schedule with a stream in it, where the mark
moves only the schedules the rule itself changes.

**What the scenario found.** On the sweep's own disk, every operation taking a tenth of a
millisecond to two, the open half fails on most seeds under the correct server and under
`RefusedCountsForQuorum` alike: 87 of 100 seeds under each, the same 87, and 827 of 1 000
under each. Under the leader as built, 87 and 822 of those were a step-down with nothing
uncounted, none named the refused follower, and 5 at a thousand seeds were no step-down
at all — the leader kept its office and the re-seed did not finish within the hold, on
seeds 117, 186, 204, 408 and 445. Under the correct leader 86 and 812 were a step-down with
nothing uncounted, 1 and 14 named the refused follower, where the stream itself stalled
for a window on the slow disk, and 1 at a thousand, seed 408, was no step-down. A refused
server answers nothing at all while it verifies the stream it staged, repairs it, adopts
the install and opens the store it built: its re-seed loop is busy, and the heartbeats
wait for the next incarnation. Measured over the thousand seeds of that half, the same
under both leaders, from the final chunk's delivery to the `Installed` answer's (the
verification and the repair) took a median 58.1 ms, from the `Installed` answer to
`RaftAdopted` 59.0 ms, and from `RaftAdopted` to `RaftReseeded` 65.2 ms; the whole
silence, from the refused follower's last rejection to its first answer from the new
store, took 115.2 ms to 373.4 ms, median 185.6 ms, against a window of 100 ms by the
leader's clock. On seed 0 the last rejection reached the leader 75.6 ms after the cut, the
`Installed` answer at 135.7 ms, the adoption was traced at 197.8 ms and the first answer
from the re-seeded store arrived at 267.7 ms; the leader stepped down at 219.2 ms with
nothing uncounted. No way of counting a refused follower's answers covers a follower that
sends none, so the leader loses its office in that silence under the rule as built, the
rule decided and the rule rejected alike, and with the re-seeded server never voting
nothing commits until the other follower returns. That is the re-seed's own cost, not
this rule's: the halves are asked on a disk that takes no time, and
`on_the_sweeps_disk_the_install_silence_deposes_the_leader_under_either_count` prints the
figures above, with the seeds that did not step down, and asserts from a hundred seeds
that the silence still deposes the leader as built.

**Alternatives.** *Nothing from a refused follower counts.* Rejected. The owner's reason
was that it would leave a cluster leaderless mid-re-seed whenever the third server is
away; measured, that holds on most re-seeds and not on all, and most of the rule's
advantage over it is during the stream, since both lose the leader in the silence after
it on the sweep's disk. The figures are measured on this tree; the earlier throwaway
script's were not used. The measure is `raft::Report::reseed_episodes`, which the correct
server's sweep prints as `re-seed episodes (D-049)`:
`ANANKE_SEEDS=1000 cargo test --release -p ananke-sim --test raft the_correct_server_passes_every_seed -- --nocapture`.
An episode runs from the first rejection stamped incarnation 0 that a follower answered a
leader with in its term to that follower's `Installed` answer, or to the end of the
leader's tenure; a completed episode is followed on through the adoption to the
follower's first answer from its new store, or to the tenure's end if that comes first.
Re-seed progress in it is what the leader's code counts, reconstructed from the trace: an
acknowledgement that takes the stream the leader has open past the furthest point it had
reached, and the `Installed` answer, with a freshly opened stream starting its own count
(`raft::StreamProgress`). Each completed episode is measured as if the other follower had
been away for the whole of it, against twenty evenly spaced placements of the leader's
check-quorum windows, since where its windows fall is its own ticks' business: the share
of placements in which a window lying inside the stretch finds nothing to count is the
chance the leader would have stepped down in it; the sum of those shares is the expected
number of step-downs, and an episode in which every placement finds one is a certain
step-down.

Over 1 000 seeds, 1 407 episodes, 1 372 of them completed, with a median length to the
install of 2.56 windows and a longest of 56.6; 1 299 of those reached an answer from the
new store within the tenure, and the rest are measured to the tenure's end, the adoption
stretch a median 1.50 windows and a longest 24.6.

| Counting the refused follower | expected, to `Installed` | certain | expected, through adoption | certain |
|---|---|---|---|---|
| nothing (rejected) | 1 158.05 (84.4 %) | 947 (69.0 %) | 1 344.10 (98.0 %) | 1 329 (96.9 %) |
| rejection beside progress (decided) | 327.40 (23.9 %) | 215 (15.7 %) | 1 261.30 (91.9 %) | 1 047 (76.3 %) |
| every rejection (as built) | 100.30 (7.3 %) | 45 (3.3 %) | 1 251.80 (91.2 %) | 1 026 (74.8 %) |

Over 100 seeds, 148 episodes and 143 completed (138 reaching a store answer), median 2.60
windows and longest 27.4 to the install: to `Installed`, expected 123.15 counting nothing
(103 certain), 37.05 decided (26), 12.40 as built (6); through adoption, 140.90 (140),
133.05 (112) and 131.60 (107).

So, with the third server away for a re-seed, counting nothing would have deposed the
leader before the install completed on an expected 84 % of re-seeds, 69 % certainly,
where the decided rule would on 24 %, 16 % certainly: 830.65 fewer expected step-downs in
1 372, which is where the rule earns its keep. Carried through the adoption the gap is
82.80 expected step-downs, 6.0 points of 1 372 — 98.0 % against 91.9 % — because on the
sweep's disk the adoption's silence deposes the leader under every rule, the rule as built
included at 91.2 %. The decided rule's own cost over the rule as built is 227.10 expected
step-downs to the install, windows in which a rejection arrived but no acknowledgement
moved the stream, and 9.50 through adoption. That the cost needs the other follower away
is the configuration's, not the sweep's: this sweep left a leader's majority needing the
refused follower for more than two windows before the install in 2 of the 1 372 completed
re-seeds, seeds 194 and 263, and in 0 of the 143 at a hundred seeds; the directed scenario
builds it on every seed.

*A progress horizon longer than the window* — an earlier prototype counted progress for
forty-five ticks — keeps a leader through a stalled stream for up to four windows more:
not the rule. *An acknowledgement counting as an answer on its own*: the rule counts the
rejection. *Counting the `Installed` answer as contact through the adoption that follows*:
a leader kept in office through a silence by an answer that came before it, the rule
stretched past its text, and a way of hiding the silence's cost measured above. *An inbox
event for each acknowledgement*, and *a trace check on the random sweep*: above.
*Leaving the refusal to the disk's rot*: a refusal left to rot lands on some seeds and not
others, and each half is asserted on every seed.

**Consequences.** What the rule gives up: a leader kept alive by a re-seed stream is a
leader that cannot commit until the install completes. With its other follower away it
holds its office while the stream runs and serves no write, and its clients wait on it
rather than being told to look elsewhere; in the open half the install and the first
commit through the re-seeded follower came up to 7.35 and 14.26 windows after the cut at
a thousand seeds. And a leader whose stream fails to move in a window, or has not yet
opened, steps down, where the leader as built stayed in office: with the re-seeded server
never voting (D-035), no leader forms until the other follower returns — an expected
327.40 step-downs to the install against 100.30 as built over the 1 372 re-seeds above.
Neither rule keeps the leader through the adoption's silence on the sweep's disk: through
it, the three rules lose the leader on 91 % to 98 % of those re-seeds, and a leader kept
in office by its stream still steps down when its follower goes silent to install.

`Progress` carries two more flags, the core one more input, `RaftQuorumLost` one more
field and the simulator one more fault; the `snapshot` task and the `raft` loop share a
set of followers. Nothing moves on a run whose leader never leaves a refused follower
uncounted. Measured against the tree before this entry (`origin/main`, cd411b4, whose
code is dc603ea's), every trace of the correct server over the first hundred seeds is
byte-identical, and so is every pinned seed's trace under the server its pin runs, but
two: `IgnoreIncarnation` on seeds 2509 and 5990. Over a thousand seeds, per seed, the
traces that differ are exactly the runs with a step-down leaving a refused follower
uncounted: 1 of 1 000 under the correct server, 64 under `IgnoreIncarnation`, 6 under
`SharedSnapshotDir`; in each, the first differing record is that step-down.

The rates and coverage, against the hundred-seed run of cd411b4 and the thousand-seed
premerge of dc603ea: at a hundred seeds every rate and every coverage field is unchanged
but one, `IgnoreIncarnation`'s refused follower re-seeded and applying again on 69 seeds,
from 67; the correct server's coverage gains `step_downs_uncounting_refused: 0`. At a
thousand seeds every catch rate is unchanged. Three things moved, each by a seed the rule
stepped a leader down on. The correct server's coverage moved by seed 350 alone: leader 3
of term 8 had server 1 silent in the adoption of a live-store install and server 2
refused at 16.417 s after a crash of the refusal storm; server 2's first refused rejection
arrived at 16.429 s, before the leader had opened a stream to it, and the check at
16.431 s stepped the leader down with server 2 uncounted, where the leader as built kept
its office. From that step-down the run differs, and it passes: `quorum_losses` 4 720
from 4 719, `step_downs_uncounting_refused` 1, and — the rest of that run — `duplicates`
442 693 from 442 742, `drops` 479 548 from 479 593, `commits` 1 015 357 from 1 015 410,
`applies` 1 380 651 from 1 380 708, `snapshots_taken` 38 439 from 38 440,
`snapshot_resumes` 65 114 from 65 115, `snapshot_versions_deleted` 34 941 from 34 942,
`snapshot_streams_at_once` 695 from 696, `compactions` 35 381 from 35 383,
`progress_resets` 2 403 from 2 404, `puts` 299 858 from 299 870, `deletes` 81 026 from
81 030, `cas` 121 271 from 121 273, `completed` 798 296 from 798 313, `abandoned`
146 185 from 146 168 and `redirected` 393 135 from 393 017; every other field is
unchanged. `IgnoreIncarnation`'s re-seeded follower applying again rose to 649 from 637:
its leader keeps a stale match for a refused follower and so never re-seeds it (D-042),
and with the other follower away its check quorum now steps it down, on 64 seeds; a new
leader rebuilds its progress at `matched: 0` and re-seeds the follower, which it did on 15
of those seeds that had no completed re-seed before, and 3 lost theirs. Its catch stays 0
with 0 progress resets. `SharedSnapshotDir`'s aimed re-take arm reached its stream on 152
seeds, from 151, on seed 648 of its 6 moved seeds; its catch stays 1, seed 680, and its
re-takes at an index already taken 532. The membership scenario, the lease trials, the
incremental checker's equivalence, the engine and WAL sweeps and every other variant's
rate and coverage are unchanged, and the scenario tests and the sweep-disk measurement are
new lines.

The pinned seeds: every pin passes at twenty, a hundred and a thousand seeds. Seeds 42,
164, 385, 7381, 6325 (correct and `AdoptionAsBuilt`), 5909 (correct, each of its two
variants and the pair), 680 (correct, each single and the pair), 687 (correct and
`RefusalNotDurable`), 1885, 2023, and the eleven variant straddles but two, run
byte-identical traces, so each pin's mechanism assertion holds as it was audited. The two
are `IgnoreIncarnation`'s seeds 2509 and 5990 in
`the_nightlys_eleven_variant_catches_of_the_trace_timestamp_gap_are_not_catches`: on
2509 leader 2 of term 12 steps down at 14.547925798 s leaving server 3 uncounted, on 5990
leader 3 of term 11 at 13.944738313 s leaving server 2, each before the isolation its
straddle was at, and neither run reaches a straddle now. The pin asserts the straddle
absent and that step-down as the reason, and D-047 carries the forward pointer. The echo
scenario's golden hash is unchanged, `19f19201df99a799`.

*At ten thousand seeds* (nightly run 34852980174, on a8656e8, the tree `main` has at
94c6a54), every test passed.

- **The scenario.** The correct server passed both halves on all 10 000 seeds.
  - Blocked: every leader stepped down naming the refused follower, the slowest after 2.057
    windows.
  - Open: every re-seed completed and committed, the slowest re-seed after 7.87 windows and
    the slowest commit after 14.78.
  - `RefusedCountsForQuorum` was caught on the blocked half on 10 000 of 10 000: no
    step-down.
  - `RefusedNeverCounts` was caught on the open half on 10 000 of 10 000: every seed stepped
    down naming the refused follower, the slowest after 1.999 windows.
- **On the sweep's disk.** The open half failed on 8 377 of 10 000 under both leaders.
  - As built: 8 311 by a step-down with nothing uncounted, 66 with no step-down.
  - Correct: 8 182 by a step-down with nothing uncounted, 162 naming the refused follower,
    33 with no step-down.
- **The re-seed episodes.** 13 738 episodes, 13 435 completed and 12 900 answered from
  their new store. Expected step-downs to `Installed`: counting nothing 11 286.3 (84.0 %;
  9 218 certain), the decided rule 3 212.5 (23.9 %; 2 176), as built 1 066.1 (7.9 %; 563).
  Through adoption: 13 067.0 (97.3 %; 12 904), 12 253.7 (91.2 %; 10 114) and 12 161.6
  (90.5 %; 9 926). A leader's majority needed the refused follower for more than two
  windows before the install in 38 completed re-seeds. These are the thousand seeds'
  proportions to within a point.

Against the ten-thousand-seed run of cd411b4 (run 34839613587), four catch rates moved:
- `CountOlderTermForCommit` 4 415 from 4 413: caught now on seeds 4025, 5860 and 9983, no
  longer on 7830;
- `ResetTimerOnAnyRpc` 3 465 from 3 462: newly on 1677, 4245 and 9765;
- `RefusalNotDurable` 133 from 132: newly on 4734;
- `SnapshotWithoutCurrentLast` 3 302 from 3 303: no longer on 1537.

`AdoptionAsBuilt`, `SharedSnapshotDir` (6, by the same checks) and `IgnoreIncarnation` (0)
kept their rates, and the membership scenario and the lease trials' stale-read counts are
unchanged. Coverage moved too:
- `IgnoreIncarnation`'s re-seeded-and-applying count is 6 353, from 6 257, and its
  decision-time report removes 2 catches, from 4: seeds 2509 and 5990 no longer straddle,
  as their pins assert;
- `SharedSnapshotDir`'s aimed arm reached its stream on 1 472 seeds, from 1 470, and it
  re-took at an index already taken on 5 292, from 5 293;
- `AdoptionAsBuilt` counts 84 582 adoptions under its storm, from 84 578;
- the correct server's coverage gains `step_downs_uncounting_refused` 39, with
  `quorum_losses` 46 706 from 46 683 and the fields of those runs after their step-downs
  shifted, `commits` 10 149 501 from 10 148 606 among them.

Each move was checked seed by seed. Two temporary branches off cd411b4 and a8656e8
(runs 34885298800 and 34885317292) printed every seed's trace hash and verdict for the
correct server and the seven variants above. Their catch counts reproduce both nightlies'
exactly. 983 seed and server pairs differ:

| Server | Differing seeds |
|---|---|
| correct | 39 |
| `IgnoreIncarnation` | 697 |
| `RefusalNotDurable` | 63 |
| `SharedSnapshotDir` | 50 |
| `ResetTimerOnAnyRpc` | 45 |
| `AdoptionAsBuilt` | 33 |
| `SnapshotWithoutCurrentLast` | 32 |
| `CountOlderTermForCommit` | 24 |

Each of the 983 was re-run on both trees. In every one, the first record that differs is
an `ananke.raft.quorum-lost` on this entry's tree naming a refused follower uncounted, at a
point where the tree before it has no step-down. The nine catch changes are among them, and
no verdict changed on a seed whose trace did not.

---

## PROPOSED D-050 — A term's record carries when the message its step took was received

**Context.** Issue #32, the open point D-047 left. The pre-vote check reads a term
change by its decision time, the moment the step that took it was taken, so a change
decided before an isolation and traced inside it is not the isolated server's
election. A term-raising message can also be *delivered* before an isolation and
*stepped* inside it: the `raft` task is still awaiting a persist when the message
arrives, or finishing an install, and the message waits in the inbox. That step is
decided inside the window, no message reaches the server in the window, and the check
by decision time flags the correct server. D-047 rejected the checker-side repair,
matching each rise to the delivery that caused it, because the checker would
re-implement the core's term rule and the inbox. The issue asked for a trace fact the
server already knows, or a measured argument that the case cannot arise, and in
either case a seed that reaches the shape and asserts the verdict for its reason.

No correct-server seed of the ten-thousand-seed nightlies was flagged this way. The
case is still reachable: it needs the server busy when the message arrives, where
D-047's straddle needs only the step's own persist to span the isolation's start, so
it is rarer, and the directed scenario below reaches it on 23 of the first 100 seeds.
An argument that it cannot arise would be false.

**Decision.** The server records the fact on the record the check reads. Every site
is marked `PROPOSED(D-050)`.

*The fact.* `TraceEvent::RaftTerm` gains `received: Option<Decision>`: when the peer's
message that the step behind the record took reached the server. The `net` task takes
a decision stamp (D-047) as it receives each frame, before admitting it to the inbox,
and the inbox event carries it; the `raft` loop remembers the stamp of the message a
step takes and `Server::execute` sets it on the `RaftTerm` events that step outputs.
A step whose input is no peer's message — a tick, a completion, re-seed progress —
and the restatement at an incarnation's start carry `None`, and so does everything
the core emits, since the core has no clock. The simulator's harness reads it as
`TraceRecord::received`, global virtual time at or before the record's decision
time; `Decision` stays opaque to the code under test. The moirae export writes
`receivedNs` in the line's `data`, after the event's own fields and before
`decidedNs`, and only where it differs from the decision time, as `decidedNs` is
written only where that differs from `t`.

*The check.* `Report::check`'s pre-vote check is `isolation_keeps_the_term_by_cause`:
the check by decision time, except that an isolation is not flagged when every change
of its server's term decided in `(from, until]` carries a receipt at or before `from`.
It is that check's verdict with one named excuse, so it flags nothing the check by
decision time does not. A change from a step that took no peer's message — a
campaign on the server's own timer without pre-vote, a restatement, a completion —
carries no receipt and is flagged as before, whatever else changed its term in the
same window. With pre-vote a term rises in the step that takes the granting
`PreVoteResponse`, or the `TimeoutNow` of a transfer, and that step carries the
message's receipt: such a candidacy, received by `from`, is excused like any change a
message caused, since what decided the election reached the server before it was cut
off. (This paragraph first said that a campaign takes no message and carries no
receipt, which holds only for a campaign without pre-vote; reworded after review.) `isolation_keeps_its_term_by` and
`isolation_keeps_its_term_by_cause` give one isolation's verdict, and
`isolation_received_straddles` is the predicate: each change received by an
isolation's start and decided inside it, with the messages from a server delivered to
the isolated one at the receipt and the count delivered in the window.
`isolation_keeps_the_term_by` under either `RecordTime` is unchanged, and so is
`moved_by_decision_time`, which now compares the check by durability time with this
one; the sweeps' assertion on a removed pre-vote catch accepts a change received
before the isolation beside D-047's straddle.

*The scenario.* `Fault::IsolateOnTermRaise`, never drawn by `Schedule::draw`, and
`Schedule::term_raise_behind_a_step(tries)`: no lease trial, true clocks, and `tries`
rounds, each asking the leader to hand over to the follower after it, whose campaign
sends the third server a RequestVote of a higher term; the run advances in slices of
10 µs until a message from a server carrying a term above the third server's last
traced one is delivered to it, and cuts that server off alone at the slice's end for
300 ms. A slice ends with nothing runnable, so an idle `raft` task has already taken
the message before the isolation, which is D-047's straddle, and a busy one takes it
inside the window, which is this entry's. With eight rounds, measured in release:
the shape on 5 of 20 seeds, 23 of 100 and 299 of 1 000, 8 changes in 136 isolations,
28 in 706 and 358 in 7 036, beside 122, 648 and 6 403 of D-047's straddles. The test
`a_term_change_stepped_inside_an_isolation_from_a_message_received_before_it_is_excused`
asserts at every tier that the correct
server passes the whole check on every seed, that the shape is reached on some seed,
and, for every change it finds, the reason: received by the start and decided inside,
no message from a server delivered in the window, a message of the new term from
another server delivered at the receipt, and the check by decision time flagging the
isolation while this check does not. Seed 4 is pinned with its numbers: server 1's
RequestVote of term 5 received by server 2 at 3.679125915 s, the isolation from
3.67913 s, server 2's step 2.176899 ms into it, the check by decision time flagging
*pre-vote: server 2 raised its term from 4 to 5 while isolated from Instant(3.67913s)
to Instant(3.97913s)* and the check passing. The pair: `NoPreVote` on the same
schedule is caught by this check on 20 of 20, 100 of 100 and 1 000 of 1 000 seeds.

*No schedule moved.* The stamp reads the time under the simulator's lock and nothing
else, as D-047's does. The echo scenario's pinned body hash is unchanged,
`19f19201df99a799`. Eighteen raft traces from this tree are the traces of 94c6a54,
the tree before this entry, byte for byte once `receivedNs` is removed, and no other
line differs: the correct server's seeds 42 (100 992 lines, 0 carrying the field),
1885 (82 827, 0), 2023 (96 023, 0), 0 (75 713, 1), 1 (66 093, 2), 2 (100 144, 1), 3
(106 255, 3), 5 (84 420, 1), 7 (100 326, 1), 11 (80 543, 0) and 13 (35 916, 0), and
`IgnoreIncarnation` 1252 (97 128, 2), `SharedSnapshotDir` 3863 (92 102, 1) and 680
(184 526, 0), `ResetTimerOnAnyRpc` 5153 (66 379, 0), `ApplyBeforeCommit` 6366 (75 642,
2), `SnapshotWithoutCurrentLast` 2305 (100 722, 0) and `AdoptionAsBuilt` 1929 (85 694,
0). Seeds 42, 1885 and 2023 have the line counts D-047 measured.

*A correction to D-047's measurement.* D-047 gave the two straddles decided at a
re-seed install's completion, `SharedSnapshotDir` 3863 and `IgnoreIncarnation` 1252,
as a new incarnation's first step taking an AppendEntries of the new term delivered
2.48 ms and 16.31 ms earlier. The receipt on their records says otherwise. On 3863
server 2's change to term 11, decided at 18.221725074 s, took the AppendEntries
received at 18.081057713 s, 140.67 ms before; on 1252 server 3's change to term 11,
decided at 20.165988416 s, took the one received at 20.021554524 s, 144.43 ms before.
The inbox is first in, first out, and each server installed into a live store, where
`install_decision` drops what it pops while it waits for the install (D-030): the
first message of the new term to arrive after the install finished is the one the
first step takes. The 2.48 ms and 16.31 ms are the time from the *last* such delivery
before the step, which is what matching deliveries by hand found. Neither verdict
moves, since both steps were decided before their isolations; the correction is to
which message caused them, and it is the failure D-047 predicted for checker-side
matching. D-047's text is not edited.

**Alternatives.** *A measured argument that the case cannot arise*: it can, above.
*Checker-side matching of each change to a delivery*: rejected by D-047, and the
correction above is that approach getting a cause wrong. *A third time on every
`TraceRecord`, through a new `Environment` method*: a change to SPEC §1.1's
interface and both environments for one field one check reads; the fact belongs to
one event. *The delivery's `MessageId` on the record*: `Socket::recv` does not return
the id, so a P0 interface change, and the delivery is the network's fact where the
receipt is the server's. *An extra record per stepped message*: the traces would
differ by more than a field, and every schedule's record count would move. *Exposing
`Decision`'s instant to all code*: the stamp is opaque so that nothing under test can
compare it with a node's clock; the harness reads it through the record. *Writing
`receivedNs` on every message-caused term record*: most records would change for no
reader; where the receipt is the decision time it says nothing the line does not.
*Excusing by the receipt alone, reading every term record by `received` or else its
decision time*: those times are not monotone in record order — a campaign stepped
before an older message waiting in the inbox — so the term at an instant read that way
could hide a campaign inside the window; an excuse on the decision-time verdict
cannot.

**Consequences.** `TraceEvent::RaftTerm` gains a field, which every construction
names; `TraceRecord` gains `received()`; the `raft` task's inbox events carry a stamp;
the raft scenario's JSONL gains `receivedNs` on the term records whose message waited
for its step; `Fault` gains an arm and `Schedule` a constructor that no sweep draws.
The echo, WAL and engine traces are unchanged. The check can only remove pre-vote
catches, never add one. At a thousand seeds (`scripts/premerge.sh` on dbaec73, this
entry's and D-051's commits, against 94c6a54's) every sweep passed with every rate
and coverage field unchanged, and every sweep reported 0 catches removed and 0 added;
the new tests' lines are the only new lines. On approval, D-047 gains a forward pointer to this entry and
SPEC §1.5's export paragraph a sentence on `receivedNs`; RAFT.md §2 and §3 describe it
now, marked proposed. Issue #32 closes with this entry's approval.

---

## PROPOSED D-051 — A removed catch is asserted against the isolation or the flag it names

**Context.** Issue #33, D-047's two limits of evidence. Every raft sweep reports, per
seed, the catches that reading decision time removed (`Report::moved_by_decision_time`).
For a removed pre-vote catch the sweep asserted only that the run held *some* term
rise straddling *some* isolation's start; for a removed timer catch it asserted
nothing and printed the flagged server's decisions straddling the flag. The one timer
catch the nightlies removed, `ResetTimerOnAnyRpc` on seed 5153, was checked by hand.

**Decision.** Both are assertions in `sim/tests/raft.rs`'s `checked`, which the raft
scenario's sweeps — the correct server's, every variant's, and D-050's directed
term-raise sweep with its `NoPreVote` pair — run on every seed at every tier, the
nightly's ten thousand included. The pinned-seed tests call `Report::check` directly
and assert their own mechanism. Every site is marked `PROPOSED(D-051)`.

*A removed pre-vote catch.* Its words name an isolation. `Report::isolation_named_by`
finds the isolation whose own verdict under the check by durability time is those
words exactly (`Report::isolation_keeps_its_term_by`, one isolation's verdict), and
the sweep asserts a term change of that server straddling that isolation's start —
decided at or before `from` and traced in `(from, until]` (D-047,
`isolation_term_straddles`), or received by `from` and decided in `(from, until]`
(D-050, `isolation_received_straddles`) — with the same server, `from` and `until`. A
removed catch whose words name no isolation fails the sweep too.

The boundaries are the two readings' own: by durability time a term record is before
the window when traced at or before `from`, by decision time when decided at or before
it. D-047's predicate first read `decided < from <= at`, which misses a change decided
at the very instant the isolation began and traced after it — a step the simulator
polls at `from` after the partition, which the check by decision time places before the
window — and counts one traced at `from`, which neither reading places inside it.
Amended after review. With it the assertion is exact: a server's term records have
decision and durability times that both rise with their order, and its terms rise along
them outside a re-seed's restatement, which the check skips; so if the check by
durability time finds the term at `until` different from the term at `from` and the check
by decision time does not, some record changes the term with `decided <= from < at <=
until` (the last record traced by `from` and the last decided by it bound the records
between them, and the terms from there to the last decided by `until` are one), and if
the check by cause excuses a window the check by decision time flags, every change
decided in it is a received straddle.

*A removed timer catch.* It must be the timer replay's first gap by durability time,
in its words (`TimerGap::violation`, which the check formats through), and
`Report::timer_removal` must give its reason, read off the two replays at the gap's
flag record `X` (`TimerGap::record`, its index in the trace). The replay takes a probe
(a record and a server) and returns that server's state once the record was replayed
and checked: running, leading, re-seeded, its clock's last reset and the record that
made it, and whether it was flagged there. By durability time the flagged server is at
`X` a running follower last reset at `gap.since`, more than its bound before `X`. By
decision time the replay does not flag it at `X` exactly when it is leading, down or
re-seeded there, or its last reset `S'` is within the bound of `X`'s decision time. Each
case is a reason with the record that makes it, and every reason found is returned:

- **`StatusMoved`**: the server leads, is down or is re-seeded at `X` by decision time.
  Its status records (its `RaftTerm`s and `RaftLeader`s, its `RaftReseeded`, its
  crashes) come from its own `raft` task in sequence with rising decision times, so
  each reading replays a prefix of them before `X`, and the status differs only when
  some status record is before `X` under one reading and after it under the other;
  that record is the reason.
- **`ResetMovedBack`**: `S'` is later than `gap.since`. The record behind `S'` is after
  `X` in the trace's order and was decided before it was traced: had it been before `X`
  the durability replay would have reset the clock there too, since a reset under the
  decision order is a reset under the trace's (a delivery counts against a term no
  higher, and every other reset reads the server's own records, whose order both
  readings share).
- **`FlagMovedBack`**: `X`, read by its decision time, is within the bound of
  `gap.since`; `X` was then decided before it was traced.

So every removal has a reason and every reason names a record whose two times place it
differently against `X`: the assertion cannot fail on a removal decision time makes,
and it fails on anything else — a gap the durability replay does not make at `X`, one
the decision replay makes there too, a reason without its record.

The first form of this assertion required a reset of the flagged server decided at or
before the flag instant and traced at or after it. Review found it inexact, and it was:
it is `ResetMovedBack` alone. A record decided before the bound and traced past it can
be the flag itself (`FlagMovedBack`), with the server's next reset decided after the
flag; and a leadership decided before the flag and traced after it is no reset
(`StatusMoved`). In the simulator's traces the first record at any instant is
`TimeAdvanced`, decided as it is recorded, so the durability replay's flag record is
always one and `FlagMovedBack` does not arise there, but `StatusMoved` can: a candidate
that runs past its bound without campaigning again — a server that resets its timer on
any message — and wins with the step traced past the flag. Unit tests in `sim/raft.rs`
build each shape from records written by hand and assert the reason given, and a gap
both readings make, for which none is.

*The evidence.* `the_nightlies_removed_catches_meet_the_sweeps_assertions` runs the 28
pairs that nightly runs 34749071877 and 34852980174 printed as removed — 27 pre-vote
catches and seed 5153's timer catch — through `checked` on this tree, with the
nightlies' words. 26 are removed here in those words, each matched as above and each
run passing the check; the timer catch is matched to server 2's granted vote, decided
2.564751 ms before the flag and traced at it. `IgnoreIncarnation` 2509 and 5990 are in
the first run only and no longer reach their catch since D-049's step-down (D-047's
amendment); the test asserts that neither removes it, so the day either does, the
assertion runs on it. Both assertions were seen to bite: comparing against another
isolation fails all 25 pre-vote pairs, and requiring a reset traced strictly after the
flag fails seed 5153, whose reason is `ResetMovedBack`, the granted vote.

*Where the assertions meet real removals.* No seed of the random sweep's first
thousand, under any variant, has a removed catch. D-050's directed term-raise schedule
has one on almost every seed, and its sweeps go through `checked`: the correct server's
removes a pre-vote catch on 20 of 20, 100 of 100 and 1 000 of 1 000 seeds, each matched to a
straddle of the isolation it names (D-047's or D-050's), and its `NoPreVote` pair, whose
runs fail the pre-vote check, removes none. None of those is a timer catch, since a run
reports the first check it fails by durability time and the pre-vote check comes first;
the timer assertion meets real removals only in the nightly, seed 5153 so far.

**Alternatives.** *Parsing the isolation's instants out of the words*: `Instant`'s
`Debug` is a display format, and matching the isolation by its own verdict needs no
parse. *Keeping a second list of resets beside the replay*: two copies of the rule
drift apart, the fault D-046 and D-047 each avoid. *Asserting only at the nightly's
tier*: a removal the gate's twenty seeds see is a removal, and the assertions cost a
replay per removed catch, which is rare.

**Consequences.** A removed catch without its reason fails whichever sweep sees it,
at any tier, including the nightly. `TimerGap` gains its flag record's index, the timer
replay a probe, and `Report` `timer_removal`, `isolation_named_by` and one isolation's
verdicts; `timer_resets_by` and `timer_resets_straddling`, the first form's helpers, are
gone. D-047's straddle predicate reads `decided <= from < at`. D-047's two limits of
evidence are closed on approval, when D-047 gains its forward pointer; issue #33 closes
with it.

---

## PROPOSED D-052 — A scenario's moirae JSONL is written when it is asked for

**Context.** D-046 left `scripts/premerge.sh` at 7 minutes 26 seconds and named what its
profile had left: the allocator, the simulated filesystem's path comparisons and the
per-run JSONL export. The owner asked for those to be measured before anything
changed. Measured on this branch at dbaec73, the tree before this entry, on the
eight-core laptop, with `scripts/premerge.sh` run after a warm build (`cargo test
--release --no-run` first, so the wall time is the tests'): 573.15 s real, 3 925.74 s
user, the raft test binary 475.57 s of it, the engine binary 75.75 s, with the machine's
one-minute load average, sampled every 15 s over the run, at 15.98 (the premerge's own
threads included). That first figure is not used for the saving below: another
agent's sweep shared the machine for part of it, and its load is not the load the
later runs had. Re-measured the same way at a mean load of 11.20 — the machine idle
but for the premerge — dbaec73's premerge took **547.55 s** real, 3 852.04 s user, the
raft binary 452.11 s. The rates of these runs are the rates of 94c6a54's thousand-seed
premerge, the new tests' lines aside.

*The profile.* `sample` (the tool D-046 used), at one sample per millisecond on every
thread, of the raft test binary at 300 seeds, all its tests: 931 025 busy samples.
Self time, by what the frame is:

| what | share of busy samples |
| --- | --- |
| the allocator (`libsystem_malloc`) | 23.75% |
| `std::path`, the simulated filesystem's `BTreeMap<PathBuf, _>` keys | 9.24% |
| the simulator | 8.52% |
| the sweep's end-of-run checks and predicates (`sim/raft.rs`, `lin.rs`) | 8.39% |
| the storage engine | 6.82% |
| `moirae_trace`, the JSONL export | 6.73% |
| trace record copies and drops | 6.56% |
| `memmove`/`memset` | 6.39% |
| the Raft server and core | 4.10% |
| `invariants::Checker` | 3.27% |
| `core::fmt` | 2.90% |

Self time hides who allocates and copies. Counted inclusively from the call graph,
`Sim::to_moirae` was **27.24%** of the binary's busy samples: every run of the raft,
membership and quorum scenarios writes its whole trace as JSONL — a `Json` object per
record, a formatted line, a copy of the trace to write it from — into
`Report::jsonl`, and the string is read only when a seed fails and its trace is
written out, or by the few tests that hash a trace. Beside it: `Sim::poll`, the
simulation itself, 30.57%; the end-of-run `Report::check` 8.15%; `std::path`
comparisons 6.05%; the pre-vote check 4.29%; `Sim::trace_from` 4.31%; `leader_now`
3.31%.

**Decision.** A scenario's report keeps what the export needs and writes the JSONL
when it is asked for. `Sim::run_header` copies out what the export reads besides the
records — the configuration, the policy, each node's clock, every address ever
bound — as `sim::RunHeader`, and `RunHeader::to_moirae(records, export)` writes the
bytes `Sim::to_moirae` writes for the same records, through the same code:
`Sim::to_moirae` is now `run_header().to_moirae(&trace(), export)`. The raft,
membership and quorum `Report`s replace `pub jsonl: String` with `pub run: RunHeader`
and `Report::jsonl()`, which writes from `records`. Every site is marked
`PROPOSED(D-052)`. The echo, WAL and engine scenarios keep their eager field: the
engine binary, the largest of them, spends 3.55% of its busy samples in
`Sim::to_moirae` (a `sample` profile at a thousand seeds, 453 852 busy samples), about
2.7 s of its 75 s and under 1% of the premerge, which is not worth a change.

**What it bought.** `scripts/premerge.sh` at a thousand seeds, measured the same way:
**449.89 s** real at a mean load of 11.81, against dbaec73's 547.55 s at 11.20, **17.8%**
less; 3 113.34 s user, against 3 852.04 s, 19.2% less; the raft binary 352.48 s, against
452.11 s. (A first figure of 21.5%, against the 573.15 s taken at a load of 15.98, mixed
loads and is withdrawn.) Every sweep passed with every rate unchanged. The traces are byte-identical: 24
traces written by this tree through `Report::jsonl()` and by dbaec73 through the field
— the raft scenario's seeds 0 to 7, 42 and 1885 under the correct server, 680 under
`SharedSnapshotDir`, 1252 under `IgnoreIncarnation`, 5153 under `ResetTimerOnAnyRpc`,
3 under `NoPreVote`, 9 under `AdoptionAsBuilt` and 11 under `RefusalNotDurable`; the
membership scenario's seeds 0 to 2 and seed 3 under `SingleMajorityInJointConsensus`;
the quorum scenario's open half on seeds 0 and 1 and blocked half on seed 0 and on seed
1 under `RefusedCountsForQuorum` — equal byte for byte, and the gate's trace-identity
and pinned-hash tests pass.

**Alternatives.** *Exporting into a sink that only validates*: it builds the same `Json`
objects and formats the same lines, which is the cost. *Keeping the `Sim` in the report
to export later*: a finished run's tasks, futures and disks held for as long as the
report lives, where the header is a few hundred bytes. *Making the export itself
faster*: the export is moirae's format through `moirae-trace`, published from the
moirae repo, and it is still paid in full by every seed that is written out; not
writing it for the seeds nobody reads is the whole of the saving.

**Consequences.** A sweep no longer exports every seed's trace, so a trace that could
not be written as moirae v2 would now surface only when it is written — a failing
seed, or a test that hashes a trace — rather than as a panic at the end of the run
that made it. In `moirae-trace` 0.0.2 that cannot happen to these scenarios: the only
error a `Collect` sink returns for a well-formed stream of events is `NotAnObject`, for a
`send` line's `msg`, a `state` line's `patch` or a `log` line's `data` that is not a
JSON object (the header errors cannot arise, since the export writes the header once,
first), and integers never fail — the writer emits one past 2^53 as a decimal string.
The raft, membership and quorum scenarios decode payloads with `message::studio`, which
returns an object for every payload, a malformed one included, and every `data` the
export builds is an object. (This paragraph first gave integers beyond what a
JavaScript reader keeps exact as a failure; they are not one.)
`Report::jsonl` is a method on the three reports; the other scenarios are unchanged.

*Two more, measured on the tree with the export lazy.* A second `sample` profile of the
raft binary at 300 seeds, 690 528 busy samples, had no export left in it and put
`std::path` comparisons at 8.16% inclusive and the pre-vote check at 6.12%, with
`Report::moved_by_decision_time` at 3.04%. Who the path comparisons belong to was read
from the call graph: 79% of them are the sweep's own adoption-crash watch
(`adoption_change`, D-041), which every 250 µs of a storm reads the victim's durable
namespace (`Sim::durable_names`), builds a set of its store's names and compares it
with the set it started from; the filesystem's own lookups (`NodeFs::open`,
`sync_dir`, `rename`) are under 6% of them and `snapshot::sweep_versions` about 8%.
And the pre-vote check, which D-050 runs three ways on every run — by cause in the
check, by durability time in `moved_by_decision_time`, per isolation in `checked` —
scanned the whole trace twice for every isolation and once more for its skip.

- **The adoption watch reads a version first.** `Sim::durable_version(node)` is a
  counter the simulated disk moves at every `sync_dir` that makes a directory
  operation durable and at every crash, the only two things that change the durable
  namespace. The watch reads the namespace again only when the counter has moved:
  between two equal counts the namespace is the one it last read, which did not end
  the watch, so every storm crashes at the very slice it did.
- **The pre-vote check reads its records from one pass.** `Report::pre_vote_records`
  keeps, in record order, the term records and the records the skip looks for — a
  few hundred of a trace's tens of thousands — and the check over all isolations,
  its per-isolation forms and the straddle predicates read those. Every verdict is
  the one the full scan gave, since the helpers matched nothing else.

Each was timed on the raft test binary alone at a thousand seeds, one after the
other with the load average sampled: the tree with the export lazy **353.17 s**
(2 465.98 s user, mean load 16.07); with the adoption watch gated **320.61 s** (2 212.80
s user, load 16.07), 9.2% less; with the pre-vote records as well **287.46 s** (1 997.17 s
user, load 11.69), 10.3% less again. Each is some 33 s, about 7% of the premerge.
A second run of the first binary at a mean load of 50.62, another agent's sweep
beside it, took 545.34 s and is not used. The traces are byte-identical: 49 traces from
this tree and from dbaec73 — the raft scenario's seeds 0 to 39 under the correct
server, ten of which (1, 3, 5, 6, 9, 11, 16, 25, 31, 33) draw the adoption storm,
seed 41 under `AdoptionAsBuilt`, which draws it too, seed 6325 under the correct
server and `AdoptionAsBuilt`, 687 under `RefusalNotDurable`, 1885 and 2023, and the
membership and quorum scenarios' seed 0.

*The premerge with all three.* `scripts/premerge.sh` at a thousand seeds on 1ef6d7e,
measured the same way: **374.64 s** real at a mean load of 13.90, 2 579.53 s user, the raft
binary 278.88 s, against dbaec73's 547.55 s at 11.20: **31.6%** less wall time, on a
machine loaded a little more than the run it is compared with. Every sweep passed with
every rate and coverage field unchanged.

What the second profile leaves, each under 5% of the premerge and so left as it is,
with its share of the raft binary's busy samples: `leader_now` 4.54%, most of it the
copies `Sim::trace_from` makes as it reads back over the tail; the refused-server
watch (`refreshed_refused`), which re-reads the trace from its first record at each
crash storm, about 0.9%; the incremental checker 5.37%, which D-046 made linear; and
the allocator, 18.64% inclusive but spread over the simulation itself — the storage
engine's blocks, the simulator's timers and the trace records — with no single caller
that a change could take out. In the engine binary, which is not the raft binary's,
`Model::state_after` and the `memcmp` under it are about 23% of its busy samples, some
17 s of its 75 s and about 4% of the premerge.

---

## D-053 — RAFT.md says what the code has

**Context.** Checking RAFT.md against the tree while writing SHARD.md found places where
it describes what the code lacks (SHARD.md:23-34). The owner's answer to SHARD.md's Q1,
approved on 2026-09-15, is that the first commit of Phase 3 corrects them, and the answer
to Q35 is that Phase 3 has no scan. RAFT.md is the approved design, not an entry, so its
text is corrected where it is wrong and this entry records each correction: what RAFT.md
said, what the code does, and why the text now follows the code. None of the code
changes. Q1 names the scan, the two variants, the frame's field order and `src/read.rs`;
three more statements in or beside those passages, as plainly false, were found while
correcting them and are corrected with them: the frame's fields and payload, the
studio's name for an AppendEntries, and where a follower's entries are read from.

*§4, the scan.* RAFT.md's history had a fifth operation, `Scan(range) → Vec<(k, v)>`,
and its checker a scan check: a scan consistent iff some time in its window agrees with
every key's chosen linearization. Neither exists and neither ever did. `ClientOp` is
`Put`, `Get`, `Delete` and `Cas` (crates/ananke-env/src/trace.rs:752-779), and
`sim/lin.rs` searches each key's operations on its own and returns each key's timeline,
which only its own tests read (sim/lin.rs:207-236). A scan over many keys waits for SPEC
§6's distributed scans, in Phase 5 (SPEC.md:351-352), since with ranges a scan across
them has no linearizable form without transactions (Q35).

*§3, the frame.* RAFT.md gave `kind: u8 | term: u64 | from: u64 | fields`. The codec
writes and reads the kind, then the sender, then the term
(crates/ananke-raft/src/message.rs:4, 383-385, 492-494), the order D-025 recorded when
it landed. The same sentence called the fields length-prefixed and gave entries as
`count | (term, index, payload_len, payload)*`. A fixed-width field is written bare, and
of the variable-length fields a file name and a chunk's data each follow a `u32` length;
entries follow a `u32` count, and a payload is a tag followed by a length and the bytes
for a command, the member lists for a configuration and nothing for a no-op
(message.rs:284-354, 442-478). And the studio's decoder names an AppendEntries
`raft.append-entries`, not `raft.append` (message.rs:222-236, 669-673).

*§3, the crate.* RAFT.md listed `src/read.rs`, "read-index and lease reads". There is no
such file. The read-index round, the lease and the drift guard are the core's
(`Raft::on_read`, `Guard`, in core.rs), and the node serves the reads the core makes
ready (node.rs). The listing's `core.rs` line now says so.

*§3, the log read back.* RAFT.md said that reading entries back for a follower behind
the leader is a `scan` over the log's index range. The core holds the log in memory and
builds each AppendEntries from it (D-025); the store scans the log table when it opens,
to hand the log back to the core (crates/ananke-raft/src/store.rs:681-697).

*§5, the variants.* RAFT.md's table had eighteen rows; `Variant::BUGS` has sixteen arms
(crates/ananke-raft/src/core.rs:159-176). The two rows beyond them, `VoteBeforePersist`
and `ApplyNotAtomicWithIndex`, name variants that never existed in the code: the history
holds either name only in documents. What the code has for their rules:

- *The vote durable before it is answered.* `SendBeforePersist` covers it. The server
  enforces the order, not the core: under that variant every `Send` of a step leaves
  before the step's `Persist` (crates/ananke-raft/src/node.rs:2090-2097), and a granted
  vote is a step whose `Persist` carries the vote and whose `Send` is the answer
  (core.rs:1277-1301, 2355-2380). Its row said "the same discipline for `AppendEntries`",
  naming the vote row above it, so it now states the discipline itself.
- *The applied index in the batch of its entry's writes.* No known-buggy variant.
  The rule's crash test, `an_entrys_writes_and_the_applied_index_are_durable_together`
  (crates/ananke-raft/tests/store.rs:116-245), runs the correct store over forty seeds
  with lost syncs and bit rot and asserts the engine's state is the model's at the
  recovered applied index, exactly once per entry.

**Decision.** RAFT.md is corrected at each of these places, and each corrected passage
cites this entry: §3's crate listing, its log paragraph and its frame; §4's history,
partitioning and what is asserted, with a pointer to SPEC §6; §5's table, which drops the
two rows and restates `SendBeforePersist`'s rule, and a paragraph before it that says the
table is `Variant::BUGS` and where the two rules without a variant of their own stand.

**Alternatives.** Building what RAFT.md described instead: a scan is against Q35, and the
two variants are code the phase did not plan, a widening of scope; a known-buggy variant
beside the atomic apply's crash test is an issue to file, not part of this correction.
Superseding the passages with forward pointers and leaving the text: RAFT.md is not an
accepted entry, Q1 asks for the text corrected, and a reader of RAFT.md would still meet a
checker and two variants the code does not have. Keeping `VoteBeforePersist`'s row with a
pointer to `SendBeforePersist`: a row for a variant that does not exist invites a test that
cannot be written.

**Consequences.** Corrections move RAFT.md's lines from §3 on. SHARD.md's citations of
RAFT.md by line were re-checked in the same commit and point at the passages they quote.
Ten of them were already off before it, and are corrected with the rest: those at
SHARD.md:581, 774, 1066 and 1926 into §3, and at SHARD.md:1565, 1569, 1577 (two), 1579
and 1580 into §5. No entry of DECISIONS.md cites RAFT.md by line, so no accepted entry
gains a pointer. No code, trace hash or seed schedule moves.

---

## PROPOSED D-054 — The live install of a span: one manifest switch, numbered above the engine, from a checkpoint of the span

**Context.** Q2, approved by the owner on 2026-09-15, puts every range of a node in one
engine, and makes its entry criterion a crash test: a span's keys removed and its tables
added in one manifest switch, the installed sequence numbers above the live engine's,
green before any split code (SHARD.md §11 storage 5, §12 Stage A item 2 and its exit).
Two things were missing. The only install replaced a whole store directory at the
server's next start (crates/ananke-raft/src/snapshot.rs:305), and putting tables into a
running engine was crate-private and knew nothing of a span (`manifest_edit`, `install`,
engine.rs). And the only checkpoint copied the whole store (D-024), so there was nothing
of one span to install from (storage 4). What Q2 settles, the one switch and the numbers
above the engine's, and what the stage asks of the test, the span as it was or as
installed, every other key unchanged, a later write read over the install, and the
two-switch variant caught by the same test, are the owner's. How the engine keeps them
is not settled anywhere, and is proposed here: where the number comes from, what becomes
of the memtables and the log below it, what becomes of a table that holds keys on both
sides of the span's edge, the order level 0 is read in, what a snapshot sees, how many
installs run at once, and the test's shape and oracle. D-054 takes the checkpoint of a
span, since the crash test installs from one; D-055 does not need it. Every site is
marked `PROPOSED(D-054)`.

**Decision.** *The checkpoint of a span.* `Engine::checkpoint_span(range, dir)` writes,
under the turnstile as `checkpoint` does, the newest write at or below the newest version
applied of every key in `[start, end)` that is present, each at its own sequence number,
into tables at level 0 sealed near `sst_bytes` in key order, then `MANIFEST-000001`
listing them with `flushed_seq` the version, then `CURRENT`, each synced in that order. A
deleted key leaves nothing: the checkpoint is the span's state, not its history. A crash
leaves a whole checkpoint or one without `CURRENT`, which nothing opens.

*The source.* `Engine::open_span_source(dir)` reads `CURRENT`, the manifest it names and
every table that lists, each opened and verified whole, and writes nothing; anything
missing or damaged refuses it (`InstallRefused::SourceDamaged`). Any whole store whose
keys lie inside the span is a source, a checkpoint of the span among them.

*The number.* `Engine::install_span(range, source)` appends, as it is called, one log
record of its own that holds no write (an empty batch, synced), and every installed write
carries that record's number `S`. `SpanInstall::seq` reports it before anything is
written, as `Write::seq` does. `S` is above every write the engine has taken, and every
write taken after the call is above `S`: a later write to the span is newer than every
installed one, which is what reads it over the install.

*The memtables and the log below it.* The engine marks the install in progress. As `S`
is applied the active memtable, if it holds anything, is rotated, so every memtable holds
writes from one side of `S` only; while the install is in progress the flusher leaves
alone every memtable whose writes are all above `S`. The install waits for its record to
be durable, takes the turnstile and flushes every memtable holding a write at or below
`S` itself; a memtable the flusher was handed before is no longer the queue's head when
the flusher gets in, and it moves on. Then every write at or below `S` is in a table, and
the manifest that makes the install the state says so: its `flushed_seq` is at least `S`,
and recovery never replays a write of the span older than the install.

*The span's writes out.* Every table in service that holds a write of the span below `S`
is taken out: whole when every key it holds lies in the span and every write is below
`S`, and otherwise written again at its own level, under a new number, without those
writes, keeping every other write at its own sequence number.

*The installed tables.* The source's newest write of each key, if it is live, is written
at `S` into tables at level 0 sealed near `sst_bytes`. A key outside the span refuses the
install (`OutsideSpan`): from the source's manifest before anything is numbered, and key
by key as the tables are written, where the tables already written are orphans the next
open removes.

*One switch.* The next manifest lists the tables in service less those taken out, with
the rewrites and the installed tables, and `flushed_seq` at least `S`.
`TraceEvent::SpanInstalled` records the manifest's number, the span, `S`, the tables taken
out, each rewrite with its original, and the installed tables with their key ranges,
before the manifest is written, as `CompactionWritten` is (D-023); then the manifest is
written, synced and switched to, the result put in service, and only then are the tables
taken out deleted and the log segments at or below `S`. A crash before the switch leaves
the old manifest, the old span and the new files as orphans; after it, the installed span.

*Level 0 is read newest sequence number first.* A lookup took level 0 newest file number
first. A rewritten level-0 table keeps its writes' numbers under a new, higher file
number, so a key outside the span whose older write it holds would be read before a newer
write in a table flushed after the original but numbered below the rewrite. Level 0 is now
ordered by the highest sequence number a table holds, then its number. For flushed tables
the two orders are one, since their sequence ranges are disjoint and numbered in order, so
nothing an engine did before an install reads differently.
`a_rewritten_level_0_table_does_not_hide_a_newer_write` (tests/engine.rs) builds the case
and reads the old value under the old order.

*The install's own task.* The work after the install is numbered, from waiting for its
record to deleting what it took out, runs in a task the engine spawns through
`Environment::spawn` (`span-install`), and `SpanInstall` only waits for the outcome the
task leaves. A caller that drops the future, or never polls it, cannot stop the install
between writing its manifest and switching to it, which would leave a manifest file
under the next number that the next flush's `create_new` then fails on for good, and
cannot hold the flusher back by not polling. The task lets the flusher go before it
tells the caller. What the task owns — the hold on the flusher and the caller's
outcome — is one value whose drop lets the flusher go and then, if the work left no
outcome, leaves the error *the install's task ended without an outcome* and wakes the
caller: a task that panics, is aborted, or is dropped with its runtime or its node no
longer leaves `SpanInstall` pending for good, which on the real runtime it did
(`an_install_whose_task_ends_without_an_outcome_resolves_with_an_error` crashes the
node under an install and polls its future after). A future polled again after it
resolved says so rather than waiting.

*When an install fails.* What an error leaves depends on when it comes. Before the
install's manifest is written — flushing the memtables below it, reading the tables,
writing the rewrites and the installed tables — the engine is as it was, and the tables
written so far are orphans the next open removes. Writing that manifest, or switching
`CURRENT` to it, is different: the manifest's file may already hold the next number,
which the next flush's `create_new` would then fail on for good, and `CURRENT` may name
it, though the engine's memory still lists the manifest before. The engine quiesces
(D-044), traced as `EngineQuiesced` with the reason *an install's manifest or its switch
failed*: no flush, compaction or log deletion follows, writes are still taken into the
log, and the next open finds the span as it was or as installed, with the log replaying
over it (`an_install_whose_switch_fails_quiesces_the_engine`). Once the switch has
returned the install is in force, and it resolves `Ok` whatever follows: an error
deleting the tables it took out or the log segments at or below its number is traced as
`InstallCleanupFailed`, the tables left are orphans the next open removes, and the
segments go with the next flush's deletion
(`an_install_whose_cleanup_fails_after_the_switch_resolves_as_made`). A future dropped
at once leaves the install to its switch, and the next install is not refused
(`a_dropped_install_or_range_delete_still_runs_to_its_switch`). The simulator's
filesystem returns no error the engine did not cause, so none of this moves a trace.

*Applies in order.* The split of the memtables at `S` rests on D-021's rule that
writes apply in sequence order: every record below `S` applied before `S`, and `S`'s
rotation before any record above it. On a runtime with more than one thread that rule
had two windows, and the simulator, which polls one task at a time with no await inside
a write or an apply, reaches neither.

The first: `apply_through` popped a record under the pending lock and applied it after
letting the lock go, so two callers could apply out of order. Applies are now
serialised by a lock of their own (`apply_order`), held from the first pop to the last
apply.

The second, found by the second review: `Engine::write` had the log number a record
(`append_with`, under the log's state lock, which also wakes the log's writer) and put
it in `pending` afterwards, under the pending lock. Between the two, the writer could
sync `S - 1` and `S` together and a caller of `S` apply through it: `S - 1` was not yet
in the map, so `S` was applied and the memtable split at it, and `S - 1` landed in the
memtable past the split. The install's switch then set `flushed_seq` at or above `S`
and deleted the log segments through `S`, so a crash lost `S - 1`, which had been
acknowledged, and reads before the crash saw the span's replacement mixed with it.
The same window let a flush's rotation fall between `S` and `S - 1`, D-021's own case.
Now the record is numbered and put in `pending` under one lock: `write` takes the
pending lock, has the log number the record, and inserts it before letting go. The
lock order is `pending`, then the log's state, which `begin_install` already follows
(the install lock, then a write), and the log's writer never takes `pending`. Whoever
pops `S` from the map therefore finds every record below it either there or already
popped, and the popping is serialised. Neither change moves anything a simulated run
does, and no trace changes.

Neither window is covered by a test that forces it. The first is closed by a lock held
across the pop and the apply, and the second by a lock held across the numbering and
the insertion: in each case the interleaving a test would force is one the code no
longer has a point to stop at, and without a hook inside `write` or `apply_through`,
which the engine does not carry for tests, a test on the real runtime would be a timing
race that passes on the broken code as readily as on the fixed one.

*One at a time, and refusals.* A second install while one is in progress is refused
(`InProgress`), as are a span with no key (`EmptySpan`), a source with a key outside the
span (`OutsideSpan`) and a quiesced engine (`Quiesced`, D-044). A refusal before the
number is taken writes nothing; one after it leaves the install's record, which holds no
write. An install of an empty source takes the span's keys out and puts nothing in.

*What a snapshot sees.* The install replaces the span's history, it does not add to it: a
snapshot older than `S` reads the span as empty once the switch is made, and one at or
above `S` reads the span as it was until the switch and as installed after it. A scan
that began before the switch reads the tables it began with to the end.

*The variants.* `Variant::InstallInTwoSwitches` takes the span's writes out with one
manifest (the rewrites in, the tables taken out out) and puts the installed tables in with
a second; a crash between the two leaves the span as neither what it was nor what was
installed. `Variant::SpanCheckpointUnsynced` writes a span checkpoint's tables without
syncing them before the manifest and `CURRENT` that name them.
`Variant::InstallKeepsSourceNumbers` writes each installed key at the number its source
gave it rather than at `S`: a source from a store further along than the live engine
carries numbers above every later local write, and hides them.

*The crash test.* `sim/engine.rs`'s scenario with `Schedule::install()`: the Phase 1
workload, three writers over all 48 keys and two readers, plus a task that every one to
five milliseconds takes a source for a random span of one to twelve keys into a directory
of its own, waits half a millisecond to three and a half more while the writers write over
it, and installs the source over the span. One source in two is a checkpoint of the span,
which rolls the span back. The other is a store further along than the live engine, as a
range's snapshot from a leader is: written in the engine's own table, manifest and
`CURRENT` formats and synced in a checkpoint's order, holding most of the span's keys with
new values, every one numbered above the newest version the live engine had applied. It is
not written by a second engine on the node: that engine's log, table and manifest events
would reach the trace the oracle reads by segment, table and manifest number with nothing
to say they were another directory's, and the first attempt did exactly that, failing the
correct engine on nearly every seed; the storage crate's own test,
`an_install_from_a_store_further_along_carries_the_install_s_number`, installs from a
second engine. Every crash is aimed at
an install, from the harness's own stream: on half the epochs at a time drawn uniformly
from the twelve milliseconds after the next install is asked for, and on the other half
from the three after its replacement is traced, just before its switch. The disk faults
are the engine sweep's: lost syncs at one in five, bit rot, torn writes, lost directory
entries and latency. The oracle is the engine sweep's (D-022, D-023, D-024) extended by
the trace's account of each install. The mirror takes `SpanInstalled` as it takes a
compaction: the rewrites hold their originals' writes less the span's below `S`, the
installed tables hold the source's live keys at `S` split by their key ranges, and the
span's writes below `S` in the tables taken out are dropped. An install is in force when
the manifest in force is in the lineage of the one it named, which the mirror prunes on a
fallback as it prunes compactions, and it belongs to the start of the node it was written
in, so a later start's manifest under the same number is not its manifest. In force, its
writes are its keys at `S`, owed like any flushed write and present only in a table,
never by a replay, since its record holds none, and the span's writes it dropped are
excused as a compaction's are; not in force, it holds no write. On top of every property
the sweep had, each recovery asserts Q2's criterion directly: with an install in force no
table in service and no replayed record holds a write of its span below `S`, and without
it no table in service holds a write it installed. The model folds an install in force
as the span replaced whole at `S`, so the state check reads every key of it, the later
writes over it included, and the checkpoint of every span is opened fresh after the
crash that follows it. During the run a live read of a span's keys and the span's part of
a scan are not judged while its install is in progress, and once it resolves the model
takes the install as made.

**What the sweep found.** One thing, in the oracle. At seed 9 of the first twenty the
correct engine was reported for record 527, gone with a missing log head after a
fallback: the record was an install's that had not been in force, which holds no write,
so no table a fallback left behind held it, the way every write of its neighbours was
held and excused. A record holding no write now takes the excuse its neighbours take:
past the manifest in force, up to the furthest any manifest covered, the fallback that
explains them explains it. And one thing the span checkpoint's variant showed: the
checkpoint check skipped any checkpoint a torn write touched, and a correct checkpoint
syncs every file before it completes, so a crash tears one only if its sync was lost,
which `FsyncLost` already says. At the first twenty seeds `SpanCheckpointUnsynced` was
caught on 1, because its tables' writes were torn more often than lost whole, and every
torn one was skipped; with a torn write alone no longer an excuse it is caught on 19 of 20.

*What review found in the installed numbers.* The mirror stamps installed writes at the
number the trace records, and every source was then a checkpoint of the same engine below
it, so an install that kept its source's numbers passed everything: nothing read a number
the source gave. Now each recovery asserts that every installed table the manifest in force
lists carries the install's own number and no other, from the recovered manifest's record
of the table (`first_seq` and `max_seq` both `S`), which is what the engine wrote and not
what the trace says; half the sources are the store further along above; and
`InstallKeepsSourceNumbers` is its known-buggy engine. The storage test installs from a
second engine that has taken more records than the live one, and shows the correct engine's
installed tables at the install's number with a later local write read over them, while the
variant keeps the donor's number and returns the installed value over the later write.

*What review found in the coverage.* Whole-store checkpoints verified after a crash are
now counted apart from span checkpoints, and the default sweep asserts some of the former;
reads, scans and seeks left unjudged, in whole or in part, because an install of their span
was in progress are counted; and the crashes before an install's switch are split three
ways: before a sync of the install's own record returned, between the replacement's
`SpanInstalled` and the switch to its manifest, which is the window the one switch closes,
and the rest (flushing the memtables below the install, writing its tables, or a switch a
fallback then abandoned). The live install's and the range delete's tests assert crashes
between the replacement and the switch, and after the switch before the install resolved.

*What review found in the oracle.* Independent review of 9eb28e4 found two excuses in the
engine sweep's oracle, both older than this entry, that could have hidden a broken
install, and both are tightened. **The fallback excuse was unbounded.** Any recovery that
fell back set the fallback's excuse, including one that used the last manifest switched
to, which loses nothing, and the excuse then covered every missing write that any table
ever held and every log record past the manifest in force. Now a fallback excuses a loss
only when it went older than the last manifest switched to and a fault explains why the
manifest `CURRENT` named could not be used; and only a write that a table listed by a
manifest it abandoned held — numbered above the one it used and at or below the last
switch, in the lineage the trace mirrors — or a table dropped for a fault holds, and only
the records holding no write up to the furthest those abandoned manifests covered.
**A table deleted by the engine was excused with no fault.** A table the manifest in force
listed and the open found missing was excused whenever the engine had deleted it after
some manifest was written, fallback or not; the simulator never loses a directory sync, and
a correct engine deletes a table only after the switch that stops listing it. Now such a
table is excused only by this open's explained fallback, or by the earlier open's under the
same manifest in force, which dropped it for that fallback and whose manifest nothing has
replaced since (clauses the second review showed could never fire, and which are gone;
below); otherwise the check reports *table N, which manifest M lists, was deleted before
its manifest was in force*. The mutation the old excuse hid, a compaction that
writes its manifest, deletes its inputs and only then switches `CURRENT`, was run once
against both oracles: the old one caught it on 0 of 20 and 0 of 100 seeds, the tightened
one on 6 of 20 and 30 of 100, the first at seed 1: *table 37 at level 2 covering 373..=444,
which manifest 40 lists, was deleted before its manifest was in force*. **No correct-engine
seed turned red under the tightened oracle**: every correct test of the engine binary
passes at 20, 100 and 1000 seeds, and so does the Phase 1 schedule alone
(`Schedule::phase_1()`) at 1000.

*What the second review found in the oracle.* Two more gaps in the dropped-table
check, and two clauses of the first tightening that could never fire.
- **A fault on a table's contents excused its deletion.** The check asked whether any
  sync of the table's file had been lost, or bit rot had hit it, anywhere in the trace,
  before it asked whether the engine had deleted it. A table the engine deleted and the
  open found *missing* was excused so, though a lost sync or bit rot explains a table
  *unreadable* or *corrupt*, not a file whose deletion the trace shows and whose
  directory entry a sync made durable. Now the reason is the one this open gave in
  `SstDropped`, and a table dropped as missing that the engine deleted is judged as
  deleted, whatever faults its contents met.
- **A reused number carried its first life's deletion.** A fallback can hand out a
  table number again, and the mirror's set of deleted tables was by number across the
  whole trace, so a second table under a number could be taken for the first's
  deletion. A table written again now leaves that set, and its first life's finished
  compaction inputs, as the log's betrayed-sync set already did.
- **The fallback and carry-over clauses could not fire.** The first review excused a
  deleted table by this open's explained fallback, or by an earlier open's under the
  same manifest. But an open that falls back never reports a dropped table: it uses only
  a manifest whose every table is there (D-022), which the pin on seed 44 asserts. So
  the fallback clause never met a dropped table, and the carry-over only ever carried
  what the fallback clause gave it. Both are deleted, and so is the carried state.

No correct-engine seed turned red: every correct test of the engine binary passes at
20, 100 and 1000 seeds with every coverage and outcome field unchanged. The only rates
that moved are `DeleteBeforeManifest`'s, whose deletions a contents fault had forgiven:
12 of 20, 67 of 100 and 647 of 1000, from 11, 66 and 645. The mutations were run against
the oracle before the second review's tightening and after it, each as the correct
engine's checks over a scratch copy of the tree:

| mutation | schedule | 20 | 100 | 1000 |
| --- | --- | --- | --- | --- |
| a compaction writes its manifest, deletes its inputs, then switches | default | 7 → 7 | 29 → 30 | 258 → 280 |
| an install writes its manifest, deletes the tables it took out, then switches | `install()` | 8 → 10 | 34 → 40 | 405 → 442 |

The first catch of each is the deleted-table check: at seed 1, *table 11 at level 0
covering 83..=107, which manifest 11 lists, was deleted before its manifest was in
force*, and at seed 2, *table 68 at level 2 covering 728..=841, which manifest 65 lists,
was deleted before its manifest was in force*. The compaction mutation's rates differ
from the first review's (6 of 20, 30 of 100) because every schedule has moved since.

*Measured after review.* On the tree of the review's fixes to D-054 — the tightened
oracle, the install's own task, the store further along and the observed numbers — in
release on the eight-core laptop beside another lane's premerge (load averages 25 to
58), `cargo test -p ananke-sim --release --test engine` with `ANANKE_SEEDS` at each tier.
The correct engine passes every seed of the live install's test, the range delete's
(D-055) and the default sweep at every tier: **no correct-engine seed is red under the
tightened oracle.** The live install's crash test:

| tier | installs asked / resolved | crashes aimed | in force after a crash (resolved / not) | not in force: before the record was durable / between replacement and switch / otherwise / resolved, a fault | span keys written after an install, checked | live reads over an install | span checkpoints verified | reads left unjudged |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 20 | 274 / 178 | 153 | 157 / 16 | 1 / 19 / 59 / 18 | 2 985 | 3 874 | 120 | 2 734 |
| 100 | 1 344 / 912 | 733 | 797 / 56 | 2 / 61 / 306 / 90 | 15 614 | 19 024 | 583 | 13 667 |
| 1000 | 13 460 / 9 138 | 7 320 | 7 990 / 490 | 22 / 651 / 3 087 / 902 | 163 647 | 197 835 | 5 827 | 142 416 |

Its variants, on the same schedule: `InstallInTwoSwitches` caught on 13 of 20, 54 of 100
and 559 of 1000; `InstallKeepsSourceNumbers` on 20 of 20, 98 of 100 and 975 of 1000, the first
at seed 0: *the install at record 125 is in force (manifest 11) but its table 15 carries
records 174..=179, not the install's number*. At one seed of the thousand a kept number equalled a later
local write's under the same key and the variant's own compaction stopped at the table
writer's order assertion, which the test counts as caught and prints. Since the second
review that count is kept apart from the oracle's, only a panic whose message is that
assertion's counts, any other panic fails the test, and the oracle's own catches are what
the test asserts: 20 of 20, 98 of 100 and 974 of 1000 by the oracle, and at a thousand
one more by the assertion.
`SpanCheckpointUnsynced` is caught on 17 of 20, 81 of 100 and 816 of 1000, down from 19,
95 and 940: half the sources are now the store further along, which the harness writes and
syncs itself, so only the other half are the engine's span checkpoints that can show it.

*Measured before review.* In release on the eight-core laptop, beside another lane's builds and
sweeps (load averages from 4 to over 100 during the runs), `cargo test -p ananke-sim
--release --test engine` with `ANANKE_SEEDS` at each tier:

| tier | correct engine | installs asked / resolved | crashes aimed | after a crash: in force (resolved / not) | not in force (unresolved / resolved, a fault) | span keys written after an install, checked | `InstallInTwoSwitches` | `SpanCheckpointUnsynced` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 20 | every seed | 251 / 160 | 149 | 135 / 17 | 73 / 21 | 2 804 | 7 of 20, 12 once tightened | 19 of 20 |
| 100 | every seed | 1 241 / 801 | 732 | 696 / 43 | 387 / 80 | 12 926 | 31 of 100, 55 once tightened | 95 of 100 |
| 1000 | every seed | 12 623 / 8 277 | 7 341 | 7 190 / 416 | 3 860 / 820 | 143 868 | 384 of 1000, 565 once tightened | 940 of 1000 |

At a thousand seeds 203 995 live reads of an installed key that a later write had
overwritten agreed with the model, and 9 231 checkpoints, of spans and of the whole
store, opened fresh after a crash and matched it. Before the oracle was tightened,
`InstallInTwoSwitches` was caught only where a crash landed between its two switches and no
fallback followed, and the account first written here — that eight of fifteen such
crashes at twenty seeds left nothing to hold against it, five of them fallbacks — rested on
the unbounded excuse, which forgave any loss behind any fallback. Tightened, it is caught on
12 of the first twenty seeds. Eight catches are a crash between the two switches. Four are
a fallback that lands on the removal's manifest itself, which no correct install writes: at
seeds 3, 8 and 9 recovery fell back from the manifest that adds the tables to the one
before it, and at seed 19 from two manifests past it. Four crashes in the window, on three
seeds, go uncaught, for reasons the correct install shares: an install with nothing to take
out and nothing to add (seed 15); a rename of the second switch that survived the crash
without its directory sync, so the install came back whole (seed 15); a lost sync of
`CURRENT` whose fallback used the second switch's manifest, whole (seed 12); and a fallback
that went older than both switches, which is the span as it was (seed 13). The engine sweep's own tests, on the default
schedule, which runs none of this entry, are unchanged at every tier: the correct engine
passes every seed with the same coverage, and `NoWalBeforeMemtable`,
`ReleaseBeforeManifest` and `DeleteBeforeManifest` are caught on 19, 13 and 10 of 20, 98,
64 and 55 of 100, and 984, 627 and 570 of 1000, before this entry and after it.

**Alternatives.** A number per installed table carried in the manifest, as RocksDB's
ingested files carry a global sequence number: no rewrite of the source, but a manifest
format change and a read path that consults it; revisit when a range's snapshot is large
enough for the copy to cost. Range tombstones in the memtables and tables instead of the
flush and the rewrite: no forced flush, but a tombstone kind that every read, merge,
compaction and the oracle must learn, which is the range delete's question (D-055).
Numbering the install at its switch, with no record of its own: the number would not be
known before the install is durable, and a number no record carries is a gap the log's
numbering refuses (D-019). Installed tables at the bottom level: a rewritten table there
keeps a key range that can straddle the span, and a level's tables must not overlap.
Keeping level 0 in file-number order and compacting level 0 away before an install: a
compaction under every install, and more writing than the rewrite. Waiting for the
flusher to flush the memtables at or below `S` instead of flushing them under the
turnstile: a waker the flusher would have to answer, and the same flushes. Concurrent
installs: two pending numbers would let a memtable between them reach a table before the
older install switches, below its installed tables in level 0; Q14's one apply task per
node serialises installs anyway. Keeping the span's old versions for older snapshots:
range tombstones again; the node's apply task holds no snapshot across an install of the
span it replaces. Filtering the source to the span instead of refusing: it would drop
keys a caller meant to install without a word. Installing a whole checkpoint and taking
the span from it: the leader would stream a whole store for one range.

**Consequences.** An install forces a flush of the memtables at or below its number,
before its own switch; rewrites every table that straddles the span's edges; and copies
the source into the engine, since the simulator's filesystem has no links (D-024). The
flusher waits behind an install in progress, so memtables pile up for its length. The
install holds the turnstile from its flush of the memtables at or below its number to
its deletion of the tables it took out, so no flush, compaction or checkpoint of any
span runs on the engine meanwhile: with one engine per node (Q2), an install of one
range stalls every range's flushes and compactions for its length, as a checkpoint
already does (SHARD.md §11, storage 6). A refused install can still take a sequence
number. A snapshot is not stable across an install of the span it reads, so **Stage B
holds no snapshot across an install of the span it reads**: the node's one apply task
(Q14) takes no snapshot of a range it is installing, and a read served at one version
(SHARD.md §11, raft 15) is not served from a range while its install runs. An install
refused as `InProgress` changes nothing and may be asked for again; the engine does not
queue it. Installs and range deletes (D-055) share the rule, and Stage B's apply task,
the one caller that installs or deletes a range (Q14), serialises them, so it never
meets `InProgress`; any other caller retries after the install in progress resolves.
D-055 puts the install, the span checkpoint, the range delete and the seek on the engine
sweep's default schedule. `TraceEvent::SpanInstalled` is new, with the moirae line
`ananke.engine.span-installed`, and so is `TraceEvent::InstallCleanupFailed`, with
`ananke.engine.install-cleanup-failed`. An install that fails writing or switching to its
manifest leaves the engine quiesced until it is reopened. The engine sweep's default schedule runs no install, span
checkpoint or variant of this entry, so its seeds' schedules and its three variants'
rates are unchanged by this entry (below); the change to level 0's read order and the
flusher's check move no schedule of a run with no install, and no pinned trace hash
moves. The checkpoint check's torn-write exclusion is narrowed for every checkpoint, the
whole-store checkpoints of D-024 included, which the correct engine passes at every tier
below.

---

## PROPOSED D-055 — The bounded seek, the range delete as an install of nothing, and the engine sweep with all four primitives

**Context.** Stage A's item 7 (SHARD.md §12) asks for the engine's other primitives, each
with its own crash test and its own engine variant caught on some seed at every tier: a
bounded, ordered seek (§11, storage 2), a range delete (storage 3), and the checkpoint of a
span if the live install did not build it, which it did (D-054). And it asks for
`sim/engine.rs`'s workload extended with all four, the correct engine passing every seed,
and `NoWalBeforeMemtable`, `ReleaseBeforeManifest` and `DeleteBeforeManifest` still
caught. `scan` returned every key of a span, with no limit and no "first key at or after
`k`" (engine.rs, `scan`); a meta lookup, the first record whose end key is above `k`
(§1), and a split key chosen from a range's keys (Q18) need one. Deleting a merged range's
Raft state, a collected replica's span or the right span on a node outside the right
half's configuration (§5) was one tombstone per key in a `WriteBatch`, each kept until it
reaches the bottom level or no older write lies below it (compaction.rs). SHARD.md does
not settle the primitives' shapes, so each is proposed, one commit each, and this entry
grows with them. Every site is marked `PROPOSED(D-055)`.

**Decision.** *The seek.* `Engine::seek(range, limit, snapshot)` returns the first
`limit` present keys at or after `range.start` and below `range.end` as of the snapshot,
in key order, with their values: the one merge `scan` walks, stopped once `limit` keys
are found. A deleted key is passed over and does not count, so a seek returns fewer than
`limit` keys only when the range holds no more. The first key at or after `k` is
`seek(k..end, 1)`, and a range is paged by seeking again from just past the last key
returned. It walks forward only: nothing SHARD.md asks for walks backward, and a reverse
seek needs a backward merge over memtables and blocks, which is an issue to file rather
than code for this stage. `Variant::SeekCountsTombstones` counts a deleted key against
the limit.

*The seek's crash test.* `Schedule::seek()`: the engine sweep's workload with half the
readers' scans made bounded seeks of one to six keys at a snapshot, each of which must be
the first keys the model holds in the range at that version, and every recovery walked
by seeks of three keys at a time, each from just past the last key the one before
returned, which must be the model's state, beside the reads and the scan the state check
made already. A seek whose first keys touch the span of an install in progress is not
judged, since which keys fill its limit depends on the span (D-054).

*The range delete.* `Engine::delete_range(range)` is an install of nothing over the span
(D-054): numbered by a record of its own above every write taken, the memtables split
there and flushed up to it, the tables holding the span's older writes taken out or
written again without them, and one manifest switch that makes it the state, with no
tombstone written. It shares the install's one-at-a-time rule, its refusals and what a
snapshot sees; a write to the span after the call is newer than the delete and survives
it. It is traced as the install it is, `SpanInstalled` with no table added.
`Variant::RangeDeleteSkipsMemtables` takes the span's writes out of the tables but flushes
no memtable first and leaves the manifest's `flushed_seq` where it was, so the span's
writes still in a memtable stay readable, reach a table at the next flush, and come back
from the log after a crash.

*The range delete's crash test.* `Schedule::range_delete()`: the engine sweep's workload
with D-054's installing task deleting a random span of one to twelve keys every one to
five milliseconds, and every crash aimed at a delete as D-054 aims at an install. The
oracle is the install's with nothing installed: a delete in force leaves no table in
service and no replayed record holding a write of its span below it, a delete not in
force holds nothing, and the state check reads the span empty but for the writes after
the delete.

*The sweep.* The default schedule of `sim/engine.rs` now runs all four primitives beside
the Phase 1 workload: the installing task installs a checkpoint of a span two times in
three and deletes a span the third, and the readers seek and every recovery is walked by
seeks. Its crashes are not aimed; `Schedule::install()` and `Schedule::range_delete()` aim
them. `Schedule::phase_1()` runs none of it and is the schedule every seed ran before this
part of the entry, and each primitive's own schedule is `phase_1()` with that primitive.
The two seeds the sweep pins keep the Phase 1 schedule, on which each was found: seed 420
and seed 44 in both its modes. Re-audited: each run's moirae trace hashes the same on
268cf58, the tree before D-054, on 7e3d25d, D-054's commit, and with `Schedule::phase_1()`
on the tree committed as 9eb28e4, this part's commit (`a858ef4b153bf4c6`,
`dabd6adfaee000f5` and, refusing fallbacks, `977a703fb51c96e8`), so neither schedule moved
and seed 44's assertions of its fallbacks still bite on the run they were written for.

*Re-audited after both reviews.* Four of the review fixes' commit messages say no pinned
trace hash moves: 86d9cc2, 447a9e9, 01ef12c and 3787528. None had a run behind it when
it was written; each claim was reasoned from what the commit changed. The harness was run
afterwards, over an archive of each commit, and on every commit since the one before them:
9eb28e4, 86d9cc2, 447a9e9, 01ef12c, 3787528, a759732, d672fec and 44db7c6, the last with
code. On all eight, seeds 420 and 44 in both modes hash `a858ef4b153bf4c6`,
`dabd6adfaee000f5` and `977a703fb51c96e8`, and the raft, membership and quorum scenarios'
seed 42 hash `5a858d67288924ec`, `34f120f8565660e1` and `ea7f7371d5c8cda4`, as on 268cf58
and 9eb28e4 before. So the four claims hold, now with a run behind them. The same run
hashes the default schedule's seed 42 as a control, since that run installs and deletes
spans in each tree's own engine and must move when the engine's schedule does:
`9aaa19e003755c18` on 9eb28e4 and 86d9cc2, `1906280227f47acc` on 447a9e9, whose spawned
task draws from the stream, and `1c3d09e48dbca635` on 01ef12c, whose store further along
draws and writes. It stays `1c3d09e48dbca635` on 3787528, a759732, d672fec and 44db7c6,
whose messages say they move no schedule or no trace: the claims of a759732 and d672fec
were likewise reasoned, not run, when written, and on this seed the run agrees.

*Measured*, in release beside another lane's builds (load averages 10 to 47),
`cargo test -p ananke-sim --release --test engine` at each tier: the correct engine
passes every seed of `Schedule::seek()`, with 3 836 live seeks at twenty seeds, 2 589 of
them stopping at their limit, and 1 643 seeks walking recovered engines; at a hundred,
19 492, 13 261 and 8 199; at a thousand, 196 904, 132 680 and 80 402.
`SeekCountsTombstones` is caught on 20 of 20, 98 of 100 and 972 of 1000, the first at
seed 0: *seek of 1 of k15..k30 at version 17 saw 0 keys but the model has 1*.

The correct engine passes every seed of `Schedule::range_delete()`: at twenty seeds 387
deletes were asked for and 289 resolved, 150 crashes were aimed at one, and after them 267
deletes that had resolved and 16 that had not were in force, 79 that had not were not, and
13 that had resolved were lost to a fault's fallback, with 5 906 keys written after a delete
in force checked over it; at a hundred, 1 903 and 1 448 deletes, 711 crashes aimed, 1 276
and 44 in force, 397 and 118 not, and 24 248 keys; at a thousand, 19 767 and 15 058
deletes, 7 363 crashes aimed, 13 189 and 456 in force, 4 148 and 1 419 not, and 255 522
keys. `RangeDeleteSkipsMemtables` is caught on 17 of 20, 92 of 100 and 947 of 1000, the
first at seed 0 by a live scan that saw a deleted key: *scan of k16..k42 at version 21 saw
4 keys but the model has 3*.

The sweep's own tests on the default schedule, before this part of the entry (268cf58 and
7e3d25d, identical) and after it, at each tier:

| tier | correct engine | `NoWalBeforeMemtable` | `ReleaseBeforeManifest` | `DeleteBeforeManifest` |
| --- | --- | --- | --- | --- |
| 20 | every seed, before and after | 19 → 20 | 13 → 12 | 10 → 11 |
| 100 | every seed, before and after | 98 → 97 | 64 → 62 | 55 → 62 |
| 1000 | every seed, before and after | 984 → 981 | 627 → 623 | 570 → 630 |

With the oracle tightened as D-054 records, on the same schedules, the three are caught on
20, 13 and 11 of 20, 97, 66 and 62 of 100, and 981, 645 and 632 of 1000: the tightening
can only add catches, and it added them to `ReleaseBeforeManifest` (623 → 645) and
`DeleteBeforeManifest` (630 → 632), whose losses a fallback had been forgiving.

Every seed's schedule moved, since a new task draws from the node's stream and every
install and delete writes, flushes and switches, so which seeds catch a variant changed
and each rate is the one measured on the new schedules, not the old ones shifted. The
correct engine's coverage at a thousand seeds says what moved. Tables written rose from
45 781 to 64 898 and compactions from 13 375 to 14 678, since installs and deletes add
level-0 tables and rewrites and each forces a flush; crashes landing inside a compaction
nearly doubled, from 595 to 1 166, and `DeleteBeforeManifest`, which is caught only by a
crash between a compaction's deletion of its inputs and its manifest, rose from 570 to
630. Flushes rose from 31 040 to 32 297 and crashes with a memtable mid-flush from 4 369 to
6 061, but `ReleaseBeforeManifest` did not follow, 627 to 623: its catch needs a crash
after its early release and before the manifest, with the released segments' records
owed and no fault explaining their loss, and the four seeds' difference was not traced
further. `NoWalBeforeMemtable`
is caught whenever a crash follows a write acknowledged without a sync, on nearly every
seed on either schedule, 984 and 981. Scans fell from 395 698 to 197 937 because half of
them are now seeks (199 599), and 4 481 checkpoints opened fresh after a crash where 2 657
did, the span checkpoints among them; beside those, 4 806 installs, 4 098 range deletes,
7 817 span checkpoints and 76 453 seeks walking recovered engines, every seed passing.

*After review.* Independent review of 9eb28e4 changed what these numbers stand on, and
they are measured again on the tree of its fixes (D-054 records the oracle's two tightened
excuses, the install's own task, the store further along and the observed numbers). The
correct engine passes every seed of the range delete's test, the seek's and the default
sweep at 20, 100 and 1000. The range delete's crash test, with the crashes before a
delete's switch split as D-054 splits an install's:

| tier | deletes asked / resolved | crashes aimed | in force after a crash (resolved / not) | not in force: before the record was durable / between replacement and switch / otherwise / resolved, a fault | span keys written after a delete, checked | reads left unjudged |
| --- | --- | --- | --- | --- | --- | --- |
| 20 | 388 / 285 | 150 | 264 / 14 | 2 / 13 / 71 / 16 | 5 444 | 4 018 |
| 100 | 1 997 / 1 507 | 748 | 1 342 / 46 | 11 / 68 / 357 / 137 | 25 890 | 20 047 |
| 1000 | 19 950 / 15 205 | 7 414 | 13 334 / 456 | 154 / 659 / 3 374 / 1 497 | 259 253 | 202 429 |

`RangeDeleteSkipsMemtables` is caught on 15 of 20, 89 of 100 and 933 of 1000, the first now
at seed 0 by the direct check of Q2's criterion: *the install at record 22 is in force
(manifest 11) but table 8 still holds the write of k40 at record 11: a mixture*. The seek's
test runs no install, so its schedule did not move: its numbers above stand, and
`SeekCountsTombstones` is still caught on 20 of 20, 98 of 100 and 972 of 1000.

The default sweep's three Phase 1 variants on the moved schedules (the install's task and
the store further along both draw and write): `NoWalBeforeMemtable`,
`ReleaseBeforeManifest` and `DeleteBeforeManifest` are caught on 20, 10 and 11 of 20, 99,
51 and 66 of 100, and 985, 602 and 645 of 1000; the second review's tightening of the
oracle (D-054) moves `DeleteBeforeManifest` to 12, 67 and 647 and nothing else.
`ReleaseBeforeManifest` fell, from 645 on the same oracle a schedule earlier to 602,
though crashes with a memtable mid-flush did not (6 061 then, 6 109 now, at a thousand
seeds). Its catch needs a crash between its early
release and its manifest with the released segments' records still owed and no fault
explaining their loss, and every schedule move reshuffles which seeds meet all three; the
difference was not traced seed by seed, and it stays caught on some seed at every tier.
Coverage at a thousand seeds: 66 992 tables written, 14 815 compactions, 1 327 crashes
inside a compaction, 1 858 whole-store checkpoints opened fresh after a crash, 8 130 span
checkpoints taken, 5 806 installs, 3 998 range deletes, 196 205 seeks and 75 602 seeks
walking recovered engines.

*Deep levels.* `Schedule::deep()` is the default schedule with small level limits, so it
moved with the default. At a thousand deep seeds, as the nightly runs them, every seed
passes, and compaction wrote from level 2 or deeper in 10 132 rounds on 268cf58, in 11 599
on 9eb28e4's schedules and in 11 609 on the tree committed as 3787528, reaching level 3
each time. Its reach
did not drop, so `deep()` stays on the default schedule rather than on `phase_1()`.

*What review found in the cost.* Review found the engine binary's cost grown several
times over by this entry and D-054, much of it variants caught on nearly every seed and
swept over every seed. The four caught on four seeds in five or more,
`SpanCheckpointUnsynced`, `SeekCountsTombstones`, `RangeDeleteSkipsMemtables` and
`InstallKeepsSourceNumbers`, now run a share of the tier's seeds,
`max(seeds / 10, min(seeds, 20))`: twenty at the gate, twenty in CI, a hundred at the
premerge and a thousand at the nightly. Each still asserts a catch at every tier. A share
is the tier's first seeds, so its rates are the ones measured above at that many seeds: at
twenty 17, 20, 15 and 20; at a hundred 81, 98, 89 and 98 (the premerge below printed the
same); at a thousand 816, 972, 933 and 975. `SpanCheckpointUnsynced` is the lowest, about
four in five since half the sources became the store further along, and at a share of
twenty it still expects sixteen. `InstallInTwoSwitches`, caught on about one seed in two,
and the three Phase 1 variants, whose tests D-052 measured, run every seed as before.

`scripts/premerge.sh` at a thousand seeds on the tree committed as 3787528, measured as
D-052 measured it (a
warm build first, no other lane building at its start or its end, the one-minute load
sampled every 15 s): **540.37 s** real at a mean load of 17.64, 3 914.07 s user, the raft
binary, which this entry does not touch, 306.86 s and the engine binary 213.06 s, against
**374.64 s** at 13.90 on 1ef6d7e: 44% more wall time, most of it the engine binary. The
engine binary on 268cf58, whose engine sweep is 1ef6d7e's, took 72.51 s at a thousand
seeds on the same laptop the same day, so it now costs 2.94 times as much.

*The nightly.* On 1ef6d7e (run 34901799989) the engine binary took 1 346.89 s at ten
thousand seeds and the whole `cargo test` step 2 h 1 min 15 s, against the job's
300-minute timeout. Scaled by the same ratio, the engine binary would take about 3 958 s,
some 44 minutes more, and the step about 2 h 45 min, a little over half the timeout
(**issue #57**, filed on the owner's instruction of 2026-09-15: shard the ten thousand
across parallel jobs, or split the job per sweep, before it starts timing out). The
scaling overstates the deep-levels test, which runs a thousand deep seeds at every nightly,
took 11.38 s of them on this laptop, and whose rounds rose by a seventh (above), not by the
sweep's ratio; it leaves out what the other Stage A lanes add.

**Alternatives.** Range tombstones, as RocksDB's `DeleteRange`: no forced flush and no
rewrite, but a new kind of write that the memtable, the table format (a version bump), the
merge, every read, compaction and its truncation at table boundaries, and the oracle would
all have to learn, when the deletes Phase 3 names are rare and whole-range. A batch of one
tombstone per key, as today: a collected replica's span leaves a tombstone per key until
compaction carries it to the bottom, and every scan of the span walks them. A seek that
counts deleted keys toward its limit: a meta lookup of one record could come back empty
with records past a deleted one, which is the variant. A limit on entries read rather
than keys returned: a caller could not tell a short range from a range of tombstones. A
reverse seek now: no consumer in Phase 3.

**Consequences.** The engine sweep's default schedule moved for every seed, and its
three Phase 1 variants' rates with it (above); no pinned trace hash moved, and seeds 420
and 44 keep the schedule they were found on. The gate's engine tests take longer. `scan`
stays as it was; a seek costs what the part of a scan it walks
costs, and no more than `limit` live keys past the deleted ones it passes over. A range
delete costs what an install costs: a flush of the memtables at or below it and a rewrite
of every table straddling the span's edges, all under the turnstile, so no flush,
compaction or checkpoint runs on the engine meanwhile, and it shares the install's
`InProgress` rule: Stage B's one apply task serialises installs and range deletes, and any
other caller retries (D-054). A snapshot is not stable across a range delete of the span it
reads either, and Stage B holds none across one (D-054). Neither primitive's own commit
moved a seed's schedule; the sweep's did.

---

## PROPOSED D-056 — `SimEnv`'s send queue: bounded, drop-oldest, per sending socket and destination, drained at a modelled link rate

*Decided in part: the tier of `RefusalNotDurable`'s catch and the pinning of its first
seed are the owner's answer 1 of 2026-09-15 and are decided. The queue model itself is
proposed.*

**Context.** D-015 gives every destination of a socket a bounded queue whose overflow
drops the oldest frame with a `MessageDropped` event. `RealEnv` has it: one queue of
`SEND_QUEUE_LEN`, 1 024 frames, per destination, popped by a task that writes one frame
at a time over the destination's TCP connection (crates/ananke-env/src/real/net.rs). The
simulator had none: `send` drew a delay and delivered into the destination socket's
unbounded inbox, so no sweep ever saw a queue-full drop. SHARD.md §11 (env 3) and the
owner's answer to Q16 settle that `SimEnv` gains the queue as a simulator model, per
(sending socket, destination), drop-oldest, traced with `DropReason::QueueFull`, its
capacity a `SimConfig` setting defaulting to 1 024, filled against a modelled per-link
drain rate, landed before batching puts many ranges on one socket, and moving every
pinned hash once in one commit with every pinned seed re-audited. They do not settle
what the drain rate models, its default, where the queue sits among the fault model's
draws, or what becomes of a closed socket's queue. Those are proposed here.

**Decision.** Every site is marked `PROPOSED(D-056)`.

- *Where the queue sits.* A frame that survives the send's checks — a partitioned or
  length-limited link, then the injected drop — joins its sending socket's queue to its
  destination. Its delay, and a duplicate's, are drawn at the send from the `net`
  stream in the order they always were, so no fault draw moves (D-017). Partitions and
  frame-length limits are still checked again at delivery, and an unbound destination is
  still `Unreachable` there.
- *What drains it.* The queue writes one frame at a time, oldest first, at
  `NetFaults::link_bytes_per_sec`: a frame starts when it is sent or when the frame ahead
  of it is written, whichever is later, and takes its length over the rate, rounded up
  to the nanosecond. Its delivery, and its duplicate's, is its drawn delay after its last
  byte is written. So a frame sent behind others waits for them, as a frame waits in
  `RealEnv`'s queue while the connection writes the ones before it, and the queue fills
  when one socket sends one destination faster than the link drains, which is `RealEnv`'s
  reason and no invented count.
- *The bound.* The frame being written is not counted, as `RealEnv`'s task pops the
  frame it writes; the rest wait. When `NetFaults::send_queue_len` frames wait, a send
  first drops the oldest waiting frame: its deliveries (a duplicate's with it) are
  cancelled, `MessageDropped { reason: QueueFull }` is traced on the sender's node under
  the dropped frame's id, after the new frame's `MessageSent`, as `RealEnv` emits them, and
  the frames behind it move up.
- *The settings.* `send_queue_len` defaults to `real::SEND_QUEUE_LEN`, one constant for
  both environments, and must be at least 1; `link_bytes_per_sec` defaults to
  125 000 000, a gigabit (`sim::GIGABIT_BYTES_PER_SEC`), and must be positive. Both are in
  the moirae header's `config`, which no pinned hash covers.
- *A closed socket.* A socket dropped, or unbound by its node's crash, forgets its
  queues; frames already in them keep the deliveries they were given, as a frame in
  flight always has.

The model is held by the simulator's own tests: a frame waits only for the frames ahead
of it on its own socket's link, a drained queue holds nothing back, and a full queue
drops its oldest waiting frame, never the one being written nor the newest, cancels a
duplicate with it and moves the survivors up (crates/ananke-env/src/sim/tests.rs).

**What moved.** Every frame now arrives its write time later — 16 ns for two bytes and
800 ns for a hundred at a gigabit — from the first frame of every run, so every trace and
every schedule after a run's first delivery moved. The echo scenario's pinned body hash
moves from `19f19201df99a799` to `fcbe82ee7a0ba672` (sim/tests/echo.rs); the moirae
repository's copy of `echo-42.jsonl`, the studio's fixture pinned to the same value, needs
the same update there. Every pinned seed of sim/tests/raft.rs was re-audited on the moved
schedules, each asserting its mechanism or, with the reason, its absence:

- Seeds 164, 385 and 7381 still do not reach their situations, now with no timer gap on
  the AppendEntries-only replay (164), none on the replay without D-039's arm (385), and
  every install above its server's floor (7381); seed 6325 still crashes inside no
  adoption window, under the correct server or as built.
- Seed 5909 reaches D-042's refusal, reset and re-seed on server 1 instead of server 3.
  Under `IgnoreIncarnation` alone the stale progress for server 3 is still reached; the
  harmless re-take under a live stream under `SharedSnapshotDir` and the pair is gone, since
  no index is taken twice, and under the pair no refused follower is left stale; each is
  asserted, the absences with their reasons.
- The pair `{IgnoreIncarnation, SharedSnapshotDir}` no longer wedges seed 680. As SHARD.md
  §12 asks, the first thousand seeds were searched again: the pair is caught on 2 of 1000,
  seeds 132 and 848, both by the liveness check, on both of which `SharedSnapshotDir` alone
  is caught with the same message and `IgnoreIncarnation` alone passes, and no seed catches
  the pair without a single. Seed 132, the first, is pinned as D-045 pinned 680, with the
  wedge's mechanism asserted on it (re-takes into the leader's own directory under live
  streams, the duplicate-file loop, both followers uncounted); seed 848's wedge has no
  duplicate-file loop. Seed 680's test now asserts what the seed does instead: the pair,
  each half alone and the correct server pass it; the stream half re-takes under a live
  stream and still commits; `IgnoreIncarnation` alone leaves server 1's progress stale with
  server 2 countable. RAFT.md §5 names seed 132. *(Superseded by D-060: the key layout moved
  every raft schedule again, and on that tree the pair is caught on 0 of 1000 and each half
  on 0. Seed 132's test now asserts that absence, and RAFT.md §5 says so.)*
- Seed 687 comes nearer its situation. As built, server 1 is refused for lost state and
  restarted four times before an install replaces its store, the first half of the
  premerge's failure; the second is absent, since the refused engine flushes nothing before
  the crash and every later open is refused for the log's missing head, and that is asserted.
  Under the correct server the refused engine's quiesce is reached and asserted: engine
  quiesced, store refused, and no flush, manifest, `CURRENT` switch, segment deletion or
  restart on the node until the re-seed is adopted.
- The nightly's seeds 1885 and 2023, its eleven variant catches of the same gap, and the
  28 catches the nightlies removed no longer reach a term change straddling an isolation's
  start, and each asserts that absence and that the named isolation is either gone from its
  server or, on six seeds, still there with the server keeping its term. D-047's straddle is
  now pinned on seed 4 of the directed term-raise schedule, which reaches it seven times, and
  D-050's shape, which seed 4 no longer reaches, on seed 1 of that schedule, the lowest that
  does.

**Measured.** On the tree with the queue, in release: the correct server passes every
seed of `sim/raft.rs`, `sim/membership.rs` and `sim/quorum.rs` at 20, 100 and 1000, and
the raft sweep's coverage prints `queue_drops: 0` over the correct server's thousand seeds.
The raft sweep's rates at 1000 seeds are compared with 268cf58, the lane's base before
D-057 and D-058, which change neither the raft scenario nor its server:
`ApplyBeforeCommit` 882 (891), `CountOlderTermForCommit` 454 (463), `ResetTimerOnAnyRpc`
336 (352), `SnapshotWithoutCurrentLast` 336 (340), `AdoptionAsBuilt` 77 (57),
`RefusalNotDurable` 16 (14), `SharedSnapshotDir` 2 (1), `IgnoreIncarnation` 0 (0), and
`SendBeforePersist`, `TruncateOnEveryAppend`, `NoPreVote`, `RefusedCountsForQuorum` and
`RefusedNeverCounts` on every seed as before; the lease trial revoked on all 503 seeds
beyond the drift bound and caught 41 stale reads (49). Every change is the moved schedule's
draw and within the binomial spread of the rate the nightly measured. `AdoptionAsBuilt`'s
storm is drawn on the same 260 seeds on both trees, and at its ten-thousand-seed rate, 646 of
10 000 (6.46 %, D-047, DECISIONS.md:3144), a thousand seeds catch 64.6 on average with a
standard deviation of 7.8: 57 is one below the mean and 77 1.6 above. The lease trials'
rate, 472 stale reads in 5 023 exceeded seeds at ten thousand (9.4 %), gives 47.3 of 503 with
a deviation of 6.5: 49 and 41 are each within one. The membership scenario is compared with
the lane's tip without the queue, 9432fed: its correct server passes every seed at 20, 100 and
1000 with a joining server fed a snapshot in its learner phase on every one (59, 377 and
3 957 installs; 63, 388 and 4 056 without the queue), no refusal and no fallback, and
`SingleMajorityInJointConsensus` is caught on 8 of 20, 31 of 100 and 296 of 1000 (6, 35 and
301 without the queue).

**The tier of `RefusalNotDurable`'s catch: the owner's decision of 2026-09-15.** This part
is decided; the queue model above stays proposed. `RefusalNotDurable`'s test asserted its
catch from the hundred-seed tier (D-044). On the tree with the queue its rate is 16 of 1000
against 14 before, but none of the 16 is below seed 100 (the first is 119, where it was 80),
so the test failed at CI's hundred seeds, and the change was held off the lane's branch for
the owner. The owner decided:

- the catch is asserted from the thousand-seed tier — `scripts/premerge.sh` and the nightly
  — and at no lower tier;
- the fault's firing, a crash landing on a server sitting refused, stays asserted at every
  tier, and the catch rate is printed at every tier;
- seed 119, the first of the thousand's catches, is pinned in its own test beside the sweep,
  `seed_119_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss`, asserting
  its mechanism at every tier. As built, server 3 is refused for lost state (tables 29 and
  31 dropped at its open), crashed while refused, its refused engine flushes over the loss
  (a manifest without either table, log segments deleted), and its next open recovers clean
  with an applied index of 330 before an install, which state machine safety reports;
  decision time does not move the verdict. Under the correct server the schedule leaves the
  variant's at server 3's first refusal, which the durable mark's write and sync trace 4.9 ms
  later; the fault's three later rounds still crash server 3 inside a flush, but no open
  drops a table, so there is no refusal for lost state and no crash on a refused server,
  asserted with that reason, and the run passes. The correct server's quiesce and durable
  refusal stay pinned on seed 687. *(Superseded by D-060: the key layout moved every raft
  schedule again, the catch is 9 of 1000 with its first at seed 158, and seed 158's pin
  carries both halves — the laundered store as built and the quiesce with the durable
  refusal under the correct server. Seed 119's test asserts what it does instead, and seed
  687's the half it still reaches.)*

The reason is binomial. At the nightly's rate, 132 of 10 000 (1.32 %, D-047,
DECISIONS.md:3141, measured before this entry), a hundred seeds catch none with probability
0.9868^100 = 0.26, about one run in four, and the gate's twenty with probability 0.77; at
this tree's 16 of 1000 a hundred still miss with probability 0.20. A thousand miss with
probability 0.9868^1000 = 1.7 × 10^-6. Below a thousand seeds the assertion fails a tree with
nothing wrong on the draw alone; at a thousand the statistics support it. This supersedes D-044's
hundred-seed tier for this test alone; D-044's fix, its fault and its firing at every tier
stand. It supersedes SHARD.md §12's Stage B plan for the same assertion
(docs/SHARD.md:2348, "`AdoptionAsBuilt` and `RefusalNotDurable`: … the catch from the
hundred-seed tier"), as the approved plan's text; `AdoptionAsBuilt` stays where the plan
puts it. SHARD.md's list does not mention the two membership counters D-058 and D-061
move, `reverts_to_a_prefix` and the election while joint; those entries are the record.

**Alternatives.** *No write time*, a queue that only counts frames sent at one instant:
nothing drains it, so it could never fill against a rate, and its drops would be an
invented count. *A byte bound*: `RealEnv`'s is in frames. *One rate per node, shared by its
connections*: `RealEnv` writes each destination over its own connection, and D-015's bound
is per destination. *A down or partitioned destination as a link that does not drain*:
`RealEnv`'s frames to a peer that is down wait through the connection's backoff and are
written, stale, when it returns, and a partitioned connection stalls; modelling that would
replace `Partitioned` and `Unreachable` drops with frames delivered late, a change to Phase
2's network fault model beyond Q16, not taken and recorded here as the divergence it is.
*Losing a closed socket's queued frames*, as `RealEnv` does with the socket's tasks: it
would change what a crash does to frames already sent, which the fault model has always
delivered. *A faster default*: ten gigabits would queue less; a gigabit, the slowest common
server link, is the conservative choice.

*Where the model still differs from `RealEnv`, not changed here.* `RealEnv`'s task pops a
frame as soon as its connection's write returns, and a write returns once the bytes are in
the kernel's send buffer, not on the wire; the simulator has no such buffer and drains only
at the link's rate, so a burst to one destination fills its queue sooner than `RealEnv`'s,
and the simulator drops earlier — the conservative direction, since a drop is what the
protocol must survive. Two omissions point the other way, and are smaller wherever a backlog
outgrows the send buffer: the simulator does not count `RealEnv`'s 12-byte frame header, a
4-byte length and an 8-byte message id, over a fifth of a 53-byte heartbeat (SHARD.md §4's 61 less the range id), nor
the hello frame at each connection's start; and it has no connect or reconnect delay, during
which `RealEnv`'s queue does not drain at all. So the claim is that the simulator's queue
drops no later than `RealEnv`'s for a sustained backlog, not for every burst.

**Consequences.** No Phase 2 scenario sends one destination fast enough to fill a queue,
so queue-full drops wait for Stage B's batch frames, where many ranges share a socket. Any
future change to a frame's length moves every schedule, as any change to a record's size
always did (CLAUDE.md).

---

## PROPOSED D-057 — A named stream per node and range: `Environment::range_rng`

**Context.** SHARD.md's Q13, approved: a named stream per node and range,
`n{id}/r{range}/protocol`, through a new `Environment` method, derived from the seed in
`SimEnv` and drawn from OS entropy in `RealEnv`, decided before the stage that first pins
Phase 3 seeds. Protocol code reaches randomness only through `Environment::rng` and
`sched_rng` (crates/ananke-env/src/env.rs), and a core's generator is seeded from the
node's protocol stream at its incarnation's start, so with many groups on a node a split
would move every later range's election timeouts, against D-017's purpose (SHARD.md §11,
env 2). Q13 settles the name and the two sources. It does not settle the method's shape —
a range id or any label — whether a call starts the stream over or continues it, or how
the simulator keeps it. Those are proposed here.

**Decision.** Every site is marked `PROPOSED(D-057)`.

- `Environment::range_rng(&self, range: u64) -> Self::Rng`: this node's stream for
  `range`. It takes the range's id rather than a label, so the one name Q13 decided is
  the only name it can make: a label would let a caller ask for `protocol` or `sched`
  and be handed a second generator starting where the node's own stream starts, drawing
  the same numbers.
- `SimEnv` derives it with `moirae_sched::stream(seed, "n{id}/r{range}/protocol")` the
  first time the node is asked for that range and keeps it in the node's entry beside
  `n{id}/protocol` and `n{id}/sched`. Every later call and every handle to the node
  continues the same stream, as `rng` does, and it lives as long as the simulation, as
  the node's other streams do, so a restarted core does not draw its last incarnation's
  numbers again. Making it draws from no other stream.
- `RealEnv` returns `RealRng`, OS entropy, as for every other stream.
- Nothing calls it yet. Stage B seeds each core from it, in a commit of its own that
  moves every election timeout (SHARD.md §12, Stage B). No trace, hash or schedule moves
  here.

The simulator's tests hold it: the same seed, node and range give the same draws, and
another range, node or seed other draws, which are moirae's derivation for the name;
taking range streams and drawing from them, interleaved, leaves the node's protocol and
scheduling streams, another node's and another range's exactly as they draw without, a
node added after the takes draws the same clock skew and drift, and a scenario with
drops, duplicates and random delays whose task takes range streams in one run only
records the same trace (a draw from the `clock` or the `net` stream added to the
derivation fails it); and every handle continues one stream
(crates/ananke-env/src/sim/tests.rs). Under `RealEnv`
two draws differ (crates/ananke-env/tests/real_env.rs).

**Alternatives.** *A label* (`stream(&self, label)`): general enough for the rebalancer's
named stream Q42 mentions, but able to alias the node's own streams; a second kind of named
stream can have its own method when something needs it. *A fresh stream per call*, starting
the sequence over: two handles would draw the same numbers, and a range's core restarted at
a new incarnation would draw its last incarnation's election timeouts again, where the node's
protocol stream continues across incarnations today. *Seeding a range's stream from the
node's protocol stream*: Q13's reason against it, a split moving every later range's draws.
*Returning a reference, as `rng` does*: the simulator keeps the streams under its lock and
cannot lend one out; a `SimRng` clone shares the stream and `RealRng` is a unit, so an owned
handle costs nothing.

**Consequences.** Both environments in the workspace implement one more method. Stage B's
switch of each core's seed to its range's stream is the change that moves schedules, and
this entry is what it switches to.

---

## PROPOSED D-058 — The membership scenario past the snapshot threshold

*Decided in part: which part of issue #46 is met and the tier of `reverts_to_a_prefix`
are the owner's answers 3 and 4 of 2026-09-15 and are decided. The scenario's shape is
proposed.*

**Context.** Issue #46 and SHARD.md's Q34, approved: extend `sim/membership.rs` past the
snapshot threshold before `sim/move.rs`, so that during 3 → 5 → 3 a learner or joining
voter is fed by a snapshot, asserted per seed; the correct server passes every seed and
`SingleMajorityInJointConsensus` is still caught on some seed at every tier. The scenario
ran `RaftConfig`'s default threshold of 4 096 entries and never crossed it
(OVERNIGHT.md:188-191), so the two pieces of Phase 2 code the issue names, the
configuration key an install's repair writes and the floor a truncated configuration
entry reverts to (D-029, D-030), had only unit tests. Q34 settles that the scenario is
extended. It does not settle how a snapshot feed is made certain on every seed, and a
lower threshold alone does not make it: only a leader compacts (D-030), a leader elected
from followers holds a whole log, and such a leader catches an empty learner up with
entries from index 1. With the threshold lowered and the grow asked as before, 39 of the
first 1 000 seeds feed no joining server a snapshot in its learner phase.

**Decision.** Every site is marked `PROPOSED(D-058)`.

- The membership servers run the raft sweep's `snapshot_threshold` of 12 and
  `snapshot_chunk` of 4 096, so leaders take checkpoints and compact routinely.
- The operator asks for the grow only of a leader that has compacted since it took
  office — its `RaftCompacted` after its `RaftLeader` in the trace — waiting for one in
  slices for at most two seconds, and asks that leader alone: a request that finds it no
  longer leading ends there rather than following the hint, and the driver's next attempt
  waits for a compacted leader again. A learner's catch-up starts only when a leader
  accepts the request, and is the leader's alone (D-032), so the first leader to catch
  servers 4 and 5 up has a compacted prefix, and their first rejection, asking from index
  1, lands below it: they are fed a snapshot. When no compacted leader appears within the
  budget the request goes to the leader in force, so a seed without a snapshot feed is
  reported rather than hidden; `Report::compaction_fallbacks` counts those requests and
  `Report::compaction_waited` the time waited. The shrink is asked as before.
- `Schedule::total`, the run-length hint's estimate (D-016), does not count the wait. It is
  at most 300 ms of a run over the first 1 000 seeds (a mean of 3.85 ms, no fallback),
  where counting its budget, eight seconds over four attempts, would have lowered PCT's
  change-point rate on every run for time no run spends.
- `Report::snapshot_fed_joiners` is every snapshot a joining server installed in its
  learner phase: from the operator's first request for the grow (`Report::grow`) until the
  first joint configuration naming that server in `new` takes effect on any server, or,
  when none does, until the driver stops driving the grow. Installs after that — by a voter
  of the joint or new configuration, in the transfer's wait, the shrink or the settle — are
  not counted, nor an install's restatement at its adoption. The correct server's sweep
  fails any seed with none and its coverage asserts one on every seed; the variant's sweep
  prints how many of its runs had one; the coverage prints the seeds with a fallback.
- The run's check fails on any `RaftRefused` whose reason does not start with
  `LOST_STATE`. No crash is scheduled here, so a refusal can only come from an install's
  adoption, and a configuration key its repair wrote out of step with the log refuses the
  store at the open (store.rs) and puts the server in re-seed mode, which traces no
  failure and whose re-seed install would otherwise count as a feed. The coverage counts
  adoptions and refusals by the reason's first clause and asserts, at every tier, that none
  is for anything but lost state.

**What the extension found.** Measured on the lane's tree, before D-056's send queue moved
every membership schedule; the figures on the tree with the queue follow, under *On the tree
with D-056's queue*. No bug in the Phase 2 code it was aimed at, and one trace
inconsistency in the install path, returned to the owner unfixed:

- At 20, 100 and 1 000 seeds in release the correct server passes every seed, every seed
  feeds a joining server a snapshot in its learner phase (63, 388 and 4 056 installs), and
  no operator request fell back (the longest wait 300 ms). Every install is adopted — 109,
  651 and 6 557 adoptions — and the open after each checks the configuration key its repair
  wrote against the log; no store is refused and no server fails.
- The key repair's non-trivial branch is not reached. An install keeps a tail of the
  receiver's log only when the receiver holds the snapshot's last index with its term, and
  a server is fed a snapshot here because its log ends below the leader's prefix: over
  1 000 seeds no install kept a tail, so none wrote the key from a configuration entry in a
  tail. Every install wrote the snapshot's own configuration.
- The core's revert floor is not reached either. No truncation in a running core restored
  a compacted or installed prefix's configuration over 1 000 seeds, on the lane's tree, on
  268cf58, or with the threshold alone. What the coverage's `reverts_to_a_prefix` counts, 4 at
  100 seeds and 44 at 1 000 on the lane's tree, are installs whose snapshot's configuration
  is older than the receiver's in force, which take the receiver back to the installed
  prefix; the floor itself is `truncation_reverts_to_a_prefix`, 0. Issue #46's second item
  needs a follower that installs and then appends and truncates a configuration entry above
  its prefix; this scenario does not build it. The owner deferred it, with the key repair's
  tail branch above, to issue #56 (*Issue #46: met and deferred*, below).
- The install's `RaftConfig` (node.rs, `Snap::Finish`) writes the configuration's voters
  into `new` when it is not joint, where `TraceEvent::RaftConfig` documents `new` as empty
  outside a joint configuration and the core and the restatement write it empty; the same
  configuration is traced two ways within one install. The checks read `new` only when
  `joint` is set, so no verdict depends on it; the studio shows it. Not fixed here.
- `SingleMajorityInJointConsensus` is caught on 6 of 20, 35 of 100 and 301 of 1 000 seeds
  (275 of 1 000 on 268cf58).

The coverage at 1 000 seeds against 268cf58: grows and shrinks completed on every seed both
times; joint configurations taken 10 729 (10 663); new configurations taken 22 861 (9 760),
since each install traces its snapshot's configuration and its adoption re-states it;
learners promoted 2 524 (2 470); elections while joint 33 (51); step-downs of a leader outside
`C_new` 412 (435); completed operations 348 849 (287 719); worst completion gap 352.96 ms
(382.24 ms); slowest first write after the last heal 471.89 ms (450.07 ms); configuration
reverts 69 (709). The reverts fall because installs now take conflicting configuration
entries out of force where truncations did. On 268cf58, 717 configuration entries a server
held in force had another term in the committed log at that index, and 709 of them left
force by a truncation, each a revert. On the lane's tree 780 did: 25 by a truncation, 746 by an
install, and 9 not before the run ended. Of those 746, 44 took the receiver to an older
configuration and count as reverts, and 702 took it to a newer one, which the counter,
comparing indices, does not see as leaving an entry out of force. The threshold alone gives
79 reverts (29 truncations, 50 installs).

Timings, the two membership tests at 1 000 seeds in release, another agent's sweeps sharing
the machine: on a563205, the commit before this review's changes, 26.0 s with the one-minute
load at 18.4 and falling at its start, and 14.0 s at 10.1 to 11.6, against 10.4 s on 268cf58
at 9.8 to 10.1; on the lane's tree 26.2 s at a mean load of 84.0 over six samples, against 19.7 s on
268cf58 at 79.6 over four. The load moves these more than the change does.

Every figure above has its command and output in the lane's scratchpad, `scratchpad stage-a/n/audit-d058`:
`run-audit.sh` runs them all; `membership-{20,100,1000}.log` and `membership-1000-268cf58.log`
are the tests' coverage and timings with their load samples; `feeds-new.log` and
`feeds-threshold-alone.log` are the learner-phase feeds, fallbacks and waits
(`zz_m5_new.rs`); `traces-new.log`, `traces-threshold-alone.log` and
`traces-268cf58.log` are the reverts, truncations, installs, tails, refusals and conflicting
configuration entries (`zz_m5_trace.rs`).

**On the tree with D-056's queue.** D-056's send queue landed after the figures above and
moved every membership schedule; no verdict moved with it. The membership tests in release
on 4177c5b with this entry's tier change below (`RUSTFLAGS="-D warnings" ANANKE_SEEDS=<n>
cargo test --workspace --all-features --release --test raft -- --nocapture membership
one_majority`, `scratchpad stage-a/q/q2-membership-{20,100,1000}.log`; the same coverage at
100 and 1 000 in the whole raft suite on 4177c5b, `q/raft-{100,1000}-4177c5b.log`): the
correct server passes every seed at 20, 100 and 1 000, and every seed feeds a joining server
a snapshot in its learner phase (59, 377 and 3 957 installs), with no fallback (the longest
wait 50, 50 and 300 ms). Every install is adopted — 111, 641 and 6 377 adoptions — and no
store is refused. No install kept a tail or carried a configuration entry in one, and no
truncation reverted a configuration to a prefix, at any of the three
(`installs_keeping_a_tail`, `installs_whose_tail_carries_a_configuration` and
`truncation_reverts_to_a_prefix` all 0). `reverts_to_a_prefix` is 0, 1 and 28: once on each
of 28 seeds of the thousand, and of the first hundred on seed 97 alone
(`q/probe-reverts.log`, a throwaway copy of the test printing each seed's count, deleted
and never committed). `SingleMajorityInJointConsensus` is caught on 8 of 20, 31 of 100 and
296 of 1 000 seeds.

**Issue #46: met and deferred — the owner's decision of 2026-09-15.** This part is decided;
the scenario's extension above stays proposed. The owner accepted Q34 as "met for the
snapshot feed and the configuration key" and had the rest filed apart, so #46 is met in
part by Stage A and is not fully closed; SHARD.md §12's "Resolves #46" for Stage A means the
met part below.

- *Met in Stage A.* `sim/membership.rs` crosses the snapshot threshold. During 3 → 5 → 3 a
  learner is fed by an install from a compacted leader, asserted per seed. The configuration
  key the install's repair writes is exercised on that path, and the open after each
  adoption checks it against the log. The correct server passes every seed, and
  `SingleMajorityInJointConsensus` is still caught at every tier.
- *Deferred to issue #56*, filed 2026-09-15, "Membership: the truncation revert floor and the
  kept-tail key repair under snapshots (split from #46)". It holds the truncation revert
  floor, a running server's configuration reverting to its snapshot's when a configuration
  entry above the snapshot is truncated, reached on 0 of 1 000 seeds before the queue and
  after it. It also holds the install repair's kept-tail branch, never taken. Both keep their
  unit tests alone until #56 builds the shapes that reach them.
- *The tier of `reverts_to_a_prefix`.* Its count above zero is asserted from the
  thousand-seed tier, `scripts/premerge.sh` and the nightly, and at no lower tier. It stays
  printed with the coverage at every tier. It was asserted from a hundred seeds, where the
  lane's tree counted 4; on the tree with the queue a hundred count 1, on seed 97, and a thousand
  see it on 28 seeds. On the tree with the key layout (D-060) a thousand see it on 25 and a
  hundred on 3, seeds 40, 94 and 95. At that rate, 2.5 %, a hundred seeds see none with
  probability 0.975^100 = 0.080 and the gate's twenty with 0.60. Below a thousand the
  assertion would fail a tree with nothing wrong on the draw alone; a thousand see none
  with probability 0.975^1000 = 1.3 × 10^-11. The counter is installs taking a receiver back to an older
  configuration, not the revert floor #56 holds.

**Alternatives.** *The threshold alone*: 39 seeds in 1 000 without a learner-phase feed.
*Crashing or isolating a joining server until its leader compacts past it*: a second fault
in a scenario about membership under partition, and D-037's designation would feed it
only after two quiet election timeouts. *Asserting the feed only on seeds that reach it*: the
issue asks it of every seed, as §10's *every seed* standard does of a directed shape.
*Counting any install by a joining server during 3 → 5 → 3*: it counts voters fed after the
change, which the exit criterion does not ask for. *Directing the shrink too*: no server
joins in the shrink.

**Consequences.** The scenario's schedule moved: a lower threshold changes every
membership run from its first checkpoint on, and the run-length hint no longer counts a
wait. Its pinned assertions are the sweep's and the variant's, both re-run above. The worst
completion gap of 549.359683 ms that SPEC §3, RAFT.md §1 and the scenario's module comment
cite was measured at ten thousand seeds before this change; the next ten-thousand-seed
nightly re-measures it on the moved schedule, and those three places are marked as measured
before D-058. `sim/move.rs` can rely on a learner fed by snapshot during a change on one group,
not on the key repair's tail branch or the truncation revert floor, which this scenario does
not reach and issue #56 holds. If a seed at ten thousand exhausts the wait, reaches a leader that has not
compacted, or refuses a store, the correct server's sweep names it.

---

## D-059 — A store in 0.3.0's format is refused at open, never migrated and never read

**Context.** SHARD.md's Q5, approved as load-bearing, lets the key layout of Stage A's
item 6 break 0.3.0's on-disk format on three conditions:
1. the on-disk format version is bumped;
2. a store in the old format is refused at open with an error naming both format
   versions, never misread;
3. the break is recorded in the implementing entry and in the release notes
   (SHARD.md:71-76).

The owner's addition of 2026-09-15 to Stage A's entry criteria asks for two things
before the layout's code (SHARD.md:2115-2122):
- the decision on what becomes of a v0.3.0 store;
- the test that holds it: a store written by the v0.3.0 tag's own code, kept as a
  fixture rather than made by the new code, opened by the new code and refused with
  that error, with no key of it read as the new layout's.

The same test must pass on the tree after item 6 (SHARD.md:2144-2146).

Nothing records which format a store is in:
- the engine's versions (2 in the manifest, 2 in a table's footer) are unchanged since
  0.3.0;
- the store's `RAFT-STORE` marker carries none (SHARD.md §11, storage 1).

Under the new layout 0.3.0's keys lie where the new code looks for nothing. A 0.3.0
store the new code did not refuse would open as an empty one, the way seed 6325's voter
once came back blank (D-041).

**Decision.** The owner's, given in the brief of Stage A's lane L: a store in 0.3.0's
format is refused at open with a clear error naming its format version and the one the
code expects. It is never migrated, and never read under the new layout.

- *The versions.*
  - 0.3.0's store, which records no version, is Raft store format 1: the layout RAFT.md
    §3 gave at the tag.
  - Format 2 is the layout of item 6 and the only format this build writes or opens.
- *Newer versions too.* A store that records a version newer than the build's is
  refused the same way, naming both. A build cannot read a format it predates, and
  reading one would be the misreading Q5 forbids. (The owner, 2026-09-15.)
- *Not a loss, and not replaced.*
  - The store is whole in its own format, so the refusal is not D-022's refusal of lost
    state.
  - A server that finds one traces `RaftServerFailed` with the refusal's words and
    returns it.
  - It writes no lost mark (D-044) and does not wait in re-seed mode, where a leader's
    snapshot would take the store's place.
  - What becomes of the store is its operator's decision.
- *Read before anything writes.* The format is read before anything writes to the store
  directory, so a refused store is left byte for byte as it was found: no new log
  segment, no marker, no lost mark, no adoption. (The owner, 2026-09-15.)
- *The format before lost state.* The format is checked before any check for lost state:
  the engine's recovery, the marker, the lost mark and a staged install. A 0.3.0 store
  that has also lost state is refused for its format and never re-seeded into its own
  directory. (The owner, 2026-09-15.)

Where the version lives, how a fresh store is told from an unrecorded one, and the order
of the start are the mechanism, recorded in D-060, PROPOSED. Code sites of this decision
carry `// D-059`.

**The fixture.** `crates/ananke-raft/tests/fixtures/v0.3.0-store/store`: fifteen files
and 5 162 bytes, written by the v0.3.0 tag's code (0d30df5) and by nothing else.

A program, `generate.rs` beside it, does what a server does, in its order:
1. opens the engine as `node.rs` opens it, with a 512-byte memtable and 4 KiB log
   segments so the state is partly in tables and partly only in the log;
2. opens the store, writing incarnation 1, and writes the store marker;
3. persists term 2 and a vote for server 1, with a configuration entry at index 1 and
   six commands at 2 to 7;
4. applies 1 to 4, and takes a snapshot at index 4 into `/raft/snap-4-1`;
5. compacts the log to it, and applies 5;
6. persists term 3, a vote for server 2 and an eighth entry.

It writes under the simulator, which makes the bytes the same on every run and makes the
snapshot record's path `/raft` rather than a directory of the machine that ran it. It
copies the files out through `RealEnv`. It was run in the tag's own tree, with the tag's
`Cargo.lock` and toolchain, from this repository's root:

```
git worktree add --detach <scratch>/v030 v0.3.0
mkdir -p <scratch>/v030/crates/ananke-raft/examples
cp crates/ananke-raft/tests/fixtures/v0.3.0-store/generate.rs \
   <scratch>/v030/crates/ananke-raft/examples/v030_store_fixture.rs
(cd <scratch>/v030 && CARGO_TARGET_DIR=<scratch>/target-v030 \
   cargo run -p ananke-raft --example v030_store_fixture -- <scratch>/fixture)
cp -R <scratch>/fixture crates/ananke-raft/tests/fixtures/v0.3.0-store/store
git worktree remove --force <scratch>/v030
```

- No tracked file of the tag's tree was changed; `--force` removes the untracked example.
- Four runs, the last of the program as committed, were identical under `diff -r`.
- The README beside the store lists what it holds under 0.3.0's keys, with every file's
  size and SHA-256.
- `crates/ananke-raft/tests/v030_store.rs` holds the fixture to that README at the
  engine, under 0.3.0's keys spelled out byte by byte rather than through the build's
  helpers:
  - an engine whose recovery lost nothing, three tables and a log;
  - the hard state, the applied index, the incarnation, the configuration key and the
    snapshot record;
  - entries 5 to 8 and the user's `a`: ten keys in all.

**The test.** It landed with the layout, in D-060's commit, after `SimEnv`'s queue, as
the owner ordered on 2026-09-15. `crates/ananke-raft/tests/v030_store.rs` asserts that:
- the fixture is refused naming format 1, format 2 and 0.3.0, and not as lost state;
- nothing on its disk changes, with no file added — not even the empty log segment every
  engine open adds, since the engine never opens;
- a server started on it traces one `RaftServerFailed` naming both formats, and no
  `RaftRefused`, `RaftAdopted`, `RaftRecovered` or engine record;
- the fixture with a lost mark, a rotted table, a rotted log record, a completed staged
  install, or nothing but its marker is refused for its format all the same, beside the
  start that checked lost state first, which the same five shapes catch on 5 of 5;
- it is refused on a real filesystem with every name, every size and every byte
  unchanged, and a directory that is not there is fresh and stays missing.

The test's names and D-060's tests of the mechanism are listed in D-060.

**Alternatives.**
- *Migrating 0.3.0's store to the new layout*: the owner's decision rules it out; 0.x has
  no users (Q5).
- *Treating the refusal as lost state*: the server would mark the store lost and re-seed,
  so a leader's snapshot would replace a whole store its operator has not decided about.
- *Refusing only a missing or older version*: a newer one would be read by a build that
  cannot know its layout.

**Consequences.**
- Stores written by 0.3.0 are refused by the release that ships item 6 and must be
  discarded or rebuilt. The break is recorded for the release notes in D-060 (Q5,
  condition 3).
- The first draft of this entry (9e90eed) proposed a mechanism: the version as an engine
  key, read after the engine's open and after lost state. It contradicted the last two
  bullets above, and is superseded by D-060, where it is kept as a rejected alternative
  and as the known-buggy start order `LostStateBeforeFormat`.

---

## PROPOSED D-060 — The key layout of Q5, and the store's format record read before anything writes

**Context.** Stage A's item 6 (SHARD.md §12) has three parts:
- the Raft store parameterised by a key prefix (Q40);
- one group's Raft state under `0 / <range: u64 BE> / <purpose> / name`, with tenant 1
  the system tenant and user data moved from tenant 1 to tenant 2 (Q5, SHARD.md §1);
- the on-disk format version bumped and recorded where a store's open can read it, with
  a store in 0.3.0's format refused (D-059).

The owner's answers of 2026-09-15 add three constraints, recorded as decided in D-059:
- newer formats are refused too;
- the version is read before anything writes;
- the format is checked before lost state.

Before this commit a server's start wrote before it could read a version stored anywhere
in the engine (node.rs:385-513 at 9e90eed):
- the adoption can sweep, copy over or mark a store (snapshot.rs:318-449);
- `Engine::open` creates the directory, sweeps orphans and always starts a new log
  segment (engine.rs:898, 1023-1029; wal.rs:600-614);
- every open error becomes a lost mark and a re-seed (node.rs:461-485).

The simulated disk sets further limits:
- it rots one bit per block at every crash (sim/fs.rs:361-381; `p_bitrot` 0.02 in the
  raft and membership scenarios);
- a crash keeps a prefix of each directory's unsynced entry operations, but never loses a
  synced entry (sim/fs.rs:276-289, 611-634);
- a lost fsync leaves pending writes to a later crash (sim/fs.rs:317-330).

Any engine key can be lost with a dropped table or a damaged log. Under
`RefusalNotDurable` a dropped table can even be laundered into a store with no damage
(D-044, seed 687; seed 158's pin, which asserts the laundered store's restatement as
built — seed 119's pin asserts the absence of any refusal on its run). So a version kept
only in the engine cannot tell
"format 2 that lost its version" from "format 1 that lost state".

**Decision.** Every code site carries `// PROPOSED(D-060)`, and the refusal's sites carry
`// D-059`.

*The record.*
- **What it is.** A file `RAFT-FORMAT` in the store directory, beside `RAFT-STORE`
  (`crates/ananke-raft/src/format.rs`). Two copies of `b"ananke raft store format\n" |
  version: u64 LE | crc32c: u32 LE`, 37 bytes each, at offsets 0 and 37, 74 bytes in one
  block. The bytes are pinned in `tests/format.rs`.
- **How it decodes.**
  - A copy is valid when its magic and CRC match.
  - Valid copies that agree give the version.
  - Valid copies that disagree are refused, naming the version that is not 2.
  - No valid copy means unreadable.
  - One flip can never read as another version (the CRC). One crash's rot can never make
    the record unreadable (two copies in one block).
- **Permanence.**
  - Every later format keeps the name, the magic, the copy's shape and offsets, and the
    rule that checkpoints and staged installs carry a record. Only the version changes.
  - No build rewrites a record in place with another version.
  - So a record that does not decode can only be damage, and a valid copy naming another
    version is always a refusal.

*The gate, read before anything writes.* `format::check_format(env, dir)` opens the record
and, only when it is absent or unreadable, lists the directory. It writes nothing, creates
nothing and traces nothing; three filesystem operations at a healthy start.
- A record naming 2 is *recorded*. The record is *whole* when both copies are valid and the
  length is 74.
- A record naming another version is refused (`FormatRefused { found: Recorded(v) }`),
  with the words "newer than format 2" or "this build reads format 2 only".
- No record, and nothing else in the directory but `RAFT-FORMAT.tmp` (or no directory at
  all), is *fresh*.
- No record beside any of `CURRENT`, `CURRENT.tmp`, `MANIFEST-*`, `*.sst`, `*.wal`,
  `RAFT-STORE`, `install` or `snap-*` is refused as format 1, 0.3.0's (`Unrecorded`).
- No record beside only other names is refused as `Foreign`, naming them. It is not
  started fresh, and not called format 1.
- An unreadable record with nothing else is *fresh*.
- An unreadable record beside anything else is *damaged*: lost state, never a format.

*The writes.*
- **A fresh directory's first write is its record**: tmp, sync, rename, directory sync,
  before the engine or anything else creates an entry there. So every durable directory
  holding any other entry holds the record, at every crash point (invariant I1).
- **A record with one bad copy is healed at the start** by rewriting the whole record in
  place with byte-identical content, then a sync.
  - A torn prefix of identical bytes cannot damage the valid copy, and a lost sync leaves
    the old bytes. So a heal never makes a readable record unreadable.
  - A rename-based heal can, under a lost fsync, and does.
- **An unreadable record is rewritten** (tmp and rename) only by the adoption, right after
  its `CURRENT` switch and before the marker.
- **Every checkpoint carries its own record**, written after `Engine::checkpoint`.
  - A checkpoint is complete only when its `CURRENT` parses and its record reads 2.
  - The sender streams the record first.
  - `Assembler::verify` reads the staged record before any table and refuses a stream in
    another format or with none.
  - Both adoptions read the staged record before their first write: another version or
    none is refused (`Subject::StagedInstall`); an unreadable one is staging damage
    (`Damage::StagingFormatUnreadable`), refused and never swept (D-041).
- The engine never reads, lists as its own or removes the record; nor do the adoption's
  removals, the version sweep or the assembler's. The as-built adoption's copy loop skips
  it.

*The start* (`node::start_store`, shared by `run` and the tests), in this order:
1. the gate: a refusal or a read error stops the server with nothing written;
2. a fresh directory's record;
3. the adoption, with its staged check (a format refusal stops the server; staging damage
   is lost state);
4. the heal;
5. a damaged record not replaced by an adoption is lost state (`Damage::FormatUnreadable`);
6. the marker (D-041, D-044);
7. `Engine::open`, then `RaftStore::open`.

`RaftStore::open` requires a `FormatChecked` token, which only the gate, the record's
write and the start construct. The token carries its directory, compared with a new
`Engine::dir()`, the engine's only change. `RaftStore::open_dir` runs the gate for callers
outside the server.

The gate now runs before the marker read. D-044's sentence that `refuse_lost_store` reads
the marker before `CURRENT` still holds, since the gate reads neither. If D-044 is read as
"the marker is read first at every start", this entry supersedes exactly that reading and
nothing else.

*The layout.*
- **Keys.** A group's Raft state is `0 / <group: u64 BE> / <purpose: u64 BE> / name`
  (`store::KeyPrefix::group`), with RAFT.md §3's table ids as purposes:
  - 0: `hard`, `applied`, `reseeded`, `incarnation`;
  - 1: the log, keys of 32 bytes;
  - 2: `config`;
  - 3: `snapshot`.
- **Today's group** is **group 2** (`node::SINGLE_GROUP`), SHARD.md §2's range 2, which
  holds what today's group replicates.
- **Tenants.** User data is tenant 2 (`apply::USER_TENANT`). Tenant 1 is the system tenant
  (`apply::SYSTEM_TENANT`), which nothing in Stage A writes. A range may span tenants.
- **The version** is a file, inside no range's table, span checkpoint or range delete.
- **Room for #21 and the descriptor.** Purposes 4 and up are unassigned. #21's session
  table (Q11) and Stage C's descriptor each add keys under a new purpose or name and move
  none. The unit test asserts purpose 4's span holds no key of this build's.
- **No aliasing.** No key format 2 writes is a key 0.3.0 wrote. Format 2's tenant-0 keys
  are 28 bytes or more; 0.3.0's are 20 to 24 bytes, or 27 bytes with an 11-byte name, and
  no format-2 name is 3 bytes long. The gate, not the group id, keeps 0.3.0's keys unread;
  this is the defence in depth, enumerated in a unit test.

**The pairs.** The known-buggy orders are `StartOrder` values in node.rs, `#[doc(hidden)]`
and not `core::Variant`s, since no sweep reaches them:

| Order | What it does | Caught by, measured |
|---|---|---|
| `LostStateBeforeFormat` | D-059's first draft and the held check patch's order | the fixture's five lost-state shapes: 5 of 5 |
| `FormatAfterFirstBatch` | the record written after the engine's open and the first batch | the first start's crash sweep: 16 of 40 seeds refused for the format, and I1 broken on the same 16 |
| `HealByRename` | the heal by tmp and rename | the heal's crash test at `p_durable` 0.7: 3 of 40 seeds left the record unreadable, against 0 of 80 for the correct heal |
| `UnreadableIsUnrecorded` | an unreadable record read as none | the damaged record's re-seed: the start stops the server instead |
| `StagedFormatUnchecked` | the adoption without its staged check | a staged install recording format 3: adopted, and the store's tree changes |

The codec's pair is a decoder without the checksum, which reads flips of the version's
bytes as other versions.

**Tests.**
- **`crates/ananke-raft/tests/v030_store.rs`** (D-059's):
  - `the_v0_3_0_store_fixture_holds_0_3_0_state_under_0_3_0_keys`
  - `the_v0_3_0_tags_store_is_refused_before_anything_writes`
  - `a_server_on_the_v0_3_0_store_stops_and_changes_no_byte`
  - `a_v0_3_0_store_that_lost_state_is_refused_for_its_format_never_reseeded`
  - `a_v0_3_0_store_on_a_real_filesystem_is_refused_and_left_untouched`
- **`crates/ananke-raft/tests/format.rs`:**
  - `the_format_record_survives_one_flip_and_never_reads_as_another_version`: 592 flips,
    87 616 flip pairs across the copies, every truncation, and the conflicting record
  - `a_store_this_build_writes_records_format_2_and_opens_again`
  - `a_store_recording_format_1_or_3_is_refused_naming_both_and_left_untouched`
  - `a_directory_without_a_record_is_refused_or_fresh_by_what_it_holds`
  - `a_crash_in_a_fresh_stores_first_open_never_leaves_it_refused_for_its_format`
  - `a_record_with_one_bad_copy_is_healed_in_place_and_a_crash_never_loses_the_other_copy`
  - `an_unreadable_record_beside_a_store_is_lost_state_and_the_adoption_rewrites_it`
  - `the_format_record_survives_the_engine_the_adoptions_and_the_sweeps`
  - `a_checkpoint_carries_its_record_and_a_stream_of_another_format_is_refused_unread`
- **`crates/ananke-raft/tests/node.rs`:**
  `a_server_whose_store_lost_state_asks_to_be_reseeded_and_grants_nothing`, extended: the
  record is `Recorded { whole: true }` before the start and its bytes are unchanged
  through the refusal, with no `RaftServerFailed`; and
  `a_store_whose_log_record_rotted_reseeds_and_keeps_its_record`.
- **Unit tests in `store.rs`:**
  `every_raft_key_lies_under_its_group_prefix_and_purpose`,
  `no_format_2_key_has_a_0_3_0_shape`,
  `user_keys_are_tenant_2_and_tenant_1_is_the_system_tenant`,
  `the_single_group_prefix_is_0_2`; and in `tests/store.rs`
  `two_group_prefixes_share_one_engine_and_nothing_else`,
  `a_log_key_of_another_length_under_the_log_purpose_refuses_the_open`,
  `user_keys_are_tenant_2_and_tenant_1_stays_empty`.
- The heal's pair is asserted *caught*, over 160 seeds, and its rate is printed beside it
  and not asserted. The owner's rule of 2026-09-15 moves a thin catch to the tier whose
  seeds support it, and a fixed seed set inside a crate test has no tier to move to: it
  runs the same seeds at every one (D-061's carve-out). A floor on the rate of such a set
  fails a tree with nothing wrong the next time the schedules are redrawn, which is what
  this entry did to every one of them. Measured: 7 of 160 unreadable under the pair,
  against the in-place heal's 0 of 320. The set is listed in D-061's table.

**What moved.**
- **Why.** The record's operations and inodes, the stream's extra file and eight more bytes
  on every Raft key move the simulated disk's latency, torn-write and bit-rot draws.
- **Moved:** every schedule of `sim/raft.rs`, `sim/membership.rs` and `sim/quorum.rs`, and
  with them every pinned seed of `sim/tests/raft.rs` and the seeded tests of ananke-raft's
  test binaries. Measured on the branch's base for this commit, `8717a71` (Stage A's lanes
  S and N, D-056's queue, D-058 and D-061), and on the tree of this commit: the raft
  scenario's seed-42 trace hashes `64fc3b553a9e80c2` before and `ab289ace35a8415f` after,
  the membership scenario's `bac1e6936795e5c0` and `9c88d575d39acd76`, and the re-seed
  scenario's `84d6b14ed6d684ab`/`eedef54307ed8c23` and `0d9167f75474e730`/`d8d63ed0109360ec` (the whole trace as
  written, header included, as `moirae_trace::trace_hash` takes it; `scratchpad
  stage-a/l/reaudit/hashes-*.log`). Both columns are measured the same way on the two
  trees, so the comparison holds; unlike echo's golden, which `sim/tests/echo.rs` takes
  over the trace *without* its header, these values move with the crate version in the
  header and are not goldens.
  On the lane's own base, 903f37c, before the queue, the raft hash was `810bcc9bb59159e2`.
- **Not moved,** measured on the same two trees: `sim/engine.rs` seed 42, `sim/wal.rs` seed
  42 and echo's golden, `fcbe82ee7a0ba672`, which `sim/tests/echo.rs` asserts and which is
  green. The engine gains one accessor, `Engine::dir()`, which does no I/O and which no
  sweep calls, and `ananke-env` is untouched; the engine, WAL and echo sweeps print the same
  rates at a thousand seeds as `2ec4bf7` did for D-061's table (below).

**The cost in time.** SHARD.md §12's shared rules (docs/SHARD.md:2041-2045) ask each stage
to record its measured premerge beside the last one measured and to size its new scenarios'
seed shares to stay near D-040's quarter of an hour. `scripts/premerge.sh` at a thousand
seeds, on the tree the review's commits leave, measured as D-052 and D-055 measured it —
a warm release build first, the one-minute load sampled every 15 s across the run:
**593.87 s** real (9 min 53.9 s), 68 min 5.0 s user and 2 min 39.1 s sys, at a **mean
one-minute load of 20.87** over 46 samples (10.12 to 49.75; another lane built on the
machine through part of it), against D-055's **540.37 s** at a mean load of 17.64 on
3787528 and D-052's 374.64 s on 1ef6d7e. Per binary: `sim/tests/raft.rs` **347.76 s**
(306.86 s at D-055), `sim/tests/engine.rs` **232.34 s** (213.06 s), the WAL binary 8.48 s,
`sim/tests/echo_cluster.rs` 2.43 s, everything else under 1.2 s each, and ananke-raft's
four test binaries 0.36 s together, since their seed sets are fixed (D-061). So the
layout, the record and D-056's queue together cost about a tenth of the tier's wall time,
almost all of it in the raft binary, which is where they change every store write:
**the tier is still inside D-040's quarter of an hour, and no seed share needs resizing.**
The whole suite at a hundred seeds in release takes 86.4 s.
A premerge attempted earlier on the same code, while two lanes were building and the load
reached 89, timed the engine binary alone at 535.69 s and never finished; read as a
budget it said the tier had blown the quarter of an hour by three quarters, and it had
not — the same binary on the same sweep takes 232.34 s on a moderately loaded machine.
A premerge time means nothing without the load beside it, which is why D-052's protocol
records one. The same caution applies to the two figures above: this run's mean load is
about a fifth higher than D-055's, so part of the 10 % is the machine and not the tree,
and neither number is precise enough to re-scale D-055's nightly projection from. Taken at
face value the engine binary's 232.34 s would move that projection from about 2 h 45 min
to about 2 h 51 min against the job's 300-minute timeout — the same picture, and the same
answer: **issue #57**, which the owner asked for on 2026-09-15.

**The re-audit of every pinned seed.** CLAUDE.md:58-67: a commit that moves a schedule
re-audits every pinned seed it moves, and a pin asserts its mechanism or the absence of its
situation with the reason, never a bare green. Every pinned test of `sim/tests/raft.rs` was
run, its trace read, and its assertions and its prose rewritten to what the seed now does.

| Pinned seed | What it did before | What it does now, asserted |
| --- | --- | --- |
| 164 | no snapshot-fed timer gap, over three refusals | the same absence; the replay finds no gap at all, over four refusals (server 2's for lost state at 6.405597155 s and server 3's three from 8.66689086 s) |
| 385 | no gap for a restatement to rescue, over five refusals | the same absence, and the run now holds **no refusal at all**, over seven crashes and six isolations |
| 7381 | the two floor rules agree; one refusal, server 2's at 12.575 s | the same; one refusal, server 1's at 6.396598792 s for lost state (table 1), and `floor_lowering_installs` empty, which is the agreement itself |
| 6325 (correct and as built) | no crash inside an adoption window; 8 and 9 windows | the same absence; 6 and 6 windows, and the schedule's first crash now lands 691 ms *before* server 1's next adoption under the correct server and 712 ms before it as built |
| 5909, correct | D-042's refusal, reset and re-seed on server 1 | **the mechanism, on server 3**: refused at 9.793751132 s, progress reset at 9.798229515 s, re-seeded at 10.218206571 s |
| 5909, the pair and each half | stale progress under `IgnoreIncarnation` alone; none under the pair | **the mechanism on each**: server 3 stale under `IgnoreIncarnation` (leader 1 of term 9, 229 matched, 105 probes, 106 rejections), server 1 stale under the pair with server 1 alone uncounted after the heal, nothing stale under `SharedSnapshotDir`; no run takes one index twice (32, 33, 18 and 37 takes), so the stream half is out of reach on all three |
| 132 | the pair's liveness catch, with the wedge's mechanism | **the absence, with the reason**: the pair, both halves and the correct server all pass; no run takes one index twice (31, 31, 15 and 24 takes) and the pair's two runs are refusal-free, so the pair's trace is the stream half's record for record. The search over seeds 0..1000 found the pair caught on **0 of 1000** and each half on 0 |
| 680 | the pair passes; `IgnoreIncarnation` alone leaves server 1 stale | the pair now leaves server 3 stale and only it uncounted after the heal; `SharedSnapshotDir` alone and the correct server refuse server 3 at 17.852606007 s and 12.162591955 s, reset and re-seed it; and under the pair and the stream half alone the seed **does** reach the stream half's shape — 73 takes, 37 at an index already taken, 7 of those under a live stream, every one of them a stream the follower still installs at afterwards, which the pin asserts |
| 687 | as built, the first half; under the correct server, the quiesce | as built, the first half again (table 44 dropped, refused at 18.109367393 s, two crashes on the refused server, two restarts, nothing laundered); under the correct server **the absence with its reason** — no table dropped, no engine quiesced, no lost-state refusal, its two refusals being damage found before the engine could lose anything |
| **158 (new)** | — | **the pin `RefusalNotDurable` needed**: the first of the thousand's 9 catches, with the laundered store's restatement as built (table 87, manifest 15 without it, segments deleted, a clean open at applied 423) *and* D-044's own mechanism under the correct server on the same seed (table 94, the engine quiesced at 21.378303251 s, the refusal traced at 21.382950125 s, three crashes on the refused server each refused again on the durable mark, nothing flushed until the install at 22.202624757 s) |
| 119 | `RefusalNotDurable`'s catch | **the absence with its reason**: no store is refused at all on the run, so the variant has nothing to change and its trace is the correct server's record for record |
| 1885, 2023 | no term change straddles an isolation's start | the same absence, re-measured: on 1885 server 1 is not isolated at 15.203 s and holds term 9 across that stretch; on 2023 it is cut off from 19.37 s, not 19.22 s, and keeps term 13 through the window |
| the nightly's eleven variant catches | no straddle; no uncounted step-down on any | no straddle, and the two seeds whose named isolation still comes are the same (5203, 6691); seed 1252 now steps a leader down leaving a follower uncounted, which the test asserts by seed rather than forbidding outright |
| the 28 removed catches | no catch to remove; six kept their isolation; seed 6717 the one catch | no catch to remove; **five** keep their isolation (5203, 6691, 5051, 5879, 2578); seed 5153's own gap is gone and the five the replay finds there are on a run the timer bound is not asked of (§2's carve-out, D-035); **seed 2305** under `SnapshotWithoutCurrentLast` is now caught by state machine safety over its own bug, asserted |
| term-raise seed 1 | D-050's shape, one change received before its isolation | **D-047's straddle**: four rises decided before their isolations and traced inside them, the first server 2's from term 1 at 1.21785 s |
| term-raise seed 4 | D-047's straddle, seven rises | **D-050's shape again**, which it held before D-056: server 3's change from term 4 to 5, received 7.362 µs before the isolation at 2.85382 s and stepped 12.88 µs into it |

Two pins changed their names with what they assert:
`seed_119_…_which_a_hundred_seeds_can_miss` becomes `seed_158_…`, with
`seed_119_which_pinned_the_refusal_that_is_not_durable_before_the_layout_refuses_nothing`
beside it; `seed_132_pins_the_combined_variant_…` becomes
`seed_132_which_pinned_the_combined_variant_before_the_layout_reaches_no_wedge`; and seeds
1 and 4 of the term-raise schedule exchange their test names with their shapes.
`assert_stream_wedge`, the helper that read the wedge off seed 132's trace, is removed with
the wedge: no seed of the first thousand is *caught* on this tree.

*Corrected after this entry landed.* The rows above for seeds 5909, 132 and 680 read "no
run takes a snapshot" from `Report::snapshot_takes`, which pairs a take's record with the
checkpoint its take wrote. This entry's own checkpoint format record put an awaited write
between those two records, and the fold asked them to carry the same instant, so it
answered empty on every seed of every variant and the three pins asserted nothing. The
fold pairs by node and claim now, the rows say what the seeds do, and the wedge's stream
half turns out to be built often: under `SharedSnapshotDir` a re-take lands under a live
stream on 180 of the first thousand seeds and the follower never installs at that index
afterwards on 135 of them, against the correct server's 0 of a thousand. None of them
stalls a commit, so none is caught; the sweep asserts the shape from the hundred-seed
tier.

**Alternatives.**
- *The version as the engine key `0`, read after the engine's open and after lost state*
  (D-059's first draft, 9e90eed, and its held patch).
  - It writes a new segment into a refused store.
  - It re-seeds a 0.3.0 store that lost state.
  - Measured on that change alone: every pinned raft run moved and nine pinned tests failed
    at the gate.
  - It is the pair `LostStateBeforeFormat`.
- *The engine key read by a new read-only recovery* (`Engine::inspect`).
  - It is a second recovery that must agree with the first forever, and it reads every
    table twice per start.
  - A loss that takes the key makes format 2 and format 1 the same bytes, so a damaged
    0.3.0 store without a surviving 0.3.0 key would re-seed.
  - 0.3.0 directories holding only a marker or an empty engine open fresh.
  - One dropped table defeats "refuse newer".
- *The engine key beside the file, checked in `RaftStore::open` after lost state.* Under
  `RefusalNotDurable` the flusher launders a dropped table holding the key into a store with
  no damage, and the key rule then refuses a format-2 store as format 1. Checking a staged
  key would add table reads to both adoptions and change D-041's classification of a rotted
  staged table.
- *A manifest field*: the engine names no layout; a rotted `CURRENT` hides it; it moves the
  engine sweep.
- *The version in `RAFT-STORE`*: written after the first open, rewritten in place at every
  refusal, and 0.3.0 wrote markers.
- *One copy, plain text, or no CRC*: a flip reads 2 as 3, or one rot re-seeds and
  quarantines a voter for good (D-035).
- *Two copies without a heal*: one rot from a re-seed for life.
- *The heal by tmp and rename*: it can lose the surviving copy under a lost fsync.
- *An unreadable record alone counted as lost state*: a lost fsync on a new node's first
  write would re-seed a node holding nothing.
- *A fresh directory as one holding no store-family name*: a store started in a directory
  holding things nobody identified.
- *The record streamed last*: under `SnapshotWithoutCurrentLast` a staged `CURRENT` could
  precede it, changing that variant's catch.
- *A missing staged record as staging damage*: a re-seed would discard, unread, an install
  of unknown format. It is unreachable for this build's stagings, and stopping writes
  nothing.
- *The adoption rewriting a damaged record before its copies*: it would label the old
  store, of unknown format, as format 2.
- *An unreadable record refused before the adoption*: the re-seeded install would never be
  adopted.
- *No proof token*: any other caller of `RaftStore::open` could read an ungated store's
  keys.
- *Group 0 for today's group*: range 0 is §2's root; kept as question 3.
- *The prefix in `NodeConfig`*: churns every configuration for a constant Stage B
  replaces.
- *A trace event for the record's writes*: question 5.
- *Migration*: ruled out (D-059).

**Consequences.**
- *The format break, for the release notes of the release that ships this* (Q5, condition
  3): stores written by ananke-raft 0.3.0 are refused at open, naming format 1 and format
  2, and must be discarded or rebuilt; there is no migration. No release-notes file exists
  in the tree, so this entry is the record until one does.
- A crash in a fresh store's first start can leave it refused as lost exactly where it
  could before — the engine's own rule for a manifest without a `CURRENT` (D-024) — and
  never refused for its format. The record's *own* cost in re-seeds is counted apart from
  the engine's and is **0 of 200 seeds** on a disk that keeps its syncs; at `p_durable`
  0.7, which no Raft scenario models, it is 7 of 200.
- Under a lost fsync, which the Raft scenarios do not model, a new node's record may be
  torn beside its first engine files. It then re-seeds.
- A directory holding foreign entries (`lost+found`, `.DS_Store`) is refused. Operators
  point the store at an empty directory. The refusal names at most eight of them.
- A healthy format-2 store whose `RAFT-FORMAT` file is *removed* — not damaged, removed —
  is refused as 0.3.0's and the server stops for good, where a store whose record is
  damaged beside it is lost state and re-seeds. Nothing in this build removes it (the
  sweeps enter only `snap-*`, `is_store_file` excludes it, the as-built copy loop skips it)
  and a synced directory entry is never lost in the simulator, so it is an operator's
  action or a real filesystem's loss. Question 1 below is where that asymmetry is put.
- Clusters on different formats cannot stream snapshots to each other. The next format bump
  needs a rolling-upgrade decision.
- Stage B's per-range stream must carry and check a record (the permanence rule).
- Stage C's descriptor must either bump the format or refuse a range table without a
  descriptor, since a format-2 store's group 2 would otherwise look like a range 2 replica
  missing only its descriptor.
- Every start reads the record: three filesystem operations when it is whole.
- node.rs's comment that the as-built adoption's disk sees exactly the nightly's operations
  is amended. The gate runs under every variant.
- A variant pair not swept today, `{SharedSnapshotDir, SnapshotWithoutCurrentLast}`, could
  stage a `CURRENT` from a stream whose listing lacked the record. The adoption would then
  stop the server rather than adopt.
- SHARD.md's citations of store.rs and apply.rs describe the tree before this commit, and
  so do its sixteen citations of RAFT.md lines 450 and above
  (`grep -nE "RAFT\.md:(4[5-9][0-9]|[5-9][0-9][0-9])" docs/SHARD.md`): item 1's correction
  and this commit inserted at RAFT.md +455, +466, +541, +702 and +708 and took the file
  from 721 lines to 770, so those citations land a few lines off or on other prose. The
  approved plan is not edited; the integrator or the owner refreshes them.
- Issue notes, not code:
  - the store directory's entry is never fsynced in its parent;
  - `valid_name` accepts `RAFT-*` chunk names;
  - the RealEnv test cannot check modification times or a read-only directory, since
    `std::fs::metadata` and `std::fs::set_permissions` are banned outside `ananke-env`
    (clippy.toml, `scripts/check-direct-io.sh`); it checks every name, size and byte.

**Questions for the owner.** Each of questions 1 to 5 is resolved above the conservative
way and none of them blocks. Question 6 is different: it is the escalation SHARD.md
§12's own rule (docs/SHARD.md:2356-2364) makes when a schedule move leaves a pair with no
catch, so what is conservative about it — leaving the pair asserted-absent on seed 132 —
is stated inside the question rather than settled above it, and whether it blocks Stage A's
exit is the owner's call, not this entry's.
1. **An unreadable record beside a store.** "Refuse newer" and "nothing written to a
   refused store" cannot be checked when the record no longer says its version. D-044 and
   the requirement that a format-2 store whose version record is lost still re-seeds call
   for lost state instead.
   - Taken: lost state, a lost mark and a re-seed. It needs two independent rots.
   - The cost: a newer-format store in that state, after a downgrade, is replaced rather
     than refused.
   - Does "nothing is written to a refused store" cover this store, or only a store
     refused for its format?
2. A directory with entries but no store file and no record is refused as `Foreign` rather
   than started fresh. Confirm.
3. Today's group is group 2 (§2's range 2) rather than group 0. Confirm, with the Stage C
   consequence above.
4. The gate, the staged check, the record's writes and the stream's order apply under
   every variant, `AdoptionAsBuilt` and `RefusalNotDurable` included. Confirm.
5. The record's write, heal and rewrite are untraced, like the marker's. A
   `RaftFormatRecorded` event would add an ananke-env variant and a moirae line. Add one?
6. **The pair `{IgnoreIncarnation, SharedSnapshotDir}` has no catch on this tree.** The
   search SHARD.md:2356-2364 asks for at a schedule move found the pair caught on 0 of the
   first thousand seeds and each half on 0, so seed 132 asserts an absence and no seed
   pins the pair's catch. The plan says that goes to the owner, and this is it. The stream
   half's *shape* is reached often (135 of a thousand, above) and the sweep now asserts it;
   what is missing is a catch. Leave it asserted-absent, search the nightly's ten thousand,
   or aim an arm at the wedge as `RefusedCountsForQuorum` has a directed scenario?

---

## D-061 — A catch or a coverage state seen on under 5 % of seeds is asserted from the thousand-seed tier

**Context.** A sweep's "caught on some seed" and a coverage counter's "seen above zero" are
draws. The seeds are fixed, so an assertion that a thin rate happens to meet on the gate's
twenty passes every gate until a change redraws the schedules, and then fails a tree with
nothing wrong. D-041 and D-044 gave such catches the hundred-seed tier. In its answers of
2026-09-15 the owner moved `RefusalNotDurable`'s catch, 1.32 % at the nightlies, to the
thousand-seed tier (D-056) and `reverts_to_a_prefix`, 2.8 %, likewise (D-058), and made it a
rule, in answer 1: "Add a general rule to CLAUDE.md: a variant caught on under 5% of seeds
asserts its catch at the premerge tier, never at the gate tier — the assertion belongs where
the statistics support it. Audit the other variants against that rule and move any that are
similarly fragile." The tiers are D-040's: 20 seeds at the gate, 100 in CI, 1 000 under
`scripts/premerge.sh`, 10 000 in the nightly.

**Decision — the owner's of 2026-09-15.** The rule is in CLAUDE.md's working agreements
beside the pair rule and the pinned-seed rule, read as the owner applied it to
`RefusalNotDurable`:

- A variant caught on under 5 % of seeds asserts its catch when `seeds() >= 1000`, the
  premerge and the nightly, and never at the gate's 20 or CI's 100.
- Its firing, the fault injected or the shape it aims at reached, is still asserted at every
  tier where that is itself well above 5 %, and the catch rate is printed at every tier.
- The rate is over the seeds the assertion actually sees. A variant that `high_rate_share`
  runs on a share of the tier, 20, 20, 100 and 1 000 seeds, is counted over that share. The
  incremental checker compares `min(seeds, 100)`. A counter of events counts the seeds that
  saw one, not the events.
- A coverage counter asserted above zero, a state the correct server's sweep must reach, is
  the same draw as a catch; it is what the owner moved in `reverts_to_a_prefix`. This entry
  applies the 5 % rule to every such counter as the owner's "move any that are similarly
  fragile".
- A rate at or above 5 % stays at its tier.

Out of the rule's scope: assertions made of every seed ("on every seed", "caught on every
seed", a count equal to zero), pinned seeds, which CLAUDE.md's pinned-seed rule governs, and
the fixed seed sets inside crate tests. Those sets run the same seeds at every tier, so they
have no tier to move to; they are listed at the end with their figures.

**How it was measured.** On 2ec4bf7, the tree with D-056's send queue, which moved every
raft schedule, so no older rate is this tree's. Probe copies of the sweeps' test files
printed each seed's coverage: `sim/tests/zz_audit_{wal,engine,raft,echo}.rs`, each the
committed file plus `eprintln!` lines, deleted from the tree and never committed. They ran
in release at `ANANKE_SEEDS=1000`, and the engine's deep-levels test at
`ANANKE_DEEP_SEEDS=1000`, the nightly's count. The crate tests with fixed seed sets ran
in debug with a count printed and were restored with `git checkout`. Everything is in
`scratchpad stage-a/q/audit`:

- `measure-1000.sh` runs the probes; their output is in `m1000-zz_audit_{wal,engine,raft}.log`,
  `m1000-zz_audit_echo.log`, `m1000-echo-sweep.log` and `deep1000-zz_audit_engine.log`.
- `parse_probe.py` gives the per-seed counts, in `m1000-{wal,engine,raft}-perseed.txt`.
- `crate-fixed-loops.log` holds the fixed seed sets, and `probes/` the probe files.

The ten-thousand-seed figures come from the last two green nightlies. Run 34908018220 ran
on 0557590 and run 34948461479 on 94c6a54 (`nightly-<run>.clean.log`). They print the same
figures, except that 94c6a54 predates D-050's term-raise schedule. Both trees predate
D-056's queue, Stage A's lanes S and N (no install, range delete or seek in the engine
sweep) and D-058, so they are other schedules. Where the nightly prints events rather than
seeds, the seeds that saw one are at most that many, given as "≤". P(none) = (1 − p)^n,
where p is the rate at 1 000 and n is the number of seeds the assertion sees at the lowest
tier it is asserted at; "~0" is below 10^-12.

**The audit.** Seeds seen are given at the gate, CI, the premerge and the nightly. Tiers are
"every" (from the gate's twenty), "≥ 100", "≥ 1 000" and "≥ 10 000". Line numbers are on the
commit that lands this entry, 8717a71; a3beb04 moved `sim/tests/raft.rs` and ananke-raft's
test binaries, and the review commits after it moved them again, so read the names rather
than the numbers.

| Assertion | Where | Seeds seen | At 1 000, 2ec4bf7 | At 10 000, nightlies | Tier before → after | P(none) at its tier |
| --- | --- | --- | --- | --- | --- | --- |
| Echo: pongs received, both journals | sim/tests/echo.rs:97 | 20 / 100 / 1 000 / 10 000 | 1 000 (100 %) | not printed | every → every | ~0 |
| Echo `NoSyncDir`, every fault seen: bit rot, corrupt records, torn writes, lost directory entries, a vanished journal | echo.rs:190 | 20 / 100 / 1 000 / 10 000 | 670, 329, 452, 876, 433 | 6 827, 3 350, 4 593, 8 622, 4 323 | every → every | 3.4 × 10^-4 (corrupt records) |
| Echo correct journal, disk faults seen: bit rot, corrupt records, torn writes, torn files at replay | echo.rs:172 | 20 / 100 / 1 000 / 10 000 | 670, 409, 452, 452 | 6 827, 4 289, 4 593, 4 593 | every → every | 2.7 × 10^-5 |
| WAL variants caught: `NoSyncDir`, `NoChecksum`, `AckBeforeSync` | wal.rs:67 (72, 77, 82) | 20 / 100 / 1 000 / 10 000 | 909, 964, 1 000 | 9 150, 9 744, 10 000 | every → every | ~0 |
| WAL coverage: torn writes, lost fsyncs, bit rot, stops at a torn record, stops at a bad checksum, discarded segments, the lost-fsync excuse, the bit-rot excuse | wal.rs:144 | 20 / 100 / 1 000 / 10 000 | 1 000, 1 000, 1 000, 977, 999, 1 000, 994, 994 | 9 999, 10 000, 10 000 seeds; the rest thousands of epochs | every → every | ~0 |
| WAL: a gap | wal.rs:152 | 20 / 100 / 1 000 / 10 000 | 51 (5.1 %) | 615 epochs, ≤ 6.2 % | ≥ 100 → ≥ 100 | 0.0053 |
| **WAL: the betrayed-cut excuse** | wal.rs:165 | 20 / 100 / 1 000 / 10 000 | 34 (3.4 %) | 401 epochs, ≤ 4.0 % | **every → ≥ 1 000** | 0.50 at 20 → 9.5 × 10^-16 |
| Engine Phase 1 variants caught: `NoWalBeforeMemtable`, `ReleaseBeforeManifest`, `DeleteBeforeManifest` | engine.rs:553 (558, 563, 568) | 20 / 100 / 1 000 / 10 000 | 985, 602, 647 | 9 825, 6 482, 5 893 | every → every | 9.9 × 10^-9 |
| Engine `InstallInTwoSwitches` caught | engine.rs:296 | 20 / 100 / 1 000 / 10 000 | 559 | not on those trees | every → every | 7.7 × 10^-8 |
| Engine `RangeDeleteSkipsMemtables` caught, on the share | engine.rs:359 | 20 / 20 / 100 / 1 000 | 89 of 100 | not on those trees | every → every | 6.7 × 10^-20 |
| Engine `SeekCountsTombstones` caught, on the share | engine.rs:422 | 20 / 20 / 100 / 1 000 | 98 of 100 | not on those trees | every → every | ~0 |
| Engine `InstallKeepsSourceNumbers` caught by the oracle, on the share | engine.rs:489 | 20 / 20 / 100 / 1 000 | 98 of 100 | not on those trees | every → every | ~0 |
| Engine `SpanCheckpointUnsynced` caught, on the share | engine.rs:530 | 20 / 20 / 100 / 1 000 | 81 of 100 | not on those trees | every → every | 3.8 × 10^-15 |
| Engine coverage, 29 counters: live reads, scans, rotations, flushes, crashes mid-flush, recoveries that replayed, excused losses, lost fsyncs, bit rot, torn writes, tables written, segments deleted, orphans removed, tables dropped, manifest fallbacks, missing log heads, batches, unsynced writes, checkpoints opened after a crash, compactions, compactions below level 0, inputs deleted, writes dropped, tombstones dropped, installs, range deletes, span checkpoints, seeks, recovery seeks | engine.rs:774 | 20 / 100 / 1 000 / 10 000 | 1 000, 1 000, 990, 983, 965, 970, 964, 1 000, 988, 997, 988, 983, 956, 831, 818, 704, 999, 1 000, 819, 976, 973, 981, 976, 976, 977, 961, 993, 1 000, 971 | the first 24 above zero (lost fsyncs 9 998, bit rot 9 906, torn writes 9 972 seeds); the last five not on those trees | every → every | 2.7 × 10^-11 (missing log heads) |
| Engine: a crash inside a compaction | engine.rs:780 | 20 / 100 / 1 000 / 10 000 | 742 | 6 202 events | ≥ 100 → ≥ 100 | ~0 |
| Engine: a store refused for a fault | engine.rs:784 | 20 / 100 / 1 000 / 10 000 | 162 (16.2 %) | 1 146 (11.5 %) | ≥ 100 → ≥ 100 | 2.1 × 10^-8 |
| Live install: span checkpoints verified, live reads over an install | engine.rs:232, 233 | 20 / 100 / 1 000 / 10 000 | 977, 998 | not on those trees | every → every | ~0 |
| Live install's windows: aimed, between replacement and switch, after the switch, keys written after | engine.rs:256–265 (from 230) | 20 / 100 / 1 000 / 10 000 | 1 000, 490, 411, 990 | not on those trees | every → every | 2.5 × 10^-5 |
| Range delete's windows, the same four | engine.rs:256–265 (from 330) | 20 / 100 / 1 000 / 10 000 | 1 000, 497, 377, 984 | not on those trees | every → every | 7.8 × 10^-5 |
| Seek: a seek stopped at its limit, a recovery walked | engine.rs:394, 395 | 20 / 100 / 1 000 / 10 000 | 999, 972 | not on those trees | every → every | ~0 |
| Deep levels: a round from level 2 or deeper, level 3 reached | engine.rs:177, 181 | 0 / 0 / 0 / 1 000 deep seeds | 965, 965 of 1 000 deep seeds | 10 132 rounds, deepest 3 | nightly only → nightly only | ~0 |
| Raft variants caught: `SendBeforePersist`, `ApplyBeforeCommit`, `CountOlderTermForCommit`, `TruncateOnEveryAppend`, `ResetTimerOnAnyRpc`, `SnapshotWithoutCurrentLast` | raft.rs:1736 (1926–1957) | 20 / 100 / 1 000 / 10 000 | 1 000, 882, 454, 1 000, 336, 336 | 10 000, 8 902, 4 415, 9 995, 3 465, 3 302 | every → every | 2.8 × 10^-4 |
| `NoPreVote` caught by the pre-vote check | raft.rs:1918 | 20 / 100 / 1 000 / 10 000 | 1 000 | 9 999 | every → every | ~0 |
| D-050's term-raise shape reached | raft.rs:1471 | 20 / 100 / 1 000 / 10 000 | 298 | 2 893 (0557590 only) | every → every | 8.4 × 10^-4 |
| `NoPreVote` caught on the term-raise schedule | raft.rs:1637 | 20 / 100 / 1 000 / 10 000 | 1 000 | 10 000 (0557590 only) | every → every | ~0 |
| `AdoptionAsBuilt`'s firing: the storm drawn, adoptions under it | raft.rs:2004, 2008 | 20 / 100 / 1 000 / 10 000 | 260, 1 000 | 2 529 seeds; 84 582 adoptions | every → every | 2.4 × 10^-3 |
| `AdoptionAsBuilt` caught | raft.rs:2013 | 20 / 100 / 1 000 / 10 000 | 77 (7.7 %) | 646 (6.46 %) | ≥ 100 → ≥ 100 | 3.3 × 10^-4 |
| `RefusalNotDurable`'s firing: a crash on a refused server | raft.rs:2050 | 20 / 100 / 1 000 / 10 000 | 347 | 3 318 | every → every | 2.0 × 10^-4 |
| `RefusalNotDurable` caught | raft.rs:2074 | 20 / 100 / 1 000 / 10 000 | 16 (1.6 %) | 133 (1.33 %) | ≥ 100 → ≥ 1 000, by the owner (D-056) | 9.9 × 10^-8 |
| `IgnoreIncarnation`: a refused follower re-seeded and applying | raft.rs:2358 | 20 / 100 / 1 000 / 10 000 | 659 | 6 353 | ≥ 100 → ≥ 100 | ~0 |
| `SharedSnapshotDir`'s firing: a re-take at an index already taken | raft.rs:2460 | 20 / 100 / 1 000 / 10 000 | 525 | 5 292 | every → every | 3.4 × 10^-7 |
| `SharedSnapshotDir`'s aimed arm reached its stream | raft.rs:2464 | 20 / 100 / 1 000 / 10 000 | 143 (14.3 %) | 1 472 (14.7 %) | every → every; for the owner, below | **0.046** |
| **`SharedSnapshotDir`'s stream half: a re-take under a live stream the follower never installs at afterwards** (added after this entry, by D-060's re-audit of `snapshot_takes`; `a_leader_that_shares_one_snapshot_directory_…`) | the same sweep | 20 / 100 / 1 000 / 10 000 | not measured: the fold was vacuous until the re-audit. On this tree **135 (13.5 %)**, and 10 of the first hundred, against the correct server's 0 of 1 000 | not measured on those trees | **new → ≥ 100** | 5.0 × 10^-7 at 100; 0.055 at 20, which is why the gate's twenty do not carry it |
| `SharedSnapshotDir` caught by the liveness check | raft.rs:2469 | 20 / 100 / 1 000 / 10 000 | 2 (0.2 %) | 4 (0.04 %) | ≥ 10 000 → ≥ 10 000 | 2.0 × 10^-9; 0.018 at the nightlies' rate, below |
| Lease: drift beyond the bound, the guard revoked | raft.rs:2602, 2603 | 20 / 100 / 1 000 / 10 000 | 503, 503 | 5 023, 5 023 | every → every | 8.5 × 10^-7 |
| **`LeaseTrustsTheClock` caught (a stale read)** | raft.rs:2615 | 20 / 100 / 1 000 / 10 000 | 41 (4.1 %) | 472 (4.72 %) | **every → ≥ 1 000** | 0.43 at 20 → 6.6 × 10^-19 |
| Raft coverage, 33 counters: partitions, one-way blocks, crashes, leader crashes, stale-sender faults, figure-8 drivers, burst puts, drift beyond the bound, lease reads, read-index reads, lease revocations, check-quorum step-downs, duplicates, injected drops, elections, a term above one, truncations, snapshots taken, compactions, crash-mid-install faults, crash-mid-adoption faults, re-take-under-a-stream faults, commits, applies, bit rot, puts, gets, deletes, compare-and-sets, completed, abandoned, redirected, uniformly scheduled seeds | raft.rs:3000 | 20 / 100 / 1 000 / 10 000 | 1 000, 432, 1 000, 425, 431, 727, 727, 503, 584, 1 000, 992, 1 000, 1 000, 1 000, 1 000, 1 000, 1 000, 1 000, 1 000, 517, 260, 246, 1 000, 1 000, 999, 1 000 (×7), 500 | all above zero (drift 5 023, a term above one 10 000, uniform 5 000 seeds) | every → every | 3.5 × 10^-3 (re-take faults); uniform scheduling is half the seeds by `Policy::for_seed`, seed 0 among them, not a draw |
| Raft coverage from 100: refusals, torn writes, snapshots installed, streams resumed, re-seeded servers, re-seeds completed, adoptions, progress resets | raft.rs:3006–3040 | 20 / 100 / 1 000 / 10 000 | 853, 495, 1 000, 1 000, 852, 832, 1 000, 844 | all above zero (re-seeds completed on 8 163 seeds) | ≥ 100 → ≥ 100 | ~0 |
| `SingleMajorityInJointConsensus` caught | raft.rs:3122 | 20 / 100 / 1 000 / 10 000 | 296 | 2 720 | every → every | 8.9 × 10^-4 |
| Membership coverage, 9: grows, shrinks, joint and new configurations, learners promoted, partitions, completed, uniform seeds, compactions | raft.rs:3329 | 20 / 100 / 1 000 / 10 000 | 1 000 each; uniform 500 | all above zero where printed (before D-058) | every → every | ~0; uniform not a draw |
| Membership: an install adopted | raft.rs:3337 | 20 / 100 / 1 000 / 10 000 | 1 000 | not printed before D-058 | every → every | ~0 |
| Membership from 100: step-downs outside `C_new`, configuration reverts | raft.rs:3354 | 20 / 100 / 1 000 / 10 000 | 390, 56 (5.6 %) | 4 253 and 7 025 events, before D-058 | ≥ 100 → ≥ 100 | 0.0031 (reverts) |
| **Membership: an election while joint** | raft.rs:3367 | 20 / 100 / 1 000 / 10 000 | 31 (3.1 %) | 463 events, ≤ 4.6 %, before D-058 | **≥ 100 → ≥ 1 000** | 0.043 at 100 → 2.1 × 10^-14 |
| Membership: `reverts_to_a_prefix` | raft.rs:3382 | 20 / 100 / 1 000 / 10 000 | 28 (2.8 %) | D-058's counter, not on those trees | ≥ 100 → ≥ 1 000, by the owner (D-058) | 4.6 × 10^-13 |
| Incremental checker: a compared seed in violation | raft.rs:3471 | 20 / 100 / 100 / 100 | 61 of 100 | 59 of 100 | every → every | 6.6 × 10^-9 |
| Quorum, `RefusedCountsForQuorum` blocked: a chunk lost to the limit | raft.rs:3585 | 20 / 100 / 1 000 / 10 000 | 1 000 | 300 304 events | every → every | ~0 |
| Quorum on the sweep's disk: the install silence deposes `RefusedCountsForQuorum` | raft.rs:3679 | 20 / 100 / 1 000 / 10 000 | 844 | 8 311 | ≥ 100 → ≥ 100 | ~0 |

**Re-measured on the tree with the key layout and the store's format record (D-060).**
The layout moved every raft, membership and re-seed schedule, so every rate in the table
above that comes from those three scenarios is a fresh draw; the whole table was
re-measured at `ANANKE_SEEDS=1000` in release on the commit that lands D-060
(`scratchpad stage-a/l/reaudit/raft-1000-merged.log` and `ewe-1000-merged.log`).
**No assertion crossed the 5 % line, so the rule moves nothing and every tier above
stands.** What changed:

- The engine, WAL and echo rows are unchanged, figure for figure: those scenarios do not
  touch `ananke-raft`, `ananke-storage` gains only a no-I/O accessor, and their seed-42
  traces are byte-identical on both trees.
- Raft variants at every tier: `ApplyBeforeCommit` 890 (882), `CountOlderTermForCommit`
  451 (454), `ResetTimerOnAnyRpc` 344 (336), `SnapshotWithoutCurrentLast` 356 (336);
  `SendBeforePersist`, `TruncateOnEveryAppend` and `NoPreVote` on every seed as before.
- `AdoptionAsBuilt`: caught on **57 of 1 000 (5.7 %)**, against 77 (7.7 %), with its storm
  drawn on the same 260 seeds and 8 707 adoptions under it. It is the nearest thing to the
  line and stays at the hundred-seed tier, where a hundred seeds see none with probability
  0.943^100 = 2.7 × 10^-3. At the nightlies' 6.46 % a thousand seeds catch 64.6 on average
  with a standard deviation of 7.8, so 57 is one below the mean.
- `RefusalNotDurable`: caught on **9 of 1 000 (0.9 %)**, against 16, firing on 340 seeds
  (347). Already at the thousand-seed tier by the owner's decision (D-056); the first catch
  moves from seed 119 to **seed 158**, which the pin follows.
- `LeaseTrustsTheClock`: **37 stale reads on the thousand seeds (3.7 %)**, against 41
  (4.1 %). Already moved to the thousand-seed tier by this entry; the firing is unchanged —
  the drift exceeds the bound on 503 seeds and the guard revokes on all 503. The rate is
  over the tier's seeds, as every row of the table is; 37 of the 503 exceeded seeds would
  be 7.4 %, and is not the number the rule reads.
- `SharedSnapshotDir`: **caught on 0 of 1 000**, against 2. Its catch is asserted only at
  the nightly's ten thousand, so nothing fails; its firing is unchanged — a re-take at an
  index already taken on 525 seeds — and the aimed arm reaches its stream on **153 (15.3 %)**
  against 143. A third firing measure was added after this entry, once `snapshot_takes` was
  repaired: the re-take landing under a live stream the follower never installs at
  afterwards, D-043's scrambled stream, on **135 of 1 000 (13.5 %)** and 10 of the first
  hundred against the correct server's 0, asserted from the hundred-seed tier. This
  sharpens the second open question below: on this tree the liveness catch's rate is at
  most 0.1 %, and the next nightly is what measures it.
- `SingleMajorityInJointConsensus` 236 (296); D-050's term-raise shape reached on 276 (298);
  the incremental checker 59 of 100 (61); `IgnoreIncarnation`'s state, a refused follower
  re-seeded and applying, on 626 (659); the quorum scenario's silent step-downs on the
  sweep's disk 886 (844).
- Membership, in the unit the 5 % rule reads, which is seeds: `elections_while_joint` on
  **34 seeds, 3.4 %** (31 seeds, 3.1 %, and the same 34 events), `reverts_to_a_prefix` on
  **25** (28), `config_reverts` on **58 seeds, 5.8 %** (56, 5.6 %) over 62 events (57),
  `step_downs_outside_new` on 372 (390); installs adopted 7 468, and a joining server fed a
  snapshot on every one of the thousand seeds. `config_reverts` is the row nearest the
  line and stays above it, where a hundred seeds see none with probability
  0.942^100 = 0.0025.
- The raft coverage counters asserted from a hundred seeds are all far above their line:
  refusals 3 444, torn writes 680, snapshots installed 19 496, streams resumed 68 261,
  re-seeded servers 4 070, re-seeds completed 803, adoptions 9 049, progress resets 2 350.

These are not draws, and are left as they are:

- `IgnoreIncarnation` tracing no progress reset (raft.rs:2352) and the correct log losing no
  directory entry (wal.rs:173) are assertions of zero on every seed.
- `RefusedCountsForQuorum` and `RefusedNeverCounts` caught on every seed are assertions of
  every seed.

The fixed seed sets in crate tests run the same seeds at every tier. The two ananke-raft
rows were the pre-layout tree's when this entry landed and are re-measured here, on the
tree with the key layout; the store's row is the thinnest margin in the table, three seeds
above its floor, and the three `tests/format.rs` rows were added by the review that
re-measured them. The five ananke-raft figures are printed by their own tests at every
tier (`store.rs:296`, `snapshot.rs:997`, `format.rs:787`, `:836` and `:953`), so a redraw
that moves one is visible in any test run; the five older rows keep their counts inside
assertion messages, which print only on failure, and their figures are the ones measured
here:

| Assertion | Where | Seeds | Measured | P(none) over its seeds |
| --- | --- | --- | --- | --- |
| A write torn at a crash; more than three durable lengths | crates/ananke-env/src/sim/tests.rs:856, 857 | 64 | torn on 28; 13 lengths | 1.0 × 10^-16 |
| More than one interleaving | tests.rs:191 | 20 | 11 distinct | not a rate |
| A rename lost and a rename kept | tests.rs:1639 | 64 | lost on 32, kept on 32 | 1.1 × 10^-19 |
| An unsynced create vanished | tests.rs:1675 | 32 | 12 | 2.9 × 10^-7 |
| Two records under one sync | crates/ananke-storage/tests/wal.rs:184 | 20 | 15 | 9.1 × 10^-13 |
| At least 20 of 40 stores came back as a state | `an_entrys_writes_and_the_applied_index_are_durable_together`, crates/ananke-raft/tests/store.rs | 40 | **23** (31 before the key layout) | **0.13** of under 20 |
| A crash inside the adoption before the switch; one after it | `a_crash_inside_the_adoption_leaves_a_store_and_the_next_start_adopts`, crates/ananke-raft/tests/snapshot.rs | 24 | **13 before; 11 after** (10 and 14 before the layout) | 7.4 × 10^-9; 4.1 × 10^-7 |
| Every crash window of a fresh store's first start, W0 to W3 | `a_crash_in_a_fresh_stores_first_open_never_leaves_it_refused_for_its_format`, crates/ananke-raft/tests/format.rs | 200 | 29, **10**, 47, 114 at `p_durable` 1.0; 31, 9, 49, 111 at 0.7 | 3.5 × 10^-5 of an empty W1 |
| The record written after the first batch is caught | the same test | 200 | 89 | ~0 |
| The heal by rename loses the surviving copy (the targeted arm asserts it) | `a_record_with_one_bad_copy_is_healed_in_place_and_a_crash_never_loses_the_other_copy`, crates/ananke-raft/tests/format.rs | 160 | 7 targeted, 7 spread; the in-place heal 0 of 320 | 7.8 × 10^-4 |

**What moved.** The owner's rule moves three assertions to `seeds() >= 1000`. Each is printed
at every tier as before and commented with its rate, its tier and this entry.

- *`LeaseTrustsTheClock`'s catch* moves from every tier (sim/tests/raft.rs). Its firing, drift
  beyond the bound and the guard's revoke, each on half the seeds, stays asserted at every
  tier. RAFT.md §5 now names the tier.
- *The WAL's betrayed-cut excuse* moves from every tier (sim/tests/wal.rs).
- *The membership scenario's election while joint* moves from the hundred-seed tier
  (sim/tests/raft.rs).

`RefusalNotDurable`'s catch and `reverts_to_a_prefix` were moved by the owner in D-056 and
D-058. Every other assertion is at or above 5 % and stays where it is. Two of them are near
the line. A gap in the WAL is on 5.1 % of the thousand; the membership scenario's
configuration reverts are on 5.6 %. At a thousand seeds either could be a little below 5 %,
since a thousand seeds only estimate the rate. The rule as written leaves both at the
hundred-seed tier, where a hundred see none with probability 0.005 and 0.003. Were a later
measurement to put either under 5 %, the rule moves it.

**For the owner — not decided.** The rule as written does not move these; they are the
owner's to decide.

- *`SharedSnapshotDir`'s aimed arm reaching its stream*
  (`a_leader_that_shares_one_snapshot_directory_…` in sim/tests/raft.rs). It reaches its
  stream on **15.3 %** of the thousand on this tree (14.3 % when this entry landed, 14.7 %
  at the nightlies), above 5 %. It is
  asserted at every tier, and the gate's twenty see none with probability 0.857^20 = 0.046,
  above one in a hundred. It is a firing assertion: the shape the arm exists to build. The
  arm rides one seed in four (`RETAKE_STREAM_IN`, D-043), and the other firing assertion
  beside it, the re-take at an index already taken, is on 52.5 %. The conservative options
  are to leave it, or to assert it from the hundred-seed tier, where a hundred see none
  with probability 2 × 10^-7.
- *`SharedSnapshotDir`'s liveness catch at the nightly's ten thousand*
  (the same test). It is under 5 % and already above the thousand-seed tier, so the rule
  leaves it. On the tree with the queue it was on 2 of the thousand, 0.2 %; **on the tree
  with the key layout it is on 0 of the thousand**, so its rate here is at most 0.1 % and
  ten thousand seeds see none with probability at least 0.37. At the nightlies' measured
  rate, 4 of 10 000, ten thousand see none with probability 0.9996^10000 = 0.018. The next
  nightly is what measures it. D-060's question 6 puts the harder half to the owner: the
  pair and each half are caught on 0 of the first thousand, so no seed pins the catch and
  seed 132 asserts its absence, which SHARD.md:2362-2363 routes to the owner.

**Alternatives.** *D-044's shape, a thin catch asserted from the hundred-seed tier*: at 3 %
a hundred seeds see none about one time in twenty, and the owner moved both measured cases
off it. *A tier chosen by P(none) at the tier instead of by rate*: it would also move the aimed
arm's assertion above, but the owner's rule is by rate, and a rate is what every sweep
prints. *More seeds at the gate*: the gate's twenty keep it under a few minutes, which is
its purpose.

**Consequences.** The gate and CI no longer assert the three moved states. The premerge
and the nightly assert them, and every tier prints them, so a sweep that stops reaching one
is seen in the rates before it is asserted. No schedule and no pinned hash moves: the
changes are the tier of three assertions and their comments. Rates are functions of the whole
schedule, so a later change that moves the schedules can carry a rate across 5 % either way;
the rates printed at the premerge and the nightly are where that shows. SHARD.md §12's Stage B
plan (docs/SHARD.md:2344) still says `LeaseTrustsTheClock`'s stale read is caught at every
tier, as the approved plan's text; this entry supersedes it for that assertion.

---

_Next entry: D-062. Add one before implementing anything not covered above._

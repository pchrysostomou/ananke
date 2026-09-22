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

**Corrected by PROPOSED D-062.** The rule above is written without direction: any record
that does not continue the numbering stops recovery, "treated like any other stop". The
nightly's ten thousand found at seed 3123 that a record numbered *behind* the reading,
at a segment's first byte, is not a hole at all — it is the live segment arriving after
a segment a betrayed cut resurrected, and stopping there destroys acknowledged records
and replays stale ones. D-062 makes a backwards jump at a segment's first record a
supersede; a forward jump is still a stop.

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

**Extended by PROPOSED D-062.** "A jump in the numbering that lands at or below the head
… is not a stop" gains its backward twin. A jump *backwards* at a segment's first record
is not a stop either, and needs no head to justify it: the order segments are created in
is what proves the earlier copies stale. Monotone segment numbers, decided here so the
oracle could tell two files apart, are what make that argument available to the reader.

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

**Extended by D-068.** The checkpoint and the install above take one span. D-068 extends
both to a set of spans, sorted and disjoint, because a range lives in two (D-066): the
checkpoint copies every span at one version, and the install takes every span out and puts
the source in with the one switch described here, carrying the receiver's repair in it as a
table of its own at the record after the install's. `checkpoint_span` and `install_span`
are its one-span forms and behave as above. The crash test above now installs over two or
three spans with a repair, and the figures measured since are D-068's.

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

**What the mutation pass found, and what now holds it.** Nine mutations of the engine
oracle, the two new primitives' variant gates and the pinned-seed machinery
(`sim/engine.rs`, `crates/ananke-storage/src/engine.rs`, `sim/tests/raft.rs`), each
reproduced independently on the tree at 09bed88. The primitives' gates held: taking the
seek's tombstone count out of its variant gate and taking the range delete's memtable
skip out of its own are each caught three ways, by a storage unit test and by two or three
sim tests at twenty seeds, seed 42's pinned trace among them. So did D-060's re-audited
`snapshot_takes` fold — pairing takes by instant again is caught four ways at twenty seeds,
including the sweep's `takes_paired` assertion — and a from/to confusion in
`uncounted_after_heal`, which two pins' positive halves catch. Four survived.

- *The installed-table number check made unreachable* (`sim/engine.rs`): Q2's criterion,
  that an install's tables carry the install's own number, stops being checked as such.
  The variant is still caught, so the test's non-emptiness assertion passes — but on
  **92 of the thousand-seed tier's hundred-seed share instead of 98**, and seed 0's first
  catch degrades from *"its table 15 carries records 174..=179, not the install's number"*
  to a bare *"key k21 holds None but the model has Some(…)"*. The margin and the reason
  were both unasserted. Held now: `an_install_that_keeps_its_sources_numbers_is_caught`
  asserts that some catch is the check's own, by its words. It reads runs the sweep
  already makes and moves nothing; it does pin a message, which a schedule move must
  re-audit with the rest.
- *The bounded fallback excuse made unbounded* (`sim/engine.rs`): an abandoned manifest
  excuses every install record past the manifest in force rather than only what it
  covered. Survived at 20, 100 and 1 000 seeds. It is equivalent with respect to every
  verdict and coverage field the suite computes, but **not** with respect to the `excused`
  map — the loose rule exceeds the bound on 465 of 1 074 fallbacks over the engine binary
  at a hundred seeds (53 of the 109 on the install schedule alone) — and the excess is
  simply never consulted, because `check_epoch` tests `present(seq)` before it consults
  `excused` and the only other consumer keys on a seq that is absent by definition. So the
  bound is real code doing nothing today. A note, not a test: what would close it is a
  known-buggy `Variant` deleting an install's log segments before the manifest switch, run
  as a negative control, which is a new variant rather than an assertion and is left as an
  issue.
- *The deleted-table clause reverted to the pre-review rule* (`sim/engine.rs`): **an
  equivalent mutant on every executed schedule.** Over 100 seeds and all seventeen tests,
  2 187 dropped-table judgements (1 458 unreadable, 558 corrupt, 171 missing) and
  `mirror.deleted` — which is not empty, holding up to 176 numbers — contained the number
  in none of them, because `DeleteBeforeManifest` deletes before that set is filled, so its
  deletions arrive as plain `missing` and the generic arm catches them.
  `DeleteBeforeManifest` is caught on 647 of 1 000 seeds with the clause and without it,
  identically. The rate the review recorded for that fix is carried by its other half, the
  narrowed `sst_betrayed` set; this half is a dead clause. Recorded as what it is: only
  making the state reachable — a table deleted early whose file also took bit rot earlier —
  closes it, and finding that shape needs a search, not an assertion.
- *A pinned seed's absence assertion made vacuous* (`sim/tests/raft.rs`): narrowing seed
  132's refusal matcher to a server number that cannot exist — ids run from 1 — leaves
  `assert!(refusals.is_empty())` unable to fail, and the whole workspace stayed green with
  that slip and its twin on seed 119 applied together. The matcher really was vacuous: on
  a report carrying six refusals it matched none. Held now by D-060's own rule applied
  once more — the matcher is one shared `refusals` helper, and seed 132 asserts it finds
  refusals under `IgnoreIncarnation` and under the correct server (6 and 2 on this tree,
  server 2's log stopping at a bad checksum) before it asserts none under the pair and the
  stream half. The same fix does **not** transfer to seed 119, which refuses nothing under
  `Correct`, `RefusalNotDurable`, `IgnoreIncarnation` or `SharedSnapshotDir`: there is no
  report in that test whose refusals can be non-empty, so no companion is possible there.
  What stands in its place is the shared matcher — a matcher narrowed until it finds
  nothing fails on seed 132 before it can make seed 119's absence vacuous — and seed 119's
  test says so.

D-056's drop-newest policy was mutated here as well and confirms the same reading from the
other side: the raft sweep never fills a send queue, so flipping the policy leaves 42/42
raft tests green at 20 and at 100 seeds and moves no pinned trace, and only the env unit
test catches it. That gap, and the one that matters more beside it, are closed in D-056.

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

**Extended by D-068.** The range delete above takes one span. `Engine::delete_ranges` is the
install of nothing over a set of spans, in one switch (D-068), and `delete_range` is its
one-span form, unchanged. The range delete's crash test and the default schedule now delete
two or three spans at once, and the rates measured since are D-068's.

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

**What the mutation pass found, and what now holds it.** Twenty mutations of
`crates/ananke-env/src/sim/net.rs`, each reproduced independently, over the tree at
09bed88. Ten were caught — the drop-newest policy, the off-by-one on the bound, the
silent drop, the wrong `DropReason`, the delay started at the enqueue rather than the
write, the rate charged per message, the two frames written at one instant, the LIFO
re-timing, the rate ignored entirely, and losing a closed socket's queued frames, which
is caught by ten unit tests and fifteen raft tests and un-catches three negative
controls. Five survived every tier, and four of the five are now held by directed tests
in `crates/ananke-env/src/sim/tests.rs`, each of which the mutation makes fail; none
touches a scenario, so no schedule and no pinned hash moves.

- *The eviction loop deleted*, so the bound counts frames **ever sent** rather than
  frames outstanding. The single most consequential survivor: the whole workspace stayed
  green at twenty seeds and the raft binary 42/42 at a hundred (1 007 s), while the
  correct server's `queue_drops` went from 0 to **3 691** at twenty seeds and **42 393**
  at a hundred, and the trace carried `MessageDropped { QueueFull }` for ids it had
  already delivered. The counter was printed and not asserted. Held two ways now:
  `a_written_frame_leaves_the_queue_so_the_bound_counts_what_is_outstanding` (two bursts
  500 ms apart on a queue of two: with the eviction gone the second burst's first frame
  is lost and the two behind it arrive 200 ms early, and the same drops are traced for
  delivered ids), and `queue_drops` asserted 0 in the correct server's coverage —
  measured 0 at 20, 100 and 1 000 seeds on this tree as on the trees above, and
  structurally out of reach, since a drop needs 1 025 frames outstanding to one
  destination inside the 8 µs a kilobyte frame takes to write at a gigabit.
- *The eviction's boundary*, `written <= now` weakened to `written < now`: a frame whose
  last byte is written at exactly this instant keeps its slot against the bound. Survived
  the workspace at twenty seeds and the raft binary at a hundred. Held by
  `a_frame_written_at_this_instant_has_already_left_the_queue`. Exact-instant
  coincidences are common here: replaying the seed-42 trace the suite writes, **1 717 of
  13 106 sends (13 %)** land at the same instant as the previous send on the same link.
- *`write_time` rounding down* instead of up, against this entry's "rounded up to the
  nanosecond, so that every byte takes time". It survives at **every** tier by
  arithmetic, not by sampling: the tree configures exactly two rates, the 125 000 000
  default and `slow_link`'s 1 000, and `div_ceil` never rounds at either, so the mutant is
  bit-identical on every seed in debug and release. It is not cosmetic — at 2 000 000 000
  B/s a one-byte frame's write time becomes 0 ns, every frame is written at the instant
  it is sent, the eviction clears the queue on every admit and the bound can never be
  reached. Held by `a_frame_takes_time_on_a_link_faster_than_a_byte_a_nanosecond`.
- *The `QueueFull` drop traced before the `MessageSent` that caused it*, against the
  clause above ("after the new frame's `MessageSent`, as `RealEnv` emits them"), built
  minimally so every other trace record keeps its place. Survived every tier: the same
  frame is dropped at the same instant for the same reason, and the existing test compared
  only the filtered list of dropped ids. Held now by the ordered `(kind, id)` sequence
  asserted inside `a_full_queue_drops_its_oldest_waiting_frame_and_the_frames_behind_it_move_up`.
- *`forget_queues` made a no-op*, so a closed socket keeps its queues. **An equivalent
  mutant, not a test gap**, and provably so: `next_socket` is written at two lines and
  only ever incremented, so a socket id is never reused; `Fabric::queues` is read at
  exactly two sites and never iterated, so no ordering can leak; and the moirae export
  does not mention it. A queue left under a dead id is unreachable for the life of the
  run. It is recorded here as a note rather than closed by a test that would assert the
  size of a private map; it stops being equivalent the day an address is rebound by a
  restarting node, which is when a stale queue would start to bite.
- *Dropping `.max(enqueued)` from the re-timing loop* is equivalent too, and dead
  defensive code: after the eviction every frame left has `written > now`, and every
  frame's `enqueued` is the `now` of its own monotone admit.

One more finding is about where the model is held rather than whether it is. *One queue
per socket, shared by all its destinations* — the natural "one queue per connection" slip
— was caught only by **eight pinned-seed tests**, at twenty seeds and at a hundred alike,
whose failures read "re-audit the pin". Those are change detectors: an intended change to
the model fails them the same way, and the documented answer is to re-audit and re-pin,
so the realistic failure mode was to re-pin eight seeds and ship a queue bounded per
socket with a green gate and a green CI. No property, sweep, coverage assertion or golden
hash noticed, membership and quorum included. The rule is now asserted as a rule, by
`a_frame_waits_only_behind_the_frames_to_its_own_destination`: three 100-byte frames to one
destination and one to another from a single socket, the fourth arriving at 101 ms rather
than the 401 ms a shared queue gives it.

The margin is also smaller than *Consequences* below suggests. On seed 42 the deepest
same-instant burst on one link is **348 frames** (then 267, 175, 157, 143, 127, 81, 64)
against the 1 024 bound — a factor of three, not a large one, and with one socket per raft
node those bursts are exactly one (socket, destination) queue. Worth remembering when
Stage B's batch frames put many ranges on one socket.

Not mutated by anyone, and still without tests: `MAX_FRAME_LEN`'s guard, the moirae
header's `sendQueueLen`/`linkBytesPerSec` export, `NetFaults`' validation bounds
(`send_queue_len` at least 1, `link_bytes_per_sec` positive — no `should_panic` anywhere),
and `RealEnv`'s own queue.

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

**What the mutation pass found.** Seven mutations of the derivation and its cache
(`crates/ananke-env/src/sim/state.rs`), each reproduced independently. Six were caught,
and the striking thing is by how little: the range dropped from the name and the node
dropped from the name are each caught by two tests; seeding the range's stream from the
node's own protocol stream by all three; a fresh `RandomState` salt per call by all
three; but **one process-wide `OnceLock<RandomState>` salt** — stable inside a run and
different across runs, the insidious form — is caught by the moirae-name assertion alone
(`assert_eq!(draws, moirae_sched::stream(7, "n1/r3/protocol"))`), since every same-seed
and cross-seed equality in that test still holds; a cache keyed by node rather than by
(node, range) only by `taking_a_range_stream_perturbs_no_other_stream`, because the other
test builds a fresh `Sim` per call and so never puts two ranges on one node; and no
caching at all only by `every_env_handle_continues_a_nodes_range_stream`. Three of the six
have exactly one detector each, which is what to know before any of them is changed.

The seventh, turning `panic!("unknown node")` into a silent fallthrough that hands out an
unregistered, restarting stream, survived — and is **an unreachable guard, not a test
gap**. `Sim::env` already panics `unknown node {node}` before it hands out a `SimEnv`;
that line is the only `SimEnv` construction in the workspace, its `node` field is private
and never reassigned, and `nodes` is never removed from, reset or reassigned anywhere in
the crate. So `Shared::range_stream` can only be called with a node `Sim::env` has already
validated, and the `unwrap_or_else(|| panic!(…))` is a redundant second guard on a
crate-internal path. Reaching it at all needs a test that calls
`sim.shared.lock().range_stream(…)` directly, which would assert something nothing can
trip; it is recorded here as the note it is, and no test was added.

Both findings turn on the last bullet above: **nothing calls `range_rng` yet**, so no
seed tier, sweep or scenario can catch any mutation of it, in this stage or the nightly.
The four unit tests named below are its only guard, and stay its only guard until Stage B
seeds each core from it.

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

**What the mutation pass found, and what now holds it.** Twelve mutations of
`sim/membership.rs`, its two assertions in `sim/tests/raft.rs` and the install repair they
lean on, each reproduced independently on the tree at 09bed88. The scenario's mechanisms
held: dropping the variant's joint-majority guard is caught at twenty seeds by the correct
server's own sweep, making the variant never fire is caught at twenty by the negative
control, and turning `await_compacted_leader` into a single look with no wait is caught at
twenty on seed 4 — deterministically, since `sweep` runs seeds 0..count and seed 4 is
inside every tier, though the margin is thin: at a hundred seeds only 3 miss without the
wait. What did not hold is the fold the assertions read and the clause beside it.

- *The learner phase dropped from the fold*, so an install by a server already a voter of
  the joint configuration counts as a learner-phase feed. Survived at 20 and 100 seeds and
  the whole workspace with it; `snapshot_fed_joiners` went **80 → 93** at twenty seeds and
  **523 → 584** at a hundred, and nothing failed.
- *Every server counted as a joiner*, servers 1 to 3 included, where only servers above
  `INITIAL_VOTERS` can join. Survived the same way: **80 → 92** and **523 → 581**, so
  twelve of the ninety-two counted feeds at the gate's tier are installs by original
  voters — an ordinary follower catching up behind a compacted leader, which the
  pre-D-058 scenario already produced.

Both are invisible for one reason: every reader of `snapshot_fed_joiners` asks only
whether the list is empty (the sweep's per-seed check and the coverage's
`seeds_with_a_snapshot_fed_joiner`), and both readers are monotone in the fold, so *any*
widening of either bound passes at every tier. The predicate's two bounds — who, and when
— were each unguarded. Now: the fold is `snapshot_fed_joiners_of(records, grow)`, a
function of a trace and the grow's window with its own unit test over records written by
hand, which asserts both bounds at once (an install before the grow, one by an original
voter inside the window, one by a joiner in its learner phase, one by the same joiner
after the joint configuration admits it, a take rather than a feed, a restatement, and one
after the driver stopped: only the two learner-phase feeds are returned); and the correct
server's sweep now fails any seed where a counted server is not one of the joiners. Both
read records a run already produced, so no schedule and no pinned hash moves.

- *The refusal clause's negation flipped*, so a store refused for anything **other** than
  lost state passes the run silently. Survived the workspace at twenty seeds and the
  membership tests at 20 and 100. The clause has never discriminated on any tier: the
  coverage's `refusals` is empty at 20, 100 and 1 000 seeds on the tree of this commit
  too, and the coverage's companion assertion (every refusal clause starts with
  `LOST_STATE`) quantifies over that empty map. It is now given a case that reaches it —
  a unit test on `Report::check` over seed 0's own passing run with one `RaftRefused`
  record appended, asserted to pass for a lost-state reason and to fail, with the words,
  for a configuration key out of step with the log. **It remains a guard no run of this
  scenario has ever tripped**, and that is the honest statement of it: the unit test makes
  the clause discriminate, the scenario still does not reach the state, and only the shapes
  issue #56 holds would change that.
- *Both snapshot-fed-joiner assertions deleted*, and the same with the compacted-leader
  wait broken as well: both survived, the whole workspace green. Nothing else in the
  workspace — not the invariants, not commit majority, not linearizability, not the
  liveness or availability bounds, not the coverage's other counters — notices that no
  joining server was ever fed a snapshot. **The extension's value rests on those two
  lines.** A deleted assertion can only be caught by a test of the test, and the shape
  that would pin it — a scenario knob asking for the grow of the leader in force instead
  of a compacted one, asserted to produce a seed with no learner-phase feed — is left as an
  issue rather than taken here: wired through `Schedule::draw` it would move every
  membership schedule on every seed and every pinned hash with them.

Two corrections to what this entry records, both from the same pass.

- *The membership scenario adds no discriminating power over the install repair's
  configuration key.* Making the repair a no-op, so the leader's un-rewritten key rides
  into the installed store, leaves all three membership tests green at twenty seeds with
  `refusals: {}`, while the coverage moves (joint configurations 216 → 211, new
  configurations 459 → 465, learners promoted 50 → 48, reverts 3 → 2, elections while
  joint 1 → 0, feeds 80 → 82), so servers really did come up differently. It is caught by
  `ananke-raft`'s own `tests/snapshot.rs` alone. The "Met in Stage A" bullet above is right
  that the key is written on that path and checked at each adoption's open; it should not
  be read as saying the scenario would notice a wrong key.
- *The install repair's kept-tail branch has no unit test anywhere in the workspace.* The
  deferral bullet above says of the truncation revert floor and the kept-tail key repair
  that "Both keep their unit tests alone until #56 builds the shapes that reach them".
  That is true of the revert floor and **not** of the kept-tail branch: every `Repair`
  built in `crates/ananke-raft/tests/{snapshot,format}.rs` passes `tail: Vec::new()` except
  one, which passes two plain command entries, so the fold over `repair.tail` returns
  `None` on every test in the workspace and removing its non-trivial branch outright
  changes nothing — the whole `ananke-raft` suite and the membership tests stay green. The
  branch is production-reachable: the live install builds its tail from the receiver's own
  log above the snapshot's last index, and those entries can be `Payload::Config`. Its
  failure mode is now measured rather than predicted — with the branch removed, a store
  whose kept tail carries a configuration entry above the snapshot is **refused at open**,
  "the configuration key is out of step with the log", and the server drops into re-seed.
  The test that would close it is one near-copy of
  `an_install_carries_the_receivers_identity_and_is_adopted_at_open` with a `Payload::Config`
  in the tail; it needs no seeds and moves nothing. It belongs to issue #56, which should
  carry this correction: the branch is untested, not merely unreached.

The coverage at 1 000 seeds on the tree of this commit, for the record: 4 855 learner-phase
feeds on 1 000 seeds with every seed showing one, 7 468 adoptions, `refusals: {}`,
`reverts_to_a_prefix` 25, and `truncation_reverts_to_a_prefix`, `installs_keeping_a_tail`
and `installs_whose_tail_carries_a_configuration` all 0.

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

**At ten thousand, on the stage's two green nightlies.** Runs 35161762372 (605e62e) and
35172923002 (the tip, 27f6c97) print this scenario's whole `MembershipCoverage`, identical
in both, and the correct server passes every one of the ten thousand seeds: no seed
exhausts the wait, none refuses a store (`refusals: {}`), and a joining server is fed a
snapshot on all 10 000. Four things this entry left open are in that line.

- **The truncation revert floor is reached.** `truncation_reverts_to_a_prefix: 3` over the
  ten thousand — the floor deferred to issue #56 as one the scenario does not reach, in
  fact reached, on at most 3 seeds, 0.03 %. **It is a printed counter, asserted nowhere,
  and too thin for any tier D-061 allows:** at 0.03 % a thousand seeds see none with
  probability 0.9997^1000 = **0.74**, a hundred with 0.97 and the gate's twenty with 0.994,
  and even the nightly's own ten thousand see none about one run in twenty
  (0.9997^10000 = 0.050). Three seeds in ten thousand say the shape exists on this
  schedule; they are not a rate an assertion can stand on, at the nightly tier or any
  other. What it changes is the deferral's wording — the floor is reached rarely rather
  than unreachable — and issue #56 should carry that. Whether the shapes #56 holds are
  still wanted, and whether these seeds are worth finding and pinning, is the owner's.
- **The kept-tail branch is still not reached.** `installs_keeping_a_tail: 0` and
  `installs_whose_tail_carries_a_configuration: 0` over the same ten thousand, so the
  correction above stands at the highest tier there is: the branch is untested rather than
  merely unreached, and no seed of ten thousand builds a kept tail to test it with.
- **`reverts_to_a_prefix`**: 264 over the ten thousand, at most 2.64 % of seeds, beside the
  2.8 % and 2.5 % measured at a thousand. It sits where the owner put it, the thousand-seed
  tier, where a thousand seeds see none with probability 0.9736^1000 = 2.4 × 10^-12.
- **The worst completion gap, re-measured on the moved schedule.** This entry says the
  549.359683 ms that SPEC §3, RAFT.md §1 and the scenario's module comment cite was measured
  at ten thousand seeds before this change, and that the next ten-thousand-seed nightly
  re-measures it. It has: **`worst_completion_gap: 555.458839ms`**, with
  `slowest_write_after_heal: 633.927431ms`, against the 2 s bound SPEC §3 states. The moved
  schedule's worst gap is 6.1 ms worse than the pre-D-058 figure and still under a third of
  the bound. Those three places are not edited here — this commit touches DECISIONS.md
  alone — and they are marked as measured before D-058; the figure to carry into them is
  this one.

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
   **What the stage's green nightlies add to the question** (runs 35161762372 and
   35172923002, the same figures in both): at ten thousand seeds **each half is caught**,
   where at a thousand both were 0. `IgnoreIncarnation` is caught on **1 of 10 000**, seed
   5220, by linearizability — "caught on 1 of 10000 seeds, 0 progress resets, a refused
   follower re-seeded and applying again on 6283 seeds" — and `SharedSnapshotDir` on **5 of
   10 000**, first seed 3300, 3 by linearizability and 2 by the liveness check. So of the
   three options above, "search the nightly's ten thousand" is the one with evidence behind
   it: the band where each half catches at all is the band a pair search would have to run
   in, and the thousand-seed search that found nothing was looking below it. That is not an
   answer — the pair itself is run at no tier, so nothing here says the pair catches
   anywhere in the ten thousand, and at 1 in 10 000 and 5 in 10 000 a pair search could as
   easily come back empty and cost a nightly to learn it. The question stays the owner's.

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
- *A new variant* — the owner's, of 2026-09-19, on PR #68 — asserts its catch at whatever
  tier its measured rate supports under this rule, the rate measured before the assertion is
  written, never after. A new variant caught on no seed at any tier needs a directed scenario
  that builds its situation, not a lower bar: Phase 2's standing rule, as `RefusedCountsForQuorum`
  got its directed scenario (D-049).

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

| Assertion | Where | Seeds seen | At 1 000, 2ec4bf7 | At 10 000, the older nightlies | At 10 000, run 35172923002 (27f6c97) | Tier before → after | P(none) at its tier |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Echo: pongs received, both journals | sim/tests/echo.rs:97 | 20 / 100 / 1 000 / 10 000 | 1 000 (100 %) | not printed | not printed | every → every | ~0 |
| Echo `NoSyncDir`, every fault seen: bit rot, corrupt records, torn writes, lost directory entries, a vanished journal | echo.rs:190 | 20 / 100 / 1 000 / 10 000 | 670, 329, 452, 876, 433 | 6 827, 3 350, 4 593, 8 622, 4 323 | 6 827, 3 350, 4 593, 8 622, 4 323 — the same five | every → every | 3.4 × 10^-4 (corrupt records) |
| Echo correct journal, disk faults seen: bit rot, corrupt records, torn writes, torn files at replay | echo.rs:172 | 20 / 100 / 1 000 / 10 000 | 670, 409, 452, 452 | 6 827, 4 289, 4 593, 4 593 | 6 827, 4 289, 4 593, 4 593 — the same four | every → every | 2.7 × 10^-5 |
| WAL variants caught: `NoSyncDir`, `NoChecksum`, `AckBeforeSync` | wal.rs:67 (72, 77, 82) | 20 / 100 / 1 000 / 10 000 | 909, 964, 1 000 | 9 150, 9 744, 10 000 | 9 150, 9 744, 10 000 — the same three | every → every | ~0 |
| WAL coverage: torn writes, lost fsyncs, bit rot, stops at a torn record, stops at a bad checksum, discarded segments, the lost-fsync excuse, the bit-rot excuse | wal.rs:144 | 20 / 100 / 1 000 / 10 000 | 1 000, 1 000, 1 000, 977, 999, 1 000, 994, 994 | 9 999, 10 000, 10 000 seeds; the rest thousands of epochs | 9 999, 10 000, 10 000 seeds; 29 026, 40 607, 68 533, 37 804, 32 074 over 80 000 epochs | every → every | ~0 |
| WAL: a gap | wal.rs:152 | 20 / 100 / 1 000 / 10 000 | 51 (5.1 %) | 615 epochs, ≤ 6.2 % | 615 epochs, ≤ 6.2 % | ≥ 100 → ≥ 100 | 0.0053 |
| **WAL: the betrayed-cut excuse** | wal.rs:165 | 20 / 100 / 1 000 / 10 000 | 34 (3.4 %) | 401 epochs, ≤ 4.0 % | 401 epochs, ≤ 4.0 % | **every → ≥ 1 000** | 0.50 at 20 → 9.5 × 10^-16 |
| **WAL: a betrayed cut — a cut of recovery's own whose sync the disk lied about** (D-062) | wal.rs:193 | 20 / 100 / 1 000 / 10 000 | **765 (76.5 %)**, on the D-062 tree, not 2ec4bf7 | not on those trees: the counter is D-062's | **7 806 epochs, ≤ 78 %** | **new → every** | 2.6 × 10^-13 (0.235²⁰) |
| **WAL: a betrayed cut to nothing**, which resurrects a whole segment (D-062) | wal.rs:208 | 20 / 100 / 1 000 / 10 000 | **60 (6.0 %)**, on the D-062 tree | not on those trees: the counter is D-062's | **579 epochs, ≤ 5.8 %** | **new → ≥ 100** | 0.0021 at 100 (0.94¹⁰⁰); 0.29 at 20, where the twenty in fact see none |
| **WAL: a supersede, the rule itself firing** (D-062) | printed with the coverage, wal.rs:46 | 20 / 100 / 1 000 / 10 000 | **0 of 1 000**, on the D-062 tree | not on those trees: the counter is D-062's | **0 of the sweep's 10 000 seeds (80 000 epochs)** | **new → asserted nowhere; printed at every tier** | not asserted. At the engine seek sweep's measured 1 in 10 000 a thousand seeds see none with probability 0.90, so no tier can carry it; seed 3123's pin carries it instead |
| Engine Phase 1 variants caught: `NoWalBeforeMemtable`, `ReleaseBeforeManifest`, `DeleteBeforeManifest` | engine.rs:553 (558, 563, 568) | 20 / 100 / 1 000 / 10 000 | 985, 602, 647 | 9 825, 6 482, 5 893 | 9 823, 6 254, 6 473 | every → every | 9.9 × 10^-9 |
| Engine `InstallInTwoSwitches` caught | engine.rs:296 | 20 / 100 / 1 000 / 10 000 | 559 | not on those trees | 5 457 | every → every | 7.7 × 10^-8 |
| Engine `RangeDeleteSkipsMemtables` caught, on the share | engine.rs:359 | 20 / 20 / 100 / 1 000 | 89 of 100 | not on those trees | 933 of its 1 000 | every → every | 6.7 × 10^-20 |
| Engine `SeekCountsTombstones` caught, on the share | engine.rs:422 | 20 / 20 / 100 / 1 000 | 98 of 100 | not on those trees | 972 of its 1 000 | every → every | ~0 |
| Engine `InstallKeepsSourceNumbers` caught by the oracle, on the share | engine.rs:489 | 20 / 20 / 100 / 1 000 | 98 of 100 | not on those trees | 974 of its 1 000, and 1 by the writer's order assertion | every → every | ~0 |
| Engine `SpanCheckpointUnsynced` caught, on the share | engine.rs:530 | 20 / 20 / 100 / 1 000 | 81 of 100 | not on those trees | 816 of its 1 000 | every → every | 3.8 × 10^-15 |
| Engine coverage, 29 counters: live reads, scans, rotations, flushes, crashes mid-flush, recoveries that replayed, excused losses, lost fsyncs, bit rot, torn writes, tables written, segments deleted, orphans removed, tables dropped, manifest fallbacks, missing log heads, batches, unsynced writes, checkpoints opened after a crash, compactions, compactions below level 0, inputs deleted, writes dropped, tombstones dropped, installs, range deletes, span checkpoints, seeks, recovery seeks | engine.rs:774 | 20 / 100 / 1 000 / 10 000 | 1 000, 1 000, 990, 983, 965, 970, 964, 1 000, 988, 997, 988, 983, 956, 831, 818, 704, 999, 1 000, 819, 976, 973, 981, 976, 976, 977, 961, 993, 1 000, 971 | the first 24 above zero (lost fsyncs 9 998, bit rot 9 906, torn writes 9 972 seeds); the last five not on those trees | all 29 above zero: lost fsyncs 9 999, bit rot 9 920, torn writes 9 976 seeds; missing log heads 11 973; the last five now printed — installs 58 901, range deletes 40 672, span checkpoints 82 264, seeks 1 999 417, recovery seeks 758 002 | every → every | 2.7 × 10^-11 (missing log heads) |
| Engine: a crash inside a compaction | engine.rs:780 | 20 / 100 / 1 000 / 10 000 | 742 | 6 202 events | 13 504 events | ≥ 100 → ≥ 100 | ~0 |
| Engine: a store refused for a fault | engine.rs:784 | 20 / 100 / 1 000 / 10 000 | 162 (16.2 %) | 1 146 (11.5 %) | 1 668 events, ≤ 16.7 % | ≥ 100 → ≥ 100 | 2.1 × 10^-8 |
| Live install: span checkpoints verified, live reads over an install | engine.rs:232, 233 | 20 / 100 / 1 000 / 10 000 | 977, 998 | not on those trees | 59 164 and 2 020 152 events | every → every | ~0 |
| Live install's windows: aimed, between replacement and switch, after the switch, keys written after | engine.rs:256–265 (from 230) | 20 / 100 / 1 000 / 10 000 | 1 000, 490, 411, 990 | not on those trees | 73 255, 6 403, 4 758, 1 649 955 events | every → every | 2.5 × 10^-5 |
| Range delete's windows, the same four | engine.rs:256–265 (from 330) | 20 / 100 / 1 000 / 10 000 | 1 000, 497, 377, 984 | not on those trees | 73 899, 6 187, 4 569, 2 606 393 events | every → every | 7.8 × 10^-5 |
| Seek: a seek stopped at its limit, a recovery walked | engine.rs:394, 395 | 20 / 100 / 1 000 / 10 000 | 999, 972 | not on those trees | 1 355 458 and 807 889 of 2 017 057 live seeks | every → every | ~0 |
| Deep levels: a round from level 2 or deeper, level 3 reached | engine.rs:177, 181 | 0 / 0 / 0 / 1 000 deep seeds | 965, 965 of 1 000 deep seeds | 10 132 rounds, deepest 3 | 11 609 rounds, deepest 3 | nightly only → nightly only | ~0 |
| Raft variants caught: `SendBeforePersist`, `ApplyBeforeCommit`, `CountOlderTermForCommit`, `TruncateOnEveryAppend`, `ResetTimerOnAnyRpc`, `SnapshotWithoutCurrentLast` | raft.rs:1736 (1926–1957) | 20 / 100 / 1 000 / 10 000 | 1 000, 882, 454, 1 000, 336, 336 | 10 000, 8 902, 4 415, 9 995, 3 465, 3 302 | 10 000, 8 843, 4 359, 9 996, 3 548, 3 371 | every → every | 2.8 × 10^-4 |
| `NoPreVote` caught by the pre-vote check | raft.rs:1918 | 20 / 100 / 1 000 / 10 000 | 1 000 | 9 999 | 9 998 | every → every | ~0 |
| D-050's term-raise shape reached | raft.rs:1471 | 20 / 100 / 1 000 / 10 000 | 298 | 2 893 (0557590 only) | 2 833 | every → every | 8.4 × 10^-4 |
| `NoPreVote` caught on the term-raise schedule | raft.rs:1637 | 20 / 100 / 1 000 / 10 000 | 1 000 | 10 000 (0557590 only) | 10 000 | every → every | ~0 |
| `AdoptionAsBuilt`'s firing: the storm drawn, adoptions under it | raft.rs:2004, 2008 | 20 / 100 / 1 000 / 10 000 | 260, 1 000 | 2 529 seeds; 84 582 adoptions | 2 529 seeds; 87 456 adoptions | every → every | 2.4 × 10^-3 |
| `AdoptionAsBuilt` caught | raft.rs:2013 | 20 / 100 / 1 000 / 10 000 | 77 (7.7 %) | 646 (6.46 %) | 637 (6.37 %) | ≥ 100 → ≥ 100 | 3.3 × 10^-4 |
| `RefusalNotDurable`'s firing: a crash on a refused server | raft.rs:2050 | 20 / 100 / 1 000 / 10 000 | 347 | 3 318 | 3 371 | every → every | 2.0 × 10^-4 |
| `RefusalNotDurable` caught | raft.rs:2074 | 20 / 100 / 1 000 / 10 000 | 16 (1.6 %) | 133 (1.33 %) | 129 (1.29 %) | ≥ 100 → ≥ 1 000, by the owner (D-056) | 9.9 × 10^-8 |
| `IgnoreIncarnation`: a refused follower re-seeded and applying | raft.rs:2358 | 20 / 100 / 1 000 / 10 000 | 659 | 6 353 | 6 283 | ≥ 100 → ≥ 100 | ~0 |
| `SharedSnapshotDir`'s firing: a re-take at an index already taken | raft.rs:2460 | 20 / 100 / 1 000 / 10 000 | 525 | 5 292 | 5 265 | every → every | 3.4 × 10^-7 |
| `SharedSnapshotDir`'s aimed arm reached its stream | raft.rs:2464 | 20 / 100 / 1 000 / 10 000 | 143 (14.3 %) | 1 472 (14.7 %) | 1 477 (14.77 %) | every → every; for the owner, below | **0.046** |
| **`SharedSnapshotDir`'s stream half: a re-take under a live stream the follower never installs at afterwards** (added after this entry, by D-060's re-audit of `snapshot_takes`; `a_leader_that_shares_one_snapshot_directory_…`) | the same sweep | 20 / 100 / 1 000 / 10 000 | not measured: the fold was vacuous until the re-audit. On this tree **135 (13.5 %)**, and 10 of the first hundred, against the correct server's 0 of 1 000 | not measured on those trees | **1 308 (13.08 %)**, with 5 948 duplicate-chunk loops after them | **new → ≥ 100** | 5.0 × 10^-7 at 100; 0.055 at 20, which is why the gate's twenty do not carry it |
| `SharedSnapshotDir` caught by the liveness check | raft.rs:2469 | 20 / 100 / 1 000 / 10 000 | 2 (0.2 %) | 4 (0.04 %) | **2 (0.02 %)**; 5 caught in all, by check {linearizability 3, liveness 2} | ≥ 10 000 → ≥ 10 000 | 2.0 × 10^-9; 0.018 at the nightlies' rate, below |
| Lease: drift beyond the bound, the guard revoked | raft.rs:2602, 2603 | 20 / 100 / 1 000 / 10 000 | 503, 503 | 5 023, 5 023 | 5 023, 5 023 | every → every | 8.5 × 10^-7 |
| **`LeaseTrustsTheClock` caught (a stale read)** | raft.rs:2615 | 20 / 100 / 1 000 / 10 000 | 41 (4.1 %) | 472 (4.72 %) | 450 (4.50 %) | **every → ≥ 1 000** | 0.43 at 20 → 6.6 × 10^-19 |
| Raft coverage, 33 counters: partitions, one-way blocks, crashes, leader crashes, stale-sender faults, figure-8 drivers, burst puts, drift beyond the bound, lease reads, read-index reads, lease revocations, check-quorum step-downs, duplicates, injected drops, elections, a term above one, truncations, snapshots taken, compactions, crash-mid-install faults, crash-mid-adoption faults, re-take-under-a-stream faults, commits, applies, bit rot, puts, gets, deletes, compare-and-sets, completed, abandoned, redirected, uniformly scheduled seeds | raft.rs:3000 | 20 / 100 / 1 000 / 10 000 | 1 000, 432, 1 000, 425, 431, 727, 727, 503, 584, 1 000, 992, 1 000, 1 000, 1 000, 1 000, 1 000, 1 000, 1 000, 1 000, 517, 260, 246, 1 000, 1 000, 999, 1 000 (×7), 500 | all above zero (drift 5 023, a term above one 10 000, uniform 5 000 seeds) | all above zero (drift 5 023, a term above one 10 000, uniform 5 000 seeds; the three fault counters nearest the line: crash-mid-install 5 038, crash-mid-adoption 2 529, re-take-under-a-stream 2 500) | every → every | 3.5 × 10^-3 (re-take faults); uniform scheduling is half the seeds by `Policy::for_seed`, seed 0 among them, not a draw |
| Raft coverage from 100: refusals, torn writes, snapshots installed, streams resumed, re-seeded servers, re-seeds completed, adoptions, progress resets | raft.rs:3006–3040 | 20 / 100 / 1 000 / 10 000 | 853, 495, 1 000, 1 000, 852, 832, 1 000, 844 | all above zero (re-seeds completed on 8 163 seeds) | all above zero: 32 494, 7 092, 192 919, 672 557, 40 313, 8 007, 89 498, 22 671 | ≥ 100 → ≥ 100 | ~0 |
| `SingleMajorityInJointConsensus` caught | raft.rs:3122 | 20 / 100 / 1 000 / 10 000 | 296 | 2 720 | 2 386 | every → every | 8.9 × 10^-4 |
| Membership coverage, 9: grows, shrinks, joint and new configurations, learners promoted, partitions, completed, uniform seeds, compactions | raft.rs:3329 | 20 / 100 / 1 000 / 10 000 | 1 000 each; uniform 500 | all above zero where printed (before D-058) | all above zero: grows and shrinks 10 000 each, joint 105 524, new 245 527, learners promoted 23 976, partitions 20 000, completed 3 658 749, compactions 153 406; uniform 5 000 seeds | every → every | ~0; uniform not a draw |
| Membership: an install adopted | raft.rs:3337 | 20 / 100 / 1 000 / 10 000 | 1 000 | not printed before D-058 | 74 102 adoptions, and a snapshot-fed joiner on all 10 000 seeds | every → every | ~0 |
| Membership from 100: step-downs outside `C_new`, configuration reverts | raft.rs:3354 | 20 / 100 / 1 000 / 10 000 | 390, 56 (5.6 %) | 4 253 and 7 025 events, before D-058 | 3 870 and 569 events | ≥ 100 → ≥ 100 | 0.0031 (reverts) |
| **Membership: an election while joint** | raft.rs:3367 | 20 / 100 / 1 000 / 10 000 | 31 (3.1 %) | 463 events, ≤ 4.6 %, before D-058 | 365 events, ≤ 3.65 % | **≥ 100 → ≥ 1 000** | 0.043 at 100 → 2.1 × 10^-14 |
| Membership: `reverts_to_a_prefix` | raft.rs:3382 | 20 / 100 / 1 000 / 10 000 | 28 (2.8 %) | D-058's counter, not on those trees | **264 events, ≤ 2.64 %** | ≥ 100 → ≥ 1 000, by the owner (D-058) | 4.6 × 10^-13 |
| Incremental checker: a compared seed in violation | raft.rs:3471 | 20 / 100 / 100 / 100 | 61 of 100 | 59 of 100 | 59 of 100 | every → every | 6.6 × 10^-9 |
| Quorum, `RefusedCountsForQuorum` blocked: a chunk lost to the limit | raft.rs:3585 | 20 / 100 / 1 000 / 10 000 | 1 000 | 300 304 events | 294 589 events | every → every | ~0 |
| Quorum on the sweep's disk: the install silence deposes `RefusedCountsForQuorum` | raft.rs:3679 | 20 / 100 / 1 000 / 10 000 | 844 | 8 311 | 9 058 failed, 9 002 of them by a step-down with nothing uncounted | ≥ 100 → ≥ 100 | ~0 |

**The ten-thousand-seed column, re-measured on this stage's own nightly.** The column
"At 10 000, run 35172923002 (27f6c97)" is this stage's own evidence: the green
ten-thousand-seed nightly on the branch tip, whose log is
`scratchpad stage-a/nightly4-green.log`. Every figure in it is read off that log, and a
row whose counter the log does not print says so rather than carrying a number over. The
column beside it, "At 10 000, the older nightlies", is runs 34908018220 (0557590) and
34948461479 (94c6a54) as before, and **both of those trees predate this stage** — its
lanes S and N, D-056's queue, D-058 and D-060 — so their raft, membership, re-seed and
engine figures are other schedules, and the two columns are not a before-and-after of one
tree. The unit is the log's: where a counter counts events the cell says so and gives the
seed rate as "≤", since the seeds that saw one are at most that many, and the WAL sweep's
counters run over 80 000 epochs on its 10 000 seeds.

**No rate in the new column crosses the 5 % line**, in either direction, so the rule moves
nothing here and every tier above stands. The rows near the line are the ones that were
near it before: `AdoptionAsBuilt` at 6.37 % (646 → 637), where a hundred seeds see none
with probability 0.9363^100 = 1.4 × 10^-3; the membership scenario's configuration reverts
at ≤ 5.69 %, where a hundred see none with 0.0029; a gap in the WAL at ≤ 6.15 %, 0.0018;
and D-062's betrayed cut to nothing at ≤ 5.79 %, 0.0026. The last three are event counts,
so the seed rate behind each could be a little under 5 % — the nightly does not settle
that, and the thousand-seed measurements above, which the rule reads, are 5.6 %, 5.1 % and
6.0 %. The two that moved furthest are both already at the thousand-seed tier:
`LeaseTrustsTheClock`'s stale read at 4.50 % (472 → 450) and the betrayed-cut excuse at
≤ 4.01 %. `SharedSnapshotDir`'s liveness catch, the thinnest row in the table, is at the
foot of the "For the owner" section below with what this nightly measured of it.

**The stage's nightly record.** SHARD.md §12 asks that before a stage is tagged the
nightly at ten thousand seeds run on the stage's branch and be green — every sweep and
directed scenario the stage runs, the correct system on every seed, every variant to its
standard (docs/SHARD.md:2030-2037, the owner's addition of 2026-09-15). Four ran on
`phase-3-stage-a` (PR #60):

| Run | Tree | Started → ended, UTC | Wall | Outcome |
| --- | --- | --- | --- | --- |
| 34908018220 / 34948461479 | 0557590, 94c6a54 | before the stage | — | the older nightlies the column above names; not this branch |
| 35080746132 | 09bed88 | 2026-09-16 09:40 → 11:05 | 1 h 24 m 55 s | **red** — the correct engine, seek schedule, seed 3123 (D-062) |
| 35111624618 | 1a1cad2 | 2026-09-16 14:53 → 18:25 | 3 h 32 m 02 s | **red** — the correct server, raft sweep, seed 2605 (D-063) |
| 35161762372 | 605e62e | 2026-09-16 23:19 → 2026-09-17 02:02 | 2 h 43 m 42 s | **green** |
| 35172923002 | 27f6c97, the tip | 2026-09-17 02:03 → 05:41 | 3 h 37 m 35 s | **green** |

The two red runs' downloaded logs hold only the `cargo test` step, so their wall is that
step's; the two green ones are the whole job, whose step is within twelve seconds of it.
The two green runs are the evidence for §12's bullet, and they agree with each other
figure for figure: every rate, every coverage counter and every first-catch seed in the
new column is identical in `nightly3-green.log` and `nightly4-green.log`. The only
differences between the two logs' summaries are the wall times, the order the parallel
binaries finished in, the last digits of two floats in `ReseedEpisodes`, and
`ananke-sim`'s lib tests at 17 against 19 — the two checker-level tests over hand-written
records that 46e95c0 added between the two trees (D-063), which need no seed and move no
schedule. That is what a stage whose last commits move no schedule should look like.

**The tip's run covers the tip, and this commit is docs-only on top of it.** Run
35172923002 ran on 27f6c97, the tree this entry is being written on. The commit that
lands these paragraphs touches docs/DECISIONS.md and nothing else — no crate, no sweep,
no test, no script, nothing the nightly exercises — so the green run is evidence for the
tree the commit makes as much as for the tree it ran on, and `scripts/gate.sh` is green
on that tree, as CLAUDE.md asks of every commit. It is worth saying plainly because the
opposite is the usual case: a nightly is evidence for the tree it ran on, and a commit
that changed a line of the model would need its own.

**Issue #57, in live figures.** D-055 projected the nightly's `cargo test` step at about
2 h 45 min against the job's 300-minute timeout and D-060's re-measurement moved that to
about 2 h 51 min. What the four runs took: the red run that got furthest, 35111624618,
**212 minutes**, and the two green ones **164** and **218 minutes**, the tip's run at 73 %
of the timeout. The two green runs did the same work — raft 5 834.48 s against 7 509.64 s,
engine 3 746.41 s against 5 228.23 s, wal 154.11 s against 204.16 s, a third more wall for
figures identical seed for seed — so that spread is the runner, not the tree. A 54-minute
swing between two runs of the same work is the sharpest thing the issue has: the headroom
left is a draw, not a margin.

**The three WAL rows marked D-062** were added by the commit that closed that entry's
gaps, so the register holds every assertion the supersede rule brought with it rather
than leaving their rates in D-062's prose alone. Their line numbers are on that commit
and their figures are that tree's, re-measured for this table rather than copied over:
one run of `ANANKE_SEEDS=1000 cargo test -p ananke-sim --test wal --release` gives
`betrayed_cuts 765, betrayed_cuts_to_nothing 60, superseded 0`, the same three figures
D-062 records, in a `Coverage` whose every pre-existing counter is also unchanged
(`epochs 8000, records 2059643, stops_torn 2880, stops_bad_checksum 4074, stops_gap 54,
discarded 6838, excused_lost_fsync 3782, excused_bit_rot 3192, excused_betrayed_cut 36`).
The two tiers below were measured too, since the second row's tier turns on them: at a
hundred seeds 79 and 4, at the gate's twenty 14 and **0**. So the gate would be asserting
a betrayed cut to nothing on a counter that is in fact zero there — the assertion is
guarded at `seeds() >= 100`, which is the rule doing its work rather than a precaution.

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
  **Measured on this stage's own nightly**, run 35172923002 on the tip: the arm reaches its
  stream on **1 477 of 10 000, 14.77 %**, beside 5 265 re-takes at an index already taken
  and 1 308 seeds of the stream half. So the number the question turns on has not moved —
  14.3 % and 15.3 % at a thousand on two trees, 14.7 % at the older nightlies and now
  14.77 % at this stage's own, four measurements on three trees — and at
  14.77 % the gate's twenty see none with probability 0.8523^20 = 0.041, still above one in
  a hundred. The measurement is recorded; the choice between leaving it and moving it to
  the hundred-seed tier is the owner's and is not made here.
- *`SharedSnapshotDir`'s liveness catch at the nightly's ten thousand*
  (the same test). It is under 5 % and already above the thousand-seed tier, so the rule
  leaves it. On the tree with the queue it was on 2 of the thousand, 0.2 %; **on the tree
  with the key layout it is on 0 of the thousand**, so its rate here is at most 0.1 % and
  ten thousand seeds see none with probability at least 0.37. At the nightlies' measured
  rate, 4 of 10 000, ten thousand see none with probability 0.9996^10000 = 0.018. The next
  nightly is what measures it. D-060's question 6 puts the harder half to the owner: the
  pair and each half are caught on 0 of the first thousand, so no seed pins the catch and
  seed 132 asserts its absence, which SHARD.md:2362-2363 routes to the owner.
  **The next nightly has now run, and this is what it measured.** Run 35172923002 on the
  tip, and run 35161762372 before it, print the same line:

  ```
  SharedSnapshotDir: caught on 5 of 10000 seeds, 2 by the liveness check, by check
  {"linearizability": 3, "liveness": 2}, re-took at an index already taken on 5265 seeds,
  scrambled a live stream the follower never installed after on 1308 seeds (5948
  duplicate-chunk loops after those), the aimed re-take arm reached its stream on 1477
  seeds, first: seed 3300: liveness: no client write completed after the last heal at
  Instant(13.289s)
  ```

  So the catch is on **5 of 10 000, 0.05 %**, of which **2 are the liveness check's,
  0.02 %** — half what the older nightlies measured (4) and not the 0 the thousand-seed
  tree suggested. The assertion is the liveness half alone, at the ten-thousand-seed tier,
  and at 0.02 % ten thousand seeds see none with probability 0.9998^10000 = **0.135**: about
  one nightly in seven would fail this tree with nothing wrong in it. Read against the whole
  catch, 5 of 10 000, ten thousand see none with 0.0067. Both figures are measurements, not
  a decision: whether an assertion that misses one run in seven belongs at the nightly tier
  at all, or belongs beside the liveness check as a whole-catch assertion, or nowhere, stays
  the owner's to answer.

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

## PROPOSED D-062 — A segment whose first record is behind the reading supersedes what was read

**Context.** The nightly's ten thousand seeds failed on the **correct** engine (GitHub
run 35080746132, `phase-3-stage-a` at 09bed88, PR #60), at the seek schedule's seed
3123: `epoch 3: record 166 came back changed: 44 bytes recovered, 29 appended`, the WAL
checker's Property A. Everything else in the binary passed; the premerge's thousand and
the gate's twenty are green on the same tree, so the seed is beyond the thousand. It is
not a checker artefact and not a variant's catch: on the correct engine a legal disk
fault made recovery return superseded records under live numbers, throw away thirteen
acknowledged records whose syncs the simulator had honoured, and replay a stale record
into the memtable above `flushed_seq` — the one thing D-018 says is never excusable.

The defect is shipped Phase 1 code, not Stage A's: `parse_segment`'s numbering rule
(D-019), reached through `Wal::open`'s mid-log cut (D-018). A recovery that stops
mid-log discards the tail by cutting the stopping segment and syncing it, and then
trusts that cut. When the disk lies about that fsync (`FsyncLost`, legal under SPEC
§1.3) and the next crash drops the still-pending truncation, the segment comes back
whole, holding records under numbers the log has since re-issued — `next_seq` reuses
discarded numbers on purpose, because a number is a position and not an identity
(D-018). At the next open the reader meets that resurrected segment *before* the live
one and, with the stop rule as D-019 wrote it, has nothing to tell them apart.

At seed 3123 segment 14 had been cut to nothing and came back with 166..=172. Its first
record satisfied `seq > expected && seq <= expected_head` (166 ≤ 172), D-022's forward
skip, so the thirteen records already read from segment 13 were dropped and the whole
numbering re-based onto the stale segment. The live segment 15, whose first record 165
is *behind* the reading, was then an ordinary `Gap { expected: 173, found: 165 }` — a
stop. Recovery kept seven stale records, cut the live segment to nothing, removed the
one after it, destroyed the acknowledged 165..=177 and replayed the stale 172 above
`flushed_seq` 171. The simulator did only what a real filesystem does with an
`ftruncate` whose journal transaction has not committed, and the checker is right to
fail: nothing here is excusable. The comment at the cut already named this failure ("a
cut whose sync the disk lied about brings the old records back at the next crash …
numbered as if they were current", found at seed 191) and forbade it — but only in the
head-gap branch, which discards by removal for exactly this reason.

What the reader was missing is the *direction* of the jump. Segments are created in
increasing number order, and D-022 made segment numbers monotone so that a number names
one file for the life of the log. So a later segment whose first record repeats a number
an earlier segment supplied **proves the earlier segment stale**. `parse_segment`
treated forward and backward jumps identically.

**Decision.** A record numbered *behind* the reading, at a segment's **first** byte, is
not a stop: it supersedes. The copies already read that are numbered at or above it are
dropped, the numbering resumes at it, and reading goes on into that segment. The drop is
safe because the records discarded are exactly the ones this segment re-supplies, and
anything below them is below the first number read, which is at or below
`expected_head`, so the caller holds it in the tables. A backwards jump anywhere else in
a segment is corruption and still stops: a betrayed cut leaves a prefix and brings back
a tail contiguous with it, never a jump in mid-file. D-022's forward skip keeps its head
guard; the backward rule needs none, because the order of creation and not the head is
what settles it — which is why it repairs the shape where the tables are further behind
than the resurrected numbers, and a head-guarded rule would not.

The supersede emits `WalSuperseded { segment, expected, found, dropped }`, bridged as
`ananke.wal.superseded`, so the sweep and the pin can see the mechanism rather than
infer it (CLAUDE.md: if it can't be seen in the studio it didn't happen).

This corrects D-019, whose stop rule is written without direction and decides this case
the wrong way, extends D-022's "a jump that lands at or below the head is not a stop"
with its backward twin, and amends SPEC.md §2.2's stop bullet. Both entries carry a note
saying so. The oracle is unchanged: `Excuse::BetrayedCut` matches only a stop exactly
where a lost-sync cut had been, so it never excused this, which is correct — nothing
here should be excused.

**The pair, and where each half is asserted.** No existing variant covers the rule;
`Variant::TrustsAStaleSegment` is today's reader kept beside the fix — on a backwards
jump it keeps the earlier prefix and stops. **It cannot be paired at the sweep tier, and
that was measured, not assumed:** run over the seek sweep's whole nightly band, seeds
0..10000, the variant is caught on **1 seed — 3123, with the nightly's own message** —
a catch rate of 0.01 %, three orders below D-061's five per cent. The shape needs a
betrayed cut *and* a crash that drops the truncation *and* a later segment that re-issues
the numbers. The honest pairing is therefore in three parts.

- *The catch, deterministic.* Two hand-built on-disk states in
  `crates/ananke-storage/tests/wal.rs` run the correct log and the variant side by side
  at every tier: the nightly's own shape, a segment cut to nothing that came back whole
  in front of the live one (13/14/15 holding 152..=164, the stale 166..=172 and the live
  165..=177, head 172), and the shape a cut to a *shorter* length leaves, a stale tail
  behind live records in the same segment with the tables further behind than the stale
  numbers. The correct log returns the live records with no stop; the variant returns the
  stale ones, stops, and cuts the live segment away. A third state beside them pins the
  rule's *narrowing* rather than its catch: a backwards jump at a non-zero offset inside a
  segment, where both readers stop and keep the live records whole. Without it the
  `at == 0` guard was a claim no test held — deleting the guard broke nothing in the suite,
  at any tier.
- *The seed.* 3123 is pinned in `sim/tests/engine.rs` with its mechanism: under the fix
  the run still reaches a cut to nothing whose sync the disk lied about and is still seen
  to supersede a resurrected segment — *that* one, by its numbers,
  `WalSuperseded { segment: 15, expected: 173, found: 165, dropped: 7 }`, so that a
  schedule which moved the seed onto some other resurrection fails the pin instead of
  passing it — and the variant still fails that seed with the violation it was pinned for.
  The situation survived the fix, so the pin asserts it rather than its absence.
- *The shape, in the sweep.* `sim/tests/wal.rs` counts the precondition. Measured over
  the first thousand seeds of the WAL sweep on this tree: a cut of recovery's own made on
  a sync the disk lied about on **765 of 1000, 76.5 %**, asserted at every tier (the
  gate's twenty see none with probability 0.235²⁰ = 3 × 10⁻¹³); such a cut *to nothing*,
  which resurrects a whole segment, on **60 of 1000, 6.0 %**, which by D-061 stays at the
  hundred-seed tier (a hundred see none with probability 0.94¹⁰⁰ = 0.002, the gate's
  twenty with 0.29). The rule *firing* is rarer than either and is asserted nowhere: 0 of
  those thousand superseded. Every tier prints all three.

**Rates measured on this tree.** WAL sweep, `ANANKE_SEEDS=1000`: betrayed cuts 765,
betrayed cuts to nothing 60, supersedes 0. Engine seek schedule, the nightly's whole band
**0..10000 run locally with the fix: 0 failures**, betrayed cuts 4393 (43.9 %), betrayed
cuts to nothing 337 (3.4 %), and the supersede fired on **exactly one seed of the ten
thousand — 3123**; the band 3000..3400 gives 180, 12 and the same single supersede. The
same band under `TrustsAStaleSegment` gives the identical 4393 and 337 and the one
failure, which is what "the variant changes only that seed" means in numbers. The
precondition rate is schedule-independent within noise (the investigation measured 441
and 30 per thousand under both `seek` and `phase_1`), and the rule's own rate, 1 in
10 000 here and one in the first five thousand on the unmodified model, is why a
thousand-seed premerge misses the failure about 85 % of the time and why no tier can
assert the rule firing.

**Nothing moved.** No pinned trace hash and no seed's schedule moves. The change alters
recovery's decision only on a run that meets a backwards jump at a segment's first
record: no RNG draw, no byte written to disk, no record framing, no new fault arm. The
proof is the counter: on a seed where no supersede fires, no new code runs, so the trace
is byte-identical to the tree before the fix. The WAL sweep's whole `Coverage` at a
thousand seeds is unchanged (`stops_torn 2880, stops_bad_checksum 4074, stops_gap 54,
discarded 6838, excused_lost_fsync 3782, excused_bit_rot 3192, excused_betrayed_cut 36`)
with 0 supersedes, and the three WAL variants' catch rates with it; the engine's seek
schedule supersedes on 1 of its first ten thousand seeds, so 9 999 of them are unmoved,
and the one that moves is 3123, which is the seed being fixed. No other pinned seed's
schedule moves and no pinned trace hash moves, so nothing else is owed a re-audit.

**Alternatives.** *Make a cut to nothing a removal instead, as the head-gap branch does*:
prophylactic, not a repair — it cannot help a log that already holds a resurrected
segment, does nothing for a cut to a non-zero offset, and it breaks the pinned test
`recovery_stops_at_a_gap_in_the_numbering`, which pins today's cut-to-nothing behaviour.
*The same rebase guarded by `expected_head`, as the forward skip is*: repairs the
nightly's shape but not the one where the tables are further behind than the resurrected
numbers, which is the commoner of the two on disk. *A generation stamp per segment*: the
only thing that closes the residual shape below, and the only thing that would let the
reader answer "which writing is this" without inference; it costs a format change and is
BACKLOG. *Numbering records by identity rather than position, so a discarded number is
never re-issued*: it removes the conflict at its root, and with it D-018's "position, not
identity" and every oracle that reads a segment's sync history by number.

**Consequences.** One branch in the reader, and the reader is where it belongs: the
writer and the disk are behaving correctly and a real filesystem may drop a
not-yet-committed `ftruncate` exactly this way. The log now ships five variants where
D-018 said four, and the fifth is the first that the crash sweep does **not** catch; the
sweep asserts the shape it needs instead, and `tests/wal.rs` catches the variant itself.
There is a residual shape no reader-side rule can see: a stale tail whose numbers *abut*
the live segment's first number instead of overlapping it, where nothing in the numbering
conflicts. It is harmless to engine state by construction — the fresh segment can only be
numbered above the stale tail when `expected_head` was already above it, so those records
were flushed, and replay skips everything at or below `flushed_seq` — but it is the limit
of this fix and is an issue, not code. Two observability gaps the investigation tripped
over are also issues, not code: the simulated filesystem emits no trace event when a
crash drops a pending `PendingOp::Truncate`, so the very fault that makes this shape is
invisible in the studio; and `Wal::open`'s `firsts` map records a segment's first number
as the running total rather than the segment's own, an over-estimate that survives a
supersede in the same direction, so `delete_segments_through` only ever deletes later
than necessary. `Schedule::wal_variant` is new in `sim/engine.rs` so the pin can run seed
3123 beside the variant; it is `Correct` everywhere else. The three counters this entry
adds to the WAL sweep have rows in D-061's register, with their rates, tiers and
probabilities, so the register stays the one place every such assertion is listed.

**Issues filed out of this entry**, all of them notes rather than code, so the fix does
not widen past its one branch:

- **#61** — the correct engine fails seed **30490** on the seek schedule: `record 1 is gone
  although the log stopped at nothing near it` (records 1..=0, tables through 0). It is a
  second, pre-existing defect and not this one, and that was measured rather than argued:
  it reproduces identically on 09bed88 and on the tree with this fix, with no supersede
  firing on that seed, so the branch above never runs and the code path is byte-identical
  on both. It is outside the nightly's band (about 1 seed in 30 000 of the seek schedule)
  and its trace points at the manifest and `CURRENT` fallback, not the WAL reader. The
  exit criterion is therefore met for the nightly's ten thousand and not for the whole
  model, which the owner should hear before signing "passes every seed".
- **#62** — the residual shape named above: a resurrected segment whose numbers *abut* the
  live ones instead of overlapping them, which no reader-side rule can see. The limit of
  this fix; closing it wants a per-segment generation stamp, an on-disk format change with
  its own entry and a format version beside D-060's.
- **#63** — the two observability gaps: the crash model traces nothing when it drops a
  pending `PendingOp::Truncate`, so the very fault that builds this shape is invisible in
  the studio, and `Wal::open`'s `firsts` map records a running total rather than each
  segment's own first record.

**At ten thousand, on the stage's two green nightlies.** The tier that found this defect
has since run twice on this branch with the fix in: run 35161762372 on 605e62e and run
35172923002 on the tip, 27f6c97. In both, `the_seek_crash_test_passes_every_seed` passes
over the nightly's ten thousand and **the whole engine binary is green, 18 passed and 0
failed** (5 228.23 s on the tip, 3 746.41 s on the run before it), with
`seed_3123_which_the_nightly_found_supersedes_a_resurrected_segment` among the tests that
pass — so the seed this entry was written for is asserted to still reach a betrayed cut and
still supersede its resurrected segment, by its numbers, rather than merely to come back
green. Seed 30490, issue #61's, is outside the nightly's band as this entry says, and
neither run reaches it.

The WAL sweep's own `superseded` counter is **0 at ten thousand seeds** in both runs:
`Coverage { seeds: 10000, epochs: 80000, …, excused_betrayed_cut: 401, betrayed_cuts: 7806,
betrayed_cuts_to_nothing: 579, superseded: 0 }`. That is this entry's "rarer than either and
asserted nowhere", measured a tier higher than it could be measured when the entry was
written: the two preconditions are reached — 7 806 and 579 over 80 000 epochs, at most 78 %
and 5.8 % of the ten thousand seeds, the first far above the 5 % line its tier reads and the
second just above it, as the 6.0 % at a thousand was — and the rule still fires on none of
the WAL sweep's ten thousand. Its rows in D-061's register carry the same three figures.

**This sharpens the question below; it does not answer it.** `TrustsAStaleSegment` is caught
by no sweep at any tier, and the nightly is the highest tier there is: at ten thousand the
WAL sweep does not fire the rule once, so a sweep-tier pairing cannot be reached by running
more seeds of that sweep. The only place the rule is seen to fire in a green nightly is seed
3123's pin, on the engine's seek schedule — a fixed seed, not a draw. So one option the
owner might have weighed, wait for a larger tier to catch it, is closed by measurement
rather than by argument; what remains is what this entry already puts to the owner, the
deterministic states and the pin as they stand, or an arm that builds the shape deliberately.

**For the owner — not decided here.** `Variant::TrustsAStaleSegment` is the first WAL
variant that **no sweep catches at any tier**. It is caught deterministically, at every
tier, by the two hand-built on-disk states above, and by the pinned seed 3123, which runs
the variant through the engine sweep's own scenario and asserts the violation the nightly
reported. What it has not got, and cannot get, is the shape every other variant has — seen
to fail on some seed of a sweep: the measured catch rate is 1 seed in the seek schedule's
ten thousand and 0 of the WAL sweep's first thousand, so even the nightly would catch it
only sometimes, and D-061's rule puts an assertion at the tier its rate supports, which
here is no tier at all. CLAUDE.md's pair rule asks that a known-buggy variant be kept
beside the correct code and *seen to fail*. Whether the deterministic states and the pin
satisfy that rule as written, or whether a variant this thin should be paired some other
way — a sweep arm that builds the shape deliberately, as `RETAKE_STREAM_IN` does for
D-043, which is a good deal more than a one-branch fix — is the owner's to settle. This
entry records the substitution and what it measured; it does not decide it.

---

## PROPOSED D-063 — A server adopting a completed install is not running, and the timer check stops measuring it

**Context.** The nightly's ten thousand seeds failed on the **correct** server (GitHub
run 35111624618, `phase-3-stage-a` at 1a1cad2, PR #60), at the raft sweep's seed 2605:

```
seed 2605: timers: server 3 heard from no leader of its term and granted no vote
since Instant(19.679704065s) and had not campaigned by Instant(19.994418991s)
```

Everything else in that run passed — the whole engine binary, so D-062's fix holds at the
tier that found it; the membership and quorum scenarios; every variant's sweep; the
incremental checker and every pinned seed — and `41 passed; 1 failed` in 7 483 s. The
gate's twenty, CI's hundred and the premerge's thousand are green on the same tree, so
the seed is beyond the thousand. Decision time (D-047) removed one catch in that run
(seed 2313) and added none.

**The defect is in the check, not in shipped code and not in Stage A's own new code.**
Nothing in `crates/` is wrong and nothing in `crates/` changes. The rule at
`sim/raft.rs` was already wrong on 903f37c, before D-056's send queue and D-060's key
layout, and it is wrong in the released v0.3.0, whose harness has the identical
`up`/`RaftRecovered`/`NodeCrashed` structure. What the stage moved is the margin, not the
rule: measured over seeds 0..3000 on each tree, the longest *silent* adoption window —
one with no leader frame delivered inside it — runs 0.81 of its server's whole timer
bound on 903f37c and on main (14c3e17) and 1.03 on 1a1cad2, and the window's own length
goes from p50 162.9 ms to 171.4 ms, min 94.7 to 104.6, max 348.8 to 353.1. That +8.5 ms
at the median is the `RAFT-FORMAT` record's filesystem operations (D-059, D-060),
corroborating the layout's own measurement at 3 000 seeds instead of 200.

**What the server does.** A completed install **ends the incarnation**.
`install_decision` stops racing the tick and awaits only `inbox.pop()`
(`crates/ananke-raft/src/node.rs`), returns `Next::Reinstall`, the outer loop takes it and
re-runs `start_store` — D-059/D-060's format read, D-041's `adopt_checked`, the marker,
the engine, the store — **with no core and no election timer**. The next timeout is drawn
in `Raft::restore_compacted` and armed by the loop's first tick, immediately after the
restatement, exactly as `start_store`'s own comment says: "nothing awaits between these
records and the loop arming its first tick, which is where the new core's election timer
really starts counting." The `Next::Reinstall` the re-seed path returns (D-035) is the
same thing.

**What the check did.** The replay's only notion of "has a running incarnation" is `up`,
which a server entered at its `RaftTerm` and left only at `NodeCrashed`. D-039's arm
resets the clock **at** the restatement, which closes the far end of the window; the near
end — the whole adoption — was still charged to the last contact before the completion.
On seed 2605: server 3 heard the leader's last AppendEntries at 19.679704065 s; its
install of snapshot 374 completed 24.655 ms later, decided at 19.704359292 s and durable
at 19.763649610 s; the adoption ran `RaftAdopted` at 19.880743071 s and the WAL recovered
at 19.957360426 s; the restatement landed at 20.002065925 s. That is 322.361860 ms
against server 3's 313.983572 ms bound (drift 273 952 ppm), over by 8.378288 ms, and
**297.706633 ms of the measured stretch is a window in which the server had no timer to
fire**. The flag fell at the first record past the bound, 19.994418991 s — 8 ms before
the restatement — and `sim/tests/raft.rs` panicked in the nightly's words. Ten of the
leader's frames were aimed at the server inside that stretch — nine `AppendEntries` and
one `InstallSnapshot`, all from server 1 — and the partition at 19.729 s dropped every
one of them at the send as `Partitioned`, along with one client frame; nothing at all was
delivered to server 3 in the window, so nothing reset the check's clock by accident.

**Decision.** For the timer check, a completed install takes the server **out of the
replay's running set** until its restatement puts it back — the same treatment a crash
gets, for the same reason: between the completion and the restatement there is no
incarnation to campaign. The site is `TraceEvent::RaftSnapshot { taken: false }` on a
server the replay holds up, excluding the restatement's own re-trace of the store's
snapshot, which is told apart by the `RaftRecovered` that follows it at the same instant
in `start_store`'s stable order (D-029). The server's `RaftTerm` at the end of the
restatement re-admits it and resets the clock, as every start does. The protocol is
unchanged, `TIMER_TIMEOUTS` is unchanged, the bound is unchanged, and no trace changes.

This **supersedes D-039's arm** under `TimerResets::ALL`: every restatement on a server
that never went down follows a completed install, so the server is no longer in `up` when
that arm is reached and the arm is now unreachable in the check. It stays in the code,
with `TimerResets::WITHOUT_RESTATEMENT`, because seed 385's pin *is* that replay — the
check as it stood on f54b468 — and D-039's account of seed 385 stands: an install that
takes 225 ms and a fresh timer after it is exactly this window, seen from the other end.

**Alternatives.** *Widening the bound, or `TIMER_TIMEOUTS` from two to three*: forbidden
by D-030, D-039 and RAFT.md §5 — "what was wrong was the check's model of the protocol,
not the bound" — and it dulls the catch of `ResetTimerOnAnyRpc`. Tightening it instead is
the measurement that shows this is a model error rather than a margin: with a check-only
`ANANKE_BOUND_SCALE` knob on `timer_bound`, 1a1cad2 at **0.85 × bound** fails on **two**
seeds of 0..3000 — 2605 and 500, both the same shape, a silent adoption window — and the
tree with this fix fails on **none**. *Resetting the clock at the install's completion
instead of exempting the window*: measured, not argued, and not enough. Of 11 421 uniform
completions in 0..3000, four already run longer than their server's entire bound between
the completion and the restatement, and 169 of them (1.48 %) are silent; and the two are
not independent, because `Fault::CrashAdopting` — one seed in four (D-041) — isolates a
follower and then crashes it mid-adoption, which is how 2605 was built. *Making the
server inherit the old timer's elapsed count*: D-039 rejected it and this entry does not
reopen it; it is a change to the protocol's timing, not to the check.

**The pair.** The existing one covers it: **`ResetTimerOnAnyRpc`** (RAFT.md §5, moirae
rule 5), the variant this rule is written for. The narrowing takes nothing from its catch,
measured by running both replays over every seed of a band and diffing the gaps they find,
violation text for violation text:

| Band, on the committed tree | Caught | Gaps this entry adds | Gaps it removes |
| --- | --- | --- | --- |
| `ResetTimerOnAnyRpc`, seeds 0..1000 | **344** (34.4 %), **330** of them by the timer check | **0** | **0** |
| `SnapshotWithoutCurrentLast`, seeds 0..1000 | 356 (35.6 %), 0 by the timer check | 0 | 0 |
| `AdoptionAsBuilt`, seeds 0..1000 | 57 (5.7 %), 0 by the timer check | 0 | 0 |
| the correct server, seeds 2000..3000 | 0 failures; 1 adoption-rescued gap, seed 2605 | 0 | **1** — the nightly's |
| the correct server, seeds 5000..9000 | 0 failures; no adoption-rescued gap | 0 | 0 |

So over the 6 000 correct-server seeds and the 3 000 variant seeds run here, the arm's
whole effect is the removal of one gap: seed 2605's. Nothing else it touches, in either
direction, on any seed. At 34.4 % the pair's catch is far above D-061's 5 % line, so
nothing moves tier; and the pin below asserts the catch on one named seed, which is
deterministic and runs at every tier.

**The pinned seed.** `seed_2605_which_the_nightly_found_is_an_adoption_window_and_still_
catches_the_variant` asserts the mechanism both ways, not green (CLAUDE.md):
`Report::timer_gaps_rescued_by_adoption` is the replay with every arm but this one —
`TimerResets::WITHOUT_ADOPTION`, the check exactly as it stood on 1a1cad2 — and on seed
2605 it is the nightly's one gap, its violation word for word, on server 3, with the one
completed install in it, while `check()` is green. It also asserts that that replay finds
nothing else on the seed, so the arm is seen to be exempting the adoption and not more.
And the pair runs on the same seed's own schedule: `ResetTimerOnAnyRpc` is still caught
there by the timer check, with two gaps, neither holding a completed install, and the two
replays find the same two. The day the first assertion fails the seed's schedule has moved
off the window and the pin is re-audited, not deleted.

**What fences the arm, beyond the seed** (issue #65's first two items). A pin is one
schedule, and a schedule holds only what it happens to contain: two mutations of this arm
leave seed 2605's pin green, the whole raft binary green at a hundred seeds and all three
catch rates unchanged — the arm could be widened to something plainly wrong, or replaced
by the alternative this entry rejects, and no sweep would say so. Both are now held by
checker-level tests over trace records written by hand (`sim/raft.rs`, `mod tests`), the
shape the membership fold's unit tests use, which need no seed and move no schedule. The
report they are built on has three servers whose clocks run true, so each bound is 400 ms.

- **`a_snapshot_a_server_took_itself_leaves_its_election_timer_running`** — the extent. A
  server up at 0 ms, silence after it, one `RaftSnapshot { taken: true }` at 200 ms and no
  restatement: the gap at 450 ms is still reported, with `adoptions: 0`, because a snapshot
  a server takes of its own accord retires no incarnation — it is the live core's own work
  — and leaves its election timer counting. The same trace with the take replaced by a
  completed install, and that install's restatement at 500 ms, is excused: `timer_gaps`
  under `ALL` is empty while `timer_gaps_rescued_by_adoption` is exactly that stretch with
  `adoptions: 1`, the mechanism both ways as the pin is. And 450 ms of silence *after* the
  restatement is a gap again, since the restatement, so the exemption is seen to close
  where it opens rather than leaving the server blind for good.
- **`a_coreless_window_longer_than_the_bound_is_removed_not_measured_from_the_completion`**
  — the alternative. The honest case is one where the coreless window *alone* outlasts the
  bound, which a reset at the completion would still measure and this entry's removal does
  not: the install completes at 100 ms, the restatement lands at 700 ms, and a record sits
  inside the window at 650 ms. The test asserts that the window outlasts the bound by
  itself, so the distinction is the assertion and not an accident of the numbers; that
  `timer_gaps(ALL)` is empty, a server with no incarnation not being measured against a
  timer that does not exist; and that the check as it stood on 1a1cad2
  (`WITHOUT_ADOPTION`) flags the stretch.

**What the mutations showed.** Each was applied in a throwaway copy outside the worktree
and run against `ananke-sim`'s whole test set in release at the gate's twenty seeds, with
`--no-fail-fast` so every target reports:

| mutation | the test that fails, and its words |
| --- | --- |
| `taken: false` dropped from the arm's pattern, so every `RaftSnapshot` excuses (issue #65's own mutation) | `a_snapshot_a_server_took_itself_…`: "a snapshot the server took is not a completed install and excuses nothing", `left: []`, `right: [TimerGap { server: 1, since: Instant(0ns), at: Instant(450ms), record: 2, installs: 0, restatements: 0, adoptions: 0 }]` |
| the arm's body `clocks.reset(*server, at)` in place of `up.remove`: the clock reset at the completion, this entry's rejected alternative | `a_coreless_window_…`: "a server with no incarnation is not measured: `[TimerGap { server: 1, since: Instant(100ms), at: Instant(650ms), record: 2, … }]`" |

In each run everything else is green — the other eighteen lib tests, the run's *other* new
test among them, and every sweep binary: echo 5, engine 18, parallel 1, raft 43 (seed
2605's own pin included, which is the hole), wal 6. So each mutation fails exactly one
test and it is the one written for it, where before this commit each failed none. The first two items of issue #65 are closed by this; its third — the
check being more forgiving than the server on install chunks, D-030's arm excusing a gap
the real follower does have — is pre-existing, is untouched here, and stays open there. It
is also the second issue note below.

**Nothing moved.** The change is confined to `Report` — the replay, its arms, `TimerGap`,
`TimerResets` and one predicate — and to `Report::timer_removal`, which learns that a
completed install is a status record so D-051's reasoning can name it; the sweep, the
scenario, the faults and the protocol are untouched. **No schedule moves and no pinned
trace hash moves**: seed 2605's run is byte-identical instant for instant before and after,
`same_seed_gives_byte_identical_trace`, `the_seed_42_trace_is_written_for_the_studio` and
the membership scenario's hash test pass, every pinned-seed test in `sim/tests` passes —
the nineteen `seed_*` tests, this entry's included — and `ResetTimerOnAnyRpc`,
`AdoptionAsBuilt` and `SnapshotWithoutCurrentLast` catch the same seeds line for line. So nothing is owed a re-audit. Decision time's removal on the raft
sweep is also unchanged — `ANANKE_SEEDS=2606` prints "removed 1 catches and added 0 /
removed: seed 2313", the nightly's own — so D-051 still resolves it.

**Measured on this tree.** The correct server over seeds **0..5000** and again over
**5000..9000** with this fix: **0 failures**, and in all nine thousand exactly **one** seed
with an adoption-rescued gap, 2605 — the same count the nightly's ten thousand give, one. Over 0..3000 there are 22 669 completed installs
whose restatement arrived while the check held the server up; twelve adoptions run longer
than their server's entire timer bound (seed 79 server 3 at 1.1712 ×, then 2472, 1802,
2515, 893, 771, 893, 159, 1546, 1681, 2515, 618) and each passes today **only** because
eight to seventeen leader frames were delivered into the socket of a coreless server, the
last of them 1.0 to 43.2 ms before the restatement. That accident is what this entry
removes as a load-bearing mechanism. The completion-to-restatement window itself, over
0..3000: min 104.603 ms, p50 171.396, p99 256.661, max 353.091.

**The stage's premerge, on the stage's tip.** §12 asks every stage to record the premerge it
measured beside the last one measured, and the last one in this document is D-060's, on
`ae54a20`, six commits back. On the tip, `46e95c0`, `scripts/premerge.sh` at a thousand
seeds is **green in 822.64 s** (13 min 42.6 s), 319 tests passed and none failed, at a mean
one-minute load of **60.94** over 87 samples (43.90 to 82.90) — `sim/tests/raft.rs`
535.89 s, `sim/tests/engine.rs` 245.96 s, `sim/tests/wal.rs` 23.26 s, everything else under
three seconds together. Beside it: D-060's **593.87 s at load 20.87** (`ae54a20`), D-055's
**540.37 s at load 17.64** (`3787528`) and D-052's **374.64 s** (`1ef6d7e`). The comparison
is confounded by load in the direction that flatters nothing — this run carried three times
D-060's — so the honest reading is that the quarter of an hour D-040 set still holds with
77 s to spare on a machine three times busier, not that the stage cost 229 s. One part is
attributable: the WAL binary tripled, 8.48 s to 23.26 s, which is D-062's two hand-built
recoveries and its wider bands. The earlier figure of 1 473 s reported for this fix's tree
was taken while another lane was building and sampled no load; it is withdrawn in favour of
this one, measured under D-052's protocol on an otherwise idle machine.

**At ten thousand, on the stage's two green nightlies.** The tier that found seed 2605 has
since run twice on this branch with this arm in: run 35161762372 on 605e62e and run
35172923002 on the tip, 27f6c97. In both **the raft binary is green, 43 passed and 0
failed** (7 509.64 s on the tip, 5 834.48 s on the run before it) — the whole sweep, the
membership and quorum scenarios, the incremental checker and every pinned seed, this
entry's
`seed_2605_which_the_nightly_found_is_an_adoption_window_and_still_catches_the_variant`
among them. The seed the nightly failed on now passes at the tier that found it, with its
mechanism asserted both ways rather than green, and the correct server passes every one of
the ten thousand.

The adoption figures the two runs print, identical in both:

- `AdoptionAsBuilt`: **caught on 637 of 10 000**, its storm drawn on **2 529 seeds with
  87 456 adoptions under it**, first seed 1; decision time (D-047) removed 3 catches and
  added none. The pair this entry leans on, `ResetTimerOnAnyRpc`, is **caught on 3 548 of
  10 000, 35.48 %**, far above D-061's line, as the 34.4 % measured at a thousand said it
  would be; decision time removed 10 of its catches and added none.
- The raft sweep's coverage on the correct server: **89 498 adoptions**, 2 529
  crash-mid-adoption faults and 5 038 crash-mid-install faults, 192 919 snapshots
  installed, 40 313 re-seeded servers, 8 007 re-seeds completed.
- D-049's re-seed episodes: 13 326 episodes, 12 956 completed, 12 259 answered from the
  store, and the adoption's own length in election-timeout windows, **median 1.559 and
  longest 48.551**, beside the stream's median 2.753 and longest 56.224. Those are re-seed
  episodes' adoptions (D-035's path), not the completed-install window this entry measures;
  they are the nearest thing the nightly prints to it.
- The membership scenario: **74 102 adoptions** over its own ten thousand, with a
  snapshot-fed joiner on every seed.

**What a green nightly cannot show, and does not.** It prints no count of
adoption-rescued gaps, so "in all nine thousand exactly one seed with an adoption-rescued
gap, 2605 — the same count the nightly's ten thousand give" above rests on run
35111624618's single failure and on the local runs recorded there, not on a counter in a
green run: a green run is silent about the gaps this arm removed, by construction. Nor does
it print the completion-to-restatement window, so that window's figures above (min
104.603 ms, p50 171.396, p99 256.661, max 353.091 over 0..3000) stay local measurements.
The instrument that would put either in a nightly is the first issue note below, which is
not code this entry writes.

**Consequences.** `up` now means exactly "the server has a live incarnation": one ends at
a shutdown, a crash, or a completed install, and begins at a `RaftTerm`. The check is
still a function of the trace alone. **Nothing now bounds the adoption itself**, which is
the cost: D-039's stated sensitivity — "an install that takes longer than the bound with
no completion in the window would still trip the check … the sweep should see an install
that slow" — is no longer carried by the timer rule, and belongs in a separate bound on
completion → restatement. Today such a bound would have to sit above 353.091 ms (seed 79,
server 3, 1.1712 × that server's bound) to pass, so it is a measurement and an entry of
its own, not a line added here. It is the first of the notes below, not code.

**Issue notes for the owner**, named here rather than written as code, so the fix stays one
arm; none is filed on GitHub by this commit, which pushes nothing:

- **The adoption has no bound of its own.** The instrument D-039's sensitivity wants, and
  also the only honest way to *pair* this exemption: a "never restates after an install"
  variant would be invisible to the timer rule exactly as a server that never restarts is,
  so the pair for a bound is the bound, not a variant. It needs its own entry and its own
  measurement of where the bound sits.
- **The check is more forgiving than the server on install chunks.** The replay's comment
  says an install keeps "its incarnation's timer … fresh", but the core pushes
  `Message::InstallSnapshot` to the snapshot task and `continue`s without stepping it, so a
  chunk does **not** reset the real follower's `election_elapsed`. D-030's arm therefore
  excuses a gap the real follower does have. Pre-existing, forgiving, and it dulls
  `ResetTimerOnAnyRpc` slightly.
- **The adoption's own cost**: ~297.7 ms on seed 2605, about 29.5 ms per staged file, of
  which D-060's format record is ~8.5 ms at the median. An engine-cost question, which
  moves this threshold without touching correctness.

## PROPOSED D-064 — The nightly runs as seven jobs: six shards of the sweeps, balanced by measured cost, and the rest

**Context.** The nightly is the only place ten thousand seeds run (D-040), and every stage of
Phase 3 closes on a green one on its branch (SHARD.md §12, the owner's addition of
2026-09-15). It was one job running the whole workspace's tests under a 300-minute limit. On
Stage A's tree it took 164 and 218 minutes green (runs 35161762372 and 35172923002, the same
tree 54 minutes apart, which is runner variance, D-061), 212 minutes to the failure of run
35111624618, and 161 minutes on `main` after the merge (run 35323559664 on fc75f68); Phase 2's
first nightly runs were cancelled at the limit (37e3bad, 78a3711). Stage B adds the node's
scenarios on top. Issue #57 asked for the fix before a stage's nightly times out, and the owner
asked for it before Stage B's first change, as its own change: shard the seeds across parallel
jobs, or split per sweep, whichever the measurement supports.

**Measured.** Every test of ananke-sim's integration binaries (`sim/tests/*.rs`) run alone, in
release, at `ANANKE_SEEDS=1000` and `ANANKE_DEEP_SEEDS=100` (the nightly's one to ten), on
fc75f68, on an Apple M2 with nothing else running — four performance and four efficiency
cores — its CPU time taken as user plus system from `/usr/bin/time -p`. Seventy-three tests, **7 450 CPU seconds** in all: the
engine binary 2 985, the raft binary 4 330, the WAL binary 122, echo 7, the determinism test 6.
The five heaviest: `an_install_in_two_switches_is_caught` 625 (8.4 % of the whole),
`the_live_install_crash_test_passes_every_seed` 582, `the_range_delete_crash_test_passes_every_seed`
418, the lease trial 356, the term-raise schedule's excuse 333. The other crates' tests are
under three seconds together at any tier.

A sweep's cost is its CPU: every sweep runs its seeds on rayon's global pool, one pool per
process (`sim/parallel.rs`), so a test binary's wall time is its tests' CPU over the cores it
has, and a job on a runner of its own has cores of its own. The unit is this machine's CPU
second, not a runner's: a test that keeps few cores busy spends a larger share of its time on
the M2's slower efficiency cores than one that fills all eight, so the weights rank the tests
and balance the shards roughly, not exactly, on a runner.

**Decision.**

- *Split per sweep, not per seed.* Each test runs whole in exactly one job, over all its seeds.
  Longest-first into the lightest shard puts the seventy-three tests into six shards of equal
  weight at a thousand seeds: no test is large enough to unbalance them, the heaviest being
  8.4 % of the whole and half of one shard.
- *Six shards and the rest.* `scripts/nightly-shards.txt` names each test's shard and records
  the weight it was placed by. `scripts/nightly.sh <1-6>` runs a shard, binary by binary, with
  `--exact` filters; `scripts/nightly.sh rest` runs every other test of the workspace, with
  `--skip --exact` for each name in the table, so the seven jobs together run each test once.
  The workflow runs them as a matrix of seven jobs at once, each with the seed counts as before,
  its own trace artifact, one shared build cache that one job saves, `fail-fast: false` so a
  failing shard does not cancel the others, and a limit of 150 minutes per job.
- *The table is held to the tests.* `scripts/check-nightly-shards.sh`, run by the gate and by
  CI after the tests are built, fails when a row names a shard outside one to six, a binary
  outside `sim/tests`, or a test that binary lacks; when a row appears twice; when a test of
  `sim/tests/*.rs` is in no row; and when a name in the table also names a test outside it,
  which `rest`'s `--skip` would then skip. A new sweep names its shard in the commit that adds
  it, and a renamed one cannot leave its shard running an empty filter.

**Why per sweep.** Sharding the seeds would change what every sweep's assertions mean. A
"caught on some seed" and a coverage counter above zero are asserted over the seeds one test
sees, at the tier its rate supports (D-061); a shard of 2 500 seeds is a different tier.
`SharedSnapshotDir`'s liveness catch is 2 of 10 000 on Stage A's tree and asserted only at the
nightly's count: split four ways, most shards see none, and the assertion would need a job that
gathers every shard's verdicts and asserts over their union. Splitting per sweep keeps every
assertion as it is and needs no such job, and the measurement shows it balances. Seed sharding
becomes the right tool only if one sweep alone outgrows a job, which none is near.

**Why six.** Scaling the old job's throughput: 74 500 CPU seconds, ten times the measurement,
finished in 164 to 218 minutes, so a sixth of it predicts 27 to 36 minutes a shard, plus a
cached build. That leaves room for Phase 3's later stages to triple the work before a shard
nears two hours. The prediction is checked by the first sharded run below, and the 150-minute
limit is set from it, not from the old job's 300.

**A shard may be bounded by one test rather than by its CPU.**
`the_correct_server_passes_every_seed` used 312 CPU seconds over 193 wall seconds on eight
cores, less than two cores' worth: a long tail of slow seeds, which no amount of cores
shortens. Its shard's time is the larger of its CPU over the runner's cores and that test's own
tail; the first sharded run measures which.

**Verified before the change.** The seven jobs run the workspace's tests once each: on this
tree `scripts/nightly.sh <shard> --list` over the seven names 319 tests, and sorted with their
repeats they are line for line the 319 that one `cargo test --workspace --all-features --release
--all-targets -- --list` names; run at twenty seeds, the seven jobs pass 319 tests between them
and fail none, as the single command does. The check was shown to fail on each thing it claims,
each planted in the table or the tree and then removed: a test in no shard, a row twice, a
renamed test's stale row, a shard outside one to six, a row of three fields, and a test in
another crate named as a sweep is (`crates/ananke-env/tests/`, which `rest` would have skipped).

**Measured on the branch.** Run 35411994368 on ecc30ef, the first sharded nightly: green, the
seven jobs passing **319 tests** between them and failing none, the single job's count. The
whole run took **39 minutes**, against 161 to 218 for the single job. Each shard's
`scripts/nightly.sh` step, which on this first run includes a cold release build, since the
cache's new key had nothing to restore:

| Shard | Its heaviest test | Minutes |
|---|---|---|
| 1 | `an_install_in_two_switches_is_caught` | 26.6 |
| 2 | `the_live_install_crash_test_passes_every_seed` | 36.9 |
| 3 | `the_range_delete_crash_test_passes_every_seed` | 32.4 |
| 4 | the lease trial | 36.1 |
| 5 | the term-raise schedule's excuse | 39.0 |
| 6 | `the_correct_server_passes_every_seed` | 37.5 |
| rest | — | 1.6 |

The prediction of 27 to 36 minutes a shard held, give or take the cold build. The shards are
not equal on a runner, as the unit above warns: the one heaviest in engine sweeps ran shortest
and the raft-heavy ones longest, so the raft sweeps cost a runner relatively more than they
cost the M2. The heaviest shard is 12 % above the mean. Shard 6's slow-tailed test did not
make it the longest: its tail is shorter than its shard's CPU on four cores. The deep-levels
sweep printed the figure Stage A's nightly printed, 11 609 rounds from level 2 or deeper, so
`ANANKE_DEEP_SEEDS` still reaches it.

**Review.** One adversarial review of ecc30ef found no blocker and eight minor points. Six are
fixed in the commit after it:

- a failing test no longer stops its job, so a night's log holds every sweep's verdict and
  rate — a shard runs every binary and fails at the end, and `rest` runs with
  `--no-fail-fast`;
- each shard checks, before running a binary, that the binary has every test its rows name,
  since a filter that matches nothing passes: a row pointing at the wrong binary would
  otherwise run its test nowhere on a green night, the check in the gate being no help to a
  nightly dispatched on a branch whose CI never ran;
- the check prints cargo's errors when it cannot list a binary's tests, instead of failing
  silently;
- a job that reaches its limit uploads its traces, since a timed-out job is cancelled, not
  failed;
- this entry states the measuring machine's cores and drops a balance figure more precise
  than its unit;
- CLAUDE.md and CONTRIBUTING.md name the check and the table.

The other two are hardening against changes not yet made, recorded as an issue rather than
built here: the check finds sweeps only in top-level `sim/tests/*.rs` files, so a test target
in a subdirectory or declared in `Cargo.toml` would pass it unweighed into `rest`; and
excluding tests from `rest` by name reserves the table's names across the workspace, where
excluding ananke-sim's integration targets from `rest` by target would not.

**Alternatives.**

- *Seed ranges per shard, with a job that gathers the verdicts.* Keeps any number of jobs
  balanced whatever one sweep costs, but changes every tier's meaning and adds a job whose
  failure mode is to assert over too few seeds. Not needed while every sweep fits a job.
- *One job per test binary.* Needs no table, but the raft binary alone is 58 % of the work: its
  job would take most of today's two hours and grow with every scenario Stage B adds to it.
- *A larger runner.* Costs money the project does not spend, and only postpones the limit.
- *Assigning tests by a hash of their name.* Needs no table either, but ignores cost: with
  eight tests of 300 to 600 CPU seconds among seventy-three, a hash puts two of them in one
  shard often enough to double its time.

**Consequences.**

- The nightly uses about the same runner minutes as before plus six cached builds; the
  repository is public, so the minutes are free.
- A failing shard names its seed in its own job's log and uploads its traces as
  `failing-traces-<shard>`.
- `scripts/premerge.sh` is unchanged: a thousand seeds still run as one process on the
  machine in front of you.
- The weights are a measurement of one tree. When a shard's time drifts above an hour, the
  table is re-measured by the same procedure and the shards re-balanced, in a change of its
  own.
- Issue #57 is closed by this entry.

## D-065 — A follower compacts its log to its own applied index, and takes a checkpoint only when one is asked for

**Context.** Stage B's first question before code (SHARD.md:2189-2193; §11, raft 13,
SHARD.md:1897-1901). Only a leader compacts: the take is asked from the leader's tick once its
log is `snapshot_threshold` entries past its last take (core.rs:1427-1441; 4 096 by default,
core.rs:381, and 12 in the sweeps, sim/raft.rs:3650), and `maybe_compact` returns at once on any
other role (core.rs:1834-1837). A follower's log shrinks only by truncation or when an install
replaces its store (SHARD.md:333-340). On a node most replicas are followers, so their
in-memory logs grow without bound while their ranges take writes. SHARD.md leaves open whether
a follower compacts "to its own applied checkpoint, or to one its leader names", and the answer
changes RAFT.md's rule that a leader compacts (RAFT.md:249).

**Three options, on the facts as they stand.**

- *A. Its own applied checkpoint.* Every replica takes a checkpoint when its log passes the
  threshold, and a follower compacts to its take at once. The Raft paper's rule: each server
  snapshots independently, covering only committed entries (Ongaro and Ousterhout, 2014, §7).
- *B. An index its leader names.* The leader sends each follower an index it has matched and
  applied; the follower drops its prefix to it. Needs a field on the wire.
- *C. Its own applied index, with a checkpoint taken only when one is asked for.* A follower
  whose log passes the threshold writes a snapshot record at its applied index and drops the
  prefix, with no checkpoint. It takes one only when it must stream a snapshot, as a leader
  already does when the record it holds has no complete checkpoint under it.

**Decided: C** — the owner's, of 2026-09-19, on PR #68, approved as written here. It needs no
new state and no new message:

- a snapshot record with no local checkpoint under it already exists, after an install and
  after a crash between D-036's record and its checkpoint (DECISIONS.md:1594-1597);
- a leader already asks for a take when it finds no complete version to stream
  (node.rs:2064-2071, "the record is an install's. Ask for a take"), and split-born ranges
  depend on that path (SHARD.md §11, raft 6);
- a follower's compaction is a record and a deletion of log keys at or below it, which
  `RaftStore::open` already performs (store.rs:825-836).

The record is written in the apply task at the applied index, as D-036 writes a take's, so its
last term and configuration are exact. The compaction keeps D-029's revert floor, the
configuration in force at the new prefix's end. A follower's applied index never passes its
commit index on the correct system, so nothing uncommitted is dropped. The leader keeps its
rules unchanged: its threshold take, its two-election-timeout hold-off, and D-037's condition
for compacting.

**Why not A.** A's cost is paid in the steady state, on every replica. On a node the factor
is the replicas it hosts over the replicas it leads, not three: with four ranges on three
nodes, a node that leads none goes from no takes to four per threshold. In the sweeps, where
the threshold is 12, every replica would take every twelve entries. A take holds the node's
turnstile, so no flush or compaction runs, and it holds every range's applies (D-036;
SHARD.md §11, storage 6). Stage B measures how long one take holds them, which cannot see how
often takes come. A follower's take would also stall the no-op of a range the node has just
started leading, which is what the leader's hold-off exists to avoid (core.rs:1427-1432). C
pays for a take only when a snapshot is actually streamed, which is when A's take would have
been read.

**Why not B.** The wire field buys nothing C lacks. A follower's own applied index is already
a safe compaction point, and a follower needs no leader's word for it.

**What C costs.** A follower that becomes leader must take on demand before it can feed a
follower behind its prefix. That take happens at a moment of change rather than in the steady
state; the path is the one a leader uses today after an install.

**Measured before asserted.** Stage B's exit asserts the largest in-memory log of any follower
replica, under `sim/raft.rs`'s client writes on the correct system, below a bound set from that
measurement and stated as a multiple of `snapshot_threshold` (Q39). Stage B also measures, for
whichever option is chosen, the share of time a node's apply task is held by takes, with and
without follower compaction, and takes it to the owner if it exceeds a heartbeat interval's
share.

**Re-measured in the commit that builds it.**
- Under `ApplyBeforeCommit` a follower's applied index passes its commit index
  (node.rs:2418-2429), so compacting to it drops uncommitted entries. The core then counts a
  request below the prefix as matched (core.rs:2423-2448), and the variant's catch may move.
- `TruncateOnEveryAppend` never truncates below the prefix (core.rs:1693), so its window
  shrinks on followers.
- D-029's revert floor, reached on 3 of 10 000 seeds today and deferred to issue #56, becomes
  a routine path on every follower.

**Departures from the stage plan, approved by the owner**, since the plan predates what Stage A
shipped.
- The plan has the entry supersede RAFT.md:249 with a forward pointer, as D-048 did
  (SHARD.md:2192-2193). This entry defers the pointer to the commit that builds follower
  compaction, since RAFT.md says what the code does (D-053).
- The plan builds follower compaction inside the node (SHARD.md:2233-2234), not in a commit of
  its own. Under the owner's rule of one change per PR, it is proposed here as its own PR,
  after the node, with its own re-audit.

**What the owner weighed.** The first draft of this entry proposed A. Its cost, a checkpoint
per hosted replica, each holding every range's applies, is what the review caught and what
decided the matter.

## D-066 — Every install on the node is a live install; a range lives in two key intervals, which Stage A's primitives do not reach in one step

**Context.** Stage B's second question before code (SHARD.md:2194-2198). Today an install stages
a whole store, retires the server's run-loop incarnation, and is adopted at the next start
before the engine opens (RAFT.md:225-247; D-041). On a node with one engine, ending the run
loop and reopening the engine "would restart every range on it" (SHARD.md:1799). Stage A built
the live install of a span, with its crash test (PROPOSED D-054; Q2). The question asks:
- which installs go through the live install;
- whether the staged whole-store adoption survives on the node;
- what `RaftAdopted` then records (§8 keeps it per node, SHARD.md:1131-1136);
- which node code path each install-path Phase 2 variant breaks.

The stage plan proposes answers in its builds (SHARD.md:2236-2242) and its variant mapping
(SHARD.md:2374-2400).

**A range lives in two key intervals, and Stage A's primitives take one.** A range's Raft state
is the interval `0 / <range>` (store.rs:115) and its user keys lie in tenant 2 (store.rs:35;
apply.rs:26). SHARD.md already says a range's snapshot "needs the span's user keys and that
range's Raft keys at one version" (SHARD.md:1790-1791).
- `checkpoint_span` takes one interval (engine.rs:1404).
- `install_span` takes one interval and refuses any source key outside it (engine.rs:1608,
  1639-1643; D-054's `OutsideSpan`).

So neither a range's take nor its install can be done in one step with what Stage A built, and
the repair cannot be carried in the switch that installs the user keys. It is a question for
the owner before the node's code:

- *(a) Extend the primitives to a set of disjoint intervals.* `checkpoint_span` over several
  intervals at one version; `install_span` removing several intervals' keys and adding their
  tables, and the receiver's repair writes, in one manifest switch. D-054's crash test is
  generalised to two intervals, with a variant that switches them one at a time. This lands
  as its own PR before the node. The switch stays the one commit point, and a crash leaves
  the range as it was or as installed with its repair, never a mixture.
- *(b) Two single-interval steps bracketed by a durable marker.* The marker "installing at
  index I" is written synced into the range's Raft state first. Then the user span is
  installed, then the Raft state and repair, then the marker is cleared. A restart that finds
  the marker treats the range's user span as untrusted and asks for the snapshot again.
  Either order without the marker is unsafe: new Raft state over old user keys misstates what
  is applied, and old Raft state over new user keys replays applied commands onto them.

**Decided: (a)** — the owner's, of 2026-09-19, on PR #68. The primitives are extended to
several intervals in one switch, in a PR of their own with the generalised crash test and its
variant, before any node code. Not (b), in the owner's words: it "puts a durable marker between
two steps that must be one commit point, which is the same shape as the D-041 adoption bug — a
point of no return before the new state is durable." The adoption as first built had that shape
and paid for it (D-041).

**Decided, with (a):**

- *Every install on the node is a live install.* This covers a follower behind its leader's
  compacted prefix, and each range of Q15's re-seed into the fresh engine. The whole-store
  staged install adopted at the next start is not kept for a replica's install, and nothing
  on the node reopens the engine to install a range.
- *The range is held from its repair's capture to the switch.* Today `install_decision`
  closes the apply queue and drops every other event before capturing term, vote and tail
  (node.rs:1283-1286). On the node, the range's core must take no input and no tick from the
  capture to the switch. Otherwise a vote or append persisted in between would be erased by
  the install, which removes every write of the span below its number (D-054), or would
  survive above it and diverge from the core. After the switch the range's core restarts from
  the installed store and traces its restatement.
- *The repair is the whole of today's.* It carries the receiver's term and vote, applied
  index, snapshot record, kept log tail, configuration key, quarantine flag and incarnation,
  and tombstones for the leader's log keys the tail does not replace (RAFT.md:225-233; D-030,
  D-035, D-038, D-042).
- *The shared apply task's hold during an install is measured* beside a take's (Stage B's
  measurements), since the install quiesces applies for its flush and switch.
- *`RaftAdopted` records only a node taking a fresh directory as its store after a whole-node
  refusal (Q15).* It is traced when the fresh engine is opened and before any range installs
  into it, and never for a replica's install. Its readers are re-keyed in the node's commit,
  each named:
  - the coverage assertions that every install is adopted (sim/tests/raft.rs:3354-3359,
    3694-3696) count `RangeCreated { cause: snapshot }` and installs per replica instead;
  - `adoption_windows` and `restarts_after_lost_state_refusal` (sim/raft.rs:2513, 2559) move
    to the fresh-directory switch;
  - D-063's timer exemption, which takes a server out of the replay's running set at a
    completed install (sim/raft.rs:1665), does not apply to a live install, which ends no
    incarnation. It is re-keyed to the range's hold above, the one stretch in which a
    replica's core has no timer to fire.
- *At start a node opens the newest directory not marked lost.* A refused directory is marked
  lost before the fresh one is created. A crash between the two leaves the node to create the
  fresh one at its restart, and never to reopen the refused one.

**The install-path variants, each on the node path it breaks.**

- `SnapshotWithoutCurrentLast` makes the switch before the repair is carried or made durable.
  State machine safety after `Fault::CrashInstalling` catches it, as today; its catch depends
  on the answer to (a) or (b).
- `RefusalNotDurable` keeps the whole node's refusal in the process alone, and lets the
  refused engine flush (RAFT.md:753). Its catch lives in the window before the fresh directory
  exists: a crash there, under the variant, restarts the node on the refused directory,
  unmarked, which the start's rule then opens. `Fault::CrashRefused` is aimed at that window.
- `IgnoreIncarnation` and `SharedSnapshotDir` break the stream's and the leader's progress
  rules, which the node keeps per range, with the path unchanged.
- `RefusedCountsForQuorum` and `RefusedNeverCounts` (D-049) read the progress of a refused
  follower's re-seed stream. With whole-node refusal, four streams head for one node under
  Q14's receive cap. If `sim/quorum.rs` sets that cap below its ranges, a range whose stream
  waits shows no progress, and its correct leader steps down where RAFT.md:754-755 expects it to
  keep office. The quorum scenario's cap is set at or above its ranges, or D-049's rule counts
  a queued stream as progress. That is the owner's choice when the node is built, noted here.
- `AdoptionAsBuilt` loses one of its three rules on the node; the owner's choice is below.

**`AdoptionAsBuilt`.** It breaks three rules today (RAFT.md:750; D-041).
1. *Copy and switch before delete.* On the node this is the live install's single switch. The
   variant breaks it by removing the span's keys in a switch of their own before adding the
   tables, which is Stage A's engine variant `InstallInTwoSwitches` (D-054) reached through the
   node.
2. *A damaged staging `CURRENT` refused.* This moves into the engine: `open_span_source`
   refuses a damaged source (`SourceDamaged`, D-054). `Fault::CrashAdopting` has no path on
   the node.
3. *A marked store never opens fresh.* This becomes the start's rule above; the variant
   neither checks nor writes the mark.

§10 requires every Phase 2 variant re-asserted. *Decided* — the owner's, the stricter reading,
as the plan recommended: `AdoptionAsBuilt` is re-asserted on rules 1 and 3, under crash arms
aimed at the live install's switch and at the window before the fresh directory exists, at its
Phase 2 tier. §10 is not amended.

**Dependencies.** This entry builds on D-054 and D-055, which are still PROPOSED
(DECISIONS.md:4157, 4583), and (a) extends their primitives; the multi-interval PR carries that
extension and its own entry. The quorum scenario's receive cap is settled when the node is
built, as noted above.

**Merge order**, the owner's: this entry's PR, then the multi-interval primitives, then Stage
B's node, one PR each.

**Alternatives.**
- *Keep the staged whole-store install for the re-seed.* A re-seed replaces every range, so a
  node could stage them all and adopt once. But the fresh directory's ranges arrive one stream
  at a time under a receive cap (Q15; Stage B's shape caps two of four), and waiting for all
  of them keeps every range down until the slowest stream ends.
- *Give the fresh-directory switch an event of its own and retire `RaftAdopted` on the node.*
  Clearer for new readers, but §8 keeps `RaftAdopted` per node, and the readers are re-keyed
  either way.

**Consequences.** `snapshot.rs`'s staged adoption stays in `ananke-raft` for the single-group
server until nothing uses it; the node does not call it.

## D-067 — The re-seed shape's variant: `ReseedMarkNotSynced`, the replica's refused mark written unsynced

**Context.** Stage B's third question before code (SHARD.md:2199-2202). Q15 refuses a whole node
whose shared engine lost state. The node re-seeds into a fresh engine in a new directory, and
writes a durable per-replica refused mark into it before that replica serves
(SHARD.md:1818-1824). The mark is D-041's rule, applied per replica: a replica whose state was
lost never opens fresh. The reason is D-035's: opening fresh, with no term and no vote, would
let it vote in a term it may already have voted in before the loss. Once its re-seed completes,
the quarantine flag carried in the repair keeps the rebuilt replica from voting (D-035). §10
names no variant for this path, and CLAUDE.md's pair rule asks for a known-buggy one beside the
directed re-seed shape Stage B adds (SHARD.md:2253-2262).

**Decided** — the owner's, of 2026-09-19, on PR #68, approved as written here, the crash before
any other sync and the check that catches a lost mark every time included. `ReseedMarkNotSynced`
writes each replica's refused mark in a batch that is not
synced. Otherwise it behaves as the correct node: it traces the mark as the correct node does,
and answers as the correct node would once the mark is written.

*The crash must come before anything else syncs the new engine's log.* An unsynced write becomes
durable at the next sync from any writer on the same log (D-024; the simulated disk syncs a
whole file, fs.rs:317-330). In the re-seed shape several things sync:
- installs write synced records (engine.rs:1650);
- re-seeded ranges persist;
- `RaftStore::open` writes a synced incarnation (store.rs:813-823).

So the `reseed-crash` arm crashes the node on the mark's own trace event, before any later sync
of the new engine's log. This is the precondition `RemovalNotDurable`'s shape states
(SHARD.md:1709-1716). The shape asserts per seed, as coverage, that the mark was still unsynced
at the crash; otherwise the catch rate would say nothing about the variant.

*What catches it.*
- The shape's trace-order check (c) cannot (SHARD.md:2280-2291), since the variant traces what
  the correct node traces.
- What a crash keeps of an unsynced write is the disk's draw, so on some seeds the mark is gone
  at the restart. The replica then has no state and no mark, so `RaftStore::open` opens it
  fresh: term 0, no vote, a new incarnation (store.rs:792-823). It is later fed a snapshot as
  an ordinary lagging follower, its quarantine tombstoned by the repair (RAFT.md:229), and it
  votes from then on.
- Check (d) catches it: the replica whose mark was written must be restarted refused and
  re-seeded. For (d) to read that, the restart must trace it per replica. §8 keeps
  `RaftRefused` per node, so `RaftRecovered`, per replica (§8), gains the replica's state at
  its restatement: refused, quarantined, or neither.
- A second check catches the loss whenever it happens, not only when a vote follows: every
  replica of a node refused whole restates as refused or quarantined, never neither, until the
  node is re-seeded. Election safety fails only when the unprotected replica actually grants a
  vote in a term it voted in before the loss, which is rare. The check is the one that matters.

*Its standard is rate*, as §10 sets for `RemovalNotDurable` (SHARD.md:1748). The arm's firing,
and the mark still unsynced at the crash, are asserted on every seed at every tier. The catch
is asserted at the tier its measured rate supports under D-061's rule, the rate measured
before the assertion is written; the owner added that rule for new variants to D-061 rather
than to this entry. Were it caught on no seed at any tier, it would need a directed scenario,
not a lower bar.

*Where it lives.* In the range layer's variant set in `ananke-shard` (SHARD.md:1557-1561),
outside §10's count of range-layer variants, as the plan says.

**Why it matters.** The first draft's catch could have been near zero, since any later sync
makes an unsynced mark durable. That is the failure the pair rule exists to prevent: a variant
the scenario cannot catch proves nothing about the correct node beside it.

**Alternatives.**
- *A variant that writes no mark at all.* Caught on every seed by (c) and (d), so it tests the
  checks more than the crash. It cannot tell a durable mark from a written one, which is the
  property the path depends on.
- *A variant that writes the mark after the replica's first answer.* Caught by (c) on every
  seed, since the order is in the trace. It pairs the ordering rule, not durability, and the
  shape asserts the order anyway.

---

## PROPOSED D-068 — The span primitives over a set of spans: one version, one switch, the repair a table in it

**Context.** D-066 decided option (a). A range lives in two key intervals, its Raft state
under `0 / <range>` and its user keys in tenant 2, and each of Stage A's primitives took one:
`checkpoint_span`, `install_span` and `delete_range` (engine.rs; D-054, D-055). The owner
asked for them extended to a set of disjoint intervals, in a PR of its own before any node
code, with the generalised crash test and its variants. What the extension must guarantee is
settled there:
- the checkpoint copies every interval at one version;
- the install removes every interval's keys, adds the source's tables, and carries the
  receiver's repair in the same manifest switch, numbered above the live engine as D-054
  numbers it. The repair is the whole of today's: term and vote, applied index, snapshot
  record, kept log tail, configuration key, quarantine flag, incarnation, and tombstones
  (D-066);
- the range delete is the install of nothing over the set, in one switch (D-055);
- a crash leaves every interval as it was, or every interval installed with its repair,
  never a mixture across intervals or between the tables and the repair.

The owner's brief for this PR adds that the repair is a table of its own, numbered above the
source's. It left open the API's shape, how the repair is numbered, the refusals, and the
variants' shapes. Each is proposed here, taking the most conservative option where there was
a choice. "Span" below means one interval, as D-054 uses the word. Every site is marked
`PROPOSED(D-068)`.

**Decision.**

*The shape.* Each primitive gains a form over a set, and the one-span form becomes a thin
wrapper over it, so every existing caller is unchanged: twenty-one call sites in the storage
crate's tests, and none outside the tests and the sweep, whose three now call the set forms.
- `Engine::checkpoint_spans(&[Range<&[u8]>], dir)`; `checkpoint_span(range, dir)` is it over
  one span.
- `Engine::install_spans(Vec<Range<Bytes>>, SpanSource, WriteBatch)`, the batch being the
  repair; `install_span(range, source)` is it over one span with an empty repair.
- `Engine::delete_ranges(Vec<Range<Bytes>>)`; `delete_range(range)` is it over one span.
- `SpanInstall::repair_seq()` and `InstallInfo::repair_seq` report the repair's number.

The wrappers were chosen over one-element sets at every call site because a one-span install
with no repair is exactly D-054's, byte for byte in what it writes and traces, and the diff
then shows that no caller's meaning moved. The node will call the set forms.

*A set is sorted and disjoint.* Each span must end at or before the next begins; adjacent
spans are allowed. An install or a delete is refused with `EmptySpan` when the set is empty or
any span in it holds no key, and with the new `SpansOverlap { end, start }` when one span ends
past the next one's start, which covers spans out of order too. A checkpoint drops spans that
hold no key, as a single empty span always copied nothing, and refuses the rest with the same
`SpansOverlap` when they are out of order or overlap, before anything is written. Unrefused,
spans out of order would feed the table writer keys out of order, which it asserts against.

*One version.* `checkpoint_spans` reads the newest version applied once, under the turnstile,
before the first span's copy. Each span then walks a merge of the memtables and tables in
service and keeps the newest write at or below that version. The turnstile holds off every
flush, compaction and install, so every write at or below the version stays where the walks
find it, and writes applied during the copy are above it. Tables are sealed near `sst_bytes`
in key order across the spans, so one table can hold keys of two. `CheckpointInfo::version`
and the manifest's `flushed_seq` are that one version.

*The repair's number.* A repair that is not empty takes the log record after the install's:
`S` for the install and `R = S + 1` for the repair, both holding no write. They are appended
under one hold of the pending lock, so no other write is numbered between them (D-021's
lock, which D-054 made the numbering's). The install's task waits for both to be durable.
Every installed write carries `S` and every repair write `R`, so a repair write of a key is
read over the source's write of it, and every write taken after the call is above both. An
empty repair takes no number and writes no table, so a single-span install without one is
D-054's.
- Why not `S` for both, the source's writes of the repair's keys dropped: that is one fewer
  record, but the repair would then be spliced into the installed tables rather than being a
  table of its own. Without the splice, two writes under one internal key would stop the
  table writer, as `InstallKeepsSourceNumbers` once did (D-054).
- Why under one lock: were another write `W` numbered between `S` and `R`, the switch's
  `flushed_seq` of at least `R` would claim `W` was in a table while it sat in a memtable
  the flusher held back. The log segments through `R` would then be deleted and `W` lost at
  the next crash, acknowledged.
- *Argued, not tested*, as D-054 says of its own two windows. In the simulator no await sits
  between the two numbers and no other task runs there, so no test can put a write between
  them; on the real runtime a test would be a timing race that passes on broken code as
  readily as on this. So `begin_install` checks, right after the numbering and before the
  install is marked in progress, that `R` is exactly `S + 1`, and otherwise refuses the
  install with the new `RepairNotNext { seq, repair_seq }`. A change that let a write in
  between would then refuse every install carrying a repair on the real runtime, loudly,
  rather than switch to a manifest that loses `W`. What the review's mutation of it showed is
  under *The review*, below.

*The repair is a table in the switch.* The repair's last write per key is written at `R`
into one level-0 table, deletes kept as tombstones since a delete must hide the source's
write of its key. The install's own manifest lists that table beside the rewrites and the
installed tables; its `flushed_seq` is at least `R`, and once it is switched to, the log
segments through `R` are deleted. A crash before the switch leaves the spans as they were and
the repair's table an orphan the next open removes; after it, every span is installed and the
repair is there. The repair is one table whatever its size. The kept log tail can make it
large, and sealing it near `sst_bytes` is left until a measured repair needs it.

*Taking the spans out.* A table in service that meets any span is taken out whole only when
one span holds both its first and its last key and every write it holds is below `S`: then
every key it holds lies in that span. A table whose two ends lie in two spans can hold keys
between them, so it is walked and written again without the spans' writes below `S`, as a
table straddling one span's edge already was.

*The refusals.* In the order they are checked, before anything is numbered:
1. `EmptySpan`.
2. `SpansOverlap`.
3. `Quiesced`.
4. `OutsideSpan` for a source table with its first or last key outside every span; the
   refusal still carries the source's smallest and largest key, as D-054's does.
5. The new `RepairOutsideSpan { key }` for a repair write outside every span. The install
   removes and replaces the spans' keys alone, so such a write would be one the log never
   numbered, over a key the install was not asked to touch.
6. `InProgress`.

Right after the numbering, `RepairNotNext` when the repair's number is not the install's plus
one, which the one hold of the lock rules out (above). The install is not yet marked in
progress, and both records hold no write. Later, a source key between two spans, inside a
table whose ends lie in both, refuses the install as `OutsideSpan` key by key, as D-054's
second check does. The tables written so far are then orphans, and the two records hold no
write.

*What a snapshot sees.* As D-054, over every span. On a runtime with more than one thread a
snapshot can be taken with its version at `S`, between the applies of `S` and `R`. Once the
switch is made, such a snapshot reads the installed spans without the repair. Stage B holds
no snapshot across an install of the range it reads (D-054), so the node never meets it. In
the simulator the two records are synced in one group and no task runs between their applies.

*The trace.* `TraceEvent::SpanInstalled` carries `spans`, the sorted list, in place of
`start` and `end`. It also carries `repair`: the repair's number, its table's number and its
first and last key, or nothing. The moirae line `ananke.engine.span-installed` carries them as
`spans` (objects of `start` and `end`) and `repair` (an object, or null). No other event
changed, and no scenario other than the engine's emits this one.

*The variants.* Each differs from the correct engine only in the property it breaks, and each
is caught by its primitive's crash test, the live install's (below).
- `Variant::InstallSwitchPerSpan` installs the spans one at a time, in order. Each is the
  install of that span alone at `S`, with the source's keys and the repair's writes in it at
  their numbers, taken out and put in against the tables the previous span's switch left,
  and traced and switched before the next span is begun. A crash between two switches leaves
  the earlier spans installed and the later as they were.
- `Variant::RepairAfterSwitch` makes the install's switch without the repair's table and
  lists that table with a second switch after it. A crash between the two leaves the
  installed tables without their repair. The brief described the repair as written "in a
  batch after" the switch; the variant writes it with a switch of its own, not through the
  log. A log record written after the switch would take a number that neither the caller nor
  the sweep's model was told of when the install was asked for, one past whatever the writers
  had taken meanwhile. The model numbers every record as it is appended, so every write after
  it would be off by one. The sweep would then catch the variant on every install by the
  miscount, before any crash, which says nothing about the window. As a second switch, every
  number is where the correct engine puts it, and the only difference is the window.
- `Variant::RepairBeforeSwitch`, added by the review (below), is that pair the other way
  round: a switch of its own lists the repair's table first, and the install's switch takes
  the spans' keys out and puts the installed tables in after it. A crash between the two
  leaves the repair without its tables: the receiver's writes over the spans as they were.
  The replacement is traced once the repair's manifest is written and before `CURRENT` names
  it, and names the install's own manifest, the next one; the turnstile holds every other
  manifest off until then. Traced before the repair's manifest is written, it would name a
  manifest above one written after it, which the sweep's mirror takes for a lineage a
  fallback abandoned.
- `Variant::CheckpointVersionPerSpan` reads the version again before each span's copy and
  names the last. A write applied between two spans' copies is then in the later span's copy
  and not in the earlier's.

*The crash test.* `sim/engine.rs`'s installer task now draws two spans, or three one time in
three, and every install and range delete covers them all. The key space is cut into as many
equal slices, with a span of one to eight keys in each. Half the sources are
`checkpoint_spans` over the set, and half a store further along holding most of the spans'
keys, as before. Every install carries a repair of one to four writes to keys of the spans,
one in three a delete. This applies to `Schedule::install()`, `Schedule::range_delete()` and
the default schedule, whose installer is the same task; `phase_1()` and `seek()` run none of
it. The model folds the repair at `R` over the installed spans when the install is made. The
mirror gives the repair's table its writes at `R`, and compaction's rule reads a repair's
deletes from the repair rather than from the model's ops, where `R` holds none. On top of
every check the oracle had, each recovery now judges, per install:
- *The spans agree*, two ways. By the trace: a span is in force when a replacement in force
  switched it, and one in force beside one not is a mixture. By what the tables in service
  and the log's replay hold: a write the install put in one span at `S` beside a write older
  than the install in another is a mixture too. A repair write at `R` does not count as
  installed there, so a repair without its tables is named by the repair's own check below,
  not as a mixture (the review, below). The second does not rest on how many replacements
  the engine traced.
- *The repair is present exactly when the installed tables are.* With the install in force,
  every repair write must be in a table in service or lost to what a fault or a compaction
  explains. Without it, no table in service may hold one.
- *The repair's table carries `R`* and no other number, from the recovered manifest's record
  of it, beside D-054's check that the installed tables carry `S`.

These are judged before the per-write account, so a mixture is named as one. What the oracle
checked before stays: keys outside every span as they were, bar what a fault explains; a
write after the install read over the installed version; the installed numbers above the
live engine's. The crash windows the live install's and the range delete's tests assert stay
as they were. Three counters are added and asserted above zero:
- installs over several spans judged after a crash, counted when two of the spans or more held
  a write to judge by, the source's at `S` or one older than the install;
- installs in force after a crash whose repair was found;
- installs left out by a crash between their replacement and their switch, the window in which
  a repair could stand without its tables, whose repair was found absent.

Each test now prints, beside every window's count of events, the number of seeds that saw one,
which is what D-061's rule reads.

**Measured before the assertions were written.** In release on the eight-core laptop,
`ANANKE_SEEDS` at each tier, the engine binary run whole. Every
correct test of the binary passes every seed at 20, 100 and 1 000. The four variants on the
live install's schedule, `RepairBeforeSwitch` the review's, each rate over the seeds its test
sees. All four run on the high-rate share, so the gate and CI see twenty seeds of each, the
premerge a hundred and the nightly a thousand; the columns are the variant on 20, 100 and
1 000 seeds. P(none) is D-061's, (1 − p)^20 with p the thousand's rate, since the gate's share
is twenty:

| Variant | Seeds it runs | 20 | 100 | 1 000 | Caught by its own check | Tier asserted | P(none) at the gate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `InstallSwitchPerSpan` | the share: 20, 20, 100, 1 000 | 18 | 90 | 922 | every catch, the spans' agreement | every | ~0 |
| `RepairAfterSwitch` | the share | 16 | 61 | 616 | every catch, the repair's check | every | 4.9 × 10^-9 |
| `RepairBeforeSwitch` | the share | 14 | 52 | 529 | every catch, the repair's check | every | 2.9 × 10^-7 |
| `CheckpointVersionPerSpan` | the share | 9 | 47 | 468 | 4, 25 and 324, the span checkpoint's check | every | 3.3 × 10^-6; 4.0 × 10^-4 by its check |

Every rate is well above 5 %, so each catch, and each catch by the variant's own check, is
asserted from the gate's twenty (D-061, and its rule for new variants). All four run on
`high_rate_share()`: `InstallSwitchPerSpan`, caught on more than four seeds in five, by D-055's
rule for its high-rate variants; the other three, below four in five, by a choice this entry
puts to the owner (*For the owner*, below). Each test asserts that some catch is its variant's
own check, by its words, so the property is what catches it and not merely a key that reads
wrong afterwards:
- `InstallSwitchPerSpan`'s first catch, at seed 0: *the install at record 114 over k13..k20,
  k43..k46 is in force for k13..k20 and not for k43..k46 (manifest 12): a mixture across its
  spans*.
- `RepairAfterSwitch`'s, at seed 1: *the install at record 204 is in force (manifest 17) but
  its repair's write of k26 at record 205 is in no table in service and no fault explains it:
  the installed tables without their repair*.
- `RepairBeforeSwitch`'s, at seed 1: *the install at record 204 is not in force (manifest 17)
  but a table in service holds its repair's write of k26 at record 205: a repair without its
  tables*.
- `CheckpointVersionPerSpan`'s, at seed 2: *checkpoint /stage/0001 at version 260: key k02
  holds Some(…) but the model has None*. Its other catches are installs from such a
  checkpoint, whose installed values no one version held. Of the first hundred seeds' 47,
  8 are an installed write the model expects and no table holds, and 14 are a key or a scan
  read wrong, live or after the crash; a probe printed them and was deleted.

`InstallSwitchPerSpan` traces a replacement per span, so the agreement by the trace could be
thought to rest on the variant saying what it did. It does not: a probe copy of the tree with
that half of the check taken out still caught the variant on 89 of the first hundred seeds,
76 of them by the agreement by what the tables hold. The first such catch, at seed 0: *left
k13..k20 as installed (its write of k13 at record 114) and k43..k46 as it was (the write of
k43 at record 79)*. The probe was deleted and never committed. Run again once the review had
stopped the agreement by the tables counting a repair write as installed, in a throwaway copy,
the same probe still catches 89 of the hundred, 70 of them by the tables. In the other six the
only write that read as installed was a repair's at `R`, which is now the repair's check's to
judge.

The correct engine's crash windows, each as events and, in brackets, the seeds that saw one
(the rate D-061's rule reads):

| Test | Tier | Aimed | Between replacement and switch | After the switch | Keys written after | Several spans judged | Repair found | Repair found absent |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| live install | 20 | 141 (20) | 15 (11) | 16 (10) | 4 736 (20) | 123 (20) | 154 (20) | 15 (11) |
| live install | 100 | 724 (100) | 59 (47) | 55 (41) | 24 379 (100) | 639 (100) | 796 (100) | 59 (47) |
| live install | 1 000 | 7 177 (1 000) | 604 (459) | 477 (392) | 229 108 (984) | 6 202 (983) | 7 806 (984) | 604 (459) |
| range delete | 20 | 151 (20) | 12 (9) | 12 (10) | 7 839 (20) | 71 (19) | — | — |
| range delete | 100 | 706 (100) | 55 (44) | 38 (33) | 36 643 (96) | 337 (91) | — | — |
| range delete | 1 000 | 7 425 (1 000) | 621 (468) | 487 (396) | 388 264 (983) | 3 599 (965) | — | — |

"Several spans judged" and "Repair found absent" are counted as the review made them count
(*The review*, below). As first built they counted every install over several spans judged,
245 (20), 1 275 (100) and 12 697 (985), and every install left out by a crash, 91 (19),
479 (98) and 4 891 (970); and every delete judged, 378 (20), 1 825 (96) and 19 280 (984).
Every window the two tests assert is seen on a third of the seeds or more at every tier. The
thinnest is the range delete's crash after its switch, on 10 of 20, 33 of 100 and 396 of
1 000 seeds. So each assertion, the three new ones included, is made from the gate's twenty.

**What moved, and the re-audit.** Measured on the branch's base, 4690d00, and on this tree,
the same way: `moirae_trace::trace_hash` of each run's moirae trace as written, header
included, from a probe test file in `sim/tests` that was deleted and never committed.
Echo's golden is the trace without its header, as `sim/tests/echo.rs` takes it.

| Run | Base 4690d00 | This tree | |
| --- | --- | --- | --- |
| engine, default schedule, seed 42 | `7112afc050205615` | `edb96df998f44632` | moved |
| engine, `install()`, seed 42 | `e26fae961366b062` | `bef5286e53f6eef8` | moved |
| engine, `range_delete()`, seed 42 | `338df31d8136bf5e` | `aa28d75149ab3206` | moved |
| engine, `seek()`, seed 42 | `15d9190a66b34ec4` | `15d9190a66b34ec4` | not moved |
| engine, `phase_1()`, seed 420 (pinned) | `62953c18b447290a` | `62953c18b447290a` | not moved |
| engine, seed 44's schedule, fallback allowed (pinned) | `dd5cb91cd46d6cc1` | `dd5cb91cd46d6cc1` | not moved |
| engine, seed 44's schedule, fallback refused (pinned) | `e47f2a5d152d8a7c` | `e47f2a5d152d8a7c` | not moved |
| engine, `seek()`, seed 3123 (pinned, D-062) | `8436a85fea896e33` | `8436a85fea896e33` | not moved |
| engine, `seek()`, seed 3123, `TrustsAStaleSegment` | `19c196ed7efc928f` | `19c196ed7efc928f` | not moved |
| WAL, seed 42 | `4cd95dedc1202acc` | `4cd95dedc1202acc` | not moved |
| echo's golden, seed 42 | `fcbe82ee7a0ba672` | `fcbe82ee7a0ba672` | not moved |
| raft, seed 42 | `ab289ace35a8415f` | `ab289ace35a8415f` | not moved |
| membership, seed 42 | `9c88d575d39acd76` | `9c88d575d39acd76` | not moved |
| quorum, seed 42, stream blocked / open | `0d9167f75474e730` / `d8d63ed0109360ec` | the same | not moved |

What moved is exactly what runs the installer task: the default, install and range-delete
schedules of `sim/engine.rs`. Their spans, repairs and extra log record move every draw
after the first install. The raft, membership and quorum sweeps call none of these
primitives, and their seed-42 hashes are the ones D-060 recorded. `ananke-env`'s change is
confined to the `SpanInstalled` event and its export, which only the engine emits.

The re-audit, seed by seed (CLAUDE.md's pinned-seed rule):
- *Seed 420* runs `phase_1()`, which runs no install. Its trace hashes as on the base, so its
  schedule did not move, and its pin is as D-055 left it.
- *Seed 44*, in both its modes, runs `phase_1()` with a larger level 1. Both traces hash as
  on the base; its assertions of every fallback's manifest and of the refusal still bite on
  the run they were written for.
- *Seed 3123* runs `seek()`, which runs no install. Its trace and its `TrustsAStaleSegment`
  run hash as on the base, so D-062's supersede `(15, 173, 165, 7)` and the violation *record
  166 came back changed* are asserted on the same run as before.
- *Seed 42 of the default schedule* is the studio trace, not a pin for a mechanism; its test
  asserted only green. Its schedule moved. It now also asserts that the trace shows an
  install over two spans or more, carrying a repair, switched to the manifest it names, so
  the day the schedule stops reaching one the test says so. The hashes of seed 42 in
  `sim/tests/parallel.rs` compare a run with itself, and hold.
- No test pins a seed of the install or range-delete schedules, so nothing else moved.

The engine sweep's other rates, before this entry (the base) and after, at 20, 100 and 1 000:

| Test | Base | This tree |
| --- | --- | --- |
| `NoWalBeforeMemtable`, default schedule | 20, 99, 985 | 20, 98, 977 |
| `ReleaseBeforeManifest`, default | 10, 51, 602 | 8, 58, 599 |
| `DeleteBeforeManifest`, default | 12, 67, 647 | 11, 59, 655 |
| `InstallInTwoSwitches`, install | 13, 54, 559 | 13, 58, 580 |
| `InstallKeepsSourceNumbers`, install, on the share | 20 of 20, 98 of 100 | 20 of 20, 98 of 100 |
| `SpanCheckpointUnsynced`, install, on the share | 17 of 20, 81 of 100 | 15 of 20, 77 of 100 |
| `RangeDeleteSkipsMemtables`, range delete, on the share | 15 of 20, 89 of 100 | 20 of 20, 95 of 100 |
| `SeekCountsTombstones`, seek, on the share | 20 of 20, 98 of 100 | the same: its schedule did not move |

Every one stays well above 5 % and keeps its tier. One note for the owner:
`SpanCheckpointUnsynced` is now caught on 77 of the premerge's hundred-seed share. D-055 put
it on the share as a variant caught on four seeds in five or more, and it is now just below
that. Its assertion is unaffected, since a share of twenty sees none with probability
1.7 × 10^-13. Whether it should go back to every seed is the owner's call, and nothing here
changes it. The seek schedule's live seeks, 196 904 at a thousand seeds, are the base's to
the seek. Coverage of the default schedule at a thousand seeds: 70 728 tables written
against 66 992, 14 939 compactions against 14 815, 1 455 crashes inside a compaction against
1 327, 5 553 installs and 3 876 range deletes against 5 806 and 3 998, and 7 978 span
checkpoints against 8 130. The installs are fewer and larger: two or three spans of one to
eight keys, against one span of one to twelve.

**PROPOSED, for the owner: three of the variants on every seed, or on the high-rate share.**
3e66e86 moved `RepairAfterSwitch` and `CheckpointVersionPerSpan` onto `high_rate_share()`, and
the review built `RepairBeforeSwitch` there beside them. That step was not this entry's to take.
D-055 put on the share only the variants caught on four seeds in five or more, and none of the
three reaches that: 616, 529 and 468 of 1 000. D-061 sets the tier an assertion is made at, not
which variants run a share. 9478da3 had left the step to the owner; 3e66e86 took it as though
D-061 settled it. It is set out here as a choice. The code stays as built, on the share, until
the owner chooses.

Either way each catch, and each catch by the variant's own check, is asserted from the gate's
twenty, since every rate is far above D-061's 5 %, and the gate runs twenty seeds of each under
both. What differs is how many seeds CI, the premerge and the nightly give these three tests,
and what the premerge costs. SHARD.md §12 asks a stage to size its new tests' seed shares so the
premerge stays near the quarter of an hour D-040 set.

- *Every seed*, as D-055's rule reads. CI sees 100 seeds of each, the premerge 1 000 and the
  nightly 10 000. The premerge exceeds the quarter of an hour: with `RepairAfterSwitch` and
  `CheckpointVersionPerSpan` on every seed, the engine binary alone weighed 2 747 CPU seconds
  at a thousand seeds, against the base's 1 574 (f2c6581 and 4690d00, 9478da3). On every seed
  `RepairBeforeSwitch` weighs about 370 CPU seconds more at a thousand, against 37 on its share
  (weighed alone on this tree, idle).
- *The share, as built.* Twenty seeds at the gate and in CI, a hundred at the premerge, a
  thousand at the nightly. The engine binary's tests weighed 1 824 CPU seconds at a thousand
  seeds on 3e66e86, idle, and the premerge ran in 622 s there and in 613 s on the review's
  tree, ea44e38 (below). The cost is coverage.
  CI's hundred now runs the same twenty seeds as the gate for these three tests, so CI adds
  nothing to them that the gate did not see, and the premerge sees a hundred seeds of each
  rather than a thousand. The thinnest check is the checkpoint's own: its catches at CI fall
  from 25 to 4, and at the premerge from 324 to 25. `RepairAfterSwitch`'s fall from 61 to 16 at
  CI and from 616 to 61 at the premerge; `RepairBeforeSwitch`'s from 52 to 14, and from 529 to
  52.

At the gate, under either option, P(none) is by D-061's method, (1 − p)^20 with p the
thousand-seed rate: 4.9 × 10^-9 for `RepairAfterSwitch`, 2.9 × 10^-7 for `RepairBeforeSwitch`,
and for `CheckpointVersionPerSpan`'s own check (1 − 0.324)^20 ≈ 4.0 × 10^-4 (3.97 × 10^-4). The
gate's own 4 of 20 gives a more pessimistic reading, 0.8^20 ≈ 1.2 × 10^-2. A schedule move that
takes that check to none at the gate is a move of the kind D-061 describes, measured and moved
by its rule, never loosened in place.

`InstallSwitchPerSpan`, at 922 of 1 000, is on the share by D-055's own rule and is not part of
the choice.

**The premerge, on an otherwise idle machine.** Measured on the tree with the share, after a
warm release build, with the one-minute load sampled every ten seconds: `scripts/premerge.sh`
green at a thousand seeds in **622 s**, at a mean one-minute load of **11.72** over 62 samples.
Beside it: D-060's 593.87 s at load 20.87 (`ae54a20`) and D-063's 822.64 s at load 60.94
(`46e95c0`). The engine binary's tests, each run alone at a thousand seeds on the same tree and
machine, now total **1 824 CPU seconds** against the base's 1 574, the added 250 being the
heavier install and range-delete schedules, with two or three spans and a repair each time,
and the three new tests at 35 to 40 CPU seconds each on their share. The review's
`RepairBeforeSwitch` adds 37 more on its share.

On the review's tree, ea44e38, measured the same way, started once the one-minute load was
below 2.5: `scripts/premerge.sh` green at a thousand seeds in **613.38 s**, 327 tests passed
and none failed, at a mean one-minute load of **28.49** over 62 samples (2.42 to 68.87).
Nothing else ran on the machine, so that load is the premerge's own. Per binary: raft
343.93 s, engine 246.94 s, WAL 9.01 s, every other under two and a half seconds.

An earlier figure, 2 122.50 s at a mean load of 58.40, was taken on f2c6581, before the share
change, while the machine carried its own background work: the raft binary, which runs no code
this change touches, took 2.1 times its usual time in it. It is withdrawn in favour of the two
measurements above, which D-052's protocol asks for.

**The nightly's shards.** The four new tests are rows of `scripts/nightly-shards.txt` (D-064),
each weighed alone in release at `ANANKE_SEEDS=1000` and `ANANKE_DEEP_SEEDS=100` on the idle
machine, on the tree with the share, and placed in the lightest shard:

| Test | Seeds at a thousand | CPU s | Shard |
| --- | --- | --- | --- |
| `an_install_whose_repair_follows_its_switch_is_caught` | 100, its share | 39.8 | 3 |
| `an_install_that_switches_one_span_at_a_time_is_caught` | 100, its share | 38.6 | 4 |
| `a_checkpoint_that_copies_each_span_at_its_own_version_is_caught` | 100, its share | 34.6 | 2 |
| `an_install_whose_repair_precedes_its_switch_is_caught` (the review's) | 100, its share | 36.8 | 1 |

The last is the mean of two runs, 38.0 and 35.5, on the review's tree; shard 1 and shard 6 were
the lightest, level at 1 241.7, and it went to the first. The shards now weigh 1 241.7 to
1 281.3 CPU seconds, within 3 % of one another. The older rows keep D-064's weights, though
this change makes the install and range-delete crash tests heavier (their idle weights on this
tree, 346.8 and 223.7 CPU seconds, are measured in a different condition from D-064's and are
not mixed into the table). The next nightly measures what that
adds; if a shard passes an hour, D-064's procedure re-balances the table in a change of its own.

**The review.** One adversarial review of this PR found no engine bug. It found gaps in the
PR's own tests and record, each fixed in the PR:

- *The repair's check was shadowed.* The agreement by what the tables hold counted a repair
  write at `R` as installed. A span holding only a repair write, beside a span holding its own
  old writes, then read as "left … as installed", and the agreement named the catch first. So
  the check that names a repair without its tables caught almost nothing, and the review's
  mutation O3, which makes that check accept one, passed every test at 20 and 100 seeds. The
  agreement now counts only the source's writes at `S`. The pair rule for that direction had
  no variant; it has `RepairBeforeSwitch` now, measured at 20, 100 and 1 000 seeds before its
  assertion was written, every catch by the repair's check in its words (the tables above).
- *Nothing else moved with the fix.* Every other install-schedule variant ran on the same
  thousand seeds before and after it, and every seed's verdict, down to the words of its
  violation, is the same. On 20, 100 and 1 000 seeds of each: `InstallInTwoSwitches`, every
  seed, 13, 58 and 580; on the share, `SpanCheckpointUnsynced` 15, 77 and 812,
  `InstallKeepsSourceNumbers` 20, 98 and 967 (2 of them by the table writer's order
  assertion), `InstallSwitchPerSpan` 18, 90 and 922, `RepairAfterSwitch` 16, 61 and 616, and
  `CheckpointVersionPerSpan` 9, 47 and 468. `InstallSwitchPerSpan`'s test asks for a catch
  named "a mixture across its spans". Every one of its catches is still the agreement by the
  trace, which the fix does not touch, and the agreement by the tables alone still finds 70 of
  the hundred's 89 (above).
- *Two counters asserted above zero were close to vacuous.* "Several spans judged" counted
  every install over several spans after a crash, though the agreement by the tables has
  nothing to judge where fewer than two spans hold a write. "Repair found absent" counted
  every install a crash left out, wherever the crash fell, though only a crash between the
  replacement and the switch can leave the repair's table written and not yet listed. The
  first now counts an install only when two spans or more held a write, installed or older;
  the second only such a crash. Measured before their assertions were kept: installs judged
  on 20 of 20, 100 of 100 and 983 of 1 000 seeds, deletes judged on 19 of 20, 91 of 100 and
  965 of 1 000, repairs found absent on 11 of 20, 47 of 100 and 459 of 1 000. Every rate is
  far above 5 %, so each stays asserted from the gate's twenty; the thinnest, the repair found
  absent, at P(none) = 0.541^20 ≈ 4.6 × 10^-6.
- *The repair's number was argued, not tested.* It still is, and now says so, with a check
  that refuses the install if the argument ever fails (*The repair's number*, above).
- *The repair's boundary and adjacent spans had no directed test.* The refusal test now puts a
  repair write exactly at each span's end, which is outside it, and expects
  `RepairOutsideSpan` naming that key. A new crate test installs over two adjacent spans with a
  repair that deletes the first span's first key, puts at the shared boundary and at each
  span's last key, and puts then deletes one key. Every key and a scan are read right after
  the install, after a compaction takes the repair's table into level 1, and after a reopen.
- *The record contradicted itself after 3e66e86.* Two variants were said to run every seed, the
  binary to cost three quarters more CPU, the checkpoint check's P(none) was not by D-061's
  method, and the share was presented as decided. Each is corrected above.

The review's mutations, re-run in a throwaway copy of the fixed tree with its own target
directory, the storage crate's tests in debug and the engine binary in release:
- O3, the repair's check accepting a repair without its tables:
  `an_install_whose_repair_precedes_its_switch_is_caught` fails at 20 and at 100 seeds. The
  variant is still caught on 8 of 20 by a key read wrong, and on none by the repair's check.
- E10, a repair write at a span's end accepted: `an_install_of_several_spans_is_refused_when_it_cannot_be_made`
  fails, the install with a write at the end numbered rather than refused.
- E11, adjacent spans refused: `an_install_of_two_adjacent_spans_reads_right_at_their_boundary`
  fails, its checkpoint of the two refused as `SpansOverlap { end: k008, start: k008 }`.
- E8, the two numbers taken under two holds of the lock: every test passes, as it must, since
  nothing runs between the holds in the simulator. With a write let in between them, as
  another thread could, the crate's installs with a repair are refused as
  `RepairNotNext { seq: 151, repair_seq: 153 }`, and two crate tests fail on it. The sweep
  fails too, but on its model's own count, since it never asked for the write let in.

No trace hash moved. The fixes change what the oracle checks, add a refusal the simulator never
reaches, and add a variant that runs only in its own test. Each run in *What moved* above
hashes, on the fixed tree, to exactly its "This tree" column, by a probe built on 3e66e86 and on
the fixed tree, deleted and never committed.

**Alternatives.**
- *Option (b) of D-066, two single-span steps bracketed by a durable marker.* The owner
  decided against it (D-066).
- *One number for the source and the repair, the source's writes of the repair's keys
  dropped as the installed tables are written.* One fewer log record, but the repair would
  no longer be a table of its own, as the brief asks. It would also be spliced into the
  source's tables, so neither the trace nor the oracle could tell a repair write from an
  installed one.
- *The repair through the log, as a batch after the switch.* That is the crash window
  `RepairAfterSwitch` models: installed tables without their repair.
- *The repair as the install's own log record, holding its writes.* A crash before the
  switch would replay the repair over the spans as they were: a repair without its tables,
  the window `RepairBeforeSwitch` models.
- *One-element sets at every call site, with no wrappers.* The same behaviour, with a larger
  diff that shows no caller's meaning moving.
- *Refusing an empty span in a checkpoint too.* An empty span has always given an empty
  checkpoint, and dropping it keeps that. An install refuses one, as it always has.
- *Sealing the repair near `sst_bytes`.* Several tables would be needed only when a kept log
  tail is large. The trace would then carry a list, and the oracle would judge every table in
  it; that is left until a measured repair needs it.

**Consequences.**
- Every one-span caller is unchanged, and a one-span install without a repair writes and
  traces what it did before, bar the event's `spans` field in place of `start` and `end`.
- An install with a repair takes two log records, so a caller counting records sees one more.
  The repair's table is one more level-0 table per install, which compaction folds in like
  any other.
- The node (Stage B's next PR) calls `checkpoint_spans` for a range's take and
  `install_spans` for its install, over the range's two spans, with the repair D-066 lists.
- The engine sweep's default, install and range-delete schedules moved for every seed. The
  studio trace of seed 42 moved with them and now asserts the shape it shows. No pinned seed
  moved.
- The engine binary costs about a sixth more CPU at a thousand seeds with the variants on the
  share, 1 824 CPU seconds against the base's 1 574 (+16 %), measured on 3e66e86, and about
  half as much again at the gate's twenty, 64.07 s against 40.96 s in debug (+56 %), where
  the share is every seed. Most of the first is the heavier install and range-delete
  schedules. The review's `RepairBeforeSwitch` adds 37 CPU seconds at a thousand on its
  share, and at the gate's twenty in debug 45 CPU seconds, 8.6 s run alone. Under the owner's
  other option, every seed, the binary weighed 2 747 CPU seconds at a thousand with
  `RepairAfterSwitch` and `CheckpointVersionPerSpan` there, and `RepairBeforeSwitch` would add
  about 370 more (above).

---

## PROPOSED D-069 — The trace of SHARD.md §8: a range on every replica event, `key` and `effect` on an apply, and the read traced where it is served

**Context.** Stage B's second build (SHARD.md:2208-2218): "The trace of §8 (§11, env 1)".
SHARD.md §8 is the list of the invariants of a sharded cluster and the trace events each
check folds, and it opens by saying what today's trace lacks: "Today no event names a
group: every `Raft*` event names a server (trace.rs:368-652), and the enum already says
it 'grows with each phase (range id, …)'" (SHARD.md:1113-1116). Every check of §8 is
keyed by range; none of them can be written until the events carry one. §11's env item 1
says the same and adds that "Every pinned trace hash moves once, deliberately, with the
reason in the commit" (SHARD.md:1933-1935).

This entry is the trace alone. It changes what is recorded and what the existing checks
read, not what the system does — with one exception, stated below, which is how a read is
served: two reads at one engine version, where it was one read of the latest state.

**Decision.**

*A range on every `Raft*` event about a replica.* Twenty-three event kinds gain a `range:
u64` field — every `Raft*` event about a replica, `RaftProposed` among them, which §9's
history closure needs (SHARD.md:1128). The three
events about a node's *store* — `RaftRefused`, `RaftAdopted` and `RaftServerFailed` —
stay per node and carry none, as §8 says (SHARD.md:1127-1129). The moirae export writes
`range` immediately after `server`, so a studio filter carves a per-range trace
(SPEC.md:117-124). Today a node runs one group and the id is the one D-060's key layout
uses, `node::SINGLE_GROUP`; the core carries it in `RaftConfig::range`, whose default is
that constant, and the node stamps it on the events it traces itself.

*`key` and `effect` on `RaftApply`.* `key` is the key a single-key command touches, and
`None` for an entry that names none. `effect` is a new enum, `ApplyEffect`, with §8's
seven values (SHARD.md:1126). Two are reachable today and are emitted: `applied` for a
client command executed in its range, whatever it wrote — a `Cas` whose compare failed
writes nothing and is still `applied` — and `none` for a no-op or a configuration entry.
The other five belong to what later stages build and are defined here, with the stage
that emits each named in the doc comment: `out_of_span` and `frozen` with §3's re-check
at apply, `took`, `aborted` and `refused` with §5's split and §6's merge.

*`RaftRead` moved from the core to the server.* The core traced it when a read was
confirmed (core.rs:1175, 1202), before the server held the key or had served anything;
it is now traced by the server that serves the read (§11, raft 15; SHARD.md:1913-1918).
It keeps `index` and `lease` and gains `key` and `applied`, the applied index of the
engine version the value was read at. The server takes one `Engine::snapshot` and reads
both the value, with `get_at`, and the applied index, with a new
`RaftStore::applied_at`, at that version: the applied index is a key of the store,
written in the same synced batch as the entry's own writes (store.rs), so a read at one
version is the state at exactly that index. `Output::ReadReady` gains `lease`, since the
core is where the lease is known and the server is where the record is written. The
record's own time is when the server answered, and it carries the decision stamp of the
step that confirmed the read (D-047).

*The one version is one version while no live span install and no range delete runs on
this engine.* `Engine::get_at` excepts exactly those: from an install's or a range
delete's switch, a read at a version below the install's number sees the span it replaced
as empty rather than as it stood (D-054, engine.rs:783-788, 1275-1281). A value and an
applied index taken at one version could then straddle a switch and be the state at no
index at all, which is stronger than the guarantee stated above. Nothing reaches it
today, and the tree says so: `install_span`, `install_spans`, `delete_range` and
`delete_ranges` have no caller outside ananke-storage's own tests and the engine sweep —
none in `ananke-raft`, none in `ananke-server`, none in the raft, membership or quorum
scenarios — and a server's engine is fixed for the life of its incarnation, since a
re-seed opens a fresh one in a new directory behind a restart. Stage B's live per-range
install (SHARD.md §11, storage 8; Q15) is what first makes the straddle possible, so
that item carries this caveat — **filed as issue #72**, which states what it must settle and
what it measured, so it cannot be lost between two PRs as D-030's account of seed 164 once
was: it must say what a read served across an install of its
own range sees, and either order the two reads against the switch or read the descriptor
the same way. The caveat is on `RaftStore::applied_at`'s own doc comment, where the next
caller will read it. `applied_at` also answers `Ok(0)` where the key is absent at that
version; a served read cannot see it, since the core holds a confirmed read until
`applied >= index` and a read's index is at or above the leader's first entry of its
term, so at least one apply — which writes that key in its own batch — is durable at the
version served from. The sweeps fold it anyway: every `RaftRead`'s `applied` is at or
above its `index`, and a zero would be below it.

*The new event kinds of §8.* All twenty-two are added to `TraceEvent` with the fields
§8's table lists, each with its `convert` arm in the moirae bridge and its row in the
bridge's doc table. Three are emitted by this commit, because the core already does the
work they report:

- `RaftMatchStarted` — a leader's `matched` for a follower rose for the first time under
  the store incarnation the follower's answer carried (§11, raft 12). `Progress` gains a
  `match_started` flag, cleared when the leader takes office, when the follower is first
  tracked and at every change of its recorded incarnation.
- `RaftLearnerRound` — a leader ended a catch-up round for a learner (raft 9). `Learner`
  gains `from_index`, the learner's match when the round began.
- `RaftChangeAccepted` — a change accepted when the core has none in flight, as today
  (raft 8; D-029, DECISIONS.md:1099-1104).

Every other new kind is emitted by the stage that builds what it reports, and says so in
its doc comment: the `Range*` family, `MetaApplied`, `RebalanceMove`, `NodeAdded`,
`NodeRemoved`, `RangeIdsLeased`, `RangeMismatchSent`, `ClientSend` and `ClientMismatch`.
Six supporting enums and one struct come with them: `ApplyEffect`, `RangeCause`,
`RangeRemovedCause`, `RangeState`, `MismatchAt`, `RebalancePhase` and `MetaDescriptor`.

**What §8 leaves open, settled here, each marked `// PROPOSED(D-069)` in the code.**

1. *The new events carry no `server`.* §8's table lists none, and every record already
   carries its node (D-047). A replica is (range, node) and `ServerId` stays the node
   (Q26), so the node is the server. The existing `Raft*` events keep their `server`
   field, which is redundant with the record's node and always has been; nothing is
   removed.
2. *`effect`'s exported names are §8's own spellings*, `out_of_span` included, although
   the other enums the bridge writes are kebab-case (`torn-record`, `queue-full`). §8's
   table is the specification of these values and matching it exactly is the
   conservative reading; the check reads the Rust enum, not the string.
3. *`RaftInboxDropped` and `RaftSnapshotStreams` gain `range`.* §8 exempts only the three
   events about a node's store. A dropped frame is a message of some range's replica, and
   Q10 puts an 8-byte range id on every frame; a stream is per (range, follower) from
   Stage B (raft 14). Both are about a replica, so both carry one.
4. *The core's range lives in `RaftConfig`.* The alternative was a constructor parameter
   on `Raft::new`, `restore` and `restore_compacted`. The config is already per core and
   already carries the variant set; this keeps every existing call site unchanged and
   gives Stage B one place to set a range per core.
5. *`RaftChangeAccepted` is traced for every change the core does not refuse*, including
   one naming the voters already in force, which the core accepts and the server answers
   `Done` for. "Accepted" is what the caller is told, and check 22 folds what the caller
   was told. Two consequences of that reading, stated so a check does not read the
   record for more than it says. *It counts accepted requests, not changes.* A re-sent
   `Change` naming the voters of the change already under way is not refused either —
   it asks for what is already true (D-029, core.rs:1985-1991) — so it is accepted and
   traced again, and one change can carry several records, each with the `applied` and
   `term` of its own acceptance. A count of these records is a count of what the leader
   answered `Done`, never a count of configurations. *It is traced after the entry it
   accepts.* The record is written at the end of `on_change`, after `append_joint` has
   appended and replicated the joint entry (core.rs:2050-2058), so the joint entry's
   `RaftAppend` and `RaftConfig` precede the `RaftChangeAccepted` that reports accepting
   it; a fold that expects the acceptance first would see none. The learner branch
   traces in the same place, before any of the change's entries exist.
6. *`RaftMatchStarted` is also traced when an install's answer raises `matched`* — the
   install's answer carries the follower's incarnation and is recorded by the same
   `note_incarnation` (core.rs). §8's wording is "the first rise ... under the store
   incarnation the follower's answer carried", and an install's completion is such an
   answer. Including it can only make the re-add window assertion see more.
7. *`match_started` is cleared at every change of the recorded incarnation*, whether or
   not the progress itself is reset, so `Variant::IgnoreIncarnation` — which records and
   never resets — still traces the first rise under each incarnation rather than one
   for the leader's whole term.
8. *An entry whose command the state machine cannot decode is `none` with no key.* It
   wrote nothing, no client of this workspace encodes one, and `none` is the value that
   claims least. `Transfer` and `Change` are never entries and fall here too.
9. *A key a client named is exported as text* (as `ananke.client.invoke` has always
   written its `key`, so the studio lines an operation up with its apply) *and a span
   bound as hex* (as `ananke.engine.span-installed` writes its keys, since a bound is not
   necessarily text).
10. *The existing checks are untouched.* State machine safety still folds (term, hash)
    per index; §8's check 4 adds `effect` to that value when the checks are keyed by
    range, which is the next PR.

**Alternatives.**

- *Stamping the range in the environment rather than on the event.* `SimEnv::trace` knows
  the node, not the replica; one node will run many ranges from Stage B. Rejected.
- *Keeping `RaftRead` in the core and adding the key there.* The core holds neither the
  key — it stays in the server's `reads` map — nor an engine version. Rejected: §8 and
  §11 raft 15 both say the record must be the serving server's.
- *Reading the applied index from `RaftStore::applied()`, the in-memory atomic.* It is
  free and moves no schedule, but it is the store's newest applied index, not the one of
  the version the value came from; a read served from an older snapshot would carry a
  newer index and check 9 would read the wrong descriptor. Rejected. A map from engine
  version to applied index, kept in memory, would also avoid the extra read; it is more
  state to keep correct across a re-open for a figure the engine can be asked for, and
  §11 raft 15 names `Engine::snapshot` and `get_at`. Rejected.
- *Defining only the three emitted events now.* The export's `convert` is an exhaustive
  match, so a later stage adding an event cannot compile without its export line; adding
  them all now is what §8's "every new event kind" asks for and lets the checks of the
  next PR be written against a type that exists.

**Consequences.**

- *No pinned trace hash moves.* The tree holds exactly one, echo's golden body hash in
  `sim/tests/echo.rs`, and the echo scenario runs no Raft, so it is unchanged — the
  moirae repository's studio fixture, which is pinned to it, does not move either. §11's
  env item 1 anticipated that every pinned hash would move once; on this tree there is
  none to move. The raft, membership and quorum traces all change: every `Raft*` line
  about a replica carries `range`, `ananke.raft.apply` carries `effect` and a `key`, and
  `ananke.raft.read` is written by the server with `key` and `applied`.
- *Every raft, membership and quorum schedule moves on any seed that serves a read.*
  Serving a read now takes two reads at one engine version where it took one read of the
  latest state, and the second read can await the disk, which redraws the schedule from
  the first served read on. This is the one behavioural change in the commit. Every
  pinned seed in `sim/tests` is re-audited in the same commit; the table is below.
- *A served read pins an engine version for as long as it is being served*, which is the
  second thing that changed beyond the trace. The `Engine::snapshot` the server takes is
  a live snapshot in the engine's set until it is dropped, and compaction's
  `smallest_snapshot` (compaction.rs:85) is the oldest live one: while a read is served,
  compaction cannot drop the versions at or above it, so a round that overlaps a read
  keeps writes it would otherwise have merged away. The window is one `get_at` and one
  `applied_at`, either of which can await the disk, and a read that is never served
  cannot hold one: the version is taken in the `ReadReady` arm and dropped before the
  client is answered (node.rs). Nothing measures the retention today; it is named here
  because a per-range read of Stage B holds a version per read in flight, where this
  holds at most one per server.
- *The correct system passes every seed* at 20, 100 and 1 000 seeds, and every variant
  keeps the standard and the tier it held before.
- *`RefusalNotDurable`'s catch rate moved* from 9 of the first thousand seeds to 7,
  0.7 %, and its first catch from seed 158 to seed 102. The assertion stays at the
  thousand-seed tier under D-061's rule; at 0.7 % a hundred seeds miss about half the
  time and a thousand about once in a thousand.
- *Coverage counters are added for the three events this commit emits*: match starts in
  the raft sweep, and match starts, learner rounds, learner rounds that caught up and
  accepted changes in the membership sweep. Measured at a thousand seeds in release on
  this tree: the raft sweep sees 18 373 match starts on 1 000 of 1 000 seeds; the
  membership sweep sees 11 671 match starts, 5 171 learner rounds of which 2 494 caught
  up, and 3 143 accepted changes, each on 1 000 of 1 000 seeds. Four of the five are on
  every seed, so each is asserted *per seed* — `seeds_with_a_match_start == seeds` in
  both sweeps, and the learner rounds and the accepted changes the same way in the
  membership sweep — which is what D-061's rule asks of a counter at 100 %: the rate is
  over the seeds the assertion sees, and a whole-sweep total above zero is not a rate at
  all, since one event on one seed satisfies it however the emission rule behaves
  elsewhere. Rounds that *caught up* are 2 494 over 5 171 rounds and are not on every
  seed, so that one stays a total above zero. The counters as first written asserted
  only totals, and the membership sweep's three per-seed counters were printed and never
  asserted; both are corrected here.
- *What the counters cannot see, and what does.* A counter says an event fired, never
  that it fired by its rule. Emitting `RaftMatchStarted` on *every* rise of `matched`
  rather than the first under an incarnation — deleting one clause from the core's
  guard — raises the raft sweep's count from 1 840 to 63 164 at a hundred seeds, on
  every one of which a match start is still seen, with every seed still green (measured
  on this tree with the fold below silenced). So the sweeps fold the rule itself, per
  run: at most one `RaftMatchStarted` per (leader, term, follower, incarnation), which
  that mutation fails on the first seed. What remains unverified is the rest of the
  rule — that an
  event fires *where it must*, and in particular that the first rise after a re-add is
  the one traced. Nothing in the tree reads these three records yet: §8's checks are
  keyed by range and arrive with the stages that emit the rest of §8's events, and the
  re-add window assertion of §8 comes with Stage D's `IncarnationFromRangeStream`
  variant (SHARD.md:2583), the known-buggy pair that makes `RaftMatchStarted` the record
  a check turns on. Until then these events are recorded, counted per seed and held to
  the one-per-window rule, and nothing more is claimed for them.
- *The payload's own structure is folded by the sweeps*, since nothing else reads it.
  Before this, no code in the tree read `range`, `key`, `effect` or a read's `applied`:
  a mutation stamping the wrong range on one event kind, `applied` on a configuration
  entry, or dropping the `key` from a single-key apply passed 20 and 100 seeds green.
  The raft and membership checks now fold three structural facts over the records they
  already walk — every `Raft*` record about a replica carries `node::SINGLE_GROUP`; an
  apply is `applied` with a key or `none` without one, and no other effect exists at
  this stage; a read's `applied` is at or above its `index` — and each of the three
  mutations above now fails, naming the record, as does a fourth stamping a single-key
  apply `none`. They are the oracle the checks of §8 will replace. Both folds are green
  on every seed at the gate's twenty and at CI's hundred; the thousand-seed premerge
  below was run before them, so the next premerge is the first to run them at that
  tier.
- *RAFT.md §2's event table is updated in the same commit* (D-053): the `range` rule and
  its three exceptions, `RaftApply`'s `key` and `effect`, `RaftRead`'s move and its two
  new fields, and the three new events.
- *`scripts/premerge.sh` at a thousand seeds, measured against `main` in one session*, since
  a figure from another day says nothing on a machine whose speed drifts (D-052's protocol,
  sharpened here). Both trees built first, then run one after the other on the same idle
  8-core Apple M2, the one-minute load sampled every ten seconds:

  | Tree | Wall | Mean load | raft | engine | WAL |
  |---|---|---|---|---|---|
  | this commit, a7c9f54 | 1 476 s | 17.15 | 893.2 s | 538.2 s | 23.3 s |
  | `main`, b292034 | 1 473 s | 20.16 | 888.2 s | 541.2 s | 22.9 s |

  The raft binary came out 5 s slower here, the engine binary 3 s faster and the WAL's
  0.4 s slower. **What this pair supports is that the change makes no difference the
  premerge can see, not a figure of 0.6 %.** It is one unrepeated measurement; its two
  halves ran at different loads (mean 17.15 against 20.16, sampled every ten seconds) on
  a machine measured between 1.7 and 2.4 times slower than it was two days earlier; and
  the two raft binaries do not run the same work — this branch's has 44 tests to `main`'s
  43, the new one being seed 102's pin, which is two full scenario runs of its own, so
  part of the 5 s is a test `main` does not run. The engine binary, which this change
  does not touch, moved 3 s the other way on the same pair, which is the size of the
  noise. A cost for the two-reads-at-one-version of `RaftRead` and the fields on every
  replica event would take the pair repeated, with the test sets matched; no decision
  here needs that number.

  Both trees are over D-040's quarter of an hour on this machine today, and `main` is over it
  by itself: the same code that ran the premerge in 613 s two days ago (D-068, on ea44e38)
  takes 1 473 s now, and D-063's 46e95c0 ran the raft binary in 535.9 s where `main` now takes
  888.2 s. The machine is between 1.7 and 2.4 times slower than it was for those runs — no
  thermal or power warning is recorded, and nothing else of this session was running — so the
  premerge's budget cannot be read from figures taken on different days. What a change costs
  is the paired measurement above; what the premerge costs in wall time is a question for the
  machine it runs on, and the earlier figure of 1 615 s at mean load 18.39, taken on this tree
  alone, is withdrawn in favour of the pair.
- *Every variant's catch rate at a thousand seeds on this tree*, against the tier each
  asserts at: `RefusalNotDurable` 7 (0.7 %, from the thousand-seed tier, D-061),
  `LeaseTrustsTheClock`'s stale read 40 (4.0 %, same tier; it was 37), `SharedSnapshotDir`
  1 and `IgnoreIncarnation` 0 (both asserted only at the nightly's ten thousand, as
  before), `AdoptionAsBuilt` 62, `SingleMajorityInJointConsensus` 230,
  `SnapshotWithoutCurrentLast` 334, `ResetTimerOnAnyRpc` 341,
  `CountOlderTermForCommit` 449, `ApplyBeforeCommit` 882, `TruncateOnEveryAppend` 999,
  `NoPreVote`, `SendBeforePersist`, `RefusedNeverCounts` and `RefusedCountsForQuorum`
  1 000. No variant changed the tier it asserts at.
- *The next PR* keys checks 1 to 4 by range and adds check 4's `effect`; the checks of
  §8 that fold the events defined here arrive with the stages that emit them.

**The per-seed re-audit** (CLAUDE.md:58-67, and the owner's ask on this PR). Every pinned
seed in `sim/tests`, with what it asserts on the moved schedule — its mechanism, or the
situation's absence with the reason. No pin is left asserting green alone.

| Pin | On this tree |
|---|---|
| `seed_164_…_stays_green` (raft) | Absence, unchanged. No snapshot-fed timer gap on the run; `snapshot_fed_timer_gaps` empty, and the pin fails the day it is not. |
| `seed_385_…_stays_green` | Absence, unchanged. `timer_gaps_rescued_by_restatement` empty: the replay without D-039's arm finds no gap, so there is no stretch for a restatement to rescue. |
| `seed_2605_…_adoption_window…` | **Was a mechanism, now an absence.** The correct run holds 14 adoption windows but the replay without D-063's arm finds no gap at all, so no window lies inside one. The search of the first thousand seeds for a home found none: 0 of 1000 reach a gap holding a completed install, so the pin stays here as an asserted absence, as D-056's did for seed 132's pair. Its pair moved too: `ResetTimerOnAnyRpc` is no longer caught here, because the run's majority is not up at the end and RAFT.md §2's carve-out withholds the timer bound; the replay still finds the variant out twice, which is asserted, as is the carve-out being the reason. The variant's catch is asserted at every tier by its own sweep. |
| `seed_7381_…_stays_green` | Absence, unchanged. `recoveries_under_a_lost_floor` and `floor_lowering_installs` both empty. |
| `seed_6325_…_stays_green` | Absence, unchanged, under both servers: adoption windows present (non-vacuity asserted) and no crash inside one. |
| `seed_5909_…_stays_green` | **Mechanism, moved instants.** D-042's refusal → reset → re-seed is still on server 3, now refused at 18.200834224 s on the marker its lost-state refusal wrote, reset at 18.206060773 s, re-seeded at 18.492068093 s. No stream wedge; 29 takes, no index twice. |
| `seed_5909_passes_under_both_bugs_together` | **Mechanism, one set changed.** Under `IgnoreIncarnation` alone server 3's progress still goes stale and now also leaves server 3 uncounted after the last heal, where it left nothing before; the stream half and the pair are unchanged ([], and [1] stale and uncounted). Each run's stale and uncounted sets are asserted exactly. |
| `seed_132_…_reaches_no_wedge` | **Was "reaches nothing", now a mechanism and an absence.** Every one of the four runs refuses server 2 at least once, where the pair and the stream half refused nothing before, so `IgnoreIncarnation` has something to ignore: under the pair leader 3 of term 13 had 335 acknowledged, server 2 is refused at 13.750549431 s, and of 806 probes after it 778 are rejected and none accepted, leaving server 2 the one follower uncounted after the last heal — one, not the wedge's two. The stream half stays out of reach for its old reason, no index taken twice, which is asserted with its non-vacuity. The fix — refusal, reset, re-seed — is asserted where the leader keeps D-042's rule. |
| `seed_680_…_no_longer_wedges` | Mechanism, unchanged: the stream half's shape reached (re-takes under live streams that still complete), D-042's half under the pair, one uncounted follower, and no wedge. |
| `seed_687_…_stays_green` | **The two halves swapped.** The fix's half is here now and is asserted record by record: table 34 dropped at 19.024651157 s, the engine quiesced at 19.061652101 s, the node restarted at 19.083 s, and that open refused at 19.093731093 s on the durable mark, with the quiesced engine writing no manifest, switching no `CURRENT`, deleting no log segment and opening clean nowhere before the install adopted at 19.318300999 s. The bug's half is absent with its reason: as built no engine on the run reports lost state at all, so there is no refusal for a flush to launder and no crash lands on a refused server. |
| `seed_1885_…_no_longer_straddles…` | Absence, unchanged: no term change straddles any isolation's start, the check by durability time flags nothing, and the isolation the nightly named does not begin. |
| `seed_2023_…_no_longer_straddles…` | Absence, unchanged, the same three ways. |
| `seed_1_of_the_term_raise_schedule…` | **Mechanism, count changed 4 → 5.** Every one of the run's five isolations now straddles, where four did; the first — server 2, term 1 to 2, decided 1.217846847 s at the delivery of server 1's RequestVote of term 2 and traced 2.758 ms into the window at 1.220607818 s — is unchanged instant for instant, and is what the pin names. D-050's shape is still absent here, which is asserted. |
| `seed_4_of_the_term_raise_schedule…` | Mechanism, unchanged: exactly one change received before an isolation and stepped inside it, server 3 from term 4 to 5, receipt 2.853812638 s, isolation 2.85382 s, step 2.853832880 s. |
| `seed_158_pins_the_refusal_that_is_not_durable…` | **Was the mechanism, now an absence, and renamed** to `seed_158_which_pinned_the_refusal_that_is_not_durable_before_the_read_moved_loses_nothing`. No open on either run drops a table, so no engine reports lost state: the variant has no refusal to launder and the correct server has nothing to quiesce. The seed still refuses stores — a log that stops at a bad checksum, an unreadable manifest, an unreadable marker — which is asserted as the companion that keeps the absence from being vacuous. |
| `seed_102_pins_the_refusal_that_is_not_durable…` | **New**, the re-pin the search found: `RefusalNotDurable` is caught on 7 of the first thousand seeds (102, 293, 378, 465, 744, 893, 926) and seed 102 is the first. The mechanism is asserted whole — table 6 dropped at 13.556327079 s, the lost-state refusal at 13.578870962 s, the crash on the refused server and its restart, the second refusal at 13.67192931 s, the refused engine's flush and manifest 10 without table 6, `CURRENT` switched at 13.690493628 s, the orphan removal of `000006.sst`, the clean open at applied 331 with no install between, and state machine safety's report of it — with the correct server's half absent here and its reason asserted. |
| `seed_119_…_refuses_nothing` | Absence, unchanged: neither run refuses a store, the two traces are record for record the same, and no crash lands on a refused server. |
| `the_nightlys_eleven_variant_catches…` | Eleven (seed, variant) pins, each an absence of the straddle: unchanged, all eleven. The companion fact moved: the one run whose leader steps down leaving a follower uncounted is seed 3087's, not seed 1252's, and the pin asserts which rather than that none does. |
| `the_nightlies_removed_catches…` | Twenty-eight (seed, variant) pins, each an absence of the catch: unchanged, all twenty-eight, and no catch is added on any. Two companion facts moved. The runs that keep the isolation their catch named are now six, seed 5918's having come back (5203, 6691, 5051, 5879, 5918, 2578). And three runs are now caught over their own variant's bug rather than passing — seed 2305 under `SnapshotWithoutCurrentLast` by state machine safety at applied 282 over index 281 (it was 288 over 279), seed 6717 under `ResetTimerOnAnyRpc` by the timer check, and seed 9557 under `AdoptionAsBuilt` by committed-entries-stay, server 2 truncating from index 1 — each asserted by its words, and `checked` asserts for each that decision time removes nothing. Seed 5153's replay by durability time finds three gaps where it found five, on a run the timer bound is still not asked of, which is asserted. |
| `the_seed_42_trace_is_written_for_the_studio` (raft) | Not a mechanism pin: it writes `out/raft-42.jsonl` and asserts the run's shape. Green; the trace's content moved with the new fields, as the whole commit's does. |
| `same_seed_gives_byte_identical_trace` (raft, echo, wal, engine) | Determinism pins. Green: two runs of one seed still agree byte for byte. |
| `trace_hash_matches_the_pinned_golden` (echo) | The one pinned trace hash in the tree. **Unmoved**: echo runs no Raft. |
| `the_membership_scenario_has_byte_identical_traces_for_one_seed` (seed 7) | Determinism pin on the membership scenario. Green. |
| `seed_420_…`, `seed_44_…`, `seed_3123_…` (engine) | The engine scenario's three pins. Green and unmoved: the engine sweep runs no Raft, so nothing on their schedules changed. |
| The quorum tests (`the_correct_leader_steps_down_on_a_blocked_reseed…`, `a_leader_that_counts_a_refused_followers_rejections…`, `a_leader_that_counts_nothing_from_a_refused_follower…`, `on_the_sweeps_disk_the_install_silence_deposes_the_leader…`) | Not seed pins: each asserts on *every* seed of the directed quorum scenario. Each holds its standard — the correct leader steps down naming the refused follower on every seed, and each variant is caught on every seed — on the moved schedule. |

## PROPOSED D-070 — The premerge says what machine it ran on

**Context.** D-052 set how the premerge is measured: warm build, the one-minute load
sampled, the figure recorded beside the last one. D-069 then measured the same tree twice
and found the protocol was not enough. `scripts/premerge.sh` ran in **613 s** on 2026-09-19
and in **1 473 s** on 2026-09-20 on this laptop, with no code between them that touched the
sweeps; the raft binary went from 535.9 s (D-063) to 888.2 s for the same tests. Every
premerge figure in this document was therefore incomparable with every other, and D-069's
own paired run — both trees measured back to back — was the only honest way to say what a
change cost. A figure that cannot be compared is a figure that misleads: the owner asked,
on 2026-09-20, that a slow run explain itself in its own output rather than become a mystery
a week later.

**The cause, most likely, and the reason this is cheap to fix.** The machine was on battery
and discharging at 29 % when the slow figures were taken; macOS throttles sustained load on
battery, and `pmset -g therm` records no thermal warning in that state, so nothing in the
old output said anything was different. One line of the run's own output would have said it.

**Decision.** `scripts/premerge.sh` prints, before the sweeps and again after them:

- the kernel, the processor and the core count, once;
- the one, five and fifteen-minute load averages;
- the power source — `AC Power` or `Battery Power` on macOS, the `online` flag under
  `/sys/class/power_supply` on Linux;
- any thermal pressure — the CPU speed limit `pmset -g therm` reports, or "no thermal
  warning recorded"; `thermal_zone0` on Linux;
- and its own wall time, so a run's cost and the state it ran in are in the same three lines.

The state after the sweeps is printed whether they passed or failed, and a failed run says so
with its own time: the machine's state matters most exactly when a run was slow or red, so the
script keeps the sweeps' status and reports it after that line rather than ending at the
failure.

Whatever a machine does not report reads `unknown`, never nothing: a missing field must be
visibly missing. The script runs on macOS and on Linux, and adds no dependency.

**Consequences.**

- A premerge figure quoted in an entry carries the state it was taken in, and two figures
  are comparable only when their states are.
- D-052's protocol stands, with this added: a figure from another day is evidence only
  beside its machine state, and what a change costs is still the paired measurement D-069
  used — both trees, one session, one machine.
- The gate and the nightly are unchanged. The gate is a yes or no, and the nightly runs on
  GitHub's runners, whose own variance issue #57 records.

---

## PROPOSED D-071 — The checks of SHARD.md §8 keyed by range: checks 1 to 4 over a group key, the history's closure, and the bounds per replica, per range and per key

**Context.** Stage B's third build (SHARD.md:2219-2225): "The checks keyed by range (§8;
§11, raft 12): checks 1 to 4 in `ananke-raft` over a generic group key, each range's
first configuration taken from its `RangeCreated`; check 6 per range; the history's
closure keyed by `(range, index, term)` (§9; §11, env 9); the timer check and pre-vote's
property per (range, server); the checks about time asked only of a range whose
unimpaired replicas form a majority, and the write bound asked per key (§8)."

§11's raft item 12 says what the tree has: `invariants::Checker` "keys every map by
server, term or index (invariants.rs:253-282) and takes `1..=servers` as the first
configuration (invariants.rs:289-295)". D-069 put a `range` on every replica event and
defined `RangeCreated` and `RangeRemoved`; this entry keys the checks that read them.

**It changes no behaviour.** Every line it touches is a check over a trace the code under
test produced: no event is added or moved, no draw changes, no fault arm changes. No
pinned trace hash moves and no schedule moves, which the determinism pins and every
pinned seed's own assertion say on the gate.

**Nothing in this tree emits `RangeCreated` or `RangeRemoved`, and the table below should
be read with that in front of it.** The two events exist in the trace type (D-069), in
the moirae export, in the checks here that read them, and in the hand-built unit cases of
this PR — and nowhere else. No scenario, no node, no sweep produces one. So every clause
of this entry that turns on them is true of the unit cases and of nothing that runs: "a
group's first configuration is the voters of its `RangeCreated`" describes a path no seed
takes and `Checker::initial_voters` returns `1..=servers` on every seed of every tier; no
replica's election timer is ever armed by a creation or ended by a removal; no isolation
ever meets a range created inside it; no floor and no applied memory is ever set or
cleared by either event outside a test. The keys are built and proved here so that the
stage which emits the events finds the checks already keyed — that is the whole of what
this PR buys. What the sweeps do exercise is the other half: the `(range, server)` and
`(group, index)` keys over the events every seed *does* trace, which with one group say
exactly what the keys before them said, which is why the oracle below is hand-built and
not a sweep.

**Decision — each check's key as built.**

| Check | Where | Key as built | The key before |
|---|---|---|---|
| 1, election safety | `invariants.rs` | `(group, term) → server` | `term → server` |
| 2, log matching | `invariants.rs` | a log and a snapshot floor per `(group, server)`; a `RangeCreated` sets its replica's floor as an installed snapshot does; two snapshots are compared at an index only within one group | per `server` |
| 3, leader completeness | `invariants.rs` | the committed set per `(group, index)`, and the rescan at every `RaftLeader` is over that group's range of it | one set, per `index` |
| 3, commit by majority | `invariants.rs` | the configuration in force per `(group, server)`; a group's first configuration is the voters of its `RangeCreated`, and `1..=servers` only for a group whose creation the trace does not hold | per `server`, always `1..=servers` |
| 3, commit by current term | `invariants.rs` | who leads, per `(group, server)` | per `server` |
| 3, committed entries stay | `invariants.rs` | the commit index per `(group, server)`; a node's `RaftRefused` clears every group on it, a `RangeRemoved` that one replica | per `server` |
| 4, state machine safety | `invariants.rs` | a map per group from index to `(term, hash, effect)`; the applied index per `(group, server)`, consecutive from the floor a `RangeCreated` or an install sets; a `RangeRemoved` ends that replica's memory as a refusal ends a store's | per `index`, and per `server` |
| 5, linearizability | `sim/lin.rs` | the history's closure by `(range, index, term)`; the partition stays the key, which no boundary moves (§9) | `(index, term)` |
| 6, lease safety under drift | `sim/tests/raft.rs` | nothing of its own: it is not a fold but check 5 on every seed — whose closure is now keyed — and the with-guard/without-guard test, and it is carried over as that | — |
| the timer check | `sim/raft.rs` | every set of the replay per `(range, server)`: running, leading, re-seeded, the term, the clock and its last reset. A replica's `RangeCreated` arms its timer and its `RangeRemoved` ends it; a `NodeCrashed` takes every replica on the node down | per `server` |
| pre-vote's property | `sim/raft.rs` | asked of each replica of the isolated node, `(range, server)` — the replicas its term records name, and only those (item 12); a range created on it during the isolation takes its `RangeCreated`'s floor term as the term the window began with | per `server` |
| the checks about time | `sim/raft.rs` | asked of each range whose replicas that are neither refused nor quarantined form a majority, at the two places the carve-out is used and not only where it is computed (rows M1 and X3); a node's refusal impairs every range on it — every range of the whole run, in fact, which over-impairs (item 13) | per cluster |
| the write bound | `sim/raft.rs` | per key: the first write to each key the run wrote after the heal completes within ten maximum election timeouts | one minimum over every write |

**The oracle: what a wrong key would look like, and the case that catches it.** With one
group every event of every sweep carries `SINGLE_GROUP`, so a check keyed by `(group,
term)` and one keyed by `term` say the same thing on every seed at every tier: no sweep
can tell them apart, and a wrongly keyed check would ship green. Each keyed check
therefore has a hand-made two-range case of its own, in the style of the folds' existing
unit tests — `crates/ananke-raft/tests/invariants.rs` for checks 1 to 4, `sim/lin.rs`'s
and `sim/raft.rs`'s own test modules for the rest — and each is a pair: a two-range trace
the keyed check accepts and a wrongly keyed one rejects, and a one-range trace that is a
real violation and the keyed check still rejects, so that keying has not widened a check
into a check of nothing.

Then each wrong key was planted, one at a time, in a copy of this tree with a target
directory of its own, and the tests run. **Every row fails, and every row names the case
written for it.** A check whose wrong key nothing catches is not keyed; the first run of
this table had two such rows, and both were the case's fault, not the check's:
`a_created_replica_starts_at_the_floor_its_creation_names` exercised only the *applied*
floor a creation sets and not the *log's*, and now commits index 6 of a group created at
floor 5 through the majority check, which asks for index 1 without the floor; and
`a_leader_of_one_group_is_not_a_leader_of_another` traced the leader's role record before
the follower's, so the wrong key's own `remove` undid it, and the two are now in the
order a node takes them in.

The table below *is* that run and not a transcription of one. The harness is `scratchpad
stage-b/checks-fix/mutate.py` and the run these cells are read off is `scratchpad
stage-b/checks-fix/mutations-d071.log`, which plants all twenty-three rows in sequence
and prints the table at the end; no row of it says "NOTHING FAILED". Three rows plant
something that is not a key, and say so: **X3** and **T0** plant the timer check's body,
and **4f** is check 4's mismatch message (item 14).

**X3 is the row that matters most, and the first draft of this table did not have it.**
M1 plants the wrong key inside `ranges_with_a_majority_up` and is caught — but that only
proves the *helper*. The carve-out is **used** in two places, `Report::timers_fire_by`
and `Report::liveness`, and replacing both of those with the cluster-wide
`Report::majority_up`, leaving the helper perfectly correct, passed every sim unit test
and every sweep tier as this PR first stood: with one group the two predicates coincide,
so the wedged range beside a live one that §8 exists to prevent would have shipped green.
`a_live_ranges_gap_is_flagged_though_the_range_beside_it_has_no_majority` drives the
consuming end instead of the helper — one range short of a majority, the range beside it
live and past its timer bound, and the check must report that gap — and it is the only
case that fails under X3. The liveness half of the carve-out cannot be driven by a case
today, and is not: `range_of_key` is the constant `SINGLE_GROUP` for every key (item 6),
so no hand-built history can put two keys in two ranges, and that half is driven by the
stage that gives `range_of_key` a map.

| Wrong key planted | The case that fails |
|---|---|
| 1 election safety: the leader map keyed by the term alone | `two_groups_may_elect_different_leaders_in_one_term` |
| 2a log matching: the logs and floors keyed by the server alone | `two_groups_on_one_server_may_differ_at_one_index`, `two_groups_snapshots_at_one_index_may_carry_different_terms` |
| 2b log matching: two groups' snapshot floors compared at one index | `two_groups_snapshots_at_one_index_may_carry_different_terms` |
| 2c log matching: a `RangeCreated` sets no floor | `a_created_replica_starts_at_the_floor_its_creation_names` |
| 3a commit majority: the first configuration taken as `1..=servers` | `a_group_commits_on_a_majority_of_the_voters_it_was_created_with`, `a_group_may_not_commit_on_a_minority_of_the_voters_it_was_created_with` |
| 3b leader completeness: the rescan over every group's committed set | `a_new_leader_of_one_group_need_not_hold_another_groups_committed_entries` |
| 3c commit by current term: who leads kept per server | `a_leader_of_one_group_is_not_a_leader_of_another` |
| 3d committed entries stay: the commit index kept per server | `a_commit_index_in_one_group_does_not_bind_another_groups_truncation` |
| 4a state machine safety: the applied map keyed by the index alone | `two_groups_may_apply_different_entries_at_one_index`, `a_servers_applies_are_consecutive_within_each_group`, `one_group_may_not_apply_one_entry_to_two_effects`, `a_nodes_refusal_ends_every_groups_memory_on_it` |
| 4b state machine safety: the applied index kept per server | `a_servers_applies_are_consecutive_within_each_group`, `a_removal_ends_one_replicas_memory_and_no_other`, `a_nodes_refusal_ends_every_groups_memory_on_it` |
| 4c state machine safety: the effect left out of the value | `one_group_may_not_apply_one_entry_to_two_effects` |
| 4d state machine safety: a removal read as the node's | `a_removal_ends_one_replicas_memory_and_no_other` |
| 4e state machine safety: a removal read as every node's | `a_removal_ends_one_replicas_memory_and_no_other` |
| 4f *not a key, the message*: check 4's mismatch names the terms alone | `one_group_may_not_apply_two_entries_at_one_index` |
| 5 the history's closure keyed by `(index, term)` | `lin::tests::an_operations_proposal_is_closed_only_by_its_own_ranges_apply` |
| T0 *not a key, the body*: the timer check's gap report neutered | in `raft::tests`: `a_snapshot_a_server_took_itself_leaves_its_election_timer_running`, `a_coreless_window_longer_than_the_bound_is_removed_not_measured_from_the_completion`, D-051's four `a_timer_catch_*` cases, `one_ranges_reset_does_not_stand_in_for_anothers_silence` and `a_live_ranges_gap_is_flagged_though_the_range_beside_it_has_no_majority` — eight in all |
| T1 the timer check's clocks kept per server | `raft::tests::one_ranges_reset_does_not_stand_in_for_anothers_silence` |
| T2 the timer check: a creation arms no timer and a removal ends none | `raft::tests::a_creation_arms_a_replicas_timer_and_a_removal_ends_it` |
| P1 pre-vote: the isolated server's terms read as one sequence | `raft::tests::one_servers_two_ranges_keep_their_terms_apart` |
| P2 pre-vote: a range created under the isolation starts at term 0 | `raft::tests::a_range_created_under_an_isolation_starts_at_its_creations_term` |
| M1 the checks about time asked of the cluster in the *helper* | `raft::tests::a_majority_is_asked_of_each_range_and_a_refusal_is_the_whole_nodes`, `raft::tests::a_live_ranges_gap_is_flagged_though_the_range_beside_it_has_no_majority` |
| X3 the checks about time asked of the cluster where the carve-out is *used* | `raft::tests::a_live_ranges_gap_is_flagged_though_the_range_beside_it_has_no_majority` |
| W1 the write bound as one minimum over every write | `raft::tests::the_write_bound_is_asked_of_every_key_written_after_the_heal` |

The pairs' other halves — the one-range violations — are in the same files beside them.
There are **ten**:
`one_group_may_not_elect_two_leaders_in_one_term`,
`one_group_may_not_hold_two_payloads_at_one_index_and_term`,
`a_new_leader_of_the_group_that_committed_an_entry_must_hold_it`,
`a_group_may_not_commit_on_a_minority_of_the_voters_it_was_created_with`,
`a_leader_may_not_commit_an_older_terms_entry_of_its_own_group`,
`a_replica_may_not_truncate_below_its_own_groups_commit_index`,
`one_group_may_not_apply_two_entries_at_one_index`,
`a_replica_may_not_apply_one_index_twice`,
`an_operations_proposal_is_closed_by_its_own_ranges_apply`, and
`a_replica_that_raises_its_own_ranges_term_while_isolated_is_caught`.

**The timer check is the one keyed check with no one-range violation of its own in this
PR**, and it does not need one: its one-range half is the suite that was already there
and that this PR did not write.
`a_snapshot_a_server_took_itself_leaves_its_election_timer_running`,
`a_coreless_window_longer_than_the_bound_is_removed_not_measured_from_the_completion` and
D-051's four catch cases — the three removals
(`a_timer_catch_removed_by_a_reset_decided_before_the_flag`,
`a_timer_catch_removed_by_the_flag_record_decided_within_the_bound`,
`a_timer_catch_removed_by_leadership_decided_before_the_flag`) and
`a_timer_catch_both_readings_make_has_no_removal_reason` — all read the gap report on one
range, and all six die when that report is neutered (mutation row T0 below), so the check
is held to a one-range violation exactly as the other ten are.
`each_ranges_own_reset_keeps_its_own_timer` is *not* one of them: it is the two-range
**accept** case beside `one_ranges_reset_does_not_stand_in_for_anothers_silence`, and an
earlier draft of this list miscounted it as a violation.

**What §8 leaves open, settled here, each marked `// PROPOSED(D-071)` in the code.**

1. *The checker is fed the node beside the event.* `RangeCreated` and `RangeRemoved` name
   their range and no server — §8's table lists none, and every record carries its node
   (D-069, item 1) — but the state they key is a *replica's*, `(range, server)`. The
   checker's input becomes `invariants::Traced`, the event with the node that traced it;
   a `&TraceEvent` converts into one with no node, so every existing caller compiles
   unchanged and the two range events are skipped there, naming no replica. The sweeps
   pass the record's node (`ananke_sim::traced`), and the scenarios' node ids are their
   server ids. The alternative, a `server` field on the two events, would contradict §8's
   table and D-069's first settled item.
2. *Check 4's value is `(term, hash, effect)`, and a recovered apply has no effect.* The
   entries a restart's recovered applied index accounts for are read from the replica's
   log, which holds the entry and not what applying it did, so their effect is `None`; it
   agrees with any effect and is filled in by the first apply that names one. Term and
   hash are compared as before. The conservative reading is the one taken: an unknown
   effect never makes a violation and never hides a disagreement between two apply
   records.
3. *The messages name the group.* Every violation of checks 1 to 4 names it — "both led
   term 5 of group 2", "server 1 applied index 1 of group 2 after 1" — and four pinned
   literals in `sim/tests/raft.rs` moved with them, on the same seeds, over the same
   mechanisms. **The pre-vote violation does not name the range**: it is asserted word for
   word by forty-four pinned assertions in `sim/tests/raft.rs`, a run of this stage has
   one range, and the stage that
   gives a node many ranges moves every one of those pins for its own reasons (SHARD.md,
   Stage B: the node and the seed switch each move every schedule). Naming it there costs
   nothing and naming it here would rewrite forty-four assertions twice.
4. *§9's other two changes are not built here.* §11's env item 9 asks for three things:
   the closure keyed by range, closing only on effect `applied`, and an operation's
   proposals gathered across the fresh sequence numbers of its resends by `ClientSend`'s
   `invoked`. Stage B's bullet asks for the first, and the other two have nothing to act
   on yet: no apply of this stage can carry an effect other than `applied` or `none`,
   which D-069's payload oracle asserts on every seed, and `ClientSend` is emitted by the
   stage that routes a client to a range. Each belongs to the stage that can make it
   false.
5. *The timer check's other skips are not built here.* §8 lists four skips for the
   *sharded* sweep: a replica that is not a voter of its configuration in force, a
   placeholder between its `RangeReplicaCreated` and its `RangeCreated`, one stalled at a
   merge, and one after its `RangeRemoved`. The first three read events no stage emits
   yet and a notion of the configuration in force that the replay does not keep; the
   fourth is built, with the creation that arms the timer, because both are this
   stage's events and both are what make a replica's timer its own.
6. *A delivered frame resets the timer of the group it carries.* A frame holds one
   message of one group today, so the reset is `SINGLE_GROUP`'s; §4's batch frame tags
   each message it holds with its range (§11, raft 1), which the decode reads there. The
   same holds for the key's range in the write bound: the scenario's ranges are fixed and
   a client takes its key's range from that map (Stage B), so `range_of_key` is one
   group until the node gives it more. **It is a constant function**: it returns
   `SINGLE_GROUP` whatever key it is handed. That is why the liveness half of the majority
   carve-out has no case of its own in the oracle above — with every key in one range, no
   hand-built history can put a live range and a range without a majority on two different
   keys, so `Report::liveness`'s `live.contains(&range_of_key(&key))` cannot be told apart
   from the cluster-wide reading by any test that can be written today. The timer half
   *can* be told apart, and is (row X3). The stage that gives `range_of_key` a map owes
   the liveness half its own two-range case in the same PR.
7. *The write bound is asked of the keys the run wrote to after the heal, and of no
   others.* A key no client wrote to in the window is no evidence either way — the two
   clients draw their keys at random, and a key can go a whole window unwritten — and a
   write no leader ever proposed is not in the history at all. A key whose post-heal
   writes all stayed pending *is* the wedge the check is here to see, and is a violation.
   With no range left with a majority the check asks nothing, as it asked nothing when it
   was the cluster's.
8. *The per-range majority keeps the down set and the quarantine apart.* A refusal is
   cleared by that replica's `RaftRecovered`; a re-seed's quarantine is not cleared by
   anything, because a re-seeded server never votes again (D-035). Folding the two into
   one map was the first thing written here and it was wrong: on seed 8 a recovery
   cleared a quarantine, the range read as live, and the liveness check was asked of a
   run RAFT.md §2 withholds it from. The pair of sets is the tree's own reading, now per
   replica. Seed 8 is also the seed this tree reaches an *empty* live set on, and the
   helper's doc comment now says so: it told the story of seed 60 instead, carried over
   from D-030 and D-035, which described the release runs of their own day. Every schedule
   has been redrawn many times since and seed 60 of this tree has its range live
   (`live = {2}`), so the anecdote no longer reproduced. On seed 8 rot refuses server 2, a
   leader re-seeds it and the quarantine sticks, rot then refuses server 1 and no leader
   ever re-seeds it, and server 3 is left with nobody able to grant it a vote — D-035's
   priced-in deadlock, exactly what the two older entries described.
9. *`sim/membership.rs`'s own write bound is left alone.* §8 keys the checks about time
   where RAFT.md §2's run and cites the raft sweep's own lines for both the majority
   carve-out (sim/raft.rs:967-986) and the write bound (sim/raft.rs:990-998); the
   membership scenario's liveness is D-029's availability check, its bound is asked of
   every uniform seed with no carve-out, and keying it per key is an assertion that
   would need its own thousand-seed measurement on a scenario this build does not
   otherwise touch. It is keyed by the stage that runs that scenario on the node with
   four ranges, which is Stage B's own exit criterion.
10. *The pinned-seed straddle predicates keep the per-node skip.* `Report::isolation_term_straddles`
   and `isolation_received_straddles` describe the situation a seed was pinned for and
   read the isolated *server*'s term records; they skip an isolation in which any replica
   on the node was refused, re-seeded or finished installing (`reseeding_during_any`),
   which is the skip as it stood. The pre-vote check itself takes it per replica. A pin
   must read the window it was pinned on.
11. *A `RangeCreated` and a `RangeRemoved` are believed on sight, and neither `cause` is
   read.* Check 2 and check 4 take a creation's `floor_index` and `floor_term` as a floor
   exactly as an install's completion sets one, and a removal ends that replica's commit
   index and its applied memory exactly as a node's refusal ends a store's. §8 sanctions
   both, and both are an amnesty with nothing behind them. A replica that applies 1 and 2,
   traces `RangeCreated { floor_index: 9 }` and then applies 10 now passes where it failed
   "state machine safety: server 1 applied index 10 of group 2 after 2"; a `RangeRemoved`
   erases a commit index, so a later truncation below it passes too. Neither event's
   `cause` is read, so nothing here tells a legitimate split, merge or install from a
   bogus one: a replica that forged either event would launder its own violation past
   checks 2, 3 and 4. §8's **check 7** — a range's replicas agree on the sequence of its
   configurations, of which every creation and removal is a step — is what ties them down,
   and it is not built here: there is no membership change of a range for it to read yet
   (item 5, and the paragraph above on what emits these events, which is nothing). Until
   check 7 exists, the checks of this entry hold only against traces whose range events
   the code under test produced honestly. **The stage that emits these events inherits an
   unguarded amnesty and owes check 7 in the same PR.**
12. *Pre-vote's per-replica set is read from term records alone.* `Report::ranges_of` says
   which replicas of the isolated node the property is asked of, and a first draft read a
   `RangeCreated` on the node as well as a `RaftTerm`. **That arm is removed here.** It
   could catch nothing the `RaftTerm` arm does not — a replica that ever steps traces a
   term record, and a replica that never steps has no term to keep — and it could produce
   a catch that is simply wrong: for a replica with a creation and no term record,
   `term_by` reads 0 at both ends of the window while `created_term_in` makes the window's
   opening term the creation's `floor_term`, so the check would report a raise "from
   `floor_term` to 0" that no replica made. Deleting it leaves every sim unit test green,
   because the case written for it
   (`a_range_created_under_an_isolation_starts_at_its_creations_term`) also traces a
   `RaftTerm` for the created range and `ranges_of` scans the whole trace with no window —
   so the arm was untestable as well as useless. The creation is still read where it earns
   its place, for the opening term of a replica that *does* step (`created_term_in`,
   mutation row P2).
13. *A refusal marks the node down for every range of the whole run: a known
   over-impairment, left for the node's PR to close.* `ranges_with_a_majority_up` reads a
   `RaftRefused` as "every range this trace ever names is down on this server", where the
   honest reading is "every range this replica held at that moment". It over-impairs: a
   range created after the refusal, and a range the node never held, each count a replica
   down that was never there, so a range can read as short of a majority and be skipped by
   the checks about time that should have been asked of it. Inert today — every trace has
   one range and every node holds it — and silently weakening the day a node holds four.
   It is **not** fixed here, on purpose. The honest predicate needs to know what a node
   holds, and the only source for that in this tree is what the node happened to trace,
   which is empty at exactly the moment it matters: `RaftRefused` is traced when the start
   returns the loss, before the store is open and before the node has traced one record
   about any replica (`crates/ananke-raft/src/node.rs`, `Start::Refused`). A refusal at a
   node's first start would then impair nothing at all, its range would read as live, and
   the checks about time would be asked of a run RAFT.md §2 withholds them from — which is
   the failure item 8 above already cost this entry once, on seed 8. Under-impairing is
   the dangerous direction and over-impairing the safe one, so the safe one stands until
   there is range membership to read rather than guess: the PR that gives a node many
   ranges emits `RangeCreated` and `RangeRemoved` for real, and with them a refusal can
   mark down the ranges that node is known to hold at that point. **That PR closes this.**
14. *Check 4's mismatch message names the payload as well as the term.* The value the
   check compares at an index is `(term, payload hash)`, and the message printed only the
   two terms, so a disagreement over the *payload* alone — two replicas applying two
   different commands at one index of one term, which is the worst thing this check can
   see — read "index 1 of group 2 was applied as term 1 on one server and term 1 on
   server 1": true, and useless. It cost a reviewer of this PR three wrong diagnoses. The
   message now names both, in check 2's own vocabulary ("with different payloads"), and
   `one_group_may_not_apply_two_entries_at_one_index` asserts it on a term disagreement
   and on a payload-only one (mutation row 4f). The fault is older than this PR; it is
   fixed here because this PR keyed the line it is on.

**Measurements.**

- *The correct system, at 1 000 seeds in release on this tree*: green, every seed, with
  the per-key write bound, and green at the gate's twenty and CI's hundred. **The margin
  is worth knowing.** The raft sweep's coverage prints `slowest_write_after_heal`, and it
  now prints the figure the bound is asked against — the worst first completion of a
  write after the last heal over every key of every seed, where it printed the best of
  the keys. Over the same thousand runs, which are deterministic and so are the same
  runs either way, the best of the keys is 1.114473988 s and the worst is 1.785543304 s,
  against the bound of ten maximum election timeouts, 2 s: the per-key check runs 214 ms
  under it where the old one ran 886 ms under it. The bound is SHARD.md's and
  `LIVENESS_TIMEOUTS`'s and is not moved here; what is recorded is that a change which
  redraws the schedules has a *tenth* of the bound to play with on this check — 214 ms of
  2 s, one of the ten election timeouts, not two — where the old one had a little under a
  half, and that the next stage's four ranges per node will want this figure measured
  again.
- *Every variant keeps its standard at its tier*, the gate's twenty and the premerge's
  thousand, and every rate is the one D-069 recorded: `TruncateOnEveryAppend` 999 of
  1 000, `LeaseTrustsTheClock`'s stale read 40 of the 503 seeds whose drift exceeds the
  bound, with the guard revoking on every one of them. No catch moved, because no
  schedule moved and the checks say of one group exactly what they said before.
- *`scripts/premerge.sh` at a thousand seeds on this tree*: **green in 713.53 s** real
  (4 879.02 s user, 196.76 s sys), on the 8-core Apple M2 **on AC power** — the machine
  throttles on battery — with the load sampled every ten seconds through the run at mean
  29.64, minimum 12.11 and maximum 95.08, which is the premerge's own parallelism and
  nothing else of this session running. It is inside D-040's quarter of an hour. Against
  the figures beside it: D-071 changes no code that runs in a simulation, so this is a
  measurement of the machine and the tier, not of the change; the pair D-069 recorded
  says what a change costs, and nothing here needs that number.
- *And again at a thousand seeds after this entry's review fixes* (fc21689 and 8ce19a3, on
  the same 8-core Apple M2, again **on AC power**): **green in 625.01 s** real (4 465.90 s
  user, 180.21 s sys), with the load sampled every ten seconds at mean 12.70, minimum 9.38
  and maximum 16.16 over 63 samples — lower than the run above because that one built the
  tree first and this one had it built. The whole suite is green in release at CI's
  hundred too. The figure that matters is not the time: **nothing moved.** The raft
  sweep's `slowest_write_after_heal` is 1.785543304 s to the nanosecond, the same run it
  was above, and every variant's rate is the one recorded here and by D-069 —
  `TruncateOnEveryAppend` 999 of 1 000, `LeaseTrustsTheClock`'s stale read 40 of the 503
  seeds whose drift exceeds the bound with the guard revoking on all 503. A check whose
  message changed (item 14) and a check whose arm was deleted (item 12) leave the
  schedules exactly where they were, which is what the two runs being the same run says.

## PROPOSED D-072 — The node's wire: a frame of range-tagged messages, a per-peer outbox keyed by range and bounded in bytes, and an inbox bounded in bytes that makes room for the messages carrying data

**Context.** Stage B's third build (SHARD.md:2219-2232) is §4's node. This entry is its
first slice, the wire, and nothing else: "one socket; frames tagged with an 8-byte range
id (Q10) and cut into batch frames by a per-peer outbox under `MAX_FRAME_LEN`, with a
studio decoder that yields several messages per frame; one inbox per node bounded in
bytes with constant- or logarithmic-time admission (Q14)". The `raft` and `apply` tasks
and Q41's round, the snapshot task, bootstrap ranges and the four ranges per node in the
scenarios, Q15's refusal and re-seed, and follower compaction are each their own PR.
Nothing in `sim/` changes behaviour here and the single-group server runs exactly as it
did.

What the tree has. A frame is one message, `kind | from | term | fields`
(message.rs:1-8), and nothing on the wire names a group (SHARD.md:317-321). The inbox is
bounded by message count, 128 in the sweep, and drops the oldest heartbeat of any sender
first, then the oldest message of the arriving one's kind and sender — and **never** an
AppendEntries with entries or an InstallSnapshot chunk, which it admits over the bound
deliberately (node.rs:22-26, 2333-2337). That clause is not a defect: it is the guarantee
that a range's replication cannot be stopped by a flood of cheap messages, and what a
byte bound has to keep by some other means. What is a defect is the scan: `count` is a
linear filter and `remove_first` a linear search, called up to twice, so a tick costs
arrivals × capacity (SHARD.md:567-576; §11, raft item 11). And `ananke-shard` did not
exist. Q40 divides the crates; §11's raft items 1, 2 and 11 are
what this slice of them needs.

**Decision.**

*The crate as built.* `ananke-shard`, a member of the workspace like every other crate,
with its own `README.md` and copies of both licences, crate and module documentation,
`-D warnings` clean under `cargo clippy --all-targets --all-features`, and clippy.toml's
bans in force — `scripts/check-direct-io.sh` needed no change, since it scans `crates`
and `sim` whole. It depends on `ananke-env`, for `MAX_FRAME_LEN`, and on `ananke-raft`,
for the codec and the types; **`ananke-raft` does not depend on it** and still carries
**no range descriptor and no span**, which is what Q40 asks. It does name a range where
the trace needs one — `RaftConfig::range`, and `range` on every `Raft*` event (D-069) —
and that is a `u64` label on a group, not knowledge of what keys a range holds; where a
range's bounds, its splits and its placement live is this crate's side of the line. Four
modules: `range`, the id; `frame`, the batch frame
with its builder, its decoder and the studio's view of one; `outbox`; `inbox`. Nothing
here spawns anything or touches a clock, a disk or a socket: it is the wire's shape and
two queues, and the tasks that will drive them are the next PR's.

*`RangeId` is a newtype over `u64`, not a bare `u64`.* A range and a server are both
`u64`, the wire carries both, eight bytes apart, and a range where a server belongs is
the mistake this layer is most exposed to; the type system is the cheapest place to catch
it, and the mutation table below has two rows that are exactly that mistake. The trace
and `RaftConfig::range` keep the bare `u64` (D-069), so the field is public.

*The frame.* `version: u8 | count: u32`, then per message `range: u64 | len: u32 |
message`, everything little-endian as the message codec is. The message is exactly the
bytes `ananke_raft::Frame::encode` produces: the codec is wrapped, never re-implemented
(Q40). That is why the length is there at all — `Frame::decode` takes a whole frame and
refuses trailing bytes, so a wrapper that does not parse a message has to be told where
it ends, and the alternative, a streaming decode that walks each message's fields, is
`ananke-raft`'s parser moved into `ananke-shard`.

So a message costs **12 bytes beyond its own length**, Q10's 8 and 4 more, and a frame
costs 5. §4's arithmetic counts the 8 alone: a heartbeat 53 → 61, a response 66 → 74
(SHARD.md:487-489). On this wire they are 65 and 78 inside a frame of several, 70 and 83
in a frame of one. §4's largest idle frame, 222 responses of one node pair on one phase
at 10 000 ranges, is 17.3 kB where it says 16.4 kB — both four orders under
`MAX_FRAME_LEN`, so the extra 4 bytes change nothing §4 concludes. At 1 000 ranges, the
count Phase 3 builds for, an idle pair's frame is 22 messages, 1.7 kB.

*The cut, over a queue per (peer, range).* A flush is at most one frame per peer, built
in peer order, cut under the cap — §4's "one frame per peer per flush" read literally, so
that a flush's work and its bytes are both bounded, and since Q41's round flushes at
least once a round a backlog drains at the round's rate.

**The queue is keyed by (peer, range), not by peer, and the frame is cut round-robin over
the peer's ranges.** One queue a peer puts every range behind whichever range spoke
first: eight in-flight AppendEntries for one range hold every other range's heartbeat to
that peer behind them, up to eight flushes, which at Q41's 10 ms tick is 80 ms against a
minimum election timeout of 100 ms (core.rs:373-377) — one slow range starting elections
in the other 299 a node leads. Measured on this outbox: with a 24 kB frame cap, eight
4 KiB entry-carriers for one range against 299 ranges with one heartbeat each, **every
one of the 299 heartbeats leaves in the first frame** and the backlogged range leads that
frame too. Order within a range is the order it was queued in, which is all Raft asks of
it; order between ranges is the round-robin's, which is what stops one range spending
another's frame. A range whose next message does not fit the frame being cut is passed
over and leads the next flush's frame, so no range is passed over twice running. Since a
message that could not fit an empty frame is refused at the push, every flush of a
non-empty queue takes at least one message and a queue always drains.

*The outbox is bounded, per peer, in bytes.* The queue in front of the socket is bounded
— 1 024 frames a destination, oldest dropped, traced as `MessageDropped` (net.rs:9-11) —
and an unbounded queue in front of a bounded one bounds nothing: a peer that is not
draining grows the outbox without limit and the socket's bound is never reached. Measured
on the outbox as it was: 100 000 pushes with no flush, nothing refused and nothing
dropped. So each peer's queue holds at most `2 * MAX_FRAME_LEN` of frame bytes — the
frame a flush is about to cut and one behind it — and a push over that drops the *oldest*
message queued for that peer, whatever range it is about, handing it back so the caller
traces it exactly as the socket's own drop is traced. A message is never dropped for one
of another peer. Measured: 100 000 pushes into a 64 kB test bound, 98 992 dropped and
65 520 bytes queued, the bound never passed and every dropped message handed back. The
figure 2 × 16 MiB is a bound and not a tuning — at §4's largest idle frame, 17.3 kB, it
is about 1 900 rounds of headroom, and what a node's queues actually reach is the node's
stage's to measure, with the drops now traceable.

*A message larger than a frame is refused at the push*, with the peer, the range and its
length, and nothing is queued. It is not truncated; it is not split across frames, since
nothing on the receiving side reassembles one; and it is not queued to be refused later
by a socket that fails anything over `MAX_FRAME_LEN` anyway (net.rs:44-46). The caller is
told in front of the send it asked for, and the node's PR traces it. This is not a
theoretical case: the core batches up to `max_batch` entries, 64 (core.rs:343, 382), and
nothing bounds a command's size, so 64 commands of 256 KiB would make one. Snapshot
chunks never come through the outbox at all — they go in frames of their own on the
snapshot task's own socket handle (Q41) — so the 16 MiB cap is spent on entries and
nothing else.

*The inbox.* One per node, bounded in bytes, and a message's cost is **the frame bytes it
occupied** — its 12-byte tag, its own length, and for the first message of a frame that
frame's 5-byte header, so a frame's bytes are charged to its messages exactly and the
bound is the bytes received rather than the bytes received less five a frame. Admission
is per message, not per frame: a frame is admitted as far as it fits, in order. Nothing
is torn by a refusal, since the framing decides where a message ends before any of this.

*What it drops when it is full,* which §4 does not settle. This is **not the policy this
entry first proposed**. It proposed refusing the arrival and dropping nothing, and that
policy has two defects that its own probes did not look for:

- *A message larger than the whole bound is refused for ever.* An AppendEntries carrying
  a 64 KiB command costs 65 627 bytes of frame; against the 16 kB inbox §4's arithmetic
  suggests, it is refused **into an empty inbox**, and every retransmission of it is
  refused identically, because the arrival is the same size every time and the inbox is
  empty every time. That range never replicates again. Measured: 1 000 retransmissions,
  1 000 refusals. `ananke_raft::node` does not have this defect — it admits entry-carriers
  and InstallSnapshot chunks over its bound deliberately (node.rs:22-26, 2333-2337) — so
  the first wording of this entry recast that clause as a defect of the tree when it is
  the tree's liveness guarantee.
- *Refusing the arrival starves the messages that carry data, systematically.* A pressured
  inbox admits heartbeats indefinitely and refuses entry-carriers indefinitely: 63
  heartbeats fill a 4 kB inbox, and over 100 rounds every heartbeat was admitted and all
  101 entry-carriers refused. Heartbeats keep arriving, so nothing times out and no
  election repairs it; the commit index simply stops. "A refusal costs one retransmission,
  not a round" is true of one refusal and false of a standing one.

So the policy is **two rules and one guard**, which is what §11's raft item 11 asks for —
"a kept size **and an index of heartbeats by sender and range**", of which the first
wording built the size and dropped the index:

- A message that *carries data* — an AppendEntries with entries, or an InstallSnapshot
  chunk — is admitted by **making room**: the oldest heartbeat of the (sender, range) pair
  holding the most of them is dropped, and again until it fits. It is never refused for
  want of room a heartbeat is holding. This is what `ananke_raft::node` protects by
  admitting these over its bound; dropping heartbeats for them keeps the protection and
  keeps the bound.
- Any other arrival — a heartbeat, a vote, a response — is refused when it does not fit.
  It is the cheap one to lose: a heartbeat repeats every 20 ms against a minimum election
  timeout of 100 ms (core.rs:373-377), and a follower taking entries has its timer reset
  by the entries themselves, so heartbeats losing under pressure cannot start the election
  that entry-carriers losing would need to repair. What it gives up is a follower's timer
  reset and a leader's lease promise under sustained pressure, which is what the node's
  stage measures.
- And over both: **nothing is ever refused into an empty queue.** A message larger than
  the whole bound is admitted over it rather than refused for ever. The bound is then
  exceeded by an inbox holding exactly one message, which the next pop empties, so the
  overshoot is one message and not a growing one. Measured: 1 000 retransmissions, 1 000
  admissions, 0 refusals.

A carrier that no room can make fit, into a queue that is not empty, *is* refused — and
nothing is dropped for it, since whether the room can be made is decided before any of it
is made. That refusal is transient by construction: the queue holds real work, draining
it empties the queue, and the retransmission into the empty queue is admitted.

*The index* is §11's: a map from (sender, range) to that pair's queued heartbeats in
arrival order, and a ranking of those pairs by how many each holds, so the fullest pair's
oldest heartbeat is two lookups and no scan. Choosing the noisiest pair is what keeps one
range from being starved of its heartbeats by another's flood; ties go to the highest
(sender, range), a total order, so the choice is the same on every node and in every run.

*What is given up against today's policy* is that a heartbeat is lost where today a
message of the arriving one's own kind and sender would have been; and that an arrival
that carries no data can be refused where today something older goes for it. Both are
measured by refusals and drops per kind and per range, which the node's stage takes at
four ranges a node, together with whether any range's progress is held over an election
timeout by refusals alone.

*The studio.* moirae pairs a send with its delivery by `msgId`, so one frame is one
`send` line and its `msg` is one object. The decoder therefore puts the frame's messages
*inside* that object: `type` is `shard.batch`, with `count`, `ranges` — how many distinct
ranges the frame is about — and `msgs`, one object per message in the frame's order, each
the object `ananke-raft`'s own decoder makes of it with `range` inserted immediately
after `type`, as the trace writes `range` immediately after `server` (D-069). A frame of
six messages of three ranges reads in the studio as six messages and not as one, which is
what §11's raft item 1 asks for. The contract is stated where the next decoder will read
it, on `moirae::Decoder` and in that module's doc table.

**What §4 leaves open, settled here, each marked `// PROPOSED(D-072)` in the code.**

1. *The drop policy and the oversized message*, above: entry-carriers and snapshot
   chunks make room by dropping heartbeats, other arrivals are refused, nothing is
   refused into an empty queue, and a message larger than a frame is refused at the
   outbox's push.
2. *The cut leaves the overflowing range's message queued*, above: a flush is one frame a
   peer, cut round-robin over the peer's ranges, not as many frames as the queue needs
   and not one range's queue drained before another's is looked at. And the outbox is
   bounded per peer, in bytes, dropping the oldest.
3. *A message costs 4 bytes beyond Q10's 8.* The alternative is moving the message
   parser into this crate, which Q40 puts the other side of the boundary.
4. *Framing and content are refused separately, and `decode` now agrees with `studio`.*
   Framing that does not parse — another version, a torn frame, a count that does not
   match, trailing bytes — hides where every message of the frame ends, so `decode`
   refuses the frame whole and never reads a prefix. A frame whose framing parses but one
   of whose messages the codec refuses loses **that message only**, counted in
   `Decoded::malformed` so the caller can trace it. This reverses the first wording of
   this entry, which refused the frame whole for both. `slices` validates the framing
   without reading a message, so the messages beside a bad one are exactly as delimited as
   they were; and §4's largest idle frame carries 222 responses of one node pair, so
   refusing it whole turns **1 lost message into 222**, of up to 222 different ranges,
   for one message written badly. The argument for refusing whole — that a peer which
   disagrees about one message is not to be trusted about the rest — proves too much when
   the framing it wrote parsed exactly.
5. *A version byte.* One byte a frame, so that a frame from something that does not
   write this format is refused rather than read as a count and a range.
6. *Every message carries its sender, as `ananke-raft`'s codec writes it*, 8 bytes a
   message that are the same for every message of a frame. Hoisting it into the header
   would change that codec, which Q40 keeps as it is; it is 8 bytes against the 65 a
   heartbeat costs, and it is what lets a frame's messages be handed on one at a time
   without the frame beside them.
7. *The bound is in wire bytes, not in live heap.* A decoded command is a `Bytes` slice
   of the frame it arrived in (message.rs:324), so one admitted message can keep its whole
   frame alive. **A heartbeat cannot do it** — it holds no `Bytes` at all, so the frame
   beside it costs it nothing — which is what the first wording of this entry got wrong by
   illustrating the hazard with one. The message that pins a frame is the smallest one
   *carrying a payload*: an AppendEntries with one entry costs **86 bytes of the bound**
   with an empty command, and holds every byte of the frame it arrived in. Measured: a
   92-byte carrier holding a 798 883-byte frame, **8 683×**; and the ratio is bounded by
   `MAX_FRAME_LEN` over the smallest carrier, **195 083×**, which is the figure to put
   against the bound rather than "batching changes its size, not its kind". A single
   message a frame, as the server sends today, has the same exposure at a frame's size;
   batching multiplies the *ratio* by how many messages share the frame, because any one
   of them holds all of it. Bounding live heap instead would mean copying every message
   out of its frame at admission — a copy per message on the receive path — and the figure
   to decide that on is the inbox's live bytes against its bound, which the node's stage
   measures with the refusal and drop rates above. The bound this entry sets is what the
   node took off the wire, which is also what a sender can be held to.

8. *A frame's header is charged to its first message.* A message's cost is its tag, its
   own length, and for the first message of a frame that frame's 5 bytes, so a frame's
   messages' costs sum to the frame exactly and the bound is the bytes received rather
   than the bytes received less five a frame. The header goes to the first message because
   a frame is carried for its first message as much as for its last; a frame of no
   messages carries nobody and charges nobody.

9. *An empty message is refused by the builder.* `Frame::decode` refuses zero bytes, so an
   empty message is 12 bytes of frame that every reader of it drops. The caller has lost a
   message before the frame is cut, which is its bug; it is told with a panic, as it is
   for a message that does not fit.

10. *A closed inbox refuses.* Nothing will drain it, so `Admitted` there is a message lost
    under a word that says it was not.

11. *A range id above `i64::MAX` exports as a JSON string*, as every other `u64` the trace
    carries does (`ananke_raft::message::int`): JSON's numbers are `i64` and the
    alternative is a negative range id in the studio. Pinned by a test that reads the same
    value through both crates' exporters, so the two cannot drift apart.

12. *`encoded_len` saturates*, as `Builder::fits` does: a length no frame could hold has
    to compare as one rather than wrap to a small one that fits.

**Measurements.**

- *The admission's cost, in queue entries examined* — the figure Stage B's measurements
  ask for (SHARD.md §12). At lengths 1, 2, 4, … 4 096: an admission with room examines
  **0 queue entries at every length**, a refusal **0 at every length**, and one that makes
  room **1 entry per heartbeat it drops** — 1 at every length in the measurement, where
  one heartbeat is the room needed. Over a run of 2 000 rounds of mixed arrivals and
  drains: 2 667 messages admitted, **2 667 queue entries examined in all**, since every
  entry examined is a message leaving and a message leaves once. Against it: today's
  admission examines the whole queue once and up to twice more, so at the sweep's 128
  messages a tick's 200 arrivals cost about 25 600 entry examinations.

  **What makes the figure a measurement and not a structural zero.** The first wording of
  this entry counted probes only in `pop_front`, which `admit` never called, so the figure
  was zero for *any* admission, linear ones included — the review planted a linear
  admission through an accessor that did not count and it survived, still printing zero.
  The guarantee is now the shape of `Slots`: its whole surface is `push_back`,
  `pop_front`, `take_noisiest_heartbeat`, `len`, `heartbeats`, `heartbeat_bytes` and
  `probes`, with no iterator, no indexing, no `front`, no `get` and no borrow of a queued
  message, and each of the two methods that reaches a message counts it. So the only
  traversals that *can* be written are those two, and a test writes both — a drain, and
  one admission that has to take every heartbeat in the queue — and shows each costing one
  probe an entry, 64 at length 64, a figure that fails the bound the measurement asserts.
  That is the honest form of the claim: not "an admission examines nothing" as a law of
  nature, but "every entry any admission examines is counted, and the counted figure is
  constant".

- *The head-of-line probe.* Eight in-flight AppendEntries for one range against 299 ranges
  with one heartbeat each, to one peer, at a 24 kB frame cap: **every one of the 299
  heartbeats leaves in the first frame**, and the backlogged range leads that frame too.
  With one queue a peer they are behind all eight, up to eight flushes, 80 ms at Q41's
  10 ms tick against a 100 ms minimum election timeout.

- *The outbox's bound.* 100 000 pushes with no flush, into a 64 kB per-peer test bound:
  98 992 dropped, **65 520 bytes queued**, the bound never passed, every dropped message
  handed back to be traced, and another peer's queue untouched. On the outbox as this
  entry first proposed it the same 100 000 pushes refused nothing and dropped nothing.

- *The blocker and the starvation.* An AppendEntries carrying a 64 KiB command costs
  **65 627 bytes** against a 16 kB inbox: 1 000 retransmissions, **1 000 admissions and 0
  refusals**, where refusing the arrival gave 1 000 refusals. And under pressure — 63
  heartbeats filling a 4 kB inbox, then 100 rounds of a heartbeat and an entry-carrier
  arriving against a `raft` task draining two — **100 carriers admitted, 0 refused**, 14
  heartbeats dropped to make their room, where refusing the arrival admitted every
  heartbeat and refused all 101 carriers.

- *What a byte of the bound can pin*, item 7 above: a 92-byte entry-carrier holding a
  798 883-byte frame, 8 683×, against a bound of `MAX_FRAME_LEN` over the smallest
  carrier's 86 bytes, 195 083×. Asserted from the addresses, not from the lengths: the
  command is a slice of the frame it arrived in.
- *The frame's arithmetic*, above: 65 and 78 bytes for §4's heartbeat and response inside
  a frame of several, against the 61 and 74 §4 counts, and 17.3 kB for its largest idle
  frame against 16.4 kB. Asserted in `frame.rs`, from `Frame::encode` rather than from
  the table.
- *Ten planted bugs, ten caught, no survivors* — the table below. The harness is
  `scratchpad/stage-b/wire/mutate.py`: each row is one edit to the tree, `cargo test -p
  ananke-shard --all-targets`, the failing tests recorded, the tree restored.

| # | The bug planted | Caught by |
|---|---|---|
| 1 | The outbox tags every message of a frame with the first message's range | `a_flush_is_one_frame_a_peer_carrying_every_range_queued_for_it`, `a_message_that_would_overflow_a_frame_starts_the_next_one` — "left: [(RangeId(1), 0), (RangeId(1), 1), (RangeId(1), 2), (RangeId(4), 3)…]" |
| 2 | The decoder yields one message per frame and ignores the rest | Nine tests, the whole crate: `a_frame_carries_several_messages_each_with_its_own_range`, `the_studio_sees_six_messages_of_three_ranges_and_not_one`, `a_frame_s_messages_are_admitted_one_by_one_under_the_node_s_bound`, and the outbox's three |
| 3 | The cut counts a message without its 12-byte tag, so a frame goes over the cap | `a_message_that_would_overflow_a_frame_starts_the_next_one` — "a frame of 265 bytes over the cap of 260" |
| 4 | The inbox tests its bound before adding the arrival, so it admits past it | `the_bound_is_in_bytes_and_admission_never_goes_over_it`, and three more |
| 5 | A full inbox drops the oldest message it had admitted to make room | `a_full_inbox_refuses_the_arrival_and_keeps_every_message_it_admitted`, and three more |
| 6 | Admission counts the queued bytes over the queue on every arrival | `an_admission_examines_no_more_of_the_queue_as_the_queue_grows` **only** — "an admission at length 8 examined 8 entries, against 1 at length 1" |
| 7 | The inbox takes the sender's id for the range | `a_frame_s_messages_are_admitted_one_by_one_under_the_node_s_bound` |
| 8 | The studio shows every message of a frame under the frame's first range | `the_studio_sees_six_messages_of_three_ranges_and_not_one` |
| 9 | A message's cost to the inbox loses its tag | `a_frame_carries_several_messages_each_with_its_own_range`, `a_frame_s_messages_are_admitted_one_by_one_under_the_node_s_bound` |
| 10 | The outbox queues a message for the server whose id is the range's | The outbox's three tests |

Rows 6 and 7 are the ones that say the tests are not one test three times over: row 6 is
caught by the cost measurement alone, which is why that measurement is a test and not a
printed figure, and row 7 is the range/server confusion `RangeId` exists to make hard.

- *`scripts/gate.sh`*: green, as it must be before each of this PR's commits.
- *`scripts/premerge.sh` at a thousand seeds*: **green in 779 s, on AC power**, the run's
  own machine lines beside it, as D-070 asks:

  ```
  premerge: Darwin 25.6.0 arm64, Apple M2, 8 cores
  premerge: before, load 18.72/23.69/28.80, AC Power, no thermal warning recorded
  premerge: after, load 51.08/59.77/47.66, AC Power, no thermal warning recorded
  premerge: green at 1000 seeds in 779 s
  ```

  It was owed for two sessions: the machine was on battery while the wire was written
  (74 %) and while the review's findings were fixed (64 %), and this laptop throttles
  there, so a figure taken then would have been the incomparable kind D-070 exists to
  stop. Beside D-071's 625 s on AC at a mean load of 12.70, this run started at a
  one-minute load of 18.72 and ended at 51.08, which is the premerge's own parallelism on
  a machine that was already busy; what the two say together is that this PR did not move
  the tier, which is what the figure is for here. Nothing in `sim/` changes behaviour, no
  schedule moves, and `ananke-shard`'s own tests are 34 unit tests that run in 0.26 s at
  any tier.

## PROPOSED D-073 — The node's tasks: one `raft` task over every core in Q41's round, and one `apply` task over every range

**Context.** Stage B's third build (SHARD.md:2219-2232) is §4's node. D-072 built its
wire. This entry is the next slice, the **tasks and the round**, and nothing else: "one
`raft` task stepping every core on one ticker in Q41's round, every output after a
core's `Persist` executed when that core's own persist resolves and its messages and
ticks held until then; one `apply` task (Q14)". The snapshot task keyed by range and
follower, bootstrap ranges and the four ranges per node in the scenarios, Q15's refusal
and re-seed, and follower compaction are each their own PR. The single-group server in
`ananke-raft` runs exactly as it did; nothing in `sim/` changes behaviour; the node runs
one range, `node::SINGLE_GROUP`, until the slice that switches the sweeps to it.

What the tree has. One server is one group: `incarnation` (node.rs:854-1285) owns one
`Raft`, one ticker and one `Server::execute` (node.rs:2432-2562) that runs a step's
outputs in order, awaiting `store.persist(..)` before everything after it. That order is
right for one core and wrong for many: it makes every core on the node wait on every
other core's disk. Q41 says what replaces it, and §4 sets it out in full
(SHARD.md:384-425).

**Decision.** `ananke-shard` gains three modules beside the wire.

`round` is the discipline alone — no clock, no socket, no disk — so the order can be
asserted without a simulation. `Cores` holds every range's core keyed by `RangeId` and
hands back a `Round`: the outputs that leave **before** the round's sync (those that
precede a core's `Persist`, and all outputs of a core that persisted nothing), and the
persists the round submits **together**. Everything a core produced after its `Persist` —
its sends, `Apply`, `ReadReady`, `ReadDropped`, snapshot actions and its trace events —
is held for that core and executed when *that core's own* persist resolves, and that
core steps no further until then. The work handed to a waiting core meanwhile is held in
arrival order, one entry per missed tick, and replayed in that order when its persist
resolves, every missed tick stepped and none collapsed; the replay stops at the first
step that persists again.

`node` is the two tasks. `Node::raft` is one loop over a three-way race of the inbox,
the ticker and the outstanding persists: a round is a tick's steps, or the messages
drained since the last round. It never waits on a persist — while a sync is outstanding
it goes on stepping the cores that persisted nothing, so a later round's persists can be
submitted behind it (SHARD.md:519-543). `Persists` holds the round's persists side by
side; the round submits them all and then arms them in one synchronous pass with no
await between, so a round's records reach the WAL writer together and are never split
across two of its groups, and the writer syncs them once (wal.rs:16-20, D-018). A round
submitted while an earlier sync is outstanding joins the group that sync's records did
not take, which is §4's loaded case (SHARD.md:519-533). `node::apply` is one task per node taking every range's jobs one at a time, in
the order they were queued, whatever the range: Q14's rule, and D-036's consequence that
one range's take holds every range's applies. Sends leave through D-072's per-peer
outbox, one frame per peer per flush.

`variant` is the node's known-buggy variants. They are a set of their own, not more of
`ananke_raft::Variant`: the core is untouched, they are not Raft bugs, and they are
outside §10's count. The node is not under the sweeps until the slice that puts it
there, so each is caught by a deterministic check in the crate. That is what CLAUDE.md's
pair rule asks — the buggy variant is *seen to fail* the check the correct code passes —
and D-061's tier rule does not apply, because it is a rule about a *sweep's* assertion
and there is no rate to measure in a check that is a single deterministic run.

`Host` and `Applier` are what the tasks need of the node's disk, socket and clients. The
node is the schedule; the host is what it drives. Splitting them is what lets a check
drive the round with a host that resolves persists in an order it chooses, which is the
only way to assert Q41's rule that each core's later outputs wait on that core's own
persist and on no other's.

**What §4 leaves open, settled here, each marked `// PROPOSED(D-073)` in the code.**

1. *The order several ready persists resolve in* (`node.rs`, `Persists`). §4 says each
   core's outputs follow its own persist and says nothing about ties. A fixed order —
   range order — would let one range's persists always be seen before another's, which
   is exactly what the round exists to prevent. The bit comes from the environment's
   scheduling stream, as `race` draws it, and only where there is a choice: a node with
   one range draws nothing, so its schedule is the one-group server's and no pinned seed
   moves.
2. *Which round a resolution's flush belongs to* (`node.rs`, `Frames`). A round flushes
   once before its sync and again as each of its persists resolves. The replayed work's
   own early outputs ride the resolution's flush rather than taking one of their own:
   both are correct, since the discipline forbids only flushing a core's later outputs
   *early*, and one flush is the option that puts fewer frames on the wire. The
   resolution's flush is credited to the round whose persist resolved, which is what
   makes "frames per peer in a round" a figure with an answer.
3. *Whether a held message keeps its charge against the node's bound*
   (`inbox.rs`, `Inbox::hold_at`). §4 says a message for a waiting core "is still taken
   from the inbox and held for that core, counted against the node's byte bound (Q14)",
   and D-072's inbox releases a message's bytes when it is popped. The node tells the
   inbox what it holds, as a whole figure rather than a delta, because the node knows
   what it holds and a lost increment would leak the bound away. D-072's rule that
   nothing is refused into an *empty queue* is narrowed to match, which is **PROPOSED
   D-074**: without that, the rule and this one together make the bound bind nothing,
   because the `raft` task drains the queue to empty on every wake.
4. *Where the `net` task's receipt comes from* (`node.rs`, `received`). D-050's stamp is
   taken by the `net` task in the one-group server and carried on the inbox entry.
   D-072's `Received` does not carry one, so the node takes the stamp as it hands the
   message to its core. Taking one reads the time and nothing else (D-047), so it moves
   no schedule; the conservative reading is that it is a receipt of the wrong moment by
   however long the message waited in the inbox, and the fix belongs on `Received`, in
   the slice that next touches the wire.
5. *The outbox's drops* (`node.rs`, `Node::act`). `TraceEvent` has no kind for a message
   the outbox dropped over its per-peer bound or refused as oversized (D-072); the node
   counts both on `Frames` and traces neither. Adding an event kind moves every pinned
   trace hash, and nothing would emit it until the node is under the sweeps: the event
   belongs with that slice, where it can also be seen to fire. This is the one place
   this slice is knowingly short of "every state transition that matters emits a trace
   event", and it is short of it in the crate's own counters, not silently.
6. *A persist that fails*. The task fails the node — `Host::failed` and the error
   returned — as `Server::execute` does today (node.rs:2432-2562). Not open, recorded
   because a node holding many ranges fails all of them at once, which is Q15's
   territory and the slice after next's.

**The pair, for each variant (CLAUDE.md:52-67).** Each is built beside the correct round
and caught by the same check that asserts the correct round's order.

| variant | what it does | the check that catches it |
|---|---|---|
| `DeferredFlushedEarly` | a core's outputs after its `Persist` go with the round's early ones | `a_cores_later_outputs_wait_on_its_own_persist_and_on_no_others` (the response leaves before either persist resolved) and `the_applies_reach_the_task_after_their_persists_and_partition_the_log` |
| `StepWhilePersisting` | a waiting core is stepped anyway | `every_tick_a_core_missed_is_stepped_when_its_persist_resolves` (nothing is held, no tick is replayed) |
| `CollapseHeldTicks` | the missed ticks become one | the same check (one tick replayed where three fell due), `the_ticks_a_core_missed_reach_its_timer_and_not_only_its_counter` (the follower's election timer is short by the whole sync, so it does not campaign when the sync resolves) and `the_variant_collapses_the_missed_ticks_into_one` |
| `PersistsOneAtATime` | each persist pays a sync of its own | `a_rounds_persists_are_submitted_together` (the second reaches the writer only after the first resolved) |
| `HeldNotCounted` | a held message stops counting against the bound | `what_the_node_holds_fills_the_nodes_bound` (the node refuses no arrival against a bound of four messages and holds every one of them) |
| `PersistsNotArmed` | the round's persists are submitted but left unpolled | `a_rounds_persists_reach_the_writer_before_the_task_takes_anything_else` (on some seeds the round's record reaches the writer only after the task has taken a tick and shipped a later round's frame) |
| `AppliedNotAdvanced` | the index handed to the `apply` task is not remembered | `the_applies_reach_the_task_after_their_persists_and_partition_the_log` (the jobs are `[1]`, `[1,2]`, `[1,2,3]` where they should partition the log) |
| `TakeToSnapshotTask` | a take goes to the `snapshot` task, not the `apply` task | `every_output_of_a_step_goes_where_section_four_sends_it` (the take never reaches the apply queue, so D-036's stall stops holding) |

Three of these — `PersistsNotArmed`, `AppliedNotAdvanced`, `TakeToSnapshotTask` — were
added by the adversarial review's findings 7, 5 and 6: the behaviours were in the code
and stated in its documentation, and nothing in the crate distinguished them from their
absence. `PersistsNotArmed` is the one variant whose catch depends on the scheduling
draw, because what it breaks is which side of the task's three-way race is polled first;
its directed scenario is a fixed set of 24 seeds, on all of which the correct node
submits before its next note and on some of which the variant does not (D-061's rule for
a new variant with no rate to measure).

**Measurements.** Machine: Darwin 25.6.0 arm64, Apple M2, 8 cores, AC Power. Other
agents were building in parallel on it, so the load averages are recorded beside each
figure.

*A core step's cost*, release, host time, outside the simulator (SHARD.md §12). The
adversarial review's finding 8 was that the figures here were not reproducible and this
entry recorded them as if they were — a *quieter* machine gave a figure 53 % higher. It
was right, and the harness was changed for it: each shape now runs **five** times and the
figure reported is the **lowest**, which is the statistic to take on a shared machine
(every run is the step's cost plus whatever interference it met), with the spread printed
beside it. The shapes were re-measured on the fixed tree, three times over, and the runs
are all recorded because the spread between them is the point:

```
cargo run --release -p ananke-shard --example step-cost
                  run A            run B            run C
                  load 29.8        load 28.1        load 25.3 (39.3 by the end)
idle tick         42 (42..86)      29 (29..59)      73 (73..91)     ns/step
leader tick       55 (55..183)     27 (27..134)     65 (65..186)
heartbeat in     107 (107..135)    41 (41..46)     141 (141..251)
response in      306 (306..345)   119 (119..122)   373 (373..642)
loaded           561 (561..831)   235 (235..251)   269 (269..313)
```

Take the lowest of the three, **29 ns an idle step**, and read the rest as a range:
29–73 ns idle, 27–65 ns a leader tick, 41–141 ns a heartbeat in, 119–373 ns a response
in, 235–561 ns loaded. The *loaded* shape also changed (finding 9): it used to propose a
million commands into a log that never compacted — the example printed "1 001 001
entries" — which measured a growing `Vec` as much as a step, and was the figure that
moved most. The leader is now compacted every 9 500 commands, which is §4's ~608 KiB log
at 64 bytes a command, and the example ends with a log of 4 001 entries.

**The pass bound is an idle step below 20 µs**, computed from §4's constants — 500 steps
in a 10 ms tick at 1 000 ranges (SHARD.md:504, 512-517), not measured. Every run of every
shape is **two orders of magnitude** under it, the lowest and the highest alike; stating
the margin as an order of magnitude rather than as "625 times" is finding 8's other half,
because the ratio is a ratio of one machine's run to a computed constant and it moved by
a factor of 2.5 between runs of the same tree. **PASS**, and nothing goes to the owner on
this figure. One `raft` task per node holds 1 000 ranges' idle ticks in 0.1–0.4 % of a
tick, and 10 000 ranges' in 1.5–3.7 %. The two tick shapes run on a core whose election
timeout is set long, so that every tick measured is the tick that does not time out — the
tick almost every one of a node's 300 replicas takes on any given tick of a steady
cluster. Without that the follower times out after ten ticks and the figure becomes an
election storm's, which is a different step and not one of §4's 500.

*The frames per peer in a round with persists* (SHARD.md:490-492, which leaves it to this
stage). A round flushes once before its sync and once as each of its persists resolves,
so a round with `p` persisting cores costs, per peer, one frame at each flush that has
sends for it: **up to 1 + p frames per peer per round**. §4 asks how many flushes that
is, saying it "depends on how the group commit resolves the round's persists": it is one
per persisting core *even when one group commit resolves them all*, which
`a_shared_sync_still_costs_one_later_flush_per_persisting_core` measures — two cores
whose persists resolve at the same instant cost three flushes and two frames to the one
peer, not one. That check measures the `p`; the `1` is measured by
`a_round_that_sends_before_its_sync_costs_that_frame_too`, a round in which one core
answers a pre-vote without persisting and another appends, which costs two flushes and
two frames to the one peer — the pre-sync flush's and the resolution's. (The review's
finding 10: the earlier entry claimed the first check measured `1 + p`, and it measured
`p`, because both its cores persist on their first step and its pre-sync flush ships
nothing.) An idle tick's round persists nothing and costs the one frame §4 counts
(SHARD.md:485-492); a round in which 100 of a node's leader replicas persist costs up to
100 frames to each peer. **This is what the slice asks the owner** — see below.

*The replay burst after a slow persist* (SHARD.md §12): the ticks a core replays once
its persist resolves, times the cores held, times a step's cost, against the 10 ms tick
at 1 000 ranges. A core behind a sync of `s` holds one tick per 10 ms of it, every one
stepped; a node at 1 000 ranges holds 300 replicas (SHARD.md:504). At the lowest and the
highest idle figures measured, 29 ns and 73 ns:

```
sync      ticks replayed   burst at 29 ns     burst at 73 ns
    2 ms          0        0.00 %             0.00 %
   20 ms          2        0.17 %             0.44 %
   80 ms          8        0.70 %             1.75 %
  200 ms         20        1.74 %             4.38 %
 1000 ms        100        8.70 %            21.90 %
```

The burst first fills a whole tick at a sync of **4.6 s** on the slowest of the three
runs and 11.5 s on the fastest. The sweep's disk operations are 100 µs to 2 ms
(sim/raft.rs:2807-2808) and the sync §4 calls as bad as a crash is 80 ms, so the burst
costs **0.7–1.8 % of a tick** at every latency the tree models. It does not break the
tick budget, and nothing goes to the owner on this figure either. (The earlier entry
quoted 0.77 % to two decimals off one run; the range is what the figure supports.)

**What it asks of the owner.** One thing, and it is the frames figure, not a bound.

> A round with `p` persisting cores costs up to `1 + p` frames per peer, because §4's
> rule is read literally: each core's later outputs are flushed *when that core's own
> persist resolves*, one flush per resolution, even when one group commit resolved the
> whole round at one instant. At 1 000 ranges a write burst touching 100 of a node's
> leader replicas therefore costs up to 100 frames toward each peer in that round,
> against the one frame an idle tick costs. Coalescing the resolutions that are ready in
> the same poll into one flush would cut that to one frame and would still be correct —
> every core's outputs would still follow its own persist — but it is a change to when
> outputs leave, which is a scheduling decision and not this slice's to make. **The
> recommendation** is to leave it as built through Stage B, which is the conservative
> option and the one §4's words say, and to decide it before Stage C with the figure
> measured on the sweeps' real traffic, where the node runs four ranges and the number
> of cores that persist in one round is a measurement rather than an argument.

**What the adversarial review changed.** One blocker and five majors, all in the tree
now, with the mutation that proves each fixed. The blocker is its own entry, D-074.

| # | finding | what changed | proved by |
|---|---|---|---|
| 1 | the node's byte bound bound nothing: the task drains the queue to empty, so D-072's empty-queue exemption fired at every arrival | **PROPOSED D-074** — the exemption now also asks that the node hold nothing | mutation `F1-revert-empties` (the exemption as it was) is caught by `what_the_node_holds_fills_the_nodes_bound` |
| 2 | the `HeldNotCounted` check asserted the figure the node had just written, not the bound | the check now runs a 256-byte bound against eleven arrivals and asks the *inbox* which got in | `M1` (`charged()` returns `self.bytes`) — a survivor before, caught now |
| 3 | "every missed tick stepped" was asserted by a counter; nothing saw the tick reach the core | `the_ticks_a_core_missed_reach_its_timer_and_not_only_its_counter`: the follower's election timeout is four ticks and its sync spans six, so the correct node campaigns at the resolution and a node whose timer stood still does not | `M2` (count the replayed tick, do not step it) — a survivor before, caught now |
| 4 | a deferred `Apply` read the core *after* the replay had stepped it, and `entries_to_apply` dropped missing indices silently while `applied_sent` advanced past them | the entries are read at the step that named them and carried on the `Act`; a missing index is `Err(index)` and fails the node | `F4-gap-passed-over` (the old `filter_map`) is caught by `the_entries_an_apply_names_are_the_ones_not_handed_out_yet`. The *scenario* in which the gap arises needs a compaction path the node does not have yet — issue #79 |
| 5 | nothing caught the node re-applying its whole log from index 1 | `Note::Applied` carries the job's indices and the check asserts they partition the committed log; `AppliedNotAdvanced` is the variant | `M7` (`applied` never advances) — a survivor before, caught now |
| 6 | "a take is the `apply` task's" was stated three times and checked nowhere | `every_output_of_a_step_goes_where_section_four_sends_it` drives `Node::act` over every output kind and asserts where each lands; `TakeToSnapshotTask` is the variant | `M11` (route the take to `snapshot`) and `M18` (drop `Output::Rejected`) — both survivors before, both caught now |
| 7 | the third commit's `arm().await` had no check that distinguished armed from unarmed | `a_rounds_persists_reach_the_writer_before_the_task_takes_anything_else` over 24 seeds; `PersistsNotArmed` is the variant | `M4` (delete `arm().await`) — a survivor before, caught now |
| 8, 9 | the step-cost figures were not reproducible, and the *loaded* shape measured a leader's log growing to a million entries | five runs a shape, the minimum reported with its spread, the margin stated as an order of magnitude; the loaded leader compacts every 9 500 commands | the three runs recorded above, which span 29–73 ns for the same tree |
| 10 | the "1" of `1 + p` frames per peer was never observed | `a_round_that_sends_before_its_sync_costs_that_frame_too` | `M20` (discard the pre-sync flush's frames) — a survivor before, caught now |
| 11 | stale `ananke-raft` citations in the new code and in this entry | corrected here and in `round.rs`; SHARD.md carries the same two and is shared with the parallel slices — issue #80 |  |
| 12 | `Round::persists` documented "in range order", which is only a tick's round | documented as the order the round's cores persisted |  |
| 13 | dead public API | `ApplyWork::Retake` dropped; `Round::sends_early` now feeds `Frames::rounds_sending_before_their_sync`; `Inbox::charged_bytes` is what finding 2's check reads |  |
| 14 | this entry said the inbox had twelve tests | it has fourteen |  |
| 15 | a message for a range the node does not hold was dropped in silence | `Meters::messages_for_ranges_not_held`, with a check | `F15-silent-unrouted` is caught |
| 16 | `Output::Rejected` and the resolution path's `hold_at` were unasserted | the routing check covers the first, the bound check covers the second | `M18` and `M12` — both survivors before, both caught now |

`M6` (`take_ready` always takes index 0) is left a survivor on purpose: it is point 1
above, the draw that happens only where there is a choice, and a node with one range
never has one. It is a PROPOSED point, not a defect.

**What moved.** Nothing. No schedule moves and no pinned seed moves: `ananke-raft`,
`ananke-env`, `ananke-storage` and `sim/` are untouched but for RAFT.md, the new code
runs in no scenario yet, and the one draw the node makes from the scheduling stream is
taken only where two or more persists are outstanding, which a one-range node never has.
`Inbox`'s arithmetic gained a `held` term and D-074's narrowing of the empty-queue
exemption; the `held` term is zero everywhere D-072's own fourteen tests reach it, so the
narrowing changes none of them and all fourteen are unchanged and green. So there is no
re-audit to do, and this entry says so rather than leaving it unsaid.

**What is not done, and why.** The node is not wired to a `RaftStore`, a socket or the
scenarios: `Host` and `Applier` are the seam, and the slice that switches the sweeps to
the node implements them over the store, the engine and the `net` task. The `snapshot`
task, bootstrap ranges, four ranges per node, Q15's whole-node refusal and follower
compaction are each their own PR, as the plan sets out. The measurements the node's
entry also owes — the inbox's drops under its byte bound, the apply lag, how long one
range's take holds the node's other ranges' applies, and the trace records per range per
virtual second — are all measurements *under the sweep's client load*, which needs the
node in the scenarios: they belong to the slice that puts it there, and this entry
records the three that do not. Two of the review's findings are filed rather than fixed
because fixing them here would widen the slice: **#79**, the directed scenario for the
applied-stream gap, which needs a compaction path the node has not got until D-065's
slice; and **#80**, SHARD.md's two stale `ananke-raft` citations, which is a shared file
the parallel slices are also in.

---

## PROPOSED D-074 — Nothing is refused into an empty queue, *while the node holds nothing*

**Context.** D-072's inbox has one rule over both of its drop policies: **nothing is
ever refused into an empty queue**. A message larger than the whole bound is admitted
over it rather than refused for ever — an AppendEntries carrying a 64 KiB command costs
65 622 bytes against a 16 kB inbox, and refusing it refuses every retransmission of it
identically, so the range it is about never replicates again. The bound is then exceeded
only by a queue holding exactly one message, which the next pop empties.

D-073 point 3 added the other half of the node's bound: a message taken from the inbox
and held for a core whose persist is outstanding is **still counted against the bound**
(SHARD.md §4, Q14). The node tells the inbox what it holds; the inbox charges the queue
and the hold together.

The two together bind nothing. `Node::raft` drains the queue to empty on every wake —
`while let Some(next) = inbox.take()` — and hands everything to `Cores`, which holds what
it cannot step. So the queue is **empty** at the moment the next frame arrives, every
time, and the exemption fires at every arrival whatever the hold says. The arithmetic
that charges the hold is correct and the real path never reaches it. A node behind a
slow sync accumulates one held message per arrival, without limit, for the whole sync:
at 10 000 ranges and 2 000 messages a tick (SHARD.md:504) an 80 ms sync — the one §4
calls as bad as a crash — takes 16 000 messages out of a bounded inbox and refuses none
of them. This was the adversarial review's finding 1 on the tasks slice, reproduced
end-to-end: against a 256-byte bound the node held 19 136 bytes, 74 times the bound, and
refused 0 of 300 arrivals.

**Decision.** The exemption is narrowed to what it is for: a message no *emptying* could
make room for, at a node that is holding nothing.

```rust
let empties = inner.held == 0
    && (inner.items.len() == 0
        || (room > 0 && inner.items.len() == inner.items.heartbeats()));
```

Nothing else changes: the two drop policies, the heartbeat victim index, and the
arithmetic are D-072's. With the term, what the node holds is bounded by the bound —
each arrival that is held was charged before it was admitted, so the hold cannot pass
the bound by more than the one message the exemption ever lets through.

**Why this and not a cap on the hold.** The other way to bound it is to stop draining
into the hold once `Cores::held_bytes()` reaches the bound, leaving later arrivals in the
queue where D-072's arithmetic already binds them. That is a change to the `raft` task's
round — the round is *the messages drained since the last round*, and this would make it
"the messages drained until the hold is full", a scheduling decision. The narrowing is a
change to an admission test that was already conditional, so it is the smaller and the
more conservative of the two. It is also the one that keeps the bound a property of the
inbox, which is where a check can reach it.

**What it costs.** A message larger than the whole bound, arriving while the node is
holding something, is refused where D-072 would have admitted it. It is not refused for
ever: what the node holds drains when the sync resolves — a persist resolves or the node
fails (D-073 point 6) — so the retransmission that finds the node holding nothing is
admitted under D-072's rule unchanged. The cost is a delay of at most one slow sync on a
message that only a node already at its bound would meet, against a node that otherwise
has no bound at all while a sync is outstanding. The conservative reading, and the one
taken here, is that a bound that binds is worth a retransmission.

**Recommendation.** As built. If the owner prefers the hold to be capped in the `raft`
task instead, that is a scheduling decision and belongs with the slice that puts the node
under the sweeps, where the drops under the byte bound are measured rather than argued
(SHARD.md §12).

**The pair (CLAUDE.md:52-67).** `NodeVariant::HeldNotCounted` is the variant: it takes
the message and stops counting it. The check is
`node::what_the_node_holds_fills_the_nodes_bound` — a 256-byte bound, a core behind a
65 ms sync, and eleven 64-byte arrivals fed one a millisecond. The correct node holds
four of them, is charged 256 bytes, refuses the rest, and admits again once the sync has
resolved and released what it held. The variant holds all eleven and refuses none. The
check fails on the code as it stood before this entry, which is finding 1; the same check
also kills the review's mutation M1 (`charged()` returns `self.bytes`) and M12 (no
`hold_at` on the resolution path), which both survived the slice as merged.

**What moved.** Nothing outside `crates/ananke-shard`. D-072's fourteen inbox tests never
set `held`, so `held == 0` holds throughout them and the added term changes no decision
any of them makes; all fourteen are unchanged and green. `sim/` does not depend on
`ananke-shard`, no schedule moves and no pinned seed moves.

---

## PROPOSED D-075 — The `snapshot` task keyed by range and follower: one assembly per (range, sender), paths keyed by range, and every install D-066's live install

**Context.** Stage B's node bullet asks for "one `snapshot` task keyed by range and
follower, one assembly per (range, sender) under per-node receive caps, no cap on streams
sent, and chunks in frames of their own on its own socket handle (Q14, Q41); staging,
version directories and their sweep keyed by range" (SHARD.md:2224-2229; §11, raft 14).
D-066 decided what an install on the node is. Today all of it is one group's: one staging
directory per engine directory (snapshot.rs:96-101), a version directory named by index and
take alone (snapshot.rs:119-121), one `Assembler` per snapshot task holding one stream and
abandoning it for a chunk of another identity (snapshot.rs:1018-1027; node.rs:1430), and a
sweep that deletes every unpinned version directory the store's single snapshot record does
not name (snapshot.rs:193-228). On a node each of those is wrong in a way a single-range
world cannot show: two ranges' takes at one index collide, one range's sweep deletes
another range's checkpoints, and the re-seeds heading for one node restart each other.

**What this entry builds.** `ananke-shard`'s `snapshot` module: the task's discipline over
keys, caps, frames and switches, with no clock, no socket and no disk, asserted the way
`round`'s is (D-073). The streams' bytes, the engine call an `Install` describes
(`Engine::install_spans`, D-068) and the trace events are the node's wiring, which the
slice that puts the node under the sweeps carries.

- *Keyed both ways.* Sends are keyed by (range, follower) and receives by (range, sender).
  Nothing caps the sends, so a leader feeds every designated follower of a range at once
  (Q14, D-043). A per-node cap bounds the assemblies; a (range, sender) over it is answered
  with a restart, writes nothing and disturbs no admitted assembly, and takes a slot by
  asking for it again once one is free. This is the re-seed shape's "wait for one another"
  (SHARD.md:2257-2259). The send half names the directory its followers stage under from
  the node's *own* id, because that is the sender the receiver keys the name by.
- *Paths keyed by range.* `version_name` is `snap-r<range>-<index>-<take>` and `staging_name`
  is `staging-r<range>-s<sender>`. `Snapshots::sweep` is one range's sweep: it proposes for
  deletion only version directories of the range it is sweeping, and among those only the
  ones that range's own record does not pin.
- *Chunks in frames of their own.* `Snapshots::route` builds a frame carrying exactly one
  message, the chunk, for the task's own socket handle; a chunk that would not fit a frame
  is refused as the outbox refuses an oversized message, never split (Q41).
- *Installs are D-066's.* A completed stream yields an `Install`: the range's two key
  intervals, sorted and disjoint (D-068), the staged source, the repair carried in the
  switch, no engine reopen, and `adopted: false`. A range the task was never told it hosts
  has no spans to switch and fails here rather than at the switch, and a chunk that starts
  its assembly over never completes a stream, whatever its `done` says: the directory it
  would install from has just been cleared. `RaftAdopted` is `Snapshots::adopt_fresh` alone
  — a node taking a fresh directory after a whole-node refusal, traced before any range
  installs into it, which the task asserts against the installs it counted for that
  directory rather than asserting nothing and reporting a constant.

**What the design documents left open, and the conservative choice taken.**

1. *Staging keyed by range, or by range and sender?* §11's raft item 14 asks for both
   "staging, versions and the sweep need keying by range" and "one assembly per (range,
   sender)". With two senders of one range — a leader and the stale leader it replaced —
   those two are only consistent if the staging name carries the sender too: two assemblies
   in one directory write over each other's files exactly as two ranges would. *Taken:* the
   staging name carries both. It is the stricter of the two readings and keys nothing
   together that the design wants apart.
2. *The receive cap's default.* Q14 asks for "per-node caps on streams received and
   assembled" and fixes no number; the re-seed shape sets two, below its four ranges, on
   purpose. *Taken:* no default at all — `Snapshots::new` takes the cap — with the
   recommendation that a scenario which is not about the cap sets it at or above the node's
   range count, so no stream waits by accident. A wrong default would be invisible; a cap
   that must be named is not.
3. *What a stream over the cap is told, and who gets the slot when one frees.* Nothing
   settles whether an over-cap stream is refused, queued or admitted by evicting another.
   *Taken:* refused with a restart, nothing evicted. Eviction would make the node the thing
   that restarts streams, which is the bug §11 names. *Amended after the review:* the freed
   slot is **granted to a stream that asks for it**, not reserved for the waiter at the head
   of the queue. A reservation is held for a (range, sender) that may never send again — a
   waiter's leader can change while it waits, which is the case this whole slice exists for
   — and nothing here has a clock to reclaim it, so a node under §12's shape fills both its
   slots with reservations for a departed sender and re-seeds nothing more, for ever. Strict
   FIFO by ask-order cannot be had without a clock: the queue keeps the order the waiters
   asked in, and reports it as `ahead`, but the order holds only among streams that are
   still asking. A standing wedge is worse than a fair order, and `SlotReservedForWaiter`
   keeps the reservation beside the correct code.
4. *The one-group layout's directories.* `snap-<index>-<take>` belongs to no range.
   *Taken:* it parses as no range's, so no range's sweep proposes it for deletion. A sweep
   never deletes a directory it cannot attribute; what becomes of an engine directory that
   predates the node is its start's business, not its sweep's.
5. *When overlapping spans are refused.* `Engine::install_spans` refuses them at the switch
   (`SpansOverlap`, D-068). *Taken:* `Snapshots::host` asserts sorted, disjoint, non-empty
   spans when the range is registered, so a node that has lost track of what a range holds
   fails where it lost track, not at the switch of an install.
6. *D-066's open question about `sim/quorum.rs`'s cap* — "set at or above its ranges, or
   D-049's rule counts a queued stream as progress" — is not this slice's to close, since
   that scenario is Q15's slice. *Recommendation:* set the cap at or above its ranges.
   D-049's rule is a variant's catch condition, and changing what counts as progress to suit
   a scenario's cap weakens the rule the variants are caught by.
7. *What becomes of the chunk that restarts an assembly.* RAFT.md:203-207 says a change of
   identity "starts the receiver's staging over" and that a stream told to start over is
   "restarted from its first byte"; the one-group receiver keeps the restarting chunk only
   when it is that stream's first (offset 0) and asks for a restart otherwise
   (snapshot.rs:1018-1027). Nothing here carries a chunk's offset, so the node cannot tell
   the two apart. *Taken:* a restarting chunk is discarded with the assembly it restarted,
   the directory starts over empty, and the sender is asked for the stream from its first
   byte — including when that chunk says it is its stream's last, which is the ordinary
   case for a range small enough to fit one chunk. The alternative, completing on it, would
   install one snapshot's staged files under another's label; that is
   `CompleteOnRestart`. The cost is one round trip per restart, and it terminates: the
   resent stream is at the assembly's own identity and completes.
8. *What an adoption does with what the refused store was carrying.* Q15's slice owns the
   refusal; nothing settles whether the task's assemblies, waiters and sends survive it.
   *Taken:* they do not. Every one of them names a directory under the store the node just
   refused, so `adopt_fresh` drops them and the streams come again against the fresh one.

**The pair (CLAUDE.md:52-67), and the mutation standard.** Eleven variants, each built
beside the correct code and each failing a check here. Seven of the eleven are mutations
that a single-range, single-follower world could not catch at all — with one range and one
stream they are indistinguishable from the correct node:

| Variant | The mutation | The check that fails under it |
| --- | --- | --- |
| `SharedStagingDir` | one staging directory per engine directory, as today | `two_ranges_assemble_into_directories_of_their_own` |
| `OneAssemblyPerNode` | one assembly for the node, abandoned for a chunk of another sender | `a_chunk_of_another_stream_never_abandons_an_assembly` |
| `VersionDirWithoutRange` | `snap-<index>-<take>`, as today | `two_ranges_takes_at_one_index_do_not_collide` |
| `SweepAcrossRanges` | the sweep deletes every unpinned version, whatever range's | `a_ranges_sweep_leaves_every_other_ranges_versions` |
| `CapStreamsSent` | a per-node cap on streams sent | `a_leader_feeds_every_designated_follower_at_once` |
| `ChunksInBatchFrames` | chunks through the per-peer outbox | `a_chunk_travels_in_a_frame_of_its_own` |
| `InstallWithoutRepair` | the switch made without the range's repair (D-066) | `an_install_is_a_live_install_of_the_ranges_spans_with_its_repair` |
| `AdoptedOnRangeInstall` | `RaftAdopted` traced for a replica's install | `only_a_fresh_directory_after_a_refusal_is_adopted` |
| `StagingByRangeAlone` | the staging directory keyed by range and not also by sender | `two_senders_of_one_range_assemble_into_directories_of_their_own` |
| `SlotReservedForWaiter` | a freed slot reserved for the waiter at the head of the queue | `a_freed_slot_goes_to_a_stream_still_asking_for_it` |
| `CompleteOnRestart` | a stream completed on the very chunk that restarted it | `a_restarted_stream_is_never_installed` |

Seven need more than one range, more than one sender or more than one follower to be wrong
about, and that is the mutation standard applied to this slice: `SharedStagingDir`,
`StagingByRangeAlone`, `OneAssemblyPerNode`, `VersionDirWithoutRange`, `SweepAcrossRanges`,
`CapStreamsSent` and `SlotReservedForWaiter` — the last needs three keys and a cap below
them, which is §12's shape exactly, and the first two are the same mistake one level apart.
The other four — `ChunksInBatchFrames`, `InstallWithoutRepair`, `AdoptedOnRangeInstall` and
`CompleteOnRestart` — are wrong with one range and one follower too, and are here because
Q41, D-066 and RAFT.md:203-207 name them. (This entry first claimed "six of the eight" and
then contradicted itself in the next sentence; the count above is against the standard, not
against the table's row order, and it is the corrected one.)

**What the adversarial review changed.** The review ran three probes against the *correct*
node and found two safety defects and one that turned an ordinary case into a permanent
refusal. Each fix below is the smallest one that makes the code do what this entry already
claimed, and each is proved by the mutation that undoes it being caught. The mutations were
run one at a time against the fixed tree, reverted between runs.

| Finding | The fix | The mutation that proves it |
| --- | --- | --- |
| A chunk that restarted an assembly *and* said it was its stream's last was installed: `Install` pointed at a directory holding two streams' files, labelled as one | the restart wins; the stream is restarted from its first byte and completes on the next pass (open choice 7 above) | completing on a restart → `a_restarted_stream_is_never_installed` fails; kept as `CompleteOnRestart` |
| `finish` reserved the freed slot for the head of the queue, so a waiter whose leader had changed held a slot for ever and the node wedged under §12's own shape | the slot is freed, not reserved, and granted to whichever stream asks (open choice 3, amended) | reserving on `finish` → `a_freed_slot_goes_to_a_stream_still_asking_for_it` and the cap check fail; kept as `SlotReservedForWaiter` |
| A range the task was never told it hosts completed with `spans: []`, which `Engine::install_spans` refuses at every switch (`EmptySpan`, D-068) — a permanent refusal for a newly placed replica | `on_chunk` panics where the range is unknown, as `host` does | `unwrap_or_default()` → `a_range_the_task_does_not_host_never_completes_a_stream` fails |
| The send half recorded the staging name of the *follower*, a path that exists on no node, and `is_streaming` compared it with itself | `Snapshots::new` takes the node's own id and the name is built from it; the tautology is gone | naming it by the follower → `the_name_a_leader_records_is_the_name_its_followers_stage_under` fails |
| `adopt_fresh` asserted "no assembly is open" while its doc said "no range has installed", and `ranges_installed` was the literal `0` | installs are counted per engine directory; the assertion and the figure both read that count, and the adoption drops what the refused store was carrying (open choice 8) | dropping the assertion → `a_directory_a_range_installed_into_is_not_adopted_as_fresh` fails |
| The slice's most-argued decision — staging keyed by (range, sender) — had no variant, and was held up only by a path literal in a check about adoption | `StagingByRangeAlone`, and a check about two senders of one range | keying by range alone → `two_senders_of_one_range_assemble_into_directories_of_their_own` fails |
| `host`'s span assertions (open choice 5) had no check at all | two `#[should_panic]` checks | dropping either assertion → the matching check fails |
| `sent`, `is_streaming`'s `false`, `Install.at`, the waiting queue's duplicate guard, the bytes an assembly holds after a restart, and `route`'s guards were all unasserted; an empty chunk was reported as `Oversized { len: 0 }`, which is not what it is | each is asserted now; an empty chunk and a chunk for a follower no stream is running to are panics, since both are the node's own bug and not a peer's message | each mutation is caught: see the list above |

Two of the review's points are recorded rather than changed. Its count of the variants that
need more than one range is corrected in the paragraph above. Its observation that two of
the four measurements were constants rather than observations is why the Measurements table
now says which is which, and the frames figure is now read at three chunk sizes instead of
one.

**Tiers and rates (D-061).** Nothing here is asserted at a tier, because nothing here is a
sweep: each check is deterministic and fails under its variant on every run. D-061 asks a
tier of a *sweep's* assertion, and the slice that puts the node under the sweeps is where
these variants meet seeds. No bound is asserted against a measured rate in this slice, so
none was widened or lowered.

**Measurements.** Machine: Apple M2, 8 cores, on battery (`pmset -g batt`: "Now drawing from
'Battery Power'"), with other Stage B slices building in parallel — load averages 19.00 18.68
30.98 at the time of the run. Counts, not times, so they are the same on any machine:

| What | Figure | Observation or constant | Where |
| --- | --- | --- | --- |
| frames a chunk travels in | 1 per chunk, at each of three chunk sizes — one byte, §4's 256 KiB, and the largest chunk that fits a frame | the *decode* is an observation: the frame is read back and asserted to carry one message, that range's tag and the chunk's bytes, at each size. The counter pair `chunks == frames` is arithmetic on one path | `a_chunk_travels_in_a_frame_of_its_own` |
| assemblies abandoned for a chunk that is not their own | 0 on the correct node, over four interleaved (range, sender) streams | a **constant**: with the key carrying the sender, the abandon path is unreachable by construction. The figure cannot distinguish a correct-side mutation, and is here as the variant's contrast, not as a count | `a_chunk_of_another_stream_never_abandons_an_assembly` |
| assemblies open under a cap of two with four ranges arriving | 2 open, 2 waiting; a freed slot stays free until a waiter asks, and a resend queues nothing new | observation | `the_receive_cap_holds_and_a_freed_slot_admits_the_first_waiter` |
| slots recovered when the waiters' leader is replaced | 2 of 2 — both freed slots go to the new leader's streams | observation, and the figure the review's wedge turned into a check | `a_freed_slot_goes_to_a_stream_still_asking_for_it` |
| `RaftAdopted` events per replica install | 0; one per fresh directory, and `ranges_installed` is read from the installs counted for that directory | observation | `only_a_fresh_directory_after_a_refusal_is_adopted` |
| streams installed from a directory that had just been started over | 0 | observation | `a_restarted_stream_is_never_installed` |

Stage B's timed measurements — the step cost, the frames per peer in a round, the replay
burst, the inbox's drops, the apply lag, the take's hold — belong to the tasks slice and the
slice that runs the sweeps; this slice adds none and relies on none.

**The premerge.** `ANANKE_SEEDS=1000 scripts/premerge.sh` on the commit this entry lands
with, in the words the script printed (D-070):

```
premerge: Darwin 25.6.0 arm64, Apple M2, 8 cores
premerge: before, load 26.10/61.10/56.18, AC Power, no thermal warning recorded
premerge: after, load 154.70/140.32/112.73, AC Power, no thermal warning recorded
premerge: green at 1000 seeds in 1308 s
```

On AC throughout and with no thermal warning, but the load lines are the figure's context
and not decoration: six other Stage B slices were building and running their own sweeps on
this laptop for the whole run, and a load average of 140 on eight cores is why 1 308 s sits
beside D-068's 613 s and D-072's 779 s on the same machine. It is a green, not a timing.

And again on the tip the review's fixes land with, which is what this entry's premerge
figure now is:

```
premerge: Darwin 25.6.0 arm64, Apple M2, 8 cores
premerge: before, load 26.23/25.48/24.17, AC Power, no thermal warning recorded
premerge: after, load 48.89/30.80/26.39, AC Power, no thermal warning recorded
premerge: green at 1000 seeds in 2558 s
```

AC throughout, no thermal warning, and a quieter machine than the first run's — load 26
rising to 49 rather than 140. The 2 558 s against the first run's 1 308 s is not the fixes,
which add a dozen checks that run in a millisecond and touch no sweep: the release build was
cold for this tree and the other slices' sweeps had the cores for most of it. Both are
greens, and neither is a timing.

**What moved.** Nothing outside `crates/ananke-shard` and the two documents. `sim/` does not
depend on `ananke-shard`, so no schedule moves, no pinned trace hash moves and no pinned seed
is re-audited: `crates/ananke-shard/src/variant.rs` gained eleven variants at bits 8 to 18,
which no existing code reads, and the crate's other files are unchanged but for `lib.rs`'s
description of this module. `Snapshots::new` takes the node's own `ServerId`, which only
this module's own checks call. The one-group
server's `snapshot.rs` is untouched and keeps working exactly as RAFT.md §1 says; RAFT.md
gains the node's naming, the node's assembly rule, the freed slot's rule, the restart's and
the node's snapshot task beside it (D-053).

**Alternatives.**
- *Key the staging directory by range alone and let a second sender of a range restart the
  first.* It is what today does, and it is the behaviour §11 calls out as wrong for re-seeds;
  the argument that two senders of one range are rare is the argument that made one assembly
  per task look safe.
- *Put the range-keyed paths in `ananke-raft::snapshot` beside today's.* A `u64` group id is
  inside Q40's line, and the crate would then own both layouts. Kept out: the node's layout
  is the node's, and `ananke-raft` keeps a layout with no node in it until the one-group
  server retires.
- *Give the fresh-directory adoption an event of its own.* D-066 considered and rejected it;
  this entry does not reopen it.

**Consequences.** The node's `snapshot` task can be wired to a socket and a disk without
another decision about keys, caps or frames. Nothing here has met a seed: every variant in
`NodeVariant::SNAPSHOT` is caught by a deterministic check, and the tier each is caught at on
the sweeps is the scenarios slice's to measure.

---

## PROPOSED D-076 — Four ranges on every node: the ranges configuration fixes at bootstrap, the node that runs them, and the checks of §8 with something to be wrong about

**The number.** SHARD.md's Stage B plan and the work order for this slice name this
entry D-075. The branch this one is stacked on (the snapshot task's) had already taken
D-075 on the same file, and two entries under one number in one document is worse than
a number out of the plan's order, so this is D-076 and the footer moves to D-077. Every
code site of it carries `// PROPOSED(D-076)`. Nothing of D-075's is renumbered. The
integrator may renumber this entry when the branches merge; nothing in the tree depends
on the number beyond those markers and this heading.

**Context.** SHARD.md's Stage B (SHARD.md:2185-2422) asks for "ranges fixed at bootstrap
from configuration, as §2 generalises `initial_voters`, each traced
`RangeCreated { cause: bootstrap }`", for "`sim/raft.rs`'s arms, `sim/membership.rs`
with #46's extension, and `sim/quorum.rs`, each run on the node with four ranges on
every node", and for each core to be "seeded from `n{id}/r{range}/protocol` (Q13)".
D-071 keyed the checks of §8 by range and proved every key on hand-built two-range
traces, because with one group per trace "a check keyed by `(group, term)` and one keyed
by `term` say the same thing on every seed at every tier". This entry is the first trace
in the tree with more than one range in it.

### What is built

1. **The node as a running server**, `ananke_shard::server::run`: one engine, one socket,
   one inbox bounded in bytes, one `raft` task stepping every core on one ticker in
   Q41's round, one `apply` task, and **a Raft store and a core per range**, each under
   its own key prefix. `RaftStore::open_sibling` opens the second and later stores on the
   engine the first opened: the two checks `RaftStore::open` makes first — the
   directory's format (D-059) and the recovery's loss (D-044) — are the *engine's* and
   are made once for every store on it, and everything else is per prefix.
2. **Ranges fixed at bootstrap from configuration.** A node's configuration names the
   ranges it hosts, each with the span it holds, and the voters each starts with: §2's
   `initial_voters` generalised. A replica whose store is fresh is created at the node's
   first start and traced `RangeCreated { cause: bootstrap }` with that span, generation
   1, those voters, floor 0 and its store's incarnation; a replica whose store already
   holds state is restated as a server's is and traces no creation.
3. **Each core seeded from `n{id}/r{range}/protocol`** — `Environment::range_rng`
   (D-057), which until now nothing had called. Two ranges on one node draw different
   election timeouts, which is what keeps four ranges from campaigning in lockstep, and
   a directed scenario says so: `ranges::alone` runs one node whose two fellow voters
   are configured and never started, so nothing ever resets an election timer and every
   core campaigns on its own. Over seeds 1 to 4 the node's four ranges first campaign at
   4, 3, 3 and 4 distinct instants; a node that drew one seed for all four gives 1 on
   every seed, and the check fails. The sweep cannot say this — three live nodes reset
   each other's timers, and a range whose leader is elsewhere never campaigns at all —
   which is D-061's rule for a variant no seed of any tier catches: a directed scenario,
   not a lower bar.
4. **A range on every client message**: `ananke_shard::client`, the `ananke-raft` client
   request and response with the range in front of them. A client takes its key's range
   from the scenario's fixed map and the node reads it there; nothing routes.
5. **The node scenario**, `sim/ranges.rs` and its sweep `sim/tests/ranges.rs`: three
   nodes, **four ranges on every node**, each placed as today's one group is, two clients
   writing and reading keys over four contiguous spans, and a fault model of crashes,
   isolations and leader-relative isolations whose **range is drawn from the arm's own
   stream** (§11, env 8). Its trace is put through the checks D-071 keyed
   (`raft::Report::over_a_run`): checks 1 to 4 keyed by range, the history's closure by
   `(range, index, term)`, the timer check and pre-vote per `(range, server)`, the checks
   about time per range with a majority, the write bound per key, and the payload oracle.
6. **Two checks that only a four-range trace could show wrong**, both found by this
   sweep on its first run and both fixed here (see the table).
7. **Check 7's first step** (`Report::creations_agree`), which D-071's item 11 said the
   stage emitting `RangeCreated` owes: a range's replicas agree on the descriptor they
   were created with, every creation is a bootstrap one, and no replica is created twice.
8. **The liveness half of the majority carve-out's case**, which D-071's item 6 owed to
   "the stage that gives `range_of_key` a map".

### What the design documents left open, settled here

Each is marked `// PROPOSED(D-076)` in the code.

1. *The node is added beside the one-group server, not in its place.* SHARD.md's Stage B
   reads as though `ananke_raft::run` becomes the node, and says two of its commits
   "move every schedule" and re-audit every pinned seed. This slice does not move them:
   `ananke_raft::run` is untouched, every one-group scenario keeps its schedules, its
   pinned hashes and its forty-four pinned pre-vote assertions, and the node runs beside
   it with a scenario of its own. The conservative reading, and the one taken: a pinned
   seed that does not move needs no re-audit, and the node can be seen to work before
   anything that works today is disturbed. **What it costs**, said plainly: Stage B's
   exit criterion that `sim/raft.rs`'s arms, `sim/membership.rs` and `sim/quorum.rs`
   *themselves* run on the node is **not met** by this slice (see "What is not built").
2. *A node's configuration names its ranges and their spans, and nothing routes by a
   span.* §8's `RangeCreated` names the span a replica was created for, so configuration
   has to carry one. It is not a descriptor: no apply changes it, no message is routed by
   it, and the node keeps it only to trace it. Stage C gives ranges real descriptors.
3. *The node's own inputs are the host's own type.* A client's request and an index the
   `apply` task made durable are not peer messages and do not belong on the inbox, whose
   admission is about frames and bytes (D-072). `Host::Local` is the host's associated
   type and the `raft` task races a queue of them beside the inbox, the ticker and the
   persists. The alternative, a range on `ananke_raft::client::Request`, would put the
   client protocol's knowledge of ranges in the crate Q40 says names no range.
4. *A node-local input is held for a persisting core exactly as a message of its range
   is.* §4 fixes that rule for messages; a client's proposal stepped into a core whose
   persist is outstanding would be a step the round exists to prevent. So the node holds
   it, in the order it arrived, and steps it when that core's persist resolves. A client
   of one range waits behind that range's disk and behind no other's.
5. *A client's answer decided inside a round leaves through the node's `answers` task.*
   The round's steps are synchronous — the host is told what a core decided while the
   round is being driven — so an answer decided there cannot await the socket where it is
   decided. It is queued and sent by one task of the node's own. A task per answer was
   the first thing written here and it is wrong: it would put a task on the simulator's
   scheduler for every rejection.
6. *A delivered payload is read as a one-group frame when it parses as one, and as a
   batch frame otherwise.* The two codecs' first bytes collide: a batch frame's version
   is 1 and `ananke-raft`'s tag 1 is a pre-vote, and a pre-vote frame **does** parse as a
   batch frame of two messages the codec then refuses. Read batch-first, every pre-vote
   delivery of every one-group scenario would reset no timer at all. It cannot go the
   other way — a batch frame's first byte is 1, a pre-vote is exactly 33 bytes,
   `Frame::decode` refuses trailing bytes and the smallest batch frame is 34 — and the
   node scenario pins that direction on **every seed of every tier**, as one of
   `ranges::Report::check`'s own steps (`frames_are_this_nodes`): every payload a node
   sent a node parses as this node's batch frame and as no frame of the one-group
   server's. It was pinned on one seed until the review of this entry found the claim
   wider than the check; `no_batch_frame_of_the_node_parses_as_a_frame_of_the_one_group
   _server` is now where the argument is written down, beside a run of it and a forged
   one-group payload the check catches.
7. *A run says which ranges it hosts and how its keys map to them.* `Report` carries
   `ranges`, which the payload oracle asks every replica record to name one of, and
   `key_range`, the map the write bound and the liveness check read. `range_of_key` was a
   constant function; it is now the run's own map, which is what lets the liveness half
   of the carve-out be told from the cluster-wide reading at all (D-071, item 6).
8. *Check 7's first step is the creations this slice emits.* D-071 keys checks 2, 3 and 4
   off `RangeCreated` and reads neither its `cause` nor anything behind it: "a replica
   that forged either event would launder its own violation past checks 2, 3 and 4", and
   §8's check 7 is what ties them down. Check 7 proper — a range's replicas agree on the
   *sequence* of its configurations — needs a membership change of a range, which no
   stage produces yet. What exists here is one step of that sequence, and it is checked:
   every replica of a range was created with the same span, generation, voters and floor,
   every creation is a bootstrap creation, and no node creates a replica twice. The
   amnesty is closed for the creations this slice emits and stays open for the ones a
   split or an install will emit; that stage owes the rest of check 7.
9. *A post-heal write is measured from its own call, and the recovery time proper is
   asked per range.* The bound of §8 is that a client write to every key of a live range
   completes within ten maximum election timeouts of the last heal. Read as
   `ret − last_heal` that charges the cluster with the client's own idleness: with eight
   keys, two clients and a 60 % write mix, the first post-heal write to one particular
   key can simply not be *issued* for two seconds. With one range and two keys the two
   readings all but coincided, so no sweep in the tree had ever seen the difference; the
   nightly on this branch (run 35645688334) failed six seeds of ten thousand on it, and
   on each of the three reproduced the key was served in about 25 ms once anybody asked
   for it. So the per-key reading is now `ret − max(call, last_heal)` — the write's own
   interval — and the tooth it stopped carrying, how long after the heal the range became
   writable at all, is asked per range instead, where no client's choice of key can
   lengthen it (every range of this scenario is written to within milliseconds of any
   moment its clients are running). **Neither bound is widened**: both are
   `election_max() * 10`, and the wedge arms — a key, or a range, whose post-heal writes
   all stayed pending — are untouched. The alternative, a round-robin key draw in the
   scenario, is equally faithful and was not taken: it moves every schedule of the run
   and would have invalidated this entry's whole measurement and mutation table, where
   this reading moves none. The case is `a_post_heal_write_is_measured_from_its_own_call
   _and_the_range_from_the_heal`, whose second history is the pair — a range that takes
   2.5 s after the heal to complete any write while every write that completes is quick
   passes the per-key reading and is caught by the per-range one.
10. *A read a replica refuses gives its registration back at the step.* `local_input`
   registers every read it hands a core, because a core that leads answers it later by
   id. A core that does **not** lead answers `Output::Rejected` and never names the id
   again, so no `ReadDropped` follows and `answer_read` — the only other place a
   registration is taken back — is never reached for it. The step's own refusal is the
   last moment the id is known, and `Replica::refuse` takes it there. Until this fix the
   entry was left behind: a follower that refuses reads for a living grew an unbounded
   map of `(SocketAddr, Request)` — measured at 39 to 70 refused reads per six virtual
   seconds across twelve replicas — which is a standing failure and not, as this entry
   first said, only a latency wart. The client is still *not* answered at the step and
   still waits for its own timeout: answering there queues a packet inside the round and
   moves every schedule this entry measures. That answer stays the next slice's first
   job.
11. *The paths this node does not have are asserted absent rather than left to be found.*
   It has no `snapshot` task, no install, no re-seed and no follower compaction — each is
   another Stage B slice's. So the scenario keeps `snapshot_threshold` far above what its
   clients write and rots no bit, and `Report::check` fails on every seed that traces a
   snapshot or a refusal, naming the slice that owns it. The day a schedule reaches one,
   the sweep says so instead of passing over it (CLAUDE.md:58-67).

### The measurements

Every figure below was taken on this branch, on an **Apple M2 of 8 cores, on AC Power**
(`pmset -g batt`: "Now drawing from 'AC Power'"), with the other Stage B slices building
beside it: the load averages are recorded with each figure and are high for that reason.

| What | Command | Figure |
|---|---|---|
| The correct node at a thousand seeds | `ANANKE_SEEDS=1000 cargo test --release -p ananke-sim --test ranges every_seed_passes_on_the_correct_node` | green; 12 000 bootstrap creations, leaders by range {2: 2707, 3: 2684, 4: 2658, 5: 2701}, applies by range {2: 125 605, 3: 125 474, 4: 125 297, 5: 125 884}, 37 469 381 records; load 149 |
| Trace records per virtual second, divided by the range count | the same run | at most **1 905**, against `TRACE_CAP` of 400 000: a run of this scenario holds four ranges for some 52 virtual seconds before the cap, and its own runs are under 3 s. The name is what the figure is: the numerator is the *whole* trace — client operations, every `MessageSent`/`MessageDelivered`, the engine's records — of which only a small part names a range at all, so it is an upper bound on any range's own rate and not that rate |
| The busiest range's own records per virtual second | the same run | **131**, about a fifteenth of the figure above (`busiest_range_records_per_second`, folding `raft::range_of` over the records). A cap sized from the row above is sized conservatively, which is the direction to be wrong in; both figures are here so that neither is read as the other |
| Peer frames carrying messages of more than one range | the same run | **1 121 821** over a thousand seeds, about 1 122 a seed; on seed 3 alone, messages per frame {1: 2 554, 2: 877, 3: 64, 4: 4} and ranges per frame {1: 2 625, 2: 811, 3: 59, 4: 4}. The sweep asserts a hundred a seed, measured before it was asserted (D-061). This is the claim four ranges to a node is the parameter for (SHARD.md §12), and it is read off the frames: counting the ranges a *run* names would restate the scenario's own shape and pass a node whose every frame carried one message |
| `StepWhilePersisting` (the node's pair) | the same command, `a_node_that_steps_a_core_while_its_persist_is_outstanding_is_caught` | caught on **639 of 1 000 seeds (63.9 %)**, which is why it is asserted at every tier (D-061) |
| The write bound's margin, four ranges | `ANANKE_SEEDS=1000` over the node sweep | see below |
| The recovery time's margin, per range | `ANANKE_SEEDS=1000` over the node sweep | see below |
| The premerge on this branch's tip | `scripts/premerge.sh` | first tip: **green at a thousand seeds in 1 252 s** (`before, load 41.40/56.73/59.91, AC Power, no thermal warning recorded`; `after, load 43.04/48.76/50.33, AC Power`). Fixed tip, after the review: **green at a thousand seeds in 673 s** — `premerge: Darwin 25.6.0 arm64, Apple M2, 8 cores`; `premerge: before, load 10.70/10.18/7.46, AC Power, no thermal warning recorded`; `premerge: after, load 19.29/15.63/11.89, AC Power, no thermal warning recorded` |

**The write bound's margin is the figure for the owner**, and the reading it is taken
under changed under the review of this entry (settled point 9 above): a post-heal write
is measured from its own call, and the recovery time proper is asked per range. The
figures below are all under the new reading, at `ANANKE_SEEDS=1000` in release on an
Apple M2 of 8 cores on AC Power, load average 3.7 (an idle machine this time):

| Reading | Worst, of the 2 s bound | Margin |
|---|---|---|
| The first post-heal write to a key of a live range, **from its own call**, on a run the bound is asked of | **30.554792 ms** | **1.969445208 s** |
| The same on any run, asked of or not (D-016 withholds every claim about time on a non-uniform schedule) | **31.858991 ms** | — |
| **A live range's recovery**: the first write to any of its keys completed after the heal, from the heal | **1.163172939 s** | **836.827061 ms** |

Nothing else in the run moved: 12 000 bootstrap creations, leaders by range {2: 2707,
3: 2684, 4: 2658, 5: 2701}, applies {2: 125 605, 3: 125 474, 4: 125 297, 5: 125 884} and
37 469 381 records are the figures this entry reported before the fix, to the record.
That is the point of taking the measurement rather than the schedule: this reading moves
nothing, so every other figure here and the whole mutation table below stand.

**What the old figures were, and why they are not comparable.** This entry first reported
1.8404 s of the 2 s bound with four ranges (a margin of 159.6 ms) and 504.6 ms with one,
both read as `ret − last_heal` per key. The nightly then failed six seeds of ten thousand
at up to 2.4348 s — seeds 2400, 4976, 5193, 6508, 6605 and 9204, run 35645688334, job
`10 000 seeds (5)` — and on the three reproduced the failing key had been *asked for*
2.411 s, 1.982 s and 2.081 s after the heal and was served in 23.5 ms, 28.1 ms and
31.3 ms. The old figures were the clients' idleness as much as the node's latency, which
is the model error, and the 159.6 ms margin they showed was never the margin of anything
the cluster does.

**What the narrowing that is left is.** The recovery margin, 836.8 ms at a thousand
seeds, is the one to watch: four ranges elect, replicate and apply through one `raft`
task, one `apply` task and one engine on each node, so a range's recovery after a heal
waits behind the other three ranges' work as well as its own. The one-range scenarios
read under the same two readings, on the premerge of this branch's fixed tip at a
thousand seeds, are the comparison:

| Reading | One range | Four ranges to a node |
|---|---|---|
| A post-heal write from its own call | **44.623736 ms** (`sim/raft.rs`'s sweep, `Coverage::slowest_write_after_heal`) | 30.554792 ms |
| A range's recovery from the heal | **664.94607 ms** (`sim/membership.rs`'s sweep, whose figure is `Report::time_to_write_after_heal`) | 1.163172939 s |

So a range on the node recovers in about 1.75 times what one group takes, and an
individual write is no slower. **The bound is not widened** (D-030, D-039). If the nightly at ten thousand seeds finds a seed past it, that is a
recovery genuinely over ten election timeouts and a finding for the owner, not a bound to
move: the recommendation would be to look at the node's round and its engine, not at the
number.

### The mutation standard: what the sweeps can now be wrong about

D-071 keyed twenty-three checks and proved each key on a hand-built two-range trace,
because no sweep could tell a right key from a wrong one. Each of those keys was planted
again here, one at a time, in a copy of this tree with a target directory of its own, and
the **node scenario's sweep** was run against it: at twenty seeds first, then a hundred,
then a thousand for the rows twenty missed. The harness is `scratchpad
stage-b/ranges/mutate.py` and the runs these cells are read off are `scratchpad
stage-b/ranges/mutations-d075.log` and `mutations2-d076.log`.

**Four rows were planted twice, and the second planting is the one to read.** A map
written under one key and read under another is not a wrongly keyed check, it is a check
that stopped checking: "not caught" would then say nothing about the key. The first
planting of checks 3c, 3d, 4a and 4b changed one site each and left the map's other
sites alone; the second (`mutate2.py`) keys every site of that map — every insert, every
get, every remove — and both are in the table, the second marked "every site". The two
that changed verdict, 3c and 3d, are exactly the two whose first planting was inert.

| Wrong key planted | What the four-range sweep says |
|---|---|
| 1 election safety keyed by the term alone | **caught at 20 seeds** |
| 2a log matching keyed by the server alone | **caught at 20 seeds** |
| 2b two groups' snapshot floors compared at one index | **not caught** to a thousand seeds |
| 2c a RangeCreated sets no floor | **not caught** to a thousand seeds |
| 3a the first configuration taken as 1..=servers | **not caught** to a thousand seeds |
| 3b the rescan over every group's committed set | **caught at 20 seeds** |
| 3c who leads kept per server | **not caught** to a thousand seeds — *but this planting left the check inert rather than mis-keyed; read the "every site" row below* |
| 3d the commit index kept per server | **not caught** to a thousand seeds — *but this planting left the check inert rather than mis-keyed; read the "every site" row below* |
| 4a the applied map keyed by the index alone | **caught at 20 seeds** — *but this planting left the check inert rather than mis-keyed; read the "every site" row below* |
| 4b the applied index kept per server | **caught at 20 seeds** — *but this planting left the check inert rather than mis-keyed; read the "every site" row below* |
| 4c the effect left out of the value | **not caught** to a thousand seeds |
| 5 the history's closure keyed by (index, term) | **caught at 1000 seeds** |
| T1 the timer check's clocks kept per server | **caught at 20 seeds** |
| T2 a creation arms no timer | **not caught** to a thousand seeds |
| P1 the isolated server's terms read as one sequence | **not caught** to a thousand seeds |
| M1 the checks about time asked of the cluster in the helper | **caught at 20 seeds** |
| X3 the checks about time asked of the cluster where the carve-out is used | **not caught** to a thousand seeds |
| W1 the write bound as one minimum over every write | **not caught** to a thousand seeds |
| MS the match starts keyed by the node alone (this slice's own) | **caught at 20 seeds** |
| F1 a delivered frame read as one group's (this slice's own) | **caught at 20 seeds** |
| 3c who leads kept per server (every site) | **caught at 1000 seeds** |
| 3d the commit index kept per server (every site) | **caught at 20 seeds** |
| 3d' a removal of every group's commit index | **not caught** to a thousand seeds |
| 4a the applied map keyed by the index alone (every site) | **caught at 20 seeds** |
| 4b the applied index kept per server (every site) | **caught at 20 seeds** |
| 4d/4e a removal read as the node's / as every node's | **not asked**: nothing in this slice emits a `RangeRemoved`, so there is no removal for either key to be wrong about |
| 4f check 4's mismatch message | not a key: the message, whose case is `one_group_may_not_apply_two_entries_at_one_index` |
| T0 the timer check's gap report neutered | not a key: the check's body, held by its eight one-range cases |
| P2 a range created under an isolation starts at term 0 | **not asked**: every creation here is at the node's start, never inside an isolation |

**Two of the rows are this slice's own**, and both were found by this sweep on its first
run against code that was green on every one-group tier:

- `match_starts_are_first_rises` kept its terms and its first rises **per node**, since
  the fold was written when a node ran one group. A node leading four ranges reads
  whichever range's term record came last for all four, and one leader's four first rises
  under one follower collapse into one key. It is now keyed by
  `(range, leader, term, follower, incarnation)`, and the sweep fails on every seed under
  the old key.
- The timer check's replay read a delivered frame with `Frame::decode` and credited the
  reset to `SINGLE_GROUP` — which D-071 said in as many words would be the batch frame's
  decode "which the decode reads there". A node's frames are batch frames, so under the
  old reading no follower replica was ever seen to hear from its leader between its own
  appends, and the sweep failed on half its seeds with the correct system. It now reads
  every message a batch frame carries and resets the timer of each message's range.

**What the review of this entry planted against the node's own code, and what came of
it.** The mutations above are the *checks'* keys; these are the node's code, each planted
alone on a copy of the tree with a target directory of its own and run against the node
scenario at twenty seeds and then two hundred, and against `cargo test -p ananke-shard
--lib`.

| Mutation | Site | Was | Now |
|---|---|---|---|
| M1 a local input for a persisting core dropped instead of held | `node.rs` | **survived** 200 seeds of the sweep and the lib tests | **caught** by `a_local_input_for_a_persisting_core_is_held_and_stepped_in_order`, the check the new `HeldLocalDropped` variant is the pair of |
| M7 held locals replayed last-in-first-out (`pop_back`) | `node.rs` | **survived** 200 seeds | **caught** by the order half of the same check |
| M4 one seed drawn once for every core on the node | `server.rs` | **survived** 200 seeds | **caught** by `the_ranges_of_one_node_draw_their_own_election_timeouts`: the node's four ranges campaign at one instant instead of four |
| M8 a refused read's registration left behind (the node as it was) | `server.rs` | no check at all | **caught** by `a_read_a_replica_refuses_leaves_nothing_behind` |
| M2 the `apply` task never feeds `Local::Applied` back | `server.rs` | caught at 20 seeds | unchanged |
| M3 cores built without their range (every range traces range 2) | `server.rs` | caught at 20 seeds | unchanged |
| M6 every sibling store opened under the first range's prefix | `server.rs` | caught at 20 seeds | unchanged |
| M5 `fresh` without the term/vote/quarantine clauses | `server.rs` | survived, and the bug does not manifest: after warmup a replica's log is never empty, so no second `RangeCreated` is emitted | unchanged; the clauses are defensive, not load-bearing, and this is recorded rather than covered |

`HeldLocalDropped` is the node's twentieth variant and the first one a sweep cannot see:
a client retries a request it loses and an `Applied` is superseded by the next one, so
the node scenario passes it at a thousand seeds. It keeps a deterministic check instead,
as the node's variants did before this slice put one of them under a sweep, and
`Meters::locals_held` is what says the path is reached at all — 8 to 12 deferrals against
700 to 870 local inputs in a run of the node scenario (about 1.4 %), and exactly 3 in the
directed check, which asserts the figure.

### What is not built, and why

- **`sim/raft.rs`'s arms, `sim/membership.rs` and `sim/quorum.rs` do not run on the
  node.** They run on `ananke_raft::run`, the one-group server, exactly as before. The
  node runs the arms this scenario has: crashes, isolations and leader-relative
  isolations. Moving those three scenarios onto the node needs what the node has not got
  yet — the `snapshot` task keyed by range and follower, the install, Q15's refusal and
  re-seed, follower compaction — because a third of `sim/raft.rs`'s arms aim at exactly
  those paths (`CrashInstalling`, `RetakeUnderStream`, the re-seed arms), and
  `sim/quorum.rs` is a re-seed scenario from end to end. That is why this slice adds the
  node beside the server: the slices that build those paths can move the scenarios over
  when the paths exist. **Stage B's first exit criterion is therefore owed**, and with it
  the re-assertion of Phase 2's sixteen variants on the node.
- **Rows 2b and 2c of the mutation table are not caught by any sweep**, and the
  hand-built cases stay their only oracle: both are about a snapshot floor, and this
  node takes no snapshot.
- **A read a replica refuses is answered by the client's own timeout, not by the node.**
  The one-group server answers `NotLeader` at the step that rejects a read
  (`node.rs`'s read arm); this node registers the read, sees the rejection through
  `Host::rejected`, gives the registration back there (settled point 10) and leaves the
  client to time out after 40 ms and ask elsewhere. What is left is a latency wart and
  not a violation — the read is still linearizable and the client still gets its answer —
  and, since the review of this entry, no longer a leak: the entry the refusal used to
  leave behind is taken back at the step. Answering the client there is what remains
  undone, and it is undone for one reason: it queues a packet inside the round, which
  moves every schedule of the scenario whose thousand-seed figures and whose mutation
  table this entry reports. It is the first thing the next slice on the node's client
  path should take.
- **`Node::raft`'s extra race moved the schedules of `node.rs`'s own directed tests**,
  and this entry said "nothing moved" without that exception. The loop races a local
  queue beside the inbox, the ticker and the persists, and `race` draws a scheduling bit
  per poll (`ananke-env`'s `future.rs`), so those tests run on a different scheduling
  stream than before this slice. They are directed tests, not pinned seeds, and each was
  re-read to confirm it still asserts its mechanism rather than passing by luck — the
  replay check still asserts `ticks_replayed_most == 3`, the order checks still assert
  where their notes fall. No sim scenario and no pinned hash is touched: `Node::raft` has
  exactly two callers, those tests and `server::run`, and the node scenario is this
  slice's own.
- **The node's meters are not readable from the sim.** `Meters::locals_held` says the
  holding path was reached, and the node scenario cannot assert it: `server::run` owns
  the `Node` inside a spawned task and hands nothing back. Making it readable means a
  trace event for the node's own meters, which belongs to the slice that puts the node's
  measurements in the trace (PROPOSED D-073's counter is in the same position). Until
  then the figure is the directed check's, which asserts exactly 3, and the sweep's is
  measured by hand: 8 to 12 deferrals against 700 to 870 local inputs a run.
- `RaftRead` is traced by the node as decided when it is answered, where the one-group
  server carries the step's own decision stamp (D-047): the host serves the read inside
  the round that confirmed it and takes no stamp of its own. Nothing reads a `RaftRead`'s
  decision time today.
- The step cost, the frames per peer in a round, the replay burst, the inbox's drops and
  admission cost, the apply lag and the take's hold are the measurements of the slices
  that own those parts; this entry adds the trace records per virtual second (whole
  and per range), the frames that carry several ranges, the write bound's margin per key
  and the recovery margin per range.

---

## PROPOSED D-078 — Follower compaction as D-065 decided it: a record at the applied index, no checkpoint under it, and the follower log bounded at 64 × `snapshot_threshold`

**What this builds.** D-065's option C, on today's single-group server, independent of
the node (SHARD.md §12, Stage B; §11, raft 13). A replica that is not leading and whose
log has outgrown its prefix by `snapshot_threshold` asks the `apply` task, on its next
tick, for `SnapshotAction::Record`: the snapshot record at the applied index, written
synced between applies as D-036 writes a take's, with **no checkpoint under it**. The
core then compacts to it, and the step's persist deletes the prefix — record durable
first, deletes after, the order a leader's compaction already uses. The configuration
in force at the new prefix's end stays as D-029's revert floor, on a follower as on a
leader, because the record is written where the `apply` task's `config` is exactly the
one at that index.

**It needed no new state and no new message, and that was verified as it was built.**

- The record is written with `taken: false` and no directory: the shape an install's
  repair writes (`snapshot.rs`, `Repair`, the record built once the staged store is
  adopted), and the shape a crash between a take's record and its checkpoint already
  leaves. It is that shape in the two fields that decide what a later stream does, and
  in exactly one field it is *not*: the repair writes `take: 0`, because an install's
  versions start over and a compaction's do not, so this carries the counter forward.
  That one field is settled point 4 below, and calling the two "the same shape" — as
  this entry first did — overstated it. `RaftStore::open` already deletes log keys at or
  below the record, so a crash between the record and the persist's deletes is completed
  at the next open, where a take's crash window already is.
- A server that later has to stream finds no complete version of that index and asks
  for a take: `start_stream`'s existing "the record is an install's. Ask for a take"
  arm (`node.rs`), which answers `StreamFailed { retake: true }`, clears the core's
  `taken` and lets the next tick ask. No arm was added for this.
- The record carries the store's take counter forward rather than resetting it, so a
  later take still numbers its version directory past every one this store has made
  (D-043).
- The leader's rules are untouched: its threshold take, its two-election-timeout
  hold-off and D-037's condition for compacting are the same code. `maybe_compact`'s
  role check became a branch: a leader still waits for every follower's match or its
  snapshot designation; a replica that is not leading has no follower to wait for.

**What the design documents left open, and the conservative choice taken.**

1. *Whether a follower holds off as a leader does.* D-065 says the leader keeps its
   hold-off and says nothing about a follower's. **No hold-off**, because the reason for
   the leader's is a checkpoint that stalls every range's applies for its duration
   (D-036), and a record is one synced batch. Stated in RAFT.md.
2. *What the trace says about the record.* The first build traced
   `RaftSnapshot { taken: false }` for it, which is what an install traces. That quietly
   turned the raft sweep's `snapshots_installed` from 19 359 into 75 742 over a thousand
   seeds, and — worse — it fed the checker's applied floor (D-030), so a follower's
   compaction raised the floor past what `ApplyBeforeCommit` had applied without
   committing and state machine safety stopped seeing the variant on 125 seeds of a
   thousand. **The record traces nothing of its own**: the transition is the
   compaction, which the core traces as `RaftCompacted` once the prefix's deletes are
   durable, and a crash between the two is reported by the restatement at the next open,
   where a take's crash window is reported. It is the smaller change and the one that
   keeps a variant's catch where it was. A new event kind would have been the
   alternative; it is not needed and would collide with §8's own.

   It does **not** leave every existing counter meaning what it meant, as this entry
   first claimed; the review of this slice was right to strike that. The record surfaces
   in the trace one open later, as the re-statement of a durable prefix. Before D-065 a
   replica that had neither taken nor installed opened with `snap_index == 0` and
   re-stated no snapshot at all; now every replica that has compacted re-states
   `RaftSnapshot { taken: false }` at every later open. The raft sweep counted that whole
   population as `snapshots_installed`, so it reported installs that never happened —
   **10 788 re-statements against 9 342 real installs** over a thousand seeds, which
   together are the 20 130 this entry first printed as installs. The counter is now split
   (`raft::Report::snapshots_installed_and_restated`, which reads a re-statement off its
   position: the `apply` loop traces the durable log as a `RaftTruncate` to one past its
   end and then, if there is a prefix, the snapshot standing in for it, with nothing of
   that server's between the two). The sweep's `snapshots_installed > 0` assertion is a
   statement about installs again, where any re-statement satisfied it before.
3. *What a follower does when the record names an index its log no longer holds.* It
   cannot happen on the correct system and does under `ApplyBeforeCommit`.
   `maybe_compact` **returns rather than draining past the log's end**, so the variant is
   caught by the checks it is there to be caught by rather than by a panic in `drain`.
   The real property is asserted over the trace instead, below. No sweep reaches the
   branch — at a thousand seeds `ApplyBeforeCommit` never gets there, and dropping the
   guard changes nothing any sweep sees — so it is held by a directed test instead:
   `a_record_past_the_log_compacts_nothing` in `crates/ananke-raft/tests/snapshot.rs`
   hands a replica that is not leading a record five entries past an empty log and
   asserts nothing is compacted, nothing is traced and the prefix does not move. Without
   the guard that test panics in `drain`.
4. *The take counter on a follower's record* (above): carried forward, never reset.

**The assertion D-065 asks for.** "A follower's applied index never passes its commit
index on the correct system, so nothing uncommitted is dropped. Assert it."
`raft::compaction_stays_committed` folds the trace: every `RaftCompacted { server,
through }` is at or below an index that server knew committed — its own `RaftCommit`s,
and the last index of any snapshot it restated or installed, whose entries are committed
by construction. It is asked of every server, not only followers, since a leader's take
is at its applied index too, and it runs inside `Report::check` on every seed at every
tier. It is placed last among the folds so that a run a Phase 2 variant already fails
still fails with the violation its pinned seed names.

Its pair, under CLAUDE.md's rule, is `ApplyBeforeCommit`, the known-buggy variant that
breaks exactly the step the property rests on: it hands the `apply` task the entries of
a persist as they become durable, before any commit index reaches them, so a follower's
record names an index nothing has committed.
`a_server_that_applies_before_commit_compacts_past_its_commit_index` asks the fold of
that variant on its own, rather than through `check()`, which returns state machine
safety first on most seeds. **The fold catches it on 999 of 1 000 seeds**, so its
assertion sits at every tier, not at the thousand-seed one (D-061); the correct system's
side of the pair is `Report::check`'s own fold, silent on every seed of every sweep. The
test names its nightly shard in `scripts/nightly-shards.txt` in this commit (D-064),
weighed at `ANANKE_SEEDS=1000 ANANKE_DEEP_SEEDS=100` in release at **50.7 cpu s**
(48.20 user, 2.46 sys) with the machine at load 69.71/57.02/39.49 — other agents'
slices building on it — and placed in shard 5, the lower-numbered of the two lightest.

**Measurements.** All on an 8-core Apple M2, **on AC power**, in release, with other
agents' slices building on the same machine; the load average is given beside each. The
tree is this branch, cut from `ae75bdf`; the "before" figures are `ae75bdf` itself, run
the same way by `git stash`. The command is
`ANANKE_SEEDS=1000 cargo test --release -p ananke-sim --test raft <test> -- --exact --nocapture`.

*The follower log (Stage B's exit; Q39).* The largest in-memory log any replica held
while it was not leading, in entries, folded from the trace by `LogShape`. The fold is
not quite `last_index - snap_index`: a take moves neither, and only the `RaftCompacted`
that follows it does, for the reason the type's own notes give. The review of this slice
measured the first build of the fold against the core itself and found it reading the log
*shorter* than the core held it on 27 of 1 000 seeds, by up to 12 entries — the wrong
direction for a bound — so a take no longer moves the fold's prefix.

- **342 entries, 28.5 × `snapshot_threshold`**, on server 1 of seed 514, over a thousand
  seeds (load 142/81/53, other agents' slices sweeping). The maximum is the same entry
  and the same seed the review found by instrumenting the core directly, so the headline
  is ground truth and not an artefact of the fold. The distribution of the per-seed
  maximum, in multiples of the threshold: 1 × on 223 seeds, the bulk at 4 × to 8 × on
  561, then 50 seeds at 12 × or more, 13 at 16 × or more, 8 at 20 × or more, and one at
  28 ×.
- With `Variant::FollowerNeverCompacts` — the server as it was built before D-065, now a
  variant in the tree rather than a local edit — the same sweep at the same tier reaches
  **878 entries, 73 ×**, on seed 512, and would have gone on growing with a longer run: a
  follower's log had nothing to bound it (SHARD.md:339-340 — 337-338 are the sentence
  about *how* a follower's log shrinks, not the one about its growth).
- The bound asserted is **64 ×**, `raft::FOLLOWER_LOG_MULTIPLE`, on every seed at every
  tier. 64 × is 768 entries: 2.25 × over the measured maximum and below the 878 the same
  sweep reaches with the compaction off, so it is not a vacuous bound.
- **The tail is not geometric, and this entry's first risk model was wrong.** It said ten
  thousand seeds should reach about 38 × and pass 48 × about one run in ten, putting the
  nightly's chance of reaching 64 × near one in 470. The nightly it cited says otherwise:
  ten thousand seeds reached 28 ×, on seed 514 — the identical seed and the identical
  maximum as one thousand. The tail is truncated, not geometric, and the margin is larger
  than the model claimed, not smaller. No number is quoted for it here: the honest
  statement is that neither a thousand nor ten thousand seeds has come within 2 × of the
  bound, and the nightly on this tip re-states the distribution on the corrected fold.
- The multiple is this scenario's, at its threshold of 12. What it really bounds is the
  follower's apply lag in entries, which the threshold does not scale; at the server's
  own 4 096 the same lag is a fraction of one threshold, so 64 × read as a production
  figure is conservative, and SHARD.md's exit asks for the multiple of 4 096. **This is
  the one number the owner may want to look at**, and the ten-thousand-seed figure above
  says the conservatism is greater than this entry first claimed.
- **Its pair.** `Variant::FollowerNeverCompacts` is caught on **5 of the first thousand
  seeds** — 116, 429, 512, 577, 757 — and on every one of the five it is this bound that
  catches it. Half a per cent is far too thin for the gate's twenty seeds, so the pair is
  pinned rather than swept (D-061): `a_replica_that_never_compacts_outgrows_the_follower_log_bound`
  runs **seed 512** both ways, asserts the violation names this bound and that the log is
  past it, and asserts the correct half of the same seed passes inside it. 512 is the one
  of the five with the most room — 878 entries against 768, where seed 116, the lowest of
  the five, holds 788 — because a pin two per cent over a bound would go quiet at the
  next schedule move and say nothing about it. Before this
  variant existed the bound was unfalsifiable inside the tree: the review widened
  `FOLLOWER_LOG_MULTIPLE` from 64 to 4 096 and the whole raft suite passed at 20 seeds
  and at 200.

*The three D-065 names to re-measure, before and after, at a thousand seeds.*

| | before (`ae75bdf`) | after |
| --- | --- | --- |
| `ApplyBeforeCommit` caught | 882 of 1 000 | **1 000 of 1 000** |
| `TruncateOnEveryAppend` caught | 999 of 1 000 | 999 of 1 000 |
| membership `reverts_to_a_prefix` | 25 | 35 |
| membership `truncation_reverts_to_a_prefix` (the core's revert floor) | 0 | 0 |
| membership `config_reverts` | 60 | 54 |
| membership compactions | 15 339 | 48 411 |
| raft-sweep compactions | 35 031 | 91 386 |

- `ApplyBeforeCommit`'s catch rises to every seed, and the way it got there is the
  evidence for the second settled point above. With the record traced as
  `RaftSnapshot { taken: false }` — the first build — the rate was **875** of 1 000,
  where `ae75bdf` gives 882. The checker sets its applied floor from that event
  (D-030), so every follower compaction was raising the floor past entries the variant
  had applied and never committed, and state machine safety could no longer see them:
  the trace was hiding the bug. With the event gone and the compaction reported by
  `RaftCompacted` alone, the rate is **1 000** of 1 000. The new fold catches the variant
  on 999 of 1 000 by itself, printed by its own test.
- `TruncateOnEveryAppend` is unchanged at 999 of 1 000. Its window does shrink on a
  follower — a truncation never reaches below the prefix, and the prefix is now the
  applied index — but the variant truncates on *every* append, so a truncation that
  removes a committed entry is still found on all but one seed.
- D-029's revert floor. **This entry's first figure for it was wrong, and in the way
  D-039 warns of: it was structural, not observed.** It read
  `follower_compactions_swallowing_the_config` as "a compaction through an index at or
  past the server's *last* `RaftConfig`", and that is true of every compaction by
  construction — `Raft::new` sets `membership_index` to `snap_index` when the log holds
  no configuration entry, 0 at a first open — so the counter was a copy of the total
  under another name. It read **58 228 of 58 228**, and it would have read 100 % on a
  tree with D-029's floor code deleted. The review of this slice caught it. The measure
  is now observational: a compaction that carried the prefix from `prev` to `through`
  swallowed a configuration entry only when the server holds one at an index strictly
  inside that step, `prev < index <= through`.
- On the corrected measure, **the raft sweep cannot evidence this claim at all**: it is
  **0 of 58 228**, because the raft scenario appends no configuration entries. The claim
  is true, and it is true where configuration entries exist — the **membership**
  scenario, at **5 437 of 33 270 follower compactions, 16.3 %**, on **1 000 of 1 000**
  seeds. The review took the same figure from the other side, by instrumenting
  `maybe_compact`'s own `drained.is_some()` in the core: 5 389 of 33 279, 16.2 %. The two
  agree to about one per cent, which is the trace's view of a role lagging the core's.
  That is what D-065 predicted, at its real size: the floor a leader's compaction reached
  on 3 of 10 000 seeds is reached by about one follower compaction in six, on every run
  that changes its membership. It is not "load-bearing on every follower of every run",
  as this entry first said — on a run with no configuration entry it is load-bearing on
  nothing.
- What has *not* changed is how often a truncation actually reaches the floor:
  `truncation_reverts_to_a_prefix` is 0 of 1 000 before and after, and the 3-of-10 000
  figure issue #56 records is the nightly's, which this branch's nightly will re-state.

The correct system's own figures are the same on the tree with the record's trace and on
the tree without it, to the entry and the seed — the follower-log maximum, its whole
distribution, every compaction count — because a trace event is an observation and draws
nothing. Only what the checker could *see* moved.

*What moved beside them, at the same tier:* `snapshots_taken` 38 200 → 37 085, `applies`
1 362 871 → 1 309 687, `commits` 994 478 → 973 363. Follower compactions: 58 228 over a
thousand seeds, on every seed.

`snapshots_installed` was first reported here as 19 359 → 20 130, "the restatements of
the extra opens the moved schedules make, not a change of install behaviour". That was
wrong on both halves and the review struck it: the 20 130 is **9 342 installs and 10 788
re-statements**, and the rise is exactly this change putting a prefix under replicas that
had none, not the schedules. The two are now counted apart (settled point 2 above), and
`snapshot_prefixes_restated` is the sweep's name for the second.

**What moved, and the re-audit.** This change moves every schedule: a follower writes a
snapshot record and deletes log keys where it wrote and deleted nothing, so every
simulated disk draw from the first compaction on is another draw and the run after it is
another run. Eleven pinned tests in `sim/tests/raft.rs` moved, and every pinned seed was
re-audited in the commit that moves them (CLAUDE.md:58-67). Each now asserts its
mechanism, or, with the reason, the situation's absence:

- **Seed 2605** — the pair has come *back*. `ResetTimerOnAnyRpc` is caught here again:
  the run's majority is up at its end, so D-035's carve-out no longer withholds the
  timer bound, and the replay's five gaps (none holding a completed install) make the
  first the run's violation. The pin asserts the catch in the check's own words, in
  place of the absence it asserted before. The correct half is where it was: 20 adoption
  windows, no gap at all without D-063's arm.
- **Seed 687** — the fix's half is still here but has moved to **server 1** and its last
  step has changed: the open at 18.853009241 s drops table 74, the engine is quiesced at
  18.876708665 s, the node restarts at 18.896 s, and the second open drops table 74 again
  and refuses at 18.958974816 s *for the loss it found itself*, not for the mark the
  first refusal wrote. The pin asserts that, asserts the refusal-on-the-mark shape absent
  after the quiesce with this reason, and keeps the marker's own survival non-vacuous
  through the earlier refusal of the same store at 16.730362789 s, which names the
  `RAFT-STORE` marker.
- **Seed 5909** — the stream half is still out of reach under every variant, asserted
  with its non-vacuity. D-042's half moved again: `IgnoreIncarnation` still leaves the
  leader's progress for server 3 stale, the pair now leaves nothing stale, and **no
  variant leaves anything uncounted after the last heal**, asserted empty everywhere.
- **Seed 132** — the reason the stream half is out of reach moved down one step. An
  index *is* taken twice now, under the runs carrying `SharedSnapshotDir` and only
  those, but no such re-take lands under a live stream, which
  `retakes_under_streams().is_empty()` says directly — a stronger statement than the one
  it replaces. The refusals moved too: the correct server and `IgnoreIncarnation` refuse,
  the two runs sharing one snapshot directory refuse nothing, so D-042's half is reached
  under `IgnoreIncarnation` alone where it was reached under the pair.
- **Seed 680** — both halves have left it. No run takes an index twice, nothing is
  re-taken under a live stream, and nothing is left uncounted or stale under the pair or
  either half. Asserted with its non-vacuity.
- **Seed 102** — see the finding below.
- **Seed 119** — it refuses a store again, under both servers: a start that finds a
  `RAFT-STORE` marker it cannot read. That is damage the start finds, not the lost-state
  refusal the variant is about, and no recovery here reports lost state, so the variant
  still has nothing to do and neither run is caught. The pin now asserts the refusal
  present, no lost-state refusal, nothing dropped and no engine quiesced — and it gains
  the companion for non-vacuity it could not have before.
- **Seed 1 and seed 4 of the term-raise schedule** — seed 1 still reaches D-047's shape
  and not D-050's, with **seven** straddles now rather than five, the first server 2's
  from term 1 to 2 at 1.24716 s, caused by an AppendEntries rather than a RequestVote.
  Seed 4 still holds exactly one received-straddle and keeps D-050's pin, with new
  numbers: server 2, terms 1 to 2, received 1.224602510 s, isolated from 1.22461 s,
  stepped at 1.225627305 s.
- **The nightly's eleven** — `UNCOUNTED_STEP_DOWN` is now empty: none of the eleven
  steps a leader down leaving a follower uncounted, where seed 3087's run did. Both
  seeds whose named isolation still comes are unchanged (5203, 6691).
- **The nightlies' 28 removed catches** — six still hold the isolation their catch
  named, a different six: seed 5918's of server 3 from 9.532 s is gone and seed 4814's
  of server 3 from 12.111 s has come back. Four of the 28 are now caught over their own
  variants' bugs where three were, and a different set: seed 9557 under `AdoptionAsBuilt`
  passes outright, seed 5153 under `ResetTimerOnAnyRpc` and seed 6366 under
  `ApplyBeforeCommit` are caught where they were not, and seeds 2305 and 6717 keep their
  catch with new numbers. Seed 5153's own block changes with it: its replay by durability
  time finds four gaps rather than three, the nightly's own still not among them, and the
  timer bound *is* asked of the run now, so the first of the four is that catch.

**The review's fixes move no schedule, so no pinned seed is re-audited for them.** Every
change made for the review is either read-only over the trace or inert on the correct
system: `LogShape` and `follower_compactions` fold records and decide nothing;
`snapshots_installed_and_restated` is a new fold; `compaction_record` is the record the
`apply` task already wrote, given a name; and `Variant::FollowerNeverCompacts` guards the
follower trigger with a set the correct server's is empty of, so the correct server takes
the same branch it took before. The correct raft sweep fails 0 of 1 000 seeds, and the
follower-log maximum is the same 342 on the same server of the same seed as before the
fold was corrected. The re-audit below is the one the original commit owed, for the
schedule move that D-065 itself makes, and it stands.

**A search for a moved mechanism, as CLAUDE.md asks, and what it found.** Two findings
go to the owner; neither is a bound the correct system trips and neither is a
correct-server failure.

1. **`RefusalNotDurable`'s own mechanism is no longer reached at a thousand seeds.**
   Seed 102 pinned D-044's shape — a refused engine that goes on flushing launders the
   evidence of the loss, and the next clean open's restatement claims an applied index
   its log cannot account for — at every tier, because the sweep asserts the catch only
   from the thousand-seed tier. The search over seeds 0..1000 in release, for a run whose
   verdict is a state machine safety violation *and* that crashes a refused server *and*
   restarts it after a lost-state refusal before any install, finds **0**; on `ae75bdf`
   it finds **7** (seeds 102, 293, 378, 465, 744, 893, 926). The variant is still caught,
   and more often — **58 of 1 000 against 26** — but every one of the 58 is now the
   `match starts` oracle, where the 26 were 19 by the oracle and 7 by state machine
   safety. The cause is D-065 itself: a follower's compaction leaves a snapshot record,
   so the laundered store that opens fresh has a prefix to account for its applied index
   with and the restatement no longer contradicts its own log; the oracle catches the
   other consequence of the same bug, a store that lost its refusal taking a new
   incarnation. **The variant's Phase 2 standard — the catch from the hundred-seed tier
   (§10) — still holds, at 5.8 %.** What no longer holds is that a pinned seed keeps
   D-044's named mechanism at the gate's twenty. Seed 102's test asserts the absence with
   this reason until the owner says otherwise.
2. **The pair `{IgnoreIncarnation, SharedSnapshotDir}` has no wedge seed in the first
   thousand, before or after.** SHARD.md §12 asks, at a move of seed 680's schedule, that
   the first thousand seeds be searched again for a seed the pair is caught on and that
   the answer go to the owner if none is found. On this tree the pair is caught on four —
   332, 796 and 848 by liveness, 847 by linearizability — and on **every one of them
   `SharedSnapshotDir` alone is caught too**, so none needs both bugs. The same search on
   `ae75bdf` finds one, seed 954, where `SharedSnapshotDir` alone is caught as well. So
   the absence predates D-078 and is not this change's doing; it is reported because the
   pair is Phase 2's control for a wedge that needs both bugs (D-045).

**The nightly on this branch's first tip was red, and the two failures are the owner's
to rule on.** Run 35597322808 on `85412c6` failed shards 5 and 6 on three seeds above a
thousand, which is why the premerge at a thousand was green. `origin/main` passes all
three. Both were reproduced here from a `git archive` of each tree, in release, and both
were diagnosed to the end. **Neither is a fault of this slice's code, and neither is
fixed here**, because fixing either means changing a check that a merged decision owns —
which is the owner's call under D-030 and D-039, not a slice's.

1. **Membership seed 7205: `match_starts_are_first_rises` (D-069) tripped by the correct
   system.** Filed as **issue #81**. Under leader 3 of term 2 the scenario drives grow,
   shrink, grow: `change-accepted [1,2,3,4,5] applied 151`, then `[1,2,3] applied 156`,
   then `[1,2,3,4,5] applied 161`. `on_change` treats a target that is not in
   `self.membership.voters` as a learner and re-inserts its `Progress` with
   `incarnation: None` and `match_started: false`, so the re-grow rebuilds server 4's
   progress although its store incarnation never changed, and the next rise is traced as
   a first rise under incarnation 1 for the second time. The leader really did begin
   tracking server 4 afresh, and SHARD.md §8 says that is what the event is for — "on a
   re-add, a success from the collected replica stamped with its own incarnation would
   show here". The oracle's key, `(leader, term, follower, incarnation)`, is what is
   incomplete. It is a model error in the check, latent on `origin/main`, which this
   slice only moved a schedule onto. The review's reading — that D-065's revert floor
   drops and re-adds a voter — is not what the trace shows; the shrink is an ordinary
   membership change. The fix proposed in #81 is to have `on_change` trace
   `RaftProgressReset` when it replaces a `Progress` it already held, and to have the
   oracle forget that follower's keys when it sees one; the check keeps all of its power,
   since D-069's own mutation leaves no reset between the repeated rises.
2. **Raft seeds 3085 and 4065: the linearizability checker's search budget, not a
   violation.** Filed as **issue #82**. Both histories **are** linearizable: with
   `sim::lin::BUDGET` raised in a throwaway copy, 3085 linearizes at 4 000 000 states and
   4065 at 400 000 000, against the 2 000 000 in the tree. The budget is not a knife
   edge — `lin.rs` instrumented to report the deepest search of each run gives a worst
   case of **2 562** states over a thousand seeds on this branch and **7 423** on
   `origin/main`, three orders of magnitude inside the cap, with no seed on either tree
   over 500 000. The cost distribution is close to bimodal: almost every history is free
   and a very small minority explodes, and main's worst thousand-seed history is the more
   expensive of the two, so nothing says this change makes histories harder. Two of ten
   thousand schedules landed in the tail. `BUDGET` is **not** raised: at 400 000 000 the
   checker is 200 × slower on every seed and the next tail history is past whatever
   number is chosen (D-030, D-039). #82 proposes committing forced prefixes without
   backtracking, dropping pending operations that cannot matter, and reporting an
   exhausted search as *undecided* with a visible count rather than as a failure.

Until the owner rules on both, **this branch's gate — a nightly green on its tip — is not
met, and the pull request is not mergeable.** Everything else the review found is fixed
below.

**What the review changed, beyond the two above.** Every one of its findings that was a
defect in this slice's own code, tests or entry is fixed, each with the mutation or run
that proves it:

- The D-029 measure, structural rather than observed (above). Now observational; the
  raft figure is 0 and the claim is restated where it is true, in the membership
  scenario.
- `FOLLOWER_LOG_MULTIPLE` had no pair: `Variant::FollowerNeverCompacts` is the server as
  it was before D-065, and seed 512 is pinned against it (above). The review's M11 —
  widening the bound from 64 to 4 096 — now fails that test.
- `snapshots_installed` no longer counted installs (settled point 2 above), and
  RAFT.md's `RaftSnapshot` row said the compaction emits the event, which it does not.
  Both corrected; the row and this entry now say the same thing.
- The record's own shape and the past-end branch were untested, four mutation survivors.
  `compaction_record` in `node.rs` is now a named function with its own unit tests
  (`taken` false, no directory, the counter carried forward), and
  `a_record_past_the_log_compacts_nothing` holds the guard. The follower trigger's two
  clauses, which the review found "caught only by pinned-seed schedule sensitivity", are
  now held by `a_replica_that_is_not_leading_records_at_its_applied_index`: a log exactly
  the threshold past its prefix asks for nothing, a replica that has applied nothing asks
  for nothing, and the record that lands compacts in the same step.
- `LogShape` read a follower's log *shorter* than the core held it on 27 of 1 000 seeds
  (above). A live take no longer moves the fold's prefix and a re-statement still does,
  which is `Restating`; the maximum is unchanged at 342 on server 1 of seed 514, which is
  the core's own ground truth. `LogShape.committed`, written by the fold and read by
  nothing, is deleted.
- The citations that did not land: the repair's record is `snapshot.rs`'s, not
  `node.rs`'s, and it is *not* the same shape in `take`; SHARD.md's unbounded-growth
  sentence is 339-340, not 337-338. Both corrected above.

**Two mutations are left standing, and are recorded as such rather than as verified.**
The other three survivors on this slice's code are killed by the tests above.

- The review's M10 — `Event::Recorded` also clearing `fresh_take` — survives. The arm's
  reasoning is that no version was written, so nothing is swept and the flag is left as
  it was; whether clearing it too would change anything depends on the node loop's
  state, and reaching it needs a test over the loop, which this tree has no shape for.
  It is stated here as an untested decision.
- The review's M12 — dropping the `RaftSnapshot` arm from `compaction_stays_committed` —
  still survives the whole raft suite (46 tests, 20 seeds), re-measured on the tree that
  ships. It is left standing deliberately, because the mutation makes the oracle
  *stricter*, not weaker: the arm only ever raises the floor a compaction is compared
  against, so dropping it can produce a false failure and can hide nothing. That it
  survives says the arm is not load-bearing on the correct system at that tier — a
  compaction's index is always covered by a `RaftCommit` the same server traced — and
  the arm is kept for the install case, where a prefix is committed by construction and
  no `RaftCommit` of that server need name it. A pair for it would have to be a variant
  that compacts past an installed prefix, which is `ApplyBeforeCommit`'s ground and
  already covered.

**RAFT.md.** Updated where it describes what changed (D-053): the paragraph after "A
leader compacts the Raft log to its last checkpoint…" now says what a follower does,
which supersedes RAFT.md's rule that only a leader compacts, as Stage B's question 1
asked, and adds that the record traces nothing of its own. `RaftSnapshot`'s row first
said the event is emitted for "a snapshot recorded by a follower's compaction", which
contradicted this entry's settled point 2 and was wrong: the compaction emits none. The
row now says the event is a take, an install or a re-statement, and that a compaction's
record surfaces only as the re-statement of the next open. The variants table gains
`Variant::FollowerNeverCompacts` and its count of rows goes from sixteen to seventeen.

**The premerge**, re-run on the tip that carries the review's fixes, quoting its own
machine lines:
`premerge: Darwin 25.6.0 arm64, Apple M2, 8 cores`;
`premerge: before, load 10.06/10.07/28.14, AC Power, no thermal warning recorded`;
`premerge: after, load 29.42/23.10/22.65, AC Power, no thermal warning recorded`;
`premerge: green at 1000 seeds in 1279 s`. **On AC power** throughout. The first build's
premerge was green in 1410 s at loads of 20 to 135; this one is faster because fewer of
the stage's other slices were sweeping, not because anything here got cheaper. Neither
figure is comparable with D-071's 625 s on an otherwise idle machine, and both are
recorded with their loads for that reason (D-070). Every rate quoted above is from one of
those outputs or from a run made the same way.

**What is not done.**

- The nightly on the fixed tip is dispatched, not waited for; its ten thousand seeds are
  what test the 64 × bound, what re-state the follower-log distribution on the corrected
  fold, and what will re-state issue #56's 3-of-10 000 revert-floor figure. **It will
  still be red on the three seeds above**, which are issues #81 and #82 and not this
  slice's to fix.
- Issues #81 and #82 are filed and open. Neither is fixed here, and the branch's gate is
  not met until the owner rules on them.
- The apply-task hold D-065 asks Stage B to measure "with and without follower
  compaction" is the node's measurement, on the node's four ranges, and is not this
  slice's: there is one range here, and C's whole point is that it takes no checkpoint,
  so the figure this slice could produce would be the leader's hold unchanged.

---

## PROPOSED D-082 — `sim/raft.rs`'s arms on the node: one set of arms, two clusters

**The number.** SHARD.md's Stage B plan does not number this entry, and the footer on
`main` reads D-079. Three branches open at the same time are each taking the next free
number against their own copy of this file — the re-seed slice (PR #86) writes D-077,
the match-starts fix (PR #89) D-079 and the linearizability fix (PR #87) D-080 — so a
number taken from the footer here would collide with one of them at the merge. This
entry takes **D-082**, past all three, and the footer moves to D-083. Every code site
carries `// PROPOSED(D-082)`. The integrator may renumber it at the merge; nothing in
the tree depends on the number beyond those markers, this heading and the footer.

**Context.** SHARD.md §12's Stage B has, as its **first** exit criterion,
"`sim/raft.rs`'s arms, `sim/membership.rs` with #46's extension, and `sim/quorum.rs`,
each run on the node with four ranges on every node, each range placed as today's one
group is (§10): the correct system passes every seed at every tier"
(SHARD.md:2280-2290). §10 adds that Phase 2's sixteen variants are re-asserted there
"on the same arm or directed scenario run with several ranges per node"
(SHARD.md:1573-1580), each "to the standard its Phase 2 test asserts and no stronger,
at the tier each uses today".

Every slice of Stage B so far has left it owed, and D-076 said so in as many words
when it added the node beside the one-group server:

> **`sim/raft.rs`'s arms, `sim/membership.rs` and `sim/quorum.rs` do not run on the
> node.** They run on `ananke_raft::run`, the one-group server, exactly as before. …
> Moving those three scenarios onto the node needs what the node has not got yet — the
> `snapshot` task keyed by range and follower, the install, Q15's refusal and re-seed,
> follower compaction — because a third of `sim/raft.rs`'s arms aim at exactly those
> paths (`CrashInstalling`, `RetakeUnderStream`, the re-seed arms), and `sim/quorum.rs`
> is a re-seed scenario from end to end. … **Stage B's first exit criterion is
> therefore owed**, and with it the re-assertion of Phase 2's sixteen variants on the
> node.

That sentence is still true of this tree, and it is the fact this entry has to be
designed around. `ananke_shard::snapshot` exists and is tested (D-075), but it is
**not wired to `ananke_shard::server::ServerHost`**: a core that asks for a snapshot
action gets `Gaps::snapshot_actions += 1` and nothing else (`server.rs:570`), and a
store refused for lost state stops the node (`server.rs:697-699`). No branch open
against `main` wires either. So this slice can move two thirds of the arms and cannot
move the third that aims at the paths, and the question this entry settles is what
shape does that without losing what Phase 2 already asserts.

### The question

Do `sim/raft.rs`'s arms **move onto the node in place**, or does the node grow a
**second scenario** and the arms are **shared**?

**Moving them in place** is what the plan's own wording suggests — SHARD.md's Stage B
names "the node itself" as one of three commits of the stage that may move every
schedule, and asks each of the three to re-audit every pinned seed in
`sim/tests/raft.rs` (SHARD.md:2361-2370), which only makes sense if `sim/raft.rs`'s
own sweep is what is moved. It costs nothing in duplication: one scenario, one
schedule, one set of pinned seeds.

It is not possible on this tree, and the reason is not a matter of taste. A third of
`sim/raft.rs`'s arms — `CrashInstalling`, `CrashAdopting`, `CrashRefused`,
`RetakeUnderStream` — aim at the install, the adoption and the refusal, and with them
go the Phase 2 assertions of `SnapshotWithoutCurrentLast`, `AdoptionAsBuilt`,
`RefusalNotDurable`, `IgnoreIncarnation`, `SharedSnapshotDir` and the pair
`{IgnoreIncarnation, SharedSnapshotDir}`, and the pinned seeds 2605, 6325, 5909, 132,
680, 687, 102, 158, 119 and the nightlies' thirty-nine hold them to what they do. On a
node with no snapshot task and no refusal, every one of those arms reduces to an
isolation or an ordinary crash and every one of those assertions becomes unassertable.
§10 is explicit that this is not allowed: "A variant the sweep does not catch is a hole
in the sweep, not a variant to delete." Moving in place would delete six variants' worth
of Phase 2 coverage to buy a re-assertion of seven others.

### The decision

**The arms are shared, and the node is a second cluster of the same arms.**

1. `sim/raft.rs` gains `Cluster`, an enum of two: `OneGroup`, today's
   `ananke_raft::run`, and `Node`, `ananke_shard::server::run` with four ranges on
   every node. Everything that differs between them is on that enum and is small —
   how a server is spawned, which ranges it holds, how many keys a client draws from,
   the map from a key to its range, how a request and a response are encoded, and
   whether the disk rots. **Nothing else differs.** `run_with` becomes `run_on`, one
   body: which arm fires when, what it waits for, which server it aims at and what it
   heals is the same code whichever system is underneath, so the two clusters cannot
   drift apart. A second copy of the Figure 8 driver would be the alternative, and a
   second copy is exactly what goes stale.
2. **The one-group sweep is left running exactly as it is**, and its draws are
   byte-identical. `Schedule::draw` draws from the same streams in the same order it
   drew from before `Cluster` existed; the client's per-range leader map holds one key
   and spends the same dice; the lease trial's operator keeps sequence numbers 0 and 1
   on sockets 1 and 2. The evidence is not an argument: seed 42's moirae JSONL, 12 898
   025 bytes of it, has SHA-256
   `445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on this branch
   and on `origin/main`. So every pinned seed in `sim/tests/raft.rs` still runs the run
   it was pinned on, and every Phase 2 assertion the node cannot host keeps standing
   where it stands.
3. **The node's sweep is `sim/tests/node.rs`**, `raft::run_on_the_node`. It runs the
   arms the node has a path for — the two lease trials, `Isolate`, `IsolateLeader`,
   `OneWay`, `Crash`, `CrashLeader`, `StaleSender` and `FigureEight` — drawn from
   `Schedule::draw_on_the_node`, which is `Schedule::draw`'s own draw with the four
   arms above taken out. They are taken out rather than left to no-op because an arm
   that waits out `INSTALL_WAIT_BUDGET` and then fires as an isolation costs the tier
   its time and asserts nothing.
4. **A leader-relative arm on the node draws its range from its own stream**, as §11's
   env item 8 asks (`Schedule::range_picks`, stream `arm-range`). "The leader" on a
   node of four ranges is a question about a range: node `a` leads one while node `b`
   leads the one beside it, and an arm that cut off "the leader" without saying of what
   would cut off whichever range elected last. The draw is spent only on the node —
   `Schedule::draw` leaves `range_picks` empty and `Schedule::range_of` answers the
   only range one group has — which is why (2) holds.
   The Figure 8 driver's burst and the re-take driver's fill write keys **of the range
   their arm drew**, prefixed with that range's first key so the fixed map routes them
   there and they sort inside the span `RangeCreated` names: the backlog the driver
   needs is a backlog of *that range's* log.
5. **The lease trial hands over every range the node holds**, not one. The trial is
   about a lease the slowest clock holds while it is cut off with a reading client; a
   node that led one range of four would leave the client asking three other nodes for
   the rest, and `LeaseTrustsTheClock`'s window would be a quarter of what it is on one
   group. One group has one range, so this is exactly the one transfer it always sent.
6. **What the node has not got is asserted absent, with its reason, on every seed**
   (CLAUDE.md:58-67): the node cluster's `snapshot_threshold` is far above what a run
   writes and its disk does not rot, and `Report::check`'s node clauses fail any seed
   that traces a snapshot action or a refused store, naming the slice that owns the
   path. The day either arrives the sweep says so instead of passing over it.

7. **Q41's round honours `SendBeforePersist`, which it did not.** §10 calls this the
   Phase 2 variant that matters most against the round — "a send that follows a
   core's persist leaves when that persist resolves, and the variant sends it first"
   — and on the node it was **a no-op**. The bit was set, the core carried it, and
   nothing on the node read it: `Variant::SendBeforePersist` is implemented in the
   one-group server's `execute` (`ananke-raft` node.rs:2520) and the node has an
   `execute` of its own. The first run of the node's sweep caught it on 0 of 20
   seeds, which is what a variant that is not injected looks like. `Cores::step` now
   reads it off the core's own configuration and lets that core's sends leave with
   the round's early outputs while its `Apply`, its reads and its trace events still
   wait for the persist — which is the variant, and not
   [`NodeVariant::DeferredFlushedEarly`], whose whole point is that it hands out the
   others early too. With it the variant is caught on 20 of 20 seeds.

   This is the reason a re-assertion has to be run and not argued. Nothing in the
   tree was wrong: the node had never been asked to carry the variant, so nothing
   had ever been silent about it. What would have been wrong is a sweep reporting
   the variant re-asserted on the node.

### What the other two sweeps have to do

`sim/membership.rs` and `sim/quorum.rs` are the next two PRs' and are built on this
harness. What each has to do, and what it gets for free:

- **Free**: `Cluster`, `Schedule::draw_on_the_node`, `Schedule::range_of`,
  `leader_of_range`, `leader_of_the_cluster`, `config_on`, `node_server_config`,
  `client_on`, `spread_on` and the node's key map, and `Report::over_a_run` for a
  scenario that drives its own faults (D-076).
- **`sim/membership.rs`** drives its own scenario rather than `raft::run_on`: it keeps
  a `Driver` with a `Sim` of its own and asks `leader_now` of it in four places. Each
  becomes `leader_of_range(&self.sim, range)` for the range the change is about, and
  its client becomes `client_on(cluster, …)`. Its changes are `Command::Change`, which
  `ananke_shard::server` already takes per range (`server.rs:342`), so the membership
  arm needs no new node code — and #46's extension (a change of a range while another
  range on the node is changing) is the thing four ranges make possible and one group
  could not. `SingleMajorityInJointConsensus` is re-asserted there.
- **`sim/quorum.rs`** is a re-seed scenario from end to end: it refuses a server by its
  store's lost mark at a restart and counts what the leader does while the re-seed
  runs. It therefore **stacks on the re-seed slice (PR #86)**, which builds the
  whole-node refusal and the per-replica marks, and it cannot start before that lands.
  `RefusedCountsForQuorum` and `RefusedNeverCounts` are caught on every seed at every
  tier today because the scenario builds their situation directly, and that standard
  holds on the node: it wants four ranges' re-seeds against a node's receive cap, which
  is the shape SHARD.md's Stage B calls the directed re-seed shape.
- **Neither** should re-draw `Schedule::draw`. A sweep that wants the node's arms takes
  `draw_on_the_node`; a directed scenario builds its own `Schedule` as it does today and
  fills `range_picks` itself.

**The machine, for every figure below** (D-070). Darwin 25.6.0 arm64, Apple M2,
8 cores, **on AC Power**, no thermal warning recorded, with four other agents' slices
building on it throughout: load averages **72.97/133.34/149.19** before the run and
**67.23/128.14/146.95** after. **Every figure in this entry is a gate-tier (20 seed) or
CI-tier (100 seed) figure and is labelled as one. `scripts/premerge.sh` has not run**:
a thousand-seed figure taken at load 150 says nothing under D-070, and the owner is
scheduling it for a quiet machine. Nothing here may be read as a thousand-seed
measurement.

### The rates, every one measured before its assertion was written (D-061)

Phase 2's seven variants on `sim/raft.rs`'s arms, on the node, **at 100 seeds**. Six
run `high_rate_share()`, a tenth of the tier and never fewer than twenty, as
`sim/tests/engine.rs`'s variants do (D-055); the share at this tier is 20, and the rate
is over the share, as D-061 requires.

| Variant | On the node | Tier asserted | Phase 2's own standard |
|---|---|---|---|
| `SendBeforePersist` | **20/20 (100 %)** | every tier | every tier — matched |
| `ApplyBeforeCommit` | **20/20 (100 %)** | every tier | every tier — matched |
| `NoPreVote` | **20/20 (100 %)** | every tier | every tier — matched |
| `TruncateOnEveryAppend` | **20/20 (100 %)** | every tier | every tier — matched |
| `CountOlderTermForCommit` | **14/20 (70 %)** | every tier | every tier — matched |
| `ResetTimerOnAnyRpc` | **10/20 (50 %)** | every tier | every tier — matched |
| `LeaseTrustsTheClock` | **0/100 (0 %)** | `seeds() >= 1000` | `seeds() >= 1000` — matched |

`SendBeforePersist`'s row is the one to read twice. Before the round was taught to
honour it, it was **0/20** — not because the node was right, but because the node had
never been asked. The variant is implemented in the one-group server's `execute`, and
the node has an `execute` of its own.

`LeaseTrustsTheClock` at 0/100 is not a failure and not a weakening: its catch is a
stale read, caught on 4.0 % of the first thousand seeds on one group, and D-061 puts a
catch under 5 % at the thousand-seed tier. What *is* asserted at every tier is the
fault's firing, and it fires hard: the drift bound was exceeded on **52 of 100** seeds,
the correct node's guard revoked on **100 of 100**, and **8 039** reads were served by
a lease against **21 804** after a heartbeat round. A tier where the guard never
revoked would fail here rather than report a catch of nothing.

**The shape the keyed checks need**, at 100 seeds: leaders by range
{2: 767, 3: 747, 4: 773, 5: 722}; applies by range
{2: 31 264, 3: 36 021, 4: 32 403, 5: 33 776}; **270 688 peer frames carried messages of
more than one range**, about 2 707 a seed, read off the frames themselves; and
**115 of 115 leader-relative arms hit the leader of the range they drew**.

### The measurements SHARD.md §12 asks for under the sweeps

All on the node cluster, three nodes, four ranges each, under the arms above, on the
correct system. D-073 records these four as owed by "the slice that puts the node in
the scenarios", which is this one.

**The apply lag per range.** From a range's `RaftCommit` reaching an index on a node
to that node's `RaftApply` of it, in virtual time, which the sweep's disk latencies
drive. §4's threshold is on the **median** and is one heartbeat interval, 20 ms: Q14's
grouped applies are built if it is exceeded.

At **100 seeds**, over **133 464 applies**: the median over every range is
**3.044002 ms**, and per range **2.855565 ms** (range 2), **3.272496 ms** (3),
**2.961882 ms** (4) and **3.11965 ms** (5). At the gate's 20 seeds it was
**2.811332 ms** over 23 888 applies, which is the same figure at a sixth of the
evidence.

**It is not exceeded — 3.04 ms against 20 ms, a margin of 6.6× — so Q14's grouped
applies are not built**, and nothing goes to the owner on this one. The four ranges
agree with each other to within 15 %, which is what says the one `apply` task is not
starving one of them.

**How long one range's applies hold the node's others (D-036).** The take's own hold
**cannot be measured here**: this node takes no snapshot, so there is no take to hold
anything, and the slice that wires the `snapshot` task owes that figure. What is
measured is the hold by an ordinary apply, which is the same mechanism — one `apply`
task per node taking every range's jobs one at a time (Q14) — and which no single-range
scenario could produce at all.

At **100 seeds**, over **5 323 waits that crossed a range**: the median hold is
**2.074936 ms** and the **largest is 572.863467 ms**.

**The median is well under a heartbeat interval and the maximum is far over it, so the
maximum goes to the owner**, which is what SHARD.md §12 says to do with this figure.
What the maximum is: a single apply job of one range that ran for more than half a
second of virtual time — a follower's catch-up batch, applied as one synced job — with
another range's already-committed entry waiting behind it the whole time. The fold
clamps every hold to one job's own span, so this is not a crash or an isolation read as
a hold; it is one job.

Two things the owner should weigh with it. First, this is the hold by an **ordinary
apply**, and D-036's subject is a **take**, which is strictly longer: the figure is a
floor under the thing §12 actually asks about, not a substitute for it. Second,
grouping applies does not shorten a take and does not shorten this either — a batch
that takes 573 ms takes 573 ms whichever way the task batches it — so what the figure
bears on is §11's storage item 6 and not Q14's grouped applies, exactly as §12 says.

**The inbox's drops under its byte bound.** At 100 seeds, and at 20: **none at all**,
of any kind, of any range. The coverage line prints `{}`.

This is a measurement that reached nothing, and saying so carefully is the
measurement. **Zero drops at three nodes of four ranges means the bound was never
approached — it does not mean the bound works.** The two are different claims and only
the first is evidence. §4's pressure is 200 messages a tick at 1 000 ranges against a
capacity the node's byte bound stands in for; this scenario's traffic is a small
fraction of that, and 64 KiB is never filled by it, so the policy that decides *which*
message to drop was never asked a question. Two things follow, and neither is for this
entry to settle:

- **the figure §4's analysis actually wants is how close a node's inbox comes to its
  bound**, its high-water mark in bytes — and that figure **is not visible to a
  scenario at all.** `Inbox`'s occupancy, like `Meters::locals_held` (D-076) and
  `Gaps::snapshot_actions`, lives inside a `Node` that `server::run` moves into a
  spawned task and never hands back. Making it visible means a trace event for the
  node's own meters, which D-073 already records as owed, and this slice does not
  build it;
- a scenario that *does* reach the bound has far more ranges or far more load than
  four ranges on three nodes, which is Stage C to E's shape and not this stage's
  parameter.

What this sweep can honestly say is that the byte bound cost the correct node nothing
over the tier, and that a drop, if one came, would be counted and printed by kind and
by range.

**The trace records a run holds per range per virtual second, against `TRACE_CAP`.**

At **100 seeds**, over **9 037 027 records**: at most **1 728 records per virtual
second per range** by the conservative reading — the whole trace, every client
operation and every `MessageSent` and `MessageDelivered` included, divided by the four
ranges — and at most **264** of the busiest range's own records per virtual second,
about a seventh of it. `TRACE_CAP` is **400 000**.

So four ranges under these arms hold **about 231 virtual seconds** before the cap on
the conservative reading, and about 1 500 on the observed one; a run of this scenario is
a few virtual seconds long.

**Four ranges fit under `TRACE_CAP` with room**, so Stage B's range count does not go
to the owner and is not lowered. The number to carry into Stages C to E is the
conservative one.

### The mutation table: what a single-range world could not catch

The owner's standing demand on this stage is that a check with more than one range to
be wrong about show the mutation a single-range world could not catch. Seven mutations
were planted in the harness **one at a time**, each run at **100 seeds**, each reverted
before the next. Every one of them is a no-op on one group: with a single range,
`range_of` answers the only range there is, the key map has one answer, the client's
per-range leader map has one key and the trial hands over the one range.

| | Mutation | What it does on one group | Caught by |
|---|---|---|---|
| **M1** | `leader_of_range` ignores its `range` argument | nothing: one range, one leader | **the arms floor.** 115/115 (100 %) correct, **82/115 (71.3 %)** mutated, against a 90 % floor. Not one in four, because three nodes hold four ranges and the two leaders often coincide — which is why this needed a measured floor and not an intuition |
| **M2** | `Schedule::range_of` always answers the first range | nothing: `range_picks` is empty and the first range is the only one | **`the_nodes_arms_aim_at_every_range_and_not_at_one`** |
| **M3** | the node's key map answers one range | nothing: every key is in the one range | **the apply-spread floor.** least over busiest **0.87** correct, **0.13** mutated, against a floor of 0.5. `applies.contains_key(&range)` passed it: a range that applied only its leader's no-ops satisfies "every range applied something" |
| **M4** | the client keeps one leader for the cluster, not per range | nothing: one range, one leader | **not caught.** Every check passes. The run gets slower and noisier — more `NotLeader` round trips — and, oddly, `LeaseTrustsTheClock` is caught on **4 of 100** seeds where the correct harness catches 0, because a client that keeps asking the wrong node reads more staleness into the window. Recorded, not covered: what would catch it is a ceiling on the clients' redirects, and a ceiling on a count that faults legitimately raise is a bound the correct system would trip |
| **M5** | the Figure 8 burst writes the first range's key whatever the arm drew | nothing: one range | **not caught**, and on reflection it should not be. `CountOlderTermForCommit` stays at 14/20: the driver needs *a* backlog of more than `max_batch` uncommitted entries behind an isolated follower, and a backlog is a backlog whichever range carries it. What the aimed key buys is that the backlog is on the range whose leader the arm steered, which is tidier but is not what the window depends on |
| **M6** | the lease trial hands over one range, not every range the node holds | nothing: one range | **not caught.** `LeaseTrustsTheClock` is 0/100 either way at this tier, so there is no rate to move, and the lease coverage stays above its floors. The place this would show is the thousand-seed tier, where the variant's catch lives; it is recorded so the premerge can be read against it |
| **M7** | Q41's round stops honouring `Variant::SendBeforePersist` | n/a — this is the hole this branch closed | **`a_server_that_sends_before_it_persists_is_caught_on_the_node`**, 0/20, which is exactly what the branch found before it was fixed |

**Four of seven caught, three recorded and not covered.** Two of the four — M1 and M3 —
were *not* caught when the campaign began: both figures were printed and neither had a
floor, and the campaign is what turned two prints into two bounds. That is the campaign
earning its cost, and it is the honest reading of the table: a mutation campaign whose
every row is caught has usually been written after the checks rather than against them.

### The pinned seeds, re-audited

SHARD.md's Stage B names the node as one of three commits of the stage that may move
every schedule, and asks each of them to re-audit every pinned seed in
`sim/tests/raft.rs` (SHARD.md:2361-2370). Under the shape this entry chose, **the
one-group schedules do not move**, and the audit begins with the evidence for that
rather than with an assurance: seed 42's moirae JSONL is 12 898 025 bytes and hashes
to `445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on
`origin/main` and on this branch, from `the_seed_42_trace_is_written_for_the_studio`
run in a worktree of each. A schedule that moved would move that trace: it carries
every message, every delivery, every fault and every step of a run with two lease
trials, the drawn faults, the install crash, the adoption storm, the refusal storm and
the re-take, over 12.9 MB.

So no pin's run changed, and the audit is of what each pin **says**, under
CLAUDE.md:58-67: a mechanism, or the situation's absence with the reason. Every one
was read and every one was run at the gate's twenty seeds on this tree. Twenty-three
tests pin a seed or a fixed list of seeds:

| Pinned | What it pins | Verdict |
|---|---|---|
| 42, `same_seed_gives_byte_identical_trace` | determinism: two runs' JSONL byte for byte | **mechanism** |
| 42, `the_seed_42_trace_is_written_for_the_studio` | writes `out/raft-42.jsonl`, then `check().unwrap()` | **bare green**, and the only one: see below |
| 164 | a snapshot-fed timer gap, absent — `snapshot_fed_timer_gaps()` empty, with the reason and the instruction to pin the mechanism the day it is not | absence, with reason |
| 385 | an install's restatement inside a follower's timer window, absent (`timer_gaps_rescued_by_restatement()`) | absence, with reason |
| 2605 | the correct half: an adoption window reached at all (asserted non-vacuous) and no timer gap rescued by one; the variant half: `ResetTimerOnAnyRpc` caught with the **exact violation string**, its majority up, five gaps, none resting on an adoption | absence + **mechanism** |
| 7381 | a recovered applied index replayed under a lost floor, absent; and no install below a floor already reached | absence, with reason |
| 6325 | under `Correct` and `AdoptionAsBuilt`: an adoption window reached (non-vacuity) and no crash inside one | absence, with reason |
| 5909 | `refusal_reset_reseed(3)` present — the D-042 path the pin is about — and no stream wedge | **mechanism** + absence |
| 5909, the pair | under each half and both: passes, takes a snapshot at all (non-vacuity), no index taken twice, and the exact stale and uncounted sets per variant | absence, with per-variant equalities |
| 132 | under the pair, each half and `Correct`: the per-variant equalities for a twice-taken index, a refusal, the stale set and the uncounted set, and the correct server's `refusal_reset_reseed(2)` | absence + **mechanism** |
| 680 | under the pair, each half and `Correct`: no wedge, and under `IgnoreIncarnation` the mechanism itself — server 3 refused, and the leader **not** resetting its progress, which is the fix the variant turns off | absence + **mechanism** |
| 687 | the fix's half record by record — table 74 dropped twice, the engine quiesced over a recovery that lost writes, the store refused again after the quiesce, no manifest written or `CURRENT` switched between them; the bug's half absent with its reason | **mechanism** + absence |
| 1885, 2023 | the pre-vote straddle the nightly failed on, absent by decision time and present by durability time, with the isolation named | absence, with reason |
| the nightly's eleven | eleven (seed, variant) pairs of the same straddle, each absent, each run still passing, with `KEPT` naming the two whose isolation still comes | absence, with reason |
| the nightlies' twenty-eight | every catch the ten-thousand-seed nightlies removed: each asserted removed for its reason, and the four with a bug of their own asserted **caught over that bug with its exact violation string** | **mechanism** + absence |
| seed 1 of the term-raise schedule | seven isolations straddled by decision time, the first with its exact cause `[(1, "append-entries", 2)]`, and `Moved::Lost` of the named violation; D-050's own shape absent | **mechanism** + absence |
| seed 4 of the term-raise schedule | D-050's shape, to the nanosecond: `received` 1 224 602 510, `from` 1 224 610 000, `decided` 1 225 627 305, `until` 1 524 610 000, with the cause `(1, "request-vote", 2)` | **mechanism** |
| 512, the follower-log bound | `FollowerNeverCompacts` caught **by the bound's own violation** ("follower log:"), its log past the bound, and the correct server's under it | **mechanism** |
| 102 | `RefusalNotDurable`'s mechanism absent — no lost-state refusal, no crash on a refused server, no restart after one — with the reason | absence, with reason |
| 158 | the same, plus no table dropped and no engine quiesced | absence, with reason |
| 119 | the same, plus the two runs are **not** record for record identical | absence + a difference |
| membership seed 7 | determinism of the membership scenario's JSONL | **mechanism** |

Twenty-two of the twenty-three assert a mechanism, an absence with its reason, or
both. **One is a bare green**: `the_seed_42_trace_is_written_for_the_studio`, whose
body is `write_trace(...)` and `report.check().unwrap()`. It is not a pin a sweep
found — seed 42 is the studio's example trace, and `echo`, `wal` and `engine` each
have a test of the same name — so CLAUDE.md's rule about a seed a sweep found does not
reach it; it is still a `check().unwrap()` on a fixed seed in a file where every other
fixed seed says what it is for. **Issue #97**, with both readings and the question for
the owner.

Two smaller findings, both **issue #98** rather than fixed here (one change per pull
request):

- `seed_102_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss` is
  named for a mechanism its body no longer asserts: D-078 recorded that the mechanism
  left the seed and moved the body to four absences, correctly, and the name did not
  move with it.
- `seed_687_which_the_premerge_found_stays_green` looks the quiesce up on
  `NodeId::new(1)` while its `expect` message says "server 3's engine". One of the two
  is wrong, and which it is decides whether the pin still asserts what it says. That
  one is worth checking before it is renamed.

### The node's snapshot wiring: designed here, built next, and owned

`ananke_shard::snapshot` (D-075) is the discipline — keys, caps, frames, switches —
and says of itself that "the engine call an `Install` describes
(`Engine::install_spans`, D-068) and the streams' bytes are the node's wiring, which
Stage B's scenarios slice puts under the sweeps". This is that slice. Neither `main`
nor any open branch wires it, and PR #86 records the same thing from the other side:
what a re-seeded replica waits for "is the node's snapshot *wiring*, which this slice
does not build". So the wiring has had no owner, and it is taken here: **designed in full below, and
built on the branch that follows this one**, by whoever is holding this entry. It is
not built in this pull request for one reason, and it is a reason about review and not
about scope: this branch already carries a harness, a second cluster, a production fix
to Q41's round and a re-audit, and the wiring is a task, a queue, four `Local` variants,
an apply-task rewrite and a live install per range — the size of D-075 again. One
reviewer cannot hold both, and a wiring that is waved through is worse than a wiring
that waits a day. What must not happen is the wiring belonging to nobody, and it does
not: it belongs to the branch after this one, and its design is here so that the owner
can rule on it before a line of it is written.

**What the node has today, and what is wrong with it.** `Host::snapshot` is

```rust
fn snapshot(&self, _range: RangeId, _action: SnapshotAction) {
    lock(&self.gaps).snapshot_actions += 1;
}
```

and two things follow that are worth stating before any code:

- **The counter is write-only.** `ServerHost::gaps()` is reachable only through the
  `Node`, and `server::run` moves the `Node` into a spawned task and hands nothing
  back, so no scenario can read it — the same fact D-076 recorded of
  `Meters::locals_held`. A node that reached this path would count it and no sweep
  would see it. That is why the node sweep asserts the *condition* behind every
  action — a log longer than `snapshot_threshold` — and not the absence of a
  `RaftSnapshot` record, which would be an absence asserted against a silence.
- **`InstallSnapshot` is admitted and then swallowed.** The `net` task admits every
  decoded message to the inbox, `Inbox::carries_data` deliberately admits an
  `InstallSnapshot` by dropping heartbeats to make room, and `Raft::on_message`'s arm
  for it is empty, because "the server routes these to it before the core sees them"
  — which the one-group server's `net` loop does and the node's does not. So a chunk
  that reached a node today would cost a heartbeat its place in the inbox and then
  vanish. **Nothing reaches the node today, so nothing is lost**: it is a latent hole,
  not a live bug, and the wiring closes it. **Issue #96**.

**The shape.** A fourth task, `snapshot`, beside `net`, `answers` and `apply`, on a
fourth handle of the one socket — not a second bind, which would give it a different
address and make it a different peer. It owns a `Snapshots` (D-075) as its planner and
keeps the I/O the planner deliberately has none of:

1. **`Host::snapshot` becomes a push**, onto a `Queue<SnapJob>` the host owns beside
   `jobs` and `answers`, closed after `Node::raft` returns as those are. The trait
   method is already `&self` and synchronous, which is exactly what a queue push
   needs and what an await would not allow.
2. **A take is the `apply` task's**, between two applies, as RAFT.md §1 has it and
   D-036 requires for exact metadata. `ApplyWork` is `#[non_exhaustive]` and already
   has `Take`, which `Node::act` already routes there and `ServerApplier::run`
   currently drops; the arm is filled, and `Record` (D-078's follower compaction) is
   added beside it. `ServerApplier` has to carry per range what the one-group apply
   task carries for its one group: the applied index, the applied **term**, and the
   configuration at that index, followed entry by entry. A take writes the record
   first, synced, then checkpoints the range's spans into
   `Snapshots::version(range, index, take)`; a record writes the record and no
   checkpoint, and traces no `RaftSnapshot`, which the one-group task is explicit
   about because tracing it moved the sweep's count of installs by a factor of four.
3. **A stream out** is a `raft::snapshot::Sender` per (range, follower), opened on the
   version directory `Snapshots::stream` pinned, with `Snapshots::route` cutting each
   chunk into a frame of its own (Q41) for `sock.send`. There is no per-node cap on
   streams sent, so a leader feeds every designated follower of a range at once
   (D-043).
4. **A stream in** is an assembly per (range, sender), staging under
   `Snapshots::staging(range, from)`, with `Snapshots::on_chunk` deciding between
   staging the bytes, restarting the sender, making it wait for a slot under the
   node's receive cap, and completing.
5. **A completed stream is D-066's live install**, and this is where the node parts
   from the one-group server for good. The one-group server ends its run-loop
   incarnation and reopens the engine, "which in a shared node would restart every
   range on it"; the node calls `Engine::install_spans(spans, source, repair)` — the
   staged tables and the range's repair in **one** manifest switch, the engine left
   open, the node's other ranges untouched. The staged directory is a checkpoint of
   the sender's spans, so `Engine::open_span_source` reads it directly and is also the
   check that it is whole: a directory left short by a crash fails there and the
   stream restarts, which is the property `Fault::CrashInstalling` aims at.
6. **The repair** is the range's own term, vote, applied index, snapshot record, log
   tail, configuration, quarantine and incarnation, with tombstones for the log keys
   the tail does not replace, written under **the receiver's** key prefix and carried
   in the switch. `NodeVariant::InstallWithoutRepair` is the node's translation of
   `SnapshotWithoutCurrentLast`, which D-075 already named: it makes the switch
   without the repair, and what catches it is state machine safety after the crash
   the arm aims at.
7. **The answers to the core** go back the way the `apply` task's do: a push onto the
   node's `Queue<Local>`, which `Node::raft` already races, turning into
   `Input::SnapshotTaken`, `SnapshotInstalled`, `SnapshotFailed` and `SnapshotAcked`
   through `Host::local_input`. `Local` grows one variant per answer, each carrying
   its range. Nothing about the round changes.
8. **The `net` task diverts `InstallSnapshot` and `InstallSnapshotResponse`** to the
   snapshot queue before `Inbox::admit`, so a chunk is charged to the snapshot task's
   receive cap and not to the inbox's byte bound. That moves where those bytes are
   counted, which the inbox's drops are measured against, and the entry records the
   figures on both sides of it.

**What it opens that the tree has not answered.** From the first live install on a
node's engine, a read served from a pinned version can straddle the switch on its own
span, since a read below the install's number sees the replaced span as empty (D-069,
D-054). SHARD.md's Stage B carries this as **issue #72** and asks the install item to
say what such a read sees and to pair it. It is not a question this wiring can settle
on its own, and it is put to the owner with the wiring rather than decided here.

### What is not built, and what goes to the owner

- **The install path on the node was not built by anyone**, and this slice takes it:
  `ananke_shard::snapshot` was not wired to `ServerHost` on `main` or on any open
  branch, and D-075 said in as many words that "the streams' bytes are the node's
  wiring, which Stage B's scenarios slice puts under the sweeps" — this slice. It is
  designed in full above and built on the branch that follows this one, for the reason
  given there. Until it exists a variant on that path cannot be re-asserted on the node
  at all, and the node's sweep says so on **every seed** rather than passing. `checked`
  in `sim/tests/node.rs` fails a seed two ways: on a snapshot action traced, and — the
  one that matters — on any replica reaching an index at or past this cluster's
  `snapshot_threshold`, which is the condition behind every action a core can ask for.
  The second is there because the first is not evidence on its own: the node's
  `Host::snapshot` bumps `Gaps::snapshot_actions` and traces nothing, so an action the
  node dropped on the floor would leave the first clause green against a silence. A
  variant whose situation a run cannot reach is the one failure mode a sweep cannot
  report on its own, and an absence asserted against a silence is how it hides.
- **`AdoptionAsBuilt` is re-asserted on the two rules that remain, and §10 is not
  amended.** SHARD.md's Stage B asks this stage to return the variant to the owner
  with a proposal: "re-asserted on the two rules that remain, under a crash arm aimed
  at the live install's switch and at the refused directory's open, at its Phase 2
  tier; or §10 amended for it". The owner has already ruled this exact shape. Answering
  the intervals slice on 2026-09-20, on a tree where one of the variant's three rules
  likewise had no path: *"AdoptionAsBuilt: re-assert it on the two rules that remain
  rather than amend §10."* That ruling is applied here rather than asked for again, and
  it is recorded as a precedent applied so the owner can overturn it.

  **The rule with no path, and why.** The variant breaks three rules (RAFT.md:696).
  The second is *a damaged staging `CURRENT` refused*, and it has no path on the node
  for a reason that is the node's whole design and not an omission: a replica's
  install on the node is Stage A's live install into the running engine, and the
  whole-store staged install adopted at the next start is **not kept**
  (SHARD.md §12, question 2's proposal). A node that adopts no staged store at its
  start has no staging `CURRENT` to find damaged, and neither has the fault that
  reaches it — `Fault::CrashAdopting` crashes "the moment the adoption's first change
  to the store directory is durable", and there is no such adoption. The rule is not
  weakened, dropped or amended out of §10: it has no subject on this node.

  **The two that remain, and where each is re-asserted.** The first, *copy and switch
  before delete*, becomes the live install's single manifest switch, which the variant
  breaks by removing the span's keys in a switch of their own before adding the
  tables; its path is `Engine::install_spans`, which is the snapshot wiring designed
  below and built on the branch after this one. The third, *a marked store never opens
  fresh*, becomes Q15's refused directory, which stays marked lost and which the
  variant neither checks nor writes; its path is PR #86's. So the variant is
  re-asserted on the node in two places, neither of which exists in this tree, and in
  neither case is the standard lowered: it keeps its Phase 2 tier. Until both land it
  keeps its Phase 2 assertion on `Cluster::OneGroup`, unmoved.
- **`RefusalNotDurable` is the sixth, and it stacks on PR #86.** Its rule on the node is
  the whole node's mark in the refused directory (Q15), which #86 builds. It is not
  re-asserted here and is re-asserted in the commit that follows #86 into this branch.
- **The pair's pin on seed 680 is not retired, because nothing moved it.** SHARD.md's
  Stage B says the node's schedule move retires D-045's pin and that the first thousand
  seeds are searched again. Under this shape the one-group schedules do not move — that
  is the point of (2) — so seed 680 keeps asserting exactly what it asserts today, and
  the re-audit below states it. The pair's re-assertion on the node is blocked behind
  the install path with the rest.
- **The take's hold on the node's other ranges' applies (D-036) cannot be measured
  here**, for the same reason: this node takes no snapshot, so there is no take to
  hold anything. What is measured instead, above, is the hold by an ordinary apply —
  the same mechanism, one `apply` task per node taking every range's jobs one at a
  time (Q14), with the take's own hold owed by the slice that wires it.
- **That hold's maximum goes to the owner**, which is what §12 says to do with this
  figure when it exceeds a heartbeat interval. At 100 seeds the median is 2.07 ms and
  the **maximum is 572.9 ms**, against 20 ms. It is one apply job of one range —
  clamped to a single job's span by the fold, so not a crash or an isolation misread —
  with another range's committed entry waiting behind it throughout. It bears on §11's
  storage item 6 rather than on Q14's grouped applies, since grouping does not shorten
  a job, and it is a **floor** under D-036's real subject: a take is strictly longer
  than an ordinary apply.
- **Follower compaction (D-078) does not run on the node either.** The core's
  compaction asks the `apply` task for `SnapshotAction::Record`, which this host
  counts and drops, so the follower-log bound Stage B's exit asks for cannot be
  asserted on the node. It stays asserted on `Cluster::OneGroup`, where D-078 measured
  it.

---

## PROPOSED D-083 — The node runs its `snapshot` task: a take of the range's own spans, a stream per (range, follower), and D-066's live install with the range held across its switch

**Context.** SHARD.md §12's Stage B lists "installs on the node" (§11, storage 5) and no
branch has built it. `ananke_shard::snapshot` is the discipline — keys, caps, frames,
switches — merged as PR #83 under PROPOSED D-075, and `ananke_shard::server::run` never
ran it; PR #86 states the same thing from the other side, that what a re-seeded replica
waits for "is the node's snapshot *wiring*, which this slice does not build". D-082
designed that wiring in full and, for a reason about review rather than scope, left it
to the branch after it. This is that branch. Four Phase 2 variants, the directed re-seed
shape and the quorum sweep on the node are all behind it.

D-082's design is followed where it holds. Four places it is departed from are set out
below, each with what was found and what was done instead; one of them is a hole in the
design rather than a preference, and one is an issue filed against it after it was
written (#103).

### What is built

`crates/ananke-shard/src/install.rs`, the fourth task, beside `net`, `answers` and
`apply`, on a fourth handle of the one socket — not a second bind, which would give it
a different address and make it a different peer. It owns the node's `Snapshots` (D-075)
as its planner and keeps the I/O the planner deliberately has none of.

1. **`Host::snapshot` is a push**, onto a `Queue<SnapJob>` the host owns beside `jobs`
   and `answers`, closed after `Node::raft` returns as those are. `server.rs:566-571`,
   where a core asking for a snapshot action bumped `Gaps::snapshot_actions` and nothing
   else happened, is that push now. The counter is kept: a scenario that asserts the
   path's absence keeps its figure, and it is no longer the *only* thing that happens.
2. **A take is the `apply` task's**, between two applies, as RAFT.md §1 has it and D-036
   requires for exact metadata. `ApplyWork::Take` was already routed there and dropped;
   the arm is filled and `Record` (D-078's follower compaction) added beside it.
   `ServerApplier` now carries per range what the one-group `apply` task carries for its
   one group — the applied index, the applied **term**, and the configuration at that
   index, followed entry by entry — because a record whose term or configuration is off
   by one apply is a record D-029's revert floor reads wrong.
3. **A stream out** is a `raft::snapshot::Sender` per (range, follower), opened on the
   version directory that range's *own* snapshot record names and pinned to it for the
   stream's life (D-043), with `Snapshots::route` cutting each chunk into a frame of its
   own for `sock.send` (Q41). There is no per-node cap on streams sent, so a leader
   feeds every designated follower of a range at once.
4. **A stream in** is an assembly per (range, sender) staging under
   `Snapshots::staging(range, from)`, with `Snapshots::on_chunk` deciding between
   staging, restarting, waiting under the node's receive cap, and completing. The
   file-level bookkeeping an acknowledgement needs — which file, which offset — is the
   task's, since no chunk carries it and the planner counts bytes rather than files.
5. **A completed stream is D-066's live install**: `Engine::open_span_source` on the
   staged directory, then `Engine::install_spans(spans, source, repair)` — the range's
   two key intervals and the receiver's repair in **one** manifest switch, the engine
   left open, the node's other ranges untouched. The staged directory is a checkpoint,
   so the engine's own checks are what say it is whole: one left short by a crash fails
   at `open_span_source` and the stream restarts, which is the property
   `Fault::CrashInstalling` aims at.
6. **The answers go back through the node's `Queue<Local>`**, which `Node::raft` already
   races, turning into `Input::SnapshotTaken`, `SnapshotInstalled`, `SnapshotFailed` and
   `SnapshotAcked`. `Local` grows one variant carrying a `SnapAnswer`, and every one of
   them names its range.
7. **The `net` task diverts `InstallSnapshot` and `InstallSnapshotResponse`** to the
   snapshot queue *before* `Inbox::admit`. **This closes issue #96**, and the entry says
   so because the hole stops being latent the moment this lands: the inbox deliberately
   admits a chunk by dropping heartbeats to make room (`Inbox::carries_data`), and
   `Raft::on_message`'s arm for it is empty, because "the server routes these to it
   before the core sees them" — which the one-group server's `net` loop does and the
   node's did not. A chunk that reached the node would have cost a heartbeat its place
   under the byte bound and then vanished. Nothing reached the node before this wiring,
   so nothing was lost; this wiring is what makes chunks arrive, and the divert is what
   catches them. It also moves where those bytes are counted — a chunk is charged to the
   snapshot task's receive cap (D-075) and not to the inbox's bound, which is what the
   inbox's drops are measured against. `NodeVariant::ChunksToTheInbox` keeps the hole
   beside the fix, and it is caught on every seed of the directed scenario.

### Where this departs from D-082, and why

**1. The stream carries no log key, and the repair therefore tombstones none.**

D-082's item 6 has the repair carry "tombstones for the log keys the tail does not
replace", which is what `Assembler::finish` does on the one-group path: it walks the
staged tables, collects every log index they hold, and tombstones the ones the receiver's
kept tail does not overwrite. Built here, the take checkpoints the range's Raft state
**except its log purpose**, and its user keys. The install's spans are still the range's
two whole intervals, so the switch removes the receiver's own log along with everything
else in them, and the repair's kept tail is all that goes back.

The end state is the same and the reasons to prefer it are three. It is fewer bytes on
the wire — a leader's whole log, streamed and then deleted. It removes a standing
obligation on the receiver to enumerate what the sender sent, which is a second place
for the two ends to disagree. And the enumeration is not reachable from this crate
anyway without widening `ananke-raft`'s surface: `SpanSource` exposes no key walk, and
every SST and key helper the one-group walk uses is `pub(crate)` there, which is Q40's
line and not an accident. The cost is that the two install paths no longer build their
repair from the same set of log keys; what keeps them from drifting is that they build
it from the same *function* — see below.

**2. One builder for the repair's writes, in `ananke-raft`.**

D-082 did not say where the repair is built. It could not be built in `ananke-shard` at
all: every key constructor (`hard_key`, `applied_key`, `log_key`, `config_key`,
`snapshot_key`, `quarantine_key`, `incarnation_key`) and every encoder
(`encode_hard`, `encode_applied`, `encode_snapshot_record`, `encode_config`,
`encode_incarnation`, `encode_entry`) is `pub(crate)` in `ananke-raft`, which owns what a
Raft key is. Rather than widen that, `ananke_raft::snapshot::repair_writes` is added:
`pub`, returning a `WriteBatch` in key order, with `replaced_log` as a parameter so a
caller whose source carries no log keys passes an empty set and gets no tombstones —
which is not the same thing as forgetting them. `Assembler::finish` was rewritten to use
it and is behaviour-preserving; the one-group tests pass unchanged. There is now one
statement of what a repair *is*, used by two switches of very different shapes.

**3. `ananke_raft::snapshot::staged_record`: the configuration, out of the streamed bytes.**

Neither D-082 nor D-075 says where the receiver gets the configuration in force at the
snapshot's last index. It needs it twice — the repair writes it into the snapshot record,
and `Raft::restore_compacted` takes it as the floor a revert cannot go below (D-029) —
and no message carries it. It cannot use the configuration *it* believes is in force: a
replica being fed a snapshot is behind by definition, and a membership change inside the
compacted prefix is exactly what it has not seen. The one-group path reads it while it
verifies the staged store; a live install verifies nothing of its own, because the engine
does that at `open_span_source`. So a reader is added beside the repair builder, in the
crate that owns the layout: one key, read from the staged directory's own tables, nothing
written.

**4. The design's hole: a live install needs the range *held*, and its replica *replaced*.**

D-082's item 7 ends "Nothing about the round changes". That is not true of the receive
side, and the gap is not small.

A server ends its whole run-loop incarnation across an install and comes back on the
store the switch built: that is how its core learns it installed, and how nothing steps
that core across the switch. A node cannot do either, because reopening the engine "would
restart every range on it" (SHARD.md §11, storage 5) — which is the whole reason the
install is live. So two things had to be built that D-082's four `Local` variants do not
describe:

- **The hold.** Between the install's decision and its manifest switch, the range's core
  must step nothing. The replica being replaced is behind its leader by definition, so a
  step of it in that window appends entries at indices the switch is about to compact
  past, and the store comes back with a log below its own snapshot record. The node
  already has exactly this primitive — a core whose persist is outstanding queues its
  messages and its node-local inputs — so the install reuses it: `Slot::installing`
  beside `Slot::persisting`, and the same queue. The hold is **one range's**; every other
  range on the node goes on stepping, which is the cost a live install exists to avoid.
  A hold asked for while that range's persist is outstanding waits behind it like any
  other local input, so the install never switches under a write still in flight.
- **The replacement.** When the switch is durable the range's core is rebuilt on the
  state the repair wrote — term, vote, snapshot index and term, the configuration in
  force at it, the kept tail, the quarantine — and takes the place of the one it
  replaced, with the work held meanwhile stepped into it in the order it arrived.

Both are asked for through one new `Host` method, `local_core`, answering a `CoreWork` of
`Step`, `Hold`, `Restore` or `Release`. It is deliberately not an `Input`: neither of
these is a step of a core, and expressing them as steps would put an install's
bookkeeping inside a Raft core that knows nothing about a node's engine. `Step` is the
default, so a host with no install path answers it for every input and nothing about the
round changes for it — which is the sentence D-082 wanted, true of every host but this
one.

**And an ordering the hold depends on.** `Host::local_core` is not a pure question: the
install's repair is built in its `Ready` arm, from the held core, and handed to the
`snapshot` task there. So the node has to know whether an input is *held* before it asks
what the input wants — asked the other way round, a `Ready` that arrived while its own
range's persist was still outstanding would build a repair and let the switch carrying it
proceed, against a write in flight, which is the one thing the hold exists to prevent.
The check therefore comes first, and `Host::local_releases` — pure, and default
`false` — is what lets the two answers that *end* a hold through it rather than queueing
them behind the hold they end.

**A bug this found, and it is the reason the hold and the replacement are not enough on
their own.** The first run of the directed scenario stopped a node with

```
range RangeId(5): an apply through 81 names index 1, which the core does not hold
```

`Cores` keeps the highest index handed to the `apply` task per range, beside the cores,
because the entries an `Apply` names are read at the step that named them. A switch that
replaced the replica and left that watermark where the *replaced* core stood had the next
`Apply` name an index below the installed snapshot, the node failed that range, and
`Node::raft` returned — so the node stopped, and the installs that had not happened yet
never did. Five of eight installs completed, and the three that did not looked like a
timing problem. The watermark moves with the replica now. It is recorded here because
nothing about the hold or the replacement suggests it, and only a scenario that installs
on several ranges of one node reaches it: with one range, the node stops after its only
install and the run has nothing left to get wrong.

### Issue #103: which core a stream's answer is stepped into

Filed against this design while it was being built. `Input::SnapshotAcked { to }` names
the follower and not the range — complete information for a server with one core,
incomplete for a node with four. A wiring that took the follower's identity as the
address would set `stream_acked` on every range's progress for that follower, and D-049's
rule (core.rs:1607-1613) — a *refused* follower counts for check quorum only while its
re-seed stream progresses — would hold on one range and be void on the other three, with
no event, no counter and every check green.

**Decided: the address is the range, and it travels on the `Local`, not in the `Input`.**
Every node-local input already carries its range and is routed by `Host::local_range` to
exactly one core (D-073, D-076); the snapshot answers carry theirs the same way. The
range was *not* added to `Input::SnapshotAcked`, and the reason is that it would be the
weaker fix, not the cheaper one: all four snapshot inputs have this shape, the field
would be read by nothing — a core knows its own range — and one of the four carrying it
would make the other three look addressed when they are not. What the wiring gains
instead is a variant and a check: `NodeVariant::SnapshotAckToEveryCore` fans the answer
to every core on the node, and `acked_for` is the decision it mutates, asserted
deterministically. The check also asserts the thing the issue is really about — that with
one range on the node the fan-out and the correct route are the same route, so no
single-range scenario could catch it even in principle.

### The directed evidence: a stream flows and an install completes

`sim/install.rs` and `sim/tests/install.rs`. Five voters and four ranges; three nodes
start, **two start late**, after the running three have written past `snapshot_threshold`
and their leaders have compacted. The two are then behind every range's compacted prefix,
so each range's leader streams to both at once: four ranges times two followers.

Two late nodes rather than one is deliberate. With a single follower behind the prefix, a
leader that fed its followers one at a time would be indistinguishable from one that fed
them all at once, and `CapStreamsSent` — D-043's rule, one of the seven D-075 says a
single-stream world cannot catch — would have no situation here at all.

The writer round-robins over the *ranges* rather than drawing a key at random. With a
random draw, some seeds left a range short of its threshold, that range's late replica
caught up by AppendEntries and needed no install, and the check reported correct
behaviour as a missing install. Every range crosses its threshold by construction now.

On seed 1, and on every seed the binary runs:

| What | Figure |
| --- | --- |
| streams opened, as (leader, range, follower) | 8 of 8 owed |
| installs completed on the node, as (server, range) | 8 of 8 owed |
| replicas created by their install (`RangeCreated { cause: snapshot }`) | 8 of 8 owed |
| the most streams one leader had running at once | 6 |

`Report::reached` fails any seed that does not reach the situation — no take at all, or
fewer than four ranges taking — before any of the rest is asserted, because a scenario
built to exercise a path is worth nothing if a seed quietly fails to reach it. That is
the same rule `sim/tests/node.rs` keeps from the other side, and it is why the two do not
contradict each other: that binary holds `snapshot_threshold` at `1 << 30` and asserts
the path is *not* reached; this one is built to reach it and fails if it does not.

**`RangeCreated { cause: snapshot }` on this node.** §8 defines the cause as an install
onto a node that held no initialised replica of the range. It is traced when the replica
the install replaces held nothing — no applied index, no log, no snapshot — so the
install is what gave that range state. A node restarted on a store it kept does not reach
it; a replica that bootstrapped empty and was filled by a stream does, which is this
scenario, and Q15's re-seed into a fresh engine is the case the cause exists for.

### The mutation table: what a single-range world could not catch

The owner's standing demand on this stage. Seven mutations, each built beside the correct
code and each seen to fail a check the correct code passes. **Four of the seven are the
correct wiring exactly on a node of one range or one follower**, and are marked.

| Variant | The mutation | Caught by | Needs more than one range or follower |
| --- | --- | --- | --- |
| `TakeCheckpointsTheWholeNode` | the take checkpoints the whole engine directory, as the one-group take does, instead of the range's own key intervals | `a_stream_flows_…`, 3/3 seeds: 8 of 8 installs refused at the switch | **yes** — with one range the engine *is* that range's spans and the two takes are the same bytes |
| `InstallHoldsEveryRange` | the hold is taken on every range the node hosts, which is the node reaching for the incarnation a server ends | `a_live_install_holds_its_range_alone_…`: another range's work waits on this one's switch, and `ranges_held_for_install` is 2 rather than 1 | **yes** — with one range, holding "every" range is holding this range |
| `InstallSweepsEveryStaging` | a completed install clears every staging directory, destroying a neighbour's half-assembled stream without telling its sender | `a_completed_install_clears_its_own_assemblys_directory_alone` | **yes** — with one range and one sender there is only ever one directory to clear |
| `SnapshotAckToEveryCore` | the follower's identity taken as the address, so one range's chunks answer every range's core (issue #103) | `a_streams_acknowledgement_answers_its_own_ranges_core_alone` | **yes** — with one core the fan-out is the correct route |
| `StepWhileInstalling` | the range is stepped inside its own install's window, so the replica being replaced appends at indices the switch is about to compact past | `a_live_install_holds_its_range_alone_…` | no |
| `InstallKeepsTheOldCore` | the switch is made and the replica is not replaced: the store holds the snapshot and the core goes on from the log it had | `a_live_install_holds_its_range_alone_…`: the node runs at the old core's term | no |
| `ChunksToTheInbox` | `InstallSnapshot` admitted to the byte-bounded inbox instead of diverted, which is issue #96 as it stood | `a_stream_flows_…`, 3/3 seeds: 8 of 8 installs never complete | no |

`CapStreamsSent` is D-075's variant rather than this slice's, and it is listed because
this scenario is the first thing in the tree to reach its situation: it is caught on 3/3
seeds, and it needs more than one follower behind the prefix. Two more of D-075's —
`VersionDirWithoutRange` and `SweepAcrossRanges` — are deliberately *not* asserted here:
their situations are two ranges taking at one index, and one range's sweep running over
another's versions, neither of which this scenario produces. They keep D-075's
deterministic checks, which build those situations on purpose. Listing them would have
been a sweep that passes because nothing was injected, which is the one failure mode a
sweep cannot report on its own. `InstallWithoutRepair` is left out for a nearer reason:
the install still *completes* under it — the switch is made, the event is traced — and
what it breaks is state machine safety after a crash, which this scenario has no crash
arm to reach.

### What is unblocked, and what is not taken

Now reachable on the node, and not before: a snapshot stream, an install that completes,
and every counter and event a scenario needs to assert their situations — `RaftSnapshot`
with `taken` true for a take and false for an install, `RaftSnapshotStreams` per (range,
follower), `RaftSnapshotResumed`, and `RangeCreated { cause: snapshot }`. The quorum
sweep's runs recorded 0 `RaftSnapshotStreams` and 0 installs on the node; both are
non-zero now.

**`RaftReseeded` was emitted by the node for no replica at all**, which is a gap this
slice found beside its own and closes: the one-group server states it at every
restatement of a quarantined replica (node.rs:975-980), and the node's restatement did
not, so the event would have been *missing* for the slice that refuses a node's store
rather than merely zero. It is emitted now in both places the node states a replica —
its start, and the restoration a live install builds, which keeps the quarantine across
the install (D-035). It stays at zero until a store is refused, because nothing on this
node is quarantined yet and Q15's refusal is PR #86's; what has changed is that the
zero is now an observation rather than a silence.

Not taken here, and each owed to the slice that owns it:

- **The four Phase 2 variants on the node** — `SnapshotWithoutCurrentLast`,
  `IgnoreIncarnation`, `SharedSnapshotDir` and the pair
  `{IgnoreIncarnation, SharedSnapshotDir}` — are the sweeps slice's to re-assert, and
  are not re-asserted here.
- **The directed re-seed shape** (D-067's `ReseedMarkNotSynced`, and exit points (a), (b)
  and (d) PR #86 records as owed) is buildable now and is a later slice's.
- **This scenario under the four tiers**, with the weight D-064's shard table wants. It
  runs a fixed eight seeds at every tier today, and deliberately does not read
  `ANANKE_SEEDS`: its situation is reached by construction on every seed rather than
  searched for, so at the nightly's tier it would be ten thousand runs of five nodes for
  twenty simulated seconds — about a whole shard's CPU — bought against a catch that does
  not vary. Tiering it is owed, and the figure has to be measured on a quiet machine:
  taken on this laptop, with several Stage B slices building on it, it would be exactly
  the incomparable kind D-070 exists to stop.
- **Issue #72** is now live rather than latent: from the first live install on a node's
  engine, a read served from a pinned version can straddle the switch on its own span,
  since a read below the install's number sees the replaced span as empty (D-069, D-054).
  D-082 put it to the owner with the wiring; this entry does not settle it either, and
  records that the wiring is what makes it reachable.
- **`AdoptionAsBuilt`'s first rule** — copy and switch before delete — has its path now,
  `Engine::install_spans`, and D-082 already carries the owner's ruling on how the
  variant is re-asserted. Doing it is the slice that owns the crash arm.

### Consequences

`ananke_shard::snapshot` is no longer a module the node does not run. `ananke-raft` gains
two public functions and keeps its layout private. The one-group server is unchanged in
behaviour: `Assembler::finish` builds its repair from the shared builder and its tests
pass as they stood. `sim/` gains one scenario and one test binary, and the three
scenarios that ran before this are unchanged but for `ServerConfig::snapshot_cap`, which
every one of them sets at its range count as D-075 recommends for a scenario that is not
about the cap.

---

## PROPOSED D-086 — The node reaches the stream path: four bugs the reach found, and what each of Phase 2's four stream variants measures there

**The number.** The footer reads D-084 on this branch's base. Two branches open at the
same time are taking the next free number against their own copy of this file, so a
number taken from the footer here would collide with one of them at the merge. This
entry takes **D-086**, past both, and the footer moves to D-087. Every code site carries
`// PROPOSED(D-086)`. The integrator may renumber it at the merge; nothing in the tree
depends on the number beyond those markers, this heading and the footer.

**Context.** SHARD.md §12's Stage B re-asserts Phase 2's sixteen variants on the node
"to the standard their Phase 2 tests assert and no stronger, at the tier each uses
today" (Q39; §10). PROPOSED D-082 re-asserted seven of them on `sim/raft.rs`'s arms and
recorded four as **blocked**, by name and for one reason: the node had no `snapshot`
task, so "until it exists a variant on that path cannot be re-asserted on the node at
all". PROPOSED D-083 built the wiring and listed the same four as owed:
`SnapshotWithoutCurrentLast`, `IgnoreIncarnation`, `SharedSnapshotDir` and the pair
`{IgnoreIncarnation, SharedSnapshotDir}` — "the sweeps slice's to re-assert, and are not
re-asserted here". This is that slice.

### What reaching the path cost, and why that is the finding

The node's sweep held `snapshot_threshold` at `1 << 30` — far above what its clients
write — and `Schedule::draw_on_the_node` took `Fault::CrashInstalling` and
`Fault::RetakeUnderStream` out of every schedule. Both were absences D-082 asserted on
every seed, with their reason, rather than left to be found. This entry turns the
threshold down to the one-group sweep's **12**, which is also `sim/install.rs`'s, and
puts the two stream arms back.

**The correct node then failed on 17 of the gate's 20 seeds**, and the five faults
behind it are the substance of this slice. Every one of them is a node bug that no
scenario in the tree could reach, every one is fixed here, and no bound was widened for
any of them (D-030, D-039; CLAUDE.md).

1. **The applied watermark is zero at every start** (`Cores::insert`, round.rs).
   `Cores` keeps the highest index handed to the `apply` task per range, because the
   entries an `Apply` names are read at the step that named them. D-083 found that a
   live install must move it and fixed it at `Cores::installed`; **the start path was
   left at zero**. A node whose log is never compacted restarts on a log that still
   begins at index 1, and zero is correct for it — so nothing saw this until a node
   restarted on a compacted log. The first one did: *"an apply through 41 names index 1,
   which the core does not hold"*, and the node stopped. The watermark is now the core's
   at every start.

2. **A live install leaves the `apply` task's applied state behind** (`ServerHost`,
   `ServerApplier`, server.rs). The switch replaces a range's state machine with no
   entry passing through that task, and the task, still holding what it applied before,
   fails the range's next job on the gap: *"range 2: apply of 13 after 10"*. The map is
   shared with the host now and the switch moves the range's entry — index, term **and**
   configuration, because the take reads all three for its snapshot record and D-083
   records that a record whose term or configuration is off is one D-029's revert floor
   reads wrong. `sim/install.rs` could not reach it: its late nodes have applied
   nothing, and the applier's `if applied == 0` fallback covers exactly that case.

3. **A live install leaves the store's own caches stale**
   (`RaftStore::restate_after_install`, store.rs). The hard state, the log bounds and
   the applied index are in-memory caches of keys the store writes itself, in `persist`
   and `apply`. `Engine::install_spans` writes all of them in one manifest switch that
   passes through neither, and the store then refuses the range's next apply on the old
   replica's applied index: *"applying 13 after 10"*. A server needs none of this — it
   ends its run-loop incarnation across an install and opens the store again — and a
   node cannot, which is the same sentence D-083 wrote for the hold and the replacement
   and is now true of three more fields.

4. **`SnapshotAction::Record` was dropped on the floor** (node.rs). D-078's follower
   compaction asks the `apply` task for a `Record`. `Node::act` routed only `Take`
   there; `Record` fell to `Host::snapshot`, and `install::job_of` answers `None` for
   it. **The cost is not a missing compaction.** The core sets `take_pending` when it
   asks and only an answer clears it, so a replica that asked once **never asked for
   another snapshot for the rest of its life**. When that replica later leads and a
   follower falls behind its log, `replicate` finds `taken` empty and `take_pending`
   set, asks for nothing, and sends an empty AppendEntries at its own last index
   forever while the follower rejects with a hint the leader is not in a branch to
   read. The range commits nothing again. That is the **liveness bound, tripped by the
   correct node**, on seeds 17, 20 and 64 of the first hundred; the evidence is seed
   17's range 2, where server 2 led at term 8, appended to index 378 and committed
   nothing after 13 s while servers 1 and 3 rejected 150 and 158 times with hints 222
   and 58. Nothing before this slice could see it: `sim/install.rs` has no follower that
   compacts and later leads, and every other node scenario holds `snapshot_threshold`
   above what its clients write, so `Record` was never asked for at all.

5. **A stream opened on the store's snapshot record, and that record moves**
   (`Task::open`, install.rs). This one the fourth fix *uncovered*: with `Record`
   answered, takes become frequent, and the `apply` task rewrites the range's snapshot
   record on every one of them while the `Install` the core asked for carries the index
   its `taken` held when it asked. Matched exactly, a take landing between the ask and
   the read moved the record past the ask, `open` answered `retake`, and the core took,
   asked and lost the race again — **a cascade in which the install never completes,
   however long the run is given**, which is how it was told apart from a slow install:
   `sim/install.rs`'s seed 7 failed at `AFTER` of 14 s and failed identically at 20 s.

   The fix took three attempts and the first two are worth recording, because each was
   plausible and each was wrong. Matching the record **at or past** the ask fixed the
   cascade and broke something worse: the stream's identity then moved with every take,
   so each re-open of one logical install carried a new identity, `is_streaming` never
   suppressed the duplicate, and each fresh stream restarted the receiver's assembly
   under it (RAFT.md:203-207) — three leaders of one range opening streams at three
   identities at once, and the install never completing for that reason instead.

   What is correct is the **one-group server's own rule**, which neither attempt
   consulted: open a **complete version directory of the index the core asked for**
   (`start_stream` and `snapshot::find_version`, crates/ananke-raft/src/node.rs:2182 and
   snapshot.rs:165-191). `ananke_shard::snapshot::find_version` is that function keyed
   by range. The core's ask is the only one of the three candidates that is *stable*,
   version directories are numbered so an earlier one survives later takes — which is
   what the take counter is for (D-043) — and the record's motion stops mattering.

   It needed one more thing: **the node's take did not write D-060's checkpoint format
   record**, which `checkpoint_complete` requires, so every version this node wrote read
   as incomplete and no stream opened at all. `format::write_checkpoint_record` is `pub`
   now and the take calls it, as `snapshot::take_numbered` always has. A crash between
   the engine's checkpoint and that record leaves a version that reads incomplete, costs
   a retake and never streams a half-written checkpoint, which is the property the
   record exists for.

With the five fixed, **the correct node passes every seed of the gate's twenty and CI's
hundred**, over **5 912** snapshot actions at 100 seeds where D-082 asserted zero, and
`sim/install.rs`'s three tests pass unchanged.

**A sixth fault was in this slice's own variant translation, and the review found it.**
It is not a node bug — the correct node never reuses a version directory — but it made
this entry's central measurement a measurement of nothing. `ServerApplier::take` did not
empty the version directory before checkpointing into it, where the one-group take has
always done so (`snapshot::take_numbered`, snapshot.rs:671-677). `SharedSnapshotDir`
names its directory `snap-<range>-<index>` with no take counter, so a re-take at one
index finds that directory full; `Engine::checkpoint_spans` refuses a non-empty
directory with `AlreadyExists` (engine.rs:1517-1523); and `take_failed` answers the core
with no `RaftSnapshot { taken: true }` traced. **The variant therefore never rewrote a
directory a live stream had open — D-043's actual bug — it turned the re-take into a
failed take.** Every fold that reads the symptom was structurally zero, and this entry's
first draft reported "the re-take half is not built on the node" on that basis. The take
empties the directory now, which is a no-op for the correct node (a fresh numbered name
is empty) and is what lets the variant express itself. The rates below are the
re-measured ones; the first ones are withdrawn.

### The one-group cluster did not move, and the evidence is not an argument

D-082's shape holds: everything here is on `Cluster::Node`, on the node's own code, or
on folds whose one-group answer is unchanged by construction. Seed 42's moirae JSONL is
**12 898 025 bytes** and hashes to
`445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on this branch —
byte for byte the figure D-082 recorded for `origin/main` and for its own branch. Every
pinned seed in `sim/tests/raft.rs` therefore runs the run it was pinned on, and the
whole binary passes: 46 tests, seeds 164, 385, 680, 687, 2605, 5909, 7381, 102, 118,
132, 158 included.

### Seed 680, and the search SHARD.md asks for

SHARD.md's Stage B says the node's schedule move retires D-045's pin on seed 680, that
seed 680's test asserts the wedge where the moved schedule still reaches it or, with the
reason, the situation's absence, and that the first thousand seeds are searched again
for a seed the pair is caught on.

**Seed 680's pin did not move and is not touched**, for the reason D-082 gave and the
hash above proves: this slice changes nothing about `Cluster::OneGroup`, so
`seed_680_which_pinned_the_combined_variant_before_d056_no_longer_wedges` runs the run
it was pinned on and keeps saying what it says — the absence of both halves, with their
non-vacuity, and the search over 0..1000 that D-078 already recorded as finding no wedge
that needs both bugs.

**On the node the pair is the stream half alone, and the search cannot be run yet.**
`IgnoreIncarnation` is a no-op on this node (below), so a seed the pair is "caught on"
here is a seed `SharedSnapshotDir` alone is caught on — not a wedge that needs both
bugs, which is what D-045 pinned. Measured on a share of 20 seeds: the pair on 7, `{1, 4, 5, 9, 12, 16, 17}`, the
stream half alone on exactly that set, the incarnation half alone on 0, and **on 0 seeds is the pair
caught where neither half alone is**.
`the_pair_on_the_node_is_the_stream_half_alone_until_the_reseed_lands` asserts the
**per-seed sets**, not the counts, and asserts the pair-only set empty. The counts were
what it compared until the review: a count equality cannot see the pin disappear, since
a genuine D-045 wedge on one seed and a stream-only catch on another leave the counts
equal and the wedge — the whole reason the pair exists — unreported. `IgnoreIncarnation`
alone is caught on 0 of 10 000 on one group too (RAFT.md:697, SHARD.md:1579), so that
assertion is not the guard either; the two set assertions are. **This goes to the
owner**: the pair is Phase 2's control for a wedge that needs both bugs, and on the node
it has only one bug to work with until PR #86 lands.

### The rates, every one measured on the node before its assertion was written (D-061)

**The machine** (D-070). Darwin 25.6.0 arm64, Apple M2, 8 cores, **on AC Power**, no
thermal warning recorded, with other agents' slices building on it throughout: load
averages **7.96/11.11/16.52**. Every figure below is a **gate-tier (20 seed) or CI-tier
(100 seed)** figure and is labelled as one. **`scripts/premerge.sh` has not run**; it is
the owner's to schedule.

The four stream variants on the node. Every figure below was re-measured on the tree
that ships, after the re-review found the previous set did not match it; they are
deterministic across repeated runs. `SharedSnapshotDir`'s test runs `high_rate_share()`,
a tenth of the tier, so its counts are over that share and its tier is named beside them
(D-061).

| Variant | On the node | Phase 2's standard | Verdict |
|---|---|---|---|
| `SnapshotWithoutCurrentLast` | caught **0/100**; `Fault::CrashInstalling` reached the final chunk of the range it drew and crashed there on **0/100** | caught on some seed at every tier | **not met — to the owner**; see below, its one assertion is in question too |
| `SharedSnapshotDir` | over a share of 100 at the thousand-seed tier: fault fired **100/100**, scramble **17/100** with 14 duplicate-chunk loops, aimed arm **1/100**, **caught 30/100**, every one by the liveness check. At the nightly's tier, over a share of 1 000: caught **266/1 000**, scramble **172/1 000**, arm **21/1 000** | fault + arm at every tier; scramble from 100; liveness at 10 000 | **fault at every tier; scramble and arm moved up; liveness at Phase 2's 10 000 — two weakenings to the owner** |
| `IgnoreIncarnation` | the correct node reset a follower's progress **0** times and the variant **0**, over **0** store refusals, on a share of 20 | injection at every tier, reach from 100 | **blocked on PR #86 — see below** |
| `{IgnoreIncarnation, SharedSnapshotDir}` | pair **7/20** on `{1, 4, 5, 9, 12, 16, 17}` = stream half alone **7/20**, the same set; incarnation half **0/20**; pair-only **0/20** | pinned on a seed of the first thousand | **blocked with its half — to the owner** |

**The 30 % is a fact about the node and not a fourth artefact of this slice**, which
matters because the two figures before it were artefacts. The re-review established it
four independent ways: every catch is the liveness check and the per-seed sets agree;
the directory half is **necessary** — disable only `version_dir`'s variant path and the
catch is 0/100 — and **sufficient** — disable only the one-stream cap and it is 18/100
with the scramble at 27/100; and it is not a fragility of the take or the lookup, since
failed checkpoints fall from 905 per hundred seeds on the broken tree to 67, while
lookup misses under the variant run about 64 000 per hundred seeds against about 1 100
on the correct node, a ratio the variant's own rewriting produces.

The range stays in the directory name, so this is not two ranges colliding with each
other — that is `VersionDirWithoutRange`'s bug (D-075). It is that one `apply` task takes
for four ranges and one `snapshot` task streams for them, so a repeated applied index,
and with it a take into a directory a stream already has open, comes round far more often
than on a server with one range.

**Where the four assertions sit, and why two moved.** The tier gates read the **tier**
and the counts read the **share**; they read the share for both until this round, which
put every assertion one tier higher than it claimed and left the liveness catch needing
`ANANKE_SEEDS` of 100 000 — it never ran at any tier this project uses. With that fixed:

- **the fault, at every tier**: 100 % of seeds. The shared name makes a second take at a
  repeated applied index a re-take by construction, which is why it is every seed;
- **the scramble, from the thousand** where Phase 2 has it from a hundred. At 17 % the
  hundred-seed tier's sample is twenty and sees none about one run in forty — a flake;
  the thousand's sample of a hundred sees none about once in 10^8;
- **the aimed arm, from ten thousand** where Phase 2 has it at every tier. About 2 %:
  the variant wedges runs before `RetakeUnderStream`'s setup completes, and with the
  directory half disabled the arm returns to 15/100, which is what says the fall is the
  variant's effect and not a broken arm;
- **the liveness catch at ten thousand**, Phase 2's own tier. 30 % would carry it at a
  hundred and it is deliberately not written there, because §12 re-asserts a Phase 2
  variant to its own standard **and no stronger**.

The two moves are weakenings on this cluster's own measurements, and they are the
owner's to confirm.

**What `scrambled` does and does not explain.** It is a minority of the catches — at
most 17 of the 30, and 4 of 25 on a per-seed probe — so calling
`retakes_under_streams` "the wedge's stream half", as this entry did, overstates it. The
dominant observable is the rewrite making a version unfindable: a lookup miss, a retake,
the cascade, the wedge. Same bug; that fold sees one face of it.

The seven D-082 measured are unchanged in kind and moved where the schedules moved, the
threshold being a disk-work change: `SendBeforePersist` **20/20**, `ApplyBeforeCommit`
**20/20**, `NoPreVote` **20/20**, `TruncateOnEveryAppend` **20/20**,
`CountOlderTermForCommit` **7/20** (35 %, was 14/20), `ResetTimerOnAnyRpc` **10/20**
(50 %, unchanged), `LeaseTrustsTheClock` **0/100**, which keeps the thousand-seed tier D-082 set for it. All are above 5 % but the last,
which keeps its thousand-seed tier as D-082 set it.

**`IgnoreIncarnation` is blocked by Q15's re-seed, not by the wiring**, and the reason is
one fact with a citation. A leader resets a follower's progress when the store
incarnation that follower answers with **changes** (`note_incarnation`,
core.rs:1834-1862), and a store's incarnation is "1 for a store started fresh, a fresh
value on every store a re-seed rebuilt" (store.rs:28). The node's live install
deliberately **keeps** it — "an install into a live store keeps its incarnation: the kept
tail is everything acknowledged past the snapshot, so nothing a leader matched is lost"
(`ServerHost::repair`, D-042). So no replica's incarnation ever changes on this node, the
correct leader never resets either, and an assertion that the variant's leader traces no
reset would pass on a node where nothing was injected — the one failure mode a sweep
cannot report on its own. The test asserts the **absence with its reason and its
non-vacuity** instead: no reset under either leader, no refusal, over runs asserted to
reach the install path. The day PR #86's re-seed lands, the correct node resets, the test
fails, and the variant is re-asserted then rather than a day later.

**Neither `SnapshotWithoutCurrentLast` nor `SharedSnapshotDir`'s re-take half has its
tier lowered here.** D-061's rule is that a catch under 5 % is asserted from the
thousand-seed tier and that the tier a Phase 2 variant keeps is the owner's. At a
measured arm-firing of 2 % and a catch of 0, an assertion at any local tier would be a
green that means nothing, and one at the nightly would be a guess. So the assertions are
**not written weaker — they are not written**, the rates are printed at every tier, both
variants keep their Phase 2 assertions on `Cluster::OneGroup` unmoved, and the numbers
are here for the owner to rule on. What *is* asserted is each variant's own
non-vacuity at the tier its rate carries: `SharedSnapshotDir`'s fault firing and its arm
reaching a stream, at every tier; `SnapshotWithoutCurrentLast`'s arm firing, from a
thousand. The install variant's test asserted `actions > 0` until the review, which
`checked()` already asserts on every seed of every sweep — so it could not fail, and
removing the variant's bit from `with_repair` left the node binary green at a hundred
seeds. `aimed_installs` is the discriminator the test now asserts on — and the number
below says that assertion is itself in question.

**What the arms do on a node, which is the owner's decision to make.** On one group
`Fault::CrashInstalling` has one range to aim at and its victim is behind that range by
construction. On a node the victim is drawn without regard to which of its four ranges
it is behind on, so the arm must find a victim behind *that* range's compacted prefix,
designated for it, and streamed within `INSTALL_WAIT_BUDGET`. Measured on the tree that
ships it does so on **0 of 100 seeds**.

**That leaves `SnapshotWithoutCurrentLast` with an assertion whose own premise is
unmeasured.** Its catch is not asserted anywhere, and the one thing that is —
`aimed_installs > 0` from the thousand-seed tier — rests on a rate of zero at a hundred.
Whether it holds at a thousand is measured and recorded beside this paragraph; if it
does not, the variant has no assertion on the node at all and that is the state to take
to the owner rather than to paper over. Aiming an arm at a range its victim is actually
behind on would change the number, and it is a change to how the arms are drawn — the
owner's, not this slice's.

**For `SharedSnapshotDir` the aim was never the blocker**, which is the correction this
entry owes twice over: its fault fires on every seed and its wedge is caught on 30 % of
them. What its aimed arm wants is not a better aim but a bigger sample, which is what
moving that one assertion to the nightly's tier does.

### The mutation table: what a single-range world could not catch

The owner's standing demand on this stage is that a check with more than one range to
be wrong about show the mutation a single-range world could not catch. Seven mutations
were planted in the harness **one at a time**, each run at **100 seeds**, each reverted
before the next. Every one of them is a no-op on one group: with a single range,
`range_of` answers the only range there is, the key map has one answer, the client's
per-range leader map has one key and the trial hands over the one range.

| | Mutation | What it does on one group | Caught by |
|---|---|---|---|
| **M1** | `leader_of_range` ignores its `range` argument | nothing: one range, one leader | **the arms floor.** 115/115 (100 %) correct, **82/115 (71.3 %)** mutated, against a 90 % floor. Not one in four, because three nodes hold four ranges and the two leaders often coincide — which is why this needed a measured floor and not an intuition |
| **M2** | `Schedule::range_of` always answers the first range | nothing: `range_picks` is empty and the first range is the only one | **`the_nodes_arms_aim_at_every_range_and_not_at_one`** |
| **M3** | the node's key map answers one range | nothing: every key is in the one range | **the apply-spread floor.** least over busiest **0.87** correct, **0.13** mutated, against a floor of 0.5. `applies.contains_key(&range)` passed it: a range that applied only its leader's no-ops satisfies "every range applied something" |
| **M4** | the client keeps one leader for the cluster, not per range | nothing: one range, one leader | **not caught.** Every check passes. The run gets slower and noisier — more `NotLeader` round trips — and, oddly, `LeaseTrustsTheClock` is caught on **4 of 100** seeds where the correct harness catches 0, because a client that keeps asking the wrong node reads more staleness into the window. Recorded, not covered: what would catch it is a ceiling on the clients' redirects, and a ceiling on a count that faults legitimately raise is a bound the correct system would trip |
| **M5** | the Figure 8 burst writes the first range's key whatever the arm drew | nothing: one range | **not caught**, and on reflection it should not be. `CountOlderTermForCommit` stays at 14/20: the driver needs *a* backlog of more than `max_batch` uncommitted entries behind an isolated follower, and a backlog is a backlog whichever range carries it. What the aimed key buys is that the backlog is on the range whose leader the arm steered, which is tidier but is not what the window depends on |
| **M6** | the lease trial hands over one range, not every range the node holds | nothing: one range | **not caught.** `LeaseTrustsTheClock` is 0/100 either way at this tier, so there is no rate to move, and the lease coverage stays above its floors. The place this would show is the thousand-seed tier, where the variant's catch lives; it is recorded so the premerge can be read against it |
| **M7** | Q41's round stops honouring `Variant::SendBeforePersist` | n/a — this is the hole this branch closed | **`a_server_that_sends_before_it_persists_is_caught_on_the_node`**, 0/20, which is exactly what the branch found before it was fixed |

**Four of seven caught, three recorded and not covered.** Two of the four — M1 and M3 —
were *not* caught when the campaign began: both figures were printed and neither had a
floor, and the campaign is what turned two prints into two bounds. That is the campaign
earning its cost, and it is the honest reading of the table: a mutation campaign whose
every row is caught has usually been written after the checks rather than against them.

### The pinned seeds, re-audited

SHARD.md's Stage B names the node as one of three commits of the stage that may move
every schedule, and asks each of them to re-audit every pinned seed in
`sim/tests/raft.rs` (SHARD.md:2361-2370). Under the shape this entry chose, **the
one-group schedules do not move**, and the audit begins with the evidence for that
rather than with an assurance: seed 42's moirae JSONL is 12 898 025 bytes and hashes
to `445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on
`origin/main` and on this branch, from `the_seed_42_trace_is_written_for_the_studio`
run in a worktree of each. A schedule that moved would move that trace: it carries
every message, every delivery, every fault and every step of a run with two lease
trials, the drawn faults, the install crash, the adoption storm, the refusal storm and
the re-take, over 12.9 MB.

So no pin's run changed, and the audit is of what each pin **says**, under
CLAUDE.md:58-67: a mechanism, or the situation's absence with the reason. Every one
was read and every one was run at the gate's twenty seeds on this tree. Twenty-three
tests pin a seed or a fixed list of seeds:

| Pinned | What it pins | Verdict |
|---|---|---|
| 42, `same_seed_gives_byte_identical_trace` | determinism: two runs' JSONL byte for byte | **mechanism** |
| 42, `the_seed_42_trace_is_written_for_the_studio` | writes `out/raft-42.jsonl`, then `check().unwrap()` | **bare green**, and the only one: see below |
| 164 | a snapshot-fed timer gap, absent — `snapshot_fed_timer_gaps()` empty, with the reason and the instruction to pin the mechanism the day it is not | absence, with reason |
| 385 | an install's restatement inside a follower's timer window, absent (`timer_gaps_rescued_by_restatement()`) | absence, with reason |
| 2605 | the correct half: an adoption window reached at all (asserted non-vacuous) and no timer gap rescued by one; the variant half: `ResetTimerOnAnyRpc` caught with the **exact violation string**, its majority up, five gaps, none resting on an adoption | absence + **mechanism** |
| 7381 | a recovered applied index replayed under a lost floor, absent; and no install below a floor already reached | absence, with reason |
| 6325 | under `Correct` and `AdoptionAsBuilt`: an adoption window reached (non-vacuity) and no crash inside one | absence, with reason |
| 5909 | `refusal_reset_reseed(3)` present — the D-042 path the pin is about — and no stream wedge | **mechanism** + absence |
| 5909, the pair | under each half and both: passes, takes a snapshot at all (non-vacuity), no index taken twice, and the exact stale and uncounted sets per variant | absence, with per-variant equalities |
| 132 | under the pair, each half and `Correct`: the per-variant equalities for a twice-taken index, a refusal, the stale set and the uncounted set, and the correct server's `refusal_reset_reseed(2)` | absence + **mechanism** |
| 680 | under the pair, each half and `Correct`: no wedge, and under `IgnoreIncarnation` the mechanism itself — server 3 refused, and the leader **not** resetting its progress, which is the fix the variant turns off | absence + **mechanism** |
| 687 | the fix's half record by record — table 74 dropped twice, the engine quiesced over a recovery that lost writes, the store refused again after the quiesce, no manifest written or `CURRENT` switched between them; the bug's half absent with its reason | **mechanism** + absence |
| 1885, 2023 | the pre-vote straddle the nightly failed on, absent by decision time and present by durability time, with the isolation named | absence, with reason |
| the nightly's eleven | eleven (seed, variant) pairs of the same straddle, each absent, each run still passing, with `KEPT` naming the two whose isolation still comes | absence, with reason |
| the nightlies' twenty-eight | every catch the ten-thousand-seed nightlies removed: each asserted removed for its reason, and the four with a bug of their own asserted **caught over that bug with its exact violation string** | **mechanism** + absence |
| seed 1 of the term-raise schedule | seven isolations straddled by decision time, the first with its exact cause `[(1, "append-entries", 2)]`, and `Moved::Lost` of the named violation; D-050's own shape absent | **mechanism** + absence |
| seed 4 of the term-raise schedule | D-050's shape, to the nanosecond: `received` 1 224 602 510, `from` 1 224 610 000, `decided` 1 225 627 305, `until` 1 524 610 000, with the cause `(1, "request-vote", 2)` | **mechanism** |
| 512, the follower-log bound | `FollowerNeverCompacts` caught **by the bound's own violation** ("follower log:"), its log past the bound, and the correct server's under it | **mechanism** |
| 102 | `RefusalNotDurable`'s mechanism absent — no lost-state refusal, no crash on a refused server, no restart after one — with the reason | absence, with reason |
| 158 | the same, plus no table dropped and no engine quiesced | absence, with reason |
| 119 | the same, plus the two runs are **not** record for record identical | absence + a difference |
| membership seed 7 | determinism of the membership scenario's JSONL | **mechanism** |

Twenty-two of the twenty-three assert a mechanism, an absence with its reason, or
both. **One is a bare green**: `the_seed_42_trace_is_written_for_the_studio`, whose
body is `write_trace(...)` and `report.check().unwrap()`. It is not a pin a sweep
found — seed 42 is the studio's example trace, and `echo`, `wal` and `engine` each
have a test of the same name — so CLAUDE.md's rule about a seed a sweep found does not
reach it; it is still a `check().unwrap()` on a fixed seed in a file where every other
fixed seed says what it is for. **Issue #97**, with both readings and the question for
the owner.

Two smaller findings, both **issue #98** rather than fixed here (one change per pull
request):

- `seed_102_pins_the_refusal_that_is_not_durable_which_a_hundred_seeds_can_miss` is
  named for a mechanism its body no longer asserts: D-078 recorded that the mechanism
  left the seed and moved the body to four absences, correctly, and the name did not
  move with it.
- `seed_687_which_the_premerge_found_stays_green` looks the quiesce up on
  `NodeId::new(1)` while its `expect` message says "server 3's engine". One of the two
  is wrong, and which it is decides whether the pin still asserts what it says. That
  one is worth checking before it is renamed.

### The node's snapshot wiring: designed here, built next, and owned

`ananke_shard::snapshot` (D-075) is the discipline — keys, caps, frames, switches —
and says of itself that "the engine call an `Install` describes
(`Engine::install_spans`, D-068) and the streams' bytes are the node's wiring, which
Stage B's scenarios slice puts under the sweeps". This is that slice. Neither `main`
nor any open branch wires it, and PR #86 records the same thing from the other side:
what a re-seeded replica waits for "is the node's snapshot *wiring*, which this slice
does not build". So the wiring has had no owner, and it is taken here: **designed in full below, and
built on the branch that follows this one**, by whoever is holding this entry. It is
not built in this pull request for one reason, and it is a reason about review and not
about scope: this branch already carries a harness, a second cluster, a production fix
to Q41's round and a re-audit, and the wiring is a task, a queue, four `Local` variants,
an apply-task rewrite and a live install per range — the size of D-075 again. One
reviewer cannot hold both, and a wiring that is waved through is worse than a wiring
that waits a day. What must not happen is the wiring belonging to nobody, and it does
not: it belongs to the branch after this one, and its design is here so that the owner
can rule on it before a line of it is written.

**What the node has today, and what is wrong with it.** `Host::snapshot` is

```rust
fn snapshot(&self, _range: RangeId, _action: SnapshotAction) {
    lock(&self.gaps).snapshot_actions += 1;
}
```

and two things follow that are worth stating before any code:

- **The counter is write-only.** `ServerHost::gaps()` is reachable only through the
  `Node`, and `server::run` moves the `Node` into a spawned task and hands nothing
  back, so no scenario can read it — the same fact D-076 recorded of
  `Meters::locals_held`. A node that reached this path would count it and no sweep
  would see it. That is why the node sweep asserts the *condition* behind every
  action — a log longer than `snapshot_threshold` — and not the absence of a
  `RaftSnapshot` record, which would be an absence asserted against a silence.
- **`InstallSnapshot` is admitted and then swallowed.** The `net` task admits every
  decoded message to the inbox, `Inbox::carries_data` deliberately admits an
  `InstallSnapshot` by dropping heartbeats to make room, and `Raft::on_message`'s arm
  for it is empty, because "the server routes these to it before the core sees them"
  — which the one-group server's `net` loop does and the node's does not. So a chunk
  that reached a node today would cost a heartbeat its place in the inbox and then
  vanish. **Nothing reaches the node today, so nothing is lost**: it is a latent hole,
  not a live bug, and the wiring closes it. **Issue #96**.

**The shape.** A fourth task, `snapshot`, beside `net`, `answers` and `apply`, on a
fourth handle of the one socket — not a second bind, which would give it a different
address and make it a different peer. It owns a `Snapshots` (D-075) as its planner and
keeps the I/O the planner deliberately has none of:

1. **`Host::snapshot` becomes a push**, onto a `Queue<SnapJob>` the host owns beside
   `jobs` and `answers`, closed after `Node::raft` returns as those are. The trait
   method is already `&self` and synchronous, which is exactly what a queue push
   needs and what an await would not allow.
2. **A take is the `apply` task's**, between two applies, as RAFT.md §1 has it and
   D-036 requires for exact metadata. `ApplyWork` is `#[non_exhaustive]` and already
   has `Take`, which `Node::act` already routes there and `ServerApplier::run`
   currently drops; the arm is filled, and `Record` (D-078's follower compaction) is
   added beside it. `ServerApplier` has to carry per range what the one-group apply
   task carries for its one group: the applied index, the applied **term**, and the
   configuration at that index, followed entry by entry. A take writes the record
   first, synced, then checkpoints the range's spans into
   `Snapshots::version(range, index, take)`; a record writes the record and no
   checkpoint, and traces no `RaftSnapshot`, which the one-group task is explicit
   about because tracing it moved the sweep's count of installs by a factor of four.
3. **A stream out** is a `raft::snapshot::Sender` per (range, follower), opened on the
   version directory `Snapshots::stream` pinned, with `Snapshots::route` cutting each
   chunk into a frame of its own (Q41) for `sock.send`. There is no per-node cap on
   streams sent, so a leader feeds every designated follower of a range at once
   (D-043).
4. **A stream in** is an assembly per (range, sender), staging under
   `Snapshots::staging(range, from)`, with `Snapshots::on_chunk` deciding between
   staging the bytes, restarting the sender, making it wait for a slot under the
   node's receive cap, and completing.
5. **A completed stream is D-066's live install**, and this is where the node parts
   from the one-group server for good. The one-group server ends its run-loop
   incarnation and reopens the engine, "which in a shared node would restart every
   range on it"; the node calls `Engine::install_spans(spans, source, repair)` — the
   staged tables and the range's repair in **one** manifest switch, the engine left
   open, the node's other ranges untouched. The staged directory is a checkpoint of
   the sender's spans, so `Engine::open_span_source` reads it directly and is also the
   check that it is whole: a directory left short by a crash fails there and the
   stream restarts, which is the property `Fault::CrashInstalling` aims at.
6. **The repair** is the range's own term, vote, applied index, snapshot record, log
   tail, configuration, quarantine and incarnation, with tombstones for the log keys
   the tail does not replace, written under **the receiver's** key prefix and carried
   in the switch. `NodeVariant::InstallWithoutRepair` is the node's translation of
   `SnapshotWithoutCurrentLast`, which D-075 already named: it makes the switch
   without the repair, and what catches it is state machine safety after the crash
   the arm aims at.
7. **The answers to the core** go back the way the `apply` task's do: a push onto the
   node's `Queue<Local>`, which `Node::raft` already races, turning into
   `Input::SnapshotTaken`, `SnapshotInstalled`, `SnapshotFailed` and `SnapshotAcked`
   through `Host::local_input`. `Local` grows one variant per answer, each carrying
   its range. Nothing about the round changes.
8. **The `net` task diverts `InstallSnapshot` and `InstallSnapshotResponse`** to the
   snapshot queue before `Inbox::admit`, so a chunk is charged to the snapshot task's
   receive cap and not to the inbox's byte bound. That moves where those bytes are
   counted, which the inbox's drops are measured against, and the entry records the
   figures on both sides of it.

**What it opens that the tree has not answered.** From the first live install on a
node's engine, a read served from a pinned version can straddle the switch on its own
span, since a read below the install's number sees the replaced span as empty (D-069,
D-054). SHARD.md's Stage B carries this as **issue #72** and asks the install item to
say what such a read sees and to pair it. It is not a question this wiring can settle
on its own, and it is put to the owner with the wiring rather than decided here.

### What is not built, and what goes to the owner

- **The install path on the node was not built by anyone**, and this slice takes it:
  `ananke_shard::snapshot` was not wired to `ServerHost` on `main` or on any open
  branch, and D-075 said in as many words that "the streams' bytes are the node's
  wiring, which Stage B's scenarios slice puts under the sweeps" — this slice. It is
  designed in full above and built on the branch that follows this one, for the reason
  given there. Until it exists a variant on that path cannot be re-asserted on the node
  at all, and the node's sweep says so on **every seed** rather than passing. `checked`
  in `sim/tests/node.rs` fails a seed two ways: on a snapshot action traced, and — the
  one that matters — on any replica reaching an index at or past this cluster's
  `snapshot_threshold`, which is the condition behind every action a core can ask for.
  The second is there because the first is not evidence on its own: the node's
  `Host::snapshot` bumps `Gaps::snapshot_actions` and traces nothing, so an action the
  node dropped on the floor would leave the first clause green against a silence. A
  variant whose situation a run cannot reach is the one failure mode a sweep cannot
  report on its own, and an absence asserted against a silence is how it hides.
- **`AdoptionAsBuilt` is re-asserted on the two rules that remain, and §10 is not
  amended.** SHARD.md's Stage B asks this stage to return the variant to the owner
  with a proposal: "re-asserted on the two rules that remain, under a crash arm aimed
  at the live install's switch and at the refused directory's open, at its Phase 2
  tier; or §10 amended for it". The owner has already ruled this exact shape. Answering
  the intervals slice on 2026-09-20, on a tree where one of the variant's three rules
  likewise had no path: *"AdoptionAsBuilt: re-assert it on the two rules that remain
  rather than amend §10."* That ruling is applied here rather than asked for again, and
  it is recorded as a precedent applied so the owner can overturn it.

  **The rule with no path, and why.** The variant breaks three rules (RAFT.md:696).
  The second is *a damaged staging `CURRENT` refused*, and it has no path on the node
  for a reason that is the node's whole design and not an omission: a replica's
  install on the node is Stage A's live install into the running engine, and the
  whole-store staged install adopted at the next start is **not kept**
  (SHARD.md §12, question 2's proposal). A node that adopts no staged store at its
  start has no staging `CURRENT` to find damaged, and neither has the fault that
  reaches it — `Fault::CrashAdopting` crashes "the moment the adoption's first change
  to the store directory is durable", and there is no such adoption. The rule is not
  weakened, dropped or amended out of §10: it has no subject on this node.

  **The two that remain, and where each is re-asserted.** The first, *copy and switch
  before delete*, becomes the live install's single manifest switch, which the variant
  breaks by removing the span's keys in a switch of their own before adding the
  tables; its path is `Engine::install_spans`, which is the snapshot wiring designed
  below and built on the branch after this one. The third, *a marked store never opens
  fresh*, becomes Q15's refused directory, which stays marked lost and which the
  variant neither checks nor writes; its path is PR #86's. So the variant is
  re-asserted on the node in two places, neither of which exists in this tree, and in
  neither case is the standard lowered: it keeps its Phase 2 tier. Until both land it
  keeps its Phase 2 assertion on `Cluster::OneGroup`, unmoved.
- **`RefusalNotDurable` is the sixth, and it stacks on PR #86.** Its rule on the node is
  the whole node's mark in the refused directory (Q15), which #86 builds. It is not
  re-asserted here and is re-asserted in the commit that follows #86 into this branch.
- **The pair's pin on seed 680 is not retired, because nothing moved it.** SHARD.md's
  Stage B says the node's schedule move retires D-045's pin and that the first thousand
  seeds are searched again. Under this shape the one-group schedules do not move — that
  is the point of (2) — so seed 680 keeps asserting exactly what it asserts today, and
  the re-audit below states it. The pair's re-assertion on the node is blocked behind
  the install path with the rest.
- **The take's hold on the node's other ranges' applies (D-036) cannot be measured
  here**, for the same reason: this node takes no snapshot, so there is no take to
  hold anything. What is measured instead, above, is the hold by an ordinary apply —
  the same mechanism, one `apply` task per node taking every range's jobs one at a
  time (Q14), with the take's own hold owed by the slice that wires it.
- **That hold's maximum goes to the owner**, which is what §12 says to do with this
  figure when it exceeds a heartbeat interval. At 100 seeds the median is 2.07 ms and
  the **maximum is 572.9 ms**, against 20 ms. It is one apply job of one range —
  clamped to a single job's span by the fold, so not a crash or an isolation misread —
  with another range's committed entry waiting behind it throughout. It bears on §11's
  storage item 6 rather than on Q14's grouped applies, since grouping does not shorten
  a job, and it is a **floor** under D-036's real subject: a take is strictly longer
  than an ordinary apply.
- **Follower compaction (D-078) does not run on the node either.** The core's
  compaction asks the `apply` task for `SnapshotAction::Record`, which this host
  counts and drops, so the follower-log bound Stage B's exit asks for cannot be
  asserted on the node. It stays asserted on `Cluster::OneGroup`, where D-078 measured
  it.

---

## PROPOSED D-083 — The node runs its `snapshot` task: a take of the range's own spans, a stream per (range, follower), and D-066's live install with the range held across its switch

**Context.** SHARD.md §12's Stage B lists "installs on the node" (§11, storage 5) and no
branch has built it. `ananke_shard::snapshot` is the discipline — keys, caps, frames,
switches — merged as PR #83 under PROPOSED D-075, and `ananke_shard::server::run` never
ran it; PR #86 states the same thing from the other side, that what a re-seeded replica
waits for "is the node's snapshot *wiring*, which this slice does not build". D-082
designed that wiring in full and, for a reason about review rather than scope, left it
to the branch after it. This is that branch. Four Phase 2 variants, the directed re-seed
shape and the quorum sweep on the node are all behind it.

D-082's design is followed where it holds. Four places it is departed from are set out
below, each with what was found and what was done instead; one of them is a hole in the
design rather than a preference, and one is an issue filed against it after it was
written (#103).

### What is built

`crates/ananke-shard/src/install.rs`, the fourth task, beside `net`, `answers` and
`apply`, on a fourth handle of the one socket — not a second bind, which would give it
a different address and make it a different peer. It owns the node's `Snapshots` (D-075)
as its planner and keeps the I/O the planner deliberately has none of.

1. **`Host::snapshot` is a push**, onto a `Queue<SnapJob>` the host owns beside `jobs`
   and `answers`, closed after `Node::raft` returns as those are. `server.rs:566-571`,
   where a core asking for a snapshot action bumped `Gaps::snapshot_actions` and nothing
   else happened, is that push now. The counter is kept: a scenario that asserts the
   path's absence keeps its figure, and it is no longer the *only* thing that happens.
2. **A take is the `apply` task's**, between two applies, as RAFT.md §1 has it and D-036
   requires for exact metadata. `ApplyWork::Take` was already routed there and dropped;
   the arm is filled and `Record` (D-078's follower compaction) added beside it.
   `ServerApplier` now carries per range what the one-group `apply` task carries for its
   one group — the applied index, the applied **term**, and the configuration at that
   index, followed entry by entry — because a record whose term or configuration is off
   by one apply is a record D-029's revert floor reads wrong.
3. **A stream out** is a `raft::snapshot::Sender` per (range, follower), opened on the
   version directory that range's *own* snapshot record names and pinned to it for the
   stream's life (D-043), with `Snapshots::route` cutting each chunk into a frame of its
   own for `sock.send` (Q41). There is no per-node cap on streams sent, so a leader
   feeds every designated follower of a range at once.
4. **A stream in** is an assembly per (range, sender) staging under
   `Snapshots::staging(range, from)`, with `Snapshots::on_chunk` deciding between
   staging, restarting, waiting under the node's receive cap, and completing. The
   file-level bookkeeping an acknowledgement needs — which file, which offset — is the
   task's, since no chunk carries it and the planner counts bytes rather than files.
5. **A completed stream is D-066's live install**: `Engine::open_span_source` on the
   staged directory, then `Engine::install_spans(spans, source, repair)` — the range's
   two key intervals and the receiver's repair in **one** manifest switch, the engine
   left open, the node's other ranges untouched. The staged directory is a checkpoint,
   so the engine's own checks are what say it is whole: one left short by a crash fails
   at `open_span_source` and the stream restarts, which is the property
   `Fault::CrashInstalling` aims at.
6. **The answers go back through the node's `Queue<Local>`**, which `Node::raft` already
   races, turning into `Input::SnapshotTaken`, `SnapshotInstalled`, `SnapshotFailed` and
   `SnapshotAcked`. `Local` grows one variant carrying a `SnapAnswer`, and every one of
   them names its range.
7. **The `net` task diverts `InstallSnapshot` and `InstallSnapshotResponse`** to the
   snapshot queue *before* `Inbox::admit`. **This closes issue #96**, and the entry says
   so because the hole stops being latent the moment this lands: the inbox deliberately
   admits a chunk by dropping heartbeats to make room (`Inbox::carries_data`), and
   `Raft::on_message`'s arm for it is empty, because "the server routes these to it
   before the core sees them" — which the one-group server's `net` loop does and the
   node's did not. A chunk that reached the node would have cost a heartbeat its place
   under the byte bound and then vanished. Nothing reached the node before this wiring,
   so nothing was lost; this wiring is what makes chunks arrive, and the divert is what
   catches them. It also moves where those bytes are counted — a chunk is charged to the
   snapshot task's receive cap (D-075) and not to the inbox's bound, which is what the
   inbox's drops are measured against. `NodeVariant::ChunksToTheInbox` keeps the hole
   beside the fix, and it is caught on every seed of the directed scenario.

### Where this departs from D-082, and why

**1. The stream carries no log key, and the repair therefore tombstones none.**

D-082's item 6 has the repair carry "tombstones for the log keys the tail does not
replace", which is what `Assembler::finish` does on the one-group path: it walks the
staged tables, collects every log index they hold, and tombstones the ones the receiver's
kept tail does not overwrite. Built here, the take checkpoints the range's Raft state
**except its log purpose**, and its user keys. The install's spans are still the range's
two whole intervals, so the switch removes the receiver's own log along with everything
else in them, and the repair's kept tail is all that goes back.

The end state is the same and the reasons to prefer it are three. It is fewer bytes on
the wire — a leader's whole log, streamed and then deleted. It removes a standing
obligation on the receiver to enumerate what the sender sent, which is a second place
for the two ends to disagree. And the enumeration is not reachable from this crate
anyway without widening `ananke-raft`'s surface: `SpanSource` exposes no key walk, and
every SST and key helper the one-group walk uses is `pub(crate)` there, which is Q40's
line and not an accident. The cost is that the two install paths no longer build their
repair from the same set of log keys; what keeps them from drifting is that they build
it from the same *function* — see below.

**2. One builder for the repair's writes, in `ananke-raft`.**

D-082 did not say where the repair is built. It could not be built in `ananke-shard` at
all: every key constructor (`hard_key`, `applied_key`, `log_key`, `config_key`,
`snapshot_key`, `quarantine_key`, `incarnation_key`) and every encoder
(`encode_hard`, `encode_applied`, `encode_snapshot_record`, `encode_config`,
`encode_incarnation`, `encode_entry`) is `pub(crate)` in `ananke-raft`, which owns what a
Raft key is. Rather than widen that, `ananke_raft::snapshot::repair_writes` is added:
`pub`, returning a `WriteBatch` in key order, with `replaced_log` as a parameter so a
caller whose source carries no log keys passes an empty set and gets no tombstones —
which is not the same thing as forgetting them. `Assembler::finish` was rewritten to use
it and is behaviour-preserving; the one-group tests pass unchanged. There is now one
statement of what a repair *is*, used by two switches of very different shapes.

**3. `ananke_raft::snapshot::staged_record`: the configuration, out of the streamed bytes.**

Neither D-082 nor D-075 says where the receiver gets the configuration in force at the
snapshot's last index. It needs it twice — the repair writes it into the snapshot record,
and `Raft::restore_compacted` takes it as the floor a revert cannot go below (D-029) —
and no message carries it. It cannot use the configuration *it* believes is in force: a
replica being fed a snapshot is behind by definition, and a membership change inside the
compacted prefix is exactly what it has not seen. The one-group path reads it while it
verifies the staged store; a live install verifies nothing of its own, because the engine
does that at `open_span_source`. So a reader is added beside the repair builder, in the
crate that owns the layout: one key, read from the staged directory's own tables, nothing
written.

**4. The design's hole: a live install needs the range *held*, and its replica *replaced*.**

D-082's item 7 ends "Nothing about the round changes". That is not true of the receive
side, and the gap is not small.

A server ends its whole run-loop incarnation across an install and comes back on the
store the switch built: that is how its core learns it installed, and how nothing steps
that core across the switch. A node cannot do either, because reopening the engine "would
restart every range on it" (SHARD.md §11, storage 5) — which is the whole reason the
install is live. So two things had to be built that D-082's four `Local` variants do not
describe:

- **The hold.** Between the install's decision and its manifest switch, the range's core
  must step nothing. The replica being replaced is behind its leader by definition, so a
  step of it in that window appends entries at indices the switch is about to compact
  past, and the store comes back with a log below its own snapshot record. The node
  already has exactly this primitive — a core whose persist is outstanding queues its
  messages and its node-local inputs — so the install reuses it: `Slot::installing`
  beside `Slot::persisting`, and the same queue. The hold is **one range's**; every other
  range on the node goes on stepping, which is the cost a live install exists to avoid.
  A hold asked for while that range's persist is outstanding waits behind it like any
  other local input, so the install never switches under a write still in flight.
- **The replacement.** When the switch is durable the range's core is rebuilt on the
  state the repair wrote — term, vote, snapshot index and term, the configuration in
  force at it, the kept tail, the quarantine — and takes the place of the one it
  replaced, with the work held meanwhile stepped into it in the order it arrived.

Both are asked for through one new `Host` method, `local_core`, answering a `CoreWork` of
`Step`, `Hold`, `Restore` or `Release`. It is deliberately not an `Input`: neither of
these is a step of a core, and expressing them as steps would put an install's
bookkeeping inside a Raft core that knows nothing about a node's engine. `Step` is the
default, so a host with no install path answers it for every input and nothing about the
round changes for it — which is the sentence D-082 wanted, true of every host but this
one.

**And an ordering the hold depends on.** `Host::local_core` is not a pure question: the
install's repair is built in its `Ready` arm, from the held core, and handed to the
`snapshot` task there. So the node has to know whether an input is *held* before it asks
what the input wants — asked the other way round, a `Ready` that arrived while its own
range's persist was still outstanding would build a repair and let the switch carrying it
proceed, against a write in flight, which is the one thing the hold exists to prevent.
The check therefore comes first, and `Host::local_releases` — pure, and default
`false` — is what lets the two answers that *end* a hold through it rather than queueing
them behind the hold they end.

**A bug this found, and it is the reason the hold and the replacement are not enough on
their own.** The first run of the directed scenario stopped a node with

```
range RangeId(5): an apply through 81 names index 1, which the core does not hold
```

`Cores` keeps the highest index handed to the `apply` task per range, beside the cores,
because the entries an `Apply` names are read at the step that named them. A switch that
replaced the replica and left that watermark where the *replaced* core stood had the next
`Apply` name an index below the installed snapshot, the node failed that range, and
`Node::raft` returned — so the node stopped, and the installs that had not happened yet
never did. Five of eight installs completed, and the three that did not looked like a
timing problem. The watermark moves with the replica now. It is recorded here because
nothing about the hold or the replacement suggests it, and only a scenario that installs
on several ranges of one node reaches it: with one range, the node stops after its only
install and the run has nothing left to get wrong.

### Issue #103: which core a stream's answer is stepped into

Filed against this design while it was being built. `Input::SnapshotAcked { to }` names
the follower and not the range — complete information for a server with one core,
incomplete for a node with four. A wiring that took the follower's identity as the
address would set `stream_acked` on every range's progress for that follower, and D-049's
rule (core.rs:1607-1613) — a *refused* follower counts for check quorum only while its
re-seed stream progresses — would hold on one range and be void on the other three, with
no event, no counter and every check green.

**Decided: the address is the range, and it travels on the `Local`, not in the `Input`.**
Every node-local input already carries its range and is routed by `Host::local_range` to
exactly one core (D-073, D-076); the snapshot answers carry theirs the same way. The
range was *not* added to `Input::SnapshotAcked`, and the reason is that it would be the
weaker fix, not the cheaper one: all four snapshot inputs have this shape, the field
would be read by nothing — a core knows its own range — and one of the four carrying it
would make the other three look addressed when they are not. What the wiring gains
instead is a variant and a check: `NodeVariant::SnapshotAckToEveryCore` fans the answer
to every core on the node, and `acked_for` is the decision it mutates, asserted
deterministically. The check also asserts the thing the issue is really about — that with
one range on the node the fan-out and the correct route are the same route, so no
single-range scenario could catch it even in principle.

### The directed evidence: a stream flows and an install completes

`sim/install.rs` and `sim/tests/install.rs`. Five voters and four ranges; three nodes
start, **two start late**, after the running three have written past `snapshot_threshold`
and their leaders have compacted. The two are then behind every range's compacted prefix,
so each range's leader streams to both at once: four ranges times two followers.

Two late nodes rather than one is deliberate. With a single follower behind the prefix, a
leader that fed its followers one at a time would be indistinguishable from one that fed
them all at once, and `CapStreamsSent` — D-043's rule, one of the seven D-075 says a
single-stream world cannot catch — would have no situation here at all.

The writer round-robins over the *ranges* rather than drawing a key at random. With a
random draw, some seeds left a range short of its threshold, that range's late replica
caught up by AppendEntries and needed no install, and the check reported correct
behaviour as a missing install. Every range crosses its threshold by construction now.

On seed 1, and on every seed the binary runs:

| What | Figure |
| --- | --- |
| streams opened, as (leader, range, follower) | 8 of 8 owed |
| installs completed on the node, as (server, range) | 8 of 8 owed |
| replicas created by their install (`RangeCreated { cause: snapshot }`) | 8 of 8 owed |
| the most streams one leader had running at once | 6 |

`Report::reached` fails any seed that does not reach the situation — no take at all, or
fewer than four ranges taking — before any of the rest is asserted, because a scenario
built to exercise a path is worth nothing if a seed quietly fails to reach it. That is
the same rule `sim/tests/node.rs` keeps from the other side, and it is why the two do not
contradict each other: that binary holds `snapshot_threshold` at `1 << 30` and asserts
the path is *not* reached; this one is built to reach it and fails if it does not.

**`RangeCreated { cause: snapshot }` on this node.** §8 defines the cause as an install
onto a node that held no initialised replica of the range. It is traced when the replica
the install replaces held nothing — no applied index, no log, no snapshot — so the
install is what gave that range state. A node restarted on a store it kept does not reach
it; a replica that bootstrapped empty and was filled by a stream does, which is this
scenario, and Q15's re-seed into a fresh engine is the case the cause exists for.

### The mutation table: what a single-range world could not catch

The owner's standing demand on this stage. Seven mutations, each built beside the correct
code and each seen to fail a check the correct code passes. **Four of the seven are the
correct wiring exactly on a node of one range or one follower**, and are marked.

| Variant | The mutation | Caught by | Needs more than one range or follower |
| --- | --- | --- | --- |
| `TakeCheckpointsTheWholeNode` | the take checkpoints the whole engine directory, as the one-group take does, instead of the range's own key intervals | `a_stream_flows_…`, 3/3 seeds: 8 of 8 installs refused at the switch | **yes** — with one range the engine *is* that range's spans and the two takes are the same bytes |
| `InstallHoldsEveryRange` | the hold is taken on every range the node hosts, which is the node reaching for the incarnation a server ends | `a_live_install_holds_its_range_alone_…`: another range's work waits on this one's switch, and `ranges_held_for_install` is 2 rather than 1 | **yes** — with one range, holding "every" range is holding this range |
| `InstallSweepsEveryStaging` | a completed install clears every staging directory, destroying a neighbour's half-assembled stream without telling its sender | `a_completed_install_clears_its_own_assemblys_directory_alone` | **yes** — with one range and one sender there is only ever one directory to clear |
| `SnapshotAckToEveryCore` | the follower's identity taken as the address, so one range's chunks answer every range's core (issue #103) | `a_streams_acknowledgement_answers_its_own_ranges_core_alone` | **yes** — with one core the fan-out is the correct route |
| `StepWhileInstalling` | the range is stepped inside its own install's window, so the replica being replaced appends at indices the switch is about to compact past | `a_live_install_holds_its_range_alone_…` | no |
| `InstallKeepsTheOldCore` | the switch is made and the replica is not replaced: the store holds the snapshot and the core goes on from the log it had | `a_live_install_holds_its_range_alone_…`: the node runs at the old core's term | no |
| `ChunksToTheInbox` | `InstallSnapshot` admitted to the byte-bounded inbox instead of diverted, which is issue #96 as it stood | `a_stream_flows_…`, 3/3 seeds: 8 of 8 installs never complete | no |

`CapStreamsSent` is D-075's variant rather than this slice's, and it is listed because
this scenario is the first thing in the tree to reach its situation: it is caught on 3/3
seeds, and it needs more than one follower behind the prefix. Two more of D-075's —
`VersionDirWithoutRange` and `SweepAcrossRanges` — are deliberately *not* asserted here:
their situations are two ranges taking at one index, and one range's sweep running over
another's versions, neither of which this scenario produces. They keep D-075's
deterministic checks, which build those situations on purpose. Listing them would have
been a sweep that passes because nothing was injected, which is the one failure mode a
sweep cannot report on its own. `InstallWithoutRepair` is left out for a nearer reason:
the install still *completes* under it — the switch is made, the event is traced — and
what it breaks is state machine safety after a crash, which this scenario has no crash
arm to reach.

### What is unblocked, and what is not taken

Now reachable on the node, and not before: a snapshot stream, an install that completes,
and every counter and event a scenario needs to assert their situations — `RaftSnapshot`
with `taken` true for a take and false for an install, `RaftSnapshotStreams` per (range,
follower), `RaftSnapshotResumed`, and `RangeCreated { cause: snapshot }`. The quorum
sweep's runs recorded 0 `RaftSnapshotStreams` and 0 installs on the node; both are
non-zero now.

**`RaftReseeded` was emitted by the node for no replica at all**, which is a gap this
slice found beside its own and closes: the one-group server states it at every
restatement of a quarantined replica (node.rs:975-980), and the node's restatement did
not, so the event would have been *missing* for the slice that refuses a node's store
rather than merely zero. It is emitted now in both places the node states a replica —
its start, and the restoration a live install builds, which keeps the quarantine across
the install (D-035). It stays at zero until a store is refused, because nothing on this
node is quarantined yet and Q15's refusal is PR #86's; what has changed is that the
zero is now an observation rather than a silence.

Not taken here, and each owed to the slice that owns it:

- **The four Phase 2 variants on the node** — `SnapshotWithoutCurrentLast`,
  `IgnoreIncarnation`, `SharedSnapshotDir` and the pair
  `{IgnoreIncarnation, SharedSnapshotDir}` — are the sweeps slice's to re-assert, and
  are not re-asserted here.
- **The directed re-seed shape** (D-067's `ReseedMarkNotSynced`, and exit points (a), (b)
  and (d) PR #86 records as owed) is buildable now and is a later slice's.
- **This scenario under the four tiers**, with the weight D-064's shard table wants. It
  runs a fixed eight seeds at every tier today, and deliberately does not read
  `ANANKE_SEEDS`: its situation is reached by construction on every seed rather than
  searched for, so at the nightly's tier it would be ten thousand runs of five nodes for
  twenty simulated seconds — about a whole shard's CPU — bought against a catch that does
  not vary. Tiering it is owed, and the figure has to be measured on a quiet machine:
  taken on this laptop, with several Stage B slices building on it, it would be exactly
  the incomparable kind D-070 exists to stop.
- **Issue #72** is now live rather than latent: from the first live install on a node's
  engine, a read served from a pinned version can straddle the switch on its own span,
  since a read below the install's number sees the replaced span as empty (D-069, D-054).
  D-082 put it to the owner with the wiring; this entry does not settle it either, and
  records that the wiring is what makes it reachable.
- **`AdoptionAsBuilt`'s first rule** — copy and switch before delete — has its path now,
  `Engine::install_spans`, and D-082 already carries the owner's ruling on how the
  variant is re-asserted. Doing it is the slice that owns the crash arm.

### Consequences

`ananke_shard::snapshot` is no longer a module the node does not run. `ananke-raft` gains
two public functions and keeps its layout private. The one-group server is unchanged in
behaviour: `Assembler::finish` builds its repair from the shared builder and its tests
pass as they stood. `sim/` gains one scenario and one test binary, and the three
scenarios that ran before this are unchanged but for `ServerConfig::snapshot_cap`, which
every one of them sets at its range count as D-075 recommends for a scenario that is not
about the cap.

---

## PROPOSED D-086 — The node reaches the stream path: four bugs the reach found, and what each of Phase 2's four stream variants measures there

**The number.** The footer reads D-084 on this branch's base. Two branches open at the
same time are taking the next free number against their own copy of this file, so a
number taken from the footer here would collide with one of them at the merge. This
entry takes **D-086**, past both, and the footer moves to D-087. Every code site carries
`// PROPOSED(D-086)`. The integrator may renumber it at the merge; nothing in the tree
depends on the number beyond those markers, this heading and the footer.

**Context.** SHARD.md §12's Stage B re-asserts Phase 2's sixteen variants on the node
"to the standard their Phase 2 tests assert and no stronger, at the tier each uses
today" (Q39; §10). PROPOSED D-082 re-asserted seven of them on `sim/raft.rs`'s arms and
recorded four as **blocked**, by name and for one reason: the node had no `snapshot`
task, so "until it exists a variant on that path cannot be re-asserted on the node at
all". PROPOSED D-083 built the wiring and listed the same four as owed:
`SnapshotWithoutCurrentLast`, `IgnoreIncarnation`, `SharedSnapshotDir` and the pair
`{IgnoreIncarnation, SharedSnapshotDir}` — "the sweeps slice's to re-assert, and are not
re-asserted here". This is that slice.

### What reaching the path cost, and why that is the finding

The node's sweep held `snapshot_threshold` at `1 << 30` — far above what its clients
write — and `Schedule::draw_on_the_node` took `Fault::CrashInstalling` and
`Fault::RetakeUnderStream` out of every schedule. Both were absences D-082 asserted on
every seed, with their reason, rather than left to be found. This entry turns the
threshold down to the one-group sweep's **12**, which is also `sim/install.rs`'s, and
puts the two stream arms back.

**The correct node then failed on 17 of the gate's 20 seeds**, and the five faults
behind it are the substance of this slice. Every one of them is a node bug that no
scenario in the tree could reach, every one is fixed here, and no bound was widened for
any of them (D-030, D-039; CLAUDE.md).

1. **The applied watermark is zero at every start** (`Cores::insert`, round.rs).
   `Cores` keeps the highest index handed to the `apply` task per range, because the
   entries an `Apply` names are read at the step that named them. D-083 found that a
   live install must move it and fixed it at `Cores::installed`; **the start path was
   left at zero**. A node whose log is never compacted restarts on a log that still
   begins at index 1, and zero is correct for it — so nothing saw this until a node
   restarted on a compacted log. The first one did: *"an apply through 41 names index 1,
   which the core does not hold"*, and the node stopped. The watermark is now the core's
   at every start.

2. **A live install leaves the `apply` task's applied state behind** (`ServerHost`,
   `ServerApplier`, server.rs). The switch replaces a range's state machine with no
   entry passing through that task, and the task, still holding what it applied before,
   fails the range's next job on the gap: *"range 2: apply of 13 after 10"*. The map is
   shared with the host now and the switch moves the range's entry — index, term **and**
   configuration, because the take reads all three for its snapshot record and D-083
   records that a record whose term or configuration is off is one D-029's revert floor
   reads wrong. `sim/install.rs` could not reach it: its late nodes have applied
   nothing, and the applier's `if applied == 0` fallback covers exactly that case.

3. **A live install leaves the store's own caches stale**
   (`RaftStore::restate_after_install`, store.rs). The hard state, the log bounds and
   the applied index are in-memory caches of keys the store writes itself, in `persist`
   and `apply`. `Engine::install_spans` writes all of them in one manifest switch that
   passes through neither, and the store then refuses the range's next apply on the old
   replica's applied index: *"applying 13 after 10"*. A server needs none of this — it
   ends its run-loop incarnation across an install and opens the store again — and a
   node cannot, which is the same sentence D-083 wrote for the hold and the replacement
   and is now true of three more fields.

4. **`SnapshotAction::Record` was dropped on the floor** (node.rs). D-078's follower
   compaction asks the `apply` task for a `Record`. `Node::act` routed only `Take`
   there; `Record` fell to `Host::snapshot`, and `install::job_of` answers `None` for
   it. **The cost is not a missing compaction.** The core sets `take_pending` when it
   asks and only an answer clears it, so a replica that asked once **never asked for
   another snapshot for the rest of its life**. When that replica later leads and a
   follower falls behind its log, `replicate` finds `taken` empty and `take_pending`
   set, asks for nothing, and sends an empty AppendEntries at its own last index
   forever while the follower rejects with a hint the leader is not in a branch to
   read. The range commits nothing again. That is the **liveness bound, tripped by the
   correct node**, on seeds 17, 20 and 64 of the first hundred; the evidence is seed
   17's range 2, where server 2 led at term 8, appended to index 378 and committed
   nothing after 13 s while servers 1 and 3 rejected 150 and 158 times with hints 222
   and 58. Nothing before this slice could see it: `sim/install.rs` has no follower that
   compacts and later leads, and every other node scenario holds `snapshot_threshold`
   above what its clients write, so `Record` was never asked for at all.

5. **A stream opened on the store's snapshot record, and that record moves**
   (`Task::open`, install.rs). This one the fourth fix *uncovered*: with `Record`
   answered, takes become frequent, and the `apply` task rewrites the range's snapshot
   record on every one of them while the `Install` the core asked for carries the index
   its `taken` held when it asked. Matched exactly, a take landing between the ask and
   the read moved the record past the ask, `open` answered `retake`, and the core took,
   asked and lost the race again — **a cascade in which the install never completes,
   however long the run is given**, which is how it was told apart from a slow install:
   `sim/install.rs`'s seed 7 failed at `AFTER` of 14 s and failed identically at 20 s.

   The fix took three attempts and the first two are worth recording, because each was
   plausible and each was wrong. Matching the record **at or past** the ask fixed the
   cascade and broke something worse: the stream's identity then moved with every take,
   so each re-open of one logical install carried a new identity, `is_streaming` never
   suppressed the duplicate, and each fresh stream restarted the receiver's assembly
   under it (RAFT.md:203-207) — three leaders of one range opening streams at three
   identities at once, and the install never completing for that reason instead.

   What is correct is the **one-group server's own rule**, which neither attempt
   consulted: open a **complete version directory of the index the core asked for**
   (`start_stream` and `snapshot::find_version`, crates/ananke-raft/src/node.rs:2182 and
   snapshot.rs:165-191). `ananke_shard::snapshot::find_version` is that function keyed
   by range. The core's ask is the only one of the three candidates that is *stable*,
   version directories are numbered so an earlier one survives later takes — which is
   what the take counter is for (D-043) — and the record's motion stops mattering.

   It needed one more thing: **the node's take did not write D-060's checkpoint format
   record**, which `checkpoint_complete` requires, so every version this node wrote read
   as incomplete and no stream opened at all. `format::write_checkpoint_record` is `pub`
   now and the take calls it, as `snapshot::take_numbered` always has. A crash between
   the engine's checkpoint and that record leaves a version that reads incomplete, costs
   a retake and never streams a half-written checkpoint, which is the property the
   record exists for.

With the five fixed, **the correct node passes every seed of the gate's twenty and CI's
hundred**, over **5 912** snapshot actions at 100 seeds where D-082 asserted zero, and
`sim/install.rs`'s three tests pass unchanged.

**A sixth fault was in this slice's own variant translation, and the review found it.**
It is not a node bug — the correct node never reuses a version directory — but it made
this entry's central measurement a measurement of nothing. `ServerApplier::take` did not
empty the version directory before checkpointing into it, where the one-group take has
always done so (`snapshot::take_numbered`, snapshot.rs:671-677). `SharedSnapshotDir`
names its directory `snap-<range>-<index>` with no take counter, so a re-take at one
index finds that directory full; `Engine::checkpoint_spans` refuses a non-empty
directory with `AlreadyExists` (engine.rs:1517-1523); and `take_failed` answers the core
with no `RaftSnapshot { taken: true }` traced. **The variant therefore never rewrote a
directory a live stream had open — D-043's actual bug — it turned the re-take into a
failed take.** Every fold that reads the symptom was structurally zero, and this entry's
first draft reported "the re-take half is not built on the node" on that basis. The take
empties the directory now, which is a no-op for the correct node (a fresh numbered name
is empty) and is what lets the variant express itself. The rates below are the
re-measured ones; the first ones are withdrawn.

### The one-group cluster did not move, and the evidence is not an argument

D-082's shape holds: everything here is on `Cluster::Node`, on the node's own code, or
on folds whose one-group answer is unchanged by construction. Seed 42's moirae JSONL is
**12 898 025 bytes** and hashes to
`445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on this branch —
byte for byte the figure D-082 recorded for `origin/main` and for its own branch. Every
pinned seed in `sim/tests/raft.rs` therefore runs the run it was pinned on, and the
whole binary passes: 46 tests, seeds 164, 385, 680, 687, 2605, 5909, 7381, 102, 118,
132, 158 included.

### Seed 680, and the search SHARD.md asks for

SHARD.md's Stage B says the node's schedule move retires D-045's pin on seed 680, that
seed 680's test asserts the wedge where the moved schedule still reaches it or, with the
reason, the situation's absence, and that the first thousand seeds are searched again
for a seed the pair is caught on.

**Seed 680's pin did not move and is not touched**, for the reason D-082 gave and the
hash above proves: this slice changes nothing about `Cluster::OneGroup`, so
`seed_680_which_pinned_the_combined_variant_before_d056_no_longer_wedges` runs the run
it was pinned on and keeps saying what it says — the absence of both halves, with their
non-vacuity, and the search over 0..1000 that D-078 already recorded as finding no wedge
that needs both bugs.

**On the node the pair is the stream half alone, and the search cannot be run yet.**
`IgnoreIncarnation` is a no-op on this node (below), so a seed the pair is "caught on"
here is a seed `SharedSnapshotDir` alone is caught on — not a wedge that needs both
bugs, which is what D-045 pinned. Measured on a share of 20 seeds: the pair on 7, `{1, 4, 5, 9, 12, 16, 17}`, the
stream half alone on exactly that set, the incarnation half alone on 0, and **on 0 seeds is the pair
caught where neither half alone is**.
`the_pair_on_the_node_is_the_stream_half_alone_until_the_reseed_lands` asserts the
**per-seed sets**, not the counts, and asserts the pair-only set empty. The counts were
what it compared until the review: a count equality cannot see the pin disappear, since
a genuine D-045 wedge on one seed and a stream-only catch on another leave the counts
equal and the wedge — the whole reason the pair exists — unreported. `IgnoreIncarnation`
alone is caught on 0 of 10 000 on one group too (RAFT.md:697, SHARD.md:1579), so that
assertion is not the guard either; the two set assertions are. **This goes to the
owner**: the pair is Phase 2's control for a wedge that needs both bugs, and on the node
it has only one bug to work with until PR #86 lands.

### The rates, every one measured on the node before its assertion was written (D-061)

**The machine** (D-070). Darwin 25.6.0 arm64, Apple M2, 8 cores, **on AC Power**, no
thermal warning recorded, with other agents' slices building on it throughout: load
averages **7.96/11.11/16.52**. Every figure below is a **gate-tier (20 seed) or CI-tier
(100 seed)** figure and is labelled as one. **`scripts/premerge.sh` has not run**; it is
the owner's to schedule.

The four stream variants on the node. Every figure below was re-measured on the tree
that ships, after the re-review found the previous set did not match it; they are
deterministic across repeated runs. `SharedSnapshotDir`'s test runs `high_rate_share()`,
a tenth of the tier, so its counts are over that share and its tier is named beside them
(D-061).

| Variant | On the node | Phase 2's standard | Verdict |
|---|---|---|---|
| `SnapshotWithoutCurrentLast` | caught **0/100**; `Fault::CrashInstalling` reached the final chunk of the range it drew and crashed there on **0/100** | caught on some seed at every tier | **not met — to the owner**; see below, its one assertion is in question too |
| `SharedSnapshotDir` | over a share of 100 at the thousand-seed tier: fault fired **100/100**, scramble **17/100** with 14 duplicate-chunk loops, aimed arm **1/100**, **caught 30/100**, every one by the liveness check. At the nightly's tier, over a share of 1 000: caught **266/1 000**, scramble **172/1 000**, arm **21/1 000** | fault + arm at every tier; scramble from 100; liveness at 10 000 | **fault at every tier; scramble and arm moved up; liveness at Phase 2's 10 000 — two weakenings to the owner** |
| `IgnoreIncarnation` | the correct node reset a follower's progress **0** times and the variant **0**, over **0** store refusals, on a share of 20 | injection at every tier, reach from 100 | **blocked on PR #86 — see below** |
| `{IgnoreIncarnation, SharedSnapshotDir}` | pair **7/20** on `{1, 4, 5, 9, 12, 16, 17}` = stream half alone **7/20**, the same set; incarnation half **0/20**; pair-only **0/20** | pinned on a seed of the first thousand | **blocked with its half — to the owner** |

**The 30 % is a fact about the node and not a fourth artefact of this slice**, which
matters because the two figures before it were artefacts. The re-review established it
four independent ways: every catch is the liveness check and the per-seed sets agree;
the directory half is **necessary** — disable only `version_dir`'s variant path and the
catch is 0/100 — and **sufficient** — disable only the one-stream cap and it is 18/100
with the scramble at 27/100; and it is not a fragility of the take or the lookup, since
failed checkpoints fall from 905 per hundred seeds on the broken tree to 67, while
lookup misses under the variant run about 64 000 per hundred seeds against about 1 100
on the correct node, a ratio the variant's own rewriting produces.

The range stays in the directory name, so this is not two ranges colliding with each
other — that is `VersionDirWithoutRange`'s bug (D-075). It is that one `apply` task takes
for four ranges and one `snapshot` task streams for them, so a repeated applied index,
and with it a take into a directory a stream already has open, comes round far more often
than on a server with one range.

**Where the four assertions sit, and why two moved.** The tier gates read the **tier**
and the counts read the **share**; they read the share for both until this round, which
put every assertion one tier higher than it claimed and left the liveness catch needing
`ANANKE_SEEDS` of 100 000 — it never ran at any tier this project uses. With that fixed:

- **the fault, at every tier**: 100 % of seeds. The shared name makes a second take at a
  repeated applied index a re-take by construction, which is why it is every seed;
- **the scramble, from the thousand** where Phase 2 has it from a hundred. At 17 % the
  hundred-seed tier's sample is twenty and sees none about one run in forty — a flake;
  the thousand's sample of a hundred sees none about once in 10^8;
- **the aimed arm, from ten thousand** where Phase 2 has it at every tier. About 2 %:
  the variant wedges runs before `RetakeUnderStream`'s setup completes, and with the
  directory half disabled the arm returns to 15/100, which is what says the fall is the
  variant's effect and not a broken arm;
- **the liveness catch at ten thousand**, Phase 2's own tier. 30 % would carry it at a
  hundred and it is deliberately not written there, because §12 re-asserts a Phase 2
  variant to its own standard **and no stronger**.

The two moves are weakenings on this cluster's own measurements, and they are the
owner's to confirm.

**What `scrambled` does and does not explain.** It is a minority of the catches — at
most 17 of the 30, and 4 of 25 on a per-seed probe — so calling
`retakes_under_streams` "the wedge's stream half", as this entry did, overstates it. The
dominant observable is the rewrite making a version unfindable: a lookup miss, a retake,
the cascade, the wedge. Same bug; that fold sees one face of it.

The seven D-082 measured are unchanged in kind and moved where the schedules moved, the
threshold being a disk-work change: `SendBeforePersist` **20/20**, `ApplyBeforeCommit`
**20/20**, `NoPreVote` **20/20**, `TruncateOnEveryAppend` **20/20**,
`CountOlderTermForCommit` **7/20** (35 %, was 14/20), `ResetTimerOnAnyRpc` **10/20**
(50 %, unchanged), `LeaseTrustsTheClock` **0/100**, which keeps the thousand-seed tier D-082 set for it. All are above 5 % but the last,
which keeps its thousand-seed tier as D-082 set it.

**`IgnoreIncarnation` is blocked by Q15's re-seed, not by the wiring**, and the reason is
one fact with a citation. A leader resets a follower's progress when the store
incarnation that follower answers with **changes** (`note_incarnation`,
core.rs:1834-1862), and a store's incarnation is "1 for a store started fresh, a fresh
value on every store a re-seed rebuilt" (store.rs:28). The node's live install
deliberately **keeps** it — "an install into a live store keeps its incarnation: the kept
tail is everything acknowledged past the snapshot, so nothing a leader matched is lost"
(`ServerHost::repair`, D-042). So no replica's incarnation ever changes on this node, the
correct leader never resets either, and an assertion that the variant's leader traces no
reset would pass on a node where nothing was injected — the one failure mode a sweep
cannot report on its own. The test asserts the **absence with its reason and its
non-vacuity** instead: no reset under either leader, no refusal, over runs asserted to
reach the install path. The day PR #86's re-seed lands, the correct node resets, the test
fails, and the variant is re-asserted then rather than a day later.

**Neither `SnapshotWithoutCurrentLast` nor `SharedSnapshotDir`'s re-take half has its
tier lowered here.** D-061's rule is that a catch under 5 % is asserted from the
thousand-seed tier and that the tier a Phase 2 variant keeps is the owner's. At a
measured arm-firing of 2 % and a catch of 0, an assertion at any local tier would be a
green that means nothing, and one at the nightly would be a guess. So the assertions are
**not written weaker — they are not written**, the rates are printed at every tier, both
variants keep their Phase 2 assertions on `Cluster::OneGroup` unmoved, and the numbers
are here for the owner to rule on. What *is* asserted is each variant's own
non-vacuity at the tier its rate carries: `SharedSnapshotDir`'s fault firing and its arm
reaching a stream, at every tier; `SnapshotWithoutCurrentLast`'s arm firing, from a
thousand. The install variant's test asserted `actions > 0` until the review, which
`checked()` already asserts on every seed of every sweep — so it could not fail, and
removing the variant's bit from `with_repair` left the node binary green at a hundred
seeds. `aimed_installs` is the discriminator, 1/100 against 0/100, and it is asserted
where 1 % belongs.

**What the two rates say about the arms, which is the owner's decision to make.** On one
group `Fault::CrashInstalling` has one range to aim at and its victim is behind that
range by construction. On a node the victim is drawn without regard to which of its four
ranges it is behind on, so the arm must find a victim behind *that* range's compacted
prefix, designated for it, and streamed within `INSTALL_WAIT_BUDGET` — and it does, on
1 seed in 100. `RetakeUnderStream` reaches its stream on 16 seeds in 100 by the same
shape.

**For `SharedSnapshotDir` the aim is not the blocker**, and saying so is the correction
this entry owes: with the sixth fault fixed its fault fires on 25 % of seeds and its
scramble on 2 %, so what it wants is not a better-aimed arm but more seeds, which is
what moving the scramble to the thousand-seed tier does. For `SnapshotWithoutCurrentLast`
the aim *is* the blocker: at 1 % nothing local carries its catch. Aiming an arm at a
range its victim is actually behind on is a change to how the arms are drawn — the
owner's, not this slice's — and the figure it would have to reach is D-061's 5 %.

### The mutation table: what a single-range world could not catch

The owner's standing demand on this stage: a check with more than one range to be wrong
about shows the mutation a single-range world could not catch. Thirteen are below. Nine
were **planted one at a time**, each reverted before the next and
each restore followed by an assertion that the next build really recompiled the unit
(issue #102: a file restored by copy carries an mtime older than the build that compiled
the mutant, and cargo then calls the unit fresh). Three were not planted at all — they
are the live bugs the reach found, which is stronger evidence than a plant.

Every one of the thirteen is a **no-op on one group**: with a single range, `range_of`
answers the only range there is, a take's `(server, index)` names it uniquely, a watch
for "a stream to the victim" can only mean the one range's, and a server rebuilds every
cache here by ending its run-loop incarnation.

| | Mutation | Caught by |
|---|---|---|
| **M1** | `Report::retakes_under_streams` groups takes by `(server, index)`, dropping the range | **not caught**, and re-run at a thousand seeds once the re-take half *was* reached, because the first reading of this row ("the half is not reached, 0 either way") stopped being true. The figures are identical with the range and without it — caught 30/100, fault 100/100, scramble 17/100, 14 loops, arm 1/100 — and the reason is that the fold's other keys already separate the ranges: the chunks and the stream openings it matches on are filtered by range regardless, so dropping the range from the *take* grouping alone changes nothing it can observe. The range in that key is defence in depth, not the load-bearing part |
| **M2** | the take-counter check drops the range from its key, which is exactly how `sim/tests/raft.rs`'s own `took_an_index_twice` reads it | **caught while the check was keyed on the index, and the catch is withdrawn with the check.** Keyed on `(server, index)` the correct node looked like it "re-took at an index it had already taken" on **95 of 100** seeds against 0, and the assertion failed. Then the fifth fix landed and the *correct* node's figure went to **46 of 100** — because after a live install the core's `taken` names a snapshot this replica never took (D-078), the record behind it names an older one, and the take that follows is at an index already taken, into a directory of its own. **The index was never the property**; the directory is, and the check is keyed `(server, range, index, dir)` now, which the correct node satisfies on every seed and `SharedSnapshotDir` breaks by construction. A range-blind key does not fool the directory check — `snap-r2-24-2` and `snap-r3-24-2` differ — so this row ends **not covered**. The campaign's real yield here was finding that the bound it produced was wrong |
| **M3** | `install_landing` waits for the final chunk of *any* range | **not caught**, recorded with a number: the install crash's firing rises from **1/100 to 17/100**. A range-blind watch reports the arm working eight times as often while crashing the victim at some other range's install — the arm would look healthy and be aiming at nothing |
| **M4** | `stream_opened` waits for a stream opening to the victim of *any* range | **not caught**, recorded: the re-take arm "reached its stream" on **18/100** against **16/100**, and the freeze that follows would hold a leader over a stream of a range it did not draw |
| **M5** | `Report::raft_messages` decodes one-group frames only — which is what it did before this slice | **not caught today, and this is the row to read twice.** On a node `Frame::decode` sees none of a batch frame, so every fold built on it — `retakes_under_streams`, `duplicate_chunk_loop`, the two `SharedSnapshotDir`'s catch is read from — answers **empty on every seed, whatever the node does**. The numbers do not move only because the re-take half is not reached either way. It is fixed here and recorded as not covered: an assertion over a fold that cannot report is green because nothing was read |
| **M6** | `SharedSnapshotDir`'s shared version directory drops the *range* as well as the take | **not caught**, recorded: two ranges' checkpoints colliding is `VersionDirWithoutRange`'s bug (D-075) and not D-043's, and the variant made strictly more broken catches no more |
| **M7a** | `SharedSnapshotDir`'s directory drops the take counter from the name entirely, rather than pinning it at zero | **caught by its own incoherence rather than by a check, and that is the point.** `parse_version` does not parse `snap-r<range>-<index>`, so `find_version` could not see the variant's own directories, every stream answered `retake`, and the run wedged: a 38 % liveness "catch" that was the lookup failing. Recorded because it is the third time in this slice that a number about the node turned out to be a number about the harness |
| **M7** | `Cores::insert` leaves the applied watermark at zero | **found as a live bug, not planted.** 17 of the gate's 20 seeds: "an apply through 41 names index 1, which the core does not hold", and the node stops |
| **M8** | a live install moves neither the `apply` task's applied state nor the store's caches | **found as a live bug.** "range 2: apply of 13 after 10", then one layer down "applying 13 after 10", on 12 of 20 seeds |
| **M9** | `SnapshotAction::Record` is not routed to the `apply` task | **found as a live bug.** The liveness bound, on the **correct** node, on seeds 17, 20 and 64 of the first hundred |
| **M10** | a stream opens only on a record whose index matches the ask exactly | **found as a live bug**, by fixing M9: `sim/install.rs`'s seed 7, one install of eight never completing at any run length |
| **M11** | `SnapshotAction::Record` routed to the node's **first** range rather than the range that asked — textually a no-op on one group, which holds one range | **the snapshot-actions floor**, at the gate's twenty: 23.3 actions a seed against the correct node's 58.8 and a floor of 30. The node sweep sees it otherwise only from **seed 25**, so the gate's twenty node seeds were green and only `sim/install.rs`'s seed 1 saw it. This is the reviewer's mutation and it is the reason the floor is 30 rather than 20, where it passed |
| **M12** | `SnapshotAction::Record` dropped on the floor again, which is M9 as it stood | **the same floor**, at the gate: 11.6 a seed. Before the floor existed, only seed 17 of the gate's twenty caught it, by the liveness bound |

**Two of the nine planted mutations are caught by an assertion, four were live bugs,
and seven are recorded and not covered.** The two that are caught, M11 and M12, are both
caught by a bound the **review** asked for and not by anything this slice wrote on its
own: nothing here checked that a replica which asks for a snapshot gets an answer, and
D-078's follower compaction had no direct check at all. The one mutation this slice's
own campaign caught, M2, was caught by a bound the campaign itself had just invented,
and the fifth fix then showed that bound wrong about the correct system — so that row is
withdrawn rather than kept.

What has teeth here are the checks about the *correct* node's own behaviour: the take
counter's directory property, the arms hitting the range they drew, the snapshot-actions
floor, and the five failures the reach itself produced. `SharedSnapshotDir`'s catch now
has teeth too, at the tiers its own rates support. `SnapshotWithoutCurrentLast`'s does
not, and its arm firing on 1 seed in 100 is why. A campaign whose every row
is caught has usually been written after the checks rather than against them; this one
was written against them, and it says where the sweep is thin.

### The cost this slice adds, which is the one thing it does not settle

Making `SharedSnapshotDir` express its bug made the node's sweep **much more
expensive**, and the figures belong to the owner because Q39 holds
`scripts/premerge.sh` near a quarter of an hour.

The variant's own churn is most of it and is inherent: it wedges **30 % of seeds**
against one group's 4 in 10 000, and a wedged run plays out its whole length. Its test
runs `high_rate_share()` for that reason — a tenth of the tier, which at rates of 100 %,
17 %, 2 % and 30 % measures each well enough to place its assertion.

The correct node is dearer too, and that part is not the variant's: every take now writes
D-060's checkpoint format record and empties its directory first, and every stream open
reads a record and probes for a complete version. Takes are frequent — 59 a seed — so
these are not rare paths.

**The four rows in `scripts/nightly-shards.txt` are known LOW, and that file now says so
rather than claiming they are weighed on this tree.** The re-review measured
`a_leader_that_shares_one_snapshot_directory_is_caught_on_the_node` at **438.2 cpu s**
against its row of 265.8 (+65 %) and the correct-node sweep at about **244** against
163.5 (+49 %); shard 6, already the heaviest at 1 567.6, therefore lands near **1 740**,
about 19 % over the ideal rather than the 6 % the placement note claims. The rows are
deliberately **not** re-placed on new figures: re-weighing here was attempted and
abandoned at load average 177, which is the incomparable kind D-070 exists to stop, and
the owner takes it when the machine is quiet.

**This goes to the owner as a cost question, not a correctness one.** The options, none
of them taken here: leave it, shrink the variant's share further, or shorten the node
cluster's runs. What would be wrong is to buy the time back by making the variant express
less, which is twice now the state a review has found this entry in.

### What is not taken, and what goes to the owner

- **`SnapshotWithoutCurrentLast` has no assertion below the nightly's tier**, and that
  is the plainest way to say its state. Its catch is 0 of 1 000 and is asserted nowhere;
  its arm fires on 0 of 100 and 1 of 1 000, so the one assertion it has — the arm's own
  firing — sits at ten thousand, because at a tenth of a per cent a thousand seeds would
  see none about one run in three and fail a tree with nothing wrong. Its standard is not
  lowered and it keeps its Phase 2 assertion on one group.
- **`SharedSnapshotDir`'s two moved assertions**: the scramble from Phase 2's hundred to
  a thousand, because at 17 % the hundred-seed tier's sample of twenty sees none about
  one run in forty; and the aimed arm from every tier to ten thousand, at about 2 %. Both
  are weaker than Phase 2's and both are the owner's to confirm. Its fault fires on every
  seed and is asserted at every tier, and its liveness catch sits at Phase 2's own ten
  thousand and no stronger.
- **`IgnoreIncarnation` and the pair** are blocked on PR #86's whole-node refusal and
  re-seed. They are re-asserted in the commit that follows #86 into this branch, as
  D-082 says of `RefusalNotDurable`.
- **The pair has no wedge of its own on the node** until then, so D-045's control for a
  wedge that needs both bugs is unavailable here. This is the point the plan says to
  bring to the owner if no seed of the first thousand catches the pair, brought with the
  evidence above rather than with a search whose answer is already determined by one
  half being a no-op.
- **Aiming the stream arms per range.** The two arms draw a victim and a range
  independently; a victim that is behind the range the arm drew is what the arm is
  about, and the draw does not ask for one. The figures are above.
- **The take's hold on the node's other ranges' applies (D-036)**, which D-082 recorded
  as owed by the slice that wires the take: the node takes snapshots now, so the figure
  is measurable for the first time. It is not taken here, and the ordinary-apply hold
  D-082 measured has moved with the threshold: at 100 seeds the median is **2.36 ms**
  and the maximum **606.6 ms**, against D-082's 2.07 ms and 572.9 ms and a heartbeat
  interval of 20 ms. The maximum goes to the owner again, larger than before.
- **`AdoptionAsBuilt`'s first rule** still has no crash arm aimed at the live install's
  switch; `Fault::CrashAdopting` and `Fault::CrashRefused` stay out of the node's draw,
  each with its reason recorded in `Fault::needs_a_path_the_node_has_not_got`.

### Consequences

`sim/tests/node.rs` no longer asserts the snapshot path absent; it asserts it **reached**
on every seed, and a seed that takes no snapshot fails there. The refusal stays an
absence, with its reason, exactly as it was. Five bugs of the node's snapshot wiring are
fixed, four of them in code D-083 wrote and one in the routing D-078's follower
compaction needed; each is a path a server gets for free by ending its run-loop
incarnation and a node cannot. `sim/install.rs` is unchanged and green. The stream folds — `raft_messages`, `snapshot_takes`,
`retakes_under_streams`, `duplicate_chunk_loop` — are keyed by range, and
`raft_messages` reads the node's batch frames, which it did not: every fold built on it
answered empty on every seed of the node cluster, whatever the node did.

---

_Next entry: D-087. Add one before implementing anything not covered above._

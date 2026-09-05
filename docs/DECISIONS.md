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

_Next entry: D-028. Add one before implementing anything not covered above._

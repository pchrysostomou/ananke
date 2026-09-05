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

_Next entry: D-016. Add one before implementing anything not covered above._

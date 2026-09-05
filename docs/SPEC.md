# SPEC.md — ananke technical specification

Status: draft v0.1 (September 2026). This document evolves per phase. Sections marked
`[P<n>]` are frozen once that phase is tagged; later changes require a DECISIONS.md entry.

---

## 0. Glossary

- **Node** — one ananke process. Holds replicas of zero or more ranges.
- **Range** — a contiguous key interval `[start, end)`. Unit of replication and rebalancing.
- **Replica** — one copy of a range on one node, participating in that range's Raft group.
- **Tenant** — an isolation boundary. All keys are prefixed by tenant id. Separate
  encryption keys, separate RBAC scope.
- **Environment** — the trait through which all non-deterministic I/O flows.
- **Simulation** — a run where `Environment` is implemented by the moirae-driven
  deterministic simulator.
- **Trace** — the moirae event stream for a run. Replayable.

---

## 1. Phase 0 — Deterministic runtime `[P0]`

### 1.1 The `Environment` trait

```rust
pub trait Environment: Send + Sync + 'static {
    type Clock: Clock;
    type Fs: FileSystem;
    type Net: Network;
    type Rng: Rng;

    fn clock(&self) -> &Self::Clock;
    fn fs(&self) -> &Self::Fs;
    fn net(&self) -> &Self::Net;
    fn rng(&self) -> &Self::Rng;
    fn spawn<F: Future<Output = ()> + Send + 'static>(&self, name: &'static str, f: F) -> TaskHandle;
    fn trace(&self, event: TraceEvent);
}
```

Sub-traits (all futures are `Send` so generic code can spawn them):

```rust
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;       // ananke-env type, monotonic (D-013)
    fn wall(&self) -> WallTime;     // ananke-env type, Unix-epoch based (D-013)
    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send;
}
pub trait FileSystem: Send + Sync + 'static { type File: File; /* open, rename, read_dir, sync_dir, ... */ }
pub trait Network: Send + Sync + 'static { type Socket: Socket; fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Socket>> + Send; }
pub trait Socket: Send + Sync + 'static { /* local_addr, send (enqueue-and-return), recv -> (from, msg); see D-015 */ }
pub trait Rng: Send + Sync + 'static { fn fill_bytes(&self, dest: &mut [u8]); /* next_u64, below, ... */ }
```

`ananke-env` also exports `DetHashMap` / `DetHashSet`, hash maps whose hasher is seeded
from `Environment::rng()` at construction (D-014). `std::collections::HashMap` and
`HashSet` are banned outside `ananke-env`.

Two implementations:

- `RealEnv` — tokio, `std::fs` (with fsync semantics honoured), OS entropy RNG,
  system clock.
- `SimEnv` — single-threaded deterministic executor. Virtual clock advanced by the
  scheduler. In-memory filesystem with **fault model** (see 1.3). In-memory network with
  **fault model**. Seeded RNG. Every scheduling decision is made by the moirae scheduler
  and recorded in the trace.

### 1.2 Clock semantics

- `Clock::now() -> Instant` (monotonic) and `Clock::wall() -> WallTime`. Both are
  `ananke-env` types (D-013): plain nanosecond counters with `Duration` arithmetic,
  converted to and from `std::time` only inside `ananke-env`. `std::time::Instant` and
  `std::time::SystemTime` are banned outside `ananke-env`.
- Under simulation, each node has its own clock with configurable **skew** and **drift**.
  Nothing in ananke may assume clocks are synchronised across nodes.
- Timers are futures resolved by the scheduler. No `tokio::time::sleep` anywhere.

### 1.3 Simulated filesystem fault model

- **Torn writes**: a write may persist any prefix at crash.
- **Lost fsync**: `fsync` returns Ok but data is only durable with probability `p_durable`
  unless the sim is in "strict" mode.
- **Bit rot**: after crash, any block may be corrupted (checksums must catch this).
- **Directory entry loss**: rename without directory fsync may be lost.
- **Latency**: configurable distribution per op.

These are the faults FoundationDB and TigerBeetle test against. If the storage engine
survives 10k random seeds here, it is likely correct on real disks.

### 1.4 Simulated network fault model

- Message drop, duplicate, reorder, delay (per-link distributions).
- **Partitions**: arbitrary node subsets, symmetric or asymmetric, time-bounded.
- **Slow links** and **bandwidth caps**.
- **Node crash/restart** with the filesystem fault model applied at crash.
- Byzantine faults are **out of scope** (we assume crash-stop + authenticated links).

### 1.5 moirae bridge

- `SimEnv` is built on `moirae-core`'s scheduler and emits `moirae-protocols` trace events.
- ananke defines a `TraceEvent` enum; the bridge serialises it to moirae's format with
  ananke-specific payloads (range id, term, log index, txn id) so the studio can
  filter/visualise per range and per txn.
- Trace must be stable: a bug reproduced from seed `s` in version `v` must still reproduce
  in `v` given the same seed.

### 1.6 Phase 0 exit criteria

- `sim/echo.rs`: N nodes running an echo protocol under partitions; trace opens in studio.
- Two runs with the same seed produce byte-identical traces (CI check).
- `cargo clippy` with `disallowed-methods` for all direct I/O.

---

## 2. Phase 1 — Storage engine `[P1]`

An LSM tree. Not a B-tree, because LSM write path is simpler to make crash-safe and the
workload (Raft log + MVCC versions) is write-heavy.

### 2.1 On-disk layout

```
<data_dir>/
  MANIFEST-<n>         current manifest (versioned, CRC)
  <n>.wal              write-ahead log segments
  <n>.sst              sorted string tables
  CURRENT              points at active manifest (written via rename)
```

### 2.2 WAL

- Append-only, segmented. Record = `len | crc32c | payload`.
- Group commit: batch writers waiting on the same fsync.
- Recovery reads until first CRC failure or torn record; everything after is discarded.

### 2.3 Memtable

- Skiplist (crossbeam-skiplist). Flushed to SST when > `memtable_bytes` (default 64 MiB).
- Immutable memtables kept until flush completes; reads consult them.

### 2.4 SSTable

- Blocks of ~4 KiB, prefix-compressed keys, per-block CRC.
- Bloom filter per SST (10 bits/key).
- Index block with first key per data block.
- Footer: offsets + magic + format version.

### 2.5 Compaction

- Leveled compaction, L0 → L6, size ratio 10.
- Manual trigger API for tests; background task under `Environment::spawn` in production.
- Compaction is **crash-safe**: new SSTs written, manifest updated atomically, old SSTs
  deleted only after manifest fsync.

### 2.6 Key encoding

All keys are byte strings, ordered lexicographically:

```
<tenant_id:u64 BE> <table_id:u64 BE> <user_key...> <!version:u64 BE, inverted>
```

Inverted version means newest version sorts first for a given user key — MVCC reads
become a single seek.

### 2.7 API

```rust
pub trait Engine {
    fn get(&self, key: &[u8], snapshot: Version) -> Result<Option<Bytes>>;
    fn scan(&self, range: Range<&[u8]>, snapshot: Version) -> Result<Iter>;
    fn write(&self, batch: WriteBatch, sync: bool) -> Result<()>;
    fn snapshot(&self) -> Version;
    fn checkpoint(&self, dir: &Path) -> Result<()>;   // for Raft snapshots
}
```

### 2.8 Exit criteria

- Property test: sequence of random ops + random crash points ⇒ recovered state equals
  model (an in-memory BTreeMap replaying only committed batches).
- 10k seeds green in CI nightly.
- Bench: > 200k writes/s single-threaded on real env (sanity, not a goal).

---

## 3. Phase 2 — Raft `[P2]`

Raft as in the extended paper (Ongaro 2014), with:

- **Pre-vote** (prevents disruptive rejoining nodes).
- **Leader lease reads** (read-index for linearizable reads without a log round-trip,
  with a lease bounded by *max clock drift*, which the sim will actively violate to
  prove we handle it).
- **Joint consensus** membership changes (not single-server changes — we want to test
  the hard version).
- **Snapshots** via `Engine::checkpoint`, streamed in chunks, resumable.
- **Batching + pipelining** of AppendEntries.

State machine is the storage engine from Phase 1; the Raft log itself is also stored
in the engine under a reserved tenant id.

RAFT.md holds the invariants we check under simulation:

1. Election safety — at most one leader per term.
2. Log matching.
3. Leader completeness.
4. State machine safety — same index ⇒ same command applied on every node.
5. Linearizability of the KV API (checked with a Wing-Gong / porcupine-style checker
   in Rust over the trace).

### Exit criteria

- All five invariants hold across 10k seeds with the full network+disk fault model.
- Membership change from 3 → 5 → 3 nodes under partition, no availability loss beyond
  one election timeout.
- Devlog post: "Breaking Raft with moirae" showing a real bug found and its trace.

---

## 4. Phase 3 — Multi-raft sharding `[P3]`

- **Range descriptor** stored in a meta range (itself replicated; bootstrap range 0 is
  found via config, then meta range via range 0 — Spanner/CockroachDB pattern).
- Each node runs many Raft groups; a shared **Raft ticker** and message batcher per
  peer pair, so 10k ranges don't mean 10k heartbeat streams.
- **Split**: leader proposes split at key `k`; on apply, both halves become live with
  the same replica set. New group starts at term 1 with the parent's applied index
  recorded as its snapshot.
- **Merge**: only adjacent ranges with identical replica sets; two-phase with a
  "subsume" command.
- **Rebalancer**: background task on a leaseholder-elected node. Moves replicas to
  balance range count and leader count. Uses joint consensus per move.
- **Routing**: clients cache range descriptors; on `RangeMismatch` error they refresh.

### Exit criteria

- Linearizability holds across split/merge/rebalance under faults.
- 1000-range cluster in simulation stays balanced within 10% after node add/remove.

---

## 5. Phase 4 — Transactions `[P4]`

**Percolator model** (Google 2010), as in TiKV: no dedicated coordinator, transaction
state lives in the data.

- **Timestamps**: a hybrid logical clock (HLC) per node, with a *timestamp oracle*
  range for strict ordering in the first version. (DECISIONS: revisit TrueTime-style
  bounded uncertainty later.)
- Each row has three logical columns: `data`, `lock`, `write`.
- **Prewrite**: for each key, check no newer write, no lock, then write lock + data.
  Primary key chosen first; secondaries reference it.
- **Commit**: commit primary (write `write` record, remove lock); secondaries committed
  lazily or by readers who resolve the primary's state.
- **Isolation**: snapshot isolation. Optional SSI (serializable) via read-set validation
  is a stretch goal.
- **Deadlock**: wait-die on lock conflict, with a TTL on locks so a crashed client
  can't hold keys forever.

### Exit criteria

- elle (Jepsen's checker) run against traces exported from simulation reports no
  anomalies for SI (G1a, G1b, G1c, G-single).
- A deliberately-injected bug (skip secondary lock check) is caught by elle within
  100 seeds — proves the harness has teeth.

---

## 6. Phase 5 — SQL `[P5]`

Small, correct subset. Postgres wire protocol so existing tools (psql, drivers) work.

- **Parser**: hand-written recursive descent (not sqlparser-rs, to keep error messages
  and the grammar ours). Grammar in `docs/GRAMMAR.ebnf`.
- **Catalog**: tables, columns, indexes stored in a system tenant.
- **Types**: `INT8`, `TEXT`, `BYTEA`, `BOOL`, `TIMESTAMPTZ`, `DECIMAL`. No nulls
  ambiguity: SQL three-valued logic implemented properly.
- **Planner**: rule-based. Predicate pushdown, index selection, join ordering by
  simple cardinality heuristic.
- **Executor**: volcano-style iterators over the txn API. Distributed scans fan out per
  range and merge.
- **Indexes**: secondary indexes as separate key ranges, maintained transactionally.
- **DDL**: online, via schema versions in the catalog and lazy row upgrade.

Supported statements at exit: `CREATE/DROP TABLE`, `CREATE/DROP INDEX`, `INSERT`,
`UPDATE`, `DELETE`, `SELECT` with `WHERE`, `JOIN` (inner/left), `ORDER BY`, `LIMIT`,
`GROUP BY` with `COUNT/SUM/MIN/MAX`, `BEGIN/COMMIT/ROLLBACK`.

### Exit criteria

- sqllogictest subset passes.
- `psql` connects and runs the demo schema.

---

## 7. Phase 6 — Security `[P6]`

This is the phase that aligns with the Information Security MSc and should be written
up as a formal design doc (`docs/SECURITY.md`) with a threat model.

- **Node identity**: each node has an X.509 cert issued by a cluster CA; all node-to-node
  and client-to-node traffic is mTLS (rustls). Cert rotation without restart.
- **Threat model**: attacker with network access (mitigated by mTLS), attacker with
  disk access to a stolen node (mitigated by encryption at rest), malicious tenant
  (mitigated by RBAC + key isolation). Not in scope: compromised node with live keys in
  memory, side channels.
- **Encryption at rest**: envelope encryption. Per-tenant DEK, wrapped by a cluster KEK
  held in a KMS abstraction (file-based dev implementation; interface for external KMS).
  SST blocks encrypted with AES-256-GCM; nonce derived from (sst id, block id). WAL
  encrypted per segment.
- **Key rotation**: tenant DEK rotation triggers background re-encryption compaction.
- **RBAC**: roles, grants at database/table/column level, stored in the catalog and
  cached with versioned invalidation.
- **Audit log**: append-only, hash-chained (each entry includes the hash of the previous),
  stored in its own replicated range so a single node cannot silently truncate it.
- **Row-level security**: policy expressions evaluated by the planner. Stretch.

### Exit criteria

- Threat model document reviewed against STRIDE.
- Test: dump a simulated node's disk; assert no plaintext user data recoverable.
- Test: audit chain verifies; a tampered entry is detected.

---

## 8. Phase 7 — External verification `[P7]`

- Jepsen test suite (Clojure) against a real 5-node deployment, with nemesis for
  partitions/clock skew/kill. Published results.
- Fuzzing: cargo-fuzz targets for SST parsing, WAL parsing, wire protocol, SQL parser.
- Long-running "chaos" simulation: 24h continuous seeds, dashboard of found bugs.

---

## 9. Cross-cutting

### 9.1 Observability
- Every crate emits `tracing` spans. In simulation these map 1:1 to moirae events.
- Prometheus metrics endpoint in `ananke-server`.

### 9.2 Configuration
- Single TOML file. Every knob has a doc comment and a default. Config schema validated
  at startup.

### 9.3 Testing pyramid
1. Unit tests per crate.
2. Simulation scenarios in `sim/` (the bulk of confidence).
3. Property tests (proptest) for encoders/parsers.
4. Real-env integration tests, few and slow.
5. Jepsen, Phase 7.

### 9.4 Versioning
- Workspace version bumps together. Disk format version in every file footer; reader
  supports N-1.

### 9.5 Out of scope (v1)
- Byzantine fault tolerance. Geo-partitioning. Column-store / analytics. Stored
  procedures. Full-text search. Change data capture (maybe v2).
